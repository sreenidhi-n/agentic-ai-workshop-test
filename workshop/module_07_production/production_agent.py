"""
Module 07 — Production hardening: observability, storage, and tool serving.

Adds:
  - OpenTelemetry tracing for all agent spans
  - DynamoDB session persistence
  - S3 audit log export
  - FastMCP server exposing the triage agent as a tool
"""

import boto3
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.trace.export.otlp import OTLPSpanExporter
from strands import Agent
from strands.models import BedrockModel
from mcp.server.fastmcp import FastMCP


# ── Observability ─────────────────────────────────────────────────────────────
# Sends spans to an OTLP collector (e.g. AWS X-Ray OTLP endpoint).

provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
provider.add_span_processor(
    BatchSpanProcessor(
        OTLPSpanExporter(endpoint="https://xray.us-east-1.amazonaws.com/v1/traces")
    )
)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("caladrius.triage")


# ── AWS clients ───────────────────────────────────────────────────────────────

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
bedrock_rt = boto3.client("bedrock-runtime", region_name="us-east-1")

session_table = dynamodb.Table("caladrius-agent-sessions")
AUDIT_BUCKET = "caladrius-agent-audit-logs"


# ── Triage agent ──────────────────────────────────────────────────────────────

model = BedrockModel(model_id="anthropic.claude-3-5-sonnet-20241022-v2:0")

triage_agent = Agent(
    model=model,
    system_prompt=(
        "You are a triage agent for Caladrius Health. "
        "Route patient inquiries to billing, technical support, or clinical review."
    ),
)


def run_with_tracing(user_input: str, session_id: str) -> str:
    with tracer.start_as_current_span("triage.run") as span:
        span.set_attribute("session.id", session_id)
        span.set_attribute("input.length", len(user_input))

        # Persist session to DynamoDB
        session_table.put_item(Item={
            "session_id": session_id,
            "input": user_input,
            "status": "running",
        })

        response = triage_agent(user_input)
        result = str(response)

        # Write audit log to S3
        s3.put_object(
            Bucket=AUDIT_BUCKET,
            Key=f"sessions/{session_id}/trace.json",
            Body=result.encode(),
            ContentType="application/json",
        )

        span.set_attribute("output.length", len(result))
        return result


# ── MCP server — expose triage as a remote tool ───────────────────────────────

mcp = FastMCP("caladrius-triage-mcp")


@mcp.tool()
def triage(query: str, session_id: str = "anonymous") -> str:
    """Route a patient support query to the appropriate specialist."""
    return run_with_tracing(query, session_id)


@mcp.tool()
def list_sessions(limit: int = 10) -> list[dict]:
    """Return recent agent sessions from DynamoDB."""
    response = session_table.scan(Limit=limit)
    return response.get("Items", [])


if __name__ == "__main__":
    mcp.run(transport="stdio")
