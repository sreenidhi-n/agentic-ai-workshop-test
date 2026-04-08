"""
Health data pipeline — document processing, messaging, and secrets management.
Uses AWS services not present anywhere in the existing workshop modules.
"""

import boto3

# ── Document processing (not in any existing module) ──────────────────────────
textract = boto3.client("textract", region_name="us-east-1")
comprehend_medical = boto3.client("comprehendmedical", region_name="us-east-1")

def extract_clinical_notes(document_bytes: bytes) -> dict:
    """Extract structured data from a scanned clinical document."""
    result = textract.analyze_document(
        Document={"Bytes": document_bytes},
        FeatureTypes=["FORMS", "TABLES"],
    )
    return result

def detect_medical_entities(text: str) -> list:
    """Detect medications, conditions, and dosages from free text."""
    result = comprehend_medical.detect_entities_v2(Text=text)
    return result.get("Entities", [])


# ── Async messaging (not in any existing module) ───────────────────────────────
sqs = boto3.client("sqs", region_name="us-east-1")
sns = boto3.client("sns", region_name="us-east-1")

TRIAGE_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789/caladrius-triage"
ALERT_TOPIC_ARN  = "arn:aws:sns:us-east-1:123456789:caladrius-alerts"

def enqueue_case(case_id: str, payload: dict) -> str:
    """Send a patient case to the async triage queue."""
    resp = sqs.send_message(
        QueueUrl=TRIAGE_QUEUE_URL,
        MessageBody=str(payload),
        MessageAttributes={
            "case_id": {"DataType": "String", "StringValue": case_id}
        },
    )
    return resp["MessageId"]

def broadcast_alert(message: str) -> None:
    """Broadcast a clinical alert to all subscribed endpoints."""
    sns.publish(
        TopicArn=ALERT_TOPIC_ARN,
        Message=message,
        Subject="Caladrius Clinical Alert",
    )


# ── Secrets management (not in any existing module) ────────────────────────────
secrets = boto3.client("secretsmanager", region_name="us-east-1")

def get_ehr_credentials() -> dict:
    """Fetch EHR API credentials from Secrets Manager at runtime."""
    resp = secrets.get_secret_value(SecretId="caladrius/ehr-api-key")
    return {"api_key": resp["SecretString"]}


# ── Streaming audit log (not in any existing module) ───────────────────────────
kinesis = boto3.client("kinesis", region_name="us-east-1")

def stream_audit_event(event_type: str, data: dict) -> None:
    """Stream every agent decision to Kinesis for real-time compliance monitoring."""
    import json, time
    kinesis.put_record(
        StreamName="caladrius-audit-stream",
        Data=json.dumps({"event": event_type, "data": data, "ts": time.time()}).encode(),
        PartitionKey=event_type,
    )

