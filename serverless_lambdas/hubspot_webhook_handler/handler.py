import json
import boto3
from credentials_load import (is_running_in_lambda, load_secrets, load_secrets_locally)
from typing import Dict, Any

def load_jackielondon_creds() -> Dict[str, Any]:
    if is_running_in_lambda():
        return load_secrets("JACKIE_LONDON_KEYS")
    return load_secrets_locally("JACKIE_LONDON_KEYS")

CREDS = load_jackielondon_creds()
HUBSPOT_TOKEN: str = CREDS.get("HUBSPOT_TOKEN", "")
USERNAME: str = CREDS.get("GRAPHQL_USERNAME", "")
PASSWORD: str = CREDS.get("GRAPHQL_PASSWORD", "")


lambda_client = boto3.client("lambda")

# TARGET_LAMBDA = "arn:aws:lambda:us-east-1:637423516866:function:hubspot-changes-2-netsuite"


def lambda_handler(event, context):

    try:
        body = event.get("body", "[]")
        events = json.loads(body) if isinstance(body, str) else body
        object_id = events[0].get("objectId") if events else None

        # lambda_client.invoke(
        #     FunctionName=TARGET_LAMBDA,
        #     InvocationType="Event",  # async
        #     Payload=json.dumps({"records": events})
        # )
        return {
            "statusCode": 200,
            # "body": json.dumps({"status": "ok", "message": f"Invoked {TARGET_LAMBDA} with {len(events)} events"})
            "body": json.dumps({"status": "ok", "message": f"Invokedwith {len(events)} events"})
        }

    except Exception as e:
        print(f"Error parsing event body: {e}")
        return {"status": "error", "message": "Invalid event body"}
