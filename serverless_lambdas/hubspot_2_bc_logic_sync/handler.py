import json
from requests_oauthlib import OAuth1Session
# from c1_webhook_logic_handlers import handle_contact, handle_company, handle_deal, handle_line_item
from typing import List, Dict
from collections import defaultdict


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

creds = {

}

# --------------------------------------------------
# NetSuite helpers
# --------------------------------------------------



ROUTER = {
    # "contact": handle_contact,
    # "company": handle_company,
    # "deal": handle_deal,
    # "line_item": handle_line_item,
}


# ------------------------------
# Core processor
# ------------------------------
def process_events(event):

    for e1 in event:
        grouped: Dict[str, List[str]] = defaultdict(list)
        sub_type = e1.get("subscriptionType", "")
        if e1["changeSource"] == "INTEGRATION" or e1["changeSource"] == "DATA_ENRICHMENT" or e1["subscriptionType"] == "deal.associationChange":
            print(f"⏭️ Skipping non-UI changeSource event: {e1}")
            continue
        obj_type = sub_type.split(".")[0] if "." in sub_type else ""
        handler = ROUTER.get(obj_type.lower())

        if handler:
            result = handler(e1, creds)
            grouped[obj_type].append(result)
        else:
            grouped["unknown"].append(f"⚠️ No handler for subscriptionType: {sub_type}")



# --------------------------------------------------
# Lambda
# --------------------------------------------------
# def lambda_handler(event, context):
#
#     try:
#         body = event.get("body", "[]")
#         events = json.loads(body) if isinstance(body, str) else body
#         process_events(events)
#         return {"statusCode": 200, "body": json.dumps({"status": "ok"})}
#     except Exception as e:
#         print(f"❌ Error processing events: {e}")
#         return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


import json


def lambda_handler(event, context):
    try:
        # Case 1: Lambda-to-Lambda (your new flow)
        if "records" in event:
            events = event["records"]

        # Case 2: API Gateway (existing behavior)
        else:
            body = event.get("body", "[]")
            events = json.loads(body) if isinstance(body, str) else body

        # normalize to list
        if isinstance(events, dict):
            events = [events]

        print(f"Received {len(events)} events, {events}")
        return
        process_events(events)

        return {
            "statusCode": 200,
            "body": json.dumps({"status": "ok", "count": len(events)})
        }

    except Exception as e:
        print(f"❌ Error processing events: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


event_deal_create_new_company = {
    'version': '2.0',
    'routeKey': 'ANY /hubspot-changes-2-netsuite',
    'rawPath': '/hubspot-changes-2-netsuite',
    'rawQueryString': '',
    'headers': {
        'accept': '*/*', 'content-length': '267', 'content-type': 'application/json', 'host': 'fcjrfxesn5.execute-api.us-east-1.amazonaws.com',
        'user-agent': 'HubSpot Connect 2.0 (http://dev.hubspot.com/) (namespace: webhooks-nio-http-client) - WebhooksExecutorDaemon-executor',
        'x-amzn-trace-id': 'Root=1-69720c8a-7f640f40011b2cf4316e45c4', 'x-forwarded-for': '216.157.40.40', 'x-forwarded-port': '443',
        'x-forwarded-proto': 'https', 'x-hubspot-request-timestamp': '1769081994066', 'x-hubspot-signature': 'f79b8b68e04d31c8dc9268f1858c8d28e6d54688aa94a1563b85bf452c09a43b',
        'x-hubspot-signature-v3': 'GMupBqijZdO2OoFsfNLc78nY01nm76QSDxKcwVzAOrs=', 'x-hubspot-signature-version': 'v1', 'x-hubspot-timeout-millis': '10000'},
    'requestContext': {
        'accountId': '637423516866', 'apiId': 'fcjrfxesn5', 'domainName': 'fcjrfxesn5.execute-api.us-east-1.amazonaws.com', 'domainPrefix': 'fcjrfxesn5',
        'http': {
            'method': 'POST', 'path': '/hubspot-changes-2-netsuite', 'protocol': 'HTTP/1.1', 'sourceIp': '216.157.40.40',
            'userAgent': 'HubSpot Connect 2.0 (http://dev.hubspot.com/) (namespace: webhooks-nio-http-client) - WebhooksExecutorDaemon-executor'},
        'requestId': 'Xlblqj4RoAMEP5w=', 'routeKey': 'ANY /hubspot-changes-2-netsuite', 'stage': '$default', 'time': '22/Jan/2026:11:39:54 +0000', 'timeEpoch': 1769081994221},
    'body': '[{"eventId":1030198139,"subscriptionId":4578255,"portalId":242711451,"appId":19635961,"occurredAt":1769081993375,"subscriptionType":"deal.creation","attemptNumber":0,"objectId":267448862446,"changeFlag":"CREATED","changeSource":"CRM_UI","sourceId":"userId:52530071"}]',
    'isBase64Encoded': False
}
if __name__ == "__main__":

    test_events = [
        # event_deal_create_old_company,
        event_deal_create_new_company,
        ]

    for input in test_events:
        lambda_handler(input, None)