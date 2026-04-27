import os
import boto3
import json
from botocore.exceptions import ClientError
from typing import Dict, Any


# ------------------------------
# Credentials loader
# ------------------------------
def is_running_in_lambda() -> bool:
    return "AWS_LAMBDA_FUNCTION_NAME" in os.environ


def load_secrets(secret_name, region="us-east-1") -> Dict[str, Any]:
    session = boto3.session.Session()
    client = session.client("secretsmanager", region_name=region)
    try:
        resp = client.get_secret_value(SecretId=secret_name)
        return json.loads(resp["SecretString"])
    except ClientError as e:
        print(f"❌ Failed to fetch secret {secret_name}: {e}")
        raise


def load_secrets_locally(secret_name: str, region: str = "us-east-1", profile: str = "jackie_london") -> Dict[str, Any]:
    """
    Load a secret from AWS Secrets Manager using a local AWS CLI profile.
    """
    try:
        session = boto3.session.Session(profile_name=profile)
        client = session.client("secretsmanager", region_name=region)

        resp = client.get_secret_value(SecretId=secret_name)
        return json.loads(resp["SecretString"])

    except ClientError as e:
        print(f"❌ Failed to fetch secret {secret_name}: {e}")
        raise