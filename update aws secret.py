import boto3
import json

AWS_PROFILE = "jackie_london"
REGION_NAME = "us-east-1"
SECRET_NAME = "JACKIE_LONDON_KEYS"


def upsert_secret_key(key, value):
    session = boto3.Session(profile_name=AWS_PROFILE) if AWS_PROFILE else boto3
    client = session.client("secretsmanager", region_name=REGION_NAME)

    # Get existing secret (or start fresh)
    try:
        current = client.get_secret_value(SecretId=SECRET_NAME)
        secret_dict = json.loads(current["SecretString"])
    except client.exceptions.ResourceNotFoundException:
        secret_dict = {}

    # Update / insert key
    secret_dict[key] = value

    # Write back
    client.put_secret_value(
        SecretId=SECRET_NAME,
        SecretString=json.dumps(secret_dict)
    )

    print(f"Updated key '{key}' in {SECRET_NAME}")


if __name__ == "__main__":
    upsert_secret_key("NEW_KEY", "new_value")




