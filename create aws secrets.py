import boto3
import json

# 🔧 Configure
AWS_PROFILE = "jackie_london"   # your AWS CLI profile (omit for Lambda/EC2 IAM role)
REGION_NAME = "us-east-1"  # region
SECRET_NAME = "JACKIE_LONDON_KEYS"
SECRET_VALUE = {
    "HUBSPOT_TOKEN": ""
}


def create_secret():
    # Session (profile for local use, default for Lambda)
    session = boto3.Session(profile_name=AWS_PROFILE) if AWS_PROFILE else boto3
    client = session.client("secretsmanager", region_name=REGION_NAME)

    try:
        response = client.create_secret(
            Name=SECRET_NAME,
            SecretString=json.dumps(SECRET_VALUE)
        )
        print(f"✅ Secret created: {response['ARN']}")
    except client.exceptions.ResourceExistsException:
        print(f"⚠️ Secret '{SECRET_NAME}' already exists. Updating...")
        response = client.update_secret(
            SecretId=SECRET_NAME,
            SecretString=json.dumps(SECRET_VALUE)
        )
        print(f"🔄 Secret updated: {response['ARN']}")


if __name__ == "__main__":
    create_secret()

