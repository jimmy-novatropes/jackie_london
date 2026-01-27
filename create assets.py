from pathlib import Path

# 🔧 Configure these
AWS_ACCOUNT_ID = ""
STATE_MACHINE_NAME = "jackie_london_sync"
AWS_PROFILE = "jackie_london"

LAMBDA_NAMES = [
    "bc_2_hs_company_sync",
    "bc_2_hs_contact_sync",
    "bc_2_hs_deal_sync",
    "hs_contact_company_associations",
    "hs_deal_company_associations",
    "hs_deal_contact_associations",
]


def make_serverless_yml(lambda_name: str) -> str:
    return f"""\
service: {lambda_name.replace('_', '-')}

provider:
  name: aws
  runtime: python3.11
  region: us-east-1
  stage: ${{{{opt:stage, 'dev'}}}}
  maximumRetryAttempts: 0
  iamRoleStatements:
    - Effect: Allow
      Action:
        - ec2:CreateNetworkInterface
        - ec2:DescribeNetworkInterfaces
        - ec2:DeleteNetworkInterface
      Resource: "*"

functions:
  {lambda_name}:
    name: {lambda_name.replace('_', '-')}
    handler: handler.lambda_handler
    timeout: 160
    memorySize: 128
    package:
      patterns:
        - '!**'
        - 'handler.py'
        - 'supporting_functions.py'
        - 'object_properties.json'
        - 'src/**'
        - '!**/*tests*/**'
        - '!.pytest_cache/**'
        - '!**/__pycache__/**'
        - '!**/*.pyc'

plugins:
  - serverless-python-requirements

custom:
  pythonRequirements:
    dockerizePip: false
    slim: true
    useDownloadCache: true
    useStaticCache: true

package:
  individually: true
"""


def scaffold(base_dir="."):
    base = Path(base_dir)

    # -------------------
    # GLOBAL FILES
    # -------------------

    global_templates = {
        ".github/workflows/prod_cicd_serverless_lambda_update.yml": """\
name: (PROD) Create Update Serverless Lambdas
on: [workflow_dispatch]

jobs:
  update-lambdas:
    runs-on: ubuntu-latest
    environment: prod

    steps:
      - name: Checkout repo
        uses: actions/checkout@v2

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install Serverless
        run: npm install -g serverless@3.38.0

      - name: Deploy all Serverless Lambdas
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_OPENFLOW_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_OPENFLOW_SECRET_ACCESS_KEY }}
          AWS_REGION: us-east-1
        run: |
          cd serverless_lambdas
          for dir in */ ; do
            cd "$dir"
            if [ -f requirements.txt ]; then
              pip install -r requirements.txt -t .
            fi
            if grep -q serverless-python-requirements serverless.yml; then
              serverless plugin install -n serverless-python-requirements
            fi
            serverless deploy --stage prod
            cd ..
          done
"""
    }

    # Only scaffold step functions if AWS_ACCOUNT_ID is set
    if AWS_ACCOUNT_ID.strip():
        global_templates.update({
            ".github/workflows/prod_cicd_stepfunctions_update.yml": f"""\
name: (PROD) Create or Update Step Function Workflow
on: [workflow_dispatch]

jobs:
  deploy-stepfunction:
    runs-on: ubuntu-latest
    environment: prod

    steps:
      - name: Checkout repo
        uses: actions/checkout@v2

      - name: Create or Update Step Function
        env:
          AWS_ACCESS_KEY_ID: ${{{{ secrets.AWS_OPENFLOW_ACCESS_KEY_ID }}}}
          AWS_SECRET_ACCESS_KEY: ${{{{ secrets.AWS_OPENFLOW_SECRET_ACCESS_KEY }}}}
          AWS_REGION: us-east-1
          STATE_MACHINE_NAME: {STATE_MACHINE_NAME}
          STATE_MACHINE_ARN: arn:aws:states:us-east-1:{AWS_ACCOUNT_ID}:stateMachine:{STATE_MACHINE_NAME}
          ROLE_ARN: arn:aws:iam::{AWS_ACCOUNT_ID}:role/service-role/StepFunctions-{STATE_MACHINE_NAME}-role
        run: |
          set -e
          if aws stepfunctions describe-state-machine --state-machine-arn "$STATE_MACHINE_ARN" > /dev/null 2>&1; then
            aws stepfunctions update-state-machine \
              --state-machine-arn "$STATE_MACHINE_ARN" \
              --definition file://stepfunctions/stepfunction_definition.json \
              --role-arn "$ROLE_ARN"
          else
            aws stepfunctions create-state-machine \
              --name "$STATE_MACHINE_NAME" \
              --definition file://stepfunctions/stepfunction_definition.json \
              --role-arn "$ROLE_ARN" \
              --type STANDARD
          fi
""",
            "stepfunctions/stepfunction_definition.json": "{\n  \"Comment\": \"State machine placeholder\",\n  \"StartAt\": \"Pass\",\n  \"States\": { \"Pass\": { \"Type\": \"Pass\", \"End\": true } }\n}\n",
            "stepfunctions/get_stepfunction_configuration.py": f"""\
import boto3, json

profile = '{AWS_PROFILE}'
state_machine_arn = 'arn:aws:states:us-east-1:{AWS_ACCOUNT_ID}:stateMachine:{STATE_MACHINE_NAME}'
output_file = 'stepfunction_definition.json'

session = boto3.Session(profile_name=profile)
sfn = session.client('stepfunctions')

response = sfn.describe_state_machine(stateMachineArn=state_machine_arn)

with open(output_file, 'w') as f:
    json.dump(json.loads(response['definition']), f, indent=2)

print("Definition saved")
"""
        })

    # write global files
    for rel, content in global_templates.items():
        path = base / rel
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

    # -------------------
    # PER-LAMBDA FILES
    # -------------------

    for name in LAMBDA_NAMES:
        lambda_dir = base / "serverless_lambdas" / name

        if lambda_dir.exists():
            print(f"⏭️ Skipping existing lambda folder: {name}")
            continue

        files = {
            f"serverless_lambdas/{name}/serverless.yml": make_serverless_yml(name),
            f"serverless_lambdas/{name}/handler.py": "",
            f"serverless_lambdas/{name}/supporting_functions.py": "",
            f"serverless_lambdas/{name}/requirements.txt": "",
            f"serverless_lambdas/{name}/object_properties.json": "{}",
            f"serverless_lambdas/{name}/.gitkeep": ""
        }

        for rel, content in files.items():
            path = base / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

        print(f"✅ Created lambda scaffold: {name}")


if __name__ == "__main__":
    scaffold(".")

