import boto3, json

profile = 'jackie_london'
state_machine_arn = 'arn:aws:states:us-east-1:630977122946:stateMachine:jackie_london_sync'
state_machine_arn = 'arn:aws:states:us-east-1:630977122946:stateMachine:bc_2_hubspot_sync'
output_file = 'stepfunction_definition.json'

session = boto3.Session(profile_name=profile)
sfn = session.client('stepfunctions')

response = sfn.describe_state_machine(stateMachineArn=state_machine_arn)

with open(output_file, 'w') as f:
    json.dump(json.loads(response['definition']), f, indent=2)

print("Definition saved")
