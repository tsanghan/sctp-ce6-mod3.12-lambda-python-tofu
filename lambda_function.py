import os
import json
import uuid
import boto3
import base64

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')

# Get environment variables
TABLE_NAME = os.environ.get('TABLE_NAME')
QUEUE_URL = os.environ.get('QUEUE_URL')

# Get the DynamoDB table
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    try:
        # Parse the JSON body
        print(event)
        item = json.loads(base64.b64decode(event['body']).decode('utf-8'))

        # Add a unique ID to the item
        item['id'] = str(uuid.uuid4())

        # Put item into DynamoDB
        table.put_item(Item=item)

        # Send message to SQS
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(item)
        )

        response = {
            'statusCode': 200,
            'body': json.dumps({'message': 'Success', 'id': item['id']})
        }
    except Exception as e:
        print('Error:', e)
        response = {
            'statusCode': 500,
            'body': json.dumps({'message': 'Error', 'error': str(e)})
        }

    return response
