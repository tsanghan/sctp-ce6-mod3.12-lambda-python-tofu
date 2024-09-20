import pytest
from moto import mock_dynamodb2, mock_sqs
import os
import json
import uuid
import boto3
import base64
from lambda_function import lambda_handler

# Mock the environment variables
os.environ['TABLE_NAME'] = 'TestTable'
os.environ['QUEUE_URL'] = 'https://sqs.us-east-1.amazonaws.com/123456789012/TestQueue'

# Helper function to create the mock DynamoDB table
def create_mock_dynamodb_table(dynamodb_resource):
    table = dynamodb_resource.create_table(
        TableName=os.environ['TABLE_NAME'],
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'  # Partition key
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'S'  # String
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    table.wait_until_exists()
    return table

@pytest.fixture
def mock_sqs_client():
    with mock_sqs():
        sqs_client = boto3.client('sqs', region_name='us-east-1')
        sqs_client.create_queue(QueueName='TestQueue')
        yield sqs_client

@pytest.fixture
def mock_dynamodb_resource():
    with mock_dynamodb2():
        dynamodb_resource = boto3.resource('dynamodb', region_name='us-east-1')
        create_mock_dynamodb_table(dynamodb_resource)
        yield dynamodb_resource

def test_lambda_handler_success(mock_dynamodb_resource, mock_sqs_client):
    # Arrange
    test_event = {
        'body': base64.b64encode(json.dumps({
            'attribute1': 'value1',
            'attribute2': 'value2'
        }).encode('utf-8')).decode('utf-8')
    }

    # Act
    response = lambda_handler(test_event, None)

    # Assert
    response_body = json.loads(response['body'])
    assert response['statusCode'] == 200
    assert 'id' in response_body
    assert response_body['message'] == 'Success'

    # Verify DynamoDB contains the item
    table = mock_dynamodb_resource.Table(os.environ['TABLE_NAME'])
    stored_item = table.get_item(Key={'id': response_body['id']})
    assert 'Item' in stored_item
    assert stored_item['Item']['attribute1'] == 'value1'

    # Verify message sent to SQS
    messages = mock_sqs_client.receive_message(
        QueueUrl=os.environ['QUEUE_URL'],
        MaxNumberOfMessages=1
    )
    assert 'Messages' in messages
    sqs_message_body = json.loads(messages['Messages'][0]['Body'])
    assert sqs_message_body['attribute1'] == 'value1'

def test_lambda_handler_failure(mock_dynamodb_resource, mock_sqs_client):
    # Arrange
    test_event = {
        'body': 'Invalid base64'  # This will trigger an error in base64 decoding
    }

    # Act
    response = lambda_handler(test_event, None)

    # Assert
    response_body = json.loads(response['body'])
    assert response['statusCode'] == 500
    assert response_body['message'] == 'Error'
    assert 'error' in response_body
