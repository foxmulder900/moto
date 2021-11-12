import json

import boto3
import pytest

from moto import mock_dynamodb2, mock_iam
from botocore.exceptions import ClientError


from moto.core import set_initial_no_auth_action_count


@mock_iam
def create_user_with_access_key(user_name="test-user"):
    client = boto3.client("iam", region_name="us-east-1")
    client.create_user(UserName=user_name)
    return client.create_access_key(UserName=user_name)["AccessKey"]


@mock_iam
def create_user_with_access_key_and_attached_policy(
    user_name, policy_document, policy_name="policy1"
):
    client = boto3.client("iam", region_name="us-east-1")
    client.create_user(UserName=user_name)
    policy_arn = client.create_policy(
        PolicyName=policy_name, PolicyDocument=json.dumps(policy_document)
    )["Policy"]["Arn"]
    client.attach_user_policy(UserName=user_name, PolicyArn=policy_arn)
    return client.create_access_key(UserName=user_name)["AccessKey"]


@set_initial_no_auth_action_count(4)
@mock_dynamodb2
def test_get_object_access_denied():
    access_key = create_user_with_access_key()
    dynamodb = boto3.resource(
        "dynamodb",
        region_name="us-east-1",
        aws_access_key_id=access_key["AccessKeyId"],
        aws_secret_access_key=access_key["SecretAccessKey"]
    )
    dynamodb.create_table(
        TableName="test-table",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}]
    )
    dynamodb_table = dynamodb.Table("test-table")
    dynamodb_table.put_item(Item={"id": "test-item-1"})

    with pytest.raises(ClientError) as e:
        dynamodb_table.get_item(Key={"id": "test-item-1"})

    assert e.value.response["Error"]["Code"] == "403"


@set_initial_no_auth_action_count(6)
@mock_dynamodb2
def test_get_object_access_allowed():
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*"}],
    }
    access_key = create_user_with_access_key_and_attached_policy("test-user", policy_document)

    dynamodb = boto3.resource(
        "dynamodb",
        region_name="us-east-1",
        aws_access_key_id=access_key["AccessKeyId"],
        aws_secret_access_key=access_key["SecretAccessKey"]
    )
    dynamodb.create_table(
        TableName="test-table",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}]
    )
    dynamodb_table = dynamodb.Table("test-table")
    dynamodb_table.put_item(Item={"id": "test-item-1"})

    response = dynamodb_table.get_item(Key={"id": "test-item-1"})

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
