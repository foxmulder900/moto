import boto3
import pytest

from moto import mock_dynamodb2, mock_iam
from botocore.exceptions import ClientError


# @pytest.fixture
# def dynamodb_table():
#     with mock_dynamodb2():
#         dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
#         dynamodb.create_table(
#             TableName="test-table",
#             KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
#             AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}]
#         )
#         dynamodb_table = dynamodb.Table("test-table")
#         dynamodb_table.put_item(Item={"id": "test-item-1"})
#         yield dynamodb_table
from moto.core import set_initial_no_auth_action_count


@mock_iam
def create_user_with_access_key(user_name="test-user"):
    client = boto3.client("iam", region_name="us-east-1")
    client.create_user(UserName=user_name)
    return client.create_access_key(UserName=user_name)["AccessKey"]


@set_initial_no_auth_action_count(4)
@mock_dynamodb2
def test_get_object():
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
    print('Table created')
    dynamodb_table.put_item(Item={"id": "test-item-1"})
    print('Item created')

    with pytest.raises(ClientError) as e:
        dynamodb_table.get_item(Key={"id": "test-item-1"})

    print(e.value.response)
    assert e.value.response["Error"]["Code"] == "AccessDenied"
