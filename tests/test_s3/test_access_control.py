import boto3
import pytest

from moto import mock_s3, mock_iam
from botocore.exceptions import ClientError


@mock_s3
@mock_iam
def test_get_object():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(Bucket="test-bucket", Key="test-key", Body="test-value")

    with pytest.raises(ClientError) as e:
        s3.get_object(
            Bucket="test-bucket",
            Key="test-key"
        )

    assert e.value.response["Error"]["Code"] == "AccessDenied"
