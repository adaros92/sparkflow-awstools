import boto3
import pytest

from sparkflowtools.utils import aws


def test_get_client():
    """ Tests aws.get_client utility function """
    boto3_clients = ['s3', 'emr', 'glue', 'sqs', 'sns', 'athena']
    client_name = ['S3', 'EMR', 'Glue', 'SQS', 'SNS', 'Athena']
    # Ensure that the expected clients are returned
    for boto3_client, client_name in zip(boto3_clients, client_name):
        expected_client = "<class 'botocore.client.{0}'>".format(client_name)
        # Instantiate new client
        retrieved_client = aws.get_client(boto3_client)
        client_type = type(retrieved_client)
        assert str(client_type) == expected_client
        # Inject existing client
        client = boto3.client(boto3_client)
        retrieved_client = aws.get_client(boto3_client, client=client)
        client_type = type(retrieved_client)
        assert str(client_type) == str(type(client))
        # Credentials without required keys should throw assertion errors
        bogus_credentials = {"some_unexpected_key": -1}
        with pytest.raises(AssertionError):
            aws.get_client(boto3_client, credentials=bogus_credentials)
        # Otherwise the client should be instantiated normally
        bogus_credentials = {"aws_access_key_id": -1, "aws_secret_access_key": -1}
        retrieved_client = aws.get_client(boto3_client, credentials=bogus_credentials)
        client_type = type(retrieved_client)
        assert str(client_type) == expected_client


def test_get_resource():
    """ Tests aws.get_resouce utility function """
    boto3_resources = ['dynamodb', 'sqs']
    resource_name = ['dynamodb', 'sqs']
    for resource, expected in zip(boto3_resources, resource_name):
        retrieved_resource = aws.get_resource(resource)
        resource_type = retrieved_resource.meta.service_name
        assert resource == resource_type
