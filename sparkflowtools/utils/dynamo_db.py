import boto3
import logging

from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential

from sparkflowtools.utils import aws, config


def get_dynamo_resource(resource: boto3.resource = None, credentials: dict = None) -> boto3.resource:
    """Retrieves a DynamoDB boto3 resource as either the one provided or a new instantiated one

    :param resource: an optional boto3 resource to use for the request
    :param credentials: an optional dictionary of credentials to assume
    :return: a new boto3 DynamoDB resource or the given one if one is provided
    """
    return aws.get_resource('dynamodb', resource=resource, credentials=credentials)


def get_dynamo_table(table_name: str, dynamodb_resource=None, table=None, credentials: dict = None):
    """Retrieves a DynamoDB boto3 table object or assumes the given one

    :param table_name: the name of the DynamoDB table to use
    :param dynamodb_resource: an optional boto3 dynamodb resource object to inject
    :param table: an optional boto3 dynamodb table object to inject
    :param credentials: an optional dictionary of credentials to assume
    :return: a boto3 DynamoDB table object and the DynamoDB resource used to create the table object
    """
    dynamodb_resource = get_dynamo_resource(dynamodb_resource, credentials=credentials)
    if not table:
        table = dynamodb_resource.Table(table_name)
    return table, dynamodb_resource


def _get_table_response_status(response: dict) -> dict:
    return response.get("ResponseMetadata", {}).get("HTTPStatusCode")


@retry(
    wait=wait_exponential(
        multiplier=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MULTIPLIER,
        min=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MIN,
        max=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MAX),
    stop=stop_after_attempt(config.AWSApiConfig.RETRY_MAX)
)
def get_item_from_dynamodb_table(table_name: str, key: dict, dynamodb_resource: boto3.resource = None) -> tuple:
    table, dynamodb_resource = get_dynamo_table(table_name, dynamodb_resource)
    try:
        response = table.get_item(Key=key)
        response_status = _get_table_response_status(response)
    except ClientError as e:
        logging.critical(e.response["Error"]["Message"])
        raise
    else:
        return response.get("Item"), response_status


@retry(
    wait=wait_exponential(
        multiplier=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MULTIPLIER,
        min=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MIN,
        max=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MAX),
    stop=stop_after_attempt(config.AWSApiConfig.RETRY_MAX)
)
def write_item_to_dynamodb(table_name: str, item_dictionary: dict, dynamodb_resource: boto3.resource = None) -> dict:
    table, dynamodb_resource = get_dynamo_table(table_name, dynamodb_resource)
    try:
        response = table.put_item(Item=item_dictionary)
        response_status = _get_table_response_status(response)
    except ClientError as e:
        logging.critical(e.response["Error"]["Message"])
        raise
    else:
        return response_status
