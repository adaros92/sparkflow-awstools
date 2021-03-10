import boto3
import logging
import time

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
    """Helper function to retrieve the status from a DynamoDB API request object

    :param response the response from a DynamoDB API request
    :returns the status in the response provided by the API
    """
    return response.get("ResponseMetadata", {}).get("HTTPStatusCode")


@retry(
    wait=wait_exponential(
        multiplier=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MULTIPLIER,
        min=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MIN,
        max=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MAX),
    stop=stop_after_attempt(config.AWSApiConfig.RETRY_MAX)
)
def get_item_from_dynamodb_table(
        table_name: str, key: dict,
        dynamodb_resource: boto3.resource = None, dynamo_table_object=None) -> tuple:
    """Retrieves a single item from the DynamoDB table matching the given key

    :param table_name the name of the DynamoDB table to retrieve the item from
    :param key the key that identifies the record to retrieve in DynamoDB
    :param dynamodb_resource an optional DynamoDB table resource to use for the request
    :param dynamo_table_object an optional DynamoDB table object to use for the request
    :returns the item data and the response from the API request
    """
    table, dynamodb_resource = get_dynamo_table(table_name, dynamodb_resource, dynamo_table_object)
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
def write_item_to_dynamodb(
        table_name: str, item_dictionary: dict,
        dynamodb_resource: boto3.resource = None, dynamo_table_object=None) -> dict:
    """Inserts a single item into a DynamoDB table

    :param table_name the name of the DynamoDB table to insert the item to
    :param item_dictionary the item data to insert
    :param dynamodb_resource an optional DynamoDB table resource to use for the request
    :param dynamo_table_object an optional DynamoDB table object to use for the request
    :returns the response from the API request
    """
    table, dynamodb_resource = get_dynamo_table(table_name, dynamodb_resource, table=dynamo_table_object)
    try:
        response = table.put_item(Item=item_dictionary)
        response_status = _get_table_response_status(response)
    except ClientError as e:
        logging.critical(e.response["Error"]["Message"])
        raise
    else:
        return response_status


def _query_dynamo(
        table, index_name: str, expression: str, expression_attribute_values: dict, exclusive_start=None) -> tuple:
    """Performs a query on a Dynamo table with a given index and pagination token

    :param table the Dynamo table resourse to use for the query
    :param index_name the name of the index to use in the query
    :param expression a DynamoDb boto3 query expression
    :param expression_attribute_values the values to substitute in the expression
    :param exclusive_start the pagination token to use in the request
    :returns the items retrieved from dynamo and the response object from the API request
    """
    inputs = {
        "IndexName": index_name,
        "KeyConditionExpression": "{0}".format(expression),
        "ExpressionAttributeValues": expression_attribute_values
    }
    if exclusive_start:
        inputs["ExclusiveStartKey"] = exclusive_start
    response = table.query(**inputs)
    return response.get("Items"), response


@retry(
    wait=wait_exponential(
        multiplier=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MULTIPLIER,
        min=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MIN,
        max=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MAX),
    stop=stop_after_attempt(config.AWSApiConfig.RETRY_MAX)
)
def get_items_with_index(
        table_name: str, index_name: str, expression: str,
        expression_attribute_values: dict, dynamodb_resource: boto3.resource = None,
        dynamo_table_object=None) -> tuple:
    """Retrieves all items in a DynamoDB table using the given index and matching the given expression

    :param table_name the name of the DynamoDB table to query
    :param index_name the name of the index to use in the query
    :param expression a DynamoDb boto3 query expression
    :param expression_attribute_values the values to substitute in the expression
    :param dynamodb_resource an optional DynamoDB table resource to use for the request
    :param dynamo_table_object an optional DynamoDB table object to use for the request
    """
    table, dynamodb_resource = get_dynamo_table(table_name, dynamodb_resource, table=dynamo_table_object)
    items = []
    while True:
        if not table.global_secondary_indexes or table.global_secondary_indexes[0]["IndexStatus"] != "ACTIVE":
            logging.info("waiting for {0}'s index to populate".format(table_name))
            time.sleep(10)
            table.reload()
        else:
            break
    try:
        items_received, response = _query_dynamo(table, index_name, expression, expression_attribute_values)
        items.extend(items_received)
        while "LastEvaluatedKey" in response:
            items_received, response = _query_dynamo(
                table, index_name, expression, expression_attribute_values,
                exclusive_start=response["LastEvaluatedKey"])
            items.extend(items_received)
        response_status = _get_table_response_status(response)
    except ClientError as e:
        logging.critical(e.response["Error"]["Message"])
        raise
    else:
        return items, response_status


@retry(
    wait=wait_exponential(
        multiplier=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MULTIPLIER,
        min=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MIN,
        max=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MAX),
    stop=stop_after_attempt(config.AWSApiConfig.RETRY_MAX)
)
def delete_item_by_key(
        table_name: str, key: dict, dynamodb_resource: boto3.resource = None, dynamo_table_object=None):
    """Delete a single item identified by the given key from the Dynamo table with the given name

    :param table_name the name of the table to delete the item from
    :param key a dictionary containing both the partition and sort values that uniquely identify a record to delete
    :param dynamodb_resource an optional DynamoDB table resource to use for the request
    :param dynamo_table_object an optional DynamoDB table object to use for the request
    """
    table, dynamodb_resource = get_dynamo_table(table_name, dynamodb_resource, table=dynamo_table_object)
    try:
        table.delete_item(Key=key)
    except Exception as e:
        logging.warning("in sparkflowtools.utils.dynamo_db could not delete item from table with key {0}".format(key))
        logging.exception(e)
        raise
