import boto3
import json
import logging

from tenacity import retry, stop_after_attempt, wait_exponential

from sparkflowtools.utils import aws, config


def get_lambda_client(client: boto3.client = None, credentials: dict = None) -> boto3.client:
    """Retrieves a Lambda boto3 client as either the one provided or a new instantiated one

    :param client: an optional boto3 client to use for the request
    :param credentials: an optional dictionary of credentials to assume
    :return: a new boto3 Lambda client or the given one if one is provided
    """
    return aws.get_client('lambda', client=client, credentials=credentials)


@retry(
    wait=wait_exponential(
        multiplier=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MULTIPLIER,
        min=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MIN,
        max=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MAX),
    stop=stop_after_attempt(config.AWSApiConfig.RETRY_MAX)
)
def invoke_function(function_name: str, payload: dict, client: boto3.client = None) -> dict:
    """Invokes a Lambda function identified by the given name

    :param function_name the name of the function to invoke
    :param payload a payload to give the Lambda
    :param client an optional boto3 Lambda client to use for the request
    :returns the payload from the request to the invoke boto3 Lambda endpoint
    """
    client = get_lambda_client(client)
    try:
        response = client.invoke(
            FunctionName=function_name,
            LogType="None",
            Payload=json.dumps(payload),
            InvocationType="Event"
        )
    except Exception as e:
        logging.warning("utils.lambda.invoke_function unable to invoke {0}".format(function_name))
        logging.exception(e)
        raise
    return response["Payload"]
