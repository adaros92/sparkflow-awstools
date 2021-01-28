import boto3
import logging

from tenacity import retry, stop_after_attempt, wait_exponential

from sparkflowtools.utils import aws, config


def get_emr_client(client: boto3.client = None, credentials: dict = None) -> boto3.client:
    """Retrieves an EMR boto3 client as either the one provided or a new instantiated one

    :param client: an optional boto3 client to use for the request
    :param credentials: an optional dictionary of credentials to assume
    :return: a new boto3 EMR client or the given one if one is provided
    """
    return aws.get_client('emr', client=client, credentials=credentials)


@retry(
    wait=wait_exponential(
        multiplier=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MULTIPLIER,
        min=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MIN,
        max=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MAX),
    stop=stop_after_attempt(config.AWSApiConfig.RETRY_MAX)
)
def get_step_status(cluster_id: str, step_id: str, client: boto3.client = None) -> dict:
    """Retrieves the status of a job step on EMR

    If a client is not provided it will just assume a default EMR client with the current session's
    credentials

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_step

    :param cluster_id: the ID of the cluster the job is in
    :param step_id: the ID of the step running on the cluster to poll
    :param client: an optional boto3 client to use for the request
    :return: a step status dictionary as documented in describe_step response syntax
    """
    client = get_emr_client(client=client)
    try:
        response = client.describe_step(ClusterId=cluster_id, StepId=step_id)
        step_status = response['Step']['Status']
    except Exception as e:
        logging.warning("utils.emr.get_step_status unable to obtain EMR step status")
        logging.exception(e)
        raise
    return step_status


@retry(
    wait=wait_exponential(
        multiplier=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MULTIPLIER,
        min=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MIN,
        max=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MAX),
    stop=stop_after_attempt(config.AWSApiConfig.RETRY_MAX)
)
def create_cluster(job_flow: dict, client: boto3.client = None) -> dict:
    """Creates an EMR cluster with the given job flow parameters as documented below

    If a client is not provided it will just assume a default EMR client with the current session's
    credentials

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.run_job_flow

    :param job_flow: a dictionary of job flow parameters as documented in link
    :param client: an optional EMR boto3 client to use for the request
    :return: a response dictionary
    """
    client = get_emr_client(client=client)
    response = {}
    try:
        tmp_response = client.run_job_flow(**job_flow)
        response["cluster_id"] = tmp_response["JobFlowId"]
        response["cluster_arn"] = tmp_response["ClusterArn"]
    except Exception as e:
        logging.warning("utils.emr.create_cluster unable to submit job flow")
        logging.exception(e)
        raise
    return response


@retry(
    wait=wait_exponential(
        multiplier=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MULTIPLIER,
        min=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MIN,
        max=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MAX),
    stop=stop_after_attempt(config.AWSApiConfig.RETRY_MAX)
)
def get_cluster_info(cluster_id: str, client: boto3.client = None) -> dict:
    """Retrieves information from a cluster with the given cluster_id

    If a client is not provided it will just assume a default EMR client with the current session's
    credentials

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_cluster

    :param cluster_id: the ID of a cluster to retrieve the information form
    :param client: an optional EMR boto3 client to use for the request
    :return: a dictionary of EMR parameters and values for the given cluster_id
    """
    client = get_emr_client(client=client)
    try:
        response = client.describe_cluster(ClusterId=cluster_id)["Cluster"]
    except Exception as e:
        logging.warning("utils.emr.get_cluster_info unable to retrieve cluster data")
        logging.exception(e)
        raise
    return response


@retry(
    wait=wait_exponential(
        multiplier=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MULTIPLIER,
        min=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MIN,
        max=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MAX),
    stop=stop_after_attempt(config.AWSApiConfig.RETRY_MAX)
)
def terminate_clusters(cluster_ids: list, client: boto3.client = None) -> None:
    """Shuts down the clusters with the ids contained in the given list

    If a client is not provided it will just assume a default EMR client with the current session's
    credentials

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.terminate_job_flows

    :param cluster_ids: a list of cluster IDs of clusters to terminate
    :param client: an optional EMR boto3 client to use for the request
    :return:
    """
    client = get_emr_client(client=client)
    try:
        client.terminate_job_flows(JobFlowIds=cluster_ids)
    except Exception as e:
        logging.warning("utils.emr.terminate_clusters unable to terminate cluster")
        logging.exception(e)
        raise
