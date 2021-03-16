import boto3
import logging

from datetime import datetime
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
def submit_step(cluster_id: str, steps: list, client: boto3.client) -> dict:
    """Submits a list of steps on the EMR cluster by the given ID

    If a client is not provided it will just assume a default EMR client with the current session's
    credentials

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.add_job_flow_steps
    
    :param cluster_id: a unique ID of the EMR cluster to submit steps to
    :param steps: a list of Steps as defined in link
    :param client: an optional boto3 client to use for the request
    :return: the response from add_job_flow_steps containing the resulting step_id after submitting to EMR
    """
    client = get_emr_client(client=client)
    try:
        response = client.add_job_flow_steps(JobFlowId=cluster_id, Steps=steps)
    except Exception as e:
        logging.warning("utils.emr.submit_step unable to submit job run on EMR cluster {0}".format(cluster_id))
        logging.exception(e)
        raise
    return response


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
    """
    client = get_emr_client(client=client)
    try:
        client.terminate_job_flows(JobFlowIds=cluster_ids)
    except Exception as e:
        logging.warning("utils.emr.terminate_clusters unable to terminate cluster")
        logging.exception(e)
        raise


def _get_step_status(step: dict) -> dict:
    """Returns a flattened dictionary of step status information from the list_steps API endpoint

    :param step the response from the list_steps API endpoint for an individual step to retrieve data from
    :returns a dictionary containing relevant step attributes
    """
    return {
        "step_id": step["Id"],
        "name": step["Name"],
        "status": step["Status"]["State"]
    }


@retry(
    wait=wait_exponential(
        multiplier=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MULTIPLIER,
        min=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MIN,
        max=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MAX),
    stop=stop_after_attempt(config.AWSApiConfig.RETRY_MAX)
)
def get_step_statuses(cluster_id: str, states: list = None, step_ids: list = None, client: boto3.client = None):
    """Retrieves cluster statuses for all steps in the cluster the caller has visibility to matching the given list of
     states or for the given list  of step_ids in that cluster

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.list_steps

    :param cluster_id the ID of the cluster running the steps to get statuses from
    :param states the states of steps to get
        ('PENDING'|'CANCEL_PENDING'|'RUNNING'|'COMPLETED'|'CANCELLED'|'FAILED'|'INTERRUPTED')
    :param step_ids an optional list of step IDs to filter results to
    :param client an optional EMR boto3 client to use for the request
    :return a list of statuses by step_id
    """
    expected_states = ["PENDING", "CANCEL_PENDING", "RUNNING", "COMPLETED", "CANCELLED", "FAILED", "INTERRUPTED"]
    if states:
        for state in states:
            if state not in expected_states:
                raise ValueError("in utils.emr.get_step_statuses the given state of {0} is not one of {1}".format(
                    state, expected_states))
    else:
        states = expected_states
    client = get_emr_client(client=client)
    try:
        response = client.list_steps(ClusterId=cluster_id, StepStates=states)
        marker = response.get("Marker", True)
        statuses = []
        while marker:
            marker = response.get("Marker", False)
            steps = response["Steps"]
            for step in steps:
                status = _get_step_status(step)
                # If a list of specific cluster_ids are provided then only retrieve the status for those
                if step_ids:
                    if status["step_id"] in step_ids:
                        statuses.append(status)
                # Otherwise keep all statuses
                else:
                    statuses.append(status)
            if marker:
                response = client.list_steps(ClusterId=cluster_id, StepStates=states, Marker=marker)
        logging.info("retrieved {0} steps for cluster {1}".format(len(statuses), cluster_id))
        return statuses
    except Exception as e:
        logging.warning("utils.emr.get_step_statuses could not list steps")
        logging.exception(e)
        raise


def _get_cluster_status(cluster: dict) -> dict:
    """Returns a flattened dictionary of cluster status information from the list_clusters API endpoint

    :param cluster the response from the list_clusters API endpoint for an individual cluster to retrieve data from
    :returns a dictionary containing relevant cluster attributes
    """
    return {
        "cluster_id": cluster["Id"],
        "cluster_name": cluster["Name"],
        "status": cluster["Status"]["State"],
        "state_change_reason": cluster["Status"].get("StateChangeReason", {}).get("Code", ""),
        "creation_datetime": cluster["Status"].get("Timeline", {}).get(
            "CreationDateTime", datetime(1900, 12, 1, 1, 0, 0)).strftime("%Y-%m-%dT%H:%M"),
        "end_datetime": cluster["Status"].get("Timeline", {}).get(
            "EndDateTime", datetime(2050, 12, 1, 1, 0, 0)).strftime("%Y-%m-%dT%H:%M"),
        "cluster_arn": cluster["ClusterArn"],
        "instance_hours": cluster.get("NormalizedInstanceHours", 0)
    }


@retry(
    wait=wait_exponential(
        multiplier=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MULTIPLIER,
        min=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MIN,
        max=config.AWSApiConfig.EXPONENTIAL_BACKOFF_MAX),
    stop=stop_after_attempt(config.AWSApiConfig.RETRY_MAX)
)
def get_cluster_statuses(
        states: list, created_after: datetime = None, cluster_ids: list = None, client: boto3.client = None) -> list:
    """Retrieves cluster statuses for all clusters the caller has visibility to or for the given list of cluster_ids

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.list_clusters

    :param states the states of clusters to get (STARTING, BOOTSTRAPPING, RUNNING, WAITING, TERMINATING, TERMINATED)
    :param created_after an optional datetime date to filter the request by
    :param cluster_ids a list of cluster IDs to filter the status retrieval to
    :param client an optional EMR boto3 client to use for the request
    :return a list of statuses by cluster_id
    """
    expected_states = {"STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING", "TERMINATING", "TERMINATED"}
    for state in states:
        if state not in expected_states:
            raise ValueError("in utils.emr.get_cluster_statuses the given state of {0} is not one of {1}".format(
                state, expected_states))
    client = get_emr_client(client=client)
    if not created_after:
        created_after = datetime(1900, 1, 1)
    try:
        response = client.list_clusters(ClusterStates=states, CreatedAfter=created_after)
        marker = response.get("Marker", True)
        statuses = []
        while marker:
            marker = response.get("Marker", False)
            clusters = response["Clusters"]
            for cluster in clusters:
                status = _get_cluster_status(cluster)
                cluster_id = status["cluster_id"]
                active_steps_on_cluster = get_step_statuses(
                    cluster_id,
                    states=["PENDING", "CANCEL_PENDING", "RUNNING"]
                )
                status["number_of_active_steps"] = len(active_steps_on_cluster)
                # If a list of specific cluster_ids are provided then only retrieve the status for those
                if cluster_ids:
                    if cluster_id in cluster_ids:
                        statuses.append(status)
                # Otherwise keep all statuses
                else:
                    statuses.append(status)
            if marker:
                response = client.list_clusters(ClusterStates=states, Marker=marker, CreatedAfter=created_after)
        return statuses
    except Exception as e:
        logging.warning("utils.emr.get_cluster_statuses could not list clusters")
        logging.exception(e)
        raise
