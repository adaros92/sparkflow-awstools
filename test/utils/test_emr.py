import boto3
import pytest

from sparkflowtools.utils import emr

mock_client = pytest.mock_emr_client


def test_get_emr_client():
    assert str(type(emr.get_emr_client())) == str(type(boto3.client("emr")))


def test_get_step_status():
    cluster_id = "some_cluster"
    step_id = "some_step_id"
    response = emr.get_step_status(cluster_id, step_id, mock_client)
    assert response == mock_client.describe_step()['Step']['Status']


def test_submit_step():
    cluster_id = "some_cluster"
    steps = []
    response = emr.submit_step(cluster_id, steps, mock_client)
    assert response == mock_client.add_job_flow_steps()


def test_create_cluster():
    job_flow = {}
    expected_response = mock_client.run_job_flow()
    response = emr.create_cluster(job_flow, mock_client)
    assert response["cluster_id"] == expected_response["JobFlowId"] \
           and response["cluster_arn"] == expected_response["ClusterArn"]


def test_get_cluster_info():
    cluster_id = "some_cluster"
    response = emr.get_cluster_info(cluster_id, mock_client)
    assert response == mock_client.describe_cluster()["Cluster"]


def test_terminate_clusters():
    clusters = ['some_cluster']
    emr.terminate_clusters(clusters, mock_client)

