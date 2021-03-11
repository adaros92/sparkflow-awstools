import boto3
import datetime
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


def test_get_cluster_status():
    sample_response = {
        'Id': 'j-30MCDMWFHSW7G',
        'Name': 'TransferA pool | SparkFlow-cluster-1-2021-03-10T065249',
        'Status': {'State': 'TERMINATED', 'StateChangeReason':
            {'Code': 'USER_REQUEST', 'Message': 'Terminated by user request'},
                   'Timeline': {'CreationDateTime': datetime.datetime(2021, 3, 9, 22, 52, 49, 967000),
                                'EndDateTime': datetime.datetime(2021, 3, 9, 22, 54, 28, 20000)}},
        'NormalizedInstanceHours': 0,
        'ClusterArn': 'arn:aws:elasticmapreduce:us-east-1:146066720211:cluster/j-30MCDMWFHSW7G'}
    expected_status = {
        "cluster_id": 'j-30MCDMWFHSW7G',
        "cluster_name": 'TransferA pool | SparkFlow-cluster-1-2021-03-10T065249',
        "status": 'TERMINATED',
        "state_change_reason": 'USER_REQUEST',
        "creation_datetime": '2021-03-09T22:52',
        "end_datetime": '2021-03-09T22:54',
        "cluster_arn": 'arn:aws:elasticmapreduce:us-east-1:146066720211:cluster/j-30MCDMWFHSW7G',
        "instance_hours": 0}
    assert emr._get_cluster_status(sample_response) == expected_status
