import pytest

from sparkflowtools.models import step

mock_client = pytest.mock_emr_client


def test_construct_payload():
    step_name = "some_step"
    emr_step = step.EmrStep("some_step")
    emr_step.spark_args = {"--executor-memory": "6G", "--num-executors": "1"}
    emr_step.job_args = {"-some_argument": "some_value"}
    emr_step.job_class = "some_class"
    emr_step.job_jar = "some_jar"
    received_payload = emr_step.payload
    expected_payload = {
            'Name': step_name,
            'ActionOnFailure': "TERMINATE_CLUSTER",
            'HadoopJarStep': {
                'Jar': "command-runner.jar",
                'Args': [
                    'spark-submit', '--deploy-mode', 'cluster',
                    "--executor-memory", "6G", "--num-executors", "1",
                    "--class", "some_class", "some_jar",
                    "-some_argument", "some_value"
                ]
            }
        }
    print("received_payload", received_payload)
    print("expected_payload", expected_payload)
    assert received_payload == expected_payload


def test_assign_to_cluster():
    name = "some_step_name"
    emr_step = step.EmrStep(name)
    cluster_id = "some_cluster"
    step_id = "some_step"
    step_returned = emr_step.assign_to_cluster(cluster_id, step_id, client=mock_client)
    assert step_returned.cluster_id == cluster_id and step_returned.step_id == step_id and step_returned.name == name


def test_fetch_status():
    name = "some_step_name"
    emr_step = step.EmrStep(name)
    assert emr_step.fetch_status(mock_client) == {}
    emr_step.step_id = "some_step_id"
    emr_step.cluster_id = "some_cluster_id"
    status_response = emr_step.fetch_status(mock_client)
    assert status_response == mock_client.describe_step()["Step"]["Status"]
