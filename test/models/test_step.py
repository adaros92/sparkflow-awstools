import pytest

from sparkflowtools.models import step

mock_client = pytest.mock_emr_client


def test_construct_payload():
    emr_step = step.EmrStep("some_step")
    expected_payload = {
        'Name': 'string',
        'ActionOnFailure': "TERMINATE_CLUSTER",
        'HadoopJarStep': {
            'Jar': "some_path",
            'MainClass': "some_class",
            'Args': []
        }
    }
    emr_step.action_on_failure = expected_payload["ActionOnFailure"]
    emr_step.script_path = expected_payload["HadoopJarStep"]["Jar"]
    emr_step.main_class = expected_payload["HadoopJarStep"]["MainClass"]
    emr_step.args = expected_payload["HadoopJarStep"]["Args"]
    assert emr_step._construct_step_payload() == expected_payload and emr_step.payload == expected_payload


def test_assign_to_cluster():
    name = "some_step_name"
    emr_step = step.EmrStep(name)
    cluster_id = "some_cluster"
    step_id = "some_step"
    step_returned = emr_step.assign_to_cluster(cluster_id, step_id, client=mock_client)
    assert step_returned.cluster_id == cluster_id and step_returned.step_id == step_id and step_returned.name == name


def test_add_script():
    name = "some_step_name"
    emr_step = step.EmrStep(name)
    script_path = "some_path"
    main_class = "some_class"
    step_returned = emr_step.add_script(script_path, main_class)
    assert step_returned.script_path == script_path and step_returned.main_class == main_class \
           and step_returned.name == name


def test_fetch_status():
    name = "some_step_name"
    emr_step = step.EmrStep(name)
    assert emr_step.fetch_status(mock_client) == {}
    emr_step.step_id = "some_step_id"
    emr_step.cluster_id = "some_cluster_id"
    status_response = emr_step.fetch_status(mock_client)
    assert status_response == mock_client.describe_step()["Step"]["Status"]
