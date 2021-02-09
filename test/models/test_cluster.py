import pytest

from sparkflowtools.models import cluster


def test_creation_from_existing_cluster():
    cluster_builder = cluster.EmrBuilder()
    new_cluster = cluster_builder.build_from_existing_cluster("some_cluster_id", client=pytest.mock_emr_client)

