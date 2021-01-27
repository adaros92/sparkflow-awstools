import pytest

from sparkflowtools.models import instance_fleets


def test_instance_fleet_configs():
    """ Tests instance_fleet EMR fleet types and factory function to retrieves instantiated EMR fleet objects """
    expected_fleet_names = ["nano", "tiny", "small", "standard", "medium", "large", "huge"]
    for fleet_name in expected_fleet_names:
        fleet_object = instance_fleets.get_fleet(fleet_name)
        assert fleet_object.name == fleet_name
        fleet_config = fleet_object.get_fleet()
        # There must be driver, core, and task sub-configs in fleet_config
        assert len(fleet_config) == 3
    with pytest.raises(ValueError):
        instance_fleets.get_fleet("some_random_fleet_name")
