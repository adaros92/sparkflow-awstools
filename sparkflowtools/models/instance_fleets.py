
class Fleet(object):

    name = "default"
    driver_instance_type_configs = [
        {
            'InstanceType': 'm5.xlarge',
            'WeightedCapacity': 1
        },
        {
            'InstanceType': 'm4.xlarge',
            'WeightedCapacity': 1
        },
    ]
    core_instance_type_configs = [
        {
            'InstanceType': 'm5.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'm4.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        }
    ]
    task_instance_type_configs = core_instance_type_configs

    def __init__(self,
                 core_instance_count, task_instance_count,
                 timeout_duration=25, timeout_action="SWITCH_TO_ON_DEMAND"):
        self.core_instance_count = core_instance_count
        self.task_instance_count = task_instance_count
        self.timeout_action = timeout_action
        self.timeout_duration = timeout_duration

    def get_fleet(self) -> list:
        """Retrieves the fleet configurations for driver, core, and task nodes to use in launching a new EMR
        cluster

        :return: a list containing individual instance configs such as number of instances and size
        """
        return [
            {
                "Name": "Driver Node",
                "InstanceFleetType": "MASTER",
                "TargetOnDemandCapacity": 1,
                "InstanceTypeConfigs": self.driver_instance_type_configs,
            },
            {
                "Name": "Core Nodes",
                "InstanceFleetType": "CORE",
                "TargetSpotCapacity": self.core_instance_count,
                "InstanceTypeConfigs": self.core_instance_type_configs,
                "LaunchSpecifications": {
                    "SpotSpecification": {
                        "TimeoutDurationMinutes": self.timeout_duration,
                        "TimeoutAction": self.timeout_action
                    }
                }
            },
            {
                "Name": "Task Nodes",
                "InstanceFleetType": "TASK",
                "TargetSpotCapacity": self.task_instance_count,
                "InstanceTypeConfigs": self.task_instance_type_configs,
                "LaunchSpecifications": {
                    "SpotSpecification": {
                        "TimeoutDurationMinutes": self.timeout_duration,
                        "TimeoutAction": self.timeout_action
                    }
                }
            }
        ]


class NanoFleet(Fleet):

    name = "nano"
    core_instance_type_configs = [
        {
            'InstanceType': 'm5.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'r4.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'r5.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'm4.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        }
    ]
    task_instance_type_configs = core_instance_type_configs

    def __init__(self, core_instance_count=2, task_instance_count=1):
        super().__init__(core_instance_count, task_instance_count)


class TinyFleet(Fleet):
    name = "tiny"
    core_instance_type_configs = [
        {
            'InstanceType': 'm5.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'r4.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'r5.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'm4.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        }
    ]
    task_instance_type_configs = core_instance_type_configs

    def __init__(self, core_instance_count=4, task_instance_count=1):
        super().__init__(core_instance_count, task_instance_count)


class SmallFleet(Fleet):
    name = "small"
    core_instance_type_configs = [
        {
            'InstanceType': 'm5.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'r4.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'r5.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'm4.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        }
    ]
    task_instance_type_configs = core_instance_type_configs

    def __init__(self, core_instance_count=6, task_instance_count=6):
        super().__init__(core_instance_count, task_instance_count)


class StandardFleet(Fleet):
    name = "standard"
    core_instance_type_configs = [
        {
            'InstanceType': 'm5.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'r4.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'r5.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'm4.xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        }
    ]
    task_instance_type_configs = core_instance_type_configs

    def __init__(self, core_instance_count=10, task_instance_count=10):
        super().__init__(core_instance_count, task_instance_count)


class MediumFleet(Fleet):
    name = "medium"
    core_instance_type_configs = [
        {
            'InstanceType': 'm5.2xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'r4.2xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'r5.2xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'm4.2xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        }
    ]
    task_instance_type_configs = core_instance_type_configs

    def __init__(self, core_instance_count=12, task_instance_count=12):
        super().__init__(core_instance_count, task_instance_count)


class LargeFleet(Fleet):
    name = "large"
    core_instance_type_configs = [
        {
            'InstanceType': 'm5.4xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'r4.4xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'r5.4xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'm4.4xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        }
    ]
    task_instance_type_configs = core_instance_type_configs

    def __init__(self, core_instance_count=24, task_instance_count=24):
        super().__init__(core_instance_count, task_instance_count)


class HugeFleet(Fleet):
    name = "huge"
    core_instance_type_configs = [
        {
            'InstanceType': 'm5.8xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'r4.8xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'r5.8xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        },
        {
            'InstanceType': 'm4.8xlarge',
            'WeightedCapacity': 1,
            'BidPriceAsPercentageOfOnDemandPrice': 100.0
        }
    ]
    task_instance_type_configs = core_instance_type_configs

    def __init__(self, core_instance_count=48, task_instance_count=48):
        super().__init__(core_instance_count, task_instance_count)


def get_fleet(fleet_name: str) -> Fleet:
    """Factory function for different fleets based on a given name

    :param fleet_name: a name of the fleet to create and retrieve
    :return: an instantiated fleet matching the given type name
    """
    fleet_types = [NanoFleet, TinyFleet, SmallFleet, StandardFleet, MediumFleet, LargeFleet, HugeFleet]
    fleet_map = {fleet.name: fleet() for fleet in fleet_types}
    if fleet_name not in fleet_map:
        raise ValueError("fleet type {0} is not one of the supported types: {1}".format(fleet_name, fleet_types))
    return fleet_map[fleet_name]
