import json
import pkg_resources


class AWSApiConfig:
    RETRY_MAX = 5
    EXPONENTIAL_BACKOFF_MULTIPLIER = 1
    EXPONENTIAL_BACKOFF_MIN = 4
    EXPONENTIAL_BACKOFF_MAX = 10


def get_sample_emr_config() -> dict:
    """Retrives a sample EMR config object as expected by sparkflowtools to create EMR clusters

    :returns the dictionary parsed from the config JSON
    """
    filepath = pkg_resources.resource_filename('sparkflowtools', 'configs/sample_creation_config.json')
    with open(filepath, "r") as f:
        config_object = json.load(f)
    return config_object
