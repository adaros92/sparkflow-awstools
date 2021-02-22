import boto3


def instantiate_boto3_object(service_name, service_class, injected_object = None, credentials: dict = None):
    # Assume boto3 object if provided with one
    if injected_object:
        return injected_object
    # otherwise instantiate a new one
    if credentials:
        assert "aws_access_key_id" in credentials and "aws_secret_access_key" in credentials
        return service_class(service_name, **credentials)
    return service_class(service_name)


def get_client(service_name: str, client: boto3.client = None, credentials: dict = None) -> boto3.client:
    """Retrieves a boto3 client for the given service

    :param service_name: the name of the service to retrieve a client for
    :param client: an optional instantiated client to inject
    :param credentials: an optional dictionary of AWS credentials to use; otherwise assume current session
        creds
    :return: a boto3 client corresponding to the service required
    """
    return instantiate_boto3_object(service_name, boto3.client, client, credentials)


def get_resource(service_name: str, resource: boto3.resource = None, credentials: dict = None) -> boto3.resource:
    """Retrieves a boto3 resource for the given service

    :param service_name: the name of the service to retrieve a resource for
    :param resource: an optional instantiated resource to inject
    :param credentials: an optional dictionary of AWS credentials to use; otherwise assume current session creds
    :return: a boto3 resource corresponding o the service required
    """
    return instantiate_boto3_object(service_name, boto3.resource, resource, credentials)
