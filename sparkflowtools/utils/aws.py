import boto3


def get_client(service_name: str, client: boto3.client = None, credentials: dict = None) -> boto3.client:
    """ Retrieves a boto3 client for the given service

    :param service_name: the name of the service to retrieve a client for
    :param client: an optional instantiated client to inject
    :param credentials: an optional dictionary of AWS credentials to use; otherwise assume current session
        creds
    :return: a boto3 client corresponding to the service required
    """
    # Assume client if provided with one
    if client:
        return client
    # Otherwise instantiate a new one
    if credentials:
        assert "aws_access_key_id" in credentials and "aws_secret_access_key" in credentials
        return boto3.client(service_name, **credentials)
    return boto3.client(service_name)
