import pytest

from sparkflowtools.utils import aws_lambda

mock_client = pytest.mock_lambda_client


def test_invoke_function():
    function_name = "some_function_arn"
    payload = {}
    response = aws_lambda.invoke_function(function_name, payload, mock_client)
    assert response == mock_client.invoke()["Payload"]
