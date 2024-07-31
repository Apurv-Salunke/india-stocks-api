from core.brokers.base.errors import (
    InputError,
    ResponseError,
    TokenDownloadError,
    RequestTimeout,
    NetworkError,
    BrokerError,
)


def test_input_error():
    error_message = "Invalid input format."
    exception = InputError(error_message)
    assert isinstance(exception, InputError)
    assert str(exception) == error_message


def test_response_error():
    error_message = "Invalid response from service."
    exception = ResponseError(error_message)
    assert isinstance(exception, ResponseError)
    assert str(exception) == error_message


def test_token_download_error():
    error_message = "Failed to download token."
    exception = TokenDownloadError(error_message)
    assert isinstance(exception, TokenDownloadError)
    assert str(exception) == error_message


def test_request_timeout():
    error_message = "Request timed out."
    exception = RequestTimeout(error_message)
    assert isinstance(exception, RequestTimeout)
    assert str(exception) == error_message


def test_network_error():
    error_message = "Network error occurred."
    exception = NetworkError(error_message)
    assert isinstance(exception, NetworkError)
    assert str(exception) == error_message


def test_broker_error():
    error_message = "Broker-specific error occurred."
    exception = BrokerError(error_message)
    assert isinstance(exception, BrokerError)
    assert str(exception) == error_message
