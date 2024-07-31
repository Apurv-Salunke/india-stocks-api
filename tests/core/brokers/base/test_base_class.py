import random
from ssl import SSLError
import pytest
from unittest.mock import patch, Mock
from requests.models import Response
from requests.exceptions import (
    Timeout,
    ConnectionError as RequestsConnectionError,
    HTTPError,
    RequestException,
    TooManyRedirects,
)

from core.brokers.base import Broker
from core.brokers.base import RequestTimeout, BrokerError, NetworkError


@pytest.fixture
def mock_session():
    """
    Fixture to mock the _session attribute of the Broker class.

    This fixture patches the Broker class's _session attribute to use a mock object,
    allowing for the simulation of various behaviors and exceptions in the tests.
    """
    with patch.object(Broker, "_session", new_callable=Mock) as mock:
        yield mock


def test_fetch_success(mock_session):
    """
    Test the successful execution of the fetch method.

    This test sets up a mock response with a 200 status code and a JSON body. It verifies
    that the fetch method correctly returns this response and that the response's status
    code and text match the expected values.
    """
    mock_response = Mock(spec=Response)
    mock_response.status_code = 200
    mock_response.text = '{"key": "value"}'
    mock_session.request.return_value = mock_response

    broker = Broker()
    response = broker.fetch(method="GET", url="https://example.com")
    assert response.status_code == 200
    assert response.text == '{"key": "value"}'


def test_fetch_timeout(mock_session):
    """
    Test the fetch method's behavior when a Timeout exception is raised.

    This test simulates a timeout error by setting the mock session to raise a Timeout
    exception. It verifies that the fetch method raises a RequestTimeout exception in
    response to this error.
    """
    mock_session.request.side_effect = Timeout

    broker = Broker()
    with pytest.raises(RequestTimeout):
        broker.fetch(method="GET", url="https://example.com")


def test_fetch_connection_error(mock_session):
    """
    Test the fetch method's behavior when a ConnectionError is raised.

    This test simulates a generic connection error by setting the mock session to raise
    a RequestsConnectionError. It verifies that the fetch method raises a NetworkError
    in response to this error.
    """
    mock_session.request.side_effect = RequestsConnectionError

    broker = Broker()
    with pytest.raises(NetworkError):
        broker.fetch(method="GET", url="https://example.com")


def test_fetch_connection_read_timed_out_error(mock_session):
    """
    Test the fetch method's behavior when a ConnectionError with 'Read timed out' is raised.

    This test simulates a connection read timeout error by setting the mock session to
    raise a RequestsConnectionError with the message "Read timed out". It verifies that
    the fetch method raises a RequestTimeout exception in response to this specific error.
    """
    mock_session.request.side_effect = RequestsConnectionError("Read timed out")

    broker = Broker()
    with pytest.raises(RequestTimeout):
        broker.fetch(method="GET", url="https://example.com")


def test_fetch_connection_reset_error(mock_session):
    """
    Test the fetch method's behavior when a ConnectionResetError is raised.

    This test simulates a connection reset error by setting the mock session to raise a
    ConnectionResetError. It verifies that the fetch method raises a NetworkError in
    response to this error.
    """
    mock_session.request.side_effect = ConnectionResetError

    broker = Broker()
    with pytest.raises(NetworkError):
        broker.fetch(method="GET", url="https://example.com")


def test_fetch_too_many_redirects(mock_session):
    """
    Test the fetch method's behavior when a TooManyRedirects exception is raised.

    This test simulates an error due to too many redirects by setting the mock session
    to raise a TooManyRedirects exception. It verifies that the fetch method raises a
    BrokerError in response to this error.
    """
    mock_session.request.side_effect = TooManyRedirects

    broker = Broker()
    with pytest.raises(BrokerError):
        broker.fetch(method="GET", url="https://example.com")


def test_fetch_ssl_error(mock_session):
    """
    Test the fetch method's behavior when an SSLError is raised.

    This test simulates an SSL error by setting the mock session to raise an SSLError.
    It verifies that the fetch method raises a BrokerError in response to this error.
    """
    mock_session.request.side_effect = SSLError

    broker = Broker()
    with pytest.raises(BrokerError):
        broker.fetch(method="GET", url="https://example.com")


def test_fetch_http_error(mock_session):
    """
    Test the fetch method's behavior when an HTTPError is raised.

    This test simulates an HTTP error with a 500 status code by setting the mock session
    to raise an HTTPError. It verifies that the fetch method raises a BrokerError in
    response to this error.
    """
    mock_session.request.side_effect = HTTPError("500 Internal Server Error")

    broker = Broker()
    with pytest.raises(BrokerError):
        broker.fetch(method="GET", url="https://example.com")


def test_fetch_request_exception(mock_session):
    """
    Test the fetch method's behavior when a generic RequestException is raised.

    This test simulates a generic request exception by setting the mock session to raise
    a RequestException with a random error message from a predefined list. It verifies
    that the fetch method raises a NetworkError in response to this error.
    """
    error = random.choice(["ECONNRESET", "Connection aborted.", "Connection broken:"])
    mock_session.request.side_effect = RequestException(error)

    broker = Broker()
    with pytest.raises(NetworkError):
        broker.fetch(method="GET", url="https://example.com")


def test_fetch_request_other_exception(mock_session):
    """
    Test the fetch method's behavior when an unknown RequestException is raised.

    This test simulates an unknown request exception by setting the mock session to raise
    a RequestException with a generic error message. It verifies that the fetch method
    raises a BrokerError in response to this error.
    """
    mock_session.request.side_effect = RequestException("Unknown error")

    broker = Broker()
    with pytest.raises(BrokerError):
        broker.fetch(method="GET", url="https://example.com")
