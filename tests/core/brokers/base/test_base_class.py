from json import JSONDecodeError, dumps
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
from core.brokers.base.errors import ResponseError


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


@pytest.fixture
def mock_response():
    """
    Fixture that provides a mock Response object.

    This fixture creates a mock Response object that can be used in tests to simulate
    different kinds of responses from a server.
    """
    return Response()


def test_json_parser_valid_json(mock_response):
    """
    Test that the _json_parser method correctly parses a valid JSON object.

    This test verifies that the method correctly parses a simple JSON object from a
    Response object and returns the expected dictionary.
    """
    mock_response.status_code = 200
    mock_response._content = b'{"key": "value"}'
    mock_response.url = "http://test.com"
    mock_response.reason = "OK"

    result = Broker._json_parser(mock_response)
    assert result == {"key": "value"}


def test_json_parser_valid_json_list(mock_response):
    """
    Test that the _json_parser method correctly parses a valid JSON array.

    This test verifies that the method correctly parses a JSON array from a Response object
    and returns the expected list of dictionaries.
    """
    mock_response.status_code = 200
    mock_response._content = b'[{"key1": "value1"}, {"key2": "value2"}]'
    mock_response.url = "http://test.com"
    mock_response.reason = "OK"

    result = Broker._json_parser(mock_response)
    assert result == [{"key1": "value1"}, {"key2": "value2"}]


def test_json_parser_invalid_json(mock_response):
    """
    Test that the _json_parser method raises a ResponseError for invalid JSON.

    This test verifies that the method raises a ResponseError when it encounters a
    Response object containing invalid JSON data.
    """
    mock_response.status_code = 200
    mock_response._content = b'{"key": "value"'  # Missing closing brace
    mock_response.url = "http://test.com"
    mock_response.reason = "OK"

    with pytest.raises(ResponseError) as exc_info:
        Broker._json_parser(mock_response)

    assert "Invalid JSON in response" in str(exc_info.value)


def test_json_parser_empty_response(mock_response):
    """
    Test that the _json_parser method raises a ResponseError for an empty response.

    This test verifies that the method raises a ResponseError when it encounters a
    Response object with no content (empty body).
    """
    mock_response.status_code = 200
    mock_response._content = b""
    mock_response.url = "http://test.com"
    mock_response.reason = "OK"

    with pytest.raises(ResponseError) as exc_info:
        Broker._json_parser(mock_response)

    assert "Invalid JSON in response" in str(exc_info.value)


def test_json_parser_whitespace_only(mock_response):
    """
    Test that the _json_parser method raises a ResponseError for a response with only whitespace.

    This test verifies that the method raises a ResponseError when it encounters a
    Response object containing only whitespace characters in the body.
    """
    mock_response.status_code = 200
    mock_response._content = b"   \n\t  "
    mock_response.url = "http://test.com"
    mock_response.reason = "OK"

    with pytest.raises(ResponseError) as exc_info:
        Broker._json_parser(mock_response)

    assert "Invalid JSON in response" in str(exc_info.value)


def test_json_parser_non_json_content(mock_response):
    """
    Test that the _json_parser method raises a ResponseError for non-JSON content.

    This test verifies that the method raises a ResponseError when it encounters a
    Response object containing non-JSON content (e.g., plain text).
    """
    mock_response.status_code = 200
    mock_response._content = b"This is not JSON"
    mock_response.url = "http://test.com"
    mock_response.reason = "OK"

    with pytest.raises(ResponseError) as exc_info:
        Broker._json_parser(mock_response)

    assert "Invalid JSON in response" in str(exc_info.value)

    mock_response.status_code = 200
    mock_response._content = b"This is not JSON"
    mock_response.url = "http://test.com"
    mock_response.reason = "OK"

    with pytest.raises(ResponseError) as exc_info:
        Broker._json_parser(mock_response)

    assert "Invalid JSON in response" in str(exc_info.value)


def test_json_dumps_simple_dict():
    """Test json_dumps with a simple dictionary."""
    data = {"key": "value"}
    result = Broker.json_dumps(data)
    assert result == '{"key": "value"}'


def test_json_dumps_nested_dict():
    """Test json_dumps with a nested dictionary."""
    data = {"outer": {"inner": "value"}}
    result = Broker.json_dumps(data)
    assert result == '{"outer": {"inner": "value"}}'


def test_json_dumps_with_list():
    """Test json_dumps with a dictionary containing a list."""
    data = {"key": [1, 2, 3]}
    result = Broker.json_dumps(data)
    assert result == '{"key": [1, 2, 3]}'


def test_json_dumps_with_numbers():
    """Test json_dumps with various number types."""
    data = {"integer": 42, "float": 3.14, "negative": -10}
    result = Broker.json_dumps(data)
    assert result == '{"integer": 42, "float": 3.14, "negative": -10}'


def test_json_dumps_with_boolean():
    """Test json_dumps with boolean values."""
    data = {"true": True, "false": False}
    result = Broker.json_dumps(data)
    assert result == '{"true": true, "false": false}'


def test_json_dumps_with_null():
    """Test json_dumps with None (null in JSON)."""
    data = {"null_value": None}
    result = Broker.json_dumps(data)
    assert result == '{"null_value": null}'


def test_json_dumps_empty_dict():
    """Test json_dumps with an empty dictionary."""
    data = {}
    result = Broker.json_dumps(data)
    assert result == "{}"


def test_json_dumps_complex_structure():
    """Test json_dumps with a complex nested structure."""
    data = {
        "string": "value",
        "number": 42,
        "float": 3.14,
        "boolean": True,
        "null": None,
        "list": [1, "two", 3.0],
        "nested": {"a": 1, "b": [2, 3, 4], "c": {"d": "nested"}},
    }
    result = Broker.json_dumps(data)
    expected = dumps(data)  # Using the actual json.dumps for comparison
    assert result == expected


def test_json_dumps_non_dict_input():
    """Test json_dumps with non-dictionary input to ensure it raises a TypeError."""
    with pytest.raises(TypeError):
        Broker.json_dumps([1, 2, 3])  # List instead of dictionary


def test_on_json_response_valid_json(mock_response):
    """
    Test case for valid JSON content in the response.

    Verifies that a properly formatted JSON response is correctly parsed into a dictionary.
    """
    mock_response._content = b'{"key": "value"}'
    result = Broker.on_json_response(mock_response)
    assert result == {"key": "value"}


def test_on_json_response_invalid_json(mock_response):
    """
    Test case for invalid JSON content in the response.

    Verifies that an invalid JSON response raises a JSONDecodeError.
    """
    mock_response._content = b'{"key": "value"'  # Invalid JSON
    with pytest.raises(JSONDecodeError):
        Broker.on_json_response(mock_response)


def test_on_json_response_empty_response(mock_response):
    """
    Test case for an empty response.

    Verifies that an empty response raises a JSONDecodeError.
    """
    mock_response._content = b""  # Empty response
    with pytest.raises(JSONDecodeError):
        Broker.on_json_response(mock_response)


def test_on_json_response_non_json_content(mock_response):
    """
    Test case for non-JSON content in the response.

    Verifies that a response with non-JSON content raises a JSONDecodeError.
    """
    mock_response._content = b"This is not JSON"  # Non-JSON content
    with pytest.raises(JSONDecodeError):
        Broker.on_json_response(mock_response)
