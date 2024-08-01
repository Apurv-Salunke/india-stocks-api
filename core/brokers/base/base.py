from __future__ import annotations

import base64
from json import JSONDecodeError, dumps, loads
from ssl import SSLError
from typing import Any, Dict, List, Optional, Tuple, Union
from pandas import DataFrame, read_csv, read_json
from pyotp import TOTP
from requests.adapters import HTTPAdapter
from requests.sessions import session as req_session
from requests.models import Response
from requests.exceptions import (
    HTTPError,
    Timeout,
    TooManyRedirects,
    RequestException,
    ConnectionError as RequestsConnectionError,
)

from core.config.network import RETRY_STRATEGY
from core.brokers.base.errors import (
    InputError,
    RequestTimeout,
    NetworkError,
    BrokerError,
    ResponseError,
)

__all__ = ["Broker"]


class Broker:
    """Base Class Common to All Brokers"""

    id = ""
    indices: Dict[str, Any] = {}
    eq_tokens: Dict[str, Any] = {}
    nfo_tokens: Dict[str, Any] = {}
    expiry_dates: Dict[str, List[str]] = {}
    cookies: Dict[str, Any] = {}
    _session: Optional[req_session] = None

    nfo_url = "https://www.nseindia.com/api/option-chain-indices"
    bfo_url = "https://api.bseindia.com/BseIndiaAPI/api/ddlExpiry_IV/w"

    def __repr__(self) -> str:
        return f"Indian-Stock-Api.{self.id}()"

    @classmethod
    def _create_session(cls) -> req_session:
        """
        Creates a new requests session.

        Returns:
            requests.Session: A new requests session object.
        """
        session = req_session()
        session._session.mount("https://", HTTPAdapter(max_retries=RETRY_STRATEGY))
        return session

    @classmethod
    def fetch(
        cls,
        method: str,
        url: str,
        headers: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        auth: Optional[Tuple[str, str]] = None,
        timeout: int = 10,
    ) -> Response:
        """
        A Wrapper for Python Requests module,
        sending requests over a session which persists the cookies over the entire session.

        Args:
            method (str): The HTTP method to use (e.g. "GET", "POST", "PUT", "DELETE").
            url (str): The URL to fetch.
            headers (Optional[Dict[str, Any]]): Any additional headers to include in the request.
            data (Optional[Dict[str, Any]]): Any data to include in the request body.
            json (Optional[Dict[str, Any]]): Any JSON data to include in the request body.
            params (Optional[Dict[str, Any]]): Any query parameters to include in the URL.
            auth (Optional[Tuple[str, str]]): A tuple of username and password for authentication.
            timeout (Optional[int]): The timeout in seconds for the request. If None, uses DEFAULT_TIMEOUT=10.

        Returns:
            requests.Response: The response object from the request.

        Raises:
            RequestTimeout: If the request times out.
            BrokerError: If there is an issue with the request, such as too many redirects or an SSL error.
            NetworkError: If there is a network-related error, such as a connection reset or aborted connection.
        """
        if cls._session is None:
            cls._create_session()

        try:
            response = None
            response = cls._session.request(
                method=method,
                url=url,
                headers=headers,
                data=data,
                json=json,
                params=params,
                auth=auth,
                timeout=timeout,
            )
            response.raise_for_status()
            return response

        except Timeout as exc:
            details = f"{cls.id} {method} {url}"
            raise RequestTimeout(details) from exc

        except RequestsConnectionError as exc:
            error_string = str(exc)
            details = " ".join([cls.id, method, url, error_string])
            if "Read timed out" in error_string:
                raise RequestTimeout(details) from exc
            raise NetworkError(details) from exc

        except ConnectionResetError as exc:
            details = f"{cls.id} {method} {url}"
            raise NetworkError(details) from exc

        except TooManyRedirects as exc:
            details = f"{cls.id} {method} {url}"
            raise BrokerError(details) from exc

        except SSLError as exc:
            details = f"{cls.id} {method} {url}"
            raise BrokerError(details) from exc

        except HTTPError as exc:
            if response is not None:
                details = (
                    f"{cls.id} {method} {response.status_code} {url} {response.text}"
                )
            else:
                details = f"{cls.id} {method} {url} (no response available)"
            raise BrokerError(details) from exc

        except RequestException as exc:
            error_string = str(exc)
            details = f"{cls.id} {method} {url}"
            if any(
                x in error_string
                for x in ["ECONNRESET", "Connection aborted.", "Connection broken:"]
            ):
                raise NetworkError(exc) from exc
            raise BrokerError(exc) from exc

    @staticmethod
    def _json_parser(response: Response) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Get JSON object from a request Response.

        Args:
            response (Response): Response Object

        Returns:
            Union[Dict[str, Any], List[Dict[str, Any]]]: JSON Object received from Response.

        Raises:
            ResponseError: If there's an error parsing the response.
        """
        try:
            return loads(response.text.strip())
        except JSONDecodeError as json_err:
            raise ResponseError(
                {
                    "Status": response.status_code,
                    "Error": "Invalid JSON in response",
                    "URL": response.url,
                    "Reason": response.reason,
                }
            ) from json_err
        except Exception as exc:
            raise ResponseError(
                {
                    "Status": response.status_code,
                    "Error": response.text,
                    "URL": response.url,
                    "Reason": response.reason,
                }
            ) from exc

    @staticmethod
    def json_dumps(json_data: dict) -> str:
        """
        Convert a Python dictionary to a JSON string.

        Args:
            json_data (dict): The Python dictionary to convert to JSON.

        Returns:
            str: The JSON string representation of the input dictionary.

        Raises:
            TypeError: If the input is not a dictionary.
        """
        if not isinstance(json_data, dict):
            raise TypeError("Input must be a dictionary")
        return dumps(json_data)

    @staticmethod
    def on_json_response(response: Response) -> dict[Any, Any]:
        """
        Get json object from a request Response.

        Parameters:
            response (Response): Response Object

        Returns:
            dict: Json Object received from Response.
        """
        return loads(response.text.strip())

    @staticmethod
    def generate_verified_totp(totpbase: str, max_attempts: int = 3) -> str:
        """
        Generate and verify a TOTP from the given base string.

        Parameters:
            totpbase (str): String used to Generate TOTP.
            max_attempts (int): Maximum number of attempts to generate a valid TOTP.

        Returns:
            str: Six-Digit TOTP

        Raises:
            ValueError: If unable to generate a valid TOTP within the maximum attempts.
        """
        if not totpbase:
            raise ValueError("Invalid TOTP base")

        try:
            base64.b32decode(totpbase, casefold=True)
        except ValueError:
            raise ValueError("Invalid TOTP base")
        for _ in range(max_attempts):
            totpobj = TOTP(totpbase)
            totp = totpobj.now()

            if totpobj.verify(totp):
                return totp

        raise ValueError(
            f"Unable to generate a valid TOTP after {max_attempts} attempts"
        )

    @staticmethod
    def data_reader(
        link: str,
        filetype: str,
        dtype: dict | None = None,
        sep: str = ",",
        col_names: list = [],
    ) -> DataFrame:
        """
        Pandas.read_csv & Pandas.read_json Functions Wrapper

        Parameters:
            link (str): URL to get the Data From.
            filetype (str): 'json' | 'csv'
            dtype (dict | None, optional): A Dicitonary with Column-Names as the Keys and their Datatypes as the values. Defaults to None.
            sep (str, optional): Needed with filetype as 'csv', to input the data seperator. Defaults to ','.

        Raises:
            InputError: If Wrong filetype Given as Input

        Returns:
            DataFrame: Pandas DataFrame
        """
        if filetype == "json":
            return read_json(link)

        if filetype == "csv":
            if col_names:
                return read_csv(
                    link,
                    dtype=dtype,
                    sep=sep,
                    names=col_names,
                    # sep="|",
                )

            return read_csv(
                link,
                dtype=dtype,
                sep=sep,
                on_bad_lines="skip",
                encoding_errors="ignore",
            )

        raise InputError(
            f"Wrong Filetype: {filetype}, the possible values are: 'json', 'csv'"
        )
