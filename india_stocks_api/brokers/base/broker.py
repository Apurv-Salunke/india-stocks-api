from __future__ import annotations

from datetime import datetime, timedelta
from os import popen
import re
from time import sleep
from numpy import nan
from pandas.errors import OutOfBoundsDatetime
from json import JSONDecodeError, dumps, loads
from ssl import SSLError
from typing import Any, Dict, List, Optional, Tuple, Union
from pandas import (
    DataFrame,
    DateOffset,
    Series,
    Timestamp,
    read_csv,
    read_json,
    to_datetime,
    concat as pd_concat,
)
from pyotp import TOTP
import pytz
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

from india_stocks_api.brokers.base.constants import DATETIME_FORMAT, Root, WeeklyExpiry
from india_stocks_api.config.network import RETRY_STRATEGY
from india_stocks_api.brokers.base.errors import (
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
    def _create_session(cls) -> None:
        """
        Creates a new requests session.

        Returns:
            requests.Session: A new requests session object.
        """
        session = req_session()
        session.mount("https://", HTTPAdapter(max_retries=RETRY_STRATEGY))
        cls._session = session

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
    def _json_dumps(json_data: dict) -> str:
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
    def _eq_mapper(
        dictionary: dict,
        key: str,
    ) -> str:
        """
        A Simple Function to help the User if they input a wrong Symbol in the eq_tokens dictionary,
        also tells the User the possible Symbols for the Segment.

        Parameters:
            dictionary (dict): Dicitonary
            key (str): Dictionary Key to Check Against, should be capital Letters.

        Raises:
            KeyError: If Key Does not exist in the Dicitonary.

        Returns:
            str: The Value of the Key in the Dicitonary.
        """
        key = key.upper()
        if key in dictionary:
            return dictionary[key]

        r = re.compile(r"[A-Z]*<str>[A-Z]*$".replace("<str>", key))
        possible_values = list(filter(r.findall, dictionary))

        raise KeyError(f"Invalid Symbol!: {key}, Possible Values: {possible_values}")

    @staticmethod
    def _key_mapper(
        dictionary: dict,
        key: str,
        name: str,
    ) -> str:
        """
        A Simple Function to help the User if they input a wrong Key in the Dictionary,
        also tells the User the possible Keys for the Dicitonary.

        Parameters:
            dictionary (dict): Dicitonary
            key (str): Dictionary Key to Check Against.
            name (str): The right Key to the Dicitonary.

        Raises:
            KeyError: If Key Does not exist in the Dicitonary.

        Returns:
            str: The Value of the Key in the Dicitonary.
        """

        if key in dictionary:
            return dictionary[key]

        raise KeyError(
            f"Invalid {name}!: {key}, Possible Values: {list(dictionary.keys())}"
        )

    @staticmethod
    def totp_creator(totpbase: str, max_attempts: int = 3) -> str:
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

        # Generate TOTP
        for _ in range(max_attempts):
            totpobj = TOTP(totpbase.replace(" ", ""))
            return totpobj.now()

        raise ValueError("Failed to generate a valid TOTP within the maximum attempts")

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

    @staticmethod
    def pd_datetime(
        datetime_obj: Union[str, int, float],
        unit: str = "ns",
        tz: str = None,
    ) -> Timestamp:
        valid_units = ["D", "s", "ms", "us", "ns"]
        if unit not in valid_units:
            raise ValueError(f"Invalid unit. Must be one of {valid_units}")

        if tz and tz not in pytz.all_timezones:
            raise ValueError(f"Invalid timezone: {tz}")

        try:
            if isinstance(datetime_obj, str) and datetime_obj.isdigit():
                datetime_obj = float(datetime_obj)

            if isinstance(datetime_obj, (int, float)):
                timestamp = Timestamp(datetime_obj, unit=unit)
            else:
                timestamp = to_datetime(datetime_obj)

            if tz:
                timestamp = timestamp.tz_localize("UTC").tz_convert(tz)

            return timestamp

        except OutOfBoundsDatetime:
            raise OutOfBoundsDatetime(
                f"Datetime value is out of range for unit '{unit}': {datetime_obj}"
            )
        except ValueError as e:
            raise ValueError(f"Invalid input: {e}")

    @staticmethod
    def datetime_strp(
        datetime_str: str,
        dtformat: str = DATETIME_FORMAT,
    ) -> datetime:
        """
        Python datetime.datetime.strptime Function Wrapper

        Parameters:
            datetime_str (str): Datetime String to convert to datetime object.
            dtformat (str): corresponding datetime format string.

        Returns:
            datetime: datetime.datetime object

        Raises:
            ValueError: If the datetime string doesn't match the given format.
        """
        try:
            return datetime.strptime(datetime_str, dtformat)
        except ValueError as e:
            raise ValueError(f"Error parsing datetime: {e}")

    @staticmethod
    def datetime_format(
        datetime_obj: datetime,
        dtformat: str = DATETIME_FORMAT,
    ) -> str:
        """
        Converts a datetime object to a formatted string.

        Parameters:
            datetime_obj (datetime): The datetime object to format.
            dtformat (str): The desired datetime format string.

        Returns:
            str: A string representation of the datetime object in the specified format.

        Raises:
            ValueError: If the datetime object is not a valid datetime or the format is invalid.
        """
        # List of supported specifiers based on Python's datetime library
        supported_specifiers = set("aAbBcdHIjmMpSUwWxXyYZfz%")

        # Find all potential format specifiers in the string
        try:
            specifiers_in_format = {
                match.group(1) for match in re.finditer(r"%(.)", dtformat)
            }
        except IndexError:
            raise ValueError("Invalid format string.")

        # Identify invalid specifiers
        invalid_specifiers = specifiers_in_format - supported_specifiers
        if invalid_specifiers:
            raise ValueError(
                f"Invalid format string: Unsupported specifiers {invalid_specifiers}"
            )

        # Proceed with formatting
        if not isinstance(datetime_obj, datetime):
            raise ValueError("Input must be a datetime object.")
        return datetime_obj.strftime(dtformat)

    @staticmethod
    def from_timestamp(datetime_obj: Union[int, float]) -> datetime:
        """
        Convert Epoch Time to datetime.datetime object

        Parameters:
            datetime_obj (int or float): Epoch datetime in seconds

        Returns:
            datetime: datetime.datetime object in local timezone

        Raises:
            ValueError: If the timestamp is out of the range of values supported by datetime
            TypeError: If the input is not a number
        """
        try:
            if not isinstance(datetime_obj, (int, float)) or datetime_obj < 0:
                raise ValueError("Timestamp must be a non-negative number.")
            return datetime.fromtimestamp(datetime_obj)
        except (ValueError, OverflowError, OSError) as e:
            raise ValueError(f"Invalid timestamp: {e}")
        except TypeError as e:
            raise TypeError(f"Invalid input type: {e}")

    @staticmethod
    def current_datetime() -> datetime:
        """
        Get Current System Datetime

        Returns:
            datetime: datetime.datetime object
        """
        return datetime.now()

    @staticmethod
    def time_delta(
        datetime_object: datetime, delta: int, dtformat: str, default="sub"
    ) -> str:
        """
        Add Days to a datetime.datetime object

        Parameters:
            datetime_object (datetime): datetime object
            delta (int): No. of Days to add or subtract from datetime_obj
            dtformat (str): corresponding datetime format string.
            default (str, optional): Whether to add or subtract a Day from datetime_obj ('add' | 'sub'). Defaults to 'sub'.

        Raises:
            InputError: If Wrong Value Given for default Parameter.

        Returns:
            str: A datetime string.
        """
        if default == "sub":
            return (datetime_object - timedelta(days=delta)).strftime(dtformat)
        if default == "add":
            return (datetime_object + timedelta(days=delta)).strftime(dtformat)

        raise InputError(
            f"Wrong default: {default}, the possible values are 'sub', 'add'"
        )

    @staticmethod
    def dateoffset(*args: Any, **kwargs: Any) -> DateOffset:
        """
        Create a Pandas DateOffset object.

        Args:
            *args: Positional arguments to pass to DateOffset.
            **kwargs: Keyword arguments to pass to DateOffset.

        Returns:
            DateOffset: A Pandas DateOffset object.

        Raises:
            ValueError: If invalid arguments are provided.

        Example:
            offset = Broker.dateoffset(days=1, hours=2)
        """
        try:
            return DateOffset(*args, **kwargs)
        except ValueError as e:
            raise ValueError(f"Invalid arguments for DateOffset: {e}")

    @staticmethod
    def concatenate_dataframes(dfs: list[DataFrame], **kwargs) -> DataFrame:
        """
        Concatenate a list of pandas DataFrames.

        Args:
            dfs (list[DataFrame]): List of DataFrames to concatenate.
            **kwargs: Additional keyword arguments to pass to pandas.concat.

        Returns:
            DataFrame: Concatenated DataFrame.

        Raises:
            ValueError: If the input list is empty.
            TypeError: If any element in the list is not a DataFrame.
        """
        if not dfs:
            raise ValueError("Input list is empty")
        if not all(isinstance(df, DataFrame) for df in dfs):
            raise TypeError("All elements must be pandas DataFrames")
        return pd_concat(dfs, **kwargs)

    @staticmethod
    def filter_future_dates(data: Union[List[str], Series]) -> List[str]:
        """
        Filter and sort dates, returning only future dates.

        Args:
            data (Union[List[str], Series]): Input dates to filter.

        Returns:
            List[str]: Sorted list of future dates in string format (YYYY-MM-DD).
        """
        # Convert the input data to a pandas Series if it's a list
        if isinstance(data, list):
            data = Series(data)

        # Check for invalid date formats and raise ValueError
        if not all(to_datetime(data, errors="coerce").notna()):
            raise ValueError("Invalid date format found in input data.")

        # Convert to datetime
        dates = to_datetime(data, errors="coerce")

        # Filter out NaT values and future dates
        now = Timestamp.now()
        future_dates = dates[dates >= now].dropna()

        # Format and return the result as a sorted list of strings
        return sorted(future_dates.dt.strftime("%Y-%m-%d").tolist())

    @classmethod
    def download_expiry_dates_nfo(cls, root):
        temp_session = req_session()

        for _ in range(5):
            try:
                headers = {
                    "accept": "*/*",
                    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7",
                    "dnt": "1",
                    "referer": "https://www.nseindia.com/option-chain",
                    "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                }

                params = {
                    "symbol": f"{root}",
                }

                response = temp_session.request(
                    method="GET",
                    url=cls.nfo_url,
                    params=params,
                    cookies=cls.cookies,
                    headers=headers,
                    timeout=10,
                )
                data = response.json()
                expiry_dates = data["records"]["expiryDates"]
                cls.expiry_dates[root] = cls.filter_future_dates(expiry_dates)
                return None

            except Exception as exc:
                print(f"Error in req_session: {exc}")

            try:
                # Fallback to curl
                response = popen(
                    f'curl "{cls.nfo_url}?symbol={root}" -H "authority: beta.nseindia.com" -H "cache-control: max-age=0" -H "dnt: 1" -H "upgrade-insecure-requests: 1" -H "user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36" -H "sec-fetch-user: ?1" -H "accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9" -H "sec-fetch-site: none" -H "sec-fetch-mode: navigate" -H "accept-encoding: gzip, deflate, br" -H "accept-language: en-US,en;q=0.9,hi;q=0.8" --compressed'
                ).read()
                data = loads(response)
                expiry_dates = data["records"]["expiryDates"]
                cls.expiry_dates[root] = cls.filter_future_dates(expiry_dates)
                return None

            except Exception as exc:
                print(f"Error in curl: {exc}")

            sleep(5)

    @classmethod
    def download_expiry_dates_bfo(
        cls,
        root,
    ):
        if root == Root.SENSEX:
            scrip_cd = 1
        elif root == Root.BANKEX:
            scrip_cd = 12

        temp_session = req_session()

        for _ in range(5):
            try:
                headers = {
                    "accept": "application/json, text/plain, */*",
                    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7",
                    "dnt": "1",
                    "if-modified-since": "Sun, 24 Mar 2024 11:21:31 GMT",
                    "origin": "https://www.bseindia.com",
                    "referer": "https://www.bseindia.com/",
                    "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-site",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                }

                params = {
                    "ProductType": "IO",
                    "scrip_cd": scrip_cd,
                }

                response = temp_session.request(
                    method="GET",
                    url=cls.bfo_url,
                    params=params,
                    headers=headers,
                    timeout=10,
                )
                data = response.json()

                expiry_dates = data["Table1"]
                expiry_dates = [i["ExpiryDate"] for i in expiry_dates]
                cls.expiry_dates[root] = cls.filter_future_dates(expiry_dates)
                return None

            except Exception as exec:
                print(f"Error while fetching BSE data: {exec}")

            try:
                response = popen(
                    f"curl '{cls.bfo_url}?ProductType=IO&scrip_cd=1' -H 'accept: application/json, text/plain, */*' -H 'accept-language: en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7' -H 'dnt: 1' -H 'if-modified-since: Sun, 24 Mar 2024 11:21:31 GMT' -H 'origin: https://www.bseindia.com' -H 'referer: https://www.bseindia.com/' -H 'sec-ch-ua-mobile: ?0' -H 'sec-fetch-dest: empty' -H 'sec-fetch-mode: cors' -H 'sec-fetch-site: same-site' -H 'user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'"
                ).read()
                data = loads(response)

                expiry_dates = data["Table1"]
                expiry_dates = [i["ExpiryDate"] for i in expiry_dates]
                cls.expiry_dates[root] = cls.filter_future_dates(expiry_dates)
                return None

            except Exception as exc:  # noqa: E722
                print(f"Error: {exc}")

            sleep(5)

    @classmethod
    def jsonify_expiry(
        cls,
        data_frame: DataFrame,
    ) -> dict[Any, Any]:
        """
        Creates a india-stocks-api Unified Dicitonary for BankNifty & Nifty Options,
        in the following Format:

            Global[expiry][root][option][strikeprice]

            expiry: 'CURRENT' | 'NEXT' | 'FAR' | 'Expiries'(A List of All Expiries).
            root: 'BANKNIFTY' | 'NIFTY' | 'FINNIFTY | MIDCPNIFTY.
            option: 'CE' | 'PE'.
            strikeprice: Integer Values for Strike Price.

            {
                'CURRENT': {
                    'BANKNIFTY': {
                        'CE': {
                            34500: {
                                'Token': 43048,
                                'Symbol': 'BANKNIFTY2331634500CE',
                                'Expiry': '2023-03-16',
                                'Option': 'CE',
                                'StrikePrice': 34500,
                                'LotSize': 25,
                                'Root': 'BANKNIFTY',
                                'TickSize': 0.05,
                                'ExpiryName': 'CURRENT'
                                },
                    ...

                'Expiries': [
                     '2023-03-16', '2023-03-23', '2023-03-29', '2023-04-06',
                     '2023-04-13', '2023-04-27', '2023-05-25', '2023-06-29',
                     '2023-09-28', '2023-12-28', '2024-06-28', '2024-12-27',
                     '2025-06-27', '2025-12-25', '2026-06-25', '2026-12-31',
                     '2027-06-24', '2027-12-30']

            }

        Parameters:
            data_frame (DataFrame): DataFrame to Convert to india-stocks-api Unified Expiry Dictionary

        Returns:
            dict[Any, Any]: Dictioanry With 3 Most Recent Expiries for Both BankNifty & Nifty.
        """

        expiry_data = {
            WeeklyExpiry.CURRENT: {
                Root.BNF: {},
                Root.NF: {},
                Root.FNF: {},
                Root.MIDCPNF: {},
                Root.SENSEX: {},
                Root.BANKEX: {},
            },
            WeeklyExpiry.NEXT: {
                Root.BNF: {},
                Root.NF: {},
                Root.FNF: {},
                Root.MIDCPNF: {},
                Root.SENSEX: {},
                Root.BANKEX: {},
            },
            WeeklyExpiry.FAR: {
                Root.BNF: {},
                Root.NF: {},
                Root.FNF: {},
                Root.MIDCPNF: {},
                Root.SENSEX: {},
                Root.BANKEX: {},
            },
            WeeklyExpiry.EXPIRY: {
                Root.BNF: [],
                Root.NF: [],
                Root.FNF: [],
                Root.MIDCPNF: [],
                Root.SENSEX: [],
                Root.BANKEX: [],
            },
            WeeklyExpiry.LOTSIZE: {
                Root.BNF: nan,
                Root.NF: nan,
                Root.FNF: nan,
                Root.MIDCPNF: nan,
                Root.SENSEX: nan,
                Root.BANKEX: nan,
            },
        }

        data_frame = data_frame.sort_values(by=["Expiry"])

        for root in [
            Root.BNF,
            Root.NF,
            Root.FNF,
            Root.MIDCPNF,
            Root.SENSEX,
            Root.BANKEX,
        ]:
            if root not in cls.expiry_dates:
                if root in [Root.SENSEX, Root.BANKEX]:
                    cls.download_expiry_dates_bfo(root=root)
                else:
                    cls.download_expiry_dates_nfo(root=root)
            small_df = data_frame[data_frame["Root"] == root]

            if small_df.shape[0]:
                expiries = small_df["Expiry"].unique()
                expiries = expiries[expiries >= str(datetime.now().date())]
                lotsize = small_df["LotSize"].unique()[0]

                dfex1 = small_df[small_df["Expiry"] == cls.expiry_dates[root][0]]
                dfex2 = small_df[small_df["Expiry"] == cls.expiry_dates[root][1]]
                dfex3 = small_df[small_df["Expiry"] == cls.expiry_dates[root][2]]

                dfexs = [("CURRENT", dfex1), ("NEXT", dfex2), ("FAR", dfex3)]

                for expiry_name, dfex in dfexs:
                    dfex["ExpiryName"] = expiry_name

                    global_dict = {"CE": {}, "PE": {}}

                    for j, i in dfex.groupby(["Option", "StrikePrice"]):
                        global_dict[j[0]][j[1]] = i.to_dict("records")[0]
                        expiry_data[expiry_name][root] = global_dict

                    expiry_data["Expiry"][root] = list(expiries)
                    expiry_data["LotSize"][root] = lotsize
            else:
                pass

        return expiry_data
