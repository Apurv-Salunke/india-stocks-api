import json
import time
from datetime import timedelta

import pandas as pd
from india_stocks_api.internal.context import (
    get_br_symbol,
    get_httpx_client,
    get_logger,
    get_token,
    get_api_key
)

logger = get_logger(__name__)


def get_api_response(endpoint, auth, method="GET", payload=""):
    """
    Perform an HTTP request to the Angel One API and return the parsed JSON response.
    
    Parameters:
    	endpoint (str): Path and query portion of the API URL (e.g. '/client/v1/api/endpoint').
    	auth (str): Bearer authentication token.
    	method (str): HTTP method to use; typically "GET" or "POST". Defaults to "GET".
    	payload (dict|str): Request payload; if a dict, it will be serialized to JSON.
    
    Returns:
    	parsed (dict|list): The JSON-decoded response body.
    
    Raises:
    	Exception: If the API returns HTTP 403 (authentication failed) or if the response body cannot be parsed as JSON.
    """
    AUTH_TOKEN = auth

    api_key = get_api_key()

    # Get the shared httpx client with connection pooling
    client = get_httpx_client()

    headers = {
        "Authorization": f"Bearer {AUTH_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "CLIENT_LOCAL_IP",
        "X-ClientPublicIP": "CLIENT_PUBLIC_IP",
        "X-MACAddress": "MAC_ADDRESS",
        "X-PrivateKey": api_key,
    }

    if isinstance(payload, dict):
        payload = json.dumps(payload)

    url = f"https://apiconnect.angelbroking.com{endpoint}"

    try:
        if method == "GET":
            response = client.get(url, headers=headers)
        elif method == "POST":
            response = client.post(url, headers=headers, content=payload)
        else:
            response = client.request(method, url, headers=headers, content=payload)

        # Add status attribute for compatibility with the existing codebase
        response.status = response.status_code

        if response.status_code == 403:
            logger.debug(f"Debug - API returned 403 Forbidden. Headers: {headers}")
            logger.debug(f"Debug - Response text: {response.text}")
            raise Exception(
                "Authentication failed. Please check your API key and auth token."
            )

        return json.loads(response.text)
    except json.JSONDecodeError:
        logger.error(
            f"Debug - Failed to parse response. Status code: {response.status_code}"
        )
        logger.debug(f"Debug - Response text: {response.text}")
        raise Exception(f"Failed to parse API response (status {response.status_code})")


class BrokerData:
    def __init__(self, auth_token):
        """
        Initialize the BrokerData handler with credentials and default timeframe mappings.
        
        Parameters:
            auth_token (str): Authentication token used for API requests to the Angel One service.
        
        Description:
            Stores the provided auth token and defines a mapping from common interval strings
            (e.g., "1m", "D") to the Angel One API resolution identifiers used when requesting
            historical or streaming data.
        """
        self.auth_token = auth_token
        # Map common timeframe format to Angel resolutions
        self.timeframe_map = {
            # Minutes
            "1m": "ONE_MINUTE",
            "3m": "THREE_MINUTE",
            "5m": "FIVE_MINUTE",
            "10m": "TEN_MINUTE",
            "15m": "FIFTEEN_MINUTE",
            "30m": "THIRTY_MINUTE",
            # Hours
            "1h": "ONE_HOUR",
            # Daily
            "D": "ONE_DAY",
        }

    def get_quotes(self, symbol: str, exchange: str) -> dict:
        """
        Retrieve the latest market quote for a symbol from the broker and normalize it into a standard dictionary.
        
        Parameters:
            symbol (str): Trading symbol.
            exchange (str): Exchange code (e.g., "NSE", "BSE", "NFO", "BFO", "CDS", "MCX"). Index exchanges "NSE_INDEX", "BSE_INDEX", and "MCX_INDEX" are normalized to "NSE", "BSE", and "MCX" respectively.
        
        Returns:
            dict: Normalized quote with the following keys:
                - bid (float): Best bid price (0 if unavailable).
                - ask (float): Best ask price (0 if unavailable).
                - open (float): Opening price.
                - high (float): High price.
                - low (float): Low price.
                - ltp (float): Last traded price.
                - prev_close (float): Previous close price.
                - volume (int): Trade volume.
                - oi (int): Open interest.
        
        Raises:
            Exception: If the broker API returns an error, no quote data is received, or any other failure occurs while fetching or parsing the quote.
        """
        try:
            # Convert symbol to broker format and get token
            #br_symbol = get_br_symbol(symbol, exchange)
            token = get_token(symbol, exchange)

            if exchange == "NSE_INDEX":
                exchange = "NSE"
            elif exchange == "BSE_INDEX":
                exchange = "BSE"
            elif exchange == "MCX_INDEX":
                exchange = "MCX"

            # Prepare payload for Angel's quote API
            payload = {"mode": "FULL", "exchangeTokens": {exchange: [token]}}

            response = get_api_response(
                "/rest/secure/angelbroking/market/v1/quote/",
                self.auth_token,
                "POST",
                payload,
            )

            if not response.get("status"):
                raise Exception(
                    f"Error from Angel API: {response.get('message', 'Unknown error')}"
                )

            # Extract quote data from response
            fetched_data = response.get("data", {}).get("fetched", [])
            if not fetched_data:
                raise Exception("No quote data received")

            quote = fetched_data[0]

            # Return quote in common format
            depth = quote.get("depth", {})
            bids = depth.get("buy", [])
            asks = depth.get("sell", [])

            return {
                "bid": float(bids[0].get("price", 0)) if bids else 0,
                "ask": float(asks[0].get("price", 0)) if asks else 0,
                "open": float(quote.get("open", 0)),
                "high": float(quote.get("high", 0)),
                "low": float(quote.get("low", 0)),
                "ltp": float(quote.get("ltp", 0)),
                "prev_close": float(quote.get("close", 0)),
                "volume": int(quote.get("tradeVolume", 0)),
                "oi": int(quote.get("opnInterest", 0)),
            }

        except Exception as e:
            raise Exception(f"Error fetching quotes: {str(e)}")

    def get_multiquotes(self, symbols: list) -> list:
        """
        Retrieve real-time quotes for multiple symbols, automatically batching requests to respect API limits.
        
        Parameters:
            symbols (list): List of mappings each containing 'symbol' and 'exchange' keys.
                            Example: [{'symbol': 'SBIN', 'exchange': 'NSE'}, ...]
        
        Returns:
            list: A list of per-symbol result dictionaries. Successful entries include keys
                  'symbol', 'exchange', and 'data' (the normalized quote payload). Failed or
                  unresolved symbols may be represented as dictionaries with an 'error' key
                  describing the failure.
        """
        try:
            BATCH_SIZE = 50  # Angel API limit: 50 symbols per request
            RATE_LIMIT_DELAY = 1.0  # Angel rate limit: 1 request per second

            # If symbols exceed batch size, process in batches
            if len(symbols) > BATCH_SIZE:
                logger.info(
                    f"Processing {len(symbols)} symbols in batches of {BATCH_SIZE}"
                )
                all_results = []

                # Split symbols into batches
                for i in range(0, len(symbols), BATCH_SIZE):
                    batch = symbols[i : i + BATCH_SIZE]
                    logger.debug(
                        f"Processing batch {i // BATCH_SIZE + 1}: symbols {i + 1} to {min(i + BATCH_SIZE, len(symbols))}"
                    )

                    # Process this batch
                    batch_results = self._process_quotes_batch(batch)
                    all_results.extend(batch_results)

                    # Rate limit delay between batches
                    if i + BATCH_SIZE < len(symbols):
                        time.sleep(RATE_LIMIT_DELAY)

                logger.info(
                    f"Successfully processed {len(all_results)} quotes in {(len(symbols) + BATCH_SIZE - 1) // BATCH_SIZE} batches"
                )
                return all_results
            else:
                # Single batch processing
                return self._process_quotes_batch(symbols)

        except Exception as e:
            logger.exception("Error fetching multiquotes")
            raise Exception(f"Error fetching multiquotes: {e}")

    def _process_quotes_batch(self, symbols: list) -> list:
        """
        Fetch quotes for up to 50 symbols and return normalized quote entries along with any skipped symbols.
        
        Parameters:
            symbols (list): List of dictionaries each containing 'symbol' and 'exchange' keys (maximum 50 entries).
        
        Returns:
            list: A list where entries that failed token resolution appear first as dicts with keys
                'symbol', 'exchange', and 'error', followed by successful quote entries of the form:
                {
                    "symbol": str,
                    "exchange": str,
                    "data": {
                        "bid": float,
                        "ask": float,
                        "open": float,
                        "high": float,
                        "low": float,
                        "ltp": float,
                        "prev_close": float,
                        "volume": int,
                        "oi": int
                    }
                }
        """
        # Group symbols by exchange and build token map
        exchange_tokens = {}  # {exchange: [token1, token2, ...]}
        token_map = {}  # {exchange:token -> {symbol, exchange, br_symbol}}
        skipped_symbols = []  # Track symbols that couldn't be resolved

        for item in symbols:
            symbol = item["symbol"]
            exchange = item["exchange"]

            try:
                br_symbol = get_br_symbol(symbol, exchange)
                token = get_token(symbol, exchange)

                # Track symbols that couldn't be resolved
                if not token:
                    logger.warning(
                        f"Skipping symbol {symbol} on {exchange}: could not resolve token"
                    )
                    skipped_symbols.append(
                        {
                            "symbol": symbol,
                            "exchange": exchange,
                            "error": "Could not resolve token",
                        }
                    )
                    continue

                # Normalize exchange for indices
                api_exchange = exchange
                if exchange == "NSE_INDEX":
                    api_exchange = "NSE"
                elif exchange == "BSE_INDEX":
                    api_exchange = "BSE"
                elif exchange == "MCX_INDEX":
                    api_exchange = "MCX"

                # Add token to exchange group
                if api_exchange not in exchange_tokens:
                    exchange_tokens[api_exchange] = []
                exchange_tokens[api_exchange].append(token)

                # Store mapping for response parsing
                token_map[f"{api_exchange}:{token}"] = {
                    "symbol": symbol,
                    "exchange": exchange,
                    "br_symbol": br_symbol,
                    "token": token,
                }

            except Exception as e:
                logger.warning(f"Skipping symbol {symbol} on {exchange}: {str(e)}")
                skipped_symbols.append(
                    {"symbol": symbol, "exchange": exchange, "error": str(e)}
                )
                continue

        # Return skipped symbols if no valid tokens
        if not exchange_tokens:
            logger.warning("No valid tokens to fetch quotes for")
            return skipped_symbols

        # Prepare payload for Angel's quote API
        payload = {"mode": "FULL", "exchangeTokens": exchange_tokens}

        logger.info(
            f"Requesting quotes for {sum(len(t) for t in exchange_tokens.values())} instruments across {len(exchange_tokens)} exchanges"
        )
        logger.debug(f"Exchange tokens: {exchange_tokens}")

        # Make API call
        response = get_api_response(
            "/rest/secure/angelbroking/market/v1/quote/",
            self.auth_token,
            "POST",
            payload,
        )

        if not response.get("status"):
            error_msg = (
                f"Error from Angel API: {response.get('message', 'Unknown error')}"
            )
            logger.error(error_msg)
            raise Exception(error_msg)

        # Parse response and build results
        results = []
        fetched_data = response.get("data", {}).get("fetched", [])
        unfetched_data = response.get("data", {}).get("unfetched", [])

        if unfetched_data:
            logger.warning(f"Some symbols could not be fetched: {unfetched_data}")

        # Create a lookup by exchange:token for quick access
        quotes_by_token = {}
        for quote in fetched_data:
            exchange = quote.get("exchange")
            token = quote.get("symbolToken")
            if exchange and token:
                quotes_by_token[f"{exchange}:{token}"] = quote

        # Build results from token_map
        for key, original in token_map.items():
            quote = quotes_by_token.get(key)

            if not quote:
                logger.warning(f"No quote data found for {original['symbol']} ({key})")
                results.append(
                    {
                        "symbol": original["symbol"],
                        "exchange": original["exchange"],
                        "error": "No quote data available",
                    }
                )
                continue

            # Parse and format quote data
            depth = quote.get("depth", {})
            bids = depth.get("buy", [])
            asks = depth.get("sell", [])

            result_item = {
                "symbol": original["symbol"],
                "exchange": original["exchange"],
                "data": {
                    "bid": float(bids[0].get("price", 0)) if bids else 0,
                    "ask": float(asks[0].get("price", 0)) if asks else 0,
                    "open": float(quote.get("open", 0)),
                    "high": float(quote.get("high", 0)),
                    "low": float(quote.get("low", 0)),
                    "ltp": float(quote.get("ltp", 0)),
                    "prev_close": float(quote.get("close", 0)),
                    "volume": int(quote.get("tradeVolume", 0)),
                    "oi": int(quote.get("opnInterest", 0)),
                },
            }
            results.append(result_item)

        # Include skipped symbols in results
        return skipped_symbols + results

    def get_history(
        self, symbol: str, exchange: str, interval: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Retrieve historical OHLCV (and open interest for F&O) data for a symbol over a date range.
        
        Parameters:
            symbol (str): Trading symbol in broker format or user format resolved internally.
            exchange (str): Exchange code (e.g., "NSE", "BSE", "NFO", "BFO", "CDS", "MCX"). Index exchanges ("NSE_INDEX", "BSE_INDEX", "MCX_INDEX") are normalized.
            interval (str): Candle interval; one of "1m", "3m", "5m", "10m", "15m", "30m", "1h", or "D".
            start_date (str): Start date in "YYYY-MM-DD" format.
            end_date (str): End date in "YYYY-MM-DD" format.
        
        Returns:
            pd.DataFrame: Historical data with columns [close, high, low, open, timestamp, volume, oi].
                - timestamp is Unix epoch seconds.
                - oi is included and populated for F&O exchanges (NFO, BFO, CDS, MCX); otherwise it's 0.
        
        Raises:
            Exception: If the timeframe or interval is unsupported, if the API returns an error, or on other failures while fetching or processing data.
        """
        try:
            # Convert symbol to broker format and get token
            br_symbol = get_br_symbol(symbol, exchange)

            token = get_token(symbol, exchange)
            logger.debug(f"Debug - Broker Symbol: {br_symbol}, Token: {token}")

            if exchange == "NSE_INDEX":
                exchange = "NSE"
            elif exchange == "BSE_INDEX":
                exchange = "BSE"
            elif exchange == "MCX_INDEX":
                exchange = "MCX"

            # Check for unsupported timeframes
            if interval not in self.timeframe_map:
                supported = list(self.timeframe_map.keys())
                raise Exception(
                    f"Timeframe '{interval}' is not supported by Angel. Supported timeframes are: {', '.join(supported)}"
                )

            # Convert dates to datetime objects
            from_date = pd.to_datetime(start_date)
            to_date = pd.to_datetime(end_date)

            # Set start time to 00:00 for the start date
            from_date = from_date.replace(hour=0, minute=0)

            # If end_date is today, set the end time to current time
            current_time = pd.Timestamp.now()
            if to_date.date() == current_time.date():
                to_date = current_time.replace(
                    second=0, microsecond=0
                )  # Remove seconds and microseconds
            else:
                # For past dates, set end time to 23:59
                to_date = to_date.replace(hour=23, minute=59)

            # Initialize empty list to store DataFrames
            dfs = []

            # Set chunk size based on interval as per Angel API documentation
            interval_limits = {
                "1m": 30,  # ONE_MINUTE
                "3m": 60,  # THREE_MINUTE
                "5m": 100,  # FIVE_MINUTE
                "10m": 100,  # TEN_MINUTE
                "15m": 200,  # FIFTEEN_MINUTE
                "30m": 200,  # THIRTY_MINUTE
                "1h": 400,  # ONE_HOUR
                "D": 2000,  # ONE_DAY
            }

            chunk_days = interval_limits.get(interval)
            if not chunk_days:
                supported = list(interval_limits.keys())
                raise Exception(
                    f"Interval '{interval}' not supported. Supported intervals: {', '.join(supported)}"
                )

            # Process data in chunks
            current_start = from_date
            while current_start <= to_date:
                # Calculate chunk end date
                current_end = min(
                    current_start + timedelta(days=chunk_days - 1), to_date
                )

                # Prepare payload for historical data API
                payload = {
                    "exchange": exchange,
                    "symboltoken": token,
                    "interval": self.timeframe_map[interval],
                    "fromdate": current_start.strftime("%Y-%m-%d %H:%M"),
                    "todate": current_end.strftime("%Y-%m-%d %H:%M"),
                }
                logger.debug(
                    f"Debug - Fetching chunk from {current_start} to {current_end}"
                )
                logger.debug(f"Debug - API Payload: {payload}")

                try:
                    response = get_api_response(
                        "/rest/secure/angelbroking/historical/v1/getCandleData",
                        self.auth_token,
                        "POST",
                        payload,
                    )
                    logger.info(
                        f"Debug - API Response Status: {response.get('status')}"
                    )

                    # Check if response is empty or invalid
                    if not response:
                        logger.debug(
                            f"Debug - Empty response for chunk {current_start} to {current_end}"
                        )
                        current_start = current_end + timedelta(days=1)
                        continue

                    if not response.get("status"):
                        logger.info(
                            f"Debug - Error response: {response.get('message', 'Unknown error')}"
                        )
                        current_start = current_end + timedelta(days=1)
                        continue

                except Exception as chunk_error:
                    logger.error(
                        f"Debug - Error fetching chunk {current_start} to {current_end}: {str(chunk_error)}"
                    )
                    current_start = current_end + timedelta(days=1)
                    continue

                if not response.get("status"):
                    raise Exception(
                        f"Error from Angel API: {response.get('message', 'Unknown error')}"
                    )

                # Extract candle data and create DataFrame
                data = response.get("data", [])
                if data:
                    chunk_df = pd.DataFrame(
                        data,
                        columns=["timestamp", "open", "high", "low", "close", "volume"],
                    )
                    dfs.append(chunk_df)
                    logger.debug(f"Debug - Received {len(data)} candles for chunk")
                else:
                    logger.debug("Debug - No data received for chunk")

                # Move to next chunk
                current_start = current_end + timedelta(days=1)

                # Rate limit delay between chunks (0.5 seconds)
                if current_start <= to_date:
                    time.sleep(0.5)

            # If no data was found, return empty DataFrame
            if not dfs:
                logger.debug("Debug - No data received from API")
                return pd.DataFrame(
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                )

            # Combine all chunks
            df = pd.concat(dfs, ignore_index=True)

            # Convert timestamp to datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # For daily timeframe, convert UTC to IST by adding 5 hours and 30 minutes
            if interval == "D":
                df["timestamp"] = df["timestamp"] + pd.Timedelta(hours=5, minutes=30)

            # Convert timestamp to Unix epoch
            df["timestamp"] = (
                df["timestamp"].astype("int64") // 10**9
            )  # Convert to Unix epoch

            # Ensure numeric columns and proper order
            numeric_columns = ["open", "high", "low", "close", "volume"]
            df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)

            # Sort by timestamp and remove duplicates
            df = (
                df.sort_values("timestamp")
                .drop_duplicates(subset=["timestamp"])
                .reset_index(drop=True)
            )

            # Always fetch OI data for F&O contracts
            if exchange in ["NFO", "BFO", "CDS", "MCX"]:
                try:
                    oi_df = self.get_oi_history(
                        symbol, exchange, interval, start_date, end_date
                    )
                    if not oi_df.empty:
                        # Merge OI data with candle data
                        df = pd.merge(df, oi_df, on="timestamp", how="left")
                        # Fill any missing OI values with 0
                        df["oi"] = df["oi"].fillna(0).astype(int)
                    else:
                        # Add empty OI column if no data available
                        df["oi"] = 0
                except Exception as oi_error:
                    logger.error(f"Debug - Error fetching OI data: {str(oi_error)}")
                    # Add empty OI column on error
                    df["oi"] = 0

            # Reorder columns to match REST API format
            if "oi" in df.columns:
                df = df[["close", "high", "low", "open", "timestamp", "volume", "oi"]]
            else:
                # Add OI column with zeros if not present
                df["oi"] = 0
                df = df[["close", "high", "low", "open", "timestamp", "volume", "oi"]]

            return df

        except Exception as e:
            logger.error(f"Debug - Error: {str(e)}")
            raise Exception(f"Error fetching historical data: {str(e)}")

    def get_oi_history(
        self, symbol: str, exchange: str, interval: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Retrieve historical open interest (OI) data for a symbol between the given start and end dates at the specified interval.
        
        Parameters:
            symbol (str): Trading symbol.
            exchange (str): Exchange identifier (e.g., "NFO", "BFO", "CDS", "MCX").
            interval (str): Candle interval key (one of "1m", "3m", "5m", "10m", "15m", "30m", "1h", "D").
            start_date (str): Start date in "YYYY-MM-DD" format.
            end_date (str): End date in "YYYY-MM-DD" format.
        
        Returns:
            pd.DataFrame: DataFrame with columns `["timestamp", "oi"]` where `timestamp` is a Unix epoch (seconds) and `oi` is numeric.
        """
        try:
            # Get token for the symbol
            token = get_token(symbol, exchange)

            # Convert dates to datetime objects
            from_date = pd.to_datetime(start_date)
            to_date = pd.to_datetime(end_date)

            # Set start time to 00:00 for the start date
            from_date = from_date.replace(hour=0, minute=0)

            # If end_date is today, set the end time to current time
            current_time = pd.Timestamp.now()
            if to_date.date() == current_time.date():
                to_date = current_time.replace(second=0, microsecond=0)
            else:
                # For past dates, set end time to 23:59
                to_date = to_date.replace(hour=23, minute=59)

            # Initialize empty list to store DataFrames
            dfs = []

            # Set chunk size based on interval (same as candle data)
            interval_limits = {
                "1m": 30,  # ONE_MINUTE
                "3m": 60,  # THREE_MINUTE
                "5m": 100,  # FIVE_MINUTE
                "10m": 100,  # TEN_MINUTE
                "15m": 200,  # FIFTEEN_MINUTE
                "30m": 200,  # THIRTY_MINUTE
                "1h": 400,  # ONE_HOUR
                "D": 2000,  # ONE_DAY
            }

            chunk_days = interval_limits.get(interval)
            if not chunk_days:
                raise Exception(f"Interval '{interval}' not supported for OI data")

            # Process data in chunks
            current_start = from_date
            while current_start <= to_date:
                # Calculate chunk end date
                current_end = min(
                    current_start + timedelta(days=chunk_days - 1), to_date
                )

                # Prepare payload for OI data API
                payload = {
                    "exchange": exchange,
                    "symboltoken": token,
                    "interval": self.timeframe_map[interval],
                    "fromdate": current_start.strftime("%Y-%m-%d %H:%M"),
                    "todate": current_end.strftime("%Y-%m-%d %H:%M"),
                }

                try:
                    response = get_api_response(
                        "/rest/secure/angelbroking/historical/v1/getOIData",
                        self.auth_token,
                        "POST",
                        payload,
                    )

                    if not response or not response.get("status"):
                        logger.debug(
                            f"Debug - No OI data for chunk {current_start} to {current_end}"
                        )
                        current_start = current_end + timedelta(days=1)
                        continue

                except Exception as chunk_error:
                    logger.error(f"Debug - Error fetching OI chunk: {str(chunk_error)}")
                    current_start = current_end + timedelta(days=1)
                    continue

                # Extract OI data and create DataFrame
                data = response.get("data", [])
                if data:
                    chunk_df = pd.DataFrame(data)
                    # Rename 'time' to 'timestamp' for consistency
                    chunk_df.rename(columns={"time": "timestamp"}, inplace=True)
                    dfs.append(chunk_df)

                # Move to next chunk
                current_start = current_end + timedelta(days=1)

                # Rate limit delay between chunks (0.5 seconds)
                if current_start <= to_date:
                    time.sleep(0.5)

            # If no data was found, return empty DataFrame
            if not dfs:
                return pd.DataFrame(columns=["timestamp", "oi"])

            # Combine all chunks
            df = pd.concat(dfs, ignore_index=True)

            # Convert timestamp to datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # For daily timeframe, convert UTC to IST by adding 5 hours and 30 minutes
            if interval == "D":
                df["timestamp"] = df["timestamp"] + pd.Timedelta(hours=5, minutes=30)

            # Convert timestamp to Unix epoch
            df["timestamp"] = df["timestamp"].astype("int64") // 10**9

            # Ensure oi column is numeric
            df["oi"] = pd.to_numeric(df["oi"])

            # Sort by timestamp and remove duplicates
            df = (
                df.sort_values("timestamp")
                .drop_duplicates(subset=["timestamp"])
                .reset_index(drop=True)
            )

            return df

        except Exception as e:
            logger.error(f"Debug - Error fetching OI data: {str(e)}")
            # Return empty DataFrame on error
            return pd.DataFrame(columns=["timestamp", "oi"])

    def get_depth(self, symbol: str, exchange: str) -> dict:
        """
        Retrieve market depth for a symbol on a specific exchange, including the top five bid and ask levels and summary metrics.
        
        Returns:
            dict: Market depth with keys:
                - bids (list): Five entries of {"price": number, "quantity": number} for top buy levels (padded with zeros).
                - asks (list): Five entries of {"price": number, "quantity": number} for top sell levels (padded with zeros).
                - high (number): Day high price.
                - low (number): Day low price.
                - ltp (number): Last traded price.
                - ltq (number): Last traded quantity.
                - open (number): Opening price.
                - prev_close (number): Previous close price.
                - volume (number): Trade volume.
                - oi (number): Open interest.
                - totalbuyqty (number): Total buy quantity.
                - totalsellqty (number): Total sell quantity.
        
        Raises:
            Exception: If the API returns an error, no depth data is received, or other failures occur while fetching or parsing market depth.
        """
        try:
            # Convert symbol to broker format and get token
            #br_symbol = get_br_symbol(symbol, exchange)
            token = get_token(symbol, exchange)

            if exchange == "NSE_INDEX":
                exchange = "NSE"
            elif exchange == "BSE_INDEX":
                exchange = "BSE"
            elif exchange == "MCX_INDEX":
                exchange = "MCX"

            # Prepare payload for market depth API
            payload = {"mode": "FULL", "exchangeTokens": {exchange: [token]}}

            response = get_api_response(
                "/rest/secure/angelbroking/market/v1/quote/",
                self.auth_token,
                "POST",
                payload,
            )

            if not response.get("status"):
                raise Exception(
                    f"Error from Angel API: {response.get('message', 'Unknown error')}"
                )

            # Extract depth data
            fetched_data = response.get("data", {}).get("fetched", [])
            if not fetched_data:
                raise Exception("No depth data received")

            quote = fetched_data[0]
            depth = quote.get("depth", {})

            # Format bids and asks with exactly 5 entries each
            bids = []
            asks = []

            # Process buy orders (top 5)
            buy_orders = depth.get("buy", [])
            for i in range(5):  # Ensure exactly 5 entries
                if i < len(buy_orders):
                    bid = buy_orders[i]
                    bids.append(
                        {
                            "price": bid.get("price", 0),
                            "quantity": bid.get("quantity", 0),
                        }
                    )
                else:
                    bids.append({"price": 0, "quantity": 0})

            # Process sell orders (top 5)
            sell_orders = depth.get("sell", [])
            for i in range(5):  # Ensure exactly 5 entries
                if i < len(sell_orders):
                    ask = sell_orders[i]
                    asks.append(
                        {
                            "price": ask.get("price", 0),
                            "quantity": ask.get("quantity", 0),
                        }
                    )
                else:
                    asks.append({"price": 0, "quantity": 0})

            # Return depth data in common format matching REST API response
            return {
                "bids": bids,
                "asks": asks,
                "high": quote.get("high", 0),
                "low": quote.get("low", 0),
                "ltp": quote.get("ltp", 0),
                "ltq": quote.get("lastTradeQty", 0),
                "open": quote.get("open", 0),
                "prev_close": quote.get("close", 0),
                "volume": quote.get("tradeVolume", 0),
                "oi": quote.get("opnInterest", 0),
                "totalbuyqty": quote.get("totBuyQuan", 0),
                "totalsellqty": quote.get("totSellQuan", 0),
            }

        except Exception as e:
            raise Exception(f"Error fetching market depth: {str(e)}")