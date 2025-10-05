"""
Unified Broker API for India Stocks
Provides a single interface for all 24 brokers from OpenAlgo
"""

import importlib
from typing import Dict, Any, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class BrokerError(Exception):
    """Base exception for broker operations"""

    pass


class AuthenticationError(BrokerError):
    """Authentication failed"""

    pass


class OrderError(BrokerError):
    """Order operation failed"""

    pass


class DataError(BrokerError):
    """Data fetching failed"""

    pass


class Broker:
    """
    Unified Broker API - Single interface for all Indian stock brokers

    Supports 24+ brokers:
    - AngelOne, Zerodha, Upstox, Dhan, Fyers, Groww, Kotak,
    - AliceBlue, Flattrade, Shoonya, 5Paisa, Firstock, and more

    Example:
        >>> broker = Broker("angelone")
        >>> broker.authenticate({
        ...     "user_id": "A1234",
        ...     "pin": "1234",
        ...     "totp_secret": "BASE32SECRET",
        ...     "api_key": "your_key"
        ... })
        >>> positions = broker.get_positions()
        >>> candles = broker.get_historical_data("RELIANCE", "NSE", "1m", start_date, end_date)
    """

    # Supported brokers
    SUPPORTED_BROKERS = [
        "angel",
        "angelone",
        "zerodha",
        "upstox",
        "dhan",
        "fyers",
        "groww",
        "kotak",
        "aliceblue",
        "flattrade",
        "shoonya",
        "fivepaisa",
        "5paisa",
        "firstock",
        "compositedge",
        "definedge",
        "dhan_sandbox",
        "fivepaisaxts",
        "ibulls",
        "iifl",
        "indmoney",
        "paytm",
        "pocketful",
        "tradejini",
        "wisdom",
        "zebu",
    ]

    def __init__(self, broker_name: str):
        """
        Initialize broker instance

        Args:
            broker_name: Name of the broker (e.g., "angelone", "zerodha")

        Raises:
            ValueError: If broker is not supported
        """
        self.broker_name = self._normalize_broker_name(broker_name)

        if self.broker_name not in self.SUPPORTED_BROKERS:
            raise ValueError(
                f"Broker '{broker_name}' not supported. "
                f"Supported brokers: {', '.join(self.SUPPORTED_BROKERS)}"
            )

        self._auth_token = None
        self._feed_token = None
        self._api_key = None
        self._credentials = {}
        self._data_manager = None

        # Dynamically load broker modules
        self._broker_module = self._load_broker_module()

        logger.info(f"Initialized {self.broker_name} broker")

    def _normalize_broker_name(self, name: str) -> str:
        """Normalize broker name"""
        name = name.lower().strip()
        # Handle aliases
        if name in ["angelone", "angelbroking"]:
            return "angel"
        elif name == "5paisa":
            return "fivepaisa"
        return name

    def _load_broker_module(self) -> Any:
        """Dynamically load broker module"""
        try:
            # Import the broker's API modules
            auth_module = importlib.import_module(
                f"india_stocks_api.brokers.{self.broker_name}.api.auth_api"
            )
            order_module = importlib.import_module(
                f"india_stocks_api.brokers.{self.broker_name}.api.order_api"
            )
            data_module = importlib.import_module(
                f"india_stocks_api.brokers.{self.broker_name}.api.data"
            )

            return {
                "auth": auth_module,
                "orders": order_module,
                "data": data_module,
            }
        except ImportError as e:
            raise BrokerError(
                f"Failed to load broker module for {self.broker_name}: {e}"
            )

    def authenticate(self, credentials: Dict[str, Any]) -> bool:
        """
        Authenticate with the broker

        Args:
            credentials: Broker-specific credentials
                Common keys: user_id, pin, totp_secret, api_key, api_secret

        Returns:
            bool: True if authentication successful

        Raises:
            AuthenticationError: If authentication fails

        Example:
            >>> broker.authenticate({
            ...     "user_id": "A1234",
            ...     "pin": "1234",
            ...     "totp_secret": "BASE32SECRET",
            ...     "api_key": "your_key"
            ... })
        """
        try:
            self._credentials = credentials
            self._api_key = credentials.get("api_key")

            # Call broker-specific authentication
            if self.broker_name == "angel":
                auth_func = getattr(self._broker_module["auth"], "authenticate_broker")

                # Generate TOTP
                totp_code = self._generate_totp(credentials.get("totp_secret", ""))

                # Authenticate
                auth_token, feed_token, error = auth_func(
                    clientcode=credentials["user_id"],
                    broker_pin=credentials["pin"],
                    totp_code=totp_code,
                )

                if auth_token:
                    self._auth_token = auth_token
                    self._feed_token = feed_token

                    # Initialize data manager
                    data_class = getattr(self._broker_module["data"], "BrokerData")
                    self._data_manager = data_class(auth_token)

                    logger.info(f"Successfully authenticated with {self.broker_name}")
                    return True
                else:
                    raise AuthenticationError(f"Authentication failed: {error}")

            elif self.broker_name == "zerodha":
                # Zerodha uses request_token flow
                auth_func = getattr(self._broker_module["auth"], "authenticate_broker")

                request_token = credentials.get("request_token")
                if not request_token:
                    raise AuthenticationError(
                        "Zerodha requires 'request_token' in credentials"
                    )

                auth_token, error = auth_func(request_token)

                if auth_token:
                    self._auth_token = auth_token

                    # Initialize data manager
                    data_class = getattr(self._broker_module["data"], "BrokerData")
                    self._data_manager = data_class(auth_token)

                    logger.info(f"Successfully authenticated with {self.broker_name}")
                    return True
                else:
                    raise AuthenticationError(f"Authentication failed: {error}")

            else:
                raise BrokerError(
                    f"Authentication not yet implemented for {self.broker_name}"
                )

        except Exception as e:
            logger.error(f"Authentication error for {self.broker_name}: {e}")
            raise AuthenticationError(str(e))

    def _generate_totp(self, secret: str) -> str:
        """Generate TOTP code"""
        try:
            import pyotp

            totp = pyotp.TOTP(secret.replace(" ", ""))
            return totp.now()
        except Exception as e:
            logger.error(f"TOTP generation error: {e}")
            return ""

    def place_order(
        self,
        symbol: str,
        exchange: str,
        side: str,
        quantity: int,
        order_type: str = "MARKET",
        price: float = 0.0,
        product: str = "MIS",
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Place an order

        Args:
            symbol: Trading symbol (e.g., "RELIANCE")
            exchange: Exchange (e.g., "NSE", "BSE")
            side: "BUY" or "SELL"
            quantity: Order quantity
            order_type: "MARKET", "LIMIT", "SL", "SL-M"
            price: Price (for LIMIT orders)
            product: Product type - "MIS" (intraday), "CNC" (delivery), "NRML" (carry forward)
            **kwargs: Additional broker-specific parameters

        Returns:
            dict: Order response with order_id, status, message

        Example:
            >>> order = broker.place_order("RELIANCE", "NSE", "BUY", 10, "MARKET", product="MIS")
            >>> print(order["order_id"])
        """
        if not self._auth_token:
            raise AuthenticationError("Not authenticated. Call authenticate() first.")

        try:
            place_order_func = getattr(self._broker_module["orders"], "place_order_api")

            # Prepare order data
            order_data = {
                "apikey": self._api_key,
                "symbol": symbol,
                "exchange": exchange,
                "action": side.upper(),
                "quantity": str(quantity),
                "pricetype": order_type,
                "product": product,
                "price": str(price),
                "trigger_price": str(kwargs.get("trigger_price", 0)),
                **kwargs,
            }

            # Place order
            response, response_data, order_id = place_order_func(
                order_data, self._auth_token
            )

            return {
                "order_id": order_id,
                "status": "SUCCESS" if order_id else "FAILED",
                "message": response_data.get("message", ""),
                "raw_response": response_data,
            }

        except Exception as e:
            logger.error(f"Order placement error: {e}")
            raise OrderError(str(e))

    def get_positions(self) -> List[Dict[str, Any]]:
        """
        Get current positions

        Returns:
            list: List of position dictionaries

        Example:
            >>> positions = broker.get_positions()
            >>> for pos in positions:
            ...     print(f"{pos['symbol']}: {pos['netqty']}")
        """
        if not self._auth_token:
            raise AuthenticationError("Not authenticated. Call authenticate() first.")

        try:
            get_positions_func = getattr(self._broker_module["orders"], "get_positions")
            positions_data = get_positions_func(self._auth_token)

            if positions_data.get("status") and positions_data.get("data"):
                return positions_data["data"]
            return []

        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            raise DataError(str(e))

    def get_order_book(self) -> List[Dict[str, Any]]:
        """
        Get order book (all orders)

        Returns:
            list: List of order dictionaries

        Example:
            >>> orders = broker.get_order_book()
            >>> for order in orders:
            ...     print(f"{order['orderid']}: {order['status']}")
        """
        if not self._auth_token:
            raise AuthenticationError("Not authenticated. Call authenticate() first.")

        try:
            get_orderbook_func = getattr(
                self._broker_module["orders"], "get_order_book"
            )
            orderbook_data = get_orderbook_func(self._auth_token)

            if orderbook_data.get("status") and orderbook_data.get("data"):
                return orderbook_data["data"]
            return []

        except Exception as e:
            logger.error(f"Error fetching order book: {e}")
            raise DataError(str(e))

    def get_trade_book(self) -> List[Dict[str, Any]]:
        """
        Get trade book (executed trades)

        Returns:
            list: List of trade dictionaries
        """
        if not self._auth_token:
            raise AuthenticationError("Not authenticated. Call authenticate() first.")

        try:
            get_tradebook_func = getattr(
                self._broker_module["orders"], "get_trade_book"
            )
            tradebook_data = get_tradebook_func(self._auth_token)

            if tradebook_data.get("status") and tradebook_data.get("data"):
                return tradebook_data["data"]
            return []

        except Exception as e:
            logger.error(f"Error fetching trade book: {e}")
            raise DataError(str(e))

    def get_holdings(self) -> List[Dict[str, Any]]:
        """
        Get holdings (long-term positions)

        Returns:
            list: List of holding dictionaries
        """
        if not self._auth_token:
            raise AuthenticationError("Not authenticated. Call authenticate() first.")

        try:
            get_holdings_func = getattr(self._broker_module["orders"], "get_holdings")
            holdings_data = get_holdings_func(self._auth_token)

            if holdings_data.get("status") and holdings_data.get("data"):
                return holdings_data["data"]
            return []

        except Exception as e:
            logger.error(f"Error fetching holdings: {e}")
            raise DataError(str(e))

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        Cancel an order

        Args:
            order_id: Order ID to cancel

        Returns:
            dict: Cancellation response
        """
        if not self._auth_token:
            raise AuthenticationError("Not authenticated. Call authenticate() first.")

        try:
            cancel_order_func = getattr(self._broker_module["orders"], "cancel_order")
            response, status_code = cancel_order_func(order_id, self._auth_token)

            return {
                "status": "SUCCESS" if status_code == 200 else "FAILED",
                "message": response.get("message", ""),
                "raw_response": response,
            }

        except Exception as e:
            logger.error(f"Error cancelling order: {e}")
            raise OrderError(str(e))

    def get_historical_data(
        self,
        symbol: str,
        exchange: str,
        interval: str,
        from_date: datetime,
        to_date: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Get historical OHLCV data

        Args:
            symbol: Trading symbol (e.g., "RELIANCE")
            exchange: Exchange (e.g., "NSE", "BSE")
            interval: Candle interval - "1m", "3m", "5m", "15m", "30m", "1h", "D"
            from_date: Start date
            to_date: End date

        Returns:
            list: List of candle dictionaries with OHLCV data

        Example:
            >>> from datetime import datetime, timedelta
            >>> end = datetime.now()
            >>> start = end - timedelta(days=1)
            >>> candles = broker.get_historical_data("RELIANCE", "NSE", "1m", start, end)
            >>> for candle in candles:
            ...     print(f"{candle['timestamp']}: O={candle['open']}, C={candle['close']}")
        """
        if not self._data_manager:
            raise AuthenticationError("Not authenticated. Call authenticate() first.")

        try:
            # Use data manager's get_history method
            df = self._data_manager.get_history(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                start_date=from_date.strftime("%Y-%m-%d"),
                end_date=to_date.strftime("%Y-%m-%d"),
            )

            # Convert DataFrame to list of dictionaries
            if not df.empty:
                return df.to_dict("records")
            return []

        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            raise DataError(str(e))

    def get_quotes(self, symbol: str, exchange: str) -> Dict[str, Any]:
        """
        Get live quotes for a symbol

        Args:
            symbol: Trading symbol (e.g., "RELIANCE")
            exchange: Exchange (e.g., "NSE", "BSE")

        Returns:
            dict: Quote data with LTP, bid, ask, volume, etc.

        Example:
            >>> quote = broker.get_quotes("RELIANCE", "NSE")
            >>> print(f"LTP: {quote['ltp']}, Volume: {quote['volume']}")
        """
        if not self._data_manager:
            raise AuthenticationError("Not authenticated. Call authenticate() first.")

        try:
            return self._data_manager.get_quotes(symbol, exchange)

        except Exception as e:
            logger.error(f"Error fetching quotes: {e}")
            raise DataError(str(e))

    def get_market_depth(self, symbol: str, exchange: str) -> Dict[str, Any]:
        """
        Get market depth (order book) for a symbol

        Args:
            symbol: Trading symbol (e.g., "RELIANCE")
            exchange: Exchange (e.g., "NSE", "BSE")

        Returns:
            dict: Market depth with bids, asks, and other details
        """
        if not self._data_manager:
            raise AuthenticationError("Not authenticated. Call authenticate() first.")

        try:
            return self._data_manager.get_depth(symbol, exchange)

        except Exception as e:
            logger.error(f"Error fetching market depth: {e}")
            raise DataError(str(e))

    @property
    def is_authenticated(self) -> bool:
        """Check if broker is authenticated"""
        return self._auth_token is not None

    def __repr__(self) -> str:
        return f"Broker('{self.broker_name}', authenticated={self.is_authenticated})"
