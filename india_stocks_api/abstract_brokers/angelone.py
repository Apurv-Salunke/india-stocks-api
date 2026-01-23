from algotrade.abstract_brokers.base import BaseBroker

# Import the specific API functions from the angelone implementation
from algotrade.internal.database.auth_db import generate_totp_code, store_broker_token
from algotrade.internal.openalgo_brokers.angelone.api import (
    auth_api,
    funds,
    order_api,
    margin_api,
)
from algotrade.internal.openalgo_brokers.angelone.api.data import BrokerData


class AngelOne(BaseBroker):
    """
    An implementation of the BaseBroker for Angel One.
    """

    def __init__(self, api_key, clientcode, password, totp):
        """
        Initializes the AngelOne broker instance and establishes a session.

        Args:
            api_key (str): The SmartAPI Key generated from the Angel One developer portal.
            clientcode (str): The Angel One User ID (e.g., 'A123456').
            password (str): The login password for the Angel One account.
            totp (str): The 6-digit Time-based One-Time Password (TOTP) from the
                       authenticator app or generated via a library like `pyotp`.

        Raises:
            Exception: If authentication fails due to invalid credentials or API errors.
        """
        self.auth_token = None
        self.feed_token = None
        self.api_key = api_key

        totp = generate_totp_code(totp)

        self.auth_token, self.feed_token, error = auth_api.authenticate_broker(
            api_key, clientcode, password, totp
        )
        
        store_broker_token(
            broker_name="angelone",
            auth_token=self.auth_token,
            feed_token=self.feed_token,
            client_id=clientcode,
            expires_in_hours=24,
        )
        if error:
            raise Exception(f"Angel One login failed: {error}")

        self.data_handler = BrokerData(self.auth_token)

    def logout(self):
        """
        Logs out the user from Angel One.
        Note: Angel One API does not have a dedicated logout endpoint.
        Session is invalidated by deleting the token.
        """
        self.auth_token = None
        self.feed_token = None
        # Here you could also call a token invalidation endpoint if one existed
        print("Logged out from Angel One.")

    def get_margin_data(self):
        """
        Retrieves the user's margin data from Angel One.
        """
        if not self.auth_token:
            raise Exception("User not logged in.")
        return funds.get_margin_data(self.api_key, self.auth_token)

    def get_order_book(self):
        """
        Retrieves the user's order book from Angel One.
        """
        if not self.auth_token:
            raise Exception("User not logged in.")
        return order_api.get_order_book(self.auth_token)

    def get_trade_book(self):
        """
        Retrieves the user's trade book from Angel One.
        """
        if not self.auth_token:
            raise Exception("User not logged in.")
        return order_api.get_trade_book(self.auth_token)

    def get_positions(self):
        """
        Retrieves the user's positions from Angel One.
        """
        if not self.auth_token:
            raise Exception("User not logged in.")
        return order_api.get_positions(self.auth_token)

    def get_holdings(self):
        """
        Retrieves the user's holdings from Angel One.
        """
        if not self.auth_token:
            raise Exception("User not logged in.")
        return order_api.get_holdings(self.auth_token)

    def place_order(self, order_details):
        """
        Places an order on Angel One.
        """
        if not self.auth_token:
            raise Exception("User not logged in.")
        _, _, order_id = order_api.place_order_api(order_details, self.auth_token)
        return order_id

    def modify_order(self, order_id, order_details):
        """
        Modifies an order on Angel One.
        """
        if not self.auth_token:
            raise Exception("User not logged in.")
        # Note: The existing order_api.modify_order takes 'data' which includes the order_id.
        # We might need to adjust the signature in the ABC or the implementation.
        # For now, we'll merge them.
        data = order_details.copy()
        data["orderid"] = order_id
        return order_api.modify_order(data, self.auth_token)

    def cancel_order(self, order_id):
        """
        Cancels an order on Angel One.
        """
        if not self.auth_token:
            raise Exception("User not logged in.")
        return order_api.cancel_order(order_id, self.auth_token)

    def calculate_margin(self, positions: list):
        """
        Calculates the required margin for a basket of positions.

        Args:
            positions: List of positions in OpenAlgo format.
                       Example: [{'symbol': 'SBIN', 'exchange': 'NSE', 'action': 'BUY', ...}]

        Returns:
            dict: Standardized margin response with required margin details.
        """
        if not self.auth_token:
            raise Exception("User not logged in.")

        # Call the margin calculation utility
        # This typically returns a tuple (raw_response, standardized_data)
        _, margin_data = margin_api.calculate_margin_api(
            api_key=self.api_key, positions=positions, auth=self.auth_token
        )

        return margin_data

    def get_quotes(self, symbol: str, exchange: str):
        """
        Retrieves real-time quotes for a specific symbol.
        """
        if not self.auth_token:
            raise Exception("User not logged in.")
        return self.data_handler.get_quotes(symbol, exchange)

    def get_history(self, instrument, interval, from_date, to_date):
        """
        Fetches historical OHLCV data for a specific instrument.
        Spreads the instrument object to get symbol and exchange.
        """
        if not self.auth_token:
            raise Exception("User not logged in.")

        # Extract parameters from the Instrument/Index object
        symbol = instrument.symbol
        exchange = instrument.exchange

        # Pass the individual parameters to the internal data handler
        return self.data_handler.get_history(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start_date=from_date,
            end_date=to_date,
        )

    def get_depth(self, symbol: str, exchange: str):
        """
        Retrieves market depth (Level 2 data).
        """
        if not self.auth_token:
            raise Exception("User not logged in.")
        return self.data_handler.get_depth(symbol, exchange)

    def get_oi_history(
        self, symbol: str, exchange: str, interval: str, start_date: str, end_date: str
    ):
        """
        Retrieves historical Open Interest (OI) data for a specific symbol.

        Args:
            symbol: Trading symbol.
            exchange: Exchange (e.g., NFO, BFO, CDS, MCX).
            interval: Candle interval (1m, 3m, 5m, 10m, 15m, 30m, 1h, D).
            start_date: Start date (YYYY-MM-DD).
            end_date: End date (YYYY-MM-DD).

        Returns:
            pd.DataFrame: Historical OI data with columns [timestamp, oi].
        """
        if not self.auth_token:
            raise Exception("User not logged in.")

        return self.data_handler.get_oi_history(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start_date=start_date,
            end_date=end_date,
        )
