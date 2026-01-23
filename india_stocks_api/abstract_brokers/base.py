from abc import ABC, abstractmethod
from typing import Any, Dict, List

class BaseBroker(ABC):
    """
    An abstract base class that defines the common interface for all broker integrations.
    """

    @abstractmethod
    def logout(self):
        """
        Logs out the user and terminates the session.

        :raises NotImplementedError: This is an abstract method.
        """
        raise NotImplementedError

    @abstractmethod
    def get_order_book(self):
        """
        Retrieves the user's order book for the current day.

        :raises NotImplementedError: This is an abstract method.
        """
        raise NotImplementedError

    @abstractmethod
    def get_trade_book(self):
        """
        Retrieves the user's trade book for the current day.

        :raises NotImplementedError: This is an abstract method.
        """
        raise NotImplementedError

    @abstractmethod
    def get_positions(self):
        """
        Retrieves the user's open positions.

        :raises NotImplementedError: This is an abstract method.
        """
        raise NotImplementedError

    @abstractmethod
    def get_holdings(self):
        """
        Retrieves the user's equity holdings.

        :raises NotImplementedError: This is an abstract method.
        """
        raise NotImplementedError

    @abstractmethod
    def place_order(self, order_details):
        """
        Places a new order.

        :param order_details: A dictionary containing the order specifics such as
                             symbol, quantity, price, transaction_type, etc.
        :return: The order ID of the placed order.
        :raises NotImplementedError: This is an abstract method.
        """
        raise NotImplementedError

    @abstractmethod
    def modify_order(self, order_id, order_details):
        """
        Modifies an existing order.

        :param order_id: The ID of the order to be modified.
        :param order_details: A dictionary containing the modified order parameters.
        :return: The order ID of the modified order.
        :raises NotImplementedError: This is an abstract method.
        """
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, order_id):
        """
        Cancels an existing order.

        :param order_id: The ID of the order to be cancelled.
        :return: The order ID of the cancelled order.
        :raises NotImplementedError: This is an abstract method.
        """
        raise NotImplementedError

    @abstractmethod
    def get_quotes(self, symbol: str, exchange: str):
        """Retrieves real-time quote for a symbol."""
        pass

    @abstractmethod
    def get_history(self, instruments: str, interval: str, start_date: str, end_date: str):
        """Fetches historical OHLCV data."""
        pass

    @abstractmethod
    def get_depth(self, symbol: str, exchange: str):
        """Retrieves Level 2 Market Depth (Snapquote)."""
        pass

    # --- NEW: Risk & Compliance ---
    @abstractmethod
    def calculate_margin(self, positions: List[Dict[str, Any]]):
        """Calculates required margin for a basket of trades."""
        pass