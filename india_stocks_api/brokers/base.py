from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, Type
from functools import singledispatchmethod

from ..instruments.models import Equity, Future, Option, Index
from ..constants import TransactionType, OrderType, ProductType, OrderValidity, CandleInterval
from ..internal import context

class BaseBroker(ABC):
    """
    Common interface for all Stock Brokers.
    """
    _registry: Dict[str, Type["BaseBroker"]] = {}
    
    def __init_subclass__(cls, broker_name: str = None, **kwargs):
        """Auto-register subclasses when they're defined."""
        super().__init_subclass__(**kwargs)
        if broker_name:
            BaseBroker._registry[broker_name] = cls
    
    @classmethod
    def create(cls, broker_name: str, **kwargs) -> "BaseBroker":
        """Factory method to create broker instances."""
        if broker_name not in cls._registry:
            raise ValueError(f"Unknown broker: {broker_name}. Available: {list(cls._registry.keys())}")
        return cls._registry[broker_name](**kwargs)

    @abstractmethod
    def authenticate(self) -> bool:
        """Authenticate with the broker (e.g. Login)."""
        ...

    @abstractmethod
    def place_order(
        self,
        instrument: Equity | Future | Option,
        transaction_type: TransactionType,
        quantity: int,
        order_type: OrderType = OrderType.MARKET,
        product_type: ProductType = ProductType.INTRADAY,
        price: float = 0.0,
        trigger_price: float = 0.0,
        validity: OrderValidity = OrderValidity.DAY,
        **kwargs
    ) -> dict:
        """Place a new order."""
        ...

    @abstractmethod
    def get_positions(self) -> list:
        """Get current open positions."""
        ...

    @abstractmethod
    def get_funds(self) -> dict:
        """Get funds/margin info."""
        ...

    @abstractmethod
    def get_history(self, instrument: Equity | Future | Option | Index, 
                   start_date: str, end_date: str, interval: CandleInterval) -> Any:
        """
        Get historical candles.
        """
        ...

    @abstractmethod
    def get_depth(self, instrument: Equity | Future | Option | Index) -> dict:
        """Get market depth (Level 2 data)."""
        ...
    
    @abstractmethod
    def get_quote(self, instrument: Equity | Future | Option | Index) -> dict:
        """Get current quote for an instrument."""
        ...

    @abstractmethod
    def get_holdings(self) -> list:
        """Get long term holdings."""
        ...

    @abstractmethod
    def get_orders(self) -> list:
        """Get order book."""
        ...

    @abstractmethod
    def modify_order(self, order_id: str, price: float = 0.0, trigger_price: float = 0.0, quantity: int = 0) -> dict:
        """Modify an open order."""
        ...

    @abstractmethod
    def cancel_order(self, order_id: str) -> dict:
        """Cancel an open order."""
        ...

    @abstractmethod
    def get_profile(self) -> dict:
        """Get user profile details."""
        ...

    @abstractmethod
    def get_trades(self) -> list:
        """Get executed trades."""
        ...

    @abstractmethod
    def get_order_details(self, order_id: str) -> dict:
        """Get detailed status and history for a specific order."""
        ...

    @abstractmethod
    def cancel_all_orders(self) -> dict:
        """Cancel all open/pending orders."""
        ...

    @abstractmethod
    def square_off_all_positions(self) -> dict:
        """Exit all open positions at market price."""
        ...

    # --- GTT Orders ---
    
    @abstractmethod
    def create_gtt(self, instrument: Equity | Future | Option, transaction_type: TransactionType, 
                  quantity: int, trigger_price: float, price: float, 
                  product_type: ProductType = ProductType.DELIVERY, time_period: int = 365) -> dict:
        """Create a new GTT rule."""
        ...

    @abstractmethod
    def modify_gtt(self, id: int, instrument: Equity | Future | Option, 
                  quantity: int, trigger_price: float, price: float) -> dict:
        """Modify an existing GTT rule."""
        ...

    @abstractmethod
    def cancel_gtt(self, id: int, instrument: Equity | Future | Option) -> dict:
        """Cancel an active GTT rule."""
        ...

    @abstractmethod
    def get_gtt_list(self, status: list = ["FOR_SETTLEMENT", "CANCELLED", "TRIGGERED"]) -> list:
        """Get list of GTT rules."""
        ...

    @abstractmethod
    def get_gtt_details(self, id: int) -> dict:
        """Get details for a specific GTT rule."""
        ...

    # --- Convenience Helpers ---

    def get_executed_orders(self) -> list:
        """Get only executed/filled orders."""
        all_orders = self.get_orders()
        return [o for o in all_orders if o.get('status') == 'complete']

    def get_pending_orders(self) -> list:
        """Get only pending/open orders."""
        all_orders = self.get_orders()
        return [o for o in all_orders if o.get('status') in ['open', 'trigger pending', 'validation pending']]

    # --- Resolution Logic (Common) ---
    
    @singledispatchmethod
    def _resolve_instrument(self, instrument) -> dict:
        """
        Resolve instrument to broker-specific details.
        Returns a dict containing 'symbol', 'exchange', 'token', etc.
        """
        raise TypeError(f"Cannot resolve instrument of type: {type(instrument)}")
    
    @_resolve_instrument.register
    def _(self, instrument: Equity) -> dict:
        # Standard Resolution using context shims (which use InstrumentDB)
        return {
            "symbol": instrument.symbol,
            "tradingsymbol": context.get_tradingsymbol(instrument.symbol, instrument.exchange),
            "exchange": instrument.exchange,
            "token": context.get_token(instrument.symbol, instrument.exchange) or "DUMMY"
        }
    
    @_resolve_instrument.register
    def _(self, instrument: Future) -> dict:
        # For Futures: standard OpenAlgo symbol format
        oa_symbol = f"{instrument.symbol}{instrument.expiry.strftime('%d%b%y').upper()}FUT"
        return {
            "symbol": oa_symbol,
            "tradingsymbol": context.get_tradingsymbol(oa_symbol, instrument.exchange),
            "exchange": instrument.exchange,
            "token": context.get_token(oa_symbol, instrument.exchange) or "DUMMY"
        }

    @_resolve_instrument.register
    def _(self, instrument: Option) -> dict:
        # For Options: standard OpenAlgo symbol format
        oa_symbol = f"{instrument.symbol}{instrument.expiry.strftime('%d%b%y').upper()}{int(instrument.strike)}{instrument.opt_type.value}"
        return {
            "symbol": oa_symbol,
            "tradingsymbol": context.get_tradingsymbol(oa_symbol, instrument.exchange),
            "exchange": instrument.exchange,
            "token": context.get_token(oa_symbol, instrument.exchange) or "DUMMY"
        }

    @_resolve_instrument.register
    def _(self, instrument: Index) -> dict:
        return {
            "symbol": instrument.symbol,
            "tradingsymbol": context.get_tradingsymbol(instrument.symbol, instrument.exchange),
            "exchange": instrument.exchange,
            "token": context.get_token(instrument.symbol, instrument.exchange) or "DUMMY"
        }
