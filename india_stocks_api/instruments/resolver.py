"""
Instrument Resolver.
Resolves high-level Domain Objects to low-level Broker Tokens.
"""

from functools import singledispatchmethod
from typing import Any, Dict

from .database import InstrumentDB
from .models import Equity, Future, Option


class Resolver:
    def __init__(self, db: InstrumentDB):
        self.db = db

    def resolve(self, instrument) -> Dict[str, Any]:
        """
        Main entry point. Dispatches based on instrument type.
        """
        # Call the specific resolver
        return self._resolve_dispatch(instrument)

    @singledispatchmethod
    def _resolve_dispatch(self, instrument):
        raise TypeError(f"Cannot resolve instrument of type: {type(instrument)}")

    @_resolve_dispatch.register
    def _(self, instrument: Equity):
        result = self.db.lookup_token(symbol=instrument.symbol, exchange=instrument.exchange)
        if not result:
            raise ValueError(f"Equity not found: {instrument.symbol} on {instrument.exchange}")
        return self._to_dict(result)

    @_resolve_dispatch.register
    def _(self, instrument: Future):
        # Futures usually don't have strike/opt_type
        result = self.db.lookup_token(
            symbol=instrument.symbol, exchange=instrument.exchange, expiry=instrument.expiry, opt_type=None
        )
        if not result:
            raise ValueError(f"Future not found: {instrument.symbol} expiry {instrument.expiry}")
        return self._to_dict(result)

    @_resolve_dispatch.register
    def _(self, instrument: Option):
        result = self.db.lookup_token(
            symbol=instrument.symbol,
            exchange=instrument.exchange,
            expiry=instrument.expiry,
            strike=instrument.strike,
            opt_type=instrument.opt_type.value,  # Enum to string
        )
        if not result:
            raise ValueError(
                f"Option not found: {instrument.symbol} {instrument.expiry} {instrument.strike} {instrument.opt_type}"
            )
        return self._to_dict(result)

    def _to_dict(self, record):
        return {
            "token": record.token,
            "tradingsymbol": record.tradingsymbol,
            "exchange": record.exchange,
            "symbol": record.tradingsymbol,  # Use unique identifier for Adapter compatibility
            "lot_size": record.lot_size,
        }
