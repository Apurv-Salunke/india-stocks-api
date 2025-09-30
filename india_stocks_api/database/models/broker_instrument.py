"""
Broker instrument mapping model for database
"""

from dataclasses import dataclass
from typing import Optional
from datetime import date, datetime
from .enums import OptionType


@dataclass
class BrokerInstrument:
    """Broker-specific instrument mapping"""

    instrument_id: int  # Foreign key to instruments table
    broker_name: str  # angelone, zerodha, etc.
    broker_symbol: str  # Broker's symbol format
    broker_token: str  # Broker's internal token
    tick_size: float  # Minimum price movement
    lot_size: int  # Minimum trading quantity

    # Optional fields with defaults
    broker_instrument_key: Optional[str] = None  # Alternative identifier
    margin_percentage: Optional[float] = None  # Margin requirement
    expiry_date: Optional[date] = None  # For derivatives
    strike_price: Optional[float] = None  # For options
    option_type: Optional[str] = None  # CE/PE for options
    is_tradeable: bool = True  # Can be traded
    is_active: bool = True  # Active in broker system
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()

        if not self.instrument_id:
            raise ValueError("Instrument ID is required")
        if not self.broker_name:
            raise ValueError("Broker name is required")
        if not self.broker_symbol:
            raise ValueError("Broker symbol is required")
        if not self.broker_token:
            raise ValueError("Broker token is required")
        if self.tick_size <= 0:
            raise ValueError("Tick size must be positive")
        if self.lot_size <= 0:
            raise ValueError("Lot size must be positive")

    def is_option(self) -> bool:
        """Check if this is an option instrument"""
        return self.option_type is not None

    def is_call_option(self) -> bool:
        """Check if this is a call option"""
        return self.option_type == OptionType.CALL.value

    def is_put_option(self) -> bool:
        """Check if this is a put option"""
        return self.option_type == OptionType.PUT.value

    def is_future(self) -> bool:
        """Check if this is a future instrument"""
        return self.expiry_date is not None and self.option_type is None

    def get_unique_key(self) -> str:
        """Get unique key for this broker instrument"""
        key_parts = [
            self.broker_name,
            self.broker_symbol,
            str(self.expiry_date) if self.expiry_date else "",
            str(self.strike_price) if self.strike_price else "",
            self.option_type or "",
        ]
        return "|".join(key_parts)
