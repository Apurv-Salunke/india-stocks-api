"""
Instrument model for database
"""

from dataclasses import dataclass
from typing import Optional
from datetime import date, datetime


@dataclass
class Instrument:
    """Comprehensive instrument model"""

    standardized_symbol: str  # RELIANCE, GOLD, USDINR, etc.
    instrument_name: str  # Full instrument name
    exchange_id: int  # Foreign key to exchanges table
    category_id: int  # Foreign key to instrument_categories table
    subcategory_id: Optional[int] = (
        None  # Foreign key to instrument_subcategories table
    )

    # Common fields
    isin: Optional[str] = None  # For equity instruments
    sector: Optional[str] = None  # For equity
    industry: Optional[str] = None  # For equity
    market_cap: Optional[float] = None  # For equity
    face_value: Optional[float] = None  # For equity/bonds

    # Commodity specific
    commodity_type: Optional[str] = None  # METAL, ENERGY, AGRICULTURE
    commodity_unit: Optional[str] = None  # KG, TONNE, BARREL, etc.
    delivery_center: Optional[str] = None  # For commodities

    # Currency specific
    base_currency: Optional[str] = None  # USD, EUR, etc.
    quote_currency: Optional[str] = None  # INR, USD, etc.

    # Derivative specific
    underlying_symbol: Optional[str] = None  # For F&O instruments
    underlying_type: Optional[str] = None  # EQUITY, INDEX, COMMODITY, CURRENCY

    # Status and metadata
    is_active: bool = True
    listing_date: Optional[date] = None
    expiry_date: Optional[date] = None  # For derivatives
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()

        if not self.standardized_symbol:
            raise ValueError("Standardized symbol is required")
        if not self.instrument_name:
            raise ValueError("Instrument name is required")
        if not self.exchange_id:
            raise ValueError("Exchange ID is required")
        if not self.category_id:
            raise ValueError("Category ID is required")

    def get_instrument_type(self) -> str:
        """Determine instrument type based on category and other fields"""
        if self.category_id == 1:  # EQUITY
            return "EQUITY"
        elif self.category_id == 2:  # FUTURES
            if self.underlying_type == "INDEX":
                return "INDEX_FUTURE"
            elif self.underlying_type == "EQUITY":
                return "STOCK_FUTURE"
            elif self.underlying_type == "COMMODITY":
                return "COMMODITY_FUTURE"
            else:
                return "FUTURE"
        elif self.category_id == 3:  # OPTIONS
            if self.underlying_type == "INDEX":
                return "INDEX_OPTION"
            elif self.underlying_type == "EQUITY":
                return "STOCK_OPTION"
            elif self.underlying_type == "COMMODITY":
                return "COMMODITY_OPTION"
            else:
                return "OPTION"
        elif self.category_id == 4:  # COMMODITY
            return "COMMODITY"
        elif self.category_id == 5:  # CURRENCY
            return "CURRENCY"
        else:
            return "UNKNOWN"

    def is_derivative(self) -> bool:
        """Check if instrument is a derivative"""
        return self.category_id in [2, 3]  # FUTURES, OPTIONS

    def is_equity(self) -> bool:
        """Check if instrument is equity"""
        return self.category_id == 1  # EQUITY

    def is_commodity(self) -> bool:
        """Check if instrument is commodity"""
        return self.category_id == 4  # COMMODITY

    def is_currency(self) -> bool:
        """Check if instrument is currency"""
        return self.category_id == 5  # CURRENCY
