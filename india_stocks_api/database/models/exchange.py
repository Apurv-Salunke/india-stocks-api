"""
Exchange model for database
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class ExchangeModel:
    """Exchange information model"""

    exchange_code: str  # NSE, BSE, MCX, etc.
    exchange_name: str  # Full exchange name
    country: str = "INDIA"
    currency: str = "INR"
    timezone: str = "Asia/Kolkata"
    trading_hours: Optional[str] = None  # JSON format for trading hours
    is_active: bool = True
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()

        if not self.exchange_code:
            raise ValueError("Exchange code is required")
        if not self.exchange_name:
            raise ValueError("Exchange name is required")

    def get_trading_hours_dict(self) -> dict:
        """Parse trading hours from JSON string"""
        import json

        if self.trading_hours:
            try:
                return json.loads(self.trading_hours)
            except json.JSONDecodeError:
                return {}
        return {}

    def set_trading_hours_dict(self, hours_dict: dict):
        """Set trading hours from dictionary"""
        import json

        self.trading_hours = json.dumps(hours_dict)
