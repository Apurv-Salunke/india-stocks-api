"""
Base provider class for data providers
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class BaseProvider(ABC):
    """Abstract base class for data providers"""

    def __init__(self, symbol_db, broker_name: str):
        self.db = symbol_db
        self.broker_name = broker_name
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def fetch_equity_data(self) -> List[Dict[str, Any]]:
        """Fetch equity instruments data"""
        pass

    @abstractmethod
    def fetch_fno_data(self) -> List[Dict[str, Any]]:
        """Fetch F&O instruments data"""
        pass

    @abstractmethod
    def fetch_commodity_data(self) -> List[Dict[str, Any]]:
        """Fetch commodity instruments data"""
        pass

    @abstractmethod
    def fetch_currency_data(self) -> List[Dict[str, Any]]:
        """Fetch currency instruments data"""
        pass

    def sync_all_instruments(self) -> Dict[str, int]:
        """Sync all instrument types from provider"""
        results = {"equity": 0, "fno": 0, "commodity": 0, "currency": 0}

        try:
            self.logger.info(f"Starting sync for {self.broker_name}")

            # Sync each instrument type
            results["equity"] = self.sync_equity_instruments()
            results["fno"] = self.sync_fno_instruments()
            results["commodity"] = self.sync_commodity_instruments()
            results["currency"] = self.sync_currency_instruments()

            self.logger.info(f"Sync completed for {self.broker_name}: {results}")
            return results

        except Exception as e:
            self.logger.error(f"Error during sync for {self.broker_name}: {e}")
            raise

    def sync_equity_instruments(self) -> int:
        """Sync equity instruments (default implementation)"""
        try:
            equity_data = self.fetch_equity_data()
            return self._store_equity_data(equity_data)
        except Exception as e:
            self.logger.error(f"Error syncing equity instruments: {e}")
            return 0

    def sync_fno_instruments(self) -> int:
        """Sync F&O instruments (default implementation)"""
        try:
            fno_data = self.fetch_fno_data()
            return self._store_fno_data(fno_data)
        except Exception as e:
            self.logger.error(f"Error syncing F&O instruments: {e}")
            return 0

    def sync_commodity_instruments(self) -> int:
        """Sync commodity instruments (default implementation)"""
        try:
            commodity_data = self.fetch_commodity_data()
            return self._store_commodity_data(commodity_data)
        except Exception as e:
            self.logger.error(f"Error syncing commodity instruments: {e}")
            return 0

    def sync_currency_instruments(self) -> int:
        """Sync currency instruments (default implementation)"""
        try:
            currency_data = self.fetch_currency_data()
            return self._store_currency_data(currency_data)
        except Exception as e:
            self.logger.error(f"Error syncing currency instruments: {e}")
            return 0

    def _store_equity_data(self, equity_data: List[Dict[str, Any]]) -> int:
        """Store equity data in database (to be implemented by subclasses)"""
        raise NotImplementedError("Subclasses must implement _store_equity_data")

    def _store_fno_data(self, fno_data: List[Dict[str, Any]]) -> int:
        """Store F&O data in database (to be implemented by subclasses)"""
        raise NotImplementedError("Subclasses must implement _store_fno_data")

    def _store_commodity_data(self, commodity_data: List[Dict[str, Any]]) -> int:
        """Store commodity data in database (to be implemented by subclasses)"""
        raise NotImplementedError("Subclasses must implement _store_commodity_data")

    def _store_currency_data(self, currency_data: List[Dict[str, Any]]) -> int:
        """Store currency data in database (to be implemented by subclasses)"""
        raise NotImplementedError("Subclasses must implement _store_currency_data")

    def get_provider_info(self) -> Dict[str, Any]:
        """Get provider information"""
        return {
            "broker_name": self.broker_name,
            "provider_class": self.__class__.__name__,
            "supported_instruments": ["equity", "fno", "commodity", "currency"],
        }
