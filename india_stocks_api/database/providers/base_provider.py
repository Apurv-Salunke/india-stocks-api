"""
Base provider class for data providers
"""

import json
import os
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import logging

logger = logging.getLogger(__name__)


class BaseProvider(ABC):
    """Abstract base class for data providers"""

    def __init__(self, symbol_db, broker_name: str, max_workers: int = 8):
        self.db = symbol_db
        self.broker_name = broker_name
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.max_workers = max_workers

        # Threading lock for database operations
        self._db_lock = threading.Lock()

        # Initialize instrument store for database operations
        from ..services.instrument_store import InstrumentStore

        self._instrument_store = InstrumentStore(str(symbol_db.db_path))

        # Cache configuration (to be set by subclasses)
        self.cache_file = None
        self.cache_validity_hours = 24

    @abstractmethod
    def fetch_equity_data(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Fetch equity instruments data"""
        pass

    @abstractmethod
    def fetch_fno_data(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Fetch F&O instruments data"""
        pass

    @abstractmethod
    def fetch_commodity_data(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Fetch commodity instruments data"""
        pass

    @abstractmethod
    def fetch_currency_data(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
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
        """Store equity data in database using InstrumentStore"""
        if len(equity_data) == 0:
            print("ℹ️  No equity instruments to store")
            return 0

        print(f"💾 Storing {len(equity_data):,} equity instruments...")

        try:
            total_stored = self._instrument_store.upsert_equities(equity_data)
            print(f"✅ Stored {total_stored:,} equity instruments successfully")
            return total_stored
        except Exception as e:
            self.logger.error(f"Error storing equity data: {e}")
            return 0

    def _store_fno_data(self, fno_data: List[Dict[str, Any]]) -> int:
        """Store F&O data in database using InstrumentStore"""
        if len(fno_data) == 0:
            print("ℹ️  No F&O instruments to store")
            return 0

        print(f"💾 Storing {len(fno_data):,} F&O instruments...")
        print("⏳ This may take a few minutes for large datasets...")

        try:
            total_stored = self._instrument_store.upsert_fno(fno_data)
            print(f"✅ Stored {total_stored:,} F&O instruments successfully")
            return total_stored
        except Exception as e:
            self.logger.error(f"Error storing F&O data: {e}")
            return 0

    def _store_commodity_data(self, commodity_data: List[Dict[str, Any]]) -> int:
        """Store commodity data in database using InstrumentStore"""
        if len(commodity_data) == 0:
            print("ℹ️  No commodity instruments to store")
            return 0

        print(f"💾 Storing {len(commodity_data):,} commodity instruments...")

        try:
            total_stored = self._instrument_store.upsert_commodities(commodity_data)
            print(f"✅ Stored {total_stored:,} commodity instruments successfully")
            return total_stored
        except Exception as e:
            self.logger.error(f"Error storing commodity data: {e}")
            return 0

    def _store_currency_data(self, currency_data: List[Dict[str, Any]]) -> int:
        """Store currency data in database using InstrumentStore"""
        if len(currency_data) == 0:
            print("ℹ️  No currency instruments to store")
            return 0

        print(f"💾 Storing {len(currency_data):,} currency instruments...")

        try:
            total_stored = self._instrument_store.upsert_currencies(currency_data)
            print(f"✅ Stored {total_stored:,} currency instruments successfully")
            return total_stored
        except Exception as e:
            self.logger.error(f"Error storing currency data: {e}")
            return 0

    # Cache management methods
    def _read_cache(self) -> Optional[Dict[str, Any]]:
        """Read cache file if it exists"""
        if self.cache_file and os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                self.logger.warning(f"Error reading cache file: {e}")
                return None
        return None

    def _write_cache(self, data: List[Dict[str, Any]]):
        """Write data to cache file with UTC timestamp"""
        if not self.cache_file:
            return

        try:
            # Create cache directory if it doesn't exist
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)

            # Use UTC timestamp for consistency
            cache_data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": data,
            }

            with open(self.cache_file, "w") as f:
                json.dump(cache_data, f)

        except IOError as e:
            self.logger.warning(f"Error writing cache file: {e}")

    def _is_cache_valid(self, cache_data: Dict[str, Any]) -> bool:
        """Check if cache is still valid based on Indian trading day"""
        try:
            # Indian timezone (IST = UTC+5:30)
            IST = timezone(timedelta(hours=5, minutes=30))

            # Get current time in IST
            current_time_ist = datetime.now(IST)

            # Parse cache timestamp (assume it's in UTC and convert to IST)
            cache_time_utc = datetime.fromisoformat(cache_data["timestamp"])
            if cache_time_utc.tzinfo is None:
                # If no timezone info, assume UTC
                cache_time_utc = cache_time_utc.replace(tzinfo=timezone.utc)

            cache_time_ist = cache_time_utc.astimezone(IST)

            # Check if cache is from the same trading day
            current_date = current_time_ist.date()
            cache_date = cache_time_ist.date()

            # If cache is from a different date, it's invalid
            if current_date != cache_date:
                return False

            # If it's the same date, cache is valid
            return True

        except (ValueError, KeyError, TypeError):
            return False

    def clear_cache(self) -> bool:
        """Clear the cache file to force fresh data on next fetch"""
        if not self.cache_file:
            return False

        try:
            if os.path.exists(self.cache_file):
                os.remove(self.cache_file)
                self.logger.info(f"Cache file cleared: {self.cache_file}")
                return True
            else:
                self.logger.info("No cache file to clear")
                return False
        except OSError as e:
            self.logger.error(f"Error clearing cache file: {e}")
            return False

    def is_new_trading_day(self) -> bool:
        """Check if it's a new Indian trading day"""
        if not self.cache_file:
            return True

        try:
            # Indian timezone (IST = UTC+5:30)
            IST = timezone(timedelta(hours=5, minutes=30))
            current_time_ist = datetime.now(IST)

            # Check if cache exists
            cached_data = self._read_cache()
            if not cached_data:
                return True  # No cache means new day

            # Parse cache timestamp
            cache_time_utc = datetime.fromisoformat(cached_data["timestamp"])
            if cache_time_utc.tzinfo is None:
                cache_time_utc = cache_time_utc.replace(tzinfo=timezone.utc)

            cache_time_ist = cache_time_utc.astimezone(IST)

            # Compare dates
            current_date = current_time_ist.date()
            cache_date = cache_time_ist.date()

            return current_date != cache_date

        except (ValueError, KeyError, TypeError):
            return True  # If we can't determine, assume new day

    # Common utility methods
    def _extract_option_type(self, symbol: str) -> Optional[str]:
        """Extract option type (CE/PE) from symbol"""
        from ..models.enums import OptionType

        # Check if it's a future first (futures end with FUT)
        if symbol.endswith("FUT"):
            return None

        # Check for option types
        if "CE" in symbol:
            return OptionType.CALL.value
        elif "PE" in symbol:
            return OptionType.PUT.value
        return None

    def _determine_underlying_type(self, item: Dict[str, Any]) -> str:
        """Determine underlying type for derivatives"""
        instrument_type = item.get("instrumenttype", "")
        if instrument_type in ["FUTIDX", "OPTIDX"]:
            return "INDEX"
        elif instrument_type in ["FUTSTK", "OPTSTK"]:
            return "EQUITY"
        elif instrument_type in ["FUTCOM", "OPTCOM"]:
            return "COMMODITY"
        elif instrument_type in ["FUTCUR", "OPTCUR"]:
            return "CURRENCY"
        return "UNKNOWN"

    def _extract_base_currency(self, symbol: str) -> Optional[str]:
        """Extract base currency from symbol"""
        if "USDINR" in symbol:
            return "USD"
        elif "EURINR" in symbol:
            return "EUR"
        elif "GBPINR" in symbol:
            return "GBP"
        elif "JPYINR" in symbol:
            return "JPY"
        return None

    def _extract_quote_currency(self, symbol: str) -> str:
        """Extract quote currency from symbol"""
        if "INR" in symbol:
            return "INR"
        return "INR"  # Default for Indian markets

    def get_provider_info(self) -> Dict[str, Any]:
        """Get provider information"""
        return {
            "broker_name": self.broker_name,
            "provider_class": self.__class__.__name__,
            "supported_instruments": ["equity", "fno", "commodity", "currency"],
        }
