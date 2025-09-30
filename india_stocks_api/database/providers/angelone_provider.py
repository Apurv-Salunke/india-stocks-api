"""
AngelOne data provider for instrument database
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import requests
from tqdm import tqdm

from .base_provider import BaseProvider
from ..models.enums import OptionType
from ..utils.db_utils import DatabaseUtils
from ...utils.cache_utils import get_cache_file_path


class AngelOneProvider(BaseProvider):
    """AngelOne data provider implementation"""

    def __init__(self, symbol_db, broker_name: str = "angelone", max_workers: int = 8):
        super().__init__(symbol_db, broker_name)

        # AngelOne specific configuration
        self.base_urls = {
            "market_data": "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        }
        self.cache_file = get_cache_file_path("angelone_tokens_cache.json")
        self.cache_validity_hours = 24
        self.max_workers = max_workers

        # Threading lock for database operations
        self._db_lock = threading.Lock()

        # Headers for API requests
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "cross-site",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
        }

    def fetch_equity_data(self) -> List[Dict[str, Any]]:
        """Fetch equity instruments data from AngelOne"""
        print("🔍 Fetching equity instruments from AngelOne...")

        try:
            # Fetch raw data
            raw_data = self._fetch_market_data()

            # Filter for equity instruments with progress bar
            equity_data = []
            with tqdm(
                total=len(raw_data),
                desc="📊 Processing equity instruments",
                unit="instr",
                ncols=80,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
            ) as pbar:
                for item in raw_data:
                    if self._is_equity_instrument(item):
                        standardized_symbol = self._standardize_symbol(
                            item["symbol"], item["exch_seg"]
                        )
                        mapped_exchange = self._map_segment_to_exchange(
                            item["exch_seg"]
                        )
                        equity_info = {
                            "standardized_symbol": standardized_symbol,
                            "instrument_name": item.get("name", ""),
                            "exchange_code": mapped_exchange,
                            "broker_symbol": item["symbol"],
                            "broker_token": str(item["token"]),
                            "tick_size": float(item.get("tick_size", 0)) / 100,
                            "lot_size": int(item.get("lotsize", 1)),
                            "isin": item.get("isin", ""),
                            "sector": item.get("sector", ""),
                            "industry": item.get("industry", ""),
                            "instrument_type": item.get("instrumenttype", ""),
                        }
                        equity_data.append(equity_info)
                    pbar.update(1)

            print(f"✅ Found {len(equity_data):,} equity instruments")
            return equity_data

        except Exception as e:
            self.logger.error(f"Error fetching equity data: {e}")
            return []

    def fetch_fno_data(self) -> List[Dict[str, Any]]:
        """Fetch F&O instruments data from AngelOne"""
        print("🔍 Fetching F&O instruments from AngelOne...")

        try:
            raw_data = self._fetch_market_data()

            fno_data = []
            with tqdm(
                total=len(raw_data),
                desc="📊 Processing F&O instruments",
                unit="instr",
                ncols=80,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
            ) as pbar:
                for item in raw_data:
                    if self._is_fno_instrument(item):
                        standardized_symbol = self._standardize_symbol(
                            item["symbol"], item["exch_seg"]
                        )
                        mapped_exchange = self._map_segment_to_exchange(
                            item["exch_seg"]
                        )
                        fno_info = {
                            "standardized_symbol": standardized_symbol,
                            "instrument_name": item.get("name", ""),
                            "exchange_code": mapped_exchange,
                            "broker_symbol": item["symbol"],
                            "broker_token": str(item["token"]),
                            "tick_size": float(item.get("tick_size", 0)) / 100,
                            "lot_size": int(item.get("lotsize", 1)),
                            "expiry_date": item.get("expiry", ""),
                            "strike_price": float(item.get("strike", -1))
                            if item.get("strike", -1) != -1
                            else None,
                            "option_type": self._extract_option_type(item["symbol"]),
                            "underlying_symbol": item.get("name", ""),
                            "underlying_type": self._determine_underlying_type(item),
                            "instrument_type": item.get("instrumenttype", ""),
                        }
                        fno_data.append(fno_info)
                    pbar.update(1)

            print(f"✅ Found {len(fno_data):,} F&O instruments")
            return fno_data

        except Exception as e:
            self.logger.error(f"Error fetching F&O data: {e}")
            return []

    def fetch_commodity_data(self) -> List[Dict[str, Any]]:
        """Fetch commodity instruments data from AngelOne"""
        self.logger.info("Fetching commodity data from AngelOne...")

        try:
            raw_data = self._fetch_market_data()

            commodity_data = []
            for item in raw_data:
                if self._is_commodity_instrument(item):
                    standardized_symbol = self._standardize_symbol(
                        item["symbol"], item["exch_seg"]
                    )
                    mapped_exchange = self._map_segment_to_exchange(item["exch_seg"])
                    commodity_info = {
                        "standardized_symbol": standardized_symbol,
                        "instrument_name": item.get("name", ""),
                        "exchange_code": mapped_exchange,
                        "broker_symbol": item["symbol"],
                        "broker_token": str(item["token"]),
                        "tick_size": float(item.get("tick_size", 0)) / 100,
                        "lot_size": int(item.get("lotsize", 1)),
                        "expiry_date": item.get("expiry", ""),
                        "strike_price": float(item.get("strike", -1))
                        if item.get("strike", -1) != -1
                        else None,
                        "option_type": self._extract_option_type(item["symbol"]),
                        "commodity_type": DatabaseUtils.determine_commodity_type(
                            item["symbol"], item.get("name", "")
                        ),
                        "commodity_unit": item.get("unit", ""),
                        "delivery_center": item.get("delivery_center", ""),
                        "instrument_type": item.get("instrumenttype", ""),
                    }
                    commodity_data.append(commodity_info)

            self.logger.info(f"Fetched {len(commodity_data)} commodity instruments")
            return commodity_data

        except Exception as e:
            self.logger.error(f"Error fetching commodity data: {e}")
            return []

    def fetch_currency_data(self) -> List[Dict[str, Any]]:
        """Fetch currency instruments data from AngelOne"""
        self.logger.info("Fetching currency data from AngelOne...")

        try:
            raw_data = self._fetch_market_data()

            currency_data = []
            for item in raw_data:
                if self._is_currency_instrument(item):
                    standardized_symbol = self._standardize_symbol(
                        item["symbol"], item["exch_seg"]
                    )
                    mapped_exchange = self._map_segment_to_exchange(item["exch_seg"])
                    currency_info = {
                        "standardized_symbol": standardized_symbol,
                        "instrument_name": item.get("name", ""),
                        "exchange_code": mapped_exchange,
                        "broker_symbol": item["symbol"],
                        "broker_token": str(item["token"]),
                        "tick_size": float(item.get("tick_size", 0)) / 100,
                        "lot_size": int(item.get("lotsize", 1)),
                        "expiry_date": item.get("expiry", ""),
                        "strike_price": float(item.get("strike", -1))
                        if item.get("strike", -1) != -1
                        else None,
                        "option_type": self._extract_option_type(item["symbol"]),
                        "base_currency": self._extract_base_currency(item["symbol"]),
                        "quote_currency": self._extract_quote_currency(item["symbol"]),
                        "instrument_type": item.get("instrumenttype", ""),
                    }
                    currency_data.append(currency_info)

            self.logger.info(f"Fetched {len(currency_data)} currency instruments")
            return currency_data

        except Exception as e:
            self.logger.error(f"Error fetching currency data: {e}")
            return []

    def _fetch_market_data(self) -> List[Dict[str, Any]]:
        """Fetch raw market data from AngelOne API"""
        # Check cache first
        cached_data = self._read_cache()
        if cached_data and self._is_cache_valid(cached_data):
            print("📋 Using cached AngelOne data")
            return cached_data["data"]

        print("🌐 Fetching fresh data from AngelOne API...")

        try:
            with tqdm(
                total=1,
                desc="🌐 Downloading market data",
                unit="MB",
                ncols=80,
                bar_format="{l_bar}{bar}| {elapsed}",
            ) as pbar:
                response = requests.get(
                    self.base_urls["market_data"], headers=self.headers, timeout=30
                )
                response.raise_for_status()
                pbar.update(1)

            data = response.json()
            self._write_cache(data)

            print(f"✅ Downloaded {len(data):,} instruments from AngelOne")
            return data

        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from AngelOne API: {e}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing JSON response: {e}")
            raise

    def _is_equity_instrument(self, item: Dict[str, Any]) -> bool:
        """Check if item is an equity instrument"""
        symbol = item.get("symbol", "")
        exchange = item.get("exch_seg", "")
        instrument_type = item.get("instrumenttype", "")

        # NSE equity instruments (symbols ending with -EQ and empty instrumenttype)
        if exchange == "NSE" and symbol.endswith("-EQ") and instrument_type == "":
            return True

        # BSE equity instruments (symbols not ending with -EQ and empty instrumenttype)
        if exchange == "BSE" and not symbol.endswith("-EQ") and instrument_type == "":
            return True

        # Legacy support for explicit EQ/BE types
        if exchange in ["NSE", "BSE"] and instrument_type in ["EQ", "BE"]:
            return True

        return False

    def _is_fno_instrument(self, item: Dict[str, Any]) -> bool:
        """Check if item is an F&O instrument"""
        exchange = item.get("exch_seg", "")
        instrument_type = item.get("instrumenttype", "")

        # F&O segments (NFO = NSE F&O, BFO = BSE F&O) with valid instrument types
        if exchange in ["NFO", "BFO"]:
            # Index futures and options
            if instrument_type in ["FUTIDX", "OPTIDX"]:
                return True
            # Stock futures and options
            if instrument_type in ["FUTSTK", "OPTSTK"]:
                return True

        return False

    def _is_commodity_instrument(self, item: Dict[str, Any]) -> bool:
        """Check if item is a commodity instrument"""
        exchange = item.get("exch_seg", "")
        instrument_type = item.get("instrumenttype", "")

        # Commodity exchanges
        if exchange in ["MCX", "NCDEX", "ICEX"]:
            # Commodity futures and options
            if instrument_type in ["FUTCOM", "OPTCOM"]:
                return True

        return False

    def _is_currency_instrument(self, item: Dict[str, Any]) -> bool:
        """Check if item is a currency instrument"""
        exchange = item.get("exch_seg", "")
        instrument_type = item.get("instrumenttype", "")

        # Currency exchanges
        if exchange in ["CDS", "BCD"]:
            # Currency futures and options
            if instrument_type in ["FUTCUR", "OPTCUR"]:
                return True

        return False

    def _map_segment_to_exchange(self, segment: str) -> str:
        """Map broker segment to standardized exchange"""
        segment_mapping = {
            "NFO": "NSE",  # NSE F&O segment maps to NSE
            "BFO": "BSE",  # BSE F&O segment maps to BSE
            "CDS": "NSE",  # NSE Currency Derivatives segment maps to NSE
            "BCD": "BSE",  # BSE Currency Derivatives segment maps to BSE
        }
        return segment_mapping.get(segment, segment)

    def _standardize_symbol(self, symbol: str, exchange: str) -> str:
        """Convert broker symbol to standardized NSE format"""
        if exchange == "NSE" and symbol.endswith("-EQ"):
            return symbol[:-3]  # Remove -EQ suffix
        return symbol

    def _extract_option_type(self, symbol: str) -> Optional[str]:
        """Extract option type (CE/PE) from symbol"""
        if "CE" in symbol:
            return OptionType.CALL.value
        elif "PE" in symbol:
            return OptionType.PUT.value
        return None

    def _determine_underlying_type(self, item: Dict[str, Any]) -> str:
        """Determine underlying type for derivatives"""
        if item.get("instrumenttype") in ["FUTIDX", "OPTIDX"]:
            return "INDEX"
        elif item.get("instrumenttype") in ["FUTSTK", "OPTSTK"]:
            return "EQUITY"
        elif item.get("instrumenttype") in ["FUTCOM", "OPTCOM"]:
            return "COMMODITY"
        elif item.get("instrumenttype") in ["FUTCUR", "OPTCUR"]:
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

    def _read_cache(self) -> Optional[Dict[str, Any]]:
        """Read cache file if it exists"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                self.logger.warning(f"Error reading cache file: {e}")
                return None
        return None

    def _write_cache(self, data: List[Dict[str, Any]]):
        """Write data to cache file"""
        try:
            # Create cache directory if it doesn't exist
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)

            cache_data = {"timestamp": datetime.now().isoformat(), "data": data}

            with open(self.cache_file, "w") as f:
                json.dump(cache_data, f)

        except IOError as e:
            self.logger.warning(f"Error writing cache file: {e}")

    def _is_cache_valid(self, cache_data: Dict[str, Any]) -> bool:
        """Check if cache is still valid"""
        try:
            cache_time = datetime.fromisoformat(cache_data["timestamp"])
            return datetime.now() - cache_time < timedelta(
                hours=self.cache_validity_hours
            )
        except (ValueError, KeyError):
            return False

    def _process_batch(self, batch: List[Dict[str, Any]], data_type: str) -> int:
        """Process a batch of instruments using bulk insert (NO nested threading)"""
        stored_count = 0

        try:
            if data_type == "equity":
                stored_count = self._bulk_store_equity_batch(batch)
            elif data_type == "fno":
                stored_count = self._bulk_store_fno_batch(batch)
            elif data_type == "commodity":
                stored_count = self._bulk_store_commodity_batch(batch)
            elif data_type == "currency":
                stored_count = self._bulk_store_currency_batch(batch)
        except Exception as e:
            self.logger.error(f"Error processing {data_type} batch: {e}")

        return stored_count

    def _bulk_store_fno_batch(self, batch: List[Dict[str, Any]]) -> int:
        """Bulk store F&O batch using raw SQL for maximum performance"""
        if not batch:
            return 0

        try:
            import sqlite3

            # Use WAL mode and timeout for better concurrency
            with sqlite3.connect(str(self.db.db_path), timeout=30.0) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("BEGIN IMMEDIATE TRANSACTION")

                # Pre-fetch exchange and category IDs to avoid repeated lookups
                exchange_ids = {}
                category_ids = {}

                for item in batch:
                    exchange_code = item["exchange_code"]
                    if exchange_code not in exchange_ids:
                        exchange_ids[exchange_code] = DatabaseUtils.get_exchange_id(
                            exchange_code, str(self.db.db_path)
                        )

                    category_code = "FUT" if not item.get("option_type") else "OPT"
                    if category_code not in category_ids:
                        category_ids[category_code] = DatabaseUtils.get_category_id(
                            category_code, str(self.db.db_path)
                        )

                # Bulk insert instruments
                instrument_sql = """
                INSERT OR REPLACE INTO instruments
                (standardized_symbol, instrument_name, exchange_id, category_id, subcategory_id,
                 underlying_symbol, expiry_date, strike_price, option_type, tick_size, lot_size,
                 is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """

                instrument_data = []
                for item in batch:
                    exchange_id = exchange_ids.get(item["exchange_code"])
                    category_code = "FUT" if not item.get("option_type") else "OPT"
                    category_id = category_ids.get(category_code)

                    if not exchange_id or not category_id:
                        continue

                    instrument_data.append(
                        (
                            item["standardized_symbol"],
                            item["instrument_name"],
                            exchange_id,
                            category_id,
                            None,  # subcategory_id
                            item.get("underlying_symbol"),
                            item.get("expiry_date"),
                            item.get("strike_price"),
                            item.get("option_type"),
                            item["tick_size"],
                            item["lot_size"],
                        )
                    )

                if instrument_data:
                    # Get the current max ID before insertion
                    cursor = conn.execute(
                        "SELECT COALESCE(MAX(id), 0) FROM instruments"
                    )
                    max_id_before = cursor.fetchone()[0]

                    # Insert instruments
                    conn.executemany(instrument_sql, instrument_data)

                    # Calculate the first inserted ID
                    first_id = max_id_before + 1

                    # Bulk insert broker instruments
                    broker_sql = """
                    INSERT OR REPLACE INTO broker_instruments
                    (instrument_id, broker_name, broker_symbol, broker_token, tick_size, lot_size,
                     created_at, updated_at)
                    VALUES (?, 'angelone', ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """

                    broker_data = []
                    for i, item in enumerate(batch):
                        if i < len(
                            instrument_data
                        ):  # Only for successfully inserted instruments
                            broker_data.append(
                                (
                                    first_id + i,
                                    item["broker_symbol"],
                                    item["broker_token"],
                                    item["tick_size"],
                                    item["lot_size"],
                                )
                            )

                    if broker_data:
                        conn.executemany(broker_sql, broker_data)

                conn.commit()
                return len(instrument_data)

        except Exception as e:
            self.logger.error(f"Error in bulk F&O insert: {e}")
            return 0

    def _bulk_store_equity_batch(self, batch: List[Dict[str, Any]]) -> int:
        """Bulk store equity batch using raw SQL for maximum performance"""
        if not batch:
            return 0

        try:
            import sqlite3

            # Use WAL mode and timeout for better concurrency
            with sqlite3.connect(str(self.db.db_path), timeout=30.0) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("BEGIN IMMEDIATE TRANSACTION")

                # Pre-fetch exchange and category IDs
                exchange_ids = {}
                for item in batch:
                    exchange_code = item["exchange_code"]
                    if exchange_code not in exchange_ids:
                        exchange_ids[exchange_code] = DatabaseUtils.get_exchange_id(
                            exchange_code, str(self.db.db_path)
                        )

                category_id = DatabaseUtils.get_category_id("EQ", str(self.db.db_path))

                # Bulk insert instruments
                instrument_sql = """
                INSERT OR REPLACE INTO instruments
                (standardized_symbol, instrument_name, exchange_id, category_id, subcategory_id,
                 underlying_symbol, expiry_date, strike_price, option_type, tick_size, lot_size,
                 is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """

                instrument_data = []
                for item in batch:
                    exchange_id = exchange_ids.get(item["exchange_code"])

                    if not exchange_id or not category_id:
                        continue

                    instrument_data.append(
                        (
                            item["standardized_symbol"],
                            item["instrument_name"],
                            exchange_id,
                            category_id,
                            None,  # subcategory_id
                            None,  # underlying_symbol
                            None,  # expiry_date
                            None,  # strike_price
                            None,  # option_type
                            item["tick_size"],
                            item["lot_size"],
                        )
                    )

                if instrument_data:
                    cursor = conn.executemany(instrument_sql, instrument_data)

                    # Get the first inserted ID to calculate range
                    first_id = cursor.lastrowid - len(instrument_data) + 1

                    # Bulk insert broker instruments
                    broker_sql = """
                    INSERT OR REPLACE INTO broker_instruments
                    (instrument_id, broker_name, broker_symbol, broker_token, tick_size, lot_size,
                     created_at, updated_at)
                    VALUES (?, 'angelone', ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """

                    broker_data = []
                    for i, item in enumerate(batch):
                        if i < len(instrument_data):
                            broker_data.append(
                                (
                                    first_id + i,
                                    item["broker_symbol"],
                                    item["broker_token"],
                                    item["tick_size"],
                                    item["lot_size"],
                                )
                            )

                    if broker_data:
                        conn.executemany(broker_sql, broker_data)

                conn.commit()
                return len(instrument_data)

        except Exception as e:
            self.logger.error(f"Error in bulk equity insert: {e}")
            return 0

    def _bulk_store_commodity_batch(self, batch: List[Dict[str, Any]]) -> int:
        """Bulk store commodity batch - placeholder"""
        return 0

    def _bulk_store_currency_batch(self, batch: List[Dict[str, Any]]) -> int:
        """Bulk store currency batch - placeholder"""
        return 0

    def _store_equity_data(self, equity_data: List[Dict[str, Any]]) -> int:
        """Store equity data in database using threading"""
        if len(equity_data) == 0:
            print("ℹ️  No equity instruments to store")
            return 0

        print(
            f"💾 Storing {len(equity_data):,} equity instruments using {self.max_workers} threads..."
        )

        # Split data into larger batches for better performance (bulk insert)
        batch_size = max(1000, len(equity_data) // self.max_workers)
        batches = [
            equity_data[i : i + batch_size]
            for i in range(0, len(equity_data), batch_size)
        ]

        total_stored = 0
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_batch = {
                executor.submit(self._process_batch, batch, "equity"): batch
                for batch in batches
            }

            # Show progress for batch processing
            with tqdm(
                total=len(batches),
                desc="💾 Storing equity batches",
                unit="batch",
                ncols=80,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
            ) as pbar:
                for future in as_completed(future_to_batch):
                    batch_stored = future.result()
                    total_stored += batch_stored
                    self.logger.debug(
                        f"Processed batch: {batch_stored} instruments stored"
                    )
                    pbar.update(1)

        print(f"✅ Stored {total_stored:,} equity instruments successfully")
        return total_stored

    def _store_single_equity_item(self, item: Dict[str, Any]) -> bool:
        """Store a single equity instrument item (thread-safe)"""
        with self._db_lock:
            try:
                # Get exchange and category IDs
                exchange_id = DatabaseUtils.get_exchange_id(
                    item["exchange_code"], self.db.db_path
                )
                category_id = DatabaseUtils.get_category_id("EQ", self.db.db_path)

                if not exchange_id or not category_id:
                    self.logger.warning(
                        f"Could not find exchange or category for {item['standardized_symbol']}"
                    )
                    return False

                # Check if instrument already exists
                instrument_id = DatabaseUtils.get_instrument_id(
                    item["standardized_symbol"],
                    exchange_id,
                    category_id,
                    self.db.db_path,
                )

                if not instrument_id:
                    # Create new instrument
                    instrument_data = {
                        "standardized_symbol": item["standardized_symbol"],
                        "instrument_name": item["instrument_name"],
                        "exchange_id": exchange_id,
                        "category_id": category_id,
                        "isin": item.get("isin"),
                        "sector": item.get("sector"),
                        "industry": item.get("industry"),
                    }
                    instrument_id = self.db.add_instrument(instrument_data)

                # Create broker instrument mapping
                broker_instrument_data = {
                    "instrument_id": instrument_id,
                    "broker_name": self.broker_name,
                    "broker_symbol": item["broker_symbol"],
                    "broker_token": item["broker_token"],
                    "tick_size": item["tick_size"],
                    "lot_size": item["lot_size"],
                }
                self.db.add_broker_instrument(broker_instrument_data)
                return True

            except Exception as e:
                self.logger.error(
                    f"Error storing equity instrument {item.get('standardized_symbol', 'unknown')}: {e}"
                )
                return False

    def _store_fno_data(self, fno_data: List[Dict[str, Any]]) -> int:
        """Store F&O data in database using threading"""
        if len(fno_data) == 0:
            print("ℹ️  No F&O instruments to store")
            return 0

        print(
            f"💾 Storing {len(fno_data):,} F&O instruments using {self.max_workers} threads..."
        )
        print("⏳ This may take a few minutes for large datasets...")

        # Split data into larger batches for better performance (bulk insert)
        batch_size = max(1000, len(fno_data) // self.max_workers)
        batches = [
            fno_data[i : i + batch_size] for i in range(0, len(fno_data), batch_size)
        ]

        total_stored = 0
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_batch = {
                executor.submit(self._process_batch, batch, "fno"): batch
                for batch in batches
            }

            # Show progress for batch processing
            with tqdm(
                total=len(batches),
                desc="💾 Storing F&O batches",
                unit="batch",
                ncols=80,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
            ) as pbar:
                for future in as_completed(future_to_batch):
                    batch_stored = future.result()
                    total_stored += batch_stored
                    self.logger.debug(
                        f"Processed F&O batch: {batch_stored} instruments stored"
                    )
                    pbar.update(1)

        print(f"✅ Stored {total_stored:,} F&O instruments successfully")
        return total_stored

    def _store_single_fno_item(self, item: Dict[str, Any]) -> bool:
        """Store a single F&O instrument item (thread-safe)"""
        with self._db_lock:
            try:
                # Determine category (FUTURES or OPTIONS)
                category_code = "FUT" if not item.get("option_type") else "OPT"
                exchange_id = DatabaseUtils.get_exchange_id(
                    item["exchange_code"], self.db.db_path
                )
                category_id = DatabaseUtils.get_category_id(
                    category_code, self.db.db_path
                )

                if not exchange_id or not category_id:
                    self.logger.warning(
                        f"Could not find exchange or category for {item['standardized_symbol']}"
                    )
                    return False

                # Check if instrument already exists
                instrument_id = DatabaseUtils.get_instrument_id(
                    item["standardized_symbol"],
                    exchange_id,
                    category_id,
                    self.db.db_path,
                )

                if not instrument_id:
                    # Create new instrument
                    instrument_data = {
                        "standardized_symbol": item["standardized_symbol"],
                        "instrument_name": item["instrument_name"],
                        "exchange_id": exchange_id,
                        "category_id": category_id,
                        "underlying_symbol": item.get("underlying_symbol"),
                        "underlying_type": item.get("underlying_type"),
                    }
                    instrument_id = self.db.add_instrument(instrument_data)

                # Create broker instrument mapping
                broker_instrument_data = {
                    "instrument_id": instrument_id,
                    "broker_name": self.broker_name,
                    "broker_symbol": item["broker_symbol"],
                    "broker_token": item["broker_token"],
                    "tick_size": item["tick_size"],
                    "lot_size": item["lot_size"],
                    "expiry_date": item.get("expiry_date"),
                    "strike_price": item.get("strike_price"),
                    "option_type": item.get("option_type"),
                }
                self.db.add_broker_instrument(broker_instrument_data)
                return True

            except Exception as e:
                self.logger.error(
                    f"Error storing F&O instrument {item.get('standardized_symbol', 'unknown')}: {e}"
                )
                return False

    def _store_commodity_data(self, commodity_data: List[Dict[str, Any]]) -> int:
        """Store commodity data in database"""
        stored_count = 0

        for item in commodity_data:
            try:
                exchange_id = DatabaseUtils.get_exchange_id(
                    item["exchange_code"], self.db.db_path
                )
                category_id = DatabaseUtils.get_category_id("COM", self.db.db_path)

                if not exchange_id or not category_id:
                    self.logger.warning(
                        f"Could not find exchange or category for {item['standardized_symbol']}"
                    )
                    continue

                # Get subcategory ID
                subcategory_id = None
                if item.get("commodity_type"):
                    subcategory_id = DatabaseUtils.get_subcategory_id(
                        item["commodity_type"], category_id, self.db.db_path
                    )

                # Check if instrument already exists
                instrument_id = DatabaseUtils.get_instrument_id(
                    item["standardized_symbol"],
                    exchange_id,
                    category_id,
                    self.db.db_path,
                )

                if not instrument_id:
                    # Create new instrument
                    instrument_data = {
                        "standardized_symbol": item["standardized_symbol"],
                        "instrument_name": item["instrument_name"],
                        "exchange_id": exchange_id,
                        "category_id": category_id,
                        "subcategory_id": subcategory_id,
                        "commodity_type": item.get("commodity_type"),
                        "commodity_unit": item.get("commodity_unit"),
                        "delivery_center": item.get("delivery_center"),
                    }
                    instrument_id = self.db.add_instrument(instrument_data)

                # Create broker instrument mapping
                broker_instrument_data = {
                    "instrument_id": instrument_id,
                    "broker_name": self.broker_name,
                    "broker_symbol": item["broker_symbol"],
                    "broker_token": item["broker_token"],
                    "tick_size": item["tick_size"],
                    "lot_size": item["lot_size"],
                    "expiry_date": item.get("expiry_date"),
                    "strike_price": item.get("strike_price"),
                    "option_type": item.get("option_type"),
                }
                self.db.add_broker_instrument(broker_instrument_data)
                stored_count += 1

            except Exception as e:
                self.logger.error(
                    f"Error storing commodity instrument {item.get('standardized_symbol', 'unknown')}: {e}"
                )

        return stored_count

    def _store_currency_data(self, currency_data: List[Dict[str, Any]]) -> int:
        """Store currency data in database"""
        stored_count = 0

        for item in currency_data:
            try:
                exchange_id = DatabaseUtils.get_exchange_id(
                    item["exchange_code"], self.db.db_path
                )
                category_id = DatabaseUtils.get_category_id("CUR", self.db.db_path)

                if not exchange_id or not category_id:
                    self.logger.warning(
                        f"Could not find exchange or category for {item['standardized_symbol']}"
                    )
                    continue

                # Check if instrument already exists
                instrument_id = DatabaseUtils.get_instrument_id(
                    item["standardized_symbol"],
                    exchange_id,
                    category_id,
                    self.db.db_path,
                )

                if not instrument_id:
                    # Create new instrument
                    instrument_data = {
                        "standardized_symbol": item["standardized_symbol"],
                        "instrument_name": item["instrument_name"],
                        "exchange_id": exchange_id,
                        "category_id": category_id,
                        "base_currency": item.get("base_currency"),
                        "quote_currency": item.get("quote_currency"),
                    }
                    instrument_id = self.db.add_instrument(instrument_data)

                # Create broker instrument mapping
                broker_instrument_data = {
                    "instrument_id": instrument_id,
                    "broker_name": self.broker_name,
                    "broker_symbol": item["broker_symbol"],
                    "broker_token": item["broker_token"],
                    "tick_size": item["tick_size"],
                    "lot_size": item["lot_size"],
                    "expiry_date": item.get("expiry_date"),
                    "strike_price": item.get("strike_price"),
                    "option_type": item.get("option_type"),
                }
                self.db.add_broker_instrument(broker_instrument_data)
                stored_count += 1

            except Exception as e:
                self.logger.error(
                    f"Error storing currency instrument {item.get('standardized_symbol', 'unknown')}: {e}"
                )

        return stored_count
