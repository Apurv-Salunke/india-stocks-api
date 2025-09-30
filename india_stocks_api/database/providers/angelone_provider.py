"""
AngelOne data provider for instrument database
"""

import json
from typing import Dict, List, Any

import requests
from tqdm import tqdm

from .base_provider import BaseProvider
from ..utils.db_utils import DatabaseUtils
from ...utils.cache_utils import get_cache_file_path


class AngelOneTokensManager(BaseProvider):
    """AngelOne tokens manager: fetch + transform of instrument data"""

    def __init__(self, symbol_db, broker_name: str = "angelone", max_workers: int = 8):
        super().__init__(symbol_db, broker_name, max_workers)

        # AngelOne specific configuration
        self.base_urls = {
            "market_data": "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        }
        self.cache_file = get_cache_file_path("angelone_tokens_cache.json")

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

    def fetch_equity_data(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Fetch equity instruments data from AngelOne"""
        print("🔍 Fetching equity instruments from AngelOne...")

        try:
            # Fetch raw data
            raw_data = self._fetch_market_data(force_refresh=force_refresh)

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

    def fetch_fno_data(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Fetch F&O instruments data from AngelOne"""
        print("🔍 Fetching F&O instruments from AngelOne...")

        try:
            raw_data = self._fetch_market_data(force_refresh=force_refresh)

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

    def fetch_commodity_data(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Fetch commodity instruments data from AngelOne"""
        self.logger.info("Fetching commodity data from AngelOne...")

        try:
            raw_data = self._fetch_market_data(force_refresh=force_refresh)

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

    def fetch_currency_data(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Fetch currency instruments data from AngelOne"""
        self.logger.info("Fetching currency data from AngelOne...")

        try:
            raw_data = self._fetch_market_data(force_refresh=force_refresh)

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

    def _fetch_market_data(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Fetch raw market data from AngelOne API"""
        # Check cache first (unless force refresh is requested)
        if not force_refresh:
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
