from datetime import datetime, timedelta
from json import JSONDecodeError, dump, load
import os
from core.brokers.base.base import Broker
from core.brokers.base.constants import (
    ExchangeCode,
    OrderType,
    Product,
    Side,
    Status,
    Validity,
    Variety,
)
from core.brokers.base import TokenDownloadError


class AngelOne(Broker):
    """
    AngelOne Broker Class.

    Returns:
        indian_stocks_api.angel_one: india-stocks-api AngelOne Broker Object.
    """

    indices = {}
    eq_tokens = {}
    fno_tokens = {}
    token_params = [
        "user_id",
        "pin",
        "totpstr",
        "api_key",
    ]
    id = "angelone"
    _session = Broker._create_session()
    # Cache File for storing tokens master data
    _CACHE_FILE = "_cache/angelone_tokens_cache.json"
    # Base URLs
    base_urls = {
        "api_doc": "https://smartapi.angelbroking.com/docs",
        "access_token": "https://apiconnect.angelbroking.com/rest/auth/angelbroking/user/v1/loginByPassword",
        "base": "https://apiconnect.angelbroking.com/rest/secure/angelbroking",
        "market_data": "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
    }

    # Order Placing URLs

    urls = {
        "place_order": f"{base_urls['base']}/order/v1/placeOrder",
        "modify_order": f"{base_urls['base']}/order/v1/modifyOrder",
        "cancel_order": f"{base_urls['base']}/order/v1/cancelOrder",
        "orderbook": f"{base_urls['base']}/order/v1/getOrderBook",
        "tradebook": f"{base_urls['base']}/order/v1/getTradeBook",
        "positions": f"{base_urls['base']}/order/v1/getPosition",
        "holdings": f"{base_urls['base']}/portfolio/v1/getAllHolding",
        "rms_limits": f"{base_urls['base']}/user/v1/getRMS",
        "profile": f"{base_urls['base']}/user/v1/getProfile",
    }

    # Request Parameters Dictionaries
    req_exchange = {
        ExchangeCode.NSE: "NSE",
        ExchangeCode.BSE: "BSE",
        ExchangeCode.NFO: "NFO",
        ExchangeCode.MCX: "MCX",
    }

    req_side = {
        Side.BUY: "BUY",
        Side.SELL: "SELL",
    }

    req_product = {
        Product.MIS: "INTRADAY",
        Product.NRML: "CARRYFORWARD",
        Product.CNC: "DELIVERY",
        Product.MARGIN: "MARGIN",
        Product.BO: "BO",
    }

    req_order_type = {
        OrderType.MARKET: "MARKET",
        OrderType.LIMIT: "LIMIT",
        OrderType.SL: "STOPLOSS_LIMIT",
        OrderType.SLM: "STOPLOSS_MARKET",
    }

    req_variety = {
        Variety.REGULAR: "NORMAL",
        Variety.STOPLOSS: "STOPLOSS",
        Variety.AMO: "AMO",
        Variety.BO: "ROBO",
    }

    req_validity = {
        Validity.DAY: "DAY",
        Validity.IOC: "IOC",
    }

    # Response Parameters Dictionaries

    resp_status = {
        "open pending": Status.PENDING,
        "not modified": Status.PENDING,
        "not cancelled": Status.PENDING,
        "modify pending": Status.PENDING,
        "trigger pending": Status.PENDING,
        "cancel pending": Status.PENDING,
        "validation pending": Status.PENDING,
        "put order req received": Status.PENDING,
        "modify validation pending": Status.PENDING,
        "after market order req received": Status.PENDING,
        "modify after market order req received": Status.PENDING,
        "cancelled": Status.CANCELLED,
        "cancelled after market order": Status.CANCELLED,
        "open": Status.OPEN,
        "complete": Status.FILLED,
        "rejected": Status.REJECTED,
        "modified": Status.MODIFIED,
    }

    resp_order_type = {
        "MARKET": OrderType.MARKET,
        "LIMIT": OrderType.LIMIT,
        "STOPLOSS_LIMIT": OrderType.SL,
        "STOPLOSS_MARKET": OrderType.SLM,
    }

    resp_product = {
        "DELIVERY": Product.CNC,
        "CARRYFORWARD": Product.NRML,
        "MARGIN": Product.MARGIN,
        "INTRADAY": Product.MIS,
        "BO": Product.BO,
    }

    resp_variety = {
        "NORMAL": Variety.REGULAR,
        "STOPLOSS": Variety.STOPLOSS,
        "AMO": Variety.AMO,
        "ROBO": Variety.BO,
    }

    @classmethod
    def _fetch_tokens(cls):
        """
        Fetches all segment tokens from AngelOne, using cache if available and valid.
        """
        cache_data = cls._read_cache()

        if cache_data and cls._is_cache_valid(cache_data):
            return cache_data["data"]

        headers = {
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

        response = cls.fetch(
            method="GET", url=cls.base_urls["market_data"], headers=headers, timeout=15
        )
        AngelOne.cookies = dict(response.cookies)
        data = cls._json_parser(response)

        cls._write_cache(data)
        return data

    @classmethod
    def _read_cache(cls):
        """
        Reads the cache file if it exists.
        """
        if os.path.exists(cls._CACHE_FILE):
            try:
                with open(cls._CACHE_FILE, "r") as f:
                    return load(f)
            except JSONDecodeError:
                return None
        return None

    @classmethod
    def _write_cache(cls, data):
        """
        Writes the data to the cache file.
        """
        cache_data = {"timestamp": datetime.now().isoformat(), "data": data}
        with open(cls._CACHE_FILE, "w") as f:
            dump(cache_data, f)

    @classmethod
    def _is_cache_valid(cls, cache_data):
        """
        Checks if the cache is still valid (less than one day old).
        """
        cache_time = datetime.fromisoformat(cache_data["timestamp"])
        return datetime.now() - cache_time < timedelta(days=1)

    @classmethod
    def create_eq_tokens(cls) -> dict:
        """
        Downlaods NSE & BSE Equity Info for F&O Segment.
        Stores them in the angelone.indices Dictionary.

        Returns:
            dict: Unified fenix indices format.
        """
        print("Fetching NSE & BSE Equity Info for Equity, F&O Segment...")
        json_list = cls._fetch_tokens()
        if not json_list:
            raise TokenDownloadError("No data fetched from AngelOne API.")

        df = cls.data_frame(json_list)
        print("Data fetching complete.")

        if "tick_size" not in df.columns:
            raise TokenDownloadError(
                "Required 'tick_size' column not found in fetched data."
            )

        df["tick_size"] = df["tick_size"].astype(float) / 100

        df_bse = df[df["exch_seg"] == ExchangeCode.BSE][
            ["symbol", "token", "tick_size", "lotsize", "exch_seg"]
        ]
        df_bse.rename(
            {
                "symbol": "Symbol",
                "token": "Token",
                "tick_size": "TickSize",
                "lotsize": "LotSize",
                "exch_seg": "Exchange",
            },
            axis=1,
            inplace=True,
        )

        df_bse["Token"] = df_bse["Token"].astype(int)
        df_bse.drop_duplicates(subset=["Symbol"], keep="first", inplace=True)
        df_bse.set_index(df_bse["Symbol"], inplace=True)

        df_nse = df[df["symbol"].str.endswith("-EQ")][
            ["name", "symbol", "token", "tick_size", "lotsize", "exch_seg"]
        ]
        df_nse.rename(
            {
                "name": "Index",
                "symbol": "Symbol",
                "token": "Token",
                "tick_size": "TickSize",
                "lotsize": "LotSize",
                "exch_seg": "Exchange",
            },
            axis=1,
            inplace=True,
        )

        df_nse["Token"] = df_nse["Token"].astype(int)
        df_nse = df_nse.drop_duplicates(subset=["Index"], keep="first")
        df_nse.set_index(df_nse["Index"], inplace=True)
        df_nse.drop(columns="Index", inplace=True)

        cls.eq_tokens[ExchangeCode.NSE] = df_nse.to_dict(orient="index")
        cls.eq_tokens[ExchangeCode.BSE] = df_bse.to_dict(orient="index")

        return cls.eq_tokens
