import os
from threading import Thread
from datetime import datetime, timedelta  # noqa: F401
import json

from core.brokers.base import Broker, InputError, constants, errors  # noqa: F401
from core.brokers.base import (  # noqa: F401
    ResponseError,
    TokenDownloadError,
    RequestTimeout,
    NetworkError,
    BrokerError,
)
from core.brokers.base import (  # noqa: F401
    Side,
    Root,
    WeeklyExpiry,
    Option,
    OrderType,
    ExchangeCode,
    Product,
    Validity,
    Variety,
    Status,
    Order,
    Position,
    Profile,
    UniqueID,
)
from core.brokers.angel_one import AngelOne  # noqa: F401

__version__ = "1.0.0"

brokers = ["angelone"]

base = ["Broker", "brokers", "constants"]

__all__ = base + errors.__all__ + brokers + constants.__all__

# Cache file path
CACHE_FILE = "_cache/brokers_cache.json"


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            cache = json.load(f)
        cache_date = datetime.fromtimestamp(cache["timestamp"]).date()
        if cache_date == datetime.now().date():
            return cache
    return None


def save_cache(data):
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, "w") as f:
        json.dump(data, f)


def initialize_brokers():
    cache = load_cache()

    if cache:
        print("Using cached data from today.")
        Broker.cookies = cache["cookies"]
        Broker.expiry_dates = cache["expiry_dates"]
    else:
        print("Fetching fresh data.")
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-GB,en;q=0.9",
            "dnt": "1",
            "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        }

        temp_session = Broker._create_session()
        response = temp_session.request(
            method="GET", url="https://www.nseindia.com/option-chain", headers=headers
        )
        Broker.cookies = dict(response.cookies)

        threads = []
        for root in [Root.BNF, Root.NF, Root.FNF, Root.MIDCPNF]:
            thread = Thread(target=Broker.download_expiry_dates_nfo, args=(root,))
            thread.start()
            threads.append(thread)

        for root in [Root.SENSEX, Root.BANKEX]:
            thread = Thread(target=Broker.download_expiry_dates_bfo, args=(root,))
            thread.start()
            threads.append(thread)

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Save the new data to cache
        cache_data = {
            "timestamp": datetime.now().timestamp(),
            "cookies": Broker.cookies,
            "expiry_dates": Broker.expiry_dates,
        }
        save_cache(cache_data)


# Run initialization
initialize_brokers()
