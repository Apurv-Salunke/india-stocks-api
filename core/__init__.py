#  ------------------------------------------------------------------------------


__version__ = "1.0.5"


#  ------------------------------------------------------------------------------

from threading import Thread

from core.brokers.base import Broker  # noqa: F401


from core.brokers.base import errors  # noqa: F401
from core.brokers.base.errors import InputError  # noqa: F401
from core.brokers.base.errors import ResponseError  # noqa: F401
from core.brokers.base.errors import TokenDownloadError  # noqa: F401
from core.brokers.base.errors import RequestTimeout  # noqa: F401
from core.brokers.base.errors import NetworkError  # noqa: F401
from core.brokers.base.errors import BrokerError  # noqa: F401


from core.brokers.base import constants  # noqa: F401
from core.brokers.base.constants import Side  # noqa: F401
from core.brokers.base.constants import Root  # noqa: F401
from core.brokers.base.constants import WeeklyExpiry  # noqa: F401
from core.brokers.base.constants import Option  # noqa: F401
from core.brokers.base.constants import OrderType  # noqa: F401
from core.brokers.base.constants import ExchangeCode  # noqa: F401
from core.brokers.base.constants import Product  # noqa: F401
from core.brokers.base.constants import Validity  # noqa: F401
from core.brokers.base.constants import Variety  # noqa: F401
from core.brokers.base.constants import Status  # noqa: F401
from core.brokers.base.constants import Order  # noqa: F401
from core.brokers.base.constants import Position  # noqa: F401
from core.brokers.base.constants import Profile  # noqa: F401
from core.brokers.base.constants import UniqueID  # noqa: F401


brokers = [
    "aliceblue",
    "angelone",
    "choice",
    "finvasia",
    "fivepaisa",
    "fyers",
    "iifl",
    "kotak",
    "kotakneo",
    "kunjee",
    "mastertrust",
    "motilaloswal",
    "paper",
    "symphony",
    "upstox",
    "vpc",
    "zerodha",
]

base = [
    "Broker",
    "brokers",
    "constants",
]

__all__ = base + errors.__all__ + brokers + constants.__all__


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

if not Broker.expiry_dates:
    for root in [Root.BNF, Root.NF, Root.FNF, Root.MIDCPNF]:
        Thread(target=Broker.download_expiry_dates_nfo, args=(root,)).start()

    for root in [Root.SENSEX, Root.BANKEX]:
        Thread(target=Broker.download_expiry_dates_bfo, args=(root,)).start()
