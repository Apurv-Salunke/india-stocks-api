"""India Stocks API - A Python library for trading Indian stocks.

This library provides a unified interface for trading stocks in India
through various brokers like Angel One, etc.
"""

__version__ = "0.1.0"
__author__ = "Apurv Salunke"
__license__ = "MIT"

from india_stocks_api import brokers
from india_stocks_api import config

__all__ = [
    "brokers",
    "config",
]
