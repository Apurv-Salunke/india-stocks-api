"""Canonical response objects returned by public broker methods."""

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(slots=True, frozen=True)
class QuoteResponse:
    bid: float
    ask: float
    open: float
    high: float
    low: float
    ltp: float
    prev_close: float
    volume: int
    oi: int


@dataclass(slots=True, frozen=True)
class DepthLevel:
    price: float
    quantity: int


@dataclass(slots=True, frozen=True)
class DepthResponse:
    bids: tuple[DepthLevel, ...]
    asks: tuple[DepthLevel, ...]
    high: float
    low: float
    ltp: float
    ltq: int
    open: float
    prev_close: float
    volume: int
    oi: int
    total_buy_qty: int
    total_sell_qty: int


@dataclass(slots=True, frozen=True)
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    oi: int


@dataclass(slots=True, frozen=True)
class HistoryResponse:
    symbol: str
    exchange: str
    interval: str
    candles: tuple[Candle, ...]

    def to_dataframe(self) -> pd.DataFrame:
        rows = [
            {
                "timestamp": c.timestamp,
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
                "volume": c.volume,
                "oi": c.oi,
            }
            for c in self.candles
        ]
        return pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])


@dataclass(slots=True, frozen=True)
class FundsResponse:
    available_cash: float
    collateral: float
    m2m_realized: float
    m2m_unrealized: float
    utilized_debits: float


@dataclass(slots=True, frozen=True)
class ProfileResponse:
    client_code: str | None
    name: str | None
    exchanges: tuple[str, ...]
    products: tuple[str, ...]
    email: str | None
    mobile: str | None
    raw: dict[str, Any]
