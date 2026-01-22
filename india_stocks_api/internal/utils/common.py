"""
File and formatting utilities for India Stocks API
"""

from pathlib import Path
from datetime import datetime
import pytz

# India timezone
IST = pytz.timezone("Asia/Kolkata")


def get_cache_directory() -> Path:
    """Get cache directory path"""
    cache_dir = Path("_cache")
    cache_dir.mkdir(exist_ok=True)
    return cache_dir


def ensure_directory_exists(path: Path) -> Path:
    """
    Ensure directory exists

    Args:
        path: Directory path

    Returns:
        Path object
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def format_currency(amount: float, currency: str = "₹") -> str:
    """
    Format currency amount

    Args:
        amount: Amount to format
        currency: Currency symbol

    Returns:
        Formatted currency string
    """
    return f"{currency}{amount:,.2f}"


def format_percentage(value: float, decimals: int = 2) -> str:
    """
    Format percentage value

    Args:
        value: Percentage value (e.g., 0.05 for 5%)
        decimals: Number of decimal places

    Returns:
        Formatted percentage string
    """
    return f"{value * 100:.{decimals}f}%"


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safe division with default value for zero denominator

    Args:
        numerator: Numerator
        denominator: Denominator
        default: Default value if denominator is zero

    Returns:
        Division result or default value
    """
    return numerator / denominator if denominator != 0 else default


def get_india_time() -> datetime:
    """Get current time in India timezone (IST)"""
    return datetime.now(IST)


def utc_to_ist(utc_datetime: datetime) -> datetime:
    """Convert UTC datetime to India timezone"""
    if utc_datetime.tzinfo is None:
        utc_datetime = pytz.utc.localize(utc_datetime)
    return utc_datetime.astimezone(IST)


def ist_to_utc(ist_datetime: datetime) -> datetime:
    """Convert India timezone datetime to UTC"""
    if ist_datetime.tzinfo is None:
        ist_datetime = IST.localize(ist_datetime)
    return ist_datetime.astimezone(pytz.UTC)
