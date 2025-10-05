"""
File and formatting utilities for India Stocks API
"""

from pathlib import Path


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
