"""
Tests for common utility functions
"""
from pathlib import Path
from datetime import datetime
from india_stocks_api.internal.utils.common import (
    get_cache_directory,
    ensure_directory_exists,
    format_currency,
    format_percentage,
    safe_divide,
    get_india_time,
    utc_to_ist,
    ist_to_utc
)
import pytz


class TestGetCacheDirectory:
    """Test cases for get_cache_directory function"""

    def test_get_cache_directory_returns_path(self):
        """Test that get_cache_directory returns a Path object"""
        cache_dir = get_cache_directory()
        assert isinstance(cache_dir, Path)
        assert cache_dir.name == "_cache"

    def test_get_cache_directory_creates_directory(self):
        """Test that get_cache_directory creates the directory if it doesn't exist"""
        cache_dir = get_cache_directory()
        assert cache_dir.exists()
        assert cache_dir.is_dir()


class TestEnsureDirectoryExists:
    """Test cases for ensure_directory_exists function"""

    def test_ensure_directory_exists_creates_directory(self, tmp_path):
        """Test that ensure_directory_exists creates directory"""
        test_dir = tmp_path / "test_dir"
        result = ensure_directory_exists(test_dir)
        assert result.exists()
        assert result.is_dir()
        assert result == test_dir

    def test_ensure_directory_exists_creates_nested_directories(self, tmp_path):
        """Test that ensure_directory_exists creates nested directories"""
        nested_dir = tmp_path / "level1" / "level2" / "level3"
        result = ensure_directory_exists(nested_dir)
        assert result.exists()
        assert result.is_dir()

    def test_ensure_directory_exists_returns_path(self, tmp_path):
        """Test that ensure_directory_exists returns Path object"""
        test_dir = tmp_path / "test_dir"
        result = ensure_directory_exists(test_dir)
        assert isinstance(result, Path)


class TestFormatCurrency:
    """Test cases for format_currency function"""

    def test_format_currency_default(self):
        """Test format_currency with default currency"""
        assert format_currency(1000.50) == "₹1,000.50"
        assert format_currency(1234567.89) == "₹1,234,567.89"

    def test_format_currency_custom_currency(self):
        """Test format_currency with custom currency"""
        assert format_currency(1000.50, "$") == "$1,000.50"
        assert format_currency(500, "USD ") == "USD 500.00"

    def test_format_currency_zero(self):
        """Test format_currency with zero"""
        assert format_currency(0) == "₹0.00"

    def test_format_currency_negative(self):
        """Test format_currency with negative values"""
        assert format_currency(-1000.50) == "₹-1,000.50"

    def test_format_currency_large_number(self):
        """Test format_currency with large numbers"""
        assert format_currency(999999999.99) == "₹999,999,999.99"


class TestFormatPercentage:
    """Test cases for format_percentage function"""

    def test_format_percentage_default(self):
        """Test format_percentage with default decimals"""
        assert format_percentage(0.05) == "5.00%"
        assert format_percentage(0.1234) == "12.34%"

    def test_format_percentage_custom_decimals(self):
        """Test format_percentage with custom decimal places"""
        assert format_percentage(0.05, 0) == "5%"
        assert format_percentage(0.1234, 1) == "12.3%"
        assert format_percentage(0.1234, 4) == "12.3400%"

    def test_format_percentage_zero(self):
        """Test format_percentage with zero"""
        assert format_percentage(0) == "0.00%"

    def test_format_percentage_one(self):
        """Test format_percentage with 1.0 (100%)"""
        assert format_percentage(1.0) == "100.00%"

    def test_format_percentage_negative(self):
        """Test format_percentage with negative values"""
        assert format_percentage(-0.05) == "-5.00%"


class TestSafeDivide:
    """Test cases for safe_divide function"""

    def test_safe_divide_normal_division(self):
        """Test safe_divide with normal division"""
        assert safe_divide(10, 2) == 5.0
        assert safe_divide(15, 3) == 5.0
        assert safe_divide(7, 2) == 3.5

    def test_safe_divide_zero_denominator_default(self):
        """Test safe_divide with zero denominator and default value"""
        assert safe_divide(10, 0) == 0.0
        assert safe_divide(10, 0, default=100.0) == 100.0
        assert safe_divide(10, 0, default=-1) == -1.0

    def test_safe_divide_zero_numerator(self):
        """Test safe_divide with zero numerator"""
        assert safe_divide(0, 5) == 0.0

    def test_safe_divide_negative_values(self):
        """Test safe_divide with negative values"""
        assert safe_divide(-10, 2) == -5.0
        assert safe_divide(10, -2) == -5.0
        assert safe_divide(-10, -2) == 5.0

    def test_safe_divide_float_result(self):
        """Test safe_divide returns float"""
        result = safe_divide(10, 2)
        assert isinstance(result, float)


class TestGetIndiaTime:
    """Test cases for get_india_time function"""

    def test_get_india_time_returns_datetime(self):
        """Test that get_india_time returns a datetime object"""
        result = get_india_time()
        assert isinstance(result, datetime)

    def test_get_india_time_has_timezone(self):
        """Test that get_india_time returns datetime with timezone"""
        result = get_india_time()
        assert result.tzinfo is not None
        assert result.tzinfo == pytz.timezone("Asia/Kolkata")


class TestUtcToIst:
    """Test cases for utc_to_ist function"""

    def test_utc_to_ist_with_timezone(self):
        """Test utc_to_ist with UTC datetime that has timezone"""
        utc_dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
        ist_dt = utc_to_ist(utc_dt)
        assert ist_dt.tzinfo == pytz.timezone("Asia/Kolkata")
        # IST is UTC+5:30
        assert ist_dt.hour == 17
        assert ist_dt.minute == 30

    def test_utc_to_ist_without_timezone(self):
        """Test utc_to_ist with naive datetime (assumes UTC)"""
        naive_dt = datetime(2024, 1, 1, 12, 0, 0)
        ist_dt = utc_to_ist(naive_dt)
        assert ist_dt.tzinfo is not None
        assert ist_dt.tzinfo == pytz.timezone("Asia/Kolkata")

    def test_utc_to_ist_returns_datetime(self):
        """Test that utc_to_ist returns datetime"""
        utc_dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
        result = utc_to_ist(utc_dt)
        assert isinstance(result, datetime)


class TestIstToUtc:
    """Test cases for ist_to_utc function"""

    def test_ist_to_utc_with_timezone(self):
        """Test ist_to_utc with IST datetime that has timezone"""
        ist = pytz.timezone("Asia/Kolkata")
        ist_dt = datetime(2024, 1, 1, 17, 30, 0, tzinfo=ist)
        utc_dt = ist_to_utc(ist_dt)
        assert utc_dt.tzinfo == pytz.UTC
        # IST is UTC+5:30, so 17:30 IST = 12:00 UTC
        assert utc_dt.hour == 12
        assert utc_dt.minute == 0

    def test_ist_to_utc_without_timezone(self):
        """Test ist_to_utc with naive datetime (assumes IST)"""
        naive_dt = datetime(2024, 1, 1, 17, 30, 0)
        utc_dt = ist_to_utc(naive_dt)
        assert utc_dt.tzinfo is not None
        assert utc_dt.tzinfo == pytz.UTC

    def test_ist_to_utc_returns_datetime(self):
        """Test that ist_to_utc returns datetime"""
        ist = pytz.timezone("Asia/Kolkata")
        ist_dt = datetime(2024, 1, 1, 17, 30, 0, tzinfo=ist)
        result = ist_to_utc(ist_dt)
        assert isinstance(result, datetime)

    def test_utc_ist_roundtrip(self):
        """Test that converting UTC to IST and back gives original"""
        utc_dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
        ist_dt = utc_to_ist(utc_dt)
        back_to_utc = ist_to_utc(ist_dt)
        assert back_to_utc == utc_dt
