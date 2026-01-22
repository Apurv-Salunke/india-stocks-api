"""
Pytest configuration and shared fixtures
"""
import pytest
from pathlib import Path
import tempfile
import shutil


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing"""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def sample_order_details():
    """Sample order details for testing"""
    return {
        "symbol": "RELIANCE",
        "exchange": "NSE",
        "quantity": 10,
        "price": 2500.0,
        "transaction_type": "BUY",
        "order_type": "LIMIT",
        "product_type": "INTRADAY"
    }


@pytest.fixture
def sample_positions():
    """Sample positions data for testing"""
    return [
        {
            "symbol": "RELIANCE",
            "exchange": "NSE",
            "quantity": 10,
            "average_price": 2500.0
        },
        {
            "symbol": "TCS",
            "exchange": "NSE",
            "quantity": 5,
            "average_price": 3500.0
        }
    ]
