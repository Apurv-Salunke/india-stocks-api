"""
Pytest configuration and fixtures
"""

import pytest
import tempfile
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


@pytest.fixture(scope="session")
def test_db_path():
    """Create temporary database path for testing"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        yield tmp.name
    # Clean up after tests
    if os.path.exists(tmp.name):
        os.unlink(tmp.name)


@pytest.fixture
def instrument_service(test_db_path):
    """Create instrument service with test database"""
    from india_stocks_api.database import InstrumentService

    return InstrumentService(test_db_path)


@pytest.fixture
def migration_manager(test_db_path):
    """Create migration manager with test database"""
    from india_stocks_api.database.migrations import MigrationManager

    return MigrationManager(test_db_path)


@pytest.fixture(scope="session")
def sample_instrument_data():
    """Sample instrument data for testing"""
    return {
        "standardized_symbol": "RELIANCE",
        "instrument_name": "Reliance Industries Ltd",
        "exchange_id": 1,
        "category_id": 1,
        "isin": "INE002A01018",
        "sector": "Oil & Gas",
        "industry": "Refineries",
    }


@pytest.fixture(scope="session")
def sample_broker_instrument_data():
    """Sample broker instrument data for testing"""
    return {
        "instrument_id": 1,
        "broker_name": "angelone",
        "broker_symbol": "RELIANCE-EQ",
        "broker_token": "2881",
        "tick_size": 0.05,
        "lot_size": 1,
    }


@pytest.fixture
def mock_angelone_data():
    """Mock AngelOne API data for testing"""
    return [
        {
            "token": "2881",
            "symbol": "RELIANCE-EQ",
            "name": "Reliance Industries Ltd",
            "expiry": "",
            "strike": "-1.000000",
            "lotsize": "1",
            "instrumenttype": "EQ",
            "exch_seg": "NSE",
            "tick_size": "5.000000",
        },
        {
            "token": "738561",
            "symbol": "TCS-EQ",
            "name": "Tata Consultancy Services Ltd",
            "expiry": "",
            "strike": "-1.000000",
            "lotsize": "1",
            "instrumenttype": "EQ",
            "exch_seg": "NSE",
            "tick_size": "5.000000",
        },
    ]


# Markers for different test types
def pytest_configure(config):
    """Configure pytest markers"""
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "slow: marks tests as slow running")
    config.addinivalue_line(
        "markers", "requires_credentials: marks tests that require API credentials"
    )
