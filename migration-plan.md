Excellent! Let's create a comprehensive implementation plan to build this SQLite-based instrument database system using AngelOne as our test broker. I'll break this down into phases with clear deliverables and testing strategies.


[1 tool called]

# 🚀 Implementation Plan: SQLite Instrument Database System

## 📋 **Project Overview**

**Goal**: Implement a comprehensive SQLite-based instrument database system to replace the current token dictionary approach, starting with AngelOne integration.

**Timeline**: 2-3 weeks with incremental testing
**Testing Strategy**: Unit tests + Integration tests + Live AngelOne API testing

---

## 🏗️ **Phase 1: Foundation Setup (Week 1, Days 1-2)**

### **1.1 Project Structure Setup**
```
india_stocks_api/
├── database/                          # NEW MODULE
│   ├── __init__.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── exchange.py
│   │   ├── instrument.py
│   │   ├── broker_instrument.py
│   │   └── enums.py
│   ├── migrations/
│   │   ├── __init__.py
│   │   ├── 001_initial_schema.sql
│   │   └── migration_manager.py
│   ├── providers/
│   │   ├── __init__.py
│   │   ├── base_provider.py
│   │   └── angelone_provider.py
│   ├── services/
│   │   ├── __init__.py
│   │   └── instrument_service.py
│   └── utils/
│       ├── __init__.py
│       └── db_utils.py
├── tests/
│   ├── database/                      # NEW TEST MODULE
│   │   ├── __init__.py
│   │   ├── test_models.py
│   │   ├── test_migrations.py
│   │   ├── test_angelone_provider.py
│   │   ├── test_instrument_service.py
│   │   └── fixtures/
│   │       ├── __init__.py
│   │       ├── sample_data.py
│   │       └── test_db.sqlite
│   └── integration/
│       ├── __init__.py
│       └── test_angelone_integration.py
├── scripts/                           # NEW UTILITY SCRIPTS
│   ├── __init__.py
│   ├── migrate_to_sqlite.py
│   ├── populate_angelone_data.py
│   └── test_angelone_connection.py
└── (existing files...)
```

### **1.2 Dependencies Update**
```python
# requirements.txt additions
sqlite3  # Built-in, but ensure version compatibility
pytest-sqlalchemy  # For database testing
faker  # For test data generation
```

### **1.3 Deliverables:**
- [ ] Create directory structure
- [ ] Set up `__init__.py` files
- [ ] Create base model classes
- [ ] Set up test structure
- [ ] Update requirements

### **1.4 Tests:**
```python
# tests/database/test_models.py
def test_base_model_creation():
    """Test base model functionality"""
    pass

def test_directory_structure():
    """Test that all required directories exist"""
    pass
```

---

## 🗄️ **Phase 2: Database Schema & Models (Week 1, Days 3-4)**

### **2.1 SQLite Schema Implementation**
```sql
-- database/migrations/001_initial_schema.sql
-- (Complete schema from previous design)
```

### **2.2 Model Classes**
```python
# database/models/enums.py
from enum import Enum

class Exchange(Enum):
    NSE = "NSE"
    BSE = "BSE"
    MCX = "MCX"
    NCDEX = "NCDEX"
    # ... etc

# database/models/instrument.py
from dataclasses import dataclass
from typing import Optional
from datetime import date
from .enums import Exchange, InstrumentCategory

@dataclass
class Instrument:
    """Comprehensive instrument model"""
    # ... (from previous design)
```

### **2.3 Migration Manager**
```python
# database/migrations/migration_manager.py
class MigrationManager:
    """Handles database schema migrations"""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def run_migrations(self):
        """Execute pending migrations"""
        # Implementation
        pass

    def get_current_version(self) -> int:
        """Get current database version"""
        # Implementation
        pass
```

### **2.4 Deliverables:**
- [ ] Complete SQLite schema file
- [ ] All model classes with proper validation
- [ ] Migration manager
- [ ] Database connection utilities

### **2.5 Tests:**
```python
# tests/database/test_models.py
def test_instrument_model():
    """Test instrument model creation and validation"""
    instrument = Instrument(
        standardized_symbol="RELIANCE",
        instrument_name="Reliance Industries Ltd",
        exchange=Exchange.NSE,
        category=InstrumentCategory.EQUITY
    )
    assert instrument.standardized_symbol == "RELIANCE"

def test_migration_manager():
    """Test migration manager functionality"""
    # Test migration execution
    pass

def test_database_creation():
    """Test database schema creation"""
    # Test that all tables are created correctly
    pass
```

---

## 🔌 **Phase 3: AngelOne Data Provider (Week 1, Days 5-7)**

### **3.1 Base Provider Interface**
```python
# database/providers/base_provider.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseProvider(ABC):
    """Abstract base class for data providers"""

    @abstractmethod
    def fetch_equity_data(self) -> List[Dict[str, Any]]:
        """Fetch equity instruments data"""
        pass

    @abstractmethod
    def fetch_fno_data(self) -> List[Dict[str, Any]]:
        """Fetch F&O instruments data"""
        pass

    @abstractmethod
    def fetch_commodity_data(self) -> List[Dict[str, Any]]:
        """Fetch commodity instruments data"""
        pass
```

### **3.2 AngelOne Provider Implementation**
```python
# database/providers/angelone_provider.py
import pandas as pd
from typing import List, Dict, Any
from ..models.instrument import Instrument, BrokerInstrument
from ..models.enums import Exchange, InstrumentCategory
from .base_provider import BaseProvider

class AngelOneProvider(BaseProvider):
    """AngelOne-specific data provider"""

    def __init__(self, symbol_db):
        self.db = symbol_db
        self.broker_name = "angelone"
        self.base_urls = {
            "market_data": "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        }

    def sync_all_instruments(self) -> Dict[str, int]:
        """Sync all instrument types from AngelOne"""
        results = {
            "equity": 0,
            "fno": 0,
            "commodity": 0,
            "currency": 0
        }

        # Fetch and sync each instrument type
        results["equity"] = self.sync_equity_instruments()
        results["fno"] = self.sync_fno_instruments()
        results["commodity"] = self.sync_commodity_instruments()
        results["currency"] = self.sync_currency_instruments()

        return results

    def sync_equity_instruments(self) -> int:
        """Sync equity instruments from AngelOne"""
        # Implementation using existing AngelOne logic
        pass

    def sync_fno_instruments(self) -> int:
        """Sync F&O instruments from AngelOne"""
        # Implementation
        pass
```

### **3.3 Deliverables:**
- [ ] Base provider interface
- [ ] AngelOne provider implementation
- [ ] Data transformation logic
- [ ] Error handling and retry logic

### **3.6 Tests:**
```python
# tests/database/test_angelone_provider.py
import pytest
from unittest.mock import patch, Mock

class TestAngelOneProvider:

    @pytest.fixture
    def provider(self):
        """Create AngelOne provider instance for testing"""
        return AngelOneProvider(Mock())

    def test_provider_initialization(self, provider):
        """Test provider initialization"""
        assert provider.broker_name == "angelone"
        assert provider.base_urls is not None

    @patch('requests.get')
    def test_fetch_equity_data(self, mock_get, provider):
        """Test equity data fetching"""
        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                "token": "2881",
                "symbol": "RELIANCE-EQ",
                "name": "Reliance Industries Ltd",
                "exch_seg": "NSE",
                "tick_size": "5.000000",
                "lotsize": "1"
            }
        ]
        mock_get.return_value = mock_response

        # Test data fetching
        data = provider.fetch_equity_data()
        assert len(data) == 1
        assert data[0]["symbol"] == "RELIANCE-EQ"

    def test_data_transformation(self, provider):
        """Test data transformation logic"""
        angelone_data = {
            "token": "2881",
            "symbol": "RELIANCE-EQ",
            "name": "Reliance Industries Ltd",
            "exch_seg": "NSE",
            "tick_size": "5.000000",
            "lotsize": "1"
        }

        transformed = provider._transform_equity_data(angelone_data)
        assert transformed["standardized_symbol"] == "RELIANCE"
        assert transformed["broker_token"] == "2881"
```

---

## 🔄 **Phase 4: Instrument Service Layer (Week 2, Days 1-2)**

### **4.1 Instrument Service Implementation**
```python
# database/services/instrument_service.py
import sqlite3
from typing import List, Optional, Dict, Any
from ..models.instrument import Instrument, BrokerInstrument
from ..models.enums import Exchange, InstrumentCategory

class InstrumentService:
    """Main service for instrument operations"""

    def __init__(self, db_path: str = "instruments.db"):
        self.db_path = db_path
        self.init_database()

    def resolve_instrument(self, standardized_symbol: str, broker_name: str,
                          exchange: Exchange, category: InstrumentCategory,
                          **kwargs) -> Optional[Dict[str, Any]]:
        """Resolve standardized symbol to broker format"""
        # Implementation
        pass

    def search_instruments(self, query: str, broker_name: str,
                          exchange: Optional[Exchange] = None,
                          category: Optional[InstrumentCategory] = None) -> List[Dict[str, Any]]:
        """Search instruments with filters"""
        # Implementation
        pass
```

### **4.2 Deliverables:**
- [ ] Complete instrument service
- [ ] Query optimization
- [ ] Caching layer
- [ ] Error handling

### **4.3 Tests:**
```python
# tests/database/test_instrument_service.py
class TestInstrumentService:

    @pytest.fixture
    def service(self):
        """Create instrument service for testing"""
        return InstrumentService(":memory:")  # Use in-memory DB for testing

    def test_resolve_equity_symbol(self, service):
        """Test equity symbol resolution"""
        # Add test data
        # Test resolution
        pass

    def test_search_functionality(self, service):
        """Test symbol search"""
        # Test search with various filters
        pass
```

---

## 🧪 **Phase 5: Migration & Integration (Week 2, Days 3-5)**

### **5.1 Migration Script**
```python
# scripts/migrate_to_sqlite.py
#!/usr/bin/env python3
"""
Migration script to move from token dictionaries to SQLite database
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from india_stocks_api.database import InstrumentDatabase
from india_stocks_api.database.providers import AngelOneProvider

def migrate_angelone_data():
    """Migrate AngelOne data to SQLite"""
    print("Starting AngelOne data migration...")

    # Initialize database
    db = InstrumentDatabase()

    # Initialize provider
    provider = AngelOneProvider(db)

    # Sync all data
    results = provider.sync_all_instruments()

    print(f"Migration completed:")
    print(f"  Equity instruments: {results['equity']}")
    print(f"  F&O instruments: {results['fno']}")
    print(f"  Commodity instruments: {results['commodity']}")
    print(f"  Currency instruments: {results['currency']}")

if __name__ == "__main__":
    migrate_angelone_data()
```

### **5.2 AngelOne Broker Integration**
```python
# brokers/angelone.py (MODIFIED)
from india_stocks_api.database import InstrumentService
from india_stocks_api.database.models.enums import Exchange, InstrumentCategory

class AngelOne(Broker):
    def __init__(self):
        self.instrument_service = InstrumentService()
        self.broker_name = "angelone"

    def market_order_eq(self, exchange: str, symbol: str, quantity: int,
                       side: str, unique_id: str, headers: dict, ...):
        """Place market order using new database system"""

        # Resolve symbol using new service
        symbol_data = self.instrument_service.resolve_instrument(
            standardized_symbol=symbol,
            broker_name=self.broker_name,
            exchange=Exchange.NSE if exchange == "NSE" else Exchange.BSE,
            category=InstrumentCategory.EQUITY
        )

        if not symbol_data:
            raise ValueError(f"Symbol {symbol} not found for {self.broker_name}")

        # Create order with resolved data
        json_data = {
            "symboltoken": symbol_data["broker_token"],
            "exchange": exchange,
            "tradingsymbol": symbol_data["broker_symbol"],
            # ... rest of order data
        }

        # Continue with existing order logic
        response = self.fetch(
            method="POST",
            url=self.urls["place_order"],
            json=json_data,
            headers=headers["headers"],
        )

        return self._create_order_parser(response=response, headers=headers)
```

### **5.3 Testing Script**
```python
# scripts/test_angelone_connection.py
#!/usr/bin/env python3
"""
Test script to verify AngelOne integration with new database
"""

import os
from india_stocks_api.database import InstrumentService
from india_stocks_api.brokers.angelone import AngelOne

def test_angelone_integration():
    """Test AngelOne integration with real credentials"""

    # Initialize services
    instrument_service = InstrumentService()
    broker = AngelOne()

    # Test 1: Symbol resolution
    print("Testing symbol resolution...")
    rel_data = instrument_service.resolve_instrument(
        standardized_symbol="RELIANCE",
        broker_name="angelone",
        exchange=Exchange.NSE,
        category=InstrumentCategory.EQUITY
    )

    if rel_data:
        print(f"✅ RELIANCE resolved: {rel_data['broker_symbol']} (Token: {rel_data['broker_token']})")
    else:
        print("❌ RELIANCE resolution failed")

    # Test 2: Search functionality
    print("\nTesting search functionality...")
    search_results = instrument_service.search_instruments("TCS", "angelone")
    print(f"✅ Found {len(search_results)} TCS symbols")

    # Test 3: Broker integration (without placing actual orders)
    print("\nTesting broker integration...")
    try:
        # This would normally place an order, but we'll just test symbol resolution
        headers = broker.generate_headers({
            'user_id': os.getenv('ANGELONE_USER_ID'),
            'pin': os.getenv('ANGELONE_PIN'),
            'totpstr': os.getenv('ANGELONE_TOTP'),
            'api_key': os.getenv('ANGELONE_API_KEY')
        })

        # Test symbol resolution in broker context
        symbol_data = broker.instrument_service.resolve_instrument(
            standardized_symbol="RELIANCE",
            broker_name="angelone",
            exchange=Exchange.NSE,
            category=InstrumentCategory.EQUITY
        )

        if symbol_data:
            print("✅ Broker integration successful")
        else:
            print("❌ Broker integration failed")

    except Exception as e:
        print(f"❌ Broker integration error: {e}")

if __name__ == "__main__":
    test_angelone_integration()
```

### **5.4 Deliverables:**
- [ ] Migration script
- [ ] Updated AngelOne broker
- [ ] Integration testing script
- [ ] Documentation

---

## 🧪 **Phase 6: Comprehensive Testing (Week 2, Days 6-7)**

### **6.1 Unit Tests**
```python
# tests/database/test_complete_system.py
class TestCompleteSystem:

    def test_end_to_end_workflow(self):
        """Test complete workflow from data fetch to symbol resolution"""
        # 1. Initialize services
        # 2. Fetch data from AngelOne
        # 3. Store in database
        # 4. Resolve symbols
        # 5. Verify results
        pass

    def test_performance(self):
        """Test database performance with large datasets"""
        # Test query performance
        # Test memory usage
        # Test concurrent access
        pass
```

### **6.2 Integration Tests**
```python
# tests/integration/test_angelone_integration.py
class TestAngelOneIntegration:

    @pytest.mark.integration
    def test_live_api_connection(self):
        """Test live connection to AngelOne API"""
        # Requires real credentials
        pass

    @pytest.mark.integration
    def test_data_sync(self):
        """Test data synchronization from AngelOne"""
        # Test fetching and storing real data
        pass
```

### **6.3 Performance Tests**
```python
# tests/performance/test_database_performance.py
import time
import pytest

class TestDatabasePerformance:

    def test_query_performance(self):
        """Test database query performance"""
        service = InstrumentService()

        # Test symbol resolution speed
        start_time = time.time()
        for _ in range(1000):
            service.resolve_instrument("RELIANCE", "angelone", Exchange.NSE, InstrumentCategory.EQUITY)
        end_time = time.time()

        avg_time = (end_time - start_time) / 1000
        assert avg_time < 0.01  # Should be under 10ms per query

    def test_search_performance(self):
        """Test search performance"""
        # Test search speed with various query sizes
        pass
```

---

## 📊 **Phase 7: Documentation & Deployment (Week 3)**

### **7.1 Documentation**
- [ ] API documentation
- [ ] Database schema documentation
- [ ] Migration guide
- [ ] Usage examples

### **7.2 Deployment Scripts**
```python
# scripts/setup_database.py
#!/usr/bin/env python3
"""
Setup script for production database
"""

def setup_production_database():
    """Setup production database with initial data"""
    # 1. Create database
    # 2. Run migrations
    # 3. Populate initial data
    # 4. Verify setup
    pass
```

---

## 🎯 **Testing Strategy**

### **Test Categories:**
1. **Unit Tests**: Individual component testing
2. **Integration Tests**: Component interaction testing
3. **Performance Tests**: Speed and memory testing
4. **Live API Tests**: Real AngelOne API testing (with credentials)

### **Test Environment Setup:**
```python
# conftest.py
import pytest
import tempfile
import os

@pytest.fixture(scope="session")
def test_db_path():
    """Create temporary database for testing"""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        yield tmp.name
    os.unlink(tmp.name)

@pytest.fixture
def instrument_service(test_db_path):
    """Create instrument service with test database"""
    return InstrumentService(test_db_path)
```

---

## 🚀 **Execution Timeline**

| Phase | Duration | Deliverables | Testing |
|-------|----------|--------------|---------|
| 1 | 2 days | Project structure, base models | Unit tests |
| 2 | 2 days | Database schema, models | Model tests |
| 3 | 3 days | AngelOne provider | Provider tests |
| 4 | 2 days | Instrument service | Service tests |
| 5 | 3 days | Migration, integration | Integration tests |
| 6 | 2 days | Comprehensive testing | Performance tests |
| 7 | 3 days | Documentation, deployment | Final validation |

**Total: ~17 days (2.5 weeks)**

---

## 🔧 **Development Setup**

### **Environment Variables:**
```bash
# .env file
ANGELONE_USER_ID=your_user_id
ANGELONE_PIN=your_pin
ANGELONE_TOTP=your_totp_secret
ANGELONE_API_KEY=your_api_key
```

### **Development Commands:**
```bash
# Setup
python scripts/setup_database.py

# Run tests
pytest tests/database/
pytest tests/integration/

# Run migration
python scripts/migrate_to_sqlite.py

# Test connection
python scripts/test_angelone_connection.py
```

This plan provides a structured approach to implementing the SQLite-based instrument database system with comprehensive testing and AngelOne integration. Each phase builds upon the previous one, ensuring we can test incrementally and catch issues early.


[1 tool called]

Would you like me to start implementing Phase 1 (Foundation Setup) right away, or would you prefer to review and modify any part of this plan first?
