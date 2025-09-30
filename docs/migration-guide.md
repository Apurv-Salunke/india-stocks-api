# Migration Guide

## Overview

This guide explains how to migrate from older versions of the Indian Stock API to newer versions, including database schema changes, API changes, and breaking changes.

## Migration System

### Automatic Migrations
The package automatically runs migrations when imported:
```python
import india_stocks_api  # Migrations run automatically
```

### Manual Migrations
You can also run migrations manually:
```python
from india_stocks_api.database.migrations import MigrationManager

manager = MigrationManager("/path/to/database.db")
manager.run_all_migrations()
```

## Version History

### Version 1.0.0 → 1.1.0

#### Breaking Changes
- **Database Schema**: Added new columns to `broker_instruments` table
- **API Changes**: Modified instrument resolution methods
- **Cache Format**: Changed cache file structure

#### Migration Steps
1. **Backup your database**:
   ```bash
   cp instruments.db instruments.db.backup
   ```

2. **Update the package**:
   ```bash
   pip install --upgrade india-stocks-api
   ```

3. **Import the package** (migrations run automatically):
   ```python
   import india_stocks_api
   ```

4. **Verify the migration**:
   ```python
   from india_stocks_api.database.migrations import MigrationManager

   manager = MigrationManager()
   info = manager.get_database_info()
   print(f"Current version: {info['current_version']}")
   ```

#### Database Changes
```sql
-- New columns added to broker_instruments table
ALTER TABLE broker_instruments ADD COLUMN tick_size REAL;
ALTER TABLE broker_instruments ADD COLUMN lot_size INTEGER;
ALTER TABLE broker_instruments ADD COLUMN strike_price REAL;
ALTER TABLE broker_instruments ADD COLUMN option_type TEXT;
```

#### API Changes
- `resolve_equity_instrument()` now returns additional fields
- `resolve_fno_instrument()` now includes strike price and option type
- Cache format changed to include timestamps

### Version 1.1.0 → 1.2.0

#### Breaking Changes
- **Exchange Removal**: Removed USE exchange
- **Segment Consolidation**: BCD and CDS are now segments, not exchanges
- **Provider Renaming**: `AngelOneProvider` renamed to `AngelOneTokensManager`

#### Migration Steps
1. **Update imports**:
   ```python
   # Old
   from india_stocks_api.database import AngelOneProvider

   # New
   from india_stocks_api.database import AngelOneTokensManager
   ```

2. **Update class names**:
   ```python
   # Old
   provider = AngelOneProvider(service, "angelone")

   # New
   provider = AngelOneTokensManager(service, "angelone")
   ```

3. **Update database** (automatic):
   ```python
   import india_stocks_api  # Migrations run automatically
   ```

#### Database Changes
```sql
-- Remove USE exchange
DELETE FROM exchanges WHERE exchange_code = 'USE';

-- Remove BCD and CDS as separate exchanges
DELETE FROM exchanges WHERE exchange_code IN ('BCD', 'CDS');
```

#### API Changes
- `AngelOneProvider` class renamed to `AngelOneTokensManager`
- USE exchange no longer supported
- BCD and CDS are now segments, not exchanges

## Common Migration Issues

### Issue 1: Database Locked
**Error**: `sqlite3.OperationalError: database is locked`

**Solution**:
```python
import time
import sqlite3

def wait_for_database(db_path, timeout=30):
    """Wait for database to be unlocked"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            conn = sqlite3.connect(db_path, timeout=1)
            conn.close()
            return True
        except sqlite3.OperationalError:
            time.sleep(0.1)
    return False

# Usage
if wait_for_database("instruments.db"):
    # Proceed with migration
    pass
else:
    print("Database is locked, please try again later")
```

### Issue 2: Migration Already Applied
**Error**: `UNIQUE constraint failed: schema_version.version`

**Solution**:
```python
from india_stocks_api.database.migrations import MigrationManager

manager = MigrationManager()
info = manager.get_database_info()

if info['current_version'] >= 2:
    print("Migration already applied")
else:
    manager.run_all_migrations()
```

### Issue 3: Cache File Corruption
**Error**: `json.JSONDecodeError: Expecting value`

**Solution**:
```python
from india_stocks_api.database.providers import AngelOneTokensManager
from india_stocks_api.database import InstrumentService

# Clear corrupted cache
service = InstrumentService()
provider = AngelOneTokensManager(service, "angelone")
provider.clear_cache()

# Fetch fresh data
provider.fetch_equity_data(force_refresh=True)
```

### Issue 4: Missing Dependencies
**Error**: `ModuleNotFoundError: No module named 'tqdm'`

**Solution**:
```bash
pip install tqdm==4.67.1
```

## Migration Best Practices

### 1. Always Backup
```bash
# Create backup before migration
cp instruments.db instruments.db.backup
cp -r _cache _cache.backup
```

### 2. Test in Development
```python
# Test migration in development environment first
import india_stocks_api

# Verify everything works
from india_stocks_api import brokers
broker = brokers.AngelOne()
# Test basic functionality
```

### 3. Monitor Migration Progress
```python
from india_stocks_api.database.migrations import MigrationManager

manager = MigrationManager()
info = manager.get_database_info()

print(f"Current version: {info['current_version']}")
print(f"Pending migrations: {info['pending_migrations']}")
print(f"Database exists: {info['database_exists']}")
```

### 4. Verify Data Integrity
```python
from india_stocks_api.database import InstrumentService

service = InstrumentService()
stats = service.get_database_stats()

print(f"Total instruments: {stats['total_instruments']}")
print(f"Total broker instruments: {stats['total_broker_instruments']}")
print(f"Total exchanges: {stats['total_exchanges']}")
print(f"Total categories: {stats['total_categories']}")
```

## Rollback Procedures

### Database Rollback
```python
import shutil
import os

def rollback_database(backup_path, current_path):
    """Rollback database to previous version"""
    if os.path.exists(backup_path):
        shutil.copy2(backup_path, current_path)
        print("Database rolled back successfully")
    else:
        print("Backup file not found")

# Usage
rollback_database("instruments.db.backup", "instruments.db")
```

### Cache Rollback
```python
import shutil
import os

def rollback_cache(backup_path, current_path):
    """Rollback cache to previous version"""
    if os.path.exists(backup_path):
        shutil.rmtree(current_path)
        shutil.copytree(backup_path, current_path)
        print("Cache rolled back successfully")
    else:
        print("Cache backup not found")

# Usage
rollback_cache("_cache.backup", "_cache")
```

## Troubleshooting

### Check Migration Status
```python
from india_stocks_api.database.migrations import MigrationManager

manager = MigrationManager()
info = manager.get_database_info()

print("Migration Status:")
print(f"  Database exists: {info['database_exists']}")
print(f"  Current version: {info['current_version']}")
print(f"  Pending migrations: {info['pending_migrations']}")
print(f"  Migration files: {info['migration_files']}")
```

### Check Database Schema
```python
import sqlite3

def check_schema(db_path):
    """Check database schema"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print("Tables:", [table[0] for table in tables])

    # Check schema version
    cursor.execute("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
    version = cursor.fetchone()
    print(f"Schema version: {version[0] if version else 'None'}")

    conn.close()

# Usage
check_schema("instruments.db")
```

### Check Cache Status
```python
import json
import os
from datetime import datetime

def check_cache_status(cache_file):
    """Check cache file status"""
    if not os.path.exists(cache_file):
        print("Cache file does not exist")
        return

    try:
        with open(cache_file, 'r') as f:
            cache_data = json.load(f)

        timestamp = cache_data.get('timestamp')
        if timestamp:
            cache_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            print(f"Cache timestamp: {cache_time}")
            print(f"Cache age: {datetime.now() - cache_time}")

        data_count = len(cache_data.get('data', []))
        print(f"Cache data count: {data_count}")

    except json.JSONDecodeError:
        print("Cache file is corrupted")
    except Exception as e:
        print(f"Error reading cache: {e}")

# Usage
check_cache_status("_cache/angelone_tokens_cache.json")
```

## Performance Considerations

### Migration Performance
- Migrations run in transactions for atomicity
- Large datasets may take time to migrate
- Consider running migrations during off-peak hours
- Monitor disk space during migrations

### Post-Migration Optimization
```python
import sqlite3

def optimize_database(db_path):
    """Optimize database after migration"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Analyze tables for better query planning
    cursor.execute("ANALYZE")

    # Vacuum to reclaim space
    cursor.execute("VACUUM")

    conn.close()
    print("Database optimized")

# Usage
optimize_database("instruments.db")
```

## Support and Help

### Getting Help
- Check the [API Reference](api-reference.md) for detailed documentation
- Review the [Architecture Overview](architecture.md) for system understanding
- Check the [Database Schema](database-schema.md) for data structure details
- Look at the [Broker Integration Guide](broker-integration-guide.md) for implementation details

### Common Issues
- **Database locked**: Wait for other processes to finish
- **Migration failed**: Check error logs and retry
- **Cache corrupted**: Clear cache and refetch data
- **Missing dependencies**: Install required packages

### Reporting Issues
When reporting migration issues, include:
- Package version
- Database version
- Error messages
- Steps to reproduce
- System information
