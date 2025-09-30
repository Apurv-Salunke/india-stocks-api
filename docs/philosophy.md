# Design Philosophy

## Core Principles

### 1. Unified Interface
The library provides a single, consistent API across all Indian brokers. Users don't need to learn different APIs for different brokers.

### 2. Broker Abstraction
We abstract away broker-specific details like:
- Symbol formats (RELIANCE vs RELIANCE-EQ vs RELIANCE.NS)
- Token management and resolution
- Request/response format differences
- Authentication methods

### 3. Easy Broker Switching
Users can switch brokers with minimal code changes - just change the broker class instantiation.

### 4. Standardized Data
All brokers return data in the same format, regardless of their internal representations.

## Architecture Philosophy

### Base Provider Pattern
We use a base provider pattern where:
- **BaseProvider**: Contains common functionality (caching, storage, utilities)
- **Broker-specific classes**: Handle broker-specific API integration
- **InstrumentStore**: Manages database operations
- **MigrationManager**: Handles schema versioning

### Separation of Concerns
- **Data Fetching**: Broker-specific API calls
- **Data Transformation**: Standardizing broker data to common format
- **Data Storage**: Database operations and caching
- **Data Resolution**: Converting standard symbols to broker tokens

### Database-First Approach
- All instrument data is stored in SQLite database
- Automatic migrations ensure schema consistency
- Date-based caching for optimal performance
- WAL mode for concurrent access

## Design Decisions

### Why Custom JSON Parsing?
Instead of using `json.loads()` directly, we use custom parsing methods to:
- Handle broker-specific data formats
- Provide better error handling
- Standardize data structures
- Add validation and type checking

### Why SQLite?
- **Lightweight**: No external database server required
- **Fast**: Optimized for read-heavy workloads
- **Reliable**: ACID compliance and crash recovery
- **Portable**: Single file database

### Why Date-Based Caching?
- **Indian Market Hours**: Cache invalidates at midnight IST
- **Performance**: Avoids unnecessary API calls
- **Reliability**: Works even if API is temporarily unavailable
- **Cost**: Reduces API rate limit usage

## Future Considerations

### Extensibility
The architecture is designed to easily add new brokers by:
1. Extending BaseProvider
2. Implementing broker-specific methods
3. Adding symbol mapping logic
4. Testing with real data

### Scalability
- Database can handle millions of instruments
- Caching reduces API load
- WAL mode supports concurrent access
- Migration system handles schema evolution

### Maintainability
- Clear separation of concerns
- Comprehensive testing
- Automatic migrations
- Standardized error handling
