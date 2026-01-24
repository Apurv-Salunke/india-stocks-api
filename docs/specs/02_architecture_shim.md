# Architecture: Information Hiding & The "Shim"

To use the existing OpenAlgo broker files without modification (or with minimal modification), we must provide replacements for the internal modules they import.

## The Dependency Problem

The current broker adapters import:

- `database.token_db` (for symbol mapping)
- `database.auth_db` (for credentials)
- `utils.httpx_client` (for networking)
- `utils.logging` (for logging)

## The Solution: `openalgo_brokers.internal`

We will move the raw broker code into an `internal/` hidden directory and patch the imports to point to a new "Shim" or "Context" module.

### Structure

```text
openalgo_brokers/
├── __init__.py
├── adapters/            # Public facing clean adapters
│   ├── zerodha.py
│   └── angel.py
└── internal/            # Hidden implementation details
    ├── context.py       # The Shim
    ├── zerodha/         # Raw code from OpenAlgo
    └── ...
```

### The Shim (`context.py`)

This module provides the necessary functions that the raw broker code expects, but redirects them to the `Broker` instance's local state.

**Example Shim Implementation:**

```python
# openalgo_brokers/internal/context.py

def get_httpx_client():
    # Returns the standard httpx client configured for this wrapper
    pass

def get_auth_token(user_id):
    # Returns the token stored in the adapter instance
    pass

def get_br_symbol(symbol, exchange):
    # Calls the user-provided symbol mapper or returns identity
    pass
```

### Build Process

An extraction script will:

1. Copy `broker/<name>` to `openalgo_brokers/src/openalgo_brokers/internal/<name>`.
2. Rewrite imports in the copied files:
   - `from database.token_db import ...` -> `from openalgo_brokers.internal.context import ...`
   - `from utils.httpx_client import ...` -> `from openalgo_brokers.internal.context import ...`

This architecture allows the core logic to remain "tested" and "active" while severing ties to the Flask app.
