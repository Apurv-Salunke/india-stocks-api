"""
Shim Context for Internal OpenAlgo Code.
This module mocks/replaces the `database` and `utils` dependencies that
the ported OpenAlgo broker code expects.

It also acts as the single source of truth for broker credentials.
"""
import json
import httpx
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from india_stocks_api.instruments.database import InstrumentDB

# --- Shared State (Global for simplicity in Shim) ---
_CONFIG = {
    "access_token": None,
    "feed_token": None,
    # In a real implementation, this would be a proper mapped object or DB
    "symbol_map": {} 
}

_HTTP_CLIENT: Optional[httpx.Client] = None

# --- Credential Persistence (creds.json) ---

_CREDS_CACHE: Dict[str, Dict[str, Any]] = {}
_CREDS_LOADED: bool = False
_CREDS_FILE_NAME = Path(__file__).parent.parent.parent / "_cache" / "creds.json"
_CREDS_FILE_NAME.parent.mkdir(parents=True, exist_ok=True)

def _get_creds_path() -> Path:
    """
    Get the filesystem path for the credentials JSON cache, creating its parent directory if necessary.
    
    Returns:
        Path: Path to the creds.json file used to persist broker credentials.
    """
    return Path(_CREDS_FILE_NAME)


def _load_all_creds() -> Dict[str, Dict[str, Any]]:
    """
    Load and cache all broker credentials from the creds.json file.
    
    Reads creds.json (creating an empty in-memory cache if the file is missing, unreadable, or contains non-dict data), stores the result in the module-level _CREDS_CACHE, marks credentials as loaded by setting _CREDS_LOADED to True, and returns the in-memory credentials mapping. Subsequent calls return the cached mapping without re-reading the file.
    
    Returns:
        Dict[str, Dict[str, Any]]: A mapping from broker identifier to its credentials dictionary; empty if no valid data was found.
    """
    global _CREDS_LOADED, _CREDS_CACHE
    if _CREDS_LOADED:
        return _CREDS_CACHE

    path = _get_creds_path()
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                # Ensure nested structure is dict-of-dicts
                _CREDS_CACHE = {
                    str(broker): (creds if isinstance(creds, dict) else {})
                    for broker, creds in data.items()
                }
            else:
                _CREDS_CACHE = {}
        except Exception:
            # Corrupt or unreadable file – fail soft and start fresh in memory
            _CREDS_CACHE = {}
    else:
        _CREDS_CACHE = {}

    _CREDS_LOADED = True
    return _CREDS_CACHE


def _flush_creds() -> None:
    """
    Persist the in-memory credentials cache to the creds.json file.
    
    Writes the contents of _CREDS_CACHE to the path returned by _get_creds_path().
    If writing fails, logs a warning and suppresses the exception so callers treat persistence as best-effort.
    """
    path = _get_creds_path()
    try:
        path.write_text(json.dumps(_CREDS_CACHE, indent=2), encoding="utf-8")
    except Exception as e:
        # Persistence failure should not crash trading flows
        # Callers treat this as best-effort storage.
        logging.getLogger(__name__).warning(f"Failed to save credentials to {path}: {e}")


def set_credentials(broker: str, creds: Dict[str, Any]) -> None:
    """
    Store credentials for the specified broker in the in-memory cache and persist them to creds.json.
    
    Parameters:
    	broker (str): Broker identifier under which to store the credentials.
    	creds (Dict[str, Any]): Mapping of credential fields (e.g., api_key, secret). A shallow copy of this mapping is stored to avoid accidental external mutation.
    """
    global _CREDS_CACHE
    all_creds = _load_all_creds()
    # Store a shallow copy to avoid accidental external mutation
    all_creds[broker] = dict(creds or {})
    _CREDS_CACHE = all_creds
    _flush_creds()


def get_credentials(broker: str) -> Dict[str, Any]:
    """
    Return a copy of the stored credentials for the given broker.
    
    Parameters:
        broker (str): Broker identifier whose credentials should be retrieved.
    
    Returns:
        creds (Dict[str, Any]): A copy of the broker's credential dictionary; empty dict if none are stored.
    """
    all_creds = _load_all_creds()
    creds = all_creds.get(broker) or {}
    # Return a copy so callers cannot mutate internal state
    return dict(creds)


def remove_credentials(broker: str) -> None:
    """
    Remove credentials associated with the given broker and persist the change to the credentials cache file.
    
    This updates the in-memory credentials cache and writes the modified cache to creds.json. If no credentials exist for the broker, the function does nothing.
    
    Parameters:
        broker (str): Broker identifier whose stored credentials should be removed.
    """
    global _CREDS_CACHE
    all_creds = _load_all_creds()
    if broker in all_creds:
        del all_creds[broker]
        _CREDS_CACHE = all_creds
        _flush_creds()


def get_api_key(broker: str) -> Optional[str]:
    """
    Retrieve the stored API key for the specified broker.
    
    Parameters:
        broker (str): Broker identifier whose credentials are queried.
    
    Returns:
        api_key (Optional[str]): The stored `api_key` for the broker if present, `None` otherwise.
    """
    creds = get_credentials(broker)
    return creds.get("api_key")


# --- Configuration Setters (Called by Broker Adapter) ---

# def set_api_key(key: str):
#     # Legacy runtime-only storage for internal code that expects this.
#     _CONFIG["api_key"] = key

def set_auth_token(token: str):
    """
    Store the access token used for authenticated API requests.
    
    Parameters:
        token (str): Access token string to use for subsequent authenticated operations.
    """
    _CONFIG["access_token"] = token

def set_feed_token(token: str):
    _CONFIG["feed_token"] = token

# --- Shimmed Functions (Replacements for OpenAlgo imports) ---

def get_auth_token(user_id: Optional[str] = None) -> Optional[str]:
    """Replacement for database.auth_db.get_auth_token"""
    # The ported code expects a user_id, but in this standalone package,
    # we just return the active token set by the client.
    return _CONFIG["access_token"]

def get_feed_token(user_id: Optional[str] = None) -> Optional[str]:
    """Replacement for database.auth_db.get_feed_token"""
    return _CONFIG["feed_token"]

def get_httpx_client() -> httpx.Client:
    """Replacement for utils.httpx_client.get_httpx_client"""
    global _HTTP_CLIENT
    if _HTTP_CLIENT is None:
        # Create a standard client with reasonable defaults
        _HTTP_CLIENT = httpx.Client(timeout=10.0)
    return _HTTP_CLIENT

def get_logger(name: str) -> logging.Logger:
    """Replacement for utils.logging.get_logger"""
    return logging.getLogger(f"india_stocks_api.internal.{name}")

# --- Symbol Mapping Shims (Replacements for database.token_db) ---

_INSTRUMENT_DB = None

def _get_db():
    """
    Return a cached InstrumentDB instance, initializing it on first use by locating an `instruments.db` file.
    
    The function lazily constructs and caches an InstrumentDB; on first call it attempts to locate a local `instruments.db` file (checking the current working directory and a path relative to the package) and initializes InstrumentDB with the resolved path. Subsequent calls return the same cached instance.
    
    Returns:
        InstrumentDB: The initialized and cached InstrumentDB instance.
    """
    global _INSTRUMENT_DB
    if _INSTRUMENT_DB is None:
        # Assuming DB is in src/instruments.db relative to this file
        # This path logic might need adjustment based on installation
        # For now, assumes running from src or tests where CWD is root or src
        # Or better: use absolute path relative to package
        chk_path = Path("instruments.db") # CWD (e.g., src/)
        if not chk_path.exists():
             # Try side-by-side with package?
             chk_path = Path(__file__).parent.parent.parent / "instruments.db"

        _INSTRUMENT_DB = InstrumentDB(str(chk_path))
    return _INSTRUMENT_DB

def get_br_symbol(symbol: str, exchange: str) -> str:
    """
    Replacement for database.token_db.get_br_symbol.
    """
    db = _get_db()
    # lookup_token requires more args for options, but for equity 'symbol' + 'exchange' is key
    # However, 'symbol' argument here might comprise multiple parts for options?
    # OpenAlgo uses 'symbol' as the unique key.
    # In my DB, I have 'symbol' (underlying) and 'tradingsymbol' (unique).
    # If the input 'symbol' is "NIFTY24DECFUT", that maps to 'tradingsymbol' in my DB?
    # Actually, OpenAlgo's 'symbol' column IS the standardized string.
    
    # Simple query: SELECT tradingsymbol FROM instruments WHERE symbol (OA col) = symbol
    # But wait, my DB 'symbol' column is 'NIFTY'. My 'tradingsymbol' is 'NIFTY...'.
    # I need to match the input `symbol` to SOMETHING in DB.
    # If standard OA symbol is used as input (e.g. "RELIANCE"), match `symbol`
    
    # For now, let's treat the input `symbol` as the 'symbol' column for Equities
    # casting wide net.
    
    record = db.lookup_token(symbol, exchange)
    if record:
        return record.tradingsymbol
    return symbol # Fallback

def get_token(symbol: str, exchange: str) -> Optional[str]:
    """Replacement for database.token_db.get_token"""
    db = _get_db()
    record = db.lookup_token(symbol, exchange)
    if record:
        return record.token
    return None

def get_tradingsymbol(symbol: str, exchange: str) -> Optional[str]:
    """Get broker-specific tradingsymbol (e.g. SBIN-EQ)"""
    db = _get_db()
    record = db.lookup_token(symbol, exchange)
    if record:
        return record.tradingsymbol
    return symbol

def get_symbol(token: str, exchange: str) -> Optional[str]:
    """Replacement for database.token_db.get_symbol"""
    return token # TODO implementation

def get_oa_symbol(brsymbol: str, exchange: str) -> Optional[str]:
    """Replacement for database.token_db.get_oa_symbol"""
    return brsymbol