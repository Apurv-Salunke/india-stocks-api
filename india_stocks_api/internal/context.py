"""
Shim Context for Internal OpenAlgo Code.
This module mocks/replaces the `database` and `utils` dependencies that
the ported OpenAlgo broker code expects.
"""
import httpx
import logging
from typing import Optional, Dict, Any

# --- Shared State (Global for simplicity in Shim) ---
_CONFIG = {
    "api_key": None,
    "access_token": None,
    "feed_token": None,
    "broker_creds": {}, # Generic storage for broker-specific credentials
    # In a real implementation, this would be a proper mapped object or DB
    "symbol_map": {} 
}

_HTTP_CLIENT: Optional[httpx.Client] = None

# --- Configuration Setters (Called by Broker Adapter) ---

def set_api_key(key: str):
    _CONFIG["api_key"] = key

def set_auth_token(token: str):
    _CONFIG["access_token"] = token

def set_feed_token(token: str):
    _CONFIG["feed_token"] = token

def set_broker_creds(creds: Dict[str, Any]):
    _CONFIG["broker_creds"] = creds

def get_broker_creds() -> Dict[str, Any]:
    return _CONFIG["broker_creds"]

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

# Global DB instance for the Shim
from india_stocks_api.instruments.database import InstrumentDB
import os

_INSTRUMENT_DB = None

def _get_db():
    global _INSTRUMENT_DB
    if _INSTRUMENT_DB is None:
        # Assuming DB is in src/instruments.db relative to this file
        # This path logic might need adjustment based on installation
        # For now, assumes running from src or tests where CWD is root or src
        # Or better: use absolute path relative to package
        from pathlib import Path
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
