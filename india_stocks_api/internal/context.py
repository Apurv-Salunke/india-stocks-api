"""
Shim Context for Internal OpenAlgo Code.
This module mocks/replaces the `database` and `utils` dependencies that
the ported OpenAlgo broker code expects.
"""
import httpx
import logging
import json
from pathlib import Path
from typing import Optional, Dict, Any

_logger = logging.getLogger(__name__)

# --- Shared State (Global for simplicity in Shim) ---
_CONFIG = {
    "access_token": None,
    "feed_token": None,
    "symbol_map": {}
}

_HTTP_CLIENT: Optional[httpx.Client] = None

# --- Session Persistence (_cache/sessions.json) ---
#
# Stores ONLY data needed for API calls: access_token, feed_token,
# api_key, client_code, authenticated_at, expires_at.
# Sensitive secrets (password, totp_key) are NEVER written to disk.

_SESSION_CACHE: Dict[str, Dict[str, Any]] = {}
_SESSION_LOADED: bool = False
_SESSION_FILE = Path(__file__).parent.parent.parent / "_cache" / "sessions.json"


def _get_session_path() -> Path:
    """Return path to sessions.json, creating parent dir lazily."""
    path = _SESSION_FILE
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        _logger.warning(f"Failed to create session cache dir {path.parent}: {e}")
    return path


def _load_all_sessions() -> Dict[str, Dict[str, Any]]:
    """Load all sessions from disk into memory (once per process)."""
    global _SESSION_LOADED, _SESSION_CACHE
    if _SESSION_LOADED:
        return _SESSION_CACHE

    path = _get_session_path()
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _SESSION_CACHE = {
                    str(broker): (sess if isinstance(sess, dict) else {})
                    for broker, sess in data.items()
                }
            else:
                _SESSION_CACHE = {}
        except Exception:
            _SESSION_CACHE = {}
    else:
        _SESSION_CACHE = {}

    _SESSION_LOADED = True
    return _SESSION_CACHE


def _flush_sessions() -> None:
    """Persist in-memory session cache to sessions.json (chmod 600)."""
    path = _get_session_path()
    try:
        path.write_text(json.dumps(_SESSION_CACHE, indent=2), encoding="utf-8")
        try:
            path.chmod(0o600)
        except OSError as e:
            _logger.warning(f"Failed to set perms on {path}: {e}")
    except Exception as e:
        _logger.warning(f"Failed to save sessions to {path}: {e}")


def save_session(broker: str, session: Dict[str, Any]) -> None:
    """
    Persist a broker session to _cache/sessions.json.

    Called AFTER successful authentication. The session dict should
    contain only: access_token, feed_token, api_key, client_code,
    authenticated_at, expires_at. Never store passwords or TOTP keys.
    """
    global _SESSION_CACHE
    all_sessions = _load_all_sessions()
    all_sessions[broker] = dict(session)
    _SESSION_CACHE = all_sessions
    _flush_sessions()


def load_session(broker: str) -> Dict[str, Any]:
    """
    Load a broker session from memory / sessions.json.

    Returns an empty dict if no session exists.
    """
    all_sessions = _load_all_sessions()
    session = all_sessions.get(broker) or {}
    return dict(session)


def clear_session(broker: str) -> None:
    """Remove a broker's session from sessions.json."""
    global _SESSION_CACHE
    all_sessions = _load_all_sessions()
    if broker in all_sessions:
        del all_sessions[broker]
        _SESSION_CACHE = all_sessions
        _flush_sessions()


def get_api_key(broker: str) -> Optional[str]:
    """Get api_key for a broker from the session cache."""
    session = load_session(broker)
    return session.get("api_key")


# --- Configuration Setters (Called by Broker Adapter) ---

def set_auth_token(token: str):
    _CONFIG["access_token"] = token

def set_feed_token(token: str):
    _CONFIG["feed_token"] = token


# --- Shimmed Functions (Replacements for OpenAlgo imports) ---

def get_auth_token(user_id: Optional[str] = None) -> Optional[str]:
    """Replacement for database.auth_db.get_auth_token"""
    return _CONFIG["access_token"]

def get_feed_token(user_id: Optional[str] = None) -> Optional[str]:
    """Replacement for database.auth_db.get_feed_token"""
    return _CONFIG["feed_token"]

def get_httpx_client() -> httpx.Client:
    """Replacement for utils.httpx_client.get_httpx_client"""
    global _HTTP_CLIENT
    if _HTTP_CLIENT is None:
        _HTTP_CLIENT = httpx.Client(timeout=10.0)
    return _HTTP_CLIENT

def get_logger(name: str) -> logging.Logger:
    """Replacement for utils.logging.get_logger"""
    return logging.getLogger(f"india_stocks_api.internal.{name}")


# --- Symbol Mapping Shims (Replacements for database.token_db) ---

from india_stocks_api.instruments.database import InstrumentDB

_INSTRUMENT_DB = None

def _get_db():
    global _INSTRUMENT_DB
    if _INSTRUMENT_DB is None:
        chk_path = Path("instruments.db")
        if not chk_path.exists():
            chk_path = Path(__file__).parent.parent.parent / "instruments.db"
        _INSTRUMENT_DB = InstrumentDB(str(chk_path))
    return _INSTRUMENT_DB

def get_br_symbol(symbol: str, exchange: str) -> str:
    """Replacement for database.token_db.get_br_symbol."""
    db = _get_db()
    record = db.lookup_token(symbol, exchange)
    if record:
        return record.tradingsymbol
    return symbol

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
    return token  # TODO implementation

def get_oa_symbol(brsymbol: str, exchange: str) -> Optional[str]:
    """Replacement for database.token_db.get_oa_symbol"""
    return brsymbol
