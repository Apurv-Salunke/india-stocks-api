# Session & Authentication

This document explains token management, session persistence, and authentication flows.

---

## Overview

The SDK manages:

- JWT access tokens for API calls
- Feed tokens for WebSocket streaming
- Session persistence across process restarts
- Automatic session expiry detection

---

## Authentication Flow

### Sequence Diagram

```text
User                     Broker Adapter              Context             Broker API
  │                           │                         │                    │
  │ authenticate()            │                         │                    │
  │ ─────────────────────────>│                         │                    │
  │                           │                         │                    │
  │                           │ Generate TOTP           │                    │
  │                           │────────┐                │                    │
  │                           │        │                │                    │
  │                           │<───────┘                │                    │
  │                           │                         │                    │
  │                           │ HTTP POST /login        │                    │
  │                           │ ───────────────────────────────────────────>│
  │                           │                         │                    │
  │                           │ {access_token, feed_token}                  │
  │                           │ <───────────────────────────────────────────│
  │                           │                         │                    │
  │                           │ set_auth_token()        │                    │
  │                           │ ───────────────────────>│                    │
  │                           │                         │                    │
  │                           │ save_session()          │                    │
  │                           │ ───────────────────────>│                    │
  │                           │                         │ Write sessions.json│
  │                           │                         │ ───────┐           │
  │                           │                         │        │           │
  │                           │                         │ <──────┘           │
  │                           │                         │                    │
  │ True                      │                         │                    │
  │ <─────────────────────────│                         │                    │
```

---

## Context Module

### In-Memory Token Storage

```python
# internal/context.py

_CONFIG = {
    "access_token": None,
    "feed_token": None,
    "symbol_map": {},
}

def get_auth_token() -> str | None:
    """Get current JWT token."""
    return _CONFIG.get("access_token")

def set_auth_token(token: str | None):
    """Set JWT token."""
    _CONFIG["access_token"] = token

def get_feed_token() -> str | None:
    """Get WebSocket feed token."""
    return _CONFIG.get("feed_token")

def set_feed_token(token: str | None):
    """Set WebSocket feed token."""
    _CONFIG["feed_token"] = token
```

---

## Session Persistence

### File Location

```text
_cache/
└── sessions.json    # Contains session data (chmod 600)
```

### File Format

```json
{
  "angel": {
    "access_token": "<ACCESS_TOKEN>",
    "feed_token": "<FEED_TOKEN>",
    "api_key": "<API_KEY>",
    "client_code": "<CLIENT_CODE>",
    "authenticated_at": "2024-01-15T09:30:00+05:30",
    "expires_at": "2024-01-16T00:00:00+05:30"
  }
}
```

### What Is Stored

| Field | Stored | Reason |
|-------|--------|--------|
| `access_token` | ✅ | Required for API calls |
| `feed_token` | ✅ | Required for WebSocket |
| `api_key` | ✅ | Needed for some API calls |
| `client_code` | ✅ | User identifier |
| `authenticated_at` | ✅ | Audit trail |
| `expires_at` | ✅ | Session validation |
| `password` | ❌ | Never stored |
| `totp_key` | ❌ | Never stored |

---

## Session Lifecycle

### Creation

```python
# In broker.authenticate()

# After successful login
context.set_auth_token(jwt_token)
context.set_feed_token(feed_token)

# Compute expiry (midnight IST)
now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
expires_at = (now_ist + timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
)

# Persist
context.save_session("angel", {
    "access_token": jwt_token,
    "feed_token": feed_token,
    "api_key": self.api_key,
    "client_code": self.client_code,
    "authenticated_at": now_ist.isoformat(),
    "expires_at": expires_at.isoformat(),
})
```

### Validation

```python
# In broker._require_auth()

def _require_auth(self) -> str:
    """Validate session and return JWT token."""
    token = context.get_auth_token()
    
    if not token:
        raise AuthenticationError("Not authenticated. Call authenticate() first.")
    
    session = context.load_session(self._broker_name)
    expires_at = session.get("expires_at")
    
    if expires_at:
        if datetime.fromisoformat(expires_at) <= datetime.now(_IST):
            # Clear expired tokens
            context.set_auth_token(None)
            context.set_feed_token(None)
            raise SessionExpiredError(
                "Session expired (past midnight IST). Call authenticate() to start a new session."
            )
    
    return token
```

### Expiry

Sessions expire at midnight IST. After expiry:

```python
try:
    quote = broker.get_quote(Equity("RELIANCE"))
except SessionExpiredError:
    broker.authenticate()  # Re-authenticate
    quote = broker.get_quote(Equity("RELIANCE"))
```

---

## Security Considerations

### File Permissions

Sessions file is created with restricted permissions:

```python
def _flush_sessions():
    path = _get_session_path()
    path.write_text(json.dumps(_SESSION_CACHE, indent=2))
    try:
        path.chmod(0o600)  # Owner read/write only
    except OSError:
        pass  # May fail on Windows
```

### Token Security

- Tokens are stored only in `_cache/sessions.json`
- Never logged or printed
- Not included in error messages
- Cleared from memory on expiry

### TOTP Handling

TOTP is generated fresh for each authentication:

```python
def authenticate(self):
    # TOTP is generated, used once, and discarded
    totp_code = pyotp.TOTP(self.totp_key).now()
    
    # Send to broker
    jwt_token, feed_token, error = authenticate_broker(
        api_key=self.api_key,
        clientcode=self.client_code,
        broker_pin=self.password,
        totp_code=totp_code
    )
    
    # totp_code goes out of scope, never stored
```

---

## Session Recovery

### On Process Restart

When the process restarts, the SDK can resume:

```python
# Context loads sessions from disk on first access
session = context.load_session("angel")

if session.get("access_token"):
    context.set_auth_token(session["access_token"])
    context.set_feed_token(session.get("feed_token"))
    
    # Check if still valid
    if not session_expired(session):
        # Can use existing session
        pass
    else:
        # Need to re-authenticate
        pass
```

### Session Reuse (Not Recommended)

While sessions can be reused, it's safer to authenticate fresh:

```python
# Not recommended - session may be invalid
broker = AngelOne(...)
session = context.load_session("angel")
if session:
    context.set_auth_token(session["access_token"])
    # May fail if broker invalidated session

# Recommended - always authenticate
broker = AngelOne(...)
broker.authenticate()
```

---

## Token Flow for API Calls

### Standard API Call

```python
def get_quote(self, instrument):
    # 1. Validate session
    jwt_token = self._require_auth()
    
    # 2. Resolve instrument
    token_info = self._resolve_instrument(instrument)
    
    # 3. Call internal API with token
    bd = BrokerData(jwt_token)
    payload = bd.get_quotes(
        symbol=token_info["symbol"],
        exchange=token_info["exchange"]
    )
    
    # 4. Return mapped response
    return self._map_quote_response(payload)
```

### WebSocket Connection

```python
def start_streaming(self):
    # Requires both tokens
    jwt_token = self._require_auth()
    feed_token = context.get_feed_token()
    
    if not feed_token:
        raise AuthenticationError("No feed token available.")
    
    # Create WebSocket with both tokens
    self._ws_client = SmartWebSocketV2(
        auth_token=jwt_token,
        api_key=self.api_key,
        client_code=self.client_code,
        feed_token=feed_token,
    )
```

---

## Multi-Broker Sessions

The session system supports multiple brokers:

```json
{
  "angel": {
    "access_token": "...",
    "expires_at": "..."
  },
  "zerodha": {
    "access_token": "...",
    "expires_at": "..."
  }
}
```

Each broker has independent session management.

---

## Debugging Sessions

### Check Session Status

```python
from india_stocks_api.internal import context

session = context.load_session("angel")
print(f"Authenticated at: {session.get('authenticated_at')}")
print(f"Expires at: {session.get('expires_at')}")
print(f"Has token: {bool(session.get('access_token'))}")
```

### Clear Session

```python
# Clear specific broker
context.save_session("angel", {})

# Or delete file
import os
os.remove("_cache/sessions.json")
```

---

## Error Scenarios

### Missing Authentication

```python
broker = AngelOne(...)
# Forgot to call authenticate()
broker.get_quote(Equity("RELIANCE"))
# Raises: AuthenticationError("Not authenticated. Call authenticate() first.")
```

### Expired Session

```python
# Session was valid yesterday
broker.get_quote(Equity("RELIANCE"))
# Raises: SessionExpiredError("Session expired (past midnight IST)...")
```

### Invalid Token

```python
# Broker invalidated token (password change, etc.)
broker.get_quote(Equity("RELIANCE"))
# Raises: AuthenticationError or BrokerError with auth error code
```

---

## Next Steps

- [Error Handling](error-handling.md) - Exception hierarchy
- [Architecture](architecture.md) - Overall design
