# Broker Support

This guide documents broker-specific capabilities, limitations, and differences.

---

## Supported Brokers

| Broker | Status | Trading | Data | Streaming | GTT |
|--------|--------|---------|------|-----------|-----|
| Angel One | ✅ Complete | ✅ | ✅ | ✅ | ✅ |
| Zerodha | 🚧 In Progress | - | - | - | - |

---

## Angel One

### Credentials Required

| Credential | Description |
|------------|-------------|
| `api_key` | API key from Angel One SmartAPI |
| `client_code` | Your Angel One client ID |
| `password` | 4-digit MPIN |
| `totp_key` | TOTP secret (base32 encoded) |

### Supported Features

| Feature | Status | Notes |
|---------|--------|-------|
| Market orders | ✅ | All segments |
| Limit orders | ✅ | All segments |
| Stop-loss orders | ✅ | SL and SL-M |
| Bracket orders | ❌ | Not supported |
| Cover orders | ❌ | Not supported |
| GTT orders | ✅ | Up to 365 days |
| Real-time quotes | ✅ | All segments |
| Historical data | ✅ | Varies by interval |
| Market depth | ✅ | Level 2 data |
| WebSocket streaming | ✅ | LTP, Quote, Depth modes |
| Positions | ✅ | Day and net |
| Holdings | ✅ | Demat holdings |
| Funds | ✅ | Available margin |
| Profile | ✅ | Account info |

### Supported Exchanges

| Exchange | Code | Description |
|----------|------|-------------|
| NSE | NSE | Equity cash |
| BSE | BSE | Equity cash |
| NFO | NFO | NSE F&O |
| BFO | BFO | BSE F&O |
| MCX | MCX | Commodities |
| CDS | CDS | Currency derivatives |

---

### Session Behavior

- Sessions expire at midnight IST
- TOTP rate limit: ~2 per 30 seconds
- Feed token required for streaming

### Known Limitations

1. **No AMO orders**: After-market orders not supported via API
2. **No basket orders**: Single orders only
3. **GTT instrument required**: Cancel/modify needs instrument object
4. **Depth mode**: Full depth only for NSE CM

---

## Zerodha (Coming Soon)

### Expected Credentials

| Credential | Description |
|------------|-------------|
| `api_key` | Kite Connect API key |
| `api_secret` | Kite Connect secret |
| `request_token` | From OAuth redirect |

### Expected Features

| Feature | Expected Support |
|---------|-----------------|
| Market orders | ✅ |
| Limit orders | ✅ |
| Stop-loss orders | ✅ |
| GTT orders | ✅ |
| Streaming | ✅ |
| Historical data | ✅ |

---

## Cross-Broker Behavior

### Consistent Across Brokers

| Feature | Behavior |
|---------|----------|
| Domain objects | Same `Equity`, `Future`, `Option` classes |
| Enums | Same `OrderType`, `TransactionType`, etc. |
| Response models | Same `QuoteResponse`, `Position`, etc. |
| Error handling | Same exception hierarchy |
| Instrument resolution | Automatic symbol normalization |

### Broker-Specific Differences

| Aspect | May Differ |
|--------|------------|
| Session expiry | Depends on broker policy |
| Rate limits | Varies by broker |
| Available exchanges | Depends on broker support |
| Historical data range | Broker-specific limits |
| Streaming modes | Feature availability varies |

---

## Switching Brokers

The SDK is designed for broker-agnostic code:

```python
# Current: Angel One
from india_stocks_api.brokers import AngelOne
broker = AngelOne(api_key, client_code, password, totp_key)

# Future: Zerodha (example)
# from india_stocks_api.brokers import Zerodha
# broker = Zerodha(api_key, api_secret, request_token)

# Same operations work regardless of broker
broker.authenticate()
quote = broker.get_quote(Equity("RELIANCE"))
```

### Handling Broker Differences

```python
def get_depth_if_available(broker, instrument):
    """Get depth if supported, otherwise fall back to quote."""
    try:
        return broker.get_depth(instrument)
    except NotImplementedError:
        # Some brokers may not support depth
        return broker.get_quote(instrument)
```

---

## Feature Detection

Check broker capabilities at runtime:

```python
# Check broker name
broker_name = broker._broker_name  # "angel"

# Check available methods
has_gtt = hasattr(broker, 'create_gtt')
has_streaming = hasattr(broker, 'start_streaming')
```

---

## Credential Security

### Best Practices

| Do | Don't |
|----|-------|
| Store credentials in `.env` | Hardcode in source code |
| Use environment variables | Commit credentials to git |
| Restrict `.env` permissions | Share credentials |
| Rotate API keys periodically | Log credentials |

### Environment Setup

```bash
# .env file
ANGEL_API_KEY=xxx
ANGEL_CLIENT_ID=xxx
ANGEL_PIN=xxxx
ANGEL_TOTP_SECRET=xxx

# .gitignore
.env
_cache/
```

---

## Next Steps

- [Production Guide](production-guide.md) - Production deployment patterns
