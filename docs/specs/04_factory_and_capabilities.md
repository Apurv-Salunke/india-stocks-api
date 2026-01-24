# Factory & Capabilities: The Dynamic Proxy

We solve the "User Experience" challenge of varying broker capabilities using a Class Composition pattern, ensuring users only see methods available for their specific broker.

## Requirement

- **Zerodha**: Supports GTT, Options Chain.
- **BasicBroker**: Supports only regular orders.
- **Constraint**: `BasicBroker` client instances must **not** have `place_gtt` or `get_option_chain` methods visible in the IDE or callable at runtime.

## The Solution

### 1. Mixin Classes

We define atomic capability buckets.

```python
class SupportedGTT:
    def place_gtt_order(self, ...): ...

class SupportedOptionsChain:
    def get_option_chain(self, ...): ...
```

### 2. Adapter Composition

Specific broker adapters inherit _only_ what they support.

```python
class ZerodhaAdapter(BaseBroker, SupportedGTT, SupportedOptionsChain):
    pass

class BasicAdapter(BaseBroker):
    pass
```

### 3. The Factory (`__init__.py`)

We use Python's `overload` decorators to tell the IDE exactly what `create()` returns based on the input string.

```python
from typing import overload, Literal

class Broker:
    @overload
    @staticmethod
    def create(broker: Literal["zerodha"], **kwargs) -> ZerodhaAdapter: ...

    @overload
    @staticmethod
    def create(broker: Literal["angel"], **kwargs) -> AngelAdapter: ...

    @staticmethod
    def create(broker: str, **kwargs):
        if broker == "zerodha":
            return ZerodhaAdapter(**kwargs)
        elif broker == "angel":
            return AngelAdapter(**kwargs)
        # ...
```

## User Experience Result

```python
# Case 1: Advanced Broker
client = Broker.create("zerodha", ...)
client.place_gtt(...)       # ✅ Valid, Auto-completes

# Case 2: Basic Broker
client = Broker.create("others", ...)
client.place_gtt(...)       # ❌ IDE Error: "Method not found"
                            # ❌ Runtime Error: AttributeError
```

This ensures a clean, non-polluted interface where "what you see is what you get".
