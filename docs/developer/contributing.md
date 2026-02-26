# Contributing Guide

This document covers coding standards, development workflow, and contribution guidelines.

---

## Development Setup

### Prerequisites

- Python 3.10+
- Poetry (package manager)
- Git

### Clone and Install

```bash
git clone https://github.com/yourorg/india-stocks-api.git
cd india-stocks-api
poetry install
```

### Install Pre-commit Hooks

```bash
poetry run pre-commit install
```

---

## Code Style

### Formatting Rules

- **Python version:** 3.10+
- **Indentation:** 4 spaces
- **Line length:** 120 characters max
- **Formatter:** Ruff

### Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Modules | `snake_case` | `order_api.py` |
| Classes | `PascalCase` | `AngelOne`, `QuoteResponse` |
| Functions | `snake_case` | `place_order()` |
| Constants | `UPPER_SNAKE_CASE` | `MAX_RETRIES` |
| Private | `_leading_underscore` | `_map_response()` |

### Type Annotations

Required on all public interfaces:

```python
# Good
def place_order(
    self,
    instrument: Instrument,
    qty: int,
    side: TransactionType,
    order_type: OrderType,
    price: float | None = None,
) -> OrderResponse:
    ...

# Avoid: No type hints
def place_order(self, instrument, qty, side, order_type, price=None):
    ...
```

### Docstrings

Use Google-style docstrings:

```python
def place_order(
    self,
    instrument: Instrument,
    qty: int,
    side: TransactionType,
) -> OrderResponse:
    """Place a new order.
    
    Args:
        instrument: The instrument to trade.
        qty: Quantity to order.
        side: BUY or SELL.
    
    Returns:
        Order details with broker order ID.
    
    Raises:
        AuthenticationError: If not authenticated.
        OrderRejectedError: If broker rejects order.
    """
```

---

## Tools

### Linting

```bash
# Check for issues
poetry run ruff check .

# Auto-fix issues
poetry run ruff check . --fix
```

### Formatting

```bash
poetry run ruff format .
```

### Type Checking

```bash
poetry run mypy india_stocks_api/
```

### All Checks (Pre-commit)

```bash
poetry run pre-commit run --all-files
```

---

## Git Workflow

### Branch Naming

```
feature/add-zerodha-support
fix/session-expiry-handling
refactor/instrument-resolution
docs/streaming-guide
```

### Commit Messages

Follow conventional commits: `type(scope): message`

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `refactor`: Code change (no new feature or fix)
- `docs`: Documentation only
- `test`: Adding/changing tests
- `ci`: CI/CD changes
- `chore`: Maintenance tasks

**Examples:**
```
feat(orders): add GTT order support
fix(auth): handle session expiry correctly
refactor(instruments): simplify resolution logic
docs(streaming): add WebSocket reconnection guide
test(auth): add session persistence tests
ci: add Python 3.12 to test matrix
```

### Pull Request Process

1. **Fork and branch**
   ```bash
   git checkout -b feature/your-feature
   ```

2. **Make changes**
   - Write code
   - Add tests
   - Update docs

3. **Run checks**
   ```bash
   poetry run pre-commit run --all-files
   poetry run pytest tests/unit/ -v
   ```

4. **Commit**
   ```bash
   git add .
   git commit -m "feat(scope): description"
   ```

5. **Push and PR**
   ```bash
   git push origin feature/your-feature
   ```

6. **PR description template**
   ```markdown
   ## Summary
   Brief description of changes.
   
   ## Related Issue
   Closes #123
   
   ## Changes
   - Added X
   - Modified Y
   - Removed Z
   
   ## Testing
   - [ ] Unit tests pass
   - [ ] Integration tests pass (if applicable)
   
   ## Notes
   Any special considerations for reviewers.
   ```

---

## Adding a New Broker

Adding a new broker follows a strict 4-step workflow. Each step is completed in a separate branch to keep changes small, reviewable, and reversible.

### Rules

- **One logical change per branch**
- **Never mix raw vendor code with refactoring**
- **No large, multi-purpose PRs**
- **Always branch from the latest dev (or base branch)**
- **Follow naming conventions strictly**

### Branch Order

```
broker/<broker-name>           # Step 1: Raw code
    ↓
feat/<broker-name>-remove-env  # Step 2: Cleanup
    ↓
broker/<broker-name>-adapter   # Step 3: Adapter class
    ↓
broker/<broker-name>-instruments  # Step 4: Instrument DB
```

---

### Step 1 — Add OpenAlgo's Broker Code (No Modifications)

**Branch name:** `broker/<broker-name>`

Copy the broker folder from OpenAlgo exactly as-is:

```
openalgo/broker/zerodha → india_stocks_api/internal/zerodha
```

**Actions:**
- Copy OpenAlgo broker code exactly as provided
- Do NOT modify anything
- Do NOT refactor
- Do NOT remove env variables
- Direct commit (no PR required)

**Purpose:** Keep an untouched baseline for traceability and future diffs.

**Final structure:**
```
india_stocks_api/internal/<broker-name>/
├── __init__.py
├── api/
│   ├── __init__.py
│   ├── auth_api.py
│   ├── data.py
│   ├── funds.py
│   ├── order_api.py
│   └── ...
├── database/
│   └── ...
├── mapping/
│   └── ...
└── streaming/
    └── ...
```

---

### Step 2 — Refactor & Remove Environment Dependencies

**Branch name:** `feat/<broker-name>-remove-env`

**Branch from:** `broker/<broker-name>`

**What to do:**

Modify imported code to:
- Remove `dotenv`/env usage
- Remove Flask/db dependencies
- Replace imports with `internal.context`
- Ensure standalone usage

**Actions:**
- Remove environment variable dependencies
- Fix imports to match project structure
- Standardize paths
- Make code repository-compliant
- No functional changes

**Example changes:**

```python
# Before (OpenAlgo style)
import os
from dotenv import load_dotenv
load_dotenv()
API_KEY = os.getenv("BROKER_API_KEY")

# After (india-stocks-api style)
from ...internal import context
# API_KEY passed via context or constructor
```

**Output:**
- Open PR for review
- Merge after approval

**Purpose:** Separate vendor code from project-specific adjustments.

---

### Step 3 — Create Broker Adapter

**Branch name:** `broker/<broker-name>-adapter`

**Branch from:** Latest merged branch

**What to do:**

Create `brokers/<broker-name>.py` implementing:

```python
from .base import BaseBroker

class NewBroker(BaseBroker, broker_name="newbroker"):
    def __init__(self, api_key: str, client_code: str, ...):
        ...
    
    def authenticate(self) -> bool:
        ...
    
    def place_order(self, ...) -> OrderResponse:
        ...
    
    def get_quote(self, instrument) -> QuoteResponse:
        ...
    
    # ... other required methods
```

**Actions:**
- Implement broker adapter class
- Follow `BaseBroker` interface/contracts
- Map broker-specific logic to unified entry points
- Integrate with resolver for instrument resolution
- Map broker enums to standard enums
- Apply architecture patterns (see [Broker Adapter Guide](broker-adapter-guide.md))

**Output:**
- Raise PR
- Peer review required
- Merge after approval

**Purpose:** Ensure the broker integrates cleanly without leaking vendor logic.

---

### Step 4 — Master Instrument DB Integration

**Branch name:** `broker/<broker-name>-instruments`

**Branch from:** Latest merged branch

**Actions:**
- Add master contract download function
- Implement `_download_master_contract()` in adapter
- Register broker instruments in database
- Update schemas or mappings if required
- Validate instrument resolution compatibility

**Implementation:**

```python
# india_stocks_api/internal/<broker>/database/master_contract.py
def master_contract_download():
    """Download and populate instruments.db"""
    ...

# In broker adapter
def _download_master_contract(self):
    from ..internal.<broker>.database import master_contract_download
    master_contract_download()
```

**Output:**
- Raise PR
- Merge after review

**Purpose:** Ensure instruments are properly recognized and managed.

---

### Final Checklist

After all 4 steps are merged:

- [ ] Add broker to exports in `brokers/__init__.py`
- [ ] Update [broker-support.md](../user/broker-support.md) documentation
- [ ] Add unit tests: `tests/unit/test_<broker>_*.py`
- [ ] Add integration tests: `tests/integration/test_<broker>_live.py`
- [ ] Update examples if needed

See [Broker Adapter Guide](broker-adapter-guide.md) for detailed implementation patterns.

---

## Adding Other Features

### New Response Types

1. Add to responses.py:
   ```python
   @dataclass(frozen=True, slots=True)
   class NewResponse:
       field1: str
       field2: float
   ```

2. Export in \_\_init\_\_.py:
   ```python
   from .responses import NewResponse
   ```

3. Add mapping in broker adapter
4. Write unit tests

### New Exceptions

1. Add to exceptions.py:
   ```python
   class NewError(ISAError):
       pass
   ```

2. Add error code if needed:
   ```python
   class ErrorCode(IntEnum):
       NEW_ERROR = 7001
   ```

3. Export and document

See [Error Handling](error-handling.md) for details.

---

## Testing Requirements

### All Changes

- Unit tests for new code
- No decrease in coverage
- All CI checks pass

### Breaking Changes

- Integration tests updated
- Migration guide in PR
- Version bump documented

### Quick Test Check

```bash
# Run unit tests
poetry run pytest tests/unit/ -v

# Run specific test file
poetry run pytest tests/unit/test_your_file.py -v

# Run with coverage
poetry run pytest tests/unit/ --cov=india_stocks_api
```

---

## Code Review Checklist

### For Authors

- [ ] Code follows style guide
- [ ] Type hints on public APIs
- [ ] Docstrings for public functions
- [ ] Unit tests added
- [ ] No breaking changes (or documented)
- [ ] Commits follow conventional style

### For Reviewers

- [ ] Logic is correct
- [ ] Error handling is complete
- [ ] Tests cover edge cases
- [ ] No sensitive data logged
- [ ] Documentation is clear

---

## Release Process

### Version Bumping

```bash
# Update version in pyproject.toml
poetry version patch  # 2.0.0 -> 2.0.1
poetry version minor  # 2.0.0 -> 2.1.0
poetry version major  # 2.0.0 -> 3.0.0
```

### Changelog

Update CHANGELOG.md with:
- New features
- Bug fixes
- Breaking changes
- Migration notes

---

## Getting Help

### Questions

- Open a GitHub Discussion
- Tag maintainers in PR comments

### Issues

- Use issue templates
- Include reproduction steps
- Attach relevant logs

---

## License

Contributions are licensed under the project's MIT license.

---

## Next Steps

- [Testing Strategy](testing.md) - Writing and running tests
- [Architecture](architecture.md) - System design overview
