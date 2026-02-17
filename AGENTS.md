# Repository Guidelines

## Project Structure & Module Organization
Core library code lives in `india_stocks_api/`.
- `brokers/`: public broker adapters (for example, `angel.py`).
- `instruments/`: instrument models, database access, and symbol resolution.
- `internal/`: broker-specific internals (API clients, mappings, streaming, context).
- `constants.py` and `exceptions.py`: shared enums and error types.

Tests are under `tests/`:
- `tests/unit/` for fast isolated tests.
- `tests/integration/` for credentialed/live flows.
- `tests/scripts/` for manual or exploratory broker checks.

Reference docs are in `docs/`; CI is in `.github/workflows/ci.yml`.

## Build, Test, and Development Commands
- `poetry install` installs runtime and dev dependencies.
- `poetry run pytest tests/unit/ -v` runs the unit suite (matches CI).
- `poetry run pytest -m "not integration"` runs all non-integration tests.
- `poetry run pytest tests/integration/ -v` runs live integration tests (requires broker credentials).
- `poetry run ruff check .` runs lint checks.
- `poetry run ruff format .` formats code.
- `poetry run mypy india_stocks_api/` runs static type checks.
- `poetry run pre-commit run --all-files` runs the same hooks used in CI.

## Coding Style & Naming Conventions
Target Python is 3.10+ with 4-space indentation and max line length 120 (`ruff`).
Use `snake_case` for functions/modules, `PascalCase` for classes, and `UPPER_SNAKE_CASE` for constants.
Prefer explicit typing on public interfaces and keep broker-facing enums/constants centralized in `constants.py`.

## Testing Guidelines
Use `pytest` with tests named `test_*.py` and functions named `test_*`.
Keep unit tests deterministic and isolated; reserve network/live broker behavior for `integration`-marked tests.
When adding features, include unit coverage first, then integration tests if API behavior changes.

## Commit & Pull Request Guidelines
Follow the observed conventional style: `type(scope): summary` (examples: `fix(auth): ...`, `refactor(instruments): ...`, `ci: ...`).
Keep commit messages imperative and concise.
For PRs, include:
- clear problem/solution summary,
- linked issue (if applicable),
- test evidence (commands run and results),
- notes on env vars or credentials needed for verification.
