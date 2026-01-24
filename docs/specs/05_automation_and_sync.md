# Automation & Sync: Keeping Brokers Up-to-Date

## Overview

This document describes the automated system for keeping `india-stocks-api` in sync with the upstream `openalgo` repository without manual copy-pasting.

## The Problem

We have ported code from `openalgo/broker/angel/` to `india_stocks_api/internal/angel/`. The key transformation is **import rewriting**:

| OpenAlgo Import                                          | India Stocks API Import                                                  |
| -------------------------------------------------------- | ------------------------------------------------------------------------ |
| `from database.auth_db import get_auth_token`            | `from india_stocks_api.internal.context import get_auth_token`           |
| `from database.token_db import get_token, get_br_symbol` | `from india_stocks_api.internal.context import get_token, get_br_symbol` |
| `from utils.httpx_client import get_httpx_client`        | `from india_stocks_api.internal.context import get_httpx_client`         |
| `from utils.logging import get_logger`                   | `from india_stocks_api.internal.context import get_logger`               |
| `from broker.angel.mapping.xxx`                          | `from india_stocks_api.internal.angel.mapping.xxx`                       |

Some files (like `SmartWebSocketV2.py`) require **no shimming** because they only use standard library imports.

---

## Proposed Architecture

```
india-stocks-api/
├── scripts/
│   ├── sync_upstream.py        # Main extraction script
│   ├── shim_rules.yaml         # Import rewrite rules
│   └── broker_manifest.yaml    # Which files to sync per broker
├── .github/
│   └── workflows/
│       └── sync-upstream.yml   # GitHub Action
└── SYNC_VERSION                # Tracks last synced OpenAlgo commit/tag
```

---

## Component 1: `broker_manifest.yaml`

Defines **what to sync** per broker:

```yaml
brokers:
  angel:
    source: broker/angel
    dest: india_stocks_api/internal/angel
    files:
      # Files requiring shimming
      shimmed:
        - api/order_api.py
        - api/auth_api.py
        - api/data.py
        - api/funds.py
        - api/margin_api.py
        - mapping/transform_data.py
        - mapping/order_data.py
      # Files copied as-is (no imports to rewrite)
      passthrough:
        - streaming/smartWebSocketV2.py
      # Files we DON'T sync (we have custom implementations)
      ignore:
        - database/master_contract_db.py # We use our own builder.py
        - __init__.py # We maintain our own
```

---

## Component 2: `shim_rules.yaml`

Defines **import rewrite rules**:

```yaml
rules:
  # Database imports → context
  - pattern: "from database.auth_db import"
    replace: "from india_stocks_api.internal.context import"
  - pattern: "from database.token_db import"
    replace: "from india_stocks_api.internal.context import"

  # Utils imports → context
  - pattern: "from utils.httpx_client import"
    replace: "from india_stocks_api.internal.context import"
  - pattern: "from utils.logging import"
    replace: "from india_stocks_api.internal.context import"

  # Internal broker imports → absolute paths
  - pattern: "from broker.angel."
    replace: "from india_stocks_api.internal.angel."
  - pattern: "from broker.zerodha."
    replace: "from india_stocks_api.internal.zerodha."
```

---

## Component 3: `sync_upstream.py`

Python script that:

1. **Clones or fetches** OpenAlgo repo to a temp directory
2. **Reads** `broker_manifest.yaml` to know what to sync
3. **For each file**:
   - If in `passthrough`: Copy as-is
   - If in `shimmed`: Apply `shim_rules.yaml` regex replacements, then copy
   - If in `ignore`: Skip
4. **Detects changes** via `git diff`
5. **Outputs** a summary of what changed

```python
# Pseudo-code
def sync_broker(broker_name):
    manifest = load_manifest()
    rules = load_shim_rules()

    for file in manifest[broker_name]['shimmed']:
        content = read_upstream(file)
        shimmed = apply_rules(content, rules)
        write_to_internal(file, shimmed)

    for file in manifest[broker_name]['passthrough']:
        copy_as_is(file)
```

---

## Component 4: GitHub Action

```yaml
name: Sync Upstream Brokers
on:
  schedule:
    - cron: "0 8 * * 1" # Weekly on Monday 8 AM UTC
  workflow_dispatch: # Manual trigger

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Clone OpenAlgo
        run: |
          git clone --depth 1 https://github.com/marketcalls/openalgo.git /tmp/openalgo
          echo "OPENALGO_SHA=$(git -C /tmp/openalgo rev-parse HEAD)" >> $GITHUB_ENV

      - name: Run Sync Script
        run: |
          python scripts/sync_upstream.py \
            --source /tmp/openalgo \
            --dest india_stocks_api/internal \
            --manifest scripts/broker_manifest.yaml \
            --rules scripts/shim_rules.yaml

      - name: Check for Changes
        id: changes
        run: |
          if git diff --quiet; then
            echo "changed=false" >> $GITHUB_OUTPUT
          else
            echo "changed=true" >> $GITHUB_OUTPUT
          fi

      - name: Create Pull Request
        if: steps.changes.outputs.changed == 'true'
        uses: peter-evans/create-pull-request@v5
        with:
          title: "chore: Sync brokers from OpenAlgo (${{ env.OPENALGO_SHA }})"
          body: |
            Automated sync of broker files from upstream OpenAlgo.

            **Upstream Commit:** ${{ env.OPENALGO_SHA }}

            Please review:
            - [ ] Import rewrites look correct
            - [ ] Tests pass
            - [ ] No breaking changes
          branch: sync/openalgo-${{ env.OPENALGO_SHA }}
          labels: dependencies, automated
```

---

## Component 5: Version Tracking (`SYNC_VERSION`)

A simple file that records the last synced commit:

```
# Last synced OpenAlgo commit
abc123def456...
```

The sync script checks this before running to avoid redundant syncs.

---

## Workflow Summary

```
┌─────────────────────────────────────────────────────────────┐
│                    GitHub Action (Weekly)                    │
├─────────────────────────────────────────────────────────────┤
│  1. Clone OpenAlgo to /tmp                                  │
│  2. Compare HEAD with SYNC_VERSION                          │
│  3. If same → exit (no changes)                             │
│  4. Run sync_upstream.py                                    │
│      - Read broker_manifest.yaml                            │
│      - Apply shim_rules.yaml                                │
│      - Copy/transform files                                 │
│  5. If git diff shows changes → Create PR                   │
│  6. Human reviews PR, runs tests, merges                    │
│  7. SYNC_VERSION updated on merge                           │
└─────────────────────────────────────────────────────────────┘
```

---

## Edge Cases Handled

| Scenario                | Handling                                            |
| ----------------------- | --------------------------------------------------- |
| New file added upstream | Ignored unless added to manifest                    |
| File deleted upstream   | Manual cleanup (manifest outdated)                  |
| New broker added        | Requires manual manifest entry + initial shim setup |
| Shim rule doesn't match | File copied with original imports (will fail tests) |
| Upstream restructures   | Script fails → alert in Actions → manual fix        |

---

## Benefits

1. **Low Noise**: Only syncs files we care about
2. **Safe**: Human review required before merge
3. **Traceable**: PR shows exact diff + upstream commit
4. **Flexible**: Easy to add new brokers or change rules
5. **Testable**: Run `sync_upstream.py` locally before committing

---

## Implementation Checklist

- [ ] Create `scripts/broker_manifest.yaml`
- [ ] Create `scripts/shim_rules.yaml`
- [ ] Implement `scripts/sync_upstream.py`
- [ ] Create `.github/workflows/sync-upstream.yml`
- [ ] Create `SYNC_VERSION` file
- [ ] Test locally before enabling GitHub Action
- [ ] Document manual override process
