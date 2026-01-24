# Automation & Sync: Keeping Brokers Up-to-Date

To ensure `openalgo-brokers` stays in sync with the upstream `openalgo` repository without manual copy-pasting, we will implement an automated "Watch and Pull" system.

## The Workflow

We will use **GitHub Actions** to automate this process. This creates a low-maintenance pipeline that respects the "Human Review" requirement.

### Trigger

The workflow runs on a **Schedule (e.g., Daily)** or a **Repository Dispatch** event.

### The Automation Steps

1.  **Check for Updates**: query the GitHub API to see if a new release (tag) has been published in `openalgo` that is newer than our `LAST_SYNCED_VERSION`.
2.  **Checkout**:
    - Clone the `openalgo-brokers` repository.
    - Clone the `openalgo` repository (at the new tag).
3.  **Run Extraction Script**:
    - Execute the `scripts/extract_brokers.py` script.
    - This script performs the "Smart Copy": copying files from `openalgo/broker/` to `openalgo_brokers/internal/` and applying the Shim (regex replacements) automatically.
4.  **Detect Changes**: Use `git status` to see if the extraction resulted in any modified files.
5.  **Create Pull Request**:
    - If changes are detected, the action creates a new branch (e.g., `sync/release-v2.5`).
    - It opens a Pull Request against `main` with the title: "chore: Sync brokers from OpenAlgo v2.5".
    - The body of the PR lists the files changed.

## Automation Script Logic (Concept)

```yaml
name: Sync Upstream Brokers
on:
  schedule:
    - cron: "0 8 * * *" # Runs daily at 8 AM

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Run Extraction
        run: |
          # 1. Fetch upstream
          git clone https://github.com/openalgo/openalgo.git temp_upstream

          # 2. Run Python Shim/Copy Script
          python scripts/extract_brokers.py --source temp_upstream/broker --dest src/openalgo_brokers/internal

      - name: Create Pull Request
        uses: peter-evans/create-pull-request@v5
        with:
          title: "Update Brokers from Upstream"
          body: "Automated sync of broker files. Please review the Shim application before merging."
          branch: "auto-sync/brokers"
```

## Practicality Assessment

**Is this practical?**
**Yes, highly.**

1.  **Low Noise**: The PR is only created if there are actual changes in the `broker` folder. If `openalgo` updates its UI or documentation, our system ignores it.
2.  **Safety Net**: Because we use an automated extraction script (the same one used for the initial setup), 90% of the "Shimming" work happens automatically.
3.  **Human Control**: The PR allows your developer to:
    - Verify that the Regex Shim didn't break anything.
    - Fix any imports that the script missed.
    - Run tests against the new code before merging.
4.  **Conflict Resolution**: If the upstream structure changes radically, the `extract_brokers.py` script might fail. This failure will be alerted in the Actions log, prompting a developer to investigate, rather than silently pushing broken code.
