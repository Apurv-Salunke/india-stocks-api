# Project Overview: OpenAlgo Brokers Package

## Objective

To extract the `broker` directory from the existing OpenAlgo repository and package it as a standalone Python library (`openalgo-brokers`).

## Goals

1.  **Reuse Tested Code**: Leverage the existing, battle-tested connection logic for 24+ brokers.
2.  **Active Maintenance**: Ensure the layout allows for easy syncing with the active OpenAlgo codebase.
3.  **Standalone**: The new package must not depend on the OpenAlgo Flask server, UI, or database models.
4.  **Clean Interface**: Provide a unified, type-safe interface for end-users (Python scripts/strategies) without exposing internal complexity.

## Scope

- **Input**: The centralized `broker/` directory containing adapters for Zerodha, Angel One, Fyers, etc.
- **Output**: A PyPI-installable package.
- **Constraints**: Minimize changes to the `broker/` source files to maintain upgradeability. Use "Shims" to handle dependencies.
