# Rust + IBKR Scaffold

## Why Rust

Rust is the recommended production language for this project because it gives us:

- Predictable performance for high-volume analysis
- Safe concurrency for streaming market data and order workflows
- Strong typing around risk-sensitive code paths
- A growing native ecosystem for Interactive Brokers integration

## IBKR Compatibility

The scaffold is built around the `ibapi` crate, which is a Rust implementation of the Interactive Brokers TWS and IB Gateway API. Its current release line supports:

- TWS and IB Gateway connectivity
- Async and blocking clients
- Real-time and historical market data
- Account and position queries
- Order submission and update streams

The local scaffold uses the async client so it can scale toward concurrent symbol analysis and order monitoring.

## Current Modules

- `config`: loads `.env` settings for host, port, client ID, account, runtime mode, and symbol list
- `ibkr`: contains the broker connection descriptor and a startup probe that calls IBKR server time
- `scanner`: holds the first scan-plan abstraction for the target symbol universe
- `scoring`: ports the current ranking idea into deterministic Rust functions with unit tests
- `models`: shared domain types for snapshots, score inputs, and ranked candidates

## What Is Still Placeholder

The scaffold does not yet:

- Request real option chains from IBKR
- Map IBKR contract details into internal candidate records
- Stream market data for thousands of options concurrently
- Place or manage orders
- Persist state, risk limits, or execution history

Those are the next implementation phases.
