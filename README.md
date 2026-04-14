# Options Trading

This repository is for a real-time options analysis and execution system that will integrate with the Interactive Brokers API.

The current implementation lives in the `Python Code` folder and performs batch-style analysis using:

- A configurable ticker universe loaded from CSV
- External market-data lookups that should now be replaced by IBKR
- User-defined thresholds for price, expiration window, and contract type
- Ranking metrics used to produce a sorted candidate list

## Current Python Baseline

The existing script reads settings from `options_analysis_settings.txt`, filters the input universe, requests live market data from Finnhub, scores nearby call/put contracts, and exports a sorted CSV of trade candidates.

Key limitations in the current baseline:

- Sequential requests will not scale well to thousands of symbols in real time
- Market data and execution are split across systems instead of using IBKR end to end
- API credentials are hard-coded in the script and should be moved into environment-based configuration
- The project is structured as a single script rather than a testable, modular system

## Proposed Near-Term Direction

We should use the Python script as the functional reference while we design a faster runtime for the production scanner and execution engine.

Strong candidates:

- Rust: best fit for low-latency concurrent analysis, memory safety, and long-running services
- C#: strong option if Windows tooling and Interactive Brokers ecosystem support become a priority
- Go: simpler concurrency model, but less ideal if we need tight numeric optimization and strict latency control

Recommended first target: Rust for the analysis/execution service, while keeping the Python script only as a temporary validation harness during migration.

## Rust Scaffold

The new Rust workspace is centered on `crates/ibkr-options-engine`.

Its initial shape is:

- `config`: environment-driven runtime configuration
- `ibkr`: broker connectivity and market-data adapter boundary
- `scanner`: orchestration for symbol analysis runs
- `scoring`: deterministic option scoring logic
- `models`: shared domain types for quotes, contracts, and ranked candidates

The scaffold assumes IBKR is the source for both market data and order execution. Finnhub is not part of the target architecture.

## Repository Layout

- `Python Code/`: existing baseline implementation and inputs
- `docs/`: project notes, review findings, and setup instructions
- `crates/ibkr-options-engine/`: Rust analysis and execution engine
- `.env.example`: local configuration template

## Next Steps

1. Replace the placeholder scanner with real IBKR contract discovery and option-chain requests
2. Port the Python scoring math into tested Rust modules
3. Add paper-trading order submission and execution monitoring
4. Add risk controls, rate limiting, and replayable integration tests
