# Project Review

## What The Existing Python Script Does

The baseline script in `Python Code/250604_FinnHub_Dual_Key_Analysis_from_Input_Files.py` is a settings-driven options scanner.

Its workflow is:

1. Load analysis thresholds and file names from `options_analysis_settings.txt`
2. Read a CSV universe of stocks and normalize ticker, price, and beta values
3. Filter equities by underlying share price and expiration window
4. Fetch current quotes and option chains from Finnhub
5. Score nearby option contracts using custom yield and buffer metrics
6. Export a sorted CSV of candidates for later review

## Key Inputs

- `Russell3000_tickers_V2.csv`: stock universe
- `options_analysis_settings.txt`: analysis thresholds and contract-type selection

## Important Derived Metrics

The script calculates several custom fields, including:

- `MY`: return-like metric based on margin/income relationship
- `PB`: distance from current price to strike
- `LB`: buffer between underlying price and derived entry condition
- `MY*PB/SQRT(beta) w/ Conditions`: ranking score used to sort candidates
- `Annualized Yield (%)`: normalized yield estimate by days to expiration

## Observed Issues And Risks

- Finnhub API tokens are hard-coded directly in source and should be removed from versioned code
- Contract-type parsing is duplicated in the script
- Network calls are fully sequential, which will become a major bottleneck at scale
- Error handling is broad and mostly suppresses root causes
- The script is analysis-only and does not yet include position management, order routing, or Interactive Brokers integration
- There are no tests, no package structure, and no environment bootstrap yet

## What This Means For The Rewrite

The existing Python version is best treated as a prototype and scoring reference, not the long-term production architecture.

For the production system, we should design around:

- Concurrent market-data retrieval
- Rate-limit-aware batching and caching
- Deterministic scoring modules
- Broker integration isolated behind a clear execution interface
- Safe paper-trading validation before live execution

## Architecture Decision

The rewrite should use Interactive Brokers as the system of record for both market data and execution. That removes Finnhub from the production path and keeps symbol lookup, options discovery, pricing, and order routing under one API boundary.
