# Options Trading

This repository is building a Rust-first IBKR options scanner and automation engine. The production path is now centered on `crates/ibkr-options-engine`, while `Python Code/` remains a parity/reference harness only.

The current vertical slice is designed around token-efficient Codex sessions:

- scheduled batch scans instead of an always-on daemon
- delayed or delayed-frozen snapshots before live data
- a small watchlist first, then controlled expansion
- guarded deep-ITM covered-call buy-write intent generation with combo-BAG paper routing behind explicit flags
- IB Gateway as the default broker runtime for unattended or semi-attended scans
- a default 50-symbol universe loaded from `docs/50_stocks_list.csv` with a `$1-$20` underlying filter

## Current Architecture

The Rust crate is organized around the runtime layers we want long term:

- `config`: env-driven runtime, market-data, schedule, and guardrail settings
- `market_data`: watchlist loading plus the replay-testable market-data boundary
- `ibkr`: narrow Interactive Brokers adapter for connectivity, snapshots, positions, and chains
- `strategy`: deep-ITM covered-call candidate evaluation only
- `state`: portfolio summaries and deep-ITM buy-write order-intent guardrails
- `paper_state`: persistent paper-order idempotency and hold-to-close lifecycle tracking
- `execution`: analysis-only plus guarded paper-only combo-BAG submission layer
- `scanner`: end-to-end cycle orchestration and cycle-report generation
- `scoring`: legacy/reference scoring math carried forward for parity work

The engine currently supports:

- loading a starter universe from CSV or `IBKR_SYMBOLS`
- connecting to IBKR and switching market-data modes
- fetching account state, positions, underlying snapshots, option chains, and option quotes
- ranking deep-ITM covered-call buy-write candidates using intrinsic-entry math adapted from the Python harness
- keeping conservative liquidity checks on bid and spread even when delayed/frozen data is thin
- generating guarded deep-ITM buy-write order intents with paper-order metadata
- emitting a structured cycle report as JSON

## Development Roadmap

### Phase A

- Preserve Python parity logic as a reference
- Keep the Rust scanner deterministic and testable
- Use small watchlists plus replay-style tests

### Phase B

- Run delayed-data scans on 25-100 symbols
- Tune request budgets to stay comfortably below IBKR line limits
- Improve diagnostics and cycle reporting

### Phase C

- Harden deep-ITM buy-write order construction
- Add paper-trading submission behind explicit flags
- Reconcile fills, positions, and duplicate-symbol prevention

### Phase D

- Expand open paper-position monitoring
- Improve hold-to-close lifecycle reporting
- Revisit exits only in a later dedicated milestone

### Later Phases

- selective live-data upgrade for paper trading
- larger universe management with measured batching changes
- live-money readiness only after paper stability
- optional historical-options backtesting on a separate data track

## Running The Current Scanner

The preferred workflow is TOML-backed CLI runs:

1. Copy `ibkr-options-engine.example.toml` to a local file such as `paper-trading.local.toml`
2. Edit `paper-trading.local.toml` with your IBKR account, expirations, watchlist, budget, and execution flags
3. Keep `platform = "gateway"` unless you intentionally want TWS
4. For paper Gateway, use `port = 4002` unless your Gateway settings show a different API port
5. Keep `tickers_file = "docs/50_stocks_list.csv"` unless you intentionally want a different CSV universe
6. Keep `min_underlying_price = 1.0` and `max_underlying_price = 20.0` for the current sub-$20 watchlist
7. Keep `read_only = true` and `enable_paper_orders = false` for early testing
8. Set `connect_on_start = true` only when IB Gateway or TWS paper is already running
9. In IB Gateway, enable `Configure > Settings > API > Settings > Enable ActiveX and Socket Clients`
10. If localhost is restricted, add `127.0.0.1` to trusted IPs
11. Run `cargo test -p ibkr-options-engine`
12. Run `cargo run -p ibkr-options-engine -- scan --config paper-trading.local.toml`
13. Run `cargo run -p ibkr-options-engine -- status --config paper-trading.local.toml`

Environment variables still work, and `.env.example` remains as a compatibility template, but the CLI `--config` flow is now the clearest path for repeatable paper runs.

The current screening defaults mirror the Python reference more closely for deep-ITM calls, with explicit expiration-date selection available through `EXPIRATION_DATES=20260515` or a comma-separated list such as `EXPIRATION_DATES=20260515,20250620`. Ranking still increases with both annualized return and ITM depth, while scaling down higher-beta names.

Paper submission is now guarded behind `read_only = false`, `enable_paper_orders = true`, `runtime_mode = "paper"`, `market_data_mode = "live"`, and `enable_live_orders = false`. Routed paper sizing now requires `AVAILABLE_FUNDS` from the IBKR account summary, while `capital_source = "buying_power"` remains analysis-only for allocation previews and logs. The guarded paper flow routes one combo BAG order, refuses to route symbols that relied on delayed/frozen data, persists a durable per-symbol/per-intent paper ledger to block duplicate submissions across restarts, and can auto-reprice the combo debit through a small capped ladder (`auto_reprice`, `reprice_attempts`, `reprice_wait_seconds`) without breaching the configured profit floor. No automated exit strategy is implemented in this slice, so tracked paper positions remain hold-to-close only until IBKR reports them closed.

For thin delayed/frozen option markets, the scanner now retries one model-price snapshot before rejecting a contract as missing premium and includes IBKR notices plus the fields that were actually returned. This is especially useful for off-hours troubleshooting on `NVTS`, `PTON`, and `BB`, where delayed/frozen data often returns greeks or underlying marks without a usable bid/last/close on the option itself.

## Repository Layout

- `AGENTS.md`: Codex workflow rules for this project
- `Python Code/`: legacy baseline and input artifacts
- `docs/`: setup docs, notes, and starter watchlist
- `crates/ibkr-options-engine/`: Rust scanner and execution engine
- `.env.example`: local configuration template
