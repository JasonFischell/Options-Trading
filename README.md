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

1. Copy `.env.example` to `.env`
2. Keep `IBKR_PLATFORM=gateway` unless you intentionally want TWS
3. For paper Gateway, use `IBKR_PORT=4002` unless your Gateway settings show a different API port
4. Leave `UNIVERSE_FILE=docs/50_stocks_list.csv` unless you intentionally want a different CSV universe
5. Keep `MIN_UNDERLYING_PRICE=1` and `MAX_UNDERLYING_PRICE=20` for the current sub-$20 watchlist
6. Keep `IBKR_READ_ONLY=true` and `ENABLE_PAPER_ORDERS=false` for early testing
7. Set `IBKR_CONNECT_ON_START=true` only when IB Gateway or TWS paper is running
8. In IB Gateway, enable `Configure > Settings > API > Settings > Enable ActiveX and Socket Clients`
9. If localhost is restricted, add `127.0.0.1` to trusted IPs
10. Run `cargo test`
11. Run `cargo run -p ibkr-options-engine`

The current screening defaults mirror the Python reference more closely for deep-ITM calls, with explicit expiration-date selection available through `EXPIRATION_DATES=20260515` or a comma-separated list such as `EXPIRATION_DATES=20260515,20250620`. Ranking still increases with both annualized return and ITM depth, while scaling down higher-beta names.

Paper submission is now guarded behind `IBKR_READ_ONLY=false`, `ENABLE_PAPER_ORDERS=true`, `IBKR_RUNTIME_MODE=paper`, `MARKET_DATA_MODE=live`, and `ENABLE_LIVE_ORDERS=false`. The flow submits the stock leg first, persists a durable per-symbol/per-intent paper ledger to block duplicate submissions across restarts, refuses to route symbols that relied on delayed/frozen data, requires `BUYING_POWER` to be present on the IBKR paper account summary, refreshes open positions after broker activity, and only advances the short call after fill reconciliation confirms covered shares. No automated exit strategy is implemented in this slice, so tracked paper positions remain hold-to-close only until IBKR reports them closed.

For thin delayed/frozen option markets, the scanner now retries one model-price snapshot before rejecting a contract as missing premium and includes IBKR notices plus the fields that were actually returned. This is especially useful for off-hours troubleshooting on `NVTS`, `PTON`, and `BB`, where delayed/frozen data often returns greeks or underlying marks without a usable bid/last/close on the option itself.

## Repository Layout

- `AGENTS.md`: Codex workflow rules for this project
- `Python Code/`: legacy baseline and input artifacts
- `docs/`: setup docs, notes, and starter watchlist
- `crates/ibkr-options-engine/`: Rust scanner and execution engine
- `.env.example`: local configuration template
