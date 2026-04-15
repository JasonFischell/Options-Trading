# Options Trading

This repository is building a Rust-first IBKR options scanner and automation engine. The production path is now centered on `crates/ibkr-options-engine`, while `Python Code/` remains a parity/reference harness only.

The current vertical slice is designed around token-efficient Codex sessions:

- scheduled batch scans instead of an always-on daemon
- delayed or delayed-frozen snapshots before live data
- a small watchlist first, then controlled expansion
- guarded dry-run buy-write intent generation before broker submission

## Current Architecture

The Rust crate is organized around the runtime layers we want long term:

- `config`: env-driven runtime, market-data, schedule, and guardrail settings
- `market_data`: watchlist loading plus the replay-testable market-data boundary
- `ibkr`: narrow Interactive Brokers adapter for connectivity, snapshots, positions, and chains
- `strategy`: covered-call candidate evaluation and basic exit logic
- `state`: portfolio summaries and buy-write order-intent guardrails
- `execution`: guarded dry-run execution layer
- `scanner`: end-to-end cycle orchestration and cycle-report generation
- `scoring`: legacy/reference scoring math carried forward for parity work

The engine currently supports:

- loading a starter universe from CSV or `IBKR_SYMBOLS`
- connecting to IBKR and switching market-data modes
- fetching account state, positions, underlying snapshots, option chains, and option quotes
- ranking covered-call buy-write candidates with conservative liquidity filters
- generating guarded dry-run order intents
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

- Harden buy-write order construction
- Add paper-trading submission behind explicit flags
- Reconcile fills, positions, and duplicate-symbol prevention

### Phase D

- Poll open paper positions
- Apply basic profit-take and max-loss exits
- Extend reporting around lifecycle events

### Later Phases

- selective live-data upgrade for paper trading
- larger universe management with measured batching changes
- live-money readiness only after paper stability
- optional historical-options backtesting on a separate data track

## Running The Current Scanner

1. Copy `.env.example` to `.env`
2. Point `UNIVERSE_FILE` to `docs/starter_watchlist.csv` or set `IBKR_SYMBOLS`
3. Keep `IBKR_READ_ONLY=true` and `ENABLE_PAPER_ORDERS=false` for early testing
4. Set `IBKR_CONNECT_ON_START=true` only when IB Gateway or TWS paper is running
5. Run `cargo test`
6. Run `cargo run -p ibkr-options-engine`

The current execution layer is intentionally dry-run only, even if `ENABLE_PAPER_ORDERS=true`. That flag is present now so the runtime contract is stable before real order submission is introduced.

## Repository Layout

- `AGENTS.md`: Codex workflow rules for this project
- `Python Code/`: legacy baseline and input artifacts
- `docs/`: setup docs, notes, and starter watchlist
- `crates/ibkr-options-engine/`: Rust scanner and execution engine
- `.env.example`: local configuration template
