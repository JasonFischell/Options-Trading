# Manual Setup

These are the manual steps still needed before coding the Interactive Brokers-enabled version.

## 1. Install Core Tools

Install the following on the development machine:

1. Git
2. Python 3.11+ only if you want to run the legacy baseline script during migration
3. A code editor or IDE
4. Rust via `rustup`
5. IB Gateway from Interactive Brokers
6. TWS only if you want a separate manual-debugging UI

## 2. Create Accounts And API Access

1. Confirm you have an Interactive Brokers account with paper trading enabled
2. Confirm your IBKR market-data subscriptions cover the symbols and options markets you want to trade
3. Default to IB Gateway paper trading for automation and scheduled runs

## 3. Enable Interactive Brokers API Access

In IB Gateway:

1. Log in to paper trading first
2. Open `Configure > Settings > API > Settings`
3. Enable `ActiveX and Socket Clients`
4. Confirm the API port
5. For paper Gateway, default expectation is `4002`
6. Allow localhost connections or add `127.0.0.1` to trusted IPs if you use the trusted-IP setting
7. Keep read-only mode enabled during early testing unless you intentionally begin guarded paper-order submission

## 4. Move Secrets And Runtime Settings Out Of Source Code

Before we add broker connectivity:

1. Create a local TOML config in `docs/local/`, such as `docs/local/paper-trading.local.toml`
2. Copy values from `docs/local/ibkr-options-engine.example.toml` or `docs/local/ibkr-options-engine.paper-trading.toml`
3. Fill in IBKR host, port, client ID, and account identifiers
4. Use `.env` only for compatibility or temporary local overrides

Suggested settings to review in the TOML file:

- `[broker].host`
- `[broker].platform`
- `[broker].port`
- `[broker].client_id`
- `[broker].account`
- `[broker].runtime_mode`
- `[broker].read_only`
- `[broker].connect_on_start`
- `[universe].tickers_file`
- `[strategy].expiration_dates`
- `[allocation].deployment_budget`
- `[allocation].capital_source`
- `[allocation].max_distribution_per_symbol_pct`
- `[allocation].min_cash_reserve_pct`
- `[execution].enable_paper_orders`
- `[execution].auto_reprice`
- `[execution].reprice_attempts`
- `[execution].reprice_wait_seconds`

## 5. Validate The Current Python Baseline

Before rewriting anything:

1. Prepare the ticker CSV input file if you want parity testing against the legacy script
2. Review `options_analysis_settings.txt`
3. Run the Python script once if you want a before-and-after benchmark
4. Save one output CSV as a baseline sample for later parity checks

## 6. Recommended Next Manual Decision

Choose one of these before we build the new core:

1. Rust as the primary production language
2. C# if Windows-first tooling and .NET ecosystem support are preferred
3. Go if developer speed matters more than peak optimization flexibility

My recommendation is Rust unless there is a strong team preference for the .NET ecosystem.

## 7. Verify The Rust Scaffold

From the repository root:

1. Open a new terminal after the Rust install so `cargo` and `rg` are on `PATH`
2. Copy `docs/local/ibkr-options-engine.example.toml` to `docs/local/paper-trading.local.toml`
3. Start IB Gateway in paper mode
4. Run `cargo build`
5. Run `cargo run -p ibkr-options-engine -- scan --config docs/local/paper-trading.local.toml`
6. Run `cargo run -p ibkr-options-engine -- status --config docs/local/paper-trading.local.toml`
7. Set `connect_on_start = true` only when you want the app to test a real broker connection at startup
8. Leave `enable_live_orders = false`; guarded buy-write submission is paper-only and routes as a combo BAG

Recommended starting values:

- `platform = "gateway"`
- `port = 4002`
- `runtime_mode = "paper"`
- `read_only = true`
- `tickers_file = "docs/50_stocks_list.csv"`
- `expiration_dates = ["20260515"]`
- `deployment_budget = 10000.0`
- `capital_source = "available_funds"`
- `max_distribution_per_symbol_pct = 20.0`
- `max_new_trades_per_cycle = 5`
- `max_open_positions = 5`
- `auto_reprice = true`
- `reprice_attempts = 3`
- `reprice_wait_seconds = 2`
