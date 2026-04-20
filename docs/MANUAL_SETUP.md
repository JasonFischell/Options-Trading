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

## 4. Move Secrets Out Of Source Code

Before we add broker connectivity:

1. Create a local `.env` file that is not committed
2. Copy values from `.env.example`
3. Fill in IBKR host, port, client ID, and account identifiers

Suggested variables:

- `IBKR_HOST`
- `IBKR_PLATFORM`
- `IBKR_PORT`
- `IBKR_CLIENT_ID`
- `IBKR_ACCOUNT`
- `IBKR_RUNTIME_MODE`
- `IBKR_READ_ONLY`
- `UNIVERSE_FILE`
- `TARGET_EXPIRY`
- `IBKR_CONNECT_ON_START`
- `MIN_UNDERLYING_PRICE`
- `MAX_UNDERLYING_PRICE`

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
2. Copy `.env.example` to `.env`
3. Start IB Gateway in paper mode
4. Run `cargo build`
5. Run `cargo run -p ibkr-options-engine`
6. Set `IBKR_CONNECT_ON_START=true` only when you want the app to test a real broker connection at startup
7. Leave `ENABLE_LIVE_ORDERS=false`; guarded buy-write submission is paper-only and routes as a combo BAG

Recommended starting values:

- `IBKR_PLATFORM=gateway`
- `IBKR_PORT=4002`
- `IBKR_RUNTIME_MODE=paper`
- `IBKR_READ_ONLY=true`
- `UNIVERSE_FILE=docs/50_stocks_list.csv`
- `TARGET_EXPIRY=20260424`
- `MIN_EXPIRATION_YIELD_PCT=1.0`
- `MIN_UNDERLYING_PRICE=1`
- `MAX_UNDERLYING_PRICE=20`
