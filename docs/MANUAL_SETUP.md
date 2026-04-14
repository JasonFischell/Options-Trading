# Manual Setup

These are the manual steps still needed before coding the Interactive Brokers-enabled version.

## 1. Install Core Tools

Install the following on the development machine:

1. Git
2. Python 3.11+ only if you want to run the legacy baseline script during migration
3. A code editor or IDE
4. Rust via `rustup`
5. TWS or IB Gateway from Interactive Brokers

## 2. Create Accounts And API Access

1. Confirm you have an Interactive Brokers account with paper trading enabled
2. Confirm your IBKR market-data subscriptions cover the symbols and options markets you want to trade
3. Decide whether early development will use TWS paper trading or IB Gateway paper trading

## 3. Enable Interactive Brokers API Access

In TWS or IB Gateway:

1. Log in to paper trading first
2. Open API settings
3. Enable socket/API clients
4. Note the host and port values
5. Restrict trusted IPs as needed for local development
6. Decide whether read-only mode should stay enabled during early testing

## 4. Move Secrets Out Of Source Code

Before we add broker connectivity:

1. Create a local `.env` file that is not committed
2. Copy values from `.env.example`
3. Fill in IBKR host, port, client ID, and account identifiers

Suggested variables:

- `IBKR_HOST`
- `IBKR_PORT`
- `IBKR_CLIENT_ID`
- `IBKR_ACCOUNT`
- `IBKR_RUNTIME_MODE`
- `IBKR_READ_ONLY`
- `IBKR_SYMBOLS`
- `IBKR_CONNECT_ON_START`

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
3. Start TWS or IB Gateway in paper mode
4. Run `cargo build`
5. Run `cargo run -p ibkr-options-engine`
6. Set `IBKR_CONNECT_ON_START=true` only when you want the app to test a real broker connection at startup
