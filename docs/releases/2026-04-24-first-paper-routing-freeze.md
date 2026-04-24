# 2026-04-24 First Paper Routing Freeze

This document records the first functional release snapshot of the Rust-first IBKR automation flow that both allocates a defined cash budget across positions and routes guarded paper trades.

## Release Scope

- Rust production path in `crates/ibkr-options-engine`
- Cash deployment sizing through `[allocation]`
- Guarded paper-only combo BAG order routing through `[execution]`
- Persistent paper-order state to prevent duplicate submissions across restarts
- Current operational config preserved in `docs/releases/2026-04-24-paper-trading.toml`

## Intended Use

- Preserve the current behavior as a stable baseline before later cleanup and improvements
- Keep this snapshot focused on paper trading only
- Use it as the rollback point if a later refactor destabilizes allocation or routing

## Current Guardrails

- `runtime_mode = "paper"`
- `enable_paper_orders = true`
- `enable_live_orders = false`
- `capital_source = "available_funds"`
- `min_cash_reserve_ratio = 0.05`
- `max_cash_per_symbol_ratio = 0.10`

## Verification Target

This freeze should continue to pass targeted workspace verification with:

```powershell
cargo test -p ibkr-options-engine
```
