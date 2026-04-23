# IBKR Account Ops

This helper is a separate Rust binary for account-management tasks that sit outside the main scan/route flow.

File:
- `crates/ibkr-options-engine/src/bin/ibkr_account_ops.rs`

Run pattern:
```powershell
cargo run -p ibkr-options-engine --bin ibkr_account_ops -- <command> [options]
```

Commands:

- `positions`
  Pulls the current account positions, includes a summarized open-position view, and reports available cash from IBKR account state.

- `orders`
  Pulls orders and groups them into:
  `fulfilled`, `working` by live status bucket, and `terminal_non_filled`.

- `cancel-open`
  Targets all currently open orders that IBKR still reports as working.
  Without `--execute`, this is a preview only.
  With `--execute`, it requests cancellation for each open order.

- `close-bags`
  Builds BAG closeout plans for covered-call positions that currently look balanced as stock plus short call.
  Without `--execute`, this previews the BAG orders that would be submitted.
  With `--execute`, it submits the BAG closeout orders.
  Optional:
  `--symbols AAPL MSFT` limits the action to specific symbols.

- `reconcile-log`
  Reconciles the local paper-trade ledger with the account's currently reported positions and orders, then persists the refreshed ledger state to disk.

Examples:
```powershell
cargo run -p ibkr-options-engine --bin ibkr_account_ops -- positions
cargo run -p ibkr-options-engine --bin ibkr_account_ops -- orders
cargo run -p ibkr-options-engine --bin ibkr_account_ops -- cancel-open
cargo run -p ibkr-options-engine --bin ibkr_account_ops -- cancel-open --execute
cargo run -p ibkr-options-engine --bin ibkr_account_ops -- close-bags
cargo run -p ibkr-options-engine --bin ibkr_account_ops -- close-bags --symbols AAPL MSFT
cargo run -p ibkr-options-engine --bin ibkr_account_ops -- close-bags --execute
cargo run -p ibkr-options-engine --bin ibkr_account_ops -- reconcile-log
```

Safety notes:

- `cancel-open` and `close-bags` are preview-only unless `--execute` is passed.
- `close-bags` only plans exits for covered-call positions that can be reconstructed as a clean BAG closeout.
- This helper is meant for paper-account operations unless you intentionally point it elsewhere through your IBKR config.
