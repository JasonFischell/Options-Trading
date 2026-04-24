use std::collections::BTreeMap;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use dotenvy::dotenv;
use ibapi::{
    contracts::{ComboLeg, ComboLegOpenClose},
    orders::{Action, OrderStatus, PlaceOrder, TagValue, TimeInForce, order_builder},
    prelude::{Client, Contract, Currency, Exchange, SecurityType, Symbol},
};
use ibkr_options_engine::{
    config::AppConfig,
    ibkr::{
        SelectedOptionContract, cancel_open_order, connect, fetch_account_state,
        fetch_completed_orders, fetch_open_orders, fetch_positions, log_server_time,
        request_option_quote, request_underlying_snapshot, resolve_option_contract,
        resolve_primary_stock_contract, switch_market_data_mode,
    },
    models::{
        BrokerCompletedOrder, BrokerOpenOrder, InventoryPosition, OpenPositionState,
        PaperTradeLifecycleRecord,
    },
    paper_state::PaperTradeLedger,
    state::summarize_open_positions,
};
use serde::Serialize;
use tokio::time::{Duration, Instant, timeout};

#[derive(Debug, Parser)]
#[command(name = "ibkr_account_ops")]
#[command(about = "Standalone IBKR account-management helper")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Positions(ConfigArgs),
    Orders(ConfigArgs),
    CancelOpen(WriteArgs),
    CloseBags(CloseBagsArgs),
    ReconcileLog(ConfigArgs),
}

#[derive(Debug, Clone, Args, Default)]
struct ConfigArgs {
    #[arg(long)]
    config: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone, Args)]
struct WriteArgs {
    #[command(flatten)]
    config: ConfigArgs,
    #[arg(long, default_value_t = false)]
    execute: bool,
}

#[derive(Debug, Clone, Args)]
struct CloseBagsArgs {
    #[command(flatten)]
    config: ConfigArgs,
    #[arg(long, default_value_t = false)]
    execute: bool,
    #[arg(long)]
    symbols: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct OrdersReport {
    fulfilled: Vec<BrokerCompletedOrder>,
    working: BTreeMap<String, Vec<BrokerOpenOrder>>,
    terminal_non_filled: Vec<BrokerCompletedOrder>,
}

#[derive(Debug, Clone, Serialize)]
struct CancelOpenReport {
    account: String,
    endpoint: String,
    execute: bool,
    targeted_orders: Vec<BrokerOpenOrder>,
    cancelled_order_ids: Vec<i32>,
}

#[derive(Debug, Clone, Serialize)]
struct LedgerReconcileReport {
    account: String,
    endpoint: String,
    open_positions: Vec<OpenPositionState>,
    open_orders: Vec<BrokerOpenOrder>,
    completed_orders: Vec<BrokerCompletedOrder>,
    paper_trade_lifecycle_after_reconcile: Vec<PaperTradeLifecycleRecord>,
    action_log: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CloseBagPlan {
    symbol: String,
    stock_contract_id: i32,
    option_contract_id: i32,
    lots: i32,
    stock_shares: i32,
    stock_bid: Option<f64>,
    stock_last: Option<f64>,
    option_ask: Option<f64>,
    option_last: Option<f64>,
    estimated_limit_credit: f64,
    expiry: String,
    strike: f64,
    right: String,
    notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CloseBagsReport {
    account: String,
    endpoint: String,
    execute: bool,
    requested_symbols: Vec<String>,
    planned: Vec<CloseBagPlan>,
    skipped: Vec<String>,
    submitted_order_ids: Vec<i32>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let cli = Cli::parse();
    match cli.command {
        Command::Positions(args) => run_positions(args).await,
        Command::Orders(args) => run_orders(args).await,
        Command::CancelOpen(args) => run_cancel_open(args).await,
        Command::CloseBags(args) => run_close_bags(args).await,
        Command::ReconcileLog(args) => run_reconcile_log(args).await,
    }
}

async fn run_positions(args: ConfigArgs) -> Result<()> {
    let config = AppConfig::from_path(args.config.as_deref())?;
    let client = connect_and_log(&config).await?;
    let account_state = fetch_account_state(&client, &config.account).await?;
    let positions = fetch_positions(&client).await?;
    let open_positions = summarize_open_positions(&positions);

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "account": config.account,
            "endpoint": config.endpoint(),
            "available_cash": account_state.available_funds,
            "account_state": account_state,
            "raw_positions": positions,
            "open_positions": open_positions,
        }))?
    );
    Ok(())
}

async fn run_orders(args: ConfigArgs) -> Result<()> {
    let config = AppConfig::from_path(args.config.as_deref())?;
    let client = connect_and_log(&config).await?;
    let open_orders = fetch_open_orders(&client, &config.account).await?;
    let completed_orders = fetch_completed_orders(&client, &config.account).await?;

    let mut working: BTreeMap<String, Vec<BrokerOpenOrder>> = BTreeMap::new();
    for order in open_orders {
        working
            .entry(normalized_order_bucket(&order.status))
            .or_default()
            .push(order);
    }

    let mut fulfilled = Vec::new();
    let mut terminal_non_filled = Vec::new();
    for order in completed_orders {
        if completed_order_is_filled(&order) {
            fulfilled.push(order);
        } else {
            terminal_non_filled.push(order);
        }
    }

    let report = OrdersReport {
        fulfilled,
        working,
        terminal_non_filled,
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn run_cancel_open(args: WriteArgs) -> Result<()> {
    let config = AppConfig::from_path(args.config.config.as_deref())?;
    let client = connect_and_log(&config).await?;
    let targeted_orders = fetch_open_orders(&client, &config.account).await?;
    let mut cancelled_order_ids = Vec::new();
    let endpoint = config.endpoint();

    if args.execute {
        for order in &targeted_orders {
            cancel_open_order(&client, order.order_id).await?;
            cancelled_order_ids.push(order.order_id);
        }
    }

    let report = CancelOpenReport {
        account: config.account.clone(),
        endpoint,
        execute: args.execute,
        targeted_orders,
        cancelled_order_ids,
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn run_close_bags(args: CloseBagsArgs) -> Result<()> {
    let config = AppConfig::from_path(args.config.config.as_deref())?;
    let client = connect_and_log(&config).await?;
    switch_market_data_mode(&client, config.market_data_mode).await?;

    let positions = fetch_positions(&client).await?;
    let requested_symbols = normalize_symbols(&args.symbols);
    let (planned, skipped) = build_close_bag_plans(&client, &positions, &requested_symbols).await?;
    let mut submitted_order_ids = Vec::new();
    let endpoint = config.endpoint();

    if args.execute {
        for plan in &planned {
            let order_id = submit_close_bag_order(&client, &config, plan).await?;
            submitted_order_ids.push(order_id);
        }
    }

    let report = CloseBagsReport {
        account: config.account.clone(),
        endpoint,
        execute: args.execute,
        requested_symbols,
        planned,
        skipped,
        submitted_order_ids,
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn run_reconcile_log(args: ConfigArgs) -> Result<()> {
    let config = AppConfig::from_path(args.config.as_deref())?;
    let client = connect_and_log(&config).await?;
    let positions = fetch_positions(&client).await?;
    let open_positions = summarize_open_positions(&positions);
    let open_orders = fetch_open_orders(&client, &config.account).await?;
    let completed_orders = fetch_completed_orders(&client, &config.account).await?;
    let mut ledger = PaperTradeLedger::load(&config)?;
    let mut action_log = Vec::new();
    let endpoint = config.endpoint();

    ledger.reconcile_with_positions(&open_positions, &mut action_log);
    ledger.reconcile_with_broker_orders(&open_orders, &completed_orders, &mut action_log);
    ledger.persist(&config)?;

    let report = LedgerReconcileReport {
        account: config.account.clone(),
        endpoint,
        open_positions,
        open_orders,
        completed_orders,
        paper_trade_lifecycle_after_reconcile: ledger.snapshot(),
        action_log,
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn connect_and_log(config: &AppConfig) -> Result<Client> {
    let client = connect(&config.endpoint(), config.client_id).await?;
    log_server_time(&client).await?;
    Ok(client)
}

fn normalized_order_bucket(status: &str) -> String {
    let normalized = status.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        normalized.replace(' ', "-")
    }
}

fn completed_order_is_filled(order: &BrokerCompletedOrder) -> bool {
    let combined = format!(
        "{} {} {}",
        order.status, order.completed_status, order.warning_text
    )
    .to_ascii_lowercase();

    combined.contains("fill") && !combined.contains("cancel") && !combined.contains("reject")
}

async fn build_close_bag_plans(
    client: &Client,
    positions: &[InventoryPosition],
    requested_symbols: &[String],
) -> Result<(Vec<CloseBagPlan>, Vec<String>)> {
    let open_positions = summarize_open_positions(positions);
    let mut plans = Vec::new();
    let mut skipped = Vec::new();

    for open_position in open_positions {
        if !requested_symbols.is_empty()
            && !requested_symbols.iter().any(|symbol| symbol == &open_position.symbol)
        {
            continue;
        }

        match build_close_bag_plan(client, positions, &open_position).await {
            Ok(plan) => plans.push(plan),
            Err(error) => skipped.push(format!("{}: {}", open_position.symbol, error)),
        }
    }

    for symbol in requested_symbols {
        if !plans.iter().any(|plan| &plan.symbol == symbol)
            && !skipped.iter().any(|line| line.starts_with(&format!("{symbol}:")))
        {
            skipped.push(format!("{symbol}: symbol was requested but no open position is present"));
        }
    }

    Ok((plans, skipped))
}

async fn build_close_bag_plan(
    client: &Client,
    positions: &[InventoryPosition],
    open_position: &OpenPositionState,
) -> Result<CloseBagPlan> {
    if open_position.stock_shares < 100.0 || open_position.short_call_contracts < 1.0 {
        anyhow::bail!("position is not an open covered-call BAG candidate");
    }

    let lot_quantity = open_position.short_call_contracts.floor() as i32;
    let stock_shares = open_position.stock_shares.round() as i32;
    if stock_shares != lot_quantity * 100 {
        anyhow::bail!(
            "position is not balanced for a BAG closeout: shares={} contracts={}",
            stock_shares,
            lot_quantity
        );
    }

    let option_position = positions
        .iter()
        .find(|position| {
            position.symbol == open_position.symbol
                && position.security_type == "OPT"
                && position.quantity < 0.0
                && position.expiry.is_some()
                && position.strike.is_some()
                && position
                    .right
                    .as_deref()
                    .is_some_and(|right| right.eq_ignore_ascii_case("C"))
        })
        .with_context(|| format!("missing short call position details for {}", open_position.symbol))?;

    let stock_contract = resolve_primary_stock_contract(client, &open_position.symbol).await?;
    let selected = SelectedOptionContract {
        symbol: open_position.symbol.clone(),
        right: option_position
            .right
            .clone()
            .unwrap_or_else(|| "C".to_string()),
        expiration: option_position
            .expiry
            .clone()
            .with_context(|| format!("missing option expiry for {}", open_position.symbol))?,
        strike: option_position
            .strike
            .with_context(|| format!("missing option strike for {}", open_position.symbol))?,
        chain_metadata: Vec::new(),
    };
    let option_contract = resolve_option_contract(client, &selected).await?;
    let underlying = request_underlying_snapshot(client, &open_position.symbol).await?;
    let option_quote = request_option_quote(client, selected.clone()).await?;

    let stock_exit_price = underlying
        .bid
        .or(underlying.last)
        .or(underlying.close)
        .with_context(|| format!("missing usable stock exit price for {}", open_position.symbol))?;
    let option_cover_price = option_quote
        .ask
        .or(option_quote.last)
        .or(option_quote.close)
        .or(option_quote.option_price)
        .with_context(|| format!("missing usable option cover price for {}", open_position.symbol))?;
    let estimated_limit_credit = round_to_cents(stock_exit_price - option_cover_price);
    if estimated_limit_credit <= 0.0 {
        anyhow::bail!(
            "derived combo credit is non-positive for {}: stock_exit_price={stock_exit_price:.2}, option_cover_price={option_cover_price:.2}",
            open_position.symbol
        );
    }

    let mut notes = Vec::new();
    if underlying.price_source.contains("delayed") {
        notes.push(format!(
            "underlying snapshot appears non-live ({})",
            underlying.price_source
        ));
    }
    if option_quote.is_non_live() {
        notes.push("option snapshot appears non-live".to_string());
    }

    Ok(CloseBagPlan {
        symbol: open_position.symbol.clone(),
        stock_contract_id: stock_contract.contract_id,
        option_contract_id: option_contract.contract_id,
        lots: lot_quantity,
        stock_shares,
        stock_bid: underlying.bid,
        stock_last: underlying.last,
        option_ask: option_quote.ask,
        option_last: option_quote.last,
        estimated_limit_credit,
        expiry: selected.expiration,
        strike: selected.strike,
        right: selected.right,
        notes,
    })
}

async fn submit_close_bag_order(
    client: &Client,
    config: &AppConfig,
    plan: &CloseBagPlan,
) -> Result<i32> {
    let order_id = client.next_order_id();
    let contract = Contract {
        symbol: Symbol::from(plan.symbol.as_str()),
        security_type: SecurityType::Spread,
        exchange: Exchange::from("SMART"),
        currency: Currency::from("USD"),
        combo_legs: vec![
            ComboLeg {
                contract_id: plan.stock_contract_id,
                ratio: 100,
                action: "SELL".to_string(),
                exchange: "SMART".to_string(),
                open_close: ComboLegOpenClose::Same,
                ..Default::default()
            },
            ComboLeg {
                contract_id: plan.option_contract_id,
                ratio: 1,
                action: "BUY".to_string(),
                exchange: "SMART".to_string(),
                open_close: ComboLegOpenClose::Same,
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let mut order = order_builder::combo_limit_order(
        Action::Sell,
        plan.lots as f64,
        plan.estimated_limit_credit,
        false,
    );
    order.account = config.account.clone();
    order.order_type = "LMT".to_string();
    order.limit_price = Some(plan.estimated_limit_credit);
    order.tif = TimeInForce::Day;
    order.transmit = true;
    order.outside_rth = false;
    order.order_ref = format!("deepitm-buywrite:{}:combo:close", plan.symbol);
    order.smart_combo_routing_params = vec![TagValue {
        tag: "NonGuaranteed".to_string(),
        value: "0".to_string(),
    }];

    let mut subscription = client
        .place_order(order_id, &contract, &order)
        .await
        .with_context(|| format!("failed to place BAG closeout order for {}", plan.symbol))?;
    let started = Instant::now();
    let collection_window = Duration::from_secs(3);
    let idle_timeout = Duration::from_secs(1);

    while started.elapsed() < collection_window {
        match timeout(idle_timeout, subscription.next()).await {
            Ok(Some(result)) => match result? {
                PlaceOrder::OrderStatus(OrderStatus { status, .. })
                    if is_terminal_order_status(&status) =>
                {
                    break;
                }
                _ => {}
            },
            Ok(None) | Err(_) => break,
        }
    }

    Ok(order_id)
}

fn is_terminal_order_status(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "filled" | "cancelled" | "apicancelled" | "inactive"
    )
}

fn normalize_symbols(symbols: &[String]) -> Vec<String> {
    symbols
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .collect()
}

fn round_to_cents(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::{completed_order_is_filled, normalize_symbols, normalized_order_bucket};
    use ibkr_options_engine::models::BrokerCompletedOrder;

    #[test]
    fn groups_blank_status_as_unknown() {
        assert_eq!(normalized_order_bucket(""), "unknown");
        assert_eq!(normalized_order_bucket("Pre Submitted"), "pre-submitted");
    }

    #[test]
    fn treats_only_filled_completed_orders_as_fulfilled() {
        let filled = BrokerCompletedOrder {
            account: "DU123".to_string(),
            order_id: 1,
            client_id: 1,
            perm_id: 1,
            symbol: "AAPL".to_string(),
            security_type: "BAG".to_string(),
            action: "BUY".to_string(),
            total_quantity: 1.0,
            order_type: "LMT".to_string(),
            limit_price: Some(1.0),
            status: "Filled".to_string(),
            completed_status: "Filled".to_string(),
            reject_reason: String::new(),
            warning_text: String::new(),
            completed_time: String::new(),
        };
        let cancelled = BrokerCompletedOrder {
            status: "Cancelled".to_string(),
            completed_status: "Cancelled".to_string(),
            ..filled.clone()
        };

        assert!(completed_order_is_filled(&filled));
        assert!(!completed_order_is_filled(&cancelled));
    }

    #[test]
    fn normalizes_symbol_filters() {
        assert_eq!(
            normalize_symbols(&[" aapl ".to_string(), "msft".to_string()]),
            vec!["AAPL", "MSFT"]
        );
    }
}
