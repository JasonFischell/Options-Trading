use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use ibapi::{
    orders::{
        Action, CommissionReport, ExecutionData, Order, OrderStatus, PlaceOrder, order_builder,
    },
    prelude::{Client, Contract, Currency, Exchange, SecurityType, Symbol},
};
use tokio::time::{Duration, timeout};

use crate::{
    config::{AppConfig, RuntimeMode},
    models::{
        ExecutionLegRecord, ExecutionRecord, FillReconciliationRecord, InstrumentType, OrderIntent,
        OrderLegIntent, TradeAction,
    },
};

#[async_trait(?Send)]
pub trait OrderExecutor {
    async fn execute(
        &self,
        intents: &[OrderIntent],
        config: &AppConfig,
    ) -> Result<Vec<ExecutionRecord>>;
}

#[derive(Debug, Default)]
pub struct GuardedDryRunExecutor;

#[async_trait(?Send)]
impl OrderExecutor for GuardedDryRunExecutor {
    async fn execute(
        &self,
        intents: &[OrderIntent],
        config: &AppConfig,
    ) -> Result<Vec<ExecutionRecord>> {
        let mut records = intents
            .iter()
            .map(|intent| dry_run_record(intent, config, dry_run_note(config), "dry-run"))
            .collect::<Vec<_>>();

        if intents.is_empty() {
            records.push(ExecutionRecord {
                symbol: "N/A".to_string(),
                status: "noop".to_string(),
                submission_mode: "dry-run".to_string(),
                note: "no order intents passed all guardrails in this cycle".to_string(),
                legs: Vec::new(),
                fill_reconciliation: None,
            });
        }

        Ok(records)
    }
}

#[derive(Debug, Clone)]
enum BrokerOrderEvent {
    OrderStatus(OrderStatus),
    ExecutionData(ExecutionData),
    CommissionReport(CommissionReport),
    Message { code: i32, message: String },
}

#[derive(Debug, Clone)]
struct SubmittedLegOutcome {
    order_id: i32,
    events: Vec<BrokerOrderEvent>,
}

#[derive(Debug, Clone)]
struct LegFillSummary {
    status: String,
    filled_quantity: f64,
    average_fill_price: Option<f64>,
    execution_ids: Vec<String>,
    total_commission: Option<f64>,
    note: String,
}

impl LegFillSummary {
    fn is_fully_filled(&self, expected_quantity: i32) -> bool {
        (self.filled_quantity - expected_quantity as f64).abs() < 0.0001
    }
}

#[async_trait(?Send)]
trait BrokerOrderGateway {
    fn next_order_id(&self) -> i32;

    async fn place_order_and_collect(
        &self,
        order_id: i32,
        contract: &Contract,
        order: &Order,
        idle_timeout: Duration,
    ) -> Result<Vec<BrokerOrderEvent>>;
}

struct IbkrOrderGateway {
    client: Arc<Client>,
}

impl IbkrOrderGateway {
    fn new(client: Arc<Client>) -> Self {
        Self { client }
    }
}

#[async_trait(?Send)]
impl BrokerOrderGateway for IbkrOrderGateway {
    fn next_order_id(&self) -> i32 {
        self.client.next_order_id()
    }

    async fn place_order_and_collect(
        &self,
        order_id: i32,
        contract: &Contract,
        order: &Order,
        idle_timeout: Duration,
    ) -> Result<Vec<BrokerOrderEvent>> {
        let mut subscription = self
            .client
            .place_order(order_id, contract, order)
            .await
            .with_context(|| format!("failed to place IBKR order {order_id}"))?;
        let mut events = Vec::new();

        loop {
            let next_item = timeout(idle_timeout, subscription.next()).await;
            let next_stream_item = match next_item {
                Ok(item) => item,
                Err(_) => break,
            };

            let Some(result) = next_stream_item else {
                break;
            };

            match result
                .with_context(|| format!("failed while monitoring IBKR order {order_id}"))?
            {
                PlaceOrder::OrderStatus(status) => {
                    events.push(BrokerOrderEvent::OrderStatus(status))
                }
                PlaceOrder::ExecutionData(data) => {
                    events.push(BrokerOrderEvent::ExecutionData(data))
                }
                PlaceOrder::CommissionReport(report) => {
                    events.push(BrokerOrderEvent::CommissionReport(report))
                }
                PlaceOrder::Message(notice) => events.push(BrokerOrderEvent::Message {
                    code: notice.code,
                    message: notice.message,
                }),
                PlaceOrder::OpenOrder(_) => {}
            }
        }

        Ok(events)
    }
}

pub struct GuardedPaperOrderExecutor {
    inner: GuardedPaperOrderExecutorInner<IbkrOrderGateway>,
}

struct GuardedPaperOrderExecutorInner<G> {
    gateway: G,
    idle_timeout: Duration,
}

impl GuardedPaperOrderExecutor {
    pub fn from_client(client: Arc<Client>) -> Self {
        Self {
            inner: GuardedPaperOrderExecutorInner {
                gateway: IbkrOrderGateway::new(client),
                idle_timeout: Duration::from_secs(5),
            },
        }
    }
}

impl<G> GuardedPaperOrderExecutorInner<G> {
    #[cfg(test)]
    fn new(gateway: G, idle_timeout: Duration) -> Self {
        Self {
            gateway,
            idle_timeout,
        }
    }
}

#[async_trait(?Send)]
impl OrderExecutor for GuardedPaperOrderExecutor {
    async fn execute(
        &self,
        intents: &[OrderIntent],
        config: &AppConfig,
    ) -> Result<Vec<ExecutionRecord>> {
        self.inner.execute(intents, config).await
    }
}

#[async_trait(?Send)]
impl<G> OrderExecutor for GuardedPaperOrderExecutorInner<G>
where
    G: BrokerOrderGateway,
{
    async fn execute(
        &self,
        intents: &[OrderIntent],
        config: &AppConfig,
    ) -> Result<Vec<ExecutionRecord>> {
        let mut records = Vec::new();

        for intent in intents {
            if config.risk.enable_live_orders || matches!(config.mode, RuntimeMode::Live) {
                records.push(dry_run_record(
                    intent,
                    config,
                    "live-order execution remains disabled; guarded paper routing only".to_string(),
                    "live-disabled",
                ));
                continue;
            }

            if !paper_submission_enabled(config) {
                records.push(dry_run_record(
                    intent,
                    config,
                    dry_run_note(config),
                    "dry-run",
                ));
                continue;
            }

            records.push(self.submit_guarded_buy_write(intent).await?);
        }

        if intents.is_empty() {
            records.push(ExecutionRecord {
                symbol: "N/A".to_string(),
                status: "noop".to_string(),
                submission_mode: "dry-run".to_string(),
                note: "no order intents passed all guardrails in this cycle".to_string(),
                legs: Vec::new(),
                fill_reconciliation: None,
            });
        }

        Ok(records)
    }
}

impl<G> GuardedPaperOrderExecutorInner<G>
where
    G: BrokerOrderGateway,
{
    async fn submit_guarded_buy_write(&self, intent: &OrderIntent) -> Result<ExecutionRecord> {
        let stock_leg = intent
            .legs
            .iter()
            .find(|leg| {
                leg.instrument_type == InstrumentType::Stock && leg.action == TradeAction::Buy
            })
            .with_context(|| {
                format!(
                    "deep-ITM buy-write intent for {} is missing the stock buy leg",
                    intent.symbol
                )
            })?;
        let option_leg = intent
            .legs
            .iter()
            .find(|leg| {
                leg.instrument_type == InstrumentType::Option && leg.action == TradeAction::Sell
            })
            .with_context(|| {
                format!(
                    "deep-ITM buy-write intent for {} is missing the short-call leg",
                    intent.symbol
                )
            })?;

        let stock_submission = self.submit_leg(intent, stock_leg).await?;
        let stock_summary = summarize_leg_outcome(stock_leg, Some(&stock_submission));
        let stock_record = execution_leg_record(stock_leg, Some(&stock_submission), &stock_summary);

        if !stock_summary.is_fully_filled(stock_leg.quantity) {
            return Ok(ExecutionRecord {
                symbol: intent.symbol.clone(),
                status: "stock-pending".to_string(),
                submission_mode: "paper".to_string(),
                note: "submitted stock leg in paper mode; short call remains blocked until fill reconciliation confirms covered shares"
                    .to_string(),
                legs: vec![
                    stock_record,
                    pending_leg_record(
                        option_leg,
                        "awaiting-stock-fill",
                        "stock leg has not fully filled yet; short-call submission remains gated",
                    ),
                ],
                fill_reconciliation: Some(reconcile_buy_write_fill(
                    stock_leg,
                    &stock_summary,
                    option_leg,
                    None,
                )),
            });
        }

        let option_submission = self.submit_leg(intent, option_leg).await?;
        let option_summary = summarize_leg_outcome(option_leg, Some(&option_submission));
        let option_record =
            execution_leg_record(option_leg, Some(&option_submission), &option_summary);
        let fill_reconciliation =
            reconcile_buy_write_fill(stock_leg, &stock_summary, option_leg, Some(&option_summary));

        Ok(ExecutionRecord {
            symbol: intent.symbol.clone(),
            status: fill_reconciliation.status.clone(),
            submission_mode: "paper".to_string(),
            note: "submitted guarded deep-ITM covered-call buy-write legs in paper mode using stock-first sequencing"
                .to_string(),
            legs: vec![stock_record, option_record],
            fill_reconciliation: Some(fill_reconciliation),
        })
    }

    async fn submit_leg(
        &self,
        intent: &OrderIntent,
        leg: &OrderLegIntent,
    ) -> Result<SubmittedLegOutcome> {
        let contract = build_ibkr_contract(leg)?;
        let order = build_ibkr_order(intent, leg)?;
        let order_id = self.gateway.next_order_id();
        let events = self
            .gateway
            .place_order_and_collect(order_id, &contract, &order, self.idle_timeout)
            .await?;

        Ok(SubmittedLegOutcome { order_id, events })
    }
}

fn paper_submission_enabled(config: &AppConfig) -> bool {
    config.risk.enable_paper_orders
        && matches!(config.mode, RuntimeMode::Paper)
        && !config.read_only
        && !config.risk.enable_live_orders
}

fn dry_run_note(config: &AppConfig) -> String {
    if config.risk.enable_live_orders {
        "live-order execution remains disabled in this milestone".to_string()
    } else if config.risk.enable_paper_orders
        && matches!(config.mode, RuntimeMode::Paper)
        && !config.read_only
    {
        "paper-order flag is enabled; stock-first guarded submission will only run when a live IBKR client is attached"
            .to_string()
    } else {
        "proposed dry-run order only; no broker submission attempted".to_string()
    }
}

fn dry_run_record(
    intent: &OrderIntent,
    config: &AppConfig,
    note: String,
    status: &str,
) -> ExecutionRecord {
    ExecutionRecord {
        symbol: intent.symbol.clone(),
        status: status.to_string(),
        submission_mode: if paper_submission_enabled(config) {
            "paper".to_string()
        } else {
            "dry-run".to_string()
        },
        note,
        legs: intent
            .legs
            .iter()
            .map(|leg| {
                pending_leg_record(
                    leg,
                    "not-submitted",
                    "guardrails kept this leg out of broker routing",
                )
            })
            .collect(),
        fill_reconciliation: None,
    }
}

fn build_ibkr_contract(leg: &OrderLegIntent) -> Result<Contract> {
    match leg.instrument_type {
        InstrumentType::Stock => Ok(Contract {
            symbol: Symbol::from(leg.symbol.as_str()),
            security_type: SecurityType::Stock,
            exchange: Exchange::from(leg.exchange.as_deref().unwrap_or("SMART")),
            currency: Currency::from(leg.currency.as_deref().unwrap_or("USD")),
            ..Default::default()
        }),
        InstrumentType::Option => {
            let expiry = leg
                .expiry
                .as_deref()
                .with_context(|| format!("missing option expiry for {}", leg.description))?;
            let strike = leg
                .strike
                .with_context(|| format!("missing option strike for {}", leg.description))?;
            let right = leg
                .right
                .as_deref()
                .with_context(|| format!("missing option right for {}", leg.description))?;
            let mut contract = Contract::option(&leg.symbol, expiry, strike, right);
            contract.exchange = Exchange::from(leg.exchange.as_deref().unwrap_or("SMART"));
            contract.currency = Currency::from(leg.currency.as_deref().unwrap_or("USD"));
            if let Some(trading_class) = &leg.trading_class {
                contract.trading_class = trading_class.clone();
            }
            if let Some(multiplier) = &leg.multiplier {
                contract.multiplier = multiplier.clone();
            }
            Ok(contract)
        }
    }
}

fn build_ibkr_order(intent: &OrderIntent, leg: &OrderLegIntent) -> Result<Order> {
    let action = match leg.action {
        TradeAction::Buy => Action::Buy,
        TradeAction::Sell => Action::Sell,
    };
    let limit_price = leg
        .limit_price
        .with_context(|| format!("missing limit price for {}", leg.description))?;
    let mut order = order_builder::limit_order(action, leg.quantity as f64, limit_price);
    order.account = intent.account.clone();
    order.order_ref = format!(
        "deepitm-buywrite:{}:{}:{}",
        intent.symbol,
        match leg.instrument_type {
            InstrumentType::Stock => "stock",
            InstrumentType::Option => "option",
        },
        match leg.action {
            TradeAction::Buy => "buy",
            TradeAction::Sell => "sell",
        }
    );
    Ok(order)
}

fn summarize_leg_outcome(
    leg: &OrderLegIntent,
    outcome: Option<&SubmittedLegOutcome>,
) -> LegFillSummary {
    let Some(outcome) = outcome else {
        return LegFillSummary {
            status: "not-submitted".to_string(),
            filled_quantity: 0.0,
            average_fill_price: None,
            execution_ids: Vec::new(),
            total_commission: None,
            note: "leg not submitted".to_string(),
        };
    };

    let mut status = "submitted".to_string();
    let mut filled_quantity: f64 = 0.0;
    let mut execution_ids = Vec::new();
    let mut last_average_fill_price = None;
    let mut weighted_fill_value = 0.0;
    let mut weighted_fill_shares = 0.0;
    let mut total_commission = 0.0;
    let mut saw_commission = false;
    let mut notes = Vec::new();

    for event in &outcome.events {
        match event {
            BrokerOrderEvent::OrderStatus(order_status) => {
                if !order_status.status.is_empty() {
                    status = order_status.status.clone();
                }
                filled_quantity = filled_quantity.max(order_status.filled);
                if order_status.average_fill_price > 0.0 {
                    last_average_fill_price = Some(order_status.average_fill_price);
                }
            }
            BrokerOrderEvent::ExecutionData(data) => {
                filled_quantity = filled_quantity.max(
                    data.execution
                        .cumulative_quantity
                        .max(data.execution.shares),
                );
                if data.execution.shares > 0.0 && data.execution.price > 0.0 {
                    weighted_fill_value += data.execution.shares * data.execution.price;
                    weighted_fill_shares += data.execution.shares;
                }
                if !data.execution.execution_id.is_empty() {
                    execution_ids.push(data.execution.execution_id.clone());
                }
            }
            BrokerOrderEvent::CommissionReport(report) => {
                total_commission += report.commission;
                saw_commission = true;
            }
            BrokerOrderEvent::Message { code, message } => {
                notes.push(format!("{code}: {message}"));
                if (200..300).contains(code) {
                    status = "rejected".to_string();
                }
            }
        }
    }

    let average_fill_price = if weighted_fill_shares > 0.0 {
        Some(weighted_fill_value / weighted_fill_shares)
    } else {
        last_average_fill_price
    };

    if notes.is_empty() {
        notes.push(format!("processed {} {}", leg.quantity, leg.description));
    }

    LegFillSummary {
        status,
        filled_quantity,
        average_fill_price,
        execution_ids,
        total_commission: saw_commission.then_some(total_commission),
        note: notes.join(" | "),
    }
}

fn execution_leg_record(
    leg: &OrderLegIntent,
    outcome: Option<&SubmittedLegOutcome>,
    summary: &LegFillSummary,
) -> ExecutionLegRecord {
    ExecutionLegRecord {
        leg_symbol: leg.symbol.clone(),
        instrument_type: leg.instrument_type.clone(),
        action: leg.action.clone(),
        quantity: leg.quantity,
        order_id: outcome.map(|value| value.order_id),
        submission_status: summary.status.clone(),
        limit_price: leg.limit_price,
        filled_quantity: summary.filled_quantity,
        average_fill_price: summary.average_fill_price,
        execution_ids: summary.execution_ids.clone(),
        note: summary.note.clone(),
    }
}

fn pending_leg_record(leg: &OrderLegIntent, status: &str, note: &str) -> ExecutionLegRecord {
    ExecutionLegRecord {
        leg_symbol: leg.symbol.clone(),
        instrument_type: leg.instrument_type.clone(),
        action: leg.action.clone(),
        quantity: leg.quantity,
        order_id: None,
        submission_status: status.to_string(),
        limit_price: leg.limit_price,
        filled_quantity: 0.0,
        average_fill_price: None,
        execution_ids: Vec::new(),
        note: note.to_string(),
    }
}

fn reconcile_buy_write_fill(
    stock_leg: &OrderLegIntent,
    stock_summary: &LegFillSummary,
    option_leg: &OrderLegIntent,
    option_summary: Option<&LegFillSummary>,
) -> FillReconciliationRecord {
    let short_call_filled_contracts = option_summary
        .map(|summary| summary.filled_quantity)
        .unwrap_or(0.0);
    let stock_filled_shares = stock_summary.filled_quantity;
    let uncovered_shares = (stock_filled_shares - (short_call_filled_contracts * 100.0)).max(0.0);
    let total_commission = match (
        stock_summary.total_commission,
        option_summary.and_then(|summary| summary.total_commission),
    ) {
        (Some(stock), Some(option)) => Some(stock + option),
        (Some(stock), None) => Some(stock),
        (None, Some(option)) => Some(option),
        (None, None) => None,
    };

    let (status, note) =
        if option_summary.is_none() && stock_summary.is_fully_filled(stock_leg.quantity) {
            (
                "stock-filled-awaiting-short-call".to_string(),
                "stock fill reconciled; short-call leg is now eligible for submission".to_string(),
            )
        } else if option_summary
            .map(|summary| summary.is_fully_filled(option_leg.quantity))
            .unwrap_or(false)
        {
            (
                "deep-itm-covered-call-open".to_string(),
                "stock and short-call fills reconcile to a deep-ITM covered-call paper position"
                    .to_string(),
            )
        } else if option_summary.is_some() {
            (
            "short-call-submitted".to_string(),
            "stock fill reconciled; short-call leg has been submitted and is still pending fill"
                .to_string(),
        )
        } else {
            (
                "stock-pending".to_string(),
                "stock leg has not fully filled, so the short-call leg remains gated".to_string(),
            )
        };

    FillReconciliationRecord {
        stock_filled_shares,
        stock_average_fill_price: stock_summary.average_fill_price,
        short_call_filled_contracts,
        short_call_average_fill_price: option_summary
            .and_then(|summary| summary.average_fill_price),
        total_commission,
        eligible_for_short_call: option_summary.is_none()
            && stock_summary.is_fully_filled(stock_leg.quantity),
        uncovered_shares,
        status,
        note,
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::VecDeque};

    use super::{
        BrokerOrderEvent, BrokerOrderGateway, GuardedPaperOrderExecutorInner, OrderExecutor,
        SubmittedLegOutcome, build_ibkr_contract, build_ibkr_order, reconcile_buy_write_fill,
        summarize_leg_outcome,
    };
    use crate::{
        config::{
            AppConfig, BrokerPlatform, MarketDataMode, RiskConfig, RunMode, RuntimeMode,
            StrategyConfig,
        },
        models::{InstrumentType, OrderIntent, OrderLegIntent, TradeAction},
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use ibapi::{
        orders::{CommissionReport, Execution, ExecutionData, Order, OrderStatus},
        prelude::Contract,
    };
    use tokio::time::Duration;

    struct MockGateway {
        next_order_ids: RefCell<VecDeque<i32>>,
        submissions: RefCell<Vec<(i32, Contract, Order)>>,
        events: RefCell<VecDeque<Vec<BrokerOrderEvent>>>,
    }

    impl MockGateway {
        fn new(next_order_ids: Vec<i32>, events: Vec<Vec<BrokerOrderEvent>>) -> Self {
            Self {
                next_order_ids: RefCell::new(next_order_ids.into()),
                submissions: RefCell::new(Vec::new()),
                events: RefCell::new(events.into()),
            }
        }
    }

    #[async_trait(?Send)]
    impl BrokerOrderGateway for MockGateway {
        fn next_order_id(&self) -> i32 {
            self.next_order_ids
                .borrow_mut()
                .pop_front()
                .expect("missing test order id")
        }

        async fn place_order_and_collect(
            &self,
            order_id: i32,
            contract: &Contract,
            order: &Order,
            _idle_timeout: Duration,
        ) -> Result<Vec<BrokerOrderEvent>> {
            self.submissions
                .borrow_mut()
                .push((order_id, contract.clone(), order.clone()));
            Ok(self.events.borrow_mut().pop_front().unwrap_or_default())
        }
    }

    fn paper_config() -> AppConfig {
        AppConfig {
            host: "127.0.0.1".to_string(),
            platform: BrokerPlatform::Gateway,
            port: 4002,
            client_id: 100,
            account: "DU1234567".to_string(),
            mode: RuntimeMode::Paper,
            read_only: false,
            connect_on_start: true,
            run_mode: RunMode::Manual,
            scan_schedule: "manual".to_string(),
            market_data_mode: MarketDataMode::DelayedFrozen,
            universe_file: None,
            symbols: vec!["AAPL".to_string()],
            startup_warnings: Vec::new(),
            strategy: StrategyConfig::default(),
            risk: RiskConfig {
                enable_paper_orders: true,
                enable_live_orders: false,
                ..RiskConfig::default()
            },
        }
    }

    fn buy_write_intent() -> OrderIntent {
        OrderIntent {
            symbol: "AAPL".to_string(),
            strategy: "deep-ITM covered-call buy-write".to_string(),
            account: "DU1234567".to_string(),
            mode: "paper-stock-first".to_string(),
            estimated_net_debit: 8_600.0,
            estimated_credit: 1_400.0,
            max_profit: 400.0,
            legs: vec![
                OrderLegIntent {
                    instrument_type: InstrumentType::Stock,
                    action: TradeAction::Buy,
                    symbol: "AAPL".to_string(),
                    description: "Buy 100 shares of AAPL".to_string(),
                    quantity: 100,
                    limit_price: Some(100.0),
                    expiry: None,
                    strike: None,
                    right: None,
                    exchange: Some("SMART".to_string()),
                    trading_class: None,
                    multiplier: None,
                    currency: Some("USD".to_string()),
                },
                OrderLegIntent {
                    instrument_type: InstrumentType::Option,
                    action: TradeAction::Sell,
                    symbol: "AAPL".to_string(),
                    description: "Sell 1 deep-ITM covered call AAPL 20260515 90".to_string(),
                    quantity: 1,
                    limit_price: Some(14.0),
                    expiry: Some("20260515".to_string()),
                    strike: Some(90.0),
                    right: Some("C".to_string()),
                    exchange: Some("SMART".to_string()),
                    trading_class: Some("AAPL".to_string()),
                    multiplier: Some("100".to_string()),
                    currency: Some("USD".to_string()),
                },
            ],
        }
    }

    #[test]
    fn builds_ibkr_order_contracts_with_option_metadata() {
        let intent = buy_write_intent();
        let stock_contract = build_ibkr_contract(&intent.legs[0]).unwrap();
        let option_contract = build_ibkr_contract(&intent.legs[1]).unwrap();
        let option_order = build_ibkr_order(&intent, &intent.legs[1]).unwrap();

        assert_eq!(stock_contract.symbol.to_string(), "AAPL");
        assert_eq!(option_contract.trading_class, "AAPL");
        assert_eq!(option_contract.multiplier, "100");
        assert_eq!(option_contract.right, "C");
        assert_eq!(
            option_contract.last_trade_date_or_contract_month,
            "20260515"
        );
        assert_eq!(option_order.account, "DU1234567");
        assert_eq!(option_order.total_quantity, 1.0);
        assert_eq!(option_order.limit_price, Some(14.0));
    }

    #[tokio::test]
    async fn submits_stock_first_and_holds_option_until_fill() {
        let gateway = MockGateway::new(
            vec![10],
            vec![vec![BrokerOrderEvent::OrderStatus(OrderStatus {
                order_id: 10,
                status: "Submitted".to_string(),
                filled: 0.0,
                remaining: 100.0,
                ..OrderStatus::default()
            })]],
        );
        let executor = GuardedPaperOrderExecutorInner::new(gateway, Duration::from_millis(1));
        let records = executor
            .execute(&[buy_write_intent()], &paper_config())
            .await
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, "stock-pending");
        assert_eq!(records[0].legs.len(), 2);
        assert_eq!(records[0].legs[0].order_id, Some(10));
        assert_eq!(records[0].legs[1].submission_status, "awaiting-stock-fill");
        assert!(
            records[0]
                .fill_reconciliation
                .as_ref()
                .is_some_and(|fill| !fill.eligible_for_short_call)
        );
    }

    #[test]
    fn fill_reconciliation_scaffolding_marks_short_call_eligibility() {
        let intent = buy_write_intent();
        let stock_summary = summarize_leg_outcome(
            &intent.legs[0],
            Some(&SubmittedLegOutcome {
                order_id: 10,
                events: vec![
                    BrokerOrderEvent::ExecutionData(ExecutionData {
                        execution: Execution {
                            execution_id: "stock-fill".to_string(),
                            shares: 100.0,
                            cumulative_quantity: 100.0,
                            price: 100.25,
                            average_price: 100.25,
                            ..Execution::default()
                        },
                        ..ExecutionData::default()
                    }),
                    BrokerOrderEvent::CommissionReport(CommissionReport {
                        execution_id: "stock-fill".to_string(),
                        commission: 1.25,
                        ..CommissionReport::default()
                    }),
                ],
            }),
        );
        let reconciliation =
            reconcile_buy_write_fill(&intent.legs[0], &stock_summary, &intent.legs[1], None);

        assert_eq!(reconciliation.status, "stock-filled-awaiting-short-call");
        assert!(reconciliation.eligible_for_short_call);
        assert_eq!(reconciliation.uncovered_shares, 100.0);
        assert_eq!(reconciliation.total_commission, Some(1.25));
    }
}
