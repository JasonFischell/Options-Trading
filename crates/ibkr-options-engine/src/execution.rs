use std::{fs, sync::Arc, time::Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ibapi::{
    contracts::{ComboLeg, ComboLegOpenClose},
    orders::{
        Action, CommissionReport, ExecutionData, Order, OrderData, OrderStatus, PlaceOrder,
        TagValue, TimeInForce, order_builder,
    },
    prelude::{Client, Contract, Currency, Exchange, SecurityType, Symbol},
};
use serde::Serialize;
use tokio::time::{Duration, timeout};

use crate::{
    artifacts::timestamped_log_path,
    config::{AppConfig, RuntimeMode},
    models::{
        BrokerEventTimelineEntry, ExecutionLegRecord, ExecutionRecord, ExecutionStepTiming,
        FillReconciliationRecord, InstrumentType, OrderIntent, OrderLegIntent, TradeAction,
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
pub struct AnalysisOnlyExecutor;

#[async_trait(?Send)]
impl OrderExecutor for AnalysisOnlyExecutor {
    async fn execute(
        &self,
        intents: &[OrderIntent],
        config: &AppConfig,
    ) -> Result<Vec<ExecutionRecord>> {
        let mut records = intents
            .iter()
            .map(|intent| {
                analysis_only_record(intent, config, analysis_only_note(config), "analysis-only")
            })
            .collect::<Vec<_>>();

        if intents.is_empty() {
            records.push(ExecutionRecord {
                symbol: "N/A".to_string(),
                status: "noop".to_string(),
                submission_mode: "analysis-only".to_string(),
                note: "no order intents passed all guardrails in this cycle".to_string(),
                legs: Vec::new(),
                fill_reconciliation: None,
                broker_event_log_path: None,
                broker_event_timeline: Vec::new(),
                execution_step_timings: Vec::new(),
            });
        }

        Ok(records)
    }
}

#[derive(Debug, Clone)]
enum BrokerOrderEvent {
    OpenOrder(Box<OrderData>),
    OrderStatus(Box<OrderStatus>),
    ExecutionData(Box<ExecutionData>),
    CommissionReport(Box<CommissionReport>),
    Message { code: i32, message: String },
}

#[derive(Debug, Clone)]
struct TimedBrokerOrderEvent {
    observed_at: DateTime<Utc>,
    elapsed_ms: i64,
    event: BrokerOrderEvent,
}

#[derive(Debug, Clone)]
struct SubmittedOrderOutcome {
    order_id: i32,
    is_combo: bool,
    events: Vec<TimedBrokerOrderEvent>,
    broker_event_log_path: Option<String>,
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

#[derive(Debug, Serialize)]
struct BrokerEventLogArtifact {
    symbol: String,
    account: String,
    order_id: i32,
    event_count: usize,
    final_status: Option<String>,
    execution_count: usize,
    total_reported_commission: f64,
    message_count: usize,
    rejection_messages: Vec<String>,
    timeline: Vec<BrokerEventTimelineEntry>,
}

impl LegFillSummary {
    fn is_fully_filled(&self, expected_quantity: i32) -> bool {
        (self.filled_quantity - expected_quantity as f64).abs() < 0.0001
    }
}

#[async_trait(?Send)]
trait BrokerOrderGateway {
    fn next_order_id(&self) -> i32;
    async fn cancel_order(&self, order_id: i32) -> Result<()>;

    async fn place_order_and_collect(
        &self,
        order_id: i32,
        contract: &Contract,
        order: &Order,
        minimum_collection_window: Duration,
        idle_timeout: Duration,
    ) -> Result<Vec<TimedBrokerOrderEvent>>;
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

    async fn cancel_order(&self, order_id: i32) -> Result<()> {
        let mut subscription = self
            .client
            .cancel_order(order_id, "")
            .await
            .with_context(|| format!("failed to request cancellation for IBKR order {order_id}"))?;
        let started = Instant::now();
        let collection_window = Duration::from_secs(5);
        let idle_timeout = Duration::from_secs(1);

        while started.elapsed() < collection_window {
            match timeout(idle_timeout, subscription.next()).await {
                Ok(Some(event)) => {
                    event.with_context(|| {
                        format!("failed while monitoring cancellation for IBKR order {order_id}")
                    })?;
                }
                Ok(None) | Err(_) => break,
            }
        }

        Ok(())
    }

    async fn place_order_and_collect(
        &self,
        order_id: i32,
        contract: &Contract,
        order: &Order,
        minimum_collection_window: Duration,
        idle_timeout: Duration,
    ) -> Result<Vec<TimedBrokerOrderEvent>> {
        let mut subscription = self
            .client
            .place_order(order_id, contract, order)
            .await
            .with_context(|| format!("failed to place IBKR order {order_id}"))?;
        let mut events = Vec::new();
        let started = Instant::now();

        loop {
            let next_item = timeout(idle_timeout, subscription.next()).await;
            let next_stream_item = match next_item {
                Ok(item) => item,
                Err(_) => {
                    if should_stop_collecting_broker_events(
                        &events,
                        started.elapsed(),
                        minimum_collection_window,
                    ) {
                        break;
                    }
                    continue;
                }
            };

            let Some(result) = next_stream_item else {
                break;
            };

            let event = match result
                .with_context(|| format!("failed while monitoring IBKR order {order_id}"))?
            {
                PlaceOrder::OpenOrder(order) => BrokerOrderEvent::OpenOrder(Box::new(order)),
                PlaceOrder::OrderStatus(status) => BrokerOrderEvent::OrderStatus(Box::new(status)),
                PlaceOrder::ExecutionData(data) => BrokerOrderEvent::ExecutionData(Box::new(data)),
                PlaceOrder::CommissionReport(report) => {
                    BrokerOrderEvent::CommissionReport(Box::new(report))
                }
                PlaceOrder::Message(notice) => BrokerOrderEvent::Message {
                    code: notice.code,
                    message: notice.message,
                },
            };

            events.push(TimedBrokerOrderEvent {
                observed_at: Utc::now(),
                elapsed_ms: started.elapsed().as_millis() as i64,
                event,
            });

            if should_stop_collecting_broker_events(
                &events,
                started.elapsed(),
                minimum_collection_window,
            ) {
                break;
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
    minimum_collection_window: Duration,
    idle_timeout: Duration,
}

impl GuardedPaperOrderExecutor {
    pub fn from_client(client: Arc<Client>) -> Self {
        Self {
            inner: GuardedPaperOrderExecutorInner {
                gateway: IbkrOrderGateway::new(client),
                minimum_collection_window: Duration::from_secs(3),
                idle_timeout: Duration::from_secs(2),
            },
        }
    }
}

impl<G> GuardedPaperOrderExecutorInner<G> {
    #[cfg(test)]
    fn new(gateway: G, idle_timeout: Duration) -> Self {
        Self {
            gateway,
            minimum_collection_window: Duration::from_secs(3),
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

        for (index, intent) in intents.iter().enumerate() {
            if config.logs.print_statements && paper_submission_enabled(config) {
                println!(
                    "PROGRESS: placing trade {}/{} for {}",
                    index + 1,
                    intents.len(),
                    intent.symbol
                );
            }

            if config.risk.enable_live_orders || matches!(config.mode, RuntimeMode::Live) {
                records.push(analysis_only_record(
                    intent,
                    config,
                    "live-order execution remains disabled; guarded paper routing only".to_string(),
                    "live-disabled",
                ));
                continue;
            }

            if !paper_submission_enabled(config) {
                records.push(analysis_only_record(
                    intent,
                    config,
                    analysis_only_note(config),
                    "analysis-only",
                ));
                continue;
            }

            records.push(self.submit_guarded_buy_write(intent, config).await?);
        }

        if intents.is_empty() {
            records.push(ExecutionRecord {
                symbol: "N/A".to_string(),
                status: "noop".to_string(),
                submission_mode: "analysis-only".to_string(),
                note: "no order intents passed all guardrails in this cycle".to_string(),
                legs: Vec::new(),
                fill_reconciliation: None,
                broker_event_log_path: None,
                broker_event_timeline: Vec::new(),
                execution_step_timings: Vec::new(),
            });
        }

        Ok(records)
    }
}

impl<G> GuardedPaperOrderExecutorInner<G>
where
    G: BrokerOrderGateway,
{
    async fn submit_guarded_buy_write(
        &self,
        intent: &OrderIntent,
        config: &AppConfig,
    ) -> Result<ExecutionRecord> {
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

        let (combo_submission, execution_step_timings) = self
            .submit_combo_buy_write(intent, stock_leg, option_leg, config)
            .await?;
        let stock_summary = summarize_leg_outcome(stock_leg, Some(&combo_submission));
        let stock_record = execution_leg_record(stock_leg, Some(&combo_submission), &stock_summary);
        let option_summary = summarize_leg_outcome(option_leg, Some(&combo_submission));
        let option_record =
            execution_leg_record(option_leg, Some(&combo_submission), &option_summary);
        let fill_reconciliation =
            reconcile_buy_write_fill(stock_leg, &stock_summary, option_leg, Some(&option_summary));

        Ok(ExecutionRecord {
            symbol: intent.symbol.clone(),
            status: fill_reconciliation.status.clone(),
            submission_mode: "paper".to_string(),
            note: "submitted guarded deep-ITM covered-call buy-write as one combo BAG order in paper mode"
                .to_string(),
            legs: vec![stock_record, option_record],
            fill_reconciliation: Some(fill_reconciliation),
            broker_event_log_path: combo_submission.broker_event_log_path.clone(),
            broker_event_timeline: broker_event_timeline(&combo_submission),
            execution_step_timings,
        })
    }

    async fn submit_combo_buy_write(
        &self,
        intent: &OrderIntent,
        stock_leg: &OrderLegIntent,
        option_leg: &OrderLegIntent,
        config: &AppConfig,
    ) -> Result<(SubmittedOrderOutcome, Vec<ExecutionStepTiming>)> {
        let mut step_timings = Vec::new();

        let contract_started = Instant::now();
        let contract = build_ibkr_combo_contract(intent, stock_leg, option_leg)?;
        push_step_timing(
            &mut step_timings,
            "build-combo-contract",
            contract_started,
            None,
            None,
            None,
        );

        let pricing_started = Instant::now();
        let max_limit_price = intent
            .combo_limit_price
            .with_context(|| format!("missing combo limit price for {}", intent.symbol))?;
        let initial_limit_price =
            derive_initial_combo_limit_price(intent, stock_leg, option_leg, max_limit_price);
        push_step_timing(
            &mut step_timings,
            "derive-combo-debit-pricing",
            pricing_started,
            None,
            None,
            Some(initial_limit_price),
        );

        let max_reprices = if config.execution.auto_reprice {
            config.execution.reprice_attempts
        } else {
            0
        };
        let reprice_wait = Duration::from_secs(config.execution.reprice_wait_seconds.max(1));
        let mut all_events = Vec::new();
        let mut final_order_id = None;
        let mut current_limit_price = initial_limit_price;

        for attempt in 0..=max_reprices {
            let order_started = Instant::now();
            let order = build_ibkr_combo_order_at_limit(
                intent,
                stock_leg,
                option_leg,
                current_limit_price,
            )?;
            push_step_timing(
                &mut step_timings,
                "build-combo-order",
                order_started,
                Some(attempt),
                None,
                Some(current_limit_price),
            );

            let order_id = self.gateway.next_order_id();
            final_order_id = Some(order_id);
            let collection_window = if attempt < max_reprices {
                self.minimum_collection_window.min(reprice_wait)
            } else {
                self.minimum_collection_window
            };

            let submit_started = Instant::now();
            let attempt_events = self
                .gateway
                .place_order_and_collect(
                    order_id,
                    &contract,
                    &order,
                    collection_window,
                    self.idle_timeout,
                )
                .await?;
            push_step_timing(
                &mut step_timings,
                "place-and-collect-combo-order",
                submit_started,
                Some(attempt),
                Some(order_id),
                Some(current_limit_price),
            );
            all_events.extend(attempt_events);

            if !should_auto_reprice_combo_order(&all_events, current_limit_price, max_limit_price)
                || attempt >= max_reprices
            {
                break;
            }

            let next_limit_price = next_reprice_limit_price(
                initial_limit_price,
                max_limit_price,
                attempt + 1,
                max_reprices,
            );
            if next_limit_price <= current_limit_price {
                break;
            }

            let cancel_started = Instant::now();
            self.gateway.cancel_order(order_id).await?;
            push_step_timing(
                &mut step_timings,
                "cancel-before-reprice",
                cancel_started,
                Some(attempt),
                Some(order_id),
                Some(current_limit_price),
            );
            current_limit_price = next_limit_price;
        }

        let order_id = final_order_id
            .with_context(|| format!("no IBKR order id was allocated for {}", intent.symbol))?;
        let broker_event_log_path = if config.logs.api_log {
            let persist_started = Instant::now();
            let path =
                persist_broker_event_log(&intent.symbol, &intent.account, order_id, &all_events)?;
            push_step_timing(
                &mut step_timings,
                "persist-broker-event-log",
                persist_started,
                None,
                Some(order_id),
                Some(current_limit_price),
            );
            Some(path.display().to_string())
        } else {
            None
        };

        Ok((
            SubmittedOrderOutcome {
                order_id,
                is_combo: true,
                events: all_events,
                broker_event_log_path,
            },
            step_timings,
        ))
    }
}

fn paper_submission_enabled(config: &AppConfig) -> bool {
    config.guarded_paper_submission_enabled()
}

fn analysis_only_note(config: &AppConfig) -> String {
    if config.risk.enable_live_orders {
        "live-order execution remains disabled in this milestone".to_string()
    } else if config.risk.enable_paper_orders
        && matches!(config.mode, RuntimeMode::Paper)
        && !config.read_only
    {
        "paper-order flag is enabled; combo BAG guarded submission will only run when a live IBKR client is attached"
            .to_string()
    } else {
        "proposed order only; broker submission is disabled for this run".to_string()
    }
}

fn analysis_only_record(
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
            "analysis-only".to_string()
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
        broker_event_log_path: None,
        broker_event_timeline: Vec::new(),
        execution_step_timings: Vec::new(),
    }
}

fn push_step_timing(
    timings: &mut Vec<ExecutionStepTiming>,
    step: &str,
    started: Instant,
    attempt: Option<usize>,
    order_id: Option<i32>,
    limit_price: Option<f64>,
) {
    timings.push(ExecutionStepTiming {
        step: step.to_string(),
        duration_ms: started.elapsed().as_millis() as i64,
        attempt,
        order_id,
        limit_price,
    });
}

fn broker_event_timeline(outcome: &SubmittedOrderOutcome) -> Vec<BrokerEventTimelineEntry> {
    outcome
        .events
        .iter()
        .map(|event| BrokerEventTimelineEntry {
            observed_at: event.observed_at,
            elapsed_ms: event.elapsed_ms,
            event_type: broker_event_type_label(&event.event).to_string(),
            detail: broker_event_detail(&event.event),
        })
        .collect()
}

fn broker_event_type_label(event: &BrokerOrderEvent) -> &'static str {
    match event {
        BrokerOrderEvent::OpenOrder(_) => "openOrder",
        BrokerOrderEvent::OrderStatus(_) => "orderStatus",
        BrokerOrderEvent::ExecutionData(_) => "executionData",
        BrokerOrderEvent::CommissionReport(_) => "commissionReport",
        BrokerOrderEvent::Message { .. } => "message",
    }
}

fn broker_event_detail(event: &BrokerOrderEvent) -> String {
    match event {
        BrokerOrderEvent::OpenOrder(order) => format!(
            "order_id={} status={} action={:?} order_type={:?} total_quantity={} limit_price={:?}",
            order.order_id,
            order.order_state.status,
            order.order.action,
            order.order.order_type,
            order.order.total_quantity,
            order.order.limit_price
        ),
        BrokerOrderEvent::OrderStatus(status) => format!(
            "status={} filled={} remaining={} avg_fill_price={} last_fill_price={} why_held={}",
            status.status,
            status.filled,
            status.remaining,
            status.average_fill_price,
            status.last_fill_price,
            status.why_held
        ),
        BrokerOrderEvent::ExecutionData(data) => format!(
            "execution_id={} contract_id={} shares={} cumulative_quantity={} price={}",
            data.execution.execution_id,
            data.contract.contract_id,
            data.execution.shares,
            data.execution.cumulative_quantity,
            data.execution.price
        ),
        BrokerOrderEvent::CommissionReport(report) => format!(
            "execution_id={} commission={} currency={} realized_pnl={:?}",
            report.execution_id, report.commission, report.currency, report.realized_pnl
        ),
        BrokerOrderEvent::Message { code, message } => format!("code={code} message={message}"),
    }
}

fn persist_broker_event_log(
    symbol: &str,
    account: &str,
    order_id: i32,
    events: &[TimedBrokerOrderEvent],
) -> Result<std::path::PathBuf> {
    let path = timestamped_log_path("api", "API", "json");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .context("failed to create logs directory for broker event logs")?;
    }
    let timeline = events
        .iter()
        .map(|event| BrokerEventTimelineEntry {
            observed_at: event.observed_at,
            elapsed_ms: event.elapsed_ms,
            event_type: broker_event_type_label(&event.event).to_string(),
            detail: broker_event_detail(&event.event),
        })
        .collect::<Vec<_>>();
    let artifact = BrokerEventLogArtifact {
        symbol: symbol.to_string(),
        account: account.to_string(),
        order_id,
        event_count: timeline.len(),
        final_status: latest_broker_status(events),
        execution_count: events
            .iter()
            .filter(|event| matches!(event.event, BrokerOrderEvent::ExecutionData(_)))
            .count(),
        total_reported_commission: events
            .iter()
            .filter_map(|event| match &event.event {
                BrokerOrderEvent::CommissionReport(report) => Some(report.commission),
                _ => None,
            })
            .sum(),
        message_count: events
            .iter()
            .filter(|event| matches!(event.event, BrokerOrderEvent::Message { .. }))
            .count(),
        rejection_messages: events
            .iter()
            .filter_map(|event| match &event.event {
                BrokerOrderEvent::Message { code, message } if (200..300).contains(code) => {
                    Some(format!("{code}: {message}"))
                }
                _ => None,
            })
            .collect(),
        timeline,
    };
    fs::write(&path, serde_json::to_string_pretty(&artifact)?).with_context(|| {
        format!(
            "failed to write IBKR broker event log to {}",
            path.display()
        )
    })?;
    Ok(path)
}

fn latest_broker_status(events: &[TimedBrokerOrderEvent]) -> Option<String> {
    events.iter().rev().find_map(|event| match &event.event {
        BrokerOrderEvent::OrderStatus(status) if !status.status.is_empty() => {
            Some(status.status.clone())
        }
        BrokerOrderEvent::OpenOrder(order) if !order.order_state.status.is_empty() => {
            Some(order.order_state.status.clone())
        }
        _ => None,
    })
}

fn should_stop_collecting_broker_events(
    events: &[TimedBrokerOrderEvent],
    elapsed: Duration,
    minimum_collection_window: Duration,
) -> bool {
    if broker_events_indicate_terminal_outcome(events) {
        return true;
    }

    !events.is_empty() && elapsed >= minimum_collection_window
}

fn broker_events_indicate_terminal_outcome(events: &[TimedBrokerOrderEvent]) -> bool {
    events.iter().any(|event| match &event.event {
        BrokerOrderEvent::Message { code, .. } => (200..300).contains(code),
        BrokerOrderEvent::OrderStatus(status) => is_terminal_order_status(&status.status),
        BrokerOrderEvent::OpenOrder(order) => is_terminal_order_status(&order.order_state.status),
        _ => false,
    })
}

fn is_terminal_order_status(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "filled" | "cancelled" | "apicancelled" | "inactive"
    )
}

fn build_ibkr_combo_contract(
    intent: &OrderIntent,
    stock_leg: &OrderLegIntent,
    option_leg: &OrderLegIntent,
) -> Result<Contract> {
    let stock_contract_id = stock_leg
        .contract_id
        .with_context(|| format!("missing stock contract id for {}", stock_leg.description))?;
    let option_contract_id = option_leg
        .contract_id
        .with_context(|| format!("missing option contract id for {}", option_leg.description))?;

    Ok(Contract {
        symbol: Symbol::from(intent.symbol.as_str()),
        security_type: SecurityType::Spread,
        exchange: Exchange::from("SMART"),
        currency: Currency::from(
            stock_leg
                .currency
                .as_deref()
                .or(option_leg.currency.as_deref())
                .unwrap_or("USD"),
        ),
        combo_legs: vec![
            ComboLeg {
                contract_id: stock_contract_id,
                ratio: 100,
                action: "BUY".to_string(),
                exchange: "SMART".to_string(),
                open_close: ComboLegOpenClose::Same,
                ..Default::default()
            },
            ComboLeg {
                contract_id: option_contract_id,
                ratio: 1,
                action: "SELL".to_string(),
                exchange: "SMART".to_string(),
                open_close: ComboLegOpenClose::Same,
                ..Default::default()
            },
        ],
        ..Default::default()
    })
}

#[cfg(test)]
fn build_ibkr_combo_order(
    intent: &OrderIntent,
    stock_leg: &OrderLegIntent,
    option_leg: &OrderLegIntent,
) -> Result<Order> {
    let limit_price = intent
        .combo_limit_price
        .with_context(|| format!("missing combo limit price for {}", intent.symbol))?;
    build_ibkr_combo_order_at_limit(intent, stock_leg, option_leg, limit_price)
}

fn build_ibkr_combo_order_at_limit(
    intent: &OrderIntent,
    stock_leg: &OrderLegIntent,
    option_leg: &OrderLegIntent,
    limit_price: f64,
) -> Result<Order> {
    if intent.lot_quantity < 1 {
        anyhow::bail!("combo lot quantity must be positive for {}", intent.symbol);
    }

    if option_leg.quantity != intent.lot_quantity || stock_leg.quantity != intent.lot_quantity * 100
    {
        anyhow::bail!(
            "combo ratio mismatch for {}: expected 100 stock shares per short call contract",
            intent.symbol
        );
    }

    let mut order = order_builder::combo_limit_order(
        Action::Buy,
        intent.lot_quantity as f64,
        limit_price,
        false,
    );
    order.account = intent.account.clone();
    order.order_type = "LMT".to_string();
    order.limit_price = Some(limit_price);
    order.tif = TimeInForce::Day;
    order.transmit = true;
    order.outside_rth = false;
    order.order_ref = format!("deepitm-buywrite:{}:combo:buywrite", intent.symbol);
    order.smart_combo_routing_params = vec![TagValue {
        tag: "NonGuaranteed".to_string(),
        value: "0".to_string(),
    }];
    Ok(order)
}

fn derive_initial_combo_limit_price(
    intent: &OrderIntent,
    stock_leg: &OrderLegIntent,
    option_leg: &OrderLegIntent,
    max_limit_price: f64,
) -> f64 {
    let derived_debit = stock_leg
        .limit_price
        .zip(option_leg.limit_price)
        .map(|(stock_price, option_price)| stock_price - option_price)
        .filter(|value| value.is_finite() && *value > 0.0);

    match derived_debit {
        Some(value) => round_to_cents(value).min(max_limit_price),
        None => intent.combo_limit_price.unwrap_or(max_limit_price),
    }
}

fn next_reprice_limit_price(
    initial_limit_price: f64,
    max_limit_price: f64,
    attempt_number: usize,
    max_reprices: usize,
) -> f64 {
    if max_reprices == 0 || max_limit_price <= initial_limit_price {
        return initial_limit_price;
    }

    let progress = attempt_number as f64 / max_reprices as f64;
    ceil_to_cents(initial_limit_price + ((max_limit_price - initial_limit_price) * progress))
        .min(max_limit_price)
}

fn should_auto_reprice_combo_order(
    events: &[TimedBrokerOrderEvent],
    current_limit_price: f64,
    max_limit_price: f64,
) -> bool {
    if current_limit_price >= max_limit_price {
        return false;
    }

    if broker_events_indicate_terminal_outcome(events) || broker_events_show_any_fill(events) {
        return false;
    }

    latest_broker_status(events)
        .map(|status| {
            matches!(
                status.trim().to_ascii_lowercase().as_str(),
                "submitted" | "presubmitted" | "pendingsubmit" | "pending submit"
            )
        })
        .unwrap_or(true)
}

fn broker_events_show_any_fill(events: &[TimedBrokerOrderEvent]) -> bool {
    events.iter().any(|event| match &event.event {
        BrokerOrderEvent::ExecutionData(data) => {
            data.execution.shares > 0.0 || data.execution.cumulative_quantity > 0.0
        }
        BrokerOrderEvent::OrderStatus(status) => status.filled > 0.0,
        _ => false,
    })
}

fn round_to_cents(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn ceil_to_cents(value: f64) -> f64 {
    (value * 100.0).ceil() / 100.0
}

fn summarize_leg_outcome(
    leg: &OrderLegIntent,
    outcome: Option<&SubmittedOrderOutcome>,
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
    let leg_contract_id = leg.contract_id;

    for timed_event in &outcome.events {
        match &timed_event.event {
            BrokerOrderEvent::OpenOrder(_) => {}
            BrokerOrderEvent::OrderStatus(order_status) => {
                if !order_status.status.is_empty() {
                    status = order_status.status.clone();
                }
                if !outcome.is_combo {
                    filled_quantity = filled_quantity.max(order_status.filled);
                }
                if !outcome.is_combo && order_status.average_fill_price > 0.0 {
                    last_average_fill_price = Some(order_status.average_fill_price);
                }
            }
            BrokerOrderEvent::ExecutionData(data) => {
                if leg_contract_id.is_some()
                    && data.contract.contract_id != leg_contract_id.unwrap()
                {
                    continue;
                }
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
            BrokerOrderEvent::CommissionReport(_) => {}
            BrokerOrderEvent::Message { code, message } => {
                notes.push(format!("{code}: {message}"));
                if (200..300).contains(code) {
                    status = "rejected".to_string();
                }
            }
        }
    }

    for timed_event in &outcome.events {
        let BrokerOrderEvent::CommissionReport(report) = &timed_event.event else {
            continue;
        };
        if execution_ids
            .iter()
            .any(|execution_id| execution_id == &report.execution_id)
        {
            total_commission += report.commission;
            saw_commission = true;
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
    outcome: Option<&SubmittedOrderOutcome>,
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

    let stock_filled = stock_summary.is_fully_filled(stock_leg.quantity);
    let option_filled = option_summary
        .map(|summary| summary.is_fully_filled(option_leg.quantity))
        .unwrap_or(false);
    let saw_partial_combo_fill = stock_filled_shares > 0.0 || short_call_filled_contracts > 0.0;
    let saw_rejection = stock_summary.status.eq_ignore_ascii_case("rejected")
        || option_summary
            .map(|summary| summary.status.eq_ignore_ascii_case("rejected"))
            .unwrap_or(false);

    let (status, note) = if saw_rejection {
        (
            "combo-rejected".to_string(),
            "the combo BAG order was rejected before the covered-call position could be opened"
                .to_string(),
        )
    } else if stock_filled && option_filled {
        (
            "deep-itm-covered-call-open".to_string(),
            "the combo BAG fill reconciles to a deep-ITM covered-call paper position".to_string(),
        )
    } else if saw_partial_combo_fill {
        (
            "combo-partial".to_string(),
            "the combo BAG order partially filled and still leaves temporary uncovered-share risk"
                .to_string(),
        )
    } else if option_summary.is_some() {
        (
            "combo-submitted".to_string(),
            "the combo BAG order has been submitted and is awaiting a synchronized fill"
                .to_string(),
        )
    } else {
        (
            "not-submitted".to_string(),
            "the combo BAG order has not been submitted".to_string(),
        )
    };

    FillReconciliationRecord {
        stock_filled_shares,
        stock_average_fill_price: stock_summary.average_fill_price,
        short_call_filled_contracts,
        short_call_average_fill_price: option_summary
            .and_then(|summary| summary.average_fill_price),
        total_commission,
        eligible_for_short_call: false,
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
        SubmittedOrderOutcome, TimedBrokerOrderEvent, build_ibkr_combo_contract,
        build_ibkr_combo_order, derive_initial_combo_limit_price, reconcile_buy_write_fill,
        summarize_leg_outcome,
    };
    use crate::{
        config::{
            AllocationConfig, AppConfig, BrokerPlatform, ExecutionTuningConfig, LogsConfig,
            MarketDataMode, PerformanceConfig, RiskConfig, RunMode, RuntimeMode, StrategyConfig,
        },
        models::{InstrumentType, OrderIntent, OrderLegIntent, TradeAction},
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use chrono::Utc;
    use ibapi::{
        orders::{CommissionReport, Execution, ExecutionData, Order, OrderStatus, TimeInForce},
        prelude::{Contract, SecurityType},
    };
    use tokio::time::Duration;

    struct MockGateway {
        next_order_ids: RefCell<VecDeque<i32>>,
        submissions: RefCell<Vec<(i32, Contract, Order)>>,
        cancellations: RefCell<Vec<i32>>,
        events: RefCell<VecDeque<Vec<BrokerOrderEvent>>>,
    }

    impl MockGateway {
        fn new(next_order_ids: Vec<i32>, events: Vec<Vec<BrokerOrderEvent>>) -> Self {
            Self {
                next_order_ids: RefCell::new(next_order_ids.into()),
                submissions: RefCell::new(Vec::new()),
                cancellations: RefCell::new(Vec::new()),
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

        async fn cancel_order(&self, order_id: i32) -> Result<()> {
            self.cancellations.borrow_mut().push(order_id);
            Ok(())
        }

        async fn place_order_and_collect(
            &self,
            order_id: i32,
            contract: &Contract,
            order: &Order,
            _minimum_collection_window: Duration,
            _idle_timeout: Duration,
        ) -> Result<Vec<TimedBrokerOrderEvent>> {
            self.submissions
                .borrow_mut()
                .push((order_id, contract.clone(), order.clone()));
            Ok(self
                .events
                .borrow_mut()
                .pop_front()
                .unwrap_or_default()
                .into_iter()
                .map(|event| TimedBrokerOrderEvent {
                    observed_at: Utc::now(),
                    elapsed_ms: 0,
                    event,
                })
                .collect())
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
            allocation: AllocationConfig::default(),
            performance: PerformanceConfig::default(),
            execution: ExecutionTuningConfig::default(),
            logs: LogsConfig {
                print_statements: false,
                ..LogsConfig::default()
            },
        }
    }

    fn buy_write_intent() -> OrderIntent {
        OrderIntent {
            symbol: "AAPL".to_string(),
            strategy: "deep-ITM covered-call buy-write".to_string(),
            account: "DU1234567".to_string(),
            mode: "paper-combo-bag".to_string(),
            lot_quantity: 1,
            combo_limit_price: Some(86.0),
            estimated_net_debit: 8_600.0,
            estimated_credit: 1_400.0,
            max_profit: 400.0,
            legs: vec![
                OrderLegIntent {
                    instrument_type: InstrumentType::Stock,
                    action: TradeAction::Buy,
                    contract_id: Some(265598),
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
                    contract_id: Some(900000001),
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
    fn builds_ibkr_combo_order_contracts_with_leg_metadata() {
        let intent = buy_write_intent();
        let combo_contract =
            build_ibkr_combo_contract(&intent, &intent.legs[0], &intent.legs[1]).unwrap();
        let combo_order =
            build_ibkr_combo_order(&intent, &intent.legs[0], &intent.legs[1]).unwrap();

        assert_eq!(combo_contract.symbol.to_string(), "AAPL");
        assert_eq!(combo_contract.security_type, SecurityType::Spread);
        assert_eq!(combo_contract.combo_legs.len(), 2);
        assert_eq!(combo_contract.combo_legs[0].ratio, 100);
        assert_eq!(combo_contract.combo_legs[0].action, "BUY");
        assert_eq!(combo_contract.combo_legs[1].ratio, 1);
        assert_eq!(combo_contract.combo_legs[1].action, "SELL");
        assert_eq!(combo_order.account, "DU1234567");
        assert_eq!(combo_order.total_quantity, 1.0);
        assert_eq!(combo_order.order_type, "LMT");
        assert_eq!(combo_order.limit_price, Some(86.0));
        assert_eq!(combo_order.tif, TimeInForce::Day);
        assert!(combo_order.transmit);
        assert!(!combo_order.outside_rth);
        assert_eq!(combo_order.smart_combo_routing_params.len(), 1);
        assert_eq!(
            combo_order.smart_combo_routing_params[0].tag,
            "NonGuaranteed"
        );
        assert_eq!(combo_order.smart_combo_routing_params[0].value, "0");
    }

    #[test]
    fn multi_lot_combo_order_uses_single_combo_quantity() {
        let mut intent = buy_write_intent();
        intent.lot_quantity = 3;
        intent.estimated_net_debit = 25_800.0;
        intent.estimated_credit = 4_200.0;
        intent.max_profit = 1_200.0;
        intent.legs[0].quantity = 300;
        intent.legs[0].description = "Buy 300 shares of AAPL".to_string();
        intent.legs[1].quantity = 3;
        intent.legs[1].description =
            "Sell 3 deep-ITM covered call contract(s) AAPL 20260515 90".to_string();

        let combo_order =
            build_ibkr_combo_order(&intent, &intent.legs[0], &intent.legs[1]).unwrap();

        assert_eq!(combo_order.total_quantity, 3.0);
    }

    #[tokio::test]
    async fn submits_combo_bag_once_and_tracks_both_legs_together() {
        let gateway = MockGateway::new(
            vec![10],
            vec![vec![BrokerOrderEvent::OrderStatus(Box::new(OrderStatus {
                order_id: 10,
                status: "Submitted".to_string(),
                filled: 0.0,
                remaining: 1.0,
                ..OrderStatus::default()
            }))]],
        );
        let executor = GuardedPaperOrderExecutorInner::new(gateway, Duration::from_millis(1));
        let records = executor
            .execute(&[buy_write_intent()], &paper_config())
            .await
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, "combo-submitted");
        assert_eq!(records[0].legs.len(), 2);
        assert_eq!(records[0].legs[0].order_id, Some(10));
        assert_eq!(records[0].legs[1].order_id, Some(10));
        assert_eq!(records[0].legs[0].submission_status, "Submitted");
        assert_eq!(records[0].legs[1].submission_status, "Submitted");
        assert!(
            records[0]
                .fill_reconciliation
                .as_ref()
                .is_some_and(|fill| !fill.eligible_for_short_call)
        );
        assert!(!records[0].execution_step_timings.is_empty());
    }

    #[tokio::test]
    async fn starts_at_derived_combo_debit_and_reprices_toward_cap() {
        let gateway = MockGateway::new(
            vec![10, 11, 12],
            vec![
                vec![BrokerOrderEvent::OrderStatus(Box::new(OrderStatus {
                    order_id: 10,
                    status: "Submitted".to_string(),
                    filled: 0.0,
                    remaining: 1.0,
                    ..OrderStatus::default()
                }))],
                vec![BrokerOrderEvent::OrderStatus(Box::new(OrderStatus {
                    order_id: 11,
                    status: "Submitted".to_string(),
                    filled: 0.0,
                    remaining: 1.0,
                    ..OrderStatus::default()
                }))],
                vec![BrokerOrderEvent::OrderStatus(Box::new(OrderStatus {
                    order_id: 12,
                    status: "Submitted".to_string(),
                    filled: 0.0,
                    remaining: 1.0,
                    ..OrderStatus::default()
                }))],
            ],
        );
        let mut config = paper_config();
        config.execution.reprice_attempts = 2;
        config.execution.reprice_wait_seconds = 1;

        let mut intent = buy_write_intent();
        intent.combo_limit_price = Some(86.3);

        let executor = GuardedPaperOrderExecutorInner::new(gateway, Duration::from_millis(1));
        let records = executor.execute(&[intent], &config).await.unwrap();
        let gateway = &executor.gateway;
        let submissions = gateway.submissions.borrow();
        let cancellations = gateway.cancellations.borrow();

        assert_eq!(records.len(), 1);
        assert_eq!(submissions.len(), 3);
        assert_eq!(submissions[0].2.limit_price, Some(86.0));
        assert_eq!(submissions[1].2.limit_price, Some(86.15));
        assert_eq!(submissions[2].2.limit_price, Some(86.3));
        assert_eq!(cancellations.as_slice(), &[10, 11]);
        assert!(
            records[0]
                .execution_step_timings
                .iter()
                .any(|timing| timing.step == "cancel-before-reprice")
        );
    }

    #[test]
    fn derives_initial_combo_debit_from_leg_prices_and_respects_cap() {
        let mut intent = buy_write_intent();
        intent.combo_limit_price = Some(85.95);

        let derived =
            derive_initial_combo_limit_price(&intent, &intent.legs[0], &intent.legs[1], 85.95);

        assert_eq!(derived, 85.95);
    }

    #[test]
    fn fill_reconciliation_scaffolding_tracks_combo_fills_per_leg() {
        let intent = buy_write_intent();
        let stock_summary = summarize_leg_outcome(
            &intent.legs[0],
            Some(&SubmittedOrderOutcome {
                order_id: 10,
                is_combo: true,
                events: vec![
                    TimedBrokerOrderEvent {
                        observed_at: Utc::now(),
                        elapsed_ms: 0,
                        event: BrokerOrderEvent::ExecutionData(Box::new(ExecutionData {
                            contract: Contract {
                                contract_id: intent.legs[0].contract_id.unwrap(),
                                ..Contract::default()
                            },
                            execution: Execution {
                                execution_id: "stock-fill".to_string(),
                                shares: 100.0,
                                cumulative_quantity: 100.0,
                                price: 100.25,
                                average_price: 100.25,
                                ..Execution::default()
                            },
                            ..ExecutionData::default()
                        })),
                    },
                    TimedBrokerOrderEvent {
                        observed_at: Utc::now(),
                        elapsed_ms: 1,
                        event: BrokerOrderEvent::CommissionReport(Box::new(CommissionReport {
                            execution_id: "stock-fill".to_string(),
                            commission: 1.25,
                            ..CommissionReport::default()
                        })),
                    },
                ],
                broker_event_log_path: None,
            }),
        );
        let option_summary = summarize_leg_outcome(
            &intent.legs[1],
            Some(&SubmittedOrderOutcome {
                order_id: 10,
                is_combo: true,
                events: vec![
                    TimedBrokerOrderEvent {
                        observed_at: Utc::now(),
                        elapsed_ms: 0,
                        event: BrokerOrderEvent::ExecutionData(Box::new(ExecutionData {
                            contract: Contract {
                                contract_id: intent.legs[1].contract_id.unwrap(),
                                ..Contract::default()
                            },
                            execution: Execution {
                                execution_id: "option-fill".to_string(),
                                shares: 1.0,
                                cumulative_quantity: 1.0,
                                price: 14.0,
                                average_price: 14.0,
                                ..Execution::default()
                            },
                            ..ExecutionData::default()
                        })),
                    },
                    TimedBrokerOrderEvent {
                        observed_at: Utc::now(),
                        elapsed_ms: 1,
                        event: BrokerOrderEvent::CommissionReport(Box::new(CommissionReport {
                            execution_id: "option-fill".to_string(),
                            commission: 0.65,
                            ..CommissionReport::default()
                        })),
                    },
                ],
                broker_event_log_path: None,
            }),
        );
        let reconciliation = reconcile_buy_write_fill(
            &intent.legs[0],
            &stock_summary,
            &intent.legs[1],
            Some(&option_summary),
        );

        assert_eq!(reconciliation.status, "deep-itm-covered-call-open");
        assert!(!reconciliation.eligible_for_short_call);
        assert_eq!(reconciliation.uncovered_shares, 0.0);
        assert_eq!(reconciliation.total_commission, Some(1.9));
    }
}
