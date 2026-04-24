use anyhow::Result;
use chrono::Utc;
use futures::{StreamExt, stream};
use std::{collections::BTreeSet, time::Instant};

use crate::{
    config::AppConfig,
    execution::OrderExecutor,
    ibkr::is_invalid_underlying_contract_error,
    market_data::{MarketDataProvider, load_universe},
    models::{
        AllocationSummary, CapitalSourceDetails, CycleReport, CycleThroughputCounters,
        CycleTimingMetrics, GuardrailRejection,
    },
    paper_state::PaperTradeLedger,
    state::build_order_intents,
    strategy::evaluate_buy_write_candidate,
};

#[derive(Debug, Clone)]
pub struct ScanPlan {
    pub symbols: Vec<String>,
    pub run_mode: &'static str,
    pub execution_mode: &'static str,
}

pub fn build_scan_plan(config: &AppConfig, symbols: &[String]) -> ScanPlan {
    let execution_mode = if config.read_only {
        "read-only"
    } else if config.risk.enable_live_orders
        || matches!(config.mode, crate::config::RuntimeMode::Live)
    {
        "live-disabled"
    } else if config.risk.enable_paper_orders {
        "paper-combo-bag"
    } else {
        "analysis-only"
    };

    let run_mode = match config.run_mode {
        crate::config::RunMode::Manual => "manual",
        crate::config::RunMode::Scheduled => "scheduled",
    };

    ScanPlan {
        symbols: symbols.to_vec(),
        run_mode,
        execution_mode,
    }
}

pub async fn run_scan_cycle<P, E>(
    provider: &P,
    executor: &E,
    config: &AppConfig,
) -> Result<CycleReport>
where
    P: MarketDataProvider,
    E: OrderExecutor,
{
    let started_at = Utc::now();
    let cycle_started = Instant::now();
    let universe = load_universe(config)?;
    let universe_symbols = universe
        .iter()
        .map(|record| record.symbol.clone())
        .collect::<Vec<_>>();
    let _plan = build_scan_plan(config, &universe_symbols);

    let account = provider.load_account_state().await?;
    let positions = provider.load_inventory().await?;
    let mut paper_trade_ledger = PaperTradeLedger::load(config)?;

    let mut guardrail_rejections = Vec::new();
    let mut candidates = Vec::new();
    let mut symbols_scanned = 0usize;
    let mut underlying_snapshots = 0usize;
    let mut option_quotes_considered = 0usize;
    let mut non_live_symbols = BTreeSet::new();
    let mut warnings = Vec::new();
    let mut action_log = vec![format!(
        "Loaded {} symbols for a {} scan in {} mode against {}.",
        universe.len(),
        match config.run_mode {
            crate::config::RunMode::Manual => "manual",
            crate::config::RunMode::Scheduled => "scheduled",
        },
        if config.read_only {
            "read-only"
        } else {
            "broker-connected"
        },
        config.platform.label()
    )];
    action_log.push(format!(
        "Account summary for {}: buying_power={:?}, available_funds={:?}, net_liquidation={:?}.",
        account.account, account.buying_power, account.available_funds, account.net_liquidation
    ));

    if !config.prefers_live_market_data() {
        warnings.push(format!(
            "Configured market data mode is {} instead of live; switch MARKET_DATA_MODE=live before relying on this scan for paper-trading decisions.",
            crate::ibkr::market_data_mode_label(config.market_data_mode)
        ));
    }
    if config.guarded_paper_submission_enabled() && account.available_funds.is_none() {
        warnings.push(
            "IBKR account summary did not return AVAILABLE_FUNDS for the configured paper account; guarded paper routing will stay blocked."
                .to_string(),
        );
    }

    let mut open_orders = Vec::new();
    if config.guarded_paper_submission_enabled() {
        let mut completed_orders;
        open_orders = provider.load_open_orders().await?;
        completed_orders = provider.load_completed_orders().await?;
        paper_trade_ledger.reconcile_with_broker_orders(
            &open_orders,
            &completed_orders,
            &mut action_log,
        );

        let stale_open_orders = open_orders
            .iter()
            .filter(|order| should_cancel_unfilled_strategy_order(order))
            .cloned()
            .collect::<Vec<_>>();
        for order in &stale_open_orders {
            provider.cancel_order(order.order_id).await?;
            action_log.push(format!(
                "{}: requested cancellation for unfilled strategy BAG order {} before new paper submissions.",
                order.symbol, order.order_id
            ));
        }

        if !stale_open_orders.is_empty() {
            open_orders = provider.load_open_orders().await?;
            completed_orders = provider.load_completed_orders().await?;
            paper_trade_ledger.reconcile_with_broker_orders(
                &open_orders,
                &completed_orders,
                &mut action_log,
            );
        }
    }

    let symbol_concurrency = config.performance.symbol_concurrency.max(1);
    let option_quote_concurrency = config
        .performance
        .option_quote_concurrency_per_symbol
        .max(1);
    provider.prepare_scan_cycle(config).await?;
    action_log.push(format!(
        "Prepared market-data mode {} once for this cycle with symbol_concurrency={} and option_quote_concurrency_per_symbol={}.",
        crate::ibkr::market_data_mode_label(config.market_data_mode),
        symbol_concurrency,
        option_quote_concurrency
    ));

    let market_data_started = Instant::now();
    let mut snapshot_results = stream::iter(universe.iter().cloned().enumerate().map(
        |(index, record)| async move {
            let snapshot = provider.fetch_symbol_snapshot(&record, config).await;
            (index, record, snapshot)
        },
    ))
    .buffer_unordered(symbol_concurrency)
    .collect::<Vec<_>>()
    .await;
    snapshot_results.sort_by_key(|(index, _, _)| *index);
    let market_data_elapsed_ms = market_data_started.elapsed().as_millis() as i64;

    for (_, record, snapshot_result) in snapshot_results {
        symbols_scanned += 1;
        let snapshot = match snapshot_result {
            Ok(snapshot) => snapshot,
            Err(error) if is_invalid_underlying_contract_error(&error) => {
                warnings.push(format!(
                    "{} could not be resolved to an IBKR stock contract and was skipped: {}",
                    record.symbol, error
                ));
                None
            }
            Err(error) => return Err(error),
        };
        let Some(snapshot) = snapshot else {
            guardrail_rejections.push(GuardrailRejection {
                symbol: record.symbol.clone(),
                stage: "market-data".to_string(),
                reason: "no usable market data snapshot returned".to_string(),
            });
            action_log.push(format!(
                "{}: no market-data snapshot was returned by IBKR.",
                record.symbol
            ));
            continue;
        };

        underlying_snapshots += 1;
        if snapshot.underlying.is_non_live() {
            non_live_symbols.insert(record.symbol.clone());
            warnings.push(format!(
                "{} underlying snapshot used non-live data ({})",
                record.symbol, snapshot.underlying.price_source
            ));
        }
        action_log.push(format!(
            "{}: underlying reference {:?} from {}.",
            record.symbol,
            snapshot.underlying.reference_price(),
            snapshot.underlying.price_source
        ));
        let Some(price) = snapshot.underlying.reference_price() else {
            let diagnostic_suffix = if snapshot.underlying.market_data_notices.is_empty() {
                String::new()
            } else {
                format!(
                    "; ibkr notices: {}",
                    snapshot.underlying.market_data_notices.join(" | ")
                )
            };
            guardrail_rejections.push(GuardrailRejection {
                symbol: record.symbol.clone(),
                stage: "market-data".to_string(),
                reason: format!("missing usable underlying price{diagnostic_suffix}"),
            });
            action_log.push(format!(
                "{}: rejected before option screening because no usable underlying price was available.",
                record.symbol
            ));
            continue;
        };

        if price < config.risk.min_underlying_price || price > config.risk.max_underlying_price {
            guardrail_rejections.push(GuardrailRejection {
                symbol: record.symbol.clone(),
                stage: "prefilter".to_string(),
                reason: format!(
                    "underlying price {:.2} is outside configured range {:.2}-{:.2}",
                    price, config.risk.min_underlying_price, config.risk.max_underlying_price
                ),
            });
            action_log.push(format!(
                "{}: rejected by underlying price filter at {:.2}.",
                record.symbol, price
            ));
            continue;
        }

        for option_quote in snapshot.option_quotes {
            option_quotes_considered += 1;
            if option_quote.is_non_live() {
                non_live_symbols.insert(record.symbol.clone());
                warnings.push(format!(
                    "{} {} {:.2}: option quote carried delayed/frozen diagnostics",
                    option_quote.symbol, option_quote.expiry, option_quote.strike
                ));
            }
            match evaluate_buy_write_candidate(
                &record,
                &snapshot.underlying,
                &option_quote,
                &config.strategy,
            ) {
                Ok(candidate) => {
                    action_log.push(format!(
                        "{}: accepted {} {:.2} expiring {} with annualized yield {:.2}%, expiration profit {:.2}/share, and score {:.4}.",
                        candidate.symbol,
                        candidate.right,
                        candidate.strike,
                        candidate.expiry,
                        candidate.annualized_yield_ratio * 100.0,
                        candidate.expiration_profit_per_share,
                        candidate.score
                    ));
                    candidates.push(candidate)
                }
                Err(mut rejection) => {
                    if rejection.reason.contains("missing usable option premium")
                        && !option_quote.diagnostics.is_empty()
                    {
                        rejection.reason = format!(
                            "{}; ibkr notices: {}",
                            rejection.reason,
                            option_quote.diagnostics.join(" | ")
                        );
                    }
                    action_log.push(format!(
                        "{}: rejected {} {:.2} expiring {} because {}.",
                        option_quote.symbol,
                        option_quote.right,
                        option_quote.strike,
                        option_quote.expiry,
                        rejection.reason
                    ));
                    guardrail_rejections.push(rejection);
                }
            }
        }
    }

    candidates.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let intent_build = build_order_intents(&account, &positions, &candidates, config);
    let CapitalSourceDetails {
        configured_source,
        preview,
        routed_orders,
    } = intent_build.capital_source_details;
    let AllocationSummary {
        candidate_symbols_considered,
        selected_symbols,
        total_lots,
        existing_exposure_cash,
        allocated_cash,
        remaining_cash,
    } = intent_build.allocation_summary;
    let mut proposed_orders = intent_build.intents;
    let mut open_positions = intent_build.open_positions;
    guardrail_rejections.extend(intent_build.rejections);
    paper_trade_ledger.reconcile_with_positions(&open_positions, &mut action_log);
    action_log.push(format!(
        "Capital allocation: configured_source={} | preview {}={:?} deployable {:.2} | routed {}={:?} deployable {:.2} | per-symbol distribution cap {:.2} | selected {} symbol(s) / {} lot(s) across {} collapsed candidate symbol(s) | existing exposure {:.2} | newly allocated {:.2} | remaining {:.2}.",
        configured_source,
        preview.source,
        preview.reported_amount,
        preview.deployable_cash,
        routed_orders.source,
        routed_orders.reported_amount,
        routed_orders.deployable_cash,
        if config.guarded_paper_submission_enabled() {
            routed_orders.max_cash_per_symbol
        } else {
            preview.max_cash_per_symbol
        },
        selected_symbols,
        total_lots,
        candidate_symbols_considered,
        existing_exposure_cash,
        allocated_cash,
        remaining_cash
    ));

    if config.guarded_paper_submission_enabled() {
        let blocked_symbols = proposed_orders
            .iter()
            .filter(|intent| non_live_symbols.contains(&intent.symbol))
            .map(|intent| intent.symbol.clone())
            .collect::<Vec<_>>();

        if !blocked_symbols.is_empty() {
            warnings.push(
                "Paper submission was blocked for symbols that relied on delayed/frozen market data."
                    .to_string(),
            );
        }

        proposed_orders.retain(|intent| {
            if non_live_symbols.contains(&intent.symbol) {
                guardrail_rejections.push(GuardrailRejection {
                    symbol: intent.symbol.clone(),
                    stage: "paper-safety".to_string(),
                    reason: "paper submission requires live market data for the underlying and candidate option quote"
                        .to_string(),
                });
                action_log.push(format!(
                    "{}: blocked before paper submission because delayed/frozen market data was observed in this cycle.",
                    intent.symbol
                ));
                false
            } else {
                true
            }
        });

        proposed_orders = paper_trade_ledger.reject_duplicate_intents(
            proposed_orders,
            &open_orders,
            &mut guardrail_rejections,
            &mut action_log,
        );
    }

    let execution_records = executor.execute(&proposed_orders, config).await?;
    paper_trade_ledger.record_execution_results(
        &execution_records,
        &proposed_orders,
        &mut action_log,
    );

    if config.guarded_paper_submission_enabled()
        && execution_records
            .iter()
            .any(|record| record.submission_mode == "paper" && record.symbol != "N/A")
    {
        let refreshed_open_orders = provider.load_open_orders().await?;
        let refreshed_completed_orders = provider.load_completed_orders().await?;
        paper_trade_ledger.reconcile_with_broker_orders(
            &refreshed_open_orders,
            &refreshed_completed_orders,
            &mut action_log,
        );
        let refreshed_positions = provider.load_inventory().await?;
        open_positions = crate::state::summarize_open_positions(&refreshed_positions);
        paper_trade_ledger.reconcile_with_positions(&open_positions, &mut action_log);
        action_log.push(
            "Refreshed IBKR positions after paper submissions to update hold-to-close lifecycle state."
                .to_string(),
        );
    }

    paper_trade_ledger.persist(config)?;
    let paper_trade_lifecycle = paper_trade_ledger.snapshot();
    for intent in &proposed_orders {
        action_log.push(format!(
            "{}: proposed {} for {} lot(s) with estimated net debit {:.2} and max profit {:.2}.",
            intent.symbol,
            intent.strategy,
            intent.lot_quantity,
            intent.estimated_net_debit,
            intent.max_profit
        ));
    }
    for execution in &execution_records {
        action_log.push(format!(
            "{}: execution record status={} mode={} note={}",
            execution.symbol, execution.status, execution.submission_mode, execution.note
        ));
    }
    let completed_at = Utc::now();
    let timing_metrics = CycleTimingMetrics {
        total_elapsed_ms: cycle_started.elapsed().as_millis() as i64,
        market_data_elapsed_ms,
    };
    let throughput_counters = CycleThroughputCounters {
        configured_symbol_concurrency: symbol_concurrency,
        configured_option_quote_concurrency_per_symbol: option_quote_concurrency,
        symbols_completed: symbols_scanned,
        underlying_snapshots_completed: underlying_snapshots,
        option_quotes_completed: option_quotes_considered,
        symbols_per_second: per_second(symbols_scanned, market_data_elapsed_ms),
        underlying_snapshots_per_second: per_second(underlying_snapshots, market_data_elapsed_ms),
        option_quotes_per_second: per_second(option_quotes_considered, market_data_elapsed_ms),
    };
    action_log.push(format!(
        "Cycle timing: total_elapsed_ms={}, market_data_elapsed_ms={}; throughput: symbols_per_second={:.2}, underlying_snapshots_per_second={:.2}, option_quotes_per_second={:.2}.",
        timing_metrics.total_elapsed_ms,
        timing_metrics.market_data_elapsed_ms,
        throughput_counters.symbols_per_second,
        throughput_counters.underlying_snapshots_per_second,
        throughput_counters.option_quotes_per_second
    ));

    Ok(CycleReport {
        started_at,
        completed_at,
        run_mode: format!("{:?}", config.run_mode),
        schedule: config.scan_schedule.clone(),
        market_data_mode: format!("{:?}", config.market_data_mode),
        account_state: account.clone(),
        universe_size: universe.len(),
        symbols_scanned,
        underlying_snapshots,
        option_quotes_considered,
        candidates_ranked: candidates.len(),
        accepted_candidates: candidates,
        guardrail_rejections,
        proposed_orders,
        execution_records,
        open_positions,
        paper_trade_lifecycle,
        live_data_requested: config.prefers_live_market_data(),
        non_live_symbols: non_live_symbols.into_iter().collect(),
        capital_source_details: CapitalSourceDetails {
            configured_source,
            preview,
            routed_orders,
        },
        allocation_summary: AllocationSummary {
            candidate_symbols_considered,
            selected_symbols,
            total_lots,
            existing_exposure_cash,
            allocated_cash,
            remaining_cash,
        },
        warnings,
        action_log,
        timing_metrics,
        throughput_counters,
        human_log_path: None,
        notes: vec![
            "Scanner currently uses short-lived snapshot-style requests instead of long-lived subscriptions to stay comfortably within IBKR market-data line limits."
                .to_string(),
            "IB Gateway remains the default broker platform; switch to TWS by setting IBKR_PLATFORM=tws and letting the platform-specific default port follow unless you intentionally override IBKR_PORT.".to_string(),
            if config.guarded_paper_submission_enabled() {
                "Deep-ITM covered-call buy-write execution submits a single combo BAG order in paper mode, persists idempotency state on disk, and tracks the combined fill as one routed order."
                    .to_string()
            } else if config.risk.enable_live_orders
                || matches!(config.mode, crate::config::RuntimeMode::Live)
            {
                "Live-order routing remains disabled; this cycle stayed on the guarded paper/analysis-only path."
                    .to_string()
            } else {
                "Deep-ITM covered-call buy-write execution remains in analysis-only mode until paper submission is explicitly enabled."
                    .to_string()
            },
            "No automated exit strategy is implemented in this milestone; tracked paper positions remain hold-to-close only until IBKR reports them closed."
                .to_string(),
        ],
    })
}

fn should_cancel_unfilled_strategy_order(order: &crate::models::BrokerOpenOrder) -> bool {
    order.security_type.eq_ignore_ascii_case("BAG")
        && order.action.eq_ignore_ascii_case("BUY")
        && order.order_ref.starts_with("deepitm-buywrite:")
        && order.remaining_quantity > 0.0
        && order.filled_quantity <= f64::EPSILON
}

fn per_second(count: usize, elapsed_ms: i64) -> f64 {
    if count == 0 {
        return 0.0;
    }

    let elapsed_seconds = (elapsed_ms.max(1) as f64) / 1000.0;
    (count as f64) / elapsed_seconds
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        path::PathBuf,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use anyhow::Result;
    use async_trait::async_trait;
    use ibapi::Error as IbkrError;

    use crate::{
        config::{
            AllocationConfig, AppConfig, BrokerPlatform, ExecutionTuningConfig, MarketDataMode,
            PerformanceConfig, RiskConfig, RunMode, RuntimeMode, StrategyConfig,
        },
        execution::OrderExecutor,
        market_data::{MarketDataProvider, SymbolMarketSnapshot},
        models::{
            AccountState, BrokerCompletedOrder, BrokerOpenOrder, ExecutionRecord,
            InventoryPosition, OptionQuoteSnapshot, UnderlyingSnapshot, UniverseRecord,
        },
        scanner::run_scan_cycle,
    };

    struct ReplayProvider {
        account: AccountState,
        positions: Vec<InventoryPosition>,
        open_orders: Mutex<Vec<BrokerOpenOrder>>,
        completed_orders: Mutex<Vec<BrokerCompletedOrder>>,
        cancelled_order_ids: Mutex<Vec<i32>>,
        symbols: HashMap<String, SymbolMarketSnapshot>,
        symbol_errors: HashMap<String, String>,
        delays_ms: HashMap<String, u64>,
        prepare_calls: AtomicUsize,
        active_requests: AtomicUsize,
        max_active_requests: AtomicUsize,
    }

    #[async_trait(?Send)]
    impl MarketDataProvider for ReplayProvider {
        async fn prepare_scan_cycle(&self, _config: &AppConfig) -> Result<()> {
            self.prepare_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn load_account_state(&self) -> Result<AccountState> {
            Ok(self.account.clone())
        }

        async fn load_inventory(&self) -> Result<Vec<InventoryPosition>> {
            Ok(self.positions.clone())
        }

        async fn load_open_orders(&self) -> Result<Vec<BrokerOpenOrder>> {
            Ok(self.open_orders.lock().unwrap().clone())
        }

        async fn load_completed_orders(&self) -> Result<Vec<BrokerCompletedOrder>> {
            Ok(self.completed_orders.lock().unwrap().clone())
        }

        async fn cancel_order(&self, order_id: i32) -> Result<()> {
            self.cancelled_order_ids.lock().unwrap().push(order_id);
            let cancelled = {
                let mut open_orders = self.open_orders.lock().unwrap();
                let cancelled = open_orders
                    .iter()
                    .find(|order| order.order_id == order_id)
                    .cloned();
                open_orders.retain(|order| order.order_id != order_id);
                cancelled
            };
            if let Some(order) = cancelled {
                self.completed_orders
                    .lock()
                    .unwrap()
                    .push(BrokerCompletedOrder {
                        account: order.account,
                        order_id: order.order_id,
                        client_id: order.client_id,
                        perm_id: order.perm_id,
                        symbol: order.symbol,
                        security_type: order.security_type,
                        action: order.action,
                        total_quantity: order.total_quantity,
                        order_type: order.order_type,
                        limit_price: order.limit_price,
                        status: "Cancelled".to_string(),
                        completed_status: "Cancelled".to_string(),
                        reject_reason: String::new(),
                        warning_text: "cancelled by replay provider".to_string(),
                        completed_time: "20260420 00:00:00 America/Denver".to_string(),
                    });
            }
            Ok(())
        }

        async fn fetch_symbol_snapshot(
            &self,
            record: &UniverseRecord,
            _config: &AppConfig,
        ) -> Result<Option<SymbolMarketSnapshot>> {
            let active = self.active_requests.fetch_add(1, Ordering::SeqCst) + 1;
            update_max(&self.max_active_requests, active);
            if let Some(delay_ms) = self.delays_ms.get(&record.symbol).copied() {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            if let Some(error_kind) = self.symbol_errors.get(&record.symbol) {
                self.active_requests.fetch_sub(1, Ordering::SeqCst);
                return match error_kind.as_str() {
                    "invalid-underlying" => Err(anyhow::Error::new(IbkrError::Message(
                        200,
                        "No security definition has been found for the request".to_string(),
                    ))
                    .context(format!(
                        "failed to resolve underlying contract details for {}",
                        record.symbol
                    ))),
                    other => Err(anyhow::anyhow!(
                        "unexpected replay provider symbol error kind: {other}"
                    )),
                };
            }
            let snapshot = self.symbols.get(&record.symbol).cloned();
            self.active_requests.fetch_sub(1, Ordering::SeqCst);
            Ok(snapshot)
        }
    }

    #[derive(Default)]
    struct RecordingExecutor {
        recorded: Mutex<Vec<String>>,
    }

    #[async_trait(?Send)]
    impl OrderExecutor for RecordingExecutor {
        async fn execute(
            &self,
            intents: &[crate::models::OrderIntent],
            _config: &AppConfig,
        ) -> Result<Vec<ExecutionRecord>> {
            let mut recorded = self.recorded.lock().unwrap();
            for intent in intents {
                recorded.push(intent.symbol.clone());
            }
            Ok(intents
                .iter()
                .map(|intent| ExecutionRecord {
                    symbol: intent.symbol.clone(),
                    status: "analysis-only".to_string(),
                    submission_mode: "analysis-only".to_string(),
                    note: "recorded in test executor".to_string(),
                    legs: Vec::new(),
                    fill_reconciliation: None,
                    broker_event_log_path: None,
                    broker_event_timeline: Vec::new(),
                    execution_step_timings: Vec::new(),
                })
                .collect())
        }
    }

    fn test_config() -> AppConfig {
        AppConfig {
            host: "127.0.0.1".to_string(),
            platform: BrokerPlatform::Gateway,
            port: 4002,
            client_id: 100,
            account: "DU123".to_string(),
            mode: RuntimeMode::Paper,
            read_only: true,
            connect_on_start: false,
            run_mode: RunMode::Scheduled,
            scan_schedule: "0 45 9,12,15 * * MON-FRI".to_string(),
            market_data_mode: MarketDataMode::DelayedFrozen,
            universe_file: None,
            symbols: vec!["AAPL".to_string()],
            startup_warnings: Vec::new(),
            strategy: StrategyConfig {
                expiration_dates: vec!["20991217".to_string()],
                min_annualized_yield_ratio: 0.0001,
                min_itm_depth_ratio: 0.01,
                min_downside_buffer_ratio: 0.01,
                ..StrategyConfig::default()
            },
            risk: RiskConfig {
                min_underlying_price: 1.0,
                max_underlying_price: 250.0,
                ..RiskConfig::default()
            },
            allocation: AllocationConfig {
                max_cash_per_symbol_ratio: 1.0,
                min_cash_reserve_ratio: 0.0,
                ..AllocationConfig::default()
            },
            performance: PerformanceConfig::default(),
            execution: ExecutionTuningConfig::default(),
        }
    }

    fn test_account_state() -> AccountState {
        AccountState {
            account: "DU123".to_string(),
            available_funds: Some(20_000.0),
            buying_power: Some(20_000.0),
            net_liquidation: Some(30_000.0),
        }
    }

    fn replay_provider(symbols: HashMap<String, SymbolMarketSnapshot>) -> ReplayProvider {
        ReplayProvider {
            account: test_account_state(),
            positions: Vec::new(),
            open_orders: Mutex::new(Vec::new()),
            completed_orders: Mutex::new(Vec::new()),
            cancelled_order_ids: Mutex::new(Vec::new()),
            symbols,
            symbol_errors: HashMap::new(),
            delays_ms: HashMap::new(),
            prepare_calls: AtomicUsize::new(0),
            active_requests: AtomicUsize::new(0),
            max_active_requests: AtomicUsize::new(0),
        }
    }

    fn update_max(target: &AtomicUsize, value: usize) {
        loop {
            let observed = target.load(Ordering::SeqCst);
            if value <= observed {
                break;
            }
            if target
                .compare_exchange(observed, value, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
    }

    fn snapshot_for(symbol: &str, price: f64, strike: f64) -> SymbolMarketSnapshot {
        SymbolMarketSnapshot {
            underlying: UnderlyingSnapshot {
                contract_id: 1,
                symbol: symbol.to_string(),
                price,
                bid: Some(price - 0.1),
                ask: Some(price + 0.1),
                last: Some(price),
                close: Some(price - 0.5),
                implied_volatility: None,
                beta: Some(1.1),
                price_source: "realtime-or-frozen".to_string(),
                market_data_notices: Vec::new(),
            },
            option_quotes: vec![OptionQuoteSnapshot {
                contract_id: 2,
                symbol: symbol.to_string(),
                expiry: "20991217".to_string(),
                strike,
                right: "C".to_string(),
                exchange: "SMART".to_string(),
                trading_class: symbol.to_string(),
                multiplier: "100".to_string(),
                bid: Some(14.00),
                ask: Some(14.30),
                last: Some(14.10),
                close: Some(13.80),
                option_price: Some(14.10),
                implied_volatility: Some(0.2),
                delta: Some(0.8),
                underlying_price: Some(price),
                quote_source: Some("test".to_string()),
                diagnostics: Vec::new(),
            }],
        }
    }

    fn test_ledger_path(account: &str) -> std::path::PathBuf {
        test_ledger_dir().join(format!("paper-trade-state-{account}.json"))
    }

    fn test_ledger_dir() -> PathBuf {
        std::env::temp_dir()
            .join("ibkr-options-engine-tests")
            .join("paper-state")
    }

    fn set_test_ledger_dir() {
        let dir = test_ledger_dir();
        std::fs::create_dir_all(&dir).unwrap();
        unsafe {
            std::env::set_var("IBKR_PAPER_STATE_DIR", &dir);
        }
    }

    fn clear_test_ledger_dir(account: &str) {
        let ledger_path = test_ledger_path(account);
        let _ = std::fs::remove_file(&ledger_path);
    }

    #[tokio::test]
    async fn builds_one_ranked_candidate_and_order_intent() {
        set_test_ledger_dir();
        clear_test_ledger_dir("DU123");

        let mut symbols = HashMap::new();
        symbols.insert(
            "AAPL".to_string(),
            SymbolMarketSnapshot {
                underlying: UnderlyingSnapshot {
                    contract_id: 1,
                    symbol: "AAPL".to_string(),
                    price: 100.0,
                    bid: Some(99.9),
                    ask: Some(100.1),
                    last: Some(100.0),
                    close: Some(99.5),
                    implied_volatility: None,
                    beta: Some(1.1),
                    price_source: "realtime-or-frozen".to_string(),
                    market_data_notices: Vec::new(),
                },
                option_quotes: vec![OptionQuoteSnapshot {
                    contract_id: 2,
                    symbol: "AAPL".to_string(),
                    expiry: "20991217".to_string(),
                    strike: 90.0,
                    right: "C".to_string(),
                    exchange: "SMART".to_string(),
                    trading_class: "AAPL".to_string(),
                    multiplier: "100".to_string(),
                    bid: Some(14.00),
                    ask: Some(14.30),
                    last: Some(14.10),
                    close: Some(13.80),
                    option_price: Some(14.10),
                    implied_volatility: Some(0.2),
                    delta: Some(0.8),
                    underlying_price: Some(100.0),
                    quote_source: Some("test".to_string()),
                    diagnostics: Vec::new(),
                }],
            },
        );

        let report = run_scan_cycle(
            &replay_provider(symbols),
            &RecordingExecutor::default(),
            &test_config(),
        )
        .await
        .unwrap();

        assert_eq!(report.candidates_ranked, 1);
        assert_eq!(report.proposed_orders.len(), 1);
        assert!(report.paper_trade_lifecycle.is_empty());

        clear_test_ledger_dir("DU123");
    }

    #[tokio::test]
    async fn preserves_symbol_order_and_populates_throughput_metrics_under_concurrency() {
        set_test_ledger_dir();
        clear_test_ledger_dir("DU123");

        let mut config = test_config();
        config.symbols = vec!["AAPL".to_string(), "MSFT".to_string(), "NVDA".to_string()];
        config.performance.symbol_concurrency = 2;
        config.performance.option_quote_concurrency_per_symbol = 2;

        let mut symbols = HashMap::new();
        symbols.insert("AAPL".to_string(), snapshot_for("AAPL", 100.0, 90.0));
        symbols.insert("MSFT".to_string(), snapshot_for("MSFT", 101.0, 91.0));
        symbols.insert("NVDA".to_string(), snapshot_for("NVDA", 102.0, 92.0));

        let mut provider = replay_provider(symbols);
        provider.delays_ms.insert("AAPL".to_string(), 40);
        provider.delays_ms.insert("MSFT".to_string(), 5);
        provider.delays_ms.insert("NVDA".to_string(), 15);

        let report = run_scan_cycle(&provider, &RecordingExecutor::default(), &config)
            .await
            .unwrap();

        let aapl_log = report
            .action_log
            .iter()
            .position(|entry| entry.contains("AAPL: underlying reference"))
            .unwrap();
        let msft_log = report
            .action_log
            .iter()
            .position(|entry| entry.contains("MSFT: underlying reference"))
            .unwrap();
        let nvda_log = report
            .action_log
            .iter()
            .position(|entry| entry.contains("NVDA: underlying reference"))
            .unwrap();

        assert!(aapl_log < msft_log);
        assert!(msft_log < nvda_log);
        assert_eq!(provider.prepare_calls.load(Ordering::SeqCst), 1);
        assert!(provider.max_active_requests.load(Ordering::SeqCst) >= 2);
        assert_eq!(report.symbols_scanned, 3);
        assert_eq!(report.underlying_snapshots, 3);
        assert_eq!(report.option_quotes_considered, 3);
        assert!(
            report.timing_metrics.total_elapsed_ms >= report.timing_metrics.market_data_elapsed_ms
        );
        assert!(report.timing_metrics.market_data_elapsed_ms > 0);
        assert_eq!(report.throughput_counters.configured_symbol_concurrency, 2);
        assert_eq!(
            report
                .throughput_counters
                .configured_option_quote_concurrency_per_symbol,
            2
        );
        assert_eq!(report.throughput_counters.symbols_completed, 3);
        assert_eq!(report.throughput_counters.underlying_snapshots_completed, 3);
        assert_eq!(report.throughput_counters.option_quotes_completed, 3);
        assert!(report.throughput_counters.symbols_per_second > 0.0);
        assert!(report.throughput_counters.option_quotes_per_second > 0.0);

        clear_test_ledger_dir("DU123");
    }

    #[tokio::test]
    async fn skips_invalid_underlying_symbols_without_aborting_scan_cycle() {
        set_test_ledger_dir();
        clear_test_ledger_dir("DU123");

        let mut config = test_config();
        config.symbols = vec!["AAPL".to_string(), "000430".to_string(), "MSFT".to_string()];

        let mut symbols = HashMap::new();
        symbols.insert("AAPL".to_string(), snapshot_for("AAPL", 100.0, 90.0));
        symbols.insert("MSFT".to_string(), snapshot_for("MSFT", 101.0, 91.0));

        let mut provider = replay_provider(symbols);
        provider
            .symbol_errors
            .insert("000430".to_string(), "invalid-underlying".to_string());

        let report = run_scan_cycle(&provider, &RecordingExecutor::default(), &config)
            .await
            .unwrap();

        assert_eq!(report.symbols_scanned, 3);
        assert_eq!(report.underlying_snapshots, 2);
        assert_eq!(report.candidates_ranked, 2);
        assert!(report.guardrail_rejections.iter().any(|rejection| {
            rejection.symbol == "000430"
                && rejection.stage == "market-data"
                && rejection.reason == "no usable market data snapshot returned"
        }));
        assert!(
            report.action_log.iter().any(
                |entry| entry.contains("000430: no market-data snapshot was returned by IBKR.")
            )
        );

        clear_test_ledger_dir("DU123");
    }

    #[tokio::test]
    async fn blocks_paper_submission_when_candidate_uses_non_live_data() {
        set_test_ledger_dir();
        clear_test_ledger_dir("DU123");

        let mut config = test_config();
        config.read_only = false;
        config.market_data_mode = MarketDataMode::Live;
        config.risk.enable_paper_orders = true;

        let mut symbols = HashMap::new();
        symbols.insert(
            "AAPL".to_string(),
            SymbolMarketSnapshot {
                underlying: UnderlyingSnapshot {
                    contract_id: 1,
                    symbol: "AAPL".to_string(),
                    price: 100.0,
                    bid: Some(99.9),
                    ask: Some(100.1),
                    last: Some(100.0),
                    close: Some(99.5),
                    implied_volatility: None,
                    beta: Some(1.1),
                    price_source: "delayed".to_string(),
                    market_data_notices: vec![
                        "10089: Delayed market data is available.".to_string(),
                    ],
                },
                option_quotes: vec![OptionQuoteSnapshot {
                    contract_id: 2,
                    symbol: "AAPL".to_string(),
                    expiry: "20991217".to_string(),
                    strike: 90.0,
                    right: "C".to_string(),
                    exchange: "SMART".to_string(),
                    trading_class: "AAPL".to_string(),
                    multiplier: "100".to_string(),
                    bid: Some(14.00),
                    ask: Some(14.30),
                    last: Some(14.10),
                    close: Some(13.80),
                    option_price: Some(14.10),
                    implied_volatility: Some(0.2),
                    delta: Some(0.8),
                    underlying_price: Some(100.0),
                    quote_source: Some("test".to_string()),
                    diagnostics: vec![
                        "observed data origin: delayed-or-delayed-frozen".to_string(),
                    ],
                }],
            },
        );

        let executor = RecordingExecutor::default();
        let report = run_scan_cycle(&replay_provider(symbols), &executor, &config)
            .await
            .unwrap();

        assert_eq!(report.candidates_ranked, 1);
        assert!(report.proposed_orders.is_empty());
        assert!(report.guardrail_rejections.iter().any(|rejection| {
            rejection.symbol == "AAPL"
                && rejection.stage == "paper-safety"
                && rejection
                    .reason
                    .contains("paper submission requires live market data")
        }));
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("Paper submission was blocked"))
        );
        assert_eq!(executor.recorded.lock().unwrap().len(), 0);

        clear_test_ledger_dir("DU123");
    }

    #[tokio::test]
    async fn blocks_duplicate_paper_intent_from_persisted_state() {
        set_test_ledger_dir();
        let mut config = test_config();
        config.account = "DU123-IDEMPOTENT".to_string();
        config.read_only = false;
        config.market_data_mode = MarketDataMode::Live;
        config.risk.enable_paper_orders = true;

        let ledger_path = test_ledger_path(&config.account);
        clear_test_ledger_dir(&config.account);
        std::fs::write(
            &ledger_path,
            serde_json::json!({
                "entries": [{
                    "symbol": "AAPL",
                    "intent_key": "AAPL|deep-ITM covered-call buy-write|seed",
                    "status": "stock-pending",
                    "first_recorded_at": "2026-04-16T00:00:00Z",
                    "last_updated_at": "2026-04-16T00:00:00Z",
                    "hold_until_close": true,
                    "stock_order_id": 10,
                    "short_call_order_id": null,
                    "stock_filled_shares": 0.0,
                    "short_call_filled_contracts": 0.0,
                    "stock_average_fill_price": null,
                    "short_call_average_fill_price": null,
                    "observed_stock_shares": 0.0,
                    "observed_short_call_contracts": 0.0,
                    "note": "seeded"
                }]
            })
            .to_string(),
        )
        .unwrap();

        let mut symbols = HashMap::new();
        symbols.insert(
            "AAPL".to_string(),
            SymbolMarketSnapshot {
                underlying: UnderlyingSnapshot {
                    contract_id: 1,
                    symbol: "AAPL".to_string(),
                    price: 100.0,
                    bid: Some(99.9),
                    ask: Some(100.1),
                    last: Some(100.0),
                    close: Some(99.5),
                    implied_volatility: None,
                    beta: Some(1.1),
                    price_source: "realtime-or-frozen".to_string(),
                    market_data_notices: Vec::new(),
                },
                option_quotes: vec![OptionQuoteSnapshot {
                    contract_id: 2,
                    symbol: "AAPL".to_string(),
                    expiry: "20991217".to_string(),
                    strike: 90.0,
                    right: "C".to_string(),
                    exchange: "SMART".to_string(),
                    trading_class: "AAPL".to_string(),
                    multiplier: "100".to_string(),
                    bid: Some(14.00),
                    ask: Some(14.30),
                    last: Some(14.10),
                    close: Some(13.80),
                    option_price: Some(14.10),
                    implied_volatility: Some(0.2),
                    delta: Some(0.8),
                    underlying_price: Some(100.0),
                    quote_source: Some("test".to_string()),
                    diagnostics: Vec::new(),
                }],
            },
        );

        let executor = RecordingExecutor::default();
        let provider = ReplayProvider {
            positions: Vec::new(),
            open_orders: Mutex::new(vec![BrokerOpenOrder {
                account: "DU123".to_string(),
                order_id: 10,
                client_id: 100,
                perm_id: 99,
                order_ref: "deepitm-buywrite:AAPL:combo:buywrite".to_string(),
                symbol: "AAPL".to_string(),
                security_type: "BAG".to_string(),
                action: "BUY".to_string(),
                total_quantity: 1.0,
                order_type: "LMT".to_string(),
                limit_price: Some(86.08),
                status: "PreSubmitted".to_string(),
                filled_quantity: 0.0,
                remaining_quantity: 1.0,
            }]),
            ..replay_provider(symbols)
        };
        let report = run_scan_cycle(&provider, &executor, &config).await.unwrap();

        assert_eq!(report.proposed_orders.len(), 1);
        assert_eq!(executor.recorded.lock().unwrap().as_slice(), ["AAPL"]);
        assert_eq!(
            provider.cancelled_order_ids.lock().unwrap().as_slice(),
            [10]
        );

        std::fs::remove_file(ledger_path).unwrap();
    }
}
