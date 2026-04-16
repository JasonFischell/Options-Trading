use anyhow::Result;
use chrono::Utc;

use crate::{
    config::AppConfig,
    execution::OrderExecutor,
    market_data::{MarketDataProvider, load_universe},
    models::{CycleReport, GuardrailRejection},
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
        "paper-stock-first"
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
    let universe = load_universe(config)?;
    let universe_symbols = universe
        .iter()
        .map(|record| record.symbol.clone())
        .collect::<Vec<_>>();
    let _plan = build_scan_plan(config, &universe_symbols);

    let account = provider.load_account_state().await?;
    let positions = provider.load_inventory().await?;

    let mut guardrail_rejections = Vec::new();
    let mut candidates = Vec::new();
    let mut symbols_scanned = 0usize;
    let mut underlying_snapshots = 0usize;
    let mut option_quotes_considered = 0usize;

    for record in &universe {
        symbols_scanned += 1;
        let Some(snapshot) = provider.fetch_symbol_snapshot(record, config).await? else {
            guardrail_rejections.push(GuardrailRejection {
                symbol: record.symbol.clone(),
                stage: "market-data".to_string(),
                reason: "no usable market data snapshot returned".to_string(),
            });
            continue;
        };

        underlying_snapshots += 1;
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
            continue;
        }

        for option_quote in snapshot.option_quotes {
            option_quotes_considered += 1;
            match evaluate_buy_write_candidate(
                record,
                &snapshot.underlying,
                &option_quote,
                &config.strategy,
            ) {
                Ok(candidate) => candidates.push(candidate),
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

    let (proposed_orders, risk_rejections, open_positions) =
        build_order_intents(&account, &positions, &candidates, config);
    guardrail_rejections.extend(risk_rejections);

    let execution_records = executor.execute(&proposed_orders, config).await?;
    let completed_at = Utc::now();

    Ok(CycleReport {
        started_at,
        completed_at,
        run_mode: format!("{:?}", config.run_mode),
        schedule: config.scan_schedule.clone(),
        market_data_mode: format!("{:?}", config.market_data_mode),
        universe_size: universe.len(),
        symbols_scanned,
        underlying_snapshots,
        option_quotes_considered,
        candidates_ranked: candidates.len(),
        guardrail_rejections,
        proposed_orders,
        execution_records,
        open_positions,
        notes: vec![
            "Scanner currently prefers delayed/frozen snapshots over long-lived subscriptions."
                .to_string(),
            if config.risk.enable_paper_orders
                && matches!(config.mode, crate::config::RuntimeMode::Paper)
                && !config.read_only
                && !config.risk.enable_live_orders
            {
                "Buy-write execution submits the stock leg first in paper mode and only advances the short call after fill reconciliation."
                    .to_string()
            } else if config.risk.enable_live_orders
                || matches!(config.mode, crate::config::RuntimeMode::Live)
            {
                "Live-order routing remains disabled; this cycle stayed on the guarded paper/dry-run path."
                    .to_string()
            } else {
                "Buy-write execution remains on the guarded dry-run path until paper submission is explicitly enabled."
                    .to_string()
            },
        ],
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex};

    use anyhow::Result;
    use async_trait::async_trait;

    use crate::{
        config::{
            AppConfig, BrokerPlatform, MarketDataMode, RiskConfig, RunMode, RuntimeMode,
            StrategyConfig,
        },
        execution::OrderExecutor,
        market_data::{MarketDataProvider, SymbolMarketSnapshot},
        models::{
            AccountState, ExecutionRecord, InventoryPosition, OptionQuoteSnapshot,
            UnderlyingSnapshot, UniverseRecord,
        },
        scanner::run_scan_cycle,
    };

    struct ReplayProvider {
        account: AccountState,
        positions: Vec<InventoryPosition>,
        symbols: HashMap<String, SymbolMarketSnapshot>,
    }

    #[async_trait(?Send)]
    impl MarketDataProvider for ReplayProvider {
        async fn load_account_state(&self) -> Result<AccountState> {
            Ok(self.account.clone())
        }

        async fn load_inventory(&self) -> Result<Vec<InventoryPosition>> {
            Ok(self.positions.clone())
        }

        async fn fetch_symbol_snapshot(
            &self,
            record: &UniverseRecord,
            _config: &AppConfig,
        ) -> Result<Option<SymbolMarketSnapshot>> {
            Ok(self.symbols.get(&record.symbol).cloned())
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
                    status: "dry-run".to_string(),
                    submission_mode: "dry-run".to_string(),
                    note: "recorded in test executor".to_string(),
                    legs: Vec::new(),
                    fill_reconciliation: None,
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
            strategy: StrategyConfig {
                min_expiry_days: 1,
                max_expiry_days: 36500,
                min_annualized_yield_pct: 0.01,
                ..StrategyConfig::default()
            },
            risk: RiskConfig {
                min_underlying_price: 1.0,
                max_underlying_price: 250.0,
                ..RiskConfig::default()
            },
        }
    }

    #[tokio::test]
    async fn builds_one_ranked_candidate_and_order_intent() {
        let mut symbols = HashMap::new();
        symbols.insert(
            "AAPL".to_string(),
            SymbolMarketSnapshot {
                underlying: UnderlyingSnapshot {
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
                    symbol: "AAPL".to_string(),
                    expiry: "20991217".to_string(),
                    strike: 103.0,
                    right: "C".to_string(),
                    exchange: "SMART".to_string(),
                    trading_class: "AAPL".to_string(),
                    multiplier: "100".to_string(),
                    bid: Some(1.60),
                    ask: Some(1.70),
                    last: Some(1.65),
                    close: Some(1.55),
                    option_price: Some(1.65),
                    implied_volatility: Some(0.2),
                    delta: Some(0.25),
                    underlying_price: Some(100.0),
                    quote_source: Some("test".to_string()),
                    diagnostics: Vec::new(),
                }],
            },
        );

        let report = run_scan_cycle(
            &ReplayProvider {
                account: AccountState {
                    account: "DU123".to_string(),
                    available_funds: Some(20_000.0),
                    buying_power: Some(20_000.0),
                    net_liquidation: Some(30_000.0),
                },
                positions: Vec::new(),
                symbols,
            },
            &RecordingExecutor::default(),
            &test_config(),
        )
        .await
        .unwrap();

        assert_eq!(report.candidates_ranked, 1);
        assert_eq!(report.proposed_orders.len(), 1);
    }
}
