use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::Local;

use crate::{
    config::AppConfig,
    models::{
        CycleReport, ExecutionLegRecord, ExecutionRecord, FillReconciliationRecord, OrderIntent,
        PaperTradeLifecycleRecord, ScoredOptionCandidate,
    },
};

pub fn write_cycle_outputs(config: &AppConfig, report: &CycleReport) -> Result<(PathBuf, PathBuf)> {
    let logs_dir = Path::new("logs");
    fs::create_dir_all(logs_dir).context("failed to create logs directory")?;

    let timestamp = Local::now().format("%Y%m%d-%H%M%S").to_string();
    let base_name = format!(
        "scan-{}-{}-{}",
        timestamp,
        config
            .platform
            .label()
            .to_ascii_lowercase()
            .replace(' ', "-"),
        config.account
    );

    let human_log_path = logs_dir.join(format!("{base_name}.log"));
    let json_report_path = logs_dir.join(format!("{base_name}.json"));

    fs::write(&human_log_path, render_human_log(config, report))
        .with_context(|| format!("failed to write human log to {}", human_log_path.display()))?;
    fs::write(&json_report_path, serde_json::to_string_pretty(report)?).with_context(|| {
        format!(
            "failed to write JSON report to {}",
            json_report_path.display()
        )
    })?;

    Ok((human_log_path, json_report_path))
}

fn render_human_log(config: &AppConfig, report: &CycleReport) -> String {
    let mut lines = vec![
        "IBKR Deep-ITM Scanner Run".to_string(),
        format!("Started: {}", report.started_at),
        format!("Completed: {}", report.completed_at),
        format!("Platform: {}", config.platform.label()),
        format!("Endpoint: {}", config.endpoint()),
        format!(
            "Runtime: {:?}, read_only={}, connect_on_start={}",
            config.mode, config.read_only, config.connect_on_start
        ),
        format!(
            "Requested market data: {} (live preferred={})",
            crate::ibkr::market_data_mode_label(config.market_data_mode),
            report.live_data_requested
        ),
        format!(
            "Account summary: buying_power={:?}, available_funds={:?}, net_liquidation={:?}",
            report.account_state.buying_power,
            report.account_state.available_funds,
            report.account_state.net_liquidation
        ),
        format!(
            "Universe: {} scanned, {} underlying snapshots, {} option quotes",
            report.symbols_scanned, report.underlying_snapshots, report.option_quotes_considered
        ),
        format!(
            "Candidates ranked: {}, proposed orders: {}, execution records: {}, open positions: {}",
            report.candidates_ranked,
            report.proposed_orders.len(),
            report.execution_records.len(),
            report.open_positions.len()
        ),
        format!(
            "Tracked paper lifecycle records: {}",
            report.paper_trade_lifecycle.len()
        ),
    ];

    if !report.non_live_symbols.is_empty() {
        lines.push(format!(
            "Non-live symbols observed: {}",
            report.non_live_symbols.join(", ")
        ));
    }

    append_section(
        &mut lines,
        "Run Summary",
        vec![
            format!(
                "Candidate funnel: {} accepted -> {} proposed -> {} execution records",
                report.accepted_candidates.len(),
                report.proposed_orders.len(),
                report.execution_records.len()
            ),
            format!(
                "Execution statuses: {}",
                render_status_counts(
                    report
                        .execution_records
                        .iter()
                        .map(|record| record.status.as_str())
                )
            ),
            format!(
                "Lifecycle statuses at close: {}",
                render_status_counts(
                    report
                        .paper_trade_lifecycle
                        .iter()
                        .map(|record| record.status.as_str())
                )
            ),
        ],
    );

    if !report.warnings.is_empty() {
        append_section(
            &mut lines,
            "Warnings",
            report
                .warnings
                .iter()
                .map(|warning| format!("- {warning}"))
                .collect(),
        );
    }

    append_section(
        &mut lines,
        "Accepted Candidates",
        render_candidates(&report.accepted_candidates),
    );
    append_section(
        &mut lines,
        "Proposed Orders",
        render_proposed_orders(&report.proposed_orders),
    );
    append_section(
        &mut lines,
        "Execution Outcomes",
        render_execution_records(&report.execution_records),
    );

    if !report.guardrail_rejections.is_empty() {
        append_section(
            &mut lines,
            "Guardrail Rejections",
            render_guardrail_rejections(report),
        );
    }

    append_section(
        &mut lines,
        "System State At Close",
        render_system_state(report),
    );

    let broker_event_records = report
        .execution_records
        .iter()
        .filter(|record| {
            record.broker_event_log_path.is_some() || !record.broker_event_timeline.is_empty()
        })
        .collect::<Vec<_>>();
    if !broker_event_records.is_empty() {
        lines.push(String::new());
        lines.push("Broker Event Timelines".to_string());
        for record in broker_event_records {
            let log_path = record
                .broker_event_log_path
                .as_deref()
                .unwrap_or("not persisted");
            lines.push(format!(
                "- {} [{}] {} events | artifact={}",
                record.symbol,
                record.status,
                record.broker_event_timeline.len(),
                log_path
            ));
            for event in &record.broker_event_timeline {
                lines.push(format!(
                    "  +{}ms {} {}",
                    event.elapsed_ms, event.event_type, event.detail
                ));
            }
        }
    }

    if !report.action_log.is_empty() {
        append_section(&mut lines, "Detailed Action Log", render_action_log(report));
    }

    if !report.notes.is_empty() {
        append_section(
            &mut lines,
            "Notes",
            report
                .notes
                .iter()
                .map(|note| format!("- {note}"))
                .collect(),
        );
    }

    lines.join("\n")
}

fn append_section(lines: &mut Vec<String>, title: &str, content: Vec<String>) {
    if content.is_empty() {
        return;
    }

    lines.push(String::new());
    lines.push(format!("{title}:"));
    lines.extend(content);
}

fn render_status_counts<'a>(statuses: impl Iterator<Item = &'a str>) -> String {
    let mut counts = BTreeMap::new();
    for status in statuses {
        *counts.entry(status.to_string()).or_insert(0usize) += 1;
    }

    if counts.is_empty() {
        "none".to_string()
    } else {
        counts
            .into_iter()
            .map(|(status, count)| format!("{status}={count}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn render_candidates(candidates: &[ScoredOptionCandidate]) -> Vec<String> {
    if candidates.is_empty() {
        return vec!["- No candidates survived option-screening guardrails.".to_string()];
    }

    candidates
        .iter()
        .take(10)
        .enumerate()
        .map(|(index, candidate)| {
            format!(
                "- #{:02} {} {} {:.2} exp {} | score {:.4} | underlying {:.2} | bid {:.2} | combo debit {:.2} | exp profit {:.2}/share | yield {:.2}% annualized | buffer {:.2}%",
                index + 1,
                candidate.right,
                candidate.symbol,
                candidate.strike,
                candidate.expiry,
                candidate.score,
                candidate.underlying_price,
                candidate.option_bid,
                candidate.underlying_ask.unwrap_or(candidate.underlying_price) - candidate.option_bid,
                candidate.expiration_profit_per_share,
                candidate.annualized_yield_pct,
                candidate.downside_buffer_pct
            )
        })
        .collect()
}

fn render_proposed_orders(proposed_orders: &[OrderIntent]) -> Vec<String> {
    if proposed_orders.is_empty() {
        return vec![
            "- No orders were proposed after account/risk/idempotency checks.".to_string(),
        ];
    }

    let mut lines = Vec::new();
    for intent in proposed_orders {
        lines.push(format!(
            "- {} | {} | est debit {:.2} | est credit {:.2} | max profit {:.2} | combo limit {} | mode {}",
            intent.symbol,
            intent.strategy,
            intent.estimated_net_debit,
            intent.estimated_credit,
            intent.max_profit,
            format_optional_price(intent.combo_limit_price),
            intent.mode
        ));
        for leg in &intent.legs {
            let instrument = match leg.instrument_type {
                crate::models::InstrumentType::Stock => "stock",
                crate::models::InstrumentType::Option => "option",
            };
            let action = match leg.action {
                crate::models::TradeAction::Buy => "BUY",
                crate::models::TradeAction::Sell => "SELL",
            };
            lines.push(format!(
                "  {} {} x{} | limit {} | {}",
                action,
                instrument,
                leg.quantity,
                format_optional_price(leg.limit_price),
                leg.description
            ));
        }
    }

    lines
}

fn render_execution_records(records: &[ExecutionRecord]) -> Vec<String> {
    if records.is_empty() {
        return vec!["- No execution records were produced.".to_string()];
    }

    let mut lines = Vec::new();
    for record in records {
        lines.push(format!(
            "- {} | status={} | mode={} | {}",
            record.symbol, record.status, record.submission_mode, record.note
        ));
        if let Some(fill) = &record.fill_reconciliation {
            lines.extend(render_fill_reconciliation(fill));
        }
        for leg in &record.legs {
            lines.push(render_execution_leg(leg));
        }
        if let Some(path) = &record.broker_event_log_path {
            lines.push(format!("  broker artifact: {path}"));
        }
    }

    lines
}

fn render_fill_reconciliation(fill: &FillReconciliationRecord) -> Vec<String> {
    vec![
        format!(
            "  fill reconciliation: status={} | stock {:.0} @ {} | short calls {:.0} @ {} | uncovered shares {:.0} | commission {}",
            fill.status,
            fill.stock_filled_shares,
            format_optional_price(fill.stock_average_fill_price),
            fill.short_call_filled_contracts,
            format_optional_price(fill.short_call_average_fill_price),
            fill.uncovered_shares,
            format_optional_price(fill.total_commission)
        ),
        format!("  reconciliation note: {}", fill.note),
    ]
}

fn render_execution_leg(leg: &ExecutionLegRecord) -> String {
    let instrument = match leg.instrument_type {
        crate::models::InstrumentType::Stock => "stock",
        crate::models::InstrumentType::Option => "option",
    };
    let action = match leg.action {
        crate::models::TradeAction::Buy => "BUY",
        crate::models::TradeAction::Sell => "SELL",
    };
    format!(
        "  leg: {} {} {} x{} | order_id={} | status={} | limit {} | filled {:.0} @ {} | exec_ids={} | note={}",
        action,
        leg.leg_symbol,
        instrument,
        leg.quantity,
        leg.order_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        leg.submission_status,
        format_optional_price(leg.limit_price),
        leg.filled_quantity,
        format_optional_price(leg.average_fill_price),
        if leg.execution_ids.is_empty() {
            "none".to_string()
        } else {
            leg.execution_ids.join(",")
        },
        leg.note
    )
}

fn render_guardrail_rejections(report: &CycleReport) -> Vec<String> {
    let mut lines = Vec::new();
    let mut stage_counts = BTreeMap::new();
    let mut symbol_counts = BTreeMap::new();

    for rejection in &report.guardrail_rejections {
        *stage_counts
            .entry(rejection.stage.clone())
            .or_insert(0usize) += 1;
        *symbol_counts
            .entry(rejection.symbol.clone())
            .or_insert(0usize) += 1;
    }

    lines.push(format!(
        "- Total rejections: {} | by stage: {}",
        report.guardrail_rejections.len(),
        stage_counts
            .into_iter()
            .map(|(stage, count)| format!("{stage}={count}"))
            .collect::<Vec<_>>()
            .join(", ")
    ));

    let mut sorted_symbols = symbol_counts.into_iter().collect::<Vec<_>>();
    sorted_symbols.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

    for (symbol, count) in sorted_symbols.into_iter().take(15) {
        let reasons = report
            .guardrail_rejections
            .iter()
            .filter(|rejection| rejection.symbol == symbol)
            .map(|rejection| format!("[{}] {}", rejection.stage, rejection.reason))
            .collect::<Vec<_>>();
        lines.push(format!("- {}: {} rejection(s)", symbol, count));
        for reason in reasons {
            lines.push(format!("  {reason}"));
        }
    }

    lines
}

fn render_system_state(report: &CycleReport) -> Vec<String> {
    let mut lines = Vec::new();

    if report.open_positions.is_empty() {
        lines.push("- Open positions: none reported by IBKR at end of run.".to_string());
    } else {
        lines.push("- Open positions reported by IBKR:".to_string());
        for position in &report.open_positions {
            lines.push(format!(
                "  {} | shares {:.0} | short calls {:.0} | avg stock cost {}",
                position.symbol,
                position.stock_shares,
                position.short_call_contracts,
                format_optional_price(position.average_stock_cost)
            ));
        }
    }

    if report.paper_trade_lifecycle.is_empty() {
        lines.push("- Paper lifecycle ledger: empty.".to_string());
    } else {
        lines.push("- Paper lifecycle ledger:".to_string());
        for lifecycle in &report.paper_trade_lifecycle {
            lines.push(render_lifecycle_record(lifecycle));
        }
    }

    lines
}

fn render_lifecycle_record(lifecycle: &PaperTradeLifecycleRecord) -> String {
    format!(
        "  {} | status={} | stock fill {:.0} @ {} | short calls {:.0} @ {} | observed shares {:.0} | observed short calls {:.0} | hold_until_close={} | note={}",
        lifecycle.symbol,
        lifecycle.status,
        lifecycle.stock_filled_shares,
        format_optional_price(lifecycle.stock_average_fill_price),
        lifecycle.short_call_filled_contracts,
        format_optional_price(lifecycle.short_call_average_fill_price),
        lifecycle.observed_stock_shares,
        lifecycle.observed_short_call_contracts,
        lifecycle.hold_until_close,
        lifecycle.note
    )
}

fn render_action_log(report: &CycleReport) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!("- {} action(s) captured.", report.action_log.len()));
    lines.extend(
        report
            .action_log
            .iter()
            .map(|action| format!("- {action}"))
            .collect::<Vec<_>>(),
    );
    lines
}

fn format_optional_price(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2}"))
        .unwrap_or_else(|| "n/a".to_string())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::render_human_log;
    use crate::{
        config::{
            AppConfig, BrokerPlatform, MarketDataMode, RiskConfig, RunMode, RuntimeMode,
            StrategyConfig,
        },
        models::{
            AccountState, CycleReport, ExecutionLegRecord, ExecutionRecord,
            FillReconciliationRecord, GuardrailRejection, InstrumentType, OpenPositionState,
            OrderIntent, OrderLegIntent, PaperTradeLifecycleRecord, ScoredOptionCandidate,
            TradeAction,
        },
    };

    fn test_config() -> AppConfig {
        AppConfig {
            host: "127.0.0.1".to_string(),
            platform: BrokerPlatform::Gateway,
            port: 4002,
            client_id: 7,
            account: "DU1234567".to_string(),
            mode: RuntimeMode::Paper,
            read_only: false,
            connect_on_start: true,
            run_mode: RunMode::Manual,
            scan_schedule: "manual".to_string(),
            market_data_mode: MarketDataMode::Live,
            universe_file: None,
            symbols: vec!["AAPL".to_string()],
            startup_warnings: Vec::new(),
            strategy: StrategyConfig::default(),
            risk: RiskConfig::default(),
        }
    }

    #[test]
    fn human_log_surfaces_trade_and_closeout_sections() {
        let report = CycleReport {
            started_at: Utc::now(),
            completed_at: Utc::now(),
            run_mode: "Manual".to_string(),
            schedule: "manual".to_string(),
            market_data_mode: "Live".to_string(),
            account_state: AccountState {
                account: "DU1234567".to_string(),
                available_funds: Some(10_000.0),
                buying_power: Some(20_000.0),
                net_liquidation: Some(10_500.0),
            },
            universe_size: 1,
            symbols_scanned: 1,
            underlying_snapshots: 1,
            option_quotes_considered: 1,
            candidates_ranked: 1,
            accepted_candidates: vec![ScoredOptionCandidate {
                symbol: "AAPL".to_string(),
                beta: 1.0,
                underlying_contract_id: 1,
                underlying_price: 100.0,
                underlying_ask: Some(100.25),
                option_contract_id: 2,
                strike: 90.0,
                expiry: "20991217".to_string(),
                right: "C".to_string(),
                exchange: "SMART".to_string(),
                trading_class: "AAPL".to_string(),
                multiplier: "100".to_string(),
                days_to_expiration: 30,
                option_bid: 11.0,
                option_ask: Some(11.2),
                delta: Some(0.9),
                itm_depth_pct: 10.0,
                downside_buffer_pct: 12.0,
                expiration_profit_per_share: 0.75,
                annualized_yield_pct: 15.0,
                expiration_yield_pct: 1.2,
                score: 2.5,
            }],
            guardrail_rejections: vec![GuardrailRejection {
                symbol: "TSLA".to_string(),
                stage: "strategy".to_string(),
                reason: "downside buffer below configured minimum".to_string(),
            }],
            proposed_orders: vec![OrderIntent {
                symbol: "AAPL".to_string(),
                strategy: "deep-ITM covered-call buy-write".to_string(),
                account: "DU1234567".to_string(),
                mode: "paper-combo-bag".to_string(),
                combo_limit_price: Some(89.25),
                estimated_net_debit: 8_925.0,
                estimated_credit: 1_100.0,
                max_profit: 75.0,
                legs: vec![
                    OrderLegIntent {
                        instrument_type: InstrumentType::Stock,
                        action: TradeAction::Buy,
                        contract_id: Some(1),
                        symbol: "AAPL".to_string(),
                        description: "Buy 100 shares of AAPL".to_string(),
                        quantity: 100,
                        limit_price: Some(100.25),
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
                        contract_id: Some(2),
                        symbol: "AAPL".to_string(),
                        description: "Sell 1 AAPL 20991217 90C".to_string(),
                        quantity: 1,
                        limit_price: Some(11.0),
                        expiry: Some("20991217".to_string()),
                        strike: Some(90.0),
                        right: Some("C".to_string()),
                        exchange: Some("SMART".to_string()),
                        trading_class: Some("AAPL".to_string()),
                        multiplier: Some("100".to_string()),
                        currency: Some("USD".to_string()),
                    },
                ],
            }],
            execution_records: vec![ExecutionRecord {
                symbol: "AAPL".to_string(),
                status: "combo-submitted".to_string(),
                submission_mode: "paper".to_string(),
                note: "submitted combo order".to_string(),
                legs: vec![ExecutionLegRecord {
                    leg_symbol: "AAPL".to_string(),
                    instrument_type: InstrumentType::Stock,
                    action: TradeAction::Buy,
                    quantity: 100,
                    order_id: Some(10),
                    submission_status: "Submitted".to_string(),
                    limit_price: Some(100.25),
                    filled_quantity: 0.0,
                    average_fill_price: None,
                    execution_ids: Vec::new(),
                    note: "awaiting fill".to_string(),
                }],
                fill_reconciliation: Some(FillReconciliationRecord {
                    stock_filled_shares: 0.0,
                    stock_average_fill_price: None,
                    short_call_filled_contracts: 0.0,
                    short_call_average_fill_price: None,
                    total_commission: None,
                    eligible_for_short_call: false,
                    uncovered_shares: 0.0,
                    status: "combo-submitted".to_string(),
                    note: "awaiting synchronized fill".to_string(),
                }),
                broker_event_log_path: Some("logs/example.json".to_string()),
                broker_event_timeline: Vec::new(),
            }],
            open_positions: vec![OpenPositionState {
                symbol: "AAPL".to_string(),
                stock_shares: 100.0,
                short_call_contracts: 1.0,
                average_stock_cost: Some(100.0),
            }],
            paper_trade_lifecycle: vec![PaperTradeLifecycleRecord {
                symbol: "AAPL".to_string(),
                intent_key: "intent".to_string(),
                status: "combo-submitted".to_string(),
                first_recorded_at: Utc::now(),
                last_updated_at: Utc::now(),
                hold_until_close: true,
                stock_order_id: Some(10),
                short_call_order_id: Some(10),
                stock_filled_shares: 0.0,
                short_call_filled_contracts: 0.0,
                stock_average_fill_price: None,
                short_call_average_fill_price: None,
                observed_stock_shares: 100.0,
                observed_short_call_contracts: 1.0,
                note: "tracked in paper ledger".to_string(),
            }],
            live_data_requested: true,
            non_live_symbols: Vec::new(),
            warnings: vec!["example warning".to_string()],
            action_log: vec!["example action".to_string()],
            human_log_path: None,
            notes: vec!["example note".to_string()],
        };

        let log = render_human_log(&test_config(), &report);

        assert!(log.contains("Accepted Candidates:"));
        assert!(log.contains("Proposed Orders:"));
        assert!(log.contains("Execution Outcomes:"));
        assert!(log.contains("System State At Close:"));
        assert!(log.contains("Guardrail Rejections:"));
    }
}
