use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::Local;

use crate::{
    artifacts::{logs_dir, timestamped_log_path_in},
    config::AppConfig,
    models::{
        CycleReport, ExecutionLegRecord, ExecutionRecord, FillReconciliationRecord, OrderIntent,
        PaperTradeLifecycleRecord, ScoredOptionCandidate, StatusReport,
    },
};

const DIAGNOSTIC_LOG_DIR: &str = "diagnostic";
const ACTION_LOG_DIR: &str = "action";
const TRADE_LOG_DIR: &str = "trades";
const API_LOG_DIR: &str = "api";

#[derive(Debug, Default, Clone)]
pub struct OutputArtifacts {
    pub diagnostic_log_path: Option<PathBuf>,
    pub action_log_path: Option<PathBuf>,
    pub trade_log_path: Option<PathBuf>,
    pub api_log_path: Option<PathBuf>,
}

impl OutputArtifacts {
    pub fn terminal_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        if let Some(path) = &self.diagnostic_log_path {
            lines.push(format!("Diagnostic log: {}", path.display()));
        }
        if let Some(path) = &self.action_log_path {
            lines.push(format!("Action log: {}", path.display()));
        }
        if let Some(path) = &self.trade_log_path {
            lines.push(format!("Trade log: {}", path.display()));
        }
        if let Some(path) = &self.api_log_path {
            lines.push(format!("API log: {}", path.display()));
        }
        lines
    }
}

pub fn write_cycle_outputs(config: &AppConfig, report: &CycleReport) -> Result<OutputArtifacts> {
    write_cycle_outputs_in(&logs_dir(), config, report)
}

fn write_cycle_outputs_in(
    root: &Path,
    config: &AppConfig,
    report: &CycleReport,
) -> Result<OutputArtifacts> {
    let mut outputs = OutputArtifacts::default();

    if config.logs.diagnostic_log {
        let path = timestamped_log_path_in(root, DIAGNOSTIC_LOG_DIR, "Diagnostic", "json");
        write_log_file(&path, &serde_json::to_string_pretty(report)?)?;
        outputs.diagnostic_log_path = Some(path);
    }
    if config.logs.action_log {
        let path = timestamped_log_path_in(root, ACTION_LOG_DIR, "Action", "txt");
        write_log_file(&path, &render_cycle_action_log(report))?;
        outputs.action_log_path = Some(path);
    }
    if config.logs.trade_log {
        let path = trade_log_path(root);
        write_log_file(&path, &render_trade_log(report))?;
        outputs.trade_log_path = Some(path);
    }
    if config.logs.api_log {
        let path = timestamped_log_path_in(root, API_LOG_DIR, "API", "txt");
        write_log_file(&path, &render_cycle_api_log(config, report))?;
        outputs.api_log_path = Some(path);
    }

    Ok(outputs)
}

pub fn write_status_outputs(config: &AppConfig, report: &StatusReport) -> Result<OutputArtifacts> {
    write_status_outputs_in(&logs_dir(), config, report)
}

fn write_status_outputs_in(
    root: &Path,
    config: &AppConfig,
    report: &StatusReport,
) -> Result<OutputArtifacts> {
    let mut outputs = OutputArtifacts::default();

    if config.logs.diagnostic_log {
        let path = timestamped_log_path_in(root, DIAGNOSTIC_LOG_DIR, "Diagnostic", "json");
        write_log_file(&path, &serde_json::to_string_pretty(report)?)?;
        outputs.diagnostic_log_path = Some(path);
    }
    if config.logs.action_log {
        let path = timestamped_log_path_in(root, ACTION_LOG_DIR, "Action", "txt");
        write_log_file(&path, &render_status_action_log(report))?;
        outputs.action_log_path = Some(path);
    }
    if config.logs.api_log {
        let path = timestamped_log_path_in(root, API_LOG_DIR, "API", "txt");
        write_log_file(&path, &render_status_api_log(config, report))?;
        outputs.api_log_path = Some(path);
    }

    Ok(outputs)
}

fn write_log_file(path: &PathBuf, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create log directory {}", parent.display()))?;
    }
    fs::write(path, contents)
        .with_context(|| format!("failed to write log artifact to {}", path.display()))
}

pub fn render_human_log(config: &AppConfig, report: &CycleReport) -> String {
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
            "Capital allocation: configured source {} | preview {} deployable {:.2} | routed {} deployable {:.2}",
            report.capital_source_details.configured_source,
            report.capital_source_details.preview.source,
            report.capital_source_details.preview.deployable_cash,
            report.capital_source_details.routed_orders.source,
            report.capital_source_details.routed_orders.deployable_cash
        ),
        format!(
            "Universe: {} scanned, {} underlying snapshots, {} option quotes",
            report.symbols_scanned, report.underlying_snapshots, report.option_quotes_considered
        ),
        format!(
            "Cycle timing: total {} ms, market data {} ms",
            report.timing_metrics.total_elapsed_ms, report.timing_metrics.market_data_elapsed_ms
        ),
        format!(
            "Throughput: symbol concurrency {} | option quote concurrency {} | symbols/sec {:.2} | underlying snapshots/sec {:.2} | option quotes/sec {:.2}",
            report.throughput_counters.configured_symbol_concurrency,
            report
                .throughput_counters
                .configured_option_quote_concurrency_per_symbol,
            report.throughput_counters.symbols_per_second,
            report.throughput_counters.underlying_snapshots_per_second,
            report.throughput_counters.option_quotes_per_second
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
            format!("Outcome scoreboard: {}", render_scan_scoreboard(report)),
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

    append_section(&mut lines, "Allocation", render_allocation_summary(report));

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

pub fn render_status_log(config: &AppConfig, report: &StatusReport) -> String {
    let mut lines = vec![
        "IBKR Paper Status".to_string(),
        format!("Account: {}", report.account),
        format!("Platform: {}", report.platform),
        format!("Endpoint: {}", report.endpoint),
        format!(
            "Runtime: {:?}, connect_on_start={}",
            config.mode, report.connect_on_start
        ),
        format!(
            "Account summary: buying_power={:?}, available_funds={:?}, net_liquidation={:?}",
            report.account_state.buying_power,
            report.account_state.available_funds,
            report.account_state.net_liquidation
        ),
        format!(
            "Capital policy: source={} deployment_budget={:.2}",
            report.capital_source, report.deployment_budget
        ),
        format!(
            "At a glance: {} open position group(s), {} open order(s), {} completed order(s), {} paper ledger record(s)",
            report.open_positions.len(),
            report.open_orders.len(),
            report.completed_orders.len(),
            report.paper_trade_lifecycle.len()
        ),
        format!(
            "Open-order statuses: {}",
            render_status_counts(report.open_orders.iter().map(|order| {
                if order.status.is_empty() {
                    "open"
                } else {
                    order.status.as_str()
                }
            }))
        ),
        format!(
            "Ledger statuses: {}",
            render_status_counts(
                report
                    .paper_trade_lifecycle
                    .iter()
                    .map(|record| record.status.as_str())
            )
        ),
    ];

    append_section(
        &mut lines,
        "Open Positions",
        render_status_positions(report),
    );
    append_section(&mut lines, "Open Orders", render_status_open_orders(report));
    append_section(
        &mut lines,
        "Recent Completed Orders",
        render_status_completed_orders(report),
    );
    append_section(
        &mut lines,
        "Paper Lifecycle Ledger",
        render_status_lifecycle(report),
    );

    if !report.action_log.is_empty() {
        append_section(
            &mut lines,
            "Reconcile Actions",
            report
                .action_log
                .iter()
                .map(|entry| format!("- {entry}"))
                .collect(),
        );
    }

    lines.join("\n")
}

pub fn render_trade_summary(report: &CycleReport) -> Vec<String> {
    render_current_position_lines(report)
}

pub fn render_current_open_position_summary(report: &CycleReport) -> Vec<String> {
    let lines = render_current_position_lines(report);
    if lines.is_empty() {
        vec!["- none".to_string()]
    } else {
        lines.into_iter().map(|line| format!("- {line}")).collect()
    }
}

pub fn render_filled_trade_summary(report: &CycleReport) -> Vec<String> {
    report
        .execution_records
        .iter()
        .filter(|record| execution_record_represents_filled_trade(record))
        .map(render_execution_trade_record)
        .collect()
}

pub fn render_left_open_trade_summary(report: &CycleReport) -> Vec<String> {
    report
        .execution_records
        .iter()
        .filter(|record| execution_record_represents_left_open_trade(record))
        .map(render_execution_trade_record)
        .collect()
}

fn render_current_position_lines(report: &CycleReport) -> Vec<String> {
    report
        .paper_trade_lifecycle
        .iter()
        .filter(|lifecycle| lifecycle_represents_opened_trade(lifecycle))
        .map(|lifecycle| {
            format!(
                "{} | status={} | placed_at={} | purchase net debit {} | current value net credit n/a | current profit n/a | expected profit {} | stock {:.0} @ {} | short calls {:.0} @ {} | observed shares {:.0} | observed short calls {:.0} | order_ids={}",
                lifecycle.symbol,
                lifecycle.status,
                lifecycle.first_recorded_at.to_rfc3339(),
                format_optional_price(lifecycle.entry_net_debit),
                format_optional_price(lifecycle.expected_profit),
                lifecycle.stock_filled_shares,
                format_optional_price(lifecycle.stock_average_fill_price),
                lifecycle.short_call_filled_contracts,
                format_optional_price(lifecycle.short_call_average_fill_price),
                lifecycle.observed_stock_shares,
                lifecycle.observed_short_call_contracts,
                render_lifecycle_order_ids(lifecycle)
            )
        })
        .collect()
}

fn render_trade_log(report: &CycleReport) -> String {
    let filled = render_filled_trade_summary(report);
    let current_positions = render_trade_summary(report);
    let mut lines = Vec::new();

    append_section(
        &mut lines,
        "Trades Filled this Run",
        section_lines_or_none(filled),
    );
    append_section(
        &mut lines,
        "Current Positions (after this run)",
        section_lines_or_none(current_positions),
    );

    lines.join("\n")
}

fn render_cycle_action_log(report: &CycleReport) -> String {
    let mut lines = vec!["Action Log".to_string()];
    lines.push(format!(
        "Proposed orders: {} | execution records: {}",
        report.proposed_orders.len(),
        report.execution_records.len()
    ));
    if !report.action_log.is_empty() {
        lines.push(String::new());
        lines.push("Recorded actions:".to_string());
        lines.extend(report.action_log.iter().map(|entry| format!("- {entry}")));
    }

    let submitted_trade_lines = render_submitted_trade_records(report);
    if !submitted_trade_lines.is_empty() {
        lines.push(String::new());
        lines.push("Submitted trade records:".to_string());
        lines.extend(submitted_trade_lines);
    }
    lines.join("\n")
}

fn render_status_action_log(report: &StatusReport) -> String {
    let mut lines = vec!["Action Log".to_string()];
    lines.push(format!(
        "Open orders: {} | completed orders: {} | lifecycle records: {}",
        report.open_orders.len(),
        report.completed_orders.len(),
        report.paper_trade_lifecycle.len()
    ));
    if !report.action_log.is_empty() {
        lines.push(String::new());
        lines.push("Recorded actions:".to_string());
        lines.extend(report.action_log.iter().map(|entry| format!("- {entry}")));
    }
    lines.join("\n")
}

fn render_cycle_api_log(config: &AppConfig, report: &CycleReport) -> String {
    let mut lines = vec![
        "API Log".to_string(),
        format!("Endpoint: {}", config.endpoint()),
        format!("Account: {}", config.account),
    ];
    lines.extend(report.api_log.iter().map(|entry| format!("- {entry}")));

    let broker_event_records = report
        .execution_records
        .iter()
        .filter(|record| !record.broker_event_timeline.is_empty())
        .collect::<Vec<_>>();
    if !broker_event_records.is_empty() {
        lines.push(String::new());
        lines.push("Broker event timelines:".to_string());
        for record in broker_event_records {
            lines.push(format!(
                "- {} [{}] {} event(s)",
                record.symbol,
                record.status,
                record.broker_event_timeline.len()
            ));
            lines.extend(record.broker_event_timeline.iter().map(|event| {
                format!(
                    "  +{}ms {} {}",
                    event.elapsed_ms, event.event_type, event.detail
                )
            }));
        }
    }

    lines.join("\n")
}

fn render_status_api_log(config: &AppConfig, report: &StatusReport) -> String {
    let mut lines = vec![
        "API Log".to_string(),
        format!("Endpoint: {}", config.endpoint()),
        format!("Account: {}", config.account),
    ];
    lines.extend(report.api_log.iter().map(|entry| format!("- {entry}")));
    lines.join("\n")
}

fn lifecycle_represents_opened_trade(lifecycle: &PaperTradeLifecycleRecord) -> bool {
    lifecycle.stock_filled_shares > 0.0
        || lifecycle.short_call_filled_contracts > 0.0
        || lifecycle.observed_stock_shares > 0.0
        || lifecycle.observed_short_call_contracts > 0.0
        || matches!(
            lifecycle.status.as_str(),
            "filled" | "open-covered-call" | "long-stock-awaiting-short-call"
        )
}

fn execution_record_represents_filled_trade(record: &ExecutionRecord) -> bool {
    record.submission_mode == "paper"
        && record.symbol != "N/A"
        && record.fill_reconciliation.as_ref().is_some_and(|fill| {
            fill.stock_filled_shares > 0.0 && fill.short_call_filled_contracts > 0.0
        })
}

fn execution_record_represents_left_open_trade(record: &ExecutionRecord) -> bool {
    record.submission_mode == "paper"
        && record.symbol != "N/A"
        && record.legs.iter().any(|leg| leg.order_id.is_some())
        && !execution_record_represents_filled_trade(record)
}

fn render_lifecycle_order_ids(lifecycle: &PaperTradeLifecycleRecord) -> String {
    let mut order_ids = Vec::new();
    if let Some(order_id) = lifecycle.stock_order_id {
        order_ids.push(order_id.to_string());
    }
    if let Some(order_id) = lifecycle.short_call_order_id {
        if !order_ids
            .iter()
            .any(|existing| existing == &order_id.to_string())
        {
            order_ids.push(order_id.to_string());
        }
    }

    if order_ids.is_empty() {
        "n/a".to_string()
    } else {
        order_ids.join(", ")
    }
}

fn render_submitted_trade_records(report: &CycleReport) -> Vec<String> {
    report
        .execution_records
        .iter()
        .filter(|record| {
            record.submission_mode == "paper"
                && record.symbol != "N/A"
                && record.legs.iter().any(|leg| leg.order_id.is_some())
        })
        .map(|record| {
            let stock_leg = record
                .legs
                .iter()
                .find(|leg| leg.instrument_type == crate::models::InstrumentType::Stock);
            let option_leg = record
                .legs
                .iter()
                .find(|leg| leg.instrument_type == crate::models::InstrumentType::Option);
            format!(
                "- {} | status={} | stock {} @ {} | option {} | order_ids={}",
                record.symbol,
                record.status,
                stock_leg.map(|leg| leg.quantity).unwrap_or_default(),
                format_optional_price(stock_leg.and_then(|leg| leg.limit_price)),
                option_leg
                    .map(render_submitted_trade_option_leg)
                    .unwrap_or_else(|| "n/a".to_string()),
                record
                    .legs
                    .iter()
                    .filter_map(|leg| leg.order_id)
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
        .collect()
}

fn render_execution_trade_record(record: &ExecutionRecord) -> String {
    let stock_leg = record
        .legs
        .iter()
        .find(|leg| leg.instrument_type == crate::models::InstrumentType::Stock);
    let option_leg = record
        .legs
        .iter()
        .find(|leg| leg.instrument_type == crate::models::InstrumentType::Option);

    let stock_quantity = stock_leg
        .map(|leg| {
            if leg.filled_quantity > 0.0 {
                format!("{:.0}", leg.filled_quantity)
            } else {
                leg.quantity.to_string()
            }
        })
        .unwrap_or_else(|| "0".to_string());
    let stock_price = stock_leg.and_then(|leg| leg.average_fill_price.or(leg.limit_price));
    let option_price = option_leg.and_then(|leg| leg.average_fill_price.or(leg.limit_price));

    format!(
        "{} | status={} | stock {} @ {} | option {} | order_ids={}",
        record.symbol,
        record.status,
        stock_quantity,
        format_optional_price(stock_price),
        option_leg
            .map(|leg| {
                let quantity = if leg.filled_quantity > 0.0 {
                    format!("{:.0}", leg.filled_quantity)
                } else {
                    leg.quantity.to_string()
                };
                format!(
                    "{} x{} @ {}",
                    leg.leg_symbol,
                    quantity,
                    format_optional_price(option_price)
                )
            })
            .unwrap_or_else(|| "n/a".to_string()),
        record
            .legs
            .iter()
            .filter_map(|leg| leg.order_id)
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn section_lines_or_none(lines: Vec<String>) -> Vec<String> {
    if lines.is_empty() {
        vec!["- none".to_string()]
    } else {
        lines.into_iter().map(|line| format!("- {line}")).collect()
    }
}

fn render_submitted_trade_option_leg(leg: &ExecutionLegRecord) -> String {
    format!(
        "{} x{} @ {}",
        leg.leg_symbol,
        leg.quantity,
        format_optional_price(leg.limit_price)
    )
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

fn render_scan_scoreboard(report: &CycleReport) -> String {
    let market_data_rejections = report
        .guardrail_rejections
        .iter()
        .filter(|rejection| rejection.stage == "market-data")
        .count();
    let risk_rejections = report
        .guardrail_rejections
        .iter()
        .filter(|rejection| rejection.stage == "risk")
        .count();
    let pricing_rejections = report
        .guardrail_rejections
        .iter()
        .filter(|rejection| rejection.stage == "pricing")
        .count();

    format!(
        "accepted={} | proposed={} | executed={} | guardrail_rejections={} (market-data={}, risk={}, pricing={})",
        report.accepted_candidates.len(),
        report.proposed_orders.len(),
        report.execution_records.len(),
        report.guardrail_rejections.len(),
        market_data_rejections,
        risk_rejections,
        pricing_rejections
    )
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
                candidate.annualized_yield_ratio * 100.0,
                candidate.downside_buffer_ratio * 100.0
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
            "- {} | {} | lots {} | est debit {:.2} | est credit {:.2} | max profit {:.2} | combo limit {} | mode {}",
            intent.symbol,
            intent.strategy,
            intent.lot_quantity,
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

fn render_allocation_summary(report: &CycleReport) -> Vec<String> {
    let preview = &report.capital_source_details.preview;
    let routed = &report.capital_source_details.routed_orders;

    vec![
        format!(
            "- Configured source={} | preview source={} reported={:?} reserve {:.2}% ({:.2}) | cash after reserve {:.2} | deployment budget {:.2} | deployable {:.2} | per-symbol distribution cap {:.2}",
            report.capital_source_details.configured_source,
            preview.source,
            preview.reported_amount,
            preview.reserve_ratio * 100.0,
            preview.reserve_amount,
            preview.cash_after_reserve,
            preview.deployment_budget,
            preview.deployable_cash,
            preview.max_cash_per_symbol
        ),
        format!(
            "- Routed orders source={} reported={:?} reserve {:.2}% ({:.2}) | cash after reserve {:.2} | deployable {:.2} | per-symbol distribution cap {:.2}",
            routed.source,
            routed.reported_amount,
            routed.reserve_ratio * 100.0,
            routed.reserve_amount,
            routed.cash_after_reserve,
            routed.deployable_cash,
            routed.max_cash_per_symbol
        ),
        format!(
            "- Collapsed candidate symbols {} | selected symbols {} | total lots {} | existing exposure {:.2} | newly allocated cash {:.2} | remaining cash {:.2}",
            report.allocation_summary.candidate_symbols_considered,
            report.allocation_summary.selected_symbols,
            report.allocation_summary.total_lots,
            report.allocation_summary.existing_exposure_cash,
            report.allocation_summary.allocated_cash,
            report.allocation_summary.remaining_cash
        ),
    ]
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
        for timing in &record.execution_step_timings {
            lines.push(render_execution_step_timing(timing));
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

fn render_execution_step_timing(timing: &crate::models::ExecutionStepTiming) -> String {
    format!(
        "  timing: {} | duration={}ms | attempt={} | order_id={} | limit {}",
        timing.step,
        timing.duration_ms,
        timing
            .attempt
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        timing
            .order_id
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        format_optional_price(timing.limit_price)
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
        "  {} | status={} | placed_at={} | purchase net debit {} | current value net credit n/a | current profit n/a | expected profit {} | stock fill {:.0} @ {} | short calls {:.0} @ {} | observed shares {:.0} | observed short calls {:.0} | hold_until_close={} | note={}",
        lifecycle.symbol,
        lifecycle.status,
        lifecycle.first_recorded_at.to_rfc3339(),
        format_optional_price(lifecycle.entry_net_debit),
        format_optional_price(lifecycle.expected_profit),
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

fn render_status_positions(report: &StatusReport) -> Vec<String> {
    if report.open_positions.is_empty() {
        return vec!["- No open stock/short-call position groups reported by IBKR.".to_string()];
    }

    report
        .open_positions
        .iter()
        .map(|position| {
            format!(
                "- {} | shares {:.0} | short calls {:.0} | avg stock cost {}",
                position.symbol,
                position.stock_shares,
                position.short_call_contracts,
                format_optional_price(position.average_stock_cost)
            )
        })
        .collect()
}

fn render_status_open_orders(report: &StatusReport) -> Vec<String> {
    if report.open_orders.is_empty() {
        return vec!["- No open orders currently reported by IBKR.".to_string()];
    }

    report
        .open_orders
        .iter()
        .map(|order| {
            format!(
                "- {} | order_id={} | {} {} {} | status={} | qty filled {:.0}/{:.0} | limit {}",
                order.symbol,
                order.order_id,
                order.action,
                order.security_type,
                order.order_type,
                if order.status.is_empty() {
                    "open"
                } else {
                    order.status.as_str()
                },
                order.filled_quantity,
                order.total_quantity,
                format_optional_price(order.limit_price)
            )
        })
        .collect()
}

fn render_status_completed_orders(report: &StatusReport) -> Vec<String> {
    if report.completed_orders.is_empty() {
        return vec![
            "- No completed orders returned by IBKR for this account snapshot.".to_string(),
        ];
    }

    report
        .completed_orders
        .iter()
        .take(10)
        .map(|order| {
            format!(
                "- {} | order_id={} | {} {} {} | status={} | completed_status={} | completed_time={}",
                order.symbol,
                order.order_id,
                order.action,
                order.security_type,
                order.order_type,
                order.status,
                if order.completed_status.is_empty() {
                    "n/a"
                } else {
                    order.completed_status.as_str()
                },
                if order.completed_time.is_empty() {
                    "n/a"
                } else {
                    order.completed_time.as_str()
                }
            )
        })
        .collect()
}

fn render_status_lifecycle(report: &StatusReport) -> Vec<String> {
    if report.paper_trade_lifecycle.is_empty() {
        return vec!["- Paper lifecycle ledger is currently empty.".to_string()];
    }

    report
        .paper_trade_lifecycle
        .iter()
        .map(render_lifecycle_record)
        .collect()
}

fn format_optional_price(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2}"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn trade_log_path(root: &Path) -> PathBuf {
    root.join(TRADE_LOG_DIR).join(format!(
        "{}_Trade_Log.txt",
        Local::now().format("%Y%m%d-%H-%M-%S")
    ))
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use chrono::Utc;

    use super::{
        render_cycle_action_log, render_human_log, render_status_log, render_trade_log,
        write_cycle_outputs_in, write_status_outputs_in,
    };
    use crate::{
        config::{
            AllocationConfig, AppConfig, BrokerPlatform, ExecutionTuningConfig, LogsConfig,
            MarketDataMode, PerformanceConfig, RiskConfig, RunMode, RuntimeMode, StrategyConfig,
        },
        models::{
            AccountState, AllocationSummary, BrokerCompletedOrder, BrokerOpenOrder,
            CapitalAllocationView, CapitalSourceDetails, CycleReport, CycleThroughputCounters,
            CycleTimingMetrics, ExecutionLegRecord, ExecutionRecord, FillReconciliationRecord,
            GuardrailRejection, InstrumentType, OpenPositionState, OrderIntent, OrderLegIntent,
            PaperTradeLifecycleRecord, ScoredOptionCandidate, StatusReport, TradeAction,
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
            allocation: AllocationConfig::default(),
            performance: PerformanceConfig::default(),
            execution: ExecutionTuningConfig::default(),
            logs: LogsConfig::default(),
        }
    }

    struct TestArtifactDir {
        path: PathBuf,
    }

    impl TestArtifactDir {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "options-trading-reporting-tests-{name}-{}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &std::path::Path {
            &self.path
        }
    }

    impl Drop for TestArtifactDir {
        fn drop(&mut self) {
            if !std::thread::panicking() {
                let _ = fs::remove_dir_all(&self.path);
            }
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
                itm_depth_ratio: 0.10,
                downside_buffer_ratio: 0.12,
                expiration_profit_per_share: 0.75,
                annualized_yield_ratio: 0.15,
                expiration_yield_ratio: 0.012,
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
                lot_quantity: 1,
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
                execution_step_timings: Vec::new(),
            }],
            open_positions: vec![OpenPositionState {
                symbol: "ORCL".to_string(),
                stock_shares: 100.0,
                short_call_contracts: 1.0,
                average_stock_cost: Some(100.0),
            }],
            paper_trade_lifecycle: vec![PaperTradeLifecycleRecord {
                symbol: "ORCL".to_string(),
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
                entry_net_debit: Some(8_925.0),
                expected_profit: Some(75.0),
                observed_stock_shares: 100.0,
                observed_short_call_contracts: 1.0,
                note: "tracked in paper ledger".to_string(),
            }],
            live_data_requested: true,
            non_live_symbols: Vec::new(),
            capital_source_details: CapitalSourceDetails {
                configured_source: "available_funds".to_string(),
                preview: CapitalAllocationView {
                    source: "available_funds".to_string(),
                    reported_amount: Some(10_000.0),
                    reserve_ratio: 0.05,
                    reserve_amount: 500.0,
                    cash_after_reserve: 9_500.0,
                    deployment_budget: 10_000.0,
                    deployable_cash: 9_500.0,
                    max_cash_per_symbol: 2_000.0,
                },
                routed_orders: CapitalAllocationView {
                    source: "available_funds".to_string(),
                    reported_amount: Some(10_000.0),
                    reserve_ratio: 0.05,
                    reserve_amount: 500.0,
                    cash_after_reserve: 9_500.0,
                    deployment_budget: 10_000.0,
                    deployable_cash: 9_500.0,
                    max_cash_per_symbol: 2_000.0,
                },
            },
            allocation_summary: AllocationSummary {
                candidate_symbols_considered: 1,
                selected_symbols: 1,
                total_lots: 1,
                existing_exposure_cash: 2_000.0,
                allocated_cash: 8_925.0,
                remaining_cash: 575.0,
            },
            warnings: vec!["example warning".to_string()],
            diagnostic_log: vec!["example diagnostic".to_string()],
            action_log: vec!["example action".to_string()],
            api_log: vec!["example api".to_string()],
            timing_metrics: CycleTimingMetrics {
                total_elapsed_ms: 1_500,
                market_data_elapsed_ms: 900,
            },
            throughput_counters: CycleThroughputCounters {
                configured_symbol_concurrency: 4,
                configured_option_quote_concurrency_per_symbol: 2,
                symbols_completed: 1,
                underlying_snapshots_completed: 1,
                option_quotes_completed: 1,
                symbols_per_second: 1.11,
                underlying_snapshots_per_second: 1.11,
                option_quotes_per_second: 1.11,
            },
            human_log_path: None,
            notes: vec!["example note".to_string()],
        };

        let log = render_human_log(&test_config(), &report);

        assert!(log.contains("Accepted Candidates:"));
        assert!(log.contains("Proposed Orders:"));
        assert!(log.contains("Execution Outcomes:"));
        assert!(log.contains("System State At Close:"));
        assert!(log.contains("Guardrail Rejections:"));
        assert!(log.contains("Allocation:"));
        assert!(log.contains("Collapsed candidate symbols 1 | selected symbols 1 | total lots 1"));
        assert!(log.contains("Outcome scoreboard:"));
        assert!(log.contains("Cycle timing: total 1500 ms, market data 900 ms"));
        assert!(log.contains("Throughput: symbol concurrency 4 | option quote concurrency 2"));
    }

    #[test]
    fn trade_log_only_includes_opened_or_filled_trades() {
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
            candidates_ranked: 0,
            accepted_candidates: Vec::new(),
            guardrail_rejections: Vec::new(),
            proposed_orders: Vec::new(),
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
                fill_reconciliation: None,
                broker_event_log_path: None,
                broker_event_timeline: Vec::new(),
                execution_step_timings: Vec::new(),
            }],
            open_positions: Vec::new(),
            paper_trade_lifecycle: vec![
                PaperTradeLifecycleRecord {
                    symbol: "AAPL".to_string(),
                    intent_key: "aapl".to_string(),
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
                    entry_net_debit: None,
                    expected_profit: None,
                    observed_stock_shares: 0.0,
                    observed_short_call_contracts: 0.0,
                    note: "awaiting fill".to_string(),
                },
                PaperTradeLifecycleRecord {
                    symbol: "MSFT".to_string(),
                    intent_key: "msft".to_string(),
                    status: "open-covered-call".to_string(),
                    first_recorded_at: Utc::now(),
                    last_updated_at: Utc::now(),
                    hold_until_close: true,
                    stock_order_id: Some(11),
                    short_call_order_id: Some(11),
                    stock_filled_shares: 100.0,
                    short_call_filled_contracts: 1.0,
                    stock_average_fill_price: Some(95.0),
                    short_call_average_fill_price: Some(6.5),
                    entry_net_debit: Some(8_850.0),
                    expected_profit: Some(150.0),
                    observed_stock_shares: 100.0,
                    observed_short_call_contracts: 1.0,
                    note: "opened".to_string(),
                },
            ],
            live_data_requested: true,
            non_live_symbols: Vec::new(),
            capital_source_details: CapitalSourceDetails {
                configured_source: "available_funds".to_string(),
                preview: CapitalAllocationView {
                    source: "available_funds".to_string(),
                    reported_amount: Some(10_000.0),
                    reserve_ratio: 0.05,
                    reserve_amount: 500.0,
                    cash_after_reserve: 9_500.0,
                    deployment_budget: 10_000.0,
                    deployable_cash: 9_500.0,
                    max_cash_per_symbol: 2_000.0,
                },
                routed_orders: CapitalAllocationView {
                    source: "available_funds".to_string(),
                    reported_amount: Some(10_000.0),
                    reserve_ratio: 0.05,
                    reserve_amount: 500.0,
                    cash_after_reserve: 9_500.0,
                    deployment_budget: 10_000.0,
                    deployable_cash: 9_500.0,
                    max_cash_per_symbol: 2_000.0,
                },
            },
            allocation_summary: AllocationSummary {
                candidate_symbols_considered: 0,
                selected_symbols: 0,
                total_lots: 0,
                existing_exposure_cash: 0.0,
                allocated_cash: 0.0,
                remaining_cash: 10_000.0,
            },
            warnings: Vec::new(),
            diagnostic_log: Vec::new(),
            action_log: Vec::new(),
            api_log: Vec::new(),
            timing_metrics: CycleTimingMetrics {
                total_elapsed_ms: 1,
                market_data_elapsed_ms: 1,
            },
            throughput_counters: CycleThroughputCounters {
                configured_symbol_concurrency: 1,
                configured_option_quote_concurrency_per_symbol: 1,
                symbols_completed: 1,
                underlying_snapshots_completed: 1,
                option_quotes_completed: 1,
                symbols_per_second: 1.0,
                underlying_snapshots_per_second: 1.0,
                option_quotes_per_second: 1.0,
            },
            human_log_path: None,
            notes: Vec::new(),
        };

        let trade_log = render_trade_log(&report);

        assert!(trade_log.contains("Trades Filled this Run:"));
        assert!(trade_log.contains("- none"));
        assert!(trade_log.contains("Current Positions (after this run):"));
        assert!(trade_log.contains("MSFT | status=open-covered-call"));
        assert!(trade_log.contains("placed_at="));
        assert!(!trade_log.contains("AAPL | status=combo-submitted"));
    }

    #[test]
    fn action_log_includes_submitted_trade_records_even_when_not_filled() {
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
            candidates_ranked: 0,
            accepted_candidates: Vec::new(),
            guardrail_rejections: Vec::new(),
            proposed_orders: Vec::new(),
            execution_records: vec![ExecutionRecord {
                symbol: "AAPL".to_string(),
                status: "combo-submitted".to_string(),
                submission_mode: "paper".to_string(),
                note: "submitted combo order".to_string(),
                legs: vec![
                    ExecutionLegRecord {
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
                    },
                    ExecutionLegRecord {
                        leg_symbol: "AAPL".to_string(),
                        instrument_type: InstrumentType::Option,
                        action: TradeAction::Sell,
                        quantity: 1,
                        order_id: Some(10),
                        submission_status: "Submitted".to_string(),
                        limit_price: Some(11.0),
                        filled_quantity: 0.0,
                        average_fill_price: None,
                        execution_ids: Vec::new(),
                        note: "awaiting fill".to_string(),
                    },
                ],
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
                broker_event_log_path: None,
                broker_event_timeline: Vec::new(),
                execution_step_timings: Vec::new(),
            }],
            open_positions: Vec::new(),
            paper_trade_lifecycle: Vec::new(),
            live_data_requested: true,
            non_live_symbols: Vec::new(),
            capital_source_details: CapitalSourceDetails {
                configured_source: "available_funds".to_string(),
                preview: CapitalAllocationView {
                    source: "available_funds".to_string(),
                    reported_amount: Some(10_000.0),
                    reserve_ratio: 0.05,
                    reserve_amount: 500.0,
                    cash_after_reserve: 9_500.0,
                    deployment_budget: 10_000.0,
                    deployable_cash: 9_500.0,
                    max_cash_per_symbol: 2_000.0,
                },
                routed_orders: CapitalAllocationView {
                    source: "available_funds".to_string(),
                    reported_amount: Some(10_000.0),
                    reserve_ratio: 0.05,
                    reserve_amount: 500.0,
                    cash_after_reserve: 9_500.0,
                    deployment_budget: 10_000.0,
                    deployable_cash: 9_500.0,
                    max_cash_per_symbol: 2_000.0,
                },
            },
            allocation_summary: AllocationSummary {
                candidate_symbols_considered: 0,
                selected_symbols: 0,
                total_lots: 0,
                existing_exposure_cash: 0.0,
                allocated_cash: 0.0,
                remaining_cash: 10_000.0,
            },
            warnings: Vec::new(),
            diagnostic_log: Vec::new(),
            action_log: vec![
                "AAPL: blocked duplicate paper submission for prior intent.".to_string(),
            ],
            api_log: Vec::new(),
            timing_metrics: CycleTimingMetrics {
                total_elapsed_ms: 1,
                market_data_elapsed_ms: 1,
            },
            throughput_counters: CycleThroughputCounters {
                configured_symbol_concurrency: 1,
                configured_option_quote_concurrency_per_symbol: 1,
                symbols_completed: 1,
                underlying_snapshots_completed: 1,
                option_quotes_completed: 1,
                symbols_per_second: 1.0,
                underlying_snapshots_per_second: 1.0,
                option_quotes_per_second: 1.0,
            },
            human_log_path: None,
            notes: Vec::new(),
        };

        let action_log = render_cycle_action_log(&report);

        assert!(action_log.contains("Recorded actions:"));
        assert!(action_log.contains("Submitted trade records:"));
        assert!(action_log.contains("AAPL | status=combo-submitted"));
    }

    #[test]
    fn status_log_surfaces_at_a_glance_rollups_and_ledger() {
        let report = StatusReport {
            account: "DU1234567".to_string(),
            endpoint: "127.0.0.1:4002".to_string(),
            platform: "IB Gateway".to_string(),
            runtime_mode: "Paper".to_string(),
            connect_on_start: true,
            account_state: crate::models::AccountState {
                account: "DU1234567".to_string(),
                available_funds: Some(12_500.0),
                buying_power: Some(25_000.0),
                net_liquidation: Some(18_000.0),
            },
            capital_source: "available_funds".to_string(),
            deployment_budget: 10_000.0,
            open_orders: vec![BrokerOpenOrder {
                account: "DU1234567".to_string(),
                order_id: 11,
                client_id: 7,
                perm_id: 99,
                order_ref: "deepitm-buywrite:AAPL:combo:buywrite".to_string(),
                symbol: "AAPL".to_string(),
                security_type: "BAG".to_string(),
                action: "BUY".to_string(),
                total_quantity: 1.0,
                order_type: "LMT".to_string(),
                limit_price: Some(89.25),
                status: "Submitted".to_string(),
                filled_quantity: 0.0,
                remaining_quantity: 1.0,
            }],
            completed_orders: vec![BrokerCompletedOrder {
                account: "DU1234567".to_string(),
                order_id: 10,
                client_id: 7,
                perm_id: 98,
                symbol: "MSFT".to_string(),
                security_type: "BAG".to_string(),
                action: "BUY".to_string(),
                total_quantity: 1.0,
                order_type: "LMT".to_string(),
                limit_price: Some(45.10),
                status: "Filled".to_string(),
                completed_status: "Filled".to_string(),
                reject_reason: String::new(),
                warning_text: String::new(),
                completed_time: "20260421 14:00:00 America/Denver".to_string(),
            }],
            open_positions: vec![OpenPositionState {
                symbol: "ORCL".to_string(),
                stock_shares: 100.0,
                short_call_contracts: 1.0,
                average_stock_cost: Some(100.0),
            }],
            paper_trade_lifecycle: vec![PaperTradeLifecycleRecord {
                symbol: "ORCL".to_string(),
                intent_key: "intent".to_string(),
                status: "open-covered-call".to_string(),
                first_recorded_at: Utc::now(),
                last_updated_at: Utc::now(),
                hold_until_close: true,
                stock_order_id: Some(11),
                short_call_order_id: Some(11),
                stock_filled_shares: 100.0,
                short_call_filled_contracts: 1.0,
                stock_average_fill_price: Some(100.0),
                short_call_average_fill_price: Some(11.0),
                entry_net_debit: Some(8_900.0),
                expected_profit: Some(100.0),
                observed_stock_shares: 100.0,
                observed_short_call_contracts: 1.0,
                note: "tracked".to_string(),
            }],
            diagnostic_log: vec!["status diagnostic".to_string()],
            action_log: vec!["AAPL: refreshed from broker snapshot.".to_string()],
            api_log: vec!["Fetched status snapshot from IBKR.".to_string()],
        };

        let log = render_status_log(&test_config(), &report);

        assert!(log.contains("At a glance: 1 open position group(s), 1 open order(s), 1 completed order(s), 1 paper ledger record(s)"));
        assert!(log.contains("Account summary: buying_power=Some(25000.0), available_funds=Some(12500.0), net_liquidation=Some(18000.0)"));
        assert!(log.contains("Open-order statuses: Submitted=1"));
        assert!(log.contains("Ledger statuses: open-covered-call=1"));
        assert!(log.contains("Paper Lifecycle Ledger:"));
        assert!(log.contains("Recent Completed Orders:"));
    }

    #[test]
    fn cycle_output_writes_logs_into_requested_log_subfolders() {
        let artifact_dir = TestArtifactDir::new("cycle-output");
        let mut config = test_config();
        config.logs.diagnostic_log = true;
        config.logs.action_log = true;
        config.logs.trade_log = true;
        config.logs.api_log = true;

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
            accepted_candidates: Vec::new(),
            guardrail_rejections: Vec::new(),
            proposed_orders: Vec::new(),
            execution_records: vec![ExecutionRecord {
                symbol: "MSFT".to_string(),
                status: "deep-itm-covered-call-open".to_string(),
                submission_mode: "paper".to_string(),
                note: "filled".to_string(),
                legs: vec![
                    ExecutionLegRecord {
                        leg_symbol: "MSFT".to_string(),
                        instrument_type: InstrumentType::Stock,
                        action: TradeAction::Buy,
                        quantity: 100,
                        order_id: Some(11),
                        submission_status: "Filled".to_string(),
                        limit_price: Some(95.0),
                        filled_quantity: 100.0,
                        average_fill_price: Some(95.0),
                        execution_ids: vec!["stock-fill".to_string()],
                        note: "filled".to_string(),
                    },
                    ExecutionLegRecord {
                        leg_symbol: "MSFT 20260515 90 C".to_string(),
                        instrument_type: InstrumentType::Option,
                        action: TradeAction::Sell,
                        quantity: 1,
                        order_id: Some(11),
                        submission_status: "Filled".to_string(),
                        limit_price: Some(6.5),
                        filled_quantity: 1.0,
                        average_fill_price: Some(6.5),
                        execution_ids: vec!["option-fill".to_string()],
                        note: "filled".to_string(),
                    },
                ],
                fill_reconciliation: Some(FillReconciliationRecord {
                    stock_filled_shares: 100.0,
                    stock_average_fill_price: Some(95.0),
                    short_call_filled_contracts: 1.0,
                    short_call_average_fill_price: Some(6.5),
                    total_commission: Some(1.9),
                    eligible_for_short_call: false,
                    uncovered_shares: 0.0,
                    status: "deep-itm-covered-call-open".to_string(),
                    note: "filled".to_string(),
                }),
                broker_event_log_path: None,
                broker_event_timeline: Vec::new(),
                execution_step_timings: Vec::new(),
            }],
            open_positions: vec![OpenPositionState {
                symbol: "MSFT".to_string(),
                stock_shares: 100.0,
                short_call_contracts: 1.0,
                average_stock_cost: Some(95.0),
            }],
            paper_trade_lifecycle: vec![PaperTradeLifecycleRecord {
                symbol: "MSFT".to_string(),
                intent_key: "msft".to_string(),
                status: "open-covered-call".to_string(),
                first_recorded_at: Utc::now(),
                last_updated_at: Utc::now(),
                hold_until_close: true,
                stock_order_id: Some(11),
                short_call_order_id: Some(11),
                stock_filled_shares: 100.0,
                short_call_filled_contracts: 1.0,
                stock_average_fill_price: Some(95.0),
                short_call_average_fill_price: Some(6.5),
                entry_net_debit: Some(8_850.0),
                expected_profit: Some(150.0),
                observed_stock_shares: 100.0,
                observed_short_call_contracts: 1.0,
                note: "opened".to_string(),
            }],
            live_data_requested: true,
            non_live_symbols: Vec::new(),
            capital_source_details: CapitalSourceDetails {
                configured_source: "available_funds".to_string(),
                preview: CapitalAllocationView {
                    source: "available_funds".to_string(),
                    reported_amount: Some(10_000.0),
                    reserve_ratio: 0.05,
                    reserve_amount: 500.0,
                    cash_after_reserve: 9_500.0,
                    deployment_budget: 10_000.0,
                    deployable_cash: 9_500.0,
                    max_cash_per_symbol: 2_000.0,
                },
                routed_orders: CapitalAllocationView {
                    source: "available_funds".to_string(),
                    reported_amount: Some(10_000.0),
                    reserve_ratio: 0.05,
                    reserve_amount: 500.0,
                    cash_after_reserve: 9_500.0,
                    deployment_budget: 10_000.0,
                    deployable_cash: 9_500.0,
                    max_cash_per_symbol: 2_000.0,
                },
            },
            allocation_summary: AllocationSummary {
                candidate_symbols_considered: 1,
                selected_symbols: 1,
                total_lots: 1,
                existing_exposure_cash: 0.0,
                allocated_cash: 9_500.0,
                remaining_cash: 500.0,
            },
            warnings: Vec::new(),
            diagnostic_log: vec!["diagnostic".to_string()],
            action_log: vec!["action".to_string()],
            api_log: vec!["api".to_string()],
            timing_metrics: CycleTimingMetrics {
                total_elapsed_ms: 1,
                market_data_elapsed_ms: 1,
            },
            throughput_counters: CycleThroughputCounters {
                configured_symbol_concurrency: 1,
                configured_option_quote_concurrency_per_symbol: 1,
                symbols_completed: 1,
                underlying_snapshots_completed: 1,
                option_quotes_completed: 1,
                symbols_per_second: 1.0,
                underlying_snapshots_per_second: 1.0,
                option_quotes_per_second: 1.0,
            },
            human_log_path: None,
            notes: Vec::new(),
        };

        let outputs = write_cycle_outputs_in(artifact_dir.path(), &config, &report).unwrap();

        assert!(
            outputs
                .diagnostic_log_path
                .as_ref()
                .is_some_and(|path| path.starts_with(artifact_dir.path().join("diagnostic")))
        );
        assert!(
            outputs
                .diagnostic_log_path
                .as_ref()
                .and_then(|path| path.file_name())
                .is_some_and(|name| name.to_string_lossy().contains("_Diagnostic_Log.json"))
        );
        assert!(
            outputs
                .action_log_path
                .as_ref()
                .is_some_and(|path| path.starts_with(artifact_dir.path().join("action")))
        );
        assert!(
            outputs
                .action_log_path
                .as_ref()
                .and_then(|path| path.file_name())
                .is_some_and(|name| name.to_string_lossy().contains("_Action_Log.txt"))
        );
        assert!(
            outputs
                .trade_log_path
                .as_ref()
                .is_some_and(|path| path.starts_with(artifact_dir.path().join("trades")))
        );
        assert!(
            outputs
                .trade_log_path
                .as_ref()
                .and_then(|path| path.file_name())
                .is_some_and(|name| name.to_string_lossy().contains("_Trade_Log.txt"))
        );
        assert!(
            outputs
                .api_log_path
                .as_ref()
                .is_some_and(|path| path.starts_with(artifact_dir.path().join("api")))
        );
        assert!(
            outputs
                .api_log_path
                .as_ref()
                .and_then(|path| path.file_name())
                .is_some_and(|name| name.to_string_lossy().contains("_API_Log.txt"))
        );
    }

    #[test]
    fn status_output_only_writes_enabled_log_types() {
        let artifact_dir = TestArtifactDir::new("status-output");
        let mut config = test_config();
        config.logs.diagnostic_log = false;
        config.logs.action_log = true;
        config.logs.trade_log = false;
        config.logs.api_log = false;

        let report = StatusReport {
            account: "DU1234567".to_string(),
            endpoint: "127.0.0.1:4002".to_string(),
            platform: "IB Gateway".to_string(),
            runtime_mode: "Paper".to_string(),
            connect_on_start: true,
            account_state: AccountState {
                account: "DU1234567".to_string(),
                available_funds: Some(12_500.0),
                buying_power: Some(25_000.0),
                net_liquidation: Some(18_000.0),
            },
            capital_source: "available_funds".to_string(),
            deployment_budget: 10_000.0,
            open_orders: Vec::new(),
            completed_orders: Vec::new(),
            open_positions: Vec::new(),
            paper_trade_lifecycle: Vec::new(),
            diagnostic_log: vec!["status diagnostic".to_string()],
            action_log: vec!["status action".to_string()],
            api_log: vec!["status api".to_string()],
        };

        let outputs = write_status_outputs_in(artifact_dir.path(), &config, &report).unwrap();

        assert!(outputs.diagnostic_log_path.is_none());
        assert!(outputs.trade_log_path.is_none());
        assert!(outputs.api_log_path.is_none());
        assert!(
            outputs
                .action_log_path
                .as_ref()
                .is_some_and(|path| path.starts_with(artifact_dir.path().join("action")))
        );
    }
}
