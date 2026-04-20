use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::Local;

use crate::{config::AppConfig, models::CycleReport};

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
            "Candidates ranked: {}, proposed orders: {}, execution records: {}",
            report.candidates_ranked,
            report.proposed_orders.len(),
            report.execution_records.len()
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

    if !report.warnings.is_empty() {
        lines.push(String::new());
        lines.push("Warnings:".to_string());
        for warning in &report.warnings {
            lines.push(format!("- {warning}"));
        }
    }

    if !report.action_log.is_empty() {
        lines.push(String::new());
        lines.push("Actions:".to_string());
        for action in &report.action_log {
            lines.push(format!("- {action}"));
        }
    }

    if !report.guardrail_rejections.is_empty() {
        lines.push(String::new());
        lines.push("Guardrail rejections:".to_string());
        for rejection in &report.guardrail_rejections {
            lines.push(format!(
                "- {} [{}] {}",
                rejection.symbol, rejection.stage, rejection.reason
            ));
        }
    }

    if !report.paper_trade_lifecycle.is_empty() {
        lines.push(String::new());
        lines.push("Paper lifecycle:".to_string());
        for lifecycle in &report.paper_trade_lifecycle {
            lines.push(format!(
                "- {} [{}] stock_fill={:.0} short_call_fill={:.0} observed_shares={:.0} observed_short_calls={:.0} hold_until_close={} note={}",
                lifecycle.symbol,
                lifecycle.status,
                lifecycle.stock_filled_shares,
                lifecycle.short_call_filled_contracts,
                lifecycle.observed_stock_shares,
                lifecycle.observed_short_call_contracts,
                lifecycle.hold_until_close,
                lifecycle.note
            ));
        }
    }

    let broker_event_records = report
        .execution_records
        .iter()
        .filter(|record| {
            record.broker_event_log_path.is_some() || !record.broker_event_timeline.is_empty()
        })
        .collect::<Vec<_>>();
    if !broker_event_records.is_empty() {
        lines.push(String::new());
        lines.push("Broker event timelines:".to_string());
        for record in broker_event_records {
            let log_path = record
                .broker_event_log_path
                .as_deref()
                .unwrap_or("not persisted");
            lines.push(format!(
                "- {} [{}] {} events logged at {}",
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

    if !report.notes.is_empty() {
        lines.push(String::new());
        lines.push("Notes:".to_string());
        for note in &report.notes {
            lines.push(format!("- {note}"));
        }
    }

    lines.join("\n")
}
