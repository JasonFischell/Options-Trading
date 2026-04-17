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
        config.platform.label().to_ascii_lowercase().replace(' ', "-"),
        config.account
    );

    let human_log_path = logs_dir.join(format!("{base_name}.log"));
    let json_report_path = logs_dir.join(format!("{base_name}.json"));

    fs::write(&human_log_path, render_human_log(config, report))
        .with_context(|| format!("failed to write human log to {}", human_log_path.display()))?;
    fs::write(&json_report_path, serde_json::to_string_pretty(report)?)
        .with_context(|| format!("failed to write JSON report to {}", json_report_path.display()))?;

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
            "Universe: {} scanned, {} underlying snapshots, {} option quotes",
            report.symbols_scanned, report.underlying_snapshots, report.option_quotes_considered
        ),
        format!(
            "Candidates ranked: {}, proposed orders: {}, execution records: {}",
            report.candidates_ranked,
            report.proposed_orders.len(),
            report.execution_records.len()
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

    if !report.notes.is_empty() {
        lines.push(String::new());
        lines.push("Notes:".to_string());
        for note in &report.notes {
            lines.push(format!("- {note}"));
        }
    }

    lines.join("\n")
}
