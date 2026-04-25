use anyhow::{Result, bail};
use clap::Parser;
use dotenvy::dotenv;
use ibkr_options_engine::{
    cli::{Cli, Command, ConfigArgs},
    config::{AppConfig, RuntimeMode},
    execution::GuardedPaperOrderExecutor,
    ibkr::{
        connect, fetch_account_state, fetch_completed_orders, fetch_open_orders, fetch_positions,
        log_server_time,
    },
    market_data::{IbkrMarketDataProvider, load_universe},
    models::StatusReport,
    paper_state::PaperTradeLedger,
    reporting::{
        render_filled_trade_summary, render_left_open_trade_summary, write_cycle_outputs,
        write_status_outputs,
    },
    scanner::{build_scan_plan, run_scan_cycle},
    state::summarize_open_positions,
};
use std::io::{self, Write};
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    init_tracing();

    let cli = Cli::parse();
    match cli.command.unwrap_or(Command::Scan(ConfigArgs::default())) {
        Command::Scan(args) => run_scan(args).await,
        Command::Status(args) => run_status(args).await,
    }
}

async fn run_scan(args: ConfigArgs) -> Result<()> {
    let config = AppConfig::from_path(args.config.as_deref())?;
    print_preflight_messages(&config);
    confirm_live_trading_intent(&config)?;
    let universe = load_universe(&config)?;
    let symbols = universe
        .iter()
        .map(|record| record.symbol.clone())
        .collect::<Vec<_>>();
    let plan = build_scan_plan(&config, &symbols);

    info!(
        platform = ?config.platform,
        endpoint = %config.endpoint(),
        client_id = config.client_id,
        account = %config.account,
        read_only = config.read_only,
        universe_source = %config.universe_source_label(),
        symbols = ?plan.symbols,
        run_mode = plan.run_mode,
        execution_mode = plan.execution_mode,
        "loaded IBKR engine configuration"
    );
    info!("{}", config.connection_guidance());

    if !config.connect_on_start {
        if config.logs.print_statements {
            println!("WARN: connect_on_start is false; skipping broker connection.");
        }
        warn!("IBKR_CONNECT_ON_START is false; skipping live connectivity probe");
        return Ok(());
    }

    let provider = IbkrMarketDataProvider::connect(&config)
        .await
        .map_err(|error| {
            anyhow::anyhow!(
                "{}\nConnection checklist: {}\nIf IB Gateway is already open, confirm API sockets are enabled and that the configured port matches the Gateway session.",
                error,
                config.connection_guidance()
            )
        })?;
    if config.logs.print_statements {
        println!(
            "INFO: connected to {} at {}",
            config.platform.label(),
            config.endpoint()
        );
    }
    let executor = GuardedPaperOrderExecutor::from_client(provider.shared_client());
    let mut report = run_scan_cycle(&provider, &executor, &config).await?;
    let outputs = write_cycle_outputs(&config, &report)?;
    report.human_log_path = outputs
        .diagnostic_log_path
        .as_ref()
        .or(outputs.action_log_path.as_ref())
        .or(outputs.trade_log_path.as_ref())
        .or(outputs.api_log_path.as_ref())
        .map(|path| path.display().to_string());

    info!(
        diagnostic_log_path = ?outputs.diagnostic_log_path.as_ref().map(|path| path.display().to_string()),
        action_log_path = ?outputs.action_log_path.as_ref().map(|path| path.display().to_string()),
        trade_log_path = ?outputs.trade_log_path.as_ref().map(|path| path.display().to_string()),
        api_log_path = ?outputs.api_log_path.as_ref().map(|path| path.display().to_string()),
        "wrote scan artifacts"
    );
    if config.logs.print_statements {
        println!(
            "COMPLETE: scanned {} stock(s), captured {} underlying snapshot(s), considered {} option quote(s), proposed {} order(s), recorded {} execution record(s).",
            report.symbols_scanned,
            report.underlying_snapshots,
            report.option_quotes_considered,
            report.proposed_orders.len(),
            report.execution_records.len()
        );
        for warning in &report.warnings {
            println!("WARN: {warning}");
        }
        for line in outputs.terminal_lines() {
            println!("{line}");
        }
        let filled_trade_summaries = render_filled_trade_summary(&report);
        let left_open_trade_summaries = render_left_open_trade_summary(&report);
        println!("Trades executed this run:");
        println!("FILLED:");
        if filled_trade_summaries.is_empty() {
            println!("- none");
        } else {
            for trade in filled_trade_summaries {
                println!("- {trade}");
            }
        }
        println!("LEFT OPEN:");
        if left_open_trade_summaries.is_empty() {
            println!("- none");
        } else {
            for trade in left_open_trade_summaries {
                println!("- {trade}");
            }
        }
    }

    Ok(())
}

async fn run_status(args: ConfigArgs) -> Result<()> {
    let config = AppConfig::from_path(args.config.as_deref())?;
    print_preflight_messages(&config);
    confirm_live_trading_intent(&config)?;

    if !config.connect_on_start {
        if config.logs.print_statements {
            println!("WARN: connect_on_start is false; status requires a broker connection.");
        }
        warn!("IBKR_CONNECT_ON_START is false; status output requires a broker connection");
        return Ok(());
    }

    let client = connect(&config.endpoint(), config.client_id).await?;
    let server_time = log_server_time(&client).await?;
    if config.logs.print_statements {
        println!("INFO: connected to IBKR, server time {}", server_time);
    }

    let account_state = fetch_account_state(&client, &config.account).await?;
    let positions = fetch_positions(&client).await?;
    let open_positions = summarize_open_positions(&positions);
    let open_orders = fetch_open_orders(&client, &config.account).await?;
    let completed_orders = fetch_completed_orders(&client, &config.account).await?;
    let mut ledger = PaperTradeLedger::load(&config)?;
    let mut diagnostic_log = config.startup_warnings.clone();
    let mut action_log = Vec::new();
    let mut api_log = vec![
        format!(
            "Connected to {} at {}.",
            config.platform.label(),
            config.endpoint()
        ),
        format!("Server time reported by IBKR: {server_time}."),
        format!("Fetched {} inventory row(s).", positions.len()),
        format!(
            "Fetched {} open order(s) and {} completed order(s).",
            open_orders.len(),
            completed_orders.len()
        ),
    ];

    ledger.reconcile_with_positions(&open_positions, &mut action_log);
    ledger.reconcile_with_broker_orders(&open_orders, &completed_orders, &mut action_log);
    ledger.persist(&config)?;
    diagnostic_log.push(format!(
        "Status snapshot captured {} grouped open position(s).",
        open_positions.len()
    ));
    api_log.push("Persisted refreshed paper-trade ledger state.".to_string());

    let report = StatusReport {
        account: config.account.clone(),
        endpoint: config.endpoint(),
        platform: config.platform.label().to_string(),
        runtime_mode: format!("{:?}", config.mode),
        connect_on_start: config.connect_on_start,
        account_state,
        capital_source: config.allocation.capital_source.label().to_string(),
        deployment_budget: config.allocation.deployment_budget,
        open_orders,
        completed_orders,
        open_positions,
        paper_trade_lifecycle: ledger.snapshot(),
        diagnostic_log,
        action_log,
        api_log,
    };
    let outputs = write_status_outputs(&config, &report)?;

    info!(
        diagnostic_log_path = ?outputs.diagnostic_log_path.as_ref().map(|path| path.display().to_string()),
        action_log_path = ?outputs.action_log_path.as_ref().map(|path| path.display().to_string()),
        api_log_path = ?outputs.api_log_path.as_ref().map(|path| path.display().to_string()),
        "wrote status artifacts"
    );
    if config.logs.print_statements {
        println!(
            "STATUS: {} open position group(s), {} open order(s), {} completed order(s), {} lifecycle record(s).",
            report.open_positions.len(),
            report.open_orders.len(),
            report.completed_orders.len(),
            report.paper_trade_lifecycle.len()
        );
        for line in outputs.terminal_lines() {
            println!("{line}");
        }
    }

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("ibkr_options_engine=warn"));

    fmt().with_env_filter(filter).init();
}

fn print_preflight_messages(config: &AppConfig) {
    if !config.logs.print_statements {
        return;
    }

    match (config.risk.enable_live_orders, config.mode) {
        (true, _) | (_, RuntimeMode::Live) => println!(
            "WARN: LIVE-TRADING SETTINGS DETECTED. Automated live order routing remains disabled in this milestone."
        ),
        _ if config.guarded_paper_submission_enabled() => println!(
            "WARN: paper-trading is enabled; qualifying orders may be routed to the IBKR paper account."
        ),
        _ => println!("INFO: read-only / analysis-only mode; no broker orders will be submitted."),
    }

    if config.guarded_paper_submission_enabled() && !config.prefers_live_market_data() {
        println!(
            "WARN: paper submission requires live market data; delayed or frozen symbols will be blocked."
        );
    }
}

fn confirm_live_trading_intent(config: &AppConfig) -> Result<()> {
    if !(config.risk.enable_live_orders || matches!(config.mode, RuntimeMode::Live)) {
        return Ok(());
    }

    print!("Do you want to enable trading with live funds? Type Y / N: ");
    io::stdout().flush()?;

    let mut response = String::new();
    io::stdin().read_line(&mut response)?;
    if confirm_live_trading_intent_response(&response) {
        Ok(())
    } else {
        bail!("aborted because live-funds trading was not confirmed")
    }
}

fn confirm_live_trading_intent_response(response: &str) -> bool {
    response.trim().eq_ignore_ascii_case("Y")
}

#[cfg(test)]
mod tests {
    use super::confirm_live_trading_intent_response;

    #[test]
    fn accepts_live_trading_confirmation_only_for_y() {
        assert!(confirm_live_trading_intent_response("Y"));
        assert!(confirm_live_trading_intent_response(" y "));
        assert!(!confirm_live_trading_intent_response("N"));
        assert!(!confirm_live_trading_intent_response(""));
    }
}
