use anyhow::Result;
use clap::Parser;
use dotenvy::dotenv;
use ibkr_options_engine::{
    cli::{Cli, Command, ConfigArgs},
    config::AppConfig,
    execution::GuardedPaperOrderExecutor,
    ibkr::{connect, fetch_completed_orders, fetch_open_orders, fetch_positions, log_server_time},
    market_data::{IbkrMarketDataProvider, load_universe},
    models::StatusReport,
    paper_state::PaperTradeLedger,
    reporting::{render_status_log, write_cycle_outputs, write_status_outputs},
    scanner::{build_scan_plan, run_scan_cycle},
    state::summarize_open_positions,
};
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
    for startup_warning in &config.startup_warnings {
        warn!("{startup_warning}");
    }
    info!("{}", config.connection_guidance());

    if !config.connect_on_start {
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
    let executor = GuardedPaperOrderExecutor::from_client(provider.shared_client());
    let mut report = run_scan_cycle(&provider, &executor, &config).await?;
    let (human_log_path, json_report_path) = write_cycle_outputs(&config, &report)?;
    report.human_log_path = Some(human_log_path.display().to_string());

    info!(
        human_log_path = %human_log_path.display(),
        json_report_path = %json_report_path.display(),
        "wrote scan artifacts"
    );
    println!("{}", serde_json::to_string_pretty(&report)?);
    println!(
        "Human-readable log: {}\nJSON report: {}",
        human_log_path.display(),
        json_report_path.display()
    );

    Ok(())
}

async fn run_status(args: ConfigArgs) -> Result<()> {
    let config = AppConfig::from_path(args.config.as_deref())?;
    for startup_warning in &config.startup_warnings {
        warn!("{startup_warning}");
    }

    if !config.connect_on_start {
        warn!("IBKR_CONNECT_ON_START is false; status output requires a broker connection");
        return Ok(());
    }

    let client = connect(&config.endpoint(), config.client_id).await?;
    log_server_time(&client).await?;

    let positions = fetch_positions(&client).await?;
    let open_positions = summarize_open_positions(&positions);
    let open_orders = fetch_open_orders(&client, &config.account).await?;
    let completed_orders = fetch_completed_orders(&client, &config.account).await?;
    let mut ledger = PaperTradeLedger::load(&config)?;
    let mut action_log = Vec::new();

    ledger.reconcile_with_positions(&open_positions, &mut action_log);
    ledger.reconcile_with_broker_orders(&open_orders, &completed_orders, &mut action_log);
    ledger.persist(&config)?;

    let report = StatusReport {
        account: config.account.clone(),
        endpoint: config.endpoint(),
        platform: config.platform.label().to_string(),
        runtime_mode: format!("{:?}", config.mode),
        connect_on_start: config.connect_on_start,
        capital_source: config.allocation.capital_source.label().to_string(),
        deployment_budget: config.allocation.deployment_budget,
        open_orders,
        completed_orders,
        open_positions,
        paper_trade_lifecycle: ledger.snapshot(),
        action_log,
    };
    let (human_log_path, json_report_path) = write_status_outputs(&config, &report)?;

    info!(
        human_log_path = %human_log_path.display(),
        json_report_path = %json_report_path.display(),
        "wrote status artifacts"
    );
    println!("{}", render_status_log(&config, &report));
    println!(
        "Human-readable log: {}\nJSON report: {}",
        human_log_path.display(),
        json_report_path.display()
    );

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("ibkr_options_engine=info"));

    fmt().with_env_filter(filter).init();
}
