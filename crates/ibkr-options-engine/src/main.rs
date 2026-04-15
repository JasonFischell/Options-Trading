use anyhow::Result;
use dotenvy::dotenv;
use ibkr_options_engine::{
    config::AppConfig,
    execution::GuardedPaperOrderExecutor,
    market_data::{IbkrMarketDataProvider, load_universe},
    scanner::{build_scan_plan, run_scan_cycle},
};
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    init_tracing();

    let config = AppConfig::from_env()?;
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
        symbols = ?plan.symbols,
        run_mode = plan.run_mode,
        execution_mode = plan.execution_mode,
        "loaded IBKR engine configuration"
    );
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
    let report = run_scan_cycle(&provider, &executor, &config).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("ibkr_options_engine=info"));

    fmt().with_env_filter(filter).init();
}
