use anyhow::Result;
use dotenvy::dotenv;
use ibkr_options_engine::{
    config::AppConfig,
    ibkr::{IbkrClientDescriptor, probe_connection},
    scanner::build_scan_plan,
};
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    init_tracing();

    let config = AppConfig::from_env()?;
    let descriptor = IbkrClientDescriptor::from(&config);
    let plan = build_scan_plan(&config);

    info!(
        endpoint = %descriptor.endpoint,
        client_id = descriptor.client_id,
        account = %descriptor.account,
        read_only = descriptor.read_only,
        symbols = ?plan.symbols,
        execution_mode = plan.execution_mode,
        "loaded IBKR engine configuration"
    );

    if config.connect_on_start {
        probe_connection(&config).await?;
    } else {
        warn!("IBKR_CONNECT_ON_START is false; skipping live connectivity probe");
    }

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("ibkr_options_engine=info"));

    fmt().with_env_filter(filter).init();
}
