use anyhow::Result;
use dotenvy::dotenv;
use ibkr_options_engine::{
    config::AppConfig,
    ibkr::{connect, fetch_completed_orders, fetch_open_orders, fetch_positions, log_server_time},
    paper_state::PaperTradeLedger,
    state::summarize_open_positions,
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let config = AppConfig::from_env()?;
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

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "account": config.account,
            "endpoint": config.endpoint(),
            "open_orders": open_orders,
            "completed_orders": completed_orders,
            "open_positions": open_positions,
            "paper_trade_lifecycle_after_reconcile": ledger.snapshot(),
            "action_log": action_log,
        }))?
    );

    Ok(())
}
