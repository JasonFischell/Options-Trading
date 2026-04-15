use anyhow::Result;
use dotenvy::dotenv;
use ibkr_options_engine::{
    config::AppConfig,
    ibkr::{
        IbkrClientDescriptor, connect, log_account_summary, log_option_chain_for_underlying,
        log_server_time, log_stock_contract_details, request_snapshot, resolve_option_contract,
        resolve_primary_stock_contract_id, select_option_contract, switch_to_frozen_market_data,
    },
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
        let client = connect(&descriptor.endpoint, descriptor.client_id).await?;
        log_server_time(&client).await?;
        log_account_summary(&client).await?;
        log_stock_contract_details(&client, &plan.symbols).await?;
        if let Some(symbol) = plan.symbols.first() {
            let contract_id = resolve_primary_stock_contract_id(&client, symbol).await?;
            let option_chains =
                log_option_chain_for_underlying(&client, symbol, contract_id).await?;
            switch_to_frozen_market_data(&client).await?;

            let underlying_contract = ibapi::prelude::Contract::stock(symbol).build();
            let underlying_snapshot = request_snapshot(
                &client,
                &underlying_contract,
                &[],
                &format!("{symbol} underlying"),
            )
            .await?;

            if let Some(reference_price) = underlying_snapshot.reference_price() {
                let selected = select_option_contract(symbol, &option_chains, reference_price)?;
                println!(
                    "Selected option candidate: symbol={} expiry={} strike={} right={} chain_exchange={} trading_class={} underlying_contract_id={}",
                    selected.symbol,
                    selected.expiration,
                    selected.strike,
                    selected.right,
                    selected.exchange,
                    selected.trading_class,
                    selected.underlying_contract_id
                );

                let option_contract = resolve_option_contract(&client, &selected).await?;
                let option_label = format!(
                    "{} {} {} {}",
                    selected.symbol, selected.expiration, selected.right, selected.strike
                );
                let _option_snapshot = request_snapshot(
                    &client,
                    &option_contract,
                    &["100", "101", "104", "106"],
                    &option_label,
                )
                .await?;
            } else {
                println!(
                    "Unable to derive a reference price for {}; skipping concrete option selection.",
                    symbol
                );
            }
        }
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
