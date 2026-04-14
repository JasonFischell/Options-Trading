use anyhow::{Context, Result};
use ibapi::Client;
use tracing::info;

use crate::config::AppConfig;

#[derive(Debug, Clone)]
pub struct IbkrClientDescriptor {
    pub endpoint: String,
    pub client_id: i32,
    pub account: String,
    pub read_only: bool,
}

impl From<&AppConfig> for IbkrClientDescriptor {
    fn from(config: &AppConfig) -> Self {
        Self {
            endpoint: config.endpoint(),
            client_id: config.client_id,
            account: config.account.clone(),
            read_only: config.read_only,
        }
    }
}

pub async fn probe_connection(config: &AppConfig) -> Result<()> {
    let endpoint = config.endpoint();
    info!("probing IBKR connectivity at {}", endpoint);

    let client = Client::connect(&endpoint, config.client_id)
        .await
        .with_context(|| format!("failed to connect to IBKR at {endpoint}"))?;

    let server_time = client
        .server_time()
        .await
        .context("connected to IBKR but failed to request server time")?;

    info!("connected to IBKR, server time is {}", server_time);
    Ok(())
}
