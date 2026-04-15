use anyhow::Result;
use async_trait::async_trait;

use crate::{
    config::{AppConfig, RuntimeMode},
    models::{ExecutionRecord, OrderIntent},
};

#[async_trait(?Send)]
pub trait OrderExecutor {
    async fn execute(&self, intents: &[OrderIntent], config: &AppConfig) -> Result<Vec<ExecutionRecord>>;
}

#[derive(Debug, Default)]
pub struct GuardedDryRunExecutor;

#[async_trait(?Send)]
impl OrderExecutor for GuardedDryRunExecutor {
    async fn execute(
        &self,
        intents: &[OrderIntent],
        config: &AppConfig,
    ) -> Result<Vec<ExecutionRecord>> {
        let mut records = Vec::new();

        for intent in intents {
            let note = if config.risk.enable_live_orders {
                "live-order execution is intentionally disabled in this milestone".to_string()
            } else if config.risk.enable_paper_orders
                && matches!(config.mode, RuntimeMode::Paper)
                && !config.read_only
            {
                "paper-order flag is enabled; this milestone still keeps buy-write execution in dry-run mode until staged submission is hardened".to_string()
            } else {
                "proposed dry-run order only; no broker submission attempted".to_string()
            };

            records.push(ExecutionRecord {
                symbol: intent.symbol.clone(),
                status: "dry-run".to_string(),
                note,
            });
        }

        if intents.is_empty() {
            records.push(ExecutionRecord {
                symbol: "N/A".to_string(),
                status: "noop".to_string(),
                note: "no order intents passed all guardrails in this cycle".to_string(),
            });
        }

        Ok(records)
    }
}
