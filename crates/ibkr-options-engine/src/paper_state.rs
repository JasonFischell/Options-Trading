use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::Utc;

use crate::{
    config::AppConfig,
    models::{
        ExecutionRecord, GuardrailRejection, InstrumentType, OpenPositionState, OrderIntent,
        PaperTradeLifecycleRecord,
    },
};

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PaperTradeLedger {
    entries: Vec<PaperTradeLifecycleRecord>,
}

impl PaperTradeLedger {
    pub fn load(config: &AppConfig) -> Result<Self> {
        let path = ledger_path(config);
        if !path.exists() {
            return Ok(Self::default());
        }

        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read paper-trade ledger {}", path.display()))?;
        let ledger = serde_json::from_str(&contents)
            .with_context(|| format!("failed to parse paper-trade ledger {}", path.display()))?;
        Ok(ledger)
    }

    pub fn persist(&self, config: &AppConfig) -> Result<()> {
        let path = ledger_path(config);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(&path, serde_json::to_string_pretty(self)?)
            .with_context(|| format!("failed to write paper-trade ledger {}", path.display()))?;
        Ok(())
    }

    pub fn reconcile_with_positions(
        &mut self,
        positions: &[OpenPositionState],
        action_log: &mut Vec<String>,
    ) {
        for entry in &mut self.entries {
            if entry.observed_stock_shares > 0.0 || entry.observed_short_call_contracts > 0.0 {
                let still_open = positions
                    .iter()
                    .any(|position| position.symbol == entry.symbol);
                if !still_open {
                    entry.status = "closed-observed".to_string();
                    entry.observed_stock_shares = 0.0;
                    entry.observed_short_call_contracts = 0.0;
                    entry.last_updated_at = Utc::now();
                    entry.note =
                        "IBKR no longer reports an open paper position for this tracked symbol"
                            .to_string();
                    action_log.push(format!(
                        "{}: paper ledger marked closed after IBKR stopped reporting an open position.",
                        entry.symbol
                    ));
                }
            }
        }

        for position in positions {
            if position.stock_shares.abs() < 100.0 && position.short_call_contracts <= 0.0 {
                continue;
            }

            let status = if position.short_call_contracts > 0.0 {
                "open-covered-call"
            } else {
                "long-stock-awaiting-short-call"
            };
            let note = if position.short_call_contracts > 0.0 {
                format!(
                    "IBKR reports {:.0} shares and {:.0} short call contracts; hold-to-close monitoring only",
                    position.stock_shares, position.short_call_contracts
                )
            } else {
                format!(
                    "IBKR reports {:.0} shares and no short call yet; submission remains stock-first and hold-to-close only",
                    position.stock_shares
                )
            };

            self.upsert_observed_position(position, status, note);
            action_log.push(format!(
                "{}: paper ledger observed {:.0} shares and {:.0} short call contracts in IBKR.",
                position.symbol, position.stock_shares, position.short_call_contracts
            ));
        }
    }

    pub fn reject_duplicate_intents(
        &self,
        intents: Vec<OrderIntent>,
        guardrail_rejections: &mut Vec<GuardrailRejection>,
        action_log: &mut Vec<String>,
    ) -> Vec<OrderIntent> {
        let mut retained = Vec::new();

        for intent in intents {
            let intent_key = intent_key(&intent);
            if let Some(existing) = self.find_active(&intent.symbol) {
                guardrail_rejections.push(GuardrailRejection {
                    symbol: intent.symbol.clone(),
                    stage: "idempotency".to_string(),
                    reason: format!(
                        "duplicate paper submission blocked because {} is already tracked in state as {} since {}",
                        intent.symbol, existing.status, existing.first_recorded_at
                    ),
                });
                action_log.push(format!(
                    "{}: blocked duplicate paper submission for intent {} because ledger status is {}.",
                    intent.symbol, intent_key, existing.status
                ));
                continue;
            }

            retained.push(intent);
        }

        retained
    }

    pub fn record_execution_results(
        &mut self,
        executions: &[ExecutionRecord],
        intents: &[OrderIntent],
        action_log: &mut Vec<String>,
    ) {
        for execution in executions {
            if execution.submission_mode != "paper" || execution.symbol == "N/A" {
                continue;
            }

            let Some(intent) = intents
                .iter()
                .find(|intent| intent.symbol == execution.symbol)
            else {
                continue;
            };
            let now = Utc::now();
            let intent_key = intent_key(intent);
            let (stock_order_id, short_call_order_id) = extract_order_ids(execution);
            let (
                stock_filled_shares,
                short_call_filled_contracts,
                stock_average_fill_price,
                short_call_average_fill_price,
            ) = if let Some(fill) = &execution.fill_reconciliation {
                (
                    fill.stock_filled_shares,
                    fill.short_call_filled_contracts,
                    fill.stock_average_fill_price,
                    fill.short_call_average_fill_price,
                )
            } else {
                (0.0, 0.0, None, None)
            };

            match self
                .entries
                .iter_mut()
                .find(|entry| entry.symbol == execution.symbol)
            {
                Some(entry) => {
                    entry.intent_key = intent_key.clone();
                    entry.status = execution.status.clone();
                    entry.last_updated_at = now;
                    entry.hold_until_close = true;
                    entry.stock_order_id = stock_order_id;
                    entry.short_call_order_id = short_call_order_id;
                    entry.stock_filled_shares = stock_filled_shares;
                    entry.short_call_filled_contracts = short_call_filled_contracts;
                    entry.stock_average_fill_price = stock_average_fill_price;
                    entry.short_call_average_fill_price = short_call_average_fill_price;
                    entry.note = execution.note.clone();
                }
                None => self.entries.push(PaperTradeLifecycleRecord {
                    symbol: execution.symbol.clone(),
                    intent_key: intent_key.clone(),
                    status: execution.status.clone(),
                    first_recorded_at: now,
                    last_updated_at: now,
                    hold_until_close: true,
                    stock_order_id,
                    short_call_order_id,
                    stock_filled_shares,
                    short_call_filled_contracts,
                    stock_average_fill_price,
                    short_call_average_fill_price,
                    observed_stock_shares: 0.0,
                    observed_short_call_contracts: 0.0,
                    note: execution.note.clone(),
                }),
            }

            action_log.push(format!(
                "{}: paper ledger recorded execution status {} for intent {}.",
                execution.symbol, execution.status, intent_key
            ));
        }
    }

    pub fn snapshot(&self) -> Vec<PaperTradeLifecycleRecord> {
        let mut entries = self.entries.clone();
        entries.sort_by(|left, right| left.symbol.cmp(&right.symbol));
        entries
    }

    fn find_active(&self, symbol: &str) -> Option<&PaperTradeLifecycleRecord> {
        self.entries
            .iter()
            .find(|entry| entry.symbol == symbol && entry_is_active(entry))
    }

    fn upsert_observed_position(
        &mut self,
        position: &OpenPositionState,
        status: &str,
        note: String,
    ) {
        let now = Utc::now();
        match self
            .entries
            .iter_mut()
            .find(|entry| entry.symbol == position.symbol)
        {
            Some(entry) => {
                entry.status = status.to_string();
                entry.last_updated_at = now;
                entry.hold_until_close = true;
                entry.observed_stock_shares = position.stock_shares;
                entry.observed_short_call_contracts = position.short_call_contracts;
                entry.note = note;
            }
            None => self.entries.push(PaperTradeLifecycleRecord {
                symbol: position.symbol.clone(),
                intent_key: format!("observed-position:{}", position.symbol),
                status: status.to_string(),
                first_recorded_at: now,
                last_updated_at: now,
                hold_until_close: true,
                stock_order_id: None,
                short_call_order_id: None,
                stock_filled_shares: 0.0,
                short_call_filled_contracts: 0.0,
                stock_average_fill_price: position.average_stock_cost,
                short_call_average_fill_price: None,
                observed_stock_shares: position.stock_shares,
                observed_short_call_contracts: position.short_call_contracts,
                note,
            }),
        }
    }
}

fn entry_is_active(entry: &PaperTradeLifecycleRecord) -> bool {
    !matches!(
        entry.status.as_str(),
        "rejected" | "cancelled" | "closed-observed"
    )
}

fn extract_order_ids(execution: &ExecutionRecord) -> (Option<i32>, Option<i32>) {
    let mut stock_order_id = None;
    let mut short_call_order_id = None;

    for leg in &execution.legs {
        match leg.instrument_type {
            InstrumentType::Stock => stock_order_id = leg.order_id,
            InstrumentType::Option => short_call_order_id = leg.order_id,
        }
    }

    (stock_order_id, short_call_order_id)
}

fn intent_key(intent: &OrderIntent) -> String {
    let mut leg_parts = intent
        .legs
        .iter()
        .map(|leg| {
            let expiry = leg.expiry.clone().unwrap_or_else(|| "na".to_string());
            let strike = leg
                .strike
                .map(|value| format!("{value:.2}"))
                .unwrap_or_else(|| "na".to_string());
            let limit = leg
                .limit_price
                .map(|value| format!("{value:.4}"))
                .unwrap_or_else(|| "na".to_string());
            format!(
                "{:?}:{:?}:{}:{}:{}:{}",
                leg.instrument_type, leg.action, leg.symbol, expiry, strike, limit
            )
        })
        .collect::<Vec<_>>();
    leg_parts.sort();
    format!(
        "{}|{}|{}",
        intent.symbol,
        intent.strategy,
        leg_parts.join("|")
    )
}

fn ledger_path(config: &AppConfig) -> PathBuf {
    Path::new("logs").join(format!("paper-trade-state-{}.json", config.account))
}

#[cfg(test)]
mod tests {
    use super::PaperTradeLedger;
    use crate::models::{
        ExecutionLegRecord, ExecutionRecord, FillReconciliationRecord, InstrumentType,
        OpenPositionState, OrderIntent, OrderLegIntent, TradeAction,
    };

    fn intent(symbol: &str) -> OrderIntent {
        OrderIntent {
            symbol: symbol.to_string(),
            strategy: "deep-ITM covered-call buy-write".to_string(),
            account: "DU123".to_string(),
            mode: "paper-stock-first".to_string(),
            estimated_net_debit: 1.0,
            estimated_credit: 1.0,
            max_profit: 1.0,
            legs: vec![
                OrderLegIntent {
                    instrument_type: InstrumentType::Stock,
                    action: TradeAction::Buy,
                    symbol: symbol.to_string(),
                    description: "Buy".to_string(),
                    quantity: 100,
                    limit_price: Some(10.0),
                    expiry: None,
                    strike: None,
                    right: None,
                    exchange: Some("SMART".to_string()),
                    trading_class: None,
                    multiplier: None,
                    currency: Some("USD".to_string()),
                },
                OrderLegIntent {
                    instrument_type: InstrumentType::Option,
                    action: TradeAction::Sell,
                    symbol: symbol.to_string(),
                    description: "Sell".to_string(),
                    quantity: 1,
                    limit_price: Some(1.0),
                    expiry: Some("20260515".to_string()),
                    strike: Some(9.0),
                    right: Some("C".to_string()),
                    exchange: Some("SMART".to_string()),
                    trading_class: Some(symbol.to_string()),
                    multiplier: Some("100".to_string()),
                    currency: Some("USD".to_string()),
                },
            ],
        }
    }

    #[test]
    fn blocks_duplicate_symbols_once_paper_trade_is_tracked() {
        let mut ledger = PaperTradeLedger::default();
        ledger.record_execution_results(
            &[ExecutionRecord {
                symbol: "AAPL".to_string(),
                status: "stock-pending".to_string(),
                submission_mode: "paper".to_string(),
                note: "submitted stock leg".to_string(),
                legs: vec![ExecutionLegRecord {
                    leg_symbol: "AAPL".to_string(),
                    instrument_type: InstrumentType::Stock,
                    action: TradeAction::Buy,
                    quantity: 100,
                    order_id: Some(11),
                    submission_status: "Submitted".to_string(),
                    limit_price: Some(10.0),
                    filled_quantity: 0.0,
                    average_fill_price: None,
                    execution_ids: Vec::new(),
                    note: "submitted".to_string(),
                }],
                fill_reconciliation: Some(FillReconciliationRecord {
                    stock_filled_shares: 0.0,
                    stock_average_fill_price: None,
                    short_call_filled_contracts: 0.0,
                    short_call_average_fill_price: None,
                    total_commission: None,
                    eligible_for_short_call: false,
                    uncovered_shares: 0.0,
                    status: "stock-pending".to_string(),
                    note: "pending".to_string(),
                }),
            }],
            &[intent("AAPL")],
            &mut Vec::new(),
        );

        let retained = ledger.reject_duplicate_intents(
            vec![intent("AAPL"), intent("MSFT")],
            &mut Vec::new(),
            &mut Vec::new(),
        );

        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].symbol, "MSFT");
        assert_eq!(ledger.snapshot()[0].status, "stock-pending");
    }

    #[test]
    fn closes_tracked_symbol_when_ibkr_no_longer_reports_position() {
        let mut ledger = PaperTradeLedger::default();
        ledger.reconcile_with_positions(
            &[OpenPositionState {
                symbol: "AAPL".to_string(),
                stock_shares: 100.0,
                short_call_contracts: 1.0,
                average_stock_cost: Some(10.0),
            }],
            &mut Vec::new(),
        );
        ledger.reconcile_with_positions(&[], &mut Vec::new());

        assert_eq!(ledger.snapshot()[0].status, "closed-observed");
    }
}
