use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UniverseRecord {
    pub symbol: String,
    pub beta: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnderlyingSnapshot {
    pub contract_id: i32,
    pub symbol: String,
    pub price: f64,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub last: Option<f64>,
    pub close: Option<f64>,
    pub implied_volatility: Option<f64>,
    pub beta: Option<f64>,
    pub price_source: String,
    pub market_data_notices: Vec<String>,
}

impl UnderlyingSnapshot {
    pub fn reference_price(&self) -> Option<f64> {
        if self.price > 0.0 {
            return Some(self.price);
        }

        self.last
            .or(self.close)
            .or_else(|| match (self.bid, self.ask) {
                (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
                (Some(bid), None) => Some(bid),
                (None, Some(ask)) => Some(ask),
                (None, None) => None,
            })
    }

    pub fn is_non_live(&self) -> bool {
        self.price_source.contains("delayed")
    }
}

fn observed_data_origin(diagnostics: &[String]) -> Option<&str> {
    diagnostics
        .iter()
        .find_map(|diagnostic| diagnostic.strip_prefix("observed data origin: "))
        .map(str::trim)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OptionQuoteSnapshot {
    pub contract_id: i32,
    pub symbol: String,
    pub expiry: String,
    pub strike: f64,
    pub right: String,
    pub exchange: String,
    pub trading_class: String,
    pub multiplier: String,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub last: Option<f64>,
    pub close: Option<f64>,
    pub option_price: Option<f64>,
    pub implied_volatility: Option<f64>,
    pub delta: Option<f64>,
    pub underlying_price: Option<f64>,
    pub quote_source: Option<String>,
    pub diagnostics: Vec<String>,
}

impl OptionQuoteSnapshot {
    pub fn midpoint(&self) -> Option<f64> {
        match (self.bid, self.ask) {
            (Some(bid), Some(ask)) if ask >= bid => Some((bid + ask) / 2.0),
            _ => None,
        }
    }

    pub fn best_credit(&self) -> Option<f64> {
        self.bid
            .or(self.option_price)
            .or(self.midpoint())
            .or(self.last)
            .or(self.close)
    }

    pub fn has_usable_premium(&self) -> bool {
        self.best_credit().is_some()
    }

    pub fn missing_premium_diagnostic(&self) -> String {
        let available_fields = [
            ("bid", self.bid),
            ("ask", self.ask),
            ("last", self.last),
            ("close", self.close),
            ("option_price", self.option_price),
            ("delta", self.delta),
            ("underlying_price", self.underlying_price),
        ]
        .into_iter()
        .filter_map(|(label, value)| value.map(|_| label))
        .collect::<Vec<_>>();

        let source = self.quote_source.as_deref().unwrap_or("snapshot");
        if available_fields.is_empty() {
            format!(
                "no bid/ask/last/close/model fields returned from {source} for {} {} {} {}",
                self.symbol, self.expiry, self.right, self.strike
            )
        } else {
            format!(
                "missing premium fields from {source}; available fields: {}",
                available_fields.join(", ")
            )
        }
    }

    pub fn spread_pct(&self) -> Option<f64> {
        let bid = self.bid?;
        let ask = self.ask?;
        let midpoint = (bid + ask) / 2.0;
        if midpoint <= 0.0 {
            return None;
        }
        Some((ask - bid) / midpoint)
    }

    pub fn is_non_live(&self) -> bool {
        if let Some(origin) = observed_data_origin(&self.diagnostics) {
            return origin.contains("delayed");
        }

        self.diagnostics.iter().any(|diagnostic| {
            diagnostic
                .to_ascii_lowercase()
                .contains("delayed market data")
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OptionCandidate {
    pub symbol: String,
    pub strike: f64,
    pub expiry: String,
    pub premium: f64,
    pub score: f64,
    pub annualized_yield_pct: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScoredOptionCandidate {
    pub symbol: String,
    pub beta: f64,
    pub underlying_contract_id: i32,
    pub underlying_price: f64,
    pub underlying_ask: Option<f64>,
    pub option_contract_id: i32,
    pub strike: f64,
    pub expiry: String,
    pub right: String,
    pub exchange: String,
    pub trading_class: String,
    pub multiplier: String,
    pub days_to_expiration: i64,
    pub option_bid: f64,
    pub option_ask: Option<f64>,
    pub delta: Option<f64>,
    pub itm_depth_pct: f64,
    pub downside_buffer_pct: f64,
    pub expiration_profit_per_share: f64,
    pub annualized_yield_pct: f64,
    pub expiration_yield_pct: f64,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScoreInputs {
    pub underlying_price: f64,
    pub strike: f64,
    pub premium: f64,
    pub days_to_expiration: i64,
    pub beta: f64,
    pub is_call: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountState {
    pub account: String,
    pub available_funds: Option<f64>,
    pub buying_power: Option<f64>,
    pub net_liquidation: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InventoryPosition {
    pub account: String,
    pub symbol: String,
    pub security_type: String,
    pub quantity: f64,
    pub average_cost: f64,
    pub expiry: Option<String>,
    pub strike: Option<f64>,
    pub right: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenPositionState {
    pub symbol: String,
    pub stock_shares: f64,
    pub short_call_contracts: f64,
    pub average_stock_cost: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrokerOpenOrder {
    pub account: String,
    pub order_id: i32,
    pub client_id: i32,
    pub perm_id: i32,
    pub order_ref: String,
    pub symbol: String,
    pub security_type: String,
    pub action: String,
    pub total_quantity: f64,
    pub order_type: String,
    pub limit_price: Option<f64>,
    pub status: String,
    pub filled_quantity: f64,
    pub remaining_quantity: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrokerCompletedOrder {
    pub account: String,
    pub order_id: i32,
    pub client_id: i32,
    pub perm_id: i32,
    pub symbol: String,
    pub security_type: String,
    pub action: String,
    pub total_quantity: f64,
    pub order_type: String,
    pub limit_price: Option<f64>,
    pub status: String,
    pub completed_status: String,
    pub reject_reason: String,
    pub warning_text: String,
    pub completed_time: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstrumentType {
    Stock,
    Option,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeAction {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderLegIntent {
    pub instrument_type: InstrumentType,
    pub action: TradeAction,
    pub contract_id: Option<i32>,
    pub symbol: String,
    pub description: String,
    pub quantity: i32,
    pub limit_price: Option<f64>,
    pub expiry: Option<String>,
    pub strike: Option<f64>,
    pub right: Option<String>,
    pub exchange: Option<String>,
    pub trading_class: Option<String>,
    pub multiplier: Option<String>,
    pub currency: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderIntent {
    pub symbol: String,
    pub strategy: String,
    pub account: String,
    pub mode: String,
    pub combo_limit_price: Option<f64>,
    pub estimated_net_debit: f64,
    pub estimated_credit: f64,
    pub max_profit: f64,
    pub legs: Vec<OrderLegIntent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GuardrailRejection {
    pub symbol: String,
    pub stage: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionLegRecord {
    pub leg_symbol: String,
    pub instrument_type: InstrumentType,
    pub action: TradeAction,
    pub quantity: i32,
    pub order_id: Option<i32>,
    pub submission_status: String,
    pub limit_price: Option<f64>,
    pub filled_quantity: f64,
    pub average_fill_price: Option<f64>,
    pub execution_ids: Vec<String>,
    pub note: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FillReconciliationRecord {
    pub stock_filled_shares: f64,
    pub stock_average_fill_price: Option<f64>,
    pub short_call_filled_contracts: f64,
    pub short_call_average_fill_price: Option<f64>,
    pub total_commission: Option<f64>,
    pub eligible_for_short_call: bool,
    pub uncovered_shares: f64,
    pub status: String,
    pub note: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrokerEventTimelineEntry {
    pub observed_at: DateTime<Utc>,
    pub elapsed_ms: i64,
    pub event_type: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionStepTiming {
    pub step: String,
    pub duration_ms: i64,
    pub attempt: Option<usize>,
    pub order_id: Option<i32>,
    pub limit_price: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionRecord {
    pub symbol: String,
    pub status: String,
    pub submission_mode: String,
    pub note: String,
    pub legs: Vec<ExecutionLegRecord>,
    pub fill_reconciliation: Option<FillReconciliationRecord>,
    pub broker_event_log_path: Option<String>,
    pub broker_event_timeline: Vec<BrokerEventTimelineEntry>,
    pub execution_step_timings: Vec<ExecutionStepTiming>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PaperTradeLifecycleRecord {
    pub symbol: String,
    pub intent_key: String,
    pub status: String,
    pub first_recorded_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
    pub hold_until_close: bool,
    pub stock_order_id: Option<i32>,
    pub short_call_order_id: Option<i32>,
    pub stock_filled_shares: f64,
    pub short_call_filled_contracts: f64,
    pub stock_average_fill_price: Option<f64>,
    pub short_call_average_fill_price: Option<f64>,
    pub observed_stock_shares: f64,
    pub observed_short_call_contracts: f64,
    pub note: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CycleTimingMetrics {
    pub total_elapsed_ms: i64,
    pub market_data_elapsed_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CycleThroughputCounters {
    pub configured_symbol_concurrency: usize,
    pub configured_option_quote_concurrency_per_symbol: usize,
    pub symbols_completed: usize,
    pub underlying_snapshots_completed: usize,
    pub option_quotes_completed: usize,
    pub symbols_per_second: f64,
    pub underlying_snapshots_per_second: f64,
    pub option_quotes_per_second: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CycleReport {
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub run_mode: String,
    pub schedule: String,
    pub market_data_mode: String,
    pub account_state: AccountState,
    pub universe_size: usize,
    pub symbols_scanned: usize,
    pub underlying_snapshots: usize,
    pub option_quotes_considered: usize,
    pub candidates_ranked: usize,
    pub accepted_candidates: Vec<ScoredOptionCandidate>,
    pub guardrail_rejections: Vec<GuardrailRejection>,
    pub proposed_orders: Vec<OrderIntent>,
    pub execution_records: Vec<ExecutionRecord>,
    pub open_positions: Vec<OpenPositionState>,
    pub paper_trade_lifecycle: Vec<PaperTradeLifecycleRecord>,
    pub live_data_requested: bool,
    pub non_live_symbols: Vec<String>,
    pub warnings: Vec<String>,
    pub action_log: Vec<String>,
    pub timing_metrics: CycleTimingMetrics,
    pub throughput_counters: CycleThroughputCounters,
    pub human_log_path: Option<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusReport {
    pub account: String,
    pub endpoint: String,
    pub platform: String,
    pub runtime_mode: String,
    pub connect_on_start: bool,
    pub capital_source: String,
    pub deployment_budget: f64,
    pub open_orders: Vec<BrokerOpenOrder>,
    pub completed_orders: Vec<BrokerCompletedOrder>,
    pub open_positions: Vec<OpenPositionState>,
    pub paper_trade_lifecycle: Vec<PaperTradeLifecycleRecord>,
    pub action_log: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::OptionQuoteSnapshot;

    fn quote_with_diagnostics(diagnostics: &[&str]) -> OptionQuoteSnapshot {
        OptionQuoteSnapshot {
            contract_id: 1,
            symbol: "MARA".to_string(),
            expiry: "20991217".to_string(),
            strike: 1.0,
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: "MARA".to_string(),
            multiplier: "100".to_string(),
            bid: Some(1.0),
            ask: Some(1.1),
            last: Some(1.05),
            close: Some(1.0),
            option_price: Some(1.05),
            implied_volatility: None,
            delta: None,
            underlying_price: Some(10.0),
            quote_source: Some("test".to_string()),
            diagnostics: diagnostics
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
        }
    }

    #[test]
    fn treats_realtime_or_frozen_option_quotes_as_live_enough() {
        let quote = quote_with_diagnostics(&[
            "observed tick types: Close, Bid, Ask",
            "observed data origin: realtime-or-frozen",
        ]);

        assert!(!quote.is_non_live());
    }

    #[test]
    fn treats_delayed_option_quotes_as_non_live() {
        let quote = quote_with_diagnostics(&[
            "10167: Requested market data is not subscribed. Displaying delayed market data.",
            "observed data origin: delayed-or-delayed-frozen",
        ]);

        assert!(quote.is_non_live());
    }
}
