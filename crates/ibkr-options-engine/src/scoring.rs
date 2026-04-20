use crate::models::{OptionCandidate, ScoreInputs, UnderlyingSnapshot};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoreThresholds {
    pub min_yield_threshold: f64,
    pub min_itm_depth_threshold: f64,
    pub min_downside_buffer_threshold: f64,
}

impl Default for ScoreThresholds {
    fn default() -> Self {
        Self {
            min_yield_threshold: 0.02,
            min_itm_depth_threshold: 0.05,
            min_downside_buffer_threshold: 0.12,
        }
    }
}

pub fn score_option(
    snapshot: &UnderlyingSnapshot,
    expiry: &str,
    inputs: ScoreInputs,
    thresholds: ScoreThresholds,
) -> Option<OptionCandidate> {
    if inputs.underlying_price <= 0.0
        || inputs.premium <= 0.0
        || inputs.strike <= 0.0
        || inputs.days_to_expiration <= 0
        || inputs.beta <= 0.0
    {
        return None;
    }

    if !inputs.is_call {
        return None;
    }

    let intrinsic_entry = inputs.underlying_price - inputs.premium;

    if intrinsic_entry <= 0.0 {
        return None;
    }

    let itm_depth = (inputs.underlying_price - inputs.strike) / inputs.underlying_price;
    if itm_depth <= 0.0 {
        return None;
    }

    let expiration_income = (inputs.strike - intrinsic_entry).max(0.0);
    let margin_yield = (expiration_income / intrinsic_entry).max(0.0);
    let downside_buffer = inputs.premium / inputs.underlying_price;

    if margin_yield <= thresholds.min_yield_threshold
        || itm_depth <= thresholds.min_itm_depth_threshold
        || downside_buffer <= thresholds.min_downside_buffer_threshold
    {
        return None;
    }

    let score = (margin_yield / (inputs.days_to_expiration as f64 / 365.0)) * itm_depth
        / inputs.beta.sqrt();
    let annualized_yield_pct = margin_yield / (inputs.days_to_expiration as f64 / 365.0) * 100.0;

    Some(OptionCandidate {
        symbol: snapshot.symbol.clone(),
        strike: inputs.strike,
        expiry: expiry.to_string(),
        premium: inputs.premium,
        score,
        annualized_yield_pct,
    })
}

#[cfg(test)]
mod tests {
    use super::{ScoreThresholds, score_option};
    use crate::models::{ScoreInputs, UnderlyingSnapshot};

    #[test]
    fn returns_candidate_when_thresholds_are_met() {
        let snapshot = UnderlyingSnapshot {
            contract_id: 1,
            symbol: "AAPL".to_string(),
            price: 180.0,
            bid: None,
            ask: None,
            last: None,
            close: None,
            implied_volatility: None,
            beta: Some(1.1),
            price_source: "realtime-or-frozen".to_string(),
            market_data_notices: Vec::new(),
        };

        let candidate = score_option(
            &snapshot,
            "2026-05-15",
            ScoreInputs {
                underlying_price: 180.0,
                strike: 150.0,
                premium: 36.0,
                days_to_expiration: 31,
                beta: 1.1,
                is_call: true,
            },
            ScoreThresholds::default(),
        );

        assert!(candidate.is_some());
        assert_eq!(candidate.unwrap().symbol, "AAPL");
    }

    #[test]
    fn rejects_invalid_inputs() {
        let snapshot = UnderlyingSnapshot {
            contract_id: 1,
            symbol: "MSFT".to_string(),
            price: 0.0,
            bid: None,
            ask: None,
            last: None,
            close: None,
            implied_volatility: None,
            beta: Some(1.0),
            price_source: "unknown".to_string(),
            market_data_notices: Vec::new(),
        };

        let candidate = score_option(
            &snapshot,
            "2026-05-15",
            ScoreInputs {
                underlying_price: 0.0,
                strike: 150.0,
                premium: 8.0,
                days_to_expiration: 31,
                beta: 1.0,
                is_call: true,
            },
            ScoreThresholds::default(),
        );

        assert!(candidate.is_none());
    }
}
