#[derive(Debug, Clone, PartialEq)]
pub struct UnderlyingSnapshot {
    pub symbol: String,
    pub price: f64,
    pub implied_volatility: Option<f64>,
    pub beta: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OptionCandidate {
    pub symbol: String,
    pub strike: f64,
    pub expiry: String,
    pub premium: f64,
    pub score: f64,
    pub annualized_yield_pct: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScoreInputs {
    pub underlying_price: f64,
    pub strike: f64,
    pub premium: f64,
    pub days_to_expiration: i64,
    pub beta: f64,
    pub is_call: bool,
}
