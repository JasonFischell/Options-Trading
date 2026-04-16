use anyhow::{Context, Result};
use chrono::{NaiveDate, Utc};

use crate::{
    config::StrategyConfig,
    models::{
        CoveredCallPositionState, ExitDecision, ExitRuleState, GuardrailRejection,
        OptionQuoteSnapshot, ScoredOptionCandidate, UnderlyingSnapshot, UniverseRecord,
    },
};

pub fn evaluate_buy_write_candidate(
    record: &UniverseRecord,
    underlying: &UnderlyingSnapshot,
    option: &OptionQuoteSnapshot,
    config: &StrategyConfig,
) -> Result<ScoredOptionCandidate, GuardrailRejection> {
    let underlying_price = underlying
        .reference_price()
        .ok_or_else(|| GuardrailRejection {
            symbol: record.symbol.clone(),
            stage: "strategy".to_string(),
            reason: "missing usable underlying price".to_string(),
        })?;

    let premium = option.best_credit().ok_or_else(|| GuardrailRejection {
        symbol: record.symbol.clone(),
        stage: "strategy".to_string(),
        reason: format!(
            "missing usable option premium ({})",
            option.missing_premium_diagnostic()
        ),
    })?;

    if premium < config.min_option_bid {
        return Err(GuardrailRejection {
            symbol: record.symbol.clone(),
            stage: "strategy".to_string(),
            reason: format!(
                "option bid {:.2} below configured minimum {:.2}",
                premium, config.min_option_bid
            ),
        });
    }

    if let Some(spread_pct) = option.spread_pct() {
        if spread_pct > config.max_option_spread_pct {
            return Err(GuardrailRejection {
                symbol: record.symbol.clone(),
                stage: "strategy".to_string(),
                reason: format!(
                    "option spread {:.2}% exceeds maximum {:.2}%",
                    spread_pct * 100.0,
                    config.max_option_spread_pct * 100.0
                ),
            });
        }
    }

    if let Some(delta) = option.delta {
        if delta.abs() > config.max_short_call_delta {
            return Err(GuardrailRejection {
                symbol: record.symbol.clone(),
                stage: "strategy".to_string(),
                reason: format!(
                    "call delta {:.2} exceeds configured cap {:.2}",
                    delta.abs(),
                    config.max_short_call_delta
                ),
            });
        }
    }

    let days_to_expiration =
        days_to_expiration(&option.expiry).map_err(|error| GuardrailRejection {
            symbol: record.symbol.clone(),
            stage: "strategy".to_string(),
            reason: format!("invalid option expiry {}: {error}", option.expiry),
        })?;

    if days_to_expiration < config.min_expiry_days || days_to_expiration > config.max_expiry_days {
        return Err(GuardrailRejection {
            symbol: record.symbol.clone(),
            stage: "strategy".to_string(),
            reason: format!(
                "expiry {} is outside {}-{} day window",
                option.expiry, config.min_expiry_days, config.max_expiry_days
            ),
        });
    }

    let strike_buffer_pct = (option.strike - underlying_price) / underlying_price;
    if strike_buffer_pct < config.min_strike_buffer_pct {
        return Err(GuardrailRejection {
            symbol: record.symbol.clone(),
            stage: "strategy".to_string(),
            reason: format!(
                "strike buffer {:.2}% below configured minimum {:.2}%",
                strike_buffer_pct * 100.0,
                config.min_strike_buffer_pct * 100.0
            ),
        });
    }

    let net_debit = underlying_price - premium;
    if net_debit <= 0.0 {
        return Err(GuardrailRejection {
            symbol: record.symbol.clone(),
            stage: "strategy".to_string(),
            reason: "net debit is non-positive after premium credit".to_string(),
        });
    }

    let max_profit = (option.strike - underlying_price).max(0.0) + premium;
    let max_profit_yield_pct = (max_profit / net_debit) * 100.0;
    let annualized_yield_pct = max_profit_yield_pct / (days_to_expiration as f64 / 365.0);

    if annualized_yield_pct < config.min_annualized_yield_pct {
        return Err(GuardrailRejection {
            symbol: record.symbol.clone(),
            stage: "strategy".to_string(),
            reason: format!(
                "annualized yield {:.2}% below configured minimum {:.2}%",
                annualized_yield_pct, config.min_annualized_yield_pct
            ),
        });
    }

    let beta = if record.beta > 0.0 {
        record.beta
    } else {
        config.default_beta
    };

    let score = (annualized_yield_pct / 100.0) * (1.0 + strike_buffer_pct) / beta.sqrt();

    Ok(ScoredOptionCandidate {
        symbol: record.symbol.clone(),
        beta,
        underlying_price,
        strike: option.strike,
        expiry: option.expiry.clone(),
        right: option.right.clone(),
        exchange: option.exchange.clone(),
        trading_class: option.trading_class.clone(),
        multiplier: option.multiplier.clone(),
        days_to_expiration,
        option_bid: premium,
        option_ask: option.ask,
        delta: option.delta,
        strike_buffer_pct,
        annualized_yield_pct,
        max_profit_yield_pct,
        score,
    })
}

pub fn evaluate_basic_exit(
    position: &CoveredCallPositionState,
    current_underlying_price: f64,
    current_option_mark: f64,
    rules: &ExitRuleState,
) -> Option<ExitDecision> {
    let covered_entry = (position.stock_average_cost - position.short_call_credit).max(0.01);
    let covered_mark = current_underlying_price - current_option_mark;
    let pnl_pct = (covered_mark - covered_entry) / covered_entry;

    if pnl_pct >= rules.profit_take_pct {
        return Some(ExitDecision {
            symbol: position.symbol.clone(),
            action: "close_position".to_string(),
            reason: format!(
                "covered-call profit {:.2}% reached target {:.2}%",
                pnl_pct * 100.0,
                rules.profit_take_pct * 100.0
            ),
        });
    }

    if pnl_pct <= -rules.max_loss_pct {
        return Some(ExitDecision {
            symbol: position.symbol.clone(),
            action: "close_position".to_string(),
            reason: format!(
                "covered-call loss {:.2}% breached max loss {:.2}%",
                pnl_pct.abs() * 100.0,
                rules.max_loss_pct * 100.0
            ),
        });
    }

    None
}

fn days_to_expiration(expiry: &str) -> Result<i64> {
    let parsed = parse_expiry_date(expiry)?;
    let today = Utc::now().date_naive();
    Ok((parsed - today).num_days())
}

pub fn parse_expiry_date(expiry: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(expiry, "%Y%m%d")
        .or_else(|_| NaiveDate::parse_from_str(expiry, "%Y-%m-%d"))
        .with_context(|| format!("unsupported expiry format: {expiry}"))
}

#[cfg(test)]
mod tests {
    use super::{evaluate_basic_exit, evaluate_buy_write_candidate};
    use crate::{
        config::StrategyConfig,
        models::{
            CoveredCallPositionState, ExitRuleState, OptionQuoteSnapshot, UnderlyingSnapshot,
            UniverseRecord,
        },
    };

    #[test]
    fn builds_scored_buy_write_candidate() {
        let record = UniverseRecord {
            symbol: "AAPL".to_string(),
            beta: 1.1,
        };
        let underlying = UnderlyingSnapshot {
            symbol: "AAPL".to_string(),
            price: 100.0,
            bid: Some(99.9),
            ask: Some(100.1),
            last: Some(100.0),
            close: Some(99.5),
            implied_volatility: None,
            beta: Some(1.1),
            price_source: "realtime-or-frozen".to_string(),
            market_data_notices: Vec::new(),
        };
        let option = OptionQuoteSnapshot {
            symbol: "AAPL".to_string(),
            expiry: "20991217".to_string(),
            strike: 103.0,
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            bid: Some(1.50),
            ask: Some(1.60),
            last: Some(1.55),
            close: Some(1.45),
            option_price: Some(1.55),
            implied_volatility: Some(0.25),
            delta: Some(0.25),
            underlying_price: Some(100.0),
            quote_source: Some("test".to_string()),
            diagnostics: Vec::new(),
        };
        let config = StrategyConfig {
            min_expiry_days: 1,
            max_expiry_days: 36500,
            min_annualized_yield_pct: 0.01,
            ..StrategyConfig::default()
        };

        let candidate =
            evaluate_buy_write_candidate(&record, &underlying, &option, &config).unwrap();
        assert_eq!(candidate.symbol, "AAPL");
        assert_eq!(candidate.exchange, "SMART");
        assert!(candidate.annualized_yield_pct > 0.0);
    }

    #[test]
    fn reports_detailed_missing_premium_reason_for_thin_snapshots() {
        let record = UniverseRecord {
            symbol: "NVTS".to_string(),
            beta: 1.1,
        };
        let underlying = UnderlyingSnapshot {
            symbol: "NVTS".to_string(),
            price: 4.2,
            bid: Some(4.1),
            ask: Some(4.3),
            last: None,
            close: Some(4.15),
            implied_volatility: None,
            beta: Some(1.1),
            price_source: "realtime-or-frozen".to_string(),
            market_data_notices: Vec::new(),
        };
        let option = OptionQuoteSnapshot {
            symbol: "NVTS".to_string(),
            expiry: "20991217".to_string(),
            strike: 4.5,
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: "NVTS".to_string(),
            multiplier: "100".to_string(),
            bid: None,
            ask: None,
            last: None,
            close: None,
            option_price: None,
            implied_volatility: Some(0.45),
            delta: Some(0.20),
            underlying_price: Some(4.2),
            quote_source: Some("default-snapshot".to_string()),
            diagnostics: vec!["354: Not subscribed to requested market data".to_string()],
        };

        let rejection =
            evaluate_buy_write_candidate(&record, &underlying, &option, &StrategyConfig::default())
                .unwrap_err();

        assert!(rejection.reason.contains("missing usable option premium"));
        assert!(
            rejection
                .reason
                .contains("available fields: delta, underlying_price")
        );
    }

    #[test]
    fn exit_rule_triggers_profit_take() {
        let decision = evaluate_basic_exit(
            &CoveredCallPositionState {
                symbol: "MSFT".to_string(),
                stock_average_cost: 100.0,
                short_call_credit: 2.0,
                strike: 105.0,
                shares: 100,
                contracts: 1,
            },
            110.0,
            0.25,
            &ExitRuleState {
                profit_take_pct: 0.05,
                max_loss_pct: 0.1,
            },
        );

        assert!(decision.is_some());
    }
}
