use anyhow::{Context, Result};
use chrono::{NaiveDate, Utc};

use crate::{
    config::StrategyConfig,
    models::{
        GuardrailRejection, OptionQuoteSnapshot, ScoredOptionCandidate, UnderlyingSnapshot,
        UniverseRecord,
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

    if option.right.trim().to_ascii_uppercase() != "C" {
        return Err(GuardrailRejection {
            symbol: record.symbol.clone(),
            stage: "strategy".to_string(),
            reason: format!("option right {} is not a call", option.right),
        });
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

    let itm_depth_pct = (underlying_price - option.strike) / underlying_price;
    if itm_depth_pct <= 0.0 {
        return Err(GuardrailRejection {
            symbol: record.symbol.clone(),
            stage: "strategy".to_string(),
            reason: format!(
                "strike {:.2} is not in the money versus underlying {:.2}",
                option.strike, underlying_price
            ),
        });
    }

    if itm_depth_pct > config.max_itm_depth_pct {
        return Err(GuardrailRejection {
            symbol: record.symbol.clone(),
            stage: "strategy".to_string(),
            reason: format!(
                "ITM depth {:.2}% exceeds configured maximum {:.2}%",
                itm_depth_pct * 100.0,
                config.max_itm_depth_pct * 100.0
            ),
        });
    }

    if itm_depth_pct < config.min_itm_depth_pct {
        return Err(GuardrailRejection {
            symbol: record.symbol.clone(),
            stage: "strategy".to_string(),
            reason: format!(
                "ITM depth {:.2}% below configured minimum {:.2}%",
                itm_depth_pct * 100.0,
                config.min_itm_depth_pct * 100.0
            ),
        });
    }

    let intrinsic_entry = underlying_price - premium;
    if intrinsic_entry <= 0.0 {
        return Err(GuardrailRejection {
            symbol: record.symbol.clone(),
            stage: "strategy".to_string(),
            reason: "intrinsic entry is non-positive after premium credit".to_string(),
        });
    }

    let expiration_profit = (option.strike - intrinsic_entry).max(0.0);
    if expiration_profit < config.min_expiration_profit_per_share {
        return Err(GuardrailRejection {
            symbol: record.symbol.clone(),
            stage: "strategy".to_string(),
            reason: format!(
                "expiration profit {:.2} per share is below configured minimum {:.2}",
                expiration_profit, config.min_expiration_profit_per_share
            ),
        });
    }
    let expiration_yield_pct = (expiration_profit / intrinsic_entry) * 100.0;
    let annualized_yield_pct = expiration_yield_pct / (days_to_expiration as f64 / 365.0);

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

    let downside_buffer_pct = premium / underlying_price;
    if downside_buffer_pct < config.min_downside_buffer_pct {
        return Err(GuardrailRejection {
            symbol: record.symbol.clone(),
            stage: "strategy".to_string(),
            reason: format!(
                "downside buffer {:.2}% below configured minimum {:.2}%",
                downside_buffer_pct * 100.0,
                config.min_downside_buffer_pct * 100.0
            ),
        });
    }

    let beta = if let Some(beta) = underlying.beta.filter(|beta| *beta > 0.0) {
        beta
    } else if record.beta > 0.0 {
        record.beta
    } else {
        config.default_beta
    };

    let score = (annualized_yield_pct / 100.0) * itm_depth_pct / beta.sqrt();

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
        itm_depth_pct,
        downside_buffer_pct,
        expiration_profit_per_share: expiration_profit,
        annualized_yield_pct,
        expiration_yield_pct,
        score,
    })
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
    use super::evaluate_buy_write_candidate;
    use crate::{
        config::StrategyConfig,
        models::{OptionQuoteSnapshot, UnderlyingSnapshot, UniverseRecord},
    };

    #[test]
    fn builds_scored_deep_itm_buy_write_candidate() {
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
            strike: 90.0,
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            bid: Some(14.00),
            ask: Some(14.30),
            last: Some(14.10),
            close: Some(13.80),
            option_price: Some(14.10),
            implied_volatility: Some(0.25),
            delta: Some(0.80),
            underlying_price: Some(100.0),
            quote_source: Some("test".to_string()),
            diagnostics: Vec::new(),
        };
        let config = StrategyConfig {
            min_expiry_days: 1,
            max_expiry_days: 36500,
            min_annualized_yield_pct: 0.01,
            min_expiration_profit_per_share: 0.01,
            min_itm_depth_pct: 0.0,
            min_downside_buffer_pct: 0.01,
            ..StrategyConfig::default()
        };

        let candidate =
            evaluate_buy_write_candidate(&record, &underlying, &option, &config).unwrap();
        assert_eq!(candidate.symbol, "AAPL");
        assert_eq!(candidate.exchange, "SMART");
        assert!(candidate.annualized_yield_pct > 0.0);
        assert!(candidate.itm_depth_pct > 0.0);
        assert!(candidate.score > 0.0);
    }

    #[test]
    fn prefers_underlying_beta_from_ibkr_snapshot_over_universe_fallback() {
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
            beta: Some(2.0),
            price_source: "realtime-or-frozen".to_string(),
            market_data_notices: Vec::new(),
        };
        let option = OptionQuoteSnapshot {
            symbol: "AAPL".to_string(),
            expiry: "20991217".to_string(),
            strike: 90.0,
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            bid: Some(14.00),
            ask: Some(14.30),
            last: Some(14.10),
            close: Some(13.80),
            option_price: Some(14.10),
            implied_volatility: Some(0.25),
            delta: Some(0.80),
            underlying_price: Some(100.0),
            quote_source: Some("test".to_string()),
            diagnostics: Vec::new(),
        };
        let config = StrategyConfig {
            min_expiry_days: 1,
            max_expiry_days: 36500,
            min_annualized_yield_pct: 0.01,
            min_expiration_profit_per_share: 0.01,
            min_itm_depth_pct: 0.0,
            min_downside_buffer_pct: 0.01,
            ..StrategyConfig::default()
        };

        let candidate =
            evaluate_buy_write_candidate(&record, &underlying, &option, &config).unwrap();

        assert_eq!(candidate.beta, 2.0);
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
    fn rejects_calls_that_are_not_deep_enough_itm() {
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
            strike: 98.0,
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            bid: Some(4.0),
            ask: Some(4.2),
            last: Some(4.1),
            close: Some(4.0),
            option_price: Some(4.1),
            implied_volatility: Some(0.2),
            delta: Some(0.55),
            underlying_price: Some(100.0),
            quote_source: Some("test".to_string()),
            diagnostics: Vec::new(),
        };

        let rejection = evaluate_buy_write_candidate(
            &record,
            &underlying,
            &option,
            &StrategyConfig {
                min_expiry_days: 1,
                max_expiry_days: 36500,
                min_annualized_yield_pct: 0.01,
                min_expiration_profit_per_share: 0.01,
                min_itm_depth_pct: 0.05,
                min_downside_buffer_pct: 0.01,
                ..StrategyConfig::default()
            },
        )
        .unwrap_err();

        assert!(rejection.reason.contains("ITM depth"));
    }

    #[test]
    fn rejects_calls_that_are_too_deep_itm() {
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
            strike: 40.0,
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            bid: Some(60.5),
            ask: Some(60.8),
            last: Some(60.6),
            close: Some(60.4),
            option_price: Some(60.6),
            implied_volatility: Some(0.2),
            delta: Some(0.99),
            underlying_price: Some(100.0),
            quote_source: Some("test".to_string()),
            diagnostics: Vec::new(),
        };

        let rejection = evaluate_buy_write_candidate(
            &record,
            &underlying,
            &option,
            &StrategyConfig {
                min_expiry_days: 1,
                max_expiry_days: 36500,
                min_annualized_yield_pct: 0.01,
                min_expiration_profit_per_share: 0.01,
                min_itm_depth_pct: 0.0,
                max_itm_depth_pct: 0.50,
                min_downside_buffer_pct: 0.01,
                ..StrategyConfig::default()
            },
        )
        .unwrap_err();

        assert!(rejection.reason.contains("configured maximum"));
    }

    #[test]
    fn rejects_calls_with_too_little_absolute_expiration_profit() {
        let record = UniverseRecord {
            symbol: "BTBT".to_string(),
            beta: 1.2,
        };
        let underlying = UnderlyingSnapshot {
            symbol: "BTBT".to_string(),
            price: 1.53,
            bid: Some(1.52),
            ask: Some(1.54),
            last: Some(1.53),
            close: Some(1.50),
            implied_volatility: None,
            beta: Some(1.2),
            price_source: "delayed-or-delayed-frozen".to_string(),
            market_data_notices: vec![
                "observed data origin: delayed-or-delayed-frozen".to_string(),
            ],
        };
        let option = OptionQuoteSnapshot {
            symbol: "BTBT".to_string(),
            expiry: "20991217".to_string(),
            strike: 1.0,
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: "BTBT".to_string(),
            multiplier: "100".to_string(),
            bid: Some(0.53),
            ask: Some(0.55),
            last: Some(0.54),
            close: Some(0.53),
            option_price: Some(0.54),
            implied_volatility: Some(0.7),
            delta: Some(0.95),
            underlying_price: Some(1.53),
            quote_source: Some("default+model-fallback".to_string()),
            diagnostics: vec!["observed data origin: delayed-or-delayed-frozen".to_string()],
        };

        let rejection = evaluate_buy_write_candidate(
            &record,
            &underlying,
            &option,
            &StrategyConfig {
                min_expiry_days: 1,
                max_expiry_days: 36500,
                min_annualized_yield_pct: 0.01,
                min_expiration_profit_per_share: 0.05,
                min_itm_depth_pct: 0.0,
                min_downside_buffer_pct: 0.01,
                ..StrategyConfig::default()
            },
        )
        .unwrap_err();

        assert!(rejection.reason.contains("expiration profit"));
    }
}
