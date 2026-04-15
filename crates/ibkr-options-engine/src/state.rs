use std::collections::{BTreeMap, BTreeSet};

use crate::{
    config::AppConfig,
    models::{
        AccountState, GuardrailRejection, InstrumentType, InventoryPosition, OpenPositionState,
        OrderIntent, OrderLegIntent, ScoredOptionCandidate, TradeAction,
    },
};

pub fn summarize_open_positions(positions: &[InventoryPosition]) -> Vec<OpenPositionState> {
    let mut by_symbol: BTreeMap<String, OpenPositionState> = BTreeMap::new();

    for position in positions {
        let entry = by_symbol
            .entry(position.symbol.clone())
            .or_insert(OpenPositionState {
                symbol: position.symbol.clone(),
                stock_shares: 0.0,
                short_call_contracts: 0.0,
                average_stock_cost: None,
            });

        match position.security_type.as_str() {
            "STK" => {
                entry.stock_shares += position.quantity;
                if position.quantity > 0.0 {
                    entry.average_stock_cost = Some(position.average_cost);
                }
            }
            "OPT" if position.quantity < 0.0 => {
                entry.short_call_contracts += position.quantity.abs();
            }
            _ => {}
        }
    }

    by_symbol.into_values().collect()
}

pub fn build_order_intents(
    account: &AccountState,
    positions: &[InventoryPosition],
    candidates: &[ScoredOptionCandidate],
    config: &AppConfig,
) -> (Vec<OrderIntent>, Vec<GuardrailRejection>, Vec<OpenPositionState>) {
    let open_positions = summarize_open_positions(positions);
    let mut rejections = Vec::new();
    let mut intents = Vec::new();

    let existing_symbols: BTreeSet<String> = open_positions
        .iter()
        .filter(|position| position.stock_shares.abs() >= 100.0 || position.short_call_contracts > 0.0)
        .map(|position| position.symbol.clone())
        .collect();

    let currently_open = existing_symbols.len();
    for candidate in candidates {
        if intents.len() >= config.risk.max_new_trades_per_cycle {
            break;
        }

        if existing_symbols.contains(&candidate.symbol) {
            rejections.push(GuardrailRejection {
                symbol: candidate.symbol.clone(),
                stage: "risk".to_string(),
                reason: "symbol already has an open stock or option position".to_string(),
            });
            continue;
        }

        if currently_open + intents.len() >= config.risk.max_open_positions {
            rejections.push(GuardrailRejection {
                symbol: candidate.symbol.clone(),
                stage: "risk".to_string(),
                reason: format!(
                    "max open position cap {} would be exceeded",
                    config.risk.max_open_positions
                ),
            });
            break;
        }

        let estimated_stock_cost = candidate.underlying_price * 100.0;
        let required_buying_power =
            estimated_stock_cost * (1.0 + config.risk.min_buying_power_buffer_pct / 100.0);
        let buying_power = account.buying_power.or(account.available_funds);

        match buying_power {
            Some(value) if value >= required_buying_power => {}
            Some(value) => {
                rejections.push(GuardrailRejection {
                    symbol: candidate.symbol.clone(),
                    stage: "risk".to_string(),
                    reason: format!(
                        "buying power {:.2} is below required {:.2}",
                        value, required_buying_power
                    ),
                });
                continue;
            }
            None => {
                rejections.push(GuardrailRejection {
                    symbol: candidate.symbol.clone(),
                    stage: "risk".to_string(),
                    reason: "missing buying power or available funds from IBKR".to_string(),
                });
                continue;
            }
        }

        let estimated_credit = candidate.option_bid * 100.0;
        let estimated_net_debit = estimated_stock_cost - estimated_credit;
        let max_profit = ((candidate.strike - candidate.underlying_price).max(0.0)
            + candidate.option_bid)
            * 100.0;

        intents.push(OrderIntent {
            symbol: candidate.symbol.clone(),
            strategy: "buy-write covered call".to_string(),
            account: account.account.clone(),
            mode: if config.risk.enable_paper_orders && !config.read_only {
                "paper-ready-dry-run".to_string()
            } else {
                "dry-run".to_string()
            },
            estimated_net_debit,
            estimated_credit,
            max_profit,
            legs: vec![
                OrderLegIntent {
                    instrument_type: InstrumentType::Stock,
                    action: TradeAction::Buy,
                    symbol: candidate.symbol.clone(),
                    description: format!("Buy 100 shares of {}", candidate.symbol),
                    quantity: 100,
                    limit_price: Some(candidate.underlying_price),
                    expiry: None,
                    strike: None,
                    right: None,
                },
                OrderLegIntent {
                    instrument_type: InstrumentType::Option,
                    action: TradeAction::Sell,
                    symbol: candidate.symbol.clone(),
                    description: format!(
                        "Sell 1 covered call {} {} {}",
                        candidate.symbol, candidate.expiry, candidate.strike
                    ),
                    quantity: 1,
                    limit_price: Some(candidate.option_bid),
                    expiry: Some(candidate.expiry.clone()),
                    strike: Some(candidate.strike),
                    right: Some("C".to_string()),
                },
            ],
        });
    }

    (intents, rejections, open_positions)
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{AppConfig, BrokerPlatform, MarketDataMode, RiskConfig, RunMode, RuntimeMode, StrategyConfig},
        models::{AccountState, InventoryPosition, ScoredOptionCandidate},
        state::{build_order_intents, summarize_open_positions},
    };

    fn test_config() -> AppConfig {
        AppConfig {
            host: "127.0.0.1".to_string(),
            platform: BrokerPlatform::Gateway,
            port: 4002,
            client_id: 100,
            account: "DU1234567".to_string(),
            mode: RuntimeMode::Paper,
            read_only: true,
            connect_on_start: false,
            run_mode: RunMode::Manual,
            scan_schedule: "manual".to_string(),
            market_data_mode: MarketDataMode::DelayedFrozen,
            universe_file: None,
            symbols: vec!["AAPL".to_string()],
            strategy: StrategyConfig::default(),
            risk: RiskConfig::default(),
        }
    }

    #[test]
    fn summarizes_stock_and_short_call_positions() {
        let positions = vec![
            InventoryPosition {
                account: "DU123".to_string(),
                symbol: "AAPL".to_string(),
                security_type: "STK".to_string(),
                quantity: 100.0,
                average_cost: 180.0,
                expiry: None,
                strike: None,
                right: None,
            },
            InventoryPosition {
                account: "DU123".to_string(),
                symbol: "AAPL".to_string(),
                security_type: "OPT".to_string(),
                quantity: -1.0,
                average_cost: 2.0,
                expiry: Some("20260515".to_string()),
                strike: Some(185.0),
                right: Some("C".to_string()),
            },
        ];

        let summary = summarize_open_positions(&positions);
        assert_eq!(summary.len(), 1);
        assert_eq!(summary[0].short_call_contracts, 1.0);
    }

    #[test]
    fn blocks_duplicate_symbols() {
        let candidate = ScoredOptionCandidate {
            symbol: "AAPL".to_string(),
            beta: 1.1,
            underlying_price: 100.0,
            strike: 103.0,
            expiry: "20260515".to_string(),
            days_to_expiration: 30,
            option_bid: 1.5,
            option_ask: Some(1.6),
            delta: Some(0.25),
            strike_buffer_pct: 0.03,
            annualized_yield_pct: 20.0,
            max_profit_yield_pct: 5.0,
            score: 0.2,
        };
        let positions = vec![InventoryPosition {
            account: "DU123".to_string(),
            symbol: "AAPL".to_string(),
            security_type: "STK".to_string(),
            quantity: 100.0,
            average_cost: 90.0,
            expiry: None,
            strike: None,
            right: None,
        }];

        let (_intents, rejections, _summary) = build_order_intents(
            &AccountState {
                account: "DU123".to_string(),
                available_funds: Some(50_000.0),
                buying_power: Some(50_000.0),
                net_liquidation: Some(75_000.0),
            },
            &positions,
            &[candidate],
            &test_config(),
        );

        assert_eq!(rejections.len(), 1);
    }
}
