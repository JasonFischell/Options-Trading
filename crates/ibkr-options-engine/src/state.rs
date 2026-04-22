use std::collections::{BTreeMap, BTreeSet};

use crate::{
    config::{AppConfig, CapitalSource},
    models::{
        AccountState, AllocationSummary, CapitalAllocationView, CapitalSourceDetails,
        GuardrailRejection, InstrumentType, InventoryPosition, OpenPositionState, OrderIntent,
        OrderLegIntent, ScoredOptionCandidate, TradeAction,
    },
};

pub struct OrderIntentBuildResult {
    pub intents: Vec<OrderIntent>,
    pub rejections: Vec<GuardrailRejection>,
    pub open_positions: Vec<OpenPositionState>,
    pub capital_source_details: CapitalSourceDetails,
    pub allocation_summary: AllocationSummary,
}

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
) -> OrderIntentBuildResult {
    let open_positions = summarize_open_positions(positions);
    let mut rejections = Vec::new();
    let mut intents = Vec::new();
    let capital_source_details = build_capital_source_details(account, config);
    let sizing_view = if config.guarded_paper_submission_enabled() {
        &capital_source_details.routed_orders
    } else {
        &capital_source_details.preview
    };
    let collapsed_candidates = collapse_candidates_by_symbol(candidates);
    let mut remaining_cash = sizing_view.deployable_cash;

    let existing_symbols: BTreeSet<String> = open_positions
        .iter()
        .filter(|position| {
            position.stock_shares.abs() >= 100.0 || position.short_call_contracts > 0.0
        })
        .map(|position| position.symbol.clone())
        .collect();
    let mut blocked_symbols = existing_symbols.clone();

    let currently_open = existing_symbols.len();
    for candidate in &collapsed_candidates {
        if intents.len() >= config.risk.max_new_trades_per_cycle {
            break;
        }

        if blocked_symbols.contains(&candidate.symbol) {
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

        let Some(stock_ask) = candidate.underlying_ask else {
            rejections.push(GuardrailRejection {
                symbol: candidate.symbol.clone(),
                stage: "pricing".to_string(),
                reason: "missing underlying ask required for combo BAG debit pricing".to_string(),
            });
            continue;
        };

        let combo_limit_price = floor_to_cents(combo_limit_price_from_profit_floor(
            candidate,
            &config.strategy,
        ));
        if combo_limit_price <= 0.0 {
            rejections.push(GuardrailRejection {
                symbol: candidate.symbol.clone(),
                stage: "pricing".to_string(),
                reason: format!(
                    "combo debit {:.2} is non-positive after applying the configured profit-floor guardrails",
                    combo_limit_price
                ),
            });
            continue;
        }

        let expiration_profit_per_share = (candidate.strike - combo_limit_price).max(0.0);
        if expiration_profit_per_share < config.strategy.min_expiration_profit_per_share {
            rejections.push(GuardrailRejection {
                symbol: candidate.symbol.clone(),
                stage: "pricing".to_string(),
                reason: format!(
                    "combo BAG debit {:.2} yields only {:.2} expiration profit per share, below configured minimum {:.2}",
                    combo_limit_price,
                    expiration_profit_per_share,
                    config.strategy.min_expiration_profit_per_share
                ),
            });
            continue;
        }

        let expiration_yield_pct = (expiration_profit_per_share / combo_limit_price) * 100.0;
        if expiration_yield_pct < config.strategy.min_expiration_yield_pct {
            rejections.push(GuardrailRejection {
                symbol: candidate.symbol.clone(),
                stage: "pricing".to_string(),
                reason: format!(
                    "combo BAG debit {:.2} yields only {:.2}% to expiration, below configured minimum {:.2}%",
                    combo_limit_price,
                    expiration_yield_pct,
                    config.strategy.min_expiration_yield_pct
                ),
            });
            continue;
        }

        let annualized_yield_pct =
            expiration_yield_pct / (candidate.days_to_expiration as f64 / 365.0);
        if annualized_yield_pct < config.strategy.min_annualized_yield_pct {
            rejections.push(GuardrailRejection {
                symbol: candidate.symbol.clone(),
                stage: "pricing".to_string(),
                reason: format!(
                    "combo BAG debit {:.2} yields only {:.2}% annualized, below configured minimum {:.2}%",
                    combo_limit_price,
                    annualized_yield_pct,
                    config.strategy.min_annualized_yield_pct
                ),
            });
            continue;
        }

        let estimated_stock_cost = stock_ask * 100.0;
        let estimated_net_debit_per_lot = combo_limit_price * 100.0;
        let sizing_amount = sizing_view.reported_amount;

        match sizing_amount {
            Some(_) => {}
            None => {
                rejections.push(GuardrailRejection {
                    symbol: candidate.symbol.clone(),
                    stage: "risk".to_string(),
                    reason: missing_capital_reason(config, sizing_view),
                });
                continue;
            }
        }

        if estimated_net_debit_per_lot <= 0.0 {
            rejections.push(GuardrailRejection {
                symbol: candidate.symbol.clone(),
                stage: "pricing".to_string(),
                reason: format!(
                    "combo BAG debit {:.2} does not leave any positive deployable cash per lot",
                    estimated_net_debit_per_lot
                ),
            });
            continue;
        }

        let max_lots_by_remaining_cash =
            (remaining_cash / estimated_net_debit_per_lot).floor() as i32;
        if max_lots_by_remaining_cash < 1 {
            rejections.push(GuardrailRejection {
                symbol: candidate.symbol.clone(),
                stage: "risk".to_string(),
                reason: format!(
                    "remaining deployable cash {:.2} is below one-lot debit {:.2}",
                    remaining_cash, estimated_net_debit_per_lot
                ),
            });
            continue;
        }

        let max_lots_by_symbol_cap =
            (sizing_view.max_cash_per_symbol / estimated_net_debit_per_lot).floor() as i32;
        if max_lots_by_symbol_cap < 1 {
            rejections.push(GuardrailRejection {
                symbol: candidate.symbol.clone(),
                stage: "risk".to_string(),
                reason: format!(
                    "per-symbol cash cap {:.2} is below one-lot debit {:.2}",
                    sizing_view.max_cash_per_symbol, estimated_net_debit_per_lot
                ),
            });
            continue;
        }

        let lot_quantity = max_lots_by_remaining_cash.min(max_lots_by_symbol_cap);
        if lot_quantity < 1 {
            rejections.push(GuardrailRejection {
                symbol: candidate.symbol.clone(),
                stage: "risk".to_string(),
                reason: format!(
                    "no lot quantity fit within remaining deployable cash {:.2} and per-symbol cap {:.2}",
                    remaining_cash, sizing_view.max_cash_per_symbol
                ),
            });
            continue;
        }

        let estimated_credit = candidate.option_bid * 100.0 * lot_quantity as f64;
        let estimated_net_debit = estimated_net_debit_per_lot * lot_quantity as f64;
        let max_profit = expiration_profit_per_share * 100.0 * lot_quantity as f64;
        let stock_quantity = 100 * lot_quantity;

        if estimated_stock_cost > 0.0 && estimated_net_debit > 0.0 {
            remaining_cash = (remaining_cash - estimated_net_debit).max(0.0);
        } else {
            rejections.push(GuardrailRejection {
                symbol: candidate.symbol.clone(),
                stage: "risk".to_string(),
                reason: if config.guarded_paper_submission_enabled() {
                    "guarded paper routing requires IBKR AVAILABLE_FUNDS for the configured paper account"
                        .to_string()
                } else {
                    "missing deployable cash from the selected capital source".to_string()
                },
            });
            continue;
        }

        intents.push(OrderIntent {
            symbol: candidate.symbol.clone(),
            strategy: "deep-ITM covered-call buy-write".to_string(),
            account: account.account.clone(),
            mode: if config.guarded_paper_submission_enabled() {
                "paper-combo-bag".to_string()
            } else {
                "analysis-only".to_string()
            },
            lot_quantity,
            combo_limit_price: Some(combo_limit_price),
            estimated_net_debit,
            estimated_credit,
            max_profit,
            legs: vec![
                OrderLegIntent {
                    instrument_type: InstrumentType::Stock,
                    action: TradeAction::Buy,
                    contract_id: Some(candidate.underlying_contract_id),
                    symbol: candidate.symbol.clone(),
                    description: format!("Buy {stock_quantity} shares of {}", candidate.symbol),
                    quantity: stock_quantity,
                    limit_price: Some(stock_ask),
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
                    contract_id: Some(candidate.option_contract_id),
                    symbol: candidate.symbol.clone(),
                    description: format!(
                        "Sell {} deep-ITM covered call contract(s) {} {} {}",
                        lot_quantity, candidate.symbol, candidate.expiry, candidate.strike
                    ),
                    quantity: lot_quantity,
                    limit_price: Some(candidate.option_bid),
                    expiry: Some(candidate.expiry.clone()),
                    strike: Some(candidate.strike),
                    right: Some(candidate.right.clone()),
                    exchange: Some(candidate.exchange.clone()),
                    trading_class: Some(candidate.trading_class.clone()),
                    multiplier: Some(candidate.multiplier.clone()),
                    currency: Some("USD".to_string()),
                },
            ],
        });
        blocked_symbols.insert(candidate.symbol.clone());
    }

    OrderIntentBuildResult {
        allocation_summary: AllocationSummary {
            candidate_symbols_considered: collapsed_candidates.len(),
            selected_symbols: intents.len(),
            total_lots: intents.iter().map(|intent| intent.lot_quantity).sum(),
            allocated_cash: sizing_view.deployable_cash - remaining_cash,
            remaining_cash,
        },
        capital_source_details,
        intents,
        rejections,
        open_positions,
    }
}

fn collapse_candidates_by_symbol(
    candidates: &[ScoredOptionCandidate],
) -> Vec<ScoredOptionCandidate> {
    let mut seen_symbols = BTreeSet::new();
    let mut collapsed = Vec::new();

    for candidate in candidates {
        if seen_symbols.insert(candidate.symbol.clone()) {
            collapsed.push(candidate.clone());
        }
    }

    collapsed
}

fn build_capital_source_details(
    account: &AccountState,
    config: &AppConfig,
) -> CapitalSourceDetails {
    let routed_orders = capital_allocation_view(
        "available_funds",
        account.available_funds,
        config.allocation.min_cash_reserve_pct,
        config.allocation.deployment_budget,
        config.allocation.max_cash_per_symbol_pct,
    );
    let preview = match config.allocation.capital_source {
        CapitalSource::AvailableFunds => routed_orders.clone(),
        CapitalSource::BuyingPower if config.guarded_paper_submission_enabled() => {
            routed_orders.clone()
        }
        CapitalSource::BuyingPower => capital_allocation_view(
            "buying_power",
            account.buying_power,
            0.0,
            config.allocation.deployment_budget,
            config.allocation.max_cash_per_symbol_pct,
        ),
    };

    CapitalSourceDetails {
        configured_source: config.allocation.capital_source.label().to_string(),
        preview,
        routed_orders,
    }
}

fn capital_allocation_view(
    source: &str,
    reported_amount: Option<f64>,
    reserve_pct: f64,
    deployment_budget: f64,
    max_cash_per_symbol_pct: f64,
) -> CapitalAllocationView {
    let reported_amount = reported_amount.filter(|value| value.is_finite() && *value > 0.0);
    let reserve_amount = reported_amount.unwrap_or(0.0) * (reserve_pct.max(0.0) / 100.0);
    let cash_after_reserve = (reported_amount.unwrap_or(0.0) - reserve_amount).max(0.0);
    let deployable_cash = cash_after_reserve.min(deployment_budget.max(0.0));
    let max_cash_per_symbol =
        (cash_after_reserve * (max_cash_per_symbol_pct.max(0.0) / 100.0)).min(deployable_cash);

    CapitalAllocationView {
        source: source.to_string(),
        reported_amount,
        reserve_pct,
        reserve_amount,
        cash_after_reserve,
        deployment_budget,
        deployable_cash,
        max_cash_per_symbol,
    }
}

fn missing_capital_reason(config: &AppConfig, sizing_view: &CapitalAllocationView) -> String {
    if config.guarded_paper_submission_enabled() {
        "guarded paper routing requires IBKR AVAILABLE_FUNDS for the configured paper account"
            .to_string()
    } else if sizing_view.source == "buying_power" {
        "analysis-only allocation preview requested buying power, but IBKR did not return BUYING_POWER"
            .to_string()
    } else {
        "missing available funds from IBKR".to_string()
    }
}

fn floor_to_cents(value: f64) -> f64 {
    (value * 100.0).floor() / 100.0
}

fn combo_limit_price_from_profit_floor(
    candidate: &ScoredOptionCandidate,
    strategy: &crate::config::StrategyConfig,
) -> f64 {
    let min_profit_cap = candidate.strike - strategy.min_expiration_profit_per_share;
    let min_expiration_yield_cap =
        max_debit_for_yield_floor(candidate.strike, strategy.min_expiration_yield_pct / 100.0);
    let annualized_floor =
        strategy.min_annualized_yield_pct / 100.0 * (candidate.days_to_expiration as f64 / 365.0);
    let min_annualized_yield_cap = max_debit_for_yield_floor(candidate.strike, annualized_floor);

    min_profit_cap
        .min(min_expiration_yield_cap)
        .min(min_annualized_yield_cap)
}

fn max_debit_for_yield_floor(strike: f64, required_yield: f64) -> f64 {
    strike / (1.0 + required_yield.max(0.0))
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{
            AllocationConfig, AppConfig, BrokerPlatform, CapitalSource, ExecutionTuningConfig,
            MarketDataMode, PerformanceConfig, RiskConfig, RunMode, RuntimeMode, StrategyConfig,
        },
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
            startup_warnings: Vec::new(),
            strategy: StrategyConfig::default(),
            risk: RiskConfig::default(),
            allocation: AllocationConfig::default(),
            performance: PerformanceConfig::default(),
            execution: ExecutionTuningConfig::default(),
        }
    }

    fn candidate(symbol: &str) -> ScoredOptionCandidate {
        ScoredOptionCandidate {
            symbol: symbol.to_string(),
            beta: 1.1,
            underlying_contract_id: 1001,
            underlying_price: 100.0,
            underlying_ask: Some(100.10),
            option_contract_id: 2001,
            strike: 90.0,
            expiry: "20260515".to_string(),
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: symbol.to_string(),
            multiplier: "100".to_string(),
            days_to_expiration: 30,
            option_bid: 14.0,
            option_ask: Some(14.1),
            delta: Some(0.8),
            itm_depth_pct: 0.1,
            downside_buffer_pct: 0.14,
            expiration_profit_per_share: 4.0,
            annualized_yield_pct: 20.0,
            expiration_yield_pct: 4.0,
            score: 0.2,
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
            underlying_contract_id: 1001,
            underlying_price: 100.0,
            underlying_ask: Some(100.1),
            option_contract_id: 2001,
            strike: 103.0,
            expiry: "20260515".to_string(),
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            days_to_expiration: 30,
            option_bid: 1.5,
            option_ask: Some(1.6),
            delta: Some(0.25),
            itm_depth_pct: 0.03,
            downside_buffer_pct: 0.15,
            expiration_profit_per_share: 5.0,
            annualized_yield_pct: 20.0,
            expiration_yield_pct: 5.0,
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

        let result = build_order_intents(
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

        assert_eq!(result.rejections.len(), 1);
    }

    #[test]
    fn blocks_duplicate_symbols_already_selected_this_cycle() {
        let first = ScoredOptionCandidate {
            symbol: "AAPL".to_string(),
            beta: 1.1,
            underlying_contract_id: 1001,
            underlying_price: 100.0,
            underlying_ask: Some(100.1),
            option_contract_id: 2001,
            strike: 103.0,
            expiry: "20260515".to_string(),
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            days_to_expiration: 30,
            option_bid: 1.5,
            option_ask: Some(1.6),
            delta: Some(0.25),
            itm_depth_pct: 0.03,
            downside_buffer_pct: 0.15,
            expiration_profit_per_share: 5.0,
            annualized_yield_pct: 20.0,
            expiration_yield_pct: 5.0,
            score: 0.2,
        };
        let second = ScoredOptionCandidate {
            strike: 104.0,
            expiry: "20260522".to_string(),
            option_bid: 1.3,
            score: 0.19,
            ..first.clone()
        };

        let result = build_order_intents(
            &AccountState {
                account: "DU123".to_string(),
                available_funds: Some(50_000.0),
                buying_power: Some(50_000.0),
                net_liquidation: Some(75_000.0),
            },
            &[],
            &[first, second],
            &AppConfig {
                risk: RiskConfig {
                    max_new_trades_per_cycle: 2,
                    ..RiskConfig::default()
                },
                allocation: AllocationConfig {
                    deployment_budget: 50_000.0,
                    max_cash_per_symbol_pct: 100.0,
                    min_cash_reserve_pct: 0.0,
                    ..AllocationConfig::default()
                },
                ..test_config()
            },
        );

        assert_eq!(result.intents.len(), 1);
        assert_eq!(result.rejections.len(), 0);
        assert_eq!(result.allocation_summary.candidate_symbols_considered, 1);
    }

    #[test]
    fn guarded_paper_mode_requires_available_funds_field() {
        let candidate = ScoredOptionCandidate {
            symbol: "AAPL".to_string(),
            beta: 1.1,
            underlying_contract_id: 1001,
            underlying_price: 100.0,
            underlying_ask: Some(100.1),
            option_contract_id: 2001,
            strike: 103.0,
            expiry: "20260515".to_string(),
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            days_to_expiration: 30,
            option_bid: 1.5,
            option_ask: Some(1.6),
            delta: Some(0.25),
            itm_depth_pct: 0.03,
            downside_buffer_pct: 0.15,
            expiration_profit_per_share: 5.0,
            annualized_yield_pct: 20.0,
            expiration_yield_pct: 5.0,
            score: 0.2,
        };

        let result = build_order_intents(
            &AccountState {
                account: "DU123".to_string(),
                available_funds: None,
                buying_power: Some(50_000.0),
                net_liquidation: Some(75_000.0),
            },
            &[],
            &[candidate],
            &AppConfig {
                read_only: false,
                risk: RiskConfig {
                    enable_paper_orders: true,
                    ..RiskConfig::default()
                },
                ..test_config()
            },
        );

        assert!(
            result
                .rejections
                .iter()
                .any(|rejection| { rejection.reason.contains("requires IBKR AVAILABLE_FUNDS") })
        );
    }

    #[test]
    fn builds_combo_bag_intent_from_profit_floor_debit_cap() {
        let candidate = ScoredOptionCandidate {
            symbol: "AAPL".to_string(),
            beta: 1.1,
            underlying_contract_id: 1001,
            underlying_price: 100.0,
            underlying_ask: Some(100.10),
            option_contract_id: 2001,
            strike: 90.0,
            expiry: "20260515".to_string(),
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            days_to_expiration: 30,
            option_bid: 14.0,
            option_ask: Some(14.1),
            delta: Some(0.8),
            itm_depth_pct: 0.1,
            downside_buffer_pct: 0.14,
            expiration_profit_per_share: 4.0,
            annualized_yield_pct: 20.0,
            expiration_yield_pct: 4.0,
            score: 0.2,
        };

        let result = build_order_intents(
            &AccountState {
                account: "DU123".to_string(),
                available_funds: Some(50_000.0),
                buying_power: Some(50_000.0),
                net_liquidation: Some(75_000.0),
            },
            &[],
            &[candidate],
            &test_config(),
        );

        assert!(result.rejections.is_empty());
        assert_eq!(result.intents.len(), 1);
        assert_eq!(result.intents[0].combo_limit_price, Some(89.10));
        assert_eq!(result.intents[0].estimated_net_debit, 8_910.0);
        assert!((result.intents[0].max_profit - 90.0).abs() < 0.001);
        assert_eq!(result.intents[0].legs[0].contract_id, Some(1001));
        assert_eq!(result.intents[0].legs[1].contract_id, Some(2001));
    }

    #[test]
    fn uses_annualized_yield_floor_when_it_is_more_conservative() {
        let candidate = ScoredOptionCandidate {
            symbol: "AAPL".to_string(),
            beta: 1.1,
            underlying_contract_id: 1001,
            underlying_price: 100.0,
            underlying_ask: Some(100.10),
            option_contract_id: 2001,
            strike: 90.0,
            expiry: "20261015".to_string(),
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            days_to_expiration: 180,
            option_bid: 14.0,
            option_ask: Some(14.1),
            delta: Some(0.8),
            itm_depth_pct: 0.1,
            downside_buffer_pct: 0.14,
            expiration_profit_per_share: 4.0,
            annualized_yield_pct: 20.0,
            expiration_yield_pct: 4.0,
            score: 0.2,
        };

        let result = build_order_intents(
            &AccountState {
                account: "DU123".to_string(),
                available_funds: Some(50_000.0),
                buying_power: Some(50_000.0),
                net_liquidation: Some(75_000.0),
            },
            &[],
            &[candidate],
            &test_config(),
        );

        assert!(result.rejections.is_empty());
        assert_eq!(result.intents.len(), 1);
        assert_eq!(result.intents[0].combo_limit_price, Some(84.97));
    }

    #[test]
    fn uses_absolute_profit_floor_when_it_is_more_conservative() {
        let candidate = ScoredOptionCandidate {
            symbol: "AAPL".to_string(),
            beta: 1.1,
            underlying_contract_id: 1001,
            underlying_price: 100.0,
            underlying_ask: Some(100.10),
            option_contract_id: 2001,
            strike: 99.5,
            expiry: "20260515".to_string(),
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            days_to_expiration: 30,
            option_bid: 1.5,
            option_ask: Some(1.6),
            delta: Some(0.25),
            itm_depth_pct: 0.03,
            downside_buffer_pct: 0.15,
            expiration_profit_per_share: 5.0,
            annualized_yield_pct: 20.0,
            expiration_yield_pct: 5.0,
            score: 0.2,
        };

        let result = build_order_intents(
            &AccountState {
                account: "DU123".to_string(),
                available_funds: Some(50_000.0),
                buying_power: Some(50_000.0),
                net_liquidation: Some(75_000.0),
            },
            &[],
            &[candidate],
            &AppConfig {
                allocation: AllocationConfig {
                    max_cash_per_symbol_pct: 100.0,
                    min_cash_reserve_pct: 0.0,
                    ..AllocationConfig::default()
                },
                strategy: StrategyConfig {
                    min_expiration_profit_per_share: 1.0,
                    ..test_config().strategy
                },
                ..test_config()
            },
        );

        assert!(result.rejections.is_empty());
        assert_eq!(result.intents.len(), 1);
        assert_eq!(result.intents[0].combo_limit_price, Some(98.50));
        assert_eq!(result.intents[0].max_profit, 100.0);
    }

    #[test]
    fn allocation_respects_deployment_budget_cap() {
        let result = build_order_intents(
            &AccountState {
                account: "DU123".to_string(),
                available_funds: Some(50_000.0),
                buying_power: Some(50_000.0),
                net_liquidation: Some(75_000.0),
            },
            &[],
            &[candidate("AAPL")],
            &AppConfig {
                allocation: AllocationConfig {
                    deployment_budget: 18_000.0,
                    max_cash_per_symbol_pct: 100.0,
                    min_cash_reserve_pct: 0.0,
                    ..AllocationConfig::default()
                },
                ..test_config()
            },
        );

        assert!(result.rejections.is_empty());
        assert_eq!(result.intents.len(), 1);
        assert_eq!(result.intents[0].lot_quantity, 2);
        assert_eq!(result.allocation_summary.allocated_cash, 17_820.0);
        assert_eq!(result.allocation_summary.remaining_cash, 180.0);
    }

    #[test]
    fn allocation_applies_cash_reserve_before_budget() {
        let result = build_order_intents(
            &AccountState {
                account: "DU123".to_string(),
                available_funds: Some(10_000.0),
                buying_power: Some(50_000.0),
                net_liquidation: Some(75_000.0),
            },
            &[],
            &[candidate("AAPL")],
            &AppConfig {
                allocation: AllocationConfig {
                    deployment_budget: 10_000.0,
                    max_cash_per_symbol_pct: 100.0,
                    min_cash_reserve_pct: 15.0,
                    ..AllocationConfig::default()
                },
                ..test_config()
            },
        );

        assert!(result.intents.is_empty());
        assert!(result.rejections.iter().any(|rejection| {
            rejection
                .reason
                .contains("remaining deployable cash 8500.00 is below one-lot debit 8910.00")
        }));
        assert_eq!(
            result.capital_source_details.routed_orders.reserve_amount,
            1_500.0
        );
        assert_eq!(
            result.capital_source_details.routed_orders.deployable_cash,
            8_500.0
        );
    }

    #[test]
    fn allocation_enforces_per_symbol_cap() {
        let result = build_order_intents(
            &AccountState {
                account: "DU123".to_string(),
                available_funds: Some(50_000.0),
                buying_power: Some(50_000.0),
                net_liquidation: Some(75_000.0),
            },
            &[],
            &[candidate("AAPL")],
            &AppConfig {
                allocation: AllocationConfig {
                    deployment_budget: 50_000.0,
                    max_cash_per_symbol_pct: 10.0,
                    min_cash_reserve_pct: 0.0,
                    ..AllocationConfig::default()
                },
                ..test_config()
            },
        );

        assert!(result.intents.is_empty());
        assert!(result.rejections.iter().any(|rejection| {
            rejection
                .reason
                .contains("per-symbol cash cap 5000.00 is below one-lot debit 8910.00")
        }));
    }

    #[test]
    fn allocation_sizes_single_symbol_as_multi_lot_combo() {
        let result = build_order_intents(
            &AccountState {
                account: "DU123".to_string(),
                available_funds: Some(40_000.0),
                buying_power: Some(40_000.0),
                net_liquidation: Some(75_000.0),
            },
            &[],
            &[candidate("AAPL")],
            &AppConfig {
                allocation: AllocationConfig {
                    deployment_budget: 40_000.0,
                    max_cash_per_symbol_pct: 100.0,
                    min_cash_reserve_pct: 0.0,
                    ..AllocationConfig::default()
                },
                ..test_config()
            },
        );

        assert!(result.rejections.is_empty());
        assert_eq!(result.intents.len(), 1);
        assert_eq!(result.intents[0].lot_quantity, 4);
        assert_eq!(result.intents[0].estimated_net_debit, 35_640.0);
        assert_eq!(result.intents[0].estimated_credit, 5_600.0);
        assert!((result.intents[0].max_profit - 360.0).abs() < 0.001);
        assert_eq!(result.intents[0].legs[0].quantity, 400);
        assert_eq!(result.intents[0].legs[1].quantity, 4);
        assert_eq!(result.allocation_summary.total_lots, 4);
    }

    #[test]
    fn analysis_only_buying_power_mode_sizes_from_preview_source() {
        let result = build_order_intents(
            &AccountState {
                account: "DU123".to_string(),
                available_funds: Some(1_000.0),
                buying_power: Some(20_000.0),
                net_liquidation: Some(75_000.0),
            },
            &[],
            &[candidate("AAPL")],
            &AppConfig {
                allocation: AllocationConfig {
                    deployment_budget: 20_000.0,
                    capital_source: CapitalSource::BuyingPower,
                    max_cash_per_symbol_pct: 100.0,
                    min_cash_reserve_pct: 5.0,
                    ..AllocationConfig::default()
                },
                ..test_config()
            },
        );

        assert!(result.rejections.is_empty());
        assert_eq!(result.intents.len(), 1);
        assert_eq!(result.intents[0].lot_quantity, 2);
        assert_eq!(
            result.capital_source_details.configured_source,
            "buying_power"
        );
        assert_eq!(result.capital_source_details.preview.source, "buying_power");
        assert_eq!(
            result.capital_source_details.preview.deployable_cash,
            20_000.0
        );
        assert_eq!(
            result.capital_source_details.routed_orders.deployable_cash,
            950.0
        );
    }
}
