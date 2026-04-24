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

#[derive(Debug, Clone)]
struct CandidateAllocationPlan {
    candidate: ScoredOptionCandidate,
    stock_ask: f64,
    combo_limit_price: f64,
    expiration_profit_per_share: f64,
    estimated_net_debit_per_lot: f64,
    existing_symbol_allocation: f64,
    max_lots_by_symbol_cap: i32,
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
    let existing_symbol_allocations =
        estimated_symbol_allocations(&open_positions, sizing_view.max_cash_per_symbol);
    let existing_symbols = existing_symbol_allocations
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let existing_exposure_cash = existing_symbol_allocations.values().sum::<f64>();
    let mut remaining_cash = sizing_view.deployable_cash;
    let current_open_symbols = existing_symbols.len();
    let mut candidate_plans = Vec::new();

    for candidate in &collapsed_candidates {
        let existing_symbol_allocation = existing_symbol_allocations
            .get(&candidate.symbol)
            .copied()
            .unwrap_or(0.0);
        match build_candidate_allocation_plan(
            candidate,
            config,
            sizing_view,
            existing_symbol_allocation,
        ) {
            Ok(plan) => candidate_plans.push(plan),
            Err(rejection) => rejections.push(rejection),
        }
    }

    if remaining_cash > 0.0 {
        let projected_remaining_cash = greedy_remaining_cash_after_distribution(
            &candidate_plans,
            &existing_symbols,
            current_open_symbols,
            config,
            remaining_cash,
        );
        let min_one_lot_debit = candidate_plans
            .iter()
            .map(|plan| plan.estimated_net_debit_per_lot)
            .fold(f64::INFINITY, f64::min);
        let should_refuse_partial_deployment = if min_one_lot_debit.is_finite() {
            projected_remaining_cash + 0.005 >= min_one_lot_debit
        } else {
            projected_remaining_cash > 0.0
        };

        if should_refuse_partial_deployment {
            rejections.push(GuardrailRejection {
                symbol: "allocation".to_string(),
                stage: "risk".to_string(),
                reason: format!(
                    "capped per-symbol distribution can only absorb {:.2} of the remaining deployment budget {:.2}; proceeding with the valid subset",
                    remaining_cash - projected_remaining_cash,
                    remaining_cash
                ),
            });
        }
    }

    let mut new_symbol_intents = 0usize;
    for plan in candidate_plans {
        if remaining_cash <= 0.0 {
            break;
        }
        if intents.len() >= config.risk.max_new_trades_per_cycle {
            break;
        }

        let consumes_new_open_slot = !existing_symbols.contains(&plan.candidate.symbol);
        if consumes_new_open_slot
            && current_open_symbols + new_symbol_intents >= config.risk.max_open_positions
        {
            rejections.push(GuardrailRejection {
                symbol: plan.candidate.symbol.clone(),
                stage: "risk".to_string(),
                reason: format!(
                    "max open position cap {} would be exceeded",
                    config.risk.max_open_positions
                ),
            });
            continue;
        }

        let max_lots_by_remaining_cash =
            (remaining_cash / plan.estimated_net_debit_per_lot).floor() as i32;
        if max_lots_by_remaining_cash < 1 {
            rejections.push(GuardrailRejection {
                symbol: plan.candidate.symbol.clone(),
                stage: "risk".to_string(),
                reason: format!(
                    "remaining deployment budget {:.2} is below one-lot debit {:.2}",
                    remaining_cash, plan.estimated_net_debit_per_lot
                ),
            });
            continue;
        }

        let lot_quantity = max_lots_by_remaining_cash.min(plan.max_lots_by_symbol_cap);
        if lot_quantity < 1 {
            rejections.push(GuardrailRejection {
                symbol: plan.candidate.symbol.clone(),
                stage: "risk".to_string(),
                reason: format!(
                    "no lot quantity fit within remaining deployment budget {:.2} and per-symbol distribution headroom {:.2}",
                    remaining_cash,
                    (sizing_view.max_cash_per_symbol - plan.existing_symbol_allocation).max(0.0)
                ),
            });
            continue;
        }

        let estimated_credit = plan.candidate.option_bid * 100.0 * lot_quantity as f64;
        let estimated_net_debit = plan.estimated_net_debit_per_lot * lot_quantity as f64;
        let max_profit = plan.expiration_profit_per_share * 100.0 * lot_quantity as f64;
        let stock_quantity = 100 * lot_quantity;

        remaining_cash = (remaining_cash - estimated_net_debit).max(0.0);
        if consumes_new_open_slot {
            new_symbol_intents += 1;
        }

        intents.push(OrderIntent {
            symbol: plan.candidate.symbol.clone(),
            strategy: "deep-ITM covered-call buy-write".to_string(),
            account: account.account.clone(),
            mode: if config.guarded_paper_submission_enabled() {
                "paper-combo-bag".to_string()
            } else {
                "analysis-only".to_string()
            },
            lot_quantity,
            combo_limit_price: Some(plan.combo_limit_price),
            estimated_net_debit,
            estimated_credit,
            max_profit,
            legs: vec![
                OrderLegIntent {
                    instrument_type: InstrumentType::Stock,
                    action: TradeAction::Buy,
                    contract_id: Some(plan.candidate.underlying_contract_id),
                    symbol: plan.candidate.symbol.clone(),
                    description: format!(
                        "Buy {stock_quantity} shares of {}",
                        plan.candidate.symbol
                    ),
                    quantity: stock_quantity,
                    limit_price: Some(plan.stock_ask),
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
                    contract_id: Some(plan.candidate.option_contract_id),
                    symbol: plan.candidate.symbol.clone(),
                    description: format!(
                        "Sell {} deep-ITM covered call contract(s) {} {} {}",
                        lot_quantity,
                        plan.candidate.symbol,
                        plan.candidate.expiry,
                        plan.candidate.strike
                    ),
                    quantity: lot_quantity,
                    limit_price: Some(plan.candidate.option_bid),
                    expiry: Some(plan.candidate.expiry.clone()),
                    strike: Some(plan.candidate.strike),
                    right: Some(plan.candidate.right.clone()),
                    exchange: Some(plan.candidate.exchange.clone()),
                    trading_class: Some(plan.candidate.trading_class.clone()),
                    multiplier: Some(plan.candidate.multiplier.clone()),
                    currency: Some("USD".to_string()),
                },
            ],
        });
    }

    OrderIntentBuildResult {
        allocation_summary: AllocationSummary {
            candidate_symbols_considered: collapsed_candidates.len(),
            selected_symbols: intents.len(),
            total_lots: intents.iter().map(|intent| intent.lot_quantity).sum(),
            existing_exposure_cash,
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
        config.allocation.min_cash_reserve_ratio,
        config.allocation.deployment_budget,
        config.allocation.max_cash_per_symbol_ratio,
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
            config.allocation.max_cash_per_symbol_ratio,
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
    reserve_ratio: f64,
    deployment_budget: f64,
    max_cash_per_symbol_ratio: f64,
) -> CapitalAllocationView {
    let reported_amount = reported_amount.filter(|value| value.is_finite() && *value > 0.0);
    let reserve_amount = reported_amount.unwrap_or(0.0) * reserve_ratio.max(0.0);
    let cash_after_reserve = (reported_amount.unwrap_or(0.0) - reserve_amount).max(0.0);
    let deployment_budget = deployment_budget.max(0.0);
    let deployable_cash = cash_after_reserve.min(deployment_budget);
    let max_cash_per_symbol = deployment_budget * max_cash_per_symbol_ratio.max(0.0);

    CapitalAllocationView {
        source: source.to_string(),
        reported_amount,
        reserve_ratio,
        reserve_amount,
        cash_after_reserve,
        deployment_budget,
        deployable_cash,
        max_cash_per_symbol,
    }
}

fn build_candidate_allocation_plan(
    candidate: &ScoredOptionCandidate,
    config: &AppConfig,
    sizing_view: &CapitalAllocationView,
    existing_symbol_allocation: f64,
) -> Result<CandidateAllocationPlan, GuardrailRejection> {
    let Some(stock_ask) = candidate.underlying_ask else {
        return Err(GuardrailRejection {
            symbol: candidate.symbol.clone(),
            stage: "pricing".to_string(),
            reason: "missing underlying ask required for combo BAG debit pricing".to_string(),
        });
    };

    let combo_limit_price = floor_to_cents(combo_limit_price_from_profit_floor(
        candidate,
        &config.strategy,
    ));
    if combo_limit_price <= 0.0 {
        return Err(GuardrailRejection {
            symbol: candidate.symbol.clone(),
            stage: "pricing".to_string(),
            reason: format!(
                "combo debit {:.2} is non-positive after applying the configured profit-floor guardrails",
                combo_limit_price
            ),
        });
    }

    let expiration_profit_per_share = (candidate.strike - combo_limit_price).max(0.0);
    if expiration_profit_per_share < config.strategy.min_expiration_profit_per_share {
        return Err(GuardrailRejection {
            symbol: candidate.symbol.clone(),
            stage: "pricing".to_string(),
            reason: format!(
                "combo BAG debit {:.2} yields only {:.2} expiration profit per share, below configured minimum {:.2}",
                combo_limit_price,
                expiration_profit_per_share,
                config.strategy.min_expiration_profit_per_share
            ),
        });
    }

    let expiration_yield_ratio = expiration_profit_per_share / combo_limit_price;
    if expiration_yield_ratio < config.strategy.min_expiration_yield_ratio {
        return Err(GuardrailRejection {
            symbol: candidate.symbol.clone(),
            stage: "pricing".to_string(),
            reason: format!(
                "combo BAG debit {:.2} yields only {:.2}% to expiration, below configured minimum {:.2}%",
                combo_limit_price,
                expiration_yield_ratio * 100.0,
                config.strategy.min_expiration_yield_ratio * 100.0
            ),
        });
    }

    let annualized_yield_ratio =
        expiration_yield_ratio / (candidate.days_to_expiration as f64 / 365.0);
    if annualized_yield_ratio < config.strategy.min_annualized_yield_ratio {
        return Err(GuardrailRejection {
            symbol: candidate.symbol.clone(),
            stage: "pricing".to_string(),
            reason: format!(
                "combo BAG debit {:.2} yields only {:.2}% annualized, below configured minimum {:.2}%",
                combo_limit_price,
                annualized_yield_ratio * 100.0,
                config.strategy.min_annualized_yield_ratio * 100.0
            ),
        });
    }

    if sizing_view.reported_amount.is_none() {
        return Err(GuardrailRejection {
            symbol: candidate.symbol.clone(),
            stage: "risk".to_string(),
            reason: missing_capital_reason(config, sizing_view),
        });
    }

    let estimated_net_debit_per_lot = combo_limit_price * 100.0;
    if estimated_net_debit_per_lot <= 0.0 {
        return Err(GuardrailRejection {
            symbol: candidate.symbol.clone(),
            stage: "pricing".to_string(),
            reason: format!(
                "combo BAG debit {:.2} does not leave any positive deployable cash per lot",
                estimated_net_debit_per_lot
            ),
        });
    }

    let per_symbol_headroom =
        (sizing_view.max_cash_per_symbol - existing_symbol_allocation).max(0.0);
    let max_lots_by_symbol_cap = (per_symbol_headroom / estimated_net_debit_per_lot).floor() as i32;
    if max_lots_by_symbol_cap < 1 {
        let reason = if existing_symbol_allocation > 0.0 {
            format!(
                "remaining per-symbol distribution headroom {:.2} after existing brokerage exposure {:.2} is below one-lot debit {:.2}",
                per_symbol_headroom, existing_symbol_allocation, estimated_net_debit_per_lot
            )
        } else {
            format!(
                "per-symbol distribution cap {:.2} is below one-lot debit {:.2}",
                sizing_view.max_cash_per_symbol, estimated_net_debit_per_lot
            )
        };
        return Err(GuardrailRejection {
            symbol: candidate.symbol.clone(),
            stage: "risk".to_string(),
            reason,
        });
    }

    Ok(CandidateAllocationPlan {
        candidate: candidate.clone(),
        stock_ask,
        combo_limit_price,
        expiration_profit_per_share,
        estimated_net_debit_per_lot,
        existing_symbol_allocation,
        max_lots_by_symbol_cap,
    })
}

fn estimated_symbol_allocations(
    open_positions: &[OpenPositionState],
    per_symbol_distribution_cap: f64,
) -> BTreeMap<String, f64> {
    open_positions
        .iter()
        .filter_map(|position| {
            let allocation =
                estimate_open_position_allocation(position, per_symbol_distribution_cap);
            if allocation > 0.0 {
                Some((position.symbol.clone(), allocation))
            } else {
                None
            }
        })
        .collect()
}

fn estimate_open_position_allocation(
    position: &OpenPositionState,
    per_symbol_distribution_cap: f64,
) -> f64 {
    let effective_share_count = position
        .stock_shares
        .abs()
        .max(position.short_call_contracts.max(0.0) * 100.0);

    if effective_share_count <= 0.0 {
        return 0.0;
    }

    match position.average_stock_cost {
        Some(cost) if cost.is_finite() && cost > 0.0 => effective_share_count * cost,
        _ if position.short_call_contracts > 0.0 => per_symbol_distribution_cap,
        _ => 0.0,
    }
}

fn greedy_remaining_cash_after_distribution(
    plans: &[CandidateAllocationPlan],
    existing_symbols: &BTreeSet<String>,
    current_open_symbols: usize,
    config: &AppConfig,
    starting_cash: f64,
) -> f64 {
    let mut remaining_cash = starting_cash;
    let mut remaining_trade_slots = config.risk.max_new_trades_per_cycle;
    let mut additional_open_symbols = 0usize;

    for plan in plans {
        if remaining_trade_slots == 0 || remaining_cash <= 0.0 {
            break;
        }

        let consumes_new_open_slot = !existing_symbols.contains(&plan.candidate.symbol);
        if consumes_new_open_slot
            && current_open_symbols + additional_open_symbols >= config.risk.max_open_positions
        {
            continue;
        }

        let max_lots_by_remaining_cash =
            (remaining_cash / plan.estimated_net_debit_per_lot).floor() as i32;
        let lot_quantity = max_lots_by_remaining_cash.min(plan.max_lots_by_symbol_cap);
        if lot_quantity < 1 {
            continue;
        }

        remaining_cash =
            (remaining_cash - plan.estimated_net_debit_per_lot * lot_quantity as f64).max(0.0);
        remaining_trade_slots -= 1;
        if consumes_new_open_slot {
            additional_open_symbols += 1;
        }
    }

    remaining_cash
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
        max_debit_for_yield_floor(candidate.strike, strategy.min_expiration_yield_ratio);
    let annualized_floor =
        strategy.min_annualized_yield_ratio * (candidate.days_to_expiration as f64 / 365.0);
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
            LogsConfig, MarketDataMode, PerformanceConfig, RiskConfig, RunMode, RuntimeMode,
            StrategyConfig,
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
            risk: RiskConfig {
                max_new_trades_per_cycle: 5,
                max_open_positions: 5,
                ..RiskConfig::default()
            },
            allocation: AllocationConfig {
                max_cash_per_symbol_ratio: 1.0,
                min_cash_reserve_ratio: 0.0,
                ..AllocationConfig::default()
            },
            performance: PerformanceConfig::default(),
            execution: ExecutionTuningConfig::default(),
            logs: LogsConfig::default(),
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
            itm_depth_ratio: 0.1,
            downside_buffer_ratio: 0.14,
            expiration_profit_per_share: 4.0,
            annualized_yield_ratio: 0.20,
            expiration_yield_ratio: 0.04,
            score: 0.2,
        }
    }

    fn spread_candidate(symbol: &str) -> ScoredOptionCandidate {
        ScoredOptionCandidate {
            symbol: symbol.to_string(),
            beta: 1.1,
            underlying_contract_id: 1001,
            underlying_price: 25.0,
            underlying_ask: Some(25.05),
            option_contract_id: 2001,
            strike: 20.0,
            expiry: "20260515".to_string(),
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: symbol.to_string(),
            multiplier: "100".to_string(),
            days_to_expiration: 30,
            option_bid: 5.5,
            option_ask: Some(5.6),
            delta: Some(0.8),
            itm_depth_ratio: 0.1,
            downside_buffer_ratio: 0.14,
            expiration_profit_per_share: 0.2,
            annualized_yield_ratio: 0.20,
            expiration_yield_ratio: 0.01,
            score: 0.2,
        }
    }

    #[test]
    fn summarizes_stock_and_short_call_positions() {
        let positions = vec![
            InventoryPosition {
                account: "DU123".to_string(),
                symbol: "IBM".to_string(),
                security_type: "STK".to_string(),
                quantity: 100.0,
                average_cost: 180.0,
                expiry: None,
                strike: None,
                right: None,
            },
            InventoryPosition {
                account: "DU123".to_string(),
                symbol: "IBM".to_string(),
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
    fn existing_brokerage_positions_consume_symbol_distribution_headroom() {
        let candidate = ScoredOptionCandidate {
            symbol: "IBM".to_string(),
            beta: 1.1,
            underlying_contract_id: 1001,
            underlying_price: 100.0,
            underlying_ask: Some(100.1),
            option_contract_id: 2001,
            strike: 103.0,
            expiry: "20260515".to_string(),
            right: "C".to_string(),
            exchange: "SMART".to_string(),
            trading_class: "IBM".to_string(),
            multiplier: "100".to_string(),
            days_to_expiration: 30,
            option_bid: 1.5,
            option_ask: Some(1.6),
            delta: Some(0.25),
            itm_depth_ratio: 0.03,
            downside_buffer_ratio: 0.15,
            expiration_profit_per_share: 5.0,
            annualized_yield_ratio: 0.20,
            expiration_yield_ratio: 0.05,
            score: 0.2,
        };
        let positions = vec![InventoryPosition {
            account: "DU123".to_string(),
            symbol: "IBM".to_string(),
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
            &AppConfig {
                allocation: AllocationConfig {
                    deployment_budget: 10_000.0,
                    max_cash_per_symbol_ratio: 0.20,
                    min_cash_reserve_ratio: 0.0,
                    ..AllocationConfig::default()
                },
                ..test_config()
            },
        );

        assert!(result.intents.is_empty());
        assert!(result.rejections.iter().any(|rejection| {
            rejection
                .reason
                .contains("existing brokerage exposure 9000.00")
        }));
        assert_eq!(result.allocation_summary.existing_exposure_cash, 9_000.0);
        assert_eq!(result.allocation_summary.allocated_cash, 0.0);
        assert_eq!(result.allocation_summary.remaining_cash, 10_000.0);
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
            itm_depth_ratio: 0.03,
            downside_buffer_ratio: 0.15,
            expiration_profit_per_share: 5.0,
            annualized_yield_ratio: 0.20,
            expiration_yield_ratio: 0.05,
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
                    max_cash_per_symbol_ratio: 1.0,
                    min_cash_reserve_ratio: 0.0,
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
            itm_depth_ratio: 0.03,
            downside_buffer_ratio: 0.15,
            expiration_profit_per_share: 5.0,
            annualized_yield_ratio: 0.20,
            expiration_yield_ratio: 0.05,
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
            itm_depth_ratio: 0.1,
            downside_buffer_ratio: 0.14,
            expiration_profit_per_share: 4.0,
            annualized_yield_ratio: 0.20,
            expiration_yield_ratio: 0.04,
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
            itm_depth_ratio: 0.1,
            downside_buffer_ratio: 0.14,
            expiration_profit_per_share: 4.0,
            annualized_yield_ratio: 0.20,
            expiration_yield_ratio: 0.04,
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
            itm_depth_ratio: 0.03,
            downside_buffer_ratio: 0.15,
            expiration_profit_per_share: 5.0,
            annualized_yield_ratio: 0.20,
            expiration_yield_ratio: 0.05,
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
                    max_cash_per_symbol_ratio: 1.0,
                    min_cash_reserve_ratio: 0.0,
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
                    max_cash_per_symbol_ratio: 1.0,
                    min_cash_reserve_ratio: 0.0,
                    ..AllocationConfig::default()
                },
                ..test_config()
            },
        );

        assert!(result.rejections.is_empty());
        assert_eq!(result.intents.len(), 1);
        assert_eq!(result.intents[0].lot_quantity, 2);
        assert_eq!(result.allocation_summary.existing_exposure_cash, 0.0);
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
                    max_cash_per_symbol_ratio: 1.0,
                    min_cash_reserve_ratio: 0.15,
                    ..AllocationConfig::default()
                },
                ..test_config()
            },
        );

        assert!(result.intents.is_empty());
        assert!(result.rejections.iter().any(|rejection| {
            rejection
                .reason
                .contains("remaining deployment budget 8500.00 is below one-lot debit 8910.00")
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
                    max_cash_per_symbol_ratio: 0.10,
                    min_cash_reserve_ratio: 0.0,
                    ..AllocationConfig::default()
                },
                ..test_config()
            },
        );

        assert!(result.intents.is_empty());
        assert!(result.rejections.iter().any(|rejection| {
            rejection
                .reason
                .contains("per-symbol distribution cap 5000.00 is below one-lot debit 8910.00")
        }));
    }

    #[test]
    fn allocation_spreads_budget_across_five_symbols_under_default_distribution_cap() {
        let result = build_order_intents(
            &AccountState {
                account: "DU123".to_string(),
                available_funds: Some(50_000.0),
                buying_power: Some(50_000.0),
                net_liquidation: Some(75_000.0),
            },
            &[],
            &[
                spread_candidate("AAPL"),
                spread_candidate("MSFT"),
                spread_candidate("NVDA"),
                spread_candidate("AMD"),
                spread_candidate("META"),
            ],
            &AppConfig {
                allocation: AllocationConfig {
                    deployment_budget: 10_000.0,
                    max_cash_per_symbol_ratio: 0.20,
                    min_cash_reserve_ratio: 0.0,
                    ..AllocationConfig::default()
                },
                ..test_config()
            },
        );

        assert!(result.rejections.is_empty());
        assert_eq!(result.intents.len(), 5);
        assert!(result.intents.iter().all(|intent| intent.lot_quantity == 1));
        assert_eq!(result.allocation_summary.total_lots, 5);
        assert_eq!(result.allocation_summary.existing_exposure_cash, 0.0);
        assert_eq!(result.allocation_summary.allocated_cash, 9_900.0);
        assert_eq!(result.allocation_summary.remaining_cash, 100.0);
    }

    #[test]
    fn allocation_keeps_valid_subset_when_budget_cannot_fill_under_cap() {
        let result = build_order_intents(
            &AccountState {
                account: "DU123".to_string(),
                available_funds: Some(50_000.0),
                buying_power: Some(50_000.0),
                net_liquidation: Some(75_000.0),
            },
            &[],
            &[
                spread_candidate("AAPL"),
                spread_candidate("MSFT"),
                spread_candidate("NVDA"),
                spread_candidate("AMD"),
            ],
            &AppConfig {
                allocation: AllocationConfig {
                    deployment_budget: 10_000.0,
                    max_cash_per_symbol_ratio: 0.20,
                    min_cash_reserve_ratio: 0.0,
                    ..AllocationConfig::default()
                },
                ..test_config()
            },
        );

        assert_eq!(result.intents.len(), 4);
        assert!(result.intents.iter().all(|intent| intent.lot_quantity == 1));
        assert_eq!(result.allocation_summary.total_lots, 4);
        assert_eq!(result.allocation_summary.allocated_cash, 7_920.0);
        assert_eq!(result.allocation_summary.remaining_cash, 2_080.0);
        assert!(result.rejections.iter().any(|rejection| {
            rejection
                .reason
                .contains("proceeding with the valid subset")
        }));
    }

    #[test]
    fn existing_positions_do_not_reduce_new_trade_deployment_budget() {
        let positions = vec![InventoryPosition {
            account: "DU123".to_string(),
            symbol: "ORCL".to_string(),
            security_type: "STK".to_string(),
            quantity: 100.0,
            average_cost: 19.8,
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
            &[
                spread_candidate("MSFT"),
                spread_candidate("NVDA"),
                spread_candidate("AMD"),
                spread_candidate("META"),
            ],
            &AppConfig {
                allocation: AllocationConfig {
                    deployment_budget: 9_900.0,
                    max_cash_per_symbol_ratio: 0.20,
                    min_cash_reserve_ratio: 0.0,
                    ..AllocationConfig::default()
                },
                ..test_config()
            },
        );

        assert_eq!(result.intents.len(), 4);
        assert!(result.intents.iter().all(|intent| intent.lot_quantity == 1));
        assert_eq!(result.allocation_summary.existing_exposure_cash, 1_980.0);
        assert_eq!(result.allocation_summary.allocated_cash, 7_920.0);
        assert_eq!(result.allocation_summary.remaining_cash, 1_980.0);
        assert!(result.rejections.iter().any(|rejection| {
            rejection
                .reason
                .contains("proceeding with the valid subset")
        }));
    }

    #[test]
    fn oversized_existing_symbol_does_not_zero_out_budget_for_other_symbols() {
        let positions = vec![InventoryPosition {
            account: "DU123".to_string(),
            symbol: "ORCL".to_string(),
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
            &[
                spread_candidate("ORCL"),
                spread_candidate("MSFT"),
                spread_candidate("NVDA"),
                spread_candidate("AMD"),
                spread_candidate("META"),
                spread_candidate("TSLA"),
            ],
            &AppConfig {
                risk: RiskConfig {
                    max_open_positions: 6,
                    ..test_config().risk
                },
                allocation: AllocationConfig {
                    deployment_budget: 10_000.0,
                    max_cash_per_symbol_ratio: 0.20,
                    min_cash_reserve_ratio: 0.0,
                    ..AllocationConfig::default()
                },
                ..test_config()
            },
        );

        assert_eq!(result.intents.len(), 5);
        assert!(result.intents.iter().all(|intent| intent.symbol != "ORCL"));
        assert!(result.rejections.iter().any(|rejection| {
            rejection.symbol == "ORCL"
                && rejection
                    .reason
                    .contains("existing brokerage exposure 9000.00")
        }));
        assert_eq!(result.allocation_summary.existing_exposure_cash, 9_000.0);
        assert_eq!(result.allocation_summary.allocated_cash, 9_900.0);
        assert_eq!(result.allocation_summary.remaining_cash, 100.0);
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
                    max_cash_per_symbol_ratio: 1.0,
                    min_cash_reserve_ratio: 0.0,
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
                    max_cash_per_symbol_ratio: 1.0,
                    min_cash_reserve_ratio: 0.05,
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
