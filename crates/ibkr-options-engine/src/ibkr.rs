use std::collections::BTreeMap;

use anyhow::{Context, Result};
use ibapi::Error as IbkrError;
use ibapi::accounts::types::{AccountGroup, AccountId};
use ibapi::accounts::{AccountSummaryResult, AccountSummaryTags, AccountUpdate, PositionUpdate};
use ibapi::market_data::MarketDataType;
use ibapi::market_data::realtime::{TickPriceSize, TickType, TickTypes};
use ibapi::orders::Orders;
use ibapi::prelude::{Client, Contract, Currency, Exchange, SecurityType, Symbol};
use tokio::time::{Duration, Instant, timeout};
use tracing::{info, warn};

use crate::{
    config::{AppConfig, MarketDataMode},
    models::{
        AccountState, BrokerCompletedOrder, BrokerOpenOrder, InventoryPosition,
        OptionQuoteSnapshot, UnderlyingSnapshot,
    },
    strategy::parse_expiry_date,
};

#[derive(Debug, Clone)]
pub struct IbkrClientDescriptor {
    pub endpoint: String,
    pub client_id: i32,
    pub account: String,
    pub read_only: bool,
}

impl From<&AppConfig> for IbkrClientDescriptor {
    fn from(config: &AppConfig) -> Self {
        Self {
            endpoint: config.endpoint(),
            client_id: config.client_id,
            account: config.account.clone(),
            read_only: config.read_only,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OptionChainSummary {
    pub underlying_contract_id: i32,
    pub trading_class: String,
    pub multiplier: String,
    pub exchange: String,
    pub expirations: Vec<String>,
    pub strikes: Vec<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotSummary {
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub last: Option<f64>,
    pub close: Option<f64>,
    pub option_price: Option<f64>,
    pub implied_volatility: Option<f64>,
    pub delta: Option<f64>,
    pub underlying_price: Option<f64>,
    pub beta: Option<f64>,
    pub observed_tick_types: Vec<String>,
    pub notices: Vec<String>,
}

impl SnapshotSummary {
    pub fn data_origin_label(&self) -> &'static str {
        if self
            .observed_tick_types
            .iter()
            .any(|tick_type| tick_type.starts_with("Delayed"))
        {
            "delayed-or-delayed-frozen"
        } else if self.observed_tick_types.is_empty() {
            "unknown"
        } else {
            "realtime-or-frozen"
        }
    }
}

#[derive(Debug, Clone)]
pub struct OptionChainMetadata {
    pub exchange: String,
    pub trading_class: String,
    pub multiplier: String,
    pub underlying_contract_id: i32,
}

#[derive(Debug, Clone)]
pub struct SelectedOptionContract {
    pub symbol: String,
    pub right: String,
    pub expiration: String,
    pub strike: f64,
    pub chain_metadata: Vec<OptionChainMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OptionResolutionCandidate {
    exchange: String,
    trading_class: String,
    multiplier: String,
}

pub async fn log_server_time(client: &Client) -> Result<()> {
    let server_time = client
        .server_time()
        .await
        .context("connected to IBKR but failed to request server time")?;

    println!("Connected to IBKR, server time is {}.", server_time);
    Ok(())
}

pub async fn connect(endpoint: &str, client_id: i32) -> Result<Client> {
    Client::connect(endpoint, client_id)
        .await
        .with_context(|| format!("failed to connect to IBKR at {endpoint}"))
}

pub async fn fetch_account_state(client: &Client, account: &str) -> Result<AccountState> {
    let mut state = AccountState {
        account: account.to_string(),
        available_funds: None,
        buying_power: None,
        net_liquidation: None,
    };
    let mut cash_available_funds = None;
    let mut fallback_available_funds = None;
    let mut diagnostics = AccountMetricDiagnostics::default();

    populate_account_state_from_summary(
        client,
        account,
        &mut state,
        &mut cash_available_funds,
        &mut fallback_available_funds,
        &mut diagnostics,
    )
    .await?;

    if !has_any_account_metric(&state) {
        populate_account_state_from_updates(
            client,
            account,
            &mut state,
            &mut cash_available_funds,
            &mut fallback_available_funds,
            &mut diagnostics,
        )
        .await?;
    }

    if cash_available_funds.is_some() {
        state.available_funds = cash_available_funds;
    } else if state.available_funds.is_none() {
        state.available_funds = fallback_available_funds;
    }

    info!(
        configured_account = %account,
        summary_rows_seen = diagnostics.summary_rows_seen,
        summary_rows_matched = diagnostics.summary_rows_matched,
        update_rows_seen = diagnostics.update_rows_seen,
        update_rows_matched = diagnostics.update_rows_matched,
        available_funds = ?state.available_funds,
        buying_power = ?state.buying_power,
        net_liquidation = ?state.net_liquidation,
        "resolved IBKR account metrics"
    );
    if !has_any_account_metric(&state) {
        warn!(
            configured_account = %account,
            summary_rows_seen = diagnostics.summary_rows_seen,
            summary_rows_matched = diagnostics.summary_rows_matched,
            update_rows_seen = diagnostics.update_rows_seen,
            update_rows_matched = diagnostics.update_rows_matched,
            samples = %diagnostics.render_samples(),
            "IBKR returned no usable account metrics; inspect raw account metric samples"
        );
    }

    Ok(state)
}

async fn populate_account_state_from_summary(
    client: &Client,
    account: &str,
    state: &mut AccountState,
    cash_available_funds: &mut Option<f64>,
    fallback_available_funds: &mut Option<f64>,
    diagnostics: &mut AccountMetricDiagnostics,
) -> Result<()> {
    let tags = &[
        AccountSummaryTags::NET_LIQUIDATION,
        ACCOUNT_SUMMARY_TAG_TOTAL_CASH_VALUE,
        AccountSummaryTags::AVAILABLE_FUNDS,
        AccountSummaryTags::FULL_AVAILABLE_FUNDS,
        AccountSummaryTags::LOOK_AHEAD_AVAILABLE_FUNDS,
        AccountSummaryTags::BUYING_POWER,
    ];

    let mut subscription = client
        .account_summary(&AccountGroup("All".to_string()), tags)
        .await
        .context("failed to request IBKR account summary")?;

    while let Some(result) = subscription.next().await {
        match result.context("failed to receive IBKR account summary update")? {
            AccountSummaryResult::Summary(summary) => {
                diagnostics.summary_rows_seen += 1;
                let matched = account_summary_matches(account, &summary.account);
                diagnostics.push_sample(format!(
                    "summary account={} tag={} value={} currency={} matched={}",
                    summary.account, summary.tag, summary.value, summary.currency, matched
                ));
                if !matched {
                    continue;
                }
                diagnostics.summary_rows_matched += 1;

                apply_account_metric(
                    state,
                    cash_available_funds,
                    fallback_available_funds,
                    &summary.tag,
                    summary.value.parse::<f64>().ok(),
                );
            }
            AccountSummaryResult::End => break,
        }
    }

    Ok(())
}

async fn populate_account_state_from_updates(
    client: &Client,
    account: &str,
    state: &mut AccountState,
    cash_available_funds: &mut Option<f64>,
    fallback_available_funds: &mut Option<f64>,
    diagnostics: &mut AccountMetricDiagnostics,
) -> Result<()> {
    let mut subscription = client
        .account_updates(&AccountId::from(account))
        .await
        .context("failed to request IBKR account updates")?;

    while let Some(result) = subscription.next().await {
        match result.context("failed to receive IBKR account update")? {
            AccountUpdate::AccountValue(value) => {
                diagnostics.update_rows_seen += 1;
                let update_account = value.account.as_deref().unwrap_or("");
                let matched =
                    update_account.is_empty() || account_summary_matches(account, update_account);
                diagnostics.push_sample(format!(
                    "update account={} key={} value={} currency={} matched={}",
                    if update_account.is_empty() {
                        "<none>"
                    } else {
                        update_account
                    },
                    value.key,
                    value.value,
                    value.currency,
                    matched
                ));
                if let Some(update_account) = value.account.as_deref()
                    && !account_summary_matches(account, update_account)
                {
                    continue;
                }
                diagnostics.update_rows_matched += 1;

                apply_account_metric(
                    state,
                    cash_available_funds,
                    fallback_available_funds,
                    &value.key,
                    value.value.parse::<f64>().ok(),
                );
            }
            AccountUpdate::End => break,
            AccountUpdate::PortfolioValue(_) | AccountUpdate::UpdateTime(_) => {}
        }
    }

    Ok(())
}

fn apply_account_metric(
    state: &mut AccountState,
    cash_available_funds: &mut Option<f64>,
    fallback_available_funds: &mut Option<f64>,
    key: &str,
    parsed_value: Option<f64>,
) {
    match canonical_account_metric_key(key) {
        Some(ACCOUNT_SUMMARY_TAG_TOTAL_CASH_VALUE) => {
            *cash_available_funds = parsed_value;
        }
        Some(AccountSummaryTags::AVAILABLE_FUNDS) => state.available_funds = parsed_value,
        Some(AccountSummaryTags::FULL_AVAILABLE_FUNDS)
        | Some(AccountSummaryTags::LOOK_AHEAD_AVAILABLE_FUNDS) => {
            if fallback_available_funds.is_none() {
                *fallback_available_funds = parsed_value;
            }
        }
        Some(AccountSummaryTags::BUYING_POWER) => state.buying_power = parsed_value,
        Some(AccountSummaryTags::NET_LIQUIDATION) => state.net_liquidation = parsed_value,
        _ => {}
    }
}

fn canonical_account_metric_key(key: &str) -> Option<&'static str> {
    for candidate in [
        ACCOUNT_SUMMARY_TAG_TOTAL_CASH_VALUE,
        AccountSummaryTags::AVAILABLE_FUNDS,
        AccountSummaryTags::FULL_AVAILABLE_FUNDS,
        AccountSummaryTags::LOOK_AHEAD_AVAILABLE_FUNDS,
        AccountSummaryTags::BUYING_POWER,
        AccountSummaryTags::NET_LIQUIDATION,
    ] {
        if key == candidate || key.starts_with(&format!("{candidate}-")) {
            return Some(candidate);
        }
    }

    None
}

const ACCOUNT_SUMMARY_TAG_TOTAL_CASH_VALUE: &str = "TotalCashValue";

fn has_any_account_metric(state: &AccountState) -> bool {
    state.available_funds.is_some()
        || state.buying_power.is_some()
        || state.net_liquidation.is_some()
}

#[derive(Debug, Default)]
struct AccountMetricDiagnostics {
    summary_rows_seen: usize,
    summary_rows_matched: usize,
    update_rows_seen: usize,
    update_rows_matched: usize,
    samples: Vec<String>,
}

impl AccountMetricDiagnostics {
    const MAX_SAMPLES: usize = 12;

    fn push_sample(&mut self, sample: String) {
        if self.samples.len() < Self::MAX_SAMPLES {
            self.samples.push(sample);
        }
    }

    fn render_samples(&self) -> String {
        if self.samples.is_empty() {
            "none".to_string()
        } else {
            self.samples.join(" | ")
        }
    }
}

fn account_summary_matches(configured_account: &str, summary_account: &str) -> bool {
    let configured = normalized_account_id(configured_account);
    let summary = normalized_account_id(summary_account);

    if configured.is_empty() || summary.is_empty() {
        return configured == summary;
    }

    configured == summary || configured.ends_with(&summary) || summary.ends_with(&configured)
}

fn normalized_account_id(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_uppercase)
        .collect()
}

pub async fn fetch_positions(client: &Client) -> Result<Vec<InventoryPosition>> {
    let mut subscription = client
        .positions()
        .await
        .context("failed to request IBKR positions")?;

    let mut positions = Vec::new();
    while let Some(result) = subscription.next().await {
        match result.context("failed to receive position update")? {
            PositionUpdate::Position(position) => positions.push(InventoryPosition {
                account: position.account.clone(),
                symbol: position.contract.symbol.to_string(),
                security_type: position.contract.security_type.to_string(),
                quantity: position.position,
                average_cost: position.average_cost,
                expiry: (!position
                    .contract
                    .last_trade_date_or_contract_month
                    .is_empty())
                .then(|| position.contract.last_trade_date_or_contract_month.clone()),
                strike: (position.contract.strike > 0.0).then_some(position.contract.strike),
                right: (!position.contract.right.is_empty())
                    .then(|| position.contract.right.clone()),
            }),
            PositionUpdate::PositionEnd => break,
        }
    }

    Ok(positions)
}

pub async fn fetch_open_orders(client: &Client, account: &str) -> Result<Vec<BrokerOpenOrder>> {
    let mut subscription = client
        .all_open_orders()
        .await
        .context("failed to request IBKR open orders")?;

    let mut orders_by_id = BTreeMap::new();
    let mut status_by_id = BTreeMap::new();
    while let Some(result) = subscription.next().await {
        match result.context("failed to receive open order update")? {
            Orders::OrderData(order) => {
                if order.order.account != account {
                    continue;
                }
                let (status, filled_quantity, remaining_quantity) =
                    status_by_id.remove(&order.order_id).unwrap_or_else(|| {
                        (
                            order.order_state.status.clone(),
                            0.0,
                            order.order.total_quantity,
                        )
                    });
                orders_by_id.insert(
                    order.order_id,
                    BrokerOpenOrder {
                        account: order.order.account.clone(),
                        order_id: order.order_id,
                        client_id: order.order.client_id,
                        perm_id: order.order.perm_id,
                        order_ref: order.order.order_ref.clone(),
                        symbol: order.contract.symbol.to_string(),
                        security_type: order.contract.security_type.to_string(),
                        action: format!("{:?}", order.order.action),
                        total_quantity: order.order.total_quantity,
                        order_type: format!("{:?}", order.order.order_type),
                        limit_price: order.order.limit_price.filter(|price| *price > 0.0),
                        status,
                        filled_quantity,
                        remaining_quantity,
                    },
                );
            }
            Orders::OrderStatus(status) => {
                if let Some(order) = orders_by_id.get_mut(&status.order_id) {
                    if !status.status.is_empty() {
                        order.status = status.status.clone();
                    }
                    order.filled_quantity = status.filled;
                    order.remaining_quantity = status.remaining;
                } else {
                    status_by_id.insert(
                        status.order_id,
                        (status.status.clone(), status.filled, status.remaining),
                    );
                }
            }
            Orders::Notice(_) => {}
        }
    }

    Ok(orders_by_id.into_values().collect())
}

pub async fn fetch_completed_orders(
    client: &Client,
    account: &str,
) -> Result<Vec<BrokerCompletedOrder>> {
    let mut subscription = client
        .completed_orders(false)
        .await
        .context("failed to request IBKR completed orders")?;

    let mut orders = Vec::new();
    while let Some(result) = subscription.next().await {
        match result.context("failed to receive completed order update")? {
            Orders::OrderData(order) => {
                if order.order.account != account {
                    continue;
                }
                orders.push(BrokerCompletedOrder {
                    account: order.order.account.clone(),
                    order_id: order.order_id,
                    client_id: order.order.client_id,
                    perm_id: order.order.perm_id,
                    symbol: order.contract.symbol.to_string(),
                    security_type: order.contract.security_type.to_string(),
                    action: format!("{:?}", order.order.action),
                    total_quantity: order.order.total_quantity,
                    order_type: format!("{:?}", order.order.order_type),
                    limit_price: order.order.limit_price.filter(|price| *price > 0.0),
                    status: order.order_state.status.clone(),
                    completed_status: order.order_state.completed_status.clone(),
                    reject_reason: order.order_state.reject_reason.clone(),
                    warning_text: order.order_state.warning_text.clone(),
                    completed_time: order.order_state.completed_time.clone(),
                });
            }
            Orders::OrderStatus(_) | Orders::Notice(_) => {}
        }
    }

    Ok(orders)
}

pub async fn cancel_open_order(client: &Client, order_id: i32) -> Result<()> {
    let mut subscription = client
        .cancel_order(order_id, "")
        .await
        .with_context(|| format!("failed to request cancellation for IBKR order {order_id}"))?;
    let started = Instant::now();
    let collection_window = Duration::from_secs(5);
    let idle_timeout = Duration::from_secs(1);

    while started.elapsed() < collection_window {
        match timeout(idle_timeout, subscription.next()).await {
            Ok(Some(event)) => {
                event.with_context(|| {
                    format!("failed while monitoring cancellation for IBKR order {order_id}")
                })?;
            }
            Ok(None) | Err(_) => break,
        }
    }

    Ok(())
}

pub async fn resolve_primary_stock_contract_id(client: &Client, symbol: &str) -> Result<i32> {
    Ok(resolve_primary_stock_contract(client, symbol)
        .await?
        .contract_id)
}

pub async fn resolve_primary_stock_contract(client: &Client, symbol: &str) -> Result<Contract> {
    let contract = Contract {
        symbol: Symbol::from(symbol),
        security_type: SecurityType::Stock,
        exchange: Exchange::from("SMART"),
        currency: Currency::from("USD"),
        ..Default::default()
    };

    let details = client
        .contract_details(&contract)
        .await
        .with_context(|| format!("failed to resolve underlying contract details for {symbol}"))?;

    let primary = details
        .first()
        .with_context(|| format!("no stock contract details returned for {symbol}"))?;

    Ok(Contract {
        contract_id: primary.contract.contract_id,
        symbol: primary.contract.symbol.clone(),
        security_type: primary.contract.security_type.clone(),
        last_trade_date_or_contract_month: primary
            .contract
            .last_trade_date_or_contract_month
            .clone(),
        strike: primary.contract.strike,
        right: primary.contract.right.clone(),
        multiplier: primary.contract.multiplier.clone(),
        exchange: primary.contract.exchange.clone(),
        primary_exchange: primary.contract.primary_exchange.clone(),
        currency: primary.contract.currency.clone(),
        local_symbol: primary.contract.local_symbol.clone(),
        trading_class: primary.contract.trading_class.clone(),
        ..Default::default()
    })
}

pub async fn request_option_chain_for_underlying(
    client: &Client,
    symbol: &str,
    contract_id: i32,
) -> Result<Vec<OptionChainSummary>> {
    let mut option_chain_stream = client
        .option_chain(symbol, "", SecurityType::Stock, contract_id)
        .await
        .with_context(|| format!("failed to request option chain for {symbol}"))?;

    let mut chain_count = 0usize;
    let max_chain_messages = 3usize;
    let idle_timeout = Duration::from_secs(5);
    let mut summaries = Vec::new();

    loop {
        let next_item = timeout(idle_timeout, option_chain_stream.next()).await;

        let next_stream_item = match next_item {
            Ok(item) => item,
            Err(_) => {
                break;
            }
        };

        let Some(result) = next_stream_item else {
            break;
        };

        let chain =
            result.with_context(|| format!("error while receiving option chain for {symbol}"))?;
        chain_count += 1;
        summaries.push(OptionChainSummary {
            underlying_contract_id: chain.underlying_contract_id,
            trading_class: chain.trading_class.clone(),
            multiplier: chain.multiplier.clone(),
            exchange: chain.exchange.clone(),
            expirations: chain.expirations.clone(),
            strikes: chain.strikes.clone(),
        });

        if chain_count >= max_chain_messages {
            break;
        }
    }

    Ok(summaries)
}

pub async fn switch_market_data_mode(client: &Client, mode: MarketDataMode) -> Result<()> {
    let ibkr_mode = match mode {
        MarketDataMode::Live => MarketDataType::Realtime,
        MarketDataMode::Frozen => MarketDataType::Frozen,
        MarketDataMode::Delayed => MarketDataType::Delayed,
        MarketDataMode::DelayedFrozen => MarketDataType::DelayedFrozen,
    };

    client
        .switch_market_data_type(ibkr_mode)
        .await
        .context("failed to switch IBKR market data mode")?;
    info!(requested_mode = %market_data_mode_label(mode), "requested IBKR market data mode");
    Ok(())
}

pub async fn request_snapshot(
    client: &Client,
    contract: &Contract,
    generic_ticks: &[&str],
    label: &str,
) -> Result<SnapshotSummary> {
    let mut subscription = client
        .market_data(contract)
        .generic_ticks(generic_ticks)
        .snapshot()
        .subscribe()
        .await
        .with_context(|| format!("failed to request market data snapshot for {label}"))?;

    let idle_timeout = Duration::from_secs(10);
    let mut summary = SnapshotSummary::default();

    loop {
        let next_item = timeout(idle_timeout, subscription.next()).await;
        let next_stream_item = match next_item {
            Ok(item) => item,
            Err(_) => break,
        };

        let Some(result) = next_stream_item else {
            break;
        };

        match result
            .with_context(|| format!("error while receiving market data snapshot for {label}"))?
        {
            TickTypes::Price(price) => {
                record_tick_type(&mut summary, &price.tick_type);
                update_snapshot_from_price(&mut summary, &price.tick_type, price.price)
            }
            TickTypes::PriceSize(price_size) => {
                record_tick_type(&mut summary, &price_size.price_tick_type);
                update_snapshot_from_price_size(&mut summary, &price_size)
            }
            TickTypes::OptionComputation(computation) => {
                if summary.option_price.is_none() {
                    summary.option_price = computation.option_price;
                }
                if summary.implied_volatility.is_none() {
                    summary.implied_volatility = computation.implied_volatility;
                }
                if summary.delta.is_none() {
                    summary.delta = computation.delta;
                }
                if summary.underlying_price.is_none() {
                    summary.underlying_price = computation.underlying_price;
                }
            }
            TickTypes::String(tick_string) => {
                record_tick_type(&mut summary, &tick_string.tick_type);
                if matches!(tick_string.tick_type, TickType::FundamentalRatios)
                    && summary.beta.is_none()
                {
                    summary.beta = parse_beta_from_fundamental_ratios(&tick_string.value);
                }
            }
            TickTypes::Generic(generic_tick) => {
                record_tick_type(&mut summary, &generic_tick.tick_type);
            }
            TickTypes::Notice(notice) => {
                summary
                    .notices
                    .push(format!("{}: {}", notice.code, notice.message));
            }
            TickTypes::SnapshotEnd => break,
            _ => {}
        }
    }

    Ok(summary)
}

pub async fn request_underlying_snapshot(
    client: &Client,
    symbol: &str,
) -> Result<UnderlyingSnapshot> {
    let contract = resolve_primary_stock_contract(client, symbol).await?;
    request_underlying_snapshot_for_contract(client, symbol, &contract).await
}

pub async fn request_underlying_snapshot_for_contract(
    client: &Client,
    symbol: &str,
    contract: &Contract,
) -> Result<UnderlyingSnapshot> {
    let snapshot = request_snapshot(client, contract, &[], &format!("{symbol} underlying")).await?;
    let price = snapshot
        .last
        .or(snapshot.close)
        .or_else(|| match (snapshot.bid, snapshot.ask) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            (Some(bid), None) => Some(bid),
            (None, Some(ask)) => Some(ask),
            (None, None) => None,
        })
        .unwrap_or_default();

    Ok(UnderlyingSnapshot {
        contract_id: contract.contract_id,
        symbol: symbol.to_string(),
        price,
        bid: snapshot.bid,
        ask: snapshot.ask,
        last: snapshot.last,
        close: snapshot.close,
        implied_volatility: snapshot.implied_volatility,
        beta: None,
        price_source: snapshot.data_origin_label().to_string(),
        market_data_notices: snapshot.diagnostics(),
    })
}

pub fn select_buy_write_contracts(
    symbol: &str,
    chains: &[OptionChainSummary],
    reference_price: f64,
    config: &AppConfig,
) -> Result<Vec<SelectedOptionContract>> {
    let max_strike = reference_price * (1.0 - config.strategy.min_itm_depth_ratio);
    let min_strike = reference_price * (1.0 - config.strategy.max_itm_depth_ratio);
    let mut candidates: Vec<SelectedOptionContract> = Vec::new();
    let mut total_expirations = 0usize;
    let mut expirations_matching_filter = 0usize;
    let mut strikes_matching_filter = 0usize;

    for chain in chains {
        for expiration in &chain.expirations {
            total_expirations += 1;
            let expiry_date = match parse_expiry_date(expiration) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let days_to_expiration = (expiry_date - chrono::Utc::now().date_naive()).num_days();
            if days_to_expiration <= 0
                || !config
                    .strategy
                    .expiration_dates
                    .iter()
                    .any(|configured| configured == expiration)
            {
                continue;
            }
            expirations_matching_filter += 1;

            let mut strikes = chain
                .strikes
                .iter()
                .copied()
                .filter(|strike| {
                    *strike > 0.0
                        && *strike < reference_price
                        && *strike >= min_strike
                        && *strike <= max_strike
                })
                .collect::<Vec<_>>();
            strikes_matching_filter += strikes.len();
            strikes.sort_by(|left, right| {
                right.partial_cmp(left).unwrap_or(std::cmp::Ordering::Equal)
            });

            for strike in strikes
                .into_iter()
                .take(config.risk.max_option_quotes_per_underlying)
            {
                let metadata = OptionChainMetadata {
                    exchange: chain.exchange.clone(),
                    trading_class: chain.trading_class.clone(),
                    multiplier: chain.multiplier.clone(),
                    underlying_contract_id: chain.underlying_contract_id,
                };

                if let Some(existing) = candidates.iter_mut().find(|candidate| {
                    candidate.expiration == *expiration
                        && candidate.strike.to_bits() == strike.to_bits()
                        && candidate.right == "C"
                }) {
                    if existing.chain_metadata.iter().all(|existing_metadata| {
                        existing_metadata.exchange != metadata.exchange
                            || existing_metadata.trading_class != metadata.trading_class
                            || existing_metadata.multiplier != metadata.multiplier
                            || existing_metadata.underlying_contract_id
                                != metadata.underlying_contract_id
                    }) {
                        existing.chain_metadata.push(metadata);
                    }
                    continue;
                }

                candidates.push(SelectedOptionContract {
                    symbol: symbol.to_string(),
                    right: "C".to_string(),
                    expiration: expiration.clone(),
                    strike,
                    chain_metadata: vec![metadata],
                });
            }
        }
    }

    candidates.sort_by(|left, right| {
        right.expiration.cmp(&left.expiration).then_with(|| {
            right
                .strike
                .partial_cmp(&left.strike)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    });
    candidates.truncate(config.risk.max_option_quotes_per_underlying);

    if candidates.is_empty() {
        anyhow::bail!(
            "no covered-call option contracts matched for {symbol}; reference_price={reference_price:.2}, min_strike={min_strike:.2}, max_strike={max_strike:.2}, chain_responses={}, expirations_seen={}, expirations_matching_filter={}, strikes_matching_filter={}, configured_expirations={}",
            chains.len(),
            total_expirations,
            expirations_matching_filter,
            strikes_matching_filter,
            config.strategy.expiration_dates.join(",")
        );
    }

    Ok(candidates)
}

pub fn is_invalid_option_contract_error(error: &anyhow::Error) -> bool {
    if error
        .to_string()
        .contains("no option contract details returned")
    {
        return true;
    }

    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<IbkrError>(),
            Some(IbkrError::Message(200, _))
        )
    })
}

pub fn is_invalid_underlying_contract_error(error: &anyhow::Error) -> bool {
    if error
        .to_string()
        .contains("no stock contract details returned")
    {
        return true;
    }

    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<IbkrError>(),
            Some(IbkrError::Message(200, _))
        )
    })
}

pub async fn resolve_option_contract(
    client: &Client,
    selected: &SelectedOptionContract,
) -> Result<Contract> {
    let mut last_invalid_error = None;

    for candidate in option_resolution_candidates(selected) {
        let mut option_contract = Contract::option(
            &selected.symbol,
            &selected.expiration,
            selected.strike,
            &selected.right,
        );
        option_contract.exchange = Exchange::from(if candidate.exchange.is_empty() {
            "SMART"
        } else {
            candidate.exchange.as_str()
        });
        option_contract.trading_class = candidate.trading_class.clone();
        option_contract.multiplier = candidate.multiplier.clone();

        let details = match client
            .contract_details(&option_contract)
            .await
            .with_context(|| {
                format!(
                    "failed to resolve option contract details for {} {} {} {} on {} {}",
                    selected.symbol,
                    selected.expiration,
                    selected.strike,
                    selected.right,
                    candidate.exchange,
                    candidate.trading_class
                )
            }) {
            Ok(details) => details,
            Err(error) if is_invalid_option_contract_error(&error) => {
                last_invalid_error = Some(error);
                continue;
            }
            Err(error) => return Err(error),
        };

        let Some(primary) = details.first() else {
            last_invalid_error = Some(anyhow::anyhow!(
                "no option contract details returned for {} {} {} {} on {} {}",
                selected.symbol,
                selected.expiration,
                selected.strike,
                selected.right,
                candidate.exchange,
                candidate.trading_class
            ));
            continue;
        };

        return Ok(Contract {
            contract_id: primary.contract.contract_id,
            symbol: primary.contract.symbol.clone(),
            security_type: primary.contract.security_type.clone(),
            last_trade_date_or_contract_month: primary
                .contract
                .last_trade_date_or_contract_month
                .clone(),
            strike: primary.contract.strike,
            right: primary.contract.right.clone(),
            multiplier: primary.contract.multiplier.clone(),
            exchange: primary.contract.exchange.clone(),
            primary_exchange: primary.contract.primary_exchange.clone(),
            currency: primary.contract.currency.clone(),
            local_symbol: primary.contract.local_symbol.clone(),
            trading_class: primary.contract.trading_class.clone(),
            ..Default::default()
        });
    }

    Err(last_invalid_error.unwrap_or_else(|| {
        anyhow::anyhow!(
            "no valid option contract metadata variants remained for {} {} {} {}",
            selected.symbol,
            selected.expiration,
            selected.strike,
            selected.right
        )
    }))
}

fn option_resolution_candidates(
    selected: &SelectedOptionContract,
) -> Vec<OptionResolutionCandidate> {
    let mut candidates = Vec::new();

    for metadata in &selected.chain_metadata {
        let smart_variant = OptionResolutionCandidate {
            exchange: "SMART".to_string(),
            trading_class: metadata.trading_class.clone(),
            multiplier: metadata.multiplier.clone(),
        };
        if !candidates.contains(&smart_variant) {
            candidates.push(smart_variant);
        }

        let direct_variant = OptionResolutionCandidate {
            exchange: if metadata.exchange.is_empty() {
                "SMART".to_string()
            } else {
                metadata.exchange.clone()
            },
            trading_class: metadata.trading_class.clone(),
            multiplier: metadata.multiplier.clone(),
        };
        if !candidates.contains(&direct_variant) {
            candidates.push(direct_variant);
        }
    }

    if candidates.is_empty() {
        candidates.push(OptionResolutionCandidate {
            exchange: "SMART".to_string(),
            trading_class: selected.symbol.clone(),
            multiplier: "100".to_string(),
        });
    }

    candidates
}

pub async fn request_option_quote(
    client: &Client,
    selected: SelectedOptionContract,
) -> Result<OptionQuoteSnapshot> {
    let option_contract = resolve_option_contract(client, &selected).await?;
    let option_label = format!(
        "{} {} {} {}",
        selected.symbol, selected.expiration, selected.right, selected.strike
    );
    let mut snapshot = request_snapshot(client, &option_contract, &[], &option_label).await?;
    let used_model_fallback = if option_snapshot_has_usable_premium(&snapshot) {
        false
    } else {
        let fallback = request_snapshot(
            client,
            &option_contract,
            &["106"],
            &format!("{option_label} model"),
        )
        .await?;
        merge_snapshot_summary(&mut snapshot, fallback);
        true
    };

    Ok(OptionQuoteSnapshot {
        contract_id: option_contract.contract_id,
        symbol: option_contract.symbol.to_string(),
        expiry: option_contract.last_trade_date_or_contract_month.clone(),
        strike: option_contract.strike,
        right: option_contract.right.clone(),
        exchange: option_contract.exchange.to_string(),
        trading_class: option_contract.trading_class.clone(),
        multiplier: option_contract.multiplier.clone(),
        bid: snapshot.bid,
        ask: snapshot.ask,
        last: snapshot.last,
        close: snapshot.close,
        option_price: snapshot.option_price,
        implied_volatility: snapshot.implied_volatility,
        delta: snapshot.delta,
        underlying_price: snapshot.underlying_price,
        quote_source: Some(if used_model_fallback {
            "default+model-fallback".to_string()
        } else {
            "default-snapshot".to_string()
        }),
        diagnostics: snapshot.diagnostics(),
    })
}

fn option_snapshot_has_usable_premium(snapshot: &SnapshotSummary) -> bool {
    snapshot.bid.is_some()
        || snapshot.option_price.is_some()
        || matches!((snapshot.bid, snapshot.ask), (Some(_), Some(_)))
        || snapshot.last.is_some()
        || snapshot.close.is_some()
}

fn merge_snapshot_summary(primary: &mut SnapshotSummary, fallback: SnapshotSummary) {
    if primary.bid.is_none() {
        primary.bid = fallback.bid;
    }
    if primary.ask.is_none() {
        primary.ask = fallback.ask;
    }
    if primary.last.is_none() {
        primary.last = fallback.last;
    }
    if primary.close.is_none() {
        primary.close = fallback.close;
    }
    if primary.option_price.is_none() {
        primary.option_price = fallback.option_price;
    }
    if primary.implied_volatility.is_none() {
        primary.implied_volatility = fallback.implied_volatility;
    }
    if primary.delta.is_none() {
        primary.delta = fallback.delta;
    }
    if primary.underlying_price.is_none() {
        primary.underlying_price = fallback.underlying_price;
    }
    if primary.beta.is_none() {
        primary.beta = fallback.beta;
    }
    primary.notices.extend(fallback.notices);
    for tick_type in fallback.observed_tick_types {
        if !primary
            .observed_tick_types
            .iter()
            .any(|existing| existing == &tick_type)
        {
            primary.observed_tick_types.push(tick_type);
        }
    }
}

fn parse_beta_from_fundamental_ratios(value: &str) -> Option<f64> {
    value
        .split(';')
        .find_map(|entry| {
            let (key, raw_value) = entry.split_once('=')?;
            (key.trim().eq_ignore_ascii_case("beta")).then_some(raw_value.trim())
        })
        .and_then(|raw_value| raw_value.parse::<f64>().ok())
        .filter(|beta| beta.is_finite() && *beta > 0.0)
}

fn update_snapshot_from_price(summary: &mut SnapshotSummary, tick_type: &TickType, price: f64) {
    let Some(price) = normalize_market_price(price) else {
        return;
    };

    match tick_type {
        TickType::Bid | TickType::DelayedBid => summary.bid = Some(price),
        TickType::Ask | TickType::DelayedAsk => summary.ask = Some(price),
        TickType::Last | TickType::DelayedLast => summary.last = Some(price),
        TickType::Close | TickType::DelayedClose => summary.close = Some(price),
        _ => {}
    }
}

fn record_tick_type(summary: &mut SnapshotSummary, tick_type: &TickType) {
    let tick_type = format!("{tick_type:?}");
    if !summary
        .observed_tick_types
        .iter()
        .any(|existing| existing == &tick_type)
    {
        summary.observed_tick_types.push(tick_type);
    }
}

fn update_snapshot_from_price_size(summary: &mut SnapshotSummary, tick: &TickPriceSize) {
    update_snapshot_from_price(summary, &tick.price_tick_type, tick.price);
}

fn normalize_market_price(price: f64) -> Option<f64> {
    (price.is_finite() && price > 0.0).then_some(price)
}

impl SnapshotSummary {
    fn diagnostics(&self) -> Vec<String> {
        let mut diagnostics = self.notices.clone();

        if !self.observed_tick_types.is_empty() {
            diagnostics.push(format!(
                "observed tick types: {}",
                self.observed_tick_types.join(", ")
            ));
        }

        diagnostics.push(format!(
            "observed data origin: {}",
            self.data_origin_label()
        ));

        diagnostics
    }
}

pub fn market_data_mode_label(mode: MarketDataMode) -> &'static str {
    match mode {
        MarketDataMode::Live => "live",
        MarketDataMode::Frozen => "frozen",
        MarketDataMode::Delayed => "delayed",
        MarketDataMode::DelayedFrozen => "delayed-frozen",
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::{
        OptionChainMetadata, OptionChainSummary, SelectedOptionContract, SnapshotSummary,
        account_summary_matches, canonical_account_metric_key, merge_snapshot_summary,
        normalize_market_price, normalized_account_id, option_resolution_candidates,
        parse_beta_from_fundamental_ratios, select_buy_write_contracts,
    };
    use crate::config::{
        AllocationConfig, AppConfig, BrokerPlatform, ExecutionTuningConfig, MarketDataMode,
        PerformanceConfig, RiskConfig, RunMode, RuntimeMode, StrategyConfig,
    };
    use ibapi::accounts::AccountSummaryTags;

    #[test]
    fn selects_least_itm_contracts_first_within_allowed_expiration_dates() {
        let chains = vec![OptionChainSummary {
            underlying_contract_id: 123,
            trading_class: "PTON".to_string(),
            multiplier: "100".to_string(),
            exchange: "NASDAQOM".to_string(),
            expirations: vec!["20991217".to_string(), "21000121".to_string()],
            strikes: vec![4.0, 4.5, 5.0, 5.5],
        }];
        let config = AppConfig {
            host: "127.0.0.1".to_string(),
            platform: BrokerPlatform::Gateway,
            port: 4002,
            client_id: 100,
            account: "DU123456".to_string(),
            mode: RuntimeMode::Paper,
            read_only: true,
            connect_on_start: false,
            run_mode: RunMode::Manual,
            scan_schedule: "manual".to_string(),
            market_data_mode: MarketDataMode::DelayedFrozen,
            universe_file: None,
            symbols: vec!["PTON".to_string()],
            startup_warnings: Vec::new(),
            strategy: StrategyConfig {
                expiration_dates: vec!["20991217".to_string(), "21000121".to_string()],
                min_itm_depth_ratio: 0.05,
                ..StrategyConfig::default()
            },
            risk: RiskConfig {
                max_option_quotes_per_underlying: 2,
                ..RiskConfig::default()
            },
            allocation: AllocationConfig::default(),
            performance: PerformanceConfig::default(),
            execution: ExecutionTuningConfig::default(),
        };

        let selected = select_buy_write_contracts("PTON", &chains, 5.4, &config).unwrap();
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].expiration, "21000121");
        assert_eq!(selected[0].right, "C");
        assert_eq!(selected[0].strike, 5.0);
        assert_eq!(selected[1].strike, 4.5);
        assert_eq!(selected[0].chain_metadata.len(), 1);
        assert_eq!(selected[0].chain_metadata[0].trading_class, "PTON");
    }

    #[test]
    fn excludes_strikes_below_half_the_reference_price() {
        let chains = vec![OptionChainSummary {
            underlying_contract_id: 123,
            trading_class: "MARA".to_string(),
            multiplier: "100".to_string(),
            exchange: "SMART".to_string(),
            expirations: vec!["20991217".to_string()],
            strikes: vec![1.0, 2.0, 3.0, 4.0, 5.0],
        }];
        let config = AppConfig {
            host: "127.0.0.1".to_string(),
            platform: BrokerPlatform::Gateway,
            port: 4002,
            client_id: 100,
            account: "DU123456".to_string(),
            mode: RuntimeMode::Paper,
            read_only: true,
            connect_on_start: false,
            run_mode: RunMode::Manual,
            scan_schedule: "manual".to_string(),
            market_data_mode: MarketDataMode::DelayedFrozen,
            universe_file: None,
            symbols: vec!["MARA".to_string()],
            startup_warnings: Vec::new(),
            strategy: StrategyConfig {
                expiration_dates: vec!["20991217".to_string()],
                ..StrategyConfig::default()
            },
            risk: RiskConfig {
                max_option_quotes_per_underlying: 5,
                ..RiskConfig::default()
            },
            allocation: AllocationConfig::default(),
            performance: PerformanceConfig::default(),
            execution: ExecutionTuningConfig::default(),
        };

        let selected = select_buy_write_contracts("MARA", &chains, 6.0, &config).unwrap();
        let strikes = selected
            .into_iter()
            .map(|contract| contract.strike)
            .collect::<Vec<_>>();

        assert_eq!(strikes, vec![5.0, 4.0, 3.0]);
    }

    #[test]
    fn preserves_multiple_chain_metadata_variants_for_same_contract() {
        let chains = vec![
            OptionChainSummary {
                underlying_contract_id: 123,
                trading_class: "AAPL".to_string(),
                multiplier: "100".to_string(),
                exchange: "SMART".to_string(),
                expirations: vec!["20991217".to_string()],
                strikes: vec![110.0],
            },
            OptionChainSummary {
                underlying_contract_id: 456,
                trading_class: "AAPLW".to_string(),
                multiplier: "100".to_string(),
                exchange: "CBOE".to_string(),
                expirations: vec!["20991217".to_string()],
                strikes: vec![110.0],
            },
        ];
        let config = AppConfig {
            host: "127.0.0.1".to_string(),
            platform: BrokerPlatform::Gateway,
            port: 4002,
            client_id: 100,
            account: "DU123456".to_string(),
            mode: RuntimeMode::Paper,
            read_only: true,
            connect_on_start: false,
            run_mode: RunMode::Manual,
            scan_schedule: "manual".to_string(),
            market_data_mode: MarketDataMode::DelayedFrozen,
            universe_file: None,
            symbols: vec!["AAPL".to_string()],
            startup_warnings: Vec::new(),
            strategy: StrategyConfig {
                expiration_dates: vec!["20991217".to_string()],
                min_itm_depth_ratio: 0.05,
                ..StrategyConfig::default()
            },
            risk: RiskConfig {
                max_option_quotes_per_underlying: 2,
                ..RiskConfig::default()
            },
            allocation: AllocationConfig::default(),
            performance: PerformanceConfig::default(),
            execution: ExecutionTuningConfig::default(),
        };

        let selected = select_buy_write_contracts("AAPL", &chains, 120.0, &config).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].chain_metadata.len(), 2);
        assert_eq!(selected[0].chain_metadata[0].exchange, "SMART");
        assert_eq!(selected[0].chain_metadata[1].exchange, "CBOE");
    }

    #[test]
    fn merges_fallback_snapshot_fields_without_clobbering_primary_values() {
        let mut primary = SnapshotSummary {
            bid: Some(0.1),
            ask: None,
            option_price: None,
            notices: vec!["primary".to_string()],
            ..SnapshotSummary::default()
        };
        let fallback = SnapshotSummary {
            ask: Some(0.2),
            option_price: Some(0.15),
            notices: vec!["fallback".to_string()],
            ..SnapshotSummary::default()
        };

        merge_snapshot_summary(&mut primary, fallback);

        assert_eq!(primary.bid, Some(0.1));
        assert_eq!(primary.ask, Some(0.2));
        assert_eq!(primary.option_price, Some(0.15));
        assert_eq!(primary.notices, vec!["primary", "fallback"]);
    }

    #[test]
    fn normalizes_ibkr_sentinel_prices_to_none() {
        assert_eq!(normalize_market_price(-1.0), None);
        assert_eq!(normalize_market_price(0.0), None);
        assert_eq!(normalize_market_price(1.25), Some(1.25));
    }

    #[test]
    fn parses_beta_from_fundamental_ratios_snapshot_field() {
        let beta = parse_beta_from_fundamental_ratios("MKTCAP=1234.5;BETA=1.37;TTMREV=456.7");
        assert_eq!(beta, Some(1.37));
    }

    #[test]
    fn prefers_smart_before_single_exchange_option_resolution() {
        let selected = SelectedOptionContract {
            symbol: "RGTI".to_string(),
            right: "C".to_string(),
            expiration: "20991217".to_string(),
            strike: 1.0,
            chain_metadata: vec![OptionChainMetadata {
                exchange: "EDGX".to_string(),
                trading_class: "RGTI".to_string(),
                multiplier: "100".to_string(),
                underlying_contract_id: 1,
            }],
        };

        let candidates = option_resolution_candidates(&selected);

        assert_eq!(candidates[0].exchange, "SMART");
        assert_eq!(candidates[0].trading_class, "RGTI");
        assert_eq!(candidates[1].exchange, "EDGX");
    }

    #[test]
    fn configured_expiration_dates_override_previous_day_window_logic() {
        let target_expiry = (Utc::now().date_naive() + Duration::days(7))
            .format("%Y%m%d")
            .to_string();
        let later_expiry = (Utc::now().date_naive() + Duration::days(35))
            .format("%Y%m%d")
            .to_string();
        let chains = vec![OptionChainSummary {
            underlying_contract_id: 123,
            trading_class: "AAPL".to_string(),
            multiplier: "100".to_string(),
            exchange: "SMART".to_string(),
            expirations: vec![later_expiry, target_expiry.clone()],
            strikes: vec![90.0, 95.0],
        }];
        let config = AppConfig {
            host: "127.0.0.1".to_string(),
            platform: BrokerPlatform::Gateway,
            port: 4002,
            client_id: 100,
            account: "DU123456".to_string(),
            mode: RuntimeMode::Paper,
            read_only: true,
            connect_on_start: false,
            run_mode: RunMode::Manual,
            scan_schedule: "manual".to_string(),
            market_data_mode: MarketDataMode::Live,
            universe_file: None,
            symbols: vec!["AAPL".to_string()],
            startup_warnings: Vec::new(),
            strategy: StrategyConfig {
                expiration_dates: vec![target_expiry.clone()],
                min_itm_depth_ratio: 0.01,
                ..StrategyConfig::default()
            },
            risk: RiskConfig {
                max_option_quotes_per_underlying: 2,
                ..RiskConfig::default()
            },
            allocation: AllocationConfig::default(),
            performance: PerformanceConfig::default(),
            execution: ExecutionTuningConfig::default(),
        };

        let selected = select_buy_write_contracts("AAPL", &chains, 100.0, &config).unwrap();

        assert_eq!(selected.len(), 2);
        assert!(
            selected
                .iter()
                .all(|contract| contract.expiration == target_expiry)
        );
    }

    #[test]
    fn prioritizes_later_expiries_when_truncating_selected_contracts() {
        let near_expiry = (Utc::now().date_naive() + Duration::days(31))
            .format("%Y%m%d")
            .to_string();
        let far_expiry = (Utc::now().date_naive() + Duration::days(45))
            .format("%Y%m%d")
            .to_string();
        let chains = vec![OptionChainSummary {
            underlying_contract_id: 123,
            trading_class: "OPEN".to_string(),
            multiplier: "100".to_string(),
            exchange: "SMART".to_string(),
            expirations: vec![near_expiry, far_expiry.clone()],
            strikes: vec![4.5, 4.0, 3.5],
        }];
        let config = AppConfig {
            host: "127.0.0.1".to_string(),
            platform: BrokerPlatform::Gateway,
            port: 4002,
            client_id: 100,
            account: "DU123456".to_string(),
            mode: RuntimeMode::Paper,
            read_only: true,
            connect_on_start: false,
            run_mode: RunMode::Manual,
            scan_schedule: "manual".to_string(),
            market_data_mode: MarketDataMode::Live,
            universe_file: None,
            symbols: vec!["OPEN".to_string()],
            startup_warnings: Vec::new(),
            strategy: StrategyConfig {
                expiration_dates: vec![far_expiry.clone()],
                min_itm_depth_ratio: 0.01,
                ..StrategyConfig::default()
            },
            risk: RiskConfig {
                max_option_quotes_per_underlying: 2,
                ..RiskConfig::default()
            },
            allocation: AllocationConfig::default(),
            performance: PerformanceConfig::default(),
            execution: ExecutionTuningConfig::default(),
        };

        let selected = select_buy_write_contracts("OPEN", &chains, 5.9, &config).unwrap();

        assert_eq!(selected.len(), 2);
        assert!(
            selected
                .iter()
                .all(|contract| contract.expiration == far_expiry)
        );
    }

    #[test]
    fn account_summary_match_accepts_trimmed_and_suffix_variants() {
        assert!(account_summary_matches("DU1234567", "DU1234567"));
        assert!(account_summary_matches(" DU1234567 ", "du1234567"));
        assert!(account_summary_matches("DU1234567", "U1234567"));
        assert!(account_summary_matches("U1234567", "DU1234567"));
        assert!(!account_summary_matches("DU1234567", "DU7654321"));
    }

    #[test]
    fn normalized_account_id_strips_non_alphanumeric_characters() {
        assert_eq!(normalized_account_id(" du-123 4567 "), "DU1234567");
    }

    #[test]
    fn canonical_account_metric_key_accepts_segment_suffixes() {
        assert_eq!(
            canonical_account_metric_key("TotalCashValue-S"),
            Some(super::ACCOUNT_SUMMARY_TAG_TOTAL_CASH_VALUE)
        );
        assert_eq!(
            canonical_account_metric_key("AvailableFunds-S"),
            Some(AccountSummaryTags::AVAILABLE_FUNDS)
        );
        assert_eq!(
            canonical_account_metric_key("BuyingPower-C"),
            Some(AccountSummaryTags::BUYING_POWER)
        );
        assert_eq!(
            canonical_account_metric_key("NetLiquidation-S"),
            Some(AccountSummaryTags::NET_LIQUIDATION)
        );
    }
}
