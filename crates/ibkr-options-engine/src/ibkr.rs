use anyhow::{Context, Result};
use ibapi::accounts::{AccountSummaryResult, AccountSummaryTags, PositionUpdate};
use ibapi::accounts::types::AccountGroup;
use ibapi::market_data::MarketDataType;
use ibapi::market_data::realtime::{TickPriceSize, TickType, TickTypes};
use ibapi::prelude::{Client, Contract, Currency, Exchange, SecurityType, Symbol};
use tokio::time::{Duration, timeout};

use crate::{
    config::{AppConfig, MarketDataMode},
    models::{AccountState, InventoryPosition, OptionQuoteSnapshot, UnderlyingSnapshot},
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
}

#[derive(Debug, Clone)]
pub struct SelectedOptionContract {
    pub symbol: String,
    pub right: String,
    pub expiration: String,
    pub strike: f64,
    pub exchange: String,
    pub trading_class: String,
    pub multiplier: String,
    pub underlying_contract_id: i32,
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
    let tags = &[
        AccountSummaryTags::NET_LIQUIDATION,
        AccountSummaryTags::AVAILABLE_FUNDS,
        AccountSummaryTags::BUYING_POWER,
    ];

    let mut subscription = client
        .account_summary(&AccountGroup("All".to_string()), tags)
        .await
        .context("failed to request IBKR account summary")?;

    let mut state = AccountState {
        account: account.to_string(),
        available_funds: None,
        buying_power: None,
        net_liquidation: None,
    };

    while let Some(result) = subscription.next().await {
        match result.context("failed to receive IBKR account summary update")? {
            AccountSummaryResult::Summary(summary) => {
                if summary.account != account {
                    continue;
                }

                let parsed_value = summary.value.parse::<f64>().ok();
                match summary.tag.as_str() {
                    AccountSummaryTags::AVAILABLE_FUNDS => state.available_funds = parsed_value,
                    AccountSummaryTags::BUYING_POWER => state.buying_power = parsed_value,
                    AccountSummaryTags::NET_LIQUIDATION => state.net_liquidation = parsed_value,
                    _ => {}
                }
            }
            AccountSummaryResult::End => break,
        }
    }

    Ok(state)
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
                expiry: (!position.contract.last_trade_date_or_contract_month.is_empty())
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

pub async fn resolve_primary_stock_contract_id(client: &Client, symbol: &str) -> Result<i32> {
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

    Ok(primary.contract.contract_id)
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
                if chain_count == 0 {
                }
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
                update_snapshot_from_price(&mut summary, &price.tick_type, price.price)
            }
            TickTypes::PriceSize(price_size) => {
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
            TickTypes::Notice(notice) => {
                let _ = (notice.code, notice.message);
            }
            TickTypes::SnapshotEnd => break,
            _ => {}
        }
    }

    Ok(summary)
}

pub async fn request_underlying_snapshot(client: &Client, symbol: &str) -> Result<UnderlyingSnapshot> {
    let contract = Contract::stock(symbol).build();
    let snapshot = request_snapshot(client, &contract, &[], &format!("{symbol} underlying")).await?;
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
        symbol: symbol.to_string(),
        price,
        bid: snapshot.bid,
        ask: snapshot.ask,
        last: snapshot.last,
        close: snapshot.close,
        implied_volatility: snapshot.implied_volatility,
        beta: None,
    })
}

pub fn select_buy_write_contracts(
    symbol: &str,
    chains: &[OptionChainSummary],
    reference_price: f64,
    config: &AppConfig,
) -> Result<Vec<SelectedOptionContract>> {
    let min_strike = reference_price * (1.0 + config.strategy.min_strike_buffer_pct);
    let mut candidates = Vec::new();

    for chain in chains {
        for expiration in &chain.expirations {
            let expiry_date = match parse_expiry_date(expiration) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let days_to_expiration = (expiry_date - chrono::Utc::now().date_naive()).num_days();
            if days_to_expiration < config.strategy.min_expiry_days
                || days_to_expiration > config.strategy.max_expiry_days
            {
                continue;
            }

            let mut strikes = chain
                .strikes
                .iter()
                .copied()
                .filter(|strike| *strike >= min_strike)
                .collect::<Vec<_>>();
            strikes.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));

            for strike in strikes.into_iter().take(config.risk.max_option_quotes_per_underlying) {
                candidates.push(SelectedOptionContract {
                    symbol: symbol.to_string(),
                    right: "C".to_string(),
                    expiration: expiration.clone(),
                    strike,
                    exchange: chain.exchange.clone(),
                    trading_class: chain.trading_class.clone(),
                    multiplier: chain.multiplier.clone(),
                    underlying_contract_id: chain.underlying_contract_id,
                });
            }
        }
    }

    candidates.sort_by(|left, right| {
        left.expiration
            .cmp(&right.expiration)
            .then_with(|| {
                (left.strike - reference_price)
                    .abs()
                    .partial_cmp(&(right.strike - reference_price).abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });
    candidates.dedup_by(|left, right| {
        left.expiration == right.expiration
            && left.strike == right.strike
            && left.trading_class == right.trading_class
    });
    candidates.truncate(config.risk.max_option_quotes_per_underlying);

    if candidates.is_empty() {
        anyhow::bail!("no buy-write option contracts matched for {symbol}");
    }

    Ok(candidates)
}

pub async fn resolve_option_contract(
    client: &Client,
    selected: &SelectedOptionContract,
) -> Result<Contract> {
    let mut option_contract = Contract::option(
        &selected.symbol,
        &selected.expiration,
        selected.strike,
        &selected.right,
    );
    option_contract.exchange = Exchange::from("SMART");
    option_contract.trading_class = selected.trading_class.clone();
    option_contract.multiplier = selected.multiplier.clone();

    let details = client
        .contract_details(&option_contract)
        .await
        .with_context(|| {
            format!(
                "failed to resolve option contract details for {} {} {} {}",
                selected.symbol, selected.expiration, selected.strike, selected.right
            )
        })?;

    let primary = details.first().with_context(|| {
        format!(
            "no option contract details returned for {} {} {} {}",
            selected.symbol, selected.expiration, selected.strike, selected.right
        )
    })?;

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

pub async fn request_option_quote(
    client: &Client,
    selected: &SelectedOptionContract,
) -> Result<OptionQuoteSnapshot> {
    let option_contract = resolve_option_contract(client, selected).await?;
    let option_label = format!(
        "{} {} {} {}",
        selected.symbol, selected.expiration, selected.right, selected.strike
    );
    let snapshot = request_snapshot(
        client,
        &option_contract,
        &["100", "101", "104", "106"],
        &option_label,
    )
    .await?;

    Ok(OptionQuoteSnapshot {
        symbol: selected.symbol.clone(),
        expiry: selected.expiration.clone(),
        strike: selected.strike,
        right: selected.right.clone(),
        exchange: selected.exchange.clone(),
        trading_class: selected.trading_class.clone(),
        multiplier: selected.multiplier.clone(),
        bid: snapshot.bid,
        ask: snapshot.ask,
        last: snapshot.last,
        close: snapshot.close,
        option_price: snapshot.option_price,
        implied_volatility: snapshot.implied_volatility,
        delta: snapshot.delta,
        underlying_price: snapshot.underlying_price,
    })
}

fn update_snapshot_from_price(summary: &mut SnapshotSummary, tick_type: &TickType, price: f64) {
    match tick_type {
        TickType::Bid | TickType::DelayedBid => summary.bid = Some(price),
        TickType::Ask | TickType::DelayedAsk => summary.ask = Some(price),
        TickType::Last | TickType::DelayedLast => summary.last = Some(price),
        TickType::Close | TickType::DelayedClose => summary.close = Some(price),
        _ => {}
    }
}

fn update_snapshot_from_price_size(summary: &mut SnapshotSummary, tick: &TickPriceSize) {
    update_snapshot_from_price(summary, &tick.price_tick_type, tick.price);
}

#[cfg(test)]
mod tests {
    use super::{OptionChainSummary, select_buy_write_contracts};
    use crate::config::{AppConfig, MarketDataMode, RiskConfig, RunMode, RuntimeMode, StrategyConfig};

    #[test]
    fn selects_otm_contracts_on_earliest_expiration() {
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
            strategy: StrategyConfig {
                min_expiry_days: 1,
                max_expiry_days: 36500,
                min_strike_buffer_pct: 0.01,
                ..StrategyConfig::default()
            },
            risk: RiskConfig {
                max_option_quotes_per_underlying: 2,
                ..RiskConfig::default()
            },
        };

        let selected = select_buy_write_contracts("PTON", &chains, 4.6, &config).unwrap();
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].expiration, "20991217");
        assert_eq!(selected[0].right, "C");
    }
}
