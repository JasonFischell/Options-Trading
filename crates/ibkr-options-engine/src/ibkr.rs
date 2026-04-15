use anyhow::{Context, Result};
use ibapi::accounts::types::AccountGroup;
use ibapi::market_data::MarketDataType;
use ibapi::market_data::realtime::{TickPriceSize, TickType, TickTypes};
use ibapi::prelude::{
    AccountSummaryResult, AccountSummaryTags, Client, Contract, Currency, Exchange, SecurityType,
    Symbol,
};
use tokio::time::{Duration, timeout};

use crate::config::AppConfig;

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

impl SnapshotSummary {
    pub fn reference_price(&self) -> Option<f64> {
        self.last
            .or(self.close)
            .or_else(|| match (self.bid, self.ask) {
                (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
                (Some(bid), None) => Some(bid),
                (None, Some(ask)) => Some(ask),
                (None, None) => None,
            })
    }
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

pub async fn log_account_summary(client: &Client) -> Result<()> {
    let tags = &[
        AccountSummaryTags::ACCOUNT_TYPE,
        AccountSummaryTags::NET_LIQUIDATION,
        AccountSummaryTags::TOTAL_CASH_VALUE,
        AccountSummaryTags::BUYING_POWER,
    ];

    println!("Requesting IBKR account summary...");

    let mut subscription = client
        .account_summary(&AccountGroup("All".to_string()), tags)
        .await
        .context("failed to request IBKR account summary")?;

    while let Some(result) = subscription.next().await {
        match result.context("failed to receive IBKR account summary update")? {
            AccountSummaryResult::Summary(summary) => {
                if summary.currency.is_empty() {
                    println!(
                        "Account summary: account={} tag={} value={}",
                        summary.account, summary.tag, summary.value
                    );
                } else {
                    println!(
                        "Account summary: account={} tag={} value={} currency={}",
                        summary.account, summary.tag, summary.value, summary.currency
                    );
                }
            }
            AccountSummaryResult::End => {
                println!("Account summary request complete.");
                break;
            }
        }
    }

    Ok(())
}

pub async fn log_stock_contract_details(client: &Client, symbols: &[String]) -> Result<()> {
    for symbol in symbols {
        println!("Requesting contract details for {}...", symbol);

        let contract = Contract {
            symbol: Symbol::from(symbol.as_str()),
            security_type: SecurityType::Stock,
            exchange: Exchange::from("SMART"),
            currency: Currency::from("USD"),
            ..Default::default()
        };

        let details = client
            .contract_details(&contract)
            .await
            .with_context(|| format!("failed to request contract details for {symbol}"))?;

        println!(
            "Received {} contract detail match(es) for {}.",
            details.len(),
            symbol
        );

        if let Some(primary) = details.first() {
            println!(
                "Primary match: symbol={} local_symbol={} contract_id={} exchange={} primary_exchange={} currency={} long_name={} min_tick={}",
                primary.contract.symbol,
                primary.contract.local_symbol,
                primary.contract.contract_id,
                primary.contract.exchange,
                primary.contract.primary_exchange,
                primary.contract.currency,
                primary.long_name,
                primary.min_tick
            );
        }
    }

    Ok(())
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

pub async fn log_option_chain_for_underlying(
    client: &Client,
    symbol: &str,
    contract_id: i32,
) -> Result<Vec<OptionChainSummary>> {
    println!(
        "Requesting option chain for {} using underlying contract_id={}...",
        symbol, contract_id
    );

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
                    println!(
                        "No option chain data arrived for {} within {} seconds.",
                        symbol,
                        idle_timeout.as_secs()
                    );
                } else {
                    println!(
                        "Option chain stream idle after {} response(s); ending diagnostic request for {}.",
                        chain_count, symbol
                    );
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

        let expirations_preview = if chain.expirations.is_empty() {
            "none".to_string()
        } else {
            chain
                .expirations
                .iter()
                .take(5)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        };

        let strikes_preview = if chain.strikes.is_empty() {
            "none".to_string()
        } else {
            chain
                .strikes
                .iter()
                .take(5)
                .map(|strike| format!("{strike:.2}"))
                .collect::<Vec<_>>()
                .join(", ")
        };

        println!(
            "Option chain {}: exchange={} trading_class={} multiplier={} underlying_contract_id={}",
            chain_count,
            chain.exchange,
            chain.trading_class,
            chain.multiplier,
            chain.underlying_contract_id
        );
        println!(
            "  Expirations sample ({} total): {}",
            chain.expirations.len(),
            expirations_preview
        );
        println!(
            "  Strikes sample ({} total): {}",
            chain.strikes.len(),
            strikes_preview
        );

        if chain_count >= max_chain_messages {
            println!(
                "Read {} option chain response(s); ending diagnostic request for {}.",
                chain_count, symbol
            );
            break;
        }
    }

    if chain_count == 0 {
        println!("No option chain data returned for {}.", symbol);
    } else {
        println!("Option chain request complete for {}.", symbol);
    }

    Ok(summaries)
}

pub async fn switch_to_frozen_market_data(client: &Client) -> Result<()> {
    client
        .switch_market_data_type(MarketDataType::Frozen)
        .await
        .context("failed to switch IBKR market data type to Frozen")?;
    println!("Switched IBKR market data type to Frozen for after-hours snapshot testing.");
    Ok(())
}

pub async fn request_snapshot(
    client: &Client,
    contract: &Contract,
    generic_ticks: &[&str],
    label: &str,
) -> Result<SnapshotSummary> {
    println!("Requesting snapshot market data for {}...", label);

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
            Err(_) => {
                println!(
                    "Snapshot for {} timed out after {} seconds.",
                    label,
                    idle_timeout.as_secs()
                );
                break;
            }
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
                println!(
                    "Market data notice for {}: code={} message={}",
                    label, notice.code, notice.message
                );
            }
            TickTypes::SnapshotEnd => {
                println!("Snapshot request complete for {}.", label);
                break;
            }
            _ => {}
        }
    }

    println!(
        "Snapshot summary for {}: bid={:?} ask={:?} last={:?} close={:?} option_price={:?} implied_volatility={:?} delta={:?} underlying_price={:?}",
        label,
        summary.bid,
        summary.ask,
        summary.last,
        summary.close,
        summary.option_price,
        summary.implied_volatility,
        summary.delta,
        summary.underlying_price
    );

    Ok(summary)
}

pub fn select_option_contract(
    symbol: &str,
    chains: &[OptionChainSummary],
    reference_price: f64,
) -> Result<SelectedOptionContract> {
    let chain = chains
        .iter()
        .find(|chain| !chain.expirations.is_empty() && !chain.strikes.is_empty())
        .with_context(|| format!("no usable option chain responses found for {symbol}"))?;

    let expiration = chain
        .expirations
        .iter()
        .min()
        .with_context(|| format!("no expiration dates available for {symbol}"))?
        .clone();

    let strike = chain
        .strikes
        .iter()
        .min_by(|left, right| {
            (*left - reference_price)
                .abs()
                .partial_cmp(&(*right - reference_price).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .copied()
        .with_context(|| format!("no strikes available for {symbol}"))?;

    Ok(SelectedOptionContract {
        symbol: symbol.to_string(),
        right: "C".to_string(),
        expiration,
        strike,
        exchange: chain.exchange.clone(),
        trading_class: chain.trading_class.clone(),
        multiplier: chain.multiplier.clone(),
        underlying_contract_id: chain.underlying_contract_id,
    })
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

    println!(
        "Resolved option contract: symbol={} local_symbol={} contract_id={} expiry={} strike={} right={} exchange={} trading_class={}",
        primary.contract.symbol,
        primary.contract.local_symbol,
        primary.contract.contract_id,
        primary.contract.last_trade_date_or_contract_month,
        primary.contract.strike,
        primary.contract.right,
        primary.contract.exchange,
        primary.contract.trading_class
    );

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
    use super::{OptionChainSummary, SnapshotSummary, select_option_contract};

    #[test]
    fn reference_price_prefers_last_then_close_then_midpoint() {
        let summary = SnapshotSummary {
            bid: Some(9.0),
            ask: Some(11.0),
            last: None,
            close: Some(10.5),
            option_price: None,
            implied_volatility: None,
            delta: None,
            underlying_price: None,
        };

        assert_eq!(summary.reference_price(), Some(10.5));
    }

    #[test]
    fn selects_nearest_strike_on_earliest_expiration() {
        let chains = vec![OptionChainSummary {
            underlying_contract_id: 123,
            trading_class: "PTON".to_string(),
            multiplier: "100".to_string(),
            exchange: "NASDAQOM".to_string(),
            expirations: vec!["20260417".to_string(), "20260424".to_string()],
            strikes: vec![4.0, 4.5, 5.0, 5.5],
        }];

        let selected = select_option_contract("PTON", &chains, 4.6).unwrap();
        assert_eq!(selected.expiration, "20260417");
        assert_eq!(selected.strike, 4.5);
        assert_eq!(selected.right, "C");
    }
}
