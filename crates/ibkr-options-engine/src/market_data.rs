use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use csv::StringRecord;
use regex::Regex;
use tracing::{info, warn};

use crate::{
    config::{AppConfig, MarketDataMode},
    ibkr::{
        IbkrClientDescriptor, SelectedOptionContract, connect, fetch_account_state,
        fetch_positions, is_invalid_option_contract_error, log_server_time, market_data_mode_label,
        request_option_chain_for_underlying, request_option_quote, request_underlying_snapshot,
        resolve_primary_stock_contract_id, select_buy_write_contracts, switch_market_data_mode,
    },
    models::{
        AccountState, InventoryPosition, OptionQuoteSnapshot, UnderlyingSnapshot, UniverseRecord,
    },
};

#[derive(Debug, Clone)]
pub struct SymbolMarketSnapshot {
    pub underlying: UnderlyingSnapshot,
    pub option_quotes: Vec<OptionQuoteSnapshot>,
}

#[async_trait(?Send)]
pub trait MarketDataProvider {
    async fn load_account_state(&self) -> Result<AccountState>;
    async fn load_inventory(&self) -> Result<Vec<InventoryPosition>>;
    async fn fetch_symbol_snapshot(
        &self,
        record: &UniverseRecord,
        config: &AppConfig,
    ) -> Result<Option<SymbolMarketSnapshot>>;
}

pub struct IbkrMarketDataProvider {
    client: Arc<ibapi::prelude::Client>,
    account: String,
}

impl IbkrMarketDataProvider {
    pub async fn connect(config: &AppConfig) -> Result<Self> {
        let descriptor = IbkrClientDescriptor::from(config);
        let client = connect(&descriptor.endpoint, descriptor.client_id).await?;
        log_server_time(&client).await?;

        Ok(Self {
            client: Arc::new(client),
            account: config.account.clone(),
        })
    }

    pub fn shared_client(&self) -> Arc<ibapi::prelude::Client> {
        self.client.clone()
    }
}

#[async_trait(?Send)]
impl MarketDataProvider for IbkrMarketDataProvider {
    async fn load_account_state(&self) -> Result<AccountState> {
        fetch_account_state(&self.client, &self.account).await
    }

    async fn load_inventory(&self) -> Result<Vec<InventoryPosition>> {
        fetch_positions(&self.client).await
    }

    async fn fetch_symbol_snapshot(
        &self,
        record: &UniverseRecord,
        config: &AppConfig,
    ) -> Result<Option<SymbolMarketSnapshot>> {
        switch_market_data_mode(&self.client, config.market_data_mode).await?;
        let mut underlying = request_underlying_snapshot(&self.client, &record.symbol).await?;
        if matches!(config.market_data_mode, MarketDataMode::Live)
            && underlying.reference_price().is_none()
            && delayed_retry_available(&underlying.market_data_notices)
        {
            warn!(
                symbol = %record.symbol,
                "live underlying snapshot was unavailable; retrying once with delayed market data"
            );
            switch_market_data_mode(&self.client, MarketDataMode::Delayed).await?;
            underlying = request_underlying_snapshot(&self.client, &record.symbol).await?;
            underlying.market_data_notices.push(
                "scanner retried with delayed market data after the live request returned no usable underlying price"
                    .to_string(),
            );
            switch_market_data_mode(&self.client, config.market_data_mode).await?;
        }
        underlying.beta = Some(record.beta);

        info!(
            symbol = %record.symbol,
            requested_market_data_mode = %market_data_mode_label(config.market_data_mode),
            observed_data_origin = %underlying.price_source,
            underlying_bid = ?underlying.bid,
            underlying_ask = ?underlying.ask,
            underlying_last = ?underlying.last,
            underlying_close = ?underlying.close,
            underlying_reference_price = ?underlying.reference_price(),
            underlying_notices = ?underlying.market_data_notices,
            "captured IBKR underlying snapshot"
        );

        let reference_price = match underlying.reference_price() {
            Some(value) => value,
            None => {
                return Ok(Some(SymbolMarketSnapshot {
                    underlying,
                    option_quotes: Vec::new(),
                }));
            }
        };

        if reference_price < config.risk.min_underlying_price
            || reference_price > config.risk.max_underlying_price
        {
            info!(
                symbol = %record.symbol,
                reference_price,
                min_underlying_price = config.risk.min_underlying_price,
                max_underlying_price = config.risk.max_underlying_price,
                "skipping option-chain fetch because underlying price is outside configured range"
            );
            return Ok(Some(SymbolMarketSnapshot {
                underlying,
                option_quotes: Vec::new(),
            }));
        }

        let contract_id = resolve_primary_stock_contract_id(&self.client, &record.symbol).await?;
        let chains =
            request_option_chain_for_underlying(&self.client, &record.symbol, contract_id).await?;
        info!(
            symbol = %record.symbol,
            underlying_contract_id = contract_id,
            chain_response_count = chains.len(),
            expiration_count = chains.iter().map(|chain| chain.expirations.len()).sum::<usize>(),
            strike_count = chains.iter().map(|chain| chain.strikes.len()).sum::<usize>(),
            "received IBKR option chain metadata"
        );
        let selected_contracts = match select_buy_write_contracts(
            &record.symbol,
            &chains,
            reference_price,
            config,
        ) {
            Ok(selected_contracts) => selected_contracts,
            Err(error) => {
                warn!(
                    symbol = %record.symbol,
                    reference_price,
                    requested_market_data_mode = %market_data_mode_label(config.market_data_mode),
                    error = %error,
                    "unable to select deep-ITM buy-write option contracts from IBKR chain"
                );
                return Ok(Some(SymbolMarketSnapshot {
                    underlying,
                    option_quotes: Vec::new(),
                }));
            }
        };
        let option_quotes = fetch_option_quotes_with(&selected_contracts, |selected| {
            request_option_quote(&self.client, selected)
        })
        .await?;

        for option_quote in &option_quotes {
            info!(
                symbol = %option_quote.symbol,
                expiry = %option_quote.expiry,
                strike = option_quote.strike,
                right = %option_quote.right,
                bid = ?option_quote.bid,
                ask = ?option_quote.ask,
                last = ?option_quote.last,
                close = ?option_quote.close,
                delta = ?option_quote.delta,
                implied_volatility = ?option_quote.implied_volatility,
                underlying_price = ?option_quote.underlying_price,
                quote_source = ?option_quote.quote_source,
                diagnostics = ?option_quote.diagnostics,
                "captured IBKR option snapshot"
            );
        }

        Ok(Some(SymbolMarketSnapshot {
            underlying,
            option_quotes,
        }))
    }
}

fn delayed_retry_available(notices: &[String]) -> bool {
    notices.iter().any(|notice| {
        let notice = notice.to_ascii_lowercase();
        notice.contains("delayed market data is available")
    })
}

pub fn load_universe(config: &AppConfig) -> Result<Vec<UniverseRecord>> {
    if let Some(universe_file) = &config.universe_file {
        let mut records = load_universe_from_csv(universe_file, config)?;
        if records.is_empty() {
            anyhow::bail!("universe file {} did not yield any symbols", universe_file);
        }
        records.truncate(config.risk.max_underlyings_per_cycle);
        return Ok(records);
    }

    Ok(config
        .symbols
        .iter()
        .take(config.risk.max_underlyings_per_cycle)
        .map(|symbol| UniverseRecord {
            symbol: symbol.clone(),
            beta: config.strategy.default_beta,
        })
        .collect())
}

fn load_universe_from_csv(path: &str, config: &AppConfig) -> Result<Vec<UniverseRecord>> {
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("failed to open universe file {}", path))?;
    let headers = reader
        .headers()
        .context("failed to read universe CSV headers")?
        .clone();

    let ticker_index = find_header_index(&headers, &["Ticker", "ticker"]);
    let company_index = find_header_index(&headers, &["Company", "company"]);
    let beta_index = find_header_index(&headers, &["Beta", "BETA", "beta"]);
    let price_index = find_header_index(&headers, &["Price", "price"]);

    let ticker_regex = Regex::new(r"\((?:XNYS|XNAS|ARCX|XASE|XPHL|PINX|OTC):([A-Z\.]+)\)")
        .context("failed to compile ticker extraction regex")?;

    let mut records = Vec::new();
    let mut seen = BTreeSet::new();
    for result in reader.records() {
        let row = result.context("failed to read universe row")?;
        let symbol = extract_symbol(&row, ticker_index, company_index, &ticker_regex);
        let Some(symbol) = symbol else {
            continue;
        };

        if !seen.insert(symbol.clone()) {
            continue;
        }

        if let Some(price) = parse_optional_f64(price_index.and_then(|index| row.get(index))) {
            if price < config.risk.min_underlying_price || price > config.risk.max_underlying_price
            {
                continue;
            }
        }

        let beta = parse_optional_f64(beta_index.and_then(|index| row.get(index)))
            .filter(|value| *value > 0.0)
            .unwrap_or(config.strategy.default_beta);

        records.push(UniverseRecord { symbol, beta });
    }

    Ok(records)
}

fn find_header_index(headers: &StringRecord, candidates: &[&str]) -> Option<usize> {
    headers
        .iter()
        .position(|header| candidates.iter().any(|candidate| header == *candidate))
}

fn extract_symbol(
    row: &StringRecord,
    ticker_index: Option<usize>,
    company_index: Option<usize>,
    ticker_regex: &Regex,
) -> Option<String> {
    let ticker = ticker_index.and_then(|index| row.get(index)).map(str::trim);
    if let Some(ticker) = ticker.filter(|value| !value.is_empty()) {
        return Some(ticker.to_ascii_uppercase());
    }

    let company = company_index.and_then(|index| row.get(index))?;
    ticker_regex
        .captures(company)
        .and_then(|captures| captures.get(1))
        .map(|match_| match_.as_str().to_ascii_uppercase())
}

fn parse_optional_f64(value: Option<&str>) -> Option<f64> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "#FIELD!")
        .map(|value| value.replace(['$', ','], ""))
        .and_then(|value| value.parse::<f64>().ok())
}

async fn fetch_option_quotes_with<F, Fut>(
    selected_contracts: &[SelectedOptionContract],
    mut fetch_quote: F,
) -> Result<Vec<OptionQuoteSnapshot>>
where
    F: FnMut(SelectedOptionContract) -> Fut,
    Fut: Future<Output = Result<OptionQuoteSnapshot>>,
{
    let mut option_quotes = Vec::new();

    for selected in selected_contracts {
        let selected = selected.clone();

        match fetch_quote(selected.clone()).await {
            Ok(option_quote) => option_quotes.push(option_quote),
            Err(error) if is_invalid_option_contract_error(&error) => {
                warn!(
                    symbol = %selected.symbol,
                    expiry = %selected.expiration,
                    strike = selected.strike,
                    right = %selected.right,
                    error = %error,
                    "skipping invalid IBKR option contract candidate"
                );
            }
            Err(error) => return Err(error),
        }
    }

    Ok(option_quotes)
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{
            AppConfig, BrokerPlatform, MarketDataMode, RiskConfig, RunMode, RuntimeMode,
            StrategyConfig,
        },
        ibkr::{OptionChainMetadata, SelectedOptionContract},
        market_data::{delayed_retry_available, fetch_option_quotes_with, load_universe},
        models::OptionQuoteSnapshot,
    };

    #[test]
    fn falls_back_to_symbols_when_no_csv_is_set() {
        let config = AppConfig {
            host: "127.0.0.1".to_string(),
            platform: BrokerPlatform::Gateway,
            port: 4002,
            client_id: 100,
            account: "DU123".to_string(),
            mode: RuntimeMode::Paper,
            read_only: true,
            connect_on_start: false,
            run_mode: RunMode::Manual,
            scan_schedule: "manual".to_string(),
            market_data_mode: MarketDataMode::DelayedFrozen,
            universe_file: None,
            symbols: vec!["AAPL".to_string(), "MSFT".to_string()],
            strategy: StrategyConfig::default(),
            risk: RiskConfig::default(),
        };

        let universe = load_universe(&config).unwrap();
        assert_eq!(universe.len(), 2);
        assert_eq!(universe[0].symbol, "AAPL");
    }

    #[tokio::test]
    async fn skips_invalid_contract_errors_and_continues_collecting_quotes() {
        let selected_contracts = vec![
            SelectedOptionContract {
                symbol: "AAPL".to_string(),
                right: "C".to_string(),
                expiration: "20991217".to_string(),
                strike: 101.0,
                chain_metadata: vec![OptionChainMetadata {
                    exchange: "SMART".to_string(),
                    trading_class: "AAPL".to_string(),
                    multiplier: "100".to_string(),
                    underlying_contract_id: 1,
                }],
            },
            SelectedOptionContract {
                symbol: "AAPL".to_string(),
                right: "C".to_string(),
                expiration: "20991217".to_string(),
                strike: 102.0,
                chain_metadata: vec![OptionChainMetadata {
                    exchange: "SMART".to_string(),
                    trading_class: "AAPL".to_string(),
                    multiplier: "100".to_string(),
                    underlying_contract_id: 1,
                }],
            },
        ];

        let option_quotes = fetch_option_quotes_with(&selected_contracts, |selected| async move {
            if selected.strike == 101.0 {
                return Err(anyhow::Error::new(ibapi::Error::Message(
                    200,
                    "No security definition has been found for the request".to_string(),
                ))
                .context("failed to resolve option contract details"));
            }

            Ok(OptionQuoteSnapshot {
                symbol: selected.symbol.clone(),
                expiry: selected.expiration.clone(),
                strike: selected.strike,
                right: selected.right.clone(),
                exchange: "SMART".to_string(),
                trading_class: "AAPL".to_string(),
                multiplier: "100".to_string(),
                bid: Some(1.25),
                ask: Some(1.35),
                last: Some(1.30),
                close: Some(1.20),
                option_price: Some(1.30),
                implied_volatility: Some(0.22),
                delta: Some(0.28),
                underlying_price: Some(100.0),
                quote_source: Some("test".to_string()),
                diagnostics: Vec::new(),
            })
        })
        .await
        .unwrap();

        assert_eq!(option_quotes.len(), 1);
        assert_eq!(option_quotes[0].strike, 102.0);
    }

    #[test]
    fn detects_when_delayed_retry_is_available() {
        assert!(delayed_retry_available(&[
            "10089: Requested market data requires additional subscription for API. Delayed market data is available.".to_string()
        ]));
        assert!(!delayed_retry_available(&["observed data origin: unknown".to_string()]));
    }
}
