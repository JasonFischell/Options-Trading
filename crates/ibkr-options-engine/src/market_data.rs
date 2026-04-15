use std::collections::BTreeSet;

use anyhow::{Context, Result};
use async_trait::async_trait;
use csv::StringRecord;
use regex::Regex;

use crate::{
    config::AppConfig,
    ibkr::{
        IbkrClientDescriptor, connect, fetch_account_state, fetch_positions,
        log_server_time, request_option_chain_for_underlying, request_option_quote,
        request_underlying_snapshot, resolve_primary_stock_contract_id, select_buy_write_contracts,
        switch_market_data_mode,
    },
    models::{AccountState, InventoryPosition, OptionQuoteSnapshot, UnderlyingSnapshot, UniverseRecord},
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
    client: ibapi::prelude::Client,
    account: String,
}

impl IbkrMarketDataProvider {
    pub async fn connect(config: &AppConfig) -> Result<Self> {
        let descriptor = IbkrClientDescriptor::from(config);
        let client = connect(&descriptor.endpoint, descriptor.client_id).await?;
        log_server_time(&client).await?;

        Ok(Self {
            client,
            account: config.account.clone(),
        })
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
        underlying.beta = Some(record.beta);

        let reference_price = match underlying.reference_price() {
            Some(value) => value,
            None => return Ok(None),
        };

        let contract_id = resolve_primary_stock_contract_id(&self.client, &record.symbol).await?;
        let chains =
            request_option_chain_for_underlying(&self.client, &record.symbol, contract_id).await?;
        let selected_contracts =
            select_buy_write_contracts(&record.symbol, &chains, reference_price, config)?;

        let mut option_quotes = Vec::new();
        for selected in selected_contracts {
            option_quotes.push(request_option_quote(&self.client, &selected).await?);
        }

        Ok(Some(SymbolMarketSnapshot {
            underlying,
            option_quotes,
        }))
    }
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
    headers.iter().position(|header| candidates.iter().any(|candidate| header == *candidate))
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

#[cfg(test)]
mod tests {
    use crate::{
        config::{AppConfig, BrokerPlatform, MarketDataMode, RiskConfig, RunMode, RuntimeMode, StrategyConfig},
        market_data::load_universe,
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
}
