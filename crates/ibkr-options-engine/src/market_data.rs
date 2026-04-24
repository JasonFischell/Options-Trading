use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use csv::StringRecord;
use futures::{StreamExt, lock::Mutex as AsyncMutex, stream};
use regex::Regex;
use tracing::{info, warn};

use crate::{
    config::{AppConfig, MarketDataMode},
    ibkr::{
        IbkrClientDescriptor, SelectedOptionContract, cancel_open_order, connect,
        fetch_account_state, fetch_completed_orders, fetch_open_orders, fetch_positions,
        is_invalid_option_contract_error, is_invalid_underlying_contract_error, log_server_time,
        market_data_mode_label, request_option_chain_for_underlying, request_option_quote,
        request_underlying_snapshot_for_contract, resolve_primary_stock_contract,
        select_buy_write_contracts, switch_market_data_mode,
    },
    models::{
        AccountState, BrokerCompletedOrder, BrokerOpenOrder, InventoryPosition,
        OptionQuoteSnapshot, UnderlyingSnapshot, UniverseRecord,
    },
};

#[derive(Debug, Clone)]
pub struct SymbolMarketSnapshot {
    pub underlying: UnderlyingSnapshot,
    pub option_quotes: Vec<OptionQuoteSnapshot>,
}

#[async_trait(?Send)]
pub trait MarketDataProvider {
    async fn prepare_scan_cycle(&self, _config: &AppConfig) -> Result<()> {
        Ok(())
    }

    async fn load_account_state(&self) -> Result<AccountState>;
    async fn load_inventory(&self) -> Result<Vec<InventoryPosition>>;
    async fn load_open_orders(&self) -> Result<Vec<BrokerOpenOrder>>;
    async fn load_completed_orders(&self) -> Result<Vec<BrokerCompletedOrder>>;
    async fn cancel_order(&self, order_id: i32) -> Result<()>;
    async fn fetch_symbol_snapshot(
        &self,
        record: &UniverseRecord,
        config: &AppConfig,
    ) -> Result<Option<SymbolMarketSnapshot>>;
}

pub struct IbkrMarketDataProvider {
    client: Arc<ibapi::prelude::Client>,
    delayed_fallback_client: AsyncMutex<Option<Arc<ibapi::prelude::Client>>>,
    delayed_fallback_descriptor: IbkrClientDescriptor,
    account: String,
}

impl IbkrMarketDataProvider {
    pub async fn connect(config: &AppConfig) -> Result<Self> {
        let descriptor = IbkrClientDescriptor::from(config);
        let client = connect(&descriptor.endpoint, descriptor.client_id).await?;
        log_server_time(&client).await?;
        let delayed_fallback_descriptor = IbkrClientDescriptor {
            endpoint: descriptor.endpoint.clone(),
            client_id: descriptor
                .client_id
                .checked_add(10_000)
                .context("IBKR client id overflow while reserving delayed fallback client")?,
            account: descriptor.account.clone(),
            read_only: true,
        };

        Ok(Self {
            client: Arc::new(client),
            delayed_fallback_client: AsyncMutex::new(None),
            delayed_fallback_descriptor,
            account: config.account.clone(),
        })
    }

    pub fn shared_client(&self) -> Arc<ibapi::prelude::Client> {
        self.client.clone()
    }

    async fn delayed_fallback_client(&self) -> Result<Arc<ibapi::prelude::Client>> {
        let mut delayed_fallback_client = self.delayed_fallback_client.lock().await;
        if let Some(client) = delayed_fallback_client.as_ref() {
            return Ok(client.clone());
        }

        let client = connect(
            &self.delayed_fallback_descriptor.endpoint,
            self.delayed_fallback_descriptor.client_id,
        )
        .await
        .with_context(|| {
            format!(
                "failed to connect delayed fallback IBKR client at {} with client id {}",
                self.delayed_fallback_descriptor.endpoint,
                self.delayed_fallback_descriptor.client_id
            )
        })?;
        switch_market_data_mode(&client, MarketDataMode::Delayed).await?;
        let client = Arc::new(client);
        *delayed_fallback_client = Some(client.clone());
        Ok(client)
    }
}

#[async_trait(?Send)]
impl MarketDataProvider for IbkrMarketDataProvider {
    async fn prepare_scan_cycle(&self, config: &AppConfig) -> Result<()> {
        switch_market_data_mode(&self.client, config.market_data_mode).await
    }

    async fn load_account_state(&self) -> Result<AccountState> {
        fetch_account_state(&self.client, &self.account).await
    }

    async fn load_inventory(&self) -> Result<Vec<InventoryPosition>> {
        fetch_positions(&self.client).await
    }

    async fn load_open_orders(&self) -> Result<Vec<BrokerOpenOrder>> {
        fetch_open_orders(&self.client, &self.account).await
    }

    async fn load_completed_orders(&self) -> Result<Vec<BrokerCompletedOrder>> {
        fetch_completed_orders(&self.client, &self.account).await
    }

    async fn cancel_order(&self, order_id: i32) -> Result<()> {
        cancel_open_order(&self.client, order_id).await
    }

    async fn fetch_symbol_snapshot(
        &self,
        record: &UniverseRecord,
        config: &AppConfig,
    ) -> Result<Option<SymbolMarketSnapshot>> {
        let primary_contract =
            match resolve_primary_stock_contract(&self.client, &record.symbol).await {
                Ok(primary_contract) => primary_contract,
                Err(error) if is_invalid_underlying_contract_error(&error) => {
                    warn!(
                        symbol = %record.symbol,
                        error = %error,
                        "skipping symbol because IBKR could not resolve the underlying contract"
                    );
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
        let mut underlying = request_underlying_snapshot_for_contract(
            &self.client,
            &record.symbol,
            &primary_contract,
        )
        .await?;
        if matches!(config.market_data_mode, MarketDataMode::Live)
            && underlying.reference_price().is_none()
            && delayed_retry_available(&underlying.market_data_notices)
        {
            warn!(
                symbol = %record.symbol,
                "live underlying snapshot was unavailable; retrying once with delayed market data"
            );
            let delayed_fallback_client = self.delayed_fallback_client().await?;
            underlying = request_underlying_snapshot_for_contract(
                delayed_fallback_client.as_ref(),
                &record.symbol,
                &primary_contract,
            )
            .await?;
            underlying.market_data_notices.push(
                "scanner retried with delayed market data after the live request returned no usable underlying price"
                    .to_string(),
            );
        }
        if underlying.beta.is_none() && record.beta > 0.0 {
            underlying.beta = Some(record.beta);
            underlying.market_data_notices.push(
                "underlying beta was unavailable from IBKR; falling back to configured universe beta"
                    .to_string(),
            );
        }

        info!(
            symbol = %record.symbol,
            requested_market_data_mode = %market_data_mode_label(config.market_data_mode),
            observed_data_origin = %underlying.price_source,
            underlying_bid = ?underlying.bid,
            underlying_ask = ?underlying.ask,
            underlying_last = ?underlying.last,
            underlying_close = ?underlying.close,
            underlying_beta = ?underlying.beta,
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

        let chains = request_option_chain_for_underlying(
            &self.client,
            &record.symbol,
            primary_contract.contract_id,
        )
        .await?;
        info!(
            symbol = %record.symbol,
            underlying_contract_id = primary_contract.contract_id,
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
                    "unable to select covered-call option contracts from the configured strike window"
                );
                return Ok(Some(SymbolMarketSnapshot {
                    underlying,
                    option_quotes: Vec::new(),
                }));
            }
        };
        let option_quotes = fetch_option_quotes_with(
            &selected_contracts,
            config.performance.option_quote_concurrency_per_symbol,
            |selected| request_option_quote(&self.client, selected),
        )
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
    if !config.symbols.is_empty() {
        return Ok(config
            .symbols
            .iter()
            .take(config.risk.max_underlyings_per_cycle)
            .map(|symbol| UniverseRecord {
                symbol: symbol.clone(),
                beta: config.strategy.default_beta,
            })
            .collect());
    }

    if let Some(universe_file) = &config.universe_file {
        let mut records = load_universe_from_csv(universe_file, config)?;
        if records.is_empty() {
            anyhow::bail!("universe file {} did not yield any symbols", universe_file);
        }
        records.truncate(config.risk.max_underlyings_per_cycle);
        return Ok(records);
    }

    anyhow::bail!("no enabled universe source is available for this run")
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
    let headerless_symbol_list =
        ticker_index.is_none() && company_index.is_none() && headers.len() == 1;

    let ticker_regex = Regex::new(r"\((?:XNYS|XNAS|ARCX|XASE|XPHL|PINX|OTC):([A-Z\.]+)\)")
        .context("failed to compile ticker extraction regex")?;

    let mut records = Vec::new();
    let mut seen = BTreeSet::new();
    if headerless_symbol_list {
        push_universe_record(&mut records, &mut seen, headers.get(0), None, None, config);
    }
    for result in reader.records() {
        let row = result.context("failed to read universe row")?;
        let symbol = if headerless_symbol_list {
            row.get(0)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.trim_matches('"').to_ascii_uppercase())
        } else {
            extract_symbol(&row, ticker_index, company_index, &ticker_regex)
        };
        let Some(symbol) = symbol else {
            continue;
        };

        push_universe_record(
            &mut records,
            &mut seen,
            Some(symbol.as_str()),
            beta_index.and_then(|index| row.get(index)),
            price_index.and_then(|index| row.get(index)),
            config,
        );
    }

    Ok(records)
}

fn push_universe_record(
    records: &mut Vec<UniverseRecord>,
    seen: &mut BTreeSet<String>,
    symbol: Option<&str>,
    beta: Option<&str>,
    price: Option<&str>,
    config: &AppConfig,
) {
    let Some(symbol) = symbol
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(|symbol| symbol.trim_matches('"').to_ascii_uppercase())
    else {
        return;
    };

    if let Some(price) = parse_optional_f64(price)
        && (price < config.risk.min_underlying_price || price > config.risk.max_underlying_price)
    {
        return;
    }

    if !seen.insert(symbol.clone()) {
        return;
    }

    let beta = parse_optional_f64(beta)
        .filter(|value| *value > 0.0)
        .unwrap_or(config.strategy.default_beta);

    records.push(UniverseRecord { symbol, beta });
}

fn find_header_index(headers: &StringRecord, candidates: &[&str]) -> Option<usize> {
    headers
        .iter()
        .position(|header| candidates.contains(&header))
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
    concurrency_limit: usize,
    fetch_quote: F,
) -> Result<Vec<OptionQuoteSnapshot>>
where
    F: Fn(SelectedOptionContract) -> Fut,
    Fut: Future<Output = Result<OptionQuoteSnapshot>>,
{
    let mut quote_results = stream::iter(selected_contracts.iter().cloned().enumerate().map(
        |(index, selected)| {
            let request = fetch_quote(selected.clone());
            async move { (index, selected, request.await) }
        },
    ))
    .buffer_unordered(concurrency_limit.max(1))
    .collect::<Vec<_>>()
    .await;
    quote_results.sort_by_key(|(index, _, _)| *index);

    let mut option_quotes = Vec::new();

    for (_, selected, result) in quote_results {
        match result {
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
    use std::{
        sync::Arc,
        sync::atomic::{AtomicUsize, Ordering},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use crate::{
        config::{
            AllocationConfig, AppConfig, BrokerPlatform, ExecutionTuningConfig, MarketDataMode,
            PerformanceConfig, RiskConfig, RunMode, RuntimeMode, StrategyConfig,
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
            startup_warnings: Vec::new(),
            strategy: StrategyConfig::default(),
            risk: RiskConfig::default(),
            allocation: AllocationConfig::default(),
            performance: PerformanceConfig::default(),
            execution: ExecutionTuningConfig::default(),
        };

        let universe = load_universe(&config).unwrap();
        assert_eq!(universe.len(), 2);
        assert_eq!(universe[0].symbol, "AAPL");
    }

    #[test]
    fn loads_headerless_symbol_list_csv() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("headerless-symbols-{unique}.csv"));
        std::fs::write(&path, "SNAP\nRGTI\nSNAP\n").unwrap();

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
            universe_file: Some(path.display().to_string()),
            symbols: Vec::new(),
            startup_warnings: Vec::new(),
            strategy: StrategyConfig::default(),
            risk: RiskConfig::default(),
            allocation: AllocationConfig::default(),
            performance: PerformanceConfig::default(),
            execution: ExecutionTuningConfig::default(),
        };

        let universe = load_universe(&config).unwrap();

        assert_eq!(universe.len(), 2);
        assert_eq!(universe[0].symbol, "SNAP");
        assert_eq!(universe[1].symbol, "RGTI");

        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn preserves_option_quote_order_under_bounded_concurrency() {
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
            SelectedOptionContract {
                symbol: "AAPL".to_string(),
                right: "C".to_string(),
                expiration: "20991217".to_string(),
                strike: 103.0,
                chain_metadata: vec![OptionChainMetadata {
                    exchange: "SMART".to_string(),
                    trading_class: "AAPL".to_string(),
                    multiplier: "100".to_string(),
                    underlying_contract_id: 1,
                }],
            },
        ];
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));

        let option_quotes = fetch_option_quotes_with(&selected_contracts, 2, {
            let in_flight = in_flight.clone();
            let max_in_flight = max_in_flight.clone();
            move |selected| {
                let in_flight = in_flight.clone();
                let max_in_flight = max_in_flight.clone();
                async move {
                    let active = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    while active > max_in_flight.load(Ordering::SeqCst) {
                        if max_in_flight
                            .compare_exchange(
                                max_in_flight.load(Ordering::SeqCst),
                                active,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .is_ok()
                        {
                            break;
                        }
                    }

                    let pause = match selected.strike as i32 {
                        101 => Duration::from_millis(40),
                        102 => Duration::from_millis(5),
                        103 => Duration::from_millis(15),
                        _ => Duration::from_millis(1),
                    };
                    tokio::time::sleep(pause).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);

                    Ok(OptionQuoteSnapshot {
                        contract_id: selected.strike as i32,
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
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(
            option_quotes
                .iter()
                .map(|quote| quote.strike)
                .collect::<Vec<_>>(),
            vec![101.0, 102.0, 103.0]
        );
        assert!(max_in_flight.load(Ordering::SeqCst) >= 2);
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

        let option_quotes =
            fetch_option_quotes_with(&selected_contracts, 2, |selected| async move {
                if selected.strike == 101.0 {
                    return Err(anyhow::Error::new(ibapi::Error::Message(
                        200,
                        "No security definition has been found for the request".to_string(),
                    ))
                    .context("failed to resolve option contract details"));
                }

                Ok(OptionQuoteSnapshot {
                    contract_id: 2,
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
        assert!(!delayed_retry_available(&[
            "observed data origin: unknown".to_string()
        ]));
    }
}
