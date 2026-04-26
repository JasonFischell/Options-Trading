use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Args, Parser};
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use dotenvy::dotenv;
use ibkr_options_engine::{
    config::AppConfig,
    ibkr::{connect, log_server_time, request_underlying_snapshot, switch_market_data_mode},
};
use reqwest::Client as HttpClient;
use serde_json::Value;

const FINNHUB_BASE_URL: &str = "https://finnhub.io/api/v1";
const FINNHUB_KEYS: [&str; 2] = [
    "d0v03h9r01qmg3ujdm9gd0v03h9r01qmg3ujdma0",
    "d0v1v91r01qmg3ujo8h0d0v1v91r01qmg3ujo8hg",
];

#[derive(Debug, Parser)]
#[command(name = "ibkr_csv_quote_beta")]
#[command(about = "Refresh a ticker CSV in place with IBKR prices and Finnhub beta values")]
struct Cli {
    #[command(flatten)]
    config: ConfigArgs,
    #[arg(long)]
    input: PathBuf,
}

#[derive(Debug, Clone, Args, Default)]
struct ConfigArgs {
    #[arg(long)]
    config: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct CsvLayout {
    header: StringRecord,
    rows: Vec<StringRecord>,
}

#[derive(Debug, Clone)]
struct RowUpdate {
    row_index: usize,
    ticker: String,
    price: Option<String>,
    beta: Option<String>,
}

#[derive(Debug, Clone)]
struct FinnhubClient {
    http: HttpClient,
    base_url: String,
    tokens: Vec<String>,
}

impl FinnhubClient {
    fn new() -> Self {
        Self {
            http: HttpClient::new(),
            base_url: FINNHUB_BASE_URL.to_string(),
            tokens: FINNHUB_KEYS.iter().map(|key| key.to_string()).collect(),
        }
    }

    async fn fetch_beta(&self, ticker: &str) -> Result<Option<f64>> {
        let symbol = ticker.trim().to_uppercase();
        for token in &self.tokens {
            let response = self
                .http
                .get(format!("{}/stock/metric", self.base_url))
                .query(&[
                    ("symbol", symbol.as_str()),
                    ("metric", "all"),
                    ("token", token.as_str()),
                ])
                .send()
                .await
                .with_context(|| format!("failed to request Finnhub beta for {symbol}"))?;

            if !response.status().is_success() {
                continue;
            }

            let payload: Value = response
                .json()
                .await
                .with_context(|| format!("failed to decode Finnhub beta payload for {symbol}"))?;

            if let Some(beta) = extract_beta_from_metric_payload(&payload) {
                return Ok(Some(beta));
            }
        }

        Ok(None)
    }

    async fn fetch_price(&self, ticker: &str) -> Result<Option<f64>> {
        let symbol = ticker.trim().to_uppercase();
        for token in &self.tokens {
            let response = self
                .http
                .get(format!("{}/quote", self.base_url))
                .query(&[("symbol", symbol.as_str()), ("token", token.as_str())])
                .send()
                .await
                .with_context(|| format!("failed to request Finnhub price for {symbol}"))?;

            if !response.status().is_success() {
                continue;
            }

            let payload: Value = response
                .json()
                .await
                .with_context(|| format!("failed to decode Finnhub price payload for {symbol}"))?;

            if let Some(price) = extract_price_from_quote_payload(&payload) {
                return Ok(Some(price));
            }
        }

        Ok(None)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let cli = Cli::parse();
    let input_path = cli.input.canonicalize().with_context(|| {
        format!(
            "failed to resolve input CSV path {}",
            cli.input.to_string_lossy()
        )
    })?;
    let config = AppConfig::from_path(cli.config.config.as_deref())?;

    let client = connect(&config.endpoint(), config.client_id).await?;
    let _ = log_server_time(&client).await?;
    switch_market_data_mode(&client, config.market_data_mode).await?;

    let finnhub = FinnhubClient::new();
    let layout = load_csv_layout(&input_path)?;
    let updates = build_updates(&client, &finnhub, &layout.rows).await?;
    write_updated_csv(&input_path, layout, &updates)?;

    println!(
        "Updated {} ticker row(s) in {}.",
        updates.len(),
        input_path.display()
    );
    Ok(())
}

async fn build_updates(
    ibkr_client: &ibapi::prelude::Client,
    finnhub: &FinnhubClient,
    rows: &[StringRecord],
) -> Result<Vec<RowUpdate>> {
    let mut updates = Vec::new();
    let tickers = rows
        .iter()
        .filter_map(|row| {
            let ticker = row.get(0).unwrap_or_default().trim();
            (!ticker.is_empty()).then_some(ticker)
        })
        .count();
    let mut ticker_position = 0usize;

    for (row_index, row) in rows.iter().enumerate() {
        let raw_ticker = row.get(0).unwrap_or_default().trim();
        if raw_ticker.is_empty() {
            continue;
        }
        ticker_position += 1;

        let ibkr_symbol = normalize_ticker_for_lookup(raw_ticker);
        println!(
            "Pulling price and beta for {} (stock {} of {})",
            ibkr_symbol, ticker_position, tickers
        );

        let price = match request_underlying_snapshot(ibkr_client, &ibkr_symbol).await {
            Ok(snapshot) => snapshot.reference_price(),
            Err(error) => {
                eprintln!(
                    "IBKR price lookup failed for {}: {}. Trying Finnhub quote fallback.",
                    ibkr_symbol, error
                );
                None
            }
        };
        let price = match price {
            Some(price) => Some(price),
            None => {
                let finnhub_price = finnhub.fetch_price(&ibkr_symbol).await?;
                if finnhub_price.is_none() {
                    eprintln!(
                        "No price available from IBKR or Finnhub for {}; leaving price unchanged.",
                        ibkr_symbol
                    );
                }
                finnhub_price
            }
        };
        let beta = finnhub.fetch_beta(&ibkr_symbol).await?;
        if beta.is_none() {
            eprintln!(
                "No Finnhub beta available for {}; leaving beta unchanged.",
                ibkr_symbol
            );
        }

        updates.push(RowUpdate {
            row_index,
            ticker: raw_ticker.to_string(),
            price: price.map(format_decimal),
            beta: beta.map(format_decimal),
        });
    }

    Ok(updates)
}

fn load_csv_layout(path: &Path) -> Result<CsvLayout> {
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("failed to open CSV {}", path.display()))?;

    let rows = reader
        .records()
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("failed to read CSV records from {}", path.display()))?;

    Ok(normalize_csv_layout(rows))
}

fn normalize_csv_layout(mut rows: Vec<StringRecord>) -> CsvLayout {
    if rows.is_empty() {
        return CsvLayout {
            header: StringRecord::from(vec!["ticker", "price", "beta"]),
            rows: Vec::new(),
        };
    }

    if row_looks_like_header(&rows[0]) {
        let existing_header = rows.remove(0);
        CsvLayout {
            header: normalized_header(&existing_header),
            rows,
        }
    } else {
        CsvLayout {
            header: StringRecord::from(vec!["ticker", "price", "beta"]),
            rows,
        }
    }
}

fn row_looks_like_header(row: &StringRecord) -> bool {
    let first = row.get(0).unwrap_or_default().trim().to_ascii_lowercase();
    let second = row.get(1).unwrap_or_default().trim().to_ascii_lowercase();
    let third = row.get(2).unwrap_or_default().trim().to_ascii_lowercase();

    matches!(first.as_str(), "ticker" | "symbol")
        || matches!(second.as_str(), "price")
        || matches!(third.as_str(), "beta")
}

fn normalized_header(existing: &StringRecord) -> StringRecord {
    let mut header = existing.clone();
    ensure_len(&mut header, 3);
    set_field(&header, 0, "ticker")
        .and_then(|updated| set_field(&updated, 1, "price"))
        .and_then(|updated| set_field(&updated, 2, "beta"))
        .unwrap_or_else(|| StringRecord::from(vec!["ticker", "price", "beta"]))
}

fn write_updated_csv(path: &Path, layout: CsvLayout, updates: &[RowUpdate]) -> Result<()> {
    let mut rows = layout.rows;
    for update in updates {
        let row = rows
            .get_mut(update.row_index)
            .with_context(|| format!("missing CSV row {}", update.row_index))?;
        ensure_len(row, 3);
        let mut updated = set_field(row, 0, &update.ticker)
            .with_context(|| format!("failed to update CSV row {}", update.row_index))?;
        if let Some(price) = &update.price {
            updated = set_field(&updated, 1, price)
                .with_context(|| format!("failed to update price for row {}", update.row_index))?;
        }
        if let Some(beta) = &update.beta {
            updated = set_field(&updated, 2, beta)
                .with_context(|| format!("failed to update beta for row {}", update.row_index))?;
        }
        *row = updated;
    }

    let temp_path = temporary_output_path(path);
    {
        let mut writer = WriterBuilder::new()
            .has_headers(false)
            .from_path(&temp_path)
            .with_context(|| format!("failed to open temp CSV {}", temp_path.display()))?;
        writer.write_record(&layout.header)?;
        for row in rows {
            writer.write_record(&row)?;
        }
        writer.flush()?;
    }

    fs::copy(&temp_path, path)
        .with_context(|| format!("failed to overwrite CSV {}", path.display()))?;
    fs::remove_file(&temp_path)
        .with_context(|| format!("failed to clean up temp CSV {}", temp_path.display()))?;
    Ok(())
}

fn temporary_output_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("tickers.csv");
    path.with_file_name(format!("{file_name}.codex-tmp"))
}

fn ensure_len(record: &mut StringRecord, len: usize) {
    if record.len() >= len {
        return;
    }

    let mut fields = record
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    fields.resize(len, String::new());
    *record = StringRecord::from(fields);
}

fn set_field(record: &StringRecord, index: usize, value: &str) -> Option<StringRecord> {
    if index >= record.len() {
        return None;
    }

    let mut fields = record
        .iter()
        .map(|entry| entry.to_string())
        .collect::<Vec<_>>();
    fields[index] = value.to_string();
    Some(StringRecord::from(fields))
}

fn normalize_ticker_for_lookup(ticker: &str) -> String {
    ticker.trim().to_uppercase()
}

fn format_decimal(value: f64) -> String {
    let mut rendered = format!("{value:.6}");
    while rendered.contains('.') && rendered.ends_with('0') {
        rendered.pop();
    }
    if rendered.ends_with('.') {
        rendered.pop();
    }
    rendered
}

fn extract_beta_from_metric_payload(payload: &Value) -> Option<f64> {
    payload
        .get("metric")
        .and_then(|metric| metric.get("beta"))
        .and_then(Value::as_f64)
        .filter(|beta| beta.is_finite() && *beta > 0.0)
}

fn extract_price_from_quote_payload(payload: &Value) -> Option<f64> {
    payload
        .get("c")
        .and_then(Value::as_f64)
        .filter(|price| price.is_finite() && *price > 0.0)
}

#[cfg(test)]
mod tests {
    use super::{
        extract_beta_from_metric_payload, extract_price_from_quote_payload, format_decimal,
        normalize_csv_layout, normalized_header, row_looks_like_header, write_updated_csv,
    };
    use std::fs;

    use anyhow::Result;
    use csv::StringRecord;
    use serde_json::json;

    #[test]
    fn recognizes_existing_header_row() {
        assert!(row_looks_like_header(&StringRecord::from(vec![
            "Ticker", "Price", "Beta"
        ])));
        assert!(row_looks_like_header(&StringRecord::from(vec![
            "Symbol", "", ""
        ])));
        assert!(!row_looks_like_header(&StringRecord::from(vec![
            "AAPL", "", ""
        ])));
    }

    #[test]
    fn normalizes_existing_header_names() {
        let header = normalized_header(&StringRecord::from(vec!["Symbol", "last", "risk"]));
        assert_eq!(header, StringRecord::from(vec!["ticker", "price", "beta"]));
    }

    #[test]
    fn creates_default_header_when_csv_has_no_header() {
        let layout = normalize_csv_layout(vec![
            StringRecord::from(vec!["AAPL"]),
            StringRecord::from(vec!["MSFT"]),
        ]);
        assert_eq!(
            layout.header,
            StringRecord::from(vec!["ticker", "price", "beta"])
        );
        assert_eq!(layout.rows.len(), 2);
        assert_eq!(layout.rows[0], StringRecord::from(vec!["AAPL"]));
    }

    #[test]
    fn formats_decimals_without_trailing_zero_noise() {
        assert_eq!(format_decimal(123.450000), "123.45");
        assert_eq!(format_decimal(10.0), "10");
    }

    #[test]
    fn extracts_beta_from_finnhub_metric_payload() {
        assert_eq!(
            extract_beta_from_metric_payload(&json!({"metric": {"beta": 1.23}})),
            Some(1.23)
        );
        assert_eq!(
            extract_beta_from_metric_payload(&json!({"metric": {"beta": -1.0}})),
            None
        );
    }

    #[test]
    fn extracts_price_from_finnhub_quote_payload() {
        assert_eq!(
            extract_price_from_quote_payload(&json!({"c": 101.25})),
            Some(101.25)
        );
        assert_eq!(extract_price_from_quote_payload(&json!({"c": 0.0})), None);
    }

    #[test]
    fn rewrites_csv_in_place_with_updated_columns() -> Result<()> {
        let temp_root = std::env::temp_dir().join(format!(
            "options-trading-ibkr-csv-quote-beta-{}",
            std::process::id()
        ));
        fs::create_dir_all(&temp_root)?;
        let csv_path = temp_root.join("sample.csv");
        fs::write(&csv_path, "ticker,price,beta\r\nAAPL,1,2\r\nMSFT\r\n")?;

        let layout = super::normalize_csv_layout(vec![
            StringRecord::from(vec!["ticker", "price", "beta"]),
            StringRecord::from(vec!["AAPL", "1", "2"]),
            StringRecord::from(vec!["MSFT"]),
        ]);
        let updates = vec![
            super::RowUpdate {
                row_index: 0,
                ticker: "AAPL".to_string(),
                price: Some("123.45".to_string()),
                beta: Some("1.2".to_string()),
            },
            super::RowUpdate {
                row_index: 1,
                ticker: "MSFT".to_string(),
                price: Some("234.56".to_string()),
                beta: Some("0.9".to_string()),
            },
        ];

        write_updated_csv(&csv_path, layout, &updates)?;
        let written = fs::read_to_string(&csv_path)?;
        assert!(written.contains("ticker,price,beta"));
        assert!(written.contains("AAPL,123.45,1.2"));
        assert!(written.contains("MSFT,234.56,0.9"));

        fs::remove_file(&csv_path)?;
        fs::remove_dir(&temp_root)?;
        Ok(())
    }

    #[test]
    fn preserves_existing_cells_when_updates_are_missing() -> Result<()> {
        let temp_root = std::env::temp_dir().join(format!(
            "options-trading-ibkr-csv-quote-beta-partial-{}",
            std::process::id()
        ));
        fs::create_dir_all(&temp_root)?;
        let csv_path = temp_root.join("sample.csv");
        fs::write(&csv_path, "ticker,price,beta\r\nAAPL,1,2\r\n")?;

        let layout = super::normalize_csv_layout(vec![
            StringRecord::from(vec!["ticker", "price", "beta"]),
            StringRecord::from(vec!["AAPL", "1", "2"]),
        ]);
        let updates = vec![super::RowUpdate {
            row_index: 0,
            ticker: "AAPL".to_string(),
            price: None,
            beta: Some("1.5".to_string()),
        }];

        write_updated_csv(&csv_path, layout, &updates)?;
        let written = fs::read_to_string(&csv_path)?;
        assert!(written.contains("AAPL,1,1.5"));

        fs::remove_file(&csv_path)?;
        fs::remove_dir(&temp_root)?;
        Ok(())
    }
}
