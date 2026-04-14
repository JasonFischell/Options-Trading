use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeMode {
    Paper,
    Live,
}

impl RuntimeMode {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "paper" => Ok(Self::Paper),
            "live" => Ok(Self::Live),
            other => anyhow::bail!("unsupported IBKR runtime mode: {other}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub client_id: i32,
    pub account: String,
    pub mode: RuntimeMode,
    pub read_only: bool,
    pub connect_on_start: bool,
    pub symbols: Vec<String>,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let host = env_var("IBKR_HOST")?;
        let port = env_var("IBKR_PORT")?
            .parse()
            .context("IBKR_PORT must be a valid u16")?;
        let client_id = env_var("IBKR_CLIENT_ID")?
            .parse()
            .context("IBKR_CLIENT_ID must be a valid i32")?;
        let account = env_var("IBKR_ACCOUNT")?;
        let mode = RuntimeMode::parse(&env_var("IBKR_RUNTIME_MODE")?)?;
        let read_only = parse_bool(&env_var("IBKR_READ_ONLY")?)?;
        let connect_on_start = parse_bool(&env_var("IBKR_CONNECT_ON_START")?)?;
        let symbols = parse_symbols(&env_var("IBKR_SYMBOLS")?);

        Ok(Self {
            host,
            port,
            client_id,
            account,
            mode,
            read_only,
            connect_on_start,
            symbols,
        })
    }

    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

fn env_var(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("missing required environment variable {key}"))
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => anyhow::bail!("unsupported boolean value: {other}"),
    }
}

fn parse_symbols(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(|symbol| symbol.to_ascii_uppercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{RuntimeMode, parse_bool, parse_symbols};

    #[test]
    fn parses_bool_flags() {
        assert!(parse_bool("true").unwrap());
        assert!(!parse_bool("No").unwrap());
    }

    #[test]
    fn normalizes_symbols() {
        let symbols = parse_symbols("aapl, msft , nvda");
        assert_eq!(symbols, vec!["AAPL", "MSFT", "NVDA"]);
    }

    #[test]
    fn parses_runtime_mode() {
        assert_eq!(RuntimeMode::parse("paper").unwrap(), RuntimeMode::Paper);
        assert_eq!(RuntimeMode::parse("LIVE").unwrap(), RuntimeMode::Live);
    }
}
