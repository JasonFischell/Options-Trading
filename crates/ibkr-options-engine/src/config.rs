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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerPlatform {
    Gateway,
    Tws,
}

impl BrokerPlatform {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "gateway" | "ibgateway" | "ib-gateway" => Ok(Self::Gateway),
            "tws" => Ok(Self::Tws),
            other => anyhow::bail!("unsupported IBKR platform: {other}"),
        }
    }

    pub fn default_port(self, mode: RuntimeMode) -> u16 {
        match (self, mode) {
            (Self::Gateway, RuntimeMode::Paper) => 4002,
            (Self::Gateway, RuntimeMode::Live) => 4001,
            (Self::Tws, RuntimeMode::Paper) => 7497,
            (Self::Tws, RuntimeMode::Live) => 7496,
        }
    }

    pub fn expected_port_hint(self, mode: RuntimeMode) -> &'static str {
        match (self, mode) {
            (Self::Gateway, RuntimeMode::Paper) => "4002",
            (Self::Gateway, RuntimeMode::Live) => "4001",
            (Self::Tws, RuntimeMode::Paper) => "7497",
            (Self::Tws, RuntimeMode::Live) => "7496",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Gateway => "IB Gateway",
            Self::Tws => "TWS",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RunMode {
    Manual,
    Scheduled,
}

impl RunMode {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "manual" => Ok(Self::Manual),
            "scheduled" => Ok(Self::Scheduled),
            other => anyhow::bail!("unsupported run mode: {other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MarketDataMode {
    Live,
    Frozen,
    Delayed,
    DelayedFrozen,
}

impl MarketDataMode {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "live" | "realtime" => Ok(Self::Live),
            "frozen" => Ok(Self::Frozen),
            "delayed" => Ok(Self::Delayed),
            "delayed_frozen" | "delayed-frozen" | "delayedfrozen" => Ok(Self::DelayedFrozen),
            other => anyhow::bail!("unsupported market data mode: {other}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyConfig {
    pub default_beta: f64,
    pub min_expiry_days: i64,
    pub max_expiry_days: i64,
    pub min_annualized_yield_pct: f64,
    pub min_itm_depth_pct: f64,
    pub min_downside_buffer_pct: f64,
    pub min_option_bid: f64,
    pub max_option_spread_pct: f64,
    pub profit_take_pct: f64,
    pub max_loss_pct: f64,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            default_beta: 1.5,
            min_expiry_days: 30,
            max_expiry_days: 60,
            min_annualized_yield_pct: 12.0,
            min_itm_depth_pct: 0.05,
            min_downside_buffer_pct: 0.12,
            min_option_bid: 0.15,
            max_option_spread_pct: 0.25,
            profit_take_pct: 0.5,
            max_loss_pct: 0.1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RiskConfig {
    pub min_underlying_price: f64,
    pub max_underlying_price: f64,
    pub max_underlyings_per_cycle: usize,
    pub max_option_quotes_per_underlying: usize,
    pub max_new_trades_per_cycle: usize,
    pub max_open_positions: usize,
    pub min_buying_power_buffer_pct: f64,
    pub enable_paper_orders: bool,
    pub enable_live_orders: bool,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            min_underlying_price: 1.0,
            max_underlying_price: 20.0,
            max_underlyings_per_cycle: 50,
            max_option_quotes_per_underlying: 3,
            max_new_trades_per_cycle: 1,
            max_open_positions: 3,
            min_buying_power_buffer_pct: 5.0,
            enable_paper_orders: false,
            enable_live_orders: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AppConfig {
    pub host: String,
    pub platform: BrokerPlatform,
    pub port: u16,
    pub client_id: i32,
    pub account: String,
    pub mode: RuntimeMode,
    pub read_only: bool,
    pub connect_on_start: bool,
    pub run_mode: RunMode,
    pub scan_schedule: String,
    pub market_data_mode: MarketDataMode,
    pub universe_file: Option<String>,
    pub symbols: Vec<String>,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let host = env_var("IBKR_HOST")?;
        let mode = RuntimeMode::parse(&env_var("IBKR_RUNTIME_MODE")?)?;
        let platform = BrokerPlatform::parse(&env_or_default("IBKR_PLATFORM", "gateway")?)?;
        let port = optional_env("IBKR_PORT")
            .map(|value| value.parse().context("IBKR_PORT must be a valid u16"))
            .transpose()?
            .unwrap_or_else(|| platform.default_port(mode));
        let client_id = env_var("IBKR_CLIENT_ID")?
            .parse()
            .context("IBKR_CLIENT_ID must be a valid i32")?;
        let account = env_var("IBKR_ACCOUNT")?;
        let read_only = parse_bool(&env_var("IBKR_READ_ONLY")?)?;
        let connect_on_start = parse_bool(&env_var("IBKR_CONNECT_ON_START")?)?;
        let run_mode = RunMode::parse(&env_or_default("RUN_MODE", "manual")?)?;
        let scan_schedule = env_or_default("SCAN_SCHEDULE", "0 45 9,12,15 * * MON-FRI")?;
        let market_data_mode = MarketDataMode::parse(&env_or_default("MARKET_DATA_MODE", "live")?)?;
        let universe_file = optional_env("UNIVERSE_FILE");
        let symbols = optional_env("IBKR_SYMBOLS")
            .map(|value| parse_symbols(&value))
            .unwrap_or_default();

        if symbols.is_empty() && universe_file.is_none() {
            anyhow::bail!("set IBKR_SYMBOLS or UNIVERSE_FILE before starting the scanner");
        }

        let defaults = StrategyConfig::default();
        let risk_defaults = RiskConfig::default();

        Ok(Self {
            host,
            platform,
            port,
            client_id,
            account,
            mode,
            read_only,
            connect_on_start,
            run_mode,
            scan_schedule,
            market_data_mode,
            universe_file,
            symbols,
            strategy: StrategyConfig {
                default_beta: env_or_default("DEFAULT_BETA", &defaults.default_beta.to_string())?
                    .parse()
                    .context("DEFAULT_BETA must be numeric")?,
                min_expiry_days: env_or_default(
                    "MIN_EXPIRY_DAYS",
                    &defaults.min_expiry_days.to_string(),
                )?
                .parse()
                .context("MIN_EXPIRY_DAYS must be numeric")?,
                max_expiry_days: env_or_default(
                    "MAX_EXPIRY_DAYS",
                    &defaults.max_expiry_days.to_string(),
                )?
                .parse()
                .context("MAX_EXPIRY_DAYS must be numeric")?,
                min_annualized_yield_pct: env_or_default(
                    "MIN_ANNUALIZED_YIELD_PCT",
                    &defaults.min_annualized_yield_pct.to_string(),
                )?
                .parse()
                .context("MIN_ANNUALIZED_YIELD_PCT must be numeric")?,
                min_itm_depth_pct: env_or_default(
                    "MIN_ITM_DEPTH_PCT",
                    &defaults.min_itm_depth_pct.to_string(),
                )?
                .parse()
                .context("MIN_ITM_DEPTH_PCT must be numeric")?,
                min_downside_buffer_pct: env_or_default(
                    "MIN_DOWNSIDE_BUFFER_PCT",
                    &defaults.min_downside_buffer_pct.to_string(),
                )?
                .parse()
                .context("MIN_DOWNSIDE_BUFFER_PCT must be numeric")?,
                min_option_bid: env_or_default(
                    "MIN_OPTION_BID",
                    &defaults.min_option_bid.to_string(),
                )?
                .parse()
                .context("MIN_OPTION_BID must be numeric")?,
                max_option_spread_pct: env_or_default(
                    "MAX_OPTION_SPREAD_PCT",
                    &defaults.max_option_spread_pct.to_string(),
                )?
                .parse()
                .context("MAX_OPTION_SPREAD_PCT must be numeric")?,
                profit_take_pct: env_or_default(
                    "PROFIT_TAKE_PCT",
                    &defaults.profit_take_pct.to_string(),
                )?
                .parse()
                .context("PROFIT_TAKE_PCT must be numeric")?,
                max_loss_pct: env_or_default("MAX_LOSS_PCT", &defaults.max_loss_pct.to_string())?
                    .parse()
                    .context("MAX_LOSS_PCT must be numeric")?,
            },
            risk: RiskConfig {
                min_underlying_price: env_or_default(
                    "MIN_UNDERLYING_PRICE",
                    &risk_defaults.min_underlying_price.to_string(),
                )?
                .parse()
                .context("MIN_UNDERLYING_PRICE must be numeric")?,
                max_underlying_price: env_or_default(
                    "MAX_UNDERLYING_PRICE",
                    &risk_defaults.max_underlying_price.to_string(),
                )?
                .parse()
                .context("MAX_UNDERLYING_PRICE must be numeric")?,
                max_underlyings_per_cycle: env_or_default(
                    "MAX_UNDERLYINGS_PER_CYCLE",
                    &risk_defaults.max_underlyings_per_cycle.to_string(),
                )?
                .parse()
                .context("MAX_UNDERLYINGS_PER_CYCLE must be numeric")?,
                max_option_quotes_per_underlying: env_or_default(
                    "MAX_OPTION_QUOTES_PER_UNDERLYING",
                    &risk_defaults.max_option_quotes_per_underlying.to_string(),
                )?
                .parse()
                .context("MAX_OPTION_QUOTES_PER_UNDERLYING must be numeric")?,
                max_new_trades_per_cycle: env_or_default(
                    "MAX_NEW_TRADES_PER_CYCLE",
                    &risk_defaults.max_new_trades_per_cycle.to_string(),
                )?
                .parse()
                .context("MAX_NEW_TRADES_PER_CYCLE must be numeric")?,
                max_open_positions: env_or_default(
                    "MAX_OPEN_POSITIONS",
                    &risk_defaults.max_open_positions.to_string(),
                )?
                .parse()
                .context("MAX_OPEN_POSITIONS must be numeric")?,
                min_buying_power_buffer_pct: env_or_default(
                    "MIN_BUYING_POWER_BUFFER_PCT",
                    &risk_defaults.min_buying_power_buffer_pct.to_string(),
                )?
                .parse()
                .context("MIN_BUYING_POWER_BUFFER_PCT must be numeric")?,
                enable_paper_orders: parse_bool(&env_or_default("ENABLE_PAPER_ORDERS", "false")?)?,
                enable_live_orders: parse_bool(&env_or_default("ENABLE_LIVE_ORDERS", "false")?)?,
            },
        })
    }

    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn connection_guidance(&self) -> String {
        format!(
            "Targeting {} {:?} at {}. Expected default port is {}. Switch platforms by changing IBKR_PLATFORM between gateway and tws; the port will follow the selected platform unless IBKR_PORT overrides it. For IB Gateway, enable Configure > Settings > API > Settings > Enable ActiveX and Socket Clients, and allow localhost or add 127.0.0.1 to Trusted IPs if needed.",
            self.platform.label(),
            self.mode,
            self.endpoint(),
            self.platform.expected_port_hint(self.mode)
        )
    }

    pub fn prefers_live_market_data(&self) -> bool {
        matches!(self.market_data_mode, MarketDataMode::Live)
    }
}

fn env_var(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("missing required environment variable {key}"))
}

fn env_or_default(key: &str, default: &str) -> Result<String> {
    Ok(optional_env(key).unwrap_or_else(|| default.to_string()))
}

fn optional_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub fn parse_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => anyhow::bail!("unsupported boolean value: {other}"),
    }
}

pub fn parse_symbols(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(|symbol| symbol.to_ascii_uppercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{BrokerPlatform, MarketDataMode, RunMode, RuntimeMode, parse_bool, parse_symbols};

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

    #[test]
    fn parses_run_and_market_data_modes() {
        assert_eq!(RunMode::parse("scheduled").unwrap(), RunMode::Scheduled);
        assert_eq!(
            MarketDataMode::parse("delayed-frozen").unwrap(),
            MarketDataMode::DelayedFrozen
        );
    }

    #[test]
    fn gateway_default_port_matches_mode() {
        assert_eq!(
            BrokerPlatform::Gateway.default_port(RuntimeMode::Paper),
            4002
        );
        assert_eq!(
            BrokerPlatform::Gateway.default_port(RuntimeMode::Live),
            4001
        );
    }
}
