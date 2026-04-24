use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::NaiveDate;
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CapitalSource {
    AvailableFunds,
    BuyingPower,
}

impl CapitalSource {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "available_funds" | "available-funds" | "cash" => Ok(Self::AvailableFunds),
            "buying_power" | "buying-power" => Ok(Self::BuyingPower),
            other => anyhow::bail!("unsupported capital source: {other}"),
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::AvailableFunds => "available_funds",
            Self::BuyingPower => "buying_power",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StrategyConfig {
    pub default_beta: f64,
    pub expiration_dates: Vec<String>,
    pub min_annualized_yield_ratio: f64,
    pub min_expiration_yield_ratio: f64,
    pub min_expiration_profit_per_share: f64,
    pub min_itm_depth_ratio: f64,
    pub max_itm_depth_ratio: f64,
    pub min_downside_buffer_ratio: f64,
    pub min_option_bid: f64,
    pub max_option_spread_ratio: f64,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            default_beta: 1.5,
            expiration_dates: Vec::new(),
            min_annualized_yield_ratio: 0.12,
            min_expiration_yield_ratio: 0.01,
            min_expiration_profit_per_share: 0.05,
            min_itm_depth_ratio: 0.0,
            max_itm_depth_ratio: 0.50,
            min_downside_buffer_ratio: 0.10,
            min_option_bid: 0.08,
            max_option_spread_ratio: 0.25,
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
    pub min_buying_power_buffer_ratio: f64,
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
            max_new_trades_per_cycle: 5,
            max_open_positions: 5,
            min_buying_power_buffer_ratio: 0.05,
            enable_paper_orders: false,
            enable_live_orders: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AllocationConfig {
    pub deployment_budget: f64,
    pub capital_source: CapitalSource,
    pub max_cash_per_symbol_ratio: f64,
    pub min_cash_reserve_ratio: f64,
}

impl Default for AllocationConfig {
    fn default() -> Self {
        Self {
            deployment_budget: 10_000.0,
            capital_source: CapitalSource::AvailableFunds,
            max_cash_per_symbol_ratio: 0.20,
            min_cash_reserve_ratio: 0.05,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PerformanceConfig {
    pub symbol_concurrency: usize,
    pub option_quote_concurrency_per_symbol: usize,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            symbol_concurrency: 4,
            option_quote_concurrency_per_symbol: 2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionTuningConfig {
    pub auto_reprice: bool,
    pub reprice_attempts: usize,
    pub reprice_wait_seconds: u64,
}

impl Default for ExecutionTuningConfig {
    fn default() -> Self {
        Self {
            auto_reprice: true,
            reprice_attempts: 3,
            reprice_wait_seconds: 2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogsConfig {
    pub diagnostic_log: bool,
    pub action_log: bool,
    pub trade_log: bool,
    pub print_statements: bool,
    pub api_log: bool,
}

impl Default for LogsConfig {
    fn default() -> Self {
        Self {
            diagnostic_log: false,
            action_log: false,
            trade_log: true,
            print_statements: true,
            api_log: false,
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
    pub startup_warnings: Vec<String>,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
    pub allocation: AllocationConfig,
    pub performance: PerformanceConfig,
    pub execution: ExecutionTuningConfig,
    pub logs: LogsConfig,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_path(None)
    }

    pub fn from_path(path: Option<&Path>) -> Result<Self> {
        let mut merged = ConfigOverrides::from_env()?;
        let config_path = path.map(Path::to_path_buf).or_else(default_config_path);
        if let Some(path) = config_path.as_deref() {
            let mut file_overrides = ConfigOverrides::from_file(path)?;
            file_overrides
                .startup_warnings
                .push(format!("Loaded configuration from {}.", path.display()));
            merged.apply(file_overrides);
        }
        merged.build()
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

    pub fn universe_source_label(&self) -> String {
        if !self.symbols.is_empty() {
            "env-symbols".to_string()
        } else if let Some(universe_file) = &self.universe_file {
            format!("csv:{universe_file}")
        } else {
            "disabled".to_string()
        }
    }

    pub fn guarded_paper_submission_requested(&self) -> bool {
        self.risk.enable_paper_orders
            && matches!(self.mode, RuntimeMode::Paper)
            && !self.risk.enable_live_orders
    }

    pub fn guarded_paper_submission_enabled(&self) -> bool {
        self.guarded_paper_submission_requested() && !self.read_only
    }
}

fn default_config_path() -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    [
        cwd.join("docs")
            .join("local")
            .join("ibkr-options-engine.paper-trading.toml"),
        cwd.join("docs")
            .join("local")
            .join("ibkr-options-engine.example.toml"),
        cwd.join("ibkr-options-engine.paper-trading.toml"),
        cwd.join("ibkr-options-engine.toml"),
    ]
    .into_iter()
    .find(|path| path.is_file())
}

#[derive(Debug, Clone, Default)]
struct ConfigOverrides {
    host: Option<String>,
    platform: Option<BrokerPlatform>,
    port: Option<u16>,
    client_id: Option<i32>,
    account: Option<String>,
    mode: Option<RuntimeMode>,
    read_only: Option<bool>,
    connect_on_start: Option<bool>,
    run_mode: Option<RunMode>,
    scan_schedule: Option<String>,
    market_data_mode: Option<MarketDataMode>,
    universe_file: SourceOption<String>,
    symbols: SourceOption<Vec<String>>,
    expiration_dates: SourceOption<Vec<String>>,
    default_beta: Option<f64>,
    min_annualized_yield_ratio: Option<f64>,
    min_expiration_yield_ratio: Option<f64>,
    min_expiration_profit_per_share: Option<f64>,
    min_itm_depth_ratio: Option<f64>,
    max_itm_depth_ratio: Option<f64>,
    min_downside_buffer_ratio: Option<f64>,
    min_option_bid: Option<f64>,
    max_option_spread_ratio: Option<f64>,
    min_underlying_price: Option<f64>,
    max_underlying_price: Option<f64>,
    max_underlyings_per_cycle: Option<usize>,
    max_option_quotes_per_underlying: Option<usize>,
    max_new_trades_per_cycle: Option<usize>,
    max_open_positions: Option<usize>,
    min_buying_power_buffer_ratio: Option<f64>,
    enable_paper_orders: Option<bool>,
    enable_live_orders: Option<bool>,
    deployment_budget: Option<f64>,
    capital_source: Option<CapitalSource>,
    max_cash_per_symbol_ratio: Option<f64>,
    min_cash_reserve_ratio: Option<f64>,
    symbol_concurrency: Option<usize>,
    option_quote_concurrency_per_symbol: Option<usize>,
    auto_reprice: Option<bool>,
    reprice_attempts: Option<usize>,
    reprice_wait_seconds: Option<u64>,
    diagnostic_log: Option<bool>,
    action_log: Option<bool>,
    trade_log: Option<bool>,
    print_statements: Option<bool>,
    api_log: Option<bool>,
    startup_warnings: Vec<String>,
}

impl ConfigOverrides {
    fn from_env() -> Result<Self> {
        let mut startup_warnings = Vec::new();
        let raw_universe_file = first_raw_optional_env(&["UNIVERSE_FILE", "TICKERS_FILE"]);
        let raw_symbols = first_raw_optional_env(&["IBKR_SYMBOLS", "TICKERS"]);
        let raw_expirations =
            first_raw_optional_env(&["EXPIRATION_DATES", "OPTION_EXPIRATION_DATES"]);

        if raw_universe_file
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            startup_warnings.push(
                "UNIVERSE_FILE was set blank, so CSV universe loading is explicitly disabled for this run."
                    .to_string(),
            );
        }
        if raw_symbols
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            startup_warnings.push(
                "IBKR_SYMBOLS was set blank, so env-symbol universe loading is explicitly disabled for this run."
                    .to_string(),
            );
        }
        if raw_expirations
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            startup_warnings.push(
                "EXPIRATION_DATES was set blank, so at least one expiration date must come from a higher-precedence source."
                    .to_string(),
            );
        }

        Ok(Self {
            host: first_optional_env(&["IBKR_HOST"]),
            platform: parse_optional(
                first_optional_env(&["IBKR_PLATFORM"]).as_deref(),
                BrokerPlatform::parse,
            )?,
            port: parse_optional_num(first_optional_env(&["IBKR_PORT"]).as_deref(), "IBKR_PORT")?,
            client_id: parse_optional_num(
                first_optional_env(&["IBKR_CLIENT_ID"]).as_deref(),
                "IBKR_CLIENT_ID",
            )?,
            account: first_optional_env(&["IBKR_ACCOUNT"]),
            mode: parse_optional(
                first_optional_env(&["IBKR_RUNTIME_MODE"]).as_deref(),
                RuntimeMode::parse,
            )?,
            read_only: parse_optional(
                first_optional_env(&["IBKR_READ_ONLY"]).as_deref(),
                parse_bool,
            )?,
            connect_on_start: parse_optional(
                first_optional_env(&["IBKR_CONNECT_ON_START"]).as_deref(),
                parse_bool,
            )?,
            run_mode: parse_optional(first_optional_env(&["RUN_MODE"]).as_deref(), RunMode::parse)?,
            scan_schedule: first_optional_env(&["SCAN_SCHEDULE"]),
            market_data_mode: parse_optional(
                first_optional_env(&["MARKET_DATA_MODE"]).as_deref(),
                MarketDataMode::parse,
            )?,
            universe_file: SourceOption::from_raw(raw_universe_file),
            symbols: SourceOption::from_raw_with(raw_symbols, |value| Ok(parse_symbols(&value)))?,
            expiration_dates: SourceOption::from_raw_with(
                raw_expirations,
                normalize_expiry_list_raw,
            )?,
            default_beta: parse_optional_num(
                first_optional_env(&["DEFAULT_BETA"]).as_deref(),
                "DEFAULT_BETA",
            )?,
            min_annualized_yield_ratio: parse_optional_ratio_env(
                &["MIN_ANNUALIZED_YIELD_RATIO", "MIN_ANNUALIZED_YIELD_PCT"],
                "MIN_ANNUALIZED_YIELD_RATIO",
                &mut startup_warnings,
            )?,
            min_expiration_yield_ratio: parse_optional_ratio_env(
                &[
                    "MIN_EXPIRATION_YIELD_RATIO",
                    "MIN_PROFIT_PCT_OF_INVESTMENT",
                    "MIN_EXPIRATION_YIELD_PCT",
                ],
                "MIN_EXPIRATION_YIELD_RATIO",
                &mut startup_warnings,
            )?,
            min_expiration_profit_per_share: parse_optional_num(
                first_optional_env(&[
                    "MIN_PROFIT_DOLLARS_PER_SHARE",
                    "MIN_EXPIRATION_PROFIT_PER_SHARE",
                ])
                .as_deref(),
                "MIN_PROFIT_DOLLARS_PER_SHARE",
            )?,
            min_itm_depth_ratio: parse_optional_ratio_env(
                &["MIN_ITM_DEPTH_RATIO", "MIN_ITM_DEPTH_PCT"],
                "MIN_ITM_DEPTH_RATIO",
                &mut startup_warnings,
            )?,
            max_itm_depth_ratio: parse_optional_ratio_env(
                &["MAX_ITM_DEPTH_RATIO", "MAX_ITM_DEPTH_PCT"],
                "MAX_ITM_DEPTH_RATIO",
                &mut startup_warnings,
            )?,
            min_downside_buffer_ratio: parse_optional_ratio_env(
                &[
                    "MIN_DOWNSIDE_BUFFER_RATIO",
                    "MIN_PROFIT_BUFFER_PCT",
                    "MIN_DOWNSIDE_BUFFER_PCT",
                ],
                "MIN_DOWNSIDE_BUFFER_RATIO",
                &mut startup_warnings,
            )?,
            min_option_bid: parse_optional_num(
                first_optional_env(&["MIN_OPTION_BID"]).as_deref(),
                "MIN_OPTION_BID",
            )?,
            max_option_spread_ratio: parse_optional_ratio_env(
                &["MAX_OPTION_SPREAD_RATIO", "MAX_OPTION_SPREAD_PCT"],
                "MAX_OPTION_SPREAD_RATIO",
                &mut startup_warnings,
            )?,
            min_underlying_price: parse_optional_num(
                first_optional_env(&["MIN_UNDERLYING_PRICE"]).as_deref(),
                "MIN_UNDERLYING_PRICE",
            )?,
            max_underlying_price: parse_optional_num(
                first_optional_env(&["MAX_UNDERLYING_PRICE"]).as_deref(),
                "MAX_UNDERLYING_PRICE",
            )?,
            max_underlyings_per_cycle: parse_optional_num(
                first_optional_env(&["MAX_UNDERLYINGS_PER_CYCLE"]).as_deref(),
                "MAX_UNDERLYINGS_PER_CYCLE",
            )?,
            max_option_quotes_per_underlying: parse_optional_num(
                first_optional_env(&["MAX_OPTION_QUOTES_PER_UNDERLYING"]).as_deref(),
                "MAX_OPTION_QUOTES_PER_UNDERLYING",
            )?,
            max_new_trades_per_cycle: parse_optional_num(
                first_optional_env(&["MAX_NEW_TRADES_PER_CYCLE"]).as_deref(),
                "MAX_NEW_TRADES_PER_CYCLE",
            )?,
            max_open_positions: parse_optional_num(
                first_optional_env(&["MAX_OPEN_POSITIONS"]).as_deref(),
                "MAX_OPEN_POSITIONS",
            )?,
            min_buying_power_buffer_ratio: parse_optional_ratio_env(
                &[
                    "MIN_BUYING_POWER_BUFFER_RATIO",
                    "MIN_BUYING_POWER_BUFFER_PCT",
                ],
                "MIN_BUYING_POWER_BUFFER_RATIO",
                &mut startup_warnings,
            )?,
            enable_paper_orders: parse_optional(
                first_optional_env(&["ENABLE_PAPER_ORDERS"]).as_deref(),
                parse_bool,
            )?,
            enable_live_orders: parse_optional(
                first_optional_env(&["ENABLE_LIVE_ORDERS"]).as_deref(),
                parse_bool,
            )?,
            deployment_budget: parse_optional_num(
                first_optional_env(&["DEPLOYMENT_BUDGET"]).as_deref(),
                "DEPLOYMENT_BUDGET",
            )?,
            capital_source: parse_optional(
                first_optional_env(&["CAPITAL_SOURCE"]).as_deref(),
                CapitalSource::parse,
            )?,
            max_cash_per_symbol_ratio: parse_optional_ratio_env(
                &[
                    "MAX_CASH_PER_SYMBOL_RATIO",
                    "MAX_DISTRIBUTION_PER_SYMBOL_PCT",
                    "MAX_CASH_PER_SYMBOL_PCT",
                ],
                "MAX_CASH_PER_SYMBOL_RATIO",
                &mut startup_warnings,
            )?,
            min_cash_reserve_ratio: parse_optional_ratio_env(
                &["MIN_CASH_RESERVE_RATIO", "MIN_CASH_RESERVE_PCT"],
                "MIN_CASH_RESERVE_RATIO",
                &mut startup_warnings,
            )?,
            symbol_concurrency: parse_optional_num(
                first_optional_env(&["SYMBOL_CONCURRENCY"]).as_deref(),
                "SYMBOL_CONCURRENCY",
            )?,
            option_quote_concurrency_per_symbol: parse_optional_num(
                first_optional_env(&["OPTION_QUOTE_CONCURRENCY_PER_SYMBOL"]).as_deref(),
                "OPTION_QUOTE_CONCURRENCY_PER_SYMBOL",
            )?,
            auto_reprice: parse_optional(
                first_optional_env(&["AUTO_REPRICE"]).as_deref(),
                parse_bool,
            )?,
            reprice_attempts: parse_optional_num(
                first_optional_env(&["REPRICE_ATTEMPTS"]).as_deref(),
                "REPRICE_ATTEMPTS",
            )?,
            reprice_wait_seconds: parse_optional_num(
                first_optional_env(&["REPRICE_WAIT_SECONDS"]).as_deref(),
                "REPRICE_WAIT_SECONDS",
            )?,
            diagnostic_log: parse_optional(
                first_optional_env(&["DIAGNOSTIC_LOG"]).as_deref(),
                parse_bool,
            )?,
            action_log: parse_optional(first_optional_env(&["ACTION_LOG"]).as_deref(), parse_bool)?,
            trade_log: parse_optional(first_optional_env(&["TRADE_LOG"]).as_deref(), parse_bool)?,
            print_statements: parse_optional(
                first_optional_env(&["PRINT_STATEMENTS"]).as_deref(),
                parse_bool,
            )?,
            api_log: parse_optional(first_optional_env(&["API_LOG"]).as_deref(), parse_bool)?,
            startup_warnings,
        })
    }

    fn from_file(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let parsed: FileConfig = toml::from_str(&raw)
            .with_context(|| format!("failed to parse TOML config file {}", path.display()))?;
        parsed.into_overrides()
    }

    fn apply(&mut self, higher: Self) {
        self.host = higher.host.or(self.host.take());
        self.platform = higher.platform.or(self.platform.take());
        self.port = higher.port.or(self.port.take());
        self.client_id = higher.client_id.or(self.client_id.take());
        self.account = higher.account.or(self.account.take());
        self.mode = higher.mode.or(self.mode.take());
        self.read_only = higher.read_only.or(self.read_only.take());
        self.connect_on_start = higher.connect_on_start.or(self.connect_on_start.take());
        self.run_mode = higher.run_mode.or(self.run_mode.take());
        self.scan_schedule = higher.scan_schedule.or(self.scan_schedule.take());
        self.market_data_mode = higher.market_data_mode.or(self.market_data_mode.take());
        self.universe_file = higher.universe_file.or(self.universe_file.clone());
        self.symbols = higher.symbols.or(self.symbols.clone());
        self.expiration_dates = higher.expiration_dates.or(self.expiration_dates.clone());
        self.default_beta = higher.default_beta.or(self.default_beta.take());
        self.min_annualized_yield_ratio = higher
            .min_annualized_yield_ratio
            .or(self.min_annualized_yield_ratio.take());
        self.min_expiration_yield_ratio = higher
            .min_expiration_yield_ratio
            .or(self.min_expiration_yield_ratio.take());
        self.min_expiration_profit_per_share = higher
            .min_expiration_profit_per_share
            .or(self.min_expiration_profit_per_share.take());
        self.min_itm_depth_ratio = higher
            .min_itm_depth_ratio
            .or(self.min_itm_depth_ratio.take());
        self.max_itm_depth_ratio = higher
            .max_itm_depth_ratio
            .or(self.max_itm_depth_ratio.take());
        self.min_downside_buffer_ratio = higher
            .min_downside_buffer_ratio
            .or(self.min_downside_buffer_ratio.take());
        self.min_option_bid = higher.min_option_bid.or(self.min_option_bid.take());
        self.max_option_spread_ratio = higher
            .max_option_spread_ratio
            .or(self.max_option_spread_ratio.take());
        self.min_underlying_price = higher
            .min_underlying_price
            .or(self.min_underlying_price.take());
        self.max_underlying_price = higher
            .max_underlying_price
            .or(self.max_underlying_price.take());
        self.max_underlyings_per_cycle = higher
            .max_underlyings_per_cycle
            .or(self.max_underlyings_per_cycle.take());
        self.max_option_quotes_per_underlying = higher
            .max_option_quotes_per_underlying
            .or(self.max_option_quotes_per_underlying.take());
        self.max_new_trades_per_cycle = higher
            .max_new_trades_per_cycle
            .or(self.max_new_trades_per_cycle.take());
        self.max_open_positions = higher.max_open_positions.or(self.max_open_positions.take());
        self.min_buying_power_buffer_ratio = higher
            .min_buying_power_buffer_ratio
            .or(self.min_buying_power_buffer_ratio.take());
        self.enable_paper_orders = higher
            .enable_paper_orders
            .or(self.enable_paper_orders.take());
        self.enable_live_orders = higher.enable_live_orders.or(self.enable_live_orders.take());
        self.deployment_budget = higher.deployment_budget.or(self.deployment_budget.take());
        self.capital_source = higher.capital_source.or(self.capital_source.take());
        self.max_cash_per_symbol_ratio = higher
            .max_cash_per_symbol_ratio
            .or(self.max_cash_per_symbol_ratio.take());
        self.min_cash_reserve_ratio = higher
            .min_cash_reserve_ratio
            .or(self.min_cash_reserve_ratio.take());
        self.symbol_concurrency = higher.symbol_concurrency.or(self.symbol_concurrency.take());
        self.option_quote_concurrency_per_symbol = higher
            .option_quote_concurrency_per_symbol
            .or(self.option_quote_concurrency_per_symbol.take());
        self.auto_reprice = higher.auto_reprice.or(self.auto_reprice.take());
        self.reprice_attempts = higher.reprice_attempts.or(self.reprice_attempts.take());
        self.reprice_wait_seconds = higher
            .reprice_wait_seconds
            .or(self.reprice_wait_seconds.take());
        self.diagnostic_log = higher.diagnostic_log.or(self.diagnostic_log.take());
        self.action_log = higher.action_log.or(self.action_log.take());
        self.trade_log = higher.trade_log.or(self.trade_log.take());
        self.print_statements = higher.print_statements.or(self.print_statements.take());
        self.api_log = higher.api_log.or(self.api_log.take());
        self.startup_warnings.extend(higher.startup_warnings);
    }

    fn build(self) -> Result<AppConfig> {
        let strategy_defaults = StrategyConfig::default();
        let risk_defaults = RiskConfig::default();
        let allocation_defaults = AllocationConfig::default();
        let performance_defaults = PerformanceConfig::default();
        let execution_defaults = ExecutionTuningConfig::default();
        let logs_defaults = LogsConfig::default();

        let host = self.host.unwrap_or_else(|| "127.0.0.1".to_string());
        let mode = self.mode.unwrap_or(RuntimeMode::Paper);
        let platform = self.platform.unwrap_or(BrokerPlatform::Gateway);
        let port = self.port.unwrap_or_else(|| platform.default_port(mode));
        let client_id = self.client_id.unwrap_or(100);
        let account = self
            .account
            .filter(|value| !value.trim().is_empty())
            .context("missing required IBKR account (set IBKR_ACCOUNT or broker.account)")?;
        let read_only = self.read_only.unwrap_or(true);
        let connect_on_start = self.connect_on_start.unwrap_or(false);
        let run_mode = self.run_mode.unwrap_or(RunMode::Scheduled);
        let scan_schedule = self
            .scan_schedule
            .unwrap_or_else(|| "0 45 9,12,15 * * MON-FRI".to_string());
        let market_data_mode = self
            .market_data_mode
            .unwrap_or(MarketDataMode::DelayedFrozen);
        let explicit_universe_file = self.universe_file.into_option();
        let symbols = self.symbols.into_vec();
        let expiration_dates = self.expiration_dates.into_vec();

        let mut startup_warnings = self.startup_warnings;
        if !symbols.is_empty() && explicit_universe_file.is_some() {
            startup_warnings.push(format!(
                "Both a ticker list and universe file were set; explicit tickers will override {} for this run.",
                explicit_universe_file
                    .as_deref()
                    .unwrap_or("the configured CSV universe")
            ));
        }

        if symbols.is_empty() && explicit_universe_file.is_none() {
            anyhow::bail!("set a ticker file or at least one ticker before starting the scanner");
        }
        if expiration_dates.is_empty() {
            anyhow::bail!(
                "set at least one expiration date in YYYYMMDD format before starting the scanner"
            );
        }

        let allocation = AllocationConfig {
            deployment_budget: self
                .deployment_budget
                .unwrap_or(allocation_defaults.deployment_budget),
            capital_source: self
                .capital_source
                .unwrap_or(allocation_defaults.capital_source),
            max_cash_per_symbol_ratio: self
                .max_cash_per_symbol_ratio
                .unwrap_or(allocation_defaults.max_cash_per_symbol_ratio),
            min_cash_reserve_ratio: self
                .min_cash_reserve_ratio
                .unwrap_or(allocation_defaults.min_cash_reserve_ratio),
        };
        if matches!(allocation.capital_source, CapitalSource::BuyingPower) {
            startup_warnings.push(
                "CAPITAL_SOURCE=buying_power is analysis-only; routed paper sizing will continue to use available funds."
                    .to_string(),
            );
        }

        Ok(AppConfig {
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
            universe_file: explicit_universe_file,
            symbols,
            startup_warnings,
            strategy: StrategyConfig {
                default_beta: self.default_beta.unwrap_or(strategy_defaults.default_beta),
                expiration_dates,
                min_annualized_yield_ratio: self
                    .min_annualized_yield_ratio
                    .unwrap_or(strategy_defaults.min_annualized_yield_ratio),
                min_expiration_yield_ratio: self
                    .min_expiration_yield_ratio
                    .unwrap_or(strategy_defaults.min_expiration_yield_ratio),
                min_expiration_profit_per_share: self
                    .min_expiration_profit_per_share
                    .unwrap_or(strategy_defaults.min_expiration_profit_per_share),
                min_itm_depth_ratio: self
                    .min_itm_depth_ratio
                    .unwrap_or(strategy_defaults.min_itm_depth_ratio),
                max_itm_depth_ratio: self
                    .max_itm_depth_ratio
                    .unwrap_or(strategy_defaults.max_itm_depth_ratio),
                min_downside_buffer_ratio: self
                    .min_downside_buffer_ratio
                    .unwrap_or(strategy_defaults.min_downside_buffer_ratio),
                min_option_bid: self
                    .min_option_bid
                    .unwrap_or(strategy_defaults.min_option_bid),
                max_option_spread_ratio: self
                    .max_option_spread_ratio
                    .unwrap_or(strategy_defaults.max_option_spread_ratio),
            },
            risk: RiskConfig {
                min_underlying_price: self
                    .min_underlying_price
                    .unwrap_or(risk_defaults.min_underlying_price),
                max_underlying_price: self
                    .max_underlying_price
                    .unwrap_or(risk_defaults.max_underlying_price),
                max_underlyings_per_cycle: self
                    .max_underlyings_per_cycle
                    .unwrap_or(risk_defaults.max_underlyings_per_cycle),
                max_option_quotes_per_underlying: self
                    .max_option_quotes_per_underlying
                    .unwrap_or(risk_defaults.max_option_quotes_per_underlying),
                max_new_trades_per_cycle: self
                    .max_new_trades_per_cycle
                    .unwrap_or(risk_defaults.max_new_trades_per_cycle),
                max_open_positions: self
                    .max_open_positions
                    .unwrap_or(risk_defaults.max_open_positions),
                min_buying_power_buffer_ratio: self
                    .min_buying_power_buffer_ratio
                    .unwrap_or(risk_defaults.min_buying_power_buffer_ratio),
                enable_paper_orders: self
                    .enable_paper_orders
                    .unwrap_or(risk_defaults.enable_paper_orders),
                enable_live_orders: self
                    .enable_live_orders
                    .unwrap_or(risk_defaults.enable_live_orders),
            },
            allocation,
            performance: PerformanceConfig {
                symbol_concurrency: self
                    .symbol_concurrency
                    .unwrap_or(performance_defaults.symbol_concurrency),
                option_quote_concurrency_per_symbol: self
                    .option_quote_concurrency_per_symbol
                    .unwrap_or(performance_defaults.option_quote_concurrency_per_symbol),
            },
            execution: ExecutionTuningConfig {
                auto_reprice: self.auto_reprice.unwrap_or(execution_defaults.auto_reprice),
                reprice_attempts: self
                    .reprice_attempts
                    .unwrap_or(execution_defaults.reprice_attempts),
                reprice_wait_seconds: self
                    .reprice_wait_seconds
                    .unwrap_or(execution_defaults.reprice_wait_seconds),
            },
            logs: LogsConfig {
                diagnostic_log: self.diagnostic_log.unwrap_or(logs_defaults.diagnostic_log),
                action_log: self.action_log.unwrap_or(logs_defaults.action_log),
                trade_log: self.trade_log.unwrap_or(logs_defaults.trade_log),
                print_statements: self
                    .print_statements
                    .unwrap_or(logs_defaults.print_statements),
                api_log: self.api_log.unwrap_or(logs_defaults.api_log),
            },
        })
    }
}

#[derive(Debug, Clone, Default)]
enum SourceOption<T> {
    #[default]
    Unset,
    Value(T),
    ExplicitNone,
}

impl<T> SourceOption<T> {
    fn from_raw_with<F>(raw: Option<String>, parser: F) -> Result<Self>
    where
        F: FnOnce(String) -> Result<T>,
    {
        match raw {
            Some(value) if value.trim().is_empty() => Ok(Self::ExplicitNone),
            Some(value) => Ok(Self::Value(parser(value)?)),
            None => Ok(Self::Unset),
        }
    }

    fn or(self, lower: Self) -> Self {
        match self {
            Self::Unset => lower,
            _ => self,
        }
    }
}

impl SourceOption<String> {
    fn from_raw(raw: Option<String>) -> Self {
        match raw {
            Some(value) if value.trim().is_empty() => Self::ExplicitNone,
            Some(value) => Self::Value(value.trim().to_string()),
            None => Self::Unset,
        }
    }

    fn into_option(self) -> Option<String> {
        match self {
            Self::Value(value) => Some(value),
            Self::Unset | Self::ExplicitNone => None,
        }
    }
}

impl SourceOption<Vec<String>> {
    fn into_vec(self) -> Vec<String> {
        match self {
            Self::Value(values) => values,
            Self::Unset | Self::ExplicitNone => Vec::new(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    broker: Option<BrokerSection>,
    universe: Option<UniverseSection>,
    strategy: Option<StrategySection>,
    allocation: Option<AllocationSection>,
    performance: Option<PerformanceSection>,
    execution: Option<ExecutionSection>,
    logs: Option<LogsSection>,
}

#[derive(Debug, Default, Deserialize)]
struct BrokerSection {
    host: Option<String>,
    platform: Option<String>,
    port: Option<u16>,
    client_id: Option<i32>,
    account: Option<String>,
    runtime_mode: Option<String>,
    read_only: Option<bool>,
    connect_on_start: Option<bool>,
    run_mode: Option<String>,
    scan_schedule: Option<String>,
    market_data_mode: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct UniverseSection {
    tickers_file: Option<String>,
    tickers: Option<StringListValue>,
}

#[derive(Debug, Default, Deserialize)]
struct StrategySection {
    expiration_dates: Option<StringListValue>,
    min_underlying_price: Option<f64>,
    max_underlying_price: Option<f64>,
    min_profit_dollars_per_share: Option<f64>,
    min_profit_pct_of_investment: Option<f64>,
    min_profit_buffer_pct: Option<f64>,
    #[serde(alias = "min_annualized_yield_pct")]
    min_annualized_yield_ratio: Option<f64>,
    #[serde(alias = "min_expiration_yield_pct")]
    min_expiration_yield_ratio: Option<f64>,
    #[serde(alias = "min_itm_depth_pct")]
    min_itm_depth_ratio: Option<f64>,
    #[serde(alias = "max_itm_depth_pct")]
    max_itm_depth_ratio: Option<f64>,
    #[serde(alias = "min_downside_buffer_pct")]
    min_downside_buffer_ratio: Option<f64>,
    min_option_bid: Option<f64>,
    #[serde(alias = "max_option_spread_pct")]
    max_option_spread_ratio: Option<f64>,
    default_beta: Option<f64>,
}

#[derive(Debug, Default, Deserialize)]
struct AllocationSection {
    deployment_budget: Option<f64>,
    capital_source: Option<String>,
    max_distribution_per_symbol_pct: Option<f64>,
    #[serde(alias = "max_cash_per_symbol_pct")]
    max_cash_per_symbol_ratio: Option<f64>,
    #[serde(alias = "min_cash_reserve_pct")]
    min_cash_reserve_ratio: Option<f64>,
    max_new_trades_per_cycle: Option<usize>,
    max_open_positions: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct PerformanceSection {
    symbol_concurrency: Option<usize>,
    option_quote_concurrency_per_symbol: Option<usize>,
    max_underlyings_per_cycle: Option<usize>,
    max_option_quotes_per_underlying: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct ExecutionSection {
    enable_paper_orders: Option<bool>,
    enable_live_orders: Option<bool>,
    auto_reprice: Option<bool>,
    reprice_attempts: Option<usize>,
    reprice_wait_seconds: Option<u64>,
    #[serde(alias = "min_buying_power_buffer_pct")]
    min_buying_power_buffer_ratio: Option<f64>,
}

#[derive(Debug, Default, Deserialize)]
struct LogsSection {
    diagnostic_log: Option<bool>,
    action_log: Option<bool>,
    trade_log: Option<bool>,
    print_statements: Option<bool>,
    #[serde(rename = "API_log", alias = "api_log")]
    api_log: Option<bool>,
}

impl FileConfig {
    fn into_overrides(self) -> Result<ConfigOverrides> {
        let broker = self.broker.unwrap_or_default();
        let universe = self.universe.unwrap_or_default();
        let strategy = self.strategy.unwrap_or_default();
        let allocation = self.allocation.unwrap_or_default();
        let performance = self.performance.unwrap_or_default();
        let execution = self.execution.unwrap_or_default();
        let logs = self.logs.unwrap_or_default();
        let mut startup_warnings = Vec::new();

        Ok(ConfigOverrides {
            host: normalize_optional_string(broker.host),
            platform: parse_optional(
                normalize_optional_string(broker.platform).as_deref(),
                BrokerPlatform::parse,
            )?,
            port: broker.port,
            client_id: broker.client_id,
            account: normalize_optional_string(broker.account),
            mode: parse_optional(
                normalize_optional_string(broker.runtime_mode).as_deref(),
                RuntimeMode::parse,
            )?,
            read_only: broker.read_only,
            connect_on_start: broker.connect_on_start,
            run_mode: parse_optional(
                normalize_optional_string(broker.run_mode).as_deref(),
                RunMode::parse,
            )?,
            scan_schedule: normalize_optional_string(broker.scan_schedule),
            market_data_mode: parse_optional(
                normalize_optional_string(broker.market_data_mode).as_deref(),
                MarketDataMode::parse,
            )?,
            universe_file: match normalize_optional_string(universe.tickers_file) {
                Some(value) => SourceOption::Value(value),
                None => SourceOption::Unset,
            },
            symbols: universe
                .tickers
                .map(|value| value.into_symbols())
                .map(SourceOption::Value)
                .unwrap_or(SourceOption::Unset),
            expiration_dates: match strategy.expiration_dates {
                Some(values) => SourceOption::Value(normalize_expiry_values(values.into_values())?),
                None => SourceOption::Unset,
            },
            default_beta: strategy.default_beta,
            min_annualized_yield_ratio: normalize_optional_ratio(
                strategy.min_annualized_yield_ratio,
                "strategy.min_annualized_yield_ratio",
                "strategy.min_annualized_yield_ratio",
                &mut startup_warnings,
            )?
            .or(normalize_optional_ratio(
                strategy.min_profit_pct_of_investment,
                "strategy.min_profit_pct_of_investment",
                "strategy.min_expiration_yield_ratio",
                &mut startup_warnings,
            )?),
            min_expiration_yield_ratio: normalize_optional_ratio(
                strategy.min_expiration_yield_ratio,
                "strategy.min_expiration_yield_ratio",
                "strategy.min_expiration_yield_ratio",
                &mut startup_warnings,
            )?
            .or(normalize_optional_ratio(
                strategy.min_profit_pct_of_investment,
                "strategy.min_profit_pct_of_investment",
                "strategy.min_expiration_yield_ratio",
                &mut startup_warnings,
            )?),
            min_expiration_profit_per_share: strategy.min_profit_dollars_per_share,
            min_itm_depth_ratio: normalize_optional_ratio(
                strategy.min_itm_depth_ratio,
                "strategy.min_itm_depth_ratio",
                "strategy.min_itm_depth_ratio",
                &mut startup_warnings,
            )?,
            max_itm_depth_ratio: normalize_optional_ratio(
                strategy.max_itm_depth_ratio,
                "strategy.max_itm_depth_ratio",
                "strategy.max_itm_depth_ratio",
                &mut startup_warnings,
            )?,
            min_downside_buffer_ratio: normalize_optional_ratio(
                strategy.min_downside_buffer_ratio,
                "strategy.min_downside_buffer_ratio",
                "strategy.min_downside_buffer_ratio",
                &mut startup_warnings,
            )?
            .or(normalize_optional_ratio(
                strategy.min_profit_buffer_pct,
                "strategy.min_profit_buffer_pct",
                "strategy.min_downside_buffer_ratio",
                &mut startup_warnings,
            )?),
            min_option_bid: strategy.min_option_bid,
            max_option_spread_ratio: normalize_optional_ratio(
                strategy.max_option_spread_ratio,
                "strategy.max_option_spread_ratio",
                "strategy.max_option_spread_ratio",
                &mut startup_warnings,
            )?,
            min_underlying_price: strategy.min_underlying_price,
            max_underlying_price: strategy.max_underlying_price,
            max_underlyings_per_cycle: performance.max_underlyings_per_cycle,
            max_option_quotes_per_underlying: performance.max_option_quotes_per_underlying,
            max_new_trades_per_cycle: allocation.max_new_trades_per_cycle,
            max_open_positions: allocation.max_open_positions,
            min_buying_power_buffer_ratio: normalize_optional_ratio(
                execution.min_buying_power_buffer_ratio,
                if execution.min_buying_power_buffer_ratio.is_some() {
                    "execution.min_buying_power_buffer_ratio"
                } else {
                    "execution.min_buying_power_buffer_pct"
                },
                "execution.min_buying_power_buffer_ratio",
                &mut startup_warnings,
            )?,
            enable_paper_orders: execution.enable_paper_orders,
            enable_live_orders: execution.enable_live_orders,
            deployment_budget: allocation.deployment_budget,
            capital_source: parse_optional(
                normalize_optional_string(allocation.capital_source).as_deref(),
                CapitalSource::parse,
            )?,
            max_cash_per_symbol_ratio: normalize_optional_ratio(
                allocation
                    .max_cash_per_symbol_ratio
                    .or(allocation.max_distribution_per_symbol_pct),
                if allocation.max_cash_per_symbol_ratio.is_some() {
                    "allocation.max_cash_per_symbol_ratio"
                } else {
                    "allocation.max_distribution_per_symbol_pct"
                },
                "allocation.max_cash_per_symbol_ratio",
                &mut startup_warnings,
            )?,
            min_cash_reserve_ratio: normalize_optional_ratio(
                allocation.min_cash_reserve_ratio,
                if allocation.min_cash_reserve_ratio.is_some() {
                    "allocation.min_cash_reserve_ratio"
                } else {
                    "allocation.min_cash_reserve_pct"
                },
                "allocation.min_cash_reserve_ratio",
                &mut startup_warnings,
            )?,
            symbol_concurrency: performance.symbol_concurrency,
            option_quote_concurrency_per_symbol: performance.option_quote_concurrency_per_symbol,
            auto_reprice: execution.auto_reprice,
            reprice_attempts: execution.reprice_attempts,
            reprice_wait_seconds: execution.reprice_wait_seconds,
            diagnostic_log: logs.diagnostic_log,
            action_log: logs.action_log,
            trade_log: logs.trade_log,
            print_statements: logs.print_statements,
            api_log: logs.api_log,
            startup_warnings,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum StringListValue {
    Csv(String),
    List(Vec<String>),
}

impl StringListValue {
    fn into_values(self) -> Vec<String> {
        match self {
            Self::Csv(value) => value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.to_string())
                .collect(),
            Self::List(values) => values,
        }
    }

    fn into_symbols(self) -> Vec<String> {
        self.into_values()
            .into_iter()
            .map(|value| value.trim().to_ascii_uppercase())
            .filter(|value| !value.is_empty())
            .collect()
    }
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn normalize_expiry_list_raw(raw: String) -> Result<Vec<String>> {
    normalize_expiry_values(
        raw.split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string())
            .collect(),
    )
}

fn normalize_expiry_values(values: Vec<String>) -> Result<Vec<String>> {
    values
        .into_iter()
        .map(|value| {
            NaiveDate::parse_from_str(value.trim(), "%Y%m%d")
                .or_else(|_| NaiveDate::parse_from_str(value.trim(), "%Y-%m-%d"))
                .with_context(|| "expiration dates must use YYYYMMDD or YYYY-MM-DD".to_string())
                .map(|parsed| parsed.format("%Y%m%d").to_string())
        })
        .collect()
}

fn first_raw_optional_env(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| std::env::var(key).ok())
}

fn first_optional_env(keys: &[&str]) -> Option<String> {
    first_raw_optional_env(keys)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn first_present_env<'a>(keys: &'a [&'a str]) -> Option<(&'a str, String)> {
    keys.iter()
        .find_map(|key| std::env::var(key).ok().map(|value| (*key, value)))
}

fn parse_optional<T, F>(value: Option<&str>, parser: F) -> Result<Option<T>>
where
    F: FnOnce(&str) -> Result<T>,
{
    value.map(parser).transpose()
}

fn parse_optional_num<T>(value: Option<&str>, label: &str) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .map(|value| {
            value
                .parse()
                .map_err(|error| anyhow::anyhow!("{label} must be numeric: {error}"))
        })
        .transpose()
}

fn parse_optional_ratio_env(
    keys: &[&str],
    canonical_name: &str,
    startup_warnings: &mut Vec<String>,
) -> Result<Option<f64>> {
    let Some((used_key, raw_value)) = first_present_env(keys) else {
        return Ok(None);
    };
    let value = raw_value
        .trim()
        .parse::<f64>()
        .map_err(|error| anyhow::anyhow!("{used_key} must be numeric: {error}"))?;
    normalize_ratio_value(value, used_key, canonical_name, startup_warnings).map(Some)
}

fn normalize_optional_ratio(
    value: Option<f64>,
    source_name: &str,
    canonical_name: &str,
    startup_warnings: &mut Vec<String>,
) -> Result<Option<f64>> {
    value
        .map(|value| normalize_ratio_value(value, source_name, canonical_name, startup_warnings))
        .transpose()
}

fn normalize_ratio_value(
    value: f64,
    source_name: &str,
    canonical_name: &str,
    startup_warnings: &mut Vec<String>,
) -> Result<f64> {
    if !value.is_finite() {
        anyhow::bail!("{source_name} must be finite");
    }
    if value < 0.0 {
        anyhow::bail!("{source_name} must be non-negative");
    }

    let normalized = if value > 1.0 { value / 100.0 } else { value };
    if source_name != canonical_name {
        startup_warnings.push(format!(
            "{source_name} is deprecated; use {canonical_name} with ratio values in the 0.0..=1.0 range."
        ));
    }
    if value > 1.0 {
        startup_warnings.push(format!(
            "{source_name}={value} was interpreted as a whole percent and normalized to ratio {normalized:.6}."
        ));
    }

    Ok(normalized)
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
    use std::path::PathBuf;

    use super::{
        AppConfig, BrokerPlatform, CapitalSource, LogsConfig, MarketDataMode, RunMode, RuntimeMode,
        normalize_expiry_values, parse_bool, parse_symbols,
    };

    fn temp_config_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ibkr-options-engine-{name}-{}.toml",
            std::process::id()
        ))
    }

    fn temp_test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ibkr-options-engine-{name}-dir-{}",
            std::process::id()
        ))
    }

    fn clear_env() {
        for key in [
            "IBKR_HOST",
            "IBKR_PLATFORM",
            "IBKR_PORT",
            "IBKR_CLIENT_ID",
            "IBKR_ACCOUNT",
            "IBKR_RUNTIME_MODE",
            "IBKR_READ_ONLY",
            "IBKR_CONNECT_ON_START",
            "RUN_MODE",
            "SCAN_SCHEDULE",
            "MARKET_DATA_MODE",
            "UNIVERSE_FILE",
            "TICKERS_FILE",
            "IBKR_SYMBOLS",
            "TICKERS",
            "EXPIRATION_DATES",
            "OPTION_EXPIRATION_DATES",
            "DEFAULT_BETA",
            "MIN_ANNUALIZED_YIELD_PCT",
            "MIN_PROFIT_PCT_OF_INVESTMENT",
            "MIN_EXPIRATION_YIELD_PCT",
            "MIN_PROFIT_DOLLARS_PER_SHARE",
            "MIN_EXPIRATION_PROFIT_PER_SHARE",
            "MIN_ITM_DEPTH_PCT",
            "MAX_ITM_DEPTH_PCT",
            "MIN_PROFIT_BUFFER_PCT",
            "MIN_DOWNSIDE_BUFFER_PCT",
            "MIN_OPTION_BID",
            "MAX_OPTION_SPREAD_PCT",
            "MIN_UNDERLYING_PRICE",
            "MAX_UNDERLYING_PRICE",
            "MAX_UNDERLYINGS_PER_CYCLE",
            "MAX_OPTION_QUOTES_PER_UNDERLYING",
            "MAX_NEW_TRADES_PER_CYCLE",
            "MAX_OPEN_POSITIONS",
            "MIN_BUYING_POWER_BUFFER_PCT",
            "ENABLE_PAPER_ORDERS",
            "ENABLE_LIVE_ORDERS",
            "DEPLOYMENT_BUDGET",
            "CAPITAL_SOURCE",
            "MAX_DISTRIBUTION_PER_SYMBOL_PCT",
            "MAX_CASH_PER_SYMBOL_PCT",
            "MIN_CASH_RESERVE_PCT",
            "SYMBOL_CONCURRENCY",
            "OPTION_QUOTE_CONCURRENCY_PER_SYMBOL",
            "AUTO_REPRICE",
            "REPRICE_ATTEMPTS",
            "REPRICE_WAIT_SECONDS",
            "DIAGNOSTIC_LOG",
            "ACTION_LOG",
            "TRADE_LOG",
            "PRINT_STATEMENTS",
            "API_LOG",
        ] {
            unsafe {
                std::env::remove_var(key);
            }
        }
    }

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
    fn normalizes_expiration_date_values() {
        let expirations =
            normalize_expiry_values(vec!["2026-04-24".to_string(), "20260515".to_string()])
                .unwrap();
        assert_eq!(expirations, vec!["20260424", "20260515"]);
    }

    #[test]
    fn file_config_overrides_env_values() {
        clear_env();
        unsafe {
            std::env::set_var("IBKR_ACCOUNT", "DU-ENV");
            std::env::set_var("UNIVERSE_FILE", "docs/50_stocks_list.csv");
            std::env::set_var("EXPIRATION_DATES", "20260515");
        }

        let path = temp_config_path("override");
        std::fs::write(
            &path,
            r#"
[broker]
account = "DU-FILE"

[universe]
tickers = ["AAPL", "MSFT"]

[strategy]
expiration_dates = ["20260619"]

[allocation]
deployment_budget = 2500
capital_source = "buying_power"
max_distribution_per_symbol_pct = 15
"#,
        )
        .unwrap();

        let config = AppConfig::from_path(Some(&path)).unwrap();
        assert_eq!(config.account, "DU-FILE");
        assert_eq!(config.symbols, vec!["AAPL", "MSFT"]);
        assert_eq!(config.strategy.expiration_dates, vec!["20260619"]);
        assert_eq!(config.allocation.deployment_budget, 2500.0);
        assert_eq!(config.allocation.capital_source, CapitalSource::BuyingPower);
        assert_eq!(config.allocation.max_cash_per_symbol_ratio, 0.15);
        assert!(
            config
                .startup_warnings
                .iter()
                .any(|warning| { warning.contains("analysis-only") })
        );

        std::fs::remove_file(path).unwrap();
        clear_env();
    }

    #[test]
    fn auto_loads_default_paper_trading_config_from_current_directory() {
        clear_env();

        let test_dir = temp_test_dir("autodiscover");
        let local_dir = test_dir.join("docs").join("local");
        std::fs::create_dir_all(&local_dir).unwrap();
        let path = local_dir.join("ibkr-options-engine.paper-trading.toml");
        std::fs::write(
            &path,
            r#"
[broker]
account = "DU-AUTO"
read_only = false
connect_on_start = true

[universe]
tickers = ["NVTS"]

[strategy]
expiration_dates = ["20260501"]

[execution]
enable_paper_orders = true
"#,
        )
        .unwrap();

        let config = AppConfig::from_path(Some(&path)).unwrap();

        assert_eq!(config.account, "DU-AUTO");
        assert_eq!(config.symbols, vec!["NVTS"]);
        assert!(!config.read_only);
        assert!(config.connect_on_start);
        assert!(config.risk.enable_paper_orders);
        assert!(config.startup_warnings.iter().any(|warning| {
            warning.contains("Loaded configuration from")
                && warning.contains("ibkr-options-engine.paper-trading.toml")
        }));

        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir_all(test_dir);
        clear_env();
    }

    #[test]
    fn requires_universe_source_and_expiration_dates() {
        clear_env();
        let original_dir = std::env::current_dir().unwrap();
        let test_dir = temp_test_dir("requires-config");
        std::fs::create_dir_all(&test_dir).unwrap();
        std::env::set_current_dir(&test_dir).unwrap();
        unsafe {
            std::env::set_var("IBKR_ACCOUNT", "DU1234567");
        }

        let error = AppConfig::from_env().unwrap_err().to_string();
        assert!(error.contains("ticker file or at least one ticker"));

        std::env::set_current_dir(&original_dir).unwrap();
        std::fs::remove_dir_all(test_dir).unwrap();
        clear_env();
    }

    #[test]
    fn default_values_match_wrapper_expectations() {
        clear_env();
        let path = temp_config_path("defaults");
        std::fs::write(
            &path,
            r#"
[broker]
account = "DU1234567"

[universe]
tickers = ["AAPL"]

[strategy]
expiration_dates = ["20260515"]
"#,
        )
        .unwrap();

        let config = AppConfig::from_path(Some(&path)).unwrap();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.platform, BrokerPlatform::Gateway);
        assert_eq!(config.mode, RuntimeMode::Paper);
        assert_eq!(config.run_mode, RunMode::Scheduled);
        assert_eq!(config.market_data_mode, MarketDataMode::DelayedFrozen);
        assert_eq!(config.allocation.deployment_budget, 10_000.0);
        assert_eq!(
            config.allocation.capital_source,
            CapitalSource::AvailableFunds
        );
        assert_eq!(config.allocation.max_cash_per_symbol_ratio, 0.20);
        assert_eq!(config.risk.max_new_trades_per_cycle, 5);
        assert_eq!(config.risk.max_open_positions, 5);
        assert_eq!(config.strategy.min_expiration_profit_per_share, 0.05);
        assert_eq!(config.strategy.min_expiration_yield_ratio, 0.01);
        assert_eq!(config.strategy.min_downside_buffer_ratio, 0.10);
        assert_eq!(config.logs, LogsConfig::default());

        std::fs::remove_file(path).unwrap();
        clear_env();
    }
}
