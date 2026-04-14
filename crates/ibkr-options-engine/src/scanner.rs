use crate::config::AppConfig;

#[derive(Debug, Clone)]
pub struct ScanPlan {
    pub symbols: Vec<String>,
    pub execution_mode: &'static str,
}

pub fn build_scan_plan(config: &AppConfig) -> ScanPlan {
    let execution_mode = if config.read_only {
        "read-only"
    } else {
        "trading-enabled"
    };

    ScanPlan {
        symbols: config.symbols.clone(),
        execution_mode,
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{AppConfig, RuntimeMode};
    use crate::scanner::build_scan_plan;

    #[test]
    fn preserves_symbols_and_mode() {
        let config = AppConfig {
            host: "127.0.0.1".to_string(),
            port: 4002,
            client_id: 101,
            account: "DU123456".to_string(),
            mode: RuntimeMode::Paper,
            read_only: true,
            connect_on_start: false,
            symbols: vec!["AAPL".to_string(), "MSFT".to_string()],
        };

        let plan = build_scan_plan(&config);
        assert_eq!(plan.symbols, vec!["AAPL", "MSFT"]);
        assert_eq!(plan.execution_mode, "read-only");
    }
}
