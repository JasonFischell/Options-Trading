use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "ibkr-options-engine")]
#[command(about = "IBKR covered-call scanner and paper-routing helper")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Scan(ConfigArgs),
    Status(ConfigArgs),
}

#[derive(Debug, Clone, Args, Default)]
pub struct ConfigArgs {
    #[arg(long)]
    pub config: Option<PathBuf>,
}
