use clap::Parser;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Parser, Serialize, Deserialize)]
#[command(name = "yamlbase")]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(short, long, value_name = "FILE", help = "Path to YAML database file")]
    pub file: PathBuf,

    #[arg(short, long, default_value = "5432", help = "Port to listen on")]
    pub port: u16,

    #[arg(
        long,
        default_value = "0.0.0.0",
        help = "Address to bind to"
    )]
    pub bind_address: String,

    #[arg(
        long,
        value_enum,
        default_value = "postgres",
        help = "SQL protocol to use"
    )]
    pub protocol: Protocol,

    #[arg(short = 'u', long, default_value = "admin", help = "Authentication username")]
    pub username: String,

    #[arg(short = 'P', long, default_value = "password", help = "Authentication password")]
    pub password: String,

    #[arg(long, help = "Enable hot-reloading of YAML file changes")]
    pub hot_reload: bool,

    #[arg(short, long, help = "Enable verbose logging")]
    pub verbose: bool,

    #[arg(
        long,
        default_value = "info",
        help = "Set log level: debug, info, warn, error"
    )]
    pub log_level: String,

    #[arg(long, help = "Database name")]
    pub database: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, clap::ValueEnum)]
pub enum Protocol {
    Postgres,
    Mysql,
    Sqlserver,
}

impl Config {
    pub fn init_logging(&self) -> anyhow::Result<()> {
        let log_level = if self.verbose {
            "debug"
        } else {
            &self.log_level
        };

        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level));

        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .with_thread_ids(false)
            .with_file(self.verbose)
            .with_line_number(self.verbose)
            .init();

        Ok(())
    }
}