use clap::Parser;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone, Parser, Serialize, Deserialize)]
#[command(name = "yamlbase")]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(short, long, value_name = "FILE", help = "Path to YAML database file")]
    pub file: PathBuf,

    #[arg(
        short,
        long,
        help = "Port to listen on (default: 5432 for postgres, 3306 for mysql)"
    )]
    pub port: Option<u16>,

    #[arg(long, default_value = "0.0.0.0", help = "Address to bind to")]
    pub bind_address: String,

    #[arg(
        long,
        value_enum,
        default_value = "postgres",
        help = "SQL protocol to use"
    )]
    pub protocol: Protocol,

    #[arg(
        short = 'u',
        long,
        default_value = "admin",
        help = "Authentication username"
    )]
    pub username: String,

    #[arg(
        short = 'P',
        long,
        default_value = "password",
        help = "Authentication password"
    )]
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

    #[arg(
        long,
        help = "Allow anonymous connections (no authentication required)"
    )]
    pub allow_anonymous: bool,

    // Connection management settings (not exposed via CLI - configured via YAML)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[clap(skip)]
    pub max_connections: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[clap(skip)]
    pub connection_timeout: Option<Duration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "humantime_serde")]
    #[clap(skip)]
    pub idle_timeout: Option<Duration>,

    #[serde(default)]
    #[clap(skip)]
    pub enable_keepalive: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, clap::ValueEnum)]
pub enum Protocol {
    Postgres,
    Mysql,
    Sqlserver,
    Teradata,
}

impl Config {
    pub fn effective_port(&self) -> u16 {
        self.port.unwrap_or(match self.protocol {
            Protocol::Postgres => 5432,
            Protocol::Mysql => 3306,
            Protocol::Sqlserver => 1433,
            Protocol::Teradata => 1025, // Default Teradata port
        })
    }

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
