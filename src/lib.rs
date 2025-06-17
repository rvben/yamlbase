pub mod config;
pub mod database;
pub mod protocol;
pub mod server;
pub mod sql;
pub mod yaml;

// Make test_utils available for integration tests
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub use config::Config;
pub use database::Database;
pub use server::Server;

#[derive(thiserror::Error, Debug)]
pub enum YamlBaseError {
    #[error("YAML parsing error: {0}")]
    YamlParse(#[from] serde_yaml::Error),

    #[error("SQL parsing error: {0}")]
    SqlParse(#[from] sqlparser::parser::ParserError),

    #[error("Database error: {message}")]
    Database { message: String },

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Type conversion error: {0}")]
    TypeConversion(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),
}

pub type Result<T> = std::result::Result<T, YamlBaseError>;
