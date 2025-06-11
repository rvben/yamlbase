use std::sync::Arc;
use tokio::net::TcpStream;
use tracing::error;

use crate::config::{Config, Protocol};
use crate::database::Database;
use crate::protocol::{MySqlProtocol, PostgresProtocol};

pub struct Connection {
    config: Arc<Config>,
    database: Arc<tokio::sync::RwLock<Database>>,
}

impl Connection {
    pub fn new(config: Arc<Config>, database: Arc<tokio::sync::RwLock<Database>>) -> Self {
        Self { config, database }
    }

    pub async fn handle(&self, stream: TcpStream) -> crate::Result<()> {
        match self.config.protocol {
            Protocol::Postgres => {
                let protocol = PostgresProtocol::new(self.config.clone(), self.database.clone());
                protocol.handle_connection(stream).await
            }
            Protocol::Mysql => {
                let protocol = MySqlProtocol::new(self.config.clone(), self.database.clone());
                protocol.handle_connection(stream).await
            }
            Protocol::Sqlserver => {
                error!("SQL Server protocol not yet implemented");
                Err(crate::YamlBaseError::NotImplemented(
                    "SQL Server protocol not yet implemented".to_string()
                ))
            }
        }
    }
}