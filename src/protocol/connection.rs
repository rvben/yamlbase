use std::sync::Arc;
use tokio::net::TcpStream;
use tracing::error;

use crate::config::{Config, Protocol};
use crate::database::Storage;
use crate::protocol::{MySqlProtocol, PostgresProtocol};

pub struct Connection {
    config: Arc<Config>,
    storage: Arc<Storage>,
}

impl Connection {
    pub fn new(config: Arc<Config>, storage: Arc<Storage>) -> Self {
        Self { config, storage }
    }

    pub async fn handle(&self, stream: TcpStream) -> crate::Result<()> {
        match self.config.protocol {
            Protocol::Postgres => {
                let mut protocol =
                    PostgresProtocol::new(self.config.clone(), self.storage.clone()).await?;
                protocol.handle_connection(stream).await
            }
            Protocol::Mysql => {
                let protocol =
                    MySqlProtocol::new(self.config.clone(), self.storage.clone()).await?;
                protocol.handle_connection(stream).await
            }
            Protocol::Sqlserver => {
                error!("SQL Server protocol not yet implemented");
                Err(crate::YamlBaseError::NotImplemented(
                    "SQL Server protocol not yet implemented".to_string(),
                ))
            }
        }
    }
}
