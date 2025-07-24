use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::config::Config;
use crate::database::Storage;
use crate::yaml::{FileWatcher, parse_yaml_database};

mod connection_manager;
pub use connection_manager::{ConnectionManager, ConnectionStats};

#[cfg(test)]
mod tests;

pub struct Server {
    config: Arc<Config>,
    storage: Storage,
}

impl Server {
    pub async fn new(mut config: Config) -> crate::Result<Self> {
        // Parse initial database
        let (database, auth_config) = parse_yaml_database(&config.file).await?;

        // If auth is specified in YAML, override command line args
        if let Some(auth) = auth_config {
            info!(
                "Using authentication from YAML file: username={}",
                auth.username
            );
            config.username = auth.username;
            config.password = auth.password;
        } else {
            info!("Using default authentication: username={}", config.username);
        }

        let config = Arc::new(config);
        let storage = Storage::new(database);

        Ok(Self { config, storage })
    }

    pub async fn run(self) -> crate::Result<()> {
        let addr = format!(
            "{}:{}",
            self.config.bind_address,
            self.config.effective_port()
        );
        info!("Starting YamlBase server on {}", addr);

        // Set up hot reload if enabled
        if self.config.hot_reload {
            self.setup_hot_reload()?;
        }

        // Create connection manager for stable connection handling
        let connection_manager = ConnectionManager::new(
            self.config.clone(),
            Arc::new(self.storage.clone())
        );

        // Start background monitoring for connection stability
        let _monitoring_handle = connection_manager.start_monitoring();

        // Start listening
        let listener = TcpListener::bind(&addr).await?;
        info!("Server listening on {} with connection stability features", addr);

        // Accept connections with enhanced stability handling
        loop {
            let (stream, client_addr) = listener.accept().await?;
            let client_addr_str = client_addr.to_string();
            info!("New connection from {}", client_addr_str);

            let manager = connection_manager.clone();
            tokio::spawn(async move {
                if let Err(e) = manager.handle_connection(stream, client_addr_str.clone()).await {
                    error!("Connection error from {}: {}", client_addr_str, e);
                }
            });
        }
    }

    fn setup_hot_reload(&self) -> crate::Result<()> {
        let (watcher, mut rx) = FileWatcher::new(self.config.file.clone());
        watcher
            .start()
            .map_err(|e| crate::YamlBaseError::Io(std::io::Error::other(e)))?;

        let storage = self.storage.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            while let Some(()) = rx.recv().await {
                info!("Reloading database from file");
                match parse_yaml_database(&config.file).await {
                    Ok((new_db, _auth)) => {
                        // Note: We don't update auth on hot reload for security reasons
                        // Auth changes require a server restart
                        let db_arc = storage.database();
                        let mut db = db_arc.write().await;
                        *db = new_db;
                        drop(db);
                        storage.rebuild_indexes().await;
                        info!("Database reloaded successfully");
                    }
                    Err(e) => {
                        error!("Failed to reload database: {}", e);
                    }
                }
            }
        });

        Ok(())
    }
}
