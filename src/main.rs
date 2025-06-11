use clap::Parser;
use tracing::info;
use yamlbase::{Config, Server};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments
    let config = Config::parse();

    // Initialize logging
    config.init_logging()?;

    info!("Starting YamlBase v{}", env!("CARGO_PKG_VERSION"));
    info!("Loading database from: {}", config.file.display());

    // Create and run server
    let server = Server::new(config).await?;
    server.run().await?;

    Ok(())
}
