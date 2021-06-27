mod discovery_service;
mod meta_service;

use std::io::Read;

use discovery_service::*;
use meta_service::*;
use serde_derive::Deserialize;
use std::error::Error;
use tonic::transport::Server;
use tracing::info;

const CONFIG_PATH: &str = "sigmund.toml";

#[derive(Deserialize, Clone, Debug)]
struct ServerConfig {
    server_url: String,
    server_port: u16,
    heartbeat: HeartbeatConfig,
}

fn setup_logger() {
    use tracing::Level;
    if cfg!(debug_assertions) {
        tracing_subscriber::fmt()
            .pretty()
            .with_max_level(Level::DEBUG)
            .init();
    } else {
        tracing_subscriber::fmt()
            .pretty()
            .with_max_level(Level::INFO)
            .init();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    setup_logger();
    let config: ServerConfig = {
        info!("loading config from file {}", CONFIG_PATH);
        let mut f = std::fs::File::open(CONFIG_PATH)?;
        let mut v = Vec::new();
        f.read_to_end(&mut v)?;
        toml::from_slice(&v)?
    };
    info!("preparing discovery service");
    let ds = DiscoveryService::new(config.heartbeat)?;
    let ds = DiscoveryServer::new(ds);
    info!("preparing metastore service");
    let ms = MetaService::new()?;
    let ms = MetastoreServer::new(ms);
    info!(
        "server startup at {}:{}.",
        config.server_url, config.server_port
    );
    Server::builder()
        .add_service(ds)
        .add_service(ms)
        .serve(format!("{}:{}", config.server_url, config.server_port).parse()?)
        .await
        .map_err(|e| Box::new(e).into())
}
