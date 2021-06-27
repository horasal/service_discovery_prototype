mod discovery {
    tonic::include_proto!("moonrace.discovery");
}

use discovery::discovery_client::DiscoveryClient;
pub use discovery::entry::EntryTag;
use discovery::Entry;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{sync::watch::Receiver, task::JoinHandle};
use tonic::{transport::Uri, Status};
use tracing::{debug, error, info, instrument, warn};

#[derive(Error, Debug)]
pub enum DiscoveryCoreError {
    #[error("connection to discovery server error")]
    ConnectionError(String),
    #[error("discovery return bad status when get config")]
    BadStatusConfig(Status),
    #[error("can not register entry to discovery server")]
    RegisterError(Status),
}

#[derive(Debug)]
pub struct DiscoveryCore {
    entry_id: Arc<AtomicU64>,
    register_interval: u64,
    register_service: JoinHandle<()>,
}

impl DiscoveryCore {
    pub fn entry_id(&self) -> Arc<AtomicU64> {
        self.entry_id.clone()
    }

    pub async fn new(
        heartbeat_factor: u64,
        discovery_server_addr: &str,

        entry_name: &str,
        tag: discovery::entry::EntryTag,
        heartbeat_external_addr: &str,
        service_external_addr: &str,
        service_filter: &str,
        latest_instant: Receiver<Instant>,
    ) -> Result<Self, DiscoveryCoreError> {
        let heartbeat_factor = if heartbeat_factor < 1 {
            warn!("heartbeat factor can not be < 1, reset it to default 5.");
            5
        } else {
            heartbeat_factor
        };
        info!("connecting to discovery server.");
        let mut client = DiscoveryClient::connect(discovery_server_addr.to_string())
            .await
            .map_err(|e| DiscoveryCoreError::ConnectionError(e.to_string()))?;
        info!("connection established, fetch heartbeat config.");
        let config = client
            .get_config(discovery::Empty {})
            .await
            .map_err(|e| DiscoveryCoreError::BadStatusConfig(e))?
            .into_inner();
        let register_interval =
            config.heartbeat_interval * heartbeat_factor + config.heartbeat_timeout;
        debug!(
            "heartbeat config: interval={}, timeout={}, auto-register={}",
            config.heartbeat_interval, config.heartbeat_timeout, register_interval,
        );

        info!("initial register.");
        let entry = Entry {
            entry_name: entry_name.to_string(),
            heartbeat_url: heartbeat_external_addr.to_string(),
            service_url: service_external_addr.to_string(),
            message_filter: service_filter.to_string(),
            tag: tag as i32,
        };
        let entry_id = match client.add_entry(entry.clone()).await {
            Ok(id) => id.into_inner().entry_id,
            Err(e) => {
                warn!(
                    "initial register entry failed, will try register later, {}.",
                    e
                );
                0
            }
        };
        info!("initial entry id {}", entry_id);
        let entry_id = Arc::new(AtomicU64::new(entry_id));
        info!("spawn register service");
        let register_service = tokio::spawn(register_service(
            discovery_server_addr.to_string(),
            Arc::clone(&entry_id),
            latest_instant,
            entry,
            register_interval,
            config.heartbeat_interval,
        ));

        Ok(DiscoveryCore {
            register_interval: register_interval,
            entry_id: entry_id,
            register_service: register_service,
        })
    }
}

#[instrument]
async fn register_service(
    discovery_server_addr: String,
    entry_id: Arc<AtomicU64>,
    instant_receiver: Receiver<Instant>,
    entry_info: Entry,
    register_interval: u64,
    heartbeat_interval: u64,
) {
    let max_duration = Duration::from_millis(register_interval);
    let mut timer = tokio::time::interval(Duration::from_millis(heartbeat_interval));
    let uri: Uri = discovery_server_addr
        .parse()
        .expect("Discovery Sever uri error");
    info!("time initialized, interval={}", heartbeat_interval);
    loop {
        debug!("register loop ticked");
        if max_duration < instant_receiver.borrow().elapsed() {
            info!("no heartbeat for too long, try to register self");
            match DiscoveryClient::connect(
                tonic::transport::Channel::builder(uri.clone())
                    .timeout(Duration::from_millis(heartbeat_interval)),
            )
            .await
            {
                Ok(mut client) => match client.add_entry(entry_info.clone()).await {
                    Ok(id) => {
                        let id = id.into_inner().entry_id;
                        info!("register success, new entry id {}", id);
                        entry_id.store(id, Ordering::Release);
                    }
                    Err(e) => error!("register task: add entry failed, {}", e),
                },
                Err(e) => error!("register task: failed to reach server, {}", e),
            }
        }
        let _ = timer.tick().await;
    }
}
