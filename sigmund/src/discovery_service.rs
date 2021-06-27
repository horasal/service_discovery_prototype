mod discovery {
    tonic::include_proto!("moonrace.discovery");
}

mod heartbeat {
    tonic::include_proto!("moonrace.heartbeat");
}

use discovery::discovery_server::Discovery;
pub use discovery::discovery_server::DiscoveryServer;
use futures::select_biased;
use sled::{Config, Db, Tree};
use std::{collections::btree_map::Entry, time::Duration};
use thiserror::Error;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, instrument, warn};

use serde_derive::{Deserialize, Serialize};
use tonic::{Response, Status};

const DISOCVERY_TREE: &str = "discovery_tree";

#[derive(Error, Clone, Debug)]
pub enum DiscoveryError {
    #[error("Database error")]
    DbError(#[from] sled::Error),
    #[error("Serialize error {0}")]
    SerializeError(String),
    #[error("data is empty")]
    EmptyData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatConfig {
    heartbeat_interval: u64,
    heartbeat_timeout: u64,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        HeartbeatConfig {
            heartbeat_timeout: 3_000,
            heartbeat_interval: 10_000,
        }
    }
}

impl HeartbeatConfig {
    #[allow(dead_code)]
    pub fn builder() -> Self {
        HeartbeatConfig::default()
    }

    #[allow(dead_code)]
    pub fn interval(mut self, interval: u64) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    #[allow(dead_code)]
    pub fn timeout(mut self, timeout: u64) -> Self {
        self.heartbeat_timeout = timeout;
        self
    }
}

#[derive(Debug)]
pub struct DiscoveryService {
    hashmap: Db,
    discovery_tree: Tree,
    heartbeat_service: JoinHandle<()>,
    heartbeat_config: HeartbeatConfig,
}

impl DiscoveryService {
    /// Setup a new discovery_service.
    ///  duration: time for heartbeat, in ms, e.g. 5000 will tick heartbeat every 5s.
    pub fn new(heartbeat_config: HeartbeatConfig) -> Result<Self, DiscoveryError> {
        let db = Config::default()
            .temporary(true)
            .use_compression(true)
            .open()
            .map_err(|e| {
                error!("can not open in-memory db, {}", e);
                DiscoveryError::DbError(e)
            })?;
        debug!("successfully opened in-memory database for discovery.");
        let discovery_tree = db.open_tree(DISOCVERY_TREE).map_err(|e| {
            error!("can not open discovery mem-tree, {}", e);
            DiscoveryError::DbError(e)
        })?;
        debug!("successfully opened in-memory tree for discovery.");
        let heartbeat = tokio::spawn(heartbeat(
            heartbeat_config.heartbeat_interval,
            heartbeat_config.heartbeat_timeout,
            discovery_tree.clone(),
        ));
        debug!("successfully spawned heartbeat thread.");
        Ok(DiscoveryService {
            hashmap: db,
            discovery_tree: discovery_tree,
            heartbeat_service: heartbeat,
            heartbeat_config: heartbeat_config,
        })
    }

    /// generate a unqiue id
    /// notice that the id will only be unqiue in current session
    /// it will restart from 0 if you restart the server.
    fn generate_entry_id(&self) -> Result<u64, DiscoveryError> {
        self.hashmap.generate_id().map_err(|e| {
            error!("unable to generate a unique id for entry, {}", e);
            DiscoveryError::DbError(e)
        })
    }

    fn add_entry(&self, e: DiscoveryEntry) -> Result<u64, DiscoveryError> {
        let data =
            bincode::serialize(&e).map_err(|e| DiscoveryError::SerializeError(e.to_string()))?;
        debug!("serialize entry");
        self.discovery_tree
            .insert(e.entry_id.to_be_bytes(), data)
            .map_err(|e| DiscoveryError::DbError(e))?;
        debug!("insert entry into database");
        Ok(e.entry_id)
    }

    fn get_entry(&self, id: u64) -> Result<DiscoveryEntry, DiscoveryError> {
        let data = self
            .discovery_tree
            .get(id.to_be_bytes())
            .map_err(|e| DiscoveryError::DbError(e))?;
        debug!("fetch entry from database");
        if let Some(data) = data {
            bincode::deserialize(&data).map_err(|e| DiscoveryError::SerializeError(e.to_string()))
        } else {
            Err(DiscoveryError::EmptyData)
        }
    }

    fn list_entry(&self) -> Result<Vec<DiscoveryEntry>, DiscoveryError> {
        debug!("listing entries..");
        Ok(self
            .discovery_tree
            .iter()
            // any error will be deleted by heartbeat, we are safe here
            .filter(|e| e.is_ok())
            .map(|e| e.unwrap())
            .map(|(_key, value)| bincode::deserialize(&value))
            .filter(|e| e.is_ok())
            .map(|e| e.unwrap())
            .collect())
    }

    fn remove_entry(&self, id: u64) -> Result<u64, DiscoveryError> {
        debug!("remove entry {}", id);
        self.discovery_tree
            .remove(id.to_be_bytes())
            .map_err(|e| DiscoveryError::DbError(e))
            .map(|_| id)
    }
}

// heartbeat module
// it ticks every duration/1000 second, and check all the entrys in entries
// heartbeat will fail after timeout/1000 second
#[instrument]
async fn heartbeat(duration: u64, timeout: u64, entries: Tree) {
    let duration = if duration == 0 {
        warn!("heartbeat timer is setted to 0, fix it to default(5s).");
        5_000
    } else {
        duration
    };
    let mut timer = tokio::time::interval(Duration::from_millis(duration));
    info!(
        "heartbeat timer started, every {} second it will be ticked.",
        duration / 1000
    );
    loop {
        debug!("heartbeat ticked.");
        // TODO: add a connection pool for this heartbeat
        for entry in entries.iter() {
            match entry {
                Ok((key, value)) => {
                    debug!("heartbeating {:?}", &key);
                    match bincode::deserialize::<DiscoveryEntry>(&value) {
                        Ok(entry) => {
                            if !do_heartbeat(entry.heartbeat_url, timeout).await {
                                debug!("{:?} heartbeat failed, remove it.", key);
                                remove_entry(&key, &entries);
                            }
                        }
                        Err(e) => {
                            warn!("entry is broken, {}.", e);
                            remove_entry(&key, &entries);
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "error happened when iter over entries, will try in next round: {}.",
                        e
                    );
                }
            }
        }
        let _ = timer.tick().await;
    }
}

#[instrument]
async fn do_heartbeat(url: String, timeout: u64) -> bool {
    use heartbeat::heart_beat_client::HeartBeatClient;
    use heartbeat::Ping;
    use tonic::transport::Endpoint;
    match Endpoint::from_shared(url).map(|e| e.timeout(Duration::from_millis(timeout))) {
        Ok(e) => match HeartBeatClient::connect(e).await {
            Ok(mut client) => match client.ping(Ping {}).await {
                Ok(_) => true,
                Err(e) => {
                    warn!("error occured {} in heartbeat", e);
                    false
                }
            },
            Err(e) => {
                warn!("can not connect to entry {}.", e);
                false
            }
        },
        Err(e) => {
            warn!("invalid url {}", e);
            false
        }
    }
}

fn remove_entry(key: &[u8], tree: &Tree) {
    if let Err(e) = tree.remove(key) {
        warn!("unable to remove entry {:?}, {}", key, e);
    } else {
        debug!("entry {:?} is removed from list", key);
    }
}

#[derive(Serialize, Deserialize)]
enum DiscoveryEntryTag {
    Service,
    Storage,
    Frontend,
    Unknown(i32),
}

#[derive(Serialize, Deserialize)]
struct DiscoveryEntry {
    entry_id: u64,
    entry_name: String,
    service_url: String,
    heartbeat_url: String,
    message_filter: String,
    tag: DiscoveryEntryTag,
}

impl Into<discovery::Entry> for DiscoveryEntry {
    fn into(self) -> discovery::Entry {
        discovery::Entry {
            entry_name: self.entry_name,
            service_url: self.service_url,
            heartbeat_url: self.heartbeat_url,
            message_filter: self.message_filter,
            tag: match self.tag {
                DiscoveryEntryTag::Frontend => discovery::entry::EntryTag::Frontend as i32,
                DiscoveryEntryTag::Service => discovery::entry::EntryTag::Service as i32,
                DiscoveryEntryTag::Storage => discovery::entry::EntryTag::Storage as i32,
                DiscoveryEntryTag::Unknown(i) => i,
            },
        }
    }
}

impl From<(u64, discovery::Entry)> for DiscoveryEntry {
    fn from((id, e): (u64, discovery::Entry)) -> Self {
        DiscoveryEntry {
            entry_id: id,
            entry_name: e.entry_name,
            service_url: e.service_url,
            heartbeat_url: e.heartbeat_url,
            message_filter: e.message_filter,
            tag: match discovery::entry::EntryTag::from_i32(e.tag) {
                Some(discovery::entry::EntryTag::Frontend) => DiscoveryEntryTag::Frontend,
                Some(discovery::entry::EntryTag::Service) => DiscoveryEntryTag::Service,
                Some(discovery::entry::EntryTag::Storage) => DiscoveryEntryTag::Storage,
                None => DiscoveryEntryTag::Unknown(e.tag),
            },
        }
    }
}

#[tonic::async_trait]
impl Discovery for DiscoveryService {
    #[instrument]
    async fn get_config(
        &self,
        _request: tonic::Request<discovery::Empty>,
    ) -> Result<tonic::Response<discovery::HeartbeatConfig>, tonic::Status> {
        Ok(tonic::Response::new(discovery::HeartbeatConfig {
            heartbeat_timeout: self.heartbeat_config.heartbeat_timeout,
            heartbeat_interval: self.heartbeat_config.heartbeat_interval,
        }))
    }

    #[instrument]
    async fn add_entry(
        &self,
        request: tonic::Request<discovery::Entry>,
    ) -> Result<tonic::Response<discovery::EntryId>, tonic::Status> {
        self.generate_entry_id()
            .and_then(|id| {
                let request = request.into_inner();
                self.add_entry((id, request).into())
            })
            .map(|id| Response::new(discovery::EntryId { entry_id: id }))
            .map_err(|e| {
                warn!("generate id error, {}.", e);
                Status::internal(e.to_string())
            })
    }

    #[instrument]
    async fn remove_entry(
        &self,
        request: tonic::Request<discovery::EntryId>,
    ) -> Result<tonic::Response<discovery::EntryId>, tonic::Status> {
        self.remove_entry(request.get_ref().entry_id)
            .map(|_id| Response::new(request.into_inner()))
            .map_err(|e| Status::internal(e.to_string()))
    }

    #[instrument]
    async fn get_entry(
        &self,
        request: tonic::Request<discovery::EntryId>,
    ) -> Result<tonic::Response<discovery::Entry>, tonic::Status> {
        self.get_entry(request.get_ref().entry_id)
            .map(|entry| Response::new(entry.into()))
            .map_err(|e| Status::internal(e.to_string()))
    }

    #[instrument]
    async fn list_entry(
        &self,
        _: tonic::Request<discovery::Empty>,
    ) -> Result<tonic::Response<discovery::EntryList>, tonic::Status> {
        self.list_entry()
            .map(|list| list.into_iter().map(|entry| entry.into()).collect())
            .map(|e| Response::new(discovery::EntryList { entries: e }))
            .map_err(|e| Status::internal(e.to_string()))
    }
}
