mod metastore {
    tonic::include_proto!("moonrace.metastore");
}

use metastore::metastore_server::Metastore;
pub use metastore::metastore_server::MetastoreServer;
use serde_derive::Deserialize;
use sled::{Config, Db, Tree};
use std::io::Read;
use thiserror::Error;
use tonic::{Response, Status};
use tracing::{debug, error, info, warn};

const META_TREE: &str = "meta_tree";
const STATIC_META: &str = "meta";

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("database error")]
    DbError(#[from] sled::Error),
}

pub struct MetaService {
    #[allow(dead_code)]
    hashmap: Db,
    meta_tree: Tree,
}

impl MetaService {
    pub fn new() -> Result<Self, MetadataError> {
        let db = Config::default()
            .temporary(true)
            .use_compression(true)
            .open()
            .map_err(|e| {
                error!("can not open in-memory db, {}", e);
                MetadataError::DbError(e)
            })?;
        let meta_tree = db.open_tree(META_TREE).map_err(|e| {
            error!("can not open discovery mem-tree, {}", e);
            MetadataError::DbError(e)
        })?;
        info!("loading static metadata");
        visit_dir(STATIC_META, meta_tree.clone());

        Ok(MetaService {
            hashmap: db,
            meta_tree: meta_tree,
        })
    }
}

#[derive(Deserialize)]
struct MetaEntry {
    key: String,
    value: String,
}

fn visit_dir<T: AsRef<std::path::Path>>(dir: T, storage: Tree) {
    if dir.as_ref().is_dir() {
        debug!("scanning dir");
        match dir.as_ref().read_dir() {
            Ok(list) => {
                for entry in list.filter(|e| {
                    if let Ok(e) = e.as_ref() {
                        e.file_name()
                            .to_string_lossy()
                            .to_ascii_lowercase()
                            .ends_with(".toml")
                    } else {
                        true
                    }
                }) {
                    if let Err(e) = entry
                        .map(|dir| dir.path())
                        .map_err(|e| e.to_string())
                        .and_then(|pat| {
                            info!(
                                "found static metadata {}",
                                pat.to_str().unwrap_or("UNKNOWN")
                            );
                            let mut v = Vec::new();
                            std::fs::File::open(pat)
                                .and_then(|mut f| f.read_to_end(&mut v))
                                .map_err(|e| e.to_string())?;
                            Ok(v)
                        })
                        .and_then(|buf| {
                            toml::from_slice::<MetaEntry>(&buf).map_err(|e| e.to_string())
                        })
                        .and_then(|meta| {
                            storage
                                .insert(meta.key.as_bytes(), meta.value.as_bytes())
                                .map_err(|e| e.to_string())
                        })
                    {
                        warn!("one of entries listing fails, {}", e);
                    }
                }
            }
            Err(e) => {
                warn!("can not list static meta files, {}", e);
            }
        }
    }
}

#[tonic::async_trait]
impl Metastore for MetaService {
    async fn put_metadata(
        &self,
        request: tonic::Request<metastore::Metadata>,
    ) -> Result<tonic::Response<metastore::Key>, tonic::Status> {
        let req = request.into_inner();
        match (req.key, req.value) {
            (Some(key), Some(value)) => self
                .meta_tree
                .insert(key.key, value.value)
                .map(|v| {
                    Response::new(metastore::Key {
                        key: v.map(|b| b.to_vec()).unwrap_or(Vec::new()),
                    })
                })
                .map_err(|e| Status::internal(e.to_string())),
            _ => Err(Status::invalid_argument("key or value is empty")),
        }
    }

    async fn get_metadata(
        &self,
        request: tonic::Request<metastore::Key>,
    ) -> Result<tonic::Response<metastore::Metadata>, tonic::Status> {
        let key = request.into_inner().key;
        self.meta_tree
            .get(&key)
            .map_err(|e| Status::internal(e.to_string()))
            .map(|e| metastore::Metadata {
                key: Some(metastore::Key { key: key }),
                value: e.map(|b| metastore::Value { value: b.to_vec() }),
            })
            .map(|e| Response::new(e))
    }

    async fn remove_metadata(
        &self,
        request: tonic::Request<metastore::Key>,
    ) -> Result<tonic::Response<metastore::Value>, tonic::Status> {
        let key = request.into_inner().key;
        self.meta_tree
            .remove(&key)
            .map_err(|e| Status::internal(e.to_string()))
            .map(|e| metastore::Value {
                value: e.map(|b| b.to_vec()).unwrap_or(Vec::new()),
            })
            .map(|e| Response::new(e))
    }

    async fn list_metadata(
        &self,
        _: tonic::Request<metastore::Empty>,
    ) -> Result<tonic::Response<metastore::KeyList>, tonic::Status> {
        Ok(Response::new(metastore::KeyList {
            keys: self
                .meta_tree
                .iter()
                .filter(|e| e.is_ok())
                .map(|entry| entry.unwrap())
                .map(|(k, _)| metastore::Key { key: k.to_vec() })
                .collect(),
        }))
    }
}
