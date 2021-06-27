mod heartbeat {
    tonic::include_proto!("moonrace.heartbeat");
}
use std::{
    net::{AddrParseError, SocketAddr},
    time::Instant,
};
use tokio::{
    sync::watch::{channel, Receiver, Sender},
    task::JoinHandle,
};

use futures::TryFutureExt;
use heartbeat::heart_beat_server::{HeartBeat, HeartBeatServer};
use heartbeat::{Ping, Pong};
use thiserror::Error;
use tonic::Status;
use tracing::{error, info, instrument, warn};

#[derive(Error, Clone, Debug)]
pub enum HeartbeatCoreError {
    #[error("discovery client connection error, {0}")]
    DiscoveryClientConnectionError(String),
    #[error("receive bad status when receiving heartbeat config {0}")]
    HeartbeatConfigBadStatus(Status),
    #[error("incorrect service url")]
    IncorrectServiceUrl(#[from] AddrParseError),
    #[error("heart beat service error")]
    HeartbeatServiceError(String),
    #[error("serve on heartbeat error")]
    ServingError(String),
}

#[derive(Debug, Clone, Default)]
pub struct HeartbeatCoreBuilder {
    service_addr: String,
}

impl HeartbeatCoreBuilder {
    pub fn builder() -> Self {
        Self::default()
    }

    pub fn service_url(mut self, url: &str) -> Self {
        self.service_addr = url.to_string();
        self
    }

    pub fn build(self) -> Result<HeartbeatCore, HeartbeatCoreError> {
        HeartbeatCore::new(&self.service_addr)
    }
}

#[derive(Debug)]
pub struct HeartbeatCore {
    last_heartbeat_instant: Receiver<Instant>,
    pub heartbeat: JoinHandle<Result<(), HeartbeatCoreError>>,
}

impl HeartbeatCore {
    pub fn new(
        // heartbeat service addr, must be ipaddr:port
        heartbeat_service_addr: &str,
    ) -> Result<Self, HeartbeatCoreError> {
        info!("prepare heartbeat instant channel.");
        let (sender, receiver) = channel(Instant::now());
        let addr: SocketAddr = heartbeat_service_addr
            .parse()
            .map_err(|e| HeartbeatCoreError::IncorrectServiceUrl(e))?;
        info!("start heartbeat service.");
        let heartbeat_service = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(HeartBeatServer::new(HeartbeatWorker::new(sender)))
                .serve(addr)
                .map_err(|e| HeartbeatCoreError::HeartbeatServiceError(e.to_string())),
        );
        Ok(HeartbeatCore {
            last_heartbeat_instant: receiver,
            heartbeat: heartbeat_service,
        })
    }

    pub fn instant_receiver(&self) -> Receiver<Instant> {
        self.last_heartbeat_instant.clone()
    }

    pub async fn wait(self) -> Result<(), HeartbeatCoreError> {
        match self
            .heartbeat
            .await
            .map_err(|e| HeartbeatCoreError::ServingError(e.to_string()))
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(e),
        }
    }
}

#[derive(Debug)]
struct HeartbeatWorker {
    sender: Sender<Instant>,
}

impl HeartbeatWorker {
    fn new(sender: Sender<Instant>) -> Self {
        HeartbeatWorker { sender: sender }
    }
}

#[tonic::async_trait]
impl HeartBeat for HeartbeatWorker {
    #[instrument]
    async fn ping(&self, _request: tonic::Request<Ping>) -> Result<tonic::Response<Pong>, Status> {
        info!("heartbeat from server.");
        if let Err(e) = self.sender.send(Instant::now()) {
            warn!(
                "can not record current heartbeat time, will try in next heartbeat, {}",
                e
            );
        }
        Ok(tonic::Response::new(Pong {}))
    }
}
