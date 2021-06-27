use sigmund_client::*;
use std::{sync::atomic::Ordering, time::Duration};
use tracing::{info, Level};

struct DummyClient {
    hb_core: HeartbeatCore,
    #[allow(dead_code)]
    entry_core: DiscoveryCore,
}

impl DummyClient {
    async fn new() -> Self {
        let hb_core = HeartbeatCore::new("127.0.0.1:8001").expect("can not build hbcore");
        let instant = hb_core.instant_receiver();
        let entry_core = DiscoveryCore::new(
            5,
            "http://127.0.0.1:8000",
            "test_client",
            EntryTag::Service,
            "http://127.0.0.1:8001",
            "http://127.0.0.1:8001",
            "",
            instant,
        )
        .await
        .expect("can not build discovery core");
        let id = entry_core.entry_id();
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_secs(5));
            loop {
                let _ = timer.tick().await;
                info!("current entryid is {}.", id.load(Ordering::Relaxed));
            }
        });
        DummyClient {
            hb_core: hb_core,
            entry_core: entry_core,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .pretty()
        .with_max_level(Level::INFO)
        .init();
    DummyClient::new()
        .await
        .hb_core
        .wait()
        .await
        .map_err(|e| Box::new(e).into())
}
