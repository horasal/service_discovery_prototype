mod heartbeat_core;
pub use heartbeat_core::{HeartbeatCore, HeartbeatCoreBuilder, HeartbeatCoreError};

mod discovery_client;
pub use discovery_client::EntryTag;
pub use discovery_client::{DiscoveryCore, DiscoveryCoreError};
