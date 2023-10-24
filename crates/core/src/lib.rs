pub(crate) mod client_events;
pub mod config;
mod contract;
mod message;
mod node;
mod operations;
mod resource_manager;
mod ring;
mod router;
mod runtime;
#[cfg(feature = "websocket")]
pub mod server;
pub mod util;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Exports to build a running local node.
pub mod local_node {
    use super::*;
    pub use contract::Executor;
    pub use contract::OperationMode;
    pub use node::NodeConfig;
}

/// Exports to build a running network simulation.
pub mod network_sim {
    use super::*;
    pub use client_events::{ClientEventsProxy, ClientId, OpenRequest};
    pub use node::{InitPeerNode, NodeBuilder, NodeConfig};
    pub use ring::Location;
}

/// Exports for the dev tool.
pub mod dev_tool {
    use super::*;
    pub use crate::config::Config;
    pub use client_events::{ClientEventsProxy, ClientId, OpenRequest};
    pub use contract::{storages::Storage, Executor, OperationMode};
    pub use runtime::{ContractStore, DelegateStore, Runtime, SecretsStore, StateStore};
}
