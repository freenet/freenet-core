pub(crate) mod client_events;
pub mod config;
mod contract;
pub mod generated;
mod message;
mod node;
mod operations;
mod resources;
mod ring;
mod router;
mod runtime;
#[cfg(feature = "websocket")]
pub mod server;
mod topology;
mod tracing;
pub mod util;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Exports to build a running local node.
pub mod local_node {
    use super::*;
    pub use contract::Executor;
    pub use contract::OperationMode;
    pub use node::PeerCliConfig;
}

/// Exports for the dev tool.
pub mod dev_tool {
    use super::*;
    pub use crate::config::Config;
    pub use client_events::{test::MemoryEventsGen, ClientEventsProxy, ClientId, OpenRequest};
    pub use contract::{storages::Storage, Executor, OperationMode};
    pub use flatbuffers;
    pub use node::{
        testing_impl::{EventChain, NodeLabel, SimNetwork, SimPeer},
        InitPeerNode, InterProcessConnManager, NodeConfig, PeerCliConfig, PeerId,
    };
    pub use ring::Location;
    pub use runtime::{ContractStore, DelegateStore, Runtime, SecretsStore, StateStore};
}
