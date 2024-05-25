/// Clients events related logic and type definitions.
pub(crate) mod client_events;
/// Peer node configuration.
pub mod config;
/// Handling of contracts and delegates functionality.
mod contract;
/// Generated messages from the flatbuffers schema for the network monitor.
pub mod generated;
/// Network messages for transactions.
mod message;
/// Node configuration, implementations and execution (entry points for the binaries).
mod node;
/// Network operation/transaction state machines.
mod operations;
/// Ring connections and routing.
mod ring;
/// Router implementation.
mod router;
/// Local server used to communicate with the peer core.
#[cfg(feature = "websocket")]
pub mod server;
/// Local network topology management.
mod topology;
/// Tracing and loging infrastructure. Includes our custom event log register. Tracing collectors, etc.
mod tracing;
/// Code for communicating with other peers over UDP, handles hole-punching, error handling, etc.
mod transport;
pub mod util;
/// WASM code execution runtime, tailored for the contract and delegate APIs.
mod wasm_runtime;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Exports to build a running local node.
pub mod local_node {
    use super::*;
    pub use contract::Executor;
    pub use contract::OperationMode;
}

/// Exports for the dev tool.
pub mod dev_tool {
    use super::*;
    pub use crate::config::Config;
    pub use client_events::{
        test::MemoryEventsGen, test::NetworkEventGenerator, ClientEventsProxy, ClientId,
        OpenRequest,
    };
    pub use contract::{storages::Storage, Executor, OperationMode};
    pub use flatbuffers;
    pub use message::Transaction;
    pub use node::{
        testing_impl::{EventChain, NetworkPeer, NodeLabel, PeerMessage, PeerStatus, SimNetwork},
        InitPeerNode, InterProcessConnManager, NodeConfig, PeerId,
    };
    pub use ring::Location;
    pub use transport::TransportKeypair;
    pub use wasm_runtime::{ContractStore, DelegateStore, Runtime, SecretsStore, StateStore};
}

#[cfg(test)]
pub mod test_utils;
