pub(crate) mod client_events;
mod config;
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

pub type WrappedContract = crate::runtime::prelude::WrappedContract;
pub type WrappedState = crate::runtime::prelude::WrappedState;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

// exports:
pub use crate::config::Config;
pub use client_events::{
    combinator::ClientEventsCombinator, AuthToken, BoxedClient, ClientEventsProxy, ClientId,
    HostResult, OpenRequest,
};
pub use contract::storages::Storage;
pub use contract::{Executor, ExecutorError, OperationMode};
pub use either;
pub use libp2p;
pub use node::PeerKey;
pub use node::{InitPeerNode, NodeBuilder, NodeConfig};
pub use ring::Location;
