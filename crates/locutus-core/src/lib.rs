pub(crate) mod client_events;
mod config;
mod contract;
mod executor;
mod message;
mod node;
mod operations;
mod resource_manager;
mod ring;
mod router;
pub mod util;

pub type WrappedContract = locutus_runtime::prelude::WrappedContract;
pub type WrappedState = locutus_runtime::prelude::WrappedState;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

// exports:
pub use crate::config::Config;
#[cfg(feature = "websocket")]
pub use client_events::websocket::WebSocketProxy;
pub use client_events::{
    combinator::ClientEventsCombinator, AuthToken, BoxedClient, ClientEventsProxy, ClientId,
    HostResult, OpenRequest,
};
pub use contract::storages::{Storage, StorageContractHandler};
pub use either;
pub use executor::{Executor, OperationMode};
pub use libp2p;
pub use locutus_runtime;
pub use node::PeerKey;
pub use node::{InitPeerNode, NodeConfig};
pub use ring::Location;
