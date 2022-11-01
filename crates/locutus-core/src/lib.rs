extern crate core;

pub(crate) mod client_events;
mod config;
mod contract;
mod executor;
mod message;
mod node;
mod operations;
mod ring;
pub mod util;

pub(crate) type WrappedContract = locutus_runtime::prelude::WrappedContract;
pub type WrappedState = locutus_runtime::prelude::WrappedState;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

// exports:
pub use crate::config::Config;
#[cfg(feature = "websocket")]
pub use client_events::websocket::WebSocketProxy;
pub use client_events::{
    combinator::ClientEventsCombinator, BoxedClient, ClientError, ClientEventsProxy, ClientId,
    ClientRequest, ComponentRequest, ContractRequest, ContractResponse, ErrorKind, HostResponse,
    HostResult, OpenRequest, RequestError,
};
pub use contract::{SQLiteContractHandler, SqlitePool};
pub use either;
pub use executor::Executor;
pub use libp2p;
pub use locutus_runtime;
pub use node::PeerKey;
pub use node::{InitPeerNode, NodeConfig};
pub use ring::Location;
pub use rmp;
pub use rmp_serde;
