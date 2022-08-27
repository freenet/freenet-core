#![allow(dead_code)]
#![allow(unreachable_code)]
pub(crate) mod client_events;
mod config;
mod contract;
mod message;
mod node;
mod operations;
mod ring;
pub(crate) mod util;

pub(crate) type WrappedContract<'a> = locutus_runtime::prelude::WrappedContract<'a>;
pub type WrappedState = locutus_runtime::prelude::WrappedState;

// exports:
pub use crate::config::Config;
#[cfg(feature = "websocket")]
pub use client_events::websocket::WebSocketProxy;
pub use client_events::{
    combinator::ClientEventsCombinator, BoxedClient, ClientError, ClientEventsProxy, ClientId,
    ClientRequest, ErrorKind, HostResponse, HostResult, OpenRequest, RequestError,
};
pub use contract::{SQLiteContractHandler, SqlitePool};
pub use either;
pub use libp2p;
pub use node::PeerKey;
pub use node::{InitPeerNode, NodeConfig};
pub use ring::Location;
pub use rmp;
pub use rmp_serde;
