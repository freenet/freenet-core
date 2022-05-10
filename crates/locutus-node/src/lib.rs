pub(crate) mod client_events;
mod config;
mod contract;
// pub(crate) mod client_interfaces;
mod message;
mod node;
mod operations;
mod ring;
pub(crate) mod util;

pub(crate) type WrappedContract<'a> = locutus_runtime::prelude::WrappedContract<'a>;
pub(crate) type WrappedState = locutus_runtime::prelude::WrappedState;

// exports:
pub use crate::config::Config;
#[cfg(feature = "websocket")]
pub use client_events::websocket::WebSocketProxy;
pub use client_events::{
    combinator::ClientEventsCombinator, BoxedClient, ClientError, ClientEventsProxy, ClientId,
    ClientRequest, ErrorKind, HostResponse,
};
pub use node::{InitPeerNode, NodeConfig};
pub use ring::Location;
