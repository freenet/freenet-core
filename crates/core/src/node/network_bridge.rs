//! Types and definitions to handle all inter-peer communication.
//!
//! This module provides the `NetworkBridge` trait, an abstraction layer for network interactions.
//! Implementations manage peer connections, message serialization, and routing.
//! It receives outgoing messages from the `Node` and `OpManager` event loops and forwards
//! incoming network messages (`NetMessage`) to the `Node`'s event loop via channels.
//!
//! See [`../../architecture.md`](../../architecture.md) for its interactions with event loops and other components.

use std::future::Future;

use either::Either;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, Receiver, Sender};

use super::PeerId;
use crate::message::{NetMessage, NodeEvent};

mod handshake;
pub(crate) mod in_memory;
pub(crate) mod p2p_protoc;

pub(crate) type ConnResult<T> = std::result::Result<T, ConnectionError>;

/// Allows handling of connections to the network as well as sending messages
/// to other peers in the network with whom connection has been established.
pub(crate) trait NetworkBridge: Send + Sync {
    fn drop_connection(&mut self, peer: &PeerId) -> impl Future<Output = ConnResult<()>> + Send;

    fn send(&self, target: &PeerId, msg: NetMessage)
        -> impl Future<Output = ConnResult<()>> + Send;
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub(crate) enum ConnectionError {
    #[error("location unknown for this node")]
    LocationUnknown,
    #[error("unable to send message")]
    SendNotCompleted(PeerId),
    #[error("Unexpected connection req")]
    UnexpectedReq,
    #[error("error while de/serializing message")]
    #[serde(skip)]
    Serialization(#[from] Option<Box<bincode::ErrorKind>>),
    #[error("{0}")]
    TransportError(String),
    #[error("failed connect")]
    FailedConnectOp,
    #[error("unwanted connection")]
    UnwantedConnection,
    #[error("connection to/from address {0} blocked by local policy")]
    AddressBlocked(std::net::SocketAddr),

    // errors produced while handling the connection:
    #[error("IO error: {0}")]
    IOError(String),
    #[error("timeout error while waiting for a message")]
    Timeout,
}

impl From<std::io::Error> for ConnectionError {
    fn from(err: std::io::Error) -> Self {
        Self::IOError(format!("{err}"))
    }
}

impl From<crate::transport::TransportError> for ConnectionError {
    fn from(err: crate::transport::TransportError) -> Self {
        Self::TransportError(err.to_string())
    }
}

impl Clone for ConnectionError {
    fn clone(&self) -> Self {
        match self {
            Self::LocationUnknown => Self::LocationUnknown,
            Self::Serialization(_) => Self::Serialization(None),
            Self::SendNotCompleted(peer) => Self::SendNotCompleted(peer.clone()),
            Self::IOError(err) => Self::IOError(err.clone()),
            Self::Timeout => Self::Timeout,
            Self::UnexpectedReq => Self::UnexpectedReq,
            Self::TransportError(err) => Self::TransportError(err.clone()),
            Self::FailedConnectOp => Self::FailedConnectOp,
            Self::UnwantedConnection => Self::UnwantedConnection,
            Self::AddressBlocked(addr) => Self::AddressBlocked(*addr),
        }
    }
}

pub(crate) fn event_loop_notification_channel(
) -> (EventLoopNotificationsReceiver, EventLoopNotificationsSender) {
    let (notification_tx, notification_rx) = mpsc::channel(100);
    let (op_execution_tx, op_execution_rx) = mpsc::channel(100);
    (
        EventLoopNotificationsReceiver {
            notifications_receiver: notification_rx,
            op_execution_receiver: op_execution_rx,
        },
        EventLoopNotificationsSender {
            notifications_sender: notification_tx,
            op_execution_sender: op_execution_tx,
        },
    )
}

pub(crate) struct EventLoopNotificationsReceiver {
    pub(crate) notifications_receiver: Receiver<Either<NetMessage, NodeEvent>>,
    pub(crate) op_execution_receiver: Receiver<(Sender<NetMessage>, NetMessage)>,
}

#[allow(dead_code)] // FIXME: enable async sub-transactions
impl EventLoopNotificationsReceiver {
    pub(crate) fn notifications_receiver(&self) -> &Receiver<Either<NetMessage, NodeEvent>> {
        &self.notifications_receiver
    }

    pub(crate) fn op_execution_receiver(&self) -> &Receiver<(Sender<NetMessage>, NetMessage)> {
        &self.op_execution_receiver
    }
}

#[derive(Clone)]
pub(crate) struct EventLoopNotificationsSender {
    pub(crate) notifications_sender: Sender<Either<NetMessage, NodeEvent>>,
    pub(crate) op_execution_sender: Sender<(Sender<NetMessage>, NetMessage)>,
}

impl EventLoopNotificationsSender {
    pub(crate) fn notifications_sender(&self) -> &Sender<Either<NetMessage, NodeEvent>> {
        &self.notifications_sender
    }

    #[allow(dead_code)] // FIXME: enable async sub-transactions
    pub(crate) fn op_execution_sender(&self) -> &Sender<(Sender<NetMessage>, NetMessage)> {
        &self.op_execution_sender
    }
}
