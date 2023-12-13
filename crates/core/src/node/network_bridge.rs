//! Types and definitions to handle all socket communication for the peer nodes.

use std::ops::{Deref, DerefMut};

use either::Either;
use libp2p::swarm::StreamUpgradeError;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, Receiver, Sender};

use super::PeerId;
use crate::{
    client_events::ClientId,
    message::{NetMessage, NodeEvent},
};

pub(crate) mod in_memory;
pub(crate) mod inter_process;
pub(crate) mod p2p_protoc;

// TODO: use this constants when we do real net i/o
// const PING_EVERY: Duration = Duration::from_secs(30);
// const DROP_CONN_AFTER: Duration = Duration::from_secs(30 * 10);

pub(crate) type ConnResult<T> = std::result::Result<T, ConnectionError>;

/// Allows handling of connections to the network as well as sending messages
/// to other peers in the network with whom connection has been established.
#[async_trait::async_trait]
pub(crate) trait NetworkBridge: Send + Sync {
    async fn add_connection(&mut self, peer: PeerId) -> ConnResult<()>;

    async fn drop_connection(&mut self, peer: &PeerId) -> ConnResult<()>;

    async fn send(&self, target: &PeerId, msg: NetMessage) -> ConnResult<()>;
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub(crate) enum ConnectionError {
    #[error("location unknown for this node")]
    LocationUnknown,
    #[error("unable to send message")]
    SendNotCompleted,
    #[error("error while de/serializing message")]
    #[serde(skip)]
    Serialization(#[from] Option<Box<bincode::ErrorKind>>),

    // errors produced while handling the connection:
    #[error("IO error: {0}")]
    IOError(String),
    #[error("timeout error while opening a connectio.")]
    Timeout,
    #[error("no protocols could be agreed upon")]
    NegotiationFailed,
    #[error("protocol upgrade error: {0}")]
    Upgrade(String),
}

impl From<std::io::Error> for ConnectionError {
    fn from(err: std::io::Error) -> Self {
        Self::IOError(format!("{err}"))
    }
}

impl<TUpgrErr: std::error::Error> From<StreamUpgradeError<TUpgrErr>> for ConnectionError {
    fn from(err: StreamUpgradeError<TUpgrErr>) -> Self {
        match err {
            StreamUpgradeError::Timeout => Self::Timeout,
            StreamUpgradeError::Apply(err) => Self::Upgrade(format!("{err}")),
            StreamUpgradeError::NegotiationFailed => Self::NegotiationFailed,
            StreamUpgradeError::Io(err) => Self::IOError(format!("{err}")),
        }
    }
}

impl Clone for ConnectionError {
    fn clone(&self) -> Self {
        match self {
            Self::LocationUnknown => Self::LocationUnknown,
            Self::Serialization(_) => Self::Serialization(None),
            Self::SendNotCompleted => Self::SendNotCompleted,
            Self::IOError(err) => Self::IOError(err.clone()),
            Self::Timeout => Self::Timeout,
            Self::Upgrade(err) => Self::Upgrade(err.clone()),
            Self::NegotiationFailed => Self::NegotiationFailed,
        }
    }
}

pub(super) struct EventLoopNotifications(
    Receiver<Either<(NetMessage, Option<ClientId>), NodeEvent>>,
);

impl EventLoopNotifications {
    pub fn channel() -> (EventLoopNotifications, EventLoopNotificationsSender) {
        let (notification_tx, notification_rx) = mpsc::channel(100);
        (
            EventLoopNotifications(notification_rx),
            EventLoopNotificationsSender(notification_tx),
        )
    }
}

impl Deref for EventLoopNotifications {
    type Target = Receiver<Either<(NetMessage, Option<ClientId>), NodeEvent>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for EventLoopNotifications {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Clone)]
pub(crate) struct EventLoopNotificationsSender(
    Sender<Either<(NetMessage, Option<ClientId>), NodeEvent>>,
);

impl Deref for EventLoopNotificationsSender {
    type Target = Sender<Either<(NetMessage, Option<ClientId>), NodeEvent>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
