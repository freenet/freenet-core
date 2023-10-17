//! Main message type which encapsulated all the messaging between nodes.

use std::{fmt::Display, time::Duration};

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    node::{ConnectionError, PeerKey},
    operations::{
        get::GetMsg, join_ring::JoinRingMsg, put::PutMsg, subscribe::SubscribeMsg,
        update::UpdateMsg,
    },
    ring::{Location, PeerKeyLocation},
};
pub(crate) use sealed_msg_type::{TransactionType, TransactionTypeId};

/// An transaction is a unique, universal and efficient identifier for any
/// roundtrip transaction as it is broadcasted around the Freenet network.
///
/// The identifier conveys all necessary information to identify and classify the
/// transaction:
/// - The unique identifier itself.
/// - The type of transaction being performed.
/// - If the transaction has been finalized, this allows for the connection manager
///   to sweep any garbage left by a finished (or timed out) transaction.
///
/// A transaction may span different messages sent across the network.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy)]
pub(crate) struct Transaction {
    id: Ulid,
    ty: TransactionTypeId,
}

impl Transaction {
    pub fn new<T: TxType>() -> Transaction {
        let ty = <T as TxType>::tx_type_id();
        let id = Ulid::new();

        // 3 word size for 64-bits platforms
        Self { id, ty }
    }

    pub fn tx_type(&self) -> TransactionType {
        self.ty.desc()
    }
}

impl Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

/// Get the transaction type associated to a given message type.
pub(crate) trait TxType: sealed_msg_type::SealedTxType {
    fn tx_type_id() -> TransactionTypeId;
}

impl<T> TxType for T
where
    T: sealed_msg_type::SealedTxType,
{
    fn tx_type_id() -> TransactionTypeId {
        <Self as sealed_msg_type::SealedTxType>::tx_type_id()
    }
}

mod sealed_msg_type {
    use crate::operations::update::UpdateMsg;

    use super::*;

    pub(crate) trait SealedTxType {
        fn tx_type_id() -> TransactionTypeId;
    }

    #[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
    pub(crate) struct TransactionTypeId(TransactionType);

    impl TransactionTypeId {
        pub fn desc(&self) -> TransactionType {
            self.0
        }
    }

    #[repr(u8)]
    #[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
    pub(crate) enum TransactionType {
        JoinRing,
        Put,
        Get,
        Subscribe,
        Update,
        Canceled,
    }

    impl Display for TransactionType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                TransactionType::JoinRing => write!(f, "join ring"),
                TransactionType::Put => write!(f, "put"),
                TransactionType::Get => write!(f, "get"),
                TransactionType::Subscribe => write!(f, "subscribe"),
                TransactionType::Update => write!(f, "update"),
                TransactionType::Canceled => write!(f, "canceled"),
            }
        }
    }

    macro_rules! transaction_type_enumeration {
        (decl struct { $( $var:tt -> $ty:tt),+ }) => {
            $(
                impl From<$ty> for Message {
                    fn from(msg: $ty) -> Self {
                        Self::$var(msg)
                    }
                }

                impl SealedTxType for $ty {
                    fn tx_type_id() -> TransactionTypeId {
                        TransactionTypeId(TransactionType::$var)
                    }
                }
            )+
        };
    }

    transaction_type_enumeration!(decl struct {
        JoinRing -> JoinRingMsg,
        Put -> PutMsg,
        Get -> GetMsg,
        Subscribe -> SubscribeMsg,
        Update -> UpdateMsg
    });
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum Message {
    JoinRing(JoinRingMsg),
    Put(PutMsg),
    Get(GetMsg),
    Subscribe(SubscribeMsg),
    Update(UpdateMsg),
    /// Failed a transaction, informing of cancellation.
    Canceled(Transaction),
}

pub(crate) trait InnerMessage: Into<Message> {
    fn id(&self) -> &Transaction;
}

/// Internal node events emitted to the event loop.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum NodeEvent {
    /// For unspecified reasons the node is gracefully shutting down.
    ShutdownNode,
    /// Received a confirmation from a peer that a physical connection was established.
    ConfirmedInbound,
    /// Drop the given peer connection.
    DropConnection(PeerKey),
    /// Accept the connections from the given peer.
    AcceptConnection(PeerKey),
    /// Error while sending a message by the connection bridge from within the ops.
    #[serde(skip)]
    Error(ConnectionError),
}

impl Display for NodeEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeEvent::ShutdownNode => f.write_str("ShutdownNode"),
            NodeEvent::ConfirmedInbound => f.write_str("ConfirmedInbound"),
            NodeEvent::DropConnection(peer) => {
                f.write_str(&format!("DropConnection (from {peer})"))
            }
            NodeEvent::AcceptConnection(peer) => {
                f.write_str(&format!("AcceptConnection (from {peer})"))
            }
            NodeEvent::Error(err) => f.write_str(&format!("{err}")),
        }
    }
}

impl Message {
    pub fn id(&self) -> &Transaction {
        use Message::*;
        match self {
            JoinRing(op) => op.id(),
            Put(op) => op.id(),
            Get(op) => op.id(),
            Subscribe(op) => op.id(),
            Update(_op) => todo!(),
            Canceled(tx) => tx,
        }
    }

    pub fn target(&self) -> Option<&PeerKeyLocation> {
        use Message::*;
        match self {
            JoinRing(op) => op.target(),
            Put(op) => op.target(),
            Get(op) => op.target(),
            Subscribe(op) => op.target(),
            Update(_op) => todo!(),
            Canceled(_) => None,
        }
    }

    /// Is the last expected message for this chain of messages.
    pub fn terminal(&self) -> bool {
        use Message::*;
        match self {
            JoinRing(op) => op.terminal(),
            Put(op) => op.terminal(),
            Get(op) => op.terminal(),
            Subscribe(op) => op.terminal(),
            Update(_op) => todo!(),
            Canceled(_) => true,
        }
    }

    pub fn track_stats(&self) -> bool {
        use Message::*;
        !matches!(self, JoinRing(_) | Subscribe(_) | Canceled(_))
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Message::*;
        write!(f, "Message {{")?;
        match self {
            JoinRing(msg) => msg.fmt(f)?,
            Put(msg) => msg.fmt(f)?,
            Get(msg) => msg.fmt(f)?,
            Subscribe(msg) => msg.fmt(f)?,
            Update(_op) => todo!(),
            Canceled(msg) => msg.fmt(f)?,
        };
        write!(f, "}}")
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct ProbeRequest {
    pub hops_to_live: u8,
    pub target: Location,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct ProbeResponse {
    pub visits: Vec<Visit>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct Visit {
    pub hop: u8,
    pub latency: Duration,
    pub location: Location,
}
