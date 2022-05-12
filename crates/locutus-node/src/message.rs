//! Main message type which encapsulated all the messaging between nodes.

use std::{
    fmt::Display,
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};
use uuid::{
    v1::{Context, Timestamp},
    Uuid,
};

use crate::{
    node::{ConnectionError, PeerKey},
    operations::{get::GetMsg, join_ring::JoinRingMsg, put::PutMsg, subscribe::SubscribeMsg},
    ring::{Location, PeerKeyLocation},
};
pub(crate) use sealed_msg_type::{TransactionType, TransactionTypeId};

/// An transaction is a unique, universal and efficient identifier for any
/// roundtrip transaction as it is broadcasted around the F2 network.
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
    /// UUID V1, can retrieve timestamp information later to check for possible out of time
    /// expired transactions which have been clean up already.
    id: Uuid,
    ty: TransactionTypeId,
}

static UUID_CONTEXT: Context = Context::new(14);

impl Transaction {
    pub fn new(ty: TransactionTypeId, initial_peer: &PeerKey) -> Transaction {
        // using v1 UUID to keep to keep track of the creation ts
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("infallible");
        let now_secs = now.as_secs();
        let now_nanos = now.as_nanos();
        let now_nanos = now_nanos - (now_secs as u128 * 1_000_000_000);
        let ts = Timestamp::from_unix(&UUID_CONTEXT, now_secs, now_nanos as u32);

        // event in the net this UUID should be unique since peer keys are unique
        // however some id collision may be theoretically possible if two transactions
        // are created at the same exact time and the first 6 bytes of the key coincide;
        // in practice the chance of this happening is astronomically low

        let b = &mut [0; 6];
        b.copy_from_slice(&initial_peer.to_bytes()[0..6]);
        let id = Uuid::new_v1(ts, b);
        // 2 word size for 64-bits platforms most likely since msg type
        // probably will be aligned to 64 bytes
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
        Canceled,
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
        Subscribe -> SubscribeMsg
    });
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum Message {
    JoinRing(JoinRingMsg),
    Put(PutMsg),
    Get(GetMsg),
    Subscribe(SubscribeMsg),
    /// Failed a transaction, informing of cancellation.
    Canceled(Transaction),
}

pub(crate) trait InnerMessage {
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
            Canceled(_) => true,
        }
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
