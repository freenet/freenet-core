//! Main message type which encapsulated all the messaging between nodes.

use std::{fmt::Display, time::Duration};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    conn_manager::PeerKeyLocation,
    operations::{get::GetMsg, join_ring::JoinRingMsg, put::PutMsg},
    ring::Location,
};
pub(crate) use sealed_msg_type::TransactionTypeId;

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
    id: Uuid,
    ty: TransactionTypeId,
    completed: bool,
}

impl Transaction {
    pub fn new(ty: TransactionTypeId) -> Transaction {
        // 3 word size for 64-bits platforms most likely since msg type
        // probably will be aligned to 64 bytes
        Self {
            id: Uuid::new_v4(),
            ty,
            completed: false,
        }
    }

    pub fn tx_type(&self) -> TransactionTypeId {
        self.ty
    }
}

impl Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

/// Get the transaction type associated to a given message type.
pub(crate) trait TransactionType: sealed_msg_type::SealedTxType {
    fn tx_type_id() -> TransactionTypeId;
}

impl<T> TransactionType for T
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

    #[repr(u8)]
    #[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
    pub(crate) enum TransactionTypeId {
        JoinRing,
        Put,
        Get,
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
                        TransactionTypeId::$var
                    }
                }
            )+
        };
    }

    transaction_type_enumeration!(decl struct {
        JoinRing -> JoinRingMsg,
        Put -> PutMsg,
        Get -> GetMsg,
        Canceled -> Transaction
    });
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum Message {
    JoinRing(JoinRingMsg),
    Put(PutMsg),
    Get(GetMsg),
    /// Failed a transaction, informing of cancellation.
    Canceled(Transaction),
}

impl Message {
    fn msg_type_repr(&self) -> &'static str {
        todo!()
    }

    pub fn id(&self) -> &Transaction {
        use Message::*;
        match self {
            JoinRing(op) => op.id(),
            Put(op) => op.id(),
            Get(op) => op.id(),
            Canceled(tx) => tx,
        }
    }

    pub fn sender(&self) -> Option<&PeerKeyLocation> {
        use Message::*;
        match self {
            JoinRing(op) => op.sender(),
            Put(op) => op.sender(),
            Get(op) => op.sender(),
            Canceled(_) => None,
        }
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg_type_repr())
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
