use std::{fmt::Display, time::Duration};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ring_proto::{messages::*, Location};
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
    fn msg_type_id() -> TransactionTypeId;
}

impl<T> TransactionType for T
where
    T: sealed_msg_type::SealedTxType,
{
    fn msg_type_id() -> TransactionTypeId {
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
        OpenConnection,
        Probe,
    }

    impl TransactionTypeId {
        pub const fn enumeration() -> [Self; 2] {
            [Self::OpenConnection, Self::Probe]
        }
    }

    macro_rules! transaction_type_enumeration {
         (decl struct { $($type:tt -> $var:tt),+ }) => {
            $( transaction_type_enumeration!(@conversion $type -> $var); )+
        };

        (@conversion $ty:tt -> $var:tt) => {
            impl From<(Transaction, $ty)> for Message {
                fn from(oc: (Transaction, $ty)) -> Self {
                    let (tx_id, oc) = oc;
                    Self::$ty(tx_id, oc)
                }
            }

            impl SealedTxType for $ty {
                fn tx_type_id() -> TransactionTypeId {
                    TransactionTypeId::$var
                }
            }
        };
    }

    transaction_type_enumeration!(decl struct {
        JoinRequest -> OpenConnection,
        JoinResponse -> OpenConnection,
        OpenConnection -> OpenConnection,
        ProbeRequest -> Probe,
        ProbeResponse -> Probe
    });
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum Message {
    // Ring
    JoinRequest(Transaction, JoinRequest),
    JoinResponse(Transaction, JoinResponse),
    OpenConnection(Transaction, OpenConnection),

    // Probe
    ProbeRequest(Transaction, ProbeRequest),
    ProbeResponse(Transaction, ProbeResponse),
}

impl Message {
    fn msg_type_repr(&self) -> &'static str {
        use Message::*;
        match self {
            JoinRequest(_, _) => "JoinRequest",
            JoinResponse(_, _) => "JoinResponse",
            OpenConnection(_, _) => "OpenConnection",
            ProbeRequest(_, _) => "ProbeRequest",
            ProbeResponse(_, _) => "ProbeResponse",
        }
    }

    pub fn id(&self) -> &Transaction {
        use Message::*;
        match self {
            JoinRequest(id, _) => id,
            JoinResponse(id, _) => id,
            OpenConnection(id, _) => id,
            ProbeRequest(id, _) => id,
            ProbeResponse(id, _) => id,
        }
    }

    pub fn msg_type(&self) -> TransactionTypeId {
        match self {
            Self::JoinRequest(_, _) => <JoinRequest as TransactionType>::msg_type_id(),
            Self::JoinResponse(_, _) => <JoinResponse as TransactionType>::msg_type_id(),
            Self::OpenConnection(_, _) => <OpenConnection as TransactionType>::msg_type_id(),
            Self::ProbeRequest(_, _) => <ProbeRequest as TransactionType>::msg_type_id(),
            Self::ProbeResponse(_, _) => <ProbeResponse as TransactionType>::msg_type_id(),
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
