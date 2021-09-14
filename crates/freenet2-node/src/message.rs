use std::{fmt::Display, time::Duration};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ring_proto::{messages::*, Location};
pub(crate) use _seal_msg_type::MsgTypeId;

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
    ty: MsgTypeId,
    completed: bool,
}

impl Transaction {
    pub fn new(ty: MsgTypeId) -> Transaction {
        // 3 word size for 64-bits platforms most likely since msg type
        // probably will be aligned to 64 bytes
        Self {
            id: Uuid::new_v4(),
            ty,
            completed: false,
        }
    }
}

impl Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

pub(crate) trait MsgType: _seal_msg_type::SealedMsgType {
    fn msg_type_id() -> MsgTypeId;
}

impl<T> MsgType for T
where
    T: _seal_msg_type::SealedMsgType,
{
    fn msg_type_id() -> MsgTypeId {
        <Self as _seal_msg_type::SealedMsgType>::msg_type_id()
    }
}

mod _seal_msg_type {
    use super::*;

    pub(crate) trait SealedMsgType {
        fn msg_type_id() -> MsgTypeId;
    }

    macro_rules! message_enumeration {
         { [$($var:tt),+] } => {
            #[repr(u8)]
            #[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
            pub(crate) enum MsgTypeId {
                $($var,)+
            }

            impl MsgTypeId {
                pub fn enumeration() -> [Self; 5] {
                    [
                        $( Self::$var, )+
                    ]
                }
            }

            $(
                message_enumeration!(@transform $var);
            )+
        };

        (@transform $ty:tt) => {
            impl From<(Transaction, $ty)> for Message {
                fn from(oc: (Transaction, $ty)) -> Self {
                    let (tx_id, oc) = oc;
                    Self::$ty(tx_id, oc)
                }
            }

            impl SealedMsgType for $ty {
                fn msg_type_id() -> MsgTypeId {
                    MsgTypeId::$ty
                }
            }
        };
    }

    message_enumeration! { [
        OpenConnection,
        JoinRequest,
        JoinResponse,
        ProbeRequest,
        ProbeResponse
    ] }
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

    pub fn msg_type(&self) -> MsgTypeId {
        match self {
            Self::JoinRequest(_, _) => <JoinRequest as MsgType>::msg_type_id(),
            Self::JoinResponse(_, _) => <JoinResponse as MsgType>::msg_type_id(),
            Self::OpenConnection(_, _) => <OpenConnection as MsgType>::msg_type_id(),
            Self::ProbeRequest(_, _) => <ProbeRequest as MsgType>::msg_type_id(),
            Self::ProbeResponse(_, _) => <ProbeResponse as MsgType>::msg_type_id(),
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
