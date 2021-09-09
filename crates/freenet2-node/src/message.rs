use std::{fmt::Display, time::Duration};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ring_proto::{messages::*, Location};

/// An transaction id is a unique, universal and efficient identifier for any
/// roundtrip transaction as it is broadcasted around the F2 network.
///
/// The identifier conveys all necessary information to identify and classify the
/// transaction:
/// - The unique identifier itself.
/// - The type of transaction being performed.
///
/// A transaction may span different messages sent across the network.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy)]
pub(crate) struct TransactionId {
    id: Uuid,
    ty: MsgTypeId,
}

impl TransactionId {
    pub fn new(ty: MsgTypeId) -> TransactionId {
        // 3 word size for 64-bits platforms most likely since msg type
        // probably will be aligned to 64 bytes
        Self {
            id: Uuid::new_v4(),
            ty,
        }
    }

    /// Return the type of the message.
    pub fn msg_type(&self) -> MsgTypeId {
        self.ty
    }

    /// Returns the bytes representing the unique identifier for this message.
    pub fn unique_identifier(&self) -> &[u8; 16] {
        self.id.as_bytes()
    }
}

impl Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
pub(crate) struct MsgTypeId(u8);

impl MsgTypeId {
    /// Return all possible message type id's
    pub fn enumeration() -> [MsgTypeId; 3] {
        [
            <OpenConnection as MsgType>::msg_type_id(),
            <JoinRequest as MsgType>::msg_type_id(),
            <JoinResponse as MsgType>::msg_type_id(),
        ]
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

    macro_rules! impl_msg_conversion {
        ($ty:tt -> $id:tt) => {
            impl From<(TransactionId, $ty)> for Message {
                fn from(oc: (TransactionId, $ty)) -> Self {
                    let (tx_id, oc) = oc;
                    // assert_eq!(tx_id.msg_type(), <$ty as MsgType>::msg_type_id());
                    Self::$ty(tx_id, oc)
                }
            }

            impl SealedMsgType for $ty {
                fn msg_type_id() -> MsgTypeId {
                    MsgTypeId($id)
                }
            }
        };
    }

    impl_msg_conversion!(OpenConnection -> 0);
    impl_msg_conversion!(JoinRequest -> 1);
    impl_msg_conversion!(JoinResponse -> 2);
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum Message {
    // Ring ops
    JoinRequest(TransactionId, JoinRequest),
    JoinResponse(TransactionId, JoinResponse),
    OpenConnection(TransactionId, OpenConnection),
}

impl Message {
    fn msg_type_repr(&self) -> &'static str {
        use Message::*;
        match self {
            JoinRequest(_, _) => "JoinRequest",
            JoinResponse(_, _) => "JoinResponse",
            OpenConnection(_, _) => "OpenConnection",
        }
    }

    pub fn id(&self) -> &TransactionId {
        use Message::*;
        match self {
            JoinRequest(id, _) => id,
            JoinResponse(id, _) => id,
            OpenConnection(id, _) => id,
        }
    }

    pub fn msg_type(&self) -> MsgTypeId {
        match self {
            Self::JoinRequest(_id, _) => <JoinRequest as MsgType>::msg_type_id(),
            Self::JoinResponse(_id, _) => <JoinResponse as MsgType>::msg_type_id(),
            Self::OpenConnection(_id, _) => <OpenConnection as MsgType>::msg_type_id(),
        }
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg_type_repr())
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ProbeRequest;

#[derive(Serialize, Deserialize)]
pub(crate) struct ProbeResponse {
    pub visits: Vec<Visit>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Visit {
    pub hop: u8,
    pub latency: Duration,
    pub location: Location,
}
