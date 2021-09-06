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
/// - The type of transaction.
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

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
pub(crate) struct MsgTypeId(u8);

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

    impl SealedMsgType for JoinRequest {
        fn msg_type_id() -> MsgTypeId {
            MsgTypeId(0)
        }
    }

    impl SealedMsgType for JoinResponse {
        fn msg_type_id() -> MsgTypeId {
            MsgTypeId(1)
        }
    }

    impl SealedMsgType for OpenConnection {
        fn msg_type_id() -> MsgTypeId {
            MsgTypeId(2)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
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
}

impl From<OpenConnection> for Message {
    fn from(oc: OpenConnection) -> Self {
        let msg_id = TransactionId::new(<OpenConnection as MsgType>::msg_type_id());
        Self::OpenConnection(msg_id, oc)
    }
}

impl From<JoinRequest> for Message {
    fn from(jr: JoinRequest) -> Self {
        let msg_id = TransactionId::new(<JoinRequest as MsgType>::msg_type_id());
        Self::JoinRequest(msg_id, jr)
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
