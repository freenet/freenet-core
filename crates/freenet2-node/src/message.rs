use std::{fmt::Display, sync::atomic::AtomicU64, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    conn_manager::{PeerKey, PeerKeyLocation},
    ring_proto::{messages::*, Location},
};

static MESSAGE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MessageId(u64);

impl MessageId {
    pub fn new() -> MessageId {
        // FIXME: in kotling this initialized with a random value, is necessary?
        Self(MESSAGE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
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
    JoinRequest(JoinRequest),
    JoinResponse(JoinResponse),
    OpenConnection(OpenConnection),
}

impl Message {
    fn msg_type_repr(&self) -> &'static str {
        use Message::*;
        match self {
            JoinRequest(_) => "JoinRequest",
            JoinResponse(_) => "JoinResponse",
            OpenConnection(_) => "OpenConnection",
        }
    }
}

impl From<OpenConnection> for Message {
    fn from(oc: OpenConnection) -> Self {
        Self::OpenConnection(oc)
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
