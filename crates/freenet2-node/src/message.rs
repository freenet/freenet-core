use std::{fmt::Display, sync::atomic::AtomicU64, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    conn_manager::{PeerKey, PeerKeyLocation},
    ring_proto::Location,
};

static MESSAGE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MessageId(u64);

impl MessageId {
    fn new() -> MessageId {
        // FIXME: in kotling this initialized with a random value, is necessary?
        Self(MESSAGE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Message {
    JoinRequest(JoinRequest),
    JoinResponse(JoinResponse),
}

impl Message {
    pub fn msg_type(&self) -> &str {
        use Message::*;
        match self {
            JoinRequest(_) => "JoinRequest",
            JoinResponse(_) => "JoinResponse",
        }
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg_type())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum JoinRequest {
    Initial { key: PeerKey },
    Proxy { joiner: PeerKeyLocation },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum JoinResponse {
    Initial {
        accepted: Vec<PeerKeyLocation>,
        reply_to: MessageId,
        your_location: Location,
    },
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
