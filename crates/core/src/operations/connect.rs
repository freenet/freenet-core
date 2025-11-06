use std::borrow::Borrow;
use std::collections::HashSet;
use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::dev_tool::Location;
use crate::message::{InnerMessage, Transaction};
use crate::node::PeerId;
use crate::ring::PeerKeyLocation;
use crate::transport::TransportPublicKey;

pub(crate) use self::messages::{ConnectMsg, ConnectResponse};

mod messages {
    use super::*;

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum ConnectMsg {
        Request {
            id: Transaction,
            target: PeerKeyLocation,
            msg: ConnectRequest,
        },
        Response {
            id: Transaction,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
            msg: ConnectResponse,
        },
        Connected {
            id: Transaction,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
        },
    }

    impl InnerMessage for ConnectMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::Request { id, .. } => id,
                Self::Response { id, .. } => id,
                Self::Connected { id, .. } => id,
            }
        }

        #[allow(refining_impl_trait)]
        fn target(&self) -> Option<impl Borrow<PeerKeyLocation>> {
            use ConnectMsg::*;
            match self {
                Request { target, .. } => Some(target),
                Response { target, .. } => Some(target),
                Connected { target, .. } => Some(target),
            }
        }

        fn requested_location(&self) -> Option<Location> {
            self.target().and_then(|pkloc| pkloc.borrow().location)
        }
    }

    impl Display for ConnectMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::Request {
                    target,
                    msg: ConnectRequest::StartJoinReq { .. },
                    ..
                } => write!(f, "StartRequest(id: {id}, target: {target})"),
                Self::Request {
                    target,
                    msg:
                        ConnectRequest::CheckConnectivity {
                            sender,
                            joiner,
                            ..
                        },
                    ..
                } => write!(
                    f,
                    "CheckConnectivity(id: {id}, target: {target}, sender: {sender}, joiner: {joiner})"
                ),
                Self::Response {
                    target,
                    msg:
                        ConnectResponse::AcceptedBy {
                            accepted, acceptor, ..
                        },
                    ..
                } => write!(
                    f,
                    "AcceptedBy(id: {id}, target: {target}, accepted: {accepted}, acceptor: {acceptor})"
                ),
                Self::Connected { .. } => write!(f, "Connected(id: {id})"),
                ConnectMsg::Request { id, target, .. } => {
                    write!(f, "Request(id: {id}, target: {target})")
                }
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum ConnectRequest {
        /// A request to join a gateway.
        StartJoinReq {
            /// The peer attempting to join (set when the PeerConnection is established).
            joiner: Option<PeerId>,
            joiner_key: TransportPublicKey,
            /// Used for deterministic testing purposes. Ignored in production.
            joiner_location: Option<Location>,
            hops_to_live: usize,
            max_hops_to_live: usize,
            /// Peers we don't want to connect to directly.
            skip_connections: HashSet<PeerId>,
            /// Peers we don't want to forward connectivity messages to (avoid loops).
            skip_forwards: HashSet<PeerId>,
        },
        /// Query target should find a good candidate for joiner to join.
        FindOptimalPeer {
            query_target: PeerKeyLocation,
            ideal_location: Location,
            joiner: PeerKeyLocation,
            max_hops_to_live: usize,
            skip_connections: HashSet<PeerId>,
            skip_forwards: HashSet<PeerId>,
        },
        CheckConnectivity {
            sender: PeerKeyLocation,
            joiner: PeerKeyLocation,
            hops_to_live: usize,
            max_hops_to_live: usize,
            skip_connections: HashSet<PeerId>,
            skip_forwards: HashSet<PeerId>,
        },
        CleanConnection {
            joiner: PeerKeyLocation,
        },
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum ConnectResponse {
        AcceptedBy {
            accepted: bool,
            acceptor: PeerKeyLocation,
            joiner: PeerId,
        },
    }
}
