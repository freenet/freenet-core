// Every GET wire variant dispatches to a driver —
// `op_ctx_task::start_client_get`, `start_relay_get`,
// `start_sub_op_get`, and `start_targeted_sub_op_get`. The
// wire-format types (`GetMsg`, `GetMsgResult`,
// `GetStreamingPayload`) and the originator result envelope
// (`GetResult`) survive here because the drivers and external
// crates consume them.

pub(crate) mod op_ctx_task;

use freenet_stdlib::prelude::*;

use crate::{
    contract::StoreResponse,
    message::{InnerMessage, Transaction},
    ring::Location,
};

pub(crate) use self::messages::{GetMsg, GetMsgResult, GetStreamingPayload};

#[derive(Clone, Debug)]
pub(crate) struct GetResult {
    pub state: WrappedState,
    pub contract: Option<ContractContainer>,
}

impl GetResult {
    /// Construct a `GetResult` directly from its fields. Used by the
    /// driver sub-op GET driver, which assembles the result from
    /// the wire-level terminal reply.
    pub(crate) fn new(state: WrappedState, contract: Option<ContractContainer>) -> Self {
        Self { state, contract }
    }
}

mod messages {
    use std::fmt::Display;

    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::transport::peer_connection::StreamId;

    /// Payload for streaming GET responses.
    /// Contains the result of a GET operation, serialized for streaming transfer.
    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) struct GetStreamingPayload {
        pub key: ContractKey,
        pub value: StoreResponse,
    }

    /// Result of a GET operation - either the contract was found or it wasn't.
    ///
    /// This provides explicit semantics for "contract not found" rather than
    /// requiring interpretation of empty responses or timeouts.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum GetMsgResult {
        /// Contract was found - includes full key and value
        Found {
            key: ContractKey,
            value: StoreResponse,
        },
        /// Contract was not found after exhaustive search
        NotFound,
    }

    /// GET operation messages.
    ///
    /// Uses hop-by-hop routing: each node stores `upstream_addr` from the transport layer
    /// to route responses back. No `PeerKeyLocation` is embedded in wire messages.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum GetMsg {
        /// Request to retrieve a contract. Forwarded hop-by-hop toward contract location.
        /// Uses instance_id since the requester may not have the full key yet.
        Request {
            id: Transaction,
            instance_id: ContractInstanceId,
            fetch_contract: bool,
            /// Hops to live - decremented at each hop. When 0, stop forwarding.
            htl: usize,
            /// Bloom filter tracking visited peers to prevent loops.
            visited: super::super::VisitedPeers,
            /// Whether the originator wants a subscription established on the
            /// response path. When true, relay nodes set up forwarding
            /// (upstream/downstream registration) so the subscription tree is
            /// formed by the time the response reaches the originator.
            #[serde(default)]
            subscribe: bool,
        },
        /// Response for a GET operation. Routed hop-by-hop back to originator.
        /// Uses instance_id for routing (always available from the request).
        /// The full ContractKey is only present in GetMsgResult::Found.
        Response {
            id: Transaction,
            instance_id: ContractInstanceId,
            result: GetMsgResult,
            /// Forward-path hop count: how many hops the originating Request
            /// traversed before reaching the node that produced this Response
            /// (the storer for Found, or the HTL-exhausted relay for NotFound).
            ///
            /// Computed as `max_hops_to_live - htl_at_responder`. The relay
            /// chain preserves this value as the Response bubbles back to the
            /// originator — it does NOT increment on the return path. This
            /// gives the whitepaper's "routing depth" metric (forward hops),
            /// not round-trip.
            ///
            /// `#[serde(default)]` is set for source-level clarity. Bincode
            /// does not honour serde defaults (positional encoding), so wire
            /// compat with peers that lack this field is handled at the
            /// handshake layer via `MIN_COMPATIBLE_VERSION`.
            #[serde(default)]
            hop_count: usize,
        },

        /// Streaming response for large contract data. Used when the response payload
        /// exceeds streaming_threshold (default 64KB). The actual data is sent via
        /// a separate stream identified by stream_id.
        ///
        /// This variant is only used when streaming is enabled in config.
        ResponseStreaming {
            id: Transaction,
            instance_id: ContractInstanceId,
            /// Identifies the stream carrying the response data
            stream_id: StreamId,
            /// Full contract key (known since we found the contract)
            key: ContractKey,
            /// Total size of the streamed payload in bytes
            total_size: u64,
            /// Whether the response includes the contract code (not just state)
            includes_contract: bool,
        },

        /// Acknowledgment that a streaming response was received.
        /// Sent back to the responder to confirm stream completion.
        ResponseStreamingAck {
            id: Transaction,
            stream_id: StreamId,
        },

        /// Lightweight ACK sent by a relay peer back to its upstream when it forwards
        /// a GET request to the next hop. Tells the upstream "I'm working on it" so
        /// the GC task can distinguish dead peers from slow multi-hop chains.
        /// Fire-and-forget — no response expected.
        ForwardingAck {
            id: Transaction,
            instance_id: ContractInstanceId,
        },
    }

    impl InnerMessage for GetMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::Request { id, .. }
                | Self::Response { id, .. }
                | Self::ResponseStreaming { id, .. }
                | Self::ResponseStreamingAck { id, .. }
                | Self::ForwardingAck { id, .. } => id,
            }
        }

        fn requested_location(&self) -> Option<Location> {
            match self {
                Self::Request { instance_id, .. }
                | Self::Response { instance_id, .. }
                | Self::ResponseStreaming { instance_id, .. }
                | Self::ForwardingAck { instance_id, .. } => Some(Location::from(instance_id)),
                Self::ResponseStreamingAck { .. } => {
                    // Ack doesn't carry location info - routed via stream_id
                    None
                }
            }
        }
    }

    impl Display for GetMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::Request {
                    instance_id, htl, ..
                } => {
                    write!(
                        f,
                        "Get::Request(id: {id}, instance_id: {instance_id}, htl: {htl})"
                    )
                }
                Self::Response {
                    instance_id,
                    result,
                    ..
                } => {
                    let result_str = match result {
                        GetMsgResult::Found { key, .. } => format!("Found({key})"),
                        GetMsgResult::NotFound => "NotFound".to_string(),
                    };
                    write!(
                        f,
                        "Get::Response(id: {id}, instance_id: {instance_id}, result: {result_str})"
                    )
                }
                Self::ResponseStreaming {
                    instance_id,
                    stream_id,
                    key,
                    total_size,
                    ..
                } => {
                    write!(
                        f,
                        "Get::ResponseStreaming(id: {id}, instance_id: {instance_id}, key: {key}, stream: {stream_id}, size: {total_size})"
                    )
                }
                Self::ResponseStreamingAck { stream_id, .. } => {
                    write!(
                        f,
                        "Get::ResponseStreamingAck(id: {id}, stream: {stream_id})"
                    )
                }
                Self::ForwardingAck { instance_id, .. } => {
                    write!(
                        f,
                        "Get::ForwardingAck(id: {id}, instance_id: {instance_id})"
                    )
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::wildcard_enum_match_arm)]
mod tests {
    use super::*;
    use crate::operations::VisitedPeers;
    use crate::operations::test_utils::make_contract_key;

    #[test]
    fn get_msg_id_returns_transaction() {
        let tx = Transaction::new::<GetMsg>();
        let msg = GetMsg::Request {
            id: tx,
            instance_id: *make_contract_key(1).id(),
            fetch_contract: false,
            htl: 5,
            visited: VisitedPeers::new(&tx),
            subscribe: false,
        };
        assert_eq!(*msg.id(), tx, "id() should return the transaction ID");
    }

    #[test]
    fn get_msg_display_formats_correctly() {
        let tx = Transaction::new::<GetMsg>();
        let msg = GetMsg::Request {
            id: tx,
            instance_id: *make_contract_key(1).id(),
            fetch_contract: false,
            htl: 5,
            visited: VisitedPeers::new(&tx),
            subscribe: false,
        };
        let display = format!("{}", msg);
        assert!(
            display.contains("Request"),
            "Display should contain message type name"
        );
    }

    #[test]
    fn get_msg_response_found_display_formats_correctly() {
        let tx = Transaction::new::<GetMsg>();
        let key = make_contract_key(1);
        let msg = GetMsg::Response {
            id: tx,
            instance_id: *key.id(),
            result: GetMsgResult::Found {
                key,
                value: StoreResponse {
                    state: Some(WrappedState::new(vec![1, 2, 3])),
                    contract: None,
                },
            },
            hop_count: 0,
        };
        let display = format!("{}", msg);
        assert!(
            display.contains("Response"),
            "Display should contain message type name"
        );
        assert!(
            display.contains("Found"),
            "Display should indicate Found result"
        );
    }

    #[test]
    fn get_msg_response_notfound_display_formats_correctly() {
        let tx = Transaction::new::<GetMsg>();
        let instance_id = *make_contract_key(1).id();
        let msg = GetMsg::Response {
            id: tx,
            instance_id,
            result: GetMsgResult::NotFound,
            hop_count: 0,
        };
        let display = format!("{}", msg);
        assert!(
            display.contains("Response"),
            "Display should contain message type name"
        );
        assert!(
            display.contains("NotFound"),
            "Display should indicate NotFound result"
        );
    }

    #[test]
    fn get_msg_result_found_contains_key_and_value() {
        let key = make_contract_key(1);
        let state = WrappedState::new(vec![1, 2, 3]);
        let result = GetMsgResult::Found {
            key,
            value: StoreResponse {
                state: Some(state.clone()),
                contract: None,
            },
        };
        if let GetMsgResult::Found {
            key: found_key,
            value,
        } = result
        {
            assert_eq!(found_key, key);
            assert_eq!(value.state, Some(state));
        } else {
            panic!("Expected Found variant");
        }
    }

    #[test]
    fn get_msg_result_notfound_is_unit_variant() {
        let result = GetMsgResult::NotFound;
        assert!(
            matches!(result, GetMsgResult::NotFound),
            "NotFound should match NotFound"
        );
    }

    #[test]
    fn get_msg_response_requested_location_uses_instance_id() {
        let tx = Transaction::new::<GetMsg>();
        let key = make_contract_key(1);
        let instance_id = *key.id();

        // Found variant
        let msg_found = GetMsg::Response {
            id: tx,
            instance_id,
            result: GetMsgResult::Found {
                key,
                value: StoreResponse {
                    state: Some(WrappedState::new(vec![])),
                    contract: None,
                },
            },
            hop_count: 0,
        };
        let location_found = msg_found.requested_location();
        assert!(
            location_found.is_some(),
            "Response should have a requested location"
        );
        assert_eq!(
            location_found.unwrap(),
            Location::from(&instance_id),
            "Location should be derived from instance_id"
        );

        // NotFound variant — still routed by instance_id
        let msg_notfound = GetMsg::Response {
            id: tx,
            instance_id,
            result: GetMsgResult::NotFound,
            hop_count: 0,
        };
        let location_notfound = msg_notfound.requested_location();
        assert!(
            location_notfound.is_some(),
            "NotFound Response should still have a requested location"
        );
        assert_eq!(location_notfound.unwrap(), Location::from(&instance_id));
    }

    /// Pin test: `GetMsg::ForwardingAck` survives as a wire variant +
    /// telemetry-only handler. This serde round-trip guards against
    /// accidental bincode-discriminant shifts that would break
    /// cross-version compatibility for any deployed peer still emitting
    /// the variant.
    #[test]
    fn forwarding_ack_serde_roundtrip() {
        let id = Transaction::new::<GetMsg>();
        let instance_id = ContractInstanceId::new([42; 32]);
        let msg = GetMsg::ForwardingAck { id, instance_id };

        let serialized = bincode::serialize(&msg).expect("serialize");
        let deserialized: GetMsg = bincode::deserialize(&serialized).expect("deserialize");

        match deserialized {
            GetMsg::ForwardingAck {
                id: deser_id,
                instance_id: deser_iid,
            } => {
                assert_eq!(deser_id, id);
                assert_eq!(deser_iid, instance_id);
            }
            other @ GetMsg::Request { .. }
            | other @ GetMsg::Response { .. }
            | other @ GetMsg::ResponseStreaming { .. }
            | other @ GetMsg::ResponseStreamingAck { .. } => {
                panic!("Expected ForwardingAck, got {other}")
            }
        }
    }

    /// Regression test: GetMsg::Response.hop_count roundtrips through bincode.
    ///
    /// Before this fix `hop_count` was computed at log time via
    /// `op_manager.get_current_hop(id)`, which returned `None` once the
    /// operation had been cleaned up — i.e., on the vast majority of
    /// terminal GET events.  The fix carries the value on the wire so the
    /// originator has it when constructing log events.  This test asserts
    /// that the new field survives round-trip serialisation for both
    /// `Found` and `NotFound` variants of `GetMsg::Response` — i.e., the
    /// wire format actually carries it.
    ///
    /// bincode-positional caveat: any future positional change here will
    /// break older binaries; see the MIN_COMPATIBLE_VERSION bump that
    /// accompanies this PR.
    #[test]
    fn test_get_msg_response_hop_count_roundtrip() {
        let key = make_contract_key(7);
        let cases: &[(&str, usize)] = &[
            ("zero",  0),
            ("one",   1),
            ("mid",   4),
            ("htl",   10),
            ("large", 64),
        ];
        for (label, hop_count) in cases.iter().copied() {
            // Found variant
            let found = GetMsg::Response {
                id: Transaction::new::<GetMsg>(),
                instance_id: *key.id(),
                result: GetMsgResult::Found {
                    key,
                    value: StoreResponse {
                        state: Some(WrappedState::new(vec![hop_count as u8])),
                        contract: None,
                    },
                },
                hop_count,
            };
            let bytes = bincode::serialize(&found).expect(label);
            let restored: GetMsg = bincode::deserialize(&bytes).expect(label);
            match restored {
                GetMsg::Response { hop_count: hc, .. } => assert_eq!(
                    hc, hop_count,
                    "Found.hop_count must roundtrip ({label})"
                ),
                _ => panic!("expected Response for {label}"),
            }

            // NotFound variant
            let notfound = GetMsg::Response {
                id: Transaction::new::<GetMsg>(),
                instance_id: *key.id(),
                result: GetMsgResult::NotFound,
                hop_count,
            };
            let bytes = bincode::serialize(&notfound).expect(label);
            let restored: GetMsg = bincode::deserialize(&bytes).expect(label);
            match restored {
                GetMsg::Response { hop_count: hc, .. } => assert_eq!(
                    hc, hop_count,
                    "NotFound.hop_count must roundtrip ({label})"
                ),
                _ => panic!("expected Response for {label}"),
            }
        }
    }

    /// Round-trip serialization: subscribe field is preserved through bincode.
    /// Note: bincode uses positional encoding, so #[serde(default)] does NOT
    /// provide backward compat with older binaries missing the field. Wire
    /// compat is handled by MIN_COMPATIBLE_VERSION + auto-update at handshake.
    #[test]
    fn test_get_msg_subscribe_roundtrip() {
        let msg = GetMsg::Request {
            id: Transaction::new::<GetMsg>(),
            instance_id: ContractInstanceId::new([1; 32]),
            fetch_contract: true,
            htl: 10,
            visited: VisitedPeers::default(),
            subscribe: true,
        };
        let bytes = bincode::serialize(&msg).unwrap();
        let restored: GetMsg = bincode::deserialize(&bytes).unwrap();
        match restored {
            GetMsg::Request { subscribe, .. } => assert!(subscribe),
            _ => panic!("expected Request"),
        }

        let msg_false = GetMsg::Request {
            id: Transaction::new::<GetMsg>(),
            instance_id: ContractInstanceId::new([2; 32]),
            fetch_contract: true,
            htl: 10,
            visited: VisitedPeers::default(),
            subscribe: false,
        };
        let bytes_false = bincode::serialize(&msg_false).unwrap();
        let restored_false: GetMsg = bincode::deserialize(&bytes_false).unwrap();
        match restored_false {
            GetMsg::Request { subscribe, .. } => assert!(!subscribe),
            _ => panic!("expected Request"),
        }
    }
}
