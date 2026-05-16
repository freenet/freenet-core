//! A contract is PUT within a location distance: all nodes within a
//! given radius cache a copy of the contract and its current value,
//! and broadcast updates to subscribers.
//!
//! Every PUT wire variant dispatches to a driver —
//! `op_ctx_task::start_client_put`, `start_relay_put`, and
//! `start_relay_put_streaming`. The wire-format types
//! (`PutMsg`, `PutStreamingPayload`), the originator finalization
//! helpers, and `put_contract` survive here because the drivers
//! consume them.

pub(crate) mod op_ctx_task;

pub(crate) use self::messages::{PutMsg, PutStreamingPayload};
use freenet_stdlib::prelude::*;

use super::OpError;
use crate::{
    contract::ContractHandlerEvent, message::Transaction, node::OpManager, ring::PeerKeyLocation,
    tracing::NetEventLog,
};
use either::Either;

/// Telemetry data for originator-side PUT finalization.
pub(super) struct PutFinalizationData {
    pub sender: PeerKeyLocation,
    pub hop_count: Option<usize>,
    pub state_hash: Option<String>,
    pub state_size: Option<usize>,
}

/// Originator-side finalization after a PUT has been accepted by the network.
///
/// Emits `put_success` telemetry and, if `subscribe` is true,
/// starts a post-PUT subscription.
pub(super) async fn finalize_put_at_originator(
    op_manager: &OpManager,
    id: Transaction,
    key: ContractKey,
    telemetry: PutFinalizationData,
    subscribe: bool,
    blocking_subscribe: bool,
) {
    if let Some(event) = NetEventLog::put_success(
        &id,
        &op_manager.ring,
        key,
        telemetry.sender,
        telemetry.hop_count,
        telemetry.state_hash,
        telemetry.state_size,
    ) {
        op_manager.ring.register_events(Either::Left(event)).await;
    }

    // Mark the contract as locally-accessed now that the originator's PUT
    // has succeeded and the local cache entry exists (created by the
    // shared `relay_put_store_locally` path during `put_contract` +
    // `host_contract`). Without this, self-hosted contracts that were
    // PUT'd by a local client but never GET'd would never get the
    // `local_client_access` flag — the GET path is the only other call
    // site for `mark_local_client_access`. Missing the flag excludes the
    // contract from `contracts_needing_renewal`, the subscription expires,
    // the entry eventually gets evicted under byte-budget pressure, and
    // the next cold remote GET fails the `is_locally_hosted` shortcut and
    // routes to the network — where, for a contract no other peer is
    // subscribed to, the GetOp hangs until the WS client times out.
    // (freenet-stdlib mirror demo, 2026-05-14: 180s timeouts on
    // freenet:96rknpy1GYhZ/freenet-stdlib for exactly this reason.)
    //
    // This is the originator-side mark the relay-helper doc-comment in
    // `op_ctx_task::relay_put_store_locally` was waiting for.
    op_manager.ring.mark_local_client_access(&key);

    if subscribe {
        start_subscription_after_put(op_manager, id, key, blocking_subscribe).await;
    }
}

/// The `blocking_subscription` parameter controls subscription behavior:
/// - When false (default): subscription completes asynchronously and PUT response
///   is sent immediately
/// - When true: PUT response waits for subscription to complete
///
/// This value comes from the client request's `blocking_subscribe` field
/// (`ContractRequest::Put`).
async fn start_subscription_after_put(
    op_manager: &OpManager,
    parent_tx: Transaction,
    key: ContractKey,
    blocking_subscription: bool,
) {
    let child_tx =
        super::start_subscription_request(op_manager, parent_tx, key, blocking_subscription);
    tracing::debug!(
        tx = %parent_tx,
        child_tx = %child_tx,
        contract = %key,
        blocking = blocking_subscription,
        phase = "subscribe",
        "Started subscription after PUT"
    );
}

/// Stores the contract state and returns (new_state, state_changed).
/// `state_changed` is true if the stored state was actually modified
/// (old state != new state), which is needed to trigger UPDATE propagation.
pub(super) async fn put_contract(
    op_manager: &OpManager,
    key: ContractKey,
    state: WrappedState,
    related_contracts: RelatedContracts<'static>,
    contract: &ContractContainer,
) -> Result<(WrappedState, bool), OpError> {
    match op_manager
        .notify_contract_handler(ContractHandlerEvent::PutQuery {
            key,
            state,
            related_contracts,
            contract: Some(contract.clone()),
        })
        .await
    {
        Ok(ContractHandlerEvent::PutResponse {
            new_value: Ok(new_val),
            state_changed,
        }) => {
            op_manager.notify_contract_stored(&key);
            // Invariant: after a successful PUT the stored state must be non-empty.
            // A successful PutResponse with an empty value indicates a contract handler bug.
            debug_assert!(
                new_val.size() > 0,
                "put_contract: stored state must be non-empty after successful PUT for contract {key}"
            );
            Ok((new_val, state_changed))
        }
        Ok(ContractHandlerEvent::PutResponse {
            new_value: Err(err),
            ..
        }) => {
            tracing::error!(contract = %key, error = %err, phase = "error", "Failed to update contract value");
            Err(OpError::from(err))
        }
        Err(err) => Err(err.into()),
        Ok(_) => Err(OpError::UnexpectedOpState),
    }
}

mod messages {
    use std::{collections::HashSet, fmt::Display};

    use freenet_stdlib::prelude::*;
    use serde::{Deserialize, Serialize};

    use crate::message::{InnerMessage, Transaction};
    use crate::ring::Location;
    use crate::transport::peer_connection::StreamId;

    /// Payload for streaming PUT requests.
    /// This struct is serialized and sent via the stream, while the metadata
    /// is sent via the RequestStreaming message.
    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) struct PutStreamingPayload {
        pub contract: ContractContainer,
        #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
        pub related_contracts: RelatedContracts<'static>,
        pub value: WrappedState,
    }

    /// PUT operation messages.
    ///
    /// The PUT operation stores a contract and its initial state in the network.
    /// It uses hop-by-hop routing: each node forwards toward the contract location
    /// and remembers where the request came from to route the response back.
    ///
    /// If a PUT reaches a node that is already subscribed to the contract and the
    /// merged state differs from the input, an Update operation is triggered to
    /// propagate the change to other subscribers.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum PutMsg {
        /// Request to store a contract. Forwarded hop-by-hop toward contract location.
        /// Each receiving node:
        /// 1. Stores the contract locally (caching)
        /// 2. Forwards to the next hop closer to contract location
        /// 3. Remembers upstream_addr to route the response back
        Request {
            id: Transaction,
            contract: ContractContainer,
            #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
            related_contracts: RelatedContracts<'static>,
            value: WrappedState,
            /// Hops to live - decremented at each hop, request fails if reaches 0
            htl: usize,
            /// Addresses to skip when selecting next hop (prevents loops)
            skip_list: HashSet<std::net::SocketAddr>,
        },
        /// Response indicating the PUT completed. Routed hop-by-hop back to originator
        /// using each node's stored upstream_addr.
        Response { id: Transaction, key: ContractKey },

        /// Streaming request to store a large contract. Used when payload exceeds
        /// streaming_threshold (default 64KB). The actual data is sent via a separate
        /// stream identified by stream_id.
        ///
        /// This variant is only used when streaming is enabled in config.
        RequestStreaming {
            id: Transaction,
            /// Identifies the stream carrying the contract and state data
            stream_id: StreamId,
            /// Key of the contract being stored
            contract_key: ContractKey,
            /// Total size of the streamed payload in bytes
            total_size: u64,
            /// Hops to live - decremented at each hop
            htl: usize,
            /// Addresses to skip when selecting next hop
            skip_list: HashSet<std::net::SocketAddr>,
            /// Whether to subscribe to updates after storing
            subscribe: bool,
        },

        /// Streaming response indicating PUT completed for a streaming request.
        /// Sent back to the originator after the stream has been fully received
        /// and the contract stored.
        ResponseStreaming {
            id: Transaction,
            key: ContractKey,
            /// Whether the receiving node should continue forwarding to other peers
            continue_forwarding: bool,
        },

        /// Lightweight ACK sent by a relay peer back to its upstream when it forwards
        /// a PUT request to the next hop. Tells the upstream "I received the data and
        /// am processing it" so the GC task can distinguish dead peers from slow
        /// multi-hop chains. Fire-and-forget — no response expected.
        ForwardingAck {
            id: Transaction,
            contract_key: ContractKey,
        },
    }

    impl InnerMessage for PutMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::Request { id, .. }
                | Self::Response { id, .. }
                | Self::RequestStreaming { id, .. }
                | Self::ResponseStreaming { id, .. }
                | Self::ForwardingAck { id, .. } => id,
            }
        }

        fn requested_location(&self) -> Option<Location> {
            match self {
                Self::Request { contract, .. } => Some(Location::from(contract.id())),
                Self::Response { key, .. } => Some(Location::from(key.id())),
                Self::RequestStreaming { contract_key, .. } => {
                    Some(Location::from(contract_key.id()))
                }
                Self::ResponseStreaming { key, .. } => Some(Location::from(key.id())),
                Self::ForwardingAck { contract_key, .. } => Some(Location::from(contract_key.id())),
            }
        }
    }

    impl Display for PutMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Request {
                    id, contract, htl, ..
                } => {
                    write!(
                        f,
                        "PutRequest(id: {}, key: {}, htl: {})",
                        id,
                        contract.key(),
                        htl
                    )
                }
                Self::Response { id, key } => {
                    write!(f, "PutResponse(id: {}, key: {})", id, key)
                }
                Self::RequestStreaming {
                    id,
                    stream_id,
                    contract_key,
                    total_size,
                    htl,
                    ..
                } => {
                    write!(
                        f,
                        "PutRequestStreaming(id: {}, key: {}, stream: {}, size: {}, htl: {})",
                        id, contract_key, stream_id, total_size, htl
                    )
                }
                Self::ResponseStreaming {
                    id,
                    key,
                    continue_forwarding,
                } => {
                    write!(
                        f,
                        "PutResponseStreaming(id: {}, key: {}, continue: {})",
                        id, key, continue_forwarding
                    )
                }
                Self::ForwardingAck { id, contract_key } => {
                    write!(f, "PutForwardingAck(id: {}, key: {})", id, contract_key)
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::wildcard_enum_match_arm)]
mod tests {
    use super::*;
    use crate::message::{InnerMessage, Transaction};
    use crate::operations::test_utils::make_contract_key;

    #[test]
    fn put_msg_id_returns_transaction() {
        let tx = Transaction::new::<PutMsg>();
        let msg = PutMsg::Response {
            id: tx,
            key: make_contract_key(1),
        };
        assert_eq!(*msg.id(), tx, "id() should return the transaction ID");
    }

    #[test]
    fn put_msg_display_formats_correctly() {
        let tx = Transaction::new::<PutMsg>();
        let msg = PutMsg::Response {
            id: tx,
            key: make_contract_key(1),
        };
        let display = format!("{}", msg);
        assert!(
            display.contains("PutResponse"),
            "Display should contain message type name"
        );
    }

    #[test]
    fn test_forwarding_ack_serde_roundtrip() {
        let tx = Transaction::new::<PutMsg>();
        let key = make_contract_key(42);
        let msg = PutMsg::ForwardingAck {
            id: tx,
            contract_key: key,
        };

        let serialized = bincode::serialize(&msg).expect("serialize");
        let deserialized: PutMsg = bincode::deserialize(&serialized).expect("deserialize");

        match deserialized {
            PutMsg::ForwardingAck { id, contract_key } => {
                assert_eq!(id, tx);
                assert_eq!(contract_key, key);
            }
            other => panic!("Expected ForwardingAck, got {other}"),
        }
    }

    /// Pin: the PUT GC speculative retry accumulator and retry-count
    /// map must not return. Their reintroduction risks the per-tx
    /// DashMap leak the surrounding code was rebuilt to avoid.
    #[test]
    fn put_gc_speculative_retry_block_must_stay_deleted() {
        let src = include_str!("../node/op_state_manager.rs");
        assert!(
            !src.contains("put_retry_candidates"),
            "`put_retry_candidates` accumulator must stay deleted"
        );
        assert!(
            !src.contains("put_retried"),
            "`put_retried` retry-count map must stay deleted"
        );
    }

    /// Pin: `OpManager::completed` must not touch any per-op DashMap.
    /// The surviving completion side effects are limited to the global
    /// `under_progress` / `completed` sets and the `request_router`.
    #[test]
    fn completed_must_not_touch_per_op_dashmaps() {
        let src = include_str!("../node/op_state_manager.rs");
        let fn_start = src
            .find("pub fn completed(&self, id: Transaction)")
            .expect("OpManager::completed not found");
        let fn_end = src[fn_start..]
            .find("\n    }\n")
            .expect("OpManager::completed closing brace not found")
            + fn_start;
        let body = &src[fn_start..fn_end];
        for forbidden in [
            "self.ops.connect",
            "self.ops.put",
            "self.ops.get",
            "self.ops.subscribe",
            "self.ops.update",
        ] {
            assert!(
                !body.contains(forbidden),
                "OpManager::completed must not reference `{forbidden}`"
            );
        }
    }

    /// Pin: ForwardingAck senders must not return. Relay drivers omit
    /// them (would race the capacity-1 reply waiter) and the consumer
    /// is a no-op.
    #[test]
    fn put_forwarding_ack_senders_must_stay_deleted() {
        let src = include_str!("put.rs");
        let needle = format!("NetMessage::from({}::ForwardingAck", "PutMsg",);
        assert!(
            !src.contains(&needle),
            "ForwardingAck senders must not be reintroduced"
        );
    }

    /// Regression guard for the freenet-stdlib mirror demo 180s timeout
    /// (2026-05-14). `finalize_put_at_originator` MUST call
    /// `mark_local_client_access` so PUT'd-but-never-GET'd contracts get
    /// the local-client flag set on their hosting cache entry. Without
    /// this, `contracts_needing_renewal` excludes them, the subscription
    /// expires, the entry eventually gets evicted, and the next cold
    /// remote GET fails the `is_locally_hosted` shortcut and routes to
    /// the network — where, for a contract no other peer subscribes to,
    /// the GetOp hangs until the WS client's 180s timeout fires.
    ///
    /// The GET path already calls `mark_local_client_access` (see
    /// `client_events::serve_get`); this PR adds the symmetric call on
    /// the originator-PUT path so the flag is no longer GET-only.
    #[test]
    fn finalize_put_at_originator_marks_local_client_access() {
        const SOURCE: &str = include_str!("put.rs");

        // Locate the function definition.
        let fn_start = SOURCE
            .find("pub(super) async fn finalize_put_at_originator(")
            .expect("finalize_put_at_originator definition not found");
        // Body extent: from the opening `{` of the body to the matching
        // closing `}\n`. Use a coarse scan that's robust enough for the
        // small function — we just need the call site to be inside.
        let body_start = SOURCE[fn_start..]
            .find('{')
            .map(|p| fn_start + p)
            .expect("function body opening brace not found");
        let body_end = SOURCE[body_start..]
            .find("\n}\n")
            .map(|p| body_start + p)
            .expect("function body closing brace not found");
        let body = &SOURCE[body_start..body_end];

        assert!(
            body.contains("mark_local_client_access"),
            "finalize_put_at_originator MUST call mark_local_client_access \
             so self-hosted contracts get the local-client flag set on the \
             hosting cache entry. Without this call, the freenet-stdlib \
             mirror demo cold-GET timeout (2026-05-14) returns. See \
             contracts_needing_renewal at hosting.rs:971-991 for the \
             downstream gate that depends on this flag."
        );
    }
}
