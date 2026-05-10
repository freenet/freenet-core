//! A contract is PUT within a location distance, this entails that all nodes within
//! a given radius will cache a copy of the contract and it's current value,
//! as well as will broadcast updates to the contract value to all subscribers.

pub(crate) mod op_ctx_task;

pub(crate) use self::messages::{PutMsg, PutStreamingPayload};
use freenet_stdlib::{
    client_api::{ErrorKind, HostResponse},
    prelude::*,
};

use super::{OpError, OpOutcome, put};

use crate::node::IsOperationCompleted;
use crate::{
    client_events::HostResult,
    contract::ContractHandlerEvent,
    message::Transaction,
    node::OpManager,
    ring::{Location, PeerKeyLocation},
    tracing::{NetEventLog, OperationFailure},
};
use either::Either;

/// Routing stats for put operations, used to report success/failure to the router.
///
/// `PutStats`, `PutOp`, `PutState`, `AwaitingResponseData`, `FinishedData`
/// are kept under `#[allow(dead_code)]` after #1454 phase 5 final retired
/// the legacy state machine. The 25 inline outcome / failure-routing /
/// wire-format tests in `mod tests` below exercise them as historical
/// documentation; phase 6 will remove them entirely.
#[allow(dead_code)]
struct PutStats {
    target_peer: PeerKeyLocation,
    contract_location: Location,
}

#[allow(dead_code)]
pub(crate) struct PutOp {
    pub id: Transaction,
    state: Option<PutState>,
    /// The address we received this operation's message from.
    /// Used for connection-based routing: responses are sent back to this address.
    upstream_addr: Option<std::net::SocketAddr>,
    /// Routing stats for reporting outcomes to the router.
    stats: Option<PutStats>,
}

#[allow(dead_code)]
impl PutOp {
    pub(super) fn outcome(&self) -> OpOutcome<'_> {
        if self.finalized() {
            if let Some(ref stats) = self.stats {
                return OpOutcome::ContractOpSuccessUntimed {
                    target_peer: &stats.target_peer,
                    contract_location: stats.contract_location,
                };
            }
            return OpOutcome::Irrelevant;
        }
        // Not completed — if we have stats, report as failure
        if let Some(ref stats) = self.stats {
            OpOutcome::ContractOpFailure {
                target_peer: &stats.target_peer,
                contract_location: stats.contract_location,
            }
        } else {
            OpOutcome::Incomplete
        }
    }

    /// Returns true if this PUT was initiated by a local client (not forwarded from a peer).
    pub(crate) fn is_client_initiated(&self) -> bool {
        self.upstream_addr.is_none()
    }

    /// Extract routing failure info for timeout reporting.
    pub(crate) fn failure_routing_info(&self) -> Option<(PeerKeyLocation, Location)> {
        self.stats
            .as_ref()
            .map(|s| (s.target_peer.clone(), s.contract_location))
    }

    pub(super) fn finalized(&self) -> bool {
        self.state.is_none() || matches!(self.state, Some(PutState::Finished(_)))
    }

    pub(super) fn to_host_result(&self) -> HostResult {
        if let Some(PutState::Finished(data)) = &self.state {
            let key = &data.key;
            Ok(HostResponse::ContractResponse(
                freenet_stdlib::client_api::ContractResponse::PutResponse { key: *key },
            ))
        } else {
            Err(ErrorKind::OperationError {
                cause: "put didn't finish successfully".into(),
            }
            .into())
        }
    }

    /// Get the next hop address if this operation is in a state that needs to send
    /// an outbound message to a downstream peer.
    pub(crate) fn get_next_hop_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.state {
            Some(PutState::AwaitingResponse(data)) => data.next_hop,
            _ => None,
        }
    }

    /// Get the current HTL (remaining hops) for this operation.
    /// Returns None if the operation is not in AwaitingResponse state.
    pub(crate) fn get_current_htl(&self) -> Option<usize> {
        match &self.state {
            Some(PutState::AwaitingResponse(data)) => Some(data.current_htl),
            _ => None,
        }
    }

    /// Handle aborted connections.
    pub(crate) async fn handle_abort(self, op_manager: &OpManager) -> Result<(), OpError> {
        tracing::warn!(
            tx = %self.id,
            "Put operation aborted due to connection failure"
        );

        // Extract key and current_htl from state if available
        let (key, current_htl) = match &self.state {
            Some(PutState::AwaitingResponse(data)) => (None, Some(data.current_htl)),
            Some(PutState::Finished(data)) => (Some(data.key), None),
            None => (None, None),
        };

        // Calculate hop_count: max_htl - current_htl
        let hop_count = current_htl.map(|htl| op_manager.ring.max_hops_to_live.saturating_sub(htl));

        // Emit failure event if we have the key
        if let Some(key) = key {
            if let Some(event) = NetEventLog::put_failure(
                &self.id,
                &op_manager.ring,
                key,
                OperationFailure::ConnectionDropped,
                hop_count,
            ) {
                op_manager.ring.register_events(Either::Left(event)).await;
            }
        }

        // Create an error result to notify the client
        let error_result: crate::client_events::HostResult =
            Err(freenet_stdlib::client_api::ErrorKind::OperationError {
                cause: "Put operation failed: peer connection dropped".into(),
            }
            .into());

        // Send the error to the client via the result router.
        // Use try_send to avoid blocking the event loop (see channel-safety.md).
        if let Err(err) = op_manager
            .result_router_tx
            .try_send((self.id, error_result))
        {
            tracing::error!(
                tx = %self.id,
                error = %err,
                "Failed to send abort notification to client \
                 (result router channel full or closed)"
            );
        }

        // Mark the operation as completed so it's removed from tracking
        op_manager.completed(self.id);
        Ok(())
    }
}

impl IsOperationCompleted for PutOp {
    fn is_completed(&self) -> bool {
        matches!(self.state, Some(put::PutState::Finished(_)))
    }
}

/// Telemetry data for originator-side PUT finalization.
pub(super) struct PutFinalizationData {
    pub sender: PeerKeyLocation,
    pub hop_count: Option<usize>,
    pub state_hash: Option<String>,
    pub state_size: Option<usize>,
}

/// Originator-side finalization after a PUT has been accepted by the network.
///
/// Emits `put_success` telemetry and, if `subscribe` is true, starts a
/// post-PUT subscription. Called by both the legacy `process_message`
/// originator branches and the task-per-tx driver (Phase 3a).
///
/// The caller is responsible for constructing and delivering the client
/// result (`OperationResult::ContinueOp` on the legacy path,
/// `DriverOutcome::Publish` on the task-per-tx path).
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

// State machine for PUT operations.
//
// State transitions:
// - Originator (task-per-tx driver, operations/put/op_ctx_task.rs):
//   drives the request directly; state enters at AwaitingResponse.
// - Forwarder (legacy relay path): receives Request → AwaitingResponse →
//   receives Response → done.
// - Final node: receives Request → stores contract → sends Response → done.
//
// ── Type-state data structs ──────────────────────────────────────────────
//
// Each state is a named struct; transition methods encode compile-time
// guarantees that invalid transitions are unrepresentable.

/// Data for the AwaitingResponse state: waiting for downstream node's response.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AwaitingResponseData {
    /// If true, start a subscription after PUT completes (originator only)
    pub subscribe: bool,
    /// If true, the PUT response waits for the subscription to complete
    pub blocking_subscribe: bool,
    /// Next hop address for routing the outbound message
    pub next_hop: Option<std::net::SocketAddr>,
    /// Current HTL (remaining hops) for hop_count calculation.
    pub current_htl: usize,
}

/// Data for the Finished state: PUT operation completed successfully.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct FinishedData {
    pub key: ContractKey,
}

// ── State enum (wraps the typed structs) ─────────────────────────────────

/// State machine for PUT operations.
/// - Originator (task-per-tx): state enters at AwaitingResponse.
/// - Forwarder (legacy relay): ReceivedRequest → AwaitingResponse → done.
/// - Final node: receives Request → stores → sends Response → done.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
#[allow(dead_code)]
pub enum PutState {
    /// Waiting for response from downstream node.
    AwaitingResponse(AwaitingResponseData),
    /// Operation completed successfully.
    Finished(FinishedData),
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
    // after the contract has been cached, push the update query
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
            // Notify any waiters that this contract has been stored
            op_manager.notify_contract_stored(&key);
            // Invariant: after a successful PUT, the stored state must be non-empty.
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
            // TODO: not a valid value update, notify back to requester
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

    fn make_put_op(state: Option<PutState>) -> PutOp {
        PutOp {
            id: Transaction::new::<PutMsg>(),
            state,
            upstream_addr: None,
            stats: None,
        }
    }

    // Tests for finalized() method
    #[test]
    fn put_op_finalized_when_state_is_none() {
        let op = make_put_op(None);
        assert!(
            op.finalized(),
            "PutOp should be finalized when state is None"
        );
    }

    #[test]
    fn put_op_finalized_when_state_is_finished() {
        let op = make_put_op(Some(PutState::Finished(FinishedData {
            key: make_contract_key(1),
        })));
        assert!(
            op.finalized(),
            "PutOp should be finalized when state is Finished"
        );
    }

    #[test]
    fn put_op_not_finalized_when_awaiting_response() {
        let op = make_put_op(Some(PutState::AwaitingResponse(AwaitingResponseData {
            subscribe: false,
            blocking_subscribe: false,
            next_hop: None,
            current_htl: 10,
        })));
        assert!(
            !op.finalized(),
            "PutOp should not be finalized in AwaitingResponse state"
        );
    }

    // Tests for to_host_result() method
    #[test]
    fn put_op_to_host_result_success_when_finished() {
        let key = make_contract_key(1);
        let op = make_put_op(Some(PutState::Finished(FinishedData { key })));
        let result = op.to_host_result();
        assert!(
            result.is_ok(),
            "to_host_result should return Ok for Finished state"
        );

        if let Ok(HostResponse::ContractResponse(
            freenet_stdlib::client_api::ContractResponse::PutResponse { key: returned_key },
        )) = result
        {
            assert_eq!(returned_key, key, "Returned key should match");
        } else {
            panic!("Expected PutResponse");
        }
    }

    #[test]
    fn put_op_to_host_result_error_when_not_finished() {
        let op = make_put_op(Some(PutState::AwaitingResponse(AwaitingResponseData {
            subscribe: false,
            blocking_subscribe: false,
            next_hop: None,
            current_htl: 10,
        })));
        let result = op.to_host_result();
        assert!(
            result.is_err(),
            "to_host_result should return Err for non-Finished state"
        );
    }

    #[test]
    fn put_op_to_host_result_error_when_none() {
        let op = make_put_op(None);
        let result = op.to_host_result();
        assert!(
            result.is_err(),
            "to_host_result should return Err when state is None"
        );
    }

    // Tests for is_completed() trait method
    #[test]
    fn put_op_is_completed_when_finished() {
        let op = make_put_op(Some(PutState::Finished(FinishedData {
            key: make_contract_key(1),
        })));
        assert!(
            op.is_completed(),
            "is_completed should return true for Finished state"
        );
    }

    #[test]
    fn put_op_is_not_completed_when_in_progress() {
        let op = make_put_op(Some(PutState::AwaitingResponse(AwaitingResponseData {
            subscribe: false,
            blocking_subscribe: false,
            next_hop: None,
            current_htl: 10,
        })));
        assert!(
            !op.is_completed(),
            "is_completed should return false for AwaitingResponse state"
        );
    }

    // Tests for PutMsg helper methods
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

    // Tests for blocking_subscribe propagation on AwaitingResponse state.
    //
    // Replace deleted `start_op_*` constructor tests: since the
    // task-per-tx driver (operations/put/op_ctx_task.rs) owns the
    // originator path, the `PrepareRequest` state was removed and the
    // first reachable PutState is AwaitingResponse. These tests lock
    // the behavior the deleted tests covered (blocking_subscribe
    // propagation, stats-none on fresh op) against the new shape.

    fn awaiting_response_data(subscribe: bool, blocking_subscribe: bool) -> AwaitingResponseData {
        AwaitingResponseData {
            subscribe,
            blocking_subscribe,
            next_hop: None,
            current_htl: 10,
        }
    }

    #[test]
    fn awaiting_response_carries_blocking_subscribe_true() {
        let op = make_put_op(Some(PutState::AwaitingResponse(awaiting_response_data(
            true, true,
        ))));
        match op.state {
            Some(PutState::AwaitingResponse(data)) => assert!(
                data.blocking_subscribe,
                "blocking_subscribe should be true in AwaitingResponse"
            ),
            other => panic!("Expected AwaitingResponse state, got {:?}", other),
        }
    }

    #[test]
    fn awaiting_response_with_explicit_tx_carries_blocking_subscribe_true() {
        // Covers the removed start_op_with_id_propagates_blocking_subscribe_true:
        // verify an explicit tx is preserved on the originator-side state.
        let tx = Transaction::new::<PutMsg>();
        let op = PutOp {
            id: tx,
            state: Some(PutState::AwaitingResponse(awaiting_response_data(
                true, true,
            ))),
            upstream_addr: None,
            stats: None,
        };
        assert_eq!(op.id, tx, "explicit tx must round-trip into PutOp.id");
        match op.state {
            Some(PutState::AwaitingResponse(data)) => assert!(
                data.blocking_subscribe,
                "blocking_subscribe carries through AwaitingResponse when constructed with explicit tx"
            ),
            other => panic!("Expected AwaitingResponse state, got {:?}", other),
        }
    }

    #[test]
    fn awaiting_response_defaults_blocking_subscribe_false() {
        // Covers the removed start_op_defaults_blocking_subscribe_false:
        // subscribe=true + blocking_subscribe=false is a valid combo.
        let op = make_put_op(Some(PutState::AwaitingResponse(awaiting_response_data(
            true, false,
        ))));
        match op.state {
            Some(PutState::AwaitingResponse(data)) => assert!(
                !data.blocking_subscribe,
                "blocking_subscribe should be false when unset, even if subscribe=true"
            ),
            other => panic!("Expected AwaitingResponse state, got {:?}", other),
        }
    }

    #[test]
    fn fresh_awaiting_response_op_has_no_stats() {
        // Covers the removed start_op_creates_put_with_no_stats: a freshly
        // spawned originator-side PutOp has stats=None until a target peer
        // is chosen, so outcome() must be Incomplete.
        let op = make_put_op(Some(PutState::AwaitingResponse(awaiting_response_data(
            false, false,
        ))));
        assert!(
            op.stats.is_none(),
            "fresh AwaitingResponse PutOp should have stats=None"
        );
        assert!(matches!(op.outcome(), OpOutcome::Incomplete));
    }

    // Tests for outcome() method
    #[test]
    fn put_op_outcome_success_untimed_when_finalized_with_stats() {
        use crate::operations::OpOutcome;
        use crate::ring::{Location, PeerKeyLocation};

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();
        let op = PutOp {
            id: Transaction::new::<PutMsg>(),
            state: Some(PutState::Finished(FinishedData {
                key: make_contract_key(1),
            })),
            upstream_addr: None,
            stats: Some(PutStats {
                target_peer: target_peer.clone(),
                contract_location,
            }),
        };
        match op.outcome() {
            OpOutcome::ContractOpSuccessUntimed {
                target_peer: peer,
                contract_location: loc,
            } => {
                assert_eq!(*peer, target_peer);
                assert_eq!(loc, contract_location);
            }
            OpOutcome::ContractOpSuccess { .. }
            | OpOutcome::ContractOpFailure { .. }
            | OpOutcome::Incomplete
            | OpOutcome::Irrelevant => {
                panic!("Expected ContractOpSuccessUntimed for finalized put with stats")
            }
        }
    }

    #[test]
    fn put_op_outcome_irrelevant_when_finalized_without_stats() {
        use crate::operations::OpOutcome;

        let op = PutOp {
            id: Transaction::new::<PutMsg>(),
            state: Some(PutState::Finished(FinishedData {
                key: make_contract_key(1),
            })),
            upstream_addr: None,
            stats: None,
        };
        assert!(matches!(op.outcome(), OpOutcome::Irrelevant));
    }

    #[test]
    fn put_op_outcome_failure_when_not_finalized_with_stats() {
        use crate::operations::OpOutcome;
        use crate::ring::{Location, PeerKeyLocation};

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();
        let op = PutOp {
            id: Transaction::new::<PutMsg>(),
            state: Some(PutState::AwaitingResponse(AwaitingResponseData {
                subscribe: false,
                blocking_subscribe: false,
                next_hop: None,
                current_htl: 10,
            })),
            upstream_addr: None,
            stats: Some(PutStats {
                target_peer: target_peer.clone(),
                contract_location,
            }),
        };
        match op.outcome() {
            OpOutcome::ContractOpFailure {
                target_peer: peer,
                contract_location: loc,
            } => {
                assert_eq!(*peer, target_peer);
                assert_eq!(loc, contract_location);
            }
            OpOutcome::ContractOpSuccess { .. }
            | OpOutcome::ContractOpSuccessUntimed { .. }
            | OpOutcome::Incomplete
            | OpOutcome::Irrelevant => {
                panic!("Expected ContractOpFailure for non-finalized put with stats")
            }
        }
    }

    #[test]
    fn put_op_outcome_incomplete_when_not_finalized_without_stats() {
        use crate::operations::OpOutcome;

        let op = make_put_op(Some(PutState::AwaitingResponse(AwaitingResponseData {
            subscribe: false,
            blocking_subscribe: false,
            next_hop: None,
            current_htl: 10,
        })));
        assert!(matches!(op.outcome(), OpOutcome::Incomplete));
    }

    #[test]
    fn put_op_failure_routing_info_with_stats() {
        use crate::ring::{Location, PeerKeyLocation};

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();
        let op = PutOp {
            id: Transaction::new::<PutMsg>(),
            state: None,
            upstream_addr: None,
            stats: Some(PutStats {
                target_peer: target_peer.clone(),
                contract_location,
            }),
        };
        let info = op.failure_routing_info().expect("should have routing info");
        assert_eq!(info.0, target_peer);
        assert_eq!(info.1, contract_location);
    }

    #[test]
    fn put_op_failure_routing_info_without_stats() {
        let op = make_put_op(None);
        assert!(op.failure_routing_info().is_none());
    }

    /// Simulate a put operation lifecycle: initially no stats (state=None),
    /// then stats are set when forwarding, then state is Finished →
    /// outcome should be SuccessUntimed.
    #[test]
    fn put_op_stats_lifecycle_from_initial_to_finished() {
        use crate::ring::{Location, PeerKeyLocation};

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        // Step 1: Initial state - in-flight AwaitingResponse, no stats yet
        let mut op = make_put_op(Some(PutState::AwaitingResponse(AwaitingResponseData {
            subscribe: false,
            blocking_subscribe: false,
            next_hop: None,
            current_htl: 10,
        })));
        assert!(matches!(op.outcome(), OpOutcome::Incomplete));
        assert!(op.failure_routing_info().is_none());

        // Step 2: Stats are set when forwarding to next peer
        op.stats = Some(PutStats {
            target_peer: target_peer.clone(),
            contract_location,
        });
        // Not finalized yet, but has stats → failure
        match op.outcome() {
            OpOutcome::ContractOpFailure {
                target_peer: peer, ..
            } => assert_eq!(*peer, target_peer),
            OpOutcome::ContractOpSuccess { .. }
            | OpOutcome::ContractOpSuccessUntimed { .. }
            | OpOutcome::Incomplete
            | OpOutcome::Irrelevant => {
                panic!("Expected ContractOpFailure for in-progress put with stats")
            }
        }
        assert!(op.failure_routing_info().is_some());

        // Step 3: Operation finishes successfully
        op.state = Some(PutState::Finished(FinishedData {
            key: make_contract_key(1),
        }));
        match op.outcome() {
            OpOutcome::ContractOpSuccessUntimed {
                target_peer: peer,
                contract_location: loc,
            } => {
                assert_eq!(*peer, target_peer);
                assert_eq!(loc, contract_location);
            }
            OpOutcome::ContractOpSuccess { .. }
            | OpOutcome::ContractOpFailure { .. }
            | OpOutcome::Incomplete
            | OpOutcome::Irrelevant => {
                panic!("Expected ContractOpSuccessUntimed for finished put with stats")
            }
        }
    }

    /// Verify that a forwarding node (state=None, stats=None) returns Irrelevant
    /// since it is finalized (state is None) but has no routing decision to report.
    #[test]
    fn put_op_forwarding_node_outcome_is_irrelevant() {
        let op = PutOp {
            id: Transaction::new::<PutMsg>(),
            state: None,
            upstream_addr: Some("127.0.0.1:12345".parse().unwrap()),
            stats: None,
        };
        // state=None → finalized, stats=None → Irrelevant
        assert!(op.finalized());
        assert!(matches!(op.outcome(), OpOutcome::Irrelevant));
    }

    #[test]
    fn is_client_initiated_true_when_no_upstream() {
        let op = make_put_op(None);
        assert!(op.is_client_initiated());
    }

    #[test]
    fn is_client_initiated_false_when_forwarded() {
        let op = PutOp {
            id: Transaction::new::<PutMsg>(),
            state: None,
            upstream_addr: Some("127.0.0.1:12345".parse().unwrap()),
            stats: None,
        };
        assert!(!op.is_client_initiated());
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

    // ── Intermediate node stats tracking tests (#3527) ─────────────────────

    /// Non-finalized PUT with stats reports ContractOpFailure on timeout.
    #[test]
    fn test_put_failure_outcome_with_stats() {
        use crate::operations::test_utils::make_peer;
        let target = make_peer(9001);
        let contract_key = make_contract_key(42);
        let contract_location = Location::from(&contract_key);

        let op = PutOp {
            id: Transaction::new::<PutMsg>(),
            state: Some(PutState::AwaitingResponse(AwaitingResponseData {
                subscribe: false,
                blocking_subscribe: false,
                current_htl: 10,
                next_hop: target.socket_addr(),
            })),
            upstream_addr: Some("127.0.0.1:12345".parse().unwrap()),
            stats: Some(PutStats {
                target_peer: target.clone(),
                contract_location,
            }),
        };

        assert!(!op.finalized());
        match op.outcome() {
            OpOutcome::ContractOpFailure {
                target_peer,
                contract_location: loc,
            } => {
                assert_eq!(target_peer, &target);
                assert_eq!(loc, contract_location);
            }
            other => panic!("Expected ContractOpFailure, got {:?}", other),
        }
    }

    /// Non-finalized PUT without stats reports Incomplete.
    #[test]
    fn test_put_failure_outcome_without_stats() {
        let op = PutOp {
            id: Transaction::new::<PutMsg>(),
            state: Some(PutState::AwaitingResponse(AwaitingResponseData {
                subscribe: false,
                blocking_subscribe: false,
                current_htl: 10,
                next_hop: None,
            })),
            upstream_addr: Some("127.0.0.1:12345".parse().unwrap()),
            stats: None,
        };

        assert!(!op.finalized());
        assert!(
            matches!(op.outcome(), OpOutcome::Incomplete),
            "PUT without stats should return Incomplete"
        );
    }

    // === Pins for retired PUT GC speculative retry surface (PR #3964) ===
    //
    // These pins catch accidental re-introduction of the retired
    // ack-aware speculative retry mechanism. The surface was retired
    // because PutOp entries no longer leak into `OpManager.ops.put`
    // for completed task-per-tx originators (the leak was fixed by
    // extending `OpManager::completed` to remove the per-type
    // DashMap entry).

    /// Pin: `PutOp` MUST NOT regrow `speculative_paths` / `ack_received`
    /// fields. The GC speculative retry block (deleted in PR #3964) was
    /// the only consumer; reintroducing the fields silently brings back
    /// the dead-code surface.
    #[test]
    fn put_op_must_not_regrow_speculative_or_ack_fields() {
        let src = include_str!("put.rs");
        let struct_start = src
            .find("pub(crate) struct PutOp {")
            .expect("PutOp struct declaration not found");
        let struct_end = src[struct_start..]
            .find("\n}\n")
            .expect("PutOp struct closing brace not found")
            + struct_start;
        let struct_body = &src[struct_start..struct_end];
        assert!(
            !struct_body.contains("speculative_paths"),
            "PutOp must not regrow `speculative_paths` — retired in PR #3964"
        );
        assert!(
            !struct_body.contains("ack_received"),
            "PutOp must not regrow `ack_received` — retired in PR #3964"
        );
    }

    /// Pin: `AwaitingResponseData` MUST NOT regrow the retry-related
    /// fields (alternatives, tried_peers, attempts_at_hop, visited,
    /// retry_payload, contract_key). They existed only to feed
    /// `retry_with_next_alternative`, which was deleted with the GC
    /// speculative retry block.
    #[test]
    fn awaiting_response_data_must_not_regrow_retry_fields() {
        let src = include_str!("put.rs");
        let struct_start = src
            .find("pub struct AwaitingResponseData {")
            .expect("AwaitingResponseData struct declaration not found");
        let struct_end = src[struct_start..]
            .find("\n}\n")
            .expect("AwaitingResponseData closing brace not found")
            + struct_start;
        let body = &src[struct_start..struct_end];
        for retired in [
            "alternatives",
            "tried_peers",
            "attempts_at_hop",
            "visited",
            "retry_payload",
            "contract_key",
        ] {
            assert!(
                !body.contains(retired),
                "AwaitingResponseData must not regrow `{retired}` — retired in PR #3964"
            );
        }
    }

    /// Pin: the retired retry-with-next-alternative impl method MUST
    /// stay deleted. Reintroducing it implies regrowing the
    /// retry-related fields (which the previous pin guards) and
    /// bringing back the GC speculative retry block.
    #[test]
    fn retry_method_signature_must_stay_deleted() {
        let src = include_str!("put.rs");
        // Compose the needle at runtime so the pin's own source line
        // does not contain the literal we're searching for.
        let needle = format!(
            "{vis} fn retry_with_next_{kind}",
            vis = "pub(crate)",
            kind = "alternative",
        );
        assert!(
            !src.contains(&needle),
            "retry method retired in PR #3964 — see PutOp pins"
        );
    }

    /// Pin: PUT GC speculative retry block MUST NOT come back.
    /// `op_state_manager.rs` should no longer contain the
    /// `put_retry_candidates` accumulator or the `put_retried` map.
    #[test]
    fn put_gc_speculative_retry_block_must_stay_deleted() {
        let src = include_str!("../node/op_state_manager.rs");
        assert!(
            !src.contains("put_retry_candidates"),
            "PUT GC speculative retry block was retired in PR #3964 — \
             reintroduction risks the Phase 3a DashMap leak interaction"
        );
        assert!(
            !src.contains("put_retried"),
            "PUT GC retry-count map was retired in PR #3964"
        );
    }

    /// Pin: `OpManager::completed` MUST keep removing the per-type
    /// DashMap entry for the remaining DashMap-backed ops (Connect,
    /// Get, Subscribe). PUT and UPDATE no longer have DashMap entries
    /// post-#1454 phase 5 final. Without this the Phase 3a-style leak
    /// comes back for the remaining ops.
    #[test]
    fn completed_must_remove_per_type_dashmap_entry() {
        let src = include_str!("../node/op_state_manager.rs");
        let fn_start = src
            .find("pub fn completed(&self, id: Transaction)")
            .expect("OpManager::completed not found");
        let fn_end = src[fn_start..]
            .find("\n    }\n")
            .expect("OpManager::completed closing brace not found")
            + fn_start;
        let body = &src[fn_start..fn_end];
        // SUBSCRIBE is the only surviving DashMap with a task-per-tx
        // leak risk after #1454 phase 5 final (GET/PUT/UPDATE retired).
        assert!(
            body.contains("self.ops.subscribe.remove"),
            "OpManager::completed must call `self.ops.subscribe.remove(&id)` \
             to prevent the Phase 3a-style task-per-tx DashMap leak"
        );
        // PUT removed in PUT slice of phase 5 final; GET in GET slice.
        for retired in ["self.ops.put", "self.ops.get"] {
            assert!(
                !body.contains(retired),
                "OpManager::completed must not reference `{retired}` after \
                 #1454 phase 5 final retired the corresponding DashMap"
            );
        }
    }

    /// Pin: legacy ForwardingAck senders MUST NOT come back at the
    /// legacy relay forward sites. Slice A/B drivers omit them
    /// (would race the capacity-1 task-per-tx waiter), and PR #3964
    /// removed the legacy senders since the consumer is now a no-op.
    #[test]
    fn put_forwarding_ack_senders_must_stay_deleted() {
        let src = include_str!("put.rs");
        // Compose the needle at runtime so the pin's own assert line
        // does not contain the literal we're searching for.
        let needle = format!("NetMessage::from({}::ForwardingAck", "PutMsg",);
        assert!(
            !src.contains(&needle),
            "ForwardingAck senders retired in PR #3964 — slice A/B drivers omit them"
        );
    }
}
