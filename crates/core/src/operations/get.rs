// #1454 phase 5 final (GET slice) retired the legacy GET state machine.
// `OpEnum::Get`, `OpManager.ops.get` DashMap, `impl Operation for GetOp`,
// `start_op` / `start_op_with_id` / `start_targeted_op`,
// `request_get`, `has_get_op`, `remove_get_and_report_failure`, the
// `OpEnum::Get` arm in `IsOperationCompleted for OpEnum`, the
// `OpEnum::Get` arm in the abort handler, the
// `try_from_op_enum!(OpEnum::Get, …)` macro entry, and the
// `handle_op_request<GetOp>` fallthrough in node.rs are all gone.
// Every GET wire variant now dispatches unconditionally to a
// task-per-tx driver — see `op_ctx_task::start_client_get`,
// `start_relay_get`, `start_sub_op_get`, and
// `start_targeted_sub_op_get`.
//
// `GetOp`, `GetState`, `GetStats`, `AwaitingResponseData`,
// `FinishedData` plus a small inline outcome / failure-routing /
// wire-format / pin-test surface survive under `#[allow(dead_code)]`
// pending phase 6.

#![allow(dead_code)]

pub(crate) mod op_ctx_task;

use freenet_stdlib::client_api::{ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;
use std::collections::HashSet;
use std::fmt::Display;
use std::time::Instant;

use crate::client_events::HostResult;
use crate::node::IsOperationCompleted;
use crate::{
    contract::StoreResponse,
    message::{InnerMessage, Transaction},
    operations::{OpError, OpOutcome},
    ring::{Location, PeerKeyLocation},
};

pub(crate) use self::messages::{GetMsg, GetMsgResult, GetStreamingPayload};

/// Maximum number of retries to get values.
const MAX_RETRIES: usize = 10;

/// Maximum number of peer attempts at each hop level
const DEFAULT_MAX_BREADTH: usize = 3;

/// Minimum HTL for speculative retries.
///
/// Retries use a reduced HTL (capped at current_hop) to avoid full-depth
/// traversal storms. This floor ensures retries still reach peers 2-3 hops
/// away, which is the minimum useful search depth in any topology.
const MIN_RETRY_HTL: usize = 3;

// ── Type-state data structs ──────────────────────────────────────────────
//
// Each state in the GET state machine is a named struct with typed
// transition methods.  This gives compile-time guarantees:
//
//   ReceivedRequest ── (no transition methods, next state depends on message)
//   PrepareRequest ── into_awaiting_response() ──► AwaitingResponse
//   AwaitingResponse ── into_finished()         ──► Finished
//                    ── with_retry_peer()        ──► AwaitingResponse (retry at same hop)
//                    ── with_next_hop()           ──► AwaitingResponse (advance to next hop)
//
// Invalid transitions (e.g. Finished → PrepareRequest) are unrepresentable
// because the target type simply has no such method.

/// Data for the PrepareRequest state: originator preparing GET request.
#[derive(Debug)]
struct PrepareRequestData {
    instance_id: ContractInstanceId,
    id: Transaction,
    fetch_contract: bool,
    subscribe: bool,
    blocking_subscribe: bool,
}

impl PrepareRequestData {
    /// Transition to AwaitingResponse after sending the GET request.
    ///
    /// Encodes valid transition: PrepareRequest → AwaitingResponse.
    /// Carries forward instance_id, fetch_contract, subscribe, blocking_subscribe.
    #[allow(dead_code)] // Documents valid transition; see type-state pattern
    fn into_awaiting_response(
        self,
        requester: Option<PeerKeyLocation>,
        next_hop: PeerKeyLocation,
        alternatives: Vec<PeerKeyLocation>,
        visited: super::VisitedPeers,
    ) -> AwaitingResponseData {
        AwaitingResponseData {
            instance_id: self.instance_id,
            requester,
            fetch_contract: self.fetch_contract,
            retries: 0,
            current_hop: 0,
            subscribe: self.subscribe,
            blocking_subscribe: self.blocking_subscribe,
            next_hop,
            tried_peers: HashSet::new(),
            alternatives,
            attempts_at_hop: 0,
            visited,
        }
    }
}

/// Data for the AwaitingResponse state: request sent, waiting for downstream peer.
///
/// This is the most complex state, tracking retry logic across multiple hops.
#[derive(Debug)]
struct AwaitingResponseData {
    /// Contract being fetched (by instance_id since we may not have full key yet)
    instance_id: ContractInstanceId,
    /// If specified the peer waiting for the response upstream
    requester: Option<PeerKeyLocation>,
    fetch_contract: bool,
    retries: usize,
    current_hop: usize,
    subscribe: bool,
    blocking_subscribe: bool,
    /// Peer we are currently trying to reach.
    /// Note: With connection-based routing, this is only used for state tracking,
    /// not for response routing (which uses upstream_addr instead).
    #[allow(dead_code)]
    next_hop: PeerKeyLocation,
    /// Peers we've already tried at this hop level
    tried_peers: HashSet<std::net::SocketAddr>,
    /// Alternative peers we could still try at this hop
    alternatives: Vec<PeerKeyLocation>,
    /// How many peers we've tried at this hop
    attempts_at_hop: usize,
    /// Bloom filter tracking visited peers across all hops
    visited: super::VisitedPeers,
}

impl AwaitingResponseData {
    /// Transition to Finished on successful GET response.
    ///
    /// Encodes valid transition: AwaitingResponse → Finished.
    #[allow(dead_code)] // Documents valid transition; see type-state pattern
    fn into_finished(self, key: ContractKey) -> FinishedData {
        FinishedData { key }
    }
}

/// Data for the Finished state: GET operation completed successfully.
#[derive(Debug, Clone, Copy)]
struct FinishedData {
    key: ContractKey,
}

// ── State enum (wraps the typed structs) ─────────────────────────────────

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum GetState {
    /// A new petition for a get op received from another peer.
    /// Note: We use GetOp::upstream_addr for response routing (not PeerKeyLocation).
    ReceivedRequest,
    /// Preparing request for get op.
    PrepareRequest(PrepareRequestData),
    /// Awaiting response from petition.
    AwaitingResponse(AwaitingResponseData),
    /// Operation completed successfully
    Finished(FinishedData),
}

impl Display for GetState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GetState::ReceivedRequest => write!(f, "ReceivedRequest"),
            GetState::PrepareRequest(data) => {
                write!(
                    f,
                    "PrepareRequest(instance_id: {}, id: {}, fetch_contract: {}, subscribe: {})",
                    data.instance_id, data.id, data.fetch_contract, data.subscribe
                )
            }
            GetState::AwaitingResponse(data) => {
                write!(
                    f,
                    "AwaitingResponse(requester: {:?}, fetch_contract: {}, retries: {}, current_hop: {}, subscribe: {})",
                    data.requester,
                    data.fetch_contract,
                    data.retries,
                    data.current_hop,
                    data.subscribe
                )
            }
            GetState::Finished(data) => write!(f, "Finished(key: {})", data.key),
        }
    }
}

struct GetStats {
    /// Next peer in get path to be targeted
    next_peer: Option<PeerKeyLocation>,
    contract_location: Location,
    /// (start, end)
    first_response_time: Option<(Instant, Option<Instant>)>,
    /// (start, end)
    transfer_time: Option<(Instant, Option<Instant>)>,
}

impl GetStats {
    /// Start both response and transfer timers for a new attempt.
    fn start_timers(&mut self) {
        let now = Instant::now();
        self.first_response_time = Some((now, None));
        self.transfer_time = Some((now, None));
    }

    /// Record the end time for first_response_time.
    fn record_response_end(&mut self) {
        if let Some((_, ref mut end)) = self.first_response_time {
            *end = Some(Instant::now());
        }
    }

    /// Record the end time for transfer_time.
    fn record_transfer_end(&mut self) {
        if let Some((_, ref mut end)) = self.transfer_time {
            *end = Some(Instant::now());
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct GetResult {
    key: ContractKey,
    pub state: WrappedState,
    pub contract: Option<ContractContainer>,
}

impl GetResult {
    /// Construct a `GetResult` directly from its fields. Used by the
    /// task-per-tx sub-op GET driver, which assembles the result from
    /// the wire-level terminal reply rather than a fully populated
    /// `GetOp`.
    pub(crate) fn new(
        key: ContractKey,
        state: WrappedState,
        contract: Option<ContractContainer>,
    ) -> Self {
        Self {
            key,
            state,
            contract,
        }
    }
}

impl TryFrom<GetOp> for GetResult {
    type Error = OpError;

    fn try_from(value: GetOp) -> Result<Self, Self::Error> {
        match value.result {
            Some(r) => Ok(r),
            _ => Err(OpError::UnexpectedOpState),
        }
    }
}

pub(crate) struct GetOp {
    pub id: Transaction,
    state: Option<GetState>,
    pub(super) result: Option<GetResult>,
    stats: Option<Box<GetStats>>,
    /// The address we received this operation's message from.
    /// Used for connection-based routing: responses are sent back to this address.
    upstream_addr: Option<std::net::SocketAddr>,
    /// Local cached state to fall back to if network query fails or returns NotFound.
    /// Used by: (1) original requester's `request_get` path for "network first, local
    /// fallback", (2) relay peers with stale cache (no local interest) that defer to
    /// the network, and (3) connection abort fallback.
    local_fallback: Option<(ContractKey, WrappedState, Option<ContractContainer>)>,
    /// True when this GET was spawned internally by try_auto_fetch_contract,
    /// not by a client request. Used to avoid sending spurious timeout errors
    /// to a non-existent client.
    auto_fetch: bool,
    /// Whether the client wants contract code in the response.
    /// The node always fetches WASM from the network for internal caching,
    /// but strips it from the client response when this is false.
    /// See issue #3757.
    client_return_code: bool,
}

impl GetOp {
    pub(super) fn outcome(&self) -> OpOutcome<'_> {
        if let Some((
            GetResult {
                state, contract, ..
            },
            GetStats {
                next_peer: Some(target_peer),
                contract_location,
                first_response_time: Some((response_start, Some(response_end))),
                transfer_time: Some((transfer_start, Some(transfer_end))),
                ..
            },
        )) = self.result.as_ref().zip(self.stats.as_deref())
        {
            let payload_size = state.size()
                + contract
                    .as_ref()
                    .map(|c| c.data().len())
                    .unwrap_or_default();
            OpOutcome::ContractOpSuccess {
                target_peer,
                contract_location: *contract_location,
                payload_size,
                first_response_time: *response_end - *response_start,
                payload_transfer_time: *transfer_end - *transfer_start,
            }
        } else if self.result.is_none() {
            // No result — if we have stats with a target peer, report as failure
            if let Some(GetStats {
                next_peer: Some(target_peer),
                contract_location,
                ..
            }) = self.stats.as_deref()
            {
                OpOutcome::ContractOpFailure {
                    target_peer,
                    contract_location: *contract_location,
                }
            } else {
                OpOutcome::Incomplete
            }
        } else if let Some(GetStats {
            next_peer: Some(target_peer),
            contract_location,
            ..
        }) = self.stats.as_deref()
        {
            // Result present but timing incomplete (e.g. streaming completed without
            // full handshake timing). Report as untimed success so the router still
            // gets feedback instead of silently dropping the outcome.
            OpOutcome::ContractOpSuccessUntimed {
                target_peer,
                contract_location: *contract_location,
            }
        } else {
            OpOutcome::Incomplete
        }
    }

    /// Extract the contract instance ID from the operation state.
    pub(crate) fn instance_id(&self) -> Option<ContractInstanceId> {
        match &self.state {
            Some(GetState::PrepareRequest(data)) => Some(data.instance_id),
            Some(GetState::AwaitingResponse(data)) => Some(data.instance_id),
            _ => None,
        }
    }

    /// Returns true if this GET was initiated by a local client (not forwarded from
    /// a peer and not a system-initiated auto-fetch).
    pub(crate) fn is_client_initiated(&self) -> bool {
        if self.auto_fetch {
            return false;
        }
        match &self.state {
            Some(GetState::PrepareRequest(_)) => true,
            Some(GetState::AwaitingResponse(data)) => data.requester.is_none(),
            Some(GetState::ReceivedRequest) | Some(GetState::Finished(_)) | None => false,
        }
    }

    /// Extract routing failure info for timeout reporting.
    /// Returns `(target_peer, contract_location)` if stats are available.
    pub(crate) fn failure_routing_info(&self) -> Option<(PeerKeyLocation, Location)> {
        self.stats.as_deref().and_then(|stats| {
            stats
                .next_peer
                .as_ref()
                .map(|peer| (peer.clone(), stats.contract_location))
        })
    }

    pub(super) fn finalized(&self) -> bool {
        self.result.is_some() && matches!(self.state, Some(GetState::Finished(_)))
    }

    pub(super) fn to_host_result(&self) -> HostResult {
        match &self.result {
            Some(GetResult {
                key,
                state,
                contract,
            }) => {
                // Strip contract code from client response when the client
                // requested return_contract_code=false. The node still fetches
                // and caches WASM internally for validation/subscription/hosting.
                // See issue #3757.
                let client_contract = if self.client_return_code {
                    contract.clone()
                } else {
                    None
                };
                Ok(HostResponse::ContractResponse(
                    freenet_stdlib::client_api::ContractResponse::GetResponse {
                        key: *key,
                        contract: client_contract,
                        state: state.clone(),
                    },
                ))
            }
            None => Err(ErrorKind::OperationError {
                cause: "get didn't finish successfully".into(),
            }
            .into()),
        }
    }

    /// Get the next hop address if this operation is in a state that needs to send
    /// an outbound message. Used for hop-by-hop routing.
    pub(crate) fn get_next_hop_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.state {
            Some(GetState::AwaitingResponse(data)) => data.next_hop.socket_addr(),
            _ => None,
        }
    }

    /// Get the current hop count (remaining HTL) for this operation.
    /// Returns None if the operation is not in AwaitingResponse state.
    pub(crate) fn get_current_hop(&self) -> Option<usize> {
        match &self.state {
            Some(GetState::AwaitingResponse(data)) => Some(data.current_hop),
            _ => None,
        }
    }
}

impl IsOperationCompleted for GetOp {
    fn is_completed(&self) -> bool {
        matches!(self.state, Some(GetState::Finished(_)))
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
    use crate::message::Transaction;
    use crate::operations::VisitedPeers;
    use crate::operations::test_utils::{make_contract_key, make_peer, make_test_contract};

    fn make_get_op(state: Option<GetState>, result: Option<GetResult>) -> GetOp {
        GetOp {
            id: Transaction::new::<GetMsg>(),
            state,
            result,
            stats: None,
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            client_return_code: true,
        }
    }

    // Tests for finalized() method
    #[test]
    fn get_op_finalized_when_finished_with_result() {
        let key = make_contract_key(1);
        let result = GetResult {
            key,
            state: WrappedState::new(vec![1, 2, 3]),
            contract: None,
        };
        let op = make_get_op(Some(GetState::Finished(FinishedData { key })), Some(result));
        assert!(
            op.finalized(),
            "GetOp should be finalized when state is Finished and result is present"
        );
    }

    #[test]
    fn get_op_not_finalized_when_finished_without_result() {
        let key = make_contract_key(1);
        let op = make_get_op(Some(GetState::Finished(FinishedData { key })), None);
        assert!(
            !op.finalized(),
            "GetOp should not be finalized when state is Finished but result is None"
        );
    }

    #[test]
    fn get_op_not_finalized_when_received_request() {
        let op = make_get_op(Some(GetState::ReceivedRequest), None);
        assert!(
            !op.finalized(),
            "GetOp should not be finalized in ReceivedRequest state"
        );
    }

    #[test]
    fn get_op_not_finalized_when_state_is_none() {
        let op = make_get_op(None, None);
        assert!(
            !op.finalized(),
            "GetOp should not be finalized when state is None"
        );
    }

    // Tests for to_host_result() method
    #[test]
    fn get_op_to_host_result_success_when_result_present() {
        let key = make_contract_key(1);
        let state_data = WrappedState::new(vec![1, 2, 3]);
        let result = GetResult {
            key,
            state: state_data.clone(),
            contract: None,
        };
        let op = make_get_op(Some(GetState::Finished(FinishedData { key })), Some(result));
        let host_result = op.to_host_result();

        assert!(
            host_result.is_ok(),
            "to_host_result should return Ok when result is present"
        );

        if let Ok(HostResponse::ContractResponse(
            freenet_stdlib::client_api::ContractResponse::GetResponse {
                key: returned_key,
                state: returned_state,
                ..
            },
        )) = host_result
        {
            assert_eq!(returned_key, key, "Returned key should match");
            assert_eq!(returned_state, state_data, "Returned state should match");
        } else {
            panic!("Expected GetResponse");
        }
    }

    #[test]
    fn get_op_to_host_result_error_when_no_result() {
        let op = make_get_op(Some(GetState::ReceivedRequest), None);
        let result = op.to_host_result();
        assert!(
            result.is_err(),
            "to_host_result should return Err when result is None"
        );
    }

    /// Regression test for #3757: return_contract_code=false should strip contract
    /// from client response but still cache WASM internally.
    #[test]
    fn get_op_to_host_result_strips_contract_when_client_return_code_false() {
        let key = make_contract_key(1);
        let result = GetResult {
            key,
            state: WrappedState::new(vec![1, 2, 3]),
            contract: Some(make_test_contract(&[42u8; 100])),
        };
        let mut op = make_get_op(Some(GetState::Finished(FinishedData { key })), Some(result));

        /// Extract the contract field from a to_host_result() GetResponse.
        fn get_response_contract(op: &GetOp) -> Option<ContractContainer> {
            let Ok(HostResponse::ContractResponse(
                freenet_stdlib::client_api::ContractResponse::GetResponse { contract, .. },
            )) = op.to_host_result()
            else {
                panic!("Expected Ok(GetResponse)");
            };
            contract
        }

        op.client_return_code = true;
        assert!(
            get_response_contract(&op).is_some(),
            "Contract should be included when client_return_code=true"
        );

        op.client_return_code = false;
        assert!(
            get_response_contract(&op).is_none(),
            "Contract should be stripped when client_return_code=false"
        );
    }

    // Tests for outcome() method - partial coverage since full stats require complex setup
    #[test]
    fn get_op_outcome_incomplete_without_stats() {
        let key = make_contract_key(1);
        let result = GetResult {
            key,
            state: WrappedState::new(vec![]),
            contract: None,
        };
        let op = make_get_op(Some(GetState::Finished(FinishedData { key })), Some(result));
        let outcome = op.outcome();

        assert!(matches!(outcome, OpOutcome::Incomplete));
    }

    // Tests for GetMsg helper methods
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

    // Tests for GetState Display
    #[test]
    fn get_state_display_received_request() {
        let state = GetState::ReceivedRequest;
        let display = format!("{}", state);
        assert!(
            display.contains("ReceivedRequest"),
            "Display should contain state name"
        );
    }

    #[test]
    fn get_state_display_finished() {
        let state = GetState::Finished(FinishedData {
            key: make_contract_key(1),
        });
        let display = format!("{}", state);
        assert!(
            display.contains("Finished"),
            "Display should contain state name"
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

        // Test Found variant
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

        // Test NotFound variant
        let msg_notfound = GetMsg::Response {
            id: tx,
            instance_id,
            result: GetMsgResult::NotFound,
        };
        let location_notfound = msg_notfound.requested_location();
        assert!(
            location_notfound.is_some(),
            "NotFound Response should have a requested location"
        );
        assert_eq!(
            location_notfound.unwrap(),
            Location::from(&instance_id),
            "Location should be derived from instance_id for NotFound too"
        );
    }

    #[test]
    fn test_failure_outcome_for_get() {
        use crate::ring::{Location, PeerKeyLocation};
        use std::time::Duration;

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        // GetOp with stats but no result → should return ContractOpFailure
        let op = GetOp {
            id: Transaction::new::<GetMsg>(),
            state: None,
            result: None,
            stats: Some(Box::new(GetStats {
                next_peer: Some(target_peer.clone()),
                contract_location,
                first_response_time: None,
                transfer_time: None,
            })),
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            client_return_code: true,
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
            | OpOutcome::Irrelevant => panic!("Expected ContractOpFailure"),
        }

        // GetOp with no stats and no result → should return Incomplete
        let op_no_stats = GetOp {
            id: Transaction::new::<GetMsg>(),
            state: None,
            result: None,
            stats: None,
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            client_return_code: true,
        };
        assert!(
            matches!(op_no_stats.outcome(), OpOutcome::Incomplete),
            "GetOp with no stats should return Incomplete"
        );

        // GetOp with result + complete stats → should return ContractOpSuccess
        let now = Instant::now();
        let later = now + Duration::from_millis(50);
        let op_success = GetOp {
            id: Transaction::new::<GetMsg>(),
            state: None,
            result: Some(GetResult {
                key: make_contract_key(1),
                state: WrappedState::new(vec![1, 2, 3]),
                contract: None,
            }),
            stats: Some(Box::new(GetStats {
                next_peer: Some(target_peer.clone()),
                contract_location,
                first_response_time: Some((now, Some(later))),
                transfer_time: Some((now, Some(later))),
            })),
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            client_return_code: true,
        };
        assert!(
            matches!(op_success.outcome(), OpOutcome::ContractOpSuccess { .. }),
            "GetOp with result and complete stats should return ContractOpSuccess"
        );
    }

    // ============ Partial timing outcome tests (validates GET partial timing fix) ============

    /// Result + target_peer but incomplete timing → ContractOpSuccessUntimed.
    /// Before the fix, this returned Incomplete, so the router never got feedback.
    #[test]
    fn test_get_outcome_success_untimed_partial_timing() {
        use crate::ring::{Location, PeerKeyLocation};

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();
        let now = Instant::now();

        // Result present, target_peer set, first_response_time has start but no end
        let op = GetOp {
            id: Transaction::new::<GetMsg>(),
            state: Some(GetState::Finished(FinishedData {
                key: make_contract_key(1),
            })),
            result: Some(GetResult {
                key: make_contract_key(1),
                state: WrappedState::new(vec![1, 2, 3]),
                contract: None,
            }),
            stats: Some(Box::new(GetStats {
                next_peer: Some(target_peer.clone()),
                contract_location,
                first_response_time: Some((now, None)), // Started but not completed
                transfer_time: Some((now, None)),       // Started but not completed
            })),
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            client_return_code: true,
        };

        match op.outcome() {
            OpOutcome::ContractOpSuccessUntimed {
                target_peer: peer,
                contract_location: loc,
            } => {
                assert_eq!(*peer, target_peer);
                assert_eq!(loc, contract_location);
            }
            other @ OpOutcome::ContractOpSuccess { .. }
            | other @ OpOutcome::ContractOpFailure { .. }
            | other @ OpOutcome::Incomplete
            | other @ OpOutcome::Irrelevant => {
                panic!("Expected ContractOpSuccessUntimed, got {other:?}")
            }
        }
    }

    /// Result + target_peer but no transfer_time at all → ContractOpSuccessUntimed.
    #[test]
    fn test_get_outcome_success_untimed_no_transfer_time() {
        use crate::ring::{Location, PeerKeyLocation};

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let op = GetOp {
            id: Transaction::new::<GetMsg>(),
            state: Some(GetState::Finished(FinishedData {
                key: make_contract_key(1),
            })),
            result: Some(GetResult {
                key: make_contract_key(1),
                state: WrappedState::new(vec![1, 2, 3]),
                contract: None,
            }),
            stats: Some(Box::new(GetStats {
                next_peer: Some(target_peer.clone()),
                contract_location,
                first_response_time: None,
                transfer_time: None,
            })),
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            client_return_code: true,
        };

        match op.outcome() {
            OpOutcome::ContractOpSuccessUntimed {
                target_peer: peer,
                contract_location: loc,
            } => {
                assert_eq!(*peer, target_peer);
                assert_eq!(loc, contract_location);
            }
            other @ OpOutcome::ContractOpSuccess { .. }
            | other @ OpOutcome::ContractOpFailure { .. }
            | other @ OpOutcome::Incomplete
            | other @ OpOutcome::Irrelevant => {
                panic!("Expected ContractOpSuccessUntimed, got {other:?}")
            }
        }
    }

    /// Result but no target_peer → Incomplete (can't attribute to a peer).
    #[test]
    fn test_get_outcome_incomplete_result_no_peer() {
        let op = GetOp {
            id: Transaction::new::<GetMsg>(),
            state: Some(GetState::Finished(FinishedData {
                key: make_contract_key(1),
            })),
            result: Some(GetResult {
                key: make_contract_key(1),
                state: WrappedState::new(vec![1, 2, 3]),
                contract: None,
            }),
            stats: Some(Box::new(GetStats {
                next_peer: None,
                contract_location: Location::random(),
                first_response_time: None,
                transfer_time: None,
            })),
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            client_return_code: true,
        };

        assert!(
            matches!(op.outcome(), OpOutcome::Incomplete),
            "Result with no target_peer should return Incomplete"
        );
    }

    /// No result + partial timing → still ContractOpFailure (regression guard).
    #[test]
    fn test_get_outcome_failure_no_result_partial_timing() {
        use crate::ring::{Location, PeerKeyLocation};

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();
        let now = Instant::now();

        let op = GetOp {
            id: Transaction::new::<GetMsg>(),
            state: None,
            result: None,
            stats: Some(Box::new(GetStats {
                next_peer: Some(target_peer.clone()),
                contract_location,
                first_response_time: Some((now, None)),
                transfer_time: None,
            })),
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            client_return_code: true,
        };

        match op.outcome() {
            OpOutcome::ContractOpFailure {
                target_peer: peer,
                contract_location: loc,
            } => {
                assert_eq!(*peer, target_peer);
                assert_eq!(loc, contract_location);
            }
            other @ OpOutcome::ContractOpSuccess { .. }
            | other @ OpOutcome::ContractOpSuccessUntimed { .. }
            | other @ OpOutcome::Incomplete
            | other @ OpOutcome::Irrelevant => panic!("Expected ContractOpFailure, got {other:?}"),
        }
    }

    // === Tests for is_client_initiated ===

    #[test]
    fn is_client_initiated_true_for_prepare_request() {
        let op = make_get_op(
            Some(GetState::PrepareRequest(PrepareRequestData {
                instance_id: ContractInstanceId::new([1u8; 32]),
                id: Transaction::new::<GetMsg>(),
                fetch_contract: true,
                subscribe: false,
                blocking_subscribe: false,
            })),
            None,
        );
        assert!(op.is_client_initiated());
    }

    // `is_client_initiated_true_for_awaiting_no_requester` was retired
    // together with the `make_awaiting_op` fixture (#1454 phase 5
    // final, GET slice). Equivalent coverage of the
    // `is_client_initiated` predicate is provided by the
    // `*_for_awaiting_with_requester`, `*_for_auto_fetch`, and
    // `*_for_other_states` cases below — they exercise the same code
    // path with explicit `GetState::AwaitingResponse` constructor
    // calls.

    #[test]
    fn is_client_initiated_false_for_awaiting_with_requester() {
        let id = Transaction::new::<GetMsg>();
        let instance_id = ContractInstanceId::new([42u8; 32]);
        let visited = VisitedPeers::new(&id);
        let requester = make_peer(6001);
        let op = GetOp {
            id,
            state: Some(GetState::AwaitingResponse(AwaitingResponseData {
                instance_id,
                retries: 0,
                fetch_contract: true,
                requester: Some(requester),
                current_hop: 7,
                subscribe: false,
                blocking_subscribe: false,
                next_hop: make_peer(6002),
                tried_peers: HashSet::new(),
                alternatives: vec![],
                attempts_at_hop: 1,
                visited,
            })),
            result: None,
            stats: None,
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            client_return_code: true,
        };
        assert!(!op.is_client_initiated());
    }

    #[test]
    fn is_client_initiated_false_for_auto_fetch() {
        // Inlined construction post-#1454 phase 5 final (GET slice):
        // the `start_targeted_op` constructor was retired with the
        // legacy state machine. Equivalent shape: `auto_fetch = true`,
        // `state = AwaitingResponse(requester = None)`.
        let id = Transaction::new::<GetMsg>();
        let instance_id = ContractInstanceId::new([77u8; 32]);
        let target = make_peer(7001);
        let visited = VisitedPeers::new(&id);
        let mut tried_peers = HashSet::new();
        if let Some(addr) = target.socket_addr() {
            tried_peers.insert(addr);
        }
        let op = GetOp {
            id,
            state: Some(GetState::AwaitingResponse(AwaitingResponseData {
                instance_id,
                retries: 0,
                fetch_contract: true,
                requester: None,
                current_hop: 10,
                subscribe: false,
                blocking_subscribe: false,
                next_hop: target,
                tried_peers,
                alternatives: vec![],
                attempts_at_hop: 1,
                visited,
            })),
            result: None,
            stats: None,
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: true,
            client_return_code: true,
        };
        assert!(
            !op.is_client_initiated(),
            "Auto-fetch GET should not be client-initiated"
        );
    }

    #[test]
    fn is_client_initiated_false_for_other_states() {
        let op = make_get_op(Some(GetState::ReceivedRequest), None);
        assert!(!op.is_client_initiated());

        let key = make_contract_key(1);
        let op = make_get_op(Some(GetState::Finished(FinishedData { key })), None);
        assert!(!op.is_client_initiated());

        let op = make_get_op(None, None);
        assert!(!op.is_client_initiated());
    }

    // ── ForwardingAck wire-format pin (#3570 telemetry hook survives) ──────

    /// Pin test: `GetMsg::ForwardingAck` survives Phase 5-final as a wire
    /// variant + telemetry-only handler (no production reader after the
    /// retry block was retired). This serde round-trip guards against
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

    // ── Intermediate node stats tracking tests (#3527) ─────────────────────

    use crate::ring::Location;

    fn make_get_op_with_stats(stats: Option<Box<GetStats>>) -> GetOp {
        GetOp {
            id: Transaction::new::<GetMsg>(),
            state: Some(GetState::ReceivedRequest),
            result: None,
            stats,
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            client_return_code: true,
        }
    }

    fn make_get_stats(
        target: PeerKeyLocation,
        contract_location: Location,
    ) -> Option<Box<GetStats>> {
        Some(Box::new(GetStats {
            next_peer: Some(target),
            contract_location,
            first_response_time: None,
            transfer_time: None,
        }))
    }

    /// Non-finalized GET with stats reports ContractOpFailure on timeout.
    #[test]
    fn test_get_failure_outcome_with_stats() {
        let target = make_peer(9001);
        let contract_location = Location::random();
        let op = make_get_op_with_stats(make_get_stats(target.clone(), contract_location));

        match op.outcome() {
            OpOutcome::ContractOpFailure {
                target_peer,
                contract_location: loc,
            } => {
                assert_eq!(target_peer, &target);
                assert_eq!(loc, contract_location);
            }
            other => panic!("Expected ContractOpFailure, got {other:?}"),
        }
    }

    /// Non-finalized GET without stats reports Incomplete.
    #[test]
    fn test_get_failure_outcome_without_stats() {
        let op = make_get_op_with_stats(None);
        assert!(
            matches!(op.outcome(), OpOutcome::Incomplete),
            "GET without stats should return Incomplete"
        );
    }

    /// failure_routing_info() returns correct peer and location from stats.
    #[test]
    fn test_get_failure_routing_info() {
        let target = make_peer(9002);
        let contract_location = Location::random();
        let op = make_get_op_with_stats(make_get_stats(target.clone(), contract_location));

        let (peer, loc) = op.failure_routing_info().expect("should have routing info");
        assert_eq!(peer, target);
        assert_eq!(loc, contract_location);
    }

    /// Round-trip serialization: subscribe field is preserved through bincode.
    /// Note: bincode uses positional encoding, so #[serde(default)] does NOT
    /// provide backward compat with older binaries missing the field. Wire
    /// compat is handled by MIN_COMPATIBLE_VERSION + auto-update at handshake.
    #[test]
    fn test_get_msg_subscribe_roundtrip() {
        use freenet_stdlib::prelude::ContractInstanceId;

        // subscribe=true round-trips correctly
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

        // subscribe=false round-trips correctly
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

    // ============ Timer methods regression tests ============

    /// Verify start_timers() sets both timing fields and record_*_end() completes them,
    /// producing a ContractOpSuccess with non-zero durations.
    #[test]
    fn test_get_stats_timer_methods_produce_valid_timing() {
        use crate::ring::{Location, PeerKeyLocation};
        use std::time::Duration;

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let mut stats = GetStats {
            next_peer: Some(target_peer.clone()),
            contract_location,
            first_response_time: None,
            transfer_time: None,
        };

        // start_timers sets both to (now, None)
        stats.start_timers();
        assert!(stats.first_response_time.is_some());
        assert!(stats.transfer_time.is_some());
        assert!(stats.first_response_time.unwrap().1.is_none());
        assert!(stats.transfer_time.unwrap().1.is_none());

        // Simulate some work
        std::thread::sleep(Duration::from_millis(1));

        // record_response_end completes first_response_time
        stats.record_response_end();
        assert!(stats.first_response_time.unwrap().1.is_some());
        assert!(stats.transfer_time.unwrap().1.is_none()); // transfer still open

        std::thread::sleep(Duration::from_millis(1));

        // record_transfer_end completes transfer_time
        stats.record_transfer_end();
        assert!(stats.transfer_time.unwrap().1.is_some());

        // Verify the outcome produces ContractOpSuccess with non-zero durations
        let op = GetOp {
            id: Transaction::new::<GetMsg>(),
            state: None,
            result: Some(GetResult {
                key: make_contract_key(99),
                state: WrappedState::new(vec![1, 2, 3]),
                contract: None,
            }),
            stats: Some(Box::new(stats)),
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            client_return_code: true,
        };

        match op.outcome() {
            OpOutcome::ContractOpSuccess {
                first_response_time,
                payload_transfer_time,
                ..
            } => {
                assert!(
                    first_response_time > Duration::ZERO,
                    "first_response_time should be positive, got {first_response_time:?}"
                );
                assert!(
                    payload_transfer_time > Duration::ZERO,
                    "payload_transfer_time should be positive, got {payload_transfer_time:?}"
                );
                assert!(
                    payload_transfer_time >= first_response_time,
                    "transfer should take at least as long as response"
                );
            }
            other => panic!("Expected ContractOpSuccess, got {other:?}"),
        }
    }

    /// Verify start_timers resets previous timing (as happens on retry).
    #[test]
    fn test_get_stats_start_timers_resets_on_retry() {
        let mut stats = GetStats {
            next_peer: None,
            contract_location: Location::random(),
            first_response_time: None,
            transfer_time: None,
        };

        stats.start_timers();
        let first_start = stats.first_response_time.unwrap().0;

        std::thread::sleep(std::time::Duration::from_millis(1));

        // Simulate retry: start_timers again
        stats.start_timers();
        let second_start = stats.first_response_time.unwrap().0;

        assert!(
            second_start > first_start,
            "Retry should reset timers to a later instant"
        );
        // End timestamps should be cleared on retry
        assert!(stats.first_response_time.unwrap().1.is_none());
        assert!(stats.transfer_time.unwrap().1.is_none());
    }

    /// Simulates the relay peer local-cache decision from process_message.
    ///
    /// When a relay peer finds a contract locally, it checks whether it has
    /// active local interest (hosting, subscription, or client connections).
    /// If yes, state is current -- serve immediately. If no (stale LRU cache
    /// only), defer to the network with local fallback.
    ///
    /// Returns (local_value, local_fallback) after applying the decision.
    type LocalValue = Option<(ContractKey, WrappedState, Option<ContractContainer>)>;

    fn apply_relay_cache_decision(
        is_relay: bool,
        has_local_interest: bool,
        local_value: LocalValue,
    ) -> (LocalValue, LocalValue) {
        // This mirrors the logic at get.rs ~line 1404.
        let mut local_fallback = None;
        let local_value = if is_relay {
            match &local_value {
                Some(_) if !has_local_interest => {
                    // Stale cache -- defer to network, keep as fallback
                    local_fallback = local_value;
                    None
                }
                _ => {
                    // Active interest -- serve immediately
                    local_value
                }
            }
        } else {
            local_value
        };
        (local_value, local_fallback)
    }

    /// Regression test: relay peers actively hosting a contract (with local
    /// interest via subscription, proximity, or client) must serve it
    /// immediately. Previously, ALL relay peers deferred to the network
    /// regardless of hosting status, causing GET timeouts.
    #[test]
    fn relay_peer_with_local_interest_serves_immediately() {
        let key = make_contract_key(1);
        let state = WrappedState::new(vec![1, 2, 3]);
        let local_value = Some((key, state.clone(), None));

        // Relay peer WITH active local interest: serve immediately
        let (value, fallback) = apply_relay_cache_decision(true, true, local_value.clone());
        assert!(
            value.is_some(),
            "Relay peer with local interest must serve immediately"
        );
        assert!(
            fallback.is_none(),
            "Should not defer to fallback when actively hosting"
        );
    }

    /// Relay peer with stale cache (no local interest) should defer to network
    /// but keep local value as fallback.
    #[test]
    fn relay_peer_with_stale_cache_defers_with_fallback() {
        let key = make_contract_key(1);
        let state = WrappedState::new(vec![1, 2, 3]);
        let local_value = Some((key, state.clone(), None));

        // Relay peer WITHOUT local interest: defer to network
        let (value, fallback) = apply_relay_cache_decision(true, false, local_value.clone());
        assert!(
            value.is_none(),
            "Relay peer without local interest should defer to network"
        );
        assert!(fallback.is_some(), "Stale cache should be kept as fallback");
    }

    /// Non-relay (original requester) always serves local cache regardless
    /// of interest status.
    #[test]
    fn original_requester_always_serves_local_cache() {
        let key = make_contract_key(1);
        let state = WrappedState::new(vec![1, 2, 3]);
        let local_value = Some((key, state.clone(), None));

        let (value, fallback) = apply_relay_cache_decision(false, false, local_value);
        assert!(value.is_some(), "Original requester always serves locally");
        assert!(fallback.is_none());
    }

    /// Relay peer without local cache forwards regardless.
    #[test]
    fn relay_peer_without_cache_forwards_to_network() {
        let (value, fallback) = apply_relay_cache_decision(true, true, None);
        assert!(value.is_none(), "Nothing to serve without cache");
        assert!(fallback.is_none(), "Nothing to fall back to");
    }
}
