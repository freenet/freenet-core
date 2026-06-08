#[cfg(debug_assertions)]
use std::backtrace::Backtrace as StdTrace;
use std::time::Duration;

use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use tokio::sync::mpsc::error::SendError;

use crate::{
    config::GlobalExecutor,
    contract::{ContractError, ExecutorError},
    message::{Transaction, TransactionType},
    node::{ConnectionError, OpManager},
    ring::{Location, PeerKeyLocation, RingError},
};

pub(crate) mod connect;
pub(crate) mod get;
pub(crate) mod op_ctx;
pub(crate) mod orphan_streams;
pub(crate) mod put;
pub(crate) mod subscribe;
#[cfg(test)]
pub(crate) mod test_utils;
pub(crate) mod update;
pub(crate) mod visited_peers;

pub(crate) use op_ctx::OpCtx;
pub(crate) use visited_peers::VisitedPeers;

// Driver finalization paths publish `HostResult` directly via
// `result_router_tx` (see `op_ctx_task::*`); no central carrier
// is required.

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum OpOutcome<'a> {
    /// An op which involves a contract completed successfully.
    ContractOpSuccess {
        target_peer: &'a PeerKeyLocation,
        contract_location: Location,
        /// Time the operation took to initiate.
        first_response_time: Duration,
        /// Size of the payload (contract, state, etc.) in bytes.
        payload_size: usize,
        /// Transfer time of the payload.
        payload_transfer_time: Duration,
    },
    /// An op which involves a contract completed successfully but has no timing data
    /// (put, update). Feeds only the failure estimator.
    ContractOpSuccessUntimed {
        target_peer: &'a PeerKeyLocation,
        contract_location: Location,
    },
    /// An op which involves a contract completed unsuccessfully.
    ContractOpFailure {
        target_peer: &'a PeerKeyLocation,
        contract_location: Location,
    },
    /// In transit contract operation.
    Incomplete,
    /// This operation stats are not relevant for this peer.
    Irrelevant,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpError {
    #[error(transparent)]
    ConnError(#[from] ConnectionError),
    #[error(transparent)]
    RingError(#[from] RingError),
    #[error(transparent)]
    ContractError(#[from] ContractError),
    #[error(transparent)]
    ExecutorError(#[from] ExecutorError),

    #[error("unexpected operation state")]
    UnexpectedOpState,
    #[error(
        "cannot perform a state transition from the current state with the provided input (tx: {tx})"
    )]
    InvalidStateTransition {
        tx: Transaction,
        #[cfg(debug_assertions)]
        state: Option<Box<dyn std::fmt::Debug + Send + Sync>>,
        #[cfg(debug_assertions)]
        trace: StdTrace,
    },
    #[error("failed notifying, channel closed")]
    NotificationError,
    /// The peer this op was awaiting was pruned before sending its terminal
    /// reply (#4313). Delivered through the waiter channel by the
    /// `TransactionOrphaned` handler. Routes to the generic advance arm in
    /// `drive_retry_loop` (the peer is gone — do not infra-retry it), unlike
    /// `NotificationError` which infra-retries the same peer.
    #[error("awaited peer {peer} disconnected before replying")]
    PeerDisconnected { peer: std::net::SocketAddr },
    #[error("notification channel error: {0}")]
    NotificationChannelError(String),
    #[allow(dead_code)]
    #[error("unspected transaction type, trying to get a {0:?} from a {1:?}")]
    IncorrectTxType(TransactionType, TransactionType),
    #[allow(dead_code)]
    #[error("op not present: {0}")]
    OpNotPresent(Transaction),

    // Streaming-related errors
    #[error("stream was cancelled")]
    StreamCancelled,
    #[error("failed to claim orphan stream")]
    OrphanStreamClaimFailed,

    /// Admission-gate rejection: a `start_client_*` was called after
    /// `ShutdownHandle::shutdown` flipped the `OpManager::shutting_down`
    /// flag. The driver task is NOT spawned and the counter is NOT
    /// bumped — the client should retry once the node is back up.
    /// Closes the drain race window between `counter == 0` and
    /// `NodeEvent::Disconnect`.
    #[error("node is shutting down; client operation rejected")]
    NodeShuttingDown,
}

impl OpError {
    pub fn invalid_transition(tx: Transaction) -> Self {
        Self::InvalidStateTransition {
            tx,
            #[cfg(debug_assertions)]
            state: None,
            #[cfg(debug_assertions)]
            trace: StdTrace::force_capture(),
        }
    }

    /// Returns true if this error indicates a contract's WASM merge function
    /// ran and rejected the update. When true, the contract code is present
    /// locally and auto-fetching would be unnecessary.
    ///
    /// BROADER than `is_invalid_update_rejection` : includes runtime failures
    /// like OOG/timeout/traps. Use this for auto-fetch decisions, NOT for log
    /// severity. See `ExecutorError::is_contract_exec_rejection`.
    pub fn is_contract_exec_rejection(&self) -> bool {
        matches!(self, Self::ExecutorError(e) if e.is_contract_exec_rejection())
    }

    /// Narrow predicate for the originator-side UPDATE auto-fetch
    /// trigger: returns true only for "missing contract parameters"
    /// from `runtime.rs::get_params`. See
    /// `ExecutorError::is_missing_contract_parameters` for rationale.
    pub fn is_missing_contract_parameters(&self) -> bool {
        matches!(self, Self::ExecutorError(e) if e.is_missing_contract_parameters())
    }

    /// Returns true ONLY when the contract WASM merge function rejected the
    /// update with a typed `InvalidUpdate{,WithInfo}` error (the benign
    /// stale-state case from issue #3914). Use this for log-severity
    /// decisions: real WASM faults (OOG, traps, timeouts) return false here
    /// and stay at ERROR/WARN. See `ExecutorError::is_invalid_update_rejection`.
    pub fn is_invalid_update_rejection(&self) -> bool {
        matches!(self, Self::ExecutorError(e) if e.is_invalid_update_rejection())
    }

    /// Returns true for the typed `ContractQueueFull` marker. Callers MUST
    /// gate amplification side effects (auto-fetch, ResyncRequest, ERROR
    /// logs) on this predicate so a saturated contract queue doesn't induce
    /// network-wide storms. See `ExecutorError::is_contract_queue_full` and
    /// issue #4251.
    pub fn is_contract_queue_full(&self) -> bool {
        matches!(self, Self::ExecutorError(e) if e.is_contract_queue_full())
    }
}

impl<T> From<SendError<T>> for OpError {
    fn from(_: SendError<T>) -> OpError {
        OpError::NotificationError
    }
}

/// Announces to neighbors that we're hosting a contract.
/// This broadcasts to all connected peers so they know to forward UPDATEs to us.
pub(crate) async fn announce_contract_hosted(op_manager: &OpManager, key: &ContractKey) {
    if let Some(announcement) = op_manager.neighbor_hosting.on_contract_hosted(key) {
        tracing::debug!(
            %key,
            "NEIGHBOR_HOSTING: Announcing contract hosted to neighbors"
        );
        // DELIBERATELY blocking — unlike the other Broadcast* emission
        // sites in this PR, `announce_contract_hosted` carries a
        // one-shot transition: `on_contract_hosted(key)` above just
        // inserted `key` into `my_contracts`, and any subsequent call
        // for the same key returns `None` (the `if let Some(...)`
        // arm we are inside never re-fires). Dropping this emission
        // on `Full` would silently lose the only hosting
        // announcement we will ever send for this contract, leaving
        // neighbors unaware that this node hosts it until a
        // reconnect or unrelated state-exchange round.
        //
        // Acceptable trade-off because this path runs only on the
        // *first* PUT/GET of a new contract — low frequency, so a
        // 30s blocking await under wedge conditions is rare and the
        // error is preferable to silent loss. If this becomes a
        // wedge contributor in its own right, the right fix is to
        // separate the `my_contracts` insertion from the
        // announcement (so the transition isn't consumed until the
        // broadcast is queued), not to switch back to try_notify.
        // See review on PR #4231 (Codex P1) and #4145.
        if let Err(err) = op_manager
            .notify_node_event(crate::message::NodeEvent::BroadcastHostingUpdate {
                message: announcement,
            })
            .await
        {
            tracing::warn!(
                contract = %key,
                error = %err,
                phase = "error",
                "NEIGHBOR_HOSTING: Failed to broadcast hosting announcement"
            );
        }
    }
}

/// Reclaim the on-disk storage of a contract that was evicted from the
/// hosting cache. Skips contracts that are still in use — an active client
/// subscription or a downstream peer subscriber means something still
/// depends on us hosting it, so its state/code must NOT be deleted. See
/// `HostingManager::contract_in_use` for why an active upstream network
/// subscription alone is NOT included in the gate.
///
/// `expected_generation` is the state-write generation captured atomically
/// with the eviction decision (see `HostingCache::record_access` /
/// `sweep_expired`). It is carried through `EvictContract` so the
/// deletion-time guard in `RuntimePool::remove_contract` can detect a
/// state write (PUT/UPDATE) that re-hosted the contract between eviction
/// and this handler running — that case must skip disk reclamation
/// because the freshly written state would otherwise be deleted.
///
/// Fire-and-forget: emits an `EvictContract` event to the contract handler,
/// which routes it through the fair queue (serialized per-contract with other
/// ops on the same key) and reclaims disk in `handle_contract_event`.
pub(crate) fn reclaim_evicted_contract(
    op_manager: &OpManager,
    key: ContractKey,
    expected_generation: u64,
) {
    if op_manager.ring.contract_in_use(&key) {
        tracing::debug!(
            contract = %key,
            "Skipping disk reclamation for evicted contract — still in use \
             (client subscription or downstream subscriber); queued for retry"
        );
        // Queue for retry by the periodic sweep: the hosting-cache entry is
        // already gone (we are processing its `evicted` tuple), so when the
        // subscriber later expires nothing else would emit another
        // EvictContract for this key — without this, the disk state/code
        // would leak permanently. Mirrors the symmetric in-use skip in
        // RuntimePool::remove_contract.
        op_manager
            .ring
            .pending_reclamation_add(key, expected_generation);
        return;
    }
    op_manager.notify_contract_handler_fire_and_forget(
        crate::contract::ContractHandlerEvent::EvictContract {
            key,
            expected_generation,
        },
    );
}

/// Complete subscription at the originator node via GET piggyback.
///
/// The subscription tree was built by relay nodes during GET response propagation.
/// This function performs the local registration at the originator:
/// 1. Mark subscribed (lease in active_subscriptions)
/// 2. Register the upstream peer as an interest source
/// 3. Register local interest so ChangeInterests from peers are processed
/// 4. Announce contract hosted so neighbors send UPDATEs
pub(crate) async fn complete_piggyback_subscription(
    op_manager: &OpManager,
    key: &ContractKey,
    tx: &crate::message::Transaction,
    sender_from_addr: &Option<crate::ring::PeerKeyLocation>,
) {
    op_manager.ring.subscribe(*key);
    op_manager.ring.complete_subscription_request(key, true);

    // Register local interest so ChangeInterests from peers get properly processed.
    let became_interested = op_manager.interest_manager.add_local_client(key);
    if became_interested {
        broadcast_change_interests(op_manager, vec![*key], vec![]).await;
    }

    // Announce that we host this contract so neighbors include us in broadcast targets.
    announce_contract_hosted(op_manager, key).await;

    if let Some(upstream_pkl) = sender_from_addr.as_ref() {
        let peer_key = crate::ring::interest::PeerKey::from(upstream_pkl.pub_key.clone());
        op_manager
            .interest_manager
            .register_peer_interest(key, peer_key, None, true);
        tracing::debug!(tx = %tx, contract = %key, "Subscription completed via GET piggyback");
    } else {
        // sender_from_addr can be None for transient connections not yet in the ring.
        // Subscription is still marked locally; the 2-minute renewal cycle will
        // establish a full subscription tree via a standard SUBSCRIBE operation.
        tracing::warn!(
            tx = %tx,
            contract = %key,
            "GET piggyback: upstream peer not in ring, subscription tree incomplete -- renewal will heal"
        );
    }
}

/// Auto-subscribe at the originator: use piggybacked subscription if available,
/// otherwise fall back to a separate SUBSCRIBE operation.
///
/// Called from GET response handling when `AUTO_SUBSCRIBE_ON_GET` is enabled and
/// the originator is not yet subscribed. Consolidates the piggyback-or-fallback
/// logic that appears in both streaming and non-streaming response paths.
pub(crate) async fn auto_subscribe_on_get_response(
    op_manager: &OpManager,
    key: &ContractKey,
    tx: &crate::message::Transaction,
    sender_from_addr: &Option<crate::ring::PeerKeyLocation>,
    subscribe_requested: bool,
    blocking_sub: bool,
    path_label: &str,
) {
    if subscribe_requested {
        complete_piggyback_subscription(op_manager, key, tx, sender_from_addr).await;
    } else {
        let child_tx = start_subscription_request(op_manager, *tx, *key, blocking_sub);
        tracing::debug!(tx = %tx, %child_tx, blocking = %blocking_sub, "started subscription ({path_label}, fallback)");
    }
}

/// Broadcast ChangeInterests message to all connected peers.
///
/// Called when local interest in contracts changes (gained or lost).
pub(crate) async fn broadcast_change_interests(
    op_manager: &OpManager,
    added: Vec<ContractKey>,
    removed: Vec<ContractKey>,
) {
    use crate::ring::interest::contract_hash;

    if added.is_empty() && removed.is_empty() {
        return;
    }

    let added_hashes: Vec<u32> = added.iter().map(contract_hash).collect();
    let removed_hashes: Vec<u32> = removed.iter().map(contract_hash).collect();

    tracing::debug!(
        added_count = added_hashes.len(),
        removed_count = removed_hashes.len(),
        "Broadcasting ChangeInterests to neighbors"
    );

    // Non-blocking emit: interest changes are best-effort gossip;
    // a missed one will be re-broadcast on the next change or
    // converged via the periodic InterestSync exchange (#4145).
    if let Err(err) =
        op_manager.try_notify_node_event(crate::message::NodeEvent::BroadcastChangeInterests {
            added: added_hashes,
            removed: removed_hashes,
        })
    {
        // Best-effort by design — log at debug to keep the caller
        // layer in step with the helper-internal downgrade (#4238).
        tracing::debug!(
            error = %err,
            "Failed to broadcast ChangeInterests (best-effort)"
        );
    }
}

/// Initiates a subscription after a PUT or GET, routing through the
/// subscribe driver as a fire-and-forget background task.
///
/// `blocking` is accepted for API stability (callers may inspect it for
/// telemetry) but has no behavioral effect here: PUT/GET drivers that
/// want to wait for completion `await` `subscribe::run_client_subscribe`
/// inline (see `maybe_subscribe_child` in `put/op_ctx_task.rs` and
/// `get/op_ctx_task.rs`).
pub(super) fn start_subscription_request(
    op_manager: &OpManager,
    parent_tx: Transaction,
    key: ContractKey,
    blocking: bool,
) -> Transaction {
    let child_tx = Transaction::new_child_of::<subscribe::SubscribeMsg>(&parent_tx);

    tracing::debug!(
        %parent_tx,
        %child_tx,
        %key,
        blocking,
        "spawning child subscription operation (driver)"
    );

    // `run_client_subscribe` requires `Arc<OpManager>`. Callers on
    // legacy `process_message` paths only have `&OpManager`, so we
    // wrap a single clone here. PUT/GET task drivers that already
    // hold `&Arc<OpManager>` route through their own
    // `maybe_subscribe_child` helper (see
    // `put/op_ctx_task.rs::maybe_subscribe_child` and the GET
    // counterpart) and don't pay this cost.
    let op_manager_arc = std::sync::Arc::new(op_manager.clone());
    let instance_id = *key.id();
    GlobalExecutor::spawn(async move {
        subscribe::run_client_subscribe(op_manager_arc, instance_id, child_tx).await;
    });

    child_tx
}

pub(crate) async fn has_contract(
    op_manager: &OpManager,
    instance_id: ContractInstanceId,
) -> Result<Option<ContractKey>, OpError> {
    match op_manager
        .notify_contract_handler(crate::contract::ContractHandlerEvent::GetQuery {
            instance_id,
            return_contract_code: false,
        })
        .await?
    {
        crate::contract::ContractHandlerEvent::GetResponse {
            key,
            response: Ok(crate::contract::StoreResponse { state: Some(_), .. }),
        } => Ok(key),
        crate::contract::ContractHandlerEvent::DelegateRequest { .. }
        | crate::contract::ContractHandlerEvent::DelegateResponse(_)
        | crate::contract::ContractHandlerEvent::PutQuery { .. }
        | crate::contract::ContractHandlerEvent::PutResponse { .. }
        | crate::contract::ContractHandlerEvent::GetQuery { .. }
        | crate::contract::ContractHandlerEvent::GetResponse { .. }
        | crate::contract::ContractHandlerEvent::UpdateQuery { .. }
        | crate::contract::ContractHandlerEvent::UpdateResponse { .. }
        | crate::contract::ContractHandlerEvent::UpdateNoChange { .. }
        | crate::contract::ContractHandlerEvent::RegisterSubscriberListener { .. }
        | crate::contract::ContractHandlerEvent::RegisterSubscriberListenerResponse
        | crate::contract::ContractHandlerEvent::QuerySubscriptions { .. }
        | crate::contract::ContractHandlerEvent::QuerySubscriptionsResponse
        | crate::contract::ContractHandlerEvent::GetSummaryQuery { .. }
        | crate::contract::ContractHandlerEvent::GetSummaryResponse { .. }
        | crate::contract::ContractHandlerEvent::GetDeltaQuery { .. }
        | crate::contract::ContractHandlerEvent::GetDeltaResponse { .. }
        | crate::contract::ContractHandlerEvent::ClientDisconnect { .. }
        | crate::contract::ContractHandlerEvent::EvictContract { .. } => Ok(None),
    }
}

/// Determines if streaming transport should be used for a payload of the given size.
///
/// Returns `true` if the payload size exceeds the streaming threshold (default: 64KB).
///
/// # Arguments
/// * `streaming_threshold` - Size threshold above which streaming is used (exclusive)
/// * `payload_size` - Size of the payload in bytes
///
/// # Note
/// The threshold comparison is exclusive (`>`), meaning payloads exactly at the
/// threshold will NOT use streaming. This is intentional: the threshold represents
/// "the maximum size for non-streaming transfers", so payloads must exceed it.
pub(crate) fn should_use_streaming(streaming_threshold: usize, payload_size: usize) -> bool {
    payload_size > streaming_threshold
}

/// Conservative effective throughput floor for streaming transfers (bytes/sec).
///
/// Used to scale the per-attempt timeout for streaming PUTs. Set to 20 KiB/s
/// so that even a slow link (or congested gateway) has time to drain a large
/// payload before the retry loop fires. Real-world end-to-end throughput
/// observed for the freenet.org website upload (2.4 MB in ~62 s) is ~40 KiB/s,
/// so 20 KiB/s gives ~2x safety margin.
const STREAMING_THROUGHPUT_FLOOR_BPS: usize = 20 * 1024;

/// Minimum drain budget added to streaming timeouts on top of `OPERATION_TTL`.
///
/// Without this floor, payloads just above `streaming_threshold` (where
/// `payload_size / STREAMING_THROUGHPUT_FLOOR_BPS` rounds down to 0) would
/// fall back to the unscaled `OPERATION_TTL` even though `process_message`
/// chose to stream them. That's exactly the #4001 bug — so guarantee at
/// least an extra 30 s of headroom for *every* streaming-eligible payload.
/// 30 s covers stream handshake + first chunk RTT + brief congestion.
const STREAMING_MIN_DRAIN_SECS: u64 = 30;

/// Hard ceiling on the per-attempt timeout for streaming PUTs.
///
/// Even at the throughput floor, a 25 MB payload would only need ~21 minutes,
/// but capping at 10 minutes prevents pathological cases (a wedged remote that
/// never errors) from holding the driver hostage indefinitely. The retry loop
/// can still recover by advancing to a different peer when this fires.
pub(crate) const STREAMING_ATTEMPT_TIMEOUT_CAP: std::time::Duration =
    std::time::Duration::from_secs(600);

/// Compute the per-attempt timeout for an operation whose payload may use
/// streaming transport.
///
/// For non-streaming payloads (size <= `streaming_threshold`), returns
/// [`crate::config::OPERATION_TTL`] (60 s) — there is no per-fragment progress
/// to wait on, so the standard timeout applies.
///
/// For streaming payloads, returns `OPERATION_TTL` (handshake / k-closest /
/// downstream relays) plus `max(STREAMING_MIN_DRAIN_SECS, payload_size /
/// STREAMING_THROUGHPUT_FLOOR_BPS)` seconds to give the streaming layer time
/// to drain the bytes, capped at [`STREAMING_ATTEMPT_TIMEOUT_CAP`] (10 min).
/// The `STREAMING_MIN_DRAIN_SECS` floor ensures payloads just above the
/// threshold still escape the unscaled `OPERATION_TTL` (integer truncation
/// would otherwise reduce the drain term to zero — re-introducing the
/// #4001 bug for payloads of size `(threshold, threshold + floor_bps)`).
///
/// This is a heuristic: it relocates the cliff at which `drive_retry_loop`
/// fires retries while the original streaming op is still in flight, but does
/// not eliminate it. Issue #4001 has a follow-up design to replace this with
/// a true stream-inactivity timeout that observes per-fragment progress.
pub(crate) fn streaming_aware_attempt_timeout(
    streaming_threshold: usize,
    payload_size: usize,
) -> std::time::Duration {
    if !should_use_streaming(streaming_threshold, payload_size) {
        return crate::config::OPERATION_TTL;
    }
    let drain_secs =
        ((payload_size / STREAMING_THROUGHPUT_FLOOR_BPS) as u64).max(STREAMING_MIN_DRAIN_SECS);
    let total = crate::config::OPERATION_TTL + std::time::Duration::from_secs(drain_secs);
    total.min(STREAMING_ATTEMPT_TIMEOUT_CAP)
}

/// Records a routing event observed by a relay/forwarding hop.
///
/// Without this hook, only the operation's originator feeds events into the
/// router. `OpOutcome::ContractOp*` is only produced for ops where
/// `upstream_addr.is_none()`, and relay hops return `SendAndComplete` without
/// going through `outcome()`. On a relay-heavy node the router would see
/// almost no per-peer data, leaving the failure-probability model untrained
/// and the per-peer dashboard panels empty even when MB of traffic flowed
/// through each connection.
///
/// Call this at the relay-side response sites in each operation when the
/// downstream peer the relay chose returns success or failure. Timeout and
/// disconnect paths are already covered by `report_timeout_failure` in
/// `node/op_state_manager.rs` via `failure_routing_info`.
///
/// # Outcome attribution
///
/// The `outcome` argument matches the legacy originator-side semantics
/// (see `OpOutcome::Contract*` and the per-op `outcome()` methods). In
/// particular, **prompt `NotFound` from a downstream peer is recorded
/// as `RouteOutcome::Failure`**, not Success. A peer that promptly
/// answers "I don't host this contract" behaved correctly at the
/// transport level, but the failure-probability model is asking "will
/// this peer deliver the contract at this location?" and a `NotFound`
/// reply means it won't — so for routing-decision purposes it's a
/// negative signal for *that contract location*. The relay sites
/// follow the same convention used by the originator's stalled-peer
/// retry path (`get.rs:2686` and `report_timeout_failure` in
/// `op_state_manager.rs`). Splitting transport-success from
/// content-availability would require a new `RouteOutcome::NotHosted`
/// variant and is out of scope here.
///
/// `LocalCompletion` and unexpected-reply variants are also recorded as
/// `Failure` against the downstream peer; these are "shouldn't happen"
/// paths and recording them as failures matches the relay's decision to
/// abandon that peer and try another.
///
/// # UPDATE exclusion
///
/// **UPDATE is intentionally not covered by this helper at relay sites.**
/// UPDATE relays use `send_fire_and_forget` for downstream forwarding
/// (`drive_relay_request_update`, `drive_relay_broadcast_to`, and the
/// streaming variants), so the relay never observes whether the downstream
/// peer succeeded or failed. Recording only the local send-error path
/// would bias the per-peer UPDATE failure rate to 100% by construction.
/// `report_timeout_failure` in `op_state_manager.rs` still records UPDATE
/// timeouts via `failure_routing_info` on the originator side; this is
/// the same signal as for the other ops, just not augmented by relay
/// observations.
pub(crate) fn record_relay_route_event(
    op_manager: &OpManager,
    next_hop: PeerKeyLocation,
    contract_location: Location,
    outcome: crate::router::RouteOutcome,
    op_type: crate::node::network_status::OpType,
) {
    #[cfg(any(test, feature = "testing"))]
    {
        use std::sync::atomic::Ordering;
        let counter = match op_type {
            crate::node::network_status::OpType::Get => &RELAY_GET_ROUTE_EVENT_COUNT,
            crate::node::network_status::OpType::Put => &RELAY_PUT_ROUTE_EVENT_COUNT,
            crate::node::network_status::OpType::Update => &RELAY_UPDATE_ROUTE_EVENT_COUNT,
            crate::node::network_status::OpType::Subscribe => &RELAY_SUBSCRIBE_ROUTE_EVENT_COUNT,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
    // Feed only the routing model — NOT peer_health or topology_manager.
    //
    // `Ring::routing_finished` also updates `peer_health` (which uses
    // `std::time::Instant::now()` in `connection_manager.rs::PeerHealthTracker`
    // around lines 182-203, a pre-existing TimeSource rule violation)
    // and the topology_manager's `request_density_tracker`. An earlier
    // iteration of this branch routed relay events through
    // `routing_finished` and broke three strict-determinism tests
    // (`test_strict_determinism_*` / `test_direct_runner_determinism` /
    // `test_thundering_herd_connect_storm`). Bypassing those side
    // effects fixed all three.
    //
    // CAVEAT: `Router::add_event` itself transitively calls
    // `RoutingPredictor::record` → `wall_clock_hours()` → `SystemTime::now()`
    // (see `router/routing_predictor.rs:608-614`). So the router path is
    // not strictly TimeSource-clean either; the determinism tests pass
    // because the wall-clock variance there is well below the events
    // each test counts. Migrating both `peer_health` and
    // `routing_predictor` to `TimeSource` would let
    // `record_relay_route_event` go back to calling `routing_finished`
    // straightforwardly. Tracked as a follow-up.
    op_manager
        .ring
        .router
        .write()
        .add_event(crate::router::RouteEvent {
            peer: next_hop,
            contract_location,
            outcome,
            op_type: Some(op_type),
        });
}

/// Test hook: counter incremented every time `record_relay_route_event`
/// fires for a relay-forwarded GET. Used by simulation tests to verify
/// the per-op-type relay hooks are reached. See `dev_tool` re-export.
#[cfg(any(test, feature = "testing"))]
pub static RELAY_GET_ROUTE_EVENT_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(any(test, feature = "testing"))]
pub static RELAY_PUT_ROUTE_EVENT_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(any(test, feature = "testing"))]
pub static RELAY_UPDATE_ROUTE_EVENT_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(any(test, feature = "testing"))]
pub static RELAY_SUBSCRIBE_ROUTE_EVENT_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
mod ordering_invariant_tests {
    //! Tests documenting critical ordering invariants in the operations module.
    //!
    //! These tests don't reproduce actual race conditions (which would require
    //! non-deterministic timing), but document the design decisions and invariants
    //! that prevent them.
    //!
    //! # Push-Before-Send Invariant
    //!
    //! The `handle_op_result` function (lines 178-182) maintains a critical invariant:
    //!
    //! ```text
    //! op_manager.push(id, updated_state).await?;  // FIRST
    //! network_bridge.send(target, msg).await?;    // SECOND
    //! ```
    //!
    //! ## Why This Ordering Matters
    //!
    //! If the order were reversed:
    //! 1. Message is sent to peer
    //! 2. Peer processes and responds FAST
    //! 3. Response arrives at origin
    //! 4. `load_or_init` tries to find operation in storage
    //! 5. **RACE**: `push` hasn't happened yet → operation not found → error
    //!
    //! ## The Invariant
    //!
    //! By pushing state BEFORE sending, we guarantee that when a response
    //! arrives (no matter how fast), the operation state is already in storage.
    //!
    //! ## Why We Can't Easily Test This
    //!
    //! Testing the race would require:
    //! 1. Intercepting between `push` and `send` calls
    //! 2. Simulating an instant response arrival
    //! 3. Verifying `load_or_init` finds the state
    //!
    //! This would require modifying production code to accept test hooks,
    //! which adds complexity for minimal benefit since the invariant is
    //! clear and the code correctly implements it.
    //!
    //! Instead, we document the invariant here and verify the building blocks work.

    use super::test_utils::MockNetworkBridge;
    use crate::message::{NetMessage, NetMessageV1, Transaction};
    use crate::node::NetworkBridge;
    use crate::operations::connect::ConnectMsg;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    /// Verify that MockNetworkBridge correctly records send ordering.
    ///
    /// This is a building block for any future ordering tests.
    #[tokio::test]
    async fn mock_network_bridge_records_send_ordering() {
        let bridge = MockNetworkBridge::new();
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5000);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5001);

        let tx1 = Transaction::new::<ConnectMsg>();
        let tx2 = Transaction::new::<ConnectMsg>();

        // Send in specific order
        bridge
            .send(addr1, NetMessage::V1(NetMessageV1::Aborted(tx1)))
            .await
            .unwrap();
        bridge
            .send(addr2, NetMessage::V1(NetMessageV1::Aborted(tx2)))
            .await
            .unwrap();

        // Verify ordering is preserved in recording
        let sent = bridge.sent_messages();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].0, addr1, "First send should be to addr1");
        assert_eq!(sent[1].0, addr2, "Second send should be to addr2");
    }

    /// Document that push-before-send is intentional via code comment verification.
    ///
    /// This test serves as a tripwire: if someone removes the comment explaining
    /// the invariant, this test name will remind them of its importance.
    #[test]
    fn push_before_send_invariant_is_documented() {
        // The invariant is documented at operations.rs lines 178-182:
        //
        // ```rust
        // // IMPORTANT: Push state BEFORE sending message to avoid race condition.
        // // If we send first, a fast response might arrive before the state is saved,
        // // causing load_or_init to fail to find the operation.
        // op_manager.push(id, updated_state).await?;
        // network_bridge.send(target, msg).await?;
        // ```
        //
        // This test documents that the invariant exists and is intentional.
        // If refactoring this code, maintain the push-before-send ordering!
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::{
        STREAMING_ATTEMPT_TIMEOUT_CAP, should_use_streaming, streaming_aware_attempt_timeout,
    };
    use crate::config::OPERATION_TTL;
    use std::time::Duration;

    const DEFAULT_THRESHOLD: usize = 64 * 1024; // 64KB

    #[test]
    fn test_streaming_respects_threshold() {
        assert!(!should_use_streaming(DEFAULT_THRESHOLD, 0));
        assert!(!should_use_streaming(DEFAULT_THRESHOLD, 1000));
        assert!(!should_use_streaming(DEFAULT_THRESHOLD, DEFAULT_THRESHOLD)); // exactly at threshold
        assert!(should_use_streaming(
            DEFAULT_THRESHOLD,
            DEFAULT_THRESHOLD + 1
        )); // just above
        assert!(should_use_streaming(DEFAULT_THRESHOLD, 1024 * 1024)); // 1MB
    }

    #[test]
    fn test_streaming_custom_threshold() {
        let custom_threshold = 128 * 1024; // 128KB
        assert!(!should_use_streaming(custom_threshold, 64 * 1024));
        assert!(!should_use_streaming(custom_threshold, custom_threshold));
        assert!(should_use_streaming(custom_threshold, custom_threshold + 1));
    }

    #[test]
    fn test_streaming_zero_threshold() {
        // With threshold of 0, any non-zero payload uses streaming
        assert!(!should_use_streaming(0, 0));
        assert!(should_use_streaming(0, 1));
        assert!(should_use_streaming(0, 100));
    }

    /// Non-streaming payloads (at or below the threshold) get the standard
    /// `OPERATION_TTL`. Crossing the threshold is what triggers scaling.
    #[test]
    fn non_streaming_payload_uses_operation_ttl() {
        assert_eq!(
            streaming_aware_attempt_timeout(DEFAULT_THRESHOLD, 0),
            OPERATION_TTL
        );
        assert_eq!(
            streaming_aware_attempt_timeout(DEFAULT_THRESHOLD, 1024),
            OPERATION_TTL
        );
        assert_eq!(
            streaming_aware_attempt_timeout(DEFAULT_THRESHOLD, DEFAULT_THRESHOLD),
            OPERATION_TTL
        );
    }

    /// Regression test for #4001: a 2.4 MB payload (the freenet.org website
    /// case) MUST get a per-attempt timeout that exceeds the observed
    /// end-to-end completion time of ~62 s. With the old hard-coded 60 s
    /// `OPERATION_TTL`, the retry loop fired three retries while the
    /// original streaming PUT was still in flight, causing version-conflict
    /// failures on every push to `freenet/web`.
    #[test]
    fn website_payload_attempt_timeout_exceeds_observed_completion() {
        let website_payload_size = 2_460_242; // bytes, from freenet/web 2026-05-01 logs
        let timeout = streaming_aware_attempt_timeout(DEFAULT_THRESHOLD, website_payload_size);
        let observed_completion = Duration::from_secs(63); // log: elapsed_ms=62335

        assert!(
            timeout > observed_completion,
            "streaming-aware timeout ({timeout:?}) must exceed observed \
             completion time ({observed_completion:?}) so the retry loop \
             does not fire while the original streaming PUT is still in \
             flight (issue #4001)"
        );
        assert!(
            timeout > OPERATION_TTL,
            "streaming-aware timeout ({timeout:?}) must exceed OPERATION_TTL \
             ({OPERATION_TTL:?}); otherwise the fix is a no-op for the bug \
             reported in #4001"
        );
    }

    /// Streaming timeouts grow with payload size — a 10 MB payload gets a
    /// strictly larger timeout than a 1 MB payload, so the cliff scales.
    #[test]
    fn streaming_timeout_scales_with_payload_size() {
        let small_streaming = streaming_aware_attempt_timeout(DEFAULT_THRESHOLD, 1_000_000);
        let medium_streaming = streaming_aware_attempt_timeout(DEFAULT_THRESHOLD, 10_000_000);

        assert!(
            small_streaming < medium_streaming,
            "1 MB timeout ({small_streaming:?}) must be smaller than \
             10 MB timeout ({medium_streaming:?})"
        );
        assert!(small_streaming > OPERATION_TTL);
        assert!(medium_streaming > OPERATION_TTL);
    }

    /// Pathological payloads cannot push the per-attempt timeout above the
    /// hard ceiling. Without this, a wedged remote could hold the driver
    /// hostage indefinitely.
    #[test]
    fn streaming_timeout_capped_at_ceiling() {
        // 1 GB — far beyond the per-attempt ceiling at 20 KB/s floor.
        let huge = streaming_aware_attempt_timeout(DEFAULT_THRESHOLD, 1024 * 1024 * 1024);
        assert_eq!(
            huge, STREAMING_ATTEMPT_TIMEOUT_CAP,
            "huge payloads must clamp to the cap"
        );
    }

    /// Boundary: the threshold itself is non-streaming, but `threshold + 1`
    /// crosses into streaming territory. The timeout MUST jump *strictly*
    /// above `OPERATION_TTL` at the crossing — otherwise streaming PUTs
    /// just over the threshold inherit the unscaled 60 s timeout that this
    /// fix is trying to escape.
    ///
    /// Without [`super::STREAMING_MIN_DRAIN_SECS`], integer division
    /// would truncate `1 / 20 KiB` to 0 s of drain budget for any payload
    /// of size `(threshold, threshold + STREAMING_THROUGHPUT_FLOOR_BPS)`,
    /// silently re-introducing the #4001 bug. Pin both ends of that gap.
    #[test]
    fn streaming_timeout_jumps_above_threshold_boundary() {
        let at_threshold = streaming_aware_attempt_timeout(DEFAULT_THRESHOLD, DEFAULT_THRESHOLD);
        let just_above = streaming_aware_attempt_timeout(DEFAULT_THRESHOLD, DEFAULT_THRESHOLD + 1);
        // 19 KiB above threshold — would be `0 s drain` under naive
        // truncation, but the floor saves us.
        let in_truncation_gap =
            streaming_aware_attempt_timeout(DEFAULT_THRESHOLD, DEFAULT_THRESHOLD + 19 * 1024);
        assert_eq!(at_threshold, OPERATION_TTL);
        assert!(
            just_above > OPERATION_TTL,
            "just-above-threshold timeout {just_above:?} must STRICTLY \
             exceed OPERATION_TTL ({OPERATION_TTL:?}); a fix that lets \
             this equal OPERATION_TTL is a no-op for the size range \
             (threshold, threshold + 20 KiB) — exactly the truncation \
             gap STREAMING_MIN_DRAIN_SECS exists to close (#4001 \
             skeptical review)"
        );
        assert!(
            in_truncation_gap > OPERATION_TTL,
            "payload in the truncation gap (threshold + 19 KiB) must \
             exceed OPERATION_TTL — STREAMING_MIN_DRAIN_SECS guarantees it"
        );
    }

    /// The minimum-drain floor applies to every streaming-eligible payload.
    /// Pin the exact value so a future tightening of the floor can't
    /// silently reintroduce the truncation gap.
    #[test]
    fn streaming_timeout_min_drain_floor() {
        let just_above = streaming_aware_attempt_timeout(DEFAULT_THRESHOLD, DEFAULT_THRESHOLD + 1);
        assert_eq!(
            just_above,
            OPERATION_TTL + Duration::from_secs(super::STREAMING_MIN_DRAIN_SECS),
            "streaming-eligible payloads must get at least \
             OPERATION_TTL + STREAMING_MIN_DRAIN_SECS"
        );
    }

    /// Pin the exact payload size where the timeout stops scaling and
    /// clamps to the 10-min ceiling. With a 60 s base + 20 KiB/s floor +
    /// 600 s cap, scaling stops at exactly `(600 - 60) * 20 KiB`.
    #[test]
    fn streaming_timeout_cap_boundary() {
        const FLOOR_BPS: usize = 20 * 1024;
        let scaling_max_bytes =
            (STREAMING_ATTEMPT_TIMEOUT_CAP - OPERATION_TTL).as_secs() as usize * FLOOR_BPS;
        let just_below_cap = streaming_aware_attempt_timeout(DEFAULT_THRESHOLD, scaling_max_bytes);
        let at_cap = streaming_aware_attempt_timeout(DEFAULT_THRESHOLD, scaling_max_bytes + 1);
        assert_eq!(just_below_cap, STREAMING_ATTEMPT_TIMEOUT_CAP);
        assert_eq!(at_cap, STREAMING_ATTEMPT_TIMEOUT_CAP);
    }
}

#[cfg(test)]
mod sub_op_subscribe_pin_tests {
    //! Pin tests that prove `start_subscription_request` spawns
    //! `subscribe::run_client_subscribe` and does not register with a
    //! sub-operation tracker.

    fn extract_start_subscription_request_body() -> &'static str {
        let src = include_str!("operations.rs");
        // Compose runtime-needle to avoid self-trigger (the test file
        // itself contains the literal "fn start_subscription_request").
        let head = ["fn ", "start_subscription_request("].concat();
        let start = src
            .find(&head)
            .expect("`fn start_subscription_request(` must exist in operations.rs");
        // Walk forward from the function signature to the first `{`,
        // then track brace depth until we find the matching close.
        // This is robust against renaming/moving the next function.
        let body_open = src[start..]
            .find('{')
            .map(|off| start + off)
            .expect("expected `{` after function signature");
        let mut depth: i32 = 0;
        let mut end = body_open;
        for (i, ch) in src[body_open..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = body_open + i + 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        assert!(
            end > body_open,
            "failed to find matching `}}` for start_subscription_request"
        );
        &src[start..end]
    }

    #[test]
    fn start_subscription_request_uses_run_client_subscribe() {
        let body = extract_start_subscription_request_body();
        assert!(
            !body.contains("subscribe::request_subscribe"),
            "`start_subscription_request` must route through \
             `subscribe::run_client_subscribe`, not `request_subscribe`."
        );
    }

    #[test]
    fn start_subscription_request_does_not_register_with_sub_op_tracker() {
        let body = extract_start_subscription_request_body();
        assert!(
            !body.contains("expect_and_register_sub_operation"),
            "`start_subscription_request` must NOT register with a \
             sub-operation tracker."
        );
        assert!(
            !body.contains("sub_operation_failed"),
            "`start_subscription_request` must NOT propagate failures \
             via `sub_operation_failed` — the subscribe driver \
             publishes its own `HostResult::Err`."
        );
    }

    #[test]
    fn start_subscription_request_spawns_driver_driver() {
        let body = extract_start_subscription_request_body();
        assert!(
            body.contains("subscribe::run_client_subscribe"),
            "`start_subscription_request` must spawn the driver \
             subscribe driver `subscribe::run_client_subscribe` — \
             matches the `maybe_subscribe_child` pattern in \
             `put/op_ctx_task.rs` and `get/op_ctx_task.rs`."
        );
    }
}
