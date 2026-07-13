//! SUBSCRIBE operation: registers downstream interest in a contract
//! and threads UPDATE broadcasts back through the subscription tree.
//!
//! Every SUBSCRIBE wire variant dispatches unconditionally:
//!
//! - `SubscribeMsg::Request` → `op_ctx_task::start_relay_subscribe`
//! - `SubscribeMsg::Response` → reply bypass to originator waiter
//! - `SubscribeMsg::Unsubscribe` → `handle_unsubscribe_inbound`
//! - `SubscribeMsg::ForwardingAck` → no-op telemetry hook
//!
//! The wire-format types, the originator-shared helpers
//! (`InitialRequest`, `prepare_initial_request`,
//! `complete_local_subscription`, `wait_for_contract_with_timeout`),
//! the inbound `Unsubscribe` handler, and
//! `register_downstream_subscriber` survive here because the
//! drivers consume them.

use either::Either;

pub(crate) use self::messages::{SubscribeMsg, SubscribeMsgResult};
use super::OpError;
use super::bootstrap::bootstrap_gateway_target;
use crate::ring::PeerKeyLocation;
use crate::tracing::NetEventLog;
use crate::{
    message::{InnerMessage, Transaction},
    node::OpManager,
    ring::Location,
};
use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, sleep};

/// Maximum peers to try per hop (breadth search).
/// (GET's former DEFAULT_MAX_BREADTH no longer exists — its selection
/// uses k=1 per advance; this constant is now SUBSCRIBE-only.)
pub(super) const MAX_BREADTH: usize = 3;

/// Maximum retry rounds (each round queries k_closest for new candidates).
/// (No longer tied to GET's MAX_RETRIES, which is 3; this constant is
/// SUBSCRIBE-only.)
pub(super) const MAX_RETRIES: usize = 10;

/// Base inter-attempt delay for the Phase 2b client-subscribe retry loop.
///
/// The task-per-transaction driver loops `send_and_await` → advance →
/// `send_and_await` with no time spent in the event loop between
/// attempts. Legacy got incidental backoff from cycling through the
/// event loop; the task-per-tx driver does not. Without a delay, a
/// contract no peer hosts lets one client subscribe burn
/// `MAX_BREADTH * MAX_RETRIES` (= 30) attempts as fast as the network
/// replies — a topology-wide DoS vector (see #3808).
///
/// 50ms is the issue's suggested ballpark: large enough to break the
/// tight reply-driven spin loop (worst case 30 attempts now span ~1.5s
/// of pacing instead of completing in a few network round-trips), small
/// enough to be negligible against the `OPERATION_TTL`-bounded per-attempt
/// cost of a real subscribe. Per-attempt jitter (±20%) is applied on top
/// to avoid synchronised retry waves across many clients.
pub(super) const RETRY_BASE_DELAY: Duration = Duration::from_millis(50);

/// Timeout for waiting on contract storage notification.
/// Used when a subscription arrives before the contract has been propagated via PUT.
pub(super) const CONTRACT_WAIT_TIMEOUT_MS: u64 = 2_000;

/// Wait for a contract to become available, using channel-based notification.
///
/// This handles the race condition where a subscription arrives before the contract
/// has been propagated via PUT. The flow is:
/// 1. Fast path: check if contract already exists
/// 2. Register notification waiter
/// 3. Check again (handles race between step 1 and 2)
/// 4. Wait for notification or timeout
/// 5. Final verification of actual state
pub(super) async fn wait_for_contract_with_timeout(
    op_manager: &OpManager,
    instance_id: ContractInstanceId,
    timeout_ms: u64,
) -> Result<Option<ContractKey>, OpError> {
    // Fast path - contract already exists
    if let Some(key) = super::has_contract(op_manager, instance_id).await? {
        return Ok(Some(key));
    }

    // Register waiter BEFORE second check to avoid race condition
    let notifier = op_manager.wait_for_contract(instance_id);

    // Check again - contract may have arrived between first check and registration
    if let Some(key) = super::has_contract(op_manager, instance_id).await? {
        return Ok(Some(key));
    }

    // Wait for notification or timeout (we don't care which triggers first)
    crate::deterministic_select! {
        _ = notifier => {},
        _ = sleep(Duration::from_millis(timeout_ms)) => {},
    }

    // Always verify actual state - don't trust notification alone
    super::has_contract(op_manager, instance_id).await
}

/// Outcome of [`prepare_initial_request`]: the decision about how to originate
/// a subscribe request based on the node's current ring state and contract
/// availability.
///
/// This type exists so all driver subscribe entry points
/// (`op_ctx_task::run_client_subscribe`,
/// `op_ctx_task::run_renewal_subscribe`,
/// `op_ctx_task::run_executor_subscribe`) share the "which peer, or
/// local-complete, or give up?" decision logic without duplicating
/// `k_closest_potentially_hosting` + fallback + local-completion
/// handling.
///
/// The returned values describe what the caller should do; the helper does
/// NOT mutate `op_manager` or push state. Any side-effects (emitting
/// telemetry via `NetEventLog::subscribe_request`, calling
/// `complete_local_subscription`, sending the wire message) are the
/// caller's responsibility.
#[derive(Debug)]
pub(super) enum InitialRequest {
    /// Contract is available locally and no network round-trip is required.
    /// Caller should call [`complete_local_subscription`] with the original
    /// transaction id and propagate its result.
    LocallyComplete { key: ContractKey },
    /// No remote peers could be found and the contract is not available
    /// locally. Caller should return `RingError::NoHostingPeers`.
    NoHostingPeers,
    /// Peer has not joined the ring yet (no own location) and the contract is
    /// not available locally. Caller should return `RingError::PeerNotJoined`.
    PeerNotJoined,
    /// A target peer was selected and the caller should send a
    /// `SubscribeMsg::Request` to it. `visited` and `alternatives` carry the
    /// routing state the caller must thread into its retry logic.
    NetworkRequest {
        /// The chosen target peer (already removed from `alternatives`).
        target: PeerKeyLocation,
        /// Socket address of `target`; pre-resolved for convenience.
        target_addr: std::net::SocketAddr,
        /// Bloom filter of visited peers, already including `own_addr` and
        /// `target_addr`. Callers should clone this into the outgoing
        /// `SubscribeMsg::Request.visited`.
        visited: super::VisitedPeers,
        /// Remaining k_closest candidates not yet tried at this hop.
        alternatives: Vec<PeerKeyLocation>,
        /// Initial HTL for the request (= `op_manager.ring.max_hops_to_live`).
        htl: usize,
    },
}

/// Compute the initial "where do we send this subscribe, or do we complete
/// locally?" decision for a subscribe request.
///
/// All driver subscribe entry points (`op_ctx_task::run_client_subscribe`,
/// `run_renewal_subscribe`, `run_executor_subscribe`) reuse the same ring
/// lookup / fallback / local-completion logic via this helper. The helper is
/// pure modulo telemetry emission: it calls `NetEventLog::subscribe_request`
/// on the `NetworkRequest` branch so all callers get identical event logs,
/// but it does not mutate `op_manager` state.
///
/// Decision branches:
///
/// 1. If the peer has no ring location (hasn't joined), check local contract
///    availability and either complete locally or return `PeerNotJoined`.
/// 2. Query `k_closest_potentially_hosting` with `own_addr` already visited.
///    If it returns candidates, take the first as the target.
/// 3. If `k_closest` returned empty, fall back to any connected peer that
///    hasn't been visited (sorted by location for determinism).
/// 4. If no fallback is available either, check local contract availability
///    one more time (standalone node / sole holder case) and either complete
///    locally or return `NoHostingPeers`.
///
/// When `first_hop` is `Some(holder)`, the ring-selection block (branches 2–4)
/// is bypassed entirely and `holder` is used as the target directly (a directed
/// subscribe, e.g. driven by a received `SubscribeHint`). The peer-not-joined
/// check still applies. There are no alternatives in this mode.
pub(super) async fn prepare_initial_request(
    op_manager: &OpManager,
    id: Transaction,
    instance_id: ContractInstanceId,
    is_renewal: bool,
    first_hop: Option<PeerKeyLocation>,
) -> Result<InitialRequest, OpError> {
    let own_addr = match op_manager.ring.connection_manager.peer_addr() {
        Ok(addr) => addr,
        Err(_) => {
            // Peer hasn't joined the network yet - check if contract is available locally
            if let Some(key) = super::has_contract(op_manager, instance_id).await? {
                tracing::info!(
                    tx = %id,
                    contract = %key,
                    phase = "local_complete",
                    "Peer not joined, but contract available locally - completing subscription locally"
                );
                return Ok(InitialRequest::LocallyComplete { key });
            }
            tracing::warn!(
                tx = %id,
                contract = %instance_id,
                phase = "peer_not_joined",
                "Cannot subscribe: peer has not joined network yet and contract not available locally"
            );
            return Ok(InitialRequest::PeerNotJoined);
        }
    };

    // IMPORTANT: Even if we have the contract locally (e.g., from PUT forwarding),
    // we MUST send a Subscribe::Request to the network to register ourselves as a
    // downstream subscriber in the subscription tree. Otherwise, when the contract
    // is updated at the source, we won't receive the update because we're not
    // registered in the upstream peer's subscriber list.
    //
    // The local subscription completion happens when we receive the Response back.
    // This ensures proper subscription tree management for update propagation.

    // Find a peer to forward the request to (needed even if we have contract locally)
    let mut visited = super::VisitedPeers::new(&id);
    visited.mark_visited(own_addr);

    // Directed subscribe: a caller (e.g. a received SubscribeHint) named the
    // first hop explicitly. Bypass ring selection and target `holder` directly,
    // with no alternatives.
    let (target, candidates): (PeerKeyLocation, Vec<PeerKeyLocation>) = if let Some(holder) =
        first_hop
    {
        tracing::debug!(
            tx = %id,
            contract = %instance_id,
            target = ?holder.socket_addr(),
            phase = "directed_first_hop",
            "Using caller-supplied first hop for subscription (bypassing ring selection)"
        );
        (holder, Vec::new())
    } else {
        let mut candidates =
            op_manager
                .ring
                .k_closest_potentially_hosting(&instance_id, &visited, MAX_BREADTH);

        // First try the best candidates from k_closest_potentially_hosting.
        // If that returns empty, fall back to any available connection.
        // This ensures we join the subscription tree even when the routing algorithm
        // can't find ideal candidates (e.g., due to timing or location filtering).
        let target = if !candidates.is_empty() {
            candidates.remove(0)
        } else {
            // k_closest_potentially_hosting returned empty - try any connected peer as fallback.
            // The subscription will be forwarded toward the contract location.
            let connections = op_manager
                .ring
                .connection_manager
                .get_connections_by_location();
            // Sort keys for deterministic iteration order (HashMap iteration is non-deterministic)
            let mut sorted_keys: Vec<_> = connections.keys().collect();
            sorted_keys.sort();
            let fallback_target = sorted_keys
                .into_iter()
                .filter_map(|loc| connections.get(loc))
                .flatten()
                .find(|conn| {
                    conn.location
                        .socket_addr()
                        .map(|addr| !visited.probably_visited(addr))
                        .unwrap_or(false)
                })
                .map(|conn| conn.location.clone());

            match fallback_target {
                Some(target) => {
                    tracing::debug!(
                        tx = %id,
                        contract = %instance_id,
                        target = ?target.socket_addr(),
                        phase = "fallback_routing",
                        "Using fallback connection for subscription (k_closest returned empty)"
                    );
                    target
                }
                None => {
                    // Bootstrap fallback (#4361 / #4365): the ring-connection
                    // scan above sees only promoted ring peers
                    // (`get_connections_by_location`), so an empty-ring node
                    // finds nothing here even though it holds a live transient
                    // gateway connection. Route the initial request via a
                    // configured gateway so a freshly-bootstrapped node can
                    // still join the subscription tree, before falling back to
                    // local completion / NoHostingPeers.
                    if let Some((gateway, gateway_addr)) =
                        bootstrap_gateway_target(op_manager, |addr| visited.probably_visited(addr))
                    {
                        tracing::info!(
                            tx = %id,
                            contract = %instance_id,
                            gateway = %gateway_addr,
                            phase = "bootstrap_gateway",
                            "subscribe: ring empty — routing initial request via configured gateway"
                        );
                        gateway
                    } else if let Some(key) = super::has_contract(op_manager, instance_id).await? {
                        // No gateway either - fall back to local completion only if isolated.
                        // This handles a standalone node or when we're the only node with the contract.
                        tracing::info!(
                            tx = %id,
                            contract = %key,
                            phase = "local_complete",
                            "Contract available locally and no network connections, completing subscription locally"
                        );
                        return Ok(InitialRequest::LocallyComplete { key });
                    } else {
                        tracing::warn!(tx = %id, contract = %instance_id, phase = "error", "No remote peers available for subscription");
                        return Ok(InitialRequest::NoHostingPeers);
                    }
                }
            }
        };
        (target, candidates)
    };

    let target_addr = target
        .socket_addr()
        .expect("target must have socket address");
    visited.mark_visited(target_addr);

    tracing::debug!(
        tx = %id,
        contract = %instance_id,
        is_renewal,
        target_peer = %target_addr,
        "subscribe: forwarding Request to target peer"
    );

    // Emit telemetry for subscribe request initiation so both legacy and
    // driver paths produce identical `NetEventLog::subscribe_request`
    // events.
    if let Some(event) = NetEventLog::subscribe_request(
        &id,
        &op_manager.ring,
        instance_id,
        target.clone(),
        op_manager.ring.max_hops_to_live,
    ) {
        op_manager.ring.register_events(Either::Left(event)).await;
    }

    Ok(InitialRequest::NetworkRequest {
        target,
        target_addr,
        visited,
        alternatives: candidates,
        htl: op_manager.ring.max_hops_to_live,
    })
}

/// Complete a **standalone** local subscription by notifying the client layer.
///
/// **IMPORTANT:** Only used when no remote peers are available (standalone
/// node). For normal network subscriptions, the driver awaits the wire
/// `Response` and publishes through `result_router_tx`.
///
/// **Architecture Note (Issue #2075):**
/// Local client subscriptions are deliberately kept separate from network subscriptions:
/// - **Network subscriptions** are stored in `ring.hosting_manager.subscribers` and are used
///   for peer-to-peer UPDATE propagation between nodes.
/// - **Local subscriptions** are managed by the contract executor via `update_notifications`
///   channels, which deliver `UpdateNotification` directly to WebSocket clients.
async fn complete_local_subscription(
    op_manager: &OpManager,
    id: Transaction,
    key: ContractKey,
    is_renewal: bool,
) -> Result<(), OpError> {
    tracing::debug!(
        %key,
        tx = %id,
        is_renewal,
        "Local subscription completed - client will receive updates via executor notification channel"
    );

    // Register local interest so that ChangeInterests from peers get
    // processed. Enables bidirectional interest discovery: when peers
    // announce they seed this contract, our has_local_interest() check
    // passes, and we register their peer interest, enabling direct
    // update broadcasts from them to us.
    if !is_renewal {
        let became_interested = op_manager.interest_manager.add_local_client(&key);
        if became_interested {
            super::broadcast_change_interests(op_manager, vec![key], vec![]).await;
        }
    }

    // Notify client layer that subscription is complete. The actual update
    // delivery happens through the executor's update_notifications when
    // contract state changes, not through network broadcast targets.
    op_manager
        .notify_node_event(crate::message::NodeEvent::LocalSubscribeComplete {
            tx: id,
            key,
            subscribed: true,
            is_renewal,
        })
        .await?;

    op_manager.completed(id);
    Ok(())
}

/// Fetch the contract body locally if we do not already have it.
///
/// Fire-and-forget GET sub-op + bounded wait for the contract to land in
/// local storage. Used by the originator finalization helper so that a
/// peer that successfully subscribed to a contract it had never seen can
/// answer subsequent GETs from local state rather than returning
/// `get_not_found`. See issue #4223 — the v0.2.51+ task-per-tx SUBSCRIBE
/// migration dropped this call, leaving subscribers with the lease
/// installed but no contract body, so 37% of GETs that routed through a
/// subscriber were returning NotFound.
///
/// Returns `Ok(Some(key))` if the contract is locally available after the
/// fetch attempt (including the case where it was already present),
/// `Ok(None)` if it could not be obtained within `CONTRACT_WAIT_TIMEOUT_MS`
/// (the originator's subscribe still completes — the contract may arrive
/// later via UPDATE), and `Err` only for local infrastructure failures
/// (channel closed, etc.).
pub(super) async fn fetch_contract_if_missing(
    op_manager: &OpManager,
    instance_id: ContractInstanceId,
    directed_holder: Option<PeerKeyLocation>,
) -> Result<Option<ContractKey>, OpError> {
    if let Some(key) = super::has_contract(op_manager, instance_id).await? {
        return Ok(Some(key));
    }

    // Spawn the sub-op GET. We drop the receiver/transaction — we don't care
    // about the structured `SubOpGetOutcome`, only the side effect of caching
    // the contract locally. `wait_for_contract_with_timeout` (storage poll +
    // wait_for_contract channel + timeout) detects arrival.
    match directed_holder {
        // Placement nudge: fetch the body THROUGH the holder, not greedily.
        // A greedy GET toward the key could dead-end at the close non-hosting
        // cluster (the very failure the migration resolves), leaving this peer
        // subscribed-but-bodyless.
        Some(holder) => {
            let _tx =
                super::get::op_ctx_task::start_targeted_sub_op_get(op_manager, instance_id, holder);
        }
        None => {
            let (_tx, _rx) =
                super::get::op_ctx_task::start_sub_op_get(op_manager, instance_id, true);
        }
    }

    wait_for_contract_with_timeout(op_manager, instance_id, CONTRACT_WAIT_TIMEOUT_MS).await
}

/// Finalize an originator-side subscribe success.
///
/// Called from the task-per-tx SUBSCRIBE driver
/// (`op_ctx_task::drive_client_subscribe_inner`) after a `Subscribed`
/// reply arrives. Performs every side-effect the originator needs so the
/// subscription is fully usable end-to-end:
///
/// 1. Register the responding peer as our upstream interest (so
///    `send_unsubscribe_upstream` can find it on client disconnect — #3874).
/// 2. Install the lease in `active_subscriptions`
///    (`op_manager.ring.subscribe`).
/// 3. Clear pending backoff state for this contract
///    (`op_manager.ring.complete_subscription_request(..., true)`).
/// 4. **Fetch the contract body locally if missing** (#4223). Without
///    this, the peer registers as a subscriber but answers `NotFound`
///    on GETs that route through it because the contract body never
///    arrived.
/// 5. **Announce that we host this contract to neighbors**, *only when
///    the body is locally present after step 4*. Announcing without a
///    body would tell neighbors to forward UPDATEs to a peer that
///    cannot validate or store them. If the body lands later via the
///    sub-op GET's own cache path, `get/op_ctx_task.rs` calls
///    `announce_contract_hosted` there *on first-time cache* (gated
///    on `is_new && put_persisted`). A niche case where the contract
///    was previously hosted then evicted, the lease expires, and the
///    re-subscribe fetch then times out, would not re-trigger the
///    announce via that fallback — neighbors learn we host the
///    contract again either through the next renewal cycle's
///    finalization (if the body has arrived by then) or via UPDATE
///    delivery + auto-fetch.
/// 6. Register the contract in our local interest manager (so inbound
///    `ChangeInterests` for this contract get processed) and broadcast
///    a `ChangeInterests` so connected peers learn we became interested.
///    Gated on `!is_renewal` — `add_local_client` is NOT idempotent
///    (`ring::interest::Contract::add_client` increments
///    `local_client_count` on every call); calling it on every renewal
///    cycle (~2 minutes) would leak the gauge unboundedly.
///
/// Latency note: because this function is awaited before the driver
/// publishes `SubscribeResponse` to the client, a first-time subscribe
/// to a contract we don't host can take up to `CONTRACT_WAIT_TIMEOUT_MS`
/// (2 s) longer than the prior task-per-tx path took. This implements
/// issue #4223's proposed approach point 2 ("do not emit
/// subscribe_success to the client until fetch_contract_if_missing
/// completes"); the latency is the cost of the client knowing the
/// subscriber can actually serve a follow-up GET.
///
/// (The former `crate::operations::complete_piggyback_subscription`
/// GET-piggyback originator finalization path was removed with
/// GET-auto-subscribe in piece E of the demand-driven hosting redesign;
/// explicit `subscribe=true` GETs now route through the ordinary subscribe
/// driver, which reaches this helper.)
pub(super) async fn finalize_originator_subscribe(
    op_manager: &OpManager,
    key: ContractKey,
    upstream_addr: std::net::SocketAddr,
    is_renewal: bool,
    // `Some(_)` marks a directed placement nudge (`SubscribeHint`) subscribe;
    // `None` marks an ordinary subscribe. When `Some`, the body fetch is routed
    // through the peer that accepted the subscription (`upstream_addr`) so it
    // cannot dead-end the way a greedy GET would; when `None` the fetch is
    // greedy. (The value is only used as the directed/ordinary marker — the
    // actual fetch target is the responder, resolved below.)
    directed_holder: Option<PeerKeyLocation>,
) {
    if let Some(pkl) = op_manager
        .ring
        .connection_manager
        .get_peer_by_addr(upstream_addr)
    {
        let peer_key = crate::ring::interest::PeerKey::from(pkl.pub_key);
        let is_new = op_manager
            .interest_manager
            .register_peer_interest(&key, peer_key, None, true);
        if is_new {
            // #4359 (MUST-FIX 1): this upstream peer is now a viable broadcast
            // target. If a fresh-contract PUT gave up with no targets and is
            // stashed, flush it so the deferred state reaches the network.
            op_manager.flush_pending_broadcast_on_interest(&key).await;
        }
    }

    op_manager.ring.subscribe(key);
    op_manager.ring.complete_subscription_request(&key, true);

    // Fetch the contract body if we don't have it locally. The result
    // gates the announce step: we only advertise hosting to neighbors
    // when the body actually lands. On timeout (Ok(None)) or
    // infrastructure failure (Err), the subscription is still finalized
    // (lease installed, peer registered) — the contract may arrive
    // later via UPDATE propagation or the sub-op GET completing past
    // the 2 s wait window, at which point the GET driver's own
    // `announce_contract_hosted` call fires.
    //
    // For a directed (migration) subscribe, route the body fetch through the
    // peer that ACTUALLY accepted the subscription (`upstream_addr`) — which
    // holds the contract — rather than the original hint holder. In the common
    // case the responder IS the hint holder; but if the hinted holder went away
    // and the subscribe fell back to a greedy peer, the responder is that peer,
    // so the targeted fetch still goes to a peer that has the body instead of
    // the stale holder (which would dead-end the fetch). A greedy GET toward the
    // key could dead-end at the same close non-hosting cluster the migration is
    // resolving, hence the targeted fetch. `None` (ordinary subscribe) keeps the
    // greedy fetch.
    let directed_fetch_target = directed_holder.as_ref().and_then(|_| {
        op_manager
            .ring
            .connection_manager
            .get_peer_by_addr(upstream_addr)
    });
    let have_body =
        match fetch_contract_if_missing(op_manager, *key.id(), directed_fetch_target).await {
            Ok(Some(_)) => true,
            Ok(None) => {
                tracing::debug!(
                    contract = %key,
                    timeout_ms = CONTRACT_WAIT_TIMEOUT_MS,
                    "subscribe: contract body did not arrive within timeout; \
                     deferring announce_contract_hosted — the sub-op GET will \
                     announce when it caches, or UPDATE delivery will fill the gap"
                );
                false
            }
            Err(err) => {
                // Infrastructure failure (notification channel closed,
                // contract handler down). `warn` rather than `debug`
                // because this signals broken local plumbing, not a
                // routine cache miss.
                tracing::warn!(
                    contract = %key,
                    error = %err,
                    "subscribe: fetch_contract_if_missing returned infra error; \
                     deferring announce_contract_hosted — operator should \
                     investigate (contract handler / notification channel)"
                );
                false
            }
        };

    // Reconcile-controller SHADOW comparison (keystone step-2, #4642),
    // HOST-FORMATION site (subscribe originator). About to (conditionally)
    // announce hosting; does the controller agree? Focused on `Announce`. Actual
    // = `{Announce}` iff production announces a NOT-yet-advertised host this event
    // (`have_body` AND not already advertised — `announce_contract_hosted` is
    // idempotent), else `{}`. Built BEFORE the announce so `is_advertised`
    // reflects the pre-announce state. DRIVES NOTHING.
    op_manager.record_reconcile_shadow_event(
        crate::node::network_status::ReconcileShadowSite::HostFormation,
        &key,
        &[crate::ring::reconcile::Action::Announce],
        |inputs| {
            if have_body && !inputs.is_advertised {
                vec![crate::ring::reconcile::Action::Announce]
            } else {
                Vec::new()
            }
        },
    );

    if have_body {
        super::announce_contract_hosted(op_manager, &key).await;
    }

    if !is_renewal {
        let became_interested = op_manager.interest_manager.add_local_client(&key);
        if became_interested {
            super::broadcast_change_interests(op_manager, vec![key], vec![]).await;
        }
    }
}

/// Register a downstream subscriber for a contract.
///
/// Resolves the requester's `PeerKey` from the pre-resolved public key (preferred,
/// avoids NAT timing window failures) or falls back to an address lookup. If a key
/// is found, records the peer in both the downstream subscriber list and the interest
/// manager so UPDATE broadcasts reach them immediately.
pub(crate) async fn register_downstream_subscriber(
    op_manager: &OpManager,
    key: &ContractKey,
    requester_addr: std::net::SocketAddr,
    requester_pub_key: Option<&crate::transport::TransportPublicKey>,
    source_addr: Option<std::net::SocketAddr>,
    tx: &Transaction,
    warn_suffix: &str,
) {
    let peer_key = requester_pub_key
        .map(|pk| crate::ring::interest::PeerKey::from(pk.clone()))
        .or_else(|| {
            op_manager
                .ring
                .connection_manager
                .get_peer_by_addr(requester_addr)
                .or_else(|| {
                    source_addr
                        .and_then(|sa| op_manager.ring.connection_manager.get_peer_by_addr(sa))
                })
                .map(|pkl| crate::ring::interest::PeerKey::from(pkl.pub_key))
        });

    if let Some(peer_key) = peer_key {
        if op_manager
            .ring
            .add_downstream_subscriber(key, peer_key.clone())
        {
            let is_new_peer = op_manager
                .interest_manager
                .register_peer_interest(key, peer_key, None, false);
            // Only increment downstream count for genuinely new peers, not
            // renewals. add_downstream_subscriber (hosting) returns true for
            // both new and renewed peers, so use register_peer_interest's
            // is_new return to avoid over-counting on renewal cycles.
            if is_new_peer {
                // #4359: a fresh-contract PUT whose initial broadcast found no
                // targets is stashed by the fan-out handler. This new
                // downstream subscriber is the first viable target — flush the
                // deferred state to it so the never-before-seen id reaches the
                // network instead of staying locally-hosted only.
                op_manager.flush_pending_broadcast_on_interest(key).await;
                let became_interested = op_manager.interest_manager.add_downstream_subscriber(key);
                if became_interested {
                    super::broadcast_change_interests(op_manager, vec![*key], vec![]).await;
                }
            }
        } else {
            tracing::warn!(
                tx = %tx,
                contract = %key,
                "Downstream subscriber limit reached — skipping peer interest registration"
            );
        }
    } else {
        tracing::warn!(
            tx = %tx,
            contract = %key,
            requester_addr = %requester_addr,
            source_addr = ?source_addr,
            "Subscribe: could not find peer to register interest{}",
            warn_suffix
        );
    }
}

/// Host-formation sequence for a peer that just handled a relayed SUBSCRIBE:
/// fetch the contract body FIRST, and only if it lands do we register the
/// requester as a downstream subscriber, install the local subscription lease,
/// and announce hosting. This makes the phantom (`contract_in_use &&
/// !contract_state_present`) unrepresentable — a peer registers demand only
/// AFTER it holds state (D-ORDER, step 10 §1b; the register-after-state fix).
///
/// Steps, in order (the pin `finalize_host_subscribe_fetches_before_register`
/// enforces fetch BEFORE register BEFORE announce, and NO `add_local_client`):
/// 1. **Fetch the body if missing** (`fetch_contract_if_missing`). Greedy fetch
///    toward the key — the downstream that answered `Subscribed` holds the body,
///    so a route toward the key reaches it.
/// 2. On fetch failure (timeout `Ok(None)` / infra `Err`) register NOTHING and
///    return: the caller forwards / answers NotFound as before, and the chain
///    hole heals on the requester's next lease renewal. This differs from the
///    pre-step-10 relay behavior, which registered the downstream subscriber
///    unconditionally and so left a stateless phantom (#4404/#4612).
/// 3. On success: register the requester as our downstream subscriber, install
///    the local subscription lease (`ring.subscribe`) so we become a real host
///    kept fresh in the update mesh (piece D: chain hops are real hosts), clear
///    the pending-subscribe backoff, and announce hosting (idempotent backstop).
///
/// Does NOT call `add_local_client` (D-NO-LOCAL-CLIENT): a relay has no local
/// client, and `add_local_client` is non-idempotent — copying it here would
/// inflate `local_client_count` and wrongly make the relay evicted-last.
///
/// The `requester_addr` / `source_addr` / `tx` / `warn_suffix` params are
/// forwarded verbatim to [`register_downstream_subscriber`].
pub(super) async fn finalize_host_subscribe(
    op_manager: &OpManager,
    key: ContractKey,
    requester_addr: std::net::SocketAddr,
    source_addr: Option<std::net::SocketAddr>,
    // The peer to route the body fetch THROUGH, when known (review Fix 4). On the
    // forwarded relay path this is the downstream that just answered `Subscribed`
    // (`next_hop`) — it holds the body, so a directed fetch reaches it instead of
    // greedily routing toward the key and dead-ending at a closer non-hosting
    // peer (which would leave this hop registering nothing yet still bubbling
    // `Subscribed` upstream — a chain hole that drops updates). `None` = ordinary
    // greedy fetch (used by the local-hit path, where state is already present).
    directed_holder: Option<PeerKeyLocation>,
    tx: &Transaction,
    warn_suffix: &str,
) {
    // Fetch BEFORE register (D-ORDER). `directed_holder` routes the fetch through
    // the responding peer when known, else greedy toward the key.
    let have_body = match fetch_contract_if_missing(op_manager, *key.id(), directed_holder).await {
        Ok(Some(_)) => true,
        Ok(None) => {
            tracing::debug!(
                contract = %key,
                timeout_ms = CONTRACT_WAIT_TIMEOUT_MS,
                "subscribe host-formation: body did not arrive within timeout; \
                 registering nothing (no phantom) — chain hole heals on next renewal"
            );
            false
        }
        Err(err) => {
            tracing::warn!(
                contract = %key,
                error = %err,
                "subscribe host-formation: fetch_contract_if_missing infra error; \
                 registering nothing (no phantom) — operator should investigate \
                 (contract handler / notification channel)"
            );
            false
        }
    };

    if !have_body {
        // Register-after-state: on fetch-fail we create NO downstream
        // registration, so `contract_in_use` stays false and no phantom exists.
        return;
    }

    // We hold the body — become a real subscribed host and register the
    // requester as our downstream subscriber (register-AFTER-state).
    register_downstream_subscriber(
        op_manager,
        &key,
        requester_addr,
        None,
        source_addr,
        tx,
        warn_suffix,
    )
    .await;
    op_manager.ring.subscribe(key);
    // NOTE (review Fix B): do NOT call `complete_subscription_request` here. That
    // clears the node-wide per-contract pending-subscription dedup slot and
    // records backoff success — but a RELAY never CLAIMED that slot
    // (`mark_subscription_pending` is called only by the client-subscribe /
    // renewal / re-root drivers, not the relay path). Releasing it here would
    // free a concurrent LOCAL renewal/re-root's dedup slot early and overwrite
    // its retry state → duplicate renewals / masked failures. It is correct only
    // in `finalize_originator_subscribe`, where the originator DID claim the slot.
    // Announce backstop, gated on have_body. `fetch_contract_if_missing`'s own
    // cache path already announces on a first-time store; `announce_contract_hosted`
    // is one-shot (dedup via `neighbor_hosting.rs`'s `my_contracts` DashSet), so a
    // duplicate is a no-op — this just guarantees the advert fires even when the
    // body was already present locally (nothing to trigger the cache-path announce).
    super::announce_contract_hosted(op_manager, &key).await;
}

/// Handle a fresh inbound `SubscribeMsg::Unsubscribe` from a peer.
///
/// Removes the sender's downstream subscriber tracking for the contract and
/// chains the unsubscribe upstream if no remaining interest is present.
///
/// Called from the node.rs dispatch site for inbound Unsubscribe
/// wire messages.
pub(crate) async fn handle_unsubscribe_inbound(
    op_manager: &OpManager,
    tx: Transaction,
    instance_id: ContractInstanceId,
    source_addr: Option<std::net::SocketAddr>,
) {
    tracing::debug!(
        tx = %tx,
        %instance_id,
        ?source_addr,
        "received unsubscribe notification"
    );

    let sender_peer = source_addr.and_then(|addr| {
        op_manager
            .ring
            .connection_manager
            .get_peer_by_addr(addr)
            .map(|pkl| crate::ring::interest::PeerKey::from(pkl.pub_key))
    });

    let key = match super::has_contract(op_manager, instance_id).await {
        Ok(Some(key)) => key,
        Ok(None) => {
            tracing::debug!(
                tx = %tx,
                %instance_id,
                "Contract not found locally, ignoring unsubscribe"
            );
            return;
        }
        Err(err) => {
            tracing::warn!(
                tx = %tx,
                %instance_id,
                %err,
                "Contract lookup failed while handling unsubscribe"
            );
            return;
        }
    };

    if let Some(peer) = &sender_peer {
        let was_downstream = op_manager.ring.remove_downstream_subscriber(&key, peer);
        let was_interested = op_manager.interest_manager.remove_peer_interest(&key, peer);
        // Only decrement downstream count if the peer was actually tracked,
        // to stay in sync with the increment in register_downstream_subscriber.
        if was_downstream || was_interested {
            let lost_interest = op_manager
                .interest_manager
                .remove_downstream_subscriber(&key);
            if lost_interest {
                super::broadcast_change_interests(op_manager, vec![], vec![key]).await;
            }
        }
    } else {
        tracing::warn!(
            tx = %tx,
            %instance_id,
            ?source_addr,
            "Unsubscribe: could not resolve sender peer, downstream entry not removed"
        );
    }

    // FLIP (keystone P6, #4642), INBOUND-UNSUBSCRIBE site. The downstream peer just
    // left (removal above is complete, so the strict-farther gate reflects it);
    // does that leave us not-in-use, i.e. should the controller tear down our lease?
    // The collapse decision is now DRIVEN by the reconcile controller's
    // strict-farther interest gate (`reconcile_wants_collapse` = `!contract_in_use`),
    // replacing the legacy ANY-downstream `should_unsubscribe_upstream`. The strict
    // gate collapses the "still-subscribed-only-by-a-CLOSER-downstream / mutual
    // co-host" case the legacy predicate left hosting (v0.2.93 shadow measured this
    // InboundUnsubscribe collapse diff at ~0.00%). The teardown still targets the
    // STORED upstream (narrow flip keeps the flag); the repurposed `Collapse`
    // reconcile counter is recorded inside `reconcile_wants_collapse`.
    if op_manager.reconcile_wants_collapse(
        &key,
        crate::node::network_status::ReconcileShadowSite::InboundUnsubscribe,
    ) {
        tracing::debug!(
            tx = %tx,
            contract = %key,
            "No remaining strict-farther interest, propagating unsubscribe upstream"
        );
        op_manager.send_unsubscribe_upstream(&key).await;
    } else {
        tracing::debug!(
            tx = %tx,
            contract = %key,
            "Still have strict-farther interest, not propagating unsubscribe"
        );
    }
}

#[cfg(test)]
mod tests;

/// Source-scrape pins for the subscribe-seeds-state-via-GET invariant.
///
/// This is the load-bearing property that makes removing the Source-2
/// (interest-manager) live-update fan-out safe (#4642 step 9, see
/// `operations/update.rs::get_broadcast_targets_update`). Because
/// `finalize_originator_subscribe` awaits `fetch_contract_if_missing` (which
/// drives a sub-op GET) BEFORE it advertises hosting or returns subscribe
/// success, a genuine subscriber pulls current state (including any
/// already-committed UPDATE) via its OWN GET and never persists in the
/// "interested but no local state" condition. That is why it is safe for live
/// UPDATE fan-out to target advertised co-hosts only: a real subscriber has
/// state and advertises, so it is a Source-1 target and has a non-empty summary.
///
/// If a future change let a peer register as a subscriber WITHOUT that seeding
/// GET, a no-state subscriber relying on live fan-out would reappear and the
/// Source-2 removal WOULD drop its first update. These pins fail loudly if the
/// seed-GET is dropped, made fire-and-forget, or reordered after the advertise.
#[cfg(test)]
mod source_pin_tests {
    /// Extract the balanced-brace body of the named `async fn` from this file.
    /// The needle is composed at runtime so the pin does not match itself.
    fn extract_fn_body(fn_name: &str) -> &'static str {
        let src = include_str!("subscribe.rs");
        let head = ["async fn ", fn_name, "("].concat();
        let start = src
            .find(&head)
            .unwrap_or_else(|| panic!("`async fn {fn_name}(` must exist in subscribe.rs"));
        let body_open = src[start..].find('{').map(|off| start + off).unwrap();
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
        &src[start..end]
    }

    #[test]
    fn subscribe_seeds_state_via_get_before_finalize() {
        let body = extract_fn_body("finalize_originator_subscribe");

        // (1) The seed-GET call is present at all.
        let fetch_idx = body.find("fetch_contract_if_missing(").expect(
            "#4642 step 9 safety: `finalize_originator_subscribe` MUST call \
             `fetch_contract_if_missing` to seed baseline state via GET before \
             finalizing. Without it, a peer can become a subscriber with no local \
             state and rely on a live UPDATE fan-out that Source-2 removal deleted.",
        );

        // Match the CALL (`announce_contract_hosted(`), not the several prose
        // mentions of the name in comments above it.
        let announce_idx = body.find("announce_contract_hosted(").expect(
            "`finalize_originator_subscribe` should still advertise hosting via \
             `announce_contract_hosted`.",
        );

        // (2) The seed-GET precedes the advertise (seed BEFORE announce): a peer
        // must not advertise itself as a host before it has pulled state.
        assert!(
            fetch_idx < announce_idx,
            "#4642 step 9 safety: `fetch_contract_if_missing` (seed state via GET) \
             must run BEFORE `announce_contract_hosted` in \
             `finalize_originator_subscribe`. Advertising before seeding would make \
             the peer a Source-1 target with no state.",
        );

        // (3) It is AWAITED (not fire-and-forget): the fetch result must gate the
        // subsequent finalize steps, so a `.await` must appear between the call
        // and the advertise.
        assert!(
            body[fetch_idx..announce_idx].contains(".await"),
            "#4642 step 9 safety: the `fetch_contract_if_missing` seed-GET must be \
             AWAITED before finalizing. A fire-and-forget fetch would let the \
             subscription complete before state lands, reintroducing the no-state \
             subscriber that live fan-out no longer covers.",
        );

        // (4) The fetch genuinely drives a GET (not merely a local cache peek), so
        // committed-but-not-yet-seen state is actually pulled in.
        let fetch_body = extract_fn_body("fetch_contract_if_missing");
        assert!(
            fetch_body.contains("start_sub_op_get")
                || fetch_body.contains("start_targeted_sub_op_get"),
            "#4642 step 9 safety: `fetch_contract_if_missing` must drive a sub-op \
             GET (`start_sub_op_get` / `start_targeted_sub_op_get`) so a subscriber \
             pulls current state, including any already-committed UPDATE.",
        );
    }
}

/// Task-per-transaction SUBSCRIBE drivers.
pub(crate) mod op_ctx_task;

pub(crate) use op_ctx_task::{
    RenewalOutcome, run_client_subscribe, run_executor_subscribe, run_renewal_subscribe,
    start_client_subscribe, start_directed_subscribe,
};

mod messages {
    use std::fmt::Display;

    use super::*;

    /// Result of a SUBSCRIBE operation - either subscription succeeded or contract was not found.
    ///
    /// This provides explicit semantics for "contract not found" rather than
    /// requiring interpretation of `subscribed: false` which could also mean other failures.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum SubscribeMsgResult {
        /// Subscription succeeded - includes full contract key
        Subscribed { key: ContractKey },
        /// Contract was not found after exhaustive search
        NotFound,
    }

    /// Subscribe operation messages.
    ///
    /// Uses hop-by-hop routing: each node stores `requester_addr` from the transport layer
    /// to route responses back. No `PeerKeyLocation` is embedded in wire messages.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum SubscribeMsg {
        /// Request to subscribe to a contract. Forwarded hop-by-hop toward contract location.
        Request {
            id: Transaction,
            /// Contract instance to subscribe to (full key not needed for routing)
            instance_id: ContractInstanceId,
            /// Hops to live - decremented at each hop
            htl: usize,
            /// Bloom filter tracking visited peers to prevent loops
            visited: super::super::VisitedPeers,
            /// Whether this is a renewal (requester already has contract state).
            /// If true, responder skips sending state to save bandwidth.
            is_renewal: bool,
        },
        /// Response for a SUBSCRIBE operation. Routed hop-by-hop back to originator.
        /// Uses instance_id for routing (always available from the request).
        /// The full ContractKey is only present in SubscribeMsgResult::Subscribed.
        Response {
            id: Transaction,
            instance_id: ContractInstanceId,
            result: SubscribeMsgResult,
            /// Forward-path hop count: how many hops the originating Request
            /// traversed before reaching the node that produced this Response
            /// (the hosting peer for `Subscribed`, or the relay that reported
            /// exhaustion / no-candidates / forward-failure for `NotFound`).
            ///
            /// Computed as `max_hops_to_live - htl_at_responder`. The relay
            /// chain preserves this value as the Response bubbles back to the
            /// originator — it does NOT increment on the return path. For
            /// `NotFound`, treat this as the depth at which exhaustion
            /// occurred, not depth-to-the-contract.
            ///
            /// `#[serde(default)]` is set for source-level clarity. Bincode
            /// does not honour serde defaults (positional encoding), so wire
            /// compat with peers that lack this field is handled at the
            /// handshake layer via `MIN_COMPATIBLE_VERSION`.
            ///
            /// Mirror of `GetMsg::Response.hop_count` (PR #4245); see also
            /// `PutMsg::Response.hop_count`.
            #[serde(default)]
            hop_count: usize,
        },
        /// Explicit unsubscribe notification sent upstream for fast cleanup.
        /// Fire-and-forget: does not require a response or existing operation state.
        Unsubscribe {
            id: Transaction,
            instance_id: ContractInstanceId,
        },

        /// Lightweight ACK sent by a relay peer back to its upstream when it forwards
        /// a SUBSCRIBE request to the next hop. Tells the upstream "I'm working on it"
        /// so the GC task can distinguish dead peers from slow multi-hop chains.
        /// Fire-and-forget — no response expected.
        ForwardingAck {
            id: Transaction,
            instance_id: ContractInstanceId,
        },
    }

    impl InnerMessage for SubscribeMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::Request { id, .. }
                | Self::Response { id, .. }
                | Self::Unsubscribe { id, .. }
                | Self::ForwardingAck { id, .. } => id,
            }
        }

        fn requested_location(&self) -> Option<Location> {
            match self {
                Self::Request { instance_id, .. }
                | Self::Response { instance_id, .. }
                | Self::Unsubscribe { instance_id, .. }
                | Self::ForwardingAck { instance_id, .. } => Some(Location::from(instance_id)),
            }
        }
    }

    impl Display for SubscribeMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::Request { instance_id, .. } => {
                    write!(f, "Subscribe::Request(id: {id}, contract: {instance_id})")
                }
                Self::Response {
                    instance_id,
                    result,
                    ..
                } => {
                    let result_str = match result {
                        SubscribeMsgResult::Subscribed { key } => format!("Subscribed({key})"),
                        SubscribeMsgResult::NotFound => "NotFound".to_string(),
                    };
                    write!(
                        f,
                        "Subscribe::Response(id: {id}, instance_id: {instance_id}, result: {result_str})"
                    )
                }
                Self::Unsubscribe { instance_id, .. } => {
                    write!(
                        f,
                        "Subscribe::Unsubscribe(id: {id}, contract: {instance_id})"
                    )
                }
                Self::ForwardingAck { instance_id, .. } => {
                    write!(
                        f,
                        "Subscribe::ForwardingAck(id: {id}, instance_id: {instance_id})"
                    )
                }
            }
        }
    }
}
