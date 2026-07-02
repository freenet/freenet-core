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
                    // Truly no connections available - fall back to local completion only if isolated.
                    // This handles the case of a standalone node or when we're the only node with the contract.
                    if let Some(key) = super::has_contract(op_manager, instance_id).await? {
                        tracing::info!(
                            tx = %id,
                            contract = %key,
                            phase = "local_complete",
                            "Contract available locally and no network connections, completing subscription locally"
                        );
                        return Ok(InitialRequest::LocallyComplete { key });
                    }
                    tracing::warn!(tx = %id, contract = %instance_id, phase = "error", "No remote peers available for subscription");
                    return Ok(InitialRequest::NoHostingPeers);
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
/// reply arrives. Runs the shared host-formation sequence
/// (`finalize_host_subscribe`: register update source, install lease, clear
/// backoff, fetch body, announce-if-present) and then the originator-only
/// step 6: register local client interest and broadcast `ChangeInterests`.
///
/// Latency note: because this function is awaited before the driver
/// publishes `SubscribeResponse` to the client, a first-time subscribe
/// to a contract we don't host can take up to `CONTRACT_WAIT_TIMEOUT_MS`
/// (2 s) longer than the prior task-per-tx path took. This implements
/// issue #4223's proposed approach point 2 ("do not emit
/// subscribe_success to the client until fetch_contract_if_missing
/// completes"); the latency is the cost of the client knowing the
/// subscriber can actually serve a follow-up GET.
pub(super) async fn finalize_originator_subscribe(
    op_manager: &OpManager,
    key: ContractKey,
    upstream_addr: std::net::SocketAddr,
    is_renewal: bool,
    directed_holder: Option<PeerKeyLocation>,
) {
    // Steps 1-5 (register the responder as our update source, install the
    // lease, clear backoff, fetch the body, announce-if-present) are the
    // host-formation sequence shared with the chain-host path; they live in
    // `finalize_host_subscribe`. For the originator, the peer we subscribed to
    // IS `upstream_addr`, so it is the responder.
    finalize_host_subscribe(op_manager, key, upstream_addr, directed_holder).await;

    // 6. Register local client interest (originator-only). Gated on
    //    `!is_renewal` — `add_local_client` is NOT idempotent
    //    (`ring::interest::Contract::add_client` increments `local_client_count`
    //    on every call), so calling it on every ~2-minute renewal cycle would
    //    leak the gauge unboundedly.
    if !is_renewal {
        let became_interested = op_manager.interest_manager.add_local_client(&key);
        if became_interested {
            super::broadcast_change_interests(op_manager, vec![key], vec![]).await;
        }
    }
}

/// Host-formation side-effect sequence shared by the originator and the
/// chain-host subscribe paths, run after a `Subscribed` reply.
///
/// There is no "forwarder that holds no state": a peer on a live subscription
/// chain IS a subscribed host — kept fresh in the contract's update mesh and
/// findable by GETs that route to it. This helper is that host-formation
/// sequence. The chain of hosts collapses inward once demand fades, because
/// the lease installed in step 2 is renewed only while the contract is
/// `contract_in_use` (see `HostingManager::contracts_needing_renewal` section
/// 1); a host with no downstream interest stops being renewed and drops out of
/// the mesh, which unwinds its upstream in turn.
///
/// Steps, in order:
///
/// 1. Register the RESPONDER (`responder_addr` — the peer that answered
///    `Subscribed`; for a chain host this is the next hop toward the key) as
///    our upstream interest, so UPDATEs propagate back to us and #3874
///    unsubscribe-on-disconnect can find it.
/// 2. Install/refresh the local subscription lease (`ring.subscribe`).
/// 3. Clear pending subscribe backoff (`complete_subscription_request`).
/// 4. **Fetch the contract body locally if missing** (#4223). Without this the
///    peer registers as a subscriber but answers `NotFound` on GETs that route
///    through it because the body never arrived.
/// 5. **Announce hosting to neighbors, only when the body is present after step
///    4.** Announcing without a body would advertise a copy we cannot serve or
///    keep fresh (hosting invariant 1) and would tell neighbors to forward
///    UPDATEs to a peer that cannot validate or store them. If the body lands
///    later via the sub-op GET's own cache path, `get/op_ctx_task.rs` announces
///    there on first-time cache.
///
/// See also `crate::operations::complete_piggyback_subscription` — the
/// GET-piggyback finalization path, which performs the same conceptual steps in
/// a different order (no fetch step: the body arrived inside the GET response).
/// Keep the two in sync when adding host side effects.
pub(super) async fn finalize_host_subscribe(
    op_manager: &OpManager,
    key: ContractKey,
    responder_addr: std::net::SocketAddr,
    // `Some(_)` routes the body fetch THROUGH the responder rather than greedily
    // toward the key. The chain-host path always passes the responder here (it
    // just answered `Subscribed`, so it holds the body and will not dead-end the
    // fetch); the originator passes `Some` for a directed placement nudge and
    // `None` for an ordinary greedy subscribe. The value is only a
    // directed/ordinary marker — the actual fetch target is the responder,
    // resolved below.
    directed_holder: Option<PeerKeyLocation>,
) {
    if let Some(pkl) = op_manager
        .ring
        .connection_manager
        .get_peer_by_addr(responder_addr)
    {
        let peer_key = crate::ring::interest::PeerKey::from(pkl.pub_key);
        let is_new = op_manager
            .interest_manager
            .register_peer_interest(&key, peer_key, None, true);
        if is_new {
            // #4359 (MUST-FIX 1): this responder peer is now a viable broadcast
            // target. If a fresh-contract PUT gave up with no targets and is
            // stashed, flush it so the deferred state reaches the network.
            op_manager.flush_pending_broadcast_on_interest(&key).await;
        }
    }

    op_manager.ring.subscribe(key);
    op_manager.ring.complete_subscription_request(&key, true);

    // Fetch the contract body if we don't have it locally. The result gates the
    // announce step: we only advertise hosting to neighbors when the body
    // actually lands. On timeout (Ok(None)) or infrastructure failure (Err), the
    // subscription is still finalized (lease installed, responder registered) —
    // the contract may arrive later via UPDATE propagation or the sub-op GET
    // completing past the 2 s wait window, at which point the GET driver's own
    // `announce_contract_hosted` call fires.
    //
    // The targeted fetch (when `directed_holder` is set) routes the body GET
    // THROUGH the responder — which just proved it holds the contract by
    // answering `Subscribed` — rather than greedily toward the key. A greedy GET
    // could dead-end at a close non-hosting cluster, leaving this peer
    // subscribed-but-bodyless. `None` keeps the greedy fetch.
    let directed_fetch_target = directed_holder.as_ref().and_then(|_| {
        op_manager
            .ring
            .connection_manager
            .get_peer_by_addr(responder_addr)
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

    if have_body {
        super::announce_contract_hosted(op_manager, &key).await;
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
    // Resolve the requester to a connected peer (with a ring location) —
    // preferred via the pre-resolved pub key, else by address (NAT-timing
    // fallback). We need the location for the §6 strict-distance guard below.
    let peer_pkl = requester_pub_key
        .and_then(|pk| op_manager.ring.connection_manager.get_peer_by_pub_key(pk))
        .or_else(|| {
            op_manager
                .ring
                .connection_manager
                .get_peer_by_addr(requester_addr)
        })
        .or_else(|| {
            source_addr.and_then(|sa| op_manager.ring.connection_manager.get_peer_by_addr(sa))
        });

    let peer_key = requester_pub_key
        .map(|pk| crate::ring::interest::PeerKey::from(pk.clone()))
        .or_else(|| {
            peer_pkl
                .as_ref()
                .map(|pkl| crate::ring::interest::PeerKey::from(pkl.pub_key.clone()))
        });

    if let Some(peer_key) = peer_key {
        // §6 strict-distance guard (demand-driven-hosting design). A downstream
        // subscriber must be STRICTLY FARTHER from the contract's key than us.
        // If we register a peer that is actually CLOSER — i.e. our upstream —
        // as a downstream, two connected co-hosts would each count the other as
        // demand and prop each other up forever with no real demand behind
        // either (mutual self-perpetuation, §7). Skip registration only when the
        // peer's position is RESOLVABLE and closer-or-equal to the key; if we
        // can't resolve its location we register (the lease-expiry backstop
        // bounds any mistake). This is the collapse-side dual of the computed
        // upstream's "strictly closer" rule; together they keep the chain
        // acyclic and its collapse terminating.
        if let (Some(peer_loc), Some(my_loc)) = (
            peer_pkl.as_ref().and_then(|pkl| pkl.location()),
            op_manager.ring.connection_manager.own_location().location(),
        ) {
            let contract_loc = crate::ring::Location::from(key);
            if peer_loc.distance(contract_loc) <= my_loc.distance(contract_loc) {
                tracing::debug!(
                    tx = %tx,
                    contract = %key,
                    "skip downstream registration: peer is not strictly farther \
                     from the contract key than us (it is our upstream, §6){}",
                    warn_suffix
                );
                return;
            }
        }
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

    if op_manager.ring.should_unsubscribe_upstream(&key) {
        tracing::debug!(
            tx = %tx,
            contract = %key,
            "No remaining subscribers, propagating unsubscribe upstream"
        );
        op_manager.send_unsubscribe_upstream(&key).await;
    } else {
        tracing::debug!(
            tx = %tx,
            contract = %key,
            "Still have subscribers, not propagating unsubscribe"
        );
    }
}

#[cfg(test)]
mod tests;

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
