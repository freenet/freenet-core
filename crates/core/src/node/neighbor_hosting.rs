//! Neighbor hosting manager for tracking which neighbors host which contracts.
//!
//! This module enables UPDATE forwarding to nearby hosts who may not be explicitly
//! subscribed. When a peer hosts a contract (via PUT or GET), it announces this to
//! its neighbors. Neighbors track this information and include known hosts in
//! UPDATE broadcast targets.
//!
//! # Design Note: Two Independent Mechanisms
//!
//! Freenet uses two complementary mechanisms for UPDATE propagation:
//!
//! 1. **Subscription Tree** (existing): Propagates updates to peers with downstream
//!    interest (users/peers subscribed through them). Tree structure rooted at
//!    contract location, extending toward clients. Tracked via `HostingManager.subscribers`.
//!
//! 2. **Neighbor Hosting** (this module): Propagates updates to nearby hosts who
//!    have the contract but may not be explicitly subscribed. Mesh structure among
//!    peers near the contract's location. Tracked via `NeighborHostingManager.neighbor_contracts`.
//!
//! These mechanisms are kept independent because they have different lifecycles
//! (subscriptions are explicit; hosting follows cache eviction) and different data
//! structures. They are combined at the broadcast targeting point in
//! `OpManager::get_broadcast_targets_update()`, where HashSet naturally deduplicates
//! any overlap.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use tracing::{debug, info, trace};

use crate::message::NeighborHostingMessage;
use crate::transport::TransportPublicKey;

// Advertisement-layer anti-entropy cadence (#4642 spec step 1, "Fix 1").
//
// The on-connect `HostingStateRequest`/`HostingStateResponse` exchange and the
// per-eviction retraction broadcast are both best-effort; a dropped one leaves a
// co-host with a stale view of what its neighbor hosts (a missed add ⇒ lost
// fan-out; a missed retraction ⇒ a phantom host that poisons fan-out and
// read-routing, invariant 1). The reliability backstop is a periodic full-set
// re-request: it is PIGGYBACKED on the InterestSync `interest_heartbeat` loop
// (`ring.rs`), so it runs on that loop's `INTEREST_HEARTBEAT_INTERVAL` (~5 min)
// cadence and shares its RNG-free scheduling — a separate task with its own
// `GlobalRng` initial-delay draw would perturb the deterministic simulation
// harness. The `HostingStateResponse` handler REPLACES our view of the responding
// neighbor, so both a dropped add and a dropped retraction self-heal within one
// interval. That interval is also the advertisement-staleness / reconcile TOCTOU
// bound the spec calls out: a co-host advertisement can be stale for at most one
// heartbeat interval after a dropped update.

/// Most contract ids this node remembers one neighbour advertising (65,536,
/// about 4 MiB of set per neighbour).
///
/// `neighbor_contracts[peer]` is filled from the peer's own `HostingAnnounce`
/// and `HostingStateResponse` messages and lives as long as the connection, so
/// without a bound one neighbour decides how much heap this node spends on it
/// (ANT-2026-N74RDRBN: 4 x 1,000,000 ids cost about 330 MiB).
///
/// Sizing, from what an honest peer can advertise. A node advertises every
/// contract it hosts, and the hosting cache bounds that count by memory: each
/// hosted contract is charged at least `HOSTED_ENTRY_BYTES` (8 KiB) against a
/// resident-overhead budget of 1/8 of the node's RAM. A 2 GiB node therefore
/// tops out at 32,768 hosted contracts. Measured sets are far smaller: a
/// `hosting_contract_count` telemetry sample (16,578 readings) had median 417
/// and max 2,814, and a busy peer was seen hosting about 6,200. 65,536
/// is twice the 2 GiB ceiling and about 10 times the largest measured set. A
/// node with more than 16 GiB of RAM can hold more than this, and its
/// neighbours then stop consulting it as a co-host for the overflow ids. That
/// is the same best-effort outcome as a dropped announce: freshness of those
/// contracts falls back to the subscription tree and InterestSync anti-entropy
/// (hosting-invariants 1), and nothing is evicted or forgotten elsewhere.
///
/// Ids over the cap are not stored and the peer is not told. Saturation is
/// counted (`over_cap_ids_total`) and logged at `info!` on power-of-two message
/// counts only, so release builds show it while the sender cannot flood the log.
/// Both write sites go through this bound.
pub(crate) const MAX_ADVERTISED_CONTRACTS_PER_NEIGHBOR: usize = 65_536;

/// Result from handling a neighbor hosting message.
///
/// Contains an optional response message to send back, plus any contracts
/// discovered to overlap with the neighbor's hosted contracts (for proactive state sync).
pub struct NeighborHostingResult {
    /// Optional response message to send back to the neighbor.
    pub response: Option<NeighborHostingMessage>,
    /// Contracts that both we and the neighbor are hosting.
    /// Used to trigger proactive state sync via `BroadcastStateChange`.
    pub overlapping_contracts: Vec<ContractInstanceId>,
}

impl NeighborHostingResult {
    /// Shorthand for a response-only result with no overlapping contracts.
    fn response_only(response: NeighborHostingMessage) -> Self {
        Self {
            response: Some(response),
            overlapping_contracts: vec![],
        }
    }

    /// Shorthand for a result with no response and no overlapping contracts.
    fn empty() -> Self {
        Self {
            response: None,
            overlapping_contracts: vec![],
        }
    }
}

/// Manages neighbor hosting tracking for UPDATE forwarding.
///
/// Tracks:
/// - Which contracts this node is hosting locally
/// - Which contracts each connected neighbor is hosting
///
/// This information is used to forward UPDATEs to hosts who have a contract
/// but may not be explicitly subscribed to it.
pub struct NeighborHostingManager {
    /// Contracts we are hosting locally, with the full key of each so the
    /// hosting sweep can reconcile the advertised set against the hosted set
    /// (#5782).
    my_contracts: Arc<DashMap<ContractInstanceId, ContractKey>>,

    /// What we know about our neighbors' hosted contracts.
    /// Maps neighbor public key to the set of contracts they're hosting.
    /// Keyed by TransportPublicKey (stable identity) rather than SocketAddr (mutable
    /// due to NAT/reconnection), so entries survive address changes.
    neighbor_contracts: DashMap<TransportPublicKey, HashSet<ContractInstanceId>>,

    /// Messages whose ids were partly dropped by the per-neighbour cap.
    over_cap_messages: AtomicU64,
    /// Ids dropped by the per-neighbour cap, in total.
    over_cap_ids: AtomicU64,
    /// Inbound messages whose proximity syncs hit the per-message budget.
    sync_budget_hits: AtomicU64,
}

/// Log at `info!` when `count` is a power of two (1, 2, 4, 8, ...): saturation
/// shows up in release builds (`debug!` is compiled out) while the volume of
/// log lines stays logarithmic in a peer-controlled event count.
fn is_log_point(count: u64) -> bool {
    count.is_power_of_two()
}

impl Default for NeighborHostingManager {
    fn default() -> Self {
        Self::new()
    }
}

impl NeighborHostingManager {
    /// Create a new neighbor hosting manager.
    pub fn new() -> Self {
        Self {
            my_contracts: Arc::new(DashMap::new()),
            neighbor_contracts: DashMap::new(),
            over_cap_messages: AtomicU64::new(0),
            over_cap_ids: AtomicU64::new(0),
            sync_budget_hits: AtomicU64::new(0),
        }
    }

    /// Record that `dropped` ids from `from` exceeded the per-neighbour cap.
    fn note_over_cap(&self, from: &TransportPublicKey, dropped: usize) {
        if dropped == 0 {
            return;
        }
        self.over_cap_ids
            .fetch_add(dropped as u64, Ordering::Relaxed);
        let messages = self.over_cap_messages.fetch_add(1, Ordering::Relaxed) + 1;
        if is_log_point(messages) {
            info!(
                peer = %from,
                dropped,
                messages_over_cap = messages,
                cap = MAX_ADVERTISED_CONTRACTS_PER_NEIGHBOR,
                "NEIGHBOR_HOSTING: neighbor advertised more contracts than the per-neighbor cap; overflow ignored (logged at power-of-two counts)"
            );
        }
    }

    /// Record that an inbound message's proximity syncs hit the per-message
    /// budget. Called by the dispatch loop in `node.rs`; logs at `info!` on
    /// power-of-two counts only.
    pub(crate) fn note_sync_budget_hit(&self, from: &TransportPublicKey, budget: usize) {
        let hits = self.sync_budget_hits.fetch_add(1, Ordering::Relaxed) + 1;
        if is_log_point(hits) {
            info!(
                peer = %from,
                budget,
                hits,
                "NEIGHBOR_HOSTING: proximity sync budget hit; the rest is left to InterestSync (logged at power-of-two counts)"
            );
        }
    }

    /// Total ids dropped by the per-neighbour cap since start.
    #[allow(dead_code)]
    pub(crate) fn over_cap_ids_total(&self) -> u64 {
        self.over_cap_ids.load(Ordering::Relaxed)
    }

    /// Total messages whose proximity syncs hit the per-message budget.
    #[allow(dead_code)]
    pub(crate) fn sync_budget_hits_total(&self) -> u64 {
        self.sync_budget_hits.load(Ordering::Relaxed)
    }

    /// Called when we start hosting a new contract (via PUT or successful GET).
    ///
    /// Returns a `HostingAnnounce` message to broadcast to neighbors if this
    /// is a newly hosted contract, or `None` if we already had it.
    pub fn on_contract_hosted(&self, contract_key: &ContractKey) -> Option<NeighborHostingMessage> {
        let contract_id = *contract_key.id();

        if self
            .my_contracts
            .insert(contract_id, *contract_key)
            .is_none()
        {
            info!(
                contract = %contract_key,
                "NEIGHBOR_HOSTING: Added contract to locally hosted"
            );

            Some(NeighborHostingMessage::HostingAnnounce {
                added: vec![contract_id],
                removed: vec![],
                is_response: false,
            })
        } else {
            trace!(
                contract = %contract_key,
                "NEIGHBOR_HOSTING: Contract already hosted locally"
            );
            None
        }
    }

    /// Called when we STOP hosting a contract — the state has left this node
    /// (background eviction; and, once built, evict-to-admit displacement and
    /// interest-gated collapse-then-evict, which all funnel through the same disk
    /// reclamation path). Removes the contract from our advertised set and returns
    /// a retraction `HostingAnnounce{removed}` to broadcast to connected co-hosts,
    /// or `None` if we were not advertising it (idempotent).
    ///
    /// Wiring this is Fix 1's retraction half (#4642 spec step 1). Without it a
    /// co-host keeps a phantom advertisement for a contract we evicted, so it
    /// fans updates to us and routes reads to us for state we no longer hold — the
    /// stale-advertisement failure class (invariant 1: advertise iff a fresh
    /// in-mesh host). Idempotent by construction (`DashSet::remove` returns `None`
    /// on a repeat), so the pending-reclamation retry path at the call site can
    /// re-fire safely; the broadcast only happens on the transition.
    ///
    /// Best-effort like its `on_contract_hosted` sibling: a dropped retraction is
    /// healed by the periodic full-set re-request (piggybacked on the interest
    /// heartbeat, ~5 min).
    pub fn on_contract_unhosted(
        &self,
        contract_key: &ContractKey,
    ) -> Option<NeighborHostingMessage> {
        let contract_id = *contract_key.id();

        if self.my_contracts.remove(&contract_id).is_some() {
            debug!(
                contract = %contract_key,
                "NEIGHBOR_HOSTING: Removed contract from locally hosted (retracting advertisement)"
            );
            crate::config::GlobalTestMetrics::record_neighbor_hosting_retraction();

            Some(NeighborHostingMessage::HostingAnnounce {
                added: vec![],
                removed: vec![contract_id],
                is_response: false,
            })
        } else {
            None
        }
    }

    /// [`Self::on_contract_unhosted`] for a contract we evicted, with the
    /// re-host check applied AFTER the removal so the two cannot interleave
    /// into a wrong result.
    ///
    /// `still_hosted` is the caller's live re-host / re-subscribe check
    /// (`is_hosting_contract || contract_in_use`). It is evaluated only once we
    /// have already taken `key` out of `my_contracts`; when it reports the
    /// contract is wanted again the removal is undone and `None` is returned, so
    /// no retraction goes on the wire and the retraction counter is not
    /// incremented for a retraction that never happened.
    ///
    /// # Why remove-then-check, not check-then-remove
    ///
    /// A concurrent re-host runs `host_contract` (the hosting-cache insert that
    /// makes `still_hosted` true) and only THEN
    /// `operations::announce_contract_hosted` → [`Self::on_contract_hosted`].
    /// Because our removal happens first, every interleaving lands advertised
    /// iff the contract is hosted:
    ///
    /// - re-host insert lands before our check → we observe it, undo, stay
    ///   advertised, and its later `on_contract_hosted` is the usual
    ///   already-present no-op;
    /// - re-host insert lands after our check → we retract, and its later
    ///   `on_contract_hosted` finds `my_contracts` empty for the key, so it
    ///   re-announces (the announce is one-shot per advertised interval, and we
    ///   just ended that interval).
    ///
    /// Check-then-remove has no such ordering: the announce can fire between the
    /// check and the removal, and the removal then silently deletes it, leaving
    /// a hosted-but-unadvertised contract that the ~5-min full-set re-request
    /// CANNOT heal (that exchange replays `my_contracts`, which is the thing
    /// that is wrong).
    pub fn on_contract_unhosted_unless_rehosted(
        &self,
        contract_key: &ContractKey,
        still_hosted: impl FnOnce() -> bool,
    ) -> Option<NeighborHostingMessage> {
        let contract_id = *contract_key.id();
        self.my_contracts.remove(&contract_id)?;
        if still_hosted() {
            // Re-host / re-subscribe raced us: put the advertisement back and
            // emit nothing. Neighbors never observed the removal.
            self.my_contracts.insert(contract_id, *contract_key);
            trace!(
                contract = %contract_key,
                "NEIGHBOR_HOSTING: eviction retraction skipped — contract re-hosted \
                 or back in use"
            );
            return None;
        }
        debug!(
            contract = %contract_key,
            "NEIGHBOR_HOSTING: Removed contract from locally hosted (retracting advertisement \
             for an evicted contract)"
        );
        crate::config::GlobalTestMetrics::record_neighbor_hosting_retraction();
        Some(NeighborHostingMessage::HostingAnnounce {
            added: vec![],
            removed: vec![contract_id],
            is_response: false,
        })
    }

    /// Process an incoming neighbor hosting message from a neighbor.
    ///
    /// Returns a [`NeighborHostingResult`] containing an optional response message
    /// and any overlapping contracts discovered (for proactive state sync).
    pub fn handle_message(
        &self,
        from: &TransportPublicKey,
        message: NeighborHostingMessage,
    ) -> NeighborHostingResult {
        match message {
            NeighborHostingMessage::HostingAnnounce {
                added,
                removed,
                is_response,
            } => {
                // Update the neighbor's view and learn which ids are genuinely
                // NEW from it. The set is never cloned: `insert` reports
                // novelty, so a message costs O(message), not O(stored set).
                // `newly_added` is also de-duplicated (a repeated id is
                // already stored when its second copy is seen) and excludes
                // ids dropped by the per-neighbour cap.
                //
                // Removals are applied BEFORE additions so a capacity-neutral
                // swap at the cap (add X, remove Y) keeps X. An id named in
                // both lists stays removed, as it did when removals ran last,
                // and is not reported as new.
                let mut newly_added: Vec<ContractInstanceId> = Vec::new();
                let mut over_cap = 0usize;
                {
                    let removed_set: HashSet<ContractInstanceId> =
                        removed.iter().copied().collect();
                    let mut apply = |set: &mut HashSet<ContractInstanceId>| {
                        for id in &removed_set {
                            set.remove(id);
                        }
                        for id in &added {
                            if removed_set.contains(id) || set.contains(id) {
                                continue;
                            }
                            if set.len() >= MAX_ADVERTISED_CONTRACTS_PER_NEIGHBOR {
                                over_cap += 1;
                                continue;
                            }
                            set.insert(*id);
                            newly_added.push(*id);
                        }
                    };
                    if let Some(mut entry) = self.neighbor_contracts.get_mut(from) {
                        apply(&mut entry);
                    } else if !added.is_empty() {
                        apply(
                            self.neighbor_contracts
                                .entry(from.clone())
                                .or_default()
                                .value_mut(),
                        );
                    }
                }
                self.note_over_cap(from, over_cap);
                crate::config::GlobalTestMetrics::record_neighbor_hosting_update();

                let neighbor_contracts: usize = self
                    .neighbor_contracts
                    .get(from)
                    .map(
                        |entry: dashmap::mapref::one::Ref<
                            '_,
                            TransportPublicKey,
                            HashSet<ContractInstanceId>,
                        >| entry.value().len(),
                    )
                    .unwrap_or(0);

                info!(
                    peer = %from,
                    total_contracts = neighbor_contracts,
                    is_response = is_response,
                    "NEIGHBOR_HOSTING: Updated neighbor hosting state"
                );

                // Don't respond to responses - this prevents ping-pong.
                // The is_response flag indicates the sender was already responding
                // to our announcement, so they already know we have these contracts.
                //
                // Returning empty overlapping_contracts here means only the initial
                // announcer's side triggers proactive state sync. This is intentional:
                // if the responding peer has newer state, the CRDT merge at the
                // broadcast recipient produces CurrentWon → emits its own
                // BroadcastStateChange back — so stale state self-corrects within
                // one round-trip.
                if is_response {
                    return NeighborHostingResult::empty();
                }

                // Find contracts that are:
                // 1. NEW from this peer (not previously known from them)
                // 2. Also in our local cache
                // We respond so they can include us in their UPDATE targets.
                let overlapping: Vec<ContractInstanceId> = newly_added
                    .into_iter()
                    .filter(|id| self.my_contracts.contains_key(id)) // That we also have
                    .collect();

                let response = if !overlapping.is_empty() {
                    debug!(
                        peer = %from,
                        overlapping_count = overlapping.len(),
                        "NEIGHBOR_HOSTING: Responding with reciprocal announcement for shared contracts"
                    );
                    Some(NeighborHostingMessage::HostingAnnounce {
                        added: overlapping.clone(),
                        removed: vec![],
                        is_response: true, // Mark as response to prevent ping-pong
                    })
                } else {
                    None
                };

                NeighborHostingResult {
                    response,
                    overlapping_contracts: overlapping,
                }
            }

            NeighborHostingMessage::HostingStateRequest => {
                let mut contracts: Vec<ContractInstanceId> =
                    self.my_contracts.iter().map(|r| *r.key()).collect();
                // Sort for deterministic message order (DashSet iteration is non-deterministic).
                // ContractInstanceId doesn't impl Ord, so compare raw bytes. Do NOT sort by
                // `to_string()`: that base58-encodes per comparison (~O(n log n) encodes per
                // request) and was ~40% of CPU on a peer hosting ~6,200 contracts. Ids are
                // unique, so an unstable sort is still deterministic.
                contracts.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

                debug!(
                    peer = %from,
                    hosted_count = contracts.len(),
                    "NEIGHBOR_HOSTING: Responding to hosting state request"
                );

                NeighborHostingResult::response_only(NeighborHostingMessage::HostingStateResponse {
                    contracts,
                })
            }

            NeighborHostingMessage::HostingStateResponse { contracts } => {
                let count = contracts.len();

                // Capture what we previously knew this peer hosted BEFORE the full
                // replace below, so `overlapping` (which drives the proactive
                // state sync) is computed only over NEWLY-learned shared contracts,
                // mirroring the `HostingAnnounce` arm's `previously_known` diff.
                //
                // This is what makes the periodic full-set re-request
                // (piggybacked on the interest heartbeat, Fix 1) cheap and purely
                // advertisement-layer: without the diff, every re-request would
                // re-trigger a targeted state sync for EVERY actively-served shared
                // contract every interval — redundant with the InterestSync STATE
                // anti-entropy and a needless per-interval fetch load. On a fresh
                // connection (or a reconnect, where `on_peer_disconnected` cleared
                // this peer's entry) `previously_known` is empty, so every shared
                // contract counts as new and the reconnection-recovery sync is
                // preserved exactly.
                //
                // The previous set is not cloned: `DashMap::insert` hands it back
                // when the snapshot replaces it, so the diff below reads the old
                // set directly and a snapshot costs O(message), not O(old set).

                // Build the replacement view: de-duplicated and capped at
                // `MAX_ADVERTISED_CONTRACTS_PER_NEIGHBOR`. `shared` records, in
                // message order, each id we also host that made it into the view,
                // so a repeated id is reported once and an over-cap id never.
                let mut view: HashSet<ContractInstanceId> =
                    HashSet::with_capacity(count.min(MAX_ADVERTISED_CONTRACTS_PER_NEIGHBOR));
                let mut shared: Vec<ContractInstanceId> = Vec::new();
                let mut over_cap = 0usize;
                for id in &contracts {
                    if view.contains(id) {
                        continue;
                    }
                    if view.len() >= MAX_ADVERTISED_CONTRACTS_PER_NEIGHBOR {
                        over_cap += 1;
                        continue;
                    }
                    view.insert(*id);
                    if self.my_contracts.contains_key(id) {
                        shared.push(*id);
                    }
                }
                self.note_over_cap(from, over_cap);

                // A `HostingStateResponse` is a FULL SNAPSHOT: it REPLACES our
                // whole view of this peer, so a contract the peer stopped hosting
                // silently disappears from our view here. That full-replace is what
                // lets the periodic re-request heal a dropped retraction (Fix 1).
                let previously_known: HashSet<ContractInstanceId> = self
                    .neighbor_contracts
                    .insert(from.clone(), view)
                    .unwrap_or_default();
                crate::config::GlobalTestMetrics::record_neighbor_hosting_update();

                // Announce back only the NEWLY-learned overlaps so the peer can
                // include us in its UPDATE targets for shared contracts it did not
                // previously know we host.
                let overlapping: Vec<ContractInstanceId> = shared
                    .into_iter()
                    .filter(|id| !previously_known.contains(id)) // Only NEW from this peer
                    .collect();

                info!(
                    peer = %from,
                    contracts = count,
                    overlapping = overlapping.len(),
                    "NEIGHBOR_HOSTING: Received full hosting state from neighbor"
                );

                // CRITICAL: Send back our overlapping contracts so the peer can include us
                // in UPDATE broadcasts. Without this, peers that reconnect after missing an
                // update broadcast won't receive future updates because neighbors don't know
                // they're still caching the contract.
                //
                // This uses is_response=true to prevent ping-pong loops - the peer receiving
                // this announcement won't respond since they already know we requested their state.
                let response = if !overlapping.is_empty() {
                    debug!(
                        peer = %from,
                        overlapping_count = overlapping.len(),
                        "NEIGHBOR_HOSTING: Announcing our overlapping contracts to peer"
                    );
                    Some(NeighborHostingMessage::HostingAnnounce {
                        added: overlapping.clone(),
                        removed: vec![],
                        is_response: true, // Prevent ping-pong
                    })
                } else {
                    None
                };

                NeighborHostingResult {
                    response,
                    overlapping_contracts: overlapping,
                }
            }
        }
    }

    /// [`Self::handle_message`] for a message from a ring neighbour, with the
    /// ring-membership check made AFTER the message is applied (ANT-2026-VLEFNWN1).
    ///
    /// Inbound messages are applied on a task of their own, concurrently with
    /// the event loop's disconnect handling, and building a large snapshot takes
    /// long enough for a whole disconnect to fit in between. Checking membership
    /// first and writing second therefore lets `on_peer_disconnected` run before
    /// the write, find nothing, and leave the late write as an entry for a peer
    /// that is gone, which nothing removes.
    ///
    /// Here the write comes first. A disconnect prunes the ring connection and
    /// only then calls [`Self::on_peer_disconnected`], so either the prune is
    /// visible to `still_connected` after our write (we remove the entry
    /// ourselves), or the clean-up has not run yet and it removes the entry.
    /// `still_connected` is the caller's live "is `from` still in the ring" check.
    /// When it reports the peer gone, the entry is removed and `None` is returned,
    /// so the caller sends no reply and no state sync toward a departed peer.
    pub fn handle_message_unless_disconnected(
        &self,
        from: &TransportPublicKey,
        message: NeighborHostingMessage,
        still_connected: impl FnOnce() -> bool,
    ) -> Option<NeighborHostingResult> {
        let result = self.handle_message(from, message);
        if still_connected() {
            Some(result)
        } else {
            self.on_peer_disconnected(from);
            None
        }
    }

    /// Get the list of neighbors who are hosting a specific contract.
    ///
    /// Returns TransportPublicKey (stable identity) rather than SocketAddr,
    /// so callers can resolve to current addresses via ConnectionManager.
    /// Used by UPDATE operations to find additional targets beyond explicit subscribers.
    pub fn neighbors_with_contract(&self, contract_key: &ContractKey) -> Vec<TransportPublicKey> {
        self.neighbors_with_contract_id(contract_key.id())
    }

    /// Same as [`Self::neighbors_with_contract`] but keyed by
    /// [`ContractInstanceId`] directly.
    ///
    /// Relay operation drivers (GET / SUBSCRIBE) carry the `instance_id`
    /// on the wire rather than a full `ContractKey`, so the terminal
    /// advertisement consult (invariant 5: "findability is routing +
    /// on-demand advertisement") looks up advertised hosts by id.
    pub fn neighbors_with_contract_id(
        &self,
        contract_id: &ContractInstanceId,
    ) -> Vec<TransportPublicKey> {
        let mut neighbors: Vec<TransportPublicKey> = self
            .neighbor_contracts
            .iter()
            .filter(
                |entry: &dashmap::mapref::multiple::RefMulti<
                    '_,
                    TransportPublicKey,
                    HashSet<ContractInstanceId>,
                >| entry.value().contains(contract_id),
            )
            .map(
                |entry: dashmap::mapref::multiple::RefMulti<
                    '_,
                    TransportPublicKey,
                    HashSet<ContractInstanceId>,
                >| entry.key().clone(),
            )
            .collect();

        // Sort for deterministic iteration order (DashMap iteration is non-deterministic)
        neighbors.sort();

        if !neighbors.is_empty() {
            debug!(
                contract = %contract_id,
                neighbor_count = neighbors.len(),
                "NEIGHBOR_HOSTING: Found neighbors with contract"
            );
        }

        neighbors
    }

    /// The set of contract instance-ids `pub_key` currently advertises hosting.
    ///
    /// Read-only reverse lookup (does NOT mutate, unlike
    /// [`Self::on_peer_disconnected`]). The reconcile connection-drop shadow
    /// (keystone step-2, #4642) calls this BEFORE `on_peer_disconnected` clears
    /// the entry, to learn which contracts a dropped peer co-hosted so it can ask
    /// whether the controller would re-root. Returns an empty set for an unknown
    /// peer.
    pub(crate) fn contracts_for_peer(
        &self,
        pub_key: &TransportPublicKey,
    ) -> HashSet<ContractInstanceId> {
        self.neighbor_contracts
            .get(pub_key)
            .map(|entry| entry.value().clone())
            .unwrap_or_default()
    }

    /// Handle peer disconnection by removing their hosting state.
    pub fn on_peer_disconnected(&self, pub_key: &TransportPublicKey) {
        if let Some((_, removed_cache)) = self.neighbor_contracts.remove(pub_key) {
            let count = removed_cache.len();
            debug!(
                peer = %pub_key,
                hosted_contracts = count,
                "NEIGHBOR_HOSTING: Removed disconnected peer from neighbor hosting"
            );
        }
    }

    /// Called when a new ring connection is established.
    ///
    /// Returns a message to send to the peer to exchange hosting state,
    /// enabling UPDATE forwarding to nearby hosts.
    pub fn on_ring_connection_established(
        &self,
        pub_key: &TransportPublicKey,
    ) -> Option<NeighborHostingMessage> {
        debug!(
            peer = %pub_key,
            "NEIGHBOR_HOSTING: New ring connection, requesting hosting state"
        );
        Some(NeighborHostingMessage::HostingStateRequest)
    }

    /// Initialize my_contracts from contracts loaded from disk.
    /// Must be called after loading the hosting cache and before ring connections establish.
    pub fn initialize_from_hosting_cache(&self, contract_keys: impl Iterator<Item = ContractKey>) {
        let mut count = 0;
        for key in contract_keys {
            self.my_contracts.insert(*key.id(), key);
            count += 1;
        }
        if count > 0 {
            info!(
                count,
                "NEIGHBOR_HOSTING: Initialized local hosting from persisted contracts"
            );
        }
    }

    /// Check if we are hosting a contract locally.
    #[allow(dead_code)]
    pub fn is_hosted_locally(&self, contract_key: &ContractKey) -> bool {
        self.my_contracts.contains_key(contract_key.id())
    }

    /// The contracts this node advertises hosting (`my_contracts`).
    pub(crate) fn advertised_contract_keys(&self) -> Vec<ContractKey> {
        self.my_contracts
            .iter()
            .map(|entry| *entry.value())
            .collect()
    }

    /// Get the number of contracts we advertise hosting locally (`my_contracts`).
    #[allow(dead_code)]
    pub fn local_hosted_count(&self) -> usize {
        self.my_contracts.len()
    }

    /// Get the number of neighbors we're tracking.
    #[allow(dead_code)]
    pub fn neighbor_count(&self) -> usize {
        self.neighbor_contracts.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TransportKeypair;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};

    fn test_contract_key() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([1u8; 32]), CodeHash::new([2u8; 32]))
    }

    fn test_contract_key_2() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([3u8; 32]), CodeHash::new([4u8; 32]))
    }

    fn make_pub_key(_seed: u8) -> TransportPublicKey {
        TransportKeypair::new().public().clone()
    }

    #[test]
    fn test_hosting_announcement_on_new_contract() {
        let manager = NeighborHostingManager::new();
        let key = test_contract_key();

        // First hosting should return announcement
        let announcement = manager.on_contract_hosted(&key);
        assert!(announcement.is_some());

        if let Some(NeighborHostingMessage::HostingAnnounce {
            added,
            removed,
            is_response,
        }) = announcement
        {
            assert_eq!(added.len(), 1);
            assert!(removed.is_empty());
            assert_eq!(added[0], *key.id());
            assert!(
                !is_response,
                "Initial announcement should not be a response"
            );
        } else {
            panic!("Expected HostingAnnounce");
        }

        // Second hosting of same contract should return None
        assert!(manager.on_contract_hosted(&key).is_none());
    }

    #[test]
    fn test_contract_unhosting() {
        let manager = NeighborHostingManager::new();
        let key = test_contract_key();

        // Host the contract
        manager.on_contract_hosted(&key);

        // Eviction should return announcement
        let announcement = manager.on_contract_unhosted(&key);
        assert!(announcement.is_some());

        if let Some(NeighborHostingMessage::HostingAnnounce {
            added,
            removed,
            is_response,
        }) = announcement
        {
            assert!(added.is_empty());
            assert_eq!(removed.len(), 1);
            assert_eq!(removed[0], *key.id());
            assert!(
                !is_response,
                "Eviction announcement should not be a response"
            );
        } else {
            panic!("Expected HostingAnnounce");
        }

        // Second eviction should return None
        assert!(manager.on_contract_unhosted(&key).is_none());
    }

    #[test]
    fn test_neighbor_hosting_tracking() {
        let manager = NeighborHostingManager::new();
        let key = test_contract_key();
        let neighbor = make_pub_key(1);

        // Initially no neighbors with contract
        assert!(manager.neighbors_with_contract(&key).is_empty());

        // Receive announcement from neighbor (initial, not a response)
        let msg = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };
        manager.handle_message(&neighbor, msg);

        // Now neighbor should be found
        let neighbors = manager.neighbors_with_contract(&key);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], neighbor);
    }

    #[test]
    fn test_neighbors_with_contract_id_matches_key_lookup() {
        // The terminal advertisement consult (piece C) looks up advertised
        // hosts by ContractInstanceId; it must return the same neighbors as
        // the ContractKey-keyed method.
        let manager = NeighborHostingManager::new();
        let key = test_contract_key();
        let neighbor = make_pub_key(1);

        assert!(manager.neighbors_with_contract_id(key.id()).is_empty());

        manager.handle_message(
            &neighbor,
            NeighborHostingMessage::HostingAnnounce {
                added: vec![*key.id()],
                removed: vec![],
                is_response: false,
            },
        );

        let by_id = manager.neighbors_with_contract_id(key.id());
        let by_key = manager.neighbors_with_contract(&key);
        assert_eq!(by_id, by_key);
        assert_eq!(by_id.len(), 1);
        assert_eq!(by_id[0], neighbor);

        // A contract no neighbor advertised yields no hosts.
        assert!(
            manager
                .neighbors_with_contract_id(test_contract_key_2().id())
                .is_empty()
        );
    }

    #[test]
    fn test_hosting_state_request_response() {
        let manager = NeighborHostingManager::new();
        let key1 = test_contract_key();
        let key2 = test_contract_key_2();
        let neighbor = make_pub_key(1);

        // Host some contracts locally
        manager.on_contract_hosted(&key1);
        manager.on_contract_hosted(&key2);

        // Handle hosting state request
        let result = manager.handle_message(&neighbor, NeighborHostingMessage::HostingStateRequest);
        assert!(result.response.is_some());

        if let Some(NeighborHostingMessage::HostingStateResponse { contracts }) = result.response {
            assert_eq!(contracts.len(), 2);
            assert!(contracts.contains(key1.id()));
            assert!(contracts.contains(key2.id()));
        } else {
            panic!("Expected HostingStateResponse");
        }
    }

    /// The `HostingStateResponse` contract list must be in ascending id-byte order
    /// regardless of insertion order (the backing `DashSet` iterates
    /// non-deterministically), so the message is deterministic. Ids are chosen so
    /// byte order differs from insertion order.
    #[test]
    fn test_hosting_state_response_is_sorted_by_id_bytes() {
        let neighbor = make_pub_key(1);
        let seeds: Vec<u8> = (0..64u8)
            .map(|i| i.wrapping_mul(37).wrapping_add(11))
            .collect();
        let key_for = |s: u8| {
            ContractKey::from_id_and_code(
                ContractInstanceId::new([s; 32]),
                CodeHash::new([s ^ 0xff; 32]),
            )
        };

        let respond = |order: &[u8]| -> Vec<ContractInstanceId> {
            let manager = NeighborHostingManager::new();
            for &s in order {
                manager.on_contract_hosted(&key_for(s));
            }
            match manager
                .handle_message(&neighbor, NeighborHostingMessage::HostingStateRequest)
                .response
            {
                Some(NeighborHostingMessage::HostingStateResponse { contracts }) => contracts,
                other => panic!("Expected HostingStateResponse, got {other:?}"),
            }
        };

        let mut expected: Vec<ContractInstanceId> = seeds
            .iter()
            .map(|&s| ContractInstanceId::new([s; 32]))
            .collect();
        expected.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let insertion_order: Vec<ContractInstanceId> = seeds
            .iter()
            .map(|&s| ContractInstanceId::new([s; 32]))
            .collect();
        assert_ne!(
            insertion_order, expected,
            "test ids must not already be in byte order"
        );

        let mut reversed = seeds.clone();
        reversed.reverse();
        let a = respond(&seeds);
        let b = respond(&reversed);
        assert_eq!(a, expected, "response must be in ascending id-byte order");
        assert_eq!(a, b, "response must not depend on insertion order");
    }

    #[test]
    fn test_peer_disconnection() {
        let manager = NeighborHostingManager::new();
        let key = test_contract_key();
        let neighbor = make_pub_key(1);

        // Add neighbor's cache info
        let msg = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };
        manager.handle_message(&neighbor, msg);

        assert_eq!(manager.neighbor_count(), 1);

        // Disconnect neighbor
        manager.on_peer_disconnected(&neighbor);

        assert_eq!(manager.neighbor_count(), 0);
        assert!(manager.neighbors_with_contract(&key).is_empty());
    }

    fn test_contract_key_3() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([5u8; 32]), CodeHash::new([6u8; 32]))
    }

    // === Bidirectional Announcement Tests ===

    #[test]
    fn test_bidirectional_announcement_for_overlapping_contracts() {
        // Scenario: Node A has contract X hosted. Node B announces it also has X.
        // Expected: Node A should respond with its own announcement so B knows A has X too.
        let manager_a = NeighborHostingManager::new();
        let key = test_contract_key();
        let node_b = make_pub_key(2);

        // Node A caches contract X
        manager_a.on_contract_hosted(&key);

        // Node B announces it has contract X (initial announcement, not a response)
        let announcement_from_b = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };

        // Node A should respond with a reciprocal announcement
        let result = manager_a.handle_message(&node_b, announcement_from_b);
        assert!(
            result.response.is_some(),
            "Expected reciprocal announcement for overlapping contract"
        );
        assert_eq!(result.overlapping_contracts.len(), 1);
        assert_eq!(result.overlapping_contracts[0], *key.id());

        if let Some(NeighborHostingMessage::HostingAnnounce {
            added,
            removed,
            is_response,
        }) = result.response
        {
            assert_eq!(
                added.len(),
                1,
                "Should announce the one overlapping contract"
            );
            assert_eq!(added[0], *key.id());
            assert!(removed.is_empty());
            assert!(
                is_response,
                "Reciprocal announcement should be marked as response"
            );
        } else {
            panic!("Expected HostingAnnounce response");
        }
    }

    #[test]
    fn test_no_response_for_non_overlapping_contracts() {
        // Scenario: Node A has contract X. Node B announces contract Y (which A doesn't have).
        // Expected: No response needed since there's no overlap.
        let manager_a = NeighborHostingManager::new();
        let key_x = test_contract_key();
        let key_y = test_contract_key_2();
        let node_b = make_pub_key(2);

        // Node A only has contract X
        manager_a.on_contract_hosted(&key_x);

        // Node B announces contract Y (which A doesn't have)
        let announcement = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key_y.id()],
            removed: vec![],
            is_response: false,
        };

        let result = manager_a.handle_message(&node_b, announcement);
        assert!(
            result.response.is_none(),
            "No response expected when contracts don't overlap"
        );
        assert!(
            result.overlapping_contracts.is_empty(),
            "No overlapping contracts when contracts don't match"
        );
    }

    #[test]
    fn test_partial_overlap_only_announces_overlapping() {
        // Scenario: Node A has contracts X and Y. Node B announces X and Z.
        // Expected: Node A responds with only X (the overlap), not Y.
        let manager_a = NeighborHostingManager::new();
        let key_x = test_contract_key();
        let key_y = test_contract_key_2();
        let key_z = test_contract_key_3();
        let node_b = make_pub_key(2);

        // Node A has X and Y
        manager_a.on_contract_hosted(&key_x);
        manager_a.on_contract_hosted(&key_y);

        // Node B announces X and Z (overlap is only X)
        let announcement = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key_x.id(), *key_z.id()],
            removed: vec![],
            is_response: false,
        };

        let result = manager_a.handle_message(&node_b, announcement);
        assert!(
            result.response.is_some(),
            "Expected response for partial overlap"
        );
        assert_eq!(
            result.overlapping_contracts.len(),
            1,
            "Only contract X should overlap"
        );
        assert_eq!(result.overlapping_contracts[0], *key_x.id());

        if let Some(NeighborHostingMessage::HostingAnnounce {
            added,
            removed,
            is_response,
        }) = result.response
        {
            assert_eq!(
                added.len(),
                1,
                "Should only announce the overlapping contract X"
            );
            assert_eq!(added[0], *key_x.id());
            assert!(removed.is_empty());
            assert!(is_response, "Response should be marked as response");
        } else {
            panic!("Expected HostingAnnounce response");
        }
    }

    // === Ping-Pong Termination Tests ===

    #[test]
    fn test_ping_pong_terminates_after_one_round() {
        // This is the critical test for preventing infinite announcement loops.
        //
        // Scenario:
        // 1. Node A and B both have contract X
        // 2. A announces X to B (is_response=false)
        // 3. B responds with X (is_response=true, automatically set by handle_message)
        // 4. A receives B's response - should NOT respond because is_response=true
        //
        // The is_response flag explicitly prevents ping-pong by marking responses.

        let manager_a = NeighborHostingManager::new();
        let manager_b = NeighborHostingManager::new();
        let key = test_contract_key();
        let key_a = make_pub_key(1);
        let key_b = make_pub_key(2);

        // Both nodes cache contract X
        manager_a.on_contract_hosted(&key);
        manager_b.on_contract_hosted(&key);

        // Step 1: A announces to B (initial announcement, not a response)
        let a_announcement = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };

        // Step 2: B receives A's announcement, generates reciprocal (with is_response=true)
        let b_result = manager_b.handle_message(&key_a, a_announcement);
        assert!(
            b_result.response.is_some(),
            "B should respond with reciprocal announcement"
        );

        // Verify B's response has is_response=true
        if let Some(NeighborHostingMessage::HostingAnnounce { is_response, .. }) =
            &b_result.response
        {
            assert!(is_response, "B's response should have is_response=true");
        }

        // Step 3: A receives B's response
        let a_second_result = manager_a.handle_message(&key_b, b_result.response.unwrap());

        // Step 4: A should NOT respond again - the loop terminates due to is_response=true
        assert!(
            a_second_result.response.is_none(),
            "Ping-pong must terminate: A should not respond to B's reciprocal announcement"
        );
        assert!(
            a_second_result.overlapping_contracts.is_empty(),
            "is_response=true must not trigger proactive state sync"
        );
    }

    #[test]
    fn test_ping_pong_with_multiple_contracts() {
        // More complex scenario with multiple overlapping contracts.
        // Ensures ping-pong termination works when multiple contracts are involved.

        let manager_a = NeighborHostingManager::new();
        let manager_b = NeighborHostingManager::new();
        let key_x = test_contract_key();
        let key_y = test_contract_key_2();
        let key_a = make_pub_key(1);
        let key_b = make_pub_key(2);

        // Both have X and Y
        manager_a.on_contract_hosted(&key_x);
        manager_a.on_contract_hosted(&key_y);
        manager_b.on_contract_hosted(&key_x);
        manager_b.on_contract_hosted(&key_y);

        // A announces both (initial, not a response)
        let a_announcement = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key_x.id(), *key_y.id()],
            removed: vec![],
            is_response: false,
        };

        // B responds with both (reciprocal, is_response=true set automatically)
        let b_result = manager_b.handle_message(&key_a, a_announcement);
        assert!(b_result.response.is_some());

        if let Some(NeighborHostingMessage::HostingAnnounce {
            ref added,
            is_response,
            ..
        }) = b_result.response
        {
            assert_eq!(
                added.len(),
                2,
                "B should announce both overlapping contracts"
            );
            assert!(is_response, "B's response should have is_response=true");
        }

        // A receives B's response - should NOT respond because is_response=true
        let a_second = manager_a.handle_message(&key_b, b_result.response.unwrap());
        assert!(
            a_second.response.is_none(),
            "Ping-pong must terminate with multiple contracts"
        );
    }

    #[test]
    fn test_new_contract_after_initial_exchange_triggers_response() {
        // Ensures that after initial exchange, a genuinely NEW contract still triggers response.
        //
        // Scenario:
        // 1. A and B exchange announcements for contract X (ping-pong completes)
        // 2. Later, B caches contract Y which A also has
        // 3. B announces Y to A
        // 4. A should respond because Y is NEW (not previously known from B)

        let manager_a = NeighborHostingManager::new();
        let key_x = test_contract_key();
        let key_y = test_contract_key_2();
        let key_b = make_pub_key(2);

        // A has both X and Y
        manager_a.on_contract_hosted(&key_x);
        manager_a.on_contract_hosted(&key_y);

        // Initial exchange: B announces X (initial announcement)
        let b_announces_x = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key_x.id()],
            removed: vec![],
            is_response: false,
        };
        let result_x = manager_a.handle_message(&key_b, b_announces_x);
        assert!(
            result_x.response.is_some(),
            "A should respond to initial X announcement"
        );

        // Later: B announces Y (genuinely new, initial announcement)
        let b_announces_y = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key_y.id()],
            removed: vec![],
            is_response: false,
        };
        let result_y = manager_a.handle_message(&key_b, b_announces_y);
        assert!(
            result_y.response.is_some(),
            "A should respond to NEW contract Y even after X exchange completed"
        );

        if let Some(NeighborHostingMessage::HostingAnnounce { added, .. }) = result_y.response {
            assert_eq!(added.len(), 1);
            assert_eq!(added[0], *key_y.id(), "Response should be for contract Y");
        }
    }

    #[test]
    fn test_re_announcing_known_contract_no_response() {
        // Edge case: B announces contract X that A already knows B has.
        // This could happen if B's announcement is retransmitted or duplicated.
        // A should not respond since X is not new information.

        let manager_a = NeighborHostingManager::new();
        let key = test_contract_key();
        let key_b = make_pub_key(2);

        // A has contract X
        manager_a.on_contract_hosted(&key);

        // First announcement from B (initial, not a response)
        let announcement = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };
        let first_result = manager_a.handle_message(&key_b, announcement.clone());
        assert!(
            first_result.response.is_some(),
            "First announcement should get response"
        );

        // Duplicate/retransmitted announcement from B (still not a response)
        let second_result = manager_a.handle_message(&key_b, announcement);
        assert!(
            second_result.response.is_none(),
            "Duplicate announcement should not trigger response (already known)"
        );
    }

    #[test]
    fn test_bidirectional_awareness_enables_update_forwarding() {
        // Integration-style test verifying the end goal: both nodes can find each other
        // via neighbors_with_contract after the bidirectional exchange.

        let manager_a = NeighborHostingManager::new();
        let manager_b = NeighborHostingManager::new();
        let key = test_contract_key();
        let key_a = make_pub_key(1);
        let key_b = make_pub_key(2);

        // Both cache the contract
        manager_a.on_contract_hosted(&key);
        manager_b.on_contract_hosted(&key);

        // Before any exchange: neither knows about the other
        assert!(
            manager_a.neighbors_with_contract(&key).is_empty(),
            "A shouldn't know about B yet"
        );
        assert!(
            manager_b.neighbors_with_contract(&key).is_empty(),
            "B shouldn't know about A yet"
        );

        // A announces to B (initial announcement, not a response)
        let a_announcement = NeighborHostingMessage::HostingAnnounce {
            added: vec![*key.id()],
            removed: vec![],
            is_response: false,
        };
        let b_result = manager_b.handle_message(&key_a, a_announcement);

        // Now B knows about A
        let b_neighbors = manager_b.neighbors_with_contract(&key);
        assert_eq!(b_neighbors.len(), 1);
        assert_eq!(b_neighbors[0], key_a);

        // A still doesn't know about B
        assert!(
            manager_a.neighbors_with_contract(&key).is_empty(),
            "A still shouldn't know about B"
        );

        // B's response goes to A
        assert!(b_result.response.is_some());
        let _ = manager_a.handle_message(&key_b, b_result.response.unwrap());

        // Now A also knows about B - bidirectional awareness achieved!
        let a_neighbors = manager_a.neighbors_with_contract(&key);
        assert_eq!(a_neighbors.len(), 1);
        assert_eq!(a_neighbors[0], key_b);

        // Both can now forward updates to each other
    }

    // === Reconnection Tests ===
    // These tests cover the scenario where a peer disconnects/reconnects and needs
    // to re-establish bidirectional awareness via HostingStateRequest/Response.

    #[test]
    fn test_hosting_state_response_triggers_announcement_for_overlapping_contracts() {
        // CRITICAL: This test catches the bug where peers that reconnect after missing
        // an update broadcast don't receive future updates.
        //
        // Scenario:
        // 1. Node A has contract X hosted
        // 2. Node B reconnects and has contract X hosted
        // 3. A sends HostingStateRequest to B (via on_ring_connection_established)
        // 4. B responds with HostingStateResponse containing X
        // 5. A receives HostingStateResponse - should announce X back to B
        // 6. Now B knows A also has X and can include A in UPDATE broadcasts
        //
        // Without this fix, step 5 returned None and B never learned A has X.

        let manager_a = NeighborHostingManager::new();
        let key = test_contract_key();
        let key_b = make_pub_key(2);

        // A has contract X hosted locally
        manager_a.on_contract_hosted(&key);

        // B reconnects and sends HostingStateResponse (as if responding to A's request)
        let b_state_response = NeighborHostingMessage::HostingStateResponse {
            contracts: vec![*key.id()],
        };

        // A handles the response - should return announcement for overlapping contract X
        let a_result = manager_a.handle_message(&key_b, b_state_response);

        assert!(
            a_result.response.is_some(),
            "CRITICAL: A must announce overlapping contracts when receiving HostingStateResponse"
        );
        assert_eq!(a_result.overlapping_contracts.len(), 1);
        assert_eq!(a_result.overlapping_contracts[0], *key.id());

        if let Some(NeighborHostingMessage::HostingAnnounce {
            added,
            removed,
            is_response,
        }) = a_result.response
        {
            assert_eq!(
                added.len(),
                1,
                "Should announce the one overlapping contract"
            );
            assert_eq!(added[0], *key.id());
            assert!(removed.is_empty());
            assert!(
                is_response,
                "Should be marked as response to prevent ping-pong"
            );
        } else {
            panic!("Expected HostingAnnounce, got something else");
        }
    }

    #[test]
    fn test_hosting_state_response_no_announcement_when_no_overlap() {
        // When receiving HostingStateResponse with no overlapping contracts,
        // no announcement should be sent.

        let manager_a = NeighborHostingManager::new();
        let key_x = test_contract_key();
        let key_y = test_contract_key_2();
        let key_b = make_pub_key(2);

        // A only has contract X
        manager_a.on_contract_hosted(&key_x);

        // B has contract Y (no overlap with A)
        let b_state_response = NeighborHostingMessage::HostingStateResponse {
            contracts: vec![*key_y.id()],
        };

        let result = manager_a.handle_message(&key_b, b_state_response);
        assert!(
            result.response.is_none(),
            "No announcement needed when no contracts overlap"
        );
        assert!(
            result.overlapping_contracts.is_empty(),
            "No overlapping contracts when caches don't intersect"
        );

        // But B's contracts should still be tracked
        let b_neighbors = manager_a.neighbors_with_contract(&key_y);
        assert_eq!(b_neighbors.len(), 1);
        assert_eq!(b_neighbors[0], key_b);
    }

    #[test]
    fn test_reconnection_bidirectional_awareness_via_state_request_response() {
        // Full reconnection flow test:
        // 1. Both A and B have contract X
        // 2. B disconnects and reconnects
        // 3. A initiates cache state exchange with B
        // 4. After exchange, both should know about each other for UPDATE forwarding

        let manager_a = NeighborHostingManager::new();
        let manager_b = NeighborHostingManager::new();
        let key = test_contract_key();
        let key_a = make_pub_key(1);
        let key_b = make_pub_key(2);

        // Both have contract X hosted
        manager_a.on_contract_hosted(&key);
        manager_b.on_contract_hosted(&key);

        // Initially neither knows about the other
        assert!(manager_a.neighbors_with_contract(&key).is_empty());
        assert!(manager_b.neighbors_with_contract(&key).is_empty());

        // Step 1: A establishes ring connection with B, generates HostingStateRequest
        let a_request = manager_a.on_ring_connection_established(&key_b);
        assert!(matches!(
            a_request,
            Some(NeighborHostingMessage::HostingStateRequest)
        ));

        // Step 2: B receives request, responds with its hosting state
        let b_result = manager_b.handle_message(&key_a, a_request.unwrap());
        assert!(matches!(
            b_result.response,
            Some(NeighborHostingMessage::HostingStateResponse { .. })
        ));

        // Step 3: A receives B's state response
        // CRITICAL: A should now announce its overlapping contracts back to B
        let a_result = manager_a.handle_message(&key_b, b_result.response.unwrap());
        assert!(
            a_result.response.is_some(),
            "A must announce overlapping contracts after receiving B's state"
        );

        // A now knows B has contract X
        let a_neighbors = manager_a.neighbors_with_contract(&key);
        assert_eq!(a_neighbors.len(), 1, "A should know B has contract X");
        assert_eq!(a_neighbors[0], key_b);

        // Step 4: B receives A's announcement
        let b_final = manager_b.handle_message(&key_a, a_result.response.unwrap());
        // B should not respond (is_response=true prevents ping-pong)
        assert!(
            b_final.response.is_none(),
            "B should not respond to A's announcement (is_response=true)"
        );

        // B now knows A has contract X
        let b_neighbors = manager_b.neighbors_with_contract(&key);
        assert_eq!(b_neighbors.len(), 1, "B should know A has contract X");
        assert_eq!(b_neighbors[0], key_a);

        // SUCCESS: Both peers now have bidirectional awareness and can forward updates
    }

    #[test]
    fn test_initialize_from_hosting_cache() {
        let manager = NeighborHostingManager::new();
        let key1 = test_contract_key();
        let key2 = test_contract_key_2();

        // Initially empty
        assert_eq!(manager.local_hosted_count(), 0);

        // Initialize from hosting cache
        manager.initialize_from_hosting_cache(vec![key1, key2].into_iter());

        // Should now report both contracts, under their full keys
        assert_eq!(manager.local_hosted_count(), 2);
        let mut keys = manager.advertised_contract_keys();
        keys.sort_by(|a, b| a.id().as_bytes().cmp(b.id().as_bytes()));
        let code_hashes: Vec<CodeHash> = keys.iter().map(|k| *k.code_hash()).collect();
        assert_eq!(code_hashes, vec![*key1.code_hash(), *key2.code_hash()]);
        assert!(manager.is_hosted_locally(&key1));
        assert!(manager.is_hosted_locally(&key2));

        // HostingStateRequest should return both
        let neighbor = make_pub_key(1);
        let result = manager.handle_message(&neighbor, NeighborHostingMessage::HostingStateRequest);
        if let Some(NeighborHostingMessage::HostingStateResponse { contracts }) = result.response {
            assert_eq!(contracts.len(), 2);
        } else {
            panic!("Expected HostingStateResponse");
        }

        // Hosting an already-initialized contract should return None (no duplicate announcement)
        assert!(manager.on_contract_hosted(&key1).is_none());
    }

    // === Advertisement-layer retraction + reliability (Fix 1, #4642 step 1) ===

    #[test]
    fn test_removed_announce_converges_at_receiver() {
        // Fix 1 retraction propagation: a `HostingAnnounce{removed}` drops the
        // contract from our view of the neighbor, so we stop treating it as a
        // host for Source-1 fan-out and terminal read-routing.
        let manager = NeighborHostingManager::new();
        let key = test_contract_key();
        let neighbor = make_pub_key(1);

        manager.handle_message(
            &neighbor,
            NeighborHostingMessage::HostingAnnounce {
                added: vec![*key.id()],
                removed: vec![],
                is_response: false,
            },
        );
        assert_eq!(
            manager.neighbors_with_contract(&key),
            vec![neighbor.clone()]
        );

        // The neighbor retracts (stopped hosting) → we must drop it.
        let result = manager.handle_message(
            &neighbor,
            NeighborHostingMessage::HostingAnnounce {
                added: vec![],
                removed: vec![*key.id()],
                is_response: false,
            },
        );
        assert!(
            manager.neighbors_with_contract(&key).is_empty(),
            "a removed-announce must retract the neighbor's advertisement"
        );
        // A pure retraction has no overlap to sync back and needs no response.
        assert!(result.response.is_none());
        assert!(result.overlapping_contracts.is_empty());
    }

    #[test]
    fn test_hosting_state_response_full_replace_drops_stale() {
        // The periodic full-set re-request (piggybacked on `interest_heartbeat`,
        // Fix 1) heals a DROPPED retraction: a `HostingStateResponse` is a full snapshot
        // that REPLACES our view of the peer, so a contract the peer stopped
        // hosting disappears even though we never saw its removed-announce.
        let manager = NeighborHostingManager::new();
        let key_x = test_contract_key();
        let key_y = test_contract_key_2();
        let neighbor = make_pub_key(1);

        // We believe the neighbor hosts X and Y (from earlier announces).
        manager.handle_message(
            &neighbor,
            NeighborHostingMessage::HostingAnnounce {
                added: vec![*key_x.id(), *key_y.id()],
                removed: vec![],
                is_response: false,
            },
        );
        assert_eq!(
            manager.neighbors_with_contract(&key_x),
            vec![neighbor.clone()]
        );
        assert_eq!(
            manager.neighbors_with_contract(&key_y),
            vec![neighbor.clone()]
        );

        // A periodic re-request response shows the neighbor now hosts only X (it
        // evicted Y and our retraction was dropped). Full-replace drops Y.
        manager.handle_message(
            &neighbor,
            NeighborHostingMessage::HostingStateResponse {
                contracts: vec![*key_x.id()],
            },
        );
        assert_eq!(
            manager.neighbors_with_contract(&key_x),
            vec![neighbor.clone()]
        );
        assert!(
            manager.neighbors_with_contract(&key_y).is_empty(),
            "a full-set re-request must drop a contract the peer stopped hosting \
             (the dropped-retraction heal)"
        );
    }

    #[test]
    fn test_hosting_state_response_overlap_only_for_newly_learned() {
        // The periodic re-request must NOT re-trigger the proactive state sync for
        // an already-known overlap every interval (that would duplicate the
        // InterestSync STATE anti-entropy and re-arm needless per-interval fetch
        // load). `overlapping` is computed only over NEWLY-learned shared
        // contracts, mirroring the `HostingAnnounce` path.
        let manager = NeighborHostingManager::new();
        let key = test_contract_key();
        let neighbor = make_pub_key(1);

        manager.on_contract_hosted(&key);

        // First response (fresh view): the shared contract is new → overlap.
        let first = manager.handle_message(
            &neighbor,
            NeighborHostingMessage::HostingStateResponse {
                contracts: vec![*key.id()],
            },
        );
        assert_eq!(
            first.overlapping_contracts,
            vec![*key.id()],
            "first response must report the shared contract as a new overlap"
        );

        // Second (periodic) response with the SAME set: already known → no
        // overlap, so no redundant per-interval state sync.
        let second = manager.handle_message(
            &neighbor,
            NeighborHostingMessage::HostingStateResponse {
                contracts: vec![*key.id()],
            },
        );
        assert!(
            second.overlapping_contracts.is_empty(),
            "a periodic re-request of an already-known overlap must NOT re-sync state"
        );
        assert!(second.response.is_none());

        // Reconnection recovery preserved: after a disconnect clears our view, the
        // same shared contract is new again → overlap re-reported (fast recovery).
        manager.on_peer_disconnected(&neighbor);
        let after_reconnect = manager.handle_message(
            &neighbor,
            NeighborHostingMessage::HostingStateResponse {
                contracts: vec![*key.id()],
            },
        );
        assert_eq!(
            after_reconnect.overlapping_contracts,
            vec![*key.id()],
            "after reconnect (view cleared) the overlap must be re-reported"
        );
    }

    #[test]
    fn test_on_contract_unhosted_is_idempotent() {
        // The eviction call site (`reclaim_evicted_contract`) can re-fire on the
        // pending-reclamation retry, so the retraction primitive must be
        // idempotent: it emits the retraction once and returns `None` thereafter.
        let manager = NeighborHostingManager::new();
        let key = test_contract_key();

        manager.on_contract_hosted(&key);
        assert!(manager.is_hosted_locally(&key), "advertised after hosting");

        assert!(
            manager.on_contract_unhosted(&key).is_some(),
            "first unhost emits a retraction"
        );
        assert!(
            !manager.is_hosted_locally(&key),
            "no longer advertised after retraction"
        );
        assert!(
            manager.on_contract_unhosted(&key).is_none(),
            "repeat unhost is a no-op (idempotent) — the retry path must not re-broadcast"
        );
    }

    /// The eviction retraction (#5059) must retract when the contract really is
    /// gone, and must leave the advertisement exactly as it found it when a
    /// re-host raced the decision — including when the contract was never
    /// advertised at all, where the guard must not even be consulted.
    #[test]
    fn on_contract_unhosted_unless_rehosted_retracts_only_when_still_unhosted() {
        let manager = NeighborHostingManager::new();
        let key = test_contract_key();

        // Never advertised: nothing to retract, and the guard is irrelevant.
        assert!(
            manager
                .on_contract_unhosted_unless_rehosted(&key, || panic!(
                    "the re-host guard must not be consulted for an unadvertised contract"
                ))
                .is_none(),
            "an unadvertised contract yields no retraction"
        );

        manager.on_contract_hosted(&key);
        // Re-hosted since the eviction decision: undo, emit nothing, stay advertised.
        assert!(
            manager
                .on_contract_unhosted_unless_rehosted(&key, || true)
                .is_none(),
            "a re-hosted contract must not be retracted"
        );
        let restored = manager.advertised_contract_keys();
        assert_eq!(restored.len(), 1);
        assert_eq!(
            restored[0].code_hash(),
            key.code_hash(),
            "the restored entry must carry the full key (#5782)"
        );
        assert!(
            manager.is_hosted_locally(&key),
            "the advertisement must be restored intact when the guard fires — a \
             hosted-but-unadvertised contract is unreachable by fan-out and the \
             periodic full-set re-request replays this same set, so it cannot heal"
        );

        // Genuinely gone: retract, naming this contract.
        let retraction = manager
            .on_contract_unhosted_unless_rehosted(&key, || false)
            .expect("an evicted contract must be retracted");
        assert!(
            matches!(
                &retraction,
                NeighborHostingMessage::HostingAnnounce { removed, added, .. }
                    if removed == &vec![*key.id()] && added.is_empty()
            ),
            "the retraction must name the evicted contract, and only it: {retraction:?}"
        );
        assert!(!manager.is_hosted_locally(&key));
        assert!(
            manager
                .on_contract_unhosted_unless_rehosted(&key, || false)
                .is_none(),
            "idempotent — a pending-reclamation retry must not re-broadcast"
        );
    }

    // ---- Bounds on peer-controlled state (ANT-2026-N74RDRBN / 27GZI7ZB / VLEFNWN1) ----

    fn id_n(n: u32) -> ContractInstanceId {
        let mut b = [0u8; 32];
        b[..4].copy_from_slice(&n.to_le_bytes());
        ContractInstanceId::new(b)
    }

    fn announce(
        added: Vec<ContractInstanceId>,
        removed: Vec<ContractInstanceId>,
    ) -> NeighborHostingMessage {
        NeighborHostingMessage::HostingAnnounce {
            added,
            removed,
            // A response: no reply is built, only the stored view is exercised.
            is_response: true,
        }
    }

    /// A neighbour can advertise at most `MAX_ADVERTISED_CONTRACTS_PER_NEIGHBOR`
    /// ids through `HostingAnnounce`; the overflow is not stored, ids inside the
    /// cap are, and removals free room for later announcements.
    #[test]
    fn announce_is_capped_per_neighbor() {
        let manager = NeighborHostingManager::new();
        let peer = make_pub_key(1);
        let cap = MAX_ADVERTISED_CONTRACTS_PER_NEIGHBOR as u32;

        manager.handle_message(&peer, announce((0..cap + 10).map(id_n).collect(), vec![]));
        assert_eq!(
            manager.contracts_for_peer(&peer).len(),
            cap as usize,
            "the per-neighbour set must stop growing at the cap"
        );
        assert_eq!(
            manager.neighbors_with_contract_id(&id_n(0)),
            vec![peer.clone()]
        );
        assert!(
            manager.neighbors_with_contract_id(&id_n(cap)).is_empty(),
            "an id past the cap must not be stored"
        );

        // Re-announcing a known id at the cap is not growth and is harmless.
        manager.handle_message(&peer, announce(vec![id_n(0)], vec![]));
        assert_eq!(manager.contracts_for_peer(&peer).len(), cap as usize);

        // Removal frees room; a later announce of the overflow id is then stored.
        manager.handle_message(&peer, announce(vec![], vec![id_n(0)]));
        manager.handle_message(&peer, announce(vec![id_n(cap)], vec![]));
        assert_eq!(manager.contracts_for_peer(&peer).len(), cap as usize);
        assert_eq!(
            manager.neighbors_with_contract_id(&id_n(cap)),
            vec![peer.clone()]
        );
    }

    /// The same bound for an (unsolicited) full `HostingStateResponse`.
    #[test]
    fn state_response_is_capped_per_neighbor() {
        let manager = NeighborHostingManager::new();
        let peer = make_pub_key(1);
        let cap = MAX_ADVERTISED_CONTRACTS_PER_NEIGHBOR as u32;

        manager.handle_message(
            &peer,
            NeighborHostingMessage::HostingStateResponse {
                contracts: (0..cap + 10).map(id_n).collect(),
            },
        );
        assert_eq!(manager.contracts_for_peer(&peer).len(), cap as usize);
        assert!(
            manager
                .neighbors_with_contract_id(&id_n(cap + 5))
                .is_empty()
        );
    }

    /// Duplicate ids inside one message are reported once in
    /// `overlapping_contracts` and once in the reply, so the dispatch loop does
    /// one state fetch and one sync per contract, not one per copy.
    #[test]
    fn duplicate_ids_in_announce_overlap_once() {
        let manager = NeighborHostingManager::new();
        let peer = make_pub_key(1);
        let key = test_contract_key();
        manager.on_contract_hosted(&key);
        let id = *key.id();

        let result = manager.handle_message(
            &peer,
            NeighborHostingMessage::HostingAnnounce {
                added: vec![id; 1000],
                removed: vec![],
                is_response: false,
            },
        );
        assert_eq!(result.overlapping_contracts, vec![id]);
        match result.response {
            Some(NeighborHostingMessage::HostingAnnounce { added, .. }) => {
                assert_eq!(
                    added,
                    vec![id],
                    "the reciprocal announce must not repeat ids"
                )
            }
            other => panic!("expected a reciprocal announce, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_ids_in_state_response_overlap_once() {
        let manager = NeighborHostingManager::new();
        let peer = make_pub_key(1);
        let key = test_contract_key();
        manager.on_contract_hosted(&key);
        let id = *key.id();

        let result = manager.handle_message(
            &peer,
            NeighborHostingMessage::HostingStateResponse {
                contracts: vec![id; 1000],
            },
        );
        assert_eq!(result.overlapping_contracts, vec![id]);
    }

    /// A message applied after its sender's disconnect clean-up must not leave
    /// an entry behind (check-then-write race), for both entry-creating arms.
    #[test]
    fn message_applied_after_disconnect_leaves_no_entry() {
        for snapshot in [false, true] {
            let manager = NeighborHostingManager::new();
            let peer = make_pub_key(1);
            let msg = if snapshot {
                NeighborHostingMessage::HostingStateResponse {
                    contracts: vec![id_n(1), id_n(2)],
                }
            } else {
                announce(vec![id_n(1), id_n(2)], vec![])
            };
            // The real interleaving: the message is written, THEN the
            // disconnect clean-up runs (inside the liveness check, after the
            // write), then the check reports the sender gone. The clean-up
            // alone cannot be relied on here: in the opposite order it finds
            // nothing, which is the race this guards.
            let result = manager.handle_message_unless_disconnected(&peer, msg, || {
                assert!(
                    !manager.contracts_for_peer(&peer).is_empty(),
                    "the message must already be written when liveness is checked"
                );
                manager.on_peer_disconnected(&peer);
                false
            });
            assert!(result.is_none(), "no reply or sync toward a departed peer");
            assert!(
                manager.contracts_for_peer(&peer).is_empty() && manager.neighbor_count() == 0,
                "snapshot={snapshot}: a departed sender's entry must be removed"
            );
        }
    }

    /// The other interleaving: the clean-up already ran (and found nothing)
    /// before the write, so only the post-write undo can remove the entry.
    #[test]
    fn late_write_after_cleanup_is_undone() {
        let manager = NeighborHostingManager::new();
        let peer = make_pub_key(1);
        manager.on_peer_disconnected(&peer);
        let result = manager.handle_message_unless_disconnected(
            &peer,
            NeighborHostingMessage::HostingStateResponse {
                contracts: vec![id_n(1)],
            },
            || false,
        );
        assert!(result.is_none());
        assert_eq!(manager.neighbor_count(), 0, "the late write must be undone");
    }

    /// A capacity-neutral swap at the cap (add X, remove Y) keeps X: removals
    /// are applied before additions.
    #[test]
    fn announce_swap_at_cap_keeps_the_new_id() {
        let manager = NeighborHostingManager::new();
        let peer = make_pub_key(1);
        let cap = MAX_ADVERTISED_CONTRACTS_PER_NEIGHBOR as u32;
        manager.handle_message(&peer, announce((0..cap).map(id_n).collect(), vec![]));
        assert_eq!(manager.contracts_for_peer(&peer).len(), cap as usize);

        manager.handle_message(&peer, announce(vec![id_n(cap)], vec![id_n(0)]));
        assert_eq!(
            manager.neighbors_with_contract_id(&id_n(cap)),
            vec![peer.clone()]
        );
        assert!(manager.neighbors_with_contract_id(&id_n(0)).is_empty());
        assert_eq!(manager.contracts_for_peer(&peer).len(), cap as usize);
    }

    /// An id named in both `added` and `removed` ends up removed and is not
    /// reported as overlapping (no reciprocal announce, no sync).
    #[test]
    fn id_in_both_added_and_removed_is_removed_and_not_overlapping() {
        let manager = NeighborHostingManager::new();
        let peer = make_pub_key(1);
        let key = test_contract_key();
        manager.on_contract_hosted(&key);
        let id = *key.id();
        let result = manager.handle_message(
            &peer,
            NeighborHostingMessage::HostingAnnounce {
                added: vec![id],
                removed: vec![id],
                is_response: false,
            },
        );
        assert!(manager.neighbors_with_contract_id(&id).is_empty());
        assert!(result.overlapping_contracts.is_empty());
        assert!(result.response.is_none());
    }

    /// Saturation is counted in release-visible state, per dropped id.
    #[test]
    fn over_cap_ids_are_counted() {
        let manager = NeighborHostingManager::new();
        let peer = make_pub_key(1);
        let cap = MAX_ADVERTISED_CONTRACTS_PER_NEIGHBOR as u32;
        manager.handle_message(&peer, announce((0..cap + 7).map(id_n).collect(), vec![]));
        assert_eq!(manager.over_cap_ids_total(), 7);
        manager.handle_message(
            &peer,
            NeighborHostingMessage::HostingStateResponse {
                contracts: (0..cap + 3).map(id_n).collect(),
            },
        );
        assert_eq!(manager.over_cap_ids_total(), 10);
    }

    #[test]
    fn message_from_connected_peer_is_applied() {
        let manager = NeighborHostingManager::new();
        let peer = make_pub_key(1);
        let result = manager
            .handle_message_unless_disconnected(&peer, announce(vec![id_n(1)], vec![]), || true)
            .expect("a connected sender's message is applied");
        assert!(result.response.is_none());
        assert_eq!(manager.neighbors_with_contract_id(&id_n(1)), vec![peer]);
    }
}
