//! UPDATE operation: applies a state change to a contract and
//! broadcasts to subscribers.
//!
//! Every UPDATE wire variant dispatches to a driver —
//! `op_ctx_task::start_client_update`, `start_relay_request_update`,
//! `start_relay_broadcast_to`, `start_relay_request_update_streaming`,
//! and `start_relay_broadcast_to_streaming`. The wire-format types,
//! `BroadcastDedupCache`, `update_contract`, the log helpers, and
//! the post-merge propagation helpers survive here because the
//! drivers consume them.

pub(crate) mod op_ctx_task;
pub(crate) mod pending_broadcast;
pub(crate) mod propagation_stats;

use freenet_stdlib::prelude::*;

pub(crate) use self::messages::{BroadcastStreamingPayload, UpdateMsg, UpdateStreamingPayload};
use super::OpError;
use crate::contract::{ContractHandlerEvent, ExecutorError, StoreResponse};
use crate::message::{NodeEvent, Transaction};
use crate::node::OpManager;
use crate::ring::PeerKeyLocation;
use std::collections::VecDeque;
use std::net::SocketAddr;

use dashmap::DashMap;
use tokio::time::Instant;

/// Cache for deduplicating broadcast payloads.
///
/// When the same delta/state is broadcast to us by multiple peers (which is
/// expected in the gossip topology), we skip the expensive WASM merge call
/// for duplicates. Uses ahash for fast hashing of payload bytes.
pub(crate) struct BroadcastDedupCache {
    /// Per-contract dedup entries, newest at back.
    entries: DashMap<ContractKey, VecDeque<DedupEntry>>,
}

struct DedupEntry {
    delta_hash: u64,
    inserted_at: Instant,
}

/// Maximum entries per contract in the dedup cache.
const DEDUP_MAX_ENTRIES_PER_CONTRACT: usize = 64;

/// TTL for dedup entries — entries older than this are evicted.
const DEDUP_TTL: std::time::Duration = std::time::Duration::from_secs(60);

impl BroadcastDedupCache {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Check if this payload was already seen for this contract.
    /// If not, insert it and return `false` (not a duplicate).
    /// If yes, return `true` (duplicate — skip processing).
    ///
    /// `is_delta` distinguishes delta payloads from full state payloads so they
    /// don't collide in the hash space (a delta and full state could have the
    /// same bytes but represent different semantic operations).
    ///
    /// Note: Uses non-cryptographic ahash for speed. A hash collision would
    /// cause a legitimate update to be silently skipped, but the gossip
    /// protocol will deliver the data via another peer eventually.
    pub fn check_and_insert(
        &self,
        key: &ContractKey,
        payload_bytes: &[u8],
        is_delta: bool,
        now: Instant,
    ) -> bool {
        use ahash::AHasher;
        use std::hash::Hasher;

        let mut hasher = AHasher::default();
        // Include payload type discriminant to avoid delta/full-state collisions
        hasher.write_u8(if is_delta { 1 } else { 0 });
        hasher.write(payload_bytes);
        let delta_hash = hasher.finish();

        let mut entry = self.entries.entry(*key).or_default();
        let queue = entry.value_mut();

        // Evict expired entries from the front
        while let Some(front) = queue.front() {
            if now.duration_since(front.inserted_at) > DEDUP_TTL {
                queue.pop_front();
            } else {
                break;
            }
        }

        // Check if hash already exists
        if queue.iter().any(|e| e.delta_hash == delta_hash) {
            return true; // Duplicate
        }

        // Evict oldest if at capacity
        while queue.len() >= DEDUP_MAX_ENTRIES_PER_CONTRACT {
            queue.pop_front();
        }

        queue.push_back(DedupEntry {
            delta_hash,
            inserted_at: now,
        });

        false // Not a duplicate
    }
}

/// Result of `get_broadcast_targets_update()` with skip-reason counters
/// for broadcast delivery diagnostics (issue #3046).
pub(crate) struct BroadcastTargetResult {
    pub targets: Vec<PeerKeyLocation>,
    pub proximity_found: usize,
    pub proximity_resolve_failed: usize,
    /// Interest-manager (Source-2) fan-out was removed in #4642 step 9, so
    /// these are always 0. They are retained for telemetry-schema continuity:
    /// the #4281 propagation-stats window (`propagation_stats.rs`) and the
    /// #3046 broadcast-delivery event (`tracing::register`) still carry them.
    /// See `get_broadcast_targets_update`'s rustdoc.
    pub interest_found: usize,
    pub interest_resolve_failed: usize,
    pub skipped_self: usize,
    pub skipped_sender: usize,
}

pub(crate) struct UpdateExecution {
    pub(crate) value: WrappedState,
    pub(crate) summary: StateSummary<'static>,
    pub(crate) changed: bool,
}

/// Cooldown before retrying a self-healing contract fetch, in milliseconds.
/// 5 minutes: long enough to avoid hammering peers if the contract genuinely
/// doesn't exist, short enough to recover within a session if there was a
/// transient routing failure.
pub(crate) const CONTRACT_FETCH_COOLDOWN_MS: u64 = 300_000;

/// Why a `try_auto_fetch_contract` call was made — decides whether the
/// phantom-interest gate applies.
///
/// The auto-fetch helper is reached from two kinds of site, and only one of
/// them is the #4473 churn:
///
/// * [`AutoFetchReason::Originator`] — a *local client* submitted an UPDATE for
///   a contract whose code/params we lack, and the driver is self-healing so
///   the client's retry can succeed (the `phase = "auto_fetch_originator"`
///   site). This is demand-driven: a real client is waiting on it, exactly
///   like a subscribe-driven fetch, so it must NOT be gated even when no
///   subscription exists yet (a one-shot updater need not be subscribed).
///   Suppressing it would strand the client's UPDATE behind a contract that
///   never gets fetched (Codex review on PR #4489).
/// * [`AutoFetchReason::InboundRelay`] — an *inbound* relayed UPDATE / broadcast
///   failed its merge because we lack the contract. A node carrying phantom
///   interest (the #4404 placement-migration after-effect) hits this on every
///   such message and would spawn a `fetch_contract` sub-op for state nothing
///   on this node depends on — the residual #4473 churn. This is the path the
///   `contract_in_use` gate suppresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AutoFetchReason {
    /// Local-client UPDATE self-heal — demand-driven, never gated.
    Originator,
    /// Inbound relay/broadcast self-heal — gated on `contract_in_use`.
    InboundRelay,
}

impl OpManager {
    /// Trigger a background GET when an UPDATE broadcast fails because the node
    /// doesn't have the contract's parameters (code + params). This self-heals
    /// the node by fetching the contract directly from the UPDATE sender, who
    /// is known to have the contract.
    ///
    /// For [`AutoFetchReason::InboundRelay`] calls this is gated to contracts we
    /// actually have a reason to hold: skipped unless a local client or a
    /// downstream peer is subscribed (`contract_in_use`).
    /// [`AutoFetchReason::Originator`] calls are demand-driven (a local client
    /// is waiting on the retry) and bypass the gate.
    /// Rate-limited: at most one fetch attempt per contract per 5 minutes.
    pub(crate) fn try_auto_fetch_contract(
        &self,
        key: &ContractKey,
        sender_addr: SocketAddr,
        reason: AutoFetchReason,
    ) {
        use crate::config::GlobalSimulationTime;

        let instance_id = *key.id();

        // Don't self-heal-fetch a contract nothing on this node depends on —
        // but ONLY for inbound relay/broadcast self-heal, never for the
        // originator path (see `AutoFetchReason`). An inbound UPDATE/broadcast
        // for a contract we carry only phantom interest in (e.g. the #4404
        // placement migration left stale interest with no subscriber) reaches
        // this path and would spawn a sub-op GET — a `fetch_contract` span
        // burst against state we have no client or downstream subscriber to
        // serve. The existing 5-minute cooldown only *throttles* that to one
        // burst per contract per window; gating on `contract_in_use` (a live
        // local-client subscription or a downstream peer subscriber) stops it
        // at the source. This is the fetch-path analogue of the #4475 / #4482
        // `is_hosting_contract || contract_in_use` summarize gates. We
        // intentionally do NOT also accept `is_hosting_contract` here: on this
        // path the merge already failed because we lack the contract, so
        // hosting is moot; nor a bare upstream network subscription, which
        // `contract_in_use` deliberately excludes (an orphaned upstream sub
        // renews its lease unboundedly and is meant to be torn down, not kept
        // alive by self-heal fetches — see `HostingManager::contract_in_use`).
        // A contract that gains a real local client or downstream subscriber
        // becomes `contract_in_use` and its next inbound UPDATE re-arms this
        // fetch.
        if reason == AutoFetchReason::InboundRelay && !self.ring.contract_in_use(key) {
            tracing::debug!(
                contract = %key,
                sender = %sender_addr,
                "Skipping inbound-relay auto-fetch: no local client or downstream \
                 subscriber depends on this contract (phantom interest)"
            );
            return;
        }

        let now_ms = GlobalSimulationTime::read_time_ms();

        // Atomic rate-limit: try to insert a new entry. If one exists and hasn't
        // expired, bail out. Uses entry API to avoid TOCTOU between check and insert.
        {
            use dashmap::mapref::entry::Entry;
            match self.pending_contract_fetches.entry(instance_id) {
                Entry::Occupied(mut existing) => {
                    let elapsed_ms = now_ms.saturating_sub(*existing.get());
                    if elapsed_ms < CONTRACT_FETCH_COOLDOWN_MS {
                        return; // Still in cooldown
                    }
                    // Cooldown expired — update timestamp while still holding the lock
                    *existing.get_mut() = now_ms;
                }
                Entry::Vacant(slot) => {
                    slot.insert(now_ms);
                }
            }
        }

        // Look up the sender's PeerKeyLocation so we can target them directly
        let sender_pkl = match self.ring.connection_manager.get_peer_by_addr(sender_addr) {
            Some(pkl) => pkl,
            None => {
                tracing::debug!(
                    contract = %key,
                    sender = %sender_addr,
                    "Cannot auto-fetch: UPDATE sender not found in connection manager"
                );
                self.pending_contract_fetches.remove(&instance_id);
                return;
            }
        };

        tracing::info!(
            contract = %key,
            sender = %sender_addr,
            "Auto-fetching contract from UPDATE sender (missing parameters)"
        );

        // Spawn a targeted driver GET. The driver targets `sender_pkl`
        // for its first hop and falls back to `k_closest_potentially_hosting`
        // for any retries. Fire-and-forget — the side effect (contract cached
        // locally via `cache_contract_locally`) is what callers depend on.
        // Outcome is logged inside the driver.
        let _tx = super::get::op_ctx_task::start_targeted_sub_op_get(self, instance_id, sender_pkl);
    }

    /// Resolve the set of peers to broadcast a state update to.
    ///
    /// Targets are the advertised co-hosts from the proximity neighbor cache
    /// (peers who announced, via the advertisement layer, that they host this
    /// contract). Skips the sender (to avoid echo) and this node (unless we are
    /// the local originator). Returns skip-reason counters alongside the
    /// resolved targets for broadcast delivery diagnostics (issue #3046).
    ///
    /// ## Source-1-only live fan-out (#4642 step 9)
    ///
    /// This previously unioned a second source: the interest manager's
    /// `get_interested_peers` (peers who registered interest via the protocol).
    /// That arm was removed. Live fan-out is now advertised-co-hosts-only.
    ///
    /// Removing it does NOT drop a subscriber's update, because a genuine
    /// subscriber never persists in the "interested but no local state" condition
    /// that arm used to cover:
    ///
    /// 1. A subscriber seeds its own baseline state via its subscribe-GET before
    ///    the subscription is finalized. `finalize_originator_subscribe`
    ///    (`subscribe.rs`) awaits `fetch_contract_if_missing` (which drives a
    ///    sub-op GET) and only returns success once current state, including any
    ///    already-committed UPDATE, is stored locally. So no real subscriber is
    ///    left relying on a live fan-out for its first copy of the state. Pinned
    ///    by `subscribe_seeds_state_via_get_before_finalize` (`subscribe.rs`).
    /// 2. Once it holds state it advertises (`announce_contract_hosted`), which
    ///    makes it a Source-1 target for later live fan-out AND gives it a
    ///    non-empty state summary. From then on the summary-based InterestSync
    ///    anti-entropy (`node.rs` Summaries -> `SyncStateToPeer`) and the
    ///    advertisement-reconciliation exchange keep it fresh if it ever misses a
    ///    live update: the ~5-min `ring.rs::interest_heartbeat` sends each
    ///    neighbor a `HostingStateRequest`, the neighbor replies with a
    ///    `HostingStateResponse` snapshot of its hosted set, and we full-replace
    ///    that into our Source-1 view (#4722).
    ///
    /// The summary-based anti-entropy path deliberately does NOT heal a peer with
    /// no state: a `None` summary is skipped by the `zip` staleness gate in
    /// `node.rs`. That is exactly why the subscribe-GET seeding in (1), not the
    /// heartbeat, is the load-bearing safety property. See
    /// `.claude/rules/hosting-invariants.md` invariant 1 and
    /// `docs/design/demand-driven-hosting.md`.
    ///
    /// `InterestManager` itself is retained: it still drives proactive summary
    /// notifications (`send_proactive_summary_notification`), interest-heartbeat
    /// TTL refresh, and eviction demand-counting. Only the state fan-out arm was
    /// removed here.
    pub(crate) fn get_broadcast_targets_update(
        &self,
        key: &ContractKey,
        sender: &SocketAddr,
    ) -> BroadcastTargetResult {
        use std::collections::HashSet;

        let self_addr = self.ring.connection_manager.get_own_addr();
        let is_local_update_initiator = self_addr.as_ref().map(|me| me == sender).unwrap_or(false);

        let mut targets: HashSet<PeerKeyLocation> = HashSet::new();
        let mut proximity_resolve_failed: usize = 0;
        let mut skipped_self: usize = 0;
        let mut skipped_sender: usize = 0;

        // Source 1 (the ONLY live fan-out source): the proximity cache of
        // advertised co-hosts — peers who announced they host this contract via
        // the advertisement layer. The former interest-manager arm (Source 2)
        // was removed in #4642 step 9; see this function's rustdoc.
        let proximity_pub_keys = self.neighbor_hosting.neighbors_with_contract(key);
        let proximity_found = proximity_pub_keys.len();

        for pub_key in proximity_pub_keys {
            if let Some(pkl) = self.ring.connection_manager.get_peer_by_pub_key(&pub_key) {
                if let Some(pkl_addr) = pkl.socket_addr() {
                    if &pkl_addr == sender && !is_local_update_initiator {
                        skipped_sender += 1;
                        continue;
                    }
                    if !is_local_update_initiator && self_addr.as_ref() == Some(&pkl_addr) {
                        skipped_self += 1;
                        continue;
                    }
                }
                targets.insert(pkl);
            } else {
                proximity_resolve_failed += 1;
                // Stale proximity-cache entry (#4756): the neighbor announced it
                // seeds this contract but is no longer in the connection manager.
                // Disconnect teardown prunes the ring connection (keyed by addr)
                // but several paths leave this proximity entry (keyed by pub_key)
                // behind; with no TTL it would WARN on every UPDATE forever.
                // Self-heal at the detection point: reap the disconnected
                // neighbor's proximity state so it fires at most once and then
                // disappears. A reconnecting peer re-announces via the on-connect
                // HostingStateRequest exchange and the periodic full-set
                // re-request, so no fan-out is permanently lost. Demote the
                // per-miss log to DEBUG (mirroring the interest arm below); the
                // aggregate counter (proximity_resolve_failed) still feeds the
                // summary log.
                self.neighbor_hosting.on_peer_disconnected(&pub_key);
                tracing::debug!(
                    contract = %format!("{:.8}", key),
                    proximity_neighbor = %pub_key,
                    is_local = is_local_update_initiator,
                    phase = "target_lookup_failed",
                    "Proximity cache neighbor not found in connection manager; reaped stale entry"
                );
            }
        }

        // Source 2 (interest manager) REMOVED in #4642 step 9. Do NOT re-add a
        // fan-out arm here that unions `interest_manager.get_interested_peers`.
        // A genuine subscriber does not stay "interested but stateless": it seeds
        // baseline state via its own subscribe-GET before finalizing
        // (`finalize_originator_subscribe`), then advertises and becomes a
        // Source-1 target. See this function's rustdoc and
        // `.claude/rules/hosting-invariants.md` invariant 1.

        let mut result: Vec<PeerKeyLocation> = targets.into_iter().collect();
        result.sort();

        if !result.is_empty() {
            tracing::debug!(
                contract = %format!("{:.8}", key),
                peer_addr = %sender,
                targets = %result
                    .iter()
                    .filter_map(|s| s.socket_addr())
                    .map(|addr| format!("{:.8}", addr))
                    .collect::<Vec<_>>()
                    .join(","),
                count = result.len(),
                proximity_sources = proximity_found,
                phase = "broadcast",
                "UPDATE_PROPAGATION"
            );
        } else {
            // NO_TARGETS at DEBUG: this function is called up to 4
            // times per BroadcastStateChange (initial + 3 retries) so
            // per-attempt WARN amplifies 4x on stuck contracts. The
            // operator-actionable signal is the outer streak-suppressed
            // WARN in p2p_protoc.rs (grep for
            // "BROADCAST_NO_TARGETS: no targets found after"); the
            // per-attempt detail belongs in metrics/structured counters
            // (proximity_resolve_failed). Issue #4251 re-review M2.
            tracing::debug!(
                contract = %format!("{:.8}", key),
                peer_addr = %sender,
                self_addr = ?self_addr.map(|a| format!("{:.8}", a)),
                proximity_sources = proximity_found,
                proximity_resolve_failed,
                phase = "warning",
                "UPDATE_PROPAGATION: NO_TARGETS - update will not propagate further"
            );
        }

        BroadcastTargetResult {
            targets: result,
            proximity_found,
            proximity_resolve_failed,
            // Source-2 interest fan-out removed in #4642 step 9; kept at 0 for
            // telemetry-schema continuity (see the struct's field docs).
            interest_found: 0,
            interest_resolve_failed: 0,
            skipped_self,
            skipped_sender,
        }
    }
}

/// Logs the outcome of an `update_contract` failure with severity dependent on
/// whether the rejection is a benign stale-state rejection (INFO) or a real
/// failure (ERROR). See PR #3914.
///
/// The narrow predicate `is_invalid_update_rejection` matches the contract
/// WASM's typed "InvalidUpdate{,WithInfo}" rejection EXCLUSIVELY — OOG /
/// traps / timeouts stay at ERROR.
fn log_update_contract_failure(key: &ContractKey, err: &ExecutorError) {
    if err.is_invalid_update_rejection() {
        tracing::info!(
            contract = %key,
            error = %err,
            event = "merge_rejected_invalid_update",
            "Update rejected by contract: incoming state invalid (likely stale rebroadcast), keeping local"
        );
    } else if err.is_contract_queue_full() {
        // Issue #4251: transient backpressure, not operator-actionable. A
        // hot contract fires this hundreds of times/sec — DEBUG keeps real
        // failures visible at ERROR.
        tracing::debug!(
            contract = %key,
            error = %err,
            event = "queue_full",
            "Update skipped: per-contract queue saturated"
        );
    } else {
        tracing::error!(
            contract = %key,
            error = %err,
            phase = "error",
            "Failed to update contract value"
        );
    }
}

/// Logs the failure outcome of a `BroadcastToStreaming` relay attempt and
/// returns whether the caller should trigger a self-healing GET.
///
/// The two decisions (log severity, auto-fetch) use DIFFERENT predicates
/// because they answer different questions:
///
/// - Log severity uses `is_invalid_update_rejection` (narrow): only the
///   contract WASM's typed "invalid contract update" rejection counts as
///   benign. Out-of-gas, traps, timeouts stay at WARN even though the
///   contract code is present. Issue #4251 adds a third level: queue-full
///   logs at DEBUG (transient backpressure, not an operator-actionable
///   fault).
///
/// - Auto-fetch uses `is_contract_exec_rejection` (broad) AND
///   `!is_contract_queue_full` (issue #4251): any time the contract code
///   DID execute (whether successfully rejecting a stale state or running
///   out of gas), OR the contract's queue is just saturated, the code is
///   present locally and a self-heal GET would be wasted. Auto-fetch only
///   fires for failures where the contract is actually missing (e.g.,
///   missing parameters after restart, storage error).
///
/// Returning `true` means "fetch missing contract code"; returning `false`
/// means "contract is present (or queue saturated), skip auto-fetch".
///
/// Note: this helper takes `&OpError` while `log_update_contract_failure`
/// takes `&ExecutorError` because the two call sites have different error
/// types in scope (the streaming branch operates on the OpError already
/// produced by `update_contract`'s `Err(err.into())`).
pub(crate) fn log_broadcast_to_streaming_failure(
    tx: &Transaction,
    key: &ContractKey,
    err: &OpError,
) -> bool {
    if err.is_invalid_update_rejection() {
        tracing::info!(
            tx = %tx,
            %key,
            error = %err,
            event = "merge_rejected_invalid_update",
            "BroadcastToStreaming merge rejected: incoming state invalid (likely stale rebroadcast), keeping local"
        );
    } else if err.is_contract_queue_full() {
        // Issue #4251: transient backpressure. DEBUG (not WARN), and (via the
        // return value) skip the spurious auto-fetch GET.
        tracing::debug!(
            tx = %tx,
            %key,
            error = %err,
            event = "queue_full",
            "BroadcastToStreaming update skipped: per-contract queue saturated"
        );
    } else if err.is_scheduler_timeout() {
        // #4864 round-7: the merge closure sat queued on a saturated execution
        // pool past the deadline and the guest NEVER ran. The contract IS present
        // (so no auto-fetch — the return value below already gates on
        // is_contract_exec_rejection, which is true for a scheduler timeout), and
        // this fires under exactly the load it represents. DEBUG, not WARN,
        // mirroring the #4251 queue-full treatment: a WARN here would be loud
        // precisely when the node is most overloaded, and it would be factually
        // wrong to blame the contract ("not ready locally") for a pool-saturation
        // event.
        tracing::debug!(
            tx = %tx,
            %key,
            error = %err,
            event = "scheduler_overloaded",
            "BroadcastToStreaming update skipped: execution pool saturated, guest never ran (transient)"
        );
    } else {
        tracing::warn!(
            tx = %tx,
            %key,
            error = %err,
            "BroadcastToStreaming update skipped: contract not ready locally"
        );
    }
    // Self-heal GET only when the contract is genuinely missing. Skip when
    // the WASM merge ran (pre-#3914) or when the queue was saturated (#4251)
    // — in both cases the contract code is present, and enqueuing a GET on
    // a saturated handler only amplifies the storm.
    !err.is_contract_exec_rejection() && !err.is_contract_queue_full()
}

/// Apply an update to a contract.
///
/// This function:
/// 1. Fetches the current state (for change detection)
/// 2. Calls UpdateQuery to merge the update and persist
/// 3. Returns the merged state, summary, and whether the state changed
///
/// The `update_data` parameter can be:
/// - `UpdateData::Delta(delta)` - A delta from the client, merged with current state
/// - `UpdateData::State(state)` - A full state from PUT or executor
pub(crate) async fn update_contract(
    op_manager: &OpManager,
    key: ContractKey,
    update_data: UpdateData<'static>,
    related_contracts: RelatedContracts<'static>,
    priority: crate::contract::Priority,
) -> Result<UpdateExecution, OpError> {
    let previous_state = match op_manager
        .notify_contract_handler_prioritized(
            ContractHandlerEvent::GetQuery {
                instance_id: *key.id(),
                return_contract_code: false,
            },
            priority,
        )
        .await
    {
        Ok(ContractHandlerEvent::GetResponse {
            response: Ok(StoreResponse { state, .. }),
            ..
        }) => state,
        Ok(other) => {
            tracing::trace!(?other, %key, "Unexpected get response while preparing update summary");
            None
        }
        Err(err) => {
            tracing::debug!(%key, %err, "Failed to fetch existing contract state before update");
            None
        }
    };

    match op_manager
        .notify_contract_handler_prioritized(
            ContractHandlerEvent::UpdateQuery {
                key,
                data: update_data.clone(),
                related_contracts,
            },
            priority,
        )
        .await
    {
        Ok(ContractHandlerEvent::UpdateResponse {
            new_value: Ok(new_val),
            state_changed,
        }) => {
            // Invariant: after a successful UPDATE the resulting state must be non-empty.
            // A successful UpdateResponse with an empty value indicates a contract handler bug.
            debug_assert!(
                new_val.size() > 0,
                "update_contract: state must be non-empty after successful UPDATE for contract {key}"
            );
            let new_bytes = State::from(new_val.clone()).into_bytes();
            let summary = StateSummary::from(new_bytes);

            Ok(UpdateExecution {
                value: new_val,
                summary,
                changed: state_changed,
            })
        }
        Ok(ContractHandlerEvent::UpdateResponse {
            new_value: Err(err),
            ..
        }) => {
            log_update_contract_failure(&key, &err);
            Err(err.into())
        }
        Ok(ContractHandlerEvent::UpdateNoChange { .. }) => {
            // Helper to extract state from UpdateData variants that contain state
            fn extract_state_from_update_data(
                update_data: &UpdateData<'static>,
            ) -> Option<WrappedState> {
                match update_data {
                    UpdateData::State(s) => Some(WrappedState::from(s.clone().into_bytes())),
                    UpdateData::StateAndDelta { state, .. }
                    | UpdateData::RelatedState { state, .. }
                    | UpdateData::RelatedStateAndDelta { state, .. } => {
                        Some(WrappedState::from(state.clone().into_bytes()))
                    }
                    UpdateData::Delta(_) | UpdateData::RelatedDelta { .. } => None,
                    // `UpdateData` is `#[non_exhaustive]` since stdlib
                    // 0.6.0. We can't materialize state from an unknown
                    // variant, so return None and let the caller treat
                    // it as "no state to extract."
                    _ => None,
                }
            }

            let resolved_state = match previous_state {
                Some(prev_state) => prev_state,
                None => {
                    // Try to fetch current state from store (same priority as
                    // the rest of this update — keep the ClientLocal lane for a
                    // client UPDATE that CRDT-merges to no change; #4534).
                    let fetched_state = op_manager
                        .notify_contract_handler_prioritized(
                            ContractHandlerEvent::GetQuery {
                                instance_id: *key.id(),
                                return_contract_code: false,
                            },
                            priority,
                        )
                        .await
                        .ok()
                        .and_then(|event| match event {
                            ContractHandlerEvent::GetResponse {
                                response: Ok(StoreResponse { state, .. }),
                                ..
                            } => state,
                            ContractHandlerEvent::DelegateRequest { .. }
                            | ContractHandlerEvent::DelegateResponse(_)
                            | ContractHandlerEvent::ExportUserSecrets { .. }
                            | ContractHandlerEvent::ExportUserSecretsResponse(_)
                            | ContractHandlerEvent::ImportSecrets { .. }
                            | ContractHandlerEvent::ImportSecretsResponse(_)
                            | ContractHandlerEvent::PutQuery { .. }
                            | ContractHandlerEvent::PutResponse { .. }
                            | ContractHandlerEvent::GetQuery { .. }
                            | ContractHandlerEvent::GetResponse { .. }
                            | ContractHandlerEvent::UpdateQuery { .. }
                            | ContractHandlerEvent::UpdateResponse { .. }
                            | ContractHandlerEvent::UpdateNoChange { .. }
                            | ContractHandlerEvent::RegisterSubscriberListener { .. }
                            | ContractHandlerEvent::RegisterSubscriberListenerResponse { .. }
                            | ContractHandlerEvent::QuerySubscriptions { .. }
                            | ContractHandlerEvent::QuerySubscriptionsResponse
                            | ContractHandlerEvent::GetSummaryQuery { .. }
                            | ContractHandlerEvent::GetSummaryResponse { .. }
                            | ContractHandlerEvent::GetDeltaQuery { .. }
                            | ContractHandlerEvent::GetDeltaResponse { .. }
                            | ContractHandlerEvent::ClientDisconnect { .. }
                            | ContractHandlerEvent::EvictContract { .. } => None,
                        });

                    match fetched_state {
                        Some(state) => state,
                        None => {
                            tracing::debug!(
                                %key,
                                "Fallback fetch for UpdateNoChange returned no state; trying to extract from update_data"
                            );
                            match extract_state_from_update_data(&update_data) {
                                Some(state) => state,
                                None => {
                                    tracing::error!(
                                        %key,
                                        "Cannot extract state from delta-only UpdateData in NoChange fallback"
                                    );
                                    return Err(OpError::UnexpectedOpState);
                                }
                            }
                        }
                    }
                }
            };

            let bytes = State::from(resolved_state.clone()).into_bytes();
            let summary = StateSummary::from(bytes);
            Ok(UpdateExecution {
                value: resolved_state,
                summary,
                changed: false,
            })
        }
        Err(err) => Err(err.into()),
        Ok(other) => {
            tracing::error!(event = ?other, contract = %key, phase = "error", "Unexpected event from contract handler during update");
            Err(OpError::UnexpectedOpState)
        }
    }
}

/// Send proactive summary notifications to interested peers after a successful
/// state change. This tells neighbors "my state just updated — here's my new
/// summary" so they can update their cached view of us and skip sending
/// redundant broadcasts.
///
/// Accepts the already-computed summary from `UpdateExecution` to avoid an
/// extra WASM `summarize_state` call.
pub(crate) async fn send_proactive_summary_notification(
    op_manager: &OpManager,
    key: &ContractKey,
    sender_addr: SocketAddr,
    summary: StateSummary<'static>,
) {
    use crate::message::{InterestMessage, SummaryEntry};
    use crate::ring::interest::contract_hash;

    // Throttle: at most one notification per contract per 100ms
    if !op_manager
        .interest_manager
        .should_send_summary_notification(key)
    {
        return;
    }

    // Build the Summaries message with our updated summary
    let hash = contract_hash(key);
    let message = InterestMessage::Summaries {
        entries: vec![SummaryEntry::from_summary(hash, Some(&summary))],
    };

    // Get interested peers and send to each (excluding the sender who just sent us the update)
    let interested = op_manager.interest_manager.get_interested_peers(key);
    let self_addr = op_manager.ring.connection_manager.get_own_addr();

    for (peer_key, _interest) in &interested {
        // Resolve peer to socket address
        let peer_addr = match op_manager
            .ring
            .connection_manager
            .get_peer_by_pub_key(&peer_key.0)
        {
            Some(pkl) => match pkl.socket_addr() {
                Some(addr) => addr,
                None => continue,
            },
            None => continue,
        };

        // Skip sender (they just gave us this data) and ourselves
        if peer_addr == sender_addr {
            continue;
        }
        if self_addr.as_ref() == Some(&peer_addr) {
            continue;
        }

        if let Err(e) = op_manager
            .notify_node_event(NodeEvent::SendInterestMessage {
                target: peer_addr,
                message: message.clone(),
            })
            .await
        {
            tracing::debug!(
                contract = %key,
                peer = %peer_addr,
                error = %e,
                "Failed to send proactive summary notification"
            );
        }
    }

    tracing::debug!(
        contract = %key,
        peer_count = interested.len(),
        "Sent proactive summary notifications after state change"
    );
}

/// Send our current summary to the peer whose broadcast we just rejected,
/// **only when the peer's included summary equals our own** — i.e. the
/// peer and we already agree on state, but the peer's cached view of
/// our summary is stale (`None` or out of date) so their send path
/// fell back to full-state at `broadcast_queue.rs:352-356` instead of
/// hitting the summaries-equal fast-path skip.
///
/// The gate on `sender_summary_bytes == our_summary` is load-bearing.
/// Without it, when our state is strictly ahead of the peer's and we
/// reject their stale broadcast, the `InterestMessage::Summaries`
/// handler at `node.rs:1791-1839` detects a mismatch and pushes the
/// peer's state back via `SyncStateToPeer` — which we then reject
/// again, creating a tight reject→summary→resync→reject loop bounded
/// only by the 60 s `BroadcastDedupCache` (and defeated entirely by
/// payload-byte variation). Restricting to the matching-summary case
/// makes this helper a pure convergence nudge: the `Summaries`
/// receiver's stale-detector returns `is_stale = false`, no
/// `SyncStateToPeer` fires, the only outcome is the sender's
/// peer-summary cache of us flipping from `None` → `Some(our_summary)`
/// so its next broadcast takes the fast-path skip.
///
/// Observed in production (nova, vega): ~80–130 rejections/hour per
/// gateway, all with `incoming_state_size == local_state_size` and
/// "version N == N" — i.e. exactly the same-summary case this helper
/// targets. The 5-min `Interests`/`Summaries` heartbeat eventually
/// populates the sender's cache; this shortcut closes the loop in one
/// round-trip instead of waiting for the next heartbeat tick.
///
/// Unlike `send_proactive_summary_notification` (success path, fans
/// out to all interested peers, explicitly excludes the sender), this
/// targets the single sender — on rejection, the sender is the ONE
/// peer whose cache we know is wrong about us.
///
/// Call sites MUST gate this on `err.is_invalid_update_rejection()`
/// (not the broader `is_contract_exec_rejection`): the stricter
/// predicate matches ONLY the benign "new state version not higher"
/// case, not OOG / `MaxComputeTimeExceeded` / WASM traps / validation
/// failures — which are attacker-inducible and shouldn't amplify into
/// extra messages.
pub(crate) async fn send_summary_back_on_rejection(
    op_manager: &OpManager,
    key: &ContractKey,
    target_addr: SocketAddr,
    sender_summary_bytes: Vec<u8>,
) {
    use crate::message::{InterestMessage, SummaryEntry};
    use crate::ring::interest::contract_hash;

    // Throttle BEFORE the WASM `summarize_state` call. Even with call
    // sites gated on `is_invalid_update_rejection`, a flood of
    // crafted-payload broadcasts that all produce benign rejections
    // would otherwise force one `summarize_state` call per rejection.
    // `should_send_summary_notification` caps at ~10 calls/sec/contract.
    //
    // Sharing the throttle map with `send_proactive_summary_notification`
    // is intentional: both paths emit the same `InterestMessage::Summaries`
    // shape, and the existing throttle's purpose (don't re-spam summary
    // updates for the same contract in bursts) applies to both callers.
    if !op_manager
        .interest_manager
        .should_send_summary_notification(key)
    {
        return;
    }

    let Some(our_summary) = op_manager
        .interest_manager
        .get_contract_summary(op_manager, key)
        .await
    else {
        tracing::debug!(
            contract = %key,
            peer = %target_addr,
            "Skipping summary-back on rejection — no local summary available"
        );
        return;
    };

    // Critical: only proceed when summaries match. See function docs for
    // the SyncStateToPeer-loop rationale. A differing summary means the
    // peer is genuinely out of sync, in which case the 5-min heartbeat
    // is the right convergence mechanism — firing Summaries here would
    // escalate into a per-rejection state ping-pong.
    if our_summary.as_ref() != sender_summary_bytes.as_slice() {
        tracing::debug!(
            contract = %key,
            peer = %target_addr,
            "Skipping summary-back on rejection — sender's summary differs \
             from ours (peer is genuinely out of sync; heartbeat will converge)"
        );
        return;
    }

    let hash = contract_hash(key);
    let message = InterestMessage::Summaries {
        entries: vec![SummaryEntry::from_summary(hash, Some(&our_summary))],
    };

    if let Err(e) = op_manager
        .notify_node_event(NodeEvent::SendInterestMessage {
            target: target_addr,
            message,
        })
        .await
    {
        // info! not debug! — debug! is stripped in release builds via
        // `tracing_max_level_info`, so a saturated event-loop channel
        // would fail silently in production.
        tracing::info!(
            contract = %key,
            peer = %target_addr,
            error = %e,
            "Failed to send summary-back after broadcast rejection"
        );
    }
}

mod messages {
    use std::fmt::Display;

    use freenet_stdlib::prelude::{ContractKey, RelatedContracts, WrappedState};
    use serde::{Deserialize, Serialize};

    use crate::{
        message::{InnerMessage, Transaction},
        ring::Location,
        transport::peer_connection::StreamId,
    };

    /// Payload for streaming UPDATE requests.
    ///
    /// Contains the same data as RequestUpdate but serialized for streaming.
    /// The metadata (key, stream_id, total_size) is sent via RequestUpdateStreaming message.
    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) struct UpdateStreamingPayload {
        #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
        pub related_contracts: RelatedContracts<'static>,
        pub value: WrappedState,
    }

    /// Payload for streaming broadcast updates.
    ///
    /// Contains full state for broadcasting to subscribers via streaming.
    /// Used when the full state is large (>streaming_threshold).
    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) struct BroadcastStreamingPayload {
        /// Full contract state bytes
        pub state_bytes: Vec<u8>,
        /// Sender's current state summary bytes
        pub sender_summary_bytes: Vec<u8>,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    /// Update operation messages.
    ///
    /// Uses hop-by-hop routing for request forwarding. Broadcasting to subscribers
    /// uses explicit addresses since there are multiple targets.
    pub(crate) enum UpdateMsg {
        /// Request to update a contract state. Forwarded hop-by-hop toward contract location.
        RequestUpdate {
            id: Transaction,
            key: ContractKey,
            #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
            related_contracts: RelatedContracts<'static>,
            value: WrappedState,
        },
        /// Broadcasting a change to a specific subscriber.
        ///
        /// Supports delta-based synchronization: when we know the peer's state summary,
        /// we send a delta instead of full state to reduce bandwidth.
        BroadcastTo {
            id: Transaction,
            key: ContractKey,
            /// The payload: either a delta (if we know peer's summary) or full state.
            payload: crate::message::DeltaOrFullState,
            /// Sender's current state summary bytes, so receiver can update their tracking.
            /// Use `StateSummary::from(sender_summary_bytes.clone())` to convert.
            sender_summary_bytes: Vec<u8>,
        },

        // ---- Streaming variants ----
        /// Streaming variant of RequestUpdate for large state updates.
        ///
        /// Used when the state size exceeds the streaming threshold (default 64KB).
        /// The actual state data is sent via a separate stream identified by stream_id.
        RequestUpdateStreaming {
            id: Transaction,
            /// Identifies the stream carrying the update payload
            stream_id: StreamId,
            /// Contract key being updated
            key: ContractKey,
            /// Total size of the streamed payload in bytes
            total_size: u64,
        },

        /// Streaming variant of BroadcastTo for large full state broadcasts.
        ///
        /// Used when broadcasting full state (not delta) and the state size exceeds
        /// the streaming threshold. Deltas are typically small and use regular BroadcastTo.
        BroadcastToStreaming {
            id: Transaction,
            /// Identifies the stream carrying the broadcast payload
            stream_id: StreamId,
            /// Contract key being broadcast
            key: ContractKey,
            /// Total size of the streamed payload in bytes
            total_size: u64,
        },
    }

    impl InnerMessage for UpdateMsg {
        fn id(&self) -> &Transaction {
            match self {
                UpdateMsg::RequestUpdate { id, .. }
                | UpdateMsg::BroadcastTo { id, .. }
                | UpdateMsg::RequestUpdateStreaming { id, .. }
                | UpdateMsg::BroadcastToStreaming { id, .. } => id,
            }
        }

        fn requested_location(&self) -> Option<crate::ring::Location> {
            match self {
                UpdateMsg::RequestUpdate { key, .. }
                | UpdateMsg::BroadcastTo { key, .. }
                | UpdateMsg::RequestUpdateStreaming { key, .. }
                | UpdateMsg::BroadcastToStreaming { key, .. } => Some(Location::from(key.id())),
            }
        }
    }

    impl Display for UpdateMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                UpdateMsg::RequestUpdate { id, .. } => write!(f, "RequestUpdate(id: {id})"),
                UpdateMsg::BroadcastTo { id, .. } => write!(f, "BroadcastTo(id: {id})"),
                UpdateMsg::RequestUpdateStreaming { id, stream_id, .. } => {
                    write!(f, "RequestUpdateStreaming(id: {id}, stream: {stream_id})")
                }
                UpdateMsg::BroadcastToStreaming { id, stream_id, .. } => {
                    write!(f, "BroadcastToStreaming(id: {id}, stream: {stream_id})")
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::wildcard_enum_match_arm)]
mod tests {
    use super::*;
    use crate::operations::test_utils::make_contract_key;

    /// Source-level pin for the UPDATE_PROPAGATION NO_TARGETS log site.
    /// Originally INFO; briefly promoted to WARN in PR #4252 commit 2;
    /// then demoted back to DEBUG in commit 3 because
    /// `get_broadcast_targets_update` is called up to 4 times per
    /// `BroadcastStateChange` (initial + 3 retries) — per-attempt WARN
    /// 4x amplifies on stuck contracts. The operator-actionable summary
    /// lives in the outer streak-suppressed WARN in `p2p_protoc.rs`
    /// (grep `BROADCAST_NO_TARGETS: no targets found after`). Per #4251
    /// re-review M2 (skeptical).
    #[test]
    fn no_targets_propagation_logs_at_debug_pin_test() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/operations/update.rs");
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("must read own source at {}: {e}", path.display()));
        let needle = "UPDATE_PROPAGATION: NO_TARGETS - update will not propagate further";
        let idx = source
            .find(needle)
            .expect("NO_TARGETS log message must still exist in source");
        // Anchor on the closest preceding `tracing::` macro (rfind) rather
        // than a byte window, so the assertion is immune to refactors that
        // move the target site relative to other nearby tracing macros.
        // Adopted from the #4272 pin tests; see those for rationale.
        let preceding = &source[..idx];
        let macro_idx = preceding
            .rfind("tracing::")
            .expect("a tracing macro must precede the NO_TARGETS log site");
        let line_start = preceding[..macro_idx].rfind('\n').map_or(0, |n| n + 1);
        let line_prefix = &preceding[line_start..macro_idx];
        assert!(
            line_prefix.chars().all(char::is_whitespace),
            "rfind matched `tracing::` inside a string literal or comment, \
             not a macro invocation. Prefix on its line: {line_prefix:?}"
        );
        let after_macro = &preceding[macro_idx + "tracing::".len()..];
        let macro_name = after_macro.split('!').next().unwrap_or("");
        let tail = &preceding[preceding.len().saturating_sub(200)..];
        assert_eq!(
            macro_name, "debug",
            "NO_TARGETS log site must be DEBUG to avoid 4x amplification on retries \
             (closest preceding macro is `tracing::{macro_name}!`). \
             Re-promotion to WARN/INFO regresses #4251 review M2.\n\
             Preceding source (last 200 bytes):\n{tail}"
        );
    }

    /// Source-level pin for the `UPDATE_PROPAGATION` broadcast (populated-
    /// targets) branch. Fires once per fan-out per UPDATE — at INFO it was
    /// ~43% of the post-#4252 log volume on a River-subscribed peer (see
    /// #4251 follow-up). The `phase = "broadcast",` literal disambiguates
    /// this site from the NO_TARGETS branch pinned above (whose phase is
    /// `"warning"`).
    ///
    /// Anchored on the *closest* preceding `tracing::` macro via `rfind`
    /// rather than a byte-window scan, so the assertion can't false-pass
    /// when the macro's arg list grows and a window-based check sees an
    /// earlier unrelated `tracing::debug!` site.
    #[test]
    fn broadcast_propagation_logs_at_debug_pin_test() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/operations/update.rs");
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("must read own source at {}: {e}", path.display()));
        let needle = "phase = \"broadcast\",";
        let idx = source
            .find(needle)
            .expect("UPDATE_PROPAGATION broadcast log site must still exist in source");
        let preceding = &source[..idx];
        let macro_idx = preceding
            .rfind("tracing::")
            .expect("a tracing macro must precede the broadcast log site");
        let line_start = preceding[..macro_idx].rfind('\n').map_or(0, |n| n + 1);
        let line_prefix = &preceding[line_start..macro_idx];
        assert!(
            line_prefix.chars().all(char::is_whitespace),
            "rfind matched `tracing::` inside a string literal or comment, \
             not a macro invocation. Prefix on its line: {line_prefix:?}"
        );
        let after_macro = &preceding[macro_idx + "tracing::".len()..];
        let macro_name = after_macro.split('!').next().unwrap_or("");
        let tail = &preceding[preceding.len().saturating_sub(200)..];
        assert_eq!(
            macro_name, "debug",
            "UPDATE_PROPAGATION broadcast log site must be DEBUG \
             (closest preceding macro is `tracing::{macro_name}!`). \
             Re-promotion to INFO/WARN restores the #4251 / #4272 log-volume regression.\n\
             Preceding source (last 200 bytes):\n{tail}"
        );
    }

    /// Regression tests for issue #3914: misleading ERROR/WARN log noise from
    /// benign WASM rejections of stale broadcast UPDATEs. The contract correctly
    /// rejects an incoming state at a version we already hold (a re-broadcast
    /// the dedup cache missed). On production gateways this generated 80-130
    /// ERROR-level lines per hour per gateway. The tests below pin both that
    /// the benign case is now INFO AND that real WASM failures (out-of-gas,
    /// max-compute-time, traps) stay at ERROR/WARN, so the predicate cannot
    /// silently broaden and hide real failures.
    mod log_severity {
        use super::*;
        use crate::contract::ExecutorError;
        use crate::test_utils::TestLogger;
        use freenet_stdlib::client_api::{ContractError as StdContractError, RequestError};

        // Constructors use `From<RequestError> for ExecutorError` (a public
        // impl) rather than the module-private `ExecutorError::request`, so
        // these helpers don't require widening any visibility for tests.
        fn invalid_update_rejection() -> ExecutorError {
            // Mirrors the production cause string exactly: stdlib's
            // `update_exec_error` prefixes "execution error: " and the
            // contract WASM's `InvalidUpdateWithInfo` Display produces
            // "invalid contract update, reason: ...".
            let req: RequestError = StdContractError::update_exec_error(
                make_contract_key(1),
                "invalid contract update, reason: New state version 100 must be higher than current version 100",
            )
            .into();
            req.into()
        }

        fn out_of_gas_failure() -> ExecutorError {
            // Real WASM fault: contract ran out of gas. Same `update_exec_error`
            // wrapper as the benign case (so the loose `is_contract_exec_rejection`
            // predicate matches both), but the cause string starts with
            // "execution error: The operation ran out of gas..." which the
            // tighter `is_invalid_update_rejection` predicate must REJECT.
            let req: RequestError = StdContractError::update_exec_error(
                make_contract_key(1),
                "The operation ran out of gas. This might be caused by an infinite loop or an inefficient computation.",
            )
            .into();
            req.into()
        }

        fn missing_parameters_failure() -> ExecutorError {
            // Real failure case: contract not ready locally, auto-fetch needed.
            let req: RequestError = StdContractError::Update {
                key: make_contract_key(2),
                cause: "missing contract parameters".into(),
            }
            .into();
            req.into()
        }

        fn queue_full_failure() -> ExecutorError {
            // Mirrors `send_queue_full_response` (contract.rs).
            ExecutorError::other(crate::contract::ContractQueueFull)
        }

        fn scheduler_timeout_failure() -> ExecutorError {
            // Build via the HOST path (#4864 round-9): a real scheduler-overload
            // timeout carries the unforgeable typed provenance
            // (classify_result → ContractExecError::SchedulerOverloaded → execution),
            // which is what is_scheduler_timeout now keys on. A plain
            // update_exec_error string would NOT classify — that is exactly the
            // contract-forge case the typing rejects.
            ExecutorError::test_host_scheduler_timeout(make_contract_key(1))
        }

        #[test]
        fn update_contract_failure_logs_info_for_invalid_update_rejection() {
            let logger = TestLogger::new().capture_logs().with_level("info").init();

            log_update_contract_failure(&make_contract_key(1), &invalid_update_rejection());

            assert!(
                logger.contains("merge_rejected_invalid_update"),
                "expected event=merge_rejected_invalid_update in logs, got: {:?}",
                logger.logs()
            );
            assert!(
                logger.contains("INFO"),
                "expected INFO-level log for invalid-update rejection, got: {:?}",
                logger.logs()
            );
            assert!(
                !logger.logs().iter().any(|l| l.contains("ERROR")),
                "invalid-update rejection must not produce ERROR-level logs, got: {:?}",
                logger.logs()
            );
        }

        #[test]
        fn update_contract_failure_logs_error_for_real_failure() {
            let logger = TestLogger::new().capture_logs().with_level("info").init();

            log_update_contract_failure(&make_contract_key(2), &missing_parameters_failure());

            assert!(
                logger.contains("ERROR"),
                "real failures must remain ERROR-level, got: {:?}",
                logger.logs()
            );
            assert!(
                logger.contains("Failed to update contract value"),
                "expected ERROR message text, got: {:?}",
                logger.logs()
            );
        }

        /// CRITICAL: out-of-gas comes through the same `update_exec_error`
        /// wrapper as the benign rejection, so the loose
        /// `is_contract_exec_rejection` predicate matches it (used for the
        /// auto-fetch gate, where this is correct: contract code IS present).
        /// But for log severity, OOG is a real bug operators must see and
        /// MUST stay at ERROR. This test pins that.
        #[test]
        fn update_contract_failure_logs_error_for_out_of_gas() {
            let logger = TestLogger::new().capture_logs().with_level("info").init();

            log_update_contract_failure(&make_contract_key(1), &out_of_gas_failure());

            assert!(
                logger.contains("ERROR"),
                "out-of-gas must remain ERROR-level (real WASM fault), got: {:?}",
                logger.logs()
            );
            assert!(
                !logger
                    .logs()
                    .iter()
                    .any(|l| l.contains("merge_rejected_invalid_update")),
                "out-of-gas must NOT be classified as a benign rejection, got: {:?}",
                logger.logs()
            );
        }

        #[test]
        fn broadcast_to_streaming_failure_logs_info_and_skips_auto_fetch_for_invalid_update() {
            let logger = TestLogger::new().capture_logs().with_level("info").init();
            let tx = Transaction::new::<UpdateMsg>();
            let err: OpError = invalid_update_rejection().into();

            let needs_auto_fetch =
                log_broadcast_to_streaming_failure(&tx, &make_contract_key(1), &err);

            assert!(
                !needs_auto_fetch,
                "invalid-update rejection must NOT trigger self-heal auto-fetch (contract code is present)"
            );
            assert!(
                logger.contains("merge_rejected_invalid_update"),
                "expected event=merge_rejected_invalid_update in logs, got: {:?}",
                logger.logs()
            );
            assert!(
                !logger.logs().iter().any(|l| l.contains("WARN")),
                "invalid-update rejection must not produce WARN-level logs (the old misleading 'contract not ready locally' line), got: {:?}",
                logger.logs()
            );
            assert!(
                !logger
                    .logs()
                    .iter()
                    .any(|l| l.contains("contract not ready locally")),
                "the misleading 'contract not ready locally' message must not appear for invalid-update rejections, got: {:?}",
                logger.logs()
            );
        }

        #[test]
        fn broadcast_to_streaming_failure_logs_warn_and_triggers_auto_fetch_for_real_failure() {
            let logger = TestLogger::new().capture_logs().with_level("info").init();
            let tx = Transaction::new::<UpdateMsg>();
            let err: OpError = missing_parameters_failure().into();

            let needs_auto_fetch =
                log_broadcast_to_streaming_failure(&tx, &make_contract_key(2), &err);

            assert!(
                needs_auto_fetch,
                "real failures must trigger self-heal auto-fetch"
            );
            assert!(
                logger.contains("WARN"),
                "real failures remain WARN-level for the streaming branch, got: {:?}",
                logger.logs()
            );
            assert!(
                logger.contains("contract not ready locally"),
                "expected the WARN message text for real failure, got: {:?}",
                logger.logs()
            );
        }

        /// Issue #4251 regression: when the per-contract fair queue
        /// rejects an event, `log_update_contract_failure` must classify
        /// it as `is_contract_queue_full` and log at DEBUG (NOT ERROR).
        /// A hot contract that saturates its own queue would otherwise
        /// flood logs with ERROR-level "Failed to update contract value"
        /// lines (~30% of all log volume on production gateways).
        #[test]
        fn update_contract_failure_logs_debug_for_queue_full() {
            let logger = TestLogger::new().capture_logs().with_level("debug").init();

            log_update_contract_failure(&make_contract_key(1), &queue_full_failure());

            assert!(
                logger.contains("queue_full"),
                "queue-full must emit event=queue_full, got: {:?}",
                logger.logs()
            );
            assert!(
                !logger.logs().iter().any(|l| l.contains("ERROR")),
                "queue-full must NOT log at ERROR (it's transient backpressure, not a fault), got: {:?}",
                logger.logs()
            );
            assert!(
                !logger
                    .logs()
                    .iter()
                    .any(|l| l.contains("Failed to update contract value")),
                "queue-full must NOT emit the legacy ERROR message, got: {:?}",
                logger.logs()
            );
        }

        /// Issue #4251 regression: the streaming branch's helper MUST
        /// (1) log queue-full at DEBUG, NOT WARN — a buggy contract that
        /// floods broadcasts must not also flood operator logs; and
        /// (2) return `false` so the caller skips
        /// `try_auto_fetch_contract` — enqueuing a GET right back onto
        /// the saturated handler is the amplification path the typed
        /// predicate exists to break.
        #[test]
        fn broadcast_to_streaming_failure_skips_auto_fetch_and_uses_debug_for_queue_full() {
            let logger = TestLogger::new().capture_logs().with_level("debug").init();
            let tx = Transaction::new::<UpdateMsg>();
            let err: OpError = queue_full_failure().into();

            let needs_auto_fetch =
                log_broadcast_to_streaming_failure(&tx, &make_contract_key(3), &err);

            assert!(
                !needs_auto_fetch,
                "queue-full MUST NOT trigger self-heal auto-fetch — enqueuing a GET \
                 onto the saturated handler is exactly the amplification we're \
                 trying to break (issue #4251)"
            );
            assert!(
                logger.contains("queue_full"),
                "queue-full must emit event=queue_full in the streaming branch, got: {:?}",
                logger.logs()
            );
            assert!(
                !logger.logs().iter().any(|l| l.contains("WARN")),
                "queue-full must NOT log at WARN — a saturated contract would otherwise \
                 fill operator dashboards with false-alarm WARNs, got: {:?}",
                logger.logs()
            );
            assert!(
                !logger
                    .logs()
                    .iter()
                    .any(|l| l.contains("contract not ready locally")),
                "the misleading 'contract not ready locally' message must not appear \
                 for queue-full — the contract IS present, the queue is just busy, \
                 got: {:?}",
                logger.logs()
            );
        }

        /// #4864 round-7: a SCHEDULER timeout (the merge closure sat queued on a
        /// saturated execution pool and the guest never ran) must log at DEBUG,
        /// NOT WARN — like queue-full (#4251), a WARN here would be loud precisely
        /// under the overload it represents — and must NOT blame the contract with
        /// the "contract not ready locally" message (the contract IS present).
        /// Auto-fetch is skipped (contract present ⇒ is_contract_exec_rejection).
        #[test]
        fn broadcast_to_streaming_failure_skips_auto_fetch_and_uses_debug_for_scheduler_timeout() {
            let logger = TestLogger::new().capture_logs().with_level("debug").init();
            let tx = Transaction::new::<UpdateMsg>();
            let err: OpError = scheduler_timeout_failure().into();
            // Precondition: the fixture really is classified as a scheduler timeout.
            assert!(
                err.is_scheduler_timeout(),
                "fixture must classify as a scheduler timeout"
            );

            let needs_auto_fetch =
                log_broadcast_to_streaming_failure(&tx, &make_contract_key(1), &err);

            assert!(
                !needs_auto_fetch,
                "scheduler timeout MUST NOT trigger self-heal auto-fetch — the contract \
                 IS present, the pool was just saturated; enqueuing a GET onto the \
                 saturated handler is the amplification we avoid"
            );
            assert!(
                logger.contains("scheduler_overloaded"),
                "scheduler timeout must emit event=scheduler_overloaded, got: {:?}",
                logger.logs()
            );
            assert!(
                !logger.logs().iter().any(|l| l.contains("WARN")),
                "scheduler timeout must NOT log at WARN — it fires under exactly the \
                 saturation it represents, got: {:?}",
                logger.logs()
            );
            assert!(
                !logger
                    .logs()
                    .iter()
                    .any(|l| l.contains("contract not ready locally")),
                "the misleading 'contract not ready locally' message must not appear \
                 for a scheduler timeout — the contract IS present, got: {:?}",
                logger.logs()
            );
        }

        /// Mirror of `update_contract_failure_logs_error_for_out_of_gas` for
        /// the streaming branch: OOG must stay at WARN, AND auto-fetch must
        /// be SKIPPED because the contract code is present (broader predicate
        /// `is_contract_exec_rejection` correctly catches this case). The two
        /// decisions are deliberately decoupled by the helper.
        #[test]
        fn broadcast_to_streaming_failure_logs_warn_and_skips_auto_fetch_for_out_of_gas() {
            let logger = TestLogger::new().capture_logs().with_level("info").init();
            let tx = Transaction::new::<UpdateMsg>();
            let err: OpError = out_of_gas_failure().into();

            let needs_auto_fetch =
                log_broadcast_to_streaming_failure(&tx, &make_contract_key(1), &err);

            assert!(
                logger.contains("WARN"),
                "out-of-gas must remain WARN-level for the streaming branch, got: {:?}",
                logger.logs()
            );
            assert!(
                !logger
                    .logs()
                    .iter()
                    .any(|l| l.contains("merge_rejected_invalid_update")),
                "out-of-gas must NOT be classified as a benign rejection, got: {:?}",
                logger.logs()
            );
            assert!(
                !needs_auto_fetch,
                "out-of-gas must NOT trigger self-heal auto-fetch (contract code is present locally; the broader is_contract_exec_rejection predicate catches this case independently of log severity)"
            );
        }
    }

    /// Pin: `send_summary_back_on_rejection` helper MUST gate on
    /// `sender_summary_bytes` matching our current summary before sending
    /// `InterestMessage::Summaries`. Without this gate, a mismatch triggers
    /// `SyncStateToPeer` at `node.rs:1791-1839` which re-sends the sender's
    /// stale state back to us, creating a reject→summary→resync→reject
    /// loop. ALSO pins the throttle-before-WASM ordering to prevent
    /// attacker-induced `summarize_state` amplification under rejection
    /// floods. See the helper's docs and PR description.
    #[test]
    fn summary_back_helper_gates_on_summary_equality() {
        let src = include_str!("update.rs");
        let fn_start = src
            .find("pub(crate) async fn send_summary_back_on_rejection(")
            .expect("send_summary_back_on_rejection fn not found");
        let fn_end_offset = src[fn_start..]
            .find("\n}\n")
            .expect("send_summary_back_on_rejection fn close not found");
        let fn_body = &src[fn_start..fn_start + fn_end_offset];

        assert!(
            fn_body.contains("sender_summary_bytes"),
            "send_summary_back_on_rejection must take sender_summary_bytes \
             as a parameter"
        );

        // Pin throttle-before-WASM ordering. Without this, an attacker
        // inducing rejections can force one `summarize_state` WASM call per
        // rejection — the equality gate below guards only the outgoing send.
        let throttle_pos = fn_body
            .find("should_send_summary_notification")
            .expect("helper MUST call should_send_summary_notification (throttle)");
        let wasm_call_pos = fn_body
            .find("get_contract_summary")
            .expect("helper must call get_contract_summary to compute our_summary");
        assert!(
            throttle_pos < wasm_call_pos,
            "should_send_summary_notification MUST run before get_contract_summary \
             — otherwise attacker-induced rejections force unbounded WASM amplification"
        );

        // Pin the EXACT inequality direction: on difference → early return.
        // If the direction is reversed (== → early return, meaning "send only
        // when they differ"), the helper reintroduces the SyncStateToPeer
        // loop that this PR fixes.
        let inequality_check_pos = fn_body
            .find("our_summary.as_ref() != sender_summary_bytes.as_slice()")
            .expect(
                "helper MUST use `our_summary.as_ref() != sender_summary_bytes.as_slice()` \
                 as the gate condition (reversed direction reintroduces the SyncStateToPeer \
                 loop — see node.rs:1791-1839)",
            );
        let after_check = &fn_body[inequality_check_pos..];
        let return_pos = after_check.find("return;").expect(
            "gate must early-return on inequality (reversed direction would bypass \
             the SyncStateToPeer safeguard)",
        );
        let notify_pos = after_check
            .find("notify_node_event")
            .expect("helper must still call notify_node_event on the equality path");
        assert!(
            return_pos < notify_pos,
            "early return on inequality MUST precede the notify_node_event \
             send — otherwise mismatched summaries would still be transmitted"
        );
    }

    /// Pin (#4473): `try_auto_fetch_contract` MUST gate on
    /// `self.ring.contract_in_use(...)` and early-return BEFORE it spawns a
    /// sub-op GET (`start_targeted_sub_op_get`) — but ONLY for the
    /// `AutoFetchReason::InboundRelay` path. Without the gate, an inbound
    /// relayed UPDATE for a contract this node carries only phantom interest in
    /// (no client, no downstream subscriber) spawns a `fetch_contract` sub-op
    /// every time the per-contract cooldown lapses — the residual #4473 churn
    /// the gate stops at the source. The existing 5-minute cooldown only
    /// throttles that; it does not prevent it. If a refactor drops this gate or
    /// moves it after the spawn, the storm regresses silently (all behavioural
    /// tests still pass), so pin the ordering at the source level.
    ///
    /// Equally important (Codex review on #4489): the gate MUST stay scoped to
    /// `InboundRelay`, never unconditional. Making it unconditional again would
    /// re-suppress the `Originator` self-heal path, stranding a local client's
    /// UPDATE behind a contract that never gets fetched. Pin that the gate
    /// condition mentions `AutoFetchReason::InboundRelay`.
    #[test]
    fn try_auto_fetch_contract_gates_on_contract_in_use_before_spawn() {
        let src = include_str!("update.rs");
        let fn_start = src
            .find("pub(crate) fn try_auto_fetch_contract(")
            .expect("try_auto_fetch_contract not found");
        let fn_src = &src[fn_start..];

        let gate_pos = fn_src.find("self.ring.contract_in_use(key)").expect(
            "try_auto_fetch_contract MUST gate on self.ring.contract_in_use(key) \
             so phantom-interest contracts are not auto-fetched (#4473)",
        );
        // The gate condition must be scoped to the inbound-relay reason so the
        // originator self-heal path is never suppressed (#4489).
        let reason_pos = fn_src.find("AutoFetchReason::InboundRelay").expect(
            "the contract_in_use gate MUST be conditioned on \
             AutoFetchReason::InboundRelay so Originator self-heal bypasses it (#4489)",
        );
        let return_pos = fn_src[gate_pos..]
            .find("return;")
            .map(|p| gate_pos + p)
            .expect("the contract_in_use gate must early-return when not in use");
        let spawn_pos = fn_src
            .find("start_targeted_sub_op_get")
            .expect("try_auto_fetch_contract must still spawn the sub-op GET on the in-use path");

        assert!(
            reason_pos < gate_pos,
            "the AutoFetchReason::InboundRelay guard MUST precede the \
             contract_in_use check, or the gate becomes unconditional and \
             re-suppresses the Originator self-heal path (#4489)"
        );
        assert!(
            gate_pos < return_pos && return_pos < spawn_pos,
            "the contract_in_use gate + early return MUST precede the \
             start_targeted_sub_op_get spawn, or phantom-interest contracts \
             regress the #4473 fetch_contract churn"
        );
    }

    /// Pin (#4489): the originator self-heal site MUST call
    /// `try_auto_fetch_contract` with `AutoFetchReason::Originator`, and every
    /// inbound relay/broadcast site MUST use `AutoFetchReason::InboundRelay`.
    /// If a refactor flips the originator site to `InboundRelay`, a one-shot
    /// client UPDATE for a contract we lack would be silently suppressed by the
    /// phantom-interest gate (the Codex finding on #4489); if it flips a relay
    /// site to `Originator`, the #4473 churn regresses. Pin both at the source.
    #[test]
    fn auto_fetch_call_sites_use_correct_reason() {
        let driver_src = include_str!("update/op_ctx_task.rs");

        // The originator site is the one paired with the `auto_fetch_originator`
        // tracing phase; it must use the Originator (ungated) reason.
        let originator_anchor = driver_src
            .find("phase = \"auto_fetch_originator\"")
            .expect("originator auto-fetch site (auto_fetch_originator phase) not found");
        let originator_call = driver_src[originator_anchor..]
            .find("try_auto_fetch_contract(")
            .map(|p| originator_anchor + p)
            .expect("originator site must still call try_auto_fetch_contract");
        let originator_reason = driver_src[originator_call..]
            .find("AutoFetchReason::")
            .map(|p| originator_call + p)
            .expect("originator try_auto_fetch_contract call must pass an AutoFetchReason");
        assert!(
            driver_src[originator_reason..].starts_with("AutoFetchReason::Originator"),
            "the auto_fetch_originator site MUST pass AutoFetchReason::Originator \
             so a local client's UPDATE self-heal is never gated (#4489)"
        );

        // No inbound relay/broadcast site may use Originator: those are the
        // #4473 churn paths and must stay gated.
        assert!(
            !driver_src.contains("sender_addr, AutoFetchReason::Originator"),
            "inbound relay/broadcast auto-fetch sites (keyed on sender_addr) MUST \
             use AutoFetchReason::InboundRelay, not Originator, or #4473 regresses"
        );
    }
}
