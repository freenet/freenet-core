//! Outbound broadcast, interest-propagation, and state-sync helpers for
//! [`P2pConnManager`].
//!
//! Behavior-preserving extraction from `p2p_protoc.rs`.

use super::*;

impl P2pConnManager {
    /// Broadcast a message to a list of peers off the event loop.
    ///
    /// In production, the sends are spawned via `tokio::spawn` to prevent
    /// self-deadlock: the bridge channel is drained by the event loop, so
    /// inline fan-out of >capacity sends would block forever.
    /// In simulation tests, sends run inline for determinism.
    pub(super) async fn broadcast_to_peers(
        bridge: &P2pBridge,
        peers: Vec<SocketAddr>,
        msg: crate::message::NetMessage,
    ) {
        let bridge = bridge.clone();

        let send_all = async move {
            for peer_addr in peers {
                // channel-safety: ok — in production this future is detached via
                // `tokio::spawn(send_all)` below, so the blocking bridge.send cannot
                // stall the event loop; it is only awaited inline under
                // `simulation_tests` (single-threaded deterministic ordering).
                if let Err(e) = bridge.send(peer_addr, msg.clone()).await {
                    tracing::warn!(
                        peer_addr = %peer_addr,
                        error = %e,
                        "Failed to send broadcast message to peer"
                    );
                }
            }
        };

        #[cfg(not(feature = "simulation_tests"))]
        tokio::spawn(send_all);

        #[cfg(feature = "simulation_tests")]
        send_all.await;
    }

    /// Broadcast a hosting update message to all connected peers.
    pub(super) async fn handle_hosting_broadcast(
        &mut self,
        message: crate::message::NeighborHostingMessage,
    ) {
        tracing::debug!(
            message = ?message,
            peer_count = self.connections.len(),
            phase = "broadcast",
            "Broadcasting hosting update to connected peers"
        );

        let msg = crate::message::NetMessage::V1(crate::message::NetMessageV1::NeighborHosting {
            message: message.clone(),
        });
        let peers: Vec<SocketAddr> = self.connections.keys().copied().collect();
        Self::broadcast_to_peers(&self.bridge, peers, msg).await;
    }

    /// Broadcast ChangeInterests message to all connected peers.
    pub(super) async fn handle_broadcast_change_interests(
        &mut self,
        added: Vec<u32>,
        removed: Vec<u32>,
    ) {
        tracing::debug!(
            added_count = added.len(),
            removed_count = removed.len(),
            peer_count = self.connections.len(),
            "Broadcasting ChangeInterests to connected peers"
        );

        let msg = crate::message::NetMessage::V1(crate::message::NetMessageV1::InterestSync {
            message: crate::message::InterestMessage::ChangeInterests { added, removed },
        });
        let peers: Vec<SocketAddr> = self.connections.keys().copied().collect();
        Self::broadcast_to_peers(&self.bridge, peers, msg).await;
    }

    /// Send an interest message to a specific peer.
    pub(super) async fn handle_send_interest_message(
        &mut self,
        target: SocketAddr,
        message: crate::message::InterestMessage,
    ) {
        tracing::debug!(
            target = %target,
            "Sending interest message to peer"
        );

        let msg =
            crate::message::NetMessage::V1(crate::message::NetMessageV1::InterestSync { message });

        // Non-blocking (#4145): this handler is dispatched INLINE on the
        // event-loop task from the NodeEvent::SendInterestMessage arm, so
        // bridge.send()'s await on the cap-512 ev_listener_tx (drained by this
        // same task) would self-stall. A dropped InterestSync is recovered by
        // the next periodic InterestSync exchange / summary reconciliation.
        if let Err(e) = self.bridge.try_send(target, msg) {
            tracing::warn!(
                peer_addr = %target,
                error = %e,
                "Failed to send interest message to peer (will re-sync periodically)"
            );
        }
    }

    /// Send an arbitrary `NetMessage` to a target peer without registering
    /// a `pending_op_results` callback for the message's transaction.
    ///
    /// Mirrors `handle_send_interest_message` but accepts a
    /// fully-formed `NetMessage`. Used by the CONNECT joiner to
    /// emit `ConnectFailed` upstream without disturbing its own
    /// multi-reply receiver slot.
    pub(super) async fn handle_send_net_message(
        &mut self,
        target: SocketAddr,
        msg: crate::message::NetMessage,
    ) {
        tracing::debug!(
            target = %target,
            tx = %msg.id(),
            "Sending net message to peer"
        );

        // Non-blocking (#4145): dispatched INLINE on the event-loop task from
        // the NodeEvent::SendNetMessage arm, so bridge.send()'s await on the
        // cap-512 ev_listener_tx (drained by this same task) would self-stall.
        // This path carries the CONNECT joiner's upstream ConnectFailed; if the
        // message is dropped here, the upstream connect operation still fails on
        // its own OPERATION_TTL rather than hanging — so dropping is safe, and a
        // wedged event loop (the alternative) would be strictly worse.
        if let Err(e) = self.bridge.try_send(target, msg) {
            tracing::warn!(
                peer_addr = %target,
                error = %e,
                "Failed to send net message to peer (upstream op falls back to TTL)"
            );
        }
    }

    /// Broadcast ReadyState to all connected ring peers.
    pub(super) async fn handle_broadcast_ready_state(&mut self, ready: bool) {
        let msg =
            crate::message::NetMessage::V1(crate::message::NetMessageV1::ReadyState { ready });

        tracing::info!(
            ready,
            peer_count = self.connections.len(),
            "Broadcasting ReadyState to connected peers"
        );

        let peers: Vec<SocketAddr> = self.connections.keys().copied().collect();
        Self::broadcast_to_peers(&self.bridge, peers, msg).await;
    }

    /// Maximum retry attempts when a broadcast finds no targets.
    const MAX_BROADCAST_RETRIES: u8 = 3;

    /// Base delay between broadcast retries (scaled by attempt number for linear backoff).
    const BROADCAST_RETRY_BASE_DELAY: Duration = Duration::from_secs(1);

    /// Maximum entries in the no-target streak tracker. Prevents unbounded growth
    /// from network-influenced contract keys.
    const MAX_BROADCAST_STREAK_ENTRIES: usize = 256;

    /// Notify interested network peers about a state change.
    ///
    /// Echo-back is prevented by summary comparison: we skip peers whose cached
    /// summary matches ours (they already have our state).
    ///
    /// When no targets are found (race between contract updates and subscription
    /// establishment), the broadcast is retried with linear backoff up to
    /// [`Self::MAX_BROADCAST_RETRIES`] times.
    pub(super) async fn handle_broadcast_state_change(
        &mut self,
        op_manager: &Arc<OpManager>,
        key: freenet_stdlib::prelude::ContractKey,
        new_state: freenet_stdlib::prelude::WrappedState,
        // `false` for a fresh executor-emitted broadcast, `true` for a
        // no-target retry re-emission this handler scheduled. Used only to
        // count fresh logical broadcasts once for the #4281 summary.
        is_retry: bool,
        // `true` when this is a deferred re-emission of a stashed fresh-contract
        // broadcast (issue #4359). Suppresses re-recording a `no_targets`
        // propagation-summary event on give-up (the originating PUT already
        // counted one when it first gave up) so a flush per interested peer
        // does not inflate the #4281 stats.
        is_reemit: bool,
    ) {
        tracing::debug!(
            contract = %key,
            state_size = new_state.size(),
            "BroadcastStateChange event received"
        );

        // Phase 7 egress self-block (#4300). A locally-applied UPDATE
        // (e.g. delegate-driven) drives this fan-out to subscribers who
        // don't know about our ban. If we have banned the contract, skip
        // the fan-out entirely — and the no-target retry re-emission with
        // it — rather than push state for a contract we have decided is
        // harmful. There is no client to notify here (this is the
        // fire-and-forget broadcast path), so we skip with a debug log
        // instead of returning a typed error. Complements the
        // client-originated UPDATE gate in `start_client_update` and the
        // receive-side `UpdateMsg::*` drop in node.rs (PR #4299).
        if op_manager.ring.contract_ban_list.is_banned(key.id()) {
            tracing::debug!(
                contract = %key,
                phase = "broadcast_state_change_banned_skip",
                "skipping state-change broadcast for banned contract"
            );
            // Drop any in-flight retry/streak bookkeeping for this
            // contract: if it was banned mid-retry-cycle, a previously
            // scheduled re-emission would otherwise leave a stale entry
            // here that nothing clears until the contract is unbanned and
            // broadcast again. Mirrors the cleanup the targets-found and
            // retries-exhausted paths perform.
            self.broadcast_retries.remove(&key);
            self.broadcast_no_target_streak.remove(&key);
            return;
        }

        let self_addr = op_manager.ring.connection_manager.get_own_addr();
        let Some(self_addr) = self_addr else {
            tracing::warn!(
                contract = %key,
                "Cannot broadcast state change - no own address"
            );
            return;
        };

        let target_result = op_manager.get_broadcast_targets_update(&key, &self_addr);
        tracing::debug!(
            contract = %key,
            target_count = target_result.targets.len(),
            self_addr = %self_addr,
            "BroadcastStateChange: found targets"
        );

        if target_result.targets.is_empty() {
            // Record the NO_TARGETS outcome once per *fresh* no-target broadcast
            // for the #4281 summary, but NOT for retry re-emissions. Gating on
            // `is_retry` (carried on the event) rather than the per-contract
            // `broadcast_retries` counter is what makes the count accurate:
            // retries and fresh broadcasts share that counter, so a fresh
            // broadcast arriving mid-retry-cycle would otherwise be absorbed
            // and uncounted (Codex P2). With the explicit flag, every fresh
            // no-target broadcast is counted exactly once and retries never
            // double-count. If a later retry heals (finds targets), the
            // targets-found path below additionally records that success — both
            // the initial miss and the eventual propagation are surfaced.
            //
            // `is_reemit` (issue #4359) is also excluded: a deferred re-emission
            // of a stashed fresh-contract broadcast that STILL finds no targets
            // must not re-record a `no_targets` event — the originating PUT
            // already counted one when it first gave up, and counting again per
            // interested-peer flush would inflate the #4281 no_targets/targets_avg
            // stats.
            if !is_retry && !is_reemit {
                op_manager.update_propagation_stats.record_broadcast(
                    *key.id(),
                    0,
                    target_result.interest_resolve_failed,
                );
            }

            let retry_count = self.broadcast_retries.entry(key).or_insert(0);
            if *retry_count < Self::MAX_BROADCAST_RETRIES {
                *retry_count += 1;
                let attempt = *retry_count;
                tracing::info!(
                    contract = %key,
                    self_addr = %self_addr,
                    attempt,
                    max_retries = Self::MAX_BROADCAST_RETRIES,
                    "BROADCAST_NO_TARGETS: scheduling retry - subscriptions may still be in-flight"
                );
                // Schedule a delayed re-emission of BroadcastStateChange.
                // DELIBERATELY blocking — this whole block runs inside a
                // `tokio::spawn`, so the blocking await cannot wedge the
                // event loop, only its own detached task. Switching to
                // `try_notify_node_event` here would silently drop the
                // retry in the exact recovery path the executor-side
                // try_notify sites depend on for healing.
                //
                // Spawn-task accumulation is bounded per-contract by
                // `MAX_BROADCAST_RETRIES = 3` and
                // `BROADCAST_RETRY_BASE_DELAY = 1s`. Worst-case under
                // simultaneous wedges is
                // `MAX_BROADCAST_RETRIES × concurrent_contracts_in_flight`
                // spawn tasks blocked up to 30 s each on the same
                // saturated channel — bounded by the small per-tick
                // contract-touch count, so soft memory pressure only.
                // (Per PR #4231 re-review.)
                let op_mgr = op_manager.clone();
                let delay = Self::BROADCAST_RETRY_BASE_DELAY * u32::from(attempt);
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    if let Err(e) = op_mgr
                        .notify_node_event(crate::message::NodeEvent::BroadcastStateChange {
                            key,
                            new_state,
                            is_retry: true,
                            is_reemit: false,
                        })
                        .await
                    {
                        tracing::warn!(
                            contract = %key,
                            error = %e,
                            "Failed to re-emit BroadcastStateChange for retry"
                        );
                    }
                });
            } else {
                // Retries exhausted. Track consecutive no-target cycles to
                // suppress repetitive WARN logging after the first occurrence.
                self.broadcast_retries.remove(&key);

                // Issue #4359: a fresh contract id loses the race between this
                // broadcast give-up (~6 s) and the much slower interest/
                // subscription resolve for a never-before-seen key. Instead of
                // permanently abandoning the state — which silently leaves it
                // locally-hosted only while every other node GETs NotFound —
                // stash it. When the first interested peer/subscriber for this
                // contract appears later, the interest path drains the stash
                // and re-emits a single BroadcastStateChange, which then finds
                // the freshly-registered target and propagates. The store is
                // bounded by size and TTL so a churn of fresh ids that never
                // gain a subscriber cannot pin memory.
                op_manager
                    .pending_broadcasts
                    .stash(*key.id(), new_state.clone());

                // Lost-wakeup guard (#4359 re-review): stash() runs here on the
                // event-loop task while the interest-resolve flush runs on the
                // subscribe/op driver task — they are concurrent with no
                // happens-before. An interested peer could have registered (and
                // already flushed → found nothing to take) in the window between
                // `get_broadcast_targets_update` above and this stash. That would
                // leave the state stashed with the only interested peer already
                // past its flush, draining nothing until the NEXT subscriber or
                // the TTL. Re-resolve targets now that the stash is in place: if a
                // target has appeared, take the stash back and re-emit
                // immediately rather than waiting on a future flush.
                let recheck = op_manager.get_broadcast_targets_update(&key, &self_addr);
                if !recheck.targets.is_empty() {
                    if let Some(stashed) = op_manager.pending_broadcasts.take(key.id()) {
                        tracing::debug!(
                            contract = %key,
                            target_count = recheck.targets.len(),
                            phase = "pending_broadcast_stash_recheck",
                            "A target appeared while giving up; re-emitting the deferred \
                             fresh-contract broadcast immediately instead of leaving it \
                             stashed for a future flush (#4359)"
                        );
                        if let Err(e) = op_manager.try_notify_node_event(
                            crate::message::NodeEvent::BroadcastStateChange {
                                key,
                                new_state: stashed.clone(),
                                is_retry: false,
                                is_reemit: true,
                            },
                        ) {
                            // Channel full/closed: re-stash so the next interest
                            // flush still recovers it. Losing it would re-open the
                            // locally-hosted-only failure this fix closes.
                            op_manager.pending_broadcasts.stash(*key.id(), stashed);
                            tracing::debug!(
                                contract = %key,
                                error = %e,
                                "pending_broadcast_stash_recheck: immediate re-emit dropped; \
                                 re-stashed for the next interest signal"
                            );
                        }
                    }
                }

                // Evict oldest entry if at capacity to prevent unbounded growth.
                if !self.broadcast_no_target_streak.contains_key(&key)
                    && self.broadcast_no_target_streak.len() >= Self::MAX_BROADCAST_STREAK_ENTRIES
                {
                    if let Some(evict_key) = self.broadcast_no_target_streak.keys().next().cloned()
                    {
                        self.broadcast_no_target_streak.remove(&evict_key);
                    }
                }
                let streak = self.broadcast_no_target_streak.entry(key).or_insert(0);
                *streak = streak.saturating_add(1);
                let current_streak = *streak;

                if current_streak <= 1 {
                    tracing::warn!(
                        contract = %key,
                        self_addr = %self_addr,
                        "BROADCAST_NO_TARGETS: no targets found after {} retries, giving up",
                        Self::MAX_BROADCAST_RETRIES
                    );
                } else {
                    tracing::debug!(
                        contract = %key,
                        self_addr = %self_addr,
                        consecutive_misses = current_streak,
                        "BROADCAST_NO_TARGETS: still no targets (suppressing repeated warns)"
                    );
                }

                // Emit delivery summary for diagnostics (only on first miss per streak)
                if current_streak <= 1 {
                    let update_tx =
                        crate::message::Transaction::new::<crate::operations::update::UpdateMsg>();
                    if let Some(log) = NetEventLog::broadcast_delivery_summary(
                        &update_tx,
                        &op_manager.ring,
                        key,
                        &target_result,
                        0,
                        0,
                        0,
                    ) {
                        self.bridge
                            .log_register
                            .register_events(Either::Left(log))
                            .await;
                    }
                }
                // The NO_TARGETS outcome was already recorded for the #4281
                // summary at the start of this retry cycle (retry_count == 0
                // above); retry exhaustion does not record again, so a stuck
                // contract counts as one no_targets per fresh broadcast, not
                // once per retry.
            }
            return;
        }

        // Targets found - clear any pending retry and streak state for this contract
        self.broadcast_retries.remove(&key);
        self.broadcast_no_target_streak.remove(&key);
        // Also drop any deferred re-broadcast stash (#4359): this fan-out is
        // reaching targets now, so a previously-abandoned state for this
        // contract is superseded and must not be re-emitted later as stale.
        let _ = op_manager.pending_broadcasts.take(key.id());

        // Record a successful fan-out for the #4281 propagation summary. Fires
        // whether targets were found on the initial attempt or on a healing
        // retry, so a recovered in-flight-subscription race is surfaced as a
        // propagated update (with its real target count). A fresh broadcast
        // that found targets on the first try has no `broadcast_retries` entry,
        // so it is recorded exactly once here.
        op_manager.update_propagation_stats.record_broadcast(
            *key.id(),
            target_result.targets.len(),
            target_result.interest_resolve_failed,
        );

        // In production, enqueue each target into the broadcast queue.
        // The queue worker handles delta computation and streaming with bounded
        // concurrency (default: 2 concurrent streams) to prevent uplink saturation.
        //
        // In simulation tests, call inline to preserve deterministic message
        // ordering — tokio::spawn in start_paused(true) changes broadcast
        // delivery order and breaks convergence.
        #[cfg(not(feature = "simulation_tests"))]
        {
            for target in &target_result.targets {
                self.broadcast_queue
                    .enqueue(key, target.clone(), new_state.clone())
                    .await;
            }
            // Emit broadcast emitted telemetry (issue #3622)
            //
            // Production path limitations:
            // - Transaction ID is synthetic (not from the original update operation).
            //   BroadcastQueue creates its own TX per target, so this ID cannot be
            //   correlated with downstream BroadcastReceived/BroadcastApplied events.
            // - `broadcasted_to` = number of targets enqueued, not actual delivery
            //   count. Actual sends are async via BroadcastQueue.
            // - `broadcast_to` is empty to avoid cloning up to 512 PeerKeyLocations
            //   purely for telemetry. The peer list can be reconstructed from
            //   BroadcastReceived events on the receiving end.
            let update_tx =
                crate::message::Transaction::new::<crate::operations::update::UpdateMsg>();
            let enqueued_count = target_result.targets.len();
            if let Some(log) = NetEventLog::update_broadcast_emitted(
                &update_tx,
                &op_manager.ring,
                key,
                Vec::new(),
                enqueued_count,
                new_state,
            ) {
                self.bridge
                    .log_register
                    .register_events(Either::Left(log))
                    .await;
            }
        }
        #[cfg(feature = "simulation_tests")]
        {
            Self::broadcast_state_to_peers(
                self.bridge.clone(),
                op_manager,
                key,
                new_state,
                target_result,
            )
            .await;
        }
    }

    /// Send state to a specific peer that reported a stale summary.
    ///
    /// Unlike `handle_broadcast_state_change` which fans out to ALL subscribers,
    /// this targets only the peer that needs catching up. Used by the interest
    /// sync summary-mismatch handler to avoid O(peers^2) broadcast storms.
    pub(super) async fn handle_sync_state_to_peer(
        &mut self,
        op_manager: &Arc<OpManager>,
        key: freenet_stdlib::prelude::ContractKey,
        new_state: freenet_stdlib::prelude::WrappedState,
        target_addr: std::net::SocketAddr,
    ) {
        let target = op_manager
            .ring
            .connection_manager
            .get_peer_by_addr(target_addr);
        let Some(target) = target else {
            tracing::debug!(
                contract = %key,
                peer = %target_addr,
                "SyncStateToPeer: peer not found in connection manager, skipping"
            );
            return;
        };

        tracing::debug!(
            contract = %key,
            peer = %target_addr,
            "SyncStateToPeer: sending state to stale peer"
        );

        #[cfg(not(feature = "simulation_tests"))]
        {
            self.broadcast_queue.enqueue(key, target, new_state).await;
        }
        #[cfg(feature = "simulation_tests")]
        {
            super::super::broadcast_queue::broadcast_to_single_peer(
                &self.bridge,
                op_manager,
                key,
                new_state,
                target,
            )
            .await;
        }
    }

    /// Send state change broadcasts to interested peers.
    ///
    /// Used only in simulation tests for inline (deterministic) broadcast ordering.
    /// In production, the `BroadcastQueue` handles per-target sends with bounded
    /// concurrency.
    #[cfg(feature = "simulation_tests")]
    async fn broadcast_state_to_peers(
        bridge: P2pBridge,
        op_manager: &Arc<OpManager>,
        key: freenet_stdlib::prelude::ContractKey,
        new_state: freenet_stdlib::prelude::WrappedState,
        target_result: crate::operations::update::BroadcastTargetResult,
    ) {
        // Skip the whole fan-out when we hold no local state for `key`: there is
        // nothing to broadcast, and the per-target `get_contract_summary` calls
        // below are the residual #4473 summarize storm on this path. Mirrors the
        // production `broadcast_to_single_peer` gate. See
        // `broadcast_queue::should_broadcast_contract`.
        if !super::super::broadcast_queue::should_broadcast_contract(op_manager, &key) {
            tracing::trace!(
                contract = %key,
                "Skipping broadcast fan-out - contract not hosted or in use"
            );
            return;
        }

        // Get our summary once for all targets
        let our_summary = op_manager
            .interest_manager
            .get_contract_summary(op_manager, &key)
            .await;

        let update_tx = crate::message::Transaction::new::<crate::operations::update::UpdateMsg>();

        tracing::debug!(
            tx = %update_tx,
            contract = %key,
            target_count = target_result.targets.len(),
            "Broadcasting state change to network peers"
        );

        let mut skipped_summary_match: usize = 0;
        let mut send_success: usize = 0;
        let mut send_failed: usize = 0;

        // Per-fan-out probe budget shared across all targets, mirroring the
        // production `broadcast_to_single_peer` semantic skip and the
        // `Summaries` handler's per-message MAX_STALENESS_PROBES_PER_SUMMARIES
        // cap (see `broadcast_queue::fanout_send_needed`).
        let mut staleness_probes_used = 0usize;

        for target in &target_result.targets {
            let Some(peer_addr) = target.socket_addr() else {
                continue;
            };

            let peer_key = PeerKey::from(target.pub_key().clone());

            // Get peer's cached summary
            let their_summary = op_manager
                .interest_manager
                .get_peer_summary(&key, &peer_key);

            // Semantic skip (#4894's fan-out counterpart): byte-equal
            // summaries skip as before; byte-differing summaries consult the
            // shared delta cache / a bounded contract probe so a
            // converged-but-nondeterministic-summary pair does not re-flood
            // full state. Mirrors production `broadcast_to_single_peer`.
            if let (Some(ours), Some(theirs)) = (&our_summary, &their_summary) {
                if !super::super::broadcast_queue::fanout_send_needed(
                    op_manager,
                    &key,
                    super::super::broadcast_queue::SummaryPair { ours, theirs },
                    &mut staleness_probes_used,
                )
                .await
                {
                    tracing::trace!(
                        contract = %key,
                        peer = %peer_addr,
                        "Skipping broadcast - peer already has our state \
                         (byte-equal or logically converged summaries)"
                    );
                    skipped_summary_match += 1;
                    continue;
                }
            }

            // Compute delta if we have their summary (uses memoization cache)
            // Track whether we successfully computed a delta vs sent full state
            let (payload, sent_delta) = match (&our_summary, &their_summary) {
                (Some(ours), Some(theirs)) => {
                    match op_manager
                        .interest_manager
                        .compute_delta(op_manager, &key, theirs, ours, new_state.size())
                        .await
                    {
                        Ok(Some(delta)) => (
                            crate::message::DeltaOrFullState::Delta(delta.as_ref().to_vec()),
                            true,
                        ),
                        Ok(None) => {
                            // Empty delta = the peer is logically converged
                            // despite byte-differing summaries. The pre-fix
                            // arm sent FULL STATE here (the
                            // nondeterministic-summary heal storm). Mirrors
                            // production `broadcast_to_single_peer`: skip.
                            tracing::trace!(
                                contract = %key,
                                peer = %peer_addr,
                                "Skipping broadcast - contract reported empty \
                                 delta (peer converged)"
                            );
                            skipped_summary_match += 1;
                            continue;
                        }
                        Err(err) => {
                            tracing::debug!(
                                contract = %key,
                                error = %err,
                                "Delta computation failed, falling back to full state"
                            );
                            (
                                crate::message::DeltaOrFullState::FullState(
                                    new_state.as_ref().to_vec(),
                                ),
                                false,
                            )
                        }
                    }
                }
                _ => (
                    crate::message::DeltaOrFullState::FullState(new_state.as_ref().to_vec()),
                    false,
                ),
            };

            // Save payload size before moving it into the message
            let payload_size = payload.size();

            // Check if we should use streaming for full state broadcasts
            let use_streaming = matches!(&payload, crate::message::DeltaOrFullState::FullState(_))
                && crate::operations::should_use_streaming(
                    op_manager.streaming_threshold,
                    payload_size,
                );

            let send_result = if use_streaming {
                let sender_summary_bytes = our_summary
                    .as_ref()
                    .map(|s| s.as_ref().to_vec())
                    .unwrap_or_default();
                let state_bytes = match payload {
                    crate::message::DeltaOrFullState::FullState(data) => data,
                    _ => unreachable!("checked above"),
                };
                let streaming_payload = crate::operations::update::BroadcastStreamingPayload {
                    state_bytes,
                    sender_summary_bytes,
                };
                let payload_bytes = match bincode::serialize(&streaming_payload) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(
                            tx = %update_tx,
                            error = %e,
                            "Failed to serialize BroadcastStreamingPayload, skipping"
                        );
                        continue;
                    }
                };
                let sid = StreamId::next_operations();
                tracing::debug!(
                    tx = %update_tx,
                    contract = %key,
                    peer = %peer_addr,
                    stream_id = %sid,
                    payload_size,
                    "Using streaming for BroadcastTo"
                );
                let msg = crate::operations::update::UpdateMsg::BroadcastToStreaming {
                    id: update_tx,
                    stream_id: sid,
                    key,
                    total_size: payload_bytes.len() as u64,
                };
                let net_msg: NetMessage = msg.into();
                // Serialize metadata for embedding in fragment #1 (fix #2757)
                let metadata = match bincode::serialize(&net_msg) {
                    Ok(bytes) => Some(bytes::Bytes::from(bytes)),
                    Err(e) => {
                        tracing::warn!(
                            ?peer_addr,
                            error = %e,
                            "Failed to serialize BroadcastTo metadata for embedding"
                        );
                        None
                    }
                };
                // channel-safety: ok — broadcast_state_to_peers is
                // `#[cfg(feature = "simulation_tests")]` only and never compiled into
                // the production binary; the sim runner drives inline deterministic
                // broadcast ordering. Production uses the BroadcastQueue path.
                let send_res = bridge.send(peer_addr, net_msg).await;
                match send_res {
                    Ok(()) => {
                        // The metadata message went out; the streamed full state is
                        // the actual payload. The success arm below caches
                        // `our_summary` for this peer (FIX 1 of #4145), which is only
                        // correct if the STATE was sent — so this branch's result must
                        // reflect the stream send, not just the metadata send. If
                        // `send_stream` fails, a streamed full state that never landed
                        // would otherwise be marked as delivered, recreating the
                        // #2763/#4235 divergence hazard (a wrongly-cached summary makes
                        // the next delta unappliable). Propagate the stream-send error.
                        // channel-safety: ok — sim-only path (see the
                        // `#[cfg(feature = "simulation_tests")]` gate on this fn);
                        // never compiled into the production binary.
                        bridge
                            .send_stream(
                                peer_addr,
                                sid,
                                bytes::Bytes::from(payload_bytes),
                                metadata,
                            )
                            .await
                            .inspect_err(|err| {
                                tracing::warn!(
                                    tx = %update_tx,
                                    peer = %peer_addr,
                                    error = %err,
                                    "Failed to send broadcast stream data"
                                );
                            })
                    }
                    // Metadata send failed; nothing was streamed. Propagate the error.
                    Err(err) => Err(err),
                }
            } else {
                let msg = crate::operations::update::UpdateMsg::BroadcastTo {
                    id: update_tx,
                    key,
                    payload,
                    // Include our summary so peer doesn't echo back
                    sender_summary_bytes: our_summary
                        .as_ref()
                        .map(|s| s.as_ref().to_vec())
                        .unwrap_or_default(),
                };
                // channel-safety: ok — sim-only path (see the
                // `#[cfg(feature = "simulation_tests")]` gate on this fn); never
                // compiled into the production binary.
                bridge.send(peer_addr, msg.into()).await
            };

            if let Err(err) = send_result {
                send_failed += 1;
                tracing::warn!(
                    tx = %update_tx,
                    peer = %peer_addr,
                    error = %err,
                    "Failed to send state change broadcast"
                );
            } else {
                send_success += 1;
                // Track delta vs full state sends for testing (PR #2763)
                if sent_delta {
                    op_manager
                        .interest_manager
                        .record_delta_send(new_state.size(), payload_size);
                    crate::config::GlobalTestMetrics::record_delta_send();
                } else {
                    op_manager.interest_manager.record_full_state_send();
                    crate::config::GlobalTestMetrics::record_full_state_send();
                }

                // Issue #3046: Refresh the peer's interest TTL on every successful
                // broadcast send. Without this, interest entries for peers who only
                // receive full-state broadcasts (no delta → no update_peer_summary
                // call) expire after INTEREST_TTL, even though broadcasts
                // are being successfully delivered. This caused ~49% of River room
                // subscribers to miss updates.
                op_manager
                    .interest_manager
                    .refresh_peer_interest(&key, &peer_key);

                // Issue #4145: Cache the peer summary on ANY successful broadcast
                // send — delta OR full state — not just deltas. Separate from the
                // TTL refresh above: update_peer_summary sets the cached summary
                // (used for delta computation), refresh_peer_interest only extends
                // the TTL.
                //
                // PR #2763 originally gated this on `sent_delta`, which created a
                // chicken-and-egg: a delta needs the peer's cached summary, but the
                // summary was only cached after a delta — so every NEW subscriber
                // (or a peer whose summary was cleared) starts on full state and is
                // trapped sending full state forever. Under sustained fan-out that
                // is the #4233 full-state broadcast storm.
                //
                // Safe now because this runs only inside the send-success (`else`)
                // arm. This is the inline non-streaming `BroadcastTo` path: a
                // successful `bridge.send` is the terminal state we can observe
                // (the same assumption the delta path already made). Caching on a
                // delivered broadcast is safe even when the cache is momentarily
                // wrong: a wrongly-cached summary is corrected by the periodic
                // InterestSync summary exchange (~5 min, node.rs) and by the
                // delta-apply-failure → ResyncRequest path that clears the
                // sender's cached summary (node.rs ~2119). Caching here lets the
                // NEXT broadcast to this peer be a small delta. The streaming
                // (full-state) path is gated more strictly on a `Delivered`
                // completion (#4235) in `broadcast_queue.rs` — note that signal is
                // sender-side completion, not a receiver ack, so a lost stream
                // tail is covered by the same two backstops.
                // (Telemetry above still records delta-vs-full-state separately.)
                if let Some(summary) = &our_summary {
                    op_manager.interest_manager.update_peer_summary(
                        &key,
                        &peer_key,
                        Some(summary.clone()),
                    );
                }
            }
        }

        // Emit broadcast emitted telemetry (issue #3622)
        if let Some(log) = NetEventLog::update_broadcast_emitted(
            &update_tx,
            &op_manager.ring,
            key,
            target_result.targets.clone(),
            send_success,
            new_state,
        ) {
            bridge.log_register.register_events(Either::Left(log)).await;
        }

        // Emit broadcast delivery summary telemetry (issue #3046)
        if let Some(log) = NetEventLog::broadcast_delivery_summary(
            &update_tx,
            &op_manager.ring,
            key,
            &target_result,
            skipped_summary_match,
            send_success,
            send_failed,
        ) {
            bridge.log_register.register_events(Either::Left(log)).await;
        }
    }
}
