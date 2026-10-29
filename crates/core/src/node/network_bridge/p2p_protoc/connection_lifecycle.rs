//! Connection lifecycle for [`P2pConnManager`]: teardown of existing
//! connections, the handshake / new-connection setup path, and binding a
//! connection's peer identity (with `identity`).

use super::*;
use crate::transport::{ConnIdentity, TRANSPORT_METRICS};

/// The identity telemetry kinds of a ConnectPeer for an existing connection.
const CONNECT_PEER_KINDS: [ConnIdentity; 3] = [
    ConnIdentity::SetConnectPeer,
    ConnIdentity::SameConnectPeer,
    ConnIdentity::ChangedConnectPeer,
];

/// The identity telemetry kinds of a ConnectRequest that names its sender.
const CONNECT_REQUEST_KINDS: [ConnIdentity; 3] = [
    ConnIdentity::SetConnectRequest,
    ConnIdentity::SameConnectRequest,
    ConnIdentity::ChangedConnectRequest,
];

/// The identity telemetry kind for a [`BindOutcome`] on a path whose
/// `set`, `same` and `changed` kinds are given, or `None` when the outcome
/// is counted elsewhere or not at all.
fn identity_kind(
    outcome: &BindOutcome,
    [set, same, changed]: [ConnIdentity; 3],
) -> Option<ConnIdentity> {
    match outcome {
        BindOutcome::Bound => Some(set),
        BindOutcome::Same => Some(same),
        BindOutcome::Mismatch(_) => Some(changed),
        BindOutcome::Held(_) => Some(ConnIdentity::ClaimHeld),
        // Counted as `quiet_holder_replaced` when the quiet holder is replaced.
        BindOutcome::HeldByQuiet(_) => None,
        BindOutcome::NoConnection => None,
    }
}

/// The most replacements and binds one pending-claim retry pass does,
/// bounding how long it holds the event loop (as the zombie sweep bounds its
/// slices).
pub(super) const MAX_PENDING_CLAIM_WORK_PER_PASS: usize = 8;

/// When the event loop runs [`P2pConnManager::retry_pending_claims`]: every
/// `interval`, and only while a claim may be pending. The loop passes
/// [`Self::deadline`] to its timer wake-up and calls [`Self::take_due`] on
/// every pass.
#[derive(Debug)]
pub(super) struct PendingClaimRetry {
    last: Instant,
    interval: Duration,
}

impl PendingClaimRetry {
    pub(super) fn new(now: Instant, interval: Duration) -> Self {
        Self {
            last: now,
            interval,
        }
    }

    /// The timer deadline for the next retry: `None` while no claim may be
    /// pending, so an idle loop arms no timer for it.
    pub(super) fn deadline(&self, may_have_pending_claims: bool) -> Option<Instant> {
        may_have_pending_claims.then(|| self.last + self.interval)
    }

    /// Make the next retry due at `now`: a pass stopped with work left.
    pub(super) fn run_again_now(&mut self, now: Instant) {
        self.last = now.checked_sub(self.interval).unwrap_or(now);
    }

    /// Whether a retry is due at `now`. When it is, the interval restarts
    /// from `now`, so the next one is a full interval later.
    pub(super) fn take_due(&mut self, now: Instant) -> bool {
        if now.saturating_duration_since(self.last) < self.interval {
            return false;
        }
        self.last = now;
        true
    }
}

/// Whether [`P2pConnManager::promote_existing_connection`] put the
/// connection into the ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Promotion {
    /// The ring took the connection, or already held it.
    InRing,
    /// The connection is not in the ring: the ring is at its cap, or the
    /// connection has no identity to promote it under.
    Refused,
}

impl P2pConnManager {
    /// Bind `key`, from `source`, as the peer identity of the open connection
    /// at `addr` (see `identity::bind` for the rules). On
    /// [`BindOutcome::Held`] or [`BindOutcome::HeldByQuiet`], `pending`
    /// decides whether the connection is kept waiting for the key:
    /// `Some(promote)` keeps it, with whether binding it should promote the
    /// connection; `None` drops it.
    ///
    /// On a bind it counts `own_key` when `key` is this node's own key.
    pub(super) fn bind_identity(
        &mut self,
        addr: SocketAddr,
        key: &TransportPublicKey,
        source: IdentitySource,
        pending: Option<bool>,
    ) -> BindOutcome {
        let outcome = identity::bind(
            &mut self.connections,
            &mut self.addr_by_pub_key,
            addr,
            key,
            source,
            pending,
        );
        if outcome == BindOutcome::Bound
            && *key == *self.bridge.op_manager.ring.connection_manager.pub_key
        {
            TRANSPORT_METRICS.record_conn_identity(addr, ConnIdentity::OwnKey);
        }
        outcome
    }

    /// The connection at `addr`, bound to `key`, is closing: updates the
    /// key-to-address index (see `identity::release`).
    pub(super) fn unbind_identity(
        &mut self,
        addr: SocketAddr,
        key: &TransportPublicKey,
    ) -> Release {
        identity::release(&self.connections, &mut self.addr_by_pub_key, addr, key)
    }

    /// Whether an open connection other than `exclude` is bound to `key` and
    /// in the ring.
    fn key_in_ring(&self, key: &TransportPublicKey, exclude: Option<SocketAddr>) -> bool {
        let connection_manager = &self.bridge.op_manager.ring.connection_manager;
        self.connections.iter().any(|(addr, entry)| {
            Some(*addr) != exclude
                && entry.pub_key() == Some(key)
                && connection_manager.is_in_ring(*addr)
        })
    }

    /// The connection at `closing`, bound to `key`, has closed: forget the
    /// peer's subscriptions and cached state unless another connection bound
    /// to `key` keeps it in the ring. Every disconnect path decides this here,
    /// from ring membership, whichever connection the index pointed at.
    pub(super) fn forget_peer_state_unless_in_ring(
        &self,
        key: &TransportPublicKey,
        closing: SocketAddr,
    ) {
        if !self.key_in_ring(key, Some(closing)) {
            self.bridge.op_manager.on_ring_connection_lost(key);
        }
    }

    /// The connection at `addr`, bound to `key`, has been removed from
    /// `connections`. Updates the index; binds the newest pending claim of
    /// `key`, if no other connection holds the key now, promoting its
    /// connection when the claim would have; then (only with
    /// `forget_peer_state`) forgets the peer's state unless a connection
    /// bound to `key` is still in the ring.
    pub(super) async fn release_identity(
        &mut self,
        addr: SocketAddr,
        key: &TransportPublicKey,
        forget_peer_state: bool,
    ) {
        debug_assert!(!self.connections.contains_key(&addr));
        self.unbind_identity(addr, key);
        if let Some((claimant, claim)) =
            identity::pending_claimant(&self.connections, Some(addr), key)
        {
            self.bind_pending_claim(claimant, &claim).await;
        }
        if forget_peer_state {
            self.forget_peer_state_unless_in_ring(key, addr);
        }
    }

    /// Bind the pending `claim` on the connection at `claimant`, if it can
    /// now be bound, and promote the connection when the claim does.
    async fn bind_pending_claim(&mut self, claimant: SocketAddr, claim: &identity::PendingClaim) {
        let (key, promote) = (&claim.key, claim.promote);
        let outcome = self.bind_identity(claimant, key, IdentitySource::Claimed, Some(promote));
        if outcome != BindOutcome::Bound {
            return;
        }
        TRANSPORT_METRICS.record_conn_identity(claimant, ConnIdentity::PendingClaimBound);
        let promotion = if promote {
            Some(self.promote_existing_connection(claimant, key, None).await)
        } else {
            None
        };
        tracing::info!(
            %claimant,
            pub_key = %key,
            ?promotion,
            "Bound a pending identity claim"
        );
    }

    /// Close the connection at `quiet`, bound to `key` but not heard from
    /// within `identity::HOLD_WINDOW`, so that a claim of `key` can be bound:
    /// its release binds the newest pending claim (see
    /// [`Self::release_identity`]). When the quiet connection is in the ring,
    /// that claim takes its place there: it is promoted, and the peer's state
    /// is kept.
    ///
    /// When this node sits exactly at its readiness threshold, the close can
    /// broadcast "not ready" before the claim's promotion broadcasts "ready"
    /// again: a short flap, the same as for any connection closed and
    /// replaced at that threshold.
    async fn replace_quiet_holder(
        &mut self,
        quiet: SocketAddr,
        key: &TransportPublicKey,
        handshake_commands: &HandshakeCommandSender,
    ) {
        TRANSPORT_METRICS.record_conn_identity(quiet, ConnIdentity::QuietHolderReplaced);
        if self
            .bridge
            .op_manager
            .ring
            .connection_manager
            .is_in_ring(quiet)
        {
            identity::promote_newest_claim(&mut self.connections, key);
        }
        tracing::info!(
            %quiet,
            pub_key = %key,
            "Closing a connection that has gone quiet; a newer connection claims its key"
        );
        self.drop_connection_by_addr(
            quiet,
            handshake_commands,
            crate::tracing::DisconnectReason::Explicit("replaced_by_same_key".into()),
        )
        .await;
    }

    /// For each key with a pending claim: when the connection holding the
    /// key has gone quiet, replace it, and its release binds the newest claim
    /// of the key; when no connection holds the key, bind the newest claim.
    /// Claims held by a live connection keep waiting; one that has waited
    /// `identity::LONG_CLAIM_WAIT` is counted once (`claim_held_long`, see
    /// the known limit in `identity`). Run from the event
    /// loop's timer (see [`PendingClaimRetry`]), so a peer that reconnected
    /// from another address is linked within about `identity::HOLD_WINDOW` of
    /// its old session going quiet.
    ///
    /// Each replacement or bind awaits pruning, promotion and sends on the
    /// event loop, so one pass does at most
    /// [`MAX_PENDING_CLAIM_WORK_PER_PASS`] of them. Returns whether it
    /// stopped with work left, in which case the loop runs the next pass as
    /// soon as no event is waiting.
    pub(super) async fn retry_pending_claims(
        &mut self,
        handshake_commands: &HandshakeCommandSender,
    ) -> bool {
        let mut work = 0;
        for key in identity::pending_keys(&self.connections) {
            if work == MAX_PENDING_CLAIM_WORK_PER_PASS {
                return true;
            }
            match identity::key_holder(&self.connections, None, &key) {
                Some((_, true)) => {
                    for waiting in
                        identity::mark_long_waits(&mut self.connections, &key, Instant::now())
                    {
                        TRANSPORT_METRICS
                            .record_conn_identity(waiting, ConnIdentity::ClaimHeldLong);
                    }
                }
                Some((quiet, false)) => {
                    work += 1;
                    self.replace_quiet_holder(quiet, &key, handshake_commands)
                        .await
                }
                None => {
                    if let Some((claimant, claim)) =
                        identity::pending_claimant(&self.connections, None, &key)
                    {
                        work += 1;
                        self.bind_pending_claim(claimant, &claim).await;
                    }
                }
            }
        }
        identity::clear_pending_hint(&self.connections, &mut self.addr_by_pub_key);
        false
    }

    /// Count disconnect cleanup of the connection `entry` at `addr`, when the
    /// cleanup is not keyed by a key recorded for
    /// this connection: either it has none (`teardown_unidentified`) or
    /// `addr_by_pub_key` maps its key to another connection
    /// (`teardown_key_elsewhere`) or to none (`teardown_key_unmapped`). Call
    /// before `addr_by_pub_key` is changed.
    fn record_identity_teardown(&self, addr: SocketAddr, entry: &ConnectionEntry) {
        match entry.pub_key() {
            None => {
                TRANSPORT_METRICS.record_conn_identity(addr, ConnIdentity::TeardownUnidentified)
            }
            Some(key) => {
                match self.addr_by_pub_key.get(key) {
                    Some(mapped) if *mapped == addr => {}
                    Some(_) => TRANSPORT_METRICS
                        .record_conn_identity(addr, ConnIdentity::TeardownKeyElsewhere),
                    None => TRANSPORT_METRICS
                        .record_conn_identity(addr, ConnIdentity::TeardownKeyUnmapped),
                }
            }
        }
    }

    /// The peer that disconnect cleanup of the connection at `addr` passes to
    /// `Ring::prune_connection`: the key recorded for the connection, or,
    /// for a connection that never named one, this node's own key. That
    /// fallback reaches nothing keyed by peer: `prune_connection` prunes by
    /// address and uses the key only in its log line and in the disconnect
    /// event it emits for a ring connection, and every ring connection has a
    /// recorded key. Callers must not pass the fallback key to anything else
    /// (`on_ring_connection_lost` runs only for a recorded key).
    pub(super) fn teardown_peer_id(
        &self,
        addr: SocketAddr,
        recorded: Option<&TransportPublicKey>,
    ) -> PeerId {
        let key = recorded
            .cloned()
            .unwrap_or_else(|| (*self.bridge.op_manager.ring.connection_manager.pub_key).clone());
        PeerId::new(key, addr)
    }

    /// `NodeEvent::ExpectPeerConnection`: a connect op expects a connection
    /// from `addr` and is dialling it under `peer_key`. Registers the address
    /// with the transport and, with the key, with the handshake driver, so a
    /// matched inbound connection is bound to the key the connect op dialled
    /// and promoted under it. The match is by address only, so that binding is
    /// a claim: it is held back while another connection holds the key, and
    /// the connection is closed if the peer later names another key.
    pub(super) fn handle_expect_peer_connection(
        addr: SocketAddr,
        peer_key: TransportPublicKey,
        state: &EventListenerState,
        handshake_cmd_sender: &HandshakeCommandSender,
    ) {
        tracing::debug!(
            peer_addr = %addr,
            direction = "inbound",
            phase = "connect",
            "Expecting peer connection - registering with handshake handler"
        );
        state.expected_inbound.expect_incoming(addr);
        // Non-blocking (#4145): this runs on the event-loop task, so a
        // `.send().await` that back-pressures on the handshake command channel
        // (cap 128) would stall the loop. Dropping the ExpectInbound
        // registration is recoverable: when the inbound connection actually
        // arrives without a matching expectation it is still accepted
        // provisionally (see handle_handshake_action, "accepting
        // provisionally"), and the originating connect op retries if its
        // handshake does not complete. We `warn!` because a dropped
        // expectation makes admission less reliable under load.
        if !handshake_cmd_sender.try_send(HandshakeCommand::ExpectInbound {
            addr,
            peer_key: Some(peer_key),
            transaction: None,
            transient: false,
        }) {
            tracing::warn!(
                peer_addr = %addr,
                phase = "connect",
                "Handshake command channel full/closed; dropped \
                 ExpectInbound registration. Inbound connection will \
                 be accepted provisionally; connect op retries if the \
                 handshake does not complete."
            );
        }
    }

    /// Drop a single connection by socket address: notifies the handshake handler,
    /// prunes topology, handles orphaned transactions, cleans up subscriptions,
    /// and forwards a DropConnection event to the per-peer listener.
    ///
    /// Returns `true` if the connection existed and was dropped.
    pub(super) async fn drop_connection_by_addr(
        &mut self,
        peer_addr: SocketAddr,
        handshake_cmd_sender: &HandshakeCommandSender,
        reason: crate::tracing::DisconnectReason,
    ) -> bool {
        let Some(entry) = self.connections.get(&peer_addr) else {
            return false;
        };

        let pub_key_to_remove = entry.pub_key().cloned();
        self.record_identity_teardown(peer_addr, entry);

        tracing::trace!(
            peer_addr = %peer_addr,
            conn_map_size = self.connections.len(),
            reason = "drop_requested",
            "Removing connection from tracking map"
        );

        // Non-blocking (#4145): drop_connection_by_addr runs on the event-loop
        // task, so a `.send().await` that back-pressures on the handshake
        // command channel (cap 128) would stall the loop. The DropConnection is
        // redundant teardown — we remove the connection from ctx.connections /
        // addr_by_pub_key and prune the ring below regardless. A missed
        // DropConnection leaves at most one stale per-SocketAddr
        // InboundMatcher entry for the address, which the driver sweeps once
        // it outlives EXPECTED_INBOUND_WINDOW and whose key it does not honour
        // after that.
        if !handshake_cmd_sender.try_send(HandshakeCommand::DropConnection { addr: peer_addr }) {
            tracing::warn!(
                peer_addr = %peer_addr,
                phase = "disconnect",
                "Handshake command channel full/closed; skipping redundant drop notification"
            );
        }

        // Immediately prune topology counters so we don't leak open connection slots.
        let prune_result = self
            .bridge
            .op_manager
            .ring
            .prune_connection(
                self.teardown_peer_id(peer_addr, pub_key_to_remove.as_ref()),
                reason.clone(),
            )
            .await;

        // Handle orphaned transactions immediately (retry via alternate routes).
        self.bridge
            .handle_orphaned_transactions(prune_result.orphaned_transactions, peer_addr)
            .await;

        // Broadcast unready if we dropped below the readiness threshold
        if prune_result.became_unready {
            self.handle_broadcast_ready_state(false).await;
        }

        // Forget this peer's subscriptions and cached contract state, only
        // when no other open connection is bound to its key. A connection
        // that never named a peer has none.
        let removed = self.connections.remove(&peer_addr);
        if let Some(pub_key) = &pub_key_to_remove {
            self.release_identity(peer_addr, pub_key, true).await;
        }
        if let Some(conn) = removed {
            notify_connection_dropped(conn.sender, peer_addr, reason);
        }

        true
    }

    /// Drop a zombie transport connection using non-blocking sends to the handshake
    /// driver. This prevents a deadlock where the event loop blocks sending
    /// DropConnection commands while the handshake driver blocks sending
    /// InboundConnection events back to the event loop (#3519).
    ///
    /// Unlike `drop_connection_by_addr`, this method:
    /// - Uses `try_send` for the handshake command (skips if channel full)
    /// - Uses a shorter timeout for the per-connection drop notification
    pub(super) async fn drop_zombie_connection(
        &mut self,
        peer_addr: SocketAddr,
        handshake_cmd_sender: &HandshakeCommandSender,
    ) {
        let Some(entry) = self.connections.get(&peer_addr) else {
            return;
        };

        let pub_key_to_remove = entry.pub_key().cloned();
        self.record_identity_teardown(peer_addr, entry);

        // Non-blocking send to handshake driver to avoid deadlock (#3519).
        // For zombie transports (already established connections), the expected_inbound
        // entry was already consumed when the inbound connection arrived, so the
        // DropConnection command is effectively a no-op.
        if !handshake_cmd_sender.try_send(HandshakeCommand::DropConnection { addr: peer_addr }) {
            tracing::debug!(
                peer_addr = %peer_addr,
                "Handshake channel full during zombie cleanup, skipping handshake notification"
            );
        }

        // Prune from ring topology (non-blocking — only takes write locks, no cross-task channels)
        let prune_result = self
            .bridge
            .op_manager
            .ring
            .prune_connection(
                self.teardown_peer_id(peer_addr, pub_key_to_remove.as_ref()),
                crate::tracing::DisconnectReason::Explicit("unpromoted_transport".into()),
            )
            .await;

        // Handle orphaned transactions
        self.bridge
            .handle_orphaned_transactions(prune_result.orphaned_transactions, peer_addr)
            .await;

        if prune_result.became_unready {
            self.handle_broadcast_ready_state(false).await;
        }

        // Remove from transport tracking and notify the per-connection task.
        // The peer's subscriptions and cached contract state are forgotten
        // only when no other open connection is bound to its key (a
        // connection that never named a peer has none).
        let removed = self.connections.remove(&peer_addr);
        if let Some(pub_key) = &pub_key_to_remove {
            self.release_identity(peer_addr, pub_key, true).await;
        }
        if let Some(conn) = removed {
            // Short timeout — zombie connections may already be dead
            match timeout(
                Duration::from_millis(100),
                conn.sender
                    .send(Right(ConnEvent::NodeAction(NodeEvent::DropConnection(
                        peer_addr,
                        crate::tracing::DisconnectReason::Explicit("unpromoted_transport".into()),
                    )))),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => {
                    // Expected for zombie connections — the peer task may already be gone
                }
            }
        }
    }

    /// Promote the open connection at `peer_addr` into the ring under `key`,
    /// unless the ring already holds it. Used when a ConnectPeer names an
    /// existing connection (which is bound to `key` after a promotion) and
    /// when a pending identity claim of `key` is bound. `tx` is the connect
    /// transaction, for logs.
    pub(super) async fn promote_existing_connection(
        &mut self,
        peer_addr: SocketAddr,
        key: &TransportPublicKey,
        tx: Option<Transaction>,
    ) -> Promotion {
        if !self.connections.contains_key(&peer_addr) {
            return Promotion::Refused;
        }
        let key = key.clone();
        let connection_manager = &self.bridge.op_manager.ring.connection_manager;
        let was_transient = connection_manager.drop_transient(peer_addr);

        // Promote if: (a) was tracked as transient, OR (b) not already in ring.
        // Case (b) handles expired transient TTL: transport preserved per 76058cf4
        // but tracking entry gone. Without this, CONNECT succeeds but peer is never
        // added to ring topology (#3113).
        let needs_promotion = was_transient.is_some() || !connection_manager.is_in_ring(peer_addr);
        let transient_expired = was_transient.is_none() && needs_promotion;

        if needs_promotion {
            let loc = was_transient
                .and_then(|e| e.location)
                .unwrap_or_else(|| Location::from_address(&peer_addr));
            // Don't re-run should_accept() here: the connection was already
            // accepted at the protocol level (the CONNECT terminus handler in connect.rs) and NAT traversal
            // already succeeded. Re-evaluating the probabilistic Kleinberg filter
            // would discard hard-won connections for no capacity reason (#3545).
            // Enforce the hard max_connections cap as a resource safety limit,
            // EXCEPT for a strictly-closer per-side nearest-neighbor lattice
            // edge within the bounded over-max ceiling (mechanism 3). Without
            // this exception the cap silently drops the guaranteed
            // successor/predecessor edge on the REAL CONNECT/promotion path,
            // making mechanism 3's at-capacity displacement inert in
            // production (nodes routinely sit at max). The same predicate is
            // re-checked in add_connection; the over-max topology prune sheds a
            // farther non-lattice link next tick. See
            // connection_manager.rs::{admits_lattice_edge_over_cap, LATTICE_OVERMAX_SLACK}.
            let current = connection_manager.connection_count();
            if connection_manager.refuses_connection_at_cap(loc) {
                tracing::warn!(
                    tx = ?tx,
                    %peer_addr,
                    current_connections = current,
                    max_connections = connection_manager.max_connections,
                    %loc,
                    transient_expired,
                    "connect_peer: rejecting transient promotion to enforce cap"
                );
                return Promotion::Refused;
            }
            // The key promoted is the one the caller passed: for a
            // ConnectPeer, the connection's own or the one it is bound to
            // after this promotion (a ConnectPeer naming another one is
            // refused before this); for a pending claim, the key just bound.
            // #4787 instrumentation: count the promotion only if the ring
            // actually took the connection. `add_connection`'s own `bool`
            // reports the readiness-threshold crossing, NOT acceptance, so
            // the outcome struct is what this has to branch on; otherwise
            // a cap-rejected promotion inflates `promoted_to_ring` in
            // exactly the saturation direction the counter is meant to
            // expose.
            let outcome = self
                .bridge
                .op_manager
                .ring
                .add_connection_reporting(loc, PeerId::new(key.clone(), peer_addr), true)
                .await;
            // Counted for a ConnectPeer only: a pending claim bound later
            // (`tx` is `None`) is a replacement, not a bootstrap promotion.
            if outcome.added && tx.is_some() {
                crate::node::network_status::record_bootstrap_promoted_to_ring();
            }
            let just_became_ready = outcome.just_became_ready;
            tracing::info!(
                tx = ?tx,
                remote = %peer_addr,
                transient_expired,
                added = outcome.added,
                "connect_peer: promoted to ring"
            );
            if !outcome.added
                && !self
                    .bridge
                    .op_manager
                    .ring
                    .connection_manager
                    .is_in_ring(peer_addr)
            {
                return Promotion::Refused;
            }

            // Update the dashboard/network_status with the peer's location.
            // Without this, peers promoted via this path show no location on the
            // dashboard because the initial record_peer_connected(addr, None, None)
            // from handle_successful_connection is never updated.
            let pkl = crate::ring::PeerKeyLocation::new(key.clone(), peer_addr);
            crate::node::network_status::record_peer_connected(
                peer_addr,
                Some(loc.as_f64()),
                Some(pkl),
            );

            // tell the promoted peer about our subscriptions and cache.
            // Non-blocking (#4145): handle_connect_peer runs INLINE on the
            // event-loop task, and bridge.send() awaits the cap-512
            // ev_listener_tx that this same task drains, a self-stall under
            // the connection-churn the SubscribeHint nudge drives. Use
            // bridge.try_send: a dropped interest/cache message is recovered
            // by the periodic InterestSync exchange + subscription-renewal
            // background task, which re-advertise our interests to this peer.
            for (target, msg) in self
                .bridge
                .op_manager
                .on_ring_connection_established(peer_addr, &key)
            {
                if let Err(e) = self.bridge.try_send(target, msg) {
                    tracing::warn!(
                        %peer_addr,
                        error = %e,
                        "Failed to send interest/cache data on transient promotion \
                         (will re-sync via periodic InterestSync/renewal)"
                    );
                }
            }

            // Directed-subscribe placement (#4404): the new neighbor may be
            // the closest non-hosting peer for some contracts we host.
            self.consider_migration_for_new_peer(peer_addr);

            // Broadcast readiness if we just crossed the threshold
            if just_became_ready {
                self.handle_broadcast_ready_state(true).await;
            }
        }

        Promotion::InRing
    }

    // `origin` is telemetry carried alongside the dial; bundling it with the
    // other arguments would only move them into a one-use struct.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_connect_peer(
        &mut self,
        peer: PeerKeyLocation,
        mut callback: Box<dyn ConnectResultSender>,
        tx: Transaction,
        handshake_commands: &HandshakeCommandSender,
        state: &mut EventListenerState,
        transient: bool,
        origin: crate::transport::NatAttemptOrigin,
    ) -> anyhow::Result<()> {
        // Periodic cleanup of expired backoff entries (every 60 seconds)
        const BACKOFF_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
        if state.last_backoff_cleanup.elapsed() > BACKOFF_CLEANUP_INTERVAL {
            state.peer_backoff.cleanup_expired();
            state.last_backoff_cleanup = Instant::now();
        }

        let mut peer = peer;
        let mut peer_addr = peer
            .socket_addr()
            .unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0));

        // IMPORTANT: Always try pub_key lookup first, not just for unspecified addresses.
        // The peer's advertised address (from PeerKeyLocation) may differ from the actual
        // TCP connection's remote address stored in self.connections. For example:
        // - PeerKeyLocation may have the peer's listening port (e.g., 37791)
        // - self.connections is keyed by the actual TCP source port (ephemeral port)
        // By looking up via pub_key (populated when we receive the first message from this
        // peer), we can find the correct connection entry regardless of address mismatch.
        if let Some((existing_addr, _)) = self.connection_entry_by_pub_key(peer.pub_key()) {
            if existing_addr != peer_addr {
                tracing::debug!(
                    tx = %tx,
                    peer = %peer,
                    advertised_addr = %peer_addr,
                    actual_addr = %existing_addr,
                    transient,
                    phase = "connect",
                    "Using existing connection (advertised address differs from actual)"
                );
            }
            peer_addr = existing_addr;
            peer = PeerKeyLocation::new(peer.pub_key().clone(), existing_addr);
        } else if peer_addr.ip().is_unspecified() {
            tracing::debug!(
                tx = %tx,
                transient,
                "ConnectPeer received unspecified address without existing connection reference"
            );
        }

        tracing::debug!(
            tx = %tx,
            peer = %peer,
            peer_addr = %peer_addr,
            transient,
            phase = "connect",
            "Initiating connection to peer"
        );
        if let Some(blocked_addrs) = &self.blocked_addresses {
            if blocked_addrs.contains(&peer_addr) {
                tracing::info!(
                    tx = %tx,
                    remote = %peer_addr,
                    "Outgoing connection to peer blocked by local policy"
                );
                callback
                    .send_result(Err(()))
                    .await
                    .inspect_err(|error| {
                        tracing::debug!(
                            remote = %peer_addr,
                            ?error,
                            "Failed to notify caller about blocked connection"
                        );
                    })
                    .ok();
                return Ok(());
            }
            tracing::debug!(
                tx = %tx,
                "Blocked addresses: {:?}, peer addr: {}",
                blocked_addrs,
                peer_addr
            );
        }

        // If a transient transport already exists, promote it without dialing anew.
        if self.connections.contains_key(&peer_addr) {
            tracing::info!(
                tx = %tx,
                remote = %peer,
                transient,
                "connect_peer: reusing existing transport / promoting transient if present"
            );
            // The connection keeps the identity it has, and is promoted only
            // under it: a ConnectPeer naming another key is refused before
            // anything is promoted. A connection without one is bound to the
            // named key once the ring has taken it, so a ConnectPeer refused
            // at the cap leaves it as it was.
            let check = identity::check(&self.connections, peer_addr, peer.pub_key());
            // Not reachable: a key held by another connection resolves to that
            // connection through the lookup by key above, so `peer_addr` is
            // the holder. If it were reached, nothing is kept pending, since
            // the connect op is told it failed.
            debug_assert!(
                !matches!(check, BindOutcome::Held(_) | BindOutcome::HeldByQuiet(_)),
                "a ConnectPeer for a held key resolves to the holder"
            );
            let refused = match &check {
                BindOutcome::Bound | BindOutcome::Same => {
                    if self
                        .promote_existing_connection(peer_addr, peer.pub_key(), Some(tx))
                        .await
                        == Promotion::InRing
                    {
                        let outcome = self.bind_identity(
                            peer_addr,
                            peer.pub_key(),
                            IdentitySource::Claimed,
                            None,
                        );
                        if let Some(kind) = identity_kind(&outcome, CONNECT_PEER_KINDS) {
                            TRANSPORT_METRICS.record_conn_identity(peer_addr, kind);
                        }
                        false
                    } else {
                        true
                    }
                }
                BindOutcome::Mismatch(_)
                | BindOutcome::Held(_)
                | BindOutcome::HeldByQuiet(_)
                | BindOutcome::NoConnection => {
                    if let Some(kind) = identity_kind(&check, CONNECT_PEER_KINDS) {
                        TRANSPORT_METRICS.record_conn_identity(peer_addr, kind);
                    }
                    tracing::warn!(
                        tx = %tx,
                        %peer_addr,
                        named = %peer.pub_key(),
                        outcome = ?check,
                        "connect_peer: existing connection has another identity; refusing"
                    );
                    true
                }
            };
            if refused {
                callback
                    .send_result(Err(()))
                    .await
                    .inspect_err(|err| {
                        tracing::debug!(
                            tx = %tx,
                            remote = %peer,
                            ?err,
                            "connect_peer: failed to notify refusal callback"
                        );
                    })
                    .ok();
                return Ok(());
            }

            // Return the remote peer's address we are connected to.
            let resolved_addr = peer
                .socket_addr()
                .expect("connected peer should have socket address");
            callback
                .send_result(Ok((resolved_addr, None)))
                .await
                .inspect_err(|err| {
                    tracing::debug!(
                        tx = %tx,
                        remote = %peer,
                        ?err,
                        "connect_peer: failed to notify existing-connection callback"
                    );
                })
                .ok();
            return Ok(());
        }

        // Check if this peer is in backoff due to previous connection failures.
        // This check is done AFTER the existing connection check above, so that:
        // 1. If we already have a connection (possibly from inbound), we reuse it
        // 2. Backoff only blocks NEW outbound connection attempts
        // NOTE: Backoff runs before the max_connections pre-flight check
        // to avoid unnecessary work when the peer is in backoff.
        if !peer_addr.ip().is_unspecified() && state.peer_backoff.is_in_backoff(peer_addr) {
            let remaining = state
                .peer_backoff
                .remaining_backoff(peer_addr)
                .unwrap_or(Duration::ZERO);
            tracing::debug!(
                tx = %tx,
                peer = %peer,
                peer_addr = %peer_addr,
                remaining_secs = remaining.as_secs(),
                transient,
                phase = "connect",
                "Skipping connection attempt - peer in backoff"
            );
            callback
                .send_result(Err(()))
                .await
                .inspect_err(|error| {
                    tracing::debug!(
                        remote = %peer_addr,
                        ?error,
                        "Failed to notify caller about backoff-delayed connection"
                    );
                })
                .ok();
            return Ok(());
        }

        // Pre-flight max_connections check: avoid expensive NAT hole-punching when
        // we're already at capacity. We don't re-run should_accept() here because
        // the connection was already accepted at the protocol level (the CONNECT terminus handler in connect.rs)
        // and re-rolling the probabilistic Kleinberg filter would discard connections
        // after costly NAT traversal for no capacity reason (#3545).
        // The cap has the SAME nearest-neighbor lattice exemption
        // (`admits_lattice_edge_over_cap`) as should_accept, add_connection and
        // both promotion gates; this check and the promotion gates share
        // `refuses_connection_at_cap`. Without it a peer at max refused its own
        // lattice probe's acceptor here, before NAT traversal, so tightening at
        // capacity never succeeded from our side (#5827).
        if !transient && !peer_addr.ip().is_unspecified() {
            let connection_manager = &self.bridge.op_manager.ring.connection_manager;
            let current = connection_manager.connection_count();
            let loc = Location::from_address(&peer_addr);
            if connection_manager.refuses_connection_at_cap(loc) {
                tracing::info!(
                    tx = %tx,
                    remote = %peer_addr,
                    current_connections = current,
                    max_connections = connection_manager.max_connections,
                    "connect_peer: skipping connection — at max_connections"
                );
                callback
                    .send_result(Err(()))
                    .await
                    .inspect_err(|err| {
                        tracing::debug!(
                            tx = %tx,
                            remote = %peer_addr,
                            ?err,
                            "connect_peer: failed to notify max-connections-rejection callback"
                        );
                    })
                    .ok();
                return Ok(());
            }
        }

        match state.awaiting_connection.entry(peer_addr) {
            std::collections::hash_map::Entry::Occupied(mut callbacks) => {
                let txs_entry = state.awaiting_connection_txs.entry(peer_addr).or_default();
                if !txs_entry.contains(&tx) {
                    txs_entry.push(tx);
                }
                tracing::debug!(
                    tx = %tx,
                    remote = %peer_addr,
                    pending = callbacks.get().len(),
                    transient,
                    "Connection already pending, queuing additional requester"
                );
                callbacks.get_mut().push(callback);
                tracing::info!(
                    tx = %tx,
                    remote = %peer_addr,
                    pending = callbacks.get().len(),
                    pending_txs = ?txs_entry,
                    transient,
                    "connect_peer: connection already pending, queued callback"
                );
                return Ok(());
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let txs_entry = state.awaiting_connection_txs.entry(peer_addr).or_default();
                txs_entry.push(tx);
                tracing::debug!(
                        tx = %tx,
                    remote = %peer_addr,
                    transient,
                    "connect_peer: registering new pending connection"
                );
                entry.insert(vec![callback]);
                tracing::info!(
                tx = %tx,
                remote = %peer_addr,
                pending = 1,
                    pending_txs = ?txs_entry,
                    transient,
                    "connect_peer: registered new pending connection"
                );
                state.expected_inbound.expect_incoming(peer_addr);
            }
        }

        // Non-blocking (#4145): connect_peer runs on the event-loop task (it is
        // driven by the ConnectPeer event handler), so a `.send().await` that
        // back-pressures on the handshake command channel (cap 128) would stall
        // the loop. On Full/Closed we run the SAME cleanup the blocking error
        // path used: prune the in-transit connection slot and fail every
        // awaiting callback with Err(()), so callers (including the SubscribeHint
        // resend-waiter registered above) are notified immediately instead of
        // parking. The connect op can retry on a later attempt; this failure is
        // LOCAL channel contention, not a remote connection failure, so we do
        // NOT record peer backoff.
        let origin = dial_origin(origin, transient, peer_addr, &self.gateways);
        if !handshake_commands.try_send(HandshakeCommand::Connect {
            peer: peer.clone(),
            transaction: tx,
            transient,
            origin,
        }) {
            tracing::warn!(
                tx = %tx,
                remote = %peer_addr,
                transient,
                "Handshake command channel full/closed; failed to enqueue connect \
                 command. Failing awaiting callbacks (op will retry)."
            );
            // Prune the phantom in-transit reservation (connection-manager side
            // effect; needs the live ring). We intentionally do NOT record peer
            // backoff: this is LOCAL channel contention, not a remote connection
            // failure — backoff would wrongly delay a legitimate retry.
            self.bridge
                .op_manager
                .ring
                .connection_manager
                .prune_in_transit_connection(peer_addr);
            // Fail/clear the awaiting-connection state (callbacks + tx maps).
            fail_awaiting_connection(state, peer_addr, tx, transient).await;
        } else {
            tracing::debug!(
                tx = %tx,
                remote = %peer_addr,
                transient,
                "connect_peer: handshake command dispatched"
            );
        }

        Ok(())
    }

    pub(super) async fn handle_handshake_action(
        &mut self,
        event: HandshakeEvent,
        state: &mut EventListenerState,
        handshake_commands: &HandshakeCommandSender,
    ) -> anyhow::Result<()> {
        tracing::info!(?event, "handle_handshake_action: received handshake event");
        match event {
            HandshakeEvent::InboundConnection {
                transaction,
                matched,
                connection,
                transient,
            } => {
                tracing::info!(?matched, transient, tx = ?transaction, "InboundConnection event");
                let _conn_manager = &self.bridge.op_manager.ring.connection_manager;
                let remote_addr = connection.remote_addr();

                if let Some(blocked_addrs) = &self.blocked_addresses {
                    if blocked_addrs.contains(&remote_addr) {
                        tracing::info!(
                            remote = %remote_addr,
                            transient,
                            transaction = ?transaction,
                            "Inbound connection blocked by local policy"
                        );
                        return Ok(());
                    }
                }

                // A connection matched to an expectation is taken to be the one
                // the connect op is dialling, so a peer that reaches us before
                // (or instead of) our dial is still linked: it is bound to the
                // key the op dialled, as a claim, and promoted under it. The
                // claim is held back while another connection holds the key
                // (see `handle_successful_connection`). Any other
                // inbound connection starts without a peer identity and gets
                // one when the connect op names the peer in a ConnectPeer, or
                // when the peer names itself in a ConnectRequest.
                let peer = match matched {
                    HandshakeInboundMatch::Expected(Some(peer_key)) => {
                        // As for a successful dial: CONNECT routing stops
                        // avoiding an address that is now connected.
                        self.bridge
                            .op_manager
                            .ring
                            .connection_manager
                            .clear_failed_addr(remote_addr);
                        Some(PeerKeyLocation::new(peer_key, remote_addr))
                    }
                    HandshakeInboundMatch::Expected(None) => {
                        TRANSPORT_METRICS.record_conn_identity(
                            remote_addr,
                            ConnIdentity::ExpectedInboundUnbound,
                        );
                        None
                    }
                    HandshakeInboundMatch::Unexpected => {
                        tracing::info!(
                            remote = %remote_addr,
                            transient,
                            transaction = ?transaction,
                            "Inbound connection arrived without matching expectation; accepting provisionally"
                        );
                        None
                    }
                };

                tracing::info!(
                    remote = %remote_addr,
                    peer_known = peer.is_some(),
                    transient,
                    transaction = ?transaction,
                    "Inbound connection established"
                );

                // Clear backoff on inbound connection: the peer is demonstrably alive (#3252).
                if !remote_addr.ip().is_unspecified() {
                    state.peer_backoff.record_success(remote_addr);
                }

                // Treat only transient connections as transient. Normal inbound dials (including
                // gateway bootstrap from peers) should be promoted into the ring once established.
                let is_transient = transient;

                self.handle_successful_connection(
                    peer,
                    connection,
                    state,
                    None,
                    is_transient,
                    handshake_commands,
                    IdentitySource::Claimed,
                )
                .await?;
            }
            HandshakeEvent::OutboundEstablished {
                transaction,
                peer,
                connection,
                transient,
            } => {
                // Outbound peers must have known addresses - use type-safe conversion
                // If conversion fails, log error but continue since connection is established
                let peer_addr = match KnownPeerKeyLocation::try_from(&peer) {
                    Ok(k) => k.socket_addr(),
                    Err(e) => {
                        tracing::error!(
                            transaction = %transaction,
                            pub_key = %e.pub_key,
                            "INTERNAL ERROR: outbound connection established but peer has unknown address"
                        );
                        // Use peer's pub_key in log, but we need an address for downstream logging
                        // This should never happen, so use unspecified as last resort
                        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
                    }
                };
                tracing::info!(
                    remote = %peer_addr,
                    transient,
                    transaction = %transaction,
                    "Outbound connection established"
                );

                // Clear backoff and failed-addr tracking on successful connection
                if !peer_addr.ip().is_unspecified() {
                    state.peer_backoff.record_success(peer_addr);
                    self.bridge
                        .op_manager
                        .ring
                        .connection_manager
                        .clear_failed_addr(peer_addr);
                }
                // The dial the expectation for this address waited on has
                // succeeded, so a later inbound connection from the address is
                // not that peer's. Non-blocking (#4145); a missed drop leaves
                // the entry to the driver's window sweep.
                if !handshake_commands
                    .try_send(HandshakeCommand::DropConnection { addr: peer_addr })
                {
                    tracing::debug!(
                        remote = %peer_addr,
                        "Handshake command channel full/closed; expectation left to its window"
                    );
                }

                // For outbound connections, respect the transient flag from the handshake.
                // Gateway connections should remain transient until CONNECT acceptance.
                self.handle_successful_connection(
                    Some(peer),
                    connection,
                    state,
                    None,
                    transient,
                    handshake_commands,
                    IdentitySource::Dialled,
                )
                .await?;
            }
            HandshakeEvent::OutboundFailed {
                transaction,
                peer,
                error,
                transient,
            } => {
                // Outbound peers must have known addresses - extract once for reuse
                let peer_addr = match KnownPeerKeyLocation::try_from(&peer) {
                    Ok(k) => k.socket_addr(),
                    Err(e) => {
                        // This is an internal consistency error - we initiated this connection,
                        // so we should always know the target address
                        tracing::error!(
                            transaction = %transaction,
                            pub_key = %e.pub_key,
                            "INTERNAL ERROR: outbound connection failed but peer has unknown address"
                        );
                        return Ok(());
                    }
                };

                tracing::info!(
                    remote = %peer_addr,
                    transient,
                    transaction = %transaction,
                    ?error,
                    "Outbound connection failed"
                );

                self.bridge
                    .op_manager
                    .ring
                    .connection_manager
                    .prune_in_transit_connection(peer_addr);

                // Only record transport-level failures (NAT traversal, timeout, I/O)
                // so future CONNECT requests avoid routing to this peer. Protocol
                // errors or rejections don't indicate the peer is unreachable.
                if matches!(
                    error,
                    ConnectionError::TransportError(_)
                        | ConnectionError::Timeout
                        | ConnectionError::IOError(_)
                ) {
                    self.bridge
                        .op_manager
                        .ring
                        .connection_manager
                        .record_failed_addr(peer_addr);
                }

                // Record failure for the connecting page diagnostics
                let failure_reason = match &error {
                    ConnectionError::TransportError(msg) => {
                        crate::node::network_status::classify_transport_error(msg)
                    }
                    ConnectionError::Timeout => crate::node::network_status::FailureReason::Timeout,
                    ConnectionError::IOError(msg) => {
                        crate::node::network_status::FailureReason::Other(msg.clone())
                    }
                    other @ ConnectionError::LocationUnknown
                    | other @ ConnectionError::SendNotCompleted(_)
                    | other @ ConnectionError::UnexpectedReq
                    | other @ ConnectionError::Serialization(_)
                    | other @ ConnectionError::FailedConnectOp
                    | other @ ConnectionError::UnwantedConnection
                    | other @ ConnectionError::AddressBlocked(_) => {
                        crate::node::network_status::FailureReason::Other(other.to_string())
                    }
                };
                crate::node::network_status::record_gateway_failure(peer_addr, failure_reason);

                // Record failure for exponential backoff to prevent rapid retries
                if !peer_addr.ip().is_unspecified() {
                    state.peer_backoff.record_failure(peer_addr);
                }

                let pending_txs = state
                    .awaiting_connection_txs
                    .remove(&peer_addr)
                    .unwrap_or_default();

                if let Some(callbacks) = state.awaiting_connection.remove(&peer_addr) {
                    tracing::debug!(
                        remote = %peer_addr,
                        callbacks = callbacks.len(),
                        pending_txs = ?pending_txs,
                        transient,
                        "Notifying callbacks after outbound failure"
                    );

                    let mut callbacks = callbacks.into_iter();
                    if let Some(mut cb) = callbacks.next() {
                        cb.send_result(Err(()))
                            .await
                            .inspect_err(|err| {
                                tracing::debug!(
                                    remote = %peer_addr,
                                    ?err,
                                    "Failed to deliver outbound failure notification"
                                );
                            })
                            .ok();
                    }
                    for mut cb in callbacks {
                        cb.send_result(Err(()))
                            .await
                            .inspect_err(|err| {
                                tracing::debug!(
                                    remote = %peer_addr,
                                    ?err,
                                    "Failed to deliver secondary outbound failure notification"
                                );
                            })
                            .ok();
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn handle_handshake_stream_closed(
        &mut self,
        state: &mut EventListenerState,
    ) -> anyhow::Result<()> {
        if state.awaiting_connection.is_empty() {
            return Ok(());
        }

        tracing::warn!(
            awaiting = state.awaiting_connection.len(),
            "Handshake driver closed; notifying pending callbacks"
        );

        let awaiting = std::mem::take(&mut state.awaiting_connection);
        let awaiting_txs = std::mem::take(&mut state.awaiting_connection_txs);

        for (addr, callbacks) in awaiting {
            let pending_txs = awaiting_txs.get(&addr).cloned().unwrap_or_default();
            tracing::debug!(
                remote = %addr,
                callbacks = callbacks.len(),
                pending_txs = ?pending_txs,
                "Delivering handshake driver shutdown notification"
            );
            for mut cb in callbacks {
                cb.send_result(Err(()))
                    .await
                    .inspect_err(|err| {
                        tracing::debug!(
                            remote = %addr,
                            ?err,
                            "Failed to deliver handshake driver shutdown notification"
                        );
                    })
                    .ok();
            }
        }

        Ok(())
    }

    /// `peer_id` is the peer identity the connection was established with,
    /// bound from `source`: the dialled peer of an outbound connection
    /// ([`IdentitySource::Dialled`]), or the key an expected-inbound
    /// registration carried for an inbound connection matched to it
    /// ([`IdentitySource::Claimed`]). `None` for any other inbound
    /// connection, which starts without an identity.
    #[allow(clippy::too_many_arguments)]
    async fn handle_successful_connection(
        &mut self,
        peer_id: Option<PeerKeyLocation>,
        connection: Box<dyn PeerConnectionApi>,
        state: &mut EventListenerState,
        remaining_checks: Option<usize>,
        is_transient: bool,
        handshake_commands: &HandshakeCommandSender,
        source: IdentitySource,
    ) -> anyhow::Result<()> {
        let mut broadcast_ready: Option<bool> = None;
        let connection_manager = &self.bridge.op_manager.ring.connection_manager;
        // For transient connections, we may not know the peer's identity yet - use connection's remote_addr
        let peer_addr = peer_id
            .as_ref()
            .and_then(|p| p.socket_addr())
            .unwrap_or_else(|| connection.remote_addr());

        if is_transient && !connection_manager.try_register_transient(peer_addr, None) {
            tracing::warn!(
                remote = %peer_addr,
                budget = connection_manager.transient_budget(),
                current = connection_manager.transient_count(),
                "Transient connection budget exhausted; dropping inbound connection"
            );
            if let Some(callbacks) = state.awaiting_connection.remove(&peer_addr) {
                for mut cb in callbacks {
                    // Best effort - caller will handle the connection failure
                    #[allow(clippy::let_underscore_must_use)]
                    let _ = cb.send_result(Err(())).await;
                }
            }
            state.awaiting_connection_txs.remove(&peer_addr);
            return Ok(());
        }

        // IMPORTANT: Insert into self.connections BEFORE removing from awaiting_connection.
        // This prevents a race condition where another connect() call could slip through
        // during the window when neither structure has the entry, causing the transport
        // layer to tear down the connection we just established.
        // See: handle_connect_peer checks self.connections first, then awaiting_connection.
        // If the peer already has a connection (e.g., reconnected with new identity after
        // suspend/resume), replace it. Dropping the old sender causes its
        // peer_connection_listener to fire TransportClosed; the connection_id mechanism
        // ensures that stale event won't remove the new entry.
        if let Some(old) = self.connections.remove(&peer_addr) {
            self.record_identity_teardown(peer_addr, &old);

            // Full cleanup: handle both transient and ring-promoted connections.
            // If transient, drop_transient releases the budget slot.
            // If ring-promoted, prune_connection cleans up topology, orphaned txs,
            // subscriptions, and notifies the handshake driver.
            let was_transient = self
                .bridge
                .op_manager
                .ring
                .connection_manager
                .drop_transient(peer_addr)
                .is_some();

            if !was_transient {
                // Old connection was ring-promoted — mirror TransportClosed cleanup
                let prune_result = self
                    .bridge
                    .op_manager
                    .ring
                    .prune_connection(
                        self.teardown_peer_id(peer_addr, old.pub_key()),
                        crate::tracing::DisconnectReason::Explicit(
                            "replaced_by_new_connection".into(),
                        ),
                    )
                    .await;
                self.bridge
                    .handle_orphaned_transactions(prune_result.orphaned_transactions, peer_addr)
                    .await;
                if prune_result.became_unready {
                    // Deferred: broadcast after connection_manager borrow ends
                    broadcast_ready = Some(false);
                }
                // Non-blocking (#4145): this runs on the event-loop task while
                // handling an inbound handshake event, so a `.send().await` that
                // back-pressures on the handshake command channel (cap 128) would
                // stall the loop. The DropConnection for the REPLACED (stale)
                // connection is redundant cleanup — we've already pruned the old
                // peer from the ring above and are about to install the new
                // connection entry. A missed DropConnection leaves at most one
                // stale per-SocketAddr InboundMatcher entry for the address,
                // which the driver sweeps once it outlives
                // EXPECTED_INBOUND_WINDOW and whose key it does not honour
                // after that.
                if !handshake_commands
                    .try_send(HandshakeCommand::DropConnection { addr: peer_addr })
                {
                    tracing::warn!(
                        remote = %peer_addr,
                        "Handshake command channel full/closed; skipping redundant \
                         drop notification for replaced connection"
                    );
                }
            }

            // Per-peer state is forgotten for a connection that was in the
            // ring, and only when no other open connection is bound to its key.
            if let Some(old_pub_key) = old.pub_key() {
                self.release_identity(peer_addr, old_pub_key, !was_transient)
                    .await;
            }
            tracing::info!(
                %peer_addr,
                old_connection_id = old.connection_id,
                was_transient,
                "Replacing the connection entry at this address"
            );
        }

        if is_transient {
            let cm = &self.bridge.op_manager.ring.connection_manager;
            let current = cm.transient_count();
            if current >= cm.transient_budget() {
                tracing::warn!(
                    remote = %peer_addr,
                    budget = cm.transient_budget(),
                    current,
                    "Transient connection budget exhausted; dropping inbound connection before insert"
                );
                return Ok(());
            }
        }
        let conn_id = next_connection_id();
        // Capture the remote's negotiated protocol version BEFORE the connection
        // is moved into the spawned listener task below. Used to gate
        // version-dependent message types (e.g. SubscribeHint).
        let remote_version = connection.remote_version();
        let receipt = connection.receipt_stamp();
        let (tx, rx) = mpsc::channel(10);
        tracing::debug!(
            self_peer = %self.bridge.op_manager.ring.connection_manager.pub_key,
            peer_id = ?peer_id,
            %peer_addr,
            connection_id = conn_id,
            conn_map_size = self.connections.len(),
            "[CONN_TRACK] INSERT: adding connection to HashMap"
        );
        let now = Instant::now();
        let link_use = zombie_sweep::LinkUseStamp::new(now);
        self.connections.insert(
            peer_addr,
            ConnectionEntry {
                sender: tx,
                // Bound below through `bind_identity`. An unexpected inbound
                // connection stays unbound until a ConnectPeer or the peer's
                // first ConnectRequest names it.
                identity: Default::default(),
                receipt,
                connection_id: conn_id,
                created_at: now,
                link_use: link_use.clone(),
                remote_version,
            },
        );
        // Mirror the same version onto `ConnectionManager` so op drivers
        // (reached via `op_manager.ring.connection_manager`, not this
        // network-bridge layer) can gate emission of version-sensitive wire
        // variants — e.g. the summary-first PUT probe's
        // `supports_summary_first_put` check. Keep this write paired with
        // the `self.connections.insert` above if that call site changes.
        // Write the Option THROUGH — unconditionally, so a reconnection whose
        // version is unknown CLEARS any stale entry rather than leaving us
        // believing a downgraded peer is still current. Since #5161 a `None`
        // here means the remote is genuinely below the version-carrying-ack
        // floor, not that this path cannot observe it.
        // See `ConnectionManager::record_remote_version`.
        self.bridge
            .op_manager
            .ring
            .connection_manager
            .record_remote_version(peer_addr, remote_version);
        // Bind the identity the connection was established with: the dialled
        // key, or the key an expected-inbound registration carried, which is
        // a claim. A key that another connection still holds is not bound
        // yet, whatever the source: the connection continues unidentified,
        // neither recorded nor promoted under the key, with the claim pending
        // until the holder closes or goes quiet (`release_identity` then
        // binds it, and promotes it when this connection would have been
        // promoted now).
        let promote_on_bind =
            !is_transient || self.bridge.op_manager.ring.connection_manager.is_gateway();
        // Whether another connection holds the key, so this one is not bound
        // under it now.
        let mut held_back = false;
        let peer_id = match peer_id {
            Some(peer) => {
                let outcome =
                    self.bind_identity(peer_addr, peer.pub_key(), source, Some(promote_on_bind));
                match outcome {
                    BindOutcome::Bound => {
                        let kind = match source {
                            IdentitySource::Dialled => ConnIdentity::SetDialed,
                            IdentitySource::Claimed => ConnIdentity::ExpectedInboundBound,
                        };
                        TRANSPORT_METRICS.record_conn_identity(peer_addr, kind);
                        Some(peer)
                    }
                    BindOutcome::Held(holder) => {
                        held_back = true;
                        TRANSPORT_METRICS.record_conn_identity(peer_addr, ConnIdentity::ClaimHeld);
                        tracing::info!(
                            %peer_addr,
                            %holder,
                            pub_key = %peer.pub_key(),
                            ?source,
                            "New connection left unbound until the connection holding its \
                             key closes or goes quiet"
                        );
                        None
                    }
                    // The quiet holder is replaced; its release binds the newest
                    // pending claim of the key (normally this one) and promotes
                    // it, so nothing is left to promote here.
                    BindOutcome::HeldByQuiet(quiet) => {
                        held_back = true;
                        self.replace_quiet_holder(quiet, peer.pub_key(), handshake_commands)
                            .await;
                        None
                    }
                    // The entry was inserted above without an identity.
                    BindOutcome::Same | BindOutcome::Mismatch(_) | BindOutcome::NoConnection => {
                        tracing::error!(
                            %peer_addr,
                            ?outcome,
                            "INTERNAL ERROR: a new connection entry was not unbound"
                        );
                        None
                    }
                }
            }
            None => None,
        };
        let connection_manager = &self.bridge.op_manager.ring.connection_manager;
        let Some(conn_events) = self.conn_event_tx.as_ref().cloned() else {
            anyhow::bail!("Connection event channel not initialized");
        };

        // Phase 4: Set orphan stream registry on connection for handling race conditions
        // between stream fragments and metadata messages (RequestStreaming/ResponseStreaming).
        let mut connection = connection;
        connection
            .set_inbound_stream_registry(self.bridge.op_manager.orphan_stream_registry().clone());

        // Use tokio::spawn directly instead of GlobalExecutor::spawn.
        // GlobalExecutor::spawn uses Handle::try_current().spawn() which doesn't
        // reliably poll tasks in certain test contexts (see issue #2709).
        let outbound_mix = self.bridge.op_manager.outbound_mix.clone();
        tokio::spawn(async move {
            peer_connection_listener(
                rx,
                connection,
                peer_addr,
                conn_events,
                conn_id,
                outbound_mix,
                link_use,
            )
            .await;
        });
        // Yield to allow the spawned peer_connection_listener task to start.
        // This is important because on some runtimes (especially in tests with boxed_local
        // futures), spawned tasks may not be scheduled immediately, causing messages
        // sent to the channel to pile up without being processed.
        tokio::task::yield_now().await;
        let newly_inserted = true;

        // Now safe to remove from awaiting_connection and notify callbacks.
        // self.connections already has the entry, so concurrent connect() calls
        // will see it and reuse the existing connection instead of tearing it down.
        let pending_txs = state
            .awaiting_connection_txs
            .remove(&peer_addr)
            .unwrap_or_default();
        if let Some(callbacks) = state.awaiting_connection.remove(&peer_addr) {
            // The callback expects the remote peer's address (not our own)
            let resolved_addr = peer_addr;
            tracing::debug!(
                remote = %peer_addr,
                callbacks = callbacks.len(),
                "handle_successful_connection: notifying waiting callbacks"
            );
            tracing::info!(
                remote = %peer_addr,
                callbacks = callbacks.len(),
                pending_txs = ?pending_txs,
                remaining_checks = ?remaining_checks,
                "handle_successful_connection: connection established"
            );
            for mut cb in callbacks {
                match timeout(
                    Duration::from_secs(60),
                    cb.send_result(Ok((resolved_addr, remaining_checks))),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(())) => {
                        tracing::debug!(
                            remote = %peer_addr,
                            "Callback dropped before receiving connection result"
                        );
                    }
                    Err(error) => {
                        tracing::error!(
                            remote = %peer_addr,
                            ?error,
                            "Failed to deliver connection result"
                        );
                    }
                }
            }
        } else {
            tracing::debug!(
                peer_id = ?peer_id,
                %peer_addr,
                pending_txs = ?pending_txs,
                "No callback for connection established"
            );
        }

        // Only promote to ring if we know the peer's identity.
        // For transient connections without known identity, we keep them as transport-only
        // until the peer identifies itself (e.g., via ConnectRequest).
        // A connection is bound only when no other connection holds its key,
        // so promoting it never puts its key in the ring at a second address.
        let promote_to_ring =
            peer_id.is_some() && (!is_transient || connection_manager.is_gateway());
        // A connection that is not promoted here because another connection
        // holds its key releases the reservation the connect op made for it,
        // which would otherwise wait for its expiry. One left out of the ring
        // for another reason (a transient connection) keeps its reservation.
        if held_back
            && !promote_to_ring
            && !connection_manager.is_in_ring(peer_addr)
            && connection_manager.has_connection_or_pending(peer_addr)
        {
            connection_manager.prune_in_transit_connection(peer_addr);
        }

        if newly_inserted {
            tracing::info!(peer_id = ?peer_id, %peer_addr, is_transient, "handle_successful_connection: inserted new connection entry");
            // Already in the ring when a quiet holder of its key was replaced
            // on arrival and this connection promoted in its place; that
            // promotion recorded its location and key, which this would erase.
            if !connection_manager.is_in_ring(peer_addr) {
                crate::node::network_status::record_peer_connected(peer_addr, None, None);
            }
            if promote_to_ring {
                // Only prune reservation when promoting to ring - transient connections
                // don't go through should_accept() so they have no reservation to prune
                let pending_loc = connection_manager.prune_in_transit_connection(peer_addr);
                // Safe to unwrap: promote_to_ring is only true when peer_id.is_some()
                let peer = peer_id
                    .as_ref()
                    .expect("promote_to_ring requires known peer_id");
                let loc = pending_loc.unwrap_or_else(|| Location::from_address(&peer_addr));
                tracing::info!(
                    %peer_addr,
                    %loc,
                    pending_loc_known = pending_loc.is_some(),
                    "handle_successful_connection: evaluating promotion to ring"
                );
                // Don't re-run should_accept() here — the connection was already
                // accepted at the protocol level (the CONNECT terminus handler in connect.rs) and transport
                // establishment (NAT traversal) already succeeded. Re-evaluating
                // the probabilistic Kleinberg filter would discard hard-won
                // connections for no capacity reason (#3545).
                // Enforce the hard max_connections cap below, EXCEPT for a
                // strictly-closer per-side nearest-neighbor lattice edge within
                // the bounded over-max ceiling (mechanism 3). Without this
                // exception the cap silently drops the guaranteed
                // successor/predecessor edge on the REAL CONNECT/promotion path,
                // making mechanism 3's at-capacity displacement inert in
                // production (nodes routinely sit at max). The same predicate is
                // re-checked in add_connection; the over-max topology prune sheds a
                // farther non-lattice link next tick. See
                // connection_manager.rs::{admits_lattice_edge_over_cap, LATTICE_OVERMAX_SLACK}.
                let current = connection_manager.connection_count();
                if connection_manager.refuses_connection_at_cap(loc) {
                    tracing::warn!(
                        %peer_addr,
                        current_connections = current,
                        max_connections = connection_manager.max_connections,
                        %loc,
                        "handle_successful_connection: rejecting new connection to enforce cap"
                    );
                    // Drop the connection immediately instead of letting it linger
                    // as a zombie for up to 5 minutes confusing the dashboard.
                    crate::node::network_status::record_peer_disconnected(peer_addr);
                    self.drop_connection_by_addr(
                        peer_addr,
                        handshake_commands,
                        crate::tracing::DisconnectReason::ConnectionLimitReached,
                    )
                    .await;
                    return Ok(());
                }
                tracing::info!(%peer_addr, %loc, "handle_successful_connection: promoting connection into ring");
                // #4787 instrumentation: this is the OTHER promotion path, and
                // on a gateway it is the one inbound connections with a known
                // peer_id actually take (`promote_to_ring` above is true for
                // any transient when `is_gateway()`). Leaving it uncounted made
                // the `transient_expired : promoted_to_ring` ratio the PR's own
                // doc tells operators to build read far worse than reality.
                // Gated on `added` for the same reason as the `handle_connect_peer`
                // site.
                let outcome = self
                    .bridge
                    .op_manager
                    .ring
                    .add_connection_reporting(
                        loc,
                        PeerId::new(peer.pub_key().clone(), peer_addr),
                        true,
                    )
                    .await;
                if outcome.added {
                    crate::node::network_status::record_bootstrap_promoted_to_ring();
                }
                let just_became_ready = outcome.just_became_ready;
                let pkl = crate::ring::PeerKeyLocation::new(peer.pub_key().clone(), peer_addr);
                crate::node::network_status::record_peer_connected(
                    peer_addr,
                    Some(loc.as_f64()),
                    Some(pkl),
                );
                // Only count as NAT success for non-gateway peers (gateway connections are direct)
                if !crate::node::network_status::is_known_gateway(&peer_addr) {
                    crate::node::network_status::record_nat_attempt(true);
                }

                // tell the new peer about our subscriptions and cache.
                // Non-blocking (#4145): handle_successful_connection runs INLINE
                // on the event-loop task, and bridge.send() awaits the cap-512
                // ev_listener_tx that this same task drains — a self-stall on
                // exactly the connection-establishment fan-out the SubscribeHint
                // nudge drives. Use bridge.try_send: a dropped interest/cache
                // message is recovered by the periodic InterestSync exchange +
                // subscription-renewal background task.
                for (target, msg) in self
                    .bridge
                    .op_manager
                    .on_ring_connection_established(peer_addr, peer.pub_key())
                {
                    if let Err(e) = self.bridge.try_send(target, msg) {
                        tracing::warn!(
                            %peer_addr,
                            error = %e,
                            "Failed to send interest/cache data to new peer \
                             (will re-sync via periodic InterestSync/renewal)"
                        );
                    }
                }

                // Directed-subscribe placement (#4404): the new neighbor may be
                // the closest non-hosting peer for some contracts we host.
                self.consider_migration_for_new_peer(peer_addr);

                // Broadcast readiness if we just crossed the threshold (deferred)
                if just_became_ready {
                    broadcast_ready = Some(true);
                }

                // Note: In the simplified 2026-01 architecture, subscriptions are lease-based
                // and renewed periodically by the subscription renewal background task.
                // We no longer attempt to establish subscriptions opportunistically on peer connect.

                if is_transient {
                    connection_manager.drop_transient(peer_addr);
                }
            } else {
                // Not promoting to ring - either unknown identity or transient on non-gateway.
                //
                // IMPORTANT: Before registering as transient, check if the connection was already
                // promoted to ring by another code path (e.g., handle_connect_peer running during
                // our yield_now().await or callback sends). This prevents a race condition where:
                // 1. We start with peer_id=None, so promote_to_ring=false
                // 2. We yield at yield_now().await or cb.send_result().await
                // 3. Message arrives, handle_connect_peer promotes connection to ring
                // 4. We resume and would register as transient (re-inserting a dropped entry)
                // 5. TTL timer fires and removes the connection from ring!
                //
                // By checking has_connection_or_pending, we skip transient registration if the
                // connection was already promoted to ring during our await points.
                if connection_manager.has_connection_or_pending(peer_addr) {
                    tracing::debug!(
                        %peer_addr,
                        "Skipping transient registration - connection already in ring topology"
                    );
                } else {
                    // These connections don't go through should_accept() so there's no pending
                    // reservation. Compute location from address directly.
                    let loc = Location::from_address(&peer_addr);
                    // #4787 instrumentation: `try_register_transient` returns
                    // `true` both for a fresh insertion and for refreshing an
                    // address it already tracks, and `false` when the transient
                    // budget refused it (nothing inserted at all). Only a fresh
                    // insertion is a "transient registered".
                    let already_transient = connection_manager.is_transient(peer_addr);
                    let registered =
                        connection_manager.try_register_transient(peer_addr, Some(loc));
                    if registered && !already_transient {
                        crate::node::network_status::record_bootstrap_transient_registered();
                    }
                    tracing::info!(
                        peer_id = ?peer_id,
                        %peer_addr,
                        "Registered transient connection (not added to ring topology)"
                    );
                    let ttl = connection_manager.transient_ttl();
                    let cm = connection_manager.clone();
                    GlobalExecutor::spawn(async move {
                        sleep(ttl).await;
                        if cm.drop_transient(peer_addr).is_some() {
                            // Only remove the transient tracking entry. Do NOT
                            // dispatch DropConnection — that tears down the
                            // underlying transport, killing any in-progress
                            // CONNECT handshake. If the CONNECT succeeds later,
                            // handle_connect_peer() will detect the missing
                            // transient entry via is_in_ring() and still promote
                            // the peer to ring topology (see #3113 fix). If it
                            // never succeeds, the transport idle timeout handles
                            // cleanup.
                            crate::node::network_status::record_bootstrap_transient_expired();
                            tracing::info!(
                                %peer_addr,
                                "Transient connection expired; removed tracking entry \
                                 (transport preserved, handle_connect_peer will promote via fallback)"
                            );
                        }
                    });
                }
            }
        } else if is_transient {
            // We reserved budget earlier, but didn't take ownership of the connection.
            connection_manager.drop_transient(peer_addr);
        }
        // Deferred broadcast: connection_manager borrow is now dropped
        if let Some(ready) = broadcast_ready {
            self.handle_broadcast_ready_state(ready).await;
        }
        Ok(())
    }

    pub(super) async fn handle_transport_event(
        &mut self,
        event: Option<ConnEvent>,
        state: &mut EventListenerState,
        handshake_commands: &HandshakeCommandSender,
    ) -> anyhow::Result<EventResult> {
        match event {
            Some(ConnEvent::InboundMessage(mut inbound)) => {
                let tx = *inbound.msg.id();

                if let Some(remote_addr) = inbound.remote_addr {
                    if let Some(sender_peer) = extract_sender_from_message(&inbound.msg) {
                        // Try to get known address, fall back to remote_addr if unknown
                        let sender_addr = KnownPeerKeyLocation::try_from(&sender_peer)
                            .ok()
                            .map(|k| k.socket_addr());
                        if sender_addr.map(|a| a == remote_addr).unwrap_or(false)
                            || sender_addr.map(|a| a.ip().is_unspecified()).unwrap_or(true)
                        {
                            // A sender given with an unspecified address gets
                            // the connection's remote address.
                            if sender_addr.is_some_and(|a| a.ip().is_unspecified()) {
                                if let Some(sender_mut) =
                                    extract_sender_from_message_mut(&mut inbound.msg)
                                {
                                    if sender_mut
                                        .socket_addr()
                                        .is_none_or(|a| a.ip().is_unspecified())
                                    {
                                        sender_mut.set_addr(remote_addr);
                                    }
                                }
                            }
                            // The peer names itself. A connection without an
                            // identity is bound to the named key; one that has
                            // an identity keeps it.
                            let outcome = self.bind_identity(
                                remote_addr,
                                sender_peer.pub_key(),
                                IdentitySource::Claimed,
                                Some(false),
                            );
                            if let Some(kind) = identity_kind(&outcome, CONNECT_REQUEST_KINDS) {
                                TRANSPORT_METRICS.record_conn_identity(remote_addr, kind);
                            }
                            if sender_addr == Some(remote_addr)
                                && outcome != BindOutcome::NoConnection
                            {
                                TRANSPORT_METRICS.record_conn_identity(
                                    remote_addr,
                                    ConnIdentity::ConnectRequestKnownAddr,
                                );
                            }
                            match outcome {
                                BindOutcome::Bound => {
                                    // Not promoted here: a connection is
                                    // promoted only when the connect op accepts
                                    // the peer with a ConnectPeer
                                    // (handle_connect_peer).
                                    tracing::info!(
                                        remote = %remote_addr,
                                        pub_key = %sender_peer.pub_key(),
                                        "Bound peer identity from its ConnectRequest"
                                    );
                                }
                                BindOutcome::Same | BindOutcome::NoConnection => {}
                                BindOutcome::Mismatch(bound) => {
                                    // A connection's identity does not change:
                                    // the request is dropped and the connection
                                    // closed, so the peer reconnects under the
                                    // key it names.
                                    tracing::warn!(
                                        remote = %remote_addr,
                                        %bound,
                                        named = %sender_peer.pub_key(),
                                        %tx,
                                        "ConnectRequest names a key other than the \
                                         connection's; closing the connection"
                                    );
                                    self.drop_connection_by_addr(
                                        remote_addr,
                                        handshake_commands,
                                        crate::tracing::DisconnectReason::Explicit(
                                            "identity_mismatch".into(),
                                        ),
                                    )
                                    .await;
                                    return Ok(EventResult::Continue);
                                }
                                BindOutcome::Held(holder) => {
                                    // The key is bound to another connection
                                    // that is still heard from. The request is
                                    // dropped and the claim kept pending on
                                    // this connection; it is bound when the
                                    // holder closes or goes quiet (for a peer
                                    // that restarted elsewhere, about
                                    // `identity::HOLD_WINDOW` after its old
                                    // session stops). The peer's connect op
                                    // retries the request.
                                    tracing::info!(
                                        remote = %remote_addr,
                                        %holder,
                                        named = %sender_peer.pub_key(),
                                        %tx,
                                        "ConnectRequest names a key another connection \
                                         holds; dropping it"
                                    );
                                    return Ok(EventResult::Continue);
                                }
                                BindOutcome::HeldByQuiet(quiet) => {
                                    // The connection holding the key has gone
                                    // quiet: it is replaced, and its release
                                    // binds the newest pending claim of the
                                    // key. The request is passed on if that
                                    // was this connection.
                                    self.replace_quiet_holder(
                                        quiet,
                                        sender_peer.pub_key(),
                                        handshake_commands,
                                    )
                                    .await;
                                    let bound_here =
                                        self.connections.get(&remote_addr).is_some_and(|entry| {
                                            entry.pub_key() == Some(sender_peer.pub_key())
                                        });
                                    if !bound_here {
                                        return Ok(EventResult::Continue);
                                    }
                                }
                            }
                        }
                    }
                }

                tracing::debug!(
                    peer_addr = ?inbound.remote_addr,
                    %tx,
                    tx_type = ?tx.transaction_type(),
                    "Queueing inbound NetMessage from peer connection"
                );
                Ok(EventResult::Event(
                    ConnEvent::InboundMessage(inbound).into(),
                ))
            }
            Some(ConnEvent::TransportClosed {
                remote_addr,
                error,
                connection_id,
                reason,
            }) => {
                tracing::debug!(
                    remote = %remote_addr,
                    ?error,
                    connection_id,
                    %reason,
                    "peer_connection_listener reported transport closure"
                );
                // Ignore stale TransportClosed from replaced connections: the old
                // listener's channel was dropped, but we must not remove the new entry.
                if let Some(current) = self.connections.get(&remote_addr) {
                    if current.connection_id != connection_id {
                        tracing::info!(
                            remote = %remote_addr,
                            stale_id = connection_id,
                            current_id = current.connection_id,
                            "Ignoring stale TransportClosed from replaced connection"
                        );
                        return Ok(EventResult::Continue);
                    }
                }
                // Look up the connection directly by address
                if let Some(entry) = self.connections.remove(&remote_addr) {
                    self.record_identity_teardown(remote_addr, &entry);
                    tracing::debug!(self_peer = %self.bridge.op_manager.ring.connection_manager.pub_key, peer = ?entry.pub_key(), socket_addr = %remote_addr, conn_map_size = self.connections.len(), "[CONN_TRACK] REMOVE: TransportClosed - removing from connections HashMap");
                    let prune_result = self
                        .bridge
                        .op_manager
                        .ring
                        .prune_connection(
                            self.teardown_peer_id(remote_addr, entry.pub_key()),
                            reason,
                        )
                        .await;

                    // Handle orphaned transactions immediately (retry via alternate routes)
                    self.bridge
                        .handle_orphaned_transactions(
                            prune_result.orphaned_transactions,
                            remote_addr,
                        )
                        .await;

                    if prune_result.became_unready {
                        self.handle_broadcast_ready_state(false).await;
                    }

                    // Forget this peer's subscriptions and cached contract
                    // state, only when no other open connection is bound to
                    // its key; a connection that never named a peer has none.
                    if let Some(pub_key) = entry.pub_key() {
                        self.release_identity(remote_addr, pub_key, true).await;
                    }

                    // Backoff prevents reconnect-timeout-reconnect cycles to dead peers (#3252).
                    if !remote_addr.ip().is_unspecified() {
                        state.peer_backoff.record_failure(remote_addr);
                        tracing::debug!(remote = %remote_addr, "Recorded peer backoff after transport closure");
                    }

                    // Non-blocking (#4145): handle_transport_event runs on the
                    // event-loop task, so a `.send().await` that back-pressures on
                    // the handshake command channel (cap 128) would stall the loop.
                    // The DropConnection is redundant teardown — the connection is
                    // already removed from ctx.connections and pruned from the ring
                    // above. A missed DropConnection leaves at most one stale
                    // per-SocketAddr InboundMatcher entry for the address, which
                    // the driver sweeps once it outlives EXPECTED_INBOUND_WINDOW
                    // and whose key it does not honour after that.
                    if !handshake_commands
                        .try_send(HandshakeCommand::DropConnection { addr: remote_addr })
                    {
                        tracing::warn!(
                            remote = %remote_addr,
                            "Handshake command channel full/closed; skipping redundant \
                             drop notification for closed connection"
                        );
                    }
                }
                Ok(EventResult::Continue)
            }
            Some(other) => {
                tracing::warn!(?other, "Unexpected event from peer connection listener");
                Ok(EventResult::Continue)
            }
            None => {
                tracing::error!("All peer connection event channels closed");
                Ok(EventResult::Continue)
            }
        }
    }
}

/// Tell the per-connection task of a dropped connection to close, without
/// blocking the caller, which runs on the event loop: a full per-peer
/// channel must not stall it (`.claude/rules/channel-safety.md`). When the
/// channel is full, the send is moved to a short task that waits up to 1 s,
/// as the inline send used to.
fn notify_connection_dropped(
    sender: PeerConnChannelSender,
    peer_addr: SocketAddr,
    reason: crate::tracing::DisconnectReason,
) {
    let event = Right(ConnEvent::NodeAction(NodeEvent::DropConnection(
        peer_addr, reason,
    )));
    match sender.try_send(event) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::TrySendError::Full(event)) => {
            GlobalExecutor::spawn(async move {
                match timeout(Duration::from_secs(1), sender.send(event)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => tracing::debug!(
                        %peer_addr,
                        "Connection task gone before the drop notice was delivered"
                    ),
                    Err(_) => tracing::warn!(
                        %peer_addr,
                        "Timeout while sending drop connection message"
                    ),
                }
            });
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
            tracing::debug!(%peer_addr, "Connection task gone before the drop notice");
        }
    }
}

/// The origin a dial is reported under: `Gateway` for any dial to a
/// configured gateway (or one the caller marked as a gateway dial), whichever
/// path started it, otherwise the caller's `origin`. A CONNECT joiner whose
/// acceptor is a gateway, for example, is still a gateway dial.
fn dial_origin(
    origin: crate::transport::NatAttemptOrigin,
    is_gw: bool,
    peer_addr: SocketAddr,
    gateways: &[PeerKeyLocation],
) -> crate::transport::NatAttemptOrigin {
    if is_gw
        || gateways
            .iter()
            .any(|gw| gw.socket_addr() == Some(peer_addr))
    {
        crate::transport::NatAttemptOrigin::Gateway
    } else {
        origin
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod dial_origin_tests {
    use super::*;
    use crate::transport::NatAttemptOrigin;

    fn gateway() -> PeerKeyLocation {
        PeerKeyLocation::new(
            crate::transport::TransportKeypair::new().public().clone(),
            "192.0.2.1:31337".parse().unwrap(),
        )
    }

    /// A dial to a configured gateway is `Gateway` whatever path started it.
    #[test]
    fn a_configured_gateway_is_always_a_gateway_dial() {
        let gw = gateway();
        let addr = gw.socket_addr().unwrap();
        for origin in NatAttemptOrigin::ALL {
            assert_eq!(
                dial_origin(origin, false, addr, std::slice::from_ref(&gw)),
                NatAttemptOrigin::Gateway,
                "{origin:?}"
            );
        }
    }

    /// A dial the caller marked as a gateway dial is `Gateway` even before
    /// the gateway list knows the address.
    #[test]
    fn a_marked_gateway_dial_is_a_gateway_dial() {
        let addr: SocketAddr = "198.51.100.4:9".parse().unwrap();
        assert_eq!(
            dial_origin(NatAttemptOrigin::RingPeerNoTransport, true, addr, &[]),
            NatAttemptOrigin::Gateway
        );
    }

    /// Source pin: `handle_connect_peer` applies `dial_origin` to the
    /// origin it hands to the handshake driver (the event loop has no
    /// harness here).
    #[test]
    fn handle_connect_peer_applies_dial_origin() {
        const SOURCE: &str = include_str!("connection_lifecycle.rs");
        let tests_at = SOURCE
            .find("\n#[cfg(test)]\nmod dial_origin_tests")
            .expect("test module");
        let start = SOURCE
            .find("pub(super) async fn handle_connect_peer(")
            .expect("handle_connect_peer renamed");
        assert!(start < tests_at, "the anchor must be production code");
        let open = start
            + SOURCE[start..]
                .find(") -> anyhow::Result<()> {")
                .expect("body")
            + 24;
        let (mut depth, mut end) = (0i32, open);
        for (i, b) in SOURCE.bytes().enumerate().skip(open) {
            match b {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = i;
                        break;
                    }
                }
                _ => {}
            }
        }
        assert!(end > open && end < tests_at, "unbalanced body slice");
        let code: String = SOURCE[open..end].split_whitespace().collect();
        let applied = code
            .find("letorigin=dial_origin(origin,transient,peer_addr,&self.gateways);")
            .expect("handle_connect_peer must apply dial_origin");
        let sent = code
            .find("HandshakeCommand::Connect{peer:peer.clone(),transaction:tx,transient,origin,}")
            .expect("the handshake command carries the origin");
        assert!(applied < sent, "applied before it is sent");
    }

    /// Any other dial keeps its origin, including one to another port of a
    /// gateway's IP.
    #[test]
    fn a_non_gateway_dial_keeps_its_origin() {
        let gw = gateway();
        let other_port: SocketAddr = "192.0.2.1:31338".parse().unwrap();
        for origin in NatAttemptOrigin::ALL {
            assert_eq!(
                dial_origin(origin, false, other_port, std::slice::from_ref(&gw)),
                origin,
                "{origin:?}"
            );
        }
    }
}
