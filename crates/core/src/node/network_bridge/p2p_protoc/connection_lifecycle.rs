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
    /// The ring took the connection now. The caller gives the connection's
    /// transport its key once the connection is bound to it
    /// (`P2pConnManager::tell_transport_peer_identity`).
    Promoted,
    /// The ring already held the connection.
    AlreadyInRing,
    /// The connection is not in the ring: the ring is at its cap, or the
    /// connection has no identity to promote it under.
    Refused,
}

impl P2pConnManager {
    /// Report how this end's native setup ended, once per setup (gate 22).
    /// Best effort: the event is dropped when telemetry is off or full, and
    /// the collector's per-type admission counters say how often.
    fn report_native_setup_outcome(
        &self,
        transaction: &Transaction,
        peer: &PeerKeyLocation,
        outcome: &str,
        origin: crate::transport::NatAttemptOrigin,
    ) {
        crate::tracing::telemetry::send_standalone_event_with_peer_id(
            "native_setup_outcome",
            &self
                .bridge
                .op_manager
                .ring
                .connection_manager
                .own_location()
                .to_string(),
            crate::operations::connect::native::native_setup_outcome_body(
                transaction,
                peer.pub_key(),
                outcome,
                origin,
            ),
        );
    }

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
        if promotion == Some(Promotion::Promoted) {
            self.tell_transport_peer_identity(claimant, key);
        }
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
        let removed = self.remove_connection(peer_addr);
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
        let removed = self.remove_connection(peer_addr);
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

    /// The connection at `addr` has just been promoted into the ring under
    /// `promoted`: give its transport the peer key, which the transport
    /// holds from then on (`PeerConnectionApi::set_peer_identity`). Only when
    /// the transport has a QUIC endpoint, since only a QUIC upgrade reads the
    /// key. The key sent is the identity bound to the connection, the node's
    /// own record; when `promoted` is not that key, or none is bound, nothing
    /// is sent and the mismatch is counted (`promotion_key_mismatch`). A key
    /// handed to the connection's task is counted `transport_key_sent`, and
    /// one that could not be handed over within the fallback's timeout
    /// `transport_key_undelivered`.
    ///
    /// Called only where a connection enters the ring (after
    /// `handle_successful_connection` adds it, and when
    /// `promote_existing_connection` returns `Promotion::Promoted`, once the
    /// connection is bound), so a connection's transport learns a key only
    /// at promotion. Does not wait: a full
    /// channel to the connection's task moves the send to a short task (see
    /// [`notify_connection_task`]).
    fn tell_transport_peer_identity(&self, addr: SocketAddr, promoted: &TransportPublicKey) {
        if !self.transport_has_quic {
            return;
        }
        let Some(entry) = self.connections.get(&addr) else {
            return;
        };
        let bound = match entry.pub_key() {
            Some(bound) if bound == promoted => bound.clone(),
            bound => {
                TRANSPORT_METRICS.record_conn_identity(addr, ConnIdentity::PromotionKeyMismatch);
                tracing::error!(
                    %addr,
                    %promoted,
                    ?bound,
                    "Connection promoted under a key other than the one bound to it; \
                     not giving its transport a key"
                );
                return;
            }
        };
        #[cfg(any(test, feature = "testing"))]
        if entry.native {
            let own = self
                .bridge
                .op_manager
                .ring
                .connection_manager
                .get_own_addr()
                .unwrap_or_else(|| SocketAddr::new(self.listening_ip, self.listening_port));
            crate::operations::connect::native_wire_trace::record_at(
                addr,
                crate::operations::connect::native_wire_trace::NativeWireEvent::NativeKeySent {
                    own,
                },
            );
        }
        notify_connection_task(
            entry.sender.clone(),
            Right(ConnEvent::SetPeerIdentity(bound)),
            addr,
            "peer identity",
            Some(DeliveryCounts {
                delivered: ConnIdentity::TransportKeySent,
                timed_out: ConnIdentity::TransportKeyUndelivered,
            }),
        );
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
        // Read, not removed: the connection keeps its transient slot unless
        // the ring takes it (`add_connection` removes the entry), so a
        // promotion refused at the cap leaves it counted.
        let was_transient = connection_manager.transient_entry(peer_addr);

        // Promote if: (a) was tracked as transient, OR (b) not already in ring.
        // Case (b) handles an open connection with no tracking entry (#3113:
        // the entry used to expire at the TTL while the transport stayed open;
        // an open connection now holds its entry until it is promoted or
        // closed, so this is the fallback for one that never had one).
        // Without this, CONNECT succeeds but peer is never added to ring
        // topology.
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
                self.count_refused_promotion(peer_addr);
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
            // `added` is also true when the ring re-adds an address it holds.
            let was_in_ring = connection_manager.is_in_ring(peer_addr);
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
                self.count_refused_promotion(peer_addr);
                return Promotion::Refused;
            }
            // A promotion only when the ring took an address it did not hold.
            let promotion = if outcome.added && !was_in_ring {
                Promotion::Promoted
            } else {
                Promotion::AlreadyInRing
            };

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
            return promotion;
        }

        Promotion::AlreadyInRing
    }

    /// A connection the ring refused stays open outside it, so it must hold a
    /// transient slot like any other such connection. One with no entry (it
    /// never had one) is registered and held here; when the budget or its
    /// source's share refuses that, it stays uncounted and
    /// [`Self::uncounted_outside_ring`] reports it, for the caller to close.
    fn count_refused_promotion(&self, peer_addr: SocketAddr) {
        let cm = &self.bridge.op_manager.ring.connection_manager;
        if cm.is_transient(peer_addr) || cm.is_in_ring(peer_addr) {
            return;
        }
        if cm
            .register_transient(peer_addr, Some(Location::from_address(&peer_addr)))
            .is_ok()
        {
            cm.hold_transient(peer_addr);
        }
    }

    /// An open connection at `peer_addr` that is neither in the ring nor
    /// holding a transient slot.
    fn uncounted_outside_ring(&self, peer_addr: SocketAddr) -> bool {
        let cm = &self.bridge.op_manager.ring.connection_manager;
        self.connections.contains_key(&peer_addr)
            && !cm.is_transient(peer_addr)
            && !cm.is_in_ring(peer_addr)
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
        transport: crate::operations::connect::native::DialTransport,
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
        #[cfg(any(test, feature = "testing"))]
        if transport == crate::operations::connect::native::DialTransport::Udp {
            crate::operations::connect::native_wire_trace::record_at(
                peer_addr,
                crate::operations::connect::native_wire_trace::NativeWireEvent::UdpDialFor {
                    tx,
                    own: self
                        .bridge
                        .op_manager
                        .ring
                        .connection_manager
                        .get_own_addr(),
                    peer: peer_addr,
                },
            );
        }

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
                    .send_result(Err(None))
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
                    let promotion = self
                        .promote_existing_connection(peer_addr, peer.pub_key(), Some(tx))
                        .await;
                    if promotion == Promotion::Refused {
                        true
                    } else {
                        let outcome = self.bind_identity(
                            peer_addr,
                            peer.pub_key(),
                            IdentitySource::Claimed,
                            None,
                        );
                        if let Some(kind) = identity_kind(&outcome, CONNECT_PEER_KINDS) {
                            TRANSPORT_METRICS.record_conn_identity(peer_addr, kind);
                        }
                        // After the bind: a connection without an identity is
                        // bound only once the ring has taken it.
                        if promotion == Promotion::Promoted {
                            self.tell_transport_peer_identity(peer_addr, peer.pub_key());
                        }
                        false
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
                    .send_result(Err(None))
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
                // Kept open, it would be outside the ring and uncounted.
                if self.uncounted_outside_ring(peer_addr) {
                    self.drop_connection_by_addr(
                        peer_addr,
                        handshake_commands,
                        crate::tracing::DisconnectReason::ConnectionLimitReached,
                    )
                    .await;
                    fail_awaiting_connection_callbacks(state, peer_addr).await;
                }
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
                .send_result(Err(None))
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
                    .send_result(Err(None))
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

        // A UDP dial to an address with a native dial pending runs beside it
        // (`EventListenerState::udp_beside_native`), and a later UDP dial
        // joins the one already beside it.
        if !transport.is_native() {
            if let Some(beside) = state.udp_beside_native.get_mut(&peer_addr) {
                beside.push((tx, callback));
                tracing::info!(
                    tx = %tx,
                    remote = %peer_addr,
                    pending = beside.len(),
                    "connect_peer: UDP dial beside a native dial pending; queued callback"
                );
                return Ok(());
            }
            if state.native_pending.contains(&peer_addr) {
                tracing::info!(
                    tx = %tx,
                    remote = %peer_addr,
                    transient,
                    "connect_peer: native dial pending; starting a UDP dial beside it"
                );
                state
                    .udp_beside_native
                    .insert(peer_addr, vec![(tx, callback)]);
                state.expected_inbound.expect_incoming(peer_addr);
                let origin = dial_origin(origin, transient, peer_addr, &self.gateways);
                if !handshake_commands.try_send(HandshakeCommand::Connect {
                    peer: peer.clone(),
                    transaction: tx,
                    transient,
                    origin,
                    transport,
                    // The beside dial exists to be UDP.
                    gateway_join: false,
                }) {
                    tracing::warn!(
                        tx = %tx,
                        remote = %peer_addr,
                        "Handshake command channel full/closed; failing the UDP dial \
                         beside a native one"
                    );
                    fail_udp_beside_native(state, peer_addr).await;
                }
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
                // A native dial expects no UDP handshake: its connection comes
                // from the QUIC endpoint, so a UDP intro from the address is
                // not this dial's.
                if transport.is_native() {
                    state.native_pending.insert(peer_addr);
                } else {
                    state.expected_inbound.expect_incoming(peer_addr);
                }
            }
        }

        // Non-blocking (#4145): connect_peer runs on the event-loop task (it is
        // driven by the ConnectPeer event handler), so a `.send().await` that
        // back-pressures on the handshake command channel (cap 128) would stall
        // the loop. On Full/Closed we run the SAME cleanup the blocking error
        // path used: prune the in-transit connection slot and fail every
        // awaiting callback with Err(None), so callers (including the SubscribeHint
        // resend-waiter registered above) are notified immediately instead of
        // parking. The connect op can retry on a later attempt; this failure is
        // LOCAL channel contention, not a remote connection failure, so we do
        // NOT record peer backoff.
        // Only the joiner's own dial to a gateway (the event loop's dial for
        // a message to a gateway it has no transport to) may try a native join
        // first. It carries the `Gateway` origin as the CALLER's, before
        // `dial_origin` widens it: a CONNECT acceptor's dial to a gateway's
        // address does not.
        let gateway_join = origin == crate::transport::NatAttemptOrigin::Gateway;
        let origin = dial_origin(origin, transient, peer_addr, &self.gateways);
        if !handshake_commands.try_send(HandshakeCommand::Connect {
            peer: peer.clone(),
            transaction: tx,
            transient,
            origin,
            transport,
            gateway_join,
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
            state.native_pending.remove(&peer_addr);
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

                // A gateway's admitted native joiner (#5940, phase B) has its
                // own path: its key is proven, and the survivor rule below
                // applies the other way round.
                if let Some(key) = connection.native_joiner_key() {
                    return self
                        .accept_native_join(connection, key, state, handshake_commands)
                        .await;
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

                // The survivor rule (`native_holds_peer`): an inbound UDP
                // connection from a peer a native connection holds is dropped,
                // leaving the native entry, the ring and the transient budget
                // untouched. The peer applies the same rule to its own copy
                // of this pair, so it normally keeps the native connection
                // too; see `native_holds_peer` for when the two ends can
                // decide differently.
                if let Some(holder) =
                    self.native_holds_peer(remote_addr, peer.as_ref().map(|p| p.pub_key()))
                {
                    tracing::info!(
                        remote = %remote_addr,
                        %holder,
                        transient,
                        transaction = ?transaction,
                        "Inbound UDP connection from a peer a native connection holds; \
                         keeping the native connection"
                    );
                    if self.native_entry_awaits_frame(holder, remote_addr) {
                        // Option (b): the native connection has not carried
                        // a frame from the peer yet, so this UDP connection
                        // becomes the entry and the native one is held
                        // beside it; the peer may never have finished the
                        // native setup.
                        self.swap_udp_in_beside_native(holder, connection).await?;
                        return Ok(());
                    }
                    TRANSPORT_METRICS.record_connect_native_survivor(false);
                    #[cfg(any(test, feature = "testing"))]
                    crate::operations::connect::native_wire_trace::record_at(
                        remote_addr,
                        crate::operations::connect::native_wire_trace::NativeWireEvent::Survivor {
                            swapped: false,
                            inbound: true,
                        },
                    );
                    drop(connection);
                    return Ok(());
                }

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
                transport,
                origin,
            } => {
                if transport.is_native() {
                    self.report_native_setup_outcome(
                        &transaction,
                        &peer,
                        crate::transport::NativeSetupOutcome::Established.label(),
                        origin,
                    );
                }
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
                if !transport.is_native() {
                    let beside = state.udp_beside_native.remove(&peer_addr);
                    // The survivor rule (`native_holds_peer`): a UDP dial to a
                    // peer a native connection holds is dropped without
                    // touching the ring or the transient budget, whether it
                    // ran beside that native dial or not, and its requesters
                    // are answered with the native connection.
                    if let Some(holder) = self.native_holds_peer(peer_addr, Some(peer.pub_key())) {
                        // The in-transit reservation this dial made at its
                        // own address is not used: the peer is held at
                        // `holder`. At the holder's own address it is the
                        // ring entry, which stays.
                        // Not while a native dial to that address is
                        // pending: the reservation is that dial's too.
                        if holder != peer_addr && !state.native_pending.contains(&peer_addr) {
                            let cm = &self.bridge.op_manager.ring.connection_manager;
                            if !cm.is_in_ring(peer_addr) && cm.has_connection_or_pending(peer_addr)
                            {
                                cm.prune_in_transit_connection(peer_addr);
                            }
                        }
                        tracing::info!(
                            %holder,
                            remote = %peer_addr,
                            transaction = %transaction,
                            beside = beside.is_some(),
                            "UDP dial connected to a peer a native connection holds; \
                             keeping the native connection"
                        );
                        // Option (b): a native connection that has not
                        // carried a frame from the peer yet does not displace
                        // this UDP connection; it is held beside it.
                        let hold = self.native_entry_awaits_frame(holder, peer_addr);
                        if !hold {
                            TRANSPORT_METRICS.record_connect_native_survivor(false);
                            #[cfg(any(test, feature = "testing"))]
                            crate::operations::connect::native_wire_trace::record_at(
                                peer_addr,
                                crate::operations::connect::native_wire_trace::NativeWireEvent::Survivor {
                                    swapped: false,
                                    inbound: false,
                                },
                            );
                        }
                        let mut requesters = Vec::new();
                        if let Some(beside) = beside {
                            // The pair's own native dial connected first.
                            TRANSPORT_METRICS.record_connect_native_both_dials_connected(true);
                            requesters = beside;
                        }
                        state.awaiting_connection_txs.remove(&peer_addr);
                        if let Some(callbacks) = state.awaiting_connection.remove(&peer_addr) {
                            requesters.extend(
                                callbacks
                                    .into_iter()
                                    .map(|callback| (transaction, callback)),
                            );
                        }
                        // Answered with the connection that holds the peer.
                        answer_beside(peer_addr, requesters, Ok((holder, None))).await;
                        if hold {
                            self.swap_udp_in_beside_native(holder, connection).await?;
                        } else {
                            drop(connection);
                        }
                        return Ok(());
                    }
                    if let Some(beside) = beside {
                        // A UDP dial beside a native one connected first: its
                        // requesters join the native dial's, and this
                        // connection answers them all, since every one of them
                        // asked for a connection to this peer. If the native
                        // dial then connects too, its connection is swapped in
                        // for this one.
                        if state.native_pending.contains(&peer_addr) {
                            state.beside_udp_connected.insert(peer_addr);
                        }
                        for (tx, callback) in beside {
                            state
                                .awaiting_connection
                                .entry(peer_addr)
                                .or_default()
                                .push(callback);
                            state
                                .awaiting_connection_txs
                                .entry(peer_addr)
                                .or_default()
                                .push(tx);
                        }
                    }
                }

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

                // A native dial ends here whatever follows, so it is no longer
                // pending, and its setup worked, so it resets the demotion
                // streak of the role it was made in (joiner or acceptor, each
                // counted apart) before anything decides which connection to
                // keep. Then, if the peer already has an entry under the same
                // key:
                // - a UDP one: the survivor rule (`native_holds_peer`) swaps
                //   this native connection in for it, in place, without
                //   promotion, prune or transient budget call;
                // - a native one: the first is kept, and this one is closed
                //   (dropped) without touching the ring or the budget.
                // Either way the native dial's requesters are answered with
                // the kept connection. An entry under another key is stale
                // (the peer came back under a new key, e.g. after a suspend)
                // and is replaced by the ordinary path below.
                if transport.is_native() {
                    state.native_pending.remove(&peer_addr);
                    let after_beside_udp = state.beside_udp_connected.remove(&peer_addr);
                    #[cfg(any(test, feature = "testing"))]
                    {
                        use crate::operations::connect::native_wire_trace::{
                            NativeWireEvent, record_at,
                        };
                        record_at(peer_addr, NativeWireEvent::NativeDial { established: true });
                        if let Some(own) = self
                            .bridge
                            .op_manager
                            .ring
                            .connection_manager
                            .get_own_addr()
                        {
                            record_at(
                                peer_addr,
                                NativeWireEvent::NativeSetupEnded {
                                    tx: transaction,
                                    own,
                                    established: true,
                                },
                            );
                        }
                    }
                    let existing_native = self
                        .connections
                        .get(&peer_addr)
                        .filter(|entry| entry.pub_key() == Some(peer.pub_key()))
                        .map(|entry| entry.native);
                    if let Some(existing_native) = existing_native {
                        tracing::info!(
                            remote = %peer_addr,
                            transaction = %transaction,
                            after_beside_udp,
                            existing_native,
                            "native dial connected after the pair connected another way"
                        );
                        if existing_native {
                            drop(connection);
                        } else {
                            // Kept beside the UDP entry until it carries a
                            // frame from the peer; `promote_held_native`
                            // makes it the entry then.
                            self.hold_native(peer_addr, connection, peer.pub_key().clone())
                                .await?;
                            if after_beside_udp {
                                TRANSPORT_METRICS.record_connect_native_both_dials_connected(false);
                            }
                        }
                        state.awaiting_connection_txs.remove(&peer_addr);
                        let mut requesters: Vec<_> = state
                            .awaiting_connection
                            .remove(&peer_addr)
                            .unwrap_or_default()
                            .into_iter()
                            .map(|callback| (transaction, callback))
                            .collect();
                        // A UDP dial beside this one still runs; its outcome
                        // ends the entry, which stays, empty.
                        if let Some(beside) = state.udp_beside_native.get_mut(&peer_addr) {
                            requesters.append(beside);
                        }
                        answer_beside(peer_addr, requesters, Ok((peer_addr, None))).await;
                        return Ok(());
                    }
                }
                // A native connection's key was proven in its handshake. A
                // gateway counts a native join here, as the UDP listener counts
                // a legacy one when its handshake completes.
                let source = match transport {
                    crate::operations::connect::native::DialTransport::Udp => {
                        IdentitySource::Dialled
                    }
                    crate::operations::connect::native::DialTransport::Native { .. } => {
                        self.record_native_gateway_join(peer_addr, connection.remote_version());
                        IdentitySource::Proven
                    }
                };
                // For outbound connections, respect the transient flag from the handshake.
                // Gateway connections should remain transient until CONNECT acceptance.
                self.handle_successful_connection(
                    Some(peer),
                    connection,
                    state,
                    None,
                    transient,
                    handshake_commands,
                    source,
                )
                .await?;
                // The native dial connected while the UDP dial beside it is
                // still running: its requesters are answered with this
                // connection now, if it was kept; the entry stays, empty,
                // until that dial's outcome arrives.
                if transport.is_native() && self.connections.contains_key(&peer_addr) {
                    if let Some(beside) = state.udp_beside_native.get_mut(&peer_addr) {
                        let requesters = std::mem::take(beside);
                        answer_beside(peer_addr, requesters, Ok((peer_addr, None))).await;
                    }
                }
            }
            HandshakeEvent::OutboundFailed {
                transaction,
                peer,
                error,
                transient,
                transport,
                native_outcome,
                origin,
            } => {
                if transport.is_native() {
                    self.report_native_setup_outcome(
                        &transaction,
                        &peer,
                        native_outcome.map_or("unknown", |outcome| outcome.label()),
                        origin,
                    );
                }
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
                #[cfg(any(test, feature = "testing"))]
                if transport.is_native() {
                    if let Some(own) = self
                        .bridge
                        .op_manager
                        .ring
                        .connection_manager
                        .get_own_addr()
                    {
                        crate::operations::connect::native_wire_trace::record_at(
                            peer_addr,
                            crate::operations::connect::native_wire_trace::NativeWireEvent::NativeSetupEnded {
                                tx: transaction,
                                own,
                                established: false,
                            },
                        );
                    }
                }

                let native = transport.is_native();
                // A native dial ends here, so it is no longer pending.
                if native {
                    state.native_pending.remove(&peer_addr);
                    state.beside_udp_connected.remove(&peer_addr);
                }
                // A UDP dial beside a native one failed.
                // - The pair is connected (the native dial won): its
                //   requesters get that connection; nothing else.
                // - The native dial is still pending: its requesters get the
                //   failure; the native dial keeps the in-transit slot, the
                //   address is not marked, and there is no backoff, since the
                //   native dial's own UDP fallback, up to
                //   NATIVE_FALLBACK_AFTER later, still needs the peer and a
                //   30 s backoff would refuse it.
                // - The native dial has ended without a connection: this was
                //   the last dial, so its requesters take the ordinary failure
                //   path below (in-transit release, failed address,
                //   diagnostics, backoff).
                if !native {
                    if let Some(beside) = state.udp_beside_native.remove(&peer_addr) {
                        let connected = self.connections.contains_key(&peer_addr);
                        let native_pending = state.native_pending.contains(&peer_addr);
                        tracing::info!(
                            remote = %peer_addr,
                            transaction = %transaction,
                            ?error,
                            connected,
                            native_pending,
                            "UDP dial beside a native dial failed"
                        );
                        if connected {
                            answer_beside(peer_addr, beside, Ok((peer_addr, None))).await;
                            return Ok(());
                        }
                        if native_pending {
                            answer_beside(peer_addr, beside, Err(None)).await;
                            return Ok(());
                        }
                        for (tx, callback) in beside {
                            state
                                .awaiting_connection
                                .entry(peer_addr)
                                .or_default()
                                .push(callback);
                            state
                                .awaiting_connection_txs
                                .entry(peer_addr)
                                .or_default()
                                .push(tx);
                        }
                    }
                }
                // A native setup can end after the pair connected another way:
                // the server waits longer than the client, and the client's UDP
                // fallback can connect the pair meanwhile. Its failure then
                // must not touch that connection: the in-transit prune below
                // removes an address's ring entry whether or not it is in
                // transit, and nothing waits on this dial any more (the
                // connection answered its callback). A path failure here is a
                // rescue when that connection is UDP, struck above; nothing
                // else to do.
                if native && self.connections.contains_key(&peer_addr) {
                    tracing::debug!(
                        remote = %peer_addr,
                        transaction = %transaction,
                        "QUIC-native setup ended after the peer connected another way; keeping that connection"
                    );
                    #[cfg(any(test, feature = "testing"))]
                    crate::operations::connect::native_wire_trace::record_at(
                        peer_addr,
                        crate::operations::connect::native_wire_trace::NativeWireEvent::NativeSuperseded,
                    );
                    return Ok(());
                }

                tracing::info!(
                    remote = %peer_addr,
                    transient,
                    transaction = %transaction,
                    ?error,
                    "Outbound connection failed"
                );

                // A UDP dial beside this native one still holds the in-transit
                // slot; it releases it when it ends.
                if !(native && state.udp_beside_native.contains_key(&peer_addr)) {
                    self.bridge
                        .op_manager
                        .ring
                        .connection_manager
                        .prune_in_transit_connection(peer_addr);
                }

                // Only record transport-level failures (NAT traversal, timeout, I/O)
                // so future CONNECT requests avoid routing to this peer. Protocol
                // errors or rejections don't indicate the peer is unreachable.
                // A native failure is followed at once by a UDP dial to the same
                // address (the connect op's fallback), whose own outcome decides
                // whether the peer is unreachable: neither this mark nor the
                // backoff below is taken for it, or the fallback would be
                // refused.
                #[cfg(any(test, feature = "testing"))]
                if native {
                    use crate::operations::connect::native_wire_trace::{
                        NativeWireEvent, record_at,
                    };
                    record_at(
                        peer_addr,
                        NativeWireEvent::NativeDial { established: false },
                    );
                    record_at(
                        peer_addr,
                        NativeWireEvent::NativeFailedFor {
                            tx: transaction,
                            own: self
                                .bridge
                                .op_manager
                                .ring
                                .connection_manager
                                .get_own_addr(),
                            peer: peer_addr,
                        },
                    );
                }
                if !native
                    && matches!(
                        error,
                        ConnectionError::TransportError(_)
                            | ConnectionError::Timeout
                            | ConnectionError::IOError(_)
                    )
                {
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
                if !native && !peer_addr.ip().is_unspecified() {
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

                    // A native dial's requesters learn how its setup ended,
                    // so the connect op can tell a path failure from a
                    // local refusal (the rescue definition, 10.2 item 16);
                    // a UDP dial's carry nothing.
                    let failure = if native { native_outcome } else { None };
                    let mut callbacks = callbacks.into_iter();
                    if let Some(mut cb) = callbacks.next() {
                        cb.send_result(Err(failure))
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
                        cb.send_result(Err(failure))
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

    /// A native join handed over by this gateway's transport (#5940, phase
    /// B), with the key the joiner proved in its handshake.
    ///
    /// - A UDP entry for the same key at the address: this one is held
    ///   beside it until it carries a frame (`hold_native`), as for a native
    ///   dial. The UDP survivor rule does not apply to a proven native join.
    /// - Otherwise, including a native entry for the same key (the joiner
    ///   joined again, so the older connection is taken to be stale), it
    ///   replaces any entry at the address with the full cleanup of
    ///   `handle_successful_connection`, and becomes the entry, native,
    ///   bound to the proven key
    ///   without promotion, which is where a legacy join stands once its
    ///   ConnectRequest named it: the connect op promotes it (ConnectPeer),
    ///   and a ConnectRequest naming another key closes it
    ///   (`BindOutcome::Mismatch`). It is counted as a native join, and
    ///   closed if no ConnectRequest arrives within
    ///   [`NATIVE_JOIN_REQUEST_WITHIN`].
    async fn accept_native_join(
        &mut self,
        connection: Box<dyn PeerConnectionApi>,
        key: TransportPublicKey,
        state: &mut EventListenerState,
        handshake_commands: &HandshakeCommandSender,
    ) -> anyhow::Result<()> {
        let remote_addr = connection.remote_addr();
        let udp_entry_for_key = self
            .connections
            .get(&remote_addr)
            .is_some_and(|entry| !entry.native && entry.pub_key() == Some(&key));
        if udp_entry_for_key {
            tracing::info!(
                remote = %remote_addr,
                "Native join from a peer this node holds over UDP; holding it beside"
            );
            return self.hold_native(remote_addr, connection, key).await;
        }
        tracing::info!(remote = %remote_addr, pub_key = %key, "Native join established");
        self.record_native_gateway_join(remote_addr, connection.remote_version());
        if !remote_addr.ip().is_unspecified() {
            state.peer_backoff.record_success(remote_addr);
        }
        self.handle_successful_connection(
            None,
            connection,
            state,
            None,
            false,
            handshake_commands,
            IdentitySource::Proven,
        )
        .await?;
        let outcome = self.bind_identity(remote_addr, &key, IdentitySource::Proven, Some(false));
        let kind = match outcome {
            BindOutcome::Bound => Some(ConnIdentity::SetProven),
            BindOutcome::Held(_) | BindOutcome::HeldByQuiet(_) => Some(ConnIdentity::ClaimHeld),
            BindOutcome::Same | BindOutcome::Mismatch(_) | BindOutcome::NoConnection => None,
        };
        if let Some(kind) = kind {
            TRANSPORT_METRICS.record_conn_identity(remote_addr, kind);
        }
        if let BindOutcome::HeldByQuiet(quiet) = outcome {
            self.replace_quiet_holder(quiet, &key, handshake_commands)
                .await;
        }
        if let Some(entry) = self.connections.get(&remote_addr) {
            watch_native_join(
                entry.sender.downgrade(),
                entry.link_use.clone(),
                remote_addr,
            );
        }
        Ok(())
    }

    /// A gateway counts a native join when a native connection is kept, as
    /// the UDP listener counts a legacy one when its handshake completes.
    fn record_native_gateway_join(
        &self,
        peer_addr: SocketAddr,
        remote_version: Option<(u8, u8, u16)>,
    ) {
        let connection_manager = &self.bridge.op_manager.ring.connection_manager;
        if connection_manager.is_gateway() {
            crate::transport::join_versions::record_gateway_join(
                connection_manager.get_own_addr().unwrap_or(peer_addr),
                crate::transport::join_versions::JoinTransport::Native,
                remote_version,
            );
        }
    }

    /// Starts the task that owns `connection` for the entry at `peer_addr`
    /// whose sender feeds `rx` and whose id is `conn_id`.
    async fn spawn_peer_connection_listener(
        &self,
        rx: PeerConnChannelRecv,
        mut connection: Box<dyn PeerConnectionApi>,
        peer_addr: SocketAddr,
        conn_id: u64,
        link_use: zombie_sweep::LinkUseStamp,
        report_first_frame: bool,
    ) -> anyhow::Result<()> {
        let Some(conn_events) = self.conn_event_tx.as_ref().cloned() else {
            anyhow::bail!("Connection event channel not initialized");
        };

        // Phase 4: Set orphan stream registry on connection for handling race conditions
        // between stream fragments and metadata messages (RequestStreaming/ResponseStreaming).
        connection
            .set_inbound_stream_registry(self.bridge.op_manager.orphan_stream_registry().clone());

        // Use tokio::spawn directly instead of GlobalExecutor::spawn.
        // GlobalExecutor::spawn uses Handle::try_current().spawn() which doesn't
        // reliably poll tasks in certain test contexts (see issue #2709).
        let outbound_mix = self.bridge.op_manager.outbound_mix.clone();
        let propagation = crate::operations::update::propagation_stats::PropagationSink::of(
            &self.bridge.op_manager,
        );
        let native_wire = self.bridge.op_manager.ring.connection_manager.native_wire();
        tokio::spawn(async move {
            peer_connection_listener(
                rx,
                connection,
                peer_addr,
                conn_events,
                conn_id,
                outbound_mix,
                propagation,
                link_use,
                native_wire,
                report_first_frame,
            )
            .await;
        });
        // Yield to allow the spawned peer_connection_listener task to start.
        // This is important because on some runtimes (especially in tests with boxed_local
        // futures), spawned tasks may not be scheduled immediately, causing messages
        // sent to the channel to pile up without being processed.
        tokio::task::yield_now().await;
        Ok(())
    }

    /// The survivor rule: when this node holds, or is handed, a QUIC-native
    /// and a UDP connection to the same peer, it keeps the native one,
    /// whichever arrived first and whichever side dialled the UDP one. Both
    /// ends of a pair apply the rule, so they normally keep the same
    /// connection without exchanging anything. Two UDP connections to one
    /// address keep the older behaviour (the later replaces the earlier).
    ///
    /// Residual: liveness is judged by each end on its own receipt stamp, so
    /// a UDP connection arriving when one end's native connection has just
    /// passed `identity::HOLD_WINDOW` and the other's has not can be kept at
    /// one end and dropped at the other. That needs the native connection to
    /// have been silent for about two keepalive intervals (the window is at
    /// least twice the interval, asserted below), so about two lost
    /// keepalives in a row; it is rare, and the first enable step measures it
    /// by joining both ends' setup outcomes (gate 22).
    ///
    /// The address of the live native connection that holds the peer a UDP
    /// connection at `addr` claims as `key`, if one does: the entry bound to
    /// `key`, or, with no key, the entry at `addr`, when it is native and was
    /// heard from within `identity::HOLD_WINDOW`. A native entry at `addr`
    /// bound to another key is stale (the peer came back under a new key), so
    /// it does not hold the peer. Nor does a native entry gone quiet: the
    /// peer may have restarted, or its NAT rebound, and its native connection
    /// will only end at the QUIC idle timeout. In both cases the UDP
    /// connection takes the ordinary path, which replaces the stale entry.
    fn native_holds_peer(
        &self,
        addr: SocketAddr,
        key: Option<&TransportPublicKey>,
    ) -> Option<SocketAddr> {
        let at = match key {
            Some(key) => *self.addr_by_pub_key.get(key)?,
            None => addr,
        };
        self.connections
            .get(&at)
            .is_some_and(|entry| {
                entry.native
                    && key.is_none_or(|k| entry.pub_key() == Some(k))
                    && identity::heard_from_recently(entry)
            })
            .then_some(at)
    }

    /// Whether the UDP connection that `native_holds_peer` found a holder
    /// for at `addr` should be kept as the entry, with the native one held
    /// beside it (option (b)): the holder is the entry at the UDP
    /// connection's own address, it is native, bound to a key, has not
    /// carried a frame from the peer, and nothing is held there yet. A
    /// holder at another address keeps the earlier behaviour (the UDP
    /// connection is dropped), as does one that has already carried a frame.
    fn native_entry_awaits_frame(&self, holder: SocketAddr, addr: SocketAddr) -> bool {
        holder == addr
            && !self.held_native.contains_key(&holder)
            && self
                .connections
                .get(&holder)
                .is_some_and(|entry| entry.native && !entry.frame_seen && entry.pub_key().is_some())
    }

    /// Option (b) of the survivor rule: a native connection is kept beside
    /// a UDP entry for the same peer until it carries a frame from the peer,
    /// because only one end may have finished the native setup (the other's
    /// Hello timed out), and an end that dropped its UDP connection for a
    /// native one the peer never held is left with nothing. Both ends send a
    /// probe on every native connection when it starts
    /// ([`send_native_probe`]), so a pair that both hold a native connection
    /// proves it to each other within a round trip; [`Self::promote_held_native`]
    /// then makes it the entry and ends the UDP connection, as the survivor
    /// rule's swap always did. If no frame comes within
    /// `HELD_NATIVE_WINDOW`, or the native connection closes, the UDP entry
    /// stays and the native connection is dropped ([`Self::expire_held_native`]).
    ///
    /// Keeps `connection` as the held native connection for the entry at
    /// `peer_addr`, bound to `key`, with its listener running. Callers check
    /// the entry exists and is UDP under `key`. A second native connection
    /// for an address already holding one replaces it (the newer is kept).
    async fn hold_native(
        &mut self,
        peer_addr: SocketAddr,
        connection: Box<dyn PeerConnectionApi>,
        key: TransportPublicKey,
    ) -> anyhow::Result<()> {
        let Some(entry) = self.connections.get(&peer_addr) else {
            return Ok(());
        };
        // A second native connection for an address that already holds one
        // replaces it: a re-dial means the older one is likely stale. Dropping
        // the old `HeldNative` drops its sender, which closes its connection;
        // its later `TransportClosed` carries the old id and is ignored.
        let link_use = entry.link_use.clone();
        let connection_id = next_connection_id();
        let (tx, rx) = mpsc::channel(10);
        send_native_probe(&tx, peer_addr);
        self.held_native.insert(
            peer_addr,
            HeldNative {
                sender: tx,
                connection_id,
                receipt: connection.receipt_stamp(),
                remote_version: connection.remote_version(),
                key,
                since: Instant::now(),
            },
        );
        tracing::info!(
            %peer_addr,
            connection_id,
            "Native connection kept beside the UDP one until it carries a frame from the peer"
        );
        #[cfg(any(test, feature = "testing"))]
        crate::operations::connect::native_wire_trace::record_at(
            peer_addr,
            crate::operations::connect::native_wire_trace::NativeWireEvent::NativeHeld,
        );
        self.spawn_peer_connection_listener(
            rx,
            connection,
            peer_addr,
            connection_id,
            link_use,
            true,
        )
        .await
    }

    /// Option (b), the other order: a UDP connection arrived for a peer
    /// whose native entry has not yet carried a frame from it. The UDP
    /// connection becomes the entry, in place, and the native connection
    /// becomes the held one (listener untouched, its sender moves to
    /// `held_native`). Callers check that the entry is a native one bound to
    /// a key and that nothing is held at the address. Everything the
    /// `ConnectionManager` and ring hold for the address is kept, as in
    /// [`Self::promote_held_native`].
    async fn swap_udp_in_beside_native(
        &mut self,
        peer_addr: SocketAddr,
        connection: Box<dyn PeerConnectionApi>,
    ) -> anyhow::Result<()> {
        let conn_id = next_connection_id();
        let remote_version = connection.remote_version();
        let receipt = connection.receipt_stamp();
        let (tx, rx) = mpsc::channel(10);
        let Some(entry) = self.connections.get_mut(&peer_addr) else {
            debug_assert!(false, "swap_udp_in_beside_native without an entry");
            return Ok(());
        };
        let Some(key) = entry.pub_key().cloned() else {
            debug_assert!(false, "swap_udp_in_beside_native without a bound key");
            return Ok(());
        };
        let held = HeldNative {
            sender: std::mem::replace(&mut entry.sender, tx),
            connection_id: std::mem::replace(&mut entry.connection_id, conn_id),
            receipt: std::mem::replace(&mut entry.receipt, receipt),
            remote_version: std::mem::replace(&mut entry.remote_version, remote_version),
            key,
            since: Instant::now(),
        };
        entry.native = false;
        entry.frame_seen = true;
        let link_use = entry.link_use.clone();
        self.bridge
            .op_manager
            .ring
            .connection_manager
            .record_remote_version(peer_addr, remote_version);
        tracing::info!(
            %peer_addr,
            held_connection_id = held.connection_id,
            connection_id = conn_id,
            "UDP connection kept as the entry: the native one has not carried a frame yet"
        );
        self.held_native.insert(peer_addr, held);
        #[cfg(any(test, feature = "testing"))]
        {
            crate::operations::connect::native_wire_trace::record_at(
                peer_addr,
                crate::operations::connect::native_wire_trace::NativeWireEvent::NativeHeld,
            );
            self.trace_transport(peer_addr, Some(false));
        }
        self.spawn_peer_connection_listener(rx, connection, peer_addr, conn_id, link_use, false)
            .await
    }

    /// The held native connection at `peer_addr` carried a frame from the
    /// peer: it takes over the UDP entry's transport in place, which ends
    /// the UDP connection (its listener ends as its sender drops, and its
    /// `TransportClosed` carries the old connection id, so it is ignored).
    ///
    /// Replaced: the sender, the connection id, the receipt stamp, the remote
    /// version (on the entry and on the `ConnectionManager`) and `native`.
    /// Kept: the identity binding, `created_at`, the link-use stamp, and
    /// everything the `ConnectionManager` and ring hold for the address:
    /// ring membership, transient registration, ReadyState and the health
    /// record, so there is no promotion, prune or transient budget call.
    /// Does nothing (and drops the held connection) when the entry is gone,
    /// is not UDP, or is bound to another key.
    fn promote_held_native(&mut self, peer_addr: SocketAddr) {
        let Some(held) = self.held_native.remove(&peer_addr) else {
            return;
        };
        let Some(entry) = self.connections.get_mut(&peer_addr) else {
            return;
        };
        if entry.native || entry.pub_key() != Some(&held.key) {
            return;
        }
        let old_id = entry.connection_id;
        let old_sender = std::mem::replace(&mut entry.sender, held.sender);
        entry.connection_id = held.connection_id;
        entry.receipt = held.receipt;
        entry.remote_version = held.remote_version;
        entry.native = true;
        entry.frame_seen = true;
        drop(old_sender);
        self.bridge
            .op_manager
            .ring
            .connection_manager
            .record_remote_version(peer_addr, held.remote_version);
        tracing::info!(
            %peer_addr,
            old_connection_id = old_id,
            connection_id = held.connection_id,
            "Native connection carried a frame from the peer; swapped in for the UDP one"
        );
        TRANSPORT_METRICS.record_connect_native_survivor(true);
        #[cfg(any(test, feature = "testing"))]
        {
            crate::operations::connect::native_wire_trace::record_at(
                peer_addr,
                crate::operations::connect::native_wire_trace::NativeWireEvent::Survivor {
                    swapped: true,
                    inbound: false,
                },
            );
            self.trace_transport(peer_addr, Some(true));
        }
        self.record_native_gateway_join(peer_addr, held.remote_version);
        // The swapped-in transport has not been told the key the ring holds
        // it under: the promotion's SetPeerIdentity went to the connection
        // it replaced, and a ConnectPeer for a peer already in the ring
        // sends none. A gateway's native joiner promotes its joiner charge
        // on it (`NativePeerConnection::set_peer_identity`); any other native
        // connection already holds this key and only counts it. Either
        // order of arrival ends here (`swap_udp_in_beside_native`).
        if self
            .bridge
            .op_manager
            .ring
            .connection_manager
            .is_in_ring(peer_addr)
        {
            self.tell_transport_peer_identity(peer_addr, &held.key);
        }
    }

    /// Drops every held native connection that has waited
    /// `HELD_NATIVE_WINDOW` for a frame, and every one whose UDP entry is
    /// gone, replaced, or no longer UDP under the same key. Dropping the
    /// sender closes the connection. Cheap when nothing is held; the event
    /// loop runs it on every iteration.
    /// When the earliest held native connection's window ends, for the event
    /// loop's wake-up, so expiry runs on a quiet node.
    pub(super) fn held_native_deadline(&self) -> Option<Instant> {
        self.held_native
            .values()
            .map(|held| held.since + HELD_NATIVE_WINDOW)
            .min()
    }

    pub(super) fn expire_held_native(&mut self, now: Instant) {
        if self.held_native.is_empty() {
            return;
        }
        let connections = &self.connections;
        self.held_native.retain(|addr, held| {
            let alive = connections
                .get(addr)
                .is_some_and(|entry| !entry.native && entry.pub_key() == Some(&held.key));
            let waiting = now.saturating_duration_since(held.since) < HELD_NATIVE_WINDOW;
            if alive && waiting {
                return true;
            }
            tracing::info!(
                peer_addr = %addr,
                connection_id = held.connection_id,
                entry_alive = alive,
                "Dropping the held native connection: {}",
                if alive { "no frame from the peer in time, keeping UDP" } else { "its UDP entry is gone or changed" }
            );
            #[cfg(any(test, feature = "testing"))]
            crate::operations::connect::native_wire_trace::record_at(
                *addr,
                crate::operations::connect::native_wire_trace::NativeWireEvent::NativeHoldExpired,
            );
            false
        });
    }

    /// A first-frame report or a close for a connection that is held beside
    /// an entry (or is the entry): see [`HeldNative`].
    fn on_native_first_frame(&mut self, remote_addr: SocketAddr, connection_id: u64) {
        // A frame that arrives after the hold's deadline does not promote:
        // the hold is dropped first, as the sweep would have.
        self.expire_held_native(Instant::now());
        if let Some(entry) = self.connections.get_mut(&remote_addr) {
            if entry.connection_id == connection_id {
                entry.frame_seen = true;
                return;
            }
        }
        if self
            .held_native
            .get(&remote_addr)
            .is_some_and(|held| held.connection_id == connection_id)
        {
            self.promote_held_native(remote_addr);
        }
    }

    /// Records, in the simulation's native wire trace, that a live native
    /// entry for `peer_addr` was replaced by a UDP connection.
    #[cfg(any(test, feature = "testing"))]
    fn trace_native_replaced_by_udp(&self, peer_addr: SocketAddr) {
        let own = self
            .bridge
            .op_manager
            .ring
            .connection_manager
            .get_own_addr()
            .unwrap_or_else(|| SocketAddr::new(self.listening_ip, self.listening_port));
        crate::operations::connect::native_wire_trace::record_at(
            peer_addr,
            crate::operations::connect::native_wire_trace::NativeWireEvent::NativeReplacedByUdp {
                own,
            },
        );
    }

    /// Records, in the simulation's native wire trace, which transport this
    /// node now holds for `peer_addr` (`None`: none), so a test can compare
    /// the two ends of each pair.
    #[cfg(any(test, feature = "testing"))]
    pub(super) fn trace_transport(&self, peer_addr: SocketAddr, native: Option<bool>) {
        let own = self
            .bridge
            .op_manager
            .ring
            .connection_manager
            .get_own_addr()
            .unwrap_or_else(|| SocketAddr::new(self.listening_ip, self.listening_port));
        crate::operations::connect::native_wire_trace::record_at(
            peer_addr,
            crate::operations::connect::native_wire_trace::NativeWireEvent::Transport {
                own,
                native,
            },
        );
    }

    pub(super) async fn handle_handshake_stream_closed(
        &mut self,
        state: &mut EventListenerState,
    ) -> anyhow::Result<()> {
        state.native_pending.clear();
        state.beside_udp_connected.clear();
        if state.awaiting_connection.is_empty() && state.udp_beside_native.is_empty() {
            return Ok(());
        }

        tracing::warn!(
            awaiting = state.awaiting_connection.len(),
            beside = state.udp_beside_native.len(),
            "Handshake driver closed; notifying pending callbacks"
        );

        let awaiting = std::mem::take(&mut state.awaiting_connection);
        let awaiting_txs = std::mem::take(&mut state.awaiting_connection_txs);
        for (addr, beside) in std::mem::take(&mut state.udp_beside_native) {
            for (_, mut callback) in beside {
                callback
                    .send_result(Err(None))
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

        for (addr, callbacks) in awaiting {
            let pending_txs = awaiting_txs.get(&addr).cloned().unwrap_or_default();
            tracing::debug!(
                remote = %addr,
                callbacks = callbacks.len(),
                pending_txs = ?pending_txs,
                "Delivering handshake driver shutdown notification"
            );
            for mut cb in callbacks {
                cb.send_result(Err(None))
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
                    let _ = cb.send_result(Err(None)).await;
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
        let removed = self.remove_connection(peer_addr);
        // A live native entry replaced by a UDP connection is what the
        // survivor rule forbids; the simulation's trace checks it. Recorded
        // here, before the insert below, since the removal has already
        // cleared the trace's entry.
        #[cfg(any(test, feature = "testing"))]
        if let Some(old) = &removed {
            if old.native
                && !matches!(source, IdentitySource::Proven)
                && identity::heard_from_recently(old)
            {
                self.trace_native_replaced_by_udp(peer_addr);
            }
        }
        if let Some(old) = removed {
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

        // The slot taken at the top of this function is gone if the
        // replacement above released the old connection's slot at this
        // address; take it again. Registering an address already tracked
        // succeeds without taking a second slot.
        if is_transient {
            let cm = &self.bridge.op_manager.ring.connection_manager;
            if !cm.try_register_transient(peer_addr, None) {
                tracing::warn!(
                    remote = %peer_addr,
                    budget = cm.transient_budget(),
                    current = cm.transient_count(),
                    "Transient connection budget exhausted; dropping inbound connection before insert"
                );
                // As the refusal at the top: whoever waits for this
                // connection is told it failed.
                if let Some(callbacks) = state.awaiting_connection.remove(&peer_addr) {
                    for mut cb in callbacks {
                        // Best effort - caller will handle the connection failure
                        #[allow(clippy::let_underscore_must_use)]
                        let _ = cb.send_result(Err(None)).await;
                    }
                }
                state.awaiting_connection_txs.remove(&peer_addr);
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
        self.insert_connection(
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
                // Only a QUIC-native connection's key is proven in its
                // handshake, so the source says which transport this is.
                native: matches!(source, IdentitySource::Proven),
                // A UDP connection's handshake was a round trip; a native
                // connection has to carry a frame first.
                frame_seen: !matches!(source, IdentitySource::Proven),
            },
        );
        #[cfg(any(test, feature = "testing"))]
        self.trace_transport(peer_addr, Some(matches!(source, IdentitySource::Proven)));
        // Mirror the same version onto `ConnectionManager` so op drivers
        // (reached via `op_manager.ring.connection_manager`, not this
        // network-bridge layer) can gate emission of version-sensitive wire
        // variants — e.g. the summary-first PUT probe's
        // `supports_summary_first_put` check. Keep this write paired with
        // the `self.insert_connection` above if that call site changes.
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
                            IdentitySource::Proven => ConnIdentity::SetProven,
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
        let native = matches!(source, IdentitySource::Proven);
        self.spawn_peer_connection_listener(rx, connection, peer_addr, conn_id, link_use, native)
            .await?;
        if native {
            if let Some(entry) = self.connections.get(&peer_addr) {
                send_native_probe(&entry.sender, peer_addr);
            }
        }
        let connection_manager = &self.bridge.op_manager.ring.connection_manager;
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
                // The ring holds no entry for the address here: the reservation
                // prune above removes one, so an add is a new entry.
                if outcome.added {
                    self.tell_transport_peer_identity(peer_addr, peer.pub_key());
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
                    let cm = connection_manager.clone();
                    let mut registered = cm.register_transient(peer_addr, Some(loc));
                    let mut promoted_meanwhile = false;
                    if registered == Err(crate::ring::TransientRefusal::Budget) {
                        // The budget is spent by sources each within their
                        // share. Close an unpromoted connection (the oldest
                        // of the source holding the most slots, see
                        // `oldest_held_transient`) and admit this one, so
                        // whoever fills the budget has to keep opening
                        // connections to keep it full, rather than holding
                        // it and refusing every joiner.
                        // Only a UDP entry with a native connection held
                        // beside it (`held_native`) is spared: the hold lasts
                        // `HELD_NATIVE_WINDOW`, so the exemption is bounded
                        // in time and closing the entry would end a path the
                        // two ends are still settling. A QUIC-native entry is
                        // evictable like a UDP one: it costs a requester the
                        // same, a native leg that never promotes is closed
                        // by nothing but the zombie sweep or the idle
                        // timeout, and exempting it (or only the ones that
                        // have not carried a frame, which the first-frame
                        // probe makes every live leg within a round trip)
                        // would let a few sources fill the pool with legs
                        // that cannot be evicted. A UDP session cannot have
                        // a QUIC path while it is unpromoted (the upgrade
                        // starts only at promotion, which takes the
                        // connection out of this pool).
                        let victim =
                            cm.oldest_held_transient(|addr| self.held_native.contains_key(&addr));
                        if let Some(victim) = victim {
                            cm.record_transient_eviction(victim);
                            self.drop_connection_by_addr(
                                victim,
                                handshake_commands,
                                crate::tracing::DisconnectReason::ConnectionLimitReached,
                            )
                            .await;
                            fail_awaiting_connection_callbacks(state, victim).await;
                            // Closing the victim can release a key a pending
                            // claim on this connection names, which binds the
                            // claim and promotes this connection into the
                            // ring (`release_identity`). Then it is not
                            // transient and must not take a slot.
                            promoted_meanwhile = cm.has_connection_or_pending(peer_addr);
                            if !promoted_meanwhile {
                                registered = cm.register_transient(peer_addr, Some(loc));
                            }
                        }
                    }
                    let connection_manager = &cm;
                    if promoted_meanwhile {
                        tracing::debug!(
                            %peer_addr,
                            "Promoted while the transient budget evicted; not registered as transient"
                        );
                    } else if let Err(refusal) = registered {
                        // The budget bounds the connections held outside the
                        // ring. An inbound connection nobody expected arrives
                        // with `is_transient == false`, so the check at the
                        // top of this function did not count it and this is
                        // the first place it is counted. Kept after a refusal,
                        // it would be an open connection the budget does not
                        // count, so it is closed, as at the top. A refusal for
                        // the source's share is counted and logged by the
                        // connection manager.
                        if refusal == crate::ring::TransientRefusal::Budget {
                            tracing::warn!(
                                remote = %peer_addr,
                                budget = connection_manager.transient_budget(),
                                current = connection_manager.transient_count(),
                                "Transient connection budget exhausted; dropping inbound connection"
                            );
                        }
                        self.drop_connection_by_addr(
                            peer_addr,
                            handshake_commands,
                            crate::tracing::DisconnectReason::ConnectionLimitReached,
                        )
                        .await;
                        fail_awaiting_connection_callbacks(state, peer_addr).await;
                    } else {
                        // The connection stays open outside the ring, so it
                        // keeps its slot until it is promoted or closed
                        // (`Ring::prune_connection`), not for `transient_ttl`:
                        // a slot given back while the transport stays open
                        // would let the budget be filled again every TTL.
                        // The held entry is exempt from age expiry and
                        // bounded only by the connection closing (zombie
                        // sweep or transport idle timeout), which is not
                        // guaranteed for a connection kept open indefinitely
                        // (#5922); eviction at the budget limits how many
                        // slots such entries can take.
                        connection_manager.hold_transient(peer_addr);
                        if !already_transient {
                            crate::node::network_status::record_bootstrap_transient_registered();
                        }
                        tracing::info!(
                            peer_id = ?peer_id,
                            %peer_addr,
                            "Registered transient connection (not added to ring topology)"
                        );
                        let ttl = connection_manager.transient_ttl();
                        let cm = connection_manager.clone();
                        // Keyed to this entry, so a later connection or a
                        // new registration at the address is not counted.
                        let generation = cm.transient_generation(peer_addr);
                        GlobalExecutor::spawn(async move {
                            sleep(ttl).await;
                            // #4787's series keeps its meaning: a
                            // registration whose TTL elapsed before it was
                            // promoted, whether the connection is still open
                            // or has closed.
                            match cm.transient_ttl_outcome(peer_addr, generation) {
                                crate::ring::TransientTtlOutcome::Promoted => {}
                                crate::ring::TransientTtlOutcome::Closed => {
                                    crate::node::network_status::record_bootstrap_transient_expired(
                                    );
                                }
                                crate::ring::TransientTtlOutcome::StillHeld => {
                                    crate::node::network_status::record_bootstrap_transient_expired(
                                    );
                                    // Do NOT dispatch DropConnection: that
                                    // tears down the transport, killing any
                                    // in-progress CONNECT. If the CONNECT
                                    // succeeds later, handle_connect_peer()
                                    // promotes the peer (#3113). Otherwise
                                    // the slot is given back only when the
                                    // connection closes (zombie sweep or
                                    // transport idle timeout), which is not
                                    // guaranteed for one kept open
                                    // indefinitely (#5922).
                                    tracing::info!(
                                        %peer_addr,
                                        "Transient connection past its TTL unpromoted \
                                         (transport preserved, budget slot held until promoted or closed)"
                                    );
                                }
                            }
                        });
                    }
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
                // A held native connection that closed is just forgotten: the
                // UDP entry stays.
                if self
                    .held_native
                    .get(&remote_addr)
                    .is_some_and(|held| held.connection_id == connection_id)
                {
                    self.held_native.remove(&remote_addr);
                    tracing::info!(
                        remote = %remote_addr,
                        connection_id,
                        "Held native connection closed before carrying a frame; keeping UDP"
                    );
                    return Ok(EventResult::Continue);
                }
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
                // The UDP entry closed while a native connection is held beside
                // it for the same key: the peer may have promoted that native
                // connection and dropped UDP (the two can arrive in either
                // order), so it becomes the entry, as the connection the peer
                // chose, rather than the peer being torn down. If it is dead
                // too, its own close removes the entry as usual.
                if self.held_native.get(&remote_addr).is_some_and(|held| {
                    self.connections
                        .get(&remote_addr)
                        .is_some_and(|e| !e.native && e.pub_key() == Some(&held.key))
                }) {
                    self.promote_held_native(remote_addr);
                    if let Some(entry) = self.connections.get_mut(&remote_addr) {
                        // No frame has been seen on it.
                        entry.frame_seen = false;
                    }
                    return Ok(EventResult::Continue);
                }
                // Look up the connection directly by address
                if let Some(entry) = self.remove_connection(remote_addr) {
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
            Some(ConnEvent::NativeFirstFrame {
                remote_addr,
                connection_id,
            }) => {
                self.on_native_first_frame(remote_addr, connection_id);
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

/// Tells whoever still waits for a connection to `addr` that it failed, for a
/// connection the transient budget closes: as the refusal at the top of
/// `handle_successful_connection`, so no waiter is left until its timeout.
async fn fail_awaiting_connection_callbacks(state: &mut EventListenerState, addr: SocketAddr) {
    if let Some(callbacks) = state.awaiting_connection.remove(&addr) {
        for mut cb in callbacks {
            // Best effort - caller will handle the connection failure
            #[allow(clippy::let_underscore_must_use)]
            let _ = cb.send_result(Err(None)).await;
        }
    }
    state.awaiting_connection_txs.remove(&addr);
    // A UDP dial queued beside a native one for this address is a waiter
    // too: the budget has closed the address it was to connect. An inbound
    // native eviction thus fails our pending outbound's UDP waiters, as it
    // already does for `awaiting_connection`.
    fail_udp_beside_native(state, addr).await;
}

/// How long a gateway's native joiner has, from the hand-over, to send its
/// ConnectRequest (#5940, phase B). A joiner sends it as soon as its
/// connection is up, so this only closes a connection that is not used for
/// a join.
///
/// A one-shot timer per join ([`watch_native_join`]) enforces it, not the
/// zombie sweep: the sweep runs on the 30 s stats tick (backlog slices can be
/// spaced up to 4x a 6.4 s slice), so through it the bound would be 30 s or
/// more. The timer closes such a join 5 s after the hand-over, which follows
/// the joiner's Hello at once; the sweep stays the backstop for a join that
/// sent a request and was never promoted, as for a legacy join.
const NATIVE_JOIN_REQUEST_WITHIN: Duration = Duration::from_secs(5);

/// Close the native join at `peer_addr` unless its peer sends a
/// ConnectRequest (`zombie_sweep::LinkUseStamp::connect_requested`; any
/// other request does not count) within
/// [`NATIVE_JOIN_REQUEST_WITHIN`]. The drop notice goes to that
/// connection's own task, so a later connection at the address is not
/// touched. The task ends after one wait. It holds the channel weakly, so a
/// connection replaced or dropped in the meantime closes at once rather than
/// at the deadline, and is not counted.
fn watch_native_join(
    sender: tokio::sync::mpsc::WeakSender<Either<NetMessage, ConnEvent>>,
    link_use: zombie_sweep::LinkUseStamp,
    peer_addr: SocketAddr,
) {
    drop(GlobalExecutor::spawn(async move {
        sleep(NATIVE_JOIN_REQUEST_WITHIN).await;
        if link_use.connect_requested() {
            return;
        }
        let Some(sender) = sender.upgrade().filter(|sender| !sender.is_closed()) else {
            return;
        };
        tracing::debug!(%peer_addr, "Native join sent no ConnectRequest in time; closing it");
        TRANSPORT_METRICS.record_gateway_admission(
            crate::transport::metrics::GatewayAdmission::NoConnectRequest,
        );
        notify_connection_dropped(
            sender,
            peer_addr,
            crate::tracing::DisconnectReason::Explicit("native_join_no_connect_request".into()),
        );
    }));
}

/// Sends the first-frame probe on a native connection's channel: a message
/// the peer's listener decodes and the event loop ignores (`Aborted` of a
/// transaction nothing owns), so each end's native connection carries a frame
/// whatever else is or is not sent. `try_send` on a channel this call has
/// just made (or a fresh entry's) cannot find it full; if it somehow did, the
/// peer's hold window expires and UDP stays, which is the safe outcome.
fn send_native_probe(sender: &PeerConnChannelSender, peer_addr: SocketAddr) {
    #[cfg(any(test, feature = "testing"))]
    if crate::operations::connect::native_wire_trace::native_probe_suppressed(peer_addr) {
        return;
    }
    let probe = NetMessage::V1(NetMessageV1::Aborted(crate::message::Transaction::new::<
        crate::operations::connect::ConnectMsg,
    >()));
    if sender.try_send(Left(probe)).is_err() {
        tracing::debug!(%peer_addr, "Could not queue the native first-frame probe");
    }
}

/// Tell the per-connection task of a dropped connection to close, without
/// blocking the caller, which runs on the event loop: a full per-peer
/// channel must not stall it (`.claude/rules/channel-safety.md`). See
/// [`notify_connection_task`].
fn notify_connection_dropped(
    sender: PeerConnChannelSender,
    peer_addr: SocketAddr,
    reason: crate::tracing::DisconnectReason,
) {
    let event = Right(ConnEvent::NodeAction(NodeEvent::DropConnection(
        peer_addr, reason,
    )));
    notify_connection_task(sender, event, peer_addr, "drop notice", None);
}

/// What [`notify_connection_task`] counts for an event (on the event's
/// remote address): handed to the connection's task, or not handed over
/// within the fallback's timeout.
#[derive(Clone, Copy)]
struct DeliveryCounts {
    delivered: ConnIdentity,
    timed_out: ConnIdentity,
}

/// Send `event` (`what`, for logs) to a connection's task without blocking
/// the caller, which runs on the event loop: when the channel is full, the
/// send is moved to a short task that waits up to 1 s. With `counts`, the
/// outcome is counted on `peer_addr`; a connection task already gone counts
/// nothing.
fn notify_connection_task(
    sender: PeerConnChannelSender,
    event: Either<NetMessage, ConnEvent>,
    peer_addr: SocketAddr,
    what: &'static str,
    counts: Option<DeliveryCounts>,
) {
    let count = move |kind: fn(DeliveryCounts) -> ConnIdentity| {
        if let Some(counts) = counts {
            TRANSPORT_METRICS.record_conn_identity(peer_addr, kind(counts));
        }
    };
    match sender.try_send(event) {
        Ok(()) => count(|c| c.delivered),
        Err(tokio::sync::mpsc::error::TrySendError::Full(event)) => {
            GlobalExecutor::spawn(async move {
                match timeout(Duration::from_secs(1), sender.send(event)).await {
                    Ok(Ok(())) => count(|c| c.delivered),
                    Ok(Err(_)) => tracing::debug!(
                        %peer_addr,
                        what,
                        "Connection task gone before the notice was delivered"
                    ),
                    Err(_) => {
                        count(|c| c.timed_out);
                        tracing::warn!(
                            %peer_addr,
                            what,
                            "Timeout while sending a notice to the connection task"
                        )
                    }
                }
            });
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
            tracing::debug!(%peer_addr, what, "Connection task gone before the notice");
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

/// Fails every requester of the UDP dial beside a native one to `peer_addr`.
async fn fail_udp_beside_native(state: &mut EventListenerState, peer_addr: SocketAddr) {
    let requesters = state
        .udp_beside_native
        .remove(&peer_addr)
        .unwrap_or_default();
    answer_beside(peer_addr, requesters, Err(None)).await;
}

/// Answers requesters of a UDP dial beside a native one with `result`.
async fn answer_beside(
    peer_addr: SocketAddr,
    requesters: Vec<(Transaction, Box<dyn ConnectResultSender>)>,
    result: crate::message::ConnectResult,
) {
    for (tx, mut callback) in requesters {
        callback
            .send_result(result)
            .await
            .inspect_err(|err| {
                tracing::debug!(
                    %tx,
                    remote = %peer_addr,
                    ?err,
                    "Failed to answer a UDP dial beside a native one"
                );
            })
            .ok();
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
            .rfind("letorigin=dial_origin(origin,transient,peer_addr,&self.gateways);")
            .expect("handle_connect_peer must apply dial_origin");
        let sent = code
            .find("HandshakeCommand::Connect{peer:peer.clone(),transaction:tx,transient,origin,transport,gateway_join,}")
            .expect("the handshake command carries the origin");
        assert!(applied < sent, "applied before it is sent");
        // Only the joiner's own join dial may try native first: the flag is
        // read from the caller's origin BEFORE `dial_origin` widens it to
        // `Gateway` for any dial to a gateway's address, and the UDP dial
        // beside a pending native one is never a join.
        let flag = code
            .find("letgateway_join=origin==crate::transport::NatAttemptOrigin::Gateway;")
            .expect("the join flag comes from the caller's origin");
        assert!(flag < applied, "read before the origin is widened");
        assert_eq!(
            code.matches("gateway_join,}").count(),
            1,
            "the handshake command carries the flag"
        );
        assert_eq!(
            code.matches("gateway_join:false,").count(),
            1,
            "the UDP dial beside a native one is not a join"
        );
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
