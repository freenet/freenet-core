//! Connection lifecycle for [`P2pConnManager`]: teardown of existing
//! connections plus the handshake / new-connection setup path.
//!
//! Behavior-preserving extraction from `p2p_protoc.rs`.

use super::*;

impl P2pConnManager {
    /// Drop a single connection by socket address: notifies the handshake handler,
    /// prunes topology, handles orphaned transactions, cleans up subscriptions,
    /// and forwards a DropConnection event to the per-peer listener.
    ///
    /// Returns `true` if the connection existed and was dropped.
    pub(super) async fn drop_connection_by_addr(
        &mut self,
        peer_addr: SocketAddr,
        handshake_cmd_sender: &HandshakeCommandSender,
    ) -> bool {
        let Some(entry) = self.connections.get(&peer_addr) else {
            return false;
        };

        let peer = if let Some(ref pub_key) = entry.pub_key {
            PeerKeyLocation::new(pub_key.clone(), peer_addr)
        } else {
            PeerKeyLocation::new(
                (*self.bridge.op_manager.ring.connection_manager.pub_key).clone(),
                peer_addr,
            )
        };
        let pub_key_to_remove = entry.pub_key.clone();

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
        // ExpectedInboundTracker entry (no TTL sweep, but overwritten on the
        // next register() and consumed on a matching inbound — bounded, benign).
        if !handshake_cmd_sender.try_send(HandshakeCommand::DropConnection { peer: peer.clone() }) {
            tracing::warn!(
                peer = %peer,
                phase = "disconnect",
                "Handshake command channel full/closed; skipping redundant drop notification"
            );
        }

        // Immediately prune topology counters so we don't leak open connection slots.
        let prune_result = self
            .bridge
            .op_manager
            .ring
            .prune_connection(PeerId::new(peer.pub_key().clone(), peer_addr))
            .await;

        // Handle orphaned transactions immediately (retry via alternate routes).
        self.bridge
            .handle_orphaned_transactions(prune_result.orphaned_transactions, peer_addr)
            .await;

        // Broadcast unready if we dropped below the readiness threshold
        if prune_result.became_unready {
            self.handle_broadcast_ready_state(false).await;
        }

        // Forget this peer's subscriptions and cached contract state
        self.bridge
            .op_manager
            .on_ring_connection_lost(peer.pub_key());

        if let Some(conn) = self.connections.remove(&peer_addr) {
            if let Some(pub_key) = pub_key_to_remove {
                self.addr_by_pub_key.remove(&pub_key);
            }
            // TODO: review: this could potentially leave garbage tasks in the background with peer listener
            match timeout(
                Duration::from_secs(1),
                conn.sender
                    .send(Right(ConnEvent::NodeAction(NodeEvent::DropConnection(
                        peer_addr,
                    )))),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(send_error)) => {
                    tracing::error!(
                        peer_addr = %peer_addr,
                        error = ?send_error,
                        phase = "error",
                        "Failed to send drop connection message to peer"
                    );
                }
                Err(elapsed) => {
                    tracing::error!(
                        peer_addr = %peer_addr,
                        error = ?elapsed,
                        phase = "timeout",
                        "Timeout while sending drop connection message"
                    );
                }
            }
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

        let peer = if let Some(ref pub_key) = entry.pub_key {
            PeerKeyLocation::new(pub_key.clone(), peer_addr)
        } else {
            PeerKeyLocation::new(
                (*self.bridge.op_manager.ring.connection_manager.pub_key).clone(),
                peer_addr,
            )
        };
        let pub_key_to_remove = entry.pub_key.clone();

        // Non-blocking send to handshake driver to avoid deadlock (#3519).
        // For zombie transports (already established connections), the expected_inbound
        // entry was already consumed when the inbound connection arrived, so the
        // DropConnection command is effectively a no-op.
        if !handshake_cmd_sender.try_send(HandshakeCommand::DropConnection { peer: peer.clone() }) {
            tracing::debug!(
                peer = %peer,
                "Handshake channel full during zombie cleanup, skipping handshake notification"
            );
        }

        // Prune from ring topology (non-blocking — only takes write locks, no cross-task channels)
        let prune_result = self
            .bridge
            .op_manager
            .ring
            .prune_connection(PeerId::new(peer.pub_key().clone(), peer_addr))
            .await;

        // Handle orphaned transactions
        self.bridge
            .handle_orphaned_transactions(prune_result.orphaned_transactions, peer_addr)
            .await;

        if prune_result.became_unready {
            self.handle_broadcast_ready_state(false).await;
        }

        // Forget subscriptions and cached contract state
        self.bridge
            .op_manager
            .on_ring_connection_lost(peer.pub_key());

        // Remove from transport tracking and notify the per-connection task
        if let Some(conn) = self.connections.remove(&peer_addr) {
            if let Some(pub_key) = pub_key_to_remove {
                self.addr_by_pub_key.remove(&pub_key);
            }
            // Short timeout — zombie connections may already be dead
            match timeout(
                Duration::from_millis(100),
                conn.sender
                    .send(Right(ConnEvent::NodeAction(NodeEvent::DropConnection(
                        peer_addr,
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

    pub(super) async fn handle_connect_peer(
        &mut self,
        peer: PeerKeyLocation,
        mut callback: Box<dyn ConnectResultSender>,
        tx: Transaction,
        handshake_commands: &HandshakeCommandSender,
        state: &mut EventListenerState,
        transient: bool,
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
            let connection_manager = &self.bridge.op_manager.ring.connection_manager;
            let was_transient = connection_manager.drop_transient(peer_addr);

            // Promote if: (a) was tracked as transient, OR (b) not already in ring.
            // Case (b) handles expired transient TTL — transport preserved per 76058cf4
            // but tracking entry gone. Without this, CONNECT succeeds but peer is never
            // added to ring topology (#3113).
            let needs_promotion =
                was_transient.is_some() || !connection_manager.is_in_ring(peer_addr);
            let transient_expired = was_transient.is_none() && needs_promotion;

            if needs_promotion {
                let loc = was_transient
                    .and_then(|e| e.location)
                    .unwrap_or_else(|| Location::from_address(&peer_addr));
                // Don't re-run should_accept() here — the connection was already
                // accepted at the protocol level (the CONNECT terminus handler in connect.rs) and NAT traversal
                // already succeeded. Re-evaluating the probabilistic Kleinberg filter
                // would discard hard-won connections for no capacity reason (#3545).
                // Only enforce the hard max_connections cap as a resource safety limit.
                let current = connection_manager.connection_count();
                if current >= connection_manager.max_connections {
                    tracing::warn!(
                        tx = %tx,
                        %peer,
                        current_connections = current,
                        max_connections = connection_manager.max_connections,
                        %loc,
                        transient_expired,
                        "connect_peer: rejecting transient promotion to enforce cap"
                    );
                    callback
                        .send_result(Err(()))
                        .await
                        .inspect_err(|err| {
                            tracing::debug!(
                                tx = %tx,
                                remote = %peer,
                                ?err,
                                "connect_peer: failed to notify cap-rejection callback"
                            );
                        })
                        .ok();
                    return Ok(());
                }
                let just_became_ready = self
                    .bridge
                    .op_manager
                    .ring
                    .add_connection(loc, PeerId::new(peer.pub_key().clone(), peer_addr), true)
                    .await;
                tracing::info!(
                    tx = %tx,
                    remote = %peer,
                    transient_expired,
                    "connect_peer: promoted to ring"
                );

                // Update the dashboard/network_status with the peer's location.
                // Without this, peers promoted via this path show "—" on the
                // dashboard because the initial record_peer_connected(addr, None, None)
                // from handle_successful_connection is never updated.
                let pkl = crate::ring::PeerKeyLocation::new(peer.pub_key().clone(), peer_addr);
                crate::node::network_status::record_peer_connected(
                    peer_addr,
                    Some(loc.as_f64()),
                    Some(pkl),
                );

                // tell the promoted peer about our subscriptions and cache.
                // Non-blocking (#4145): handle_connect_peer runs INLINE on the
                // event-loop task, and bridge.send() awaits the cap-512
                // ev_listener_tx that this same task drains — a self-stall under
                // the connection-churn the SubscribeHint nudge drives. Use
                // bridge.try_send: a dropped interest/cache message is recovered
                // by the periodic InterestSync exchange + subscription-renewal
                // background task, which re-advertise our interests to this peer.
                for (target, msg) in self
                    .bridge
                    .op_manager
                    .on_ring_connection_established(peer_addr, peer.pub_key())
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

            // Now that we know the peer's identity (from ConnectRequest), update the
            // transport-level tracking so QueryConnections returns this peer.
            if let Some(entry) = self.connections.get_mut(&peer_addr) {
                if entry.pub_key.is_none() {
                    entry.pub_key = Some(peer.pub_key().clone());
                    self.addr_by_pub_key
                        .insert(peer.pub_key().clone(), peer_addr);
                    tracing::info!(
                        tx = %tx,
                        %peer_addr,
                        pub_key = %peer.pub_key(),
                        "connect_peer: updated transport entry with peer identity"
                    );
                } else {
                    tracing::info!(
                        tx = %tx,
                        %peer_addr,
                        existing_pub_key = ?entry.pub_key,
                        "connect_peer: transport entry already has pub_key"
                    );
                }
            } else {
                tracing::warn!(
                    tx = %tx,
                    %peer_addr,
                    "connect_peer: no transport entry found for peer_addr"
                );
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
        if !transient && !peer_addr.ip().is_unspecified() {
            let connection_manager = &self.bridge.op_manager.ring.connection_manager;
            let current = connection_manager.connection_count();
            if current >= connection_manager.max_connections {
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
        if !handshake_commands.try_send(HandshakeCommand::Connect {
            peer: peer.clone(),
            transaction: tx,
            transient,
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
                peer,
                connection,
                transient,
            } => {
                tracing::info!(provided = ?peer, transient, tx = ?transaction, "InboundConnection event");
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

                // For transient inbound connections, peer may be None - we don't know the
                // peer's identity yet. The identity will be learned when the peer sends
                // its first message (e.g., ConnectRequest).
                if peer.is_none() {
                    tracing::info!(
                        remote = %remote_addr,
                        transient,
                        transaction = ?transaction,
                        "Inbound connection arrived without matching expectation; accepting provisionally"
                    );
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

                // Pass peer directly - it may be None for transient connections
                self.handle_successful_connection(
                    peer,
                    connection,
                    state,
                    None,
                    is_transient,
                    handshake_commands,
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

                // For outbound connections, respect the transient flag from the handshake.
                // Gateway connections should remain transient until CONNECT acceptance.
                self.handle_successful_connection(
                    Some(peer),
                    connection,
                    state,
                    None,
                    transient,
                    handshake_commands,
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

    async fn handle_successful_connection(
        &mut self,
        peer_id: Option<PeerKeyLocation>,
        connection: Box<dyn PeerConnectionApi>,
        state: &mut EventListenerState,
        remaining_checks: Option<usize>,
        is_transient: bool,
        handshake_commands: &HandshakeCommandSender,
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
            if let Some(ref old_pub_key) = old.pub_key {
                self.addr_by_pub_key.remove(old_pub_key);
            }

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
                let old_peer = if let Some(ref pub_key) = old.pub_key {
                    PeerKeyLocation::new(pub_key.clone(), peer_addr)
                } else {
                    PeerKeyLocation::new(
                        (*self.bridge.op_manager.ring.connection_manager.pub_key).clone(),
                        peer_addr,
                    )
                };
                let prune_result = self
                    .bridge
                    .op_manager
                    .ring
                    .prune_connection(PeerId::new(old_peer.pub_key().clone(), peer_addr))
                    .await;
                self.bridge
                    .handle_orphaned_transactions(prune_result.orphaned_transactions, peer_addr)
                    .await;
                if prune_result.became_unready {
                    // Deferred: broadcast after connection_manager borrow ends
                    broadcast_ready = Some(false);
                }
                self.bridge
                    .op_manager
                    .on_ring_connection_lost(old_peer.pub_key());
                // Non-blocking (#4145): this runs on the event-loop task while
                // handling an inbound handshake event, so a `.send().await` that
                // back-pressures on the handshake command channel (cap 128) would
                // stall the loop. The DropConnection for the REPLACED (stale)
                // connection is redundant cleanup — we've already pruned the old
                // peer from the ring above and are about to install the new
                // connection entry. A missed DropConnection leaves at most one
                // stale per-SocketAddr ExpectedInboundTracker entry (no TTL sweep,
                // but overwritten on the next register() and consumed on a
                // matching inbound — bounded, benign).
                if !handshake_commands.try_send(HandshakeCommand::DropConnection {
                    peer: old_peer.clone(),
                }) {
                    tracing::warn!(
                        remote = %peer_addr,
                        "Handshake command channel full/closed; skipping redundant \
                         drop notification for replaced connection"
                    );
                }
            }

            tracing::info!(
                %peer_addr,
                old_connection_id = old.connection_id,
                was_transient,
                "Replacing stale connection entry (peer reconnected with new identity)"
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
        let (tx, rx) = mpsc::channel(10);
        tracing::debug!(
            self_peer = %self.bridge.op_manager.ring.connection_manager.pub_key,
            peer_id = ?peer_id,
            %peer_addr,
            connection_id = conn_id,
            conn_map_size = self.connections.len(),
            "[CONN_TRACK] INSERT: adding connection to HashMap"
        );
        self.connections.insert(
            peer_addr,
            ConnectionEntry {
                sender: tx,
                // For transient connections, we don't know the pub_key yet - it will be learned
                // when the peer sends its first message (e.g., ConnectRequest)
                pub_key: peer_id.as_ref().map(|p| p.pub_key().clone()),
                connection_id: conn_id,
                created_at: Instant::now(),
                remote_version,
            },
        );
        // Only add to reverse lookup if we know the pub_key
        // For transient connections, this will be populated when identity is learned
        if let Some(ref peer) = peer_id {
            self.addr_by_pub_key
                .insert(peer.pub_key().clone(), peer_addr);
        }
        let Some(conn_events) = self.conn_event_tx.as_ref().cloned() else {
            anyhow::bail!("Connection event channel not initialized");
        };

        // Phase 4: Set orphan stream registry on connection for handling race conditions
        // between stream fragments and metadata messages (RequestStreaming/ResponseStreaming).
        let mut connection = connection;
        connection
            .set_orphan_stream_registry(self.bridge.op_manager.orphan_stream_registry().clone());

        // Use tokio::spawn directly instead of GlobalExecutor::spawn.
        // GlobalExecutor::spawn uses Handle::try_current().spawn() which doesn't
        // reliably poll tasks in certain test contexts (see issue #2709).
        tokio::spawn(async move {
            peer_connection_listener(rx, connection, peer_addr, conn_events, conn_id).await;
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
        let promote_to_ring =
            peer_id.is_some() && (!is_transient || connection_manager.is_gateway());

        if newly_inserted {
            tracing::info!(peer_id = ?peer_id, %peer_addr, is_transient, "handle_successful_connection: inserted new connection entry");
            crate::node::network_status::record_peer_connected(peer_addr, None, None);
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
                // Only enforce the hard max_connections cap below.
                let current = connection_manager.connection_count();
                if current >= connection_manager.max_connections {
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
                    self.drop_connection_by_addr(peer_addr, handshake_commands)
                        .await;
                    return Ok(());
                }
                tracing::info!(%peer_addr, %loc, "handle_successful_connection: promoting connection into ring");
                let just_became_ready = self
                    .bridge
                    .op_manager
                    .ring
                    .add_connection(loc, PeerId::new(peer.pub_key().clone(), peer_addr), true)
                    .await;
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
                    connection_manager.try_register_transient(peer_addr, Some(loc));
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
                            let resolved_addr = {
                                let raw_addr = sender_addr.unwrap_or(remote_addr);
                                if raw_addr.ip().is_unspecified() {
                                    if let Some(sender_mut) =
                                        extract_sender_from_message_mut(&mut inbound.msg)
                                    {
                                        if sender_mut
                                            .socket_addr()
                                            .map(|a| a.ip().is_unspecified())
                                            .unwrap_or(true)
                                        {
                                            sender_mut.set_addr(remote_addr);
                                        }
                                    }
                                    remote_addr
                                } else {
                                    raw_addr
                                }
                            };
                            let new_peer_id =
                                PeerId::new(sender_peer.pub_key().clone(), resolved_addr);
                            // Check if we have a connection but with a different pub_key
                            if let Some(entry) = self.connections.get(&remote_addr) {
                                // If we don't have the pub_key stored yet or it differs from the new one, update it
                                let should_update = match &entry.pub_key {
                                    None => true,
                                    Some(old_pub_key) => old_pub_key != new_peer_id.pub_key(),
                                };
                                if should_update {
                                    let old_pub_key = entry.pub_key.clone();
                                    let is_first_identity = old_pub_key.is_none();
                                    tracing::info!(
                                        remote = %remote_addr,
                                        old_pub_key = ?old_pub_key,
                                        new_pub_key = %new_peer_id.pub_key(),
                                        is_first_identity,
                                        "Updating peer identity after inbound message"
                                    );
                                    // Remove old reverse lookup if it exists
                                    if let Some(old_key) = old_pub_key {
                                        self.addr_by_pub_key.remove(&old_key);
                                        // Update ring with old PeerId -> new PeerId
                                        let old_peer_id = PeerId::new(old_key, remote_addr);
                                        self.bridge.op_manager.ring.update_connection_identity(
                                            &old_peer_id,
                                            new_peer_id.clone(),
                                        );
                                    }
                                    // Update the entry's pub_key
                                    if let Some(entry) = self.connections.get_mut(&remote_addr) {
                                        entry.pub_key = Some(new_peer_id.pub_key().clone());
                                    }
                                    // Add new reverse lookup
                                    self.addr_by_pub_key
                                        .insert(new_peer_id.pub_key().clone(), remote_addr);
                                    // Note: We do NOT automatically promote to ring here.
                                    // Transient connections are promoted only when the Connect
                                    // operation explicitly accepts via NodeEvent::ConnectPeer,
                                    // which is handled by handle_connect_peer().
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
            }) => {
                tracing::debug!(
                    remote = %remote_addr,
                    ?error,
                    connection_id,
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
                    // Construct PeerKeyLocation for prune_connection and DropConnection
                    let peer = if let Some(ref pub_key) = entry.pub_key {
                        PeerKeyLocation::new(pub_key.clone(), remote_addr)
                    } else {
                        PeerKeyLocation::new(
                            (*self.bridge.op_manager.ring.connection_manager.pub_key).clone(),
                            remote_addr,
                        )
                    };
                    // Remove from reverse lookup
                    if let Some(pub_key) = entry.pub_key {
                        self.addr_by_pub_key.remove(&pub_key);
                    }
                    tracing::debug!(self_peer = %self.bridge.op_manager.ring.connection_manager.pub_key, %peer, socket_addr = %remote_addr, conn_map_size = self.connections.len(), "[CONN_TRACK] REMOVE: TransportClosed - removing from connections HashMap");
                    let prune_result = self
                        .bridge
                        .op_manager
                        .ring
                        .prune_connection(PeerId::new(peer.pub_key().clone(), remote_addr))
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

                    // forget this peer's subscriptions and cached contract state
                    self.bridge
                        .op_manager
                        .on_ring_connection_lost(peer.pub_key());

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
                    // per-SocketAddr ExpectedInboundTracker entry (no TTL sweep, but
                    // overwritten on the next register() and consumed on a matching
                    // inbound — bounded, benign).
                    if !handshake_commands
                        .try_send(HandshakeCommand::DropConnection { peer: peer.clone() })
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
