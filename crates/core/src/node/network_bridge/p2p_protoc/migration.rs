//! Directed-subscribe placement / contract-migration helpers for
//! [`P2pConnManager`].
//!
//! Behavior-preserving extraction from `p2p_protoc.rs`.

use super::*;

impl P2pConnManager {
    /// Whether the peer at `addr` reports a negotiated protocol version new
    /// enough to understand [`SubscribeHint`](crate::message::NetMessageV1::SubscribeHint).
    ///
    /// `None` (unknown version) is treated as unsupported, since an older peer
    /// would fail to deserialize the new wire variant and drop the connection.
    fn peer_supports_subscribe_hint(&self, addr: &SocketAddr) -> bool {
        let remote = self.connections.get(addr).and_then(|e| e.remote_version);
        // Production floor, unless a sim test opted into the cascade by setting a
        // per-node override (never set in production — see NodeConfig).
        let floor = self
            .bridge
            .op_manager
            .ring
            .connection_manager
            .subscribe_hint_floor_override()
            .unwrap_or(SUBSCRIBE_HINT_MIN_VERSION);
        version_supports_subscribe_hint(remote, floor)
    }

    /// Pick the single best peer to nudge into hosting `key`, or `None`.
    ///
    /// Directed-subscribe placement (#4404): we host `key` but a connected
    /// neighbor may be strictly closer to the contract's key in the ring than
    /// we are. Nudging that neighbor to subscribe-and-host migrates the
    /// contract toward its ideal location. We pick ONLY the single closest
    /// qualifying neighbor (not a fan-out), and ONLY when it is strictly
    /// closer than us and not already hosting.
    ///
    /// A candidate qualifies iff it has a known socket address, a known ring
    /// location, is not already hosting `key` (per neighbor-hosting state),
    /// reports a protocol version new enough to understand `SubscribeHint`, and
    /// is strictly closer to the contract location than we are. Returns the
    /// qualifying candidate with the smallest distance to the contract, breaking
    /// ties deterministically by public-key bytes.
    ///
    /// The version filter is applied HERE (before the pure selection core) so
    /// that an old/unknown-version peer that happens to be closest does not
    /// suppress migration: we fall through to the next-closest peer that CAN
    /// receive the hint. This matters during a mixed-version rolling upgrade.
    fn select_migration_target(
        &self,
        key: &freenet_stdlib::prelude::ContractKey,
    ) -> Option<PeerKeyLocation> {
        let contract_loc = Location::from(key);
        let me = self
            .bridge
            .op_manager
            .ring
            .connection_manager
            .own_location();
        let my_dist = contract_loc.distance(me.location()?);

        let hosting: HashSet<TransportPublicKey> = self
            .bridge
            .op_manager
            .neighbor_hosting
            .neighbors_with_contract(key)
            .into_iter()
            .collect();

        let connections = self
            .bridge
            .op_manager
            .ring
            .connection_manager
            .get_connections_by_location();

        pick_closest_migration_target(
            contract_loc,
            my_dist,
            &hosting,
            connections
                .values()
                .flatten()
                .map(|conn| &conn.location)
                // Only peers that can understand the appended wire variant are
                // eligible; a closer but too-old peer is skipped so the next
                // eligible peer is chosen (mixed-version safety).
                .filter(|pkl| {
                    pkl.socket_addr()
                        .is_some_and(|addr| self.peer_supports_subscribe_hint(&addr))
                }),
        )
    }

    /// Best-effort nudge for a single contract we host: if there is a closer,
    /// non-hosting neighbor that understands `SubscribeHint`, tell it we are
    /// the current holder so it can directed-subscribe and host `key`.
    ///
    /// No-op if we do not host `key` or if there is no qualifying target
    /// (closer, non-hosting, AND version-supported — the selection skips peers
    /// too old to understand the hint). Fire-and-forget and
    /// NON-BLOCKING: the nudge is dispatched with `try_send`, so a congested
    /// bridge channel drops the hint (logged at debug) rather than stalling the
    /// connection-handling event loop — see `.claude/rules/channel-safety.md`.
    /// Migration is reconsidered on the next hosting/peer event, so a dropped
    /// hint is self-healing.
    pub(super) fn consider_contract_migration(&self, key: freenet_stdlib::prelude::ContractKey) {
        // Disabled 0.2.88: over-aggressive migration storm (#4630-adjacent). do
        // NOT re-enable aggressive placement migration; see
        // .claude/rules/hosting-invariants.md (anti-pattern: holding-driven
        // placement push). Production nodes (no floor override) emit nothing
        // while the kill switch is off; simulations (override present) defer to
        // the floor checks below, preserving the migration-mechanism tests.
        if !placement_migration_enabled(
            self.bridge
                .op_manager
                .ring
                .connection_manager
                .subscribe_hint_floor_override(),
        ) {
            return;
        }
        if !self.bridge.op_manager.ring.is_hosting_contract(&key) {
            return;
        }
        let Some(target) = self.select_migration_target(&key) else {
            return;
        };
        let Some(addr) = target.socket_addr() else {
            return;
        };
        // Defense-in-depth: `select_migration_target` already filters to
        // version-supported peers, but re-gate here so a SubscribeHint can NEVER
        // be sent to a peer too old to deserialize it (which would drop the
        // connection) even if the selection path changes.
        if !self.peer_supports_subscribe_hint(&addr) {
            return;
        }
        // The migration target is a connected neighbor we just looked up and
        // version-checked, so it resolves via `get_peer_by_addr`.
        let Some(target_pkl) = self
            .bridge
            .op_manager
            .ring
            .connection_manager
            .get_peer_by_addr(addr)
        else {
            return;
        };
        let me = self
            .bridge
            .op_manager
            .ring
            .connection_manager
            .own_location();
        let msg = NetMessage::V1(NetMessageV1::SubscribeHint(
            crate::message::SubscribeHintMsg { key, holder: me },
        ));
        // Placement-migration telemetry (#4404 follow-up): count the dispatch of
        // this nudge. Counted here, at the point we commit to sending, so the
        // `sent` total reflects nudges we attempted to dispatch (a bridge-full
        // drop below is self-healing and re-tried on the next migration trigger).
        self.bridge
            .op_manager
            .ring
            .placement_migration_metrics()
            .record_sent();
        self.bridge
            .op_manager
            .sending_transaction(&target_pkl, &msg);
        // Non-blocking dispatch: never `.send().await` from the event loop.
        if let Err(e) = self
            .bridge
            .ev_listener_tx
            .try_send(P2pBridgeEvent::Message(target_pkl, Box::new(msg)))
        {
            tracing::debug!(%addr, %key, error = %e, "SubscribeHint nudge dropped (bridge busy or closed)");
        }
    }

    /// On gaining a new neighbor, reconsider migrating each contract we host.
    ///
    /// The new peer may be the closest non-hosting neighbor for some of our
    /// contracts; `consider_contract_migration` recomputes the single best
    /// target per contract, so we do not special-case `peer_addr` here beyond
    /// an early skip when the new peer is not even closer than us. The number
    /// of contracts examined per call is capped at
    /// [`MIGRATION_SCAN_CAP_PER_NEW_PEER`] so we never do an unbounded inline
    /// scan + nudge fan-out from the event loop (channel-safety).
    pub(super) fn consider_migration_for_new_peer(&self, peer_addr: SocketAddr) {
        // Disabled 0.2.88: over-aggressive migration storm (#4630-adjacent). do
        // NOT re-enable aggressive placement migration; see
        // .claude/rules/hosting-invariants.md (anti-pattern: holding-driven
        // placement push). Gaining a neighbor must NOT nudge it for our hosted
        // contracts while the kill switch is off (this is the storm source).
        // Production nodes (no floor override) early-return; simulations
        // (override present) defer to the floor checks, preserving test coverage.
        if !placement_migration_enabled(
            self.bridge
                .op_manager
                .ring
                .connection_manager
                .subscribe_hint_floor_override(),
        ) {
            return;
        }
        let new_peer_loc = self
            .connections
            .get(&peer_addr)
            .and_then(|e| e.pub_key.as_ref())
            .and_then(|pk| {
                self.bridge
                    .op_manager
                    .ring
                    .connection_manager
                    .get_peer_by_addr(peer_addr)
                    .filter(|p| p.pub_key() == pk)
                    .and_then(|p| p.location())
            })
            .or_else(|| {
                self.bridge
                    .op_manager
                    .ring
                    .connection_manager
                    .get_peer_by_addr(peer_addr)
                    .and_then(|p| p.location())
            });
        let my_loc = self
            .bridge
            .op_manager
            .ring
            .connection_manager
            .own_location()
            .location();

        let keys = self.bridge.op_manager.ring.hosting_contract_keys();
        let total = keys.len();
        if total == 0 {
            return;
        }
        // Bound the per-event scan: examine at most
        // MIGRATION_SCAN_CAP_PER_NEW_PEER keys even when most of them skip the
        // cheap test below, so a node hosting thousands of contracts can't walk
        // its entire hosting set on every new connection (unbounded work on the
        // connection-handling path). A rotating start offset (the
        // `migration_scan_cursor`) makes successive events cover different
        // windows, so contracts past the cap are reached on later events rather
        // than being starved in a fixed prefix.
        let window = MIGRATION_SCAN_CAP_PER_NEW_PEER;
        let start = self
            .bridge
            .op_manager
            .ring
            .connection_manager
            .next_migration_scan_offset(window)
            % total;
        if total > window {
            tracing::debug!(
                cap = window,
                total,
                start,
                %peer_addr,
                "Capping migration scan for new peer; rotating window covers the rest on later events"
            );
        }
        for i in 0..window.min(total) {
            let key = keys[(start + i) % total];
            // Optimization: if we know both locations and the new peer isn't
            // strictly closer to this contract than we are, it can't become
            // the migration target this contract gained from the new peer, so
            // skip the full selection join.
            if let (Some(new_loc), Some(my_loc)) = (new_peer_loc, my_loc) {
                let contract_loc = Location::from(&key);
                if contract_loc.distance(new_loc) >= contract_loc.distance(my_loc) {
                    continue;
                }
            }
            self.consider_contract_migration(key);
        }
    }
}
