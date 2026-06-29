//! Bootstrap-window gateway fallback shared across operations (#4361 / #4365).
//!
//! A freshly started node has an empty ring (`connections_by_location` has no
//! entries) for its first minutes, yet already holds a live transient transport
//! connection to its gateway. Operation peer-selection consults only the ring,
//! so without a fallback every op in that window either fails or finalizes
//! locally without ever using the transient gateway connection.
//!
//! GET introduced the bounded gateway fallback in #4364; this module lifts the
//! selection core out of GET so PUT, UPDATE, and SUBSCRIBE reuse exactly the
//! same rule at their own decision points (#4365).

use std::net::SocketAddr;

use crate::node::OpManager;
use crate::ring::PeerKeyLocation;

/// Pure selection core for the bootstrap gateway fallback (#4361).
///
/// Returns the first configured gateway that is not this node and not
/// excluded by the caller's skip predicate — but ONLY when the ring is
/// empty (`ring_connection_count == 0`). A non-empty ring means normal
/// routing had ring entries to consider (its candidates may still all
/// be filtered as transient/visited, but that exhaustion is genuine),
/// so the fallback must stay out of the way.
///
/// Selection is deliberately deterministic (config order), unlike the
/// randomized pick in #3219's CONNECT re-bootstrap: independent selectors
/// (e.g. a client driver and the per-attempt loopback relay) must converge
/// on the same gateway given the same exclusion set — stream claims and
/// route telemetry are attributed via the chosen target, so divergence
/// would mis-key both. Failover diversity comes from the exclusion
/// predicate (tried/visited), not from randomization.
///
/// Split out from [`bootstrap_gateway_target`] so unit tests can
/// exercise the selection rules without constructing an `OpManager`.
pub(crate) fn select_bootstrap_gateway(
    ring_connection_count: usize,
    configured_gateways: &[PeerKeyLocation],
    own_addr: Option<SocketAddr>,
    is_excluded: impl Fn(SocketAddr) -> bool,
) -> Option<(PeerKeyLocation, SocketAddr)> {
    if ring_connection_count > 0 {
        return None;
    }
    configured_gateways.iter().find_map(|gw| {
        let addr = gw.socket_addr()?;
        if Some(addr) == own_addr || is_excluded(addr) {
            return None;
        }
        Some((gw.clone(), addr))
    })
}

/// Bootstrap gateway fallback (#4361).
///
/// A node whose ring is still empty (`connections_by_location` has no
/// entries) has zero routing candidates, so without a fallback an
/// operation either fails instantly or finalizes locally — even though
/// the node has a live transient transport connection to its gateway
/// (the same connection its CONNECT handshake is negotiating over).
/// Production analog: a freshly started node misbehaves on every op for
/// its first minutes until ring promotion completes.
///
/// When the ring is empty, fall back to a configured gateway. Wire
/// delivery works before ring promotion: the event loop's
/// `OutboundMessageWithTarget` handler resolves targets by socket addr
/// against the transport map (which includes transient connections)
/// and re-dials configured gateways if the transport lapsed. Mirrors
/// the #3219 zero-connection CONNECT re-bootstrap shape, which gates
/// on the same `connection_count() == 0` condition.
///
/// Load-bearing assumption: `connection_count()` counts only **promoted
/// ring** peers (`connections_by_location`), a map disjoint from the
/// transient-connection map. That is what makes `== 0` fire precisely
/// during the bootstrap window (transient gateway connection present,
/// ring not yet formed) and stay out of the way the moment any ring
/// connection is promoted. If a future change ever inserted transient
/// connections into `connections_by_location`, this gate would silently
/// no-op the fallback across all ops.
///
/// Tradeoff for genuinely unreachable gateways (e.g. an offline
/// machine): a dial failure is not surfaced to the waiting driver, so
/// each attempt waits its full per-attempt timeout before advancing,
/// where the pre-fallback behavior failed instantly. The instant
/// failure was a false answer (a false "contract absent" for GET, a
/// false "success" for PUT), so slower but honest is preferred; a
/// distinct fail-fast "not bootstrapped / unreachable" client error is
/// #4166's scope.
pub(crate) fn bootstrap_gateway_target(
    op_manager: &OpManager,
    is_excluded: impl Fn(SocketAddr) -> bool,
) -> Option<(PeerKeyLocation, SocketAddr)> {
    select_bootstrap_gateway(
        op_manager.ring.connection_manager.connection_count(),
        &op_manager.configured_gateways,
        op_manager.ring.connection_manager.get_own_addr(),
        is_excluded,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gw(port: u16) -> PeerKeyLocation {
        let key = crate::transport::TransportPublicKey::from_bytes([port as u8; 32]);
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        PeerKeyLocation::new(key, addr)
    }

    /// Regression for #4361: a node with an empty ring but configured
    /// gateways must select a gateway instead of exhausting. Before the
    /// fix, every op on a freshly-bootstrapped node misbehaved without
    /// sending a single wire message.
    #[test]
    fn bootstrap_fallback_selects_gateway_when_ring_empty() {
        let gateway = gw(4001);
        let selected = select_bootstrap_gateway(0, std::slice::from_ref(&gateway), None, |_| false);
        let (peer, addr) = selected.expect("empty ring + configured gateway must select it");
        assert_eq!(peer.socket_addr(), gateway.socket_addr());
        assert_eq!(addr, gateway.socket_addr().unwrap());
    }

    /// The fallback must stay out of the way when the ring has ANY
    /// connections — a non-empty ring means normal routing had its
    /// chance and exhaustion is genuine.
    #[test]
    fn bootstrap_fallback_inactive_when_ring_non_empty() {
        let gateway = gw(4001);
        assert!(
            select_bootstrap_gateway(1, &[gateway], None, |_| false).is_none(),
            "fallback must not fire with ring connections present"
        );
    }

    /// A gateway node must never select itself (self-loop guard), but
    /// must still be able to fall back to OTHER configured gateways.
    #[test]
    fn bootstrap_fallback_skips_self() {
        let gw1 = gw(4001);
        let gw2 = gw(4002);
        let own_addr = gw1.socket_addr();
        let selected =
            select_bootstrap_gateway(0, &[gw1.clone(), gw2.clone()], own_addr, |_| false);
        let (peer, _) = selected.expect("second gateway should be selected");
        assert_eq!(
            peer.socket_addr(),
            gw2.socket_addr(),
            "self gateway must be skipped"
        );
        assert!(
            select_bootstrap_gateway(0, std::slice::from_ref(&gw1), gw1.socket_addr(), |_| false)
                .is_none(),
            "sole self gateway must yield no fallback"
        );
    }

    /// Already-tried/visited gateways are excluded, so retries cannot
    /// loop on the same gateway and a request that already traversed a
    /// gateway is never bounced back to it.
    #[test]
    fn bootstrap_fallback_excludes_tried_gateways() {
        let gw1 = gw(4001);
        let gw2 = gw(4002);
        let tried = [gw1.socket_addr().unwrap()];
        let selected = select_bootstrap_gateway(0, &[gw1.clone(), gw2.clone()], None, |addr| {
            tried.contains(&addr)
        });
        let (peer, _) = selected.expect("untried gateway should be selected");
        assert_eq!(peer.socket_addr(), gw2.socket_addr());
        let all_tried = [gw1.socket_addr().unwrap(), gw2.socket_addr().unwrap()];
        assert!(
            select_bootstrap_gateway(0, &[gw1, gw2], None, |addr| all_tried.contains(&addr))
                .is_none(),
            "all gateways tried must yield no fallback"
        );
    }

    /// Boundary: a node with an empty ring but NO configured gateways has
    /// nothing to fall back to, so selection yields `None` and the caller
    /// keeps its genuinely-isolated path (PUT finalizes locally, UPDATE /
    /// SUBSCRIBE return an explicit error). The fallback is strictly
    /// additive — it never fires when there is no usable gateway.
    #[test]
    fn bootstrap_fallback_none_when_no_gateways_configured() {
        assert!(
            select_bootstrap_gateway(0, &[], None, |_| false).is_none(),
            "empty ring with no configured gateways must yield no fallback"
        );
    }
}
