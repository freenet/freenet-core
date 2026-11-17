//! A NAT for the in-memory network: which inbound datagrams a node behind
//! one receives.
//!
//! The model is endpoint-independent mapping (a node's external address is
//! its own address, so nothing is translated) with one of three filtering
//! behaviours. Each outbound datagram opens or refreshes a binding for its
//! destination, and a binding lasts [`NatModel::lifetime`] after the last
//! datagram that refreshed it. An inbound datagram is admitted only through
//! a live binding, so two nodes behind NATs reach each other only by
//! punching: both send, within a binding's lifetime of each other.
//!
//! - [`NatFiltering::Full`]: any source, once the node has sent anything
//!   (full cone).
//! - [`NatFiltering::AddressRestricted`]: a source whose IP the node has sent
//!   to.
//! - [`NatFiltering::PortRestricted`]: a source whose IP and port the node
//!   has sent to.
//!
//! Symmetric mapping (a fresh external port per destination) is not modelled
//! here (A5c-2).
//!
//! The decision is taken when a datagram ARRIVES, not when it is sent: a
//! punch that crosses one the receiver sends while it is in flight is
//! admitted, as a real NAT admits it. See `in_memory_socket`'s hooks.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

/// How a NAT filters inbound datagrams; see the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NatFiltering {
    /// Any source, once the node has sent anything (full cone).
    Full,
    /// A source whose IP the node has sent to.
    AddressRestricted,
    /// A source whose IP and port the node has sent to.
    PortRestricted,
}

impl NatFiltering {
    /// Every class, for matrices.
    pub const ALL: [Self; 3] = [Self::Full, Self::AddressRestricted, Self::PortRestricted];

    /// A short label for test output and counts.
    pub fn label(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::AddressRestricted => "address_restricted",
            Self::PortRestricted => "port_restricted",
        }
    }
}

/// How long a binding lasts after the last outbound datagram that refreshed
/// it. Provisional (P): home routers commonly keep a UDP binding 30 s to a
/// few minutes; the short end makes punching timing matter.
pub const DEFAULT_BINDING_LIFETIME: Duration = Duration::from_secs(30);

/// Above this many bindings, expired ones are pruned on the next outbound
/// datagram, so a long run does not grow the tables without bound.
const PRUNE_ABOVE: usize = 1024;

/// One node's NAT. Times are nanoseconds on the network's clock.
#[derive(Debug, Clone)]
pub struct NatModel {
    filtering: NatFiltering,
    lifetime: u64,
    /// When the node last sent anything (full cone's single binding).
    last_any: Option<u64>,
    /// When the node last sent to each IP.
    by_ip: HashMap<IpAddr, u64>,
    /// When the node last sent to each address.
    by_addr: HashMap<SocketAddr, u64>,
}

impl NatModel {
    /// A NAT of `filtering` whose bindings last `lifetime` after their last
    /// refresh, with no binding open yet.
    pub fn new(filtering: NatFiltering, lifetime: Duration) -> Self {
        Self {
            filtering,
            lifetime: u64::try_from(lifetime.as_nanos()).unwrap_or(u64::MAX),
            last_any: None,
            by_ip: HashMap::new(),
            by_addr: HashMap::new(),
        }
    }

    /// This NAT's filtering class.
    pub fn filtering(&self) -> NatFiltering {
        self.filtering
    }

    /// How long a binding lasts after its last refresh.
    pub fn lifetime(&self) -> Duration {
        Duration::from_nanos(self.lifetime)
    }

    /// The node sent a datagram to `to` at `now`: open or refresh the binding.
    pub fn on_outbound(&mut self, to: SocketAddr, now: u64) {
        self.last_any = Some(now);
        self.by_ip.insert(to.ip(), now);
        self.by_addr.insert(to, now);
        if self.by_addr.len() > PRUNE_ABOVE || self.by_ip.len() > PRUNE_ABOVE {
            let live = |at: &u64| now.saturating_sub(*at) <= self.lifetime;
            self.by_ip.retain(|_, at| live(at));
            self.by_addr.retain(|_, at| live(at));
        }
    }

    /// Whether a datagram from `from` arriving at `now` gets through.
    pub fn admits(&self, from: SocketAddr, now: u64) -> bool {
        let refreshed = match self.filtering {
            NatFiltering::Full => self.last_any,
            NatFiltering::AddressRestricted => self.by_ip.get(&from.ip()).copied(),
            NatFiltering::PortRestricted => self.by_addr.get(&from).copied(),
        };
        refreshed.is_some_and(|at| now.saturating_sub(at) <= self.lifetime)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: u64 = 1_000_000_000;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    /// Nothing gets in before the node sends; after it sends to `a`, each
    /// class admits exactly its own set of sources.
    #[test]
    fn each_class_admits_its_own_sources() {
        let a = addr("[fd00::a]:1000");
        let a_other_port = addr("[fd00::a]:2000");
        let b = addr("[fd00::b]:1000");
        for (filtering, admitted) in [
            (NatFiltering::Full, [true, true, true]),
            (NatFiltering::AddressRestricted, [true, true, false]),
            (NatFiltering::PortRestricted, [true, false, false]),
        ] {
            let mut nat = NatModel::new(filtering, DEFAULT_BINDING_LIFETIME);
            for from in [a, a_other_port, b] {
                assert!(!nat.admits(from, 0), "{filtering:?}: closed before sending");
            }
            nat.on_outbound(a, 0);
            assert_eq!(
                [a, a_other_port, b].map(|from| nat.admits(from, S)),
                admitted,
                "{filtering:?}"
            );
        }
    }

    /// A binding lasts its lifetime after the last refresh, and each
    /// outbound datagram refreshes it.
    #[test]
    fn a_binding_expires_unless_refreshed() {
        let peer = addr("[fd00::a]:1000");
        for filtering in NatFiltering::ALL {
            let mut nat = NatModel::new(filtering, Duration::from_secs(30));
            nat.on_outbound(peer, 0);
            assert!(nat.admits(peer, 30 * S), "{filtering:?}: live at its end");
            assert!(!nat.admits(peer, 30 * S + 1), "{filtering:?}: expired");
            nat.on_outbound(peer, 20 * S);
            assert!(nat.admits(peer, 50 * S), "{filtering:?}: refreshed");
            assert!(
                !nat.admits(peer, 50 * S + 1),
                "{filtering:?}: expired again"
            );
        }
    }

    /// Expired bindings are pruned once the table is large, and live ones
    /// are kept.
    #[test]
    fn expired_bindings_are_pruned_when_the_table_grows() {
        let mut nat = NatModel::new(NatFiltering::PortRestricted, Duration::from_secs(1));
        for port in 0..=PRUNE_ABOVE as u16 {
            nat.on_outbound(SocketAddr::new("fd00::a".parse().unwrap(), port), 0);
        }
        let fresh = addr("[fd00::b]:1");
        nat.on_outbound(fresh, 10 * S);
        assert_eq!(nat.by_addr.len(), 1);
        assert_eq!(nat.by_ip.len(), 1);
        assert!(nat.admits(fresh, 10 * S));
    }
}
