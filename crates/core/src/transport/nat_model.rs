//! A NAT for the in-memory network: which inbound datagrams a node behind
//! one receives.
//!
//! Three classes use endpoint-independent mapping (a node's external address
//! is its own address, so nothing is translated) with one of three filtering
//! behaviours; [`NatFiltering::Symmetric`] maps each destination to its own
//! external port. Each outbound datagram opens or refreshes a binding for its
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
//! - [`NatFiltering::Symmetric`]: each destination gets a fresh external port
//!   (on the node's own IP), and a datagram to that port is admitted only from
//!   that destination. The node's own address is not reachable at all, so a
//!   peer can reach it only at a port the node opened toward that peer.
//!   What a gateway observes (and reports) is the port opened toward the
//!   gateway, which admits nobody else.
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
    /// A fresh external port per destination, admitting only that
    /// destination.
    Symmetric,
}

impl NatFiltering {
    /// Every class with endpoint-independent mapping, for matrices. Not
    /// [`Self::Symmetric`], whose pairs are expected to fail and are tested
    /// on their own.
    pub const ALL: [Self; 3] = [Self::Full, Self::AddressRestricted, Self::PortRestricted];

    /// A short label for test output and counts.
    pub fn label(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::AddressRestricted => "address_restricted",
            Self::PortRestricted => "port_restricted",
            Self::Symmetric => "symmetric",
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

/// The first external port a symmetric NAT hands out; each new destination
/// gets the next one.
const FIRST_MAPPED_PORT: u16 = 40_000;

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
    /// Symmetric only: the external port opened toward each destination,
    /// and the destination each external port admits.
    port_for: HashMap<SocketAddr, u16>,
    dest_of: HashMap<u16, SocketAddr>,
    next_port: u16,
    /// Prune expired bindings once a table grows past this
    /// ([`PRUNE_ABOVE`], then twice the size left after a prune), or a
    /// binding lifetime after the last prune while above [`PRUNE_ABOVE`].
    prune_at: usize,
    /// When the tables were last pruned.
    pruned_at: u64,
    /// Symmetric only: the node's own address, learned from its first
    /// outbound datagram; where admitted inbound datagrams are delivered.
    internal: Option<SocketAddr>,
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
            port_for: HashMap::new(),
            dest_of: HashMap::new(),
            next_port: FIRST_MAPPED_PORT,
            internal: None,
            prune_at: PRUNE_ABOVE,
            pruned_at: 0,
        }
    }

    /// The source address a datagram from `internal` to `to` leaves the NAT
    /// with: `internal` itself, or for a symmetric NAT the external port
    /// opened toward `to` (a new one for a new destination), or `None` if
    /// every port is mapped to a live binding (the datagram is dropped).
    /// Expired mappings are reclaimed when the range is full (the table is
    /// bounded by the port range, about 25,500 entries). Call
    /// before [`Self::on_outbound`].
    pub fn map_outbound(
        &mut self,
        internal: SocketAddr,
        to: SocketAddr,
        now: u64,
    ) -> Option<SocketAddr> {
        if self.filtering != NatFiltering::Symmetric {
            return Some(internal);
        }
        self.internal = Some(internal);
        if let Some(port) = self.port_for.get(&to) {
            return Some(SocketAddr::new(internal.ip(), *port));
        }
        let port = match self.free_port(internal.port()) {
            Some(port) => port,
            None => {
                self.reclaim_expired(now);
                self.free_port(internal.port())?
            }
        };
        self.next_port = if port == u16::MAX {
            FIRST_MAPPED_PORT
        } else {
            port + 1
        };
        self.port_for.insert(to, port);
        self.dest_of.insert(port, to);
        Some(SocketAddr::new(internal.ip(), port))
    }

    /// The next unmapped external port from `next_port`, searching the whole
    /// range once; `None` when every port is mapped.
    fn free_port(&self, internal_port: u16) -> Option<u16> {
        let range = u32::from(u16::MAX - FIRST_MAPPED_PORT) + 1;
        (0..range)
            .map(|i| {
                let offset = (u32::from(self.next_port - FIRST_MAPPED_PORT) + i) % range;
                FIRST_MAPPED_PORT + offset as u16
            })
            .find(|port| *port != internal_port && !self.dest_of.contains_key(port))
    }

    /// Forgets the mappings whose binding has expired, so their ports can be
    /// handed out again. A real NAT also reuses a port once its binding
    /// times out (some sooner, some later), so this is lenient, not exact.
    fn reclaim_expired(&mut self, now: u64) {
        let lifetime = self.lifetime;
        let by_addr = &self.by_addr;
        let live = |dest: &SocketAddr| {
            by_addr
                .get(dest)
                .is_some_and(|at| now.saturating_sub(*at) <= lifetime)
        };
        self.dest_of.retain(|_, dest| live(dest));
        self.port_for.retain(|dest, _| live(dest));
    }

    /// The node's own address, if `external` is a port this (symmetric) NAT
    /// has mapped; for matching fault rules against the node that owns it.
    pub fn owner_of(&self, external: SocketAddr) -> Option<SocketAddr> {
        if self.filtering != NatFiltering::Symmetric || external.ip() != self.internal?.ip() {
            return None;
        }
        self.dest_of.get(&external.port()).and(self.internal)
    }

    /// Where a datagram arriving at `target` from `from` at `now` is
    /// delivered, or `None` if the NAT drops it. A symmetric NAT delivers to
    /// the node's own address, and only through a live port opened toward
    /// `from`; the other classes deliver to `target` when [`Self::admits`]
    /// lets the datagram in.
    pub fn map_inbound(
        &self,
        target: SocketAddr,
        from: SocketAddr,
        now: u64,
    ) -> Option<SocketAddr> {
        if self.filtering != NatFiltering::Symmetric {
            return self.admits(from, now).then_some(target);
        }
        let dest = self.dest_of.get(&target.port())?;
        if *dest == from && self.admits(from, now) {
            self.internal
        } else {
            None
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
        let size = self.by_addr.len().max(self.by_ip.len());
        let stale = now.saturating_sub(self.pruned_at) > self.lifetime;
        if size > PRUNE_ABOVE && (size > self.prune_at || stale) {
            let live = |at: &u64| now.saturating_sub(*at) <= self.lifetime;
            self.by_ip.retain(|_, at| live(at));
            self.by_addr.retain(|_, at| live(at));
            // The next prune waits for the table to double or a binding
            // lifetime to pass, so a table of live bindings is not rescanned
            // on every datagram.
            self.prune_at = PRUNE_ABOVE.max(2 * self.by_addr.len().max(self.by_ip.len()));
            self.pruned_at = now;
        }
    }

    /// Whether a datagram from `from` that this NAT drops at `now` would have
    /// been admitted through a binding that has since expired (as opposed to
    /// one never opened).
    pub fn expired_for(&self, from: SocketAddr, now: u64) -> bool {
        let refreshed = match self.filtering {
            NatFiltering::Full => self.last_any,
            NatFiltering::AddressRestricted => self.by_ip.get(&from.ip()).copied(),
            NatFiltering::PortRestricted | NatFiltering::Symmetric => {
                self.by_addr.get(&from).copied()
            }
        };
        refreshed.is_some_and(|at| now.saturating_sub(at) > self.lifetime)
    }

    /// Whether a datagram from `from` arriving at `now` gets through.
    pub fn admits(&self, from: SocketAddr, now: u64) -> bool {
        let refreshed = match self.filtering {
            NatFiltering::Full => self.last_any,
            NatFiltering::AddressRestricted => self.by_ip.get(&from.ip()).copied(),
            NatFiltering::PortRestricted | NatFiltering::Symmetric => {
                self.by_addr.get(&from).copied()
            }
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

    /// A symmetric NAT opens a port per destination; each port admits only
    /// its destination, delivers to the node's own address, and the node's
    /// own address admits nothing.
    #[test]
    fn a_symmetric_nat_maps_each_destination_to_its_own_port() {
        let node = addr("[fd00::1]:5000");
        let a = addr("[fd00::a]:1000");
        let b = addr("[fd00::b]:1000");
        let mut nat = NatModel::new(NatFiltering::Symmetric, DEFAULT_BINDING_LIFETIME);
        let to_a = nat.map_outbound(node, a, 0).unwrap();
        nat.on_outbound(a, 0);
        let to_b = nat.map_outbound(node, b, 0).unwrap();
        nat.on_outbound(b, 0);
        assert_eq!(to_a.ip(), node.ip());
        assert_ne!(to_a, node, "translated");
        assert_ne!(to_a, to_b, "a port per destination");
        assert_eq!(
            nat.map_outbound(node, a, 0).unwrap(),
            to_a,
            "stable per destination"
        );
        assert_eq!(
            nat.map_inbound(to_a, a, S),
            Some(node),
            "its destination gets in"
        );
        assert_eq!(nat.map_inbound(to_a, b, S), None, "another source does not");
        assert_eq!(
            nat.map_inbound(node, a, S),
            None,
            "the node's own address is closed"
        );
        assert_eq!(
            nat.map_inbound(to_a, a, DEFAULT_BINDING_LIFETIME.as_nanos() as u64 + 1),
            None,
            "expired"
        );
        // An endpoint-independent class translates nothing.
        let mut port_restricted =
            NatModel::new(NatFiltering::PortRestricted, DEFAULT_BINDING_LIFETIME);
        assert_eq!(port_restricted.map_outbound(node, a, 0).unwrap(), node);
    }

    /// A symmetric NAT with every external port mapped to a live binding
    /// maps nothing more (the datagram is dropped) rather than searching for
    /// ever; once those bindings expire their ports are reclaimed. It names
    /// the node that owns each of its mapped ports.
    #[test]
    fn a_symmetric_nat_runs_out_of_ports_and_reclaims_expired_ones() {
        let node = addr("[fd00::1]:5000");
        let mut nat = NatModel::new(NatFiltering::Symmetric, Duration::from_secs(30));
        let ports = usize::from(u16::MAX - FIRST_MAPPED_PORT) + 1;
        // Fewer than 30,000 destinations, all on one peer IP.
        let dest =
            |i: usize| SocketAddr::new("fd00::b".parse().unwrap(), u16::try_from(i + 1).unwrap());
        let mut first = None;
        for i in 0..ports {
            let to = dest(i);
            let mapped = nat.map_outbound(node, to, 0).expect("a free port");
            nat.on_outbound(to, 0);
            first.get_or_insert(mapped);
        }
        let first = first.unwrap();
        assert_eq!(nat.owner_of(first), Some(node), "its owner");
        assert_eq!(nat.owner_of(node), None, "not a mapped port");
        // Every port is mapped and live: no mapping, and no endless search.
        assert_eq!(nat.map_outbound(node, dest(ports), S), None);
        // Once the bindings expire, a new destination gets a reclaimed port.
        let later = 31 * S;
        let fresh = nat
            .map_outbound(node, dest(ports + 1), later)
            .expect("reclaimed");
        assert!(fresh.port() >= FIRST_MAPPED_PORT);
        assert!(nat.dest_of.len() < ports, "expired mappings were forgotten");
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

    /// `expired_for` is true only for a source whose binding existed and has
    /// since lapsed, in every filtering class; never for a source that was
    /// never opened, and not while the binding is refreshed.
    #[test]
    fn expired_for_reports_only_lapsed_bindings() {
        let a = addr("[fd00::a]:1000");
        let b = addr("[fd00::b]:1000");
        let life = DEFAULT_BINDING_LIFETIME.as_nanos() as u64;
        for filtering in [
            NatFiltering::Full,
            NatFiltering::AddressRestricted,
            NatFiltering::PortRestricted,
            NatFiltering::Symmetric,
        ] {
            let mut nat = NatModel::new(filtering, DEFAULT_BINDING_LIFETIME);
            assert!(!nat.expired_for(a, 0), "{filtering:?}: never opened");
            nat.on_outbound(a, 0);
            assert!(!nat.expired_for(a, life), "{filtering:?}: live at the edge");
            assert!(nat.expired_for(a, life + 1), "{filtering:?}: lapsed");
            assert!(!nat.admits(a, life + 1), "{filtering:?}: lapsed is dropped");
            if filtering == NatFiltering::PortRestricted {
                assert!(!nat.expired_for(b, life + 1), "{filtering:?}: never opened");
            }
            // Refreshing the binding keeps it from lapsing.
            nat.on_outbound(a, life);
            assert!(!nat.expired_for(a, life + 1), "{filtering:?}: refreshed");
            assert!(nat.admits(a, life + 1), "{filtering:?}: refreshed admits");
            assert!(
                nat.expired_for(a, 2 * life + 1),
                "{filtering:?}: lapses again"
            );
        }
    }
}
