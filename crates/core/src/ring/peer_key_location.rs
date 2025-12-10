use super::Location;
use crate::transport::TransportPublicKey;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::{fmt::Display, hash::Hash};

/// Explicit representation of peer address state.
///
/// This enum distinguishes between:
/// - `Unknown`: The address is not known by the sender (e.g., a peer doesn't know its own external address).
///   The first recipient should fill this in from the packet source address.
/// - `Known`: The address is known and explicitly specified.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash, Debug)]
pub enum PeerAddr {
    /// Address unknown - will be filled in by first recipient from packet source.
    /// Used when a peer doesn't know its own external address (e.g., behind NAT).
    Unknown,
    /// Known address - explicitly specified.
    Known(SocketAddr),
}

impl PeerAddr {
    /// Returns the socket address if known, None otherwise.
    pub fn as_known(&self) -> Option<&SocketAddr> {
        match self {
            PeerAddr::Known(addr) => Some(addr),
            PeerAddr::Unknown => None,
        }
    }

    /// Returns true if the address is known.
    pub fn is_known(&self) -> bool {
        matches!(self, PeerAddr::Known(_))
    }

    /// Returns true if the address is unknown.
    pub fn is_unknown(&self) -> bool {
        matches!(self, PeerAddr::Unknown)
    }
}

impl From<SocketAddr> for PeerAddr {
    fn from(addr: SocketAddr) -> Self {
        PeerAddr::Known(addr)
    }
}

impl std::fmt::Display for PeerAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerAddr::Unknown => write!(f, "<unknown>"),
            PeerAddr::Known(addr) => write!(f, "{}", addr),
        }
    }
}

/// The location of a peer in the ring. This location allows routing towards the peer.
///
/// # Identity vs Address
/// - `pub_key` is the true peer identity (cryptographic public key)
/// - `peer_addr` represents the network address, which may be unknown initially
///
/// # Location
/// Location is computed on-demand from the address using `Location::from_address()`.
/// Use the `location()` method to get the computed location.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct PeerKeyLocation {
    /// The peer's cryptographic identity (public key).
    pub pub_key: TransportPublicKey,
    /// The peer's network address. May be Unknown if the peer doesn't know
    /// their own external address (e.g., behind NAT).
    pub peer_addr: PeerAddr,
}

impl PeerKeyLocation {
    /// Creates a new PeerKeyLocation with the given public key and known address.
    pub fn new(pub_key: TransportPublicKey, addr: SocketAddr) -> Self {
        PeerKeyLocation {
            pub_key,
            peer_addr: PeerAddr::Known(addr),
        }
    }

    /// Creates a new PeerKeyLocation with unknown address.
    /// Used when a peer doesn't know their own external address (e.g., behind NAT).
    pub fn with_unknown_addr(pub_key: TransportPublicKey) -> Self {
        PeerKeyLocation {
            pub_key,
            peer_addr: PeerAddr::Unknown,
        }
    }

    /// Returns a reference to the peer's public key (identity).
    #[inline]
    pub fn pub_key(&self) -> &TransportPublicKey {
        &self.pub_key
    }

    /// Returns the peer's socket address if known, None otherwise.
    #[inline]
    pub fn socket_addr(&self) -> Option<SocketAddr> {
        match &self.peer_addr {
            PeerAddr::Known(addr) => Some(*addr),
            PeerAddr::Unknown => None,
        }
    }

    /// Computes the ring location from the address if known.
    /// Returns None if the address is unknown.
    #[inline]
    pub fn location(&self) -> Option<Location> {
        self.socket_addr().map(|addr| Location::from_address(&addr))
    }

    /// Sets the address from a known socket address.
    /// Used when the first recipient fills in the sender's address from packet source.
    pub fn set_addr(&mut self, addr: SocketAddr) {
        self.peer_addr = PeerAddr::Known(addr);
    }

    #[cfg(test)]
    pub fn random() -> Self {
        use crate::transport::{TransportKeypair, TransportPublicKey};
        use rand::Rng;
        use std::cell::RefCell;

        // Cache the keypair per thread to avoid expensive key generation in tests
        thread_local! {
            static CACHED_KEY: RefCell<Option<TransportPublicKey>> = const { RefCell::new(None) };
        }

        let mut rng = rand::rng();
        let mut addr_bytes = [0u8; 4];
        rng.fill(&mut addr_bytes[..]);
        // Use random port instead of get_free_port() for speed - tests don't actually bind
        let port: u16 = rng.random_range(1024..65535);
        let addr = SocketAddr::from((addr_bytes, port));

        let pub_key = CACHED_KEY.with(|cached| {
            let mut cached = cached.borrow_mut();
            match &*cached {
                Some(k) => k.clone(),
                None => {
                    let key = TransportKeypair::new().public().clone();
                    cached.replace(key.clone());
                    key
                }
            }
        });

        PeerKeyLocation {
            pub_key,
            peer_addr: PeerAddr::Known(addr),
        }
    }
}

impl Ord for PeerKeyLocation {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Order by address when both are known, otherwise order unknown addresses last
        match (&self.peer_addr, &other.peer_addr) {
            (PeerAddr::Known(a), PeerAddr::Known(b)) => a.cmp(b),
            (PeerAddr::Known(_), PeerAddr::Unknown) => std::cmp::Ordering::Less,
            (PeerAddr::Unknown, PeerAddr::Known(_)) => std::cmp::Ordering::Greater,
            (PeerAddr::Unknown, PeerAddr::Unknown) => std::cmp::Ordering::Equal,
        }
    }
}

impl PartialOrd for PeerKeyLocation {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Debug for PeerKeyLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Display>::fmt(self, f)
    }
}

impl Display for PeerKeyLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.location() {
            Some(loc) => write!(f, "{:?}@{} (@ {loc})", self.pub_key, self.peer_addr),
            None => write!(f, "{:?}@{}", self.pub_key, self.peer_addr),
        }
    }
}

#[cfg(test)]
thread_local! {
    static CACHED_PUB_KEY: std::cell::RefCell<Option<crate::transport::TransportPublicKey>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
impl<'a> arbitrary::Arbitrary<'a> for PeerKeyLocation {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        use crate::transport::TransportKeypair;
        let addr: SocketAddr = u.arbitrary()?;
        // Cache the public key to avoid expensive keypair generation on each call
        let pub_key = CACHED_PUB_KEY.with(|cached| {
            let mut cached = cached.borrow_mut();
            match &*cached {
                Some(k) => k.clone(),
                None => {
                    let key = TransportKeypair::new().public().clone();
                    cached.replace(key.clone());
                    key
                }
            }
        });
        Ok(PeerKeyLocation {
            pub_key,
            peer_addr: PeerAddr::Known(addr),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TransportKeypair;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    fn make_pub_key() -> TransportPublicKey {
        TransportKeypair::new().public().clone()
    }

    // ============ PeerAddr tests ============

    #[test]
    fn test_peer_addr_unknown() {
        let addr = PeerAddr::Unknown;
        assert!(addr.is_unknown());
        assert!(!addr.is_known());
        assert!(addr.as_known().is_none());
    }

    #[test]
    fn test_peer_addr_known() {
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080);
        let addr = PeerAddr::Known(socket);
        assert!(addr.is_known());
        assert!(!addr.is_unknown());
        assert_eq!(addr.as_known(), Some(&socket));
    }

    #[test]
    fn test_peer_addr_from_socket_addr() {
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5000);
        let addr: PeerAddr = socket.into();
        assert!(addr.is_known());
        assert_eq!(addr.as_known(), Some(&socket));
    }

    #[test]
    fn test_peer_addr_display() {
        let unknown = PeerAddr::Unknown;
        assert_eq!(format!("{}", unknown), "<unknown>");

        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9000);
        let known = PeerAddr::Known(socket);
        assert_eq!(format!("{}", known), "127.0.0.1:9000");
    }

    #[test]
    fn test_peer_addr_equality() {
        let socket1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), 1000);
        let socket2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), 1000);
        let socket3 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(5, 6, 7, 8)), 2000);

        assert_eq!(PeerAddr::Known(socket1), PeerAddr::Known(socket2));
        assert_ne!(PeerAddr::Known(socket1), PeerAddr::Known(socket3));
        assert_eq!(PeerAddr::Unknown, PeerAddr::Unknown);
        assert_ne!(PeerAddr::Unknown, PeerAddr::Known(socket1));
    }

    // ============ PeerKeyLocation tests ============

    #[test]
    fn test_peer_key_location_new() {
        let pub_key = make_pub_key();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)), 8000);
        let pkl = PeerKeyLocation::new(pub_key.clone(), addr);

        assert_eq!(pkl.pub_key(), &pub_key);
        assert_eq!(pkl.socket_addr(), Some(addr));
        assert!(pkl.location().is_some());
    }

    #[test]
    fn test_peer_key_location_with_unknown_addr() {
        let pub_key = make_pub_key();
        let pkl = PeerKeyLocation::with_unknown_addr(pub_key.clone());

        assert_eq!(pkl.pub_key(), &pub_key);
        assert!(pkl.socket_addr().is_none());
        assert!(pkl.location().is_none());
    }

    #[test]
    fn test_peer_key_location_set_addr() {
        let pub_key = make_pub_key();
        let mut pkl = PeerKeyLocation::with_unknown_addr(pub_key.clone());

        // Initially unknown
        assert!(pkl.socket_addr().is_none());
        assert!(pkl.location().is_none());

        // Set address
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 50)), 12345);
        pkl.set_addr(addr);

        // Now known
        assert_eq!(pkl.socket_addr(), Some(addr));
        assert!(pkl.location().is_some());
    }

    #[test]
    fn test_peer_key_location_location_computation() {
        let pub_key = make_pub_key();
        // Use addresses in different /24 subnets since last byte is masked for sybil mitigation
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 1, 1)), 5000);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 2, 1)), 5000);

        let pkl1 = PeerKeyLocation::new(pub_key.clone(), addr1);
        let pkl2 = PeerKeyLocation::new(pub_key.clone(), addr2);

        // Different /24 subnets should produce different locations
        let loc1 = pkl1.location().unwrap();
        let loc2 = pkl2.location().unwrap();
        assert_ne!(loc1, loc2);
    }

    #[test]
    fn test_peer_key_location_ordering_both_known() {
        let pub_key = make_pub_key();
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5000);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 5000);

        let pkl1 = PeerKeyLocation::new(pub_key.clone(), addr1);
        let pkl2 = PeerKeyLocation::new(pub_key.clone(), addr2);

        // Ordering should follow address ordering
        assert!(pkl1 < pkl2);
        assert!(pkl2 > pkl1);
    }

    #[test]
    fn test_peer_key_location_ordering_unknown_last() {
        let pub_key = make_pub_key();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080);

        let known = PeerKeyLocation::new(pub_key.clone(), addr);
        let unknown = PeerKeyLocation::with_unknown_addr(pub_key.clone());

        // Known addresses should sort before unknown
        assert!(known < unknown);
        assert!(unknown > known);
    }

    #[test]
    fn test_peer_key_location_ordering_both_unknown() {
        let pub_key1 = make_pub_key();
        let pub_key2 = make_pub_key();

        let unknown1 = PeerKeyLocation::with_unknown_addr(pub_key1);
        let unknown2 = PeerKeyLocation::with_unknown_addr(pub_key2);

        // Two unknowns should be equal in ordering (regardless of pub_key)
        assert_eq!(unknown1.cmp(&unknown2), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_peer_key_location_sorting() {
        let pub_key = make_pub_key();
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)), 5000);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5000);
        let addr3 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 5000);

        let mut peers = vec![
            PeerKeyLocation::with_unknown_addr(pub_key.clone()),
            PeerKeyLocation::new(pub_key.clone(), addr1),
            PeerKeyLocation::new(pub_key.clone(), addr2),
            PeerKeyLocation::with_unknown_addr(pub_key.clone()),
            PeerKeyLocation::new(pub_key.clone(), addr3),
        ];

        peers.sort();

        // Known addresses first (sorted), then unknowns
        assert_eq!(peers[0].socket_addr(), Some(addr2)); // 10.0.0.1
        assert_eq!(peers[1].socket_addr(), Some(addr3)); // 10.0.0.2
        assert_eq!(peers[2].socket_addr(), Some(addr1)); // 10.0.0.3
        assert!(peers[3].socket_addr().is_none()); // unknown
        assert!(peers[4].socket_addr().is_none()); // unknown
    }

    #[test]
    fn test_peer_key_location_display_with_known_addr() {
        let pub_key = make_pub_key();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9000);
        let pkl = PeerKeyLocation::new(pub_key, addr);

        let display = format!("{}", pkl);
        // Should contain the address and location
        assert!(display.contains("127.0.0.1:9000"));
        assert!(display.contains("@")); // location marker
    }

    #[test]
    fn test_peer_key_location_display_with_unknown_addr() {
        let pub_key = make_pub_key();
        let pkl = PeerKeyLocation::with_unknown_addr(pub_key);

        let display = format!("{}", pkl);
        assert!(display.contains("<unknown>"));
    }

    #[test]
    fn test_peer_key_location_equality() {
        let pub_key = make_pub_key();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080);

        let pkl1 = PeerKeyLocation::new(pub_key.clone(), addr);
        let pkl2 = PeerKeyLocation::new(pub_key.clone(), addr);

        assert_eq!(pkl1, pkl2);
    }

    #[test]
    fn test_peer_key_location_clone() {
        let pub_key = make_pub_key();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 20, 30, 40)), 5555);
        let pkl = PeerKeyLocation::new(pub_key, addr);
        let cloned = pkl.clone();

        assert_eq!(pkl, cloned);
        assert_eq!(pkl.socket_addr(), cloned.socket_addr());
    }

    #[test]
    fn test_peer_key_location_random() {
        // Test that random() generates valid PeerKeyLocations
        let pkl1 = PeerKeyLocation::random();
        let pkl2 = PeerKeyLocation::random();

        // Both should have known addresses
        assert!(pkl1.socket_addr().is_some());
        assert!(pkl2.socket_addr().is_some());

        // Addresses should differ (with very high probability)
        assert_ne!(pkl1.socket_addr(), pkl2.socket_addr());
    }

    #[test]
    fn test_peer_key_location_with_ipv6() {
        let pub_key = make_pub_key();
        let addr = SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
            8080,
        );
        let pkl = PeerKeyLocation::new(pub_key, addr);

        assert_eq!(pkl.socket_addr(), Some(addr));
        assert!(pkl.location().is_some());
    }
}
