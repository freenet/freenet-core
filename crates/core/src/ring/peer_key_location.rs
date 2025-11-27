use super::Location;
#[allow(unused_imports)] // PeerId still used in some conversions during migration
use crate::node::PeerId;
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
#[allow(dead_code)] // Will be used as refactoring progresses
pub enum PeerAddr {
    /// Address unknown - will be filled in by first recipient from packet source.
    /// Used when a peer doesn't know its own external address (e.g., behind NAT).
    Unknown,
    /// Known address - explicitly specified.
    Known(SocketAddr),
}

#[allow(dead_code)] // Will be used as refactoring progresses
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
/// The `location` field is kept for compatibility but should eventually be computed
/// from the address using `Location::from_address()`.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct PeerKeyLocation {
    /// The peer's cryptographic identity (public key).
    pub pub_key: TransportPublicKey,
    /// The peer's network address. May be Unknown if the peer doesn't know
    /// their own external address (e.g., behind NAT).
    pub peer_addr: PeerAddr,
    /// An unspecified location means that the peer hasn't been assigned a location, yet.
    /// Eventually this should be computed from addr instead of stored.
    pub location: Option<Location>,
}

impl PeerKeyLocation {
    /// Creates a new PeerKeyLocation with the given public key and known address.
    pub fn new(pub_key: TransportPublicKey, addr: SocketAddr) -> Self {
        PeerKeyLocation {
            pub_key,
            peer_addr: PeerAddr::Known(addr),
            location: None,
        }
    }

    /// Creates a new PeerKeyLocation with unknown address.
    /// Used when a peer doesn't know their own external address (e.g., behind NAT).
    pub fn with_unknown_addr(pub_key: TransportPublicKey) -> Self {
        PeerKeyLocation {
            pub_key,
            peer_addr: PeerAddr::Unknown,
            location: None,
        }
    }

    /// Creates a new PeerKeyLocation with the given public key and address, plus explicit location.
    pub fn with_location(
        pub_key: TransportPublicKey,
        addr: SocketAddr,
        location: Location,
    ) -> Self {
        PeerKeyLocation {
            pub_key,
            peer_addr: PeerAddr::Known(addr),
            location: Some(location),
        }
    }

    /// Returns a reference to the peer's public key (identity).
    #[inline]
    pub fn pub_key(&self) -> &TransportPublicKey {
        &self.pub_key
    }

    /// Returns the peer's socket address.
    ///
    /// # Panics
    /// Panics if the address is unknown. Use `socket_addr()` for a safe version.
    #[inline]
    pub fn addr(&self) -> SocketAddr {
        match &self.peer_addr {
            PeerAddr::Known(addr) => *addr,
            PeerAddr::Unknown => panic!(
                "addr() called on PeerKeyLocation with unknown address; use socket_addr() instead"
            ),
        }
    }

    /// Returns the peer's socket address if known, None otherwise.
    #[inline]
    pub fn socket_addr(&self) -> Option<SocketAddr> {
        match &self.peer_addr {
            PeerAddr::Known(addr) => Some(*addr),
            PeerAddr::Unknown => None,
        }
    }

    /// Returns the peer as a PeerId for compatibility with existing code.
    ///
    /// # Panics
    /// Panics if the address is unknown.
    #[inline]
    pub fn peer(&self) -> PeerId {
        PeerId::new(self.addr(), self.pub_key.clone())
    }

    /// Computes the ring location from the address if known.
    pub fn computed_location(&self) -> Option<Location> {
        match &self.peer_addr {
            PeerAddr::Known(addr) => Some(Location::from_address(addr)),
            PeerAddr::Unknown => None,
        }
    }

    /// Sets the address from a known socket address.
    /// Used when the first recipient fills in the sender's address from packet source.
    pub fn set_addr(&mut self, addr: SocketAddr) {
        self.peer_addr = PeerAddr::Known(addr);
    }

    /// Creates a PeerId from this PeerKeyLocation.
    /// Returns None if the address is unknown.
    #[allow(dead_code)] // Will be removed once PeerId is deprecated
    pub fn to_peer_id(&self) -> Option<PeerId> {
        match &self.peer_addr {
            PeerAddr::Known(addr) => Some(PeerId::new(*addr, self.pub_key.clone())),
            PeerAddr::Unknown => None,
        }
    }

    #[cfg(test)]
    pub fn random() -> Self {
        use crate::transport::TransportKeypair;
        use rand::Rng;

        let mut addr_bytes = [0u8; 4];
        rand::rng().fill(&mut addr_bytes[..]);
        let port = crate::util::get_free_port().unwrap();
        let addr = SocketAddr::from((addr_bytes, port));

        let pub_key = TransportKeypair::new().public().clone();

        PeerKeyLocation {
            pub_key,
            peer_addr: PeerAddr::Known(addr),
            location: Some(Location::random()),
        }
    }
}

impl From<PeerId> for PeerKeyLocation {
    fn from(peer: PeerId) -> Self {
        PeerKeyLocation {
            pub_key: peer.pub_key,
            peer_addr: PeerAddr::Known(peer.addr),
            location: None,
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
        match self.location {
            Some(loc) => write!(f, "{:?}@{} (@ {loc})", self.pub_key, self.peer_addr),
            None => write!(f, "{:?}@{}", self.pub_key, self.peer_addr),
        }
    }
}

#[cfg(test)]
impl<'a> arbitrary::Arbitrary<'a> for PeerKeyLocation {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let peer_id: PeerId = u.arbitrary()?;
        let location: Option<Location> = u.arbitrary()?;
        Ok(PeerKeyLocation {
            pub_key: peer_id.pub_key,
            peer_addr: PeerAddr::Known(peer_id.addr),
            location,
        })
    }
}
