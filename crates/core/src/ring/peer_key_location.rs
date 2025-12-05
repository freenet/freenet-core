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
