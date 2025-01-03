use anyhow::bail;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use std::fmt::Display;
use std::hash::Hasher;
use std::ops::Add;

/// An abstract location on the 1D ring, represented by a real number on the interal [0, 1]
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct Location(pub(crate) f64);

impl Location {
    #[cfg(all(not(feature = "local-simulation"), not(test)))]
    pub fn from_address(addr: &std::net::SocketAddr) -> Self {
        match addr.ip() {
            std::net::IpAddr::V4(ipv4) => {
                let octets = ipv4.octets();
                let combined_octets = (u32::from(octets[0]) << 16)
                    | (u32::from(octets[1]) << 8)
                    | u32::from(octets[2]);
                Location(combined_octets as f64 / 16777215.0) // 2^24 - 1
            }
            std::net::IpAddr::V6(ipv6) => {
                let segments = ipv6.segments();
                let combined_segments = (u64::from(segments[0]) << 32)
                    | (u64::from(segments[1]) << 16)
                    | u64::from(segments[2]);
                Location(combined_segments as f64 / 281474976710655.0) // 2^48 - 1
            }
        }
    }

    #[cfg(any(feature = "local-simulation", test))]
    pub fn from_address(_addr: &std::net::SocketAddr) -> Self {
        let random_component: f64 = rand::random();
        Location(random_component)
    }

    pub fn new(location: f64) -> Self {
        debug_assert!(
            (0.0..=1.0).contains(&location),
            "Location must be in the range [0, 1]"
        );
        Location(location)
    }

    /// Returns a new location rounded to ensure it is between 0.0 and 1.0
    pub fn new_rounded(location: f64) -> Self {
        Self::new(location.rem_euclid(1.0))
    }

    /// Returns a new random location.
    pub fn random() -> Self {
        use rand::prelude::*;
        let mut rng = rand::thread_rng();
        Location(rng.gen_range(0.0..=1.0))
    }

    /// Compute the distance between two locations.
    pub fn distance(&self, other: impl std::borrow::Borrow<Location>) -> Distance {
        let d = (self.0 - other.borrow().0).abs();
        if d < 0.5f64 {
            Distance::new(d)
        } else {
            Distance::new(1.0f64 - d)
        }
    }

    pub fn as_f64(&self) -> f64 {
        self.0
    }

    pub(crate) fn from_contract_key(bytes: &[u8]) -> Self {
        let mut value = 0.0;
        let mut divisor = 256.0;
        for byte in bytes {
            value += *byte as f64 / divisor;
            divisor *= 256.0;
        }
        Location::try_from(value).expect("value should be between 0 and 1")
    }
}

impl std::ops::Add<Distance> for Location {
    type Output = (Location, Location);

    /// Returns the positive and directive locations on the ring  at the given distance.
    fn add(self, distance: Distance) -> Self::Output {
        let neg_loc = self.0 - distance.0;
        let pos_loc = self.0 + distance.0;
        (Location(neg_loc), Location(pos_loc))
    }
}

/// Ensure at compile time locations can only be constructed from well formed contract keys
/// (which have been hashed with a strong, cryptographically safe, hash function first).
impl From<&ContractKey> for Location {
    fn from(key: &ContractKey) -> Self {
        Self::from_contract_key(key.id().as_bytes())
    }
}

impl From<&ContractInstanceId> for Location {
    fn from(key: &ContractInstanceId) -> Self {
        Self::from_contract_key(key.as_bytes())
    }
}

impl Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialEq for Location {
    fn eq(&self, other: &Self) -> bool {
        (self.0 - other.0).abs() < f64::EPSILON
    }
}

/// Since we don't allow NaN values in the construction of Location
/// we can safely assume that an equivalence relation holds.  
impl Eq for Location {}

impl Ord for Location {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .expect("always should return a cmp value")
    }
}

impl PartialOrd for Location {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for Location {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let bits = self.0.to_bits();
        state.write_u64(bits);
    }
}

impl TryFrom<f64> for Location {
    type Error = anyhow::Error;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if !(0.0..=1.0).contains(&value) {
            bail!("expected a value between 0.0 and 1.0, received {}", value)
        } else {
            Ok(Location(value))
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Distance(f64);

impl Distance {
    pub fn new(value: f64) -> Self {
        debug_assert!(!value.is_nan(), "Distance cannot be NaN");
        debug_assert!(
            (0.0..=1.0).contains(&value),
            "Distance must be in the range [0, 1.0]"
        );
        if value <= 0.5 {
            Distance(value)
        } else {
            Distance(1.0 - value)
        }
    }

    pub fn as_f64(&self) -> f64 {
        self.0
    }
}

impl Add for Distance {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let d = self.0 + rhs.0;
        if d > 0.5 {
            Distance::new(1.0 - d)
        } else {
            Distance::new(d)
        }
    }
}

impl PartialEq for Distance {
    fn eq(&self, other: &Self) -> bool {
        (self.0 - other.0).abs() < f64::EPSILON
    }
}

impl PartialOrd for Distance {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Distance {}

impl Ord for Distance {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .expect("always should return a cmp value")
    }
}

impl Display for Distance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_ipv4_address_location() {
        use rand::Rng;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        let mut rng = rand::thread_rng();

        // Generate 100 random IP addresses
        let locations: Vec<f64> = (0..100)
            .map(|_| {
                let ip = Ipv4Addr::new(rng.gen(), rng.gen(), rng.gen(), rng.gen());
                let addr = SocketAddr::new(IpAddr::V4(ip), 12345);
                Location::from_address(&addr).0
            })
            .collect();

        // Verify all locations are between 0 and 1
        for loc in &locations {
            assert!(*loc >= 0.0 && *loc <= 1.0);
        }

        // Sort locations to check distribution
        let mut sorted_locs = locations.clone();
        sorted_locs.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // Check that locations are reasonably distributed
        // by ensuring the gaps between consecutive locations
        // aren't too large (shouldn't have huge empty spaces)
        let max_acceptable_gap = 0.2; // 20% of the ring
        for i in 1..sorted_locs.len() {
            let gap = sorted_locs[i] - sorted_locs[i - 1];
            assert!(
                gap < max_acceptable_gap,
                "Found too large gap ({}) between consecutive locations",
                gap
            );
        }

        // Also check wrap-around gap
        let wrap_gap = 1.0 - sorted_locs.last().unwrap() + sorted_locs[0];
        assert!(
            wrap_gap < max_acceptable_gap,
            "Found too large wrap-around gap ({})",
            wrap_gap
        );
    }

    #[test]
    fn location_dist() {
        let l0 = Location(0.);
        let l1 = Location(0.25);
        assert!(l0.distance(l1) == Distance(0.25));

        let l0 = Location(0.75);
        let l1 = Location(0.50);
        assert!(l0.distance(l1) == Distance(0.25));
    }
}
