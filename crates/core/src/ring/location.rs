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
        Self::deterministic_loc(addr)
    }

    #[cfg(any(feature = "local-simulation", test))]
    pub fn from_address(_addr: &std::net::SocketAddr) -> Self {
        let random_component: f64 = rand::random();
        Location(random_component)
    }

    #[allow(unused)]
    fn deterministic_loc(addr: &std::net::SocketAddr) -> Self {
        match addr.ip() {
            std::net::IpAddr::V4(ipv4) => {
                let octets = ipv4.octets();
                let combined_octets = (u64::from(octets[0]) << 16)
                    | (u64::from(octets[1]) << 8)
                    | u64::from(octets[2]);
                let hashed = distribute_hash(combined_octets);
                Location(hashed as f64 / u64::MAX as f64)
            }
            std::net::IpAddr::V6(ipv6) => {
                let segments = ipv6.segments();
                let combined_segments = (u64::from(segments[0]) << 32)
                    | (u64::from(segments[1]) << 16)
                    | u64::from(segments[2]);
                let hashed = distribute_hash(combined_segments);
                Location(hashed as f64 / u64::MAX as f64)
            }
        }
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

/// A simple, non-cryptographic hash function for evenly distributing integer inputs.
///
/// # Purpose
/// This function generates a hash that evenly distributes input values, even if they
/// are sequential or exhibit patterns. It is **not cryptographically secure** but is
/// suitable for use cases such as hash tables, randomized ordering, or lookup keys.
///
/// # Supported Types
/// Works with any unsigned integer type (`u8`, `u16`, `u32`, `u64`, `u128`) and returns
/// a value of the **same type** as the input.
///
/// # Implementation
/// - Uses a series of bit-mixing operations (multiplication by large primes and XOR shifts).
/// - Inspired by techniques from MurmurHash and SplitMix64, optimized for speed and distribution.
fn distribute_hash<T: Copy + Into<u64> + From<u64>>(x: T) -> T {
    let mut h = x.into(); // Normalize input to u64
    h = h.wrapping_mul(0x9e3779b97f4a7c15); // Prime multiplier
    h ^= h >> 33;                          // XOR and shift
    h = h.wrapping_mul(0xc2b2ae3d27d4eb4f); // Second prime
    h ^= h >> 29;                          // XOR again
    h = h.wrapping_mul(0x85ebca6b2b2d3d1d); // Final prime
    h ^= h >> 32;                          // Final XOR
    T::from(h) // Convert back to the original type
}


#[cfg(test)]
mod test {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::*;

    #[test]
    fn test_ipv4_address_location_distribution() {
        use rand::prelude::*;
        use rand::rngs::StdRng;

        // Use seeded RNG for reproducible tests
        let mut rng = StdRng::seed_from_u64(12345);

        // Generate 100 random IP addresses wih seeded RNG
        let ips = (0..100)
            .map(|_| {
                let ip = Ipv4Addr::new(rng.gen(), rng.gen(), rng.gen(), rng.gen());
                SocketAddr::new(IpAddr::V4(ip), 12345)
            })
            .collect::<Vec<_>>();

        // Compute locations for each IP address
        let locations: Vec<f64> = ips
            .into_iter()
            .map(|addr| Location::deterministic_loc(&addr).0)
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
    fn test_ipv4_address_location() {
        let addresses = [
            SocketAddr::new(IpAddr::V4([86, 38, 75, 158].into()), 12345),
            SocketAddr::new(IpAddr::V4([103, 169, 0, 130].into()), 12345),
            SocketAddr::new(IpAddr::V4([20, 5, 226, 4].into()), 12345),
        ];

        let locations: Vec<Location> = addresses.iter().map(Location::deterministic_loc).collect();
        let expected_locations = vec![
            Location(0.9695508112571837),
            Location(0.6870191464639596),
            Location(0.31787443724464287),
        ];
        assert_eq!(locations, expected_locations);
    }

    #[test]
    fn test_ipv6_address_location() {
        let addresses = [
            SocketAddr::new(IpAddr::V6([0x2001, 0xdb8, 0x1234, 0x5678, 0xabcd, 0xef01, 0x2345, 0x6789].into()), 12345),
            SocketAddr::new(IpAddr::V6([0xfe80, 0x0000, 0x0000, 0x0000, 0x0202, 0xb3ff, 0xfe1e, 0x8329].into()), 12345),
            SocketAddr::new(IpAddr::V6([0x2001, 0x4860, 0x4860, 0x0000, 0x0000, 0x0000, 0x0000, 0x8888].into()), 12345),
        ];

        let locations: Vec<Location> = addresses.iter().map(Location::deterministic_loc).collect();
        let expected_locations = vec![
            Location(0.17218831909986057),
            Location(0.6061667671923302),
            Location(0.14432566176337014),
        ];
        assert_eq!(locations, expected_locations);
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
