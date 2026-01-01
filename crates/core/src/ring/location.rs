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
    pub fn from_address(addr: &std::net::SocketAddr) -> Self {
        Self::deterministic_loc(addr)
    }

    fn deterministic_loc(addr: &std::net::SocketAddr) -> Self {
        match addr.ip() {
            std::net::IpAddr::V4(ipv4) => {
                let value: u32 = ipv4.into();
                // Mask out the last byte for sybil mitigation
                let masked_value = value & 0xFFFFFF00;
                let hashed = distribute_hash(masked_value as u64);
                Location(hashed as f64 / u64::MAX as f64)
            }
            std::net::IpAddr::V6(ipv6) => {
                let segments = ipv6.segments();
                // We only use the first 3 segments for sybil migation
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
        let mut rng = rand::rng();
        Location(rng.random_range(0.0..=1.0))
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
fn distribute_hash(x: u64) -> u64 {
    let mut h = x;
    h = h.wrapping_mul(0x517cc1b727220a95);
    h ^= h >> 32;
    h = h.wrapping_mul(0x4cf5ad432745937f);
    h ^= h >> 28;
    h = h.wrapping_mul(0x2f38a814cad5c4ed);
    h ^= h >> 31;
    h
}

#[cfg(test)]
mod test {
    use rstest::rstest;
    use std::hash::Hash;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    use super::*;

    // ============ Location construction tests ============

    #[rstest]
    #[case::middle(0.5, 0.5)]
    #[case::zero(0.0, 0.0)]
    #[case::one(1.0, 1.0)]
    #[case::quarter(0.25, 0.25)]
    fn test_location_new(#[case] input: f64, #[case] expected: f64) {
        let loc = Location::new(input);
        assert_eq!(loc.as_f64(), expected);
    }

    #[rstest]
    #[case::within_range(0.75, 0.75)]
    #[case::wraps_above_one(1.25, 0.25)]
    #[case::wraps_far_above(2.5, 0.5)]
    #[case::negative_wraps(- 0.25, 0.75)]
    #[case::negative_far_wraps(- 1.25, 0.75)]
    fn test_location_new_rounded(#[case] input: f64, #[case] expected: f64) {
        let loc = Location::new_rounded(input);
        assert!((loc.as_f64() - expected).abs() < f64::EPSILON);
    }

    #[rstest]
    #[case::valid_middle(0.5, true, Some(0.5))]
    #[case::valid_zero(0.0, true, Some(0.0))]
    #[case::valid_one(1.0, true, Some(1.0))]
    #[case::invalid_above(1.5, false, None)]
    #[case::invalid_below(- 0.1, false, None)]
    fn test_location_try_from(
        #[case] input: f64,
        #[case] should_succeed: bool,
        #[case] expected: Option<f64>,
    ) {
        let loc: Result<Location, _> = input.try_into();
        assert_eq!(loc.is_ok(), should_succeed);
        if let Some(exp) = expected {
            assert_eq!(loc.unwrap().as_f64(), exp);
        }
    }

    #[test]
    fn test_location_random_in_range() {
        // Generate multiple random locations and verify they're in range
        for _ in 0..100 {
            let loc = Location::random();
            assert!(loc.as_f64() >= 0.0 && loc.as_f64() <= 1.0);
        }
    }

    // ============ Location distance tests ============

    #[rstest]
    #[case::same_location(0.5, 0.5, 0.0)]
    #[case::adjacent(0.0, 0.25, 0.25)]
    #[case::wrap_around(0.0, 0.9, 0.1)]
    #[case::exactly_half(0.0, 0.5, 0.5)]
    #[case::middle_range(0.75, 0.50, 0.25)]
    fn test_location_distance(#[case] loc1: f64, #[case] loc2: f64, #[case] expected_dist: f64) {
        let l0 = Location::new(loc1);
        let l1 = Location::new(loc2);
        assert_eq!(l0.distance(l1), Distance::new(expected_dist));
    }

    #[rstest]
    #[case::forward(0.3, 0.7)]
    #[case::backward(0.7, 0.3)]
    #[case::near_boundary(0.1, 0.9)]
    fn test_location_distance_symmetry(#[case] loc1: f64, #[case] loc2: f64) {
        let l0 = Location::new(loc1);
        let l1 = Location::new(loc2);
        assert_eq!(l0.distance(l1), l1.distance(l0));
    }

    // ============ Location arithmetic tests ============

    #[rstest]
    #[case::middle(0.5, 0.1, 0.4, 0.6)]
    #[case::near_boundary(0.05, 0.1, - 0.05, 0.15)]
    #[case::at_zero(0.0, 0.2, - 0.2, 0.2)]
    fn test_location_add_distance(
        #[case] loc: f64,
        #[case] dist: f64,
        #[case] expected_neg: f64,
        #[case] expected_pos: f64,
    ) {
        let location = Location::new(loc);
        let distance = Distance::new(dist);
        let (neg, pos) = location + distance;

        assert!((neg.as_f64() - expected_neg).abs() < f64::EPSILON);
        assert!((pos.as_f64() - expected_pos).abs() < f64::EPSILON);
    }

    // ============ Location equality and ordering tests ============

    #[rstest]
    #[case::same_value(0.5, 0.5, true)]
    #[case::different_values(0.3, 0.7, false)]
    fn test_location_equality(#[case] val1: f64, #[case] val2: f64, #[case] expected_equal: bool) {
        let l1 = Location::new(val1);
        let l2 = Location::new(val2);
        assert_eq!(l1 == l2, expected_equal);
    }

    #[test]
    fn test_location_equality_with_epsilon() {
        // Locations should be equal if difference is less than f64::EPSILON
        let l1 = Location::new(0.5);
        let l2 = Location(0.5 + f64::EPSILON / 2.0);
        assert_eq!(l1, l2);
    }

    #[rstest]
    #[case::ascending(0.3, 0.7)]
    #[case::near_zero(0.0, 0.1)]
    #[case::near_one(0.8, 0.9)]
    fn test_location_ordering(#[case] smaller: f64, #[case] larger: f64) {
        let l1 = Location::new(smaller);
        let l2 = Location::new(larger);
        assert!(l1 < l2);
        assert!(l2 > l1);
    }

    #[test]
    fn test_location_sorting() {
        let mut locs = [
            Location::new(0.7),
            Location::new(0.2),
            Location::new(0.9),
            Location::new(0.1),
        ];
        locs.sort();

        assert_eq!(locs[0].as_f64(), 0.1);
        assert_eq!(locs[1].as_f64(), 0.2);
        assert_eq!(locs[2].as_f64(), 0.7);
        assert_eq!(locs[3].as_f64(), 0.9);
    }

    // ============ Location hashing tests ============

    #[test]
    fn test_location_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;

        let loc1 = Location::new(0.5);
        let loc2 = Location::new(0.5);

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();

        loc1.hash(&mut hasher1);
        loc2.hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_location_in_hashset() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(Location::new(0.1));
        set.insert(Location::new(0.2));
        set.insert(Location::new(0.1)); // duplicate

        assert_eq!(set.len(), 2);
    }

    // ============ Location display tests ============

    #[test]
    fn test_location_display() {
        let loc = Location::new(0.5);
        assert_eq!(format!("{}", loc), "0.5");
    }

    // ============ Distance tests ============

    #[rstest]
    #[case::valid_small(0.3, 0.3)]
    #[case::exactly_half(0.5, 0.5)]
    #[case::normalizes_above_half(0.7, 0.3)]
    #[case::zero(0.0, 0.0)]
    fn test_distance_new(#[case] input: f64, #[case] expected: f64) {
        let d = Distance::new(input);
        assert!((d.as_f64() - expected).abs() < 1e-10);
    }

    #[rstest]
    #[case::simple_sum(0.1, 0.2, 0.3)]
    #[case::wraps_above_half(0.3, 0.4, 0.3)]
    #[case::at_boundary(0.25, 0.25, 0.5)]
    fn test_distance_add(#[case] d1: f64, #[case] d2: f64, #[case] expected: f64) {
        let dist1 = Distance::new(d1);
        let dist2 = Distance::new(d2);
        let sum = dist1 + dist2;
        assert!((sum.as_f64() - expected).abs() < 1e-10);
    }

    #[rstest]
    #[case::same_value(0.25, 0.25, true)]
    #[case::different_values(0.1, 0.3, false)]
    fn test_distance_equality(#[case] val1: f64, #[case] val2: f64, #[case] expected_equal: bool) {
        let d1 = Distance::new(val1);
        let d2 = Distance::new(val2);
        assert_eq!(d1 == d2, expected_equal);
    }

    #[rstest]
    #[case::ascending(0.1, 0.3)]
    #[case::near_zero(0.0, 0.1)]
    fn test_distance_ordering(#[case] smaller: f64, #[case] larger: f64) {
        let d1 = Distance::new(smaller);
        let d2 = Distance::new(larger);
        assert!(d1 < d2);
    }

    #[test]
    fn test_distance_display() {
        let d = Distance::new(0.25);
        assert_eq!(format!("{}", d), "0.25");
    }

    // ============ Contract key location tests ============

    #[test]
    fn test_location_from_contract_key() {
        // Test that contract key bytes produce valid locations
        let bytes = [0x12, 0x34, 0x56, 0x78];
        let loc = Location::from_contract_key(&bytes);
        assert!(loc.as_f64() >= 0.0 && loc.as_f64() <= 1.0);
    }

    #[test]
    fn test_location_from_contract_key_consistency() {
        let bytes = [0xAB, 0xCD, 0xEF, 0x01];
        let loc1 = Location::from_contract_key(&bytes);
        let loc2 = Location::from_contract_key(&bytes);
        assert_eq!(loc1, loc2);
    }

    #[test]
    fn test_location_from_contract_key_different_bytes() {
        let bytes1 = [0x00, 0x00, 0x00, 0x01];
        let bytes2 = [0x00, 0x00, 0x00, 0x02];
        let loc1 = Location::from_contract_key(&bytes1);
        let loc2 = Location::from_contract_key(&bytes2);
        assert_ne!(loc1, loc2);
    }

    // ============ IPv4 address tests ============

    #[test]
    fn test_ipv4_address_location_distribution() {
        use rand::prelude::*;
        use rand::rngs::StdRng;

        // Use seeded RNG for reproducible tests
        let mut rng = StdRng::seed_from_u64(12345);

        // Generate 100 random IP addresses wih seeded RNG
        let ips = (0..100)
            .map(|_| {
                let ip = Ipv4Addr::new(rng.random(), rng.random(), rng.random(), rng.random());
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
                "Found too large gap ({gap}) between consecutive locations"
            );
        }

        // Also check wrap-around gap
        let wrap_gap = 1.0 - sorted_locs.last().unwrap() + sorted_locs[0];
        assert!(
            wrap_gap < max_acceptable_gap,
            "Found too large wrap-around gap ({wrap_gap})"
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
            Location(0.7110288569038304),
            Location(0.6202899973091933),
            Location(0.2587135150434003),
        ];
        assert_eq!(locations, expected_locations);
    }

    #[test]
    fn test_ipv4_last_byte_masked() {
        // Two IPs that differ only in last byte should have same location
        // (sybil mitigation)
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 12345);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 255)), 12345);

        let loc1 = Location::from_address(&addr1);
        let loc2 = Location::from_address(&addr2);

        assert_eq!(loc1, loc2);
    }

    #[test]
    fn test_ipv4_different_subnets() {
        // IPs in different /24 subnets should have different locations
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 12345);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 2, 1)), 12345);

        let loc1 = Location::from_address(&addr1);
        let loc2 = Location::from_address(&addr2);

        assert_ne!(loc1, loc2);
    }

    // ============ IPv6 address tests ============

    #[test]
    fn test_ipv6_address_location() {
        let addresses = [
            SocketAddr::new(
                IpAddr::V6(
                    [
                        0x2001, 0xdb8, 0x1234, 0x5678, 0xabcd, 0xef01, 0x2345, 0x6789,
                    ]
                    .into(),
                ),
                12345,
            ),
            SocketAddr::new(
                IpAddr::V6(
                    [
                        0xfe80, 0x0000, 0x0000, 0x0000, 0x0202, 0xb3ff, 0xfe1e, 0x8329,
                    ]
                    .into(),
                ),
                12345,
            ),
            SocketAddr::new(
                IpAddr::V6(
                    [
                        0x2001, 0x4860, 0x4860, 0x0000, 0x0000, 0x0000, 0x0000, 0x8888,
                    ]
                    .into(),
                ),
                12345,
            ),
        ];

        let locations: Vec<Location> = addresses.iter().map(Location::deterministic_loc).collect();
        let expected_locations = vec![
            Location(0.4539831101283351),
            Location(0.7201264112803492),
            Location(0.2243401485619054),
        ];
        assert_eq!(locations, expected_locations);
    }

    #[test]
    fn test_ipv6_uses_first_three_segments() {
        // Two IPv6 addresses with same first 3 segments should have same location
        let addr1 = SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0x1234, 0x0001, 0, 0, 0, 1)),
            12345,
        );
        let addr2 = SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(
                0x2001, 0xdb8, 0x1234, 0x9999, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF,
            )),
            12345,
        );

        let loc1 = Location::from_address(&addr1);
        let loc2 = Location::from_address(&addr2);

        assert_eq!(loc1, loc2);
    }

    #[test]
    fn test_ipv6_different_prefix() {
        // IPv6 addresses with different first 3 segments should have different locations
        let addr1 = SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0x1234, 0, 0, 0, 0, 1)),
            12345,
        );
        let addr2 = SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0x5678, 0, 0, 0, 0, 1)),
            12345,
        );

        let loc1 = Location::from_address(&addr1);
        let loc2 = Location::from_address(&addr2);

        assert_ne!(loc1, loc2);
    }

    // ============ Original tests ============

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
