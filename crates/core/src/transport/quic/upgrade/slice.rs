//! Which pairs of peers carry data on QUIC (#5940, step e5b-3).
//!
//! ```text
//! bucket(a, b) = first 8 bytes of BLAKE3("freenet-quic-data-slice-v1" || min(a, b) || max(a, b)),
//!                big-endian, mod 100
//! ```
//!
//! `a` and `b` are the two transport public keys, so both sides compute one
//! bucket with no message between them. A pair is in the slice at a percentage
//! when its bucket is below it, so the slices nest: a pair in at 5% stays in at
//! 25%, which keeps comparisons across releases meaningful. Changing this
//! function moves pairs between releases that disagree on it, so it is pinned by
//! a test vector.

use crate::transport::TransportPublicKey;

const DOMAIN: &[u8] = b"freenet-quic-data-slice-v1";

/// The bucket of a pair, in `0..100`, the same whichever key comes first.
pub(in crate::transport) fn bucket(a: &TransportPublicKey, b: &TransportPublicKey) -> u8 {
    let (low, high) = if a <= b { (a, b) } else { (b, a) };
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN);
    hasher.update(low.as_bytes());
    hasher.update(high.as_bytes());
    let mut first = [0u8; 8];
    hasher.finalize_xof().fill(&mut first);
    // Below 100, so it fits.
    (u64::from_be_bytes(first) % 100) as u8
}

/// Whether the pair is in the slice at `percent`. Nothing is at 0, and every
/// pair is at 100.
pub(in crate::transport) fn in_slice(
    a: &TransportPublicKey,
    b: &TransportPublicKey,
    percent: u8,
) -> bool {
    bucket(a, b) < percent
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(seed: u32) -> TransportPublicKey {
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&seed.to_be_bytes());
        // Spread over the bytes, so the order of two keys is not the seed's.
        let spread = blake3::hash(&seed.to_be_bytes());
        bytes[4..].copy_from_slice(&spread.as_bytes()[..28]);
        TransportPublicKey::from_bytes(bytes)
    }

    fn pairs() -> impl Iterator<Item = (TransportPublicKey, TransportPublicKey)> {
        (0..2000).map(|i| (key(2 * i), key(2 * i + 1)))
    }

    /// Computed outside this code: `b3sum` of the domain string, 32 bytes of 1
    /// and 32 bytes of 2 starts 8ff7886fbec687fb, which is 10373910280194656251,
    /// and that mod 100 is 51. Fails if the function changes in any way.
    #[test]
    fn the_function_has_this_test_vector() {
        let low = TransportPublicKey::from_bytes([1; 32]);
        let high = TransportPublicKey::from_bytes([2; 32]);
        assert_eq!(bucket(&low, &high), 51);
        assert_eq!(bucket(&high, &low), 51);
        assert!(!in_slice(&low, &high, 51));
        assert!(in_slice(&low, &high, 52));
    }

    /// Both sides compute one answer, whichever key is theirs, at every
    /// percentage.
    #[test]
    fn both_sides_compute_the_same_slice() {
        let mut orders = [0, 0];
        for (a, b) in pairs() {
            orders[usize::from(a < b)] += 1;
            assert_eq!(bucket(&a, &b), bucket(&b, &a));
            for percent in 0..=100 {
                assert_eq!(in_slice(&a, &b, percent), in_slice(&b, &a, percent));
            }
        }
        assert!(orders[0] > 0 && orders[1] > 0, "both orders were tried");
    }

    /// 0 holds nothing and 100 holds everything, exactly.
    #[test]
    fn zero_and_one_hundred_are_exact() {
        for (a, b) in pairs() {
            assert!(bucket(&a, &b) < 100);
            assert!(!in_slice(&a, &b, 0));
            assert!(in_slice(&a, &b, 100));
        }
    }

    /// A pair in at one percentage stays in at every higher one, and each slice
    /// holds about its share.
    #[test]
    fn slices_nest_and_hold_their_share() {
        for (a, b) in pairs() {
            let mut was_in = false;
            for percent in 0..=100 {
                let now_in = in_slice(&a, &b, percent);
                assert!(now_in || !was_in, "left the slice at {percent}");
                was_in = now_in;
            }
        }
        for (percent, low, high) in [(5, 60, 140), (25, 420, 580), (50, 900, 1100)] {
            let held = pairs().filter(|(a, b)| in_slice(a, b, percent)).count();
            assert!((low..=high).contains(&held), "{percent}%: {held} of 2000");
        }
    }

    /// One key does not decide the bucket: a node is in the slice with some
    /// peers and out with others.
    #[test]
    fn the_bucket_depends_on_both_keys() {
        let one = key(0);
        let buckets: std::collections::HashSet<u8> =
            (1..200).map(|i| bucket(&one, &key(i))).collect();
        assert!(buckets.len() > 50, "{buckets:?}");
    }
}
