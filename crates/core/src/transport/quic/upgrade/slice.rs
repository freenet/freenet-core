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

use std::time::Duration;

use crate::transport::TransportPublicKey;

const DOMAIN: &[u8] = b"freenet-quic-data-slice-v1";

const HOUR: u64 = 3600;

/// The share of pairs, in percent, carrying data `elapsed` after a release's ramp
/// started (`policy::QUIC_DATA_RAMP_START`): 5% for the first 6 h, then linear to
/// 25% at 12 h, then linear to 100% at 24 h, and 100% after. Rounded down, so a
/// value is reached at or after its time and never before. It never falls as
/// `elapsed` grows, so by [`in_slice`] a pair in the slice stays in. A node whose
/// clock is before the start reads `elapsed` as 0 and gets the first value.
/// Pure: the time is the caller's.
pub(in crate::transport) fn ramp_percent(elapsed: Duration) -> u8 {
    let secs = elapsed.as_secs();
    let between = |from: u64, to: u64, low: u64, high: u64| {
        let span = (to - from) * HOUR;
        low + (high - low) * (secs - from * HOUR) / span
    };
    let percent = match secs {
        s if s < 6 * HOUR => 5,
        s if s < 12 * HOUR => between(6, 12, 5, 25),
        s if s < 24 * HOUR => between(12, 24, 25, 100),
        _ => 100,
    };
    // At most 100 by the arms above.
    percent as u8
}

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

    fn hours(h: u64) -> Duration {
        Duration::from_secs(h * HOUR)
    }

    /// The curve at each breakpoint and between them.
    #[test]
    fn the_ramp_has_these_values() {
        for (at, percent) in [
            (Duration::ZERO, 5),
            (hours(3), 5),
            (hours(6) - Duration::from_secs(1), 5),
            (hours(6), 5),
            (hours(9), 15),
            (hours(12) - Duration::from_secs(1), 24),
            (hours(12), 25),
            (hours(18), 62),
            (hours(24) - Duration::from_secs(1), 99),
            (hours(24), 100),
            (hours(1000), 100),
            (Duration::MAX, 100),
        ] {
            assert_eq!(ramp_percent(at), percent, "{at:?}");
        }
    }

    /// It never falls, so a pair in the slice at one time is in at every later
    /// time, and it covers 5 to 100.
    #[test]
    fn the_ramp_never_falls_and_pairs_stay_in() {
        let mut last = 0;
        for minute in 0..=(25 * 60) {
            let percent = ramp_percent(Duration::from_secs(minute * 60));
            assert!(percent >= last, "minute {minute}: {percent} after {last}");
            last = percent;
        }
        assert_eq!(last, 100);
        let pairs: Vec<_> = pairs().take(500).collect();
        let mut was_in = vec![false; pairs.len()];
        for minute in (0..=(25 * 60)).step_by(7) {
            let percent = ramp_percent(Duration::from_secs(minute * 60));
            for (i, (a, b)) in pairs.iter().enumerate() {
                let now_in = in_slice(a, b, percent);
                assert!(
                    now_in || !was_in[i],
                    "pair {i} left the slice at minute {minute}"
                );
                was_in[i] = now_in;
            }
        }
        assert!(was_in.iter().all(|in_now| *in_now));
    }

    fn key(seed: u32) -> TransportPublicKey {
        // Hashed, so the order of two keys is not the order of their seeds.
        TransportPublicKey::from_bytes(*blake3::hash(&seed.to_be_bytes()).as_bytes())
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
