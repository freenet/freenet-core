use crate::config::GlobalRng;
use crate::ring::{Distance, Location};

/// Minimum distance for Kleinberg sampling. On a ring with N peers uniformly
/// distributed, the expected distance to the nearest peer is ~1/(2N). We use
/// 0.01 which corresponds to a ~50 peer network. This produces ~59% of
/// connections within distance 0.1 (good locality) while ensuring enough
/// long-range connections for ring coverage in small networks.
const D_MIN: f64 = 0.01;

/// Maximum ring distance (half the ring).
const D_MAX: f64 = 0.5;

/// Number of logarithmic distance bands for Kleinberg distribution enforcement.
/// With D_MIN=0.01 and D_MAX=0.5, 4 bands gives roughly equal probability mass:
///   Band 0: [0.01, 0.024)  — very short
///   Band 1: [0.024, 0.056) — short
///   Band 2: [0.056, 0.13)  — medium
///   Band 3: [0.13, 0.5]    — long
pub(crate) const NUM_BANDS: usize = 4;

/// Generate a random link distance based on Kleinberg's d^{-1} distribution.
///
/// Used for small-world topology formation: most connections are short-range
/// with occasional long-range links, following an inverse power law.
///
/// The inverse CDF is: d = d_min * (d_max / d_min)^U where U ~ Uniform(0,1).
/// This produces samples uniform in log-distance space (each decade of distance
/// gets equal probability mass).
pub(crate) fn random_link_distance(d_min: Distance) -> Distance {
    let u: f64 = GlobalRng::with_rng(|rng| {
        use rand::Rng;
        rng.random()
    });

    let d = d_min.as_f64() * (D_MAX / d_min.as_f64()).powf(u);
    Distance::new(d)
}

/// Sample a target location from the Kleinberg 1/d distribution centered on
/// `my_location`. Returns a location on the ring biased toward short distances
/// but with a long tail for routing reachability.
pub(crate) fn kleinberg_target(my_location: Location) -> Location {
    let distance = random_link_distance(Distance::new(D_MIN));

    let sign: bool = GlobalRng::random_bool(0.5);
    let offset = if sign {
        distance.as_f64()
    } else {
        -distance.as_f64()
    };

    Location::new_rounded(my_location.as_f64() + offset)
}

/// Compute which logarithmic distance band a given distance falls into.
/// Returns `None` if the distance is outside [D_MIN, D_MAX].
///
/// Bands are uniform in log-distance space, matching the 1/d distribution
/// where each band should contain equal probability mass.
pub(crate) fn distance_band(distance: f64) -> Option<usize> {
    if !(D_MIN..=D_MAX).contains(&distance) {
        return None;
    }
    let log_ratio = (D_MAX / D_MIN).ln();
    let band = ((distance / D_MIN).ln() / log_ratio * NUM_BANDS as f64).floor() as usize;
    Some(band.min(NUM_BANDS - 1))
}

/// Score how much accepting a candidate at a given distance improves the
/// node's distance distribution toward the ideal 1/d (Kleinberg) distribution.
///
/// Returns a score in [0.0, 1.0] where higher means the candidate fills a
/// more deficient band. A candidate in the most under-represented band
/// scores 1.0; one in the most over-represented band scores close to 0.0.
///
/// `band_counts` is the number of existing connections in each of the
/// `NUM_BANDS` logarithmic distance bands.
pub(crate) fn kleinberg_band_score(
    candidate_distance: f64,
    band_counts: &[usize; NUM_BANDS],
) -> f64 {
    let band = match distance_band(candidate_distance) {
        Some(b) => b,
        // Connections outside [D_MIN, D_MAX] get a neutral score.
        // Very close connections (< D_MIN) are always valuable;
        // very far connections (> D_MAX) are never useful.
        None => {
            return if candidate_distance < D_MIN { 1.0 } else { 0.0 };
        }
    };

    // Ideal: each band gets equal share of connections in [D_MIN, D_MAX].
    // Connections outside [D_MIN, D_MAX] don't count against any band.
    let in_range: usize = band_counts.iter().sum();
    if in_range == 0 {
        return 1.0;
    }

    let ideal_per_band = in_range as f64 / NUM_BANDS as f64;
    let actual = band_counts[band] as f64;

    // Deficit ratio: how far below ideal this band is.
    // deficit > 0 means under-represented, deficit < 0 means over-represented.
    let deficit = (ideal_per_band - actual) / ideal_per_band.max(1.0);

    // Map deficit to [0, 1] score. deficit=1.0 (empty band) → score=1.0,
    // deficit=0.0 (at ideal) → score=0.5, deficit=-1.0 (2x over) → score=0.0
    (0.5 + deficit * 0.5).clamp(0.0, 1.0)
}

/// Count existing connections per logarithmic distance band relative to
/// `my_location`.
pub(crate) fn count_bands(
    my_location: Location,
    connection_locations: impl Iterator<Item = Location>,
) -> [usize; NUM_BANDS] {
    let mut counts = [0usize; NUM_BANDS];
    for loc in connection_locations {
        let dist = my_location.distance(loc).as_f64();
        if let Some(band) = distance_band(dist) {
            counts[band] += 1;
        }
    }
    counts
}

#[cfg(test)]
pub(super) mod test_utils {
    pub(in crate::topology) fn random_link_distance(
        d_min: crate::ring::Distance,
    ) -> crate::ring::Distance {
        super::random_link_distance(d_min)
    }
}

#[cfg(test)]
mod tests {
    use crate::ring::Distance;

    use super::*;
    use statrs::distribution::*;

    #[test]
    fn chi_squared_test() {
        let d_min = 0.01;
        let n = 10000;
        let num_bins = 20;
        let log_ratio = (D_MAX / d_min).ln();
        let mut bins = vec![0usize; num_bins];

        // Use log-spaced bins: the 1/d distribution is uniform in log-distance,
        // so each bin gets equal expected count (n/num_bins), making the
        // chi-squared test well-calibrated.
        for _ in 0..n {
            let d = random_link_distance(Distance::new(d_min)).as_f64();
            let bin_index = ((d / d_min).ln() / log_ratio * num_bins as f64).floor() as usize;
            if bin_index < num_bins {
                bins[bin_index] += 1;
            }
        }

        let expected = n as f64 / num_bins as f64;
        let chi_squared: f64 = bins
            .iter()
            .map(|&o| {
                let diff = o as f64 - expected;
                diff * diff / expected
            })
            .sum();

        let dof = num_bins - 1;
        let chi = ChiSquared::new(dof as f64).unwrap();
        let p_value = 1.0 - chi.cdf(chi_squared);

        assert!(
            p_value > 0.001,
            "Chi-squared test failed, p_value = {p_value}"
        );
    }

    #[test]
    fn kleinberg_target_produces_valid_locations() {
        let my_loc = Location::new(0.5);
        for _ in 0..1000 {
            let target = kleinberg_target(my_loc);
            let v = target.as_f64();
            assert!(
                (0.0..1.0).contains(&v),
                "Target location {v} outside valid ring range"
            );
        }
    }

    #[test]
    fn kleinberg_target_biases_toward_short_distances() {
        let my_loc = Location::new(0.5);
        let n = 5000;
        let mut short_count = 0;
        for _ in 0..n {
            let target = kleinberg_target(my_loc);
            let dist = my_loc.distance(target).as_f64();
            if dist < 0.1 {
                short_count += 1;
            }
        }
        // With 1/d distribution and d_min=0.01, d_max=0.5, the fraction
        // below 0.1 should be ln(0.1/0.01)/ln(0.5/0.01) ≈ 59%.
        // Use a conservative threshold.
        let ratio = short_count as f64 / n as f64;
        assert!(
            ratio > 0.5,
            "Expected majority of targets to be short-distance, got {ratio:.2}"
        );
    }

    #[test]
    fn kleinberg_target_produces_distinct_values() {
        let my_loc = Location::new(0.25);
        let targets: std::collections::BTreeSet<_> =
            (0..100).map(|_| kleinberg_target(my_loc)).collect();
        // Should produce many distinct values (no BTreeSet dedup problem)
        assert!(
            targets.len() > 80,
            "Expected diverse targets, got only {} unique out of 100",
            targets.len()
        );
    }

    #[test]
    fn kleinberg_target_wraps_correctly_near_boundary() {
        // Location near the ring boundary
        let my_loc = Location::new(0.95);
        for _ in 0..1000 {
            let target = kleinberg_target(my_loc);
            let v = target.as_f64();
            assert!(
                (0.0..1.0).contains(&v),
                "Target {v} outside valid range for boundary location"
            );
        }
    }

    #[test]
    fn distance_band_covers_full_range() {
        // Band boundaries should tile [D_MIN, D_MAX] without gaps
        assert_eq!(distance_band(0.01), Some(0));
        assert_eq!(distance_band(0.02), Some(0));
        assert_eq!(distance_band(0.05), Some(1));
        assert_eq!(distance_band(0.1), Some(2));
        assert_eq!(distance_band(0.3), Some(3));
        assert_eq!(distance_band(0.5), Some(3));
        // Outside range
        assert_eq!(distance_band(0.005), None);
        assert_eq!(distance_band(0.6), None);
    }

    #[test]
    fn band_score_favors_deficient_bands() {
        // All connections in band 3 (long range), none in band 0 (short)
        let counts = [0, 0, 0, 10];

        // Short-distance candidate should score high (fills deficit)
        let short_score = kleinberg_band_score(0.015, &counts);
        // Long-distance candidate should score low (over-represented)
        let long_score = kleinberg_band_score(0.4, &counts);

        assert!(
            short_score > long_score,
            "Short connection (score={short_score:.2}) should score higher than \
             long connection (score={long_score:.2}) when short bands are empty"
        );
        assert!(
            short_score > 0.8,
            "Empty band should score near 1.0, got {short_score:.2}"
        );
        assert!(
            long_score < 0.3,
            "Over-full band should score near 0.0, got {long_score:.2}"
        );
    }

    #[test]
    fn band_score_balanced_connections() {
        // Balanced distribution: 5 in each band
        let counts = [5, 5, 5, 5];

        // All bands at ideal, so all scores should be ~0.5
        let score = kleinberg_band_score(0.015, &counts);
        assert!(
            (0.4..=0.6).contains(&score),
            "Balanced band should score ~0.5, got {score:.2}"
        );
    }

    #[test]
    fn count_bands_correct() {
        let my_loc = Location::new(0.5);
        let peers = vec![
            Location::new(0.505), // distance 0.005 → outside D_MIN
            Location::new(0.515), // distance 0.015 → band 0
            Location::new(0.55),  // distance 0.05  → band 1
            Location::new(0.6),   // distance 0.1   → band 2
            Location::new(0.8),   // distance 0.3   → band 3
        ];
        let counts = count_bands(my_loc, peers.into_iter());
        assert_eq!(counts[0], 1); // 0.015
        assert_eq!(counts[1], 1); // 0.05
        assert_eq!(counts[2], 1); // 0.1
        assert_eq!(counts[3], 1); // 0.3
    }

    #[test]
    fn very_close_connections_always_valuable() {
        // Connections closer than D_MIN always score 1.0
        let counts = [5, 5, 5, 5];
        let score = kleinberg_band_score(0.005, &counts);
        assert_eq!(score, 1.0, "Very close connections should always score 1.0");
    }
}
