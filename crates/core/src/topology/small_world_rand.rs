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
}
