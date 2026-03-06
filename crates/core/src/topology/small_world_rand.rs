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

/// Log ratio used for mapping distances to the unit interval in log-space.
const LOG_RATIO: f64 = 3.912_023_005_428_146; // (D_MAX / D_MIN).ln()

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

/// Map a ring distance to the unit interval in log-space.
///
/// The 1/d distribution is uniform in log-distance, so this transform maps
/// the ideal connection distribution to Uniform[0, 1]. Distances at or below
/// `D_MIN` map to 0; distances at or above `D_MAX` map to 1.
fn to_log_unit(distance: f64) -> f64 {
    if distance <= D_MIN {
        return 0.0;
    }
    if distance >= D_MAX {
        return 1.0;
    }
    (distance / D_MIN).ln() / LOG_RATIO
}

/// Target the center of the largest gap in the node's current connection
/// distribution in log-space, mapped back to a ring location.
///
/// With existing connections, finds the largest gap between consecutive points
/// in log-distance space (including boundaries 0 and 1), then targets the
/// midpoint of that gap with some jitter to avoid deterministic targeting.
///
/// Falls back to pure Kleinberg 1/d sampling when `connection_distances` is empty.
pub(crate) fn gap_target(
    my_location: Location,
    connection_distances: impl Iterator<Item = f64>,
) -> Location {
    // Collect and sort connection distances in log-space
    let mut points: Vec<f64> = connection_distances
        .filter(|&d| d >= D_MIN && d <= D_MAX)
        .map(|d| to_log_unit(d))
        .collect();

    if points.is_empty() {
        return kleinberg_target(my_location);
    }

    points.sort_by(|a, b| a.partial_cmp(b).unwrap());
    points.dedup();

    // Find all gaps and pick randomly among the largest ones
    let mut gaps: Vec<(f64, f64)> = Vec::new(); // (start, size)

    let mut prev = 0.0_f64;
    for &p in &points {
        gaps.push((prev, p - prev));
        prev = p;
    }
    gaps.push((prev, 1.0 - prev));

    let best_gap_size = gaps.iter().map(|(_, size)| *size).fold(0.0_f64, f64::max);

    // Collect all gaps within 1% of the best (floating point tolerance)
    let threshold = best_gap_size * 0.99;
    let largest_gaps: Vec<(f64, f64)> = gaps
        .into_iter()
        .filter(|(_, size)| *size >= threshold)
        .collect();

    // Pick one randomly
    let idx = GlobalRng::with_rng(|rng| {
        use rand::Rng;
        rng.random_range(0..largest_gaps.len())
    });
    let (best_gap_start, best_gap_size) = largest_gaps[idx];

    // Target the midpoint with ±25% jitter within the gap
    let midpoint = best_gap_start + best_gap_size / 2.0;
    let jitter_range = best_gap_size * 0.25;
    let jitter: f64 = GlobalRng::with_rng(|rng| {
        use rand::Rng;
        rng.random_range(-jitter_range..=jitter_range)
    });
    let u_target = (midpoint + jitter).clamp(0.0, 1.0);

    // Map back from log-space unit interval to a ring distance:
    // u = ln(d/D_MIN) / LOG_RATIO  =>  d = D_MIN * exp(u * LOG_RATIO)
    let distance = D_MIN * (u_target * LOG_RATIO).exp();

    let sign: bool = GlobalRng::random_bool(0.5);
    let offset = if sign { distance } else { -distance };

    Location::new_rounded(my_location.as_f64() + offset)
}

/// Score how much a candidate connection at `candidate_distance` improves the
/// node's distance distribution toward the ideal Kleinberg 1/d distribution,
/// given existing connections at `connection_distances`.
///
/// The score equals the distance from the candidate to its nearest neighbor
/// in log-space (including the boundaries 0 and 1). Higher scores mean the
/// candidate fills a larger gap in the distribution. A candidate that bisects
/// the largest gap perfectly gets the highest possible score.
///
/// Candidates outside [D_MIN, D_MAX] score 0 — they don't contribute to
/// small-world routing. This also serves as Sybil/eclipse defense: co-located
/// peers (distance < D_MIN) are never preferred by the scoring, preventing an
/// attacker from filling all connection slots with nearby nodes.
///
/// This is O(N) where N is the number of existing connections.
pub(crate) fn kleinberg_score(
    candidate_distance: f64,
    connection_distances: impl Iterator<Item = f64>,
) -> f64 {
    if !(D_MIN..=D_MAX).contains(&candidate_distance) {
        return 0.0;
    }

    let u_c = to_log_unit(candidate_distance);

    // Find the nearest neighbors below and above the candidate in log-space.
    // Start with boundaries 0 and 1.
    let mut nearest_below = 0.0_f64;
    let mut nearest_above = 1.0_f64;

    for d in connection_distances {
        let u = to_log_unit(d);
        if u <= u_c && u > nearest_below {
            nearest_below = u;
        } else if u > u_c && u < nearest_above {
            nearest_above = u;
        }
    }

    // Score = min distance to nearest neighbor in log-space.
    // A candidate in the middle of a large gap scores high.
    // A candidate next to an existing connection scores low.
    (u_c - nearest_below).min(nearest_above - u_c)
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
    fn log_ratio_is_correct() {
        let computed = (D_MAX / D_MIN).ln();
        assert!(
            (LOG_RATIO - computed).abs() < 1e-10,
            "LOG_RATIO constant {LOG_RATIO} doesn't match computed {computed}"
        );
    }

    #[test]
    fn to_log_unit_boundaries() {
        assert_eq!(to_log_unit(D_MIN), 0.0);
        assert_eq!(to_log_unit(D_MAX), 1.0);
        assert_eq!(to_log_unit(0.001), 0.0); // below D_MIN
        assert_eq!(to_log_unit(0.6), 1.0); // above D_MAX
    }

    #[test]
    fn to_log_unit_midpoint() {
        // Geometric mean of D_MIN and D_MAX should map to 0.5
        let geometric_mean = (D_MIN * D_MAX).sqrt();
        let u = to_log_unit(geometric_mean);
        assert!(
            (u - 0.5).abs() < 0.01,
            "Geometric mean should map to ~0.5, got {u}"
        );
    }

    #[test]
    fn score_no_connections() {
        // With no existing connections, candidate is bracketed by boundaries 0 and 1.
        // Geometric mean maps to 0.5, so min(0.5, 0.5) = 0.5.
        let geometric_mean = (D_MIN * D_MAX).sqrt();
        let score = kleinberg_score(geometric_mean, std::iter::empty());
        assert!(
            (score - 0.5).abs() < 0.01,
            "Midpoint candidate with no connections should score ~0.5, got {score}"
        );
    }

    #[test]
    fn score_center_of_gap_beats_edge() {
        // Existing connections near boundaries: leave a big gap in the middle.
        let existing = [0.015, 0.45]; // near D_MIN and near D_MAX

        // Candidate A: center of gap (geometric mean ≈ 0.07)
        let center = (0.015_f64 * 0.45).sqrt();
        let score_center = kleinberg_score(center, existing.iter().copied());

        // Candidate B: right next to an existing connection
        let score_edge = kleinberg_score(0.016, existing.iter().copied());

        assert!(
            score_center > score_edge,
            "Center candidate ({score_center:.4}) should beat edge candidate ({score_edge:.4})"
        );
    }

    #[test]
    fn score_fills_largest_gap() {
        // Connections clustered at short distances, large gap at long distances.
        let existing = [0.012, 0.015, 0.02];

        // Long-distance candidate fills the big gap
        let score_long = kleinberg_score(0.15, existing.iter().copied());
        // Short-distance candidate lands in a crowded region
        let score_short = kleinberg_score(0.014, existing.iter().copied());

        assert!(
            score_long > score_short,
            "Long-distance ({score_long:.4}) should beat short-distance ({score_short:.4}) \
             when short range is crowded"
        );
    }

    #[test]
    fn score_outside_range_is_zero() {
        let existing = [0.1];
        assert_eq!(kleinberg_score(0.005, existing.iter().copied()), 0.0);
        assert_eq!(kleinberg_score(0.6, existing.iter().copied()), 0.0);
    }

    #[test]
    fn score_perfectly_uniform_scores_equal() {
        // Place connections at equally-spaced log-distances (quartiles).
        // Each new candidate in a remaining gap should score roughly equally.
        let d_at = |u: f64| D_MIN * (D_MAX / D_MIN).powf(u);
        let existing = [d_at(0.25), d_at(0.5), d_at(0.75)];

        // Candidates at 0.125 and 0.625 are both centered in their respective gaps.
        let score_a = kleinberg_score(d_at(0.125), existing.iter().copied());
        let score_b = kleinberg_score(d_at(0.625), existing.iter().copied());

        assert!(
            (score_a - score_b).abs() < 0.01,
            "Equally-centered candidates should score equally: {score_a:.4} vs {score_b:.4}"
        );
        assert!(
            (score_a - 0.125).abs() < 0.01,
            "Expected score ~0.125, got {score_a:.4}"
        );
    }

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
    fn gap_target_fills_largest_gap() {
        let _guard = crate::config::GlobalRng::seed_guard(0xDEAD_BEEF);
        let my_loc = Location::new(0.5);

        // Existing connections clustered at short distances
        let existing = [0.012, 0.015, 0.02];

        let mut long_count = 0;
        let trials = 100;
        for _ in 0..trials {
            let target = gap_target(my_loc, existing.iter().copied());
            let dist = my_loc.distance(target).as_f64();
            if dist > 0.05 {
                long_count += 1;
            }
        }
        // The largest gap is in the long-distance range, so most targets
        // should be long-distance
        assert!(
            long_count > trials * 3 / 4,
            "Expected gap targeting to favor long-distance, got {long_count}/{trials}"
        );
    }

    #[test]
    fn gap_target_produces_valid_locations() {
        let _guard = crate::config::GlobalRng::seed_guard(0xCAFE_1234);
        let my_loc = Location::new(0.5);
        let existing = [0.05, 0.2];

        for _ in 0..1000 {
            let target = gap_target(my_loc, existing.iter().copied());
            let v = target.as_f64();
            assert!(
                (0.0..1.0).contains(&v),
                "Target location {v} outside valid ring range"
            );
        }
    }

    #[test]
    fn gap_target_no_connections_falls_back() {
        let _guard = crate::config::GlobalRng::seed_guard(0xABCD_5678);
        let my_loc = Location::new(0.5);

        // With no connections, should produce valid targets (falls back to kleinberg_target)
        for _ in 0..100 {
            let target = gap_target(my_loc, std::iter::empty());
            let v = target.as_f64();
            assert!(
                (0.0..1.0).contains(&v),
                "Fallback target {v} outside valid range"
            );
        }
    }

    #[test]
    fn gap_target_uniform_connections_spreads_evenly() {
        let _guard = crate::config::GlobalRng::seed_guard(0x1234_5678);
        let my_loc = Location::new(0.5);

        // Place connections at quartiles in log-space
        let d_at = |u: f64| D_MIN * (D_MAX / D_MIN).powf(u);
        let existing = [d_at(0.25), d_at(0.5), d_at(0.75)];

        // With uniform connections, the 4 gaps are equal. Targets should
        // spread across all gaps, not cluster.
        let mut short = 0; // distance < geometric_mean
        let geo_mean = (D_MIN * D_MAX).sqrt();
        let trials = 200;
        for _ in 0..trials {
            let target = gap_target(my_loc, existing.iter().copied());
            let dist = my_loc.distance(target).as_f64();
            if dist < geo_mean {
                short += 1;
            }
        }
        // Should be roughly 50/50 between short and long
        let ratio = short as f64 / trials as f64;
        assert!(
            (0.3..0.7).contains(&ratio),
            "Expected ~50/50 split, got {ratio:.2} short ratio"
        );
    }

    #[test]
    fn gap_target_all_distances_outside_range() {
        let _guard = crate::config::GlobalRng::seed_guard(0xFEED_FACE);
        let my_loc = Location::new(0.5);
        // All distances outside [D_MIN=0.01, D_MAX=0.5] — should fallback
        let existing = [0.001, 0.002, 0.7];
        for _ in 0..100 {
            let target = gap_target(my_loc, existing.iter().copied());
            let v = target.as_f64();
            assert!(
                (0.0..1.0).contains(&v),
                "Target {v} outside valid range when all distances out of range"
            );
        }
    }

    #[test]
    fn gap_target_single_connection() {
        let _guard = crate::config::GlobalRng::seed_guard(0xBAAD_F00D);
        let my_loc = Location::new(0.5);
        // Single short-distance connection — largest gap should be at long distance
        let existing = [0.015];
        let mut long_count = 0;
        let trials = 100;
        for _ in 0..trials {
            let target = gap_target(my_loc, existing.iter().copied());
            let dist = my_loc.distance(target).as_f64();
            if dist > 0.05 {
                long_count += 1;
            }
        }
        assert!(
            long_count > trials / 2,
            "Expected gap targeting to favor long-distance with single short connection, \
             got {long_count}/{trials}"
        );
    }

    #[test]
    fn gap_target_duplicate_distances() {
        let _guard = crate::config::GlobalRng::seed_guard(0xDEAD_C0DE);
        let my_loc = Location::new(0.5);
        // Multiple connections at the same distance should behave like one
        let single = [0.1];
        let dupes = [0.1, 0.1, 0.1];
        let mut targets_single = Vec::new();
        let mut targets_dupes = Vec::new();
        for _ in 0..200 {
            targets_single.push(gap_target(my_loc, single.iter().copied()));
        }
        for _ in 0..200 {
            targets_dupes.push(gap_target(my_loc, dupes.iter().copied()));
        }
        // Both should have similar distance distributions
        let avg_dist_single: f64 = targets_single
            .iter()
            .map(|t| my_loc.distance(*t).as_f64())
            .sum::<f64>()
            / 200.0;
        let avg_dist_dupes: f64 = targets_dupes
            .iter()
            .map(|t| my_loc.distance(*t).as_f64())
            .sum::<f64>()
            / 200.0;
        let diff = (avg_dist_single - avg_dist_dupes).abs();
        assert!(
            diff < 0.1,
            "Duplicate distances should behave like single: avg diff {diff:.4}"
        );
    }

    #[test]
    fn gap_target_wraps_near_boundary() {
        let _guard = crate::config::GlobalRng::seed_guard(0xCAFE_BABE);
        let my_loc = Location::new(0.95);
        let existing = [0.1, 0.3];
        for _ in 0..1000 {
            let target = gap_target(my_loc, existing.iter().copied());
            let v = target.as_f64();
            assert!(
                (0.0..1.0).contains(&v),
                "Target {v} outside valid range near boundary"
            );
        }
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
