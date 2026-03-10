use crate::config::GlobalRng;
use crate::ring::{Distance, Location};

/// Practical minimum distance for Kleinberg targeting and log-space gap analysis.
/// Set to 0.001 (one thousandth of the ring).
///
/// This value balances two concerns:
/// - Small enough that close-neighbor connections (even at distance 0.001) are
///   supported. Real peers are never excluded from scoring.
/// - Large enough that the log-space range (~6.2 decades) provides good gap
///   discrimination. Too small a value (e.g. 1e-6) creates a 13-decade range
///   where all real-network connections cluster in the top 30%, making gap
///   analysis unable to distinguish close vs long-range candidates.
///
/// NOTE: This is NOT a scoring floor. `kleinberg_score` scores all positive
/// distances — there is no dead zone that prevents close-neighbor connections.
/// Distances below D_MIN_TARGET are simply clamped to 0.0 in log-space.
const D_MIN_TARGET: f64 = 0.001;

/// Maximum ring distance (half the ring).
const D_MAX: f64 = 0.5;

/// Log ratio for targeting: ln(D_MAX / D_MIN_TARGET).
const LOG_RATIO: f64 = 6.214_608_098_422_191; // (D_MAX / D_MIN_TARGET).ln()

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
    let distance = random_link_distance(Distance::new(D_MIN_TARGET));

    let sign: bool = GlobalRng::random_bool(0.5);
    let offset = if sign {
        distance.as_f64()
    } else {
        -distance.as_f64()
    };

    Location::new_rounded(my_location.as_f64() + offset)
}

/// Map a ring distance to the unit interval in log-space for gap analysis.
///
/// Uses D_MIN_TARGET as the lower bound for the log-space mapping.
/// Distances at or below D_MIN_TARGET map to 0; distances at or above D_MAX
/// map to 1.
fn to_log_unit(distance: f64) -> f64 {
    if distance <= D_MIN_TARGET {
        return 0.0;
    }
    if distance >= D_MAX {
        return 1.0;
    }
    (distance / D_MIN_TARGET).ln() / LOG_RATIO
}

/// Result of analyzing the gap distribution in log-distance space.
///
/// Contains the sorted gaps between consecutive connection points
/// (including boundaries 0 and 1) and the size of the largest gap.
struct GapAnalysis {
    /// All gaps as (start, size) pairs, sorted by position.
    gaps: Vec<(f64, f64)>,
    /// Size of the largest gap (in [0, 1] log-space units).
    largest_gap_size: f64,
    /// Number of distinct in-range points used for the analysis.
    point_count: usize,
}

/// Analyze the gap distribution of connection distances in log-space.
///
/// Returns `None` if there are no valid distances in (D_MIN_TARGET, D_MAX].
fn analyze_gaps(connection_distances: impl Iterator<Item = f64>) -> Option<GapAnalysis> {
    let mut points: Vec<f64> = connection_distances
        .filter(|&d| d > D_MIN_TARGET && d <= D_MAX)
        .map(to_log_unit)
        .collect();

    if points.is_empty() {
        return None;
    }

    points.sort_by(|a, b| a.partial_cmp(b).unwrap());
    points.dedup();

    let mut gaps: Vec<(f64, f64)> = Vec::new();
    let mut prev = 0.0_f64;
    for &p in &points {
        gaps.push((prev, p - prev));
        prev = p;
    }
    gaps.push((prev, 1.0 - prev));

    let largest_gap_size = gaps.iter().map(|(_, size)| *size).fold(0.0_f64, f64::max);
    let point_count = points.len();

    Some(GapAnalysis {
        gaps,
        largest_gap_size,
        point_count,
    })
}

/// Compute the size of the largest gap in the connection distribution
/// in log-distance space, normalized to [0, 1]. Also returns the number
/// of distinct in-range points used for the analysis (for the expected
/// gap formula `ln(k)/k`).
///
/// With k connections ideally distributed (uniform in log-space), the expected
/// largest gap is approximately ln(k)/k. A return value significantly larger
/// than that indicates the topology has room for improvement.
///
/// Returns `(gap_size, point_count)` where `point_count` is the number of
/// distinct in-range distances used. Returns `(1.0, 0)` when there are no
/// connections in the valid range.
pub(crate) fn largest_gap_size(connection_distances: impl Iterator<Item = f64>) -> (f64, usize) {
    analyze_gaps(connection_distances)
        .map(|a| (a.largest_gap_size, a.point_count))
        .unwrap_or((1.0, 0))
}

/// Pick a distance by targeting the largest gap's midpoint (with jitter) in
/// a `GapAnalysis`, mapping back from log-space to a ring distance.
///
/// Returns `None` if the analysis has no gaps (shouldn't happen with valid input).
fn distance_from_gap_analysis(analysis: GapAnalysis) -> f64 {
    // Collect all gaps within 1% of the best (floating point tolerance)
    let threshold = analysis.largest_gap_size * 0.99;
    let largest_gaps: Vec<(f64, f64)> = analysis
        .gaps
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
    // u = ln(d/D_MIN_TARGET) / LOG_RATIO  =>  d = D_MIN_TARGET * exp(u * LOG_RATIO)
    D_MIN_TARGET * (u_target * LOG_RATIO).exp()
}

/// Target the center of the largest gap in the node's current connection
/// distribution in log-space, mapped back to a ring location.
///
/// With existing connections, finds the largest gap between consecutive points
/// in log-distance space (including boundaries 0 and 1), then targets the
/// midpoint of that gap with some jitter to avoid deterministic targeting.
///
/// Falls back to pure Kleinberg 1/d sampling when `connection_distances` is empty.
#[cfg(test)]
fn gap_target(my_location: Location, connection_distances: impl Iterator<Item = f64>) -> Location {
    let analysis = match analyze_gaps(connection_distances) {
        Some(a) => a,
        None => return kleinberg_target(my_location),
    };

    let distance = distance_from_gap_analysis(analysis);

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
/// All positive distances up to D_MAX are scored — there is no minimum
/// distance floor. This ensures close neighbors are never excluded from
/// connection acceptance decisions.
///
/// This is O(N) where N is the number of existing connections.
pub(crate) fn kleinberg_score(
    candidate_distance: f64,
    connection_distances: impl Iterator<Item = f64>,
) -> f64 {
    if candidate_distance <= 0.0 || candidate_distance > D_MAX {
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

/// Compute the gap that would exist in log-space if a peer at `peer_distance`
/// were removed from the set of `all_distances`.
///
/// Returns the size of the merged gap (in [0, 1] log-space units) that would
/// result from removing this peer. This measures how topologically important
/// the peer is: a large removal gap means the peer fills a critical position
/// in the Kleinberg distribution.
///
/// Returns 0.0 if the peer distance is outside the valid range or not found.
///
/// This is O(N) where N is the number of distances.
pub(crate) fn removal_gap(peer_distance: f64, all_distances: impl Iterator<Item = f64>) -> f64 {
    if peer_distance <= 0.0 || peer_distance > D_MAX {
        return 0.0;
    }

    let u_peer = to_log_unit(peer_distance);

    // Find the nearest neighbors below and above the peer in log-space,
    // skipping the peer itself. Same approach as kleinberg_score but
    // excludes the target point rather than including it.
    let mut below = 0.0_f64;
    let mut above = 1.0_f64;

    for d in all_distances {
        let u = to_log_unit(d);
        // Skip the peer itself (within floating point tolerance)
        if (u - u_peer).abs() < 1e-12 {
            continue;
        }
        if u < u_peer && u > below {
            below = u;
        }
        if u > u_peer && u < above {
            above = u;
        }
    }

    above - below
}

// ======================== Directional (CW/CCW) variants ========================

/// Split signed distances into (clockwise, counterclockwise) absolute distance vectors.
///
/// CW = positive signed distances, CCW = negative (stored as positive f64).
/// d=0.5 (antipode) goes into CW by convention (signed_distance returns +0.5).
fn split_by_direction(signed_distances: &[f64]) -> (Vec<f64>, Vec<f64>) {
    let mut cw = Vec::new();
    let mut ccw = Vec::new();
    for &sd in signed_distances {
        if sd >= 0.0 {
            cw.push(sd);
        } else {
            ccw.push(-sd); // store as positive absolute distance
        }
    }
    (cw, ccw)
}

/// Target the largest gap across both half-rings, preferring the side with worse coverage.
///
/// Runs independent gap analysis on CW and CCW distances. Selects which side to
/// target with probability proportional to that side's largest gap size (soft
/// deficit weighting to avoid noisy hard-switching at low degree). Then targets
/// the midpoint of the largest gap on the chosen side.
///
/// Falls back to `kleinberg_target` when there are no valid distances on either side.
pub(crate) fn gap_target_directional(my_location: Location, signed_distances: &[f64]) -> Location {
    let (cw_dists, ccw_dists) = split_by_direction(signed_distances);

    let cw_analysis = analyze_gaps(cw_dists.into_iter());
    let ccw_analysis = analyze_gaps(ccw_dists.into_iter());

    // If both sides are empty, fall back to pure Kleinberg sampling
    if cw_analysis.is_none() && ccw_analysis.is_none() {
        return kleinberg_target(my_location);
    }

    let cw_gap = cw_analysis
        .as_ref()
        .map(|a| a.largest_gap_size)
        .unwrap_or(1.0);
    let ccw_gap = ccw_analysis
        .as_ref()
        .map(|a| a.largest_gap_size)
        .unwrap_or(1.0);

    // Soft weighting: probability of choosing a side proportional to its gap size.
    let pick_cw_prob = cw_gap / (cw_gap + ccw_gap);
    let pick_cw = GlobalRng::random_bool(pick_cw_prob);

    let (analysis, sign) = if pick_cw {
        (cw_analysis, 1.0)
    } else {
        (ccw_analysis, -1.0)
    };

    let distance = match analysis {
        Some(a) => distance_from_gap_analysis(a),
        None => {
            // This side has no connections — sample from full Kleinberg range
            random_link_distance(Distance::new(D_MIN_TARGET)).as_f64()
        }
    };

    Location::new_rounded(my_location.as_f64() + sign * distance)
}

/// Score how much a candidate improves the node's distribution, considering direction.
///
/// Evaluates gap-fill quality on the candidate's own half-ring, with a bonus
/// for the side that has fewer connections (deficit weighting).
pub(crate) fn kleinberg_score_directional(
    candidate_signed_distance: f64,
    signed_distances: &[f64],
) -> f64 {
    let candidate_abs = candidate_signed_distance.abs();
    if candidate_abs <= 0.0 || candidate_abs > D_MAX {
        return 0.0;
    }

    let (cw_dists, ccw_dists) = split_by_direction(signed_distances);
    let is_cw = candidate_signed_distance >= 0.0;

    let (same_side, other_side) = if is_cw {
        (&cw_dists, &ccw_dists)
    } else {
        (&ccw_dists, &cw_dists)
    };

    // Base score: gap-fill quality on the candidate's own side
    let base = kleinberg_score(candidate_abs, same_side.iter().copied());

    let total = same_side.len() + other_side.len();
    if total == 0 {
        return base;
    }

    // deficit_ratio: 0.0 when balanced, up to 1.0 when all connections on other side
    let actual_fraction = same_side.len() as f64 / total as f64;
    let deficit = (0.5 - actual_fraction).max(0.0) / 0.5;

    // Boost by up to 50% for a fully deficit side
    base * (1.0 + 0.5 * deficit)
}

/// Compute the gap that would exist on a peer's half-ring if it were removed.
///
/// Only considers connections on the same side (CW or CCW) as the peer being
/// evaluated, giving a directionally-aware measure of topological importance.
pub(crate) fn removal_gap_directional(peer_signed_distance: f64, signed_distances: &[f64]) -> f64 {
    let peer_abs = peer_signed_distance.abs();
    let is_cw = peer_signed_distance >= 0.0;

    let (cw_dists, ccw_dists) = split_by_direction(signed_distances);
    let same_side = if is_cw { cw_dists } else { ccw_dists };

    removal_gap(peer_abs, same_side.into_iter())
}

/// Compute the largest gap across both half-rings (returns the worse side).
///
/// Also returns the total point count across both sides for expected-gap formulas.
pub(crate) fn largest_gap_size_directional(signed_distances: &[f64]) -> (f64, usize) {
    let (cw_dists, ccw_dists) = split_by_direction(signed_distances);
    let (cw_gap, cw_count) = largest_gap_size(cw_dists.into_iter());
    let (ccw_gap, ccw_count) = largest_gap_size(ccw_dists.into_iter());
    // Return the worse side's gap and total point count
    let gap = cw_gap.max(ccw_gap);
    (gap, cw_count + ccw_count)
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
        let computed = (D_MAX / D_MIN_TARGET).ln();
        assert!(
            (LOG_RATIO - computed).abs() < 1e-10,
            "LOG_RATIO constant {LOG_RATIO} doesn't match computed {computed}"
        );
    }

    #[test]
    fn to_log_unit_boundaries() {
        assert_eq!(to_log_unit(D_MIN_TARGET), 0.0);
        assert_eq!(to_log_unit(D_MAX), 1.0);
        assert_eq!(to_log_unit(1e-4), 0.0); // below D_MIN_TARGET
        assert_eq!(to_log_unit(0.6), 1.0); // above D_MAX
    }

    #[test]
    fn to_log_unit_midpoint() {
        // Geometric mean of D_MIN_TARGET and D_MAX should map to 0.5
        let geometric_mean = (D_MIN_TARGET * D_MAX).sqrt();
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
        let geometric_mean = (D_MIN_TARGET * D_MAX).sqrt();
        let score = kleinberg_score(geometric_mean, std::iter::empty());
        assert!(
            (score - 0.5).abs() < 0.01,
            "Midpoint candidate with no connections should score ~0.5, got {score}"
        );
    }

    #[test]
    fn score_center_of_gap_beats_edge() {
        // Existing connections near boundaries: leave a big gap in the middle.
        let existing = [0.015, 0.45]; // near short and near D_MAX

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
        // Zero and negative distances score 0
        assert_eq!(kleinberg_score(0.0, existing.iter().copied()), 0.0);
        assert_eq!(kleinberg_score(-0.1, existing.iter().copied()), 0.0);
        // Above D_MAX scores 0
        assert_eq!(kleinberg_score(0.6, existing.iter().copied()), 0.0);
    }

    #[test]
    fn score_very_small_distance_is_nonzero() {
        // With no D_MIN floor, small distances should score > 0
        // (D_MIN_TARGET=0.001, so distances above that map to positive log-space)
        let existing = [0.1];
        let score = kleinberg_score(0.002, existing.iter().copied());
        assert!(
            score > 0.0,
            "Small distance (0.002) should score > 0, got {score}"
        );
        let score_small = kleinberg_score(0.005, existing.iter().copied());
        assert!(
            score_small > 0.0,
            "Small distance (0.005) should score > 0, got {score_small}"
        );
    }

    #[test]
    fn score_perfectly_uniform_scores_equal() {
        // Place connections at equally-spaced log-distances (quartiles).
        // Each new candidate in a remaining gap should score roughly equally.
        let d_at = |u: f64| D_MIN_TARGET * (D_MAX / D_MIN_TARGET).powf(u);
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
        let d_min = D_MIN_TARGET;
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

        // Place connections clustered in the upper quarter of log-space
        // (near D_MAX). This leaves a large gap in the lower 3/4 of log-space.
        let d_at = |u: f64| D_MIN_TARGET * (D_MAX / D_MIN_TARGET).powf(u);
        let existing = [d_at(0.80), d_at(0.85), d_at(0.90)];

        // The largest gap is below the cluster, so most targets should be
        // at shorter distances than the cluster.
        let cluster_lower = d_at(0.80);
        let mut below_cluster = 0;
        let trials = 100;
        for _ in 0..trials {
            let target = gap_target(my_loc, existing.iter().copied());
            let dist = my_loc.distance(target).as_f64();
            if dist < cluster_lower {
                below_cluster += 1;
            }
        }
        assert!(
            below_cluster > trials * 3 / 4,
            "Expected gap targeting to favor the large gap below the cluster, got {below_cluster}/{trials}"
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
        let d_at = |u: f64| D_MIN_TARGET * (D_MAX / D_MIN_TARGET).powf(u);
        let existing = [d_at(0.25), d_at(0.5), d_at(0.75)];

        // With uniform connections, the 4 gaps are equal. Targets should
        // spread across all gaps, not cluster.
        let mut short = 0; // distance < geometric_mean
        let geo_mean = (D_MIN_TARGET * D_MAX).sqrt();
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
        // All distances outside (D_MIN_TARGET, D_MAX] — should fallback
        let existing = [0.0, 1e-4, 0.7];
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
        // Single connection at log-space midpoint — two equal gaps on either side.
        // Targets should spread across both gaps (roughly 50/50).
        let geo_mean = (D_MIN_TARGET * D_MAX).sqrt();
        let existing = [geo_mean];
        let mut above_count = 0;
        let trials = 200;
        for _ in 0..trials {
            let target = gap_target(my_loc, existing.iter().copied());
            let dist = my_loc.distance(target).as_f64();
            if dist > geo_mean {
                above_count += 1;
            }
        }
        let ratio = above_count as f64 / trials as f64;
        assert!(
            (0.3..0.7).contains(&ratio),
            "Expected ~50/50 split around midpoint connection, got {ratio:.2}"
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
        // With 1/d distribution and d_min_target=0.001, d_max=0.5, the fraction
        // below 0.1 should be ln(0.1/0.001)/ln(0.5/0.001) ≈ 74.1%.
        // Use a conservative threshold.
        let ratio = short_count as f64 / n as f64;
        assert!(
            ratio > 0.7,
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
    fn largest_gap_no_connections() {
        assert_eq!(largest_gap_size(std::iter::empty()), (1.0, 0));
    }

    #[test]
    fn largest_gap_uniform_connections() {
        // Place connections at quartiles in log-space
        let d_at = |u: f64| D_MIN_TARGET * (D_MAX / D_MIN_TARGET).powf(u);
        let existing = [d_at(0.25), d_at(0.5), d_at(0.75)];
        let (gap, count) = largest_gap_size(existing.iter().copied());
        // 4 equal gaps of size 0.25
        assert!((gap - 0.25).abs() < 0.01, "Expected gap ~0.25, got {gap}");
        assert_eq!(count, 3);
    }

    #[test]
    fn largest_gap_clustered_connections() {
        // All connections near short distances — large gap at long distances
        let existing = [0.012, 0.015, 0.02];
        let (gap, count) = largest_gap_size(existing.iter().copied());
        // Most of log-space is empty, gap should be large
        assert!(
            gap > 0.5,
            "Clustered connections should have large gap, got {gap}"
        );
        assert_eq!(count, 3);
    }

    #[test]
    fn largest_gap_single_connection_at_midpoint() {
        // Single connection at geometric mean (log-space midpoint)
        let geo_mean = (D_MIN_TARGET * D_MAX).sqrt();
        let (gap, count) = largest_gap_size(std::iter::once(geo_mean));
        // Splits [0,1] into two ~equal halves
        assert!(
            (gap - 0.5).abs() < 0.01,
            "Midpoint connection should yield gap ~0.5, got {gap}"
        );
        assert_eq!(count, 1);
    }

    // --- removal_gap tests ---

    #[test]
    fn removal_gap_uniform_connections() {
        // Place 4 connections at quartiles in log-space.
        // Removing any one should create a gap of 0.5 (two adjacent 0.25 gaps merge).
        let d_at = |u: f64| D_MIN_TARGET * (D_MAX / D_MIN_TARGET).powf(u);
        let all = [d_at(0.25), d_at(0.5), d_at(0.75)];

        for &d in &all {
            let gap = removal_gap(d, all.iter().copied());
            // Removing middle point (0.5) merges gaps [0.25..0.5] and [0.5..0.75] → 0.5
            // Removing edge point (0.25) merges gaps [0..0.25] and [0.25..0.5] → 0.5
            // All should give 0.5 for uniform spacing
            assert!(
                (gap - 0.5).abs() < 0.02,
                "Expected removal gap ~0.5 for uniformly-spaced peer at {d:.6}, got {gap}"
            );
        }
    }

    #[test]
    fn removal_gap_critical_vs_redundant() {
        // One connection is the sole bridge across a large gap;
        // another is clustered with nearby connections.
        let d_at = |u: f64| D_MIN_TARGET * (D_MAX / D_MIN_TARGET).powf(u);

        // Isolated peer at 0.5 (large gap on both sides),
        // clustered peers at 0.1, 0.12, 0.14
        let all = [d_at(0.1), d_at(0.12), d_at(0.14), d_at(0.5)];

        let gap_isolated = removal_gap(d_at(0.5), all.iter().copied());
        let gap_clustered = removal_gap(d_at(0.12), all.iter().copied());

        assert!(
            gap_isolated > gap_clustered * 2.0,
            "Isolated peer removal gap ({gap_isolated:.4}) should be much larger \
             than clustered peer ({gap_clustered:.4})"
        );
    }

    #[test]
    fn removal_gap_outside_range_is_zero() {
        let all = [0.1, 0.2];
        assert_eq!(removal_gap(0.0, all.iter().copied()), 0.0);
        assert_eq!(removal_gap(-0.1, all.iter().copied()), 0.0);
        assert_eq!(removal_gap(0.6, all.iter().copied()), 0.0);
    }

    #[test]
    fn removal_gap_single_connection() {
        // Single connection: removing it leaves the entire [0, 1] range empty → gap = 1.0
        let d_at = |u: f64| D_MIN_TARGET * (D_MAX / D_MIN_TARGET).powf(u);
        let all = [d_at(0.5)];
        let gap = removal_gap(d_at(0.5), all.iter().copied());
        assert!(
            (gap - 1.0).abs() < 0.01,
            "Removing sole connection should give gap ~1.0, got {gap}"
        );
    }

    #[test]
    fn removal_gap_expected_value_for_uniform() {
        // With k connections uniformly distributed, removing one should create
        // a gap of approximately 2/k (two adjacent 1/k spacings merge).
        let d_at = |u: f64| D_MIN_TARGET * (D_MAX / D_MIN_TARGET).powf(u);

        for k in [4, 8, 16] {
            let all: Vec<f64> = (1..=k).map(|i| d_at(i as f64 / (k + 1) as f64)).collect();

            let expected_removal_gap = 2.0 / (k + 1) as f64;
            for &d in &all {
                let gap = removal_gap(d, all.iter().copied());
                assert!(
                    (gap - expected_removal_gap).abs() < 0.02,
                    "k={k}: expected removal gap ~{expected_removal_gap:.4}, got {gap:.4}"
                );
            }
        }
    }

    // --- directional function tests ---

    #[test]
    fn split_by_direction_basic() {
        let signed = [0.1, -0.2, 0.3, -0.05, 0.0];
        let (cw, ccw) = split_by_direction(&signed);
        assert_eq!(cw, vec![0.1, 0.3, 0.0]); // 0.0 goes to CW
        assert_eq!(ccw, vec![0.2, 0.05]); // stored as positive
    }

    #[test]
    fn gap_target_directional_prefers_empty_side() {
        let _guard = crate::config::GlobalRng::seed_guard(0xD121_0001);
        let my_loc = Location::new(0.5);

        // All connections CW (positive signed distance), none CCW
        let d_at = |u: f64| D_MIN_TARGET * (D_MAX / D_MIN_TARGET).powf(u);
        let signed: Vec<f64> = [d_at(0.25), d_at(0.5), d_at(0.75)].to_vec();

        // Most targets should go CCW (negative offset from my_location)
        let mut ccw_count = 0;
        let trials = 200;
        for _ in 0..trials {
            let target = gap_target_directional(my_loc, &signed);
            let sd = my_loc.signed_distance(target);
            if sd < 0.0 {
                ccw_count += 1;
            }
        }
        // CCW side is completely empty (gap=1.0) vs CW side has connections (gap≈0.25)
        // Probability of picking CCW ≈ 1.0/(1.0+0.25) ≈ 0.8
        assert!(
            ccw_count > trials * 60 / 100,
            "Expected most targets CCW (empty side), got {ccw_count}/{trials}"
        );
    }

    #[test]
    fn gap_target_directional_balanced_splits_evenly() {
        let _guard = crate::config::GlobalRng::seed_guard(0xD121_0002);
        let my_loc = Location::new(0.5);

        // Symmetric connections: same distances on both sides
        let d_at = |u: f64| D_MIN_TARGET * (D_MAX / D_MIN_TARGET).powf(u);
        let signed: Vec<f64> = vec![d_at(0.5), -d_at(0.5), d_at(0.25), -d_at(0.25)];

        let mut cw_count = 0;
        let trials = 200;
        for _ in 0..trials {
            let target = gap_target_directional(my_loc, &signed);
            let sd = my_loc.signed_distance(target);
            if sd >= 0.0 {
                cw_count += 1;
            }
        }
        let ratio = cw_count as f64 / trials as f64;
        assert!(
            (0.3..0.7).contains(&ratio),
            "Balanced connections should yield ~50/50 direction split, got {ratio:.2}"
        );
    }

    #[test]
    fn gap_target_directional_no_connections_falls_back() {
        let _guard = crate::config::GlobalRng::seed_guard(0xD121_0003);
        let my_loc = Location::new(0.5);

        for _ in 0..100 {
            let target = gap_target_directional(my_loc, &[]);
            let v = target.as_f64();
            assert!(
                (0.0..1.0).contains(&v),
                "Fallback target {v} outside valid range"
            );
        }
    }

    #[test]
    fn kleinberg_score_directional_boosts_deficit_side() {
        let d_at = |u: f64| D_MIN_TARGET * (D_MAX / D_MIN_TARGET).powf(u);

        // 3 connections CW, 0 CCW
        let signed: Vec<f64> = vec![d_at(0.25), d_at(0.5), d_at(0.75)];

        // Candidate on CCW side (deficit) vs same distance on CW side
        let candidate_dist = d_at(0.4);
        let score_ccw = kleinberg_score_directional(-candidate_dist, &signed);
        let score_cw = kleinberg_score_directional(candidate_dist, &signed);

        assert!(
            score_ccw > score_cw,
            "CCW (deficit side) score {score_ccw:.4} should beat CW score {score_cw:.4}"
        );
    }

    #[test]
    fn kleinberg_score_directional_balanced_no_boost() {
        let d_at = |u: f64| D_MIN_TARGET * (D_MAX / D_MIN_TARGET).powf(u);

        // Equal connections on both sides at same distances
        let signed: Vec<f64> = vec![d_at(0.25), -d_at(0.25), d_at(0.75), -d_at(0.75)];

        let candidate_dist = d_at(0.5);
        let score_cw = kleinberg_score_directional(candidate_dist, &signed);
        let score_ccw = kleinberg_score_directional(-candidate_dist, &signed);

        // Should be equal (no deficit on either side)
        assert!(
            (score_cw - score_ccw).abs() < 0.01,
            "Balanced sides should score equally: CW={score_cw:.4} CCW={score_ccw:.4}"
        );
    }

    #[test]
    fn removal_gap_directional_per_side() {
        let d_at = |u: f64| D_MIN_TARGET * (D_MAX / D_MIN_TARGET).powf(u);

        // CW: 3 connections well-spaced. CCW: 1 connection (critical).
        let signed: Vec<f64> = vec![d_at(0.25), d_at(0.5), d_at(0.75), -d_at(0.5)];

        // Removing the sole CCW connection should give gap=1.0 (entire half-ring empty)
        let gap_ccw = removal_gap_directional(-d_at(0.5), &signed);
        assert!(
            (gap_ccw - 1.0).abs() < 0.01,
            "Removing sole CCW connection should give gap ~1.0, got {gap_ccw}"
        );

        // Removing one of three CW connections should give gap ~0.5
        let gap_cw = removal_gap_directional(d_at(0.5), &signed);
        assert!(
            (gap_cw - 0.5).abs() < 0.05,
            "Removing middle CW connection should give gap ~0.5, got {gap_cw}"
        );
    }

    #[test]
    fn largest_gap_size_directional_uses_worse_side() {
        let d_at = |u: f64| D_MIN_TARGET * (D_MAX / D_MIN_TARGET).powf(u);

        // CW: 3 well-spaced connections (gap ~0.25). CCW: empty (gap = 1.0).
        let signed: Vec<f64> = vec![d_at(0.25), d_at(0.5), d_at(0.75)];

        let (gap, count) = largest_gap_size_directional(&signed);
        assert!(
            (gap - 1.0).abs() < 0.01,
            "Should return worse side's gap (1.0 for empty CCW), got {gap}"
        );
        assert_eq!(count, 3);
    }

    #[test]
    fn largest_gap_size_directional_balanced() {
        let d_at = |u: f64| D_MIN_TARGET * (D_MAX / D_MIN_TARGET).powf(u);

        // Symmetric: same spacing on both sides
        let signed: Vec<f64> = vec![
            d_at(0.25),
            d_at(0.5),
            d_at(0.75),
            -d_at(0.25),
            -d_at(0.5),
            -d_at(0.75),
        ];

        let (gap, count) = largest_gap_size_directional(&signed);
        assert!(
            (gap - 0.25).abs() < 0.02,
            "Both sides have gap ~0.25, got {gap}"
        );
        assert_eq!(count, 6);
    }
}
