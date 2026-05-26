// Consumers (the contract reaper, the peer-side load shedder, the
// governance dashboard) land in PR #2; this module is published
// foundationally so PR #1 can ship the primitive + its property tests
// independently of the rest.
#![allow(dead_code)]

//! Shared outlier detection used by per-contract governance and
//! (eventually) by peer-side load-shedding.
//!
//! See `docs/design/contract-hardening.md` — "Threshold by anomaly
//! detection" and "Shared governance module".
//!
//! The core primitive is `detect_outliers`: given a collection of samples
//! producing log-space cost/benefit ratios, compute a robust threshold
//! using trimmed median absolute deviation (MAD) and return the keys
//! whose ratio exceeds it.
//!
//! The threshold is **not** an operator-set budget. It is derived from
//! the network's own observed cost-per-benefit distribution. The single
//! tuning knob (`k`) translates directly to a false-positive rate
//! interpretation: under a roughly log-normal honest population,
//! `k = 5` corresponds to ≈ 1-in-a-million natural flagging.
//!
//! ## Why MAD instead of standard deviation
//!
//! MAD has a 50% breakdown point: up to half of the input can be
//! anomalous before the statistic itself gets corrupted. Standard
//! deviation, by contrast, is dominated by the very outliers we are
//! trying to detect. Combined with log-space (handles heavy tails) and
//! a 5% top-trim (extra contamination resistance without arbitrary
//! thresholds), this gives a threshold that adapts to the network's
//! actual norms.
//!
//! ## Sanity guardrails
//!
//! * Minimum sample size (`min_samples`, default 30) — below this MAD
//!   is too noisy to act on; the function returns no flags.
//! * Capacity ceiling (`capacity_ceiling_log`) — even with k=5 a
//!   contaminated network could drift the threshold to consume all
//!   node capacity. The threshold is clamped at the ceiling. Whether
//!   the ceiling is binding is surfaced in `OutlierResult` so the
//!   dashboard can show "MAD-derived threshold capped at ceiling."
//! * MAD-collapse handling — if all samples are identical (MAD = 0),
//!   the function returns no flags (everything is "the norm" and
//!   there's no spread to anchor a threshold to).

use std::collections::HashMap;
use std::hash::Hash;

/// Gaussian-consistency constant for MAD: under a normal distribution,
/// `σ ≈ 1.4826 × MAD`. We scale MAD by this factor before applying `k`
/// so that the doc-claimed false-positive rates (k=3 ≈ p99.7, k=5 ≈
/// 1-in-a-million, k=6 ≈ 1-in-500M) actually hold under a log-normal
/// honest population. Without this scaling, raw `k × MAD` is only
/// `k × 0.6745 × σ`, so k=5 would correspond to ~3.37σ (≈ 1-in-1500),
/// not the 1-in-a-million the design doc claims. See
/// <https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation>.
const MAD_GAUSSIAN_CONSISTENCY: f64 = 1.4826;

/// Configuration for the outlier detector. Defaults match the
/// design-doc values: k=5 (1-in-a-million false-positive rate under
/// log-normal assumption), n≥30 minimum sample size (standard
/// statistical guidance), 5% top-trim (robust-statistics convention).
#[derive(Clone, Copy, Debug)]
pub(crate) struct OutlierConfig {
    /// Number of MAD-units beyond the median that defines the
    /// threshold. Translates directly to a false-positive-rate
    /// interpretation under a roughly log-normal honest population.
    pub k: f64,
    /// Below this sample count MAD is too noisy to flag anyone.
    pub min_samples: usize,
    /// Fraction of the top tail trimmed before computing MAD, so the
    /// statistic doesn't get pulled by the very outliers being detected.
    pub trim_fraction: f64,
}

impl Default for OutlierConfig {
    fn default() -> Self {
        Self {
            k: 5.0,
            min_samples: 30,
            trim_fraction: 0.05,
        }
    }
}

/// Why a given outlier check produced no flags. Surface this in the
/// dashboard so an operator can tell "governance is working but
/// nothing's wrong" apart from "governance can't act yet."
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SkipReason {
    /// `samples.len() < config.min_samples`.
    InsufficientSamples,
    /// All samples produced the same log-ratio — MAD = 0, no spread to
    /// anchor a threshold to. Surfaces as "skip + log condition" in the
    /// dashboard rather than as a vacuous flag.
    MadCollapsed,
    /// `extract_log_ratio` returned None for every sample (e.g. zero
    /// benefit everywhere). No actionable distribution.
    NoExtractableRatios,
}

/// Result of an outlier-detection pass. Includes statistics needed by
/// the dashboard for the network-norms view (median, MAD, threshold,
/// sample size) so a single call powers both the flag set and the
/// distribution panel.
#[derive(Clone, Debug)]
pub(crate) struct OutlierResult<K> {
    /// Keys whose log-ratio exceeds the computed threshold. Empty when
    /// `skip_reason` is set.
    pub flagged: Vec<K>,
    /// Median of the trimmed log-ratio sample. None when no ratios were
    /// extractable.
    pub median_log_ratio: Option<f64>,
    /// Median Absolute Deviation of the trimmed sample. None when
    /// non-computable.
    pub mad: Option<f64>,
    /// `median + k × mad`, clamped at `capacity_ceiling_log`. None when
    /// the function early-exited.
    pub threshold: Option<f64>,
    /// True when the MAD-derived threshold was clamped at the capacity
    /// ceiling. Surfaces as a dashboard warning ("node genuinely
    /// overloaded, not just outliers").
    pub capacity_ceiling_binding: bool,
    /// Number of samples that produced a usable ratio (post-trim count
    /// is reported as `trimmed_sample_size` separately).
    pub sample_size: usize,
    /// Sample size after the top-trim. The MAD was computed on this set.
    pub trimmed_sample_size: usize,
    /// Why the pass produced no flags, when no flags were produced. None
    /// means the pass ran normally — `flagged` may still be empty if
    /// nothing crossed the threshold, which is the healthy case.
    pub skip_reason: Option<SkipReason>,
}

impl<K> OutlierResult<K> {
    fn skip(reason: SkipReason, sample_size: usize) -> Self {
        Self {
            flagged: Vec::new(),
            median_log_ratio: None,
            mad: None,
            threshold: None,
            capacity_ceiling_binding: false,
            sample_size,
            trimmed_sample_size: 0,
            skip_reason: Some(reason),
        }
    }
}

/// Detect outliers in the `samples` map. For each sample,
/// `extract_log_ratio` produces an optional `f64` — return `None` for
/// samples that should be excluded from the distribution (e.g. zero
/// benefit, zero cost — ratios are undefined). `capacity_ceiling_log`
/// is the log-space upper bound the threshold cannot exceed, derived
/// from node hardware capacity.
///
/// Flagged keys are those whose `log_ratio > threshold`, where
/// `threshold = median + k × mad`, MAD computed on a top-trimmed
/// sample (default 5%), and the threshold further clamped at
/// `capacity_ceiling_log`.
pub(crate) fn detect_outliers<K, S, F>(
    samples: &HashMap<K, S>,
    extract_log_ratio: F,
    config: &OutlierConfig,
    capacity_ceiling_log: f64,
) -> OutlierResult<K>
where
    K: Clone + Eq + Hash,
    F: Fn(&S) -> Option<f64>,
{
    // Step 1: extract ratios, dropping any non-finite values
    // (NaN, ±inf). The extractor's contract is "return None for
    // unscoreable samples", but callers can produce non-finite results
    // by dividing by zero, taking log of zero, etc.; the primitive
    // defends against that here so a single malformed sample can't
    // poison median/MAD via NaN-propagation (every comparison with
    // NaN is false, including `mad < f64::EPSILON`, so without this
    // guard the function would silently flag nothing).
    let mut pairs: Vec<(K, f64)> = samples
        .iter()
        .filter_map(|(k, s)| {
            extract_log_ratio(s).and_then(|r| {
                if r.is_finite() {
                    Some((k.clone(), r))
                } else {
                    None
                }
            })
        })
        .collect();

    let sample_size = pairs.len();
    if sample_size == 0 {
        return OutlierResult::skip(SkipReason::NoExtractableRatios, sample_size);
    }
    if sample_size < config.min_samples {
        return OutlierResult::skip(SkipReason::InsufficientSamples, sample_size);
    }

    // Step 2: trim the top tail BEFORE computing the statistic so the
    // very outliers we're detecting don't corrupt the threshold.
    //
    // Sort by ratio ascending — partial_cmp because f64 isn't Ord.
    // Non-finite values were filtered above, so partial_cmp will always
    // return Some; the unwrap_or is purely defensive.
    pairs.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    let trim_count = (sample_size as f64 * config.trim_fraction).floor() as usize;
    let trimmed_len = sample_size.saturating_sub(trim_count);
    let trimmed_ratios: Vec<f64> = pairs.iter().take(trimmed_len).map(|(_, r)| *r).collect();

    if trimmed_ratios.is_empty() {
        // Trim consumed everything — shouldn't happen with sensible
        // configs (trim_fraction < 1), but defensive.
        return OutlierResult::skip(SkipReason::InsufficientSamples, sample_size);
    }

    // Step 3: median of trimmed sample.
    let median = median_of_sorted(&trimmed_ratios);

    // Step 4: MAD — median of |x − median|. Compute deviations, sort,
    // take median again.
    let mut deviations: Vec<f64> = trimmed_ratios.iter().map(|r| (r - median).abs()).collect();
    deviations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Less));
    let mad = median_of_sorted(&deviations);

    if mad < f64::EPSILON {
        // All values identical (or within rounding) — no spread to
        // anchor a threshold to. Log the condition; flag nothing.
        return OutlierResult {
            flagged: Vec::new(),
            median_log_ratio: Some(median),
            mad: Some(mad),
            threshold: None,
            capacity_ceiling_binding: false,
            sample_size,
            trimmed_sample_size: trimmed_len,
            skip_reason: Some(SkipReason::MadCollapsed),
        };
    }

    // Step 5: threshold = median + k × (σ-scaled MAD), clamped at
    // capacity ceiling. Under a normal honest population the scaling
    // by `MAD_GAUSSIAN_CONSISTENCY` (≈1.4826) makes `k` interpretable
    // as the number of standard deviations beyond the median, so
    // k=5 ≈ 1-in-a-million as the design doc claims.
    let scaled_mad = mad * MAD_GAUSSIAN_CONSISTENCY;
    let raw_threshold = median + config.k * scaled_mad;
    let capacity_ceiling_binding = raw_threshold > capacity_ceiling_log;
    let threshold = raw_threshold.min(capacity_ceiling_log);

    // Step 6: flag everyone whose ratio strictly exceeds the threshold.
    // Walk the ORIGINAL pairs (not the trimmed view) — a sample sitting
    // in the trimmed top-tail is exactly what we want to flag.
    let flagged: Vec<K> = pairs
        .into_iter()
        .filter(|(_, r)| *r > threshold)
        .map(|(k, _)| k)
        .collect();

    OutlierResult {
        flagged,
        median_log_ratio: Some(median),
        mad: Some(mad),
        threshold: Some(threshold),
        capacity_ceiling_binding,
        sample_size,
        trimmed_sample_size: trimmed_len,
        skip_reason: None,
    }
}

/// Median of an already-sorted slice. Handles even and odd lengths.
/// Panics if the slice is empty (callers check first).
fn median_of_sorted(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    debug_assert!(n > 0, "median_of_sorted called with empty slice");
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map_from(samples: &[(&str, f64)]) -> HashMap<String, f64> {
        samples.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    fn cfg(k: f64, min_samples: usize) -> OutlierConfig {
        OutlierConfig {
            k,
            min_samples,
            trim_fraction: 0.05,
        }
    }

    #[test]
    fn empty_samples_skips_no_extractable_ratios() {
        let m: HashMap<String, f64> = HashMap::new();
        let r = detect_outliers(&m, |x| Some(*x), &OutlierConfig::default(), 10.0);
        assert_eq!(r.skip_reason, Some(SkipReason::NoExtractableRatios));
        assert!(r.flagged.is_empty());
    }

    #[test]
    fn below_min_samples_skips() {
        let m = map_from(&[("a", -1.0), ("b", -0.5), ("c", 0.0)]);
        let r = detect_outliers(&m, |x| Some(*x), &cfg(5.0, 30), 10.0);
        assert_eq!(r.skip_reason, Some(SkipReason::InsufficientSamples));
        assert!(r.flagged.is_empty());
    }

    #[test]
    fn all_identical_collapses_mad() {
        let pairs: Vec<(String, f64)> = (0..50).map(|i| (format!("c{i}"), -1.0)).collect();
        let m: HashMap<_, _> = pairs.into_iter().collect();
        let r = detect_outliers(&m, |x| Some(*x), &cfg(5.0, 30), 10.0);
        assert_eq!(r.skip_reason, Some(SkipReason::MadCollapsed));
        assert!(r.flagged.is_empty());
        assert_eq!(r.median_log_ratio, Some(-1.0));
    }

    #[test]
    fn worked_example_from_design_doc() {
        // 7 contracts: [-1.5, -1.2, -1.0, -0.9, -0.8, -0.6, +2.5]
        // Expected: median=-0.9, MAD=0.3.
        // threshold(k=5) = median + k × 1.4826 × MAD
        //                = -0.9 + 5 × 1.4826 × 0.3
        //                = +1.32390
        // abuser at +2.5 exceeds the threshold → flagged.
        //
        // n=7 is below default min_samples=30, so this test lowers
        // min_samples to 5 to let the math run. The bracketed values are
        // illustrative; production callers use the default min_samples=30.
        let m = map_from(&[
            ("a", -1.5),
            ("b", -1.2),
            ("c", -1.0),
            ("d", -0.9),
            ("e", -0.8),
            ("f", -0.6),
            ("abuser", 2.5),
        ]);
        // trim_fraction*7 = 0.35 → floor 0 → no trimming for this small set
        let cfg = OutlierConfig {
            k: 5.0,
            min_samples: 5,
            trim_fraction: 0.05,
        };
        let r = detect_outliers(&m, |x| Some(*x), &cfg, 10.0);
        assert!((r.median_log_ratio.unwrap() - (-0.9)).abs() < 1e-9);
        assert!((r.mad.unwrap() - 0.3).abs() < 1e-9);
        let expected_threshold = -0.9 + 5.0 * MAD_GAUSSIAN_CONSISTENCY * 0.3;
        assert!((r.threshold.unwrap() - expected_threshold).abs() < 1e-9);
        assert_eq!(r.flagged, vec!["abuser".to_string()]);
        assert_eq!(r.skip_reason, None);
    }

    #[test]
    fn standard_deviation_would_miss_what_mad_catches() {
        // Construct a set where the outlier is so extreme it would inflate
        // σ enough to hide itself. MAD is unaffected.
        // Bulk: 35 contracts near 0.0. One outlier at +100.
        let mut pairs: Vec<(String, f64)> = (0..35)
            .map(|i| (format!("c{i}"), (i as f64 - 17.0) * 0.02))
            .collect();
        pairs.push(("abuser".to_string(), 100.0));
        let m: HashMap<_, _> = pairs.into_iter().collect();
        let r = detect_outliers(
            &m,
            |x| Some(*x),
            &OutlierConfig::default(), // k=5, min=30, trim=0.05
            1000.0,
        );
        // Even with k=5 and the abuser in the sample, MAD catches it.
        // Trim removes top 5% of 36 = 1 sample (the abuser itself), so
        // MAD is computed on the honest bulk — exactly the property we
        // want. Then we walk the ORIGINAL set to flag, so the abuser
        // is included in `flagged`.
        assert!(r.flagged.contains(&"abuser".to_string()));
        assert!(r.threshold.unwrap() < 10.0);
    }

    #[test]
    fn capacity_ceiling_clamps_threshold() {
        // Wide-spread but honest distribution.
        let pairs: Vec<(String, f64)> = (0..50)
            .map(|i| (format!("c{i}"), (i as f64 - 25.0) * 0.5))
            .collect();
        let m: HashMap<_, _> = pairs.into_iter().collect();
        // Set ceiling at +1.0 — tighter than MAD would naturally allow.
        let r = detect_outliers(&m, |x| Some(*x), &OutlierConfig::default(), 1.0);
        assert!(r.capacity_ceiling_binding);
        assert_eq!(r.threshold, Some(1.0));
    }

    #[test]
    fn returns_keys_only_for_those_exceeding_threshold() {
        let pairs: Vec<(String, f64)> = (0..30)
            .map(|i| (format!("c{i}"), -1.0 + (i as f64) * 0.02))
            .collect();
        // Add explicit boundary cases.
        let mut all = pairs;
        all.push(("at_threshold".into(), 100.0)); // way above
        all.push(("normal".into(), -0.95));
        let m: HashMap<_, _> = all.into_iter().collect();
        let r = detect_outliers(&m, |x| Some(*x), &OutlierConfig::default(), 1000.0);
        assert!(r.flagged.contains(&"at_threshold".to_string()));
        assert!(!r.flagged.contains(&"normal".to_string()));
    }

    #[test]
    fn extract_returning_none_excludes_from_sample() {
        // Half the samples return None — should be excluded from the
        // ratio set. With 60 entries / 2 valid = 30, just at min.
        let pairs: Vec<(String, Option<f64>)> = (0..60)
            .map(|i| (format!("c{i}"), if i % 2 == 0 { Some(-1.0) } else { None }))
            .collect();
        let m: HashMap<_, _> = pairs.into_iter().collect();
        let r = detect_outliers(&m, |x: &Option<f64>| *x, &OutlierConfig::default(), 10.0);
        assert_eq!(r.sample_size, 30);
        // All identical → MAD collapses, returns no flags.
        assert_eq!(r.skip_reason, Some(SkipReason::MadCollapsed));
    }

    #[test]
    fn skip_reason_none_when_pass_succeeds() {
        let pairs: Vec<(String, f64)> = (0..30)
            .map(|i| (format!("c{i}"), -1.0 + (i as f64 - 15.0) * 0.05))
            .collect();
        let m: HashMap<_, _> = pairs.into_iter().collect();
        let r = detect_outliers(&m, |x| Some(*x), &OutlierConfig::default(), 10.0);
        assert_eq!(r.skip_reason, None);
        // No outliers in this gentle distribution.
        assert!(r.flagged.is_empty());
    }

    #[test]
    fn non_finite_ratios_are_dropped_from_sample() {
        // NaN, +inf, -inf in the extractor output must be excluded
        // before median/MAD computation — otherwise NaN propagates
        // through every comparison and the function silently flags
        // nothing. Design doc §"Edge cases" line 287.
        let mut pairs: Vec<(String, f64)> = (0..30)
            .map(|i| (format!("c{i}"), (i as f64 - 15.0) * 0.05))
            .collect();
        pairs.push(("nan".into(), f64::NAN));
        pairs.push(("pos_inf".into(), f64::INFINITY));
        pairs.push(("neg_inf".into(), f64::NEG_INFINITY));
        let m: HashMap<_, _> = pairs.into_iter().collect();
        let r = detect_outliers(&m, |x| Some(*x), &OutlierConfig::default(), 10.0);
        // Only the 30 finite samples count toward sample_size.
        assert_eq!(r.sample_size, 30);
        // No NaN-poisoning of median/MAD.
        assert!(r.median_log_ratio.unwrap().is_finite());
        // The non-finite keys are never flagged.
        assert!(!r.flagged.iter().any(|k| k == "nan"));
        assert!(!r.flagged.iter().any(|k| k == "pos_inf"));
        assert!(!r.flagged.iter().any(|k| k == "neg_inf"));
    }

    #[test]
    fn n_equals_one_with_min_samples_one() {
        // Boundary: a single sample with min_samples=1. MAD is trivially
        // zero (single sample has no spread), so we expect MadCollapsed.
        // This pins behaviour so a future refactor that, say, treats
        // n=1 as a special "always flag" or "always pass" case fails the
        // test instead of silently changing semantics.
        let m = map_from(&[("only", 0.5)]);
        let cfg = OutlierConfig {
            k: 5.0,
            min_samples: 1,
            trim_fraction: 0.05,
        };
        let r = detect_outliers(&m, |x| Some(*x), &cfg, 10.0);
        assert_eq!(r.sample_size, 1);
        assert_eq!(r.skip_reason, Some(SkipReason::MadCollapsed));
        assert!(r.flagged.is_empty());
    }

    #[test]
    fn n_equals_two_with_min_samples_two() {
        // Boundary: n=2 with distinct values. Median is the mean of the
        // two; MAD is the median of two equal deviations = that
        // deviation. Verify the math runs and doesn't flag either
        // sample (both sit exactly at ±MAD from the median, which is
        // below the k×MAD threshold).
        let m = map_from(&[("low", -0.1), ("high", 0.1)]);
        let cfg = OutlierConfig {
            k: 5.0,
            min_samples: 2,
            trim_fraction: 0.05,
        };
        let r = detect_outliers(&m, |x| Some(*x), &cfg, 10.0);
        assert_eq!(r.sample_size, 2);
        assert!((r.median_log_ratio.unwrap() - 0.0).abs() < 1e-9);
        assert!((r.mad.unwrap() - 0.1).abs() < 1e-9);
        assert!(r.flagged.is_empty());
    }

    #[test]
    fn mad_collapses_after_trim_even_when_raw_set_varies() {
        // Construct a 30-sample set whose top 5% trim (1 sample) is the
        // only variation; the remaining 29 samples are all identical.
        // MAD-of-trimmed should be 0 → MadCollapsed.
        let mut pairs: Vec<(String, f64)> = (0..29).map(|i| (format!("c{i}"), -1.0)).collect();
        pairs.push(("the_one_with_variation".into(), 5.0));
        let m: HashMap<_, _> = pairs.into_iter().collect();
        let r = detect_outliers(&m, |x| Some(*x), &OutlierConfig::default(), 10.0);
        assert_eq!(r.sample_size, 30);
        // trim removes the top 5% × 30 = 1 sample → MAD of the
        // remaining 29 (all -1.0) collapses.
        assert_eq!(r.skip_reason, Some(SkipReason::MadCollapsed));
        assert!(r.flagged.is_empty());
    }

    #[test]
    fn capacity_ceiling_not_binding_when_threshold_below() {
        // Pin the OFF state of capacity_ceiling_binding so a refactor
        // that inverts the comparison fails the test instead of
        // silently shipping. Honest bulk distribution, ceiling well
        // above the natural threshold.
        let pairs: Vec<(String, f64)> = (0..30)
            .map(|i| (format!("c{i}"), -1.0 + (i as f64 - 15.0) * 0.02))
            .collect();
        let m: HashMap<_, _> = pairs.into_iter().collect();
        let r = detect_outliers(&m, |x| Some(*x), &OutlierConfig::default(), 100.0);
        assert!(!r.capacity_ceiling_binding);
        // Threshold should equal the raw `median + k × 1.4826 × MAD`,
        // unclamped.
        assert!(r.threshold.unwrap() < 100.0);
    }

    #[test]
    fn three_outliers_exceeding_trim_fraction() {
        // n=30 with trim_fraction=0.05 trims only 1 sample. With 3
        // outliers, 2 remain in the MAD computation and could contaminate
        // it. Verify MAD's 50% breakdown point still catches all 3 in
        // the flagged set even when trim alone isn't enough.
        let mut pairs: Vec<(String, f64)> = (0..27)
            .map(|i| (format!("honest{i}"), -1.0 + (i as f64 - 13.0) * 0.02))
            .collect();
        pairs.push(("abuser1".into(), 5.0));
        pairs.push(("abuser2".into(), 6.0));
        pairs.push(("abuser3".into(), 7.0));
        let m: HashMap<_, _> = pairs.into_iter().collect();
        let r = detect_outliers(&m, |x| Some(*x), &OutlierConfig::default(), 100.0);
        // All three abusers should be in the flagged set even though
        // trim removed only the most-extreme one.
        assert!(r.flagged.contains(&"abuser1".to_string()));
        assert!(r.flagged.contains(&"abuser2".to_string()));
        assert!(r.flagged.contains(&"abuser3".to_string()));
        // No honest contracts in the flagged set.
        assert!(!r.flagged.iter().any(|k| k.starts_with("honest")));
    }

    #[test]
    fn sample_exactly_at_threshold_is_not_flagged() {
        // The flag check is strictly `>` not `>=`. Pin that so a future
        // change to `>=` doesn't silently widen the set. Construct a
        // sample whose ratio equals the computed threshold and assert
        // it is NOT in `flagged`.
        let pairs: Vec<(String, f64)> = (0..30).map(|i| (format!("c{i}"), -1.0)).collect();
        let mut m: HashMap<_, _> = pairs.into_iter().collect();
        // Compute the same threshold logic by hand: 29 are -1.0 and one
        // is -0.5 to give MAD a non-zero value. Median = -1.0, MAD = 0.
        // Better: build a wider set so MAD is non-zero.
        m.clear();
        let widened: Vec<(String, f64)> = (0..30)
            .map(|i| (format!("c{i}"), -1.0 + (i as f64 - 15.0) * 0.01))
            .collect();
        m.extend(widened);
        let cfg = OutlierConfig::default();
        let r_probe = detect_outliers(&m, |x| Some(*x), &cfg, 100.0);
        let exact_threshold = r_probe.threshold.unwrap();
        // Add a sample whose ratio is exactly the threshold value.
        m.insert("at_threshold".into(), exact_threshold);
        let r = detect_outliers(&m, |x| Some(*x), &cfg, 100.0);
        assert!(
            !r.flagged.contains(&"at_threshold".to_string()),
            "sample at threshold should NOT be flagged (strict > semantics), flagged: {:?}",
            r.flagged
        );
    }

    #[test]
    fn invariant_under_translation() {
        // Shifting every sample by a constant must shift median by the
        // same constant, leave MAD unchanged, and produce the same flag
        // set.
        let base: Vec<(String, f64)> = (0..30)
            .map(|i| (format!("c{i}"), (i as f64 - 15.0) * 0.1))
            .collect();
        let mut shifted = base.clone();
        for (_, v) in shifted.iter_mut() {
            *v += 7.5;
        }
        let m1: HashMap<_, _> = base.into_iter().collect();
        let m2: HashMap<_, _> = shifted.into_iter().collect();
        let r1 = detect_outliers(&m1, |x| Some(*x), &OutlierConfig::default(), 100.0);
        let r2 = detect_outliers(&m2, |x| Some(*x), &OutlierConfig::default(), 100.0);
        assert!((r2.median_log_ratio.unwrap() - r1.median_log_ratio.unwrap() - 7.5).abs() < 1e-9);
        assert!((r1.mad.unwrap() - r2.mad.unwrap()).abs() < 1e-9);
        assert_eq!(r1.flagged.len(), r2.flagged.len());
    }
}
