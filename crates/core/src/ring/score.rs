//! Score type used for ranking peers and contracts

#![allow(dead_code)] // Score will be used as refactoring progresses

/// A score value used for ranking peers and contracts.
///
/// Uses `f64::total_cmp` for ordering to ensure consistent behavior with NaN values.
/// This is important because the previous implementation would treat NaN as equal
/// to everything, violating `Ord` requirements and potentially causing subtle bugs
/// in sorting algorithms.
#[derive(PartialEq, Clone, Copy, Debug)]
pub struct Score(pub f64);

impl Score {
    /// Create a new score from an f64 value.
    pub fn new(value: f64) -> Self {
        Score(value)
    }

    /// Get the underlying f64 value.
    pub fn value(&self) -> f64 {
        self.0
    }
}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Use total_cmp for consistent NaN handling.
        // This treats NaN as greater than all other values, which provides
        // a total ordering required by the Ord trait.
        self.0.total_cmp(&other.0)
    }
}

impl Eq for Score {}

impl From<f64> for Score {
    fn from(value: f64) -> Self {
        Score(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_score_ordering_normal_values() {
        let s1 = Score::new(1.0);
        let s2 = Score::new(2.0);
        let s3 = Score::new(1.0);

        assert!(s1 < s2);
        assert!(s2 > s1);
        assert_eq!(s1, s3);
        assert!(s1 <= s3);
        assert!(s1 >= s3);
    }

    #[test]
    fn test_score_ordering_negative_values() {
        let neg = Score::new(-1.0);
        let zero = Score::new(0.0);
        let pos = Score::new(1.0);

        assert!(neg < zero);
        assert!(zero < pos);
        assert!(neg < pos);
    }

    #[test]
    fn test_score_ordering_with_nan() {
        // NaN should have consistent ordering behavior (greater than all values)
        let nan = Score::new(f64::NAN);
        let normal = Score::new(1.0);
        let inf = Score::new(f64::INFINITY);

        // With total_cmp, NaN is greater than everything including infinity
        assert!(nan > normal);
        assert!(nan > inf);

        // Two NaNs should be equal under total_cmp
        let nan2 = Score::new(f64::NAN);
        assert_eq!(nan.cmp(&nan2), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_score_ordering_with_infinity() {
        let neg_inf = Score::new(f64::NEG_INFINITY);
        let pos_inf = Score::new(f64::INFINITY);
        let normal = Score::new(0.0);

        assert!(neg_inf < normal);
        assert!(normal < pos_inf);
        assert!(neg_inf < pos_inf);
    }

    #[test]
    fn test_score_sorting() {
        let mut scores = [
            Score::new(3.0),
            Score::new(1.0),
            Score::new(f64::NAN),
            Score::new(2.0),
            Score::new(f64::NEG_INFINITY),
        ];

        scores.sort();

        // After sorting: NEG_INFINITY, 1.0, 2.0, 3.0, NAN
        assert_eq!(scores[0].0, f64::NEG_INFINITY);
        assert_eq!(scores[1].0, 1.0);
        assert_eq!(scores[2].0, 2.0);
        assert_eq!(scores[3].0, 3.0);
        assert!(scores[4].0.is_nan());
    }

    #[test]
    fn test_score_from_f64() {
        let score: Score = 42.0.into();
        assert_eq!(score.value(), 42.0);
    }

    #[test]
    fn test_score_equality() {
        let s1 = Score::new(1.5);
        let s2 = Score::new(1.5);
        let s3 = Score::new(1.6);

        assert_eq!(s1, s2);
        assert_ne!(s1, s3);
    }

    #[test]
    fn test_score_clone_and_copy() {
        let s1 = Score::new(1.0);
        let s2 = s1; // Copy
        #[allow(clippy::clone_on_copy)]
        let s3 = s1.clone(); // Explicit clone test

        assert_eq!(s1, s2);
        assert_eq!(s1, s3);
    }
}
