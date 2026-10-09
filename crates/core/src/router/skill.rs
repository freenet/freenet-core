//! Prequential skill scoring for a binary forecast (#4485).

/// Prequential accuracy for one prediction layer, scored against the
/// climatological base rate rather than an absolute threshold.
///
/// # Why skill rather than raw Brier
///
/// For a binary outcome with base rate `p̄`, a constant predictor that always
/// says `p̄` scores `p̄(1−p̄)`. At the ~1% failure rate a production gateway
/// actually sees that is 0.0099 — which an absolute scale reading "< 0.05
/// excellent" grades as excellent. That is not a hypothetical: it is exactly how
/// a routing model with *negative* skill went unnoticed for six months, because
/// the dashboard graded rarity and called it accuracy.
///
/// The skill score divides that out:
///
/// ```text
/// skill = 1 − brier / (p̄(1−p̄))
/// ```
///
/// Zero means "no better than assuming the base rate", one means perfect, and
/// **negative means worse than assuming nothing** — the reading that matters and
/// that an absolute threshold cannot express.
#[derive(Debug, Clone, Default)]
pub(crate) struct SkillTracker {
    squared_error: f64,
    outcome_sum: f64,
    count: u64,
}

impl SkillTracker {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn record(&mut self, predicted: f64, actual: f64) {
        if !predicted.is_finite() || !actual.is_finite() {
            return;
        }
        let error = predicted - actual;
        self.squared_error += error * error;
        self.outcome_sum += actual;
        self.count += 1;
    }

    pub(crate) fn count(&self) -> u64 {
        self.count
    }

    /// Mean squared error — the Brier score for a probabilistic binary forecast.
    pub(crate) fn brier(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        Some(self.squared_error / self.count as f64)
    }

    /// Observed base rate over the scored window.
    pub(crate) fn base_rate(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        Some(self.outcome_sum / self.count as f64)
    }

    /// Brier score a constant base-rate forecast would have achieved. This is the
    /// baseline every layer has to beat to be worth its cost.
    pub(crate) fn climatology_brier(&self) -> Option<f64> {
        let base = self.base_rate()?;
        let value = base * (1.0 - base);
        if value > 0.0 { Some(value) } else { None }
    }

    /// `1 − brier / climatology`. `None` when the window holds no variation to
    /// score against (an all-success or all-failure window), where skill is
    /// genuinely undefined rather than zero.
    pub(crate) fn skill(&self) -> Option<f64> {
        let brier = self.brier()?;
        let climatology = self.climatology_brier()?;
        let skill = 1.0 - brier / climatology;
        skill.is_finite().then_some(skill)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_is_zero_for_a_constant_base_rate_forecast() {
        // 100 events at a 10% base rate, predicting exactly the base rate every
        // time. This is the definition of zero skill and it must read as zero,
        // however flattering the absolute Brier looks.
        let mut tracker = SkillTracker::new();
        for index in 0..100 {
            let actual = if index < 10 { 1.0 } else { 0.0 };
            tracker.record(0.10, actual);
        }
        let skill = tracker
            .skill()
            .expect("skill is defined for a mixed window");
        assert!(
            skill.abs() < 1e-9,
            "a climatology forecast must score exactly zero skill, got {skill}"
        );
    }

    /// The measured production case. Reproducing it here is the regression test
    /// for the reason this was missed: an absolute "< 0.05 excellent" grade on a
    /// rare event flatters a forecast that is *worse* than assuming nothing.
    #[test]
    fn absolute_brier_can_look_excellent_while_skill_is_negative() {
        // gateway 1's recent window: 200 predictions, 2 real failures neither of
        // which was predicted, plus 1 full-confidence false alarm.
        let mut tracker = SkillTracker::new();
        tracker.record(1.0, 0.0); // false alarm at p=1.0
        tracker.record(0.0, 1.0); // missed failure
        tracker.record(0.0, 1.0); // missed failure
        for _ in 0..197 {
            tracker.record(0.0, 0.0);
        }

        let brier = tracker.brier().unwrap();
        let skill = tracker.skill().unwrap();

        assert!(
            brier < 0.05,
            "this window's absolute Brier lands in the old 'excellent' band ({brier})"
        );
        assert!(
            skill < 0.0,
            "yet its skill must be negative — worse than assuming nothing — got {skill}"
        );
    }

    #[test]
    fn skill_is_one_for_a_perfect_forecast() {
        let mut tracker = SkillTracker::new();
        for index in 0..100 {
            let actual = if index < 25 { 1.0 } else { 0.0 };
            tracker.record(actual, actual);
        }
        assert!((tracker.skill().unwrap() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn skill_is_undefined_without_variation_to_score_against() {
        // An all-success window has climatology Brier 0, so skill is genuinely
        // undefined rather than zero or infinite.
        let mut tracker = SkillTracker::new();
        for _ in 0..50 {
            tracker.record(0.02, 0.0);
        }
        assert_eq!(tracker.base_rate(), Some(0.0));
        assert_eq!(tracker.climatology_brier(), None);
        assert_eq!(tracker.skill(), None);
        assert!(
            tracker.brier().is_some(),
            "the Brier score is still defined"
        );
    }

    #[test]
    fn skill_tracker_is_empty_before_any_record() {
        let tracker = SkillTracker::new();
        assert_eq!(tracker.count(), 0);
        assert_eq!(tracker.brier(), None);
        assert_eq!(tracker.base_rate(), None);
        assert_eq!(tracker.skill(), None);
    }

    #[test]
    fn skill_tracker_ignores_non_finite_samples() {
        let mut tracker = SkillTracker::new();
        tracker.record(f64::NAN, 0.0);
        tracker.record(0.5, f64::INFINITY);
        assert_eq!(tracker.count(), 0);
    }
}
