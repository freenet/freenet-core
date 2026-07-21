use crate::topology::rate::Rate;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Debug)]
pub(crate) struct RunningAverage {
    max_samples: usize,
    samples: VecDeque<(Instant, f64)>,
    sum_samples: f64,
    total_sample_count: usize,
    /// Time of the FIRST sample ever recorded on this average (not merely the
    /// oldest retained sample, which count-truncation keeps young for
    /// high-frequency sources). Read by [`Self::windowed_rate`] so the
    /// cost-pressure trigger (cost-aware eviction, #4861) can require
    /// SUSTAINED reporting before an axis may act — a single large burst has
    /// a first-sample age of ~0 and is excluded.
    first_sample_time: Option<Instant>,
}

impl RunningAverage {
    pub fn new(max_samples: usize) -> Self {
        RunningAverage {
            max_samples,
            samples: VecDeque::with_capacity(max_samples),
            sum_samples: 0.0,
            total_sample_count: 0,
            first_sample_time: None,
        }
    }

    pub(crate) fn insert_with_time(&mut self, now: Instant, value: f64) {
        // Require that now is after the last sample time
        if let Some((last_sample_time, _)) = self.samples.back() {
            debug_assert!(now >= *last_sample_time);
        }
        if self.first_sample_time.is_none() {
            self.first_sample_time = Some(now);
        }
        self.total_sample_count += 1;
        if self.samples.len() < self.max_samples {
            self.samples.push_back((now, value));
            self.sum_samples += value;
        } else if let Some((_, old_value)) = self.samples.pop_front() {
            self.samples.push_back((now, value));
            self.sum_samples += value - old_value;
        }
    }

    pub(crate) fn get_rate_at_time(&self, now: Instant) -> Option<Rate> {
        if self.samples.is_empty() {
            return None;
        }
        let oldest_sample_time = self.samples.front().unwrap().0;
        let sample_duration = now - oldest_sample_time;
        const MINIMUM_TIME_WINDOW: Duration = Duration::from_secs(1);
        let divisor = sample_duration.max(MINIMUM_TIME_WINDOW);
        Some(Rate::new(self.sum_samples, divisor))
    }

    /// Cost-axis rate read for the cost-pressure eviction trigger
    /// (cost-aware eviction, #4861). Differs from [`Self::get_rate_at_time`]
    /// in two load-bearing ways:
    ///
    /// 1. **Sparse samples are diluted by `min_window`** instead of the
    ///    1-second clamp: a single large sample reported moments before the
    ///    read would otherwise produce an enormous instantaneous rate
    ///    (`value / 1s`), making a lone broadcast look like a sustained
    ///    storm to an EVICTION decision.
    /// 2. **A saturated buffer divides by its ACTUAL span**, not the minimum
    ///    window: the ring buffer keeps at most `max_samples` samples, so a
    ///    sustained high-frequency storm's retained samples may span only a
    ///    few tens of seconds — dividing that sum by `min_window` would cap
    ///    the representable rate at `max_samples × avg_value / min_window`,
    ///    silently under-reading the exact storm profile the trigger exists
    ///    to catch (a full buffer PROVES the activity filled the whole
    ///    retained span, so its sum/span is the true rate — no dilution
    ///    needed). Burst protection for the saturated case comes from
    ///    [`WindowedRate::first_sample_age`]: the caller requires sustained
    ///    reporting before acting.
    pub(crate) fn windowed_rate(&self, now: Instant, min_window: Duration) -> Option<WindowedRate> {
        if self.samples.is_empty() {
            return None;
        }
        let oldest_sample_time = self.samples.front().unwrap().0;
        let retained_span = now.saturating_duration_since(oldest_sample_time);
        let saturated = self.samples.len() >= self.max_samples;
        let divisor = if saturated {
            retained_span.max(Duration::from_secs(1))
        } else {
            retained_span.max(min_window).max(Duration::from_secs(1))
        };
        let first_sample_age = self
            .first_sample_time
            .map(|t| now.saturating_duration_since(t))
            .unwrap_or_default();
        Some(WindowedRate {
            rate: Rate::new(self.sum_samples, divisor),
            first_sample_age,
        })
    }
}

/// Result of [`RunningAverage::windowed_rate`]: the min-window-aware rate
/// plus how long this average has been receiving samples (the sustained-
/// pressure input for the cost-eviction trigger, #4861).
pub(crate) struct WindowedRate {
    pub rate: Rate,
    /// `now − first_sample_time`: how long ago this source FIRST reported.
    /// NOT the oldest retained sample's age — count-truncation keeps that
    /// young for high-frequency sources, which is exactly when the sustained
    /// signal matters.
    pub first_sample_age: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert() {
        let max_samples = 3;
        let mut running_avg = RunningAverage::new(max_samples);
        let now = Instant::now();

        running_avg.insert_with_time(now, 2.0);
        assert_eq!(running_avg.samples.len(), 1);
        assert_eq!(running_avg.sum_samples, 2.0);

        running_avg.insert_with_time(now + Duration::from_secs(1), 4.0);
        assert_eq!(running_avg.samples.len(), 2);
        assert_eq!(running_avg.sum_samples, 6.0);

        running_avg.insert_with_time(now + Duration::from_secs(2), 6.0);
        assert_eq!(running_avg.samples.len(), 3);
        assert_eq!(running_avg.sum_samples, 12.0);

        // Test that the oldest value is removed when max_samples is exceeded
        running_avg.insert_with_time(now + Duration::from_secs(3), 8.0);
        assert_eq!(running_avg.samples.len(), 3);
        assert_eq!(running_avg.sum_samples, 18.0);
    }

    #[test]
    fn test_per_second_measurement() {
        let max_samples = 3;
        let mut running_avg = RunningAverage::new(max_samples);
        let now = Instant::now();

        // Test with no samples
        assert!(running_avg.get_rate_at_time(now).is_none());

        // Test with one sample
        running_avg.insert_with_time(now, 2.0);
        assert_eq!(
            running_avg
                .get_rate_at_time(now + Duration::from_secs(1))
                .unwrap()
                .per_second(),
            2.0
        );

        // Test with multiple samples
        running_avg.insert_with_time(now + Duration::from_secs(1), 4.0);
        running_avg.insert_with_time(now + Duration::from_secs(2), 6.0);
        assert_eq!(
            running_avg
                .get_rate_at_time(now + Duration::from_secs(3))
                .unwrap()
                .per_second(),
            4.0
        );

        // Test with max_samples exceeded
        running_avg.insert_with_time(now + Duration::from_secs(3), 8.0);
        assert_eq!(
            running_avg
                .get_rate_at_time(now + Duration::from_secs(4))
                .unwrap()
                .per_second(),
            6.0
        );
    }
}
