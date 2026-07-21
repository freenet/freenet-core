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
}

impl RunningAverage {
    pub fn new(max_samples: usize) -> Self {
        RunningAverage {
            max_samples,
            samples: VecDeque::with_capacity(max_samples),
            sum_samples: 0.0,
            total_sample_count: 0,
        }
    }

    pub(crate) fn insert_with_time(&mut self, now: Instant, value: f64) {
        // Require that now is after the last sample time
        if let Some((last_sample_time, _)) = self.samples.back() {
            debug_assert!(now >= *last_sample_time);
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
    /// in three load-bearing ways:
    ///
    /// 1. **Only samples within the last `min_window` participate** (the
    ///    time-bound, review Fix 3). The sample buffer is COUNT-bounded, so
    ///    without this a source that stormed and then went quiet would keep
    ///    reading its stale high rate for as long as the buffer happened to
    ///    retain the old samples — feeding both the eviction candidacy and
    ///    the node-total denominator minutes after the activity stopped. The
    ///    cost rate is defined as "attributed work within the last
    ///    `min_window`", so a quiet source decays: partially at first (stale
    ///    samples drop out of the sum), and to `None` once every retained
    ///    sample is older than `min_window`.
    /// 2. **Sparse samples are diluted by `min_window`** instead of the
    ///    1-second clamp: a single large sample reported moments before the
    ///    read would otherwise produce an enormous instantaneous rate
    ///    (`value / 1s`), making a lone broadcast look like a sustained
    ///    storm to an EVICTION decision.
    /// 3. **A fully-recent saturated buffer divides by its ACTUAL span**,
    ///    not the minimum window: the ring buffer keeps at most
    ///    `max_samples` samples, so a sustained high-frequency storm's
    ///    retained samples may span only a few tens of seconds — dividing
    ///    that sum by `min_window` would cap the representable rate at
    ///    `max_samples × avg_value / min_window`, silently under-reading the
    ///    exact storm profile the trigger exists to catch (a full buffer
    ///    whose EVERY sample is recent proves the activity filled the whole
    ///    retained span, so its sum/span is the true rate — no dilution
    ///    needed). If any retained sample was excluded by the time-bound,
    ///    the remaining recent subset no longer proves saturation density
    ///    and the diluted (`min_window`) divisor applies. Burst protection
    ///    for the saturated case comes from [`WindowedRate::retained_span`]:
    ///    the caller requires the recent samples to SPAN a sustained period
    ///    before acting.
    pub(crate) fn windowed_rate(&self, now: Instant, min_window: Duration) -> Option<WindowedRate> {
        // Time-bound (review Fix 3): only samples no older than `min_window`
        // count. Samples are appended in time order, so scanning the whole
        // (small, count-bounded) buffer is cheap and needs no index math.
        let mut recent_sum = 0.0_f64;
        let mut oldest_recent: Option<Instant> = None;
        let mut newest_recent: Option<Instant> = None;
        let mut excluded_any = false;
        for (at, value) in &self.samples {
            if now.saturating_duration_since(*at) > min_window {
                excluded_any = true;
                continue;
            }
            if oldest_recent.is_none() {
                oldest_recent = Some(*at);
            }
            newest_recent = Some(*at);
            recent_sum += value;
        }
        // Every retained sample is stale (or the buffer is empty): the
        // source has done no attributable work within the window.
        let oldest = oldest_recent?;
        let newest = newest_recent.expect("newest set whenever oldest is");
        // The span the recent samples actually cover (newest − oldest): the
        // sustained-activity signal (review Fix 2). Deliberately NOT the
        // lifetime first-sample age — that is set once and never reset, so a
        // source whose first-ever sample was old would pass a "sustained"
        // gate forever and a later short burst could nominate it.
        let retained_span = newest.saturating_duration_since(oldest);
        let since_oldest = now.saturating_duration_since(oldest);
        // "Saturated" (true-rate) reading requires the full buffer AND no
        // time-excluded sample: only then does the buffer prove continuous
        // activity across its span. See rustdoc point 3.
        let saturated = self.samples.len() >= self.max_samples && !excluded_any;
        let divisor = if saturated {
            since_oldest.max(Duration::from_secs(1))
        } else {
            since_oldest.max(min_window).max(Duration::from_secs(1))
        };
        Some(WindowedRate {
            rate: Rate::new(recent_sum, divisor),
            retained_span,
        })
    }
}

/// Result of [`RunningAverage::windowed_rate`]: the min-window-aware rate
/// plus how long the RECENT (within-window) samples actually span (the
/// sustained-pressure input for the cost-eviction trigger, #4861).
pub(crate) struct WindowedRate {
    pub rate: Rate,
    /// `newest_recent_sample − oldest_recent_sample`: the period the
    /// currently-held, within-window samples actually cover. A genuine
    /// sustained storm's recent samples span a large fraction of the window;
    /// a short burst's span only a few tens of seconds — no matter how old
    /// the source's FIRST-EVER sample is (the lifetime first-sample age was
    /// the review-Fix-2 hole: set once, never reset, so an old-first-sample
    /// source passed the sustained gate forever).
    pub retained_span: Duration,
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

    /// Direct coverage for the cost-axis read (`windowed_rate`, #4861):
    /// empty buffer, single-sample dilution, saturated true-rate,
    /// unsaturated dilution, the retained-span sustained signal, and the
    /// review-Fix-3 time-bound.
    mod windowed_rate {
        use super::*;

        const MIN_WINDOW: Duration = Duration::from_secs(300);

        #[test]
        fn empty_buffer_returns_none() {
            let avg = RunningAverage::new(3);
            assert!(avg.windowed_rate(Instant::now(), MIN_WINDOW).is_none());
        }

        /// A lone sample moments before the read is diluted over the whole
        /// `min_window` (never `value / ~1s`), and its retained span is zero
        /// — a burst can neither read high nor look sustained.
        #[test]
        fn single_sample_is_diluted_by_min_window() {
            let mut avg = RunningAverage::new(100);
            let now = Instant::now();
            avg.insert_with_time(now - Duration::from_secs(1), 3_000.0);
            let windowed = avg.windowed_rate(now, MIN_WINDOW).unwrap();
            assert!((windowed.rate.per_second() - 3_000.0 / 300.0).abs() < 1e-9);
            assert_eq!(windowed.retained_span, Duration::ZERO);
        }

        /// A saturated buffer whose every sample is recent reads its TRUE
        /// rate over `now − oldest_retained` — the count-truncation fix that
        /// makes the #4861 storm profile representable.
        #[test]
        fn saturated_recent_buffer_divides_by_actual_span() {
            let mut avg = RunningAverage::new(3);
            let t0 = Instant::now();
            for i in 0..5u64 {
                avg.insert_with_time(t0 + Duration::from_secs(10 * i), 100.0);
            }
            // Retained: samples at t0+20/+30/+40; read 10s after the last.
            let now = t0 + Duration::from_secs(50);
            let windowed = avg.windowed_rate(now, MIN_WINDOW).unwrap();
            assert!((windowed.rate.per_second() - 300.0 / 30.0).abs() < 1e-9);
            assert_eq!(windowed.retained_span, Duration::from_secs(20));
        }

        /// An unsaturated buffer is diluted by `min_window` even when its
        /// samples span less: sparse reporting cannot read a high rate.
        #[test]
        fn unsaturated_buffer_is_diluted_by_min_window() {
            let mut avg = RunningAverage::new(100);
            let t0 = Instant::now();
            avg.insert_with_time(t0, 600.0);
            avg.insert_with_time(t0 + Duration::from_secs(60), 600.0);
            let now = t0 + Duration::from_secs(70);
            let windowed = avg.windowed_rate(now, MIN_WINDOW).unwrap();
            assert!((windowed.rate.per_second() - 1_200.0 / 300.0).abs() < 1e-9);
            assert_eq!(windowed.retained_span, Duration::from_secs(60));
        }

        /// The sustained signal is the span of the RECENT samples, not the
        /// lifetime first-sample age (review Fix 2): an old first-ever
        /// sample followed by a fresh saturating burst yields a SMALL
        /// retained span, because the old sample is either count-evicted or
        /// time-excluded.
        #[test]
        fn old_first_sample_then_burst_has_small_retained_span() {
            let mut avg = RunningAverage::new(100);
            let t0 = Instant::now();
            // First-ever sample, 10 minutes before the read.
            avg.insert_with_time(t0, 1.0);
            // A 60-second buffer-saturating flurry just before the read
            // (120 samples: the old sample is pushed out of the buffer).
            let burst_start = t0 + Duration::from_secs(540);
            for i in 0..120u64 {
                avg.insert_with_time(burst_start + Duration::from_millis(500 * i), 58.0);
            }
            let now = t0 + Duration::from_secs(601);
            let windowed = avg.windowed_rate(now, MIN_WINDOW).unwrap();
            assert!(
                windowed.retained_span < Duration::from_secs(60),
                "a 60s flurry must not look sustained: span {:?}",
                windowed.retained_span
            );
        }

        /// Same shape but the burst does NOT saturate the buffer, so the old
        /// sample is still RETAINED — the time-bound (review Fix 3) must
        /// exclude it, keeping the retained span small.
        #[test]
        fn old_retained_sample_is_time_excluded_from_span() {
            let mut avg = RunningAverage::new(100);
            let t0 = Instant::now();
            avg.insert_with_time(t0, 1.0);
            let burst_start = t0 + Duration::from_secs(560);
            for i in 0..40u64 {
                avg.insert_with_time(burst_start + Duration::from_secs(i), 58.0);
            }
            let now = t0 + Duration::from_secs(601);
            let windowed = avg.windowed_rate(now, MIN_WINDOW).unwrap();
            assert!(
                windowed.retained_span < Duration::from_secs(60),
                "the stale first sample must not stretch the span: {:?}",
                windowed.retained_span
            );
            // The stale sample is also excluded from the SUM.
            assert!((windowed.rate.per_second() - (40.0 * 58.0) / 300.0).abs() < 1e-9);
        }

        /// The time-bound (review Fix 3): a source that stops reporting
        /// decays — stale samples drop out of the sum, and once EVERY
        /// retained sample is older than `min_window` the read is `None` —
        /// instead of holding its stale storm rate for as long as the
        /// count-bounded buffer retains the samples.
        #[test]
        fn quiet_source_decays_to_none_within_min_window() {
            let mut avg = RunningAverage::new(100);
            let t0 = Instant::now();
            // A saturating storm: 150 samples at 1.6s cadence (~158s span).
            for i in 0..150u64 {
                avg.insert_with_time(t0 + Duration::from_millis(1600 * i), 58.0);
            }
            let storm_end = t0 + Duration::from_millis(1600 * 149);

            // Read DURING the storm: the true rate (all samples recent).
            let live = avg
                .windowed_rate(storm_end + Duration::from_secs(1), MIN_WINDOW)
                .unwrap();
            assert!(
                live.rate.per_second() > 30.0,
                "an active storm must read its true rate, got {}",
                live.rate.per_second()
            );

            // 180s of silence: the oldest samples have aged past the
            // window, so the sum shrinks AND the diluted divisor applies —
            // the rate must have decayed well below the live reading.
            let partly_stale = avg
                .windowed_rate(storm_end + Duration::from_secs(180), MIN_WINDOW)
                .unwrap();
            assert!(
                partly_stale.rate.per_second() < live.rate.per_second() / 2.0,
                "a quiet source must decay, got {}",
                partly_stale.rate.per_second()
            );

            // Once every sample is stale, the source reads None.
            assert!(
                avg.windowed_rate(storm_end + MIN_WINDOW + Duration::from_secs(1), MIN_WINDOW)
                    .is_none(),
                "a source quiet for a full min_window must read None"
            );
        }
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
