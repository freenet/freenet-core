use crate::topology::rate::Rate;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::Instant;

/// Maximum silence between two samples that still counts as ONE continuous
/// run of activity for the cost-eviction sustained gate (#4861 / #4903 review
/// BLOCKER). A source reporting at least this often is treated as
/// continuously active, so [`RunningAverage::windowed_rate`]'s `activity_span`
/// keeps growing; a gap longer than this restarts the run (so two separated
/// bursts never sum into a false "sustained", and an old-first-sample source
/// followed by a fresh burst is not sustained). It is deliberately
/// buffer-CAPACITY-independent: the previous sustained signal (span of the
/// count-bounded sample buffer) INVERTED against high-frequency storms — a
/// buffer of `max_samples` at a fast cadence spans only a few tens of seconds
/// and could never reach `min_window / 2`, so the fastest (worst) storms were
/// permanently exempted from candidacy. ~min_window/5 for the 300 s cost
/// window; a genuinely-storming contract fans out far more often than once a
/// minute, so 60 s of cost-stream silence means the storm has paused.
pub(crate) const SUSTAINED_ACTIVITY_MAX_GAP: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub(crate) struct RunningAverage {
    max_samples: usize,
    samples: VecDeque<(Instant, f64)>,
    sum_samples: f64,
    total_sample_count: usize,
    /// Start of the current continuous run of activity (gap-reset on a silence
    /// longer than [`SUSTAINED_ACTIVITY_MAX_GAP`]). Tracked at INSERT time so
    /// it survives count-bounding — a high-frequency source drops old samples
    /// but its activity start is remembered here. Read by
    /// [`Self::windowed_rate`] as the buffer-capacity-independent
    /// sustained-activity signal for the cost-eviction trigger (#4861).
    activity_start: Option<Instant>,
}

impl RunningAverage {
    pub fn new(max_samples: usize) -> Self {
        RunningAverage {
            max_samples,
            samples: VecDeque::with_capacity(max_samples),
            sum_samples: 0.0,
            total_sample_count: 0,
            activity_start: None,
        }
    }

    pub(crate) fn insert_with_time(&mut self, now: Instant, value: f64) {
        // Continuous-activity tracking for the cost-eviction sustained gate
        // (#4861): a gap longer than SUSTAINED_ACTIVITY_MAX_GAP since the last
        // sample (or the very first sample) starts a fresh run. This is
        // computed from the last sample time BEFORE the push below, and is
        // independent of the count-bounded buffer, so a fast storm whose old
        // samples are evicted still remembers when its run began.
        let gap_restart = match self.samples.back() {
            Some((last_sample_time, _)) => {
                debug_assert!(now >= *last_sample_time);
                now.saturating_duration_since(*last_sample_time) > SUSTAINED_ACTIVITY_MAX_GAP
            }
            None => true,
        };
        if gap_restart || self.activity_start.is_none() {
            self.activity_start = Some(now);
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
    ///    for the saturated case comes from [`WindowedRate::activity_span`]:
    ///    the caller requires a sustained continuous run before acting.
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
        // Sustained-activity signal (#4903 review BLOCKER): the length of the
        // current continuous run, measured from `activity_start` (set/reset at
        // insert time, see SUSTAINED_ACTIVITY_MAX_GAP) to the newest sample.
        // Deliberately NOT the span of the count-bounded sample buffer — that
        // span shrinks with cadence, so a fast storm's buffer spans only a few
        // tens of seconds and could never reach the caller's `min_window / 2`
        // bar (the inversion this replaces). `activity_start` survives
        // count-bounding, so the run length is representable at any rate. A
        // single burst (short run) still can't reach the bar, and a gap longer
        // than SUSTAINED_ACTIVITY_MAX_GAP restarts the run, so an old-first-
        // sample source followed by a fresh burst is not sustained either.
        //
        // A run only counts as CURRENT if the newest sample is itself recent
        // (within SUSTAINED_ACTIVITY_MAX_GAP of `now`): a source that stormed
        // and then fell silent is no longer sustained, so it drops out of
        // candidacy after one gap of silence (well before its samples fully
        // age out of `min_window` and its rate decays to zero), rather than
        // lingering as a candidate on the strength of a run that has ended.
        let currently_active = now.saturating_duration_since(newest) <= SUSTAINED_ACTIVITY_MAX_GAP;
        let activity_span = match self.activity_start {
            Some(start) if currently_active => newest.saturating_duration_since(start),
            _ => Duration::ZERO,
        };
        Some(WindowedRate {
            rate: Rate::new(recent_sum, divisor),
            activity_span,
        })
    }
}

/// Result of [`RunningAverage::windowed_rate`]: the min-window-aware rate
/// plus the length of the current continuous activity run (the
/// sustained-pressure input for the cost-eviction trigger, #4861).
pub(crate) struct WindowedRate {
    pub rate: Rate,
    /// `newest_recent_sample − activity_start`: how long the source has been
    /// CONTINUOUSLY active (gap-reset on a silence longer than
    /// [`SUSTAINED_ACTIVITY_MAX_GAP`]). A genuine sustained storm's run grows
    /// past `min_window / 2` at ANY report cadence above the floor; a short
    /// burst's run stays small no matter how dense; an old-first-sample source
    /// followed by a fresh burst has its run restart at the burst. This is
    /// buffer-CAPACITY-independent — the fix for the review BLOCKER, where the
    /// count-bounded buffer's span inverted against high-frequency storms
    /// (fast cadence ⇒ short buffer span ⇒ never "sustained" ⇒ the worst
    /// storms were permanently exempted from candidacy).
    pub activity_span: Duration,
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
    /// unsaturated dilution, the continuity-tracked `activity_span` sustained
    /// signal (#4903 review BLOCKER), and the review-Fix-3 time-bound.
    mod windowed_rate {
        use super::*;

        const MIN_WINDOW: Duration = Duration::from_secs(300);

        #[test]
        fn empty_buffer_returns_none() {
            let avg = RunningAverage::new(3);
            assert!(avg.windowed_rate(Instant::now(), MIN_WINDOW).is_none());
        }

        /// A lone sample moments before the read is diluted over the whole
        /// `min_window` (never `value / ~1s`), and its activity span is zero
        /// — a burst can neither read high nor look sustained.
        #[test]
        fn single_sample_is_diluted_by_min_window() {
            let mut avg = RunningAverage::new(100);
            let now = Instant::now();
            avg.insert_with_time(now - Duration::from_secs(1), 3_000.0);
            let windowed = avg.windowed_rate(now, MIN_WINDOW).unwrap();
            assert!((windowed.rate.per_second() - 3_000.0 / 300.0).abs() < 1e-9);
            assert_eq!(windowed.activity_span, Duration::ZERO);
        }

        /// A saturated buffer whose every sample is recent reads its TRUE
        /// rate over `now − oldest_retained` — the count-truncation fix that
        /// makes the #4861 storm profile representable. The `activity_span`
        /// spans the whole continuous run (from the FIRST sample, which the
        /// count-bounded buffer has since evicted), not just the retained
        /// samples — the BLOCKER fix.
        #[test]
        fn saturated_recent_buffer_divides_by_actual_span() {
            let mut avg = RunningAverage::new(3);
            let t0 = Instant::now();
            for i in 0..5u64 {
                avg.insert_with_time(t0 + Duration::from_secs(10 * i), 100.0);
            }
            // Retained samples: t0+20/+30/+40; read 10s after the last. Rate
            // divides by the retained span (30s); activity_span covers the
            // whole run from t0 (the evicted first sample) to the newest (40s).
            let now = t0 + Duration::from_secs(50);
            let windowed = avg.windowed_rate(now, MIN_WINDOW).unwrap();
            assert!((windowed.rate.per_second() - 300.0 / 30.0).abs() < 1e-9);
            assert_eq!(windowed.activity_span, Duration::from_secs(40));
        }

        /// An unsaturated buffer is diluted by `min_window` even when its
        /// samples span less: sparse reporting cannot read a high rate. Two
        /// samples 60s apart are one continuous run (gap ≤ the max gap), so
        /// the activity span is the full 60s.
        #[test]
        fn unsaturated_buffer_is_diluted_by_min_window() {
            let mut avg = RunningAverage::new(100);
            let t0 = Instant::now();
            avg.insert_with_time(t0, 600.0);
            avg.insert_with_time(t0 + Duration::from_secs(60), 600.0);
            let now = t0 + Duration::from_secs(70);
            let windowed = avg.windowed_rate(now, MIN_WINDOW).unwrap();
            assert!((windowed.rate.per_second() - 1_200.0 / 300.0).abs() < 1e-9);
            assert_eq!(windowed.activity_span, Duration::from_secs(60));
        }

        /// The sustained signal is the length of the CURRENT continuous run,
        /// not the lifetime span (#4903 review BLOCKER + Fix 2): an old
        /// first-ever sample followed by a fresh burst yields a SMALL activity
        /// span, because the >`SUSTAINED_ACTIVITY_MAX_GAP` silence restarts
        /// the run at the burst.
        #[test]
        fn old_first_sample_then_burst_has_small_activity_span() {
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
                windowed.activity_span < Duration::from_secs(60),
                "a 60s flurry after a long silence must not look sustained: span {:?}",
                windowed.activity_span
            );
        }

        /// Same shape but the burst does NOT saturate the buffer, so the old
        /// sample is still RETAINED — the continuity gap-reset must still
        /// restart the run at the burst (activity span small), and the
        /// time-bound (review Fix 3) excludes the stale sample from the SUM.
        #[test]
        fn old_retained_sample_does_not_stretch_activity_span() {
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
                windowed.activity_span < Duration::from_secs(60),
                "the stale first sample must not stretch the run: {:?}",
                windowed.activity_span
            );
            // The stale sample is also excluded from the SUM.
            assert!((windowed.rate.per_second() - (40.0 * 58.0) / 300.0).abs() < 1e-9);
        }

        /// The BLOCKER regression: a high-frequency storm — whose
        /// count-bounded buffer spans only a few tens of seconds, far under
        /// `min_window / 2` — is nonetheless SUSTAINED, because `activity_span`
        /// tracks the continuous run (from `activity_start`), not the buffer
        /// span. Before the fix this storm read a tiny "retained span" and was
        /// permanently exempt from candidacy.
        #[test]
        fn fast_storm_activity_span_reaches_sustained_bar() {
            let mut avg = RunningAverage::new(100);
            let t0 = Instant::now();
            // 0.5s cadence for 200s continuous: the 100-sample buffer spans
            // only ~50s (< min_window/2 = 150s), but the run is 200s.
            for i in 0..400u64 {
                avg.insert_with_time(t0 + Duration::from_millis(500 * i), 58.0);
            }
            let last = t0 + Duration::from_millis(500 * 399);
            let windowed = avg
                .windowed_rate(last + Duration::from_secs(1), MIN_WINDOW)
                .unwrap();
            assert!(
                windowed.activity_span >= MIN_WINDOW / 2,
                "a continuous fast storm must be sustained regardless of buffer \
                 span: activity_span {:?}",
                windowed.activity_span
            );
        }

        /// A source that stormed and then fell silent drops out of the
        /// sustained signal after one `SUSTAINED_ACTIVITY_MAX_GAP` of silence
        /// (activity_span → 0), well before its samples age out — so it stops
        /// being a candidate promptly even while its rate is still decaying.
        #[test]
        fn silent_source_activity_span_collapses_after_gap() {
            let mut avg = RunningAverage::new(100);
            let t0 = Instant::now();
            for i in 0..150u64 {
                avg.insert_with_time(t0 + Duration::from_millis(1600 * i), 58.0);
            }
            let storm_end = t0 + Duration::from_millis(1600 * 149);
            // Live: sustained.
            let live = avg
                .windowed_rate(storm_end + Duration::from_secs(1), MIN_WINDOW)
                .unwrap();
            assert!(live.activity_span >= MIN_WINDOW / 2);
            // After a gap longer than the max gap but within min_window: the
            // rate is still positive but the run is no longer current.
            let stale = avg
                .windowed_rate(
                    storm_end + SUSTAINED_ACTIVITY_MAX_GAP + Duration::from_secs(1),
                    MIN_WINDOW,
                )
                .unwrap();
            assert!(stale.rate.per_second() > 0.0);
            assert_eq!(
                stale.activity_span,
                Duration::ZERO,
                "a source silent longer than the max gap is not currently sustained"
            );
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
