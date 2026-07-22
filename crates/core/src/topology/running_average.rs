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

/// Above-floor sustained-run configuration for a contract COST axis (#4861
/// Codex round-3). Present only on the three cost-pressure axes; every other
/// [`RunningAverage`] leaves it absent and keeps the sample-continuity
/// (`activity_start`) sustained signal untouched.
#[derive(Clone, Copy, Debug)]
struct CostSustainedConfig {
    /// Axis floor (units/sec). A run counts as "above floor" for as long as
    /// the true windowed rate stays at or above this.
    floor: f64,
    /// Window the above-floor rate is measured over at insert time. MUST be
    /// the same window the reader passes to [`RunningAverage::windowed_rate`]
    /// (`COST_RATE_MIN_WINDOW`) so the insert-time detection and the read-time
    /// rate agree on which samples are in scope.
    window: Duration,
}

#[derive(Clone, Debug)]
pub(crate) struct RunningAverage {
    max_samples: usize,
    samples: VecDeque<(Instant, f64)>,
    sum_samples: f64,
    total_sample_count: usize,
    /// Start of the current continuous run of SAMPLING activity (gap-reset on
    /// a silence longer than [`SUSTAINED_ACTIVITY_MAX_GAP`]). Tracked at INSERT
    /// time so it survives count-bounding — a high-frequency source drops old
    /// samples but its activity start is remembered here. For a NON-cost
    /// average this is the sustained signal read by [`Self::windowed_rate`];
    /// for a cost average the above-floor run (`above_floor_start`) is read
    /// instead, but this field is still maintained cheaply and unchanged.
    activity_start: Option<Instant>,
    /// Above-floor sustained-run config for the contract cost axes (#4861
    /// Codex round-3). `None` for every other average.
    cost_sustained: Option<CostSustainedConfig>,
    /// Start of the current continuous ABOVE-FLOOR run: the instant the true
    /// windowed rate first reached the axis floor and has stayed there since.
    /// Gap-reset on a >[`SUSTAINED_ACTIVITY_MAX_GAP`] silence and cleared the
    /// moment the rate drops below the floor. Maintained only when
    /// `cost_sustained` is set; read by [`Self::windowed_rate`] as the cost
    /// axes' sustained signal so a below-floor keep-alive trickle that merely
    /// keeps SAMPLING continuous can never look sustained (the round-3 fix:
    /// `activity_span` proved sustained SAMPLING, not sustained COST).
    above_floor_start: Option<Instant>,
}

impl RunningAverage {
    pub fn new(max_samples: usize) -> Self {
        RunningAverage {
            max_samples,
            samples: VecDeque::with_capacity(max_samples),
            sum_samples: 0.0,
            total_sample_count: 0,
            activity_start: None,
            cost_sustained: None,
            above_floor_start: None,
        }
    }

    /// Like [`Self::new`] but tracks a sustained ABOVE-FLOOR run for a contract
    /// cost axis: [`WindowedRate::activity_span`] then measures how long the
    /// true windowed rate has continuously stayed at or above `floor`
    /// (units/sec, measured over `window`), instead of how long the source has
    /// merely been SAMPLING. This is the cost-eviction candidacy gate's
    /// sustained input (#4861 Codex round-3): a source emitting trivial
    /// below-floor keep-alive samples plus one dense burst keeps sample
    /// continuity alive but never sustains above-floor cost, so it must not be
    /// a candidate. `window` must equal the window the reader passes to
    /// [`Self::windowed_rate`] (`COST_RATE_MIN_WINDOW`).
    pub(crate) fn new_cost_sustained(max_samples: usize, floor: f64, window: Duration) -> Self {
        RunningAverage {
            cost_sustained: Some(CostSustainedConfig { floor, window }),
            ..Self::new(max_samples)
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

        // Cost-axis ABOVE-FLOOR sustained-run tracking (#4861 Codex round-3).
        // Runs only for cost-configured averages, AFTER the push (so `now` is
        // in the buffer), and captures the run at INSERT time because the read
        // sees only the final count-bounded buffer — the early trickle samples
        // are gone by read time, so the above-floor history must be recorded
        // as samples arrive.
        if let Some(cfg) = self.cost_sustained {
            // Same gap-reset as `activity_start`: a >SUSTAINED_ACTIVITY_MAX_GAP
            // silence breaks the run, so a dense burst after a long quiet
            // cannot inherit an earlier above-floor run. (`gap_restart` was
            // computed from the last sample time BEFORE the push above.)
            if gap_restart {
                self.above_floor_start = None;
            }
            // True (undiluted) windowed rate at `now`: the in-window sum over
            // its ACTUAL span. Deliberately NOT diluted by `window` the way
            // `windowed_rate`'s MAGNITUDE reading is: that dilution exists to
            // stop a lone burst from reading a huge instantaneous rate, but the
            // boolean above-floor RUN already rejects lone bursts by run LENGTH
            // (a single dense burst's run is only its own few seconds). Diluting
            // here would instead impose a spurious warm-up — a genuine fast
            // storm reads below floor until ~half the buffer fills — that
            // shortens its sustained run below the bar and stops it nominating
            // (regressing the fast-storm guarantee). The true delivery rate is
            // the correct "is this source above the floor" signal; the run
            // length is the correct "is it sustained" signal.
            let above_floor = self
                .windowed_scan(now, cfg.window)
                .map(|scan| {
                    let span = now
                        .saturating_duration_since(scan.oldest)
                        .max(Duration::from_secs(1));
                    Rate::new(scan.recent_sum, span).per_second() >= cfg.floor
                })
                .unwrap_or(false);
            if above_floor {
                // Open the run at the first above-floor sample; keep the
                // earlier start once the run is already open.
                self.above_floor_start.get_or_insert(now);
            } else {
                self.above_floor_start = None;
            }
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
        let scan = self.windowed_scan(now, min_window)?;
        let since_oldest = now.saturating_duration_since(scan.oldest);
        // "Saturated" (true-rate) reading requires the full buffer AND no
        // time-excluded sample: only then does the buffer prove continuous
        // activity across its span (see [`WindowScan`] and rustdoc point 3).
        let divisor = if scan.saturated {
            since_oldest.max(Duration::from_secs(1))
        } else {
            since_oldest.max(min_window).max(Duration::from_secs(1))
        };
        // Sustained-activity signal. For a COST-configured average this is the
        // ABOVE-FLOOR run (`above_floor_start`, tracked at insert time): the
        // run counts only while the true windowed rate stays >= the axis
        // floor, so a below-floor keep-alive trickle that merely keeps SAMPLING
        // continuous is NOT sustained (#4861 Codex round-3). For every other
        // average it is the sample-continuity run (`activity_start`, #4903
        // review BLOCKER), unchanged: the length of the current continuous
        // run, buffer-CAPACITY-independent so a fast storm whose count-bounded
        // buffer spans only tens of seconds is still recognized as sustained.
        // A single burst (short run) can't reach the caller's `min_window / 2`
        // bar, and a >SUSTAINED_ACTIVITY_MAX_GAP silence restarts the run.
        //
        // A run only counts as CURRENT if the newest sample is itself recent
        // (within SUSTAINED_ACTIVITY_MAX_GAP of `now`): a source that stormed
        // and then fell silent is no longer sustained, so it drops out of
        // candidacy after one gap of silence (well before its samples fully
        // age out of `min_window` and its rate decays to zero), rather than
        // lingering as a candidate on the strength of a run that has ended.
        let run_start = if self.cost_sustained.is_some() {
            self.above_floor_start
        } else {
            self.activity_start
        };
        let currently_active =
            now.saturating_duration_since(scan.newest) <= SUSTAINED_ACTIVITY_MAX_GAP;
        let activity_span = match run_start {
            Some(start) if currently_active => scan.newest.saturating_duration_since(start),
            _ => Duration::ZERO,
        };
        Some(WindowedRate {
            rate: Rate::new(scan.recent_sum, divisor),
            activity_span,
        })
    }

    /// Shared count-and-time-bounded scan behind BOTH the read-time
    /// [`Self::windowed_rate`] and the insert-time above-floor check. Sums the
    /// samples within `window` of `now`, reporting the recent span endpoints
    /// and whether the retained buffer is SATURATED (full AND no sample was
    /// excluded by the time-bound — only then does it prove continuous activity
    /// across its span; see `windowed_rate` rustdoc point 3). Returns `None`
    /// when no sample lies within the window. Extracted so the two callers
    /// cannot diverge on the time-bound / saturation determination — the
    /// divisor POLICY differs by purpose (see the insert-time comment) but the
    /// scan is identical (#4861 Codex round-3).
    fn windowed_scan(&self, now: Instant, window: Duration) -> Option<WindowScan> {
        // Samples are appended in time order, so scanning the whole (small,
        // count-bounded) buffer is cheap and needs no index math.
        let mut recent_sum = 0.0_f64;
        let mut oldest_recent: Option<Instant> = None;
        let mut newest_recent: Option<Instant> = None;
        let mut excluded_any = false;
        for (at, value) in &self.samples {
            if now.saturating_duration_since(*at) > window {
                excluded_any = true;
                continue;
            }
            if oldest_recent.is_none() {
                oldest_recent = Some(*at);
            }
            newest_recent = Some(*at);
            recent_sum += value;
        }
        // Every retained sample is stale (or the buffer is empty): the source
        // has done no attributable work within the window.
        let oldest = oldest_recent?;
        let newest = newest_recent.expect("newest set whenever oldest is");
        Some(WindowScan {
            recent_sum,
            oldest,
            newest,
            saturated: self.samples.len() >= self.max_samples && !excluded_any,
        })
    }
}

/// In-window scan ingredients shared by [`RunningAverage::windowed_rate`] and
/// the insert-time above-floor check (#4861 Codex round-3).
struct WindowScan {
    /// Sum of the sample values within the window.
    recent_sum: f64,
    /// Oldest in-window sample time.
    oldest: Instant,
    /// Newest in-window sample time.
    newest: Instant,
    /// The buffer is full AND no retained sample was excluded by the
    /// time-bound (so its `sum / span` is the true rate).
    saturated: bool,
}

/// Result of [`RunningAverage::windowed_rate`]: the min-window-aware rate
/// plus the length of the current continuous activity run (the
/// sustained-pressure input for the cost-eviction trigger, #4861).
pub(crate) struct WindowedRate {
    pub rate: Rate,
    /// `newest_recent_sample − run_start`: how long the current run has been
    /// CONTINUOUSLY active (gap-reset on a silence longer than
    /// [`SUSTAINED_ACTIVITY_MAX_GAP`]). For a cost-configured average the run
    /// is the ABOVE-FLOOR run (`above_floor_start`): it grows only while the
    /// true windowed rate stays at or above the axis floor, so a below-floor
    /// keep-alive trickle that keeps SAMPLING continuous does not count and a
    /// single dense burst — even one preceded by a long below-floor trickle —
    /// stays short (#4861 Codex round-3). For every other average it is the
    /// sample-continuity run (`activity_start`). Either way a genuine sustained
    /// storm's run grows past `min_window / 2` at ANY report cadence above the
    /// floor; a short burst's run stays small no matter how dense; an
    /// old-first-sample source followed by a fresh burst has its run restart at
    /// the burst. Buffer-CAPACITY-independent — the fix for the earlier review
    /// BLOCKER, where the count-bounded buffer's span inverted against
    /// high-frequency storms (fast cadence ⇒ short buffer span ⇒ never
    /// "sustained" ⇒ the worst storms were permanently exempted from
    /// candidacy).
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

        // --- above-floor sustained-run tracking (#4861 Codex round-3) ---
        // These use the cost-configured constructor; the tests above use the
        // plain `new` (non-cost) path, which keeps the `activity_start`
        // sample-continuity signal unchanged.

        const FLOOR: f64 = 10.0;

        /// A below-floor keep-alive trickle (0.1/sample every 30s — under the
        /// 60s gap so SAMPLING stays continuous) followed by ONE dense burst is
        /// NOT sustained above-floor: the above-floor run opens only mid-burst
        /// and spans a few seconds, far under `min_window / 2`. This is the
        /// Codex round-3 scenario: `activity_start` alone (sampling continuity)
        /// would report ~180s here.
        #[test]
        fn cost_trickle_then_single_burst_run_is_short() {
            let mut avg = RunningAverage::new_cost_sustained(100, FLOOR, MIN_WINDOW);
            let t0 = Instant::now();
            let mut t = Duration::ZERO;
            while t <= Duration::from_secs(180) {
                avg.insert_with_time(t0 + t, 0.1);
                t += Duration::from_secs(30);
            }
            let burst_start = t0 + Duration::from_secs(181);
            for i in 0..120u64 {
                avg.insert_with_time(burst_start + Duration::from_millis(25 * i), 58.0);
            }
            let now = burst_start + Duration::from_secs(4);
            let windowed = avg.windowed_rate(now, MIN_WINDOW).unwrap();
            assert!(
                windowed.activity_span < MIN_WINDOW / 2,
                "a below-floor trickle then one burst is not sustained \
                 above-floor: span {:?}",
                windowed.activity_span
            );
        }

        /// The counterpart: the SAME dense cadence sustained ABOVE the floor for
        /// longer than `min_window / 2` IS sustained. At 1.4s cadence (58/sample
        /// ≈ 41/s, above the floor) the run reaches the bar over its FULL length
        /// — the above-floor detection uses the true delivery rate, so there is
        /// no diluted warm-up (the naive "reuse windowed_rate's diluted divisor"
        /// approach reads below floor for ~71s and would push this 200s run's
        /// sustained span below the 150s bar).
        #[test]
        fn cost_sustained_above_floor_reaches_bar_without_warmup_penalty() {
            let mut avg = RunningAverage::new_cost_sustained(100, FLOOR, MIN_WINDOW);
            let t0 = Instant::now();
            // i in 0..143 at 1.4s cadence → last sample at 142 × 1.4 = 198.8s.
            let n = 143u64;
            for i in 0..n {
                avg.insert_with_time(t0 + Duration::from_millis(1400 * i), 58.0);
            }
            let last = t0 + Duration::from_millis(1400 * (n - 1));
            let windowed = avg
                .windowed_rate(last + Duration::from_secs(1), MIN_WINDOW)
                .unwrap();
            assert!(
                windowed.activity_span >= MIN_WINDOW / 2,
                "a 1.4s-cadence above-floor storm must be sustained over its \
                 full run (no warm-up penalty): span {:?}",
                windowed.activity_span
            );
        }

        /// A source SAMPLING continuously (every 5s, never a gap) but whose true
        /// rate stays below the floor (1.0 per 5s = 0.2/s ≪ 10/s) never opens
        /// the above-floor run, so it is never sustained no matter how long it
        /// runs — cost-sustainedness is above-FLOOR cost, not mere sampling.
        #[test]
        fn cost_continuous_below_floor_source_never_sustained() {
            let mut avg = RunningAverage::new_cost_sustained(100, FLOOR, MIN_WINDOW);
            let t0 = Instant::now();
            let mut t = Duration::ZERO;
            while t <= Duration::from_secs(400) {
                avg.insert_with_time(t0 + t, 1.0);
                t += Duration::from_secs(5);
            }
            let now = t0 + Duration::from_secs(401);
            let windowed = avg.windowed_rate(now, MIN_WINDOW).unwrap();
            assert_eq!(
                windowed.activity_span,
                Duration::ZERO,
                "a continuously-sampling but below-floor source is never \
                 sustained above-floor"
            );
        }

        /// The gap-reset applies to the above-floor run too: a sustained
        /// above-floor storm, then a >`SUSTAINED_ACTIVITY_MAX_GAP` silence, then
        /// a short fresh storm — the run restarts at the second storm, so its
        /// span is only its own length (preserving the round-2 gap-reset).
        #[test]
        fn cost_gap_reset_restarts_above_floor_run() {
            let mut avg = RunningAverage::new_cost_sustained(100, FLOOR, MIN_WINDOW);
            let t0 = Instant::now();
            let mut t = Duration::ZERO;
            while t <= Duration::from_secs(200) {
                avg.insert_with_time(t0 + t, 58.0);
                t += Duration::from_millis(1600);
            }
            let resume = t0 + Duration::from_secs(200) + Duration::from_secs(90);
            let mut t2 = Duration::ZERO;
            while t2 <= Duration::from_secs(30) {
                avg.insert_with_time(resume + t2, 58.0);
                t2 += Duration::from_millis(1600);
            }
            let now = resume + Duration::from_secs(31);
            let windowed = avg.windowed_rate(now, MIN_WINDOW).unwrap();
            assert!(
                windowed.activity_span < MIN_WINDOW / 2,
                "the >max-gap silence restarts the above-floor run at the second \
                 storm: span {:?}",
                windowed.activity_span
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
