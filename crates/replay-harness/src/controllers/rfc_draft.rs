//! `RfcDraft` — the algorithm sketched in [#4074][issue].
//!
//! Implements the downstep branch as specified:
//!
//! ```text
//! if shared_inflation > 30 ms sustained for 5 s:
//!   aggregate_R *= 0.7
//!   freeze upward moves for 30 s
//! ```
//!
//! With the N≥3 guard the rolling-stats module's own rustdoc demands
//! ("any future controller built on this must require `recent.len() >= 3`
//! before treating the result as a contention signal"). The upward branch
//! (the "aggregate_outbound < 0.5 × aggregate_R for 60 s" idle-headroom
//! check) requires modelling outbound traffic, which the v1 harness does
//! not do — so this controller can downstep but never upsteps. That's
//! deliberate: the point of v1 is to demonstrate the *trigger* behaviour
//! on real telemetry, where Phase 1 analysis already showed the 30 ms
//! threshold sits below the noise floor.
//!
//! [issue]: https://github.com/freenet/freenet-core/issues/4074

use std::collections::VecDeque;
use std::time::Duration;

use super::{Controller, ControllerView, RateDecision};

/// The minimum population the cross-peer median is trustworthy on — per the
/// rolling-stats module rustdoc. At N≤2 the upper-middle median degenerates
/// to "the worse of the two".
const MIN_PEERS_FOR_SIGNAL: usize = 3;

/// Threshold from the #4074 sketch.
const INFLATION_TRIGGER: Duration = Duration::from_millis(30);

/// How long `shared_inflation` must stay above the trigger before we
/// downstep.
const SUSTAIN_WINDOW: Duration = Duration::from_secs(5);

/// Multiplicative downstep factor.
const DOWNSTEP_FACTOR: f64 = 0.7;

#[derive(Debug, Default)]
pub struct RfcDraft {
    /// (tick_time, shared_inflation) for the last `SUSTAIN_WINDOW` worth
    /// of ticks.
    inflation_history: VecDeque<(Duration, Duration)>,
}

impl Controller for RfcDraft {
    fn name(&self) -> &str {
        "rfc_draft"
    }

    fn tick(&mut self, view: ControllerView<'_>) -> RateDecision {
        // Collect per-peer inflations that are defined (peer has both a
        // baseline and a recent median).
        let mut inflations: Vec<Duration> = view
            .per_peer
            .iter()
            .filter_map(|p| p.snapshot.inflation)
            .collect();

        // N≥3 guard from rolling_rtt_stats.rs:cross_connection_median_inflation
        // rustdoc — without it we'd act on a "median of two" that is just
        // "the worse of the two".
        if inflations.len() < MIN_PEERS_FOR_SIGNAL {
            // Clear history so a transient small-N period doesn't carry
            // stale trigger samples into a later large-N window.
            self.inflation_history.clear();
            return RateDecision::Hold;
        }

        inflations.sort_unstable();
        let shared_inflation = inflations[inflations.len() / 2];

        // Window of recent shared_inflation values, pruned to SUSTAIN_WINDOW.
        self.inflation_history
            .push_back((view.now, shared_inflation));
        let cutoff = view.now.saturating_sub(SUSTAIN_WINDOW);
        while let Some(&(t, _)) = self.inflation_history.front() {
            if t < cutoff {
                self.inflation_history.pop_front();
            } else {
                break;
            }
        }

        // Trigger requires (a) every sample in the window exceeds the
        // threshold, AND (b) the window actually covers SUSTAIN_WINDOW.
        // The latter prevents a single tick at startup from firing on its
        // own ((a) trivially holds for a one-sample window).
        let all_above = self
            .inflation_history
            .iter()
            .all(|(_, infl)| *infl > INFLATION_TRIGGER);
        let oldest_age = self
            .inflation_history
            .front()
            .map(|(t, _)| view.now.saturating_sub(*t))
            .unwrap_or(Duration::ZERO);
        let window_covers = oldest_age >= SUSTAIN_WINDOW;

        if all_above && window_covers {
            let new_rate =
                (view.current_aggregate_rate_bps as f64 * DOWNSTEP_FACTOR).round() as u64;
            // Reset history so we don't keep firing on the same sustained
            // window — production would also freeze upward moves here, but
            // since v1 has no upward branch the freeze is a no-op.
            self.inflation_history.clear();
            return RateDecision::Set {
                aggregate_rate_bps: new_rate,
                reason: "shared_inflation_sustained_over_30ms",
            };
        }

        RateDecision::Hold
    }
}
