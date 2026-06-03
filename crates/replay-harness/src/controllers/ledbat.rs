//! `LedbatPlusPlus` — a stripped-down LEDBAT++ to reproduce the death spiral.
//!
//! This controller exists to *fail* in a specific, historically-real way, so
//! the harness can pin that failure mode as a regression any Phase 2 candidate
//! must not repeat. LEDBAT++ was one of the three congestion-control schemes
//! abandoned in production (see [#4074][issue]); its core problem is that it is
//! a **per-connection** controller, so a transient excursion on a *single*
//! path (a reroute, one lost packet's retransmit) reads as congestion and
//! drives the whole rate down with a multiplicative decrease that then takes a
//! long time to additively recover.
//!
//! The harness models that single-connection sensitivity by reacting to the
//! **worst** (max) per-peer queueing delay rather than the cross-peer median
//! that [`RfcDraft`](super::rfc_draft::RfcDraft) uses. That one line is the
//! whole difference between the controller that death-spirals and the one that
//! doesn't: on `single_packet_loss` a single peer's transient spike sends this
//! controller into a deep cut + slow recovery, while RfcDraft's cross-peer
//! median rejects the same outlier and holds.
//!
//! It is deliberately *stripped down* — real LEDBAT++ filters per-packet RTT,
//! runs slow-start, and has a richer gain schedule. We keep only what is needed
//! to reproduce the asymmetric drop/recover dynamic on the harness's 1 Hz,
//! median-filtered snapshot signal.
//!
//! [issue]: https://github.com/freenet/freenet-core/issues/4074

use std::time::Duration;

use super::{Controller, ControllerView, RateDecision};

/// Target queueing delay. Above this, LEDBAT++ treats the path as congested
/// and multiplicatively decreases. Classic LEDBAT targets 100 ms; LEDBAT++
/// lowered it to 60 ms to yield more aggressively.
const TARGET: Duration = Duration::from_millis(60);

/// Multiplicative-decrease factor applied on every congested tick. Compounding
/// this across a sustained over-target window is what turns a single transient
/// spike into a deep cut.
const MD_FACTOR: f64 = 0.7;

/// Hard floor so a long death spiral can't divide the rate to zero (which would
/// make the recovery ratio meaningless). Not a real LEDBAT parameter — purely a
/// harness guard.
const FLOOR_BPS: u64 = 10_000;

#[derive(Debug, Default)]
pub struct LedbatPlusPlus {
    /// The rate observed on the first tick, used as the additive-increase
    /// ceiling so the controller recovers toward (but never above) its
    /// starting point. Captured rather than hard-coded so it tracks whatever
    /// starting rate the [`Replayer`](crate::Replayer) was configured with.
    ceiling: Option<u64>,
}

impl Controller for LedbatPlusPlus {
    fn name(&self) -> &str {
        "ledbat"
    }

    fn tick(&mut self, view: ControllerView<'_>) -> RateDecision {
        let ceiling = *self.ceiling.get_or_insert(view.current_aggregate_rate_bps);
        // Additive increase of 1% of the ceiling per tick. The asymmetry
        // between this slow linear recovery and the compounding multiplicative
        // cut below is precisely the death-spiral dynamic.
        let ai_step = (ceiling / 100).max(1);

        // Per-connection congestion view: react to the single worst path, not a
        // cross-peer aggregate. This is the modelled flaw.
        let worst = view
            .per_peer
            .iter()
            .filter_map(|p| p.snapshot.inflation)
            .max();

        let rate = view.current_aggregate_rate_bps;
        match worst {
            // No defined inflation anywhere yet (cold network): nothing to act on.
            None => RateDecision::Hold,
            Some(queue_delay) if queue_delay > TARGET => {
                // Multiplicative decrease, floored. The trailing `.min(rate)`
                // keeps a "decrease" from ever raising the rate: if the
                // configured starting rate is already below FLOOR_BPS, the
                // `.max(FLOOR_BPS)` would otherwise clamp *upward* and emit an
                // increase mislabelled as a cut.
                let new = ((rate as f64 * MD_FACTOR).round() as u64)
                    .max(FLOOR_BPS)
                    .min(rate);
                decide(rate, new, "ledbat_md_on_queue_delay")
            }
            Some(_) => {
                let new = rate.saturating_add(ai_step).min(ceiling);
                decide(rate, new, "ledbat_ai_below_target")
            }
        }
    }
}

/// Emit a `Set` only when the rate actually changes, so a controller sitting at
/// the ceiling (or the floor) records `Hold` rather than a stream of no-op sets.
fn decide(old: u64, new: u64, reason: &'static str) -> RateDecision {
    if new == old {
        RateDecision::Hold
    } else {
        RateDecision::Set {
            aggregate_rate_bps: new,
            reason,
        }
    }
}
