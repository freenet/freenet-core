//! `single_packet_loss` — one peer's RTT spikes transiently, then returns.
//!
//! Pins the LEDBAT++ death spiral. The original failure (see [#4074][issue])
//! was that a single packet loss / reroute on one connection drove a
//! multiplicative decrease that took 5–10 minutes to recover on high-RTT
//! paths. The harness has no loss event and its per-peer snapshot is a 10 s
//! median (which correctly filters a literal single sample), so we model the
//! event as a short RTT excursion on *one* peer: long enough to move that
//! peer's own recent median, but confined to a single connection.
//!
//! Two controllers must disagree here, and that disagreement is the whole
//! point:
//! - [`RfcDraft`](crate::controllers::RfcDraft) takes the cross-peer median,
//!   so one elevated peer among four is rejected — it MUST NOT fire.
//! - [`LedbatPlusPlus`](crate::controllers::ledbat::LedbatPlusPlus) reacts to
//!   the worst single connection, so it cuts hard on the spike and then
//!   recovers slowly — the death spiral, pinned in `tests/scenarios_pin.rs`.
//!
//! The scenario's *sane-controller* expectation is therefore `NeverFires`
//! (a robust controller ignores a single-connection transient); LEDBAT's
//! contrary behaviour is asserted explicitly as a documented failure mode.
//!
//! [issue]: https://github.com/freenet/freenet-core/issues/4074

use std::time::Duration;

use crate::event::Event;

use super::{Expectation, Scenario};

pub fn scenario() -> Scenario {
    let mut events = Vec::new();
    let n_peers = 4;
    let baseline_ms = 30;
    let spike_ms = 300; // 10× the baseline
    let spike_start = 60;
    // The spike must last long enough to tip peer-0's 10 s recent-median
    // window. At 1 Hz that window holds 11 samples and the upper-middle median
    // only flips to the spike value once >=6 of them are spike samples, so ~6 s
    // is the bare minimum that reproduces the death spiral at all. Use 10 s for
    // headroom — do NOT reduce below ~7 s or the median stops tipping and the
    // `ledbat_single_packet_loss_death_spiral` pin silently stops reproducing.
    let spike_end = 70;

    for peer_idx in 0..n_peers {
        let peer = format!("peer-{peer_idx}");
        for t in 0..180u64 {
            // peer-0 takes a transient 10× spike during the spike window;
            // every other peer stays flat at the baseline the whole time.
            let rtt = if peer_idx == 0 && (spike_start..spike_end).contains(&t) {
                spike_ms
            } else {
                baseline_ms
            };
            events.push(Event::RttSample {
                peer: peer.clone(),
                at: Duration::from_secs(t),
                rtt: Duration::from_millis(rtt),
            });
        }
    }

    Scenario {
        name: "single_packet_loss",
        description: "4 peers at a flat 30 ms baseline; one peer takes a transient 10× \
             (300 ms) excursion for ~10 s, then returns. A cross-peer-median controller \
             rejects the single-connection outlier and MUST NOT fire; a per-connection \
             controller (LEDBAT++) cuts hard and recovers slowly — the death spiral.",
        expectation: Expectation::NeverFires,
        events,
        run_for: Duration::from_secs(240),
    }
}
