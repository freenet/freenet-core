//! `reference_tracks_overlay` — overlay and reference inflate together.
//!
//! The companion to `reference_diverges_from_overlay`, with an identical
//! overlay signal but the *opposite* correct action. Here the parallel
//! reference path to a fixed external target inflates in lockstep with the
//! overlay: when even a contention-free external path slows down at the same
//! time as every overlay path, the shared cause is the *local uplink* being
//! contended (the user started a big upload). That is exactly the case the
//! controller exists to catch, so a sane controller MUST fire.
//!
//! Run against `reference_diverges_from_overlay`, this is the pair that shows
//! the reference signal is load-bearing: same overlay inflation, but a
//! reference-aware controller should fire here and hold there. RfcDraft fires
//! on both (it ignores the reference), so it gets this one right by luck and
//! the diverging one wrong — see that scenario's note.

use std::time::Duration;

use crate::event::Event;

use super::{Expectation, Scenario};

pub fn scenario() -> Scenario {
    let mut events = Vec::new();
    let n_peers = 5;

    // Overlay: clean 20 ms baseline for 60 s, then inflate to 200 ms together.
    for peer_idx in 0..n_peers {
        let peer = format!("peer-{peer_idx}");
        for t in 0..120u64 {
            let rtt = if t < 60 { 20 } else { 200 };
            events.push(Event::RttSample {
                peer: peer.clone(),
                at: Duration::from_secs(t),
                rtt: Duration::from_millis(rtt),
            });
        }
    }

    // Reference path: tracks the overlay — flat 20 ms, then inflates to 200 ms
    // at the same moment. A contention-free external path slowing down too
    // means the shared bottleneck is the local uplink.
    for t in 0..120u64 {
        let rtt = if t < 60 { 20 } else { 200 };
        events.push(Event::ReferenceSample {
            at: Duration::from_secs(t),
            rtt: Duration::from_millis(rtt),
        });
    }

    Scenario {
        name: "reference_tracks_overlay",
        description: "Overlay inflates 20→200 ms across all peers AND the reference path \
             inflates in lockstep. A contention-free external path slowing at the same \
             time points at the local uplink being contended — the case the controller \
             exists to catch — so a sane controller MUST fire.",
        expectation: Expectation::FiresAtLeastOnce,
        events,
        run_for: Duration::from_secs(120),
    }
}
