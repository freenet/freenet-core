//! `idle_steady_state` — 5 peers, each anchored by a fast t=0 sample,
//! then steady ~80 ms RTT for the rest of the window.
//!
//! Models the production noise-floor problem the Phase 1 analysis
//! exposed: in the wild we measured ~55 ms p50 / ~91 ms p90 inflation
//! across healthy peers, all sitting comfortably above the `RfcDraft`
//! 30 ms threshold all the time.
//!
//! The synthetic construction here uses a single t=0 40 ms sample per
//! peer to anchor `baseline_min`, then steady 80 ms for the rest of
//! the run, producing ~40 ms persistent inflation. That's an
//! intentionally simplified driver — real production sees a mix of
//! transient fast samples scattered throughout, but the *signal the
//! controller sees* (a sustained inflation well above the threshold)
//! is the same. A sane controller MUST NOT interpret this as
//! contention.

use std::time::Duration;

use crate::event::Event;

use super::{Expectation, Scenario};

pub fn scenario() -> Scenario {
    let mut events = Vec::new();

    // Establish a clean baseline window: each peer's first sample is at
    // t=0 with a low 40 ms RTT, then it drifts up to the steady state
    // 80 ms by t=10s and stays there. Without the early-fast baseline,
    // every sample would be the baseline, and `inflation` would be 0
    // forever — not what we measure in production.
    let n_peers = 5;
    for peer_idx in 0..n_peers {
        let peer = format!("peer-{peer_idx}");
        events.push(Event::RttSample {
            peer: peer.clone(),
            at: Duration::from_secs(0),
            rtt: Duration::from_millis(40),
        });
        // Steady-state 80 ms from t=10s through t=300s, one sample/s
        // per peer. ~1500 events total.
        for t in 10..300 {
            events.push(Event::RttSample {
                peer: peer.clone(),
                at: Duration::from_secs(t),
                rtt: Duration::from_millis(80),
            });
        }
    }

    Scenario {
        name: "idle_steady_state",
        description: "5 peers, steady ~40 ms baseline, ~80 ms recent (~40 ms inflation \
             from path queueing). Matches the ambient overlay noise we measure \
             in production — controller MUST NOT interpret it as contention.",
        expectation: Expectation::NeverFires,
        events,
        run_for: Duration::from_secs(300),
    }
}
