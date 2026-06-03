//! `cold_start` — brand-new peers on a slow path, no baseline history.
//!
//! Pins the cold-start requirement: a peer that appears with no baseline must
//! not be mistaken for contention, and the controller must not panic on the
//! partial state. Three peers come online at t=0 on a genuinely slow path
//! (a flat 200 ms RTT from their very first sample). Because each peer's
//! `baseline_min` is established from those same high samples, the *inflation*
//! (recent_median − baseline_min) is ~0 throughout: high absolute RTT is not
//! the same as RTT inflation.
//!
//! A sane controller MUST NOT fire: there is no baseline to inflate against, so
//! there is no contention signal — only a slow link. This guards against a
//! controller that confuses "high RTT" with "rising RTT", and against any
//! panic on snapshots taken before a full baseline window exists.

use std::time::Duration;

use crate::event::Event;

use super::{Expectation, Scenario};

pub fn scenario() -> Scenario {
    let mut events = Vec::new();
    let n_peers = 3;
    let slow_path_ms = 200; // high absolute RTT, but flat — no inflation

    for peer_idx in 0..n_peers {
        let peer = format!("cold-{peer_idx}");
        // Peers appear at t=0 with no prior history and immediately report a
        // high, flat RTT. Run only 90 s — well short of the 5-minute baseline
        // window — so the snapshot is always taken on a partially-filled
        // baseline, which is exactly the cold-start state we want to exercise.
        for t in 0..90u64 {
            events.push(Event::RttSample {
                peer: peer.clone(),
                at: Duration::from_secs(t),
                rtt: Duration::from_millis(slow_path_ms),
            });
        }
    }

    Scenario {
        name: "cold_start",
        description: "3 peers appear at t=0 on a slow path (flat 200 ms) with no baseline \
             history; the run is shorter than the baseline window so every snapshot is \
             cold. Inflation stays ~0 because high RTT is not rising RTT — a sane \
             controller MUST hold (and must not panic on the partial state).",
        expectation: Expectation::NeverFires,
        events,
        run_for: Duration::from_secs(90),
    }
}
