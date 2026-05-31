//! `correlated_inflation` — every peer's RTT rises together.
//!
//! Models the "user opened YouTube" case — the local uplink is
//! contended, so every overlay path inflates at once. This is exactly
//! the case the RFC controller is designed to detect. A sane
//! controller MUST fire.
//!
//! Note the inherent detection lag for RFC-style controllers: the
//! 10 s recent-median window plus the 5 s sustained-trigger check
//! mean the controller cannot see anything above the threshold until
//! ~15 s after the burst starts. A scenario `run_for` shorter than
//! that would falsely show "controller didn't fire". This scenario
//! uses a 60 s baseline + 60 s burst so the detection window has
//! plenty of room.

use std::time::Duration;

use crate::event::Event;

use super::{Expectation, Scenario};

pub fn scenario() -> Scenario {
    let mut events = Vec::new();
    let n_peers = 5;

    // Establish a low baseline: 20 ms across all peers for the first 60
    // seconds, one sample/s per peer. This gives the rolling stats a
    // clean baseline to inflate against.
    for peer_idx in 0..n_peers {
        let peer = format!("peer-{peer_idx}");
        for t in 0..60 {
            events.push(Event::RttSample {
                peer: peer.clone(),
                at: Duration::from_secs(t),
                rtt: Duration::from_millis(20),
            });
        }
    }

    // Contention burst: t=60s through t=120s, every peer's RTT jumps to
    // 200 ms (well above the 30 ms inflation trigger, well above the
    // noise floor, sustained way past the 5 s sustain window).
    for peer_idx in 0..n_peers {
        let peer = format!("peer-{peer_idx}");
        for t in 60..120 {
            events.push(Event::RttSample {
                peer: peer.clone(),
                at: Duration::from_secs(t),
                rtt: Duration::from_millis(200),
            });
        }
    }

    Scenario {
        name: "correlated_inflation",
        description: "5 peers, clean 20 ms baseline for 60 s, then every peer inflates \
             to 200 ms together for another 60 s. The case the RFC controller \
             exists to detect; sane controllers MUST fire.",
        expectation: Expectation::FiresAtLeastOnce,
        events,
        run_for: Duration::from_secs(120),
    }
}
