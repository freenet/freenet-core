//! `single_peer_outlier` — one peer's RTT spikes to 500 ms, others stable.
//!
//! Models a single bad path (intermediate peer queueing, routing event on
//! one route) while the local uplink is fine. The RFC explicitly chose
//! median across peers to be robust against this case. A sane controller
//! MUST NOT fire — the cross-peer median should stay close to baseline
//! even with one wild outlier.

use std::time::Duration;

use crate::event::Event;

use super::{Expectation, Scenario};

pub fn scenario() -> Scenario {
    let mut events = Vec::new();
    let n_peers = 5;

    // Clean baseline for first 60 s: 20 ms across all peers.
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

    // From t=60s onward: peer-0 spikes to 500 ms; the rest stay at
    // baseline (still ~20 ms, with a small wobble). Run for another 60 s.
    for t in 60..120 {
        events.push(Event::RttSample {
            peer: "peer-0".to_string(),
            at: Duration::from_secs(t),
            rtt: Duration::from_millis(500),
        });
        for peer_idx in 1..n_peers {
            events.push(Event::RttSample {
                peer: format!("peer-{peer_idx}"),
                at: Duration::from_secs(t),
                // Small jitter around baseline so recent_median is not
                // identically equal to baseline_min.
                rtt: Duration::from_millis(if t.is_multiple_of(2) { 22 } else { 24 }),
            });
        }
    }

    Scenario {
        name: "single_peer_outlier",
        description: "5 peers, peer-0 spikes to 500 ms while the other 4 stay at \
             baseline. Cross-peer median must reject this; sane controllers \
             MUST NOT fire.",
        expectation: Expectation::NeverFires,
        events,
        run_for: Duration::from_secs(120),
    }
}
