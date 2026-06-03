//! `slow_routing_drift` — the baseline genuinely drifts upward over 30 min.
//!
//! Pins the "min over 5 min" baseline tracking a *real* path change rather than
//! mistaking it for contention. Every peer's RTT rises by 5 ms/min for 30
//! minutes (a legitimate routing change, not local uplink contention). Because
//! the baseline window is 5 minutes, `baseline_min` always sits ~5 min behind
//! the current median, so the inflation the controller sees plateaus at
//! ~5 ms/min × 5 min = ~25 ms — just under the 30 ms trigger.
//!
//! A sane controller MUST NOT fire: slow, sustained drift shared by all peers
//! is the path moving, and the 5-minute baseline is supposed to follow it. If
//! a future controller shortens the baseline window enough that the drift
//! outruns it, this scenario flips to firing and the regression is visible.

use std::time::Duration;

use crate::event::Event;

use super::{Expectation, Scenario};

pub fn scenario() -> Scenario {
    let mut events = Vec::new();
    let n_peers = 5;
    let base_ms = 80.0;
    let drift_ms_per_min = 5.0;
    let duration_secs = 30 * 60; // 30 minutes

    for peer_idx in 0..n_peers {
        let peer = format!("peer-{peer_idx}");
        for t in 0..duration_secs {
            let rtt = base_ms + drift_ms_per_min * (t as f64 / 60.0);
            events.push(Event::RttSample {
                peer: peer.clone(),
                at: Duration::from_secs(t),
                rtt: Duration::from_millis(rtt.round() as u64),
            });
        }
    }

    Scenario {
        name: "slow_routing_drift",
        description: "5 peers whose baseline RTT drifts up 5 ms/min for 30 min (a real \
             path change, not contention). The 5-minute baseline_min trails the median, \
             so observed inflation plateaus near 25 ms — below the 30 ms trigger. A sane \
             controller MUST NOT fire on legitimate slow drift.",
        expectation: Expectation::NeverFires,
        events,
        run_for: Duration::from_secs(duration_secs),
    }
}
