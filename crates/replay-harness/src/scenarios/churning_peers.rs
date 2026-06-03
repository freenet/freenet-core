//! `churning_peers` — peers join and leave every 30 s; RTTs stay healthy.
//!
//! Pins the requirement that connection churn alone produces no rate action.
//! A stable core of peers holds a flat, healthy RTT throughout; on top of that,
//! a rotating transient peer joins, sends healthy samples for 30 s, then leaves,
//! repeatedly. Membership turns over constantly but nothing about the RTT
//! signal says "contention".
//!
//! A sane controller MUST NOT fire: the join/leave bookkeeping (and the
//! cold-start of each freshly-joined peer) must not be mistaken for a
//! congestion signal. This guards against a controller that, e.g., treats a
//! newly-joined peer's sparse baseline as inflation.

use std::time::Duration;

use crate::event::Event;

use super::{Expectation, Scenario};

pub fn scenario() -> Scenario {
    let mut events = Vec::new();
    let healthy_ms = 40;
    let duration_secs = 300u64;
    let churn_period = 30u64;

    // Stable core: three peers present the whole time at a flat healthy RTT.
    for peer_idx in 0..3 {
        let peer = format!("core-{peer_idx}");
        for t in 0..duration_secs {
            events.push(Event::RttSample {
                peer: peer.clone(),
                at: Duration::from_secs(t),
                rtt: Duration::from_millis(healthy_ms),
            });
        }
    }

    // Rotating transient peer: a new one joins each 30 s window, sends healthy
    // samples while present, then leaves. Explicit join/leave events make the
    // churn visible in the trace; we stop sampling a peer once it leaves so it
    // is not silently resurrected (see `replayer::apply_event` docs).
    let mut window = 0u64;
    while window * churn_period < duration_secs {
        let start = window * churn_period;
        let end = (start + churn_period).min(duration_secs);
        let peer = format!("transient-{window}");
        events.push(Event::PeerJoin {
            peer: peer.clone(),
            at: Duration::from_secs(start),
        });
        for t in start..end {
            events.push(Event::RttSample {
                peer: peer.clone(),
                at: Duration::from_secs(t),
                rtt: Duration::from_millis(healthy_ms),
            });
        }
        events.push(Event::PeerLeave {
            peer: peer.clone(),
            at: Duration::from_secs(end),
        });
        window += 1;
    }

    Scenario {
        name: "churning_peers",
        description: "Three stable core peers at a flat 40 ms RTT, plus a transient peer \
             that joins/leaves every 30 s (with explicit join/leave events) while sending \
             healthy samples. Membership churns constantly but the RTT signal is clean; a \
             sane controller MUST NOT fire on churn alone.",
        expectation: Expectation::NeverFires,
        events,
        run_for: Duration::from_secs(duration_secs),
    }
}
