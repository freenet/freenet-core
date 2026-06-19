//! `small_n` — N=2 peers (below the rolling-stats N≥3 trustworthy threshold).
//!
//! The `rolling_rtt_stats.rs` rustdoc warns: at N≤2 the upper-middle
//! median collapses to "the worse of the two". The Phase 1 measurement
//! showed ~26% of production samples are in this regime, so any
//! controller that doesn't explicitly guard against it would fire
//! constantly on routine connection churn. Sane controllers MUST NOT
//! fire on this scenario.

use std::time::Duration;

use crate::event::Event;

use super::{Expectation, Scenario};

pub fn scenario() -> Scenario {
    let mut events = Vec::new();

    // 2 peers: clean 20 ms baseline for the first 30 s, then peer-0
    // inflates to 200 ms while peer-1 stays low. With N=2 and one peer
    // bad, the upper-middle median picks the bad peer — but the N<3
    // guard must prevent any action.
    for peer_idx in 0..2 {
        let peer = format!("peer-{peer_idx}");
        for t in 0..30 {
            events.push(Event::RttSample {
                peer: peer.clone(),
                at: Duration::from_secs(t),
                rtt: Duration::from_millis(20),
            });
        }
    }
    for t in 30..120 {
        events.push(Event::RttSample {
            peer: "peer-0".to_string(),
            at: Duration::from_secs(t),
            rtt: Duration::from_millis(200),
        });
        events.push(Event::RttSample {
            peer: "peer-1".to_string(),
            at: Duration::from_secs(t),
            rtt: Duration::from_millis(22),
        });
    }

    Scenario {
        name: "small_n",
        description: "Only 2 peers — below the N≥3 trustworthy threshold for the \
             cross-peer median. Even with one peer wildly inflated, sane \
             controllers MUST NOT fire (per the rolling-stats module's \
             own caveat).",
        expectation: Expectation::NeverFires,
        events,
        run_for: Duration::from_secs(120),
    }
}
