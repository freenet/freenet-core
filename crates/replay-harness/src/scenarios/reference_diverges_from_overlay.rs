//! `reference_diverges_from_overlay` — overlay inflates, reference stays flat.
//!
//! This is the scenario that motivates the Phase 1.5 reference-ping signal
//! (#4292). Every overlay path inflates together (looks exactly like
//! `correlated_inflation`), but the parallel reference path to a fixed external
//! target stays flat. That combination means the inflation is *overlay*
//! queueing (intermediate-hop buckets), NOT contention on the local uplink —
//! backing off our own send rate would not help, so a sane controller MUST NOT
//! fire.
//!
//! Telling this apart from real contention is impossible without the reference
//! signal, which is the whole reason it was added. So:
//! - The scenario's *sane-controller* expectation is `NeverFires`.
//! - [`RfcDraft`](crate::controllers::RfcDraft) is reference-blind — it only
//!   looks at cross-peer overlay inflation — so it fires here, **incorrectly**.
//!   That known limitation is pinned in `tests/scenarios_pin.rs` (exactly as
//!   the `idle_steady_state` noise-floor failure is pinned), and it is the
//!   regression a reference-aware Phase 2 controller must fix: same overlay
//!   input as `reference_tracks_overlay`, opposite correct action, and the
//!   reference signal is the only thing that distinguishes them.

use std::time::Duration;

use crate::event::Event;

use super::{Expectation, Scenario};

pub fn scenario() -> Scenario {
    let mut events = Vec::new();
    let n_peers = 5;

    // Overlay: clean 20 ms baseline for 60 s, then every peer inflates to
    // 200 ms together for 60 s — identical to `correlated_inflation`.
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

    // Reference path: flat 20 ms the entire time. The uplink is NOT contended;
    // the overlay inflation is intermediate-hop queueing.
    for t in 0..120u64 {
        events.push(Event::ReferenceSample {
            at: Duration::from_secs(t),
            rtt: Duration::from_millis(20),
        });
    }

    Scenario {
        name: "reference_diverges_from_overlay",
        description: "Overlay inflates 20→200 ms across all peers while the reference path \
             stays flat at 20 ms. Flat reference means overlay (intermediate-hop) queueing, \
             not local uplink contention, so a sane controller MUST NOT fire. A \
             reference-blind controller (RfcDraft) cannot tell this from real contention \
             and fires incorrectly.",
        expectation: Expectation::NeverFires,
        events,
        run_for: Duration::from_secs(120),
    }
}
