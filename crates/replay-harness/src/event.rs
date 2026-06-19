//! Events the [`Replayer`](crate::Replayer) consumes.

use std::time::Duration;

/// Stable per-peer identifier as it appears in OTLP `peer_id` attributes
/// (and as we make up for synthetic scenarios). Comparisons are
/// case-sensitive and exact.
pub type PeerKey = String;

#[derive(Debug, Clone)]
pub enum Event {
    /// An overlay RTT sample observed at `at` from `peer`.
    RttSample {
        peer: PeerKey,
        at: Duration,
        rtt: Duration,
    },
    /// A reference-path RTT sample (the `shadow_reference_ping` signal).
    /// Reference-ping events have no source peer in production — they are
    /// emitted per node tagged with the local peer id. For replay, the
    /// stream that produces them is responsible for routing per-node
    /// streams correctly; the harness treats reference samples as
    /// node-local, not per-overlay-peer.
    ReferenceSample { at: Duration, rtt: Duration },
    /// A new peer joined (registry insert). Used by churn scenarios.
    PeerJoin { peer: PeerKey, at: Duration },
    /// A peer left (registry remove). Used by churn scenarios.
    PeerLeave { peer: PeerKey, at: Duration },
}

impl Event {
    /// The wall-clock time at which this event happened.
    pub fn at(&self) -> Duration {
        match self {
            Event::RttSample { at, .. }
            | Event::ReferenceSample { at, .. }
            | Event::PeerJoin { at, .. }
            | Event::PeerLeave { at, .. } => *at,
        }
    }
}

/// Source of events the replayer pulls from. Iterator-shaped on purpose so
/// large OTLP files can be streamed rather than loaded into memory.
///
/// Implementations MUST yield events in non-decreasing `at` order.
pub trait EventStream {
    fn next_event(&mut self) -> Option<Event>;
}

impl<I> EventStream for I
where
    I: Iterator<Item = Event>,
{
    fn next_event(&mut self) -> Option<Event> {
        self.next()
    }
}
