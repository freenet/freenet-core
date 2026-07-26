//! Where a node's outbound bytes actually go, split by message kind.
//!
//! ## Why
//!
//! Every bandwidth fix through 0.2.109 was aimed by
//! [`broadcast_payload_mix`][bpm], which measures ONE thing: the payload the
//! update fan-out chose. A 2026-07-26 measurement (#4956) paired that rollup
//! against `resource_utilization`'s `cumulative_bytes_sent` — the process's
//! real outbound total — across 544 peers on 0.2.108 and found:
//!
//! | | |
//! |---|---|
//! | real bytes sent | median 299 MB/h, mean 707 MB/h per node |
//! | broadcast payload | median 61 MB/h, mean 189 MB/h per node |
//! | payload as share of real | **median 14.7 %, aggregate 26.7 %** |
//!
//! So roughly **three quarters of what a node sends was invisible** to the
//! instrumentation being used to aim the fixes. The candidates for that
//! remainder — GET/PUT/SUBSCRIBE payloads, the ~5-min InterestSync
//! `Summaries` exchange (which ships a full `StateSummary` per shared
//! contract to every connected peer), CONNECT/NAT traffic, and transport
//! framing plus retransmits — differ by orders of magnitude in cost and have
//! completely different remedies. Guessing between them is how the previous
//! round mis-prioritised.
//!
//! This rollup answers it by construction rather than by inference: it counts
//! bytes by [`OutboundKind`] at the one place every non-stream `NetMessage`
//! is written to a connection, so the arms SUM to the node's message traffic
//! instead of sampling a slice of it. Comparing that sum against
//! `cumulative_bytes_sent` over the same window then attributes the residual
//! to transport overhead (headers, ACKs, retransmits) — a number nothing
//! currently reports.
//!
//! ## What is counted
//!
//! Bytes are the **serialized `NetMessage` length**, recorded after
//! [`PeerConnection::send`][send] has serialized it and handed it to the
//! transport. That is the payload the transport was asked to move, NOT the
//! on-wire total: it excludes per-packet framing, ACKs and retransmits. The
//! gap between this sum and `cumulative_bytes_sent` is exactly that overhead,
//! which is the point — it is reported as a residual rather than silently
//! folded into a message arm.
//!
//! Operations-level STREAM bytes are deliberately NOT counted here. They do
//! not pass through this call site (they go out via `send_stream` /
//! `outbound_stream`), and they already have their own accounting through
//! `transfer_completed`. Double-counting them would break the "arms sum to
//! message traffic" property this module exists to provide.
//!
//! [bpm]: super::broadcast_payload_mix
//! [send]: crate::transport::peer_connection::PeerConnection::send

use std::time::Duration;

use parking_lot::Mutex;

use crate::message::{NetMessage, NetMessageV1};
use crate::node::background_task_monitor::BackgroundTaskMonitor;

/// Rollup cadence, matching [`super::broadcast_payload_mix`] so the two
/// rollups can be joined per node-minute without interpolation.
const ROLLUP_WINDOW: Duration = Duration::from_secs(60);

/// Which kind of message put these bytes on the wire.
///
/// Deliberately coarse — one arm per protocol family, not per message
/// variant. The question this answers is "which SUBSYSTEM is spending the
/// bandwidth", and a finer split would multiply the arms without changing
/// which remedy applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutboundKind {
    /// CONNECT: joining, NAT traversal, topology maintenance.
    Connect,
    /// PUT, including the summary-first probe/reconcile legs.
    Put,
    /// GET, including responses that serve state to a requester.
    Get,
    /// SUBSCRIBE / UNSUBSCRIBE.
    Subscribe,
    /// UPDATE, which carries the broadcast fan-out measured in detail by
    /// [`super::broadcast_payload_mix`]. Present here so the two rollups can
    /// be cross-checked against each other.
    Update,
    /// InterestSync: the ~5-min `Interests` / `Summaries` anti-entropy
    /// heartbeat. The leading suspect for the unexplained remainder, because
    /// `SummaryEntry::summary_bytes` ships a FULL `StateSummary` per shared
    /// contract to every connected peer on every cycle.
    InterestSync,
    /// NeighborHosting advertisements.
    NeighborHosting,
    /// Small control messages with no subsystem of their own: `Aborted`,
    /// `ReadyState`, `SubscribeHint`. [`OutboundKind::classify`] matches
    /// these EXPLICITLY rather than via a catch-all, so a newly added
    /// protocol message fails to compile instead of quietly landing here and
    /// hiding its bytes in a bucket nobody investigates.
    Other,
}

impl OutboundKind {
    pub(crate) const ALL: [OutboundKind; 8] = [
        OutboundKind::Connect,
        OutboundKind::Put,
        OutboundKind::Get,
        OutboundKind::Subscribe,
        OutboundKind::Update,
        OutboundKind::InterestSync,
        OutboundKind::NeighborHosting,
        OutboundKind::Other,
    ];

    const fn index(self) -> usize {
        match self {
            OutboundKind::Connect => 0,
            OutboundKind::Put => 1,
            OutboundKind::Get => 2,
            OutboundKind::Subscribe => 3,
            OutboundKind::Update => 4,
            OutboundKind::InterestSync => 5,
            OutboundKind::NeighborHosting => 6,
            OutboundKind::Other => 7,
        }
    }

    /// Telemetry field stem. The emitted rollup publishes `<stem>_msgs` and
    /// `<stem>_bytes`, matching the `_sends`/`_bytes` convention the payload
    /// mix already uses.
    const fn stem(self) -> &'static str {
        match self {
            OutboundKind::Connect => "connect",
            OutboundKind::Put => "put",
            OutboundKind::Get => "get",
            OutboundKind::Subscribe => "subscribe",
            OutboundKind::Update => "update",
            OutboundKind::InterestSync => "interest_sync",
            OutboundKind::NeighborHosting => "neighbor_hosting",
            OutboundKind::Other => "other",
        }
    }

    /// Classify a message without inspecting its contents.
    pub(crate) fn classify(msg: &NetMessage) -> Self {
        match msg {
            NetMessage::V1(v1) => match v1 {
                NetMessageV1::Connect(_) => OutboundKind::Connect,
                NetMessageV1::Put(_) => OutboundKind::Put,
                NetMessageV1::Get(_) => OutboundKind::Get,
                NetMessageV1::Subscribe(_) => OutboundKind::Subscribe,
                NetMessageV1::Update(_) => OutboundKind::Update,
                NetMessageV1::InterestSync { .. } => OutboundKind::InterestSync,
                NetMessageV1::NeighborHosting { .. } => OutboundKind::NeighborHosting,
                // Exhaustive on purpose (no `_` arm): a new protocol message
                // must not silently join `Other` and hide its bytes inside a
                // bucket nobody investigates. Adding a variant should break
                // this match and force a deliberate choice.
                NetMessageV1::Aborted(_)
                | NetMessageV1::ReadyState { .. }
                | NetMessageV1::SubscribeHint { .. } => OutboundKind::Other,
            },
        }
    }
}

#[derive(Default)]
struct Window {
    msgs: [u64; 8],
    bytes: [u64; 8],
    /// Largest single serialized message in the window, per arm. A big mean
    /// and a big max mean different things (steady load vs. one whale), and
    /// the InterestSync question specifically hinges on which it is.
    max_bytes: [u64; 8],
}

/// Per-message-kind outbound byte accumulator.
///
/// One `parking_lot::Mutex` covering the whole window, for the same reason
/// [`super::broadcast_payload_mix::PayloadMix`] uses one: a rollup must be a
/// consistent snapshot, so record and drain have to be atomic with respect to
/// each other. Per-field atomics would let a drain land mid-update and report
/// arms that never coexisted.
///
/// Cost is one uncontended lock acquire plus three integer updates per
/// message sent. This IS a hotter path than the payload mix (every message,
/// not every delivered broadcast), so it is deliberately kept to integer work
/// with no allocation, no map insert, and no formatting — everything else
/// happens in the aggregator task.
pub(crate) struct OutboundMix {
    window: Mutex<Window>,
}

impl OutboundMix {
    pub(crate) fn new() -> Self {
        Self {
            window: Mutex::new(Window::default()),
        }
    }

    /// Record one serialized message handed to the transport.
    ///
    /// `bytes` is the serialized `NetMessage` length, not the on-wire size —
    /// see the module docs on what the residual against
    /// `cumulative_bytes_sent` means.
    pub(crate) fn record_sent(&self, kind: OutboundKind, bytes: usize) {
        let b = bytes as u64;
        let idx = kind.index();
        let mut w = self.window.lock();
        // Saturating throughout: a wrapped counter would report a tiny number
        // for the heaviest arm, the exact opposite of the measurement's point.
        w.msgs[idx] = w.msgs[idx].saturating_add(1);
        w.bytes[idx] = w.bytes[idx].saturating_add(b);
        w.max_bytes[idx] = w.max_bytes[idx].max(b);
    }

    /// Atomically take the current window, leaving a fresh empty one.
    fn take_window(&self) -> Window {
        std::mem::take(&mut *self.window.lock())
    }
}

/// Clamp a measured elapsed span to a sane `window_secs` for the rollup.
///
/// Mirrors the payload mix: a stalled runtime can stretch the real window
/// well past [`ROLLUP_WINDOW`], and reporting the nominal 60 s there would
/// silently inflate every derived per-second rate.
fn rollup_window_secs(elapsed: Duration) -> u64 {
    elapsed.as_secs().max(1)
}

fn emit_outbound_mix_rollup(mix: &OutboundMix, local_peer_id: &str, window_secs: u64) {
    let w = mix.take_window();
    let total_msgs: u64 = w.msgs.iter().sum();
    let total_bytes: u64 = w.bytes.iter().sum();
    if total_msgs == 0 {
        // Still emit: a silent node is a data point (it distinguishes "no
        // traffic" from "telemetry stopped"), and the payload mix emits when
        // idle too, so the two stay joinable per node-minute.
    }

    let mut body = serde_json::Map::new();
    body.insert("window_secs".into(), window_secs.into());
    body.insert("total_msgs".into(), total_msgs.into());
    body.insert("total_bytes".into(), total_bytes.into());
    for kind in OutboundKind::ALL {
        let idx = kind.index();
        let stem = kind.stem();
        body.insert(format!("{stem}_msgs"), w.msgs[idx].into());
        body.insert(format!("{stem}_bytes"), w.bytes[idx].into());
        body.insert(format!("{stem}_max_bytes"), w.max_bytes[idx].into());
    }

    // Shadow priority, matching the payload mix: one event per node-minute is
    // negligible volume, but it is observation rather than operational signal.
    crate::tracing::telemetry::send_standalone_shadow_event_with_peer_id(
        "outbound_message_mix",
        local_peer_id,
        serde_json::Value::Object(body),
    );
}

/// Spawn the per-minute rollup emitter.
///
/// Observation only — nothing reads these counters to make a decision.
pub(crate) fn spawn_outbound_mix_aggregator(
    mix: std::sync::Arc<OutboundMix>,
    local_peer_id: String,
    monitor: &BackgroundTaskMonitor,
) {
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(ROLLUP_WINDOW);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // skip the immediate first tick
        let mut last_rollup = tokio::time::Instant::now();
        loop {
            ticker.tick().await;
            let now = tokio::time::Instant::now();
            let elapsed = now.saturating_duration_since(last_rollup);
            last_rollup = now;
            emit_outbound_mix_rollup(&mix, &local_peer_id, rollup_window_secs(elapsed));
        }
    });
    monitor.register("outbound_message_mix_aggregator", handle);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Taking the window leaves the accumulator empty, so consecutive rollups
    /// report windows rather than lifetime totals.
    #[test]
    fn take_window_resets_the_window() {
        let mix = OutboundMix::new();
        mix.record_sent(OutboundKind::InterestSync, 500);
        let first = mix.take_window();
        assert_eq!(first.msgs[OutboundKind::InterestSync.index()], 1);
        assert_eq!(first.bytes[OutboundKind::InterestSync.index()], 500);
        let second = mix.take_window();
        assert!(
            second.msgs.iter().all(|m| *m == 0) && second.bytes.iter().all(|b| *b == 0),
            "second take must be empty"
        );
    }

    /// The arms must partition the traffic: every recorded byte lands in
    /// exactly one arm, so the sum is the node's message total. This is the
    /// property that makes the residual against `cumulative_bytes_sent`
    /// interpretable as transport overhead rather than as "some arm we
    /// forgot".
    #[test]
    fn arms_partition_recorded_bytes() {
        let mix = OutboundMix::new();
        mix.record_sent(OutboundKind::Get, 10);
        mix.record_sent(OutboundKind::Put, 20);
        mix.record_sent(OutboundKind::InterestSync, 30);
        mix.record_sent(OutboundKind::Get, 40);
        let w = mix.take_window();
        assert_eq!(w.bytes.iter().sum::<u64>(), 100);
        assert_eq!(w.msgs.iter().sum::<u64>(), 4);
        assert_eq!(w.bytes[OutboundKind::Get.index()], 50);
    }

    /// `max_bytes` tracks the largest single message, which is what
    /// distinguishes a steady stream from one whale.
    #[test]
    fn max_bytes_tracks_the_largest_single_message() {
        let mix = OutboundMix::new();
        mix.record_sent(OutboundKind::Update, 100);
        mix.record_sent(OutboundKind::Update, 900);
        mix.record_sent(OutboundKind::Update, 50);
        let w = mix.take_window();
        assert_eq!(w.max_bytes[OutboundKind::Update.index()], 900);
        assert_eq!(w.bytes[OutboundKind::Update.index()], 1050);
    }

    /// Every arm must own a distinct index and a distinct field stem, or two
    /// arms would silently share a counter / overwrite each other's JSON key.
    #[test]
    fn arms_have_unique_indices_and_stems() {
        let mut idxs: Vec<usize> = OutboundKind::ALL.iter().map(|k| k.index()).collect();
        idxs.sort_unstable();
        idxs.dedup();
        assert_eq!(idxs.len(), OutboundKind::ALL.len(), "duplicate arm index");
        assert_eq!(
            *idxs.last().expect("non-empty"),
            OutboundKind::ALL.len() - 1,
            "indices must be dense so the fixed-size arrays cover them"
        );

        let mut stems: Vec<&str> = OutboundKind::ALL.iter().map(|k| k.stem()).collect();
        stems.sort_unstable();
        stems.dedup();
        assert_eq!(stems.len(), OutboundKind::ALL.len(), "duplicate field stem");
    }

    /// A stalled runtime must not report the nominal 60 s for a longer real
    /// window, and a sub-second window must not report zero (which would make
    /// every derived rate a division by zero).
    #[test]
    fn rollup_window_secs_is_clamped_to_the_real_elapsed_span() {
        assert_eq!(rollup_window_secs(Duration::from_millis(10)), 1);
        assert_eq!(rollup_window_secs(Duration::from_secs(60)), 60);
        assert_eq!(rollup_window_secs(Duration::from_secs(300)), 300);
    }
}
