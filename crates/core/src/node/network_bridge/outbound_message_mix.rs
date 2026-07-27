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
//! ### Relationship to `transfer_completed` — do NOT sum the two
//!
//! There are two different kinds of "stream" here and conflating them
//! produces a double count:
//!
//! * **Operations-level streams** (`send_stream` / `StreamId::next_operations`,
//!   used for full-state broadcast and large op payloads) do NOT reach this
//!   call site and are NOT counted here. They are covered by
//!   `transfer_completed`.
//! * **Transport-level streams** are just a big `NetMessage`: anything over
//!   `MAX_DATA_SIZE` (~1.2 KB) that goes through [`PeerConnection::send`][send]
//!   is fragmented by `outbound_stream`. Those messages DO pass through this
//!   call site, so their bytes ARE counted here — deliberately, since that
//!   population includes the delta broadcasts. They ALSO emit
//!   `transfer_completed`.
//!
//! So this rollup and `transfer_completed` OVERLAP on transport-level streams.
//! Each is internally consistent; adding them together is not. Use this
//! rollup for "which subsystem spent the bytes" and `transfer_completed` for
//! per-transfer transport behaviour.
//!
//! The arms here still partition *message* traffic (every non-stream-op
//! `NetMessage` lands in exactly one arm), which is the property the residual
//! against `cumulative_bytes_sent` relies on.
//!
//! [bpm]: super::broadcast_payload_mix
//! [send]: crate::transport::peer_connection::PeerConnection::send

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use freenet_stdlib::prelude::ContractInstanceId;
use parking_lot::Mutex;

use crate::message::{InterestMessage, NetMessage, NetMessageV1};
use crate::node::background_task_monitor::BackgroundTaskMonitor;

/// Per-contract attribution cap for the differing-summary map in one window.
///
/// Mirrors [`super::broadcast_payload_mix`]'s cap and exists for the same
/// reason: the key is contract-controlled, so an unbounded map is an
/// amplification surface. 256 is far above the number of contracts a node
/// realistically diverges on in a minute, so hitting it is itself a signal.
const MAX_TRACKED_CONTRACTS: usize = 256;

/// How many differing contracts the emitted rollup names. Small on purpose:
/// the decision this feeds is "is it a handful of contracts or everything",
/// which the top few answer, and the aggregate counts above carry the rest.
const TOP_DIFFERING_CONTRACTS_REPORTED: usize = 10;

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
    /// InterestSync request leg: `Interests` and `ChangeInterests`, which
    /// carry only `u32` contract-id hashes (4 bytes per interest).
    ///
    /// Split from the reply leg below because the two have wildly different
    /// per-message costs and completely different remedies, and the combined
    /// arm could not tell them apart — the #4965 measurement hinges on which
    /// leg the 53-75% actually sits in.
    InterestSyncInterests,
    /// InterestSync reply leg: `Summaries`. The leading suspect, because
    /// `SummaryEntry::summary_bytes` ships a FULL `StateSummary` per shared
    /// contract to every connected peer on every cycle
    /// (`node.rs::handle_interest_sync_message`).
    InterestSyncSummaries,
    /// InterestSync heal leg: `ResyncRequest` / `ResyncResponse`. Separate
    /// because `ResyncResponse` carries full contract STATE, so folding it
    /// into the reply arm would attribute heal traffic to the heartbeat and
    /// overstate exactly the thing being measured.
    InterestSyncResync,
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
    pub(crate) const ALL: [OutboundKind; 10] = [
        OutboundKind::Connect,
        OutboundKind::Put,
        OutboundKind::Get,
        OutboundKind::Subscribe,
        OutboundKind::Update,
        OutboundKind::InterestSyncInterests,
        OutboundKind::InterestSyncSummaries,
        OutboundKind::InterestSyncResync,
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
            OutboundKind::InterestSyncInterests => 5,
            OutboundKind::InterestSyncSummaries => 6,
            OutboundKind::InterestSyncResync => 7,
            OutboundKind::NeighborHosting => 8,
            OutboundKind::Other => 9,
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
            OutboundKind::InterestSyncInterests => "interest_sync_interests",
            OutboundKind::InterestSyncSummaries => "interest_sync_summaries",
            OutboundKind::InterestSyncResync => "interest_sync_resync",
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
                NetMessageV1::InterestSync { message } => match message {
                    // Exhaustive on purpose, same rationale as the outer match:
                    // a new InterestMessage variant must force a deliberate
                    // choice rather than defaulting into whichever arm it
                    // happens to resemble.
                    InterestMessage::Interests { .. } | InterestMessage::ChangeInterests { .. } => {
                        OutboundKind::InterestSyncInterests
                    }
                    InterestMessage::Summaries { .. } => OutboundKind::InterestSyncSummaries,
                    InterestMessage::ResyncRequest { .. }
                    | InterestMessage::ResyncResponse { .. } => OutboundKind::InterestSyncResync,
                },
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
    msgs: [u64; 10],
    bytes: [u64; 10],
    /// Largest single serialized message in the window, per arm. A big mean
    /// and a big max mean different things (steady load vs. one whale), and
    /// the InterestSync question specifically hinges on which it is.
    max_bytes: [u64; 10],
    /// InterestSync summary comparisons where both sides held a summary and
    /// the bytes were IDENTICAL. See [`OutboundMix::record_summary_comparison`].
    summary_entries_identical: u64,
    /// Same, but the summary bytes DIFFERED.
    summary_entries_differing: u64,
    /// Which contracts the differing comparisons belonged to, bounded at
    /// [`MAX_TRACKED_CONTRACTS`].
    ///
    /// Load-bearing for reading a low identical rate: "the design is wrong"
    /// and "these three contracts serialize non-deterministically" produce the
    /// same aggregate ratio and have completely different fixes (#4857 /
    /// `contract-summary-determinism.md`). Only the DIFFERING side is
    /// attributed — the identical side needs no diagnosis, and tracking it
    /// would double the map for no decision.
    differing_by_contract: HashMap<ContractInstanceId, u64>,
    /// Differing comparisons that could NOT be attributed because
    /// [`Window::differing_by_contract`] was already at
    /// [`MAX_TRACKED_CONTRACTS`].
    ///
    /// Mirrors `broadcast_payload_mix`'s `attribution_dropped_*`, and exists
    /// for the same reason it does: without it a capped window is
    /// indistinguishable from "no further contracts diverged", which is the
    /// exact misreading this attribution was added to prevent. Non-zero also
    /// means the named list below is a partial view, so the reader should not
    /// treat it as the full set of offenders.
    differing_attribution_dropped: u64,
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

    /// Record one InterestSync summary comparison — the #4965 falsifier.
    ///
    /// Called from `node.rs::handle_interest_sync_message` at the point where
    /// a received `SummaryEntry` is byte-compared against our own summary for
    /// the same contract, and ONLY when both sides hold one (a `None` on
    /// either side is not a comparison and must not land in either bucket, or
    /// the ratio silently absorbs "peer has no state yet").
    ///
    /// The ratio decides whether the hash-first redesign is worth a wire
    /// change: `SummaryEntry` ships full summary bytes unconditionally, so
    /// exchanging digests first saves bytes exactly on the identical
    /// fraction. A high identical rate makes it a large win; a low one means
    /// digests would mismatch, ship the bytes anyway, and add a round trip —
    /// strictly worse than today.
    ///
    /// Lives on the outbound rollup despite being a RECEIVE-side observation,
    /// for two reasons: it belongs in the same node-minute record as the
    /// `interest_sync_summaries_bytes` it explains (joinable without
    /// interpolation), and the telemetry budget has room for exactly one more
    /// aligned rollup stream (`telemetry.rs`, `MAX_SHADOW_EVENTS_PER_SECOND`),
    /// which this measurement does not deserve to consume.
    pub(crate) fn record_summary_comparison(
        &self,
        contract: &ContractInstanceId,
        ours: &[u8],
        theirs: &[u8],
        counted_this_message: &mut HashSet<ContractInstanceId>,
    ) {
        // The per-message dedup lives HERE, not at the call site, for the same
        // reason the byte comparison does: `Summaries.entries` is peer-supplied
        // and may repeat a hash, so without it a peer can inflate either bucket
        // at will and skew the ratio that gates the wire-format redesign.
        // Taking the set makes bypassing it impossible rather than merely
        // discouraged — an earlier version guarded the CALL SITE with an `if`,
        // which mutation testing showed no source pin could protect.
        if !counted_this_message.insert(*contract) {
            return;
        }
        // The comparison lives HERE rather than at the call site on purpose.
        // Passing a pre-computed `identical: bool` put the one bit this whole
        // measurement rests on outside the tested unit, where an inverted
        // operand would compile, pass every test, and quietly invert the
        // finding that gates a wire-format redesign. Taking the operands makes
        // that failure unrepresentable instead of merely covered.
        let identical = ours == theirs;
        let mut w = self.window.lock();
        if identical {
            w.summary_entries_identical = w.summary_entries_identical.saturating_add(1);
            return;
        }
        w.summary_entries_differing = w.summary_entries_differing.saturating_add(1);
        let mut dropped = false;
        // Bounded for the same reason the payload mix bounds its attribution:
        // the key is contract-controlled, so an unbounded map here would be an
        // amplification surface. Over the cap the aggregate above keeps
        // counting and an already-tracked key keeps accruing; only NEW keys are
        // refused, and each refusal is counted.
        //
        // What this does NOT give you, stated plainly because the obvious
        // reading is wrong: once the cap binds, the named set is NOT the top
        // offenders. Slots are first-come-first-served, and arrival order is
        // not random — `get_matching_contracts` sorts by contract id ascending
        // (`ring/interest.rs`), so entries are processed in id order on every
        // peer and every window. Under sustained cap pressure the same
        // low-id contracts hold the slots and a higher-diverging high-id
        // contract stays invisible indefinitely. Read a non-zero
        // `differing_attribution_dropped` as "this list is a biased sample,
        // use the aggregate"; a fair top-N would need a replace-if-larger
        // policy, which is more machinery than this measurement warrants.
        let len = w.differing_by_contract.len();
        match w.differing_by_contract.entry(*contract) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                *e.get_mut() = e.get().saturating_add(1);
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                if len < MAX_TRACKED_CONTRACTS {
                    e.insert(1);
                } else {
                    dropped = true;
                }
            }
        }
        if dropped {
            w.differing_attribution_dropped = w.differing_attribution_dropped.saturating_add(1);
        }
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

/// Build the rollup body for one drained window.
///
/// Split out of [`emit_outbound_mix_rollup`] and returning the `Value` so the
/// shaping — field names, the descending sort, the top-N truncation — is
/// directly testable, the way `broadcast_payload_mix::payload_mix_json`
/// already is. Inlined in the emitter there was no seam: an inverted
/// comparator would have reported the LEAST-diverging contracts as "top" and
/// shipped silently.
fn outbound_mix_json(w: &Window, window_secs: u64) -> serde_json::Value {
    let total_msgs: u64 = w.msgs.iter().sum();
    let total_bytes: u64 = w.bytes.iter().sum();

    // Emitted unconditionally, including for an idle window: a silent node is
    // a data point (it distinguishes "no traffic" from "telemetry stopped"),
    // and the payload mix emits when idle too, so the two stay joinable per
    // node-minute.
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

    // #4965 falsifier. Emitted unconditionally (including as a pair of zeros)
    // so "no comparisons happened this window" is distinguishable from "the
    // field was dropped" — the same reason the arms above emit when idle.
    body.insert(
        "summary_entries_identical".into(),
        w.summary_entries_identical.into(),
    );
    body.insert(
        "summary_entries_differing".into(),
        w.summary_entries_differing.into(),
    );
    body.insert(
        "differing_attribution_dropped".into(),
        w.differing_attribution_dropped.into(),
    );
    // Top differing contracts by count, so a low identical rate can be read as
    // "specific contracts are non-deterministic" vs "the design is wrong".
    // Capped in the emitted body as well as in the map: the map bound stops
    // unbounded GROWTH, this bound stops an unbounded RECORD.
    let mut differing: Vec<(String, u64)> = w
        .differing_by_contract
        .iter()
        .map(|(k, v)| (k.to_string(), *v))
        .collect();
    differing.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    differing.truncate(TOP_DIFFERING_CONTRACTS_REPORTED);
    body.insert(
        "summary_differing_contracts".into(),
        serde_json::Value::Array(
            differing
                .into_iter()
                .map(|(key, count)| {
                    let mut o = serde_json::Map::new();
                    o.insert("contract".into(), key.into());
                    o.insert("count".into(), count.into());
                    serde_json::Value::Object(o)
                })
                .collect(),
        ),
    );

    serde_json::Value::Object(body)
}

fn emit_outbound_mix_rollup(mix: &OutboundMix, local_peer_id: &str, window_secs: u64) {
    let w = mix.take_window();
    // Shadow priority, matching the payload mix: one event per node-minute is
    // negligible volume, but it is observation rather than operational signal.
    crate::tracing::telemetry::send_standalone_shadow_event_with_peer_id(
        "outbound_message_mix",
        local_peer_id,
        outbound_mix_json(&w, window_secs),
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

    fn test_instance_id(seed: u32) -> ContractInstanceId {
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&seed.to_le_bytes());
        ContractInstanceId::new(bytes)
    }

    fn test_contract_key(seed: u32) -> freenet_stdlib::prelude::ContractKey {
        freenet_stdlib::prelude::ContractKey::from_id_and_code(
            test_instance_id(seed),
            freenet_stdlib::prelude::CodeHash::new([0u8; 32]),
        )
    }

    /// Taking the window leaves the accumulator empty, so consecutive rollups
    /// report windows rather than lifetime totals.
    #[test]
    fn take_window_resets_the_window() {
        let mix = OutboundMix::new();
        mix.record_sent(OutboundKind::InterestSyncSummaries, 500);
        let first = mix.take_window();
        assert_eq!(first.msgs[OutboundKind::InterestSyncSummaries.index()], 1);
        assert_eq!(
            first.bytes[OutboundKind::InterestSyncSummaries.index()],
            500
        );
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
        mix.record_sent(OutboundKind::InterestSyncSummaries, 30);
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

    /// The three InterestSync legs must classify apart (#4965).
    ///
    /// Before the split they shared one arm, so a rollup showing "interest_sync
    /// is 53-75% of outbound bytes" could not say whether the cost was the
    /// cheap hash advertisement, the full-summary reply, or full-state resync
    /// heals — three findings with three different remedies. Lumping them again
    /// would silently restore that ambiguity while the rollup kept reporting a
    /// plausible-looking number, so each leg is asserted individually.
    #[test]
    fn interest_sync_legs_classify_into_separate_arms() {
        let wrap = |m: InterestMessage| NetMessage::V1(NetMessageV1::InterestSync { message: m });

        let cases: [(InterestMessage, OutboundKind); 5] = [
            (
                InterestMessage::Interests { hashes: vec![1, 2] },
                OutboundKind::InterestSyncInterests,
            ),
            (
                InterestMessage::ChangeInterests {
                    added: vec![1],
                    removed: vec![],
                },
                OutboundKind::InterestSyncInterests,
            ),
            (
                InterestMessage::Summaries { entries: vec![] },
                OutboundKind::InterestSyncSummaries,
            ),
            (
                InterestMessage::ResyncRequest {
                    key: test_contract_key(7),
                },
                OutboundKind::InterestSyncResync,
            ),
            (
                InterestMessage::ResyncResponse {
                    key: test_contract_key(7),
                    state_bytes: vec![],
                    summary_bytes: vec![],
                },
                OutboundKind::InterestSyncResync,
            ),
        ];

        for (msg, expected) in cases {
            let label = format!("{msg:?}");
            assert_eq!(
                OutboundKind::classify(&wrap(msg)),
                expected,
                "wrong arm for {label}"
            );
        }
    }

    /// Identical and differing comparisons land in separate buckets — the
    /// #4965 falsifier itself. If both fell in one bucket (or the boolean were
    /// inverted) the ratio would still look like a plausible number, which is
    /// the failure mode worth a test: it would send the design decision the
    /// wrong way with no visible symptom.
    #[test]
    fn summary_comparisons_split_identical_from_differing() {
        let mix = OutboundMix::new();
        let a = test_instance_id(1);
        let b = test_instance_id(2);

        mix.record_summary_comparison(&a, b"same", b"same", &mut HashSet::new());
        mix.record_summary_comparison(&a, b"ours", b"theirs", &mut HashSet::new());
        mix.record_summary_comparison(&b, b"ours", b"theirs", &mut HashSet::new());
        mix.record_summary_comparison(&a, b"same", b"same", &mut HashSet::new());

        let w = mix.take_window();
        assert_eq!(w.summary_entries_identical, 2);
        assert_eq!(w.summary_entries_differing, 2);
        // Only the differing side is attributed per contract.
        assert_eq!(w.differing_by_contract.get(&a).copied(), Some(1));
        assert_eq!(w.differing_by_contract.get(&b).copied(), Some(1));
    }

    /// The per-contract map is bounded, and an ALREADY-TRACKED contract keeps
    /// accruing past the cap.
    ///
    /// Both halves matter. Without the bound the map is an amplification
    /// surface on a contract-controlled key. Without the keep-accruing half,
    /// the cap would silently freeze the counts of the very contracts worth
    /// naming as soon as a burst of one-off ids filled the map — the top-N
    /// report would then rank by "who arrived first", not by who diverges most.
    #[test]
    fn differing_attribution_is_bounded_but_keeps_accruing_known_contracts() {
        let mix = OutboundMix::new();
        let tracked = test_instance_id(0);
        mix.record_summary_comparison(&tracked, b"ours", b"theirs", &mut HashSet::new());

        // Fill well past the cap with distinct ids.
        for i in 1..(MAX_TRACKED_CONTRACTS as u32 + 300) {
            mix.record_summary_comparison(
                &test_instance_id(i),
                b"ours",
                b"theirs",
                &mut HashSet::new(),
            );
        }
        // ...then hit the already-tracked one again.
        mix.record_summary_comparison(&tracked, b"ours", b"theirs", &mut HashSet::new());

        let w = mix.take_window();
        assert!(
            w.differing_by_contract.len() <= MAX_TRACKED_CONTRACTS,
            "map must stay bounded, got {}",
            w.differing_by_contract.len()
        );
        assert_eq!(
            w.differing_by_contract.get(&tracked).copied(),
            Some(2),
            "an already-tracked contract must keep accruing past the cap"
        );
        // The aggregate never truncates, even though the map does.
        assert_eq!(
            w.summary_entries_differing,
            MAX_TRACKED_CONTRACTS as u64 + 301
        );
    }

    /// The emitted body ranks differing contracts by count, DESCENDING, and
    /// truncates to the top N.
    ///
    /// Worth its own test because an inverted comparator is invisible: it
    /// still emits a plausible, well-formed list of contracts — just the
    /// LEAST-diverging ones, labelled as the top offenders. Nothing downstream
    /// would flag that, and the wrong contracts would get investigated.
    #[test]
    fn rollup_body_ranks_differing_contracts_descending_and_truncates() {
        let mix = OutboundMix::new();
        // Contract i gets i differing comparisons, so the expected ranking is
        // exactly the reverse of insertion order.
        let n = TOP_DIFFERING_CONTRACTS_REPORTED as u32 + 5;
        for i in 1..=n {
            for _ in 0..i {
                mix.record_summary_comparison(
                    &test_instance_id(i),
                    b"ours",
                    b"theirs",
                    &mut HashSet::new(),
                );
            }
        }
        let w = mix.take_window();
        let body = outbound_mix_json(&w, 60);

        let listed = body
            .get("summary_differing_contracts")
            .and_then(|v| v.as_array())
            .expect("summary_differing_contracts must be an array");
        assert_eq!(
            listed.len(),
            TOP_DIFFERING_CONTRACTS_REPORTED,
            "the list must be truncated to the top N"
        );

        let counts: Vec<u64> = listed
            .iter()
            .map(|e| e.get("count").and_then(|c| c.as_u64()).expect("count"))
            .collect();
        let mut descending = counts.clone();
        descending.sort_unstable_by(|a, b| b.cmp(a));
        assert_eq!(counts, descending, "counts must be ranked descending");
        assert_eq!(
            counts[0], n as u64,
            "the highest-diverging contract must rank first"
        );
        assert_eq!(
            *counts.last().expect("non-empty"),
            (n - TOP_DIFFERING_CONTRACTS_REPORTED as u32 + 1) as u64,
            "the Nth-ranked count must be the Nth largest, not the smallest"
        );
    }

    /// The two headline counters survive the trip into the emitted body under
    /// the RIGHT keys.
    ///
    /// They were only ever checked on the `Window` struct. A swapped key in
    /// the body construction — emitting `summary_entries_differing` under the
    /// `summary_entries_identical` name — would ship an exactly inverted ratio
    /// to telemetry and send the hash-first decision the wrong way, with every
    /// other test still green. Distinguishable counts (3 vs 5) so a swap
    /// cannot pass.
    #[test]
    fn headline_counters_reach_the_rollup_body_under_the_right_keys() {
        let mix = OutboundMix::new();
        let c = test_instance_id(1);
        for _ in 0..3 {
            mix.record_summary_comparison(&c, b"same", b"same", &mut HashSet::new());
        }
        for _ in 0..5 {
            mix.record_summary_comparison(&c, b"ours", b"theirs", &mut HashSet::new());
        }
        let body = outbound_mix_json(&mix.take_window(), 60);
        assert_eq!(
            body.get("summary_entries_identical")
                .and_then(|v| v.as_u64()),
            Some(3),
            "identical count must reach the body under its own key"
        );
        assert_eq!(
            body.get("summary_entries_differing")
                .and_then(|v| v.as_u64()),
            Some(5),
            "differing count must reach the body under its own key"
        );
    }

    /// Within ONE message a contract is counted once, however many times the
    /// peer repeats it; across messages it counts again.
    ///
    /// `Summaries.entries` is peer-supplied, so without this a peer inflates
    /// either bucket at will and skews the exact ratio that decides whether
    /// the hash-first wire change gets built. The dedup lives inside this
    /// function rather than behind an `if` at the call site precisely so this
    /// property is testable — mutation testing showed no source pin could
    /// protect a call-site guard.
    #[test]
    fn a_contract_is_counted_once_per_message_however_often_repeated() {
        let mix = OutboundMix::new();
        let c = test_instance_id(1);
        let other = test_instance_id(2);

        // One message that repeats `c` five times and names `other` once.
        let mut first_message = HashSet::new();
        for _ in 0..5 {
            mix.record_summary_comparison(&c, b"ours", b"theirs", &mut first_message);
        }
        mix.record_summary_comparison(&other, b"same", b"same", &mut first_message);

        // A second message names `c` again — a genuinely new observation.
        let mut second_message = HashSet::new();
        mix.record_summary_comparison(&c, b"ours", b"theirs", &mut second_message);

        let w = mix.take_window();
        assert_eq!(
            w.summary_entries_differing, 2,
            "five repeats in one message count once; the second message counts again"
        );
        assert_eq!(w.summary_entries_identical, 1);
        assert_eq!(
            w.differing_by_contract.get(&c).copied(),
            Some(2),
            "per-contract attribution must dedup the same way as the aggregate"
        );
    }

    /// An untouched window emits the measurement fields as explicit zeros and
    /// an empty list, rather than omitting them.
    ///
    /// A missing field and a zero field look identical to a naive query but
    /// mean different things — "nothing diverged" vs "this build does not
    /// report it". The rollup emits when idle for exactly that reason.
    #[test]
    fn idle_window_emits_zeroed_measurement_fields() {
        let body = outbound_mix_json(&Window::default(), 60);
        assert_eq!(
            body.get("summary_entries_identical")
                .and_then(|v| v.as_u64()),
            Some(0)
        );
        assert_eq!(
            body.get("summary_entries_differing")
                .and_then(|v| v.as_u64()),
            Some(0)
        );
        assert_eq!(
            body.get("differing_attribution_dropped")
                .and_then(|v| v.as_u64()),
            Some(0)
        );
        assert_eq!(
            body.get("summary_differing_contracts")
                .and_then(|v| v.as_array())
                .map(|a| a.len()),
            Some(0),
            "an idle window must emit an empty list, not omit the field"
        );
    }

    /// A capped window reports HOW MANY attributions it dropped.
    ///
    /// Without this the named list is indistinguishable from the complete set,
    /// which is the exact misreading the attribution exists to prevent — and
    /// the sibling `broadcast_payload_mix` reports its drops for the same
    /// reason. Under cap pressure the list is a biased sample (slots are
    /// first-come, and entries arrive in contract-id order), so a reader needs
    /// this number to know not to trust the ranking.
    #[test]
    fn capped_attribution_reports_its_drops() {
        let mix = OutboundMix::new();
        let overflow = 7u32;
        for i in 0..(MAX_TRACKED_CONTRACTS as u32 + overflow) {
            mix.record_summary_comparison(
                &test_instance_id(i),
                b"ours",
                b"theirs",
                &mut HashSet::new(),
            );
        }
        let w = mix.take_window();
        assert_eq!(w.differing_by_contract.len(), MAX_TRACKED_CONTRACTS);
        assert_eq!(w.differing_attribution_dropped, overflow as u64);

        let body = outbound_mix_json(&w, 60);
        assert_eq!(
            body.get("differing_attribution_dropped")
                .and_then(|v| v.as_u64()),
            Some(overflow as u64),
            "the drop count must reach the rollup, not just the window"
        );
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
