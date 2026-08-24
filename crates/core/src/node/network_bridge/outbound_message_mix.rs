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

use crate::message::{InterestMessage, NetMessage, NetMessageV1, SummariesEmitter};
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
    /// InterestSync reply leg — the WHOLE summary exchange: `Summaries`, plus
    /// its hash-first replacements `SummaryDigests` and `SummaryRequest`
    /// (#4965).
    ///
    /// The leading suspect, because `SummaryEntry::summary_bytes` ships a FULL
    /// `StateSummary` per shared contract to every connected peer on every
    /// cycle (`node.rs::handle_interest_sync_message`). Measured at 49.8% of
    /// all outbound bytes on the fleet (v0.2.115, 1,174 peers). The hash-first
    /// exchange sends a digest first and the bytes only on mismatch; all three
    /// legs land here so this one field stays a like-for-like before/after
    /// total. See [`OutboundKind::classify`].
    ///
    /// That total is worth a second level of split, because this arm alone
    /// lumps unrelated emitters together — and hash-first re-encodes each of
    /// them rather than removing any, so the sub-split has to survive the
    /// change to stay readable. See [`SummariesEmitter`] and
    /// [`Window::summaries_bytes`].
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
}

/// Which emitter's bytes these are, WITHIN the `interest_sync_summaries` arm.
///
/// The arm is 49.8% of all outbound bytes and four unrelated emitters share
/// it (#5052). Because the emitters have opposite remedies — #5003 for the
/// per-state-change notification, hash-first (#4965) for the heartbeat reply —
/// a fix landing in either can be neither credited nor debugged against a
/// total that both drive.
///
/// Attribution rides on the message as a non-wire [`SummariesEmitter`] tag set
/// at construction, rather than as a `record_*` call added at each emitter.
/// That is the anti-rot shape: the tag is a MANDATORY field, so a fifth
/// emitter fails to compile until it names an arm, whereas a mirrored counter
/// silently stops being called the next time an op path is migrated (see the
/// manually-mirrored-counter row in `.claude/rules/bug-prevention-patterns.md`,
/// #4009 / #4010 / #3851).
///
/// `Default` is the UNATTRIBUTED detail — [`SummariesEmitter::Other`] with a
/// zero entry count — which is what the recorder falls back to for a
/// `Summaries` message that reached it without one. Not reachable from
/// today's call sites (the tag is mandatory), but the recorder counts such a
/// message in the residual rather than dropping it, so the sub-arms sum to the
/// parent by construction rather than by everyone remembering to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct SummariesDetail {
    emitter: SummariesEmitter,
    /// `entries.len()` — how many `SummaryEntry` this one message carried.
    ///
    /// The independent check on the attribution, and the reason it is worth
    /// recording: the notification and rejection emitters are single-entry by
    /// construction while both reply emitters are multi-entry, so a mean of
    /// entries-per-message far from 1 on a single-entry arm (or exactly 1 on a
    /// reply arm) means a call site is mislabelled. Byte totals alone cannot
    /// show that — they look plausible either way, which is precisely how the
    /// combined arm misled for as long as it did.
    entries: u64,
}

/// Telemetry field ordering for the Summaries sub-split.
///
/// Kept here rather than on [`SummariesEmitter`] itself: the enum is a
/// protocol-adjacent type, the index and the field stem are facts about this
/// rollup's wire-to-telemetry shape.
const SUMMARIES_ARMS: [SummariesEmitter; 7] = [
    SummariesEmitter::Notification,
    SummariesEmitter::InterestsReply,
    SummariesEmitter::ChangeInterestsReply,
    SummariesEmitter::Rejection,
    SummariesEmitter::DigestRequest,
    SummariesEmitter::DigestRequestReply,
    SummariesEmitter::Other,
];

/// Width of every per-sub-arm counter array, derived from the table above so a
/// new arm cannot land with the arrays still sized for the old count — that
/// would panic on the new index rather than fail to compile.
const SUMMARIES_ARM_COUNT: usize = SUMMARIES_ARMS.len();

const fn summaries_index(emitter: SummariesEmitter) -> usize {
    match emitter {
        SummariesEmitter::Notification => 0,
        SummariesEmitter::InterestsReply => 1,
        SummariesEmitter::ChangeInterestsReply => 2,
        SummariesEmitter::Rejection => 3,
        SummariesEmitter::DigestRequest => 4,
        SummariesEmitter::DigestRequestReply => 5,
        SummariesEmitter::Other => 6,
    }
}

/// Telemetry field stem, nested under the parent arm's stem so a query can
/// pattern-match `interest_sync_summaries_*` and get the split for free.
const fn summaries_stem(emitter: SummariesEmitter) -> &'static str {
    match emitter {
        SummariesEmitter::Notification => "interest_sync_summaries_notification",
        SummariesEmitter::InterestsReply => "interest_sync_summaries_interests_reply",
        SummariesEmitter::ChangeInterestsReply => "interest_sync_summaries_change_interests_reply",
        SummariesEmitter::Rejection => "interest_sync_summaries_rejection",
        SummariesEmitter::DigestRequest => "interest_sync_summaries_digest_request",
        SummariesEmitter::DigestRequestReply => "interest_sync_summaries_digest_request_reply",
        SummariesEmitter::Other => "interest_sync_summaries_other",
    }
}

/// One message's full classification: the arm, plus the Summaries sub-arm.
///
/// A single struct rather than two calls so the two levels are decided
/// together at one site. Splitting them would let the parent arm and the
/// sub-arm disagree about the same message, which is the one property the
/// reconciliation (sub-arms sum to parent) depends on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OutboundClass {
    pub(crate) kind: OutboundKind,
    /// `Some` exactly when `kind == OutboundKind::InterestSyncSummaries`.
    summaries: Option<SummariesDetail>,
}

impl OutboundClass {
    const fn plain(kind: OutboundKind) -> Self {
        Self {
            kind,
            summaries: None,
        }
    }

    /// Classify a message. Reads only the enum discriminants plus, for
    /// `Summaries`, the already-set emitter tag and `entries.len()` — no
    /// allocation, nothing that scales with payload size, so it stays cheap on
    /// the per-message send path.
    pub(crate) fn classify(msg: &NetMessage) -> Self {
        match msg {
            NetMessage::V1(v1) => match v1 {
                NetMessageV1::Connect(_) => Self::plain(OutboundKind::Connect),
                NetMessageV1::Put(_) => Self::plain(OutboundKind::Put),
                NetMessageV1::Get(_) => Self::plain(OutboundKind::Get),
                NetMessageV1::Subscribe(_) => Self::plain(OutboundKind::Subscribe),
                NetMessageV1::Update(_) => Self::plain(OutboundKind::Update),
                NetMessageV1::InterestSync { message } => match message {
                    // Exhaustive on purpose, same rationale as the outer match:
                    // a new InterestMessage variant must force a deliberate
                    // choice rather than defaulting into whichever arm it
                    // happens to resemble.
                    InterestMessage::Interests { .. } | InterestMessage::ChangeInterests { .. } => {
                        Self::plain(OutboundKind::InterestSyncInterests)
                    }
                    // All three legs of the summary exchange share ONE PARENT
                    // arm on purpose (#4965): the hash-first redesign replaces
                    // one `Summaries` with up to three messages
                    // (`SummaryDigests` → `SummaryRequest` → `Summaries`), so
                    // splitting the parent would make
                    // `interest_sync_summaries_bytes` collapse for a trivial
                    // reason and hide the extra legs in a bucket that did not
                    // exist before the change. Keeping them together makes the
                    // same field a like-for-like before/after total for the
                    // whole mechanism — which is exactly the falsifier: if
                    // hash-first does not shrink this number, it did not work.
                    //
                    // The #5052 SUB-arm is still resolved per message, so the
                    // parent stays like-for-like without the emitter split
                    // going blind the moment the gate opens.
                    InterestMessage::Summaries { entries, emitter } => Self {
                        kind: OutboundKind::InterestSyncSummaries,
                        summaries: Some(SummariesDetail {
                            emitter: *emitter,
                            entries: entries.len() as u64,
                        }),
                    },
                    // Carries the emitter of whatever built it, exactly as the
                    // `Summaries` it stands in for would have — that is the
                    // point of tagging this variant too.
                    InterestMessage::SummaryDigests { entries, emitter } => Self {
                        kind: OutboundKind::InterestSyncSummaries,
                        summaries: Some(SummariesDetail {
                            emitter: *emitter,
                            entries: entries.len() as u64,
                        }),
                    },
                    // Structural attribution: one emitter by construction (the
                    // digest-mismatch branch), so the variant IS the tag. Its
                    // `hashes` count is the entry count — same "how many
                    // contracts did this message speak for" quantity the other
                    // legs report, which keeps the per-arm entries check
                    // meaningful across the round trip.
                    InterestMessage::SummaryRequest { hashes } => Self {
                        kind: OutboundKind::InterestSyncSummaries,
                        summaries: Some(SummariesDetail {
                            emitter: SummariesEmitter::DigestRequest,
                            entries: hashes.len() as u64,
                        }),
                    },
                    InterestMessage::ResyncRequest { .. }
                    | InterestMessage::ResyncResponse { .. } => {
                        Self::plain(OutboundKind::InterestSyncResync)
                    }
                },
                NetMessageV1::NeighborHosting { .. } => Self::plain(OutboundKind::NeighborHosting),
                // Exhaustive on purpose (no `_` arm): a new protocol message
                // must not silently join `Other` and hide its bytes inside a
                // bucket nobody investigates. Adding a variant should break
                // this match and force a deliberate choice.
                NetMessageV1::Aborted(_)
                | NetMessageV1::ReadyState { .. }
                | NetMessageV1::SubscribeHint { .. } => Self::plain(OutboundKind::Other),
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
    /// The `interest_sync_summaries` arm split by emitter, indexed by
    /// [`summaries_index`] (#5052).
    ///
    /// These SUM to `bytes[InterestSyncSummaries]` / `msgs[...]` by
    /// construction: [`OutboundMix::record_sent`] updates a sub-arm in the
    /// same branch that updates the parent, and folds an unattributed message
    /// into [`SummariesEmitter::Other`] rather than skipping it. The
    /// reconciliation is therefore a property of the code path, not a
    /// convention every future call site has to honour — but it is asserted
    /// anyway (`summaries_sub_arms_reconcile_with_the_parent_arm`), since a
    /// silently non-reconciling split is worse than no split.
    summaries_msgs: [u64; SUMMARIES_ARM_COUNT],
    summaries_bytes: [u64; SUMMARIES_ARM_COUNT],
    summaries_max_bytes: [u64; SUMMARIES_ARM_COUNT],
    /// Total `SummaryEntry` count across the window's messages, per sub-arm.
    /// Divided by `summaries_msgs` this gives mean entries per message, the
    /// independent check that a call site is labelled correctly — see
    /// [`SummariesDetail::entries`].
    summaries_entries: [u64; SUMMARIES_ARM_COUNT],
    /// Largest single message's entry count, per sub-arm. Separates "every
    /// reply is moderately wide" from "one peer shares 400 contracts with us",
    /// which the mean cannot.
    summaries_max_entries: [u64; SUMMARIES_ARM_COUNT],
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
/// message sent, and eight for an InterestSync `Summaries` (the #5052
/// sub-split). This IS a hotter path than the payload mix (every message, not
/// every delivered broadcast), so it is deliberately kept to integer work with
/// no allocation, no map insert, and no formatting — everything else happens
/// in the aggregator task.
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
    pub(crate) fn record_sent(&self, class: OutboundClass, bytes: usize) {
        let b = bytes as u64;
        let idx = class.kind.index();
        let mut w = self.window.lock();
        // Saturating throughout: a wrapped counter would report a tiny number
        // for the heaviest arm, the exact opposite of the measurement's point.
        w.msgs[idx] = w.msgs[idx].saturating_add(1);
        w.bytes[idx] = w.bytes[idx].saturating_add(b);
        w.max_bytes[idx] = w.max_bytes[idx].max(b);

        // #5052 sub-split, in the SAME branch as the parent update so the two
        // levels cannot disagree about a message. Gated on the parent arm
        // rather than on `class.summaries.is_some()`: a Summaries message that
        // somehow arrived with no detail must still be counted, in the
        // residual, or the sub-arms would quietly stop summing to the parent —
        // the one failure this split cannot afford, since a shortfall would
        // read as "that emitter got smaller".
        if class.kind == OutboundKind::InterestSyncSummaries {
            let detail = class.summaries.unwrap_or_default();
            let s = summaries_index(detail.emitter);
            w.summaries_msgs[s] = w.summaries_msgs[s].saturating_add(1);
            w.summaries_bytes[s] = w.summaries_bytes[s].saturating_add(b);
            w.summaries_max_bytes[s] = w.summaries_max_bytes[s].max(b);
            w.summaries_entries[s] = w.summaries_entries[s].saturating_add(detail.entries);
            w.summaries_max_entries[s] = w.summaries_max_entries[s].max(detail.entries);
        }
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

    // #5052: the `interest_sync_summaries` arm again, split by emitter. Fields
    // on THIS rollup rather than a new event stream — deliberately: the
    // per-node budget is 10 events/s (`tracing/telemetry.rs`), 3.11% of records
    // already collide with it, and the collector runs at ~88.8 GB/day, so a
    // per-event stream is what got #4940 closed. Five extra integers per
    // node-minute costs nothing measurable.
    //
    // Emitted unconditionally including as zeros, same rule as the arms above:
    // "this emitter sent nothing" and "this build does not report it" must not
    // look the same, or the split would appear to attribute traffic that it
    // simply never counted.
    for emitter in SUMMARIES_ARMS {
        let s = summaries_index(emitter);
        let stem = summaries_stem(emitter);
        body.insert(format!("{stem}_msgs"), w.summaries_msgs[s].into());
        body.insert(format!("{stem}_bytes"), w.summaries_bytes[s].into());
        body.insert(format!("{stem}_max_bytes"), w.summaries_max_bytes[s].into());
        body.insert(format!("{stem}_entries"), w.summaries_entries[s].into());
        body.insert(
            format!("{stem}_max_entries"),
            w.summaries_max_entries[s].into(),
        );
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

    /// Record a non-Summaries message, for tests that only care about the
    /// parent arms.
    fn record(mix: &OutboundMix, kind: OutboundKind, bytes: usize) {
        mix.record_sent(OutboundClass::plain(kind), bytes);
    }

    /// A `Summaries` message as some emitter would actually build it: `n`
    /// entries carrying `per_entry` summary bytes each.
    fn summaries_msg(emitter: SummariesEmitter, n: usize, per_entry: usize) -> NetMessage {
        NetMessage::V1(NetMessageV1::InterestSync {
            message: InterestMessage::Summaries {
                entries: (0..n)
                    .map(|i| crate::message::SummaryEntry {
                        hash: i as u32,
                        summary_bytes: Some(vec![0u8; per_entry]),
                    })
                    .collect(),
                emitter,
            },
        })
    }

    /// Record a `Summaries` message the way the production path does: classify
    /// the real message at the choke point, then record what classify decided.
    /// Tests must NOT hand-build an `OutboundClass`, or they would assert the
    /// recorder against their own labelling instead of against `classify`'s.
    fn record_summaries(mix: &OutboundMix, emitter: SummariesEmitter, n: usize, bytes: usize) {
        mix.record_sent(
            OutboundClass::classify(&summaries_msg(emitter, n, 1)),
            bytes,
        );
    }

    /// Taking the window leaves the accumulator empty, so consecutive rollups
    /// report windows rather than lifetime totals.
    #[test]
    fn take_window_resets_the_window() {
        let mix = OutboundMix::new();
        record(&mix, OutboundKind::InterestSyncSummaries, 500);
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
        record(&mix, OutboundKind::Get, 10);
        record(&mix, OutboundKind::Put, 20);
        record(&mix, OutboundKind::InterestSyncSummaries, 30);
        record(&mix, OutboundKind::Get, 40);
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
        record(&mix, OutboundKind::Update, 100);
        record(&mix, OutboundKind::Update, 900);
        record(&mix, OutboundKind::Update, 50);
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
                InterestMessage::Summaries {
                    entries: vec![],
                    emitter: SummariesEmitter::InterestsReply,
                },
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
                OutboundClass::classify(&wrap(msg)).kind,
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

    // ---------------------------------------------------------------------
    // #5052 — Summaries sub-split by emitter
    // ---------------------------------------------------------------------

    /// The same sanity property the parent arms have, one level down: distinct
    /// index, distinct field stem, dense indices. Two sub-arms sharing a stem
    /// would silently overwrite each other's JSON key and the split would
    /// report one emitter's bytes under another's name.
    #[test]
    fn summaries_sub_arms_have_unique_indices_and_stems() {
        let mut idxs: Vec<usize> = SUMMARIES_ARMS
            .iter()
            .copied()
            .map(summaries_index)
            .collect();
        idxs.sort_unstable();
        idxs.dedup();
        assert_eq!(idxs.len(), SUMMARIES_ARMS.len(), "duplicate sub-arm index");
        assert_eq!(
            *idxs.last().expect("non-empty"),
            SUMMARIES_ARMS.len() - 1,
            "indices must be dense so the fixed-size arrays cover them"
        );

        let mut stems: Vec<&str> = SUMMARIES_ARMS.iter().copied().map(summaries_stem).collect();
        stems.sort_unstable();
        stems.dedup();
        assert_eq!(
            stems.len(),
            SUMMARIES_ARMS.len(),
            "duplicate sub-arm field stem"
        );

        // Every sub-arm nests under the parent stem, so a telemetry query can
        // pattern-match `interest_sync_summaries_*` and get the whole split.
        // Also guarantees the sub-arm keys can never collide with another
        // parent arm's keys.
        let parent = OutboundKind::InterestSyncSummaries.stem();
        for emitter in SUMMARIES_ARMS {
            let stem = summaries_stem(emitter);
            assert!(
                stem.starts_with(parent),
                "{stem} must nest under {parent} so the split is discoverable \
                 from the arm it refines"
            );
        }
    }

    /// The whole point of the split: each emitter's bytes land in its OWN arm.
    ///
    /// Distinguishable byte counts per emitter so a swapped or collapsed
    /// mapping cannot pass — the failure this guards is not a crash but a
    /// plausible-looking number filed under the wrong emitter, which would
    /// send #5003 vs. hash-first (#4965) the wrong way exactly as the combined
    /// arm did.
    #[test]
    fn each_emitter_lands_in_its_own_sub_arm() {
        let mix = OutboundMix::new();
        record_summaries(&mix, SummariesEmitter::Notification, 1, 100);
        record_summaries(&mix, SummariesEmitter::InterestsReply, 4, 200);
        record_summaries(&mix, SummariesEmitter::ChangeInterestsReply, 3, 400);
        record_summaries(&mix, SummariesEmitter::Rejection, 1, 800);
        record_summaries(&mix, SummariesEmitter::Other, 1, 1600);

        let w = mix.take_window();
        let bytes_of = |e| w.summaries_bytes[summaries_index(e)];
        assert_eq!(bytes_of(SummariesEmitter::Notification), 100);
        assert_eq!(bytes_of(SummariesEmitter::InterestsReply), 200);
        assert_eq!(bytes_of(SummariesEmitter::ChangeInterestsReply), 400);
        assert_eq!(bytes_of(SummariesEmitter::Rejection), 800);
        assert_eq!(bytes_of(SummariesEmitter::Other), 1600);

        for emitter in SUMMARIES_ARMS {
            assert_eq!(
                w.summaries_msgs[summaries_index(emitter)],
                1,
                "each emitter sent exactly one message: {emitter:?}"
            );
        }
    }

    /// The sub-arms must SUM to the parent arm, in both bytes and messages.
    ///
    /// Without this the split is worse than no split: a shortfall reads as
    /// "that emitter shrank" rather than "we stopped counting it", which is
    /// the exact misreading #5052 exists to end. Mixed in with non-Summaries
    /// traffic so the assertion also catches a sub-arm that double-counts from
    /// another parent arm.
    #[test]
    fn summaries_sub_arms_reconcile_with_the_parent_arm() {
        let mix = OutboundMix::new();
        record(&mix, OutboundKind::Update, 5_000);
        record(&mix, OutboundKind::InterestSyncInterests, 40);
        record_summaries(&mix, SummariesEmitter::Notification, 1, 100);
        record_summaries(&mix, SummariesEmitter::Notification, 1, 150);
        record_summaries(&mix, SummariesEmitter::InterestsReply, 9, 9_000);
        record_summaries(&mix, SummariesEmitter::ChangeInterestsReply, 2, 300);
        record_summaries(&mix, SummariesEmitter::Rejection, 1, 120);
        record(&mix, OutboundKind::InterestSyncResync, 70_000);

        let w = mix.take_window();
        let parent = OutboundKind::InterestSyncSummaries.index();
        assert_eq!(
            w.summaries_bytes.iter().sum::<u64>(),
            w.bytes[parent],
            "sub-arm bytes must sum to the parent arm"
        );
        assert_eq!(
            w.summaries_msgs.iter().sum::<u64>(),
            w.msgs[parent],
            "sub-arm messages must sum to the parent arm"
        );
        // Non-vacuous: the parent arm is a real, non-zero number, and it is
        // not simply the whole window (so a sub-arm that swept up unrelated
        // traffic would break the equality above rather than hide in it).
        assert_eq!(w.bytes[parent], 100 + 150 + 9_000 + 300 + 120);
        assert!(w.bytes.iter().sum::<u64>() > w.bytes[parent]);
    }

    /// A `Summaries` message that reaches the recorder with NO attribution is
    /// counted in the residual arm, not dropped.
    ///
    /// The mandatory tag makes this unreachable from today's call sites, which
    /// is exactly why it needs a test: it is the fallback that keeps the
    /// reconciliation above true no matter what a future call site does, and
    /// nothing else would notice if it silently `continue`d instead.
    #[test]
    fn an_unattributed_summaries_message_lands_in_the_residual_not_the_void() {
        let mix = OutboundMix::new();
        mix.record_sent(
            OutboundClass {
                kind: OutboundKind::InterestSyncSummaries,
                summaries: None,
            },
            777,
        );

        let w = mix.take_window();
        assert_eq!(
            w.summaries_bytes[summaries_index(SummariesEmitter::Other)],
            777,
            "an unattributed Summaries must land in the residual arm"
        );
        assert_eq!(
            w.summaries_bytes.iter().sum::<u64>(),
            w.bytes[OutboundKind::InterestSyncSummaries.index()],
            "the reconciliation must hold even for an unattributed message"
        );
    }

    /// Entry counts are recorded per sub-arm, which is the independent check
    /// that a call site is labelled correctly.
    ///
    /// Byte totals alone cannot catch a mislabelled emitter — they look
    /// plausible under any labelling. Mean entries per message can: the
    /// notification and rejection emitters are single-entry by construction,
    /// both reply emitters are multi-entry, so a reply arm reporting a mean of
    /// 1.0 (or a notification arm reporting 12) says the attribution is wrong
    /// even though every byte reconciles.
    #[test]
    fn entry_counts_are_recorded_per_sub_arm() {
        let mix = OutboundMix::new();
        // Two single-entry notifications.
        record_summaries(&mix, SummariesEmitter::Notification, 1, 100);
        record_summaries(&mix, SummariesEmitter::Notification, 1, 110);
        // Two multi-entry heartbeat replies, 12 and 4 entries.
        record_summaries(&mix, SummariesEmitter::InterestsReply, 12, 12_000);
        record_summaries(&mix, SummariesEmitter::InterestsReply, 4, 4_000);

        let w = mix.take_window();
        let notif = summaries_index(SummariesEmitter::Notification);
        let reply = summaries_index(SummariesEmitter::InterestsReply);

        assert_eq!(w.summaries_entries[notif], 2);
        assert_eq!(w.summaries_msgs[notif], 2);
        assert_eq!(
            w.summaries_max_entries[notif], 1,
            "a single-entry emitter must never report a wider max"
        );

        assert_eq!(w.summaries_entries[reply], 16);
        assert_eq!(w.summaries_msgs[reply], 2);
        assert_eq!(
            w.summaries_max_entries[reply], 12,
            "max_entries must track the widest single reply, not the mean"
        );

        // The derived quantity the analysis actually reads.
        assert_eq!(w.summaries_entries[notif] / w.summaries_msgs[notif], 1);
        assert_eq!(w.summaries_entries[reply] / w.summaries_msgs[reply], 8);
    }

    /// `classify` — not the caller — decides the sub-arm, and it reads the
    /// emitter tag off the message rather than guessing from its shape.
    ///
    /// This is the seam the whole attribution hangs on. A `classify` that
    /// returned a constant emitter, or inferred one from `entries.len()`,
    /// would still produce a reconciling, plausible split — and would be
    /// wrong, since a single-contract heartbeat reply and a notification are
    /// shape-identical (one entry each). So the two are asserted to classify
    /// APART at identical shape.
    #[test]
    fn classify_reads_the_emitter_tag_not_the_message_shape() {
        let detail = |m: &NetMessage| {
            let class = OutboundClass::classify(m);
            assert_eq!(class.kind, OutboundKind::InterestSyncSummaries);
            class.summaries.expect("Summaries must carry a sub-arm")
        };

        // Identical shape (one entry, same payload size), different emitters.
        let notification = detail(&summaries_msg(SummariesEmitter::Notification, 1, 64));
        let reply = detail(&summaries_msg(SummariesEmitter::InterestsReply, 1, 64));
        assert_eq!(notification.emitter, SummariesEmitter::Notification);
        assert_eq!(reply.emitter, SummariesEmitter::InterestsReply);
        assert_eq!(
            notification.entries, reply.entries,
            "the two cases must be shape-identical, or this test proves nothing"
        );

        // Entry count comes from the message, not from a default.
        assert_eq!(
            detail(&summaries_msg(SummariesEmitter::InterestsReply, 7, 8)).entries,
            7
        );

        // And the sub-arm exists for `Summaries` alone: every other message
        // carries none, so a stray `unwrap_or_default()` elsewhere could
        // not inflate the residual with non-Summaries traffic.
        let others = [
            NetMessage::V1(NetMessageV1::InterestSync {
                message: InterestMessage::Interests { hashes: vec![1] },
            }),
            NetMessage::V1(NetMessageV1::InterestSync {
                message: InterestMessage::ResyncRequest {
                    key: test_contract_key(1),
                },
            }),
            NetMessage::V1(NetMessageV1::ReadyState { ready: true }),
        ];
        for msg in others {
            let class = OutboundClass::classify(&msg);
            assert_ne!(class.kind, OutboundKind::InterestSyncSummaries);
            assert!(
                class.summaries.is_none(),
                "only Summaries may carry a sub-arm, got one for {class:?}"
            );
        }
    }

    /// Every sub-arm's five counters reach the emitted body under their own
    /// keys, and an idle window emits them as explicit zeros.
    ///
    /// The counters were only ever checked on the `Window` struct. A swapped
    /// or misspelled key in the body construction would ship one emitter's
    /// bytes under another's name — every other test still green, and the
    /// resulting number still reconciles. Distinguishable values per arm so a
    /// swap cannot pass.
    #[test]
    fn sub_arm_counters_reach_the_rollup_body_under_the_right_keys() {
        let mix = OutboundMix::new();
        // (emitter, entries, bytes) — all distinct.
        let sends = [
            (SummariesEmitter::Notification, 1usize, 11u64),
            (SummariesEmitter::InterestsReply, 22, 222),
            (SummariesEmitter::ChangeInterestsReply, 3, 333),
            (SummariesEmitter::Rejection, 4, 444),
            (SummariesEmitter::Other, 5, 555),
        ];
        for (emitter, entries, bytes) in sends {
            record_summaries(&mix, emitter, entries, bytes as usize);
        }
        let body = outbound_mix_json(&mix.take_window(), 60);
        let field = |k: &str| {
            body.get(k)
                .and_then(|v| v.as_u64())
                .unwrap_or_else(|| panic!("missing rollup field {k}"))
        };

        for (emitter, entries, bytes) in sends {
            let stem = summaries_stem(emitter);
            assert_eq!(field(&format!("{stem}_msgs")), 1, "{stem}_msgs");
            assert_eq!(field(&format!("{stem}_bytes")), bytes, "{stem}_bytes");
            assert_eq!(
                field(&format!("{stem}_max_bytes")),
                bytes,
                "{stem}_max_bytes"
            );
            assert_eq!(
                field(&format!("{stem}_entries")),
                entries as u64,
                "{stem}_entries"
            );
            assert_eq!(
                field(&format!("{stem}_max_entries")),
                entries as u64,
                "{stem}_max_entries"
            );
        }

        // The reconciliation must be checkable FROM THE BODY, since that is
        // all the telemetry query has.
        let summed: u64 = SUMMARIES_ARMS
            .iter()
            .map(|e| field(&format!("{}_bytes", summaries_stem(*e))))
            .sum();
        assert_eq!(
            summed,
            field("interest_sync_summaries_bytes"),
            "the emitted sub-arms must reconcile with the emitted parent arm"
        );

        // Idle window: explicit zeros, not omitted fields. A missing field and
        // a zero field look the same to a naive query and mean opposite things
        // ("this emitter was quiet" vs "this build has no split").
        let idle = outbound_mix_json(&Window::default(), 60);
        for emitter in SUMMARIES_ARMS {
            let stem = summaries_stem(emitter);
            for suffix in ["msgs", "bytes", "max_bytes", "entries", "max_entries"] {
                let key = format!("{stem}_{suffix}");
                assert_eq!(
                    idle.get(&key).and_then(|v| v.as_u64()),
                    Some(0),
                    "an idle window must emit {key} as an explicit zero"
                );
            }
        }
    }

    /// Emitter-completeness pin: which production files touch
    /// `InterestMessage::Summaries` at all, and which arm each tagging site
    /// claims.
    ///
    /// The mandatory `emitter` field already stops a fifth emitter from
    /// SILENTLY landing in the residual — it will not compile without naming
    /// an arm. What a mandatory field cannot stop is the lazy answer: a new
    /// emitter that reuses an existing arm because it looked close enough.
    /// That re-creates the conflation #5052 exists to undo, one level down,
    /// and it is invisible — the bytes still reconcile, so no other test
    /// notices.
    ///
    /// So this pin walks the crate source (`$CARGO_MANIFEST_DIR/src/**/*.rs`),
    /// strips `#[cfg(test)]` regions, and asserts two things:
    ///
    ///   1. the SET of production files mentioning the variant is unchanged —
    ///      a new file touching it fails CI even before we look at tags, and
    ///   2. the arm each tagging site claims, per file.
    ///
    /// Known limit, stated rather than papered over: (2) scrapes the literal
    /// `emitter: SummariesEmitter::<Arm>`, so a site that assigned the tag
    /// from a variable would escape it. (1) still catches such a site if it
    /// lives in a new file, and a wrongly-tagged one shows up as an
    /// entries-per-message anomaly (`entry_counts_are_recorded_per_sub_arm`)
    /// or in the residual arm. Needles are built with `concat!` so this test's
    /// own source cannot satisfy the scrape when the walk reaches this file.
    #[test]
    fn summaries_emitter_sites_are_pinned() {
        use crate::node::network_bridge::p2p_protoc::tests::{
            collect_rs_files, strip_cfg_test_regions,
        };
        use std::collections::{BTreeMap, BTreeSet};

        // Production files (relative to `src/`) whose code mentions the
        // variant, for ANY reason — construction, `match` pattern, `Display`.
        // Deliberately wider than "emitters": the point is that a new file
        // touching Summaries at all is a deliberate decision.
        let expected_files: BTreeSet<&str> = [
            "message.rs",                                  // variant + Display
            "node.rs",                                     // both reply emitters + the receive arm
            "node/network_bridge/outbound_message_mix.rs", // the classify choke point
            "operations/update.rs",                        // notification + rejection emitters
        ]
        .into_iter()
        .collect();

        // file → the arms it tags, one entry per DISTINCT arm.
        let expected_arms: BTreeMap<&str, Vec<&str>> = [
            ("node.rs", vec!["ChangeInterestsReply", "InterestsReply"]),
            ("operations/update.rs", vec!["Notification", "Rejection"]),
        ]
        .into_iter()
        .collect();

        let src_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        collect_rs_files(&src_root, &mut files);
        assert!(
            !files.is_empty(),
            "#5052: the source walk found no .rs files under {} — the pin \
             cannot guarantee completeness if it can't read the crate source",
            src_root.display()
        );

        let mentions = concat!("InterestMessage::", "Summaries {");
        let tag = concat!("emitter: ", "SummariesEmitter::");

        let mut found_files: BTreeSet<String> = Default::default();
        let mut found_arms: BTreeMap<String, Vec<String>> = Default::default();
        for path in &files {
            let rel = path
                .strip_prefix(&src_root)
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/");
            // Whole test files are gated by a parent `#[cfg(test)] mod`, which
            // `strip_cfg_test_regions` cannot see from inside the file.
            if rel.ends_with("/tests.rs") || rel == "tests.rs" || rel.contains("/tests/") {
                continue;
            }
            let Ok(src) = std::fs::read_to_string(path) else {
                continue;
            };
            let prod = strip_cfg_test_regions(&src);
            if !prod.contains(mentions) {
                continue;
            }
            found_files.insert(rel.clone());

            let mut arms: Vec<String> = prod
                .match_indices(tag)
                .map(|(idx, _)| {
                    prod[idx + tag.len()..]
                        .split(|c: char| !c.is_alphanumeric() && c != '_')
                        .next()
                        .unwrap_or("")
                        .to_string()
                })
                .collect();
            arms.sort();
            arms.dedup();
            if !arms.is_empty() {
                found_arms.insert(rel, arms);
            }
        }

        let found_files_view: BTreeSet<&str> = found_files.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            found_files_view, expected_files,
            "#5052: the set of production files mentioning InterestMessage::Summaries \
             changed. If this is a NEW emitter, give it its OWN SummariesEmitter arm \
             rather than reusing one — reusing a tag re-creates exactly the conflation \
             this split undoes, and the bytes still reconcile so nothing else flags it. \
             Then register the file here."
        );

        let found_arms_view: BTreeMap<&str, Vec<&str>> = found_arms
            .iter()
            .map(|(f, arms)| (f.as_str(), arms.iter().map(|a| a.as_str()).collect()))
            .collect();
        assert_eq!(
            found_arms_view, expected_arms,
            "#5052: the emitter→arm mapping changed. Every emitter must claim its own \
             arm; update this pin only after confirming the new site genuinely belongs \
             in the arm it names."
        );

        // Every declared arm except the residual must actually be claimed by a
        // production site: an arm nothing emits reports a permanent zero,
        // which reads as "that emitter is free" rather than "it is gone".
        let claimed: BTreeSet<&str> = found_arms_view.values().flatten().copied().collect();
        for emitter in SUMMARIES_ARMS {
            if emitter == SummariesEmitter::Other {
                continue;
            }
            let name = format!("{emitter:?}");
            assert!(
                claimed.contains(name.as_str()),
                "#5052: SummariesEmitter::{name} is declared but no production site \
                 emits it — it would report a permanent zero, which reads as \
                 'that emitter costs nothing' rather than 'nothing emits it'. \
                 Either wire it up or delete the arm. Claimed: {claimed:?}"
            );
        }
    }
}
