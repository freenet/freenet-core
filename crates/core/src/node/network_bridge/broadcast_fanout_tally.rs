//! Per-fan-out delta-vs-full-state tally behind `UpdateEvent::BroadcastComplete`.
//!
//! ## Why
//!
//! `UpdateEvent::BroadcastComplete` has existed since #3622-era telemetry work
//! and `telemetry.rs` has always known how to serialize it (`"type":
//! "broadcast_complete"`, including a derived `delta_ratio`), but **nothing ever
//! constructed it**, so the arm never fired. The same was true of
//! [`InterestManagerStats`]: #4922 added `delta_sends` / `full_state_sends` /
//! `delta_bytes_saved` counters to `InterestManager`, but `stats()` has no
//! caller, so those counters were tallied in memory and never read. This module
//! is the missing reporting half for the per-fan-out view (#4923).
//!
//! The gap mattered because `state_size` — the only size the update telemetry
//! carried — logs the post-apply FULL state regardless of whether a small delta
//! or the whole state crossed the wire. A River room reporting ~370 KB
//! broadcast payloads against a ~320-339 KB state and a ~15 KB delta could not
//! be explained from telemetry alone: nothing distinguished "we sent a delta"
//! from "we sent the entire state".
//!
//! ## Relationship to the #4922 payload mix
//!
//! [`PayloadMix`] answers the same question at a coarser grain and with a finer
//! *cause* breakdown: a 60-second per-node rollup of per-arm sends and real wire
//! bytes, naming which of the four full-state fallbacks fired. It is the right
//! tool for "what is this node's byte mix, and why". It deliberately does NOT
//! carry per-fan-out identity, so it cannot answer "this particular update to
//! this contract went out as N deltas and M full states".
//!
//! This tally is that second view, and only that: one event per fan-out,
//! carrying the fan-out's transaction so it joins directly against the
//! `BroadcastEmitted` emitted for the same fan-out. The two are complementary;
//! neither replaces the other.
//!
//! ## Why a tally rather than reading the counters
//!
//! [`InterestManagerStats`] counters are process-lifetime monotonic totals
//! across every contract and peer. Differencing them around a fan-out would
//! attribute concurrently-running fan-outs (other contracts, other peers) to
//! whichever one happened to sample last, because the production path runs one
//! detached task per (contract, peer) with bounded concurrency. A per-fan-out
//! accumulator is the only way to get correct attribution.
//!
//! To keep that from becoming a second, independently-rotting mirror — the
//! failure mode in the "Manually-mirrored telemetry counters" row of
//! `.claude/rules/bug-prevention-patterns.md`, and the #4009 / #4010 precedent —
//! every tally write sits immediately next to the canonical
//! `InterestManager::record_delta_send` / `record_full_state_send` call it
//! shadows, and source-scrape pins in `broadcast_queue.rs` and
//! `p2p_protoc/broadcast.rs` assert that co-location at both recording sites.
//!
//! [`InterestManagerStats`]: crate::ring::interest::InterestManagerStats
//! [`PayloadMix`]: super::broadcast_payload_mix::PayloadMix

use std::sync::Arc;

use freenet_stdlib::prelude::ContractKey;
use parking_lot::Mutex;

use crate::message::Transaction;
use crate::node::OpManager;
use crate::tracing::NetEventLog;

/// One fan-out's accumulated delta-vs-full-state split.
///
/// Returned by [`FanoutTally::finish_one`] to exactly one caller — the one that
/// retires the last outstanding target — so a fan-out can never emit twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FanoutSummary {
    pub tx: Transaction,
    pub key: ContractKey,
    pub delta_sends: usize,
    pub full_state_sends: usize,
    pub bytes_saved: u64,
    pub state_size: usize,
}

/// Mutable half of [`FanoutTally`], behind one lock.
///
/// One lock rather than per-field atomics, for the reason #4922's [`PayloadMix`]
/// documents: the terminal read has to see the counters and the outstanding
/// count as a single consistent snapshot, otherwise a send recorded between
/// "decrement to zero" and "read the counters" is silently lost from the event.
/// Cost is one uncontended acquire per *delivered broadcast* — the surrounding
/// path has already run WASM delta computation and a network write.
///
/// [`PayloadMix`]: super::broadcast_payload_mix::PayloadMix
#[derive(Debug)]
struct TallyInner {
    /// Targets that have not yet retired. Starts at 1 for the creation guard
    /// (see [`FanoutTally::new`]).
    outstanding: usize,
    delta_sends: usize,
    full_state_sends: usize,
    bytes_saved: u64,
    /// Set once the summary has been handed out, so a stray extra
    /// [`finish_one`](FanoutTally::finish_one) can never produce a second event.
    finished: bool,
}

/// Accumulates one fan-out's per-target payload choices.
///
/// Lifecycle: [`new`](Self::new) with the fan-out's target count (plus a
/// creation guard) → per-target [`record_delta`](Self::record_delta) /
/// [`record_full_state`](Self::record_full_state) → one
/// [`finish_one`](Self::finish_one) per target *and* one for the creation guard.
/// The last of those returns the [`FanoutSummary`].
#[derive(Debug)]
pub(crate) struct FanoutTally {
    tx: Transaction,
    key: ContractKey,
    /// Size of the full post-apply state being broadcast. Reported verbatim as
    /// the event's `state_size`.
    state_size: usize,
    inner: Mutex<TallyInner>,
}

impl FanoutTally {
    /// Start a tally for a fan-out with `targets` targets, plus one **creation
    /// guard**.
    ///
    /// The guard exists so a fan-out whose first target completes before the
    /// dispatcher has finished handing out the rest cannot hit zero outstanding
    /// early and emit a truncated event. The dispatcher retires it with its own
    /// [`finish_one`](Self::finish_one) once every target has been dispatched.
    ///
    /// `targets` must equal the number of retirements the caller will actually
    /// produce (one per [`FanoutTarget`], or zero for a caller that records
    /// inline). Declaring more than are produced strands the tally so it never
    /// emits; declaring fewer emits early, and the surplus retirements are
    /// absorbed by the `finished` flag rather than emitting twice.
    pub(crate) fn new(
        tx: Transaction,
        key: ContractKey,
        state_size: usize,
        targets: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            tx,
            key,
            state_size,
            inner: Mutex::new(TallyInner {
                outstanding: targets.saturating_add(1),
                delta_sends: 0,
                full_state_sends: 0,
                bytes_saved: 0,
                finished: false,
            }),
        })
    }

    /// Record that a target received a DELTA.
    ///
    /// Mirrors [`InterestManager::record_delta_send`]'s signature on purpose:
    /// the two are called as a pair at every recording site, and taking the same
    /// arguments makes a divergence between them obvious on sight.
    ///
    /// [`InterestManager::record_delta_send`]: crate::ring::interest::InterestManager::record_delta_send
    pub(crate) fn record_delta(&self, state_size: usize, delta_size: usize) {
        let mut inner = self.inner.lock();
        inner.delta_sends += 1;
        // Saturating: a delta larger than the state would otherwise wrap into a
        // huge "saving", inverting the very ratio this event exists to report.
        inner.bytes_saved = inner
            .bytes_saved
            .saturating_add(state_size.saturating_sub(delta_size) as u64);
    }

    /// Record that a target received FULL STATE.
    pub(crate) fn record_full_state(&self) {
        self.inner.lock().full_state_sends += 1;
    }

    /// Retire one outstanding target (or the creation guard).
    ///
    /// Returns `Some` exactly once per tally, to whichever caller retires the
    /// last outstanding entry. Every other call returns `None`.
    pub(crate) fn finish_one(&self) -> Option<FanoutSummary> {
        let mut inner = self.inner.lock();
        inner.outstanding = inner.outstanding.saturating_sub(1);
        if inner.outstanding > 0 || inner.finished {
            return None;
        }
        inner.finished = true;
        Some(FanoutSummary {
            tx: self.tx,
            key: self.key,
            delta_sends: inner.delta_sends,
            full_state_sends: inner.full_state_sends,
            bytes_saved: inner.bytes_saved,
            state_size: self.state_size,
        })
    }
}

/// Emit one `UpdateEvent::BroadcastComplete`.
///
/// Non-blocking: `register_events` is `try_send` internally and drops on a full
/// channel (see `EventRegister::register_events`), so telemetry can never
/// backpressure a broadcast. That property is what makes it safe to call this
/// from the fan-out path at all — see `.claude/rules/channel-safety.md`.
pub(crate) async fn emit_broadcast_complete(op_manager: &OpManager, summary: FanoutSummary) {
    if let Some(log) = NetEventLog::update_broadcast_complete(
        &summary.tx,
        &op_manager.ring,
        summary.key,
        summary.delta_sends,
        summary.full_state_sends,
        summary.bytes_saved,
        summary.state_size,
    ) {
        op_manager
            .ring
            .register_events(either::Either::Left(log))
            .await;
    }
}

/// RAII retirement of one fan-out target for the production `BroadcastQueue`.
///
/// The queue reaches its terminal states through several paths that do not
/// share a common exit — a target is sent, or its entry is superseded by
/// replace-on-dedup, or it is evicted when the queue is at capacity, and
/// `broadcast_to_single_peer` itself has several early returns. Making
/// retirement a `Drop` obligation rather than an explicit call means a future
/// path added to the queue retires its target automatically; an explicit call
/// would have to be remembered at each new site, and a missed one would silently
/// strand the fan-out so its event never fired.
///
/// Only the production path needs this. The simulation fan-out is inline and
/// sequential, so it drives [`FanoutTally`] directly and emits without a spawn,
/// keeping deterministic broadcast ordering.
#[cfg(not(feature = "simulation_tests"))]
pub(crate) struct FanoutTarget {
    tally: Arc<FanoutTally>,
    op_manager: Arc<OpManager>,
}

#[cfg(not(feature = "simulation_tests"))]
impl FanoutTarget {
    /// Take one of `tally`'s declared targets. The caller must create exactly as
    /// many of these as it passed to [`FanoutTally::new`].
    pub(crate) fn new(tally: Arc<FanoutTally>, op_manager: Arc<OpManager>) -> Self {
        Self { tally, op_manager }
    }

    /// The tally this target reports into, for the recording sites.
    pub(crate) fn tally(&self) -> &Arc<FanoutTally> {
        &self.tally
    }
}

#[cfg(not(feature = "simulation_tests"))]
impl Drop for FanoutTarget {
    fn drop(&mut self) {
        let Some(summary) = self.tally.finish_one() else {
            return;
        };
        let op_manager = self.op_manager.clone();
        // Fire-and-forget is correct here per `.claude/rules/code-style.md`:
        // this is short-lived work, not a node-lifetime task, so it does not
        // belong to the BackgroundTaskMonitor. The spawn exists only because
        // `Drop` cannot await — the future it runs does one non-blocking
        // `try_send` and returns. Dropping the JoinHandle is deliberate: a lost
        // telemetry event must never be able to fail a broadcast.
        crate::config::GlobalExecutor::spawn(async move {
            emit_broadcast_complete(&op_manager, summary).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};

    fn make_key(seed: u8) -> ContractKey {
        ContractKey::from_id_and_code(
            ContractInstanceId::new([seed; 32]),
            CodeHash::new([seed.wrapping_add(1); 32]),
        )
    }

    fn make_tx() -> Transaction {
        Transaction::new::<crate::operations::update::UpdateMsg>()
    }

    /// The creation guard is the whole reason a fan-out whose first target
    /// finishes before the dispatcher has handed out the rest does not emit
    /// early. Without it, a two-target fan-out reports only the first send.
    #[test]
    fn creation_guard_defers_emission_until_the_dispatcher_retires_it() {
        let tally = FanoutTally::new(make_tx(), make_key(1), 1_000, 2);

        tally.record_delta(1_000, 100);
        assert_eq!(
            tally.finish_one(),
            None,
            "the creation guard must keep the tally open"
        );

        tally.record_full_state();
        assert_eq!(tally.finish_one(), None);

        let summary = tally
            .finish_one()
            .expect("retiring the creation guard must complete the tally");
        assert_eq!(summary.delta_sends, 1);
        assert_eq!(summary.full_state_sends, 1);
        assert_eq!(summary.bytes_saved, 900);
        assert_eq!(summary.state_size, 1_000);
    }

    #[test]
    fn summary_is_handed_out_exactly_once() {
        let tally = FanoutTally::new(make_tx(), make_key(2), 500, 1);
        tally.record_delta(500, 50);

        assert_eq!(tally.finish_one(), None, "guard still outstanding");
        assert!(tally.finish_one().is_some(), "last retirement emits");
        assert_eq!(
            tally.finish_one(),
            None,
            "a stray extra retirement must not produce a second event"
        );
        assert_eq!(tally.finish_one(), None);
    }

    /// A fan-out that resolves every target to "already converged" still has to
    /// complete: zero recorded sends is a real, informative observation (nothing
    /// went on the wire), not a reason to withhold the event.
    #[test]
    fn fanout_with_no_recorded_sends_still_completes_with_zeroes() {
        let tally = FanoutTally::new(make_tx(), make_key(3), 4_096, 2);
        assert_eq!(tally.finish_one(), None);
        assert_eq!(tally.finish_one(), None);

        let summary = tally.finish_one().expect("guard retirement completes");
        assert_eq!(summary.delta_sends, 0);
        assert_eq!(summary.full_state_sends, 0);
        assert_eq!(summary.bytes_saved, 0);
        assert_eq!(summary.state_size, 4_096);
    }

    /// A fan-out with no targets at all (every candidate filtered out before
    /// enqueue) completes on the guard alone rather than leaking. This is also
    /// the shape the inline simulation path uses: it declares zero targets and
    /// records against the tally directly.
    #[test]
    fn fanout_with_zero_targets_completes_on_the_guard() {
        let tally = FanoutTally::new(make_tx(), make_key(4), 7, 0);
        let summary = tally.finish_one().expect("guard alone completes the tally");
        assert_eq!(summary.delta_sends, 0);
        assert_eq!(summary.full_state_sends, 0);
    }

    /// `bytes_saved` is what makes the event able to distinguish a delta from a
    /// full state at all: `state_size * sends - bytes_saved` is the real wire
    /// total. A delta larger than the state must clamp to zero saving rather
    /// than wrapping into a nonsense saving.
    #[test]
    fn oversized_delta_saves_zero_bytes_rather_than_wrapping() {
        let tally = FanoutTally::new(make_tx(), make_key(5), 100, 1);
        tally.record_delta(100, 250);
        assert_eq!(tally.finish_one(), None, "guard still outstanding");
        let summary = tally.finish_one().expect("guard retirement completes");
        assert_eq!(summary.bytes_saved, 0);
        assert_eq!(summary.delta_sends, 1);
    }

    #[test]
    fn bytes_saved_accumulates_across_targets() {
        let tally = FanoutTally::new(make_tx(), make_key(6), 1_000, 3);
        for _ in 0..3 {
            tally.record_delta(1_000, 200);
            assert_eq!(tally.finish_one(), None);
        }
        let summary = tally.finish_one().expect("guard retirement completes");
        assert_eq!(summary.delta_sends, 3);
        assert_eq!(summary.bytes_saved, 2_400);
    }

    /// Concurrent retirement is the production shape: the queue spawns one
    /// detached task per target. Exactly one of them must observe the summary,
    /// and it must carry every increment.
    #[test]
    fn concurrent_targets_emit_exactly_once_with_all_sends_counted() {
        const TARGETS: usize = 32;
        let tally = FanoutTally::new(make_tx(), make_key(7), 1_000, TARGETS);

        let completions = Arc::new(Mutex::new(Vec::new()));
        std::thread::scope(|scope| {
            for i in 0..TARGETS {
                let tally = Arc::clone(&tally);
                let completions = Arc::clone(&completions);
                scope.spawn(move || {
                    if i % 2 == 0 {
                        tally.record_delta(1_000, 100);
                    } else {
                        tally.record_full_state();
                    }
                    if let Some(summary) = tally.finish_one() {
                        completions.lock().push(summary);
                    }
                });
            }
        });

        // Every target retired but the guard is still held, so nothing emitted.
        assert!(completions.lock().is_empty());

        let summary = tally.finish_one().expect("guard retirement completes");
        assert_eq!(summary.delta_sends, TARGETS / 2);
        assert_eq!(summary.full_state_sends, TARGETS / 2);
        assert_eq!(summary.bytes_saved, (TARGETS / 2) as u64 * 900);
    }
}
