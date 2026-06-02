//! Bounded periodic aggregator for UPDATE-propagation liveness logging.
//!
//! ## Why this exists
//!
//! PRs #4252 and #4272 demoted the three high-volume per-event UPDATE log
//! sites (`Contract state updated`, the `UPDATE_PROPAGATION` broadcast line,
//! and the `shadow_rtt_aggregate` local mirror) from INFO to DEBUG to stop a
//! River-driven log-spam disk-fill. The fix worked, but at the default
//! `RUST_LOG=info` level it left operators with **no liveness signal** about
//! whether a node is propagating contract updates at all — only failure-path
//! messages survive at INFO/WARN. For a healthy node carrying River traffic
//! the log goes silent. See issue #4281.
//!
//! This module restores that visibility **without** re-introducing per-event
//! volume: the broadcast path bumps cheap per-contract counters (a single
//! `DashMap` shard write), and a background task drains-and-emits a single
//! bounded INFO summary line (plus up to a fixed number of per-contract lines)
//! once per window.
//!
//! ## Budget
//!
//! At [`SUMMARY_WINDOW`] = 60s and [`TOP_K`] = 8, the worst-case INFO rate
//! from this emitter is `(TOP_K + 1)` lines per window — under 0.2 lines/sec
//! sustained — well inside the "well under 1 line/sec" budget the issue
//! requires. A window with zero activity emits nothing at all.
//!
//! ## Counter semantics
//!
//! Counters are *windowed*: `drain_window` snapshots every entry and clears
//! the map, so each emitted summary describes exactly the activity in the
//! window that just elapsed. Both the record and the drain mutate each
//! contract's counters under the same `DashMap` per-key write guard, so a
//! drain captures each record atomically (never half-applied). The tracking
//! map is size-capped ([`MAX_TRACKED_CONTRACTS`]) so a churn of
//! network-influenced contract keys cannot grow it unboundedly (per the
//! per-key-collection rule in `.claude/rules/code-style.md`).

use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;

use crate::config::GlobalExecutor;

/// How often the summary task drains the counters and emits a window summary.
/// 60s is the natural starting point per issue #4281 — short enough for
/// usable debugging granularity, long enough to keep the log rate trivial.
const SUMMARY_WINDOW: Duration = Duration::from_secs(60);

/// Maximum number of per-contract lines emitted per window (top-K by update
/// activity). Contracts beyond the top-K are summarised in a single rollup
/// line ("…and N others"). Bounds the per-window INFO line count to
/// `TOP_K + 1`.
const TOP_K: usize = 8;

/// Maximum number of distinct contracts tracked at once. A new contract is
/// dropped (its activity goes uncounted for that window) once the map is at
/// capacity, rather than letting a key churn grow the map without bound.
/// 4096 is comfortably above the active-contract count of a busy gateway
/// while still capping memory at a few hundred KB worst case.
const MAX_TRACKED_CONTRACTS: usize = 4096;

/// Per-contract windowed counters.
///
/// Plain `u64` fields, not atomics: every read and write happens while holding
/// the `DashMap` per-key write guard (`entry`/`get_mut`), so a `record` and a
/// `drain` for the same contract are mutually exclusive. That makes each
/// multi-field record and each multi-field drain atomic *as a unit* — a drain
/// can never observe a half-written record (e.g. `updates` incremented but
/// `targets_total` not yet), which a set of independent per-field atomics
/// could split across two windows. Different contracts still update fully in
/// parallel (separate `DashMap` shards), so the hot path stays lock-free
/// across contracts.
#[derive(Default)]
struct ContractCounters {
    /// Number of broadcast fan-out attempts for this contract in the window
    /// (one per `BroadcastStateChange` that reached a terminal outcome).
    updates: u64,
    /// Sum of resolved broadcast targets across those attempts. Divided by
    /// `updates` at emit time to produce `targets_avg`.
    targets_total: u64,
    /// Number of attempts that resolved zero targets (the NO_TARGETS branch:
    /// the update did not propagate further from this node).
    no_targets: u64,
    /// Sum of interest-manager peers that failed to resolve to a live
    /// connection across the window (the `interest_resolve_failed` counter
    /// from `BroadcastTargetResult`). A persistently non-zero value points at
    /// stale interest state.
    interest_resolve_failed: u64,
}

impl ContractCounters {
    /// True when this entry recorded no activity for the window.
    fn is_empty(&self) -> bool {
        self.updates == 0
            && self.targets_total == 0
            && self.no_targets == 0
            && self.interest_resolve_failed == 0
    }
}

/// A drained, immutable snapshot of one contract's window activity, used for
/// sorting and formatting. Plain values (no atomics) so the aggregation logic
/// is a pure, testable function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ContractSummary {
    pub(crate) contract: ContractInstanceId,
    pub(crate) updates: u64,
    pub(crate) targets_total: u64,
    pub(crate) no_targets: u64,
    pub(crate) interest_resolve_failed: u64,
}

impl ContractSummary {
    /// Mean resolved targets per update attempt, rounded to one decimal.
    /// Returns 0.0 when there were no attempts (avoids divide-by-zero).
    fn targets_avg(&self) -> f64 {
        if self.updates == 0 {
            0.0
        } else {
            // Round to one decimal place for a compact, stable log field.
            let avg = self.targets_total as f64 / self.updates as f64;
            (avg * 10.0).round() / 10.0
        }
    }
}

/// Aggregated view of a single drained window, ready to log.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WindowSummary {
    /// Distinct contracts with at least one update attempt this window.
    pub(crate) contracts: usize,
    /// Total update fan-out attempts across all contracts this window.
    pub(crate) updates: u64,
    /// Total resolved targets across all attempts this window.
    pub(crate) targets_total: u64,
    /// Total attempts that resolved zero targets this window.
    pub(crate) no_targets: u64,
    /// Total interest-resolve failures across all contracts this window.
    pub(crate) interest_resolve_failed: u64,
    /// The top-K contracts by update activity (descending), for per-contract
    /// lines. Length is at most [`TOP_K`].
    pub(crate) top: Vec<ContractSummary>,
    /// Number of additional active contracts beyond the top-K (the rollup
    /// count). Zero when `contracts <= TOP_K`.
    pub(crate) others: usize,
}

impl WindowSummary {
    /// Mean resolved targets per update attempt across the whole window.
    pub(crate) fn targets_avg(&self) -> f64 {
        if self.updates == 0 {
            0.0
        } else {
            let avg = self.targets_total as f64 / self.updates as f64;
            (avg * 10.0).round() / 10.0
        }
    }
}

/// Pure aggregation: turn a set of drained per-contract summaries into a
/// bounded [`WindowSummary`]. Selects the top-`top_k` contracts by update
/// count (ties broken by contract id for determinism) and rolls the rest up
/// into an `others` count. Contracts with zero update attempts are ignored
/// (a tracked contract can have a live map entry but no activity this window).
///
/// Extracted from the draining/IO so it can be unit-tested without a runtime.
pub(crate) fn aggregate_window(
    mut summaries: Vec<ContractSummary>,
    top_k: usize,
) -> Option<WindowSummary> {
    summaries.retain(|s| s.updates > 0);
    if summaries.is_empty() {
        return None;
    }

    let contracts = summaries.len();
    let updates: u64 = summaries.iter().map(|s| s.updates).sum();
    let targets_total: u64 = summaries.iter().map(|s| s.targets_total).sum();
    let no_targets: u64 = summaries.iter().map(|s| s.no_targets).sum();
    let interest_resolve_failed: u64 = summaries.iter().map(|s| s.interest_resolve_failed).sum();

    // Most active first; deterministic tie-break on the contract's raw bytes
    // so a given window always produces the same top-K regardless of map
    // iteration order. `ContractInstanceId` does not implement `Ord`, so we
    // compare its stable public byte representation explicitly rather than
    // relying on deref-coercion to `[u8; N]::cmp`.
    summaries.sort_by(|a, b| {
        b.updates
            .cmp(&a.updates)
            .then_with(|| a.contract.as_bytes().cmp(b.contract.as_bytes()))
    });

    let others = contracts.saturating_sub(top_k);
    summaries.truncate(top_k);

    Some(WindowSummary {
        contracts,
        updates,
        targets_total,
        no_targets,
        interest_resolve_failed,
        top: summaries,
        others,
    })
}

/// Bounded per-contract UPDATE-propagation counters with a periodic summary
/// emitter. Cheap to feed from the hot broadcast path (one `DashMap` shard
/// lookup + a few relaxed atomic adds); drained and logged once per window by
/// the background task started with [`Self::start_summary_task`].
pub(crate) struct UpdatePropagationStats {
    contracts: DashMap<ContractInstanceId, ContractCounters>,
}

impl UpdatePropagationStats {
    pub(crate) fn new() -> Self {
        Self {
            contracts: DashMap::new(),
        }
    }

    /// Record the terminal outcome of one logical broadcast for `contract`.
    ///
    /// * `targets` — number of resolved broadcast targets (0 ⇒ NO_TARGETS).
    /// * `interest_resolve_failed` — interest-manager peers that failed to
    ///   resolve to a live connection on this attempt.
    ///
    /// All four counter fields are mutated together while holding the
    /// `DashMap` per-key write guard, so the whole record is atomic against a
    /// concurrent [`Self::drain_window`] of the same contract — a drain can
    /// never capture a half-applied record. Different contracts live on
    /// different shards and still update in parallel.
    ///
    /// When a brand-new contract would push the map past
    /// [`MAX_TRACKED_CONTRACTS`], the record is dropped (uncounted) rather
    /// than growing the map — existing tracked contracts are unaffected.
    pub(crate) fn record_broadcast(
        &self,
        contract: ContractInstanceId,
        targets: usize,
        interest_resolve_failed: usize,
    ) {
        // Cap only blocks *new* contracts; updates to already-tracked
        // contracts always proceed. `len()` can briefly race other inserters,
        // but the cap is a soft memory guard (not a correctness boundary), so
        // a small overshoot is acceptable.
        if !self.contracts.contains_key(&contract) && self.contracts.len() >= MAX_TRACKED_CONTRACTS
        {
            return;
        }

        let mut entry = self.contracts.entry(contract).or_default();
        entry.updates += 1;
        entry.targets_total += targets as u64;
        if targets == 0 {
            entry.no_targets += 1;
        }
        entry.interest_resolve_failed += interest_resolve_failed as u64;
    }

    /// Snapshot every tracked contract's counters into per-contract window
    /// summaries, then clear the map.
    ///
    /// Uses [`DashMap::retain`], which holds each shard's write guard while it
    /// visits that shard's entries. Because [`Self::record_broadcast`] mutates
    /// under the same per-key guard, a contract's full multi-field record is
    /// captured atomically here — a drain can never split one record across
    /// two windows (Codex review P2). The closure always returns `false`, so
    /// **every** drained entry is removed: the snapshot fully captures the
    /// window's activity, and clearing the map means the next window starts
    /// with full [`MAX_TRACKED_CONTRACTS`] headroom instead of staying
    /// saturated with the previous window's one-off contracts (Codex review
    /// P2 — the cap blind-spot under key churn). Any `record_broadcast` that
    /// runs after a shard is visited simply re-creates the entry and is
    /// counted in the next window.
    fn drain_window(&self) -> Vec<ContractSummary> {
        let mut out = Vec::with_capacity(self.contracts.len());
        self.contracts.retain(|id, counters| {
            if !counters.is_empty() {
                out.push(ContractSummary {
                    contract: *id,
                    updates: counters.updates,
                    targets_total: counters.targets_total,
                    no_targets: counters.no_targets,
                    interest_resolve_failed: counters.interest_resolve_failed,
                });
            }
            // Remove every entry — clears the map so the cap resets each window.
            false
        });
        out
    }

    /// Drain the current window and emit it at INFO. Public for the periodic
    /// task and for deterministic unit tests (which call it directly after
    /// recording, instead of waiting on the real-time ticker).
    pub(crate) fn drain_and_log(&self) {
        let Some(summary) = aggregate_window(self.drain_window(), TOP_K) else {
            // Nothing happened this window — stay silent (budget rule).
            return;
        };

        tracing::info!(
            contracts = summary.contracts,
            updates = summary.updates,
            targets_avg = summary.targets_avg(),
            no_targets = summary.no_targets,
            interest_resolve_failed = summary.interest_resolve_failed,
            window_s = SUMMARY_WINDOW.as_secs(),
            phase = "summary",
            "update_propagation_summary"
        );

        for c in &summary.top {
            tracing::info!(
                contract = %format!("{:.8}", c.contract),
                updates = c.updates,
                targets_avg = c.targets_avg(),
                no_targets = c.no_targets,
                interest_resolve_failed = c.interest_resolve_failed,
                phase = "summary_contract",
                "update_propagation_summary contract detail"
            );
        }

        if summary.others > 0 {
            tracing::info!(
                others = summary.others,
                phase = "summary_rollup",
                "update_propagation_summary …and {} other contract(s)",
                summary.others
            );
        }
    }

    /// Spawn the background summary task and return its `JoinHandle` so the
    /// caller can register it with the node's `BackgroundTaskMonitor` (this is
    /// a lifetime task, not fire-and-forget — see `code-style.md`).
    ///
    /// Drains and logs once per [`SUMMARY_WINDOW`]. Scheduling uses real time
    /// (`tokio::time::interval`) like the other periodic tasks
    /// (`InterestManager::start_sweep_task`, `OrphanStreamRegistry::start_gc_task`);
    /// the counters themselves carry no timestamps, so this is
    /// deterministic-test-friendly without a `TimeSource` — tests call
    /// [`Self::drain_and_log`] directly.
    pub(crate) fn start_summary_task(self: std::sync::Arc<Self>) -> tokio::task::JoinHandle<()> {
        GlobalExecutor::spawn(async move {
            // No startup jitter on purpose. The sibling periodic tasks draw
            // `GlobalRng` for jitter to de-correlate *network* side-effects
            // across peers, but this task only writes to the local log, so
            // de-correlation buys nothing — and an extra `GlobalRng` draw at
            // node startup would perturb the deterministic simulation RNG
            // stream and flip seed-sensitive routing-coverage sim tests
            // (e.g. `test_get_routing_coverage_low_htl`). Keeping the task
            // RNG-free preserves simulation determinism.
            let mut ticker = tokio::time::interval(SUMMARY_WINDOW);
            // The first tick fires immediately; skip it so the first emit
            // describes a full window rather than an empty startup instant.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                self.drain_and_log();
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a deterministic, distinct `ContractInstanceId` from a seed byte.
    fn cid(seed: u8) -> ContractInstanceId {
        ContractInstanceId::new([seed; 32])
    }

    #[test]
    fn aggregate_window_empty_returns_none() {
        assert!(aggregate_window(vec![], TOP_K).is_none());
    }

    #[test]
    fn aggregate_window_ignores_zero_update_contracts() {
        // A tracked contract with no update attempts this window contributes
        // nothing and must not inflate the contract count.
        let summaries = vec![ContractSummary {
            contract: cid(1),
            updates: 0,
            targets_total: 0,
            no_targets: 0,
            interest_resolve_failed: 5,
        }];
        assert!(aggregate_window(summaries, TOP_K).is_none());
    }

    #[test]
    fn aggregate_window_totals_and_average() {
        let summaries = vec![
            ContractSummary {
                contract: cid(1),
                updates: 2,
                targets_total: 10, // avg 5
                no_targets: 0,
                interest_resolve_failed: 1,
            },
            ContractSummary {
                contract: cid(2),
                updates: 3,
                targets_total: 0, // all no-target attempts
                no_targets: 3,
                interest_resolve_failed: 4,
            },
        ];
        let w = aggregate_window(summaries, TOP_K).expect("non-empty");
        assert_eq!(w.contracts, 2);
        assert_eq!(w.updates, 5);
        assert_eq!(w.targets_total, 10);
        assert_eq!(w.no_targets, 3);
        assert_eq!(w.interest_resolve_failed, 5);
        // Window-wide average: 10 targets / 5 updates = 2.0
        assert_eq!(w.targets_avg(), 2.0);
        assert_eq!(w.others, 0);
        // Most active contract (cid(2), 3 updates) sorts first.
        assert_eq!(w.top[0].contract, cid(2));
        assert_eq!(w.top[0].targets_avg(), 0.0);
        assert_eq!(w.top[1].contract, cid(1));
        assert_eq!(w.top[1].targets_avg(), 5.0);
    }

    #[test]
    fn aggregate_window_caps_top_k_and_counts_others() {
        // 5 active contracts, top_k = 2 ⇒ 2 detail lines + others = 3.
        let summaries: Vec<ContractSummary> = (0..5u8)
            .map(|i| ContractSummary {
                contract: cid(i),
                updates: u64::from(i) + 1, // 1,2,3,4,5
                targets_total: 0,
                no_targets: 0,
                interest_resolve_failed: 0,
            })
            .collect();
        let w = aggregate_window(summaries, 2).expect("non-empty");
        assert_eq!(w.contracts, 5);
        assert_eq!(w.top.len(), 2);
        assert_eq!(w.others, 3);
        // Highest update counts first: cid(4)=5 updates, cid(3)=4 updates.
        assert_eq!(w.top[0].contract, cid(4));
        assert_eq!(w.top[1].contract, cid(3));
    }

    #[test]
    fn aggregate_window_tie_break_is_deterministic_by_contract_id() {
        // Equal update counts ⇒ tie broken by contract id ascending, so the
        // top-K is stable across map iteration orders.
        let summaries = vec![
            ContractSummary {
                contract: cid(9),
                updates: 1,
                targets_total: 0,
                no_targets: 0,
                interest_resolve_failed: 0,
            },
            ContractSummary {
                contract: cid(3),
                updates: 1,
                targets_total: 0,
                no_targets: 0,
                interest_resolve_failed: 0,
            },
        ];
        let w = aggregate_window(summaries, 1).expect("non-empty");
        assert_eq!(w.top.len(), 1);
        assert_eq!(w.others, 1);
        // Lower id wins the tie.
        assert_eq!(w.top[0].contract, cid(3));
    }

    #[test]
    fn record_and_drain_accumulates_then_resets() {
        let stats = UpdatePropagationStats::new();
        let c = cid(7);

        // Two fan-outs: one with 3 targets, one with 0 (NO_TARGETS) + 2
        // interest-resolve failures.
        stats.record_broadcast(c, 3, 0);
        stats.record_broadcast(c, 0, 2);

        let drained = stats.drain_window();
        assert_eq!(drained.len(), 1);
        let s = &drained[0];
        assert_eq!(s.contract, c);
        assert_eq!(s.updates, 2);
        assert_eq!(s.targets_total, 3);
        assert_eq!(s.no_targets, 1);
        assert_eq!(s.interest_resolve_failed, 2);

        // The drained entry is removed in the same drain (not retained for a
        // window) so the next window starts with full cap headroom.
        assert_eq!(stats.contracts.len(), 0);

        // A second drain with no new activity yields nothing.
        let drained2 = stats.drain_window();
        assert!(drained2.is_empty());
        assert_eq!(stats.contracts.len(), 0);
    }

    #[test]
    fn drain_restores_cap_headroom_for_next_window() {
        // Codex P2 regression: a window that fills the cap with one-off
        // contracts must NOT block new contracts in the following window.
        let stats = UpdatePropagationStats::new();
        let id_at = |i: usize| {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(i as u64).to_le_bytes());
            ContractInstanceId::new(bytes)
        };

        // Window 1: fill to capacity, then drain.
        for i in 0..MAX_TRACKED_CONTRACTS {
            stats.record_broadcast(id_at(i), 1, 0);
        }
        assert_eq!(stats.contracts.len(), MAX_TRACKED_CONTRACTS);
        let drained = stats.drain_window();
        assert_eq!(drained.len(), MAX_TRACKED_CONTRACTS);
        assert_eq!(
            stats.contracts.len(),
            0,
            "drain must clear the map so the cap doesn't stay saturated"
        );

        // Window 2: a fresh set of contracts is accepted (not dropped).
        let fresh = id_at(MAX_TRACKED_CONTRACTS + 1);
        stats.record_broadcast(fresh, 2, 0);
        assert!(
            stats.contracts.get(&fresh).is_some(),
            "new contract must be tracked in the window after a full drain"
        );
        let drained2 = stats.drain_window();
        assert_eq!(drained2.len(), 1);
        assert_eq!(drained2[0].contract, fresh);
        assert_eq!(drained2[0].targets_total, 2);
    }

    #[test]
    fn record_broadcast_respects_tracking_cap() {
        let stats = UpdatePropagationStats::new();
        // Fill to capacity with distinct contracts.
        for i in 0..MAX_TRACKED_CONTRACTS {
            // Spread the seed across all 32 bytes so ids stay distinct past 255.
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(i as u64).to_le_bytes());
            stats.record_broadcast(ContractInstanceId::new(bytes), 1, 0);
        }
        assert_eq!(stats.contracts.len(), MAX_TRACKED_CONTRACTS);

        // A brand-new contract is dropped (uncounted) at capacity.
        let overflow = {
            let mut bytes = [0u8; 32];
            bytes[0] = 0xFF;
            bytes[8] = 0xFF; // guaranteed distinct from the seeded range
            ContractInstanceId::new(bytes)
        };
        stats.record_broadcast(overflow, 1, 0);
        assert_eq!(stats.contracts.len(), MAX_TRACKED_CONTRACTS);
        assert!(stats.contracts.get(&overflow).is_none());
    }

    #[test]
    fn concurrent_record_and_drain_conserve_totals_without_field_split() {
        // Codex P2 regression: a record and a drain of the same contract must
        // be atomic as a unit. With independent per-field atomics a drain
        // could capture `updates` but miss `targets_total` (or vice-versa),
        // splitting one record across windows and dropping data. Here many
        // threads hammer record_broadcast on a single contract while another
        // thread drains repeatedly; afterwards the grand totals across all
        // drains must exactly equal what was recorded, and no drained summary
        // may ever show targets/interest activity with `updates == 0` (the
        // tell-tale of a split record).
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        const THREADS: usize = 8;
        const PER_THREAD: u64 = 2_000;
        const TARGETS_EACH: usize = 2;
        const IRF_EACH: usize = 1;

        let stats = Arc::new(UpdatePropagationStats::new());
        let done = Arc::new(AtomicBool::new(false));
        let c = cid(123);

        let acc = std::thread::scope(|scope| {
            // Concurrent drainer: accumulates across windows and asserts no split.
            let drainer = {
                let stats = Arc::clone(&stats);
                let done = Arc::clone(&done);
                scope.spawn(move || {
                    let mut acc = (0u64, 0u64, 0u64, 0u64);
                    let mut drain_once = || {
                        for s in stats.drain_window() {
                            assert!(
                                s.updates > 0,
                                "field-split: drained summary has updates==0 with \
                                 non-zero other fields: {s:?}"
                            );
                            acc.0 += s.updates;
                            acc.1 += s.targets_total;
                            acc.2 += s.no_targets;
                            acc.3 += s.interest_resolve_failed;
                        }
                    };
                    while !done.load(AtomicOrdering::Relaxed) {
                        drain_once();
                        std::thread::yield_now();
                    }
                    // Final drain captures anything recorded after the last loop.
                    drain_once();
                    acc
                })
            };

            // Recorders.
            let mut recorders = Vec::new();
            for _ in 0..THREADS {
                let stats = Arc::clone(&stats);
                recorders.push(scope.spawn(move || {
                    for _ in 0..PER_THREAD {
                        stats.record_broadcast(c, TARGETS_EACH, IRF_EACH);
                    }
                }));
            }
            for r in recorders {
                r.join().unwrap();
            }
            done.store(true, AtomicOrdering::Relaxed);
            drainer.join().unwrap()
        });

        let expected = THREADS as u64 * PER_THREAD;
        assert_eq!(
            acc.0, expected,
            "every record must be counted exactly once across all windows"
        );
        assert_eq!(acc.1, expected * TARGETS_EACH as u64);
        assert_eq!(acc.2, 0);
        assert_eq!(acc.3, expected * IRF_EACH as u64);
    }

    #[test]
    fn drain_and_log_emits_info_summary_and_is_silent_when_idle() {
        use crate::test_utils::TestLogger;

        let logger = TestLogger::new().capture_logs().with_level("info").init();

        // Idle window: no records ⇒ no output at all (budget rule).
        let stats = UpdatePropagationStats::new();
        stats.drain_and_log();
        assert!(
            !logger.contains("update_propagation_summary"),
            "an idle window must emit nothing, got: {:?}",
            logger.logs()
        );

        // One contract, two fan-outs (5 + 3 targets) ⇒ avg 4.0, no NO_TARGETS.
        stats.record_broadcast(cid(1), 5, 0);
        stats.record_broadcast(cid(1), 3, 1);
        stats.drain_and_log();

        assert!(
            logger.contains("update_propagation_summary"),
            "active window must emit the summary, got: {:?}",
            logger.logs()
        );
        assert!(
            logger.contains("INFO"),
            "summary must be INFO-level, got: {:?}",
            logger.logs()
        );

        // Draining again with no new activity is silent and clears state.
        let before = logger.logs().len();
        stats.drain_and_log();
        let after_logs = logger.logs();
        assert!(
            after_logs.len() == before
                || !after_logs[before..]
                    .iter()
                    .any(|l| l.contains("update_propagation_summary")),
            "a post-drain idle window must not re-emit a summary, got: {:?}",
            &after_logs[before..]
        );
        assert_eq!(stats.contracts.len(), 0, "idle entries should be evicted");
    }

    /// Source-level pin: the summary emitter MUST stay at INFO. The whole
    /// point of #4281 is to restore an INFO liveness signal lost to #4272;
    /// a future refactor that demotes this to DEBUG silently re-opens the
    /// visibility gap. Anchored on the structured event name.
    #[test]
    fn summary_emitter_logs_at_info_pin_test() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/operations/update/propagation_stats.rs");
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("must read own source at {}: {e}", path.display()));
        // The drain_and_log emitter is the only `phase = "summary"` site.
        let needle = "phase = \"summary\",";
        let idx = source
            .find(needle)
            .expect("summary emitter must still exist in source");
        let preceding = &source[..idx];
        let macro_idx = preceding
            .rfind("tracing::")
            .expect("a tracing macro must precede the summary emitter");
        let after_macro = &preceding[macro_idx + "tracing::".len()..];
        let macro_name = after_macro.split('!').next().unwrap_or("");
        assert_eq!(
            macro_name, "info",
            "update_propagation_summary emitter must stay at INFO — it is the \
             liveness signal #4281 restored after the #4272 DEBUG demotions. \
             Closest preceding macro is `tracing::{macro_name}!`."
        );
    }

    /// Source-scrape pin for the broadcast-path wiring. The counters are only
    /// useful if the fan-out handler actually feeds them; a future task-per-tx
    /// migration or refactor of `handle_broadcast_state_change` could silently
    /// drop the `record_broadcast` calls (the exact "manually-mirrored
    /// telemetry counter rots after a migration" failure class called out in
    /// `.claude/rules/bug-prevention-patterns.md`).
    ///
    /// The handler records each fresh logical broadcast's outcome exactly
    /// once: a NO_TARGETS record on the fresh-broadcast path (gated on
    /// `!is_retry`, so retry re-emissions never record a miss), and a success
    /// record on the targets-found path. That's two call sites; this pins both
    /// so neither outcome arm is dropped, and pins the `!is_retry` gate so a
    /// refactor can't start counting retry re-emissions as fresh misses.
    #[test]
    fn broadcast_path_feeds_propagation_stats_pin_test() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/node/network_bridge/p2p_protoc.rs");
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("must read p2p_protoc.rs at {}: {e}", path.display()));

        let call_count = source
            .matches("update_propagation_stats.record_broadcast(")
            .count();
        assert_eq!(
            call_count, 2,
            "handle_broadcast_state_change must call \
             `update_propagation_stats.record_broadcast(...)` exactly twice — once \
             on the fresh no-target path and once on the targets-found success \
             path. Found {call_count}. A different count means an outcome arm was \
             dropped (silent telemetry rot) or recording leaked elsewhere."
        );

        // The no-target record must be gated on `!is_retry` so retry
        // re-emissions don't get counted as fresh misses (Codex P2).
        assert!(
            source.contains("if !is_retry {"),
            "the fresh-no-target record must be gated on `!is_retry` so retry \
             re-emissions are not counted as fresh broadcasts."
        );
    }
}
