//! Per-contract merge budget on the network receive path (#6190).
//!
//! ## What this does
//!
//! Caps how many network-originated UPDATE messages (`RequestUpdate*` and
//! `BroadcastTo*`) this node admits toward a merge for ONE contract per unit
//! time, summed over every sender. It is a token bucket keyed by contract id
//! ([`MERGE_BUDGET_BURST`] tokens, refilling one per
//! [`MERGE_BUDGET_REFILL_INTERVAL`]). A message that finds the bucket empty is
//! dropped before any driver runs. A dropped broadcast gets the same throttled
//! `ResyncRequest` repair as a per-pair rate-limit drop (#5510), so a sender's
//! cached summary of us is corrected rather than left wrong until
//! anti-entropy.
//!
//! The check runs in `node.rs` right after the per-(sender, contract)
//! [`crate::ring::update_rate_limit::UpdateRateLimiter`], and only for a
//! contract this node holds (`Ring::should_summarize_or_broadcast`). A node
//! that does not hold the contract forwards the message and merges nothing,
//! so there is nothing to budget, and the key space stays bounded by the
//! hosted set instead of by what peers choose to send.
//!
//! ## Why it exists
//!
//! A contract whose merge never settles (every apply yields new bytes) turns
//! every broadcast into another state change, which broadcasts again. The
//! existing bounds are all per sender or per payload: the per-pair interval
//! still admits ~50 broadcasts/s from EACH neighbour, the applied-payload
//! record skips only byte-identical re-pushes, and the 5 s WASM limit bounds
//! one merge, not their rate. Measured in simulation (12 nodes, worst-case
//! non-settling merge, gates off): ~74 merges per node per virtual second for
//! the one contract and 685x the event count of the gated run. The budget
//! bounds that per node by construction, for every contract alike, by usage
//! alone.
//!
//! It replaces the `ring::broken_invariants` gates, which froze a flagged
//! contract on a sampled correctness verdict (commit, broadcast and egress
//! all suppressed, including the node's own client UPDATEs, which were
//! answered `state_changed: false` and discarded). That flag is now
//! diagnostic only.
//!
//! ## What it does NOT touch
//!
//! - **Local client UPDATEs.** They never arrive on `NetMessageV1::Update`
//!   with a source address, so they are never charged and never refused here.
//! - **Anti-entropy and resync repairs** (`InterestSync`, `ResyncResponse`).
//!   Those ride a different wire variant and are already bounded by their own
//!   limiters (`ring::resync_rate_limit`) and the heartbeat cadence.
//!
//! ## Known trade-off
//!
//! The bucket is shared by all senders of a contract, so one peer pushing a
//! contract at its full per-pair rate can use up this node's budget for it
//! and delay honest broadcasts of that contract on THIS node. The delay is
//! bounded: each dropped honest broadcast is repaired by a throttled
//! `ResyncRequest` (one per (contract, sender) per 30 s) and by anti-entropy.
//! Before this budget the same peer forced a WASM merge per message instead.
//!
//! ## Bounded growth
//!
//! Keys are contracts this node holds; the map is additionally capped at
//! [`MAX_TRACKED_CONTRACTS`]. A contract that cannot be tracked because the
//! map is full is admitted unmetered (counted in
//! [`ContractMergeBudget::untracked_total`]): the key set is not
//! attacker-chosen, so a full map is a sizing accident, and refusing on it
//! would throttle a contract for others' traffic.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use freenet_stdlib::prelude::ContractInstanceId;
use parking_lot::Mutex;
use tokio::time::Instant;

use super::resync_rate_limit::{BucketOutcome, TokenBucketLimiter};
use crate::util::time_source::TimeSource;

/// Steady-state admission rate per contract: one message per interval (20/s).
///
/// Calibration (#6190), from production telemetry on 2026-11-15 (20.7 h,
/// 1.19M sampled `update_broadcast_received` events over 44k (node, contract)
/// pairs, flagged contracts excluded; 5% sampling scaled x20, and per-node
/// export caps make these lower bounds). Per (node, contract), true received
/// UPDATEs: p99.9 about 8/s over 10 s windows and 3.3/s over 60 s; the worst
/// pair seen about 26/s over 10 s and 10.7/s over 60 s. 20/s is about 2x the
/// worst sustained window and 6x p99.9.
///
/// In simulation (12 nodes, 60 virtual s, two contracts whose merge never
/// settles) the loop ran at ~34 merges per contract per node per second
/// unbounded, ~17.7 at this rate and ~9 at 10/s: admissions track the rate
/// regardless of neighbour count. The rate is the cost a broken contract may
/// impose, so it is set as low as healthy contracts allow, not lower: a
/// healthy contract over it loses nothing (dropped broadcasts are repaired)
/// but is delayed.
pub(crate) const MERGE_BUDGET_REFILL_INTERVAL: Duration = Duration::from_millis(50);

/// Burst a contract may spend above the steady rate: 30 s at the steady rate,
/// which covers the worst healthy 10 s window measured (about 260 messages).
pub(crate) const MERGE_BUDGET_BURST: f64 = 600.0;

/// Upper bound on tracked contracts.
pub(crate) const MAX_TRACKED_CONTRACTS: usize = 16_384;

/// Minimum spacing of the release-visible drop log line.
const DROP_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Test hook: per contract, summed over every node in the process,
/// `(admitted, dropped)`.
#[cfg(any(test, feature = "testing"))]
pub static SIM_COUNTS: Mutex<Option<std::collections::HashMap<ContractInstanceId, (u64, u64)>>> =
    Mutex::new(None);

#[cfg(any(test, feature = "testing"))]
fn sim_count(contract: ContractInstanceId, admitted: bool) {
    let mut guard = SIM_COUNTS.lock();
    let entry = guard
        .get_or_insert_with(Default::default)
        .entry(contract)
        .or_insert((0, 0));
    if admitted {
        entry.0 += 1;
    } else {
        entry.1 += 1;
    }
}

/// Outcome of [`ContractMergeBudget::check_and_record`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeBudgetDecision {
    /// A token was spent; admit the message.
    Allowed,
    /// The map had no room to track this contract; admitted without a check.
    Untracked,
    /// The contract is over its budget on this node; drop the message.
    OverBudget,
}

impl MergeBudgetDecision {
    pub(crate) fn is_allowed(self) -> bool {
        !matches!(self, MergeBudgetDecision::OverBudget)
    }
}

/// Per-contract token bucket over network UPDATE admissions. See the module
/// docs.
pub(crate) struct ContractMergeBudget {
    buckets: TokenBucketLimiter<ContractInstanceId>,
    time_source: Arc<dyn TimeSource + Send + Sync>,
    dropped_total: AtomicU64,
    untracked_total: AtomicU64,
    /// Drops since the last log line, and when that line was written.
    log_state: Mutex<(u64, Option<Instant>)>,
}

impl ContractMergeBudget {
    pub(crate) fn new(time_source: Arc<dyn TimeSource + Send + Sync>) -> Self {
        Self::with_config(
            time_source,
            MERGE_BUDGET_BURST,
            MERGE_BUDGET_REFILL_INTERVAL,
            MAX_TRACKED_CONTRACTS,
        )
    }

    pub(crate) fn with_config(
        time_source: Arc<dyn TimeSource + Send + Sync>,
        burst: f64,
        refill_interval: Duration,
        max_tracked: usize,
    ) -> Self {
        Self {
            buckets: TokenBucketLimiter::new(
                time_source.clone(),
                burst,
                refill_interval,
                max_tracked,
            ),
            time_source,
            dropped_total: AtomicU64::new(0),
            untracked_total: AtomicU64::new(0),
            log_state: Mutex::new((0, None)),
        }
    }

    /// Charge one admission to `contract`.
    pub(crate) fn check_and_record(&self, contract: ContractInstanceId) -> MergeBudgetDecision {
        match self.buckets.check_and_record_detailed(contract) {
            BucketOutcome::Allowed => {
                #[cfg(any(test, feature = "testing"))]
                sim_count(contract, true);
                MergeBudgetDecision::Allowed
            }
            BucketOutcome::Untracked => {
                self.untracked_total.fetch_add(1, Ordering::Relaxed);
                MergeBudgetDecision::Untracked
            }
            BucketOutcome::RateLimited => {
                #[cfg(any(test, feature = "testing"))]
                sim_count(contract, false);
                let total = self.dropped_total.fetch_add(1, Ordering::Relaxed) + 1;
                self.log_drop(contract, total);
                MergeBudgetDecision::OverBudget
            }
        }
    }

    /// One `info!` line per [`DROP_LOG_INTERVAL`], naming the contract that
    /// tripped it and how many drops the interval held. Per-drop logging would
    /// be the flood it reports; `debug!` is compiled out of release builds.
    fn log_drop(&self, contract: ContractInstanceId, total: u64) {
        let now = self.time_source.now();
        let mut state = self.log_state.lock();
        state.0 += 1;
        let due = state
            .1
            .is_none_or(|last| now.saturating_duration_since(last) >= DROP_LOG_INTERVAL);
        if due {
            tracing::info!(
                %contract,
                drops_since_last_log = state.0,
                drops_total = total,
                event = "contract_merge_budget_exceeded",
                "Network UPDATEs for a contract dropped: over this node's per-contract \
                 merge budget (see ring::contract_merge_budget)"
            );
            *state = (0, Some(now));
        }
    }

    /// Reclaim idle, fully refilled entries. Called from the ring reaper.
    pub(crate) fn cleanup(&self) {
        self.buckets.cleanup();
    }

    /// Messages dropped for being over budget since start.
    pub(crate) fn dropped_total(&self) -> u64 {
        self.dropped_total.load(Ordering::Relaxed)
    }

    /// Messages admitted unmetered because the map was full. Should be zero.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn untracked_total(&self) -> u64 {
        self.untracked_total.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;

    fn id(b: u8) -> ContractInstanceId {
        ContractInstanceId::new([b; 32])
    }

    fn budget(
        burst: f64,
        interval: Duration,
        cap: usize,
    ) -> (ContractMergeBudget, SharedMockTimeSource) {
        let clock = SharedMockTimeSource::new();
        let b = ContractMergeBudget::with_config(Arc::new(clock.clone()), burst, interval, cap);
        (b, clock)
    }

    #[test]
    fn burst_then_steady_rate_then_drop() {
        let (b, clock) = budget(3.0, Duration::from_millis(100), 16);
        for _ in 0..3 {
            assert_eq!(b.check_and_record(id(1)), MergeBudgetDecision::Allowed);
        }
        assert_eq!(b.check_and_record(id(1)), MergeBudgetDecision::OverBudget);
        assert_eq!(b.dropped_total(), 1);

        clock.advance_time(Duration::from_millis(100));
        assert_eq!(b.check_and_record(id(1)), MergeBudgetDecision::Allowed);
        assert_eq!(b.check_and_record(id(1)), MergeBudgetDecision::OverBudget);
        assert_eq!(b.dropped_total(), 2);
    }

    /// The budget is per contract: one contract over budget does not touch
    /// another's.
    #[test]
    fn contracts_are_independent() {
        let (b, _clock) = budget(1.0, Duration::from_secs(1), 16);
        assert!(b.check_and_record(id(1)).is_allowed());
        assert!(!b.check_and_record(id(1)).is_allowed());
        assert!(b.check_and_record(id(2)).is_allowed());
    }

    /// Admissions over a long window are bounded by burst + elapsed / interval,
    /// however hard the contract is pushed. This is the property that bounds a
    /// non-settling merge loop.
    #[test]
    fn sustained_admissions_are_bounded_by_rate() {
        let (b, clock) = budget(10.0, Duration::from_millis(100), 16);
        let mut admitted = 0u64;
        // 60 virtual seconds, 100 attempts per virtual second.
        for _ in 0..6000 {
            if b.check_and_record(id(9)).is_allowed() {
                admitted += 1;
            }
            clock.advance_time(Duration::from_millis(10));
        }
        // 10 burst + 600 refills, give or take one for boundary rounding.
        assert!((609..=611).contains(&admitted), "admitted {admitted}");
        assert_eq!(b.dropped_total(), 6000 - admitted);
    }

    /// A full map admits unmetered rather than refusing: the keys are held
    /// contracts, not attacker-chosen, so a full map says nothing about the
    /// contract being checked.
    #[test]
    fn full_map_admits_unmetered() {
        let (b, _clock) = budget(1.0, Duration::from_secs(1), 1);
        assert_eq!(b.check_and_record(id(1)), MergeBudgetDecision::Allowed);
        assert_eq!(b.check_and_record(id(2)), MergeBudgetDecision::Untracked);
        assert!(b.check_and_record(id(2)).is_allowed());
        assert_eq!(b.untracked_total(), 2);
        assert_eq!(b.dropped_total(), 0);
    }
}
