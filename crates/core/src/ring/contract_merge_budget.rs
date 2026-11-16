//! Per-contract merge budget on the network receive path (#6190).
//!
//! ## What this does
//!
//! Bounds how many state-changing merges of co-host broadcasts (`BroadcastTo*`)
//! this node performs for ONE contract per unit time, summed over every
//! sender. It is a token bucket keyed by contract id ([`MERGE_BUDGET_BURST`]
//! tokens, refilling one per [`MERGE_BUDGET_REFILL_INTERVAL`]) used in two
//! steps:
//!
//! 1. **Check** ([`ContractMergeBudget::admit`]) in `node.rs`, right after the
//!    per-(sender, contract) [`crate::ring::update_rate_limit::UpdateRateLimiter`].
//!    An empty bucket drops the broadcast before any driver runs; for a
//!    contract this node holds (`Ring::should_summarize_or_broadcast`) the
//!    drop gets the same throttled `ResyncRequest` repair as a per-pair drop
//!    (#5510). The check spends nothing and never creates an entry, so a
//!    contract this node never merges is always admitted and an
//!    attacker-chosen key costs nothing.
//! 2. **Charge** ([`ContractMergeBudget::charge_changed`]) in the two broadcast
//!    drivers (`op_ctx_task.rs`), only when the merge actually changed the
//!    stored state.
//!
//! Charging only state-changing merges is what makes the shared bucket safe
//! against a single sender. Duplicates (skipped by the broadcast dedup cache
//! or the applied-payload record), invalid deltas and merges that change
//! nothing cost no tokens, so a neighbour cannot empty a contract's bucket on
//! this node by sending junk at its per-pair rate. To spend tokens it must
//! deliver valid updates that change the state, which is exactly what a
//! merge that never settles produces.
//!
//! ## Why it exists
//!
//! A contract whose merge never settles (every apply yields new bytes) turns
//! every broadcast into another state change, which broadcasts again. The
//! existing bounds are all per sender or per payload: the per-pair interval
//! admits ~50 broadcasts/s from EACH neighbour, the applied-payload record
//! skips only byte-identical re-pushes, and the 5 s WASM limit bounds one
//! merge, not their rate. In simulation (12 nodes, worst-case non-settling
//! merge, gates off) the loop made 73x the merges and 685x the events of the
//! gated run. The budget bounds it per node by construction, for every
//! contract alike, by usage alone.
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
//!   with a source address, so they are never checked or charged.
//! - **Routed client writes** (`RequestUpdate*`). UPDATE is fire-and-forget:
//!   the originator has already told its client the write succeeded, and a
//!   dropped request has no repair. They are bounded per neighbour by the
//!   per-pair request interval and are not what drives a merge loop.
//! - **PUT merges, anti-entropy and resync repairs** (`InterestSync`,
//!   `ResyncResponse`). PUTs are their own operation; the repair paths ride a
//!   different wire variant and are bounded by their own limiters
//!   (`ring::resync_rate_limit`) and the heartbeat cadence. None sustains a
//!   loop by itself: each state change they cause is fanned out as broadcasts,
//!   which are budgeted at every receiver.
//!
//! ## Known costs and limits
//!
//! - **One permitted writer can crowd out the others.** A sender that can
//!   produce valid state changes (a member of an open-write contract) and
//!   floods them from its own node (local UPDATEs are unbudgeted there) makes
//!   each co-host spend its budget on that content and drop the excess,
//!   honest writers' broadcasts included, roughly in proportion to arrivals.
//!   Before this budget the same flood cost every co-host a merge per message
//!   instead. Telling senders apart is possible only at the first hop, so a
//!   per-sender share is not attempted here.
//! - **Over budget, duplicates are dropped too.** The check runs before the
//!   broadcast dedup, so an over-budget contract's duplicates are dropped
//!   and each tries a (throttled) repair.
//! - **It bounds merges, not their CPU.** A non-settling contract costs each
//!   holder up to the steady rate in merges plus their fan-out, indefinitely:
//!   it is throttled, never frozen. Each merge is bounded by the WASM time
//!   limit, the executor's fair queue keeps one contract from starving the
//!   others (`contract::fair_queue`, at most `MAX_QUEUED_PER_CONTRACT` queued
//!   per contract), and a zero-demand storm contract is shed by cost-pressure
//!   eviction (#4861). A broken contract that has subscribers is not.
//! - **Repair after a sustained over-budget period is slow.** Each drop asks
//!   for a `ResyncRequest`, but those pass a per-(contract, sender) throttle
//!   (one per 30 s) and a per-contract emit cap shared with the other repairs
//!   (burst 2, then one per 60 s), and nothing retries once drops stop. A
//!   healthy contract that stays over budget after its burst can therefore
//!   stay diverged on this node until anti-entropy. With changed-only
//!   charging this needs more than the steady rate of real state changes,
//!   sustained past the burst. Drops on a contract not in the
//!   broken-invariant set are the signal to watch for it.
//! - **Repairs count as delta-incompatibility evidence at the sender.** As
//!   for the per-pair drop (see the `Rejected` arm in `node.rs`), a
//!   `ResyncRequest` right after a delta reads at the sender as a failed
//!   apply, and enough of them from two or more peers switch that sender to
//!   full-state fan-out for that contract for 10 minutes. Telling the two
//!   apart needs a reason on the wire, which is out of scope.
//! - **Charge follows check, not atomically.** Messages admitted while one
//!   token is left all merge and are all charged; the bucket may go negative
//!   (floored at minus the burst), which delays later admissions, so the
//!   long-run rate still holds. Queued merges per contract are bounded by the
//!   executor queue, which a const assertion in `contract::fair_queue` keeps
//!   well under the burst; streamed broadcasts still assembling are not, so
//!   in a large burst of those the floor can forgive some debt.
//!
//! ## Bounded growth
//!
//! Keys are contracts this node holds; the map is additionally capped at
//! [`MAX_TRACKED_CONTRACTS`]. A contract that cannot be tracked because the
//! map is full is admitted unmetered (counted in
//! [`ContractMergeBudget::untracked_total`] and exported): the key set is not
//! attacker-chosen, so a full map is a sizing accident, and refusing on it
//! would throttle a contract for others' traffic. Idle, refilled entries are
//! reclaimed by [`ContractMergeBudget::cleanup`].

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;
use parking_lot::Mutex;
use tokio::time::Instant;

use crate::util::time_source::TimeSource;

/// Steady-state rate per contract: one state-changing broadcast merge per
/// interval (20/s).
///
/// Calibration (#6190), from production telemetry on 2026-11-15 (20.7 h,
/// 1.19M sampled `update_broadcast_received` events over 44k (node, contract)
/// pairs, flagged contracts excluded; 5% sampling scaled x20, and per-node
/// export caps make these lower bounds, most so on busy nodes). Per (node,
/// contract), received broadcasts: p99.9 about 8/s over 10 s windows and
/// 3.3/s over 60 s; the worst pair seen about 26/s over 10 s and 10.7/s over
/// 60 s. Those count every received broadcast, duplicates included; the
/// budget charges only the ones that change state, which is fewer. 20/s is
/// about 2x the worst sustained window and 6x p99.9.
///
/// In simulation (12 nodes, two contracts whose merge never settles) the loop
/// ran at roughly twice this rate per node unbounded and tracked the rate
/// when bounded, regardless of neighbour count. The rate is the cost a broken
/// contract may impose, so it is set as low as healthy contracts allow.
pub(crate) const MERGE_BUDGET_REFILL_INTERVAL: Duration = Duration::from_millis(50);

/// Burst a contract may spend above the steady rate: 30 s at the steady rate,
/// which covers the worst healthy 10 s window measured (about 260 messages).
pub(crate) const MERGE_BUDGET_BURST: f64 = 600.0;

/// Upper bound on tracked contracts.
pub(crate) const MAX_TRACKED_CONTRACTS: usize = 16_384;

/// Minimum spacing of the release-visible drop log line.
const DROP_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// A refilled entry idle this long is reclaimed.
const CLEANUP_AGE: Duration = Duration::from_secs(5 * 60);

/// Outcome of [`ContractMergeBudget::admit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeBudgetDecision {
    /// The bucket has a token; admit the broadcast.
    Allowed,
    /// The map had no room to track this contract; admitted without a check.
    Untracked,
    /// The contract is over its budget on this node; drop the broadcast.
    OverBudget,
}

impl MergeBudgetDecision {
    pub(crate) fn is_allowed(self) -> bool {
        !matches!(self, MergeBudgetDecision::OverBudget)
    }
}

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

impl Bucket {
    fn refill(&mut self, now: Instant, capacity: f64, interval: Duration) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        self.tokens = (self.tokens + elapsed.as_secs_f64() / interval.as_secs_f64()).min(capacity);
        self.last_refill = now;
    }
}

/// Per-contract token bucket over state-changing broadcast merges. See the
/// module docs.
pub(crate) struct ContractMergeBudget {
    buckets: DashMap<ContractInstanceId, Bucket>,
    capacity: f64,
    refill_interval: Duration,
    max_tracked: usize,
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
            buckets: DashMap::new(),
            capacity: burst,
            refill_interval,
            max_tracked,
            time_source,
            dropped_total: AtomicU64::new(0),
            untracked_total: AtomicU64::new(0),
            log_state: Mutex::new((0, None)),
        }
    }

    /// Decide whether a broadcast for `contract` may proceed to a merge.
    /// Spends nothing; see [`Self::charge_changed`].
    pub(crate) fn admit(&self, contract: ContractInstanceId) -> MergeBudgetDecision {
        let now = self.time_source.now();
        if let Some(mut bucket) = self.buckets.get_mut(&contract) {
            bucket.refill(now, self.capacity, self.refill_interval);
            if bucket.tokens >= 1.0 {
                drop(bucket);
                sim::record(contract, sim::Kind::Admitted);
                return MergeBudgetDecision::Allowed;
            }
            drop(bucket);
            sim::record(contract, sim::Kind::Dropped);
            let total = self.dropped_total.fetch_add(1, Ordering::Relaxed) + 1;
            self.log_drop(contract, total);
            return MergeBudgetDecision::OverBudget;
        }
        // Untracked contracts have a full bucket. Only the map size can refuse
        // to track one, and then it is admitted unmetered.
        if self.buckets.len() >= self.max_tracked {
            self.untracked_total.fetch_add(1, Ordering::Relaxed);
            return MergeBudgetDecision::Untracked;
        }
        sim::record(contract, sim::Kind::Admitted);
        MergeBudgetDecision::Allowed
    }

    /// Spend one token for a broadcast merge of `contract` that changed the
    /// stored state. Floored at minus the burst: overshoot from messages
    /// admitted on the last token is carried, not forgiven.
    pub(crate) fn charge_changed(&self, contract: ContractInstanceId) {
        let now = self.time_source.now();
        sim::record(contract, sim::Kind::Charged);
        if let Some(mut bucket) = self.buckets.get_mut(&contract) {
            bucket.refill(now, self.capacity, self.refill_interval);
            bucket.tokens = (bucket.tokens - 1.0).max(-self.capacity);
            return;
        }
        if self.buckets.len() >= self.max_tracked {
            return;
        }
        // `and_modify` so two first charges racing past the `get_mut` above
        // both count.
        let (capacity, interval) = (self.capacity, self.refill_interval);
        self.buckets
            .entry(contract)
            .and_modify(|bucket| {
                bucket.refill(now, capacity, interval);
                bucket.tokens = (bucket.tokens - 1.0).max(-capacity);
            })
            .or_insert(Bucket {
                tokens: capacity - 1.0,
                last_refill: now,
            });
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
                "Broadcasts for a contract dropped: over this node's per-contract \
                 merge budget (see ring::contract_merge_budget)"
            );
            *state = (0, Some(now));
        }
    }

    /// Reclaim entries that are fully refilled and idle past [`CLEANUP_AGE`].
    /// A reclaimed entry behaves exactly like an untracked one (full bucket),
    /// so this only bounds memory. Called from the ring reaper.
    pub(crate) fn cleanup(&self) {
        let now = self.time_source.now();
        let (capacity, interval) = (self.capacity, self.refill_interval);
        self.buckets.retain(|_, bucket| {
            let elapsed = now.saturating_duration_since(bucket.last_refill);
            let tokens_now = bucket.tokens + elapsed.as_secs_f64() / interval.as_secs_f64();
            !(tokens_now >= capacity && elapsed > CLEANUP_AGE)
        });
    }

    /// Broadcasts dropped for being over budget since start.
    pub(crate) fn dropped_total(&self) -> u64 {
        self.dropped_total.load(Ordering::Relaxed)
    }

    /// Broadcasts admitted unmetered because the map was full. Should be zero.
    /// Once the map is full, any peer can move this by sending broadcasts for
    /// keys that are not tracked, so read it as "the map is full", not as a
    /// count of real contracts.
    pub(crate) fn untracked_total(&self) -> u64 {
        self.untracked_total.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.buckets.len()
    }
}

/// Simulation counters (`dev_tool::sim_merge_budget`). Recording is off unless
/// a test arms it, so builds with the `testing` feature (fdev included) pay
/// one relaxed atomic load per call and keep no map.
#[cfg(any(test, feature = "testing"))]
pub(crate) mod sim {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    use freenet_stdlib::prelude::ContractInstanceId;
    use parking_lot::Mutex;

    pub(crate) enum Kind {
        Admitted,
        Dropped,
        Charged,
    }

    pub(crate) static ARMED: AtomicBool = AtomicBool::new(false);
    /// Per contract, summed over every node in the process:
    /// `(admitted, dropped, charged)`.
    /// `(admitted, dropped, charged)` per contract.
    pub(crate) type Counts = HashMap<ContractInstanceId, (u64, u64, u64)>;

    pub(crate) static COUNTS: Mutex<Option<Counts>> = Mutex::new(None);

    pub(crate) fn record(contract: ContractInstanceId, kind: Kind) {
        if !ARMED.load(Ordering::Relaxed) {
            return;
        }
        let mut guard = COUNTS.lock();
        let entry = guard
            .get_or_insert_with(Default::default)
            .entry(contract)
            .or_insert((0, 0, 0));
        match kind {
            Kind::Admitted => entry.0 += 1,
            Kind::Dropped => entry.1 += 1,
            Kind::Charged => entry.2 += 1,
        }
    }
}

#[cfg(not(any(test, feature = "testing")))]
mod sim {
    use freenet_stdlib::prelude::ContractInstanceId;

    pub(super) enum Kind {
        Admitted,
        Dropped,
        Charged,
    }

    #[inline(always)]
    pub(super) fn record(_contract: ContractInstanceId, _kind: Kind) {}
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

    /// Admit, then charge when the merge changed state: the usage the
    /// broadcast drivers produce for a merge that always changes state.
    fn admit_and_charge(b: &ContractMergeBudget, c: ContractInstanceId) -> bool {
        let ok = b.admit(c).is_allowed();
        if ok {
            b.charge_changed(c);
        }
        ok
    }

    #[test]
    fn burst_then_steady_rate_then_drop() {
        let (b, clock) = budget(3.0, Duration::from_millis(100), 16);
        for _ in 0..3 {
            assert!(admit_and_charge(&b, id(1)));
        }
        assert_eq!(b.admit(id(1)), MergeBudgetDecision::OverBudget);
        assert_eq!(b.dropped_total(), 1);

        clock.advance_time(Duration::from_millis(100));
        assert!(admit_and_charge(&b, id(1)));
        assert_eq!(b.admit(id(1)), MergeBudgetDecision::OverBudget);
        assert_eq!(b.dropped_total(), 2);
    }

    /// Admission alone spends nothing: duplicates, invalid deltas and merges
    /// that change nothing cannot empty a contract's bucket. This is what
    /// stops one sender from starving honest broadcasts of a contract.
    #[test]
    fn admissions_without_a_state_change_spend_nothing() {
        let (b, _clock) = budget(2.0, Duration::from_secs(60), 16);
        for _ in 0..10_000 {
            assert_eq!(b.admit(id(1)), MergeBudgetDecision::Allowed);
        }
        assert_eq!(b.dropped_total(), 0);
        assert!(admit_and_charge(&b, id(1)));
        assert!(admit_and_charge(&b, id(1)));
        assert!(!b.admit(id(1)).is_allowed(), "two changes spent the burst");
    }

    /// The budget is per contract: one contract over budget does not touch
    /// another's.
    #[test]
    fn contracts_are_independent() {
        let (b, _clock) = budget(1.0, Duration::from_secs(1), 16);
        assert!(admit_and_charge(&b, id(1)));
        assert!(!b.admit(id(1)).is_allowed());
        assert!(b.admit(id(2)).is_allowed());
    }

    /// State-changing merges over a long window are bounded by burst +
    /// elapsed / interval, however hard the contract is pushed. This is the
    /// property that bounds a non-settling merge loop.
    #[test]
    fn sustained_changes_are_bounded_by_rate() {
        let (b, clock) = budget(10.0, Duration::from_millis(100), 16);
        let mut changed = 0u64;
        // 60 virtual seconds, 100 attempts per virtual second.
        for _ in 0..6000 {
            if admit_and_charge(&b, id(9)) {
                changed += 1;
            }
            clock.advance_time(Duration::from_millis(10));
        }
        // 10 burst + 600 refills, give or take one for boundary rounding.
        assert!((609..=611).contains(&changed), "changed {changed}");
        assert_eq!(b.dropped_total(), 6000 - changed);
    }

    /// Overshoot from messages admitted on the last token is carried: the
    /// bucket goes negative and later admissions wait it out.
    #[test]
    fn overshoot_is_carried_not_forgiven() {
        let (b, clock) = budget(1.0, Duration::from_secs(1), 16);
        // Five broadcasts admitted on one token, all of which change state.
        for _ in 0..5 {
            assert!(b.admit(id(3)).is_allowed());
        }
        for _ in 0..5 {
            b.charge_changed(id(3));
        }
        // Floored at -1: two refills are needed to get back to one token.
        clock.advance_time(Duration::from_secs(1));
        assert!(!b.admit(id(3)).is_allowed());
        clock.advance_time(Duration::from_secs(1));
        assert!(b.admit(id(3)).is_allowed());
    }

    /// A full map admits unmetered rather than refusing: the keys are held
    /// contracts, not attacker-chosen, so a full map says nothing about the
    /// contract being checked.
    #[test]
    fn full_map_admits_unmetered() {
        let (b, _clock) = budget(1.0, Duration::from_secs(1), 1);
        assert!(admit_and_charge(&b, id(1)));
        assert_eq!(b.admit(id(2)), MergeBudgetDecision::Untracked);
        assert!(
            MergeBudgetDecision::Untracked.is_allowed(),
            "a full map fails open"
        );
        b.charge_changed(id(2));
        assert_eq!(b.admit(id(2)), MergeBudgetDecision::Untracked);
        assert_eq!(b.untracked_total(), 2);
        assert_eq!(b.dropped_total(), 0);
        assert_eq!(b.tracked(), 1);
    }

    /// Source pin: the budget is checked in exactly one place, the
    /// `NetMessageV1::Update` dispatch for a broadcast with a source address
    /// (so never for a local client UPDATE or a routed client write), and
    /// charged in exactly the two broadcast apply paths, each directly inside
    /// an `if <..>.changed {` block. A check or charge elsewhere would throttle
    /// traffic this budget must not touch; a charge outside the `changed`
    /// branch would let duplicates and no-op deltas empty a contract's bucket.
    #[test]
    fn budget_is_checked_on_network_broadcasts_and_charged_in_the_broadcast_drivers() {
        use crate::node::{collect_rs_files, strip_cfg_test_regions};

        let src_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        collect_rs_files(&src_root, &mut files);
        assert!(
            files.len() > 100,
            "the source walk found {} files",
            files.len()
        );

        let code = |src: &str| -> String {
            strip_cfg_test_regions(src)
                .lines()
                .map(|line| line.split_once("//").map_or(line, |(code, _)| code))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let squash = |src: &str| -> String { src.chars().filter(|c| !c.is_whitespace()).collect() };
        let mut admits = Vec::new();
        let mut charges = Vec::new();
        for path in &files {
            let rel = path
                .strip_prefix(&src_root)
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/");
            if rel.ends_with("tests.rs")
                || rel.contains("/tests/")
                || rel.contains("pool_tests/")
                || rel == "ring/contract_merge_budget.rs"
            {
                continue;
            }
            let Ok(src) = std::fs::read_to_string(path) else {
                continue;
            };
            let flat = squash(&code(&src));
            for _ in 0..flat.matches("contract_merge_budget.admit(").count() {
                admits.push(rel.clone());
            }
            for _ in 0..flat
                .matches("contract_merge_budget.charge_changed(")
                .count()
            {
                charges.push(rel.clone());
            }
        }
        assert_eq!(admits, vec!["node.rs".to_string()], "budget check sites");
        assert_eq!(
            charges,
            vec![
                "operations/update/op_ctx_task.rs".to_string(),
                "operations/update/op_ctx_task.rs".to_string()
            ],
            "budget charge sites"
        );

        // The check sits in the network UPDATE arm, after the source-address
        // match that local client UPDATEs never take, gated on the broadcast
        // class.
        let node = squash(&code(include_str!("../node.rs")));
        let arm = node
            .find("NetMessageV1::Update(refop)=>{")
            .expect("UPDATE dispatch arm");
        let source = arm
            + node[arm..]
                .find("ifletSome(sender_addr)=source_addr{")
                .expect("source-address match");
        let check = node
            .find("contract_merge_budget.admit(")
            .expect("budget check");
        assert!(
            arm < source && source < check,
            "check after the source match"
        );
        let gate = node[..check]
            .rfind("ifupdate_class==crate::ring::update_rate_limit::UpdateClass::Broadcast&&")
            .expect("broadcast-class gate");
        assert!(gate > source, "the check is gated on the broadcast class");
        // The whole condition, exactly: the class gate and the check, with no
        // other conjunct (a held check here would let a contract merged but
        // not held bypass the budget).
        assert_eq!(
            &node[gate..check],
            "ifupdate_class==crate::ring::update_rate_limit::UpdateClass::Broadcast&&!op_manager.ring.",
            "the budget check's condition"
        );

        // Each charge is directly inside an `if <..>.changed {` block of one
        // of the two broadcast apply paths: find the `{` that opens the
        // innermost block around the charge, and read the condition before it.
        let ops = squash(&code(include_str!("../operations/update/op_ctx_task.rs")));
        for driver in [
            "asyncfndrive_relay_broadcast_to(",
            "asyncfnapply_streaming_broadcast(",
        ] {
            let start = ops
                .find(driver)
                .unwrap_or_else(|| panic!("{driver} exists"));
            let end = start
                + 1
                + ops[start + 1..]
                    .find("asyncfn")
                    .unwrap_or(ops.len() - start - 1);
            let body = &ops[start..end];
            let at = body
                .find("contract_merge_budget.charge_changed(")
                .unwrap_or_else(|| panic!("{driver} charges"));
            let mut depth = 0i32;
            let mut open = None;
            for (i, ch) in body[..at].char_indices().rev() {
                match ch {
                    '}' => depth += 1,
                    '{' if depth == 0 => {
                        open = Some(i);
                        break;
                    }
                    '{' => depth -= 1,
                    _ => {}
                }
            }
            let open = open.expect("the charge is inside a block");
            // A whole-word `if`: preceded by a statement or block boundary.
            let cond_start = body[..open]
                .match_indices("if")
                .map(|(i, _)| i)
                .filter(|&i| i > 0 && matches!(body.as_bytes()[i - 1], b';' | b'{' | b'}'))
                .last()
                .expect("the block is an if");
            let cond = &body[cond_start + 2..open];
            assert!(
                cond.ends_with(".changed") && !cond.contains('!') && !cond.contains("else"),
                "{driver}: the charge must sit directly inside `if <..>.changed {{`, found `if {cond} {{`"
            );
        }
    }

    /// Cleanup reclaims only refilled, idle entries.
    #[test]
    fn cleanup_reclaims_only_refilled_idle_entries() {
        let (b, clock) = budget(10.0, Duration::from_secs(1), 16);
        assert!(admit_and_charge(&b, id(1)));
        b.cleanup();
        assert_eq!(b.tracked(), 1, "a fresh entry is kept");
        clock.advance_time(CLEANUP_AGE + Duration::from_secs(1));
        b.cleanup();
        assert_eq!(b.tracked(), 0, "refilled and idle: reclaimed");
    }
}
