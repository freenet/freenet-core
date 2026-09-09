//! The one place a delegate's scheduled wakeup is granted, recorded, fired or
//! cleared (freenet-core#3972).
//!
//! # What a wakeup is, and why it needs a resource model the other primitives did not
//!
//! Everything else a delegate does today is **externally triggered**: a client
//! asks, a contract notification arrives, a park resumes. Something outside the
//! delegate decides how often it runs, and every existing bound — the parked
//! count, the pending queue, `max_execution_seconds` — is written against that
//! assumption.
//!
//! A wakeup removes it. A delegate that re-arms inside its own `WakeupFired`
//! handler runs forever, on an idle node, with no user, no client and no
//! network event. `max_execution_seconds` bounds ONE invocation; nothing today
//! bounds the RATE, and until this module existed nothing even counted it. That
//! is #5597's argument, and it is why this file is mostly about admission.
//!
//! # The model: one broker, three units
//!
//! A wakeup is a **lease on future occupancy of the serial `contract_handling`
//! loop**. [`schedule`] is the only place a lease is granted, and it makes
//! three checks — each in a unit that is genuinely scarce, each bounded per
//! delegate AND node-wide with **the per-delegate bound strictly below the
//! node-wide one** (#5597 property 2, which the delegate pin cap violates: its
//! per-delegate figure is five times the node figure, so the first delegate to
//! ask takes the whole node allowance).
//!
//! **1. Bytes.** `tag` is capped at [`MAX_WAKEUP_TAG_BYTES`] and the delay must
//! lie in `[MIN_WAKEUP_DELAY, MAX_WAKEUP_DELAY]`. freenet-stdlib checks the tag
//! and clamps the delay in `DelegateCtx::schedule_wakeup`, but
//! `__frnt__delegate__schedule_wakeup` is an ordinary WASM import and a
//! delegate can declare its own `extern "C"` block: **a guest-side check on a
//! guest-declared import can never be a bound.** Both are re-checked here, and
//! this module is where they are actually enforced.
//!
//! The delay's UPPER bound is not decoration. A lease that can be held for an
//! unbounded time is the pin defect again (#5597 property 3); capping the
//! horizon is what makes "every lease is released by firing" a bounded promise
//! rather than an unbounded one.
//!
//! **2. Rows.** [`MAX_WAKEUPS_PER_DELEGATE`] outstanding leases per delegate,
//! [`MAX_WAKEUPS_PER_NODE`] node-wide. Leases are keyed `(delegate, tag)`, so
//! re-arming the same tag **replaces** rather than accumulates — a delegate
//! rotating a key weekly holds exactly one lease for its whole life.
//!
//! Reject-at-cap is the right shape *here*, and it is worth saying why, because
//! `.claude/rules/code-style.md` says reject-at-cap is WRONG for anything
//! ordinary use refreshes: a busy entry then holds its slot forever and every
//! newcomer is refused with no recovery path. A wakeup lease is not refreshable
//! by use — it is released when it FIRES, and it must fire within
//! [`MAX_WAKEUP_DELAY`]. So a newcomer's wait is bounded by construction and no
//! delegate can hold a slot by being busy. Reject-at-cap is defensible; LRU
//! eviction would silently cancel a wakeup a delegate was told it had.
//!
//! **3. Loop occupancy — and this is the one that bounds what wakeups newly
//! make possible.** The row cap above bounds SPACE and does not bound CPU at
//! all, because firing is precisely what re-arms the delegate to schedule
//! again: a delegate that reschedules inside its handler releases and
//! immediately retakes its lease, forever, never once exceeding the count. So
//! there is a second budget, metered in **microseconds of loop occupancy**,
//! debited by each wakeup-driven run's MEASURED duration and refilled as a
//! fraction of wall clock: [`DELEGATE_DUTY_DIVISOR`] per delegate,
//! [`NODE_DUTY_DIVISOR`] node-wide.
//!
//! Counting invocations instead would be a proxy, and a bad one — a 5 ms
//! handler and a 5 s handler would cost the same token. Debiting measured
//! duration bounds unprompted work to a stated share of the loop *whatever the
//! handler costs*, which is #5597 property 1 without waiting for fuel metering,
//! and it lets a cheap handler legitimately run far more often than an
//! expensive one.
//!
//! **The caveat, stated rather than buried:** wall clock keeps running while
//! the guest is inside a host call, where the epoch trap cannot fire (#5594).
//! This budget therefore inherits `max_execution_seconds`' mis-attribution — it
//! is the same defect, not a new one, and it is the reason to re-express this
//! in fuel when #5597's broker lands.
//!
//! Admission checks credit BEFORE a run and debits AFTER it, so a delegate can
//! overshoot its share by (outstanding leases x longest run). That overshoot is
//! bounded by check 2, and check 2's blindness to CPU is covered by check 3.
//! The two compose; neither is sufficient alone.
//!
//! # Refusal is a value, not a log line
//!
//! Every refusal above returns a DISTINCT [`WakeupRefusal`], which the host
//! function maps to a distinct negative code the delegate receives. That is
//! deliberate and it is the one advantage this primitive has over
//! `subscribe_contract`, which returns `bool` and collapses four different
//! refusals into a `false` a delegate cannot act on (#5565). "You are asking
//! too often" and "the node is full" call for opposite responses; a delegate
//! told neither cannot degrade, and one told `Ok(())` will simply never run and
//! never know.
//!
//! # Why the schedule is private to this module
//!
//! A wakeup has **two representations** that must never disagree: the in-memory
//! deadline index the `contract_handling` loop reads, and a durable row so a
//! week-long delay survives a restart. The stdlib states that persistence as an
//! obligation on the host precisely because the precedent runs the wrong way —
//! `DELEGATE_SUBSCRIPTIONS` is a `LazyLock<DashMap>` a restart discards.
//!
//! So [`SCHEDULE`] is private and every mutation goes through one of the
//! functions below, each of which writes both representations. This does NOT
//! make forgetting to CALL one a compile error — nothing can — but it does make
//! it impossible to reach the schedule *without* the durable half, which is the
//! failure this shape (from #5493's `delegate_subscriptions`) exists to remove:
//! a cleanup path that updated only the memory copy would leave a durable row
//! restored on every subsequent boot, firing forever into a delegate that no
//! longer exists.
//!
//! # The storage handle is a parameter, not a global
//!
//! Every caller passes its own store. The schedule is a process-global `static`
//! while a store belongs to one node, and several nodes share one process in
//! every `#[freenet_test]`; a global handle here would send one node's durable
//! writes to another node's database. `None` degrades to the in-memory schedule
//! alone — correct for the sqlite backend and the mock runtime, where a wakeup
//! survives the process and not a restart.

use std::collections::{BTreeMap, HashMap};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use freenet_stdlib::prelude::DelegateKey;

// ---------------------------------------------------------------------------
// Bounds.
// ---------------------------------------------------------------------------

/// Largest `tag` the host will accept, in bytes. Mirrors
/// `freenet_stdlib::delegate_host::MAX_WAKEUP_TAG_BYTES`.
///
/// Duplicated rather than imported because the two are not the same claim: the
/// stdlib constant documents what a well-behaved caller sends, this one is the
/// bound. A delegate built against a newer stdlib with a larger constant is
/// still held to this one, which is the entire point.
pub(crate) const MAX_WAKEUP_TAG_BYTES: usize = 128;

/// Shortest delay the host will accept.
///
/// The stdlib CLAMPS up to this value before calling; this module REFUSES.
/// The divergence is deliberate: clamping silently grants something other than
/// what was asked, which is the shape this primitive exists not to have. It is
/// only reachable by a delegate that bypasses `DelegateCtx::schedule_wakeup`,
/// since that wrapper never sends a shorter delay.
pub(crate) const MIN_WAKEUP_DELAY: Duration = Duration::from_secs(1);

/// Longest delay the host will accept: 30 days.
///
/// A lease nothing reclaims is the pin defect (#5597 property 3). This is what
/// makes "released by firing" a bounded statement — the horizon over which a
/// row can sit, and therefore the worst-case wait a newcomer faces at the row
/// cap. 30 days comfortably covers the driving consumer (River's weekly private
/// -room key rotation, freenet/river#228) with four times its period to spare.
pub(crate) const MAX_WAKEUP_DELAY: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Outstanding wakeup leases one delegate may hold.
///
/// Strictly below [`MAX_WAKEUPS_PER_NODE`] (by 64x), so no single delegate can
/// take the node's allowance. Leases are keyed by tag and replace on re-arm, so
/// 16 is 16 DISTINCT periodic jobs, not 16 ticks.
pub(crate) const MAX_WAKEUPS_PER_DELEGATE: usize = 16;

/// Outstanding wakeup leases the whole node may hold.
///
/// 1024 rows of (64-byte delegate key + <=128-byte tag + 8-byte deadline) is
/// under 250 KiB on disk and in memory — the space this cap actually bounds.
pub(crate) const MAX_WAKEUPS_PER_NODE: usize = 1024;

/// One delegate's share of the serial loop for unprompted work: 1%.
///
/// Expressed as a divisor of elapsed wall clock, so credit refills at
/// `elapsed / DELEGATE_DUTY_DIVISOR` microseconds.
pub(crate) const DELEGATE_DUTY_DIVISOR: u64 = 100;

/// The node's total share of the serial loop for unprompted work: 10%.
///
/// Ten times the per-delegate share, so property 2 holds here as well: the
/// per-principal bound is strictly below the node-wide one.
pub(crate) const NODE_DUTY_DIVISOR: u64 = 10;

/// Burst credit for one delegate: 5 s, i.e. exactly one `max_execution_seconds`
/// run.
///
/// A delegate starts able to afford one worst-case run and then refills at 1%.
/// Bursting further would let a first-run runaway spend more than the loop can
/// give back before the row cap notices.
pub(crate) const DELEGATE_DUTY_BURST_MICROS: u64 = 5_000_000;

/// Burst credit for the node: 30 s.
///
/// Six worst-case runs back-to-back, which is what absorbs a boot herd or a
/// legitimate cluster of periodic jobs landing on the same second without
/// letting sustained unprompted work exceed 10%.
pub(crate) const NODE_DUTY_BURST_MICROS: u64 = 30_000_000;

// ---------------------------------------------------------------------------
// Refusals.
// ---------------------------------------------------------------------------

/// Why [`schedule`] declined, as a VALUE the delegate receives.
///
/// Each variant maps to a distinct negative host code (see
/// `wasm_runtime::delegate_api::wakeup_error_codes`). The distinctions are not
/// cosmetic: `DelegateFull`/`DelegateBudget` say "change what you are doing",
/// `NodeFull`/`NodeBudget` say "not your fault, retry later", and a delegate
/// that cannot tell them apart cannot respond correctly to either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WakeupRefusal {
    /// `tag` exceeded [`MAX_WAKEUP_TAG_BYTES`].
    TagTooLong,
    /// The delay was below [`MIN_WAKEUP_DELAY`].
    DelayTooShort,
    /// The delay exceeded [`MAX_WAKEUP_DELAY`].
    DelayTooLong,
    /// This delegate already holds [`MAX_WAKEUPS_PER_DELEGATE`] leases.
    DelegateFull,
    /// The node already holds [`MAX_WAKEUPS_PER_NODE`] leases.
    NodeFull,
    /// This delegate's loop-occupancy budget is spent.
    DelegateBudget,
    /// The node's loop-occupancy budget is spent.
    NodeBudget,
    /// The durable write failed; the wakeup is NOT scheduled in either
    /// representation.
    Storage,
}

// ---------------------------------------------------------------------------
// The schedule.
// ---------------------------------------------------------------------------

/// A lease's identity. Re-arming the same `(delegate, tag)` replaces the lease
/// rather than taking a second one.
type WakeupId = (DelegateKey, Vec<u8>);

/// Deadline ordering key: `(due_millis_since_epoch, seq)`. The sequence number
/// breaks ties so two wakeups due in the same millisecond both survive and fire
/// in the order they were scheduled.
type Deadline = (u64, u64);

/// A time-refilled allowance of loop occupancy, in microseconds.
///
/// Not a token bucket over invocations: the quantity is the scarce one (loop
/// time), so a cheap handler is charged less than an expensive one and gets
/// correspondingly more wakeups.
#[derive(Debug)]
struct DutyBudget {
    /// Remaining credit, in microseconds of loop occupancy.
    credit_micros: u64,
    /// When `credit_micros` was last brought up to date. MONOTONIC on purpose:
    /// a wall-clock jump must not mint credit.
    last_refill: Instant,
}

impl DutyBudget {
    fn new(burst: u64, now: Instant) -> Self {
        Self {
            credit_micros: burst,
            last_refill: now,
        }
    }

    /// Bring credit up to date for the time elapsed since the last refill.
    fn refill(&mut self, now: Instant, divisor: u64, burst: u64) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        // `Instant` is monotonic, so this only moves forward; guard anyway
        // rather than assume, since `saturating_duration_since` yields zero for
        // a backwards reading and zero is the right answer for it.
        if elapsed.is_zero() {
            return;
        }
        self.last_refill = now;
        let earned = (elapsed.as_micros() as u64) / divisor;
        self.credit_micros = self.credit_micros.saturating_add(earned).min(burst);
    }

    /// Whether any credit remains. Admission is "you are not overdrawn", not
    /// "you can afford the run" — the run's cost is unknown until it happens.
    fn has_credit(&self) -> bool {
        self.credit_micros > 0
    }

    /// Charge a completed run.
    fn charge(&mut self, spent: Duration) {
        let spent = u64::try_from(spent.as_micros()).unwrap_or(u64::MAX);
        self.credit_micros = self.credit_micros.saturating_sub(spent);
    }

    /// Whether this budget is indistinguishable from a fresh one, and can
    /// therefore be dropped from the per-delegate map.
    fn is_full(&self, burst: u64) -> bool {
        self.credit_micros >= burst
    }
}

/// The whole of the node's wakeup state, behind ONE lock.
///
/// A `Mutex` and not a `DashMap`, which `.claude/rules/code-style.md` requires
/// justifying: every operation here is an ordered multi-key read-modify-write
/// across the deadline index, the reverse index, the per-delegate counts and
/// the budgets — exactly the documented exception. Making it one lock is also
/// what makes "one broker" true rather than aspirational, and the critical
/// sections are map operations on at most 1024 entries, never I/O.
#[derive(Debug)]
struct Schedule {
    /// Deadline-ordered leases. `BTreeMap` because the loop's only question is
    /// "what is due, and when is the next one".
    order: BTreeMap<Deadline, WakeupId>,
    /// Reverse index, so re-arming a tag can find and displace its old lease.
    index: HashMap<WakeupId, Deadline>,
    /// Outstanding leases per delegate. Derivable from `index`, kept explicitly
    /// so admission is O(1) rather than a scan of every lease on the node.
    per_delegate: HashMap<DelegateKey, usize>,
    /// Tie-breaker for equal deadlines, and what makes a displaced lease's
    /// stale `order` key unambiguous.
    next_seq: u64,
    /// The node's loop-occupancy allowance.
    node_budget: DutyBudget,
    /// Per-delegate loop-occupancy allowances.
    ///
    /// TIME-BOUNDED, per the AGENTS.md rule against permanently-refreshable
    /// entries: an entry is created only when a lease is granted, and dropped
    /// as soon as it is both at full credit and holding no leases — at which
    /// point it is byte-for-byte what a fresh entry would be, so dropping it
    /// changes nothing. A spent budget refills to full in
    /// `DELEGATE_DUTY_BURST_MICROS * DELEGATE_DUTY_DIVISOR` (about 8 minutes),
    /// so entries age out on their own and the map is bounded by the delegates
    /// active in that window.
    delegate_budgets: HashMap<DelegateKey, DutyBudget>,
}

impl Schedule {
    fn new(now: Instant) -> Self {
        Self {
            order: BTreeMap::new(),
            index: HashMap::new(),
            per_delegate: HashMap::new(),
            next_seq: 0,
            node_budget: DutyBudget::new(NODE_DUTY_BURST_MICROS, now),
            delegate_budgets: HashMap::new(),
        }
    }

    /// Drop a lease from every in-memory index. Returns whether one was there.
    fn remove_lease(&mut self, id: &WakeupId) -> bool {
        let Some(deadline) = self.index.remove(id) else {
            return false;
        };
        self.order.remove(&deadline);
        if let Some(count) = self.per_delegate.get_mut(&id.0) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.per_delegate.remove(&id.0);
            }
        }
        true
    }

    /// Drop budget entries that are indistinguishable from fresh ones.
    fn gc_budgets(&mut self) {
        let per_delegate = &self.per_delegate;
        self.delegate_budgets.retain(|delegate, budget| {
            per_delegate.contains_key(delegate) || !budget.is_full(DELEGATE_DUTY_BURST_MICROS)
        });
    }
}

/// **Private on purpose.** See the module docs: the in-memory schedule and the
/// durable rows must move together, and the only way to guarantee that is for
/// there to be no other way in.
static SCHEDULE: LazyLock<Mutex<Schedule>> =
    LazyLock::new(|| Mutex::new(Schedule::new(Instant::now())));

/// Take the schedule lock, recovering from a poisoned mutex.
///
/// A panic while holding this lock leaves the maps structurally intact (every
/// critical section is a sequence of infallible map operations), and refusing
/// every subsequent wakeup for the life of the process is a worse outcome than
/// continuing. Nothing here is a security invariant.
fn schedule_lock() -> std::sync::MutexGuard<'static, Schedule> {
    SCHEDULE.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn to_millis(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// The four mutating entry points.
// ---------------------------------------------------------------------------

/// Grant a wakeup lease to `delegate`, in BOTH representations.
///
/// `now` and `mono` are parameters rather than read from the clock so the
/// admission rules can be tested without sleeping — a guard you have only
/// watched pass is unverified, and every one of these has a test that watches
/// it fail.
///
/// Re-arming an existing `(delegate, tag)` REPLACES its lease and does not
/// consume a second one, so a periodic job holds one lease for its whole life.
/// It still costs an admission check: a delegate that re-arms in a tight loop
/// must still be refused, and the budget is what refuses it.
pub(crate) fn schedule<S: DelegateWakeupPersistence + ?Sized>(
    db: Option<&S>,
    delegate: &DelegateKey,
    tag: &[u8],
    after: Duration,
    now: SystemTime,
    mono: Instant,
) -> Result<(), WakeupRefusal> {
    // 1. Bytes. Re-checked here because the stdlib's checks bound only
    //    well-behaved callers — the import is guest-declared.
    if tag.len() > MAX_WAKEUP_TAG_BYTES {
        return Err(WakeupRefusal::TagTooLong);
    }
    if after < MIN_WAKEUP_DELAY {
        return Err(WakeupRefusal::DelayTooShort);
    }
    if after > MAX_WAKEUP_DELAY {
        return Err(WakeupRefusal::DelayTooLong);
    }

    let id: WakeupId = (delegate.clone(), tag.to_vec());
    let due = to_millis(now).saturating_add(after.as_millis() as u64);

    {
        let mut sched = schedule_lock();

        // 3. Loop occupancy, checked BEFORE the row caps so a delegate that is
        //    spinning is told it is spinning (`DelegateBudget`) rather than
        //    that it is full — the two call for different responses and the
        //    spinner is the case that matters.
        sched
            .node_budget
            .refill(mono, NODE_DUTY_DIVISOR, NODE_DUTY_BURST_MICROS);
        if !sched.node_budget.has_credit() {
            tracing::info!(
                delegate = %delegate.encode(),
                "Refused a delegate wakeup: the node's unprompted-execution budget is spent (#3972)"
            );
            return Err(WakeupRefusal::NodeBudget);
        }
        let has_delegate_credit = match sched.delegate_budgets.get_mut(delegate) {
            Some(budget) => {
                budget.refill(mono, DELEGATE_DUTY_DIVISOR, DELEGATE_DUTY_BURST_MICROS);
                budget.has_credit()
            }
            // No entry means no spend since the last GC, which is the same as
            // full credit. Do not create one here: a refused delegate must not
            // be able to grow this map.
            None => true,
        };
        if !has_delegate_credit {
            tracing::info!(
                delegate = %delegate.encode(),
                "Refused a delegate wakeup: this delegate's unprompted-execution \
                 budget is spent (#3972)"
            );
            return Err(WakeupRefusal::DelegateBudget);
        }

        // 2. Rows. A re-arm displaces its own lease, so it is checked against
        //    neither cap.
        let renewing = sched.index.contains_key(&id);
        if !renewing {
            if sched.index.len() >= MAX_WAKEUPS_PER_NODE {
                tracing::info!(
                    delegate = %delegate.encode(),
                    cap = MAX_WAKEUPS_PER_NODE,
                    "Refused a delegate wakeup: the node is at its wakeup cap (#3972)"
                );
                return Err(WakeupRefusal::NodeFull);
            }
            let held = sched.per_delegate.get(delegate).copied().unwrap_or(0);
            if held >= MAX_WAKEUPS_PER_DELEGATE {
                tracing::info!(
                    delegate = %delegate.encode(),
                    cap = MAX_WAKEUPS_PER_DELEGATE,
                    "Refused a delegate wakeup: this delegate is at its wakeup cap (#3972)"
                );
                return Err(WakeupRefusal::DelegateFull);
            }
        }
    }

    // Durable half FIRST. A row on disk with no lease in memory is recovered by
    // the next boot restore; a lease in memory with no row is silently lost on
    // restart, which is the failure this table exists to prevent.
    if let Some(db) = db
        && let Err(error) = db.persist_wakeup(delegate, tag, due)
    {
        tracing::error!(
            delegate = %delegate.encode(),
            %error,
            "Refused a delegate wakeup: its durable row could not be written (#3972)"
        );
        return Err(WakeupRefusal::Storage);
    }

    let mut sched = schedule_lock();
    // Re-take the lock rather than hold it across the durable write: the write
    // is I/O and this lock is on the delegate execution path. The caps were
    // checked above and can only have been relaxed by a concurrent fire, never
    // tightened past them by more than one concurrent grant — this is an
    // admission bound, not a safety invariant, and one slot of slack under
    // concurrent scheduling is not worth holding a lock across a disk write.
    sched.remove_lease(&id);
    let seq = sched.next_seq;
    sched.next_seq = sched.next_seq.saturating_add(1);
    let deadline = (due, seq);
    sched.order.insert(deadline, id.clone());
    sched.index.insert(id, deadline);
    *sched.per_delegate.entry(delegate.clone()).or_insert(0) += 1;
    sched
        .delegate_budgets
        .entry(delegate.clone())
        .or_insert_with(|| DutyBudget::new(DELEGATE_DUTY_BURST_MICROS, mono));

    tracing::debug!(
        delegate = %delegate.encode(),
        due_ms = due,
        "Granted a delegate wakeup lease (#3972)"
    );
    Ok(())
}

/// Take up to `max` leases whose deadline has passed, releasing them in BOTH
/// representations before they run.
///
/// **Remove-then-fire, deliberately.** A crash between removal and delivery
/// loses one wakeup; fire-then-remove would replay it on every subsequent boot,
/// and this is a primitive whose whole hazard is unbounded unprompted
/// execution. At-most-once is the correct side to fail on, and the stdlib's
/// guarantee is only ever "not before".
pub(crate) fn take_due<S: DelegateWakeupPersistence + ?Sized>(
    db: Option<&S>,
    now: SystemTime,
    max: usize,
) -> Vec<(DelegateKey, Vec<u8>)> {
    let cutoff = to_millis(now);
    let mut fired = Vec::new();
    {
        let mut sched = schedule_lock();
        while fired.len() < max {
            let Some((&deadline, _)) = sched.order.iter().next() else {
                break;
            };
            if deadline.0 > cutoff {
                break;
            }
            let Some(id) = sched.order.remove(&deadline) else {
                break;
            };
            sched.index.remove(&id);
            if let Some(count) = sched.per_delegate.get_mut(&id.0) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    sched.per_delegate.remove(&id.0);
                }
            }
            fired.push(id);
        }
        sched.gc_budgets();
    }
    if let Some(db) = db {
        for (delegate, tag) in &fired {
            db.forget_wakeup(delegate, tag);
        }
    }
    fired
}

/// How long until the next lease is due, or `None` if none are.
///
/// The loop uses this for a `sleep_until` arm. `Some(ZERO)` means work is
/// already due.
pub(crate) fn next_due_in(now: SystemTime) -> Option<Duration> {
    let sched = schedule_lock();
    let (&(due, _), _) = sched.order.iter().next()?;
    Some(Duration::from_millis(due.saturating_sub(to_millis(now))))
}

/// Charge a completed wakeup-driven run against the node's and the delegate's
/// loop-occupancy budgets.
///
/// Called with the MEASURED duration of the run, which is what makes this a
/// bound on the scarce quantity rather than on a proxy for it.
pub(crate) fn charge_run(delegate: &DelegateKey, spent: Duration, mono: Instant) {
    let mut sched = schedule_lock();
    sched
        .node_budget
        .refill(mono, NODE_DUTY_DIVISOR, NODE_DUTY_BURST_MICROS);
    sched.node_budget.charge(spent);
    sched
        .delegate_budgets
        .entry(delegate.clone())
        .or_insert_with(|| DutyBudget::new(DELEGATE_DUTY_BURST_MICROS, mono))
        .refill(mono, DELEGATE_DUTY_DIVISOR, DELEGATE_DUTY_BURST_MICROS);
    if let Some(budget) = sched.delegate_budgets.get_mut(delegate) {
        budget.charge(spent);
    }
}

/// Release every lease `delegate` holds, in BOTH representations.
///
/// Called on `UnregisterDelegate` and by boot reconciliation for a delegate
/// that no longer exists. Without it a durable row outlives its delegate and is
/// restored on every subsequent boot — a lease nothing will ever release, and
/// silent because the in-memory half looks correct.
pub(crate) fn forget_delegate<S: DelegateWakeupPersistence + ?Sized>(
    db: Option<&S>,
    delegate: &DelegateKey,
) {
    {
        let mut sched = schedule_lock();
        let ids: Vec<WakeupId> = sched
            .index
            .keys()
            .filter(|(d, _)| d == delegate)
            .cloned()
            .collect();
        for id in ids {
            sched.remove_lease(&id);
        }
        sched.delegate_budgets.remove(delegate);
    }
    if let Some(db) = db {
        db.forget_wakeups_for_delegate(delegate);
    }
}

// ---------------------------------------------------------------------------
// Boot restore.
// ---------------------------------------------------------------------------

/// What [`restore`] did, for the caller to log. A restore that could not read
/// the table is an `Err`, never an empty success — treating a transient read
/// failure as "no wakeups" would silently discard every delegate's schedule,
/// which is the exact outcome the table exists to prevent.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RestoreOutcome {
    /// Leases put back into the schedule.
    pub(crate) restored: usize,
    /// Rows dropped because their delegate is no longer registered.
    pub(crate) orphaned: usize,
    /// Rows dropped because the node was already at [`MAX_WAKEUPS_PER_NODE`].
    pub(crate) over_cap: usize,
    /// Restored leases whose deadline had already passed, and which were
    /// therefore smeared across [`BOOT_SPREAD`] rather than fired at once.
    pub(crate) overdue: usize,
}

/// How widely an overdue backlog is spread at boot.
///
/// A node down for a week comes back with every lease past due. Firing them in
/// one batch is a thundering herd of unprompted delegate runs at exactly the
/// moment the node is busiest re-establishing itself, and the duty budget would
/// absorb it only by refusing the RESCHEDULES that follow — punishing well
/// -behaved delegates for the node's downtime. Smearing them costs nothing: the
/// stdlib's guarantee is "not before", and these are already late.
pub(crate) const BOOT_SPREAD: Duration = Duration::from_secs(60);

/// Re-arm the durable schedule at boot, dropping rows whose delegate is gone.
///
/// `is_registered` answers whether a delegate still exists; boot reconciliation
/// is the only place that question can be asked cheaply for every row at once.
pub(crate) fn restore<S: DelegateWakeupPersistence + ?Sized>(
    db: &S,
    is_registered: impl Fn(&DelegateKey) -> bool,
    now: SystemTime,
    mono: Instant,
) -> Result<RestoreOutcome, String> {
    let rows = db.load_wakeups()?;
    let mut outcome = RestoreOutcome::default();
    let now_ms = to_millis(now);
    let spread_ms = BOOT_SPREAD.as_millis() as u64;

    let mut sched = schedule_lock();
    for (delegate, tag, due) in rows {
        if !is_registered(&delegate) {
            outcome.orphaned += 1;
            drop(sched);
            db.forget_wakeup(&delegate, &tag);
            sched = schedule_lock();
            continue;
        }
        if sched.index.len() >= MAX_WAKEUPS_PER_NODE {
            outcome.over_cap += 1;
            drop(sched);
            db.forget_wakeup(&delegate, &tag);
            sched = schedule_lock();
            continue;
        }
        let due = if due <= now_ms {
            outcome.overdue += 1;
            // Deterministic smear rather than a random jitter, so a restore is
            // reproducible in a test: successive overdue rows land at
            // successive points across the window.
            let slot = (outcome.overdue as u64).saturating_sub(1) % spread_ms.max(1);
            now_ms.saturating_add(slot)
        } else {
            due
        };
        let id: WakeupId = (delegate.clone(), tag);
        let seq = sched.next_seq;
        sched.next_seq = sched.next_seq.saturating_add(1);
        let deadline = (due, seq);
        // A duplicate row for the same (delegate, tag) cannot exist — the table
        // is keyed on exactly that — but displace rather than assume, so a
        // corrupt table cannot leave a dangling `order` entry.
        sched.remove_lease(&id);
        sched.order.insert(deadline, id.clone());
        sched.index.insert(id, deadline);
        *sched.per_delegate.entry(delegate.clone()).or_insert(0) += 1;
        sched
            .delegate_budgets
            .entry(delegate)
            .or_insert_with(|| DutyBudget::new(DELEGATE_DUTY_BURST_MICROS, mono));
        outcome.restored += 1;
    }
    Ok(outcome)
}

// ---------------------------------------------------------------------------
// Durable half.
// ---------------------------------------------------------------------------

/// How a storage backend records wakeup leases durably.
///
/// **Every method defaults to a no-op**, which is the honest answer for the
/// sqlite backend and the mock runtime: they degrade to the in-memory schedule
/// alone, so a wakeup survives the process and not a restart. redb — the
/// default feature and what ships — overrides all four.
///
/// Object-safe on purpose: the serial `contract_handling` loop reaches its
/// executor generically and gets one of these through a trait object.
///
/// Deliberately NOT a supertrait of `StateStorage`. It has no need to be, and
/// keeping it standalone means it cannot collide with the parallel
/// `DelegateSubscriptionPersistence` work in #5493.
pub trait DelegateWakeupPersistence: Send + Sync {
    /// Record a lease due at `due_millis` (ms since the Unix epoch). Replaces
    /// any existing row for the same `(delegate, tag)`.
    ///
    /// `Err` means the lease was NOT recorded, and [`schedule`] refuses on it
    /// rather than granting a lease that a restart would silently drop.
    fn persist_wakeup(
        &self,
        _delegate: &DelegateKey,
        _tag: &[u8],
        _due_millis: u64,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Forget one lease. Idempotent.
    fn forget_wakeup(&self, _delegate: &DelegateKey, _tag: &[u8]) {}

    /// Forget every lease held by `delegate`. Idempotent.
    fn forget_wakeups_for_delegate(&self, _delegate: &DelegateKey) {}

    /// Every recorded lease, for boot restore.
    ///
    /// `Err` means the store could not be read. A caller must NOT treat that as
    /// "no wakeups" — see [`RestoreOutcome`].
    fn load_wakeups(&self) -> Result<Vec<(DelegateKey, Vec<u8>, u64)>, String> {
        Ok(Vec::new())
    }
}

/// Direct access for tests that need to observe or reset the schedule.
///
/// Production code MUST NOT use these — the whole point of the module is that
/// the two representations move together. The schedule is a process-global
/// `static` and the test binary runs cases in one process, so tests that assert
/// on it have to be able to clear it.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// Reset the schedule to empty with full budgets.
    pub(crate) fn reset(now: Instant) {
        *schedule_lock() = Schedule::new(now);
    }

    /// Outstanding leases, node-wide.
    pub(crate) fn outstanding() -> usize {
        schedule_lock().index.len()
    }

    /// Outstanding leases held by one delegate.
    pub(crate) fn outstanding_for(delegate: &DelegateKey) -> usize {
        schedule_lock()
            .per_delegate
            .get(delegate)
            .copied()
            .unwrap_or(0)
    }

    /// Remaining node loop-occupancy credit, in microseconds.
    pub(crate) fn node_credit_micros() -> u64 {
        schedule_lock().node_budget.credit_micros
    }

    /// Remaining per-delegate credit, or `None` if the delegate has no entry
    /// (which is equivalent to full credit).
    pub(crate) fn delegate_credit_micros(delegate: &DelegateKey) -> Option<u64> {
        schedule_lock()
            .delegate_budgets
            .get(delegate)
            .map(|b| b.credit_micros)
    }

    /// How many delegates hold a budget entry. Used to pin the GC.
    pub(crate) fn budget_entries() -> usize {
        schedule_lock().delegate_budgets.len()
    }

    /// Drain the node's credit so a budget refusal can be provoked.
    pub(crate) fn drain_node_credit() {
        schedule_lock().node_budget.credit_micros = 0;
    }
}
