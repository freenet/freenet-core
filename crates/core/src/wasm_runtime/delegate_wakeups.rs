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
//! # Admission cannot bound occupancy. Only the fire path can.
//!
//! This is the load-bearing thing to understand about check 3, and it took a
//! wrong version to find:
//!
//! > **Admission can only ever ask "were you solvent when you asked", never
//! > "can you afford what you are about to do" — because a run's cost is
//! > unknown until it has happened.**
//!
//! So [`schedule`] credits before a run and [`charge_run`] debits after it, and
//! a delegate holding [`MAX_WAKEUPS_PER_DELEGATE`] leases that all come due
//! together gets sixteen runs on the strength of ONE solvency check. At
//! `max_execution_seconds` that is 80 s of loop time, against a 1% share that
//! takes 133 MINUTES to earn. "Bounded by the row cap" is true of that and
//! carries no information — and a bound that is technically correct and
//! uninformative is worse than an absent one, because it stops anyone looking.
//!
//! The original version was worse still: `charge` saturated at zero, so every
//! debt was DISCARDED the instant it was incurred. Each of the sixteen fires
//! found a freshly reset budget, and every schedule between them saw credit
//! refilling from zero rather than from -5 s.
//!
//! Two things fix it, and both are necessary. [`DutyBudget::credit_micros`] is
//! SIGNED, so a debt survives to be repaid; and [`affordability`] is checked at
//! FIRE time, so a lease whose delegate is in debt is deferred for the computed
//! repayment interval rather than run. The overshoot is then one run, not one
//! per lease held. The fire-time check is not belt-and-braces — it is the only
//! place the question can be asked at all.
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
use std::time::{Duration, SystemTime, UNIX_EPOCH};
// The MONOTONIC clock is tokio's, not `std`'s, so a test that pauses time
// controls budget refill and debt repayment. It is the same clock
// `util::time_source::TimeSource::now` returns, so a site here can later
// take an injected `DynTimeSource` without a type change. Deadlines are a
// separate axis and stay on `SystemTime`: they are persisted as absolute
// wall-clock milliseconds because they must survive a restart.
use tokio::time::Instant;

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
/// Log a refusal at `info!` when it CHANGES the refusing state, and at
/// `debug!` while that state persists.
///
/// A refusal path that logs unconditionally hands the REFUSED party a
/// log-volume amplifier: it chooses the retry rate, and refusing is what logs.
/// `crates/core/Cargo.toml` enables `release_max_level_info`, so an `info!`
/// here survives into shipped binaries, and a delegate can call
/// `__frnt__delegate__schedule_wakeup` in a loop inside one `process()`,
/// bounded only by `max_execution_seconds`. The delegate that does so is
/// precisely the one already at its cap, which would make the mechanism meant
/// to CONTAIN a misbehaving delegate the thing that amplifies it.
///
/// Logging the transition keeps the operationally interesting event (this
/// delegate STARTED being refused) at `info!`, while the steady state costs
/// nothing in release. A delegate cannot manufacture transitions without
/// succeeding in between, and succeeding is what the cap it is hitting
/// prevents.
///
/// A macro and not a function so the structured fields and the `#3972` message
/// survive unchanged; a helper taking `&str` would have to `format!` on a path
/// a delegate can drive.
macro_rules! log_refusal {
    ($entering:expr, $($args:tt)*) => {
        if $entering {
            tracing::info!($($args)*);
        } else {
            tracing::debug!($($args)*);
        }
    };
}

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

/// Node credit held back from NEW leases, so a delegate re-arming a lease it
/// already holds is not starved by the node's total load (#5597 property 4:
/// "admission of new grants must not be able to starve renewal of existing
/// ones").
///
/// A quarter of the node burst. The asymmetry is the point: a newcomer refused
/// asks again, while an incumbent refused STOPS — its next run was the thing
/// that would have re-armed it, so a single badly-timed refusal ends a periodic
/// job silently and permanently. Those are not the same failure and must not be
/// priced the same.
///
/// It does not weaken the node bound. A renewal still costs the delegate's own
/// 1% budget, so the reserve cannot be drained by one delegate re-arming in a
/// loop — it takes as many distinct delegates as the node bound already allows.
pub(crate) const NODE_RENEWAL_RESERVE_MICROS: u64 = NODE_DUTY_BURST_MICROS / 4;

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

impl WakeupRefusal {
    /// The negative host code this refusal is reported to the delegate as.
    ///
    /// A total match with no wildcard arm, so adding a refusal without giving
    /// it a code is a compile error rather than a silent collapse onto an
    /// existing one — which is the specific way `subscribe_contract`'s `bool`
    /// went wrong (#5565).
    pub(crate) fn code(self) -> i64 {
        use crate::wasm_runtime::delegate_api::wakeup_error_codes as codes;
        match self {
            Self::TagTooLong => codes::ERR_WAKEUP_TAG_TOO_LONG,
            Self::DelayTooShort => codes::ERR_WAKEUP_DELAY_TOO_SHORT,
            Self::DelayTooLong => codes::ERR_WAKEUP_DELAY_TOO_LONG,
            Self::DelegateFull => codes::ERR_WAKEUP_DELEGATE_FULL,
            Self::NodeFull => codes::ERR_WAKEUP_NODE_FULL,
            Self::DelegateBudget => codes::ERR_WAKEUP_DELEGATE_BUDGET,
            Self::NodeBudget => codes::ERR_WAKEUP_NODE_BUDGET,
            Self::Storage => codes::ERR_WAKEUP_STORAGE,
        }
    }
}

// ---------------------------------------------------------------------------
// The schedule.
// ---------------------------------------------------------------------------

/// A lease's identity. Re-arming the same `(delegate, tag)` replaces the lease
/// rather than taking a second one.
type WakeupId = (DelegateKey, Vec<u8>);

/// One durable row as the backend hands it back: `(delegate, tag, due_millis)`.
type PersistedWakeup = (DelegateKey, Vec<u8>, u64);

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
    ///
    /// **SIGNED, and that is the whole point.** A run is charged after it
    /// finishes, so a run always overshoots whatever credit remained. If the
    /// overshoot were discarded (`saturating_sub` to zero) the budget would
    /// forget every debt the moment it was incurred, and a delegate holding
    /// [`MAX_WAKEUPS_PER_DELEGATE`] leases could spend 16 x
    /// `max_execution_seconds` = 80 s of loop time — 133 minutes' worth of a 1%
    /// share — before anything refused it, because each fire would find a
    /// budget freshly reset to zero and every schedule between them would see
    /// credit refilled from zero rather than from -5 s.
    ///
    /// Carrying the debt is what makes the fire-time check in
    /// [`affordability`] bite: the overshoot is then ONE run, not sixteen.
    credit_micros: i64,
    /// When `credit_micros` was last brought up to date. MONOTONIC on purpose:
    /// a wall-clock jump must not mint credit.
    last_refill: Instant,
    /// Whether this delegate's LAST admission decision was a refusal.
    ///
    /// Here, and not in a set of its own, because a set keyed by delegate would
    /// be grown BY THE REFUSAL PATH: a delegate driving refusals would add an
    /// entry apiece, which is the memory version of the log problem this field
    /// exists to fix, and `schedule` already refuses to create a budget entry
    /// on that path for exactly that reason. Every delegate that can reach a
    /// per-delegate refusal already has an entry in `delegate_budgets` (a
    /// grant, a boot restore and a deferral each create one), and that map is
    /// already bounded and GC'd, so this bit inherits both rather than needing
    /// its own bounding.
    refusing: bool,
}

impl DutyBudget {
    fn new(burst: u64, now: Instant) -> Self {
        Self {
            credit_micros: burst as i64,
            last_refill: now,
            refusing: false,
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
        let earned = i64::try_from(elapsed.as_micros() / divisor as u128).unwrap_or(i64::MAX);
        self.credit_micros = self.credit_micros.saturating_add(earned).min(burst as i64);
    }

    /// Whether any credit remains.
    fn has_credit(&self) -> bool {
        self.credit_micros > 0
    }

    /// How long until this budget is solvent again, at `divisor`'s refill rate.
    ///
    /// `ZERO` when it already is. This is what a deferred wakeup waits for, so
    /// a lease that cannot be afforded is rescheduled for exactly when it can
    /// be, rather than retried on a fixed tick that is either wasteful or
    /// wrong.
    fn time_to_solvency(&self, divisor: u64) -> Duration {
        if self.credit_micros > 0 {
            return Duration::ZERO;
        }
        let deficit = self.credit_micros.unsigned_abs().saturating_add(1);
        Duration::from_micros(deficit.saturating_mul(divisor))
    }

    /// Charge a completed run, carrying the debt.
    ///
    /// Debt is floored at one burst. Unbounded debt would let a single
    /// pathological run (a guest wedged in an uninterruptible host call, #5594)
    /// disable a delegate for hours; one burst of debt is one burst-worth of
    /// recovery time, which is proportionate and bounded.
    fn charge(&mut self, spent: Duration, burst: u64) {
        let spent = i64::try_from(spent.as_micros()).unwrap_or(i64::MAX);
        self.credit_micros = self
            .credit_micros
            .saturating_sub(spent)
            .max(-(burst as i64));
    }

    /// Whether this budget is indistinguishable from a fresh one, and can
    /// therefore be dropped from the per-delegate map.
    ///
    /// `refusing` is deliberately NOT consulted here, and adding it would be a
    /// bug rather than a tightening. An entry at full credit holding no leases
    /// belongs to an IDLE delegate; dropping it discards the bit, so the next
    /// refusal reports at `info!` again. That is correct: after an idle spell
    /// it is a NEW episode of being refused, not a continuation of the old one,
    /// and it is the transition an operator wants to see. Keeping the entry
    /// alive to preserve the bit would instead make this map retainable by a
    /// delegate that does nothing but get refused.
    fn is_full(&self, burst: u64) -> bool {
        self.credit_micros >= burst as i64
    }

    /// Record a refusal, returning whether it ENTERS the refusing state.
    ///
    /// See [`log_refusal`] for why the caller needs this rather than logging
    /// every refusal.
    fn note_refused(&mut self) -> bool {
        !std::mem::replace(&mut self.refusing, true)
    }

    /// Record a grant, returning whether it ENDS a run of refusals.
    fn note_granted(&mut self) -> bool {
        std::mem::replace(&mut self.refusing, false)
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
    /// How many times a lease has been DEFERRED because its delegate was parked
    /// when it came due. See [`defer`].
    ///
    /// Only ever holds ids that are currently deferred: [`take_due`] moves the
    /// count out with the lease, so an entry exists only between a deferral and
    /// the lease's next due time. Bounded by the outstanding-lease caps.
    deferrals: HashMap<WakeupId, u32>,
    /// The node's loop-occupancy allowance.
    node_budget: DutyBudget,
    /// Whether the NODE's last admission decision was a refusal.
    ///
    /// A node-wide condition belongs on the node rather than on whichever
    /// delegate happened to ask while it held. One `bool`, so unlike a
    /// per-delegate map there is nothing here a refused caller can grow.
    node_refusing: bool,
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
            deferrals: HashMap::new(),
            node_budget: DutyBudget::new(NODE_DUTY_BURST_MICROS, now),
            node_refusing: false,
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

    /// Record a node-wide refusal, returning whether it ENTERS that state.
    fn note_node_refused(&mut self) -> bool {
        !std::mem::replace(&mut self.node_refusing, true)
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
    SCHEDULE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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

        // A RENEWAL — re-arming a tag this delegate already holds — is
        // recognised before anything else, because it changes what two of the
        // checks below mean. It takes no new row (it displaces its own), and it
        // draws on reserved node capacity (see `NODE_RENEWAL_RESERVE_MICROS`).
        let renewing = sched.index.contains_key(&id);

        // THE CALLER'S OWN RESPONSIBILITY IS CHECKED FIRST, ALWAYS.
        //
        // Both dimensions are bounded twice, per delegate and node-wide, and
        // under contention BOTH bounds can be binding at once. Whichever is
        // tested first is the code the delegate receives — so testing the node
        // first would tell a delegate that is over its OWN limit "not your
        // fault, retry later", which is the single most misleading thing this
        // interface can say. The distinct codes exist so a delegate can tell
        // "change what you are doing" from "wait"; getting the priority
        // backwards hands it the wrong one in exactly the contended case where
        // the distinction is worth having.

        // 3. Loop occupancy, checked before the row caps: a delegate that is
        //    spinning should be told it is spinning rather than that it is
        //    full, because the spinner is the case that matters.
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
            // The entry is known to exist: `has_delegate_credit` is only false
            // when the match above found one. `is_none_or` states the fallback
            // rather than unwrapping a shape that a later edit could change.
            let entering = sched
                .delegate_budgets
                .get_mut(delegate)
                .is_none_or(DutyBudget::note_refused);
            log_refusal!(
                entering,
                delegate = %delegate.encode(),
                "Refused a delegate wakeup: this delegate's unprompted-execution \
                 budget is spent (#3972)"
            );
            return Err(WakeupRefusal::DelegateBudget);
        }

        sched
            .node_budget
            .refill(mono, NODE_DUTY_DIVISOR, NODE_DUTY_BURST_MICROS);
        // RENEWAL CAPACITY IS RESERVED (#5597 property 4). A new lease must
        // leave the reserve intact; a renewal may draw on it. Without this, a
        // busy node refuses a well-behaved delegate's weekly re-arm on whatever
        // second it happens to ask, and the delegate simply stops running —
        // "the newcomer is refused" is the acceptable failure, "the incumbent
        // silently loses what it had" is not.
        //
        // This does not weaken the node bound: a renewal still costs the
        // delegate's OWN budget, checked above, so the reserve cannot be
        // drained by one delegate re-arming in a loop.
        let node_floor: i64 = if renewing {
            0
        } else {
            NODE_RENEWAL_RESERVE_MICROS as i64
        };
        if sched.node_budget.credit_micros <= node_floor {
            let entering = sched.note_node_refused();
            log_refusal!(
                entering,
                delegate = %delegate.encode(),
                renewing,
                "Refused a delegate wakeup: the node's unprompted-execution budget is spent (#3972)"
            );
            return Err(WakeupRefusal::NodeBudget);
        }

        // 2. Rows. A renewal displaces its own lease, so it is checked against
        //    neither cap.
        if !renewing {
            let held = sched.per_delegate.get(delegate).copied().unwrap_or(0);
            if held >= MAX_WAKEUPS_PER_DELEGATE {
                // Holding leases implies a budget entry: every path that adds a
                // lease (grant, boot restore, deferral) creates one.
                let entering = sched
                    .delegate_budgets
                    .get_mut(delegate)
                    .is_none_or(DutyBudget::note_refused);
                log_refusal!(
                    entering,
                    delegate = %delegate.encode(),
                    cap = MAX_WAKEUPS_PER_DELEGATE,
                    "Refused a delegate wakeup: this delegate is at its wakeup cap (#3972)"
                );
                return Err(WakeupRefusal::DelegateFull);
            }
            if sched.index.len() >= MAX_WAKEUPS_PER_NODE {
                let entering = sched.note_node_refused();
                log_refusal!(
                    entering,
                    delegate = %delegate.encode(),
                    cap = MAX_WAKEUPS_PER_NODE,
                    "Refused a delegate wakeup: the node is at its wakeup cap (#3972)"
                );
                return Err(WakeupRefusal::NodeFull);
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
    // A fresh grant resets the deferral count: this is a new lease on the same
    // tag, not a continuation of the one that kept missing its delegate.
    sched.deferrals.remove(&id);
    let seq = sched.next_seq;
    sched.next_seq = sched.next_seq.saturating_add(1);
    let deadline = (due, seq);
    sched.order.insert(deadline, id.clone());
    sched.index.insert(id, deadline);
    *sched.per_delegate.entry(delegate.clone()).or_insert(0) += 1;
    let recovered = sched
        .delegate_budgets
        .entry(delegate.clone())
        .or_insert_with(|| DutyBudget::new(DELEGATE_DUTY_BURST_MICROS, mono))
        .note_granted();
    // A grant proves the node is no longer refusing, whichever delegate
    // provoked the refusal that set it.
    sched.node_refusing = false;

    if recovered {
        tracing::info!(
            delegate = %delegate.encode(),
            "Granting this delegate wakeups again after a run of refusals (#3972)"
        );
    }
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
) -> Vec<DueWakeup> {
    let cutoff = to_millis(now);
    let mut fired: Vec<DueWakeup> = Vec::new();
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
            // Move the deferral count OUT with the lease. A lease that is
            // delivered never puts it back, so the map holds only currently
            // -deferred ids and needs no separate sweep.
            let attempts = sched.deferrals.remove(&id).unwrap_or(0);
            let (delegate, tag) = id;
            fired.push(DueWakeup {
                delegate,
                tag,
                attempts,
            });
        }
        sched.gc_budgets();
    }
    if let Some(db) = db {
        for due in &fired {
            db.forget_wakeup(&due.delegate, &due.tag);
        }
    }
    fired
}

/// A lease whose deadline has passed, handed to the loop to deliver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DueWakeup {
    pub(crate) delegate: DelegateKey,
    pub(crate) tag: Vec<u8>,
    /// How many times this lease has already been deferred because its delegate
    /// was parked. Pass it back to [`defer`] to keep the bound honest.
    pub(crate) attempts: u32,
}

/// How long a lease waits when its delegate was parked at delivery time.
///
/// Short, because a park is short: the point is to ride out the park, not to
/// reschedule the job.
pub(crate) const WAKEUP_PARK_RETRY: Duration = Duration::from_secs(2);

/// How many times one lease may be deferred before it is dropped.
///
/// **This is a backstop against an unbounded retry, not a delivery policy.**
/// Dropping a wakeup means a delegate was told `Ok(())`, spent a lease and will
/// never be woken — the accepted-then-silently-dropped shape this whole code
/// table exists to avoid — so it must not be reachable in ordinary operation,
/// and it is not: a park always terminates at `delegate_park::PARK_TTL`, and
/// `park_deferrals_outlast_the_park_ttl` FAILS if that stops being true.
///
/// That test is the difference between this and "bounded by another module's
/// timeout", which is a cross-module assumption that rots silently the first
/// time the timeout is tuned. Here, tuning `PARK_TTL` past this window breaks
/// CI and someone raises this number deliberately.
///
/// A budget deferral cannot exhaust it either: that one waits for exactly the
/// computed repayment time, so it converges in one or two attempts rather than
/// ticking.
pub(crate) const MAX_WAKEUP_DEFERRALS: u32 = 64;

/// Put a lease [`take_due`] just returned back into the schedule, later.
///
/// Used when the delegate is PARKED at delivery time (#5544): running it would
/// clobber the parked continuation's context, and dropping it would lose a job
/// the delegate was told it had.
///
/// **This grants nothing and therefore takes no admission check.** It re-inserts
/// a lease that was already admitted, at a later deadline; it cannot increase
/// the number of leases a delegate holds, and it consumes no loop occupancy
/// because no run happens. Calling it with an id `take_due` did not just return
/// WOULD be a grant, and must not be done.
///
/// Deferring is right here in a way it would not be for a contract
/// notification: a wakeup carries no payload and nothing about it goes stale,
/// and the stdlib's guarantee is only ever "not before". Queueing it behind the
/// park instead would need a third `PendingRun` variant with its own cap and
/// its own byte accounting, to solve a problem this primitive's own deadline
/// already solves.
///
/// `after` is how long to wait: [`WAKEUP_PARK_RETRY`] for a park (ride it out),
/// or the delegate's computed time to solvency for a budget deferral (come back
/// when you can afford it). A fixed tick would be wrong for the second — it
/// would either burn attempts or wait far longer than the debt.
///
/// Returns `false` if the lease has been deferred [`MAX_WAKEUP_DEFERRALS`]
/// times and was dropped instead. That is logged at `error!`, not `debug!`:
/// `release_max_level_info` compiles `debug!` out, and a wakeup that vanishes
/// after a delegate was told `Ok(())` is exactly the kind of thing that must
/// not be invisible in a shipped binary.
pub(crate) fn defer<S: DelegateWakeupPersistence + ?Sized>(
    db: Option<&S>,
    due: &DueWakeup,
    after: Duration,
    now: SystemTime,
) -> bool {
    if due.attempts >= MAX_WAKEUP_DEFERRALS {
        tracing::error!(
            delegate = %due.delegate.encode(),
            attempts = due.attempts,
            "Dropped a delegate wakeup after exhausting every delivery attempt. The \
             delegate was told this wakeup was scheduled and will not receive it \
             (#3972)"
        );
        return false;
    }
    let retry_at = to_millis(now).saturating_add(after.as_millis() as u64);

    // Durable half first, as in `schedule`. A failure here is NOT a refusal —
    // the lease is already granted and there is no caller to tell — so the
    // in-memory re-insert proceeds and the loss is bounded to a restart.
    if let Some(db) = db
        && let Err(error) = db.persist_wakeup(&due.delegate, &due.tag, retry_at)
    {
        tracing::warn!(
            delegate = %due.delegate.encode(),
            %error,
            "Could not re-persist a deferred delegate wakeup; it will still fire \
             unless the node restarts first (#3972)"
        );
    }

    let id: WakeupId = (due.delegate.clone(), due.tag.clone());
    let mut sched = schedule_lock();
    let seq = sched.next_seq;
    sched.next_seq = sched.next_seq.saturating_add(1);
    let deadline = (retry_at, seq);
    sched.remove_lease(&id);
    sched.order.insert(deadline, id.clone());
    sched.index.insert(id.clone(), deadline);
    *sched.per_delegate.entry(due.delegate.clone()).or_insert(0) += 1;
    sched.deferrals.insert(id, due.attempts.saturating_add(1));
    true
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
    sched.node_budget.charge(spent, NODE_DUTY_BURST_MICROS);
    let budget = sched
        .delegate_budgets
        .entry(delegate.clone())
        .or_insert_with(|| DutyBudget::new(DELEGATE_DUTY_BURST_MICROS, mono));
    budget.refill(mono, DELEGATE_DUTY_DIVISOR, DELEGATE_DUTY_BURST_MICROS);
    budget.charge(spent, DELEGATE_DUTY_BURST_MICROS);
}

/// Whether a due lease can be RUN right now, and if not, how long until it can.
///
/// # Why admission alone is not enough
///
/// [`schedule`] checks credit BEFORE a run and [`charge_run`] debits AFTER it,
/// so admission can only ever ask "were you solvent when you asked", never "can
/// you afford what you are about to do" — the run's cost is unknowable until it
/// has happened. A delegate holding [`MAX_WAKEUPS_PER_DELEGATE`] leases that
/// all come due at once therefore gets 16 runs on the strength of one
/// solvency check: 80 s of loop time at `max_execution_seconds`, against a 1%
/// share that takes 133 MINUTES to earn it. "Bounded by the row cap" is true of
/// that and is not a bound anyone should accept.
///
/// This closes it at the other end. A lease whose delegate is in debt is not
/// run; it is deferred until the debt is repaid, so the overshoot is ONE run
/// rather than one per lease held. That is what makes the duty budget a bound
/// on loop occupancy rather than only on the rate of asking.
///
/// Returns `Err(wait)` with the LONGER of the node's and the delegate's
/// recovery times — waiting for the shorter would just defer again.
pub(crate) fn affordability(delegate: &DelegateKey, mono: Instant) -> Result<(), Duration> {
    let mut sched = schedule_lock();
    sched
        .node_budget
        .refill(mono, NODE_DUTY_DIVISOR, NODE_DUTY_BURST_MICROS);
    let node_wait = sched.node_budget.time_to_solvency(NODE_DUTY_DIVISOR);
    let delegate_wait = match sched.delegate_budgets.get_mut(delegate) {
        Some(budget) => {
            budget.refill(mono, DELEGATE_DUTY_DIVISOR, DELEGATE_DUTY_BURST_MICROS);
            budget.time_to_solvency(DELEGATE_DUTY_DIVISOR)
        }
        // No entry is full credit; see `schedule`.
        None => Duration::ZERO,
    };
    let wait = node_wait.max(delegate_wait);
    if wait.is_zero() { Ok(()) } else { Err(wait) }
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
            sched.deferrals.remove(&id);
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
    /// Rows left on disk, untouched, because the delegate registry could not be
    /// trusted to answer "is this delegate gone" — see [`restore`]'s
    /// `registered_delegates`. Non-zero means this boot deliberately did
    /// nothing, and the next one will try again.
    pub(crate) deferred: usize,
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
///
/// # `registered_delegates`, and why reconciliation FAILS CLOSED
///
/// The delete here is irreversible, and its input is a NEGATIVE answer from a
/// registry that can be empty for two entirely different reasons: this node
/// really has no delegates, or `DelegateStore::new_with_shared` could not read
/// the delegate index and logged a `warn!` before continuing with an empty map.
/// A transient read failure would then look exactly like a mass uninstall, and
/// this function would delete every wakeup on the node — the same "a read
/// failure must not be read as 'nothing exists'" trap the wakeup table's own
/// `Err` handling exists to avoid, arriving through a different door.
///
/// So a count of zero means "do not reconcile at all this boot". The cost is a
/// boot's worth of stale rows on a node that genuinely uninstalled everything,
/// cleaned up on the next boot; the cost of the other direction is every
/// delegate's schedule, permanently.
pub(crate) fn restore<S: DelegateWakeupPersistence + ?Sized>(
    db: &S,
    registered_delegates: usize,
    is_registered: impl Fn(&DelegateKey) -> bool,
    now: SystemTime,
    mono: Instant,
) -> Result<RestoreOutcome, String> {
    let rows = db.load_wakeups()?;
    let mut outcome = RestoreOutcome::default();
    let now_ms = to_millis(now);
    let spread_ms = BOOT_SPREAD.as_millis() as u64;

    if registered_delegates == 0 && !rows.is_empty() {
        outcome.deferred = rows.len();
        tracing::error!(
            rows = rows.len(),
            "Deferred delegate-wakeup reconciliation: this node has wakeup rows but no \
             registered delegates, which is either a mass uninstall or a delegate index \
             that failed to load. Nothing was restored or deleted; the next boot will \
             try again (#3972)"
        );
        return Ok(outcome);
    }

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
    fn load_wakeups(&self) -> Result<Vec<PersistedWakeup>, String> {
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
    pub(crate) fn node_credit_micros() -> i64 {
        schedule_lock().node_budget.credit_micros
    }

    /// Remaining per-delegate credit, or `None` if the delegate has no entry
    /// (which is equivalent to full credit).
    pub(crate) fn delegate_credit_micros(delegate: &DelegateKey) -> Option<i64> {
        schedule_lock()
            .delegate_budgets
            .get(delegate)
            .map(|b| b.credit_micros)
    }

    /// How many delegates hold a budget entry. Used to pin the GC.
    pub(crate) fn budget_entries() -> usize {
        schedule_lock().delegate_budgets.len()
    }

    /// How many leases are currently recorded as deferred. Used to pin that
    /// the map is emptied by delivery rather than only added to.
    pub(crate) fn deferral_entries() -> usize {
        schedule_lock().deferrals.len()
    }

    /// Set the node's credit directly, so a reserve or debt condition can be
    /// provoked without burning wall clock.
    pub(crate) fn set_node_credit(micros: i64) {
        schedule_lock().node_budget.credit_micros = micros;
    }

    /// Drain the node's credit so a budget refusal can be provoked.
    pub(crate) fn drain_node_credit() {
        schedule_lock().node_budget.credit_micros = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;
    use std::sync::Mutex as StdMutex;

    use freenet_stdlib::prelude::CodeHash;

    /// A delegate key that differs from every other `key(n)`.
    fn key(n: u8) -> DelegateKey {
        DelegateKey::new([n; 32], CodeHash::new([n; 32]))
    }

    /// A durable row's key as the test store holds it: the 64-byte delegate
    /// identity, then the tag.
    type RowKey = (Vec<u8>, Vec<u8>);

    /// An in-memory stand-in for the durable half, so a test can assert that
    /// BOTH representations moved — the property this module exists for.
    #[derive(Default)]
    struct RecordingStore {
        rows: StdMutex<StdHashMap<RowKey, u64>>,
        /// When set, every write and read fails. Models a backend outage.
        broken: bool,
    }

    impl RecordingStore {
        fn row_key(delegate: &DelegateKey, tag: &[u8]) -> RowKey {
            let mut id = delegate.bytes().to_vec();
            id.extend_from_slice(delegate.code_hash().as_ref());
            (id, tag.to_vec())
        }

        fn len(&self) -> usize {
            self.rows.lock().unwrap().len()
        }

        fn contains(&self, delegate: &DelegateKey, tag: &[u8]) -> bool {
            self.rows
                .lock()
                .unwrap()
                .contains_key(&Self::row_key(delegate, tag))
        }

        fn due_for(&self, delegate: &DelegateKey, tag: &[u8]) -> Option<u64> {
            self.rows
                .lock()
                .unwrap()
                .get(&Self::row_key(delegate, tag))
                .copied()
        }

        fn seed(&self, delegate: &DelegateKey, tag: &[u8], due: u64) {
            self.rows
                .lock()
                .unwrap()
                .insert(Self::row_key(delegate, tag), due);
        }
    }

    impl DelegateWakeupPersistence for RecordingStore {
        fn persist_wakeup(
            &self,
            delegate: &DelegateKey,
            tag: &[u8],
            due_millis: u64,
        ) -> Result<(), String> {
            if self.broken {
                return Err("backend is down".to_string());
            }
            self.rows
                .lock()
                .unwrap()
                .insert(Self::row_key(delegate, tag), due_millis);
            Ok(())
        }

        fn forget_wakeup(&self, delegate: &DelegateKey, tag: &[u8]) {
            self.rows
                .lock()
                .unwrap()
                .remove(&Self::row_key(delegate, tag));
        }

        fn forget_wakeups_for_delegate(&self, delegate: &DelegateKey) {
            let (id, _) = Self::row_key(delegate, &[]);
            self.rows.lock().unwrap().retain(|(d, _), _| d != &id);
        }

        fn load_wakeups(&self) -> Result<Vec<PersistedWakeup>, String> {
            if self.broken {
                return Err("backend is down".to_string());
            }
            // The row key stores the 64-byte delegate identity; rebuild the
            // `DelegateKey` from it exactly as the redb backend does.
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .map(|((id, tag), due)| {
                    let mut k = [0u8; 32];
                    let mut h = [0u8; 32];
                    k.copy_from_slice(&id[..32]);
                    h.copy_from_slice(&id[32..64]);
                    (DelegateKey::new(k, CodeHash::new(h)), tag.clone(), *due)
                })
                .collect())
        }
    }

    /// Clock origin for a test. `SystemTime` because deadlines are durable;
    /// `Instant` because budgets must not be mintable by a wall-clock jump.
    fn clocks() -> (SystemTime, Instant) {
        (
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            Instant::now(),
        )
    }

    fn fresh() -> (RecordingStore, SystemTime, Instant) {
        let (now, mono) = clocks();
        test_support::reset(mono);
        (RecordingStore::default(), now, mono)
    }

    // -----------------------------------------------------------------------
    // 1. Bytes.
    // -----------------------------------------------------------------------

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_tag_over_the_cap_is_refused_and_one_at_the_cap_is_not() {
        let (db, now, mono) = fresh();
        // The BOUNDARY, not just the refusal: a cap tested only from the
        // outside is equally consistent with a cap one byte too tight.
        let exactly = vec![b'x'; MAX_WAKEUP_TAG_BYTES];
        assert_eq!(
            schedule(
                Some(&db),
                &key(1),
                &exactly,
                Duration::from_secs(60),
                now,
                mono
            ),
            Ok(())
        );

        let too_big = vec![b'x'; MAX_WAKEUP_TAG_BYTES + 1];
        assert_eq!(
            schedule(
                Some(&db),
                &key(1),
                &too_big,
                Duration::from_secs(60),
                now,
                mono
            ),
            Err(WakeupRefusal::TagTooLong)
        );
        // Refused in BOTH representations: an over-cap tag must not leave a
        // durable row behind for boot restore to resurrect.
        assert!(!db.contains(&key(1), &too_big));
        assert_eq!(test_support::outstanding(), 1);
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_delay_below_the_floor_is_refused_rather_than_clamped() {
        let (db, now, mono) = fresh();
        // freenet-stdlib CLAMPS this up to MIN_WAKEUP_DELAY before calling.
        // The host refuses instead, so a delegate that bypassed the wrapper is
        // TOLD rather than silently given something it did not ask for.
        assert_eq!(
            schedule(Some(&db), &key(1), b"t", Duration::ZERO, now, mono),
            Err(WakeupRefusal::DelayTooShort)
        );
        assert_eq!(
            schedule(
                Some(&db),
                &key(1),
                b"t",
                MIN_WAKEUP_DELAY - Duration::from_millis(1),
                now,
                mono
            ),
            Err(WakeupRefusal::DelayTooShort)
        );
        assert_eq!(
            schedule(Some(&db), &key(1), b"t", MIN_WAKEUP_DELAY, now, mono),
            Ok(())
        );
        assert_eq!(db.len(), 1);
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_delay_past_the_horizon_is_refused() {
        let (db, now, mono) = fresh();
        assert_eq!(
            schedule(
                Some(&db),
                &key(1),
                b"t",
                MAX_WAKEUP_DELAY + Duration::from_secs(1),
                now,
                mono
            ),
            Err(WakeupRefusal::DelayTooLong)
        );
        assert_eq!(
            schedule(Some(&db), &key(1), b"t", MAX_WAKEUP_DELAY, now, mono),
            Ok(())
        );
    }

    // -----------------------------------------------------------------------
    // 2. Rows.
    // -----------------------------------------------------------------------

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_delegate_at_its_lease_cap_is_refused_with_its_own_code() {
        let (db, now, mono) = fresh();
        for i in 0..MAX_WAKEUPS_PER_DELEGATE {
            assert_eq!(
                schedule(
                    Some(&db),
                    &key(1),
                    format!("tag-{i}").as_bytes(),
                    Duration::from_secs(60),
                    now,
                    mono
                ),
                Ok(()),
                "lease {i} should be granted"
            );
        }
        assert_eq!(
            test_support::outstanding_for(&key(1)),
            MAX_WAKEUPS_PER_DELEGATE
        );

        assert_eq!(
            schedule(
                Some(&db),
                &key(1),
                b"one-too-many",
                Duration::from_secs(60),
                now,
                mono
            ),
            Err(WakeupRefusal::DelegateFull)
        );
        // A DIFFERENT delegate is unaffected: the per-delegate bound must not
        // be the node bound wearing a per-delegate label (#5597 property 2).
        assert_eq!(
            schedule(
                Some(&db),
                &key(2),
                b"mine",
                Duration::from_secs(60),
                now,
                mono
            ),
            Ok(())
        );
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_full_node_refuses_with_a_code_distinct_from_the_per_delegate_one() {
        let (db, now, mono) = fresh();
        // Fill the node using many delegates, so the per-delegate cap cannot
        // be what refuses. 64 delegates x 16 leases = MAX_WAKEUPS_PER_NODE.
        let per = MAX_WAKEUPS_PER_DELEGATE;
        let delegates = MAX_WAKEUPS_PER_NODE / per;
        for d in 0..delegates {
            for i in 0..per {
                assert_eq!(
                    schedule(
                        Some(&db),
                        &key(d as u8),
                        format!("tag-{i}").as_bytes(),
                        Duration::from_secs(3600),
                        now,
                        mono
                    ),
                    Ok(())
                );
            }
        }
        assert_eq!(test_support::outstanding(), MAX_WAKEUPS_PER_NODE);

        let newcomer = key(200);
        assert_eq!(
            schedule(
                Some(&db),
                &newcomer,
                b"first",
                Duration::from_secs(60),
                now,
                mono
            ),
            Err(WakeupRefusal::NodeFull),
            "a newcomer holding no leases must be told the NODE is full, not that it is"
        );
        assert_ne!(
            WakeupRefusal::NodeFull.code(),
            WakeupRefusal::DelegateFull.code(),
            "the two call for opposite responses and must not collapse"
        );
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn rearming_a_tag_replaces_its_lease_instead_of_taking_another() {
        let (db, now, mono) = fresh();
        // The property that lets a weekly-rotation delegate hold ONE lease for
        // its whole life rather than accumulating one per rotation.
        for _ in 0..(MAX_WAKEUPS_PER_DELEGATE * 4) {
            assert_eq!(
                schedule(
                    Some(&db),
                    &key(1),
                    b"rotate",
                    Duration::from_secs(60),
                    now,
                    mono
                ),
                Ok(())
            );
        }
        assert_eq!(test_support::outstanding_for(&key(1)), 1);
        assert_eq!(db.len(), 1);

        // And the DURABLE deadline moves with the in-memory one, or a restart
        // would restore the superseded deadline.
        assert_eq!(
            schedule(
                Some(&db),
                &key(1),
                b"rotate",
                Duration::from_secs(600),
                now,
                mono
            ),
            Ok(())
        );
        assert_eq!(
            db.due_for(&key(1), b"rotate"),
            Some(to_millis(now) + 600_000)
        );
    }

    // -----------------------------------------------------------------------
    // 3. Loop occupancy.
    // -----------------------------------------------------------------------

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_delegate_that_has_spent_its_duty_budget_is_refused_and_told_why() {
        let (db, now, mono) = fresh();
        assert_eq!(
            schedule(Some(&db), &key(1), b"t", Duration::from_secs(60), now, mono),
            Ok(()),
            "with credit, the lease is granted"
        );

        // One run longer than the whole per-delegate burst. The node's burst is
        // six times larger, so the NODE still has credit and the refusal below
        // is unambiguously the per-delegate one.
        charge_run(
            &key(1),
            Duration::from_micros(DELEGATE_DUTY_BURST_MICROS + 1),
            mono,
        );
        assert_eq!(
            test_support::delegate_credit_micros(&key(1)),
            Some(-1),
            "the overshoot is CARRIED as debt, not discarded — see DutyBudget::credit_micros"
        );
        assert!(test_support::node_credit_micros() > 0);

        assert_eq!(
            schedule(
                Some(&db),
                &key(1),
                b"t2",
                Duration::from_secs(60),
                now,
                mono
            ),
            Err(WakeupRefusal::DelegateBudget)
        );
        // A different delegate is not punished for this one's spending.
        assert_eq!(
            schedule(Some(&db), &key(2), b"t", Duration::from_secs(60), now, mono),
            Ok(())
        );
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_spent_node_budget_refuses_even_a_delegate_with_full_credit() {
        let (db, now, mono) = fresh();
        test_support::drain_node_credit();
        let newcomer = key(9);
        assert_eq!(
            test_support::delegate_credit_micros(&newcomer),
            None,
            "no entry means full credit"
        );
        assert_eq!(
            schedule(
                Some(&db),
                &newcomer,
                b"t",
                Duration::from_secs(60),
                now,
                mono
            ),
            Err(WakeupRefusal::NodeBudget)
        );
        assert_ne!(
            WakeupRefusal::NodeBudget.code(),
            WakeupRefusal::DelegateBudget.code()
        );
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn duty_credit_refills_with_wall_clock_at_the_stated_share() {
        let (db, now, mono) = fresh();
        charge_run(
            &key(1),
            Duration::from_micros(DELEGATE_DUTY_BURST_MICROS),
            mono,
        );
        assert_eq!(test_support::delegate_credit_micros(&key(1)), Some(0));

        // A minute later, 1% of a minute is 600 ms of credit.
        let later = mono + Duration::from_secs(60);
        assert_eq!(
            schedule(
                Some(&db),
                &key(1),
                b"t",
                Duration::from_secs(60),
                now,
                later
            ),
            Ok(())
        );
        assert_eq!(
            test_support::delegate_credit_micros(&key(1)),
            Some(60_000_000i64 / DELEGATE_DUTY_DIVISOR as i64)
        );
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_budget_entry_is_dropped_once_it_is_indistinguishable_from_a_fresh_one() {
        let (db, now, mono) = fresh();
        assert_eq!(
            schedule(Some(&db), &key(1), b"t", Duration::from_secs(1), now, mono),
            Ok(())
        );
        assert_eq!(test_support::budget_entries(), 1);

        // Fire it. The entry is at full credit and holds no leases, so it now
        // says nothing a missing entry would not and the GC drops it — which is
        // what time-bounds this map per the AGENTS.md rule against
        // permanently-refreshable entries.
        let fired = take_due(Some(&db), now + Duration::from_secs(2), 10);
        assert_eq!(fired.len(), 1);
        assert_eq!(test_support::budget_entries(), 0);
        assert_eq!(test_support::outstanding(), 0);
        assert_eq!(db.len(), 0, "firing releases the durable row too");
    }

    // -----------------------------------------------------------------------
    // Refusal reporting. A refusal path that logs unconditionally hands the
    // refused party a log-volume amplifier, so what is reported is the
    // TRANSITION, and these assert on the transition itself rather than on
    // captured output: `tracing` resolves each callsite's `Interest` once per
    // PROCESS against whichever thread reaches it first (#5314), so a
    // log-capture assertion here would pass or fail depending on what else ran
    // beside it.
    // -----------------------------------------------------------------------

    #[test]
    fn a_refusal_is_reported_once_and_a_repeat_is_not() {
        let mut budget = DutyBudget::new(DELEGATE_DUTY_BURST_MICROS, Instant::now());
        assert!(budget.note_refused(), "the first refusal is the transition");
        assert!(
            !budget.note_refused(),
            "a delegate hammering the import must not be able to report again"
        );
        assert!(!budget.note_refused());
        assert!(
            budget.note_granted(),
            "the grant ends the run of refusals and is worth reporting once"
        );
        assert!(
            !budget.note_granted(),
            "an ordinary grant reports nothing"
        );
        assert!(
            budget.note_refused(),
            "a refusal after a grant is a NEW episode, not a repeat"
        );
    }

    #[test]
    fn a_node_wide_refusal_transitions_on_the_node_not_on_a_delegate() {
        // A local `Schedule`, so this says nothing about the global one and
        // needs no serialisation.
        let mut sched = Schedule::new(Instant::now());
        assert!(sched.note_node_refused());
        assert!(!sched.note_node_refused());
        sched.node_refusing = false;
        assert!(
            sched.note_node_refused(),
            "a grant clears the flag, so the next refusal reports again"
        );
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_refused_delegate_still_cannot_grow_the_budget_map() {
        // The guard that reporting transitions could plausibly have broken. The
        // obvious way to remember who is being refused is a set keyed by
        // delegate, and a refused delegate would then add an entry to it — the
        // memory version of the log amplifier, and the reason the bit lives on
        // an entry that already exists rather than in a map of its own.
        let (db, now, mono) = fresh();
        test_support::drain_node_credit();
        let newcomer = key(9);
        for _ in 0..32 {
            assert_eq!(
                schedule(
                    Some(&db),
                    &newcomer,
                    b"t",
                    Duration::from_secs(60),
                    now,
                    mono
                ),
                Err(WakeupRefusal::NodeBudget)
            );
        }
        assert_eq!(
            test_support::budget_entries(),
            0,
            "32 refusals must leave no per-delegate state behind"
        );
    }

    // -----------------------------------------------------------------------
    // Firing.
    // -----------------------------------------------------------------------

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn only_past_deadlines_fire_and_they_fire_in_deadline_order() {
        let (db, now, mono) = fresh();
        for (tag, secs) in [
            (b"c".as_slice(), 30u64),
            (b"a".as_slice(), 10),
            (b"b".as_slice(), 20),
            (b"d".as_slice(), 600),
        ] {
            assert_eq!(
                schedule(
                    Some(&db),
                    &key(1),
                    tag,
                    Duration::from_secs(secs),
                    now,
                    mono
                ),
                Ok(())
            );
        }
        let fired = take_due(Some(&db), now + Duration::from_secs(35), 10);
        let tags: Vec<&[u8]> = fired.iter().map(|w| w.tag.as_slice()).collect();
        assert_eq!(
            tags,
            vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
        );
        assert_eq!(
            test_support::outstanding(),
            1,
            "the 600s lease is untouched"
        );
        assert!(db.contains(&key(1), b"d"));
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn the_fire_batch_is_bounded_and_the_remainder_stays_due() {
        let (db, now, mono) = fresh();
        for i in 0..10 {
            assert_eq!(
                schedule(
                    Some(&db),
                    &key(1),
                    format!("t{i}").as_bytes(),
                    Duration::from_secs(1),
                    now,
                    mono
                ),
                Ok(())
            );
        }
        let later = now + Duration::from_secs(2);
        assert_eq!(take_due(Some(&db), later, 4).len(), 4);
        assert_eq!(test_support::outstanding(), 6);
        assert_eq!(
            next_due_in(later),
            Some(Duration::ZERO),
            "the remainder is still due, so the loop drains rather than sleeps"
        );
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn an_idle_node_with_no_wakeups_has_no_deadline_to_wake_for() {
        let (_db, now, _mono) = fresh();
        assert_eq!(next_due_in(now), None);
    }

    // -----------------------------------------------------------------------
    // Deferral (the parked-delegate path).
    // -----------------------------------------------------------------------

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn deferral_bounds_itself_without_reference_to_park_ttl() {
        let (db, now, mono) = fresh();
        assert_eq!(
            schedule(Some(&db), &key(1), b"t", Duration::from_secs(1), now, mono),
            Ok(())
        );
        let mut clock = now + Duration::from_secs(2);
        let mut deferrals = 0u32;
        loop {
            let mut due = take_due(Some(&db), clock, 10);
            assert_eq!(due.len(), 1, "the lease should still be in hand");
            let wakeup = due.remove(0);
            if !defer(Some(&db), &wakeup, WAKEUP_PARK_RETRY, clock) {
                break;
            }
            deferrals += 1;
            assert!(
                deferrals <= MAX_WAKEUP_DEFERRALS + 1,
                "deferral did not terminate"
            );
            clock += WAKEUP_PARK_RETRY + Duration::from_millis(1);
        }
        assert_eq!(deferrals, MAX_WAKEUP_DEFERRALS);
        assert_eq!(
            test_support::outstanding(),
            0,
            "the dropped lease is released"
        );
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_deferral_grants_nothing_new_and_moves_both_representations() {
        let (db, now, mono) = fresh();
        assert_eq!(
            schedule(Some(&db), &key(1), b"t", Duration::from_secs(1), now, mono),
            Ok(())
        );
        let clock = now + Duration::from_secs(2);
        let due = take_due(Some(&db), clock, 10).remove(0);
        assert_eq!(test_support::outstanding(), 0);

        assert!(defer(Some(&db), &due, WAKEUP_PARK_RETRY, clock));
        assert_eq!(
            test_support::outstanding_for(&key(1)),
            1,
            "the lease is back, and it is the SAME one"
        );
        assert_eq!(
            db.due_for(&key(1), b"t"),
            Some(to_millis(clock) + WAKEUP_PARK_RETRY.as_millis() as u64)
        );
        assert_eq!(next_due_in(clock), Some(WAKEUP_PARK_RETRY));
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_fresh_grant_resets_the_deferral_count() {
        let (db, now, mono) = fresh();
        assert_eq!(
            schedule(Some(&db), &key(1), b"t", Duration::from_secs(1), now, mono),
            Ok(())
        );
        let clock = now + Duration::from_secs(2);
        let due = take_due(Some(&db), clock, 10).remove(0);
        assert!(defer(Some(&db), &due, WAKEUP_PARK_RETRY, clock));

        // The delegate re-arms the same tag itself. That is a NEW lease, not a
        // continuation of the one that kept missing its delegate.
        assert_eq!(
            schedule(
                Some(&db),
                &key(1),
                b"t",
                Duration::from_secs(1),
                clock,
                mono
            ),
            Ok(())
        );
        let again = take_due(Some(&db), clock + Duration::from_secs(2), 10).remove(0);
        assert_eq!(again.attempts, 0);
    }

    // -----------------------------------------------------------------------
    // Teardown and boot restore.
    // -----------------------------------------------------------------------

    /// Delivery must EMPTY the deferral map, not merely read it.
    ///
    /// Found by mutation: `a_fresh_grant_resets_the_deferral_count` exercises
    /// the GRANT path's `deferrals.remove`, so `take_due` reading instead of
    /// removing left it green. The property that was unguarded is not the
    /// reset, it is the map's BOUND: moving the count out with the lease is
    /// what keeps `deferrals` holding only currently-deferred ids, which is
    /// what the field's own comment claims and what lets it have no sweep. If
    /// delivery only read the count, one entry would survive every
    /// deferred-then-fired lease and the map would grow without bound, keyed by
    /// (delegate, tag) and reachable by any delegate that gets parked.
    ///
    /// FALSIFY: make `take_due` use `deferrals.get` instead of `remove`.
    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_deferred_lease_that_fires_leaves_no_deferral_entry_behind() {
        let (db, now, mono) = fresh();
        assert_eq!(
            schedule(Some(&db), &key(1), b"t", Duration::from_secs(1), now, mono),
            Ok(())
        );
        let clock = now + Duration::from_secs(2);
        let due = take_due(Some(&db), clock, 10).remove(0);
        assert!(defer(Some(&db), &due, WAKEUP_PARK_RETRY, clock));
        assert_eq!(
            test_support::deferral_entries(),
            1,
            "a deferred lease is recorded while it waits"
        );

        let again = take_due(
            Some(&db),
            clock + WAKEUP_PARK_RETRY + Duration::from_secs(1),
            10,
        )
        .remove(0);
        assert_eq!(again.attempts, 1, "the count rides out with the lease");
        assert_eq!(
            test_support::deferral_entries(),
            0,
            "and must be GONE from the map: an entry that survives delivery is \
             one leaked per deferred-then-fired lease, forever"
        );
    }

    /// A delegate at its row cap can still re-arm a tag it already holds.
    ///
    /// Found by mutation, indirectly: forcing `renewing` to false left
    /// `rearming_a_tag_replaces_its_lease_instead_of_taking_another` green,
    /// because replacement is done unconditionally by `remove_lease` and does
    /// not depend on that flag at all. What the flag actually gates is this,
    /// and nothing covered it.
    ///
    /// It matters because the caps are per-delegate: a delegate that fills its
    /// cap with long-lived rotations could otherwise never refresh any of them,
    /// so every one of its wakeups would expire at the moment it is busiest,
    /// and the refusal it received would say it was full rather than that it
    /// had asked for something forbidden.
    ///
    /// FALSIFY: check a renewal against the row caps.
    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_delegate_at_its_cap_can_still_rearm_a_tag_it_already_holds() {
        let (db, now, mono) = fresh();
        for i in 0..MAX_WAKEUPS_PER_DELEGATE {
            assert_eq!(
                schedule(
                    Some(&db),
                    &key(1),
                    format!("t{i}").as_bytes(),
                    Duration::from_secs(600),
                    now,
                    mono
                ),
                Ok(())
            );
        }
        assert_eq!(
            test_support::outstanding_for(&key(1)),
            MAX_WAKEUPS_PER_DELEGATE
        );

        // The cap is real: a NEW tag is refused.
        assert_eq!(
            schedule(
                Some(&db),
                &key(1),
                b"new",
                Duration::from_secs(600),
                now,
                mono
            ),
            Err(WakeupRefusal::DelegateFull)
        );

        // An EXISTING tag is not, and both representations move with it.
        assert_eq!(
            schedule(
                Some(&db),
                &key(1),
                b"t0",
                Duration::from_secs(1200),
                now,
                mono
            ),
            Ok(())
        );
        assert_eq!(
            test_support::outstanding_for(&key(1)),
            MAX_WAKEUPS_PER_DELEGATE,
            "a renewal must not take a second row"
        );
        assert_eq!(
            db.due_for(&key(1), b"t0"),
            Some(to_millis(now) + 1_200_000),
            "the durable deadline moves with the renewal"
        );
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn forgetting_a_delegate_clears_both_representations_and_only_its_own() {
        let (db, now, mono) = fresh();
        for tag in [b"a".as_slice(), b"b".as_slice()] {
            assert_eq!(
                schedule(Some(&db), &key(1), tag, Duration::from_secs(60), now, mono),
                Ok(())
            );
        }
        assert_eq!(
            schedule(
                Some(&db),
                &key(2),
                b"keep",
                Duration::from_secs(60),
                now,
                mono
            ),
            Ok(())
        );

        forget_delegate(Some(&db), &key(1));
        assert_eq!(test_support::outstanding_for(&key(1)), 0);
        assert!(!db.contains(&key(1), b"a"));
        assert!(!db.contains(&key(1), b"b"));
        // The durable half is the half that matters here: a row left behind
        // would be restored on every subsequent boot, firing forever into a
        // delegate that no longer exists.
        assert!(db.contains(&key(2), b"keep"));
        assert_eq!(test_support::outstanding_for(&key(2)), 1);
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn boot_restore_reinstates_leases_and_drops_rows_whose_delegate_is_gone() {
        let (db, now, mono) = fresh();
        let live = key(1);
        let gone = key(2);
        db.seed(&live, b"weekly", to_millis(now) + 600_000);
        db.seed(&gone, b"orphan", to_millis(now) + 600_000);

        let outcome = restore(&db, 1, |k| k == &live, now, mono).expect("restore should read");
        assert_eq!(outcome.restored, 1);
        assert_eq!(outcome.orphaned, 1);
        assert_eq!(test_support::outstanding_for(&live), 1);
        assert_eq!(test_support::outstanding_for(&gone), 0);
        assert!(
            !db.contains(&gone, b"orphan"),
            "an orphan row must be deleted, not merely skipped, or every later boot replays it"
        );
        assert!(db.contains(&live, b"weekly"));
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn boot_restore_smears_an_overdue_backlog_instead_of_firing_it_at_once() {
        let (db, now, mono) = fresh();
        let d = key(1);
        for i in 0..8 {
            db.seed(&d, format!("t{i}").as_bytes(), to_millis(now) - 86_400_000);
        }
        let outcome = restore(&db, 1, |_| true, now, mono).expect("restore should read");
        assert_eq!(outcome.restored, 8);
        assert_eq!(outcome.overdue, 8);
        // Not all at `now`: a node down for a day must not boot into its whole
        // backlog in one loop iteration.
        assert_eq!(take_due(Some(&db), now, 100).len(), 1);
        assert_eq!(test_support::outstanding_for(&d), 7);
        assert_eq!(
            take_due(Some(&db), now + BOOT_SPREAD, 100).len(),
            7,
            "and the whole backlog is inside the spread window"
        );
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn boot_restore_refuses_to_read_a_failing_store_as_an_empty_schedule() {
        let (_db, now, mono) = fresh();
        let broken = RecordingStore {
            broken: true,
            ..Default::default()
        };
        // The distinction that matters: `Ok(empty)` here would be
        // indistinguishable from a clean boot while silently losing every
        // delegate's timer.
        assert!(restore(&broken, 1, |_| true, now, mono).is_err());
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_durable_write_that_fails_refuses_the_lease_in_both_representations() {
        let (_db, now, mono) = fresh();
        let broken = RecordingStore {
            broken: true,
            ..Default::default()
        };
        assert_eq!(
            schedule(
                Some(&broken),
                &key(1),
                b"t",
                Duration::from_secs(60),
                now,
                mono
            ),
            Err(WakeupRefusal::Storage)
        );
        assert_eq!(
            test_support::outstanding(),
            0,
            "granting in memory only would be a lease a restart silently drops"
        );
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_node_with_no_durable_store_still_grants_leases() {
        let (_db, now, mono) = fresh();
        // sqlite and the mock runtime. Documented degradation: the lease
        // survives the process and not a restart. Refusing here would make the
        // primitive unavailable under `--features sqlite` and in every mock
        // test.
        assert_eq!(
            schedule(
                None::<&RecordingStore>,
                &key(1),
                b"t",
                Duration::from_secs(60),
                now,
                mono
            ),
            Ok(())
        );
        assert_eq!(test_support::outstanding(), 1);
    }

    // -----------------------------------------------------------------------
    // The codes themselves.
    // -----------------------------------------------------------------------

    /// Every refusal, so a new variant cannot be added without appearing here.
    fn all_refusals() -> [WakeupRefusal; 8] {
        use WakeupRefusal::*;
        let all = [
            TagTooLong,
            DelayTooShort,
            DelayTooLong,
            DelegateFull,
            NodeFull,
            DelegateBudget,
            NodeBudget,
            Storage,
        ];
        // Destructured with no `..`, so adding a variant fails to COMPILE here
        // rather than silently escaping both tests below.
        let [_, _, _, _, _, _, _, _] = all;
        all
    }

    #[test]
    fn every_refusal_maps_to_a_distinct_negative_code() {
        let mut seen = std::collections::HashSet::new();
        for refusal in all_refusals() {
            let code = refusal.code();
            assert!(
                code < 0,
                "{refusal:?} must be negative: the guest wrapper maps every code >= 0 to Ok(())"
            );
            assert!(
                seen.insert(code),
                "{refusal:?} collides with another refusal on code {code}; collapsing two \
                 refusals into one is the #5565 defect this primitive exists not to repeat"
            );
        }
    }

    #[test]
    fn the_codes_cannot_collide_with_freenet_stdlibs_published_range() {
        // stdlib's `delegate_host::error_codes` occupies -1..-10 and -20..-24.
        // A collision would have a delegate read one refusal as another, which
        // is worse than having no code at all.
        for refusal in all_refusals() {
            assert!(
                refusal.code() <= -40,
                "{refusal:?} at {} is inside the range stdlib may grow into",
                refusal.code()
            );
        }
    }

    /// #5597 property 2, as a COMPILE-TIME check.
    ///
    /// The delegate pin cap violates this — its per-delegate figure is five
    /// times the node figure, so the first delegate to ask can take the whole
    /// node allowance, and a per-principal bound at or above the node bound is
    /// decorative. These are constants, so the relation can be enforced where
    /// it cannot be got wrong at all rather than where a test has to be run.
    #[test]
    fn the_per_delegate_bounds_sit_strictly_below_the_node_wide_ones() {
        const {
            assert!(MAX_WAKEUPS_PER_DELEGATE < MAX_WAKEUPS_PER_NODE);
            // A LARGER divisor is a SMALLER share.
            assert!(DELEGATE_DUTY_DIVISOR > NODE_DUTY_DIVISOR);
            assert!(DELEGATE_DUTY_BURST_MICROS < NODE_DUTY_BURST_MICROS);
            assert!(NODE_RENEWAL_RESERVE_MICROS < NODE_DUTY_BURST_MICROS);
        }
    }

    // -----------------------------------------------------------------------
    // Which refusal wins when BOTH bounds bind.
    // -----------------------------------------------------------------------

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_delegate_at_its_own_cap_on_a_full_node_is_told_it_is_at_its_own_cap() {
        let (db, now, mono) = fresh();
        // Fill the node to its cap, giving key(0) a full personal allocation
        // too, so BOTH bounds bind at once for that delegate.
        let per = MAX_WAKEUPS_PER_DELEGATE;
        for d in 0..(MAX_WAKEUPS_PER_NODE / per) {
            for i in 0..per {
                assert_eq!(
                    schedule(
                        Some(&db),
                        &key(d as u8),
                        format!("tag-{i}").as_bytes(),
                        Duration::from_secs(3600),
                        now,
                        mono
                    ),
                    Ok(())
                );
            }
        }
        assert_eq!(test_support::outstanding(), MAX_WAKEUPS_PER_NODE);
        assert_eq!(test_support::outstanding_for(&key(0)), per);

        // The ONE assertion this test exists for. Both caps are binding; the
        // delegate must be told the one that is ITS fault, because that is the
        // one it can act on. Telling it "the node is full" — blameless, retry
        // later — would have it retry forever against a limit that is its own.
        assert_eq!(
            schedule(
                Some(&db),
                &key(0),
                b"mine",
                Duration::from_secs(3600),
                now,
                mono
            ),
            Err(WakeupRefusal::DelegateFull),
            "when both bounds bind, the caller's OWN limit is the one reported"
        );
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_delegate_in_debt_on_a_node_in_debt_is_told_about_its_own_debt() {
        let (db, now, mono) = fresh();
        // Put BOTH budgets into debt with one very expensive run.
        charge_run(
            &key(1),
            Duration::from_micros(NODE_DUTY_BURST_MICROS * 2),
            mono,
        );
        assert!(test_support::node_credit_micros() <= 0);
        assert!(test_support::delegate_credit_micros(&key(1)).unwrap() <= 0);

        assert_eq!(
            schedule(Some(&db), &key(1), b"t", Duration::from_secs(60), now, mono),
            Err(WakeupRefusal::DelegateBudget),
            "when both budgets are spent, the delegate is told about ITS OWN — the \
             only one it can do anything about"
        );
    }

    // -----------------------------------------------------------------------
    // Renewal capacity is reserved (#5597 property 4).
    // -----------------------------------------------------------------------

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_renewal_is_admitted_from_reserved_node_capacity_that_refuses_a_new_lease() {
        let (db, now, mono) = fresh();
        let incumbent = key(1);
        assert_eq!(
            schedule(
                Some(&db),
                &incumbent,
                b"weekly",
                Duration::from_secs(60),
                now,
                mono
            ),
            Ok(())
        );

        // Drive the node's credit into the reserve band: positive, but at or
        // below the floor a NEW lease must leave intact.
        test_support::set_node_credit(NODE_RENEWAL_RESERVE_MICROS as i64);

        assert_eq!(
            schedule(
                Some(&db),
                &key(2),
                b"newcomer",
                Duration::from_secs(60),
                now,
                mono
            ),
            Err(WakeupRefusal::NodeBudget),
            "a NEW lease must not eat the renewal reserve"
        );
        assert_eq!(
            schedule(
                Some(&db),
                &incumbent,
                b"weekly",
                Duration::from_secs(604_800),
                now,
                mono
            ),
            Ok(()),
            "the incumbent's re-arm draws on the reserve: a newcomer refused asks \
             again, while an incumbent refused STOPS — its next run was what would \
             have re-armed it"
        );
        assert_eq!(test_support::outstanding_for(&incumbent), 1);
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn the_reserve_does_not_exempt_a_renewal_from_its_own_budget() {
        let (db, now, mono) = fresh();
        let spinner = key(1);
        assert_eq!(
            schedule(
                Some(&db),
                &spinner,
                b"loop",
                Duration::from_secs(60),
                now,
                mono
            ),
            Ok(())
        );
        // The delegate spends its own allowance. The node still has plenty.
        charge_run(
            &spinner,
            Duration::from_micros(DELEGATE_DUTY_BURST_MICROS + 1),
            mono,
        );
        assert!(test_support::node_credit_micros() > NODE_RENEWAL_RESERVE_MICROS as i64);

        // Without this, the reserve would be a hole: a delegate re-arming in a
        // tight loop is exactly a renewal, and exempting renewals outright would
        // remove the only bound on it.
        assert_eq!(
            schedule(
                Some(&db),
                &spinner,
                b"loop",
                Duration::from_secs(60),
                now,
                mono
            ),
            Err(WakeupRefusal::DelegateBudget),
            "a renewal draws on reserved NODE capacity, never on a waiver of its own"
        );
    }

    // -----------------------------------------------------------------------
    // The fire-time affordability check, which is what bounds the overshoot.
    // -----------------------------------------------------------------------

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_run_is_charged_as_debt_so_the_next_one_is_not_affordable() {
        let (_db, _now, mono) = fresh();
        let d = key(1);
        assert_eq!(affordability(&d, mono), Ok(()), "a fresh delegate can run");

        // One `max_execution_seconds` run against a 5 s burst leaves it just
        // solvent; a second puts it in debt. Were the debt discarded at zero,
        // 16 leases would each find a budget freshly reset and the delegate
        // would get 16 x 5 s = 80 s of loop time on one solvency check.
        charge_run(&d, Duration::from_secs(5), mono);
        charge_run(&d, Duration::from_secs(5), mono);
        let wait = affordability(&d, mono).expect_err("a delegate in debt must not run");
        assert!(
            wait >= Duration::from_secs(400),
            "the wait must be the real repayment time (5 s of debt at 1% is ~500 s), \
             got {wait:?}"
        );

        // And it recovers on its own, at the stated rate.
        assert_eq!(affordability(&d, mono + wait), Ok(()));
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn debt_is_bounded_so_one_pathological_run_cannot_disable_a_delegate_for_hours() {
        let (_db, _now, mono) = fresh();
        let d = key(1);
        // A guest wedged in an uninterruptible host call (#5594) can be charged
        // far more than its budget. The debt floor is what stops that becoming
        // an unbounded ban.
        charge_run(&d, Duration::from_secs(3600), mono);
        assert_eq!(
            test_support::delegate_credit_micros(&d),
            Some(-(DELEGATE_DUTY_BURST_MICROS as i64))
        );
        let wait = affordability(&d, mono).expect_err("still in debt");
        assert!(
            wait <= Duration::from_secs(510),
            "recovery must stay proportionate to one burst, got {wait:?}"
        );
    }

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_node_in_debt_defers_every_delegate_including_a_solvent_one() {
        let (_db, _now, mono) = fresh();
        let innocent = key(7);
        test_support::set_node_credit(-1_000_000);
        assert!(
            affordability(&innocent, mono).is_err(),
            "loop occupancy is a node resource; a solvent delegate still waits for it"
        );
    }

    // -----------------------------------------------------------------------
    // Deferral bounds.
    // -----------------------------------------------------------------------

    #[test]
    #[serial_test::serial(delegate_wakeups)]
    fn a_budget_deferral_waits_for_the_debt_rather_than_ticking() {
        let (db, now, mono) = fresh();
        let d = key(1);
        assert_eq!(
            schedule(Some(&db), &d, b"t", Duration::from_secs(1), now, mono),
            Ok(())
        );
        let clock = now + Duration::from_secs(2);
        let due = take_due(Some(&db), clock, 10).remove(0);

        charge_run(&d, Duration::from_secs(5), mono);
        charge_run(&d, Duration::from_secs(5), mono);
        let wait = affordability(&d, mono).expect_err("in debt");
        assert!(defer(Some(&db), &due, wait, clock));

        // One wait, not `wait / WAKEUP_PARK_RETRY` attempts — which at a 2 s
        // tick would be 250 of them against a bound of 64, i.e. a dropped
        // wakeup.
        //
        // Compared in MILLISECONDS because that is the resolution a durable
        // deadline has: `persist_wakeup` stores ms since the epoch, so the
        // sub-millisecond tail of `wait` is not something the schedule can
        // represent, and asserting on it would be asserting on the test's
        // arithmetic rather than on the behaviour.
        let scheduled = next_due_in(clock).expect("the lease is back in the schedule");
        assert_eq!(scheduled.as_millis(), wait.as_millis());
        assert!(
            scheduled >= WAKEUP_PARK_RETRY * 10,
            "a budget deferral must wait for the DEBT, not tick at the park interval"
        );
    }

    #[test]
    fn the_bounds_this_module_duplicates_still_match_freenet_stdlibs() {
        // Duplicated rather than imported (see the constants' docs), so they
        // can drift. This is what notices.
        assert_eq!(
            MAX_WAKEUP_TAG_BYTES,
            freenet_stdlib::delegate_host::MAX_WAKEUP_TAG_BYTES
        );
        assert_eq!(
            MIN_WAKEUP_DELAY,
            freenet_stdlib::delegate_host::MIN_WAKEUP_DELAY
        );
    }
}
