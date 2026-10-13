//! CONNECT admission from two separate budgets (#5858).
//!
//! An acceptor admits a CONNECT for one of two reasons, and the two are kept
//! apart so that neither can starve the other:
//!
//! 1. **Self-interest.** The candidate improves this peer's own topology. This
//!    is the pre-#5858 rule: below `min_connections` a soft Kleinberg filter,
//!    at/above it the [`ConnectionEvaluator`](crate::topology::connection_evaluator)
//!    window, which admits roughly one candidate per window. Unchanged.
//! 2. **Collective duty (onboarding).** The requester says it is short of
//!    connections ([`RequesterNeed::Short`]). Such a request is admitted from a
//!    small per-acceptor budget of [`ONBOARDING_PROVISIONAL_SLOTS`] links on
//!    probation, bypassing the evaluator window and never recorded into it, so
//!    onboarding traffic cannot raise the bar for self-interest candidates and
//!    routine top-up CONNECTs cannot use up the onboarding budget.
//!
//! A request from a well-connected requester states a bar instead
//! ([`RequesterNeed::Bar`]): only an acceptor closer to the target than the
//! requester's own nearest connection on that side helps it, so an acceptor
//! that is not closer rejects without consulting its own budget.
//!
//! A link admitted for onboarding is PROVISIONAL. After [`ONBOARDING_PROBATION`]
//! it graduates if the acceptor's self-interest would keep it, and is dropped
//! otherwise (`ConnectionManager::review_provisional_links`). While on
//! probation it is the first link a capacity-driven prune sheds (never an
//! optional topology swap).

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use super::Location;

/// Requesters holding fewer ring connections than this fraction of their own
/// `min_connections` declare themselves short ([`RequesterNeed::Short`]).
///
/// 1.0 is the design default (#5858): a peer below its own minimum degree is,
/// by its own configuration, under-connected. Lower it to narrow onboarding to
/// nearly-isolated peers if the onboarding budgets turn out to be contended by
/// the ~35% of live peers that sit below `min_connections`.
pub(crate) const NEEDY_FRACTION_OF_MIN_CONNECTIONS: f64 = 1.0;

/// Maximum number of onboarding (need-admitted) links an acceptor holds on
/// probation at once.
///
/// This is the single bound behind the onboarding path: a burst of joiners, or
/// a peer that falsely claims to be short, can occupy at most this many of an
/// acceptor's slots, each for at most [`ONBOARDING_PROBATION`]. A joiner needs
/// one ring connection to stop being isolated, and with roughly two thirds of
/// live peers at or above `min_connections` (#5858) a handful of slots per
/// acceptor is ample network-wide capacity. Two keeps the worst case at 1% of a
/// default 200-connection budget and lets one slot serve while the other is
/// waiting on a failed hole punch to be released.
pub(crate) const ONBOARDING_PROVISIONAL_SLOTS: usize = 2;

/// How long a need-admitted link stays on probation before the acceptor
/// decides whether to keep it.
///
/// Long enough for the joiner to acquire its own gap-targeted connections
/// through the link (the maintenance loop issues CONNECTs every few seconds
/// while below `min_connections`), short enough that a link the acceptor does
/// not want frees its slot for the next joiner within minutes.
pub(crate) const ONBOARDING_PROBATION: Duration = Duration::from_secs(600);

/// At most this many onboarding links graduate on their self-interest merits
/// per [`ONBOARDING_GRADUATION_WINDOW`]: graduation admits a link that never
/// faced the evaluator when it arrived, so it is capped like an admission. One
/// matches what the evaluator itself admits per window. (A link kept because
/// the acceptor would fall below `min_connections` without it, or because it
/// is a lattice edge, is not counted.)
pub(crate) const ONBOARDING_GRADUATIONS_PER_WINDOW: usize = 1;

/// The window [`ONBOARDING_GRADUATIONS_PER_WINDOW`] is counted over: the slow
/// evaluator window (5 min), the longer of the two.
pub(crate) const ONBOARDING_GRADUATION_WINDOW: Duration = Duration::from_secs(300);

/// A link on probation is a capacity-prune substitute only once it is this old.
///
/// Two pending-reservation lifetimes: long enough for the joiner to use the
/// link to issue its own CONNECTs (the maintenance loop's fast tick is a few
/// seconds) before the acceptor can shed it, so first-in-line pruning cannot
/// re-isolate a joiner the moment it lands.
pub(crate) const ONBOARDING_MIN_AGE_BEFORE_PRUNE: Duration = Duration::from_secs(120);

/// Admissions an onboarding slot may serve per probation period, on average.
///
/// Sets the refill rate of the onboarding token bucket (see
/// [`onboarding_refill_interval`]). Above 1 so a slot freed by a failed hole
/// punch (most attempts fail on the live network) can serve another joiner
/// before the probation period ends; small so a requester that repeatedly
/// claims to be short and then drops its link costs an acceptor at most a few
/// NAT traversals per slot per probation period. Releasing a slot never
/// refunds a token.
pub(crate) const ONBOARDING_ADMISSIONS_PER_SLOT_PER_PROBATION: u32 = 3;

/// One onboarding token accrues every
/// `ONBOARDING_PROBATION / (ONBOARDING_PROVISIONAL_SLOTS *
/// ONBOARDING_ADMISSIONS_PER_SLOT_PER_PROBATION)` (100 s with the defaults);
/// the bucket holds at most [`ONBOARDING_PROVISIONAL_SLOTS`] tokens, so a burst
/// of joiners can fill every slot at once.
pub(crate) fn onboarding_refill_interval() -> Duration {
    ONBOARDING_PROBATION
        / (ONBOARDING_PROVISIONAL_SLOTS as u32 * ONBOARDING_ADMISSIONS_PER_SLOT_PER_PROBATION)
}

/// A short requester holding at least this fraction of the ACCEPTOR's
/// `min_connections` is low priority: it may take an onboarding slot only while
/// [`ONBOARDING_SLOTS_KEPT_FOR_ISOLATED`] others stay free for requesters below
/// the band. A peer with no connections at all is never crowded out by peers
/// topping up from 24 to 25. Tuned by the saturated-network simulation.
pub(crate) const ONBOARDING_PRIORITY_FRACTION_OF_MIN: f64 = 0.25;

/// Slots kept free for high-priority (nearly isolated) requesters; see
/// [`ONBOARDING_PRIORITY_FRACTION_OF_MIN`].
pub(crate) const ONBOARDING_SLOTS_KEPT_FOR_ISOLATED: usize = 1;

/// Whether a short requester holding `requester_connections` ring connections
/// is high priority at an acceptor whose `min_connections` is given.
pub(crate) fn onboarding_high_priority(requester_connections: u8, min_connections: usize) -> bool {
    (requester_connections as f64)
        < (min_connections as f64 * ONBOARDING_PRIORITY_FRACTION_OF_MIN).ceil()
}

/// Whether a requester's bar (`RequesterNeed::Bar`) is enforced.
///
/// OFF (#5858, owner decision): requesters still send their bar and it still
/// reaches telemetry, but an acceptor admits a requester with a bar exactly
/// like a legacy one, until the simulation shows enforcing it is a net
/// benefit. Without the early stop at relays (not implemented, see
/// `RelayState::handle_request`) enforcement only adds rejections and uphill
/// hops. A request enforcement WOULD have refused is counted
/// (`AdmissionStats::bar_unmet_not_enforced`), so the cost of turning it on is
/// visible before anyone does. Read through [`requester_bar_enforced`].
pub(crate) const ENFORCE_REQUESTER_BAR: bool = false;

#[cfg(test)]
thread_local! {
    /// Test-only override of [`ENFORCE_REQUESTER_BAR`] for the current thread,
    /// so the enforced path stays tested while it ships off. `cfg(test)` only:
    /// nothing outside this crate's unit tests sets it.
    static ENFORCE_REQUESTER_BAR_OVERRIDE: std::cell::Cell<Option<bool>> =
        const { std::cell::Cell::new(None) };
}

/// Override [`ENFORCE_REQUESTER_BAR`] on the current thread (`None` restores
/// it). Unit tests only.
#[cfg(test)]
pub(crate) fn set_requester_bar_enforced(enforced: Option<bool>) {
    ENFORCE_REQUESTER_BAR_OVERRIDE.with(|c| c.set(enforced));
}

/// Whether the requester's bar is enforced on this thread: the test override
/// if set, else [`ENFORCE_REQUESTER_BAR`].
pub(crate) fn requester_bar_enforced() -> bool {
    #[cfg(test)]
    {
        if let Some(enforced) = ENFORCE_REQUESTER_BAR_OVERRIDE.with(|c| c.get()) {
            return enforced;
        }
    }
    ENFORCE_REQUESTER_BAR
}

/// Ring distance used as the bar when the requester holds no connection on the
/// target's side: half the ring is the largest possible distance, so every
/// acceptor meets it.
pub(crate) const UNCONSTRAINED_BAR: f64 = 0.5;

/// What the requester of a CONNECT says it needs (#5858).
///
/// In memory only (`#[serde(skip)]` on `ConnectRequest::need`). On the wire it
/// travels as the `improvement_bar: Option<f64>` of
/// `ConnectMsg::RequestWithNeed`, which is sent only to peers whose version
/// understands it; the legacy `ConnectMsg::Request` carries no need and decodes
/// as [`Self::Unknown`].
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) enum RequesterNeed {
    /// The request came in the legacy form, or the requester could not state
    /// its need (own location unknown). Admitted exactly as before #5858.
    #[default]
    Unknown,
    /// The requester holds fewer ring connections than its own threshold
    /// ([`requester_is_short`]); the value is how many it holds (saturating
    /// at 255). Eligible for the onboarding budget, with priority to requesters
    /// holding fewer ([`onboarding_high_priority`]).
    Short(u8),
    /// The requester is well connected; it already holds a connection this
    /// ring distance from the request's target, so only an acceptor strictly
    /// closer to the target helps it. Admitted through self-interest only.
    Bar(f64),
}

impl RequesterNeed {
    /// Decode the wire form: a `None` bar means short (holding
    /// `requester_connections`), `Some(d)` a bar of `d`.
    ///
    /// A bar that is not a finite non-negative number cannot be compared
    /// meaningfully (a NaN would reject every acceptor), so it falls back to
    /// [`Self::Unknown`], i.e. today's admission. Values above
    /// [`UNCONSTRAINED_BAR`] are clamped to it.
    pub(crate) fn from_wire(improvement_bar: Option<f64>, requester_connections: u8) -> Self {
        match improvement_bar {
            None => Self::Short(requester_connections),
            Some(d) if d.is_finite() && d >= 0.0 => Self::Bar(d.min(UNCONSTRAINED_BAR)),
            Some(_) => Self::Unknown,
        }
    }

    /// The wire form `(improvement_bar, requester_connections)`, or `None` for
    /// [`Self::Unknown`], which has no wire form other than the legacy
    /// `ConnectMsg::Request`.
    pub(crate) fn to_wire(self) -> Option<(Option<f64>, u8)> {
        match self {
            Self::Unknown => None,
            Self::Short(connections) => Some((None, connections)),
            Self::Bar(d) => Some((Some(d), 0)),
        }
    }

    /// The telemetry class of this need.
    pub(crate) fn class(self) -> NeedClass {
        match self {
            Self::Unknown => NeedClass::Unknown,
            Self::Short(_) => NeedClass::Short,
            Self::Bar(_) => NeedClass::Bar,
        }
    }

    /// A short-need value for a requester holding `open` ring connections.
    pub(crate) fn short(open: usize) -> Self {
        Self::Short(u8::try_from(open).unwrap_or(u8::MAX))
    }
}

/// The class of a requester's need, carried on the `connect_request_received`
/// telemetry event (#5858) so admissions can be read per class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) enum NeedClass {
    Unknown,
    Short,
    Bar,
}

impl NeedClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Short => "short",
            Self::Bar => "bar",
        }
    }
}

/// Whether a requester holding `open` ring connections is short of them, given
/// its own `min_connections`. Derived from configuration, never a fixed count.
pub(crate) fn requester_is_short(open: usize, min_connections: usize) -> bool {
    (open as f64) < (min_connections as f64) * NEEDY_FRACTION_OF_MIN_CONNECTIONS
}

/// The outcome of one CONNECT admission decision, with the path that admitted
/// it or the cause that refused it. Recorded on the `connect_request_received`
/// telemetry event and counted per path (`AdmissionStats`).
///
/// Stable strings ([`Self::as_str`]) are what reach the collector; keep them
/// unchanged once released.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) enum AdmissionCode {
    /// Admitted because it serves this peer's own topology (first connection,
    /// below-min soft filter, or the evaluator window).
    AcceptedSelfInterest,
    /// Admitted from the onboarding budget for a requester that is short.
    AcceptedOnboarding,
    /// Admitted as a new per-side nearest ring neighbour (lattice clause).
    AcceptedLattice,
    /// The candidate is this peer.
    RejectedSelfConnection,
    /// The candidate is already connected or pending.
    RejectedAlreadyConnected,
    /// At `max_connections` (open plus pending).
    RejectedAtCapacity,
    /// At/above `min_connections` and the score did not beat the evaluator window.
    RejectedEvaluatorWindow,
    /// Below `min_connections` and the probabilistic Kleinberg filter declined.
    RejectedSoftFilter,
    /// A short requester, but every onboarding slot it may use is on
    /// probation, and self-interest did not admit it either.
    RejectedOnboardingBudgetExhausted,
    /// The requester's bar: this peer is not closer to the target than a
    /// connection the requester already holds.
    RejectedBarUnmet,
    /// The candidate's address is on the operator blocklist.
    RejectedBlocked,
    /// Connection counters would overflow.
    RejectedCounterOverflow,
    /// A short requester, but the onboarding token bucket is empty, and
    /// self-interest did not admit it either.
    RejectedOnboardingRateLimited,
    /// A short requester, but its masked location already holds an onboarding
    /// slot, and self-interest did not admit it either.
    RejectedOnboardingLocationTaken,
}

impl AdmissionCode {
    pub(crate) fn accepted(self) -> bool {
        matches!(
            self,
            Self::AcceptedSelfInterest | Self::AcceptedOnboarding | Self::AcceptedLattice
        )
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::AcceptedSelfInterest => "self_interest",
            Self::AcceptedOnboarding => "onboarding",
            Self::AcceptedLattice => "lattice",
            Self::RejectedSelfConnection => "self_connection",
            Self::RejectedAlreadyConnected => "already_connected",
            Self::RejectedAtCapacity => "at_capacity",
            Self::RejectedEvaluatorWindow => "evaluator_window",
            Self::RejectedSoftFilter => "soft_filter",
            Self::RejectedOnboardingBudgetExhausted => "onboarding_budget_exhausted",
            Self::RejectedBarUnmet => "bar_unmet",
            Self::RejectedBlocked => "blocked",
            Self::RejectedCounterOverflow => "counter_overflow",
            Self::RejectedOnboardingRateLimited => "onboarding_rate_limited",
            Self::RejectedOnboardingLocationTaken => "onboarding_location_taken",
        }
    }
}

/// Lifetime admission counters (#5858), read on the `router_snapshot` cadence.
/// Monotonic totals; the collector differences them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AdmissionStats {
    pub admitted_self_interest: u64,
    pub admitted_onboarding: u64,
    pub admitted_lattice: u64,
    pub rejected_onboarding_budget_exhausted: u64,
    pub rejected_bar_unmet: u64,
    /// Requests whose bar this peer did not meet, decided on self-interest
    /// because the bar is not enforced (`ENFORCE_REQUESTER_BAR`): what
    /// enforcement would have refused (unless the lattice admitted them).
    pub bar_unmet_not_enforced: u64,
    /// Provisional links kept at the end of probation.
    pub provisional_graduated: u64,
    /// Provisional links dropped at the end of probation.
    pub provisional_dropped: u64,
    /// Provisional links shed early by a capacity-driven prune.
    pub provisional_pruned: u64,
    /// Provisional slots freed because the connection never established.
    pub provisional_released_unestablished: u64,
    /// Requests sent in the legacy form only because the next hop's version
    /// is unknown (`_unknown_version`, never heals) or known but below the
    /// floor (`_below_floor`, heals as the fleet upgrades).
    pub need_downgraded_unknown_version: u64,
    pub need_downgraded_below_floor: u64,
    /// Onboarding links whose connection was established (the denominator
    /// for the probation outcomes).
    pub onboarding_established: u64,
    /// CONNECT requests that arrived at this hop stating a short need / a bar
    /// (the denominators for the admission counters).
    pub requests_seen_short: u64,
    pub requests_seen_bar: u64,
    /// Provisional links currently on probation (gauge).
    pub provisional_in_use: u64,
}

/// The live counters behind [`AdmissionStats`]: cheap `Relaxed` atomics,
/// incremented at the point each decision is made, never re-derived.
#[derive(Debug, Default)]
pub(crate) struct AdmissionCounters {
    admitted_self_interest: AtomicU64,
    admitted_onboarding: AtomicU64,
    admitted_lattice: AtomicU64,
    rejected_onboarding_budget_exhausted: AtomicU64,
    rejected_bar_unmet: AtomicU64,
    bar_unmet_not_enforced: AtomicU64,
    provisional_graduated: AtomicU64,
    provisional_dropped: AtomicU64,
    provisional_pruned: AtomicU64,
    provisional_released_unestablished: AtomicU64,
    need_downgraded_unknown_version: AtomicU64,
    need_downgraded_below_floor: AtomicU64,
    onboarding_established: AtomicU64,
    requests_seen_short: AtomicU64,
    requests_seen_bar: AtomicU64,
}

/// What became of a provisional link (see [`AdmissionCounters::record_provisional`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProvisionalOutcome {
    Graduated,
    Dropped,
    Pruned,
    ReleasedUnestablished,
}

impl AdmissionCounters {
    pub(crate) fn record_decision(&self, code: AdmissionCode) {
        let counter = match code {
            AdmissionCode::AcceptedSelfInterest => &self.admitted_self_interest,
            AdmissionCode::AcceptedOnboarding => &self.admitted_onboarding,
            AdmissionCode::AcceptedLattice => &self.admitted_lattice,
            AdmissionCode::RejectedOnboardingBudgetExhausted
            | AdmissionCode::RejectedOnboardingRateLimited
            | AdmissionCode::RejectedOnboardingLocationTaken => {
                &self.rejected_onboarding_budget_exhausted
            }
            AdmissionCode::RejectedBarUnmet => &self.rejected_bar_unmet,
            // The remaining causes predate #5858 and are carried per event
            // (`connect_request_received`); no aggregate counter is added for
            // them here.
            AdmissionCode::RejectedSelfConnection
            | AdmissionCode::RejectedAlreadyConnected
            | AdmissionCode::RejectedAtCapacity
            | AdmissionCode::RejectedEvaluatorWindow
            | AdmissionCode::RejectedSoftFilter
            | AdmissionCode::RejectedBlocked
            | AdmissionCode::RejectedCounterOverflow => return,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_provisional(&self, outcome: ProvisionalOutcome) {
        let counter = match outcome {
            ProvisionalOutcome::Graduated => &self.provisional_graduated,
            ProvisionalOutcome::Dropped => &self.provisional_dropped,
            ProvisionalOutcome::Pruned => &self.provisional_pruned,
            ProvisionalOutcome::ReleasedUnestablished => &self.provisional_released_unestablished,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// One CONNECT request arriving at this hop, by its requester's need: the
    /// denominators for the admission counters.
    pub(crate) fn record_request_seen(&self, need: RequesterNeed) {
        let counter = match need {
            RequesterNeed::Short(_) => &self.requests_seen_short,
            RequesterNeed::Bar(_) => &self.requests_seen_bar,
            RequesterNeed::Unknown => return,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// A request the bar would have refused, admitted on self-interest
    /// because enforcement is off. Once per request.
    pub(crate) fn record_bar_unmet_not_enforced(&self) {
        self.bar_unmet_not_enforced.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_onboarding_established(&self) {
        self.onboarding_established.fetch_add(1, Ordering::Relaxed);
    }

    /// `unknown_version`: the hop's version is unknown (never heals), as
    /// opposed to known but below the floor (heals as the fleet upgrades).
    pub(crate) fn record_need_downgraded(&self, unknown_version: bool) {
        let counter = if unknown_version {
            &self.need_downgraded_unknown_version
        } else {
            &self.need_downgraded_below_floor
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self, provisional_in_use: usize) -> AdmissionStats {
        AdmissionStats {
            admitted_self_interest: self.admitted_self_interest.load(Ordering::Relaxed),
            admitted_onboarding: self.admitted_onboarding.load(Ordering::Relaxed),
            admitted_lattice: self.admitted_lattice.load(Ordering::Relaxed),
            rejected_onboarding_budget_exhausted: self
                .rejected_onboarding_budget_exhausted
                .load(Ordering::Relaxed),
            rejected_bar_unmet: self.rejected_bar_unmet.load(Ordering::Relaxed),
            bar_unmet_not_enforced: self.bar_unmet_not_enforced.load(Ordering::Relaxed),
            provisional_graduated: self.provisional_graduated.load(Ordering::Relaxed),
            provisional_dropped: self.provisional_dropped.load(Ordering::Relaxed),
            provisional_pruned: self.provisional_pruned.load(Ordering::Relaxed),
            provisional_released_unestablished: self
                .provisional_released_unestablished
                .load(Ordering::Relaxed),
            need_downgraded_unknown_version: self
                .need_downgraded_unknown_version
                .load(Ordering::Relaxed),
            need_downgraded_below_floor: self.need_downgraded_below_floor.load(Ordering::Relaxed),
            onboarding_established: self.onboarding_established.load(Ordering::Relaxed),
            requests_seen_short: self.requests_seen_short.load(Ordering::Relaxed),
            requests_seen_bar: self.requests_seen_bar.load(Ordering::Relaxed),
            provisional_in_use: provisional_in_use as u64,
        }
    }
}

/// Why [`ProvisionalLinks::try_reserve`] refused a slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SlotRefusal {
    /// Every slot the requester may use is on probation (or it holds one).
    Full,
    /// The token bucket is empty.
    RateLimited,
    /// Another requester at the same masked location holds a slot.
    LocationTaken,
}

impl SlotRefusal {
    pub(crate) fn code(self) -> AdmissionCode {
        match self {
            Self::Full => AdmissionCode::RejectedOnboardingBudgetExhausted,
            Self::RateLimited => AdmissionCode::RejectedOnboardingRateLimited,
            Self::LocationTaken => AdmissionCode::RejectedOnboardingLocationTaken,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Slot {
    admitted_at: Instant,
    location: Location,
}

/// The acceptor's onboarding slots: need-admitted links on probation, keyed by
/// the joiner's address, with the time each was admitted and its masked
/// location.
///
/// Three bounds, all checked in [`Self::try_reserve`]:
/// - at most `capacity` slots, the last [`ONBOARDING_SLOTS_KEPT_FOR_ISOLATED`]
///   only for high-priority requesters;
/// - a token bucket refilled every [`onboarding_refill_interval`], holding at
///   most `capacity` tokens; a reservation spends one, a release refunds none;
/// - at most one slot per masked [`Location`] (an IPv4 /24 or IPv6 /48), so one
///   host cannot take every slot from several ports.
///
/// Refusing at the cap is safe: entries are never refreshed by use, and every
/// one leaves within [`ONBOARDING_PROBATION`] (graduated or dropped) or earlier
/// when its connection fails or is pruned. Time is passed in by the caller.
#[derive(Debug)]
pub(crate) struct ProvisionalLinks {
    slots: BTreeMap<SocketAddr, Slot>,
    capacity: usize,
    tokens: f64,
    tokens_at: Option<Instant>,
    /// Self-interest graduations within the last
    /// [`ONBOARDING_GRADUATION_WINDOW`]; at most
    /// [`ONBOARDING_GRADUATIONS_PER_WINDOW`] entries.
    graduations: VecDeque<Instant>,
}

/// A probation decision to drop a link, carrying what is needed to put its
/// slot back if the drop cannot be queued
/// (`ConnectionManager::confirm_probation_drop`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProbationDrop {
    pub addr: SocketAddr,
    location: Location,
    admitted_at: Instant,
}

impl ProvisionalLinks {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            slots: BTreeMap::new(),
            capacity,
            tokens: capacity as f64,
            tokens_at: None,
            graduations: VecDeque::new(),
        }
    }

    /// Whether another self-interest graduation fits in the window at `now`.
    pub(crate) fn graduation_allowed(&mut self, now: Instant) -> bool {
        while self
            .graduations
            .front()
            .is_some_and(|at| now.saturating_duration_since(*at) >= ONBOARDING_GRADUATION_WINDOW)
        {
            self.graduations.pop_front();
        }
        self.graduations.len() < ONBOARDING_GRADUATIONS_PER_WINDOW
    }

    pub(crate) fn record_graduation(&mut self, now: Instant) {
        if self.graduations.len() < ONBOARDING_GRADUATIONS_PER_WINDOW {
            self.graduations.push_back(now);
        }
    }

    /// Take `addr`'s slot for a drop decision (see [`ProbationDrop`]).
    pub(crate) fn take_for_drop(&mut self, addr: SocketAddr) -> Option<ProbationDrop> {
        self.slots.remove(&addr).map(|slot| ProbationDrop {
            addr,
            location: slot.location,
            admitted_at: slot.admitted_at,
        })
    }

    /// Put back the slot of a drop that could not be queued, with its original
    /// admission time. Spends no token and ignores the capacity: the link
    /// never left.
    pub(crate) fn restore(&mut self, drop: ProbationDrop) {
        self.slots.insert(
            drop.addr,
            Slot {
                admitted_at: drop.admitted_at,
                location: drop.location,
            },
        );
    }

    fn refill(&mut self, now: Instant) {
        if let Some(at) = self.tokens_at {
            let elapsed = now.saturating_duration_since(at).as_secs_f64();
            self.tokens = (self.tokens + elapsed / onboarding_refill_interval().as_secs_f64())
                .min(self.capacity as f64);
        }
        self.tokens_at = Some(now);
    }

    /// Reserve a slot for `addr` at `location`. `high_priority` requesters may
    /// use every slot; others leave [`ONBOARDING_SLOTS_KEPT_FOR_ISOLATED`] free.
    pub(crate) fn try_reserve(
        &mut self,
        addr: SocketAddr,
        location: Location,
        high_priority: bool,
        now: Instant,
    ) -> Result<(), SlotRefusal> {
        let usable = if high_priority {
            self.capacity
        } else {
            self.capacity
                .saturating_sub(ONBOARDING_SLOTS_KEPT_FOR_ISOLATED)
        };
        if self.slots.len() >= usable || self.slots.contains_key(&addr) {
            return Err(SlotRefusal::Full);
        }
        if self.slots.values().any(|slot| slot.location == location) {
            return Err(SlotRefusal::LocationTaken);
        }
        self.refill(now);
        // A low-priority requester must leave a token for each slot kept for
        // the isolated, or the bucket (not the slots) would be what runs out
        // for them: one shared bucket, with the reserve enforced on both of
        // its limits, rather than per-class buckets that would double the
        // admission rate.
        let tokens_needed = if high_priority {
            1.0
        } else {
            1.0 + ONBOARDING_SLOTS_KEPT_FOR_ISOLATED as f64
        };
        if self.tokens < tokens_needed {
            return Err(SlotRefusal::RateLimited);
        }
        self.tokens -= 1.0;
        self.slots.insert(
            addr,
            Slot {
                admitted_at: now,
                location,
            },
        );
        Ok(())
    }

    /// Move `old`'s slot to `new`, keeping its admission time: the same
    /// connection, re-keyed when the peer's address is corrected.
    pub(crate) fn rename(&mut self, old: SocketAddr, new: SocketAddr) {
        if let Some(slot) = self.slots.remove(&old) {
            self.slots.insert(new, slot);
        }
    }

    /// Free `addr`'s slot. `true` if it held one.
    pub(crate) fn release(&mut self, addr: SocketAddr) -> bool {
        self.slots.remove(&addr).is_some()
    }

    pub(crate) fn contains(&self, addr: SocketAddr) -> bool {
        self.slots.contains_key(&addr)
    }

    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }

    /// Every slot with its age at `now`, oldest admission first.
    pub(crate) fn ages(&self, now: Instant) -> Vec<(SocketAddr, Duration)> {
        let mut ages: Vec<(SocketAddr, Duration)> = self
            .slots
            .iter()
            .map(|(addr, slot)| (*addr, now.saturating_duration_since(slot.admitted_at)))
            .collect();
        ages.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        ages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], port))
    }

    #[test]
    fn need_wire_form_round_trips() {
        assert_eq!(RequesterNeed::from_wire(None, 3), RequesterNeed::Short(3));
        assert_eq!(
            RequesterNeed::from_wire(Some(0.125), 0),
            RequesterNeed::Bar(0.125)
        );
        assert_eq!(RequesterNeed::Short(4).to_wire(), Some((None, 4)));
        assert_eq!(RequesterNeed::short(300), RequesterNeed::Short(u8::MAX));
        assert_eq!(RequesterNeed::Bar(0.25).to_wire(), Some((Some(0.25), 0)));
        assert_eq!(RequesterNeed::Unknown.to_wire(), None);
    }

    /// A bar that cannot be compared must not turn into "reject every
    /// acceptor": it falls back to today's admission.
    #[test]
    fn malformed_bar_falls_back_to_unknown() {
        assert_eq!(
            RequesterNeed::from_wire(Some(f64::NAN), 0),
            RequesterNeed::Unknown
        );
        assert_eq!(
            RequesterNeed::from_wire(Some(f64::INFINITY), 0),
            RequesterNeed::Unknown
        );
        assert_eq!(
            RequesterNeed::from_wire(Some(-0.1), 0),
            RequesterNeed::Unknown
        );
        assert_eq!(
            RequesterNeed::from_wire(Some(3.0), 0),
            RequesterNeed::Bar(UNCONSTRAINED_BAR)
        );
    }

    #[test]
    fn short_threshold_follows_min_connections() {
        assert!(requester_is_short(0, 25));
        assert!(requester_is_short(24, 25));
        assert!(!requester_is_short(25, 25));
        assert!(!requester_is_short(0, 0));
    }

    fn loc(x: f64) -> Location {
        Location::new(x)
    }

    #[test]
    fn provisional_links_refuse_at_capacity_and_free_on_release() {
        let now = Instant::now();
        let mut slots = ProvisionalLinks::new(2);
        assert_eq!(slots.try_reserve(addr(1), loc(0.1), true, now), Ok(()));
        assert_eq!(
            slots.try_reserve(addr(1), loc(0.2), true, now),
            Err(SlotRefusal::Full),
            "one slot per address"
        );
        assert_eq!(slots.try_reserve(addr(2), loc(0.2), true, now), Ok(()));
        assert_eq!(
            slots.try_reserve(addr(3), loc(0.3), true, now),
            Err(SlotRefusal::Full),
            "capacity is a hard bound"
        );
        assert_eq!(slots.len(), 2);
        assert!(slots.release(addr(1)));
        assert!(!slots.release(addr(1)));
        assert!(slots.contains(addr(2)));
    }

    /// A release does not refund a token: after the burst, the next admission
    /// waits for the refill interval even though a slot is free.
    #[test]
    fn released_slots_do_not_refund_onboarding_tokens() {
        let start = Instant::now();
        let mut slots = ProvisionalLinks::new(2);
        assert_eq!(slots.try_reserve(addr(1), loc(0.1), true, start), Ok(()));
        assert_eq!(slots.try_reserve(addr(2), loc(0.2), true, start), Ok(()));
        assert!(slots.release(addr(1)));
        assert!(slots.release(addr(2)));
        assert_eq!(
            slots.try_reserve(addr(3), loc(0.3), true, start + Duration::from_secs(1)),
            Err(SlotRefusal::RateLimited)
        );
        let later = start + onboarding_refill_interval() + Duration::from_secs(1);
        assert_eq!(slots.try_reserve(addr(3), loc(0.3), true, later), Ok(()));
        assert_eq!(
            slots.try_reserve(addr(4), loc(0.4), true, later),
            Err(SlotRefusal::RateLimited),
            "one interval refills one token"
        );
    }

    #[test]
    fn one_onboarding_slot_per_masked_location() {
        let now = Instant::now();
        let mut slots = ProvisionalLinks::new(2);
        assert_eq!(slots.try_reserve(addr(1), loc(0.5), true, now), Ok(()));
        assert_eq!(
            slots.try_reserve(addr(2), loc(0.5), true, now),
            Err(SlotRefusal::LocationTaken)
        );
    }

    /// Low-priority requesters leave a slot for nearly isolated ones.
    #[test]
    fn low_priority_requesters_leave_a_slot_for_the_isolated() {
        let now = Instant::now();
        let mut slots = ProvisionalLinks::new(2);
        assert_eq!(slots.try_reserve(addr(1), loc(0.1), false, now), Ok(()));
        assert_eq!(
            slots.try_reserve(addr(2), loc(0.2), false, now),
            Err(SlotRefusal::Full)
        );
        assert_eq!(slots.try_reserve(addr(3), loc(0.3), true, now), Ok(()));
        assert!(onboarding_high_priority(0, 25));
        assert!(onboarding_high_priority(6, 25));
        assert!(!onboarding_high_priority(7, 25));
    }

    /// The isolated reserve applies to the token bucket too: with one token
    /// left, a low-priority requester is refused and a nearly isolated one is
    /// admitted.
    #[test]
    fn low_priority_requesters_leave_a_token_for_the_isolated() {
        let start = Instant::now();
        let mut slots = ProvisionalLinks::new(2);
        // Drain one token with a slot that is then freed (no refund).
        assert_eq!(slots.try_reserve(addr(1), loc(0.1), true, start), Ok(()));
        assert!(slots.release(addr(1)));
        assert_eq!(
            slots.try_reserve(addr(2), loc(0.2), false, start),
            Err(SlotRefusal::RateLimited),
            "the last token is kept for the isolated"
        );
        assert_eq!(slots.try_reserve(addr(3), loc(0.3), true, start), Ok(()));
    }

    #[test]
    fn graduations_are_capped_per_window() {
        let start = Instant::now();
        let mut slots = ProvisionalLinks::new(2);
        assert!(slots.graduation_allowed(start));
        slots.record_graduation(start);
        assert!(!slots.graduation_allowed(start + Duration::from_secs(1)));
        assert!(slots.graduation_allowed(start + ONBOARDING_GRADUATION_WINDOW));
    }

    #[test]
    fn a_drop_that_is_not_queued_restores_the_slot() {
        let start = Instant::now();
        let mut slots = ProvisionalLinks::new(2);
        assert_eq!(slots.try_reserve(addr(1), loc(0.1), true, start), Ok(()));
        let drop = slots.take_for_drop(addr(1)).expect("held");
        assert!(!slots.contains(addr(1)));
        slots.restore(drop);
        assert_eq!(
            slots.ages(start + Duration::from_secs(7)),
            vec![(addr(1), Duration::from_secs(7))],
            "restored with its original admission time"
        );
    }

    #[test]
    fn provisional_rename_keeps_the_admission_time() {
        let start = Instant::now();
        let mut slots = ProvisionalLinks::new(2);
        assert_eq!(slots.try_reserve(addr(1), loc(0.1), true, start), Ok(()));
        slots.rename(addr(1), addr(9));
        assert!(!slots.contains(addr(1)));
        assert_eq!(
            slots.ages(start + Duration::from_secs(3)),
            vec![(addr(9), Duration::from_secs(3))]
        );
        slots.rename(addr(5), addr(6));
        assert_eq!(slots.len(), 1, "renaming an absent address is a no-op");
    }

    #[test]
    fn provisional_ages_are_oldest_first() {
        let start = Instant::now();
        let mut slots = ProvisionalLinks::new(2);
        assert_eq!(slots.try_reserve(addr(2), loc(0.2), true, start), Ok(()));
        assert_eq!(
            slots.try_reserve(addr(1), loc(0.1), true, start + Duration::from_secs(5)),
            Ok(())
        );
        let ages = slots.ages(start + Duration::from_secs(60));
        assert_eq!(
            ages,
            vec![
                (addr(2), Duration::from_secs(60)),
                (addr(1), Duration::from_secs(55)),
            ]
        );
    }

    /// The telemetry strings are what the collector groups by.
    #[test]
    fn admission_codes_have_distinct_strings() {
        let all = [
            AdmissionCode::AcceptedSelfInterest,
            AdmissionCode::AcceptedOnboarding,
            AdmissionCode::AcceptedLattice,
            AdmissionCode::RejectedSelfConnection,
            AdmissionCode::RejectedAlreadyConnected,
            AdmissionCode::RejectedAtCapacity,
            AdmissionCode::RejectedEvaluatorWindow,
            AdmissionCode::RejectedSoftFilter,
            AdmissionCode::RejectedOnboardingBudgetExhausted,
            AdmissionCode::RejectedBarUnmet,
            AdmissionCode::RejectedBlocked,
            AdmissionCode::RejectedCounterOverflow,
            AdmissionCode::RejectedOnboardingRateLimited,
            AdmissionCode::RejectedOnboardingLocationTaken,
        ];
        let strings: std::collections::HashSet<&str> = all.iter().map(|c| c.as_str()).collect();
        assert_eq!(strings.len(), all.len());
        assert_eq!(
            all.iter().filter(|c| c.accepted()).count(),
            3,
            "exactly the three admission paths count as accepted"
        );
    }
}
