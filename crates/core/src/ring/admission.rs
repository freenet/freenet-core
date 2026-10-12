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
//! probation it is the first link a topology prune sheds.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

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
    /// ([`requester_is_short`]). Eligible for the onboarding budget.
    Short,
    /// The requester is well connected; it already holds a connection this
    /// ring distance from the request's target, so only an acceptor strictly
    /// closer to the target helps it. Admitted through self-interest only.
    Bar(f64),
}

impl RequesterNeed {
    /// Decode the wire form: `None` means short, `Some(d)` a bar of `d`.
    ///
    /// A bar that is not a finite non-negative number cannot be compared
    /// meaningfully (a NaN would reject every acceptor), so it falls back to
    /// [`Self::Unknown`], i.e. today's admission. Values above
    /// [`UNCONSTRAINED_BAR`] are clamped to it.
    pub(crate) fn from_wire(improvement_bar: Option<f64>) -> Self {
        match improvement_bar {
            None => Self::Short,
            Some(d) if d.is_finite() && d >= 0.0 => Self::Bar(d.min(UNCONSTRAINED_BAR)),
            Some(_) => Self::Unknown,
        }
    }

    /// The wire form, or `None` for [`Self::Unknown`], which has no wire form
    /// other than the legacy `ConnectMsg::Request`.
    pub(crate) fn to_wire(self) -> Option<Option<f64>> {
        match self {
            Self::Unknown => None,
            Self::Short => Some(None),
            Self::Bar(d) => Some(Some(d)),
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
    /// A short requester, but every onboarding slot is on probation.
    RejectedOnboardingBudgetExhausted,
    /// The requester's bar: this peer is not closer to the target than a
    /// connection the requester already holds.
    RejectedBarUnmet,
    /// The candidate's address is on the operator blocklist.
    RejectedBlocked,
    /// Connection counters would overflow.
    RejectedCounterOverflow,
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
    /// Provisional links kept at the end of probation.
    pub provisional_graduated: u64,
    /// Provisional links dropped at the end of probation.
    pub provisional_dropped: u64,
    /// Provisional links shed early by a topology prune or swap.
    pub provisional_pruned: u64,
    /// Provisional slots freed because the connection never established.
    pub provisional_released_unestablished: u64,
    /// Requests sent in the legacy form only because the next hop's version
    /// does not understand the need (or is unknown).
    pub need_downgraded_to_legacy: u64,
    /// Onboarding links whose connection was established (the denominator
    /// for the probation outcomes).
    pub onboarding_established: u64,
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
    provisional_graduated: AtomicU64,
    provisional_dropped: AtomicU64,
    provisional_pruned: AtomicU64,
    provisional_released_unestablished: AtomicU64,
    need_downgraded_to_legacy: AtomicU64,
    onboarding_established: AtomicU64,
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
            AdmissionCode::RejectedOnboardingBudgetExhausted => {
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

    pub(crate) fn record_onboarding_established(&self) {
        self.onboarding_established.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_need_downgraded(&self) {
        self.need_downgraded_to_legacy
            .fetch_add(1, Ordering::Relaxed);
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
            provisional_graduated: self.provisional_graduated.load(Ordering::Relaxed),
            provisional_dropped: self.provisional_dropped.load(Ordering::Relaxed),
            provisional_pruned: self.provisional_pruned.load(Ordering::Relaxed),
            provisional_released_unestablished: self
                .provisional_released_unestablished
                .load(Ordering::Relaxed),
            need_downgraded_to_legacy: self.need_downgraded_to_legacy.load(Ordering::Relaxed),
            onboarding_established: self.onboarding_established.load(Ordering::Relaxed),
            provisional_in_use: provisional_in_use as u64,
        }
    }
}

/// The acceptor's onboarding slots: need-admitted links on probation, keyed by
/// the joiner's address, with the time each was admitted.
///
/// Bounded at insertion ([`Self::try_reserve`] refuses at capacity). Entries
/// are never refreshed by use, so refusing at the cap cannot starve newcomers:
/// every entry leaves within [`ONBOARDING_PROBATION`], when it graduates or is
/// dropped, and earlier if its connection fails or is pruned.
///
/// Time is passed in by the caller so tests can drive it.
#[derive(Debug)]
pub(crate) struct ProvisionalLinks {
    admitted_at: BTreeMap<SocketAddr, Instant>,
    capacity: usize,
}

impl ProvisionalLinks {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            admitted_at: BTreeMap::new(),
            capacity,
        }
    }

    /// Reserve a slot for `addr`. `false` when every slot is taken or `addr`
    /// already holds one.
    pub(crate) fn try_reserve(&mut self, addr: SocketAddr, now: Instant) -> bool {
        if self.admitted_at.len() >= self.capacity || self.admitted_at.contains_key(&addr) {
            return false;
        }
        self.admitted_at.insert(addr, now);
        true
    }

    /// Move `old`'s slot to `new`, keeping its admission time: the same
    /// connection, re-keyed when the peer's address is corrected.
    pub(crate) fn rename(&mut self, old: SocketAddr, new: SocketAddr) {
        if let Some(at) = self.admitted_at.remove(&old) {
            self.admitted_at.insert(new, at);
        }
    }

    /// Free `addr`'s slot. `true` if it held one.
    pub(crate) fn release(&mut self, addr: SocketAddr) -> bool {
        self.admitted_at.remove(&addr).is_some()
    }

    pub(crate) fn contains(&self, addr: SocketAddr) -> bool {
        self.admitted_at.contains_key(&addr)
    }

    pub(crate) fn len(&self) -> usize {
        self.admitted_at.len()
    }

    /// Every slot with its age at `now`, oldest admission first.
    pub(crate) fn ages(&self, now: Instant) -> Vec<(SocketAddr, Duration)> {
        let mut ages: Vec<(SocketAddr, Duration)> = self
            .admitted_at
            .iter()
            .map(|(addr, at)| (*addr, now.saturating_duration_since(*at)))
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
        assert_eq!(RequesterNeed::from_wire(None), RequesterNeed::Short);
        assert_eq!(
            RequesterNeed::from_wire(Some(0.125)),
            RequesterNeed::Bar(0.125)
        );
        assert_eq!(RequesterNeed::Short.to_wire(), Some(None));
        assert_eq!(RequesterNeed::Bar(0.25).to_wire(), Some(Some(0.25)));
        assert_eq!(RequesterNeed::Unknown.to_wire(), None);
    }

    /// A bar that cannot be compared must not turn into "reject every
    /// acceptor": it falls back to today's admission.
    #[test]
    fn malformed_bar_falls_back_to_unknown() {
        assert_eq!(
            RequesterNeed::from_wire(Some(f64::NAN)),
            RequesterNeed::Unknown
        );
        assert_eq!(
            RequesterNeed::from_wire(Some(f64::INFINITY)),
            RequesterNeed::Unknown
        );
        assert_eq!(RequesterNeed::from_wire(Some(-0.1)), RequesterNeed::Unknown);
        assert_eq!(
            RequesterNeed::from_wire(Some(3.0)),
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

    #[test]
    fn provisional_links_refuse_at_capacity_and_free_on_release() {
        let now = Instant::now();
        let mut slots = ProvisionalLinks::new(2);
        assert!(slots.try_reserve(addr(1), now));
        assert!(!slots.try_reserve(addr(1), now), "one slot per address");
        assert!(slots.try_reserve(addr(2), now));
        assert!(!slots.try_reserve(addr(3), now), "capacity is a hard bound");
        assert_eq!(slots.len(), 2);
        assert!(slots.release(addr(1)));
        assert!(!slots.release(addr(1)));
        assert!(slots.try_reserve(addr(3), now));
        assert!(slots.contains(addr(3)));
    }

    #[test]
    fn provisional_rename_keeps_the_admission_time() {
        let start = Instant::now();
        let mut slots = ProvisionalLinks::new(2);
        assert!(slots.try_reserve(addr(1), start));
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
        assert!(slots.try_reserve(addr(1), start + Duration::from_secs(5)));
        assert!(slots.try_reserve(addr(2), start));
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
