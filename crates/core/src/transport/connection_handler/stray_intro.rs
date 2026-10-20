//! Classifying the intro packets a non-gateway drops (telemetry only).
//!
//! A non-gateway accepts an intro packet only from an address it has a
//! connection or an outbound traversal for, and drops the rest. Some of those
//! drops are a hole punch that nearly worked, and which kind it was says what
//! would fix it (see [`StrayIntro`]). Nothing here changes what is dropped.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::time::Duration;

use crate::transport::metrics::{NatAttemptSeen, StrayIntro, TransportMetrics};

/// How long after an outbound traversal fails at its deadline an intro from
/// the same address still counts as [`StrayIntro::AfterDeadline`].
pub(super) const STRAY_INTRO_WINDOW: Duration = Duration::from_secs(10);

/// At most this many recent failures are remembered. When more fail within
/// [`STRAY_INTRO_WINDOW`], the oldest are forgotten early, and an intro from
/// one of them counts as another kind. Each such eviction is counted
/// (`stray_intro_failures_evicted`), so a saturated table is visible.
pub(super) const MAX_RECENT_TRAVERSAL_FAILURES: usize = 256;

/// Which kind of stray intro a dropped one is. An exact-address match on a
/// recent failure wins over a same-IP traversal, being the more specific.
pub(super) fn classify(failed_recently: bool, traversal_to_same_ip: bool) -> StrayIntro {
    if failed_recently {
        StrayIntro::AfterDeadline
    } else if traversal_to_same_ip {
        StrayIntro::SameIpOtherPort
    } else {
        StrayIntro::Other
    }
}

/// Addresses whose outbound traversal ran to its deadline and failed in the
/// last [`STRAY_INTRO_WINDOW`], owned by the listener task (no locks).
///
/// Bounded twice: an entry expires after the window (by time, which nothing
/// can renew: only a new failure adds an entry), and the table holds at most
/// [`MAX_RECENT_TRAVERSAL_FAILURES`], evicting the oldest. Expired entries are
/// pruned on every insert and lookup, and by the listener's periodic cleanup.
#[derive(Default)]
pub(super) struct RecentTraversalFailures {
    /// `(address, failed_at_nanos)` in insertion order. The listener's clock
    /// is monotonic, so this is also time order and expiry pops the front.
    order: VecDeque<(SocketAddr, u64)>,
    /// Latest failure time per address. An address that failed twice has two
    /// `order` entries; popping the older leaves this one alone.
    latest: HashMap<SocketAddr, u64>,
}

impl RecentTraversalFailures {
    /// An outbound traversal to `addr` failed at `now_nanos`, having seen
    /// `seen` (the same value its failure sample records). Only a failure
    /// that ran to its deadline is remembered: an intro after one that ended
    /// early (version mismatch, send error, ...) is not a timing miss.
    /// Unexpired entries evicted to make room are counted in `metrics`.
    pub(super) fn record_failure(
        &mut self,
        addr: SocketAddr,
        seen: NatAttemptSeen,
        now_nanos: u64,
        metrics: &TransportMetrics,
    ) {
        if !seen.deadline_reached {
            return;
        }
        self.prune(now_nanos);
        let mut evicted = 0;
        while self.order.len() >= MAX_RECENT_TRAVERSAL_FAILURES {
            self.pop_oldest();
            evicted += 1;
        }
        metrics.record_stray_intro_failures_evicted(evicted);
        self.order.push_back((addr, now_nanos));
        self.latest.insert(addr, now_nanos);
    }

    /// Whether a traversal to exactly `addr` failed within the window.
    pub(super) fn failed_recently(&mut self, addr: SocketAddr, now_nanos: u64) -> bool {
        self.prune(now_nanos);
        self.latest.contains_key(&addr)
    }

    /// Forget failures older than the window.
    pub(super) fn prune(&mut self, now_nanos: u64) {
        let window = STRAY_INTRO_WINDOW.as_nanos() as u64;
        while self
            .order
            .front()
            .is_some_and(|(_, at)| now_nanos.saturating_sub(*at) >= window)
        {
            self.pop_oldest();
        }
    }

    fn pop_oldest(&mut self) {
        if let Some((addr, at)) = self.order.pop_front() {
            if self.latest.get(&addr) == Some(&at) {
                self.latest.remove(&addr);
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        debug_assert!(self.latest.len() <= self.order.len());
        self.order.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: u64 = 1_000_000_000;

    fn seen(deadline_reached: bool) -> NatAttemptSeen {
        NatAttemptSeen {
            deadline_reached,
            ..Default::default()
        }
    }

    fn evictions(metrics: &TransportMetrics) -> u32 {
        metrics.read_snapshot().stray_intro_failures_evicted
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([192, 0, 2, 1], port))
    }

    #[test]
    fn classification_prefers_the_exact_address() {
        assert_eq!(classify(true, true), StrayIntro::AfterDeadline);
        assert_eq!(classify(true, false), StrayIntro::AfterDeadline);
        assert_eq!(classify(false, true), StrayIntro::SameIpOtherPort);
        assert_eq!(classify(false, false), StrayIntro::Other);
    }

    /// A failure is remembered for exactly the window, for its own address
    /// only.
    #[test]
    fn failure_expires_after_the_window() {
        let mut recent = RecentTraversalFailures::default();
        let m = TransportMetrics::new();
        let start = 100 * SEC;
        recent.record_failure(addr(1), seen(true), start, &m);
        assert!(recent.failed_recently(addr(1), start));
        assert!(!recent.failed_recently(addr(2), start), "other port");
        let window = STRAY_INTRO_WINDOW.as_nanos() as u64;
        assert!(recent.failed_recently(addr(1), start + window - 1));
        assert!(!recent.failed_recently(addr(1), start + window));
        assert_eq!(recent.len(), 0, "expired entries are pruned");
    }

    /// A repeat failure restarts that address's window; expiry of the older
    /// entry does not forget the newer one.
    #[test]
    fn repeat_failure_extends_only_by_its_own_time() {
        let mut recent = RecentTraversalFailures::default();
        let m = TransportMetrics::new();
        recent.record_failure(addr(1), seen(true), 0, &m);
        recent.record_failure(addr(1), seen(true), 5 * SEC, &m);
        assert!(recent.failed_recently(addr(1), 12 * SEC));
        assert!(!recent.failed_recently(addr(1), 15 * SEC));
    }

    /// The table never exceeds its bound; the oldest entries go first, and
    /// each eviction is counted (expiry is not).
    #[test]
    fn table_is_bounded_and_evicts_the_oldest() {
        let mut recent = RecentTraversalFailures::default();
        let m = TransportMetrics::new();
        let n = MAX_RECENT_TRAVERSAL_FAILURES as u16 + 10;
        for port in 0..n {
            recent.record_failure(addr(port), seen(true), 0, &m);
            assert!(recent.len() <= MAX_RECENT_TRAVERSAL_FAILURES);
            let expected = (usize::from(port) + 1).saturating_sub(MAX_RECENT_TRAVERSAL_FAILURES);
            assert_eq!(evictions(&m) as usize, expected, "after port {port}");
        }
        assert_eq!(recent.len(), MAX_RECENT_TRAVERSAL_FAILURES);
        for port in 0..10 {
            assert!(!recent.failed_recently(addr(port), 0), "{port} evicted");
        }
        for port in 10..n {
            assert!(recent.failed_recently(addr(port), 0), "{port} kept");
        }
        let window = STRAY_INTRO_WINDOW.as_nanos() as u64;
        recent.record_failure(addr(n), seen(true), window, &m);
        assert_eq!(evictions(&m), 10, "expired entries are pruned, not evicted");
        assert_eq!(recent.len(), 1);
    }

    /// A failure that ended before its deadline is not remembered.
    #[test]
    fn an_early_failure_is_not_remembered() {
        let mut recent = RecentTraversalFailures::default();
        let m = TransportMetrics::new();
        recent.record_failure(addr(1), seen(false), 0, &m);
        assert!(!recent.failed_recently(addr(1), 0));
        assert_eq!(recent.len(), 0);
    }

    /// Periodic pruning empties the table once the window has passed, with
    /// no lookup needed.
    #[test]
    fn prune_empties_expired_entries() {
        let mut recent = RecentTraversalFailures::default();
        let m = TransportMetrics::new();
        recent.record_failure(addr(1), seen(true), 0, &m);
        recent.record_failure(addr(2), seen(true), SEC, &m);
        recent.prune(10 * SEC);
        assert_eq!(recent.len(), 1);
        recent.prune(11 * SEC);
        assert_eq!(recent.len(), 0);
        assert!(recent.latest.is_empty());
    }
}
