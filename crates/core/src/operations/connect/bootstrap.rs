//! Timing of a joiner's CONNECTs while it has no ring connection (#5858).
//!
//! A fresh node joins by sending a CONNECT through each gateway. Each CONNECT
//! used to wait for `min_connections` acceptors or the 60 s transaction TTL,
//! and the join loop waited for every CONNECT of a round, so a joiner whose
//! first acceptors were unreachable sat idle for the full TTL before trying
//! again. Three pieces here bound that:
//!
//! - [`BootstrapRound`]: a CONNECT sent with no ring connection ends once
//!   nothing is in flight and nothing has arrived for
//!   [`BOOTSTRAP_REPLY_QUIET`], instead of at the TTL.
//! - [`AcceptorDialQueue`]: acceptors are dialled concurrently, up to
//!   [`MAX_CONCURRENT_ACCEPTOR_DIALS`], so an unreachable acceptor no longer
//!   holds up a reachable one until its hole-punch window has passed.
//! - [`BootstrapPacer`]: the join loop and the maintenance loop share one
//!   pacer, so at most one bootstrap round runs at a time and failed rounds
//!   are spaced by a jittered exponential backoff.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::Instant;

use crate::config::GlobalRng;
use crate::util::backoff::ExponentialBackoff;

/// How long a bootstrapping CONNECT waits, with no dial in flight, for one
/// more reply before it ends.
///
/// Two hole-punch deadlines: a dial that fails at the end of the acceptor's
/// punch window makes the joiner send `ConnectFailed`, and the relay's
/// re-routed acceptor has to answer within this window. A live gateway's first
/// reply (`ObservedAddress`) arrives within one round trip, so a CONNECT that
/// hears nothing at all in this window went into a dead gateway link.
pub(crate) const BOOTSTRAP_REPLY_QUIET: Duration =
    Duration::from_secs(2 * crate::transport::connection_handler::NAT_TRAVERSAL_DEADLINE.as_secs());

/// Acceptors of one CONNECT dialled at the same time.
///
/// A CONNECT's acceptors answer within a second or two of each other, and each
/// hole punch lasts up to the NAT traversal deadline on both sides. Dialled
/// one at a time, the second acceptor's punch window has closed by the time
/// the first unreachable one gives up. Four covers the bursts seen in the
/// saturated-join simulation (two to four acceptors per CONNECT) while
/// bounding the parallel handshakes a single CONNECT can start.
pub(crate) const MAX_CONCURRENT_ACCEPTOR_DIALS: usize = 4;

/// First wait after a bootstrap round that ended with no ring connection.
const BOOTSTRAP_RETRY_BASE: Duration = Duration::from_secs(1);

/// How a bootstrapping CONNECT ended, decided by [`BootstrapRound`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BootstrapExit {
    /// At least one acceptor was connected.
    Connected,
    /// Acceptors answered but none could be connected.
    AllFailed,
    /// Neither the gateway nor any acceptor answered: the gateway link is
    /// dead or the gateway is gone.
    NoReply,
}

/// Exit policy for one CONNECT sent while this peer had no ring connection.
///
/// Disabled (never exits early) for a peer that already had a connection when
/// the CONNECT started, which keeps the TTL-bounded collection that ring
/// acquisition relies on.
#[derive(Debug)]
pub(crate) struct BootstrapRound {
    enabled: bool,
    last_activity: Instant,
    heard_anything: bool,
    connected: bool,
}

impl BootstrapRound {
    pub(crate) fn new(enabled: bool, now: Instant) -> Self {
        Self {
            enabled,
            last_activity: now,
            heard_anything: false,
            connected: false,
        }
    }

    /// Any message for this transaction arrived.
    pub(crate) fn on_reply(&mut self, now: Instant) {
        self.heard_anything = true;
        self.last_activity = now;
    }

    /// A dial to an acceptor finished.
    pub(crate) fn on_dial_done(&mut self, now: Instant, connected: bool) {
        self.connected |= connected;
        self.last_activity = now;
    }

    /// Whether the CONNECT should end now. `busy` is true while a dial is in
    /// flight or queued: its outcome may still produce a connection or a
    /// re-route.
    pub(crate) fn exit(&self, now: Instant, busy: bool) -> Option<BootstrapExit> {
        if !self.enabled || busy {
            return None;
        }
        if now.saturating_duration_since(self.last_activity) < BOOTSTRAP_REPLY_QUIET {
            return None;
        }
        Some(if self.connected {
            BootstrapExit::Connected
        } else if self.heard_anything {
            BootstrapExit::AllFailed
        } else {
            BootstrapExit::NoReply
        })
    }
}

/// Bounded set of acceptor dials for one CONNECT: up to `cap` in flight, the
/// rest queued in arrival order.
#[derive(Debug)]
pub(crate) struct AcceptorDialQueue<T> {
    cap: usize,
    in_flight: usize,
    queued: VecDeque<T>,
}

impl<T> AcceptorDialQueue<T> {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            in_flight: 0,
            queued: VecDeque::new(),
        }
    }

    /// An acceptor answered. Returns it if it may be dialled now; otherwise it
    /// is queued until a dial finishes.
    pub(crate) fn offer(&mut self, acceptor: T) -> Option<T> {
        if self.in_flight < self.cap {
            self.in_flight += 1;
            Some(acceptor)
        } else {
            self.queued.push_back(acceptor);
            None
        }
    }

    /// A dial finished. Returns the next queued acceptor to dial, if any.
    pub(crate) fn finish(&mut self) -> Option<T> {
        self.in_flight = self.in_flight.saturating_sub(1);
        let next = self.queued.pop_front()?;
        self.in_flight += 1;
        Some(next)
    }

    /// A dial is in flight or waiting for a slot.
    pub(crate) fn busy(&self) -> bool {
        self.in_flight > 0 || !self.queued.is_empty()
    }
}

/// Spacing of bootstrap rounds, shared by `initial_join_procedure` and the
/// maintenance loop's zero-connection bootstrap.
///
/// One round at a time; after a round that ends with no ring connection the
/// next may start after `backoff.delay_for_failures(n)` ±20%, where `n` counts
/// consecutive such rounds. The cap is the transient TTL, so a joiner that
/// keeps failing still sends each gateway a request well within the
/// gateway's zombie-transport threshold (three transient TTLs) and its
/// gateway links are not reaped while it is still joining.
#[derive(Debug)]
pub(crate) struct BootstrapPacer {
    backoff: ExponentialBackoff,
    failures: u32,
    next_allowed: Option<Instant>,
    round_in_flight: bool,
}

impl BootstrapPacer {
    pub(crate) fn new(transient_ttl: Duration) -> Self {
        Self {
            backoff: ExponentialBackoff::new(
                BOOTSTRAP_RETRY_BASE,
                transient_ttl.max(BOOTSTRAP_RETRY_BASE),
            ),
            failures: 0,
            next_allowed: None,
            round_in_flight: false,
        }
    }

    /// When the next round may start, if it has to wait.
    pub(crate) fn wait_until(&self, now: Instant) -> Option<Instant> {
        self.next_allowed.filter(|t| *t > now)
    }

    /// Whether a round is running.
    #[cfg(test)]
    pub(crate) fn round_in_flight(&self) -> bool {
        self.round_in_flight
    }

    fn try_begin(&mut self, now: Instant) -> bool {
        if self.round_in_flight || self.wait_until(now).is_some() {
            return false;
        }
        self.round_in_flight = true;
        true
    }

    fn end(&mut self, now: Instant, issued: bool, connected: bool) {
        self.round_in_flight = false;
        if !issued {
            // Nothing was sent (e.g. every gateway in backoff): not a failed
            // round, so it does not grow the backoff.
            return;
        }
        if connected {
            self.failures = 0;
            self.next_allowed = None;
        } else {
            self.failures = self.failures.saturating_add(1);
            let delay = self.backoff.delay_for_failures(self.failures);
            let jitter: f64 = GlobalRng::random_range(0.8_f64..=1.2_f64);
            self.next_allowed = Some(now + delay.mul_f64(jitter));
        }
    }
}

/// A running bootstrap round. Ends the round when dropped. Set `issued` once
/// the round sends a CONNECT, and `connected` if it produced a ring
/// connection; a round that sent nothing leaves the backoff unchanged.
#[derive(Debug)]
pub(crate) struct BootstrapRoundGuard {
    pacer: Arc<Mutex<BootstrapPacer>>,
    pub(crate) issued: bool,
    pub(crate) connected: bool,
}

impl BootstrapRoundGuard {
    /// Starts a round if none is running and the backoff has passed.
    pub(crate) fn try_begin(pacer: &Arc<Mutex<BootstrapPacer>>, now: Instant) -> Option<Self> {
        pacer.lock().try_begin(now).then(|| Self {
            pacer: pacer.clone(),
            issued: false,
            connected: false,
        })
    }
}

impl Drop for BootstrapRoundGuard {
    fn drop(&mut self) {
        self.pacer
            .lock()
            .end(Instant::now(), self.issued, self.connected);
    }
}

/// How often a path waiting for a bootstrap round re-checks while the other
/// path's round is running.
const ROUND_IN_FLIGHT_POLL: Duration = Duration::from_secs(1);

/// Waits until this peer may start a bootstrap round and starts it. Returns
/// `None` if `has_connection` turns true first. The wait is interruptible by
/// `cleared` (the gateway-backoff reset signal).
pub(crate) async fn wait_for_bootstrap_round(
    pacer: &Arc<Mutex<BootstrapPacer>>,
    cleared: &tokio::sync::Notify,
    has_connection: impl Fn() -> bool,
) -> Option<BootstrapRoundGuard> {
    loop {
        if has_connection() {
            return None;
        }
        let now = Instant::now();
        if let Some(round) = BootstrapRoundGuard::try_begin(pacer, now) {
            return Some(round);
        }
        // Either the backoff is running, or the other path's round is.
        let wake = pacer
            .lock()
            .wait_until(now)
            .unwrap_or(now + ROUND_IN_FLIGHT_POLL);
        crate::deterministic_select! {
            _ = tokio::time::sleep_until(wake) => {},
            _ = cleared.notified() => {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(start: Instant, secs: f64) -> Instant {
        start + Duration::from_secs_f64(secs)
    }

    #[test]
    fn bootstrap_round_waits_for_quiet_then_reports_how_it_ended() {
        let t0 = Instant::now();
        let mut round = BootstrapRound::new(true, t0);
        round.on_reply(at(t0, 0.2));
        round.on_dial_done(at(t0, 3.0), false);
        assert_eq!(
            round.exit(at(t0, 3.0 + 5.9), false),
            None,
            "still quiet-waiting"
        );
        assert_eq!(
            round.exit(at(t0, 3.0 + 6.0), false),
            Some(BootstrapExit::AllFailed)
        );
        round.on_dial_done(at(t0, 10.0), true);
        assert_eq!(
            round.exit(at(t0, 16.0), false),
            Some(BootstrapExit::Connected)
        );
    }

    #[test]
    fn bootstrap_round_never_exits_while_a_dial_is_pending() {
        let t0 = Instant::now();
        let round = BootstrapRound::new(true, t0);
        assert_eq!(round.exit(at(t0, 1000.0), true), None);
    }

    #[test]
    fn bootstrap_round_with_no_reply_at_all_reports_a_dead_link() {
        let t0 = Instant::now();
        let round = BootstrapRound::new(true, t0);
        assert_eq!(round.exit(at(t0, 6.0), false), Some(BootstrapExit::NoReply));
    }

    #[test]
    fn bootstrap_round_disabled_for_a_connected_peer_never_exits_early() {
        let t0 = Instant::now();
        let round = BootstrapRound::new(false, t0);
        assert_eq!(round.exit(at(t0, 1000.0), false), None);
    }

    #[test]
    fn quiet_window_covers_a_failed_punch_and_a_re_route() {
        assert!(
            BOOTSTRAP_REPLY_QUIET > crate::transport::connection_handler::NAT_TRAVERSAL_DEADLINE,
            "a re-routed acceptor answers after the failed acceptor's punch window"
        );
        assert!(
            BOOTSTRAP_REPLY_QUIET < crate::config::OPERATION_TTL / 4,
            "the point is to end well before the transaction TTL"
        );
    }

    #[test]
    fn dial_queue_dials_up_to_cap_concurrently_then_queues_in_order() {
        let mut q = AcceptorDialQueue::new(2);
        assert_eq!(q.offer('a'), Some('a'));
        assert_eq!(
            q.offer('b'),
            Some('b'),
            "a second acceptor is dialled at once"
        );
        assert_eq!(q.offer('c'), None);
        assert_eq!(q.offer('d'), None);
        assert!(q.busy());
        assert_eq!(q.finish(), Some('c'));
        assert_eq!(q.finish(), Some('d'));
        assert_eq!(q.finish(), None);
        assert!(q.busy(), "d is still in flight");
        assert_eq!(q.finish(), None);
        assert!(!q.busy());
    }

    #[test]
    fn dial_queue_cap_of_zero_still_dials_one() {
        let mut q = AcceptorDialQueue::new(0);
        assert_eq!(q.offer(1), Some(1));
        assert_eq!(q.offer(2), None);
    }

    #[test]
    fn production_dial_cap_dials_more_than_one_acceptor_at_a_time() {
        let mut q = AcceptorDialQueue::new(MAX_CONCURRENT_ACCEPTOR_DIALS);
        assert_eq!(q.offer(1), Some(1));
        assert_eq!(
            q.offer(2),
            Some(2),
            "head-of-line dialling lets a reachable acceptor's punch window expire"
        );
    }

    #[test]
    fn pacer_allows_one_round_at_a_time() {
        let pacer = Arc::new(Mutex::new(BootstrapPacer::new(Duration::from_secs(30))));
        let now = Instant::now();
        let first = BootstrapRoundGuard::try_begin(&pacer, now).expect("first round");
        assert!(BootstrapRoundGuard::try_begin(&pacer, now).is_none());
        assert!(pacer.lock().round_in_flight());
        drop(first);
        assert!(!pacer.lock().round_in_flight());
    }

    #[test]
    fn pacer_backs_off_failed_rounds_with_jitter_up_to_the_cap() {
        let ttl = Duration::from_secs(30);
        let mut pacer = BootstrapPacer::new(ttl);
        let now = Instant::now();
        let mut expected = BOOTSTRAP_RETRY_BASE;
        for _ in 0..8 {
            assert!(pacer.try_begin(now));
            pacer.end(now, true, false);
            let wait = pacer
                .wait_until(now)
                .expect("a failed round must back off")
                .duration_since(now);
            assert!(
                wait >= expected.mul_f64(0.8) && wait <= expected.mul_f64(1.2),
                "{wait:?} not within ±20% of {expected:?}"
            );
            assert!(
                !pacer.try_begin(now),
                "no round before the backoff has passed"
            );
            pacer.next_allowed = None;
            expected = (expected * 2).min(ttl);
        }
        assert!(
            ttl.mul_f64(1.2) + BOOTSTRAP_REPLY_QUIET < ttl * 3,
            "a failing joiner must reach each gateway within the zombie threshold"
        );
    }

    #[test]
    fn pacer_round_that_sent_nothing_does_not_back_off() {
        let pacer = Arc::new(Mutex::new(BootstrapPacer::new(Duration::from_secs(30))));
        let now = Instant::now();
        drop(BootstrapRoundGuard::try_begin(&pacer, now).expect("round"));
        assert_eq!(pacer.lock().wait_until(now), None);
        assert_eq!(pacer.lock().failures, 0);
        let mut issued = BootstrapRoundGuard::try_begin(&pacer, now).expect("round");
        issued.issued = true;
        drop(issued);
        assert!(
            pacer.lock().wait_until(now).is_some(),
            "a failed round backs off"
        );
    }

    #[test]
    fn pacer_resets_after_a_connected_round() {
        let mut pacer = BootstrapPacer::new(Duration::from_secs(30));
        let now = Instant::now();
        assert!(pacer.try_begin(now));
        pacer.end(now, true, false);
        pacer.next_allowed = None;
        assert!(pacer.try_begin(now));
        pacer.end(now, true, true);
        assert_eq!(pacer.wait_until(now), None);
        assert_eq!(pacer.failures, 0);
    }
}
