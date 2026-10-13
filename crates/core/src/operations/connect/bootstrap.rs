//! Timing of a joiner's CONNECTs while it has no ring connection (#5858).
//!
//! A fresh node joins by sending a CONNECT through each gateway. Each CONNECT
//! used to wait for `min_connections` acceptors or the 60 s transaction TTL,
//! and the join loop waited for every CONNECT of a round, so a joiner whose
//! first acceptors were unreachable sat idle for the full TTL before trying
//! again. Three pieces here bound that:
//!
//! - [`BootstrapRound`]: a CONNECT sent with no ring connection ends once
//!   nothing is in flight and nothing has arrived for a quiet window
//!   ([`BootstrapTiming`]), instead of at the TTL.
//! - [`AcceptorDialQueue`]: acceptors are dialled concurrently, up to
//!   [`MAX_CONCURRENT_ACCEPTOR_DIALS`], so an unreachable acceptor no longer
//!   holds up a reachable one until its hole-punch window has passed.
//! - [`BootstrapPacer`]: the join loop and the maintenance loop share one
//!   pacer, so at most one bootstrap round runs at a time and failed rounds
//!   are spaced by a jittered exponential backoff.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::Instant;

use crate::config::GlobalRng;
use crate::util::backoff::ExponentialBackoff;

/// How long a bootstrapping CONNECT waits after a dial outcome, with no dial
/// in flight, for one more reply before it ends ([`BootstrapTiming`] doubles
/// it before the first dial outcome).
///
/// Two hole-punch deadlines: a dial that fails at the end of the acceptor's
/// punch window makes the joiner send `ConnectFailed`, and the relay's
/// re-routed acceptor has to answer within this window.
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

/// Consecutive bootstrapping CONNECTs through one gateway that must hear
/// nothing at all before its link is treated as dead and redialled. One
/// silent round can be a slow or overloaded gateway (its restart admission
/// ramp, say), and dropping the link then would only add handshakes to it.
pub(crate) const SILENT_ROUNDS_BEFORE_REDIAL: u32 = 2;

/// Gateways whose silent-round count is tracked at once; well above any
/// configured gateway list, and only a bound on the map.
const MAX_TRACKED_GATEWAYS: usize = 64;

/// First wait after a bootstrap round that ended with no ring connection.
const BOOTSTRAP_RETRY_BASE: Duration = Duration::from_secs(1);

/// Lowest cap on the wait between failed rounds, whatever the transient TTL.
/// A short TTL (tests, or a misconfiguration) must not turn a total gateway
/// outage into a retry every few seconds.
const BOOTSTRAP_RETRY_CAP_FLOOR: Duration = Duration::from_secs(30);

/// Wait after a round that produced a ring connection, before another may
/// start. A connection that drops at once (a churning acceptor) would
/// otherwise let rounds run back to back.
const CONNECTED_ROUND_GAP: Duration = Duration::from_secs(5);

/// How long a ring connection must persist before the failure count resets.
/// Until then a lost connection resumes the backoff where it was.
const CONNECTION_PERSISTS: Duration = Duration::from_secs(30);

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

/// How long a bootstrapping CONNECT waits for its next reply (#5858).
///
/// Two windows, because the first reply takes longer than the later ones:
/// before any acceptor has been dialled the request still has to cross the
/// gateway and a chain of relays, and an acceptor's Response comes back the
/// same way; after a dial outcome the only thing left to wait for is a
/// re-routed acceptor answering a `ConnectFailed`. Both are provisional
/// until calibrated from the `time_to_first_ring_connection` telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BootstrapTiming {
    /// Quiet time allowed before the first dial outcome.
    pub(crate) before_first_dial: Duration,
    /// Quiet time allowed after a dial outcome.
    pub(crate) after_dial: Duration,
}

impl Default for BootstrapTiming {
    fn default() -> Self {
        Self {
            before_first_dial: 2 * BOOTSTRAP_REPLY_QUIET,
            after_dial: BOOTSTRAP_REPLY_QUIET,
        }
    }
}

/// Exit policy for one CONNECT sent while this peer had no ring connection.
///
/// Disabled (never exits early) for a peer that already had a connection when
/// the CONNECT started, which keeps the TTL-bounded collection that ring
/// acquisition relies on.
///
/// The quiet clock starts when the gateway link is up (the transport is
/// established, or the gateway has answered), so a slow handshake does not
/// eat the window. If the link is not up within the NAT traversal deadline
/// the handshake has failed, and the clock starts then.
#[derive(Debug)]
pub(crate) struct BootstrapRound {
    enabled: bool,
    timing: BootstrapTiming,
    started: Instant,
    link_up_at: Option<Instant>,
    last_activity: Option<Instant>,
    dial_outcome_seen: bool,
    heard_anything: bool,
    connected: bool,
}

impl BootstrapRound {
    pub(crate) fn new(enabled: bool, now: Instant, timing: BootstrapTiming) -> Self {
        Self {
            enabled,
            timing,
            started: now,
            link_up_at: None,
            last_activity: None,
            dial_outcome_seen: false,
            heard_anything: false,
            connected: false,
        }
    }

    /// Whether the link to the gateway is known to be up.
    pub(crate) fn link_up(&self) -> bool {
        self.link_up_at.is_some()
    }

    /// The gateway transport is established.
    pub(crate) fn on_link_up(&mut self, now: Instant) {
        self.link_up_at.get_or_insert(now);
    }

    /// Any message for this transaction arrived (which also shows the
    /// gateway link is up).
    pub(crate) fn on_reply(&mut self, now: Instant) {
        self.on_link_up(now);
        self.heard_anything = true;
        self.last_activity = Some(now);
    }

    /// A dial to an acceptor finished.
    pub(crate) fn on_dial_done(&mut self, now: Instant, connected: bool) {
        self.dial_outcome_seen = true;
        self.connected |= connected;
        self.last_activity = Some(now);
    }

    /// Whether the CONNECT should end now. `busy` is true while a dial is in
    /// flight or queued: its outcome may still produce a connection or a
    /// re-route.
    pub(crate) fn exit(&self, now: Instant, busy: bool) -> Option<BootstrapExit> {
        if !self.enabled || busy {
            return None;
        }
        let origin = self
            .link_up_at
            .unwrap_or(self.started + crate::transport::connection_handler::NAT_TRAVERSAL_DEADLINE);
        let reference = self.last_activity.map_or(origin, |t| t.max(origin));
        let window = if self.dial_outcome_seen {
            self.timing.after_dial
        } else {
            self.timing.before_first_dial
        };
        if now.saturating_duration_since(reference) < window {
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
    silent_rounds: HashMap<SocketAddr, u32>,
    /// Gateways that paced rounds have sent CONNECTs through, so whatever
    /// reservation they hold is a leftover of ours. A CONNECT sent outside a
    /// round (with a ring connection) is not recorded: its reservation may
    /// belong to a CONNECT still in flight.
    owned_reservations: HashSet<SocketAddr>,
    /// Since when this peer has had a ring connection without a break.
    connected_since: Option<Instant>,
}

impl BootstrapPacer {
    pub(crate) fn new(transient_ttl: Duration) -> Self {
        Self {
            backoff: ExponentialBackoff::new(
                BOOTSTRAP_RETRY_BASE,
                transient_ttl.max(BOOTSTRAP_RETRY_CAP_FLOOR),
            ),
            failures: 0,
            next_allowed: None,
            round_in_flight: false,
            silent_rounds: HashMap::new(),
            owned_reservations: HashSet::new(),
            connected_since: None,
        }
    }

    /// Records whether this peer has a ring connection, however it got it.
    /// Once a connection has lasted [`CONNECTION_PERSISTS`] the backoff
    /// starts over.
    pub(crate) fn observe_connections(&mut self, now: Instant, has_connection: bool) {
        if !has_connection {
            self.connected_since = None;
            return;
        }
        let since = *self.connected_since.get_or_insert(now);
        if now.saturating_duration_since(since) >= CONNECTION_PERSISTS {
            self.failures = 0;
            self.next_allowed = None;
        }
    }

    /// Gateway reservations left by finished paced rounds, which the caller
    /// may clear; forgets them.
    pub(crate) fn take_owned_reservations(&mut self) -> Vec<SocketAddr> {
        self.owned_reservations.drain().collect()
    }

    /// A bootstrapping CONNECT through `gateway` heard nothing. Returns true
    /// once that has happened [`SILENT_ROUNDS_BEFORE_REDIAL`] times in a row,
    /// and resets the count: the caller drops the link to redial it.
    pub(crate) fn gateway_silent(&mut self, gateway: SocketAddr) -> bool {
        if self.silent_rounds.len() >= MAX_TRACKED_GATEWAYS
            && !self.silent_rounds.contains_key(&gateway)
        {
            self.silent_rounds.clear();
        }
        let count = self.silent_rounds.entry(gateway).or_insert(0);
        *count += 1;
        if *count >= SILENT_ROUNDS_BEFORE_REDIAL {
            self.silent_rounds.remove(&gateway);
            return true;
        }
        false
    }

    /// A bootstrapping CONNECT through `gateway` heard back.
    pub(crate) fn gateway_answered(&mut self, gateway: SocketAddr) {
        self.silent_rounds.remove(&gateway);
    }

    /// The gateway backoff was cleared (isolation recovery, suspend): the
    /// next round may start at once. The failure count stays, so the round
    /// after that is still spaced.
    pub(crate) fn clear_wait(&mut self) {
        self.next_allowed = None;
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
        let jitter: f64 = GlobalRng::random_range(0.8_f64..=1.2_f64);
        let delay = if connected {
            // The failure count stays until the connection has persisted
            // (`observe_connections`).
            CONNECTED_ROUND_GAP
        } else {
            self.failures = self.failures.saturating_add(1);
            self.backoff.delay_for_failures(self.failures)
        };
        self.next_allowed = Some(now + delay.mul_f64(jitter));
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

    /// This round is sending a CONNECT through `gateway`, so the reservation
    /// it makes there is the round's own.
    pub(crate) fn own_reservation(&self, gateway: SocketAddr) {
        let mut pacer = self.pacer.lock();
        if pacer.owned_reservations.len() < MAX_TRACKED_GATEWAYS {
            pacer.owned_reservations.insert(gateway);
        }
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

    fn round(t0: Instant) -> BootstrapRound {
        BootstrapRound::new(true, t0, BootstrapTiming::default())
    }

    #[test]
    fn bootstrap_round_uses_the_long_window_until_a_dial_outcome() {
        let t0 = Instant::now();
        let timing = BootstrapTiming::default();
        let mut round = round(t0);
        round.on_reply(at(t0, 0.2));
        assert_eq!(round.exit(at(t0, 0.2) + timing.after_dial, false), None);
        assert_eq!(
            round.exit(at(t0, 0.2) + timing.before_first_dial, false),
            Some(BootstrapExit::AllFailed)
        );
    }

    #[test]
    fn bootstrap_round_uses_the_short_window_after_a_dial_outcome() {
        let t0 = Instant::now();
        let timing = BootstrapTiming::default();
        let mut round = round(t0);
        round.on_reply(at(t0, 0.2));
        round.on_dial_done(at(t0, 3.0), false);
        let after = at(t0, 3.0);
        assert_eq!(
            round.exit(after + timing.after_dial - Duration::from_millis(1), false),
            None
        );
        assert_eq!(
            round.exit(after + timing.after_dial, false),
            Some(BootstrapExit::AllFailed)
        );
        round.on_dial_done(at(t0, 10.0), true);
        assert_eq!(
            round.exit(at(t0, 10.0) + timing.after_dial, false),
            Some(BootstrapExit::Connected)
        );
    }

    #[test]
    fn bootstrap_round_quiet_clock_starts_when_the_link_is_up() {
        let t0 = Instant::now();
        let timing = BootstrapTiming::default();
        let mut round = round(t0);
        // A slow gateway handshake: the link comes up at 5 s.
        round.on_link_up(at(t0, 5.0));
        assert_eq!(
            round.exit(
                at(t0, 5.0) + timing.before_first_dial - Duration::from_millis(1),
                false
            ),
            None,
            "the window counts from the link coming up, not from the start"
        );
        assert_eq!(
            round.exit(at(t0, 5.0) + timing.before_first_dial, false),
            Some(BootstrapExit::NoReply)
        );
    }

    #[test]
    fn bootstrap_round_without_a_link_counts_from_the_handshake_deadline() {
        let t0 = Instant::now();
        let timing = BootstrapTiming::default();
        let round = round(t0);
        let deadline = t0 + crate::transport::connection_handler::NAT_TRAVERSAL_DEADLINE;
        assert_eq!(
            round.exit(
                deadline + timing.before_first_dial - Duration::from_millis(1),
                false
            ),
            None
        );
        assert_eq!(
            round.exit(deadline + timing.before_first_dial, false),
            Some(BootstrapExit::NoReply)
        );
    }

    #[test]
    fn bootstrap_round_never_exits_while_a_dial_is_pending() {
        let t0 = Instant::now();
        let round = round(t0);
        assert_eq!(round.exit(at(t0, 1000.0), true), None);
    }

    #[test]
    fn bootstrap_round_disabled_for_a_connected_peer_never_exits_early() {
        let t0 = Instant::now();
        let round = BootstrapRound::new(false, t0, BootstrapTiming::default());
        assert_eq!(round.exit(at(t0, 1000.0), false), None);
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
    fn a_gateway_link_is_redialled_only_after_consecutive_silent_rounds() {
        let mut pacer = BootstrapPacer::new(Duration::from_secs(30));
        let gw: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let other: SocketAddr = "127.0.0.1:2".parse().unwrap();
        assert!(!pacer.gateway_silent(gw), "one silent round is not enough");
        pacer.gateway_answered(gw);
        assert!(!pacer.gateway_silent(gw), "an answer resets the count");
        assert!(!pacer.gateway_silent(other), "counts are per gateway");
        assert!(pacer.gateway_silent(gw), "two in a row redial");
        assert!(!pacer.gateway_silent(gw), "and the count starts over");
    }

    #[test]
    fn only_reservations_of_paced_rounds_are_offered_for_clearing() {
        let pacer = Arc::new(Mutex::new(BootstrapPacer::new(Duration::from_secs(30))));
        let gw: SocketAddr = "127.0.0.1:1".parse().unwrap();
        assert!(pacer.lock().take_owned_reservations().is_empty());
        let round = BootstrapRoundGuard::try_begin(&pacer, Instant::now()).unwrap();
        round.own_reservation(gw);
        drop(round);
        assert_eq!(pacer.lock().take_owned_reservations(), vec![gw]);
        assert!(
            pacer.lock().take_owned_reservations().is_empty(),
            "taken once, then forgotten"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_cleared_gateway_backoff_lets_the_next_round_start_at_once() {
        let pacer = Arc::new(Mutex::new(BootstrapPacer::new(Duration::from_secs(30))));
        let cleared = tokio::sync::Notify::new();
        for _ in 0..6 {
            pacer.lock().next_allowed = None;
            assert!(pacer.lock().try_begin(Instant::now()));
            pacer.lock().end(Instant::now(), true, false);
        }
        assert!(pacer.lock().wait_until(Instant::now()).is_some());
        let start = Instant::now();
        let waiter = wait_for_bootstrap_round(&pacer, &cleared, || false);
        let clear = async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            // What `reset_all_backoff` does.
            pacer.lock().clear_wait();
            cleared.notify_waiters();
        };
        let (round, ()) = tokio::join!(waiter, clear);
        assert!(round.is_some());
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "the round waited {:?} past the clear",
            start.elapsed()
        );
    }

    #[test]
    fn a_short_transient_ttl_does_not_lower_the_backoff_cap() {
        let mut pacer = BootstrapPacer::new(Duration::from_secs(2));
        let now = Instant::now();
        for _ in 0..10 {
            pacer.next_allowed = None;
            assert!(pacer.try_begin(now));
            pacer.end(now, true, false);
        }
        let wait = pacer.wait_until(now).unwrap() - now;
        assert!(
            wait >= BOOTSTRAP_RETRY_CAP_FLOOR.mul_f64(0.8),
            "a 2 s TTL capped the backoff at {wait:?}"
        );
    }

    #[test]
    fn a_connected_round_keeps_a_gap_and_the_failure_count() {
        let mut pacer = BootstrapPacer::new(Duration::from_secs(30));
        let now = Instant::now();
        assert!(pacer.try_begin(now));
        pacer.end(now, true, false);
        pacer.next_allowed = None;
        assert!(pacer.try_begin(now));
        pacer.end(now, true, true);
        let gap = pacer
            .wait_until(now)
            .expect("a connected round still spaces the next");
        let gap = gap - now;
        assert!(
            gap >= CONNECTED_ROUND_GAP.mul_f64(0.8) && gap <= CONNECTED_ROUND_GAP.mul_f64(1.2),
            "gap {gap:?}"
        );
        assert_eq!(
            pacer.failures, 1,
            "a connection that may churn resets nothing"
        );
    }

    #[test]
    fn a_persistent_connection_resets_the_backoff() {
        let mut pacer = BootstrapPacer::new(Duration::from_secs(30));
        let t0 = Instant::now();
        for _ in 0..4 {
            pacer.next_allowed = None;
            assert!(pacer.try_begin(t0));
            pacer.end(t0, true, false);
        }
        // A connection gained by any path: not the pacer's own round.
        pacer.observe_connections(t0, true);
        pacer.observe_connections(t0 + CONNECTION_PERSISTS - Duration::from_millis(1), true);
        assert_eq!(pacer.failures, 4, "not yet persistent");
        // A drop restarts the clock.
        pacer.observe_connections(t0 + CONNECTION_PERSISTS, false);
        pacer.observe_connections(t0 + CONNECTION_PERSISTS, true);
        pacer.observe_connections(
            t0 + 2 * CONNECTION_PERSISTS - Duration::from_millis(1),
            true,
        );
        assert_eq!(pacer.failures, 4, "the clock restarts after a drop");
        let later = t0 + 2 * CONNECTION_PERSISTS;
        pacer.observe_connections(later, true);
        assert_eq!(pacer.failures, 0);
        assert_eq!(pacer.wait_until(later), None);
    }
}
