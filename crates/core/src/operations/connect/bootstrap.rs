//! Timing of a joiner's CONNECTs while it has no ring connection (#5858).
//!
//! A fresh node joins by sending a CONNECT through each gateway. The join loop
//! used to wait for every CONNECT of a round to run to its 60 s transaction
//! TTL before sending the next, so a joiner whose first CONNECTs went nowhere
//! sat idle for a full TTL. Live replies are too slow to cut a CONNECT short
//! instead (first reply p50 3.5 s but p95 41 s; 37% of CONNECTs that are
//! answered at all are answered after 6 s), so nothing here abandons one:
//!
//! - [`BootstrapPacer`]: rounds overlap. The join loop and the maintenance
//!   loop share one pacer; a round may start once the backoff since the last
//!   one has passed, without waiting for earlier rounds' CONNECTs, and at most
//!   [`MAX_OUTSTANDING_BOOTSTRAP_ROUNDS`] rounds are outstanding.
//! - [`AcceptorDialQueue`]: acceptors are dialled concurrently, up to
//!   [`MAX_CONCURRENT_ACCEPTOR_DIALS`], so an unreachable acceptor no longer
//!   holds up a reachable one until its hole-punch window has passed.
//! - Gateway liveness: a CONNECT that starts without this peer knowing its
//!   own address gets an `ObservedAddress` from the gateway as soon as the
//!   gateway has it. [`SILENT_ROUNDS_BEFORE_REDIAL`] such CONNECTs in a row
//!   through a configured gateway without one by [`GATEWAY_ANSWER_DEADLINE`]
//!   mark the link as dead, and it is dropped for a fresh handshake, at most
//!   once per [`MIN_REDIAL_INTERVAL`].
//!
//! Request rate when no gateway answers at all: the backoff alone allows
//! rounds at 0, 5, 15, 35, 65 and 95 s and then one per 30 s (each wait
//! +-20%); the cap can only delay them (if every CONNECT lives its full 60 s
//! TTL: 0, 5, 15, 60, 90, 120 s). At most six rounds in the first 100 s where
//! the TTL-bound loop managed two, and one round per ~30 s after that against
//! one per ~62 s, about 2x. Each round sends one CONNECT per eligible gateway,
//! as before. That doubles CONNECT messages, not handshakes: the per-address
//! dial backoff still bounds the handshakes that reach the wire at about
//! main's rate.
//!
//! Paced rounds put at most [`MAX_OUTSTANDING_BOOTSTRAP_ROUNDS`] of this
//! peer's bootstrap CONNECTs on a gateway at once; a join-loop round started
//! while the peer still had a connection is unpaced and can add one more. The
//! cost in a healthy mass join is up to three times the relayed CONNECT state
//! at gateways, and three times the acceptor reservations, per joiner while it
//! has no connection.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::Instant;

use crate::config::GlobalRng;
use crate::util::backoff::ExponentialBackoff;

/// Acceptors of one CONNECT dialled at the same time.
///
/// A CONNECT's acceptors answer within a second or two of each other, and each
/// hole punch lasts up to the NAT traversal deadline on both sides. Dialled
/// one at a time, the second acceptor's punch window has closed by the time
/// the first unreachable one gives up. Four covers the bursts seen in the
/// saturated-join simulation (two to four acceptors per CONNECT) while
/// bounding the parallel handshakes a single CONNECT can start.
pub(crate) const MAX_CONCURRENT_ACCEPTOR_DIALS: usize = 4;

/// Bootstrap rounds whose CONNECTs may be outstanding at once.
///
/// A round sends one CONNECT per gateway, so this is also the most of this
/// peer's paced bootstrap CONNECTs any one gateway sees at a time (an unpaced
/// round started while the peer still had a connection can add one). Three
/// lets the
/// joiner retry twice while its first CONNECTs are still in the slow tail of
/// live replies (p90 27 s), and stops it there: more rounds into gateways that
/// have not answered three only multiply the load on them.
pub(crate) const MAX_OUTSTANDING_BOOTSTRAP_ROUNDS: usize = 3;

/// Consecutive bootstrapping CONNECTs through one gateway that must miss its
/// `ObservedAddress` deadline before the link is treated as dead and
/// redialled. One can be a slow or overloaded gateway (its restart admission
/// ramp, say), and dropping the link then would only add handshakes to it.
pub(crate) const SILENT_ROUNDS_BEFORE_REDIAL: u32 = 2;

/// How long a gateway join waits for the gateway's `ObservedAddress` before
/// it counts as silent. The gateway sends it on receipt, so this covers the
/// handshake (the 3 s NAT traversal deadline) and a long queue at a busy
/// gateway; a link that misses it twice in a row is redialled no sooner than
/// 20 s after the first round (rounds at 0 and 5 s). The CONNECT itself keeps
/// collecting replies either way.
pub(crate) const GATEWAY_ANSWER_DEADLINE: Duration = Duration::from_secs(15);

/// Least time between two redials of one gateway's link. A gateway that is
/// slow rather than gone would otherwise be re-handshaken about once a
/// minute by every joiner, landing on a gateway that is already behind; a
/// dead link still recovers within this.
const MIN_REDIAL_INTERVAL: Duration = Duration::from_secs(120);

/// Gateways whose liveness is tracked at once; well above any configured
/// gateway list, and only a bound on the map.
const MAX_TRACKED_GATEWAYS: usize = 64;

/// Wait after the first bootstrap round before the second.
///
/// Live CONNECTs get their first reply after 3.5 s at the median, so a round
/// that has heard nothing after 5 s is in the slower half: worth a second
/// round alongside it, without doubling every join that was about to succeed.
const BOOTSTRAP_RETRY_BASE: Duration = Duration::from_secs(5);

/// Lowest cap on the wait between rounds, whatever the transient TTL. A short
/// TTL (tests, or a misconfiguration) must not turn a total gateway outage
/// into a retry every few seconds.
const BOOTSTRAP_RETRY_CAP_FLOOR: Duration = Duration::from_secs(30);

/// How long a ring connection must persist before the backoff resets. Until
/// then a lost connection resumes the backoff where it was, so an acceptor
/// that connects and drops at once does not let rounds run back to back.
const CONNECTION_PERSISTS: Duration = Duration::from_secs(30);

/// Most acceptors one CONNECT will dial over its life, re-routes included.
///
/// A CONNECT collects at most `target_connections` acceptors, so twice that
/// leaves room for an acceptor re-routed after each failed punch. Without a
/// bound a misbehaving first hop could keep sending distinct Responses for the
/// whole TTL and have this peer dial every one.
pub(crate) fn acceptor_limit(target_connections: usize) -> usize {
    target_connections
        .saturating_mul(2)
        .max(MAX_CONCURRENT_ACCEPTOR_DIALS)
}

/// What [`AcceptorDialQueue::offer`] did with an acceptor.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Offer<T> {
    /// Dial it now.
    Dial(T),
    /// Queued until a dial finishes.
    Queued,
    /// The CONNECT has already taken its limit of acceptors; ignore it.
    Refused,
}

/// Bounded set of acceptor dials for one CONNECT: up to `cap` in flight, the
/// rest queued in arrival order, and no more than `limit` taken in all.
#[derive(Debug)]
pub(crate) struct AcceptorDialQueue<T> {
    cap: usize,
    limit: usize,
    taken: usize,
    in_flight: usize,
    queued: VecDeque<T>,
}

impl<T> AcceptorDialQueue<T> {
    pub(crate) fn new(cap: usize, limit: usize) -> Self {
        Self {
            cap: cap.max(1),
            limit,
            taken: 0,
            in_flight: 0,
            queued: VecDeque::new(),
        }
    }

    /// An acceptor answered: dial it now, queue it, or refuse it once the
    /// CONNECT has taken `limit` acceptors.
    pub(crate) fn offer(&mut self, acceptor: T) -> Offer<T> {
        if self.taken >= self.limit {
            return Offer::Refused;
        }
        self.taken += 1;
        if self.in_flight < self.cap {
            self.in_flight += 1;
            Offer::Dial(acceptor)
        } else {
            self.queued.push_back(acceptor);
            Offer::Queued
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

/// What this peer has heard from one gateway's bootstrapping CONNECTs.
#[derive(Debug, Default)]
struct GatewayLiveness {
    /// Consecutive CONNECTs that ended without an `ObservedAddress`.
    silent: u32,
    /// When the gateway last answered.
    last_answer: Option<Instant>,
    /// When this peer last dropped the link to redial it.
    redialled: Option<Instant>,
    /// `redialled` before the latest redial, restored if that redial could
    /// not be sent.
    previous_redial: Option<Instant>,
    /// When a bootstrap CONNECT's rejection last backed the gateway off.
    backed_off: Option<Instant>,
}

/// Spacing of bootstrap rounds, shared by `initial_join_procedure` and the
/// maintenance loop's zero-connection bootstrap.
///
/// A round is a set of CONNECTs, one per eligible gateway, sent at once. Each
/// round started grows the backoff before the next may start
/// (`backoff.delay_for_failures(n)` +-20%, `n` counting rounds since the last
/// persistent connection), and at most [`MAX_OUTSTANDING_BOOTSTRAP_ROUNDS`]
/// rounds may have CONNECTs still running. The cap on the backoff is the
/// transient TTL (at least 30 s), so a joiner that keeps failing still sends
/// each gateway a request well within the gateway's zombie-transport
/// threshold (three transient TTLs) and its gateway links are not reaped
/// while it is still joining.
#[derive(Debug)]
pub(crate) struct BootstrapPacer {
    backoff: ExponentialBackoff,
    failures: u32,
    next_allowed: Option<Instant>,
    /// A path has taken a round and is deciding what to send.
    issuing: bool,
    /// Rounds taken whose CONNECTs have not all ended.
    outstanding: usize,
    liveness: HashMap<SocketAddr, GatewayLiveness>,
    /// Gateways that paced rounds have sent CONNECTs through, and when: the
    /// reservation each made there is ours and bounded by this pacer. A
    /// CONNECT sent outside a round (with a ring connection) is not recorded,
    /// and the time keeps a later, unrelated reservation at the same gateway
    /// from being taken for ours (`ConnectionManager::clear_owned_reservations`).
    owned_reservations: HashMap<SocketAddr, Instant>,
    /// Since when this peer has had a ring connection without a break.
    connected_since: Option<Instant>,
    /// When the last round was issued.
    last_issued: Option<Instant>,
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
            issuing: false,
            outstanding: 0,
            liveness: HashMap::new(),
            owned_reservations: HashMap::new(),
            connected_since: None,
            last_issued: None,
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

    /// Gateway reservations made by paced rounds, with when each round sent
    /// its CONNECT, which the caller may clear; forgets them.
    pub(crate) fn take_owned_reservations(&mut self) -> Vec<(SocketAddr, Instant)> {
        self.owned_reservations.drain().collect()
    }

    fn liveness(&mut self, gateway: SocketAddr) -> &mut GatewayLiveness {
        if self.liveness.len() >= MAX_TRACKED_GATEWAYS && !self.liveness.contains_key(&gateway) {
            self.liveness.clear();
        }
        self.liveness.entry(gateway).or_default()
    }

    /// The gateway answered a bootstrapping CONNECT.
    pub(crate) fn gateway_answered(&mut self, gateway: SocketAddr, now: Instant) {
        let liveness = self.liveness(gateway);
        liveness.silent = 0;
        liveness.last_answer = Some(now);
    }

    /// A bootstrapping CONNECT through `gateway`, sent at `sent`, has had no
    /// `ObservedAddress` from the gateway by [`GATEWAY_ANSWER_DEADLINE`]. Returns true once that has
    /// happened [`SILENT_ROUNDS_BEFORE_REDIAL`] times in a row, and resets the
    /// count: the caller drops the link to redial it. Rounds overlap, so a
    /// CONNECT sent before the gateway's last answer, or before the last
    /// redial (it went over the old link), says nothing about the link now and
    /// is not counted. A link is redialled at most once per
    /// [`MIN_REDIAL_INTERVAL`].
    pub(crate) fn gateway_silent(
        &mut self,
        gateway: SocketAddr,
        sent: Instant,
        now: Instant,
    ) -> bool {
        let liveness = self.liveness(gateway);
        let stale = |t: Option<Instant>| t.is_some_and(|t| t >= sent);
        if stale(liveness.last_answer) || stale(liveness.redialled) {
            return false;
        }
        liveness.silent += 1;
        if liveness.silent < SILENT_ROUNDS_BEFORE_REDIAL {
            return false;
        }
        if liveness
            .redialled
            .is_some_and(|t| now.saturating_duration_since(t) < MIN_REDIAL_INTERVAL)
        {
            return false;
        }
        liveness.silent = 0;
        liveness.previous_redial = liveness.redialled;
        liveness.redialled = Some(now);
        true
    }

    /// A bootstrapping CONNECT through `gateway`, sent at `sent`, was
    /// rejected. Returns whether to back the gateway off: not if it was
    /// already backed off after this CONNECT went out, so overlapping
    /// CONNECTs that were in flight together escalate the backoff once.
    pub(crate) fn gateway_rejected(
        &mut self,
        gateway: SocketAddr,
        sent: Instant,
        now: Instant,
    ) -> bool {
        let liveness = self.liveness(gateway);
        if liveness.backed_off.is_some_and(|t| t >= sent) {
            return false;
        }
        liveness.backed_off = Some(now);
        true
    }

    /// The redial [`Self::gateway_silent`] asked for could not be sent:
    /// forget it (the previous real redial's time stands), and leave the link
    /// one silent join short of the next.
    pub(crate) fn redial_not_sent(&mut self, gateway: SocketAddr) {
        let liveness = self.liveness(gateway);
        liveness.redialled = liveness.previous_redial;
        liveness.silent = SILENT_ROUNDS_BEFORE_REDIAL.saturating_sub(1);
    }

    /// The gateway backoff was cleared (isolation recovery, suspend). If no
    /// round has started for [`BOOTSTRAP_RETRY_CAP_FLOOR`], the wait may have
    /// been set long ago (before a suspend, say): the next round may start
    /// within a jittered [`BOOTSTRAP_RETRY_BASE`]. Otherwise the wait stands.
    /// Isolation recovery fires about every 30 s while a peer has never
    /// connected, so clearing the wait outright would cut the backoff short
    /// on every reset, and the jitter keeps peers whose timers started
    /// together from firing at once. The failure count stays either way.
    pub(crate) fn backoff_reset(&mut self, now: Instant) {
        let recent = self
            .last_issued
            .is_some_and(|t| now.saturating_duration_since(t) < BOOTSTRAP_RETRY_CAP_FLOOR);
        if recent {
            return;
        }
        let jitter: f64 = GlobalRng::random_range(0.0_f64..=1.0_f64);
        self.next_allowed = Some(now + BOOTSTRAP_RETRY_BASE.mul_f64(jitter));
    }

    /// When the next round may start, if it has to wait for the backoff.
    pub(crate) fn wait_until(&self, now: Instant) -> Option<Instant> {
        self.next_allowed.filter(|t| *t > now)
    }

    /// Rounds whose CONNECTs are still running.
    #[cfg(test)]
    pub(crate) fn outstanding(&self) -> usize {
        self.outstanding
    }

    fn try_begin(&mut self, now: Instant) -> bool {
        if self.issuing
            || self.outstanding >= MAX_OUTSTANDING_BOOTSTRAP_ROUNDS
            || self.wait_until(now).is_some()
        {
            return false;
        }
        self.issuing = true;
        self.outstanding += 1;
        true
    }

    fn issue(&mut self, now: Instant) {
        self.issuing = false;
        self.last_issued = Some(now);
        self.failures = self.failures.saturating_add(1);
        let jitter: f64 = GlobalRng::random_range(0.8_f64..=1.2_f64);
        let delay = self.backoff.delay_for_failures(self.failures);
        self.next_allowed = Some(now + delay.mul_f64(jitter));
    }

    fn end(&mut self, issued: bool) {
        if !issued {
            // Nothing was sent (e.g. every gateway in backoff): not a round,
            // so it does not grow the backoff.
            self.issuing = false;
        }
        self.outstanding = self.outstanding.saturating_sub(1);
    }
}

/// A bootstrap round taken from the pacer. Call [`Self::issue`] when the
/// round sends its CONNECTs, then keep the guard until they have all ended:
/// dropping it frees the round's slot. Dropped without issuing, the round
/// leaves the backoff unchanged.
#[derive(Debug)]
pub(crate) struct BootstrapRoundGuard {
    pacer: Arc<Mutex<BootstrapPacer>>,
    issued: bool,
}

impl BootstrapRoundGuard {
    /// Takes a round if no other path is taking one, the backoff has passed,
    /// and fewer than [`MAX_OUTSTANDING_BOOTSTRAP_ROUNDS`] are outstanding.
    pub(crate) fn try_begin(pacer: &Arc<Mutex<BootstrapPacer>>, now: Instant) -> Option<Self> {
        pacer.lock().try_begin(now).then(|| Self {
            pacer: pacer.clone(),
            issued: false,
        })
    }

    /// The round is sending its CONNECTs: start the backoff to the next one.
    pub(crate) fn issue(&mut self) {
        if !self.issued {
            self.issued = true;
            self.pacer.lock().issue(Instant::now());
        }
    }

    /// This round is sending a CONNECT through `gateway`, so the reservation
    /// it makes there is the round's own.
    pub(crate) fn own_reservation(&self, gateway: SocketAddr) {
        let mut pacer = self.pacer.lock();
        if pacer.owned_reservations.len() < MAX_TRACKED_GATEWAYS {
            pacer.owned_reservations.insert(gateway, Instant::now());
        }
    }
}

impl Drop for BootstrapRoundGuard {
    fn drop(&mut self) {
        self.pacer.lock().end(self.issued);
    }
}

/// How often a path waiting for a bootstrap round re-checks while it is held
/// by the round cap or by the other path.
const ROUND_POLL: Duration = Duration::from_secs(1);

/// Waits until this peer may start a bootstrap round and takes it. Returns
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
        // The backoff is running, the cap is reached, or the other path is
        // taking a round.
        let wake = pacer
            .lock()
            .wait_until(now)
            .unwrap_or(now + ROUND_POLL)
            .min(now + ROUND_POLL);
        crate::deterministic_select! {
            _ = tokio::time::sleep_until(wake) => {},
            _ = cleared.notified() => {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pacer(ttl_secs: u64) -> Arc<Mutex<BootstrapPacer>> {
        Arc::new(Mutex::new(BootstrapPacer::new(Duration::from_secs(
            ttl_secs,
        ))))
    }

    #[test]
    fn dial_queue_dials_up_to_cap_concurrently_then_queues_in_order() {
        let mut q = AcceptorDialQueue::new(2, 10);
        assert_eq!(q.offer('a'), Offer::Dial('a'));
        assert_eq!(
            q.offer('b'),
            Offer::Dial('b'),
            "a second acceptor is dialled at once"
        );
        assert_eq!(q.offer('c'), Offer::Queued);
        assert_eq!(q.offer('d'), Offer::Queued);
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
        let mut q = AcceptorDialQueue::new(0, 10);
        assert_eq!(q.offer(1), Offer::Dial(1));
        assert_eq!(q.offer(2), Offer::Queued);
    }

    #[test]
    fn production_dial_cap_dials_more_than_one_acceptor_at_a_time() {
        let mut q = AcceptorDialQueue::new(MAX_CONCURRENT_ACCEPTOR_DIALS, 10);
        assert_eq!(q.offer(1), Offer::Dial(1));
        assert_eq!(
            q.offer(2),
            Offer::Dial(2),
            "head-of-line dialling lets a reachable acceptor's punch window expire"
        );
    }

    #[test]
    fn dial_queue_takes_no_more_than_its_limit_over_the_connects_life() {
        let mut q = AcceptorDialQueue::new(1, 3);
        assert_eq!(q.offer(1), Offer::Dial(1));
        assert_eq!(q.offer(2), Offer::Queued);
        assert_eq!(q.finish(), Some(2));
        assert_eq!(q.finish(), None);
        assert_eq!(q.offer(3), Offer::Dial(3));
        assert_eq!(
            q.offer(4),
            Offer::Refused,
            "finished dials still count: a first hop cannot refill the queue"
        );
        assert!(acceptor_limit(25) >= 25);
        assert!(acceptor_limit(0) >= 1);
    }

    #[test]
    fn one_path_takes_a_round_at_a_time() {
        let pacer = pacer(30);
        let now = Instant::now();
        let round = BootstrapRoundGuard::try_begin(&pacer, now).unwrap();
        assert!(
            BootstrapRoundGuard::try_begin(&pacer, now).is_none(),
            "the other path waits while this one decides what to send"
        );
        drop(round);
        assert!(BootstrapRoundGuard::try_begin(&pacer, now).is_some());
    }

    #[test]
    fn rounds_overlap_up_to_the_cap_and_a_finished_one_frees_a_slot() {
        let pacer = pacer(30);
        let mut rounds = Vec::new();
        for _ in 0..MAX_OUTSTANDING_BOOTSTRAP_ROUNDS {
            let now = Instant::now();
            pacer.lock().next_allowed = None;
            let mut round = BootstrapRoundGuard::try_begin(&pacer, now)
                .expect("an earlier round's CONNECTs still running must not block the next");
            round.issue();
            rounds.push(round);
        }
        pacer.lock().next_allowed = None;
        assert!(
            BootstrapRoundGuard::try_begin(&pacer, Instant::now()).is_none(),
            "no more than {MAX_OUTSTANDING_BOOTSTRAP_ROUNDS} rounds outstanding"
        );
        drop(rounds.remove(0));
        assert_eq!(
            pacer.lock().outstanding(),
            MAX_OUTSTANDING_BOOTSTRAP_ROUNDS - 1
        );
        assert!(BootstrapRoundGuard::try_begin(&pacer, Instant::now()).is_some());
    }

    #[test]
    fn each_round_grows_the_backoff_with_jitter_up_to_the_cap() {
        let mut pacer = BootstrapPacer::new(Duration::from_secs(30));
        let now = Instant::now();
        let mut waits = Vec::new();
        for _ in 0..8 {
            pacer.next_allowed = None;
            pacer.outstanding = 0;
            assert!(pacer.try_begin(now));
            pacer.issue(now);
            pacer.end(true);
            let wait = pacer.wait_until(now).expect("an issued round backs off") - now;
            assert!(
                !pacer.try_begin(now),
                "the next round waits for the backoff"
            );
            waits.push(wait);
        }
        let first = waits[0];
        assert!(
            first >= BOOTSTRAP_RETRY_BASE.mul_f64(0.8)
                && first <= BOOTSTRAP_RETRY_BASE.mul_f64(1.2),
            "first wait {first:?}"
        );
        let cap = Duration::from_secs(30);
        for wait in &waits {
            assert!(*wait <= cap.mul_f64(1.2), "wait {wait:?} beyond the cap");
        }
        assert!(waits[7] >= cap.mul_f64(0.8), "the backoff reaches the cap");
    }

    #[test]
    fn a_round_that_sent_nothing_does_not_back_off() {
        let pacer = pacer(30);
        let now = Instant::now();
        drop(BootstrapRoundGuard::try_begin(&pacer, now).unwrap());
        assert_eq!(pacer.lock().wait_until(now), None);
        assert_eq!(pacer.lock().failures, 0);
        assert_eq!(pacer.lock().outstanding(), 0);
    }

    #[test]
    fn a_gateway_link_is_redialled_only_after_consecutive_silent_rounds() {
        let mut pacer = BootstrapPacer::new(Duration::from_secs(30));
        let gw: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let other: SocketAddr = "127.0.0.1:2".parse().unwrap();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        assert!(
            !pacer.gateway_silent(gw, at(0), at(0)),
            "one silent round is not enough"
        );
        pacer.gateway_answered(gw, at(1));
        assert!(
            !pacer.gateway_silent(gw, at(2), at(2)),
            "an answer resets the count"
        );
        assert!(
            !pacer.gateway_silent(other, at(2), at(2)),
            "counts are per gateway"
        );
        assert!(
            pacer.gateway_silent(gw, at(3), at(3)),
            "two in a row redial"
        );
        assert!(
            !pacer.gateway_silent(gw, at(4), at(4)),
            "and the count starts over"
        );
    }

    /// Redial `gw` at 20 s: silent joins sent at 0 and 5 s.
    fn redialled_at_20(pacer: &mut BootstrapPacer, gw: SocketAddr, at: impl Fn(u64) -> Instant) {
        assert!(!pacer.gateway_silent(gw, at(0), at(15)));
        assert!(pacer.gateway_silent(gw, at(5), at(20)), "redial at 20 s");
    }

    #[test]
    fn connects_sent_over_the_old_link_do_not_count_after_a_redial() {
        let mut pacer = BootstrapPacer::new(Duration::from_secs(30));
        let gw: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        redialled_at_20(&mut pacer, gw, at);
        // Joins that went over the old link, judged after the redial.
        assert!(!pacer.gateway_silent(gw, at(15), at(30)));
        assert!(!pacer.gateway_silent(gw, at(18), at(33)));
        // Well past the interval, the new link needs two silent joins of
        // its own: the old ones above must not have been counted.
        let later = 30 + MIN_REDIAL_INTERVAL.as_secs();
        assert!(
            !pacer.gateway_silent(gw, at(later), at(later + 15)),
            "joins sent before the redial said nothing about the new link"
        );
        assert!(pacer.gateway_silent(gw, at(later + 5), at(later + 20)));
    }

    #[test]
    fn a_redial_that_was_not_sent_is_retried_by_the_next_silent_join() {
        let mut pacer = BootstrapPacer::new(Duration::from_secs(30));
        let gw: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        redialled_at_20(&mut pacer, gw, at);
        pacer.redial_not_sent(gw);
        assert!(
            pacer.gateway_silent(gw, at(10), at(25)),
            "the next silent join redials at once, not after the interval"
        );
    }

    #[test]
    fn overlapping_rejected_connects_back_a_gateway_off_once() {
        let mut pacer = BootstrapPacer::new(Duration::from_secs(30));
        let gw: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        // Rounds at 0, 5 and 15 s, all rejected after the first backoff.
        assert!(pacer.gateway_rejected(gw, at(0), at(16)));
        assert!(!pacer.gateway_rejected(gw, at(5), at(17)));
        assert!(!pacer.gateway_rejected(gw, at(15), at(18)));
        assert!(
            pacer.gateway_rejected(gw, at(60), at(62)),
            "a CONNECT sent after the backoff escalates it again"
        );
    }

    #[test]
    fn an_unsent_redial_keeps_the_previous_redials_time() {
        let mut pacer = BootstrapPacer::new(Duration::from_secs(30));
        let gw: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        redialled_at_20(&mut pacer, gw, at);
        // A second redial, well after the interval, that cannot be sent.
        let later = 20 + MIN_REDIAL_INTERVAL.as_secs() + 10;
        assert!(!pacer.gateway_silent(gw, at(later), at(later + 15)));
        assert!(pacer.gateway_silent(gw, at(later + 5), at(later + 20)));
        pacer.redial_not_sent(gw);
        // Joins sent before the real redial at 20 s still do not count.
        assert!(
            !pacer.gateway_silent(gw, at(10), at(later + 25)),
            "the real redial's time must survive an unsent one"
        );
    }

    #[test]
    fn a_link_is_redialled_at_most_once_per_interval() {
        let mut pacer = BootstrapPacer::new(Duration::from_secs(30));
        let gw: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        redialled_at_20(&mut pacer, gw, at);
        assert!(!pacer.gateway_silent(gw, at(25), at(40)));
        assert!(
            !pacer.gateway_silent(gw, at(30), at(45)),
            "no second redial within MIN_REDIAL_INTERVAL"
        );
        let later = 20 + MIN_REDIAL_INTERVAL.as_secs();
        assert!(
            pacer.gateway_silent(gw, at(later - 10), at(later)),
            "a still-silent link is redialled once the interval has passed"
        );
    }

    #[test]
    fn a_connect_sent_before_the_last_answer_is_not_counted_as_silent() {
        let mut pacer = BootstrapPacer::new(Duration::from_secs(30));
        let gw: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        // Rounds at 0 and 5 s; the gateway answers the 15 s round at once.
        pacer.gateway_answered(gw, at(15));
        assert!(!pacer.gateway_silent(gw, at(0), at(0)));
        assert!(
            !pacer.gateway_silent(gw, at(5), at(5)),
            "CONNECTs from before the gateway answered say nothing about the link now"
        );
        assert!(!pacer.gateway_silent(gw, at(20), at(20)));
        assert!(pacer.gateway_silent(gw, at(25), at(25)));
    }

    #[test]
    fn only_reservations_of_paced_rounds_are_offered_for_clearing() {
        let pacer = pacer(30);
        let gw: SocketAddr = "127.0.0.1:1".parse().unwrap();
        assert!(pacer.lock().take_owned_reservations().is_empty());
        let round = BootstrapRoundGuard::try_begin(&pacer, Instant::now()).unwrap();
        round.own_reservation(gw);
        drop(round);
        let owned = pacer.lock().take_owned_reservations();
        assert_eq!(owned.iter().map(|(a, _)| *a).collect::<Vec<_>>(), vec![gw]);
        assert!(
            pacer.lock().take_owned_reservations().is_empty(),
            "taken once, then forgotten"
        );
    }

    /// Issue `n` rounds back to back, leaving the backoff of the last.
    fn issue_rounds(pacer: &Arc<Mutex<BootstrapPacer>>, n: usize, at: Instant) {
        for _ in 0..n {
            let mut p = pacer.lock();
            p.next_allowed = None;
            p.outstanding = 0;
            assert!(p.try_begin(at));
            p.issue(at);
            p.end(true);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_backoff_reset_long_after_the_last_round_lets_the_next_start_soon() {
        let pacer = pacer(600);
        let cleared = tokio::sync::Notify::new();
        issue_rounds(&pacer, 12, Instant::now());
        // A long wait from before a suspend; the clock has moved on since.
        tokio::time::sleep(BOOTSTRAP_RETRY_CAP_FLOOR).await;
        assert!(pacer.lock().wait_until(Instant::now()).is_some());
        let start = Instant::now();
        let waiter = wait_for_bootstrap_round(&pacer, &cleared, || false);
        let clear = async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            // What `reset_all_backoff` does.
            pacer.lock().backoff_reset(Instant::now());
            cleared.notify_waiters();
        };
        let (round, ()) = tokio::join!(waiter, clear);
        assert!(round.is_some());
        assert!(
            start.elapsed() <= Duration::from_secs(1) + BOOTSTRAP_RETRY_BASE + ROUND_POLL,
            "the round waited {:?} past the reset",
            start.elapsed()
        );
    }

    /// Isolation recovery resets every ~30 s while a peer has never
    /// connected; a reset right after a round must not cut its backoff short
    /// (review item 4).
    #[test]
    fn a_backoff_reset_right_after_a_round_keeps_its_wait() {
        let pacer = pacer(30);
        let now = Instant::now();
        issue_rounds(&pacer, 6, now);
        let wait = pacer.lock().wait_until(now);
        assert!(wait.is_some());
        pacer.lock().backoff_reset(now + Duration::from_secs(1));
        assert_eq!(pacer.lock().wait_until(now), wait);
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiting_path_starts_once_an_outstanding_round_ends() {
        let pacer = pacer(30);
        let cleared = tokio::sync::Notify::new();
        let mut rounds = Vec::new();
        for _ in 0..MAX_OUTSTANDING_BOOTSTRAP_ROUNDS {
            pacer.lock().next_allowed = None;
            let mut round = BootstrapRoundGuard::try_begin(&pacer, Instant::now()).unwrap();
            round.issue();
            rounds.push(round);
        }
        pacer.lock().next_allowed = None;
        let start = Instant::now();
        let waiter = wait_for_bootstrap_round(&pacer, &cleared, || false);
        let finish_one = async {
            tokio::time::sleep(Duration::from_secs(40)).await;
            drop(rounds.remove(0));
        };
        let (round, ()) = tokio::join!(waiter, finish_one);
        assert!(round.is_some());
        let waited = start.elapsed();
        assert!(
            waited >= Duration::from_secs(40) && waited <= Duration::from_secs(42),
            "the round must wait for a slot, then take it promptly: waited {waited:?}"
        );
    }

    #[test]
    fn a_short_transient_ttl_does_not_lower_the_backoff_cap() {
        let mut pacer = BootstrapPacer::new(Duration::from_secs(2));
        let now = Instant::now();
        for _ in 0..10 {
            pacer.next_allowed = None;
            pacer.outstanding = 0;
            assert!(pacer.try_begin(now));
            pacer.issue(now);
            pacer.end(true);
        }
        let wait = pacer.wait_until(now).unwrap() - now;
        assert!(
            wait >= BOOTSTRAP_RETRY_CAP_FLOOR.mul_f64(0.8),
            "a 2 s TTL capped the backoff at {wait:?}"
        );
    }

    #[test]
    fn a_persistent_connection_resets_the_backoff() {
        let mut pacer = BootstrapPacer::new(Duration::from_secs(30));
        let t0 = Instant::now();
        for _ in 0..4 {
            pacer.next_allowed = None;
            pacer.outstanding = 0;
            assert!(pacer.try_begin(t0));
            pacer.issue(t0);
            pacer.end(true);
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
