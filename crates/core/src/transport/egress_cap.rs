//! Node-wide egress cap: one token bucket that every datagram the UDP
//! transport sends is charged against (#5940, decision 3).
//!
//! When `total-bandwidth-limit` is set, [`DemuxSocket`](super::DemuxSocket)
//! owns one [`EgressCap`] and charges it for every datagram it sends:
//! handshakes, ACKs, keepalives, retransmissions, short messages and stream
//! fragments. It counts UDP payload bytes; IP and UDP headers are not
//! counted. Without the setting there is no cap and no cost beyond one
//! `Option` check per send.
//!
//! # Who waits and who does not
//!
//! Only stream fragment tasks (`send_stream`, `pipe_stream`) wait for the
//! bucket. A fragment first pays its connection's own bucket with
//! `TokenBucket::reserve` (into debt, then sleep for the deficit), as it does
//! without a cap, so the fragments of one connection are admitted IN ORDER:
//! each waits for the reservations ahead of it. A reservation whose wait
//! would reach the stream's [`PACED_WAIT_DEADLINE`] is not made
//! (`TokenBucket::reserve_within`, one lock) and the stream fails at once,
//! so a refused fragment takes no place in the connection's queue; (a
//! reservation that was made and is later given back, when its stream is
//! cancelled or fails on a later wait, does not move its followers up:
//! their waits were computed when they reserved and are not recomputed.
//! A per-connection scheduler would be needed for that);
//! it is counted as refused (`egress_streams_refused_<cause>`), apart from
//! the streams that die at the deadline. A newcomer can still delay streams
//! already in progress, as on the uncapped path and in 0.2.147; a
//! per-connection scheduler that protects them is future work. This looks
//! only at the connection wait: a fragment
//! can still fail on the node cap at the deadline. Otherwise the fragment
//! keeps its payment until it is sent, through its waits on cwnd and on the
//! cap. Then, right after a cwnd check, it calls [`try_take`]: if the
//! node-wide bucket holds a datagram's worth, the bytes are taken and the
//! fragment is sent at once; otherwise the task sleeps for the deficit at
//! the cap rate (see [`jittered`]) and starts again from the cwnd check,
//! keeping its connection payment and so its place on the connection.
//! Nothing is reserved on the node-wide bucket while it sleeps, so a
//! waiting stream holds no node-wide capacity. Node-wide admission is a
//! race: whichever waiter wakes when the bucket has enough takes it. So
//! connections share the cap in proportion to their number of waiting
//! streams; there is no isolation between connections, and a peer driving
//! many streams gets a proportional share.
//!
//! The connection payment kept across a wait on cwnd or on the cap can be
//! stale: the connection's bucket refills meanwhile, so after such a wait a
//! connection may send up to one datagram per waiting stream beyond its
//! bucket's bound. That only moves capacity between connections; the node
//! cap is unaffected, since every datagram still takes from it at its send.
//! A fragment that is not sent gives its payment back, clamped to the
//! bucket's capacity. A late refund, after the refill already passed its
//! slot, can credit at most that one datagram twice; that too only moves
//! capacity between connections, and the node cap still holds. It is also
//! why no margin for timer slack is needed: a timer that fires late costs
//! at most one refunded slot. Rate updates (`TokenBucket::set_rate`, every RTT)
//! change the deficit of later reservations and can reorder waiters
//! slightly, exactly as on the uncapped reserve path; the order is not a
//! correctness property of the cap.
//!
//! Every other send is sent at once and charged after it is sent, even when
//! the bucket is empty, which puts the bucket into debt. Paced senders then
//! wait longer until the debt is paid. That covers the handshake tasks, the
//! keepalive task, short messages (sent from the connection's own task, which
//! also receives for the connection, so a wait there would delay its ACKs),
//! and everything `PeerConnection::recv` sends: ACKs, Pongs, receipts,
//! retransmissions. `recv` must never wait here: it runs as a branch of a
//! `select!` that can be cancelled, and it changes connection state before
//! some of its sends.
//!
//! # What the cap guarantees
//!
//! Over any interval of length `t` ending at a paced send, the node sends at
//! most `B + R * t` bytes (burst `B`, rate `R`), unpaced bytes included, plus
//! the unpaced bytes forgiven in the interval: a paced send needs a
//! non-negative level. Over other intervals it may also send the unpaced debt
//! not yet paid off, at most [`DEBT_LIMIT`] of the rate; an unpaced send
//! beyond that is sent but not charged (forgiven, and counted).
//!
//! When the cap binds, a waiting stream gets a datagram when it wakes and
//! finds enough in the bucket, with probability about `1/n` among `n`
//! waiting streams, so each gets one every `n * 1200 / r` seconds on average
//! at a cap of `r` bytes per second, and longer gaps have a geometric tail.
//! (When only a connection's own bucket binds, its streams are served in
//! order and a stream's gap is about one round of that connection's
//! waiting streams.) A stream whose next fragment has not been sent
//! [`PACED_WAIT_DEADLINE`] (the receiver's 5 s stream inactivity timeout)
//! after its previous one (or after the stream started) fails, measured as
//! the receiver measures it ([`StreamPacing`]). For the first fragment this
//! is deliberate admission control: a stream that cannot get a turn within
//! 5 s fails and the operation retries elsewhere. When a connection carries
//! more streams than fit in one 5 s round, the streams beyond that fail at
//! once on their connection reservation, the same overload drop-off as
//! 0.2.147 (where the receiver's inactivity timeout caught them), but fast.
//! Gaps that reach [`LONG_WAIT`] and failed streams are counted by
//! what took most of the gap: the cap, the connection's own bucket (whose
//! rate is the cap divided among the connections), or something else such
//! as cwnd or, from a relayed stream's second fragment on, the upstream stream
//! (`egress_long_waits_<cause>`, `egress_streams_abandoned_<cause>`, and for
//! the fail-fast refusals `egress_streams_refused_<cause>`), and each kind
//! is logged at WARN with its remedy, at most once per
//! [`LONG_WAIT_WARN_INTERVAL`].
//!
//! # The bucket
//!
//! The state is one atomic: `empty_at_ns`, the time at which the bucket level
//! is zero. At time `now` the level is `(now - empty_at_ns) * rate`, at most
//! the burst, and negative while `empty_at_ns` is in the future (debt).
//! Charging `n` bytes moves `empty_at_ns` later by the time the rate needs to
//! send `n` bytes. All arithmetic is in integer nanoseconds of the
//! [`TimeSource`] the cap was built with, with saturating operations.

use std::fmt;
use std::future::Future;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicI64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use super::Socket;
use super::metrics::{EgressDecision, TRANSPORT_METRICS, WaitCause};
use super::packet_data::MAX_PACKET_SIZE;
use super::peer_connection::streaming::STREAM_INACTIVITY_TIMEOUT;
use crate::config::GlobalRng;
use crate::simulation::TimeSource;

/// The bucket holds at most this much time at the cap rate, so after an idle
/// period the node can send this long at line rate before pacing starts.
const BURST_WINDOW: Duration = Duration::from_millis(25);

/// The burst is never smaller than this many full datagrams, so a cap far
/// below a packet per burst window still admits a few at once. (A count of
/// datagrams on purpose: this sizes a rate bucket and holds no memory.)
const MIN_BURST_PACKETS: u64 = 4;

/// Unpaced debt never exceeds this much time at the cap rate; an unpaced
/// charge beyond it is forgiven.
pub(crate) const DEBT_LIMIT: Duration = Duration::from_secs(1);

/// A gap between one stream's fragments at least this long is counted and
/// logged: half the receiver's stream inactivity timeout, so it is reported
/// before the receiver would give the stream up.
pub(crate) const LONG_WAIT: Duration =
    Duration::from_millis(STREAM_INACTIVITY_TIMEOUT.as_millis() as u64 / 2);

/// With a cap, a stream whose next fragment has not been sent this long
/// after its previous one (or after the stream started; for a relayed
/// stream, after its first upstream chunk arrived) fails: the receiver
/// gives up a stream after the same gap between fragments and is never told
/// the sender is still trying, so waiting longer only spends the shared cap
/// on a stream nobody will finish. The gap counts every wait (the cap, the
/// connection's bucket, cwnd, and for a relayed stream, from its second
/// fragment on, the upstream stream), as the receiver's does.
///
/// For a stream's first fragment this is stricter than the receiver: it
/// starts its 5 s timer after fragment #1 arrives and before that waits up
/// to `STREAM_CLAIM_TIMEOUT` (60 s). The sender's limit applies there on
/// purpose, as admission control: a stream that cannot get one slot within
/// 5 s fails fast and the operation retries, instead of waiting behind (and
/// delaying) the streams already in progress. A newcomer that IS admitted
/// can still delay them: it takes a place in the connection's order ahead
/// of their next fragments.
pub(crate) const PACED_WAIT_DEADLINE: Duration = STREAM_INACTIVITY_TIMEOUT;

/// The long-wait warning is logged at most once per this interval.
const LONG_WAIT_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Egress below this many bytes per second per allowed connection is too low
/// for a full set of connections: their ACKs, keepalives and handshakes alone
/// can use most of it, so streams barely move. Used for a startup warning
/// only; the cap is applied as configured.
pub(crate) const MIN_CAP_PER_CONNECTION: usize = 4 * MAX_PACKET_SIZE;

/// What a paced sender takes before its datagram is built: the largest
/// datagram the transport sends. After the send the socket returns what the
/// datagram did not use, so the bucket is charged the datagram's real size.
pub(crate) const PACED_TAKE_BYTES: usize = MAX_PACKET_SIZE;

const NANOS_PER_SEC: u128 = 1_000_000_000;

/// The most [`EgressCap::poll_admit`] wakers the cap holds. A waiter's entry
/// is removed when it is woken, next admitted, or dropped. At this many
/// entries the oldest is dropped, without waking it, before a new one is
/// added; a live waiter whose entry was dropped still has its timer. The
/// QUIC endpoint has one waiter per connection that is short of bytes, far
/// fewer than this.
pub(crate) const MAX_ADMISSION_WAKERS: usize = 1024;

/// How many waiters queued behind a dropped `poll_admit` waiter are woken to
/// move up (`AdmissionQueue::leave`). The nearest ones matter: the k-th waiter
/// behind waits about k datagrams, so one datagram of lateness costs the
/// first few the most, relatively, and those are the ones that wake soonest.
/// Eight covers the waiters within eight datagrams of the front and keeps a
/// run of N closes at no more than 8 N wakes.
const MAX_FOLLOWERS_WOKEN_PER_LEAVE: usize = 8;

/// The datagram size a `poll_admit` waiter waits for: the QUIC endpoint's
/// (`quic::config::MAX_UDP_PAYLOAD`, the same 1200 bytes as the UDP
/// transport's packets). A give-back wakes one waiter per this many bytes the
/// bucket then holds.
pub(crate) const WAKE_DATAGRAM_BYTES: usize = MAX_PACKET_SIZE;

/// The cap's own counters, so tests can assert on one cap without reading
/// process-global state. Test-only: production records only the
/// `TRANSPORT_METRICS` period counters (`egress_*`).
#[cfg(test)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct EgressStats {
    /// Datagrams sent, by [`EgressDecision`] (index = `decision as usize`).
    pub packets: [u64; EgressDecision::COUNT],
    /// Bytes charged for sent datagrams.
    pub bytes: u64,
    /// Bytes of datagrams that waited before they were sent.
    pub bytes_waited: u64,
    /// Total time paced datagrams waited, in microseconds.
    pub wait_us_sum: u64,
    /// Longest single wait, in microseconds.
    pub wait_us_max: u64,
    /// Paced fragments whose gap reached [`LONG_WAIT`], by [`WaitCause`].
    pub long_waits: [u64; WaitCause::COUNT],
    /// Streams failed at [`PACED_WAIT_DEADLINE`], by [`WaitCause`].
    pub streams_abandoned: [u64; WaitCause::COUNT],
    /// Streams failed at once because a connection reservation would have
    /// reached the deadline, by [`WaitCause`].
    pub streams_refused: [u64; WaitCause::COUNT],
    /// Takes refused because the bucket was short.
    pub refusals: u64,
    /// Largest unpaced debt seen after a charge, in bytes.
    pub debt_hwm_bytes: u64,
    /// Unpaced bytes sent but not charged because the debt was at the limit.
    pub forgiven_bytes: u64,
}

#[cfg(test)]
#[derive(Default)]
struct StatCounters {
    packets: [AtomicU64; EgressDecision::COUNT],
    bytes: AtomicU64,
    bytes_waited: AtomicU64,
    wait_us_sum: AtomicU64,
    wait_us_max: AtomicU64,
    long_waits: [AtomicU64; WaitCause::COUNT],
    streams_abandoned: [AtomicU64; WaitCause::COUNT],
    streams_refused: [AtomicU64; WaitCause::COUNT],
    refusals: AtomicU64,
    debt_hwm_bytes: AtomicU64,
    forgiven_bytes: AtomicU64,
}

/// What a rate-limited WARN reports: per [`WaitCause`], how many events
/// happened since it was last logged and the longest gap among them. Each
/// cause has its own remedy, so the counts are not pooled. Each cause's
/// `(count, longest gap in ms)` pair is behind one lock so a drain cannot
/// pair one event's gap with another window's count; this path is rare (a
/// gap of at least [`LONG_WAIT`]), never per packet.
#[derive(Default)]
struct WarnWindow {
    pairs: [parking_lot::Mutex<(u64, u64)>; WaitCause::COUNT],
}

impl WarnWindow {
    fn add(&self, cause: WaitCause, gap: Duration) {
        let mut pair = self.pairs[cause as usize].lock();
        pair.0 += 1;
        pair.1 = pair.1.max(gap.as_millis() as u64);
    }

    /// Take and reset each cause's `(count, longest gap in ms)`.
    fn drain(&self) -> [(u64, u64); WaitCause::COUNT] {
        WaitCause::ALL.map(|cause| std::mem::take(&mut *self.pairs[cause as usize].lock()))
    }

    /// The `(count, longest gap in ms)` pending for `cause`.
    #[cfg(test)]
    fn pending(&self, cause: WaitCause) -> (u64, u64) {
        *self.pairs[cause as usize].lock()
    }
}

/// The node-wide egress token bucket. See the module docs.
///
/// It is `pub` only because [`Socket::egress_cap`] returns it; everything on
/// it is crate-private.
pub struct EgressCap {
    rate_bytes_per_sec: u64,
    burst_ns: i64,
    debt_limit_ns: i64,
    clock: Box<dyn Fn() -> u64 + Send + Sync>,
    /// `TimeSource::sleep_until` of the same time source as `clock`, for
    /// [`Self::poll_admit`]'s wakeups.
    sleep_until: Box<dyn Fn(u64) -> SleepFuture + Send + Sync>,
    /// Time (ns, on `clock`) at which the bucket level is zero.
    empty_at_ns: AtomicI64,
    /// Wakers of [`Self::poll_admit`] callers that are `Pending`, in the
    /// order they first registered. Shared with each registered
    /// [`AdmissionWaiter`] so dropping one removes its entry.
    admission: Arc<AdmissionQueue>,
    /// When the long-wait warning was last logged (ns, on `clock`).
    last_long_wait_warn_ns: AtomicI64,
    /// Long gaps recorded since the long-wait warning was last logged.
    long_waits_since_warn: WarnWindow,
    /// When the abandoned-stream warning was last logged.
    last_abandon_warn_ns: AtomicI64,
    /// Streams abandoned since then.
    abandoned_since_warn: WarnWindow,
    /// When the refused-stream warning was last logged.
    last_refuse_warn_ns: AtomicI64,
    /// Streams refused since then.
    refused_since_warn: WarnWindow,
    #[cfg(test)]
    stats: StatCounters,
}

impl fmt::Debug for EgressCap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EgressCap")
            .field("rate_bytes_per_sec", &self.rate_bytes_per_sec)
            .field("burst_bytes", &self.burst_bytes())
            .finish_non_exhaustive()
    }
}

type SleepFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// The `Pending` [`EgressCap::poll_admit`] callers, by [`AdmissionWaiter`]
/// id, oldest registration first. At most [`MAX_ADMISSION_WAKERS`].
#[derive(Default)]
struct AdmissionQueue {
    wakers: parking_lot::Mutex<std::collections::VecDeque<(u64, std::task::Waker)>>,
    /// `wakers.len()`, so a give-back skips the lock when nobody waits
    /// (every node without a QUIC endpoint).
    waiting: std::sync::atomic::AtomicUsize,
}

impl AdmissionQueue {
    /// Hold `waker` under `id` and return how many entries are ahead of it.
    /// A waiter already held keeps its place (its waker is replaced), so its
    /// staggered timer stays consistent with its position. A new waiter
    /// goes to the back; at [`MAX_ADMISSION_WAKERS`] entries the OLDEST is
    /// dropped first, WITHOUT being woken (its waiter still has its timer).
    /// A linear scan, over at most that many entries, only while someone is
    /// short of bytes.
    fn register(&self, id: u64, waker: &std::task::Waker) -> usize {
        let mut wakers = self.wakers.lock();
        let ahead = if let Some(at) = wakers.iter().position(|(held, _)| *held == id) {
            if !wakers[at].1.will_wake(waker) {
                wakers[at].1 = waker.clone();
            }
            at
        } else {
            if wakers.len() >= MAX_ADMISSION_WAKERS {
                wakers.pop_front();
            }
            wakers.push_back((id, waker.clone()));
            wakers.len() - 1
        };
        self.waiting.store(wakers.len(), Ordering::Release);
        ahead
    }

    fn forget(&self, id: u64) {
        if self.waiting.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut wakers = self.wakers.lock();
        if let Some(at) = wakers.iter().position(|(held, _)| *held == id) {
            wakers.remove(at);
        }
        self.waiting.store(wakers.len(), Ordering::Release);
    }

    /// Remove a waiter that is gone without taking its slot (its connection
    /// closed) and wake the next [`MAX_FOLLOWERS_WOKEN_PER_LEAVE`] waiters
    /// queued behind it, so each re-polls and moves its staggered timer one
    /// datagram earlier, to its new place. Bounded so connections closing
    /// one by one in a long queue cost a constant number of wakes each, not
    /// one per follower.
    ///
    /// Residual: a waiter further back keeps its old deadline, at most one
    /// datagram late per drop ahead of it since it last polled; it fixes
    /// itself the next time it polls (its timer firing, or a give-back). It
    /// is never woken early. The wakers are woken after the lock is released.
    fn leave(&self, id: u64) {
        if self.waiting.load(Ordering::Acquire) == 0 {
            return;
        }
        let followers: Vec<std::task::Waker> = {
            let mut wakers = self.wakers.lock();
            let Some(at) = wakers.iter().position(|(held, _)| *held == id) else {
                return;
            };
            wakers.remove(at);
            self.waiting.store(wakers.len(), Ordering::Release);
            wakers
                .iter()
                .skip(at)
                .take(MAX_FOLLOWERS_WOKEN_PER_LEAVE)
                .map(|(_, waker)| waker.clone())
                .collect()
        };
        for waker in followers {
            waker.wake();
        }
    }
}

/// The wakeup a sender polling [`EgressCap::poll_admit`] keeps between
/// polls: one timer on the cap's time source and the time it fires at, and
/// the id under which the cap holds its waker while it waits (see
/// [`EgressCap::poll_admit`]: bytes given back wake it before the timer).
// Used by the QUIC endpoint (#5940).
pub(crate) struct AdmissionWaiter {
    id: u64,
    timer: Option<(u64, SleepFuture)>,
    /// The queue of the cap this waiter last registered with, so dropping
    /// the waiter removes its entry: a closed connection's entry must not
    /// absorb a wake meant for a live one.
    queue: Option<std::sync::Weak<AdmissionQueue>>,
}

impl Default for AdmissionWaiter {
    fn default() -> Self {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            timer: None,
            queue: None,
        }
    }
}

impl Drop for AdmissionWaiter {
    fn drop(&mut self) {
        if let Some(queue) = self.queue.take().and_then(|queue| queue.upgrade()) {
            queue.leave(self.id);
        }
    }
}

impl AdmissionWaiter {
    /// When the armed timer fires (ns on the cap's clock), if one is armed.
    #[cfg(test)]
    pub(crate) fn wake_at_ns(&self) -> Option<u64> {
        self.timer.as_ref().map(|(at, _)| *at)
    }
}

impl fmt::Debug for AdmissionWaiter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdmissionWaiter")
            .field("wake_at_ns", &self.timer.as_ref().map(|(at, _)| *at))
            .finish()
    }
}

/// Bytes a paced sender took from the bucket for a datagram it is about to
/// send. Pass it to [`Socket::send_to_paced`], which returns what the
/// datagram did not use; dropping it instead (the send failed or was
/// abandoned) returns all of it.
#[must_use = "dropping a take returns its bytes; send with Socket::send_to_paced"]
pub struct EgressTake<'a> {
    cap: &'a EgressCap,
    taken_bytes: u64,
    /// How long the sender slept for the cap before this take.
    waited: Duration,
}

impl EgressCap {
    /// A cap of `rate_bytes_per_sec`, starting with a full bucket.
    pub(crate) fn new<T: TimeSource>(rate_bytes_per_sec: u64, time_source: T) -> Self {
        // A rate of 0 cannot be met by anything; callers do not build a cap
        // for it (see `from_total_limit`). Treat it as 1 B/s rather than
        // dividing by zero.
        let rate = rate_bytes_per_sec.max(1);
        let burst_bytes = (rate as u128 * BURST_WINDOW.as_nanos() / NANOS_PER_SEC) as u64;
        let burst_bytes = burst_bytes.max(MIN_BURST_PACKETS * MAX_PACKET_SIZE as u64);
        let burst_ns = Self::nanos_for(rate, burst_bytes);
        let debt_limit_ns = i64::try_from(DEBT_LIMIT.as_nanos()).unwrap_or(i64::MAX);
        let sleeper = time_source.clone();
        let sleep_until = Box::new(move |deadline_ns| sleeper.sleep_until(deadline_ns));
        let clock = Box::new(move || time_source.now_nanos());
        let now = i64::try_from(clock()).unwrap_or(i64::MAX);
        Self {
            rate_bytes_per_sec: rate,
            burst_ns,
            debt_limit_ns,
            clock,
            sleep_until,
            empty_at_ns: AtomicI64::new(now.saturating_sub(burst_ns)),
            admission: Arc::new(AdmissionQueue::default()),
            last_long_wait_warn_ns: AtomicI64::new(i64::MIN),
            last_abandon_warn_ns: AtomicI64::new(i64::MIN),
            long_waits_since_warn: WarnWindow::default(),
            abandoned_since_warn: WarnWindow::default(),
            last_refuse_warn_ns: AtomicI64::new(i64::MIN),
            refused_since_warn: WarnWindow::default(),
            #[cfg(test)]
            stats: StatCounters::default(),
        }
    }

    /// The cap for a configured `total-bandwidth-limit`, or `None` when the
    /// value cannot be a cap (0).
    pub(crate) fn from_total_limit<T: TimeSource>(
        total_bytes_per_sec: usize,
        time_source: T,
    ) -> Option<Self> {
        if total_bytes_per_sec == 0 {
            tracing::warn!(
                "total-bandwidth-limit is 0; no node-wide egress cap is applied \
                 (a cap of 0 bytes per second cannot be met)"
            );
            return None;
        }
        Some(Self::new(total_bytes_per_sec as u64, time_source))
    }

    /// The cap's rate, in bytes per second.
    pub(crate) fn rate_bytes_per_sec(&self) -> u64 {
        self.rate_bytes_per_sec
    }

    pub(crate) fn burst_bytes(&self) -> u64 {
        self.bytes_for(self.burst_ns)
    }

    /// Time the rate needs for `bytes`, rounded up, so rounding never
    /// lets more through than the rate.
    fn nanos_for(rate: u64, bytes: u64) -> i64 {
        let ns = (bytes as u128 * NANOS_PER_SEC).div_ceil(rate.max(1) as u128);
        i64::try_from(ns).unwrap_or(i64::MAX)
    }

    fn cost_ns(&self, bytes: u64) -> i64 {
        Self::nanos_for(self.rate_bytes_per_sec, bytes)
    }

    fn bytes_for(&self, ns: i64) -> u64 {
        if ns <= 0 {
            return 0;
        }
        u64::try_from(ns as u128 * self.rate_bytes_per_sec as u128 / NANOS_PER_SEC)
            .unwrap_or(u64::MAX)
    }

    fn now(&self) -> i64 {
        i64::try_from((self.clock)()).unwrap_or(i64::MAX)
    }

    /// `empty_at_ns` refilled to `now`: the bucket never holds more than the
    /// burst.
    fn refilled(&self, empty_at: i64, now: i64) -> i64 {
        empty_at.max(now.saturating_sub(self.burst_ns))
    }

    /// The admission predicate shared by [`Self::try_take`] and
    /// [`Self::poll_admit`]: a take of `cost` at `now` is admitted when the
    /// bucket holds the whole amount. `Ok` is `empty_at_ns` after the take;
    /// `Err` is the deficit in nanoseconds at the rate.
    fn admits(&self, empty_at: i64, now: i64, cost: i64) -> Result<i64, i64> {
        let after = self.refilled(empty_at, now).saturating_add(cost);
        if after <= now {
            Ok(after)
        } else {
            Err(after - now)
        }
    }

    /// Apply `f(empty_at_ns, now)` atomically. `f` returns the new value, or
    /// `None` to leave it unchanged. Returns the value before, the value
    /// after and `now`.
    fn update(&self, f: impl Fn(i64, i64) -> Option<i64>) -> (i64, i64, i64) {
        let now = self.now();
        let mut old = self.empty_at_ns.load(Ordering::Acquire);
        loop {
            let Some(new) = f(old, now) else {
                return (old, old, now);
            };
            match self.empty_at_ns.compare_exchange_weak(
                old,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return (old, new, now),
                Err(current) => old = current,
            }
        }
    }

    /// Charge a datagram sent without pacing. The bucket may go into debt,
    /// at most [`DEBT_LIMIT`] of the rate; the part of the charge beyond that
    /// is forgiven (sent, not charged) and counted.
    pub(crate) fn charge_unpaced(&self, bytes: usize) {
        let cost = self.cost_ns(bytes as u64);
        let (old, new, now) = self.update(|empty_at, now| {
            Some(
                self.refilled(empty_at, now)
                    .saturating_add(cost)
                    .min(now.saturating_add(self.debt_limit_ns)),
            )
        });
        // Recomputed from the value the committed update started from, since
        // the closure may run more than once.
        let unclamped = self.refilled(old, now).saturating_add(cost);
        let forgiven = self.bytes_for(unclamped.saturating_sub(new));
        if forgiven > 0 {
            #[cfg(test)]
            self.stats
                .forgiven_bytes
                .fetch_add(forgiven, Ordering::Relaxed);
            TRANSPORT_METRICS.record_egress_forgiven(forgiven);
        }
        self.record_sent(EgressDecision::Unpaced, bytes as u64, Duration::ZERO);
        let debt = self.bytes_for(new.saturating_sub(now));
        if debt > 0 {
            #[cfg(test)]
            self.stats.debt_hwm_bytes.fetch_max(debt, Ordering::Relaxed);
            TRANSPORT_METRICS.record_egress_debt(debt);
        }
    }

    /// Take `bytes` if the bucket holds at least that many; otherwise leave
    /// it unchanged and say how long the rate needs to make up the deficit.
    ///
    /// Requiring the whole amount (rather than any positive level) keeps a
    /// paced send from ever putting the bucket into debt, so the bound in the
    /// module docs holds exactly. A take is at most one datagram, and the
    /// burst holds at least four, so a take always succeeds eventually.
    pub(crate) fn try_take(
        &self,
        bytes: usize,
        waited: Duration,
    ) -> Result<EgressTake<'_>, Duration> {
        let cost = self.cost_ns(bytes as u64);
        let (old, _, now) = self.update(|empty_at, now| self.admits(empty_at, now, cost).ok());
        // The same predicate on the value the last attempt saw.
        if let Err(deficit) = self.admits(old, now, cost) {
            #[cfg(test)]
            self.stats.refusals.fetch_add(1, Ordering::Relaxed);
            return Err(Duration::from_nanos(deficit as u64));
        }
        Ok(EgressTake {
            cap: self,
            taken_bytes: bytes as u64,
            waited,
        })
    }

    /// Poll form of the admission check, for a sender that is polled rather
    /// than one that can sleep and retry (a QUIC endpoint's
    /// `UdpPoller::poll_writable`, #5940). `Ready` when [`Self::try_take`] of
    /// `bytes` would succeed now (the same predicate at the same instant);
    /// otherwise `Pending`, with `waiter` holding a timer on the cap's time
    /// source that wakes `cx` when the rate has made up the deficit. Takes
    /// nothing: the sender then takes or is charged as it sends, and another
    /// sender may take the bytes first, in which case the next poll is
    /// `Pending` again.
    ///
    /// While `Pending`, the cap also holds `cx`'s waker. When bytes given back
    /// to the bucket (a paced sender that used less than it took, or whose
    /// send failed) leave it holding at least one [`WAKE_DATAGRAM_BYTES`],
    /// one held waker is woken per such datagram's worth, so a waiter is not
    /// left asleep until its original deficit after capacity returned early,
    /// and a give-back too small for anyone wakes nobody. A waiter not woken
    /// keeps its timer.
    ///
    /// Waiters are staggered so the cap does not wake them as a herd: a
    /// waiter's timer is set for its deficit plus one [`WAKE_DATAGRAM_BYTES`]
    /// of refill for every waiter registered ahead of it, so when N waiters
    /// share one deficit, each datagram's worth of refill wakes one of them,
    /// oldest first, instead of all N. A waiter keeps its place in the queue
    /// across polls. An admitted waiter takes the slot the stagger reserved
    /// for it, so nobody behind it moves. A waiter dropped while queued
    /// (its connection closed) wakes the next
    /// [`MAX_FOLLOWERS_WOKEN_PER_LEAVE`] waiters behind it, which re-poll and
    /// move their timers to their new places; waiters further back stay up
    /// to one datagram late per such drop until they next poll. An entry evicted at
    /// [`MAX_ADMISSION_WAKERS`] wakes nobody, so the waiters behind it keep
    /// timers one datagram late per eviction. No waiter is woken before the
    /// bucket can admit it.
    ///
    /// The timer is replaced only when the waiter now needs an earlier
    /// wakeup than the one armed; a later need (more debt arrived) keeps the
    /// armed timer, which wakes early and re-checks. So repeated polls do not
    /// register a new timer each.
    pub(crate) fn poll_admit(
        &self,
        bytes: usize,
        waiter: &mut AdmissionWaiter,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        debug_assert!(
            bytes as u64 <= self.burst_bytes(),
            "an admission of {bytes} bytes is larger than the burst and could never be Ready"
        );
        let cost = self.cost_ns(bytes as u64);
        loop {
            let now = self.now();
            let deficit = match self.admits(self.empty_at_ns.load(Ordering::Acquire), now, cost) {
                Ok(_) => {
                    waiter.timer = None;
                    self.admission.forget(waiter.id);
                    return Poll::Ready(());
                }
                Err(deficit) => deficit,
            };
            let ahead = self.admission.register(waiter.id, cx.waker());
            waiter.queue = Some(Arc::downgrade(&self.admission));
            // Bytes given back between the check above and the registration
            // woke nobody; check once more so they are not missed.
            if self
                .admits(self.empty_at_ns.load(Ordering::Acquire), self.now(), cost)
                .is_ok()
            {
                continue;
            }
            let stagger = i64::try_from(ahead)
                .unwrap_or(i64::MAX)
                .saturating_mul(self.cost_ns(WAKE_DATAGRAM_BYTES as u64));
            let wake_at = u64::try_from(now.saturating_add(deficit).saturating_add(stagger))
                .unwrap_or(u64::MAX);
            if !matches!(&waiter.timer, Some((at, _)) if *at <= wake_at) {
                waiter.timer = None;
            }
            let (_, timer) = waiter
                .timer
                .get_or_insert_with(|| (wake_at, (self.sleep_until)(wake_at)));
            match timer.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                // The timer fired: check the bucket again.
                Poll::Ready(()) => waiter.timer = None,
            }
        }
    }

    /// After bytes came back to the bucket: wake one `Pending`
    /// [`Self::poll_admit`] caller per [`WAKE_DATAGRAM_BYTES`] the bucket now
    /// holds, and none if it holds less, oldest registration first. Costs one
    /// atomic load when nobody waits, which is every give-back on a node
    /// without a QUIC endpoint. The wakers are woken after the lock is
    /// released.
    fn wake_admission_waiters(&self) {
        if self.admission.waiting.load(Ordering::Acquire) == 0 {
            return;
        }
        let now = self.now();
        let level_ns =
            now.saturating_sub(self.refilled(self.empty_at_ns.load(Ordering::Acquire), now));
        let datagrams = (self.bytes_for(level_ns) / WAKE_DATAGRAM_BYTES as u64) as usize;
        if datagrams == 0 {
            return;
        }
        let woken: Vec<std::task::Waker> = {
            let mut wakers = self.admission.wakers.lock();
            let take = datagrams.min(wakers.len());
            let woken = wakers.drain(..take).map(|(_, waker)| waker).collect();
            self.admission
                .waiting
                .store(wakers.len(), Ordering::Release);
            woken
        };
        for waker in woken {
            waker.wake();
        }
    }

    /// How many `Pending` [`Self::poll_admit`] wakers the cap holds.
    #[cfg(test)]
    pub(crate) fn admission_waker_count(&self) -> usize {
        self.admission.wakers.lock().len()
    }

    /// Return `bytes` to the bucket, never above the burst.
    fn give_back(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let cost = self.cost_ns(bytes);
        self.update(|empty_at, now| Some(self.refilled(empty_at.saturating_sub(cost), now)));
        self.wake_admission_waiters();
    }

    /// Record one sent datagram. Production writes only the
    /// `TRANSPORT_METRICS` period counters; the per-cap counters exist only
    /// in tests.
    fn record_sent(&self, decision: EgressDecision, bytes: u64, wait: Duration) {
        let wait_us = u64::try_from(wait.as_micros()).unwrap_or(u64::MAX);
        TRANSPORT_METRICS.record_egress(decision, bytes, wait_us);
        #[cfg(test)]
        {
            let s = &self.stats;
            s.packets[decision as usize].fetch_add(1, Ordering::Relaxed);
            s.bytes.fetch_add(bytes, Ordering::Relaxed);
            if !wait.is_zero() {
                s.bytes_waited.fetch_add(bytes, Ordering::Relaxed);
                s.wait_us_sum.fetch_add(wait_us, Ordering::Relaxed);
                s.wait_us_max.fetch_max(wait_us, Ordering::Relaxed);
            }
        }
    }

    /// Record a fragment whose gap reached [`LONG_WAIT`], attributed to
    /// `cause`, and log the gaps since the last WARN, one line per cause, at
    /// most once per [`LONG_WAIT_WARN_INTERVAL`].
    fn record_long_wait(&self, cause: WaitCause, gap: Duration) {
        TRANSPORT_METRICS.record_egress_long_wait(cause);
        #[cfg(test)]
        self.stats.long_waits[cause as usize].fetch_add(1, Ordering::Relaxed);
        self.long_waits_since_warn.add(cause, gap);
        if self.warn_due(&self.last_long_wait_warn_ns) {
            for (cause, (long_waits, longest_gap_ms)) in WaitCause::ALL
                .into_iter()
                .zip(self.long_waits_since_warn.drain())
            {
                if long_waits == 0 {
                    continue;
                }
                tracing::warn!(
                    long_waits,
                    longest_gap_ms,
                    cause = cause.label(),
                    total_bandwidth_limit = self.rate_bytes_per_sec,
                    stream_inactivity_timeout_ms = STREAM_INACTIVITY_TIMEOUT.as_millis() as u64,
                    remedy = remedy(cause),
                    "Stream fragments have waited this long since the stream's previous \
                     fragment; the stream fails at stream_inactivity_timeout_ms or at the \
                     cwnd wait timeout, whichever comes first"
                );
            }
        }
    }

    /// Record a stream failed at [`PACED_WAIT_DEADLINE`], attributed to
    /// `cause`, and log the number failed since the last WARN, one line per
    /// cause, at most once per [`LONG_WAIT_WARN_INTERVAL`].
    fn record_abandoned(&self, cause: WaitCause, gap: Duration) {
        TRANSPORT_METRICS.record_egress_stream_abandoned(cause);
        #[cfg(test)]
        self.stats.streams_abandoned[cause as usize].fetch_add(1, Ordering::Relaxed);
        self.abandoned_since_warn.add(cause, gap);
        if self.warn_due(&self.last_abandon_warn_ns) {
            for (cause, (streams_abandoned, longest_gap_ms)) in WaitCause::ALL
                .into_iter()
                .zip(self.abandoned_since_warn.drain())
            {
                if streams_abandoned == 0 {
                    continue;
                }
                tracing::warn!(
                    streams_abandoned,
                    longest_gap_ms,
                    cause = cause.label(),
                    total_bandwidth_limit = self.rate_bytes_per_sec,
                    remedy = remedy(cause),
                    "Outbound streams failed: a fragment could not be sent within the \
                     receiver's stream inactivity timeout"
                );
            }
        }
    }

    /// Count a stream failed at once because its connection reservation
    /// would have reached the deadline (`gap` includes the would-be wait),
    /// by `cause`, and log the number since the last WARN, one line per
    /// cause, at most once per [`LONG_WAIT_WARN_INTERVAL`]. Kept apart from
    /// [`Self::record_abandoned`], which counts streams that died waiting.
    fn record_refused(&self, cause: WaitCause, gap: Duration) {
        TRANSPORT_METRICS.record_egress_stream_refused(cause);
        #[cfg(test)]
        self.stats.streams_refused[cause as usize].fetch_add(1, Ordering::Relaxed);
        self.refused_since_warn.add(cause, gap);
        if self.warn_due(&self.last_refuse_warn_ns) {
            for (cause, (streams_refused, longest_gap_ms)) in WaitCause::ALL
                .into_iter()
                .zip(self.refused_since_warn.drain())
            {
                if streams_refused == 0 {
                    continue;
                }
                tracing::warn!(
                    streams_refused,
                    longest_gap_ms,
                    cause = cause.label(),
                    total_bandwidth_limit = self.rate_bytes_per_sec,
                    remedy = refusal_remedy(cause),
                    "Outbound streams refused: a fragment's turn on its connection \
                     would have come after the egress deadline, so the stream \
                     failed at once"
                );
            }
        }
    }

    /// Whether a rate-limited WARN whose last time is in `last` may be
    /// logged now; if so, records now as its last time.
    fn warn_due(&self, last: &AtomicI64) -> bool {
        let now = self.now();
        let interval = i64::try_from(LONG_WAIT_WARN_INTERVAL.as_nanos()).unwrap_or(i64::MAX);
        let previous = last.load(Ordering::Relaxed);
        if previous != i64::MIN && now.saturating_sub(previous) < interval {
            return false;
        }
        last.compare_exchange(previous, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    /// The bucket level in bytes now; negative while in debt.
    #[cfg(test)]
    pub(crate) fn level_bytes(&self) -> i64 {
        let now = self.now();
        let empty_at = self.refilled(self.empty_at_ns.load(Ordering::Acquire), now);
        let ns = now.saturating_sub(empty_at);
        let magnitude = self.bytes_for(ns.saturating_abs()) as i64;
        if ns < 0 { -magnitude } else { magnitude }
    }

    #[cfg(test)]
    pub(crate) fn stats(&self) -> EgressStats {
        let s = &self.stats;
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        EgressStats {
            packets: std::array::from_fn(|i| load(&s.packets[i])),
            bytes: load(&s.bytes),
            bytes_waited: load(&s.bytes_waited),
            wait_us_sum: load(&s.wait_us_sum),
            wait_us_max: load(&s.wait_us_max),
            long_waits: std::array::from_fn(|i| load(&s.long_waits[i])),
            streams_abandoned: std::array::from_fn(|i| load(&s.streams_abandoned[i])),
            streams_refused: std::array::from_fn(|i| load(&s.streams_refused[i])),
            refusals: load(&s.refusals),
            debt_hwm_bytes: load(&s.debt_hwm_bytes),
            forgiven_bytes: load(&s.forgiven_bytes),
        }
    }
}

impl EgressTake<'_> {
    /// Account for the datagram as sent with `sent_bytes` bytes: return what
    /// it did not use of the take and record it. Called by the socket after
    /// a successful send.
    pub(crate) fn sent(self, sent_bytes: usize) {
        let this = ManuallyDrop::new(self);
        let sent = (sent_bytes as u64).min(this.taken_bytes);
        this.cap.give_back(this.taken_bytes - sent);
        let decision = if this.waited.is_zero() {
            EgressDecision::Immediate
        } else {
            EgressDecision::Waited
        };
        this.cap.record_sent(decision, sent, this.waited);
    }
}

impl Drop for EgressTake<'_> {
    fn drop(&mut self) {
        self.cap.give_back(self.taken_bytes);
    }
}

impl fmt::Debug for EgressTake<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EgressTake")
            .field("taken_bytes", &self.taken_bytes)
            .field("waited", &self.waited)
            .finish()
    }
}

/// One attempt to admit a paced datagram through `socket`.
///
/// `Ok(None)`: the socket has no cap, send. `Ok(Some(take))`: the bytes were
/// taken, send at once with [`Socket::send_to_paced`] (no await before it
/// but the send itself). `Err(deficit)`: pass it to [`StreamPacing::sleep`],
/// then start admission again from the cwnd check. `waited` is how long this
/// datagram has already waited.
pub(crate) fn try_take<S: Socket>(
    socket: &S,
    waited: Duration,
) -> Result<Option<EgressTake<'_>>, Duration> {
    let Some(cap) = socket.egress_cap() else {
        return Ok(None);
    };
    cap.try_take(PACED_TAKE_BYTES, waited).map(Some)
}

/// How long to sleep for a refused take's `deficit`: the deficit times a
/// random factor in [1.0, 1.4), plus a random 0 to 2 ms. Never shorter than
/// the deficit, so a lone waiter wakes when the bucket has room rather than
/// half the time before it. The added 0 to 2 ms spreads waiters over at
/// least two ticks of tokio's 1 ms timer, so which of them wakes first is
/// decided by the random draw, not by the timer wheel, at any rate.
pub(crate) fn jittered(deficit: Duration) -> Duration {
    deficit
        .mul_f64(GlobalRng::random_range(1.0f64..1.4))
        .saturating_add(Duration::from_micros(GlobalRng::random_range(0..2_000)))
}

/// What a long gap or a failed stream is blamed on, for its telemetry and
/// WARN.
fn remedy(cause: WaitCause) -> &'static str {
    match cause {
        WaitCause::Cap => {
            "too many concurrent streams for the node-wide cap: raise total-bandwidth-limit"
        }
        WaitCause::Connection => {
            "too many streams for this connection's share of the cap \
             (total-bandwidth-limit / active connections, at least \
             min-bandwidth-per-connection): raise min-bandwidth-per-connection \
             or total-bandwidth-limit"
        }
        WaitCause::Other => {
            "the time went to the congestion window (slow ACKs), to the upstream \
             stream (for relayed streams) or elsewhere, not to the egress limits"
        }
    }
}

/// The remedy logged with a refusal: for `connection` the same as for a
/// deadline failure; for `other` it says the earlier wait (cwnd, the
/// upstream stream) used most of the gap and the connection's turn then
/// came too late, instead of saying the egress limits were not involved.
fn refusal_remedy(cause: WaitCause) -> &'static str {
    match cause {
        WaitCause::Cap | WaitCause::Connection => remedy(cause),
        WaitCause::Other => {
            "most of the gap went to the congestion window (slow ACKs) or to the \
             upstream stream, and the connection's queue then left no turn before \
             the deadline: look at the path first; raising \
             min-bandwidth-per-connection or total-bandwidth-limit shortens the queue"
        }
    }
}

/// One paced stream's gaps: the time since its previous fragment was sent
/// (or since it started, before its first), which is the gap the receiver's
/// `STREAM_INACTIVITY_TIMEOUT` measures, and how much of the current gap was
/// spent sleeping on the node-wide cap and on the connection's bucket. Used
/// only when the node has a cap.
pub(crate) struct StreamPacing {
    gap_start_ns: u64,
    cap_slept: Duration,
    connection_slept: Duration,
    long_reported: bool,
}

impl StreamPacing {
    pub(crate) fn new<T: TimeSource>(time_source: &T) -> Self {
        Self {
            gap_start_ns: time_source.now_nanos(),
            cap_slept: Duration::ZERO,
            connection_slept: Duration::ZERO,
            long_reported: false,
        }
    }

    /// How long until the current gap reaches [`PACED_WAIT_DEADLINE`].
    pub(crate) fn time_left<T: TimeSource>(&self, time_source: &T) -> Duration {
        PACED_WAIT_DEADLINE.saturating_sub(self.gap(time_source))
    }

    /// Fail the stream now because its connection reservation would wait
    /// `wait` and reach the deadline: count it as refused, blamed as at the
    /// deadline with `wait` counted as connection time, and return the gap
    /// it would have had (the gap so far plus `wait`). Only `connection` and
    /// `other` can occur: nothing has slept for the cap before a fragment's
    /// reservation.
    pub(crate) fn refuse_for_connection<T: TimeSource>(
        &mut self,
        time_source: &T,
        cap: &EgressCap,
        wait: Duration,
    ) -> Duration {
        let gap = self.gap(time_source).saturating_add(wait);
        cap.record_refused(self.cause_with(time_source, wait), gap);
        gap
    }

    /// How long the next sleep may last: to the deadline, and the first time
    /// to [`LONG_WAIT`], so both are reported when they happen.
    fn sleep_limit<T: TimeSource>(&self, time_source: &T) -> Duration {
        let gap = self.gap(time_source);
        let left = PACED_WAIT_DEADLINE.saturating_sub(gap);
        if !self.long_reported && gap < LONG_WAIT {
            left.min(LONG_WAIT - gap)
        } else {
            left
        }
    }

    /// Time since the stream's previous fragment was sent.
    pub(crate) fn gap<T: TimeSource>(&self, time_source: &T) -> Duration {
        Duration::from_nanos(time_source.now_nanos().saturating_sub(self.gap_start_ns))
    }

    /// How long the current fragment has slept for the cap and its
    /// connection's bucket: what `egress_waited` and the wait times count.
    pub(crate) fn slept(&self) -> Duration {
        self.cap_slept.saturating_add(self.connection_slept)
    }

    /// The limit that took most of the current gap.
    fn cause<T: TimeSource>(&self, time_source: &T) -> WaitCause {
        self.cause_with(time_source, Duration::ZERO)
    }

    /// [`Self::cause`] with `extra` more time on the connection's bucket.
    fn cause_with<T: TimeSource>(&self, time_source: &T, extra: Duration) -> WaitCause {
        let other = self.gap(time_source).saturating_sub(self.slept());
        let connection = self.connection_slept.saturating_add(extra);
        if self.cap_slept >= connection && self.cap_slept >= other {
            WaitCause::Cap
        } else if connection >= other {
            WaitCause::Connection
        } else {
            WaitCause::Other
        }
    }

    /// Check the current gap: report it once when it reaches [`LONG_WAIT`],
    /// and return `Err(gap)` when it reaches [`PACED_WAIT_DEADLINE`]: the
    /// caller must not send the fragment and fails the stream. Called before
    /// every attempt to take, and after every sleep.
    pub(crate) fn check<T: TimeSource>(
        &mut self,
        time_source: &T,
        cap: &EgressCap,
    ) -> Result<(), Duration> {
        let gap = self.gap(time_source);
        // Before the deadline, so a gap that went from below `LONG_WAIT` to
        // the deadline in one step (a long cwnd or upstream wait) is still
        // counted as a long gap.
        if gap >= LONG_WAIT && !self.long_reported {
            self.long_reported = true;
            cap.record_long_wait(self.cause(time_source), gap);
        }
        if gap >= PACED_WAIT_DEADLINE {
            cap.record_abandoned(self.cause(time_source), gap);
            return Err(gap);
        }
        Ok(())
    }

    /// Sleep for a take the node-wide cap refused with `deficit` (jittered)
    /// on the caller's `TimeSource`, crediting the time to the cap, then
    /// [`Self::check`]. The sleep stops at [`LONG_WAIT`] the first time and
    /// at the deadline, so both are reported when they happen.
    pub(crate) async fn sleep<T: TimeSource>(
        &mut self,
        time_source: &T,
        cap: &EgressCap,
        deficit: Duration,
    ) -> Result<(), Duration> {
        let left = self.sleep_limit(time_source);
        let before = time_source.now_nanos();
        time_source.sleep(jittered(deficit).min(left)).await;
        self.cap_slept += Duration::from_nanos(time_source.now_nanos().saturating_sub(before));
        self.check(time_source, cap)
    }

    /// Wait `wait`, the deficit `TokenBucket::reserve` returned for this
    /// fragment's payment on its connection's bucket, without jitter, so the
    /// connection's waiters wake in the order they reserved. The time is
    /// credited to the connection. The sleep is cut at [`LONG_WAIT`] and at
    /// the deadline as in [`Self::sleep`], then continues to the end of the
    /// wait; `Err(gap)` at the deadline.
    pub(crate) async fn wait_for_connection<T: TimeSource>(
        &mut self,
        time_source: &T,
        cap: &EgressCap,
        wait: Duration,
    ) -> Result<(), Duration> {
        let ready_at = time_source
            .now_nanos()
            .saturating_add(u64::try_from(wait.as_nanos()).unwrap_or(u64::MAX));
        loop {
            let now = time_source.now_nanos();
            if now >= ready_at {
                return Ok(());
            }
            let step = Duration::from_nanos(ready_at - now).min(self.sleep_limit(time_source));
            time_source.sleep(step).await;
            self.connection_slept +=
                Duration::from_nanos(time_source.now_nanos().saturating_sub(now));
            self.check(time_source, cap)?;
        }
    }

    /// The fragment was sent: the next gap starts now.
    pub(crate) fn sent<T: TimeSource>(&mut self, time_source: &T) {
        *self = Self::new(time_source);
    }
}

/// The startup floor for a cap: [`MIN_CAP_PER_CONNECTION`] for each of
/// `max_connections`. Returns the floor when `total_bytes_per_sec` is below
/// it.
pub(crate) fn cap_below_floor(total_bytes_per_sec: usize, max_connections: usize) -> Option<usize> {
    let floor = MIN_CAP_PER_CONNECTION.saturating_mul(max_connections);
    (total_bytes_per_sec < floor).then_some(floor)
}

/// Log the egress cap a node starts with, and warn when it is below
/// [`cap_below_floor`].
pub(crate) fn log_startup_cap(
    total_bytes_per_sec: usize,
    min_per_connection: usize,
    max_connections: usize,
) {
    tracing::info!(
        total_bandwidth_limit = total_bytes_per_sec,
        min_bandwidth_per_connection = min_per_connection.min(total_bytes_per_sec),
        "Node-wide egress cap active: every datagram the node sends is charged against \
         total-bandwidth-limit"
    );
    if min_per_connection > total_bytes_per_sec {
        tracing::warn!(
            total_bandwidth_limit = total_bytes_per_sec,
            min_bandwidth_per_connection = min_per_connection,
            "min-bandwidth-per-connection is above total-bandwidth-limit; it is clamped \
             to total-bandwidth-limit"
        );
    }
    if let Some(floor) = cap_below_floor(total_bytes_per_sec, max_connections) {
        tracing::warn!(
            total_bandwidth_limit = total_bytes_per_sec,
            max_connections,
            recommended_minimum = floor,
            "total-bandwidth-limit is low for the number of connections allowed: control \
             traffic (ACKs, keepalives, handshakes) of a full set of connections can use \
             most of it, and data transfers will be slow"
        );
    }
}

#[cfg(test)]
mod tests;
