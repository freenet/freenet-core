//! Node-wide egress cap: one token bucket that every datagram the UDP
//! transport sends is charged against (#5940, decision 3).
//!
//! When `total-bandwidth-limit` is set, [`DemuxSocket`](super::DemuxSocket)
//! owns one [`EgressCap`] and charges it for every datagram it sends:
//! handshakes, ACKs, keepalives, retransmissions, short messages and stream
//! fragments. Without the setting there is no cap and no cost beyond one
//! `Option` check per send.
//!
//! # Who waits and who does not
//!
//! Only stream fragment tasks (`send_stream`, `pipe_stream`) wait for the
//! bucket. They already wait on the per-connection token bucket; here they
//! reserve their datagram and sleep until it is due.
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
//! Paced bytes never exceed the bucket bound: in any window, the burst plus
//! the rate times the window. A reservation is only given back for the part
//! of its slot not yet passed, and a datagram sent after its slot ended is
//! charged again for its lateness, so neither a released nor a late hold
//! makes bytes free.
//!
//! Unpaced bytes are added on top of that as debt, up to [`DEBT_LIMIT`] of
//! the rate ahead of now; whatever an unpaced charge cannot add is forgiven
//! (sent, but not charged) and counted by cause. So the total holds on
//! average only while less than `DEBT_LIMIT` of paced data is queued, and
//! even then can exceed the cap over a short window by the unpaced bytes
//! sent in it. With more than `DEBT_LIMIT` queued, control traffic is not
//! charged at all and the node sends the cap plus its whole control traffic
//! (`egress_forgiven_queue_past_limit_bytes` counts it).
//!
//! A paced datagram waits for everything reserved before it. With many
//! concurrent streams the wait is the queue depth divided by the rate, which
//! has no fixed bound: `n` streams at a cap of `r` bytes per second give each
//! stream one datagram every `n * 1200 / r` seconds. When that gap exceeds the
//! receiver's `STREAM_INACTIVITY_TIMEOUT` (5 s), the receiver abandons the
//! stream. Waits of at least [`LONG_WAIT`] are counted
//! (`egress_long_waits`) and logged at WARN, at most once per
//! [`LONG_WAIT_WARN_INTERVAL`].
//!
//! # The bucket
//!
//! The state is one atomic: `empty_at_ns`, the time at which the bucket level
//! is zero. At time `now` the level is `(now - empty_at_ns) * rate`, at most
//! the burst size, and negative while `empty_at_ns` is in the future (debt,
//! or queued reservations). Charging `n` bytes moves `empty_at_ns` later by
//! the time the rate needs to send `n` bytes. A paced sender charges first (a
//! reservation) and then sleeps until `empty_at_ns` as it was after its own
//! charge, so concurrent paced senders are served in the order they
//! reserved.
//!
//! All arithmetic is in integer nanoseconds of the [`TimeSource`] the cap was
//! built with; waiters sleep on their own `TimeSource`.

use std::fmt;
use std::mem::ManuallyDrop;
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use super::Socket;
use super::metrics::{EgressDecision, ForgivenCause, TRANSPORT_METRICS};
use super::packet_data::MAX_PACKET_SIZE;
use super::peer_connection::streaming::STREAM_INACTIVITY_TIMEOUT;
use crate::simulation::TimeSource;

/// The bucket holds at most this much time at the cap rate, so after an idle
/// period the node can send this long at line rate before pacing starts.
const BURST_WINDOW: Duration = Duration::from_millis(25);

/// The burst is never smaller than this many full datagrams, so a cap far
/// below a packet per burst window still admits a few at once. (A count of
/// datagrams on purpose: this sizes a rate bucket and holds no memory.)
const MIN_BURST_PACKETS: u64 = 4;

/// An unpaced charge never takes the debt past this much time at the cap
/// rate; the rest of the charge is forgiven. This bounds debt, not waits:
/// queued paced reservations are never forgiven, so a paced wait can be
/// longer than this (see the module docs).
pub(crate) const DEBT_LIMIT: Duration = Duration::from_secs(1);

/// A paced wait at least this long is counted and logged: half the
/// receiver's stream inactivity timeout, so it is reported before gaps
/// between one stream's fragments reach the point where the receiver gives
/// the stream up.
pub(crate) const LONG_WAIT: Duration =
    Duration::from_millis(STREAM_INACTIVITY_TIMEOUT.as_millis() as u64 / 2);

/// The long-wait warning is logged at most once per this interval.
const LONG_WAIT_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Egress below this many bytes per second per allowed connection is too low
/// for a full set of connections: their ACKs, keepalives and handshakes alone
/// can use most of it, so streams barely move. Used for a startup warning
/// only; the cap is applied as configured.
pub(crate) const MIN_CAP_PER_CONNECTION: usize = 4 * MAX_PACKET_SIZE;

const NANOS_PER_SEC: u128 = 1_000_000_000;

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
    /// Paced datagrams that waited at least [`LONG_WAIT`].
    pub long_waits: u64,
    /// Largest backlog seen after a charge, in bytes: everything charged or
    /// reserved ahead of the rate (unpaced debt plus queued reservations).
    pub backlog_hwm_bytes: u64,
    /// Unpaced bytes not charged, by [`ForgivenCause`] (index = `cause as
    /// usize`).
    pub forgiven_bytes: [u64; ForgivenCause::COUNT],
}

#[cfg(test)]
#[derive(Default)]
struct StatCounters {
    packets: [AtomicU64; EgressDecision::COUNT],
    bytes: AtomicU64,
    bytes_waited: AtomicU64,
    wait_us_sum: AtomicU64,
    wait_us_max: AtomicU64,
    long_waits: AtomicU64,
    backlog_hwm_bytes: AtomicU64,
    forgiven_bytes: [AtomicU64; ForgivenCause::COUNT],
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
    /// Time (ns, on `clock`) at which the bucket level is zero.
    empty_at_ns: AtomicI64,
    /// When the long-wait warning was last logged (ns, on `clock`).
    last_long_wait_warn_ns: AtomicI64,
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

/// A reservation made at a pacing point: bytes already charged to the
/// bucket for a datagram not sent yet.
///
/// Pass it to [`Socket::send_to_reserved`] with the datagram, which replaces
/// the reserved bytes with the bytes really sent. Dropping it instead (the
/// send was abandoned or failed, or the task cancelled) returns the reserved
/// bytes.
#[must_use = "dropping a hold returns its reservation; send with Socket::send_to_reserved"]
pub struct EgressHold<'a> {
    cap: &'a EgressCap,
    reserved_bytes: u64,
    wait: Duration,
    /// End of this reservation's slot (ns, on the cap's clock): when it was
    /// made, the later of `empty_at_ns` after it and the time it was made
    /// (with tokens to spare the slot ends at once). This is when the
    /// datagram may be sent.
    due_ns: i64,
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
        let clock = Box::new(move || time_source.now_nanos());
        let now = clock() as i64;
        Self {
            rate_bytes_per_sec: rate,
            burst_ns,
            debt_limit_ns,
            clock,
            empty_at_ns: AtomicI64::new(now.saturating_sub(burst_ns)),
            last_long_wait_warn_ns: AtomicI64::new(i64::MIN),
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

    pub(crate) fn burst_bytes(&self) -> u64 {
        self.bytes_for(self.burst_ns)
    }

    /// Time the rate needs for `bytes`, rounded up, so rounding never
    /// lets more through than the rate.
    fn nanos_for(rate: u64, bytes: u64) -> i64 {
        let ns = (bytes as u128 * NANOS_PER_SEC).div_ceil(rate as u128);
        i64::try_from(ns).unwrap_or(i64::MAX)
    }

    fn cost_ns(&self, bytes: u64) -> i64 {
        Self::nanos_for(self.rate_bytes_per_sec, bytes)
    }

    fn bytes_for(&self, ns: i64) -> u64 {
        if ns <= 0 {
            return 0;
        }
        (ns as u128 * self.rate_bytes_per_sec as u128 / NANOS_PER_SEC) as u64
    }

    fn now(&self) -> i64 {
        i64::try_from((self.clock)()).unwrap_or(i64::MAX)
    }

    /// Apply `f(empty_at_ns, now)` atomically; returns the old value, the
    /// new value and `now`.
    fn update(&self, f: impl Fn(i64, i64) -> i64) -> (i64, i64, i64) {
        let now = self.now();
        let mut old = self.empty_at_ns.load(Ordering::Acquire);
        loop {
            let new = f(old, now);
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

    /// `empty_at_ns` refilled to `now`: the bucket never holds more than the
    /// burst.
    fn refilled(&self, empty_at: i64, now: i64) -> i64 {
        empty_at.max(now.saturating_sub(self.burst_ns))
    }

    /// Charge a datagram sent without pacing. The bucket may go into debt,
    /// but an unpaced charge never takes `empty_at_ns` past
    /// `now + DEBT_LIMIT`; whatever it cannot add is forgiven.
    ///
    /// It also never moves `empty_at_ns` earlier. Paced reservations may
    /// already reach past `now + DEBT_LIMIT` (many queued streams), and their
    /// holders still send at the times they were given; moving the value back
    /// would hand the same times to new reservations and let the node send
    /// one datagram per queued stream per slot, above the cap.
    pub(crate) fn charge_unpaced(&self, bytes: usize) {
        let cost = self.cost_ns(bytes as u64);
        let (old, new, now) = self.update(|empty_at, now| {
            let refilled = self.refilled(empty_at, now);
            refilled.max(
                refilled
                    .saturating_add(cost)
                    .min(now.saturating_add(self.debt_limit_ns)),
            )
        });
        // Recomputed from the value the committed update started from, since
        // the closure may run more than once.
        let refilled = self.refilled(old, now);
        let unclamped = refilled.saturating_add(cost);
        let forgiven = self.bytes_for(unclamped.saturating_sub(new));
        if forgiven > 0 {
            // Two causes, told apart for operators: unpaced debt reached the
            // limit, or paced reservations already reached PAST it (so
            // nothing unpaced can be charged at all). An unpaced charge never
            // takes `empty_at_ns` past the limit, only to it, so a value
            // strictly past it was put there by reservations.
            let cause = if refilled > now.saturating_add(self.debt_limit_ns) {
                ForgivenCause::QueuePastLimit
            } else {
                ForgivenCause::DebtOverLimit
            };
            #[cfg(test)]
            self.stats.forgiven_bytes[cause as usize].fetch_add(forgiven, Ordering::Relaxed);
            TRANSPORT_METRICS.record_egress_forgiven(cause, forgiven);
        }
        self.record_sent(EgressDecision::Unpaced, bytes as u64, Duration::ZERO, now);
        self.record_backlog(new, now);
    }

    /// Reserve `bytes` for a datagram a pacing point is about to send, and
    /// say how long to wait before sending it: until everything reserved or
    /// charged before it has been paid at the rate.
    pub(crate) fn reserve(&self, bytes: usize) -> EgressHold<'_> {
        let cost = self.cost_ns(bytes as u64);
        let (_, new, now) =
            self.update(|empty_at, now| self.refilled(empty_at, now).saturating_add(cost));
        let wait_ns = new.saturating_sub(now).max(0) as u64;
        EgressHold {
            cap: self,
            reserved_bytes: bytes as u64,
            wait: Duration::from_nanos(wait_ns),
            due_ns: new.max(now),
        }
    }

    /// Return the part of an unsent hold's slot that has not passed yet:
    /// `clamp(due - now, 0, cost)`. A slot that has passed is not returned:
    /// the bucket may have refilled over it, and returning it then would
    /// let later sends go out uncharged.
    fn release(&self, hold: &EgressHold<'_>) {
        let cost = self.cost_ns(hold.reserved_bytes);
        let due = hold.due_ns;
        self.update(|empty_at, now| {
            let refund = due.saturating_sub(now).clamp(0, cost);
            empty_at.saturating_sub(refund)
        });
    }

    /// Replace a reservation with the `sent_bytes` the socket really sent,
    /// by the same rule as [`Self::release`]: only the not-yet-passed part
    /// of the unused slot is returned, and a datagram sent after its slot
    /// ended is charged for the time it is late (up to its own cost), so a
    /// late send is never free.
    fn settle(&self, hold: &EgressHold<'_>, sent_bytes: usize) {
        debug_assert!(
            sent_bytes as u64 <= hold.reserved_bytes,
            "a {sent_bytes} byte datagram was sent on a {} byte reservation; \
             reservations must be an upper bound (see PACED_RESERVATION_BYTES)",
            hold.reserved_bytes
        );
        let reserved = self.cost_ns(hold.reserved_bytes);
        let sent = self.cost_ns(sent_bytes as u64);
        let unused = reserved.saturating_sub(sent);
        let due = hold.due_ns;
        let (_, new, now) = self.update(|empty_at, now| {
            let refund = due.saturating_sub(now).clamp(0, unused);
            let late = now.saturating_sub(due).clamp(0, sent);
            self.refilled(empty_at.saturating_sub(refund), now)
                .saturating_add(late)
        });
        let decision = if hold.wait.is_zero() {
            EgressDecision::Immediate
        } else {
            EgressDecision::Waited
        };
        self.record_sent(decision, sent_bytes as u64, hold.wait, now);
        self.record_backlog(new, now);
    }

    /// Record one sent datagram, and a long wait if it had one. Production
    /// writes only the `TRANSPORT_METRICS` period counters; the per-cap
    /// counters exist only in tests.
    fn record_sent(&self, decision: EgressDecision, bytes: u64, wait: Duration, now: i64) {
        let wait_us = u64::try_from(wait.as_micros()).unwrap_or(u64::MAX);
        let long = wait >= LONG_WAIT;
        TRANSPORT_METRICS.record_egress(decision, bytes, wait_us, long);
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
            if long {
                s.long_waits.fetch_add(1, Ordering::Relaxed);
            }
        }
        if long {
            self.warn_long_wait(wait, now);
        }
    }

    /// Log a long paced wait at WARN, at most once per
    /// [`LONG_WAIT_WARN_INTERVAL`].
    fn warn_long_wait(&self, wait: Duration, now: i64) {
        let interval = i64::try_from(LONG_WAIT_WARN_INTERVAL.as_nanos()).unwrap_or(i64::MAX);
        let last = self.last_long_wait_warn_ns.load(Ordering::Relaxed);
        if last != i64::MIN && now.saturating_sub(last) < interval {
            return;
        }
        if self
            .last_long_wait_warn_ns
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        tracing::warn!(
            wait_ms = wait.as_millis() as u64,
            total_bandwidth_limit = self.rate_bytes_per_sec,
            stream_inactivity_timeout_ms = STREAM_INACTIVITY_TIMEOUT.as_millis() as u64,
            "A stream fragment waited this long for the node-wide egress cap \
             (total-bandwidth-limit): too many concurrent streams for the cap. \
             Receivers abandon a stream after a gap of stream_inactivity_timeout_ms \
             between its fragments; raise total-bandwidth-limit if transfers fail"
        );
    }

    /// Record the backlog after a charge: everything charged or reserved
    /// ahead of the rate, i.e. unpaced debt plus queued reservations.
    fn record_backlog(&self, empty_at: i64, now: i64) {
        let backlog = self.bytes_for(empty_at.saturating_sub(now));
        if backlog > 0 {
            #[cfg(test)]
            self.stats
                .backlog_hwm_bytes
                .fetch_max(backlog, Ordering::Relaxed);
            TRANSPORT_METRICS.record_egress_backlog(backlog);
        }
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
            long_waits: load(&s.long_waits),
            backlog_hwm_bytes: load(&s.backlog_hwm_bytes),
            forgiven_bytes: std::array::from_fn(|i| load(&s.forgiven_bytes[i])),
        }
    }
}

impl EgressHold<'_> {
    /// How long to wait before sending the datagram.
    #[cfg(test)]
    pub(crate) fn wait(&self) -> Duration {
        self.wait
    }

    /// Account for the datagram as sent with `sent_bytes` bytes. Called by
    /// the socket after the send succeeded; on a failed send the socket drops
    /// the hold instead, which returns the reservation.
    pub(crate) fn settle(self, sent_bytes: usize) {
        let this = ManuallyDrop::new(self);
        this.cap.settle(&this, sent_bytes);
    }
}

impl Drop for EgressHold<'_> {
    fn drop(&mut self) {
        self.cap.release(self);
    }
}

impl fmt::Debug for EgressHold<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EgressHold")
            .field("reserved_bytes", &self.reserved_bytes)
            .field("wait", &self.wait)
            .finish()
    }
}

/// What a pacing point reserves for its datagram: the largest datagram the
/// transport sends. The reservation must not be smaller than the datagram,
/// because every sender queued behind it computed its wait from it; an
/// estimate short by `d` bytes lets each of `n` queued senders go `d` bytes
/// early, so a window holds `n * d` bytes more than the cap allows. With an
/// upper bound, a datagram smaller than its reservation wastes the rest of
/// the slot (settling returns only a part not yet passed), which delays the
/// queued senders slightly instead.
pub(crate) const PACED_RESERVATION_BYTES: usize = MAX_PACKET_SIZE;

/// Wait at a pacing point until a datagram may be sent through `socket`,
/// sleeping on `time_source`. Reserves [`PACED_RESERVATION_BYTES`].
///
/// Returns `None` when the socket has no cap. Otherwise returns the hold to
/// pass to [`Socket::send_to_reserved`]. Only stream fragment tasks call this
/// (see the module docs for who must never wait).
///
/// Cancel-safe: if the returned future is dropped while it sleeps, the hold
/// is dropped with it and the reservation is returned.
pub(crate) async fn pace<'a, S: Socket, T: TimeSource>(
    socket: &'a S,
    time_source: &T,
) -> Option<EgressHold<'a>> {
    let cap = socket.egress_cap()?;
    let hold = cap.reserve(PACED_RESERVATION_BYTES);
    if !hold.wait.is_zero() {
        time_source.sleep(hold.wait).await;
    }
    Some(hold)
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
