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
//! Only the places that already pace their sends wait for the bucket:
//!
//! - stream fragment tasks (`send_stream`, `pipe_stream`), which already wait
//!   on the per-connection token bucket, reserve their datagram here and
//!   sleep until it is due;
//! - short messages wait at most [`SHORT_MESSAGE_MAX_WAIT`] and are then sent
//!   anyway.
//!
//! Every other send (the handshake tasks, the keepalive task, and everything
//! `PeerConnection::recv` sends: ACKs, Pongs, receipts, retransmissions) is
//! sent at once and charged even when the bucket is empty, which puts the
//! bucket into debt. Paced senders then wait longer until the debt is paid.
//! `recv` must never wait here: it runs as a branch of a `select!` that can be
//! cancelled, and it changes connection state before some of its sends.
//!
//! The cap therefore holds on average. Over a short window the node can send
//! more than the cap by the bytes of unpaced sends in that window. Debt older
//! than [`DEBT_LIMIT`] is forgiven (and counted), so a period in which unpaced
//! traffic alone exceeds the cap cannot stop paced traffic for longer than
//! that.
//!
//! # The bucket
//!
//! The state is one atomic: `empty_at_ns`, the time at which the bucket level
//! is zero. At time `now` the level is `(now - empty_at_ns) * rate`, at most
//! the burst size, and negative while `empty_at_ns` is in the future (debt).
//! Charging `n` bytes moves `empty_at_ns` later by the time the rate needs to
//! send `n` bytes. A paced sender charges first (a reservation) and then
//! sleeps until `empty_at_ns` as it was after its own charge, so concurrent
//! paced senders are served in the order they reserved.
//!
//! All arithmetic is in integer nanoseconds of the [`TimeSource`] the cap was
//! built with; waiters sleep on their own `TimeSource`.

use std::fmt;
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use super::Socket;
use super::metrics::{EgressDecision, TRANSPORT_METRICS};
use super::packet_data::MAX_PACKET_SIZE;
use crate::simulation::TimeSource;

/// The bucket holds at most this much time at the cap rate, so after an idle
/// period the node can send this long at line rate before pacing starts.
const BURST_WINDOW: Duration = Duration::from_millis(25);

/// The burst is never smaller than this many full datagrams, so a cap far
/// below a packet per burst window still admits a few at once. (A count of
/// datagrams on purpose: this sizes a rate bucket and holds no memory.)
const MIN_BURST_PACKETS: u64 = 4;

/// Debt beyond this much time at the cap rate is forgiven. Bounds how long a
/// paced sender waits for one datagram (about this long plus its own
/// transmission time).
pub(crate) const DEBT_LIMIT: Duration = Duration::from_secs(1);

/// The longest a short message waits for the bucket. A short message that
/// would wait longer waits this long and is then sent into debt. Short
/// messages are sent from the connection's own task, which also receives
/// for the connection, so the wait must stay short.
pub(crate) const SHORT_MESSAGE_MAX_WAIT: Duration = Duration::from_millis(20);

/// Egress below this many bytes per second per allowed connection is too low
/// for a full set of connections: their ACKs, keepalives and handshakes alone
/// can use most of it, so streams barely move. Used for a startup warning
/// only; the cap is applied as configured.
pub(crate) const MIN_CAP_PER_CONNECTION: usize = 4 * MAX_PACKET_SIZE;

const NANOS_PER_SEC: u128 = 1_000_000_000;

/// The cap's own counters, so tests can assert on one cap without reading
/// process-global state. The node-wide period counters are in
/// `TRANSPORT_METRICS` (`egress_*`).
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
    /// Largest debt seen after a charge, in bytes.
    pub debt_hwm_bytes: u64,
    /// Debt forgiven because it exceeded [`DEBT_LIMIT`], in bytes.
    pub forgiven_bytes: u64,
}

#[derive(Default)]
struct StatCounters {
    packets: [AtomicU64; EgressDecision::COUNT],
    bytes: AtomicU64,
    bytes_waited: AtomicU64,
    wait_us_sum: AtomicU64,
    wait_us_max: AtomicU64,
    debt_hwm_bytes: AtomicU64,
    forgiven_bytes: AtomicU64,
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
/// the reserved bytes with the datagram's real size. Dropping it instead (the
/// send was abandoned or the task cancelled) returns the reserved bytes.
#[must_use = "dropping a hold returns its reservation; send with Socket::send_to_reserved"]
pub struct EgressHold<'a> {
    cap: &'a EgressCap,
    reserved_bytes: u64,
    decision: EgressDecision,
    wait: Duration,
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

    /// Charge a datagram sent without pacing. The bucket may go into debt;
    /// debt beyond [`DEBT_LIMIT`] is forgiven.
    pub(crate) fn charge_unpaced(&self, bytes: usize) {
        let cost = self.cost_ns(bytes as u64);
        let (old, new, now) = self.update(|empty_at, now| {
            let charged = self.refilled(empty_at, now).saturating_add(cost);
            charged.min(now.saturating_add(self.debt_limit_ns))
        });
        // Recomputed from the value the committed update started from, since
        // the closure may run more than once.
        let unclamped = self.refilled(old, now).saturating_add(cost);
        let forgiven = self.bytes_for(unclamped.saturating_sub(new));
        if forgiven > 0 {
            self.stats
                .forgiven_bytes
                .fetch_add(forgiven, Ordering::Relaxed);
            TRANSPORT_METRICS.record_egress_forgiven(forgiven);
        }
        self.record_sent(EgressDecision::Unpaced, bytes as u64, Duration::ZERO);
        self.record_debt(new, now);
    }

    /// Reserve `bytes` for a datagram a pacing point is about to send, and
    /// say how long to wait before sending it.
    ///
    /// With `max_wait`, a datagram that would wait longer reserves nothing
    /// and waits `max_wait` instead; it is then sent into debt.
    pub(crate) fn reserve(&self, bytes: usize, max_wait: Option<Duration>) -> EgressHold<'_> {
        let cost = self.cost_ns(bytes as u64);
        let max_wait_ns = max_wait.map(|w| i64::try_from(w.as_nanos()).unwrap_or(i64::MAX));
        let (old, new, now) = self.update(|empty_at, now| {
            let charged = self.refilled(empty_at, now).saturating_add(cost);
            let wait = charged.saturating_sub(now);
            match max_wait_ns {
                Some(limit) if wait > limit => empty_at,
                _ => charged,
            }
        });
        // Recomputed from the value the committed update started from, since
        // the closure may run more than once.
        let charged = self.refilled(old, now).saturating_add(cost);
        if new != charged {
            // Nothing was committed: the wait would have exceeded `max_wait`,
            // which is therefore `Some`.
            return EgressHold {
                cap: self,
                reserved_bytes: 0,
                decision: EgressDecision::WaitCapped,
                wait: max_wait.unwrap_or_default(),
            };
        }
        let wait_ns = new.saturating_sub(now).max(0) as u64;
        let wait = Duration::from_nanos(wait_ns);
        EgressHold {
            cap: self,
            reserved_bytes: bytes as u64,
            decision: if wait.is_zero() {
                EgressDecision::Immediate
            } else {
                EgressDecision::Waited
            },
            wait,
        }
    }

    fn release(&self, reserved_bytes: u64) {
        if reserved_bytes == 0 {
            return;
        }
        let cost = self.cost_ns(reserved_bytes);
        self.update(|empty_at, _| empty_at.saturating_sub(cost));
    }

    /// Replace a reservation of `reserved_bytes` with the `sent_bytes` the
    /// datagram really had.
    fn settle(&self, hold: &EgressHold<'_>, sent_bytes: usize) {
        debug_assert!(
            hold.reserved_bytes == 0 || sent_bytes as u64 <= hold.reserved_bytes,
            "a {sent_bytes} byte datagram was sent on a {} byte reservation; \
             reservations must be an upper bound (see PACED_RESERVATION_BYTES)",
            hold.reserved_bytes
        );
        let reserved = self.cost_ns(hold.reserved_bytes);
        let sent = self.cost_ns(sent_bytes as u64);
        let (_, new, now) = self.update(|empty_at, now| {
            self.refilled(empty_at.saturating_sub(reserved), now)
                .saturating_add(sent)
        });
        self.record_sent(hold.decision, sent_bytes as u64, hold.wait);
        self.record_debt(new, now);
    }

    fn record_sent(&self, decision: EgressDecision, bytes: u64, wait: Duration) {
        let s = &self.stats;
        s.packets[decision as usize].fetch_add(1, Ordering::Relaxed);
        s.bytes.fetch_add(bytes, Ordering::Relaxed);
        let wait_us = u64::try_from(wait.as_micros()).unwrap_or(u64::MAX);
        if !wait.is_zero() {
            s.bytes_waited.fetch_add(bytes, Ordering::Relaxed);
            s.wait_us_sum.fetch_add(wait_us, Ordering::Relaxed);
            s.wait_us_max.fetch_max(wait_us, Ordering::Relaxed);
        }
        TRANSPORT_METRICS.record_egress(decision, bytes, wait_us);
    }

    fn record_debt(&self, empty_at: i64, now: i64) {
        let debt = self.bytes_for(empty_at.saturating_sub(now));
        if debt > 0 {
            self.stats.debt_hwm_bytes.fetch_max(debt, Ordering::Relaxed);
            TRANSPORT_METRICS.record_egress_debt(debt);
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
            debt_hwm_bytes: load(&s.debt_hwm_bytes),
            forgiven_bytes: load(&s.forgiven_bytes),
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
    /// the socket immediately before it sends.
    pub(crate) fn settle(self, sent_bytes: usize) {
        let this = ManuallyDrop::new(self);
        this.cap.settle(&this, sent_bytes);
    }
}

impl Drop for EgressHold<'_> {
    fn drop(&mut self) {
        self.cap.release(self.reserved_bytes);
    }
}

impl fmt::Debug for EgressHold<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EgressHold")
            .field("reserved_bytes", &self.reserved_bytes)
            .field("decision", &self.decision)
            .field("wait", &self.wait)
            .finish()
    }
}

/// What a pacing point reserves for its datagram: the largest datagram the
/// transport sends. The reservation must not be smaller than the datagram,
/// because every sender queued behind it computed its wait from it; an
/// estimate short by `d` bytes lets each of `n` queued senders go `d` bytes
/// early, so a window holds `n * d` bytes more than the cap allows. With an
/// upper bound, settling the real size only returns bytes, which delays the
/// queued senders slightly instead.
pub(crate) const PACED_RESERVATION_BYTES: usize = MAX_PACKET_SIZE;

/// Wait at a pacing point until a datagram may be sent through `socket`,
/// sleeping on `time_source`. Reserves [`PACED_RESERVATION_BYTES`].
///
/// Returns `None` when the socket has no cap. Otherwise returns the hold to
/// pass to [`Socket::send_to_reserved`]. With `max_wait`, the wait is at most
/// that long (see [`EgressCap::reserve`]).
///
/// Cancel-safe: if the returned future is dropped while it sleeps, the hold
/// is dropped with it and the reservation is returned.
pub(crate) async fn pace<'a, S: Socket, T: TimeSource>(
    socket: &'a S,
    max_wait: Option<Duration>,
    time_source: &T,
) -> Option<EgressHold<'a>> {
    let cap = socket.egress_cap()?;
    let hold = cap.reserve(PACED_RESERVATION_BYTES, max_wait);
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
