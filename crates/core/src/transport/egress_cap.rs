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
//! cap. Then it waits in the node-wide cap's admission queue
//! ([`StreamPacing::wait_for_admission`], over [`EgressCap::poll_admit`]),
//! the same queue the QUIC endpoint waits in. If it had to wait it starts
//! again from the cwnd check, keeping its connection payment and so its
//! place on the connection, and the queue then admits it at once; right
//! after the cwnd check it calls [`EgressCap::try_take_admitted`] and sends at once. The queue
//! holds the stream's waker and its place, never bytes, so a waiting stream
//! holds no node-wide capacity. An admitted sender holds one datagram from
//! its admission to its take, at most [`ADMISSION_PASS`] (1 ms): every other
//! take leaves it in the bucket meanwhile. A sender that was admitted and has
//! not sent (its take failed, or its congestion window held it past the pass)
//! keeps its place: it queues again where it was admitted from. Waiters are admitted in the order they
//! queued (QUIC connections and UDP streams alike, one entry each), so
//! connections share the cap in proportion to their number of waiting
//! streams and QUIC connections; there is no isolation between connections,
//! and a peer driving many streams gets a proportional share.
//!
//! Until #6112 a stream did not queue: it took with `try_take` and, when
//! refused, slept the deficit with jitter and tried again, a race among the
//! waiting streams. The QUIC endpoint queued, so it won contended refills:
//! measured on a 500 kB/s cap with one QUIC path and one UDP stream at equal
//! shares, about 462 kB/s against 38 kB/s, and 249 against 251 kB/s with
//! both queued.
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
//! and everything `PeerConnection::recv` sends: ACKs, Pongs, receipts.
//! `recv` must never wait here: it runs as a branch of a `select!` that can
//! be cancelled, and it changes connection state before some of its sends.
//!
//! Retransmissions (RTO resends and tail-loss probes, sent from `recv`) do
//! not wait either, but they are not simply charged: they take from the
//! bucket only within [`RETX_DEBT_LIMIT`] and their share of the rate
//! ([`EgressCap::admit_retransmit`]). A refused resend is deferred by the
//! resend timer, for at most one effective RTO from its first refusal
//! ([`RETX_DEFER_LIMIT_RTOS`]), then sent uncapped; a refused probe is
//! skipped.
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
//! When the cap binds, the `n` waiters are served in turn, so each gets a
//! datagram about every `n * 1200 / r` seconds at a cap of `r` bytes per
//! second, without the geometric tail of the earlier race. The order does
//! not depend on which waiter polls first: [`EgressCap::poll_admit`] admits
//! a waiter only when the bucket also holds a datagram for every waiter
//! queued ahead of it and every admission whose take has not happened yet,
//! and every take leaves the datagrams owed to the other admissions, so on
//! a multi-thread runtime a sender cannot use another's turn between its
//! admission and its take. Timers decide only when a waiter looks, so a late
//! timer (tokio's are 1 ms) delays that waiter, never reorders. Two limits:
//! the queue counts at most [`MAX_ADMISSION_WAKERS`] ahead, and a stream the
//! queue admitted keeps its turn for [`ADMISSION_PASS`] while it re-checks
//! its congestion window.
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
use super::sent_packet_tracker::ResendKind;
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

/// A legacy UDP retransmit may put the bucket this far into debt, and no
/// further (`EgressCap::admit_retransmit`); past it the retransmit waits
/// for the rate, and nothing retransmitted is forgiven. Well below
/// [`DEBT_LIMIT`]: enough to send lost packets ahead of queued first copies,
/// which then wait in the admission queue, without letting a retransmit storm
/// run at whatever rate its timers produce.
pub(crate) const RETX_DEBT_LIMIT: Duration = Duration::from_millis(100);

/// The most of the cap's rate retransmits may use, over [`RETX_SHARE_WINDOW`]:
/// first copies always get the rest. Without it, where the cap binds and the
/// receiver's receipts are lost, retransmits of packets it already holds took
/// the cap ahead of first copies until a stream missed its 5 s fragment
/// deadline (the receipt-loss row in `link_model::storm_tests`).
pub(crate) const RETX_SHARE_PERCENT: u64 = 50;

/// The window the retransmit share is measured over: retransmits may burst
/// to this much of their share at once (after an outage, say), then hold to
/// the share.
pub(crate) const RETX_SHARE_WINDOW: Duration = Duration::from_secs(1);

/// [`RETX_SHARE_PERCENT`], or in tests `FREENET_RETX_SHARE_PERCENT` when set.
fn retx_share_percent() -> u64 {
    #[cfg(test)]
    if let Some(pct) = std::env::var("FREENET_RETX_SHARE_PERCENT")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
    {
        return pct.clamp(1, 100);
    }
    RETX_SHARE_PERCENT
}

/// [`RETX_DEBT_LIMIT`], or in tests `FREENET_RETX_DEBT_LIMIT_MS` when set, so a
/// storm run can sweep it without a rebuild.
fn retx_debt_limit() -> Duration {
    #[cfg(test)]
    if let Some(ms) = std::env::var("FREENET_RETX_DEBT_LIMIT_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
    {
        return Duration::from_millis(ms);
    }
    RETX_DEBT_LIMIT
}

/// The shortest a retransmit the cap refused is deferred for, however small
/// the deficit: without it a connection whose resend is refused by a
/// datagram's worth wakes its resend arm about once per datagram time while
/// the cap stays full. Chosen from the storm rows over 40 seeds (16
/// connections on a 4 Mbit/s uplink, `link_model::storm_tests`): with no
/// floor a cap at 4x the uplink completed fewer streams than uncapped
/// retransmits (166 against 192) at about 230 wakes a second per connection;
/// 2 ms completed 368 and 23 wakes; 10 ms and RTO / 16 gave back the
/// retransmits' lead over queued first copies at 2x the uplink.
pub(crate) const RETX_DEFER_MIN: Duration = Duration::from_millis(2);

/// The shortest deferral of a refused retransmit on a connection whose
/// effective RTO is `rto`: [`RETX_DEFER_MIN`]. In tests,
/// `FREENET_RETX_DEFER_MIN_US` sets a fixed floor and
/// `FREENET_RETX_DEFER_RTO_DIV` a floor of `rto / div`, so a storm run can
/// sweep it without a rebuild.
pub(crate) fn retx_defer_min(rto: Duration) -> Duration {
    #[cfg(test)]
    {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
        };
        if let Some(div) = var("FREENET_RETX_DEFER_RTO_DIV") {
            return rto / u32::try_from(div.max(1)).unwrap_or(u32::MAX);
        }
        if let Some(us) = var("FREENET_RETX_DEFER_MIN_US") {
            return Duration::from_micros(us);
        }
    }
    let _ = rto;
    RETX_DEFER_MIN
}

/// Retransmits the bucket refused come back this many per datagram time
/// at the cap's rate ([`EgressCap::space_retx_bucket_wait`]): spaced so they
/// do not all retry on one deficit, but more often than the bucket refills,
/// so a repair is not held behind the spacing when first copies take some
/// of the refill. Chosen from the storm row at 1 MB/s over 40 seeds: one
/// per datagram time completed 567 streams against main's 327 but two seeds
/// did worse than main; two per datagram time 560 with none worse; four
/// 545 with one worse.
pub(crate) const RETX_BUCKET_SPACING_DIV: i64 = 2;

/// A connection's refused RTO resends are deferred until they have been
/// refused for this many effective RTOs in a row ([`RetxDeferral`]), then
/// sent uncapped ([`EgressCap::admit_retransmit`]).
/// One: the deferral at most doubles the wait between a packet's resends,
/// so it is still abandoned after `MAX_PACKET_RETRANSMITS`, within twice the
/// time it would take without the cap.
pub(crate) const RETX_DEFER_LIMIT_RTOS: u32 = 1;

/// One connection's run of refused RTO resends, for
/// [`EgressCap::admit_retransmit`]'s deferral limit. The run starts at the
/// first refusal and lasts until the cap takes a resend again or nothing due
/// is refused ([`Self::end_run`]); once it has lasted one effective RTO
/// ([`RETX_DEFER_LIMIT_RTOS`]) every due resend goes uncapped. It is per
/// connection, not per packet, because only the head of the resend queue is
/// asked: a per-packet limit would hold the packets behind it for one RTO
/// each in turn. And it is measured from the first refusal, not from the
/// RTO deadline, because an ack that resets the backoff moves every deadline
/// earlier at once, so a resend just found due would count as long
/// deferred.
///
/// It also holds the connection's booking in the retransmit share: a resend
/// the share refuses books the next free slot instead of retrying, so the
/// connections waiting on the share wake one per slot rather than all at
/// once on the same deficit (with a herd, node-wide wakes grew with the
/// connection count toward [`RETX_DEFER_MIN`]'s 500 a second each).
#[derive(Debug, Default)]
pub(crate) struct RetxDeferral {
    /// When the run of refusals started (the connection's time source, ns).
    refused_since: Option<u64>,
    /// The share this connection has booked and not used. At most one; it
    /// is used by the next take, or refunded when the run ends or the
    /// resend goes uncapped. A connection that closes holding one loses
    /// that one datagram of share.
    booking: Option<RetxBooking>,
}

/// A booked slot in the retransmit share ([`RetxDeferral`]).
#[derive(Debug, Clone, Copy)]
pub(crate) struct RetxBooking {
    /// The share paid for it (ns on the share's meter).
    share_ns: i64,
    /// When the slot comes (ns on the cap's clock).
    slot_ns: i64,
}

impl RetxDeferral {
    /// Decide one retransmit at `now_nanos` ([`EgressCap::admit_retransmit`]),
    /// with this connection's run of refusals.
    pub(in crate::transport) fn admit<'a>(
        &mut self,
        cap: &'a EgressCap,
        kind: ResendKind,
        bytes: usize,
        rto: Duration,
        now_nanos: u64,
    ) -> RetxAdmit<'a> {
        let deferred_for = self.refused_since.map_or(Duration::ZERO, |since| {
            Duration::from_nanos(now_nanos.saturating_sub(since))
        });
        let decision = cap.admit_retransmit(kind, bytes, deferred_for, rto, &mut self.booking);
        match decision {
            // Only an RTO resend taken ends the run: a probe taken while an
            // RTO resend is refused (another packet's, after the head
            // changed) must not restart the deferral limit, or the refused
            // resend could be held past it and never abandoned (#4345).
            RetxAdmit::Take(_) if kind == ResendKind::Rto => self.refused_since = None,
            RetxAdmit::Take(_) => {}
            RetxAdmit::Defer(_) => {
                self.refused_since.get_or_insert(now_nanos);
            }
            RetxAdmit::Uncapped | RetxAdmit::SkipProbe => {}
        }
        decision
    }

    /// Tests only: the slot this connection has booked in the share.
    #[cfg(test)]
    pub(in crate::transport) fn booked_slot(&self) -> Option<i64> {
        self.booking.map(|b| b.slot_ns)
    }

    /// Nothing due was refused: the run, if any, is over, and an unused
    /// booking is handed back to `cap`.
    pub(in crate::transport) fn end_run(&mut self, cap: Option<&EgressCap>) {
        self.refused_since = None;
        if let (Some(booking), Some(cap)) = (self.booking.take(), cap) {
            cap.refund_retx_share(booking.share_ns);
        }
    }
}

/// Why a retransmit take was refused, and how long until it would fit.
#[derive(Debug, Clone, Copy)]
enum RetxRefused {
    /// The share is spent. (The wait is read only by the tests'
    /// `try_take_retransmit`: a refused resend books a slot instead.)
    Share(#[cfg_attr(not(test), allow(dead_code))] Duration),
    /// The bucket's debt would pass [`RETX_DEBT_LIMIT`].
    Bucket(Duration),
    /// The connection's booked slot has not come yet.
    Slot(Duration),
}

impl RetxRefused {
    #[cfg(test)]
    fn wait(self) -> Duration {
        match self {
            Self::Share(wait) | Self::Bucket(wait) | Self::Slot(wait) => wait,
        }
    }
}

/// What [`EgressCap::admit_retransmit`] decided.
#[derive(Debug)]
pub(crate) enum RetxAdmit<'a> {
    /// Send with this take (`Socket::send_to_paced`).
    Take(EgressTake<'a>),
    /// Send uncapped (`Socket::send_to`, charged as an unpaced send): the
    /// connection's resends have been refused for [`RETX_DEFER_LIMIT_RTOS`]
    /// effective RTOs in a row.
    Uncapped,
    /// Do not send now; ask again after this long.
    Defer(Duration),
    /// Skip this probe.
    SkipProbe,
}

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

/// The most [`EgressCap::poll_admit`] wakers the cap holds. A held waker is
/// released when it is woken, or its waiter is admitted or dropped. Past
/// this many, a newly queued waiter holds no waker and relies on its own
/// timer, which its place in the queue still staggers; no waiter loses its
/// place. The QUIC endpoint has one waiter per connection that is short of
/// bytes, and each paced UDP stream has one.
///
/// The same number bounds four things, and fits each:
/// - Held wakers: at about 16 bytes each, 16 KiB; past it a waiter relies on
///   its timer, which only makes it later, never lost.
/// - The rank counted ahead of a waiter (`QueuedWaiters::ahead_of`): at most
///   1025 steps under the queue lock per poll, a few microseconds.
/// - The clamp on a waiter's timer stagger: 1024 datagrams of refill is what
///   `quic::socket::cap_hold_validity` budgets for a QUIC transmit's wait, so
///   the hold watch never takes a live hold for an abandoned one.
/// - Live admissions (`QueuedWaiters::admitted`): each lasts at most
///   [`ADMISSION_PASS`], and at the smallest cap (4 datagrams of burst) far
///   fewer than 1024 can be admitted within one pass, so the bound binds only
///   at large caps under overload, where the next waiter waits up to one pass.
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
    /// Takes ([`EgressCap::try_take_handed`]) and admission polls
    /// ([`EgressCap::poll_admit`]) that found the bucket short.
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
    /// A sleep until the given time on the same time source as `clock`, for
    /// [`Self::poll_admit`]'s wakeups. Cancelable: a waiter woken by a
    /// give-back, or a stream that gives up at its deadline, drops its timer
    /// early, and a plain `VirtualTime` sleep would keep the wakeup and move
    /// a simulation's clock to it.
    sleep_until: Box<dyn Fn(u64) -> SleepFuture + Send + Sync>,
    /// Time (ns, on `clock`) at which the bucket level is zero.
    empty_at_ns: AtomicI64,
    /// Time (ns, on `clock`) at which the retransmit meter is empty: a leaky
    /// bucket of retransmitted bytes at [`RETX_SHARE_PERCENT`] of the rate,
    /// holding at most [`RETX_SHARE_WINDOW`] of it
    /// (`EgressCap::admit_retransmit`).
    retx_meter_empty_at_ns: AtomicI64,
    /// The next free datagram time (ns, on `clock`) for a retransmit the
    /// bucket refused ([`EgressCap::space_retx_bucket_wait`]).
    retx_bucket_next_ns: AtomicI64,
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
    /// Tests only: retransmits are sent and charged as unpaced sends, as
    /// before they were capped, so one test binary can compare both.
    #[cfg(test)]
    retransmits_uncapped: std::sync::atomic::AtomicBool,
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

/// The `Pending` [`EgressCap::poll_admit`] callers, in the order they first
/// registered, each with its place (a ticket) and, for at most
/// [`MAX_ADMISSION_WAKERS`] of them, its waker.
#[derive(Default)]
struct AdmissionQueue {
    inner: parking_lot::Mutex<QueuedWaiters>,
    /// The number of queued waiters, so a give-back skips the lock when
    /// nobody waits (every node with no paced sender short of bytes).
    waiting: std::sync::atomic::AtomicUsize,
}

#[derive(Default)]
struct QueuedWaiters {
    next_ticket: u64,
    /// Every queued waiter's ticket; the smallest is the oldest.
    ///
    /// Not capped by count, on purpose. An entry is one waiting sender: a
    /// paced UDP stream fragment or a QUIC connection's poller. It leaves
    /// when the sender is admitted or its waiter is dropped, and a stream
    /// waits at most [`PACED_WAIT_DEADLINE`] (5 s) before it fails and drops
    /// it, so the set holds the senders waiting in the last 5 s, at most.
    /// Each entry costs about 20 bytes here (a `u64` in a 104-byte B-tree
    /// leaf of 11 keys, about half full when keys arrive in order), against
    /// the stream it stands for: a task, its `StreamPacing`, and a payload
    /// larger than one packet (an operation's stream is at least the 64 KB
    /// streaming threshold). 10 000 waiting streams are about 200 KB here,
    /// against at least 12 MB of payload, and 640 MB if they are operation
    /// streams. The work per call is bounded
    /// however long the set is: `ahead_of` stops at [`MAX_ADMISSION_WAKERS`]
    /// `+ 1` steps, and insert and remove are O(log n). A cap here would fail
    /// streams that the queue could still serve in turn.
    places: std::collections::BTreeSet<u64>,
    /// The wakers held, by ticket, at most [`MAX_ADMISSION_WAKERS`]. Kept
    /// apart from `places` so waking the oldest holders never walks past
    /// waiters that hold none.
    wakers: std::collections::BTreeMap<u64, std::task::Waker>,
    /// The admissions whose take has not happened yet, oldest first, at
    /// most [`MAX_ADMISSION_WAKERS`]. [`EgressCap::poll_admit`] gives up a
    /// waiter's place when it admits it, and the caller takes afterwards;
    /// until then the admission still counts ahead of every other waiter,
    /// and every take leaves one datagram in the bucket for each admission
    /// but the taker's own, so a sender on another thread in that gap can
    /// neither be admitted on the same datagram nor take it. The taker's
    /// own is removed by its take. One whose take never comes (the stream
    /// ended, or its window held it) lapses after [`ADMISSION_PASS`], so
    /// none can owe the bucket for good; a take after that is not the
    /// admission's any more and leaves the datagrams owed to the others.
    admitted: std::collections::VecDeque<Admitted>,
}

/// An admission whose take has not happened yet ([`QueuedWaiters::admitted`]).
#[derive(Clone, Copy, Debug)]
struct Admitted {
    /// When it was made: ns on the cap's clock, read under the queue lock,
    /// so a later admission never has an earlier time.
    at: i64,
    /// Its number, kept by its waiter: the waiter withdraws it if it polls
    /// again before a take used it, or is dropped. Only a take that names
    /// this number uses it: a paced UDP stream's own
    /// ([`EgressCap::try_take_admitted`]), or the QUIC socket's, handed the
    /// number by the poller that was admitted ([`EgressCap::try_take_handed`]).
    id: u64,
}

impl QueuedWaiters {
    fn sync(&self, waiting: &std::sync::atomic::AtomicUsize) {
        waiting.store(self.places.len() + self.admitted.len(), Ordering::Release);
    }

    /// Drop admissions older than [`ADMISSION_PASS`] at `now`.
    fn lapse(&mut self, now: i64) {
        let pass = i64::try_from(ADMISSION_PASS.as_nanos()).unwrap_or(i64::MAX);
        while self
            .admitted
            .front()
            .is_some_and(|admitted| now.saturating_sub(admitted.at) > pass)
        {
            self.admitted.pop_front();
        }
    }

    /// Where in [`Self::admitted`] the admission numbered `id` is, if it has
    /// not lapsed or been used.
    fn admission_of(&self, id: Option<u64>) -> Option<usize> {
        let id = id?;
        self.admitted.iter().position(|a| a.id == id)
    }

    /// How many waiters are queued ahead of `ticket`, counted exactly up to
    /// [`MAX_ADMISSION_WAKERS`] `+ 1`, which means "more than the bound".
    /// The count stops there, so it costs at most that many steps under the
    /// lock however long the queue is.
    fn ahead_of(&self, ticket: u64) -> usize {
        self.places
            .range(..ticket)
            .take(MAX_ADMISSION_WAKERS + 1)
            .count()
    }
}

impl AdmissionQueue {
    /// Queue the waiter holding `ticket` (a new one gets the next ticket, at
    /// the back) with `waker`, and return how many waiters are ahead of it
    /// ([`QueuedWaiters::ahead_of`], so at most [`MAX_ADMISSION_WAKERS`]
    /// `+ 1`). A waiter keeps its place across polls,
    /// so its staggered timer stays consistent with its position. The queue
    /// holds at most [`MAX_ADMISSION_WAKERS`] wakers: past that, a waiter is
    /// queued with no waker, WITHOUT any other being dropped, and relies on
    /// its timer; it holds a waker on a later poll once one is free.
    fn register(&self, ticket: &mut Option<u64>, waker: &std::task::Waker) -> (usize, Option<i64>) {
        let mut queue = self.inner.lock();
        let t = match *ticket {
            Some(t) if queue.places.contains(&t) => t,
            _ => {
                let t = queue.next_ticket;
                queue.next_ticket += 1;
                queue.places.insert(t);
                *ticket = Some(t);
                t
            }
        };
        let room = queue.wakers.len() < MAX_ADMISSION_WAKERS;
        match queue.wakers.get_mut(&t) {
            Some(held) => {
                if !held.will_wake(waker) {
                    *held = waker.clone();
                }
            }
            None if room => {
                queue.wakers.insert(t, waker.clone());
            }
            None => {}
        }
        queue.sync(&self.waiting);
        // When the oldest outstanding admission lapses: the waiter's timer
        // counted its datagram, which is free from then on.
        let pass = i64::try_from(ADMISSION_PASS.as_nanos()).unwrap_or(i64::MAX);
        let lapses_at = queue
            .admitted
            .front()
            .map(|admitted| admitted.at.saturating_add(pass).saturating_add(1));
        // The same count as `try_admit`'s, so `poll_admit`'s check after
        // registering agrees with its check before.
        if queue.admitted.len() >= MAX_ADMISSION_WAKERS {
            // `try_admit` admits nobody until one lapses or is taken.
            return (MAX_ADMISSION_WAKERS + 1, lapses_at);
        }
        let ahead = queue
            .ahead_of(t)
            .saturating_add(queue.admitted.len())
            .min(MAX_ADMISSION_WAKERS + 1);
        (ahead, lapses_at)
    }

    /// Admit the waiter holding `ticket` if `fits(ahead)` says the bucket
    /// covers it, where `ahead` is how many waiters are ahead of it
    /// ([`QueuedWaiters::ahead_of`]; every queued one, for a waiter not
    /// queued) plus every admission whose take has not happened yet
    /// (`QueuedWaiters::admitted`), at most [`MAX_ADMISSION_WAKERS`] `+ 1`.
    /// `clock` is read once, under the lock, and `fits` gets that time.
    /// Returns `Ok` when admitted, else `Err((ahead, now))`.
    ///
    /// The clock read, the count, the check and the recording of the
    /// admission happen under one lock, so two senders polling at once on
    /// different threads cannot both count nobody ahead and both be admitted
    /// on one datagram, and an admission's time is never earlier than the
    /// times of those recorded before it (a time read before the lock could
    /// be, and the admission would then lapse early).
    ///
    /// `admission` is the number of the waiter's admission, if it holds one.
    /// A waiter that polls again did not use it, so it is withdrawn first;
    /// when admitted the waiter gets a new number. `held_place` is given for
    /// a waiter that keeps its place until it sends ([`AdmissionWaiter::named`]).
    ///
    /// On admission it removes the waiter's place and wakes the oldest
    /// waiter still holding a waker: the next in line. Its timer was set
    /// from its place when it last polled, and waiters ahead that left
    /// since (streams failing at their deadline) made that timer late;
    /// woken now, it re-polls from its current place, so the bucket's refill
    /// is not left unused while it sleeps. One wake per admission, after the
    /// lock is released.
    fn try_admit(
        &self,
        ticket: &mut Option<u64>,
        admission: &mut Option<u64>,
        held_place: Option<&mut Option<u64>>,
        clock: impl Fn() -> i64,
        fits: impl FnOnce(usize, i64) -> bool,
    ) -> Result<(), (usize, i64)> {
        let (next, admitted) = {
            let mut queue = self.inner.lock();
            let now = clock();
            queue.lapse(now);
            // A waiter that polls again did not use the admission it holds.
            let withdrew = admission
                .take()
                .and_then(|held| queue.admission_of(Some(held)))
                .map(|i| queue.admitted.remove(i))
                .is_some();
            // Admitted before and not sent since (its take failed, or its
            // window held it past the pass): it queues at the place it was
            // admitted from, not at the back.
            if ticket.is_none() {
                if let Some(place) = held_place.as_deref().copied().flatten() {
                    queue.places.insert(place);
                    *ticket = Some(place);
                }
            }
            let queued = match *ticket {
                Some(t) if queue.places.contains(&t) => queue.ahead_of(t),
                _ => queue.places.len(),
            };
            let ahead = queued
                .saturating_add(queue.admitted.len())
                .min(MAX_ADMISSION_WAKERS + 1);
            // At most `MAX_ADMISSION_WAKERS` admissions are live at once, all
            // within their pass after `lapse`: a waiter past that waits, as one
            // too far back does, and none is dropped while its owner may
            // still take.
            let refused = if queue.admitted.len() >= MAX_ADMISSION_WAKERS {
                Some(MAX_ADMISSION_WAKERS + 1)
            } else if !fits(ahead, now) {
                Some(ahead)
            } else {
                None
            };
            if let Some(ahead) = refused {
                // A withdrawn admission frees the datagram the next waiter's
                // timer counted on: wake it to look again.
                let next = withdrew
                    .then(|| queue.wakers.pop_first().map(|(_, waker)| waker))
                    .flatten();
                queue.sync(&self.waiting);
                (next, Err((ahead, now)))
            } else {
                let id = queue.next_ticket;
                queue.next_ticket += 1;
                *admission = Some(id);
                queue.admitted.push_back(Admitted { at: now, id });
                let given_up = ticket.take();
                // A named waiter keeps a place until it sends: the one it gave
                // up, or, admitted on its first poll, the admission's own number,
                // which is behind every waiter queued now and ahead of every
                // later one.
                if let Some(slot) = held_place {
                    *slot = Some(given_up.unwrap_or(id));
                }
                let next = given_up.and_then(|t| {
                    queue.wakers.remove(&t);
                    queue.places.remove(&t);
                    queue.wakers.pop_first().map(|(_, waker)| waker)
                });
                queue.sync(&self.waiting);
                (next, Ok(()))
            }
        };
        if let Some(waker) = next {
            waker.wake();
        }
        admitted
    }

    /// Withdraw the admission numbered `id` (its waiter was dropped before
    /// its take), if it has not lapsed.
    fn withdraw(&self, id: u64) {
        let next = {
            let mut queue = self.inner.lock();
            let Some(i) = queue.admission_of(Some(id)) else {
                return;
            };
            queue.admitted.remove(i);
            queue.sync(&self.waiting);
            // The next waiter's timer counted on this admission's datagram.
            queue.wakers.pop_first().map(|(_, waker)| waker)
        };
        if let Some(waker) = next {
            waker.wake();
        }
    }

    /// How many admissions are waiting for their take, lapsed ones included
    /// until the next poll or take drops them.
    #[cfg(test)]
    fn outstanding(&self) -> usize {
        self.inner.lock().admitted.len()
    }

    /// Remove a waiter that is gone without taking its slot (its connection
    /// closed) and wake the next [`MAX_FOLLOWERS_WOKEN_PER_LEAVE`] waiters
    /// holding wakers behind it, so each re-polls and moves its staggered
    /// timer to its new place. Bounded so connections closing one by one in
    /// a long queue cost a constant number of wakes each, not one per
    /// follower.
    ///
    /// Residual: a waiter further back keeps its old deadline, at most one
    /// datagram late per drop ahead of it since it last polled; it fixes
    /// itself the next time it polls (its timer firing, or a give-back). It
    /// is never woken early. The wakers are woken after the lock is released.
    fn leave(&self, ticket: u64) {
        let followers: Vec<std::task::Waker> = {
            let mut queue = self.inner.lock();
            queue.wakers.remove(&ticket);
            if !queue.places.remove(&ticket) {
                return;
            }
            queue.sync(&self.waiting);
            queue
                .wakers
                .range(ticket..)
                .take(MAX_FOLLOWERS_WOKEN_PER_LEAVE)
                .map(|(_, waker)| waker.clone())
                .collect()
        };
        for waker in followers {
            waker.wake();
        }
    }

    /// Take up to `n` wakers, oldest holder first. The woken keep their
    /// places.
    fn take_oldest_wakers(&self, n: usize) -> Vec<std::task::Waker> {
        let mut queue = self.inner.lock();
        let mut woken = Vec::with_capacity(n.min(queue.wakers.len()));
        while woken.len() < n {
            let Some((_, waker)) = queue.wakers.pop_first() else {
                break;
            };
            woken.push(waker);
        }
        woken
    }
}

/// The wakeup a sender polling [`EgressCap::poll_admit`] keeps between
/// polls: one timer on the cap's time source and the time it fires at, and
/// its place in the cap's queue while it waits (see
/// [`EgressCap::poll_admit`]: bytes given back wake it before the timer).
/// Used by the QUIC endpoint (#5940) and by each paced UDP stream.
#[derive(Default)]
pub(crate) struct AdmissionWaiter {
    ticket: Option<u64>,
    /// Keeps its place until its take ([`Self::named`]).
    named: bool,
    /// The number of this waiter's admission while its take has not
    /// happened yet. Only a take naming it uses it.
    admission: Option<u64>,
    /// The place (ticket) a named waiter gave up when it was admitted, until
    /// a take of its succeeds. If it polls again first (its take failed, or
    /// its congestion window held it past [`ADMISSION_PASS`]) it queues there
    /// again, ahead of everyone who queued after it, instead of at the back.
    held_place: Option<u64>,
    timer: Option<(u64, SleepFuture)>,
    /// The queue of the cap this waiter last registered with, so dropping
    /// the waiter removes its entry: a closed connection's entry must not
    /// absorb a wake meant for a live one.
    queue: Option<std::sync::Weak<AdmissionQueue>>,
}

impl Drop for AdmissionWaiter {
    fn drop(&mut self) {
        let Some(queue) = self.queue.take().and_then(|queue| queue.upgrade()) else {
            return;
        };
        if let Some(ticket) = self.ticket.take() {
            queue.leave(ticket);
        }
        if let Some(id) = self.admission.take() {
            queue.withdraw(id);
        }
    }
}

impl AdmissionWaiter {
    /// A waiter that takes with [`EgressCap::try_take_admitted`], which
    /// clears its held place, and so keeps its place until it sends. For a
    /// sender that can await between its admission and its take (a paced UDP
    /// stream re-checks its congestion window). Every waiter's admission is
    /// its own: a take that does not name it leaves its datagram in the
    /// bucket.
    pub(crate) fn named() -> Self {
        let mut waiter = Self::default();
        waiter.named = true;
        waiter
    }

    /// The number of this waiter's admission, if it was admitted and has
    /// not polled again since. Handed to the take that uses it
    /// ([`EgressCap::try_take_handed`]).
    pub(crate) fn admission(&self) -> Option<u64> {
        self.admission
    }

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
    /// Taken without admission (a retransmit), so recorded as an unpaced
    /// send, as retransmits were before they were capped.
    unpaced: bool,
    /// The retransmit share this take holds (ns on the share's meter), handed
    /// back if the datagram is not sent; 0 for a paced take.
    retx_share_ns: i64,
}

impl EgressCap {
    /// A cap of `rate_bytes_per_sec`, starting with a full bucket.
    pub(crate) fn new<T: TimeSource>(rate_bytes_per_sec: u64, time_source: T) -> Self {
        // A rate of 0 cannot be met by anything, and the node never builds a
        // cap for it (no pool is built for a total of 0). Treat it as 1 B/s
        // rather than dividing by zero.
        let rate = rate_bytes_per_sec.max(1);
        let burst_bytes = (rate as u128 * BURST_WINDOW.as_nanos() / NANOS_PER_SEC) as u64;
        let burst_bytes = burst_bytes.max(MIN_BURST_PACKETS * MAX_PACKET_SIZE as u64);
        let burst_ns = Self::nanos_for(rate, burst_bytes);
        let debt_limit_ns = i64::try_from(DEBT_LIMIT.as_nanos()).unwrap_or(i64::MAX);
        let sleeper = time_source.clone();
        let sleep_until = Box::new(move |deadline_ns: u64| {
            sleeper.sleep_cancelable(Duration::from_nanos(
                deadline_ns.saturating_sub(sleeper.now_nanos()),
            ))
        });
        let clock = Box::new(move || time_source.now_nanos());
        let now = i64::try_from(clock()).unwrap_or(i64::MAX);
        Self {
            rate_bytes_per_sec: rate,
            burst_ns,
            debt_limit_ns,
            clock,
            sleep_until,
            empty_at_ns: AtomicI64::new(now.saturating_sub(burst_ns)),
            // Long empty: the share starts full, whatever its window.
            retx_meter_empty_at_ns: AtomicI64::new(i64::MIN / 2),
            retx_bucket_next_ns: AtomicI64::new(i64::MIN / 2),
            admission: Arc::new(AdmissionQueue::default()),
            last_long_wait_warn_ns: AtomicI64::new(i64::MIN),
            last_abandon_warn_ns: AtomicI64::new(i64::MIN),
            long_waits_since_warn: WarnWindow::default(),
            abandoned_since_warn: WarnWindow::default(),
            last_refuse_warn_ns: AtomicI64::new(i64::MIN),
            refused_since_warn: WarnWindow::default(),
            #[cfg(test)]
            stats: StatCounters::default(),
            #[cfg(test)]
            retransmits_uncapped: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The cap for a configured `total-bandwidth-limit`, or `None` for 0,
    /// which cannot be a cap. The node never passes 0 (it builds no pool for
    /// it, and warns at startup, `P2pConnManager::build`), so this is only a
    /// guard.
    pub(crate) fn from_total_limit<T: TimeSource>(
        total_bytes_per_sec: usize,
        time_source: T,
    ) -> Option<Self> {
        if total_bytes_per_sec == 0 {
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

    /// The admission predicate shared by [`Self::try_take_handed`] and
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

    /// Whether retransmits must take from this cap
    /// ([`Self::admit_retransmit`]). Always, outside tests.
    pub(crate) fn caps_retransmits(&self) -> bool {
        #[cfg(test)]
        {
            !self.retransmits_uncapped.load(Ordering::Relaxed)
        }
        #[cfg(not(test))]
        {
            true
        }
    }

    /// Tests only: send retransmits uncapped, as before #6100's cap.
    #[cfg(test)]
    pub(crate) fn uncap_retransmits(&self) {
        self.retransmits_uncapped.store(true, Ordering::Relaxed);
    }

    /// Tests only: [`Self::take_retx`] with nothing booked, the refusal as a
    /// wait.
    #[cfg(test)]
    pub(crate) fn try_take_retransmit(&self, bytes: usize) -> Result<EgressTake<'_>, Duration> {
        self.take_retx(bytes, None).map_err(RetxRefused::wait)
    }

    /// The share's cost of a `bytes` retransmit and the share's window.
    fn retx_share_cost(&self, bytes: usize) -> (i64, i64) {
        let meter_cost = self.cost_ns(bytes as u64).saturating_mul(100)
            / i64::try_from(retx_share_percent()).unwrap_or(100);
        let window = i64::try_from(RETX_SHARE_WINDOW.as_nanos())
            .unwrap_or(i64::MAX)
            .max(meter_cost);
        (meter_cost, window)
    }

    /// Book the next free slot in the share for a `bytes` retransmit: pay
    /// for it now, whenever it comes ([`RetxDeferral`]).
    fn book_retx_share(&self, bytes: usize) -> RetxBooking {
        let (meter_cost, window) = self.retx_share_cost(bytes);
        let now = self.now();
        let earliest = now.saturating_sub(window);
        let mut meter = self.retx_meter_empty_at_ns.load(Ordering::Acquire);
        loop {
            let slot = meter.max(earliest).saturating_add(meter_cost);
            match self.retx_meter_empty_at_ns.compare_exchange_weak(
                meter,
                slot,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return RetxBooking {
                        share_ns: meter_cost,
                        slot_ns: slot,
                    };
                }
                Err(current) => meter = current,
            }
        }
    }

    /// How long a retransmit the bucket refused with `wait` should wait: to
    /// the next free slot after `wait` among the retransmits the bucket
    /// refused, [`RETX_BUCKET_SPACING_DIV`] slots per datagram of refill, at
    /// most half the deferral limit (`rto`, [`RETX_DEFER_LIMIT_RTOS`]) after
    /// `wait`. A
    /// timing hint only: nothing is paid, so nothing can leak.
    fn space_retx_bucket_wait(&self, bytes: usize, wait: Duration, rto: Duration) -> Duration {
        // In tests `FREENET_RETX_BUCKET_SPACING_DIV` overrides it, to sweep.
        #[cfg(test)]
        let div = std::env::var("FREENET_RETX_BUCKET_SPACING_DIV")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .map_or(RETX_BUCKET_SPACING_DIV, |d| d.max(1));
        #[cfg(not(test))]
        let div = RETX_BUCKET_SPACING_DIV;
        let cost = self.cost_ns(bytes as u64) / div;
        let now = self.now();
        let at = now.saturating_add(i64::try_from(wait.as_nanos()).unwrap_or(i64::MAX));
        // The lead is bounded: slots more than half the deferral limit past
        // `at` start again from `at`. Slots are never handed back (a
        // connection may go uncapped or end its run before its slot), so
        // without this enough lossy connections would push the next slot
        // past the deferral limit and every bucket-refused resend would wait
        // a full RTO and then go uncapped.
        let bound = i64::try_from((rto.saturating_mul(RETX_DEFER_LIMIT_RTOS) / 2).as_nanos())
            .unwrap_or(i64::MAX);
        let mut next = self.retx_bucket_next_ns.load(Ordering::Acquire);
        loop {
            let slot = if next > at.saturating_add(bound) {
                at
            } else {
                next.max(at)
            };
            match self.retx_bucket_next_ns.compare_exchange_weak(
                next,
                slot.saturating_add(cost),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Duration::from_nanos((slot - now).max(0) as u64),
                Err(current) => next = current,
            }
        }
    }

    /// Take `bytes` for a legacy UDP retransmit (an RTO resend or a tail-loss
    /// probe) if retransmits stay within their share of the rate
    /// ([`RETX_SHARE_PERCENT`] over [`RETX_SHARE_WINDOW`]) and the bucket's
    /// debt after the take stays within [`RETX_DEBT_LIMIT`]; otherwise take
    /// nothing and return how long until it would fit. Unlike
    /// [`Self::charge_unpaced`] nothing is forgiven; unlike a paced take it
    /// needs no admission and may go into debt, so a lost packet is repaired
    /// ahead of first copies waiting in the admission queue.
    ///
    /// The share is checked first, so a share refusal touches the bucket not
    /// at all, and the bucket refusal path only hands the share back: both
    /// are atomic updates with no lock and no await, and neither wakes paced
    /// waiters. Send the datagram with `Socket::send_to_paced`, which charges
    /// it once; a take dropped unsent gives back both the bucket and the
    /// share. With `prepaid` (a booking whose slot has come) the share is
    /// already paid, and a bucket refusal keeps it for the retry.
    fn take_retx(&self, bytes: usize, prepaid: Option<i64>) -> Result<EgressTake<'_>, RetxRefused> {
        let cost = self.cost_ns(bytes as u64);
        // The retransmit share: a leaky bucket refilled at the share of the
        // rate, so this datagram costs `100 / share` times its cost at the
        // full rate. It holds RETX_SHARE_WINDOW, or one datagram if that is
        // more, so a cap too small for a datagram in a window of its share
        // (under about 2400 B/s) still admits retransmits at its share.
        let (meter_cost, window) = match prepaid {
            Some(paid) => (paid, 0),
            None => self.retx_share_cost(bytes),
        };
        let now = self.now();
        let mut meter = self.retx_meter_empty_at_ns.load(Ordering::Acquire);
        while prepaid.is_none() {
            let after = meter
                .max(now.saturating_sub(window))
                .saturating_add(meter_cost);
            if after > now {
                return Err(RetxRefused::Share(Duration::from_nanos(
                    (after - now) as u64,
                )));
            }
            match self.retx_meter_empty_at_ns.compare_exchange_weak(
                meter,
                after,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => meter = current,
            }
        }
        let limit = i64::try_from(retx_debt_limit().as_nanos()).unwrap_or(i64::MAX);
        let fits = |empty_at: i64, now: i64| {
            let after = self.refilled(empty_at, now).saturating_add(cost);
            let over = after.saturating_sub(now.saturating_add(limit));
            if over <= 0 { Ok(after) } else { Err(over) }
        };
        let (old, new, now) = self.update(|empty_at, now| fits(empty_at, now).ok());
        if let Err(over) = fits(old, now) {
            // A booking is kept for the retry; a share just taken is handed back.
            if prepaid.is_none() {
                self.refund_retx_share(meter_cost);
            }
            return Err(RetxRefused::Bucket(Duration::from_nanos(over as u64)));
        }
        let debt = self.bytes_for(new.saturating_sub(now));
        if debt > 0 {
            #[cfg(test)]
            self.stats.debt_hwm_bytes.fetch_max(debt, Ordering::Relaxed);
            TRANSPORT_METRICS.record_egress_debt(debt);
        }
        Ok(EgressTake {
            cap: self,
            taken_bytes: bytes as u64,
            waited: Duration::ZERO,
            unpaced: true,
            retx_share_ns: meter_cost,
        })
    }

    /// Decide one legacy UDP retransmit under this cap: `kind` is an RTO
    /// resend or a tail-loss probe of `bytes`, `deferred_for` how long the
    /// connection's resends have been refused in a row ([`RetxDeferral`],
    /// zero at the first refusal), `rto` the connection's effective RTO.
    ///
    /// - The cap takes it ([`Self::take_retx`]): [`RetxAdmit::Take`].
    /// - A refused probe is skipped: [`RetxAdmit::SkipProbe`].
    /// - A refused RTO resend is deferred ([`RetxAdmit::Defer`]) for at least
    ///   [`RETX_DEFER_MIN`] and at most until the connection's resends have
    ///   been refused for one effective RTO ([`RETX_DEFER_LIMIT_RTOS`]);
    ///   refused then, it goes
    ///   uncapped ([`RetxAdmit::Uncapped`]), charged as an unpaced send as
    ///   before the cap. So a resend is never deferred without bound: under
    ///   unpaced debt above [`RETX_DEBT_LIMIT`] (short messages, receipts and
    ///   keepalives at or above the rate) the cap could otherwise refuse it
    ///   for as long as that lasts, and since the retransmit count moves only
    ///   when a resend goes, the packet would never be abandoned and its
    ///   flight never released (#4345). It also bounds how long one
    ///   connection's resends can be held behind another's use of the
    ///   node-wide share.
    pub(in crate::transport) fn admit_retransmit(
        &self,
        kind: ResendKind,
        bytes: usize,
        deferred_for: Duration,
        rto: Duration,
        booking: &mut Option<RetxBooking>,
    ) -> RetxAdmit<'_> {
        let booked = booking.filter(|_| kind == ResendKind::Rto);
        let attempt = match booked {
            Some(b) if self.now() < b.slot_ns => Err(RetxRefused::Slot(Duration::from_nanos(
                (b.slot_ns - self.now()).max(0) as u64,
            ))),
            Some(b) => self.take_retx(bytes, Some(b.share_ns)),
            None => self.take_retx(bytes, None),
        };
        let refused = match attempt {
            Ok(take) => {
                if booked.is_some() {
                    // The take now carries the booked share (refunded if
                    // it is dropped unsent).
                    *booking = None;
                }
                return RetxAdmit::Take(take);
            }
            Err(refused) => refused,
        };
        match kind {
            ResendKind::Probe => {
                TRANSPORT_METRICS.record_egress_probe_skipped();
                RetxAdmit::SkipProbe
            }
            ResendKind::Rto => {
                let limit = rto.saturating_mul(RETX_DEFER_LIMIT_RTOS);
                if deferred_for >= limit {
                    if let Some(b) = booking.take() {
                        self.refund_retx_share(b.share_ns);
                    }
                    TRANSPORT_METRICS.record_egress_retransmit_uncapped(bytes as u64);
                    return RetxAdmit::Uncapped;
                }
                TRANSPORT_METRICS.record_egress_retransmit_deferred();
                // The connections waiting wake one at a time, not all on the
                // same deficit: a share refusal books the next slot in the
                // share; a bucket refusal (any booking kept) waits for the
                // next free datagram time on the bucket.
                let wait = match refused {
                    RetxRefused::Slot(wait) => wait,
                    RetxRefused::Share(_) => {
                        let b = self.book_retx_share(bytes);
                        *booking = Some(b);
                        Duration::from_nanos((b.slot_ns - self.now()).max(0) as u64)
                    }
                    RetxRefused::Bucket(wait) => self.space_retx_bucket_wait(bytes, wait, rto),
                };
                RetxAdmit::Defer(
                    wait.max(retx_defer_min(rto))
                        .min(limit.saturating_sub(deferred_for)),
                )
            }
        }
    }

    /// Hand back a retransmit's share ([`Self::take_retx`], or a
    /// booking) when
    /// its datagram was not sent.
    fn refund_retx_share(&self, meter_cost: i64) {
        if meter_cost > 0 {
            self.retx_meter_empty_at_ns
                .fetch_sub(meter_cost, Ordering::AcqRel);
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
    /// burst holds at least four. It also needs one datagram for every other
    /// admission outstanding, which may be more than the burst holds, but an
    /// admission lasts at most [`ADMISSION_PASS`], so the take still succeeds
    /// eventually.
    ///
    /// The bucket must also keep one [`WAKE_DATAGRAM_BYTES`] for every
    /// admission whose take has not happened yet: this take uses none of
    /// them. See [`Self::try_take_handed`] and [`Self::try_take_admitted`]
    /// for a sender that was admitted. Production takes all go through one
    /// of those two, so this is for tests.
    #[cfg(test)]
    pub(crate) fn try_take(
        &self,
        bytes: usize,
        waited: Duration,
    ) -> Result<EgressTake<'_>, Duration> {
        self.try_take_handed(bytes, waited, None)
    }

    /// A take using the admission numbered `admission`
    /// ([`AdmissionWaiter::admission`]), if it is still outstanding: the
    /// bucket keeps a datagram for every other admission, not for that one,
    /// which the take removes. The QUIC socket's take, handed the number by
    /// the poller admitted just before it on the same thread
    /// (`quic::socket`'s handoff). `None`, or a number that lapsed, is a take
    /// with no admission.
    pub(crate) fn try_take_handed(
        &self,
        bytes: usize,
        waited: Duration,
        admission: Option<u64>,
    ) -> Result<EgressTake<'_>, Duration> {
        self.take_in_turn(bytes, waited, admission)
            .map(|(take, _)| take)
    }

    /// [`Self::try_take_handed`] for `waiter`, which takes by name
    /// ([`AdmissionWaiter::named`]): the take uses the waiter's own
    /// admission, if it has one that has not lapsed, and leaves the datagrams
    /// owed to every other admission. Without one (it lapsed, or the waiter
    /// was never admitted) it needs those datagrams as well as its own.
    pub(crate) fn try_take_admitted(
        &self,
        bytes: usize,
        waited: Duration,
        waiter: &mut AdmissionWaiter,
    ) -> Result<EgressTake<'_>, Duration> {
        let (take, used) = self.take_in_turn(bytes, waited, waiter.admission)?;
        if used {
            waiter.admission = None;
        }
        waiter.held_place = None;
        Ok(take)
    }

    /// A take using the admission numbered `admission`, if outstanding.
    /// Under the queue lock, so an admission cannot be
    /// made between the count of what is owed and the take (it would count
    /// the datagram this take is about to use). `Ok` says whether the take
    /// used that admission.
    ///
    /// The lock is taken on every paced take, also with nobody waiting, where
    /// the take was a lock-free compare-and-swap before. Measured in a release
    /// build, uncontended: 14 ns for the lock in a 108 ns take, beside a UDP
    /// send of microseconds. Only nodes with `total-bandwidth-limit` take it,
    /// and each paced sender already takes the same lock to be admitted. A
    /// fast path that skipped it while nobody waits would let an admission be
    /// made between this take's check and its swap, which is the race the lock
    /// is here to close.
    fn take_in_turn(
        &self,
        bytes: usize,
        waited: Duration,
        admission: Option<u64>,
    ) -> Result<(EgressTake<'_>, bool), Duration> {
        let cost = self.cost_ns(bytes as u64);
        let mut queue = self.admission.inner.lock();
        queue.lapse(self.now());
        let own = queue.admission_of(admission);
        let others = queue.admitted.len() - usize::from(own.is_some());
        let need = cost.saturating_add(
            i64::try_from(others)
                .unwrap_or(i64::MAX)
                .saturating_mul(self.cost_ns(WAKE_DATAGRAM_BYTES as u64)),
        );
        // Checks for `need` and takes `cost`: the datagrams owed to the
        // other admissions stay in the bucket.
        let (old, _, now) = self.update(|empty_at, now| {
            self.admits(empty_at, now, need)
                .ok()
                .map(|_| self.refilled(empty_at, now).saturating_add(cost))
        });
        // The same predicate on the value the last attempt saw.
        if let Err(deficit) = self.admits(old, now, need) {
            queue.sync(&self.admission.waiting);
            drop(queue);
            #[cfg(test)]
            self.stats.refusals.fetch_add(1, Ordering::Relaxed);
            return Err(Duration::from_nanos(deficit as u64));
        }
        if let Some(i) = own {
            queue.admitted.remove(i);
        }
        queue.sync(&self.admission.waiting);
        drop(queue);
        Ok((
            EgressTake {
                cap: self,
                taken_bytes: bytes as u64,
                waited,
                unpaced: false,
                retx_share_ns: 0,
            },
            own.is_some(),
        ))
    }

    /// Poll form of the admission check, for a sender that is polled rather
    /// than one that can sleep and retry (a QUIC endpoint's
    /// `UdpPoller::poll_writable`, #5940). `Ready` when the bucket holds
    /// `bytes` plus one [`WAKE_DATAGRAM_BYTES`] for every waiter queued ahead
    /// of `waiter` and every admission whose take has not happened yet (so
    /// the take of `bytes` that follows would succeed at the same instant,
    /// and no waiter ahead loses its turn), and then records the admission
    /// (see `QueuedWaiters::admitted`); otherwise `Pending`,
    /// with `waiter` holding a timer on the cap's time source that wakes `cx`
    /// when the rate has made up the deficit. Takes
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
    /// across polls. An admitted waiter takes the datagram owed to it, so
    /// nobody behind it moves. A waiter dropped while queued
    /// (its connection closed) wakes the next
    /// [`MAX_FOLLOWERS_WOKEN_PER_LEAVE`] waiters behind it, which re-poll and
    /// move their timers to their new places; waiters further back stay up
    /// to one datagram late per such drop until they next poll. Past
    /// [`MAX_ADMISSION_WAKERS`] queued waiters, the newer ones hold no waker
    /// and are not woken by give-backs; they keep their places and their
    /// staggered timers. No waiter is woken before the bucket can admit it.
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
        // What the waiters ahead are owed: one datagram each, clamped so
        // `quic::socket::cap_hold_validity` bounds the time to a QUIC
        // transmit's next poll however long the queue is.
        let owed = |ahead: usize| {
            i64::try_from(ahead.min(MAX_ADMISSION_WAKERS))
                .unwrap_or(i64::MAX)
                .saturating_mul(self.cost_ns(WAKE_DATAGRAM_BYTES as u64))
        };
        loop {
            // Admitted only when the bucket also holds a datagram for every
            // waiter ahead and every admission not yet taken, so a waiter
            // that polls first, on any thread, cannot take a datagram that
            // is another's turn. Nothing is set aside: a waiter ahead that
            // has not polled yet takes its datagram when it does, from the
            // same level. A waiter with more than `MAX_ADMISSION_WAKERS`
            // ahead is never admitted, whatever the level: the clamp in
            // `owed` is for its timer only.
            let held_place = waiter.named.then_some(&mut waiter.held_place);
            let refused = self.admission.try_admit(
                &mut waiter.ticket,
                &mut waiter.admission,
                held_place,
                || self.now(),
                |ahead, now| {
                    ahead <= MAX_ADMISSION_WAKERS
                        && self
                            .admits(
                                self.empty_at_ns.load(Ordering::Acquire),
                                now,
                                cost.saturating_add(owed(ahead)),
                            )
                            .is_ok()
                },
            );
            let Err((ahead, now)) = refused else {
                waiter.timer = None;
                // So dropping the waiter withdraws its admission.
                waiter.queue = Some(Arc::downgrade(&self.admission));
                return Poll::Ready(());
            };
            let need = cost.saturating_add(owed(ahead));
            let deficit = self
                .admits(self.empty_at_ns.load(Ordering::Acquire), now, need)
                .err()
                // Too far back: its timer is clamped, so it is early.
                .unwrap_or_else(|| self.cost_ns(WAKE_DATAGRAM_BYTES as u64));
            #[cfg(test)]
            self.stats.refusals.fetch_add(1, Ordering::Relaxed);
            let (ahead, lapses_at) = self.admission.register(&mut waiter.ticket, cx.waker());
            waiter.queue = Some(Arc::downgrade(&self.admission));
            let need = cost.saturating_add(owed(ahead));
            // Bytes given back between the check above and the registration
            // woke nobody; check once more so they are not missed.
            if ahead <= MAX_ADMISSION_WAKERS
                && self
                    .admits(self.empty_at_ns.load(Ordering::Acquire), self.now(), need)
                    .is_ok()
            {
                continue;
            }
            // When the bucket will hold this waiter's datagram and one for
            // each waiter ahead: the stagger, from the level now.
            let deficit = deficit.max(
                self.admits(self.empty_at_ns.load(Ordering::Acquire), now, need)
                    .err()
                    .unwrap_or(0),
            );
            // No later than the oldest outstanding admission lapses: the deficit
            // counted its datagram, and nothing else wakes this waiter then.
            let wake_at = now
                .saturating_add(deficit)
                .min(lapses_at.unwrap_or(i64::MAX));
            let wake_at = u64::try_from(wake_at).unwrap_or(u64::MAX);
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
        for waker in self.admission.take_oldest_wakers(datagrams) {
            waker.wake();
        }
    }

    /// How many `Pending` [`Self::poll_admit`] wakers the cap holds.
    #[cfg(test)]
    pub(crate) fn admission_waker_count(&self) -> usize {
        self.admission.inner.lock().wakers.len()
    }

    /// How many [`Self::poll_admit`] callers are queued, with or without a
    /// held waker.
    #[cfg(test)]
    pub(crate) fn admission_queue_len(&self) -> usize {
        self.admission.inner.lock().places.len()
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

    /// How many admissions are waiting for their take.
    #[cfg(test)]
    pub(crate) fn admissions_outstanding(&self) -> usize {
        self.admission.outstanding()
    }

    /// Whether the admission numbered `id` is still waiting for its take.
    #[cfg(test)]
    pub(crate) fn admission_outstanding(&self, id: u64) -> bool {
        self.admission.inner.lock().admission_of(Some(id)).is_some()
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
        if sent == 0 {
            this.cap.refund_retx_share(this.retx_share_ns);
        }
        let decision = if this.unpaced {
            EgressDecision::Unpaced
        } else if this.waited.is_zero() {
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
        self.cap.refund_retx_share(self.retx_share_ns);
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

/// One attempt to admit a paced stream fragment through `socket`, using the
/// admission `pacing`'s waiter holds, if any ([`EgressCap::try_take_admitted`]).
///
/// `Ok(None)`: the socket has no cap, send. `Ok(Some(take))`: the bytes were
/// taken, send at once with [`Socket::send_to_paced`] (no await before it
/// but the send itself). `Err(deficit)`: the bucket did not hold the
/// datagram (and those owed to other admissions); wait again with
/// [`StreamPacing::wait_for_admission`].
pub(crate) fn try_take<'s, S: Socket>(
    socket: &'s S,
    pacing: &mut StreamPacing,
) -> Result<Option<EgressTake<'s>>, Duration> {
    let Some(cap) = socket.egress_cap() else {
        return Ok(None);
    };
    pacing.take(cap, PACED_TAKE_BYTES).map(Some)
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
    /// The current fragment's place in the node-wide cap's admission queue,
    /// shared with the QUIC endpoint ([`Self::wait_for_admission`]). The
    /// place is given up when the fragment is admitted, so the next fragment
    /// queues at the back.
    waiter: AdmissionWaiter,
    /// When [`Self::wait_for_admission`] last returned [`Admission::Waited`]
    /// (ns on the caller's clock): the queue admitted this fragment, and its
    /// caller re-checks cwnd before taking. See [`ADMISSION_PASS`].
    admitted_at_ns: Option<u64>,
}

/// How long an admission stays good for the take: the stream that waited for
/// it calls again without queueing within it ([`StreamPacing::wait_for_admission`]),
/// and the queue holds the admitted datagram that long (`QueuedWaiters::admitted`).
/// One tokio timer tick: a stream whose window was open takes within it. One
/// held up longer by its window queues again, at the place it was admitted
/// from (`AdmissionWaiter::held_place`), so a cwnd wait costs it no turn of the
/// queue. On `VirtualTime` it is 1 ms of virtual time, so a simulated send that
/// does not yield between its admission and its take always takes within it.
pub(crate) const ADMISSION_PASS: Duration = Duration::from_millis(1);

/// How [`StreamPacing::wait_for_admission`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Admission {
    /// The cap could admit the datagram at once: take it now.
    Now,
    /// The datagram waited in the queue and the cap can now admit it.
    /// Check the congestion window again before taking, so the take and the
    /// send follow that check with no await between.
    Waited,
}

impl StreamPacing {
    pub(crate) fn new<T: TimeSource>(time_source: &T) -> Self {
        Self {
            gap_start_ns: time_source.now_nanos(),
            cap_slept: Duration::ZERO,
            connection_slept: Duration::ZERO,
            long_reported: false,
            waiter: AdmissionWaiter::named(),
            admitted_at_ns: None,
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

    /// Wait in the node-wide cap's admission queue
    /// ([`EgressCap::poll_admit`]), the one the QUIC endpoint waits in, until
    /// the cap can admit `bytes`. The wait takes nothing from the cap: the
    /// caller still takes with `try_take` once admitted. Its time is credited
    /// to the cap. The wait is cut at [`LONG_WAIT`] the first time and at the
    /// deadline, and [`Self::check`] runs after every
    /// cut, so a stream still fails at [`PACED_WAIT_DEADLINE`] (counted as
    /// abandoned) while it waits. One waiter per stream, so streams and QUIC
    /// connections are admitted in the order they queued (see "Node-wide
    /// egress cap" in `.claude/rules/transport.md` for why this replaced the
    /// race of jittered sleeps). After [`Admission::Waited`] the caller
    /// re-checks cwnd and calls again; that call takes the turn it was given
    /// without queueing, within [`ADMISSION_PASS`].
    pub(crate) async fn wait_for_admission<T: TimeSource>(
        &mut self,
        time_source: &T,
        cap: &EgressCap,
        bytes: usize,
    ) -> Result<Admission, Duration> {
        if let Some(at) = self.admitted_at_ns.take() {
            let since = time_source.now_nanos().saturating_sub(at);
            // The take that follows checks the bucket itself; if another
            // sender took the datagram meanwhile, the stream queues again at
            // its place.
            if since <= ADMISSION_PASS.as_nanos() as u64 {
                return Ok(Admission::Now);
            }
        }
        let mut waited = false;
        loop {
            let limit = self.sleep_limit(time_source);
            let before = time_source.now_nanos();
            // Built at the first `Pending`, so a fragment admitted at once
            // allocates no timer. Dropped when the cap admits first: a
            // cancelable sleep leaves nothing behind on a `VirtualTime`.
            let mut cut = None;
            let waiter = &mut self.waiter;
            let mut polled = false;
            let admitted = std::future::poll_fn(|cx| {
                if cap.poll_admit(bytes, waiter, cx).is_ready() {
                    return Poll::Ready(true);
                }
                polled = true;
                cut.get_or_insert_with(|| time_source.sleep_cancelable(limit))
                    .as_mut()
                    .poll(cx)
                    .map(|()| false)
            })
            .await;
            waited |= polled;
            self.cap_slept += Duration::from_nanos(time_source.now_nanos().saturating_sub(before));
            self.check(time_source, cap)?;
            if admitted {
                if !waited {
                    return Ok(Admission::Now);
                }
                self.admitted_at_ns = Some(time_source.now_nanos());
                return Ok(Admission::Waited);
            }
        }
    }

    /// Wait `wait`, the deficit `TokenBucket::reserve` returned for this
    /// fragment's payment on its connection's bucket, without jitter, so the
    /// connection's waiters wake in the order they reserved. The time is
    /// credited to the connection. The sleep is cut at [`LONG_WAIT`] and at
    /// the deadline as in [`Self::wait_for_admission`], then continues to the end of the
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

    /// Take `bytes` for the current fragment, using the admission this
    /// stream's waiter holds, if any ([`EgressCap::try_take_admitted`]).
    pub(crate) fn take<'c>(
        &mut self,
        cap: &'c EgressCap,
        bytes: usize,
    ) -> Result<EgressTake<'c>, Duration> {
        let waited = self.slept();
        cap.try_take_admitted(bytes, waited, &mut self.waiter)
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
