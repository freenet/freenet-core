//! When a QUIC path is closed so its peer falls back to the UDP session
//! (#5940, step M6).
//!
//! Two close codes recycle a path, each counted in `quic_close_sent_<label>`
//! ([`QuicCloseCode`]): too many locally reset sends (1, below), and
//! `PEER_NOT_ACKNOWLEDGING`, which the acknowledgement watch's rules (2-5,
//! below) all close with. Either code asks the peer to fall back to UDP for
//! THIS connection only. It is not a permanent
//! downgrade: e5 must let the pair upgrade again after its normal backoff.
//!
//! 1. **Too many locally reset sends** ([`close_code::SEND_ABORT_LIMIT`]).
//!    Each send this side resets leaves an entry in quinn's `stopped` map
//!    until the connection closes (see "Known quinn cost" in the `path`
//!    module doc), so the path counts the resets ([`SendAborts`]) and closes
//!    the connection once there are more than [`SEND_ABORT_LIMIT`]. A new
//!    connection starts with an empty map and a zero count. A stop the peer
//!    sent is not a reset of this side's and is not counted, and neither is
//!    a reset on a connection that is already closed. The count can still
//!    run high: a reset this side sends before it has processed a stop
//!    already received is counted, although quinn then reports the stream
//!    as Stopped and drops its entry. That only closes a path earlier.
//!
//! 2. **A peer that acknowledges nothing**
//!    ([`close_code::PEER_NOT_ACKNOWLEDGING`]). A peer that keeps sending
//!    but acknowledges none of our packets is not a conforming QUIC peer
//!    (RFC 9000 section 13.2.1 requires acknowledging ack-eliciting packets
//!    within `max_ack_delay`), so the path is closed. [`watch_acknowledgements`]
//!    samples the connection every [`ACK_WATCH_TICK`] on the receive task the
//!    path already has, and [`AckWatch`] closes the path when, for
//!    [`ACK_WATCH_WINDOW`]:
//!    - this side had ack-eliciting packets outstanding that the peer should
//!      have acknowledged by then (see [`AckWatch`] for when the window
//!      starts);
//!    - the peer acknowledged none of them; and
//!    - the peer kept sending: no two samples in a row of the window
//!      without anything from it.
//!
//!    It is a conformance check, not a liveness check. It targets a peer
//!    that sends ACK frames for none of this side's data; a peer that
//!    acknowledges something within each window is never closed by it.
//!
//! The same watch runs three liveness rules (#5940 PR 4), each also closing
//! with [`close_code::PEER_NOT_ACKNOWLEDGING`] and recorded as a [`Stall`]:
//!
//! 3. **Dark** ([`Stall::Dark`]): this side's packets outstanding at every
//!    sample for the window, nothing acknowledged and nothing heard from the
//!    peer, and two rounds of sends made after the window unanswered, the
//!    second for its grace (three round trips, from one tick up to
//!    [`DARK_GRACE_MAX`]). quinn sends again after an outage only at its
//!    next backed-off probe, so without those rounds a spell shorter than
//!    the window, or one lost probe round, could close the path ([`dark_close_bound`] gives the latest close). It closes
//!    only a path that carries data; every path counts its dark episodes.
//! 4. **Credit-starved** ([`Stall::CreditStarved`], [`CreditWatch`]): sends
//!    waited on the peer's flow-control credit, seen either with
//!    application-limited acknowledgements or with nothing outstanding, and
//!    the peer acknowledged under its floor rate ([`credit_floor_rate`]).
//! 5. **Stream credit withheld** ([`Stall::StreamCreditWithheld`],
//!    [`StreamCreditWatch`]): the peer granted no stream credit for
//!    [`STREAM_CREDIT_WINDOW`] while this side wanted streams, though enough
//!    of this side's streams were wholly acknowledged that an honest peer
//!    would have.
//!
//! What "acknowledged" is measured with. quinn's `ConnectionStats` has no
//! counter of packets the peer newly acknowledged: `frame_rx.acks` counts
//! ACK frames, including ones that acknowledge nothing new, so it cannot
//! tell a peer that acknowledges from one that only sends ACK frames. quinn
//! does tell the congestion controller about every ack-eliciting packet the
//! peer newly acknowledges (`Controller::on_ack`, once per packet) and, after
//! each batch of acknowledgements, the bytes still in flight
//! (`Controller::on_end_acks`); packets later declared lost are reported
//! through `on_congestion_event`. So the transport config wraps Cubic in
//! [`CappedCubic`], which records those and passes every call through,
//! except that it lowers the window to the configured bandwidth (below). The
//! other two inputs are from `ConnectionStats`:
//! - sent: the ack-eliciting frames this side sent (`frame_tx`, every frame
//!   kind except ACK and CONNECTION_CLOSE), [`ack_eliciting_frames`];
//! - the peer sent: every frame received from it (`frame_rx`), which counts
//!   only packets that were authenticated, [`frames_received`].
//!
//! What it does not fire on:
//! - an idle path: nothing ack-eliciting is outstanding (a keepalive PING is
//!   acknowledged within one round trip);
//! - a slow but conforming peer: its acknowledgements arrive within a round
//!   trip plus `max_ack_delay` (25 ms), and the window allows a round trip
//!   of several seconds;
//! - recovery from an outage in both directions: a sample with nothing from
//!   the peer ends the window, and after the outage a window starts only at
//!   a new send of this side's or an acknowledgement, which a conforming
//!   peer acknowledges in turn;
//! - the handshake: a path is built on an established connection, and the
//!   window starts no earlier than the path;
//! - a peer that went silent: the dark rule (3) is the one for that, and
//!   quinn's idle timeout ends a connection that nothing closed first;
//! - a send the node's egress cap (`total-bandwidth-limit`) holds: quinn
//!   counts a transmit's frames, and its bytes in flight, when it builds
//!   it, and the cap may hold it longer than the window.
//!
//! **Guard 2 is inactive while the egress cap holds this connection's
//! transmits.** While the cap holds one of them, or held one since the
//! previous sample, no window runs on this path. The holds are counted per
//! connection (`socket::with_poller_holds` gives quinn's poller for the
//! connection its own record), so a cap kept busy by other connections does
//! not switch guard 2 off here. Narrowing the
//! pause to the sends counted since the hold would not be enough, because a
//! held transmit is also counted in quinn's bytes in flight. A hold left
//! behind by a connection that ended stops counting after its validity
//! (`socket::cap_hold_validity`).
//!
//! It can also fire when this side's packets stop reaching a peer whose
//! packets still arrive (a path broken in one direction): from this side
//! that is the same observation, and falling back to UDP is the right
//! outcome there too. Only while the peer keeps sending, though: a quinn
//! peer that hears nothing from this side soon has its congestion window
//! full of unacknowledged data and then sends only probes, at doubling
//! intervals, so such a break usually ends the window (two samples without
//! anything from the peer) and is left to quinn's idle timeout.
//!
//! The peer's `max_ack_delay` transport parameter may be up to 2^14 ms
//! (RFC 9000 18.2), above [`ACK_WATCH_WINDOW`]; a peer that advertises that
//! and uses all of it would be closed. Every peer is a Freenet node, whose
//! quinn uses the default 25 ms (`config::transport_config` does not set
//! it), so the window is not derived from the peer's value.
//!
//! Network path changes: migration is off (`config::server_config`). Were a
//! connection to change paths anyway, quinn keeps the controller for a NAT
//! rebinding (it clones it, so the count is shared and continues) and builds
//! a new one otherwise, which [`AckWatch`] sees as a new counter and treats
//! like an acknowledgement. While quinn validates a new path it does not
//! report acknowledgements to the controller (`on_ack`), so a validation
//! lasting a window while the peer sends could close the path. That does not
//! happen while migration is off: quinn validates a path only when the
//! peer's address changes, and this node does not accept that.
//!
//! # The window cap
//!
//! `bandwidth-limit` and the fair share under `total-bandwidth-limit` pace
//! the UDP transport's streams through each connection's token bucket, which
//! QUIC does not use. So [`CappedCubic::window`] returns Cubic's window,
//! lowered to `rate x RTT` with a floor that admits two full datagrams, where
//! `rate` follows the UDP rule (`QuicRateLimit`, which reuses
//! `peer_connection::paced_rate`). quinn holds the bytes in flight under this
//! window, which is what keeps a path's rate to about `rate`; its pacer reads
//! the same window and only spreads the sends. With no limit configured, or
//! before the first acknowledgement, the window is Cubic's. The RTT is the
//! latest quinn reports to `on_ack`. quinn updates its estimate only after it
//! has called `on_ack` for every packet an ACK frame acknowledges, so each
//! `on_ack` sees the estimate from before that frame, and the first ones see
//! quinn's initial estimate (333 ms, `config::transport_config`). After the
//! handshake the stored RTT can still be that initial estimate when the first
//! data is sent (the e5b-9 cold-start lab saw 333 ms on an 800 ms path), so
//! the cap is loose, or tight, until the first data is acknowledged.
//!
//! # The initial window
//!
//! Cubic starts at [`INITIAL_WINDOW_DATAGRAMS`] datagrams of
//! [`MAX_UDP_PAYLOAD`] (38,400 bytes; a fixed byte count, not 32 x the
//! runtime MTU) instead of quinn's 10. That is above RFC 9002's recommended
//! 10 on purpose: quinn does not grow the window while the connection is
//! limited by the application, so a connection that only carries small
//! messages keeps its initial window for life, and on most connections the
//! initial window is the window.
//!
//! quinn paces it, but only partly. Its pacer starts with a full bucket of at
//! least 10 datagrams (`quinn-proto` `connection/pacing.rs`), so the first
//! 12,000 bytes leave at once, and the remaining 22 datagrams are spread at
//! 1.25 x window / RTT on quinn's RTT estimate, which says nothing about the
//! bottleneck's rate. quinn's own estimate is updated by the handshake's
//! acknowledgements; it is only the copy this controller keeps (`srtt`, see
//! "The window cap") that can still hold the 333 ms initial value then.
//!
//! While the cap binds, a send it holds back counts in quinn as blocked by
//! congestion, not limited by the application, so Cubic would keep growing on
//! every acknowledgement and stop answering loss once the cap later sat above
//! the real capacity. So `on_ack` tells Cubic the path is limited whenever the
//! cap is below Cubic's window (the idea of RFC 7661), and Cubic's window stays
//! where it was.
//!
//! The limit covers everything the path sends, short messages and control
//! frames included, where the UDP bucket paces only stream fragments.
//!
//! [`QuicCloseCode`]: crate::transport::metrics::QuicCloseCode

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use quinn::FrameStats;
use quinn::congestion::{Controller, ControllerFactory, ControllerMetrics};

use self::loss_gate::{GateVerdicts, LossGate};
use super::QuicCloser;
use super::config::{MAX_UDP_PAYLOAD, QuicRateLimit};
#[cfg(doc)]
use super::error::close_code;
use super::socket::{CapHoldReading, CapHolds};
use crate::simulation::TimeSource;
use crate::transport::metrics::QuicCloseCode;

/// Sends one path may reset before it is closed
/// ([`close_code::SEND_ABORT_LIMIT`]).
///
/// A healthy path resets a send only when the send is cancelled (a caller's
/// timeout), its chunk source fails, or a cancelled one-packet send finds
/// `path::ACK_WAITS_WITHOUT_PERMIT` sends already waiting for their
/// acknowledgement (a peer withholding acknowledgements), all rare, so 4096
/// is far above what one does over a long life, and a path that gets there
/// is closed and rebuilt with nothing lost but the upgrade. The `stopped` map entries the
/// limit bounds are measured at under `config::QUIC_STOPPED_ENTRY_BYTES`
/// (104 B) each, so at most 416 KiB per path, which the per-connection fixed
/// charge (`config::QUIC_CONNECTION_CHARGE_BYTES`) takes in full. How often
/// paths reset, and how close they come to the limit, is reported
/// (`quic_send_resets`, `quic_path_ms`, `quic_send_resets_reached_<n>`).
pub(crate) const SEND_ABORT_LIMIT: u64 = 4096;

/// How long the peer may go on sending without acknowledging packets this
/// side has outstanding, before the path is closed
/// ([`close_code::PEER_NOT_ACKNOWLEDGING`]).
///
/// A conforming peer acknowledges within a round trip plus its
/// `max_ack_delay` (25 ms by default, quinn's and ours), and a lost
/// acknowledgement is repeated in the next ACK frame or drawn out by a
/// probe after one PTO (about a round trip and a half at a steady RTT,
/// 1 s at quinn's initial estimate). 10 s is several PTOs even at a round
/// trip of 2 s, above geostationary satellite links (about 600 ms) and
/// heavily buffered access links (1 to 2 s), so a conforming peer reaches
/// it rarely: at a 2 s round trip, two probe rounds in a row lost in one
/// direction already come close. When one does, the cost is a fallback to
/// UDP for that connection only.
pub(crate) const ACK_WATCH_WINDOW: Duration = Duration::from_secs(10);

/// The most the dark-path rule waits for an answer to its post-window send
/// ([`AckWatch`]): its grace is three round trips (the round trip from
/// before the spell, since quinn's estimate does not move while nothing is
/// acknowledged), at least one [`ACK_WATCH_TICK`] and at most this, so an
/// inflated round trip cannot postpone a close indefinitely.
pub(crate) const DARK_GRACE_MAX: Duration = Duration::from_secs(10);

/// The latest the dark-path rule closes a path that stays dark, after the
/// outage begins, at round trip `rtt`. The spell's start is seen within a
/// tick, and a send counts only if the sample before it was already a window
/// past that start, so a send made after outage + window + two ticks
/// counts. quinn's probes back off by doubling, so the first one after a
/// time goes out by twice that time plus one probe timeout (at most
/// 3 x `rtt` + 25 ms, quinn's with its initial variance, half the round
/// trip; on a jittery path quinn's variance, and so the bound, can be
/// somewhat higher). The first post-window round is seen within a tick, the
/// second round must be seen at a later sample, so it goes out by twice that
/// plus a probe timeout and is seen within a tick, and the close comes after
/// its grace: 2 x (2 x (window + 2 ticks) + PTO + tick) + PTO + 2 ticks +
/// grace. About 67 s at a 50 ms round trip, 69 s at 300 ms and 74 s at 800 ms.
/// New sends leave a dark path much sooner, by suspect routing (`route`).
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn dark_close_bound(rtt: Duration) -> Duration {
    let pto = rtt * 3 + Duration::from_millis(25);
    let grace = (rtt * 3).clamp(ACK_WATCH_TICK, DARK_GRACE_MAX);
    let first = (ACK_WATCH_WINDOW + ACK_WATCH_TICK * 2) * 2 + pto + ACK_WATCH_TICK;
    first * 2 + pto + ACK_WATCH_TICK * 2 + grace
}

/// How often the acknowledgement watch samples the connection.
///
/// The window starts at the first sample that sees its starting event (a
/// send, or an acknowledgement that left packets outstanding), so it starts
/// at most one tick after that event; and the path is closed at the first
/// sample a full window after that start. So a close comes at least
/// [`ACK_WATCH_WINDOW`] after the event, and at most
/// `ACK_WATCH_WINDOW + ACK_WATCH_TICK` after it, given that the window was
/// not ended in between (see [`AckWatch`]).
pub(crate) const ACK_WATCH_TICK: Duration = Duration::from_secs(2);

/// The acknowledged rate a path whose sends wait on the peer's credit must
/// keep ([`CreditWatch`]). The receiver reads every byte as it arrives and
/// never lowers its window below the floor, so an honest peer returns credit
/// at least at floor window / round trip: 32 KiB/s at the 64 KiB small-host
/// floor up to a 2 s round trip.
pub(crate) const FLOOR_RATE_BYTES_PER_SEC: u64 = 32 * 1024;

/// The longest round trip the credit rule's floor allows for
/// ([`credit_floor_rate`]). The floor follows the round trip so a far honest
/// peer is not closed, but a peer controls how late it acknowledges, so the
/// round trip it can claim is capped: at 4 s the floor is 16 KiB/s. What
/// bounds a peer that acknowledges slowly on purpose is the stream's age
/// limit on its streams, and the ack-wait bound on ours (`bounded`).
pub(crate) const CREDIT_FLOOR_RTT_MAX: Duration = Duration::from_secs(4);

/// The acknowledged rate a credit-waiting path must keep at round trip
/// `rtt`: an honest receiver returns at least its smallest window
/// ([`super::budget::MIN_WINDOW_SMALL_HOST_BYTES`], 64 KiB) per round trip,
/// so the floor is that over the round trip (capped at
/// [`CREDIT_FLOOR_RTT_MAX`]), and at most [`FLOOR_RATE_BYTES_PER_SEC`]. An
/// unknown round trip (zero) takes the most.
pub(crate) fn credit_floor_rate(rtt: Duration) -> u64 {
    let rtt = rtt.min(CREDIT_FLOOR_RTT_MAX);
    if rtt.is_zero() {
        return FLOOR_RATE_BYTES_PER_SEC;
    }
    let per_rtt =
        u128::from(super::budget::MIN_WINDOW_SMALL_HOST_BYTES) * 1_000_000_000 / rtt.as_nanos();
    FLOOR_RATE_BYTES_PER_SEC.min(u64::try_from(per_rtt).unwrap_or(u64::MAX))
}

/// How long within [`ACK_WATCH_WINDOW`] a path must have waited on the peer's
/// credit before its acknowledged rate is judged ([`CreditWatch`]): two
/// sampling intervals, since waiting is counted in whole intervals.
pub(crate) const CREDIT_LIMITED_MIN: Duration = Duration::from_secs(2 * ACK_WATCH_TICK.as_secs());

/// Counts the sends a path reset, and closes its connection past the
/// limit. Shared by the path and every send in progress on it.
pub(crate) struct SendAborts {
    count: AtomicU64,
    limit: u64,
    closer: QuicCloser,
    node: &'static crate::transport::metrics::QuicDataCounters,
}

impl SendAborts {
    pub(crate) fn new(
        limit: u64,
        closer: QuicCloser,
        node: &'static crate::transport::metrics::QuicDataCounters,
    ) -> Self {
        Self {
            count: AtomicU64::new(0),
            limit,
            closer,
            node,
        }
    }

    /// One more send was reset on this side. Not counted once the
    /// connection is closed: closing frees quinn's map. Past the limit,
    /// close the connection (once; [`QuicCloser`] ignores later calls).
    pub(crate) fn record(&self) {
        if !self.closer.is_open() {
            return;
        }
        let count = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        self.node.record_send_reset(count);
        if count > self.limit && self.closer.close(QuicCloseCode::SendAbortLimit) {
            tracing::warn!(
                remote = %self.closer.remote_address(),
                aborts = count,
                limit = self.limit,
                "QUIC path: too many sends reset on this connection; closing it so \
                 the peer falls back to UDP"
            );
        }
    }

    /// Sends reset so far.
    #[cfg(test)]
    pub(crate) fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

/// Cubic's initial window in datagrams of [`MAX_UDP_PAYLOAD`] (quinn's default
/// is 10; RFC 9002 recommends 10), applied as `32 x 1200` bytes whatever the
/// connection's MTU. Measured in the e5b-9 cold-start lab against 10 and 64, with
/// pacing: a 64 KiB message on a fresh or application-limited connection at
/// 300 ms RTT arrives in 0.53 s instead of 0.82 s (UDP: 0.15 s), 16 KiB in one
/// round trip, and a burst of 10 such connections on a 5 Mbit/s uplink leaves
/// a competing loss-based flow less loss than the UDP transport does (2.2%
/// against 17%). 64 was faster again but cost a competing flow about what the
/// UDP transport does.
pub(crate) const INITIAL_WINDOW_DATAGRAMS: u64 = 32;

/// Builds [`CappedCubic`] controllers around Cubic, capped by `rate`.
#[derive(Debug)]
pub(crate) struct CappedCubicFactory {
    cubic: Arc<quinn::congestion::CubicConfig>,
    rate: QuicRateLimit,
}

impl CappedCubicFactory {
    pub(crate) fn new(rate: QuicRateLimit) -> Self {
        let mut cubic = quinn::congestion::CubicConfig::default();
        cubic.initial_window(INITIAL_WINDOW_DATAGRAMS * u64::from(MAX_UDP_PAYLOAD));
        Self {
            cubic: Arc::new(cubic),
            rate,
        }
    }
}

impl Default for CappedCubicFactory {
    fn default() -> Self {
        Self::new(QuicRateLimit::default())
    }
}

impl ControllerFactory for CappedCubicFactory {
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        let cubic = self.cubic.clone().build(now, current_mtu);
        #[cfg(test)]
        if self.rate.without_loss_gate {
            return Box::new(CappedCubic::new(cubic, self.rate.clone(), current_mtu));
        }
        Box::new(CappedCubic::new(
            Box::new(LossGate::new(cubic)),
            self.rate.clone(),
            current_mtu,
        ))
    }
}

/// The window [`CappedCubic`] allows at `rate` bytes per second over an RTT
/// of `srtt`: `rate x srtt`, and never less than [`window_floor`], so a slow
/// path still keeps two datagrams in flight.
pub(crate) fn rate_window(rate: u64, srtt: Duration, mtu: u16) -> u64 {
    let bytes = u128::from(rate) * srtt.as_nanos() / 1_000_000_000;
    u64::try_from(bytes)
        .unwrap_or(u64::MAX)
        .max(window_floor(mtu))
}

/// The smallest capped window: two full datagrams of `mtu` and one byte.
/// quinn refuses a send when `in_flight + bytes >= window`, so a window of
/// exactly two datagrams admits only one, and a lone packet then waits for
/// the peer's delayed acknowledgement.
pub(crate) fn window_floor(mtu: u16) -> u64 {
    2 * u64::from(mtu) + 1
}

/// What [`CappedCubic`] records, under one lock so a reader sees one
/// consistent state.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AckState {
    /// Ack-eliciting packets the peer newly acknowledged (`on_ack` calls).
    acked: u64,
    /// Their bytes (`on_ack`'s `bytes`): packet bytes, so headers and our own
    /// control frames are included, a few percent over the stream bytes.
    acked_bytes: u64,
    /// Batches of acknowledgements that came while quinn was not
    /// application-limited (`on_end_acks` with `app_limited` false): it had
    /// data to send that its congestion window held back.
    congestion_limited_batches: u64,
    /// Batches of acknowledgements (`on_end_acks` calls), counting those
    /// that acknowledged only acknowledgement-only packets, which have no
    /// `on_ack`.
    batches: u64,
    /// The bytes in flight after the latest batch of acknowledgements
    /// (`on_end_acks`).
    in_flight_after_acks: u64,
    /// Bytes declared lost since that batch (`on_congestion_event`).
    lost_since_acks: u64,
}

impl AckState {
    /// Bytes still outstanding of those in flight at the latest batch of
    /// acknowledgements. Packets sent after that batch are not included.
    fn outstanding_after_acks(&self) -> u64 {
        self.in_flight_after_acks
            .saturating_sub(self.lost_since_acks)
    }
}

/// The count of packets a connection's peer newly acknowledged, read without
/// waiting: `CappedCubic`'s record, which every clone of the controller shares.
#[derive(Clone)]
pub(crate) struct AckProgress(Arc<Mutex<AckState>>);

impl AckProgress {
    /// `None` if the connection's controller is not an [`CappedCubic`].
    pub(crate) fn of(connection: &quinn::Connection) -> Option<Self> {
        let record = connection
            .congestion_state()
            .into_any()
            .downcast::<CappedCubic>()
            .ok()?
            .state;
        Some(Self(record))
    }

    /// Ack-eliciting packets the peer has newly acknowledged so far.
    pub(crate) fn acked(&self) -> u64 {
        self.0.lock().acked
    }

    /// A record of no connection, moved by [`Self::ack`]. Tests only.
    #[cfg(test)]
    pub(crate) fn detached() -> Self {
        Self(Arc::default())
    }

    /// The peer newly acknowledged one more packet. Tests only.
    #[cfg(test)]
    pub(crate) fn ack(&self) {
        self.0.lock().acked += 1;
    }
}

/// A congestion controller that lowers `inner`'s window to the configured
/// bandwidth (see "The window cap" in the module docs), records the
/// acknowledgements the peer sends ([`AckState`]), and otherwise passes every
/// call to `inner` unchanged. Its clones (quinn's
/// `Connection::congestion_state`) share the record.
pub(crate) struct CappedCubic {
    inner: Box<dyn Controller>,
    state: Arc<Mutex<AckState>>,
    rate: QuicRateLimit,
    /// The latest smoothed RTT quinn reported (`on_ack`); `None` before the
    /// first.
    srtt: Option<Duration>,
    mtu: u16,
}

impl CappedCubic {
    pub(crate) fn new(inner: Box<dyn Controller>, rate: QuicRateLimit, mtu: u16) -> Self {
        Self {
            inner,
            state: Arc::default(),
            rate,
            srtt: None,
            mtu,
        }
    }

    /// `cubic` (Cubic's window) lowered to the rate's window, when there is a
    /// limit and an RTT; otherwise `cubic`. The one place the cap is applied,
    /// for [`Controller::window`] and [`Controller::metrics`].
    fn capped(&self, cubic: u64) -> u64 {
        match (self.rate.rate(), self.srtt) {
            (Some(rate), Some(srtt)) => cubic.min(rate_window(rate, srtt, self.mtu)),
            (None, _) | (_, None) => cubic,
        }
    }

    /// The loss gate's verdict counts, if the wrapped controller is a gate.
    fn gate_verdicts(&self) -> Option<Arc<GateVerdicts>> {
        self.inner
            .clone_box()
            .into_any()
            .downcast::<LossGate>()
            .ok()
            .map(|gate| gate.verdicts())
    }

    /// The loss gate inside, if the wrapped controller is one.
    #[cfg(test)]
    pub(crate) fn gate(&self) -> Option<Box<LossGate>> {
        self.inner
            .clone_box()
            .into_any()
            .downcast::<LossGate>()
            .ok()
    }
}

impl Controller for CappedCubic {
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
        self.inner.on_sent(now, bytes, last_packet_number);
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &quinn_proto::RttEstimator,
    ) {
        {
            let mut state = self.state.lock();
            state.acked += 1;
            state.acked_bytes = state.acked_bytes.saturating_add(bytes);
        }
        // quinn updates `rtt` only after this call for the ACK frame, so this
        // is the estimate from before it: the initial 333 ms on the first
        // frames, and possibly still when the first data goes out after the
        // handshake (see "The window cap" in the module docs). The cap reads
        // it; anything that sizes a window from the RTT before the first data
        // is acknowledged should use this packet's sample, `now - sent`.
        self.srtt = Some(rtt.get());
        // A send the cap holds back is not Cubic's doing: while the cap is
        // below Cubic's window, do not let Cubic grow (see the module docs).
        // Without a limit nothing more is asked of Cubic.
        let capped = match self.rate.rate() {
            Some(rate) => rate_window(rate, rtt.get(), self.mtu) < self.inner.window(),
            None => false,
        };
        self.inner
            .on_ack(now, sent, bytes, app_limited || capped, rtt);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        {
            let mut state = self.state.lock();
            state.batches += 1;
            if !app_limited {
                state.congestion_limited_batches += 1;
            }
            state.in_flight_after_acks = in_flight;
            state.lost_since_acks = 0;
        }
        self.inner
            .on_end_acks(now, in_flight, app_limited, largest_packet_num_acked);
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        {
            let mut state = self.state.lock();
            state.lost_since_acks = state.lost_since_acks.saturating_add(lost_bytes);
        }
        self.inner
            .on_congestion_event(now, sent, is_persistent_congestion, lost_bytes);
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.mtu = new_mtu;
        self.inner.on_mtu_update(new_mtu);
    }

    fn window(&self) -> u64 {
        self.capped(self.inner.window())
    }

    /// Cubic's metrics, with the window this controller gives quinn (the
    /// capped one) in place of Cubic's own.
    fn metrics(&self) -> ControllerMetrics {
        let mut metrics = self.inner.metrics();
        // Cubic's `congestion_window` is its window; lower it as `window`
        // does, without asking Cubic again.
        metrics.congestion_window = self.capped(metrics.congestion_window);
        metrics
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(Self {
            inner: self.inner.clone_box(),
            state: self.state.clone(),
            rate: self.rate.clone(),
            srtt: self.srtt,
            mtu: self.mtu,
        })
    }

    fn initial_window(&self) -> u64 {
        self.inner.initial_window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Ack-eliciting frames in `frames`: every kind except ACK and
/// CONNECTION_CLOSE (PADDING is not counted by quinn). A packet is
/// ack-eliciting exactly when it carries at least one of them, so this
/// grows whenever an ack-eliciting packet is sent. quinn-proto 0.11.19
/// never sends STREAMS_BLOCKED (it only parses the frame), so that count
/// stays 0 here; were a later quinn to send it uncounted, a packet carrying
/// only that frame would be missed, which can only delay a close.
pub(crate) fn ack_eliciting_frames(frames: &FrameStats) -> u64 {
    [
        frames.ack_frequency,
        frames.crypto,
        frames.data_blocked,
        frames.datagram,
        u64::from(frames.handshake_done),
        frames.immediate_ack,
        frames.max_data,
        frames.max_stream_data,
        frames.max_streams_bidi,
        frames.max_streams_uni,
        frames.new_connection_id,
        frames.new_token,
        frames.path_challenge,
        frames.path_response,
        frames.ping,
        frames.reset_stream,
        frames.retire_connection_id,
        frames.stream_data_blocked,
        frames.streams_blocked_bidi,
        frames.streams_blocked_uni,
        frames.stop_sending,
        frames.stream,
    ]
    .into_iter()
    .fold(0u64, u64::saturating_add)
}

/// Every frame in `frames`, of any kind.
pub(crate) fn frames_received(frames: &FrameStats) -> u64 {
    ack_eliciting_frames(frames)
        .saturating_add(frames.acks)
        .saturating_add(frames.connection_close)
}

/// One reading of a connection, for [`AckWatch`].
#[derive(Clone)]
pub(crate) struct AckSample {
    /// The controller's record, to notice a new controller.
    record: Arc<Mutex<AckState>>,
    /// The record's values when the sample was taken.
    state: AckState,
    /// Ack-eliciting frames this side sent.
    sent: u64,
    /// Frames received from the peer.
    received: u64,
    /// This connection's QUIC transmits the egress cap holds.
    cap: CapHoldReading,
    /// DATA_BLOCKED and STREAM_DATA_BLOCKED frames this side sent: quinn
    /// sends one when a write is blocked by the peer's flow-control limit,
    /// once per limit, and never for this side's own send window.
    blocked: u64,
    /// STREAM frames this side sent.
    stream_frames: u64,
    /// The path's sends with data still to write ([`Writers`]).
    writers: usize,
    /// quinn's smoothed round trip (`PathStats::rtt`).
    rtt: Duration,
    /// This side's sends waiting for the peer's stream credit
    /// ([`Writers::stream_waiters`]). quinn 0.11 sends no STREAMS_BLOCKED
    /// (its frame count only counts received ones), so the path counts its
    /// own waits.
    stream_waiters: usize,
    /// This side's data streams ended, in any way ([`Writers::streams_ended`]).
    streams_ended: u64,
    /// This side's data streams opened ([`Writers::streams_opened`]).
    streams_opened: u64,
}

impl AckSample {
    /// The connection's counts now, or `None` if its controller is not an
    /// [`CappedCubic`] (a config that does not install it).
    ///
    /// Read in this order: the record, then the stats, then `cap_holds`.
    /// The controller callbacks that write the record, the frame counts in
    /// the stats, and quinn's `poll_writable` (where a hold is recorded) all
    /// run under quinn's per-connection `State` lock, which
    /// `congestion_state()` and `stats()` also take; so a send counted in
    /// the stats was built after the record's state that this sample holds,
    /// and a transmit counted there that the cap then held is already
    /// recorded as held when `cap_holds` is read.
    pub(crate) fn of(
        connection: &quinn::Connection,
        cap_holds: &CapHolds,
        writers: &Writers,
    ) -> Option<Self> {
        let record = connection
            .congestion_state()
            .into_any()
            .downcast::<CappedCubic>()
            .ok()?
            .state;
        let state = *record.lock();
        let stats = connection.stats();
        let cap = cap_holds.read();
        Some(Self {
            record,
            state,
            sent: ack_eliciting_frames(&stats.frame_tx),
            received: frames_received(&stats.frame_rx),
            cap,
            blocked: stats
                .frame_tx
                .data_blocked
                .saturating_add(stats.frame_tx.stream_data_blocked),
            stream_frames: stats.frame_tx.stream,
            writers: writers.count(),
            rtt: stats.path.rtt,
            stream_waiters: writers.stream_waiters(),
            streams_ended: writers.streams_ended(),
            streams_opened: writers.streams_opened(),
        })
    }

    /// Bytes outstanding after the latest acknowledgements, minus losses.
    #[cfg(test)]
    pub(crate) fn outstanding_after_acks(&self) -> u64 {
        self.state.outstanding_after_acks()
    }

    /// Frames received from the peer.
    #[cfg(test)]
    pub(crate) fn received(&self) -> u64 {
        self.received
    }

    /// Batches of acknowledgements so far. Tests only.
    #[cfg(test)]
    pub(crate) fn batches(&self) -> u64 {
        self.state.batches
    }

    /// Bytes the peer has acknowledged so far. Tests only.
    #[cfg(test)]
    pub(crate) fn acked_bytes(&self) -> u64 {
        self.state.acked_bytes
    }

    /// Batches that came while quinn was not application-limited. Tests
    /// only.
    #[cfg(test)]
    pub(crate) fn congestion_limited_batches(&self) -> u64 {
        self.state.congestion_limited_batches
    }

    /// A sample with these counts; `batches` counts every batch of
    /// acknowledgements, those with an `on_ack` and those without.
    #[cfg(test)]
    pub(crate) fn synthetic(
        record: &Arc<Mutex<AckState>>,
        acked: u64,
        batches: u64,
        outstanding_after_acks: u64,
        sent: u64,
        received: u64,
        cap: CapHoldReading,
    ) -> Self {
        Self {
            record: record.clone(),
            state: AckState {
                acked,
                batches,
                in_flight_after_acks: outstanding_after_acks,
                ..AckState::default()
            },
            sent,
            received,
            cap,
            blocked: 0,
            stream_frames: 0,
            writers: 0,
            rtt: Duration::ZERO,
            stream_waiters: 0,
            streams_ended: 0,
            streams_opened: 0,
        }
    }

    /// The same sample with these credit-watch readings ([`CreditWatch`]):
    /// acknowledged bytes, batches while congestion-limited, blocked frames
    /// sent, STREAM frames sent and sends still writing.
    #[cfg(test)]
    pub(crate) fn with_credit(
        mut self,
        acked_bytes: u64,
        congestion_limited_batches: u64,
        blocked: u64,
        stream_frames: u64,
        writers: usize,
    ) -> Self {
        self.state.acked_bytes = acked_bytes;
        self.state.congestion_limited_batches = congestion_limited_batches;
        self.blocked = blocked;
        self.stream_frames = stream_frames;
        self.writers = writers;
        self
    }

    /// The same sample with these stream-credit readings
    /// ([`StreamCreditWatch`]): sends waiting for stream credit, streams
    /// ended and opened. Tests only.
    #[cfg(test)]
    pub(crate) fn with_stream_credit(
        mut self,
        stream_waiters: usize,
        streams_ended: u64,
        streams_opened: u64,
    ) -> Self {
        self.stream_waiters = stream_waiters;
        self.streams_ended = streams_ended;
        self.streams_opened = streams_opened;
        self
    }

    /// The same sample with quinn's smoothed round trip at `rtt`. Tests only.
    #[cfg(test)]
    pub(crate) fn with_rtt(mut self, rtt: Duration) -> Self {
        self.rtt = rtt;
        self
    }
}

/// A path's send counts, read by the watch: sends that still have data to
/// write (counted from the stream's open until the end of its data is sent,
/// or the send is dropped; read by [`CreditWatch`], so a blocked frame from a
/// send that has since ended does not keep the path counted as waiting on
/// the peer's credit), and the stream counts [`StreamCreditWatch`] reads.
#[derive(Debug, Default)]
pub(crate) struct Writers {
    /// Sends with data still to write.
    writing: AtomicUsize,
    /// Data streams that have ended so far, in any way: wholly acknowledged,
    /// stopped by the peer, or reset by this side ([`LiveStream`]).
    ended: AtomicU64,
    /// Data streams opened so far.
    opened: AtomicU64,
    /// Sends waiting in `open_uni` for the peer's stream credit now.
    waiting_for_stream: AtomicUsize,
}

impl Writers {
    /// One more send writing, until the guard is dropped; its stream is
    /// counted as opened.
    pub(crate) fn start(self: &Arc<Self>) -> Writing {
        self.writing.fetch_add(1, Ordering::Relaxed);
        self.opened.fetch_add(1, Ordering::Relaxed);
        Writing(self.clone())
    }

    /// The path's data streams opened so far.
    pub(crate) fn streams_opened(&self) -> u64 {
        self.opened.load(Ordering::Relaxed)
    }

    /// One more send waiting in `open_uni` for the peer's stream credit,
    /// until the guard is dropped.
    pub(crate) fn wait_for_stream(self: &Arc<Self>) -> StreamWait {
        self.waiting_for_stream.fetch_add(1, Ordering::Relaxed);
        StreamWait(self.clone())
    }

    /// Sends waiting for the peer's stream credit now.
    pub(crate) fn stream_waiters(&self) -> usize {
        self.waiting_for_stream.load(Ordering::Relaxed)
    }

    /// Sends with data still to write now.
    pub(crate) fn count(&self) -> usize {
        self.writing.load(Ordering::Relaxed)
    }

    /// The stream just opened is live until the guard is dropped, which
    /// counts it as ended ([`Self::streams_ended`]).
    pub(crate) fn live(self: &Arc<Self>) -> LiveStream {
        LiveStream(self.clone())
    }

    /// The path's data streams that have ended so far, in any way.
    pub(crate) fn streams_ended(&self) -> u64 {
        self.ended.load(Ordering::Relaxed)
    }
}

/// A data stream of the path, live until dropped; its drop counts it as
/// ended ([`Writers::streams_ended`]), however it ended.
#[derive(Debug)]
pub(crate) struct LiveStream(Arc<Writers>);

impl Drop for LiveStream {
    fn drop(&mut self) {
        self.0.ended.fetch_add(1, Ordering::Relaxed);
    }
}

/// One send counted in [`Writers::stream_waiters`] while it is held.
#[derive(Debug)]
pub(crate) struct StreamWait(Arc<Writers>);

impl Drop for StreamWait {
    fn drop(&mut self) {
        self.0.waiting_for_stream.fetch_sub(1, Ordering::Relaxed);
    }
}

/// One send counted in [`Writers::count`] while it is held.
#[derive(Debug)]
pub(crate) struct Writing(Arc<Writers>);

impl Drop for Writing {
    fn drop(&mut self) {
        self.0.writing.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Decides, from samples taken every tick, whether the peer has gone a
/// whole window without acknowledging packets this side has outstanding,
/// while it kept sending.
///
/// Outstanding, at a sample, means either of:
/// - bytes still in flight from the latest batch of acknowledgements, minus
///   those quinn has since declared lost; or
/// - an ack-eliciting send seen at a sample that saw no new batch of
///   acknowledgements, so made after the latest batch. quinn declares a
///   packet lost only below the largest packet the peer acknowledged, and
///   it runs loss detection only on a batch or on its loss timer for
///   packets below that largest one, so such a packet stays outstanding
///   until the next batch. Every batch counts here, also one that
///   acknowledges only this side's acknowledgement-only packets (it has no
///   `on_ack`, yet its loss detection may declare the send lost): a new
///   batch clears this, and the bytes it leaves in flight take over.
///
/// Sends at a sample that also saw a batch may have come before it and are
/// not counted; a later probe of quinn's counts them. When quinn discards
/// its handshake keys, the in-flight bytes it held for them leave without
/// any call to the controller, so the figure from the latest batch may
/// stay high until the next batch replaces it.
/// `ConnectionStats` cannot give the bytes sent since a batch directly:
/// quinn reports every datagram to the controller (`on_sent`),
/// acknowledgement-only ones included, and a node that mostly receives
/// sends those all the time.
///
/// The window starts at a sample with something from the peer that shows
/// packets outstanding which a conforming peer is about to acknowledge:
/// - one that sees a new ack-eliciting send and no acknowledgement; or
/// - one that sees an acknowledgement after which packets are still
///   outstanding, so the peer was acknowledging and left some.
///
/// Packets outstanding with nothing new sent and no acknowledgement (after
/// an outage, while quinn's probe timer is backed off) do not start a
/// window: nothing then tells this side that the peer has received anything
/// to acknowledge. A probe that quinn sends later starts one.
///
/// The window ends, without a close, at any sample where nothing is
/// outstanding, at the second sample in a row with nothing from the peer,
/// at any sample where the egress cap holds a QUIC transmit or held one
/// since the previous sample (which also clears the send latch), or at any
/// new acknowledgement (which may start a new window as above).
/// Otherwise the path is closed at the first sample at least a window after
/// the start, so a close needs at least `window / tick + 1` consecutive
/// samples with no new acknowledgement and packets outstanding, with
/// something from the peer in at least one of any two in a row.
pub(crate) struct AckWatch {
    window_nanos: u64,
    previous: AckSample,
    /// When this side's packets were last seen outstanding with nothing
    /// acknowledged since, from a send (the dark-path rule, [`Stall::Dark`]).
    dark_since: Option<u64>,
    /// The round trip when the dark episode started.
    dark_rtt: Duration,
    /// The sample that first saw an ack-eliciting send made a full window
    /// after the dark episode started (the previous sample was already past
    /// the window, so the send was too).
    dark_probe_at: Option<u64>,
    /// The first later sample that saw another send: the second round of
    /// probes after the window. Once this has gone unanswered for the grace
    /// ([`DARK_GRACE_MAX`]), the peer had two chances.
    dark_second_at: Option<u64>,
    /// When the previous sample was taken, on the watch's clock.
    previous_now: Option<u64>,
    /// Whether the path carries data, so the dark-path rule closes it; `None`
    /// is a path that does. A path that carries none only counts its dark
    /// episodes, to their end ([`Self::observing`]).
    carries_data: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// Where dark episodes are counted (`quic_dark_episode_<label>`), if
    /// anywhere.
    dark_counts: Option<&'static crate::transport::metrics::QuicDataCounters>,
    credit: CreditWatch,
    stream_credit: StreamCreditWatch,
    /// Why the last [`Self::observe`] that said close did.
    stall: Option<Stall>,
    /// When the window started.
    since: Option<u64>,
    /// An ack-eliciting send was seen after the latest batch of
    /// acknowledgements.
    sent_after_acks: bool,
    /// Samples in a row with nothing from the peer.
    quiet_samples: u32,
    /// Since when a cap hold has been reported with no new hold event.
    hold_unchanged_since: Option<u64>,
    /// When the previous sample was taken, on the caller's clock.
    previous_at: Option<u64>,
    /// Time skipped so far because a sample came late ([`Self::watch_time`]).
    skipped: u64,
}

/// Whether the egress cap holds, or held since `previous` was read, a QUIC
/// transmit, at `now_nanos`. A hold reported for longer than its validity with
/// no new hold event is a poller left behind by an ended connection and does not
/// count ([`CapHolds`]). `hold_unchanged_since` is the caller's, kept across
/// readings. The ack watch and the data sends' stall count (`path::route`) both
/// use it, so the two agree on what a cap hold is.
pub(crate) fn cap_held(
    hold_unchanged_since: &mut Option<u64>,
    now_nanos: u64,
    cap: &CapHoldReading,
    previous: &CapHoldReading,
) -> bool {
    let new_event = cap.holds != previous.holds;
    if new_event || !cap.held_now {
        *hold_unchanged_since = None;
    }
    let live_hold = cap.held_now && {
        let since = *hold_unchanged_since.get_or_insert(now_nanos);
        let validity = u64::try_from(cap.validity.as_nanos()).unwrap_or(u64::MAX);
        now_nanos.saturating_sub(since) <= validity
    };
    new_event || live_hold
}

/// Samples in a row with nothing from the peer that end a window.
const QUIET_SAMPLES_END_WINDOW: u32 = 2;

impl AckWatch {
    /// Whether the egress cap holds, or held since the previous sample, a
    /// QUIC transmit. A hold reported for longer than its validity with no
    /// new hold event is a poller left behind by an ended connection and
    /// does not count ([`CapHolds`]).
    fn cap_held(
        &mut self,
        now_nanos: u64,
        cap: &CapHoldReading,
        previous: &CapHoldReading,
    ) -> bool {
        cap_held(&mut self.hold_unchanged_since, now_nanos, cap, previous)
    }

    pub(crate) fn new(window: Duration, first: AckSample) -> Self {
        let window_nanos = u64::try_from(window.as_nanos()).unwrap_or(u64::MAX);
        Self {
            window_nanos,
            credit: CreditWatch::new(window_nanos, &first),
            stream_credit: StreamCreditWatch {
                opened_before: first.streams_opened,
                spell: None,
            },
            dark_since: None,
            dark_rtt: Duration::ZERO,
            dark_probe_at: None,
            dark_second_at: None,
            previous_now: None,
            carries_data: None,
            dark_counts: None,
            stall: None,
            previous: first,
            since: None,
            sent_after_acks: false,
            quiet_samples: 0,
            hold_unchanged_since: None,
            previous_at: None,
            skipped: 0,
        }
    }

    /// `now_nanos` on the watch's own clock, which leaves out the time a late
    /// sample covers past one tick. A sample more than two ticks after the
    /// previous one means this side's runtime stalled (or the host
    /// suspended): the interval is read as one tick, for every rule, so a
    /// stall of this side's own cannot make a healthy path look dark,
    /// unacknowledging, starved or withholding. What happened meanwhile is
    /// still read from the sample.
    fn watch_time(&mut self, now_nanos: u64) -> u64 {
        let tick = u64::try_from(ACK_WATCH_TICK.as_nanos()).unwrap_or(u64::MAX);
        if let Some(previous) = self.previous_at {
            let gap = now_nanos.saturating_sub(previous);
            if gap > tick.saturating_mul(2) {
                self.skipped = self.skipped.saturating_add(gap - tick);
            }
        }
        self.previous_at = Some(now_nanos);
        now_nanos.saturating_sub(self.skipped)
    }

    /// Count dark episodes in `node`, and let the dark-path rule close the
    /// path only while `carries_data` is set. On a path that carries no data
    /// (every path while no release sends data on QUIC), an episode runs to
    /// its end, so the counts show how long real dark spells last against the
    /// rule's window. Durations are read at the samples, so they are good to
    /// one [`ACK_WATCH_TICK`].
    pub(crate) fn observing(
        mut self,
        carries_data: Arc<std::sync::atomic::AtomicBool>,
        node: &'static crate::transport::metrics::QuicDataCounters,
    ) -> Self {
        self.carries_data = Some(carries_data);
        self.dark_counts = Some(node);
        self
    }

    fn closes_dark(&self) -> bool {
        self.carries_data
            .as_ref()
            .is_none_or(|flag| flag.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// End the dark episode in progress, if any, counted as `ended`, or not
    /// counted with `None`.
    fn end_dark(&mut self, ended: Option<crate::transport::metrics::DarkEnd>) {
        self.dark_probe_at = None;
        self.dark_second_at = None;
        if self.dark_since.take().is_some()
            && let (Some(node), Some(ended)) = (self.dark_counts, ended)
        {
            node.record_dark_episode(ended);
        }
    }

    /// The path ended: a dark episode in progress ended without recovering.
    pub(crate) fn path_ended(&mut self) {
        self.end_dark(Some(crate::transport::metrics::DarkEnd::Unrecovered));
    }

    /// Why the path must close, once [`Self::observe`] said so.
    pub(crate) fn stall(&self) -> Option<Stall> {
        self.stall
    }

    /// Take `sample`, taken at `now_nanos`; whether the path must close
    /// ([`Self::stall`] says why).
    pub(crate) fn observe(&mut self, now_nanos: u64, sample: AckSample) -> bool {
        let now_nanos = self.watch_time(now_nanos);
        self.stall = self.decide(now_nanos, sample);
        self.stall.is_some()
    }

    /// Every rule reads time on the watch's own clock ([`Self::watch_time`]):
    /// quinn's driver runs on the same runtime, so when the runtime stalls
    /// or the host suspends, the counters stop too, and an interval that
    /// long would read as the peer's silence or slowness. A peer cannot make
    /// this side's runtime late, so the forgiveness gives it nothing.
    fn decide(&mut self, now_nanos: u64, sample: AckSample) -> Option<Stall> {
        let dark_now = now_nanos;
        let same_controller = Arc::ptr_eq(&sample.record, &self.previous.record);
        // A new controller restarts the counts: read it as an
        // acknowledgement, with nothing known to be outstanding.
        let acknowledged = !same_controller || sample.state.acked != self.previous.state.acked;
        let new_batch = !same_controller || sample.state.batches != self.previous.state.batches;
        let peer_sent = same_controller && sample.received != self.previous.received;
        let sent_new = same_controller && sample.sent != self.previous.sent;
        let outstanding_after_acks = same_controller && sample.state.outstanding_after_acks() > 0;
        let previous_cap = self.previous.cap;
        let cap_held = self.cap_held(now_nanos, &sample.cap, &previous_cap);
        let previous_now = self.previous_now.replace(dark_now);
        let withheld = same_controller && self.stream_credit.observe(now_nanos, &sample);
        let rtt = sample.rtt;
        // Packets this side sent after the latest batch of acknowledgements
        // are outstanding too, though no batch counted them: at the start of
        // the interval (the latch) or within it.
        let sent_unacknowledged = self.sent_after_acks || (sent_new && !new_batch);
        let starved = self.credit.observe(
            now_nanos,
            &self.previous,
            &sample,
            same_controller,
            cap_held,
            sent_unacknowledged,
        );
        self.previous = sample;
        if new_batch {
            self.sent_after_acks = false;
        } else if sent_new {
            self.sent_after_acks = true;
        }
        self.quiet_samples = if peer_sent {
            0
        } else {
            self.quiet_samples.saturating_add(1)
        };
        if cap_held {
            // A send counted in quinn's stats may still wait in the egress
            // cap, so nothing says when it left: no latch, no window.
            self.sent_after_acks = false;
            self.since = None;
            // The cap held this side's own sends: whether the peer is dark
            // can no longer be told.
            self.end_dark(Some(crate::transport::metrics::DarkEnd::CapHeld));
            return None;
        }
        if starved {
            return Some(Stall::CreditStarved);
        }

        if withheld {
            return Some(Stall::StreamCreditWithheld);
        }
        let outstanding = outstanding_after_acks || self.sent_after_acks;
        // The dark-path rule: this side's packets outstanding at every sample
        // for the window, from a send, none acknowledged and nothing from the
        // peer. A send is required to start it, so bytes left counted from an
        // old batch (handshake keys discarded) on an idle path start nothing.
        // A peer that sends ends it: one that sends and acknowledges nothing
        // is the rule below's, and one that sends after an outage while this
        // side's backed-off probe has not gone yet is alive.
        if acknowledged || peer_sent {
            let lasted = self
                .dark_since
                .map_or(0, |start| dark_now.saturating_sub(start));
            self.end_dark(Some(crate::transport::metrics::DarkEnd::Recovered(lasted)));
        } else if !outstanding {
            self.end_dark(Some(crate::transport::metrics::DarkEnd::Unrecovered));
        } else if self.dark_since.is_none() && sent_new {
            self.dark_since = Some(dark_now);
            self.dark_rtt = rtt;
        } else if let Some(start) = self.dark_since
            && sent_new
        {
            if self.dark_probe_at.is_none()
                && previous_now.is_some_and(|at| at.saturating_sub(start) >= self.window_nanos)
            {
                self.dark_probe_at = Some(dark_now);
            } else if self.dark_probe_at.is_some() && self.dark_second_at.is_none() {
                self.dark_second_at = Some(dark_now);
            }
        }
        // Dark for the window is not enough: after an outage quinn sends
        // again only at its next backed-off probe, and an ack-only peer
        // (keepalive 15 s) sends nothing until then, so a spell shorter than
        // the window can still look dark past it. Closed once two rounds of
        // sends made after the window (the first post-window send, then one
        // seen at a later sample: quinn's next backed-off probe) went
        // unanswered, the second for its grace. Every outage shorter than
        // the window is survived whatever the backoff, and so is one lost
        // probe round (a probe pair, or the peer's acknowledgement of it).
        let grace = u64::try_from(
            (self.dark_rtt * 3)
                .clamp(ACK_WATCH_TICK, DARK_GRACE_MAX)
                .as_nanos(),
        )
        .unwrap_or(u64::MAX);
        if self.closes_dark()
            && self
                .dark_since
                .is_some_and(|start| dark_now.saturating_sub(start) >= self.window_nanos)
            && self
                .dark_second_at
                .is_some_and(|at| dark_now.saturating_sub(at) >= grace)
        {
            self.end_dark(Some(crate::transport::metrics::DarkEnd::Closed));
            return Some(Stall::Dark);
        }
        if !outstanding || self.quiet_samples >= QUIET_SAMPLES_END_WINDOW {
            self.since = None;
            return None;
        }
        if acknowledged {
            self.since = peer_sent.then_some(now_nanos);
            return None;
        }
        let start = match self.since {
            Some(start) => start,
            None if sent_new && peer_sent => *self.since.insert(now_nanos),
            None => return None,
        };
        (now_nanos.saturating_sub(start) >= self.window_nanos).then_some(Stall::NotAcknowledging)
    }
}

impl Drop for AckWatch {
    /// However the watch ends (it returned, or the path's task dropped it), a
    /// dark episode still in progress ended without recovering.
    fn drop(&mut self) {
        self.path_ended();
    }
}

/// Why [`AckWatch`] closes a path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stall {
    /// The peer kept sending and acknowledged none of this side's packets
    /// for the window.
    NotAcknowledging,
    /// This side's packets were outstanding for the window, nothing was
    /// acknowledged and nothing came from the peer.
    Dark,
    /// Sends waited on the peer's credit for at least
    /// [`CREDIT_LIMITED_MIN`] of the window, and the peer acknowledged less
    /// than [`FLOOR_RATE_BYTES_PER_SEC`] over that time ([`CreditWatch`]).
    CreditStarved,
    /// The peer granted no stream credit while this side wanted streams,
    /// for [`STREAM_CREDIT_WINDOW`], though enough of this side's streams
    /// ended that an honest peer would have ([`StreamCreditWatch`]).
    StreamCreditWithheld,
}

impl Stall {
    /// The label it is logged and counted under.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::NotAcknowledging => "not_acknowledging",
            Self::Dark => "dark",
            Self::CreditStarved => "credit_starved",
            Self::StreamCreditWithheld => "stream_credit_withheld",
        }
    }
}

/// The credit rule: a path whose sends wait on the peer's flow-control credit
/// must have its data acknowledged at [`FLOOR_RATE_BYTES_PER_SEC`] or better
/// over the time it waited.
///
/// Between two samples, the path waited on the peer's credit when all of
/// these held:
/// - a send still had data to write ([`Writers`]);
/// - quinn reported a write blocked by the peer's limit: a DATA_BLOCKED or
///   STREAM_DATA_BLOCKED frame was sent in the interval, or one was sent
///   earlier and no STREAM frame has been sent since (a credit raise that
///   lets the blocked send go on shows as STREAM frames; one that does not,
///   such as MAX_STREAM_DATA for another stream, leaves it waiting; one in
///   tiny steps blocks again at each new limit);
/// - every batch of acknowledgements in the interval came while quinn was
///   application-limited, so its congestion window was not what held the
///   data back (a path limited by its congestion window, a shared uplink or
///   the egress cap is this side's back-pressure, not the peer's); and
/// - the egress cap held no QUIC transmit.
///
/// The path is closed when the time it waited, within the last
/// [`ACK_WATCH_WINDOW`], is at least [`CREDIT_LIMITED_MIN`] and the bytes
/// acknowledged in the window are under [`FLOOR_RATE_BYTES_PER_SEC`] times
/// that time.
///
/// Time is counted in whole sampling intervals ([`ACK_WATCH_TICK`]), and a
/// block may come at the very end of one, so one interval says little about
/// how long the send waited: [`CREDIT_LIMITED_MIN`] is two intervals. A send
/// that blocks once (as an honest one may on a connection's small first
/// window) and then streams counts one. A peer that lets the send block in
/// every other interval, raising its credit between, counts every other one
/// and is judged on them.
pub(crate) struct CreditWatch {
    window_nanos: u64,
    /// A blocked frame was sent and no STREAM frame since.
    carry: bool,
    /// Per sample within the window: when it was taken, the time waited on
    /// credit in the interval it ended, seen with acknowledgements and
    /// settled (nothing outstanding), and the bytes acknowledged by then.
    /// The oldest entry is the window's start and its own interval is not
    /// counted.
    history: VecDeque<(u64, u64, u64, u64)>,
}

/// [`CreditWatch::history`]'s time for a sample taken at an unknown time.
const UNKNOWN_AT: u64 = u64::MAX;

impl CreditWatch {
    fn new(window_nanos: u64, first: &AckSample) -> Self {
        Self {
            window_nanos,
            carry: false,
            // Taken at an unknown time: the first interval is not counted.
            history: VecDeque::from([(UNKNOWN_AT, 0, 0, first.state.acked_bytes)]),
        }
    }

    /// Take `sample` at `now_nanos`, after `previous`; whether the path is
    /// starved of credit.
    fn observe(
        &mut self,
        now_nanos: u64,
        previous: &AckSample,
        sample: &AckSample,
        same_controller: bool,
        cap_held: bool,
        sent_unacknowledged: bool,
    ) -> bool {
        if !same_controller {
            self.carry = false;
            self.history = VecDeque::from([(now_nanos, 0, 0, sample.state.acked_bytes)]);
            return false;
        }
        let new_block = sample.blocked != previous.blocked;
        let streamed = sample.stream_frames != previous.stream_frames;
        let blocked = new_block || (self.carry && !streamed);
        self.carry = sample.writers > 0 && (new_block || (self.carry && !streamed));
        let congestion_limited =
            sample.state.congestion_limited_batches != previous.state.congestion_limited_batches;
        // Affirmative evidence only. An interval with acknowledgements counts
        // when every batch of them was application-limited, so what held
        // delivery back was the credit, not this side's window. An interval
        // with none counts only if nothing was outstanding at either end of
        // it ("settled": no bytes in flight after the latest batch, and no
        // send since it, `sent_unacknowledged`): everything this side sent
        // was acknowledged, so the only thing it waits for is credit. With packets outstanding and no
        // acknowledgement the peer may simply be unreachable for a moment,
        // which is the dark rule's to judge, and the interval counts nothing.
        let new_batch = sample.state.batches != previous.state.batches;
        let base = sample.writers > 0 && blocked && !cap_held;
        let waited = base && new_batch && !congestion_limited;
        let settled = base
            && !new_batch
            && !sent_unacknowledged
            && sample.state.outstanding_after_acks() == 0
            && previous.state.outstanding_after_acks() == 0;
        let since = self.history.back().map_or(UNKNOWN_AT, |&(at, ..)| at);
        let interval = |counts: bool| {
            if counts && since != UNKNOWN_AT {
                now_nanos.saturating_sub(since)
            } else {
                0
            }
        };
        self.history.push_back((
            now_nanos,
            interval(waited),
            interval(settled),
            sample.state.acked_bytes,
        ));
        let start = now_nanos.saturating_sub(self.window_nanos);
        while self.history.len() > 1
            && (self.history[0].0 == UNKNOWN_AT || self.history[1].0 <= start)
        {
            self.history.pop_front();
        }
        let (limited, quiet) = self.history.iter().skip(1).fold(
            (0u64, 0u64),
            |(limited, quiet), &(_, waited, settled, _)| (limited + waited, quiet + settled),
        );
        // Seen waiting with acknowledgements: CREDIT_LIMITED_MIN is enough.
        // Settled waiting may also be a link that went quiet just after the
        // last acknowledgement, so it counts only once it fills the whole
        // window, as the dark rule's outage must.
        let min = u64::try_from(CREDIT_LIMITED_MIN.as_nanos()).unwrap_or(u64::MAX);
        if limited < min && quiet < self.window_nanos {
            return false;
        }
        // Judged over the waiting that qualified: a settled spell does not
        // dilute a rate seen with acknowledgements.
        let counted = if limited >= min {
            limited
        } else {
            limited + quiet
        };
        let acked = sample.state.acked_bytes.saturating_sub(self.history[0].3);
        let floor = u128::from(credit_floor_rate(sample.rtt)) * u128::from(counted) / 1_000_000_000;
        u128::from(acked) < floor
    }
}

/// How long the peer may keep its stream limit closed while this side wants
/// streams, after enough of this side's streams were wholly acknowledged
/// that it should have raised it ([`StreamCreditWatch`]). Longer than the
/// other rules' window: an honest receiver lowers its limit on purpose, by
/// one per one-packet Short waiting for its consumer (`inbound::StreamLimit`),
/// and a consumer may be slow for a while; one that stalls a minute is not
/// worth keeping the path for.
pub(crate) const STREAM_CREDIT_WINDOW: Duration = Duration::from_secs(60);

/// Streams of this side ended during a withheld spell past which an honest
/// peer has granted more, at most: quinn returns stream
/// credit once more than an eighth of its limit is freed, and the limit is
/// at most `MAX_UNI_STREAMS` (64). Fewer streams in flight need fewer
/// ([`StreamCreditWatch`]).
pub(crate) const STREAM_CREDIT_FREED: u64 = super::config::MAX_UNI_STREAMS as u64 / 8 + 1;

/// How recently a send must have waited for stream credit for the
/// stream-credit rule to close the path: a peer withholding credit nobody
/// wants any more costs nothing ([`StreamCreditWatch`]).
pub(crate) const STREAM_CREDIT_DEMAND: Duration = Duration::from_secs(20);

/// The stream-credit rule: a peer that acknowledges this side's streams but
/// never grants more holds every new stream at its open deadline, so every
/// streamed send goes on UDP after it, without stalling anything the other
/// rules watch.
///
/// Credit is judged by what it does, not by frames: the peer granted credit
/// when a stream of this side opened (quinn 0.11.19 sends no STREAMS_BLOCKED,
/// and a MAX_STREAMS frame may repeat an old limit, so neither frame says
/// whether credit was granted). A withheld spell starts at the first sample
/// at which a send waits in `open_uni` for the peer's stream credit with no
/// stream opened since the previous sample, and ends only when a stream
/// opens. It survives samples with no waiting send for up to
/// [`STREAM_CREDIT_DEMAND`] (a send waits at most its open deadline, 2 s,
/// and then goes on UDP, so a path whose sends come now and then shows a
/// waiter only at some samples), and ends past that. The path is closed when
/// the spell has lasted [`STREAM_CREDIT_WINDOW`] and enough of this side's
/// streams ended during it: [`STREAM_CREDIT_FREED`], or every stream live
/// when it began if fewer. A stream ends however it ends (wholly
/// acknowledged, stopped by the peer, or reset by this side), so a stream
/// the peer stopped long ago is not counted as live. When a spell begins
/// every slot of the peer's is taken by this side's live streams, and our
/// receiver reads every stream to its end as it arrives (a Stream also
/// while it waits for its consumer), so a stream that ends during the spell
/// by its acknowledgement or the peer's stop is a slot an honest peer has
/// freed; only a one-packet Short waiting for its consumer holds one, and
/// lowers the limit it advertises while it does. A stream this side resets
/// counts as ended too, though the reset may never reach the peer (during
/// an outage nothing does): in-flight streams reset by cancellation during
/// an outage, before any send timeout makes the path suspect, with sends
/// still waiting, close the path here at about 60 s, before the dark rule
/// would, under this label instead of [`Stall::Dark`]. And with streamed
/// sends more than [`STREAM_CREDIT_DEMAND`] apart the spell ends between
/// them, so a withholding peer is not closed; each such send waits its 2 s
/// open deadline and goes on UDP.
///
/// Residual, accepted: a peer that grants one stream just before each
/// window ends keeps the path open while starving it. What bounds it is that
/// each starved send waits its 2 s open deadline and then goes on UDP, so
/// the cost is 2 s of latency per streamed message, not lost data.
pub(crate) struct StreamCreditWatch {
    /// This side's data streams opened at the previous sample.
    opened_before: u64,
    /// The withheld spell in progress, if any.
    spell: Option<WithheldSpell>,
}

/// A withheld spell ([`StreamCreditWatch`]).
struct WithheldSpell {
    /// When it began.
    since: u64,
    /// This side's streams ended when it began.
    ended_then: u64,
    /// This side's streams live (opened and not ended) when it began.
    in_flight_then: u64,
    /// When a send was last seen waiting for stream credit.
    last_waiter: u64,
}

impl StreamCreditWatch {
    /// Take `sample` at `now_nanos`; whether the peer withheld its stream
    /// credit for the window.
    fn observe(&mut self, now_nanos: u64, sample: &AckSample) -> bool {
        let nanos = |d: Duration| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        let granted = sample.streams_opened != self.opened_before;
        self.opened_before = sample.streams_opened;
        if granted {
            self.spell = None;
            return false;
        }
        if sample.stream_waiters > 0 {
            let spell = self.spell.get_or_insert(WithheldSpell {
                since: now_nanos,
                ended_then: sample.streams_ended,
                in_flight_then: sample.streams_opened.saturating_sub(sample.streams_ended),
                last_waiter: now_nanos,
            });
            spell.last_waiter = now_nanos;
        }
        // A spell nobody has wanted a stream in for a while is over: a later
        // wait starts a new one.
        if self.spell.as_ref().is_some_and(|spell| {
            now_nanos.saturating_sub(spell.last_waiter) > nanos(STREAM_CREDIT_DEMAND)
        }) {
            self.spell = None;
        }
        let Some(spell) = &self.spell else {
            return false;
        };
        let freed = sample.streams_ended.saturating_sub(spell.ended_then);
        now_nanos.saturating_sub(spell.since) >= nanos(STREAM_CREDIT_WINDOW)
            && freed >= STREAM_CREDIT_FREED.min(spell.in_flight_then)
    }
}

/// Where a path's watch reports that the peer was heard from. Empty until the
/// path is active, so a path still being set up reports nothing.
#[derive(Default)]
pub(crate) struct PeerHeard(std::sync::OnceLock<Box<dyn Fn() + Send + Sync>>);

impl PeerHeard {
    /// From now on, call `report` when the peer was heard from. A second call
    /// changes nothing.
    pub(crate) fn report_to(&self, report: Box<dyn Fn() + Send + Sync>) {
        if self.0.set(report).is_err() {
            tracing::debug!("QUIC path: its receipts already have somewhere to go");
        }
    }

    fn report(&self) {
        if let Some(report) = self.0.get() {
            report();
        }
    }
}

/// Watch `connection` every [`ACK_WATCH_TICK`] on `time`, and close it with
/// `close_code::PEER_NOT_ACKNOWLEDGING` when [`AckWatch`] says so. Returns
/// once it closed the connection, or once the connection has closed. Runs
/// beside the path's receive task, never in a task of its own.
///
/// Each sample with new frames from the peer also tells `heard`. Frames count
/// only from authenticated packets, and PINGs are frames, so a quiet path with
/// a live peer reports at its keepalive interval.
pub(crate) async fn watch_acknowledgements<T: TimeSource>(
    connection: quinn::Connection,
    closer: QuicCloser,
    time: T,
    shared: WatchShared,
    report: &mut PathReport,
) {
    let WatchShared {
        cap_holds,
        writers,
        rtt,
        heard,
        carries_data,
    } = shared;
    let Some(first) = AckSample::of(&connection, &cap_holds, &writers) else {
        tracing::warn!(
            remote = %connection.remote_address(),
            "QUIC path: the connection's congestion controller does not count \
             acknowledgements; the acknowledgement watch is off for it"
        );
        return;
    };
    let mut received = first.received;
    let mut watch = AckWatch::new(ACK_WATCH_WINDOW, first).observing(carries_data, report.node);
    loop {
        time.sleep(ACK_WATCH_TICK).await;
        rtt.set(connection.rtt());
        // Read before the counts, so a closed connection's counts are its
        // final ones.
        let closed = connection.close_reason().is_some();
        report.report(Some(time.now_nanos()));
        if closed {
            return;
        }
        let Some(sample) = AckSample::of(&connection, &cap_holds, &writers) else {
            return;
        };
        if sample.received != received {
            received = sample.received;
            heard.report();
        }
        if watch.observe(time.now_nanos(), sample) {
            #[cfg(test)]
            if !report.watch_closes {
                continue;
            }
            // One close code for all three: it tells the peer only that this
            // side stopped using the path.
            if closer.close(QuicCloseCode::PeerNotAcknowledging) {
                #[cfg(test)]
                if let Some(stall) = watch.stall() {
                    TEST_STALLS
                        .lock()
                        .push((connection.remote_address(), stall));
                }
                tracing::warn!(
                    remote = %connection.remote_address(),
                    window = ?ACK_WATCH_WINDOW,
                    stall = watch.stall().map_or("", Stall::label),
                    "QUIC path: the peer did not acknowledge this node's data within \
                     the window; closing the path so the connection falls back to UDP"
                );
            }
            return;
        }
    }
}

pub(crate) mod loss_gate;

#[cfg(test)]
mod cap_tests;

/// Why each watch closed its path, by the path's remote address. Tests only.
#[cfg(test)]
pub(crate) static TEST_STALLS: parking_lot::Mutex<Vec<(std::net::SocketAddr, Stall)>> =
    parking_lot::Mutex::new(Vec::new());

/// Why the watches of paths to `remote` closed them, in order. Tests only.
#[cfg(test)]
pub(crate) fn stalls_to(remote: std::net::SocketAddr) -> Vec<Stall> {
    TEST_STALLS
        .lock()
        .iter()
        .filter(|(to, _)| *to == remote)
        .map(|(_, stall)| *stall)
        .collect()
}

/// What a path's receive supervisor keeps for its acknowledgement watch.
pub(crate) struct AckWatchParts {
    pub(crate) shared: WatchShared,
    pub(crate) report: PathReport,
}

/// What the acknowledgement watch reads and tells, shared with the path.
pub(crate) struct WatchShared {
    pub(crate) cap_holds: Arc<CapHolds>,
    pub(crate) writers: Arc<Writers>,
    /// The connection's round trip for the consumers of its streams, set at
    /// each tick (`streaming::PathRtt`).
    pub(crate) rtt: Arc<crate::transport::peer_connection::streaming::PathRtt>,
    pub(crate) heard: Arc<PeerHeard>,
    /// Set once the path may send data (`QuicPath::enable_data`): only then
    /// does the dark-path rule close it ([`AckWatch::observing`]).
    pub(crate) carries_data: Arc<std::sync::atomic::AtomicBool>,
}

/// Watch panics `PathReport::panic_once_bucketed` raised. Tests only; only
/// the one test that sets it raises any.
#[cfg(test)]
pub(crate) static TEST_WATCH_PANICS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// A path's byte and loss counts, reported node-wide by their change at each
/// tick of the watch, and once more when the report is dropped. It is owned
/// by the path's receive supervisor and lent to the watch, so it is dropped
/// when the path's task ends (the connection closed and the receive task
/// returned, or the task was aborted with the path), and the counts up to
/// then, often the losses that ended the path, are still reported. Then the
/// window is dropped, which counts the path among the ended ones. A panic in
/// the watch, which the supervisor contains while the path stays up, does
/// not drop it: the path stays in its live bucket until it really ends.
/// After a contained watch panic nothing ticks it any more, so the path's
/// bytes and packets from then on arrive as one report when the path ends.
pub(crate) struct PathReport {
    connection: quinn::Connection,
    node: &'static crate::transport::metrics::QuicDataCounters,
    bytes: UdpBytes,
    loss: PathLoss,
    window: crate::transport::metrics::PathLossWindow,
    /// The loss gate's verdict counts and those already reported; `None` when
    /// the connection's controller has no gate.
    gate: Option<(Arc<GateVerdicts>, [u64; loss_gate::GATE_VERDICTS])>,
    gate_window: crate::transport::metrics::PathGateWindow,
    /// When the path's time was last reported, in the time source's
    /// nanoseconds.
    timed_at: u64,
    /// The path's budget meter, evaluated at each tick so an idle path's send
    /// window and held epochs are settled too.
    meter: Option<Arc<super::budget::ConnMeter>>,
    /// The next report that finds the path in a bucket panics, once
    /// (supervisor test).
    #[cfg(test)]
    pub(crate) panic_once_bucketed: bool,
    /// The watch closes the path when a rule says so (`QuicPathLimits`).
    #[cfg(test)]
    pub(crate) watch_closes: bool,
}

impl PathReport {
    /// A report for a path built at `built_at` (time-source nanoseconds).
    pub(crate) fn new(
        connection: quinn::Connection,
        node: &'static crate::transport::metrics::QuicDataCounters,
        built_at: u64,
        meter: Option<Arc<super::budget::ConnMeter>>,
    ) -> Self {
        let stats = connection.stats();
        let gate = connection
            .congestion_state()
            .into_any()
            .downcast::<CappedCubic>()
            .ok()
            .and_then(|capped| capped.gate_verdicts())
            .map(|verdicts| {
                let seen = verdicts.read();
                (verdicts, seen)
            });
        Self {
            timed_at: built_at,
            meter,
            bytes: UdpBytes::of(&stats),
            loss: PathLoss::of(&stats),
            window: crate::transport::metrics::PathLossWindow::new(node),
            gate,
            gate_window: crate::transport::metrics::PathGateWindow::new(node),
            connection,
            node,
            #[cfg(test)]
            panic_once_bucketed: false,
            #[cfg(test)]
            watch_closes: true,
        }
    }

    /// Report the change since the last report, from one read of the stats,
    /// and the time since the last one when `now` is given. The final report,
    /// on drop, has no clock, so a path's last tick to its end (under
    /// [`ACK_WATCH_TICK`]) is not counted.
    fn report(&mut self, now: Option<u64>) {
        if let Some(now) = now {
            // Whole milliseconds, carrying the rest to the next report.
            let ms = now.saturating_sub(self.timed_at) / 1_000_000;
            self.timed_at = self.timed_at.saturating_add(ms * 1_000_000);
            self.node.record_path_time(ms);
            if let Some(meter) = &self.meter {
                meter.maybe_evaluate(now);
            }
        }
        let stats = self.connection.stats();
        let (next, sent, got) = self.bytes.step(UdpBytes::of(&stats));
        self.node.record_bytes(sent, got);
        self.bytes = next;
        let (next, delta) = self.loss.step(PathLoss::of(&stats));
        self.node.record_path_loss(&mut self.window, delta);
        self.loss = next;
        if let Some((verdicts, seen)) = &mut self.gate {
            let now = verdicts.read();
            let delta: [u64; loss_gate::GATE_VERDICTS] =
                std::array::from_fn(|i| now[i].saturating_sub(seen[i]));
            *seen = now;
            self.node.record_gate(&mut self.gate_window, delta);
        }
        #[cfg(test)]
        if self.panic_once_bucketed && self.window.is_bucketed() {
            self.panic_once_bucketed = false;
            TEST_WATCH_PANICS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            panic!("test: the acknowledgement watch panics");
        }
    }
}

impl Drop for PathReport {
    fn drop(&mut self) {
        // Not while unwinding: quinn's state lock may be what panicked.
        if !std::thread::panicking() {
            self.report(None);
        }
    }
}
/// A path's UDP payload bytes as quinn counts them, reported node-wide by their
/// change at each tick of the watch (`quic_bytes_sent`, `quic_bytes_received`)
/// and when the path's task ends ([`PathReport`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UdpBytes {
    sent: u64,
    received: u64,
}

impl UdpBytes {
    fn of(stats: &quinn::ConnectionStats) -> Self {
        Self {
            sent: stats.udp_tx.bytes,
            received: stats.udp_rx.bytes,
        }
    }

    /// The bytes sent and received from `self` to `now`, and the state to report
    /// the next change from, which is `now`. Pure, so the report is tested.
    #[must_use]
    fn step(self, now: Self) -> (Self, u64, u64) {
        (
            now,
            now.sent.saturating_sub(self.sent),
            now.received.saturating_sub(self.received),
        )
    }
}

/// A path's packet counts as quinn's `PathStats` keeps them, reported
/// node-wide by their change at each tick of the watch (`quic_sent_packets`,
/// `quic_lost_packets`, `quic_lost_bytes`, `quic_congestion_events`, and the
/// path's own loss rate in `quic_loss_rate_<bucket>`), as for [`UdpBytes`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct PathLoss {
    pub(crate) sent_packets: u64,
    pub(crate) lost_packets: u64,
    pub(crate) lost_bytes: u64,
    pub(crate) congestion_events: u64,
}

impl PathLoss {
    fn of(stats: &quinn::ConnectionStats) -> Self {
        let path = stats.path;
        Self {
            sent_packets: path.sent_packets,
            lost_packets: path.lost_packets,
            lost_bytes: path.lost_bytes,
            congestion_events: path.congestion_events,
        }
    }

    /// The change from `self` to `now`, and the state to report the next change
    /// from, which is `now`. Pure, so the report is tested.
    #[must_use]
    fn step(self, now: Self) -> (Self, Self) {
        let delta = Self {
            sent_packets: now.sent_packets.saturating_sub(self.sent_packets),
            lost_packets: now.lost_packets.saturating_sub(self.lost_packets),
            lost_bytes: now.lost_bytes.saturating_sub(self.lost_bytes),
            congestion_events: now.congestion_events.saturating_sub(self.congestion_events),
        };
        (now, delta)
    }
}

#[cfg(test)]
mod tests;
