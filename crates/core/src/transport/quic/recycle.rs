//! When a QUIC path is closed so its peer falls back to the UDP session
//! (#5940, step M6).
//!
//! Two conditions close a path, each with its own close code and counted in
//! `quic_close_sent_<label>` ([`QuicCloseCode`]). Either code asks the peer
//! to fall back to UDP for THIS connection only. It is not a permanent
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
//! What "acknowledged" is measured with. quinn's `ConnectionStats` has no
//! counter of packets the peer newly acknowledged: `frame_rx.acks` counts
//! ACK frames, including ones that acknowledge nothing new, so it cannot
//! tell a peer that acknowledges from one that only sends ACK frames. quinn
//! does tell the congestion controller about every ack-eliciting packet the
//! peer newly acknowledges (`Controller::on_ack`, once per packet) and, after
//! each batch of acknowledgements, the bytes still in flight
//! (`Controller::on_end_acks`); packets later declared lost are reported
//! through `on_congestion_event`. So the transport config wraps Cubic in
//! [`AckCounting`], which records those and otherwise passes every call
//! through. The other two inputs are from `ConnectionStats`:
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
//! - a peer that went silent: quinn's idle timeout ends the connection
//!   instead;
//! - a send the node's egress cap (`total-bandwidth-limit`) holds: quinn
//!   counts a transmit's frames, and its bytes in flight, when it builds
//!   it, and the cap may hold it longer than the window.
//!
//! **Guard 2 is inactive while the egress cap holds QUIC transmits.** While
//! the cap holds any QUIC transmit, or held one since the previous sample,
//! no window runs, on any path: the cap is node-wide and quinn does not say
//! which connection a held transmit belongs to. On a node whose cap binds
//! most of the time, guard 2 is then off most of the time. Narrowing the
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
//! [`QuicCloseCode`]: crate::transport::metrics::QuicCloseCode

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use quinn::FrameStats;
use quinn::congestion::{Controller, ControllerFactory, ControllerMetrics};

use super::QuicCloser;
#[cfg(doc)]
use super::error::close_code;
use super::socket::{CapHoldReading, CapHolds};
use crate::simulation::TimeSource;
use crate::transport::metrics::QuicCloseCode;

/// Sends one path may reset before it is closed
/// ([`close_code::SEND_ABORT_LIMIT`]).
///
/// A healthy path resets a send only when the send is cancelled (a caller's
/// timeout) or its chunk source fails, both rare, so 4096 is far above what
/// one does over a long life, and a path that gets there is closed and
/// rebuilt with nothing lost but the upgrade. The `stopped` map entries the
/// limit bounds are about 100 bytes each, so at most about 400 KiB per
/// path, under one connection's flow-control window
/// (`config::MAX_WINDOW_BYTES`, 2 MiB).
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

/// Counts the sends a path reset, and closes its connection past the
/// limit. Shared by the path and every send in progress on it.
pub(crate) struct SendAborts {
    count: AtomicU64,
    limit: u64,
    closer: QuicCloser,
}

impl SendAborts {
    pub(crate) fn new(limit: u64, closer: QuicCloser) -> Self {
        Self {
            count: AtomicU64::new(0),
            limit,
            closer,
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

/// Builds [`AckCounting`] controllers around Cubic.
#[derive(Debug, Default)]
pub(crate) struct AckCountingFactory {
    cubic: Arc<quinn::congestion::CubicConfig>,
}

impl ControllerFactory for AckCountingFactory {
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(AckCounting::new(self.cubic.clone().build(now, current_mtu)))
    }
}

/// What [`AckCounting`] records, under one lock so a reader sees one
/// consistent state.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AckState {
    /// Ack-eliciting packets the peer newly acknowledged (`on_ack` calls).
    acked: u64,
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

/// A congestion controller that records the acknowledgements the peer
/// sends ([`AckState`]) and passes every call to `inner` unchanged. Its
/// clones (quinn's `Connection::congestion_state`) share the record.
pub(crate) struct AckCounting {
    inner: Box<dyn Controller>,
    state: Arc<Mutex<AckState>>,
}

impl AckCounting {
    fn new(inner: Box<dyn Controller>) -> Self {
        Self {
            inner,
            state: Arc::default(),
        }
    }

    /// Whether the wrapped controller is a `C`.
    #[cfg(test)]
    pub(crate) fn wraps<C: Controller + 'static>(&self) -> bool {
        self.inner.clone_box().into_any().downcast::<C>().is_ok()
    }
}

impl Controller for AckCounting {
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
        self.state.lock().acked += 1;
        self.inner.on_ack(now, sent, bytes, app_limited, rtt);
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
        self.inner.on_mtu_update(new_mtu);
    }

    fn window(&self) -> u64 {
        self.inner.window()
    }

    fn metrics(&self) -> ControllerMetrics {
        self.inner.metrics()
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(Self {
            inner: self.inner.clone_box(),
            state: self.state.clone(),
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
/// does not count the STREAMS_BLOCKED frames it sends; a packet carrying
/// only that frame is missed, which can only delay a close, never cause
/// one.
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
    /// The QUIC transmits the egress cap holds, node-wide.
    cap: CapHoldReading,
}

impl AckSample {
    /// The connection's counts now, or `None` if its controller is not an
    /// [`AckCounting`] (a config that does not install it).
    ///
    /// Read in this order: the record, then the stats, then `cap_holds`.
    /// The controller callbacks that write the record, the frame counts in
    /// the stats, and quinn's `poll_writable` (where a hold is recorded) all
    /// run under quinn's per-connection `State` lock, which
    /// `congestion_state()` and `stats()` also take; so a send counted in
    /// the stats was built after the record's state that this sample holds,
    /// and a transmit counted there that the cap then held is already
    /// recorded as held when `cap_holds` is read.
    pub(crate) fn of(connection: &quinn::Connection, cap_holds: &CapHolds) -> Option<Self> {
        let record = connection
            .congestion_state()
            .into_any()
            .downcast::<AckCounting>()
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
                lost_since_acks: 0,
            },
            sent,
            received,
            cap,
        }
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
    /// When the window started.
    since: Option<u64>,
    /// An ack-eliciting send was seen after the latest batch of
    /// acknowledgements.
    sent_after_acks: bool,
    /// Samples in a row with nothing from the peer.
    quiet_samples: u32,
    /// Since when a cap hold has been reported with no new hold event.
    hold_unchanged_since: Option<u64>,
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
        let new_event = cap.holds != previous.holds;
        if new_event || !cap.held_now {
            self.hold_unchanged_since = None;
        }
        let live_hold = cap.held_now && {
            let since = *self.hold_unchanged_since.get_or_insert(now_nanos);
            let validity = u64::try_from(cap.validity.as_nanos()).unwrap_or(u64::MAX);
            now_nanos.saturating_sub(since) <= validity
        };
        new_event || live_hold
    }

    pub(crate) fn new(window: Duration, first: AckSample) -> Self {
        Self {
            window_nanos: u64::try_from(window.as_nanos()).unwrap_or(u64::MAX),
            previous: first,
            since: None,
            sent_after_acks: false,
            quiet_samples: 0,
            hold_unchanged_since: None,
        }
    }

    /// Take `sample`, taken at `now_nanos`; whether the path must close.
    pub(crate) fn observe(&mut self, now_nanos: u64, sample: AckSample) -> bool {
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
            return false;
        }
        let outstanding = outstanding_after_acks || self.sent_after_acks;
        if !outstanding || self.quiet_samples >= QUIET_SAMPLES_END_WINDOW {
            self.since = None;
            return false;
        }
        if acknowledged {
            self.since = peer_sent.then_some(now_nanos);
            return false;
        }
        let start = match self.since {
            Some(start) => start,
            None if sent_new && peer_sent => *self.since.insert(now_nanos),
            None => return false,
        };
        now_nanos.saturating_sub(start) >= self.window_nanos
    }
}

/// Watch `connection` every [`ACK_WATCH_TICK`] on `time`, and close it with
/// `close_code::PEER_NOT_ACKNOWLEDGING` when [`AckWatch`] says so. Returns
/// once it closed the connection, or once the connection has closed. Runs
/// beside the path's receive task, never in a task of its own.
pub(crate) async fn watch_acknowledgements<T: TimeSource>(
    connection: quinn::Connection,
    closer: QuicCloser,
    time: T,
    cap_holds: Arc<CapHolds>,
) {
    let Some(first) = AckSample::of(&connection, &cap_holds) else {
        tracing::warn!(
            remote = %connection.remote_address(),
            "QUIC path: the connection's congestion controller does not count \
             acknowledgements; the acknowledgement watch is off for it"
        );
        return;
    };
    let mut watch = AckWatch::new(ACK_WATCH_WINDOW, first);
    loop {
        time.sleep(ACK_WATCH_TICK).await;
        if connection.close_reason().is_some() {
            return;
        }
        let Some(sample) = AckSample::of(&connection, &cap_holds) else {
            return;
        };
        if watch.observe(time.now_nanos(), sample) {
            if closer.close(QuicCloseCode::PeerNotAcknowledging) {
                tracing::warn!(
                    remote = %connection.remote_address(),
                    window = ?ACK_WATCH_WINDOW,
                    "QUIC path: the peer kept sending but acknowledged none of this \
                     node's packets; closing the path so the peer falls back to UDP"
                );
            }
            return;
        }
    }
}

#[cfg(test)]
mod tests;
