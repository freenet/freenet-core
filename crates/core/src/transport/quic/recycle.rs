//! When a QUIC path is closed so its peer falls back to the UDP session
//! (#5940, memory-gate step M6).
//!
//! Two conditions close a path, each with its own close code and counted in
//! `quic_close_sent_<label>` ([`QuicCloseCode`]):
//!
//! 1. **Too many locally reset sends** ([`close_code::SEND_ABORT_LIMIT`]).
//!    Each send this side resets leaves an entry in quinn's `stopped` map
//!    until the connection closes (see "Known quinn cost" in the `path`
//!    module doc), so the path counts the resets ([`SendAborts`]) and closes
//!    the connection once there are more than [`SEND_ABORT_LIMIT`]. A new
//!    connection starts with an empty map and a zero count.
//!
//! 2. **A peer that acknowledges nothing**
//!    ([`close_code::PEER_NOT_ACKNOWLEDGING`]). A peer that keeps sending
//!    but acknowledges none of our packets is not a conforming QUIC peer
//!    (RFC 9000 section 13.2.1 requires acknowledging ack-eliciting packets
//!    within `max_ack_delay`), so the path is closed. [`watch_acknowledgements`]
//!    checks it every [`ACK_WATCH_TICK`] on the receive task the path
//!    already has, and closes the path once, for at least
//!    [`ACK_WATCH_WINDOW`], this side has had ack-eliciting packets
//!    outstanding, none of them was acknowledged, and the peer sent
//!    datagrams ([`AckWatch`]).
//!
//! What "acknowledged" is measured with. quinn's `ConnectionStats` has no
//! counter of packets the peer newly acknowledged: `frame_rx.acks` counts
//! ACK frames, including ones that acknowledge nothing new (an ACK frame
//! repeating an old range), so it cannot tell a peer that acknowledges from
//! one that only sends ACK frames. quinn does tell the congestion controller
//! about every ack-eliciting packet the peer newly acknowledges
//! (`Controller::on_ack`, once per packet, quinn-proto 0.11.19
//! `on_packet_acked`), so the transport config wraps Cubic in
//! [`AckCounting`], which counts those calls and otherwise passes
//! everything through. The other two inputs are from `ConnectionStats`:
//! - sent: the ack-eliciting frames this side sent (`frame_tx`, every frame
//!   kind except ACK and CONNECTION_CLOSE; a packet is ack-eliciting exactly
//!   when it carries one of them), [`ack_eliciting_frames`];
//! - the peer kept sending: `udp_rx.datagrams`.
//!
//! What it does not fire on:
//! - an idle path: nothing ack-eliciting is outstanding (a keepalive PING
//!   is acknowledged within one round trip);
//! - a slow but conforming peer: its acknowledgements arrive within a round
//!   trip plus `max_ack_delay` (25 ms), and the window allows a round trip
//!   of several seconds;
//! - the handshake: a path is built on an established connection, and the
//!   window starts no earlier than the path;
//! - a peer that went silent: without datagrams from the peer, quinn's idle
//!   timeout ends the connection instead.
//!
//! It also fires when this side's packets stop reaching a peer whose
//! packets still arrive (a path broken in one direction): from this side
//! that is the same observation, and falling back to UDP is the right
//! outcome there too.
//!
//! Not covered: quinn skips `on_ack` while it validates a new network path
//! (`path.challenge`), and builds a new controller for a migrated
//! connection. Migration is off (`config::server_config`), and a new
//! controller is seen as a new counter and restarts the window, so neither
//! can close a path by itself.
//!
//! [`QuicCloseCode`]: crate::transport::metrics::QuicCloseCode

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use quinn::FrameStats;
use quinn::congestion::{Controller, ControllerFactory, ControllerMetrics};

use super::QuicCloser;
#[cfg(doc)]
use super::error::close_code;
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

/// How long this side may have ack-eliciting packets outstanding, none of
/// them acknowledged, while the peer keeps sending, before the path is
/// closed ([`close_code::PEER_NOT_ACKNOWLEDGING`]).
///
/// A conforming peer acknowledges within a round trip plus its
/// `max_ack_delay` (25 ms by default, quinn's and ours), and a lost
/// acknowledgement is repeated in the next ACK frame or drawn out by a
/// probe after one PTO (about a round trip and a half at a steady RTT,
/// 1 s at quinn's initial estimate). 10 s is several PTOs even at a round
/// trip of 2 s, above geostationary satellite links (about 600 ms) and
/// heavily buffered access links (1 to 2 s), so a conforming peer never
/// reaches it; a path that does is closed and its peer stays on UDP.
pub(crate) const ACK_WATCH_WINDOW: Duration = Duration::from_secs(10);

/// How often the acknowledgement watch samples the connection. A path is
/// closed between [`ACK_WATCH_WINDOW`] and `ACK_WATCH_WINDOW + ACK_WATCH_TICK`
/// after its first unacknowledged send, never earlier.
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

    /// One more send was reset on this side. Past the limit, close the
    /// connection (once; [`QuicCloser`] ignores later calls).
    pub(crate) fn record(&self) {
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
        Box::new(AckCounting {
            inner: self.cubic.clone().build(now, current_mtu),
            acked: Arc::default(),
        })
    }
}

/// A congestion controller that counts the ack-eliciting packets the peer
/// newly acknowledged and passes every call to `inner` unchanged. Its
/// clones (quinn's `Connection::congestion_state`) share the count, so a
/// clone taken once reads it as it grows.
pub(crate) struct AckCounting {
    inner: Box<dyn Controller>,
    acked: Arc<AtomicU64>,
}

impl AckCounting {
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
        self.acked.fetch_add(1, Ordering::Relaxed);
        self.inner.on_ack(now, sent, bytes, app_limited, rtt);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
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
            acked: self.acked.clone(),
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

/// One reading of a connection, for [`AckWatch`].
#[derive(Clone)]
pub(crate) struct AckSample {
    /// The controller's count, to notice a new controller.
    counter: Arc<AtomicU64>,
    /// Ack-eliciting packets the peer newly acknowledged.
    acked: u64,
    /// Ack-eliciting frames this side sent.
    sent: u64,
    /// Datagrams received from the peer.
    received: u64,
}

impl AckSample {
    /// The connection's counts now, or `None` if its controller is not an
    /// [`AckCounting`] (a config that does not install it).
    pub(crate) fn of(connection: &quinn::Connection) -> Option<Self> {
        let counter = connection
            .congestion_state()
            .into_any()
            .downcast::<AckCounting>()
            .ok()?
            .acked;
        let stats = connection.stats();
        Some(Self {
            acked: counter.load(Ordering::Relaxed),
            counter,
            sent: ack_eliciting_frames(&stats.frame_tx),
            received: stats.udp_rx.datagrams,
        })
    }

    #[cfg(test)]
    pub(crate) fn synthetic(
        counter: &Arc<AtomicU64>,
        acked: u64,
        sent: u64,
        received: u64,
    ) -> Self {
        Self {
            counter: counter.clone(),
            acked,
            sent,
            received,
        }
    }
}

/// Decides, from samples taken over time, whether the peer has
/// acknowledged none of this side's ack-eliciting packets for the window
/// while it kept sending.
pub(crate) struct AckWatch {
    window_nanos: u64,
    /// The last sample at which nothing was outstanding or the peer had
    /// acknowledged something new.
    base: AckSample,
    /// When the first sample with unacknowledged sends was taken, and the
    /// datagrams received by then.
    since: Option<(u64, u64)>,
}

impl AckWatch {
    pub(crate) fn new(window: Duration, first: AckSample) -> Self {
        Self {
            window_nanos: u64::try_from(window.as_nanos()).unwrap_or(u64::MAX),
            base: first,
            since: None,
        }
    }

    /// Take `sample`, taken at `now_nanos`; whether the path must close.
    ///
    /// The window starts at the first sample that sees a send not
    /// acknowledged, so it starts after that send and the path is never
    /// closed less than a window after it. Any new acknowledgement (or a
    /// new controller) ends the window; so does a sample with nothing sent
    /// since the base, as nothing is then waiting for an acknowledgement.
    pub(crate) fn observe(&mut self, now_nanos: u64, sample: AckSample) -> bool {
        let progressed = !Arc::ptr_eq(&sample.counter, &self.base.counter)
            || sample.acked != self.base.acked
            || sample.sent == self.base.sent;
        if progressed {
            self.base = sample;
            self.since = None;
            return false;
        }
        let (start, received_at_start) = *self.since.get_or_insert((now_nanos, sample.received));
        now_nanos.saturating_sub(start) >= self.window_nanos && sample.received > received_at_start
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
) {
    let Some(first) = AckSample::of(&connection) else {
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
        let Some(sample) = AckSample::of(&connection) else {
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
mod tests {
    use super::*;

    const SEC: u64 = 1_000_000_000;

    /// Feed `watch` one sample per tick from `samples(tick)`, from tick 1
    /// to `ticks`; the first tick (in seconds) at which it said close.
    fn run(
        ticks: u64,
        tick_secs: u64,
        mut samples: impl FnMut(u64) -> (u64, u64, u64),
    ) -> Option<u64> {
        let counter = Arc::new(AtomicU64::new(0));
        let (acked, sent, received) = samples(0);
        let mut watch = AckWatch::new(
            ACK_WATCH_WINDOW,
            AckSample::synthetic(&counter, acked, sent, received),
        );
        (1..=ticks).find(|tick| {
            let (acked, sent, received) = samples(*tick);
            watch.observe(
                tick * tick_secs * SEC,
                AckSample::synthetic(&counter, acked, sent, received),
            )
        })
    }

    /// Sends never acknowledged while the peer keeps sending: closed at
    /// the first sample a full window after the first one that saw them,
    /// and not before.
    #[test]
    fn unacknowledged_sends_while_the_peer_sends_close_after_the_window() {
        // A send before tick 1, never acknowledged; the peer sends a
        // datagram every second.
        let closed = run(100, 1, |t| (5, 10 + t.min(1), 100 + t));
        assert_eq!(closed, Some(1 + ACK_WATCH_WINDOW.as_secs()));
    }

    /// The same, but the peer is silent: never closed here (quinn's idle
    /// timeout ends that connection).
    #[test]
    fn a_silent_peer_is_left_to_the_idle_timeout() {
        assert_eq!(run(100, 1, |t| (5, 10 + t, 100)), None);
    }

    /// Nothing sent: idle, never closed, however much the peer sends.
    #[test]
    fn an_idle_path_is_never_closed() {
        assert_eq!(run(1000, 1, |t| (5, 10, 100 + t)), None);
    }

    /// A keepalive every 15 s, acknowledged one round trip (here up to 9 s)
    /// later, with the peer sending throughout: never closed.
    #[test]
    fn a_slow_conforming_peer_is_never_closed() {
        for rtt in [1, 3, 9] {
            let closed = run(1000, 1, |t| {
                let pings = t / 15 + 1;
                let acked = t.saturating_sub(rtt) / 15 + u64::from(t >= rtt);
                (acked, pings, t)
            });
            assert_eq!(closed, None, "rtt {rtt} s");
        }
    }

    /// A busy conforming peer: sends every tick, acknowledgements every
    /// tick: never closed.
    #[test]
    fn a_busy_conforming_peer_is_never_closed() {
        assert_eq!(run(1000, 2, |t| (t * 7, t * 9, t * 11)), None);
    }

    /// One acknowledgement inside the window restarts it.
    #[test]
    fn an_acknowledgement_restarts_the_window() {
        // Acknowledged once, at tick 8; otherwise as in the first test.
        let closed = run(100, 1, |t| (5 + u64::from(t >= 8), 10 + t, 100 + t));
        // Tick 8 resets; tick 9 starts the window again.
        assert_eq!(closed, Some(9 + ACK_WATCH_WINDOW.as_secs()));
    }

    /// A new controller (a new counter) restarts the window too.
    #[test]
    fn a_new_controller_restarts_the_window() {
        let first = Arc::new(AtomicU64::new(0));
        let second = Arc::new(AtomicU64::new(0));
        let mut watch = AckWatch::new(ACK_WATCH_WINDOW, AckSample::synthetic(&first, 0, 0, 0));
        assert!(!watch.observe(SEC, AckSample::synthetic(&first, 0, 1, 1)));
        assert!(!watch.observe(5 * SEC, AckSample::synthetic(&second, 0, 2, 2)));
        assert!(!watch.observe(6 * SEC, AckSample::synthetic(&second, 0, 3, 3)));
        assert!(!watch.observe(15 * SEC, AckSample::synthetic(&second, 0, 4, 4)));
        assert!(watch.observe(16 * SEC, AckSample::synthetic(&second, 0, 5, 5)));
    }

    /// Every frame kind except ACK and CONNECTION_CLOSE counts as
    /// ack-eliciting.
    #[test]
    fn ack_and_close_frames_are_not_ack_eliciting() {
        let mut frames = FrameStats::default();
        frames.acks = 1000;
        frames.connection_close = 1000;
        assert_eq!(ack_eliciting_frames(&frames), 0);
        frames.ping = 1;
        frames.stream = 2;
        frames.max_streams_uni = 3;
        frames.handshake_done = 4;
        assert_eq!(ack_eliciting_frames(&frames), 10);
    }
}
