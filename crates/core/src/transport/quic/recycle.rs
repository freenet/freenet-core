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
//! - a peer that went silent: quinn's idle timeout ends the connection
//!   instead;
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
//! latest quinn reports to `on_ack`, which for the first batch of
//! acknowledgements is still quinn's initial estimate (333 ms,
//! `config::transport_config`), so the cap is loose for that one batch.
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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use quinn::FrameStats;
use quinn::congestion::{Controller, ControllerFactory, ControllerMetrics};

use super::QuicCloser;
use super::config::QuicRateLimit;
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

/// Builds [`CappedCubic`] controllers around Cubic, capped by `rate`.
#[derive(Debug, Default)]
pub(crate) struct CappedCubicFactory {
    cubic: Arc<quinn::congestion::CubicConfig>,
    rate: QuicRateLimit,
}

impl CappedCubicFactory {
    pub(crate) fn new(rate: QuicRateLimit) -> Self {
        Self {
            cubic: Arc::default(),
            rate,
        }
    }
}

impl ControllerFactory for CappedCubicFactory {
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        #[cfg(feature = "bench")]
        let this = {
            let iw = lab::INITIAL_WINDOW.load(Ordering::Relaxed);
            if iw > 0 {
                let mut cubic = quinn::congestion::CubicConfig::default();
                cubic.initial_window(iw);
                Arc::new(Self {
                    cubic: Arc::new(cubic),
                    rate: self.rate.clone(),
                })
            } else {
                self
            }
        };
        #[cfg(not(feature = "bench"))]
        let this = self;
        #[cfg(feature = "bench")]
        if lab::INNER_CONTROLLER.load(Ordering::Relaxed) == lab::VENO {
            return Box::new(CappedCubic::new(
                Box::new(lab::VenoCubic::new(
                    this.cubic.clone().build(now, current_mtu),
                    current_mtu,
                )),
                this.rate.clone(),
                current_mtu,
            ));
        }
        #[cfg(feature = "bench")]
        match lab::INNER_CONTROLLER.load(Ordering::Relaxed) {
            lab::WESTWOOD => {
                return Box::new(CappedCubic::new(
                    Box::new(lab::Westwood::new(
                        this.cubic.clone().build(now, current_mtu),
                        current_mtu,
                    )),
                    this.rate.clone(),
                    current_mtu,
                ));
            }
            lab::MODEL => {
                return Box::new(CappedCubic::new(
                    Box::new(lab::Model::new(
                        now,
                        current_mtu,
                        this.cubic.clone().build(now, current_mtu).initial_window(),
                    )),
                    this.rate.clone(),
                    current_mtu,
                ));
            }
            lab::GATED => {
                return Box::new(CappedCubic::new(
                    Box::new(lab::DelayGated::new(
                        this.cubic.clone().build(now, current_mtu),
                    )),
                    this.rate.clone(),
                    current_mtu,
                ));
            }
            _ => {}
        }
        #[cfg(feature = "bench")]
        if lab::INNER_CONTROLLER.load(Ordering::Relaxed) == lab::BBR {
            return Box::new(CappedCubic::new(
                {
                    let mut bbr = quinn::congestion::BbrConfig::default();
                    let iw = lab::INITIAL_WINDOW.load(Ordering::Relaxed);
                    if iw > 0 {
                        bbr.initial_window(iw);
                    }
                    Arc::new(bbr).build(now, current_mtu)
                },
                this.rate.clone(),
                current_mtu,
            ));
        }
        Box::new(CappedCubic::new(
            this.cubic.clone().build(now, current_mtu),
            this.rate.clone(),
            current_mtu,
        ))
    }
}

/// Bench-only switches for the QUIC lab (`quic::lab`). Nothing in a node
/// sets them.
#[cfg(feature = "bench")]
pub(crate) mod lab {
    use std::sync::atomic::AtomicU8;

    /// The controller [`super::CappedCubicFactory`] wraps: [`CUBIC`] (the
    /// node's) or [`BBR`]. The cap and the acknowledgement record wrap
    /// either one the same way.
    pub(crate) static INNER_CONTROLLER: AtomicU8 = AtomicU8::new(CUBIC);
    pub(crate) const CUBIC: u8 = 0;
    pub(crate) const BBR: u8 = 1;
    /// Cubic inside [`VenoCubic`].
    pub(crate) const VENO: u8 = 2;
    /// Cubic inside [`Westwood`].
    pub(crate) const WESTWOOD: u8 = 3;
    /// Cubic inside [`DelayGated`].
    pub(crate) const GATED: u8 = 4;
    /// [`Model`], no Cubic.
    pub(crate) const MODEL: u8 = 5;

    /// [`Model`]: a round whose share of bytes lost is at least this many
    /// thousandths counts as congested (BBRv2 and v3 use 20, 2%).
    pub(crate) static MODEL_LOSS_PERMILLE: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(150);

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Mode {
        Startup,
        Drain,
        ProbeBw,
    }

    /// Lab prototype of a model-based controller in the manner of BBRv2/v3,
    /// sized for quinn's window-driven pacer.
    ///
    /// - Bandwidth: one delivery-rate sample per round (bytes acknowledged
    ///   over at least one minimum RTT), max over the last 10 rounds.
    /// - Minimum RTT: min of `now - sent` over 10 s.
    /// - Window: gain x bandwidth x minimum RTT, at least 4 datagrams. quinn
    ///   paces at 1.25 x window / srtt, so a gain of 1 paces at about 1.25 x
    ///   the estimate while no queue has built.
    /// - Startup doubles each round (gain 2) until the estimate grows by less
    ///   than 25% for 3 rounds, or a round is congested; Drain then runs one
    ///   round at gain 0.75; ProbeBw cycles 1.25, 0.75, then 1 for 6 rounds.
    /// - Loss: random loss below [`MODEL_LOSS_PERMILLE`] per round is not a
    ///   signal. A congested round caps the window at 0.7 x what was in flight
    ///   (`inflight_hi`); each round in ProbeBw's up phase without congestion
    ///   raises the cap by a quarter, and it is dropped once it is above the
    ///   model's window. Persistent congestion resets to 4 datagrams.
    pub(crate) struct Model {
        mtu: u16,
        mode: Mode,
        min_rtt: Option<Duration>,
        min_rtt_at: Instant,
        bw_samples: std::collections::VecDeque<f64>,
        round_start: Instant,
        round_acked: u64,
        round_lost: u64,
        in_flight: u64,
        full_bw: f64,
        full_bw_rounds: u32,
        cycle: usize,
        inflight_hi: Option<u64>,
        initial: u64,
        congested_round: bool,
    }

    const PROBE_GAINS: [f64; 8] = [1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];

    impl Model {
        pub(crate) fn new(now: Instant, mtu: u16, initial: u64) -> Self {
            Self {
                mtu,
                mode: Mode::Startup,
                min_rtt: None,
                min_rtt_at: now,
                bw_samples: Default::default(),
                round_start: now,
                round_acked: 0,
                round_lost: 0,
                in_flight: 0,
                full_bw: 0.0,
                full_bw_rounds: 0,
                cycle: 0,
                inflight_hi: None,
                initial,
                congested_round: false,
            }
        }

        fn bw(&self) -> f64 {
            self.bw_samples.iter().copied().fold(0.0, f64::max)
        }

        fn gain(&self) -> f64 {
            match self.mode {
                Mode::Startup => 2.0,
                Mode::Drain => 0.75,
                Mode::ProbeBw => PROBE_GAINS[self.cycle % PROBE_GAINS.len()],
            }
        }

        fn end_round(&mut self, now: Instant, span: Duration, app_limited: bool) {
            let total = self.round_acked + self.round_lost;
            let share = if total > 0 {
                self.round_lost as f64 / total as f64
            } else {
                0.0
            };
            let congested = share * 1000.0
                >= MODEL_LOSS_PERMILLE.load(std::sync::atomic::Ordering::Relaxed) as f64
                && self.round_lost > 0;
            if !app_limited && span > Duration::ZERO {
                let rate = self.round_acked as f64 / span.as_secs_f64();
                // An app-limited round may only raise the estimate.
                self.bw_samples.push_back(rate);
                if self.bw_samples.len() > 10 {
                    self.bw_samples.pop_front();
                }
            }
            if congested {
                let hi = (self.in_flight.max(self.round_acked) as f64 * 0.7) as u64;
                self.inflight_hi = Some(hi.max(4 * u64::from(self.mtu)));
            }
            match self.mode {
                Mode::Startup => {
                    let bw = self.bw();
                    if bw >= self.full_bw * 1.25 {
                        self.full_bw = bw;
                        self.full_bw_rounds = 0;
                    } else {
                        self.full_bw_rounds += 1;
                    }
                    if self.full_bw_rounds >= 3 || congested {
                        self.mode = Mode::Drain;
                    }
                }
                Mode::Drain => {
                    self.mode = Mode::ProbeBw;
                    self.cycle = 2;
                }
                Mode::ProbeBw => {
                    if PROBE_GAINS[self.cycle % PROBE_GAINS.len()] > 1.0 && !congested {
                        if let Some(hi) = self.inflight_hi {
                            let hi = hi + hi / 4;
                            self.inflight_hi = (hi < self.model_window()).then_some(hi);
                        }
                    }
                    self.cycle += 1;
                }
            }
            self.congested_round = congested;
            self.round_start = now;
            self.round_acked = 0;
            self.round_lost = 0;
        }

        fn model_window(&self) -> u64 {
            let floor = 4 * u64::from(self.mtu);
            match self.min_rtt {
                Some(min) if self.bw() > 0.0 => {
                    ((self.gain() * self.bw() * min.as_secs_f64()) as u64).max(floor)
                }
                _ => self.initial.max(floor),
            }
        }
    }

    impl Controller for Model {
        fn on_sent(&mut self, _now: Instant, bytes: u64, _last_packet_number: u64) {
            self.in_flight += bytes;
        }

        fn on_ack(
            &mut self,
            now: Instant,
            sent: Instant,
            bytes: u64,
            app_limited: bool,
            _rtt: &quinn_proto::RttEstimator,
        ) {
            self.in_flight = self.in_flight.saturating_sub(bytes);
            let sample = now.saturating_duration_since(sent);
            let stale = now.saturating_duration_since(self.min_rtt_at) > Duration::from_secs(10);
            if self.min_rtt.is_none_or(|m| sample <= m) || stale {
                self.min_rtt = Some(sample);
                self.min_rtt_at = now;
            }
            self.round_acked += bytes;
            let span = now.saturating_duration_since(self.round_start);
            if span >= self.min_rtt.unwrap_or(sample).max(Duration::from_millis(5)) {
                self.end_round(now, span, app_limited);
            }
        }

        fn on_end_acks(
            &mut self,
            _now: Instant,
            in_flight: u64,
            _app_limited: bool,
            _largest_packet_num_acked: Option<u64>,
        ) {
            self.in_flight = in_flight;
        }

        fn on_congestion_event(
            &mut self,
            _now: Instant,
            _sent: Instant,
            is_persistent_congestion: bool,
            lost_bytes: u64,
        ) {
            self.round_lost += lost_bytes;
            if is_persistent_congestion {
                // Start over from the initial window, as Cubic would from
                // its minimum: the path may have changed.
                self.bw_samples.clear();
                self.inflight_hi = None;
                self.mode = Mode::Startup;
                self.full_bw = 0.0;
                self.full_bw_rounds = 0;
            }
        }

        fn on_mtu_update(&mut self, new_mtu: u16) {
            self.mtu = new_mtu;
        }

        fn window(&self) -> u64 {
            let model = self.model_window();
            match self.inflight_hi {
                Some(hi) => model.min(hi),
                None => model,
            }
        }

        fn metrics(&self) -> ControllerMetrics {
            let mut metrics = ControllerMetrics::default();
            metrics.congestion_window = self.window();
            metrics
        }

        fn clone_box(&self) -> Box<dyn Controller> {
            Box::new(Self {
                mtu: self.mtu,
                mode: self.mode,
                min_rtt: self.min_rtt,
                min_rtt_at: self.min_rtt_at,
                bw_samples: self.bw_samples.clone(),
                round_start: self.round_start,
                round_acked: self.round_acked,
                round_lost: self.round_lost,
                in_flight: self.in_flight,
                full_bw: self.full_bw,
                full_bw_rounds: self.full_bw_rounds,
                cycle: self.cycle,
                inflight_hi: self.inflight_hi,
                initial: self.initial,
                congested_round: self.congested_round,
            })
        }

        fn initial_window(&self) -> u64 {
            self.initial
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any> {
            self
        }
    }

    /// [`DelayGated`]: a loss is taken as congestion when the smoothed RTT is
    /// at least this many thousandths of the minimum RTT above it...
    pub(crate) static GATE_DELAY_PERMILLE: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(100);
    /// ...or at least this many microseconds above it...
    pub(crate) static GATE_MIN_DELAY_US: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(2_000);
    /// ...or when the share of bytes lost over the last round trip is at
    /// least this many thousandths.
    /// Judge the queue on the smoothed RTT (the first version) instead of
    /// the minimum sample over the last two rounds.
    pub(crate) static GATE_USE_SRTT: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);
    pub(crate) static GATE_LOSS_CEIL_PERMILLE: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(250);

    /// [`DelayGated`] decisions across every connection, for the lab's
    /// reports: passed for persistent congestion, for the round's loss
    /// share, for the queue delay; ignored.
    pub static GATE_COUNTS: [std::sync::atomic::AtomicU64; 4] =
        [const { std::sync::atomic::AtomicU64::new(0) }; 4];

    /// Lab prototype: Cubic that ignores a loss when no queue is building.
    ///
    /// A drop-tail bottleneck drops when its queue is full, so a congestion
    /// loss comes with the RTT above its minimum by the queue's delay. A
    /// random loss comes with no queue. So a loss is passed to Cubic only when
    /// the smoothed RTT is above the minimum by [`GATE_DELAY_PERMILLE`] of it
    /// or [`GATE_MIN_DELAY_US`], or when the round's loss share is at least
    /// [`GATE_LOSS_CEIL_PERMILLE`] (a policer, or a path too lossy to push).
    /// Persistent congestion is always passed.
    pub(crate) struct DelayGated {
        inner: Box<dyn Controller>,
        min_rtt: Option<Duration>,
        srtt: Option<Duration>,
        /// The smallest RTT sample in this round and in the last one. Jitter
        /// only adds delay, so the minimum over a round filters it, while a
        /// standing queue raises every sample, the minimum too.
        round_min: Option<Duration>,
        last_round_min: Option<Duration>,
        round_start: Option<Instant>,
        round_acked: u64,
        round_lost: u64,
        last_round_share: f64,
        /// Losses passed to Cubic and ignored, for the lab's reports.
        pub(crate) passed: u64,
        pub(crate) ignored: u64,
    }

    impl DelayGated {
        pub(crate) fn new(inner: Box<dyn Controller>) -> Self {
            Self {
                inner,
                min_rtt: None,
                srtt: None,
                round_min: None,
                last_round_min: None,
                round_start: None,
                round_acked: 0,
                round_lost: 0,
                last_round_share: 0.0,
                passed: 0,
                ignored: 0,
            }
        }

        fn roll(&mut self, now: Instant) {
            let round = self.srtt.unwrap_or(Duration::from_millis(100));
            match self.round_start {
                Some(start) if now.saturating_duration_since(start) < round => {}
                _ => {
                    // A smoothed share (1/4 new) from rounds of at least 16
                    // datagrams: a small window's share is too noisy to
                    // read congestion from (2 lost of 8 is 25%).
                    let total = self.round_acked + self.round_lost;
                    if total >= 16 * 1200 {
                        let share = self.round_lost as f64 / total as f64;
                        self.last_round_share = 0.75 * self.last_round_share + 0.25 * share;
                    }
                    self.round_start = Some(now);
                    self.round_acked = 0;
                    self.round_lost = 0;
                    if self.round_min.is_some() {
                        self.last_round_min = self.round_min.take();
                    }
                }
            }
        }

        /// 1 for the loss share, 2 for the queue, 0 for neither.
        fn congested(&self) -> usize {
            let share = self.last_round_share;
            if share * 1000.0
                >= GATE_LOSS_CEIL_PERMILLE.load(std::sync::atomic::Ordering::Relaxed) as f64
            {
                return 1;
            }
            let recent = match (self.round_min, self.last_round_min) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            let recent = if GATE_USE_SRTT.load(std::sync::atomic::Ordering::Relaxed) {
                self.srtt
            } else {
                recent
            };
            let (Some(srtt), Some(min)) = (recent, self.min_rtt) else {
                return 2;
            };
            let queue = srtt.saturating_sub(min);
            let rel = min.mul_f64(
                GATE_DELAY_PERMILLE.load(std::sync::atomic::Ordering::Relaxed) as f64 / 1000.0,
            );
            let abs =
                Duration::from_micros(GATE_MIN_DELAY_US.load(std::sync::atomic::Ordering::Relaxed));
            if queue >= rel.max(abs) { 2 } else { 0 }
        }
    }

    impl Controller for DelayGated {
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
            let sample = now.saturating_duration_since(sent);
            self.min_rtt = Some(self.min_rtt.map_or(sample, |m| m.min(sample)));
            self.srtt = Some(rtt.get());
            self.roll(now);
            self.round_min = Some(self.round_min.map_or(sample, |m| m.min(sample)));
            self.round_acked += bytes;
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
            self.roll(now);
            self.round_lost += lost_bytes;
            let why = if is_persistent_congestion {
                0
            } else {
                self.congested()
            };
            let reason = if is_persistent_congestion {
                0
            } else if why == 0 {
                3
            } else {
                why
            };
            GATE_COUNTS[reason].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if reason != 3 {
                self.passed += 1;
                self.inner
                    .on_congestion_event(now, sent, is_persistent_congestion, lost_bytes);
            } else {
                self.ignored += 1;
            }
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
                min_rtt: self.min_rtt,
                srtt: self.srtt,
                round_min: self.round_min,
                last_round_min: self.last_round_min,
                round_start: self.round_start,
                round_acked: self.round_acked,
                round_lost: self.round_lost,
                last_round_share: self.last_round_share,
                passed: self.passed,
                ignored: self.ignored,
            })
        }

        fn initial_window(&self) -> u64 {
            self.inner.initial_window()
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any> {
            self
        }
    }

    /// Lab prototype: Cubic with a Westwood+ floor. A bandwidth estimate is
    /// kept from the acknowledged bytes (one sample per round trip, filtered
    /// 7/8 old, 1/8 new); at a congestion event Cubic backs off as usual, but
    /// the window is held at no less than estimate x minimum RTT until the
    /// next event. Under random loss the estimate is the path's delivery, so
    /// the window stays near what the path carries instead of collapsing;
    /// under congestion the estimate is the bottleneck's share and estimate x
    /// minimum RTT drains the queue.
    pub(crate) struct Westwood {
        inner: Box<dyn Controller>,
        mtu: u16,
        min_rtt: Option<Duration>,
        srtt: Option<Duration>,
        sample_start: Option<Instant>,
        sample_bytes: u64,
        bwe: f64,
        floor: u64,
    }

    impl Westwood {
        pub(crate) fn new(inner: Box<dyn Controller>, mtu: u16) -> Self {
            Self {
                inner,
                mtu,
                min_rtt: None,
                srtt: None,
                sample_start: None,
                sample_bytes: 0,
                bwe: 0.0,
                floor: 0,
            }
        }
    }

    impl Controller for Westwood {
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
            let sample = now.saturating_duration_since(sent);
            self.min_rtt = Some(self.min_rtt.map_or(sample, |m| m.min(sample)));
            self.srtt = Some(rtt.get());
            let start = *self.sample_start.get_or_insert(now);
            self.sample_bytes += bytes;
            let span = now.saturating_duration_since(start);
            let round = self.srtt.unwrap_or(sample).max(Duration::from_millis(10));
            if span >= round && !app_limited {
                let rate = self.sample_bytes as f64 / span.as_secs_f64();
                self.bwe = if self.bwe == 0.0 {
                    rate
                } else {
                    0.875 * self.bwe + 0.125 * rate
                };
                self.sample_start = Some(now);
                self.sample_bytes = 0;
            } else if span >= round {
                self.sample_start = Some(now);
                self.sample_bytes = 0;
            }
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
            self.floor = match (is_persistent_congestion, self.min_rtt) {
                (false, Some(min)) if self.bwe > 0.0 => {
                    ((self.bwe * min.as_secs_f64()) as u64).max(4 * u64::from(self.mtu))
                }
                _ => 0,
            };
        }

        fn on_mtu_update(&mut self, new_mtu: u16) {
            self.mtu = new_mtu;
            self.inner.on_mtu_update(new_mtu);
        }

        fn window(&self) -> u64 {
            self.inner.window().max(self.floor)
        }

        fn metrics(&self) -> ControllerMetrics {
            let mut metrics = self.inner.metrics();
            metrics.congestion_window = self.window();
            metrics
        }

        fn clone_box(&self) -> Box<dyn Controller> {
            Box::new(Self {
                inner: self.inner.clone_box(),
                mtu: self.mtu,
                min_rtt: self.min_rtt,
                srtt: self.srtt,
                sample_start: self.sample_start,
                sample_bytes: self.sample_bytes,
                bwe: self.bwe,
                floor: self.floor,
            })
        }

        fn initial_window(&self) -> u64 {
            self.inner.initial_window()
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any> {
            self
        }
    }

    use std::any::Any;
    use std::time::{Duration, Instant};

    use quinn::congestion::{Controller, ControllerMetrics};

    /// Lab prototype, not for a node: Cubic that backs off less on a loss
    /// that does not look like congestion, in the manner of TCP Veno.
    ///
    /// At each loss it estimates the bytes this connection has queued in the
    /// network, `window x (srtt - min_rtt) / srtt` (Veno's backlog, N). Below
    /// three datagrams the loss is taken as random: no queue is building, so
    /// the path did not ask for less. Then only every other window reduction
    /// Cubic would make is passed to it, an effective multiplicative decrease
    /// of sqrt(0.7), about 0.84, instead of Cubic's 0.7. At three datagrams
    /// or more every reduction is passed, so on a filling queue it is Cubic.
    /// quinn's Cubic has no settable decrease, so whether a loss would reduce
    /// the window is found on a clone of it.
    pub(crate) struct VenoCubic {
        inner: Box<dyn Controller>,
        mtu: u16,
        srtt: Option<Duration>,
        min_rtt: Option<Duration>,
        /// The next reduction a random loss would cause is skipped.
        skip_next: bool,
    }

    impl VenoCubic {
        pub(crate) fn new(inner: Box<dyn Controller>, mtu: u16) -> Self {
            Self {
                inner,
                mtu,
                srtt: None,
                min_rtt: None,
                skip_next: true,
            }
        }

        /// Whether the connection's queued bytes are under three datagrams.
        fn loss_looks_random(&self) -> bool {
            let (Some(srtt), Some(min)) = (self.srtt, self.min_rtt) else {
                return false;
            };
            if srtt.is_zero() {
                return false;
            }
            let queued = self.inner.window() as f64 * srtt.saturating_sub(min).as_secs_f64()
                / srtt.as_secs_f64();
            queued < 3.0 * f64::from(self.mtu)
        }
    }

    impl Controller for VenoCubic {
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
            self.srtt = Some(rtt.get());
            self.min_rtt = Some(rtt.min());
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
            if !is_persistent_congestion && self.loss_looks_random() {
                let mut probe = self.inner.clone_box();
                probe.on_congestion_event(now, sent, false, lost_bytes);
                if probe.window() < self.inner.window() {
                    self.skip_next = !self.skip_next;
                    if !self.skip_next {
                        return;
                    }
                }
            }
            self.inner
                .on_congestion_event(now, sent, is_persistent_congestion, lost_bytes);
        }

        fn on_mtu_update(&mut self, new_mtu: u16) {
            self.mtu = new_mtu;
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
                mtu: self.mtu,
                srtt: self.srtt,
                min_rtt: self.min_rtt,
                skip_next: self.skip_next,
            })
        }

        fn initial_window(&self) -> u64 {
            self.inner.initial_window()
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any> {
            self
        }
    }

    /// A floor under the inner controller's window, in bytes, applied before
    /// the cap: a loss-tolerant variant to try against random loss. 0 is
    /// none (the node's).
    pub(crate) static MIN_WINDOW: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);

    /// The inner controller's initial window in bytes, for connections built
    /// after it is set; 0 keeps quinn's default (10 datagrams of 1200).
    pub(crate) static INITIAL_WINDOW: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);

    /// A rate start: until the connection's first congestion event, the
    /// window is at least this rate (bytes per second) times the smoothed
    /// RTT, so quinn's pacer (1.25 x window / srtt) sends at about 1.25
    /// times it from the first round trip. 0 is none (the node's).
    pub(crate) static RATE_START: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);
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
    /// Bench only: the connection has had a congestion event, which ends
    /// the lab's rate start (`lab::RATE_START`).
    #[cfg(feature = "bench")]
    congested: bool,
    /// Bench only: the latest RTT sample, `now - sent` of the latest packet
    /// acknowledged. quinn updates its smoothed RTT after the controller's
    /// `on_ack` for the same ACK frame, so after the handshake `srtt` above
    /// still holds the configured initial RTT (333 ms) until the first data
    /// is acknowledged; the sample is the path's real RTT from the first ACK.
    #[cfg(feature = "bench")]
    sample: Option<Duration>,
}

impl CappedCubic {
    pub(crate) fn new(inner: Box<dyn Controller>, rate: QuicRateLimit, mtu: u16) -> Self {
        Self {
            inner,
            state: Arc::default(),
            rate,
            srtt: None,
            mtu,
            #[cfg(feature = "bench")]
            congested: false,
            #[cfg(feature = "bench")]
            sample: None,
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

    /// Whether the wrapped controller is a `C`.
    #[cfg(test)]
    pub(crate) fn wraps<C: Controller + 'static>(&self) -> bool {
        self.inner.clone_box().into_any().downcast::<C>().is_ok()
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
        self.state.lock().acked += 1;
        self.srtt = Some(rtt.get());
        #[cfg(feature = "bench")]
        {
            self.sample = Some(now.saturating_duration_since(sent));
        }
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
        #[cfg(feature = "bench")]
        {
            self.congested = true;
        }
        self.inner
            .on_congestion_event(now, sent, is_persistent_congestion, lost_bytes);
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.mtu = new_mtu;
        self.inner.on_mtu_update(new_mtu);
    }

    fn window(&self) -> u64 {
        #[cfg(feature = "bench")]
        let inner = self
            .inner
            .window()
            .max(lab::MIN_WINDOW.load(Ordering::Relaxed));
        #[cfg(feature = "bench")]
        let inner = match (lab::RATE_START.load(Ordering::Relaxed), self.sample) {
            (rate, Some(srtt)) if rate > 0 && !self.congested => {
                inner.max(rate_window(rate, srtt, self.mtu))
            }
            _ => inner,
        };
        #[cfg(not(feature = "bench"))]
        let inner = self.inner.window();
        self.capped(inner)
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
            #[cfg(feature = "bench")]
            congested: self.congested,
            #[cfg(feature = "bench")]
            sample: self.sample,
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
    /// This connection's QUIC transmits the egress cap holds.
    cap: CapHoldReading,
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
    pub(crate) fn of(connection: &quinn::Connection, cap_holds: &CapHolds) -> Option<Self> {
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
    cap_holds: Arc<CapHolds>,
    heard: Arc<PeerHeard>,
    report: &mut PathReport,
) {
    let Some(first) = AckSample::of(&connection, &cap_holds) else {
        tracing::warn!(
            remote = %connection.remote_address(),
            "QUIC path: the connection's congestion controller does not count \
             acknowledgements; the acknowledgement watch is off for it"
        );
        return;
    };
    let mut received = first.received;
    let mut watch = AckWatch::new(ACK_WATCH_WINDOW, first);
    loop {
        time.sleep(ACK_WATCH_TICK).await;
        // Read before the counts, so a closed connection's counts are its
        // final ones.
        let closed = connection.close_reason().is_some();
        report.report();
        if closed {
            return;
        }
        let Some(sample) = AckSample::of(&connection, &cap_holds) else {
            return;
        };
        if sample.received != received {
            received = sample.received;
            heard.report();
        }
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
mod cap_tests;

/// What a path's receive supervisor keeps for its acknowledgement watch.
pub(crate) struct AckWatchParts {
    pub(crate) cap_holds: Arc<CapHolds>,
    pub(crate) heard: Arc<PeerHeard>,
    pub(crate) report: PathReport,
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
    /// The next report that finds the path in a bucket panics, once
    /// (supervisor test).
    #[cfg(test)]
    pub(crate) panic_once_bucketed: bool,
}

impl PathReport {
    pub(crate) fn new(
        connection: quinn::Connection,
        node: &'static crate::transport::metrics::QuicDataCounters,
    ) -> Self {
        let stats = connection.stats();
        Self {
            bytes: UdpBytes::of(&stats),
            loss: PathLoss::of(&stats),
            window: crate::transport::metrics::PathLossWindow::new(node),
            connection,
            node,
            #[cfg(test)]
            panic_once_bucketed: false,
        }
    }

    /// Report the change since the last report, from one read of the stats.
    fn report(&mut self) {
        let stats = self.connection.stats();
        let (next, sent, got) = self.bytes.step(UdpBytes::of(&stats));
        self.node.record_bytes(sent, got);
        self.bytes = next;
        let (next, delta) = self.loss.step(PathLoss::of(&stats));
        self.node.record_path_loss(&mut self.window, delta);
        self.loss = next;
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
            self.report();
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
