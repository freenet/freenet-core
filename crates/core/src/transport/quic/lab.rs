//! A real-time lab pair for measuring a QUIC path: two endpoints on loopback
//! UDP sockets, an optional shaped link between them, and the two
//! measurements the e5b-9 lab needs (stream fairness on a runtime of the
//! caller's choosing, and single transfers for throughput and CPU per byte).
//!
//! Built only with the `bench` feature, for `benches/transport/quic_lab.rs`.
//! Nothing in a node uses it.
//!
//! The paths are built the way a node builds them ([`QuicPath::with_budget`]
//! with one peer's inbound allowance and [`QuicPathLimits::DEFAULT`]), and
//! every send goes through [`QuicPath::send_stream_bounded`] under
//! [`SendDeadlines::stream`], the node's deadlines. What is not the node's:
//! there is no UDP session or upgrade, so the route layer above the path is
//! not measured.
//!
//! # The shaped link
//!
//! [`LabSocket`] runs each datagram a socket sends through a
//! [`LinkModel`](crate::transport::link_model::LinkModel) on real time: a
//! bottleneck at the sender's uplink with a drop-tail queue, then a
//! propagation delay. The socket accepts the datagram at once, as a NIC queue
//! does, and a task sends it on the real socket when the model says it
//! arrives. Tokio's timers fire at millisecond granularity, so arrivals come
//! in bunches of up to a millisecond; the average rate is the model's.

use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;

use super::config::{QuicRateLimit, connection_window_bytes};
use super::path::{BoundedSend, QuicPath, QuicPathLimits, SendDeadlines, data_channels};
use super::{QuicEndpoint, QuicSettings};
use crate::simulation::RealTime;
use crate::transport::boundary::StreamProgressObserver;
use crate::transport::demux::DemuxSocket;
use crate::transport::link_model::{HostLink, LinkModel, Loss};
use crate::transport::packet_data::MAX_RECV_PACKET_SIZE;
use crate::transport::peer_connection::inbound_budget::SharedInboundBudget;
use crate::transport::peer_connection::streaming::{
    MAX_INBOUND_STREAM_BYTES_PER_CONNECTION, MAX_INBOUND_STREAMS_PER_CONNECTION,
};
use crate::transport::{Socket, StreamId, TransportKeypair, UdpTransportSocket};

/// The size of the pieces a streamed send writes, and of the fragments
/// [`LabPair::concurrent_streams`] logs.
pub const FRAGMENT_BYTES: usize =
    crate::transport::peer_connection::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

/// The congestion controller inside the window cap, for every connection
/// built after the call (`recycle::lab`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabController {
    /// quinn's Cubic, what a node runs.
    Cubic,
    /// quinn's BBR, under the same cap.
    Bbr,
    /// Cubic that backs off less on loss with no queue building
    /// (`recycle::lab::VenoCubic`, a lab prototype).
    Veno,
    /// Cubic with a Westwood+ floor (`recycle::lab::Westwood`).
    Westwood,
    /// Cubic that ignores a loss with no queue building
    /// (`recycle::lab::DelayGated`).
    Gated,
    /// A BBR-style model controller (`recycle::lab::Model`).
    Model,
}

/// Choose the controller for connections built from now on.
pub fn set_controller(controller: LabController) {
    use super::recycle::lab;
    let value = match controller {
        LabController::Cubic => lab::CUBIC,
        LabController::Bbr => lab::BBR,
        LabController::Veno => lab::VENO,
        LabController::Westwood => lab::WESTWOOD,
        LabController::Gated => lab::GATED,
        LabController::Model => lab::MODEL,
    };
    lab::INNER_CONTROLLER.store(value, Ordering::Relaxed);
}

/// Kept for the older lab scripts. Since #6128 a node judges a streamed
/// send's gap on the connection's progress itself, so there is nothing to
/// switch, and this does nothing whatever `on` is.
pub fn set_gap_on_connection_progress(_on: bool) {}

/// Kept for the older lab scripts; does nothing since #6128 (the node's
/// own connection window applies).
pub fn set_connection_gap(_window: Option<Duration>) {}

/// [`LabController::Gated`]'s thresholds: queue delay as thousandths of the
/// minimum RTT, its floor in microseconds, and the round's loss share in
/// thousandths above which every loss counts.
pub fn set_gate(delay_permille: u64, min_delay_us: u64, loss_ceil_permille: u64) {
    use super::recycle::lab;
    lab::GATE_USE_SRTT.store(
        std::env::var("LAB_GATE_SRTT").as_deref() == Ok("1"),
        Ordering::Relaxed,
    );
    if let Ok(v) = std::env::var("LAB_GATE_MAX_DELAY_US") {
        lab::GATE_MAX_DELAY_US.store(v.parse().expect("a number"), Ordering::Relaxed);
    }
    if let Ok(v) = std::env::var("LAB_MODEL_LOSS_PERMILLE") {
        lab::MODEL_LOSS_PERMILLE.store(v.parse().expect("a number"), Ordering::Relaxed);
    }
    lab::GATE_DELAY_PERMILLE.store(delay_permille, Ordering::Relaxed);
    lab::GATE_MIN_DELAY_US.store(min_delay_us, Ordering::Relaxed);
    lab::GATE_LOSS_CEIL_PERMILLE.store(loss_ceil_permille, Ordering::Relaxed);
}

/// [`LabController::Gated`]'s decisions since the last call, reset:
/// passed for persistent congestion, for the loss share, for the queue;
/// ignored.
pub fn take_gate_counts() -> [u64; 4] {
    let counts = &super::recycle::lab::GATE_COUNTS;
    std::array::from_fn(|i| counts[i].swap(0, Ordering::Relaxed))
}

/// The datagram size for endpoints built from now on (both ends must agree,
/// as they do in the lab); `None` for the node's 1200. Discovery stays off.
pub fn set_mtu(bytes: Option<u16>) {
    super::config::lab::MTU.store(bytes.unwrap_or(0), Ordering::Relaxed);
}

/// The size of the pieces a streamed send writes into quinn; `None` for the
/// node's (one UDP fragment's payload).
pub fn set_piece_bytes(bytes: Option<usize>) {
    super::config::lab::PIECE_BYTES.store(bytes.unwrap_or(0) as u64, Ordering::Relaxed);
}

/// The congestion controller's initial window in bytes for connections
/// built from now on; `None` for quinn's (10 datagrams, 12,000 bytes).
pub fn set_initial_window(bytes: Option<u64>) {
    super::recycle::lab::INITIAL_WINDOW.store(bytes.unwrap_or(0), Ordering::Relaxed);
}

/// A rate start for connections built from now on: until the first
/// congestion event the window is at least `bytes_per_s` x srtt, so the
/// pacer sends at about 1.25 x that from the first round trip. `None` for
/// the node's (none).
pub fn set_rate_start(bytes_per_s: Option<u64>) {
    super::recycle::lab::RATE_START.store(bytes_per_s.unwrap_or(0), Ordering::Relaxed);
}

/// A floor under the controller's window (before the bandwidth cap), for
/// every connection from now on; `None` for the node's (no floor).
pub fn set_min_window(bytes: Option<u64>) {
    super::recycle::lab::MIN_WINDOW.store(bytes.unwrap_or(0), Ordering::Relaxed);
}

/// quinn's send window for connections built from now on, or `None` for the
/// node's (the flow-control window). The send window bounds the bytes the
/// connection's streams have buffered and not yet had acknowledged, so it is
/// how far ahead of the peer a write can get.
pub fn set_send_window(bytes: Option<u64>) {
    super::config::lab::SEND_WINDOW.store(bytes.unwrap_or(0), Ordering::Relaxed);
}

/// The connection's state at one moment of a run.
#[derive(Debug, Clone, Copy)]
pub struct ConnSample {
    /// From the start of the run.
    pub at: Duration,
    /// Ack-eliciting packets newly acknowledged so far
    /// (`recycle::AckProgress`).
    pub acked: u64,
    pub sent_packets: u64,
    pub lost_packets: u64,
    pub congestion_events: u64,
    pub cwnd: u64,
    pub rtt: Duration,
}

/// One host's uplink in the shaped link.
#[derive(Debug, Clone, Copy)]
pub struct LinkSpec {
    /// Bottleneck rate in bits per second.
    pub rate_bps: u64,
    /// Propagation delay from this host to the other.
    pub one_way_delay: Duration,
    /// Drop-tail queue in front of the bottleneck, in bytes.
    pub queue_bytes: usize,
    /// Share of datagrams lost at random after the queue, each way
    /// (`Loss::Bernoulli`), from a generator seeded with `seed`.
    pub loss: f64,
    pub seed: u64,
}

/// What the lab pair is built with.
#[derive(Debug, Clone, Copy)]
pub struct LabConfig {
    /// The flow-control window per connection and direction, on both sides.
    /// Must be one `config::connection_window_bytes` can produce (128 KiB to
    /// 2 MiB).
    pub window_bytes: u64,
    /// `bandwidth-limit` on both sides, in bytes per second
    /// (`recycle::CappedCubic`), or none.
    pub bandwidth_limit: Option<usize>,
    /// The shaped link, the same in each direction, or plain loopback.
    pub link: Option<LinkSpec>,
}

struct Shaper {
    model: LinkModel,
    link: LinkSpec,
    epoch: Instant,
    local: SocketAddr,
    out: mpsc::UnboundedSender<(u64, Vec<u8>, SocketAddr)>,
    /// Queueing delay of each datagram this socket's traffic got through the
    /// bottleneck, in microseconds. The competing flow keeps its own.
    queued_us: parking_lot::Mutex<Vec<u32>>,
    /// Jitter state: the latest arrival time handed out (arrivals stay in
    /// order) and a xorshift generator.
    jitter: parking_lot::Mutex<(u64, u64)>,
}

/// Extra delay added to each datagram on every shaped link, uniform in
/// `0..JITTER_US` microseconds, keeping datagrams in order (a later datagram
/// never arrives before an earlier one). 0 for none.
pub static JITTER_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The uplink of the shaped socket on `port`: datagrams offered and dropped
/// at the queue so far, counting everything that shares it (a competing
/// flow too).
pub fn link_counts(port: u16) -> Option<(u64, u64)> {
    let shaper = shaper_on(port)?;
    let stats = shaper.model.host_stats(shaper.local);
    Some((stats.offered, stats.dropped_queue_full))
}

/// Every shaped socket, by local port, for [`link_report`] and
/// [`Competitor::start`].
static SHAPERS: std::sync::Mutex<Vec<(u16, Arc<Shaper>)>> = std::sync::Mutex::new(Vec::new());

fn shaper_on(port: u16) -> Option<Arc<Shaper>> {
    SHAPERS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .rev()
        .find(|(p, _)| *p == port)
        .map(|(_, shaper)| shaper.clone())
}

/// The link's ground truth for the shaped socket on `port`: the measured
/// loss (queue drops plus random loss, over what this socket offered) and its
/// datagrams' queueing delay at the bottleneck.
pub fn link_report(port: u16) -> Option<String> {
    shaper_on(port).map(|shaper| shaper.stats())
}

/// Queueing delay of one datagram given when the model says it arrives: the
/// arrival less the send time, the propagation delay and the datagram's own
/// serialization.
fn queueing_us(link: &LinkSpec, now: u64, arrives: u64, len: usize) -> u32 {
    let serialization = if link.rate_bps == 0 {
        0
    } else {
        (len as u128 * 8 * 1_000_000_000 / link.rate_bps as u128) as u64
    };
    let wait = arrives
        .saturating_sub(now)
        .saturating_sub(link.one_way_delay.as_nanos() as u64)
        .saturating_sub(serialization);
    u32::try_from(wait / 1000).unwrap_or(u32::MAX)
}

fn percentile(sorted: &[u32], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let at = ((sorted.len() - 1) as f64 * p).round() as usize;
    f64::from(sorted[at]) / 1000.0
}

fn queue_summary(samples: &[u32]) -> String {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    format!(
        "queue_ms_p50={:.1} queue_ms_p90={:.1} queue_ms_max={:.1}",
        percentile(&sorted, 0.5),
        percentile(&sorted, 0.9),
        percentile(&sorted, 1.0)
    )
}

/// A loss-based flow standing in for the user's own traffic on the same
/// uplink: Reno-style AIMD in whole datagrams of 1200 bytes, driven by the
/// sender's link model, so it shares the bottleneck queue and the random loss
/// with whatever else that socket sends. It does not send real datagrams:
/// the model decides each one's fate, an acknowledgement is taken to come
/// back one propagation delay after the datagram arrives (the reverse path
/// is not lossy for it, as cumulative TCP acknowledgements mostly are not),
/// and a loss is detected one round trip plus the datagram's queueing after
/// it was sent. Slow start to the first loss, then +1 datagram per window
/// per round trip, halving at most once per round trip.
pub struct Competitor {
    delivered: Arc<std::sync::atomic::AtomicU64>,
    queued_us: Arc<parking_lot::Mutex<Vec<u32>>>,
    lost: Arc<std::sync::atomic::AtomicU64>,
    offered: Arc<std::sync::atomic::AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl Competitor {
    /// Start one on the shaped socket bound on `port`. `None` if that socket
    /// is not shaped.
    pub fn start(port: u16) -> Option<Self> {
        use std::sync::atomic::AtomicU64;
        let shaper = shaper_on(port)?;
        let delivered = Arc::new(AtomicU64::new(0));
        let lost = Arc::new(AtomicU64::new(0));
        let offered = Arc::new(AtomicU64::new(0));
        let queued_us = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let (d, l, o, q) = (
            delivered.clone(),
            lost.clone(),
            offered.clone(),
            queued_us.clone(),
        );
        let task = tokio::spawn(async move {
            const LEN: usize = 1200;
            let target: SocketAddr = (Ipv4Addr::new(127, 0, 0, 1), 9).into();
            let one_way = shaper.link.one_way_delay.as_nanos() as u64;
            let mut events = std::collections::BinaryHeap::new();
            let (mut cwnd, mut ssthresh) = (10.0f64, f64::INFINITY);
            let mut inflight = 0usize;
            let mut recovery_until = 0u64;
            let mut seq = 0u64;
            loop {
                let now = shaper.epoch.elapsed().as_nanos() as u64;
                while (inflight as f64) < cwnd {
                    seq += 1;
                    let mut packet = [0u8; LEN];
                    packet[..8].copy_from_slice(&seq.to_be_bytes());
                    o.fetch_add(1, Ordering::Relaxed);
                    let arrivals = shaper.model.on_send(now, shaper.local, target, &packet);
                    inflight += 1;
                    match arrivals.first() {
                        Some(arrival) => {
                            q.lock()
                                .push(queueing_us(&shaper.link, now, arrival.at_nanos, LEN));
                            events.push(std::cmp::Reverse((arrival.at_nanos + one_way, false)));
                        }
                        None => {
                            events.push(std::cmp::Reverse((now + 2 * one_way, true)));
                        }
                    }
                }
                let Some(std::cmp::Reverse((at, was_lost))) = events.pop() else {
                    continue;
                };
                tokio::time::sleep_until((shaper.epoch + Duration::from_nanos(at)).into()).await;
                inflight -= 1;
                if was_lost {
                    l.fetch_add(1, Ordering::Relaxed);
                    if at >= recovery_until {
                        ssthresh = (cwnd / 2.0).max(2.0);
                        cwnd = ssthresh;
                        recovery_until = at + 2 * one_way;
                    }
                } else {
                    d.fetch_add(LEN as u64, Ordering::Relaxed);
                    if cwnd < ssthresh {
                        cwnd += 1.0;
                    } else {
                        cwnd += 1.0 / cwnd;
                    }
                }
            }
        });
        Some(Self {
            delivered,
            queued_us,
            lost,
            offered,
            task,
        })
    }

    /// Bytes acknowledged so far.
    pub fn delivered(&self) -> u64 {
        self.delivered.load(Ordering::Relaxed)
    }

    /// Datagrams offered and lost so far, and how many queueing samples it
    /// has (for [`Self::queue_p90_since`]).
    pub fn counts(&self) -> (u64, u64, usize) {
        (
            self.offered.load(Ordering::Relaxed),
            self.lost.load(Ordering::Relaxed),
            self.queued_us.lock().len(),
        )
    }

    /// The 90th percentile of its datagrams' queueing, in milliseconds, over
    /// the samples from index `from` on.
    pub fn queue_p90_since(&self, from: usize) -> f64 {
        let mut samples: Vec<u32> = self.queued_us.lock()[from..].to_vec();
        samples.sort_unstable();
        percentile(&samples, 0.9)
    }

    /// Its measured loss and queueing so far.
    pub fn report(&self) -> String {
        let offered = self.offered.load(Ordering::Relaxed).max(1);
        format!(
            "competitor_loss={:.3} {}",
            self.lost.load(Ordering::Relaxed) as f64 / offered as f64,
            queue_summary(&self.queued_us.lock())
        )
    }
}

impl Drop for Competitor {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A loopback UDP socket that, when shaped, sends through a link model on
/// real time (see the module docs).
pub struct LabSocket {
    inner: Arc<UdpTransportSocket>,
    shaper: Option<Arc<Shaper>>,
}

/// Links for sockets the UDP transport binds itself, by port
/// ([`shape_port`]).
static SHAPED_PORTS: std::sync::Mutex<Vec<(u16, LinkSpec)>> = std::sync::Mutex::new(Vec::new());

/// Shape the next [`LabSocket`] bound through [`Socket::bind`] on `port` with
/// `link`. For a UDP-transport pair (`create_connection_handler::<LabSocket>`),
/// which binds its own socket, so it can run over the same link as a QUIC
/// pair.
pub fn shape_port(port: u16, link: LinkSpec) {
    SHAPED_PORTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push((port, link));
}

impl LabSocket {
    async fn bind_shaped(addr: SocketAddr, link: Option<LinkSpec>) -> io::Result<Self> {
        let inner = Arc::new(<UdpTransportSocket as Socket>::bind(addr).await?);
        let Some(link) = link else {
            return Ok(Self {
                inner,
                shaper: None,
            });
        };
        let local = inner.local_addr()?;
        let model = LinkModel::new(
            HostLink {
                uplink_bps: link.rate_bps,
                queue_bytes: link.queue_bytes,
                one_way_delay: link.one_way_delay,
                loss: if link.loss > 0.0 {
                    Loss::Bernoulli(link.loss)
                } else {
                    Loss::None
                },
            },
            link.seed,
        );
        let epoch = Instant::now();
        let (out, mut queued) = mpsc::unbounded_channel::<(u64, Vec<u8>, SocketAddr)>();
        let sender = inner.clone();
        tokio::spawn(async move {
            while let Some((at, bytes, to)) = queued.recv().await {
                tokio::time::sleep_until((epoch + Duration::from_nanos(at)).into()).await;
                // A loopback send fails only when the kernel buffer is full,
                // which a real link would also show as loss.
                Socket::send_to(&*sender, &bytes, to).await.ok();
            }
        });
        let shaper = Arc::new(Shaper {
            model,
            link,
            epoch,
            local,
            out,
            queued_us: parking_lot::Mutex::new(Vec::new()),
            jitter: parking_lot::Mutex::new((0, 0x9E37_79B9_7F4A_7C15 ^ u64::from(local.port()))),
        });
        SHAPERS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((local.port(), shaper.clone()));
        Ok(Self {
            inner,
            shaper: Some(shaper),
        })
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Hand `buf` to the link if the socket is shaped; `None` if it is not.
    fn shape(&self, buf: &[u8], target: SocketAddr) -> Option<usize> {
        let shaper = self.shaper.as_ref()?;
        let now = shaper.epoch.elapsed().as_nanos() as u64;
        for arrival in shaper.model.on_send(now, shaper.local, target, buf) {
            shaper.queued_us.lock().push(queueing_us(
                &shaper.link,
                now,
                arrival.at_nanos,
                buf.len(),
            ));
            let jitter_ns = JITTER_US.load(Ordering::Relaxed) * 1000;
            let at = if jitter_ns == 0 {
                arrival.at_nanos
            } else {
                let mut state = shaper.jitter.lock();
                let (last, rng) = &mut *state;
                *rng ^= *rng << 13;
                *rng ^= *rng >> 7;
                *rng ^= *rng << 17;
                let at = (arrival.at_nanos + *rng % jitter_ns).max(*last);
                *last = at;
                at
            };
            shaper.out.send((at, buf.to_vec(), target)).ok();
        }
        Some(buf.len())
    }
}

impl Shaper {
    /// The link's ground truth for this socket's uplink.
    fn stats(&self) -> String {
        // Host counters include a competing flow on the same uplink; the
        // queueing figures are this socket's own datagrams only.
        let stats = self.model.host_stats(self.local);
        let lost = stats.dropped_queue_full + stats.dropped_random;
        format!(
            "offered={} delivered={} dropped_queue_full={} dropped_random={} \
             measured_loss={:.3} max_queueing_ms={:.1} {}",
            stats.offered,
            stats.delivered,
            stats.dropped_queue_full,
            stats.dropped_random,
            lost as f64 / stats.offered.max(1) as f64,
            stats.max_queueing_delay.as_secs_f64() * 1000.0,
            queue_summary(&self.queued_us.lock())
        )
    }
}

impl Socket for LabSocket {
    async fn bind(addr: SocketAddr) -> io::Result<Self> {
        let link = {
            let mut ports = SHAPED_PORTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            ports
                .iter()
                .position(|(port, _)| *port == addr.port())
                .map(|at| ports.remove(at).1)
        };
        Self::bind_shaped(addr, link).await
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        Socket::recv_from(&*self.inner, buf).await
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        match self.shape(buf, target) {
            Some(sent) => Ok(sent),
            None => Socket::send_to(&*self.inner, buf, target).await,
        }
    }

    fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        match self.shape(buf, target) {
            Some(sent) => Ok(sent),
            None => self.inner.send_to_blocking(buf, target),
        }
    }

    fn try_send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        match self.shape(buf, target) {
            Some(sent) => Ok(sent),
            None => self.inner.try_send_to(buf, target),
        }
    }

    fn poll_send_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.shaper {
            Some(_) => Poll::Ready(Ok(())),
            None => self.inner.poll_send_ready(cx),
        }
    }
}

/// Records which stream each fragment belonged to, and when.
struct FragmentLog {
    stream: u16,
    start: Instant,
    log: Arc<parking_lot::Mutex<Vec<(u16, Duration)>>>,
}

impl StreamProgressObserver for FragmentLog {
    fn on_fragment_sent(&self) {
        let at = self.start.elapsed();
        self.log.lock().push((self.stream, at));
    }
}

/// One run of [`LabPair::concurrent_streams`].
#[derive(Debug)]
pub struct StreamsRun {
    /// Every fragment the senders handed to quinn, in the order they did:
    /// the stream it belonged to and when, from the start of the run.
    pub log: Vec<(u16, Duration)>,
    /// Each send's outcome, `Debug`-rendered, in stream order.
    pub outcomes: Vec<String>,
    /// When each send returned, from the start of the run, in stream order.
    pub returned_at: Vec<Duration>,
    /// The connection every 20 ms.
    pub samples: Vec<ConnSample>,
    /// Whether every send returned `Sent`.
    pub all_sent: bool,
    /// Messages the receiver assembled in full.
    pub received: usize,
    /// From the start to the last send returning and the last message
    /// assembled.
    pub elapsed: Duration,
}

/// A connected pair: `a` sends, `b` receives and assembles every stream.
pub struct LabPair {
    a: Arc<QuicPath>,
    time: RealTime,
    window: u64,
    assembled: mpsc::UnboundedReceiver<usize>,
    /// When the receiver last assembled a message in full.
    last_arrival: Arc<parking_lot::Mutex<Option<Instant>>>,
    shapers: [Option<Arc<Shaper>>; 2],
    _keep: Box<dyn std::any::Any + Send>,
}

/// The listener's part for a socket with no listener: read it, so its QUIC
/// ingress queue fills.
fn pump(socket: Arc<DemuxSocket<LabSocket>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut buf = vec![0u8; MAX_RECV_PACKET_SIZE];
        while socket.recv_classified(&mut buf).await.is_ok() {}
    })
}

impl LabPair {
    /// Bind two endpoints on loopback, connect them and build a path on each
    /// side. Run inside the runtime that will carry the traffic.
    ///
    /// # Panics
    /// If the window is not one the config can produce, or the handshake
    /// fails or takes more than 20 s.
    pub async fn connect(config: LabConfig) -> Self {
        // One connection, so the window is memory / 20 / 2.
        let memory_limit = config.window_bytes as usize * 40;
        assert_eq!(
            connection_window_bytes(memory_limit, 1),
            config.window_bytes,
            "window {} is outside what the config produces",
            config.window_bytes
        );
        let settings = QuicSettings {
            max_connections: 1,
            memory_limit,
            idle_timeout: None,
            rate_limit: QuicRateLimit::new(config.bandwidth_limit, None),
        };
        let bind = || async {
            let socket = LabSocket::bind_shaped((Ipv4Addr::LOCALHOST, 0).into(), config.link)
                .await
                .expect("bind");
            let addr = socket.local_addr().expect("bound");
            let shaper = socket.shaper.clone();
            (Arc::new(DemuxSocket::wrap(socket, None)), addr, shaper)
        };
        let (a_socket, a_addr, a_shaper) = bind().await;
        let (b_socket, b_addr, b_shaper) = bind().await;
        let pumps = [pump(a_socket.clone()), pump(b_socket.clone())];
        let time = RealTime::new();
        let (a_keys, b_keys) = (TransportKeypair::new(), TransportKeypair::new());
        let a_end = QuicEndpoint::bind(a_socket.clone(), a_addr, time.clone(), &settings, &a_keys)
            .expect("endpoint a");
        let b_end = QuicEndpoint::bind(b_socket.clone(), b_addr, time.clone(), &settings, &b_keys)
            .expect("endpoint b");
        let permit = b_end
            .allow_peer(a_addr, a_keys.public())
            .expect("allow-set has room");
        let (client, server) = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::join!(a_end.connect(b_addr, b_keys.public()), async {
                b_end
                    .accept()
                    .await
                    .expect("an incoming attempt")
                    .establish()
                    .await
            })
        })
        .await
        .expect("handshake within 20 s");
        let (client, server) = (client.expect("client"), server.expect("server"));
        drop(permit);
        let budget = || {
            SharedInboundBudget::new(
                MAX_INBOUND_STREAMS_PER_CONNECTION,
                MAX_INBOUND_STREAM_BYTES_PER_CONNECTION,
            )
        };
        let (a_data, a_inbox) = data_channels();
        let (a, a_controls) = QuicPath::with_budget(
            client,
            time.clone(),
            QuicPathLimits::DEFAULT,
            budget(),
            a_data,
        );
        a.enable_data();
        let (b_data, b_inbox) = data_channels();
        let (b, b_controls) = QuicPath::with_budget(
            server,
            time.clone(),
            QuicPathLimits::DEFAULT,
            budget(),
            b_data,
        );
        b.enable_data();
        let (assembled_tx, assembled) = mpsc::unbounded_channel();
        let last_arrival = Arc::new(parking_lot::Mutex::new(None));
        let mut arrivals = b_inbox.streams;
        let arrived = last_arrival.clone();
        let reader = tokio::spawn(async move {
            while let Some(arrival) = arrivals.recv().await {
                let assembled_tx = assembled_tx.clone();
                let arrived = arrived.clone();
                tokio::spawn(async move {
                    match arrival.handle.assemble().await {
                        Ok(body) => {
                            *arrived.lock() = Some(Instant::now());
                            assembled_tx.send(body.len()).ok();
                        }
                        Err(e) => eprintln!("LAB_ASSEMBLE_ERR {e:?}"),
                    }
                });
            }
        });
        let a = Arc::new(a);
        let keep = (
            a_socket,
            b_socket,
            a_end,
            b_end,
            Arc::new(b),
            a_inbox.shorts,
            a_inbox.streams,
            b_inbox.shorts,
            a_controls,
            b_controls,
            pumps,
            reader,
        );
        Self {
            a,
            time,
            window: config.window_bytes,
            assembled,
            last_arrival,
            shapers: [a_shaper, b_shaper],
            _keep: Box::new(keep),
        }
    }

    /// The sender's local port, for [`Competitor::start`] and [`link_report`].
    pub fn sender_port(&self) -> Option<u16> {
        self.shapers[0].as_ref().map(|shaper| shaper.local.port())
    }

    /// The flow-control window both sides run with.
    pub fn window_bytes(&self) -> u64 {
        self.window
    }

    /// Send `streams` messages of `bytes` each at once, each from its own
    /// task, and log every fragment as the senders hand it to quinn. Returns
    /// once every send has returned and the receiver has assembled every
    /// message, or after `limit`.
    pub async fn concurrent_streams(
        &mut self,
        streams: u16,
        bytes: usize,
        limit: Duration,
    ) -> StreamsRun {
        let log = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let body = Bytes::from(vec![0xA5u8; bytes]);
        let start = Instant::now();
        let started = Arc::new(AtomicUsize::new(0));
        let samples = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let sampler = {
            let (connection, samples) = (self.a.quinn_connection(), samples.clone());
            let progress = super::recycle::AckProgress::of(&connection);
            tokio::spawn(async move {
                loop {
                    let acked = progress.as_ref().map_or(0, |p| p.acked());
                    let stats = connection.stats();
                    samples.lock().push(ConnSample {
                        at: start.elapsed(),
                        acked,
                        sent_packets: stats.path.sent_packets,
                        lost_packets: stats.path.lost_packets,
                        congestion_events: stats.path.congestion_events,
                        cwnd: stats.path.cwnd,
                        rtt: stats.path.rtt,
                    });
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
        };
        let mut sends = Vec::new();
        for stream in 0..streams {
            let (a, time, body, log) =
                (self.a.clone(), self.time.clone(), body.clone(), log.clone());
            let started = started.clone();
            sends.push(tokio::spawn(async move {
                started.fetch_add(1, Ordering::Relaxed);
                let observer = FragmentLog { stream, start, log };
                let outcome = a
                    .send_stream_bounded(
                        &time,
                        SendDeadlines::stream(body.len() as u64),
                        StreamId::next(),
                        None,
                        body,
                        Some(&observer),
                    )
                    .await;
                (outcome, start.elapsed())
            }));
        }
        let assembled = &mut self.assembled;
        let run = async {
            let mut outcomes = Vec::new();
            let mut returned_at = Vec::new();
            let mut all_sent = true;
            for send in sends {
                let (outcome, at) = send.await.expect("the send ran");
                all_sent &= matches!(outcome, BoundedSend::Sent);
                outcomes.push(format!("{outcome:?}"));
                returned_at.push(at);
            }
            (outcomes, returned_at, all_sent)
        };
        let (outcomes, returned_at, all_sent) = tokio::time::timeout(limit, run)
            .await
            .unwrap_or_else(|_| (vec!["run timed out".to_string()], Vec::new(), false));
        // Every message the receiver assembled: all of them when every send
        // returned Sent, and after a failure, those that still arrive within
        // the receiver's 5 s gap.
        let mut received = 0;
        while received < usize::from(streams) {
            let wait = if all_sent {
                limit
            } else {
                Duration::from_secs(6)
            };
            match tokio::time::timeout(wait, assembled.recv()).await {
                Ok(Some(_)) => received += 1,
                Ok(None) | Err(_) => break,
            }
        }
        let elapsed = start.elapsed();
        sampler.abort();
        let log = std::mem::take(&mut *log.lock());
        let samples = std::mem::take(&mut *samples.lock());
        StreamsRun {
            log,
            outcomes,
            returned_at,
            samples,
            all_sent,
            received,
            elapsed,
        }
    }

    /// Send one message of `bytes` and wait until the receiver has assembled
    /// it. Returns the time from the start of the send to the end of the
    /// assembly.
    ///
    /// # Panics
    /// If the send does not return `Sent` or the receiver gets nothing.
    pub async fn transfer(&mut self, bytes: usize) -> Duration {
        self.transfer_timed(bytes, Duration::from_secs(3600))
            .await
            .expect("the transfer completes")
            .1
    }

    /// Send one message of `bytes`. Returns the time from the start of the
    /// send to the receiver assembling it, and to the later of that and the
    /// send returning `Sent`. The first is the one to compare with the UDP
    /// transport's arrival. `Err` with the outcome when the send does not
    /// return `Sent`, or nothing arrives within `limit`.
    pub async fn transfer_timed(
        &mut self,
        bytes: usize,
        limit: Duration,
    ) -> Result<(Duration, Duration), String> {
        let body = Bytes::from(vec![0x5Au8; bytes]);
        *self.last_arrival.lock() = None;
        let start = Instant::now();
        let outcome = tokio::time::timeout(
            limit,
            self.a.send_stream_bounded(
                &self.time,
                SendDeadlines::stream(bytes as u64),
                StreamId::next(),
                None,
                body,
                None,
            ),
        )
        .await
        .map_err(|_| "limit".to_string())?;
        if !matches!(outcome, BoundedSend::Sent) {
            return Err(format!("{outcome:?}"));
        }
        let got = tokio::time::timeout(limit, self.assembled.recv())
            .await
            .map_err(|_| "not assembled".to_string())?
            .ok_or_else(|| "receiver gone".to_string())?;
        assert_eq!(got, bytes);
        let done = start.elapsed();
        let arrived = self
            .last_arrival
            .lock()
            .expect("an assembled message has an arrival time")
            .saturating_duration_since(start);
        Ok((arrived, done))
    }

    /// quinn's congestion window on the sender's path now.
    pub fn cwnd(&self) -> u64 {
        self.a.quinn_connection().stats().path.cwnd
    }

    /// quinn's view of the sender's path, and the link's ground truth for
    /// each uplink when shaped. QUIC ingress drops are process-wide, in
    /// `TRANSPORT_METRICS` (`quic_ingress_source_full`, `_queue_full`).
    pub fn describe(&self) -> String {
        let stats = self.a.quinn_connection().stats();
        let link = |shaper: &Option<Arc<Shaper>>| {
            shaper
                .as_ref()
                .map_or_else(|| "unshaped".to_string(), |shaper| shaper.stats())
        };
        format!(
            "rtt_ms={:.1} cwnd={} lost_packets={} sent_packets={} congestion_events={} \
             a_uplink[{}] b_uplink[{}]",
            stats.path.rtt.as_secs_f64() * 1000.0,
            stats.path.cwnd,
            stats.path.lost_packets,
            stats.path.sent_packets,
            stats.path.congestion_events,
            link(&self.shapers[0]),
            link(&self.shapers[1]),
        )
    }
}

/// One run of [`LabMany::send_all`].
#[derive(Debug)]
pub struct ManyRun {
    /// Per connection: how many of its sends returned `Sent`.
    pub sent: Vec<usize>,
    /// Per connection: when its last send returned, from the start.
    pub last_returned: Vec<Duration>,
    /// Every send's outcome, `Debug`-rendered, counted.
    pub outcomes: std::collections::BTreeMap<String, usize>,
    /// Messages the receivers assembled.
    pub received: usize,
    pub elapsed: Duration,
}

/// `n` QUIC connections from one sender socket, so they share its uplink
/// (one link model), each to a receiver of its own. For the many-connection
/// bottleneck test: a node holds tens of connections, and what each
/// connection's controller does adds up on one uplink.
pub struct LabMany {
    senders: Vec<Arc<QuicPath>>,
    time: RealTime,
    assembled: mpsc::UnboundedReceiver<usize>,
    sender_shaper: Option<Arc<Shaper>>,
    _keep: Box<dyn std::any::Any + Send>,
}

impl LabMany {
    /// Bind the sender and `n` receivers on loopback, every socket shaped
    /// with `config.link`, connect them, and build the paths.
    ///
    /// # Panics
    /// As [`LabPair::connect`].
    pub async fn connect(config: LabConfig, n: usize) -> Self {
        let memory_limit = config.window_bytes as usize * 40;
        assert_eq!(
            connection_window_bytes(memory_limit, 1),
            config.window_bytes
        );
        let settings = QuicSettings {
            max_connections: 1,
            memory_limit,
            idle_timeout: None,
            rate_limit: QuicRateLimit::new(config.bandwidth_limit, None),
        };
        let bind = || async {
            let socket = LabSocket::bind_shaped((Ipv4Addr::LOCALHOST, 0).into(), config.link)
                .await
                .expect("bind");
            let addr = socket.local_addr().expect("bound");
            let shaper = socket.shaper.clone();
            (Arc::new(DemuxSocket::wrap(socket, None)), addr, shaper)
        };
        let time = RealTime::new();
        let (a_socket, a_addr, sender_shaper) = bind().await;
        let a_keys = TransportKeypair::new();
        let a_end = QuicEndpoint::bind(a_socket.clone(), a_addr, time.clone(), &settings, &a_keys)
            .expect("endpoint a");
        let mut pumps = vec![pump(a_socket.clone())];
        let budget = || {
            SharedInboundBudget::new(
                MAX_INBOUND_STREAMS_PER_CONNECTION,
                MAX_INBOUND_STREAM_BYTES_PER_CONNECTION,
            )
        };
        let (assembled_tx, assembled) = mpsc::unbounded_channel();
        let mut senders = Vec::new();
        let mut keep: Vec<Box<dyn std::any::Any + Send>> = Vec::new();
        for _ in 0..n {
            let (b_socket, b_addr, _) = bind().await;
            pumps.push(pump(b_socket.clone()));
            let b_keys = TransportKeypair::new();
            let b_end =
                QuicEndpoint::bind(b_socket.clone(), b_addr, time.clone(), &settings, &b_keys)
                    .expect("endpoint b");
            let permit = b_end
                .allow_peer(a_addr, a_keys.public())
                .expect("allow-set has room");
            let (client, server) = tokio::time::timeout(Duration::from_secs(20), async {
                tokio::join!(a_end.connect(b_addr, b_keys.public()), async {
                    b_end
                        .accept()
                        .await
                        .expect("an incoming attempt")
                        .establish()
                        .await
                })
            })
            .await
            .expect("handshake within 20 s");
            let (client, server) = (client.expect("client"), server.expect("server"));
            drop(permit);
            let (a_data, a_inbox) = data_channels();
            let (a, a_controls) = QuicPath::with_budget(
                client,
                time.clone(),
                QuicPathLimits::DEFAULT,
                budget(),
                a_data,
            );
            a.enable_data();
            let (b_data, b_inbox) = data_channels();
            let (b, b_controls) = QuicPath::with_budget(
                server,
                time.clone(),
                QuicPathLimits::DEFAULT,
                budget(),
                b_data,
            );
            b.enable_data();
            let mut arrivals = b_inbox.streams;
            let assembled_tx = assembled_tx.clone();
            let reader = tokio::spawn(async move {
                while let Some(arrival) = arrivals.recv().await {
                    let assembled_tx = assembled_tx.clone();
                    tokio::spawn(async move {
                        if let Ok(body) = arrival.handle.assemble().await {
                            assembled_tx.send(body.len()).ok();
                        }
                    });
                }
            });
            senders.push(Arc::new(a));
            keep.push(Box::new((
                b_socket,
                b_end,
                Arc::new(b),
                a_inbox.shorts,
                a_inbox.streams,
                b_inbox.shorts,
                a_controls,
                b_controls,
                reader,
            )));
        }
        Self {
            senders,
            time,
            assembled,
            sender_shaper,
            _keep: Box::new((a_socket, a_end, pumps, keep)),
        }
    }

    /// The sender's local port, for [`Competitor::start`] and [`link_report`].
    pub fn sender_port(&self) -> Option<u16> {
        self.sender_shaper
            .as_ref()
            .map(|shaper| shaper.local.port())
    }

    /// Send `per_connection` messages of `bytes` on every connection at
    /// once, each from its own task, under the node's deadlines.
    pub async fn send_all(
        &mut self,
        per_connection: usize,
        bytes: usize,
        limit: Duration,
    ) -> ManyRun {
        let body = Bytes::from(vec![0xA5u8; bytes]);
        let start = Instant::now();
        let mut sends = Vec::new();
        for (index, path) in self.senders.iter().enumerate() {
            for _ in 0..per_connection {
                let (path, time, body) = (path.clone(), self.time.clone(), body.clone());
                sends.push((
                    index,
                    tokio::spawn(async move {
                        let outcome = path
                            .send_stream_bounded(
                                &time,
                                SendDeadlines::stream(body.len() as u64),
                                StreamId::next(),
                                None,
                                body,
                                None,
                            )
                            .await;
                        (outcome, start.elapsed())
                    }),
                ));
            }
        }
        let n = self.senders.len();
        let mut sent = vec![0; n];
        let mut last_returned = vec![Duration::ZERO; n];
        let mut outcomes = std::collections::BTreeMap::new();
        let collect = async {
            for (index, send) in sends {
                let (outcome, at) = send.await.expect("the send ran");
                if matches!(outcome, BoundedSend::Sent) {
                    sent[index] += 1;
                }
                last_returned[index] = last_returned[index].max(at);
                let name = format!("{outcome:?}");
                *outcomes.entry(name).or_insert(0) += 1;
            }
        };
        if tokio::time::timeout(limit, collect).await.is_err() {
            *outcomes.entry("run timed out".to_string()).or_insert(0) += 1;
        }
        let total: usize = sent.iter().sum();
        let mut received = 0;
        while received < total {
            match tokio::time::timeout(Duration::from_secs(6), self.assembled.recv()).await {
                Ok(Some(_)) => received += 1,
                Ok(None) | Err(_) => break,
            }
        }
        ManyRun {
            sent,
            last_returned,
            outcomes,
            received,
            elapsed: start.elapsed(),
        }
    }
}

impl LabPair {
    /// Send one message of each size in `sizes` at once, each from its own
    /// task, under the node's deadlines. Returns each send's outcome,
    /// `Debug`-rendered, and when it returned (`Sent` means every byte
    /// acknowledged), in `sizes` order. The receiver's assembly counts are
    /// left in its channel.
    pub async fn send_batch(&self, sizes: &[usize]) -> Vec<(String, Duration)> {
        let start = Instant::now();
        let mut sends = Vec::new();
        for &len in sizes {
            let (a, time) = (self.a.clone(), self.time.clone());
            sends.push(tokio::spawn(async move {
                let body = Bytes::from(vec![0xA5u8; len]);
                let outcome = a
                    .send_stream_bounded(
                        &time,
                        SendDeadlines::stream(len as u64),
                        StreamId::next(),
                        None,
                        body,
                        None,
                    )
                    .await;
                (format!("{outcome:?}"), start.elapsed())
            }));
        }
        let mut out = Vec::new();
        for send in sends {
            out.push(send.await.expect("the send ran"));
        }
        out
    }
}
