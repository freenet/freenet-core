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

/// One host's uplink in the shaped link.
#[derive(Debug, Clone, Copy)]
pub struct LinkSpec {
    /// Bottleneck rate in bits per second.
    pub rate_bps: u64,
    /// Propagation delay from this host to the other.
    pub one_way_delay: Duration,
    /// Drop-tail queue in front of the bottleneck, in bytes.
    pub queue_bytes: usize,
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
    epoch: Instant,
    local: SocketAddr,
    out: mpsc::UnboundedSender<(u64, Vec<u8>, SocketAddr)>,
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
                loss: Loss::None,
            },
            0,
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
        Ok(Self {
            inner,
            shaper: Some(Arc::new(Shaper {
                model,
                epoch,
                local,
                out,
            })),
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
            shaper
                .out
                .send((arrival.at_nanos, buf.to_vec(), target))
                .ok();
        }
        Some(buf.len())
    }
}

impl Shaper {
    /// The link's ground truth for this socket's uplink.
    fn stats(&self) -> String {
        let stats = self.model.host_stats(self.local);
        format!(
            "offered={} delivered={} dropped_queue_full={} max_queueing_ms={:.1}",
            stats.offered,
            stats.delivered,
            stats.dropped_queue_full,
            stats.max_queueing_delay.as_secs_f64() * 1000.0
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
        let mut arrivals = b_inbox.streams;
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
            shapers: [a_shaper, b_shaper],
            _keep: Box::new(keep),
        }
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
        let mut sends = Vec::new();
        for stream in 0..streams {
            let (a, time, body, log) =
                (self.a.clone(), self.time.clone(), body.clone(), log.clone());
            let started = started.clone();
            sends.push(tokio::spawn(async move {
                started.fetch_add(1, Ordering::Relaxed);
                let observer = FragmentLog { stream, start, log };
                a.send_stream_bounded(
                    &time,
                    SendDeadlines::stream(body.len() as u64),
                    StreamId::next(),
                    None,
                    body,
                    Some(&observer),
                )
                .await
            }));
        }
        let run = async {
            let mut outcomes = Vec::new();
            let mut all_sent = true;
            for send in sends {
                let outcome = send.await.expect("the send ran");
                all_sent &= matches!(outcome, BoundedSend::Sent);
                outcomes.push(format!("{outcome:?}"));
            }
            let mut received = 0;
            if all_sent {
                while received < usize::from(streams) {
                    match self.assembled.recv().await {
                        Some(_) => received += 1,
                        None => break,
                    }
                }
            }
            (outcomes, all_sent, received)
        };
        let (outcomes, all_sent, received) = tokio::time::timeout(limit, run)
            .await
            .unwrap_or_else(|_| (vec!["run timed out".to_string()], false, 0));
        let elapsed = start.elapsed();
        let log = std::mem::take(&mut *log.lock());
        StreamsRun {
            log,
            outcomes,
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
        let body = Bytes::from(vec![0x5Au8; bytes]);
        let start = Instant::now();
        let outcome = self
            .a
            .send_stream_bounded(
                &self.time,
                SendDeadlines::stream(bytes as u64),
                StreamId::next(),
                None,
                body,
                None,
            )
            .await;
        assert!(matches!(outcome, BoundedSend::Sent), "{outcome:?}");
        let got = self.assembled.recv().await.expect("the receiver runs");
        assert_eq!(got, bytes);
        start.elapsed()
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
