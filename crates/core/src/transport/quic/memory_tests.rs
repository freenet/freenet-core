//! Heap measurements of the QUIC endpoint (the QUIC memory gate, step M1b,
//! #5940).
//!
//! Each measurement counts the bytes left allocated on the test's thread
//! (`util::alloc_count`), rounded to jemalloc's size classes, so everything
//! measured must allocate and free on that thread. The idle-connection
//! measurement runs both endpoints, quinn's drivers and the paths' receive
//! tasks on the test's current-thread runtime, over `SimulationSocket` on
//! one `VirtualTime`, with `GlobalRng` seeded, so a run is repeatable. The
//! received-data measurements (unordered reads, ACK tail) drive quinn-proto
//! directly, with no runtime at all. The open-stream and `stopped`-entry
//! measurements (`StreamRig`) run real endpoints the same way as the
//! idle-connection one, on the current-thread runtime, and take the
//! `stopped`-entry figure as the difference of two runs, so the simulated
//! clock's retained timer-queue capacity cancels out.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::config::{
    QUIC_ACK_TAIL_BYTES, QUIC_CONNECTION_FIXED_BYTES, QUIC_MEMORY_MEASURED_WITH,
    QUIC_OPEN_STREAM_BYTES, QUIC_STOPPED_ENTRY_BYTES, QuicSettings,
};
use super::path::{QuicInbound, QuicPath, QuicPathLimits};
use super::tests::{SEED, bounded, drive};
use super::{QuicConnection, QuicEndpoint};
use crate::config::GlobalRng;
use crate::simulation::{TimeSource, VirtualTime};
use crate::transport::Socket;
use crate::transport::crypto::TransportKeypair;
use crate::transport::demux::DemuxSocket;
use crate::transport::in_memory_socket::{
    PacketDeliveryDecision, SimulationSocket, register_address_network,
    register_network_time_source, set_packet_delivery_callback, unregister_network_time_source,
};
use crate::transport::packet_data::MAX_RECV_PACKET_SIZE;
use crate::util::alloc_count::{Live, live_bytes, live_delta, peak_delta};

/// How long, in virtual time, a step may take.
const LIMIT: Duration = Duration::from_secs(600);

/// Long enough, in virtual time, for every acknowledgement, retransmission
/// timer and connection-ID exchange after a handshake to finish, and for a
/// closed connection to drain (three PTOs): about 1 s each here.
const SETTLE: Duration = Duration::from_secs(30);

/// How far the live count may move across a settle with no connection
/// change, before the baseline: anything more means traffic or timers were
/// still running, and the baseline would not be one.
const SETTLE_DRIFT_BYTES: isize = 1024;

/// The settings of a node with 2 GiB of memory (the shipped `MemoryMax`), at
/// a window of 268,435 bytes: the window such a node had at 200 connections
/// before windows were set per path from the buffer budget, which the
/// measurements' figures were taken at. A data path's window now moves from
/// the floor (128 KiB) to `budget::MAX_WINDOW_BYTES`; the window-scaled pins
/// are checked across that range below.
fn settings() -> QuicSettings {
    QuicSettings {
        memory_limit: 2 * 1024 * 1024 * 1024,
        idle_timeout: None,
        initial_window: Some(268_435),
        rate_limit: Default::default(),
    }
}

/// One side of an idle connection as a node holds it.
struct Held {
    _path: QuicPath,
    _inbound: QuicInbound,
}

/// The bytes `n` idle connections hold, per side, after their handshakes and
/// once all traffic has settled.
#[derive(Debug, Clone, Copy)]
struct Cost {
    n: usize,
    /// Freed when the client side of every connection is closed and drained.
    client: Live,
    /// Freed when the server side is then closed and drained.
    server: Live,
    /// Still allocated after both sides closed, against the baseline:
    /// capacity kept once `n` connections have existed (see
    /// `QUIC_CONNECTION_FIXED_BYTES`'s doc).
    retained: Live,
    /// The change across one settle just before the baseline.
    settle_drift: Live,
}

impl Cost {
    /// The measured cost per connection, in jemalloc-rounded bytes: the
    /// larger side, plus what was retained, charged to that side alone.
    fn per_connection(&self) -> usize {
        let side = self.client.rounded.max(self.server.rounded) + self.retained.rounded.max(0);
        (side.max(0) as usize).div_ceil(self.n)
    }
}

/// Let [`SETTLE`] of virtual time pass, running every task meanwhile.
async fn settle(time: &VirtualTime) {
    let sleep = time.sleep(SETTLE);
    drive(time, LIMIT, sleep).await;
}

/// Open `n` connections from a client endpoint to a server endpoint, with a
/// `QuicPath` on each side, and measure what they hold ([`Cost`]).
///
/// One connection is opened and closed first so that what the first
/// connection initialises once per process (lazily built statics, metric
/// entries, tables at their first size) is not counted, then one settle is
/// checked to move the count by at most [`SETTLE_DRIFT_BYTES`]. The
/// baseline is taken after
/// that; then the `n` connections are opened and settled, the network is
/// cut so neither side hears the other close, the client side is dropped
/// and drained, then the server side.
///
/// Not counted:
/// - per-peer state set up before the baseline. Every connection here
///   comes from one client address, so the server's allow-set permit, its
///   expected-key entry and the demux socket's allow-set entry exist once,
///   before the baseline; a node pays them once per peer.
/// - keep-alives. `VirtualTime` turns them off (`supports_keepalive`), so
///   the 15 s PING and the sent-packet record each one leaves until it is
///   acknowledged are not here.
/// - socket buffers. `SimulationSocket` has none; a real UDP socket's
///   kernel buffers are per endpoint, not per connection.
async fn measure(network: &str, n: usize) -> Cost {
    let _seed = GlobalRng::seed_guard(SEED);
    let net = crate::transport::test_net::subnet();
    let (a_addr, b_addr) = (net.addr(1), net.addr(2));
    let time = VirtualTime::new();
    register_network_time_source(network, time.clone());
    register_address_network(a_addr, network);
    register_address_network(b_addr, network);
    let bind = |addr| async move {
        Arc::new(DemuxSocket::wrap(
            SimulationSocket::bind(addr).await.expect("sim bind"),
            None,
        ))
    };
    let (a_socket, b_socket) = (bind(a_addr).await, bind(b_addr).await);
    let (a_keys, b_keys) = (TransportKeypair::new(), TransportKeypair::new());
    let a = QuicEndpoint::bind(a_socket.clone(), a_addr, time.clone(), &settings(), &a_keys)
        .expect("endpoint a");
    let b = QuicEndpoint::bind(b_socket.clone(), b_addr, time.clone(), &settings(), &b_keys)
        .expect("endpoint b");
    let pumps: Vec<_> = [a_socket, b_socket]
        .into_iter()
        .map(|socket| {
            tokio::spawn(async move {
                let mut buf = vec![0u8; MAX_RECV_PACKET_SIZE];
                while socket.recv_classified(&mut buf).await.is_ok() {}
            })
        })
        .collect();
    let permit = b
        .allow_peer(a_addr, a_keys.public())
        .expect("allow-set has room");
    let b_key = b_keys.public().clone();
    let connect = |a: QuicEndpoint, b: QuicEndpoint, b_key| async move {
        let (client, server) = tokio::join!(a.connect(b_addr, &b_key), async {
            b.accept()
                .await
                .expect("an incoming attempt")
                .establish()
                .await
        });
        (
            client.expect("client handshake"),
            server.expect("server accepted"),
        )
    };
    let hold = |conn: QuicConnection, time: &VirtualTime| {
        let (path, inbound) = QuicPath::new(conn, time.clone());
        Held {
            _path: path,
            _inbound: inbound,
        }
    };

    // Warm-up.
    let (client, server) = drive(&time, LIMIT, connect(a.clone(), b.clone(), b_key.clone())).await;
    drop((hold(client, &time), hold(server, &time)));
    settle(&time).await;

    let mut clients = Vec::with_capacity(n);
    let mut servers = Vec::with_capacity(n);
    let cut: crate::transport::in_memory_socket::PacketDeliveryCallback =
        Arc::new(|_, _, _, _| PacketDeliveryDecision::Drop);
    let before_drift = live_bytes();
    settle(&time).await;
    let baseline = live_bytes();
    for _ in 0..n {
        let (client, server) =
            drive(&time, LIMIT, connect(a.clone(), b.clone(), b_key.clone())).await;
        clients.push(hold(client, &time));
        servers.push(hold(server, &time));
    }
    settle(&time).await;
    set_packet_delivery_callback(network, Some(cut));
    let open = live_bytes();
    // `clear`, not `drop`: the vectors' buffers were allocated before the
    // baseline, so they stay until the last sample.
    clients.clear();
    settle(&time).await;
    let servers_only = live_bytes();
    servers.clear();
    settle(&time).await;
    let closed = live_bytes();
    drop((clients, servers));
    let cost = Cost {
        n,
        client: open - servers_only,
        server: servers_only - closed,
        retained: closed - baseline,
        settle_drift: baseline - before_drift,
    };
    tracing::info!(?cost, "idle QUIC connections measured");

    set_packet_delivery_callback(network, None);
    drop(permit);
    for pump in pumps {
        pump.abort();
    }
    unregister_network_time_source(network);
    cost
}

/// An idle QUIC connection, with the `QuicPath` a node keeps on it,
/// measures within [`QUIC_CONNECTION_FIXED_BYTES`] on either side, in
/// jemalloc-rounded bytes. Measured with 16 connections between two
/// endpoints, so the endpoints' tables have grown with them and a one-off
/// allocation is spread thin.
#[test]
fn an_idle_connection_measures_within_the_fixed_pin() {
    bounded(|| async move {
        let cost = measure("quic-memory-fixed", 16).await;
        assert!(
            cost.settle_drift.rounded.abs() <= SETTLE_DRIFT_BYTES,
            "premise: nothing was running before the baseline: {cost:?}"
        );
        assert!(
            cost.client.rounded > 0 && cost.server.rounded > 0,
            "premise: closing each side freed memory: {cost:?}"
        );
        assert!(
            cost.per_connection() <= QUIC_CONNECTION_FIXED_BYTES,
            "{} B per idle connection ({cost:?}) exceeds QUIC_CONNECTION_FIXED_BYTES \
             ({QUIC_CONNECTION_FIXED_BYTES} B); re-measure and raise it with the margin \
             its doc states",
            cost.per_connection()
        );
    })
}

/// One connection with a node's path on each side, for the stream
/// measurements, on its own network and clock.
struct StreamRig {
    time: VirtualTime,
    network: String,
    /// The client's quinn connection, to write streams by hand.
    raw: quinn::Connection,
    _held: [Held; 2],
    _permit: super::QuicPeerPermit,
    pumps: Vec<tokio::task::JoinHandle<()>>,
}

impl StreamRig {
    async fn new(network: &str) -> Self {
        let net = crate::transport::test_net::subnet();
        let (a_addr, b_addr) = (net.addr(1), net.addr(2));
        let time = VirtualTime::new();
        register_network_time_source(network, time.clone());
        register_address_network(a_addr, network);
        register_address_network(b_addr, network);
        let bind = |addr| async move {
            Arc::new(DemuxSocket::wrap(
                SimulationSocket::bind(addr).await.expect("sim bind"),
                None,
            ))
        };
        let (a_socket, b_socket) = (bind(a_addr).await, bind(b_addr).await);
        let (a_keys, b_keys) = (TransportKeypair::new(), TransportKeypair::new());
        let a = QuicEndpoint::bind(a_socket.clone(), a_addr, time.clone(), &settings(), &a_keys)
            .expect("endpoint a");
        let b = QuicEndpoint::bind(b_socket.clone(), b_addr, time.clone(), &settings(), &b_keys)
            .expect("endpoint b");
        let pumps = [a_socket, b_socket]
            .into_iter()
            .map(|socket| {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; MAX_RECV_PACKET_SIZE];
                    while socket.recv_classified(&mut buf).await.is_ok() {}
                })
            })
            .collect();
        let permit = b
            .allow_peer(a_addr, a_keys.public())
            .expect("allow-set has room");
        let b_key = b_keys.public().clone();
        let (client, server) = drive(&time, LIMIT, async move {
            let (client, server) = tokio::join!(a.connect(b_addr, &b_key), async {
                b.accept()
                    .await
                    .expect("an incoming attempt")
                    .establish()
                    .await
            });
            (
                client.expect("client handshake"),
                server.expect("server accepted"),
            )
        })
        .await;
        let raw = client.inner().clone();
        let hold = |conn: QuicConnection, time: &VirtualTime| {
            let (path, inbound) =
                QuicPath::with_limits(conn, time.clone(), QuicPathLimits::FULL_STREAMS);
            Held {
                _path: path,
                _inbound: inbound,
            }
        };
        let held = [hold(client, &time), hold(server, &time)];
        Self {
            time,
            network: network.to_string(),
            raw,
            _held: held,
            _permit: permit,
            pumps,
        }
    }
}

impl Drop for StreamRig {
    fn drop(&mut self) {
        for pump in &self.pumps {
            pump.abort();
        }
        unregister_network_time_source(&self.network);
    }
}

/// What `streams` open uni streams cost on one connection, both sides
/// together: quinn's per-stream state on the sender and the receiver, and
/// the receiver's reader for each (`path::inbound`), with one byte written
/// on each and the stream left open. A warm-up stream first, a settled
/// baseline, then the streams, settled, against it.
async fn measure_open_streams(network: &str, streams: usize) -> Live {
    let _seed = GlobalRng::seed_guard(SEED);
    let rig = StreamRig::new(network).await;
    let (time, raw) = (rig.time.clone(), rig.raw.clone());
    let warm = raw.clone();
    drive(&time, LIMIT, async move {
        let mut send = warm.open_uni().await.expect("open");
        send.write_all(&[1]).await.expect("write");
        send.finish().expect("finish");
    })
    .await;
    settle(&time).await;
    let baseline = live_bytes();
    let opened = drive(&time, LIMIT, async move {
        let mut held = Vec::with_capacity(streams);
        for _ in 0..streams {
            let mut send = raw.open_uni().await.expect("open");
            send.write_all(&[1]).await.expect("write");
            held.push(send);
        }
        held
    })
    .await;
    settle(&time).await;
    let cost = live_bytes() - baseline;
    drop(opened);
    cost
}

/// What `streams` streams leave behind on one connection, both sides together,
/// once each was opened, had `stopped()` polled if `poll_stopped` (as every
/// send of a path's does, `Outgoing`), and was reset by the sender, settled.
///
/// The total also holds a harness artifact of about 100 B per stream: each
/// inbound stream's reader arms a timer, `VirtualSleep` has no drop
/// deregistration, so `VirtualTime`'s wakeup queues reach about `streams`
/// entries, and a `Vec`/`BinaryHeap` keeps its peak capacity once they fire.
/// Production timers deregister on drop. Compare two runs to cancel it.
async fn measure_reset_streams(network: &str, streams: usize, poll_stopped: bool) -> Live {
    use futures::FutureExt;
    let _seed = GlobalRng::seed_guard(SEED);
    let rig = StreamRig::new(network).await;
    let (time, raw) = (rig.time.clone(), rig.raw.clone());
    let reset_one = move |raw: quinn::Connection| async move {
        let mut send = raw.open_uni().await.expect("open");
        send.write_all(&[1]).await.expect("write");
        if poll_stopped {
            // Registers the stream in quinn's `stopped` map.
            assert!(send.stopped().now_or_never().is_none());
        }
        send.reset(0u32.into()).expect("reset");
    };
    let warm = raw.clone();
    drive(&time, LIMIT, reset_one(warm)).await;
    settle(&time).await;
    let baseline = live_bytes();
    drive(&time, LIMIT, async move {
        for _ in 0..streams {
            reset_one(raw.clone()).await;
        }
    })
    .await;
    settle(&time).await;
    live_bytes() - baseline
}

/// Polling `stopped()` on a stream this side then resets costs at most
/// [`QUIC_STOPPED_ENTRY_BYTES`] per stream until the connection closes, in
/// jemalloc-rounded bytes: quinn's `stopped` map entry. Measured as the
/// difference between 1,024 resets with the poll and 1,024 without it, on
/// connections of their own, so everything else a reset stream leaves (and the
/// harness's timer queues, see [`measure_reset_streams`]) cancels out.
#[test]
fn polling_stopped_on_a_reset_stream_costs_at_most_its_entry() {
    bounded(|| async move {
        let streams = 1024;
        let polled = measure_reset_streams("quic-memory-stopped", streams, true).await;
        let plain = measure_reset_streams("quic-memory-unpolled", streams, false).await;
        let entries = polled.rounded - plain.rounded;
        assert!(
            entries > 0,
            "premise: the polls left memory: {polled:?} against {plain:?}"
        );
        let per_stream = (entries as usize).div_ceil(streams);
        assert!(
            per_stream <= QUIC_STOPPED_ENTRY_BYTES,
            "{per_stream} B per stopped() entry ({polled:?} against {plain:?}) exceeds \
             QUIC_STOPPED_ENTRY_BYTES ({QUIC_STOPPED_ENTRY_BYTES} B); re-measure and raise it \
             with the margin its doc states"
        );
    })
}

/// An open uni stream with one byte on it, held by a node's path on both
/// sides, measures within [`QUIC_OPEN_STREAM_BYTES`], both sides together, in
/// jemalloc-rounded bytes. Measured with `STREAMED_SEND_LIMIT` streams on one
/// connection.
#[test]
fn an_open_stream_measures_within_its_pin() {
    bounded(|| async move {
        // Not the whole stream limit: the warm-up stream's slot comes back only
        // once more than an eighth of the limit is freed.
        let streams = super::config::STREAMED_SEND_LIMIT as usize;
        let cost = measure_open_streams("quic-memory-streams", streams).await;
        assert!(
            cost.rounded > 0,
            "premise: the streams held memory: {cost:?}"
        );
        let per_stream = (cost.rounded as usize).div_ceil(streams);
        assert!(
            per_stream <= QUIC_OPEN_STREAM_BYTES,
            "{per_stream} B per open stream ({cost:?}) exceeds QUIC_OPEN_STREAM_BYTES \
             ({QUIC_OPEN_STREAM_BYTES} B); re-measure and raise it with the margin its doc \
             states"
        );
    })
}

/// One quinn-proto endpoint with at most one connection, driven by hand.
pub(super) struct ProtoPeer {
    pub(super) addr: SocketAddr,
    pub(super) endpoint: quinn_proto::Endpoint,
    pub(super) conn: Option<(quinn_proto::ConnectionHandle, quinn_proto::Connection)>,
}

impl ProtoPeer {
    pub(super) fn conn(&mut self) -> &mut quinn_proto::Connection {
        &mut self.conn.as_mut().expect("connected").1
    }

    /// Hand the endpoint one datagram from `from`, copied into its own
    /// buffer of exactly its length, as quinn's endpoint driver does
    /// (`quinn/src/endpoint.rs`, `buf[0..meta.len].into()`), so a frame
    /// kept from it keeps that allocation alive. Returns any datagram the
    /// endpoint answers with directly.
    pub(super) fn receive(
        &mut self,
        now: Instant,
        from: SocketAddr,
        datagram: &[u8],
    ) -> Vec<Vec<u8>> {
        let mut buf = Vec::new();
        let mut out = Vec::new();
        match self.endpoint.handle(
            now,
            from,
            None,
            None,
            bytes::BytesMut::from(datagram),
            &mut buf,
        ) {
            Some(quinn_proto::DatagramEvent::ConnectionEvent(_, event)) => {
                self.conn().handle_event(event)
            }
            Some(quinn_proto::DatagramEvent::NewConnection(incoming)) => {
                let accepted = self
                    .endpoint
                    .accept(incoming, now, &mut buf, None)
                    .unwrap_or_else(|error| panic!("accept: {:?}", error.cause));
                self.conn = Some(accepted);
            }
            Some(quinn_proto::DatagramEvent::Response(transmit)) => {
                out.push(buf[..transmit.size].to_vec())
            }
            None => {}
        }
        self.pump(now);
        out
    }

    /// Run due timers and pass endpoint events both ways.
    pub(super) fn pump(&mut self, now: Instant) {
        let Some((handle, conn)) = self.conn.as_mut() else {
            return;
        };
        if conn.poll_timeout().is_some_and(|at| at <= now) {
            conn.handle_timeout(now);
        }
        while let Some(event) = conn.poll_endpoint_events() {
            if let Some(event) = self.endpoint.handle_event(*handle, event) {
                conn.handle_event(event);
            }
        }
        while conn.poll().is_some() {}
    }

    /// Every datagram the connection wants to send now.
    pub(super) fn transmit(&mut self, now: Instant) -> Vec<Vec<u8>> {
        self.pump(now);
        let Some((_, conn)) = self.conn.as_mut() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let mut buf = Vec::new();
        while let Some(transmit) = conn.poll_transmit(now, 1, &mut buf) {
            out.push(buf[..transmit.size].to_vec());
            buf.clear();
        }
        out
    }
}

/// One uni stream as a peer sends it: a byte at offset 0 whose datagram
/// never arrives, then `frames` STREAM frames of `frame_len` bytes at the
/// offsets after it, one frame per datagram, every datagram padded to the
/// 1200-byte MTU. Nothing is readable past the gap, so nothing is read.
#[derive(Debug, Clone, Copy)]
struct Shape {
    frame_len: usize,
    frames: usize,
}

impl Shape {
    /// `frames` one-byte frames.
    fn one_byte(frames: usize) -> Self {
        Self {
            frame_len: 1,
            frames,
        }
    }

    /// As many `frame_len`-byte frames as `credit` bytes of flow-control
    /// credit allow, the byte at offset 0 included.
    fn filling(frame_len: usize, credit: usize) -> Self {
        Self {
            frame_len,
            frames: (credit - 1) / frame_len,
        }
    }

    /// The flow-control credit the stream uses.
    fn credit(&self) -> usize {
        1 + self.frame_len * self.frames
    }
}

/// What the receiver held while a peer fed it [`Shape`]s.
#[derive(Debug, Clone)]
struct GapHold {
    /// The largest rounded hold (stream state and the data quinn holds,
    /// net) after any frame.
    peak: isize,
    /// The largest rounded hold at any instant, inside a receive call
    /// included.
    transient: isize,
    /// The receiver's sending, net: the acknowledgement-only packets it
    /// tracks after its last ack-eliciting send (none is acknowledged here).
    acks_tracked: Live,
    /// Packets the receiver sent while being fed.
    receiver_sent: u64,
}

/// A receiver built with the node's own endpoint, transport, server and TLS
/// configuration (window for a 2 GiB node) and a sender whose datagrams the
/// test delivers or withholds, both quinn-proto endpoints driven by hand and
/// connected; and the time.
///
/// The sender pads every datagram to the MTU and has a 1 GiB congestion
/// window, so neither its window nor its pacer delays a frame.
fn connected_pair(settings: &QuicSettings) -> (ProtoPeer, ProtoPeer, Instant) {
    let mut cubic = quinn::congestion::CubicConfig::default();
    cubic.initial_window(1 << 30);
    connected_pair_with_factory(settings, Arc::new(cubic))
}

/// [`connected_pair`] at this module's settings, with the sender's
/// congestion controllers built by `sender_controller`.
pub(super) fn connected_pair_with(
    sender_controller: Arc<dyn quinn::congestion::ControllerFactory + Send + Sync>,
) -> (ProtoPeer, ProtoPeer, Instant) {
    connected_pair_with_factory(&settings(), sender_controller)
}

/// Send one uni stream of `len` bytes from `sender` to `receiver` (a
/// [`connected_pair`]), the receiver reading everything, and run both for
/// two seconds of their time so every acknowledgement is delivered.
pub(super) fn transfer_one_stream(
    receiver: &mut ProtoPeer,
    sender: &mut ProtoPeer,
    mut now: Instant,
    len: usize,
) -> Instant {
    let id = sender
        .conn()
        .streams()
        .open(quinn_proto::Dir::Uni)
        .expect("a stream credit");
    let data = vec![7u8; len];
    let mut written = 0;
    let mut accepted = Vec::new();
    for _ in 0..2000 {
        if written < len {
            if let Ok(n) = sender.conn().send_stream(id).write(&data[written..]) {
                written += n;
            }
            if written == len {
                sender.conn().send_stream(id).finish().expect("finish");
            }
        }
        for datagram in sender.transmit(now) {
            for reply in receiver.receive(now, sender.addr, &datagram) {
                sender.receive(now, receiver.addr, &reply);
            }
        }
        while let Some(stream) = receiver.conn().streams().accept(quinn_proto::Dir::Uni) {
            accepted.push(stream);
        }
        for stream in &accepted {
            let mut recv = receiver.conn().recv_stream(*stream);
            if let Ok(mut chunks) = recv.read(true) {
                while let Ok(Some(_)) = chunks.next(usize::MAX) {}
                let _transmit = chunks.finalize();
            }
        }
        for datagram in receiver.transmit(now) {
            for reply in sender.receive(now, receiver.addr, &datagram) {
                receiver.receive(now, sender.addr, &reply);
            }
        }
        now += Duration::from_millis(1);
    }
    assert_eq!(written, len, "the stream was written");
    now
}

/// [`connected_pair`] with the sender's congestion controllers built by
/// `sender_controller`.
fn connected_pair_with_factory(
    settings: &QuicSettings,
    sender_controller: Arc<dyn quinn::congestion::ControllerFactory + Send + Sync>,
) -> (ProtoPeer, ProtoPeer, Instant) {
    use crate::simulation::RealTime;

    let (a_keys, b_keys) = (TransportKeypair::new(), TransportKeypair::new());
    let a_identity = super::identity::QuicIdentity::new(&a_keys).expect("a's identity");
    let b_identity = super::identity::QuicIdentity::new(&b_keys).expect("b's identity");
    let a_peer = super::identity::PeerKey::of(a_keys.public()).expect("a's key");
    let b_peer = super::identity::PeerKey::of(b_keys.public()).expect("b's key");
    let real = RealTime::new();

    let mut receiver_transport = super::config::transport_config(settings, &real);
    // The receiver is a data path, which raises the endpoint's initial stream
    // limit (`QuicPath::with_budget`).
    receiver_transport
        .max_concurrent_uni_streams(quinn::VarInt::from_u32(super::config::MAX_UNI_STREAMS));
    let receiver_transport = Arc::new(receiver_transport);
    let server = super::config::server_config(
        Arc::new(b_identity.server_tls(&a_peer).expect("server tls")),
        receiver_transport,
        super::config::system_clock(real.clone()),
    )
    .expect("server config");
    let net = crate::transport::test_net::subnet();
    let mut receiver = ProtoPeer {
        addr: net.addr(2),
        endpoint: quinn_proto::Endpoint::new(
            Arc::new(super::config::endpoint_config().expect("endpoint config")),
            Some(Arc::new(server)),
            false,
            None,
        ),
        conn: None,
    };

    let mut sender_transport = super::config::transport_config(settings, &real);
    sender_transport
        .pad_to_mtu(true)
        .congestion_controller_factory(sender_controller);
    let client = super::config::client_config(
        Arc::new(a_identity.client_tls(&b_peer).expect("client tls")),
        Arc::new(sender_transport),
    );
    let mut sender = ProtoPeer {
        addr: net.addr(1),
        endpoint: quinn_proto::Endpoint::new(
            Arc::new(super::config::endpoint_config().expect("endpoint config")),
            None,
            false,
            None,
        ),
        conn: None,
    };
    let mut now = Instant::now();
    sender.conn = Some(
        sender
            .endpoint
            .connect(now, client, receiver.addr, super::config::SERVER_NAME)
            .expect("connect"),
    );

    // Handshake, both ways, until neither side has anything to send.
    for _ in 0..200 {
        let mut quiet = true;
        for datagram in sender.transmit(now) {
            quiet = false;
            for reply in receiver.receive(now, sender.addr, &datagram) {
                sender.receive(now, receiver.addr, &reply);
            }
        }
        for datagram in receiver.transmit(now) {
            quiet = false;
            for reply in sender.receive(now, receiver.addr, &datagram) {
                receiver.receive(now, sender.addr, &reply);
            }
        }
        if quiet && !sender.conn().is_handshaking() && !receiver.conn().is_handshaking() {
            break;
        }
        now += Duration::from_millis(1);
    }
    assert!(
        !sender.conn().is_handshaking() && !receiver.conn().is_handshaking(),
        "the handshake completed"
    );
    (receiver, sender, now)
}

/// Feed a receiver ([`connected_pair`]) one uni stream per [`Shape`], in
/// order, and measure what it holds.
///
/// quinn's public API writes a stream in order and resends what is lost,
/// so the gap is made by withholding a datagram: the sender writes the byte
/// at offset 0, its datagram is dropped, then each frame goes out in a
/// datagram of its own. The test drops every datagram the receiver sends, so
/// the sender learns nothing of the lost byte from acknowledgements; and it would resend
/// it only on a PTO, which counts from its last ack-eliciting send (about
/// 26 ms here, from the handshake's RTT). Sends 100 us apart keep that
/// timer, and every other sender timer, from falling due; that is asserted
/// before every send, and that nothing was resent at the end. Only the
/// receiver's heap is counted: every receiver call runs inside
/// [`live_delta`].
fn measure_gap_hold(shapes: &[Shape]) -> GapHold {
    measure_gap_hold_at(&settings(), shapes)
}

/// [`measure_gap_hold_at`] with nothing read, as a stream in its ordered
/// phase (header, metadata, short or control body) is while its first bytes
/// are missing.
fn measure_gap_hold_at(settings: &QuicSettings, shapes: &[Shape]) -> GapHold {
    measure_gap_hold_reading(settings, shapes, false)
}

/// [`measure_gap_hold`] with the window `settings` give. With `unordered`, the
/// receiver reads every stream unordered after each datagram, as the path's
/// reader does in a Stream's payload phase, and drops what it read: what is
/// measured is then quinn's own hold, without the charged buffer the reader
/// copies into.
fn measure_gap_hold_reading(settings: &QuicSettings, shapes: &[Shape], unordered: bool) -> GapHold {
    use quinn_proto::Dir;

    let _seed = GlobalRng::seed_guard(SEED);
    let (mut receiver, mut sender, mut now) = connected_pair(settings);
    let window = settings.window_bytes() as usize;
    let credit: usize = shapes.iter().map(Shape::credit).sum();
    assert!(credit <= window, "{credit} B of streams exceed the window");
    let sent_before = receiver.conn().stats().path.sent_packets;

    let step = Duration::from_micros(100);
    let send = |sender: &mut ProtoPeer, now: Instant, id, payload: &[u8]| {
        assert!(
            sender.conn().poll_timeout().is_none_or(|at| at > now),
            "a sender timer fell due, so it may resend the withheld byte"
        );
        let written = sender.conn().send_stream(id).write(payload).expect("write");
        assert_eq!(written, payload.len(), "flow control took the whole frame");
        let datagrams = sender.transmit(now);
        assert_eq!(datagrams.len(), 1, "one frame per datagram");
        datagrams.into_iter().next().expect("one datagram")
    };
    let mut held = Live::default();
    let mut acks_tracked = Live::default();
    let mut peak = 0isize;
    let mut transient = 0isize;
    let mut accepted = Vec::new();
    for shape in shapes {
        let id = sender
            .conn()
            .streams()
            .open(Dir::Uni)
            .expect("the receiver allows another uni stream");
        // Offset 0: sent, and withheld.
        now += step;
        drop(send(&mut sender, now, id, &[0]));
        let frame = vec![0x5a; shape.frame_len];
        for _ in 0..shape.frames {
            now += step;
            let datagram = send(&mut sender, now, id, &frame);
            assert_eq!(
                datagram.len(),
                usize::from(super::config::MAX_UDP_PAYLOAD),
                "padded to the MTU"
            );
            let (_, delta, inside) = peak_delta(|| {
                receiver.receive(now, sender.addr, &datagram);
                if unordered {
                    while let Some(stream) = receiver.conn().streams().accept(Dir::Uni) {
                        accepted.push(stream);
                    }
                    for stream in &accepted {
                        let mut recv = receiver.conn().recv_stream(*stream);
                        if let Ok(mut chunks) = recv.read(false) {
                            while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
                                drop(chunk);
                            }
                            // The receiver transmits after each datagram anyway.
                            let _should_transmit: quinn_proto::ShouldTransmit = chunks.finalize();
                        }
                    }
                }
            });
            transient = transient.max(held.rounded + inside);
            // The receiver's acknowledgements are dropped.
            let (_, acks) = live_delta(|| drop(receiver.transmit(now)));
            held += delta;
            acks_tracked += acks;
            peak = peak.max(held.rounded);
        }
    }
    assert!(
        !receiver.conn().is_closed(),
        "the receiver kept the connection open"
    );
    assert_eq!(
        sender.conn().stats().path.lost_packets,
        0,
        "the sender declared nothing lost"
    );
    GapHold {
        peak,
        transient,
        acks_tracked,
        receiver_sent: receiver.conn().stats().path.sent_packets - sent_before,
    }
}

/// The receiver tracked the acknowledgement-only packets it sent after its
/// last ack-eliciting send up to quinn-proto's cap of 1000; with none of
/// them acknowledged, they measured within [`QUIC_ACK_TAIL_BYTES`], and the
/// same after twice as many frames.
#[test]
fn the_ack_tail_measures_within_its_pin() {
    let mut tails = Vec::new();
    for frames in [4000, 8000] {
        let hold = measure_gap_hold(&[Shape::one_byte(frames)]);
        assert!(
            hold.receiver_sent >= 2000,
            "premise: twice quinn-proto's cap of acknowledgements were sent: {hold:?}"
        );
        assert!(
            hold.acks_tracked.rounded >= 100 * 1024,
            "premise: the acknowledgements were tracked: {hold:?}"
        );
        assert!(
            hold.acks_tracked.rounded <= QUIC_ACK_TAIL_BYTES as isize,
            "{} B of acknowledgement records after {frames} frames, over \
             QUIC_ACK_TAIL_BYTES ({QUIC_ACK_TAIL_BYTES} B)",
            hold.acks_tracked.rounded
        );
        tails.push(hold.acks_tracked.rounded);
    }
    assert!(
        tails[1] <= tails[0] + 16 * 1024,
        "the tail kept rising from 4000 to 8000 frames: {tails:?}"
    );
}

/// The figures in `quic::config` were measured with the quinn, quinn-proto
/// and rustls versions in [`QUIC_MEMORY_MEASURED_WITH`]. A bump of any fails
/// here until someone re-runs the measurements above, updates the figures
/// in the constants' docs, and moves the pin.
#[test]
fn quic_memory_figures_match_the_locked_versions() {
    for (name, measured) in QUIC_MEMORY_MEASURED_WITH {
        let locked = super::tests::locked_versions(name);
        assert_eq!(
            locked,
            [measured],
            "Cargo.lock locks {name} at {locked:?}, the QUIC memory figures were measured \
             with {measured}: re-measure (quic::memory_tests) before moving the pin"
        );
    }
}

/// The fixed charge per connection is the sum the buffer-budget design gives
/// (888 KiB, the `stopped` map at the reset limit's worst case), and the budget
/// for what scales with the windows is a sixteenth of node memory. A change to
/// any term moves the first figure, so the design's connection caps are
/// re-derived with it.
#[test]
fn the_fixed_charge_and_the_buffer_budget_match_the_design() {
    use super::config::{QUIC_CONNECTION_CHARGE_BYTES, quic_buffer_budget_for};
    assert_eq!(
        super::recycle::SEND_ABORT_LIMIT as usize * QUIC_STOPPED_ENTRY_BYTES,
        416 * 1024
    );
    assert_eq!(QUIC_CONNECTION_CHARGE_BYTES, 909_312);
    const MIB: usize = 1024 * 1024;
    assert_eq!(quic_buffer_budget_for(1024 * MIB), 64 * MIB);
    assert_eq!(quic_buffer_budget_for(2048 * MIB), 128 * MIB);
    assert_eq!(quic_buffer_budget_for(7600 * MIB), 475 * MIB);
}

/// Every stream is read unordered from its first byte (`path::inbound`), so
/// quinn hands over every chunk as it arrives, gap or not: with the stream's
/// first byte withheld and the reader reading after each datagram, quinn
/// holds at most the window's worth, within the `W + debt` the buffer budget
/// charges a data path for
/// received data that is not read yet (`budget::receive_charge`). Measured at
/// the floor window and at the largest a path grows to.
#[test]
fn an_unordered_reader_leaves_quinn_within_the_window_charge() {
    for window in [
        super::budget::MIN_WINDOW_BYTES,
        super::budget::MAX_WINDOW_BYTES,
    ] {
        let settings = QuicSettings {
            memory_limit: 2 * 1024 * 1024 * 1024,
            idle_timeout: None,
            initial_window: Some(window),
            rate_limit: Default::default(),
        };
        let w = window as usize;
        for len in [444, 470] {
            let held = measure_gap_hold_reading(&settings, &[Shape::filling(len, w)], false);
            assert!(
                held.peak > w as isize,
                "premise: unread, {len} B frames held more than W = {w}: {} B",
                held.peak
            );
            let read = measure_gap_hold_reading(&settings, &[Shape::filling(len, w)], true);
            assert!(
                read.transient <= w as isize,
                "read unordered, {len} B frames at W = {w}: quinn held {} B, over the window",
                read.transient
            );
        }
    }
}
