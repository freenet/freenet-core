//! Heap measurements of the QUIC endpoint (the QUIC memory gate, step M1b,
//! #5940).
//!
//! Each measurement counts the bytes left allocated on the test's thread
//! (`util::alloc_count`), rounded to jemalloc's size classes, so everything
//! measured must allocate and free on that thread. The idle-connection
//! measurement runs both endpoints, quinn's drivers and the paths' receive
//! tasks on the test's current-thread runtime, over `SimulationSocket` on
//! one `VirtualTime`, with `GlobalRng` seeded, so a run is repeatable. The
//! receive-side stream measurements drive quinn-proto directly, with no
//! runtime at all. The send-side measurements ([`measure_send`]) run real
//! `QuicPath` sends the same way as the idle one, over a link that delays
//! and loses datagrams, and count what the sending side frees when it is
//! dropped.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::config::{
    QUIC_ACK_TAIL_BYTES, QUIC_ASSEMBLER_HOLD_PER_STREAM_BYTES, QUIC_ASSEMBLER_HOLD_WINDOW_MULTIPLE,
    QUIC_CONNECTION_FIXED_BYTES, QUIC_MEMORY_MEASURED_WITH, QUIC_SEND_HOLD_PER_STREAM_BYTES,
    QUIC_SEND_HOLD_WINDOW_PERCENT, QUIC_SEND_SETTLED_BYTES, QuicSettings,
};
use bytes::Bytes;

use super::path::{QuicInbound, QuicPath, SendDeadlines};
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

/// The settings of a node with 2 GiB of memory (the shipped `MemoryMax`) and
/// the default 200 connections: a window of 268,435 bytes.
fn settings() -> QuicSettings {
    QuicSettings {
        max_connections: 200,
        memory_limit: 2 * 1024 * 1024 * 1024,
        idle_timeout: None,
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
async fn measure(network: &str, subnet: u8, n: usize) -> Cost {
    let _seed = GlobalRng::seed_guard(SEED);
    let a_addr: SocketAddr = format!("10.204.{subnet}.1:5000").parse().unwrap();
    let b_addr: SocketAddr = format!("10.204.{subnet}.2:5000").parse().unwrap();
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
        let cost = measure("quic-memory-fixed", 1, 16).await;
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
    /// The receiver's packet processing, net, after the last frame: stream
    /// state and the data its assembler holds.
    held: Live,
    /// The largest rounded [`Self::held`] after any frame.
    peak: isize,
    /// The largest rounded hold at any instant, inside a receive call
    /// included: compaction allocates the copy while the chunks it copies
    /// are still alive.
    transient: isize,
    /// The receiver's sending, net: the acknowledgement-only packets it
    /// tracks after its last ack-eliciting send (none is acknowledged here).
    acks_tracked: Live,
    /// Packets the receiver sent while being fed.
    receiver_sent: u64,
    /// Rounded [`Self::held`] after each frame of the first stream.
    first_stream_curve: Vec<isize>,
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

    let receiver_transport = Arc::new(super::config::transport_config(settings, &real));
    let server = super::config::server_config(
        Arc::new(b_identity.server_tls(&a_peer).expect("server tls")),
        receiver_transport,
        super::config::system_clock(real.clone()),
    )
    .expect("server config");
    let mut receiver = ProtoPeer {
        addr: "10.205.0.2:5000".parse().unwrap(),
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
        addr: "10.205.0.1:5000".parse().unwrap(),
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

/// [`measure_gap_hold`] with the window `settings` give.
fn measure_gap_hold_at(settings: &QuicSettings, shapes: &[Shape]) -> GapHold {
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
    let mut first_stream_curve = Vec::new();
    for (index, shape) in shapes.iter().enumerate() {
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
            let (_, delta, inside) = peak_delta(|| receiver.receive(now, sender.addr, &datagram));
            transient = transient.max(held.rounded + inside);
            // The receiver's acknowledgements are dropped.
            let (_, acks) = live_delta(|| drop(receiver.transmit(now)));
            held += delta;
            acks_tracked += acks;
            peak = peak.max(held.rounded);
            if index == 0 {
                first_stream_curve.push(held.rounded);
            }
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
        held,
        peak,
        transient,
        acks_tracked,
        receiver_sent: receiver.conn().stats().path.sent_packets - sent_before,
        first_stream_curve,
    }
}

/// The node's flow-control window at 2 GiB.
fn window() -> usize {
    settings().window_bytes() as usize
}

/// What the assembler pins allow for `streams` uni streams at window `w`:
/// `streams x QUIC_ASSEMBLER_HOLD_PER_STREAM_BYTES` plus
/// `QUIC_ASSEMBLER_HOLD_WINDOW_MULTIPLE x w`.
fn assembler_pin(streams: usize, w: usize) -> isize {
    (streams * QUIC_ASSEMBLER_HOLD_PER_STREAM_BYTES + QUIC_ASSEMBLER_HOLD_WINDOW_MULTIPLE * w)
        as isize
}

/// With one-byte frames above a gap, each in a padded datagram, a stream
/// held about one datagram per frame until the assembler's waste estimate
/// passed 32 KiB and it compacted: one stream's peak is within
/// [`QUIC_ASSEMBLER_HOLD_PER_STREAM_BYTES`], and [`MAX_UNI_STREAMS`] such
/// streams, which use almost none of the window, are within that many
/// times the pin.
///
/// [`MAX_UNI_STREAMS`]: super::config::MAX_UNI_STREAMS
#[test]
fn one_byte_frames_measure_within_the_per_stream_pin() {
    let single = measure_gap_hold(&[Shape::one_byte(64)]);
    let curve = &single.first_stream_curve;
    let peak = *curve.iter().max().expect("frames");
    let at = curve.iter().position(|held| *held == peak).expect("peak");
    assert!(
        peak <= QUIC_ASSEMBLER_HOLD_PER_STREAM_BYTES as isize,
        "one stream peaked at {peak} B: {single:?}"
    );
    let after = *curve
        .get(at + 1)
        .unwrap_or_else(|| panic!("premise: the stream compacted within the frames: {curve:?}"));
    assert!(
        peak >= 32 * 1024 && after < 8 * 1024,
        "premise: datagrams were held until compaction, then copied out: {curve:?}"
    );

    let streams = super::config::MAX_UNI_STREAMS as usize;
    let many = measure_gap_hold(&vec![Shape::one_byte(at + 1); streams]);
    let pinned = (streams * QUIC_ASSEMBLER_HOLD_PER_STREAM_BYTES) as isize;
    assert!(
        many.transient <= pinned,
        "{streams} streams of {} one-byte frames held {} B, over the pinned {pinned} B: \
         {many:?}",
        at + 1,
        many.transient
    );
    assert!(
        many.held.rounded >= streams as isize * 32 * 1024,
        "premise: every stream held its datagrams: {many:?}"
    );
}

/// With frames long enough that the proportional compaction threshold
/// governs, a stream held a datagram per frame up to the window, and
/// compaction copied while the old chunks were alive. The hold at any
/// instant is within [`assembler_pin`] for the shapes the manual sweep in
/// the constants' docs found worst and the ways to combine them: one stream
/// filling the window at 444 B (highest at any instant at 2 GiB, 4.115 W),
/// at 470 B (highest between packets, 3.05 W), both also at the smallest
/// and largest windows, and at 50 B (thousands of short frames, compacted
/// again and again); 64 streams sharing the window; and
/// `MAX_UNI_STREAMS - 1` one-byte streams beside one that fills the rest.
/// All datagrams are padded to 1200 B; other sizes are not measured.
#[test]
fn frames_filling_the_window_measure_within_the_window_multiple() {
    let w = window();
    let one_byte_streams = super::config::MAX_UNI_STREAMS as usize - 1;
    let mut mixed = vec![Shape::one_byte(28); one_byte_streams];
    let used: usize = mixed.iter().map(Shape::credit).sum();
    mixed.push(Shape::filling(444, w - used));
    let cases: Vec<(&str, Vec<Shape>)> = vec![
        ("444 B frames", vec![Shape::filling(444, w)]),
        ("470 B frames", vec![Shape::filling(470, w)]),
        ("50 B frames", vec![Shape::filling(50, w)]),
        (
            "64 streams of 460 B frames",
            vec![Shape::filling(460, w / 64); 64],
        ),
        ("one-byte streams and one of 444 B frames", mixed),
    ];
    for (name, shapes) in cases {
        let hold = measure_gap_hold(&shapes);
        let pinned = assembler_pin(shapes.len(), w);
        assert!(
            hold.transient <= pinned,
            "{name}: held {} B at its highest, over the pinned {pinned} B: {hold:?}",
            hold.transient
        );
        if name == "470 B frames" {
            assert!(
                hold.peak as f64 >= 2.8 * w as f64,
                "premise: no compaction, a datagram held per frame: {} B",
                hold.peak
            );
        }
        if name == "444 B frames" {
            assert!(
                hold.transient as f64 >= 3.5 * w as f64,
                "premise: the copy overlapped the old chunks, over 3.5 x W: {} B",
                hold.transient
            );
        }
        if name.starts_with("one-byte") {
            assert!(
                hold.held.rounded >= one_byte_streams as isize * 32 * 1024,
                "premise: the one-byte streams held their datagrams: {hold:?}"
            );
        }
    }

    // The same two lengths at the smallest window (MIN_WINDOW_BYTES, a
    // 512 MiB node) and the largest (MAX_WINDOW_BYTES, a 64 GiB node). At
    // the largest, these lengths make over 2048 chunks, so the chunk-count
    // trigger compacts them; the premise is only that the stream held more
    // than the window.
    for (memory_limit, expected) in [
        (512usize << 20, super::config::MIN_WINDOW_BYTES),
        (64usize << 30, super::config::MAX_WINDOW_BYTES),
    ] {
        let settings = QuicSettings {
            max_connections: 200,
            memory_limit,
            idle_timeout: None,
            rate_limit: Default::default(),
        };
        let w = settings.window_bytes() as usize;
        assert_eq!(w, expected as usize, "premise: the window at its extreme");
        for len in [444, 470] {
            let hold = measure_gap_hold_at(&settings, &[Shape::filling(len, w)]);
            assert!(
                hold.transient <= assembler_pin(1, w),
                "{len} B frames at W = {w}: held {} B at its highest, over the pinned {} B",
                hold.transient,
                assembler_pin(1, w)
            );
            assert!(
                hold.peak as f64 >= 1.5 * w as f64,
                "premise: the stream held more than the window at W = {w}: {} B",
                hold.peak
            );
        }
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
    let lock = include_str!("../../../../../Cargo.lock").replace("\r\n", "\n");
    for (name, measured) in QUIC_MEMORY_MEASURED_WITH {
        let header = format!("[[package]]\nname = \"{name}\"\nversion = \"");
        let locked: Vec<&str> = lock
            .match_indices(&header)
            .map(|(at, _)| {
                let rest = &lock[at + header.len()..];
                &rest[..rest.find('"').expect("closing quote")]
            })
            .collect();
        assert_eq!(
            locked,
            [measured],
            "Cargo.lock locks {name} at {locked:?}, the QUIC memory figures were measured \
             with {measured}: re-measure (quic::memory_tests) before moving the pin"
        );
    }
}

// ---------------------------------------------------------------------------
// The send side, with loss (#5940, e5b-9).
// ---------------------------------------------------------------------------

/// One-way delay of every datagram on a send-side network: a 100 ms round
/// trip, so a send window's worth of data can be in flight.
const ONE_WAY: Duration = Duration::from_millis(50);

/// After the cut, how long the sides run before the sample: long enough for
/// every datagram already on the link to land and for the sender to fill
/// its send window, and short of every send deadline and inbound stream
/// limit (see [`DRAIN`]).
const HOLD: Duration = Duration::from_millis(300);

/// How long a dropped side is given to drain before its memory is counted
/// as freed: quinn keeps a closed connection for three PTOs. [`HOLD`] plus
/// this stays under 2 s, the shortest deadline that could fire on the side
/// still running (a streamed send's open deadline; the receiver's inbound
/// limits are 5 s and up), so nothing there frees memory meanwhile that
/// would be counted to the dropped side.
const DRAIN: Duration = Duration::from_millis(1600);
const _: () = assert!(HOLD.as_millis() + DRAIN.as_millis() < 2000);

/// Pending wakeups the clock has room for before the baseline
/// ([`measure_send`]): more than the datagrams a run has on the link at
/// once.
const LINK_TIMERS: usize = 16_384;

/// What a send-side network does with each datagram: drops all of them once
/// cut, drops a seeded share while lossy, and delays the rest by
/// [`ONE_WAY`]. Both directions, so acknowledgements are lost too.
#[derive(Default)]
struct Link {
    cut: std::sync::atomic::AtomicBool,
    loss_per_mille: std::sync::atomic::AtomicU32,
    rng: std::sync::atomic::AtomicU64,
    dropped: std::sync::atomic::AtomicU64,
}

impl Link {
    fn decide(&self) -> PacketDeliveryDecision {
        use std::sync::atomic::Ordering::Relaxed;
        if self.cut.load(Relaxed) {
            return PacketDeliveryDecision::Drop;
        }
        let loss = u64::from(self.loss_per_mille.load(Relaxed));
        if loss > 0 {
            // xorshift64, seeded at the start of the run.
            let mut x = self.rng.load(Relaxed);
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.rng.store(x, Relaxed);
            if x % 1000 < loss {
                self.dropped.fetch_add(1, Relaxed);
                return PacketDeliveryDecision::Drop;
            }
        }
        PacketDeliveryDecision::DelayedDelivery(ONE_WAY)
    }
}

/// How a sent message gets its bytes to the path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Payload {
    /// `send_stream_bounded` from one `Bytes` the caller already holds:
    /// quinn keeps slices of it until they are acknowledged, so the payload
    /// itself is the caller's and is allocated before the baseline.
    Held,
    /// `send_stream_chunks_bounded` from a source that allocates each
    /// fragment as it is asked for, as a forwarded stream does: what quinn
    /// keeps of it is counted.
    Source,
    /// `send_short_bounded`: quinn copies the message into its own buffers.
    Short,
}

/// `count` concurrent sends of `len` bytes each.
#[derive(Debug, Clone, Copy)]
struct Sends {
    payload: Payload,
    count: usize,
    len: usize,
}

/// When the link is cut.
#[derive(Debug, Clone, Copy)]
enum Cut {
    /// This long after the sends start, while they run.
    At(Duration),
    /// Once every send has returned and a settle has passed.
    AfterDone,
}

#[derive(Debug, Clone, Copy)]
struct SendCase {
    memory_limit: usize,
    loss_per_mille: u32,
    sends: Option<Sends>,
    /// One-packet shorts started at the cut, each on a task of its own, so
    /// they hold streams open.
    shorts_at_cut: usize,
    /// Messages of the largest one-packet length handed to a `DataRoute`'s
    /// short queue at the cut, as the connection's task does.
    queued_at_cut: usize,
    cut: Cut,
    /// From the cut to the sample: [`HOLD`], or longer to let deadlines
    /// pass.
    hold: Duration,
}

impl SendCase {
    fn new(limit: usize, loss_per_mille: u32, sends: Option<Sends>, cut: Cut) -> Self {
        Self {
            memory_limit: limit,
            loss_per_mille,
            sends,
            shorts_at_cut: 0,
            queued_at_cut: 0,
            cut,
            hold: HOLD,
        }
    }

    /// Uni streams the case's sends open at most at once.
    fn streams(&self) -> usize {
        let sends = self.sends.map_or(0, |s| s.count);
        let queued = self
            .queued_at_cut
            .min(super::config::MAX_UNI_STREAMS as usize);
        (sends + self.shorts_at_cut + queued).min(super::config::MAX_UNI_STREAMS as usize)
    }
}

/// What a sender held at the sample, against the same run's baseline.
#[derive(Debug, Clone)]
struct SendHold {
    window: usize,
    /// Freed when the sending side was dropped and drained.
    sender: Live,
    /// Freed when the receiving side was then dropped and drained.
    receiver: Live,
    /// Still allocated after both, against the baseline.
    retained: Live,
    settle_drift: Live,
    /// How the sends that had returned by the sample returned.
    returned: Vec<String>,
    sent_packets: u64,
    lost_packets: u64,
    link_dropped: u64,
    /// Connections each endpoint still held after its side drained.
    sender_left: usize,
    receiver_left: usize,
    /// Sends running at the sample.
    running: usize,
    /// Streamed-message permits free at the sample.
    free_permits: usize,
    /// Messages the short queue took, and the ones it handed back.
    queued: usize,
    queue_refused: usize,
    /// Messages the short queue's worker sent on UDP.
    udp_sends: usize,
}

impl SendHold {
    /// What the sending side held, in jemalloc-rounded bytes: what dropping
    /// it freed, plus whatever was still allocated after both sides closed
    /// (charged to this side, as [`Cost::per_connection`] charges it to the
    /// larger side; see [`measure_send`] for what it was).
    fn sender_bytes(&self) -> isize {
        self.sender.rounded + self.retained.rounded.max(0)
    }
}

/// A short queue's UDP fallback that sends nothing and counts each call.
#[derive(Default)]
struct CountingUdp(std::sync::atomic::AtomicUsize);

impl super::upgrade::data_route::UdpShorts for CountingUdp {
    fn send_short(
        &self,
        _payload: crate::transport::connection_handler::SerializedMessage,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<(), crate::transport::TransportError>>
                + Send
                + '_,
        >,
    > {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Box::pin(async { Ok(()) })
    }
}

/// The longest message that goes as a one-packet short.
fn largest_one_packet_short() -> usize {
    (1..=usize::from(super::config::MAX_UDP_PAYLOAD))
        .rev()
        .find(|&len| crate::transport::peer_connection::fits_one_packet(len as u64))
        .expect("some length fits a packet")
}

/// The settings of a node with `limit` bytes of memory and the default 200
/// connections.
fn settings_at(limit: usize) -> QuicSettings {
    QuicSettings {
        max_connections: 200,
        memory_limit: limit,
        idle_timeout: None,
        rate_limit: Default::default(),
    }
}

/// Drain everything the receiving path delivers, reading each stream to
/// its end, as a node's consumer does.
async fn consume(inbound: QuicInbound) {
    let QuicInbound {
        mut shorts,
        mut streams,
        controls: _controls,
    } = inbound;
    let mut readers = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            short = shorts.recv() => if short.is_none() { break },
            arrival = streams.recv() => match arrival {
                Some(arrival) => {
                    readers.spawn(async move { drop(arrival.handle.assemble().await) });
                }
                None => break,
            },
            Some(_) = readers.join_next(), if !readers.is_empty() => {}
        }
    }
}

/// Run `case` between two endpoints on network `network` (subnet
/// `10.208.<subnet>`) and measure what the sending side held at the
/// sample, by dropping it and counting what it freed.
///
/// The same order as [`measure`]: a warm-up connection, a settle checked
/// for drift, the baseline, then one connection with a path on each side.
/// The sends start with the link lossy; at the cut the link drops
/// everything, so neither side hears the other again. After [`HOLD`] the
/// sending side (its send tasks, its path and its connection) is dropped
/// and drained for [`DRAIN`], then the receiving side for [`SETTLE`]. What
/// each drop frees is that side's hold at the sample.
async fn measure_send(network: &str, subnet: u8, case: SendCase) -> SendHold {
    use crate::transport::StreamId;
    use crate::transport::peer_connection::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;
    use std::sync::atomic::Ordering::Relaxed;

    let _seed = GlobalRng::seed_guard(SEED);
    let settings = settings_at(case.memory_limit);
    let a_addr: SocketAddr = format!("10.208.{subnet}.1:5000").parse().unwrap();
    let b_addr: SocketAddr = format!("10.208.{subnet}.2:5000").parse().unwrap();
    let time = VirtualTime::new();
    register_network_time_source(network, time.clone());
    register_address_network(a_addr, network);
    register_address_network(b_addr, network);
    let link = Arc::new(Link::default());
    link.rng.store(SEED | 1, Relaxed);
    {
        let link = link.clone();
        set_packet_delivery_callback(network, Some(Arc::new(move |_, _, _, _| link.decide())));
    }
    let bind = |addr| async move {
        Arc::new(DemuxSocket::wrap(
            SimulationSocket::bind(addr).await.expect("sim bind"),
            None,
        ))
    };
    let (a_socket, b_socket) = (bind(a_addr).await, bind(b_addr).await);
    let (a_keys, b_keys) = (TransportKeypair::new(), TransportKeypair::new());
    let a = QuicEndpoint::bind(a_socket.clone(), a_addr, time.clone(), &settings, &a_keys)
        .expect("endpoint a");
    let b = QuicEndpoint::bind(b_socket.clone(), b_addr, time.clone(), &settings, &b_keys)
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

    // Warm-up.
    let (client, server) = drive(&time, LIMIT, connect(a.clone(), b.clone(), b_key.clone())).await;
    drop((
        QuicPath::new(client, time.clone()),
        QuicPath::new(server, time.clone()),
    ));
    settle(&time).await;
    // Every delayed datagram is a sleep on the clock, and the clock's
    // wakeup tables keep the capacity they grow to. Reserve more than the
    // datagrams a run has on the link at once, so that growth is not counted.
    time.reserve_wakeups(LINK_TIMERS);

    // What the caller holds before the baseline: held payloads (promoted to
    // shared, so a clone allocates nothing), short payloads, handle storage.
    let sends = case.sends;
    let count = sends.map_or(0, |s| s.count);
    let held: Vec<Bytes> = match sends {
        Some(Sends {
            payload: Payload::Held,
            count,
            len,
        }) => (0..count)
            .map(|_| {
                let bytes = Bytes::from(vec![0x5a; len]);
                drop(bytes.clone());
                bytes
            })
            .collect(),
        _ => Vec::new(),
    };
    let short_payloads: Vec<Arc<Vec<u8>>> = match sends {
        Some(Sends {
            payload: Payload::Short,
            count,
            len,
        }) => (0..count).map(|_| Arc::new(vec![0x5a; len])).collect(),
        _ => Vec::new(),
    };
    let small = Arc::new(vec![0x5a; 200]);
    let mut handles = Vec::with_capacity(count + case.shorts_at_cut);
    let mut returned = Vec::with_capacity(count + case.shorts_at_cut);

    let before_drift = live_bytes();
    settle(&time).await;
    let baseline = live_bytes();

    let (client, server) = drive(&time, LIMIT, connect(a.clone(), b.clone(), b_key.clone())).await;
    let (sender, sender_in) = QuicPath::new(client, time.clone());
    let sender = Arc::new(sender);
    let (receiver, receiver_in) = QuicPath::new(server, time.clone());
    let consumer = tokio::spawn(consume(receiver_in));
    settle(&time).await;

    link.loss_per_mille.store(case.loss_per_mille, Relaxed);
    if let Some(Sends { payload, len, .. }) = sends {
        for index in 0..count {
            let path = sender.clone();
            let clock = time.clone();
            let handle = match payload {
                Payload::Held => {
                    let bytes = held[index].clone();
                    tokio::spawn(async move {
                        path.send_stream_bounded(
                            &clock,
                            SendDeadlines::stream(len as u64),
                            StreamId::next(),
                            None,
                            bytes,
                            None,
                        )
                        .await
                    })
                }
                Payload::Source => tokio::spawn(async move {
                    let fragments = len.div_ceil(FRAGMENT_PAYLOAD_SIZE);
                    let source = futures::stream::iter((0..fragments).map(move |i| {
                        let n = FRAGMENT_PAYLOAD_SIZE.min(len - i * FRAGMENT_PAYLOAD_SIZE);
                        Ok(Bytes::from(vec![0x5a; n]))
                    }));
                    path.send_stream_chunks_bounded(
                        &clock,
                        SendDeadlines::stream(len as u64),
                        StreamId::next(),
                        len as u64,
                        None,
                        source,
                        None,
                    )
                    .await
                }),
                Payload::Short => {
                    let message = short_payloads[index].clone();
                    tokio::spawn(async move {
                        path.send_short_bounded(&clock, SendDeadlines::short(len as u64), &message)
                            .await
                    })
                }
            };
            handles.push(handle);
        }
    }
    match case.cut {
        Cut::At(after) => drive(&time, LIMIT, time.sleep(after)).await,
        Cut::AfterDone => {
            let all = std::mem::take(&mut handles);
            let outcomes = drive(&time, LIMIT, async move {
                let mut outcomes = Vec::new();
                for handle in all {
                    outcomes.push(format!("{:?}", handle.await.expect("send task")));
                }
                outcomes
            })
            .await;
            returned.extend(outcomes);
            settle(&time).await;
        }
    }
    link.cut.store(true, Relaxed);
    for _ in 0..case.shorts_at_cut {
        let path = sender.clone();
        let clock = time.clone();
        let message = small.clone();
        handles.push(tokio::spawn(async move {
            path.send_short_bounded(&clock, SendDeadlines::short(200), &message)
                .await
        }));
    }
    let udp = Arc::new(CountingUdp::default());
    let mut queue = None;
    let (mut queued, mut queue_refused) = (0, 0);
    if case.queued_at_cut > 0 {
        let (route, worker) = super::upgrade::data_route::DataRoute::new(
            sender.clone(),
            super::upgrade::driver::PathClock::Virtual(time.clone()),
            udp.clone(),
            Default::default(),
        );
        let worker = tokio::spawn(worker);
        let len = largest_one_packet_short();
        for _ in 0..case.queued_at_cut {
            match route.queue_short(vec![0x5a; len]) {
                Ok(()) => queued += 1,
                Err(_) => queue_refused += 1,
            }
            // The worker takes what it may send at once before the queue
            // fills, as it would between the connection task's messages.
            drive(&time, LIMIT, async {}).await;
        }
        queue = Some((route, worker));
    }
    drive(&time, LIMIT, time.sleep(case.hold)).await;

    let open = live_bytes();
    let stats = sender.connection().inner().stats();
    let running = handles.iter().filter(|h| !h.is_finished()).count();
    let free_permits = sender.free_stream_slots();
    for handle in &handles {
        handle.abort();
    }
    if let Some((route, worker)) = queue {
        worker.abort();
        drop(route);
    }
    drop((sender, sender_in));
    drive(&time, LIMIT, time.sleep(DRAIN)).await;
    for handle in handles.drain(..) {
        if let Ok(outcome) = handle.await {
            returned.push(format!("{outcome:?}"));
        }
    }
    let sender_left = a.open_connections();
    let sender_gone = live_bytes();

    consumer.abort();
    drop(receiver);
    settle(&time).await;
    let receiver_left = b.open_connections();
    let closed = live_bytes();
    drop((held, short_payloads, small, handles));

    set_packet_delivery_callback(network, None);
    drop(permit);
    for pump in pumps {
        pump.abort();
    }
    unregister_network_time_source(network);
    SendHold {
        window: settings.window_bytes() as usize,
        sender: open - sender_gone,
        receiver: sender_gone - closed,
        retained: closed - baseline,
        settle_drift: baseline - before_drift,
        returned,
        sent_packets: stats.path.sent_packets,
        lost_packets: stats.path.lost_packets,
        link_dropped: link.dropped.load(Relaxed),
        sender_left,
        receiver_left,
        running,
        free_permits,
        queued,
        queue_refused,
        udp_sends: udp.0.load(Relaxed),
    }
}

/// The windows the send-side tests run at: the smallest (a 512 MiB node),
/// the shipped 2 GiB node's, and the largest (a 64 GiB node).
const SEND_WINDOWS: [usize; 3] = [512 << 20, 2 << 30, 64 << 30];

/// Losses the send-side tests run at, in datagrams per thousand, both ways.
const SEND_LOSSES: [u32; 3] = [0, 20, 100];

/// When a sweep cuts the link, from the start of the sends. A lossless
/// transfer of the sizes used here ends within about 1.5 s at the largest
/// window, so the cuts sample it as it grows; a lossy one runs on and holds
/// about the same from early on.
const CUTS_MS: [u64; 12] = [
    100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1200, 2500,
];

/// What the send pins allow for `streams` uni streams at window `w`:
/// `QUIC_SEND_HOLD_WINDOW_PERCENT` of `w`, plus `QUIC_SEND_SETTLED_BYTES`,
/// plus `streams x QUIC_SEND_HOLD_PER_STREAM_BYTES`.
fn send_pin(streams: usize, w: usize) -> isize {
    (w * QUIC_SEND_HOLD_WINDOW_PERCENT / 100
        + QUIC_SEND_SETTLED_BYTES
        + streams * QUIC_SEND_HOLD_PER_STREAM_BYTES) as isize
}

/// Runs of [`measure_send`] in one test, each on a network of its own on
/// the test's subnet, with the premises every run must meet checked.
struct SendRuns {
    name: &'static str,
    subnet: u8,
    runs: usize,
}

impl SendRuns {
    fn new(name: &'static str, subnet: u8) -> Self {
        Self {
            name,
            subnet,
            runs: 0,
        }
    }

    async fn run(&mut self, case: SendCase) -> SendHold {
        self.runs += 1;
        let network = format!("{}-{}", self.name, self.runs);
        let hold = measure_send(&network, self.subnet, case).await;
        tracing::info!(?case, ?hold, "QUIC send side measured");
        assert!(
            hold.settle_drift.rounded.abs() <= SETTLE_DRIFT_BYTES,
            "premise: nothing was running before the baseline: {case:?} {hold:?}"
        );
        assert_eq!(
            (hold.sender_left, hold.receiver_left),
            (0, 0),
            "premise: each side's connection was gone before its memory was counted: \
             {case:?} {hold:?}"
        );
        assert!(
            hold.sender.rounded > 0 && hold.receiver.rounded > 0,
            "premise: dropping each side freed memory: {case:?} {hold:?}"
        );
        if case.sends.is_some() || case.queued_at_cut > 0 {
            assert!(
                hold.sent_packets > 0,
                "premise: the sender sent: {case:?} {hold:?}"
            );
        }
        if case.loss_per_mille == 0 {
            assert_eq!(
                (hold.lost_packets, hold.link_dropped),
                (0, 0),
                "premise: a lossless run lost nothing before the cut: {case:?} {hold:?}"
            );
        }
        hold
    }

    /// What the sending side held at `w` with nothing sent: an idle
    /// connection with its path, subtracted from every other run's figure.
    async fn idle(&mut self, memory_limit: usize) -> isize {
        let hold = self
            .run(SendCase::new(memory_limit, 0, None, Cut::AfterDone))
            .await;
        assert!(
            hold.sender_bytes() <= QUIC_CONNECTION_FIXED_BYTES as isize,
            "premise: an idle sender measured within QUIC_CONNECTION_FIXED_BYTES: {hold:?}"
        );
        hold.sender_bytes()
    }

    /// Run `sends` cut at each of [`CUTS_MS`], `shorts_at_cut` started at
    /// each cut, and check every sample against [`send_pin`]. Returns each
    /// sample with what it held above `idle`.
    async fn sweep(
        &mut self,
        memory_limit: usize,
        loss_per_mille: u32,
        sends: Sends,
        shorts_at_cut: usize,
        idle: isize,
    ) -> Vec<(SendHold, isize)> {
        let mut samples = Vec::new();
        for ms in CUTS_MS {
            let case = SendCase {
                shorts_at_cut,
                ..SendCase::new(
                    memory_limit,
                    loss_per_mille,
                    Some(sends),
                    Cut::At(Duration::from_millis(ms)),
                )
            };
            let hold = self.run(case).await;
            let held = hold.sender_bytes() - idle;
            let pinned = send_pin(case.streams(), hold.window);
            assert!(
                held <= pinned,
                "{sends:?} at W = {}, {loss_per_mille}/1000 lost, cut at {ms} ms: the sender \
                 held {held} B above idle, over the pinned {pinned} B: {hold:?}",
                hold.window
            );
            samples.push((hold, held));
        }
        if loss_per_mille > 0 {
            assert!(
                samples.iter().any(|(hold, _)| hold.lost_packets > 0),
                "premise: quinn declared packets lost and resent them: {sends:?} at \
                 {loss_per_mille}/1000"
            );
        }
        samples
    }
}

/// The largest figure of a sweep.
fn peak(samples: &[(SendHold, isize)]) -> isize {
    samples.iter().map(|(_, held)| *held).max().unwrap_or(0)
}

/// One stream of three windows, sent while a share of datagrams is lost
/// both ways, held within [`send_pin`] for one stream at every sample: at
/// the smallest, the 2 GiB and the largest window, lossless and at 2% and
/// 10% loss. From a source that allocates each fragment (a forwarded
/// stream) and as a large short (quinn copies it), the sender held the
/// window's worth of copies, since the cut stops every acknowledgement and
/// the sender writes until its send window is full. From a payload the
/// caller holds, quinn keeps slices of it and the sender held well under
/// the window.
#[test]
fn one_stream_with_loss_measures_within_the_send_pins() {
    bounded(|| async move {
        let mut runs = SendRuns::new("quic-memory-send-one", 1);
        for memory_limit in SEND_WINDOWS {
            let idle = runs.idle(memory_limit).await;
            let w = settings_at(memory_limit).window_bytes() as usize;
            for payload in [Payload::Source, Payload::Short] {
                for loss in SEND_LOSSES {
                    let sends = Sends {
                        payload,
                        count: 1,
                        len: 3 * w,
                    };
                    let samples = runs.sweep(memory_limit, loss, sends, 0, idle).await;
                    assert!(
                        peak(&samples) >= w as isize,
                        "premise: the sender held a window's worth of copies at some cut: \
                         {sends:?} at W = {w}, {loss}/1000: {} B",
                        peak(&samples)
                    );
                }
            }
        }
        let memory_limit = 2 << 30;
        let idle = runs.idle(memory_limit).await;
        let w = settings_at(memory_limit).window_bytes() as usize;
        for loss in [0, 100] {
            let sends = Sends {
                payload: Payload::Held,
                count: 1,
                len: 3 * w,
            };
            let samples = runs.sweep(memory_limit, loss, sends, 0, idle).await;
            assert!(
                peak(&samples) < (w / 2) as isize,
                "premise: quinn kept slices of the caller's payload, not copies: {} B",
                peak(&samples)
            );
        }
    })
}

/// 48 concurrent streams (every streamed-message permit) sharing the send
/// window, with 16 one-packet shorts started at the cut, so 64 uni streams
/// (`MAX_UNI_STREAMS`) are open at the sample: held within [`send_pin`] for
/// 64 streams at every sample, at the smallest and largest window,
/// lossless and at 10% loss, from sources and as large shorts.
#[test]
fn every_stream_at_once_with_loss_measures_within_the_send_pins() {
    bounded(|| async move {
        let mut runs = SendRuns::new("quic-memory-send-many", 2);
        let streamed = super::config::STREAMED_SEND_LIMIT as usize;
        let shorts = super::config::MAX_UNI_STREAMS as usize - streamed;
        for memory_limit in [SEND_WINDOWS[0], SEND_WINDOWS[2]] {
            let idle = runs.idle(memory_limit).await;
            let w = settings_at(memory_limit).window_bytes() as usize;
            for payload in [Payload::Source, Payload::Short] {
                for loss in [0, 100] {
                    let sends = Sends {
                        payload,
                        count: streamed,
                        len: w / 8,
                    };
                    let samples = runs.sweep(memory_limit, loss, sends, shorts, idle).await;
                    assert!(
                        samples
                            .iter()
                            .any(|(hold, _)| hold.running == streamed + shorts
                                && hold.free_permits == 0),
                        "premise: at some cut every send was running, holding every permit \
                         and every uni stream: {sends:?} at W = {w}, {loss}/1000"
                    );
                    assert!(
                        peak(&samples) >= w as isize,
                        "premise: the streams held a window's worth of copies at some cut: \
                         {} B",
                        peak(&samples)
                    );
                }
            }
        }
    })
}

/// 48 streams written in full and cut before any acknowledgement, held past
/// their deadline (300 s): each send returns `MaybeDelivered` and leaves a
/// task holding its permit until the stream ends, with the data still in
/// quinn. Held within [`send_pin`] for 48 streams, at the smallest and
/// largest window.
#[test]
fn sends_past_their_deadline_measure_within_the_send_pins() {
    bounded(|| async move {
        let mut runs = SendRuns::new("quic-memory-send-detached", 3);
        let streamed = super::config::STREAMED_SEND_LIMIT as usize;
        for memory_limit in [SEND_WINDOWS[0], SEND_WINDOWS[2]] {
            let idle = runs.idle(memory_limit).await;
            let w = settings_at(memory_limit).window_bytes() as usize;
            let sends = Sends {
                payload: Payload::Source,
                count: streamed,
                len: w / 64,
            };
            let deadline = SendDeadlines::stream(sends.len as u64).total;
            let case = SendCase {
                hold: deadline + Duration::from_secs(1),
                ..SendCase::new(
                    memory_limit,
                    0,
                    Some(sends),
                    Cut::At(Duration::from_millis(1)),
                )
            };
            let hold = runs.run(case).await;
            let held = hold.sender_bytes() - idle;
            assert!(
                hold.returned.len() == streamed
                    && hold
                        .returned
                        .iter()
                        .all(|outcome| outcome.starts_with("MaybeDelivered")),
                "premise: every send was written in full and returned MaybeDelivered: {hold:?}"
            );
            assert_eq!(
                hold.free_permits, 0,
                "premise: the detached tasks held every permit: {hold:?}"
            );
            assert!(
                held >= (streamed * sends.len) as isize,
                "premise: quinn still held the unacknowledged data: {held} B: {hold:?}"
            );
            let pinned = send_pin(case.streams(), w);
            assert!(
                held <= pinned,
                "{streamed} sends past their deadline at W = {w}: {held} B above idle, over \
                 the pinned {pinned} B: {hold:?}"
            );
        }
    })
}

/// Once every send has been acknowledged, after a lossy transfer, a sender
/// holds within [`QUIC_SEND_SETTLED_BYTES`] of an idle one: what is left is
/// capacity its connection keeps. One stream and 48 streams, at the
/// smallest and largest window, lossless and at 10% loss.
#[test]
fn a_sender_returns_to_idle_after_a_lossy_transfer() {
    bounded(|| async move {
        let mut runs = SendRuns::new("quic-memory-send-settled", 4);
        let streamed = super::config::STREAMED_SEND_LIMIT as usize;
        for memory_limit in [SEND_WINDOWS[0], SEND_WINDOWS[2]] {
            let idle = runs.idle(memory_limit).await;
            let w = settings_at(memory_limit).window_bytes() as usize;
            for (count, len) in [(1, 3 * w), (streamed, w / 8)] {
                for loss in [0, 100] {
                    let sends = Sends {
                        payload: Payload::Source,
                        count,
                        len,
                    };
                    let hold = runs
                        .run(SendCase::new(
                            memory_limit,
                            loss,
                            Some(sends),
                            Cut::AfterDone,
                        ))
                        .await;
                    // At the smallest window with 10% loss, some of the 48
                    // streams wait past the 5 s gap deadline for their turn
                    // and are reset; a reset leaves an entry in quinn's
                    // `stopped` map (`path` module doc), which this counts.
                    assert!(
                        hold.returned.len() == count
                            && hold
                                .returned
                                .iter()
                                .all(|outcome| outcome == "Sent" || outcome.starts_with("Failed")),
                        "premise: every send returned before the cut: {hold:?}"
                    );
                    if loss == 0 {
                        assert!(
                            hold.returned.iter().all(|outcome| outcome == "Sent"),
                            "premise: lossless, every send was acknowledged: {hold:?}"
                        );
                    }
                    let held = hold.sender_bytes() - idle;
                    assert!(
                        held <= QUIC_SEND_SETTLED_BYTES as isize,
                        "{sends:?} at W = {w}, {loss}/1000: {held} B above idle after the \
                         transfer, over QUIC_SEND_SETTLED_BYTES ({QUIC_SEND_SETTLED_BYTES} B): \
                         {hold:?}"
                    );
                }
            }
        }
    })
}

/// The path's short queue (`DataRoute`, e5b-6) full while the link is cut:
/// `SHORTS_IN_FLIGHT` (64) one-packet shorts of the largest length on their
/// streams and `SHORT_QUEUE_CAPACITY` (256) more queued, the next one handed
/// back. The sender held within [`send_pin`] for 64 streams plus the queued
/// and in-flight messages themselves, at the smallest window.
#[test]
fn a_full_short_queue_measures_within_the_send_pins() {
    use super::upgrade::data_route::SHORT_QUEUE_CAPACITY;
    use crate::util::alloc_count::jemalloc_class;

    bounded(|| async move {
        let mut runs = SendRuns::new("quic-memory-send-queue", 5);
        let memory_limit = SEND_WINDOWS[0];
        let idle = runs.idle(memory_limit).await;
        let w = settings_at(memory_limit).window_bytes() as usize;
        let in_flight = super::config::MAX_UNI_STREAMS as usize;
        let case = SendCase {
            queued_at_cut: in_flight + SHORT_QUEUE_CAPACITY + 1,
            ..SendCase::new(memory_limit, 0, None, Cut::At(Duration::from_millis(100)))
        };
        let hold = runs.run(case).await;
        assert_eq!(
            (hold.queued, hold.queue_refused, hold.udp_sends),
            (in_flight + SHORT_QUEUE_CAPACITY, 1, 0),
            "premise: the worker took 64 and the queue filled: {hold:?}"
        );
        let held = hold.sender_bytes() - idle;
        let messages = ((in_flight + SHORT_QUEUE_CAPACITY)
            * jemalloc_class(largest_one_packet_short())) as isize;
        assert!(
            held >= messages,
            "premise: the queued messages were counted: {held} B: {hold:?}"
        );
        let pinned = send_pin(case.streams(), w) + messages;
        assert!(
            held <= pinned,
            "a full short queue: {held} B above idle, over the pinned {pinned} B: {hold:?}"
        );
    })
}
