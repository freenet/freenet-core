//! Heap measurements of the QUIC endpoint (the QUIC memory gate, step M1b).
//!
//! Each measurement counts the bytes left allocated on the test's thread
//! (`util::alloc_count`), so everything measured must allocate and free on
//! that thread: both endpoints, quinn's drivers and the paths' receive tasks
//! run on the test's current-thread runtime, over `SimulationSocket` on one
//! `VirtualTime`, with `GlobalRng` seeded, so a run is repeatable.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::config::{QUIC_CONNECTION_FIXED_BYTES, QUIC_MEMORY_MEASURED_WITH, QuicSettings};
use super::path::{QuicInbound, QuicPath};
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
use crate::util::alloc_count::live_bytes;

/// How long, in virtual time, a step may take.
const LIMIT: Duration = Duration::from_secs(600);

/// Long enough, in virtual time, for every acknowledgement, retransmission
/// timer and connection-ID exchange after a handshake to finish, and for a
/// closed connection to drain (three PTOs): about 1 s each here.
const SETTLE: Duration = Duration::from_secs(30);

/// The settings of a node with 2 GiB of memory (the shipped `MemoryMax`) and
/// the default 200 connections: a window of 268,435 bytes.
fn settings() -> QuicSettings {
    QuicSettings {
        max_connections: 200,
        memory_limit: 2 * 1024 * 1024 * 1024,
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
    client: isize,
    /// Freed when the server side is then closed and drained.
    server: isize,
    /// Still allocated after both sides closed, against the baseline: what
    /// the endpoints keep (tables grown for `n` connections).
    retained: isize,
}

impl Cost {
    /// The most one node holds per connection: the larger side, plus the
    /// retained growth charged to that side alone.
    fn per_connection(&self) -> usize {
        let side = self.client.max(self.server) + self.retained.max(0);
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
/// entries, tables at their first size) is not counted. The baseline is
/// taken after it; then the `n` connections are opened and settled, the
/// network is cut so neither side hears the other close, the client side is
/// dropped and drained, then the server side.
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
        Arc::new(|_, _, _| PacketDeliveryDecision::Drop);
    let baseline = live_bytes();
    for _ in 0..n {
        let (client, server) =
            drive(&time, LIMIT, connect(a.clone(), b.clone(), b_key.clone())).await;
        clients.push(hold(client, &time));
        servers.push(hold(server, &time));
    }
    settle(&time).await;
    let before_cut = live_bytes();
    set_packet_delivery_callback(network, Some(cut));
    let open = live_bytes();
    drop(clients);
    settle(&time).await;
    let servers_only = live_bytes();
    drop(servers);
    settle(&time).await;
    let closed = live_bytes();
    let cost = Cost {
        n,
        client: open - servers_only,
        server: servers_only - closed,
        retained: closed - baseline,
    };
    tracing::info!(
        ?cost,
        held = before_cut - baseline,
        "idle QUIC connections measured"
    );

    set_packet_delivery_callback(network, None);
    drop(permit);
    for pump in pumps {
        pump.abort();
    }
    unregister_network_time_source(network);
    cost
}

/// An idle QUIC connection, with the `QuicPath` a node keeps on it, holds no
/// more heap on either side than [`QUIC_CONNECTION_FIXED_BYTES`]. Measured
/// with 16 connections between two endpoints, so the endpoints' tables have
/// grown with them and a one-off allocation is spread thin.
#[test]
fn an_idle_connection_holds_at_most_the_declared_fixed_bytes() {
    bounded(|| async move {
        let cost = measure("quic-memory-fixed", 1, 16).await;
        assert!(
            cost.client > 0 && cost.server > 0,
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
struct ProtoPeer {
    addr: SocketAddr,
    endpoint: quinn_proto::Endpoint,
    conn: Option<(quinn_proto::ConnectionHandle, quinn_proto::Connection)>,
}

impl ProtoPeer {
    fn conn(&mut self) -> &mut quinn_proto::Connection {
        &mut self.conn.as_mut().expect("connected").1
    }

    /// Hand the endpoint one datagram from `from`, copied into its own
    /// buffer of exactly its length, as quinn's endpoint driver does
    /// (`quinn/src/endpoint.rs`, `buf[0..meta.len].into()`), so a frame
    /// kept from it keeps that allocation alive. Returns any datagram the
    /// endpoint answers with directly.
    fn receive(&mut self, now: Instant, from: SocketAddr, datagram: &[u8]) -> Vec<Vec<u8>> {
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
    fn pump(&mut self, now: Instant) {
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
    fn transmit(&mut self, now: Instant) -> Vec<Vec<u8>> {
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

/// What a receiver with the node's settings holds for `streams` uni
/// streams, each sent by a peer as a byte at offset 0 that never arrives
/// followed by `frames` one-byte STREAM frames at offsets 1..=frames, one
/// frame per datagram, each datagram padded to 1200 bytes.
#[derive(Debug, Clone)]
struct GapHold {
    streams: usize,
    frames: usize,
    /// Heap the receiver's packet processing holds after the last frame,
    /// net of what it held once connected: the streams' received data.
    held: isize,
    /// Heap the receiver's sending holds after the last frame: the
    /// acknowledgements it sent, which it tracks until they are
    /// acknowledged in turn (never, here). A per-connection term, capped by
    /// quinn-proto at 1000 such packets after its last ack-eliciting one
    /// (`connection/spaces.rs`, `MAX_UNACKED_NON_ACK_ELICTING_TAIL`).
    acks_tracked: isize,
    /// [`Self::held`] after each frame of the FIRST stream, net of what was
    /// held before that stream's first frame.
    first_stream_curve: Vec<isize>,
}

/// Drive two quinn-proto endpoints by hand: a receiver built with the
/// node's own endpoint, transport, server and TLS configuration (window
/// for a 2 GiB node), and a sender whose datagrams the test delivers,
/// reorders or withholds.
///
/// quinn's public API writes a stream in order and retransmits what is
/// lost, so the gap is made by withholding a datagram: the sender writes
/// one byte, its datagram is dropped, then each further byte goes out in a
/// datagram of its own. The sender never receives an acknowledgement, and
/// its RTT estimate (10 s) keeps its loss timer far past the test's virtual
/// span, so it never resends the missing byte; its congestion window (1
/// GiB) and padding to the MTU make every datagram a full 1200 bytes. Only
/// the receiver's heap is counted: every receiver call runs inside
/// [`crate::util::alloc_count::live_delta`].
fn measure_gap_hold(streams: usize, frames: usize) -> GapHold {
    use crate::simulation::RealTime;
    use crate::util::alloc_count::live_delta;
    use quinn_proto::Dir;

    let _seed = GlobalRng::seed_guard(SEED);
    let (a_keys, b_keys) = (TransportKeypair::new(), TransportKeypair::new());
    let a_identity = super::identity::QuicIdentity::new(&a_keys).expect("a's identity");
    let b_identity = super::identity::QuicIdentity::new(&b_keys).expect("b's identity");
    let a_peer = super::identity::PeerKey::of(a_keys.public()).expect("a's key");
    let b_peer = super::identity::PeerKey::of(b_keys.public()).expect("b's key");
    let real = RealTime::new();

    let receiver_transport = Arc::new(super::config::transport_config(&settings(), &real));
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

    let mut sender_transport = super::config::transport_config(&settings(), &real);
    let mut cubic = quinn::congestion::CubicConfig::default();
    cubic.initial_window(1 << 30);
    sender_transport
        .pad_to_mtu(true)
        .initial_rtt(Duration::from_secs(10))
        .max_idle_timeout(None)
        .congestion_controller_factory(Arc::new(cubic));
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
    let start = Instant::now();
    let mut now = start;
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

    let mut held = 0isize;
    let mut acks_tracked = 0isize;
    let mut first_stream_curve = Vec::with_capacity(frames);
    let step = Duration::from_micros(100);
    for stream in 0..streams {
        let id = sender
            .conn()
            .streams()
            .open(Dir::Uni)
            .expect("the receiver allows another uni stream");
        // Offset 0: sent, and withheld.
        sender.conn().send_stream(id).write(&[0]).expect("write");
        let withheld = sender.transmit(now);
        assert_eq!(withheld.len(), 1, "offset 0 went out in one datagram");
        let mut stream_held = 0isize;
        for offset in 1..=frames {
            now += step;
            sender
                .conn()
                .send_stream(id)
                .write(&[offset as u8])
                .expect("write");
            let datagrams = sender.transmit(now);
            assert_eq!(datagrams.len(), 1, "one frame per datagram");
            assert_eq!(
                datagrams[0].len(),
                usize::from(super::config::MAX_UDP_PAYLOAD),
                "padded to the MTU"
            );
            let (_, delta) = live_delta(|| receiver.receive(now, sender.addr, &datagrams[0]));
            // The receiver's acknowledgements are dropped.
            let (_, acks) = live_delta(|| drop(receiver.transmit(now)));
            held += delta;
            acks_tracked += acks;
            stream_held += delta;
            if stream == 0 {
                first_stream_curve.push(stream_held);
            }
        }
    }
    assert!(
        now - start < Duration::from_secs(5),
        "the sender's loss timer cannot have fired"
    );
    assert!(
        !receiver.conn().is_closed(),
        "the receiver kept the connection open"
    );
    assert_eq!(
        sender.conn().stats().path.lost_packets,
        0,
        "the sender resent nothing"
    );
    GapHold {
        streams,
        frames,
        held,
        acks_tracked,
        first_stream_curve,
    }
}

/// The most a receiver holds for one uni stream whose offset 0 has not
/// arrived. quinn-proto 0.11.19's assembler (`connection/assembler.rs`,
/// `Assembler::insert`) copies a stream's chunks out only once their
/// estimated over-allocation passes `32768.max(buffered * 3 / 2)`, and it
/// estimates each frame's allocation as its packet's payload (about 1150 B
/// of a 1200 B datagram, after header and AEAD tag). The real allocation is
/// the whole datagram plus a `Bytes` header and a heap entry, about 1240 B,
/// so about 28 one-byte frames (34.7 KB) are held before it compacts, plus
/// the stream's own state. Derived from the source; the measurement it was
/// checked against (36,794 B peak, at 28 frames) is in the test below.
const ASSEMBLER_HOLD_PER_STREAM_BOUND: isize = 40 * 1024;

/// A peer that sends one-byte frames above a gap at offset 0, one per
/// padded datagram, makes the receiver hold about one datagram per frame
/// until the assembler's estimated over-allocation passes 32 KiB and it
/// copies the bytes out. Per stream that stays under
/// [`ASSEMBLER_HOLD_PER_STREAM_BOUND`] whatever the number of frames, and
/// comes within a few KiB of it at the peak, so the bound is not loose.
///
/// Measured with quinn-proto 0.11.19 at a 2 GiB node's window: one stream
/// rises about 1240 B per frame to 36,794 B at 28 frames, falls to about
/// 1.9 KB when the 29th frame triggers compaction, and peaks at the same
/// value in every later cycle; 256 streams of 28 frames held 9,384,032 B
/// (36,656 B per stream). The acknowledgements the receiver sends, which
/// such a peer never acknowledges, are counted apart (`acks_tracked`):
/// about 191 KB per connection once quinn-proto's 1000-packet cap is
/// reached.
#[test]
fn a_gapped_stream_holds_at_most_the_assembler_bound() {
    let single = measure_gap_hold(1, 64);
    let peak = *single.first_stream_curve.iter().max().expect("frames");
    assert!(
        peak <= ASSEMBLER_HOLD_PER_STREAM_BOUND,
        "one stream peaked at {peak} B: {single:?}"
    );
    assert!(
        peak >= 28 * 1024,
        "premise: the datagrams were held until compaction: peak {peak} B, {single:?}"
    );
    let peak_frames = 1 + single
        .first_stream_curve
        .iter()
        .position(|held| *held == peak)
        .expect("peak");
    let many = measure_gap_hold(16, peak_frames);
    let per_stream = many.held / many.streams as isize;
    assert!(
        per_stream <= ASSEMBLER_HOLD_PER_STREAM_BOUND,
        "{per_stream} B per stream with {} streams of {} frames",
        many.streams,
        many.frames
    );
    assert!(
        per_stream >= 28 * 1024,
        "premise: every stream held its datagrams: {per_stream} B per stream"
    );

    // The unacknowledged acknowledgements stop growing at quinn-proto's
    // 1000-packet cap: 3000 and 6000 frames leave the same tail.
    let (half, full) = (measure_gap_hold(1, 3000), measure_gap_hold(1, 6000));
    assert!(
        full.acks_tracked <= 256 * 1024 && full.acks_tracked - half.acks_tracked < 8 * 1024,
        "the acknowledgement tail grew past the cap: {} B after 3000 frames, {} B after 6000",
        half.acks_tracked,
        full.acks_tracked
    );
}

/// [`QUIC_CONNECTION_FIXED_BYTES`] and the assembler bound were measured
/// with the quinn and quinn-proto versions in [`QUIC_MEMORY_MEASURED_WITH`].
/// A bump of either fails here until someone re-runs the measurements
/// above, updates the figures in the constants' docs, and moves the pin.
#[test]
fn quic_memory_figures_match_the_locked_quinn_versions() {
    const LOCK: &str = include_str!("../../../../../Cargo.lock");
    for (name, measured) in QUIC_MEMORY_MEASURED_WITH {
        let header = format!("[[package]]\nname = \"{name}\"\nversion = \"");
        let locked: Vec<&str> = LOCK
            .match_indices(&header)
            .map(|(at, _)| {
                let rest = &LOCK[at + header.len()..];
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
