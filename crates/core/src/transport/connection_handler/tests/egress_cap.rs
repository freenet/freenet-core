use super::super::mock_transport::{Channels, MockSocket, PacketDelayPolicy, PacketDropPolicy};
use super::super::*;
use crate::simulation::VirtualTime;
use crate::transport::demux::DemuxSocket;
use crate::transport::egress_cap::{DEBT_LIMIT, EgressCap, EgressStats};
use crate::transport::metrics::EgressDecision;
use dashmap::DashMap;

/// 10 KB/s, so a bucket at the debt limit owes 10 000 bytes and any send
/// that waited for it would wait about a second.
const RATE: u64 = 10_000;

/// A mock socket that, once armed, silently drops its next `n` sends
/// (reporting them sent, as a lossy link would).
struct Lossy {
    inner: MockSocket,
    drops_left: std::sync::atomic::AtomicUsize,
}

impl Lossy {
    fn take_drop(&self) -> bool {
        self.drops_left
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |n| n.checked_sub(1),
            )
            .is_ok()
    }
}

impl Socket for Lossy {
    async fn bind(_addr: SocketAddr) -> std::io::Result<Self> {
        unimplemented!()
    }
    async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf).await
    }
    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        if self.take_drop() {
            return Ok(buf.len());
        }
        self.inner.send_to(buf, target).await
    }
    fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        if self.take_drop() {
            return Ok(buf.len());
        }
        self.inner.send_to_blocking(buf, target)
    }
}

type CappedSocket = DemuxSocket<Lossy>;

/// A peer whose egress cap runs on its own `VirtualTime` that the test never
/// advances: the bucket is drained to the debt limit once and stays there,
/// whatever the tokio clock does, so every send meets a bucket a full second
/// in debt.
async fn capped_peer(
    port: u16,
    channels: &Channels,
    is_gateway: bool,
) -> (
    TransportPublicKey,
    OutboundConnectionHandler<CappedSocket>,
    mpsc::Receiver<PeerConnection<CappedSocket>>,
    Arc<CappedSocket>,
    SocketAddr,
) {
    let addr: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, port).into();
    let inner = Lossy {
        inner: MockSocket::new(
            PacketDropPolicy::ReceiveAll,
            PacketDelayPolicy::NoDelay,
            addr,
            channels.clone(),
        )
        .await,
        drops_left: std::sync::atomic::AtomicUsize::new(0),
    };
    let cap = EgressCap::new(RATE, VirtualTime::new());
    cap.charge_unpaced(10 * RATE as usize);
    assert_eq!(cap.level_bytes(), -(RATE as i64), "premise: full debt");
    let socket = Arc::new(DemuxSocket::wrap(inner, Some(cap)));
    let keypair = TransportKeypair::new();
    let public = keypair.public().clone();
    let (handler, inbound) =
        OutboundConnectionHandler::new_test(addr, socket.clone(), keypair, is_gateway)
            .expect("listener");
    (public, handler, inbound, socket, addr)
}

fn stats(socket: &CappedSocket) -> EgressStats {
    Socket::egress_cap(socket).expect("cap").stats()
}

/// Every datagram since `before` was charged as unpaced and none went
/// through a pacing point.
fn assert_only_unpaced(side: &str, socket: &CappedSocket, before: &EgressStats) {
    let now = stats(socket);
    let unpaced = EgressDecision::Unpaced as usize;
    assert!(
        now.packets[unpaced] > before.packets[unpaced],
        "{side}: its datagrams are charged to the cap"
    );
    for decision in [EgressDecision::Immediate, EgressDecision::Waited] {
        assert_eq!(
            now.packets[decision as usize], before.packets[decision as usize],
            "{side}: no datagram went through a pacing point ({decision:?})"
        );
    }
    assert!(
        Socket::egress_cap(socket).unwrap().level_bytes() < 0,
        "{side}: premise: still in debt"
    );
}

/// A connected joiner/gateway pair. `_keep` holds both listeners' handles:
/// a listener stops, closing its connections, once they are dropped.
struct Pair {
    joiner: PeerConnection<CappedSocket>,
    gateway: PeerConnection<CappedSocket>,
    joiner_socket: Arc<CappedSocket>,
    gateway_socket: Arc<CappedSocket>,
    handshake_elapsed: Duration,
    _keep: Box<dyn std::any::Any + Send>,
}

async fn connect_pair(channels: &Channels, ports: (u16, u16)) -> Pair {
    let (gw_pub, gw_out, mut gw_inbound, gw_socket, gw_addr) =
        capped_peer(ports.0, channels, true).await;
    let (_, mut joiner, joiner_inbound, joiner_socket, _) =
        capped_peer(ports.1, channels, false).await;
    let start = tokio::time::Instant::now();
    let connecting = joiner.connect(gw_pub, gw_addr).await;
    let (joiner_conn, gw_conn) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(connecting, gw_inbound.recv())
    })
    .await
    .expect("the handshake completes");
    let handshake_elapsed = start.elapsed();
    Pair {
        joiner: joiner_conn.expect("joiner connected"),
        gateway: gw_conn.expect("gateway accepted"),
        joiner_socket,
        gateway_socket: gw_socket,
        handshake_elapsed,
        _keep: Box::new((gw_out, gw_inbound, joiner, joiner_inbound)),
    }
}

/// With both nodes' egress buckets a full second in debt, a joiner still
/// completes its handshake with a gateway at once: every handshake datagram
/// is charged as unpaced, and none went through a pacing point. This covers
/// the handshake path only; the connection's own control traffic is
/// `control_traffic_never_waits_for_the_egress_cap`.
///
/// The test runs on tokio's paused clock, which jumps forward whenever every
/// task waits on a timer, so a handshake send that waited for the bucket would
/// show up as about a second of elapsed time per send; the decision counters
/// show which path each datagram took. A short message sent afterwards is
/// not paced either: it is sent with no elapsed time and counted unpaced.
#[tokio::test(start_paused = true)]
async fn handshake_completes_promptly_with_the_egress_bucket_in_full_debt() {
    let channels: Channels = Arc::new(DashMap::new());
    let before = EgressStats::default();
    let Pair {
        joiner: mut joiner_conn,
        gateway: mut gw_conn,
        joiner_socket,
        gateway_socket: gw_socket,
        handshake_elapsed,
        _keep,
    } = connect_pair(&channels, (47_621, 47_622)).await;
    assert!(
        handshake_elapsed < DEBT_LIMIT / 4,
        "the handshake took {handshake_elapsed:?} with the bucket in debt; a send \
         on the handshake path waited for the egress cap"
    );
    assert_only_unpaced("gateway", &gw_socket, &before);
    assert_only_unpaced("joiner", &joiner_socket, &before);

    let before_send = stats(&joiner_socket);
    let start = tokio::time::Instant::now();
    joiner_conn
        .send("through a full bucket")
        .await
        .expect("sent");
    assert_eq!(
        start.elapsed(),
        Duration::ZERO,
        "a short message must not wait for the egress cap"
    );
    let received = tokio::time::timeout(Duration::from_secs(10), gw_conn.recv())
        .await
        .expect("delivered")
        .expect("received");
    assert_eq!(&received[8..], b"through a full bucket");
    assert_only_unpaced("joiner short message", &joiner_socket, &before_send);
}

/// The connection's control traffic never waits for the egress cap, with
/// both buckets a full second in debt: receipts (standalone ACK NoOps),
/// keepalive Pings and the Pongs that answer them, and retransmissions are
/// all charged as unpaced, and none goes through a pacing point.
///
/// The test first checks that each kind of datagram really was sent, from
/// the connections' own counters, so a run in which, say, no retransmission
/// happened cannot pass by saying nothing about it. Once connected, the
/// joiner drops 30 of its sends (the handshake is not affected), so some of
/// its short messages are lost and retransmitted.
#[tokio::test(start_paused = true)]
async fn control_traffic_never_waits_for_the_egress_cap() {
    let channels: Channels = Arc::new(DashMap::new());
    let Pair {
        joiner: mut joiner_conn,
        gateway: mut gw_conn,
        joiner_socket,
        gateway_socket: gw_socket,
        _keep,
        ..
    } = connect_pair(&channels, (47_623, 47_624)).await;
    joiner_socket
        .inner_for_test()
        .drops_left
        .store(30, std::sync::atomic::Ordering::Release);
    let before_joiner = stats(&joiner_socket);
    let before_gw = stats(&gw_socket);

    const MESSAGES: usize = 60;
    for i in 0..MESSAGES {
        joiner_conn
            .send(format!("message {i}"))
            .await
            .expect("sent");
    }
    // Run both receive loops for 12 s of (paused) time: long enough for two
    // keepalive Pings and for the lost messages to be retransmitted.
    let gateway = tokio::spawn(async move {
        let mut received = 0usize;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
        while let Ok(Ok(_)) = tokio::time::timeout_at(deadline, gw_conn.recv()).await {
            received += 1;
        }
        (gw_conn, received)
    });
    let joiner = tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
        while let Ok(Ok(_)) = tokio::time::timeout_at(deadline, joiner_conn.recv()).await {}
        joiner_conn
    });
    let (gw_conn, received) = gateway.await.unwrap();
    let joiner_conn = joiner.await.unwrap();

    assert_eq!(
        received, MESSAGES,
        "every message arrived, lost ones resent"
    );
    let (retransmits, joiner_pongs, _) = joiner_conn.control_traffic_counts();
    let (_, gw_pongs, gw_acks) = gw_conn.control_traffic_counts();
    assert!(retransmits > 0, "premise: the joiner retransmitted");
    assert!(
        joiner_pongs > 0 && gw_pongs > 0,
        "premise: both sides sent keepalive Pings and answered the other's \
         with Pongs (joiner {joiner_pongs}, gateway {gw_pongs})"
    );
    assert!(gw_acks > 0, "premise: the gateway sent standalone ACKs");

    assert_only_unpaced("joiner", &joiner_socket, &before_joiner);
    assert_only_unpaced("gateway", &gw_socket, &before_gw);
}
