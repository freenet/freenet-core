use super::super::mock_transport::{MockSocket, PacketDelayPolicy, PacketDropPolicy};
use super::super::*;
use crate::transport::demux::DemuxSocket;
use crate::transport::egress_cap::{DEBT_LIMIT, EgressCap, SHORT_MESSAGE_MAX_WAIT};
use crate::transport::metrics::EgressDecision;
use dashmap::DashMap;

/// 10 KB/s, so a bucket at the debt limit owes 10 000 bytes and any send
/// that waited for it would wait about a second.
const RATE: u64 = 10_000;

async fn capped_peer(
    port: u16,
    channels: &super::super::mock_transport::Channels,
    is_gateway: bool,
) -> (
    TransportPublicKey,
    OutboundConnectionHandler<DemuxSocket<MockSocket>>,
    mpsc::Receiver<PeerConnection<DemuxSocket<MockSocket>>>,
    Arc<DemuxSocket<MockSocket>>,
    SocketAddr,
) {
    let addr: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, port).into();
    let inner = MockSocket::new(
        PacketDropPolicy::ReceiveAll,
        PacketDelayPolicy::NoDelay,
        addr,
        channels.clone(),
    )
    .await;
    let cap = EgressCap::new(RATE, RealTime::new());
    // Drain the bucket to the debt limit: far more than it can hold.
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

fn stats(socket: &DemuxSocket<MockSocket>) -> crate::transport::egress_cap::EgressStats {
    Socket::egress_cap(socket).expect("cap").stats()
}

/// With both nodes' egress buckets a full second in debt, a joiner still
/// completes its handshake with a gateway at once, every handshake datagram
/// is charged as unpaced (so the cap still sees it), and nothing on the
/// handshake or receive path waited for the bucket.
///
/// The clock is tokio's paused clock, which jumps forward whenever every task
/// is waiting on a timer. A handshake send that waited for the bucket would
/// therefore show up as about a second of elapsed time per send, so the
/// elapsed-time assertion is decisive. The decision counters say which path
/// each datagram took.
///
/// Then a short message from the joiner, which may wait, waits at most
/// `SHORT_MESSAGE_MAX_WAIT` and is delivered.
#[tokio::test(start_paused = true)]
async fn handshake_completes_promptly_with_the_egress_bucket_in_full_debt() {
    let channels: super::super::mock_transport::Channels = Arc::new(DashMap::new());
    let (gw_pub, _gw_out, mut gw_inbound, gw_socket, gw_addr) =
        capped_peer(47_621, &channels, true).await;
    let (_joiner_pub, mut joiner, _joiner_inbound, joiner_socket, _joiner_addr) =
        capped_peer(47_622, &channels, false).await;
    let before_gw = stats(&gw_socket);
    let before_joiner = stats(&joiner_socket);

    let start = tokio::time::Instant::now();
    let connecting = joiner.connect(gw_pub, gw_addr).await;
    let (joiner_conn, gw_conn) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(connecting, gw_inbound.recv())
    })
    .await
    .expect("the handshake completes");
    let handshake_elapsed = start.elapsed();
    let mut joiner_conn = joiner_conn.expect("joiner connected");
    let mut gw_conn = gw_conn.expect("gateway accepted");

    assert!(
        handshake_elapsed < DEBT_LIMIT / 4,
        "the handshake took {handshake_elapsed:?} with the bucket in debt; a send \
         on the handshake path waited for the egress cap"
    );
    for (side, socket, before) in [
        ("gateway", &gw_socket, &before_gw),
        ("joiner", &joiner_socket, &before_joiner),
    ] {
        let now = stats(socket);
        let unpaced = EgressDecision::Unpaced as usize;
        assert!(
            now.packets[unpaced] > before.packets[unpaced],
            "{side}: handshake datagrams are charged to the cap"
        );
        for decision in [
            EgressDecision::Immediate,
            EgressDecision::Waited,
            EgressDecision::WaitCapped,
        ] {
            assert_eq!(
                now.packets[decision as usize], before.packets[decision as usize],
                "{side}: no handshake datagram went through a pacing point"
            );
        }
        assert!(
            Socket::egress_cap(socket.as_ref()).unwrap().level_bytes() < 0,
            "{side}: premise: still in debt"
        );
    }

    // A short message is paced, but only briefly: the bucket is still about
    // a second behind, so it waits the short-message limit and goes into
    // debt.
    let start = tokio::time::Instant::now();
    joiner_conn
        .send("through a full bucket")
        .await
        .expect("sent");
    let send_elapsed = start.elapsed();
    let received = tokio::time::timeout(Duration::from_secs(10), gw_conn.recv())
        .await
        .expect("delivered")
        .expect("received");
    assert_eq!(&received[8..], b"through a full bucket");
    assert!(
        send_elapsed >= SHORT_MESSAGE_MAX_WAIT && send_elapsed < DEBT_LIMIT / 4,
        "the short message waited {send_elapsed:?}"
    );
    let joiner_now = stats(&joiner_socket);
    assert_eq!(
        joiner_now.packets[EgressDecision::WaitCapped as usize],
        1,
        "the short message waited the capped time"
    );
}
