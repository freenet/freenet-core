//! Tests for the receipt policy, ack timing, keepalive guarantees and the
//! cancellation-safe `recv()` timers (#5795).
//!
//! All tests run on a paused tokio clock with `RealTime`, so the keepalive
//! task is live (as in production) and timing is deterministic.

use std::net::Ipv4Addr;

use aes_gcm::KeyInit;
use bytes::Bytes;

use super::*;
use crate::transport::crypto::TransportKeypair;
use crate::transport::packet_data::{PacketData, UnknownEncryption};

type WireRx = mpsc::Receiver<(SocketAddr, Arc<[u8]>)>;

/// Socket that hands every sent datagram to a channel.
struct ChanSocket {
    tx: mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
    /// While set, every send fails (as on a network switch).
    fail: Arc<std::sync::atomic::AtomicBool>,
}

impl crate::transport::Socket for ChanSocket {
    async fn bind(_addr: SocketAddr) -> std::io::Result<Self> {
        unimplemented!()
    }

    async fn recv_from(&self, _buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        unimplemented!()
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(std::io::ErrorKind::NetworkUnreachable.into());
        }
        self.tx
            .send((target, buf.into()))
            .await
            .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
        Ok(buf.len())
    }

    fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        self.tx
            .blocking_send((target, buf.into()))
            .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
        Ok(buf.len())
    }
}

fn random_key() -> ([u8; 16], Aes128Gcm) {
    let mut key = [0u8; 16];
    crate::config::GlobalRng::fill_bytes(&mut key);
    (key, Aes128Gcm::new(&key.into()))
}

/// One end of a connection, with its wire exposed.
struct End {
    conn: PeerConnection<ChanSocket, RealTime>,
    /// Feed packets INTO this end (as if from the remote).
    inbound: mpsc::Sender<PacketData<UnknownEncryption>>,
    /// Datagrams this end SENT.
    wire: WireRx,
    /// Key this end encrypts with; decrypts what it sent.
    outbound_key: Aes128Gcm,
    /// Key the remote encrypts with; used to forge inbound packets.
    inbound_key: Aes128Gcm,
    /// Make this end's socket fail every send while set.
    fail_sends: Arc<std::sync::atomic::AtomicBool>,
}

fn end(
    addr: SocketAddr,
    remote_addr: SocketAddr,
    outbound: ([u8; 16], Aes128Gcm),
    inbound: ([u8; 16], Aes128Gcm),
    remote_version: Option<(u8, u8, u16)>,
) -> End {
    end_with_wire_capacity(addr, remote_addr, outbound, inbound, remote_version, 4096)
}

fn end_with_wire_capacity(
    addr: SocketAddr,
    remote_addr: SocketAddr,
    outbound: ([u8; 16], Aes128Gcm),
    inbound: ([u8; 16], Aes128Gcm),
    remote_version: Option<(u8, u8, u16)>,
    wire_capacity: usize,
) -> End {
    let _ = addr;
    let time_source = RealTime::new();
    let (inbound_tx, inbound_rx) = mpsc::channel(4096);
    let (wire_tx, wire_rx) = mpsc::channel(wire_capacity);
    let fail_sends = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let remote_conn = RemoteConnection {
        outbound_symmetric_key: outbound.1.clone(),
        remote_addr,
        sent_tracker: Arc::new(parking_lot::Mutex::new(
            SentPacketTracker::new_with_time_source(time_source.clone()),
        )),
        last_packet_id: Arc::new(AtomicU32::new(0)),
        inbound_packet_recv: inbound_rx,
        inbound_symmetric_key: inbound.1.clone(),
        inbound_symmetric_key_bytes: inbound.0,
        my_address: None,
        remote_protoc_version: remote_version,
        transport_secret_key: TransportKeypair::new().secret,
        congestion_controller:
            crate::transport::congestion_control::CongestionControlConfig::default()
                .build_arc_with_time_source(time_source.clone()),
        token_bucket: Arc::new(TokenBucket::new_with_time_source(
            1_000_000,
            10_000_000,
            time_source.clone(),
        )),
        socket: Arc::new(ChanSocket {
            tx: wire_tx,
            fail: fail_sends.clone(),
        }),
        global_bandwidth: None,
        rolling_rtt_stats: crate::transport::rolling_rtt_stats::RollingRttStatsHandle::new(
            remote_addr,
            time_source.clone(),
        ),
        time_source,
    };
    End {
        conn: PeerConnection::new(remote_conn),
        inbound: inbound_tx,
        wire: wire_rx,
        outbound_key: outbound.1,
        inbound_key: inbound.1,
        fail_sends,
    }
}

fn addr(port: u16) -> SocketAddr {
    SocketAddr::new(Ipv4Addr::new(10, 77, 0, 1).into(), port)
}

/// A single connection end facing a remote of the given version.
fn single(remote_version: Option<(u8, u8, u16)>) -> End {
    end(addr(1), addr(2), random_key(), random_key(), remote_version)
}

const CAPABLE: Option<(u8, u8, u16)> = Some(UNTRACKED_ACK_NOOP_MIN_VERSION);
const OLD: Option<(u8, u8, u16)> = Some((0, 2, 141));

impl End {
    /// Forge a packet from the remote and deliver it to this end.
    async fn deliver(&self, packet_id: u32, payload: SymmetricMessagePayload, receipts: Vec<u32>) {
        let packet = SymmetricMessage::serialize_msg_to_packet_data(
            packet_id,
            payload,
            &self.inbound_key,
            receipts,
        )
        .expect("encrypt");
        self.inbound
            .send(PacketData::<UnknownEncryption>::from_buf(packet.data()))
            .await
            .expect("deliver");
    }

    /// Everything this end has sent so far, decrypted.
    fn drain_sent(&mut self) -> Vec<SymmetricMessage> {
        let mut out = Vec::new();
        while let Ok((_, bytes)) = self.wire.try_recv() {
            let decrypted = PacketData::<UnknownEncryption>::from_buf(&*bytes)
                .try_decrypt_sym(&self.outbound_key)
                .expect("decrypt own packet");
            out.push(SymmetricMessage::deser(decrypted.data()).expect("deser"));
        }
        out
    }

    /// Run `recv()` for `dur` of (paused) time; return any message it yields.
    async fn pump(&mut self, dur: Duration) -> Option<Vec<u8>> {
        match tokio::time::timeout(dur, self.conn.recv()).await {
            Ok(msg) => Some(msg.expect("recv must not error")),
            Err(_) => None,
        }
    }
}

fn short(bytes: &'static [u8]) -> SymmetricMessagePayload {
    SymmetricMessagePayload::ShortMessage {
        payload: Bytes::from_static(bytes),
    }
}

fn receipts_for(sent: &[SymmetricMessage], id: u32) -> usize {
    sent.iter()
        .filter(|m| m.confirm_receipt.contains(&id))
        .count()
}

fn noops(sent: &[SymmetricMessage]) -> Vec<&SymmetricMessage> {
    sent.iter()
        .filter(|m| matches!(m.payload, SymmetricMessagePayload::NoOp))
        .collect()
}

// ---------------------------------------------------------------------------
// Receipt policy (pure)
// ---------------------------------------------------------------------------

#[test]
fn receipt_policy_table() {
    use ReceiptPolicy::*;
    let ping = SymmetricMessagePayload::Ping { sequence: 1 };
    let pong = SymmetricMessagePayload::Pong { sequence: 1 };
    let noop = SymmetricMessagePayload::NoOp;
    for ack_noops in [true, false] {
        assert_eq!(
            receipt_policy(&ping, ack_noops),
            Skip,
            "Ping is never tracked"
        );
        assert_eq!(
            receipt_policy(&pong, ack_noops),
            Skip,
            "Pong is never tracked"
        );
        assert_eq!(
            receipt_policy(&short(b"x"), ack_noops),
            Ack {
                reack_duplicate: true
            }
        );
    }
    assert_eq!(
        receipt_policy(&noop, true),
        Ack {
            reack_duplicate: true
        },
        "an old peer tracks its NoOps, so they must be acked (and re-acked)"
    );
    assert_eq!(
        receipt_policy(&noop, false),
        Skip,
        "a capable peer's NoOps are fire-and-forget"
    );
    let ack = SymmetricMessagePayload::AckConnection {
        result: Ok(symmetric_message::OutboundConnection {
            key: [0u8; 16],
            remote_addr: addr(2),
        }),
    };
    let ack_v2 = SymmetricMessagePayload::AckConnectionV2 {
        connection: symmetric_message::OutboundConnectionV2 {
            key: [0u8; 16],
            remote_addr: addr(2),
            protoc_version: [0u8; 8],
        },
    };
    for ack_noops in [true, false] {
        assert_eq!(
            receipt_policy(&ack, ack_noops),
            AckWithoutDedup,
            "the handshake ack is tracked at id 0: ack it, never dedup id 0"
        );
        assert_eq!(receipt_policy(&ack_v2, ack_noops), AckWithoutDedup);
    }
}

/// The gate fails closed on an unknown version and is inclusive at the floor.
#[test]
fn remote_tracks_ack_noops_is_fail_closed_and_inclusive_at_floor() {
    let floor = UNTRACKED_ACK_NOOP_MIN_VERSION;
    assert!(remote_tracks_ack_noops(None, floor), "unknown = old");
    assert!(remote_tracks_ack_noops(Some((0, 2, 141)), floor));
    assert!(remote_tracks_ack_noops(Some((0, 1, 65535)), floor));
    assert!(!remote_tracks_ack_noops(Some(floor), floor));
    assert!(!remote_tracks_ack_noops(Some((0, 2, 143)), floor));
    assert!(!remote_tracks_ack_noops(Some((0, 3, 0)), floor));
    assert!(!remote_tracks_ack_noops(Some((1, 0, 0)), floor));
}

/// Mirrors `ack_version_floor_tracks_the_shipping_release`: the moment a
/// release bump lifts the crate to the floor, this fails until someone
/// consciously records that the release carries the change (or raises the
/// floor).
#[test]
fn untracked_noop_floor_tracks_the_shipping_release() {
    let mut parts = env!("CARGO_PKG_VERSION")
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<u16>().expect("numeric version component"));
    let crate_version = (
        parts.next().expect("major") as u8,
        parts.next().expect("minor") as u8,
        parts.next().expect("patch"),
    );
    match UNTRACKED_ACK_NOOP_SHIPPED_IN {
        Some(shipped) => {
            assert_eq!(
                shipped, UNTRACKED_ACK_NOOP_MIN_VERSION,
                "the release that ships untracked ack-only NoOps must EQUAL the floor"
            );
            // Plausibility: the marker may name at most the NEXT patch release
            // (set just before the bump PR), never a release further out.
            let next = (crate_version.0, crate_version.1, crate_version.2 + 1);
            assert!(
                shipped <= next,
                "UNTRACKED_ACK_NOOP_SHIPPED_IN {shipped:?} is beyond the next release \
                 {next:?} — the marker records the release that carries the code"
            );
        }
        None => assert!(
            crate_version < UNTRACKED_ACK_NOOP_MIN_VERSION,
            "floor {UNTRACKED_ACK_NOOP_MIN_VERSION:?} is a PREDICTION about the next release, \
             but the crate is already at {crate_version:?}: either set \
             UNTRACKED_ACK_NOOP_SHIPPED_IN = Some(floor) (this release carries #5795) or \
             raise the floor"
        ),
    }
}

/// Companion: the floor must never drop to a release that tracks its NoOps.
#[test]
fn untracked_noop_floor_stays_above_every_release_that_tracks_noops() {
    const LAST_RELEASE_TRACKING_NOOPS: (u8, u8, u16) = (0, 2, 141);
    assert!(
        UNTRACKED_ACK_NOOP_MIN_VERSION > LAST_RELEASE_TRACKING_NOOPS,
        "dropping the floor to a release that tracks its ack-only NoOps makes us ignore \
         NoOps that peer will retransmit up to MAX_PACKET_RETRANSMITS times"
    );
}

// ---------------------------------------------------------------------------
// Receiver behaviour
// ---------------------------------------------------------------------------

/// A receipt-only NoOp from a capable peer is not acked at all — this is the
/// change that ends the ack-of-ack ping-pong.
#[tokio::test(start_paused = true)]
async fn noop_from_capable_peer_is_never_acked() {
    let mut e = single(CAPABLE);
    e.deliver(7, SymmetricMessagePayload::NoOp, vec![]).await;
    assert!(e.pump(Duration::from_secs(2)).await.is_none());
    let sent = e.drain_sent();
    assert_eq!(
        receipts_for(&sent, 7),
        0,
        "capable peer's NoOp must not be acked: {sent:?}"
    );
    assert!(noops(&sent).is_empty(), "no NoOp should be sent at all");
}

/// From an old (or unknown-version) peer, which tracks its NoOps, the NoOp is
/// acked exactly as before: within the 100 ms ack interval, far inside the
/// sender's MIN_RTO (500 ms), so it never retransmits.
#[tokio::test(start_paused = true)]
async fn noop_from_old_peer_is_acked_within_ack_interval() {
    for version in [OLD, None] {
        let mut e = single(version);
        e.deliver(7, SymmetricMessagePayload::NoOp, vec![]).await;
        assert!(
            e.pump(ACK_CHECK_INTERVAL + Duration::from_millis(10))
                .await
                .is_none()
        );
        assert_eq!(
            receipts_for(&e.drain_sent(), 7),
            1,
            "{version:?}: old peer's NoOp must be acked within the ack interval"
        );
    }
}

/// Ping and Pong are sent untracked by every release, so they never earn a
/// receipt — for any peer version. The Pong reply is still sent.
#[tokio::test(start_paused = true)]
async fn ping_and_pong_are_never_acked() {
    for version in [CAPABLE, OLD, None] {
        let mut e = single(version);
        e.deliver(3, SymmetricMessagePayload::Ping { sequence: 9 }, vec![])
            .await;
        e.deliver(4, SymmetricMessagePayload::Pong { sequence: 0 }, vec![])
            .await;
        assert!(e.pump(Duration::from_secs(2)).await.is_none());
        let sent = e.drain_sent();
        assert!(
            sent.iter()
                .any(|m| matches!(m.payload, SymmetricMessagePayload::Pong { sequence: 9 })),
            "{version:?}: Ping must still be answered with a Pong"
        );
        assert_eq!(receipts_for(&sent, 3), 0, "{version:?}: Ping acked");
        assert_eq!(receipts_for(&sent, 4), 0, "{version:?}: Pong acked");
    }
}

/// Data acks stay exactly as prompt as before: within the 100 ms ack timer.
/// The ack itself is fire-and-forget: not registered for retransmission.
#[tokio::test(start_paused = true)]
async fn data_packet_is_acked_within_ack_interval_by_untracked_noop() {
    for version in [CAPABLE, OLD] {
        let mut e = single(version);
        e.deliver(5, short(b"hello"), vec![]).await;
        let got = e.pump(Duration::from_secs(1)).await;
        assert_eq!(got.as_deref(), Some(&b"hello"[..]));
        assert!(
            e.pump(ACK_CHECK_INTERVAL + Duration::from_millis(10))
                .await
                .is_none()
        );
        let ack = e
            .drain_sent()
            .into_iter()
            .find(|m| m.confirm_receipt.contains(&5))
            .unwrap_or_else(|| panic!("{version:?}: data packet not acked within ack interval"));
        assert!(matches!(ack.payload, SymmetricMessagePayload::NoOp));
        assert!(
            !e.conn
                .remote_conn
                .sent_tracker
                .lock()
                .contains_packet(ack.packet_id),
            "{version:?}: ack-only NoOp must not be tracked for retransmission"
        );
    }
}

/// A retransmitted data packet (our receipt was lost) is re-acked and NOT
/// delivered to the application a second time. Covers the case where the
/// duplicate arrives after the 600 ms receipt deadline lapsed, which before
/// #5795 fell through to `process_inbound` and was delivered twice.
#[tokio::test(start_paused = true)]
async fn duplicate_data_packet_is_reacked_and_not_redelivered() {
    let mut e = single(CAPABLE);
    e.deliver(5, short(b"once"), vec![]).await;
    assert_eq!(
        e.pump(Duration::from_secs(1)).await.as_deref(),
        Some(&b"once"[..])
    );
    assert!(e.pump(Duration::from_millis(300)).await.is_none());
    assert_eq!(receipts_for(&e.drain_sent(), 5), 1, "first ack");

    // The sender's RTO fires (>= 500 ms) and it retransmits; by now more than
    // MESSAGE_CONFIRMATION_TIMEOUT has passed since the last report.
    tokio::time::advance(Duration::from_millis(700)).await;
    e.deliver(5, short(b"once"), vec![]).await;
    e.deliver(6, short(b"next"), vec![]).await;
    assert_eq!(
        e.pump(Duration::from_secs(1)).await.as_deref(),
        Some(&b"next"[..]),
        "the duplicate must not be delivered again"
    );
    assert!(e.pump(Duration::from_millis(300)).await.is_none());
    assert_eq!(
        receipts_for(&e.drain_sent(), 5),
        1,
        "the duplicate must be re-acked so the sender stops retransmitting"
    );
}

/// The receipt queue still forces an immediate flush at MAX_PENDING_RECEIPTS,
/// and a NoOp too large for one packet is split rather than dropped.
#[tokio::test(start_paused = true)]
async fn noop_splits_receipts_that_do_not_fit_one_packet() {
    let mut e = single(CAPABLE);
    let max = SymmetricMessage::max_num_of_confirm_receipts_of_noop_message();
    let receipts: Vec<u32> = (0..(max as u32 + 5)).collect();
    e.conn.noop(receipts.clone()).await.expect("noop");
    let sent = e.drain_sent();
    assert_eq!(sent.len(), 2, "one full packet plus the remainder");
    let mut carried: Vec<u32> = sent
        .iter()
        .flat_map(|m| m.confirm_receipt.clone())
        .collect();
    carried.sort_unstable();
    assert_eq!(carried, receipts, "every receipt carried exactly once");
    assert_ne!(sent[0].packet_id, sent[1].packet_id);
}

// ---------------------------------------------------------------------------
// Two real ends wired together
// ---------------------------------------------------------------------------

struct Link {
    a_to_b: Vec<(tokio::time::Instant, SymmetricMessage)>,
    b_to_a: Vec<(tokio::time::Instant, SymmetricMessage)>,
}

/// Returns `true` for a datagram the simulated path should DROP.
type DropFilter = Box<dyn FnMut(&SymmetricMessage) -> bool + Send>;

fn keep_all() -> DropFilter {
    Box::new(|_| false)
}

/// Build two ends that talk to each other. Nothing runs until `start`.
fn pair(
    a_version_seen_by_b: Option<(u8, u8, u16)>,
    b_version_seen_by_a: Option<(u8, u8, u16)>,
) -> (End, End) {
    let ka = random_key();
    let kb = random_key();
    let a = end(
        addr(1),
        addr(2),
        ka.clone(),
        kb.clone(),
        b_version_seen_by_a,
    );
    let b = end(addr(2), addr(1), kb, ka, a_version_seen_by_b);
    (a, b)
}

struct Running {
    log_a: Arc<parking_lot::Mutex<Vec<(tokio::time::Instant, SymmetricMessage)>>>,
    log_b: Arc<parking_lot::Mutex<Vec<(tokio::time::Instant, SymmetricMessage)>>>,
    /// Application messages each end's `recv` returned.
    got_a: Arc<parking_lot::Mutex<Vec<Vec<u8>>>>,
    got_b: Arc<parking_lot::Mutex<Vec<Vec<u8>>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Running {
    fn stop(self) -> Link {
        for t in &self.tasks {
            t.abort();
        }
        Link {
            a_to_b: std::mem::take(&mut *self.log_a.lock()),
            b_to_a: std::mem::take(&mut *self.log_b.lock()),
        }
    }
}

/// Forward each end's datagrams to the other through a lossy path, logging
/// every datagram SENT (whether or not the path then drops it), and drive both
/// `recv` loops.
fn start(
    mut a: End,
    mut b: End,
    mut drop_a_to_b: DropFilter,
    mut drop_b_to_a: DropFilter,
) -> Running {
    let a_inbound = a.inbound.clone();
    let b_inbound = b.inbound.clone();
    let mut a_wire = std::mem::replace(&mut a.wire, mpsc::channel(1).1);
    let mut b_wire = std::mem::replace(&mut b.wire, mpsc::channel(1).1);
    let a_key = a.outbound_key.clone();
    let b_key = b.outbound_key.clone();
    let log_a = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let log_b = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let got_b = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let (la, lb) = (log_a.clone(), log_b.clone());
    let mut tasks = Vec::new();
    tasks.push(tokio::spawn(async move {
        while let Some((_, bytes)) = a_wire.recv().await {
            let p = PacketData::<UnknownEncryption>::from_buf(&*bytes);
            let m = SymmetricMessage::deser(p.try_decrypt_sym(&a_key).unwrap().data()).unwrap();
            let dropped = drop_a_to_b(&m);
            la.lock().push((tokio::time::Instant::now(), m));
            if !dropped && b_inbound.send(p).await.is_err() {
                return;
            }
        }
    }));
    tasks.push(tokio::spawn(async move {
        while let Some((_, bytes)) = b_wire.recv().await {
            let p = PacketData::<UnknownEncryption>::from_buf(&*bytes);
            let m = SymmetricMessage::deser(p.try_decrypt_sym(&b_key).unwrap().data()).unwrap();
            let dropped = drop_b_to_a(&m);
            lb.lock().push((tokio::time::Instant::now(), m));
            if !dropped && a_inbound.send(p).await.is_err() {
                return;
            }
        }
    }));
    let mut a_conn = a.conn;
    let mut b_conn = b.conn;
    let got_a = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let (ga, gb) = (got_a.clone(), got_b.clone());
    tasks.push(tokio::spawn(async move {
        while let Ok(msg) = a_conn.recv().await {
            ga.lock().push(msg);
        }
    }));
    tasks.push(tokio::spawn(async move {
        while let Ok(msg) = b_conn.recv().await {
            gb.lock().push(msg);
        }
    }));
    Running {
        log_a,
        log_b,
        got_a,
        got_b,
        tasks,
    }
}

/// Wire two ends together, run both `recv` loops for `dur`, and record every
/// datagram each sent (decrypted, timestamped). `prime` data packets are sent
/// in each direction first, so the test covers a connection that has carried
/// traffic, not just a fresh one.
async fn run_link(
    a_version_seen_by_b: Option<(u8, u8, u16)>,
    b_version_seen_by_a: Option<(u8, u8, u16)>,
    prime: usize,
    dur: Duration,
) -> Link {
    run_lossy_link(
        a_version_seen_by_b,
        b_version_seen_by_a,
        prime,
        dur,
        keep_all(),
        keep_all(),
    )
    .await
}

async fn run_lossy_link(
    a_version_seen_by_b: Option<(u8, u8, u16)>,
    b_version_seen_by_a: Option<(u8, u8, u16)>,
    prime: usize,
    dur: Duration,
    drop_a_to_b: DropFilter,
    drop_b_to_a: DropFilter,
) -> Link {
    let (mut a, mut b) = pair(a_version_seen_by_b, b_version_seen_by_a);
    for i in 0..prime {
        a.conn.send(format!("a-{i}")).await.expect("prime send");
        b.conn.send(format!("b-{i}")).await.expect("prime send");
    }
    let running = start(a, b, drop_a_to_b, drop_b_to_a);
    tokio::time::sleep(dur).await;
    running.stop()
}

fn max_gap(
    log: &[(tokio::time::Instant, SymmetricMessage)],
    from: tokio::time::Instant,
    to: tokio::time::Instant,
) -> Duration {
    let mut last = from;
    let mut gap = Duration::ZERO;
    for (t, _) in log {
        gap = gap.max(t.saturating_duration_since(last));
        last = *t;
    }
    gap.max(to.saturating_duration_since(last))
}

/// NAT safety plus the headline saving, between two capable peers after the
/// connection has carried data: each direction still sends a packet at least
/// every KEEP_ALIVE_INTERVAL (the keepalive Ping and the Pong to the remote's
/// Ping), and nothing else — no ack-of-ack chatter.
#[tokio::test(start_paused = true)]
async fn idle_capable_link_keeps_nat_mapping_with_only_ping_pong() {
    let start = tokio::time::Instant::now();
    let dur = Duration::from_secs(60);
    let link = run_link(CAPABLE, CAPABLE, 3, dur).await;
    let end_t = start + dur;

    for (name, log) in [("A->B", &link.a_to_b), ("B->A", &link.b_to_a)] {
        // Ignore the first second, which carries the priming data and its acks.
        let idle: Vec<_> = log
            .iter()
            .filter(|(t, _)| *t >= start + Duration::from_secs(1))
            .collect();
        let chatter: Vec<_> = idle
            .iter()
            .filter(|(_, m)| {
                !matches!(
                    m.payload,
                    SymmetricMessagePayload::Ping { .. } | SymmetricMessagePayload::Pong { .. }
                )
            })
            .collect();
        assert!(
            chatter.is_empty(),
            "{name}: idle link carried non-keepalive packets: {:?}",
            chatter.iter().map(|(_, m)| &m.payload).collect::<Vec<_>>()
        );
        // 60 s / 5 s = 12 pings + 12 pongs per direction (allow +-1 each for phase).
        assert!(
            (22..=26).contains(&idle.len()),
            "{name}: expected ~24 keepalive packets in 60 s, got {}",
            idle.len()
        );
        let gap = max_gap(log, start, end_t);
        assert!(
            gap <= KEEP_ALIVE_INTERVAL + Duration::from_millis(50),
            "{name}: NAT mapping at risk, {gap:?} without an outbound packet"
        );
    }
}

/// Mixed-version interop, from the new side's point of view: facing a peer it
/// must treat as old (unknown version), it keeps acking that peer's NoOps
/// within the ack interval, so the old peer never retransmits. Both ends here
/// are new code, so the other end does not ack NoOps — this checks the
/// direction a real old peer depends on: every NoOp sent by the end that sees
/// its remote as capable is acked by the end that sees its remote as old.
#[tokio::test(start_paused = true)]
async fn new_peer_acks_noops_of_a_peer_it_treats_as_old() {
    let start = tokio::time::Instant::now();
    // B sees A as unknown (old) -> B acks A's NoOps. A sees B as capable.
    let link = run_link(None, CAPABLE, 3, Duration::from_secs(20)).await;
    let a_noops: Vec<_> = link
        .a_to_b
        .iter()
        .filter(|(_, m)| matches!(m.payload, SymmetricMessagePayload::NoOp))
        .collect();
    assert!(!a_noops.is_empty(), "A must have acked B's priming acks");
    for (sent_at, m) in a_noops {
        let acked_at = link
            .b_to_a
            .iter()
            .find(|(_, r)| r.confirm_receipt.contains(&m.packet_id))
            .map(|(t, _)| *t)
            .unwrap_or_else(|| panic!("A's NoOp {} never acked by B", m.packet_id));
        assert!(
            acked_at.duration_since(*sent_at) <= ACK_CHECK_INTERVAL + Duration::from_millis(10),
            "B acked A's NoOp after {:?} (start {start:?})",
            acked_at.duration_since(*sent_at)
        );
    }
}

// ---------------------------------------------------------------------------
// recv() timers survive cancellation (#5795)
//
// `peer_connection_listener` cancels `recv()` whenever an outbound message
// wins its select, so `recv()` is re-entered constantly under traffic. These
// pump `recv()` in slices shorter than each timer's period to reproduce that.
// ---------------------------------------------------------------------------

/// The 100 ms ack timer must fire even when `recv()` is re-entered every
/// 10 ms. Before #5795 each call restarted it, so it never fired and receipts
/// waited for the 20-receipt or 600 ms-on-next-packet fallbacks.
#[tokio::test(start_paused = true)]
async fn ack_timer_survives_recv_cancellation() {
    let mut e = single(CAPABLE);
    e.deliver(5, short(b"hello"), vec![]).await;
    assert_eq!(
        e.pump(Duration::from_secs(1)).await.as_deref(),
        Some(&b"hello"[..])
    );
    let start = tokio::time::Instant::now();
    let mut acked_at = None;
    while start.elapsed() < Duration::from_millis(500) && acked_at.is_none() {
        assert!(e.pump(Duration::from_millis(10)).await.is_none());
        if receipts_for(&e.drain_sent(), 5) > 0 {
            acked_at = Some(start.elapsed());
        }
    }
    let acked_at = acked_at.expect("ack timer never fired under recv() re-entry");
    assert!(
        acked_at <= ACK_CHECK_INTERVAL + Duration::from_millis(10),
        "ack took {acked_at:?} under recv() re-entry"
    );
}

/// The resend check must fire even when `recv()` is re-entered faster than
/// its initial 10 ms delay. Before #5795 every call re-armed it 10 ms out, so
/// under steady traffic an unacked packet was never retransmitted.
#[tokio::test(start_paused = true)]
async fn resend_check_survives_recv_cancellation() {
    let mut e = single(CAPABLE);
    e.conn.send("needs-ack".to_string()).await.expect("send");
    let first = e.drain_sent();
    let data_id = first
        .iter()
        .find(|m| matches!(m.payload, SymmetricMessagePayload::ShortMessage { .. }))
        .expect("data packet sent")
        .packet_id;
    let start = tokio::time::Instant::now();
    let mut resent_at = None;
    while start.elapsed() < Duration::from_secs(3) && resent_at.is_none() {
        assert!(e.pump(Duration::from_millis(5)).await.is_none());
        if e.drain_sent().iter().any(|m| m.packet_id == data_id) {
            resent_at = Some(start.elapsed());
        }
    }
    let resent_at = resent_at.expect("unacked packet never retransmitted under recv() re-entry");
    // No RTT sample yet, so no TLP: the first retransmission is the initial
    // RTO (1 s, RFC 6298), checked within one resend poll.
    assert!(
        resent_at <= Duration::from_millis(1_050),
        "retransmission after {resent_at:?}"
    );
}

/// The 5 s timeout check (idle-timeout test, `pending_pings` write lock,
/// streaming-handle sweep) runs once per 5 s, not on every `recv()` call.
/// Before #5795 its first tick was immediate and the interval was rebuilt per
/// call, so it ran on every call.
#[tokio::test(start_paused = true)]
async fn timeout_check_runs_per_period_not_per_recv_call() {
    let mut e = single(CAPABLE);
    for _ in 0..200 {
        assert!(e.pump(Duration::from_millis(1)).await.is_none());
    }
    assert_eq!(
        e.conn.timeout_checks_run, 0,
        "200 recv() calls within the first 200 ms must not run the 5 s check"
    );
    // Then keep re-entering until just past one period: exactly one check.
    let start = tokio::time::Instant::now();
    while start.elapsed() < RecvTimers::<RealTime>::TIMEOUT_CHECK_INTERVAL {
        assert!(e.pump(Duration::from_millis(50)).await.is_none());
    }
    assert_eq!(e.conn.timeout_checks_run, 1, "one check per 5 s period");
}

/// Time source that counts the sleeps it creates.
#[derive(Clone)]
struct CountingTime {
    inner: RealTime,
    sleeps: Arc<std::sync::atomic::AtomicUsize>,
}

impl TimeSource for CountingTime {
    fn now_nanos(&self) -> u64 {
        self.inner.now_nanos()
    }
    fn sleep(
        &self,
        d: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.sleeps
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.sleep(d)
    }
    fn sleep_until(
        &self,
        deadline: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.sleeps
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.sleep_until(deadline)
    }
    fn timeout<F, R>(
        &self,
        d: Duration,
        f: F,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<R>> + Send>>
    where
        F: std::future::Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        self.inner.timeout(d, f)
    }
}

/// A `TimeSourceInterval` polled many times within one period (each `tick()`
/// future cancelled, as a select loop does) creates ONE sleep for that
/// period, not one per poll — and still ticks on schedule.
#[tokio::test(start_paused = true)]
async fn interval_reuses_its_sleep_across_cancelled_ticks() {
    let ts = CountingTime {
        inner: RealTime::new(),
        sleeps: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let period = Duration::from_millis(100);
    let mut interval = TimeSourceInterval::new_at(
        ts.clone(),
        ts.now_nanos() + period.as_nanos() as u64,
        period,
    );
    for _ in 0..50 {
        assert!(
            tokio::time::timeout(Duration::from_millis(1), interval.tick())
                .await
                .is_err(),
            "must not tick before the period elapses"
        );
    }
    assert_eq!(
        ts.sleeps.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "50 cancelled polls within one period must share one sleep"
    );
    let before = tokio::time::Instant::now();
    interval.tick().await;
    assert!(before.elapsed() <= period, "tick fired late");
    interval.tick().await;
    assert_eq!(
        ts.sleeps.load(std::sync::atomic::Ordering::Relaxed),
        2,
        "one sleep per period"
    );
}

// ---------------------------------------------------------------------------
// Review fixes (#5803)
// ---------------------------------------------------------------------------

/// A near-max short message whose piggybacked receipts do not fit is split:
/// the message goes alone, the receipts in separate NoOps. If the receiver's
/// ack for the message is lost, the sender must retransmit the MESSAGE (so the
/// receiver re-acks the duplicate), not a receipt NoOp that reused its id —
/// which a capable receiver never acks, so the message was retried until
/// abandoned although it had been delivered.
#[tokio::test(start_paused = true)]
async fn split_message_lost_ack_is_recovered_by_retransmitting_the_message() {
    let (mut a, b) = pair(CAPABLE, CAPABLE);
    // Pending receipts on A, so they cannot ride along with a near-max message.
    for id in 10_000..10_030 {
        assert_ne!(
            a.conn.received_tracker.report_received_packet(id),
            ReportResult::AlreadyReceived
        );
    }
    // bincode(Vec<u8>) = 8-byte length + bytes; leave a little slack so the
    // message alone still fits one packet.
    let len = MAX_DATA_SIZE - SymmetricMessage::short_message_overhead() - 8 - 2;
    let payload = vec![7u8; len];
    let tracker = a.conn.remote_conn.sent_tracker.clone();
    a.conn.send(payload.clone()).await.expect("send");

    // The message is the first packet A sends, so it has id 0. Drop B's first
    // ack for it.
    let mut dropped = false;
    let drop_first_ack: DropFilter = Box::new(move |m| {
        if !dropped && m.confirm_receipt.contains(&0) {
            dropped = true;
            return true;
        }
        false
    });
    let running = start(a, b, keep_all(), drop_first_ack);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let got_b = running.got_b.lock().clone();
    let link = running.stop();

    let first = &link.a_to_b[0].1;
    assert!(
        first.packet_id == 0
            && matches!(first.payload, SymmetricMessagePayload::ShortMessage { .. }),
        "premise: the message is A's first packet, id 0: {first:?}"
    );
    assert!(
        link.a_to_b
            .iter()
            .filter(|(_, m)| m.packet_id != 0)
            .any(|(_, m)| m.confirm_receipt.contains(&10_000)),
        "premise: the receipts went in a separate packet with a fresh id"
    );
    for (_, m) in link.a_to_b.iter().filter(|(_, m)| m.packet_id == 0) {
        assert!(
            matches!(m.payload, SymmetricMessagePayload::ShortMessage { .. }),
            "every packet sent under the message's id must be the message: {m:?}"
        );
    }
    assert!(
        link.a_to_b.iter().filter(|(_, m)| m.packet_id == 0).count() >= 2,
        "premise: the lost ack must have caused a retransmission"
    );
    assert!(
        !tracker.lock().contains_packet(0),
        "the retransmitted message must be re-acked, clearing it from the tracker"
    );
    let expected = bincode::serialize(&payload).unwrap();
    assert_eq!(
        got_b.iter().filter(|m| **m == expected).count(),
        1,
        "delivered exactly once"
    );
}

/// When a packet enters an EMPTY flight from outside `recv()` (the spawned
/// `send_stream` task does exactly this), its first loss probe must not wait
/// for the idle resend check, which is parked a full RTO (>= 500 ms) out.
#[tokio::test(start_paused = true)]
async fn first_probe_of_a_new_burst_is_not_held_to_the_idle_rto() {
    let e = single(CAPABLE);
    let tracker = e.conn.remote_conn.sent_tracker.clone();
    // One 20 ms RTT sample so TLP is enabled: PTO = 2 * SRTT = 40 ms.
    tracker
        .lock()
        .report_sent_packet(9_000, vec![0u8; 8].into_boxed_slice());
    tokio::time::advance(Duration::from_millis(20)).await;
    let _ = tracker.lock().report_received_receipts(&[9_000]);

    let End {
        mut conn, mut wire, ..
    } = e;
    let recv_task = tokio::spawn(async move { while conn.recv().await.is_ok() {} });
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Several insertion phases relative to the idle timer; without the wake,
    // most of them wait hundreds of ms for the probe.
    for (round, offset_ms) in [7u64, 113, 251, 389, 467].into_iter().enumerate() {
        tokio::time::sleep(Duration::from_millis(offset_ms)).await;
        while wire.try_recv().is_ok() {}
        let id = 9_100 + round as u32;
        let marker: Box<[u8]> = format!("burst-{round}").into_bytes().into_boxed_slice();
        let t0 = tokio::time::Instant::now();
        tracker.lock().report_sent_packet(id, marker.clone());
        let probe_after = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let (_, bytes) = wire.recv().await.expect("wire open");
                if *bytes == *marker {
                    return t0.elapsed();
                }
            }
        })
        .await
        .expect("no probe at all");
        assert!(
            probe_after <= Duration::from_millis(100),
            "round {round}: first probe after {probe_after:?}, held to the idle RTO"
        );
        let _ = tracker.lock().report_received_receipts(&[id]);
    }
    recv_task.abort();
}

/// NAT keepalive under total reverse-path loss: even when the peer is never
/// heard from, an established connection sends at least every 15 s. It used
/// to back off to 60 s.
#[tokio::test(start_paused = true)]
async fn keepalive_gap_is_capped_when_the_peer_is_never_heard() {
    let start_t = tokio::time::Instant::now();
    let dur = Duration::from_secs(100); // under the 120 s idle timeout
    let link = run_lossy_link(CAPABLE, CAPABLE, 0, dur, keep_all(), Box::new(|_| true)).await;
    let gap = max_gap(&link.a_to_b, start_t, start_t + dur);
    // A literal, not MAX_KEEPALIVE_INTERVAL: the bound is the owner's NAT
    // requirement, and must not move if someone raises the constant.
    let nat_cap = Duration::from_secs(15);
    assert!(
        gap <= nat_cap + Duration::from_millis(50),
        "A was silent for {gap:?} with its peer unheard (NAT cap {nat_cap:?})"
    );
}

/// Lost Pongs on a live link must not stretch the keepalive interval: any
/// packet from the peer (here its own Pings) resets the backoff, so the Ping
/// cadence stays at KEEP_ALIVE_INTERVAL.
#[tokio::test(start_paused = true)]
async fn lost_pongs_do_not_stretch_the_ping_interval_on_a_live_link() {
    let start_t = tokio::time::Instant::now();
    let dur = Duration::from_secs(100);
    let drop_pongs: DropFilter =
        Box::new(|m| matches!(m.payload, SymmetricMessagePayload::Pong { .. }));
    let link = run_lossy_link(CAPABLE, CAPABLE, 0, dur, keep_all(), drop_pongs).await;
    let pings: Vec<_> = link
        .a_to_b
        .into_iter()
        .filter(|(_, m)| matches!(m.payload, SymmetricMessagePayload::Ping { .. }))
        .collect();
    let gap = max_gap(&pings, start_t, start_t + dur);
    assert!(
        gap <= KEEP_ALIVE_INTERVAL + Duration::from_millis(50),
        "Ping interval stretched to {gap:?} although the peer was heard every 5 s"
    );
}

/// The handshake's connection ack always carries packet id 0, the same id as
/// the remote's first data packet. Recording it for dedup made that data
/// packet look like a duplicate and silently dropped it.
#[tokio::test(start_paused = true)]
async fn connection_ack_does_not_shadow_the_first_data_packet() {
    let mut e = single(CAPABLE);
    let ack = SymmetricMessage::ack_ok(&e.inbound_key, [0u8; 16], addr(2)).expect("ack");
    e.inbound
        .send(PacketData::<UnknownEncryption>::from_buf(ack.data()))
        .await
        .expect("deliver ack");
    e.deliver(SymmetricMessage::FIRST_PACKET_ID, short(b"first"), vec![])
        .await;
    assert_eq!(
        e.pump(Duration::from_secs(1)).await.as_deref(),
        Some(&b"first"[..]),
        "the first data packet must be delivered"
    );
}

/// A packet already recorded in the dedup window must reach `process_inbound`
/// even if `recv()` is cancelled while receipts are being flushed: the flush is
/// deferred, never awaited between recording and processing.
#[tokio::test(start_paused = true)]
async fn packet_survives_cancellation_during_a_blocked_receipt_flush() {
    let mut e = end_with_wire_capacity(addr(1), addr(2), random_key(), random_key(), CAPABLE, 1);
    // Fill the wire so any further send blocks.
    e.conn.noop(vec![1]).await.expect("fill wire");
    // Let the 600 ms receipt deadline lapse so the next packet forces a flush.
    tokio::time::advance(Duration::from_millis(700)).await;
    e.deliver(5, short(b"keep"), vec![]).await;
    assert_eq!(
        e.pump(Duration::from_secs(1)).await.as_deref(),
        Some(&b"keep"[..]),
        "recv must deliver the packet without first awaiting the blocked flush"
    );
}

// ---------------------------------------------------------------------------
// Review round 2 (#5803)
// ---------------------------------------------------------------------------

fn is_connection_ack(m: &SymmetricMessage) -> bool {
    matches!(
        m.payload,
        SymmetricMessagePayload::AckConnection { .. }
            | SymmetricMessagePayload::AckConnectionV2 { .. }
    )
}

/// The handshake leaves each side's completion ack TRACKED at packet id 0,
/// and our reply to a connection ack is itself a connection ack. Between two
/// peers that both answered every one, a single ack bounced forever. Each
/// side answers at most once, and the tracked handshake ack still gets its
/// receipt, so it stops being retransmitted.
#[tokio::test(start_paused = true)]
async fn connection_ack_exchange_is_bounded_and_clears_the_handshake_ack() {
    let (a, b) = pair(CAPABLE, CAPABLE);
    // Production seed: A's completion ack, tracked at id 0 like
    // `connection_handler` does, and delivered to B.
    let a_ack = SymmetricMessage::ack_ok(&a.outbound_key, [0u8; 16], addr(2)).expect("ack");
    a.conn
        .remote_conn
        .sent_tracker
        .lock()
        .report_sent_packet(SymmetricMessage::FIRST_PACKET_ID, a_ack.data().into());
    b.inbound
        .send(PacketData::<UnknownEncryption>::from_buf(a_ack.data()))
        .await
        .expect("deliver");
    let a_tracker = a.conn.remote_conn.sent_tracker.clone();
    // Stop forwarding connection acks after a few, so a regression shows up
    // as a failed count instead of an unbounded loop that never lets the
    // paused clock advance (the loop answers instantly, forever).
    fn cap_acks() -> DropFilter {
        let mut seen = 0usize;
        Box::new(move |m| {
            if is_connection_ack(m) {
                seen += 1;
                return seen > 20;
            }
            false
        })
    }
    let running = start(a, b, cap_acks(), cap_acks());
    tokio::time::sleep(Duration::from_secs(10)).await;
    let link = running.stop();
    let a_acks = link
        .a_to_b
        .iter()
        .filter(|(_, m)| is_connection_ack(m))
        .count();
    let b_acks = link
        .b_to_a
        .iter()
        .filter(|(_, m)| is_connection_ack(m))
        .count();
    assert!(
        b_acks <= 1 && a_acks <= 1,
        "connection acks must not ping-pong: A sent {a_acks}, B sent {b_acks}"
    );
    assert!(b_acks == 1, "premise: B answered A's ack once");
    assert!(
        !a_tracker
            .lock()
            .contains_packet(SymmetricMessage::FIRST_PACKET_ID),
        "A's tracked handshake ack must be acked, not retransmitted until abandoned"
    );
}

/// The keepalive task must survive a send error (network switch) and resume
/// pinging, never leaving more than 15 s between attempts. It used to exit on
/// the first error, leaving only Pong replies to hold the NAT mapping.
#[tokio::test(start_paused = true)]
async fn keepalive_survives_send_errors_and_resumes_within_cap() {
    let mut e = single(CAPABLE);
    let fail = e.fail_sends.clone();
    fail.store(true, std::sync::atomic::Ordering::Relaxed);
    // 30 s of failing sends (several keepalive ticks).
    for _ in 0..300 {
        assert!(e.pump(Duration::from_millis(100)).await.is_none());
    }
    assert!(
        e.drain_sent().is_empty(),
        "premise: nothing left while failing"
    );
    fail.store(false, std::sync::atomic::Ordering::Relaxed);
    let recovered = tokio::time::Instant::now();
    let mut pings = Vec::new();
    while recovered.elapsed() < Duration::from_secs(40) {
        assert!(e.pump(Duration::from_millis(100)).await.is_none());
        for m in e.drain_sent() {
            if matches!(m.payload, SymmetricMessagePayload::Ping { .. }) {
                pings.push(recovered.elapsed());
            }
        }
    }
    let first = *pings
        .first()
        .expect("keepalive never resumed after send errors");
    assert!(
        first <= Duration::from_secs(15),
        "first Ping {first:?} after recovery, NAT cap is 15 s"
    );
    for w in pings.windows(2) {
        assert!(
            w[1] - w[0] <= Duration::from_secs(15),
            "Ping gap {:?}",
            w[1] - w[0]
        );
    }
}

/// A full receipt queue (MAX_PENDING_RECEIPTS) is flushed right away, not at
/// the next 100 ms ack tick, even though the flush is deferred to the top of
/// the recv loop.
#[tokio::test(start_paused = true)]
async fn full_receipt_queue_flushes_before_the_ack_tick() {
    use crate::transport::received_packet_tracker::MAX_PENDING_RECEIPTS;
    let mut e = single(CAPABLE);
    let n = MAX_PENDING_RECEIPTS as u32;
    for id in 0..n {
        e.deliver(100 + id, short(b"m"), vec![]).await;
    }
    let start_t = tokio::time::Instant::now();
    for _ in 0..n {
        assert!(e.pump(Duration::from_millis(1)).await.is_some());
    }
    assert!(e.pump(Duration::from_millis(1)).await.is_none());
    assert!(
        start_t.elapsed() < ACK_CHECK_INTERVAL,
        "premise: still before the first ack tick"
    );
    let sent = e.drain_sent();
    let acked: std::collections::HashSet<u32> = sent
        .iter()
        .flat_map(|m| m.confirm_receipt.clone())
        .collect();
    assert!(
        (100..100 + n).all(|id| acked.contains(&id)),
        "a full queue must be flushed immediately: acked {acked:?}"
    );
}

/// Deterministic loss: drop every `nth` datagram that passes.
fn drop_every(nth: usize) -> DropFilter {
    let mut count = 0usize;
    Box::new(move |_| {
        count += 1;
        count % nth == 0
    })
}

/// Exchange data both ways over a lossy path, then let it settle. Every
/// message is delivered exactly once, nothing stays in flight, and no packet
/// gets anywhere near the abandon limit.
async fn lossy_exchange(
    a_seen_by_b: Option<(u8, u8, u16)>,
    b_seen_by_a: Option<(u8, u8, u16)>,
    b_legacy: bool,
) {
    let (mut a, mut b) = pair(a_seen_by_b, b_seen_by_a);
    b.conn.legacy_wire_behaviour = b_legacy;
    let a_tracker = a.conn.remote_conn.sent_tracker.clone();
    let b_tracker = b.conn.remote_conn.sent_tracker.clone();
    const N: usize = 40;
    for i in 0..N {
        a.conn.send(format!("a-{i}")).await.expect("send");
        b.conn.send(format!("b-{i}")).await.expect("send");
    }
    // One multi-fragment stream each way too.
    a.conn.send(vec![1u8; 200_000]).await.expect("stream");
    b.conn.send(vec![2u8; 200_000]).await.expect("stream");
    // ~14% loss A->B, 20% B->A, hitting data, acks and retransmissions alike.
    let running = start(a, b, drop_every(7), drop_every(5));
    tokio::time::sleep(Duration::from_secs(60)).await;
    let got_a = running.got_a.lock().clone();
    let got_b = running.got_b.lock().clone();
    let link = running.stop();

    for i in 0..N {
        let want = bincode::serialize(&format!("b-{i}")).unwrap();
        assert_eq!(
            got_a.iter().filter(|m| **m == want).count(),
            1,
            "message b-{i} delivered to A exactly once"
        );
    }
    let stream_b = bincode::serialize(&vec![2u8; 200_000]).unwrap();
    assert_eq!(
        got_a.iter().filter(|m| **m == stream_b).count(),
        1,
        "B's stream delivered to A once"
    );
    for i in 0..N {
        let want = bincode::serialize(&format!("a-{i}")).unwrap();
        assert_eq!(
            got_b.iter().filter(|m| **m == want).count(),
            1,
            "message a-{i} delivered exactly once"
        );
    }
    let stream = bincode::serialize(&vec![1u8; 200_000]).unwrap();
    assert_eq!(
        got_b.iter().filter(|m| **m == stream).count(),
        1,
        "stream delivered once"
    );
    for (name, tracker, log) in [
        ("A", &a_tracker, &link.a_to_b),
        ("B", &b_tracker, &link.b_to_a),
    ] {
        let t = tracker.lock();
        // A pre-#5795 stand-in keeps one tracked ack-only NoOp in flight at
        // any instant (that is the chatter), so NoOps are exempt; every data
        // packet must have been acked.
        let noop_ids: std::collections::HashSet<u32> = log
            .iter()
            .filter(|(_, m)| matches!(m.payload, SymmetricMessagePayload::NoOp))
            .map(|(_, m)| m.packet_id)
            .collect();
        for id in 0..10_000u32 {
            assert!(
                noop_ids.contains(&id) || !t.contains_packet(id),
                "{name}: data packet {id} still in flight after settling"
            );
        }
    }
    for (name, log) in [("A", &link.a_to_b), ("B", &link.b_to_a)] {
        let mut per_id: std::collections::HashMap<u32, usize> = std::collections::HashMap::new();
        for (_, m) in log.iter().filter(|(_, m)| {
            !matches!(
                m.payload,
                SymmetricMessagePayload::Ping { .. } | SymmetricMessagePayload::Pong { .. }
            )
        }) {
            *per_id.entry(m.packet_id).or_default() += 1;
        }
        let worst = per_id.values().max().copied().unwrap_or(0);
        // 1 + MAX_PACKET_RETRANSMITS (12) sends means it was abandoned.
        assert!(
            worst < 13,
            "{name}: some packet was sent {worst} times, i.e. abandoned"
        );
    }
}

/// Gate ON (both peers at or above the floor), lossy both ways.
#[tokio::test(start_paused = true)]
async fn lossy_exchange_between_capable_peers_recovers_every_ack() {
    lossy_exchange(CAPABLE, CAPABLE, false).await;
}

/// Mixed versions, lossy: B behaves like a pre-#5795 peer (tracked ack-only
/// NoOps, a receipt for every packet, no duplicate re-ack). A sees B's version
/// as old, so it acks B's NoOps; B never ends up retransmitting them to the
/// abandon limit.
#[tokio::test(start_paused = true)]
async fn lossy_exchange_with_an_old_peer_stand_in() {
    lossy_exchange(CAPABLE, OLD, true).await;
}

/// The thread-local test API turns the receive-side gate on for connections
/// created while it is enabled, whatever version the remote reports.
#[tokio::test(start_paused = true)]
async fn force_noop_gate_api_applies_to_new_connections() {
    crate::config::SimulationForceNoopGate::enable();
    let forced = single(OLD);
    crate::config::SimulationForceNoopGate::disable();
    let normal = single(OLD);
    assert!(
        !forced.conn.ack_remote_noops,
        "forced: an old-version remote is treated as capable"
    );
    // Unless the whole run set the env override.
    if std::env::var("FREENET_TEST_FORCE_NOOP_GATE").as_deref() != Ok("1") {
        assert!(normal.conn.ack_remote_noops);
    }
}

// ---------------------------------------------------------------------------
// Review round 3 (#5803)
// ---------------------------------------------------------------------------

/// A legacy stream fragment is recorded for dedup BEFORE it is handed to its
/// reassembly task. If `recv()` is cancelled while that hand-off waits for
/// channel space, the fragment must not be lost: its retransmission would be
/// dropped as a duplicate. It stays parked and the next `recv()` delivers it.
#[tokio::test(start_paused = true)]
async fn legacy_fragment_survives_cancellation_during_a_blocked_handoff() {
    let mut e = single(CAPABLE);
    let stream_id = StreamId::next();
    assert!(!stream_id.is_operations_stream(), "premise: legacy stream");
    // A full one-slot channel stands in for a backed-up recv_stream task.
    let (tx, mut rx) = mpsc::channel(1);
    tx.try_send((99u32, bytes::Bytes::from_static(b"filler")))
        .expect("fill");
    e.conn.inbound_streams.insert(stream_id, tx);

    let fragment = SymmetricMessagePayload::StreamFragment {
        stream_id,
        total_length_bytes: 1_000_000,
        fragment_number: 7,
        payload: bytes::Bytes::from_static(b"fragment-7"),
        metadata_bytes: None,
    };
    // The hand-off blocks on the full channel; cancel it (as the listener's
    // select does when an outbound message wins).
    let blocked =
        tokio::time::timeout(Duration::from_millis(10), e.conn.process_inbound(fragment)).await;
    assert!(
        blocked.is_err(),
        "premise: the hand-off was blocked and cancelled"
    );

    // The reassembly task catches up; the next recv() must finish the hand-off.
    assert_eq!(rx.recv().await.expect("filler").0, 99);
    assert!(e.pump(Duration::from_millis(10)).await.is_none());
    let delivered = rx
        .try_recv()
        .expect("the parked fragment must be delivered");
    assert_eq!(delivered.0, 7);
    assert_eq!(&delivered.1[..], b"fragment-7");
}
