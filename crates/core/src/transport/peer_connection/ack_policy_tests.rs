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
use crate::transport::symmetric_message::UpgradeControl;

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
    inbound: mpsc::Sender<InboundPacket>,
    /// The connection's clock, to stamp fed packets as the listener does.
    clock: RealTime,
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
    let clock = time_source.clone();
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
        clock,
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
    /// Queue `packet` for this end, stamped with its clock as the listener
    /// stamps it.
    fn queued(&self, packet: PacketData<UnknownEncryption>) -> InboundPacket {
        InboundPacket {
            packet,
            queued_nanos: self.clock.now_nanos(),
        }
    }

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
            .send(InboundPacket {
                packet: PacketData::<UnknownEncryption>::from_buf(packet.data()),
                queued_nanos: self.clock.now_nanos(),
            })
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

fn upgrade(control: UpgradeControl) -> SymmetricMessagePayload {
    SymmetricMessagePayload::Upgrade(control)
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
        assert_eq!(
            receipt_policy(&upgrade(UpgradeControl::Accept { attempt: 1 }), ack_noops),
            Ack {
                reack_duplicate: true
            },
            "an upgrade message is tracked by its sender: ack it, dedup it, re-ack duplicates"
        );
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
    let (a_clock, b_clock) = (a.clock.clone(), b.clock.clone());
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
            let p = InboundPacket {
                packet: p,
                queued_nanos: b_clock.now_nanos(),
            };
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
            let p = InboundPacket {
                packet: p,
                queued_nanos: a_clock.now_nanos(),
            };
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
        .send(e.queued(PacketData::<UnknownEncryption>::from_buf(ack.data())))
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
        .send(b.queued(PacketData::<UnknownEncryption>::from_buf(a_ack.data())))
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
    let stream_id = StreamId::next_transport_internal();
    assert!(!stream_id.is_caller_named(), "premise: legacy stream");
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
    let blocked = tokio::time::timeout(
        Duration::from_millis(10),
        e.conn.process_inbound(fragment, 0),
    )
    .await;
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

// ---------------------------------------------------------------------------
// Transport telemetry (redesign baseline)
// ---------------------------------------------------------------------------

/// A forced retransmission is counted: the connection's tracker counts the
/// RTO expiry, and the recv loop records the retransmission (asserted per
/// connection, not on the process-wide counters concurrent tests also move).
#[tokio::test(start_paused = true)]
async fn forced_retransmit_increments_loss_counters() {
    let mut e = single(CAPABLE);
    e.conn.send("needs-ack".to_string()).await.expect("send");
    let data_id = e
        .drain_sent()
        .iter()
        .find(|m| matches!(m.payload, SymmetricMessagePayload::ShortMessage { .. }))
        .expect("data packet sent")
        .packet_id;

    let start = tokio::time::Instant::now();
    let mut resent = false;
    while start.elapsed() < Duration::from_secs(3) && !resent {
        assert!(e.pump(Duration::from_millis(5)).await.is_none());
        resent = e.drain_sent().iter().any(|m| m.packet_id == data_id);
    }
    assert!(resent, "the unacked packet must be retransmitted");

    assert!(
        e.conn.remote_conn.sent_tracker.lock().rto_expiries() >= 1,
        "the connection's tracker must count the RTO expiry"
    );
    assert!(
        e.conn.telemetry_test_counts.retransmissions >= 1,
        "the retransmission must be recorded"
    );
}

/// After a ping/pong round trip an otherwise idle connection has a
/// non-zero per-connection SRTT (and min RTT) for the snapshot's
/// cross-connection distribution.
#[tokio::test(start_paused = true)]
async fn ping_pong_gives_connection_nonzero_srtt() {
    let mut e = single(CAPABLE);
    assert_eq!(
        e.conn.remote_conn.rolling_rtt_stats.gauge().read(),
        (None, None)
    );

    let seq = 4242;
    let sent_at = e.conn.time_source.now_nanos();
    e.conn.pending_pings.write().insert(seq, sent_at);
    tokio::time::advance(Duration::from_millis(40)).await;
    e.deliver(1, SymmetricMessagePayload::Pong { sequence: seq }, vec![])
        .await;
    e.deliver(2, short(b"after-pong"), vec![]).await;
    assert!(e.pump(Duration::from_millis(50)).await.is_some());

    let (srtt, min_rtt) = e.conn.remote_conn.rolling_rtt_stats.gauge().read();
    let srtt = srtt.expect("SRTT must be set after a ping/pong");
    let min_rtt = min_rtt.expect("min RTT must be set after a ping/pong");
    assert!(
        (40_000..60_000).contains(&srtt),
        "SRTT {srtt}us should reflect the 40 ms round trip"
    );
    assert_eq!(srtt, min_rtt, "one sample: SRTT == min RTT");
}

/// A data ACK publishes the sent-packet tracker's SRTT, which then takes
/// precedence over the keep-alive estimate.
#[tokio::test(start_paused = true)]
async fn data_ack_publishes_tracker_srtt() {
    let mut e = single(CAPABLE);
    e.conn.send("needs-ack".to_string()).await.expect("send");
    let data_id = e
        .drain_sent()
        .iter()
        .find(|m| matches!(m.payload, SymmetricMessagePayload::ShortMessage { .. }))
        .expect("data packet sent")
        .packet_id;
    tokio::time::advance(Duration::from_millis(30)).await;
    e.deliver(1, SymmetricMessagePayload::NoOp, vec![data_id])
        .await;
    e.deliver(2, short(b"after-ack"), vec![]).await;
    assert!(e.pump(Duration::from_millis(50)).await.is_some());

    let tracker_srtt = e
        .conn
        .remote_conn
        .sent_tracker
        .lock()
        .smoothed_rtt()
        .expect("the ack must give the tracker an SRTT");
    let (srtt, _) = e.conn.remote_conn.rolling_rtt_stats.gauge().read();
    assert_eq!(srtt, Some(tracker_srtt.as_micros() as u64));
    assert!(srtt.unwrap() >= 30_000);
}

/// Standalone ack-only NoOps are counted.
#[tokio::test(start_paused = true)]
async fn standalone_ack_noops_are_counted() {
    let mut e = single(CAPABLE);
    e.deliver(5, short(b"please-ack"), vec![]).await;
    assert!(e.pump(Duration::from_millis(10)).await.is_some());
    // Let the ack timer flush the receipt as a standalone NoOp.
    let start = tokio::time::Instant::now();
    let mut acked = false;
    while start.elapsed() < Duration::from_millis(500) && !acked {
        assert!(e.pump(Duration::from_millis(20)).await.is_none());
        acked = receipts_for(&e.drain_sent(), 5) > 0;
    }
    assert!(acked, "the receipt must be sent in a standalone NoOp");
    assert!(
        e.conn.telemetry_test_counts.standalone_ack_flushes >= 1,
        "the standalone ack-only NoOp must be recorded"
    );
}

/// When nothing arrives for the idle timeout, `recv` errors and the
/// connection reports that it closed on the idle timeout, so the node labels
/// the disconnect "timeout".
#[tokio::test(start_paused = true)]
async fn idle_timeout_is_reported_for_disconnect_reason() {
    let mut e = single(CAPABLE);
    assert!(!e.conn.closed_by_idle_timeout);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    let err = loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "idle timeout never fired"
        );
        match tokio::time::timeout(Duration::from_secs(1), e.conn.recv()).await {
            Ok(Ok(_)) => panic!("no message was delivered"),
            Ok(Err(err)) => break err,
            Err(_) => {
                // Keep the wire drained so keepalive sends never block.
                e.drain_sent();
            }
        }
    };
    assert!(
        matches!(err, TransportError::ConnectionClosed(_)),
        "{err:?}"
    );
    assert!(
        crate::transport::PeerConnectionApi::close_reason(&e.conn)
            == Some(crate::transport::CloseReason::IdleTimeout),
        "the idle-timeout exit must be flagged"
    );
}

/// End to end through `send_stream`: a streamed message whose fragments are
/// all acknowledged after the stream handed them to the socket is counted as
/// a fully acknowledged transfer once the recv loop processes the receipts.
#[tokio::test(start_paused = true)]
async fn streamed_transfer_counts_final_ack() {
    let mut e = single(CAPABLE);
    e.conn.send("s".repeat(20_000)).await.expect("send");
    // Let the spawned stream task hand every fragment to the socket and
    // register its final-ACK watch.
    let mut fragments = Vec::new();
    for _ in 0..50 {
        assert!(e.pump(Duration::from_millis(2)).await.is_none());
        fragments.extend(
            e.drain_sent()
                .into_iter()
                .filter(|m| matches!(m.payload, SymmetricMessagePayload::StreamFragment { .. }))
                .map(|m| m.packet_id),
        );
    }
    assert!(fragments.len() > 1, "a 20 KB message must be streamed");
    assert_eq!(e.conn.telemetry_test_counts.transfers_acked, 0);
    e.deliver(1, SymmetricMessagePayload::NoOp, fragments).await;
    e.deliver(2, short(b"after-acks"), vec![]).await;
    assert!(e.pump(Duration::from_millis(50)).await.is_some());
    assert_eq!(
        e.conn.telemetry_test_counts.transfers_acked, 1,
        "acknowledging every fragment must count one fully acknowledged transfer"
    );
}

/// The Abandon arm of the recv loop: a packet that is never acknowledged is
/// eventually abandoned, and the abandonment is counted.
#[tokio::test(start_paused = true)]
async fn never_acked_packet_is_abandoned_and_counted() {
    let mut e = single(CAPABLE);
    e.conn.send("never-acked".to_string()).await.expect("send");
    let data_id = e
        .drain_sent()
        .iter()
        .find(|m| matches!(m.payload, SymmetricMessagePayload::ShortMessage { .. }))
        .expect("data packet sent")
        .packet_id;
    let start = tokio::time::Instant::now();
    let mut next_ping = 1_000u32;
    while e
        .conn
        .remote_conn
        .sent_tracker
        .lock()
        .contains_packet(data_id)
    {
        assert!(
            start.elapsed() < Duration::from_secs(1_200),
            "the packet was never abandoned"
        );
        // Keep the connection alive (no ACK for our packet, so the backoff
        // still escalates) by delivering a remote Ping now and then.
        e.deliver(
            next_ping,
            SymmetricMessagePayload::Ping {
                sequence: next_ping as u64,
            },
            vec![],
        )
        .await;
        next_ping += 1;
        for _ in 0..10 {
            assert!(e.pump(Duration::from_secs(1)).await.is_none());
            e.drain_sent();
        }
    }
    assert_eq!(
        e.conn.telemetry_test_counts.abandoned, 1,
        "the recv loop's Abandon arm must record exactly one abandonment"
    );
}

/// An ACK batch that yields no RTT sample (Karn's rule: it acks only a
/// retransmitted packet) must not republish the old data-ACK SRTT as the
/// freshest value over a newer keep-alive SRTT.
#[tokio::test(start_paused = true)]
async fn retransmit_only_ack_does_not_republish_stale_srtt() {
    let mut e = single(CAPABLE);
    let first_data_id = |sent: Vec<SymmetricMessage>| {
        sent.into_iter()
            .find(|m| matches!(m.payload, SymmetricMessagePayload::ShortMessage { .. }))
            .expect("data packet sent")
            .packet_id
    };
    // 1. A fresh data ACK: ack SRTT ~30 ms.
    e.conn.send("first".to_string()).await.expect("send");
    let a = first_data_id(e.drain_sent());
    tokio::time::advance(Duration::from_millis(30)).await;
    e.deliver(1, SymmetricMessagePayload::NoOp, vec![a]).await;
    e.deliver(2, short(b"x"), vec![]).await;
    assert!(e.pump(Duration::from_millis(5)).await.is_some());
    // 2. A newer keep-alive sample: keep-alive SRTT ~80 ms, now the freshest.
    let seq = 77;
    let sent_at = e.conn.time_source.now_nanos();
    e.conn.pending_pings.write().insert(seq, sent_at);
    tokio::time::advance(Duration::from_millis(80)).await;
    e.deliver(3, SymmetricMessagePayload::Pong { sequence: seq }, vec![])
        .await;
    e.deliver(4, short(b"y"), vec![]).await;
    assert!(e.pump(Duration::from_millis(5)).await.is_some());
    let (keepalive_srtt, _) = e.conn.remote_conn.rolling_rtt_stats.gauge().read();
    let keepalive_srtt = keepalive_srtt.expect("keep-alive SRTT");
    assert!(keepalive_srtt >= 80_000, "{keepalive_srtt}");
    // 3. A packet that is retransmitted, then acked: no RTT sample.
    e.conn.send("second".to_string()).await.expect("send");
    let b = first_data_id(e.drain_sent());
    let start = tokio::time::Instant::now();
    let mut resent = false;
    while start.elapsed() < Duration::from_secs(3) && !resent {
        assert!(e.pump(Duration::from_millis(5)).await.is_none());
        resent = e.drain_sent().iter().any(|m| m.packet_id == b);
    }
    assert!(resent, "packet must be retransmitted");
    e.deliver(5, SymmetricMessagePayload::NoOp, vec![b]).await;
    e.deliver(6, short(b"z"), vec![]).await;
    assert!(e.pump(Duration::from_millis(5)).await.is_some());
    assert_eq!(
        e.conn.remote_conn.rolling_rtt_stats.gauge().read().0,
        Some(keepalive_srtt),
        "a sample-less ACK batch must not make the stale ack SRTT the freshest"
    );
}

// ---------------------------------------------------------------------------
// Inbound stream admission and accounting
// ---------------------------------------------------------------------------

fn fragment(
    stream_id: StreamId,
    total_length_bytes: u64,
    fragment_number: u32,
) -> SymmetricMessagePayload {
    fragment_with(
        stream_id,
        total_length_bytes,
        fragment_number,
        bytes::Bytes::from_static(b"fragment"),
    )
}

fn fragment_with(
    stream_id: StreamId,
    total_length_bytes: u64,
    fragment_number: u32,
    payload: bytes::Bytes,
) -> SymmetricMessagePayload {
    SymmetricMessagePayload::StreamFragment {
        stream_id,
        total_length_bytes,
        fragment_number,
        payload,
        metadata_bytes: None,
    }
}

/// What the connection's budget charges a stream declaring `declared` bytes.
fn cost(declared: u64) -> u64 {
    super::inbound_budget::ChargeOverhead::DEFAULT.cost(declared)
}

/// True if the connection holds no reassembly state at all for `stream_id`.
fn holds_no_state(e: &End, stream_id: StreamId) -> bool {
    !e.conn.streaming_handles.contains_key(&stream_id)
        && e.conn.streaming_registry.get(stream_id).is_none()
        && !e.conn.inbound_streams.contains_key(&stream_id)
        && e.conn.inbound_stream_futures.is_empty()
        && !e.conn.inbound_budget.contains(stream_id)
}

async fn feed(e: &mut End, payload: SymmetricMessagePayload) -> Option<Vec<u8>> {
    let queued_nanos = e.clock.now_nanos();
    e.conn
        .process_inbound(payload, queued_nanos)
        .await
        .expect("an inbound stream fragment must not fail the connection")
}

/// Let spawned reassembly tasks run to completion.
async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

/// A fragment declaring an invalid stream length is dropped before anything
/// is created for its stream, on either reassembly path.
#[tokio::test(start_paused = true)]
async fn fragment_declaring_an_invalid_length_creates_no_stream_state() {
    use super::streaming::MAX_STREAM_TOTAL_BYTES;
    for declared in [MAX_STREAM_TOTAL_BYTES + 1, 0, u64::MAX] {
        for stream_id in [StreamId::next_transport_internal(), StreamId::next()] {
            let mut e = single(CAPABLE);
            assert!(
                feed(&mut e, fragment(stream_id, declared, 1))
                    .await
                    .is_none()
            );
            assert!(
                holds_no_state(&e, stream_id),
                "declared {declared} on {stream_id:?}: no state may be created"
            );
        }
    }
}

/// A stream declaring exactly the cap is admitted on both paths and charged
/// its declared length.
#[tokio::test(start_paused = true)]
async fn fragment_declaring_the_maximum_length_is_admitted() {
    use super::streaming::MAX_STREAM_TOTAL_BYTES;
    let mut e = single(CAPABLE);
    let ops = StreamId::next();
    feed(&mut e, fragment(ops, MAX_STREAM_TOTAL_BYTES, 1)).await;
    let (handle, _) = e.conn.streaming_handles.get(&ops).expect("registered");
    assert_eq!(handle.total_bytes(), MAX_STREAM_TOTAL_BYTES);
    assert_eq!(handle.received_fragments(), 1);
    assert_eq!(
        e.conn.inbound_budget.charged_bytes(),
        cost(MAX_STREAM_TOTAL_BYTES)
    );

    let mut e = single(CAPABLE);
    let legacy = StreamId::next_transport_internal();
    feed(&mut e, fragment(legacy, MAX_STREAM_TOTAL_BYTES, 1)).await;
    assert!(e.conn.inbound_streams.contains_key(&legacy));
    assert!(
        !e.conn.streaming_handles.contains_key(&legacy),
        "a legacy stream is reassembled on one path only"
    );
    assert_eq!(e.conn.inbound_budget.stream_count(), 1);
    assert_eq!(
        e.conn.inbound_budget.charged_bytes(),
        cost(MAX_STREAM_TOTAL_BYTES)
    );
}

/// A refused stream stays refused: a later fragment declaring a valid length
/// creates no state, since its earlier fragments were already dropped.
#[tokio::test(start_paused = true)]
async fn a_refused_stream_stays_refused() {
    use super::streaming::MAX_STREAM_TOTAL_BYTES;
    for stream_id in [StreamId::next_transport_internal(), StreamId::next()] {
        let mut e = single(CAPABLE);
        feed(&mut e, fragment(stream_id, MAX_STREAM_TOTAL_BYTES + 1, 1)).await;
        feed(&mut e, fragment(stream_id, 5_000, 2)).await;
        assert!(holds_no_state(&e, stream_id));
    }
}

/// A stream whose handle is gone but whose buffer a consumer still holds
/// keeps its charge, and its later fragments neither recreate state nor
/// slip past the allowance.
#[tokio::test(start_paused = true)]
async fn a_released_handle_cannot_restart_its_stream() {
    let mut e = single(CAPABLE);
    let id = StreamId::next();
    feed(&mut e, fragment(id, 50_000, 1)).await;
    let consumer = e.conn.streaming_registry.get(id).expect("registered");
    // As the idle sweep does.
    e.conn.streaming_handles.remove(&id);
    e.conn.streaming_registry.remove(id);

    feed(&mut e, fragment(id, 50_000, 2)).await;
    assert!(!e.conn.streaming_handles.contains_key(&id));
    assert!(e.conn.streaming_registry.get(id).is_none());
    assert_eq!(e.conn.inbound_budget.stream_count(), 1);
    assert_eq!(e.conn.inbound_budget.charged_bytes(), cost(50_000));
    drop(consumer);
}

/// A charge is released only when every holder of the stream's reassembly
/// state has let go: for an operations stream, the handle map, the registry
/// and any consumer clone; for a legacy stream, its reassembly task.
#[tokio::test(start_paused = true)]
async fn charges_are_released_only_when_all_reassembly_state_is_gone() {
    let mut e = single(CAPABLE);

    let ops = StreamId::next();
    feed(&mut e, fragment(ops, 50_000, 1)).await;
    let consumer = e.conn.streaming_registry.get(ops).expect("registered");
    e.conn.streaming_handles.remove(&ops);
    e.conn.streaming_registry.remove(ops);
    e.conn.sweep_inbound_streams();
    assert_eq!(
        e.conn.inbound_budget.charged_bytes(),
        cost(50_000),
        "a consumer clone still holds the buffer"
    );
    drop(consumer);
    e.conn.sweep_inbound_streams();
    assert_eq!(e.conn.inbound_budget.charged_bytes(), 0);

    let legacy = StreamId::next_transport_internal();
    feed(&mut e, fragment(legacy, 50_000, 1)).await;
    // Dropping the map entry alone does not release it while the task runs.
    let sender = e
        .conn
        .inbound_streams
        .remove(&legacy)
        .expect("legacy state");
    settle().await;
    e.conn.sweep_inbound_streams();
    assert_eq!(
        e.conn.inbound_budget.charged_bytes(),
        cost(50_000),
        "the reassembly task is still alive"
    );
    drop(sender);
    settle().await;
    e.conn.sweep_inbound_streams();
    assert_eq!(
        e.conn.inbound_budget.charged_bytes(),
        cost(50_000),
        "the task's output has not been taken by recv() yet"
    );
    // recv() takes the ended task's output, releasing the charge.
    assert!(e.pump(Duration::from_millis(10)).await.is_none());
    assert_eq!(e.conn.inbound_budget.charged_bytes(), 0);
    assert_eq!(e.conn.inbound_budget.stream_count(), 0);
}

/// The bytes held per connection are bounded: beside one maximum-size
/// stream there is room for ordinary streams up to the headroom, and no
/// more, while the admitted ones keep receiving.
#[tokio::test(start_paused = true)]
async fn inbound_stream_bytes_are_bounded_per_connection() {
    use super::streaming::{MAX_INBOUND_STREAM_BYTES_PER_CONNECTION, MAX_STREAM_TOTAL_BYTES};
    let headroom = 16 * 1024 * 1024;
    assert_eq!(
        MAX_INBOUND_STREAM_BYTES_PER_CONNECTION,
        cost(MAX_STREAM_TOTAL_BYTES) + headroom,
        "premise"
    );
    let mut e = single(CAPABLE);
    let first = StreamId::next();
    feed(&mut e, fragment(first, MAX_STREAM_TOTAL_BYTES, 1)).await;
    // Ordinary streams fit beside it, up to the headroom.
    let small = 1024 * 1024;
    let fit = headroom / cost(small);
    for i in 0..fit {
        let id = if i % 2 == 0 {
            StreamId::next()
        } else {
            StreamId::next_transport_internal()
        };
        feed(&mut e, fragment(id, small, 1)).await;
        assert!(e.conn.inbound_budget.contains(id), "stream {i} of {fit}");
    }

    for over in [StreamId::next(), StreamId::next_transport_internal()] {
        feed(&mut e, fragment(over, small, 1)).await;
        assert!(holds_no_state_but_others(&e, over));
    }

    feed(&mut e, fragment(first, MAX_STREAM_TOTAL_BYTES, 2)).await;
    let (handle, _) = e.conn.streaming_handles.get(&first).unwrap();
    assert_eq!(handle.received_fragments(), 2);
}

fn holds_no_state_but_others(e: &End, stream_id: StreamId) -> bool {
    !e.conn.streaming_handles.contains_key(&stream_id)
        && e.conn.streaming_registry.get(stream_id).is_none()
        && !e.conn.inbound_streams.contains_key(&stream_id)
        && !e.conn.inbound_budget.contains(stream_id)
}

/// The number of streams per connection is bounded, so many small streams
/// cannot accumulate per-stream state without limit.
#[tokio::test(start_paused = true)]
async fn inbound_stream_count_is_bounded_per_connection() {
    use super::streaming::MAX_INBOUND_STREAMS_PER_CONNECTION;
    let mut e = single(CAPABLE);
    for _ in 0..MAX_INBOUND_STREAMS_PER_CONNECTION {
        let id = StreamId::next();
        feed(&mut e, fragment(id, 10_000, 1)).await;
        assert!(e.conn.streaming_handles.contains_key(&id));
    }
    let over = StreamId::next();
    feed(&mut e, fragment(over, 10_000, 1)).await;
    assert!(holds_no_state_but_others(&e, over));
    assert_eq!(
        e.conn.inbound_budget.stream_count(),
        MAX_INBOUND_STREAMS_PER_CONNECTION
    );
}

/// A stream that keeps receiving data is still evicted at the absolute age
/// limit, and a stream that stops receiving is evicted when idle, on either
/// path; their charges are released once the state is gone.
#[tokio::test(start_paused = true)]
async fn inbound_streams_are_evicted_when_idle_or_too_old() {
    use super::streaming::{STREAM_INACTIVITY_TIMEOUT, max_inbound_stream_age};
    let mut e = single(CAPABLE);
    // `busy` arrives at ~28 KiB/s, too slow to finish within its age limit
    // (its length at 32 KiB/s); its progress keeps it from idling out.
    let total = 10 * 1024 * 1024u64;
    let age_limit = max_inbound_stream_age(total);
    let full = bytes::Bytes::from(vec![5u8; super::MAX_DATA_SIZE]);
    let per_step = 28 * 1024 * 5 / super::MAX_DATA_SIZE;
    let busy = StreamId::next();
    let quiet = StreamId::next_transport_internal();
    feed(&mut e, fragment(busy, total, 1)).await;
    feed(&mut e, fragment(quiet, total, 1)).await;

    let step = STREAM_INACTIVITY_TIMEOUT;
    let mut elapsed = Duration::ZERO;
    let mut fragment_number = 1;
    let mut quiet_evicted_at = None;
    while e.conn.streaming_handles.contains_key(&busy) {
        tokio::time::advance(step).await;
        elapsed += step;
        for _ in 0..per_step {
            fragment_number += 1;
            feed(
                &mut e,
                fragment_with(busy, total, fragment_number, full.clone()),
            )
            .await;
        }
        e.conn.sweep_inbound_streams();
        if quiet_evicted_at.is_none() && !e.conn.inbound_streams.contains_key(&quiet) {
            quiet_evicted_at = Some(elapsed);
        }
        assert!(elapsed <= age_limit + 2 * step, "never evicted");
    }
    assert!(
        elapsed > age_limit,
        "progress keeps it until the age limit, evicted at {elapsed:?}"
    );
    let quiet_at = quiet_evicted_at.expect("the idle legacy stream is evicted");
    assert!(quiet_at <= STREAM_INACTIVITY_TIMEOUT * 6 + 2 * step);

    settle().await;
    // Take the evicted legacy task's output (as recv() does; recv() itself
    // would hit the connection idle timeout after this much quiet), which
    // releases its token.
    let (result, token) = e
        .conn
        .inbound_stream_futures
        .next()
        .await
        .expect("the evicted task ended")
        .expect("task did not panic");
    assert_eq!(result.err(), Some(quiet), "ended without a message");
    drop(token);
    e.conn.sweep_inbound_streams();
    assert_eq!(e.conn.inbound_budget.stream_count(), 0);
    assert_eq!(e.conn.inbound_budget.charged_bytes(), 0);
}

/// A large stream arriving slowly but steadily (about 70 KB/s, a slow
/// residential uplink) is not evicted: its age limit scales with its length.
#[tokio::test(start_paused = true)]
async fn a_slow_steady_stream_completes() {
    let mut e = single(CAPABLE);
    let id = StreamId::next();
    let f = super::MAX_DATA_SIZE;
    let total: usize = 50 * 1024 * 1024;
    let fragments = total.div_ceil(f);
    let per_second = 70_000 / f;
    let full = bytes::Bytes::from(vec![7u8; f]);
    let mut consumer = None;
    let mut next = 1usize;
    let mut elapsed = Duration::ZERO;
    while next <= fragments {
        for _ in 0..per_second {
            if next > fragments {
                break;
            }
            let len = if next == fragments {
                total - (fragments - 1) * f
            } else {
                f
            };
            feed(
                &mut e,
                fragment_with(id, total as u64, next as u32, full.slice(..len)),
            )
            .await;
            if consumer.is_none() {
                consumer = e.conn.streaming_registry.get(id);
            }
            next += 1;
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        elapsed += Duration::from_secs(1);
        if elapsed.as_secs() % 5 == 0 {
            e.conn.sweep_inbound_streams();
        }
        if next <= fragments {
            assert!(
                e.conn.streaming_handles.contains_key(&id),
                "evicted after {elapsed:?} at fragment {next} of {fragments}"
            );
        }
    }
    assert!(
        elapsed > Duration::from_secs(700),
        "premise: slower than the old fixed age limit"
    );
    let consumer = consumer.expect("registered");
    assert!(consumer.is_complete());
    assert_eq!(consumer.received_bytes(), total as u64);
}

/// One-byte fragments cannot make a stream look complete: it completes only
/// once its full declared length has arrived.
#[tokio::test(start_paused = true)]
async fn short_fragments_do_not_complete_a_stream() {
    let mut e = single(CAPABLE);
    let id = StreamId::next();
    let total = 3_000u64;
    let max_fragment = total.div_ceil(super::MAX_DATA_SIZE as u64) as u32 + 1;
    for n in 1..=max_fragment {
        feed(
            &mut e,
            fragment_with(id, total, n, bytes::Bytes::from_static(b"x")),
        )
        .await;
    }
    let (handle, _) = e.conn.streaming_handles.get(&id).unwrap();
    assert!(!handle.is_complete());
    assert!(handle.try_assemble().is_none());
    assert_eq!(handle.received_bytes(), u64::from(max_fragment));
}

/// Refusal warnings are rate-limited per connection however many distinct
/// streams are refused.
#[tokio::test(start_paused = true)]
async fn refusals_are_logged_at_a_bounded_rate() {
    use super::streaming::MAX_STREAM_TOTAL_BYTES;
    let mut e = single(CAPABLE);
    for _ in 0..500 {
        feed(
            &mut e,
            fragment(
                StreamId::next_transport_internal(),
                MAX_STREAM_TOTAL_BYTES + 1,
                1,
            ),
        )
        .await;
    }
    assert_eq!(e.conn.refusal_log.suppressed, 499);
    tokio::time::advance(REFUSAL_WARN_INTERVAL).await;
    feed(
        &mut e,
        fragment(
            StreamId::next_transport_internal(),
            MAX_STREAM_TOTAL_BYTES + 1,
            1,
        ),
    )
    .await;
    assert_eq!(
        e.conn.refusal_log.suppressed, 0,
        "warned again, counter reset"
    );
}

/// Once an operations stream has all its bytes, the connection drops its own
/// references, so the stream's charge is released as soon as its consumers
/// let go, without waiting for the idle sweep. Holds in production and under
/// the simulation idle-timeout accommodation alike.
#[tokio::test(start_paused = true)]
async fn a_completed_stream_is_released_when_its_consumers_are() {
    /// Restores production behaviour even if an assertion below panics.
    struct ResetSimulationIdleTimeout;
    impl Drop for ResetSimulationIdleTimeout {
        fn drop(&mut self) {
            crate::config::SimulationIdleTimeout::disable();
        }
    }
    let _reset = ResetSimulationIdleTimeout;
    for simulation in [false, true] {
        if simulation {
            crate::config::SimulationIdleTimeout::enable();
        } else {
            crate::config::SimulationIdleTimeout::disable();
        }
        let mut e = single(CAPABLE);
        let id = StreamId::next();
        let f = super::MAX_DATA_SIZE;
        let total = (2 * f) as u64;
        let full = bytes::Bytes::from(vec![3u8; f]);
        feed(&mut e, fragment_with(id, total, 1, full.clone())).await;
        let consumer = e.conn.streaming_registry.get(id).expect("registered");
        feed(&mut e, fragment_with(id, total, 2, full)).await;

        assert!(consumer.is_complete());
        assert!(
            !e.conn.streaming_handles.contains_key(&id),
            "sim={simulation}"
        );
        assert!(
            e.conn.streaming_registry.get(id).is_none(),
            "sim={simulation}"
        );
        assert_eq!(
            e.conn.inbound_budget.charged_bytes(),
            cost(total),
            "consumer holds it"
        );
        drop(consumer);
        e.conn.sweep_inbound_streams();
        assert_eq!(e.conn.inbound_budget.stream_count(), 0, "sim={simulation}");
    }
}

/// A late retransmission of a stream that completed and was released does
/// not start it again, on either path.
#[tokio::test(start_paused = true)]
async fn a_retransmission_does_not_restart_a_completed_stream() {
    let mut e = single(CAPABLE);
    let one = bytes::Bytes::from_static(b"complete");
    let ops = StreamId::next();
    feed(&mut e, fragment_with(ops, one.len() as u64, 1, one.clone())).await;
    e.conn.sweep_inbound_streams();
    assert_eq!(e.conn.inbound_budget.stream_count(), 0, "released");
    feed(&mut e, fragment_with(ops, one.len() as u64, 1, one.clone())).await;
    assert!(holds_no_state(&e, ops), "not started again");

    let legacy = StreamId::next_transport_internal();
    assert!(
        feed(
            &mut e,
            fragment_with(legacy, one.len() as u64, 1, one.clone())
        )
        .await
        .is_some()
    );
    assert!(
        feed(&mut e, fragment_with(legacy, one.len() as u64, 1, one))
            .await
            .is_none(),
        "not delivered twice"
    );
    assert!(holds_no_state(&e, legacy));
}

/// A stream released without completing (here, evicted when idle) is
/// admitted again if its id reappears.
#[tokio::test(start_paused = true)]
async fn an_evicted_stream_id_can_be_admitted_again() {
    let mut e = single(CAPABLE);
    let id = StreamId::next();
    feed(&mut e, fragment(id, 50_000, 1)).await;
    tokio::time::advance(super::streaming_handle_idle_timeout() * 2).await;
    e.conn.sweep_inbound_streams();
    e.conn.sweep_inbound_streams();
    assert_eq!(
        e.conn.inbound_budget.stream_count(),
        0,
        "evicted and released"
    );

    feed(&mut e, fragment(id, 50_000, 1)).await;
    assert!(e.conn.streaming_handles.contains_key(&id));
    assert_eq!(e.conn.inbound_budget.charged_bytes(), cost(50_000));
}

/// An outbound stream refused for its length does not end the connection:
/// `recv()` observes the stream's result and carries on.
#[tokio::test(start_paused = true)]
async fn an_outbound_stream_refused_for_length_keeps_the_connection() {
    use super::streaming::MAX_STREAM_TOTAL_BYTES;
    let mut e = single(CAPABLE);
    e.conn
        .send(vec![0u8; MAX_STREAM_TOTAL_BYTES as usize])
        .await
        .expect("send hands the stream off");
    assert_eq!(
        e.conn.outbound_stream_futures.len(),
        1,
        "premise: a stream task"
    );
    // `pump` panics if `recv()` returns an error.
    assert!(e.pump(Duration::from_millis(200)).await.is_none());
    assert!(
        e.conn.outbound_stream_futures.is_empty(),
        "recv() observed the refused stream's result"
    );
    // The connection still works.
    e.deliver(1, short(b"after"), vec![]).await;
    assert_eq!(
        e.pump(Duration::from_millis(200)).await.as_deref(),
        Some(&b"after"[..])
    );
}

/// Senders before the empty-fragment fix (still allowed to connect) left
/// fragment #1 empty when its embedded metadata used the whole packet, and
/// sent the payload from fragment #2 on. Replays that fragmenting: the
/// metadata is dispatched and the stream completes intact.
#[tokio::test(start_paused = true)]
async fn a_stream_from_an_older_sender_with_an_empty_first_fragment_completes() {
    let mut e = single(CAPABLE);
    let id = StreamId::next();
    let f = super::MAX_DATA_SIZE;
    let data: Vec<u8> = (0..(3 * f + 100)).map(|i| i as u8).collect();
    let total = data.len() as u64;
    // The old sender: capacity of fragment #1 = MAX_DATA_SIZE - (1 + 8 + meta)
    // saturating to 0, then `1 + ceil(len / MAX_DATA_SIZE)` fragments.
    let metadata = bytes::Bytes::from(vec![0xAB; f - 9]);
    let first = SymmetricMessagePayload::StreamFragment {
        stream_id: id,
        total_length_bytes: total,
        fragment_number: 1,
        payload: bytes::Bytes::new(),
        metadata_bytes: Some(metadata.clone()),
    };
    assert_eq!(
        feed(&mut e, first).await.as_deref(),
        Some(&metadata[..]),
        "the embedded metadata is dispatched"
    );
    let consumer = e.conn.streaming_registry.get(id).expect("admitted");
    for (i, chunk) in data.chunks(f).enumerate() {
        feed(
            &mut e,
            fragment_with(
                id,
                total,
                i as u32 + 2,
                bytes::Bytes::copy_from_slice(chunk),
            ),
        )
        .await;
    }
    assert!(consumer.is_complete());
    assert_eq!(consumer.try_assemble().expect("complete"), data);
}

/// Refusals held back by the warning rate limit are reported on the next
/// sweep after the interval even if no further refusal arrives.
#[tokio::test(start_paused = true)]
async fn suppressed_refusals_are_flushed_without_a_further_refusal() {
    use super::streaming::MAX_STREAM_TOTAL_BYTES;
    let mut e = single(CAPABLE);
    for _ in 0..3 {
        feed(
            &mut e,
            fragment(
                StreamId::next_transport_internal(),
                MAX_STREAM_TOTAL_BYTES + 1,
                1,
            ),
        )
        .await;
    }
    assert_eq!(e.conn.refusal_log.suppressed, 2);
    e.conn.sweep_inbound_streams();
    assert_eq!(e.conn.refusal_log.suppressed, 2, "not due yet");
    tokio::time::advance(REFUSAL_WARN_INTERVAL).await;
    e.conn.sweep_inbound_streams();
    assert_eq!(e.conn.refusal_log.suppressed, 0, "flushed by the sweep");
}

/// Each refusal reason has its own counter, incremented exactly once per
/// refused stream (per-connection counts, which no other test can touch):
/// later fragments of a refused stream are dropped as previously refused,
/// not counted again, whatever they declare.
#[tokio::test(start_paused = true)]
async fn refusals_are_counted_once_per_reason() {
    use super::streaming::{MAX_INBOUND_STREAMS_PER_CONNECTION, MAX_STREAM_TOTAL_BYTES};
    use crate::transport::metrics::InboundStreamOutcome as O;
    let mut e = single(CAPABLE);
    let too_long = StreamId::next_transport_internal();
    feed(&mut e, fragment(too_long, MAX_STREAM_TOTAL_BYTES + 1, 1)).await;
    feed(&mut e, fragment(StreamId::next_transport_internal(), 0, 1)).await;
    feed(
        &mut e,
        fragment(StreamId::next(), MAX_STREAM_TOTAL_BYTES, 1),
    )
    .await;
    let over_bytes = StreamId::next();
    feed(&mut e, fragment(over_bytes, MAX_STREAM_TOTAL_BYTES, 1)).await;
    let mut f = single(CAPABLE);
    for _ in 0..MAX_INBOUND_STREAMS_PER_CONNECTION {
        feed(&mut f, fragment(StreamId::next(), 1_000, 1)).await;
    }
    let over_count = StreamId::next();
    feed(&mut f, fragment(over_count, 1_000, 1)).await;

    // Further fragments of each refused stream, now declaring a valid length.
    for (end, id) in [(0, too_long), (0, over_bytes), (1, over_count)] {
        let end = if end == 0 { &mut e } else { &mut f };
        assert_eq!(
            end.conn.admit_inbound_stream(id, 1_000),
            Err(InboundStreamRejection::PreviouslyRefused)
        );
        feed(end, fragment(id, 1_000, 2)).await;
        assert!(holds_no_state_but_others(end, id));
    }

    let mut expected = [0u64; O::COUNT];
    expected[O::RefusedInvalidLength as usize] = 2;
    expected[O::RefusedConnectionBytes as usize] = 1;
    assert_eq!(e.conn.inbound_outcomes, expected);
    let mut expected = [0u64; O::COUNT];
    expected[O::RefusedConnectionStreams as usize] = 1;
    assert_eq!(f.conn.inbound_outcomes, expected);
}

/// A stream is charged an overhead for every fragment its declared length
/// allows, so filling each of them with a one-byte payload (the most real
/// memory per payload byte) stays within what was charged.
#[tokio::test(start_paused = true)]
async fn every_fragment_a_stream_may_hold_is_charged() {
    use super::inbound_budget::ChargeOverhead;
    let mut e = single(CAPABLE);
    let id = StreamId::next();
    let f = super::MAX_DATA_SIZE as u64;
    let total = 100 * f;
    let fragments = total.div_ceil(f) + 1;
    let overhead = ChargeOverhead::DEFAULT;
    assert!(overhead.per_fragment > 0, "premise");
    for n in 1..=fragments as u32 {
        feed(
            &mut e,
            fragment_with(id, total, n, bytes::Bytes::from_static(b"x")),
        )
        .await;
    }
    let (handle, _) = e.conn.streaming_handles.get(&id).expect("admitted");
    assert_eq!(handle.received_fragments() as u64, fragments);
    let charged = e.conn.inbound_budget.charged_bytes();
    assert_eq!(charged, cost(total));
    assert!(
        charged >= total + overhead.per_stream + fragments * overhead.per_fragment,
        "{charged}"
    );
}

/// The completed-stream memory is bounded: after more completions than it
/// holds, the oldest id is forgotten (and may start a stream again) while the
/// newest is still refused.
#[tokio::test(start_paused = true)]
async fn the_completed_stream_memory_is_bounded() {
    let mut e = single(CAPABLE);
    let one = bytes::Bytes::from_static(b"complete");
    let len = one.len() as u64;
    let ids: Vec<StreamId> = (0..COMPLETED_STREAM_MEMORY.get() + 1)
        .map(|_| StreamId::next())
        .collect();
    for id in &ids {
        feed(&mut e, fragment_with(*id, len, 1, one.clone())).await;
    }
    e.conn.sweep_inbound_streams();
    assert_eq!(e.conn.inbound_budget.stream_count(), 0, "all released");
    let (oldest, newest) = (ids[0], *ids.last().unwrap());
    assert_eq!(e.conn.admit_inbound_stream(oldest, len), Ok(()));
    assert_eq!(
        e.conn.admit_inbound_stream(newest, len),
        Err(InboundStreamRejection::AlreadyCompleted)
    );
    // The second id is still remembered: only one was evicted.
    assert_eq!(
        e.conn.admit_inbound_stream(ids[1], len),
        Err(InboundStreamRejection::AlreadyCompleted)
    );
}

/// A legacy stream reassembled by its task is remembered once `recv()` takes
/// its message, so a late retransmission does not start it again.
#[tokio::test(start_paused = true)]
async fn a_legacy_stream_completed_by_its_task_is_remembered() {
    let mut e = single(CAPABLE);
    let id = StreamId::next_transport_internal();
    let f = super::MAX_DATA_SIZE;
    let data: Vec<u8> = (0..(2 * f)).map(|i| i as u8).collect();
    let total = data.len() as u64;
    for (i, chunk) in data.chunks(f).enumerate() {
        feed(
            &mut e,
            fragment_with(
                id,
                total,
                i as u32 + 1,
                bytes::Bytes::copy_from_slice(chunk),
            ),
        )
        .await;
    }
    assert_eq!(
        e.pump(Duration::from_millis(10)).await.as_deref(),
        Some(&data[..])
    );
    assert!(holds_no_state(&e, id));
    assert_eq!(
        e.conn.admit_inbound_stream(id, total),
        Err(InboundStreamRejection::AlreadyCompleted)
    );
    feed(
        &mut e,
        fragment_with(id, total, 1, bytes::Bytes::copy_from_slice(&data[..f])),
    )
    .await;
    assert!(holds_no_state(&e, id), "not started again");
}

/// An evicted legacy stream delivers nothing even when the fragments that
/// would complete it are already queued for its reassembly task.
#[tokio::test(start_paused = true)]
async fn an_evicted_legacy_stream_with_queued_fragments_delivers_nothing() {
    use super::streaming::max_inbound_stream_age;
    use crate::transport::metrics::InboundStreamOutcome as O;
    let mut e = single(CAPABLE);
    let id = StreamId::next_transport_internal();
    let f = super::MAX_DATA_SIZE;
    let total = (3 * f) as u64;
    let full = bytes::Bytes::from(vec![4u8; f]);
    feed(&mut e, fragment_with(id, total, 1, full.clone())).await;
    settle().await;
    // Past the age limit, then queue the rest without letting the task run
    // (so they are not yet progress either).
    tokio::time::advance(max_inbound_stream_age(total) + Duration::from_secs(1)).await;
    for n in 2..=3 {
        feed(&mut e, fragment_with(id, total, n, full.clone())).await;
    }
    e.conn.sweep_inbound_streams();
    assert_eq!(
        e.conn.inbound_outcomes[O::EvictedIdle as usize],
        1,
        "premise"
    );
    settle().await;
    let (result, token) = e
        .conn
        .inbound_stream_futures
        .next()
        .await
        .expect("the evicted task ended")
        .expect("task did not panic");
    assert_eq!(result.err(), Some(id), "ended without a message");
    drop(token);
    e.conn.sweep_inbound_streams();
    assert_eq!(e.conn.inbound_budget.stream_count(), 0);
}

/// Refusals stay sticky however many there are: after many more refused
/// streams than the old 64-entry memory held, a later fragment of the first
/// refused stream still creates no state.
#[tokio::test(start_paused = true)]
async fn refusals_stay_sticky_beyond_many_refusals() {
    use super::streaming::MAX_STREAM_TOTAL_BYTES;
    for first in [StreamId::next_transport_internal(), StreamId::next()] {
        let mut e = single(CAPABLE);
        feed(&mut e, fragment(first, MAX_STREAM_TOTAL_BYTES + 1, 1)).await;
        // `first` is the oldest refusal: the first one a forgetting memory
        // would drop.
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..1_000 {
            feed(
                &mut e,
                fragment(
                    StreamId::next_transport_internal(),
                    MAX_STREAM_TOTAL_BYTES + 1,
                    1,
                ),
            )
            .await;
        }
        assert_eq!(
            e.conn.admit_inbound_stream(first, 5_000),
            Err(InboundStreamRejection::PreviouslyRefused)
        );
        feed(&mut e, fragment(first, 5_000, 2)).await;
        assert!(holds_no_state(&e, first));
    }
}

/// A refusal that cannot be remembered (the connection already has as many
/// refused streams outstanding as are remembered) closes the connection,
/// counted and labelled, rather than letting that stream in part-way later.
#[tokio::test(start_paused = true)]
async fn a_refusal_that_cannot_be_remembered_closes_the_connection() {
    use super::inbound_budget::MAX_REMEMBERED_REFUSALS;
    use crate::transport::metrics::InboundStreamOutcome as O;
    let mut e = single(CAPABLE);
    for _ in 0..MAX_REMEMBERED_REFUSALS {
        feed(&mut e, fragment(StreamId::next_transport_internal(), 0, 1)).await;
    }
    // Ordinary streams are still admitted.
    let ok = StreamId::next();
    feed(&mut e, fragment(ok, 10_000, 1)).await;
    assert!(e.conn.streaming_handles.contains_key(&ok));
    assert_eq!(
        crate::transport::PeerConnectionApi::close_reason(&e.conn),
        None
    );

    let result = e
        .conn
        .process_inbound(fragment(StreamId::next_transport_internal(), 0, 1), 0)
        .await;
    assert!(
        matches!(result, Err(TransportError::ConnectionClosed(_))),
        "{result:?}"
    );
    assert_eq!(
        crate::transport::PeerConnectionApi::close_reason(&e.conn),
        Some(crate::transport::CloseReason::InboundStreamRefusals)
    );
    assert_eq!(
        e.conn.inbound_outcomes[O::ClosedTooManyRefusals as usize],
        1
    );
}

/// The periodic sweep counts an idle operations stream's eviction even
/// though the idle-handle sweep would also drop it.
#[tokio::test(start_paused = true)]
async fn the_periodic_sweep_counts_an_idle_eviction() {
    use crate::transport::metrics::InboundStreamOutcome as O;
    let mut e = single(CAPABLE);
    let id = StreamId::next();
    feed(&mut e, fragment(id, 50_000, 1)).await;
    tokio::time::advance(super::streaming_handle_idle_timeout() + Duration::from_secs(1)).await;
    e.conn.sweep_streams();
    assert_eq!(e.conn.inbound_outcomes[O::EvictedIdle as usize], 1);
    assert_eq!(e.conn.inbound_budget.stream_count(), 0, "released");
}

/// When the connection goes, its unclaimed streams are cancelled in the
/// orphan registry, complete (already retired) ones included, so GC drops
/// them rather than keeping them for the orphan timeout uncharged.
#[tokio::test(start_paused = true)]
async fn unclaimed_streams_go_with_the_connection() {
    let registry = Arc::new(crate::operations::orphan_streams::OrphanStreamRegistry::new());
    let mut e = single(CAPABLE);
    e.conn.set_inbound_stream_registry(registry.clone());
    let one = bytes::Bytes::from_static(b"complete");
    let done = StreamId::next();
    feed(&mut e, fragment_with(done, one.len() as u64, 1, one)).await;
    assert!(
        !e.conn.streaming_handles.contains_key(&done),
        "premise: retired"
    );
    feed(&mut e, fragment(StreamId::next(), 50_000, 1)).await;
    assert_eq!(registry.orphan_count(), 2, "premise: both unclaimed");
    drop(e);
    registry.gc_expired();
    assert_eq!(registry.orphan_count(), 0);
}

/// A stream whose bytes all arrive except fragment #1 (using the extra
/// fragment number) holds its full length but is not complete, so it is
/// still evicted, on either path.
#[tokio::test(start_paused = true)]
async fn a_stream_missing_its_first_fragment_is_evicted() {
    use crate::transport::metrics::InboundStreamOutcome as O;
    for id in [StreamId::next_transport_internal(), StreamId::next()] {
        let mut e = single(CAPABLE);
        let f = super::MAX_DATA_SIZE;
        let total = (4 * f) as u64;
        let full = bytes::Bytes::from(vec![7u8; f]);
        for n in 2..=5 {
            feed(&mut e, fragment_with(id, total, n, full.clone())).await;
        }
        settle().await;
        let step = Duration::from_secs(5);
        let mut elapsed = Duration::ZERO;
        while e.conn.inbound_budget.stream_count() > 0
            && e.conn.inbound_outcomes[O::EvictedIdle as usize] == 0
        {
            tokio::time::advance(step).await;
            elapsed += step;
            e.conn.sweep_inbound_streams();
            assert!(
                elapsed <= super::streaming_handle_idle_timeout() + 2 * step,
                "{id:?} never evicted"
            );
        }
        assert_eq!(
            e.conn.inbound_outcomes[O::EvictedIdle as usize],
            1,
            "{id:?}"
        );
    }
}

/// A legacy stream receiving a new fragment within every idle timeout stays
/// alive: the sweep counts the bytes its task reports as progress.
#[tokio::test(start_paused = true)]
async fn a_legacy_stream_receiving_new_fragments_stays_alive() {
    let mut e = single(CAPABLE);
    let id = StreamId::next_transport_internal();
    let f = super::MAX_DATA_SIZE;
    let total = (40 * f) as u64;
    let full = bytes::Bytes::from(vec![6u8; f]);
    feed(&mut e, fragment_with(id, total, 1, full.clone())).await;
    let step = Duration::from_secs(5);
    let mut n = 1;
    for s in 1..=36u64 {
        tokio::time::advance(step).await;
        if s % 4 == 0 {
            n += 1;
            feed(&mut e, fragment_with(id, total, n, full.clone())).await;
            settle().await;
        }
        e.conn.sweep_inbound_streams();
        assert!(
            e.conn.inbound_streams.contains_key(&id),
            "evicted after {} s while progressing",
            s * 5
        );
    }
}

/// Replays do not keep a legacy stream alive: one that receives only
/// already-held fragments (at or below its frontier, or a pending one
/// again) is evicted when idle.
#[tokio::test(start_paused = true)]
async fn a_legacy_stream_receiving_only_replays_goes_idle() {
    use crate::transport::metrics::InboundStreamOutcome as O;
    let mut e = single(CAPABLE);
    let id = StreamId::next_transport_internal();
    let f = super::MAX_DATA_SIZE;
    let total = (20 * f) as u64;
    let full = bytes::Bytes::from(vec![3u8; f]);
    for n in (1..=10).chain([15]) {
        feed(&mut e, fragment_with(id, total, n, full.clone())).await;
    }
    settle().await;
    e.conn.sweep_inbound_streams();
    let step = Duration::from_secs(5);
    let mut elapsed = Duration::ZERO;
    while e.conn.inbound_streams.contains_key(&id) {
        tokio::time::advance(step).await;
        elapsed += step;
        for n in [5, 15] {
            feed(&mut e, fragment_with(id, total, n, full.clone())).await;
        }
        settle().await;
        e.conn.sweep_inbound_streams();
        assert!(
            elapsed <= super::streaming_handle_idle_timeout() + 2 * step,
            "kept alive by replays for {elapsed:?}"
        );
    }
    assert_eq!(e.conn.inbound_outcomes[O::EvictedIdle as usize], 1);
}

/// A legacy stream delivered out of order, with its gap at the very end,
/// completes intact and is charged at least what it held.
#[tokio::test(start_paused = true)]
async fn an_out_of_order_legacy_stream_completes_within_its_charge() {
    let mut e = single(CAPABLE);
    let id = StreamId::next_transport_internal();
    let f = super::MAX_DATA_SIZE;
    let n = 60usize;
    let data: Vec<u8> = (0..n * f).map(|i| (i % 251) as u8).collect();
    let total = data.len() as u64;
    let chunk = |i: usize| bytes::Bytes::copy_from_slice(&data[(i - 1) * f..i * f]);
    // In order to past half, the rest backwards, the gap last.
    let gap = n / 2 + 2;
    let order: Vec<usize> = (1..gap).chain((gap + 1..=n).rev()).chain([gap]).collect();
    for &i in &order[..order.len() - 1] {
        feed(&mut e, fragment_with(id, total, i as u32, chunk(i))).await;
        settle().await;
    }
    assert!(e.conn.inbound_budget.charged_bytes() >= cost(total));
    assert!(cost(total) >= total + (n as u64 + 1) * super::inbound_budget::PER_FRAGMENT_OVERHEAD);
    feed(&mut e, fragment_with(id, total, gap as u32, chunk(gap))).await;
    assert_eq!(
        e.pump(Duration::from_millis(10)).await.as_deref(),
        Some(&data[..])
    );
}

/// A packet that decrypts under the session key but whose plaintext does not
/// decode as a `SymmetricMessage` (for example a payload variant from a newer
/// release) is dropped. `recv()` keeps running, delivers the next valid
/// message, and does not count the dropped packet as a decryption failure.
#[tokio::test(start_paused = true)]
async fn undecodable_authenticated_packet_is_dropped_not_fatal() {
    let mut e = single(CAPABLE);
    // Three bytes cannot hold the leading `u32` packet id, so this fails to
    // decode whatever the payload layout is. A payload variant from a newer
    // release fails later in decoding but takes the same error arm.
    let undecodable =
        PacketData::<_, { crate::transport::packet_data::MAX_PACKET_SIZE }>::from_buf_plain([
            0xFF, 0xFF, 0xFF,
        ])
        .encrypt_symmetric(&e.inbound_key);
    assert!(SymmetricMessage::deser(&[0xFF, 0xFF, 0xFF]).is_err());
    e.inbound
        .send(e.queued(PacketData::<UnknownEncryption>::from_buf(
            undecodable.data(),
        )))
        .await
        .expect("deliver");
    e.deliver(0, short(b"after"), vec![]).await;

    assert_eq!(
        e.pump(Duration::from_millis(10)).await.as_deref(),
        Some(&b"after"[..])
    );
    assert_eq!(e.conn.failure_count, 0);
    assert_eq!(e.conn.undecodable_count, 1);
}

// ---------------------------------------------------------------------------
// QUIC upgrade control message (#5940 e1): decoded, counted, ignored
// ---------------------------------------------------------------------------

/// Anything this end sent other than receipt carriers and keepalive probes.
fn replies(sent: &[SymmetricMessage]) -> Vec<&SymmetricMessage> {
    sent.iter()
        .filter(|m| {
            !matches!(
                m.payload,
                SymmetricMessagePayload::NoOp | SymmetricMessagePayload::Ping { .. }
            )
        })
        .collect()
}

/// Every form of the upgrade message, received on an established connection,
/// is counted and ignored: nothing is delivered to the application, nothing
/// but a receipt is sent back, and the connection carries on and delivers the
/// next message. This holds whatever version the remote reports, because the
/// receiver never answers.
#[tokio::test(start_paused = true)]
async fn upgrade_control_is_counted_and_ignored_without_reply() {
    for version in [CAPABLE, OLD, None] {
        let mut e = single(version);
        // Never reset, so another test's `take_snapshot` cannot disturb it;
        // other tests may only add to it, hence `>=`.
        let cumulative_before =
            crate::transport::TRANSPORT_METRICS.cumulative_upgrade_control_received();
        let controls = [
            UpgradeControl::Offer {
                attempt: 1,
                caps: u32::MAX,
            },
            UpgradeControl::Accept { attempt: 1 },
            UpgradeControl::Decline {
                attempt: 1,
                reason: u8::MAX,
            },
        ];
        for (id, control) in (1u32..).zip(controls) {
            e.deliver(id, upgrade(control), vec![]).await;
        }
        e.deliver(4, short(b"after"), vec![]).await;
        assert_eq!(
            e.pump(Duration::from_secs(1)).await.as_deref(),
            Some(&b"after"[..]),
            "{version:?}: the upgrade messages are not delivered and the next message is"
        );
        assert_eq!(e.conn.telemetry_test_counts.upgrade_controls_ignored, 3);
        assert!(
            crate::transport::TRANSPORT_METRICS.cumulative_upgrade_control_received()
                - cumulative_before
                >= 3,
            "{version:?}: each upgrade message must reach TRANSPORT_METRICS"
        );
        assert_eq!(e.conn.failure_count, 0);
        assert_eq!(e.conn.undecodable_count, 0);

        assert!(e.pump(Duration::from_secs(2)).await.is_none());
        let sent = e.drain_sent();
        assert!(
            replies(&sent).is_empty(),
            "{version:?}: the receiver must never answer an upgrade message, got {:?}",
            replies(&sent)
                .iter()
                .map(|m| m.payload.to_string())
                .collect::<Vec<_>>()
        );
        for id in 1..=3 {
            assert_eq!(
                receipts_for(&sent, id),
                1,
                "{version:?}: upgrade packet {id} is acked exactly once"
            );
        }
    }
}

/// A retransmitted upgrade message (our receipt was lost) is re-acked, as a
/// `ShortMessage` is, and not processed a second time.
#[tokio::test(start_paused = true)]
async fn duplicate_upgrade_control_is_reacked_and_not_recounted() {
    let mut e = single(CAPABLE);
    let offer = UpgradeControl::Offer {
        attempt: 7,
        caps: 0,
    };
    e.deliver(5, upgrade(offer), vec![]).await;
    assert!(e.pump(Duration::from_millis(300)).await.is_none());
    assert_eq!(receipts_for(&e.drain_sent(), 5), 1, "first ack");
    assert_eq!(e.conn.telemetry_test_counts.upgrade_controls_ignored, 1);

    tokio::time::advance(Duration::from_millis(700)).await;
    e.deliver(5, upgrade(offer), vec![]).await;
    e.deliver(6, short(b"next"), vec![]).await;
    assert_eq!(
        e.pump(Duration::from_secs(1)).await.as_deref(),
        Some(&b"next"[..])
    );
    assert!(e.pump(Duration::from_millis(300)).await.is_none());
    let sent = e.drain_sent();
    assert_eq!(
        receipts_for(&sent, 5),
        1,
        "the duplicate must be re-acked so the sender stops retransmitting"
    );
    assert!(replies(&sent).is_empty());
    assert_eq!(
        e.conn.telemetry_test_counts.upgrade_controls_ignored, 1,
        "a duplicate is not processed again"
    );
}

/// A newer peer's upgrade message with an inner variant this release does not
/// know fails to decode and is dropped like any undecodable packet: no panic,
/// no reply, not counted as an upgrade message, and the connection delivers
/// the next message.
#[tokio::test(start_paused = true)]
async fn upgrade_control_with_unknown_inner_variant_is_dropped() {
    const INNER_VARIANT_OFFSET: usize = 4 + 8 + 4;
    let mut e = single(CAPABLE);
    let mut bytes = bincode::serialize(&SymmetricMessage {
        packet_id: 3,
        confirm_receipt: vec![],
        payload: upgrade(UpgradeControl::Accept { attempt: 1 }),
    })
    .unwrap();
    bytes[INNER_VARIANT_OFFSET..INNER_VARIANT_OFFSET + 4].copy_from_slice(&3u32.to_le_bytes());
    assert!(SymmetricMessage::deser(&bytes).is_err());
    let packet =
        PacketData::<_, { crate::transport::packet_data::MAX_PACKET_SIZE }>::from_buf_plain(&bytes)
            .encrypt_symmetric(&e.inbound_key);
    e.inbound
        .send(e.queued(PacketData::<UnknownEncryption>::from_buf(packet.data())))
        .await
        .expect("deliver");
    e.deliver(4, short(b"after"), vec![]).await;

    assert_eq!(
        e.pump(Duration::from_millis(10)).await.as_deref(),
        Some(&b"after"[..])
    );
    assert_eq!(e.conn.undecodable_count, 1);
    assert_eq!(e.conn.failure_count, 0);
    assert_eq!(e.conn.telemetry_test_counts.upgrade_controls_ignored, 0);
    assert!(e.pump(Duration::from_millis(300)).await.is_none());
    let sent = e.drain_sent();
    assert!(replies(&sent).is_empty());
    assert_eq!(
        receipts_for(&sent, 3),
        0,
        "an undecodable packet earns no receipt"
    );
}

// ---------------------------------------------------------------------------
// Inbound concurrency estimate (telemetry only)
// ---------------------------------------------------------------------------

/// Short messages received through `recv()` drive the estimate: each
/// distinct one is one sample counting the short messages still inside the
/// window, a duplicate is not sampled again, and one arriving after the
/// window has passed counts alone.
#[tokio::test(start_paused = true)]
async fn received_short_messages_drive_the_inbound_concurrency_estimate() {
    let mut e = single(CAPABLE);
    assert_eq!(e.conn.remote_conn.rolling_rtt_stats.srtt(), None);
    let messages: [(u32, &'static [u8]); 3] = [(1, b"a"), (2, b"b"), (3, b"c")];
    for (id, msg) in messages {
        e.deliver(id, short(msg), vec![]).await;
        assert_eq!(e.pump(Duration::from_millis(1)).await.as_deref(), Some(msg));
    }
    // A retransmission of message 2 is re-acked, not delivered or sampled.
    e.deliver(2, short(b"b"), vec![]).await;
    assert!(e.pump(Duration::from_millis(1)).await.is_none());
    let counts = &e.conn.telemetry_test_counts;
    assert_eq!(counts.inbound_concurrency_samples, 3);
    assert_eq!(
        counts.inbound_concurrency_max, 3,
        "three short messages inside one window"
    );

    // Past the no-RTT window (100 ms + 25 ms): only the new one counts.
    tokio::time::advance(Duration::from_secs(1)).await;
    e.deliver(4, short(b"d"), vec![]).await;
    assert_eq!(
        e.pump(Duration::from_millis(1)).await.as_deref(),
        Some(&b"d"[..])
    );
    assert_eq!(e.conn.telemetry_test_counts.inbound_concurrency_samples, 4);
    assert_eq!(
        e.conn
            .inbound_concurrency
            .on_stream_admitted(e.conn.time_source.now_nanos(), None, 0),
        1,
        "the earlier three have left the window"
    );
}

/// A new inbound stream is sampled once, on admission, counting itself; its
/// later fragments are not sampled. Both reassembly paths.
#[tokio::test(start_paused = true)]
async fn an_admitted_inbound_stream_is_sampled_once() {
    let length = 3 * super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE as u64;
    for stream_id in [StreamId::next(), StreamId::next_transport_internal()] {
        let mut e = single(CAPABLE);
        feed(&mut e, short(b"m")).await;
        feed(&mut e, fragment(stream_id, length, 1)).await;
        let counts = &e.conn.telemetry_test_counts;
        assert_eq!(counts.inbound_concurrency_samples, 2, "{stream_id:?}");
        assert_eq!(
            counts.inbound_concurrency_max, 2,
            "{stream_id:?}: the open stream plus the short message"
        );
        feed(&mut e, fragment(stream_id, length, 2)).await;
        settle().await;
        assert_eq!(
            e.conn.telemetry_test_counts.inbound_concurrency_samples, 2,
            "{stream_id:?}: a later fragment is not sampled"
        );
    }
}

/// Point `e`'s inbound concurrency samples at a private metrics instance,
/// so other tests snapshotting `TRANSPORT_METRICS` cannot disturb them.
fn private_concurrency_metrics(
    e: &mut End,
) -> &'static crate::transport::metrics::TransportMetrics {
    let metrics = Box::leak(Box::new(crate::transport::metrics::TransportMetrics::new()));
    e.conn.inbound_concurrency_metrics = metrics;
    metrics
}

/// The connection's samples reach `TransportMetrics`: its peak is the
/// period's maximum and is counted at each threshold it reached, once per
/// period.
#[tokio::test(start_paused = true)]
async fn inbound_concurrency_peak_reaches_transport_metrics() {
    let mut e = single(CAPABLE);
    let metrics = private_concurrency_metrics(&mut e);
    for id in 1..=9u32 {
        e.deliver(id, short(b"s"), vec![]).await;
        assert!(e.pump(Duration::from_millis(1)).await.is_some());
    }
    let snap = metrics.take_snapshot().expect("samples are activity");
    assert_eq!(snap.inbound_concurrency_conns_sampled, 1);
    assert_eq!(snap.inbound_concurrency_max, 9);
    assert_eq!(snap.inbound_concurrency_conns_ge_8, 1);
    assert_eq!(snap.inbound_concurrency_conns_ge_16, 0);

    // The next period counts the connection again, from its own estimate.
    tokio::time::advance(Duration::from_secs(1)).await;
    e.deliver(10, short(b"t"), vec![]).await;
    assert!(e.pump(Duration::from_millis(1)).await.is_some());
    let snap = metrics.take_snapshot().expect("samples are activity");
    assert_eq!(snap.inbound_concurrency_conns_sampled, 1);
    assert_eq!(snap.inbound_concurrency_max, 1);
    assert_eq!(snap.inbound_concurrency_conns_ge_8, 0);
}

/// With a smoothed RTT published, the window is that RTT + 25 ms, not the
/// 125 ms fallback: two short messages 40 ms apart overlap without an RTT
/// and do not overlap at a 10 ms RTT.
#[tokio::test(start_paused = true)]
async fn a_published_srtt_sets_the_inbound_concurrency_window() {
    for (srtt, expected) in [(None, 2), (Some(Duration::from_millis(10)), 1)] {
        let mut e = single(CAPABLE);
        let metrics = private_concurrency_metrics(&mut e);
        if let Some(srtt) = srtt {
            e.conn.remote_conn.rolling_rtt_stats.publish_srtt(srtt);
        }
        assert_eq!(e.conn.remote_conn.rolling_rtt_stats.srtt(), srtt);
        e.deliver(1, short(b"a"), vec![]).await;
        assert!(e.pump(Duration::from_millis(1)).await.is_some());
        tokio::time::advance(Duration::from_millis(40)).await;
        e.deliver(2, short(b"b"), vec![]).await;
        assert!(e.pump(Duration::from_millis(1)).await.is_some());
        assert_eq!(
            metrics.read_snapshot().inbound_concurrency_max,
            expected,
            "srtt {srtt:?}"
        );
    }
}

/// A legacy stream that arrives whole in one fragment is never charged and
/// is counted as a short message: it stays in the window afterwards, and
/// holds no open stream.
#[tokio::test(start_paused = true)]
async fn a_single_fragment_legacy_stream_is_sampled_as_a_short_message() {
    let mut e = single(CAPABLE);
    let stream_id = StreamId::next_transport_internal();
    let delivered = feed(&mut e, fragment(stream_id, b"fragment".len() as u64, 1)).await;
    assert!(
        delivered.is_some(),
        "a one-fragment stream completes at once"
    );
    assert_eq!(e.conn.inbound_budget.stream_count(), 0);
    assert_eq!(e.conn.telemetry_test_counts.inbound_concurrency_samples, 1);
    assert_eq!(e.conn.telemetry_test_counts.inbound_concurrency_max, 1);
    assert_eq!(
        e.conn
            .inbound_concurrency
            .on_stream_admitted(e.conn.time_source.now_nanos(), None, 0),
        1,
        "it is in the short-message window"
    );
}

/// A stream refused at admission takes no sample, on either path.
#[tokio::test(start_paused = true)]
async fn a_refused_inbound_stream_is_not_sampled() {
    for stream_id in [StreamId::next(), StreamId::next_transport_internal()] {
        let mut e = single(CAPABLE);
        assert!(feed(&mut e, fragment(stream_id, 0, 1)).await.is_none());
        assert!(holds_no_state(&e, stream_id));
        assert_eq!(
            e.conn.telemetry_test_counts.inbound_concurrency_samples, 0,
            "{stream_id:?}"
        );
    }
}

/// Short messages are timed from when the listener queued them, not from
/// when the connection task got to them. The first arrives at 0 and is
/// taken at once; the second and third arrive at 50 and 100 ms while the
/// task is busy and are taken a second apart. By arrival all three fall in
/// one no-RTT window (125 ms), so the third counts 3; timed by when they
/// were taken, no sample would reach more than 2 (one short plus one still
/// queued).
#[tokio::test(start_paused = true)]
async fn queued_short_messages_count_from_their_arrival() {
    let mut e = single(CAPABLE);
    let metrics = private_concurrency_metrics(&mut e);
    e.deliver(1, short(b"q"), vec![]).await;
    assert!(e.pump(Duration::from_millis(1)).await.is_some());
    for id in 2..=3u32 {
        tokio::time::advance(Duration::from_millis(50)).await;
        e.deliver(id, short(b"q"), vec![]).await;
    }
    for _ in 0..2 {
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(e.pump(Duration::from_millis(1)).await.is_some());
    }
    assert_eq!(e.conn.telemetry_test_counts.inbound_concurrency_samples, 3);
    assert_eq!(
        metrics.read_snapshot().inbound_concurrency_max,
        3,
        "the three arrived within one window"
    );
}

/// Packets still queued for the connection count: five short messages
/// queued while the task is busy, and the first one taken already counts
/// all five (itself plus four waiting).
#[tokio::test(start_paused = true)]
async fn queued_packets_count_toward_the_estimate() {
    let mut e = single(CAPABLE);
    let metrics = private_concurrency_metrics(&mut e);
    for id in 1..=5u32 {
        e.deliver(id, short(b"w"), vec![]).await;
    }
    assert!(e.pump(Duration::from_millis(1)).await.is_some());
    assert_eq!(e.conn.telemetry_test_counts.inbound_concurrency_samples, 1);
    assert_eq!(
        e.conn.telemetry_test_counts.inbound_concurrency_max, 5,
        "one short message plus four queued packets"
    );
    assert_eq!(metrics.read_snapshot().inbound_concurrency_max, 5);
}
