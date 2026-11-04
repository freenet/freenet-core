//! Retransmission storm on a shared, saturated uplink (#6100).
//!
//! One host streams to several peers at once through an uplink far below the
//! sum of its connections' paced rates (FixedRate paces each connection at
//! 10 Mbit/s and has no node-wide cap by default). The drop-tail queue in
//! front of the uplink then drops most of what is offered, and the question
//! is how much of the uplink goes to retransmissions.
//!
//! The run drives the real `PeerConnection` loops over `MockSocket` and the
//! [`LinkModel`] on virtual time, as `r1_tests` does, with one sender and
//! many receivers. What remains of the 2x after the fix is the first
//! transmissions themselves: every connection still offers new data at
//! 10 Mbit/s with no node-wide cap, which is #6045, not this test's subject.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use super::{HostLink, LinkModel, Loss};
use crate::config::GlobalRng;
use crate::simulation::{TimeSource, VirtualTime};
use crate::transport::congestion_control::{CongestionControlAlgorithm, CongestionControlConfig};
use crate::transport::mock_transport::{Channels, create_mock_peer_with_link_model};
use crate::transport::peer_connection::{CongestionProbe, PeerConnection};
use crate::transport::{Socket, TransportError};

const ONE_WAY_DELAY: Duration = Duration::from_millis(40);
const SENDER_UPLINK_BPS: u64 = 4_000_000;
/// 100 ms at the sender's uplink rate.
const SENDER_QUEUE: usize = (SENDER_UPLINK_BPS / 8 / 10) as usize;
/// Random loss after the sender's queue, so each seed is a different run.
const RANDOM_LOSS: f64 = 0.01;

async fn drive_virtual_time(vt: VirtualTime) {
    loop {
        tokio::time::sleep(Duration::from_millis(1)).await;
        vt.trigger_expired();
        vt.advance_to_next_wakeup();
    }
}

fn block_on_seeded<F: Future>(seed: u64, f: F) -> F::Output {
    let _rng = GlobalRng::seed_guard(seed ^ 0x36_31_30_30);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("runtime");
    rt.block_on(f)
}

/// Drives both ends of a hole punch until both are established (see
/// `r1_tests::connect_pair` for why both must be polled).
async fn connect_pair<S: Socket>(
    a: impl Future<Output = Result<PeerConnection<S, VirtualTime>, TransportError>>,
    b: impl Future<Output = Result<PeerConnection<S, VirtualTime>, TransportError>>,
) -> (
    PeerConnection<S, VirtualTime>,
    PeerConnection<S, VirtualTime>,
) {
    async fn serve<S: Socket>(
        conn: &mut Option<PeerConnection<S, VirtualTime>>,
    ) -> Result<Vec<u8>, TransportError> {
        match conn {
            Some(c) => c.recv().await,
            None => std::future::pending().await,
        }
    }
    let mut a = std::pin::pin!(a);
    let mut b = std::pin::pin!(b);
    let mut conn_a = None;
    let mut conn_b = None;
    while conn_a.is_none() || conn_b.is_none() {
        tokio::select! {
            biased;
            r = &mut a, if conn_a.is_none() => conn_a = Some(r.expect("connect a")),
            r = &mut b, if conn_b.is_none() => conn_b = Some(r.expect("connect b")),
            r = serve(&mut conn_a) => panic!("a received {r:?} early"),
            r = serve(&mut conn_b) => panic!("b received {r:?} early"),
        }
    }
    (conn_a.expect("set"), conn_b.expect("set"))
}

/// What one run did, from the sender's side and the link's ground truth.
struct StormReport {
    /// Streams the receivers reassembled within the horizon.
    completed: usize,
    /// Datagrams the sender handed to its uplink, including those dropped.
    offered_packets: u64,
    /// Datagrams delivered for the first time (retransmissions of a packet
    /// already delivered do not count).
    delivered_first_copies: u64,
    dropped_queue_full: u64,
    /// Offered datagrams per first copy delivered.
    amplification: f64,
    probes: Vec<CongestionProbe>,
}

async fn run_storm(peers: usize, payload_len: usize, horizon: Duration, seed: u64) -> StormReport {
    let vt = VirtualTime::new();
    let driver = tokio::spawn(drive_virtual_time(vt.clone()));
    let fast = HostLink {
        uplink_bps: 0,
        queue_bytes: 1 << 24,
        one_way_delay: ONE_WAY_DELAY,
        loss: Loss::None,
    };
    let link = Arc::new(LinkModel::new(fast.clone(), seed));
    let channels: Channels = Arc::new(DashMap::new());
    let config = Some(CongestionControlConfig::new(
        CongestionControlAlgorithm::FixedRate,
    ));
    let (pub_a, mut peer_a, addr_a) = create_mock_peer_with_link_model(
        link.clone(),
        channels.clone(),
        vt.clone(),
        config.clone(),
    )
    .await
    .expect("peer a");
    link.set_host_link(
        addr_a,
        HostLink {
            uplink_bps: SENDER_UPLINK_BPS,
            queue_bytes: SENDER_QUEUE,
            one_way_delay: ONE_WAY_DELAY,
            loss: Loss::Bernoulli(RANDOM_LOSS),
        },
    );
    let mut senders = Vec::new();
    let mut receivers = Vec::new();
    let mut handlers = Vec::new();
    let mut addrs: Vec<SocketAddr> = Vec::new();
    for _ in 0..peers {
        let (pub_b, mut peer_b, addr_b) = create_mock_peer_with_link_model(
            link.clone(),
            channels.clone(),
            vt.clone(),
            config.clone(),
        )
        .await
        .expect("peer b");
        let (ca, cb) = futures::join!(
            peer_a.connect(pub_b, addr_b),
            peer_b.connect(pub_a.clone(), addr_a)
        );
        let (ca, cb) = connect_pair(ca, cb).await;
        senders.push(ca);
        receivers.push(cb);
        handlers.push(peer_b);
        addrs.push(addr_b);
    }

    let start = vt.now_nanos();
    let payload: Vec<u8> = (0..payload_len)
        .map(|i| (i * 31 + seed as usize) as u8)
        .collect();
    for c in senders.iter_mut() {
        c.send(payload.clone()).await.expect("send");
    }
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(peers);
    let mut tasks = Vec::new();
    for mut c in receivers {
        let tx = done_tx.clone();
        tasks.push(tokio::spawn(async move {
            // One stream per receiver, so the channel (one slot per peer)
            // never fills.
            while c.recv().await.is_ok() {
                tx.try_send(()).expect("one completion per receiver");
            }
        }));
    }
    let (probe_tx, mut probe_rx) = tokio::sync::mpsc::channel::<CongestionProbe>(peers);
    let deadline = start + horizon.as_nanos() as u64;
    for mut c in senders {
        let vt2 = vt.clone();
        let ptx = probe_tx.clone();
        tasks.push(tokio::spawn(async move {
            let mut until = vt2.sleep_until(deadline);
            loop {
                tokio::select! {
                    biased;
                    r = c.recv() => if r.is_err() { break; },
                    _ = &mut until => break,
                }
            }
            ptx.try_send(c.congestion_probe())
                .expect("one probe per sender");
        }));
    }
    drop(probe_tx);
    let mut probes = Vec::new();
    while let Some(p) = probe_rx.recv().await {
        probes.push(p);
    }
    let mut completed = 0;
    while done_rx.try_recv().is_ok() {
        completed += 1;
    }
    for t in &tasks {
        t.abort();
    }
    driver.abort();

    let stats = link.host_stats(addr_a);
    let first: u64 = addrs
        .iter()
        .map(|b| {
            link.deliveries(addr_a, *b)
                .iter()
                .filter(|d| d.first_copy)
                .count() as u64
        })
        .sum();
    drop(handlers);
    drop(peer_a);
    StormReport {
        completed,
        offered_packets: stats.offered,
        delivered_first_copies: first,
        dropped_queue_full: stats.dropped_queue_full,
        amplification: stats.offered as f64 / first.max(1) as f64,
        probes,
    }
}

/// Seeds for the gate. Override with `FREENET_STORM_SEEDS` (comma-separated).
const GATE_SEEDS: [u64; 3] = [7, 0x6100_0001, 0x6100_0002];

fn seeds() -> Vec<u64> {
    match std::env::var("FREENET_STORM_SEEDS") {
        Ok(list) if !list.trim().is_empty() => list
            .split(',')
            .map(|s| s.trim().parse().expect("decimal seed"))
            .collect(),
        _ => GATE_SEEDS.to_vec(),
    }
}

/// Sixteen connections share a 4 Mbit/s uplink, each offering FixedRate's
/// 10 Mbit/s. The ideal is about 17 s for the 8 MiB total; before #6100 only
/// 9 of the 16 streams finished in 60 s and the host offered 8.3 datagrams per
/// first copy delivered, 88% of them retransmissions (seed 7), the shape of
/// the field storms. With retransmissions ACK-clocked by
/// `RETRANSMIT_CREDIT_CAP` every stream finishes and the waste stays near the
/// queue's own drop rate.
#[test]
fn shared_saturated_uplink_does_not_collapse() {
    const PEERS: usize = 16;
    let mut failures = Vec::new();
    for seed in seeds() {
        let r = block_on_seeded(
            seed,
            run_storm(PEERS, 512 * 1024, Duration::from_secs(60), seed),
        );
        let rto: u64 = r.probes.iter().map(|p| p.rto_expiries).sum();
        let abandoned: u32 = r.probes.iter().map(|p| p.abandoned_packets).sum();
        let summary = format!(
            "seed {seed}: {}/{PEERS} streams completed, {} offered / {} first copies \
             delivered = {:.2}x, {} queue drops, {rto} RTO expiries, {abandoned} abandoned",
            r.completed,
            r.offered_packets,
            r.delivered_first_copies,
            r.amplification,
            r.dropped_queue_full,
        );
        eprintln!("{summary}");
        if r.completed != PEERS || r.amplification >= 3.0 {
            failures.push(summary);
        }
    }
    assert!(
        failures.is_empty(),
        "retransmission collapse:\n{}",
        failures.join("\n")
    );
}
