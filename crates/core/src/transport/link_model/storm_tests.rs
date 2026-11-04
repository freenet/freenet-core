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
//! many receivers, and shares its `drive_virtual_time` and `connect_pair`.
//! What remains after the fix, about 3x offered per first copy delivered, has
//! two parts. About 2.1x is the first transmissions themselves (it is what is
//! left with credit earned one-for-one): every connection still offers new
//! data at 10 Mbit/s with no node-wide cap, which is #6045, not this test's
//! subject. The rest is the allowance of `CREDIT_PER_ACKED_BYTE` = 2, which
//! lets retransmissions run at up to twice the rate they are delivered so a
//! connection recovers from an outage in a few round trips.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use super::r1_tests::{connect_pair, drive_virtual_time};
use super::{HostLink, LinkModel, Loss};
use crate::config::GlobalRng;
use crate::simulation::{TimeSource, VirtualTime};
use crate::transport::congestion_control::{CongestionControlAlgorithm, CongestionControlConfig};
use crate::transport::mock_transport::{Channels, create_mock_peer_with_link_model};
use crate::transport::peer_connection::CongestionProbe;

const ONE_WAY_DELAY: Duration = Duration::from_millis(40);
const SENDER_UPLINK_BPS: u64 = 4_000_000;
/// 100 ms at the sender's uplink rate.
const SENDER_QUEUE: usize = (SENDER_UPLINK_BPS / 8 / 10) as usize;
/// Random loss after the sender's queue, so each seed is a different run.
const RANDOM_LOSS: f64 = 0.01;

/// Runs `f` to completion on a paused current-thread runtime with
/// `GlobalRng` seeded from `seed`, as `r1_tests::block_on_seeded` does but
/// with this module's own salt, so a seed shared with R1 is a different run.
fn block_on_seeded<F: Future>(seed: u64, f: F) -> F::Output {
    let _rng = GlobalRng::seed_guard(seed ^ 0x36_31_30_30);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("runtime");
    rt.block_on(f)
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
    let link = Arc::new(LinkModel::new(fast, seed));
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
        senders.push(ca.expect("connect sender"));
        receivers.push(cb.expect("connect receiver"));
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

/// Offered datagrams per first copy delivered above which the run counts as
/// a collapse. With the credit, runs measure 2.9-3.0; with retransmissions
/// unbounded (the credit disabled, or refilled whenever a connection's
/// backoff resets) they measure 6.7-8.8.
const MAX_AMPLIFICATION: f64 = 4.0;

/// Sixteen connections share a 4 Mbit/s uplink, each offering FixedRate's
/// 10 Mbit/s. The ideal is about 17 s for the 8 MiB total. Before #6100, 6 to
/// 9 of the 16 streams finished in 60 s and the host offered 7.9-8.8
/// datagrams per first copy delivered, nearly all of the excess
/// retransmissions: the shape of the field storms. With retransmissions
/// ACK-clocked by the credit (`RETRANSMIT_CREDIT_CAP`), every stream finishes.
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
        if r.completed != PEERS || r.amplification >= MAX_AMPLIFICATION {
            failures.push(summary);
        }
    }
    assert!(
        failures.is_empty(),
        "retransmission collapse:\n{}",
        failures.join("\n")
    );
}

/// What one reverse-loss run did.
struct ReverseLossReport {
    completed_after: Option<Duration>,
    probe: CongestionProbe,
}

/// One connection, fast links both ways, a `payload_len` stream from A to B.
/// Once connected, B's uplink (which carries only B's receipts, NoOps and
/// Pongs) loses `receipt_loss` of its packets, so A loses receipts while every
/// data packet arrives.
async fn run_reverse_loss(
    payload_len: usize,
    receipt_loss: f64,
    horizon: Duration,
    seed: u64,
) -> ReverseLossReport {
    let vt = VirtualTime::new();
    let driver = tokio::spawn(drive_virtual_time(vt.clone()));
    let clean = HostLink {
        uplink_bps: 0,
        queue_bytes: 1 << 24,
        one_way_delay: ONE_WAY_DELAY,
        loss: Loss::None,
    };
    let link = Arc::new(LinkModel::new(clean.clone(), seed));
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
    let (pub_b, mut peer_b, addr_b) =
        create_mock_peer_with_link_model(link.clone(), channels.clone(), vt.clone(), config)
            .await
            .expect("peer b");
    let (ca, cb) = futures::join!(
        peer_a.connect(pub_b, addr_b),
        peer_b.connect(pub_a, addr_a)
    );
    let (conn_a, conn_b) = connect_pair(ca, cb).await;
    let (mut conn_a, mut conn_b) = (conn_a.expect("connect a"), conn_b.expect("connect b"));
    link.set_host_link(
        addr_b,
        HostLink {
            loss: Loss::Bernoulli(receipt_loss),
            ..clean
        },
    );

    let start = vt.now_nanos();
    let mut until = vt.sleep_until(start + horizon.as_nanos() as u64);
    let mut completed_after = None;
    // STREAMS back-to-back: the next starts when the receiver has the last.
    const STREAMS: usize = 3;
    let mut done = 0;
    'outer: while done < STREAMS {
        let payload: Vec<u8> =
            (0..payload_len).map(|i| (i * 7 + done + seed as usize) as u8).collect();
        conn_a.send(payload).await.expect("send");
        loop {
            tokio::select! {
                biased;
                r = conn_b.recv() => {
                    if r.is_err() { break 'outer; }
                    done += 1;
                    if done == STREAMS {
                        completed_after = Some(Duration::from_nanos(vt.now_nanos() - start));
                    }
                    break;
                }
                r = conn_a.recv() => if r.is_err() { break 'outer; },
                _ = &mut until => break 'outer,
            }
        }
    }
    // Idle 30 s with both loops running, then sample what is still in flight.
    let mut idle = vt.sleep(Duration::from_secs(30));
    loop {
        tokio::select! {
            biased;
            r = conn_b.recv() => if r.is_err() { break; },
            r = conn_a.recv() => if r.is_err() { break; },
            _ = &mut idle => break,
        }
    }
    let probe = conn_a.congestion_probe();
    driver.abort();
    drop((peer_a, peer_b));
    ReverseLossReport {
        completed_after,
        probe,
    }
}

#[test]
fn reverse_loss_exploration() {
    for loss in [0.0, 0.1, 0.3, 0.5, 0.7] {
        let r = block_on_seeded(
            11,
            run_reverse_loss(6 * 1024 * 1024, loss, Duration::from_secs(120), 11),
        );
        eprintln!(
            "receipt_loss={loss}: 3 streams completed_after={:?} flightsize_after_idle={} rto={} abandoned={} failed_streams={}",
            r.completed_after,
            r.probe.flightsize,
            r.probe.rto_expiries,
            r.probe.abandoned_packets,
            r.probe.outbound_streams_failed
        );
    }
}
