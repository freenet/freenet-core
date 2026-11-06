//! Timer retransmits under a node-wide egress cap (`EgressCap::try_take_retransmit`).
//!
//! The harness is #6100's (`fix-6100`): the real `PeerConnection` loops over
//! `MockSocket` and the [`LinkModel`] on virtual time, as `r1_tests` does,
//! with one sender and one or more receivers. The sender's socket carries a
//! node-wide egress cap, as `total-bandwidth-limit` gives a real node. Each
//! scenario runs twice in one binary: with retransmits capped, and with them
//! sent and charged as unpaced sends (`EgressCap::uncap_retransmits`), which
//! is how they went before. So every row compares the change with main on
//! the same seed.

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
use crate::transport::mock_transport::{Channels, create_mock_peer_with_link_model_and_cap};
use crate::transport::peer_connection::CongestionProbe;

const ONE_WAY_DELAY: Duration = Duration::from_millis(40);
const SENDER_UPLINK_BPS: u64 = 4_000_000;
/// 100 ms at the sender's uplink rate.
const SENDER_QUEUE: usize = (SENDER_UPLINK_BPS / 8 / 10) as usize;
/// Random loss after the sender's queue, so each seed is a different run.
const RANDOM_LOSS: f64 = 0.01;

/// The sender's egress cap in a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cap {
    /// No cap: the default node.
    None,
    /// A cap at this rate, with retransmits sent uncapped, as on main.
    Main(u64),
    /// A cap at this rate, with retransmits taking from it.
    Capped(u64),
}

impl Cap {
    fn rate(self) -> Option<u64> {
        match self {
            Cap::None => None,
            Cap::Main(rate) | Cap::Capped(rate) => Some(rate),
        }
    }
}

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

/// What one run did, from the sender's side, its cap and the link.
#[derive(Debug)]
struct StormReport {
    /// Streams the receivers reassembled within the horizon.
    completed: usize,
    /// When the last of them was reassembled, from the start.
    last_completed: Option<Duration>,
    /// Datagrams the sender handed to its uplink, including those dropped.
    offered_packets: u64,
    /// Datagrams delivered for the first time.
    delivered_first_copies: u64,
    /// Offered datagrams per first copy delivered.
    amplification: f64,
    /// Bytes the cap charged, and bytes it forgave (sent past its debt limit).
    cap_bytes: u64,
    cap_forgiven: u64,
    probes: Vec<CongestionProbe>,
}

impl StormReport {
    fn summary(&self, label: &str) -> String {
        let sum = |f: fn(&CongestionProbe) -> u64| self.probes.iter().map(f).sum::<u64>();
        format!(
            "{label}: {} completed (last at {:?}), {} offered / {} first copies = {:.2}x, \
             cap charged {} B forgave {} B, {} RTO, {} retransmits, {} deferred, {} abandoned, \
             {} streams failed",
            self.completed,
            self.last_completed,
            self.offered_packets,
            self.delivered_first_copies,
            self.amplification,
            self.cap_bytes,
            self.cap_forgiven,
            sum(|p| p.rto_expiries),
            sum(|p| u64::from(p.retransmissions)),
            sum(|p| u64::from(p.retransmits_deferred)),
            sum(|p| u64::from(p.abandoned_packets)),
            sum(|p| u64::from(p.outbound_streams_failed)),
        )
    }
}

/// One host streams `payload_len` to each of `peers` receivers through a
/// 4 Mbit/s uplink with a 100 ms drop-tail queue and 1% loss after it.
async fn run_storm(
    peers: usize,
    payload_len: usize,
    horizon: Duration,
    seed: u64,
    cap: Cap,
) -> StormReport {
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
    let (pub_a, mut peer_a, addr_a) = create_mock_peer_with_link_model_and_cap(
        link.clone(),
        channels.clone(),
        vt.clone(),
        config.clone(),
        cap.rate(),
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
        let (pub_b, mut peer_b, addr_b) = create_mock_peer_with_link_model_and_cap(
            link.clone(),
            channels.clone(),
            vt.clone(),
            config.clone(),
            None,
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
    if let (Cap::Main(_), Some(sender)) = (cap, senders.first()) {
        sender.egress_cap().expect("a cap").uncap_retransmits();
    }

    let start = vt.now_nanos();
    let payload: Vec<u8> = (0..payload_len)
        .map(|i| (i * 31 + seed as usize) as u8)
        .collect();
    for c in senders.iter_mut() {
        c.send(payload.clone()).await.expect("send");
    }
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<u64>(peers);
    let mut tasks = Vec::new();
    for mut c in receivers {
        let tx = done_tx.clone();
        let vt2 = vt.clone();
        tasks.push(tokio::spawn(async move {
            while c.recv().await.is_ok() {
                tx.try_send(vt2.now_nanos())
                    .expect("one completion per receiver");
            }
        }));
    }
    let (probe_tx, mut probe_rx) = tokio::sync::mpsc::channel(peers);
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
            ptx.try_send((c.congestion_probe(), c.egress_stats()))
                .expect("one probe per sender");
        }));
    }
    drop(probe_tx);
    let mut probes = Vec::new();
    let mut stats = None;
    while let Some((probe, egress)) = probe_rx.recv().await {
        probes.push(probe);
        stats = stats.or(egress);
    }
    let mut completed = 0;
    let mut last = None;
    while let Ok(at) = done_rx.try_recv() {
        completed += 1;
        last = Some(Duration::from_nanos(at - start)).max(last);
    }
    for t in &tasks {
        t.abort();
    }
    driver.abort();

    let host = link.host_stats(addr_a);
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
        last_completed: last,
        offered_packets: host.offered,
        delivered_first_copies: first,
        amplification: host.offered as f64 / first.max(1) as f64,
        cap_bytes: stats.as_ref().map_or(0, |s| s.bytes),
        cap_forgiven: stats.as_ref().map_or(0, |s| s.forgiven_bytes),
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

/// The storm row: 16 connections share the 4 Mbit/s uplink, 512 KiB each,
/// 60 s. With the cap at 450 kB/s, capped retransmits must do at least as
/// well as main: as many streams completed, and nothing retransmitted
/// forgiven past the cap.
#[test]
fn storm_under_a_cap_matches_or_beats_main() {
    const PEERS: usize = 16;
    // Override with `FREENET_STORM_CAP` (bytes per second) to explore.
    let cap: u64 = std::env::var("FREENET_STORM_CAP")
        .ok()
        .map(|v| v.trim().parse().expect("decimal bytes per second"))
        .unwrap_or(450_000);
    let mut failures = Vec::new();
    for seed in seeds() {
        let run = |cap| {
            block_on_seeded(
                seed,
                run_storm(PEERS, 512 * 1024, Duration::from_secs(60), seed, cap),
            )
        };
        let main = run(Cap::Main(cap));
        let capped = run(Cap::Capped(cap));
        let line = format!(
            "seed {seed}\n  {}\n  {}",
            main.summary("main  "),
            capped.summary("capped")
        );
        eprintln!("{line}");
        if capped.completed < main.completed {
            failures.push(line);
        }
    }
    assert!(
        failures.is_empty(),
        "fewer streams completed with retransmits capped:\n{}",
        failures.join("\n")
    );
}

/// The no-cap row: without a cap nothing is asked, so no resend is ever
/// deferred, and every stream completes as on main.
#[test]
fn storm_without_a_cap_defers_nothing() {
    const PEERS: usize = 16;
    let seed = GATE_SEEDS[0];
    let r = block_on_seeded(
        seed,
        run_storm(PEERS, 512 * 1024, Duration::from_secs(60), seed, Cap::None),
    );
    eprintln!("{}", r.summary("no cap"));
    let deferred: u32 = r.probes.iter().map(|p| p.retransmits_deferred).sum();
    assert_eq!(deferred, 0);
    assert_eq!(r.cap_bytes, 0, "no cap to charge");
}
