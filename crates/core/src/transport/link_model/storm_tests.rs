//! Timer retransmits under a node-wide egress cap (`EgressCap::admit_retransmit`).
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
             cap charged {} B forgave {} B, {} RTO, {} retransmits, {} deferred, {} uncapped, \
             {} abandoned, {} streams failed",
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
            sum(|p| u64::from(p.retransmits_uncapped)),
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
/// Seed 35 is the 1 MB/s seed where a wider bucket spacing trailed main by
/// eight streams: it keeps the per-seed clause of the storm gate exercised.
const GATE_SEEDS: [u64; 4] = [7, 0x6100_0001, 0x6100_0002, 35];

fn seeds() -> Vec<u64> {
    match std::env::var("FREENET_STORM_SEEDS") {
        Ok(list) if !list.trim().is_empty() => list
            .split(',')
            .map(|s| s.trim().parse().expect("decimal seed"))
            .collect(),
        _ => GATE_SEEDS.to_vec(),
    }
}

/// The storm rows: 16 connections share the 4 Mbit/s (500 kB/s) uplink,
/// 512 KiB each, 60 s, with the cap above the uplink so retransmits storm
/// (a cap under the uplink means no storm, so it cannot fail). Capped
/// retransmits must complete at least as many streams as main in total and
/// at the median per seed, at 1 MB/s (2x the uplink, the planned default cap
/// of #6045) and at 2 MB/s (4x). At 1 MB/s no seed may also trail main by
/// more than one stream: that is the measured noise floor of a per-seed
/// comparison (on 40 seeds no sweep had seen, the head before the deferral
/// limit trailed main by one stream on two seeds), and it still fails a real
/// regression such as the -8 on one seed that a wider bucket spacing gave.
/// Either way nothing retransmitted is forgiven past the cap, and each row
/// must have deferred something.
///
/// The per-seed comparison holds only while the runs are deterministic: a
/// change to scheduling or RNG consumption elsewhere can move a seed without
/// any regression here, and then a 40-seed sweep (`FREENET_STORM_SEEDS`) is
/// what tells the two apart.
#[test]
fn storm_under_a_cap_matches_or_beats_main() {
    // Override with `FREENET_STORM_PEERS` to explore the wake rate at a
    // gateway's connection count.
    let peers: usize = std::env::var("FREENET_STORM_PEERS")
        .ok()
        .map(|v| v.trim().parse().expect("decimal"))
        .unwrap_or(16);
    const PER_SEED_CAP: u64 = 1_000_000;
    const PER_SEED_SLACK: usize = 1;
    // Override with `FREENET_STORM_CAP` (bytes per second) to explore.
    let caps: Vec<u64> = match std::env::var("FREENET_STORM_CAP") {
        Ok(v) => vec![v.trim().parse().expect("decimal bytes per second")],
        Err(_) => vec![PER_SEED_CAP, 2 * PER_SEED_CAP],
    };
    let mut failures = Vec::new();
    for cap in caps {
        let mut main_done = Vec::new();
        let mut capped_done = Vec::new();
        let mut capped_deferred = 0u64;
        for seed in seeds() {
            let run = |cap| {
                block_on_seeded(
                    seed,
                    run_storm(peers, 512 * 1024, Duration::from_secs(60), seed, cap),
                )
            };
            let main = run(Cap::Main(cap));
            let capped = run(Cap::Capped(cap));
            let line = format!(
                "cap {cap} B/s, seed {seed}\n  {}\n  {}",
                main.summary("main  "),
                capped.summary("capped")
            );
            eprintln!("{line}");
            // At 1 MB/s a seed may trail main by at most PER_SEED_SLACK
            // streams, the measured noise floor (see the doc comment).
            if capped.cap_forgiven > 0
                || (cap == PER_SEED_CAP && capped.completed + PER_SEED_SLACK < main.completed)
            {
                failures.push(line);
            }
            main_done.push(main.completed);
            capped_done.push(capped.completed);
            capped_deferred += capped
                .probes
                .iter()
                .map(|p| u64::from(p.retransmits_deferred))
                .sum::<u64>();
        }
        let median = |v: &mut Vec<usize>| {
            v.sort_unstable();
            let n = v.len();
            if n % 2 == 1 {
                v[n / 2] as f64
            } else {
                (v[n / 2 - 1] + v[n / 2]) as f64 / 2.0
            }
        };
        let totals = (
            main_done.iter().sum::<usize>(),
            capped_done.iter().sum::<usize>(),
        );
        let medians = (median(&mut main_done), median(&mut capped_done));
        eprintln!(
            "cap {cap} B/s: streams completed, main / capped: total {} / {}, median {} / {}",
            totals.0, totals.1, medians.0, medians.1
        );
        assert!(
            capped_deferred > 0,
            "premise: the cap deferred resends in the storm at {cap} B/s"
        );
        if totals.1 < totals.0 || medians.1 < medians.0 {
            failures.push(format!(
                "cap {cap} B/s: total {} / {}, median {} / {}",
                totals.0, totals.1, medians.0, medians.1
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "capped retransmits did worse in the storm:\n{}",
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

/// Idle time after the last stream before the receipt-loss row reads the
/// flight size, as `fix-6100`'s reverse-loss gate.
const IDLE_SECS: u64 = 180;

/// What one receipt-loss run did.
#[derive(Debug)]
struct ReverseLossReport {
    completed_after: Option<Duration>,
    /// At the end of the idle time.
    probe: CongestionProbe,
    /// At the idle point (the last stream done, failed or out of time).
    at_idle: CongestionProbe,
    /// Flight [`IDLE_SECS`] after the idle point (the old gate's reading).
    flight_at_idle_check: usize,
    /// The largest rise in flight between two one-second readings after the
    /// idle point.
    flight_rise: usize,
    /// How long after the idle point flight first read zero.
    drained_after: Option<Duration>,
    /// The longest the abandonment rules allow flight to take to drain
    /// ([`flight_drain_bound`]), from the state at the idle point.
    drain_bound: Duration,
    /// A connection closed during the idle time.
    closed: bool,
}

/// The longest flight left at the idle point can take to drain under the
/// abandonment rules, with retransmits capped at `rate` (the derivation is
/// summarised here, term by term). Nothing new is sent
/// after the idle point, so flight leaves only by receipt or abandonment.
/// A packet tracked at the idle point has at most `M` =
/// `MAX_PACKET_RETRANSMITS` resends left before the expiry that abandons it,
/// so `M + 1` expiry intervals. Each interval is at most:
/// - the effective RTO, capped at `MAX_EFFECTIVE_RTO` whatever the backoff;
/// - plus one probe timeout, since a probe that goes refreshes the send time
///   (its own, or that of a probed packet ahead of it in the queue);
/// - plus the wait for admission. A connection's resends are refused at most
///   one effective RTO in a row (`RETX_DEFER_LIMIT_RTOS`) before the due ones
///   go uncapped, so this is at most `MAX_EFFECTIVE_RTO` plus the resend
///   yields to send them per interval, however much other unpaced traffic
///   holds the bucket in debt (it assumes the uncapped sends themselves go
///   out, as the socket sends them without waiting). On an
///   idle node it is also at most, across all intervals, the retransmits
///   still possible, `M x pending x MAX_PACKET_SIZE`, at the retransmit
///   share of the rate; the bound takes the smaller. Plus one deferral floor
///   and one resend yield per interval.
///
/// Abandonments are free on the cap but count against
/// `MAX_RESENDS_PER_ITERATION`, so they add `pending /
/// MAX_RESENDS_PER_ITERATION` resend yields. `probe_timeout` is the largest
/// read during the run, since an RTT sample can raise it.
fn flight_drain_bound(pending: usize, probe_timeout: Duration, rate: u64) -> Duration {
    use crate::transport::egress_cap::{RETX_DEFER_LIMIT_RTOS, RETX_DEFER_MIN, RETX_SHARE_PERCENT};
    use crate::transport::packet_data::MAX_PACKET_SIZE;
    use crate::transport::peer_connection::{MAX_RESENDS_PER_ITERATION, RESEND_YIELD_DELAY};
    use crate::transport::sent_packet_tracker::{MAX_EFFECTIVE_RTO, MAX_PACKET_RETRANSMITS};
    let m = u64::from(MAX_PACKET_RETRANSMITS);
    let intervals = u32::try_from(m + 1).expect("small");
    let per_interval = MAX_EFFECTIVE_RTO + probe_timeout + RETX_DEFER_MIN + RESEND_YIELD_DELAY;
    let share_rate = rate * RETX_SHARE_PERCENT / 100;
    let admission_bytes = m * pending as u64 * MAX_PACKET_SIZE as u64;
    let abandon_yields =
        u32::try_from(pending.div_ceil(MAX_RESENDS_PER_ITERATION)).unwrap_or(u32::MAX);
    let admission = Duration::from_secs_f64(admission_bytes as f64 / share_rate as f64).min(
        (MAX_EFFECTIVE_RTO * RETX_DEFER_LIMIT_RTOS + RESEND_YIELD_DELAY * abandon_yields)
            * intervals,
    );
    per_interval * intervals + admission + RESEND_YIELD_DELAY * abandon_yields
}

/// One connection, fast links both ways, three back-to-back `payload_len`
/// streams from A to B. Once connected, B's uplink (which carries only B's
/// receipts, NoOps and Pongs) loses `receipt_loss` of its packets, so A loses
/// receipts while every data packet arrives. A's socket has the cap `cap`.
async fn run_reverse_loss(
    payload_len: usize,
    receipt_loss: f64,
    horizon: Duration,
    seed: u64,
    cap: Cap,
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
    let (pub_a, mut peer_a, addr_a) = create_mock_peer_with_link_model_and_cap(
        link.clone(),
        channels.clone(),
        vt.clone(),
        config.clone(),
        cap.rate(),
    )
    .await
    .expect("peer a");
    let (pub_b, mut peer_b, addr_b) = create_mock_peer_with_link_model_and_cap(
        link.clone(),
        channels.clone(),
        vt.clone(),
        config,
        None,
    )
    .await
    .expect("peer b");
    let (ca, cb) = futures::join!(peer_a.connect(pub_b, addr_b), peer_b.connect(pub_a, addr_a));
    let (conn_a, conn_b) = connect_pair(ca, cb).await;
    let (mut conn_a, mut conn_b) = (conn_a.expect("connect a"), conn_b.expect("connect b"));
    if let Cap::Main(_) = cap {
        conn_a.egress_cap().expect("a cap").uncap_retransmits();
    }
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
    const STREAMS: usize = 3;
    let mut done = 0;
    'outer: while done < STREAMS {
        let payload: Vec<u8> = (0..payload_len)
            .map(|i| (i * 7 + done + seed as usize) as u8)
            .collect();
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
    // Read flight once a second from the idle point: until it drains and
    // the old check's IDLE_SECS have passed, or until the drain bound (with
    // the largest probe timeout seen) has passed. `FREENET_RECEIPT_IDLE_SECS`
    // sets a fixed idle time instead, to explore.
    let fixed_idle = std::env::var("FREENET_RECEIPT_IDLE_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs);
    let rate = cap.rate().expect("the receipt-loss row runs with a cap");
    let at_idle = conn_a.congestion_probe();
    let idle_start = vt.now_nanos();
    let mut probe_timeout = at_idle.probe_timeout.unwrap_or_default();
    let mut last_flight = at_idle.flightsize;
    let mut flight_rise = 0;
    let mut flight_at_idle_check = None;
    let mut drained_after = (last_flight == 0).then_some(Duration::ZERO);
    let mut closed = false;
    let mut probe = at_idle;
    'idle: loop {
        let elapsed = Duration::from_nanos(vt.now_nanos() - idle_start);
        let bound = flight_drain_bound(at_idle.pending_packets, probe_timeout, rate);
        let done = match fixed_idle {
            Some(fixed) => elapsed >= fixed,
            None => (drained_after.is_some() && flight_at_idle_check.is_some()) || elapsed >= bound,
        };
        if done {
            break;
        }
        let mut tick = vt.sleep(Duration::from_secs(1));
        loop {
            tokio::select! {
                biased;
                r = conn_b.recv() => if r.is_err() { closed = true; break 'idle; },
                r = conn_a.recv() => if r.is_err() { closed = true; break 'idle; },
                _ = &mut tick => break,
            }
        }
        probe = conn_a.congestion_probe();
        probe_timeout = probe_timeout.max(probe.probe_timeout.unwrap_or_default());
        flight_rise = flight_rise.max(probe.flightsize.saturating_sub(last_flight));
        last_flight = probe.flightsize;
        let elapsed = Duration::from_nanos(vt.now_nanos() - idle_start);
        if probe.flightsize == 0 && drained_after.is_none() {
            drained_after = Some(elapsed);
        }
        if elapsed >= Duration::from_secs(IDLE_SECS) && flight_at_idle_check.is_none() {
            flight_at_idle_check = Some(probe.flightsize);
        }
    }
    driver.abort();
    drop((peer_a, peer_b));
    ReverseLossReport {
        completed_after,
        probe,
        at_idle,
        flight_at_idle_check: flight_at_idle_check.unwrap_or(probe.flightsize),
        flight_rise,
        drained_after,
        drain_bound: flight_drain_bound(at_idle.pending_packets, probe_timeout, rate),
        closed,
    }
}

/// The receipt-loss row: receipts are not cumulative, so a path that loses
/// most of the receiver's packets loses most of the sender's receipts while
/// every data packet arrives (the case that sank #6100's ACK-clocked credit).
/// With the cap above the stream rate (FixedRate's 1.25 MB/s) and at it,
/// capped retransmits must deliver no later than main (with 10% slack for
/// timer granularity) and fail no more streams. Flight left at the idle point
/// must never rise and must drain within the bound the abandonment rules set
/// ([`flight_drain_bound`]): the check is for flight that never drains
/// (`fix-6100`'s held resends). Capping retransmits slows the abandonment,
/// which counts resends, so flight that main's failed stream released at
/// once can still be draining at the old fixed 180 s reading; that reading
/// is printed but not judged.
#[test]
fn receipt_loss_under_a_cap_matches_or_beats_main() {
    let mut failures = Vec::new();
    // Override with `FREENET_RECEIPT_SEEDS` and `FREENET_RECEIPT_RATES`
    // (comma-separated) to explore.
    let list = |name: &str, default: &[u64]| -> Vec<u64> {
        match std::env::var(name) {
            Ok(v) if !v.trim().is_empty() => v
                .split(',')
                .map(|s| s.trim().parse().expect("decimal"))
                .collect(),
            _ => default.to_vec(),
        }
    };
    let mut deferred_by_rate = std::collections::BTreeMap::new();
    for rate in list("FREENET_RECEIPT_RATES", &[2_500_000, 1_250_000]) {
        for loss in [0.5, 0.7, 0.9] {
            for seed in list("FREENET_RECEIPT_SEEDS", &[11]) {
                let run = |cap| {
                    block_on_seeded(
                        seed,
                        run_reverse_loss(
                            6 * 1024 * 1024,
                            loss,
                            Duration::from_secs(120),
                            seed,
                            cap,
                        ),
                    )
                };
                let main = run(Cap::Main(rate));
                let capped = run(Cap::Capped(rate));
                let line = |label: &str, r: &ReverseLossReport| {
                    format!(
                        "{label}: done after {:?}, {} B ({} packets) in flight at idle, {} B after \
                     {IDLE_SECS} s idle, drained after {:?} (bound {:?}), largest rise {} B, \
                     closed {}, {} RTO, {} retransmits, {} deferred, {} uncapped, \
                     {} abandoned, {} streams failed",
                        r.completed_after,
                        r.at_idle.flightsize,
                        r.at_idle.pending_packets,
                        r.flight_at_idle_check,
                        r.drained_after,
                        r.drain_bound,
                        r.flight_rise,
                        r.closed,
                        r.probe.rto_expiries,
                        r.probe.retransmissions,
                        r.probe.retransmits_deferred,
                        r.probe.retransmits_uncapped,
                        r.probe.abandoned_packets,
                        r.probe.outbound_streams_failed,
                    )
                };
                let summary = format!(
                    "cap {rate} B/s, receipt loss {loss}, seed {seed}\n  {}\n  {}",
                    line("main  ", &main),
                    line("capped", &capped)
                );
                eprintln!("{summary}");
                let late = match (main.completed_after, capped.completed_after) {
                    (Some(m), Some(c)) => c > m.mul_f64(1.1),
                    (Some(_), None) => true,
                    (None, _) => false,
                };
                *deferred_by_rate.entry(rate).or_insert(0) += capped.probe.retransmits_deferred;
                // The old flight condition, printed for comparison only.
                let old_flight_fails =
                    capped.flight_at_idle_check > main.flight_at_idle_check.max(64 * 1024);
                let undrained = capped.closed
                    || capped.flight_rise > 0
                    || capped.drained_after.is_none_or(|d| d > capped.drain_bound);
                eprintln!(
                    "  flight: old condition {}, derived bound {}",
                    if old_flight_fails { "FAILS" } else { "passes" },
                    if undrained { "FAILS" } else { "passes" },
                );
                if late
                    || capped.probe.outbound_streams_failed > main.probe.outbound_streams_failed
                    || undrained
                {
                    failures.push(summary);
                }
            }
        }
    }
    // The row must exercise the cap: at the stream rate it binds.
    match deferred_by_rate.get(&1_250_000) {
        Some(&deferred) => assert!(
            deferred > 0,
            "premise: the cap deferred resends under receipt loss at the stream rate"
        ),
        None => {
            eprintln!("premise NOT checked: FREENET_RECEIPT_RATES omits the stream rate (1250000)")
        }
    }
    assert!(
        failures.is_empty(),
        "capped retransmits did worse under receipt loss:\n{}",
        failures.join("\n")
    );
}

/// What one outage run did.
#[derive(Debug)]
struct OutageReport {
    completed_after: Option<Duration>,
    retransmits_during_outage: u32,
    probe: CongestionProbe,
}

/// One connection, a `payload_len` stream from A to B; `outage_at` into it the
/// link drops everything both ways for `outage`, then is clean again.
async fn run_outage(
    payload_len: usize,
    outage_at: Duration,
    outage: Duration,
    horizon: Duration,
    seed: u64,
    cap: Cap,
) -> OutageReport {
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
    let (pub_a, mut peer_a, addr_a) = create_mock_peer_with_link_model_and_cap(
        link.clone(),
        channels.clone(),
        vt.clone(),
        config.clone(),
        cap.rate(),
    )
    .await
    .expect("peer a");
    let (pub_b, mut peer_b, addr_b) = create_mock_peer_with_link_model_and_cap(
        link.clone(),
        channels.clone(),
        vt.clone(),
        config,
        None,
    )
    .await
    .expect("peer b");
    let (ca, cb) = futures::join!(peer_a.connect(pub_b, addr_b), peer_b.connect(pub_a, addr_a));
    let (conn_a, conn_b) = connect_pair(ca, cb).await;
    let (mut conn_a, mut conn_b) = (conn_a.expect("connect a"), conn_b.expect("connect b"));
    if let Cap::Main(_) = cap {
        conn_a.egress_cap().expect("a cap").uncap_retransmits();
    }
    let start = vt.now_nanos();
    let payload: Vec<u8> = (0..payload_len)
        .map(|i| (i * 13 + seed as usize) as u8)
        .collect();
    conn_a.send(payload).await.expect("send");
    let dark = HostLink {
        loss: Loss::Bernoulli(1.0),
        ..clean.clone()
    };
    let mut phase_end = vt.sleep_until(start + outage_at.as_nanos() as u64);
    let mut phase = 0; // 0 before, 1 during, 2 after the outage
    let mut retransmits_at_down = 0;
    let mut retransmits_during_outage = 0;
    let mut until = vt.sleep_until(start + horizon.as_nanos() as u64);
    let mut completed_after = None;
    loop {
        tokio::select! {
            biased;
            r = conn_b.recv() => {
                if r.is_ok() {
                    completed_after = Some(Duration::from_nanos(vt.now_nanos() - start));
                }
                break;
            }
            r = conn_a.recv() => if r.is_err() { break; },
            _ = &mut phase_end, if phase < 2 => {
                if phase == 0 {
                    link.set_host_link(addr_a, dark.clone());
                    link.set_host_link(addr_b, dark.clone());
                    retransmits_at_down = conn_a.congestion_probe().retransmissions;
                    phase_end = vt.sleep_until(
                        start + (outage_at + outage).as_nanos() as u64,
                    );
                } else {
                    link.set_host_link(addr_a, clean.clone());
                    link.set_host_link(addr_b, clean.clone());
                    retransmits_during_outage =
                        conn_a.congestion_probe().retransmissions - retransmits_at_down;
                }
                phase += 1;
            }
            _ = &mut until => break,
        }
    }
    let probe = conn_a.congestion_probe();
    driver.abort();
    drop((peer_a, peer_b));
    OutageReport {
        completed_after,
        retransmits_during_outage,
        probe,
    }
}

/// The outage row: a 6 MiB stream loses the link both ways for 2 s, 2 s in.
/// Longer outages end the stream in main as well: past the sender's 3 s
/// congestion-window wait (the outage pins flight) or the receiver's 5 s
/// inactivity timeout, so they cannot tell the two apart.
/// With the cap above the stream rate and at it, capped retransmits must
/// finish no later than main (10% slack) and fail no more streams.
#[test]
fn an_outage_under_a_cap_matches_or_beats_main() {
    let mut failures = Vec::new();
    let mut deferred_at_stream_rate = 0;
    for rate in [2_500_000u64, 1_250_000] {
        for seed in seeds() {
            let run = |cap| {
                block_on_seeded(
                    seed,
                    run_outage(
                        6 * 1024 * 1024,
                        Duration::from_secs(2),
                        Duration::from_secs(2),
                        Duration::from_secs(120),
                        seed,
                        cap,
                    ),
                )
            };
            let main = run(Cap::Main(rate));
            let capped = run(Cap::Capped(rate));
            let line = |label: &str, r: &OutageReport| {
                format!(
                    "{label}: done after {:?}, {} retransmits during the outage, {} RTO, \
                     {} deferred, {} uncapped, {} abandoned, {} streams failed",
                    r.completed_after,
                    r.retransmits_during_outage,
                    r.probe.rto_expiries,
                    r.probe.retransmits_deferred,
                    r.probe.retransmits_uncapped,
                    r.probe.abandoned_packets,
                    r.probe.outbound_streams_failed,
                )
            };
            let summary = format!(
                "outage, cap {rate} B/s, seed {seed}\n  {}\n  {}",
                line("main  ", &main),
                line("capped", &capped)
            );
            eprintln!("{summary}");
            let late = match (main.completed_after, capped.completed_after) {
                (Some(m), Some(c)) => c > m.mul_f64(1.1),
                (Some(_), None) => true,
                (None, _) => false,
            };
            if rate == 1_250_000 {
                deferred_at_stream_rate += capped.probe.retransmits_deferred;
            }
            if late || capped.probe.outbound_streams_failed > main.probe.outbound_streams_failed {
                failures.push(summary);
            }
        }
    }
    // The row must exercise the cap: at the stream rate the post-outage
    // repair burst meets it.
    assert!(
        deferred_at_stream_rate > 0,
        "premise: the cap deferred resends after the outage at the stream rate"
    );
    assert!(
        failures.is_empty(),
        "capped retransmits did worse after an outage:\n{}",
        failures.join("\n")
    );
}
