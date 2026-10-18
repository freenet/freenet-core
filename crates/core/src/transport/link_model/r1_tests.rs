//! R1 from the WAN transport test bench design: the RTO-coupling death spiral.
//!
//! One 8 MiB stream over a 250 ms RTT path with 1% random loss and an 8 Mbit/s
//! bottleneck (below FixedRate's 10 Mbit/s, so even the default controller
//! has to cope with a full queue), with the RTT spiking to 3.4 s (the field's
//! p90 maximum RTT) shortly after the transfer starts. The spike drives RTT
//! far past the RTO, so every controller sees timeouts. A controller that
//! couples its estimates to RTOs (BBR before #2699, LEDBAT++ before
//! #2464/#2467) collapses its window and never climbs back on a 250 ms path;
//! this test is meant to fail for exactly that.
//!
//! It runs the real `PeerConnection` send and receive loops over `MockSocket`
//! with the [`LinkModel`] behind it, on a current-thread runtime whose tokio
//! clock is paused. All transport timers run on [`VirtualTime`]; the driver
//! task advances it to the next pending wakeup each time every other task is
//! blocked, so no wall-clock time is ever waited on.
//!
//! Each run is a function of its seed: the link draws from its own seeded
//! RNG, and `GlobalRng` (which orders the transport's `deterministic_select!`
//! branches) is seeded per run on the thread that runs every task.
//! `r1_runs_are_reproducible_from_the_seed` checks that.
//!
//! Seeds: the gate runs [`GATE_SEEDS`]. Set `FREENET_WAN_BENCH_SEEDS` to a
//! comma-separated list to run others, e.g. to reproduce a nightly failure.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use super::{Delivery, HostLink, LinkEvent, LinkModel, Loss};
use crate::config::GlobalRng;
use crate::simulation::{TimeSource, VirtualTime};
use crate::transport::congestion_control::{CongestionControlAlgorithm, CongestionControlConfig};
use crate::transport::mock_transport::{Channels, create_mock_peer_with_link_model};
use crate::transport::packet_data::MAX_PACKET_SIZE;
use crate::transport::peer_connection::CongestionProbe;

// ---------------------------------------------------------------------------
// Scenario: the R1 link profile
// ---------------------------------------------------------------------------

/// Stream payload, in bytes. Large enough that a sender at the bottleneck
/// rate still has data left when the path clears after the spike, so the
/// recovery check always has something to measure.
const PAYLOAD_LEN: usize = 8 * 1024 * 1024;
const ONE_WAY_DELAY: Duration = Duration::from_millis(125);
const LOSS: f64 = 0.01;
/// 8 Mbit/s: below FixedRate's fixed 10 Mbit/s, so no controller gets a
/// bottleneck that happens to match its own rate.
const BOTTLENECK_BPS: u64 = 8_000_000;
/// Drop-tail queue capacity in bytes: 100 ms at the bottleneck rate. A
/// well-provisioned uplink, not a bloated one, so a sender overrunning it
/// sees drops.
const QUEUE_CAPACITY: usize = (BOTTLENECK_BPS / 8 / 10) as usize;
/// RTT seen by a data packet sent during the spike. The spike is applied to
/// the sender's outbound leg only, so a packet leaving the bottleneck inside
/// the window has exactly this RTT (its ACK comes back on the normal path).
const SPIKE_RTT: Duration = Duration::from_millis(3400);
/// How long, measured at the bottleneck, packets get the spike's delay. The
/// field figure is the spike's height, not its length; 1 s keeps it a
/// transient that still drives RTT far past the RTO.
const SPIKE_DURATION: Duration = Duration::from_secs(1);
/// When the spike starts, after the stream is handed to the transport.
const SPIKE_AFTER_START: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------------------
// Pass criteria
// ---------------------------------------------------------------------------

/// The stream must finish within this multiple of its ideal time.
const COMPLETION_FACTOR: u32 = 2;
/// Once the path clears, goodput over a window must reach this fraction of
/// the bottleneck rate...
const RECOVERY_FRACTION: f64 = 0.5;
/// ...measured over a window of this many base RTTs...
const RECOVERY_WINDOW_RTTS: u32 = 2;
// ...within `recovery_rtts()` base RTTs.

/// Virtual time to keep both loops running after the stream completes, so
/// the final ACKs land and any lost one is recovered by retransmission,
/// before the idle invariant is checked: 20 minimum RTOs (500 ms), room for
/// several backed-off retransmissions of a lost final ACK.
const IDLE_AFTER_COMPLETION: Duration = Duration::from_secs(10);

/// Fixed seeds for the gating run.
const GATE_SEEDS: [u64; 3] = [0x5eed_0001, 0x5eed_0002, 0x5eed_0003];

fn base_rtt() -> Duration {
    ONE_WAY_DELAY * 2
}

/// Added to the sender's outbound leg while the spike lasts.
fn spike_extra() -> Duration {
    SPIKE_RTT - base_rtt()
}

/// Offset from the start after which a packet leaving the bottleneck is no
/// longer held behind the spike's backlog (delivery is FIFO per path, so
/// everything departing before this arrives with the last spiked packet).
fn path_clear() -> Duration {
    SPIKE_AFTER_START + SPIKE_DURATION + spike_extra()
}

/// The best a sender can do: serialize the payload at the bottleneck rate,
/// plus one round trip, plus one spike window of bottleneck time. Any sender
/// whose RTO is below the spike RTT retransmits what it sent during the
/// window although nothing was lost, and that is at most one window of
/// bottleneck time.
fn ideal_completion() -> Duration {
    let serialization = Duration::from_secs_f64(PAYLOAD_LEN as f64 * 8.0 / BOTTLENECK_BPS as f64);
    serialization + base_rtt() + SPIKE_DURATION
}

/// After the path clears, the spiked backlog still has to be acknowledged
/// (one RTT) before a window-limited sender can send again; restarting from
/// one packet and doubling each RTT, it then reaches the BDP in
/// `ceil(log2(BDP / MSS))` round trips. Allow that, plus the measurement
/// window.
fn recovery_rtts() -> u32 {
    let bdp = BOTTLENECK_BPS as f64 / 8.0 * base_rtt().as_secs_f64();
    1 + (bdp / MAX_PACKET_SIZE as f64).log2().ceil() as u32 + RECOVERY_WINDOW_RTTS
}

/// Bytes a recovered sender delivers in one measurement window.
fn recovery_target_bytes() -> u64 {
    (RECOVERY_FRACTION * BOTTLENECK_BPS as f64 / 8.0
        * (base_rtt() * RECOVERY_WINDOW_RTTS).as_secs_f64()) as u64
}

fn seeds() -> Vec<u64> {
    match std::env::var("FREENET_WAN_BENCH_SEEDS") {
        Ok(list) if !list.trim().is_empty() => list
            .split(',')
            .map(|s| {
                let s = s.trim();
                s.strip_prefix("0x")
                    .map(|h| u64::from_str_radix(h, 16))
                    .unwrap_or_else(|| s.parse())
                    .unwrap_or_else(|e| panic!("bad seed {s:?} in FREENET_WAN_BENCH_SEEDS: {e}"))
            })
            .collect(),
        _ => GATE_SEEDS.to_vec(),
    }
}

#[derive(Debug)]
struct Report {
    algorithm: CongestionControlAlgorithm,
    seed: u64,
    /// Virtual time from handing the stream over to receiving it whole.
    completed_after: Option<Duration>,
    /// When goodput first reached the recovery target, measured from the
    /// moment the path cleared.
    recovered_after: Option<Duration>,
    /// Payload whose first copy had not left the bottleneck when the path
    /// cleared: what the recovery check had to work with.
    bytes_left_at_path_clear: u64,
    pre_spike_goodput_mbps: f64,
    post_clear_goodput_mbps: f64,
    /// Longest one-way delay any data packet had: ground truth that the spike
    /// happened at its full height.
    max_one_way_delay: Duration,
    /// Deliveries the link recognised as copies of an earlier one.
    duplicate_deliveries: u64,
    sender: CongestionProbe,
    receiver: CongestionProbe,
    max_queueing_delay: Duration,
    drops_queue_full: u64,
    drops_random: u64,
    /// Hash of every delivery and drop count on both paths: two runs with
    /// the same hash moved the same packets at the same virtual times.
    trace_hash: u64,
    error: Option<String>,
}

impl Report {
    /// Every pass criterion this run failed, in words.
    fn failures(&self) -> Vec<String> {
        let mut out = Vec::new();
        let bound = ideal_completion() * COMPLETION_FACTOR;
        if let Some(e) = &self.error {
            out.push(format!("transport error: {e}"));
        }
        match self.completed_after {
            None => out.push(format!("did not complete within {bound:?}")),
            Some(t) if t > bound => out.push(format!("completed in {t:?} > {bound:?}")),
            Some(_) => {}
        }
        // Vacuity guards: the scenario must actually have happened.
        if self.max_one_way_delay < ONE_WAY_DELAY + spike_extra() {
            out.push(format!(
                "no data packet saw the full spike (max one-way {:?}): the scenario did not run",
                self.max_one_way_delay
            ));
        }
        if self.sender.rto_expiries == 0 {
            out.push("the spike drove no RTO expiries: the timeout path was not exercised".into());
        }
        if self.bytes_left_at_path_clear < 2 * recovery_target_bytes() {
            out.push(format!(
                "only {} B were left when the path cleared, too little to measure recovery",
                self.bytes_left_at_path_clear
            ));
        }
        if self.recovered_after.is_none() {
            out.push(format!(
                "goodput never reached {:.0}% of the bottleneck within {} RTTs ({:?}) of the path \
                 clearing",
                RECOVERY_FRACTION * 100.0,
                recovery_rtts(),
                base_rtt() * recovery_rtts()
            ));
        }
        if self.sender.outbound_streams_failed > 0 {
            out.push(format!(
                "sender gave up on the stream ({} outbound stream failure(s), e.g. the cwnd-wait timeout)",
                self.sender.outbound_streams_failed
            ));
        }
        if self.sender.abandoned_packets > 0 {
            out.push(format!(
                "sender abandoned {} packet(s) after the maximum retransmissions",
                self.sender.abandoned_packets
            ));
        }
        // The goodput figures count a retransmission as a duplicate only
        // because it is byte-identical to the original. Retransmissions
        // certainly happened (rto_expiries > 0 above), so seeing none means
        // that identity broke and goodput is overstated.
        if self.duplicate_deliveries == 0 {
            out.push(
                "no delivery was recognised as a retransmitted copy: goodput accounting is broken"
                    .into(),
            );
        }
        for (side, p) in [("sender", &self.sender), ("receiver", &self.receiver)] {
            // Only meaningful once the transfer is over: mid-transfer, bytes
            // in flight are expected.
            if self.completed_after.is_some() && p.flightsize != 0 {
                out.push(format!(
                    "{side} flightsize {} bytes after {IDLE_AFTER_COMPLETION:?} idle, expected 0",
                    p.flightsize
                ));
            }
            if let Some(min) = p.min_rtt_sample {
                if min < base_rtt() {
                    out.push(format!(
                        "{side} took an RTT sample of {min:?}, below the true base RTT {:?}",
                        base_rtt()
                    ));
                }
            }
        }
        out
    }
}

/// Advance virtual time whenever every other task is blocked.
///
/// The tokio clock is paused, so this 1 ms tokio timer only fires when the
/// runtime has nothing else to run: it is an idle probe, not a wait, and no
/// wall-clock time passes. Every transport timer this transfer depends on
/// runs on `vt`; the one tokio-clock reader on the path is the receiver's
/// 60 s duplicate-retention window, which only decides how long dedup state
/// is kept.
async fn drive_virtual_time(vt: VirtualTime) {
    loop {
        tokio::time::sleep(Duration::from_millis(1)).await;
        vt.trigger_expired();
        vt.advance_to_next_wakeup();
    }
}

fn run(algorithm: CongestionControlAlgorithm, seed: u64) -> Report {
    // `GlobalRng` is thread-local, and a current-thread runtime polls every
    // task on this thread, so this seeds every draw the transport makes.
    // `seed_guard` also pins the thread index, so concurrent tests on other
    // threads cannot shift the stream.
    let _rng = GlobalRng::seed_guard(seed ^ 0x52_31_5f_67_72_6e_67);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("runtime");
    rt.block_on(run_inner(algorithm, seed))
}

fn trace_hash(link: &LinkModel, a: SocketAddr, b: SocketAddr) -> u64 {
    let mut h = DefaultHasher::new();
    for (from, to) in [(a, b), (b, a)] {
        for d in link.deliveries(from, to) {
            (
                d.departed_at_nanos,
                d.arrives_at_nanos,
                d.bytes,
                d.first_copy,
            )
                .hash(&mut h);
        }
        let s = link.host_stats(from);
        (
            s.offered,
            s.delivered,
            s.dropped_queue_full,
            s.dropped_random,
            s.dropped_outage,
        )
            .hash(&mut h);
    }
    h.finish()
}

/// First-copy bytes that left the bottleneck at or after `departed_from` and
/// arrived in `(from, to]`.
fn fresh_bytes(deliveries: &[Delivery], departed_from: u64, from: u64, to: u64) -> u64 {
    deliveries
        .iter()
        .filter(|d| {
            d.first_copy
                && d.departed_at_nanos >= departed_from
                && d.arrives_at_nanos > from
                && d.arrives_at_nanos <= to
        })
        .map(|d| d.bytes as u64)
        .sum()
}

async fn run_inner(algorithm: CongestionControlAlgorithm, seed: u64) -> Report {
    // Asserted directly because outcomes alone do not show it: with this
    // scenario, unseeded runs happened to match too (3 of 3 on 2026-10-18).
    assert!(
        GlobalRng::is_seeded(),
        "GlobalRng must be seeded on the thread that polls the transport"
    );
    let vt = VirtualTime::new();
    let driver = tokio::spawn(drive_virtual_time(vt.clone()));
    let link = Arc::new(LinkModel::new(
        HostLink {
            uplink_bps: BOTTLENECK_BPS,
            queue_bytes: QUEUE_CAPACITY,
            one_way_delay: ONE_WAY_DELAY,
            loss: Loss::Bernoulli(LOSS),
        },
        seed,
    ));
    let channels: Channels = Arc::new(DashMap::new());
    let config = Some(CongestionControlConfig::new(algorithm));
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
    assert_eq!(link.base_rtt(addr_a, addr_b), base_rtt());

    let (conn_a, conn_b) =
        futures::join!(peer_a.connect(pub_b, addr_b), peer_b.connect(pub_a, addr_a));
    let (conn_a, conn_b) = futures::join!(conn_a, conn_b);
    let mut conn_a = conn_a.expect("connect a");
    let mut conn_b = conn_b.expect("connect b");

    let start = vt.now_nanos();
    let spike_start = Duration::from_nanos(start) + SPIKE_AFTER_START;
    link.schedule(LinkEvent::DelaySpike {
        start: spike_start,
        duration: SPIKE_DURATION,
        extra_delay: spike_extra(),
        from: Some(addr_a),
    });
    let clear = start + path_clear().as_nanos() as u64;

    let payload: Vec<u8> = (0..PAYLOAD_LEN)
        .map(|i| (i as u64).wrapping_mul(0x9e37_79b9).wrapping_add(seed) as u8)
        .collect();
    conn_a.send(payload.clone()).await.expect("send");

    // Deliberately `biased`: polling order is fixed so a seed replays the same
    // schedule. Neither recv returns until a message or an error arrives.
    let deadline = start + (ideal_completion() * COMPLETION_FACTOR).as_nanos() as u64;
    let mut timeout = vt.sleep_until(deadline);
    let mut error = None;
    let mut completed_after = None;
    loop {
        tokio::select! {
            biased;
            r = conn_b.recv() => {
                match r {
                    Ok(bytes) => {
                        let got: Vec<u8> = bincode::deserialize(&bytes).expect("decode payload");
                        assert!(got == payload, "payload corrupted in transit");
                        completed_after = Some(Duration::from_nanos(vt.now_nanos() - start));
                    }
                    Err(e) => error = Some(format!("receiver: {e:?}")),
                }
                break;
            }
            r = conn_a.recv() => {
                if let Err(e) = r {
                    error = Some(format!("sender: {e:?}"));
                    break;
                }
            }
            _ = &mut timeout => break,
        }
    }

    // Idle: keep both loops running so the last ACKs land, then sample.
    let mut idle = vt.sleep(IDLE_AFTER_COMPLETION);
    if error.is_none() {
        loop {
            tokio::select! {
                biased;
                r = conn_b.recv() => if let Err(e) = r { error = Some(format!("receiver idle: {e:?}")); break; },
                r = conn_a.recv() => if let Err(e) = r { error = Some(format!("sender idle: {e:?}")); break; },
                _ = &mut idle => break,
            }
        }
    }
    let sender = conn_a.congestion_probe();
    let receiver = conn_b.congestion_probe();
    driver.abort();

    let deliveries = link.deliveries(addr_a, addr_b);
    let window = (base_rtt() * RECOVERY_WINDOW_RTTS).as_nanos() as u64;
    let target = recovery_target_bytes();
    let recovery_end = clear + (base_rtt() * recovery_rtts()).as_nanos() as u64;
    let step = Duration::from_millis(10).as_nanos() as u64;
    // Only packets that left the bottleneck after the path cleared count, so
    // the spike's backlog arriving in a burst cannot pass for recovery.
    let recovered_after = (clear + window..=recovery_end)
        .step_by(step as usize)
        .find(|t| fresh_bytes(&deliveries, clear, t - window, *t) >= target)
        .map(|t| Duration::from_nanos(t - clear));
    let spike_start_nanos = spike_start.as_nanos() as u64;
    let mbps = |departed_from: u64, from: u64, to: u64| {
        fresh_bytes(&deliveries, departed_from, from, to) as f64 * 8.0
            / Duration::from_nanos(to.saturating_sub(from).max(1)).as_secs_f64()
            / 1e6
    };
    let departed_before_clear: u64 = deliveries
        .iter()
        .filter(|d| d.first_copy && d.departed_at_nanos < clear)
        .map(|d| d.bytes as u64)
        .sum();
    let stats = link.host_stats(addr_a);
    Report {
        algorithm,
        seed,
        completed_after,
        recovered_after,
        bytes_left_at_path_clear: (PAYLOAD_LEN as u64).saturating_sub(departed_before_clear),
        pre_spike_goodput_mbps: mbps(0, start, spike_start_nanos),
        post_clear_goodput_mbps: mbps(clear, clear, recovery_end),
        max_one_way_delay: deliveries
            .iter()
            .map(|d| Duration::from_nanos(d.arrives_at_nanos - d.departed_at_nanos))
            .max()
            .unwrap_or_default(),
        duplicate_deliveries: deliveries.iter().filter(|d| !d.first_copy).count() as u64,
        sender,
        receiver,
        max_queueing_delay: stats.max_queueing_delay,
        drops_queue_full: stats.dropped_queue_full,
        drops_random: stats.dropped_random,
        trace_hash: trace_hash(&link, addr_a, addr_b),
        error,
    }
}

fn describe(p: &CongestionProbe) -> String {
    let cwnd = if p.cwnd > 1 << 40 {
        "unbounded".to_string()
    } else {
        p.cwnd.to_string()
    };
    format!(
        "flightsize={} cwnd={cwnd} min_rtt={:?} srtt={:?} rto_expiries={} abandoned={} streams_failed={}",
        p.flightsize,
        p.min_rtt_sample,
        p.srtt,
        p.rto_expiries,
        p.abandoned_packets,
        p.outbound_streams_failed
    )
}

fn gate(algorithm: CongestionControlAlgorithm) {
    let reports: Vec<Report> = seeds().into_iter().map(|s| run(algorithm, s)).collect();
    let mut failed = Vec::new();
    for r in &reports {
        eprintln!(
            "R1 {:<9} seed={:#x} completed={:?} (bound {:?}) recovered_after={:?} \
             left_at_clear={}B pre_spike={:.2}Mbit/s post_clear={:.2}Mbit/s \
             max_one_way={:?} duplicates={} max_queue_delay={:?} drops(queue={}, random={}) \
             trace={:#018x} sender=[{}] receiver=[{}]",
            r.algorithm.to_string(),
            r.seed,
            r.completed_after,
            ideal_completion() * COMPLETION_FACTOR,
            r.recovered_after,
            r.bytes_left_at_path_clear,
            r.pre_spike_goodput_mbps,
            r.post_clear_goodput_mbps,
            r.max_one_way_delay,
            r.duplicate_deliveries,
            r.max_queueing_delay,
            r.drops_queue_full,
            r.drops_random,
            r.trace_hash,
            describe(&r.sender),
            describe(&r.receiver),
        );
        for f in r.failures() {
            failed.push(format!("seed {:#x}: {f}", r.seed));
        }
    }
    assert!(
        failed.is_empty(),
        "R1 failed for {algorithm} (rerun one seed with FREENET_WAN_BENCH_SEEDS=<seed>):\n  {}",
        failed.join("\n  ")
    );
}

/// Same seed, same run: compares every field of the report, including a hash
/// over every delivery's timing. A different seed must give a different
/// trace, or the hash is not looking at anything.
#[test]
fn r1_runs_are_reproducible_from_the_seed() {
    let algorithm = CongestionControlAlgorithm::FixedRate;
    let first = format!("{:?}", run(algorithm, GATE_SEEDS[0]));
    let again = format!("{:?}", run(algorithm, GATE_SEEDS[0]));
    assert_eq!(first, again, "same seed, different run");
    let other = run(algorithm, GATE_SEEDS[1]);
    assert!(
        !first.contains(&format!("trace_hash: {},", other.trace_hash)),
        "a different seed produced the same trace"
    );
}

#[test]
fn r1_spike_recovery_fixed_rate() {
    gate(CongestionControlAlgorithm::FixedRate);
}

// Fails on main (measured 2026-10-18 on seeds 1-20 and the gate seeds): no
// seed finishes, and 18 of 20 abort the stream on the 3 s cwnd-wait timeout,
// because no ACK returns for the ~3.15 s the spike holds the data leg. Before
// the spike it reaches ~3.3 Mbit/s of 8. Without any spike (earlier 10 Mbit/s
// profile) it reached ~4 Mbit/s clean and was loss-limited at 1% loss.
#[test]
#[ignore = "known failure: BBR aborts the stream (cwnd-wait timeout) during a 3.4 s RTT spike and is slow on this path without one; #5929"]
fn r1_spike_recovery_bbr() {
    gate(CongestionControlAlgorithm::Bbr);
}

// Fails on main (measured 2026-10-18 on seeds 1-20 and the gate seeds): every
// seed aborts the stream on the 3 s cwnd-wait timeout during the spike. Before
// the spike it reaches ~1.4 Mbit/s of 8; 1% loss alone held it to ~0.2-1.2
// Mbit/s at this RTT on the earlier 10 Mbit/s profile.
#[test]
#[ignore = "known failure: LEDBAT++ aborts the stream (cwnd-wait timeout) during a 3.4 s RTT spike and is loss-limited far below the bottleneck at 250 ms RTT; #5930"]
fn r1_spike_recovery_ledbat() {
    gate(CongestionControlAlgorithm::Ledbat);
}
