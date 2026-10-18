//! R1 from the WAN transport test bench design: the RTO-coupling death spiral.
//!
//! One 4 MiB stream over a 250 ms RTT path with 1% random loss and a 10 Mbit/s
//! bottleneck, with an RTT spike to 3.4 s (the field's p90 maximum RTT) shortly
//! after the transfer starts. The spike drives RTT far past the RTO, so every
//! controller sees timeouts. A controller that couples its estimates to RTOs
//! (BBR before #2699, LEDBAT++ before #2464/#2467) collapses its window and
//! never climbs back on a 250 ms path; this test is meant to fail for exactly
//! that.
//!
//! It runs the real `PeerConnection` send and receive loops over `MockSocket`
//! with the [`LinkModel`] behind it, on a current-thread runtime whose tokio
//! clock is paused. All transport timers run on [`VirtualTime`]; the driver
//! task advances it to the next pending wakeup each time every other task is
//! blocked, so no wall-clock time is ever waited on.
//!
//! Seeds: the gate runs [`GATE_SEEDS`]. Set `FREENET_WAN_BENCH_SEEDS` to a
//! comma-separated list to run others, e.g. to reproduce a nightly failure.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use super::{HostLink, LinkEvent, LinkModel, unique_bytes_between};
use crate::simulation::{TimeSource, VirtualTime};
use crate::transport::congestion_control::{CongestionControlAlgorithm, CongestionControlConfig};
use crate::transport::mock_transport::{Channels, create_mock_peer_with_link_model};
use crate::transport::packet_data::MAX_PACKET_SIZE;
use crate::transport::peer_connection::CongestionProbe;

// ---------------------------------------------------------------------------
// Scenario: the R1 link profile
// ---------------------------------------------------------------------------

/// Stream payload, in bytes.
const PAYLOAD_LEN: usize = 4 * 1024 * 1024;
const ONE_WAY_DELAY: Duration = Duration::from_millis(125);
const LOSS: f64 = 0.01;
const BOTTLENECK_BPS: u64 = 10_000_000;
/// Drop-tail queue capacity in bytes: 100 ms at the bottleneck rate. A
/// well-provisioned uplink, not a bloated one, so a sender overrunning it
/// sees drops.
const QUEUE_CAPACITY: usize = (BOTTLENECK_BPS / 8 / 10) as usize;
/// RTT while the spike lasts.
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

/// The stream must finish within this multiple of its ideal time (design R1).
const COMPLETION_FACTOR: u32 = 3;
/// After the link recovers, goodput over a window must reach this fraction of
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

fn extra_one_way() -> Duration {
    (SPIKE_RTT - base_rtt()) / 2
}

/// The best a sender can do: serialize the payload at the bottleneck rate,
/// plus one round trip, plus the RTT the spike adds (a sender with an
/// unlimited window keeps the bottleneck busy through the spike, so that is
/// all it loses).
fn ideal_completion() -> Duration {
    let serialization = Duration::from_secs_f64(PAYLOAD_LEN as f64 * 8.0 / BOTTLENECK_BPS as f64);
    serialization + base_rtt() + (SPIKE_RTT - base_rtt())
}

/// A sender that restarts from one packet and doubles each RTT reaches the
/// BDP in `ceil(log2(BDP / MSS))` round trips. Allow that, plus the
/// measurement window.
fn recovery_rtts() -> u32 {
    let bdp = BOTTLENECK_BPS as f64 / 8.0 * base_rtt().as_secs_f64();
    (bdp / MAX_PACKET_SIZE as f64).log2().ceil() as u32 + RECOVERY_WINDOW_RTTS
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
    /// moment the spike's last delayed packet arrived.
    recovered_after: Option<Duration>,
    /// Payload still undelivered when the link recovered.
    bytes_left_at_link_recovery: u64,
    pre_spike_goodput_mbps: f64,
    post_recovery_goodput_mbps: f64,
    sender: CongestionProbe,
    receiver: CongestionProbe,
    max_queueing_delay: Duration,
    drops_queue_full: u64,
    drops_random: u64,
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
        let recovery_deadline = base_rtt() * recovery_rtts();
        // The transport finishing before the recovery deadline counts as
        // recovered: nothing was left to measure. `bytes_left_at_link_recovery`
        // in the report shows how much the recovery check actually had to see.
        let finished_in_time = self.completed_after.is_some_and(|t| {
            let link_recovered = SPIKE_AFTER_START + SPIKE_DURATION + extra_one_way();
            t <= link_recovered + recovery_deadline
        });
        if self.recovered_after.is_none() && !finished_in_time {
            out.push(format!(
                "goodput never reached {:.0}% of the bottleneck within {} RTTs ({recovery_deadline:?}) \
                 of the link recovering",
                RECOVERY_FRACTION * 100.0,
                recovery_rtts()
            ));
        }
        if self.sender.outbound_streams_failed > 0 {
            out.push(format!(
                "sender gave up on the stream ({} outbound stream failure(s), e.g. the cwnd-wait timeout)",
                self.sender.outbound_streams_failed
            ));
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
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("runtime");
    rt.block_on(run_inner(algorithm, seed))
}

async fn run_inner(algorithm: CongestionControlAlgorithm, seed: u64) -> Report {
    let vt = VirtualTime::new();
    let driver = tokio::spawn(drive_virtual_time(vt.clone()));
    let link = Arc::new(LinkModel::new(
        HostLink {
            uplink_bps: BOTTLENECK_BPS,
            queue_bytes: QUEUE_CAPACITY,
            one_way_delay: ONE_WAY_DELAY,
            loss: LOSS,
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
        extra_one_way: extra_one_way(),
    });
    let link_recovered = (spike_start + SPIKE_DURATION + extra_one_way()).as_nanos() as u64;

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
    let target = (RECOVERY_FRACTION * BOTTLENECK_BPS as f64 / 8.0
        * Duration::from_nanos(window).as_secs_f64()) as u64;
    let recovery_end = link_recovered + (base_rtt() * recovery_rtts()).as_nanos() as u64;
    let step = Duration::from_millis(10).as_nanos() as u64;
    let recovered_after = (link_recovered + window..=recovery_end)
        .step_by(step as usize)
        .find(|t| unique_bytes_between(&deliveries, t - window, *t) >= target)
        .map(|t| Duration::from_nanos(t - link_recovered));
    let spike_start_nanos = spike_start.as_nanos() as u64;
    let mbps = |from: u64, to: u64| {
        unique_bytes_between(&deliveries, from, to) as f64 * 8.0
            / Duration::from_nanos(to.saturating_sub(from).max(1)).as_secs_f64()
            / 1e6
    };
    let stats = link.host_stats(addr_a);
    Report {
        algorithm,
        seed,
        completed_after,
        recovered_after,
        bytes_left_at_link_recovery: (PAYLOAD_LEN as u64).saturating_sub(unique_bytes_between(
            &deliveries,
            0,
            link_recovered,
        )),
        pre_spike_goodput_mbps: mbps(start, spike_start_nanos),
        post_recovery_goodput_mbps: mbps(link_recovered, recovery_end),
        sender,
        receiver,
        max_queueing_delay: stats.max_queueing_delay,
        drops_queue_full: stats.dropped_queue_full,
        drops_random: stats.dropped_random,
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
             left_at_recovery={}B pre_spike={:.2}Mbit/s post_recovery={:.2}Mbit/s \
             max_queue_delay={:?} drops(queue={}, random={}) sender=[{}] receiver=[{}]",
            r.algorithm.to_string(),
            r.seed,
            r.completed_after,
            ideal_completion() * COMPLETION_FACTOR,
            r.recovered_after,
            r.bytes_left_at_link_recovery,
            r.pre_spike_goodput_mbps,
            r.post_recovery_goodput_mbps,
            r.max_queueing_delay,
            r.drops_queue_full,
            r.drops_random,
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

#[test]
fn r1_spike_recovery_fixed_rate() {
    gate(CongestionControlAlgorithm::FixedRate);
}

// Fails on main (measured 2026-10-18, seeds 0x5eed0001-3): 2 of 3 seeds do not
// finish within the bound and post-spike goodput stays at ~1.4-1.6 Mbit/s of
// 10. Not only the spike: with no loss and no spike BBR reaches ~4 Mbit/s, and
// with 1% loss alone it does not finish within the no-spike bound either.
#[test]
#[ignore = "known failure: BBR does not recover after an RTT spike on a 250 ms / 1% loss / 10 Mbit path (and is loss-limited without one); WAN bench R1 report"]
fn r1_spike_recovery_bbr() {
    gate(CongestionControlAlgorithm::Bbr);
}

// Fails on main (measured 2026-10-18, seeds 0x5eed0001-3): no seed finishes.
// Two abort the stream on the 3 s cwnd-wait timeout during the spike; the
// third survives it at ~0.3 Mbit/s. 1% loss alone holds it to ~0.2-1.2 Mbit/s
// at this RTT, and even the clean path does not finish within the bound.
#[test]
#[ignore = "known failure: LEDBAT++ aborts the stream (cwnd-wait timeout) during a 3.4 s RTT spike and is loss-limited far below the bottleneck at 250 ms RTT; WAN bench R1 report"]
fn r1_spike_recovery_ledbat() {
    gate(CongestionControlAlgorithm::Ledbat);
}
