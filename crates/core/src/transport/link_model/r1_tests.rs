//! R1 and R1b from the WAN transport test bench design: recovery from an RTT
//! spike (the RTO-coupling death spiral).
//!
//! One 8 MiB stream over a 250 ms RTT path with 1% random loss and an 8 Mbit/s
//! bottleneck (below FixedRate's 10 Mbit/s, so even the default controller
//! has to cope with a full queue). Shortly after the transfer starts, data
//! the sender transmits during a 1 s window is held on its outbound leg, so
//! it sees a spiked RTT; ACKs travel the normal path. This is a one-sided hard
//! stall, the shape of a radio handover or a stalled bufferbloated uplink, not
//! a symmetric RTT increase.
//!
//! - **R1** spikes the RTT to 3.4 s, the field's p90 maximum. No ACK returns
//!   for ~3.15 s, longer than the 3 s `CWND_WAIT_TIMEOUT`, so a window-limited
//!   sender aborts the stream: R1 tests stream survival as much as recovery.
//! - **R1b** spikes it to 2.4 s, so the silence (~2.15 s plus up to 100 ms of
//!   ACK delay) stays well inside that timeout. This is the variant that
//!   shows whether a controller recovers once the stream survives.
//!
//! Both drive the real `PeerConnection` send and receive loops over
//! `MockSocket` with the [`LinkModel`] behind it, on a current-thread runtime
//! whose tokio clock is paused. All transport timers run on [`VirtualTime`];
//! the driver task advances it to the next pending wakeup each time every
//! other task is blocked, so no wall-clock time is ever waited on.
//!
//! **Two kinds of gate.** Each seed must pass every binary check: it
//! completes within a loose cap, aborts and abandons nothing, recovers,
//! returns flightsize to 0 when idle, takes no RTT sample below the base RTT,
//! and actually experienced the scenario. Performance is judged across
//! seeds: the median completion time must be within 2x ideal and, given at
//! least 10 seeds (the nightly run), the p90 within 2.2x. A seed is a point
//! in a distribution that small timing changes reshuffle, so one slow seed is
//! not a regression; a collapse is.
//!
//! Each run is a function of its seed: the link draws from its own seeded
//! RNG, and `GlobalRng` (which orders the transport's `deterministic_select!`
//! branches) is seeded per run on the thread that runs every task.
//! `r1_runs_are_reproducible_from_the_seed` checks that.
//!
//! Seeds: the gate runs [`GATE_SEEDS`]. Set `FREENET_WAN_BENCH_SEEDS` to a
//! comma-separated list of decimal or `0x`-prefixed hex seeds to run others,
//! e.g. to reproduce a nightly failure.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use super::{Delivery, HostLink, LinkEvent, LinkModel, Loss, Offer};
use crate::config::GlobalRng;
use crate::simulation::{TimeSource, VirtualTime};
use crate::transport::congestion_control::{CongestionControlAlgorithm, CongestionControlConfig};
use crate::transport::connection_handler::{
    OutboundConnectionHandler, completed_in_remote_inbound,
};
use crate::transport::mock_transport::{Channels, MockSocket, create_mock_peer_with_link_model};
use crate::transport::packet_data::MAX_PACKET_SIZE;
use crate::transport::peer_connection::{CongestionProbe, PeerConnection};
use crate::transport::{Socket, TransportError};

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

/// What varies between R1 and R1b.
struct Scenario {
    name: &'static str,
    /// Stream payload, in bytes. Large enough that a sender at the bottleneck
    /// rate still has data left when the path clears after the spike, so the
    /// recovery check always has something to measure.
    payload_len: usize,
    /// RTT seen by a data packet the sender transmits during the spike.
    spike_rtt: Duration,
}

const R1: Scenario = Scenario {
    name: "R1",
    payload_len: 8 * 1024 * 1024,
    spike_rtt: Duration::from_millis(3400),
};

/// The stall a spike causes at the sender is `spike_rtt - base_rtt` (2.15 s
/// here) plus at most `MAX_CONFIRMATION_DELAY` (100 ms) before the first ACK
/// is batched back: 2.25 s, leaving 0.75 s inside the 3 s cwnd-wait timeout
/// for a lost ACK batch to be followed by the next. 2.4 s is still several
/// times the RTO, so the spike still drives RTO expiries.
const R1B: Scenario = Scenario {
    name: "R1b",
    payload_len: 8 * 1024 * 1024,
    spike_rtt: Duration::from_millis(2400),
};

/// The reproducibility check compares reports, not verdicts, so it uses a
/// shorter transfer that still overlaps the spike.
const REPRO: Scenario = Scenario {
    name: "repro",
    payload_len: 2 * 1024 * 1024,
    spike_rtt: Duration::from_millis(3400),
};

const ONE_WAY_DELAY: Duration = Duration::from_millis(125);
const LOSS: f64 = 0.01;
/// 8 Mbit/s: below FixedRate's fixed 10 Mbit/s, so no controller gets a
/// bottleneck that happens to match its own rate.
const BOTTLENECK_BPS: u64 = 8_000_000;
/// Drop-tail queue capacity in bytes: 100 ms at the bottleneck rate. A
/// well-provisioned uplink, not a bloated one, so a sender overrunning it
/// sees drops.
const QUEUE_CAPACITY: usize = (BOTTLENECK_BPS / 8 / 10) as usize;
/// How long, measured at the bottleneck, packets get the spike's delay. The
/// field figure is the spike's height, not its length; 1 s keeps it a
/// transient that still drives RTT far past the RTO.
const SPIKE_DURATION: Duration = Duration::from_secs(1);
/// When the spike starts, after the stream is handed to the transport.
const SPIKE_AFTER_START: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------------------
// Pass criteria
// ---------------------------------------------------------------------------

/// Per seed: the stream must finish within this multiple of its ideal time.
/// Deliberately loose; it catches a collapse, not a slowdown.
const COMPLETION_CAP: f64 = 3.0;
/// Across seeds: the median completion time over ideal must not exceed this.
const MEDIAN_LIMIT: f64 = 2.0;
/// Across at least `P90_MIN_SEEDS` seeds: nor may the 90th percentile.
const P90_LIMIT: f64 = 2.2;
/// A p90 over fewer seeds is just the maximum, i.e. a per-seed bound again.
const P90_MIN_SEEDS: usize = 10;
/// Once the path clears, goodput over a window must reach this fraction of
/// the bottleneck rate...
const RECOVERY_FRACTION: f64 = 0.5;
/// ...measured over a window of this many base RTTs...
const RECOVERY_WINDOW_RTTS: u32 = 2;
// ...within `recovery_rtts()` base RTTs.
/// One way the spike can show it reached the sender: it retransmitted more
/// than this share of the delivered packets it sent during the window
/// (counted at the link's input, so copies its own queue then dropped still
/// count). Lost ACK batches at 1% loss account for a few percent. This is the
/// evidence when Karn's rule leaves no RTT sample; the other is an RTT sample
/// of the spike's height, which loss cannot produce.
const SPIKE_RETRANSMIT_SHARE: f64 = 0.5;

/// Virtual time to keep both loops running after the stream completes, so
/// the final ACKs land and any lost one is recovered by retransmission,
/// before the idle invariant is checked: 20 minimum RTOs (500 ms), room for
/// several backed-off retransmissions of a lost final ACK.
const IDLE_AFTER_COMPLETION: Duration = Duration::from_secs(10);

/// Fixed seeds for the PR gate. Five, so the median means something.
const GATE_SEEDS: [u64; 5] = [
    0x5eed_0001,
    0x5eed_0002,
    0x5eed_0003,
    0x5eed_0004,
    0x5eed_0005,
];

fn base_rtt() -> Duration {
    ONE_WAY_DELAY * 2
}

impl Scenario {
    /// Added to the sender's outbound leg while the spike lasts.
    fn spike_extra(&self) -> Duration {
        self.spike_rtt - base_rtt()
    }

    /// Offset from the start after which a packet leaving the bottleneck is
    /// no longer held behind the spike's backlog (delivery is FIFO per path,
    /// so everything departing before this arrives with the last spiked
    /// packet).
    fn path_clear(&self) -> Duration {
        SPIKE_AFTER_START + SPIKE_DURATION + self.spike_extra()
    }

    /// The best a sender can do: serialize the payload at the bottleneck
    /// rate, plus one round trip, plus one spike window of bottleneck time.
    /// Any sender whose RTO is below the spike RTT retransmits what it sent
    /// during the window although nothing was lost, and that is at most one
    /// window of bottleneck time.
    fn ideal_completion(&self) -> Duration {
        let serialization =
            Duration::from_secs_f64(self.payload_len as f64 * 8.0 / BOTTLENECK_BPS as f64);
        serialization + base_rtt() + SPIKE_DURATION
    }

    fn completion_cap(&self) -> Duration {
        self.ideal_completion().mul_f64(COMPLETION_CAP)
    }
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
    scenario: &'static str,
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
    /// Share of the delivered packets sent during the spike window that the
    /// sender offered again: ground truth that the spike, not random loss,
    /// drove retransmissions.
    spike_retransmit_share: f64,
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
    /// Completion time over the scenario's ideal, if it completed.
    fn ratio(&self, sc: &Scenario) -> Option<f64> {
        self.completed_after
            .map(|t| t.as_secs_f64() / sc.ideal_completion().as_secs_f64())
    }

    /// Every per-seed binary check this run failed, in words.
    fn failures(&self, sc: &Scenario) -> Vec<String> {
        let mut out = Vec::new();
        let cap = sc.completion_cap();
        if let Some(e) = &self.error {
            out.push(format!("transport error: {e}"));
        }
        match self.completed_after {
            None => out.push(format!("did not complete within {cap:?}")),
            Some(t) if t > cap => out.push(format!("completed in {t:?} > {cap:?}")),
            Some(_) => {}
        }
        // Vacuity guards: the scenario must actually have happened.
        if self.max_one_way_delay < ONE_WAY_DELAY + sc.spike_extra() {
            out.push(format!(
                "no data packet saw the full spike (max one-way {:?}): the scenario did not run",
                self.max_one_way_delay
            ));
        }
        // An RTT sample at the spike's height can only come from the spike:
        // without it, queueing (100 ms) plus ACK delay (100 ms) cap any
        // sample on this path near 0.45 s.
        if self.sender.max_rtt_sample < sc.spike_rtt
            && self.spike_retransmit_share <= SPIKE_RETRANSMIT_SHARE
        {
            out.push(format!(
                "no evidence the spike drove the sender's timers: largest RTT sample {:?} \
                 (spike {:?}) and {:.0}% of what it sent during the window was retransmitted",
                self.sender.max_rtt_sample,
                sc.spike_rtt,
                self.spike_retransmit_share * 100.0
            ));
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
        // because it is byte-identical to the original. Spurious
        // retransmissions are all but certain here (the spike, and lost ACK
        // batches at 1% loss), so seeing none means that identity broke and
        // goodput is overstated.
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

/// Nearest-rank percentile of an ascending slice.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    let rank = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

/// Cross-seed performance checks over every seed that completed. A seed
/// that did not complete has already failed its binary checks.
fn distribution_failures(sc: &Scenario, reports: &[Report]) -> Vec<String> {
    let mut ratios: Vec<f64> = reports.iter().filter_map(|r| r.ratio(sc)).collect();
    if ratios.is_empty() {
        return Vec::new();
    }
    ratios.sort_by(f64::total_cmp);
    let mut out = Vec::new();
    let median = percentile(&ratios, 0.5);
    if median > MEDIAN_LIMIT {
        out.push(format!(
            "median completion is {median:.2}x ideal over {} seeds, limit {MEDIAN_LIMIT}x",
            ratios.len()
        ));
    }
    if ratios.len() >= P90_MIN_SEEDS {
        let p90 = percentile(&ratios, 0.9);
        if p90 > P90_LIMIT {
            out.push(format!(
                "p90 completion is {p90:.2}x ideal over {} seeds, limit {P90_LIMIT}x",
                ratios.len()
            ));
        }
    }
    out
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

fn run(sc: &'static Scenario, algorithm: CongestionControlAlgorithm, seed: u64) -> Report {
    block_on_seeded(seed, run_inner(sc, algorithm, seed))
}

/// Runs `f` to completion on a paused current-thread runtime with
/// `GlobalRng` seeded from `seed`.
fn block_on_seeded<F: Future>(seed: u64, f: F) -> F::Output {
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
    rt.block_on(f)
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

/// Share of the first copies that left the bottleneck in `[from, to)` which
/// the sender offered to the link more than once, counted at the link's
/// input so a retransmission the full queue then dropped still counts. Most
/// are retransmissions of a delivered packet whose ACK was late (or lost);
/// the count also includes a packet whose first offer the queue dropped and
/// whose retransmission is the delivered first copy. That part is bounded by
/// the queue's drop rate, at most ~20% here (FixedRate offers 10 Mbit/s into
/// an 8 Mbit/s bottleneck), so the 0.5 threshold still needs the spike.
fn reoffered_share(deliveries: &[Delivery], offers: &[Offer], from: u64, to: u64) -> f64 {
    let mut offered: HashMap<u64, u32> = HashMap::new();
    for o in offers {
        *offered.entry(o.content_hash).or_default() += 1;
    }
    let sent: Vec<&Delivery> = deliveries
        .iter()
        .filter(|d| d.first_copy && d.departed_at_nanos >= from && d.departed_at_nanos < to)
        .collect();
    if sent.is_empty() {
        return 0.0;
    }
    sent.iter()
        .filter(|d| offered.get(&d.content_hash).copied().unwrap_or(0) > 1)
        .count() as f64
        / sent.len() as f64
}

/// Waits for both ends of a hole punch, driving each connection's receive
/// loop from the moment its own handshake completes, as a node does
/// (`peer_connection_listener` is spawned when the connection is
/// established).
///
/// That loop is part of the handshake. When one side's completion ack is
/// lost, the other side keeps resending its own ack, and completes only when
/// the established connection answers it (`ack_ok` and a receipt, from
/// `PeerConnection::recv`). Awaiting both `connect`s without polling the first
/// connection to finish left the other side to run out its 3 s deadline:
/// at 1% loss, about one seed in 30 failed with "max connection attempts
/// reached" before the transfer began.
async fn connect_pair<S: Socket>(
    a: impl Future<Output = Result<PeerConnection<S, VirtualTime>, TransportError>>,
    b: impl Future<Output = Result<PeerConnection<S, VirtualTime>, TransportError>>,
) -> (
    Result<PeerConnection<S, VirtualTime>, TransportError>,
    Result<PeerConnection<S, VirtualTime>, TransportError>,
) {
    /// The connection's receive loop, or never if it is not established.
    async fn serve<S: Socket>(
        conn: &mut Option<Result<PeerConnection<S, VirtualTime>, TransportError>>,
    ) -> Result<Vec<u8>, TransportError> {
        match conn {
            Some(Ok(c)) => c.recv().await,
            _ => std::future::pending().await,
        }
    }

    let mut a = std::pin::pin!(a);
    let mut b = std::pin::pin!(b);
    let mut conn_a = None;
    let mut conn_b = None;
    while conn_a.is_none() || conn_b.is_none() {
        // Deliberately `biased`, for the same reason as the transfer loop.
        tokio::select! {
            biased;
            r = &mut a, if conn_a.is_none() => conn_a = Some(r),
            r = &mut b, if conn_b.is_none() => conn_b = Some(r),
            r = serve(&mut conn_a) => panic!("a received {r:?} before the transfer began"),
            r = serve(&mut conn_b) => panic!("b received {r:?} before the transfer began"),
        }
    }
    (conn_a.expect("set above"), conn_b.expect("set above"))
}

/// Two peers on the scenario's link, and the outcome of connecting them.
struct Pair {
    link: Arc<LinkModel>,
    addr_a: SocketAddr,
    addr_b: SocketAddr,
    conn_a: Result<PeerConnection<MockSocket, VirtualTime>, TransportError>,
    conn_b: Result<PeerConnection<MockSocket, VirtualTime>, TransportError>,
    /// Owns each peer's listener; dropping it would end the connections.
    _peers: [OutboundConnectionHandler<MockSocket, VirtualTime>; 2],
}

/// Builds the scenario's link for `seed` and hole-punches a connection
/// across it, both peers dialling at once.
async fn open_pair(seed: u64, algorithm: CongestionControlAlgorithm, vt: &VirtualTime) -> Pair {
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
    let (conn_a, conn_b) =
        futures::join!(peer_a.connect(pub_b, addr_b), peer_b.connect(pub_a, addr_a));
    let (conn_a, conn_b) = connect_pair(conn_a, conn_b).await;
    Pair {
        link,
        addr_a,
        addr_b,
        conn_a,
        conn_b,
        _peers: [peer_a, peer_b],
    }
}

async fn run_inner(
    sc: &'static Scenario,
    algorithm: CongestionControlAlgorithm,
    seed: u64,
) -> Report {
    // Asserted directly because outcomes alone do not show it: with this
    // scenario, unseeded runs happened to match too (3 of 3 on 2026-10-18).
    assert!(
        GlobalRng::is_seeded(),
        "GlobalRng must be seeded on the thread that polls the transport"
    );
    let vt = VirtualTime::new();
    let driver = tokio::spawn(drive_virtual_time(vt.clone()));
    let pair = open_pair(seed, algorithm, &vt).await;
    let (link, addr_a, addr_b) = (pair.link.clone(), pair.addr_a, pair.addr_b);
    assert_eq!(link.base_rtt(addr_a, addr_b), base_rtt());
    let mut conn_a = pair.conn_a.expect("connect a");
    let mut conn_b = pair.conn_b.expect("connect b");

    let start = vt.now_nanos();
    let spike_start = Duration::from_nanos(start) + SPIKE_AFTER_START;
    link.schedule(LinkEvent::DelaySpike {
        start: spike_start,
        duration: SPIKE_DURATION,
        extra_delay: sc.spike_extra(),
        from: Some(addr_a),
    });
    let clear = start + sc.path_clear().as_nanos() as u64;

    let payload: Vec<u8> = (0..sc.payload_len)
        .map(|i| (i as u64).wrapping_mul(0x9e37_79b9).wrapping_add(seed) as u8)
        .collect();
    conn_a.send(payload.clone()).await.expect("send");

    // Deliberately `biased`: polling order is fixed so a seed replays the same
    // schedule. Neither recv returns until a message or an error arrives.
    let deadline = start + sc.completion_cap().as_nanos() as u64;
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
    let spike_end_nanos = spike_start_nanos + SPIKE_DURATION.as_nanos() as u64;
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
        scenario: sc.name,
        algorithm,
        seed,
        completed_after,
        recovered_after,
        bytes_left_at_path_clear: (sc.payload_len as u64).saturating_sub(departed_before_clear),
        pre_spike_goodput_mbps: mbps(0, start, spike_start_nanos),
        post_clear_goodput_mbps: mbps(clear, clear, recovery_end),
        max_one_way_delay: deliveries
            .iter()
            .map(|d| Duration::from_nanos(d.arrives_at_nanos - d.departed_at_nanos))
            .max()
            .unwrap_or_default(),
        spike_retransmit_share: reoffered_share(
            &deliveries,
            &link.offers(addr_a, addr_b),
            spike_start_nanos,
            spike_end_nanos,
        ),
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
        "flightsize={} cwnd={cwnd} min_rtt={:?} max_rtt={:?} srtt={:?} rto_expiries={} abandoned={} streams_failed={}",
        p.flightsize,
        p.min_rtt_sample,
        p.max_rtt_sample,
        p.srtt,
        p.rto_expiries,
        p.abandoned_packets,
        p.outbound_streams_failed
    )
}

fn gate(sc: &'static Scenario, algorithm: CongestionControlAlgorithm) {
    let mut reports = Vec::new();
    let mut failed = Vec::new();
    for seed in seeds() {
        // A panic (a connect that fails, a corrupted payload) fails its seed
        // by name and the other seeds still run, instead of ending the test
        // before its verdict with no seed to rerun. The nightly classifier
        // (scripts/wan-bench-classify.sh) parses these `seed 0x..: ` lines;
        // its test pins both format strings.
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(sc, algorithm, seed))) {
            Ok(report) => reports.push(report),
            Err(payload) => failed.push(format!(
                "seed {seed:#x}: panicked: {}",
                panic_message(payload.as_ref())
            )),
        }
    }
    for r in &reports {
        let margin = r
            .ratio(sc)
            .map(|x| format!("{x:.2}x ideal, {:.0}% of cap", x / COMPLETION_CAP * 100.0))
            .unwrap_or_else(|| "-".into());
        eprintln!(
            "{} {:<9} seed={:#x} completed={:?} (ideal {:?}, cap {:?}) margin=[{margin}] \
             recovered_after={:?} left_at_clear={}B pre_spike={:.2}Mbit/s post_clear={:.2}Mbit/s \
             max_one_way={:?} spike_retransmitted={:.0}% duplicates={} max_queue_delay={:?} \
             drops(queue={}, random={}) trace={:#018x} sender=[{}] receiver=[{}]",
            r.scenario,
            r.algorithm.to_string(),
            r.seed,
            r.completed_after,
            sc.ideal_completion(),
            sc.completion_cap(),
            r.recovered_after,
            r.bytes_left_at_path_clear,
            r.pre_spike_goodput_mbps,
            r.post_clear_goodput_mbps,
            r.max_one_way_delay,
            r.spike_retransmit_share * 100.0,
            r.duplicate_deliveries,
            r.max_queueing_delay,
            r.drops_queue_full,
            r.drops_random,
            r.trace_hash,
            describe(&r.sender),
            describe(&r.receiver),
        );
        for f in r.failures(sc) {
            failed.push(format!("seed {:#x}: {f}", r.seed));
        }
    }
    let mut ratios: Vec<f64> = reports.iter().filter_map(|r| r.ratio(sc)).collect();
    ratios.sort_by(f64::total_cmp);
    if !ratios.is_empty() {
        eprintln!(
            "{} {} completion over ideal across {} completed seeds: median {:.2}x (limit {MEDIAN_LIMIT}x), \
             p90 {:.2}x (limit {P90_LIMIT}x from {P90_MIN_SEEDS} seeds), max {:.2}x (cap {COMPLETION_CAP}x)",
            sc.name,
            algorithm,
            ratios.len(),
            percentile(&ratios, 0.5),
            percentile(&ratios, 0.9),
            ratios[ratios.len() - 1],
        );
    }
    for f in distribution_failures(sc, &reports) {
        failed.push(format!("across seeds: {f}"));
    }
    assert!(
        failed.is_empty(),
        "{} failed for {algorithm} (rerun one seed with FREENET_WAN_BENCH_SEEDS=<seed>):\n  {}",
        sc.name,
        failed.join("\n  ")
    );
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|| "(not a string)".into())
}

/// Same seed, same run: compares every field of the report, including a hash
/// over every delivery's timing. A different seed must give a different
/// trace, or the hash is not looking at anything.
#[test]
fn r1_runs_are_reproducible_from_the_seed() {
    let algorithm = CongestionControlAlgorithm::FixedRate;
    let first = format!("{:?}", run(&REPRO, algorithm, GATE_SEEDS[0]));
    let again = format!("{:?}", run(&REPRO, algorithm, GATE_SEEDS[0]));
    assert_eq!(first, again, "same seed, different run");
    let other = run(&REPRO, algorithm, GATE_SEEDS[1]);
    assert!(
        !first.contains(&format!("trace_hash: {},", other.trace_hash)),
        "a different seed produced the same trace"
    );
}

/// Seeds on which the link drops one side's handshake completion ack, so the
/// other side completes only if the first side's established connection
/// answers its repeated ack. Each failed every nightly test before
/// `connect_pair` drove the connections during the handshake.
const LOST_HANDSHAKE_ACK_SEEDS: [u64; 2] = [7244, 7342];

#[test]
fn hole_punch_completes_when_one_side_loses_its_completion_ack() {
    for seed in LOST_HANDSHAKE_ACK_SEEDS {
        block_on_seeded(seed, async {
            let vt = VirtualTime::new();
            let driver = tokio::spawn(drive_virtual_time(vt.clone()));
            let pair = open_pair(seed, CongestionControlAlgorithm::FixedRate, &vt).await;
            driver.abort();
            // The scenario happened: the link dropped B's second packet, its
            // completion ack, and B completed on A's ack. A can then complete
            // only on a packet B's established connection sends in answer to
            // A's repeated ack.
            let b_offers = pair.link.offers(pair.addr_b, pair.addr_a);
            let b_delivered = pair.link.deliveries(pair.addr_b, pair.addr_a);
            assert!(
                b_offers.len() >= 2
                    && !b_delivered
                        .iter()
                        .any(|d| d.content_hash == b_offers[1].content_hash),
                "seed {seed}: the link did not drop B's completion ack, so this seed no longer \
                 exercises the lost-ack handshake"
            );
            assert!(
                completed_in_remote_inbound(pair.addr_b, pair.addr_a),
                "seed {seed}: B did not complete on A's ack, so this seed no longer exercises \
                 the lost-ack handshake"
            );
            assert!(
                pair.conn_a.is_ok(),
                "seed {seed}: connect a: {:?}",
                pair.conn_a.err()
            );
            assert!(
                pair.conn_b.is_ok(),
                "seed {seed}: connect b: {:?}",
                pair.conn_b.err()
            );
        });
    }
}

#[test]
fn r1_spike_recovery_fixed_rate() {
    gate(&R1, CongestionControlAlgorithm::FixedRate);
}

// Fails on main (measured 2026-10-18 on seeds 1-20 and the gate seeds): no
// seed finishes, and 18 of 20 abort the stream on the 3 s cwnd-wait timeout,
// because no ACK returns for the ~3.15 s the spike holds the data leg. Before
// the spike it reaches ~3.3 Mbit/s of 8. Without any spike (earlier 10 Mbit/s
// profile) it reached ~4 Mbit/s clean and was loss-limited at 1% loss.
#[test]
#[ignore = "known failure: BBR aborts the stream (cwnd-wait timeout) during a 3.4 s RTT spike and is slow on this path without one; #5929"]
fn r1_spike_recovery_bbr() {
    gate(&R1, CongestionControlAlgorithm::Bbr);
}

// Fails on main (measured 2026-10-18 on seeds 1-20 and the gate seeds): every
// seed aborts the stream on the 3 s cwnd-wait timeout during the spike. Before
// the spike it reaches ~1.4 Mbit/s of 8; 1% loss alone held it to ~0.2-1.2
// Mbit/s at this RTT on the earlier 10 Mbit/s profile.
#[test]
#[ignore = "known failure: LEDBAT++ aborts the stream (cwnd-wait timeout) during a 3.4 s RTT spike and is loss-limited far below the bottleneck at 250 ms RTT; #5930"]
fn r1_spike_recovery_ledbat() {
    gate(&R1, CongestionControlAlgorithm::Ledbat);
}

#[test]
fn r1b_spike_recovery_fixed_rate() {
    gate(&R1B, CongestionControlAlgorithm::FixedRate);
}

// Fails on main (measured 2026-10-18 on seeds 1-20): the stream survives the
// shorter stall on every seed (no cwnd-wait abort), yet no seed finishes
// within 3x ideal and goodput after the path clears averages ~1.2 Mbit/s of
// 8. So the R1 failure is not only the stream timeout: BBR does not recover.
// It can still abort here (2 of nightly seeds 7220-7379, e.g. 7228): when the
// ACK batch for its initial window is lost, nothing is acknowledged until an
// RTO plus the spike, which outlasts the cwnd-wait timeout.
#[test]
#[ignore = "known failure: BBR never recovers to the bottleneck rate after a 2.4 s RTT spike (~1.2 of 8 Mbit/s after it), and rarely aborts the stream during it; #5929"]
fn r1b_spike_recovery_bbr() {
    gate(&R1B, CongestionControlAlgorithm::Bbr);
}

// Fails on main (measured 2026-10-18 on seeds 1-20): 3 of 20 seeds still
// abort on the cwnd-wait timeout; none finishes within 3x ideal, and goodput
// after the path clears averages ~0.14 Mbit/s of 8 (~1.4 before the spike).
#[test]
#[ignore = "known failure: LEDBAT++ crawls after a 2.4 s RTT spike (~0.14 of 8 Mbit/s) and sometimes still aborts on the cwnd-wait timeout; #5930"]
fn r1b_spike_recovery_ledbat() {
    gate(&R1B, CongestionControlAlgorithm::Ledbat);
}
