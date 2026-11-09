//! QUIC path lab (e5b-9): stream fairness on a multi-thread runtime over a
//! slow link, and throughput and CPU per byte against the UDP transport.
//!
//! Both tests run in real time on loopback and do nothing unless
//! `FREENET_QUIC_LAB=1` is set, so a plain run of this bench target prints
//! SKIPPED for them. Each prints one `LAB_*` line per measurement.
//!
//! ```bash
//! FREENET_QUIC_LAB=1 LAB_STREAMS=16 LAB_WINDOW_KIB=128 LAB_THREADS=4 \
//!   cargo test --release -p freenet --features bench --bench transport_manual \
//!   quic_lab_fairness -- --nocapture --test-threads=1
//! FREENET_QUIC_LAB=1 cargo test --release -p freenet --features bench \
//!   --bench transport_manual quic_lab_cpu_per_byte -- --nocapture --test-threads=1
//! ```
//!
//! Fairness settings (environment): `LAB_STREAMS` (4), `LAB_STREAM_KIB`
//! (4096), `LAB_WINDOW_KIB` (2048), `LAB_THREADS` (4; 0 is a current-thread
//! runtime), `LAB_REPS` (3), and the link: `LAB_RATE_MBIT` (10; 0 is plain
//! loopback), `LAB_DELAY_MS` one way (64, so 128 ms RTT), `LAB_QUEUE_KIB`
//! (256). `LAB_DUMP` names a directory to write each run's fragment log to,
//! as `stream,microseconds` rows.
//!
//! CPU settings: `LAB_SIZES_KIB` (64,1024,16384), `LAB_LIMITS` (bytes per
//! second per connection, `none` for no `bandwidth-limit`; default
//! `none,500000`), `LAB_MIN_MIB` per measurement (16), `LAB_THREADS` (4),
//! `LAB_REPS` (3), `LAB_TRANSPORTS` (udp,quic), `LAB_LINK` (`none` for plain
//! loopback, or `rate_mbit:one_way_ms` for the same shaped link under both
//! transports, with a queue of `LAB_QUEUE_KIB`, 1024), `LAB_IDLE_S` (0; when
//! set, the CPU an idle connected pair uses over that many seconds).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use freenet::transport::quic_lab::set_connection_gap;
use freenet::transport::quic_lab::{
    Competitor, ConnSample, FRAGMENT_BYTES, LabConfig, LabController, LabMany, LabPair, LabSocket,
    LinkSpec, StreamsRun, link_counts, link_report, set_controller, set_gap_on_connection_progress,
    set_gate, set_initial_window, set_min_window, set_mtu, set_piece_bytes, set_rate_start, set_send_window,
    shape_port,
};
use freenet::transport::{TRANSPORT_METRICS, TransportKeypair, create_connection_handler};
use tokio::sync::mpsc;

fn enabled(test: &str) -> bool {
    if std::env::var("FREENET_QUIC_LAB").as_deref() == Ok("1") {
        return true;
    }
    println!("SKIPPED {test}: set FREENET_QUIC_LAB=1 to run it");
    false
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn runtime(threads: usize) -> tokio::runtime::Runtime {
    if threads == 0 {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
            .unwrap()
    }
}

/// User plus system CPU time of the whole process.
fn process_cpu() -> Duration {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage writes a full rusage into the pointer on success.
    let usage = unsafe {
        assert_eq!(libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()), 0);
        usage.assume_init()
    };
    let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
    tv(usage.ru_utime) + tv(usage.ru_stime)
}

fn ingress_drops() -> (u64, u64) {
    TRANSPORT_METRICS.take_snapshot().map_or((0, 0), |s| {
        (s.quic_ingress_source_full, s.quic_ingress_queue_full)
    })
}

/// What one fairness run showed.
#[derive(Debug, Default)]
struct Fairness {
    /// Widest distance, in fragments of all streams, between two fragments
    /// of one stream: the single-thread test's measure.
    worst_gap_frags: usize,
    /// The median over streams of each stream's widest distance.
    median_gap_frags: usize,
    /// Longest wall-clock wait between two fragments of one stream.
    worst_gap_ms: f64,
    /// Longest wait from the start to a stream's first fragment.
    max_first_ms: f64,
    /// Over 1 s windows in which at least two streams were active for the
    /// whole window: the lowest ratio of a stream's fragments to the
    /// window leader's.
    min_share: f64,
    /// (stream, window) pairs below 5% of the leader.
    starved_windows: usize,
    /// Most consecutive windows one stream spent below 5% of the leader.
    longest_starved_s: usize,
    /// Windows the share was measured over.
    windows: usize,
    first_finish_s: f64,
    last_finish_s: f64,
}

fn analyse(log: &[(u16, Duration)], streams: u16) -> Fairness {
    let n = usize::from(streams);
    let mut positions: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut times: Vec<Vec<Duration>> = vec![Vec::new(); n];
    for (at, (stream, time)) in log.iter().enumerate() {
        positions[usize::from(*stream)].push(at);
        times[usize::from(*stream)].push(*time);
    }
    let mut gaps: Vec<usize> = positions
        .iter()
        .map(|p| p.windows(2).map(|w| w[1] - w[0]).max().unwrap_or(0))
        .collect();
    let worst_gap_frags = gaps.iter().copied().max().unwrap_or(0);
    gaps.sort_unstable();
    let median_gap_frags = gaps.get(n / 2).copied().unwrap_or(0);
    let worst_gap_ms = times
        .iter()
        .flat_map(|t| t.windows(2).map(|w| (w[1] - w[0]).as_secs_f64() * 1000.0))
        .fold(0.0, f64::max);
    let max_first_ms = times
        .iter()
        .filter_map(|t| t.first())
        .map(|t| t.as_secs_f64() * 1000.0)
        .fold(0.0, f64::max);
    let finishes: Vec<f64> = times
        .iter()
        .filter_map(|t| t.last())
        .map(|t| t.as_secs_f64())
        .collect();
    let first_finish_s = finishes.iter().copied().fold(f64::INFINITY, f64::min);
    let last_finish_s = finishes.iter().copied().fold(0.0, f64::max);

    let total_s = last_finish_s.ceil() as usize;
    let mut counts = vec![vec![0usize; total_s.max(1)]; n];
    for (stream, time) in log {
        let w = (time.as_secs_f64() as usize).min(total_s.max(1) - 1);
        counts[usize::from(*stream)][w] += 1;
    }
    let active = |s: usize, w: usize| {
        let (first, last) = (times[s].first(), times[s].last());
        matches!((first, last), (Some(f), Some(l))
            if f.as_secs_f64() <= w as f64 && l.as_secs_f64() >= (w + 1) as f64)
    };
    let mut min_share: f64 = 1.0;
    let mut starved_windows = 0;
    let mut windows = 0;
    let mut runs = vec![0usize; n];
    let mut longest_starved_s = 0;
    #[allow(clippy::needless_range_loop)] // `w` indexes every stream's counts
    for w in 0..total_s {
        let live: Vec<usize> = (0..n).filter(|s| active(*s, w)).collect();
        if live.len() < 2 {
            runs.iter_mut().for_each(|r| *r = 0);
            continue;
        }
        windows += 1;
        let leader = live.iter().map(|s| counts[*s][w]).max().unwrap_or(0).max(1);
        for s in 0..n {
            if !live.contains(&s) {
                runs[s] = 0;
                continue;
            }
            let share = counts[s][w] as f64 / leader as f64;
            min_share = min_share.min(share);
            if share < 0.05 {
                starved_windows += 1;
                runs[s] += 1;
                longest_starved_s = longest_starved_s.max(runs[s]);
            } else {
                runs[s] = 0;
            }
        }
    }
    Fairness {
        worst_gap_frags,
        median_gap_frags,
        worst_gap_ms,
        max_first_ms,
        min_share,
        starved_windows,
        longest_starved_s,
        windows,
        first_finish_s,
        last_finish_s,
    }
}

/// Concurrent large streams on one QUIC path, over a shaped link, on the
/// runtime `LAB_THREADS` names; one `LAB_FAIRNESS` line per repetition.
#[test]
fn quic_lab_fairness() {
    if !enabled("quic_lab_fairness") {
        return;
    }
    let streams: u16 = env_or("LAB_STREAMS", 4);
    let stream_bytes = env_or("LAB_STREAM_KIB", 4096usize) * 1024;
    let window = env_or("LAB_WINDOW_KIB", 2048u64) * 1024;
    let threads: usize = env_or("LAB_THREADS", 4);
    let reps: usize = env_or("LAB_REPS", 3);
    let rate_mbit: u64 = env_or("LAB_RATE_MBIT", 10);
    let delay_ms: u64 = env_or("LAB_DELAY_MS", 64);
    let queue_kib: usize = env_or("LAB_QUEUE_KIB", 256);
    let loss: f64 = env_or("LAB_LOSS", 0.0);
    let seed: u64 = env_or("LAB_SEED", 0);
    let link = (rate_mbit > 0 || loss > 0.0).then(|| LinkSpec {
        rate_bps: rate_mbit * 1_000_000,
        one_way_delay: Duration::from_millis(delay_ms),
        queue_bytes: queue_kib * 1024,
        loss,
        seed,
    });
    // Twice the time the link needs, plus a minute, unless `LAB_LIMIT_S`
    // says otherwise (a lossy link runs far below its rate).
    let limit = Duration::from_secs(env_or(
        "LAB_LIMIT_S",
        60 + match rate_mbit {
            0 => 60,
            rate => 2 * (u64::from(streams) * stream_bytes as u64 * 8) / (rate * 1_000_000),
        },
    ));
    for rep in 0..reps {
        let rt = runtime(threads);
        let (run, describe): (StreamsRun, String) = rt.block_on(async {
            let mut pair = LabPair::connect(LabConfig {
                window_bytes: window,
                bandwidth_limit: None,
                link,
            })
            .await;
            ingress_drops();
            let run = pair.concurrent_streams(streams, stream_bytes, limit).await;
            (run, pair.describe())
        });
        let (source_full, queue_full) = ingress_drops();
        rt.shutdown_timeout(Duration::from_secs(1));
        let f = analyse(&run.log, streams);
        if let Ok(dir) = std::env::var("LAB_DUMP") {
            let path = format!(
                "{dir}/fairness-t{threads}-s{streams}-w{}-r{rep}.csv",
                window / 1024
            );
            let rows: String = run
                .log
                .iter()
                .map(|(stream, at)| format!("{stream},{}\n", at.as_micros()))
                .collect();
            std::fs::write(&path, rows).expect("write the fragment log");
        }
        let failed: Vec<&String> = run.outcomes.iter().filter(|o| *o != "Sent").collect();
        println!(
            "LAB_FAIRNESS threads={threads} streams={streams} stream_kib={} window_kib={} \
             rate_mbit={rate_mbit} delay_ms={delay_ms} queue_kib={queue_kib} loss={loss} rep={rep} \
             all_sent={} received={} fragments={} elapsed_s={:.2} \
             worst_gap_frags={} median_gap_frags={} worst_gap_ms={:.0} max_first_ms={:.0} \
             min_share={:.3} starved_windows={} longest_starved_s={} windows={} \
             first_finish_s={:.2} last_finish_s={:.2} ingress_source_full={source_full} \
             ingress_queue_full={queue_full} failed={failed:?} {describe}",
            stream_bytes / 1024,
            window / 1024,
            run.all_sent,
            run.received,
            run.log.len(),
            run.elapsed.as_secs_f64(),
            f.worst_gap_frags,
            f.median_gap_frags,
            f.worst_gap_ms,
            f.max_first_ms,
            f.min_share,
            f.starved_windows,
            f.longest_starved_s,
            f.windows,
            f.first_finish_s,
            f.last_finish_s,
        );
        // A run that hit its limit logged only what it got through.
        if !run.all_sent {
            continue;
        }
        assert_eq!(
            run.log.len(),
            usize::from(streams) * stream_bytes.div_ceil(FRAGMENT_BYTES),
            "every fragment was logged"
        );
    }
}

/// When the receiver of the latest [`udp_pair`] last got a whole message.
static UDP_LAST_ARRIVAL: std::sync::Mutex<Option<Instant>> = std::sync::Mutex::new(None);

/// The start-of-connection knobs, for every QUIC connection built after the
/// call: `LAB_IW_PKTS` (initial window in 1200-byte datagrams; 0 is quinn's
/// 10), `LAB_RATE_START_KBPS` (a rate start in KB/s, 1000 bytes; 0 is
/// none). Returns them for the result lines.
fn apply_start_knobs() -> String {
    let iw: u64 = env_or("LAB_IW_PKTS", 0);
    set_initial_window((iw > 0).then_some(iw * 1200));
    let rate: u64 = env_or("LAB_RATE_START_KBPS", 0);
    set_rate_start((rate > 0).then_some(rate * 1000));
    let mtu: u16 = env_or("LAB_MTU", 0);
    set_mtu((mtu > 0).then_some(mtu));
    let piece: usize = env_or("LAB_PIECE_BYTES", 0);
    set_piece_bytes((piece > 0).then_some(piece));
    let jitter: u64 = env_or("LAB_JITTER_MS", 0);
    freenet::transport::quic_lab::JITTER_US
        .store(jitter * 1000, std::sync::atomic::Ordering::Relaxed);
    let gate = (
        env_or("LAB_GATE_DELAY_PERMILLE", 100u64),
        env_or("LAB_GATE_MIN_DELAY_US", 2000u64),
        env_or("LAB_GATE_LOSS_CEIL_PERMILLE", 200u64),
    );
    set_gate(gate.0, gate.1, gate.2);
    format!(
        "iw_pkts={iw} rate_start_kbps={rate} mtu={mtu} piece_bytes={piece} gate={}/{}/{} \
         jitter_ms={jitter}",
        gate.0, gate.1, gate.2
    )
}

/// A UDP-transport pair on loopback: send a message with `send`, read each
/// assembled message's length from the receiver.
async fn udp_pair(
    limit: Option<usize>,
    link: Option<LinkSpec>,
) -> (
    mpsc::Sender<Vec<u8>>,
    mpsc::UnboundedReceiver<usize>,
    Vec<tokio::task::JoinHandle<()>>,
    u16,
) {
    let free_port = || {
        std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    };
    let host = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let (b_port, a_port) = (free_port(), free_port());
    if let Some(link) = link {
        shape_port(b_port, link);
        shape_port(a_port, link);
    }
    let b_keys = TransportKeypair::new();
    let b_pub = b_keys.public().clone();
    let (_b_out, mut b_in, b_listener) = create_connection_handler::<LabSocket>(
        b_keys, host, b_port, true, limit, None, None, None, None,
    )
    .await
    .expect("bind b");
    let (mut a_out, _a_in, a_listener) = create_connection_handler::<LabSocket>(
        TransportKeypair::new(),
        host,
        a_port,
        false,
        limit,
        None,
        None,
        None,
        None,
    )
    .await
    .expect("bind a");
    let connecting = a_out.connect(b_pub, SocketAddr::new(host, b_port)).await;
    let (a_conn, b_conn) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(connecting, b_in.next_connection())
    })
    .await
    .expect("connect within 15 s");
    let (mut a_conn, mut b_conn) = (a_conn.expect("a"), b_conn.expect("b accepts"));
    let (send_tx, mut send_rx) = mpsc::channel::<Vec<u8>>(1);
    let (got_tx, got_rx) = mpsc::unbounded_channel();
    let a_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                msg = send_rx.recv() => match msg {
                    Some(msg) => { a_conn.send(msg).await.expect("send"); }
                    None => break,
                },
                got = a_conn.recv() => if got.is_err() { break },
            }
        }
    });
    let b_task = tokio::spawn(async move {
        while let Ok(msg) = b_conn.recv().await {
            *UDP_LAST_ARRIVAL.lock().unwrap() = Some(Instant::now());
            if got_tx.send(msg.len()).is_err() {
                break;
            }
        }
    });
    // The handlers own the listeners' sockets; keep them for the pair's life.
    let keep = tokio::spawn(async move {
        let _keep = (_b_out, b_in, a_out, _a_in);
        std::future::pending::<()>().await;
    });
    // The listeners' handles only report how they ended; dropping them
    // leaves the tasks running.
    drop((a_listener, b_listener));
    (send_tx, got_rx, vec![a_task, b_task, keep], a_port)
}

enum Pair {
    Udp {
        send: mpsc::Sender<Vec<u8>>,
        got: mpsc::UnboundedReceiver<usize>,
        _tasks: Vec<tokio::task::JoinHandle<()>>,
    },
    Quic(LabPair),
}

impl Pair {
    async fn transfer(&mut self, bytes: usize) {
        match self {
            Pair::Udp { send, got, .. } => {
                send.send(vec![0x5Au8; bytes]).await.expect("driver runs");
                let len = tokio::time::timeout(Duration::from_secs(600), got.recv())
                    .await
                    .expect("delivered within 600 s")
                    .expect("receiver runs");
                // bincode prefixes a Vec<u8> with its length.
                assert_eq!(len, bytes + 8);
            }
            Pair::Quic(pair) => {
                pair.transfer(bytes).await;
            }
        }
    }
}

/// Throughput and CPU per byte for one message size at a time, QUIC path
/// against the UDP transport, on loopback; one `LAB_CPU` line per
/// repetition.
#[test]
fn quic_lab_cpu_per_byte() {
    if !enabled("quic_lab_cpu_per_byte") {
        return;
    }
    let sizes: Vec<usize> = std::env::var("LAB_SIZES_KIB")
        .unwrap_or_else(|_| "64,1024,16384".into())
        .split(',')
        .map(|s| s.trim().parse::<usize>().unwrap() * 1024)
        .collect();
    let limits: Vec<Option<usize>> = std::env::var("LAB_LIMITS")
        .unwrap_or_else(|_| "none,500000".into())
        .split(',')
        .map(|s| match s.trim() {
            "none" => None,
            n => Some(n.parse().unwrap()),
        })
        .collect();
    let transports: Vec<String> = std::env::var("LAB_TRANSPORTS")
        .unwrap_or_else(|_| "udp,quic".into())
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    let min_bytes = env_or("LAB_MIN_MIB", 16usize) * 1024 * 1024;
    let threads: usize = env_or("LAB_THREADS", 4);
    let reps: usize = env_or("LAB_REPS", 3);
    let window = env_or("LAB_WINDOW_KIB", 2048u64) * 1024;
    // `rate_mbit:one_way_ms`, the same shaped link under both transports.
    let link_spec = std::env::var("LAB_LINK").unwrap_or_else(|_| "none".into());
    let link = match link_spec.as_str() {
        "none" => None,
        spec => {
            let (rate, delay) = spec.split_once(':').expect("rate_mbit:one_way_ms");
            Some(LinkSpec {
                rate_bps: rate.parse::<u64>().unwrap() * 1_000_000,
                one_way_delay: Duration::from_millis(delay.parse().unwrap()),
                queue_bytes: env_or("LAB_QUEUE_KIB", 1024usize) * 1024,
                loss: 0.0,
                seed: 0,
            })
        }
    };
    for transport in &transports {
        for limit in &limits {
            let rt = runtime(threads);
            rt.block_on(async {
                let mut pair = match transport.as_str() {
                    "udp" => {
                        let (send, got, _tasks, _) = udp_pair(*limit, link).await;
                        Pair::Udp { send, got, _tasks }
                    }
                    "quic" => Pair::Quic(
                        LabPair::connect(LabConfig {
                            window_bytes: window,
                            bandwidth_limit: *limit,
                            link,
                        })
                        .await,
                    ),
                    other => panic!("unknown transport {other}"),
                };
                // CPU the connected pair uses with nothing to send (keepalives,
                // timers, the runtime), so a slower transfer's idle cost can
                // be told apart from its per-byte cost.
                let idle_s: u64 = env_or("LAB_IDLE_S", 0);
                if idle_s > 0 {
                    pair.transfer(1024).await;
                    let cpu0 = process_cpu();
                    tokio::time::sleep(Duration::from_secs(idle_s)).await;
                    let cpu = process_cpu() - cpu0;
                    println!(
                        "LAB_CPU_IDLE transport={transport} threads={threads} link={link_spec} \
                         limit={} idle_s={idle_s} cpu_s={:.3} cpu_ms_per_s={:.2}",
                        limit.map_or("none".to_string(), |l| l.to_string()),
                        cpu.as_secs_f64(),
                        cpu.as_secs_f64() * 1000.0 / idle_s as f64,
                    );
                }
                for &size in &sizes {
                    // One transfer to warm up the path at this size.
                    pair.transfer(size).await;
                    let count = min_bytes.div_ceil(size).max(3);
                    for rep in 0..reps {
                        let (cpu0, wall0) = (process_cpu(), Instant::now());
                        for _ in 0..count {
                            pair.transfer(size).await;
                        }
                        let (cpu, wall) = (process_cpu() - cpu0, wall0.elapsed());
                        let bytes = (size * count) as f64;
                        println!(
                            "LAB_CPU transport={transport} threads={threads} window_kib={} \
                             link={link_spec} limit={} size_kib={} rep={rep} count={count} wall_s={:.3} \
                             bytes_per_s={:.0} cpu_s={:.3} cpu_ns_per_byte={:.2}",
                            window / 1024,
                            limit.map_or("none".to_string(), |l| l.to_string()),
                            size / 1024,
                            wall.as_secs_f64(),
                            bytes / wall.as_secs_f64(),
                            cpu.as_secs_f64(),
                            cpu.as_nanos() as f64 / bytes,
                        );
                    }
                }
                if let Pair::Quic(pair) = &pair {
                    println!("LAB_CPU_PATH limit={limit:?} {}", pair.describe());
                }
            });
            rt.shutdown_timeout(Duration::from_secs(1));
        }
    }
}

/// How one QUIC send in a lossy run ended, from its outcome and the
/// fragment log: `gap` when it failed with fragments still unwritten and
/// the wait since its last fragment reached the 5 s gap; `total` when every
/// fragment was written (the wait for acknowledgements has only the total
/// deadline); `open` when it never opened; `other` otherwise.
fn quic_cause(
    outcome: &str,
    fragments: usize,
    expected: usize,
    since_last: Duration,
) -> &'static str {
    if outcome == "Sent" {
        "sent"
    } else if outcome.starts_with("MaybeDelivered") {
        "maybe_delivered"
    } else if outcome.contains("NotOpened") {
        "open"
    } else if outcome.contains("TimedOut") && fragments < expected {
        if since_last >= Duration::from_millis(4900) {
            "gap"
        } else {
            "timed_out_other"
        }
    } else if outcome.contains("TimedOut") {
        "total"
    } else {
        "other"
    }
}

/// The longest stretch in `samples` up to `until` with no packet newly
/// acknowledged, and how long before `until` the last acknowledgement was.
fn ack_silence(samples: &[ConnSample], until: Duration) -> (Duration, Duration) {
    let mut longest = Duration::ZERO;
    let mut last_change = Duration::ZERO;
    let mut last_acked = 0;
    for sample in samples.iter().take_while(|s| s.at <= until) {
        if sample.acked > last_acked {
            longest = longest.max(sample.at - last_change);
            last_change = sample.at;
            last_acked = sample.acked;
        }
    }
    let since = until.saturating_sub(last_change);
    (longest.max(since), since)
}

/// The sample at or just before `at`.
fn sample_at(samples: &[ConnSample], at: Duration) -> Option<ConnSample> {
    samples.iter().take_while(|s| s.at <= at).last().copied()
}

/// Concurrent streams over a link that loses a share of datagrams both
/// ways, on the QUIC path or (`LAB_TRANSPORT=udp`) on the UDP transport
/// with its own sender, over the same link model and seed. One `LAB_LOSS`
/// line per repetition. Settings: `LAB_TRANSPORT` (quic), `LAB_STREAMS` (48),
/// `LAB_STREAM_KIB` (256), `LAB_WINDOW_KIB` (2048, QUIC only), `LAB_LOSS`
/// (0.10), `LAB_SEED` (1), `LAB_DELAY_MS` (50 one way), `LAB_RATE_MBIT` (0,
/// no bottleneck), `LAB_THREADS` (4), `LAB_REPS` (3), `LAB_CONTROLLER`
/// (cubic, bbr or veno, QUIC only), `LAB_GAP` (stream, the node's rule, or
/// connection: the gap runs from the later of the send's last step and the
/// connection's last newly acknowledged packet), `LAB_FAILURES` (1 prints
/// one `LAB_LOSS_FAILURE` line per failed QUIC send), `LAB_COMPETITOR` (1 runs
/// a loss-based flow on the sender's uplink during the transfer, see
/// `quic_lab::Competitor`; `LAB_TRANSPORT=none` runs it alone for
/// `LAB_ALONE_S`, 30), `LAB_SEND_WINDOW_KIB`
/// (0, the node's: quinn's send window is W), `LAB_MIN_WINDOW_KIB` (0, no
/// floor under the controller's window).
#[test]
fn quic_lab_loss() {
    if !enabled("quic_lab_loss") {
        return;
    }
    let transport = std::env::var("LAB_TRANSPORT").unwrap_or_else(|_| "quic".into());
    let streams: u16 = env_or("LAB_STREAMS", 48);
    let stream_bytes = env_or("LAB_STREAM_KIB", 256usize) * 1024;
    let window = env_or("LAB_WINDOW_KIB", 2048u64) * 1024;
    let loss: f64 = env_or("LAB_LOSS", 0.10);
    let seed: u64 = env_or("LAB_SEED", 1);
    let delay_ms: u64 = env_or("LAB_DELAY_MS", 50);
    let rate_mbit: u64 = env_or("LAB_RATE_MBIT", 0);
    let threads: usize = env_or("LAB_THREADS", 4);
    let reps: usize = env_or("LAB_REPS", 3);
    let link = LinkSpec {
        rate_bps: rate_mbit * 1_000_000,
        one_way_delay: Duration::from_millis(delay_ms),
        queue_bytes: env_or("LAB_QUEUE_KIB", 1024usize) * 1024,
        loss,
        seed,
    };
    let expected = stream_bytes.div_ceil(FRAGMENT_BYTES);
    let controller = std::env::var("LAB_CONTROLLER").unwrap_or_else(|_| "cubic".into());
    set_controller(match controller.as_str() {
        "cubic" => LabController::Cubic,
        "bbr" => LabController::Bbr,
        "veno" => LabController::Veno,
        "westwood" => LabController::Westwood,
        "gated" => LabController::Gated,
        other => panic!("unknown controller {other}"),
    });
    let knobs = apply_start_knobs();
    let gap_rule = std::env::var("LAB_GAP").unwrap_or_else(|_| "stream".into());
    set_gap_on_connection_progress(match gap_rule.as_str() {
        "stream" => false,
        "connection" => true,
        other => panic!("unknown gap rule {other}"),
    });
    let conn_gap_s: u64 = env_or("LAB_CONN_GAP_S", 0);
    set_connection_gap((conn_gap_s > 0).then(|| Duration::from_secs(conn_gap_s)));
    let print_failures = std::env::var("LAB_FAILURES").as_deref() == Ok("1");
    // quinn's send window in KiB, QUIC only; 0 keeps the node's (W).
    let send_window_kib: u64 = env_or("LAB_SEND_WINDOW_KIB", 0);
    set_send_window((send_window_kib > 0).then_some(send_window_kib * 1024));
    // A floor under the controller's window in KiB, QUIC only; 0 is none.
    let min_window_kib: u64 = env_or("LAB_MIN_WINDOW_KIB", 0);
    set_min_window((min_window_kib > 0).then_some(min_window_kib * 1024));
    let competitor = std::env::var("LAB_COMPETITOR").as_deref() == Ok("1");
    // Throughput of the competing flow over `elapsed`, its loss and
    // queueing, and the sender uplink's own figures.
    let side = |comp: &Option<Competitor>, port: Option<u16>, elapsed: Duration| {
        let comp = comp.as_ref().map_or(String::new(), |c| {
            format!(
                "competitor_bytes_per_s={:.0} {} ",
                c.delivered() as f64 / elapsed.as_secs_f64(),
                c.report()
            )
        });
        let link = port
            .and_then(link_report)
            .map_or(String::new(), |l| format!("sender_link[{l}]"));
        format!("{comp}{link}")
    };
    for rep in 0..reps {
        let rt = runtime(threads);
        let line = rt.block_on(async {
            match transport.as_str() {
                "none" => {
                    // The competing flow alone on the same link.
                    let port = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
                        .unwrap()
                        .local_addr()
                        .unwrap()
                        .port();
                    shape_port(port, link);
                    let _socket = <LabSocket as freenet::transport::Socket>::bind(
                        (Ipv4Addr::LOCALHOST, port).into(),
                    )
                    .await
                    .expect("bind");
                    let start = Instant::now();
                    let comp = Competitor::start(port);
                    tokio::time::sleep(Duration::from_secs(env_or("LAB_ALONE_S", 30))).await;
                    format!("alone {}", side(&comp, None, start.elapsed()))
                }
                "quic" => {
                    let mut pair = LabPair::connect(LabConfig {
                        window_bytes: window,
                        bandwidth_limit: None,
                        link: Some(link),
                    })
                    .await;
                    let comp_start = Instant::now();
                    let comp = competitor
                        .then(|| pair.sender_port().and_then(Competitor::start))
                        .flatten();
                    let run = pair
                        .concurrent_streams(streams, stream_bytes, Duration::from_secs(900))
                        .await;
                    let side_line = side(&comp, pair.sender_port(), comp_start.elapsed());
                    drop(comp);
                    let mut per: Vec<Vec<Duration>> = vec![Vec::new(); usize::from(streams)];
                    for (stream, at) in &run.log {
                        per[usize::from(*stream)].push(*at);
                    }
                    let mut causes = std::collections::BTreeMap::<&str, usize>::new();
                    let mut worst_gap = Duration::ZERO;
                    let mut gaps = Vec::new();
                    let mut sent_times = Vec::new();
                    // Each failure: was the connection still being
                    // acknowledged (one stream starved) or silent?
                    let mut starved = 0;
                    let mut silent = 0;
                    let (longest_silence, _) = ack_silence(&run.samples, run.elapsed);
                    for (i, outcome) in run.outcomes.iter().enumerate() {
                        let times = &per[i];
                        let returned = run.returned_at.get(i).copied().unwrap_or(run.elapsed);
                        // The wait before the first fragment counts too.
                        let gap = times
                            .windows(2)
                            .map(|w| w[1] - w[0])
                            .chain(times.first().copied())
                            .max()
                            .unwrap_or(returned);
                        let since_last =
                            returned.saturating_sub(times.last().copied().unwrap_or_default());
                        // A send that failed mid-stream waited from its last
                        // fragment to its end.
                        let gap = if times.len() < expected {
                            gap.max(since_last)
                        } else {
                            gap
                        };
                        worst_gap = worst_gap.max(gap);
                        gaps.push(gap.as_millis());
                        let cause = quic_cause(outcome, times.len(), expected, since_last);
                        *causes.entry(cause).or_default() += 1;
                        if cause != "sent" {
                            let (_, conn_since) = ack_silence(&run.samples, returned);
                            if conn_since >= Duration::from_millis(4900) {
                                silent += 1;
                            } else {
                                starved += 1;
                            }
                            if print_failures {
                                let at = sample_at(&run.samples, returned);
                                println!(
                                    "LAB_LOSS_FAILURE rep={rep} stream={i} outcome={outcome} \
                                     cause={cause} at_s={:.2} fragments={}/{expected} \
                                     stream_gap_ms={} conn_last_ack_ms_ago={} sample={at:?}",
                                    returned.as_secs_f64(),
                                    times.len(),
                                    since_last.as_millis(),
                                    conn_since.as_millis(),
                                );
                            }
                        }
                        if cause == "sent" {
                            sent_times.push(returned.as_secs_f64());
                        }
                    }
                    gaps.sort_unstable();
                    sent_times.sort_by(f64::total_cmp);
                    format!(
                        "received={} causes={causes:?} failures_conn_acking={starved} \
                         failures_conn_silent={silent} longest_ack_silence_ms={} \
                         worst_gap_ms={} median_gap_ms={} \
                         first_sent_s={:.1} last_sent_s={:.1} elapsed_s={:.1} {} {side_line}",
                        run.received,
                        longest_silence.as_millis(),
                        worst_gap.as_millis(),
                        gaps.get(gaps.len() / 2).copied().unwrap_or(0),
                        sent_times.first().copied().unwrap_or(f64::NAN),
                        sent_times.last().copied().unwrap_or(f64::NAN),
                        run.elapsed.as_secs_f64(),
                        pair.describe()
                    )
                }
                "udp" => {
                    let (send, mut got, _tasks, a_port) = udp_pair(None, Some(link)).await;
                    let start = Instant::now();
                    let comp = competitor.then(|| Competitor::start(a_port)).flatten();
                    for _ in 0..streams {
                        send.send(vec![0x5Au8; stream_bytes])
                            .await
                            .expect("driver runs");
                    }
                    let mut arrivals = Vec::new();
                    // Stop at the last message, or once nothing has arrived
                    // for `LAB_UDP_IDLE_S` (30 s, six receiver gaps; streams
                    // that share a slow link all finish near the end, so
                    // raise it there), or after 600 s.
                    let idle = Duration::from_secs(env_or("LAB_UDP_IDLE_S", 30));
                    while arrivals.len() < usize::from(streams)
                        && start.elapsed() < Duration::from_secs(600)
                    {
                        match tokio::time::timeout(idle, got.recv()).await {
                            Ok(Some(len)) => {
                                assert_eq!(len, stream_bytes + 8);
                                arrivals.push(start.elapsed().as_secs_f64());
                            }
                            Ok(None) | Err(_) => break,
                        }
                    }
                    let delivered = arrivals.len();
                    let side_line = side(&comp, Some(a_port), start.elapsed());
                    drop(comp);
                    format!(
                        "received={delivered} not_delivered={} first_s={:.1} last_s={:.1} \
                         elapsed_s={:.1} {side_line}",
                        usize::from(streams) - delivered,
                        arrivals.first().copied().unwrap_or(f64::NAN),
                        arrivals.last().copied().unwrap_or(f64::NAN),
                        start.elapsed().as_secs_f64()
                    )
                }
                other => panic!("unknown transport {other}"),
            }
        });
        rt.shutdown_timeout(Duration::from_secs(1));
        println!(
            "LAB_LOSS transport={transport} threads={threads} streams={streams} stream_kib={} \
             window_kib={} loss={loss} seed={seed} delay_ms={delay_ms} rate_mbit={rate_mbit} \
             controller={controller} {knobs} gap={gap_rule} conn_gap_s={conn_gap_s} send_window_kib={send_window_kib} min_window_kib={min_window_kib} rep={rep} {line}",
            stream_bytes / 1024,
            window / 1024,
        );
    }
}

/// `LAB_CONNS` UDP-transport connections from one sender socket to as many
/// receivers, all over the same link spec: the sender's uplink is one model,
/// so the connections share it.
async fn udp_many(
    n: usize,
    link: LinkSpec,
) -> (
    Vec<mpsc::Sender<Vec<u8>>>,
    mpsc::UnboundedReceiver<(usize, usize)>,
    Vec<tokio::task::JoinHandle<()>>,
    u16,
) {
    let free_port = || {
        std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    };
    let host = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let a_port = free_port();
    shape_port(a_port, link);
    let (mut a_out, _a_in, _a_listener) = create_connection_handler::<LabSocket>(
        TransportKeypair::new(),
        host,
        a_port,
        false,
        None,
        None,
        None,
        None,
        None,
    )
    .await
    .expect("bind a");
    let (got_tx, got_rx) = mpsc::unbounded_channel();
    let mut senders = Vec::new();
    let mut tasks = Vec::new();
    let mut keep_b = Vec::new();
    for index in 0..n {
        let b_port = free_port();
        shape_port(b_port, link);
        let b_keys = TransportKeypair::new();
        let b_pub = b_keys.public().clone();
        let (b_out, mut b_in, _b_listener) = create_connection_handler::<LabSocket>(
            b_keys, host, b_port, true, None, None, None, None, None,
        )
        .await
        .expect("bind b");
        let connecting = a_out.connect(b_pub, SocketAddr::new(host, b_port)).await;
        let (a_conn, b_conn) = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::join!(connecting, b_in.next_connection())
        })
        .await
        .expect("connect within 30 s");
        let (mut a_conn, mut b_conn) = (a_conn.expect("a"), b_conn.expect("b accepts"));
        let (send_tx, mut send_rx) = mpsc::channel::<Vec<u8>>(64);
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = send_rx.recv() => match msg {
                        Some(msg) => { a_conn.send(msg).await.expect("send"); }
                        None => break,
                    },
                    got = a_conn.recv() => if got.is_err() { break },
                }
            }
        }));
        let got_tx = got_tx.clone();
        tasks.push(tokio::spawn(async move {
            while let Ok(msg) = b_conn.recv().await {
                if got_tx.send((index, msg.len())).is_err() {
                    break;
                }
            }
        }));
        senders.push(send_tx);
        keep_b.push((b_out, b_in));
    }
    tasks.push(tokio::spawn(async move {
        let _keep = (a_out, _a_in, keep_b);
        std::future::pending::<()>().await;
    }));
    (senders, got_rx, tasks, a_port)
}

/// Many connections from one node over one slow uplink: `LAB_CONNS` (20)
/// connections, each sending `LAB_STREAMS` (1) messages of `LAB_STREAM_KIB`
/// (256) at once, on the QUIC path (`LAB_TRANSPORT=quic`, with the
/// controller and gap switches of [`quic_lab_loss`]) or the UDP transport.
/// Link: `LAB_RATE_MBIT` (5), `LAB_DELAY_MS` (50) one way, `LAB_QUEUE_KIB`
/// (32), `LAB_LOSS` (0). `LAB_COMPETITOR=1` runs the competing flow on the
/// sender's uplink. One `LAB_MANY` line per repetition.
///
/// With `LAB_GAP=connection` the lab's acknowledgement clock is one per
/// process, so a send's gap counts an acknowledgement on any of the
/// connections: lenient against a per-connection rule.
#[test]
fn quic_lab_many() {
    if !enabled("quic_lab_many") {
        return;
    }
    let transport = std::env::var("LAB_TRANSPORT").unwrap_or_else(|_| "quic".into());
    let conns: usize = env_or("LAB_CONNS", 20);
    let per: usize = env_or("LAB_STREAMS", 1);
    let stream_bytes = env_or("LAB_STREAM_KIB", 256usize) * 1024;
    let window = env_or("LAB_WINDOW_KIB", 384u64) * 1024;
    let loss: f64 = env_or("LAB_LOSS", 0.0);
    let threads: usize = env_or("LAB_THREADS", 4);
    let reps: usize = env_or("LAB_REPS", 3);
    let rate_mbit: u64 = env_or("LAB_RATE_MBIT", 5);
    let link = LinkSpec {
        rate_bps: rate_mbit * 1_000_000,
        one_way_delay: Duration::from_millis(env_or("LAB_DELAY_MS", 50)),
        queue_bytes: env_or("LAB_QUEUE_KIB", 32usize) * 1024,
        loss,
        seed: env_or("LAB_SEED", 1),
    };
    let controller = std::env::var("LAB_CONTROLLER").unwrap_or_else(|_| "cubic".into());
    set_controller(match controller.as_str() {
        "cubic" => LabController::Cubic,
        "bbr" => LabController::Bbr,
        "veno" => LabController::Veno,
        "westwood" => LabController::Westwood,
        "gated" => LabController::Gated,
        other => panic!("unknown controller {other}"),
    });
    let knobs = apply_start_knobs();
    let gap_rule = std::env::var("LAB_GAP").unwrap_or_else(|_| "stream".into());
    set_gap_on_connection_progress(gap_rule == "connection");
    let conn_gap_s: u64 = env_or("LAB_CONN_GAP_S", 0);
    set_connection_gap((conn_gap_s > 0).then(|| Duration::from_secs(conn_gap_s)));
    let min_window_kib: u64 = env_or("LAB_MIN_WINDOW_KIB", 0);
    set_min_window((min_window_kib > 0).then_some(min_window_kib * 1024));
    let competitor = std::env::var("LAB_COMPETITOR").as_deref() == Ok("1");
    let limit = Duration::from_secs(env_or("LAB_LIMIT_S", 400));
    for rep in 0..reps {
        let rt = runtime(threads);
        let line = rt.block_on(async {
            // `elapsed` runs to the last delivery, and the competing flow's
            // bytes are read then, so a wait for messages that never come
            // does not count.
            let (sent, last, outcomes, received, elapsed, port, comp, comp_bytes) =
                match transport.as_str() {
                    "quic" => {
                        let mut many = LabMany::connect(
                            LabConfig {
                                window_bytes: window,
                                bandwidth_limit: None,
                                link: Some(link),
                            },
                            conns,
                        )
                        .await;
                        let comp = competitor
                            .then(|| many.sender_port().and_then(Competitor::start))
                            .flatten();
                        let run = many.send_all(per, stream_bytes, limit).await;
                        let comp_bytes = comp.as_ref().map(Competitor::delivered);
                        (
                            run.sent,
                            run.last_returned,
                            format!("{:?}", run.outcomes),
                            run.received,
                            run.elapsed,
                            many.sender_port(),
                            comp,
                            comp_bytes,
                        )
                    }
                    "udp" => {
                        let (senders, mut got, _tasks, a_port) = udp_many(conns, link).await;
                        let comp = competitor.then(|| Competitor::start(a_port)).flatten();
                        let start = Instant::now();
                        for sender in &senders {
                            for _ in 0..per {
                                sender
                                    .send(vec![0x5Au8; stream_bytes])
                                    .await
                                    .expect("driver runs");
                            }
                        }
                        let mut sent = vec![0usize; conns];
                        let mut last = vec![Duration::ZERO; conns];
                        let mut received = 0;
                        let mut active = Duration::ZERO;
                        let mut comp_bytes = comp.as_ref().map(|_| 0);
                        while received < conns * per && start.elapsed() < limit {
                            match tokio::time::timeout(Duration::from_secs(120), got.recv()).await {
                                Ok(Some((index, _))) => {
                                    sent[index] += 1;
                                    last[index] = start.elapsed();
                                    active = start.elapsed();
                                    comp_bytes = comp.as_ref().map(Competitor::delivered);
                                    received += 1;
                                }
                                Ok(None) | Err(_) => break,
                            }
                        }
                        (
                            sent,
                            last,
                            String::new(),
                            received,
                            active,
                            Some(a_port),
                            comp,
                            comp_bytes,
                        )
                    }
                    other => panic!("unknown transport {other}"),
                };
            let total = conns * per;
            let delivered: usize = sent.iter().sum();
            let complete = sent.iter().filter(|s| **s == per).count();
            let mut done: Vec<f64> = last
                .iter()
                .zip(&sent)
                .filter(|(_, s)| **s == per)
                .map(|(t, _)| t.as_secs_f64())
                .collect();
            done.sort_by(f64::total_cmp);
            let pick = |p: f64| {
                if done.is_empty() {
                    f64::NAN
                } else {
                    done[((done.len() - 1) as f64 * p).round() as usize]
                }
            };
            let comp_line = comp.as_ref().map_or(String::new(), |c| {
                format!(
                    "competitor_bytes_per_s={:.0} {} ",
                    comp_bytes.unwrap_or(0) as f64 / elapsed.as_secs_f64().max(0.001),
                    c.report()
                )
            });
            let link_line = port
                .and_then(link_report)
                .map_or(String::new(), |l| format!("sender_link[{l}]"));
            format!(
                "delivered={delivered}/{total} connections_complete={complete}/{conns} \
                 conn_done_s_p50={:.1} conn_done_s_max={:.1} received={received} elapsed_s={:.1} \
                 outcomes={outcomes} {comp_line}{link_line}",
                pick(0.5),
                pick(1.0),
                elapsed.as_secs_f64(),
            )
        });
        rt.shutdown_timeout(Duration::from_secs(1));
        println!(
            "LAB_MANY transport={transport} controller={controller} {knobs} gap={gap_rule} \
             min_window_kib={min_window_kib} conns={conns} per={per} stream_kib={} \
             window_kib={} rate_mbit={rate_mbit} queue_kib={} loss={loss} competitor={competitor} \
             rep={rep} {line}",
            stream_bytes / 1024,
            window / 1024,
            link.queue_bytes / 1024,
        );
    }
}

/// Field size buckets of streamed sends (count weights, per mille, from the
/// 24 h of `transfer_completed` events on 2026-11-05), each drawn
/// log-uniform within its bounds.
const FIELD_BUCKETS: [(u32, usize, usize); 8] = [
    (226, 600, 1_400),
    (345, 1_400, 16 << 10),
    (304, 16 << 10, 64 << 10),
    (108, 64 << 10, 256 << 10),
    (11, 256 << 10, 1 << 20),
    (5, 1 << 20, 4 << 20),
    (1, 4 << 20, 12 << 20),
    (1, 12 << 20, 30 << 20),
];

/// Streams in flight at a start, per mille: K = 1, 2, 3, 4 and 5+ (taken as 6).
const FIELD_CONCURRENCY: [(u32, usize); 5] = [(957, 1), (39, 2), (2, 3), (1, 4), (1, 6)];

struct Draw(u64);

impl Draw {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn pick<T: Copy>(&mut self, table: &[(u32, T)]) -> T {
        let total: u32 = table.iter().map(|(w, _)| w).sum();
        let mut at = (self.next() % u64::from(total)) as u32;
        for (w, v) in table {
            if at < *w {
                return *v;
            }
            at -= w;
        }
        table[table.len() - 1].1
    }

    fn size(&mut self) -> usize {
        let buckets: Vec<(u32, (usize, usize))> = FIELD_BUCKETS
            .iter()
            .map(|(w, lo, hi)| (*w, (*lo, *hi)))
            .collect();
        let (lo, hi) = self.pick(&buckets);
        let (lo, hi) = ((lo as f64).ln(), (hi as f64).ln());
        (lo + (hi - lo) * self.unit()).exp() as usize
    }
}

fn bucket_of(len: usize) -> &'static str {
    match len {
        0..=1_400 => "<=1.4KB",
        1_401..=16_383 => "1.4-16KiB",
        16_384..=65_535 => "16-64KiB",
        65_536..=262_143 => "64-256KiB",
        262_144..=1_048_575 => "256KiB-1MiB",
        1_048_576..=4_194_303 => "1-4MiB",
        4_194_304..=12_582_911 => "4-12MiB",
        _ => ">=12MiB",
    }
}

/// Replay the field's streamed-send mix on one QUIC path (or the UDP
/// transport) over a lossy link: `LAB_STARTS` (300) starts in a row, each
/// `K` concurrent messages drawn from the field's concurrency and sizes
/// (`FIELD_CONCURRENCY`, `FIELD_BUCKETS`). `LAB_STRESS=1` makes every start
/// one message of 12 to 30 MiB with one or two small ones beside it, for
/// `LAB_STARTS` starts. Prints one `LAB_MIX` line per size bucket with
/// per-message completion times. Link and controller switches as in
/// [`quic_lab_loss`] (default 5% loss, 50 ms one way, no bottleneck).
#[test]
fn quic_lab_mix() {
    if !enabled("quic_lab_mix") {
        return;
    }
    let transport = std::env::var("LAB_TRANSPORT").unwrap_or_else(|_| "quic".into());
    let starts: usize = env_or("LAB_STARTS", 300);
    let stress = std::env::var("LAB_STRESS").as_deref() == Ok("1");
    let window = env_or("LAB_WINDOW_KIB", 384u64) * 1024;
    let loss: f64 = env_or("LAB_LOSS", 0.05);
    let seed: u64 = env_or("LAB_SEED", 1);
    let link = LinkSpec {
        rate_bps: env_or("LAB_RATE_MBIT", 0u64) * 1_000_000,
        one_way_delay: Duration::from_millis(env_or("LAB_DELAY_MS", 50)),
        queue_bytes: env_or("LAB_QUEUE_KIB", 1024usize) * 1024,
        loss,
        seed,
    };
    let controller = std::env::var("LAB_CONTROLLER").unwrap_or_else(|_| "cubic".into());
    set_controller(match controller.as_str() {
        "cubic" => LabController::Cubic,
        "bbr" => LabController::Bbr,
        "veno" => LabController::Veno,
        "westwood" => LabController::Westwood,
        "gated" => LabController::Gated,
        other => panic!("unknown controller {other}"),
    });
    let gap_rule = std::env::var("LAB_GAP").unwrap_or_else(|_| "stream".into());
    set_gap_on_connection_progress(gap_rule == "connection");
    let conn_gap_s: u64 = env_or("LAB_CONN_GAP_S", 0);
    set_connection_gap((conn_gap_s > 0).then(|| Duration::from_secs(conn_gap_s)));
    let min_window_kib: u64 = env_or("LAB_MIN_WINDOW_KIB", 0);
    set_min_window((min_window_kib > 0).then_some(min_window_kib * 1024));
    let mut draw = Draw(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let batches: Vec<Vec<usize>> = (0..starts)
        .map(|_| {
            if stress {
                let big = (12usize << 20) + (draw.next() % (18 << 20)) as usize;
                let small = 1 + (draw.next() % 2) as usize;
                std::iter::once(big)
                    .chain((0..small).map(|_| draw.size().min(256 << 10)))
                    .collect()
            } else {
                let k = draw.pick(&FIELD_CONCURRENCY);
                (0..k).map(|_| draw.size()).collect()
            }
        })
        .collect();
    let rt = runtime(env_or("LAB_THREADS", 4));
    // (size, outcome, seconds from the batch start to the message's end)
    let results: Vec<(usize, String, f64)> = rt.block_on(async {
        let mut results = Vec::new();
        match transport.as_str() {
            "quic" => {
                let pair = LabPair::connect(LabConfig {
                    window_bytes: window,
                    bandwidth_limit: None,
                    link: Some(link),
                })
                .await;
                for batch in &batches {
                    for (len, (outcome, at)) in batch.iter().zip(pair.send_batch(batch).await) {
                        results.push((*len, outcome, at.as_secs_f64()));
                    }
                }
            }
            "udp" => {
                let (send, mut got, _tasks, _) = udp_pair(None, Some(link)).await;
                for batch in &batches {
                    let start = Instant::now();
                    for len in batch {
                        send.send(vec![0x5Au8; *len]).await.expect("driver runs");
                    }
                    let mut pending: Vec<usize> = batch.clone();
                    while !pending.is_empty() {
                        match tokio::time::timeout(Duration::from_secs(60), got.recv()).await {
                            Ok(Some(len)) => {
                                if let Some(at) = pending.iter().position(|p| p + 8 == len) {
                                    let len = pending.swap_remove(at);
                                    results.push((
                                        len,
                                        "Sent".into(),
                                        start.elapsed().as_secs_f64(),
                                    ));
                                }
                            }
                            Ok(None) | Err(_) => break,
                        }
                    }
                    for len in pending {
                        results.push((len, "NotDelivered".into(), start.elapsed().as_secs_f64()));
                    }
                }
            }
            other => panic!("unknown transport {other}"),
        }
        results
    });
    rt.shutdown_timeout(Duration::from_secs(1));
    // Each message of 1 MiB or more, against the fixed 120 s broadcast
    // deadline and a size-scaled one (120 s + size / 64 KB/s).
    for (len, outcome, at) in results.iter().filter(|r| r.0 >= 1 << 20) {
        let scaled = 120.0 + *len as f64 / 64_000.0;
        let sent = outcome == "Sent";
        println!(
            "LAB_MIX_MSG transport={transport} controller={controller} gap={gap_rule} \
             conn_gap_s={conn_gap_s} stress={stress} loss={loss} size_mib={:.2} outcome={outcome} \
             at_s={at:.1} fixed_120s={} scaled_s={scaled:.0} scaled={}",
            *len as f64 / f64::from(1 << 20),
            if sent && *at <= 120.0 {
                "met"
            } else {
                "missed"
            },
            if sent && *at <= scaled {
                "met"
            } else {
                "missed"
            },
        );
    }
    let order = [
        "<=1.4KB",
        "1.4-16KiB",
        "16-64KiB",
        "64-256KiB",
        "256KiB-1MiB",
        "1-4MiB",
        "4-12MiB",
        ">=12MiB",
    ];
    for bucket in order {
        let rows: Vec<&(usize, String, f64)> = results
            .iter()
            .filter(|r| bucket_of(r.0) == bucket)
            .collect();
        if rows.is_empty() {
            continue;
        }
        let mut times: Vec<f64> = rows.iter().filter(|r| r.1 == "Sent").map(|r| r.2).collect();
        times.sort_by(f64::total_cmp);
        let pick = |p: f64| {
            if times.is_empty() {
                f64::NAN
            } else {
                times[((times.len() - 1) as f64 * p).round() as usize]
            }
        };
        let failed: std::collections::BTreeMap<&str, usize> = rows
            .iter()
            .filter(|r| r.1 != "Sent")
            .fold(Default::default(), |mut m, r| {
                *m.entry(r.1.as_str()).or_default() += 1;
                m
            });
        println!(
            "LAB_MIX transport={transport} controller={controller} gap={gap_rule} \
             min_window_kib={min_window_kib} conn_gap_s={conn_gap_s} stress={stress} loss={loss} \
             window_kib={} \
             bucket={bucket} n={} sent={} p50_s={:.2} p99_s={:.2} max_s={:.2} over_120s={} \
             over_300s={} failed={failed:?}",
            window / 1024,
            rows.len(),
            times.len(),
            pick(0.5),
            pick(0.99),
            pick(1.0),
            times.iter().filter(|t| **t > 120.0).count(),
            times.iter().filter(|t| **t > 300.0).count(),
        );
    }
}

/// Time to delivery of one message on a fresh connection (or after warming
/// it), QUIC path against the UDP transport, both timed to the receiver
/// having the whole message. One `LAB_COLD` line per transfer.
///
/// Settings: `LAB_TRANSPORT` (quic or udp), `LAB_SIZES_KIB`
/// (16,64,256,1024,8192), `LAB_DELAYS_MS` one way (25,50,150,250,400),
/// `LAB_LOSS` (0) both ways with `LAB_SEED` (1, plus the repetition),
/// `LAB_RATE_MBIT` (0 for no bottleneck) and `LAB_QUEUE_KIB` (1024),
/// `LAB_WINDOW_KIB` (8192), `LAB_REPS` (3), `LAB_THREADS` (4),
/// `LAB_CONTROLLER` (cubic, bbr), the start knobs of [`apply_start_knobs`],
/// `LAB_MODE`: `cold` (the message is the connection's first),
/// `smalls` (after 50 messages of 1 KiB, 20 ms apart, which leave the window
/// where an application-limited connection keeps it), `warm` (after one 4 MiB
/// transfer and 1 s idle, the window a cached or resumed one would give).
/// `LAB_LIMIT_S` (300) bounds one transfer.
#[test]
fn quic_lab_cold() {
    if !enabled("quic_lab_cold") {
        return;
    }
    let transport = std::env::var("LAB_TRANSPORT").unwrap_or_else(|_| "quic".into());
    let list = |name: &str, default: &str| -> Vec<u64> {
        std::env::var(name)
            .unwrap_or_else(|_| default.into())
            .split(',')
            .map(|v| v.trim().parse().expect("a number"))
            .collect()
    };
    let sizes = list("LAB_SIZES_KIB", "16,64,256,1024,8192");
    let delays = list("LAB_DELAYS_MS", "25,50,150,250,400");
    let loss: f64 = env_or("LAB_LOSS", 0.0);
    let seed: u64 = env_or("LAB_SEED", 1);
    let rate_mbit: u64 = env_or("LAB_RATE_MBIT", 0);
    let queue_kib: usize = env_or("LAB_QUEUE_KIB", 1024);
    let window = env_or("LAB_WINDOW_KIB", 8192u64) * 1024;
    let reps: u64 = env_or("LAB_REPS", 3);
    let threads: usize = env_or("LAB_THREADS", 4);
    let mode = std::env::var("LAB_MODE").unwrap_or_else(|_| "cold".into());
    let limit = Duration::from_secs(env_or("LAB_LIMIT_S", 300));
    let controller = std::env::var("LAB_CONTROLLER").unwrap_or_else(|_| "cubic".into());
    set_controller(match controller.as_str() {
        "cubic" => LabController::Cubic,
        "bbr" => LabController::Bbr,
        "veno" => LabController::Veno,
        "westwood" => LabController::Westwood,
        "gated" => LabController::Gated,
        other => panic!("unknown controller {other}"),
    });
    let knobs = apply_start_knobs();
    for &delay_ms in &delays {
        for &size_kib in &sizes {
            let bytes = size_kib as usize * 1024;
            for rep in 0..reps {
                let link = LinkSpec {
                    rate_bps: rate_mbit * 1_000_000,
                    one_way_delay: Duration::from_millis(delay_ms),
                    queue_bytes: queue_kib * 1024,
                    loss,
                    seed: seed + rep,
                };
                // No delay, rate or loss: plain loopback, no shaping task.
                let shaped = (delay_ms, rate_mbit, loss) != (0, 0, 0.0);
                let link = shaped.then_some(link);
                let rt = runtime(threads);
                let line = rt.block_on(async {
                    match transport.as_str() {
                        "quic" => {
                            let mut pair = LabPair::connect(LabConfig {
                                window_bytes: window,
                                bandwidth_limit: None,
                                link,
                            })
                            .await;
                            match mode.as_str() {
                                "cold" => {}
                                "smalls" => {
                                    for _ in 0..50 {
                                        pair.transfer_timed(1024, limit).await.ok();
                                        tokio::time::sleep(Duration::from_millis(20)).await;
                                    }
                                }
                                "warm" => {
                                    pair.transfer_timed(4 << 20, limit).await.ok();
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                }
                                other => panic!("unknown mode {other}"),
                            }
                            let cwnd_before = pair.cwnd();
                            // A snapshot resets the counters, so the next is this transfer's.
                            ingress_drops();
                            let cpu0 = process_cpu();
                            let result = pair.transfer_timed(bytes, limit).await;
                            let cpu = process_cpu() - cpu0;
                            let (src, queue) = ingress_drops();
                            let drops = format!(
                                "ingress_source_full={src} ingress_queue_full={queue} \
                                 cpu_ns_per_byte={:.1}",
                                cpu.as_nanos() as f64 / bytes as f64
                            );
                            let link = pair
                                .sender_port()
                                .and_then(link_report)
                                .unwrap_or_default();
                            match result {
                                Ok((arrived, done)) => format!(
                                    "ok=1 arrival_s={:.3} sent_s={:.3} cwnd_before={cwnd_before} \
                                     {drops} {} sender_link[{link}]",
                                    arrived.as_secs_f64(),
                                    done.as_secs_f64(),
                                    pair.describe()
                                ),
                                Err(e) => format!(
                                    "ok=0 failure={e:?} cwnd_before={cwnd_before} {} \
                                     sender_link[{link}]",
                                    pair.describe()
                                ),
                            }
                        }
                        "udp" => {
                            let (send, mut got, _tasks, a_port) = udp_pair(None, link).await;
                            let mut wait = async |n: usize| {
                                tokio::time::timeout(limit, got.recv())
                                    .await
                                    .ok()
                                    .flatten()
                                    .map(|len| assert_eq!(len, n + 8))
                                    .is_some()
                            };
                            match mode.as_str() {
                                "cold" => {}
                                "smalls" => {
                                    for _ in 0..50 {
                                        send.send(vec![1u8; 1024]).await.expect("driver");
                                        wait(1024).await;
                                        tokio::time::sleep(Duration::from_millis(20)).await;
                                    }
                                }
                                "warm" => {
                                    send.send(vec![1u8; 4 << 20]).await.expect("driver");
                                    wait(4 << 20).await;
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                }
                                other => panic!("unknown mode {other}"),
                            }
                            *UDP_LAST_ARRIVAL.lock().unwrap() = None;
                            let start = Instant::now();
                            send.send(vec![0x5Au8; bytes]).await.expect("driver runs");
                            let ok = wait(bytes).await;
                            let link = link_report(a_port).unwrap_or_default();
                            if ok {
                                let arrived = UDP_LAST_ARRIVAL
                                    .lock()
                                    .unwrap()
                                    .expect("arrival recorded")
                                    .saturating_duration_since(start);
                                format!(
                                    "ok=1 arrival_s={:.3} sender_link[{link}]",
                                    arrived.as_secs_f64()
                                )
                            } else {
                                format!("ok=0 failure=\"limit\" sender_link[{link}]")
                            }
                        }
                        other => panic!("unknown transport {other}"),
                    }
                });
                rt.shutdown_timeout(Duration::from_secs(1));
                println!(
                    "LAB_COLD transport={transport} controller={controller} {knobs} mode={mode} \
                     size_kib={size_kib} delay_ms={delay_ms} rtt_ms={} loss={loss} \
                     rate_mbit={rate_mbit} queue_kib={queue_kib} window_kib={} rep={rep} {line}",
                    2 * delay_ms,
                    window / 1024,
                );
            }
        }
    }
}

/// A burst of cold connections on an uplink a loss-based flow already uses:
/// how fast the burst finishes, and what it costs that flow. One `LAB_BURST`
/// line per repetition.
///
/// `LAB_CONNS` connections from one sender socket are set up first, then the
/// competing flow (`quic_lab::Competitor`) runs alone for `LAB_WARM_S` (20),
/// then every connection sends one `LAB_STREAM_KIB` (256) message at once.
/// The flow's throughput over the `LAB_MEASURE_S` (5) before the burst is
/// the baseline, against the same length from the burst's start. Link:
/// `LAB_RATE_MBIT` (5), `LAB_DELAY_MS` one way (50), `LAB_QUEUE_KIB` (32),
/// `LAB_LOSS` (0). `LAB_TRANSPORT` quic or udp, `LAB_WINDOW_KIB` (8192), the
/// start knobs of [`apply_start_knobs`], `LAB_REPS` (3).
#[test]
fn quic_lab_burst() {
    if !enabled("quic_lab_burst") {
        return;
    }
    let transport = std::env::var("LAB_TRANSPORT").unwrap_or_else(|_| "quic".into());
    let conns: usize = env_or("LAB_CONNS", 4);
    let stream_bytes = env_or("LAB_STREAM_KIB", 256usize) * 1024;
    let window = env_or("LAB_WINDOW_KIB", 8192u64) * 1024;
    let reps: u64 = env_or("LAB_REPS", 3);
    let threads: usize = env_or("LAB_THREADS", 4);
    let rate_mbit: u64 = env_or("LAB_RATE_MBIT", 5);
    let delay_ms: u64 = env_or("LAB_DELAY_MS", 50);
    let queue_kib: usize = env_or("LAB_QUEUE_KIB", 32);
    let loss: f64 = env_or("LAB_LOSS", 0.0);
    let warm = Duration::from_secs(env_or("LAB_WARM_S", 20));
    let measure = Duration::from_secs(env_or("LAB_MEASURE_S", 5));
    let limit = Duration::from_secs(env_or("LAB_LIMIT_S", 120));
    let controller = std::env::var("LAB_CONTROLLER").unwrap_or_else(|_| "cubic".into());
    set_controller(match controller.as_str() {
        "cubic" => LabController::Cubic,
        "bbr" => LabController::Bbr,
        "veno" => LabController::Veno,
        "westwood" => LabController::Westwood,
        "gated" => LabController::Gated,
        other => panic!("unknown controller {other}"),
    });
    let knobs = apply_start_knobs();
    for rep in 0..reps {
        let link = LinkSpec {
            rate_bps: rate_mbit * 1_000_000,
            one_way_delay: Duration::from_millis(delay_ms),
            queue_bytes: queue_kib * 1024,
            loss,
            seed: 1 + rep,
        };
        let rt = runtime(threads);
        let line = rt.block_on(async {
            enum Side {
                Quic(LabMany),
                Udp(
                    Vec<mpsc::Sender<Vec<u8>>>,
                    mpsc::UnboundedReceiver<(usize, usize)>,
                    Vec<tokio::task::JoinHandle<()>>,
                ),
            }
            let (mut side, port) = match transport.as_str() {
                "quic" => {
                    let many = LabMany::connect(
                        LabConfig {
                            window_bytes: window,
                            bandwidth_limit: None,
                            link: Some(link),
                        },
                        conns,
                    )
                    .await;
                    let port = many.sender_port().expect("shaped");
                    (Side::Quic(many), port)
                }
                "udp" => {
                    let (senders, got, tasks, port) = udp_many(conns, link).await;
                    (Side::Udp(senders, got, tasks), port)
                }
                other => panic!("unknown transport {other}"),
            };
            let comp = Competitor::start(port).expect("shaped");
            tokio::time::sleep(warm - measure).await;
            let base0 = comp.delivered();
            tokio::time::sleep(measure).await;
            let base1 = comp.delivered();
            let (c_off0, c_lost0, c_q0) = comp.counts();
            let (h_off0, h_drop0) = link_counts(port).expect("shaped");
            let start = Instant::now();
            let burst = async {
                match &mut side {
                    Side::Quic(many) => {
                        let run = many.send_all(1, stream_bytes, limit).await;
                        let mut done: Vec<f64> = run
                            .last_returned
                            .iter()
                            .zip(&run.sent)
                            .filter(|(_, s)| **s == 1)
                            .map(|(t, _)| t.as_secs_f64())
                            .collect();
                        done.sort_by(f64::total_cmp);
                        (run.sent.iter().sum::<usize>(), done)
                    }
                    Side::Udp(senders, got, _) => {
                        for sender in senders.iter() {
                            sender
                                .send(vec![0x5Au8; stream_bytes])
                                .await
                                .expect("driver runs");
                        }
                        let mut done = Vec::new();
                        while done.len() < conns && start.elapsed() < limit {
                            match tokio::time::timeout(limit, got.recv()).await {
                                Ok(Some(_)) => done.push(start.elapsed().as_secs_f64()),
                                Ok(None) | Err(_) => break,
                            }
                        }
                        (done.len(), done)
                    }
                }
            };
            let watch = async {
                tokio::time::sleep(measure).await;
                let during = comp.delivered() - base1;
                let (c_off1, c_lost1, _) = comp.counts();
                let (h_off1, h_drop1) = link_counts(port).expect("shaped");
                (
                    during,
                    c_off1 - c_off0,
                    c_lost1 - c_lost0,
                    comp.queue_p90_since(c_q0),
                    h_off1 - h_off0,
                    h_drop1 - h_drop0,
                )
            };
            let ((delivered, done), (during, c_off, c_lost, c_q90, h_off, h_drop)) =
                tokio::join!(burst, watch);
            // Datagrams from the burst's own connections, and those of them
            // the queue dropped, in the measured window.
            let own_off = h_off.saturating_sub(c_off);
            let own_drop = h_drop.saturating_sub(c_lost.min(h_drop));
            let pick = |p: f64| {
                if done.is_empty() {
                    f64::NAN
                } else {
                    done[((done.len() - 1) as f64 * p).round() as usize]
                }
            };
            let secs = measure.as_secs_f64();
            format!(
                "delivered={delivered}/{conns} done_s_p50={:.2} done_s_max={:.2} \
                 comp_base_kBps={:.0} comp_during_kBps={:.0} comp_share={:.2} \
                 comp_loss_during={:.3} comp_queue_p90_ms_during={c_q90:.0} \
                 burst_datagrams={own_off} burst_dropped~={own_drop}",
                pick(0.5),
                pick(1.0),
                (base1 - base0) as f64 / secs / 1000.0,
                during as f64 / secs / 1000.0,
                during as f64 / (base1 - base0).max(1) as f64,
                c_lost as f64 / c_off.max(1) as f64,
            )
        });
        rt.shutdown_timeout(Duration::from_secs(1));
        println!(
            "LAB_BURST transport={transport} controller={controller} {knobs} conns={conns} stream_kib={} \
             rate_mbit={rate_mbit} delay_ms={delay_ms} queue_kib={queue_kib} loss={loss} rep={rep} {line}",
            stream_bytes / 1024,
        );
    }
}
