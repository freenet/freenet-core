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
//! transports, with a queue of `LAB_QUEUE_KIB`, 1024).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use freenet::transport::quic_lab::{
    FRAGMENT_BYTES, LabConfig, LabPair, LabSocket, LinkSpec, StreamsRun, shape_port,
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
    let link = (rate_mbit > 0).then(|| LinkSpec {
        rate_bps: rate_mbit * 1_000_000,
        one_way_delay: Duration::from_millis(delay_ms),
        queue_bytes: queue_kib * 1024,
    });
    // Twice the time the link needs, plus a minute.
    let limit = Duration::from_secs(
        60 + match rate_mbit {
            0 => 60,
            rate => 2 * (u64::from(streams) * stream_bytes as u64 * 8) / (rate * 1_000_000),
        },
    );
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
             rate_mbit={rate_mbit} delay_ms={delay_ms} queue_kib={queue_kib} rep={rep} \
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
        assert_eq!(
            run.log.len(),
            usize::from(streams) * stream_bytes.div_ceil(FRAGMENT_BYTES),
            "every fragment was logged"
        );
    }
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
    (send_tx, got_rx, vec![a_task, b_task, keep])
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
            })
        }
    };
    for transport in &transports {
        for limit in &limits {
            let rt = runtime(threads);
            rt.block_on(async {
                let mut pair = match transport.as_str() {
                    "udp" => {
                        let (send, got, _tasks) = udp_pair(*limit, link).await;
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
