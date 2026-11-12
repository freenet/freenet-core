//! Ramp gate (2), real time: what quinn holds for each connection between
//! two polls of our reader, on real loopback sockets with a real clock and
//! the runtime under CPU contention (#5940, `BUFFER-BUDGET-DESIGN` section 6,
//! "unread chunks between polls").
//!
//! `hostile::quinns_hold_between_reader_polls_under_cpu_load` measures the
//! same thing on the simulated network and `VirtualTime`. There a reader's
//! delay is bounded by the virtual clock, which only moves when every task
//! is idle; here it is whatever the scheduler gives it.
//!
//! A measurement, not a regression test: it prints one row per shape and
//! asserts only that every stream arrived. It takes about a minute of real
//! time and several hundred sockets, so it runs only when
//! `FREENET_QUIC_HOLD_REALTIME` is set.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::super::bounded::SendDeadlines;
use super::*;
use crate::simulation::RealTime;
use crate::transport::quic::budget::{QuicBudget, floor_window_bytes};
use crate::transport::quic::config::quic_buffer_budget_for;

/// Node memory for the hub's budget: large enough that a few hundred data
/// paths all get at least the floor window.
const HUB_MEMORY: usize = 8 << 30;

/// Streams each leaf sends at once, and their size.
const STREAMS_PER_LEAF: usize = 2;
const STREAM_BYTES: usize = MIB;

/// How often the sampler reads every hub path, on its own OS thread so the
/// runtime's load does not delay it.
const SAMPLE_EVERY: Duration = Duration::from_millis(1);

fn budget(memory: usize) -> Arc<QuicBudget> {
    QuicBudget::new(
        quic_buffer_budget_for(memory) as u64,
        floor_window_bytes(memory),
        leaked_counters(),
    )
}

/// One row.
// Read through `Debug` in the printed rows.
#[allow(dead_code)]
#[derive(Debug)]
struct RealHoldRun {
    connections: usize,
    workers: usize,
    hogs: usize,
    /// Real time from the first send to the last stream assembled.
    transfer_ms: u128,
    samples: usize,
    /// The largest gap between two samples, which bounds how much of a
    /// short-lived hold the sampler could miss.
    worst_sample_gap_us: u128,
    /// Largest estimated hold of one connection at one sample: received
    /// bytes x the path's payload fraction, less bytes read.
    worst_per_connection: u64,
    /// The same with no payload scaling (received UDP bytes less bytes
    /// read): an upper bound that counts headers and ack-only packets.
    worst_per_connection_raw: u64,
    /// Largest sum over the hub's connections at one sample.
    worst_total: u64,
    worst_total_raw: u64,
    /// 99th percentile over (connection, sample) pairs with a non-zero hold.
    p99_per_connection: u64,
    /// The floor receive window per connection at the hub's memory.
    floor_window_bytes: u64,
    assembled: usize,
    sent: usize,
}

/// A hub endpoint and `connections` leaf endpoints, each on its own real
/// loopback UDP socket, connected, with a metered data path on each side;
/// the hub's paths share one node budget. Every leaf sends
/// [`STREAMS_PER_LEAF`] streams at once while `hogs` tasks spin on the
/// runtime's `workers`.
#[allow(clippy::disallowed_methods)] // a real clock and real threads, on purpose
fn hold_real_time(connections: usize, workers: usize, hogs: usize) -> RealHoldRun {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async move {
        use super::super::super::QuicEndpoint;
        use super::super::super::config::QuicSettings;
        use crate::transport::Socket;
        use crate::transport::UdpTransportSocket;
        use crate::transport::crypto::TransportKeypair;
        use crate::transport::demux::DemuxSocket;
        use crate::transport::packet_data::MAX_RECV_PACKET_SIZE;
        use crate::transport::peer_connection::inbound_budget::SharedInboundBudget;

        let time = RealTime::new();
        let settings = QuicSettings {
            memory_limit: HUB_MEMORY,
            idle_timeout: None,
            initial_window: None,
            rate_limit: Default::default(),
        };
        let mut pumps = Vec::new();
        let bind = |settings: QuicSettings| {
            let time = time.clone();
            async move {
                let any: SocketAddr = "127.0.0.1:0".parse().unwrap();
                let raw = UdpTransportSocket::bind(any).await.expect("bind");
                let addr = raw.local_addr().expect("local addr");
                let socket = Arc::new(DemuxSocket::wrap(raw, None));
                let keys = TransportKeypair::new();
                let endpoint = QuicEndpoint::bind(socket.clone(), addr, time, &settings, &keys)
                    .expect("endpoint");
                let pump = tokio::spawn(async move {
                    let mut buf = vec![0u8; MAX_RECV_PACKET_SIZE];
                    while socket.recv_classified(&mut buf).await.is_ok() {}
                });
                (endpoint, keys, addr, pump)
            }
        };
        let (hub, hub_keys, hub_addr, pump) = bind(settings.clone()).await;
        pumps.push(pump);
        let hub_budget = budget(HUB_MEMORY);
        let leaf_budget = budget(2 << 30);
        let mut links = Vec::new();
        let mut endpoints = Vec::new();
        for _ in 0..connections {
            let (endpoint, keys, addr, pump) = bind(settings.clone()).await;
            pumps.push(pump);
            let permit = hub.allow_peer(addr, keys.public()).expect("allow-set room");
            let hub_key = hub_keys.public().clone();
            let (client, server) = tokio::join!(endpoint.connect(hub_addr, &hub_key), async {
                hub.accept().await.expect("an attempt").establish().await
            });
            drop(permit);
            let limits = QuicPathLimits::DEFAULT;
            let own = || SharedInboundBudget::new(limits.max_streams, limits.max_bytes);
            let (sender, _sender_in) = QuicPath::with_budget_and_own_channels(
                client.expect("client"),
                time.clone(),
                limits,
                own(),
                Some(&leaf_budget),
            );
            sender.enable_data();
            let (receiver, inbound) = QuicPath::with_budget_and_own_channels(
                server.expect("server"),
                time.clone(),
                limits,
                own(),
                Some(&hub_budget),
            );
            receiver.enable_data();
            links.push((Arc::new(sender), Arc::new(receiver), inbound, _sender_in));
            endpoints.push(endpoint);
        }

        let stop = Arc::new(AtomicBool::new(false));
        let mut hog_tasks = Vec::new();
        for _ in 0..hogs {
            let stop = stop.clone();
            hog_tasks.push(tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let until = std::time::Instant::now() + Duration::from_micros(200);
                    while std::time::Instant::now() < until {
                        std::hint::spin_loop();
                    }
                    tokio::task::yield_now().await;
                }
            }));
        }

        let receivers: Vec<Arc<QuicPath>> = links.iter().map(|l| l.1.clone()).collect();
        let sampling = Arc::new(AtomicBool::new(true));
        let sampler = {
            let (on, watched) = (sampling.clone(), receivers.clone());
            std::thread::spawn(move || {
                let mut samples: Vec<(std::time::Instant, Vec<(u64, u64)>)> = Vec::new();
                while on.load(Ordering::Relaxed) {
                    let row = watched
                        .iter()
                        .map(|path| {
                            (
                                path.connection().inner().stats().udp_rx.bytes,
                                path.inbound_read_total().unwrap_or(0),
                            )
                        })
                        .collect();
                    samples.push((std::time::Instant::now(), row));
                    std::thread::sleep(SAMPLE_EVERY);
                }
                samples
            })
        };

        let sent = connections * STREAMS_PER_LEAF;
        let assembled = Arc::new(AtomicUsize::new(0));
        let started = std::time::Instant::now();
        let mut work = Vec::new();
        for (sender, _receiver, mut inbound, _sender_in) in links {
            for index in 0..STREAMS_PER_LEAF {
                let (sender, time) = (sender.clone(), time.clone());
                work.push(tokio::spawn(async move {
                    let body = Bytes::from(vec![index as u8; STREAM_BYTES]);
                    sender
                        .send_stream_bounded(
                            &time,
                            SendDeadlines::stream(STREAM_BYTES as u64),
                            StreamId::next(),
                            None,
                            body,
                            None,
                        )
                        .await;
                }));
            }
            let assembled = assembled.clone();
            work.push(tokio::spawn(async move {
                let mut taken = 0;
                while taken < STREAMS_PER_LEAF {
                    let Some(arrival) = inbound.streams.recv().await else {
                        break;
                    };
                    taken += 1;
                    if arrival
                        .handle
                        .assemble()
                        .await
                        .is_ok_and(|body| body.len() == STREAM_BYTES)
                    {
                        assembled.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        let all = futures::future::join_all(work);
        tokio::time::timeout(Duration::from_secs(300), all)
            .await
            .expect("the transfers finished within 300 s");
        let transfer_ms = started.elapsed().as_millis();
        sampling.store(false, Ordering::Relaxed);
        stop.store(true, Ordering::Relaxed);
        let samples = tokio::task::spawn_blocking(move || sampler.join().expect("sampler"))
            .await
            .expect("join");
        for hog in hog_tasks {
            hog.await.ok();
        }

        let fractions: Vec<f64> = receivers
            .iter()
            .map(|path| {
                let rx = path.connection().inner().stats().udp_rx.bytes.max(1);
                path.inbound_read_total().unwrap_or(0) as f64 / rx as f64
            })
            .collect();
        let mut worst_sample_gap_us = 0u128;
        let (mut worst, mut worst_raw, mut worst_total, mut worst_total_raw) = (0u64, 0, 0, 0);
        let mut nonzero = Vec::new();
        let mut previous: Option<std::time::Instant> = None;
        for (at, row) in &samples {
            if let Some(previous) = previous {
                worst_sample_gap_us =
                    worst_sample_gap_us.max(at.duration_since(previous).as_micros());
            }
            previous = Some(*at);
            let (mut total, mut total_raw) = (0u64, 0u64);
            for (&(rx, read), fraction) in row.iter().zip(&fractions) {
                let hold = ((rx as f64 * fraction) as u64).saturating_sub(read);
                let raw = rx.saturating_sub(read);
                worst = worst.max(hold);
                worst_raw = worst_raw.max(raw);
                total += hold;
                total_raw += raw;
                if hold > 0 {
                    nonzero.push(hold);
                }
            }
            worst_total = worst_total.max(total);
            worst_total_raw = worst_total_raw.max(total_raw);
        }
        nonzero.sort_unstable();
        let p99 = nonzero
            .get(nonzero.len().saturating_sub(1) * 99 / 100)
            .copied()
            .unwrap_or(0);
        let run = RealHoldRun {
            connections,
            workers,
            hogs,
            transfer_ms,
            samples: samples.len(),
            worst_sample_gap_us,
            worst_per_connection: worst,
            worst_per_connection_raw: worst_raw,
            worst_total,
            worst_total_raw,
            p99_per_connection: p99,
            floor_window_bytes: floor_window_bytes(HUB_MEMORY) as u64,
            assembled: assembled.load(Ordering::Relaxed),
            sent,
        };
        for pump in pumps {
            pump.abort();
        }
        drop(endpoints);
        run
    })
}

#[test]
fn quinns_hold_between_reader_polls_in_real_time() {
    if std::env::var_os("FREENET_QUIC_HOLD_REALTIME").is_none() {
        return;
    }
    let shapes: Vec<(usize, usize, usize)> = match std::env::var("FREENET_QUIC_HOLD_SHAPES") {
        Ok(spec) => spec
            .split(';')
            .map(|shape| {
                let n: Vec<usize> = shape
                    .split(',')
                    .map(|n| n.trim().parse().unwrap())
                    .collect();
                (n[0], n[1], n[2])
            })
            .collect(),
        Err(_) => vec![
            (100, 2, 0),
            (100, 2, 4),
            (300, 2, 4),
            (300, 4, 8),
            (300, 1, 2),
        ],
    };
    for (connections, workers, hogs) in shapes {
        let run = hold_real_time(connections, workers, hogs);
        eprintln!("3c real-time hold: {run:?}");
        assert_eq!(run.assembled, run.sent, "{run:?}");
    }
}
