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
//! A measurement, not a regression test: it prints one row per shape, with
//! what became of every send and every received stream, and asserts nothing.
//! The same shape runs over the legacy UDP transport
//! (`udp_streams_in_real_time`) so a stream lost on a starved node can be read
//! against what UDP does there. It takes minutes of real time and several
//! hundred sockets, so it runs only when `FREENET_QUIC_HOLD_REALTIME` is set.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::super::bounded::{BoundedSend, NotOpened, SendDeadlines, SendFailure};
use super::super::hold_trace as trace;
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
    /// QUIC ingress datagrams over the transfer, process-wide (hub and
    /// leaves): (forwarded, queue_full, source_full).
    ingress: (u64, u64, u64),
    /// What each send returned.
    send_outcomes: BTreeMap<String, usize>,
    /// What each received stream came to, and streams that never arrived.
    receive_outcomes: BTreeMap<String, usize>,
}

/// Counts of outcomes by name, shared between tasks.
#[derive(Clone, Default)]
struct Tally(Arc<parking_lot::Mutex<BTreeMap<String, usize>>>);

impl Tally {
    fn add(&self, what: impl Into<String>) {
        *self.0.lock().entry(what.into()).or_default() += 1;
    }

    fn take(&self) -> BTreeMap<String, usize> {
        std::mem::take(&mut *self.0.lock())
    }
}

/// A send's outcome by name. A `Failed(TimedOut)` does not say which
/// deadline passed: with the stream open it is the gap (5 s on loopback,
/// where the round trip is tiny) or the total.
fn send_outcome(outcome: &BoundedSend) -> String {
    match outcome {
        BoundedSend::Sent => "Sent".into(),
        BoundedSend::MaybeDelivered(why) => format!("MaybeDelivered({why:?})"),
        BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut)) => {
            "Failed(NotOpened(TimedOut)) open deadline".into()
        }
        BoundedSend::Failed(SendFailure::NotOpened(NotOpened::ConnectionLost(_))) => {
            "Failed(NotOpened(ConnectionLost))".into()
        }
        BoundedSend::Failed(SendFailure::NotOpened(NotOpened::Paced)) => {
            "Failed(NotOpened(Paced))".into()
        }
        BoundedSend::Failed(SendFailure::TimedOut) => "Failed(TimedOut) gap or total".into(),
        BoundedSend::Failed(SendFailure::Paced) => "Failed(Paced)".into(),
        BoundedSend::Failed(SendFailure::Error(error)) => {
            let mut text = format!("Failed(Error({error:?}))");
            text.truncate(100);
            text
        }
    }
}

/// How long a shape may run before the rest is counted as unfinished.
const SHAPE_LIMIT: Duration = Duration::from_secs(300);

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

        if std::env::var_os("FREENET_QUIC_HOLD_TRACE").is_some() {
            trace::enable();
        }
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
        // DIAGNOSTIC: each link's transport state every 50 ms, on the trace's
        // clock, while tracing.
        let series = (trace::on()).then(|| {
            let on = sampling.clone();
            let pairs: Vec<(Arc<QuicPath>, Arc<QuicPath>)> =
                links.iter().map(|l| (l.0.clone(), l.1.clone())).collect();
            std::thread::spawn(move || {
                let mut rows: Vec<(u64, Vec<LinkSample>)> = Vec::new();
                while on.load(Ordering::Relaxed) {
                    let row = pairs
                        .iter()
                        .map(|(leaf, hub)| {
                            let l = leaf.connection().inner().stats();
                            let h = hub.connection().inner().stats();
                            LinkSample {
                                leaf_tx: l.udp_tx.bytes,
                                leaf_lost: l.path.lost_packets,
                                leaf_cwnd: l.path.cwnd,
                                leaf_rtt_ms: l.path.rtt.as_millis() as u64,
                                leaf_congestion: l.path.congestion_events,
                                hub_rx: h.udp_rx.bytes,
                                hub_tx: h.udp_tx.bytes,
                            }
                        })
                        .collect();
                    rows.push((trace::now_us(), row));
                    std::thread::sleep(Duration::from_millis(50));
                }
                rows
            })
        });
        let link_of: Arc<parking_lot::Mutex<std::collections::HashMap<u32, usize>>> =
            Default::default();
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
        let (send_tally, receive_tally) = (Tally::default(), Tally::default());
        let rcvbuf_before = host_rcvbuf_errors();
        let _ = crate::transport::TRANSPORT_METRICS.take_snapshot();
        let started = std::time::Instant::now();
        let mut work = Vec::new();
        for (link, (sender, _receiver, mut inbound, _sender_in)) in links.into_iter().enumerate() {
            for index in 0..STREAMS_PER_LEAF {
                let (sender, time, tally) = (sender.clone(), time.clone(), send_tally.clone());
                let link_of = link_of.clone();
                work.push(tokio::spawn(async move {
                    let body = Bytes::from(vec![index as u8; STREAM_BYTES]);
                    let id = StreamId::next();
                    link_of.lock().insert(id.0, link);
                    let outcome = sender
                        .send_stream_bounded(
                            &time,
                            SendDeadlines::stream(STREAM_BYTES as u64),
                            id,
                            None,
                            body,
                            None,
                        )
                        .await;
                    tally.add(send_outcome(&outcome));
                    trace::record(
                        0,
                        id.0,
                        trace::Kind::SendReturned {
                            outcome: send_outcome(&outcome),
                        },
                    );
                }));
            }
            let (assembled, tally) = (assembled.clone(), receive_tally.clone());
            work.push(tokio::spawn(async move {
                let mut taken = 0;
                while taken < STREAMS_PER_LEAF {
                    let Some(arrival) = inbound.streams.recv().await else {
                        for _ in taken..STREAMS_PER_LEAF {
                            tally.add("channel closed before it arrived");
                        }
                        break;
                    };
                    taken += 1;
                    let id = arrival.stream_id.0;
                    trace::record(0, id, trace::Kind::ConsumerTook);
                    let assembled_result = arrival.handle.assemble().await;
                    trace::record(
                        0,
                        id,
                        trace::Kind::Assembled {
                            ok: assembled_result.is_ok(),
                        },
                    );
                    match assembled_result {
                        Ok(body) if body.len() == STREAM_BYTES => {
                            assembled.fetch_add(1, Ordering::Relaxed);
                            tally.add("assembled");
                        }
                        Ok(body) => tally.add(format!("assembled {} bytes", body.len())),
                        Err(error) => tally.add(format!("assemble failed: {error:?}")),
                    }
                }
            }));
        }
        let aborts: Vec<_> = work.iter().map(|task| task.abort_handle()).collect();
        let all = futures::future::join_all(work);
        if tokio::time::timeout(SHAPE_LIMIT, all).await.is_err() {
            let unfinished = aborts.iter().filter(|task| !task.is_finished()).count();
            receive_tally.add(format!("tasks unfinished at {SHAPE_LIMIT:?}: {unfinished}"));
            for task in &aborts {
                task.abort();
            }
        }
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
        let ingress = crate::transport::TRANSPORT_METRICS
            .take_snapshot()
            .map_or((0, 0, 0), |m| {
                (
                    m.quic_ingress_forwarded,
                    m.quic_ingress_queue_full,
                    m.quic_ingress_source_full,
                )
            });
        if trace::on() {
            eprintln!(
                "trace: ingress (forwarded, queue_full, source_full) over the transfer: {ingress:?}; host RcvbufErrors delta {:?}",
                rcvbuf_before.zip(host_rcvbuf_errors()).map(|(a, b)| b.saturating_sub(a))
            );
            let rows = series.map(|t| t.join().expect("series")).unwrap_or_default();
            report_trace(trace::take(), &rows, &link_of.lock());
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
            ingress,
            send_outcomes: send_tally.take(),
            receive_outcomes: receive_tally.take(),
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
        if std::env::var_os("FREENET_QUIC_HOLD_SKIP_QUIC").is_none() {
            let run = hold_real_time(connections, workers, hogs);
            eprintln!("3c real-time hold: {run:?}");
        }
        if std::env::var_os("FREENET_QUIC_HOLD_UDP").is_some() {
            let run = udp_streams_in_real_time(connections, workers, hogs);
            eprintln!("3c real-time udp: {run:?}");
        }
    }
}

/// One row of the legacy UDP run.
// Read through `Debug` in the printed rows.
#[allow(dead_code)]
#[derive(Debug)]
struct UdpRun {
    connections: usize,
    workers: usize,
    hogs: usize,
    /// Real time from the first send to the last stream received, or to
    /// [`SHAPE_LIMIT`].
    transfer_ms: u128,
    received: usize,
    sent: usize,
    /// `StreamSendOutcome` of each send, from its completion channel.
    send_outcomes: BTreeMap<String, usize>,
    /// What the receivers' `recv` returned, and streams that never arrived.
    receive_outcomes: BTreeMap<String, usize>,
    /// Streams received by 5, 10, 30, 60, 120 and 300 s.
    received_by: Vec<(u64, usize)>,
    /// The process's UDP transport counters over the transfer
    /// (`TRANSPORT_METRICS`, process-wide: run UDP alone to read them).
    counters: String,
    /// The host's UDP receive-buffer drops over the transfer
    /// (`/proc/net/snmp` RcvbufErrors, host-wide).
    host_rcvbuf_drops: Option<u64>,
}

/// The host's UDP RcvbufErrors count, on Linux.
fn host_rcvbuf_errors() -> Option<u64> {
    let snmp = std::fs::read_to_string("/proc/net/snmp").ok()?;
    let mut rows = snmp.lines().filter(|line| line.starts_with("Udp:"));
    let (names, values) = (rows.next()?, rows.next()?);
    let at = names.split_whitespace().position(|n| n == "RcvbufErrors")?;
    values.split_whitespace().nth(at)?.parse().ok()
}

/// The shape of [`hold_real_time`] over the legacy UDP transport: a hub and
/// `connections` leaves, each on its own real loopback socket (each leaf on
/// its own 127.x address, so the per-IP intro limit does not serialize the
/// handshakes), every leaf sending [`STREAMS_PER_LEAF`] streams of
/// [`STREAM_BYTES`] at once while `hogs` tasks spin on the runtime's
/// `workers`. Default congestion control and no bandwidth limit, as a node.
#[allow(clippy::disallowed_methods)] // a real clock and real threads, on purpose
fn udp_streams_in_real_time(connections: usize, workers: usize, hogs: usize) -> UdpRun {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async move {
        use crate::test_utils::{release_local_port, reserve_local_port_on_ip};
        use crate::transport::StreamSendOutcome;
        use crate::transport::TransportError;
        use crate::transport::UdpTransportSocket;
        use crate::transport::connection_handler::create_connection_handler;
        use crate::transport::crypto::TransportKeypair;
        use std::net::{IpAddr, Ipv4Addr};

        async fn handler(
            ip: Ipv4Addr,
        ) -> (
            TransportKeypair,
            SocketAddr,
            impl Sized,
            crate::transport::connection_handler::OutboundConnectionHandler<
                crate::transport::demux::DemuxSocket<UdpTransportSocket>,
            >,
        ) {
            let keys = TransportKeypair::new();
            let port = reserve_local_port_on_ip(ip).expect("a port");
            release_local_port(port);
            let (out, inbound, task) = create_connection_handler::<UdpTransportSocket>(
                keys.clone(),
                IpAddr::V4(ip),
                port,
                false,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("handler");
            (
                keys,
                SocketAddr::new(IpAddr::V4(ip), port),
                (inbound, task),
                out,
            )
        }

        let (hub_keys, hub_addr, hub_keep, mut hub_out) = handler(Ipv4Addr::LOCALHOST).await;
        let mut keep = vec![];
        let mut pairs = Vec::new();
        for i in 0..connections {
            let ip = Ipv4Addr::new(127, 1 + (i / 250) as u8, (i % 250) as u8 + 2, 1);
            let (keys, addr, leaf_keep, mut leaf_out) = handler(ip).await;
            let (hub_side, leaf_side) = futures::join!(
                hub_out.connect(keys.public().clone(), addr),
                leaf_out.connect(hub_keys.public().clone(), hub_addr),
            );
            let (hub_conn, leaf_conn) = tokio::time::timeout(Duration::from_secs(30), async {
                futures::join!(hub_side, leaf_side)
            })
            .await
            .expect("connected within 30 s");
            pairs.push((
                leaf_conn.expect("leaf connects"),
                hub_conn.expect("hub connects"),
            ));
            keep.push((leaf_keep, leaf_out));
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

        let sent = connections * STREAMS_PER_LEAF;
        let received = Arc::new(AtomicUsize::new(0));
        let (send_tally, receive_tally) = (Tally::default(), Tally::default());
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let _ = crate::transport::TRANSPORT_METRICS.take_snapshot();
        let rcvbuf_before = host_rcvbuf_errors();
        let started = std::time::Instant::now();
        let mut done = Vec::new();
        let mut drivers = Vec::new();
        let mut completions = Vec::new();
        for (mut leaf, mut hub) in pairs {
            let mut stopped = stop_rx.clone();
            let tally = send_tally.clone();
            let (sends_tx, sends_rx) = tokio::sync::oneshot::channel();
            drivers.push(tokio::spawn(async move {
                let mut waits = Vec::new();
                for index in 0..STREAMS_PER_LEAF {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    match leaf
                        .send_serialized_observed(vec![index as u8; STREAM_BYTES], tx)
                        .await
                    {
                        Ok(_) => waits.push(rx),
                        Err(error) => tally.add(format!("send error: {error:?}")),
                    }
                }
                sends_tx.send(waits).ok();
                loop {
                    tokio::select! {
                        result = leaf.recv() => {
                            if let Err(TransportError::ConnectionClosed(_)) = result {
                                break;
                            }
                        }
                        _ = stopped.changed() => break,
                    }
                }
                leaf
            }));
            let tally = send_tally.clone();
            completions.push(tokio::spawn(async move {
                let Ok(waits) = sends_rx.await else { return };
                for wait in waits {
                    match wait.await {
                        Ok(StreamSendOutcome::Delivered) => tally.add("Delivered"),
                        Ok(other) => tally.add(format!("{other:?}")),
                        Err(_) => tally.add("no outcome reported"),
                    }
                }
            }));
            let mut stopped = stop_rx.clone();
            let (received, tally) = (received.clone(), receive_tally.clone());
            let (all_tx, all_rx) = tokio::sync::oneshot::channel::<()>();
            done.push(all_rx);
            drivers.push(tokio::spawn(async move {
                let mut all_tx = Some(all_tx);
                let mut got = 0;
                loop {
                    tokio::select! {
                        result = hub.recv() => match result {
                            Ok(message) if message.len() == STREAM_BYTES => {
                                received.fetch_add(1, Ordering::Relaxed);
                                tally.add("received");
                                got += 1;
                                if got == STREAMS_PER_LEAF {
                                    if let Some(tx) = all_tx.take() {
                                        tx.send(()).ok();
                                    }
                                }
                            }
                            Ok(_) => {}
                            Err(TransportError::ConnectionClosed(_)) => {
                                tally.add("connection closed");
                                break;
                            }
                            Err(error) => {
                                let mut text = format!("recv error: {error:?}");
                                text.truncate(100);
                                tally.add(text);
                            }
                        },
                        _ = stopped.changed() => break,
                    }
                }
                hub
            }));
        }
        let all = futures::future::join_all(done);
        let received_by = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let progress = {
            let (received, received_by) = (received.clone(), received_by.clone());
            tokio::spawn(async move {
                for at in [5u64, 10, 30, 60, 120, 300] {
                    let due = started + Duration::from_secs(at);
                    tokio::time::sleep(due.saturating_duration_since(std::time::Instant::now()))
                        .await;
                    received_by
                        .lock()
                        .push((at, received.load(Ordering::Relaxed)));
                }
            })
        };
        let timed_out = tokio::time::timeout(SHAPE_LIMIT, all).await.is_err();
        let transfer_ms = started.elapsed().as_millis();
        progress.abort();
        let host_rcvbuf_drops = rcvbuf_before
            .zip(host_rcvbuf_errors())
            .map(|(before, after)| after.saturating_sub(before));
        let counters = match crate::transport::TRANSPORT_METRICS.take_snapshot() {
            Some(m) => format!(
                "packets_sent {} retransmitted {} abandoned {} rto_expiries {} tlp_probes {} \
                 inbound_queue_drops {} inbound_queue_depth_hwm {} transfers_failed {}",
                m.packets_sent,
                m.packets_retransmitted,
                m.packets_abandoned,
                m.rto_expiries,
                m.tlp_probes,
                m.inbound_queue_drops,
                m.inbound_queue_depth_hwm,
                m.transfers_failed,
            ),
            None => "no activity".into(),
        };
        // Give send completions a moment to report, then stop everything.
        let completions = futures::future::join_all(completions);
        let _ = tokio::time::timeout(Duration::from_secs(10), completions).await;
        stop_tx.send(true).ok();
        stop.store(true, Ordering::Relaxed);
        let connections_kept = futures::future::join_all(drivers).await;
        for hog in hog_tasks {
            hog.await.ok();
        }
        let received_count = received.load(Ordering::Relaxed);
        if received_count < sent {
            receive_tally.add(format!(
                "never arrived{}",
                if timed_out {
                    " (shape limit reached)"
                } else {
                    ""
                }
            ));
            if let Some(count) = receive_tally.0.lock().get_mut(&format!(
                "never arrived{}",
                if timed_out {
                    " (shape limit reached)"
                } else {
                    ""
                }
            )) {
                *count = sent - received_count;
            }
        }
        drop(connections_kept);
        drop((keep, hub_keep, hub_out));
        UdpRun {
            connections,
            workers,
            hogs,
            transfer_ms,
            received: received_count,
            sent,
            send_outcomes: send_tally.take(),
            receive_outcomes: receive_tally.take(),
            received_by: std::mem::take(&mut *received_by.lock()),
            counters,
            host_rcvbuf_drops,
        }
    })
}

/// Print where the time went for every stream whose consumer did not
/// assemble it, and how often a stream's reads stopped for long at all.
#[derive(Clone, Copy, Debug)]
struct LinkSample {
    leaf_tx: u64,
    leaf_lost: u64,
    leaf_cwnd: u64,
    leaf_rtt_ms: u64,
    leaf_congestion: u64,
    hub_rx: u64,
    hub_tx: u64,
}

fn report_trace(
    events: Vec<trace::Event>,
    rows: &[(u64, Vec<LinkSample>)],
    link_of: &std::collections::HashMap<u32, usize>,
) {
    use trace::Kind;
    let mut by_stream: BTreeMap<u32, Vec<&trace::Event>> = BTreeMap::new();
    let mut poll_gaps: BTreeMap<usize, Vec<&trace::Event>> = BTreeMap::new();
    for event in &events {
        if event.stream == u32::MAX {
            poll_gaps.entry(event.conn).or_default().push(event);
        } else {
            by_stream.entry(event.stream).or_default().push(event);
        }
    }
    // The worst gap between two consecutive reads of each stream, from its
    // first read to its end.
    let mut worst_read_gaps = Vec::new();
    for (id, list) in &by_stream {
        let reads: Vec<u64> = list
            .iter()
            .filter(|e| matches!(e.kind, Kind::Read { .. }))
            .map(|e| e.at_us)
            .collect();
        let worst = reads
            .windows(2)
            .map(|w| (w[1] - w[0], w[0]))
            .max()
            .unwrap_or((0, 0));
        worst_read_gaps.push((worst.0, *id, worst.1));
    }
    worst_read_gaps.sort_unstable();
    let count_over = |us: u64| worst_read_gaps.iter().filter(|g| g.0 >= us).count();
    eprintln!(
        "trace: {} streams, {} events; streams whose reads paused >= 1 s: {}, >= 3 s: {}, >= 5 s: {}; \
         receive-task poll gaps >= 250 ms: {} (largest {} ms)",
        by_stream.len(),
        events.len(),
        count_over(1_000_000),
        count_over(3_000_000),
        count_over(5_000_000),
        poll_gaps.values().map(Vec::len).sum::<usize>(),
        poll_gaps
            .values()
            .flatten()
            .filter_map(|e| match e.kind {
                Kind::PollGap { gap_us } => Some(gap_us / 1000),
                _ => None,
            })
            .max()
            .unwrap_or(0),
    );
    for (gap, id, at) in worst_read_gaps.iter().rev().take(5) {
        eprintln!(
            "trace: longest read pause: stream {id} {} ms from {} ms",
            gap / 1000,
            at / 1000
        );
    }
    for (id, list) in &by_stream {
        let failed = list
            .iter()
            .any(|e| matches!(e.kind, Kind::Assembled { ok: false } | Kind::Failed(_)));
        if !failed {
            continue;
        }
        let conn = list.iter().find(|e| e.conn != 0).map_or(0, |e| e.conn);
        let ms = |us: u64| us / 1000;
        let first =
            |pred: &dyn Fn(&Kind) -> bool| list.iter().find(|e| pred(&e.kind)).map(|e| ms(e.at_us));
        let last = |pred: &dyn Fn(&Kind) -> bool| {
            list.iter()
                .rev()
                .find(|e| pred(&e.kind))
                .map(|e| ms(e.at_us))
        };
        let reads = list
            .iter()
            .filter(|e| matches!(e.kind, Kind::Read { .. }))
            .count();
        let pushes = list
            .iter()
            .filter(|e| matches!(e.kind, Kind::Push { .. }))
            .count();
        eprintln!(
            "trace: FAILED stream {id} conn {conn:x}: accepted {:?} first read {:?} last read {:?} \
             reads {reads} pushes {pushes} last push {:?} waited-for-room {:?} announced {:?} \
             end {:?} send returned {:?} consumer took {:?} assembled {:?}",
            first(&|k| matches!(k, Kind::Accepted)),
            first(&|k| matches!(k, Kind::Read { .. })),
            last(&|k| matches!(k, Kind::Read { .. })),
            last(&|k| matches!(k, Kind::Push { .. })),
            first(&|k| matches!(k, Kind::WaitedForRoom)),
            first(&|k| matches!(k, Kind::Announced)),
            first(&|k| matches!(k, Kind::End)),
            list.iter().find_map(|e| match &e.kind {
                Kind::SendReturned { outcome } => Some((ms(e.at_us), outcome.clone())),
                _ => None,
            }),
            first(&|k| matches!(k, Kind::ConsumerTook)),
            list.iter().find_map(|e| match e.kind {
                Kind::Assembled { ok } => Some((ms(e.at_us), ok)),
                _ => None,
            }),
        );
        // The link's transport state from 1 s before the stream's first long
        // read pause (or its last read) to 1 s after it ends, every 250 ms.
        let reads_at: Vec<u64> = list
            .iter()
            .filter(|e| matches!(e.kind, Kind::Read { .. }))
            .map(|e| e.at_us)
            .collect();
        let pause = reads_at
            .windows(2)
            .map(|w| (w[1] - w[0], w[0], w[1]))
            .max()
            .map(|(_, a, b)| (a, b));
        let window = match (pause, reads_at.last()) {
            (Some((a, b)), _) if b - a >= 1_000_000 => {
                Some((a.saturating_sub(1_000_000), b + 1_000_000))
            }
            (_, Some(&last)) => Some((last.saturating_sub(1_000_000), last + 12_000_000)),
            _ => None,
        };
        if let (Some(&link), Some((from, to))) = (link_of.get(id), window) {
            let mut previous: Option<(u64, LinkSample)> = None;
            for (at, row) in rows.iter().filter(|(at, _)| *at >= from && *at <= to) {
                let now = row[link];
                if let Some((then, before)) = previous {
                    if at - then < 250_000 {
                        continue;
                    }
                    eprintln!(
                        "trace:   link {link} at {} ms: leaf sent +{} B, lost +{}, congestion +{}, cwnd {} B, rtt {} ms; hub received +{} B, hub sent +{} B",
                        at / 1000,
                        now.leaf_tx - before.leaf_tx,
                        now.leaf_lost - before.leaf_lost,
                        now.leaf_congestion - before.leaf_congestion,
                        now.leaf_cwnd,
                        now.leaf_rtt_ms,
                        now.hub_rx - before.hub_rx,
                        now.hub_tx - before.hub_tx,
                    );
                }
                previous = Some((*at, now));
            }
        }
        for event in list.iter().filter(|e| matches!(e.kind, Kind::Failed(_))) {
            eprintln!(
                "trace:   reader failed at {} ms: {:?}",
                ms(event.at_us),
                event.kind
            );
        }
        // Gaps between consecutive reads over 500 ms.
        let reads: Vec<&&trace::Event> = list
            .iter()
            .filter(|e| matches!(e.kind, Kind::Read { .. }))
            .collect();
        for w in reads.windows(2) {
            if w[1].at_us - w[0].at_us >= 500_000 {
                eprintln!(
                    "trace:   read pause {} ms -> {} ms ({:?} then {:?})",
                    ms(w[0].at_us),
                    ms(w[1].at_us),
                    w[0].kind,
                    w[1].kind
                );
            }
        }
        // Other streams of the same connection, and the receive task's gaps.
        let siblings: Vec<String> = by_stream
            .iter()
            .filter(|(other, l)| *other != id && l.iter().any(|e| e.conn == conn))
            .map(|(other, l)| {
                let r: Vec<u64> = l
                    .iter()
                    .filter(|e| matches!(e.kind, Kind::Read { .. }))
                    .map(|e| ms(e.at_us))
                    .collect();
                format!(
                    "{other}: reads {}..{} ms, consumer took {:?}, assembled {:?}",
                    r.first().copied().unwrap_or(0),
                    r.last().copied().unwrap_or(0),
                    l.iter()
                        .find(|e| matches!(e.kind, Kind::ConsumerTook))
                        .map(|e| ms(e.at_us)),
                    l.iter().find_map(|e| match e.kind {
                        Kind::Assembled { ok } => Some((ms(e.at_us), ok)),
                        _ => None,
                    }),
                )
            })
            .collect();
        eprintln!("trace:   same connection: {siblings:?}");
        for gap in poll_gaps.get(&conn).into_iter().flatten() {
            if let Kind::PollGap { gap_us } = gap.kind {
                eprintln!(
                    "trace:   receive task not polled {} ms -> {} ms",
                    ms(gap.at_us - gap_us),
                    ms(gap.at_us)
                );
            }
        }
    }
}
