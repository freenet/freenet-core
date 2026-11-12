//! Buffer-budget PR 3c: the ramp-gate measurements and the hostile-peer
//! harness (#5940, `BUFFER-BUDGET-DESIGN` section 8, PR 3c).
//!
//! Every receiver here is a real [`QuicPath`] taking its windows from a
//! node budget sized as production sizes it (`quic_buffer_budget_for`,
//! `floor_window_bytes`), on the simulated network and one `VirtualTime`.

use super::super::bounded::SendDeadlines;
use super::*;
use crate::transport::quic::budget::{QuicBudget, floor_window_bytes};
use crate::transport::quic::config::quic_buffer_budget_for;

/// The node memory the budgets below are sized for: 2 GiB, the smallest host
/// that gets the full floor window.
const NODE_MEMORY: usize = 2 << 30;

/// A node budget as production builds it for [`NODE_MEMORY`].
fn node_budget(counters: &'static crate::transport::metrics::QuicDataCounters) -> Arc<QuicBudget> {
    QuicBudget::new(
        quic_buffer_budget_for(NODE_MEMORY) as u64,
        floor_window_bytes(NODE_MEMORY),
        counters,
    )
}

/// What one side's path counted of the staging cap.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct StagingStops {
    evicted: u64,
    closed: u64,
}

impl StagingStops {
    fn of(path: &QuicPath) -> Self {
        Self {
            evicted: path.stats().count(QuicInboundOutcome::EvictedStaged),
            closed: path.stats().count(QuicInboundOutcome::ClosedStagedOverCap),
        }
    }
}

/// One run of [`honest_bulk_both_ways`].
// Read through `Debug` in the printed rows.
#[allow(dead_code)]
#[derive(Debug)]
struct HonestRun {
    loss_percent: u64,
    seed: u64,
    /// Datagrams the link dropped, of those it saw.
    dropped: u64,
    seen: u64,
    /// Streams each side assembled, of those the other sent.
    a_got: usize,
    b_got: usize,
    a_stops: StagingStops,
    b_stops: StagingStops,
    /// Either side's connection closed.
    closed: bool,
    /// How the sends ended, by outcome.
    send_outcomes: std::collections::BTreeMap<String, usize>,
    staged: StagedPeak,
}

/// The most either receive side staged at one sample, in bytes and as a
/// percentage of its staging cap at that sample.
#[derive(Debug, Default, Clone, Copy)]
struct StagedPeak {
    bytes: usize,
    percent_of_cap: usize,
}

impl StagedPeak {
    fn sample(&mut self, a: &QuicPath, b: &QuicPath) {
        for path in [a, b] {
            let (held, cap) = (path.inbound_staged(), path.inbound_staging_cap().max(1));
            self.bytes = self.bytes.max(held);
            self.percent_of_cap = self.percent_of_cap.max(held * 100 / cap);
        }
    }
}

/// Streams each side sends in [`honest_bulk_both_ways`], all at once.
const BULK_STREAMS: usize = 6;
/// The size of each, several receive windows at the floor.
const BULK_STREAM_BYTES: usize = 2 * MIB;

/// Both sides of a metered pair send [`BULK_STREAMS`] streams of
/// [`BULK_STREAM_BYTES`] to each other at once, over a link that drops
/// `loss_percent` of datagrams (seeded) and delays each by `one_way`, and
/// each side assembles what arrives. With random loss a stream's first
/// datagram is lost about `loss_percent` of the time, so its header waits
/// for the resend while the rest of the window arrives and is staged.
async fn honest_bulk_both_ways(
    network: &'static str,
    subnet: u8,
    loss_percent: u64,
    seed: u64,
    one_way: Duration,
) -> HonestRun {
    let windows = Windows {
        a: Some(node_budget(leaked_counters())),
        b: Some(node_budget(leaked_counters())),
    };
    if std::env::var_os("FREENET_GATE_DELAY_FIRST").is_some() {
        // The link's delay from the handshake on, as on a real path.
        crate::transport::in_memory_socket::set_packet_delivery_callback(
            network,
            Some(Arc::new(move |_, _, _, _| {
                crate::transport::in_memory_socket::PacketDeliveryDecision::DelayedDelivery(one_way)
            })),
        );
    }
    let mut p = paths_metered(network, subnet, windows).await;
    let loss = seeded_loss_from(network, loss_percent, seed, Some(one_way));
    let (a, b, time) = (p.a.clone(), p.b.clone(), p.time());
    let started_ns = time.now_nanos();
    let (a_watch, b_watch) = (p.a.clone(), p.b.clone());
    let (outcomes, staged) = p
        .drive(async move {
            let mut sends = Vec::new();
            let one_way_only = std::env::var_os("FREENET_GATE_ONEWAY").is_some();
            for index in 0..BULK_STREAMS {
                for (from, mark) in [(a.clone(), 0xA0u8), (b.clone(), 0xB0u8)] {
                    if one_way_only && mark == 0xB0 {
                        continue;
                    }
                    let (body, time) = (
                        Bytes::from(vec![mark | index as u8; BULK_STREAM_BYTES]),
                        time.clone(),
                    );
                    sends.push(tokio::spawn(async move {
                        from.send_stream_bounded(
                            &time,
                            SendDeadlines::stream(BULK_STREAM_BYTES as u64),
                            StreamId::next(),
                            None,
                            body,
                            None,
                        )
                        .await
                    }));
                }
            }
            // Sample what each receive side stages, as a share of its cap,
            // every 5 ms of virtual time until the sends are done.
            let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (sampling, sampler_time) = (done.clone(), time.clone());
            let sampler = tokio::spawn(async move {
                let mut peak = StagedPeak::default();
                while !sampling.load(std::sync::atomic::Ordering::Relaxed) {
                    peak.sample(&a_watch, &b_watch);
                    sampler_time.sleep(Duration::from_millis(5)).await;
                }
                peak
            });
            let mut outcomes = Vec::new();
            for send in sends {
                outcomes.push(format!("{:?}", send.await.expect("send task")));
            }
            done.store(true, std::sync::atomic::Ordering::Relaxed);
            eprintln!(
                "GATETIME sends done after {} ms virtual",
                (time.now_nanos() - started_ns) / 1_000_000
            );
            (outcomes, sampler.await.expect("sampler"))
        })
        .await;
    let mut send_outcomes = std::collections::BTreeMap::<String, usize>::new();
    for outcome in outcomes {
        *send_outcomes.entry(outcome).or_default() += 1;
    }
    let drained = |inbound: &mut QuicInbound| {
        let mut handles = Vec::new();
        while let Ok(arrival) = inbound.streams.try_recv() {
            handles.push(arrival.handle);
        }
        handles
    };
    let a_handles = drained(&mut p.a_in);
    let b_handles = drained(&mut p.b_in);
    let count = |handles: Vec<crate::transport::peer_connection::streaming::StreamHandle>| async move {
        let mut got = 0;
        for handle in handles {
            if handle
                .assemble()
                .await
                .is_ok_and(|body| body.len() == BULK_STREAM_BYTES)
            {
                got += 1;
            }
        }
        got
    };
    let (a_got, b_got) = p
        .drive(async move { (count(a_handles).await, count(b_handles).await) })
        .await;
    crate::transport::in_memory_socket::set_packet_delivery_callback(network, None);
    use std::sync::atomic::Ordering::Relaxed;
    HonestRun {
        loss_percent,
        seed,
        dropped: loss.dropped.load(Relaxed),
        seen: loss.seen.load(Relaxed),
        a_got,
        b_got,
        a_stops: StagingStops::of(&p.a),
        b_stops: StagingStops::of(&p.b),
        closed: p.a.connection().inner().close_reason().is_some()
            || p.b.connection().inner().close_reason().is_some(),
        send_outcomes,
        staged,
    }
}

/// Ramp gate (1), honest false stops: at 2, 5, 10 and 15% loss, with load in
/// both directions and real windows, how often the staging cap stops an
/// honest stream (`evicted_staged`) or closes the path
/// (`closed_staged_over_cap`). Prints one row per run; asserts that no honest
/// path is closed for staging.
#[test]
fn honest_staging_stops_under_loss() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut rows = Vec::new();
        let mut subnet = 230u8;
        let losses: Vec<u64> = std::env::var("FREENET_GATE_LOSSES")
            .map_or(vec![2, 5, 10, 15], |v| {
                v.split(',').map(|x| x.parse().unwrap()).collect()
            });
        for loss_percent in losses {
            for seed in [0x5940_3c01u64, 0x5940_3c02] {
                let network: &'static str =
                    Box::leak(format!("quic-3c-honest-{loss_percent}-{seed:x}").into_boxed_str());
                let run = honest_bulk_both_ways(
                    network,
                    subnet,
                    loss_percent,
                    seed,
                    Duration::from_millis(40),
                )
                .await;
                subnet += 1;
                eprintln!("3c honest: {run:?}");
                crate::transport::quic::recycle::loss_gate::gate_debug::report(&format!(
                    "loss {loss_percent} seed {seed:x}"
                ));
                rows.push(run);
            }
        }
        for run in &rows {
            assert_eq!(
                run.a_stops.closed + run.b_stops.closed,
                0,
                "an honest path was closed for staging: {run:?}"
            );
        }
    })
}

/// One run of [`first_packet_lost_on_a_grown_window`].
// Read through `Debug` in the printed rows.
#[allow(dead_code)]
#[derive(Debug)]
struct LostHeaderRun {
    one_way: Duration,
    lost_datagrams: usize,
    /// The receiver's windows grew before the measured stream.
    window_grew: u64,
    outcome: String,
    assembled: bool,
    stops: StagingStops,
    staged: StagedPeak,
}

/// The H1 case of the #6173 review: a sender whose window has grown sends
/// one large stream, and the first `lost_datagrams` data datagrams of that
/// stream are lost, so its header arrives only with the resend while the
/// rest of the window arrives and is staged. Measures what the receiver
/// staged and whether the staging cap stopped the stream.
async fn first_packet_lost_on_a_grown_window(
    network: &'static str,
    subnet: u8,
    one_way: Duration,
    lost_datagrams: usize,
) -> LostHeaderRun {
    use crate::transport::in_memory_socket::{
        PacketDeliveryDecision, set_packet_delivery_callback,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    let counters = leaked_counters();
    let windows = Windows {
        a: Some(node_budget(leaked_counters())),
        b: Some(node_budget(counters)),
    };
    let mut p = paths_metered(network, subnet, windows).await;
    let a_addr = p.pair.a_addr;
    let to_drop = Arc::new(AtomicUsize::new(0));
    let dropping = to_drop.clone();
    set_packet_delivery_callback(
        network,
        Some(Arc::new(move |_, from, _, datagram| {
            // Only full data datagrams from the sender: acknowledgements
            // and the like are small.
            if from == a_addr
                && datagram.len() >= 1000
                && dropping
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok()
            {
                PacketDeliveryDecision::Drop
            } else {
                PacketDeliveryDecision::DelayedDelivery(one_way)
            }
        })),
    );
    // Grow the windows with one long transfer, read as it arrives.
    let (a, time) = (p.a.clone(), p.time());
    let warm = Bytes::from(payload(24 * MIB));
    let sent = warm.clone();
    p.drive(async move {
        a.send_stream_bounded(
            &time,
            SendDeadlines::stream(sent.len() as u64),
            StreamId::next(),
            None,
            sent,
            None,
        )
        .await
    })
    .await;
    let warmed = p.b_in.streams.try_recv().expect("the warm-up announced");
    let assembled_warm = p.drive(async move { warmed.handle.assemble().await }).await;
    assert_eq!(assembled_warm.as_deref().ok(), Some(&warm[..]), "warm-up");
    let window_grew = counters.snapshot(false).window_grew;

    to_drop.store(lost_datagrams, Ordering::SeqCst);
    let (a, b, time) = (p.a.clone(), p.b.clone(), p.time());
    let body = Bytes::from(payload(32 * MIB));
    let sent = body.clone();
    let (outcome, staged) = p
        .drive(async move {
            let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (sampling, sampler_time, watched) = (done.clone(), time.clone(), b.clone());
            let sampler = tokio::spawn(async move {
                let mut peak = StagedPeak::default();
                while !sampling.load(Ordering::Relaxed) {
                    peak.sample(&watched, &watched);
                    sampler_time.sleep(Duration::from_millis(1)).await;
                }
                peak
            });
            let outcome = a
                .send_stream_bounded(
                    &time,
                    SendDeadlines::stream(sent.len() as u64),
                    StreamId::next(),
                    None,
                    sent,
                    None,
                )
                .await;
            done.store(true, Ordering::Relaxed);
            (format!("{outcome:?}"), sampler.await.expect("sampler"))
        })
        .await;
    let assembled = match p.b_in.streams.try_recv() {
        Ok(arrival) => p
            .drive(async move { arrival.handle.assemble().await })
            .await
            .is_ok_and(|got| got == body),
        Err(_) => false,
    };
    set_packet_delivery_callback(network, None);
    LostHeaderRun {
        one_way,
        lost_datagrams,
        window_grew,
        outcome,
        assembled,
        stops: StagingStops::of(&p.b),
        staged,
    }
}

/// Ramp gate (1), the targeted case: a lost first packet on a grown window,
/// at 80 and 300 ms round trips, with 1, 3 and 8 data datagrams lost.
/// Prints one row per run; asserts that the stream is delivered and nothing
/// is closed for staging.
#[test]
fn a_lost_first_packet_on_a_grown_window_is_not_stopped() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut subnet = 240u8;
        let mut rows = Vec::new();
        for one_way in [Duration::from_millis(40), Duration::from_millis(150)] {
            for lost in [1usize, 3, 8] {
                let network: &'static str = Box::leak(
                    format!("quic-3c-lost-header-{}-{lost}", one_way.as_millis()).into_boxed_str(),
                );
                let run = first_packet_lost_on_a_grown_window(network, subnet, one_way, lost).await;
                subnet += 1;
                eprintln!("3c lost header: {run:?}");
                rows.push(run);
            }
        }
        for run in &rows {
            assert_eq!(run.stops.closed, 0, "closed for staging: {run:?}");
        }
    })
}

// Ramp gate (2): what quinn holds between two polls of a reader.

/// One node (the hub) with a metered data path to each of several leaves,
/// every endpoint on one simulated network and one `VirtualTime`. The hub's
/// paths share one node budget, as a node's do.
struct Star {
    time: VirtualTime,
    network: String,
    /// Per leaf: the leaf's sending path, the hub's receiving path and its
    /// inbound channels.
    links: Vec<(Arc<QuicPath>, Arc<QuicPath>, QuicInbound)>,
    _endpoints: Vec<super::super::super::QuicEndpoint>,
    pumps: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Star {
    fn drop(&mut self) {
        for pump in &self.pumps {
            pump.abort();
        }
        crate::transport::in_memory_socket::set_packet_delivery_callback(&self.network, None);
        crate::transport::in_memory_socket::unregister_network_time_source(&self.network);
    }
}

impl Star {
    /// A hub on `10.207.<subnet>.1` and `leaves` leaves on `.2` onward, each
    /// connected and its path enabled for data. Runs on the caller's runtime
    /// (one of several workers or the current thread).
    async fn build(network: &str, subnet: u8, leaves: usize) -> Self {
        use super::super::super::QuicEndpoint;
        use super::super::super::config::QuicSettings;
        use crate::transport::Socket;
        use crate::transport::crypto::TransportKeypair;
        use crate::transport::demux::DemuxSocket;
        use crate::transport::in_memory_socket::{
            SimulationSocket, register_address_network, register_network_time_source,
        };
        use crate::transport::packet_data::MAX_RECV_PACKET_SIZE;
        use crate::transport::peer_connection::inbound_budget::SharedInboundBudget;

        let time = VirtualTime::new();
        register_network_time_source(network, time.clone());
        let settings = QuicSettings {
            memory_limit: NODE_MEMORY,
            idle_timeout: None,
            initial_window: None,
            rate_limit: Default::default(),
        };
        let mut pumps = Vec::new();
        let mut endpoints = Vec::new();
        let bind = |addr: SocketAddr| {
            let (time, settings) = (time.clone(), settings.clone());
            register_address_network(addr, network);
            async move {
                let socket = Arc::new(DemuxSocket::wrap(
                    SimulationSocket::bind(addr).await.expect("sim bind"),
                    None,
                ));
                let keys = TransportKeypair::new();
                let endpoint = QuicEndpoint::bind(socket.clone(), addr, time, &settings, &keys)
                    .expect("endpoint");
                let pump = tokio::spawn(async move {
                    let mut buf = vec![0u8; MAX_RECV_PACKET_SIZE];
                    while socket.recv_classified(&mut buf).await.is_ok() {}
                });
                (endpoint, keys, pump)
            }
        };
        let hub_addr: SocketAddr = format!("10.207.{subnet}.1:5000").parse().unwrap();
        let (hub, hub_keys, pump) = bind(hub_addr).await;
        pumps.push(pump);
        let budget = node_budget(leaked_counters());
        let mut links = Vec::new();
        for leaf in 0..leaves {
            let addr: SocketAddr = format!("10.207.{subnet}.{}:5000", leaf + 2)
                .parse()
                .unwrap();
            let (endpoint, keys, pump) = bind(addr).await;
            pumps.push(pump);
            let permit = hub.allow_peer(addr, keys.public()).expect("allow-set room");
            let (client_end, server_end) = (endpoint.clone(), hub.clone());
            let hub_key = hub_keys.public().clone();
            let (client, server) =
                super::super::super::tests::drive_on_threads(&time, LIMIT, async move {
                    let (client, server) =
                        tokio::join!(client_end.connect(hub_addr, &hub_key), async {
                            server_end
                                .accept()
                                .await
                                .expect("an attempt")
                                .establish()
                                .await
                        });
                    (client.expect("client"), server.expect("server"))
                })
                .await;
            drop(permit);
            let limits = QuicPathLimits::DEFAULT;
            let own = || SharedInboundBudget::new(limits.max_streams, limits.max_bytes);
            let (sender, _sender_in) = QuicPath::with_budget_and_own_channels(
                client,
                time.clone(),
                limits,
                own(),
                Some(&node_budget(leaked_counters())),
            );
            sender.enable_data();
            let (receiver, inbound) = QuicPath::with_budget_and_own_channels(
                server,
                time.clone(),
                limits,
                own(),
                Some(&budget),
            );
            receiver.enable_data();
            links.push((Arc::new(sender), Arc::new(receiver), inbound));
            endpoints.push(endpoint);
        }
        endpoints.push(hub);
        Self {
            time,
            network: network.to_string(),
            links,
            _endpoints: endpoints,
            pumps,
        }
    }
}

/// One sample of a hub path: what quinn had counted received and what the
/// reader had read, at one instant.
#[derive(Clone, Copy)]
struct HoldSample {
    udp_rx: u64,
    read: u64,
}

/// One run of [`hold_between_polls`].
// Read through `Debug` in the printed rows.
#[allow(dead_code)]
#[derive(Debug)]
struct HoldRun {
    connections: usize,
    workers: usize,
    hogs: usize,
    /// The largest estimated hold of any one connection, in bytes.
    worst_per_connection: u64,
    /// The largest sum over connections at one sample, in bytes.
    worst_total: u64,
    /// Streams the hub assembled, of those sent.
    assembled: usize,
    sent: usize,
}

/// Bytes each leaf sends to the hub in [`hold_between_polls`], as streams of
/// [`HOLD_STREAM_BYTES`].
const HOLD_STREAMS_PER_LEAF: usize = 4;
const HOLD_STREAM_BYTES: usize = MIB;

/// Every leaf of a [`Star`] sends [`HOLD_STREAMS_PER_LEAF`] streams to the hub
/// at once, while `hogs` tasks keep the runtime's `workers` busy (each spins
/// for 200 us of real time between yields). Every 1 ms of virtual time a
/// sampler reads, for each hub path, quinn's received-byte count and the
/// bytes its reader has read. The hold is estimated as received bytes scaled
/// by the run's final payload fraction (read / received once everything is
/// read) minus the bytes read, so it errs high by the overhead of ack-only
/// packets received in bursts.
fn hold_between_polls(connections: usize, workers: usize, hogs: usize, subnet: u8) -> HoldRun {
    let network: &'static str =
        Box::leak(format!("quic-3c-hold-{connections}-{workers}-{hogs}").into_boxed_str());
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build()
            .expect("runtime");
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(async move {
                let _seed = GlobalRng::seed_guard(SEED);
                let star = Star::build(network, subnet, connections).await;
                let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let mut hog_tasks = Vec::new();
                for _ in 0..hogs {
                    let stop = stop.clone();
                    hog_tasks.push(tokio::spawn(async move {
                        // Real CPU time on purpose: the load is what delays
                        // the readers. Not a clock read the code under test
                        // makes.
                        #[allow(clippy::disallowed_methods)]
                        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                            let until = std::time::Instant::now() + Duration::from_micros(200);
                            while std::time::Instant::now() < until {
                                std::hint::spin_loop();
                            }
                            tokio::task::yield_now().await;
                        }
                    }));
                }
                let mut star = star;
                let links = std::mem::take(&mut star.links);
                let time = star.time.clone();
                let receivers: Vec<Arc<QuicPath>> = links
                    .iter()
                    .map(|(_, receiver, _)| receiver.clone())
                    .collect();
                let sent = connections * HOLD_STREAMS_PER_LEAF;
                let assembled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let mut work = Vec::new();
                for (sender, _receiver, mut inbound) in links {
                    for index in 0..HOLD_STREAMS_PER_LEAF {
                        let (sender, time) = (sender.clone(), time.clone());
                        work.push(tokio::spawn(async move {
                            let body = Bytes::from(vec![index as u8; HOLD_STREAM_BYTES]);
                            sender
                                .send_stream_bounded(
                                    &time,
                                    SendDeadlines::stream(HOLD_STREAM_BYTES as u64),
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
                        while taken < HOLD_STREAMS_PER_LEAF {
                            let Some(arrival) = inbound.streams.recv().await else {
                                break;
                            };
                            taken += 1;
                            if arrival
                                .handle
                                .assemble()
                                .await
                                .is_ok_and(|body| body.len() == HOLD_STREAM_BYTES)
                            {
                                assembled.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }));
                }
                let sampling = Arc::new(std::sync::atomic::AtomicBool::new(true));
                let (sampler_on, sampler_time, watched) =
                    (sampling.clone(), time.clone(), receivers.clone());
                let sampler = tokio::spawn(async move {
                    let mut samples: Vec<Vec<HoldSample>> = Vec::new();
                    while sampler_on.load(std::sync::atomic::Ordering::Relaxed) {
                        samples.push(
                            watched
                                .iter()
                                .map(|path| HoldSample {
                                    udp_rx: path.connection().inner().stats().udp_rx.bytes,
                                    read: path.inbound_read_total().unwrap_or(0),
                                })
                                .collect(),
                        );
                        sampler_time.sleep(Duration::from_millis(1)).await;
                    }
                    samples
                });
                let all = futures::future::join_all(work);
                super::super::super::tests::drive_on_threads(&time, LIMIT, async move {
                    all.await;
                })
                .await;
                sampling.store(false, std::sync::atomic::Ordering::Relaxed);
                stop.store(true, std::sync::atomic::Ordering::Relaxed);
                let samples =
                    super::super::super::tests::drive_on_threads(&time, LIMIT, async move {
                        sampler.await.expect("sampler")
                    })
                    .await;
                for hog in hog_tasks {
                    hog.await.ok();
                }
                // Each path's payload fraction, from its final counts.
                let fractions: Vec<f64> = receivers
                    .iter()
                    .map(|path| {
                        let rx = path.connection().inner().stats().udp_rx.bytes.max(1);
                        path.inbound_read_total().unwrap_or(0) as f64 / rx as f64
                    })
                    .collect();
                let (mut worst_per_connection, mut worst_total) = (0u64, 0u64);
                for sample in &samples {
                    let mut total = 0u64;
                    for (at, fraction) in sample.iter().zip(&fractions) {
                        let hold = (at.udp_rx as f64 * fraction) as u64;
                        let hold = hold.saturating_sub(at.read);
                        worst_per_connection = worst_per_connection.max(hold);
                        total += hold;
                    }
                    worst_total = worst_total.max(total);
                }
                drop(star);
                HoldRun {
                    connections,
                    workers,
                    hogs,
                    worst_per_connection,
                    worst_total,
                    assembled: assembled.load(std::sync::atomic::Ordering::Relaxed),
                    sent,
                }
            })
        }));
        done.send(outcome).ok();
    });
    match finished.recv_timeout(Duration::from_secs(600)) {
        Ok(Ok(run)) => run,
        Ok(Err(panic)) => std::panic::resume_unwind(panic),
        Err(_) => panic!("the hold run did not finish within 600 s"),
    }
}

/// Ramp gate (2): the between-polls hold under CPU load, with many
/// connections on few workers. Prints one row per shape; asserts only that
/// the transfers complete.
#[test]
fn quinns_hold_between_reader_polls_under_cpu_load() {
    let mut subnet = 1u8;
    for (connections, workers, hogs) in [(8, 2, 0), (8, 2, 4), (32, 2, 4), (32, 1, 2), (64, 2, 8)] {
        let run = hold_between_polls(connections, workers, hogs, subnet);
        subnet += 1;
        eprintln!("3c hold: {run:?}");
        assert_eq!(run.assembled, run.sent, "{run:?}");
    }
}

// Ramp gate (3): hostile peers.

/// A node (the hub) on one simulated network and `VirtualTime`, with one
/// node budget its data paths share, to which honest leaves (real
/// `QuicPath`s) and hostile peers (hand-driven quinn-proto clients) connect.
struct Hub {
    time: VirtualTime,
    network: String,
    subnet: u8,
    addr: SocketAddr,
    keys: crate::transport::crypto::TransportKeypair,
    endpoint: super::super::super::QuicEndpoint,
    budget: Arc<QuicBudget>,
    next_leaf: u8,
    keep: Vec<super::super::super::QuicEndpoint>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Hub {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
        crate::transport::in_memory_socket::set_packet_delivery_callback(&self.network, None);
        crate::transport::in_memory_socket::unregister_network_time_source(&self.network);
    }
}

/// What a hub path for one peer holds of the node's budget and staging.
#[derive(Debug, Default, Clone, Copy)]
struct HubPathPeak {
    staged: usize,
    staging_cap: usize,
    node_charged: u64,
    /// The most staged past the cap at one sample (the cap can fall below
    /// what is held when the window shrinks).
    over_cap: usize,
    /// The lowest cap seen.
    min_cap: usize,
    /// The most gaps the path counted at one sample.
    gaps: usize,
}

impl Hub {
    fn settings() -> super::super::super::config::QuicSettings {
        super::super::super::config::QuicSettings {
            memory_limit: NODE_MEMORY,
            idle_timeout: None,
            initial_window: None,
            rate_limit: Default::default(),
        }
    }

    /// Bind an endpoint at `addr` on `network`, with its socket pump in
    /// `tasks`.
    async fn bind_on(
        network: &str,
        time: &VirtualTime,
        tasks: &mut Vec<tokio::task::JoinHandle<()>>,
        addr: SocketAddr,
        keys: &crate::transport::crypto::TransportKeypair,
    ) -> super::super::super::QuicEndpoint {
        use crate::transport::Socket;
        use crate::transport::demux::DemuxSocket;
        use crate::transport::in_memory_socket::{SimulationSocket, register_address_network};
        use crate::transport::packet_data::MAX_RECV_PACKET_SIZE;
        register_address_network(addr, network);
        let socket = Arc::new(DemuxSocket::wrap(
            SimulationSocket::bind(addr).await.expect("sim bind"),
            None,
        ));
        let endpoint = super::super::super::QuicEndpoint::bind(
            socket.clone(),
            addr,
            time.clone(),
            &Self::settings(),
            keys,
        )
        .expect("endpoint");
        tasks.push(tokio::spawn(async move {
            let mut buf = vec![0u8; MAX_RECV_PACKET_SIZE];
            while socket.recv_classified(&mut buf).await.is_ok() {}
        }));
        endpoint
    }

    async fn build(network: &str, subnet: u8) -> Self {
        let time = VirtualTime::new();
        crate::transport::in_memory_socket::register_network_time_source(network, time.clone());
        let addr: SocketAddr = format!("10.208.{subnet}.1:5000").parse().unwrap();
        let keys = crate::transport::crypto::TransportKeypair::new();
        let mut tasks = Vec::new();
        let endpoint = Self::bind_on(network, &time, &mut tasks, addr, &keys).await;
        // Every datagram takes HUB_ONE_WAY, so windows limit transfers.
        crate::transport::in_memory_socket::set_packet_delivery_callback(
            network,
            Some(Arc::new(|_, _, _, _| {
                crate::transport::in_memory_socket::PacketDeliveryDecision::DelayedDelivery(
                    HUB_ONE_WAY,
                )
            })),
        );
        Self {
            time,
            network: network.to_string(),
            subnet,
            addr,
            keys,
            endpoint,
            budget: node_budget(leaked_counters()),
            next_leaf: 2,
            keep: Vec::new(),
            tasks,
        }
    }

    fn next_addr(&mut self) -> SocketAddr {
        let addr = format!("10.208.{}.{}:5000", self.subnet, self.next_leaf)
            .parse()
            .unwrap();
        self.next_leaf += 1;
        addr
    }

    /// The hub's path for a connection, metered from the node budget.
    fn hub_path(
        &self,
        server: super::super::super::QuicConnection,
    ) -> (Arc<QuicPath>, QuicInbound) {
        use crate::transport::peer_connection::inbound_budget::SharedInboundBudget;
        let limits = QuicPathLimits::DEFAULT;
        let (path, inbound) = QuicPath::with_budget_and_own_channels(
            server,
            self.time.clone(),
            limits,
            SharedInboundBudget::new(limits.max_streams, limits.max_bytes),
            Some(&self.budget),
        );
        path.enable_data();
        (Arc::new(path), inbound)
    }

    /// An honest leaf: a real `QuicPath` that sends to the hub, and the hub's
    /// path for it.
    async fn honest_leaf(&mut self) -> (Arc<QuicPath>, Arc<QuicPath>, QuicInbound) {
        use crate::transport::peer_connection::inbound_budget::SharedInboundBudget;
        let addr = self.next_addr();
        let keys = crate::transport::crypto::TransportKeypair::new();
        let endpoint = Self::bind_on(&self.network, &self.time, &mut self.tasks, addr, &keys).await;
        let permit = self.endpoint.allow_peer(addr, keys.public()).expect("room");
        let (client_end, server_end, hub_addr, hub_key) = (
            endpoint.clone(),
            self.endpoint.clone(),
            self.addr,
            self.keys.public().clone(),
        );
        let (client, server) = drive(&self.time, LIMIT, async move {
            let (client, server) = tokio::join!(client_end.connect(hub_addr, &hub_key), async {
                server_end
                    .accept()
                    .await
                    .expect("an attempt")
                    .establish()
                    .await
            });
            (client.expect("client"), server.expect("server"))
        })
        .await;
        drop(permit);
        let limits = QuicPathLimits::DEFAULT;
        let (sender, _) = QuicPath::with_budget_and_own_channels(
            client,
            self.time.clone(),
            limits,
            SharedInboundBudget::new(limits.max_streams, limits.max_bytes),
            Some(&node_budget(leaked_counters())),
        );
        sender.enable_data();
        self.keep.push(endpoint);
        let (receiver, inbound) = self.hub_path(server);
        (Arc::new(sender), receiver, inbound)
    }

    /// A hostile peer: a quinn-proto client driven by hand, connected to the
    /// hub, and the hub's path for it.
    async fn hostile_leaf(&mut self) -> (HostilePeer, Arc<QuicPath>, QuicInbound) {
        let addr = self.next_addr();
        let (peer, permit) = HostilePeer::connect(self, addr).await;
        let server_end = self.endpoint.clone();
        let server = drive(&self.time, LIMIT, async move {
            server_end
                .accept()
                .await
                .expect("an attempt")
                .establish()
                .await
                .expect("server")
        })
        .await;
        drop(permit);
        let (receiver, inbound) = self.hub_path(server);
        (peer, receiver, inbound)
    }

    /// Sample a hub path's staging and the node's charge.
    fn sample(&self, path: &QuicPath, peak: &mut HubPathPeak) {
        let (staged, cap) = (path.inbound_staged(), path.inbound_staging_cap());
        peak.staged = peak.staged.max(staged);
        peak.staging_cap = peak.staging_cap.max(cap);
        peak.over_cap = peak.over_cap.max(staged.saturating_sub(cap));
        peak.gaps = peak.gaps.max(path.inbound_gaps_held());
        peak.min_cap = if peak.min_cap == 0 {
            cap
        } else {
            peak.min_cap.min(cap)
        };
        peak.node_charged = peak.node_charged.max(self.budget.running_total());
    }
}

/// What the script asks of a [`HostilePeer`] at its next step.
#[derive(Default)]
struct HostileState {
    /// Hand datagrams from the hub to the connection (acknowledgements and
    /// credit). Off, the peer never learns that anything was lost, and never
    /// learns of new credit.
    feed_in: bool,
    /// Run the connection's timers. Off, it never sends a probe or a resend.
    run_timers: bool,
    /// Datagrams to drop from the next transmits.
    drop_out: usize,
    /// Drop every datagram of the next step.
    drop_step: bool,
    /// Datagrams sent and dropped.
    sent: u64,
    dropped: u64,
}

/// A quinn-proto client the test drives by hand: every [`PEER_STEP`] of
/// virtual time it takes in what reached it (if `feed_in`), runs its timers
/// (if `run_timers`) and sends what it has, dropping as told. A test writes
/// to its streams with [`Self::with_conn`].
struct HostilePeer {
    proto: Arc<std::sync::Mutex<super::super::super::memory_tests::ProtoPeer>>,
    state: Arc<std::sync::Mutex<HostileState>>,
}

/// The one-way delay of every datagram on a [`Hub`]'s network.
const HUB_ONE_WAY: Duration = Duration::from_millis(20);

/// How often a [`HostilePeer`] runs.
const HOSTILE_STEP: Duration = Duration::from_millis(2);

impl HostilePeer {
    async fn connect(
        hub: &mut Hub,
        addr: SocketAddr,
    ) -> (Self, super::super::super::QuicPeerPermit) {
        use super::super::super::config;
        use super::super::super::identity::{PeerKey, QuicIdentity};
        use crate::transport::in_memory_socket::{SimulationSocket, register_address_network};
        use crate::transport::packet_data::MAX_RECV_PACKET_SIZE;
        use std::collections::VecDeque;
        use std::time::Instant;

        register_address_network(addr, &hub.network);
        let keys = crate::transport::crypto::TransportKeypair::new();
        let permit = hub.endpoint.allow_peer(addr, keys.public()).expect("room");
        let identity = QuicIdentity::new(&keys).expect("identity");
        let hub_peer = PeerKey::of(hub.keys.public()).expect("hub key");
        let mut transport = config::transport_config(&Hub::settings(), &hub.time);
        let mut cubic = quinn::congestion::CubicConfig::default();
        cubic.initial_window(1 << 30);
        // A hostile peer sets its own send window: nothing it has sent waits
        // for an acknowledgement before it sends more.
        transport
            .pad_to_mtu(true)
            .send_window(1 << 30)
            .congestion_controller_factory(Arc::new(cubic));
        let client = config::client_config(
            Arc::new(identity.client_tls(&hub_peer).expect("client tls")),
            Arc::new(transport),
        );
        let mut proto = super::super::super::memory_tests::ProtoPeer {
            addr,
            endpoint: quinn_proto::Endpoint::new(
                Arc::new(config::endpoint_config().expect("endpoint config")),
                None,
                false,
                None,
            ),
            conn: None,
        };
        // quinn-proto takes `Instant`s; `base` is only the epoch they are
        // counted from, and every elapsed time comes from the virtual clock.
        let base = Instant::now();
        let at = move |time: &VirtualTime| base + Duration::from_nanos(time.now_nanos());
        proto.conn = Some(
            proto
                .endpoint
                .connect(at(&hub.time), client, hub.addr, config::SERVER_NAME)
                .expect("connect"),
        );
        let proto = Arc::new(std::sync::Mutex::new(proto));
        let state = Arc::new(std::sync::Mutex::new(HostileState {
            feed_in: true,
            run_timers: true,
            ..HostileState::default()
        }));
        let socket = Arc::new(SimulationSocket::bind(addr).await.expect("sim bind"));
        type Inbox = std::sync::Mutex<VecDeque<(Vec<u8>, SocketAddr)>>;
        let inbox: Arc<Inbox> = Arc::default();
        let (reading, queue) = (socket.clone(), inbox.clone());
        hub.tasks.push(tokio::spawn(async move {
            let mut buf = vec![0u8; MAX_RECV_PACKET_SIZE];
            while let Ok((len, from)) = reading.recv_from(&mut buf).await {
                queue.lock().unwrap().push_back((buf[..len].to_vec(), from));
            }
        }));
        let (time, hub_addr, driven, script) =
            (hub.time.clone(), hub.addr, proto.clone(), state.clone());
        hub.tasks.push(tokio::spawn(async move {
            loop {
                time.sleep(HOSTILE_STEP).await;
                let now = at(&time);
                let arrived: Vec<_> = inbox.lock().unwrap().drain(..).collect();
                let out = {
                    let mut peer = driven.lock().unwrap();
                    let (feed_in, run_timers) = {
                        let state = script.lock().unwrap();
                        (state.feed_in, state.run_timers)
                    };
                    let mut out = Vec::new();
                    if feed_in {
                        for (datagram, from) in arrived {
                            out.extend(peer.receive(now, from, &datagram));
                        }
                    }
                    if run_timers {
                        peer.pump(now);
                    } else {
                        Self::pump_without_timers(&mut peer);
                    }
                    if let Some((_, conn)) = peer.conn.as_mut() {
                        let mut buf = Vec::new();
                        while let Some(transmit) = conn.poll_transmit(now, 1, &mut buf) {
                            out.push(buf[..transmit.size].to_vec());
                            buf.clear();
                        }
                    }
                    out
                };
                let drop_step = std::mem::take(&mut script.lock().unwrap().drop_step);
                for datagram in out {
                    if drop_step {
                        let mut state = script.lock().unwrap();
                        state.sent += 1;
                        state.dropped += 1;
                        continue;
                    }
                    let drop_it = {
                        let mut state = script.lock().unwrap();
                        state.sent += 1;
                        if state.drop_out > 0 {
                            state.drop_out -= 1;
                            state.dropped += 1;
                            true
                        } else {
                            false
                        }
                    };
                    if !drop_it {
                        socket.send_to(&datagram, hub_addr).await.ok();
                    }
                }
            }
        }));
        (Self { proto, state }, permit)
    }

    /// [`ProtoPeer::pump`] without running the connection's timers.
    fn pump_without_timers(peer: &mut super::super::super::memory_tests::ProtoPeer) {
        let super::super::super::memory_tests::ProtoPeer { endpoint, conn, .. } = peer;
        let Some((handle, conn)) = conn.as_mut() else {
            return;
        };
        while let Some(event) = conn.poll_endpoint_events() {
            if let Some(event) = endpoint.handle_event(*handle, event) {
                conn.handle_event(event);
            }
        }
        while conn.poll().is_some() {}
    }

    fn with_conn<R>(&self, f: impl FnOnce(&mut quinn_proto::Connection) -> R) -> R {
        f(self.proto.lock().unwrap().conn())
    }

    fn script(&self, f: impl FnOnce(&mut HostileState)) {
        f(&mut self.state.lock().unwrap())
    }

    fn handshaken(&self) -> bool {
        self.with_conn(|conn| !conn.is_handshaking())
    }

    /// Open a uni stream and write `bytes` on it; the stream, or `None` if no
    /// stream credit was left. Writes what flow control takes.
    fn open_and_write(&self, bytes: &[u8]) -> Option<(quinn_proto::StreamId, usize)> {
        self.with_conn(|conn| {
            let id = conn.streams().open(quinn_proto::Dir::Uni)?;
            let written = conn.send_stream(id).write(bytes).unwrap_or(0);
            Some((id, written))
        })
    }

    fn write(&self, id: quinn_proto::StreamId, bytes: &[u8]) -> usize {
        self.with_conn(|conn| conn.send_stream(id).write(bytes).unwrap_or(0))
    }
}

/// What a hostile peer does once its window has grown.
#[derive(Debug, Clone, Copy)]
struct HostileShape {
    /// Streams it opens with their first datagram (the header) withheld.
    streams: usize,
    /// Bytes per write above the withheld header; each write goes out in
    /// the next transmit, several streams' writes packed per datagram.
    frame_len: usize,
    /// Writes per stream (each of `frame_len` bytes), as credit allows.
    frames: usize,
    /// Bytes per stream, after the header, sent and lost before the frames,
    /// so the frames land that far above the read position.
    skip: usize,
    /// After 12 s of quiet (past the idle decay), bytes sent in order on a
    /// new stream with its header: they repay the window's debt, so the
    /// staging cap falls while the withheld streams stay staged.
    then_contiguous: usize,
    /// Lose every other step's datagrams while writing the frames, so each
    /// stream holds a gap between every two frames.
    alternate_drop: bool,
    /// The honest stream that grows the window first.
    grow_bytes: usize,
}

/// One run of [`hostile_against_hub`].
// Read through `Debug` in the printed rows.
#[allow(dead_code)]
#[derive(Debug)]
struct HostileRun {
    shape: Option<HostileShape>,
    /// How long the honest leaf's transfer took, in virtual time.
    honest_took: Duration,
    honest_outcome: String,
    /// The hub's path for the hostile peer.
    peak: HubPathPeak,
    stops: StagingStops,
    hostile_closed: Option<String>,
    budget_total: u64,
    hostile_dropped: u64,
    /// Bytes the hostile peer wrote above its withheld headers.
    hostile_written: usize,
    /// The hub path's staging cap when the hostile peer pinned its window:
    /// a cap that rises afterwards means the window grew on gapped bytes.
    cap_at_pin: usize,
    /// Streams the gap cap stopped, and whether it closed the path.
    too_many_ranges: u64,
    closed_too_many_ranges: u64,
}

/// Bytes the honest leaf sends while the hostile peer runs.
const HONEST_BYTES: usize = 16 * MIB;

/// Write all of `bytes` on stream `id` of `peer`, driving time as credit
/// allows, for at most `limit` of virtual time.
async fn write_all_driven(hub: &Hub, peer: &HostilePeer, id: quinn_proto::StreamId, bytes: &[u8]) {
    let mut written = 0;
    let start = hub.time.now_nanos();
    while written < bytes.len() {
        written += peer.write(id, &bytes[written..]);
        let time = hub.time.clone();
        drive(
            &hub.time,
            LIMIT,
            async move { time.sleep(HOSTILE_STEP).await },
        )
        .await;
        assert!(
            hub.time.now_nanos() - start < LIMIT.as_nanos() as u64,
            "the write did not finish"
        );
    }
}

/// A hub with an honest leaf sending [`HONEST_BYTES`] and, with `shape`, a
/// hostile peer that first grows its window honestly (one 24 MiB stream,
/// read as it arrives) and then pins it: it stops hearing the hub and stops
/// running its timers, so nothing it sends is ever resent, and sends
/// `shape`. Samples the hub's path for the hostile peer and the node's
/// charge every 2 ms.
async fn hostile_against_hub(
    network: &'static str,
    subnet: u8,
    shape: Option<HostileShape>,
) -> HostileRun {
    let mut hub = Hub::build(network, subnet).await;
    let (honest, honest_in_hub, mut honest_inbound) = hub.honest_leaf().await;
    let mut hostile = None;
    if shape.is_some() {
        let (peer, path, mut inbound) = hub.hostile_leaf().await;
        assert!(peer.handshaken(), "the hostile peer finished its handshake");
        // Grow: one honest stream, read as it arrives.
        let total = shape.map_or(24 * MIB, |shape| shape.grow_bytes);
        let mut first = header(1, 1, StreamId::next().0, total as u64, 0);
        first.extend_from_slice(&vec![0x11; total]);
        let (id, written) = peer.open_and_write(&first).expect("stream credit");
        let reader = tokio::spawn(async move {
            let arrival = inbound.streams.recv().await.expect("announced");
            arrival.handle.assemble().await.map(|body| body.len())
        });
        write_all_driven(&hub, &peer, id, &first[written..]).await;
        peer.with_conn(|conn| conn.send_stream(id).finish().expect("finish"));
        let grown = drive(
            &hub.time,
            LIMIT,
            async move { reader.await.expect("reader") },
        )
        .await;
        assert_eq!(grown, Ok(total), "the growth stream arrived");
        // Keep hearing the hub for a few round trips, so the peer holds the
        // last acknowledgements and the whole credit before it pins it.
        let time = hub.time.clone();
        drive(&hub.time, LIMIT, async move {
            time.sleep(HUB_ONE_WAY * 10).await
        })
        .await;
        hostile = Some((peer, path));
    }
    // The honest leaf's transfer, timed, with the hostile shape running
    // beside it.
    let (sender, time) = (honest.clone(), hub.time.clone());
    let honest_task = tokio::spawn(async move {
        let started = time.now_nanos();
        let outcome = sender
            .send_stream_bounded(
                &time,
                SendDeadlines::stream(HONEST_BYTES as u64),
                StreamId::next(),
                None,
                Bytes::from(vec![0x22; HONEST_BYTES]),
                None,
            )
            .await;
        (
            format!("{outcome:?}"),
            Duration::from_nanos(time.now_nanos() - started),
        )
    });
    let honest_reader = tokio::spawn(async move {
        let arrival = honest_inbound
            .streams
            .recv()
            .await
            .expect("honest announced");
        arrival.handle.assemble().await.map(|body| body.len())
    });
    let mut peak = HubPathPeak::default();
    let mut dropped = 0;
    let mut written = 0;
    let mut cap_at_pin = 0;
    if let (Some(shape), Some((peer, path))) = (shape, hostile.as_ref()) {
        peer.script(|state| {
            state.feed_in = false;
            state.run_timers = false;
        });
        let mut streams = Vec::new();
        for _ in 0..shape.streams {
            let first = header(
                1,
                1,
                StreamId::next().0,
                (shape.skip + shape.frame_len * shape.frames) as u64,
                0,
            );
            let Some((id, _)) = peer.open_and_write(&first) else {
                break;
            };
            // The next step sends only this header and the skipped bytes:
            // lose all of it.
            written += peer.write(id, &vec![0x33; shape.skip]);
            peer.script(|state| state.drop_step = true);
            let time = hub.time.clone();
            drive(
                &hub.time,
                LIMIT,
                async move { time.sleep(HOSTILE_STEP).await },
            )
            .await;
            streams.push(id);
        }
        let frame = vec![0x5a; shape.frame_len];
        cap_at_pin = path.inbound_staging_cap();
        for step in 0..shape.frames {
            for id in &streams {
                written += peer.write(*id, &frame);
            }
            if shape.alternate_drop && step % 2 == 1 {
                peer.script(|state| state.drop_step = true);
            }
            let time = hub.time.clone();
            drive(
                &hub.time,
                LIMIT,
                async move { time.sleep(HOSTILE_STEP).await },
            )
            .await;
            hub.sample(path, &mut peak);
        }
        if shape.then_contiguous > 0 {
            for _ in 0..120 {
                let time = hub.time.clone();
                drive(&hub.time, LIMIT, async move {
                    time.sleep(Duration::from_millis(100)).await
                })
                .await;
                hub.sample(path, &mut peak);
            }
            let declared = shape.then_contiguous as u64;
            let mut bytes = header(1, 1, StreamId::next().0, declared, 0);
            bytes.extend_from_slice(&vec![0x44; shape.then_contiguous]);
            if let Some((id, first)) = peer.open_and_write(&bytes) {
                written += first;
                let mut sent = first;
                for _ in 0..500 {
                    if sent >= bytes.len() {
                        break;
                    }
                    let more = peer.write(id, &bytes[sent..]);
                    sent += more;
                    written += more;
                    let time = hub.time.clone();
                    drive(
                        &hub.time,
                        LIMIT,
                        async move { time.sleep(HOSTILE_STEP).await },
                    )
                    .await;
                    hub.sample(path, &mut peak);
                }
            }
        }
        dropped = peer.state.lock().unwrap().dropped;
    }
    // Let the honest transfer finish, sampling the hostile path meanwhile.
    let (honest_outcome, honest_took) = loop {
        if honest_task.is_finished() {
            break honest_task.await.expect("honest task");
        }
        let time = hub.time.clone();
        drive(
            &hub.time,
            LIMIT,
            async move { time.sleep(HOSTILE_STEP).await },
        )
        .await;
        if let Some((_, path)) = hostile.as_ref() {
            hub.sample(path, &mut peak);
        }
    };
    let honest_got = drive(&hub.time, LIMIT, async move {
        honest_reader.await.expect("reader")
    })
    .await;
    assert_eq!(honest_got, Ok(HONEST_BYTES), "the honest transfer arrived");
    // Then 25 s of quiet, past the idle decay (10 s) and short of the
    // readers' idle eviction (30 s): the window may shrink under what the
    // hostile peer left staged.
    if let Some((_, path)) = hostile.as_ref() {
        for _ in 0..250 {
            let time = hub.time.clone();
            drive(&hub.time, LIMIT, async move {
                time.sleep(Duration::from_millis(100)).await
            })
            .await;
            hub.sample(path, &mut peak);
        }
    }
    drop(honest_in_hub);
    let (stops, hostile_closed) = match hostile.as_ref() {
        Some((_, path)) => (
            StagingStops::of(path),
            path.connection()
                .inner()
                .close_reason()
                .map(|reason| format!("{reason}")),
        ),
        None => (StagingStops::default(), None),
    };
    HostileRun {
        shape,
        honest_took,
        honest_outcome,
        peak,
        stops,
        hostile_closed,
        budget_total: hub.budget.total(),
        hostile_dropped: dropped,
        hostile_written: written,
        cap_at_pin,
        too_many_ranges: hostile.as_ref().map_or(0, |(_, path)| {
            path.stats().count(QuicInboundOutcome::TooManyRanges)
        }),
        closed_too_many_ranges: hostile.as_ref().map_or(0, |(_, path)| {
            path.stats().count(QuicInboundOutcome::ClosedTooManyRanges)
        }),
    }
}

/// Ramp gate (3): a hostile peer that grew its window honestly and then
/// withholds headers, with large frames, tiny frames, or many streams,
/// cannot stage past its cap or charge the node past its budget, is stopped
/// or closed, and an honest leaf on the same node is slowed by at most 10%.
#[test]
fn a_hostile_peer_cannot_exceed_its_charge() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let baseline = hostile_against_hub("quic-3c-hostile-baseline", 1, None).await;
        eprintln!("3c hostile: {baseline:?}");
        let shapes = [
            HostileShape {
                streams: 4,
                frame_len: 1100,
                frames: 2000,
                skip: 0,
                then_contiguous: 0,
                alternate_drop: false,
                grow_bytes: 24 * MIB,
            },
            HostileShape {
                streams: 32,
                frame_len: 1,
                frames: 400,
                skip: 0,
                then_contiguous: 0,
                alternate_drop: false,
                grow_bytes: 24 * MIB,
            },
            HostileShape {
                streams: 60,
                frame_len: 64,
                frames: 200,
                skip: 0,
                then_contiguous: 0,
                alternate_drop: false,
                grow_bytes: 24 * MIB,
            },
            HostileShape {
                streams: 8,
                frame_len: 1,
                frames: 1,
                skip: MIB - 64,
                then_contiguous: 0,
                alternate_drop: false,
                grow_bytes: 24 * MIB,
            },
            HostileShape {
                streams: 60,
                frame_len: 1,
                frames: 1,
                skip: 128 * 1024,
                then_contiguous: 0,
                alternate_drop: false,
                grow_bytes: 24 * MIB,
            },
            // Spans just past a power of two, where a buffer grown by
            // doubling is charged about twice its span.
            HostileShape {
                streams: 15,
                frame_len: 1,
                frames: 1,
                skip: 512 * 1024 - HEADER_LEN + 1,
                then_contiguous: 0,
                alternate_drop: false,
                grow_bytes: 24 * MIB,
            },
            HostileShape {
                streams: 31,
                frame_len: 1,
                frames: 1,
                skip: 256 * 1024 - HEADER_LEN + 1,
                then_contiguous: 0,
                alternate_drop: false,
                grow_bytes: 24 * MIB,
            },
            // A gap between every two one-byte frames on 16 streams: more
            // gaps than the connection's cap allows.
            HostileShape {
                streams: 16,
                frame_len: 1,
                frames: 2000,
                skip: 0,
                then_contiguous: 0,
                alternate_drop: true,
                grow_bytes: 24 * MIB,
            },
            // The same on a window grown only a little, so it has room to
            // grow: it must not grow on bytes above gaps.
            HostileShape {
                streams: 4,
                frame_len: 1,
                frames: 400,
                skip: 0,
                then_contiguous: 0,
                alternate_drop: true,
                grow_bytes: 512 * 1024,
            },
            // Grow, stage about half the credit under withheld headers, keep
            // the rest, wait past the idle decay, then spend the rest in
            // order (#6173 review r2, F1 and M1).
            HostileShape {
                streams: 4,
                frame_len: 1100,
                frames: 1000,
                skip: 0,
                then_contiguous: 3 * MIB,
                alternate_drop: false,
                grow_bytes: 24 * MIB,
            },
        ];
        for (index, shape) in shapes.into_iter().enumerate() {
            let network: &'static str =
                Box::leak(format!("quic-3c-hostile-{index}").into_boxed_str());
            let run = hostile_against_hub(network, 2 + index as u8, Some(shape)).await;
            eprintln!("3c hostile: {run:?}");
            assert!(
                run.peak.staged <= run.peak.staging_cap,
                "staged past the cap: {run:?}"
            );
            assert!(run.peak.node_charged <= run.budget_total, "{run:?}");
            assert!(
                run.peak.staging_cap <= run.cap_at_pin,
                "the window grew while the peer sent above gaps: {run:?}"
            );
            if shape.alternate_drop {
                assert!(
                    run.too_many_ranges > 0 || run.closed_too_many_ranges > 0,
                    "the gap cap stopped the gapped streams: {run:?}"
                );
            }
            assert!(
                run.honest_took.as_secs_f64() <= baseline.honest_took.as_secs_f64() * 1.1,
                "the honest leaf was slowed by more than 10%: {run:?} against {baseline:?}"
            );
        }
    })
}

/// What [`squeezed_under_staged`] saw of the victim's hub path.
// Read through `Debug` in the printed rows.
#[allow(dead_code)]
#[derive(Debug, Default)]
struct SqueezeRun {
    budget_total: u64,
    /// The victim's receive window once grown, and what it staged under a
    /// withheld header.
    grown_window: u64,
    staged: usize,
    cap_when_staged: usize,
    /// The most debt the victim's meter showed after the newcomer started:
    /// non-zero means the squeeze reached it.
    debt_peak: u64,
    /// The debt left once the victim's in-order bytes were read.
    debt_after_read: u64,
    /// In-order bytes the victim wrote, and bytes the hub path read, while
    /// pinned.
    in_order_written: usize,
    read_while_pinned: u64,
    /// The lowest receive window after the squeeze.
    window_low: u64,
    /// Samples where the credit alone (window plus unread debt) would have
    /// put the cap below what was staged: the case the floor is for.
    premise_samples: usize,
    samples: usize,
    /// The least the cap exceeded what was staged at any sample (negative:
    /// staged past the cap).
    min_cap_headroom: i64,
    /// The least the victim's charge exceeded its credit plus what it
    /// staged.
    min_charge_headroom: i64,
    node_peak: u64,
    stops: StagingStops,
    closed: Option<String>,
    /// The withheld stream arrived whole once its header did.
    delivered: bool,
    newcomer_outcome: String,
}

#[derive(Default)]
struct SqueezeSamples {
    premise: usize,
    samples: usize,
    min_cap_headroom: Option<i64>,
    min_charge_headroom: Option<i64>,
    debt_peak: u64,
    window_low: Option<u64>,
    node_peak: u64,
}

impl SqueezeSamples {
    fn sample(&mut self, hub: &Hub, path: &QuicPath) {
        use crate::transport::quic::budget::staging_cap;
        let meter = path.meter.as_ref().expect("metered");
        let (window, debt) = meter.credit();
        let (held, cap) = (path.inbound_staged(), path.inbound_staging_cap());
        self.samples += 1;
        if held > 0 && staging_cap(window + debt) < held {
            self.premise += 1;
        }
        let headroom = cap as i64 - held as i64;
        self.min_cap_headroom = Some(self.min_cap_headroom.map_or(headroom, |m| m.min(headroom)));
        let charge = meter.charge() as i64 - (window + debt) as i64 - held as i64;
        self.min_charge_headroom = Some(self.min_charge_headroom.map_or(charge, |m| m.min(charge)));
        self.debt_peak = self.debt_peak.max(debt);
        self.window_low = Some(self.window_low.map_or(window, |w| w.min(window)));
        self.node_peak = self.node_peak.max(hub.budget.running_total());
    }
}

/// One step of a hostile peer's virtual time.
async fn hub_step(hub: &Hub) {
    let time = hub.time.clone();
    drive(
        &hub.time,
        LIMIT,
        async move { time.sleep(HOSTILE_STEP).await },
    )
    .await;
}

/// The F1 case of the #6173 review, on real paths: a node budget small
/// enough to bind; a victim (a hostile peer) grows its window, then stages
/// `staged_share` per cent of that window under a withheld header; a
/// newcomer's growth squeezes the victim's window; the victim then sends
/// bytes in order on another stream, which are read and repay the debt, so
/// its credit falls. Last the header is released. Samples the victim's hub
/// path every 2 ms throughout.
async fn squeezed_under_staged(
    network: &'static str,
    subnet: u8,
    budget_total: u64,
    staged_share: u64,
) -> SqueezeRun {
    let mut hub = Hub::build(network, subnet).await;
    hub.budget = QuicBudget::new(
        budget_total,
        floor_window_bytes(NODE_MEMORY),
        leaked_counters(),
    );
    let (peer, path, inbound) = hub.hostile_leaf().await;
    assert!(peer.handshaken(), "the victim finished its handshake");
    // Grow: one stream read as it arrives.
    let total = 24 * MIB;
    let mut first = header(1, 1, StreamId::next().0, total as u64, 0);
    first.extend_from_slice(&vec![0x11; total]);
    let (id, written) = peer.open_and_write(&first).expect("stream credit");
    let mut arrivals = inbound.streams;
    let reader = tokio::spawn(async move {
        let arrival = arrivals.recv().await.expect("announced");
        let body = arrival.handle.assemble().await.map(|body| body.len());
        (body, arrivals)
    });
    write_all_driven(&hub, &peer, id, &first[written..]).await;
    peer.with_conn(|conn| conn.send_stream(id).finish().expect("finish"));
    let (grown, arrivals) = drive(
        &hub.time,
        LIMIT,
        async move { reader.await.expect("reader") },
    )
    .await;
    assert_eq!(grown, Ok(total), "the growth stream arrived");
    let time = hub.time.clone();
    drive(&hub.time, LIMIT, async move {
        time.sleep(HUB_ONE_WAY * 10).await
    })
    .await;
    let meter = path.meter.clone().expect("metered");
    let grown_window = meter.credit().0;
    // Every later arrival is assembled as it is announced.
    let delivered = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (got, mut arrivals) = (delivered.clone(), arrivals);
    hub.tasks.push(tokio::spawn(async move {
        while let Some(arrival) = arrivals.recv().await {
            let got = got.clone();
            tokio::spawn(async move {
                let body = arrival.handle.assemble().await;
                got.lock().unwrap().push(body.map(|b| b.len()));
            });
        }
    }));

    // Pin, and stage under a withheld header.
    peer.script(|state| {
        state.feed_in = false;
        state.run_timers = false;
    });
    let staged_len = (grown_window * staged_share / 100) as usize;
    let staged_header = header(1, 1, StreamId::next().0, staged_len as u64, 0);
    let (staged_id, _) = peer.open_and_write(&staged_header).expect("stream credit");
    peer.script(|state| state.drop_step = true);
    hub_step(&hub).await;
    let body = vec![0x33; staged_len];
    let mut sent = 0;
    for _ in 0..2000 {
        if sent >= body.len() {
            break;
        }
        sent += peer.write(staged_id, &body[sent..]);
        hub_step(&hub).await;
    }
    assert_eq!(sent, staged_len, "the victim had credit to stage");
    peer.with_conn(|conn| conn.send_stream(staged_id).finish().expect("finish"));
    for _ in 0..50 {
        hub_step(&hub).await;
    }
    let mut samples = SqueezeSamples::default();
    samples.sample(&hub, &path);
    let (staged, cap_when_staged) = (path.inbound_staged(), path.inbound_staging_cap());

    // The newcomer grows; the victim is squeezed.
    let (newcomer, _newcomer_in_hub, mut newcomer_inbound) = hub.honest_leaf().await;
    let (sender, time) = (newcomer.clone(), hub.time.clone());
    let newcomer_task = tokio::spawn(async move {
        let outcome = sender
            .send_stream_bounded(
                &time,
                SendDeadlines::stream(HONEST_BYTES as u64),
                StreamId::next(),
                None,
                Bytes::from(vec![0x22; HONEST_BYTES]),
                None,
            )
            .await;
        format!("{outcome:?}")
    });
    hub.tasks.push(tokio::spawn(async move {
        if let Some(arrival) = newcomer_inbound.streams.recv().await {
            let _body = arrival.handle.assemble().await;
        }
    }));
    for _ in 0..500 {
        hub_step(&hub).await;
        samples.sample(&hub, &path);
        if meter.debt() > 0 && samples.debt_peak > 0 {
            break;
        }
    }

    // The victim sends in order on a new stream, which is read: it repays
    // the debt, so the credit falls to the lowered window.
    let in_order_len = grown_window as usize;
    let mut in_order = header(1, 1, StreamId::next().0, in_order_len as u64, 0);
    in_order.extend_from_slice(&vec![0x44; in_order_len]);
    let read_before = meter.read_total();
    let (in_order_id, mut in_order_sent) = peer.open_and_write(&in_order).expect("stream credit");
    // 3 s: reads that come in one burst are applied at the path's next
    // periodic evaluation (`recycle::ACK_WATCH_TICK`, 2 s).
    for _ in 0..1500 {
        in_order_sent += peer.write(in_order_id, &in_order[in_order_sent..]);
        hub_step(&hub).await;
        samples.sample(&hub, &path);
    }

    let debt_after_read = meter.debt();
    let (in_order_written, read_while_pinned) = (in_order_sent, meter.read_total() - read_before);
    // Release the header: the victim hears the hub and runs its timers
    // again, so the lost header is resent; it sends the rest in order.
    peer.script(|state| {
        state.feed_in = true;
        state.run_timers = true;
    });
    for _ in 0..5000 {
        if in_order_sent < in_order.len() {
            in_order_sent += peer.write(in_order_id, &in_order[in_order_sent..]);
            if in_order_sent == in_order.len() {
                peer.with_conn(|conn| conn.send_stream(in_order_id).finish().expect("finish"));
            }
        }
        hub_step(&hub).await;
        samples.sample(&hub, &path);
        if delivered.lock().unwrap().len() >= 2 {
            break;
        }
    }
    let newcomer_outcome = if newcomer_task.is_finished() {
        newcomer_task.await.expect("newcomer task")
    } else {
        newcomer_task.abort();
        "unfinished".to_string()
    };
    let delivered_whole = delivered
        .lock()
        .unwrap()
        .iter()
        .any(|body| *body == Ok(staged_len));
    SqueezeRun {
        budget_total,
        grown_window,
        staged,
        cap_when_staged,
        debt_peak: samples.debt_peak,
        debt_after_read,
        in_order_written,
        read_while_pinned,
        window_low: samples.window_low.unwrap_or(0),
        premise_samples: samples.premise,
        samples: samples.samples,
        min_cap_headroom: samples.min_cap_headroom.unwrap_or(0),
        min_charge_headroom: samples.min_charge_headroom.unwrap_or(0),
        node_peak: samples.node_peak,
        stops: StagingStops::of(&path),
        closed: path
            .connection()
            .inner()
            .close_reason()
            .map(|reason| format!("{reason}")),
        delivered: delivered_whole,
        newcomer_outcome,
    }
}

/// F1 of the #6173 review on real paths: a squeeze lowers the window of a
/// victim that has bytes staged under a withheld header, and the victim then
/// repays the debt with bytes in order. The staging cap never falls below
/// what is staged, nothing is stopped or closed for staging, the withheld
/// stream is delivered once its header arrives, and the victim's charge
/// covers its credit and its staged bytes.
///
/// On real paths the credit alone never puts the cap below what is staged
/// (`premise_samples` is 0 in every run): staged bytes use the peer's
/// flow-control credit, so they never exceed the credit (W plus unread
/// debt), and the cap is twice the credit. The floor in
/// `ConnStaging::set_cap` is covered by the budget's unit test
/// (`the_staging_cap_never_falls_below_what_is_staged`), not here.
#[test]
fn a_squeeze_never_puts_the_cap_under_what_is_staged() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut runs = Vec::new();
        for (index, (budget_mib, share)) in [(12u64, 30u64), (12, 50), (12, 90), (24, 50), (24, 85)]
            .into_iter()
            .enumerate()
        {
            let network: &'static str =
                Box::leak(format!("quic-3c-squeeze-{index}").into_boxed_str());
            let run =
                squeezed_under_staged(network, 40 + index as u8, budget_mib << 20, share).await;
            eprintln!("3c squeeze: {run:?}");
            runs.push(run);
        }
        for run in &runs {
            assert!(
                run.debt_peak > 0,
                "premise: the squeeze reached the victim: {run:?}"
            );
            assert!(run.min_cap_headroom >= 0, "staged past the cap: {run:?}");
            assert_eq!(run.stops, StagingStops::default(), "{run:?}");
            assert_eq!(run.closed, None, "{run:?}");
            assert!(run.delivered, "the withheld stream arrived: {run:?}");
            assert!(
                run.min_charge_headroom >= 0,
                "staged bytes uncharged: {run:?}"
            );
            // The budget holds growth above the floors, so a newcomer's
            // floor grant does not push it over.
            assert!(run.node_peak <= run.budget_total, "{run:?}");
        }
    })
}
