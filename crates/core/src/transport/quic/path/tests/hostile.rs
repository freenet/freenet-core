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
    let mut p = paths_metered(network, subnet, windows).await;
    let loss = seeded_loss_from(network, loss_percent, seed, Some(one_way));
    let (a, b, time) = (p.a.clone(), p.b.clone(), p.time());
    let (a_watch, b_watch) = (p.a.clone(), p.b.clone());
    let (outcomes, staged) = p
        .drive(async move {
            let mut sends = Vec::new();
            for index in 0..BULK_STREAMS {
                for (from, mark) in [(a.clone(), 0xA0u8), (b.clone(), 0xB0u8)] {
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
        for loss_percent in [2u64, 5, 10, 15] {
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
