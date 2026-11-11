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
