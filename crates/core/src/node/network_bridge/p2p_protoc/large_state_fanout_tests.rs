//! Memory held by the fan-out of a large new state to many co-hosts, through
//! the node's real fan-out: `handle_broadcast_state_change` (what the
//! executor's `BroadcastStateChange` reaches after a PUT or UPDATE commits),
//! its co-host target selection, the broadcast queue workers and
//! `broadcast_to_single_peer`.
//!
//! Each case runs on one current-thread runtime, so every allocation of the
//! fan-out lands on the measuring thread (`util::alloc_count`), counted at the
//! jemalloc size class it would be served from. The state is allocated before
//! the measurement and shared, as a node's stored state is, so a peak excludes
//! that one copy. Memory the contract executor uses for real summarize and
//! delta calls (WASM linear memory, on its own threads) is not modelled: the
//! stand-in contract below runs on the measuring thread and allocates only the
//! delta it returns.
//!
//! Every co-host has a cached summary that differs from ours, so the queue
//! predicts a small delta for each, and the stand-in contract returns a delta
//! of the size each case sets: for a contract new to the receivers that is
//! about the size of the state.
//!
//! The stand-in transport keeps every payload it is handed:
//! - a stream for `TRANSFER` of virtual time, longer than
//!   `STREAM_PERMIT_HOLD`, as a slow uplink would, then drops its bytes and
//!   reports delivery. A stream gives its lane permit back at the hold, so its
//!   buffer stays resident outside the lane for the rest of the transfer;
//! - an inline message until every target's leg has reached the transport, so
//!   a case that sends inline measures all of them resident at once rather
//!   than however many happened to overlap.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use freenet_stdlib::prelude::{
    CodeHash, ContractInstanceId, ContractKey, StateDelta, StateSummary, WrappedState,
};

use super::{P2pBridgeEvent, P2pConnManager};
use crate::contract::{ContractHandlerEvent, StoreResponse};
use crate::message::{NetMessage, NetMessageV1};
use crate::operations::update::{BroadcastStreamingPayload, UpdateMsg};
use crate::ring::PeerKey;
use crate::ring::interest::SummaryPopulationSource;
use crate::transport::{StreamSendOutcome, TransportPublicKey};
use crate::util::alloc_count::{allocated_by, jemalloc_class, peak_delta};

const STATE_LEN: usize = 30 * 1024 * 1024;
const TARGETS: u8 = 20;
/// Longer than `STREAM_PERMIT_HOLD` (120 s), so each stream's buffer outlives
/// the permit that admitted it and another leg builds its buffer meanwhile.
const TRANSFER: Duration = Duration::from_secs(150);
/// The default `streaming-threshold`; [`fan_out`] asserts the node uses it.
const STREAMING_THRESHOLD: usize = 64 * 1024;
const OUR_SUMMARY: &[u8] = &[0xA0];
const THEIR_SUMMARY: &[u8] = &[0xB0];

/// What one fan-out case observed.
struct Fanout {
    /// Peak live bytes above the shared state, in jemalloc size classes.
    peak: isize,
    /// Every byte allocated on the measuring thread during the fan-out,
    /// freed or not. A floor on it shows the counter saw the fan-out at all.
    allocated: usize,
    streams: usize,
    inline: usize,
    /// Streams whose bytes were not exactly the bincode of
    /// `BroadcastStreamingPayload { stored state, OUR_SUMMARY }`.
    malformed_streams: usize,
}

/// Fan a `STATE_LEN` state out to `TARGETS` co-hosts whose deltas the
/// stand-in contract computes as `delta_bytes` long.
fn fan_out(id: &str, delta_bytes: usize) -> Fanout {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let state = WrappedState::new(vec![0x5A; STATE_LEN]);
    let (mut mgr, op_manager, key, handler) = rt.block_on(node(id, state.clone(), delta_bytes));
    assert_eq!(
        op_manager.streaming_threshold, STREAMING_THRESHOLD,
        "the threshold cases below are written against the default"
    );
    // What a correct stream carries, built before the measurement so it adds
    // nothing to the peak.
    let expected_stream = bincode::serialize(&BroadcastStreamingPayload {
        state_bytes: state.as_ref().to_vec(),
        sender_summary_bytes: OUR_SUMMARY.to_vec(),
    })
    .expect("serialize");

    let ((observed, allocated), _, peak) = peak_delta(|| {
        allocated_by(|| {
            rt.block_on(async {
                // Virtual time from here: the node is built, and nothing below
                // waits on I/O, so a 150 s transfer costs no wall time. The
                // clock is driven explicitly (see `drive_clock`), because the
                // node keeps a task runnable and tokio only auto-advances a
                // paused clock when every task is idle.
                tokio::time::pause();
                let fan_out = async {
                    let workers = mgr
                        .broadcast_queue
                        .start_worker(mgr.bridge.clone(), op_manager.clone());
                    mgr.handle_broadcast_state_change(
                        &op_manager,
                        key,
                        state.clone(),
                        false,
                        false,
                    )
                    .await;
                    let mut transfers = tokio::task::JoinSet::new();
                    let mut held_inline = Vec::new();
                    let (mut streams, mut inline, mut malformed) = (0usize, 0usize, 0usize);
                    while streams + inline < usize::from(TARGETS) {
                        let Ok(event) =
                            tokio::time::timeout(TRANSFER * 4, mgr.conn_bridge_rx.recv()).await
                        else {
                            panic!(
                                "only {} of {TARGETS} legs reached the transport: {:?}",
                                streams + inline,
                                op_manager
                                    .update_propagation_stats
                                    .events_for_test(key.id())
                            );
                        };
                        #[allow(
                            clippy::wildcard_enum_match_arm,
                            reason = "the stand-in transport only carries broadcast payloads"
                        )]
                        match event.expect("bridge open") {
                            P2pBridgeEvent::StreamSend {
                                data,
                                completion_tx,
                                ..
                            } => {
                                streams += 1;
                                if data[..] != expected_stream[..] {
                                    malformed += 1;
                                }
                                transfers.spawn(async move {
                                    tokio::time::sleep(TRANSFER).await;
                                    drop(data);
                                    if let Some(done) = completion_tx {
                                        let _ignored = done.send(StreamSendOutcome::Delivered);
                                    }
                                });
                            }
                            // A streaming leg's metadata message carries no
                            // payload; an inline broadcast carries all of it.
                            P2pBridgeEvent::Message(_, msg) if is_inline_broadcast(&msg) => {
                                inline += 1;
                                held_inline.push(msg);
                            }
                            _ => {}
                        }
                    }
                    drop(held_inline);
                    while transfers.join_next().await.is_some() {}
                    for worker in workers {
                        worker.abort();
                        let _aborted = worker.await;
                    }
                    (streams, inline, malformed)
                };
                tokio::select! {
                    observed = fan_out => observed,
                    never = drive_clock() => match never {},
                }
            })
        })
    });
    handler.abort();
    let (streams, inline, malformed_streams) = observed;
    Fanout {
        peak,
        allocated,
        streams,
        inline,
        malformed_streams,
    }
}

/// Advance the paused clock in 10 ms steps, forever, yielding between steps
/// so every task runs at each instant it is due.
async fn drive_clock() -> std::convert::Infallible {
    loop {
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
    }
}

fn is_inline_broadcast(msg: &NetMessage) -> bool {
    matches!(
        msg,
        NetMessage::V1(NetMessageV1::Update(
            UpdateMsg::BroadcastTo { .. } | UpdateMsg::BroadcastToV2 { .. }
        ))
    )
}

/// The public key of target `n`, as [`node`] connects it.
fn target_key(n: u8) -> TransportPublicKey {
    TransportPublicKey::from_bytes([n + 1; 32])
}

/// A node hosting one contract whose stored state is `stored`, with `TARGETS`
/// connected peers that announced hosting it, and a stand-in contract handler.
async fn node(
    id: &str,
    stored: WrappedState,
    delta_bytes: usize,
) -> (
    P2pConnManager,
    Arc<crate::node::OpManager>,
    ContractKey,
    tokio::task::JoinHandle<()>,
) {
    let config_args = crate::config::ConfigArgs {
        id: Some(id.to_string()),
        mode: Some(crate::contract::OperationMode::Local),
        ..Default::default()
    };
    let mut node_config =
        crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
            .await
            .expect("build NodeConfig");
    // A gateway needs no configured remote gateway to build.
    node_config.is_gateway = true;
    let (notifications, notification_tx) = crate::node::event_loop_notification_channel();
    let (ops_ch_channel, mut ch_channel, wait_for_event) =
        crate::contract::contract_handler_channel();
    let connection_manager = crate::ring::ConnectionManager::new(&node_config);
    let (result_router_tx, result_router_rx) = tokio::sync::mpsc::channel(100);
    let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();
    let op_manager = Arc::new(
        crate::node::OpManager::new(
            notification_tx,
            ops_ch_channel,
            &node_config,
            crate::tracing::DynamicRegister::new(vec![]),
            connection_manager,
            result_router_tx,
            &task_monitor,
        )
        .expect("build OpManager"),
    );
    op_manager.ring.attach_op_manager(&op_manager);
    let mgr = P2pConnManager::build(
        &node_config,
        op_manager.clone(),
        crate::tracing::DynamicRegister::new(vec![]),
        &task_monitor,
    )
    .await
    .expect("build P2pConnManager");
    op_manager
        .ring
        .connection_manager
        .set_own_addr_local_for_test("127.0.0.1:48400".parse().unwrap());

    let key = ContractKey::from_id_and_code(
        ContractInstanceId::new([61u8; 32]),
        CodeHash::new([43u8; 32]),
    );
    let _ = op_manager.ring.host_contract(
        key,
        stored.size() as u64,
        crate::ring::AccessType::Put,
        crate::ring::HostingCause::Other,
    );
    for n in 0..TARGETS {
        let addr = SocketAddr::from(([127, 0, 0, 1], 48_401 + u16::from(n)));
        let public = target_key(n);
        assert!(op_manager.ring.connection_manager.add_connection(
            crate::ring::Location::new(f64::from(n) / f64::from(TARGETS)),
            addr,
            public.clone(),
            false,
        ));
        op_manager.neighbor_hosting.handle_message(
            &public,
            crate::message::NeighborHostingMessage::HostingAnnounce {
                added: vec![*key.id()],
                removed: vec![],
                is_response: true,
            },
        );
        op_manager.interest_manager.upsert_peer_summary_from(
            &key,
            &PeerKey::from(public),
            StateSummary::from(THEIR_SUMMARY.to_vec()),
            SummaryPopulationSource::Delivery,
        );
    }

    let handler = tokio::spawn(async move {
        // Held for the handler's lifetime: dropping them closes channels the
        // node still writes to.
        let _keep_alive = (
            notifications,
            result_router_rx,
            wait_for_event,
            task_monitor,
        );
        while let Ok((id, ev, _priority)) = ch_channel.recv_from_sender().await {
            #[allow(
                clippy::wildcard_enum_match_arm,
                reason = "stand-in executor serving only the queries a broadcast \
                          leg issues; anything else is a test bug"
            )]
            let response = match ev {
                ContractHandlerEvent::GetSummaryQuery { key } => {
                    ContractHandlerEvent::GetSummaryResponse {
                        key,
                        summary: Ok(StateSummary::from(OUR_SUMMARY.to_vec())),
                    }
                }
                ContractHandlerEvent::GetQuery { .. } => ContractHandlerEvent::GetResponse {
                    key: Some(key),
                    response: Ok(StoreResponse {
                        state: Some(stored.clone()),
                        contract: None,
                    }),
                },
                ContractHandlerEvent::GetDeltaQuery { key, .. } => {
                    ContractHandlerEvent::GetDeltaResponse {
                        key,
                        delta: Ok(StateDelta::from(vec![0xD0; delta_bytes])),
                    }
                }
                ContractHandlerEvent::GetStateAndSummaryQuery { key } => {
                    ContractHandlerEvent::GetStateAndSummaryResponse {
                        key,
                        snapshot: Ok((stored.clone(), StateSummary::from(OUR_SUMMARY.to_vec()))),
                    }
                }
                other => panic!("unexpected handler event: {other:?}"),
            };
            if ch_channel.send_to_sender(id, response).await.is_err() {
                break;
            }
        }
    });
    (mgr, op_manager, key, handler)
}

/// What a fan-out that streams every leg may hold at once.
///
/// A leg holds the delta the contract returned (dropped before its stream is
/// built) or its stream buffer, never both. A stream holds its large-lane
/// permit for at most `STREAM_PERMIT_HOLD`; a transfer longer than that keeps
/// its buffer resident after the permit has gone to the next leg. So each
/// permit can have `ceil(TRANSFER / STREAM_PERMIT_HOLD)` state-sized buffers
/// resident at once: two here. A transfer that takes longer, relative to the
/// hold, leaves more buffers resident outside the lane, which this bound does
/// not cover.
///
/// A delta small enough for the delta cache's entry cap is also kept there;
/// the cache has its own byte budget (`interest_delta_cache_budget_bytes`,
/// 4-16 MiB), which is not fan-out memory but is on this thread.
fn lane_bound() -> isize {
    let hold = super::super::broadcast_queue::STREAM_PERMIT_HOLD;
    let buffers_per_permit = TRANSFER.as_millis().div_ceil(hold.as_millis()) as usize;
    let per_leg = jemalloc_class(STATE_LEN + 1024);
    let delta_cache = jemalloc_class(crate::ring::interest::interest_delta_cache_budget_bytes());
    (super::super::broadcast_queue::LARGE_LANE_PERMITS * buffers_per_permit * per_leg
        + delta_cache
        + 1024 * 1024) as isize
}

/// A fan-out in which every leg streams the full state: each target gets the
/// state's exact bytes with our summary, and peak memory stays within
/// [`lane_bound`].
///
/// What a swapped leg records afterwards (its payload arm, the summary cached
/// for the peer) is checked on one leg by
/// `broadcast_queue::tests::a_streamed_large_delta_leg_is_counted_and_records_its_summary`.
/// It cannot be checked here: this fan-out runs about 25 minutes of virtual
/// time, past the timer that drains the counters and past `INTEREST_TTL`,
/// which expires the earliest targets' records.
fn assert_streams_within_lane_bound(case: &str, run: &Fanout) {
    assert_eq!(
        (run.streams, run.inline),
        (usize::from(TARGETS), 0),
        "{case}: every leg streams the full state"
    );
    assert_eq!(
        run.malformed_streams, 0,
        "{case}: streams carry the stored state and our summary"
    );
    // Fail closed: every leg serialized a state-sized stream buffer on the
    // measuring thread. If the counter stopped seeing the fan-out (a thread
    // move, a different allocator), the peak below would pass vacuously.
    assert!(
        run.allocated >= usize::from(TARGETS) * STATE_LEN,
        "{case}: the allocation counter saw {} bytes, less than the {TARGETS} stream \
         buffers the fan-out must have built",
        run.allocated
    );
    let bound = lane_bound();
    assert!(
        run.peak <= bound,
        "{case}: peak live fan-out bytes {} ({:.2} x the state) exceed {bound}",
        run.peak,
        run.peak as f64 / STATE_LEN as f64
    );
}

/// The shape this fix targets: a write of a large state new to its co-hosts, so each
/// co-host's delta is about the size of the state.
///
/// Before the fix each of the 20 legs built its own state-sized payload under
/// a small-lane permit and sent it inline, so all of them were resident at
/// once (measured at 22.4 x the state).
#[test]
fn a_large_new_state_fans_out_within_the_large_lane_bound() {
    let run = fan_out("fanout-state-sized-delta", STATE_LEN);
    assert_streams_within_lane_bound("state-sized deltas", &run);
}

/// A delta of about half a large state streams the state as well.
#[test]
fn a_half_state_delta_streams_within_the_lane_bound() {
    let run = fan_out("fanout-half-state-delta", STATE_LEN / 2 - 1);
    assert_streams_within_lane_bound("half-state deltas", &run);
}

/// The smallest delta of a large state that cannot go inline streams too.
#[test]
fn a_delta_just_over_the_streaming_threshold_streams() {
    let run = fan_out("fanout-threshold-plus-one-delta", streaming_threshold() + 1);
    assert_streams_within_lane_bound("threshold + 1 deltas", &run);
}

/// A delta at the streaming threshold goes inline, as `should_use_streaming`
/// would send it. Inline messages are bounded by count, not by the lanes, so
/// the bound here is every target's delta resident at once (the stand-in
/// transport holds them all) plus the legs still building theirs.
#[test]
fn deltas_at_the_streaming_threshold_stay_inline_bounded_by_target_count() {
    let delta = streaming_threshold();
    let run = fan_out("fanout-threshold-delta", delta);
    assert_eq!(
        (run.streams, run.inline),
        (0, usize::from(TARGETS)),
        "every leg sends its delta inline"
    );
    assert!(
        run.allocated >= usize::from(TARGETS) * delta,
        "the allocation counter saw {} bytes, less than {TARGETS} deltas",
        run.allocated
    );
    let worst = ((usize::from(TARGETS) + super::super::broadcast_queue::LARGE_LANE_PERMITS)
        * jemalloc_class(delta)
        + 1024 * 1024) as isize;
    assert!(run.peak <= worst, "peak {} above {worst}", run.peak);
}

/// The default streaming threshold, which [`fan_out`] checks the node
/// under test actually uses.
fn streaming_threshold() -> usize {
    STREAMING_THRESHOLD
}
