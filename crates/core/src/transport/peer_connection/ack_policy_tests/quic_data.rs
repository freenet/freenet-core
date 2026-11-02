//! What a QUIC path delivers reaches the connection's `recv` as UDP's messages
//! do (#5940 e5b-4). A real path on `VirtualTime`, built on the connection's
//! budget and channels, with a raw test peer on the other end: no node code sends
//! data on a path yet. The connection itself runs on `RealTime`, which barely moves
//! here, so its idle timers do not fire and an age limit is not reached.

use std::time::Duration;

use super::*;
use crate::operations::orphan_streams::OrphanStreamRegistry;
use crate::transport::metrics::InboundStreamOutcome as O;
use crate::transport::peer_connection::inbound_budget::{LegacyStreamToken, Liveness};
use crate::transport::peer_connection::streaming::MAX_STREAM_TOTAL_BYTES;
use crate::transport::peer_connection::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;
use crate::transport::peer_connection::streaming_handle_idle_timeout;
use crate::transport::quic::framing::FrameHeader;
use crate::transport::quic::path::{
    BoundedSend, QuicInboundOutcome as Q, QuicPathLimits, data_channels,
};
use crate::transport::quic::upgrade::driver::tests::{DataPath, data_path, run_bounded};

const SEND_LIMIT: Duration = Duration::from_secs(30);

struct Rig {
    e: End,
    path: DataPath,
    registry: Arc<OrphanStreamRegistry>,
}

impl Rig {
    fn budget(&self) -> (usize, u64) {
        let budget = self.e.conn.inbound_budget.lock();
        (budget.stream_count(), budget.charged_bytes())
    }

    fn outcome(&self, outcome: O) -> u64 {
        self.e.conn.inbound_outcomes[outcome as usize]
    }

    fn peer_addr(&self) -> SocketAddr {
        self.e.conn.remote_conn.remote_addr
    }

    /// What `recv` returns within a few turns of the scheduler, if anything.
    async fn recv_soon(&mut self) -> Option<Vec<u8>> {
        let turns = async {
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
        };
        tokio::select! {
            biased;
            message = self.e.conn.recv() => Some(message.expect("recv must not fail")),
            () = turns => None,
        }
    }
}

/// `Rig` is not `Sync`, so an `&Rig` cannot be held across an await in a spawned
/// test; these take only what they need.
impl DataPath {
    async fn send_short(&self, payload: &[u8]) {
        let sent = self
            .peer()
            .send_short_bounded(&self.time, SEND_LIMIT, payload)
            .await;
        assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
    }

    async fn send_stream(&self, id: StreamId, metadata: Option<&[u8]>, payload: Bytes) {
        let sent = self
            .peer()
            .send_stream_bounded(&self.time, SEND_LIMIT, id, metadata, payload, None)
            .await;
        assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
    }

    /// Open a stream of `total` bytes, send `sent` of them and keep it open.
    async fn partial_stream(&self, id: StreamId, total: u64, sent: usize) -> quinn::SendStream {
        let connection = self.peer().connection().inner().clone();
        let mut stream = connection.open_uni().await.expect("open");
        let header = FrameHeader::stream(id, total, 0).expect("a valid header");
        stream.write_all(&header.encode()).await.expect("header");
        stream.write_all(&vec![7; sent]).await.expect("payload");
        self.settle(Duration::from_secs(1)).await;
        stream
    }

    /// Let the virtual clock and every task run.
    async fn settle(&self, by: Duration) {
        self.time.sleep(by).await;
    }
}

async fn claim(
    rig: (&Arc<OrphanStreamRegistry>, SocketAddr),
    id: StreamId,
) -> crate::transport::IncomingStream {
    rig.0
        .claim_or_wait(rig.1, id, Duration::from_secs(1))
        .await
        .expect("the stream was registered")
}

fn run<F, Fut>(network: &'static str, subnet: u8, limits: QuicPathLimits, test: F)
where
    F: FnOnce(Rig) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    run_bounded(move || async move {
        let mut e = single(CAPABLE);
        let (data, inbox) = data_channels();
        e.conn.attach_quic_inbox(inbox);
        let registry = Arc::new(OrphanStreamRegistry::new());
        e.conn.set_inbound_stream_registry(registry.clone());
        let path = data_path(network, subnet, limits, e.conn.inbound_budget(), data).await;
        let time = path.time.clone();
        (time, test(Rig { e, path, registry }))
    });
}

fn bytes_of(len: usize) -> Bytes {
    (0..len).map(|i| i as u8).collect::<Vec<_>>().into()
}

/// A Short comes out of `recv` once with its charge released at once, and a Stream
/// is registered for its consumer, with its metadata out of `recv`, and frees its
/// charge when it is let go. Neither moves the UDP session's clocks.
#[test]
fn a_short_and_a_stream_on_a_path_arrive_as_the_udp_ones_do() {
    run(
        "quic-data-arrive",
        40,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let clocks = |e: &End| {
                (
                    e.conn.last_received_nanos,
                    e.conn
                        .last_inbound_nanos
                        .load(std::sync::atomic::Ordering::Relaxed),
                )
            };
            let before = clocks(&rig.e);
            rig.path.send_short(b"hello").await;
            assert_eq!(rig.e.conn.recv().await.unwrap(), b"hello");
            assert_eq!(rig.budget(), (0, 0), "released at once");
            assert_eq!(rig.path.ours().stats().count(Q::DeliveredShort), 1);

            let id = StreamId::next();
            let payload = bytes_of(3 * FRAGMENT_PAYLOAD_SIZE + 17);
            rig.path
                .send_stream(id, Some(b"announce"), payload.clone())
                .await;
            assert_eq!(rig.e.conn.recv().await.unwrap(), b"announce");
            assert!(rig.e.conn.completed_inbound_streams.contains(&id));
            assert_eq!(rig.budget().0, 1, "the consumer's to hold");
            let stream = claim((&rig.registry, rig.peer_addr()), id).await;
            assert_eq!(stream.assemble().await.unwrap(), payload.to_vec());
            assert_eq!(rig.path.ours().stats().count(Q::CompletedStream), 1);
            drop(stream);
            rig.e.conn.sweep_streams();
            assert_eq!(rig.budget(), (0, 0));

            assert_eq!(clocks(&rig.e), before, "QUIC moves no UDP clock");
            assert!(rig.recv_soon().await.is_none(), "each came out once");
        },
    );
}

/// Metadata sent as a Short and embedded in a Stream is dispatched once; embedded
/// and then sent as a Short it goes twice, as on UDP, and a Short is never dropped.
#[test]
fn embedded_metadata_is_suppressed_only_after_the_same_short() {
    run(
        "quic-data-meta",
        41,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            rig.path.send_short(b"first").await;
            assert_eq!(rig.e.conn.recv().await.unwrap(), b"first");
            rig.path
                .send_stream(StreamId::next(), Some(b"first"), bytes_of(10))
                .await;
            assert!(rig.recv_soon().await.is_none(), "already dispatched");

            rig.path
                .send_stream(StreamId::next(), Some(b"second"), bytes_of(10))
                .await;
            assert_eq!(rig.e.conn.recv().await.unwrap(), b"second");
            rig.path.send_short(b"second").await;
            assert_eq!(rig.e.conn.recv().await.unwrap(), b"second");
            rig.path.send_short(b"second").await;
            assert_eq!(rig.e.conn.recv().await.unwrap(), b"second", "a repeat too");
        },
    );
}

/// A path ending mid-stream fails a claimed stream at once and a registered one at
/// the registry's next pass, and both charges go. A message that arrived whole
/// before the end is still delivered.
#[test]
fn a_path_ending_mid_stream_frees_its_charge_and_the_stream_reports_failed() {
    run(
        "quic-data-end",
        42,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let (claimed, unclaimed) = (StreamId::next(), StreamId::next());
            let _claimed_tx = rig.path.partial_stream(claimed, 100_000, 3000).await;
            let _unclaimed_tx = rig.path.partial_stream(unclaimed, 100_000, 3000).await;
            rig.path.send_short(b"whole").await;
            let mut delivered = None;
            while rig.registry.orphan_count() < 2 || delivered.is_none() {
                match rig.recv_soon().await {
                    Some(message) => delivered = Some(message),
                    None => rig.path.settle(Duration::from_millis(100)).await,
                }
            }
            assert_eq!(delivered.as_deref(), Some(&b"whole"[..]));
            assert_eq!(rig.budget().0, 2);
            let consumer = claim((&rig.registry, rig.peer_addr()), claimed).await;

            rig.path.send_short(b"queued").await;
            rig.path.end_ours();
            rig.path.settle(Duration::from_secs(1)).await;

            assert_eq!(
                rig.e.conn.recv().await.unwrap(),
                b"queued",
                "still delivered"
            );
            let error = consumer.assemble().await.expect_err("the stream failed");
            assert!(
                matches!(error, crate::transport::StreamError::Cancelled),
                "{error}"
            );
            drop(consumer);
            rig.registry.gc_expired();
            assert_eq!(
                rig.registry.orphan_count(),
                0,
                "cancelled, so dropped at once"
            );
            rig.e.conn.sweep_streams();
            assert_eq!(rig.budget(), (0, 0));
        },
    );
}

/// A stream that fails while its arrival still waits in the channel is not handed
/// to the registry, and its charge goes when `recv` takes it.
#[test]
fn a_stream_that_ended_in_the_channel_is_not_registered() {
    run(
        "quic-data-queued",
        43,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let id = StreamId::next();
            let _tx = rig.path.partial_stream(id, 100_000, 3000).await;
            rig.path.end_ours();
            rig.path.settle(Duration::from_secs(1)).await;
            assert_eq!(rig.budget().0, 1, "the queued arrival holds it");
            assert!(rig.recv_soon().await.is_none());
            assert_eq!(rig.registry.orphan_count(), 0);
            assert_eq!(rig.budget(), (0, 0));
        },
    );
}

/// Fill the connection's budget so that a small stream no longer fits, with
/// tokens that hold the charges.
fn fill_budget(rig: &Rig) -> Vec<Arc<LegacyStreamToken>> {
    let mut tokens = Vec::new();
    let mut budget = rig.e.conn.inbound_budget.lock();
    while budget.check(1, inbound_budget::Releaser::Quic).is_ok() {
        let token = LegacyStreamToken::new();
        let declared = if budget
            .check(1 << 20, inbound_budget::Releaser::Quic)
            .is_ok()
        {
            1 << 20
        } else {
            1
        };
        budget.charge(
            InboundKey::QuicShort(tokens.len() as u32 + 1_000_000),
            declared,
            0,
            Duration::from_secs(3600),
            Liveness::Task(Arc::downgrade(&token)),
        );
        tokens.push(token);
    }
    tokens
}

/// Plan case (b), and a Short over one packet on a full budget (a smaller one is not charged). With QUIC holding the room a Short
/// and a UDP stream are refused and counted, the UDP id stays refused, and the
/// connection stays up. With the room back a Short is delivered.
#[test]
fn a_budget_full_of_quic_charges_refuses_and_keeps_the_connection() {
    run(
        "quic-data-full",
        44,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let tokens = fill_budget(&rig);
            let charged = rig.budget();
            // Sent or stopped: a stop can arrive after the ack, so a sender cannot rely
            // on seeing it.
            let _outcome = rig
                .path
                .peer()
                .send_short_bounded(&rig.path.time, SEND_LIMIT, &bytes_of(2000))
                .await;
            rig.path.settle(Duration::from_secs(1)).await;
            assert_eq!(
                rig.path
                    .ours()
                    .stats()
                    .shared_count(O::RefusedConnectionStreams)
                    + rig
                        .path
                        .ours()
                        .stats()
                        .shared_count(O::RefusedConnectionBytes),
                1
            );
            assert_eq!(rig.budget(), charged, "nothing charged");
            assert!(rig.recv_soon().await.is_none());

            let id = StreamId::next();
            feed(&mut rig.e, fragment(id, 100_000, 1)).await;
            assert!(holds_no_state(&rig.e, id));
            assert_eq!(
                rig.outcome(O::RefusedConnectionStreams) + rig.outcome(O::RefusedConnectionBytes),
                1
            );
            assert!(
                !rig.e.conn.closed_by_stream_refusals,
                "the connection stays up"
            );

            drop(tokens);
            rig.e.conn.sweep_streams();
            assert_eq!(rig.budget(), (0, 0));
            feed(&mut rig.e, fragment(id, 100_000, 1)).await;
            assert!(holds_no_state(&rig.e, id), "a refused id stays refused");
            rig.path.send_short(b"fits").await;
            assert_eq!(rig.e.conn.recv().await.unwrap(), b"fits");
        },
    );
}

/// Plan case (c). An incomplete QUIC stream with no progress is evicted by the
/// path's own timer, and a UDP stream that did not fit then does.
#[test]
fn a_stalled_quic_stream_is_released_and_a_udp_stream_then_fits() {
    run(
        "quic-data-stalled",
        45,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let id = StreamId::next();
            let _tx = rig
                .path
                .partial_stream(id, MAX_STREAM_TOTAL_BYTES, 3000)
                .await;
            while rig.registry.orphan_count() == 0 {
                assert!(rig.recv_soon().await.is_none());
            }
            let refused = StreamId::next();
            feed(&mut rig.e, fragment(refused, MAX_STREAM_TOTAL_BYTES, 1)).await;
            assert_eq!(rig.outcome(O::RefusedConnectionBytes), 1);

            rig.path
                .settle(streaming_handle_idle_timeout() + Duration::from_secs(2))
                .await;
            assert_eq!(rig.path.ours().stats().shared_count(O::EvictedIdle), 1);
            rig.registry.gc_expired();
            rig.e.conn.sweep_streams();
            assert_eq!(rig.budget(), (0, 0));

            let fits = StreamId::next();
            feed(&mut rig.e, fragment(fits, MAX_STREAM_TOTAL_BYTES, 1)).await;
            assert!(rig.e.conn.streaming_handles.contains_key(&fits));
        },
    );
}

/// UDP first, then QUIC: the QUIC stream is cancelled and counted, and the UDP one
/// goes on.
#[test]
fn a_stream_id_in_use_on_udp_is_refused_on_the_path() {
    run(
        "quic-data-both-udp",
        46,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let id = StreamId::next();
            feed(&mut rig.e, fragment(id, 100, 1)).await;
            assert_eq!(rig.registry.orphan_count(), 1);
            rig.path.send_stream(id, Some(b"meta"), bytes_of(50)).await;
            rig.path.settle(Duration::from_millis(100)).await;
            assert!(
                rig.recv_soon().await.is_none(),
                "no metadata from a refused stream"
            );
            assert_eq!(rig.outcome(O::StreamIdOnBothPaths), 1);
            assert!(
                !rig.e
                    .conn
                    .inbound_budget
                    .lock()
                    .contains(InboundKey::Quic(id))
            );
            let (handle, _) = rig
                .e
                .conn
                .streaming_handles
                .get(&id)
                .expect("the UDP stream");
            assert!(!handle.is_cancelled());
            assert_eq!(rig.registry.orphan_count(), 1);
        },
    );
}

/// QUIC first, then UDP: the fragment is dropped and the QUIC stream goes on. The
/// same id again on the path, once released, is refused and counted.
#[test]
fn a_stream_id_used_on_the_path_is_refused_on_udp_and_on_the_path_again() {
    run(
        "quic-data-both-quic",
        47,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let id = StreamId::next();
            let payload = bytes_of(50);
            rig.path.send_stream(id, None, payload.clone()).await;
            while rig.registry.orphan_count() == 0 {
                assert!(rig.recv_soon().await.is_none());
            }
            feed(&mut rig.e, fragment(id, 50, 1)).await;
            assert!(holds_no_state(&rig.e, id));
            assert_eq!(
                rig.outcome(O::StreamIdOnBothPaths),
                0,
                "counted in the arm only"
            );
            let stream = claim((&rig.registry, rig.peer_addr()), id).await;
            assert_eq!(stream.assemble().await.unwrap(), payload.to_vec());
            drop(stream);
            rig.e.conn.sweep_streams();
            assert_eq!(rig.budget(), (0, 0));

            rig.path.send_stream(id, Some(b"again"), bytes_of(50)).await;
            rig.path.settle(Duration::from_millis(100)).await;
            assert!(rig.recv_soon().await.is_none());
            assert_eq!(rig.outcome(O::StreamIdOnBothPaths), 1);
            assert_eq!(rig.registry.orphan_count(), 0);
            assert_eq!(rig.budget(), (0, 0));
        },
    );
}

/// `recv` is dropped while it waits, then polled again once something has
/// arrived. A cancelled one loses nothing and repeats nothing: Shorts and a
/// Stream's metadata all come out, once each. The test counts the polls that
/// were abandoned and requires some, so it cannot pass without a cancellation.
#[test]
fn a_cancelled_recv_loses_nothing() {
    run(
        "quic-data-cancel",
        48,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            const COUNT: usize = 40;
            let mut cancelled = 0usize;
            let mut got = Vec::new();
            let mut want = Vec::new();
            for i in 0..COUNT {
                // Nothing has been sent: `recv` is polled, then dropped.
                tokio::select! {
                    biased;
                    message = rig.e.conn.recv() => panic!("unexpected message {message:?}"),
                    () = tokio::task::yield_now() => cancelled += 1,
                }
                if i % 10 == 9 {
                    let id = StreamId::next();
                    let meta = format!("stream {i}").into_bytes();
                    rig.path
                        .send_stream(id, Some(meta.as_slice()), bytes_of(50))
                        .await;
                    want.push(meta);
                } else {
                    let message = format!("message {i}").into_bytes();
                    rig.path.send_short(&message).await;
                    want.push(message);
                }
                // The send is acknowledged before the reader task has delivered
                // it, so `recv` waits and is dropped again before it returns.
                for _ in 0..100_000 {
                    tokio::select! {
                        biased;
                        message = rig.e.conn.recv() => {
                            got.push(message.unwrap());
                            break;
                        }
                        () = tokio::task::yield_now() => cancelled += 1,
                    }
                }
            }
            assert!(cancelled > 0, "no recv was ever cancelled");
            got.sort();
            want.sort();
            assert_eq!(got, want);
            assert!(rig.recv_soon().await.is_none());
        },
    );
}

/// A Stream that arrives on a connection with no registry is cancelled and its
/// charge released, as every other cancel branch releases it.
#[test]
fn a_stream_with_no_registry_is_cancelled_and_its_charge_released() {
    run(
        "quic-data-noregistry",
        51,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            rig.e.conn.inbound_stream_registry = None;
            rig.path
                .send_stream(StreamId::next(), Some(b"meta"), bytes_of(50))
                .await;
            rig.path.settle(Duration::from_millis(100)).await;
            assert!(rig.recv_soon().await.is_none());
            assert_eq!(rig.budget(), (0, 0), "the charge was released");
            assert_eq!(rig.registry.orphan_count(), 0);
        },
    );
}

/// With this node's bit at 0 a data frame is refused before it is charged, so it
/// neither takes room from the UDP session nor reaches `recv`.
#[test]
fn a_path_with_the_bit_at_zero_refuses_data_before_charging() {
    run(
        "quic-data-shadow",
        49,
        QuicPathLimits::SHADOW,
        |mut rig| async move {
            let _sent = rig
                .path
                .peer()
                .send_short_bounded(&rig.path.time, SEND_LIMIT, b"refused")
                .await;
            rig.path.settle(Duration::from_secs(1)).await;
            assert_eq!(rig.path.ours().stats().count(Q::RefusedNotAcceptingData), 1);
            assert_eq!(rig.budget(), (0, 0));
            assert!(rig.recv_soon().await.is_none());
        },
    );
}

/// A connection that drops mid-transfer leaves no charge and no registry row, for
/// a stream still arriving and for a whole one nobody claimed, which would
/// otherwise wait out the orphan timeout.
#[test]
fn a_connection_drop_leaves_no_charge_and_no_registry_row() {
    run(
        "quic-data-drop",
        50,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let (partial, whole) = (StreamId::next(), StreamId::next());
            let _tx = rig.path.partial_stream(partial, 100_000, 3000).await;
            rig.path.send_stream(whole, None, bytes_of(50)).await;
            while rig.registry.orphan_count() < 2 {
                assert!(rig.recv_soon().await.is_none());
            }
            let budget = rig.e.conn.inbound_budget();
            let Rig {
                e,
                mut path,
                registry,
            } = rig;
            drop(e);
            path.end_ours();
            path.time.sleep(Duration::from_secs(1)).await;
            registry.gc_expired();
            assert_eq!(registry.orphan_count(), 0);
            budget.lock().release_gone(inbound_budget::Releaser::Quic);
            assert_eq!((budget.stream_count(), budget.charged_bytes()), (0, 0));
        },
    );
}

/// The session ends before the walk over what is live, so the connection is closed
/// first and a helper holding the path ends with it (plan 4.3). On one thread nothing
/// can run between the two, so no behaviour can see the order: this test is
/// textual, a pin on the source and not a test of what happens.
#[test]
fn a_connection_drop_ends_the_session_before_it_walks_the_streams() {
    let source = include_str!("../../peer_connection.rs");
    let body = inbound_budget::pin::fn_body(
        source,
        "impl<S, T: TimeSource> Drop for PeerConnection<S, T> {",
    );
    let end = body
        .find("self.quic_path.take()")
        .expect("the session ends");
    let walk = body.find("live_buffer_streams()").expect("the walk");
    assert!(end < walk);
}

/// `recv`'s two QUIC data arms are guarded by whether the connection has the
/// channel. The macro-level test uses literal `false` guards, so only the source
/// shows that `recv` passes these two: without them a connection with no path
/// would poll a channel that does not exist. A textual pin.
#[test]
fn recvs_data_arms_are_guarded_by_the_channels_they_read() {
    let source = include_str!("../../peer_connection.rs");
    let body =
        inbound_budget::pin::fn_body(source, "pub async fn recv(&mut self) -> Result<Vec<u8>> {");
    for arm in [
        "next_on(&mut self.quic_shorts), if self.quic_shorts.is_some() =>",
        "next_on(&mut self.quic_streams), if self.quic_streams.is_some() =>",
    ] {
        assert!(
            body.lines()
                .any(|line| !line.trim_start().starts_with("//") && line.contains(arm)),
            "recv lost the guard `{arm}`"
        );
    }
}

/// A complete stream held past its age limit is counted from the connection's
/// sweep, on the connection's own counters, once.
#[tokio::test(start_paused = true)]
async fn the_sweep_counts_a_complete_stream_held_past_its_age() {
    let mut e = single(CAPABLE);
    let token = LegacyStreamToken::new();
    assert!(token.mark_complete());
    e.conn.inbound_budget.lock().charge(
        InboundKey::QuicShort(1),
        10,
        0,
        Duration::ZERO,
        Liveness::Task(Arc::downgrade(&token)),
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    e.conn.sweep_streams();
    e.conn.sweep_streams();
    assert_eq!(
        e.conn.inbound_outcomes[O::CompleteChargePastAge as usize],
        1
    );
    assert_eq!(e.conn.inbound_budget.stream_count(), 1, "still held");
}

/// With QUIC off there is nothing to read and the two arms stay disabled.
#[test]
fn a_connection_without_upgrades_has_no_inbox() {
    let e = single(CAPABLE);
    assert!(e.conn.quic_shorts.is_none() && e.conn.quic_streams.is_none());
}
