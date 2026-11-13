//! [`NativePeerConnection`] against the [`PeerConnectionApi`] contract, over a
//! real QUIC connection on VirtualTime. The node is the server; the peer's side
//! is a plain [`QuicPath`] that carries data.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;

use super::*;
use crate::operations::orphan_streams::OrphanStreamRegistry;
use crate::simulation::VirtualTime;
use crate::transport::StreamId;
use crate::transport::boundary::StreamSendOutcome;
use crate::transport::metrics::ConnIdentity;
use crate::transport::quic::framing::FrameHeader;
use crate::transport::quic::path::{
    BoundedSend, QuicSendError, QuicSendEvent, QuicSendResult, SendFailure, SendKind, stop_code,
};
use crate::transport::quic::upgrade::data_route::SHORT_QUEUE_CAPACITY;
use crate::transport::quic::upgrade::driver::tests::{NativeServer, native_server, run_bounded};

const SEND_LIMIT: Duration = Duration::from_secs(30);
const VERSION: (u8, u8, u16) = (0, 2, 160);

struct Rig {
    conn: NativePeerConnection,
    peer: NativeServer,
    registry: Arc<OrphanStreamRegistry>,
}

impl Rig {
    fn time(&self) -> VirtualTime {
        self.peer.time.clone()
    }

    async fn settle(&self, by: Duration) {
        self.peer.time.sleep(by).await;
    }

    async fn peer_sends_short(&self, payload: &[u8]) {
        let sent = self
            .peer
            .peer
            .send_short_bounded(&self.peer.time, SEND_LIMIT, payload)
            .await;
        assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
    }

    async fn peer_sends_stream(&self, id: StreamId, metadata: Option<&[u8]>, payload: Bytes) {
        let sent = self
            .peer
            .peer
            .send_stream_bounded(&self.peer.time, SEND_LIMIT, id, metadata, payload, None)
            .await;
        assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
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
            message = self.conn.recv() => Some(message.expect("recv must not fail")),
            () = turns => None,
        }
    }

    /// How many sends of `kind` the node's path ended as `result`.
    fn sends(&self, kind: SendKind, result: QuicSendResult) -> u64 {
        self.conn.path().stats().send_count(kind, result)
    }
}

/// Run `test` on a native connection whose peer's path has `peer_limits`.
fn run<F, Fut>(
    network: &'static str,
    peer_limits: QuicPathLimits,
    idle_timeout: Option<Duration>,
    test: F,
) where
    F: FnOnce(Rig) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    run_with(network, peer_limits, idle_timeout, None, test);
}

/// [`run`], with the node's `bandwidth-limit` set to `bandwidth_limit`.
fn run_with<F, Fut>(
    network: &'static str,
    peer_limits: QuicPathLimits,
    idle_timeout: Option<Duration>,
    bandwidth_limit: Option<usize>,
    test: F,
) where
    F: FnOnce(Rig) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    run_bounded(move || async move {
        let (peer, server) = native_server(network, peer_limits, idle_timeout).await;
        let time = peer.time.clone();
        let mut conn = NativePeerConnection::new(
            server,
            VERSION,
            PathClock::Virtual(time.clone()),
            None,
            QuicRateLimit::new(bandwidth_limit, None),
            RouteNote::default(),
        );
        let registry = Arc::new(OrphanStreamRegistry::new());
        conn.set_inbound_stream_registry(registry.clone());
        (
            time,
            test(Rig {
                conn,
                peer,
                registry,
            }),
        )
    });
}

fn bytes_of(len: usize) -> Bytes {
    (0..len).map(|i| i as u8).collect::<Vec<_>>().into()
}

/// A short message and a stream from the peer come out of `recv` as a UDP
/// connection's do: the short as itself, the stream as its metadata, with its
/// payload registered under its id.
#[test]
fn a_short_and_a_stream_from_the_peer_come_out_of_recv() {
    run(
        "native-conn-inbound",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            rig.peer_sends_short(b"hello").await;
            assert_eq!(rig.conn.recv().await.unwrap(), b"hello");

            let id = StreamId::next();
            let payload = bytes_of(5000);
            rig.peer_sends_stream(id, Some(b"announce"), payload.clone())
                .await;
            assert_eq!(rig.conn.recv().await.unwrap(), b"announce");
            let stream = rig
                .registry
                .claim_or_wait(rig.conn.remote_addr(), id, Duration::from_secs(1))
                .await
                .expect("the stream was registered");
            assert_eq!(stream.assemble().await.unwrap(), payload.to_vec());
            assert!(rig.recv_soon().await.is_none());
        },
    );
}

/// Metadata already handed to the node as a short message is not handed over
/// again with its stream, through the same record a UDP connection keeps.
#[test]
fn metadata_already_dispatched_as_a_short_is_not_dispatched_again() {
    run(
        "native-conn-dedup",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            rig.peer_sends_short(b"first").await;
            assert_eq!(rig.conn.recv().await.unwrap(), b"first");
            rig.peer_sends_stream(StreamId::next(), Some(b"first"), bytes_of(10))
                .await;
            assert!(rig.recv_soon().await.is_none(), "already dispatched");

            rig.peer_sends_stream(StreamId::next(), Some(b"second"), bytes_of(10))
                .await;
            assert_eq!(rig.conn.recv().await.unwrap(), b"second");
        },
    );
}

/// `recv` loses nothing when it is dropped mid-wait, over shorts and streams.
#[test]
fn a_cancelled_recv_loses_nothing() {
    run(
        "native-conn-cancel",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            const COUNT: usize = 30;
            let mut cancelled = 0usize;
            let (mut got, mut want) = (Vec::new(), Vec::new());
            for i in 0..COUNT {
                tokio::select! {
                    biased;
                    message = rig.conn.recv() => panic!("unexpected message {message:?}"),
                    () = tokio::task::yield_now() => cancelled += 1,
                }
                let message = format!("message {i}").into_bytes();
                if i % 10 == 9 {
                    rig.peer_sends_stream(StreamId::next(), Some(&message), bytes_of(50))
                        .await;
                } else {
                    rig.peer_sends_short(&message).await;
                }
                want.push(message);
                for _ in 0..100_000 {
                    tokio::select! {
                        biased;
                        message = rig.conn.recv() => {
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

/// Every way the node sends reaches the peer on the path: a short of one
/// packet, a short past one packet, a stream with metadata, and a pipe of a
/// stream the node received.
#[test]
fn every_send_reaches_the_peer_on_the_path() {
    run(
        "native-conn-outbound",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            rig.conn.send_bytes(b"small".to_vec()).await.unwrap();
            let large = bytes_of(10_000).to_vec();
            rig.conn.send_bytes(large.clone()).await.unwrap();
            let mut shorts = Vec::new();
            for _ in 0..2 {
                let short = rig.peer.peer_inbound.shorts.recv().await.expect("a short");
                shorts.push(short.into_payload().to_vec());
            }
            shorts.sort_by_key(Vec::len);
            assert_eq!(shorts, vec![b"small".to_vec(), large]);

            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            let id = StreamId::next();
            let payload = bytes_of(7000);
            rig.conn
                .send_stream_data(
                    id,
                    payload.clone(),
                    Some(b"meta".to_vec().into()),
                    Some(done_tx),
                    None,
                )
                .await
                .unwrap();
            let arrival = rig
                .peer
                .peer_inbound
                .streams
                .recv()
                .await
                .expect("a stream");
            assert_eq!(arrival.stream_id, id);
            assert_eq!(arrival.metadata.as_deref(), Some(&b"meta"[..]));
            assert_eq!(arrival.handle.assemble().await.unwrap(), payload.to_vec());
            assert!(matches!(done_rx.await, Ok(StreamSendOutcome::Delivered)));

            // A stream from the peer, piped back to it under a new id.
            let inbound_id = StreamId::next();
            let piped = bytes_of(6000);
            rig.peer_sends_stream(inbound_id, Some(b"pipe me"), piped.clone())
                .await;
            assert_eq!(rig.conn.recv().await.unwrap(), b"pipe me");
            let source = rig
                .registry
                .claim_or_wait(rig.conn.remote_addr(), inbound_id, Duration::from_secs(1))
                .await
                .expect("registered");
            let outbound_id = StreamId::next();
            rig.conn
                .pipe_stream_data(outbound_id, source, Some(b"piped".to_vec().into()), None)
                .await
                .unwrap();
            let arrival = rig
                .peer
                .peer_inbound
                .streams
                .recv()
                .await
                .expect("the pipe");
            assert_eq!(arrival.stream_id, outbound_id);
            assert_eq!(arrival.handle.assemble().await.unwrap(), piped.to_vec());

            // The finished tasks are counted on `recv`'s next turn.
            rig.settle(Duration::from_secs(1)).await;
            assert!(rig.recv_soon().await.is_none());
            assert!(rig.conn.outbound.is_empty(), "every send task was reaped");
            assert_eq!(rig.sends(SendKind::Short, QuicSendResult::Sent), 1);
            assert_eq!(rig.sends(SendKind::LargeShort, QuicSendResult::Sent), 1);
            assert_eq!(rig.sends(SendKind::Stream, QuicSendResult::Sent), 1);
            assert_eq!(rig.sends(SendKind::Pipe, QuicSendResult::Sent), 1);
        },
    );
}

/// A peer that refuses this node's data closes the native connection on the
/// first refusal: the send is dropped (a stream's completion is told so),
/// `recv` ends with `PeerRefusedData`, and nothing is counted as sent on UDP.
/// On an upgrade path the same refusal would put the message on UDP.
#[test]
fn a_refused_send_closes_the_native_connection() {
    run(
        "native-conn-refused",
        QuicPathLimits::SHADOW,
        None,
        |mut rig| async move {
            // Past the 2 MiB window, so the peer's stop arrives before
            // the last byte could be acknowledged.
            let big = 3 * 1024 * 1024;
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            rig.conn
                .send_stream_data(StreamId::next(), bytes_of(big), None, Some(done_tx), None)
                .await
                .unwrap();
            assert!(matches!(done_rx.await, Ok(StreamSendOutcome::Dropped)));
            let ended = rig.conn.recv().await;
            assert!(
                matches!(ended, Err(TransportError::ConnectionClosed(_))),
                "{ended:?}"
            );
            assert_eq!(
                rig.conn.close_reason(),
                Some(CloseReason::QuicPath(QuicPathEnd::PeerRefusedData))
            );
            assert!(!rig.conn.path().sends_data());
            assert_eq!(rig.sends(SendKind::Stream, QuicSendResult::Dropped), 1);
            for kind in [
                SendKind::Short,
                SendKind::LargeShort,
                SendKind::Stream,
                SendKind::Pipe,
            ] {
                for result in [QuicSendResult::NotOpenedUdp, QuicSendResult::FailedUdp] {
                    assert_eq!(rig.sends(kind, result), 0, "{kind:?} {result:?}");
                }
            }
        },
    );
}

/// A short queued after the path stopped taking data is dropped and counted by
/// the route's worker, which has no UDP session to send it on. The refusal is
/// settled by hand and the short queued before anything yields, so the worker
/// finds the short queued and the path refusing data whichever arm it polls
/// first.
#[test]
fn a_short_the_path_no_longer_takes_is_dropped_by_the_worker() {
    run(
        "native-conn-worker-drop",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            let refused = BoundedSend::Failed(SendFailure::Error(QuicSendError::Stopped(
                u64::from(stop_code::NOT_ACCEPTING_DATA),
            )));
            let now = rig.time().now_nanos();
            rig.conn
                .path()
                .settle(SendKind::Stream, &refused, false, now);
            assert!(!rig.conn.path().sends_data());
            rig.conn.send_bytes(b"after".to_vec()).await.unwrap();
            rig.settle(Duration::from_secs(1)).await;
            assert_eq!(rig.sends(SendKind::Short, QuicSendResult::Dropped), 1);
            assert_eq!(rig.sends(SendKind::Short, QuicSendResult::Sent), 0);
            assert!(
                rig.peer.peer_inbound.shorts.try_recv().is_err(),
                "nothing reached the peer"
            );
        },
    );
}

/// A path the peer closes ends the connection: `recv` fails, and the reason
/// says the peer closed it.
#[test]
fn a_path_the_peer_closes_ends_recv_with_the_peers_reason() {
    run(
        "native-conn-peer-close",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            rig.peer.peer.close(QuicCloseCode::Downgrade);
            let ended = rig.conn.recv().await;
            assert!(
                matches!(ended, Err(TransportError::ConnectionClosed(addr)) if addr == rig.conn.remote_addr()),
                "{ended:?}"
            );
            assert_eq!(
                rig.conn.close_reason(),
                Some(CloseReason::QuicPath(QuicPathEnd::ClosedByPeer))
            );
        },
    );
}

/// What arrived before the peer closed is still delivered, whichever inbox
/// channel holds it, and only then does `recv` report the end.
#[test]
fn what_arrived_before_the_close_is_delivered_first() {
    run(
        "native-conn-drain",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            rig.peer_sends_stream(StreamId::next(), Some(b"last words"), bytes_of(100))
                .await;
            rig.peer.peer.close(QuicCloseCode::Downgrade);
            // Both channels are closed by now, with the arrival still queued.
            rig.settle(Duration::from_secs(1)).await;
            assert_eq!(rig.conn.recv().await.unwrap(), b"last words");
            let ended = rig.conn.recv().await;
            assert!(
                matches!(ended, Err(TransportError::ConnectionClosed(_))),
                "{ended:?}"
            );
        },
    );
}

/// A path quinn closes for its idle timeout ends `recv` with
/// [`CloseReason::IdleTimeout`], as a UDP connection's idle timeout does.
#[test]
fn an_idle_path_ends_recv_with_the_idle_reason() {
    run(
        "native-conn-idle",
        QuicPathLimits::DEFAULT,
        Some(Duration::from_secs(7)),
        |mut rig| async move {
            let start = rig.time().now();
            let ended = rig.conn.recv().await;
            assert!(
                matches!(ended, Err(TransportError::ConnectionClosed(_))),
                "{ended:?}"
            );
            assert!(rig.time().now() - start >= Duration::from_secs(7));
            assert_eq!(rig.conn.close_reason(), Some(CloseReason::IdleTimeout));
        },
    );
}

/// Dropping the connection closes it for the peer at once, with
/// NODE_DROPPED_PEER, while a send task still holds the path: here a pipe
/// whose source the peer keeps open, which would otherwise hold the path for
/// the source's gap.
#[test]
fn dropping_the_connection_closes_it_for_the_peer() {
    run(
        "native-conn-drop",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            let id = StreamId::next();
            let mut held = rig
                .peer
                .peer
                .connection()
                .inner()
                .open_uni()
                .await
                .expect("open");
            let header = FrameHeader::stream(id, 100_000, 0).expect("a valid header");
            held.write_all(&header.encode()).await.expect("header");
            held.write_all(&[7; 3000]).await.expect("payload");
            // `recv` registers the stream; it has no metadata to return.
            rig.settle(Duration::from_millis(500)).await;
            assert!(rig.recv_soon().await.is_none());
            let source = rig
                .registry
                .claim_or_wait(rig.conn.remote_addr(), id, Duration::from_secs(1))
                .await
                .expect("registered");
            rig.conn
                .pipe_stream_data(StreamId::next(), source, None, None)
                .await
                .unwrap();
            rig.settle(Duration::from_millis(500)).await;
            drop(rig.conn);
            rig.peer.time.sleep(Duration::from_secs(1)).await;
            let reason = rig.peer.peer.connection().inner().close_reason();
            assert!(
                matches!(
                    &reason,
                    Some(quinn::ConnectionError::ApplicationClosed(close))
                        if close.error_code.into_inner()
                            == u64::from(QuicCloseCode::NodeDroppedPeer.code())
                ),
                "{reason:?}"
            );
            drop(held);
        },
    );
}

/// The version is the Hello's, and the key is the one proven in the
/// handshake: a promotion under it is `Same`, under another `Refused`.
#[test]
fn the_version_is_the_hellos_and_the_key_the_handshakes() {
    run(
        "native-conn-identity",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            assert_eq!(rig.conn.remote_version(), Some(VERSION));
            let remote = rig.conn.remote_addr();
            TRANSPORT_METRICS.watch_conn_identity(remote);
            rig.conn.set_peer_identity(rig.peer.peer_key.clone());
            let other = crate::transport::crypto::TransportKeypair::new()
                .public()
                .clone();
            rig.conn.set_peer_identity(other);
            assert_eq!(
                TRANSPORT_METRICS.conn_identity_from(remote),
                vec![
                    ConnIdentity::TransportKeySame,
                    ConnIdentity::TransportKeyRefused
                ]
            );
            assert_eq!(
                rig.conn.peer_key, rig.peer.peer_key,
                "the proven key is kept"
            );
        },
    );
}

/// The receipt stamp ages while the path is quiet and is renewed when the
/// peer is heard from on it.
#[test]
fn the_receipt_stamp_moves_with_the_path() {
    run(
        "native-conn-receipt",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            let stamp = rig
                .conn
                .receipt_stamp()
                .expect("a native connection has a stamp");
            rig.settle(Duration::from_secs(20)).await;
            assert!(
                stamp.since_last_receipt() >= Duration::from_secs(18),
                "quiet"
            );
            rig.peer_sends_short(b"ping").await;
            assert_eq!(rig.conn.recv().await.unwrap(), b"ping");
            rig.settle(Duration::from_secs(3)).await;
            assert!(
                stamp.since_last_receipt() <= Duration::from_secs(3),
                "{:?}",
                stamp.since_last_receipt()
            );
        },
    );
}

/// A full short queue refuses the send with a transient error and the
/// connection carries on: nothing is closed, nothing counts as a UDP overflow,
/// and the refused message is counted as dropped. `send_bytes_observed` takes
/// the same route. Nothing yields while the queue fills, so the route's worker
/// takes nothing in between.
#[test]
fn a_full_short_queue_refuses_the_send_and_keeps_the_connection() {
    run(
        "native-conn-queue-full",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            let mut queued = 0usize;
            let refused = loop {
                match rig.conn.send_bytes(format!("m{queued}").into_bytes()).await {
                    Ok(()) => queued += 1,
                    Err(error) => break error,
                }
                assert!(queued <= SHORT_QUEUE_CAPACITY, "the queue never filled");
            };
            assert_eq!(queued, SHORT_QUEUE_CAPACITY);
            assert!(
                matches!(refused, TransportError::SendQueueFull(addr) if addr == rig.conn.remote_addr()),
                "{refused:?}"
            );
            assert!(!refused.closes_connection(), "a full queue is transient");
            let (done_tx, _done_rx) = tokio::sync::oneshot::channel();
            let observed = rig.conn.send_bytes_observed(b"x".to_vec(), done_tx).await;
            assert!(
                matches!(observed, Err(TransportError::SendQueueFull(_))),
                "{observed:?}"
            );
            assert_eq!(rig.sends(SendKind::Short, QuicSendResult::Dropped), 2);
            assert_eq!(
                rig.conn
                    .path()
                    .stats()
                    .send_event_count(QuicSendEvent::ShortOverflowUdp),
                0
            );

            // The queue drains, the connection is still up and takes more.
            let mut received = 0usize;
            while received < SHORT_QUEUE_CAPACITY {
                rig.peer.peer_inbound.shorts.recv().await.expect("a short");
                received += 1;
            }
            assert!(rig.recv_soon().await.is_none(), "recv did not end");
            rig.conn.send_bytes(b"again".to_vec()).await.unwrap();
            let again = rig.peer.peer_inbound.shorts.recv().await.expect("a short");
            assert_eq!(again.into_payload().as_ref(), b"again");
        },
    );
}

/// Once the path has ended, a send fails with `ConnectionClosed`, from both
/// `send_bytes` and `send_bytes_observed`.
#[test]
fn a_send_after_the_path_ends_reports_the_connection_closed() {
    run(
        "native-conn-send-after-end",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            rig.peer.peer.close(QuicCloseCode::Downgrade);
            assert!(rig.conn.recv().await.is_err());
            rig.settle(Duration::from_secs(1)).await;
            let sent = rig.conn.send_bytes(b"late".to_vec()).await;
            assert!(
                matches!(&sent, Err(error @ TransportError::ConnectionClosed(_)) if error.closes_connection()),
                "{sent:?}"
            );
            let (done_tx, _done_rx) = tokio::sync::oneshot::channel();
            let observed = rig
                .conn
                .send_bytes_observed(b"late".to_vec(), done_tx)
                .await;
            assert!(
                matches!(observed, Err(TransportError::ConnectionClosed(_))),
                "{observed:?}"
            );
            assert_eq!(rig.sends(SendKind::Short, QuicSendResult::Dropped), 0);
        },
    );
}

/// A stream the peer sent whole that nobody claimed is cancelled when the
/// connection drops, and so is one still arriving, as a UDP connection does:
/// neither waits out the orphan timeout.
#[test]
fn dropping_the_connection_cancels_its_unclaimed_streams() {
    run(
        "native-conn-unclaimed",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            let (whole, partial) = (StreamId::next(), StreamId::next());
            rig.peer_sends_stream(whole, None, bytes_of(50)).await;
            let mut held = rig
                .peer
                .peer
                .connection()
                .inner()
                .open_uni()
                .await
                .expect("open");
            let header = FrameHeader::stream(partial, 100_000, 0).expect("a valid header");
            held.write_all(&header.encode()).await.expect("header");
            held.write_all(&[7; 3000]).await.expect("payload");
            while rig.registry.orphan_count() < 2 {
                assert!(rig.recv_soon().await.is_none());
                rig.settle(Duration::from_millis(100)).await;
            }
            let registry = Arc::clone(&rig.registry);
            drop(rig.conn);
            rig.peer.time.sleep(Duration::from_secs(1)).await;
            registry.gc_expired();
            assert_eq!(registry.orphan_count(), 0);
            drop(held);
        },
    );
}

/// With a bandwidth limit the connection paces its data at it, on the path's
/// clock; without one, nothing is paced.
#[test]
fn the_bandwidth_limit_paces_a_native_connections_data() {
    run(
        "native-conn-unpaced",
        QuicPathLimits::DEFAULT,
        None,
        |rig| async move {
            assert!(!rig.conn.path().is_paced());
        },
    );
    run_with(
        "native-conn-paced",
        QuicPathLimits::DEFAULT,
        None,
        Some(100_000),
        |mut rig| async move {
            assert!(rig.conn.path().is_paced());
            let start = rig.time().now();
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            rig.conn
                .send_stream_data(
                    StreamId::next(),
                    bytes_of(400_000),
                    None,
                    Some(done_tx),
                    None,
                )
                .await
                .unwrap();
            let arrival = rig
                .peer
                .peer_inbound
                .streams
                .recv()
                .await
                .expect("a stream");
            assert_eq!(arrival.handle.assemble().await.unwrap().len(), 400_000);
            assert!(matches!(done_rx.await, Ok(StreamSendOutcome::Delivered)));
            let took = rig.time().now() - start;
            // 400 kB at 100 kB/s, less the bucket's burst.
            assert!(took >= Duration::from_secs(3), "took {took:?}");
        },
    );
}

/// A path whose sends keep timing out with nothing acknowledged closes, and
/// `recv` reports why.
#[test]
fn a_stalled_path_ends_recv_with_the_stall_reason() {
    run(
        "native-conn-stall",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            let later = rig.time().now_nanos() + Duration::from_secs(60).as_nanos() as u64;
            let timeout = BoundedSend::Failed(SendFailure::TimedOut);
            // The stall count closes the path after a fixed number in a row
            // (route::STALLED_SENDS_LIMIT, 8).
            let mut settled = 0;
            while rig.conn.path().quinn_connection().close_reason().is_none() {
                rig.conn
                    .path()
                    .settle(SendKind::Stream, &timeout, false, later);
                settled += 1;
                assert!(settled <= 8, "the path did not close");
            }
            assert!(rig.conn.recv().await.is_err());
            assert_eq!(
                rig.conn.close_reason(),
                Some(CloseReason::QuicPath(QuicPathEnd::Stalled))
            );
            assert!(
                !rig.conn.path().is_suspect(),
                "a native path is never suspect"
            );
        },
    );
}

/// A peer that goes dark while the node has data outstanding ends the
/// connection well before quinn's idle timeout (24 h on VirtualTime), with a
/// reason that is not a loss or an idle timeout.
#[test]
fn a_dark_native_path_closes() {
    use crate::transport::in_memory_socket::{
        PacketDeliveryDecision, set_packet_delivery_callback,
    };
    run(
        "native-conn-dark",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            let peer = rig.conn.remote_addr();
            set_packet_delivery_callback(
                "native-conn-dark",
                Some(Arc::new(move |_, sender, _, _| {
                    if sender == peer {
                        PacketDeliveryDecision::Drop
                    } else {
                        PacketDeliveryDecision::Deliver
                    }
                })),
            );
            let start = rig.time().now();
            rig.conn
                .send_stream_data(StreamId::next(), bytes_of(100_000), None, None, None)
                .await
                .unwrap();
            let ended = rig.conn.recv().await;
            let took = rig.time().now() - start;
            set_packet_delivery_callback("native-conn-dark", None);
            assert!(ended.is_err());
            assert!(took < Duration::from_secs(60), "took {took:?}");
            let reason = rig.conn.close_reason();
            assert!(
                matches!(
                    reason,
                    Some(CloseReason::QuicPath(
                        QuicPathEnd::NotAcknowledging | QuicPathEnd::Stalled
                    ))
                ),
                "{reason:?}"
            );
        },
    );
}

/// A close records its cause once: the first close names it and a later one
/// changes nothing, and `recv` reports it. A close with no cause reads as
/// `ClosedLocally`.
#[test]
fn a_close_reports_the_cause_it_was_given_first() {
    run(
        "native-conn-cause",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            let path = rig.conn.path();
            path.close_for(QuicCloseCode::SendAbortLimit, QuicPathEnd::SendAborts);
            path.close_for(QuicCloseCode::OverBudget, QuicPathEnd::PeerOverBudget);
            assert_eq!(path.closed_for(), Some(QuicPathEnd::SendAborts));
            assert!(rig.conn.recv().await.is_err());
            assert_eq!(
                rig.conn.close_reason(),
                Some(CloseReason::QuicPath(QuicPathEnd::SendAborts))
            );
        },
    );
    run(
        "native-conn-no-cause",
        QuicPathLimits::DEFAULT,
        None,
        |mut rig| async move {
            rig.conn.path().close(QuicCloseCode::Downgrade);
            assert!(rig.conn.recv().await.is_err());
            assert_eq!(
                rig.conn.close_reason(),
                Some(CloseReason::QuicPath(QuicPathEnd::ClosedLocally))
            );
        },
    );
}
