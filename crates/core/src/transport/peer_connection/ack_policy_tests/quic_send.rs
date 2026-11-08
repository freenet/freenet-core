//! A connection's data on a QUIC path, and what goes on UDP when the path cannot
//! take it (#5940 e5b-6). A real path on `VirtualTime` and a real connection, with
//! the route the session would hand it. The peer is a path of its own, read
//! directly, and UDP is the connection's wire. The connection runs on `RealTime`,
//! which barely moves here, so only the path's deadlines come due.

use std::time::Duration;

use futures::FutureExt;
use tokio::sync::oneshot;

use super::*;
use crate::transport::StreamSendOutcome;
use crate::transport::peer_connection::streaming::StreamHandle;
use crate::transport::peer_connection::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;
use crate::transport::quic::path::tests::DropFrom;
use crate::transport::quic::path::{
    QuicInboundOutcome as Q, QuicPathLimits, QuicSendEvent as E, QuicSendResult as R, SendKind,
    data_channels, stop_code,
};
use crate::transport::quic::upgrade::data_route::{DataRoute, RouteNote, SHORT_QUEUE_CAPACITY};
use crate::transport::quic::upgrade::driver::PathClock;
use crate::transport::quic::upgrade::driver::tests::{DataPath, data_path_to, run_bounded};

struct Rig {
    e: End,
    path: DataPath,
    route: DataRoute,
    worker: tokio::task::JoinHandle<()>,
}

impl Rig {
    fn stats(&self) -> &crate::transport::quic::path::QuicPathStats {
        self.route.path().stats()
    }

    fn flightsize(&self) -> usize {
        self.e.conn.remote_conn.congestion_controller.flightsize()
    }

    /// The data this end put on UDP since the last call.
    fn udp_data(&mut self) -> UdpData {
        let mut data = UdpData::default();
        for message in self.e.drain_sent() {
            if let SymmetricMessagePayload::ShortMessage { payload } = &message.payload {
                data.ids.push(message.packet_id);
                data.shorts.push(payload.to_vec());
            } else if let SymmetricMessagePayload::StreamFragment {
                stream_id, payload, ..
            } = &message.payload
            {
                data.ids.push(message.packet_id);
                data.stream_ids.push(*stream_id);
                data.stream_bytes.extend_from_slice(payload);
            }
        }
        data
    }

    /// Every `Short` the peer has received so far.
    fn delivered_shorts(&mut self) -> Vec<Vec<u8>> {
        let inbound = self.path.peer_inbound();
        let mut delivered = Vec::new();
        while let Ok(short) = inbound.shorts.try_recv() {
            delivered.push(short.payload().to_vec());
        }
        delivered
    }

    /// Acknowledge `ids` as the peer's UDP session would, and let `recv` read it.
    async fn ack(&mut self, ids: &[u32]) {
        for (packet_id, receipts) in (9_000..).zip(ids.chunks(32)) {
            let packet = SymmetricMessage::serialize_msg_to_packet_data(
                packet_id,
                SymmetricMessagePayload::NoOp,
                &self.e.inbound_key,
                receipts.to_vec(),
            )
            .expect("encrypt");
            let queued = self
                .e
                .queued(PacketData::<UnknownEncryption>::from_buf(packet.data()));
            let inbound = self.e.inbound.clone();
            inbound.send(queued).await.expect("deliver");
        }
        let turns = async {
            for _ in 0..256 {
                tokio::task::yield_now().await;
            }
        };
        tokio::select! {
            biased;
            _ = self.e.conn.recv() => {}
            () = turns => {}
        }
    }

    /// Run `recv` long enough to take the result of every finished send task.
    async fn collect_tasks(&mut self) {
        self.ack(&[]).await;
    }

    /// Fill every uni stream the peer allows with one it is still reading. The
    /// next send on the path cannot open a stream.
    fn hold_stream_credit(&self) -> impl Future<Output = Vec<quinn::SendStream>> + use<> {
        let connection = self.route.path().connection().inner().clone();
        let time = self.path.time.clone();
        async move {
            let mut held = Vec::new();
            for _ in 0..MAX_UNI_STREAMS {
                let mut stream = connection.open_uni().await.expect("open");
                let header =
                    FrameHeader::stream(StreamId::next(), 1000, 0).expect("a valid header");
                stream.write_all(&header.encode()).await.expect("header");
                held.push(stream);
            }
            time.sleep(Duration::from_millis(50)).await;
            held
        }
    }
}

use crate::transport::quic::config::{MAX_UNI_STREAMS, STREAMED_SEND_LIMIT};
use crate::transport::quic::framing::FrameHeader;
use crate::transport::quic::path::STALLED_SENDS_LIMIT;

#[derive(Default)]
struct UdpData {
    ids: Vec<u32>,
    shorts: Vec<Vec<u8>>,
    stream_ids: Vec<StreamId>,
    stream_bytes: Vec<u8>,
}

fn message(i: u32) -> Vec<u8> {
    let mut bytes = vec![7u8; 200];
    bytes[..4].copy_from_slice(&i.to_be_bytes());
    bytes
}

fn sorted(mut messages: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    messages.sort();
    messages
}

/// Bytes that show where in a message each one sits.
fn bytes_of(len: usize) -> Bytes {
    (0..len).map(|i| (i / 7) as u8).collect::<Vec<_>>().into()
}

fn run<F, Fut>(network: &'static str, subnet: u8, peer_limits: QuicPathLimits, test: F)
where
    F: FnOnce(Rig) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    run_bounded(move || async move {
        let mut e = single(CAPABLE);
        let (data, inbox) = data_channels();
        e.conn.attach_quic_inbox(inbox);
        let path = data_path_to(
            network,
            subnet,
            QuicPathLimits::DEFAULT,
            peer_limits,
            e.conn.inbound_budget(),
            data,
        )
        .await;
        let ours = path.ours_handle();
        ours.enable_data();
        let time = path.time.clone();
        let (route, worker) = DataRoute::new(
            ours,
            PathClock::Virtual(time.clone()),
            Arc::new(e.conn.udp_sender()),
            RouteNote::default(),
        );
        e.conn.quic_route_for_test = Some(route.clone());
        let worker = tokio::spawn(worker);
        (
            time,
            test(Rig {
                e,
                path,
                route,
                worker,
            }),
        )
    });
}

/// With the route up, each kind of message arrives from the path and UDP carries
/// no data at all, with nothing in flight. The result of each send is `Sent`,
/// under its kind, and the stream's sender hears `Delivered`.
#[test]
fn a_message_of_each_kind_goes_on_the_path_and_not_on_udp() {
    run(
        "quic-send-each-kind",
        50,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let short = message(1);
            rig.e.conn.send_serialized(short.clone()).await.unwrap();

            let large = vec![3u8; 4000];
            rig.e.conn.send_serialized(large.clone()).await.unwrap();

            let id = StreamId::next();
            let stream = bytes_of(50_000);
            let (done, heard) = oneshot::channel();
            rig.e
                .conn
                .outbound_stream_with_id(
                    id,
                    stream.clone(),
                    Some(Bytes::from_static(b"meta")),
                    Some(done),
                    None,
                )
                .await;

            let piped_id = StreamId::next();
            let piped = StreamHandle::new(StreamId::next(), 1000);
            piped.push_fragment(1, bytes_of(1000)).unwrap();
            rig.e
                .conn
                .pipe_stream_to_remote(
                    piped_id,
                    crate::transport::IncomingStream::from(piped),
                    None,
                    None,
                )
                .await;

            rig.path.settle(Duration::from_secs(2)).await;
            rig.collect_tasks().await;

            assert_eq!(rig.delivered_shorts(), sorted(vec![short, large]));
            let inbound = rig.path.peer_inbound();
            let first = inbound.streams.try_recv().expect("the stream arrived");
            assert_eq!(first.stream_id, id);
            assert_eq!(first.metadata.as_deref(), Some(&b"meta"[..]));
            assert_eq!(first.handle.assemble().await.unwrap(), stream.to_vec());
            let second = inbound.streams.try_recv().expect("the pipe arrived");
            assert_eq!(second.stream_id, piped_id);
            assert_eq!(second.handle.assemble().await.unwrap(), bytes_of(1000));

            assert_eq!(heard.await.unwrap(), StreamSendOutcome::Delivered);
            let udp = rig.udp_data();
            assert!(udp.ids.is_empty(), "UDP carried data: {:?}", udp.ids);
            assert_eq!(rig.flightsize(), 0);
            for kind in SendKind::ALL {
                assert_eq!(rig.stats().send_count(kind, R::Sent), 1, "{kind:?}");
            }
            assert_eq!(rig.e.conn.telemetry_test_counts.outbound_streams_failed, 0);
        },
    );
}

/// The queue takes its capacity and the rest goes on UDP at once, each counted.
/// Every message arrives once, by one route or the other.
#[test]
fn a_full_queue_overflows_to_udp_and_nothing_is_lost() {
    run(
        "quic-send-overflow",
        51,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let over = 5;
            let total = SHORT_QUEUE_CAPACITY as u32 + over;
            // No await that yields, so the worker takes nothing between these.
            for i in 0..total {
                rig.e.conn.send_serialized(message(i)).await.unwrap();
            }
            assert_eq!(
                rig.stats().send_event_count(E::ShortOverflowUdp),
                u64::from(over)
            );
            let udp = rig.udp_data();
            assert_eq!(
                sorted(udp.shorts),
                sorted(
                    (SHORT_QUEUE_CAPACITY as u32..total)
                        .map(message)
                        .collect::<Vec<_>>()
                ),
                "the newest went on UDP, each once"
            );
            rig.path.settle(Duration::from_secs(2)).await;
            let on_path = rig.delivered_shorts();
            assert_eq!(
                sorted(on_path),
                sorted(
                    (0..SHORT_QUEUE_CAPACITY as u32)
                        .map(message)
                        .collect::<Vec<_>>()
                ),
                "the queue reached the peer, each once"
            );
            assert_eq!(
                rig.stats().send_count(SendKind::Short, R::Sent),
                SHORT_QUEUE_CAPACITY as u64
            );
        },
    );
}

/// A send on a route never waits in the connection's task, however stuck the path
/// is: with every stream credit held, nothing of the queue can leave, so each
/// message past its capacity would wait if the queue were awaited. The first poll
/// of each `send_serialized` must finish it.
#[test]
fn a_short_never_waits_for_the_path() {
    run(
        "quic-send-no-wait",
        52,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let _held = rig.hold_stream_credit().await;
            for i in 0..(SHORT_QUEUE_CAPACITY as u32 * 2 + 64) {
                // The wire is a tokio channel, which yields once a poll has used
                // its budget. That is the test socket, not the route.
                if i % 32 == 0 {
                    tokio::task::yield_now().await;
                }
                rig.e
                    .conn
                    .send_serialized(message(i))
                    .now_or_never()
                    .expect("a short must not wait for the path")
                    .unwrap();
            }
        },
    );
}

/// A path closed with sends in flight and the queue full: nothing is lost and
/// nothing goes twice. Every message goes on UDP once, the peer holds none of them,
/// and the sends the path took report `NotOpened`.
#[test]
fn a_path_closed_with_a_full_queue_hands_every_message_to_udp_once() {
    run(
        "quic-send-close-drain",
        53,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let _held = rig.hold_stream_credit().await;
            let total = SHORT_QUEUE_CAPACITY as u32;
            for i in 0..total {
                rig.e.conn.send_serialized(message(i)).await.unwrap();
            }
            // The worker takes what it can start, and those sends wait for credit.
            rig.path.settle(Duration::from_millis(50)).await;
            rig.route
                .path()
                .close(crate::transport::metrics::QuicCloseCode::Downgrade);
            rig.path.settle(Duration::from_millis(50)).await;

            let udp = rig.udp_data();
            assert_eq!(
                sorted(udp.shorts),
                sorted((0..total).map(message).collect::<Vec<_>>()),
                "each message once"
            );
            assert!(rig.delivered_shorts().is_empty());
            assert_eq!(
                rig.stats().send_count(SendKind::Short, R::NotOpenedUdp),
                u64::from(MAX_UNI_STREAMS),
                "the worker held this many"
            );
            assert!(
                rig.worker.is_finished(),
                "the worker ends once the path has and its queue is empty"
            );

            // A message sent after the end finds the queue closed.
            rig.e.conn.send_serialized(message(total)).await.unwrap();
            assert_eq!(rig.udp_data().shorts, vec![message(total)]);
            assert_eq!(
                rig.stats().send_event_count(E::ShortOverflowUdp),
                0,
                "a closed queue is not an overflow"
            );
        },
    );
}

/// The peer gets a message and every acknowledgement is lost. The sender cannot
/// know, reports `MaybeDelivered`, and sends nothing on UDP: one delivery.
#[test]
fn a_message_whose_ack_is_lost_is_delivered_once() {
    run(
        "quic-send-lost-ack",
        54,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let silenced = DropFrom::new("quic-send-lost-ack", &[rig.path.peer_addr()]);
            rig.e.conn.send_serialized(message(1)).await.unwrap();
            rig.path.settle(Duration::from_secs(7)).await;
            drop(silenced);

            assert_eq!(
                rig.stats().send_count(SendKind::Short, R::MaybeDelivered),
                1
            );
            assert!(rig.udp_data().ids.is_empty());
            assert_eq!(rig.delivered_shorts(), vec![message(1)]);
        },
    );
}

/// Each way onto UDP from the path leaves flight size where the UDP code expects
/// it: charged for what the socket took, and back to 0 once it is acknowledged.
#[test]
fn flight_size_returns_to_zero_after_each_way_to_udp() {
    run(
        "quic-send-flight",
        55,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            // The queue overflows.
            for i in 0..=SHORT_QUEUE_CAPACITY as u32 {
                rig.e.conn.send_serialized(message(i)).await.unwrap();
            }
            let overflow = rig.udp_data();
            assert_eq!(overflow.ids.len(), 1);
            assert!(rig.flightsize() > 0);
            rig.ack(&overflow.ids).await;
            assert_eq!(rig.flightsize(), 0, "after an overflow");
            rig.path.settle(Duration::from_secs(1)).await;

            // The worker's own fallback: a short that could not open a stream.
            let held = rig.hold_stream_credit().await;
            rig.e.conn.send_serialized(message(1000)).await.unwrap();
            rig.path.settle(Duration::from_secs(2)).await;
            let timed_out = rig.udp_data();
            assert_eq!(timed_out.shorts, vec![message(1000)]);
            assert!(rig.flightsize() > 0);
            rig.ack(&timed_out.ids).await;
            assert_eq!(rig.flightsize(), 0, "after a send that timed out");

            // A stream on UDP under its id.
            let id = StreamId::next();
            rig.e
                .conn
                .outbound_stream_with_id(id, bytes_of(3000), None, None, None)
                .await;
            rig.path.settle(Duration::from_secs(3)).await;
            tokio::task::yield_now().await;
            let stream = rig.udp_data();
            assert!(stream.ids.len() >= 2, "{:?}", stream.ids);
            assert!(rig.flightsize() > 0);
            rig.ack(&stream.ids).await;
            assert_eq!(rig.flightsize(), 0, "after a stream that did not open");
            drop(held);

            // The path's end drains the queue.
            rig.route
                .path()
                .close(crate::transport::metrics::QuicCloseCode::Downgrade);
            rig.path.settle(Duration::from_millis(50)).await;
            rig.e.conn.send_serialized(message(2000)).await.unwrap();
            let after_end = rig.udp_data();
            rig.ack(&after_end.ids).await;
            assert_eq!(rig.flightsize(), 0, "after the path ended");
        },
    );
}

/// A failed send on UDP charges nothing: the socket never took the packet.
#[test]
fn a_udp_send_that_fails_charges_no_flight_and_is_counted() {
    run(
        "quic-send-udp-fails",
        56,
        QuicPathLimits::DEFAULT,
        |rig| async move {
            rig.e
                .fail_sends
                .store(true, std::sync::atomic::Ordering::Relaxed);
            rig.route
                .path()
                .close(crate::transport::metrics::QuicCloseCode::Downgrade);
            rig.route.queue_short(message(1)).ok();
            rig.path.settle(Duration::from_millis(50)).await;
            assert_eq!(rig.flightsize(), 0);
            assert_eq!(rig.stats().send_event_count(E::UdpSendFailed), 1);
        },
    );
}

/// The largest message a queue takes still fits a packet on the way to UDP, with
/// no receipts beside it.
#[test]
fn the_largest_short_goes_on_udp_whole() {
    run(
        "quic-send-largest",
        57,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let largest = (0u64..)
                .take_while(|len| super::super::fits_one_packet(*len))
                .last()
                .unwrap() as usize;
            let bytes = vec![9u8; largest];
            rig.route
                .path()
                .close(crate::transport::metrics::QuicCloseCode::Downgrade);
            rig.path.settle(Duration::from_millis(50)).await;
            rig.route.queue_short(bytes.clone()).ok();
            rig.e.conn.send_serialized(bytes.clone()).await.unwrap();
            assert_eq!(rig.udp_data().shorts, vec![bytes]);
            assert_eq!(rig.stats().send_event_count(E::UdpSendFailed), 0);
        },
    );
}

/// A stream that cannot open goes on UDP under its own id with the whole payload,
/// and its completion goes with it: UDP's outcome is the one heard.
#[test]
fn a_stream_that_never_opened_goes_on_udp_under_its_id() {
    run(
        "quic-send-stream-udp",
        58,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let _held = rig.hold_stream_credit().await;
            let id = StreamId::next();
            let stream = bytes_of(3000);
            let (done, heard) = oneshot::channel();
            rig.e
                .conn
                .outbound_stream_with_id(id, stream.clone(), None, Some(done), None)
                .await;
            rig.path.settle(Duration::from_secs(3)).await;
            tokio::task::yield_now().await;

            let udp = rig.udp_data();
            assert!(udp.stream_ids.iter().all(|sent| *sent == id));
            assert_eq!(udp.stream_bytes, stream.to_vec());
            assert_eq!(heard.await.unwrap(), StreamSendOutcome::Delivered);
            assert_eq!(rig.stats().send_count(SendKind::Stream, R::NotOpenedUdp), 1);
        },
    );
}

/// A message past one packet that cannot open goes on UDP as a stream of its own.
#[test]
fn a_large_short_that_never_opened_goes_on_udp_as_a_stream() {
    run(
        "quic-send-large-udp",
        59,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let _held = rig.hold_stream_credit().await;
            let bytes = bytes_of(4000);
            rig.e.conn.send_serialized(bytes.to_vec()).await.unwrap();
            rig.path.settle(Duration::from_secs(3)).await;
            tokio::task::yield_now().await;
            let udp = rig.udp_data();
            assert_eq!(udp.stream_bytes, bytes.to_vec());
            assert!(udp.stream_ids.iter().all(|id| !id.is_caller_named()));
            assert_eq!(
                rig.stats()
                    .send_count(SendKind::LargeShort, R::NotOpenedUdp),
                1
            );
        },
    );
}

/// The peer refuses data on a path this node set up for it: the message goes on
/// UDP under its id, it is counted, and the peer closes the path, so new sends go
/// on UDP.
#[test]
fn data_the_peer_refuses_goes_on_udp_and_the_peer_closes_the_path() {
    run(
        "quic-send-refused",
        60,
        QuicPathLimits::SHADOW,
        |mut rig| async move {
            let id = StreamId::next();
            let stream = bytes_of(100_000);
            let (done, heard) = oneshot::channel();
            rig.e
                .conn
                .outbound_stream_with_id(id, stream.clone(), None, Some(done), None)
                .await;
            rig.path.settle(Duration::from_secs(2)).await;
            tokio::task::yield_now().await;

            let udp = rig.udp_data();
            assert_eq!(udp.stream_bytes, stream.to_vec());
            assert!(udp.stream_ids.iter().all(|sent| *sent == id));
            assert_eq!(heard.await.unwrap(), StreamSendOutcome::Delivered);
            assert_eq!(rig.stats().send_count(SendKind::Stream, R::FailedUdp), 1);
            assert_eq!(rig.stats().send_event_count(E::DataRefused), 1);
            assert_eq!(rig.stats().stop_count(stop_code::NOT_ACCEPTING_DATA), 1);
            assert!(!rig.route.path().sends_data());
            // The receiver closed the path on its refusal, so the queue goes to
            // UDP too, and no send reaches the path again.
            assert!(
                rig.route.path().quinn_connection().close_reason().is_some(),
                "the receiver closed the path"
            );
            assert_eq!(rig.path.peer().stats().count(Q::ClosedRefusingData), 1);
            rig.path.settle(Duration::from_millis(50)).await;
            rig.e.conn.send_serialized(message(1)).await.unwrap();
            assert_eq!(rig.udp_data().shorts, vec![message(1)]);
            assert_eq!(rig.path.peer().stats().count(Q::RefusedNotAcceptingData), 1);
        },
    );
}

/// A peer short of room refuses a stream: `Dropped`, nothing on UDP, and the
/// connection goes on. A large short refused the same way goes on UDP, because UDP
/// does not charge a short and a message refused before anything was created
/// cannot arrive twice.
#[test]
fn a_stream_refused_for_room_is_dropped_and_a_short_is_not() {
    let limits = QuicPathLimits {
        max_bytes: 1024,
        ..QuicPathLimits::DEFAULT
    };
    run("quic-send-budget", 61, limits, |mut rig| async move {
        let (done, heard) = oneshot::channel();
        rig.e
            .conn
            .outbound_stream_with_id(StreamId::next(), bytes_of(100_000), None, Some(done), None)
            .await;
        rig.path.settle(Duration::from_secs(1)).await;
        rig.collect_tasks().await;
        assert_eq!(heard.await.unwrap(), StreamSendOutcome::Dropped);
        assert!(
            rig.udp_data().ids.is_empty(),
            "a stream that opened stays dropped"
        );
        assert_eq!(rig.stats().send_count(SendKind::Stream, R::Dropped), 1);
        assert_eq!(rig.stats().stop_count(stop_code::BUDGET_REFUSED), 1);
        assert_eq!(rig.e.conn.telemetry_test_counts.outbound_streams_failed, 1);

        let bytes = bytes_of(100_000);
        rig.e.conn.send_serialized(bytes.to_vec()).await.unwrap();
        rig.path.settle(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(rig.udp_data().stream_bytes, bytes.to_vec());
        assert_eq!(
            rig.stats().send_count(SendKind::LargeShort, R::FailedUdp),
            1
        );
    });
}

/// A stream that opened and stopped moving is reset and is `Dropped`, never resent
/// under its id: the receiver may remember the id.
#[test]
fn a_stream_that_stalls_after_opening_is_dropped() {
    run(
        "quic-send-stalled",
        62,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let (done, heard) = oneshot::channel();
            let silenced = DropFrom::new("quic-send-stalled", &[rig.path.peer_addr()]);
            rig.e
                .conn
                .outbound_stream_with_id(
                    StreamId::next(),
                    bytes_of(8 * 1024 * 1024),
                    None,
                    Some(done),
                    None,
                )
                .await;
            rig.path.settle(Duration::from_secs(10)).await;
            drop(silenced);
            rig.collect_tasks().await;
            assert_eq!(heard.await.unwrap(), StreamSendOutcome::Dropped);
            assert!(rig.udp_data().ids.is_empty());
            assert_eq!(rig.flightsize(), 0);
            assert_eq!(rig.stats().send_count(SendKind::Stream, R::Dropped), 1);
        },
    );
}

/// A pipe that cannot open reads its stream again from the start for UDP, though
/// the path already took the first chunk from its source.
#[test]
fn a_pipe_that_never_opened_goes_on_udp_from_its_first_byte() {
    run(
        "quic-send-pipe-udp",
        63,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let _held = rig.hold_stream_credit().await;
            let id = StreamId::next();
            let payload = bytes_of(2000);
            let source = StreamHandle::new(StreamId::next(), 2000);
            source
                .push_fragment(1, payload.slice(..FRAGMENT_PAYLOAD_SIZE))
                .unwrap();
            source
                .push_fragment(2, payload.slice(FRAGMENT_PAYLOAD_SIZE..))
                .unwrap();
            rig.e
                .conn
                .pipe_stream_to_remote(
                    id,
                    crate::transport::IncomingStream::from(source),
                    None,
                    None,
                )
                .await;
            rig.path.settle(Duration::from_secs(3)).await;
            tokio::task::yield_now().await;
            let udp = rig.udp_data();
            assert_eq!(udp.stream_bytes, payload.to_vec(), "nothing was skipped");
            assert!(udp.stream_ids.iter().all(|sent| *sent == id));
            assert_eq!(rig.stats().send_count(SendKind::Pipe, R::NotOpenedUdp), 1);
        },
    );
}

/// A source that fails before its first chunk has nothing to send on any route, so
/// the pipe is `Dropped`, with no UDP stream under the id.
#[test]
fn a_pipe_with_no_first_chunk_is_dropped() {
    run(
        "quic-send-pipe-empty",
        64,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let source = StreamHandle::new(StreamId::next(), 2000);
            source.cancel();
            rig.e
                .conn
                .pipe_stream_to_remote(
                    StreamId::next(),
                    crate::transport::IncomingStream::from(source),
                    None,
                    None,
                )
                .await;
            rig.path.settle(Duration::from_secs(1)).await;
            rig.collect_tasks().await;
            assert!(rig.udp_data().ids.is_empty());
            assert_eq!(rig.stats().send_count(SendKind::Pipe, R::Dropped), 1);
            assert_eq!(rig.e.conn.telemetry_test_counts.outbound_streams_failed, 1);
        },
    );
}

/// A receiver whose consumer is slow holds its stream credit by design, so most of
/// a sender's shorts cannot open. They go on UDP. The consumer takes a few at a
/// time, which frees credit, and the shorts that then open are acknowledged, so
/// the path stays up for as long as that goes on, past the window. Closing it would
/// drop what the receiver's waiting readers hold. Every message arrives once, by
/// one route or the other, once the consumer catches up.
#[test]
fn shorts_held_back_by_a_slow_consumer_do_not_close_the_path() {
    let limits = QuicPathLimits {
        short_capacity: 1,
        ..QuicPathLimits::DEFAULT
    };
    run(
        "quic-send-slow-consumer",
        65,
        limits,
        |mut rig| async move {
            let mut sent = 0u32;
            let mut send = async |rig: &mut Rig, count: u32| {
                for _ in 0..count {
                    if sent % 32 == 0 {
                        tokio::task::yield_now().await;
                    }
                    rig.e.conn.send_serialized(message(sent)).await.unwrap();
                    sent += 1;
                }
            };
            // Enough to use up the peer's credit, which its readers then hold.
            send(&mut rig, 120).await;
            rig.path.settle(Duration::from_secs(2)).await;
            let mut seen = Vec::new();
            // Every 3 s for 33 s the consumer takes ten, which frees credit
            // (quinn announces it in steps of eight), and a burst follows. A few of
            // the burst open and are acknowledged; at least eight time out at once,
            // so only the acknowledgements keep the path from counting them.
            for _ in 0..11 {
                for _ in 0..10 {
                    seen.extend(rig.delivered_shorts());
                    rig.path.settle(Duration::from_millis(5)).await;
                }
                rig.path.settle(Duration::from_millis(50)).await;
                send(&mut rig, 2 * STALLED_SENDS_LIMIT + 2).await;
                rig.path.settle(Duration::from_secs(3)).await;
            }
            assert!(
                rig.stats().send_count(SendKind::Short, R::NotOpenedUdp)
                    >= u64::from(10 * STALLED_SENDS_LIMIT),
                "many sends could not open"
            );
            assert_eq!(rig.stats().send_event_count(E::StalledClosed), 0);
            assert_eq!(
                rig.stats().send_event_count(E::SuspectEntered),
                0,
                "an acknowledged path is never suspect"
            );
            assert!(rig.route.path().quinn_connection().close_reason().is_none());

            // The consumer catches up. The channel holds one, so each turn lets
            // one waiting reader in.
            for _ in 0..1000 {
                seen.extend(rig.delivered_shorts());
                rig.path.settle(Duration::from_millis(5)).await;
            }
            seen.extend(rig.udp_data().shorts);
            assert_eq!(
                sorted(seen),
                sorted((0..sent).map(message).collect::<Vec<_>>()),
                "each message once"
            );
        },
    );
}

/// A busy relay holds all 48 of its streamed-send permits for long transfers that
/// are being acknowledged. Its other large messages cannot get a permit and go on
/// UDP, and the path stays up, however long that goes on: the path is moving.
#[test]
fn a_busy_relay_whose_transfers_are_acknowledged_keeps_its_path() {
    run(
        "quic-send-busy-relay",
        67,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            const PIECES: u32 = 12;
            let total = u64::from(PIECES) * FRAGMENT_PAYLOAD_SIZE as u64;
            let mut sources = Vec::new();
            for _ in 0..STREAMED_SEND_LIMIT {
                let source = StreamHandle::new(StreamId::next(), total);
                source
                    .push_fragment(1, bytes_of(FRAGMENT_PAYLOAD_SIZE))
                    .unwrap();
                rig.e
                    .conn
                    .pipe_stream_to_remote(
                        StreamId::next(),
                        crate::transport::IncomingStream::from(source.clone()),
                        None,
                        None,
                    )
                    .await;
                sources.push(source);
            }
            rig.path.settle(Duration::from_millis(200)).await;
            assert_eq!(rig.route.path().free_stream_slots(), 0, "every permit held");
            // Every 2 s each transfer moves on by a piece, inside its 5 s gap, and is
            // acknowledged; large messages wait for a permit and give up.
            for piece in 2..=PIECES {
                // Eight at once, so they time out together: only the window keeps
                // them from closing the path, since the reset on an acknowledgement
                // comes before the first of them.
                for _ in 0..STALLED_SENDS_LIMIT {
                    rig.e
                        .conn
                        .send_serialized(bytes_of(4000).to_vec())
                        .await
                        .unwrap();
                }
                rig.path.settle(Duration::from_secs(2)).await;
                for source in &sources {
                    source
                        .push_fragment(piece, bytes_of(FRAGMENT_PAYLOAD_SIZE))
                        .unwrap();
                }
            }
            assert!(
                rig.stats()
                    .send_count(SendKind::LargeShort, R::NotOpenedUdp)
                    >= u64::from(STALLED_SENDS_LIMIT),
                "enough timed out to close it if they counted"
            );
            assert_eq!(rig.stats().send_event_count(E::StalledClosed), 0);
            assert_eq!(
                rig.stats().send_event_count(E::SuspectEntered),
                0,
                "an acknowledged path is never suspect"
            );
            assert!(rig.route.path().quinn_connection().close_reason().is_none());
        },
    );
}

/// Permits held by sends waiting for acknowledgements that never come: nothing on
/// the path is acknowledged, every large message times out waiting for a permit,
/// and eight of those past the window close the path. That close is the only thing
/// that recycles such a path.
#[test]
fn permits_held_with_nothing_acknowledged_close_the_path() {
    run(
        "quic-send-held-permits",
        73,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let silenced = DropFrom::new("quic-send-held-permits", &[rig.path.peer_addr()]);
            for _ in 0..STREAMED_SEND_LIMIT {
                rig.e
                    .conn
                    .outbound_stream_with_id(StreamId::next(), bytes_of(1000), None, None, None)
                    .await;
            }
            rig.path.settle(Duration::from_secs(9)).await;
            assert_eq!(rig.route.path().free_stream_slots(), 0, "every permit held");
            for _ in 0..STALLED_SENDS_LIMIT {
                rig.e
                    .conn
                    .send_serialized(bytes_of(4000).to_vec())
                    .await
                    .unwrap();
            }
            rig.path.settle(Duration::from_secs(3)).await;
            drop(silenced);
            assert_eq!(
                rig.stats()
                    .send_count(SendKind::LargeShort, R::NotOpenedUdp),
                u64::from(STALLED_SENDS_LIMIT)
            );
            assert_eq!(rig.stats().send_event_count(E::StalledClosed), 1);
            assert!(rig.route.path().quinn_connection().close_reason().is_some());
        },
    );
}

/// Eight streams that open and then stop moving, with nothing acknowledged past the
/// window, close the path.
#[test]
fn eight_streams_that_stall_after_opening_close_the_path() {
    run(
        "quic-send-stall-close",
        68,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let silenced = DropFrom::new("quic-send-stall-close", &[rig.path.peer_addr()]);
            rig.path.settle(Duration::from_secs(6)).await;
            for _ in 0..STALLED_SENDS_LIMIT {
                rig.e
                    .conn
                    .outbound_stream_with_id(
                        StreamId::next(),
                        bytes_of(4 * 1024 * 1024),
                        None,
                        None,
                        None,
                    )
                    .await;
            }
            rig.path.settle(Duration::from_secs(10)).await;
            drop(silenced);
            assert_eq!(
                rig.stats().send_count(SendKind::Stream, R::Dropped),
                u64::from(STALLED_SENDS_LIMIT)
            );
            assert_eq!(rig.stats().send_event_count(E::StalledClosed), 1);
            assert!(rig.route.path().quinn_connection().close_reason().is_some());
        },
    );
}

/// Every kind of message arrives once when its acknowledgement is lost and the
/// path then closes: none goes again on UDP, and the stream's sender hears
/// `Dropped`, since quinn may not have sent all of it.
#[test]
fn messages_whose_ack_is_lost_arrive_once_and_a_stream_reports_dropped() {
    run(
        "quic-send-lost-acks",
        69,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let silenced = DropFrom::new("quic-send-lost-acks", &[rig.path.peer_addr()]);
            let large = bytes_of(4000).to_vec();
            rig.e.conn.send_serialized(large.clone()).await.unwrap();
            let id = StreamId::next();
            let (done, heard) = oneshot::channel();
            rig.e
                .conn
                .outbound_stream_with_id(id, bytes_of(30_000), None, Some(done), None)
                .await;
            let piped_id = StreamId::next();
            let piped = StreamHandle::new(StreamId::next(), 2000);
            piped
                .push_fragment(1, bytes_of(2000).slice(..FRAGMENT_PAYLOAD_SIZE))
                .unwrap();
            piped
                .push_fragment(2, bytes_of(2000).slice(FRAGMENT_PAYLOAD_SIZE..))
                .unwrap();
            rig.e
                .conn
                .pipe_stream_to_remote(
                    piped_id,
                    crate::transport::IncomingStream::from(piped),
                    None,
                    None,
                )
                .await;
            rig.path.settle(Duration::from_secs(2)).await;
            rig.route
                .path()
                .close(crate::transport::metrics::QuicCloseCode::Downgrade);
            rig.path.settle(Duration::from_millis(100)).await;
            drop(silenced);
            rig.collect_tasks().await;

            assert_eq!(heard.await.unwrap(), StreamSendOutcome::Dropped);
            assert!(rig.udp_data().ids.is_empty(), "nothing went again on UDP");
            for kind in [SendKind::LargeShort, SendKind::Stream, SendKind::Pipe] {
                assert_eq!(
                    rig.stats().send_count(kind, R::MaybeDelivered),
                    1,
                    "{kind:?}"
                );
            }
            assert_eq!(rig.delivered_shorts(), vec![large]);
            let inbound = rig.path.peer_inbound();
            let mut ids = Vec::new();
            while let Ok(arrival) = inbound.streams.try_recv() {
                ids.push(arrival.stream_id);
            }
            ids.sort_by_key(|id| id.0);
            let mut want = vec![id, piped_id];
            want.sort_by_key(|id| id.0);
            assert_eq!(ids, want, "each stream arrived once");
        },
    );
}

/// A pipe whose source fails after part of it was sent gives up: the receiver may
/// hold the id, so it is not read again for UDP.
#[test]
fn a_pipe_that_fails_after_sending_part_gives_up() {
    run(
        "quic-send-pipe-partial",
        70,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let source = StreamHandle::new(StreamId::next(), 4000);
            source
                .push_fragment(1, bytes_of(FRAGMENT_PAYLOAD_SIZE))
                .unwrap();
            rig.e
                .conn
                .pipe_stream_to_remote(
                    StreamId::next(),
                    crate::transport::IncomingStream::from(source.clone()),
                    None,
                    None,
                )
                .await;
            rig.path.settle(Duration::from_millis(200)).await;
            // The pipe opened on QUIC with its first chunk: the peer announced it,
            // and nothing has settled yet.
            assert!(
                rig.path.peer_inbound().streams.try_recv().is_ok(),
                "the stream reached the peer before its source failed"
            );
            assert_eq!(rig.stats().send_total(R::Dropped), 0);
            source.cancel();
            rig.path.settle(Duration::from_millis(200)).await;
            rig.collect_tasks().await;
            assert!(rig.udp_data().ids.is_empty());
            assert_eq!(rig.stats().send_count(SendKind::Pipe, R::Dropped), 1);
            assert_eq!(rig.e.conn.telemetry_test_counts.outbound_streams_failed, 1);
        },
    );
}

/// A pipe that opened, sent its first chunk and then stopped getting more from
/// its source past the 5 s gap gives up. Its source is alive, so reading it again
/// for UDP would put the first chunk on UDP under an id the receiver may hold.
#[test]
fn a_pipe_that_stalls_after_opening_gives_up() {
    run(
        "quic-send-pipe-stall-open",
        74,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let source = StreamHandle::new(StreamId::next(), 4000);
            source
                .push_fragment(1, bytes_of(FRAGMENT_PAYLOAD_SIZE))
                .unwrap();
            rig.e
                .conn
                .pipe_stream_to_remote(
                    StreamId::next(),
                    crate::transport::IncomingStream::from(source.clone()),
                    None,
                    None,
                )
                .await;
            rig.path.settle(Duration::from_millis(200)).await;
            assert!(
                rig.path.peer_inbound().streams.try_recv().is_ok(),
                "the stream reached the peer with its first chunk"
            );
            rig.path.settle(Duration::from_secs(7)).await;
            rig.collect_tasks().await;
            assert_eq!(rig.stats().send_count(SendKind::Pipe, R::Dropped), 1);
            assert!(rig.udp_data().ids.is_empty(), "not read again for UDP");
            drop(source);
        },
    );
}

/// A path whose sends time out with nothing acknowledged takes no new data: a
/// short queued to it then goes on UDP, though the path is up. Once the peer
/// acknowledges again, data goes on the path again. Across
/// the switch no message is lost or sent twice: the short already on the path
/// stays there (delivered or counted `MaybeDelivered`, never resent on UDP), and
/// each later one goes exactly once.
#[test]
fn a_suspect_path_sends_new_shorts_on_udp_until_it_is_acknowledged() {
    run(
        "quic-send-suspect",
        75,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let silenced = DropFrom::new("quic-send-suspect", &[rig.path.peer_addr()]);
            let mut on_udp: Vec<Vec<u8>> = Vec::new();
            let mut on_path: Vec<Vec<u8>> = Vec::new();
            rig.path.settle(Duration::from_secs(9)).await;
            rig.e.conn.send_serialized(message(1)).await.unwrap();
            // Its 1 s open passes with nothing acknowledged since the path was
            // built 10 s before: a short that opens is held for its 5 s total.
            rig.path.settle(Duration::from_secs(6)).await;
            assert_eq!(rig.stats().send_event_count(E::SuspectEntered), 1);
            assert!(rig.route.path().is_suspect());
            on_udp.extend(rig.udp_data().shorts);
            rig.e.conn.send_serialized(message(2)).await.unwrap();
            rig.path.settle(Duration::from_millis(50)).await;
            let udp = rig.udp_data().shorts;
            assert_eq!(udp, vec![message(2)], "on UDP at once");
            on_udp.extend(udp);
            // A short the worker takes while the path is suspect goes on UDP too.
            rig.route
                .queue_short(message(4))
                .expect("the queue is open");
            rig.path.settle(Duration::from_millis(50)).await;
            let udp = rig.udp_data().shorts;
            assert_eq!(udp, vec![message(4)], "the worker sends it on UDP");
            on_udp.extend(udp);
            assert!(rig.route.path().quinn_connection().close_reason().is_none());

            // The peer acknowledges again (quinn's keepalive, at the latest).
            drop(silenced);
            rig.path.settle(Duration::from_secs(16)).await;
            assert!(!rig.route.path().is_suspect());
            assert_eq!(rig.stats().send_event_count(E::SuspectLeft), 1);
            on_udp.extend(rig.udp_data().shorts);
            on_path.extend(rig.delivered_shorts());
            rig.e.conn.send_serialized(message(3)).await.unwrap();
            rig.path.settle(Duration::from_secs(1)).await;
            let udp = rig.udp_data().shorts;
            assert!(udp.is_empty(), "back on the path");
            on_udp.extend(udp);
            on_path.extend(rig.delivered_shorts());
            assert!(on_path.contains(&message(3)));

            // Nothing lost or sent twice across the switch.
            assert!(
                !on_udp.contains(&message(1)),
                "the short on the path is not resent"
            );
            assert!(
                on_path.contains(&message(1)) || rig.stats().send_total(R::MaybeDelivered) >= 1,
                "the short on the path was delivered or counted as maybe"
            );
            for i in [2, 3, 4] {
                let times = on_udp
                    .iter()
                    .chain(&on_path)
                    .filter(|m| **m == message(i))
                    .count();
                assert_eq!(times, 1, "message {i}: {on_udp:?} {on_path:?}");
            }
        },
    );
}

/// The connection's own route, with no test override: a session whose path is up in
/// shadow takes none of the connection's data. A short, a large message, a stream
/// and a pipe all go on UDP, and the path counts no data send.
#[test]
fn a_session_in_shadow_sends_everything_on_udp() {
    use crate::transport::quic::upgrade::driver::tests::shadow_path;
    crate::transport::quic::upgrade::driver::tests::run_bounded(|| async move {
        let shadow = shadow_path("quic-send-inert", 93).await;
        let mut e = single(CAPABLE);
        let path = Arc::clone(&shadow.path);
        let time = shadow.time.clone();
        let test = async move {
            let shadow = shadow;
            e.conn.quic_path = Some(shadow.state);
            assert!(e.conn.quic_route_for_test.is_none());
            assert!(e.conn.quic_data_route().is_none());
            e.conn.send_serialized(message(1)).await.unwrap();
            e.conn.send_serialized(vec![5u8; 4000]).await.unwrap();
            let id = StreamId::next();
            e.conn
                .outbound_stream_with_id(id, bytes_of(3000), None, None, None)
                .await;
            let piped_id = StreamId::next();
            let piped = StreamHandle::new(StreamId::next(), 1000);
            piped.push_fragment(1, bytes_of(1000)).unwrap();
            e.conn
                .pipe_stream_to_remote(
                    piped_id,
                    crate::transport::IncomingStream::from(piped),
                    None,
                    None,
                )
                .await;
            // The UDP stream tasks run on the connection's clock, which is real.
            for _ in 0..200 {
                tokio::task::yield_now().await;
            }
            let mut shorts = Vec::new();
            let mut stream_ids = Vec::new();
            for message in e.drain_sent() {
                if let SymmetricMessagePayload::ShortMessage { payload } = &message.payload {
                    shorts.push(payload.to_vec());
                } else if let SymmetricMessagePayload::StreamFragment { stream_id, .. } =
                    &message.payload
                {
                    stream_ids.push(*stream_id);
                }
            }
            assert_eq!(shorts, vec![message(1)]);
            assert!(stream_ids.contains(&id), "{stream_ids:?}");
            assert!(stream_ids.contains(&piped_id), "{stream_ids:?}");
            assert!(
                stream_ids.iter().any(|id| !id.is_caller_named()),
                "the large message as a stream of its own"
            );
            for result in [
                R::Sent,
                R::MaybeDelivered,
                R::NotOpenedUdp,
                R::FailedUdp,
                R::Dropped,
            ] {
                assert_eq!(path.stats().send_total(result), 0, "{result:?}");
            }
            drop(e);
        };
        (time, test)
    });
}

/// An upstream that stalls is not the path's doing: eight pipes that time out
/// waiting for a first chunk leave the path up.
#[test]
fn pipes_whose_source_stalls_do_not_close_the_path() {
    run(
        "quic-send-pipe-stall",
        66,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let mut sources = Vec::new();
            for _ in 0..8 {
                let source = StreamHandle::new(StreamId::next(), 2000);
                rig.e
                    .conn
                    .pipe_stream_to_remote(
                        StreamId::next(),
                        crate::transport::IncomingStream::from(source.clone()),
                        None,
                        None,
                    )
                    .await;
                sources.push(source);
            }
            rig.path.settle(Duration::from_secs(6)).await;
            assert_eq!(rig.stats().send_count(SendKind::Pipe, R::NotOpenedUdp), 8);
            assert_eq!(rig.stats().send_event_count(E::StalledClosed), 0);
            assert!(rig.route.path().quinn_connection().close_reason().is_none());
        },
    );
}

/// A UDP session that records what the worker hands it.
#[derive(Default)]
struct CollectUdp(std::sync::Mutex<Vec<Vec<u8>>>);

impl crate::transport::quic::upgrade::data_route::UdpShorts for CollectUdp {
    fn send_short(
        &self,
        payload: Vec<u8>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        self.0.lock().unwrap().push(payload);
        Box::pin(std::future::ready(Ok(())))
    }
}

/// On a multi-thread runtime: several tasks queue shorts while another closes the
/// path under them, and then timeouts settle from several threads at once. No
/// message arrives twice, and one is missing only where its send reported
/// `MaybeDelivered` (the end went out, and the close came first). The stall close
/// is counted once however the timeouts race.
#[test]
fn many_tasks_send_while_the_path_closes_and_nothing_goes_twice() {
    use crate::transport::quic::path::{BoundedSend, QuicSendError, SendFailure};
    use crate::transport::quic::upgrade::driver::tests::data_path_on_threads;
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("runtime");
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(async {
                let _seed = crate::config::GlobalRng::seed_guard(0x5940_E5B6);
                let budget =
                    crate::transport::peer_connection::inbound_budget::SharedInboundBudget::new(
                        1024,
                        1 << 30,
                    );
                let (data, _inbox) = data_channels();
                let mut path = data_path_on_threads("quic-send-threads", 72, budget, data).await;
                let ours = path.ours_handle();
                ours.enable_data();
                let time = path.time.clone();
                let udp = Arc::new(CollectUdp::default());
                let (route, worker) = DataRoute::new(
                    Arc::clone(&ours),
                    PathClock::Virtual(time.clone()),
                    udp.clone(),
                    RouteNote::default(),
                );
                tokio::spawn(worker);
                let overflow = Arc::new(std::sync::Mutex::new(Vec::new()));
                let (senders, overflowed, settler, clock) = (
                    route.clone(),
                    overflow.clone(),
                    Arc::clone(&ours),
                    time.clone(),
                );
                crate::transport::quic::tests::drive_on_threads(
                    &time,
                    Duration::from_secs(120),
                    async move {
                        let mut tasks = Vec::new();
                        for task in 0..4u32 {
                            let route = senders.clone();
                            let overflowed = overflowed.clone();
                            tasks.push(tokio::spawn(async move {
                                for i in 0..60u32 {
                                    if let Err(back) = route.queue_short(message(task * 1000 + i)) {
                                        // `send_serialized` sends it on UDP.
                                        overflowed.lock().unwrap().push(back);
                                    }
                                    tokio::task::yield_now().await;
                                }
                            }));
                        }
                        // The path closes while the senders are still queuing.
                        let closing = Arc::clone(&settler);
                        tasks.push(tokio::spawn(async move {
                            for _ in 0..20 {
                                tokio::task::yield_now().await;
                            }
                            closing.close(crate::transport::metrics::QuicCloseCode::Downgrade);
                        }));
                        for task in tasks {
                            task.await.expect("task");
                        }
                        clock.sleep(Duration::from_secs(3)).await;
                        // Then timeouts from several threads at once, on a path that
                        // has heard nothing for the window: the close is counted once.
                        let now = clock.now_nanos();
                        settler.settle(
                            SendKind::Stream,
                            &BoundedSend::Failed(SendFailure::Error(QuicSendError::Closed)),
                            false,
                            now,
                        );
                        let quiet = now + 11_000_000_000;
                        let mut settles = Vec::new();
                        for _ in 0..2 * STALLED_SENDS_LIMIT {
                            let path = Arc::clone(&settler);
                            settles.push(tokio::spawn(async move {
                                tokio::task::yield_now().await;
                                path.settle(
                                    SendKind::Stream,
                                    &BoundedSend::Failed(SendFailure::TimedOut),
                                    false,
                                    quiet,
                                );
                            }));
                        }
                        for settle in settles {
                            settle.await.expect("settle");
                        }
                    },
                )
                .await;

                let mut seen = udp.0.lock().unwrap().clone();
                seen.extend(overflow.lock().unwrap().iter().cloned());
                let inbound = path.peer_inbound();
                while let Ok(short) = inbound.shorts.try_recv() {
                    seen.push(short.payload().to_vec());
                }
                let mut counts = std::collections::BTreeMap::new();
                for message in &seen {
                    *counts.entry(message.clone()).or_insert(0u32) += 1;
                }
                assert!(counts.values().all(|n| *n == 1), "a message arrived twice");
                let maybe = ours.stats().send_count(SendKind::Short, R::MaybeDelivered);
                let missing = 240 - counts.len() as u64;
                assert!(
                    missing <= maybe,
                    "{missing} missing, {maybe} maybe delivered"
                );
                assert_eq!(ours.stats().send_event_count(E::StalledClosed), 1);
                assert!(ours.quinn_connection().close_reason().is_some());
            })
        }));
        done.send(outcome).ok();
    });
    match finished.recv_timeout(Duration::from_secs(120)) {
        Ok(Ok(())) => {}
        Ok(Err(panic)) => std::panic::resume_unwind(panic),
        Err(_) => panic!("the multi-thread test did not finish"),
    }
}

/// A pacer that grants its first `grant` reservations at once, refuses every
/// one after, and records what is charged to it.
#[derive(Default)]
struct RefusingPacer {
    grant: usize,
    calls: std::sync::atomic::AtomicUsize,
    granted: std::sync::atomic::AtomicUsize,
    charged: std::sync::atomic::AtomicUsize,
}

impl crate::transport::quic::path::pacer::SendPacer for RefusingPacer {
    fn reserve_within(&self, bytes: usize, limit: Duration) -> Result<Duration, Duration> {
        let call = self
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if call < self.grant {
            self.granted
                .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
            Ok(Duration::ZERO)
        } else {
            Err(limit)
        }
    }

    fn give_back(&self, _bytes: usize) {}

    fn charge(&self, bytes: usize) -> usize {
        self.charged
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        bytes
    }

    fn sleep(&self, wait: Duration) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(wait))
    }
}

/// A Stream and a Short past one packet that the pacer refuses go on UDP, and
/// their bytes are charged to the pacer as debt once UDP has sent them, so the
/// path's next sends wait for them.
#[test]
fn messages_the_pacer_refuses_go_on_udp_and_are_charged_to_it() {
    run(
        "quic-send-paced-udp",
        140,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let pacer = Arc::new(RefusingPacer::default());
            rig.route.path().pace_with(pacer.clone());
            let id = StreamId::next();
            let stream = bytes_of(3000);
            rig.e
                .conn
                .outbound_stream_with_id(id, stream.clone(), None, None, None)
                .await;
            let large = bytes_of(4000);
            rig.e.conn.send_serialized(large.to_vec()).await.unwrap();
            rig.path.settle(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
            let udp = rig.udp_data();
            assert_eq!(
                udp.stream_bytes.len(),
                stream.len() + large.len(),
                "both went on UDP"
            );
            assert_eq!(rig.stats().send_count(SendKind::Stream, R::NotOpenedUdp), 1);
            assert_eq!(
                rig.stats()
                    .send_count(SendKind::LargeShort, R::NotOpenedUdp),
                1
            );
            assert_eq!(
                pacer.charged.load(std::sync::atomic::Ordering::Relaxed),
                stream.len() + large.len(),
                "every refused byte is charged to the pacer"
            );
        },
    );
}

/// A Short past one packet that the pacer refuses after its stream opened is
/// reset and goes on UDP whole, and the whole message is charged to the
/// pacer: UDP put all of it on the wire, on top of what the QUIC writes before
/// the refusal already paid.
#[test]
fn a_large_short_the_pacer_refuses_after_opening_is_charged_in_full() {
    run(
        "quic-send-paced-open",
        141,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            // The header and the first write go; the next write is refused.
            let pacer = Arc::new(RefusingPacer {
                grant: 2,
                ..RefusingPacer::default()
            });
            rig.route.path().pace_with(pacer.clone());
            let large = bytes_of(4000);
            rig.e.conn.send_serialized(large.to_vec()).await.unwrap();
            rig.path.settle(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
            let udp = rig.udp_data();
            assert_eq!(udp.stream_bytes, large.to_vec(), "it went on UDP whole");
            assert_eq!(
                rig.stats().send_count(SendKind::LargeShort, R::FailedUdp),
                1
            );
            let granted = pacer.granted.load(std::sync::atomic::Ordering::Relaxed);
            assert!(
                granted > crate::transport::quic::framing::HEADER_LEN,
                "a payload write was made on QUIC"
            );
            assert_eq!(
                pacer.charged.load(std::sync::atomic::Ordering::Relaxed),
                large.len()
            );
        },
    );
}

/// A message the pacer refused whose UDP send then fails leaves no debt: the
/// pacer is charged only for what UDP sent.
#[test]
fn a_fallback_that_fails_on_udp_leaves_no_debt() {
    run(
        "quic-send-paced-udp-fails",
        142,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let pacer = Arc::new(RefusingPacer::default());
            rig.route.path().pace_with(pacer.clone());
            rig.e
                .fail_sends
                .store(true, std::sync::atomic::Ordering::Relaxed);
            rig.e
                .conn
                .outbound_stream_with_id(StreamId::next(), bytes_of(3000), None, None, None)
                .await;
            rig.path.settle(Duration::from_secs(1)).await;
            assert_eq!(rig.stats().send_count(SendKind::Stream, R::NotOpenedUdp), 1);
            assert_eq!(
                pacer.charged.load(std::sync::atomic::Ordering::Relaxed),
                0,
                "nothing went out, so nothing is owed"
            );
        },
    );
}
