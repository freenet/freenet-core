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
            match message.payload {
                SymmetricMessagePayload::ShortMessage { payload } => {
                    data.ids.push(message.packet_id);
                    data.shorts.push(payload.to_vec());
                }
                SymmetricMessagePayload::StreamFragment {
                    stream_id, payload, ..
                } => {
                    data.ids.push(message.packet_id);
                    data.stream_ids.push(stream_id);
                    data.stream_bytes.extend_from_slice(&payload);
                }
                _ => {}
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

use crate::transport::quic::config::MAX_UNI_STREAMS;
use crate::transport::quic::framing::FrameHeader;

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
        tokio::spawn(worker);
        (time, test(Rig { e, path, route }))
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
        |mut rig| async move {
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
                rig.stats().send_count(SendKind::LargeShort, R::NotOpenedUdp),
                1
            );
        },
    );
}

/// The peer refuses data on a path both sides set up for it: the message goes on
/// UDP under its id, it is counted, and new sends stay off a path that stays up.
#[test]
fn data_the_peer_refuses_goes_on_udp_and_the_path_stays() {
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
            assert!(
                rig.route.path().quinn_connection().close_reason().is_none(),
                "the path stays up"
            );

            // The queue goes straight to UDP, and no send reaches the path again.
            rig.route.queue_short(message(1)).ok();
            rig.path.settle(Duration::from_millis(50)).await;
            assert_eq!(rig.udp_data().shorts, vec![message(1)]);
            assert_eq!(
                rig.path
                    .peer()
                    .stats()
                    .count(Q::RefusedNotAcceptingData),
                1
            );
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
    run(
        "quic-send-budget",
        61,
        limits,
        |mut rig| async move {
            let (done, heard) = oneshot::channel();
            rig.e
                .conn
                .outbound_stream_with_id(StreamId::next(), bytes_of(100_000), None, Some(done), None)
                .await;
            rig.path.settle(Duration::from_secs(1)).await;
            rig.collect_tasks().await;
            assert_eq!(heard.await.unwrap(), StreamSendOutcome::Dropped);
            assert!(rig.udp_data().ids.is_empty(), "a stream that opened stays dropped");
            assert_eq!(rig.stats().send_count(SendKind::Stream, R::Dropped), 1);
            assert_eq!(rig.stats().stop_count(stop_code::BUDGET_REFUSED), 1);
            assert_eq!(rig.e.conn.telemetry_test_counts.outbound_streams_failed, 1);

            let bytes = bytes_of(100_000);
            rig.e.conn.send_serialized(bytes.to_vec()).await.unwrap();
            rig.path.settle(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
            assert_eq!(rig.udp_data().stream_bytes, bytes.to_vec());
            assert_eq!(rig.stats().send_count(SendKind::LargeShort, R::FailedUdp), 1);
        },
    );
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

/// Eight shorts in a row that cannot open a stream close the path: the stall the
/// reset count cannot see, since none of them opened anything.
#[test]
fn eight_sends_that_time_out_close_the_path() {
    run(
        "quic-send-stall-close",
        65,
        QuicPathLimits::DEFAULT,
        |mut rig| async move {
            let _held = rig.hold_stream_credit().await;
            for i in 0..u32::from(8u8) {
                rig.e.conn.send_serialized(message(i)).await.unwrap();
            }
            rig.path.settle(Duration::from_secs(2)).await;
            assert_eq!(rig.stats().send_event_count(E::StalledClosed), 1);
            assert!(rig.route.path().quinn_connection().close_reason().is_some());
            assert_eq!(rig.udp_data().shorts.len(), 8, "all of them went on UDP");
        },
    );
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
