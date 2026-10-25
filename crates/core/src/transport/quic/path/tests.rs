//! Tests for [`QuicPath`]: two endpoints on `SimulationSocket` and one
//! `VirtualTime` (the helpers of `quic::tests`), each with the other's
//! proven transport key, and a path on each side of the connection.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use super::super::framing::{HEADER_LEN, MAX_CONTROL_BYTES, MAX_METADATA_BYTES};
use super::super::tests::{MIB, Pair, SEED, bounded, drive, pair, payload};
use super::*;
use crate::config::GlobalRng;
use crate::simulation::{TimeSource, VirtualTime};
use crate::transport::IncomingStream;
use crate::transport::peer_connection::inbound_budget::ChargeOverhead;
use crate::transport::peer_connection::streaming::{
    MAX_STREAM_TOTAL_BYTES, StreamError, max_inbound_stream_age,
};
use crate::transport::peer_connection::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;
use crate::transport::peer_connection::streaming_handle_idle_timeout;

/// How long, in virtual time, a test's traffic may take.
const LIMIT: Duration = Duration::from_secs(600);

/// A connected pair with a path on each side: `a` is the client, `b` the
/// server; `b`'s path has `b_limits`.
struct Paths {
    pair: Pair,
    a: Arc<QuicPath>,
    b: Arc<QuicPath>,
    a_in: QuicInbound,
    b_in: QuicInbound,
}

impl Paths {
    fn time(&self) -> VirtualTime {
        self.pair.time.clone()
    }

    /// `a`'s raw connection, to write streams by hand.
    fn a_raw(&self) -> quinn::Connection {
        self.a.connection().inner().clone()
    }

    /// Run `fut` on virtual time.
    async fn drive<F>(&self, fut: F) -> F::Output
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        drive(&self.pair.time, LIMIT, fut).await
    }
}

/// Build the pair on `10.203.<subnet>.1` and `.2` and connect it.
async fn paths(network: &str, subnet: u8, b_limits: QuicPathLimits) -> Paths {
    let a_addr: SocketAddr = format!("10.203.{subnet}.1:5000").parse().unwrap();
    let b_addr: SocketAddr = format!("10.203.{subnet}.2:5000").parse().unwrap();
    let pair = pair(network, a_addr, b_addr, None).await;
    let permit = pair
        .b
        .allow_peer(pair.a_addr, pair.a_keys.public())
        .expect("allow-set has room");
    let (a_end, b_end) = (pair.a.clone(), pair.b.clone());
    let b_key = pair.b_keys.public().clone();
    let (client, server) = drive(&pair.time, LIMIT, async move {
        let (client, server) = tokio::join!(a_end.connect(b_addr, &b_key), async {
            b_end
                .accept()
                .await
                .expect("an incoming attempt")
                .establish()
                .await
        });
        (
            client.expect("client handshake"),
            server.expect("server accepted"),
        )
    })
    .await;
    drop(permit);
    let (a, a_in) = QuicPath::new(client, pair.time.clone());
    let (b, b_in) = QuicPath::with_limits(server, pair.time.clone(), b_limits);
    Paths {
        pair,
        a: Arc::new(a),
        b: Arc::new(b),
        a_in,
        b_in,
    }
}

/// Write `bytes` on a new uni stream of `conn` and finish it; the code the
/// peer stopped it with, or `None` if it acknowledged everything.
async fn raw_stream(conn: quinn::Connection, bytes: Vec<u8>) -> Option<u64> {
    let mut send = conn.open_uni().await.expect("open");
    if let Err(error) = send.write_all(&bytes).await {
        return match error {
            quinn::WriteError::Stopped(code) => Some(code.into_inner()),
            error @ (quinn::WriteError::ConnectionLost(_)
            | quinn::WriteError::ClosedStream
            | quinn::WriteError::ZeroRttRejected) => panic!("write failed: {error}"),
        };
    }
    send.finish().expect("finish");
    send.stopped()
        .await
        .expect("stopped")
        .map(|code| code.into_inner())
}

/// Write `bytes` on a new uni stream of `conn` WITHOUT finishing it, and
/// return the code the peer stops it with. A finished stream that fits in
/// the flow-control window can be acknowledged in full before the receiver
/// reads its header, and then nothing tells the sender it was stopped.
async fn raw_unfinished(conn: quinn::Connection, bytes: Vec<u8>) -> Option<u64> {
    let mut send = conn.open_uni().await.expect("open");
    if let Err(error) = send.write_all(&bytes).await {
        return match error {
            quinn::WriteError::Stopped(code) => Some(code.into_inner()),
            error @ (quinn::WriteError::ConnectionLost(_)
            | quinn::WriteError::ClosedStream
            | quinn::WriteError::ZeroRttRejected) => panic!("write failed: {error}"),
        };
    }
    send.stopped()
        .await
        .expect("stopped")
        .map(|code| code.into_inner())
}

/// Longer than the flow-control window, so a sender cannot finish it before
/// the receiver decides; a refusal then always reaches the sender.
fn over_window() -> Bytes {
    Bytes::from(payload(
        2 * super::super::config::MAX_WINDOW_BYTES as usize + 1,
    ))
}

/// A raw header.
fn header(version: u8, kind: u8, stream_id: u32, total_len: u64, metadata_len: u32) -> Vec<u8> {
    let mut out = vec![version, kind];
    out.extend_from_slice(&stream_id.to_be_bytes());
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&metadata_len.to_be_bytes());
    assert_eq!(out.len(), HEADER_LEN);
    out
}

fn stopped(code: u32) -> QuicSendError {
    QuicSendError::Stopped(u64::from(code))
}

fn assert_stopped(result: Result<(), QuicSendError>, code: u32) {
    match result {
        Err(QuicSendError::Stopped(got)) => assert_eq!(got, u64::from(code)),
        other => panic!("expected {:?}, got {other:?}", stopped(code)),
    }
}

/// Short messages of one byte and of 200 KiB (which the UDP transport would
/// send as a transport stream) arrive intact and in full, and the same
/// message sent twice arrives twice: dedup is not this layer's (e5 does it
/// in `PeerConnection`, where both transports meet). Short messages are
/// charged to the budget until their payload is taken.
#[test]
fn short_messages_round_trip_and_are_not_deduplicated() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-short", 1, QuicPathLimits::DEFAULT).await;
        let big = payload(200 * 1024);
        let a = p.a.clone();
        let sent = big.clone();
        p.drive(async move {
            a.send_short(b"x").await.expect("one byte");
            a.send_short(&sent).await.expect("200 KiB");
            a.send_short(b"again").await.expect("first copy");
            a.send_short(b"again").await.expect("second copy");
        })
        .await;
        let mut received = Vec::new();
        while let Ok(message) = p.b_in.shorts.try_recv() {
            received.push(message);
        }
        assert_eq!(received.len(), 4);
        assert_eq!(p.b.charged().0, 4, "queued messages hold their charge");
        let received: Vec<Vec<u8>> = received
            .into_iter()
            .map(|m| m.into_payload().to_vec())
            .collect();
        assert_eq!(received[0], b"x");
        assert!(received[1] == big, "the 200 KiB message arrived intact");
        assert_eq!(received[2], b"again");
        assert_eq!(received[3], b"again");
        assert_eq!(
            p.b.charged(),
            (0, 0),
            "taking the payload released the charge"
        );
        assert_eq!(p.b.stats().count(QuicInboundOutcome::DeliveredShort), 4);
        let a_key = super::super::identity::PeerKey::of(p.pair.a_keys.public()).unwrap();
        assert!(
            p.b.peer_key() == &a_key,
            "the path is attributed to the proven key"
        );
    })
}

/// A 1 MiB stream goes through the inbound adapter into an ordinary
/// `StreamHandle`: its metadata arrives with it, a fork and the original
/// both assemble the same bytes, the fork has the stream's identity, and
/// the charge lasts until the last handle is gone. Sent from bytes and
/// from uneven chunks.
#[test]
fn a_one_mib_stream_round_trips_through_the_adapter() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-stream", 2, QuicPathLimits::DEFAULT).await;
        let data = payload(MIB);
        let (id1, id2) = (StreamId::next(), StreamId::next());
        let a = p.a.clone();
        let sent = Bytes::from(data.clone());
        p.drive(async move {
            a.send_stream(id1, Some(b"announce"), sent.clone())
                .await
                .expect("from bytes");
            // Uneven chunks, crossing fragment boundaries.
            let mut chunks = Vec::new();
            let mut at = 0;
            for size in [1, 999, 4096, 1089, 70_000].iter().cycle() {
                if at == sent.len() {
                    break;
                }
                let end = (at + size).min(sent.len());
                chunks.push(Ok(sent.slice(at..end)));
                at = end;
            }
            a.send_stream_chunks(id2, sent.len() as u64, None, futures::stream::iter(chunks))
                .await
                .expect("from chunks");
        })
        .await;
        let first = p.b_in.streams.try_recv().expect("first stream");
        let second = p.b_in.streams.try_recv().expect("second stream");
        assert_eq!((first.stream_id, second.stream_id), (id1, id2));
        assert_eq!(first.metadata.as_deref(), Some(&b"announce"[..]));
        assert_eq!(second.metadata, None);
        for arrival in [&first, &second] {
            assert!(arrival.handle.is_complete());
            assert_eq!(arrival.handle.received_bytes(), MIB as u64);
            assert_eq!(
                arrival.handle.total_fragments(),
                MIB.div_ceil(FRAGMENT_PAYLOAD_SIZE) + 1,
                "the buffer has the UDP slot count"
            );
        }

        let fork = first.handle.fork();
        let identity = IncomingStream::from(first.handle.clone()).identity();
        assert!(IncomingStream::from(fork.clone()).is(&identity));
        assert!(!IncomingStream::from(second.handle.clone()).is(&identity));
        let (original, forked) = p
            .drive(async move {
                let (a, b) = tokio::join!(first.handle.assemble(), fork.assemble());
                (a.expect("original"), b.expect("fork"))
            })
            .await;
        assert!(original == data && forked == data, "both copies intact");
        let piped: Vec<u8> = second.handle.try_assemble().expect("complete");
        assert!(piped == data, "the chunked copy is intact");

        assert_eq!(
            p.b.charged().0,
            1,
            "the second stream's handle is still held"
        );
        drop(second);
        assert_eq!(p.b.charged(), (0, 0), "every handle gone, charge released");
        assert_eq!(p.b.stats().count(QuicInboundOutcome::CompletedStream), 2);
        assert_eq!(p.b.stats().count(QuicInboundOutcome::AnnouncedStream), 2);
    })
}

/// Metadata up to `MAX_METADATA_BYTES` arrives byte for byte; a control
/// frame up to `MAX_CONTROL_BYTES` arrives on the control channel. Either
/// direction works.
#[test]
fn metadata_and_control_frames_are_dispatched() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-meta", 3, QuicPathLimits::DEFAULT).await;
        let metadata = payload(MAX_METADATA_BYTES as usize);
        let control = payload(MAX_CONTROL_BYTES as usize);
        let id = StreamId::next();
        let (a, b) = (p.a.clone(), p.b.clone());
        let (m, c) = (metadata.clone(), control.clone());
        p.drive(async move {
            a.send_stream(id, Some(&m), Bytes::from_static(b"body"))
                .await
                .expect("stream");
            a.send_control(&c).await.expect("control");
            b.send_control(b"ready").await.expect("control back");
        })
        .await;
        let arrival = p.b_in.streams.try_recv().expect("stream");
        assert!(arrival.metadata.as_deref() == Some(&metadata[..]));
        assert_eq!(arrival.handle.try_assemble().unwrap(), b"body");
        assert!(p.b_in.controls.try_recv().unwrap() == control);
        assert_eq!(&p.a_in.controls.try_recv().unwrap()[..], b"ready");
        assert_eq!(p.b.stats().count(QuicInboundOutcome::DeliveredControl), 1);
    })
}

/// Metadata the UDP transport would not embed in fragment #1 (longer than
/// `MAX_METADATA_BYTES`) is left out of the frame, as on UDP: the stream is
/// delivered with no metadata, from bytes and from chunks. Empty metadata
/// arrives as `None` too.
#[test]
fn oversized_or_empty_metadata_is_left_out() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-meta-big", 23, QuicPathLimits::DEFAULT).await;
        let big = payload(MAX_METADATA_BYTES as usize + 1);
        let a = p.a.clone();
        p.drive(async move {
            a.send_stream(StreamId::next(), Some(&big), Bytes::from_static(b"one"))
                .await
                .expect("delivered without the metadata");
            let chunks = futures::stream::iter(vec![Ok(Bytes::from_static(b"two"))]);
            a.send_stream_chunks(StreamId::next(), 3, Some(&big), chunks)
                .await
                .expect("delivered without the metadata");
            a.send_stream(StreamId::next(), Some(&[]), Bytes::from_static(b"three"))
                .await
                .expect("delivered");
        })
        .await;
        for body in [&b"one"[..], b"two", b"three"] {
            let arrival = p.b_in.streams.try_recv().expect("announced");
            assert_eq!(arrival.metadata, None);
            assert_eq!(arrival.handle.try_assemble().unwrap(), body);
        }
        assert_eq!(
            p.b.stats().count(QuicInboundOutcome::HeaderMetadataTooLong),
            0
        );
    })
}

/// Every header the receiver must refuse is stopped with `HEADER_REFUSED`
/// and counted under its own reason; an invalid declared length is also
/// counted as the UDP transport counts it. A header cut short is counted
/// as truncated. Nothing is delivered.
#[test]
fn every_refused_header_is_stopped_and_counted() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-headers", 4, QuicPathLimits::DEFAULT).await;
        use QuicInboundOutcome as O;
        let named = 0x8000_0001;
        let cases = vec![
            (header(2, 0, 0, 10, 0), O::HeaderUnknownVersion),
            (header(1, 9, 0, 10, 0), O::HeaderUnknownKind),
            (header(1, 0, 0, 0, 0), O::HeaderInvalidLength),
            (
                header(1, 1, named, MAX_STREAM_TOTAL_BYTES + 1, 0),
                O::HeaderInvalidLength,
            ),
            (
                header(1, 2, 0, MAX_CONTROL_BYTES + 1, 0),
                O::HeaderInvalidControlLength,
            ),
            (
                header(1, 1, named, 10, MAX_METADATA_BYTES + 1),
                O::HeaderMetadataTooLong,
            ),
            (header(1, 0, 0, 10, 1), O::HeaderUnexpectedMetadata),
            (header(1, 2, 7, 10, 0), O::HeaderUnexpectedStreamId),
            (header(1, 1, 7, 10, 0), O::HeaderNotCallerNamed),
        ];
        for (bytes, outcome) in cases {
            let conn = p.a_raw();
            let mut with_body = bytes.clone();
            with_body.extend_from_slice(&[0u8; 10]);
            let code = p.drive(raw_unfinished(conn, with_body)).await;
            assert_eq!(
                code,
                Some(u64::from(stop_code::HEADER_REFUSED)),
                "{outcome:?}"
            );
        }
        // Two cases share this reason.
        assert_eq!(p.b.stats().count(O::HeaderInvalidLength), 2);
        for outcome in [
            O::HeaderUnknownVersion,
            O::HeaderUnknownKind,
            O::HeaderInvalidControlLength,
            O::HeaderMetadataTooLong,
            O::HeaderUnexpectedMetadata,
            O::HeaderUnexpectedStreamId,
            O::HeaderNotCallerNamed,
        ] {
            assert_eq!(p.b.stats().count(outcome), 1, "{outcome:?}");
        }
        assert_eq!(
            p.b.stats()
                .shared_count(InboundStreamOutcome::RefusedInvalidLength),
            2
        );

        let conn = p.a_raw();
        let code = p
            .drive(raw_stream(conn, header(1, 0, 0, 10, 0)[..5].to_vec()))
            .await;
        assert_eq!(code, None, "a finished stream has nothing left to stop");
        assert_eq!(p.b.stats().count(O::HeaderTruncated), 1);

        assert!(p.b_in.shorts.try_recv().is_err());
        assert!(p.b_in.streams.try_recv().is_err());
        assert!(p.b_in.controls.try_recv().is_err());
        assert_eq!(p.b.charged(), (0, 0));
    })
}

/// A stream or short message that does not fit the inbound budget is
/// stopped with `BUDGET_REFUSED` before anything is created for it, and
/// counted as the UDP transport counts it, by bytes and by stream count.
///
/// The refused messages are larger than the flow-control window, so their
/// senders are blocked on connection-level credit when the stop arrives.
/// The later sends on the same connection succeed only because the sender
/// resets a stopped stream (`Outgoing`); without that the refused stream
/// keeps the connection's whole window and this test runs out of virtual
/// time.
#[test]
fn a_stream_over_the_budget_is_stopped_and_counted() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let small = 10_000u64;
        let limits = QuicPathLimits {
            max_streams: 2,
            max_bytes: ChargeOverhead::DEFAULT.cost(small),
            ..QuicPathLimits::DEFAULT
        };
        let mut p = paths("quic-path-budget", 5, limits).await;
        let a = p.a.clone();
        let (big_stream, big_short) = p
            .drive(async move {
                let stream = a.send_stream(StreamId::next(), None, over_window()).await;
                let short = a.send_short(&over_window()).await;
                (stream, short)
            })
            .await;
        assert_stopped(big_stream, stop_code::BUDGET_REFUSED);
        assert_stopped(big_short, stop_code::BUDGET_REFUSED);
        assert_eq!(
            p.b.stats()
                .shared_count(InboundStreamOutcome::RefusedConnectionBytes),
            2
        );
        assert!(p.b_in.streams.try_recv().is_err(), "nothing was announced");

        // Two small streams are held (their handles sit in the channel);
        // a third is refused by count.
        let a = p.a.clone();
        let third = p
            .drive(async move {
                for _ in 0..2 {
                    a.send_stream(StreamId::next(), None, Bytes::from_static(b"held"))
                        .await
                        .expect("fits");
                }
                a.send_stream(StreamId::next(), None, over_window()).await
            })
            .await;
        assert_stopped(third, stop_code::BUDGET_REFUSED);
        assert_eq!(
            p.b.stats()
                .shared_count(InboundStreamOutcome::RefusedConnectionStreams),
            1
        );
        assert_eq!(p.b.charged().0, 2);
        // Once the held streams are gone, the next one fits.
        while p.b_in.streams.try_recv().is_ok() {}
        let a = p.a.clone();
        p.drive(async move {
            a.send_stream(StreamId::next(), None, Bytes::from_static(b"fits"))
                .await
                .expect("fits after release")
        })
        .await;
    })
}

/// A stream that ends before its declared length, or carries more, fails
/// stream-scoped: its handle is cancelled and never reports complete, its
/// consumer gets `Cancelled`, the failure is counted, and the connection
/// carries on. Also for short messages, which are then not delivered.
#[test]
fn truncated_and_overlong_streams_fail_and_are_counted() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-bounds", 6, QuicPathLimits::DEFAULT).await;
        let f = FRAGMENT_PAYLOAD_SIZE;
        let cases: Vec<(u64, usize, QuicInboundOutcome)> = vec![
            // One byte short, at and off a fragment boundary.
            (3 * f as u64, 3 * f - 1, QuicInboundOutcome::Truncated),
            (3 * f as u64, 2 * f, QuicInboundOutcome::Truncated),
            (100, 0, QuicInboundOutcome::Truncated),
            // One byte over, at and off a fragment boundary.
            (3 * f as u64, 3 * f + 1, QuicInboundOutcome::Overlong),
            (100, 101, QuicInboundOutcome::Overlong),
        ];
        for (n, (total, sent, outcome)) in cases.into_iter().enumerate() {
            let before = p.b.stats().count(outcome);
            let mut bytes = header(1, 1, StreamId::next().0, total, 0);
            bytes.extend_from_slice(&payload(sent));
            let conn = p.a_raw();
            p.drive(raw_stream(conn, bytes)).await;
            let arrival = p
                .b_in
                .streams
                .try_recv()
                .expect("announced before the payload");
            assert!(!arrival.handle.is_complete(), "case {n}");
            assert!(arrival.handle.received_bytes() < total, "case {n}");
            assert_eq!(
                arrival.handle.assemble().await,
                Err(StreamError::Cancelled),
                "case {n}"
            );
            assert_eq!(p.b.stats().count(outcome), before + 1, "case {n}");
        }
        // Short (kind 0) and control (kind 2) frames, one byte short and over.
        for (kind, total, sent, outcome) in [
            (0, 10, 9, QuicInboundOutcome::Truncated),
            (0, 10, 11, QuicInboundOutcome::Overlong),
            (2, 10, 9, QuicInboundOutcome::Truncated),
            (2, 10, 11, QuicInboundOutcome::Overlong),
        ] {
            let before = p.b.stats().count(outcome);
            let mut bytes = header(1, kind, 0, total, 0);
            bytes.extend_from_slice(&payload(sent));
            let conn = p.a_raw();
            p.drive(raw_stream(conn, bytes)).await;
            assert_eq!(p.b.stats().count(outcome), before + 1);
        }
        // An overlong stream left unfinished: the stop reaches the sender.
        let mut bytes = header(1, 1, StreamId::next().0, 100, 0);
        bytes.extend_from_slice(&payload(101));
        let conn = p.a_raw();
        let code = p.drive(raw_unfinished(conn, bytes)).await;
        assert_eq!(code, Some(u64::from(stop_code::OVERLONG)));
        drop(p.b_in.streams.try_recv().expect("announced"));
        assert!(
            p.b_in.shorts.try_recv().is_err(),
            "no short message delivered"
        );
        assert!(
            p.b_in.controls.try_recv().is_err(),
            "no control frame delivered"
        );
        assert_eq!(p.b.stats().count(QuicInboundOutcome::CompletedStream), 0);
        assert_eq!(
            p.b.charged(),
            (0, 0),
            "every failed stream released its charge"
        );

        // The connection still carries a good stream.
        let a = p.a.clone();
        p.drive(async move {
            a.send_stream(StreamId::next(), None, Bytes::from_static(b"fine"))
                .await
                .expect("still connected")
        })
        .await;
        assert!(p.b_in.streams.try_recv().unwrap().handle.is_complete());
    })
}

/// A sender whose chunk source delivers the wrong length, or fails, resets
/// the stream; the receiver cancels the handle and counts the reset.
#[test]
fn a_sender_that_gives_up_resets_the_stream() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-reset", 7, QuicPathLimits::DEFAULT).await;
        let a = p.a.clone();
        let (short, long, failed) = p
            .drive(async move {
                let chunk = || Ok(Bytes::from(payload(5000)));
                let short = a
                    .send_stream_chunks(
                        StreamId::next(),
                        20_000,
                        None,
                        futures::stream::iter(vec![chunk()]),
                    )
                    .await;
                let long = a
                    .send_stream_chunks(
                        StreamId::next(),
                        6000,
                        None,
                        futures::stream::iter(vec![chunk(), chunk()]),
                    )
                    .await;
                let failed = a
                    .send_stream_chunks(
                        StreamId::next(),
                        20_000,
                        None,
                        futures::stream::iter(vec![chunk(), Err(StreamError::InactivityTimeout)]),
                    )
                    .await;
                // Let the resets reach the receiver.
                a.send_short(b"after").await.expect("still connected");
                (short, long, failed)
            })
            .await;
        assert!(matches!(
            short,
            Err(QuicSendError::LengthMismatch {
                declared: 20_000,
                delivered: 5000
            })
        ));
        assert!(matches!(
            long,
            Err(QuicSendError::LengthMismatch {
                declared: 6000,
                delivered: 10_000
            })
        ));
        assert!(matches!(
            failed,
            Err(QuicSendError::Source(StreamError::InactivityTimeout))
        ));
        let mut arrivals = 0;
        while let Ok(arrival) = p.b_in.streams.try_recv() {
            arrivals += 1;
            assert!(!arrival.handle.is_complete());
            assert!(arrival.handle.is_cancelled());
        }
        // quinn discards the unread data of a reset stream, so a stream
        // reset before its header was read is never announced; every one
        // is counted.
        assert!(arrivals <= 3);
        assert_eq!(p.b.stats().count(QuicInboundOutcome::ResetByPeer), 3);
        assert_eq!(p.b.stats().count(QuicInboundOutcome::CompletedStream), 0);
        assert_eq!(p.b.charged().0, 1, "only the queued short message");
        drop(p.b_in.shorts.try_recv().expect("after"));
        assert_eq!(p.b.charged(), (0, 0));
    })
}

/// A stream that stops making progress is evicted after the idle timeout,
/// and one that keeps trickling is evicted at its age limit: stopped with
/// `EVICTED`, counted as the UDP transport counts them, handle cancelled.
#[test]
fn idle_and_old_streams_are_evicted() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-evict", 8, QuicPathLimits::DEFAULT).await;
        let idle = streaming_handle_idle_timeout();
        let time = p.time();

        // Idle: half the payload, then nothing.
        let conn = p.a_raw();
        let started = time.now_nanos();
        let code = p
            .drive(async move {
                let mut send = conn.open_uni().await.unwrap();
                let mut bytes = header(1, 1, StreamId::next().0, 10_000, 0);
                bytes.extend_from_slice(&payload(5000));
                send.write_all(&bytes).await.unwrap();
                send.stopped().await.unwrap().map(|c| c.into_inner())
            })
            .await;
        assert_eq!(code, Some(u64::from(stop_code::EVICTED)));
        let waited = Duration::from_nanos(time.now_nanos() - started);
        assert!(
            waited >= idle,
            "evicted after {waited:?}, idle timeout {idle:?}"
        );
        assert!(waited < idle * 2, "evicted only after {waited:?}");
        assert_eq!(
            p.b.stats().shared_count(InboundStreamOutcome::EvictedIdle),
            1
        );
        assert!(p.b_in.streams.try_recv().unwrap().handle.is_cancelled());

        // Age: a byte every half idle period, so it is never idle.
        let total = 100_000u64;
        let age = max_inbound_stream_age(total);
        let conn = p.a_raw();
        let sleeper = time.clone();
        let started = time.now_nanos();
        let code = p
            .drive(async move {
                let mut send = conn.open_uni().await.unwrap();
                send.write_all(&header(1, 1, StreamId::next().0, total, 0))
                    .await
                    .unwrap();
                loop {
                    if let Err(quinn::WriteError::Stopped(code)) = send.write_all(b"x").await {
                        return Some(code.into_inner());
                    }
                    tokio::select! {
                        () = sleeper.sleep(idle / 2) => {}
                        stopped = send.stopped() => return stopped.unwrap().map(|c| c.into_inner()),
                    }
                }
            })
            .await;
        assert_eq!(code, Some(u64::from(stop_code::EVICTED)));
        let waited = Duration::from_nanos(time.now_nanos() - started);
        assert!(waited >= age, "evicted after {waited:?}, age limit {age:?}");
        assert_eq!(
            p.b.stats().shared_count(InboundStreamOutcome::EvictedAge),
            1
        );
        assert_eq!(
            p.b.stats().shared_count(InboundStreamOutcome::EvictedIdle),
            1,
            "the trickling stream was never idle"
        );
        assert!(p.b_in.streams.try_recv().unwrap().handle.is_cancelled());
        assert_eq!(p.b.charged(), (0, 0));
    })
}

/// A stream that trickles its header (or a control frame) never goes idle
/// but holds one of the peer's stream slots while it does; it has no budget
/// charge, so it gets the smallest age limit and is evicted at it.
#[test]
fn an_uncharged_stream_is_evicted_at_its_age_limit() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-uncharged-age", 17, QuicPathLimits::DEFAULT).await;
        let idle = streaming_handle_idle_timeout();
        let age = max_inbound_stream_age(0);
        let (conn, time) = (p.a_raw(), p.time());
        let started = time.now_nanos();
        let code = p
            .drive(async move {
                let mut send = conn.open_uni().await.unwrap();
                // Far more trickle time than the age limit: the header would
                // complete only after HEADER_LEN periods of 0.9 x idle.
                for byte in header(1, 0, 0, 10, 0) {
                    if let Err(quinn::WriteError::Stopped(code)) = send.write_all(&[byte]).await {
                        return Some(code.into_inner());
                    }
                    tokio::select! {
                        () = time.sleep(idle * 9 / 10) => {}
                        stopped = send.stopped() => {
                            return stopped.unwrap().map(|c| c.into_inner())
                        }
                    }
                }
                None
            })
            .await;
        assert_eq!(code, Some(u64::from(stop_code::EVICTED)));
        let waited = Duration::from_nanos(p.time().now_nanos() - started);
        assert!(
            waited >= age && waited < age + idle,
            "evicted after {waited:?}"
        );
        assert_eq!(p.b.stats().count(QuicInboundOutcome::AgedUncharged), 1);
        assert_eq!(p.b.stats().count(QuicInboundOutcome::IdleUncharged), 0);
    })
}

/// A full channel never makes the receiver wait: a short message or control
/// frame is dropped and counted, a stream is stopped with `CHANNEL_FULL`
/// and its charge released. A second stream with an id the path already
/// holds is stopped with `STREAM_ID_IN_USE`.
#[test]
fn full_channels_and_reused_ids_are_refused_without_waiting() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let limits = QuicPathLimits {
            short_capacity: 1,
            stream_capacity: 1,
            control_capacity: 1,
            ..QuicPathLimits::DEFAULT
        };
        let mut p = paths("quic-path-full", 9, limits).await;
        let a = p.a.clone();
        let held = StreamId::next();
        let (reused, overflow) = p
            .drive(async move {
                a.send_short(b"one").await.unwrap();
                a.send_short(b"two").await.unwrap();
                a.send_control(b"one").await.unwrap();
                a.send_control(b"two").await.unwrap();
                a.send_stream(held, None, Bytes::from_static(b"held"))
                    .await
                    .unwrap();
                let reused = a.send_stream(held, None, over_window()).await;
                let overflow = a.send_stream(StreamId::next(), None, over_window()).await;
                (reused, overflow)
            })
            .await;
        assert_stopped(reused, stop_code::STREAM_ID_IN_USE);
        assert_stopped(overflow, stop_code::CHANNEL_FULL);
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::ShortDroppedFull), 1);
        assert_eq!(stats.count(QuicInboundOutcome::ControlDroppedFull), 1);
        assert_eq!(stats.count(QuicInboundOutcome::StreamIdInUse), 1);
        assert_eq!(stats.count(QuicInboundOutcome::StreamDroppedFull), 1);
        assert_eq!(&p.b_in.shorts.try_recv().unwrap().payload()[..], b"one");
        assert_eq!(&p.b_in.controls.try_recv().unwrap()[..], b"one");
        assert_eq!(p.b_in.streams.try_recv().unwrap().stream_id, held);
        assert_eq!(p.b.charged(), (0, 0));
    })
}

/// quinn's uni stream limit is back-pressure: with every one of the 256
/// stream credits held by streams the receiver is still reading, the next
/// send waits in `open_uni`. quinn returns credit in batches: it announces
/// new credit only once more than 1/8 of the limit (33 streams) has been
/// freed, so one finished stream is not enough and 33 are.
#[test]
fn the_stream_limit_is_back_pressure() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-many", 10, QuicPathLimits::DEFAULT).await;
        let limit = super::super::config::MAX_UNI_STREAMS as usize;
        let batch = limit / 8 + 1;
        let (conn, a, time) = (p.a_raw(), p.a.clone(), p.time());
        let held = p
            .drive(async move {
                // Each holds a credit: two header bytes, never finished.
                let mut held = Vec::new();
                for _ in 0..limit {
                    let mut send = conn.open_uni().await.unwrap();
                    send.write_all(&[1, 0]).await.unwrap();
                    held.push(send);
                    time.sleep(Duration::from_millis(2)).await;
                }
                let send = a.send_short(b"waits");
                tokio::pin!(send);
                let mut finished = 0;
                for target in [1, batch] {
                    tokio::select! {
                        result = &mut send => {
                            panic!("sent with {finished} streams finished: {result:?}")
                        }
                        () = time.sleep(Duration::from_secs(5)) => {}
                    }
                    // Complete held streams as valid short messages.
                    while finished < target {
                        let mut stream = held.remove(0);
                        let mut rest = header(1, 0, 0, 3, 0);
                        rest.drain(..2);
                        rest.extend_from_slice(b"abc");
                        stream.write_all(&rest).await.unwrap();
                        stream.finish().unwrap();
                        finished += 1;
                    }
                }
                send.await.expect("sent once credit came back");
                held
            })
            .await;
        assert_eq!(
            p.b.stats().count(QuicInboundOutcome::DeliveredShort) as usize,
            batch + 1
        );
        drop(held);
    })
}

/// A receiver that never drains its channel loses what does not fit and
/// counts it, while every send reports success: the bytes were acknowledged
/// (a successful send is not a delivery).
#[test]
fn a_flood_into_an_undrained_receiver_is_dropped_and_counted() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-flood", 12, QuicPathLimits::DEFAULT).await;
        let count = SHORT_CHANNEL_CAPACITY + 44;
        let a = p.a.clone();
        let results = p
            .drive(async move {
                futures::future::join_all((0..count).map(|i| {
                    let a = a.clone();
                    async move { a.send_short(&(i as u32).to_be_bytes()).await }
                }))
                .await
            })
            .await;
        assert!(
            results.iter().all(Result::is_ok),
            "every send reads as success"
        );
        let stats = p.b.stats();
        assert_eq!(
            stats.count(QuicInboundOutcome::DeliveredShort) as usize,
            SHORT_CHANNEL_CAPACITY
        );
        assert_eq!(stats.count(QuicInboundOutcome::ShortDroppedFull), 44);
    })
}

/// Once a stream's state is gone its id can be used again: after a stream
/// completes or fails and its last handle is dropped, a new stream with the
/// same id is admitted (not refused as `StreamIdInUse`). The receive side
/// releases the charge itself: these checks never call `charged()`, whose
/// `release_gone` would hide a charge left behind. A stream that fails
/// before it is announced (cut-short or reset metadata) is released by its
/// reader at once.
#[test]
fn a_stream_id_is_free_again_once_its_state_is_gone() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-reuse", 13, QuicPathLimits::DEFAULT).await;
        let id = StreamId::next();
        for round in 0..3 {
            let a = p.a.clone();
            p.drive(async move {
                a.send_stream(id, None, Bytes::from_static(b"first"))
                    .await
                    .expect("admitted")
            })
            .await;
            let arrival = p.b_in.streams.try_recv().expect("announced");
            assert!(arrival.handle.is_complete(), "round {round}");
            drop(arrival);
        }
        // A failed stream, then the same id again.
        let mut bytes = header(1, 1, id.0, 100, 0);
        bytes.extend_from_slice(&payload(10));
        let conn = p.a_raw();
        p.drive(raw_stream(conn, bytes)).await;
        drop(p.b_in.streams.try_recv().expect("announced"));
        let a = p.a.clone();
        p.drive(async move {
            a.send_stream(id, None, Bytes::from_static(b"again"))
                .await
                .expect("admitted after a failure")
        })
        .await;
        drop(p.b_in.streams.try_recv().expect("announced"));
        assert_eq!(p.b.stats().count(QuicInboundOutcome::StreamIdInUse), 0);
        assert_eq!(p.b.stats().count(QuicInboundOutcome::CompletedStream), 4);

        // Metadata cut short, and metadata reset by the sender: never
        // announced, and the reader's guard released the charge itself.
        // (The last stream's charge may still be counted: its handle was
        // dropped after its reader ended, which releases nothing until the
        // id is used again or the budget needs room.)
        let before = p.b.charged_streams_now();
        let mut cut = header(1, 1, StreamId::next().0, 100, 50);
        cut.extend_from_slice(&payload(20));
        let conn = p.a_raw();
        p.drive(raw_stream(conn, cut)).await;
        let conn = p.a_raw();
        let reset_id = StreamId::next().0;
        p.drive(async move {
            let mut send = conn.open_uni().await.unwrap();
            let mut bytes = header(1, 1, reset_id, 100, 50);
            bytes.extend_from_slice(&payload(20));
            send.write_all(&bytes).await.unwrap();
            send.reset(9u32.into()).unwrap();
        })
        .await;
        let a = p.a.clone();
        p.drive(async move { a.send_short(b"sync").await.unwrap() })
            .await;
        assert!(p.b_in.streams.try_recv().is_err(), "neither was announced");
        assert_eq!(p.b.stats().count(QuicInboundOutcome::Truncated), 2);
        assert_eq!(p.b.stats().count(QuicInboundOutcome::ResetByPeer), 1);
        assert_eq!(
            p.b.charged_streams_now(),
            before + 1,
            "only the queued sync message was added"
        );
    })
}

/// A sender whose chunk source never yields still sees the receiver's stop:
/// waiting for the next chunk races the stop, and the stream is reset.
#[test]
fn a_stalled_source_still_sees_the_stop() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let limits = QuicPathLimits {
            max_bytes: ChargeOverhead::DEFAULT.cost(10_000),
            ..QuicPathLimits::DEFAULT
        };
        let p = paths("quic-path-stall", 14, limits).await;
        let a = p.a.clone();
        let result = p
            .drive(async move {
                a.send_stream_chunks(
                    StreamId::next(),
                    1_000_000,
                    None,
                    futures::stream::pending::<Result<Bytes, StreamError>>(),
                )
                .await
            })
            .await;
        assert_stopped(result, stop_code::BUDGET_REFUSED);
    })
}

/// Dropping a send future mid-payload resets the stream: the receiver sees
/// `ResetByPeer` and cancels the handle, never a stream that just ended.
#[test]
fn a_cancelled_send_resets_the_stream() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-cancel", 15, QuicPathLimits::DEFAULT).await;
        let a = p.a.clone();
        let mut streams = std::mem::replace(&mut p.b_in.streams, tokio::sync::mpsc::channel(1).1);
        let time = p.time();
        let arrival = p
            .drive(async move {
                let arrival = tokio::select! {
                    result = a.send_stream(StreamId::next(), None, over_window()) => {
                        panic!("finished before it was announced: {result:?}")
                    }
                    arrival = streams.recv() => arrival.expect("announced"),
                };
                // The send future is dropped here, mid-payload.
                for _ in 0..1000 {
                    if arrival.handle.is_cancelled() {
                        break;
                    }
                    time.sleep(Duration::from_millis(10)).await;
                }
                arrival
            })
            .await;
        assert!(arrival.handle.is_cancelled());
        assert!(!arrival.handle.is_complete());
        assert_eq!(p.b.stats().count(QuicInboundOutcome::ResetByPeer), 1);
        assert_eq!(p.b.stats().count(QuicInboundOutcome::Truncated), 0);
    })
}

/// The header, metadata, short and control reads keep no slice of quinn's
/// datagram buffers: each result is its own allocation of exactly its
/// length (`try_into_mut` succeeds only for the sole owner, and its
/// capacity is the allocation behind the bytes). A slice of a datagram
/// would keep the whole datagram, about 1200 bytes, for a few bytes of
/// data. Also with a control frame trickled one byte per datagram.
#[test]
fn reads_keep_no_slice_of_a_datagram() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-copy", 16, QuicPathLimits::DEFAULT).await;
        let (a, conn, time) = (p.a.clone(), p.a_raw(), p.time());
        p.drive(async move {
            a.send_stream(
                StreamId::next(),
                Some(b"0123456789"),
                Bytes::from_static(b"body"),
            )
            .await
            .unwrap();
            a.send_short(b"short").await.unwrap();
            a.send_control(b"control").await.unwrap();
            let mut send = conn.open_uni().await.unwrap();
            send.write_all(&header(1, 2, 0, 200, 0)).await.unwrap();
            for byte in payload(200) {
                send.write_all(&[byte]).await.unwrap();
                time.sleep(Duration::from_millis(5)).await;
            }
            send.finish().unwrap();
            send.stopped().await.unwrap();
        })
        .await;
        let exact = |bytes: Bytes, what: &str| {
            let len = bytes.len();
            let owned = bytes
                .try_into_mut()
                .unwrap_or_else(|_| panic!("{what}: shared with another owner"));
            assert_eq!(owned.capacity(), len, "{what}: allocation is its length");
        };
        let arrival = p.b_in.streams.try_recv().unwrap();
        let mut arrival = arrival;
        exact(arrival.metadata.take().unwrap(), "metadata");
        drop(arrival);
        exact(p.b_in.shorts.try_recv().unwrap().into_payload(), "short");
        exact(p.b_in.controls.try_recv().unwrap(), "control");
        let trickled = p.b_in.controls.try_recv().unwrap();
        assert!(trickled == payload(200));
        exact(trickled, "trickled control");
    })
}

/// Payload fragments hold at most one datagram each: a fragment built from
/// small chunks is a fresh buffer of exactly its length (a grown buffer
/// would keep up to twice it), and a fragment kept as a slice of a
/// datagram keeps at most `MAX_UDP_PAYLOAD`. Sent as alternating
/// 1129-byte and 1-byte frames, one per datagram.
#[test]
fn payload_fragments_hold_at_most_one_datagram() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-frag-mem", 18, QuicPathLimits::DEFAULT).await;
        let total = 4 * FRAGMENT_PAYLOAD_SIZE;
        let data = payload(total);
        let (conn, time) = (p.a_raw(), p.time());
        let sent = data.clone();
        p.drive(async move {
            let mut send = conn.open_uni().await.unwrap();
            send.write_all(&header(1, 1, StreamId::next().0, total as u64, 0))
                .await
                .unwrap();
            let mut at = 0;
            for size in [1129usize, 1].iter().cycle() {
                if at == total {
                    break;
                }
                let end = (at + size).min(total);
                send.write_all(&sent[at..end]).await.unwrap();
                at = end;
                time.sleep(Duration::from_millis(5)).await;
            }
            send.finish().unwrap();
            send.stopped().await.unwrap();
        })
        .await;
        let arrival = p.b_in.streams.try_recv().expect("announced");
        assert!(arrival.handle.is_complete());
        let mut reader = arrival.handle.stream_with_reclaim();
        drop(arrival);
        let fragments = p
            .drive(async move {
                let mut fragments = Vec::new();
                while let Some(fragment) = futures::StreamExt::next(&mut reader).await {
                    fragments.push(fragment.expect("fragment"));
                }
                fragments
            })
            .await;
        let joined: Vec<u8> = fragments.iter().flat_map(|f| f.iter().copied()).collect();
        assert!(joined == data, "the payload arrived intact");
        let max = usize::from(super::super::config::MAX_UDP_PAYLOAD);
        for (i, fragment) in fragments.into_iter().enumerate() {
            let len = fragment.len();
            let owned = fragment
                .try_into_mut()
                .unwrap_or_else(|_| panic!("fragment {i}: shared with another owner"));
            assert!(
                owned.capacity() <= max,
                "fragment {i} ({len} bytes) keeps {} bytes",
                owned.capacity()
            );
        }
    })
}

/// More short messages in flight than quinn's stream limit, with a consumer
/// that drains them, all arrive and none is dropped.
#[test]
fn more_messages_than_the_stream_limit_all_arrive_when_drained() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-drained", 19, QuicPathLimits::DEFAULT).await;
        let count = 2 * super::super::config::MAX_UNI_STREAMS as usize + 10;
        let a = p.a.clone();
        let mut shorts = std::mem::replace(&mut p.b_in.shorts, tokio::sync::mpsc::channel(1).1);
        let received = p
            .drive(async move {
                let sends = futures::future::join_all((0..count).map(|i| {
                    let a = a.clone();
                    async move { a.send_short(&(i as u32).to_be_bytes()).await }
                }));
                let receive = async {
                    let mut got = Vec::new();
                    while got.len() < count {
                        let message = shorts.recv().await.expect("open");
                        let bytes: [u8; 4] = message.payload()[..].try_into().unwrap();
                        got.push(u32::from_be_bytes(bytes));
                    }
                    got
                };
                let (results, got) = tokio::join!(sends, receive);
                assert!(results.iter().all(Result::is_ok));
                got
            })
            .await;
        let mut sorted = received;
        sorted.sort_unstable();
        assert_eq!(sorted, (0..count as u32).collect::<Vec<_>>());
        assert_eq!(p.b.stats().count(QuicInboundOutcome::ShortDroppedFull), 0);
    })
}

/// A stream that sends part of its header and then nothing is evicted at
/// the idle timeout, stopped with `EVICTED` and counted as idle uncharged.
#[test]
fn a_silent_header_is_evicted_when_idle() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-idle-header", 20, QuicPathLimits::DEFAULT).await;
        let conn = p.a_raw();
        let code = p
            .drive(raw_unfinished(conn, header(1, 0, 0, 10, 0)[..5].to_vec()))
            .await;
        assert_eq!(code, Some(u64::from(stop_code::EVICTED)));
        assert_eq!(p.b.stats().count(QuicInboundOutcome::IdleUncharged), 1);
        assert_eq!(p.b.stats().count(QuicInboundOutcome::AgedUncharged), 0);
    })
}

/// A stream whose handle the receiving side cancels mid-transfer stops
/// being read: the next fragment is refused by the cancelled handle, the
/// stream is stopped with `CANCELLED`, and it is counted.
#[test]
fn a_locally_cancelled_stream_is_stopped() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-local-cancel", 21, QuicPathLimits::DEFAULT).await;
        let total = 20 * FRAGMENT_PAYLOAD_SIZE;
        let (conn, a) = (p.a_raw(), p.a.clone());
        let id = StreamId::next().0;
        let mut send = p
            .drive(async move {
                let mut send = conn.open_uni().await.unwrap();
                let mut bytes = header(1, 1, id, total as u64, 0);
                bytes.extend_from_slice(&payload(10));
                send.write_all(&bytes).await.unwrap();
                a.send_short(b"sync").await.unwrap();
                send
            })
            .await;
        let arrival = p.b_in.streams.try_recv().expect("announced");
        arrival.handle.cancel();
        let code = p
            .drive(async move {
                if let Err(quinn::WriteError::Stopped(code)) =
                    send.write_all(&payload(total - 10)).await
                {
                    return Some(code.into_inner());
                }
                send.stopped().await.unwrap().map(|c| c.into_inner())
            })
            .await;
        assert_eq!(code, Some(u64::from(stop_code::CANCELLED)));
        assert_eq!(p.b.stats().count(QuicInboundOutcome::CancelledLocally), 1);
        assert_eq!(p.b.stats().count(QuicInboundOutcome::CompletedStream), 0);
    })
}

/// A panic in the receive task is counted and closes the connection with
/// `close_code::DOWNGRADE`, so the peer does not keep sending to a path
/// nothing reads any more, and falls back to UDP rather than reading it as
/// this node dropping it.
#[test]
fn a_panicking_receive_task_closes_the_connection() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let limits = QuicPathLimits {
            panic_in_receive: true,
            ..QuicPathLimits::DEFAULT
        };
        let p = paths("quic-path-panic", 22, limits).await;
        let (a, conn) = (p.a.clone(), p.a_raw());
        let closed = p
            .drive(async move {
                // The send itself may fail when the connection closes.
                a.send_short(b"boom").await.ok();
                conn.closed().await
            })
            .await;
        let quinn::ConnectionError::ApplicationClosed(close) = closed else {
            panic!("closed by {closed:?}");
        };
        assert_eq!(
            close.error_code.into_inner(),
            u64::from(super::super::error::close_code::DOWNGRADE)
        );
        assert_eq!(
            p.b.stats().count(QuicInboundOutcome::ReceiveTaskPanicked),
            1
        );
    })
}

/// Dropping the receiving path cancels the streams it was still receiving.
#[test]
fn dropping_the_path_cancels_streams_in_progress() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-drop", 11, QuicPathLimits::DEFAULT).await;
        let conn = p.a_raw();
        let mut bytes = header(1, 1, StreamId::next().0, 10_000, 0);
        bytes.extend_from_slice(&payload(100));
        // Kept open (unfinished) by holding it until the end of the test.
        let open = p
            .drive(async move {
                let mut send = conn.open_uni().await.unwrap();
                send.write_all(&bytes).await.unwrap();
                send
            })
            .await;
        // Let the header and metadata reach the receiver.
        let a = p.a.clone();
        p.drive(async move { a.send_short(b"sync").await.unwrap() })
            .await;
        let arrival = p.b_in.streams.try_recv().expect("announced");
        assert!(!arrival.handle.is_cancelled());
        let b =
            Arc::try_unwrap(std::mem::replace(&mut p.b, p.a.clone())).expect("the only reference");
        drop(b);
        // The receive task is aborted on its next poll.
        p.drive(async { tokio::task::yield_now().await }).await;
        assert!(arrival.handle.is_cancelled());
        drop(open);
    })
}
