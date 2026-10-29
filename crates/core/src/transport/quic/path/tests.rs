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
use crate::transport::peer_connection::inbound_budget::{
    BudgetRejection, ChargeOverhead, InboundKey, LegacyStreamToken, Liveness, Releaser,
};
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

/// Build the pair on `10.203.<subnet>.1` and `.2` and connect it. Every
/// test needs a subnet of its own: the simulated network maps each address
/// to one network for the whole process, so two tests on one subnet send
/// each other's packets into the wrong network.
async fn paths(network: &str, subnet: u8, b_limits: QuicPathLimits) -> Paths {
    paths_with(network, subnet, b_limits, None).await
}

/// [`paths`], with `b`'s path charging `b_budget` if given
/// ([`QuicPath::with_budget`]) instead of a budget of its own.
async fn paths_with(
    network: &str,
    subnet: u8,
    b_limits: QuicPathLimits,
    b_budget: Option<SharedInboundBudget>,
) -> Paths {
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
    let (b, b_in) = match b_budget {
        Some(budget) => QuicPath::with_budget(server, pair.time.clone(), b_limits, budget),
        None => QuicPath::with_limits(server, pair.time.clone(), b_limits),
    };
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

/// quinn's uni stream limit is back-pressure: with every one of the
/// `MAX_UNI_STREAMS` stream credits held by streams the receiver is still
/// reading, the next send waits in `open_uni`. quinn returns credit in
/// batches: it announces new credit only once more than 1/8 of the limit
/// (`MAX_UNI_STREAMS / 8 + 1` streams) has been freed, so one finished
/// stream is not enough and that many are.
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

/// One peer's UDP connection and QUIC path charge one allowance when the
/// path is built on the connection's budget (`QuicPath::with_budget`). The
/// UDP side is the budget handle, charged as `PeerConnection` charges it
/// (`a_quic_charge_on_the_shared_budget_refuses_a_udp_stream` drives the
/// real UDP admission against a QUIC charge).
///
/// A UDP stream that fills the allowance makes a QUIC stream that fits on
/// its own be stopped with `BUDGET_REFUSED`. Once the UDP stream's state is
/// gone and the UDP side has released its charge (not before: a QUIC
/// admission never releases a UDP charge), the QUIC stream is admitted, and
/// while that one is held the UDP side is refused in turn.
#[test]
fn udp_and_quic_paths_share_one_inbound_allowance() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let small = 10_000u64;
        // Room for one small stream, from either path.
        let shared = SharedInboundBudget::new(4, ChargeOverhead::DEFAULT.cost(small));
        let mut p = paths_with(
            "quic-path-shared",
            24,
            QuicPathLimits::DEFAULT,
            Some(shared.clone()),
        )
        .await;

        let udp_token = LegacyStreamToken::new();
        let udp_id = StreamId::next();
        {
            let mut budget = shared.lock();
            assert_eq!(budget.check(small, Releaser::Udp), Ok(()));
            budget.charge(
                InboundKey::Udp(udp_id),
                small,
                0,
                Duration::from_secs(3600),
                Liveness::Task(Arc::downgrade(&udp_token)),
            );
        }
        // Unfinished, so the stop always reaches the sender.
        let mut bytes = header(1, 1, StreamId::next().0, small, 0);
        bytes.extend_from_slice(&payload(100));
        let conn = p.a_raw();
        let code = p.drive(raw_unfinished(conn, bytes)).await;
        assert_eq!(code, Some(u64::from(stop_code::BUDGET_REFUSED)));
        assert_eq!(
            p.b.stats()
                .shared_count(InboundStreamOutcome::RefusedConnectionBytes),
            1
        );
        assert!(p.b_in.streams.try_recv().is_err(), "nothing was announced");

        // The UDP stream's state is gone, but its charge is UDP code's to
        // release: until then the QUIC side is still refused.
        drop(udp_token);
        let mut bytes = header(1, 1, StreamId::next().0, small, 0);
        bytes.extend_from_slice(&payload(100));
        let conn = p.a_raw();
        let code = p.drive(raw_unfinished(conn, bytes)).await;
        assert_eq!(code, Some(u64::from(stop_code::BUDGET_REFUSED)));
        // A short message too (kind 0): `read_short` releases QUIC charges only.
        let mut bytes = header(1, 0, 0, small, 0);
        bytes.extend_from_slice(&payload(100));
        let conn = p.a_raw();
        let code = p.drive(raw_unfinished(conn, bytes)).await;
        assert_eq!(code, Some(u64::from(stop_code::BUDGET_REFUSED)));
        assert_eq!(
            p.b.stats()
                .shared_count(InboundStreamOutcome::RefusedConnectionBytes),
            3
        );
        assert!(shared.lock().contains(InboundKey::Udp(udp_id)));

        // Released as the UDP connection releases it: the QUIC stream fits.
        assert!(shared.lock().release_if_gone(InboundKey::Udp(udp_id)));
        let a = p.a.clone();
        p.drive(async move {
            a.send_stream(StreamId::next(), None, Bytes::from(payload(small as usize)))
                .await
                .expect("admitted once the UDP charge is released")
        })
        .await;
        let arrival = p.b_in.streams.try_recv().expect("announced");
        assert!(arrival.handle.is_complete());

        // While a consumer holds it, the UDP side is refused.
        assert!(matches!(
            shared.lock().check(small, Releaser::Udp),
            Err(BudgetRejection::Bytes { .. })
        ));
        drop(arrival);
        assert_eq!(shared.lock().check(small, Releaser::Udp), Ok(()));
    })
}

/// The same numeric id held by the UDP connection and by the QUIC path is
/// two charges: a `Stream` with a UDP stream's id is admitted (not
/// `StreamIdInUse`), a short message is charged beside a UDP
/// transport-internal id, and the QUIC charges go without touching the UDP
/// one.
#[test]
fn quic_keys_never_collide_with_udp_ids() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let shared = SharedInboundBudget::new(16, u64::MAX);
        let mut p = paths_with(
            "quic-path-keys",
            25,
            QuicPathLimits::DEFAULT,
            Some(shared.clone()),
        )
        .await;
        let id = StreamId::next();
        let udp_tokens: Vec<_> = (0..2).map(|_| LegacyStreamToken::new()).collect();
        // A UDP operations stream with `id`, and a transport-internal one with
        // the number the path's first short-message key gets.
        for (key, token) in [InboundKey::Udp(id), InboundKey::Udp(StreamId(1))]
            .into_iter()
            .zip(&udp_tokens)
        {
            shared.lock().charge(
                key,
                100,
                0,
                Duration::from_secs(3600),
                Liveness::Task(Arc::downgrade(token)),
            );
        }

        let a = p.a.clone();
        p.drive(async move {
            a.send_stream(id, None, Bytes::from_static(b"same id"))
                .await
                .expect("admitted beside the UDP charge");
            a.send_short(b"short").await.expect("short");
        })
        .await;
        let arrival = p.b_in.streams.try_recv().expect("announced");
        let short = p.b_in.shorts.try_recv().expect("delivered");
        assert_eq!(p.b.stats().count(QuicInboundOutcome::StreamIdInUse), 0);
        {
            let budget = shared.lock();
            assert_eq!(
                budget.stream_count(),
                4,
                "two UDP charges and two QUIC ones"
            );
            assert!(budget.contains(InboundKey::Quic(id)));
            assert!(budget.contains(InboundKey::QuicShort(1)));
        }

        drop(arrival);
        drop(short);
        let mut budget = shared.lock();
        assert!(budget.release_if_gone(InboundKey::Quic(id)));
        assert!(budget.release_if_gone(InboundKey::QuicShort(1)));
        assert!(!budget.release_if_gone(InboundKey::Udp(id)), "still held");
        assert_eq!(budget.release_gone(Releaser::Udp), 0);
        assert_eq!(budget.stream_count(), 2, "the UDP charges remain");
        drop(budget);
        drop(udp_tokens);
    })
}

/// A QUIC stream's guard releases only its own charge on a shared budget.
/// A stream that fails before it is announced is released by its reader's
/// guard at once; a UDP charge with the same numeric id, whose state is
/// also gone, stays until UDP code releases it. Checked without
/// `release_gone`, which would hide which side released what.
#[test]
fn a_quic_guard_releases_only_its_own_charge() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let shared = SharedInboundBudget::new(16, u64::MAX);
        let mut p = paths_with(
            "quic-path-guard",
            26,
            QuicPathLimits::DEFAULT,
            Some(shared.clone()),
        )
        .await;
        let id = StreamId::next();
        let udp_token = LegacyStreamToken::new();
        shared.lock().charge(
            InboundKey::Udp(id),
            100,
            0,
            Duration::from_secs(3600),
            Liveness::Task(Arc::downgrade(&udp_token)),
        );
        drop(udp_token);

        // Metadata cut short: admitted and charged, never announced.
        let mut cut = header(1, 1, id.0, 100, 50);
        cut.extend_from_slice(&payload(20));
        let conn = p.a_raw();
        p.drive(raw_stream(conn, cut)).await;
        let a = p.a.clone();
        p.drive(async move { a.send_short(b"sync").await.unwrap() })
            .await;
        assert!(p.b_in.streams.try_recv().is_err(), "not announced");
        assert_eq!(p.b.stats().count(QuicInboundOutcome::Truncated), 1);

        let budget = shared.lock();
        assert!(
            !budget.contains(InboundKey::Quic(id)),
            "the guard released it"
        );
        assert!(
            budget.contains(InboundKey::Udp(id)),
            "the same-numbered UDP charge is UDP code's to release"
        );
        assert_eq!(
            budget.stream_count(),
            2,
            "the UDP charge and the sync message"
        );
    })
}

/// Pin: both QUIC admissions (`read_short`, `read_stream`) hold ONE guard
/// on the budget from its first use through `check` to `charge`, as the UDP
/// side does (`udp_admission_holds_one_budget_lock_from_check_to_charge`),
/// so a UDP admission sharing the budget cannot take the room in between.
/// Also pins their keys and that they release only QUIC charges.
#[test]
fn quic_admission_holds_one_budget_lock_from_check_to_charge() {
    use crate::transport::peer_connection::inbound_budget::pin;
    let source = include_str!("inbound.rs");
    let lock = "let mut budget = self.budget.lock();";
    let check = "if let Err(rejection) = budget.check(total, Releaser::Quic)";
    let body = pin::fn_body(source, "async fn read_short(");
    pin::assert_one_guard(
        &body,
        "read_short",
        lock,
        &[check, "let key = self.short_key(&budget);"],
        "budget.charge(",
    );
    let body = pin::fn_body(source, "async fn read_stream(");
    let after = pin::assert_one_guard(
        &body,
        "read_stream",
        lock,
        &[
            "let key = InboundKey::Quic(stream_id);",
            "budget.release_if_gone(key);",
            "if budget.contains(key)",
            check,
        ],
        "budget.charge(",
    );
    assert!(
        after.trim_start().starts_with("key,"),
        "read_stream: the charge must be under the stream's `Quic` key"
    );
}

// #5940 step M6: the path recycle guards (`super::recycle`).

/// `count` sends from `path` whose chunk source fails at once, so each is
/// reset on this side ([`reset_code::SENDER_ABORTED`]).
async fn abort_sends(path: Arc<QuicPath>, count: u64) {
    for _ in 0..count {
        let failed = path
            .send_stream_chunks(
                StreamId::next(),
                20_000,
                None,
                futures::stream::iter(vec![Err(StreamError::InactivityTimeout)]),
            )
            .await;
        assert!(
            matches!(failed, Err(QuicSendError::Source(_))),
            "{failed:?}"
        );
    }
}

/// The recycle close codes this node sent to `remote`. The record is
/// process-wide and keyed by address, so every test that reads it needs
/// addresses of its own (its own subnet), or it sees another test's closes.
fn recycle_closes_to(remote: SocketAddr) -> Vec<QuicCloseCode> {
    crate::transport::metrics::TRANSPORT_METRICS
        .quic_close_sent_to(remote)
        .into_iter()
        .filter(|code| {
            matches!(
                code,
                QuicCloseCode::SendAbortLimit | QuicCloseCode::PeerNotAcknowledging
            )
        })
        .collect()
}

/// #5940 step M6, guard 1: a path that has reset its limit of sends stays
/// open (checked at the limit minus one and at the limit), and the next
/// reset closes it with `close_code::SEND_ABORT_LIMIT`, counted once; the
/// peer reads it as a request to fall back to UDP.
#[test]
fn a_path_closes_once_its_reset_sends_pass_the_limit() {
    bounded(|| async move {
        use crate::transport::{QuicErrorKind, QuicRefusal};
        let _seed = GlobalRng::seed_guard(SEED);
        const ABORT_LIMIT: u64 = 8;
        let limits = QuicPathLimits {
            max_send_aborts: ABORT_LIMIT,
            ..QuicPathLimits::DEFAULT
        };
        let p = paths("quic-path-abort-limit", 40, limits).await;
        let a_addr = p.pair.a_addr;
        for (aborts, total) in [(ABORT_LIMIT - 1, ABORT_LIMIT - 1), (1, ABORT_LIMIT)] {
            let b = p.b.clone();
            p.drive(async move {
                abort_sends(b.clone(), aborts).await;
                b.send_short(b"still open")
                    .await
                    .expect("the path is still open");
            })
            .await;
            assert_eq!(p.b.send_aborts(), total);
            assert!(p.b.connection().inner().close_reason().is_none());
            assert!(recycle_closes_to(a_addr).is_empty());
        }

        let b = p.b.clone();
        p.drive(abort_sends(b, 1)).await;
        assert_eq!(p.b.send_aborts(), ABORT_LIMIT + 1);
        assert!(p.b.connection().inner().close_reason().is_some());
        assert_eq!(
            recycle_closes_to(a_addr),
            vec![QuicCloseCode::SendAbortLimit]
        );
        let seen = p.a.connection().inner().clone();
        let error = p.drive(async move { seen.closed().await }).await;
        let quinn::ConnectionError::ApplicationClosed(close) = &error else {
            panic!("the peer saw {error:?}");
        };
        assert_eq!(
            close.error_code.into_inner(),
            u64::from(super::super::error::close_code::SEND_ABORT_LIMIT)
        );
        assert_eq!(
            QuicErrorKind::from(error),
            QuicErrorKind::Refused {
                reason: QuicRefusal::Downgrade
            }
        );
    })
}

/// #5940 step M6, guard 1: a send dropped while it waits for its source
/// (a caller's timeout) is reset by `Outgoing`'s `Drop` and counted; past
/// the limit the path closes.
#[test]
fn a_dropped_send_counts_as_a_reset() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        const ABORT_LIMIT: u64 = 2;
        let limits = QuicPathLimits {
            max_send_aborts: ABORT_LIMIT,
            ..QuicPathLimits::DEFAULT
        };
        let p = paths("quic-path-abort-dropped", 41, limits).await;
        let a_addr = p.pair.a_addr;
        let drop_sends = |count: u64| {
            let (b, time) = (p.b.clone(), p.time());
            async move {
                for _ in 0..count {
                    tokio::select! {
                        sent = b.send_stream_chunks(
                            StreamId::next(),
                            20_000,
                            None,
                            futures::stream::pending(),
                        ) => panic!("a send from a pending source ended: {sent:?}"),
                        () = time.sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
        };
        p.drive(drop_sends(ABORT_LIMIT)).await;
        assert_eq!(p.b.send_aborts(), ABORT_LIMIT);
        assert!(p.b.connection().inner().close_reason().is_none());
        p.drive(drop_sends(1)).await;
        assert_eq!(p.b.send_aborts(), ABORT_LIMIT + 1);
        assert!(p.b.connection().inner().close_reason().is_some());
        assert_eq!(
            recycle_closes_to(a_addr),
            vec![QuicCloseCode::SendAbortLimit]
        );
    })
}

/// #5940 step M6, guard 1: a stop reported through the write
/// (`WriteError::Stopped`, handled by `Outgoing::written`) ends the stream
/// as the peer's stop. `Drop` then sends no reset, and nothing is counted.
#[test]
fn a_stop_reported_by_the_write_is_not_counted() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-written-stopped", 42, QuicPathLimits::DEFAULT).await;
        let a = p.a.clone();
        let (written, aborts) = p
            .drive(async move {
                let header = FrameHeader::short(10).expect("a short header");
                let mut send = a.open(&header, &[]).await.expect("open");
                let written = send.written(Err(quinn::WriteError::Stopped(
                    stop_code::BUDGET_REFUSED.into(),
                )));
                drop(send);
                (written, a.send_aborts())
            })
            .await;
        assert!(
            matches!(written, Err(QuicSendError::Stopped(code)) if code == u64::from(stop_code::BUDGET_REFUSED)),
            "{written:?}"
        );
        assert_eq!(aborts, 0);
        assert!(p.a.connection().inner().close_reason().is_none());
    })
}

/// #5940 step M6, guard 1 counts only this side's resets: streams the
/// peer stops (here refused by its budget) are not counted, and the path
/// stays open. The source yields to the runtime between small chunks, so
/// the stop often lands while the next write has room: quinn then reports
/// it through the write (`WriteError::Stopped`) as well as through the
/// `stopped` future, and `select!` picks either (about one stream in ten
/// here, so 128 streams take the write path many times). A write blocked on
/// connection flow control never reports it, so a large single write would
/// only ever see the `stopped` future.
#[test]
fn a_stop_from_the_peer_is_not_counted_as_a_reset() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let b_limits = QuicPathLimits {
            max_bytes: ChargeOverhead::DEFAULT.cost(1000),
            ..QuicPathLimits::DEFAULT
        };
        let p = paths("quic-path-abort-stops", 43, b_limits).await;
        let b_addr = p.pair.b_addr;
        let a = p.a.clone();
        let stops = p
            .drive(async move {
                let mut stops = 0;
                for _ in 0..128 {
                    let chunks = Box::pin(futures::stream::unfold((), |()| async {
                        tokio::task::yield_now().await;
                        Some((Ok(Bytes::from(vec![0u8; 1000])), ()))
                    }));
                    match a
                        .send_stream_chunks(StreamId::next(), 1024 * 1024, None, chunks)
                        .await
                    {
                        Err(QuicSendError::Stopped(code))
                            if code == u64::from(stop_code::BUDGET_REFUSED) =>
                        {
                            stops += 1;
                        }
                        other => panic!("expected a stop, got {other:?}"),
                    }
                }
                stops
            })
            .await;
        assert_eq!(stops, 128);
        assert_eq!(p.a.send_aborts(), 0);
        assert!(p.a.connection().inner().close_reason().is_none());
        assert!(recycle_closes_to(b_addr).is_empty());
    })
}

/// Drops every datagram sent from the listed addresses on `network` while
/// it lives.
struct DropFrom(String);

impl DropFrom {
    fn new(network: &str, from: &[SocketAddr]) -> Self {
        use crate::transport::in_memory_socket::{
            PacketDeliveryDecision, set_packet_delivery_callback,
        };
        let from = from.to_vec();
        set_packet_delivery_callback(
            network,
            Some(Arc::new(move |_, sender, _, _| {
                if from.contains(&sender) {
                    PacketDeliveryDecision::Drop
                } else {
                    PacketDeliveryDecision::Deliver
                }
            })),
        );
        Self(network.to_string())
    }
}

impl Drop for DropFrom {
    fn drop(&mut self) {
        crate::transport::in_memory_socket::set_packet_delivery_callback(&self.0, None);
    }
}

/// How often the scripted peer runs (receives, sends, writes).
const PEER_STEP: Duration = Duration::from_millis(20);

/// A real `QuicPath` (`a`, the client) connected to a peer `b` that is a
/// quinn-proto endpoint driven by hand on the simulated network, so a test
/// decides when `b` sends. `b` acknowledges what reaches it like any QUIC
/// stack; with `keep_sending` set it writes 64 bytes every [`PEER_STEP`] on
/// one `Stream` frame of 1 MiB (opening a new one when a write fails), so
/// it sends all the time whatever `a` does: its congestion window is
/// 1 GiB and `a` admits the stream.
struct Scripted {
    time: VirtualTime,
    network: String,
    a: Arc<QuicPath>,
    _a_in: QuicInbound,
    _a_endpoint: super::super::QuicEndpoint,
    a_addr: SocketAddr,
    b_addr: SocketAddr,
    keep_sending: Arc<std::sync::atomic::AtomicBool>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Scripted {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
        crate::transport::in_memory_socket::set_packet_delivery_callback(&self.network, None);
        crate::transport::in_memory_socket::unregister_network_time_source(&self.network);
    }
}

impl Scripted {
    async fn connect(network: &str, subnet: u8) -> Self {
        Self::connect_capped(network, subnet, None).await
    }

    /// [`Self::connect`] with a node-wide egress cap of `a_cap` bytes per
    /// second on `a`'s socket.
    async fn connect_capped(network: &str, subnet: u8, a_cap: Option<u64>) -> Self {
        use super::super::config::{self, QuicSettings};
        use super::super::identity::{PeerKey, QuicIdentity};
        use crate::transport::Socket;
        use crate::transport::crypto::TransportKeypair;
        use crate::transport::demux::DemuxSocket;
        use crate::transport::in_memory_socket::{
            SimulationSocket, register_address_network, register_network_time_source,
        };
        use crate::transport::packet_data::MAX_RECV_PACKET_SIZE;
        use std::collections::VecDeque;
        use std::time::Instant;

        let a_addr: SocketAddr = format!("10.206.{subnet}.1:5000").parse().unwrap();
        let b_addr: SocketAddr = format!("10.206.{subnet}.2:5000").parse().unwrap();
        let time = VirtualTime::new();
        register_network_time_source(network, time.clone());
        register_address_network(a_addr, network);
        register_address_network(b_addr, network);
        let settings = QuicSettings {
            max_connections: 4,
            memory_limit: 1024 * 1024 * 1024,
            idle_timeout: None,
        };
        let (a_keys, b_keys) = (TransportKeypair::new(), TransportKeypair::new());
        let mut tasks = Vec::new();

        let a_socket = Arc::new(DemuxSocket::wrap(
            SimulationSocket::bind(a_addr).await.expect("sim bind"),
            a_cap.map(|rate| crate::transport::egress_cap::EgressCap::new(rate, time.clone())),
        ));
        let a_endpoint = super::super::QuicEndpoint::bind(
            a_socket.clone(),
            a_addr,
            time.clone(),
            &settings,
            &a_keys,
        )
        .expect("endpoint a");
        tasks.push(tokio::spawn(async move {
            let mut buf = vec![0u8; MAX_RECV_PACKET_SIZE];
            while a_socket.recv_classified(&mut buf).await.is_ok() {}
        }));

        let b_identity = QuicIdentity::new(&b_keys).expect("b's identity");
        let a_peer = PeerKey::of(a_keys.public()).expect("a's key");
        let mut transport = config::transport_config(&settings, &time);
        let mut cubic = quinn::congestion::CubicConfig::default();
        cubic.initial_window(1 << 30);
        transport.congestion_controller_factory(Arc::new(cubic));
        let server = config::server_config(
            Arc::new(b_identity.server_tls(&a_peer).expect("server tls")),
            Arc::new(transport),
            config::system_clock(time.clone()),
        )
        .expect("server config");
        let mut b = super::super::memory_tests::ProtoPeer {
            addr: b_addr,
            endpoint: quinn_proto::Endpoint::new(
                Arc::new(config::endpoint_config().expect("endpoint config")),
                Some(Arc::new(server)),
                false,
                None,
            ),
            conn: None,
        };
        let b_socket = Arc::new(SimulationSocket::bind(b_addr).await.expect("sim bind"));
        type Inbox = std::sync::Mutex<VecDeque<(Vec<u8>, SocketAddr)>>;
        let inbox: Arc<Inbox> = Arc::default();
        let (socket, queue) = (b_socket.clone(), inbox.clone());
        tasks.push(tokio::spawn(async move {
            let mut buf = vec![0u8; MAX_RECV_PACKET_SIZE];
            while let Ok((len, from)) = socket.recv_from(&mut buf).await {
                queue.lock().unwrap().push_back((buf[..len].to_vec(), from));
            }
        }));
        let keep_sending = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (peer_time, sending) = (time.clone(), keep_sending.clone());
        // quinn-proto takes `Instant`s; this one is only the epoch they
        // are counted from. Every elapsed time comes from `peer_time`
        // (virtual), as in `memory_tests::connected_pair`.
        let base = Instant::now();
        tasks.push(tokio::spawn(async move {
            let mut last_stream = None;
            loop {
                peer_time.sleep(PEER_STEP).await;
                let now = base + Duration::from_nanos(peer_time.now_nanos());
                let arrived: Vec<_> = inbox.lock().unwrap().drain(..).collect();
                for (datagram, from) in arrived {
                    for reply in b.receive(now, from, &datagram) {
                        b_socket.send_to(&reply, from).await.ok();
                    }
                }
                b.pump(now);
                let ready = b.conn.as_ref().is_some_and(|(_, c)| !c.is_handshaking());
                if ready && sending.load(std::sync::atomic::Ordering::Relaxed) {
                    let conn = b.conn();
                    let written = last_stream
                        .map(|id| conn.send_stream(id).write(&[0xEE; 64]).is_ok())
                        .unwrap_or(false);
                    if !written {
                        if let Some(id) = conn.streams().open(quinn_proto::Dir::Uni) {
                            let mut first = header(1, 1, StreamId::next().0, 1024 * 1024, 0);
                            first.extend_from_slice(&[0xEE; 64]);
                            conn.send_stream(id).write(&first).ok();
                            last_stream = Some(id);
                        }
                    }
                }
                for datagram in b.transmit(now) {
                    b_socket.send_to(&datagram, a_addr).await.ok();
                }
            }
        }));

        let (endpoint, b_key) = (a_endpoint.clone(), b_keys.public().clone());
        let conn = drive(&time, LIMIT, async move {
            endpoint.connect(b_addr, &b_key).await.expect("connect")
        })
        .await;
        let (a, a_in) = QuicPath::new(conn, time.clone());
        Self {
            time,
            network: network.to_string(),
            a: Arc::new(a),
            _a_in: a_in,
            _a_endpoint: a_endpoint,
            a_addr,
            b_addr,
            keep_sending,
            tasks,
        }
    }

    /// A sample of `a`'s connection, as the acknowledgement watch takes it.
    fn sample(&self) -> recycle::AckSample {
        recycle::AckSample::of(
            self.a.connection().inner(),
            &self.a.connection().cap_holds(),
        )
        .expect("an AckCounting controller")
    }

    fn keep_sending(&self) {
        self.keep_sending
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Let `duration` of virtual time pass.
    async fn wait(&self, duration: Duration) {
        let time = self.time.clone();
        drive(&self.time, LIMIT, async move { time.sleep(duration).await }).await;
    }

    /// Send `len` bytes from `a` to `b` in the background.
    fn a_sends(&mut self, len: usize) {
        let a = self.a.clone();
        self.tasks.push(tokio::spawn(async move {
            a.send_stream(StreamId::next(), None, Bytes::from(payload(len)))
                .await
                .ok();
        }));
    }

    /// Wait until `a`'s connection closes (at most [`LIMIT`]); how long
    /// after `from` (virtual nanoseconds) it closed.
    async fn closed_after(&self, from: u64) -> Duration {
        let (conn, time) = (self.a.connection().inner().clone(), self.time.clone());
        let at = drive(&self.time, LIMIT, async move {
            conn.closed().await;
            time.now_nanos()
        })
        .await;
        Duration::from_nanos(at - from)
    }
}

/// #5940 step M6, guard 2: `a`'s packets stop reaching `b` while `b`
/// keeps sending (it never hears from `a` again, so it acknowledges
/// nothing). `a` closes the path with `close_code::PEER_NOT_ACKNOWLEDGING`
/// no earlier than `ACK_WATCH_WINDOW` after the cut and within two ticks
/// after that.
#[test]
fn a_peer_that_keeps_sending_but_acknowledges_nothing_is_closed() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-ack-watch";
        let mut s = Scripted::connect(network, 1).await;
        s.keep_sending();
        s.wait(Duration::from_secs(3)).await;
        assert!(s.a.connection().inner().close_reason().is_none());
        let _cut = DropFrom::new(network, &[s.a_addr]);
        let cut_at = s.time.now_nanos();
        s.a_sends(20_000);
        let waited = s.closed_after(cut_at).await;
        assert!(
            waited >= recycle::ACK_WATCH_WINDOW,
            "closed {waited:?} after the cut, before the window"
        );
        assert!(
            waited <= recycle::ACK_WATCH_WINDOW + 2 * recycle::ACK_WATCH_TICK,
            "closed {waited:?} after the cut"
        );
        assert_eq!(
            recycle_closes_to(s.b_addr),
            vec![QuicCloseCode::PeerNotAcknowledging]
        );
    })
}

/// #5940 step M6, guard 2: `a` is in the middle of a transfer that `b` has
/// been acknowledging when `a`'s packets stop reaching `b`. `b` keeps
/// sending. The test checks on the real connection that `b`'s last
/// acknowledgements left bytes outstanding, and that `a` closes within the
/// window of the cut. It does not tell which rule started the window: in
/// simulation `a`'s probe timer is short, so a new send starts one as soon
/// as an acknowledgement would. The unit test
/// `packets_left_outstanding_by_an_acknowledgement_close_after_the_window`
/// pins the acknowledgement rule on its own.
#[test]
fn packets_left_outstanding_when_the_peer_stops_acknowledging_close_the_path() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-ack-watch-partial";
        let mut s = Scripted::connect(network, 2).await;
        s.keep_sending();
        s.a_sends(1024 * 1024);
        s.wait(Duration::from_millis(100)).await;
        let _cut = DropFrom::new(network, &[s.a_addr]);
        let cut_at = s.time.now_nanos();
        // `b` acknowledges what it already holds at its next step; that last
        // batch leaves part of the transfer in flight.
        s.wait(Duration::from_millis(200)).await;
        assert!(s.sample().outstanding_after_acks() > 0);
        let waited = s.closed_after(cut_at).await;
        assert!(
            waited >= recycle::ACK_WATCH_WINDOW - recycle::ACK_WATCH_TICK,
            "closed {waited:?} after the cut"
        );
        assert!(
            waited <= recycle::ACK_WATCH_WINDOW + 2 * recycle::ACK_WATCH_TICK,
            "closed {waited:?} after the cut"
        );
        assert_eq!(
            recycle_closes_to(s.b_addr),
            vec![QuicCloseCode::PeerNotAcknowledging]
        );
    })
}

/// #5940 step M6, guard 2 (review finding 2): an outage of 30 s in both
/// directions in the middle of a transfer, then recovery. `b` sends all the
/// time once it is back, but has nothing new to acknowledge, while `a`'s
/// data waits for its backed-off probe timer; `b` acknowledges the probe
/// when it comes. `b` is a conforming peer: `a` must not close.
#[test]
fn recovery_from_an_outage_in_both_directions_does_not_close() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-ack-watch-outage";
        let mut s = Scripted::connect(network, 3).await;
        s.keep_sending();
        s.a_sends(1024 * 1024);
        s.wait(Duration::from_millis(100)).await;
        // `a`'s packets are lost first, so after the outage `b` has nothing
        // new to acknowledge and `a`'s data waits for its probe timer.
        let cut = DropFrom::new(network, &[s.a_addr]);
        s.wait(Duration::from_millis(100)).await;
        // `b`'s last acknowledgements, sent at its step after the cut, left
        // `a` with data outstanding.
        assert!(s.sample().outstanding_after_acks() > 0);
        drop(cut);
        let outage = DropFrom::new(network, &[s.a_addr, s.b_addr]);
        s.wait(Duration::from_secs(30)).await;
        drop(outage);
        let at_recovery = s.sample().received();
        s.wait(Duration::from_secs(60)).await;
        // `b` sent all through the minute after the outage (one write every
        // PEER_STEP, 3000 steps), so the result is not an idle peer's.
        assert!(
            s.sample().received() > at_recovery + 1000,
            "b sent too little after recovery"
        );
        assert!(
            s.a.connection().inner().close_reason().is_none(),
            "{:?}",
            s.a.connection().inner().close_reason()
        );
        assert!(recycle_closes_to(s.b_addr).is_empty());
    })
}

/// #5940 step M6, guard 2 (review finding, fourth round): `a` has a
/// node-wide egress cap of 50 B/s, so each transmit waits about 24 s for
/// its 1200-byte admission, longer than the window, while `b` keeps
/// sending. quinn counts the frames of a transmit when it builds it, so
/// without the cap rule `a` would latch a send that has not left and close.
/// `b` is conforming: `a` must not close.
#[test]
fn a_send_held_by_the_egress_cap_does_not_close_the_path() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-ack-watch-capped";
        let mut s = Scripted::connect_capped(network, 4, Some(50)).await;
        s.keep_sending();
        s.wait(Duration::from_secs(5)).await;
        let cap_holds = s.a.connection().cap_holds();
        let (holds_before, received_before) = (cap_holds.read().holds, s.sample().received());
        s.a_sends(20_000);
        s.wait(Duration::from_secs(60)).await;
        assert!(
            cap_holds.read().holds > holds_before,
            "the cap held no transmit"
        );
        assert!(
            s.sample().received() > received_before + 1000,
            "b sent too little"
        );
        assert!(
            s.a.connection().inner().close_reason().is_none(),
            "{:?}",
            s.a.connection().inner().close_reason()
        );
        assert!(recycle_closes_to(s.b_addr).is_empty());
        // Nor did `b` send a close.
        assert_eq!(
            s.a.connection().inner().stats().frame_rx.connection_close,
            0
        );
    })
}

/// #5940 step M6, guard 2 must not fire on a slow but conforming peer: a
/// 5 s round trip (2.5 s each way, from before the handshake), with
/// messages both ways for a while and then an idle minute. Neither side
/// closes, and nothing is counted.
#[test]
fn a_slow_conforming_peer_is_not_closed() {
    bounded(|| async move {
        use crate::transport::in_memory_socket::{
            PacketDeliveryDecision, set_packet_delivery_callback,
        };
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-ack-watch-slow";
        let one_way = Duration::from_millis(2500);
        set_packet_delivery_callback(
            network,
            Some(Arc::new(move |_, _, _, _| {
                PacketDeliveryDecision::DelayedDelivery(one_way)
            })),
        );
        let p = paths(network, 45, QuicPathLimits::DEFAULT).await;
        let (a_addr, b_addr) = (p.pair.a_addr, p.pair.b_addr);
        let (a, b) = (p.a.clone(), p.b.clone());
        let time = p.time();
        p.drive(async move {
            for _ in 0..6 {
                let (to_b, to_a) = tokio::join!(
                    a.send_stream(StreamId::next(), None, Bytes::from(payload(50_000))),
                    b.send_stream(StreamId::next(), None, Bytes::from(payload(50_000))),
                );
                to_b.expect("a to b");
                to_a.expect("b to a");
            }
            time.sleep(Duration::from_secs(60)).await;
            a.send_short(b"after the idle minute")
                .await
                .expect("a open");
            b.send_short(b"after the idle minute")
                .await
                .expect("b open");
        })
        .await;
        set_packet_delivery_callback(network, None);
        assert!(p.a.connection().inner().close_reason().is_none());
        assert!(p.b.connection().inner().close_reason().is_none());
        assert!(recycle_closes_to(a_addr).is_empty());
        assert!(recycle_closes_to(b_addr).is_empty());
    })
}

/// Datagrams a [`seeded_loss`] saw and dropped.
#[derive(Default)]
struct LossCount {
    seen: std::sync::atomic::AtomicU64,
    dropped: std::sync::atomic::AtomicU64,
}

/// A seeded loss of `percent` of datagrams in both directions on `network`;
/// the counts of what it saw and dropped.
fn seeded_loss(network: &str, percent: u64) -> Arc<LossCount> {
    use crate::transport::in_memory_socket::{
        PacketDeliveryDecision, set_packet_delivery_callback,
    };
    use std::sync::atomic::Ordering::Relaxed;
    let state = Arc::new(std::sync::atomic::AtomicU64::new(0x5940_0006));
    let count = Arc::new(LossCount::default());
    let counted = count.clone();
    set_packet_delivery_callback(
        network,
        Some(Arc::new(move |_, _, _, _| {
            counted.seen.fetch_add(1, Relaxed);
            // A fixed linear congruential sequence, one draw per datagram.
            let next = state
                .fetch_update(
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                    |x| Some(x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1)),
                )
                .unwrap_or_default();
            if (next >> 33) % 100 < percent {
                counted.dropped.fetch_add(1, Relaxed);
                PacketDeliveryDecision::Drop
            } else {
                PacketDeliveryDecision::Deliver
            }
        })),
    );
    count
}

/// Sample `paths`' connections every 500 ms until `end` (virtual nanos);
/// the largest `outstanding_after_acks` seen on either.
async fn max_outstanding(paths: [Arc<QuicPath>; 2], time: VirtualTime, end: u64) -> u64 {
    let mut max = 0;
    while time.now_nanos() < end {
        for path in &paths {
            if let Some(sample) =
                recycle::AckSample::of(path.connection().inner(), &path.connection().cap_holds())
            {
                max = max.max(sample.outstanding_after_acks());
            }
        }
        time.sleep(Duration::from_millis(500)).await;
    }
    max
}

/// #5940 step M6, guard 2 must not fire on a conforming peer over a lossy
/// path: 25% of datagrams lost in both directions (seeded), a 2 MiB
/// transfer each way plus a short message each way every 500 ms, for three
/// virtual minutes.
#[test]
fn a_lossy_path_is_not_closed() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-ack-watch-lossy";
        let loss = seeded_loss(network, 25);
        let p = paths(network, 46, QuicPathLimits::DEFAULT).await;
        let (a_addr, b_addr) = (p.pair.a_addr, p.pair.b_addr);
        let (a, b, time) = (p.a.clone(), p.b.clone(), p.time());
        let (bulk_ab, bulk_ba, trickles) = p
            .drive(async move {
                let end = time.now_nanos() + Duration::from_secs(180).as_nanos() as u64;
                let trickle = |from: Arc<QuicPath>, time: VirtualTime| async move {
                    let mut sent = 0u32;
                    while time.now_nanos() < end {
                        from.send_short(b"trickle").await.expect("trickle");
                        sent += 1;
                        time.sleep(Duration::from_millis(500)).await;
                    }
                    sent
                };
                let (bulk_ab, bulk_ba, t_a, t_b, max) = tokio::join!(
                    a.send_stream(StreamId::next(), None, Bytes::from(payload(2 * MIB))),
                    b.send_stream(StreamId::next(), None, Bytes::from(payload(2 * MIB))),
                    trickle(a.clone(), time.clone()),
                    trickle(b.clone(), time.clone()),
                    max_outstanding([a.clone(), b.clone()], time.clone(), end),
                );
                (bulk_ab, bulk_ba, (t_a.min(t_b), max))
            })
            .await;
        let (trickles, max_outstanding) = trickles;
        // The loss really happened, and the watch really had packets
        // outstanding to judge, so "no close" is not vacuous.
        let (seen, dropped) = (
            loss.seen.load(std::sync::atomic::Ordering::Relaxed),
            loss.dropped.load(std::sync::atomic::Ordering::Relaxed),
        );
        assert!(seen > 2000, "{seen} datagrams");
        assert!(dropped * 100 >= seen * 20, "{dropped} of {seen} dropped");
        assert!(max_outstanding > 0, "nothing was ever outstanding");
        crate::transport::in_memory_socket::set_packet_delivery_callback(network, None);
        bulk_ab.expect("a to b");
        bulk_ba.expect("b to a");
        assert!(trickles > 50, "{trickles} trickle messages");
        assert!(p.a.connection().inner().close_reason().is_none());
        assert!(p.b.connection().inner().close_reason().is_none());
        assert!(recycle_closes_to(a_addr).is_empty());
        assert!(recycle_closes_to(b_addr).is_empty());
    })
}

/// #5940 step M6, guard 2 must not fire on a multi-megabyte transfer over a
/// 1.5 s round trip (750 ms each way, from before the handshake): 8 MiB
/// each way at once.
#[test]
fn a_bulk_transfer_at_a_long_round_trip_is_not_closed() {
    bounded(|| async move {
        use crate::transport::in_memory_socket::{
            PacketDeliveryDecision, set_packet_delivery_callback,
        };
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-ack-watch-bulk-rtt";
        let one_way = Duration::from_millis(750);
        set_packet_delivery_callback(
            network,
            Some(Arc::new(move |_, _, _, _| {
                PacketDeliveryDecision::DelayedDelivery(one_way)
            })),
        );
        let p = paths(network, 47, QuicPathLimits::DEFAULT).await;
        let (a_addr, b_addr) = (p.pair.a_addr, p.pair.b_addr);
        let (a, b, time) = (p.a.clone(), p.b.clone(), p.time());
        let (to_b, to_a, max) = p
            .drive(async move {
                let end = time.now_nanos() + Duration::from_secs(20).as_nanos() as u64;
                tokio::join!(
                    a.send_stream(StreamId::next(), None, Bytes::from(payload(8 * MIB))),
                    b.send_stream(StreamId::next(), None, Bytes::from(payload(8 * MIB))),
                    max_outstanding([a.clone(), b.clone()], time.clone(), end),
                )
            })
            .await;
        // A full round trip of data in flight: tens of kilobytes at least.
        assert!(max > 64 * 1024, "at most {max} bytes outstanding");
        set_packet_delivery_callback(network, None);
        to_b.expect("a to b");
        to_a.expect("b to a");
        assert!(p.a.connection().inner().close_reason().is_none());
        assert!(p.b.connection().inner().close_reason().is_none());
        assert!(recycle_closes_to(a_addr).is_empty());
        assert!(recycle_closes_to(b_addr).is_empty());
    })
}

/// #5940 step M6, guard 2 must not fire on an idle path: two minutes with
/// nothing to send after an exchange, then the path still carries a message.
/// Keepalive is off under `VirtualTime`, so nothing at all is sent while
/// idle here; an idle path with keepalive PINGs is covered only by the
/// unit test `recycle::tests::a_slow_conforming_peer_is_never_closed`.
#[test]
fn an_idle_path_is_not_closed() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-ack-watch-idle", 44, QuicPathLimits::DEFAULT).await;
        let (a_addr, b_addr) = (p.pair.a_addr, p.pair.b_addr);
        let (a, b) = (p.a.clone(), p.b.clone());
        let time = p.time();
        p.drive(async move {
            a.send_short(b"hello").await.expect("a to b");
            time.sleep(Duration::from_secs(120)).await;
            b.send_short(b"still here").await.expect("b to a");
        })
        .await;
        assert!(p.a.connection().inner().close_reason().is_none());
        assert!(p.b.connection().inner().close_reason().is_none());
        assert!(recycle_closes_to(a_addr).is_empty());
        assert!(recycle_closes_to(b_addr).is_empty());
    })
}
