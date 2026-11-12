//! Tests for [`QuicPath`]: two endpoints on `SimulationSocket` and one
//! `VirtualTime` (the helpers of `quic::tests`), each with the other's
//! proven transport key, and a path on each side of the connection.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use super::super::framing::{HEADER_LEN, MAX_CONTROL_BYTES, MAX_METADATA_BYTES};
use super::super::tests::{MIB, Pair, SEED, bounded, drive, pair_with, payload};
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

mod node_counts;
mod pacer;
mod route;

/// How long, in virtual time, a test's traffic may take.
const LIMIT: Duration = Duration::from_secs(600);

/// A connected pair with a path on each side: `a` is the client, `b` the
/// server; `b`'s path has `b_limits`.
struct Paths {
    pair: Pair,
    /// The subnet `a` (`.1`) and `b` (`.2`) are on, for a third endpoint.
    net: crate::transport::test_net::Subnet,
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

/// Build the pair on `.1` and `.2` of a subnet of its own
/// (`transport::test_net`) and connect it. Every pair needs its own: the
/// simulated network maps each address to one network for the whole
/// process, so two tests on one subnet send each other's packets into the
/// wrong network.
async fn paths(network: &str, b_limits: QuicPathLimits) -> Paths {
    paths_with(network, b_limits, None).await
}

/// [`paths`], with `b`'s path charging `b_budget` if given
/// ([`QuicPath::with_budget`]) instead of a budget of its own.
async fn paths_with(
    network: &str,
    b_limits: QuicPathLimits,
    b_budget: Option<SharedInboundBudget>,
) -> Paths {
    paths_built(network, b_limits, b_budget, None, Windows::default(), true).await
}

/// The buffer budgets a test's paths take their windows from, if any
/// (`budget::QuicBudget`): `b`'s, as the receiving node's, and `a`'s, as the
/// sending node's. A path with none keeps the endpoint's initial window.
#[derive(Default, Clone)]
struct Windows {
    a: Option<Arc<super::super::budget::QuicBudget>>,
    b: Option<Arc<super::super::budget::QuicBudget>>,
}

/// [`paths`] with each side's path taking its windows from a buffer budget.
async fn paths_metered(network: &str, windows: Windows) -> Paths {
    paths_built(network, QuicPathLimits::DEFAULT, None, None, windows, true).await
}

/// [`paths`] with an egress cap of `a_cap` bytes per second on `a`'s socket.
async fn paths_capped(network: &str, a_cap: u64) -> Paths {
    paths_built(
        network,
        QuicPathLimits::DEFAULT,
        None,
        Some(a_cap),
        Windows::default(),
        true,
    )
    .await
}

/// [`paths`] with plain Cubic, no loss gate (`recycle::loss_gate`), so
/// random loss collapses the window: for the path's rules on a connection
/// that drains slowly while it is still acknowledged.
async fn paths_without_loss_gate(network: &str) -> Paths {
    paths_built(
        network,
        QuicPathLimits::DEFAULT,
        None,
        None,
        Windows::default(),
        false,
    )
    .await
}

async fn paths_built(
    network: &str,
    b_limits: QuicPathLimits,
    b_budget: Option<SharedInboundBudget>,
    a_cap: Option<u64>,
    windows: Windows,
    loss_gate: bool,
) -> Paths {
    let net = crate::transport::test_net::subnet();
    let (a_addr, b_addr) = (net.addr(1), net.addr(2));
    let mut settings = super::super::tests::settings();
    settings.rate_limit.without_loss_gate = !loss_gate;
    let pair = pair_with(network, a_addr, b_addr, a_cap, settings).await;
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
    // `a` keeps its node-wide counts where `b` does.
    let a_limits = QuicPathLimits {
        counters: b_limits.counters,
        ..QuicPathLimits::DEFAULT
    };
    let own =
        |limits: QuicPathLimits| SharedInboundBudget::new(limits.max_streams, limits.max_bytes);
    let (a, a_in) = QuicPath::with_budget_and_own_channels(
        client,
        pair.time.clone(),
        a_limits,
        own(a_limits),
        windows.a.as_ref(),
    );
    a.enable_data();
    let (b, b_in) = QuicPath::with_budget_and_own_channels(
        server,
        pair.time.clone(),
        b_limits,
        b_budget.unwrap_or_else(|| own(b_limits)),
        windows.b.as_ref(),
    );
    if b_limits.accept_data {
        b.enable_data();
    }
    Paths {
        pair,
        net,
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
    Bytes::from(payload(2 * super::super::tests::TEST_WINDOW as usize + 1))
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
/// charged to the budget until their payload is taken, except those of one
/// packet or less, as on UDP.
#[test]
fn short_messages_round_trip_and_are_not_deduplicated() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-short", QuicPathLimits::DEFAULT).await;
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
        assert_eq!(
            p.b.charged().0,
            1,
            "only the 200 KiB message is charged; the others fit one packet"
        );
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
        let mut p = paths("quic-path-stream", QuicPathLimits::DEFAULT).await;
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
        let mut p = paths("quic-path-meta", QuicPathLimits::DEFAULT).await;
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
        let mut p = paths("quic-path-meta-big", QuicPathLimits::DEFAULT).await;
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
        let mut p = paths("quic-path-headers", QuicPathLimits::DEFAULT).await;
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
        let mut p = paths("quic-path-budget", limits).await;
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
        let mut p = paths("quic-path-bounds", QuicPathLimits::DEFAULT).await;
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
            // Read unordered, the payload may arrive with the header, so an
            // overlong stream can fail before it is announced; a truncated one
            // is always announced (its end comes after every byte it has).
            match p.b_in.streams.try_recv() {
                Ok(arrival) => {
                    assert!(!arrival.handle.is_complete(), "case {n}");
                    assert!(arrival.handle.received_bytes() < total, "case {n}");
                    assert_eq!(
                        arrival.handle.assemble().await,
                        Err(StreamError::Cancelled),
                        "case {n}"
                    );
                }
                Err(_) => assert_eq!(
                    outcome,
                    QuicInboundOutcome::Overlong,
                    "case {n}: only an overlong stream can fail before it is announced"
                ),
            }
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
        // Announced or not (see above), nothing completes.
        drop(p.b_in.streams.try_recv());
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
        let mut p = paths("quic-path-reset", QuicPathLimits::DEFAULT).await;
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
                a.send_short(&payload(2000)).await.expect("still connected");
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
        let mut p = paths("quic-path-evict", QuicPathLimits::DEFAULT).await;
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

/// A stream that trickles its header never goes idle but holds one of the
/// peer's stream slots while it does; it has no budget charge, so it is
/// evicted at the header phase's age limit (`inbound::HEADER_PHASE_AGE`, at
/// least the idle timeout), well before the uncharged age limit.
#[test]
fn an_uncharged_stream_is_evicted_at_its_age_limit() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-uncharged-age", QuicPathLimits::DEFAULT).await;
        let idle = streaming_handle_idle_timeout();
        let age = super::inbound::HEADER_PHASE_AGE.max(idle);
        assert!(
            age < max_inbound_stream_age(0),
            "premise: the header phase is shorter"
        );
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

/// A full control channel drops and counts the frame (nothing is waiting on a
/// control frame). A full short or stream channel makes only that message's
/// reader wait, holding the stream's credit: nothing is lost, and the message
/// arrives once the consumer takes one, its payload read meanwhile. A second
/// stream with an id the path already holds is stopped with
/// `STREAM_ID_IN_USE`.
#[test]
fn full_channels_hold_data_and_drop_control_and_reused_ids_are_refused() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let limits = QuicPathLimits {
            short_capacity: 1,
            stream_capacity: 1,
            control_capacity: 1,
            ..QuicPathLimits::DEFAULT
        };
        let mut p = paths("quic-path-full", limits).await;
        let a = p.a.clone();
        let held = StreamId::next();
        let reused = p
            .drive(async move {
                a.send_short(b"one").await.unwrap();
                a.send_short(b"two").await.unwrap();
                a.send_control(b"one").await.unwrap();
                a.send_control(b"two").await.unwrap();
                a.send_stream(held, None, Bytes::from_static(b"held"))
                    .await
                    .unwrap();
                a.send_stream(held, None, over_window()).await
            })
            .await;
        assert_stopped(reused, stop_code::STREAM_ID_IN_USE);
        // A stream behind a full channel is not stopped, and its payload keeps
        // being read into its charged buffer while it waits to be announced:
        // the sender is not held on flow control by this side's consumer, so a
        // stream larger than the flow-control window completes before the
        // consumer takes anything. (Reading stopped during the wait until the
        // unordered reader; the sender then stayed blocked.)
        let a = p.a.clone();
        let second_id = StreamId::next();
        let second =
            tokio::spawn(async move { a.send_stream(second_id, None, over_window()).await });
        let time = p.time();
        p.drive(async move { time.sleep(Duration::from_secs(1)).await })
            .await;
        assert!(
            second.is_finished(),
            "the second stream's payload was read while it waited for room"
        );
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::ShortDroppedFull), 0);
        assert_eq!(stats.count(QuicInboundOutcome::StreamDroppedFull), 0);
        assert_eq!(stats.count(QuicInboundOutcome::ControlDroppedFull), 1);
        assert_eq!(stats.count(QuicInboundOutcome::StreamIdInUse), 1);
        assert_eq!(&p.b_in.shorts.try_recv().unwrap().payload()[..], b"one");
        assert_eq!(&p.b_in.controls.try_recv().unwrap()[..], b"one");
        assert_eq!(p.b_in.streams.try_recv().unwrap().stream_id, held);
        let time = p.time();
        let second = p
            .drive(async move {
                let sent = second.await.unwrap();
                time.sleep(Duration::from_secs(1)).await;
                sent
            })
            .await;
        second.expect("sent once there was room");
        assert_eq!(&p.b_in.shorts.try_recv().unwrap().payload()[..], b"two");
        assert_eq!(p.b_in.streams.try_recv().unwrap().stream_id, second_id);
        assert_eq!(stats.count(QuicInboundOutcome::ShortDroppedFull), 0);
        assert_eq!(stats.count(QuicInboundOutcome::StreamDroppedFull), 0);
        // The second stream was received whole, its end included, while it
        // waited: the reader reads on while it waits, so quinn holds nothing
        // for it, and the waiters are bounded by the inbound budget.
    })
}

/// The receive side reads past a gap: with one of the sender's datagrams
/// lost, the bytes after it are read at once (unordered), so the stream holds
/// a gap until the retransmission fills it, and quinn keeps nothing of them
/// waiting for it. The stream still assembles byte for byte. Read in order,
/// nothing past the gap is read, and the stream never holds a gap.
#[test]
fn a_lost_datagram_does_not_stop_reading_past_it() {
    bounded(|| async move {
        use crate::transport::in_memory_socket::{
            PacketDeliveryDecision, set_packet_delivery_callback,
        };
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-unordered-gap";
        let mut p = paths(network, QuicPathLimits::DEFAULT).await;
        let from = p.pair.a_addr;
        let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = seen.clone();
        set_packet_delivery_callback(
            network,
            Some(Arc::new(move |_, sender, _, _| {
                // The fourth datagram from `a` after this point is lost once.
                if sender == from && counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 3
                {
                    PacketDeliveryDecision::Drop
                } else {
                    PacketDeliveryDecision::Deliver
                }
            })),
        );
        let len = 32 * 1024;
        let body = Bytes::from(payload(len));
        let (a, id) = (p.a.clone(), StreamId::next());
        let sent = body.clone();
        p.drive(async move { a.send_stream(id, None, sent).await.expect("sent") })
            .await;
        set_packet_delivery_callback(network, None);
        let arrival = p.b_in.streams.try_recv().expect("announced");
        assert_eq!(
            arrival.handle.assemble().await.expect("assembled"),
            body.to_vec()
        );
        assert!(
            seen.load(std::sync::atomic::Ordering::Relaxed) > 3,
            "premise: the datagram was dropped"
        );
        assert!(
            p.b.inbound_gaps_peak() >= 1,
            "bytes past the gap were read before it filled: peak {}",
            p.b.inbound_gaps_peak()
        );
        assert_eq!(p.b.inbound_gaps_kept(), 0, "the stream finished");
    })
}

/// Several streams start together and the first datagrams, which carry
/// their headers, are lost: the bytes after each header arrive about a round
/// trip before it does. They are read and staged (`inbound::Staging`) under
/// the connection's staging cap, and every stream is still read, announced and
/// assembled byte for byte, and none is stopped: honest loss of a header stays
/// well under the cap.
#[test]
fn streams_whose_first_datagrams_are_lost_all_arrive() {
    bounded(|| async move {
        use crate::transport::in_memory_socket::{
            PacketDeliveryDecision, set_packet_delivery_callback,
        };
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-first-loss";
        let mut p = paths(network, QuicPathLimits::DEFAULT).await;
        let from = p.pair.a_addr;
        let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = seen.clone();
        set_packet_delivery_callback(
            network,
            Some(Arc::new(move |_, sender, _, _| {
                // The first four datagrams from `a` after this point are lost
                // once.
                if sender == from && counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed) < 4
                {
                    PacketDeliveryDecision::Drop
                } else {
                    PacketDeliveryDecision::Deliver
                }
            })),
        );
        const STREAMS: usize = 4;
        let len = 64 * 1024;
        let bodies: Vec<Bytes> = (0..STREAMS)
            .map(|i| {
                let mut body = payload(len);
                body[0] = i as u8;
                Bytes::from(body)
            })
            .collect();
        let ids: Vec<StreamId> = (0..STREAMS).map(|_| StreamId::next()).collect();
        let a = p.a.clone();
        let sends: Vec<_> = ids.iter().copied().zip(bodies.iter().cloned()).collect();
        p.drive(async move {
            let results = futures::future::join_all(sends.into_iter().map(|(id, body)| {
                let a = a.clone();
                async move { a.send_stream(id, None, body).await }
            }))
            .await;
            for result in results {
                result.expect("sent");
            }
        })
        .await;
        set_packet_delivery_callback(network, None);
        assert!(
            seen.load(std::sync::atomic::Ordering::Relaxed) > 4,
            "premise: the datagrams were dropped"
        );
        let mut arrived = std::collections::HashMap::new();
        for _ in 0..STREAMS {
            let arrival = p.b_in.streams.try_recv().expect("announced");
            arrived.insert(arrival.stream_id, arrival);
        }
        for (id, body) in ids.iter().zip(&bodies) {
            let arrival = arrived.remove(id).expect("this stream was announced");
            assert_eq!(
                arrival.handle.assemble().await.expect("assembled"),
                body.to_vec()
            );
        }
        let stats = p.b.stats();
        assert_eq!(
            stats.count(QuicInboundOutcome::CompletedStream),
            STREAMS as u64
        );
        assert_eq!(stats.count(QuicInboundOutcome::TooManyRanges), 0);
        assert_eq!(
            stats.count(QuicInboundOutcome::EvictedStaged),
            0,
            "honest header loss stays under the staging cap"
        );
        assert_eq!(p.b.inbound_staged(), 0, "nothing is left staged");
    })
}

/// A stream stopped with gaps leaves them counted, since we cannot observe
/// when the peer ends it. An honest peer ends a stopped stream well within
/// the grace period (`KEPT_GAPS_GRACE`), after which the gaps of a stream
/// with only a few leave the count, and an honest connection's count does not drift. Here
/// the sender's datagrams stop arriving after some were lost, so the stream
/// is evicted idle with gaps.
#[test]
fn a_stream_stopped_with_gaps_is_released_after_the_grace_period() {
    bounded(|| async move {
        use crate::transport::in_memory_socket::{
            PacketDeliveryDecision, set_packet_delivery_callback,
        };
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-stopped-gaps";
        let mut p = paths(network, QuicPathLimits::DEFAULT).await;
        // Let the paths' stream-limit raises (`config::INITIAL_UNI_STREAMS`)
        // and their acknowledgements settle, so the first datagram `a` sends
        // below is the stream's.
        let settle = p.time();
        p.drive(async move { settle.sleep(Duration::from_millis(200)).await })
            .await;
        let from = p.pair.a_addr;
        let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = seen.clone();
        set_packet_delivery_callback(
            network,
            Some(Arc::new(move |_, sender, _, _| {
                if sender != from {
                    return PacketDeliveryDecision::Deliver;
                }
                // Every other one of the first 16 datagrams from `a` arrives;
                // none after.
                let n = counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if n < 16 && n % 2 == 0 {
                    PacketDeliveryDecision::Deliver
                } else {
                    PacketDeliveryDecision::Drop
                }
            })),
        );
        let (a, id) = (p.a.clone(), StreamId::next());
        let sender = tokio::spawn(async move { a.send_stream(id, None, over_window()).await });
        let time = p.time();
        let idle = streaming_handle_idle_timeout();
        p.drive(async move { time.sleep(idle + Duration::from_secs(1)).await })
            .await;
        set_packet_delivery_callback(network, None);
        let stats = p.b.stats();
        assert!(
            seen.load(std::sync::atomic::Ordering::Relaxed) > 16,
            "premise: datagrams were dropped"
        );
        assert_eq!(
            stats.shared_count(InboundStreamOutcome::EvictedIdle),
            1,
            "premise: the stream was stopped"
        );
        let arrival = p.b_in.streams.try_recv().expect("announced");
        assert!(arrival.handle.is_cancelled());
        let kept = p.b.inbound_gaps_kept();
        assert!(kept >= 1, "the stopped stream's gaps stay counted");
        assert!(
            kept <= 32,
            "premise: few enough gaps to be released: {kept}"
        );
        // Past the grace period, the next read on the connection releases them.
        let (a, time) = (p.a.clone(), p.time());
        p.drive(async move {
            assert_stopped(sender.await.unwrap(), stop_code::EVICTED);
            time.sleep(Duration::from_secs(30)).await;
            a.send_stream(StreamId::next(), None, Bytes::from_static(b"later"))
                .await
                .expect("sent");
        })
        .await;
        assert_eq!(
            p.b.inbound_gaps_kept(),
            0,
            "released after the grace period"
        );
        assert_eq!(p.b.inbound_gaps_held(), 0);
    })
}

/// Past the gap cap, the stream that went over it is stopped; its gaps stay
/// counted, so the next time past the cap closes the path with OVER_BUDGET and
/// the peer falls back to UDP. Here the cap is 2 and every other datagram is
/// lost, so each stream leaves gaps.
#[test]
fn a_second_trip_past_the_gap_cap_closes_the_path() {
    bounded(|| async move {
        use crate::transport::in_memory_socket::{
            PacketDeliveryDecision, set_packet_delivery_callback,
        };
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-gap-trips";
        let limits = QuicPathLimits {
            max_gaps: 2,
            ..QuicPathLimits::DEFAULT
        };
        let p = paths(network, limits).await;
        let from = p.pair.a_addr;
        let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = seen.clone();
        set_packet_delivery_callback(
            network,
            Some(Arc::new(move |_, sender, _, _| {
                if sender == from
                    && counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % 2 == 1
                {
                    PacketDeliveryDecision::Drop
                } else {
                    PacketDeliveryDecision::Deliver
                }
            })),
        );
        let a = p.a.clone();
        let (first, second) = p
            .drive(async move {
                let first = a.send_stream(StreamId::next(), None, over_window()).await;
                let second = a.send_stream(StreamId::next(), None, over_window()).await;
                (first, second)
            })
            .await;
        set_packet_delivery_callback(network, None);
        assert_stopped(first, stop_code::EVICTED);
        match second {
            Err(QuicSendError::ConnectionLost(quinn::ConnectionError::ApplicationClosed(
                close,
            ))) => {
                assert_eq!(
                    close.error_code.into_inner(),
                    u64::from(super::super::error::close_code::OVER_BUDGET),
                    "closed with OVER_BUDGET"
                );
            }
            other => panic!("expected the path closed with OVER_BUDGET, got {other:?}"),
        }
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::TooManyRanges), 2);
        assert_eq!(stats.count(QuicInboundOutcome::ClosedTooManyRanges), 1);
        assert_eq!(
            crate::transport::metrics::TRANSPORT_METRICS.quic_close_sent_to(from),
            vec![QuicCloseCode::OverBudget],
            "counted as quic_close_sent_over_budget"
        );
        assert!(
            p.b.inbound_gaps_kept() >= 2,
            "the first stream's gaps stayed"
        );
    })
}

/// A stream larger than the flow-control window, through random loss, is
/// delivered byte for byte, never trips the gap cap, and its gaps stay far
/// under it: honest loss leaves about loss x packets in flight.
#[test]
fn a_stream_through_random_loss_arrives_intact_far_under_the_gap_cap() {
    bounded(|| async move {
        use crate::transport::in_memory_socket::{
            PacketDeliveryDecision, set_packet_delivery_callback,
        };
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-random-loss";
        let mut p = paths(network, QuicPathLimits::DEFAULT).await;
        let lost = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = lost.clone();
        let sent = Arc::new(std::sync::atomic::AtomicU64::new(0));
        set_packet_delivery_callback(
            network,
            Some(Arc::new(move |_, _, _, _| {
                // About 3% loss, both ways, by a fixed hash of the datagram's
                // number, so the run is the same every time.
                let n = sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if (n.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) % 100 < 3 {
                    counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    PacketDeliveryDecision::Drop
                } else {
                    PacketDeliveryDecision::Deliver
                }
            })),
        );
        let body = over_window();
        let (a, id, sent) = (p.a.clone(), StreamId::next(), body.clone());
        p.drive(async move { a.send_stream(id, None, sent).await.expect("sent") })
            .await;
        set_packet_delivery_callback(network, None);
        assert!(
            lost.load(std::sync::atomic::Ordering::Relaxed) > 10,
            "premise: datagrams were lost"
        );
        let arrival = p.b_in.streams.try_recv().expect("announced");
        assert_eq!(
            arrival.handle.assemble().await.expect("assembled"),
            body.to_vec()
        );
        assert_eq!(p.b.stats().count(QuicInboundOutcome::TooManyRanges), 0);
        let peak = p.b.inbound_gaps_peak();
        assert!(peak >= 1, "premise: bytes past a gap were read");
        // Honest loss leaves about loss x packets in flight: with the loss
        // gate Cubic keeps its window, so up to a window of packets is in
        // flight (TEST_WINDOW / 1200, about 1750), and 3% of that is about
        // 52. A quarter of the cap is above that and far under the cap.
        let expected = 3 * (super::super::tests::TEST_WINDOW as usize / 1200) / 100;
        assert!(
            expected < inbound::MAX_GAPS_PER_CONNECTION / 4,
            "premise: the bound is above the expected {expected}"
        );
        assert!(
            peak < inbound::MAX_GAPS_PER_CONNECTION / 4,
            "gap peak {peak} against a cap of {}",
            inbound::MAX_GAPS_PER_CONNECTION
        );
        assert_eq!(
            p.b.inbound_gaps_held(),
            0,
            "the finished stream released its gaps"
        );
    })
}

/// A stream the sender resets while it has gaps releases them at once:
/// quinn freed its state on the reset, so they are not kept. The receiver's
/// acknowledgements are delayed so the sender cannot retransmit the lost
/// datagrams before it resets. A stream the sender finishes short releases
/// its gaps too (quinn reports the end only once every byte is in, so it has
/// none left by then).
#[test]
fn reset_and_short_streams_release_their_gaps_at_once() {
    bounded(|| async move {
        use crate::transport::in_memory_socket::{
            PacketDeliveryDecision, set_packet_delivery_callback,
        };
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-reset-gaps";
        let p = paths(network, QuicPathLimits::DEFAULT).await;
        let from = p.pair.a_addr;
        let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = seen.clone();
        set_packet_delivery_callback(
            network,
            Some(Arc::new(move |_, sender, _, _| {
                if sender != from {
                    return PacketDeliveryDecision::DelayedDelivery(Duration::from_millis(500));
                }
                // Datagrams 2 to 5 from `a` after this point are lost.
                let n = counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if (2..6).contains(&n) {
                    PacketDeliveryDecision::Drop
                } else {
                    PacketDeliveryDecision::Deliver
                }
            })),
        );
        let (conn, time) = (p.a_raw(), p.time());
        let total = 1_000_000u64;
        p.drive(async move {
            let mut reset = conn.open_uni().await.unwrap();
            reset
                .write_all(&header(1, 1, StreamId::next().0, total, 0))
                .await
                .unwrap();
            reset.write_all(&payload(8 * 1024)).await.unwrap();
            time.sleep(Duration::from_millis(100)).await;
            reset.reset(0u32.into()).unwrap();
            time.sleep(Duration::from_millis(100)).await;
        })
        .await;
        let stats = p.b.stats();
        assert!(
            seen.load(std::sync::atomic::Ordering::Relaxed) > 6,
            "premise: datagrams were dropped"
        );
        assert!(
            p.b.inbound_gaps_peak() >= 1,
            "premise: the stream held a gap"
        );
        assert_eq!(stats.count(QuicInboundOutcome::ResetByPeer), 1);
        assert_eq!(
            p.b.inbound_gaps_kept(),
            0,
            "a reset stream's gaps are not kept"
        );
        assert_eq!(p.b.inbound_gaps_held(), 0);
        set_packet_delivery_callback(network, None);
        let (conn, time) = (p.a_raw(), p.time());
        p.drive(async move {
            let mut short = conn.open_uni().await.unwrap();
            short
                .write_all(&header(1, 1, StreamId::next().0, total, 0))
                .await
                .unwrap();
            short.write_all(&payload(64 * 1024)).await.unwrap();
            short.finish().unwrap();
            time.sleep(Duration::from_secs(1)).await;
        })
        .await;
        assert_eq!(stats.count(QuicInboundOutcome::Truncated), 1);
        assert_eq!(p.b.inbound_gaps_kept(), 0);
        assert_eq!(p.b.inbound_gaps_held(), 0);
    })
}

/// The datagram carrying a stream's header and metadata arrives late rather
/// than not at all (reordered): the stream is still read, announced with its
/// metadata, and assembled byte for byte.
#[test]
fn a_delayed_header_and_metadata_still_arrive() {
    bounded(|| async move {
        use crate::transport::in_memory_socket::{
            PacketDeliveryDecision, set_packet_delivery_callback,
        };
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-delayed-header";
        let mut p = paths(network, QuicPathLimits::DEFAULT).await;
        let from = p.pair.a_addr;
        let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = seen.clone();
        set_packet_delivery_callback(
            network,
            Some(Arc::new(move |_, sender, _, _| {
                // The first datagram from `a` after this point arrives 20 ms
                // late, behind the ones after it.
                if sender == from && counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0
                {
                    PacketDeliveryDecision::DelayedDelivery(Duration::from_millis(20))
                } else {
                    PacketDeliveryDecision::Deliver
                }
            })),
        );
        let body = Bytes::from(payload(48 * 1024));
        let metadata = vec![7u8; 300];
        let (a, id, sent, meta) = (
            p.a.clone(),
            StreamId::next(),
            body.clone(),
            metadata.clone(),
        );
        p.drive(async move { a.send_stream(id, Some(&meta), sent).await.expect("sent") })
            .await;
        set_packet_delivery_callback(network, None);
        assert!(
            seen.load(std::sync::atomic::Ordering::Relaxed) > 1,
            "premise: the datagram was delayed"
        );
        let arrival = p.b_in.streams.try_recv().expect("announced");
        assert_eq!(arrival.metadata.as_deref(), Some(&metadata[..]));
        assert_eq!(
            arrival.handle.assemble().await.expect("assembled"),
            body.to_vec()
        );
        assert_eq!(p.b.stats().count(QuicInboundOutcome::EvictedStaged), 0);
        assert_eq!(p.b.inbound_staged(), 0, "nothing is left staged");
    })
}

/// A stream that ends short while it waits for room fails as Truncated at
/// once: once quinn reports the end it has freed the stream's slot, so the
/// reader must not stay waiting (the slots bound the waiters).
#[test]
fn a_short_stream_waiting_for_room_fails_at_once() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let limits = QuicPathLimits {
            stream_capacity: 1,
            ..QuicPathLimits::DEFAULT
        };
        let mut p = paths("quic-path-wait-short", limits).await;
        let a = p.a.clone();
        let held = StreamId::next();
        p.drive(async move {
            a.send_stream(held, None, Bytes::from_static(b"held"))
                .await
                .unwrap();
        })
        .await;
        let (conn, time) = (p.a_raw(), p.time());
        p.drive(async move {
            let mut send = conn.open_uni().await.unwrap();
            let mut bytes = header(1, 1, StreamId::next().0, 10_000, 0);
            bytes.extend_from_slice(&payload(5000));
            send.write_all(&bytes).await.unwrap();
            send.finish().unwrap();
            time.sleep(Duration::from_secs(1)).await;
        })
        .await;
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::Truncated), 1);
        assert_eq!(p.b_in.streams.try_recv().unwrap().stream_id, held);
        assert!(
            p.b_in.streams.try_recv().is_err(),
            "the short stream was not announced"
        );
    })
}

/// A copy of the bytes that end the metadata is buffered before the header
/// arrives, and the header then arrives in a resend that also carries them.
/// The sender writes the stream in two pieces, so its first datagram carries
/// the header and part of the metadata, and its second the rest. The second
/// arrives late, after the sender has declared both lost; until a set
/// instant every datagram carrying the header is lost (the first, and the
/// resends, which carry both pieces). Then quinn holds the second piece,
/// which starts inside the metadata, and a resend holding all of it: the
/// reads of the header and metadata take the resend, and the second piece is
/// a second copy of bytes already read. Every stream is read unordered from
/// its first byte, and an unordered read hands out each byte once, so the
/// copy is never read again and the stream arrives intact.
#[test]
fn a_second_copy_of_the_metadata_end_is_read_once() {
    bounded(|| async move {
        use crate::transport::in_memory_socket::{
            PacketDeliveryDecision, set_packet_delivery_callback,
        };
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-dup-boundary";
        let mut p = paths(network, QuicPathLimits::DEFAULT).await;
        let from = p.pair.a_addr;
        let time = p.time();
        let release = time.now_nanos() + nanos_of(Duration::from_millis(1500));
        let first_len = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let delayed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (len_seen, late, counted, clock) = (
            first_len.clone(),
            delayed.clone(),
            dropped.clone(),
            time.clone(),
        );
        set_packet_delivery_callback(
            network,
            Some(Arc::new(move |_, sender, _, data| {
                use std::sync::atomic::Ordering::Relaxed;
                if sender != from || clock.now_nanos() >= release {
                    return PacketDeliveryDecision::Deliver;
                }
                if len_seen.load(Relaxed) == 0 {
                    len_seen.store(data.len(), Relaxed);
                }
                // The first datagram, and resends of both pieces together.
                if data.len() == len_seen.load(Relaxed) || data.len() > 1000 {
                    counted.fetch_add(1, Relaxed);
                    return PacketDeliveryDecision::Drop;
                }
                // The second piece (pings and acks are small) arrives late.
                if data.len() > 200 && late.fetch_add(1, Relaxed) == 0 {
                    return PacketDeliveryDecision::DelayedDelivery(Duration::from_secs(1));
                }
                PacketDeliveryDecision::Deliver
            })),
        );
        let metadata = vec![9u8; 1000];
        let body = payload(100);
        let mut bytes = header(1, 1, StreamId::next().0, body.len() as u64, 1000);
        bytes.extend_from_slice(&metadata);
        bytes.extend_from_slice(&body);
        let (conn, sleeper) = (p.a_raw(), time.clone());
        p.drive(async move {
            let mut send = conn.open_uni().await.unwrap();
            send.write_all(&bytes[..518]).await.unwrap();
            sleeper.sleep(Duration::from_millis(50)).await;
            send.write_all(&bytes[518..]).await.unwrap();
            send.finish().unwrap();
            sleeper.sleep(Duration::from_secs(5)).await;
        })
        .await;
        set_packet_delivery_callback(network, None);
        assert!(
            dropped.load(std::sync::atomic::Ordering::Relaxed) > 1,
            "premise: the first datagram and its resends were lost"
        );
        assert!(
            delayed.load(std::sync::atomic::Ordering::Relaxed) >= 1,
            "premise: the second piece arrived late"
        );
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::ReadFailed), 0);
        let arrival = p.b_in.streams.try_recv().expect("announced");
        assert_eq!(arrival.metadata.as_deref(), Some(&metadata[..]));
        assert_eq!(arrival.handle.assemble().await.expect("assembled"), body);
    })
}

fn nanos_of(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("fits")
}

/// A message held for room in a full channel is stopped at its age limit, so
/// the wait is bounded and the charge is released.
#[test]
fn a_message_waiting_for_room_is_evicted_at_its_age_limit() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let limits = QuicPathLimits {
            short_capacity: 1,
            ..QuicPathLimits::DEFAULT
        };
        let mut p = paths("quic-path-full-age", limits).await;
        let a = p.a.clone();
        p.drive(async move {
            a.send_short(b"one").await.unwrap();
            a.send_short(b"two").await.unwrap();
        })
        .await;
        let time = p.time();
        let age = max_inbound_stream_age(0);
        p.drive(async move { time.sleep(age + Duration::from_secs(1)).await })
            .await;
        assert_eq!(&p.b_in.shorts.try_recv().unwrap().payload()[..], b"one");
        let time = p.time();
        p.drive(async move { time.sleep(Duration::from_secs(1)).await })
            .await;
        assert!(p.b_in.shorts.try_recv().is_err(), "the second was evicted");
        assert_eq!(p.b.stats().count(QuicInboundOutcome::ShortDroppedFull), 0);
    })
}

/// A data path's peer starts with `config::DATA_START_UNI_STREAMS` streams.
/// Streamed sends that keep every stream busy at once, with the consumer
/// draining, double it after as many complete, and again at 32, up to
/// `MAX_UNI_STREAMS`; every message arrives.
#[test]
fn the_stream_limit_starts_low_and_doubles_with_load() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-stream-raise", QuicPathLimits::DEFAULT).await;
        let count = 160usize;
        let mut streams = std::mem::replace(&mut p.b_in.streams, tokio::sync::mpsc::channel(1).1);
        let a = p.a.clone();
        let assembled = p
            .drive(async move {
                let drain = async move {
                    let mut assembled = 0usize;
                    while assembled < count {
                        let arrival = streams.recv().await.expect("announced");
                        tokio::spawn(async move { arrival.handle.assemble().await });
                        assembled += 1;
                    }
                    assembled
                };
                let sends = futures::future::join_all((0..count).map(|_| {
                    let a = a.clone();
                    async move {
                        a.send_stream(StreamId::next(), None, Bytes::from(payload(6000)))
                            .await
                    }
                }));
                let (sent, assembled) = tokio::join!(sends, drain);
                assert!(sent.iter().all(Result::is_ok), "every send is acked");
                assembled
            })
            .await;
        assert_eq!(assembled, count);
        let stats = p.b.stats();
        assert_eq!(
            stats.count(QuicInboundOutcome::CompletedStream) as usize,
            count
        );
        assert_eq!(
            stats.count(QuicInboundOutcome::StreamLimitRaised),
            2,
            "16 -> 32 -> 64"
        );
    })
}

/// Shorts that wait for room behind a slow consumer here do not earn their
/// peer a raise: a Short read whole is not busy, whatever this node does
/// with it. With nothing draining, the sends that complete are those the
/// channel holds plus at most the start limit waiting, and the limit stays.
#[test]
fn a_slow_consumer_earns_its_peer_no_raise() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let start = super::super::config::DATA_START_UNI_STREAMS as usize;
        let limits = QuicPathLimits {
            short_capacity: 1,
            ..QuicPathLimits::DEFAULT
        };
        let mut p = paths("quic-path-slow-no-raise", limits).await;
        for round in 0..3u32 {
            let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let count = 3 * start;
            for i in 0..count {
                let (a, done) = (p.a.clone(), done.clone());
                tokio::spawn(async move {
                    a.send_short(&(round * 1000 + i as u32).to_be_bytes())
                        .await
                        .unwrap();
                    done.fetch_add(1, Ordering::SeqCst);
                });
            }
            let time = p.time();
            p.drive(async move { time.sleep(Duration::from_secs(2)).await })
                .await;
            let held = done.load(Ordering::SeqCst);
            assert!(held <= 1 + start, "{held} completed at the start limit");
            let mut got = 0;
            while got < count {
                while p.b_in.shorts.try_recv().is_ok() {
                    got += 1;
                }
                let time = p.time();
                p.drive(async move { time.sleep(Duration::from_millis(10)).await })
                    .await;
            }
        }
        assert_eq!(p.b.stats().count(QuicInboundOutcome::StreamLimitRaised), 0);
    })
}

/// Waiters for room in a full channel are capped by the stream limit. A
/// waiting reader reads on and may read its stream's end, which frees quinn's
/// slot, so each waiting one-packet Short (uncharged: its sender may already
/// read it as `Sent`, so it must not be refused) lowers the peer's stream
/// limit by one while it waits (`inbound::StreamLimit`). With nothing
/// draining, the sends that complete are those the channel holds plus at most
/// `MAX_UNI_STREAMS` waiting; the rest block in `open_uni`, none is refused,
/// and the memory held does not grow with the peer's upload. Once the
/// consumer drains, all of them arrive.
#[test]
fn waiters_for_a_full_channel_are_capped_by_the_stream_limit() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-waiters", QuicPathLimits::DEFAULT).await;
        let limit = super::super::config::MAX_UNI_STREAMS as usize;
        let count = SHORT_CHANNEL_CAPACITY + limit + 40;
        let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for i in 0..count {
            let (a, done) = (p.a.clone(), done.clone());
            tokio::spawn(async move {
                a.send_short(&(i as u32).to_be_bytes()).await.unwrap();
                done.fetch_add(1, Ordering::SeqCst);
            });
        }
        let time = p.time();
        p.drive(async move { time.sleep(Duration::from_secs(5)).await })
            .await;
        let sent = done.load(Ordering::SeqCst);
        assert!(
            sent <= SHORT_CHANNEL_CAPACITY + limit,
            "{sent} sends completed with nothing draining"
        );
        assert!(sent >= SHORT_CHANNEL_CAPACITY, "only {sent} completed");
        let stats = p.b.stats();
        assert_eq!(p.b_in.shorts.len(), SHORT_CHANNEL_CAPACITY);
        assert_eq!(p.b.inbound_budget_streams(), 0, "no waiter is charged");
        assert_eq!(stats.count(QuicInboundOutcome::ShortRefusedBudget), 0);
        let mut got = std::collections::BTreeSet::new();
        for _ in 0..200 {
            while let Ok(message) = p.b_in.shorts.try_recv() {
                let bytes: [u8; 4] = message.payload()[..].try_into().unwrap();
                got.insert(u32::from_be_bytes(bytes));
            }
            if got.len() == count {
                break;
            }
            let time = p.time();
            p.drive(async move { time.sleep(Duration::from_millis(10)).await })
                .await;
        }
        assert_eq!(got.len(), count, "every message arrived once drained");
        assert_eq!(stats.count(QuicInboundOutcome::ShortDroppedFull), 0);
    })
}

/// While a stream waits for room, idleness is judged only while its payload
/// is still missing: a stream whose sender stalls halfway is evicted idle
/// during the wait (the peer is the one being waited on), while a stream
/// that has all its payload waits past the idle timeout and is announced once
/// there is room (the consumer is the one being waited on).
#[test]
fn a_waiting_stream_is_evicted_idle_only_while_its_payload_is_missing() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let limits = QuicPathLimits {
            stream_capacity: 1,
            ..QuicPathLimits::DEFAULT
        };
        let mut p = paths("quic-path-wait-idle", limits).await;
        let idle = streaming_handle_idle_timeout();
        let total = 10_000u64;
        assert!(
            max_inbound_stream_age(total) > idle * 5,
            "the wait must fit inside the age limit"
        );
        let a = p.a.clone();
        let held = StreamId::next();
        p.drive(async move {
            a.send_stream(held, None, Bytes::from_static(b"held"))
                .await
                .unwrap();
        })
        .await;
        // Behind the full channel: one stream half sent and never finished,
        // one sent whole.
        let conn = p.a_raw();
        let started = p.time().now_nanos();
        let stalled = tokio::spawn(async move {
            let mut send = conn.open_uni().await.unwrap();
            let mut bytes = header(1, 1, StreamId::next().0, total, 0);
            bytes.extend_from_slice(&payload(5000));
            send.write_all(&bytes).await.unwrap();
            send.stopped().await.unwrap().map(|c| c.into_inner())
        });
        let (a, whole) = (p.a.clone(), StreamId::next());
        let complete = tokio::spawn(async move {
            a.send_stream(whole, None, Bytes::from(payload(total as usize)))
                .await
        });
        let time = p.time();
        let code = p.drive(async move { stalled.await.unwrap() }).await;
        assert_eq!(code, Some(u64::from(stop_code::EVICTED)));
        let after = Duration::from_nanos(time.now_nanos() - started);
        assert!(
            after >= idle && after < idle * 2,
            "evicted {after:?} into the wait, idle timeout {idle:?}"
        );
        let stats = p.b.stats();
        assert_eq!(stats.shared_count(InboundStreamOutcome::EvictedIdle), 1);
        // The whole stream is still waiting, past the idle timeout.
        let sleeper = time.clone();
        p.drive(async move { sleeper.sleep(idle * 2).await }).await;
        assert_eq!(stats.shared_count(InboundStreamOutcome::EvictedIdle), 1);
        assert_eq!(p.b_in.streams.try_recv().unwrap().stream_id, held);
        let sleeper = time.clone();
        p.drive(async move {
            complete.await.unwrap().expect("sent");
            sleeper.sleep(Duration::from_secs(1)).await;
        })
        .await;
        let arrival = p
            .b_in
            .streams
            .try_recv()
            .expect("announced once there was room");
        assert_eq!(arrival.stream_id, whole);
        assert_eq!(
            arrival.handle.assemble().await.expect("assembled"),
            payload(total as usize)
        );
        assert_eq!(stats.shared_count(InboundStreamOutcome::EvictedAge), 0);
    })
}

/// A reader still waiting for room when its path ends is dropped with the
/// receive task; each is counted, per kind.
#[test]
fn readers_waiting_for_room_when_the_path_ends_are_counted() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let limits = QuicPathLimits {
            short_capacity: 1,
            stream_capacity: 1,
            ..QuicPathLimits::DEFAULT
        };
        let p = paths("quic-path-abandoned", limits).await;
        let a = p.a.clone();
        let second_id = StreamId::next();
        let sent = tokio::spawn(async move {
            a.send_short(b"one").await.unwrap();
            a.send_short(b"two").await.unwrap();
            a.send_stream(StreamId::next(), None, Bytes::from_static(b"held"))
                .await
                .unwrap();
            a.send_stream(second_id, None, over_window()).await
        });
        let time = p.time();
        let sleeper = time.clone();
        p.drive(async move { sleeper.sleep(Duration::from_secs(1)).await })
            .await;
        let stats = p.b.stats_handle();
        assert_eq!(stats.count(QuicInboundOutcome::ShortAbandonedWaiting), 0);
        assert_eq!(stats.count(QuicInboundOutcome::StreamAbandonedWaiting), 0);
        let Paths { pair, b, .. } = p;
        drop(b);
        drive(&pair.time, LIMIT, async move {
            time.sleep(Duration::from_secs(1)).await
        })
        .await;
        sent.abort();
        assert_eq!(stats.count(QuicInboundOutcome::ShortAbandonedWaiting), 1);
        assert_eq!(stats.count(QuicInboundOutcome::StreamAbandonedWaiting), 1);
    })
}

/// A full budget still delivers a short message of one packet or less: it is
/// not charged, so no refusal can race its acknowledgement.
#[test]
fn a_full_budget_still_delivers_a_one_packet_short() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let limits = QuicPathLimits {
            max_streams: 1,
            max_bytes: ChargeOverhead::DEFAULT.cost(10_000),
            ..QuicPathLimits::DEFAULT
        };
        let mut p = paths("quic-path-full-budget", limits).await;
        let a = p.a.clone();
        p.drive(async move {
            a.send_stream(StreamId::next(), None, Bytes::from_static(b"held"))
                .await
                .expect("fits");
            a.send_short(b"small").await.expect("sent");
        })
        .await;
        assert_eq!(p.b.charged().0, 1, "the held stream fills the budget");
        assert_eq!(&p.b_in.shorts.try_recv().unwrap().payload()[..], b"small");
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::DeliveredShort), 1);
        assert_eq!(
            stats.shared_count(InboundStreamOutcome::RefusedConnectionStreams),
            0
        );
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
        let p = paths("quic-path-many", QuicPathLimits::FULL_STREAMS).await;
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

/// A flood into a full channel loses nothing: the readers that find it full
/// wait, and every message arrives as the consumer drains. Sends read as
/// success once acknowledged, which they now are for what is delivered later.
#[test]
fn a_flood_into_a_slow_receiver_is_held_and_all_delivered() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-flood", QuicPathLimits::FULL_STREAMS).await;
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
        assert!(results.iter().all(Result::is_ok), "every send is acked");
        let mut got = std::collections::BTreeSet::new();
        for _ in 0..100 {
            while let Ok(message) = p.b_in.shorts.try_recv() {
                let bytes: [u8; 4] = message.payload()[..].try_into().unwrap();
                got.insert(u32::from_be_bytes(bytes));
            }
            if got.len() == count {
                break;
            }
            let time = p.time();
            p.drive(async move { time.sleep(Duration::from_millis(10)).await })
                .await;
        }
        assert_eq!(got.len(), count, "every message arrived");
        let stats = p.b.stats();
        assert_eq!(
            stats.count(QuicInboundOutcome::DeliveredShort) as usize,
            count
        );
        assert_eq!(stats.count(QuicInboundOutcome::ShortDroppedFull), 0);
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
        let mut p = paths("quic-path-reuse", QuicPathLimits::DEFAULT).await;
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
        p.drive(async move { a.send_short(&payload(2000)).await.unwrap() })
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
        let p = paths("quic-path-stall", limits).await;
        let a = p.a.clone();
        let result = p
            .drive(async move {
                a.send_stream_chunks(
                    StreamId::next(),
                    1_000_000,
                    None,
                    stalls_after_first_chunk(),
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
        let mut p = paths("quic-path-cancel", QuicPathLimits::DEFAULT).await;
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
        let mut p = paths("quic-path-copy", QuicPathLimits::DEFAULT).await;
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
        let mut p = paths("quic-path-frag-mem", QuicPathLimits::DEFAULT).await;
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
        let mut p = paths("quic-path-drained", QuicPathLimits::DEFAULT).await;
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
        let p = paths("quic-path-idle-header", QuicPathLimits::DEFAULT).await;
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
        let mut p = paths("quic-path-local-cancel", QuicPathLimits::DEFAULT).await;
        let total = 20 * FRAGMENT_PAYLOAD_SIZE;
        let (conn, a) = (p.a_raw(), p.a.clone());
        let id = StreamId::next().0;
        let mut send = p
            .drive(async move {
                let mut send = conn.open_uni().await.unwrap();
                let mut bytes = header(1, 1, id, total as u64, 0);
                bytes.extend_from_slice(&payload(10));
                send.write_all(&bytes).await.unwrap();
                a.send_short(&payload(2000)).await.unwrap();
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
        let p = paths("quic-path-panic", limits).await;
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
        let mut p = paths("quic-path-drop", QuicPathLimits::DEFAULT).await;
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
        p.drive(async move { a.send_short(&payload(2000)).await.unwrap() })
            .await;
        let arrival = p.b_in.streams.try_recv().expect("announced");
        assert!(!arrival.handle.is_cancelled());
        // A fork does not share the handle's cancel; it must still end.
        let fork = arrival.handle.fork();
        let forked = tokio::spawn(async move { fork.assemble().await });
        let b =
            Arc::try_unwrap(std::mem::replace(&mut p.b, p.a.clone())).expect("the only reference");
        drop(b);
        // The receive task is aborted on its next poll.
        p.drive(async { tokio::task::yield_now().await }).await;
        assert!(arrival.handle.is_cancelled());
        let forked = p.drive(async move { forked.await.unwrap() }).await;
        assert_eq!(forked, Err(StreamError::Cancelled));
        drop(open);
    })
}

/// Write the first `split` bytes of a `total`-byte Stream frame on a new
/// stream of `p.a`, let them reach `b`, and return the open stream and
/// `b`'s announcement of it.
async fn half_sent_stream(
    p: &mut Paths,
    body: &[u8],
    split: usize,
) -> (quinn::SendStream, InboundStreamArrival) {
    let conn = p.a_raw();
    let mut first = header(1, 1, StreamId::next().0, body.len() as u64, 0);
    first.extend_from_slice(&body[..split]);
    let time = p.time();
    let send = p
        .drive(async move {
            let mut send = conn.open_uni().await.unwrap();
            send.write_all(&first).await.unwrap();
            time.sleep(Duration::from_secs(1)).await;
            send
        })
        .await;
    let arrival = p.b_in.streams.try_recv().expect("announced");
    (send, arrival)
}

/// What one consumer's `assemble()` returned.
type Assembled = Result<Vec<u8>, StreamError>;

/// Assemble `arrival` through a clone and a fork of its handle, on a task.
fn assemble_both(
    arrival: &InboundStreamArrival,
) -> tokio::task::JoinHandle<(Assembled, Assembled)> {
    let handle = arrival.handle.clone();
    let fork = arrival.handle.fork();
    tokio::spawn(async move { tokio::join!(handle.assemble(), fork.assemble()) })
}

/// A QUIC-fed stream on a slow path (400 ms each way, so the reader gives
/// its consumers 24 round trips, 19.2 s) whose sender is silent for 8 s,
/// longer than the UDP transport's fragment gap (`STREAM_INACTIVITY_TIMEOUT`,
/// 5 s), still assembles, through the handle and a fork. quinn's retransmit
/// backoff at a high RTT with loss leaves such gaps in a stream it then
/// delivers. The gap passes on both clocks: the path's virtual one, and
/// tokio's paused one, which `assemble()`'s timer runs on. The reader set the
/// path's round trip on the handle, so this fails if it does not.
#[test]
fn a_quic_fed_stream_assembles_across_a_gap_past_the_udp_limit() {
    bounded(|| async move {
        use crate::transport::peer_connection::streaming::STREAM_INACTIVITY_TIMEOUT;
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-gap";
        // Before the handshake, so its round trip is quinn's first sample.
        crate::transport::in_memory_socket::set_packet_delivery_callback(
            network,
            Some(Arc::new(|_, _, _, _| {
                crate::transport::in_memory_socket::PacketDeliveryDecision::DelayedDelivery(
                    Duration::from_millis(400),
                )
            })),
        );
        let mut p = paths(network, QuicPathLimits::DEFAULT).await;
        tokio::time::pause();
        let gap = Duration::from_secs(8);
        assert!(gap > STREAM_INACTIVITY_TIMEOUT, "premise: {gap:?}");
        let body = payload(10_000);
        let (mut send, arrival) = half_sent_stream(&mut p, &body, 5000).await;
        assert!(!arrival.handle.is_complete(), "premise: half sent");
        assert!(
            arrival.handle.fragment_gap_limit() > gap,
            "the reader gave the handle its path's gap: {:?}",
            arrival.handle.fragment_gap_limit()
        );
        let assembled = assemble_both(&arrival);
        let time = p.time();
        let rest = body[5000..].to_vec();
        p.drive(async move {
            tokio::time::advance(gap).await;
            time.sleep(gap).await;
            send.write_all(&rest).await.unwrap();
            send.finish().unwrap();
            send.stopped().await.ok();
        })
        .await;
        let (handle, fork) = p.drive(async move { assembled.await.unwrap() }).await;
        assert_eq!(handle.as_deref(), Ok(&body[..]));
        assert_eq!(fork.as_deref(), Ok(&body[..]));
        assert_eq!(
            p.b.stats().shared_count(InboundStreamOutcome::EvictedIdle),
            0
        );
    })
}

/// A QUIC-fed stream whose sender goes silent ends for every consumer when
/// the path evicts it idle, with `InactivityTimeout`, forks included (a fork
/// does not see the handle's cancel), and not before.
#[test]
fn a_quic_fed_stream_evicted_idle_ends_every_consumer_with_inactivity() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-gap-idle", QuicPathLimits::DEFAULT).await;
        let idle = streaming_handle_idle_timeout();
        let body = payload(10_000);
        let (send, arrival) = half_sent_stream(&mut p, &body, 5000).await;
        let assembled = assemble_both(&arrival);
        let time = p.time();
        let started = time.now_nanos();
        let ((handle, fork), ended) = p
            .drive(async move {
                let ended = assembled.await.unwrap();
                (ended, time.now_nanos())
            })
            .await;
        assert_eq!(handle, Err(StreamError::InactivityTimeout));
        assert_eq!(fork, Err(StreamError::InactivityTimeout));
        let waited = Duration::from_nanos(ended - started);
        assert!(
            waited + Duration::from_secs(1) >= idle,
            "ended after {waited:?}, before the idle limit {idle:?}"
        );
        assert_eq!(
            p.b.stats().shared_count(InboundStreamOutcome::EvictedIdle),
            1
        );
        drop(send);
    })
}

/// A panic in the receive task ends a stream it was receiving for every
/// consumer, forks included: unwinding drops the stream's reader.
#[test]
fn a_panicking_receive_task_ends_a_stream_in_progress_for_forks() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let limits = QuicPathLimits {
            panic_in_receive: true,
            ..QuicPathLimits::DEFAULT
        };
        let mut p = paths("quic-path-gap-panic", limits).await;
        let body = payload(10_000);
        let (send, arrival) = half_sent_stream(&mut p, &body, 5000).await;
        let assembled = assemble_both(&arrival);
        let a = p.a.clone();
        let (handle, fork) = p
            .drive(async move {
                // The send itself may fail when the connection closes.
                a.send_short(b"boom").await.ok();
                assembled.await.unwrap()
            })
            .await;
        assert_eq!(handle, Err(StreamError::Cancelled));
        assert_eq!(fork, Err(StreamError::Cancelled));
        assert_eq!(
            p.b.stats().count(QuicInboundOutcome::ReceiveTaskPanicked),
            1
        );
        drop(send);
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
            a.send_short(&payload(2000)).await.expect("short");
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
        p.drive(async move { a.send_short(&payload(2000)).await.unwrap() })
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
    let body = pin::fn_body(source, "fn charge_short(");
    pin::assert_one_guard(
        &body,
        "charge_short",
        lock,
        &[
            "budget.check(total, Releaser::Quic)?;",
            "let key = self.short_key(&budget);",
        ],
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

/// `count` sends from `path` whose chunk source fails after its first chunk,
/// so each is reset on this side ([`reset_code::SENDER_ABORTED`]).
async fn abort_sends(path: Arc<QuicPath>, count: u64) {
    for _ in 0..count {
        let failed = path
            .send_stream_chunks(
                StreamId::next(),
                20_000,
                None,
                futures::stream::iter(vec![
                    Ok(Bytes::from_static(b"first")),
                    Err(StreamError::InactivityTimeout),
                ]),
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

/// The reset gauge for the fixed charge (buffer-budget PR 2): a path's
/// resets reach the node counters with the levels its count passes, and its
/// time up is reported tick by tick, so resets per connection-hour can be
/// read from the field.
#[test]
fn a_paths_resets_and_time_reach_the_node_counters() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let counters: &'static crate::transport::metrics::QuicDataCounters =
            Box::leak(Box::new(crate::transport::metrics::QuicDataCounters::new()));
        let limits = QuicPathLimits {
            counters: Some(counters),
            ..QuicPathLimits::DEFAULT
        };
        // `paths` gives `a` the same counters; only `b` resets sends.
        let p = paths("quic-path-reset-gauge", limits).await;
        let (b, time) = (p.b.clone(), p.time());
        let (resets, timed) = p
            .drive(async move {
                abort_sends(b.clone(), 64).await;
                let resets = counters.snapshot(true);
                time.sleep(Duration::from_secs(20)).await;
                (resets, counters.snapshot(true))
            })
            .await;
        assert_eq!(p.b.send_aborts(), 64);
        let keys: std::collections::BTreeMap<_, _> = resets.keys().into_iter().collect();
        assert_eq!(keys["quic_send_resets"], 64);
        assert_eq!(keys["quic_send_resets_reached_64"], 1);
        assert_eq!(keys["quic_send_resets_reached_512"], 0);
        assert_eq!(keys["quic_send_resets_path_max"], 64);
        // Two paths, 20 s between the two snapshots, each reported at its
        // watch's 2 s ticks.
        let tick = recycle::ACK_WATCH_TICK.as_millis() as u64;
        assert!(
            (2 * (20_000 - tick)..=2 * (20_000 + tick)).contains(&timed.path_ms),
            "{} ms of path time for two paths in 20 s",
            timed.path_ms
        );
        assert_eq!(timed.send_resets, 0);
    });
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
        let p = paths("quic-path-abort-limit", limits).await;
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
        let p = paths("quic-path-abort-dropped", limits).await;
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
                            stalls_after_first_chunk(),
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
        let p = paths("quic-path-written-stopped", QuicPathLimits::DEFAULT).await;
        let a = p.a.clone();
        let (written, aborts) = p
            .drive(async move {
                let header = FrameHeader::short(10).expect("a short header");
                let mut send = a
                    .open(&header, &[], None, None, SendProgress::new(), Opener::Data)
                    .await
                    .expect("open");
                let written = send.written(
                    Err(quinn::WriteError::Stopped(stop_code::BUDGET_REFUSED.into())),
                    10,
                );
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
        let p = paths("quic-path-abort-stops", b_limits).await;
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
pub(in crate::transport) struct DropFrom(String);

impl DropFrom {
    pub(in crate::transport) fn new(network: &str, from: &[SocketAddr]) -> Self {
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

/// A transfer still in flight 100 ms in, for the tests that cut the link
/// mid-transfer. The round trip here is about one [`PEER_STEP`], so the window
/// doubles every 20 ms from the initial 32 datagrams: 1 MiB would be done
/// before the cut (38 + 77 + 154 + 307 + 614 kB in five round trips).
const IN_FLIGHT_AT_THE_CUT: usize = 8 * 1024 * 1024;

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
    async fn connect(network: &str) -> Self {
        Self::connect_capped(network, None).await
    }

    /// [`Self::connect`] with a node-wide egress cap of `a_cap` bytes per
    /// second on `a`'s socket.
    async fn connect_capped(network: &str, a_cap: Option<u64>) -> Self {
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

        let net = crate::transport::test_net::subnet();
        let (a_addr, b_addr) = (net.addr(1), net.addr(2));
        let time = VirtualTime::new();
        register_network_time_source(network, time.clone());
        register_address_network(a_addr, network);
        register_address_network(b_addr, network);
        let settings = QuicSettings {
            memory_limit: 1024 * 1024 * 1024,
            idle_timeout: None,
            initial_window: Some(super::super::tests::TEST_WINDOW),
            rate_limit: Default::default(),
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
        .expect("a CappedCubic controller")
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
        let mut s = Scripted::connect(network).await;
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
        let mut s = Scripted::connect(network).await;
        s.keep_sending();
        s.a_sends(IN_FLIGHT_AT_THE_CUT);
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
        let mut s = Scripted::connect(network).await;
        s.keep_sending();
        s.a_sends(IN_FLIGHT_AT_THE_CUT);
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
        let mut s = Scripted::connect_capped(network, Some(50)).await;
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
        let p = paths(network, QuicPathLimits::DEFAULT).await;
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
    seeded_loss_from(network, percent, 0x5940_0006, None)
}

/// [`seeded_loss`] from draw state `seed`, with every delivered datagram
/// delayed by `one_way` if given.
fn seeded_loss_from(
    network: &str,
    percent: u64,
    seed: u64,
    one_way: Option<Duration>,
) -> Arc<LossCount> {
    use crate::transport::in_memory_socket::{
        PacketDeliveryDecision, set_packet_delivery_callback,
    };
    use std::sync::atomic::Ordering::Relaxed;
    let state = Arc::new(std::sync::atomic::AtomicU64::new(seed));
    let count = Arc::new(LossCount::default());
    let counted = count.clone();
    set_packet_delivery_callback(
        network,
        Some(Arc::new(move |_, _, _, _| {
            counted.seen.fetch_add(1, Relaxed);
            // A fixed linear congruential sequence, one draw per datagram.
            let next = state
                .fetch_update(Relaxed, Relaxed, |x| {
                    Some(x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1))
                })
                .unwrap_or_default();
            if (next >> 33) % 100 < percent {
                counted.dropped.fetch_add(1, Relaxed);
                PacketDeliveryDecision::Drop
            } else {
                match one_way {
                    Some(delay) => PacketDeliveryDecision::DelayedDelivery(delay),
                    None => PacketDeliveryDecision::Deliver,
                }
            }
        })),
    );
    count
}

/// A streamed send over a path with 5% random loss and a 200 ms round trip:
/// the send window (the floor: this path has no budget meter) drains at
/// cwnd/RTT, so a write waits past the 5 s
/// gap while the connection keeps acknowledging. The gap counts only when
/// nothing on the connection was acknowledged for `STALL_PROGRESS_WINDOW`, so
/// the send completes. Before, it timed out (`SendFailure::TimedOut`) and went
/// to UDP from the start.
///
/// The path runs plain Cubic: the loss gate (`recycle::loss_gate`) ignores
/// random loss with no queue, so through it this send is not slowed at all.
/// Cubic's collapse is still what happens when the gate passes losses (a
/// queue, or a loss share above its ceiling), and that is the case tested.
#[test]
fn a_lossy_path_finishes_a_stream_whose_writes_wait_past_the_gap() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-lossy-gap";
        let loss = seeded_loss_from(network, 5, 0x5940_0007, Some(Duration::from_millis(100)));
        let p = paths_without_loss_gate(network).await;
        let (a, time) = (p.a.clone(), p.time());
        const LEN: usize = 8 * MIB;
        let gaps = Arc::new(WriteGaps::new(time.clone()));
        let watched = gaps.clone();
        let sent = p
            .drive(async move {
                a.send_stream_bounded(
                    &time,
                    SendDeadlines::stream(LEN as u64),
                    StreamId::next(),
                    None,
                    payload(LEN).into(),
                    Some(&*watched),
                )
                .await
            })
            .await;
        let (seen, dropped) = (
            loss.seen.load(std::sync::atomic::Ordering::Relaxed),
            loss.dropped.load(std::sync::atomic::Ordering::Relaxed),
        );
        crate::transport::in_memory_socket::set_packet_delivery_callback(network, None);
        // The draw drops 5%; 3% is a floor with room for the draw's
        // variation, so a change that left the run nearly lossless fails here.
        assert!(
            dropped * 100 >= seen * 3,
            "the loss happened: {dropped} of {seen}"
        );
        // Without a write that waited past the 5 s gap this test would pass on
        // main too.
        let longest = gaps.longest();
        assert!(
            longest > super::super::super::peer_connection::streaming::STREAM_INACTIVITY_TIMEOUT,
            "a write waited past the gap: longest {longest:?}"
        );
        assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
    })
}

/// The loss gate's verdicts reach the node counters (`quic_gate_loss_<verdict>`)
/// from the path's report, one per congestion event: quinn counts an event in
/// `congestion_events` exactly where it calls the controller, so the verdicts
/// sum to it. With random loss and no queue most losses are ignored.
#[test]
fn the_loss_gates_verdicts_reach_the_node_counters() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let counters: &'static crate::transport::metrics::QuicDataCounters =
            Box::leak(Box::new(crate::transport::metrics::QuicDataCounters::new()));
        let network = "quic-path-gate-verdicts";
        let _loss = seeded_loss_from(network, 5, 0x5940_0011, Some(Duration::from_millis(50)));
        let limits = QuicPathLimits {
            counters: Some(counters),
            ..QuicPathLimits::DEFAULT
        };
        let p = paths(network, limits).await;
        let (a, time) = (p.a.clone(), p.time());
        const LEN: usize = MIB;
        let (sent, snap) = p
            .drive(async move {
                let sent = a
                    .send_stream_bounded(
                        &time,
                        SendDeadlines::stream(LEN as u64),
                        StreamId::next(),
                        None,
                        payload(LEN).into(),
                        None,
                    )
                    .await;
                // Two ticks of the acknowledgement watch report the counts.
                time.sleep(2 * recycle::ACK_WATCH_TICK).await;
                (sent, counters.snapshot(true))
            })
            .await;
        crate::transport::in_memory_socket::set_packet_delivery_callback(network, None);
        assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
        let keys: std::collections::BTreeMap<_, _> = snap.keys().into_iter().collect();
        let verdicts: u64 = snap.gate_loss.iter().sum();
        assert!(
            snap.congestion_events > 0,
            "the loss caused congestion events"
        );
        assert_eq!(verdicts, snap.congestion_events, "{keys:?}");
        assert!(
            keys["quic_gate_loss_ignored"] * 2 > verdicts,
            "random loss with no queue is mostly ignored: {keys:?}"
        );
        // When the path ends, `a`'s report counts it in the bucket of its
        // share passed as a queue (`b` sent too little to have 20 verdicts).
        assert!(verdicts >= 20, "{verdicts} verdicts");
        let (a, b, time) = (p.a.clone(), p.b.clone(), p.time());
        p.drive(async move {
            a.connection().close(QuicCloseCode::Downgrade);
            time.sleep(Duration::from_secs(5)).await;
            drop(b);
        })
        .await;
        let ended = counters.snapshot(true).gate_ended_queue;
        assert_eq!(ended, vec![1, 0, 0], "under 10% passed as a queue");
    })
}

/// The longest wait between two fragments a send handed to quinn.
struct WriteGaps {
    time: VirtualTime,
    last: std::sync::atomic::AtomicU64,
    longest: std::sync::atomic::AtomicU64,
}

impl WriteGaps {
    fn new(time: VirtualTime) -> Self {
        let now = time.now_nanos();
        Self {
            time,
            last: std::sync::atomic::AtomicU64::new(now),
            longest: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn longest(&self) -> Duration {
        Duration::from_nanos(self.longest.load(std::sync::atomic::Ordering::Relaxed))
    }
}

impl crate::transport::boundary::StreamProgressObserver for WriteGaps {
    fn on_fragment_sent(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let now = self.time.now_nanos();
        let gap = now.saturating_sub(self.last.swap(now, Relaxed));
        self.longest.fetch_max(gap, Relaxed);
    }
}

/// A path's watch reports its last counts when it ends, here dropped because
/// the receive task ended with the connection, so the node's packet sums end at
/// quinn's final counts. The short
/// is sent and the path closed between two ticks (2 s and 4 s), so without that
/// last report what they sent is missing.
#[test]
fn a_closed_path_reports_its_last_tick() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let counters: &'static crate::transport::metrics::QuicDataCounters =
            Box::leak(Box::new(crate::transport::metrics::QuicDataCounters::new()));
        let limits = QuicPathLimits {
            counters: Some(counters),
            ..QuicPathLimits::DEFAULT
        };
        let p = paths("quic-path-last-tick", limits).await;
        let (a, b, time) = (p.a.clone(), p.b.clone(), p.time());
        let totals = |a: &QuicPath, b: &QuicPath| {
            let (a, b) = (
                a.connection().inner().stats(),
                b.connection().inner().stats(),
            );
            (
                a.path.sent_packets + b.path.sent_packets,
                a.udp_tx.bytes + b.udp_tx.bytes,
            )
        };
        // The watches count from when the paths were built, before the drive.
        let before = totals(&a, &b);
        let (at_short, after) = p
            .drive(async move {
                time.sleep(Duration::from_millis(2500)).await;
                a.send_short(&[7u8; 600]).await.expect("sent");
                let at_short = totals(&a, &b);
                a.connection().close(QuicCloseCode::Downgrade);
                time.sleep(Duration::from_secs(5)).await;
                (at_short, totals(&a, &b))
            })
            .await;
        let snap = counters.snapshot(false);
        // Every packet up to the short, which went after the 2 s tick, is in
        // the report. A close quinn sends again after a watch has ended is in
        // quinn's counts and in no report, so the report can be below them.
        let (up_to_short, sent) = (at_short.0 - before.0, after.0 - before.0);
        assert!(
            snap.sent_packets >= up_to_short && snap.sent_packets <= sent,
            "{} reported, {up_to_short} up to the short, {sent} in all",
            snap.sent_packets
        );
        let (up_to_short, udp_sent) = (at_short.1 - before.1, after.1 - before.1);
        assert!(
            snap.bytes_sent >= up_to_short && snap.bytes_sent <= udp_sent,
            "{} reported, {up_to_short} up to the short, {udp_sent} in all",
            snap.bytes_sent
        );
        // Both paths ended, each counted once, here below the floor.
        let ended = snap.ended_loss_rate.iter().sum::<u64>() + snap.ended_below_floor;
        assert_eq!(ended, 2);
        assert_eq!(snap.loss_rate.iter().sum::<u32>(), 0);
    })
}

/// A panic in the acknowledgement watch, which the receive supervisor
/// contains while the path stays up, leaves the path in its live loss-rate
/// bucket; it is counted as ended only when the path really ends.
#[test]
fn a_watch_panic_does_not_end_the_path_in_the_loss_counts() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let counters: &'static crate::transport::metrics::QuicDataCounters =
            Box::leak(Box::new(crate::transport::metrics::QuicDataCounters::new()));
        let limits = QuicPathLimits {
            counters: Some(counters),
            panic_in_watch: true,
            ..QuicPathLimits::DEFAULT
        };
        // Only `b` panics: `paths` gives `a` the default limits.
        let p = paths("quic-path-watch-panic", limits).await;
        let panics = recycle::TEST_WATCH_PANICS.load(std::sync::atomic::Ordering::Relaxed);
        // `b` sends, so its own packet count passes the floor.
        let (b, time) = (p.b.clone(), p.time());
        let (live, ended) = p
            .drive(async move {
                b.send_stream(StreamId::next(), None, payload(2 * MIB).into())
                    .await
                    .expect("sent");
                time.sleep(Duration::from_secs(5)).await;
                let snap = counters.snapshot(false);
                (
                    snap.loss_rate.iter().sum::<u32>(),
                    snap.ended_loss_rate.iter().sum::<u64>() + snap.ended_below_floor,
                )
            })
            .await;
        assert!(
            recycle::TEST_WATCH_PANICS.load(std::sync::atomic::Ordering::Relaxed) > panics,
            "b's watch panicked"
        );
        assert!(p.b.connection().inner().close_reason().is_none(), "b is up");
        // `a` only acknowledged, below the floor; `b` is in a bucket.
        assert_eq!(live, 1, "b is still in its live bucket");
        assert_eq!(ended, 0, "nothing ended");
        let (a, b, time) = (p.a.clone(), p.b.clone(), p.time());
        p.drive(async move {
            a.connection().close(QuicCloseCode::Downgrade);
            time.sleep(Duration::from_secs(5)).await;
            drop(b);
        })
        .await;
        let snap = counters.snapshot(false);
        assert_eq!(snap.loss_rate.iter().sum::<u32>(), 0);
        assert_eq!(snap.ended_loss_rate.iter().sum::<u64>(), 1, "b ended");
        assert_eq!(snap.ended_below_floor, 1, "a ended");
    })
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
        let p = paths(network, QuicPathLimits::DEFAULT).await;
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
        let p = paths(network, QuicPathLimits::DEFAULT).await;
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
        let p = paths("quic-path-ack-watch-idle", QuicPathLimits::DEFAULT).await;
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

/// A chunk source that gives one chunk and then has nothing more: a send from
/// it opens its stream, which then waits on the source.
fn stalls_after_first_chunk()
-> impl futures::Stream<Item = Result<Bytes, StreamError>> + Unpin + Send + 'static {
    futures::stream::iter([Ok(Bytes::from_static(b"first"))]).chain(futures::stream::pending())
}

/// A future that never completes, standing in for a send blocked on the
/// peer.
async fn never() -> Result<(), QuicSendError> {
    futures::future::pending().await
}

/// `bounded_send`'s classification, on virtual time with sends that block: a
/// deadline before the stream is open is `NotOpened`, one after that
/// `Failed(TimedOut)`, one after the end of the stream
/// `MaybeDelivered(TimedOut)`; a connection lost before the stream is open is
/// `NotOpened`, after it `Failed` and after the end
/// `MaybeDelivered(ConnectionLost)`; a stop after the end is `Failed` (the peer
/// refused it); success is `Sent`. The deadline is the one given, on the given
/// clock.
#[test]
fn bounded_send_classifies_by_whether_the_stream_was_finished() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let (unopened, blocked, finished_then_blocked, timings) = drive(&time, LIMIT, async move {
            let unopened = bounded_send(&clock, Duration::from_secs(1), None, |_| never()).await;
            let start = clock.now();
            let blocked = bounded_send(
                &clock,
                Duration::from_secs(5),
                None,
                |progress| async move {
                    progress.mark_opened();
                    never().await
                },
            )
            .await;
            let blocked_after = clock.now() - start;
            let finished = bounded_send(
                &clock,
                Duration::from_secs(3),
                None,
                |progress| async move {
                    progress.mark_finished();
                    never().await
                },
            )
            .await;
            (
                unopened,
                blocked,
                finished,
                (blocked_after, clock.now() - start),
            )
        })
        .await;
        assert!(
            matches!(
                unopened,
                BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut))
            ),
            "{unopened:?}"
        );
        assert!(
            matches!(blocked, BoundedSend::Failed(SendFailure::TimedOut)),
            "{blocked:?}"
        );
        assert!(
            matches!(
                finished_then_blocked,
                BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut)
            ),
            "{finished_then_blocked:?}"
        );
        assert_eq!(timings, (Duration::from_secs(5), Duration::from_secs(8)));

        let lost = || QuicSendError::ConnectionLost(quinn::ConnectionError::LocallyClosed);
        let progress = SendProgress::new();
        assert!(matches!(
            bounded::outcome(Err(lost()), &progress),
            BoundedSend::Failed(SendFailure::NotOpened(NotOpened::ConnectionLost(_)))
        ));
        progress.mark_opened();
        assert!(matches!(
            bounded::outcome(Err(lost()), &progress),
            BoundedSend::Failed(SendFailure::Error(QuicSendError::ConnectionLost(_)))
        ));
        assert!(matches!(
            bounded::outcome(Ok(()), &progress),
            BoundedSend::Sent
        ));
        progress.mark_finished();
        assert!(matches!(
            bounded::outcome(Err(lost()), &progress),
            BoundedSend::MaybeDelivered(MaybeDelivered::ConnectionLost)
        ));
        assert!(matches!(
            bounded::outcome(Err(stopped(stop_code::CHANNEL_FULL)), &progress),
            BoundedSend::Failed(SendFailure::Error(QuicSendError::Stopped(_)))
        ));
    })
}

/// A send that finishes before its deadline leaves no timer on the clock
/// (`bounded_send` uses the cancelable sleep).
#[test]
fn bounded_send_that_finishes_early_leaves_no_wakeup() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let sent = drive(&time, LIMIT, async move {
            bounded_send(&clock, Duration::from_secs(60), None, |_| async { Ok(()) }).await
        })
        .await;
        assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
        assert_eq!(time.pending_wakeup_count(), 0);
    })
}

/// The outcomes on a real path. A `Ready` sent and acknowledged is `Sent`,
/// and the receiver decodes it. With every acknowledgement from the peer
/// dropped, the next frame is written whole and arrives, but the sender
/// cannot know: `MaybeDelivered(TimedOut)`, not `Failed`. With every uni
/// stream credit held, a send cannot open its stream: `Failed(TimedOut)`.
/// A connection closed while waiting for the acknowledgement is
/// `MaybeDelivered(ConnectionLost)`.
#[test]
fn bounded_sends_on_a_path_report_each_outcome() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-bounded", QuicPathLimits::FULL_STREAMS).await;
        let (a, time) = (p.a.clone(), p.time());
        let ready = QuicControl::Ready {
            attempt: 7,
            caps: 0,
        };
        let ack = QuicControl::ReadyAck {
            attempt: 7,
            caps: 0,
        };
        let timeout = Duration::from_secs(5);
        let sent = p
            .drive(async move { a.send_control_bounded(&time, timeout, ready).await })
            .await;
        assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
        let payload = p.b_in.controls.try_recv().expect("delivered");
        assert_eq!(p.b.decode_control(&payload), Ok(ready));

        // Nothing from b reaches a: no acknowledgement.
        let silenced = DropFrom::new("quic-path-bounded", &[p.pair.b_addr]);
        let (a, time) = (p.a.clone(), p.time());
        let unacked = p
            .drive(async move { a.send_control_bounded(&time, timeout, ack).await })
            .await;
        assert!(
            matches!(
                unacked,
                BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut)
            ),
            "{unacked:?}"
        );
        let payload = p.b_in.controls.try_recv().expect("it did arrive");
        assert_eq!(p.b.decode_control(&payload), Ok(ack));

        // Still no acknowledgements; a closes its side while waiting.
        let (a, time) = (p.a.clone(), p.time());
        let lost = p
            .drive(async move {
                let send = a.send_control_bounded(&time, Duration::from_secs(60), ack);
                tokio::pin!(send);
                tokio::select! {
                    outcome = &mut send => panic!("returned before the close: {outcome:?}"),
                    () = time.sleep(Duration::from_secs(1)) => {}
                }
                a.connection().close(QuicCloseCode::Downgrade);
                send.await
            })
            .await;
        assert!(
            matches!(
                lost,
                BoundedSend::MaybeDelivered(MaybeDelivered::ConnectionLost)
            ),
            "{lost:?}"
        );
        drop(silenced);

        // A fresh pair, every credit held by streams b is still reading.
        let p = paths("quic-path-bounded-open", QuicPathLimits::FULL_STREAMS).await;
        let (conn, a, time) = (p.a_raw(), p.a.clone(), p.time());
        let (blocked, held) = p
            .drive(async move {
                let mut held = Vec::new();
                for _ in 0..super::super::config::MAX_UNI_STREAMS {
                    let mut send = conn.open_uni().await.unwrap();
                    let head = header(1, 1, StreamId::next().0, 1000, 0);
                    send.write_all(&head).await.unwrap();
                    held.push(send);
                }
                let blocked = a.send_control_bounded(&time, timeout, ready).await;
                (blocked, held)
            })
            .await;
        assert!(
            matches!(
                blocked,
                BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut))
            ),
            "{blocked:?}"
        );
        assert_eq!(p.b.stats().count(QuicInboundOutcome::DeliveredControl), 0);
        assert_eq!(
            p.b.stats().count(QuicInboundOutcome::AnnouncedStream),
            u64::from(super::super::config::MAX_UNI_STREAMS),
            "b holds every stream, so the send was blocked by the limit"
        );
        drop(held);
    })
}

/// A control payload the receiving path cannot read is refused and counted
/// by kind: an unknown version, an unknown type, a wrong length.
#[test]
fn refused_control_payloads_are_counted() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-control-refused", QuicPathLimits::DEFAULT).await;
        let mut ready = QuicControl::Ready {
            attempt: 1,
            caps: 0,
        }
        .encode();
        assert!(p.b.decode_control(&ready).is_ok());
        ready[0] = 2;
        assert_eq!(
            p.b.decode_control(&ready),
            Err(ControlError::UnknownVersion(2))
        );
        ready[0] = 1;
        ready[1] = 9;
        assert_eq!(
            p.b.decode_control(&ready),
            Err(ControlError::UnknownType(9))
        );
        assert_eq!(
            p.b.decode_control(&[1, 0, 0]),
            Err(ControlError::WrongLength(3))
        );
        assert_eq!(p.b.decode_control(&[]), Err(ControlError::Empty));
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::ControlUnknownVersion), 1);
        assert_eq!(stats.count(QuicInboundOutcome::ControlUnknownType), 1);
        assert_eq!(stats.count(QuicInboundOutcome::ControlMalformed), 2);
    })
}

/// quinn-proto 0.11.19 announces new stream credit only past 64 / 8 freed
/// streams (`queue_max_stream_id`). Written out, not derived from
/// `UNANNOUNCED_CREDIT_SLACK`, so a wrong slack is not checked against itself.
const QUINN_UNANNOUNCED_CREDIT: usize = 8;

/// `count` streamed sends on a source that never delivers, for a test to race
/// against: each holds a permit and a stream, or waits for a permit.
fn blocked_streamed_sends(
    a: &Arc<QuicPath>,
    count: usize,
) -> futures::stream::FuturesUnordered<
    impl std::future::Future<Output = Result<(), QuicSendError>> + use<>,
> {
    (0..count)
        .map(|_| {
            let a = a.clone();
            async move {
                a.send_stream_chunks(StreamId::next(), 1000, None, stalls_after_first_chunk())
                    .await
            }
        })
        .collect()
}

/// Against a fresh data path, whose peer allows `config::DATA_START_UNI_STREAMS`
/// streams before any raise, more streamed sends than that plus a one-packet
/// Short and a control frame: with every slot held, each of the excess waits
/// in `open_uni` for its open deadline and ends `NotOpened`, never
/// `MaybeDelivered` (nothing was written), so it can go another way; each is
/// counted `stream_limit_full`. Once the slots are freed, sends open again.
#[test]
fn sends_past_a_fresh_peers_stream_limit_end_not_opened() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-fresh-limit", QuicPathLimits::DEFAULT).await;
        let start = super::super::config::DATA_START_UNI_STREAMS as usize;
        let excess = 8usize;
        let (a, time) = (p.a.clone(), p.time());
        let (streamed, short, control, after) = p
            .drive(async move {
                let mut blocked = blocked_streamed_sends(&a, start);
                let sends = async {
                    time.sleep(Duration::from_secs(1)).await;
                    let streamed = futures::future::join_all((0..excess).map(|_| {
                        let (a, time) = (a.clone(), time.clone());
                        async move {
                            a.send_stream_bounded(
                                &time,
                                SendDeadlines::stream(2000),
                                StreamId::next(),
                                None,
                                Bytes::from(payload(2000)),
                                None,
                            )
                            .await
                        }
                    }))
                    .await;
                    let short = a
                        .send_short_bounded(&time, SendDeadlines::short(7), b"through")
                        .await;
                    let control = a
                        .send_control_bounded(
                            &time,
                            Duration::from_secs(5),
                            QuicControl::Ready {
                                attempt: 1,
                                caps: 0,
                            },
                        )
                        .await;
                    (streamed, short, control)
                };
                let (streamed, short, control) = tokio::select! {
                    done = blocked.next() => panic!("a blocked send returned: {done:?}"),
                    sent = sends => sent,
                };
                drop(blocked);
                time.sleep(Duration::from_secs(1)).await;
                let after = a
                    .send_short_bounded(&time, SendDeadlines::short(5), b"later")
                    .await;
                (streamed, short, control, after)
            })
            .await;
        let not_opened =
            |sent: &BoundedSend| matches!(sent, BoundedSend::Failed(SendFailure::NotOpened(_)));
        assert!(streamed.iter().all(not_opened), "{streamed:?}");
        assert!(not_opened(&short), "{short:?}");
        assert!(not_opened(&control), "{control:?}");
        assert_eq!(
            p.a.stats().send_event_count(QuicSendEvent::StreamLimitFull) as usize,
            excess + 2
        );
        assert!(matches!(after, BoundedSend::Sent), "{after:?}");
    })
}

/// `stream_limit_full` counts a send whose open deadline ended its wait for
/// a stream credit, not one cancelled before it (a teardown, a shutdown).
#[test]
fn a_send_cancelled_while_waiting_for_a_stream_credit_is_not_counted() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-credit-cancel", QuicPathLimits::DEFAULT).await;
        let start = super::super::config::DATA_START_UNI_STREAMS as usize;
        let (a, time) = (p.a.clone(), p.time());
        p.drive(async move {
            let mut blocked = blocked_streamed_sends(&a, start);
            // The blocked sends take every slot first; the short waits for
            // a credit and is cancelled half a second in, before its 1 s
            // open deadline.
            let short = async {
                time.sleep(Duration::from_secs(1)).await;
                a.send_short_bounded(&time, SendDeadlines::short(7), b"through")
                    .await
            };
            tokio::select! {
                done = blocked.next() => panic!("a blocked send returned: {done:?}"),
                sent = short => panic!("the short could not open: {sent:?}"),
                () = time.sleep(Duration::from_millis(1500)) => {}
            }
            drop(blocked);
        })
        .await;
        assert_eq!(
            p.a.stats().send_event_count(QuicSendEvent::StreamLimitFull),
            0
        );
    })
}

/// A control frame reserved before data is enabled
/// (`QuicPath::control_first`, as the upgrade driver reserves a client's
/// `ReadyAck` in the step that enables its data) gets its stream even when
/// more data sends than the fresh peer's stream limit are already waiting
/// to open: they wait for it, then take the slots left. Without the
/// reservation the data sends take every slot and the control frame waits
/// out its deadline, which fails the upgrade.
#[test]
fn a_reserved_control_frame_opens_ahead_of_data_at_a_full_limit() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-control-first", QuicPathLimits::DEFAULT).await;
        let start = super::super::config::DATA_START_UNI_STREAMS as usize;
        let (a, time) = (p.a.clone(), p.time());
        let control = p
            .drive(async move {
                let first = a.control_first();
                let mut blocked = blocked_streamed_sends(&a, start + 4);
                // Each data send has its first chunk and its permit, and
                // would open its stream now.
                assert!(futures::poll!(blocked.next()).is_pending());
                let control = a.send_control_first(
                    &time,
                    Duration::from_secs(5),
                    QuicControl::ReadyAck {
                        attempt: 1,
                        caps: 0,
                    },
                    first,
                );
                let control = tokio::select! {
                    done = blocked.next() => panic!("a blocked send returned: {done:?}"),
                    sent = control => sent,
                };
                time.sleep(Duration::from_secs(1)).await;
                drop(blocked);
                control
            })
            .await;
        assert!(matches!(control, BoundedSend::Sent), "{control:?}");
        assert_eq!(
            p.a.stats().send_event_count(QuicSendEvent::StreamLimitFull),
            0,
            "the control frame did not wait for a slot"
        );
        let mut announced = 0;
        let mut b_in = p.b_in;
        while b_in.streams.try_recv().is_ok() {
            announced += 1;
        }
        assert!(
            announced >= start - 1,
            "the data sends opened after it: {announced}"
        );
    })
}

/// The reserve is exactly what the division of the limit says, in the worst
/// case for quinn's batched credit: with 8 freed slots not yet announced and
/// every permit held by a send blocked on a source that never delivers, 8
/// slots are left. Seven are held here and a short message takes the eighth;
/// a control frame goes through after it. The streamed sends past the limit
/// wait for a permit without opening a stream.
#[test]
fn a_short_still_sends_while_streamed_messages_are_blocked() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-reserved", QuicPathLimits::FULL_STREAMS).await;
        let limit = super::super::config::STREAMED_SEND_LIMIT as usize;
        assert_eq!(limit, 48);
        let streamed = super::super::config::MAX_UNI_STREAMS as usize + 6;
        let (a, conn, time) = (p.a.clone(), p.a_raw(), p.time());
        let (short, control, still_blocked) = p
            .drive(async move {
                // Freed at the receiver, too few for quinn to announce.
                for _ in 0..QUINN_UNANNOUNCED_CREDIT {
                    a.send_short(b"earlier").await.expect("earlier short");
                }
                time.sleep(Duration::from_secs(1)).await;
                let mut blocked = blocked_streamed_sends(&a, streamed);
                let timeout = Duration::from_secs(5);
                let sends = async {
                    time.sleep(Duration::from_secs(2)).await;
                    let mut held = Vec::new();
                    for slot in 1..=7 {
                        tokio::select! {
                            send = conn.open_uni() => held.push(send.unwrap()),
                            () = time.sleep(timeout) => panic!("reserved slot {slot} is missing"),
                        }
                    }
                    let short = a.send_short_bounded(&time, timeout, b"through").await;
                    let control = a
                        .send_control_bounded(
                            &time,
                            timeout,
                            QuicControl::Ready {
                                attempt: 1,
                                caps: 0,
                            },
                        )
                        .await;
                    (short, control)
                };
                tokio::select! {
                    done = blocked.next() => panic!("a blocked send returned: {done:?}"),
                    (short, control) = sends => (short, control, blocked.len()),
                }
            })
            .await;
        assert!(matches!(short, BoundedSend::Sent), "{short:?}");
        assert!(matches!(control, BoundedSend::Sent), "{control:?}");
        assert_eq!(still_blocked, streamed);
        assert_eq!(
            p.b.stats().count(QuicInboundOutcome::AnnouncedStream) as usize,
            limit,
            "streamed sends past the limit wait without opening a stream"
        );
        let mut shorts = Vec::new();
        while let Ok(short) = p.b_in.shorts.try_recv() {
            shorts.push(short.into_payload());
        }
        assert_eq!(shorts.len(), QUINN_UNANNOUNCED_CREDIT + 1);
        assert_eq!(&shorts[QUINN_UNANNOUNCED_CREDIT][..], b"through");
        assert!(p.b_in.controls.try_recv().is_ok());
    })
}

/// A streamed send returns its permit however it ends: more sends than
/// there are permits complete one after another, and after every permit was
/// held by a send that timed out, the next streamed send still completes.
#[test]
fn streamed_sends_return_their_permits() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-permits", QuicPathLimits::FULL_STREAMS).await;
        let limit = super::super::config::STREAMED_SEND_LIMIT as usize;
        let (a, time) = (p.a.clone(), p.time());
        let (completed, timed_out, later) = p
            .drive(async move {
                let timeout = Duration::from_secs(5);
                let body = Bytes::from_static(b"body");
                let mut completed = Vec::new();
                for _ in 0..=limit {
                    completed.push(
                        a.send_stream_bounded(
                            &time,
                            timeout,
                            StreamId::next(),
                            None,
                            body.clone(),
                            None,
                        )
                        .await,
                    );
                }
                let timed_out = futures::future::join_all((0..limit).map(|_| {
                    a.send_stream_chunks_bounded(
                        &time,
                        Duration::from_secs(1),
                        StreamId::next(),
                        1000,
                        None,
                        stalls_after_first_chunk(),
                        None,
                    )
                }))
                .await;
                let later = a
                    .send_stream_bounded(&time, timeout, StreamId::next(), None, body, None)
                    .await;
                (completed, timed_out, later)
            })
            .await;
        for outcome in &completed {
            assert!(matches!(outcome, BoundedSend::Sent), "{outcome:?}");
        }
        for outcome in &timed_out {
            assert!(
                matches!(outcome, BoundedSend::Failed(SendFailure::TimedOut)),
                "{outcome:?}"
            );
        }
        assert!(matches!(later, BoundedSend::Sent), "{later:?}");
    })
}

/// A bounded send whose deadline passes while it waits for a permit is
/// `NotOpened`, and it opened no stream: the receiver announces only the
/// streams holding permits, and nothing had to be reset.
#[test]
fn a_bounded_send_timing_out_on_a_permit_opens_no_stream() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-permit-wait", QuicPathLimits::FULL_STREAMS).await;
        let limit = super::super::config::STREAMED_SEND_LIMIT as usize;
        let (a, time) = (p.a.clone(), p.time());
        let (outcome, aborts) = p
            .drive(async move {
                let mut blocked = blocked_streamed_sends(&a, limit);
                let send = async {
                    time.sleep(Duration::from_secs(2)).await;
                    let outcome = a
                        .send_stream_bounded(
                            &time,
                            Duration::from_secs(3),
                            StreamId::next(),
                            None,
                            Bytes::from_static(b"body"),
                            None,
                        )
                        .await;
                    (outcome, a.send_aborts())
                };
                tokio::select! {
                    done = blocked.next() => panic!("a blocked send returned: {done:?}"),
                    result = send => result,
                }
            })
            .await;
        assert!(
            matches!(
                outcome,
                BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut))
            ),
            "{outcome:?}"
        );
        assert_eq!(aborts, 0, "no stream was opened, so none was reset");
        assert_eq!(
            p.b.stats().count(QuicInboundOutcome::AnnouncedStream) as usize,
            limit
        );
    })
}

/// A shadow path never sends data. Only a debug build checks.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "a shadow QUIC path sends control frames only")]
fn a_shadow_path_does_not_send_data() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-shadow-sends", QuicPathLimits::SHADOW).await;
        let time = p.time();
        p.b.send_short_bounded(&time, Duration::from_secs(5), b"data")
            .await;
    });
}

/// A path in shadow mode refuses a Short or Stream frame with
/// `NOT_ACCEPTING_DATA` before anything is charged, counts it, and closes the
/// path with DOWNGRADE after its first refusal, once: the inbound budget it shares
/// with the UDP session is untouched (no charge, no budget refusal), even when
/// that budget is full. A control frame sent first still arrives. The close waits
/// `REFUSAL_CLOSE_DELAY`, so each refused send sees its stop.
#[test]
fn a_shadow_path_refuses_data_before_charging_the_budget_and_closes() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        for (case, budget) in [
            ("open", SharedInboundBudget::new(1024, 1 << 30)),
            ("full", SharedInboundBudget::new(0, 0)),
        ] {
            let mut p = paths_with(
                &format!("quic-path-shadow-{case}"),
                QuicPathLimits::SHADOW,
                Some(budget),
            )
            .await;
            let (a, time) = (p.a.clone(), p.time());
            let timeout = Duration::from_secs(5);
            let (control, short, stream) = p
                .drive(async move {
                    let control = a
                        .send_control_bounded(
                            &time,
                            timeout,
                            QuicControl::Ready {
                                attempt: 3,
                                caps: 0,
                            },
                        )
                        .await;
                    let short = a.send_short_bounded(&time, timeout, &over_window()).await;
                    let stream = a
                        .send_stream_bounded(
                            &time,
                            timeout,
                            StreamId::next(),
                            Some(b"meta"),
                            over_window(),
                            None,
                        )
                        .await;
                    // The close follows the first refusal after the delay.
                    time.sleep(inbound::REFUSAL_CLOSE_DELAY + Duration::from_secs(1))
                        .await;
                    (control, short, stream)
                })
                .await;
            assert!(matches!(control, BoundedSend::Sent), "{control:?}");
            // Both are refused before the close, so each sender sees its stop.
            for (what, outcome) in [("short", short), ("stream", stream)] {
                let BoundedSend::Failed(SendFailure::Error(QuicSendError::Stopped(code))) = outcome
                else {
                    panic!("{what}: expected a stop, got {outcome:?}");
                };
                assert_eq!(code, u64::from(stop_code::NOT_ACCEPTING_DATA), "{what}");
            }
            assert!(
                p.a.quinn_connection().close_reason().is_some(),
                "the path closed"
            );
            let stats = p.b.stats();
            assert_eq!(stats.count(QuicInboundOutcome::RefusedNotAcceptingData), 2);
            assert_eq!(stats.count(QuicInboundOutcome::ClosedRefusingData), 1);
            assert_eq!(p.b.charged_streams_now(), 0, "nothing was charged");
            assert_eq!(p.b.charged(), (0, 0));
            for outcome in [
                InboundStreamOutcome::RefusedConnectionBytes,
                InboundStreamOutcome::RefusedConnectionStreams,
            ] {
                assert_eq!(stats.shared_count(outcome), 0, "{outcome:?}");
            }
            assert!(p.b_in.shorts.try_recv().is_err());
            assert!(p.b_in.streams.try_recv().is_err());
            let payload = p.b_in.controls.try_recv().expect("control arrives");
            assert!(p.b.decode_control(&payload).is_ok());
        }
    })
}

/// A path that ends inside the refusal's close delay is not closed again: no
/// `ClosedRefusingData` is counted, and the receiver's reader for the refused
/// frame ends with the connection.
#[test]
fn a_path_that_ends_inside_the_refusal_delay_is_not_closed_again() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths_with("quic-path-refused-end", QuicPathLimits::SHADOW, None).await;
        let (a, time) = (p.a.clone(), p.time());
        let short = p
            .drive(async move {
                let short = a
                    .send_short_bounded(&time, Duration::from_secs(5), &over_window())
                    .await;
                // Inside the delay, the path ends from the sender's side.
                time.sleep(inbound::REFUSAL_CLOSE_DELAY / 2).await;
                a.close(QuicCloseCode::NodeDroppedPeer);
                time.sleep(inbound::REFUSAL_CLOSE_DELAY * 2).await;
                short
            })
            .await;
        assert!(
            matches!(
                short,
                BoundedSend::Failed(SendFailure::Error(QuicSendError::Stopped(_)))
            ),
            "{short:?}"
        );
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::RefusedNotAcceptingData), 1);
        assert_eq!(stats.count(QuicInboundOutcome::ClosedRefusingData), 0);
        assert!(p.b.quinn_connection().close_reason().is_some());
    })
}

/// Counts `on_fragment_sent` calls.
#[derive(Default)]
struct Fragments(std::sync::atomic::AtomicUsize);

impl StreamProgressObserver for Fragments {
    fn on_fragment_sent(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

impl Fragments {
    fn count(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

/// The values the send table is written around.
#[test]
fn send_deadlines_have_these_values() {
    let one_packet = SendDeadlines::short(100);
    assert_eq!(one_packet.open, Duration::from_secs(1));
    assert_eq!(one_packet.gap, None);
    assert_eq!(one_packet.total, Duration::from_secs(5));

    for len in [1, 2000, MIB as u64] {
        let deadlines = SendDeadlines::stream(len);
        assert!(deadlines.gap.is_none_or(|gap| gap >= deadlines.open));
    }

    let stream = SendDeadlines::stream(MIB as u64);
    assert_eq!(stream.open, Duration::from_secs(2));
    assert_eq!(stream.gap, Some(Duration::from_secs(5)));
    assert_eq!(
        stream.total,
        crate::transport::peer_connection::inbound_stream_age_limit(MIB as u64)
    );

    assert_eq!(
        SendDeadlines::short(MIB as u64),
        stream,
        "a Short that does not fit a packet holds a slot as a stream does"
    );
}

/// The gap runs from the last step, each step moving it, and a send making
/// steady progress still ends at `total`. Timers are not one per step.
#[test]
fn the_gap_runs_from_the_last_step_and_total_still_binds() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let deadlines = SendDeadlines {
            open: Duration::from_secs(2),
            gap: Some(Duration::from_secs(5)),
            source_gap: None,
            total: Duration::from_secs(60),
        };
        let (stalled, stalled_after, steady, steady_after) = drive(&time, LIMIT, async move {
            let start = clock.now();
            let stepping = clock.clone();
            let stalled = bounded_send(&clock, deadlines, None, |progress| async move {
                progress.mark_opened();
                for _ in 0..3 {
                    stepping.sleep(Duration::from_secs(3)).await;
                    progress.step();
                }
                never().await
            })
            .await;
            let stalled_after = clock.now() - start;
            let start = clock.now();
            let stepping = clock.clone();
            let steady = bounded_send(&clock, deadlines, None, |progress| async move {
                progress.mark_opened();
                loop {
                    stepping.sleep(Duration::from_secs(3)).await;
                    progress.step();
                }
            })
            .await;
            (stalled, stalled_after, steady, clock.now() - start)
        })
        .await;
        assert!(
            matches!(stalled, BoundedSend::Failed(SendFailure::TimedOut)),
            "{stalled:?}"
        );
        assert_eq!(stalled_after, Duration::from_secs(14));
        assert!(
            matches!(steady, BoundedSend::Failed(SendFailure::TimedOut)),
            "{steady:?}"
        );
        assert_eq!(steady_after, Duration::from_secs(60));
    })
}

/// A send stuck between two writes, on a connection acknowledged every second
/// for its first 30 s, with deadlines of 2 s, 5 s and 120 s, timed on `clock`
/// with `connection` read through `acks` and `cap`: what it ended as, after
/// how long. `ack_until` 0 acknowledges nothing; `hold` has the cap hold a
/// transmit every second instead of the peer acknowledging.
async fn stuck_send(
    clock: VirtualTime,
    ack_until: u64,
    hold: bool,
    first_ack: bool,
    total: Duration,
) -> (BoundedSend, Duration) {
    let acks = recycle::AckProgress::detached();
    let cap = Arc::new(super::super::socket::CapHolds::new(Some(10_000_000)));
    let connection = bounded::ConnectionProgress {
        acks: acks.clone(),
        cap_holds: cap.clone(),
        quiet: Arc::default(),
        last_acked: Arc::new(bounded::AckRecord::new(acks.acked(), bounded::NO_ACK)),
    };
    let deadlines = SendDeadlines {
        open: Duration::from_secs(2),
        gap: Some(Duration::from_secs(5)),
        source_gap: None,
        total,
    };
    let start = clock.now();
    let ticking = clock.clone();
    let peer = async move {
        for second in 0..ack_until {
            ticking.sleep(Duration::from_secs(1)).await;
            if hold {
                cap.hold_one();
                if first_ack && second == 0 {
                    acks.ack();
                }
            } else {
                acks.ack();
            }
        }
        never().await
    };
    let send = bounded_send(&clock, deadlines, Some(connection), |progress| async move {
        progress.mark_opened();
        never().await
    });
    let outcome = tokio::select! {
        outcome = send => outcome,
        _ = peer => unreachable!("the peer never ends"),
    };
    (outcome, clock.now() - start)
}

/// While the connection is acknowledged a send's gap does not run out, and
/// once nothing is it ends within twice `STALL_PROGRESS_WINDOW` of the last
/// acknowledgement. The timer is first armed for `open` (2 s), before the send
/// marks its stream open; the wakes at 2, 12, 22 and 32 s each see a new
/// acknowledgement, and the one at 42 s sees none, 12 s after the last (30 s).
#[test]
fn the_gap_runs_on_while_the_connection_is_acknowledged() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let (sent, after) = drive(&time, LIMIT, async move {
            stuck_send(clock, 30, false, false, Duration::from_secs(120)).await
        })
        .await;
        assert!(
            matches!(sent, BoundedSend::Failed(SendFailure::TimedOut)),
            "{sent:?}"
        );
        assert_eq!(after, Duration::from_secs(42));
        assert!(after.as_secs() <= 30 + 2 * super::route::STALL_PROGRESS_WINDOW.as_secs());
    })
}

/// With nothing acknowledged during the send and no acknowledgement on record
/// (`NO_ACK`), the send fails at its own 5 s gap, as without the connection's
/// progress. A real path's record starts at its build time, so a path dark from
/// its start is credited for one hold credit after it is built first.
#[test]
fn the_gap_runs_out_on_its_own_when_nothing_is_acknowledged() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let (sent, after) = drive(&time, LIMIT, async move {
            stuck_send(clock, 0, false, false, Duration::from_secs(120)).await
        })
        .await;
        assert!(
            matches!(sent, BoundedSend::Failed(SendFailure::TimedOut)),
            "{sent:?}"
        );
        assert_eq!(after, Duration::from_secs(5));
    })
}

/// An egress cap holding this connection's transmits counts as the connection
/// moving, as for the stall count, but only within `STALL_PROGRESS_WINDOW` of
/// the connection's last acknowledgement: a held transmit on a connection
/// nothing has acknowledged for the window is no evidence that it is alive
/// (a dark connection's own probes, held by a saturated cap, kept its send
/// going). One acknowledgement at 1 s, then a hold every second to 30 s:
/// the 2 s wake sees the acknowledgement, the 12 s wake counts the holds (10 s
/// after it), the 22 s wake does not, and the send ends at 22 s.
#[test]
fn a_cap_hold_counts_as_the_connection_moving() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let (sent, after) = drive(&time, LIMIT, async move {
            stuck_send(clock, 30, true, true, Duration::from_secs(120)).await
        })
        .await;
        assert!(
            matches!(sent, BoundedSend::Failed(SendFailure::TimedOut)),
            "{sent:?}"
        );
        assert_eq!(after, Duration::from_secs(22));
    })
}

/// A live send whose transmits wait in a deep cap queue longer than its gap,
/// with nothing of its own acknowledged yet, is credited by the connection's
/// last acknowledgement before it (the path's record, `last_acked`): its writes
/// stall from 0 to 8 s while the cap holds every second, the connection was
/// acknowledged at 0 s, its own first acknowledgement comes at 7 s, and it is
/// `Sent`. Credited only by this send's own acknowledgements, it timed out at
/// 5 s on a live path.
#[test]
fn a_live_send_held_in_the_cap_queue_past_its_gap_is_sent() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let sent = drive(&time, LIMIT, async move {
            let acks = recycle::AckProgress::detached();
            let cap = Arc::new(super::super::socket::CapHolds::new(Some(100_000)));
            let connection = bounded::ConnectionProgress {
                acks: acks.clone(),
                cap_holds: cap.clone(),
                quiet: Arc::default(),
                last_acked: Arc::new(bounded::AckRecord::new(acks.acked(), clock.now_nanos())),
            };
            let deadlines = SendDeadlines {
                open: Duration::from_secs(2),
                gap: Some(Duration::from_secs(5)),
                source_gap: None,
                total: Duration::from_secs(120),
            };
            let ticking = clock.clone();
            let queue = async move {
                for second in 1..=8 {
                    ticking.sleep(Duration::from_secs(1)).await;
                    cap.hold_one();
                    if second == 7 {
                        acks.ack();
                    }
                }
                never().await
            };
            let stepping = clock.clone();
            let send = bounded_send(&clock, deadlines, Some(connection), |progress| async move {
                progress.mark_opened();
                stepping.sleep(Duration::from_secs(8)).await;
                progress.step();
                Ok(())
            });
            tokio::select! {
                sent = send => sent,
                _ = queue => unreachable!("the queue never ends"),
            }
        })
        .await;
        assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
    })
}

/// An acknowledgement the path's record has not seen yet (it came after the
/// last settle or wake) is dated at the send's start, not taken as no news:
/// the record says the connection was last acknowledged at 0 s, the peer
/// acknowledged again at 7 s with nobody reading, and a send starts at 9 s
/// whose writes the 1 MB/s cap holds (hold credit 10 s) until 17 s. Its first
/// wake is its open deadline, 11 s. Dated at 0 s, the hold seen there is 11 s
/// past the last acknowledgement and earns nothing, so the send timed out at
/// its 5 s gap on a live path. Dated at its start, the holds are credited and
/// the send is `Sent`. (Starting at 8 s would not show it: the 10 s wake is
/// exactly one credit after 0 s, which still counts.)
#[test]
fn an_acknowledgement_the_record_has_not_seen_credits_the_next_send() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let (sent, record) = drive(&time, LIMIT, async move {
            let acks = recycle::AckProgress::detached();
            let cap = Arc::new(super::super::socket::CapHolds::new(Some(1_000_000)));
            assert!(
                super::route::hold_credit(&cap.read()) == super::route::STALL_PROGRESS_WINDOW,
                "premise: the credit is the window at this cap"
            );
            let record = Arc::new(bounded::AckRecord::new(acks.acked(), clock.now_nanos()));
            clock.sleep(Duration::from_secs(7)).await;
            acks.ack();
            clock.sleep(Duration::from_secs(2)).await;
            let connection = bounded::ConnectionProgress {
                acks: acks.clone(),
                cap_holds: cap.clone(),
                quiet: Arc::default(),
                last_acked: record.clone(),
            };
            let deadlines = SendDeadlines {
                open: Duration::from_secs(2),
                gap: Some(Duration::from_secs(5)),
                source_gap: None,
                total: Duration::from_secs(120),
            };
            let ticking = clock.clone();
            let queue = async move {
                loop {
                    ticking.sleep(Duration::from_secs(1)).await;
                    cap.hold_one();
                }
            };
            let stepping = clock.clone();
            let send = bounded_send(&clock, deadlines, Some(connection), |progress| async move {
                progress.mark_opened();
                stepping.sleep(Duration::from_secs(8)).await;
                progress.step();
                Ok(())
            });
            let sent = tokio::select! {
                sent = send => sent,
                () = queue => unreachable!("the queue never ends"),
            };
            (sent, record.seen())
        })
        .await;
        assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
        assert_eq!(
            record.at,
            Duration::from_secs(9).as_nanos() as u64,
            "the unseen acknowledgement is dated at the send's start"
        );
    })
}

/// The path's record moves only on news. A reading of the count below the
/// record's is stale: a settle holding one (a watch dated a newer count
/// meanwhile) may move the date forward but never back to an old proof's
/// `since`, which would take away the hold credit the newer acknowledgement
/// earned; and a watch holding one does not date anything. A fresh reading
/// that matches a proof may move the date back.
#[test]
fn a_stale_count_never_moves_the_ack_record_back() {
    use bounded::{AckRecord, AckSeen, NO_ACK};
    // A watch dated count 6 at 40 s; a settle then read 5 (stale) and its
    // stall count, from a proof of count 5, dates the last ack at 10 s.
    let mut seen = AckSeen { count: 6, at: 40 };
    seen.settle(Some(5), 10);
    assert_eq!(seen, AckSeen { count: 6, at: 40 }, "not moved back");
    seen.settle(Some(5), 50);
    assert_eq!(seen, AckSeen { count: 6, at: 50 }, "forward is fine");
    // A fresh reading that matches the proof moves it back.
    seen.settle(Some(6), 20);
    assert_eq!(seen, AckSeen { count: 6, at: 20 });
    let mut none = AckSeen {
        count: 0,
        at: NO_ACK,
    };
    none.settle(Some(3), 7);
    assert_eq!(none, AckSeen { count: 3, at: 7 });

    let record = AckRecord::new(6, 40);
    assert_eq!(
        record.date_unseen(5, 90),
        Some(40),
        "a stale count is no news"
    );
    assert_eq!(record.seen(), AckSeen { count: 6, at: 40 });
    assert_eq!(record.date_unseen(7, 90), Some(90), "a higher count is");
    assert_eq!(record.seen(), AckSeen { count: 7, at: 90 });
}

/// Holds with no acknowledgement during the send count for nothing: the send
/// fails at its own gap, as on a path gone dark whose probes the cap holds.
#[test]
fn a_cap_hold_without_an_acknowledgement_is_not_progress() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let (sent, after) = drive(&time, LIMIT, async move {
            stuck_send(clock, 30, true, false, Duration::from_secs(120)).await
        })
        .await;
        assert!(
            matches!(sent, BoundedSend::Failed(SendFailure::TimedOut)),
            "{sent:?}"
        );
        assert_eq!(after, Duration::from_secs(5));
    })
}

/// A send waiting for its own source gets no time from the connection's
/// progress, even after the connection's window has pushed its timer out. The
/// wake at 2 s sees acknowledgements and arms the timer for 12 s; a step at
/// 3 s and then a wait for the source end the send at its own gap, 8 s.
#[test]
fn a_send_waiting_for_its_source_ends_at_its_own_gap() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let (sent, after) = drive(&time, LIMIT, async move {
            let acks = recycle::AckProgress::detached();
            let connection = bounded::ConnectionProgress {
                acks: acks.clone(),
                cap_holds: Arc::new(super::super::socket::CapHolds::new(None)),
                quiet: Arc::default(),
                last_acked: Arc::new(bounded::AckRecord::new(acks.acked(), bounded::NO_ACK)),
            };
            let deadlines = SendDeadlines {
                open: Duration::from_secs(2),
                gap: Some(Duration::from_secs(5)),
                source_gap: None,
                total: Duration::from_secs(120),
            };
            let start = clock.now();
            let ticking = clock.clone();
            let peer = async move {
                loop {
                    ticking.sleep(Duration::from_secs(1)).await;
                    acks.ack();
                }
            };
            let stepping = clock.clone();
            let send = bounded_send(&clock, deadlines, Some(connection), |progress| async move {
                progress.mark_opened();
                stepping.sleep(Duration::from_secs(3)).await;
                progress.step();
                progress.waiting_on_source(true);
                never().await
            });
            let outcome = tokio::select! {
                outcome = send => outcome,
                () = peer => unreachable!("the peer never ends"),
            };
            (outcome, clock.now() - start)
        })
        .await;
        assert!(
            matches!(sent, BoundedSend::Failed(SendFailure::TimedOut)),
            "{sent:?}"
        );
        assert_eq!(after, Duration::from_secs(8));
    })
}

/// The watch takes its baseline when the send starts: acknowledgements and
/// cap holds from before the send are not progress during it, so a path that
/// was dark when the send began fails it at its own gap.
#[test]
fn progress_from_before_the_send_does_not_extend_its_gap() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let (sent, after) = drive(&time, LIMIT, async move {
            let acks = recycle::AckProgress::detached();
            let cap = Arc::new(super::super::socket::CapHolds::new(Some(10_000_000)));
            for _ in 0..3 {
                acks.ack();
            }
            cap.hold_one();
            cap.release_one();
            let connection = bounded::ConnectionProgress {
                acks: acks.clone(),
                cap_holds: cap,
                quiet: Arc::default(),
                last_acked: Arc::new(bounded::AckRecord::new(acks.acked(), bounded::NO_ACK)),
            };
            let deadlines = SendDeadlines {
                open: Duration::from_secs(2),
                gap: Some(Duration::from_secs(5)),
                source_gap: None,
                total: Duration::from_secs(120),
            };
            let start = clock.now();
            let sent = bounded_send(&clock, deadlines, Some(connection), |progress| async move {
                progress.mark_opened();
                never().await
            })
            .await;
            (sent, clock.now() - start)
        })
        .await;
        assert!(
            matches!(sent, BoundedSend::Failed(SendFailure::TimedOut)),
            "{sent:?}"
        );
        assert_eq!(after, Duration::from_secs(5));
    })
}

/// A send's quiet proof says whether any of its readings saw a cap hold live
/// ([`bounded::QuietProof::live_hold_seen`]): a hold live when the send starts
/// sets it, and a send that saw none leaves it clear.
#[test]
fn a_quiet_proof_says_whether_its_send_saw_a_live_hold() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let proofs = drive(&time, LIMIT, async move {
            let cap = Arc::new(super::super::socket::CapHolds::new(None));
            let quiet: Arc<parking_lot::Mutex<Option<bounded::QuietProof>>> = Arc::default();
            let deadlines = SendDeadlines {
                open: Duration::from_secs(2),
                gap: Some(Duration::from_secs(5)),
                source_gap: None,
                total: Duration::from_secs(120),
            };
            let mut proofs = Vec::new();
            for live in [true, false] {
                if live {
                    cap.hold_one();
                }
                let connection = bounded::ConnectionProgress {
                    acks: recycle::AckProgress::detached(),
                    cap_holds: cap.clone(),
                    quiet: quiet.clone(),
                    last_acked: Arc::new(bounded::AckRecord::new(0, bounded::NO_ACK)),
                };
                let sent =
                    bounded_send(&clock, deadlines, Some(connection), |progress| async move {
                        progress.mark_opened();
                        never().await
                    })
                    .await;
                assert!(
                    matches!(sent, BoundedSend::Failed(SendFailure::TimedOut)),
                    "{sent:?}"
                );
                if live {
                    cap.release_one();
                }
                proofs.push(
                    quiet
                        .lock()
                        .take()
                        .expect("a timed-out send leaves a proof"),
                );
            }
            proofs
        })
        .await;
        assert!(proofs[0].live_hold_seen, "a hold was live at the start");
        assert!(!proofs[1].live_hold_seen, "no hold was live");
    })
}

/// A hold that starts during the send and is read live by a wake sets the
/// proof's `live_hold_seen`, even though the proof's hold count then matches
/// (the wake read the new count) and the hold is released before the settle.
/// Starts at 3 s and the 5 s wake reads it; with nothing acknowledged during
/// the send the hold extends nothing, so that wake also ends the send at its
/// own gap, and the proof covers the whole send (`since` 0).
#[test]
fn a_hold_read_live_by_a_wake_voids_the_proof() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let (sent, after, proof) = drive(&time, LIMIT, async move {
            let cap = Arc::new(super::super::socket::CapHolds::new(None));
            let quiet: Arc<parking_lot::Mutex<Option<bounded::QuietProof>>> = Arc::default();
            let connection = bounded::ConnectionProgress {
                acks: recycle::AckProgress::detached(),
                cap_holds: cap.clone(),
                quiet: quiet.clone(),
                last_acked: Arc::new(bounded::AckRecord::new(0, bounded::NO_ACK)),
            };
            let deadlines = SendDeadlines {
                open: Duration::from_secs(2),
                gap: Some(Duration::from_secs(5)),
                source_gap: None,
                total: Duration::from_secs(120),
            };
            let start = clock.now();
            let holding = clock.clone();
            let hold = async move {
                holding.sleep(Duration::from_secs(3)).await;
                cap.hold_one();
                holding.sleep(Duration::from_secs(3)).await;
                cap.release_one();
                std::future::pending::<()>().await
            };
            let send = bounded_send(&clock, deadlines, Some(connection), |progress| async move {
                progress.mark_opened();
                never().await
            });
            let sent = tokio::select! {
                sent = send => sent,
                () = hold => unreachable!("the hold never ends"),
            };
            let proof = quiet
                .lock()
                .take()
                .expect("a timed-out send leaves a proof");
            (sent, clock.now() - start, proof)
        })
        .await;
        assert!(
            matches!(sent, BoundedSend::Failed(SendFailure::TimedOut)),
            "{sent:?}"
        );
        assert_eq!(
            after,
            Duration::from_secs(5),
            "its own gap: no ack, so the hold is no progress"
        );
        assert_eq!((proof.holds, proof.since), (1, 0));
        assert!(proof.live_hold_seen, "the 5 s wake read the hold live");
    })
}

/// `source_gap` bounds only the waits for the send's own source: a send
/// waiting 20 s for its source under a 30 s source gap goes on, and the same
/// send stuck 20 s on a write (not its source) fails at the 5 s gap.
#[test]
fn the_source_gap_applies_only_to_source_waits() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let (on_source, on_write) = drive(&time, LIMIT, async move {
            let deadlines = SendDeadlines {
                open: Duration::from_secs(2),
                gap: Some(Duration::from_secs(5)),
                source_gap: None,
                total: Duration::from_secs(120),
            }
            .with_source_gap(Duration::from_secs(30));
            let sleeps = clock.clone();
            let on_source = bounded_send(&clock, deadlines, None, |progress| async move {
                progress.mark_opened();
                progress.waiting_on_source(true);
                sleeps.sleep(Duration::from_secs(20)).await;
                progress.waiting_on_source(false);
                progress.step();
                Ok(())
            })
            .await;
            let start = clock.now();
            let sleeps = clock.clone();
            let on_write = bounded_send(&clock, deadlines, None, |progress| async move {
                progress.mark_opened();
                sleeps.sleep(Duration::from_secs(20)).await;
                Ok(())
            })
            .await;
            (on_source, (on_write, clock.now() - start))
        })
        .await;
        assert!(matches!(on_source, BoundedSend::Sent), "{on_source:?}");
        let (on_write, took) = on_write;
        assert!(
            matches!(on_write, BoundedSend::Failed(SendFailure::TimedOut)),
            "{on_write:?}"
        );
        assert_eq!(took, Duration::from_secs(5));
    })
}

/// A source wait that ends re-arms the timer at the send's own gap: the
/// source gap (30 s) armed during the wait does not carry over to a write
/// that stalls after the chunk arrives, which fails at its own gap (5 s
/// after the chunk), not 30 s after the last step before the wait.
#[test]
fn the_end_of_a_source_wait_rearms_the_timer_at_the_own_gap() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let (outcome, took) = drive(&time, LIMIT, async move {
            let deadlines = SendDeadlines {
                open: Duration::from_secs(2),
                gap: Some(Duration::from_secs(5)),
                source_gap: None,
                total: Duration::from_secs(120),
            }
            .with_source_gap(Duration::from_secs(30));
            let start = clock.now();
            let sleeps = clock.clone();
            let outcome = bounded_send(&clock, deadlines, None, |progress| async move {
                progress.mark_opened();
                // A write until 3 s: the loop wakes at the 2 s open deadline
                // and arms at the own gap (5 s).
                sleeps.sleep(Duration::from_secs(3)).await;
                progress.step();
                // A source wait from 3 s to 6 s: the 5 s timer fires during
                // it, and the loop arms at the source gap (33 s).
                progress.waiting_on_source(true);
                sleeps.sleep(Duration::from_secs(3)).await;
                progress.waiting_on_source(false);
                progress.step();
                // A write that stalls from 6 s.
                sleeps.sleep(Duration::from_secs(60)).await;
                Ok(())
            })
            .await;
            (outcome, clock.now() - start)
        })
        .await;
        assert!(
            matches!(outcome, BoundedSend::Failed(SendFailure::TimedOut)),
            "{outcome:?}"
        );
        // 6 s plus the own gap, where a timer left at the source gap would
        // fire at 33 s.
        assert_eq!(took, Duration::from_secs(11));
    })
}

/// A wait for the source wakes the timer loop only when the connection's
/// window has pushed the timer past the send's own gap: a send that reads
/// its source many times with no window arms its timer only a few times, not
/// once per read.
#[test]
fn source_waits_do_not_rearm_the_timer_without_an_extension() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let arms = drive(&time, LIMIT, async move {
            let connection = bounded::ConnectionProgress {
                acks: recycle::AckProgress::detached(),
                cap_holds: Arc::new(super::super::socket::CapHolds::new(None)),
                quiet: Arc::default(),
                last_acked: Arc::new(bounded::AckRecord::new(0, bounded::NO_ACK)),
            };
            let deadlines = SendDeadlines {
                open: Duration::from_secs(2),
                gap: Some(Duration::from_secs(5)),
                source_gap: None,
                total: Duration::from_secs(120),
            };
            let arms = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let seen = arms.clone();
            let _ = bounded_send(&clock, deadlines, Some(connection), |progress| async move {
                progress.mark_opened();
                for _ in 0..50 {
                    progress.waiting_on_source(true);
                    tokio::task::yield_now().await;
                    progress.waiting_on_source(false);
                    progress.step();
                }
                seen.store(progress.arms(), std::sync::atomic::Ordering::Relaxed);
                Ok(())
            })
            .await;
            arms.load(std::sync::atomic::Ordering::Relaxed)
        })
        .await;
        assert!(arms <= 2, "{arms} timers armed for 50 source reads");
    })
}

/// While the connection's window has pushed the timer past the send's own
/// gap, the first wait for the source wakes the loop to re-arm it at the gap,
/// and later ones do not: the armed deadline is already the gap, so re-arming
/// would arm the same one. The connection is acknowledged every second; the
/// 5 s wake sees it and arms the timer for 15 s; a write at 6 s, then 50
/// source reads between 6 s and 7 s. Before, each read re-armed the timer.
#[test]
fn source_waits_rearm_the_timer_once_while_it_is_extended() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let arms = drive(&time, LIMIT, async move {
            let acks = recycle::AckProgress::detached();
            let connection = bounded::ConnectionProgress {
                acks: acks.clone(),
                cap_holds: Arc::new(super::super::socket::CapHolds::new(None)),
                quiet: Arc::default(),
                last_acked: Arc::new(bounded::AckRecord::new(acks.acked(), bounded::NO_ACK)),
            };
            let deadlines = SendDeadlines {
                open: Duration::from_secs(2),
                gap: Some(Duration::from_secs(5)),
                source_gap: None,
                total: Duration::from_secs(120),
            };
            let ticking = clock.clone();
            let peer = async move {
                loop {
                    ticking.sleep(Duration::from_secs(1)).await;
                    acks.ack();
                }
            };
            let stepping = clock.clone();
            let counted = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let seen = counted.clone();
            let send = bounded_send(&clock, deadlines, Some(connection), |progress| async move {
                progress.mark_opened();
                stepping.sleep(Duration::from_secs(6)).await;
                progress.step();
                let before = progress.arms();
                for _ in 0..50 {
                    progress.waiting_on_source(true);
                    stepping.sleep(Duration::from_millis(10)).await;
                    progress.waiting_on_source(false);
                    stepping.sleep(Duration::from_millis(10)).await;
                }
                seen.store(
                    progress.arms() - before,
                    std::sync::atomic::Ordering::Relaxed,
                );
                Ok(())
            });
            let sent = tokio::select! {
                sent = send => sent,
                () = peer => unreachable!("the peer never ends"),
            };
            assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
            counted.load(std::sync::atomic::Ordering::Relaxed)
        })
        .await;
        assert!(arms <= 2, "{arms} timers armed for 50 source reads");
    })
}

/// Two sends on a dark connection share the path's proof slot: the first
/// starts at 0 s and times out at 5 s, the second starts at 3 s and times out
/// at 8 s, before the first's settle reads the slot. The proofs are of the
/// same counts, so the slot keeps the earlier `since` (0 s), and the first
/// send's settle still dates progress back to it. Replaced, it read 3 s,
/// which delays the path going suspect.
#[test]
fn a_later_proof_of_the_same_counts_keeps_the_earlier_since() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let proof = drive(&time, LIMIT, async move {
            let acks = recycle::AckProgress::detached();
            let quiet: Arc<parking_lot::Mutex<Option<bounded::QuietProof>>> = Arc::default();
            let record = Arc::new(bounded::AckRecord::new(acks.acked(), bounded::NO_ACK));
            let connection = bounded::ConnectionProgress {
                acks,
                cap_holds: Arc::new(super::super::socket::CapHolds::new(None)),
                quiet: quiet.clone(),
                last_acked: record,
            };
            let deadlines = SendDeadlines {
                open: Duration::from_secs(2),
                gap: Some(Duration::from_secs(5)),
                source_gap: None,
                total: Duration::from_secs(120),
            };
            let stuck = |progress: bounded::SendProgress| async move {
                progress.mark_opened();
                never().await
            };
            let first = bounded_send(&clock, deadlines, Some(connection.clone()), stuck);
            let later = clock.clone();
            let second = async move {
                later.sleep(Duration::from_secs(3)).await;
                bounded_send(&later, deadlines, Some(connection), stuck).await
            };
            let (first, second) = tokio::join!(first, second);
            for sent in [first, second] {
                assert!(
                    matches!(sent, BoundedSend::Failed(SendFailure::TimedOut)),
                    "{sent:?}"
                );
            }
            *quiet.lock()
        })
        .await
        .expect("a proof");
        assert_eq!(proof.since, 0, "{proof:?}");
    })
}

/// `total` still ends a send whose connection never stops moving.
#[test]
fn total_ends_a_send_on_a_connection_that_keeps_moving() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let (sent, after) = drive(&time, LIMIT, async move {
            stuck_send(clock, 1000, false, false, Duration::from_secs(60)).await
        })
        .await;
        assert!(
            matches!(sent, BoundedSend::Failed(SendFailure::TimedOut)),
            "{sent:?}"
        );
        assert_eq!(after, Duration::from_secs(60));
    })
}

/// The wait for the acknowledgement has no gap: it includes a send window of
/// backlog, so only `total` ends it. The open deadline ends a send that has
/// not opened, however long `total` is.
#[test]
fn the_gap_ends_at_the_end_of_the_stream_and_open_binds_first() {
    bounded(|| async move {
        let time = VirtualTime::new();
        let clock = time.clone();
        let deadlines = SendDeadlines {
            open: Duration::from_secs(2),
            gap: Some(Duration::from_secs(5)),
            source_gap: None,
            total: Duration::from_secs(60),
        };
        let (finished, finished_after, unopened, unopened_after) =
            drive(&time, LIMIT, async move {
                let start = clock.now();
                let finished = bounded_send(&clock, deadlines, None, |progress| async move {
                    progress.mark_opened();
                    progress.mark_finished();
                    never().await
                })
                .await;
                let finished_after = clock.now() - start;
                let start = clock.now();
                let unopened = bounded_send(&clock, deadlines, None, |_| never()).await;
                (finished, finished_after, unopened, clock.now() - start)
            })
            .await;
        assert!(
            matches!(
                finished,
                BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut)
            ),
            "{finished:?}"
        );
        assert_eq!(finished_after, Duration::from_secs(60));
        assert!(
            matches!(
                unopened,
                BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut))
            ),
            "{unopened:?}"
        );
        assert_eq!(unopened_after, Duration::from_secs(2));
    })
}

/// A connection that has ended opens nothing: `NotOpened`, so the caller may
/// send the message another way, with the error kept for attribution.
#[test]
fn a_send_on_a_lost_connection_is_not_opened() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-lost-open", QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        let (short, stream) = p
            .drive(async move {
                a.connection().close(QuicCloseCode::Downgrade);
                let short = a
                    .send_short_bounded(&time, SendDeadlines::short(4), b"data")
                    .await;
                let stream = a
                    .send_stream_bounded(
                        &time,
                        SendDeadlines::stream(4),
                        StreamId::next(),
                        None,
                        Bytes::from_static(b"data"),
                        None,
                    )
                    .await;
                (short, stream)
            })
            .await;
        for outcome in [short, stream] {
            assert!(
                matches!(
                    outcome,
                    BoundedSend::Failed(SendFailure::NotOpened(NotOpened::ConnectionLost(_)))
                ),
                "{outcome:?}"
            );
        }
    })
}

/// A send waiting for a permit ends when the connection does, with no wait for
/// its deadline. The permits here are held outside any send: a send holding one
/// would end on the close and free it, which would wake the waiter whether or
/// not it races the close, and the semaphore itself never closes.
#[test]
fn a_close_wakes_a_send_waiting_for_a_permit_at_once() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-permit-close", QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        let (outcome, waited) = p
            .drive(async move {
                let limit = super::super::config::STREAMED_SEND_LIMIT;
                let _held = Arc::clone(&a.stream_slots)
                    .acquire_many_owned(limit)
                    .await
                    .expect("never closed");
                let closes = a.clone();
                let close_time = time.clone();
                let close = async move {
                    close_time.sleep(Duration::from_secs(1)).await;
                    closes.connection().close(QuicCloseCode::Downgrade);
                    futures::future::pending::<()>().await;
                };
                let send = async {
                    let start = time.now();
                    let outcome = a
                        .send_stream_bounded(
                            &time,
                            Duration::from_secs(300),
                            StreamId::next(),
                            None,
                            Bytes::from_static(b"body"),
                            None,
                        )
                        .await;
                    (outcome, time.now() - start)
                };
                tokio::select! {
                    () = close => unreachable!("never returns"),
                    result = send => result,
                }
            })
            .await;
        assert!(
            matches!(
                outcome,
                BoundedSend::Failed(SendFailure::NotOpened(NotOpened::ConnectionLost(_)))
            ),
            "{outcome:?}"
        );
        assert!(
            waited < Duration::from_secs(2),
            "woken by the close at 1 s, not by a deadline, after {waited:?}"
        );
    })
}

/// A transfer slower than the gap but making progress is not a stall: each
/// write on a real path moves the gap, so it ends `Sent` well past 5 s.
#[test]
fn a_slow_source_that_keeps_writing_outlives_the_gap() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-slow-source", QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        let (outcome, took) = p
            .drive(async move {
                let start = time.now();
                let sleeps = time.clone();
                let chunks = Box::pin(futures::stream::iter(0..4).then(move |_| {
                    let sleeps = sleeps.clone();
                    async move {
                        sleeps.sleep(Duration::from_secs(3)).await;
                        Ok(Bytes::from_static(b"body"))
                    }
                }));
                let outcome = a
                    .send_stream_chunks_bounded(
                        &time,
                        SendDeadlines::stream(16),
                        StreamId::next(),
                        16,
                        None,
                        chunks,
                        None,
                    )
                    .await;
                (outcome, time.now() - start)
            })
            .await;
        assert!(matches!(outcome, BoundedSend::Sent), "{outcome:?}");
        assert!(took >= Duration::from_secs(12), "{took:?}");
    })
}

/// A source with nothing yet holds no permit and opens no stream, and past the
/// gap the send is `NotOpened`. A source that ends or fails first opens nothing.
#[test]
fn a_source_with_no_first_chunk_holds_nothing() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-first-chunk", QuicPathLimits::DEFAULT).await;
        let limit = super::super::config::STREAMED_SEND_LIMIT as usize;
        let (a, time) = (p.a.clone(), p.time());
        let (waiting, free_while_waiting, waited, ended, failed) = p
            .drive(async move {
                let start = time.now();
                let send = a.send_stream_chunks_bounded(
                    &time,
                    SendDeadlines::stream(1000),
                    StreamId::next(),
                    1000,
                    None,
                    futures::stream::pending::<Result<Bytes, StreamError>>(),
                    None,
                );
                tokio::pin!(send);
                tokio::select! {
                    outcome = &mut send => panic!("ended early: {outcome:?}"),
                    () = time.sleep(Duration::from_secs(1)) => {}
                }
                let free_while_waiting = a.free_stream_slots();
                let waiting = send.await;
                let waited = time.now() - start;
                let ended = a
                    .send_stream_chunks_bounded(
                        &time,
                        SendDeadlines::stream(1000),
                        StreamId::next(),
                        1000,
                        None,
                        futures::stream::empty::<Result<Bytes, StreamError>>(),
                        None,
                    )
                    .await;
                let failed = a
                    .send_stream_chunks_bounded(
                        &time,
                        SendDeadlines::stream(1000),
                        StreamId::next(),
                        1000,
                        None,
                        futures::stream::iter([Err(StreamError::InactivityTimeout)]),
                        None,
                    )
                    .await;
                (waiting, free_while_waiting, waited, ended, failed)
            })
            .await;
        assert_eq!(free_while_waiting, limit);
        assert!(
            matches!(
                waiting,
                BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut))
            ),
            "{waiting:?}"
        );
        assert_eq!(waited, Duration::from_secs(5));
        assert!(
            matches!(
                ended,
                BoundedSend::Failed(SendFailure::Error(QuicSendError::LengthMismatch {
                    declared: 1000,
                    delivered: 0
                }))
            ),
            "{ended:?}"
        );
        assert!(
            matches!(
                failed,
                BoundedSend::Failed(SendFailure::Error(QuicSendError::Source(_)))
            ),
            "{failed:?}"
        );
        assert_eq!(p.a.send_aborts(), 0, "no stream existed to reset");
        assert_eq!(p.b.stats().count(QuicInboundOutcome::AnnouncedStream), 0);
    })
}

/// A source that stalls after its first chunk is reset at the gap and returns
/// its permit, and the receiver discards the stream.
#[test]
fn a_source_stall_resets_at_the_gap_and_returns_the_permit() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-source-stall", QuicPathLimits::DEFAULT).await;
        let limit = super::super::config::STREAMED_SEND_LIMIT as usize;
        let (a, time) = (p.a.clone(), p.time());
        let (outcome, waited, held, free_after) = p
            .drive(async move {
                let start = time.now();
                let send = a.send_stream_chunks_bounded(
                    &time,
                    SendDeadlines::stream(1000),
                    StreamId::next(),
                    1000,
                    None,
                    stalls_after_first_chunk(),
                    None,
                );
                tokio::pin!(send);
                tokio::select! {
                    outcome = &mut send => panic!("ended early: {outcome:?}"),
                    () = time.sleep(Duration::from_secs(1)) => {}
                }
                let held = a.free_stream_slots();
                let outcome = send.await;
                let waited = time.now() - start;
                a.send_short(b"after").await.expect("still connected");
                (outcome, waited, held, a.free_stream_slots())
            })
            .await;
        assert!(
            matches!(outcome, BoundedSend::Failed(SendFailure::TimedOut)),
            "{outcome:?}"
        );
        assert_eq!(waited, Duration::from_secs(5));
        assert_eq!(held, limit - 1);
        assert_eq!(free_after, limit);
        assert_eq!(p.a.send_aborts(), 1);
        assert_eq!(p.b.stats().count(QuicInboundOutcome::ResetByPeer), 1);
    })
}

/// A write that flow control holds up is a gap too: the stream is reset at the
/// gap, with the connection still up.
#[test]
fn a_write_held_by_a_silent_peer_fails_at_the_gap() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-write-stall", QuicPathLimits::DEFAULT).await;
        let _silenced = DropFrom::new("quic-path-write-stall", &[p.pair.b_addr]);
        let (a, time) = (p.a.clone(), p.time());
        let body = over_window();
        let fragments = body.len().div_ceil(FRAGMENT_PAYLOAD_SIZE);
        let deadlines = SendDeadlines::stream(body.len() as u64);
        let (outcome, ticks) = p
            .drive(async move {
                let ticks = Fragments::default();
                let outcome = a
                    .send_stream_bounded(
                        &time,
                        deadlines,
                        StreamId::next(),
                        None,
                        body,
                        Some(&ticks),
                    )
                    .await;
                (outcome, ticks.count())
            })
            .await;
        assert!(
            matches!(outcome, BoundedSend::Failed(SendFailure::TimedOut)),
            "{outcome:?}"
        );
        assert!(ticks > 0 && ticks < fragments, "{ticks} of {fragments}");
        assert_eq!(p.a.send_aborts(), 1);
        assert!(p.a.connection().inner().close_reason().is_none());
    })
}

/// A live, busy path on a node whose total cap binds (100 kB/s, where one held
/// transmit can wait about 13.3 s, past the 10 s window), its peer reading and
/// acknowledging: send timeouts settled on it every 2 s, while a bulk stream
/// keeps the cap holding its transmits, neither stall-close it nor make it
/// suspect.
#[test]
fn a_live_path_under_a_binding_cap_is_not_stall_closed() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths_capped("quic-path-live-capped", 100_000).await;
        let (a, time) = (p.a.clone(), p.time());
        let holds = a.connection().cap_holds();
        let (held, suspect) = p
            .drive(async move {
                let busy = a.clone();
                let bulk = tokio::spawn(async move {
                    busy.send_stream(StreamId::next(), None, payload(4 * MIB).into())
                        .await
                        .ok();
                });
                let before = holds.read().holds;
                let mut suspect = false;
                for _ in 0..20 {
                    time.sleep(Duration::from_secs(2)).await;
                    a.settle(
                        SendKind::Stream,
                        &BoundedSend::Failed(SendFailure::TimedOut),
                        false,
                        time.now_nanos(),
                    );
                    suspect |= a.is_suspect();
                }
                bulk.abort();
                (holds.read().holds - before, suspect)
            })
            .await;
        assert!(held > 0, "premise: the cap held the path's transmits");
        assert!(!suspect, "a live path is never suspect");
        assert!(p.a.connection().inner().close_reason().is_none());
        assert_eq!(
            p.a.stats().send_event_count(QuicSendEvent::StalledClosed),
            0
        );
    })
}

/// How a real path carries its last acknowledgement to its next send: a settle
/// that sees the peer's acknowledgement records it on the path, and a later
/// send whose writes the cap queue holds for 8 s, past its 5 s gap, with the
/// peer silenced so nothing more is acknowledged, starts from that record and
/// is `Sent`. A send handed a fresh record, or a settle that wrote none, would
/// time out at 5 s. The short goes a credit after the path is built, so only
/// the settle's record can credit the holds.
#[test]
fn a_real_path_carries_its_last_acknowledgement_to_the_next_send() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-last-acked";
        let p = paths_capped(network, 100_000).await;
        let b_addr = p.pair.b_addr;
        let (a, time) = (p.a.clone(), p.time());
        let holds = a.connection().cap_holds();
        let credit = super::route::hold_credit(&holds.read());
        let sent = p
            .drive(async move {
                // Past one credit after the build, so the record's starting
                // value (the build time) no longer credits a hold.
                time.sleep(credit + Duration::from_secs(2)).await;
                a.send_short(b"live").await.expect("acknowledged");
                a.settle(SendKind::Short, &BoundedSend::Sent, false, time.now_nanos());
                let _silenced = DropFrom::new(network, &[b_addr]);
                let deadlines = SendDeadlines {
                    open: Duration::from_secs(2),
                    gap: Some(Duration::from_secs(5)),
                    source_gap: None,
                    total: Duration::from_secs(120),
                };
                let ticking = time.clone();
                let queue = async move {
                    loop {
                        ticking.sleep(Duration::from_secs(1)).await;
                        holds.hold_one();
                    }
                };
                let stepping = time.clone();
                let send = bounded_send(
                    &time,
                    deadlines,
                    a.connection_progress(),
                    |progress| async move {
                        progress.mark_opened();
                        stepping.sleep(Duration::from_secs(8)).await;
                        progress.step();
                        Ok(())
                    },
                );
                tokio::select! {
                    sent = send => sent,
                    _ = queue => unreachable!("the queue never ends"),
                }
            })
            .await;
        assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
    })
}

/// A path on a node whose total cap binds (100 kB/s, where one held
/// transmit can wait about 13.3 s, past the 10 s window) is not made suspect
/// while the cap holds its transmits and nothing is acknowledged for longer
/// than the window but less than the hold credit; once past the credit it is.
/// The peer acknowledges a short at t0 and is then silenced; the cap holds a
/// transmit of the connection (on its real record) every 2 s, and a send
/// timeout settles each time. To t0 + 12 s the hold is credited and no timeout
/// counts; without the credit (the window alone) it would be suspect at 12 s.
/// From the credit on the hold counts for nothing, and a window after the last
/// credited progress the path is suspect.
#[test]
fn an_unacknowledged_path_under_a_binding_cap_is_held_for_the_credit() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-unacked-capped";
        let p = paths_capped(network, 100_000).await;
        let b_addr = p.pair.b_addr;
        let (a, time) = (p.a.clone(), p.time());
        let holds = a.connection().cap_holds();
        let credit = super::route::hold_credit(&holds.read());
        let window = super::route::STALL_PROGRESS_WINDOW;
        assert!(
            credit > window + Duration::from_secs(2),
            "premise: {credit:?}"
        );
        let (suspect_within, suspect_after) = p
            .drive(async move {
                a.send_short(b"live").await.expect("acknowledged");
                a.settle(SendKind::Short, &BoundedSend::Sent, false, time.now_nanos());
                let t0 = time.now();
                let _silenced = DropFrom::new(network, &[b_addr]);
                let held_timeout = |a: &QuicPath, now: u64| {
                    holds.hold_one();
                    a.settle(
                        SendKind::Stream,
                        &BoundedSend::Failed(SendFailure::TimedOut),
                        false,
                        now,
                    );
                    a.is_suspect()
                };
                let mut suspect_within = false;
                while time.now() - t0 < window + Duration::from_secs(2) {
                    time.sleep(Duration::from_secs(2)).await;
                    suspect_within |= held_timeout(&a, time.now_nanos());
                }
                let mut suspect_after = false;
                while time.now() - t0 < credit + window + Duration::from_secs(4) {
                    time.sleep(Duration::from_secs(2)).await;
                    suspect_after |= held_timeout(&a, time.now_nanos());
                }
                (suspect_within, suspect_after)
            })
            .await;
        assert!(
            !suspect_within,
            "held past the window but within the credit: not suspect"
        );
        assert!(suspect_after, "past the credit the hold no longer counts");
        assert_eq!(
            p.a.stats().send_event_count(QuicSendEvent::StalledClosed),
            0
        );
    })
}

/// A path that goes dark mid-send, here once three quarters of the stream have
/// been handed to quinn (past the first send window's worth, so writes have
/// already waited for acknowledgements): the send's own earlier
/// acknowledgements extend its gap, so it fails after its 5 s gap but within
/// twice `STALL_PROGRESS_WINDOW` of the open deadline's wake (2 s), so 12 to
/// 22 s, here 12 s; and the [`bounded::QuietProof`] it leaves makes that first
/// timeout count, so the path goes suspect at once.
#[test]
fn a_path_dark_mid_send_fails_within_two_windows_and_goes_suspect() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-dark-mid-send";
        let p = paths(network, QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        let body = over_window();
        let fragments = body.len().div_ceil(FRAGMENT_PAYLOAD_SIZE);
        let silencer = Arc::new(SilenceAfter {
            network,
            from: p.pair.b_addr,
            after: fragments * 3 / 4,
            sent: std::sync::atomic::AtomicUsize::new(0),
            silenced: parking_lot::Mutex::new(None),
        });
        let watched = silencer.clone();
        let deadlines = SendDeadlines::stream(body.len() as u64);
        let (outcome, took) = p
            .drive(async move {
                let start = time.now();
                let outcome = a
                    .send_stream_bounded(
                        &time,
                        deadlines,
                        StreamId::next(),
                        None,
                        body,
                        Some(&*watched),
                    )
                    .await;
                (outcome, time.now() - start)
            })
            .await;
        assert!(silencer.silenced.lock().is_some(), "the path went dark");
        assert!(
            matches!(outcome, BoundedSend::Failed(SendFailure::TimedOut)),
            "{outcome:?}"
        );
        let window = super::route::STALL_PROGRESS_WINDOW;
        assert!(
            took > Duration::from_secs(5) && took <= Duration::from_secs(2) + 2 * window,
            "took {took:?}"
        );
        assert_eq!(took, Duration::from_secs(12));
        p.a.settle(SendKind::Stream, &outcome, false, p.time().now_nanos());
        assert!(p.a.is_suspect(), "the first timeout makes the path suspect");
        drop(silencer.silenced.lock().take());
    })
}

/// Other sends settling around a timeout do not cost it its quiet proof. An
/// unrelated `Sent` settles at 8 s (4 s before the
/// timeout), after the timed-out send's own acknowledgements, so the stall
/// count dates progress at 8 s; after the timeout at 12 s another unrelated
/// `Sent` settles first. The timed-out
/// send's settle still finds the proof (it is read, not taken), and the proof
/// dates the last progress back to 2 s although no settle sees a new
/// acknowledgement, so the path goes suspect at that first timeout.
#[test]
fn a_quiet_proof_survives_other_settles_and_dates_earlier_progress() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-proof-survives";
        let p = paths(network, QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        let body = over_window();
        let fragments = body.len().div_ceil(FRAGMENT_PAYLOAD_SIZE);
        let silencer = Arc::new(SilenceAfter {
            network,
            from: p.pair.b_addr,
            after: fragments * 3 / 4,
            sent: std::sync::atomic::AtomicUsize::new(0),
            silenced: parking_lot::Mutex::new(None),
        });
        let watched = silencer.clone();
        let deadlines = SendDeadlines::stream(body.len() as u64);
        let other = a.clone();
        let clock = time.clone();
        // The send's wakes: the open deadline at 2 s sees its acknowledgements,
        // and the next, a window later, sees nothing and ends it. The unrelated
        // settle falls between, after those acknowledgements.
        let timeout_at = Duration::from_secs(2) + super::route::STALL_PROGRESS_WINDOW;
        let unrelated_at = timeout_at - Duration::from_secs(4);
        let (outcome, took) = p
            .drive(async move {
                let start = time.now();
                let unrelated = async {
                    clock.sleep(unrelated_at).await;
                    other.settle(
                        SendKind::Short,
                        &BoundedSend::Sent,
                        false,
                        clock.now_nanos(),
                    );
                };
                let (outcome, ()) = tokio::join!(
                    a.send_stream_bounded(
                        &time,
                        deadlines,
                        StreamId::next(),
                        None,
                        body,
                        Some(&*watched),
                    ),
                    unrelated
                );
                (outcome, time.now() - start)
            })
            .await;
        assert!(
            matches!(outcome, BoundedSend::Failed(SendFailure::TimedOut)),
            "{outcome:?}"
        );
        assert_eq!(
            took, timeout_at,
            "the send timed out where the settles assume"
        );
        let now = p.time().now_nanos();
        p.a.settle(SendKind::Short, &BoundedSend::Sent, false, now);
        assert!(!p.a.is_suspect(), "a Sent never makes the path suspect");
        p.a.settle(SendKind::Stream, &outcome, false, now);
        assert!(p.a.is_suspect(), "the first timeout makes the path suspect");
        drop(silencer.silenced.lock().take());
    })
}

/// An accepted connection on a capped endpoint counts its own holds: the
/// server side's poller is built inside `establish` (`with_poller_holds`), not
/// with an unread record.
#[test]
fn an_accepted_connection_on_a_capped_endpoint_records_its_holds() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-accepted-holds";
        let mut p = paths_capped(network, 100_000).await;
        let c_addr: SocketAddr = p.net.addr(3);
        let (c_end, c_keys) = p.pair.third(c_addr).await;
        let c_watch = c_end.clone();
        let a_end = p.pair.a.clone();
        let permit = a_end
            .allow_peer(c_addr, c_keys.public())
            .expect("allow-set has room");
        let (a_addr, a_key) = (p.pair.a_addr, p.pair.a_keys.public().clone());
        let time = p.time();
        let holds = p
            .drive(async move {
                let (client, accepted) = tokio::join!(c_end.connect(a_addr, &a_key), async {
                    a_end
                        .accept()
                        .await
                        .expect("an incoming attempt")
                        .establish()
                        .await
                });
                let _client = client.expect("client handshake");
                let accepted = accepted.expect("server accepted");
                let mut send = accepted.inner().open_uni().await.expect("open stream");
                let writing = async {
                    send.write_all(&payload(MIB)).await.ok();
                };
                tokio::select! {
                    () = writing => {}
                    () = time.sleep(Duration::from_secs(2)) => {}
                }
                accepted.cap_holds().read().holds
            })
            .await;
        drop(permit);
        assert!(
            holds > 0,
            "the accepted connection's own record saw the cap hold it"
        );
        assert_eq!(
            [&p.pair.a, &p.pair.b, &c_watch].map(|end| end.fallback_pollers()),
            [0, 0, 0],
            "every poller got its connection's record"
        );
    })
}

/// Drops every datagram from `from` on `network` once `after` fragments of a
/// send have been handed to quinn.
struct SilenceAfter {
    network: &'static str,
    from: SocketAddr,
    after: usize,
    sent: std::sync::atomic::AtomicUsize,
    silenced: parking_lot::Mutex<Option<DropFrom>>,
}

impl crate::transport::boundary::StreamProgressObserver for SilenceAfter {
    fn on_fragment_sent(&self) {
        let sent = self.sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        if sent == self.after {
            *self.silenced.lock() = Some(DropFrom::new(self.network, &[self.from]));
        }
    }
}

/// Egress cap holds count per connection, and a hold extends a send only
/// within the window of an acknowledgement seen during it (a dark connection's
/// own held probes are no evidence it is alive): on a capped node whose cap a busy connection keeps saturated, a send
/// on another connection to a peer that has gone dark ends within a hold credit
/// (`route::hold_credit`) and a window of the connection's last
/// acknowledgement, however its own probes are held, and a later send on that
/// path, one credit after the first, ends at its own 5 s gap and makes the path
/// suspect. With one node-wide record, the
/// busy connection's holds read as the dark connection moving and the send ran
/// to `total` (300 s); with holds counted without an acknowledgement, its own
/// held probes kept it going (32 s on CI).
#[test]
fn a_dark_path_beside_a_cap_held_one_times_out_and_goes_suspect() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-path-held-beside-dark";
        let mut p = paths_capped(network, 100_000).await;
        let c_addr: SocketAddr = p.net.addr(3);
        let (c_end, c_keys) = p.pair.third(c_addr).await;
        let c_watch = c_end.clone();
        let permit = c_end
            .allow_peer(p.pair.a_addr, p.pair.a_keys.public())
            .expect("allow-set has room");
        let a_end = p.pair.a.clone();
        let c_key = c_keys.public().clone();
        let time = p.time();
        let (ac, _ca) = p
            .drive(async move {
                let (client, server) = tokio::join!(a_end.connect(c_addr, &c_key), async {
                    c_end
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
        let (ac, _ac_in) = QuicPath::with_limits(ac, time.clone(), QuicPathLimits::DEFAULT);
        let (_ca, _ca_in) = QuicPath::with_limits(_ca, time.clone(), QuicPathLimits::DEFAULT);
        let ac = Arc::new(ac);
        let ab = p.a.clone();
        let ab_holds = ab.connection().cap_holds();
        let ac_holds = ac.connection().cap_holds();
        let dark_holds = ac_holds.clone();
        let busy = ab.clone();
        let sender = ac.clone();
        let gap = Duration::from_secs(5);
        let (first, second, held_beside) = p
            .drive(async move {
                // Keep the cap saturated from the a-b connection throughout.
                let bulk = tokio::spawn(async move {
                    // Aborted before it ends; its outcome does not matter.
                    busy.send_stream(StreamId::next(), None, payload(4 * MIB).into())
                        .await
                        .ok();
                });
                time.sleep(Duration::from_secs(2)).await;
                let before = ab_holds.read().holds;
                let _silenced = DropFrom::new(network, &[c_addr]);
                let send = || async {
                    let start = time.now();
                    let body = over_window();
                    let outcome = sender
                        .send_stream_bounded(
                            &time,
                            SendDeadlines::stream(body.len() as u64),
                            StreamId::next(),
                            None,
                            body,
                            None,
                        )
                        .await;
                    let took = time.now() - start;
                    sender.settle(SendKind::Stream, &outcome, false, time.now_nanos());
                    (outcome, took, sender.is_suspect())
                };
                let first = send().await;
                // A send one hold credit after the first settle (the window, or
                // the longest one transmit can wait at this cap: about 13.3 s at
                // 100 kB/s): its timeout comes that and a gap later, with nothing
                // acknowledged, so the dark connection's own held probes no
                // longer count and the timeout does.
                time.sleep(super::route::hold_credit(&dark_holds.read()))
                    .await;
                let second = send().await;
                let held_beside = ab_holds.read().holds - before;
                bulk.abort();
                (first, second, held_beside)
            })
            .await;
        assert!(
            held_beside > 0,
            "the cap held the busy connection meanwhile"
        );
        let credit = super::route::hold_credit(&ac_holds.read());
        for (outcome, _, _) in [&first, &second] {
            assert!(
                matches!(outcome, BoundedSend::Failed(SendFailure::TimedOut)),
                "{outcome:?}"
            );
        }
        // The first is credited by the connection's last acknowledgement before
        // the silence, so its own held probes may extend it, but at most for
        // the credit and a window after that acknowledgement.
        assert!(
            first.1 <= credit + super::route::STALL_PROGRESS_WINDOW,
            "took {:?}, credit {credit:?}; the dark connection's own holds were {:?}",
            first.1,
            ac_holds.read()
        );
        // The second starts a credit after the first settle, so nothing credits
        // its holds: its own gap.
        assert_eq!(
            second.1,
            gap,
            "the dark connection's own holds were {:?}",
            ac_holds.read()
        );
        assert!(
            second.2,
            "suspect once a hold credit and a gap have passed with nothing \
             acknowledged (first timeout suspect: {})",
            first.2
        );
        assert_eq!(
            [&p.pair.a, &p.pair.b, &c_watch].map(|end| end.fallback_pollers()),
            [0, 0, 0],
            "every poller got its connection's record"
        );
    })
}

/// A permit stays held past `MaybeDelivered(TimedOut)`, because the stream
/// still holds its slot, and comes back on the acknowledgement.
#[test]
fn a_permit_outlives_a_timed_out_send_until_the_ack() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-permit-ack", QuicPathLimits::DEFAULT).await;
        let limit = super::super::config::STREAMED_SEND_LIMIT as usize;
        let silenced = DropFrom::new("quic-path-permit-ack", &[p.pair.b_addr]);
        let (a, time) = (p.a.clone(), p.time());
        let body = Bytes::from_static(b"body");
        let (outcome, held) = p
            .drive(async move {
                let outcome = a
                    .send_stream_bounded(
                        &time,
                        Duration::from_secs(1),
                        StreamId::next(),
                        None,
                        body,
                        None,
                    )
                    .await;
                time.sleep(Duration::from_secs(2)).await;
                (outcome, a.free_stream_slots())
            })
            .await;
        assert!(
            matches!(
                outcome,
                BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut)
            ),
            "{outcome:?}"
        );
        assert_eq!(held, limit - 1, "the stream still holds its slot");

        drop(silenced);
        let (a, time) = (p.a.clone(), p.time());
        let returned = p
            .drive(async move {
                for _ in 0..600 {
                    if a.free_stream_slots() == limit {
                        return true;
                    }
                    time.sleep(Duration::from_millis(100)).await;
                }
                false
            })
            .await;
        assert!(returned, "the acknowledgement returns the permit");
        assert_eq!(p.a.send_aborts(), 0, "nothing was reset");
        assert!(
            p.a.connection().inner().close_reason().is_none(),
            "the permit came back from the ack, not from the connection ending"
        );
    })
}

/// With no acknowledgement ever, a close returns the held permit, so the task
/// holding it ends with the connection.
#[test]
fn a_permit_held_past_a_timed_out_send_returns_on_close() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-permit-end", QuicPathLimits::DEFAULT).await;
        let limit = super::super::config::STREAMED_SEND_LIMIT as usize;
        let _silenced = DropFrom::new("quic-path-permit-end", &[p.pair.b_addr]);
        let (a, time) = (p.a.clone(), p.time());
        let (held, after) = p
            .drive(async move {
                let outcome = a
                    .send_short_bounded(&time, Duration::from_secs(1), &payload(5000))
                    .await;
                assert!(
                    matches!(
                        outcome,
                        BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut)
                    ),
                    "{outcome:?}"
                );
                let held = a.free_stream_slots();
                a.connection().close(QuicCloseCode::Downgrade);
                time.sleep(Duration::from_millis(10)).await;
                (held, a.free_stream_slots())
            })
            .await;
        assert_eq!(held, limit - 1, "a Short past one packet takes a permit");
        assert_eq!(after, limit);
    })
}

/// A one-packet Short takes no permit, but its send epoch stays held past
/// `MaybeDelivered(TimedOut)` until the acknowledgement, so a send window
/// lowered meanwhile stays charged while quinn may still hold its data.
#[test]
fn a_one_packet_short_holds_its_send_epoch_until_the_ack() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let windows = Windows {
            a: Some(test_budget(64 << 20, leaked_counters())),
            b: None,
        };
        let network = "quic-path-short-epoch";
        let p = paths_metered(network, windows).await;
        let limit = super::super::config::STREAMED_SEND_LIMIT as usize;
        let silenced = DropFrom::new(network, &[p.pair.b_addr]);
        let (a, time) = (p.a.clone(), p.time());
        let (outcome, slots, holders) = p
            .drive(async move {
                let outcome = a
                    .send_short_bounded(&time, Duration::from_secs(1), b"short")
                    .await;
                time.sleep(Duration::from_secs(2)).await;
                (outcome, a.free_stream_slots(), a.send_epoch_holders())
            })
            .await;
        assert!(
            matches!(
                outcome,
                BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut)
            ),
            "{outcome:?}"
        );
        assert_eq!(slots, limit, "premise: a one-packet Short takes no permit");
        assert_eq!(holders, 1, "the unacknowledged Short holds its epoch");

        drop(silenced);
        let (a, time) = (p.a.clone(), p.time());
        let released = p
            .drive(async move {
                for _ in 0..600 {
                    if a.send_epoch_holders() == 0 {
                        return true;
                    }
                    time.sleep(Duration::from_millis(100)).await;
                }
                false
            })
            .await;
        assert!(released, "the acknowledgement releases the epoch");
        assert!(
            p.a.connection().inner().close_reason().is_none(),
            "released by the ack, not by the connection ending"
        );
    })
}

/// A peer that never acknowledges cancelled one-packet Shorts cannot make
/// the path keep a task per Short: past the bound, a Short's epoch goes at
/// once and is counted, and its stream is reset so quinn drops its data with
/// it. Without the bound every one of them waits.
#[test]
fn cancelled_one_packet_shorts_wait_for_their_ack_only_up_to_the_bound() {
    const BOUND: usize = 4;
    const SHORTS: usize = BOUND + 3;
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let windows = Windows {
            a: Some(test_budget(64 << 20, leaked_counters())),
            b: None,
        };
        let network = "quic-path-ack-wait-bound";
        let p = paths_metered(network, windows).await;
        p.a.ack_waits().set_limit(BOUND);
        // One acknowledged send first, so the peer's stream credit for a data
        // path has arrived before its packets are dropped.
        let (a, time) = (p.a.clone(), p.time());
        let first = p
            .drive(async move {
                a.send_short_bounded(&time, Duration::from_secs(5), b"first")
                    .await
            })
            .await;
        assert!(matches!(first, BoundedSend::Sent), "{first:?}");
        let resets_before = p.a.connection().inner().stats().frame_tx.reset_stream;
        let _silenced = DropFrom::new(network, &[p.pair.b_addr]);
        let (a, time) = (p.a.clone(), p.time());
        let (outcomes, waiting, holders, released) = p
            .drive(async move {
                let mut outcomes = Vec::new();
                for _ in 0..SHORTS {
                    outcomes.push(
                        a.send_short_bounded(&time, Duration::from_secs(1), b"short")
                            .await,
                    );
                }
                time.sleep(Duration::from_secs(1)).await;
                (
                    outcomes,
                    a.ack_waits().waiting(),
                    a.send_epoch_holders(),
                    a.stats().send_event_count(QuicSendEvent::AckWaitReleased),
                )
            })
            .await;
        for outcome in &outcomes {
            assert!(
                matches!(
                    outcome,
                    BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut)
                ),
                "premise: every Short waited for an ack that never came: {outcome:?}"
            );
        }
        assert_eq!(waiting, BOUND, "at most the bound wait");
        assert_eq!(holders, BOUND, "the rest released their epochs at once");
        assert_eq!(released, (SHORTS - BOUND) as u64, "each release is counted");
        // quinn discards a stream's unacknowledged data when it resets it, and
        // sends a RESET_STREAM for it.
        let resets = p.a.connection().inner().stats().frame_tx.reset_stream - resets_before;
        assert_eq!(
            resets,
            (SHORTS - BOUND) as u64,
            "each released Short's stream was reset, so its data went with its epoch"
        );
        assert_eq!(
            p.a.send_aborts(),
            (SHORTS - BOUND) as u64,
            "each reset counts toward the abort limit, for its stopped-map entry"
        );
    })
}

/// A Short past one packet waits for a permit as a stream does, and one that
/// fits a packet does not.
#[test]
fn only_a_short_past_one_packet_takes_a_permit() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-short-permit", QuicPathLimits::FULL_STREAMS).await;
        let limit = super::super::config::STREAMED_SEND_LIMIT as usize;
        let (a, time) = (p.a.clone(), p.time());
        let (large, small) = p
            .drive(async move {
                let mut blocked = blocked_streamed_sends(&a, limit);
                let sends = async {
                    time.sleep(Duration::from_secs(1)).await;
                    let large = a
                        .send_short_bounded(&time, SendDeadlines::short(5000), &payload(5000))
                        .await;
                    let small = a
                        .send_short_bounded(&time, SendDeadlines::short(5), b"small")
                        .await;
                    (large, small)
                };
                tokio::select! {
                    done = blocked.next() => panic!("a blocked send returned: {done:?}"),
                    result = sends => result,
                }
            })
            .await;
        assert!(
            matches!(
                large,
                BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut))
            ),
            "{large:?}"
        );
        assert!(matches!(small, BoundedSend::Sent), "{small:?}");
    })
}

/// The observer hears one tick per fragment, however the bytes were chunked,
/// and the bytes arrive whole. Ticks are what keep a streaming PUT's retry
/// loop from reading an upload as stalled.
#[test]
fn ticks_equal_the_fragment_count() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-path-ticks", QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        let body = Bytes::from(payload(3 * FRAGMENT_PAYLOAD_SIZE + 1));
        let len = body.len() as u64;
        let (from_bytes, from_chunks, bytes_ticks, chunk_ticks) = p
            .drive({
                let body = body.clone();
                async move {
                    let bytes_ticks = Fragments::default();
                    let from_bytes = a
                        .send_stream_bounded(
                            &time,
                            SendDeadlines::stream(len),
                            StreamId::next(),
                            None,
                            body.clone(),
                            Some(&bytes_ticks),
                        )
                        .await;
                    let chunk_ticks = Fragments::default();
                    let one_chunk = futures::stream::iter([Ok(body)]);
                    let from_chunks = a
                        .send_stream_chunks_bounded(
                            &time,
                            SendDeadlines::stream(len),
                            StreamId::next(),
                            len,
                            None,
                            one_chunk,
                            Some(&chunk_ticks),
                        )
                        .await;
                    (
                        from_bytes,
                        from_chunks,
                        bytes_ticks.count(),
                        chunk_ticks.count(),
                    )
                }
            })
            .await;
        assert!(matches!(from_bytes, BoundedSend::Sent), "{from_bytes:?}");
        assert!(matches!(from_chunks, BoundedSend::Sent), "{from_chunks:?}");
        assert_eq!(bytes_ticks, 4);
        assert_eq!(chunk_ticks, 4);
        for _ in 0..2 {
            let arrival = p.b_in.streams.try_recv().expect("announced");
            let handle = IncomingStream::from(arrival.handle);
            let assembled = p
                .drive(async move { handle.assemble().await })
                .await
                .expect("whole");
            assert_eq!(&assembled[..], &body[..]);
        }
    })
}

/// `total` counts the wait for the first chunk, so a plain `Duration` is one
/// deadline for a whole send from a source.
#[test]
fn a_duration_bounds_a_send_from_a_source_including_the_first_chunk() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-source-total", QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        let (outcome, took) = p
            .drive(async move {
                let start = time.now();
                let sleeps = time.clone();
                let first = futures::stream::once(async move {
                    sleeps.sleep(Duration::from_secs(3)).await;
                    Ok(Bytes::from_static(b"first"))
                });
                let chunks = Box::pin(first.chain(futures::stream::pending()));
                let outcome = a
                    .send_stream_chunks_bounded(
                        &time,
                        Duration::from_secs(5),
                        StreamId::next(),
                        1000,
                        None,
                        chunks,
                        None,
                    )
                    .await;
                (outcome, time.now() - start)
            })
            .await;
        assert!(
            matches!(outcome, BoundedSend::Failed(SendFailure::TimedOut)),
            "{outcome:?}"
        );
        assert_eq!(took, Duration::from_secs(5));
    })
}

/// A close during the wait for the first chunk ends the send at once and says
/// so, as a close during the wait for a permit does.
#[test]
fn a_close_ends_the_wait_for_the_first_chunk_at_once() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-first-close", QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        let (outcome, took) = p
            .drive(async move {
                let closes = a.clone();
                let close_time = time.clone();
                let close = async move {
                    close_time.sleep(Duration::from_secs(1)).await;
                    closes.connection().close(QuicCloseCode::Downgrade);
                    futures::future::pending::<()>().await;
                };
                let send = async {
                    let start = time.now();
                    let outcome = a
                        .send_stream_chunks_bounded(
                            &time,
                            SendDeadlines::stream(1000),
                            StreamId::next(),
                            1000,
                            None,
                            futures::stream::pending::<Result<Bytes, StreamError>>(),
                            None,
                        )
                        .await;
                    (outcome, time.now() - start)
                };
                tokio::select! {
                    () = close => unreachable!("never returns"),
                    result = send => result,
                }
            })
            .await;
        assert!(
            matches!(
                outcome,
                BoundedSend::Failed(SendFailure::NotOpened(NotOpened::ConnectionLost(_)))
            ),
            "{outcome:?}"
        );
        assert!(took < Duration::from_secs(2), "{took:?}");
    })
}

/// The permit rule turns on one length: the largest Short that fits a packet
/// takes no permit, and one byte more does.
#[test]
fn the_permit_for_a_short_starts_just_past_one_packet() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let fits = (1..=FRAGMENT_PAYLOAD_SIZE as u64 * 2)
            .take_while(|len| fits_one_packet(*len))
            .last()
            .expect("something fits a packet") as usize;
        let p = paths("quic-path-short-boundary", QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        let (at, past) = p
            .drive(async move {
                let limit = super::super::config::STREAMED_SEND_LIMIT;
                let _held = Arc::clone(&a.stream_slots)
                    .acquire_many_owned(limit)
                    .await
                    .expect("never closed");
                let at = a
                    .send_short_bounded(&time, SendDeadlines::short(fits as u64), &payload(fits))
                    .await;
                let past = a
                    .send_short_bounded(
                        &time,
                        SendDeadlines::short(fits as u64 + 1),
                        &payload(fits + 1),
                    )
                    .await;
                (at, past)
            })
            .await;
        assert!(matches!(at, BoundedSend::Sent), "{at:?}");
        assert!(
            matches!(
                past,
                BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut))
            ),
            "{past:?}"
        );
    })
}

/// Records which stream each fragment belonged to.
struct Turns(u8, Arc<std::sync::Mutex<Vec<u8>>>);

impl StreamProgressObserver for Turns {
    fn on_fragment_sent(&self) {
        self.1.lock().unwrap().push(self.0);
    }
}

/// How far apart two fragments of one send may be, on one thread. Round robin
/// among `sends` writers is about `sends`; the margin is for the writers that
/// wait on credit and rejoin in another order. Without the yield it is 3503.
fn assert_writers_took_turns(log: &[u8], sends: u8, fragments: usize) {
    assert_eq!(log.len(), usize::from(sends) * fragments);
    for send in 0..sends {
        let seen: Vec<usize> = log
            .iter()
            .enumerate()
            .filter_map(|(at, of)| (*of == send).then_some(at))
            .collect();
        let widest = seen.windows(2).map(|at| at[1] - at[0]).max().unwrap_or(0);
        assert!(
            widest <= 4 * usize::from(sends),
            "send {send} waited {widest} fragments between two of its own"
        );
    }
}

/// Concurrent streams on one path share its send window fragment by fragment.
/// quinn gives a write all the free credit without yielding, so without the
/// yield in `take_turns` the streams run one after another: 3503 fragments
/// between two of one stream's in this shape, which at 1 Mbit/s would be about
/// 30 s against the 5 s gap the sender and the receiver allow.
///
/// On one thread only, because `drive` is. A multi-thread runtime wakes in
/// another order, so this does not speak for it.
#[test]
fn streams_on_a_path_take_turns_with_their_fragments() {
    bounded(|| async move {
        for (streams, network) in [(2u8, "quic-path-turns-2"), (4, "quic-path-turns-4")] {
            let _seed = GlobalRng::seed_guard(SEED);
            let mut p = paths(network, QuicPathLimits::DEFAULT).await;
            let log = Arc::new(std::sync::Mutex::new(Vec::new()));
            let body = Bytes::from(payload(4 * MIB));
            let mut sends = Vec::new();
            for stream in 0..streams {
                let (a, time, body, log) = (p.a.clone(), p.time(), body.clone(), log.clone());
                sends.push(tokio::spawn(async move {
                    let turns = Turns(stream, log);
                    a.send_stream_bounded(
                        &time,
                        Duration::from_secs(300),
                        StreamId::next(),
                        None,
                        body,
                        Some(&turns),
                    )
                    .await
                }));
            }
            let mut arrivals =
                std::mem::replace(&mut p.b_in.streams, tokio::sync::mpsc::channel(1).1);
            let reader = tokio::spawn(async move {
                let mut held = Vec::new();
                while let Some(arrival) = arrivals.recv().await {
                    held.push(arrival);
                }
            });
            p.drive(async move {
                for send in sends {
                    let sent = send.await.expect("the send ran");
                    assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
                }
            })
            .await;
            reader.abort();
            let log = log.lock().unwrap();
            assert_writers_took_turns(&log, streams, (4 * MIB).div_ceil(FRAGMENT_PAYLOAD_SIZE));
        }
    })
}

/// The same for Shorts past one packet, which write through their own loop.
#[test]
fn shorts_past_one_packet_take_turns_with_their_fragments() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let shorts: u8 = 4;
        let p = paths("quic-path-short-turns", QuicPathLimits::DEFAULT).await;
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let body = Arc::new(payload(4 * MIB));
        let mut sends = Vec::new();
        for short in 0..shorts {
            let (a, body, log) = (p.a.clone(), body.clone(), log.clone());
            sends.push(tokio::spawn(async move {
                let turns = Turns(short, log);
                a.send_short_with(&body, Some(&turns), SendProgress::new())
                    .await
            }));
        }
        p.drive(async move {
            for send in sends {
                send.await.expect("the send ran").expect("sent");
            }
        })
        .await;
        let log = log.lock().unwrap();
        assert_writers_took_turns(&log, shorts, (4 * MIB).div_ceil(FRAGMENT_PAYLOAD_SIZE));
    })
}

/// The end of a one-packet message leaves with its data, in one STREAM frame.
/// A yield after the last piece would send them apart, two ack-eliciting
/// datagrams for the commonest message on a path.
#[test]
fn a_one_packet_message_is_one_stream_frame() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-one-frame", QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        let frames = p
            .drive(async move {
                let before = a.connection().inner().stats().frame_tx.stream;
                let sent = a
                    .send_short_bounded(&time, SendDeadlines::short(5), b"small")
                    .await;
                assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
                a.connection().inner().stats().frame_tx.stream - before
            })
            .await;
        assert_eq!(frames, 1);
    })
}

/// So does the last fragment of a stream of two pieces while another send holds
/// a permit, which is when pieces yield: the first piece goes in one STREAM
/// frame, and the second leaves with the end of the stream, in another.
#[test]
fn the_end_of_a_stream_leaves_with_its_last_fragment() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-last-frame", QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        let body = Bytes::from(payload(FRAGMENT_PAYLOAD_SIZE + 100));
        let frames = p
            .drive(async move {
                let held = Arc::clone(&a.stream_slots)
                    .acquire_owned()
                    .await
                    .expect("never closed");
                let before = a.connection().inner().stats().frame_tx.stream;
                let sent = a
                    .send_stream_bounded(
                        &time,
                        SendDeadlines::stream(body.len() as u64),
                        StreamId::next(),
                        None,
                        body,
                        None,
                    )
                    .await;
                assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
                drop(held);
                a.connection().inner().stats().frame_tx.stream - before
            })
            .await;
        assert_eq!(frames, 2);
    })
}

/// A first chunk that arrives with no time left opens nothing, whichever of
/// the chunk and the timer the select took. Repeated, because the two land on
/// the same instant and the select draws which to poll first.
#[test]
fn a_first_chunk_at_the_deadline_opens_no_stream() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-first-late", QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        let outcomes = p
            .drive(async move {
                let mut outcomes = Vec::new();
                for _ in 0..32 {
                    let sleeps = time.clone();
                    let first = futures::stream::once(async move {
                        sleeps.sleep(Duration::from_secs(5)).await;
                        Ok(Bytes::from_static(b"late"))
                    });
                    outcomes.push(
                        a.send_stream_chunks_bounded(
                            &time,
                            Duration::from_secs(5),
                            StreamId::next(),
                            4,
                            None,
                            Box::pin(first),
                            None,
                        )
                        .await,
                    );
                }
                outcomes
            })
            .await;
        for outcome in &outcomes {
            assert!(
                matches!(
                    outcome,
                    BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut))
                ),
                "{outcome:?}"
            );
        }
        assert_eq!(p.a.send_aborts(), 0);
        assert_eq!(p.b.stats().count(QuicInboundOutcome::AnnouncedStream), 0);
    })
}

// The buffer budget (#5940, buffer-budget PR 3): a data path's windows are set
// from the node's budget (`budget`).

/// A budget of `total` bytes whose paths start at the test endpoints' window.
fn test_budget(
    total: u64,
    counters: &'static crate::transport::metrics::QuicDataCounters,
) -> Arc<super::super::budget::QuicBudget> {
    super::super::budget::QuicBudget::new(total, super::super::tests::TEST_WINDOW, counters)
}

fn leaked_counters() -> &'static crate::transport::metrics::QuicDataCounters {
    Box::leak(Box::new(crate::transport::metrics::QuicDataCounters::new()))
}

/// While the buffer budget throttles a connection, a stream that was making
/// progress when the throttle began does not go idle: its idle timer is
/// paused, since the slowdown is this node's decision. It still has its age
/// limit, and reaching it while paused counts `ThrottledPastAge`, not an
/// idle or age eviction.
#[test]
fn a_throttle_pauses_idle_and_the_age_limit_counts_apart() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let windows = Windows {
            a: Some(test_budget(64 << 20, leaked_counters())),
            b: Some(test_budget(64 << 20, leaked_counters())),
        };
        let p = paths_metered("quic-budget-throttle-idle", windows).await;
        let meter = p.b.meter().expect("b reads data under a budget").clone();
        let idle = streaming_handle_idle_timeout();
        let total = 10_000u64;
        let age = max_inbound_stream_age(total);
        assert!(
            age > idle * 3,
            "premise: the age limit is past several idle periods"
        );
        let (raw, time) = (p.a_raw(), p.time());
        let started = time.now_nanos();
        let code = p
            .drive(async move {
                let id = StreamId::next().0;
                let mut bytes = header(1, 1, id, total, 0);
                bytes.extend_from_slice(&payload(1000));
                let mut send = raw.open_uni().await.unwrap();
                send.write_all(&bytes).await.unwrap();
                time.sleep(Duration::from_millis(100)).await;
                // The throttle begins just after the stream's last progress
                // and lasts past its age limit.
                let now = time.now_nanos();
                meter.throttle_for_test(now, now + 2 * age.as_nanos() as u64);
                send.stopped().await.unwrap().map(|code| code.into_inner())
            })
            .await;
        assert_eq!(code, Some(u64::from(stop_code::EVICTED)));
        let waited = Duration::from_nanos(p.time().now_nanos() - started);
        assert!(
            waited >= age,
            "stopped at its age limit, not idle: {waited:?}"
        );
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::ThrottledPastAge), 1);
        assert_eq!(stats.shared_count(InboundStreamOutcome::EvictedIdle), 0);
        assert_eq!(stats.shared_count(InboundStreamOutcome::EvictedAge), 0);
    })
}

/// A stream's handle outlives its connection (the orphan registry, an op, a
/// pipe fork) without keeping the connection in the buffer budget: the
/// handle asks whether the path is throttled through a weak hold on the
/// meter, so once the path and its reader are gone the connection is
/// deregistered and its windows leave the budget, and the handle reads the
/// path as not throttled.
#[test]
fn a_closed_path_with_a_surviving_handle_leaves_the_budget() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let budget = test_budget(64 << 20, leaked_counters());
        let windows = Windows {
            a: Some(test_budget(64 << 20, leaked_counters())),
            b: Some(budget.clone()),
        };
        let p = paths_metered("quic-budget-weak-probe", windows).await;
        let meter = p.b.meter().expect("b reads data under a budget").clone();
        let (raw, time) = (p.a_raw(), p.time());
        p.drive(async move {
            let mut bytes = header(1, 1, StreamId::next().0, 10_000, 0);
            bytes.extend_from_slice(&payload(1000));
            let mut send = raw.open_uni().await.unwrap();
            send.write_all(&bytes).await.unwrap();
            time.sleep(Duration::from_millis(200)).await;
            // Kept unfinished, so the stream stays open until the close.
            std::mem::forget(send);
        })
        .await;
        let Paths {
            pair,
            a,
            b,
            mut b_in,
            ..
        } = p;
        let handle = b_in.streams.try_recv().expect("announced").handle;
        assert_eq!(budget.connections(), 1);
        let now = pair.time.now_nanos();
        meter.throttle_for_test(now, now + Duration::from_secs(3600).as_nanos() as u64);
        assert!(
            handle.quic_path_throttled(),
            "premise: the probe reads the meter"
        );
        drop(meter);
        b.close(QuicCloseCode::Downgrade);
        drop(b);
        drop(a);
        drive(&pair.time, LIMIT, {
            let time = pair.time.clone();
            async move { time.sleep(Duration::from_secs(30)).await }
        })
        .await;
        assert_eq!(
            budget.connections(),
            0,
            "a surviving handle keeps a closed connection in the budget"
        );
        assert!(!handle.quic_path_throttled());
        drop(handle);
    })
}

/// A stream the peer opened but sent no byte on (quinn opens every lower id
/// with a higher one) is not active when a throttle begins, so its idle timer
/// is not paused: it ends idle while the throttle lasts.
#[test]
fn a_stream_with_no_byte_is_not_paused_by_a_throttle() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let windows = Windows {
            a: Some(test_budget(64 << 20, leaked_counters())),
            b: Some(test_budget(64 << 20, leaked_counters())),
        };
        let p = paths_metered("quic-budget-throttle-empty", windows).await;
        let meter = p.b.meter().expect("b reads data under a budget").clone();
        let idle = streaming_handle_idle_timeout();
        let (raw, time) = (p.a_raw(), p.time());
        p.drive(async move {
            let _empty = raw.open_uni().await.unwrap();
            // A whole control frame on the next id, which opens the first.
            let mut later = raw.open_uni().await.unwrap();
            let mut frame = header(1, 2, 0, 4, 0);
            frame.extend_from_slice(&payload(4));
            later.write_all(&frame).await.unwrap();
            later.finish().unwrap();
            let now = time.now_nanos();
            meter.throttle_for_test(now, now + 10 * idle.as_nanos() as u64);
            time.sleep(idle * 2).await;
        })
        .await;
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::ThrottledPastAge), 0);
        assert!(
            stats.count(QuicInboundOutcome::IdleUncharged) >= 1,
            "the empty stream ended idle"
        );
    })
}

/// A pause lasts only while the throttle does: a stream whose idle timer a
/// throttle paused, and which reaches its age limit after the throttle
/// lapsed, is counted `EvictedAge`, not `ThrottledPastAge`.
#[test]
fn an_age_limit_after_the_throttle_lapsed_counts_as_age() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let windows = Windows {
            a: Some(test_budget(64 << 20, leaked_counters())),
            b: Some(test_budget(64 << 20, leaked_counters())),
        };
        let p = paths_metered("quic-budget-throttle-lapse", windows).await;
        let meter = p.b.meter().expect("b reads data under a budget").clone();
        let idle = streaming_handle_idle_timeout();
        let total = 10_000u64;
        let age = max_inbound_stream_age(total);
        // The stream's last byte at 7 s; paused at each idle period after it.
        let progress = Duration::from_secs(7);
        let periods = (age - progress).as_nanos() / idle.as_nanos();
        let last_pause = progress + idle * periods as u32;
        assert!(
            age > last_pause + Duration::from_secs(2),
            "premise: the age limit comes before the next idle period"
        );
        // The throttle lapses after the last pause, before the age limit.
        let lapse = last_pause + (age - last_pause) / 2;
        let (raw, time) = (p.a_raw(), p.time());
        let code = p
            .drive(async move {
                let start = time.now_nanos();
                let mut bytes = header(1, 1, StreamId::next().0, total, 0);
                bytes.extend_from_slice(&payload(1000));
                let mut send = raw.open_uni().await.unwrap();
                send.write_all(&bytes).await.unwrap();
                time.sleep(progress).await;
                send.write_all(&payload(1)).await.unwrap();
                time.sleep(Duration::from_millis(100)).await;
                meter.throttle_for_test(time.now_nanos(), start + lapse.as_nanos() as u64);
                send.stopped().await.unwrap().map(|code| code.into_inner())
            })
            .await;
        assert_eq!(code, Some(u64::from(stop_code::EVICTED)));
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::ThrottledPastAge), 0);
        assert_eq!(stats.shared_count(InboundStreamOutcome::EvictedAge), 1);
        assert_eq!(stats.shared_count(InboundStreamOutcome::EvictedIdle), 0);
    })
}

/// A stream that completes after a gap (its first data datagram lost, so a
/// later one is read first) counts for nothing toward a raise: the peer
/// saturates its 16 streams and all 16 complete, one gapped, so no raise.
/// The same run with nothing lost raises.
#[test]
fn a_gapped_completion_does_not_count_toward_a_raise() {
    use crate::transport::in_memory_socket::{
        PacketDeliveryDecision, set_packet_delivery_callback,
    };
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let start = super::super::config::DATA_START_UNI_STREAMS as usize;
        for (network, gapped, raises) in [
            ("quic-path-raise-gapped", true, 0),
            ("quic-path-raise-gap-free", false, 1),
        ] {
            let p = paths(network, QuicPathLimits::DEFAULT).await;
            let (raw, time, from) = (p.a_raw(), p.time(), p.pair.a_addr);
            let total = 4000u64;
            let net = network.to_string();
            p.drive(async move {
                let mut held = Vec::new();
                for _ in 0..start {
                    let mut send = raw.open_uni().await.unwrap();
                    let mut bytes = header(1, 1, StreamId::next().0, total, 0);
                    bytes.extend_from_slice(&payload(100));
                    send.write_all(&bytes).await.unwrap();
                    held.push(send);
                }
                time.sleep(Duration::from_millis(200)).await;
                let last = held.pop().unwrap();
                for mut send in held {
                    send.write_all(&payload(total as usize - 100))
                        .await
                        .unwrap();
                    send.finish().unwrap();
                }
                time.sleep(Duration::from_millis(200)).await;
                if gapped {
                    let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
                    set_packet_delivery_callback(
                        &net,
                        Some(Arc::new(move |_: &str, sender, _, bytes: &[u8]| {
                            if sender == from
                                && bytes.len() >= 1000
                                && !seen.swap(true, std::sync::atomic::Ordering::Relaxed)
                            {
                                PacketDeliveryDecision::Drop
                            } else {
                                PacketDeliveryDecision::Deliver
                            }
                        })),
                    );
                }
                let mut last = last;
                last.write_all(&payload(total as usize - 100))
                    .await
                    .unwrap();
                last.finish().unwrap();
                time.sleep(Duration::from_secs(3)).await;
                set_packet_delivery_callback(&net, None);
            })
            .await;
            let stats = p.b.stats();
            assert_eq!(
                stats.count(QuicInboundOutcome::CompletedStream) as usize,
                start,
                "{network}"
            );
            assert_eq!(
                stats.count(QuicInboundOutcome::StreamLimitRaised),
                raises,
                "{network}"
            );
        }
    })
}

/// A stream whose first packet arrives after a later one (here lost, so
/// its retransmission comes last) counts as gapped: the peer saturates its
/// 16 streams, the 16th opened by its second packet, and all 16 complete,
/// so no raise. A lost LAST packet is not counted (it leaves no hole to
/// see), which `a_gapped_completion_does_not_count_toward_a_raise`'s
/// gap-free case covers from the other side.
#[test]
fn a_late_first_packet_counts_as_a_gap() {
    use crate::transport::in_memory_socket::{
        PacketDeliveryDecision, set_packet_delivery_callback,
    };
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let start = super::super::config::DATA_START_UNI_STREAMS as usize;
        for (network, lost, raises) in [
            ("quic-path-raise-first-lost", true, 0),
            ("quic-path-raise-first-kept", false, 1),
        ] {
            let p = paths(network, QuicPathLimits::DEFAULT).await;
            let (raw, time, from) = (p.a_raw(), p.time(), p.pair.a_addr);
            let total = 4000u64;
            let net = network.to_string();
            p.drive(async move {
                let mut held = Vec::new();
                for _ in 0..start - 1 {
                    let mut send = raw.open_uni().await.unwrap();
                    let mut bytes = header(1, 1, StreamId::next().0, total, 0);
                    bytes.extend_from_slice(&payload(100));
                    send.write_all(&bytes).await.unwrap();
                    held.push(send);
                }
                time.sleep(Duration::from_millis(200)).await;
                if lost {
                    let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
                    set_packet_delivery_callback(
                        &net,
                        Some(Arc::new(move |_: &str, sender, _, bytes: &[u8]| {
                            if sender == from
                                && bytes.len() >= 1000
                                && !seen.swap(true, std::sync::atomic::Ordering::Relaxed)
                            {
                                PacketDeliveryDecision::Drop
                            } else {
                                PacketDeliveryDecision::Deliver
                            }
                        })),
                    );
                }
                let mut last = raw.open_uni().await.unwrap();
                let mut bytes = header(1, 1, StreamId::next().0, total, 0);
                bytes.extend_from_slice(&payload(total as usize));
                last.write_all(&bytes).await.unwrap();
                // Long enough for the retransmission, so all 16 are busy
                // at once before any completes.
                time.sleep(Duration::from_secs(2)).await;
                for mut send in held {
                    send.write_all(&payload(total as usize - 100))
                        .await
                        .unwrap();
                    send.finish().unwrap();
                }
                last.finish().unwrap();
                time.sleep(Duration::from_secs(3)).await;
                set_packet_delivery_callback(&net, None);
            })
            .await;
            let stats = p.b.stats();
            assert_eq!(
                stats.count(QuicInboundOutcome::CompletedStream) as usize,
                start,
                "{network}"
            );
            assert_eq!(
                stats.count(QuicInboundOutcome::StreamLimitRaised),
                raises,
                "{network}"
            );
        }
    })
}

/// A header refused mid-run (here a frame of an unknown version) starts a
/// pending raise of the stream limit over, end to end: the peer saturates
/// its 16 streams, three complete (quinn returns credit once more than an
/// eighth of the limit is freed), a refused stream takes a freed slot, and
/// the other 13 complete; 16 completions, but not since the failure, so no
/// raise. The same run with a valid control frame in that slot raises.
#[test]
fn a_refused_header_starts_a_pending_raise_over() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let start = super::super::config::DATA_START_UNI_STREAMS as usize;
        for (network, refused, raises) in [
            ("quic-path-raise-refused", true, 0),
            ("quic-path-raise-control", false, 1),
        ] {
            let p = paths(network, QuicPathLimits::DEFAULT).await;
            let (raw, time) = (p.a_raw(), p.time());
            let total = 2000u64;
            p.drive(async move {
                let mut held = Vec::new();
                for _ in 0..start {
                    let mut send = raw.open_uni().await.unwrap();
                    let mut bytes = header(1, 1, StreamId::next().0, total, 0);
                    bytes.extend_from_slice(&payload(100));
                    send.write_all(&bytes).await.unwrap();
                    held.push(send);
                }
                time.sleep(Duration::from_millis(200)).await;
                let finish = |mut send: quinn::SendStream| async move {
                    send.write_all(&payload(total as usize - 100))
                        .await
                        .unwrap();
                    send.finish().unwrap();
                };
                for send in held.drain(..3) {
                    finish(send).await;
                }
                time.sleep(Duration::from_millis(200)).await;
                let mut next = raw.open_uni().await.unwrap();
                let version = if refused { 9 } else { 1 };
                let mut bytes = header(version, 2, 0, 4, 0);
                bytes.extend_from_slice(&payload(4));
                next.write_all(&bytes).await.unwrap();
                next.finish().unwrap();
                time.sleep(Duration::from_millis(200)).await;
                for send in held {
                    finish(send).await;
                }
                time.sleep(Duration::from_millis(200)).await;
            })
            .await;
            let stats = p.b.stats();
            assert_eq!(
                stats.count(QuicInboundOutcome::CompletedStream) as usize,
                start,
                "{network}"
            );
            assert_eq!(
                stats.count(QuicInboundOutcome::StreamLimitRaised),
                raises,
                "{network}"
            );
        }
    })
}

/// A stream this node's consumer cancels counts neither way toward a raise,
/// end to end: the peer saturates its 16 streams, the consumer cancels one
/// (stopped `CANCELLED` at its next fragment), the other 15 complete, and a
/// 16th completion on a fresh stream raises. Holding the cancel against the
/// peer would have started its run over, and nothing would raise.
#[test]
fn a_local_cancel_keeps_a_pending_raise() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let start = super::super::config::DATA_START_UNI_STREAMS as usize;
        let mut p = paths("quic-path-raise-cancel", QuicPathLimits::DEFAULT).await;
        let (raw, time) = (p.a_raw(), p.time());
        let total = 4 * FRAGMENT_PAYLOAD_SIZE;
        let mut held = p
            .drive({
                let (raw, time) = (raw.clone(), time.clone());
                async move {
                    let mut held = Vec::new();
                    for _ in 0..start {
                        let mut send = raw.open_uni().await.unwrap();
                        let mut bytes = header(1, 1, StreamId::next().0, total as u64, 0);
                        bytes.extend_from_slice(&payload(100));
                        send.write_all(&bytes).await.unwrap();
                        held.push(send);
                    }
                    time.sleep(Duration::from_millis(200)).await;
                    held
                }
            })
            .await;
        let first = p.b_in.streams.try_recv().expect("announced");
        first.handle.cancel();
        let mut cancelled = held.remove(0);
        p.drive(async move {
            if let Err(quinn::WriteError::Stopped(code)) =
                cancelled.write_all(&payload(total - 100)).await
            {
                assert_eq!(code.into_inner(), u64::from(stop_code::CANCELLED));
            }
            time.sleep(Duration::from_millis(200)).await;
            for mut send in held {
                send.write_all(&payload(total - 100)).await.unwrap();
                send.finish().unwrap();
            }
            time.sleep(Duration::from_millis(200)).await;
            let mut send = raw.open_uni().await.unwrap();
            let mut bytes = header(1, 1, StreamId::next().0, total as u64, 0);
            bytes.extend_from_slice(&payload(total));
            send.write_all(&bytes).await.unwrap();
            send.finish().unwrap();
            time.sleep(Duration::from_millis(500)).await;
        })
        .await;
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::CancelledLocally), 1);
        assert_eq!(
            stats.count(QuicInboundOutcome::CompletedStream) as usize,
            start
        );
        assert_eq!(stats.count(QuicInboundOutcome::StreamLimitRaised), 1);
    })
}

/// A frame kind for the raise tests below, written by hand: `Short` is kind
/// 0 with no stream id, `Stream` kind 1 with a fresh id.
#[derive(Clone, Copy)]
enum Framed {
    Short,
    Stream,
}

impl Framed {
    fn header(self, total: usize, metadata_len: u32) -> Vec<u8> {
        match self {
            Framed::Short => header(1, 0, 0, total as u64, 0),
            Framed::Stream => header(1, 1, StreamId::next().0, total as u64, metadata_len),
        }
    }
}

/// The body length of the held and completing streams in the raise tests:
/// more than one packet, so a Short of it is charged.
const RAISE_TOTAL: usize = 4 * FRAGMENT_PAYLOAD_SIZE;

/// Open `count` streams of `kind`, each with its header and 100 bytes, and
/// hold them open.
async fn hold_open(raw: &quinn::Connection, kind: Framed, count: usize) -> Vec<quinn::SendStream> {
    let mut held = Vec::new();
    for _ in 0..count {
        let mut send = raw.open_uni().await.unwrap();
        let mut bytes = kind.header(RAISE_TOTAL, 0);
        bytes.extend_from_slice(&payload(100));
        send.write_all(&bytes).await.unwrap();
        held.push(send);
    }
    held
}

/// Write one whole stream of `kind` and `total` bytes, and finish it.
async fn send_whole(raw: &quinn::Connection, kind: Framed, total: usize) {
    let mut send = raw.open_uni().await.unwrap();
    let mut bytes = kind.header(total, 0);
    bytes.extend_from_slice(&payload(total));
    send.write_all(&bytes).await.unwrap();
    send.finish().unwrap();
}

/// The end of each raise test below: the held streams complete, then one
/// more stream of `kind` completes alone. With the base's worth of streams
/// busy at once before (the held ones and the stream the test ended), that
/// raises the limit if, and only if, the ended stream did not start the run
/// over: had it voted `Failed`, the held completions count for nothing, and
/// the last stream, alone, does not saturate the limit.
async fn complete_held_then_one(
    held: Vec<quinn::SendStream>,
    raw: quinn::Connection,
    time: VirtualTime,
    kind: Framed,
) {
    for mut send in held {
        send.write_all(&payload(RAISE_TOTAL - 100)).await.unwrap();
        send.finish().unwrap();
    }
    time.sleep(Duration::from_millis(500)).await;
    send_whole(&raw, kind, RAISE_TOTAL).await;
    time.sleep(Duration::from_millis(500)).await;
}

/// A Short dropped because the node's consumer of Shorts is gone votes
/// neutral (the closed-channel arm, and the arm where the consumer goes
/// while the Short waits for room): the stream limit is still raised.
#[test]
fn a_short_whose_consumer_is_gone_keeps_a_pending_raise() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let start = super::super::config::DATA_START_UNI_STREAMS as usize;
        for (network, waits) in [
            ("quic-path-raise-short-closed", false),
            ("quic-path-raise-short-gone-waiting", true),
        ] {
            let limits = QuicPathLimits {
                short_capacity: 1,
                ..QuicPathLimits::DEFAULT
            };
            let mut p = paths(network, limits).await;
            let (raw, time) = (p.a_raw(), p.time());
            if waits {
                // Fills the channel; it completes before the limit is
                // saturated, so it counts for nothing. Two control frames
                // after it free enough streams (more than an eighth of the
                // limit) for quinn to return their credit.
                p.drive({
                    let (raw, time, a) = (raw.clone(), time.clone(), p.a.clone());
                    async move {
                        send_whole(&raw, Framed::Short, 100).await;
                        a.send_control(b"one").await.unwrap();
                        a.send_control(b"two").await.unwrap();
                        time.sleep(Duration::from_millis(200)).await;
                    }
                })
                .await;
            } else {
                p.b_in.shorts = tokio::sync::mpsc::channel(1).1;
            }
            let held = p
                .drive({
                    let (raw, time) = (raw.clone(), time.clone());
                    async move {
                        let held = hold_open(&raw, Framed::Stream, start - 1).await;
                        time.sleep(Duration::from_millis(200)).await;
                        send_whole(&raw, Framed::Short, 100).await;
                        time.sleep(Duration::from_millis(200)).await;
                        held
                    }
                })
                .await;
            if waits {
                assert_eq!(
                    p.b.stats().count(QuicInboundOutcome::ShortDroppedFull),
                    0,
                    "{network}: the Short waits for room"
                );
                p.b_in.shorts = tokio::sync::mpsc::channel(1).1;
            }
            p.drive(complete_held_then_one(held, raw, time, Framed::Stream))
                .await;
            let stats = p.b.stats();
            assert_eq!(
                stats.count(QuicInboundOutcome::ShortDroppedFull),
                1,
                "{network}"
            );
            assert_eq!(
                stats.count(QuicInboundOutcome::CompletedStream) as usize,
                start,
                "{network}"
            );
            assert_eq!(
                stats.count(QuicInboundOutcome::StreamLimitRaised),
                1,
                "{network}"
            );
        }
    })
}

/// A Stream dropped because the node's consumer of new streams is gone
/// votes neutral (the closed-channel arm, and the arm where the consumer
/// goes while the stream waits for room): the stream limit is still raised.
#[test]
fn a_stream_whose_consumer_is_gone_keeps_a_pending_raise() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let start = super::super::config::DATA_START_UNI_STREAMS as usize;
        for (network, waits) in [
            ("quic-path-raise-stream-closed", false),
            ("quic-path-raise-stream-gone-waiting", true),
        ] {
            let limits = QuicPathLimits {
                stream_capacity: 1,
                ..QuicPathLimits::DEFAULT
            };
            let mut p = paths(network, limits).await;
            let (raw, time) = (p.a_raw(), p.time());
            if waits {
                // Fills the channel; it completes before the limit is
                // saturated, so it counts for nothing. Two control frames
                // after it free enough streams (more than an eighth of the
                // limit) for quinn to return their credit.
                p.drive({
                    let (raw, time, a) = (raw.clone(), time.clone(), p.a.clone());
                    async move {
                        send_whole(&raw, Framed::Stream, 100).await;
                        a.send_control(b"one").await.unwrap();
                        a.send_control(b"two").await.unwrap();
                        time.sleep(Duration::from_millis(200)).await;
                    }
                })
                .await;
            } else {
                p.b_in.streams = tokio::sync::mpsc::channel(1).1;
            }
            let held = p
                .drive({
                    let (raw, time) = (raw.clone(), time.clone());
                    async move {
                        let held = hold_open(&raw, Framed::Short, start - 1).await;
                        time.sleep(Duration::from_millis(200)).await;
                        send_whole(&raw, Framed::Stream, 100).await;
                        time.sleep(Duration::from_millis(200)).await;
                        held
                    }
                })
                .await;
            if waits {
                assert_eq!(
                    p.b.stats().count(QuicInboundOutcome::StreamDroppedFull),
                    0,
                    "{network}: the stream waits for room"
                );
                p.b_in.streams = tokio::sync::mpsc::channel(1).1;
            }
            p.drive(complete_held_then_one(held, raw, time, Framed::Short))
                .await;
            let stats = p.b.stats();
            assert_eq!(
                stats.count(QuicInboundOutcome::StreamDroppedFull),
                1,
                "{network}"
            );
            assert_eq!(
                stats.count(QuicInboundOutcome::DeliveredShort) as usize,
                start,
                "{network}"
            );
            assert_eq!(
                stats.count(QuicInboundOutcome::StreamLimitRaised),
                1,
                "{network}"
            );
        }
    })
}

/// A Stream whose handle is cancelled before its announce votes neutral
/// where the reader finds the cancel: feeding payload that arrived while
/// the metadata was still missing (the metadata arm), or payload staged
/// before the header (the staged-feed arm). A packet of the stream is lost
/// so that payload comes first. The stream limit is still raised.
#[test]
fn a_stream_cancelled_before_its_announce_keeps_a_pending_raise() {
    use crate::transport::in_memory_socket::{
        PacketDeliveryDecision, set_packet_delivery_callback,
    };
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let start = super::super::config::DATA_START_UNI_STREAMS as usize;
        for (network, metadata) in [
            ("quic-path-raise-cancel-staged", false),
            ("quic-path-raise-cancel-metadata", true),
        ] {
            let cancel: &'static std::sync::atomic::AtomicBool =
                Box::leak(Box::new(std::sync::atomic::AtomicBool::new(false)));
            let limits = QuicPathLimits {
                cancel_next_admitted: Some(cancel),
                ..QuicPathLimits::DEFAULT
            };
            let p = paths(network, limits).await;
            let (raw, time, from) = (p.a_raw(), p.time(), p.pair.a_addr);
            let net = network.to_string();
            let held = p
                .drive({
                    let (raw, time) = (raw.clone(), time.clone());
                    async move {
                        let held = hold_open(&raw, Framed::Stream, start - 1).await;
                        time.sleep(Duration::from_millis(200)).await;
                        cancel.store(true, std::sync::atomic::Ordering::Relaxed);
                        let mut send = raw.open_uni().await.unwrap();
                        let meta_len = MAX_METADATA_BYTES as usize;
                        let mut first = Framed::Stream
                            .header(RAISE_TOTAL, if metadata { meta_len as u32 } else { 0 });
                        if metadata {
                            // The header and a little of the metadata, alone,
                            // so the header arrives and the metadata is missing.
                            first.extend_from_slice(&payload(10));
                            send.write_all(&first).await.unwrap();
                            time.sleep(Duration::from_millis(200)).await;
                        }
                        // The first full-size packet from here on (the rest
                        // of the metadata, or the header) is lost once, so
                        // the payload after it arrives first.
                        let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
                        set_packet_delivery_callback(
                            &net,
                            Some(Arc::new(move |_: &str, sender, _, bytes: &[u8]| {
                                if sender == from
                                    && bytes.len() >= 1000
                                    && !seen.swap(true, std::sync::atomic::Ordering::Relaxed)
                                {
                                    PacketDeliveryDecision::Drop
                                } else {
                                    PacketDeliveryDecision::Deliver
                                }
                            })),
                        );
                        let mut rest = if metadata {
                            payload(meta_len - 10)
                        } else {
                            first
                        };
                        rest.extend_from_slice(&payload(RAISE_TOTAL));
                        send.write_all(&rest).await.unwrap();
                        send.finish().unwrap();
                        time.sleep(Duration::from_secs(2)).await;
                        set_packet_delivery_callback(&net, None);
                        held
                    }
                })
                .await;
            let stats = p.b.stats();
            assert!(
                !cancel.load(std::sync::atomic::Ordering::Relaxed),
                "{network}: premise: the stream was admitted"
            );
            assert_eq!(
                stats.count(QuicInboundOutcome::CancelledLocally),
                1,
                "{network}"
            );
            assert_eq!(
                stats.count(QuicInboundOutcome::AnnouncedStream) as usize,
                start - 1,
                "{network}: the cancelled stream is not announced"
            );
            p.drive(complete_held_then_one(held, raw, time, Framed::Stream))
                .await;
            let stats = p.b.stats();
            assert_eq!(
                stats.count(QuicInboundOutcome::CompletedStream) as usize,
                start,
                "{network}"
            );
            assert_eq!(
                stats.count(QuicInboundOutcome::StreamLimitRaised),
                1,
                "{network}"
            );
        }
    })
}

/// On a real connection, a stream whose in-order delivery fills the receive
/// window grows it, and the node's charge stays within the budget.
#[test]
fn a_data_paths_window_grows_when_delivery_fills_it() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let counters = leaked_counters();
        let total = 64 << 20;
        let windows = Windows {
            a: Some(test_budget(total, leaked_counters())),
            b: Some(test_budget(total, counters)),
        };
        let network = "quic-budget-window-grows";
        // 20 ms each way, so the window is what limits the transfer. Set
        // before the handshake, so the loss gate's base round trip has it.
        crate::transport::in_memory_socket::set_packet_delivery_callback(
            network,
            Some(Arc::new(|_, _, _, _| {
                crate::transport::in_memory_socket::PacketDeliveryDecision::DelayedDelivery(
                    Duration::from_millis(20),
                )
            })),
        );
        let p = paths_metered(network, windows).await;
        let a = p.a.clone();
        let body = Bytes::from(payload(24 * MIB));
        let sent = body.clone();
        p.drive(async move {
            a.send_stream(StreamId::next(), None, sent)
                .await
                .expect("sent")
        })
        .await;
        let mut p = p;
        let arrival = p.b_in.streams.try_recv().expect("announced");
        assert_eq!(arrival.handle.assemble().await.expect("assembled"), body);
        let snap = counters.snapshot(false);
        assert!(snap.window_grew > 0, "the receive window grew: {snap:?}");
        // The budget holds only growth above the floor grants.
        assert!(
            snap.budget_charged_peak > 0,
            "the growth was charged: {snap:?}"
        );
        assert!(snap.budget_charged_peak <= total);
        assert!(
            p.b.inbound_staging_cap()
                > super::super::budget::staging_cap(super::super::tests::TEST_WINDOW),
            "the staging cap followed the grown window: {}",
            p.b.inbound_staging_cap()
        );
        assert_eq!(
            p.b.stats().count(QuicInboundOutcome::EvictedStaged),
            0,
            "an honest transfer stays under the staging cap"
        );
    })
}

/// A connection starts with `config::INITIAL_UNI_STREAMS` streams for its peer;
/// a path that accepts data raises that to `config::DATA_START_UNI_STREAMS`
/// (or the start its limits give), and a shadow path leaves it.
#[test]
fn only_a_path_that_takes_data_lets_its_peer_open_streams() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        for (limits, network, expected) in [
            (
                QuicPathLimits::DEFAULT,
                "quic-path-streams-data",
                super::super::config::DATA_START_UNI_STREAMS,
            ),
            (
                QuicPathLimits::FULL_STREAMS,
                "quic-path-streams-full",
                super::super::config::MAX_UNI_STREAMS,
            ),
            (
                QuicPathLimits::SHADOW,
                "quic-path-streams-shadow",
                super::super::config::INITIAL_UNI_STREAMS,
            ),
        ] {
            let p = paths(network, limits).await;
            let (raw, time) = (p.a_raw(), p.time());
            let opened = p
                .drive(async move {
                    // Let the raise reach the peer.
                    time.sleep(Duration::from_millis(200)).await;
                    let mut held = Vec::new();
                    while held.len() < 100 {
                        match futures::FutureExt::now_or_never(raw.open_uni()) {
                            Some(stream) => held.push(stream.expect("open")),
                            None => break,
                        }
                    }
                    held.len()
                })
                .await;
            assert_eq!(opened, expected as usize, "{network}");
        }
    })
}

/// Send `bytes` on a new raw uni stream of `raw` with its first datagram lost
/// (and every resend of it, while the callback stays), so the receiver reads
/// what follows before the stream's header. The stop code the sender saw, or
/// `None` if the connection was closed first.
async fn send_without_its_first_datagram(
    network: &str,
    from: SocketAddr,
    raw: quinn::Connection,
    bytes: Vec<u8>,
) -> Option<u64> {
    use crate::transport::in_memory_socket::{
        PacketDeliveryDecision, set_packet_delivery_callback,
    };
    let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    set_packet_delivery_callback(
        network,
        Some(Arc::new(move |_, sender, _, _| {
            if sender == from && seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                PacketDeliveryDecision::Drop
            } else {
                PacketDeliveryDecision::Deliver
            }
        })),
    );
    let mut send = raw.open_uni().await.expect("open");
    let stopped = match send.write_all(&bytes).await {
        Err(quinn::WriteError::Stopped(code)) => Some(code.into_inner()),
        // The receiver closed the connection (OVER_BUDGET) right after the stop.
        Err(quinn::WriteError::ConnectionLost(_)) => None,
        Err(error) => panic!("write failed: {error}"),
        Ok(()) => send
            .stopped()
            .await
            .ok()
            .flatten()
            .map(|code| code.into_inner()),
    };
    set_packet_delivery_callback(network, None);
    stopped
}

/// An honest stream whose first datagram (its header) is lost, at a window
/// the budget has grown and a round trip that makes the resend take a while:
/// what arrives meanwhile is staged and stays within the staging cap, so the
/// stream is neither stopped nor the path closed, and it is delivered whole.
/// Run at several transfer sizes, so the window has grown when the loss hits,
/// and at 20 and 100 ms each way.
#[test]
fn an_honest_stream_whose_first_packet_is_lost_stays_within_the_staging_cap() {
    for (case, mib, one_way) in [(0u8, 1usize, 20u64), (1, 8, 20), (2, 24, 20), (3, 24, 100)] {
        bounded(move || async move {
            use crate::transport::in_memory_socket::{
                PacketDeliveryDecision, set_packet_delivery_callback,
            };
            let _seed = GlobalRng::seed_guard(SEED);
            let windows = Windows {
                a: Some(test_budget(64 << 20, leaked_counters())),
                b: Some(test_budget(64 << 20, leaked_counters())),
            };
            let network = format!("quic-staging-honest-loss-{case}");
            // The delay is set before the handshake, so the loss gate's base
            // round trip has it.
            let delay = move |_: &str, _, _, _: &[u8]| {
                PacketDeliveryDecision::DelayedDelivery(Duration::from_millis(one_way))
            };
            set_packet_delivery_callback(&network, Some(Arc::new(delay)));
            let p = paths_metered(&network, windows).await;
            let a = p.a.clone();
            // Grow the window first with a transfer of the same size.
            let warm = Bytes::from(payload(mib * MIB));
            p.drive(async move {
                a.send_stream(StreamId::next(), None, warm)
                    .await
                    .expect("warm")
            })
            .await;
            let mut p = p;
            let warm = p.b_in.streams.try_recv().expect("warm announced");
            warm.handle.assemble().await.expect("warm assembled");
            // Then lose the first large datagram from a: the next stream's
            // header and first payload.
            let from = p.pair.a_addr;
            let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
            set_packet_delivery_callback(
                &network,
                Some(Arc::new(move |_: &str, sender, _, bytes: &[u8]| {
                    if sender == from
                        && bytes.len() >= 1000
                        && !seen.swap(true, std::sync::atomic::Ordering::Relaxed)
                    {
                        PacketDeliveryDecision::Drop
                    } else {
                        PacketDeliveryDecision::DelayedDelivery(Duration::from_millis(one_way))
                    }
                })),
            );
            let a = p.a.clone();
            let body = Bytes::from(payload(mib * MIB));
            let sent = body.clone();
            p.drive(async move {
                a.send_stream(StreamId::next(), None, sent)
                    .await
                    .expect("sent")
            })
            .await;
            set_packet_delivery_callback(&network, None);
            let arrival = p.b_in.streams.try_recv().expect("announced");
            assert_eq!(arrival.handle.assemble().await.expect("assembled"), body);
            let stats = p.b.stats();
            assert_eq!(
                stats.count(QuicInboundOutcome::EvictedStaged),
                0,
                "{mib} MiB"
            );
            assert_eq!(stats.count(QuicInboundOutcome::ClosedStagedOverCap), 0);
            let peak = p.b.inbound_staged_peak_permille();
            eprintln!("{mib} MiB at {one_way} ms: staged peak {peak} per mille of the cap");
            assert!(peak > 0, "premise: the lost header made the reader stage");
            // Measured at most 500 (the size-class growth); doubling the
            // staging buffer peaks at 625.
            assert!(peak <= 550, "{mib} MiB: staged {peak} per mille of the cap");
        });
    }
}

/// The per-path buffer term charges each waiting one-packet Short at
/// `budget::WAITING_SHORT_BYTES`: the largest body that fits one packet is
/// within it at its allocator size class.
#[test]
fn a_waiting_one_packet_short_fits_its_charge() {
    let largest = (1..=u64::from(u16::MAX))
        .take_while(|len| crate::transport::peer_connection::fits_one_packet(*len))
        .last()
        .expect("some length fits one packet");
    let class = super::inbound::allocation_class(largest as usize) as u64;
    assert!(
        class <= super::super::budget::WAITING_SHORT_BYTES,
        "a {largest} B body allocates {class} B"
    );
}

/// A control body whose stream's end has not arrived stays charged to the
/// connection's staging until the end comes, so a peer that withholds the
/// end of its control streams holds nothing outside the charge.
#[test]
fn a_control_body_awaiting_its_end_stays_charged() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-path-control-end", QuicPathLimits::DEFAULT).await;
        let (raw, b, time) = (p.a_raw(), p.b.clone(), p.time());
        let body = payload(4000);
        let streams = 6;
        let (held, after) = p
            .drive(async move {
                let header = FrameHeader::control(body.len() as u64)
                    .expect("a control header")
                    .encode();
                let mut sends = Vec::new();
                for _ in 0..streams {
                    let mut send = raw.open_uni().await.expect("open");
                    send.write_all(&header).await.expect("header");
                    send.write_all(&body).await.expect("body");
                    sends.push(send);
                }
                time.sleep(Duration::from_secs(1)).await;
                let held = b.inbound_staged();
                for mut send in sends {
                    send.finish().expect("finish");
                }
                time.sleep(Duration::from_secs(1)).await;
                (held, b.inbound_staged())
            })
            .await;
        let each =
            super::inbound::allocation_class(4000) + super::inbound::STAGING_BUFFER_OVERHEAD_BYTES;
        assert_eq!(
            held,
            streams * each,
            "every waiting body is charged as staged"
        );
        assert_eq!(after, 0, "released once each end arrives");
        assert_eq!(
            p.b.stats().count(QuicInboundOutcome::DeliveredControl),
            streams as u64
        );
    })
}

/// The receive side reads every stream unordered: its one quinn read is
/// `read_chunk(_, false)`, and no code outside comments calls an in-order
/// read (`read_chunk(_, true)`, `read`, `read_exact`, `read_to_end` or
/// `read_chunks`) on a `RecvStream`. An in-order read would leave what
/// quinn holds for the stream outside every bound the reader keeps.
#[test]
fn the_receive_side_never_reads_a_stream_in_order() {
    let source = include_str!("inbound.rs");
    let code = source
        .lines()
        .map(|line| line.split("//").next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    let unordered = code.matches(".read_chunk(max, false)").count();
    assert_eq!(unordered, 1, "the reader's one quinn read is unordered");
    assert_eq!(
        code.matches("read_chunk(").count(),
        unordered,
        "every read_chunk is unordered"
    );
    for in_order in [
        "recv.read(",
        "recv.read_exact(",
        "recv.read_to_end(",
        "recv.read_chunks(",
        ".read_chunk(max, true)",
    ] {
        assert!(!code.contains(in_order), "an in-order read: {in_order}");
    }
}

/// An admitted Stream whose metadata is lost (its header arrived alone)
/// sends the payload that arrives meanwhile straight to its charged buffer,
/// not to staging: with a staging cap far below the payload, the stream is
/// delivered whole, metadata included, and nothing is evicted.
#[test]
fn payload_read_while_the_metadata_is_missing_is_not_staged() {
    bounded(|| async move {
        use crate::transport::in_memory_socket::{
            PacketDeliveryDecision, set_packet_delivery_callback,
        };
        let _seed = GlobalRng::seed_guard(SEED);
        let limits = QuicPathLimits {
            staging_cap: Some(16 * 1024),
            ..QuicPathLimits::DEFAULT
        };
        let network = "quic-path-metadata-lost";
        let mut p = paths(network, limits).await;
        let (from, raw, time) = (p.pair.a_addr, p.a_raw(), p.time());
        let id = StreamId::next().0;
        let metadata = payload(1000);
        let body = payload(256 * 1024);
        let total = body.len() as u64;
        let (meta, data) = (metadata.clone(), body.clone());
        let net = network.to_string();
        p.drive(async move {
            let mut send = raw.open_uni().await.unwrap();
            send.write_all(&header(1, 1, id, total, meta.len() as u32))
                .await
                .unwrap();
            time.sleep(Duration::from_millis(50)).await;
            // The header has gone alone; lose the first large datagram after
            // it, which carries the metadata.
            let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
            set_packet_delivery_callback(
                &net,
                Some(Arc::new(move |_: &str, sender, _, bytes: &[u8]| {
                    if sender == from
                        && bytes.len() >= 1000
                        && !seen.swap(true, std::sync::atomic::Ordering::Relaxed)
                    {
                        PacketDeliveryDecision::Drop
                    } else {
                        PacketDeliveryDecision::Deliver
                    }
                })),
            );
            send.write_all(&meta).await.unwrap();
            send.write_all(&data).await.unwrap();
            send.finish().unwrap();
            send.stopped().await.unwrap();
        })
        .await;
        set_packet_delivery_callback(network, None);
        let arrival = p.b_in.streams.try_recv().expect("announced");
        assert_eq!(arrival.metadata.as_deref(), Some(&metadata[..]));
        assert_eq!(
            &arrival.handle.assemble().await.expect("assembled")[..],
            &body[..]
        );
        assert_eq!(p.b.stats().count(QuicInboundOutcome::EvictedStaged), 0);
    })
}

/// A stream whose first datagram (its header) is lost has what follows staged
/// (`inbound::Staging`); past the connection's staging cap it is stopped
/// (`EvictedStaged`) and its staged bytes freed, while another stream, whose
/// header arrived, is delivered. A second stream past the cap within
/// `inbound::STAGED_TRIP_WINDOW` closes the path with OVER_BUDGET.
#[test]
fn streams_past_the_staging_cap_are_stopped_and_a_second_closes_the_path() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let counters = leaked_counters();
        let limits = QuicPathLimits {
            counters: Some(counters),
            // Room for one datagram's worth of an honest stream's first piece,
            // not for two datagrams of a stream whose header is missing.
            staging_cap: Some(2048),
            ..QuicPathLimits::DEFAULT
        };
        let network = "quic-path-staging-cap";
        let mut p = paths(network, limits).await;
        let settle = p.time();
        p.drive(async move { settle.sleep(Duration::from_millis(200)).await })
            .await;
        let (from, raw, a) = (p.pair.a_addr, p.a_raw(), p.a.clone());
        let body = Bytes::from(payload(256 * 1024));
        let sent = body.clone();
        let stopped = p
            .drive(async move {
                let stopped = send_without_its_first_datagram(
                    "quic-path-staging-cap",
                    from,
                    raw,
                    vec![9u8; 8192],
                )
                .await;
                a.send_stream(StreamId::next(), None, sent)
                    .await
                    .expect("sent");
                stopped
            })
            .await;
        assert_eq!(
            stopped,
            Some(u64::from(stop_code::EVICTED)),
            "the stream past the cap was stopped"
        );
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::EvictedStaged), 1);
        assert_eq!(stats.count(QuicInboundOutcome::ClosedStagedOverCap), 0);
        assert_eq!(p.b.inbound_staged(), 0, "its staged bytes were freed");
        assert!(counters.snapshot(false).staged_peak_pct > 0);
        let arrival = p
            .b_in
            .streams
            .try_recv()
            .expect("the other stream was announced");
        assert_eq!(arrival.handle.assemble().await.expect("assembled"), body);

        // A second, within the window: the path closes.
        let raw = p.a_raw();
        p.drive(async move {
            send_without_its_first_datagram("quic-path-staging-cap", from, raw, vec![9u8; 8192])
                .await
        })
        .await;
        assert_eq!(stats.count(QuicInboundOutcome::EvictedStaged), 2);
        assert_eq!(stats.count(QuicInboundOutcome::ClosedStagedOverCap), 1);
        assert_eq!(p.b.closed_with(), Some(QuicCloseCode::OverBudget));
    })
}

/// A reader waiting for room in a full channel keeps reading (condition of the
/// buffer budget: quinn must hold nothing for a waiting stream). A Short whose
/// body is complete, waiting for room, and whose sender then writes past the
/// declared end, is stopped as `Overlong` at once, while the channel is still
/// full; a reader that stopped reading while it waited would leave those bytes
/// with quinn until room came or the age limit passed. The same for a Stream.
#[test]
fn a_reader_waiting_for_room_keeps_reading() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let limits = QuicPathLimits {
            short_capacity: 1,
            stream_capacity: 1,
            ..QuicPathLimits::DEFAULT
        };
        let mut p = paths("quic-path-wait-reads", limits).await;
        let (a, held) = (p.a.clone(), StreamId::next());
        p.drive(async move {
            a.send_short(b"fills").await.unwrap();
            a.send_stream(held, None, Bytes::from_static(b"fills"))
                .await
                .unwrap();
        })
        .await;
        for kind in [0u8, 1u8] {
            let (raw, time) = (p.a_raw(), p.time());
            let stopped = p
                .drive(async move {
                    let id = if kind == 1 { StreamId::next().0 } else { 0 };
                    let mut bytes = header(1, kind, id, 100, 0);
                    bytes.extend_from_slice(&payload(100));
                    let mut send = raw.open_uni().await.unwrap();
                    send.write_all(&bytes).await.unwrap();
                    // The body is complete and the reader waits for room;
                    // now bytes past the declared end.
                    time.sleep(Duration::from_millis(100)).await;
                    send.write_all(&payload(4000)).await.ok();
                    tokio::time::timeout(Duration::from_secs(1), send.stopped())
                        .await
                        .expect("stopped while the channel is still full")
                        .expect("stopped")
                        .map(|code| code.into_inner())
                })
                .await;
            assert_eq!(stopped, Some(u64::from(stop_code::OVERLONG)), "kind {kind}");
        }
        assert_eq!(p.b.stats().count(QuicInboundOutcome::Overlong), 2);
        assert!(
            p.b_in.shorts.try_recv().is_ok() && p.b_in.streams.try_recv().is_ok(),
            "the channels were full the whole time"
        );
    })
}

/// A one-packet Short that has to wait for room reads on and may read its
/// end, but stays uncharged (a refusal could lose a message its sender has
/// seen acknowledged); it is delivered once room comes, with no charge.
#[test]
fn a_waiting_one_packet_short_is_not_charged() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let limits = QuicPathLimits {
            short_capacity: 1,
            ..QuicPathLimits::DEFAULT
        };
        let mut p = paths("quic-path-wait-charge", limits).await;
        let a = p.a.clone();
        p.drive(async move { a.send_short(b"fills").await.unwrap() })
            .await;
        assert_eq!(
            p.b.inbound_budget_streams(),
            0,
            "premise: one packet is not charged"
        );
        let (a, time) = (p.a.clone(), p.time());
        // The handle is awaited later, outside the drive.
        #[allow(clippy::async_yields_async)]
        let waiting = p
            .drive(async move {
                let waiting = tokio::spawn(async move { a.send_short(b"waits").await });
                time.sleep(Duration::from_millis(200)).await;
                waiting
            })
            .await;
        assert_eq!(
            p.b.inbound_budget_streams(),
            0,
            "the waiting short is not charged"
        );
        assert_eq!(&p.b_in.shorts.try_recv().unwrap().payload()[..], b"fills");
        let time = p.time();
        p.drive(async move {
            time.sleep(Duration::from_millis(200)).await;
            waiting.await.unwrap().expect("sent");
        })
        .await;
        let delivered = p.b_in.shorts.try_recv().expect("delivered after room came");
        assert_eq!(&delivered.payload()[..], b"waits");
        let (_, key) = delivered.into_parts();
        assert!(key.is_none(), "it carries no charge");
    })
}

/// The case that motivated a path-derived consumer gap (e5b-9-perf's cold
/// start lab, 64 KiB at 800 ms RTT and 15% loss): over many seeds, the worst
/// gap between two fragments arriving at the receiver stays under the gap
/// the handle gives its consumers (24 round trips, about 19.2 s here), and
/// every stream assembles. Prints the worst gap per seed.
///
/// Deterministic: virtual time, loss drawn from a per-seed RNG, and the
/// global RNG seeded, so the gaps are the same on every run, alone or beside
/// other tests (checked: identical per-seed output over repeated and parallel
/// runs). Worst here 8.09 s against 19.2 s. A wider sweep (outside CI) is in
/// the PR.
#[test]
fn a_slow_lossy_path_stays_within_its_consumer_gap() {
    bounded(|| async move {
        use crate::transport::quic::path::bounded::SendDeadlines;
        let _seed = GlobalRng::seed_guard(SEED);
        let mut worst = Duration::ZERO;
        for len in [64 * 1024usize, 256 * 1024] {
            for seed in 1..=20u64 {
                let network: &'static str =
                    Box::leak(format!("quic-gap-lossy-{len}-{seed}").into_boxed_str());
                // The handshake runs on the delay alone, so the round trip is
                // known before the loss starts.
                crate::transport::in_memory_socket::set_packet_delivery_callback(
                    network,
                    Some(Arc::new(|_, _, _, _| {
                        crate::transport::in_memory_socket::PacketDeliveryDecision::DelayedDelivery(
                            Duration::from_millis(400),
                        )
                    })),
                );
                let mut p = paths(network, QuicPathLimits::DEFAULT).await;
                let _loss = seeded_loss_from(network, 15, seed, Some(Duration::from_millis(400)));
                let (a, time) = (p.a.clone(), p.time());
                let send = tokio::spawn(async move {
                    a.send_stream_bounded(
                        &time,
                        SendDeadlines::stream(len as u64),
                        StreamId::next(),
                        None,
                        Bytes::from(payload(len)),
                        None,
                    )
                    .await
                });
                // Wait for the announcement, then watch its fragments arrive.
                let time = p.time();
                let mut b_in =
                    std::mem::replace(&mut p.b_in.streams, tokio::sync::mpsc::channel(1).1);
                let (gap, limit, assembled) = p
                    .drive(async move {
                        let arrival = b_in.recv().await.expect("announced");
                        let handle = arrival.handle;
                        let limit = handle.fragment_gap_limit();
                        let mut last_change = time.now_nanos();
                        let mut seen = handle.received_bytes();
                        let mut gap = 0u64;
                        while !handle.is_complete() && handle.try_assemble().is_none() {
                            time.sleep(Duration::from_millis(10)).await;
                            let now = time.now_nanos();
                            let got = handle.received_bytes();
                            if got != seen {
                                seen = got;
                                last_change = now;
                            }
                            gap = gap.max(now - last_change);
                            if Duration::from_nanos(now - last_change) > Duration::from_secs(60) {
                                break;
                            }
                        }
                        let assembled = handle.try_assemble().is_some();
                        let _outcome = send.await.expect("the send task ran");
                        (Duration::from_nanos(gap), limit, assembled)
                    })
                    .await;
                eprintln!(
                    "gap lossy: len {len} seed {seed}: worst gap {gap:?}, limit {limit:?}, assembled {assembled}"
                );
                worst = worst.max(gap);
                assert!(
                    gap < limit,
                    "len {len} seed {seed}: gap {gap:?} >= {limit:?}"
                );
                assert!(assembled, "len {len} seed {seed}: did not assemble");
                crate::transport::in_memory_socket::set_packet_delivery_callback(network, None);
            }
        }
        eprintln!("gap lossy: worst over all seeds {worst:?}");
    })
}
