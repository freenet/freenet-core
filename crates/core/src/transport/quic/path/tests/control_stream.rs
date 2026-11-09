//! The long-lived control stream (`path::control`): it opens before any data
//! stream, carries control payloads, and a peer that breaks it loses the path.

use super::*;
use crate::transport::quic::control::QuicControl;
use crate::transport::quic::error::close_code;

/// How long the opens and sends in these tests may take.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// The header that opens a control stream, as a peer writes it.
fn control_stream_header() -> Vec<u8> {
    header(1, 3, 0, 0, 0)
}

/// The application code `conn` was closed with, once it closes.
async fn closed_with(conn: quinn::Connection) -> u64 {
    let closed = conn.closed().await;
    let quinn::ConnectionError::ApplicationClosed(close) = closed else {
        panic!("closed by {closed:?}");
    };
    close.error_code.into_inner()
}

/// A control stream opens, its payloads arrive on the peer's control channel
/// in order, and between payloads it may stay quiet for longer than the idle
/// timeout without anything closing.
#[test]
fn a_control_stream_carries_control_payloads_and_may_stay_quiet() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-control-stream", 131, QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        let first = QuicControl::Ready {
            attempt: 3,
            caps: 0,
        }
        .encode();
        let idle = streaming_handle_idle_timeout();
        p.drive(async move {
            assert!(a.expect_control_stream());
            a.open_control_stream_bounded(&time, SEND_TIMEOUT)
                .await
                .expect("opened");
            a.send_on_control_stream_bounded(&time, SEND_TIMEOUT, &first)
                .await
                .expect("sent");
            time.sleep(idle * 3).await;
            a.send_on_control_stream_bounded(&time, SEND_TIMEOUT, b"second")
                .await
                .expect("sent after a quiet spell");
            time.sleep(Duration::from_secs(1)).await;
        })
        .await;
        assert_eq!(&p.b_in.controls.try_recv().unwrap()[..], &first[..]);
        assert_eq!(&p.b_in.controls.try_recv().unwrap()[..], b"second");
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::ControlStreamAccepted), 1);
        assert_eq!(stats.count(QuicInboundOutcome::DeliveredControl), 2);
        assert_eq!(stats.count(QuicInboundOutcome::ControlStreamIdle), 0);
        assert!(p.b.connection().inner().close_reason().is_none());
        assert!(p.a.connection().inner().close_reason().is_none());
    })
}

/// While this side's control stream is being opened, data sends wait for it,
/// so no data stream takes a slot before it; once it is open they go on. A
/// path where no control stream is expected does not wait.
#[test]
fn data_sends_wait_for_the_control_stream_being_opened() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-control-stream-first", 132, QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        assert!(a.expect_control_stream());
        let waiting = {
            let a = a.clone();
            tokio::spawn(async move { a.send_short(b"data").await })
        };
        let sleeper = time.clone();
        p.drive(async move { sleeper.sleep(Duration::from_secs(1)).await })
            .await;
        assert!(
            !waiting.is_finished(),
            "the data send waits for the control stream"
        );
        assert!(
            p.b_in.shorts.try_recv().is_err(),
            "no data stream opened first"
        );
        p.drive(async move {
            a.open_control_stream_bounded(&time, SEND_TIMEOUT)
                .await
                .expect("opened");
            waiting
                .await
                .unwrap()
                .expect("sent once the control stream is open");
            time.sleep(Duration::from_secs(1)).await;
        })
        .await;
        assert_eq!(&p.b_in.shorts.try_recv().unwrap().payload()[..], b"data");
        assert_eq!(
            p.b.stats().count(QuicInboundOutcome::ControlStreamAccepted),
            1
        );
    })
}

/// A second control stream from the peer closes the path with CONTROL_REFUSED.
#[test]
fn a_second_control_stream_closes_the_path() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-control-stream-twice", 133, QuicPathLimits::DEFAULT).await;
        let conn = p.a_raw();
        let code = p
            .drive(async move {
                let mut first = conn.open_uni().await.unwrap();
                first.write_all(&control_stream_header()).await.unwrap();
                let mut second = conn.open_uni().await.unwrap();
                second.write_all(&control_stream_header()).await.unwrap();
                let code = closed_with(conn).await;
                drop((first, second));
                code
            })
            .await;
        assert_eq!(code, u64::from(close_code::CONTROL_REFUSED));
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::ControlStreamAccepted), 1);
        assert_eq!(stats.count(QuicInboundOutcome::ControlStreamDuplicate), 1);
    })
}

/// A control stream the peer breaks closes the path with CONTROL_REFUSED and
/// is counted by how: a payload length out of bounds, the stream finished,
/// and a payload that stops partway for the idle timeout.
#[test]
fn a_broken_control_stream_closes_the_path() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let idle = streaming_handle_idle_timeout();
        let max = u16::try_from(MAX_CONTROL_BYTES).unwrap();
        let cases: [(&str, u8, Vec<u8>, bool, QuicInboundOutcome); 4] = [
            (
                "empty payload",
                134,
                vec![0, 0],
                false,
                QuicInboundOutcome::ControlStreamMalformed,
            ),
            (
                "oversized payload",
                135,
                (max + 1).to_be_bytes().to_vec(),
                false,
                QuicInboundOutcome::ControlStreamMalformed,
            ),
            (
                "finished",
                136,
                vec![0, 1, 7],
                true,
                QuicInboundOutcome::ControlStreamEnded,
            ),
            (
                "partial payload",
                137,
                vec![0, 5, 1],
                false,
                QuicInboundOutcome::ControlStreamIdle,
            ),
        ];
        for (case, subnet, bytes, finish, outcome) in cases {
            let p = paths(
                &format!("quic-control-stream-{subnet}"),
                subnet,
                QuicPathLimits::DEFAULT,
            )
            .await;
            let (conn, time) = (p.a_raw(), p.time());
            let started = time.now_nanos();
            let code = p
                .drive(async move {
                    let mut send = conn.open_uni().await.unwrap();
                    let mut written = control_stream_header();
                    written.extend_from_slice(&bytes);
                    send.write_all(&written).await.unwrap();
                    if finish {
                        send.finish().unwrap();
                    }
                    let code = closed_with(conn).await;
                    drop(send);
                    code
                })
                .await;
            assert_eq!(code, u64::from(close_code::CONTROL_REFUSED), "{case}");
            assert_eq!(p.b.stats().count(outcome), 1, "{case}");
            if outcome == QuicInboundOutcome::ControlStreamIdle {
                let waited = Duration::from_nanos(p.time().now_nanos() - started);
                assert!(
                    waited >= idle && waited < idle * 2,
                    "{case}: closed after {waited:?}, idle timeout {idle:?}"
                );
            }
        }
    })
}

/// A path in shadow reads the peer's control stream: it is not data.
#[test]
fn a_shadow_path_reads_the_control_stream() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths("quic-control-stream-shadow", 138, QuicPathLimits::SHADOW).await;
        let (a, time) = (p.a.clone(), p.time());
        p.drive(async move {
            assert!(a.expect_control_stream());
            a.open_control_stream_bounded(&time, SEND_TIMEOUT)
                .await
                .expect("opened");
            a.send_on_control_stream_bounded(&time, SEND_TIMEOUT, b"control")
                .await
                .expect("sent");
            time.sleep(Duration::from_secs(1)).await;
        })
        .await;
        assert_eq!(&p.b_in.controls.try_recv().unwrap()[..], b"control");
        let stats = p.b.stats();
        assert_eq!(stats.count(QuicInboundOutcome::RefusedNotAcceptingData), 0);
        assert!(p.b.connection().inner().close_reason().is_none());
    })
}

/// While a control write is blocked, data writers wait between fragments; once
/// it is released they finish. The gate is shut here by hand, so this pins the
/// writers' side; the next test pins that a blocked control write shuts it.
#[test]
fn data_writers_wait_while_the_control_gate_is_held() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-control-gate", 139, QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        let gate = a.control_gate();
        let hold = gate.hold();
        let sending = {
            let a = a.clone();
            tokio::spawn(async move {
                a.send_stream(
                    StreamId::next(),
                    None,
                    Bytes::from(payload(8 * FRAGMENT_PAYLOAD_SIZE)),
                )
                .await
            })
        };
        let sleeper = time.clone();
        p.drive(async move { sleeper.sleep(Duration::from_secs(2)).await })
            .await;
        assert!(!sending.is_finished(), "the data send waits at the gate");
        drop(hold);
        p.drive(async move { sending.await.unwrap() })
            .await
            .expect("sent once the gate opened");
    })
}

/// A control write blocked on the connection's credit shuts the gate, and a
/// control payload still arrives well before the data does, with this side's
/// egress capped and data streams filling the send window. The arrival bound
/// does not separate the gate from no gate: with data written one fragment per
/// call, the payload got through within it with the gate removed too (checked
/// by mutation). What this pins is the mechanism (a blocked control write holds
/// the gate) and that the payload gets through; the test above pins the
/// writers' side.
#[test]
fn a_control_payload_gets_through_while_data_fills_the_send_window() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let mut p = paths_capped("quic-control-under-load", 140, 200_000).await;
        let (a, time) = (p.a.clone(), p.time());
        let gate = a.control_gate();
        let started = time.now_nanos();
        let sends: Vec<_> = (0..8)
            .map(|_| {
                let a = a.clone();
                tokio::spawn(async move {
                    a.send_stream(StreamId::next(), None, Bytes::from(payload(512 * 1024)))
                        .await
                })
            })
            .collect();
        let (opener, clock) = (a.clone(), time.clone());
        let (seen_held, arrived_after) = p
            .drive(async move {
                assert!(opener.expect_control_stream());
                opener
                    .open_control_stream_bounded(&clock, SEND_TIMEOUT)
                    .await
                    .expect("opened");
                clock.sleep(Duration::from_secs(2)).await;
                let sent_at = clock.now_nanos();
                let watcher = {
                    let gate = gate.clone();
                    let clock = clock.clone();
                    tokio::spawn(async move {
                        let mut seen = false;
                        for _ in 0..200 {
                            seen |= gate.is_held();
                            clock.sleep(Duration::from_millis(5)).await;
                        }
                        seen
                    })
                };
                opener
                    .send_on_control_stream_bounded(&clock, SEND_TIMEOUT, b"urgent")
                    .await
                    .expect("sent");
                let seen = watcher.await.unwrap();
                (seen, Duration::from_nanos(clock.now_nanos() - sent_at))
            })
            .await;
        let control_at = p.time().now_nanos();
        assert_eq!(&p.b_in.controls.try_recv().unwrap()[..], b"urgent");
        assert!(
            seen_held,
            "premise: the control write was blocked on credit"
        );
        assert!(
            arrived_after < Duration::from_secs(2),
            "the control payload went out {arrived_after:?} after it was sent"
        );
        p.drive(async move {
            for send in sends {
                send.await.unwrap().expect("data sent");
            }
        })
        .await;
        let data_done = Duration::from_nanos(p.time().now_nanos() - started);
        assert!(
            Duration::from_nanos(control_at - started) < data_done,
            "the control payload arrived before the data finished"
        );
    })
}
