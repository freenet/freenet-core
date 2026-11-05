//! A path's counts reach the node's (`metrics::quic_data`), each where it is
//! decided. The paths keep them in counters of the test's own, since every test in
//! the process writes `TRANSPORT_METRICS`.

use std::collections::BTreeMap;

use super::super::bounded::{NotOpened, SendFailure};
use super::*;
use crate::transport::metrics::QuicDataCounters;
use crate::transport::quic::config::{MAX_UNI_STREAMS, STREAMED_SEND_LIMIT};

fn node_counters() -> &'static QuicDataCounters {
    Box::leak(Box::new(QuicDataCounters::new()))
}

fn keys(node: &QuicDataCounters) -> BTreeMap<String, u64> {
    node.snapshot(true).keys().into_iter().collect()
}

/// Sends, their stops, the receiver's refusals by frame kind, its deliveries, the
/// streams it read at once and the bytes both sides moved: each reaches the node's
/// counts once.
#[test]
fn a_paths_sends_and_receives_reach_the_node() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let node = node_counters();
        let limits = QuicPathLimits {
            counters: Some(node),
            max_bytes: 64 * 1024,
            ..QuicPathLimits::DEFAULT
        };
        let p = paths("quic-node-counts", 80, limits).await;
        let (a, time) = (p.a.clone(), p.time());
        p.drive(async move {
            let sent = a
                .send_short_bounded(&time, Duration::from_secs(5), b"one packet")
                .await;
            assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
            a.settle(SendKind::Short, &sent, false, time.now_nanos());
            let refused = a
                .send_stream_bounded(
                    &time,
                    Duration::from_secs(5),
                    StreamId::next(),
                    None,
                    payload(256 * 1024).into(),
                    None,
                )
                .await;
            a.settle(SendKind::Stream, &refused, false, time.now_nanos());
            let large = a
                .send_short_bounded(&time, Duration::from_secs(5), &vec![1u8; 128 * 1024])
                .await;
            a.settle(SendKind::LargeShort, &large, false, time.now_nanos());
            // Past a tick of the acknowledgement watch, which reports the bytes.
            time.sleep(Duration::from_secs(3)).await;
        })
        .await;
        let keys = keys(node);
        assert_eq!(keys["quic_data_send_short_sent"], 1);
        assert_eq!(keys["quic_data_send_stream_dropped"], 1);
        assert_eq!(keys["quic_data_send_large_short_failed_udp"], 1);
        assert_eq!(keys["quic_data_stop_budget_refused"], 2);
        assert_eq!(keys["quic_inbound_delivered_short"], 1);
        assert_eq!(keys["quic_inbound_stream_refused_budget"], 1);
        assert_eq!(keys["quic_inbound_short_refused_budget"], 1);
        assert_eq!(
            keys["quic_streamed_sends"], 2,
            "the stream and the large short"
        );
        assert_eq!(keys["quic_stream_permit_waited"], 0);
        assert_eq!(keys["quic_sends"], 3);
        assert_eq!(keys["quic_open_uni_waited"], 0);
        assert!(keys["quic_bytes_sent"] > 0);
        assert!(keys["quic_bytes_received"] > 0);
        assert_eq!(
            keys["quic_streams_open_paths_sampled"], 2,
            "both ends read streams"
        );
    });
}

/// With every stream credit held, an open waits, and with every permit held a
/// streamed send waits: each is counted once, beside its denominator.
#[test]
fn waits_for_a_stream_and_for_a_permit_are_counted() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let node = node_counters();
        let limits = QuicPathLimits {
            counters: Some(node),
            ..QuicPathLimits::DEFAULT
        };
        let p = paths("quic-node-waits", 81, limits).await;
        let permits: Vec<_> = (0..STREAMED_SEND_LIMIT)
            .map(|_| {
                Arc::clone(&p.a.stream_slots)
                    .try_acquire_owned()
                    .expect("free")
            })
            .collect();
        let (conn, a, time) = (p.a_raw(), p.a.clone(), p.time());
        let (unpermitted, unopened, held) = p
            .drive(async move {
                let unpermitted = a
                    .send_stream_bounded(
                        &time,
                        Duration::from_secs(2),
                        StreamId::next(),
                        None,
                        payload(10_000).into(),
                        None,
                    )
                    .await;
                let mut held = Vec::new();
                for _ in 0..MAX_UNI_STREAMS {
                    let mut send = conn.open_uni().await.unwrap();
                    send.write_all(&header(1, 1, StreamId::next().0, 1000, 0))
                        .await
                        .unwrap();
                    held.push(send);
                }
                let unopened = a
                    .send_short_bounded(&time, Duration::from_secs(1), b"waits")
                    .await;
                (unpermitted, unopened, held)
            })
            .await;
        for outcome in [&unpermitted, &unopened] {
            assert!(
                matches!(
                    outcome,
                    BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut))
                ),
                "{outcome:?}"
            );
        }
        let keys = keys(node);
        assert_eq!(keys["quic_streamed_sends"], 1);
        assert_eq!(keys["quic_stream_permit_waited"], 1);
        assert_eq!(keys["quic_sends"], 1, "the raw streams are not the path's");
        assert_eq!(keys["quic_open_uni_waited"], 1);
        assert_eq!(
            keys["quic_streams_open_ge_48"], 1,
            "the peer read 64 at once"
        );
        drop((permits, held));
    });
}
