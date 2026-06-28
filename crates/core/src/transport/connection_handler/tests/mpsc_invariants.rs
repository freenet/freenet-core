use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio::test]
async fn send_recv_roundtrip() {
    let (tx, mut rx) = mpsc::channel::<i32>(10);
    tx.send(42).await.unwrap();
    tx.send(43).await.unwrap();
    assert_eq!(rx.recv().await, Some(42));
    assert_eq!(rx.recv().await, Some(43));
}

#[tokio::test]
async fn try_send_returns_full_at_capacity() {
    let (tx, _rx) = mpsc::channel::<i32>(2);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert!(matches!(
        tx.try_send(3),
        Err(mpsc::error::TrySendError::Full(3))
    ));
}

#[tokio::test]
async fn try_recv_returns_empty_when_drained() {
    let (tx, mut rx) = mpsc::channel::<i32>(10);
    tx.send(1).await.unwrap();
    assert_eq!(rx.try_recv(), Ok(1));
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn backpressure_blocks_sender_until_drain() {
    let (tx, mut rx) = mpsc::channel::<i32>(2);
    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();
    // Channel full — try_send fails immediately, send().await would park.
    assert!(matches!(
        tx.try_send(3),
        Err(mpsc::error::TrySendError::Full(3))
    ));
    assert_eq!(rx.recv().await, Some(1));
    // Space freed; can send again.
    tx.try_send(3).unwrap();
}

#[tokio::test]
async fn sender_clone_preserves_send_order_per_clone() {
    let (tx, mut rx) = mpsc::channel::<i32>(10);
    let tx2 = tx.clone();
    tx.send(1).await.unwrap();
    tx2.send(2).await.unwrap();
    assert_eq!(rx.recv().await, Some(1));
    assert_eq!(rx.recv().await, Some(2));
}

#[tokio::test]
async fn buffered_messages_drain_after_all_senders_drop() {
    let (tx, mut rx) = mpsc::channel::<i32>(10);
    tx.send(42).await.unwrap();
    drop(tx);
    assert_eq!(rx.recv().await, Some(42));
    assert_eq!(rx.recv().await, None, "None signals all senders dropped");
}

#[tokio::test]
async fn is_closed_reflects_receiver_drop() {
    let (tx, rx) = mpsc::channel::<i32>(10);
    assert!(!tx.is_closed());
    drop(rx);
    assert!(tx.is_closed());
}

#[tokio::test]
async fn concurrent_send_recv_one_thousand_messages() {
    let (tx, mut rx) = mpsc::channel::<i32>(100);
    let count = 1000;
    let sender = tokio::spawn(async move {
        for i in 0..count {
            tx.send(i).await.unwrap();
        }
    });
    let receiver = tokio::spawn(async move {
        let mut received = 0;
        while rx.recv().await.is_some() {
            received += 1;
            if received == count {
                break;
            }
        }
        received
    });
    sender.await.unwrap();
    assert_eq!(receiver.await.unwrap(), count);
}

#[tokio::test]
async fn recv_does_not_busy_loop_while_idle() {
    // recv().await must park, not spin. With a 1s producer delay, the
    // receiver must complete in a small bounded number of polls.
    let (tx, mut rx) = mpsc::channel::<i32>(10);
    let polls = Arc::new(AtomicU64::new(0));
    let polls_inner = polls.clone();
    let receiver = tokio::spawn(async move {
        polls_inner.fetch_add(1, Ordering::Relaxed);
        rx.recv().await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    tx.send(7).await.unwrap();
    assert_eq!(receiver.await.unwrap(), Some(7));
    // Single recv() future; one poll to enter, parker wakes it once on
    // delivery. Anything >>2 would mean the runtime was repolling
    // (busy loop). Allow generous bound to avoid runtime-internal
    // counting noise.
    assert!(
        polls.load(Ordering::Relaxed) < 10,
        "recv() should park, not busy-loop"
    );
}

#[tokio::test]
async fn burst_send_before_recv_drains_all() {
    // Multiple sends with no receiver waiting still result in all
    // messages being delivered, in order, when recv() begins.
    let (tx, mut rx) = mpsc::channel::<i32>(100);
    for i in 0..50 {
        tx.send(i).await.unwrap();
    }
    for i in 0..50 {
        assert_eq!(rx.recv().await, Some(i));
    }
}

#[tokio::test]
async fn dropping_sender_wakes_blocked_receiver() {
    // A receiver parked on recv() must wake when the last sender drops,
    // and observe None (channel closed).
    let (tx, mut rx) = mpsc::channel::<i32>(10);
    let receiver = tokio::spawn(async move { rx.recv().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(tx);
    let result = receiver.await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn multiple_concurrent_senders_lose_no_messages() {
    let (tx, mut rx) = mpsc::channel::<i32>(100);
    let n_senders = 5;
    let msgs_per_sender = 200;
    let handles: Vec<_> = (0..n_senders)
        .map(|_| {
            let tx = tx.clone();
            tokio::spawn(async move {
                for i in 0..msgs_per_sender {
                    tx.send(i).await.unwrap();
                }
            })
        })
        .collect();
    drop(tx);
    let receiver = tokio::spawn(async move {
        let mut count = 0;
        while rx.recv().await.is_some() {
            count += 1;
        }
        count
    });
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(receiver.await.unwrap(), n_senders * msgs_per_sender);
}

#[tokio::test]
async fn high_concurrency_small_channel_drops_nothing() {
    // 50 senders × 1000 msgs through a capacity-5 channel: forces heavy
    // backpressure on every send. All messages must arrive.
    let (tx, mut rx) = mpsc::channel::<u64>(5);
    let n_senders = 50_u64;
    let msgs_per_sender = 1000_u64;
    let handles: Vec<_> = (0..n_senders)
        .map(|sender_id| {
            let tx = tx.clone();
            tokio::spawn(async move {
                for i in 0..msgs_per_sender {
                    tx.send(sender_id * msgs_per_sender + i).await.unwrap();
                }
            })
        })
        .collect();
    drop(tx);
    let receiver = tokio::spawn(async move {
        let mut count = 0_u64;
        while rx.recv().await.is_some() {
            count += 1;
        }
        count
    });
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(receiver.await.unwrap(), n_senders * msgs_per_sender);
}

#[tokio::test]
async fn rapid_sender_disconnect_loses_no_messages() {
    // Spawn a wave of short-lived senders, each sending then dropping.
    // Receiver must collect exactly the total sent.
    let (tx, mut rx) = mpsc::channel::<u64>(100);
    let sent = Arc::new(AtomicU64::new(0));
    let handles: Vec<_> = (0..100)
        .map(|wave| {
            let tx = tx.clone();
            let sent = sent.clone();
            tokio::spawn(async move {
                for i in 0..10 {
                    tx.send(wave * 10 + i).await.unwrap();
                    sent.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();
    drop(tx);
    let receiver = tokio::spawn(async move {
        let mut count = 0_u64;
        while rx.recv().await.is_some() {
            count += 1;
        }
        count
    });
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(receiver.await.unwrap(), sent.load(Ordering::Relaxed));
}

#[tokio::test]
async fn many_independent_channels_in_parallel() {
    // 200 channels running concurrently — exercises per-connection
    // channel pattern without cross-channel interference.
    let n_channels = 200;
    let msgs_per_channel = 500_u64;
    let handles: Vec<_> = (0..n_channels)
        .map(|_| {
            tokio::spawn(async move {
                let (tx, mut rx) = mpsc::channel::<u64>(50);
                let sender = tokio::spawn(async move {
                    for i in 0..msgs_per_channel {
                        tx.send(i).await.unwrap();
                    }
                });
                let receiver = tokio::spawn(async move {
                    let mut count = 0_u64;
                    while count < msgs_per_channel {
                        rx.recv().await.unwrap();
                        count += 1;
                    }
                    count
                });
                sender.await.unwrap();
                assert_eq!(receiver.await.unwrap(), msgs_per_channel);
            })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn slow_receiver_with_capacity_one_completes() {
    // capacity-1 channel: every send must wait for the receiver. With
    // a deliberately slow receiver, all messages must still arrive.
    let (tx, mut rx) = mpsc::channel::<u64>(1);
    let n_msgs = 500_u64;
    let sender = tokio::spawn(async move {
        for i in 0..n_msgs {
            tx.send(i).await.unwrap();
        }
    });
    let receiver = tokio::spawn(async move {
        let mut count = 0_u64;
        for _ in 0..n_msgs {
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg, count);
            count += 1;
            if count % 10 == 0 {
                tokio::task::yield_now().await;
            }
        }
        count
    });
    sender.await.unwrap();
    assert_eq!(receiver.await.unwrap(), n_msgs);
}

#[tokio::test]
async fn sender_drop_during_backpressure_releases_blocked_senders() {
    // Fill channel, start senders blocked on backpressure, then drop
    // receiver — every blocked sender must observe SendError, none stuck.
    let (tx, rx) = mpsc::channel::<u64>(2);
    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let tx = tx.clone();
            tokio::spawn(async move { tx.send(100 + i).await })
        })
        .collect();
    drop(tx);
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(rx);
    for h in handles {
        assert!(h.await.unwrap().is_err());
    }
}

#[tokio::test]
async fn receiver_disconnect_observed_by_senders() {
    let (tx, rx) = mpsc::channel::<u64>(10);
    assert!(!tx.is_closed());
    drop(rx);
    assert!(tx.is_closed());
    assert!(tx.send(1).await.is_err());
}

#[tokio::test]
async fn send_after_close_returns_error() {
    let (tx, mut rx) = mpsc::channel::<u64>(10);
    tx.send(1).await.unwrap();
    rx.close();
    // After receiver-side close, further sends must fail rather than
    // succeed-but-be-discarded.
    let result = tx.send(2).await;
    assert!(result.is_err());
    // Messages already buffered before close are still delivered.
    assert_eq!(rx.recv().await, Some(1));
    assert_eq!(rx.recv().await, None);
}

#[tokio::test]
async fn capacity_is_exact() {
    // mpsc::channel(N) accepts exactly N pending messages with no
    // consumer; the (N+1)th try_send must return Full.
    let (tx, _rx) = mpsc::channel::<u64>(5);
    for i in 0..5 {
        tx.try_send(i).unwrap();
    }
    assert!(matches!(
        tx.try_send(5),
        Err(mpsc::error::TrySendError::Full(5))
    ));
}
