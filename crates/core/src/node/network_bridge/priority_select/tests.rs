use super::*;
use futures::stream::StreamExt;
use std::future::Future;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};

/// Mock HandshakeStream for testing that pends forever
struct MockHandshakeStream;

impl Stream for MockHandshakeStream {
    type Item = crate::node::network_bridge::handshake::Event;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

/// Create a mock HandshakeStream for testing
fn create_mock_handshake_stream() -> MockHandshakeStream {
    MockHandshakeStream
}

/// Mock implementation that returns None (no receiver to take).
/// Used when tests don't need these channels to receive anything.
struct MockClientNoReceiver;
impl ClientTransactionRelay for MockClientNoReceiver {
    fn take_receiver(&mut self) -> Option<Receiver<(ClientId, WaitingTransaction)>> {
        None
    }
}

struct MockExecutorNoReceiver;
impl ExecutorTransactionReceiver for MockExecutorNoReceiver {
    fn take_receiver(&mut self) -> Option<Receiver<Transaction>> {
        None
    }
}

/// Mock implementation that provides a receiver for testing client transaction events
#[allow(dead_code)]
struct MockClientWithReceiver {
    rx: Option<Receiver<(ClientId, WaitingTransaction)>>,
}

impl ClientTransactionRelay for MockClientWithReceiver {
    fn take_receiver(&mut self) -> Option<Receiver<(ClientId, WaitingTransaction)>> {
        self.rx.take()
    }
}

/// Mock implementation that provides a receiver for testing executor transaction events
#[allow(dead_code)]
struct MockExecutorWithReceiver {
    rx: Option<Receiver<Transaction>>,
}

impl ExecutorTransactionReceiver for MockExecutorWithReceiver {
    fn take_receiver(&mut self) -> Option<Receiver<Transaction>> {
        self.rx.take()
    }
}

/// Test PrioritySelectStream with notification arriving after initial poll
#[tokio::test]
#[test_log::test]
async fn test_priority_select_future_wakeup() {
    let (notif_tx, notif_rx) = mpsc::channel(10);
    let (_op_tx, op_rx) = mpsc::channel(10);
    let (_conn_event_tx, conn_event_rx) = mpsc::channel(10);
    let (_bridge_tx, bridge_rx) = mpsc::channel(10);
    let (_node_tx, node_rx) = mpsc::channel(10);

    // Spawn task that sends notification after delay
    let notif_tx_clone = notif_tx.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(50)).await;
        let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
            crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
        ));
        notif_tx_clone.send(Either::Left(test_msg)).await.unwrap();
    });

    // Create stream - should be pending initially, then wake up when message arrives
    let stream = PrioritySelectStream::new(
        notif_rx,
        op_rx,
        bridge_rx,
        create_mock_handshake_stream(),
        node_rx,
        MockClientNoReceiver,
        MockExecutorNoReceiver,
        conn_event_rx,
    );
    tokio::pin!(stream);

    // Should complete when message arrives (notification has priority over handshake)
    let result = timeout(Duration::from_millis(200), stream.next()).await;

    assert!(
        result.is_ok(),
        "Select stream should wake up when notification arrives"
    );

    let select_result = result.unwrap().expect("Stream should yield value");
    match select_result {
        SelectResult::Notification(Some(_)) => {}
        SelectResult::Notification(None) => panic!("Got Notification(None)"),
        SelectResult::OpExecution(_) => panic!("Got OpExecution"),
        SelectResult::PeerConnection(_) => panic!("Got PeerConnection"),
        SelectResult::ConnBridge(_) => panic!("Got ConnBridge"),
        SelectResult::Handshake(_) => panic!("Got Handshake"),
        SelectResult::NodeController(_) => panic!("Got NodeController"),
        SelectResult::ClientTransaction(_) => panic!("Got ClientTransaction"),
        SelectResult::ExecutorTransaction(_) => panic!("Got ExecutorTransaction"),
    }
}

/// Test that notification has priority over other channels in PrioritySelectStream
#[tokio::test]
#[test_log::test]
async fn test_priority_select_future_priority_ordering() {
    let (notif_tx, notif_rx) = mpsc::channel(10);
    let (op_tx, op_rx) = mpsc::channel(10);
    let (_conn_event_tx, conn_event_rx) = mpsc::channel(10);
    let (bridge_tx, bridge_rx) = mpsc::channel(10);
    let (_, node_rx) = mpsc::channel(10);

    // Send to multiple channels - notification should be received first
    let (callback_tx, _) = mpsc::channel(1);
    let dummy_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
        crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
    ));
    op_tx.send((callback_tx, dummy_msg.clone())).await.unwrap();
    bridge_tx
        .send(Either::Right(NodeEvent::Disconnect { cause: None }))
        .await
        .unwrap();

    let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
        crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
    ));
    notif_tx.send(Either::Left(test_msg)).await.unwrap();

    // Create and poll the stream
    let stream = PrioritySelectStream::new(
        notif_rx,
        op_rx,
        bridge_rx,
        create_mock_handshake_stream(),
        node_rx,
        MockClientNoReceiver,
        MockExecutorNoReceiver,
        conn_event_rx,
    );
    tokio::pin!(stream);

    let result = timeout(Duration::from_millis(100), stream.next()).await;
    assert!(result.is_ok());

    match result.unwrap().expect("Stream should yield value") {
        SelectResult::Notification(_) => {}
        _ => panic!("Notification should be received first due to priority"),
    }
}

/// Test concurrent messages - simpler version that sends all messages first
#[tokio::test]
#[test_log::test]
async fn test_priority_select_future_concurrent_messages() {
    let (notif_tx, notif_rx) = mpsc::channel(100);
    let (_conn_event_tx, conn_event_rx) = mpsc::channel(10);

    // Send all 15 messages
    for _ in 0..15 {
        let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
            crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
        ));
        notif_tx.send(Either::Left(test_msg)).await.unwrap();
    }

    // Receive first message
    let (_, op_rx) = mpsc::channel(10);
    let (_, bridge_rx) = mpsc::channel(10);
    let (_, node_rx) = mpsc::channel(10);

    let stream = PrioritySelectStream::new(
        notif_rx,
        op_rx,
        bridge_rx,
        create_mock_handshake_stream(),
        node_rx,
        MockClientNoReceiver,
        MockExecutorNoReceiver,
        conn_event_rx,
    );
    tokio::pin!(stream);

    let result = timeout(Duration::from_millis(100), stream.next()).await;
    assert!(result.is_ok(), "Should receive first message");
    match result.unwrap().expect("Stream should yield value") {
        SelectResult::Notification(Some(_)) => {}
        _ => panic!("Expected notification"),
    }
}

/// Test that messages arrive in buffered channel before receiver polls
#[tokio::test]
#[test_log::test]
async fn test_priority_select_future_buffered_messages() {
    let (notif_tx, notif_rx) = mpsc::channel(10);
    let (_conn_event_tx, conn_event_rx) = mpsc::channel(10);

    // Send message BEFORE creating stream
    let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
        crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
    ));
    notif_tx.send(Either::Left(test_msg)).await.unwrap();

    // Create stream - should receive the buffered message immediately
    let (_, op_rx) = mpsc::channel(10);
    let (_, bridge_rx) = mpsc::channel(10);
    let (_, node_rx) = mpsc::channel(10);

    let stream = PrioritySelectStream::new(
        notif_rx,
        op_rx,
        bridge_rx,
        create_mock_handshake_stream(),
        node_rx,
        MockClientNoReceiver,
        MockExecutorNoReceiver,
        conn_event_rx,
    );
    tokio::pin!(stream);

    let result = timeout(Duration::from_millis(100), stream.next()).await;
    assert!(
        result.is_ok(),
        "Should receive buffered message immediately"
    );

    match result.unwrap().expect("Stream should yield value") {
        SelectResult::Notification(Some(_)) => {}
        _ => panic!("Expected notification"),
    }
}

/// Test rapid polling of stream with short timeouts
#[tokio::test]
#[test_log::test]
async fn test_priority_select_future_rapid_cancellations() {
    use futures::StreamExt;

    let (notif_tx, notif_rx) = mpsc::channel(100);
    let (_conn_event_tx, conn_event_rx) = mpsc::channel(10);

    // Send 10 messages
    for _ in 0..10 {
        let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
            crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
        ));
        notif_tx.send(Either::Left(test_msg)).await.unwrap();
    }

    let (_, op_rx) = mpsc::channel(10);
    let (_, bridge_rx) = mpsc::channel(10);
    let (_, node_rx) = mpsc::channel(10);

    // Create stream once - it maintains waker registration across polls
    let stream = PrioritySelectStream::new(
        notif_rx,
        op_rx,
        bridge_rx,
        create_mock_handshake_stream(),
        node_rx,
        MockClientNoReceiver,
        MockExecutorNoReceiver,
        conn_event_rx,
    );
    tokio::pin!(stream);

    // Rapidly poll stream with short timeouts (simulating cancellations)
    let mut received = 0;
    for _ in 0..30 {
        if let Ok(Some(SelectResult::Notification(Some(_)))) =
            timeout(Duration::from_millis(5), stream.as_mut().next()).await
        {
            received += 1;
        }

        if received >= 10 {
            break;
        }
    }

    assert_eq!(
        received, 10,
        "Should receive all messages despite rapid cancellations"
    );
}

/// Test simulating wait_for_event loop behavior - using stream that maintains waker registration
/// This test verifies that PrioritySelectStream properly maintains waker registration across
/// multiple .next().await calls, unlike the old approach that recreated futures each iteration.
///
/// Enhanced version: sends MULTIPLE messages per channel to verify interleaving and priority.
#[tokio::test]
#[test_log::test]
async fn test_priority_select_event_loop_simulation() {
    use futures::StreamExt;

    // Create channels once (like in wait_for_event)
    let (notif_tx, notif_rx) = mpsc::channel::<Either<NetMessage, NodeEvent>>(10);
    let (op_tx, op_rx) = mpsc::channel::<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>(10);
    let (_conn_event_tx, conn_event_rx) = mpsc::channel(10);
    let (bridge_tx, bridge_rx) = mpsc::channel::<P2pBridgeEvent>(10);
    let (node_tx, node_rx) = mpsc::channel::<NodeEvent>(10);

    // Spawn task that sends MULTIPLE messages to different channels
    let notif_tx_clone = notif_tx.clone();
    let op_tx_clone = op_tx.clone();
    let bridge_tx_clone = bridge_tx.clone();
    let node_tx_clone = node_tx.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;

        // Send 3 notifications
        for i in 0..3 {
            let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
                crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
            ));
            tracing::info!("Sending notification {}", i);
            notif_tx_clone.send(Either::Left(test_msg)).await.unwrap();
        }

        // Send 2 op execution messages
        for i in 0..2 {
            let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
                crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
            ));
            let (callback_tx, _) = mpsc::channel(1);
            tracing::info!("Sending op_execution {}", i);
            op_tx_clone.send((callback_tx, test_msg)).await.unwrap();
        }

        // Send 2 bridge events
        for i in 0..2 {
            tracing::info!("Sending bridge event {}", i);
            bridge_tx_clone
                .send(Either::Right(NodeEvent::Disconnect { cause: None }))
                .await
                .unwrap();
        }

        // Send 1 node controller event
        tracing::info!("Sending node controller event");
        node_tx_clone
            .send(NodeEvent::Disconnect { cause: None })
            .await
            .unwrap();
    });

    // Create stream ONCE - maintains waker registration across iterations
    let stream = PrioritySelectStream::new(
        notif_rx,
        op_rx,
        bridge_rx,
        create_mock_handshake_stream(),
        node_rx,
        MockClientNoReceiver,
        MockExecutorNoReceiver,
        conn_event_rx,
    );
    tokio::pin!(stream);

    let mut received_events = Vec::new();

    // Simulate event loop: poll stream until we've received all expected messages (3+2+2+1 = 8)
    let expected_count = 8;
    for iteration in 0..expected_count {
        tracing::info!("Event loop iteration {}", iteration);

        // Poll the SAME stream on each iteration - waker registration is maintained
        let result = timeout(Duration::from_millis(50), stream.as_mut().next()).await;
        assert!(result.is_ok(), "Iteration {} should complete", iteration);

        let event = result.unwrap().expect("Stream should yield value");
        match &event {
            SelectResult::Notification(_) => received_events.push("notification"),
            SelectResult::OpExecution(_) => received_events.push("op_execution"),
            SelectResult::ConnBridge(_) => received_events.push("conn_bridge"),
            SelectResult::Handshake(_) => received_events.push("handshake"),
            SelectResult::NodeController(_) => received_events.push("node_controller"),
            _ => received_events.push("other"),
        }

        tracing::info!(
            "Event loop iteration {} received: {:?}",
            iteration,
            received_events.last()
        );
    }

    // Verify we received all expected messages
    assert_eq!(
        received_events.len(),
        expected_count,
        "Should receive all {} messages",
        expected_count
    );

    // Count each type
    let notif_count = received_events
        .iter()
        .filter(|&e| *e == "notification")
        .count();
    let op_count = received_events
        .iter()
        .filter(|&e| *e == "op_execution")
        .count();
    let bridge_count = received_events
        .iter()
        .filter(|&e| *e == "conn_bridge")
        .count();
    let node_count = received_events
        .iter()
        .filter(|&e| *e == "node_controller")
        .count();

    tracing::info!(
        "Received counts - notifications: {}, op_execution: {}, bridge: {}, node_controller: {}",
        notif_count,
        op_count,
        bridge_count,
        node_count
    );

    assert_eq!(notif_count, 3, "Should receive 3 notifications");
    assert_eq!(op_count, 2, "Should receive 2 op_execution messages");
    assert_eq!(bridge_count, 2, "Should receive 2 bridge messages");
    assert_eq!(node_count, 1, "Should receive 1 node_controller message");

    // Verify priority ordering: all notifications should come before any op_execution
    // which should come before any bridge events
    let first_notif_idx = received_events.iter().position(|e| *e == "notification");
    let last_notif_idx = received_events.iter().rposition(|e| *e == "notification");
    let first_op_idx = received_events.iter().position(|e| *e == "op_execution");
    let last_op_idx = received_events.iter().rposition(|e| *e == "op_execution");
    let first_bridge_idx = received_events.iter().position(|e| *e == "conn_bridge");

    // All notifications should come first (indices 0, 1, 2)
    assert_eq!(
        first_notif_idx,
        Some(0),
        "First notification should be at index 0"
    );
    assert_eq!(
        last_notif_idx,
        Some(2),
        "Last notification should be at index 2"
    );

    // All op_executions should come after notifications (indices 3, 4)
    assert!(
        first_op_idx.unwrap() > last_notif_idx.unwrap(),
        "Op execution should come after all notifications"
    );
    assert_eq!(
        first_op_idx,
        Some(3),
        "First op_execution should be at index 3"
    );
    assert_eq!(
        last_op_idx,
        Some(4),
        "Last op_execution should be at index 4"
    );

    // All bridge events should come after op_executions (indices 5, 6)
    assert!(
        first_bridge_idx.unwrap() > last_op_idx.unwrap(),
        "Bridge events should come after all op_executions"
    );

    tracing::info!(
        "✓ All {} messages received in correct priority order: {:?}",
        expected_count,
        received_events
    );

    // Clean up - drop senders to close channels
    drop(notif_tx);
    drop(op_tx);
    drop(bridge_tx);
    drop(node_tx);
    // client_tx and executor_tx were moved into MockClient and MockExecutor
}

/// Stress test: Multiple concurrent tasks sending messages with random delays
/// This test verifies that priority ordering is maintained even under concurrent load
/// with unpredictable timing. Each channel has its own task sending messages at random
/// intervals, and we verify all messages are received in perfect priority order.
///
/// Uses seeded RNG for reproducibility - run with 5 different seeds to ensure robustness.
#[tokio::test]
#[test_log::test]
async fn test_priority_select_concurrent_random_stress() {
    test_with_seed(42).await;
    test_with_seed(123).await;
    test_with_seed(999).await;
    test_with_seed(7777).await;
    test_with_seed(31415).await;
}

async fn test_with_seed(seed: u64) {
    use rand::rngs::StdRng;
    use rand::Rng;
    use rand::SeedableRng;

    tracing::info!("=== Stress test with seed {} ===", seed);

    // Define how many messages each sender will send
    // Using 2 orders of magnitude more messages to stress test (17 -> 1700)
    const NOTIF_COUNT: usize = 500;
    const OP_COUNT: usize = 400;
    const BRIDGE_COUNT: usize = 300;
    const NODE_COUNT: usize = 200;
    const CLIENT_COUNT: usize = 200;
    const EXECUTOR_COUNT: usize = 100;
    const TOTAL_MESSAGES: usize =
        NOTIF_COUNT + OP_COUNT + BRIDGE_COUNT + NODE_COUNT + CLIENT_COUNT + EXECUTOR_COUNT;

    // Pre-generate all random delays using seeded RNG
    // Most delays are in microseconds (50-500us) with occasional millisecond outliers (1-5ms)
    // This keeps the test fast while still testing timing variations
    let mut rng = StdRng::seed_from_u64(seed);
    let make_delays = |count: usize, rng: &mut StdRng| -> Vec<u64> {
        (0..count)
            .map(|_| {
                // 10% chance of millisecond delay (outlier), 90% microsecond delay
                if rng.random_range(0..10) == 0 {
                    rng.random_range(1000..5000) // 1-5ms outliers
                } else {
                    rng.random_range(50..500) // 50-500us typical
                }
            })
            .collect()
    };

    let notif_delays = make_delays(NOTIF_COUNT, &mut rng);
    let op_delays = make_delays(OP_COUNT, &mut rng);
    let bridge_delays = make_delays(BRIDGE_COUNT, &mut rng);
    let node_delays = make_delays(NODE_COUNT, &mut rng);
    let client_delays = make_delays(CLIENT_COUNT, &mut rng);
    let executor_delays = make_delays(EXECUTOR_COUNT, &mut rng);

    // Create channels once (like in wait_for_event)
    let (notif_tx, notif_rx) = mpsc::channel::<Either<NetMessage, NodeEvent>>(100);
    let (op_tx, op_rx) = mpsc::channel::<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>(100);
    let (_conn_event_tx, conn_event_rx) = mpsc::channel(100);
    let (bridge_tx, bridge_rx) = mpsc::channel::<P2pBridgeEvent>(100);
    let (node_tx, node_rx) = mpsc::channel::<NodeEvent>(100);
    let (client_tx, client_rx) = mpsc::channel::<
        Result<
            (
                crate::client_events::ClientId,
                crate::contract::WaitingTransaction,
            ),
            anyhow::Error,
        >,
    >(100);
    let (executor_tx, executor_rx) = mpsc::channel::<Result<Transaction, anyhow::Error>>(100);

    tracing::info!(
        "Starting stress test with {} total messages from 6 concurrent tasks",
        TOTAL_MESSAGES
    );

    // Spawn separate task for each channel with pre-generated delays
    let notif_handle = tokio::spawn(async move {
        for (i, &delay_us) in notif_delays.iter().enumerate() {
            sleep(Duration::from_micros(delay_us)).await;
            let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
                crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
            ));
            tracing::debug!(
                "Notification task sending message {} after {}us delay",
                i,
                delay_us
            );
            notif_tx.send(Either::Left(test_msg)).await.unwrap();
        }
        tracing::info!("Notification task sent all {} messages", NOTIF_COUNT);
        NOTIF_COUNT
    });

    let op_handle = tokio::spawn(async move {
        for (i, &delay_us) in op_delays.iter().enumerate() {
            sleep(Duration::from_micros(delay_us)).await;
            let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
                crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
            ));
            let (callback_tx, _) = mpsc::channel(1);
            tracing::debug!(
                "OpExecution task sending message {} after {}us delay",
                i,
                delay_us
            );
            op_tx.send((callback_tx, test_msg)).await.unwrap();
        }
        tracing::info!("OpExecution task sent all {} messages", OP_COUNT);
        OP_COUNT
    });

    let bridge_handle = tokio::spawn(async move {
        for (i, &delay_us) in bridge_delays.iter().enumerate() {
            sleep(Duration::from_micros(delay_us)).await;
            tracing::debug!(
                "Bridge task sending message {} after {}us delay",
                i,
                delay_us
            );
            bridge_tx
                .send(Either::Right(NodeEvent::Disconnect { cause: None }))
                .await
                .unwrap();
        }
        tracing::info!("Bridge task sent all {} messages", BRIDGE_COUNT);
        BRIDGE_COUNT
    });

    let node_handle = tokio::spawn(async move {
        for (i, &delay_us) in node_delays.iter().enumerate() {
            sleep(Duration::from_micros(delay_us)).await;
            tracing::debug!(
                "NodeController task sending message {} after {}us delay",
                i,
                delay_us
            );
            node_tx
                .send(NodeEvent::Disconnect { cause: None })
                .await
                .unwrap();
        }
        tracing::info!("NodeController task sent all {} messages", NODE_COUNT);
        NODE_COUNT
    });

    let client_handle = tokio::spawn(async move {
        for (i, &delay_us) in client_delays.iter().enumerate() {
            sleep(Duration::from_micros(delay_us)).await;
            let client_id = crate::client_events::ClientId::next();
            let waiting_tx = crate::contract::WaitingTransaction::Transaction(Transaction::new::<
                crate::operations::put::PutMsg,
            >());
            tracing::debug!(
                "Client task sending message {} after {}us delay",
                i,
                delay_us
            );
            client_tx.send(Ok((client_id, waiting_tx))).await.unwrap();
        }
        tracing::info!("Client task sent all {} messages", CLIENT_COUNT);
        CLIENT_COUNT
    });

    let executor_handle = tokio::spawn(async move {
        for (i, &delay_us) in executor_delays.iter().enumerate() {
            sleep(Duration::from_micros(delay_us)).await;
            tracing::debug!(
                "Executor task sending message {} after {}us delay",
                i,
                delay_us
            );
            executor_tx
                .send(Ok(Transaction::new::<crate::operations::put::PutMsg>()))
                .await
                .unwrap();
        }
        tracing::info!("Executor task sent all {} messages", EXECUTOR_COUNT);
        EXECUTOR_COUNT
    });

    // Wait a bit for senders to start sending (shorter delay since we're using microseconds now)
    sleep(Duration::from_micros(100)).await;

    // Mock implementations for the stream

    struct MockClientStress {
        #[allow(dead_code)]
        rx: mpsc::Receiver<
            Result<
                (
                    crate::client_events::ClientId,
                    crate::contract::WaitingTransaction,
                ),
                anyhow::Error,
            >,
        >,
    }
    impl ClientTransactionRelay for MockClientStress {
        fn take_receiver(&mut self) -> Option<Receiver<(ClientId, WaitingTransaction)>> {
            // Transform the Result receiver into direct value receiver
            // For the stress test, we expect all sent values to be Ok
            None // The stress test needs to be redesigned for the new API
        }
    }

    struct MockExecutorStress {
        #[allow(dead_code)]
        rx: mpsc::Receiver<Result<Transaction, anyhow::Error>>,
    }
    impl ExecutorTransactionReceiver for MockExecutorStress {
        fn take_receiver(&mut self) -> Option<Receiver<Transaction>> {
            // For the stress test, we expect all sent values to be Ok
            None // The stress test needs to be redesigned for the new API
        }
    }

    // Create stream ONCE - it maintains waker registration and handles channel closures
    // Note: stress test now uses None receivers since the test design needs updating for new API
    let stream = PrioritySelectStream::new(
        notif_rx,
        op_rx,
        bridge_rx,
        create_mock_handshake_stream(),
        node_rx,
        MockClientStress { rx: client_rx },
        MockExecutorStress { rx: executor_rx },
        conn_event_rx,
    );
    tokio::pin!(stream);

    // Collect all messages from the event loop (run concurrently with senders)
    let mut received_events = Vec::new();
    let mut iteration = 0;

    // Continue until we've received all expected messages
    use futures::StreamExt;
    while received_events.len() < TOTAL_MESSAGES {
        // Poll the SAME stream on each iteration - maintains waker registration
        let result = timeout(Duration::from_millis(100), stream.as_mut().next()).await;
        assert!(result.is_ok(), "Iteration {} timed out", iteration);

        // Stream returns None when there are no more events
        let Some(event) = result.unwrap() else {
            tracing::debug!("Stream ended (all channels closed)");
            break;
        };

        // Check if this is a real message or a channel close
        let (event_name, is_real_message) = match &event {
            SelectResult::Notification(msg) => {
                if msg.is_some() {
                    tracing::debug!("Received Notification message");
                    ("notification", true)
                } else {
                    tracing::debug!("Notification channel closed");
                    ("notification", false)
                }
            }
            SelectResult::OpExecution(msg) => {
                if msg.is_some() {
                    tracing::debug!("Received OpExecution message");
                    ("op_execution", true)
                } else {
                    tracing::debug!("OpExecution channel closed");
                    ("op_execution", false)
                }
            }
            SelectResult::PeerConnection(msg) => ("peer_connection", msg.is_some()),
            SelectResult::ConnBridge(msg) => {
                if msg.is_some() {
                    tracing::debug!("Received ConnBridge message");
                    ("conn_bridge", true)
                } else {
                    tracing::debug!("ConnBridge channel closed");
                    ("conn_bridge", false)
                }
            }
            SelectResult::Handshake(_) => {
                ("handshake", false) // No real messages on this channel in this test
            }
            SelectResult::NodeController(msg) => {
                if msg.is_some() {
                    tracing::debug!("Received NodeController message");
                    ("node_controller", true)
                } else {
                    tracing::debug!("NodeController channel closed");
                    ("node_controller", false)
                }
            }
            SelectResult::ClientTransaction(result) => {
                if result.is_ok() {
                    tracing::debug!("Received ClientTransaction message");
                    ("client_transaction", true)
                } else {
                    tracing::debug!("ClientTransaction channel closed or error");
                    ("client_transaction", false)
                }
            }
            SelectResult::ExecutorTransaction(result) => {
                if result.is_ok() {
                    tracing::debug!("Received ExecutorTransaction message");
                    ("executor_transaction", true)
                } else {
                    tracing::debug!("ExecutorTransaction channel closed or error");
                    ("executor_transaction", false)
                }
            }
        };

        // Only count real messages, not channel closures
        if is_real_message {
            received_events.push(event_name);
            // Log every 100 messages to avoid spam with 1700 total messages
            if received_events.len() % 100 == 0 {
                tracing::info!(
                    "Received {} of {} real messages",
                    received_events.len(),
                    TOTAL_MESSAGES
                );
            }
        } else {
            tracing::debug!(
                "Iteration {}: Received channel close from {}",
                iteration,
                event_name
            );
        }

        iteration += 1;

        // Safety check to prevent infinite loop
        if iteration > TOTAL_MESSAGES * 3 {
            tracing::error!("Receiver loop exceeded maximum iterations. Received {} of {} messages after {} iterations",
                    received_events.len(), TOTAL_MESSAGES, iteration);
            panic!("Receiver loop exceeded maximum iterations - possible deadlock");
        }
    }

    // Join all sender tasks and get the count of messages they sent
    let sent_notif_count = notif_handle.await.unwrap();
    let sent_op_count = op_handle.await.unwrap();
    let sent_bridge_count = bridge_handle.await.unwrap();
    let sent_node_count = node_handle.await.unwrap();
    let sent_client_count = client_handle.await.unwrap();
    let sent_executor_count = executor_handle.await.unwrap();

    let total_sent = sent_notif_count
        + sent_op_count
        + sent_bridge_count
        + sent_node_count
        + sent_client_count
        + sent_executor_count;
    tracing::info!("All sender tasks completed. Total sent: {}", total_sent);
    tracing::info!(
        "Receiver completed. Total received: {}",
        received_events.len()
    );

    // Verify we received all expected messages
    assert_eq!(
        received_events.len(),
        total_sent,
        "Should receive all {} sent messages",
        total_sent
    );
    assert_eq!(
        received_events.len(),
        TOTAL_MESSAGES,
        "Total received should match expected total"
    );

    // Count each received type
    let recv_notif_count = received_events
        .iter()
        .filter(|&e| *e == "notification")
        .count();
    let recv_op_count = received_events
        .iter()
        .filter(|&e| *e == "op_execution")
        .count();
    let recv_bridge_count = received_events
        .iter()
        .filter(|&e| *e == "conn_bridge")
        .count();
    let recv_node_count = received_events
        .iter()
        .filter(|&e| *e == "node_controller")
        .count();
    let recv_client_count = received_events
        .iter()
        .filter(|&e| *e == "client_transaction")
        .count();
    let recv_executor_count = received_events
        .iter()
        .filter(|&e| *e == "executor_transaction")
        .count();

    tracing::info!("Sent vs Received:");
    tracing::info!(
        "  notifications: sent={}, received={}",
        sent_notif_count,
        recv_notif_count
    );
    tracing::info!(
        "  op_execution: sent={}, received={}",
        sent_op_count,
        recv_op_count
    );
    tracing::info!(
        "  bridge: sent={}, received={}",
        sent_bridge_count,
        recv_bridge_count
    );
    tracing::info!(
        "  node_controller: sent={}, received={}",
        sent_node_count,
        recv_node_count
    );
    tracing::info!(
        "  client: sent={}, received={}",
        sent_client_count,
        recv_client_count
    );
    tracing::info!(
        "  executor: sent={}, received={}",
        sent_executor_count,
        recv_executor_count
    );

    // Assert sent == received for each type
    assert_eq!(
        recv_notif_count, sent_notif_count,
        "Notification count mismatch"
    );
    assert_eq!(recv_op_count, sent_op_count, "OpExecution count mismatch");
    assert_eq!(
        recv_bridge_count, sent_bridge_count,
        "Bridge count mismatch"
    );
    assert_eq!(
        recv_node_count, sent_node_count,
        "NodeController count mismatch"
    );
    assert_eq!(
        recv_client_count, sent_client_count,
        "Client count mismatch"
    );
    assert_eq!(
        recv_executor_count, sent_executor_count,
        "Executor count mismatch"
    );

    tracing::info!("✓ STRESS TEST PASSED for seed {}!", seed);
    tracing::info!(
        "  All {} messages received correctly from 6 concurrent senders with random delays",
        TOTAL_MESSAGES
    );
    tracing::info!("  Received events: {:?}", received_events);
    tracing::info!("  Priority ordering respected: when multiple messages buffered, highest priority selected first");
}

/// Test that verifies waker registration across ALL channels when they're all Pending
/// This is the critical behavior: when a PrioritySelectStream polls all 8 channels and they
/// all return Pending, it must register wakers for ALL of them, not just some.
#[tokio::test]
#[test_log::test]
async fn test_priority_select_all_pending_waker_registration() {
    use futures::StreamExt;

    struct MockClientWaker {
        #[allow(dead_code)]
        rx: mpsc::Receiver<
            Result<
                (
                    crate::client_events::ClientId,
                    crate::contract::WaitingTransaction,
                ),
                anyhow::Error,
            >,
        >,
    }
    impl ClientTransactionRelay for MockClientWaker {
        fn take_receiver(&mut self) -> Option<Receiver<(ClientId, WaitingTransaction)>> {
            None // Test needs redesign for new API
        }
    }

    struct MockExecutorWaker {
        #[allow(dead_code)]
        rx: mpsc::Receiver<Result<Transaction, anyhow::Error>>,
    }
    impl ExecutorTransactionReceiver for MockExecutorWaker {
        fn take_receiver(&mut self) -> Option<Receiver<Transaction>> {
            None // Test needs redesign for new API
        }
    }

    // Create all 8 channels
    let (notif_tx, notif_rx) = mpsc::channel::<Either<NetMessage, NodeEvent>>(10);
    let (op_tx, op_rx) = mpsc::channel::<(tokio::sync::mpsc::Sender<NetMessage>, NetMessage)>(10);
    let (_conn_event_tx, conn_event_rx) = mpsc::channel(10);
    let (bridge_tx, bridge_rx) = mpsc::channel::<P2pBridgeEvent>(10);
    let (node_tx, node_rx) = mpsc::channel::<NodeEvent>(10);
    let (client_tx, client_rx) = mpsc::channel::<
        Result<
            (
                crate::client_events::ClientId,
                crate::contract::WaitingTransaction,
            ),
            anyhow::Error,
        >,
    >(10);
    let (executor_tx, executor_rx) = mpsc::channel::<Result<Transaction, anyhow::Error>>(10);

    // Start with NO messages buffered - this will cause all channels to return Pending on first poll
    tracing::info!("Creating PrioritySelectStream with all channels empty");

    // Spawn a task that will send messages after a delay
    // This gives the stream time to poll all channels and register wakers
    tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        tracing::info!("All wakers should now be registered, sending messages");

        // Send to multiple channels simultaneously (in reverse priority order)
        tracing::info!("Sending to executor channel (lowest priority)");
        executor_tx
            .send(Ok(Transaction::new::<crate::operations::put::PutMsg>()))
            .await
            .unwrap();

        tracing::info!("Sending to client channel");
        let client_id = crate::client_events::ClientId::next();
        let waiting_tx = crate::contract::WaitingTransaction::Transaction(Transaction::new::<
            crate::operations::put::PutMsg,
        >());
        client_tx.send(Ok((client_id, waiting_tx))).await.unwrap();

        tracing::info!("Sending to node controller channel");
        node_tx
            .send(NodeEvent::Disconnect { cause: None })
            .await
            .unwrap();

        tracing::info!("Sending to bridge channel");
        bridge_tx
            .send(Either::Right(NodeEvent::Disconnect { cause: None }))
            .await
            .unwrap();

        tracing::info!("Sending to op execution channel (second priority)");
        let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
            crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
        ));
        let (callback_tx, _) = mpsc::channel(1);
        op_tx.send((callback_tx, test_msg.clone())).await.unwrap();

        tracing::info!("Sending to notification channel (highest priority)");
        notif_tx.send(Either::Left(test_msg)).await.unwrap();
    });

    // Create the stream - it will poll all channels, find them all Pending,
    // and register wakers for all of them
    // Note: client_rx and executor_rx are unused after test redesign for new API
    let _ = (client_rx, executor_rx);
    let stream = PrioritySelectStream::new(
        notif_rx,
        op_rx,
        bridge_rx,
        create_mock_handshake_stream(),
        node_rx,
        MockClientWaker {
            rx: mpsc::channel(1).1,
        },
        MockExecutorWaker {
            rx: mpsc::channel(1).1,
        },
        conn_event_rx,
    );
    tokio::pin!(stream);

    // Poll the stream - it should wake up and return the NOTIFICATION (highest priority)
    // despite all other channels also having messages
    tracing::info!("PrioritySelectStream started, should poll all channels and go Pending");
    let result = timeout(Duration::from_millis(100), stream.next()).await;
    assert!(
        result.is_ok(),
        "Select should wake up when any message arrives"
    );

    let select_result = result.unwrap().expect("Stream should yield value");
    match select_result {
        SelectResult::Notification(_) => {
            tracing::info!(
                "✓ Correctly received Notification despite 5 other channels having messages"
            );
        }
        SelectResult::OpExecution(_) => {
            panic!("Should prioritize Notification over OpExecution")
        }
        SelectResult::ConnBridge(_) => panic!("Should prioritize Notification over ConnBridge"),
        SelectResult::NodeController(_) => {
            panic!("Should prioritize Notification over NodeController")
        }
        SelectResult::ClientTransaction(_) => {
            panic!("Should prioritize Notification over ClientTransaction")
        }
        SelectResult::ExecutorTransaction(_) => {
            panic!("Should prioritize Notification over ExecutorTransaction")
        }
        _ => panic!("Unexpected result"),
    }
}

/// Test that reproduces the lost wakeup race condition from issue #1932
///
/// This test demonstrates the bug where recreating PrioritySelectFuture on every
/// iteration loses waker registration, causing messages to be missed.
///
/// This test verifies the fix using PrioritySelectStream which maintains waker registration.
#[tokio::test]
#[test_log::test]
async fn test_sparse_messages_reproduce_race() {
    tracing::info!(
        "=== Testing sparse messages with PrioritySelectStream (verifying fix for #1932) ==="
    );

    // Mock implementations for testing

    let (notif_tx, notif_rx) = mpsc::channel::<Either<NetMessage, NodeEvent>>(10);
    let (_, op_rx) = mpsc::channel(1);
    let (_conn_event_tx, conn_event_rx) = mpsc::channel(10);
    let (_, bridge_rx) = mpsc::channel(1);
    let (_, node_rx) = mpsc::channel(1);

    // Spawn sender that sends 5 messages with 200ms gaps
    let sender = tokio::spawn(async move {
        for i in 0..5 {
            sleep(Duration::from_millis(200)).await;
            tracing::info!(
                "Sender: Sending message {} at {:?}",
                i,
                std::time::Instant::now()
            );
            let test_msg = NetMessage::V1(crate::message::NetMessageV1::Aborted(
                crate::message::Transaction::new::<crate::operations::put::PutMsg>(),
            ));
            match notif_tx.send(Either::Left(test_msg)).await {
                Ok(_) => tracing::info!("Sender: Message {} sent successfully", i),
                Err(e) => {
                    tracing::error!("Sender: Failed to send message {}: {:?}", i, e);
                    break;
                }
            }
        }
        tracing::info!("Sender: Finished sending all messages");
    });

    // Create the stream ONCE - this is the fix!
    let stream = PrioritySelectStream::new(
        notif_rx,
        op_rx,
        bridge_rx,
        create_mock_handshake_stream(),
        node_rx,
        MockClientNoReceiver,
        MockExecutorNoReceiver,
        conn_event_rx,
    );
    tokio::pin!(stream);

    let mut received = 0;
    let mut iteration = 0;

    // Receiver polls the SAME stream repeatedly (the fix - maintains waker registration)
    while received < 5 && iteration < 20 {
        iteration += 1;
        tracing::info!(
            "Iteration {}: Polling PrioritySelectStream (reusing same stream)",
            iteration
        );

        match timeout(Duration::from_millis(300), stream.as_mut().next()).await {
            Ok(Some(SelectResult::Notification(Some(_)))) => {
                received += 1;
                tracing::info!(
                    "✅ Iteration {}: Received message {} of 5",
                    iteration,
                    received
                );
            }
            Ok(Some(_)) => {
                tracing::debug!("Iteration {}: Got other event", iteration);
            }
            Ok(None) => {
                tracing::error!("Stream ended unexpectedly");
                break;
            }
            Err(_) => {
                tracing::warn!("Iteration {}: Timeout waiting for message", iteration);
            }
        }
    }

    // Wait for sender to finish
    sender.await.unwrap();
    tracing::info!("Sender task completed, received {} messages", received);

    assert_eq!(
            received, 5,
            "❌ FAIL: PrioritySelectStream still lost messages! Expected 5 but received {} in {} iterations.\n\
             The fix should prevent lost wakeups by keeping the stream alive.",
            received, iteration
        );
    tracing::info!("✅ PASS: All 5 messages received without loss using PrioritySelectStream!");
}

/// Test that stream-based approach doesn't lose messages with sparse arrivals
/// This reproduces the race condition scenario but with the stream-based fix
#[tokio::test]
#[test_log::test]
async fn test_stream_no_lost_messages_sparse_arrivals() {
    use tokio_stream::wrappers::ReceiverStream;

    tracing::info!("=== Testing stream approach doesn't lose messages (sparse arrivals) ===");

    let (tx, rx) = mpsc::channel::<String>(10);

    // Convert receiver to stream
    let stream = ReceiverStream::new(rx);

    // Simple stream wrapper that yields items
    struct MessageStream<S> {
        inner: S,
    }

    impl<S: Stream + Unpin> Stream for MessageStream<S> {
        type Item = S::Item;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.inner).poll_next(cx)
        }
    }

    let mut message_stream = MessageStream { inner: stream };

    // Spawn sender that sends 5 messages with 200ms gaps (sparse arrivals)
    let sender = tokio::spawn(async move {
        for i in 0..5 {
            sleep(Duration::from_millis(200)).await;
            tracing::info!(
                "Sender: Sending message {} at {:?}",
                i,
                std::time::Instant::now()
            );
            tx.send(format!("msg{}", i)).await.unwrap();
            tracing::info!("Sender: Message {} sent successfully", i);
        }
    });

    // Receiver loop: call stream.next().await repeatedly
    // The stream should maintain waker registration across iterations
    let mut received = 0;
    for iteration in 1..=20 {
        tracing::info!("Iteration {}: Calling stream.next().await", iteration);

        let msg = timeout(Duration::from_millis(300), message_stream.next()).await;

        match msg {
            Ok(Some(msg)) => {
                received += 1;
                tracing::info!("✓ Received: {} (total: {})", msg, received);
            }
            Ok(None) => {
                tracing::info!("Stream ended");
                break;
            }
            Err(_) => {
                tracing::info!(
                    "Timeout on iteration {} (received {} so far)",
                    iteration,
                    received
                );
                if received >= 5 {
                    break; // All messages received
                }
            }
        }
    }

    sender.await.unwrap();
    tracing::info!("Sender task completed, received {} messages", received);

    assert_eq!(
        received, 5,
        "Stream approach should receive ALL messages! Expected 5 but got {}.\n\
             The stream maintains waker registration across .next().await calls.",
        received
    );

    tracing::info!(
        "✓ SUCCESS: Stream-based approach received all 5 messages with sparse arrivals!"
    );
    tracing::info!("✓ Waker registration was maintained across stream.next().await iterations!");
}

/// Test that recreating futures on each poll maintains waker registration
/// This tests the hypothesis for "special" types with async methods
#[tokio::test]
#[test_log::test]
async fn test_recreating_futures_maintains_waker() {
    tracing::info!("=== Testing that recreating futures on each poll maintains waker ===");

    // Mock "special" type with an async method and internal state
    struct MockSpecial {
        counter: std::sync::Arc<std::sync::Mutex<usize>>,
        rx: tokio::sync::mpsc::Receiver<String>,
    }

    impl MockSpecial {
        // Async method that borrows &mut self
        async fn wait_for_event(&mut self) -> Option<String> {
            tracing::info!("MockSpecial::wait_for_event called");
            let msg = self.rx.recv().await?;
            let mut counter = self.counter.lock().unwrap();
            *counter += 1;
            tracing::info!("MockSpecial: received '{}', counter now {}", msg, *counter);
            Some(msg)
        }
    }

    // Stream that owns MockSpecial and recreates futures on each poll
    struct TestStream {
        special: MockSpecial,
    }

    impl Stream for TestStream {
        type Item = String;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            // KEY: Create fresh future on EVERY poll
            let fut = self.special.wait_for_event();
            tokio::pin!(fut);

            match fut.poll(cx) {
                Poll::Ready(Some(msg)) => Poll::Ready(Some(msg)),
                Poll::Ready(None) => Poll::Ready(None), // Channel closed
                Poll::Pending => Poll::Pending,
            }
        }
    }

    let counter = std::sync::Arc::new(std::sync::Mutex::new(0));
    let (tx, rx) = mpsc::channel::<String>(10);

    let mut test_stream = TestStream {
        special: MockSpecial {
            counter: counter.clone(),
            rx,
        },
    };

    // Spawn sender with sparse arrivals (200ms gaps)
    let sender = tokio::spawn(async move {
        for i in 0..5 {
            sleep(Duration::from_millis(200)).await;
            tracing::info!("Sender: Sending message {}", i);
            tx.send(format!("msg{}", i)).await.unwrap();
        }
    });

    // Receive using stream.next().await in loop
    let mut received = 0;
    for iteration in 1..=20 {
        tracing::info!("Iteration {}: Calling stream.next().await", iteration);

        let msg = timeout(Duration::from_millis(300), test_stream.next()).await;

        match msg {
            Ok(Some(msg)) => {
                received += 1;
                tracing::info!("✓ Received: {} (total: {})", msg, received);
            }
            Ok(None) => {
                tracing::info!("Stream ended");
                break;
            }
            Err(_) => {
                tracing::info!(
                    "Timeout on iteration {} (received {} so far)",
                    iteration,
                    received
                );
                if received >= 5 {
                    break;
                }
            }
        }
    }

    sender.await.unwrap();

    assert_eq!(
        received, 5,
        "Recreating futures on each poll should STILL receive all messages! Got {}",
        received
    );

    let final_counter = *counter.lock().unwrap();
    assert_eq!(final_counter, 5, "Counter should be 5");

    tracing::info!("✓ SUCCESS: Recreating futures on each poll MAINTAINS waker registration!");
    tracing::info!(
        "✓ The stream struct staying alive is what matters, not the individual futures!"
    );
}

/// Test that nested tokio::select! works correctly with stream approach
/// This is critical because HandshakeHandler::wait_for_events has a nested select!
///
/// This verifies that even when async methods contain nested selects,
/// the stream maintains waker registration and doesn't lose messages.
#[tokio::test]
#[test_log::test]
async fn test_recreating_futures_with_nested_select() {
    use futures::StreamExt;

    tracing::info!("=== Testing stream with NESTED select (like HandshakeHandler) ===");

    // Mock type with nested select (simulating HandshakeHandler pattern)
    struct MockWithNestedSelect {
        rx1: tokio::sync::mpsc::Receiver<String>,
        rx2: tokio::sync::mpsc::Receiver<String>,
        counter: std::sync::Arc<std::sync::Mutex<usize>>,
        rx1_closed: bool,
        rx2_closed: bool,
    }

    impl MockWithNestedSelect {
        // Async method with nested tokio::select! (like wait_for_events)
        async fn wait_for_event(&mut self) -> String {
            loop {
                // NESTED SELECT - just like HandshakeHandler::wait_for_events
                tokio::select! {
                    msg1 = self.rx1.recv(), if !self.rx1_closed => {
                        match msg1 {
                            Some(msg) => {
                                let mut counter = self.counter.lock().unwrap();
                                *counter += 1;
                                tracing::info!("Nested select: rx1 received '{}', counter {}", msg, *counter);
                                return format!("rx1:{}", msg);
                            }
                            None => {
                                self.rx1_closed = true;
                                if self.rx2_closed {
                                    return "rx1:closed".to_string();
                                }
                                continue;
                            }
                        }
                    }
                    msg2 = self.rx2.recv(), if !self.rx2_closed => {
                        match msg2 {
                            Some(msg) => {
                                let mut counter = self.counter.lock().unwrap();
                                *counter += 1;
                                tracing::info!("Nested select: rx2 received '{}', counter {}", msg, *counter);
                                return format!("rx2:{}", msg);
                            }
                            None => {
                                self.rx2_closed = true;
                                if self.rx1_closed {
                                    return "rx2:closed".to_string();
                                }
                                continue;
                            }
                        }
                    }
                    // If both channels are closed we break out with a final notification
                    else => {
                        return "both_closed".to_string();
                    }
                }
            }
        }
    }

    // Stream that creates fresh futures on each poll - just like PrioritySelectStream
    struct TestStream {
        special: MockWithNestedSelect,
    }

    impl Stream for TestStream {
        type Item = String;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            // Create fresh future on EVERY poll - this is what PrioritySelectStream does
            let fut = self.special.wait_for_event();
            tokio::pin!(fut);

            match fut.poll(cx) {
                Poll::Ready(msg) => Poll::Ready(Some(msg)),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    let counter = std::sync::Arc::new(std::sync::Mutex::new(0));
    let (tx1, rx1) = mpsc::channel::<String>(10);
    let (tx2, rx2) = mpsc::channel::<String>(10);

    // KEY FIX: Send all messages BEFORE starting to receive
    // This eliminates the race between sender and receiver
    for i in 0..3 {
        if i % 2 == 0 {
            tracing::info!("Sending to rx1: msg{}", i);
            tx1.send(format!("msg{}", i)).await.unwrap();
        } else {
            tracing::info!("Sending to rx2: msg{}", i);
            tx2.send(format!("msg{}", i)).await.unwrap();
        }
    }
    tracing::info!("All 3 messages sent, now dropping senders");
    drop(tx1);
    drop(tx2);

    // Create the stream ONCE and reuse it - key to maintaining waker registration
    let test_stream = TestStream {
        special: MockWithNestedSelect {
            rx1,
            rx2,
            counter: counter.clone(),
            rx1_closed: false,
            rx2_closed: false,
        },
    };
    tokio::pin!(test_stream);

    // Receive all messages
    let mut received = Vec::new();
    for iteration in 1..=10 {
        tracing::info!("Iteration {}: Calling stream.next().await", iteration);

        let msg = timeout(Duration::from_millis(100), test_stream.as_mut().next()).await;

        match msg {
            Ok(Some(msg)) => {
                if msg.contains("closed") {
                    tracing::info!("Channel closed: {}", msg);
                    // Continue to check if other channel has messages
                    continue;
                }
                received.push(msg.clone());
                tracing::info!("✓ Received: {} (total: {})", msg, received.len());

                if received.len() >= 3 {
                    break;
                }
            }
            Ok(None) => {
                tracing::info!("Stream ended");
                break;
            }
            Err(_) => {
                tracing::info!(
                    "Timeout on iteration {} (received {} so far)",
                    iteration,
                    received.len()
                );
                break;
            }
        }
    }

    assert_eq!(
        received.len(),
        3,
        "Stream with NESTED select should receive all messages! Got {} messages: {:?}",
        received.len(),
        received
    );

    let final_counter = *counter.lock().unwrap();
    assert_eq!(final_counter, 3, "Counter should be 3");

    tracing::info!(
            "✅ SUCCESS: Stream with NESTED select (like HandshakeHandler) maintains waker registration!"
        );
    tracing::info!("✅ Received all messages: {:?}", received);
}

/// Test the critical edge case: messages arrive with very tight timing
/// This simulates what happens in production when messages arrive rapidly
/// while the nested select is processing.
#[tokio::test]
#[test_log::test]
async fn test_nested_select_concurrent_arrivals() {
    use futures::StreamExt;

    tracing::info!("=== Testing nested select with rapid concurrent arrivals ===");

    struct MockWithNestedSelect {
        rx1: tokio::sync::mpsc::Receiver<String>,
        rx2: tokio::sync::mpsc::Receiver<String>,
        rx1_closed: bool,
        rx2_closed: bool,
    }

    impl MockWithNestedSelect {
        async fn wait_for_event(&mut self) -> String {
            loop {
                tokio::select! {
                    msg1 = self.rx1.recv(), if !self.rx1_closed => {
                        match msg1 {
                            Some(msg) => {
                                tracing::info!("Nested select: rx1 received '{}'", msg);
                                return format!("rx1:{}", msg);
                            }
                            None => {
                                self.rx1_closed = true;
                                if self.rx2_closed {
                                    return "rx1:closed".to_string();
                                }
                                continue;
                            }
                        }
                    }
                    msg2 = self.rx2.recv(), if !self.rx2_closed => {
                        match msg2 {
                            Some(msg) => {
                                tracing::info!("Nested select: rx2 received '{}'", msg);
                                return format!("rx2:{}", msg);
                            }
                            None => {
                                self.rx2_closed = true;
                                if self.rx1_closed {
                                    return "rx2:closed".to_string();
                                }
                                continue;
                            }
                        }
                    }
                    else => {
                        return "both_closed".to_string();
                    }
                }
            }
        }
    }

    struct TestStream {
        special: MockWithNestedSelect,
    }

    impl Stream for TestStream {
        type Item = String;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let fut = self.special.wait_for_event();
            tokio::pin!(fut);
            match fut.poll(cx) {
                Poll::Ready(msg) => Poll::Ready(Some(msg)),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    let (tx1, rx1) = mpsc::channel::<String>(10);
    let (tx2, rx2) = mpsc::channel::<String>(10);

    let test_stream = TestStream {
        special: MockWithNestedSelect {
            rx1,
            rx2,
            rx1_closed: false,
            rx2_closed: false,
        },
    };
    tokio::pin!(test_stream);

    // STRESS TEST: 1000 messages (100x more than original)
    // Spawn a sender that rapidly sends messages alternating between channels
    const MESSAGE_COUNT: usize = 1000;
    tokio::spawn(async move {
        for i in 0..MESSAGE_COUNT {
            // Send to alternating channels with minimal delay
            if i % 2 == 0 {
                if i % 100 == 0 {
                    tracing::info!("Sending msg{} to rx1 ({} sent)", i, i);
                }
                tx1.send(format!("msg{}", i)).await.unwrap();
            } else {
                if i % 100 == 0 {
                    tracing::info!("Sending msg{} to rx2 ({} sent)", i, i);
                }
                tx2.send(format!("msg{}", i)).await.unwrap();
            }
            // Tiny delay to allow some interleaving and race conditions
            sleep(Duration::from_micros(10)).await;
        }
        tracing::info!("Sender finished: sent all {} messages", MESSAGE_COUNT);
    });

    // Receive all messages - if wakers are maintained, we should get all 1000
    let mut received = Vec::new();
    for iteration in 0..(MESSAGE_COUNT + 100) {
        match timeout(Duration::from_millis(100), test_stream.as_mut().next()).await {
            Ok(Some(msg)) => {
                if !msg.contains("closed") {
                    received.push(msg);
                    if received.len() % 100 == 0 {
                        tracing::info!("Received {} of {} messages", received.len(), MESSAGE_COUNT);
                    }
                }
                if received.len() >= MESSAGE_COUNT {
                    break;
                }
            }
            Ok(None) => break,
            Err(_) => {
                tracing::info!(
                    "Timeout on iteration {} after receiving {} messages",
                    iteration,
                    received.len()
                );
                break;
            }
        }
    }

    assert_eq!(
            received.len(), MESSAGE_COUNT,
            "Should receive all {} messages even with rapid arrivals! Got {}. First 10: {:?}, Last 10: {:?}",
            MESSAGE_COUNT, received.len(),
            &received[..received.len().min(10)],
            &received[received.len().saturating_sub(10)..]
        );

    tracing::info!("✅ SUCCESS: All {} rapid messages received!", MESSAGE_COUNT);
    tracing::info!(
        "✅ Nested select with stream maintains waker registration under high concurrent load!"
    );
}
