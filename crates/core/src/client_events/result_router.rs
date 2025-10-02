//! Result router for transaction results
//!
//! This module provides infrastructure for routing transaction results
//! from the network layer to the session actor.

use crate::client_events::HostResult;
use crate::contract::SessionMessage;
use crate::message::Transaction;
use tokio::sync::mpsc;

/// Result router for transaction results
pub struct ResultRouter {
    network_results: mpsc::Receiver<(Transaction, HostResult)>,
    session_actor_tx: mpsc::Sender<SessionMessage>,
}

impl ResultRouter {
    pub fn new(
        network_results: mpsc::Receiver<(Transaction, HostResult)>,
        session_actor_tx: mpsc::Sender<SessionMessage>,
    ) -> Self {
        Self {
            network_results,
            session_actor_tx,
        }
    }

    /// Main routing loop
    pub async fn run(mut self) {
        while let Some((tx, host_result)) = self.network_results.recv().await {
            tracing::info!("ResultRouter received result for transaction: {}", tx);
            let msg = SessionMessage::DeliverHostResponse {
                tx,
                response: std::sync::Arc::new(host_result),
            };
            tracing::info!(
                "ResultRouter sending result to SessionActor for transaction: {}",
                tx
            );
            if let Err(e) = self.session_actor_tx.send(msg).await {
                // TODO: Add metric for router send failures
                // metrics::ROUTER_SEND_FAILURES.increment();

                // mpsc::error::SendError only occurs when channel is closed
                let error_reason = "channel_closed";

                tracing::error!(
                    error_reason = error_reason,
                    transaction = %tx,
                    error = %e,
                    "CRITICAL: Session actor channel send failed - result routing failed. \
                     Session actor has crashed. Actor-based client delivery is broken."
                );
                // Router can't continue without session actor - exit loop
                break;
            }
        }

        tracing::error!(
            "CRITICAL: ResultRouter shutting down due to session actor failure. \
             Dual-path delivery compromised. Consider restarting node."
        );
    }
}

// Future: Result publishing integration point
// pub fn publish_transaction_result(tx: Transaction, result: QueryResult) {
//     // Hook result publishing here during client response conversion
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::get::GetMsg;
    use freenet_stdlib::client_api::HostResponse;

    fn create_test_transaction() -> Transaction {
        Transaction::new::<GetMsg>()
    }

    fn create_test_host_result() -> HostResult {
        Ok(HostResponse::Ok)
    }

    #[tokio::test]
    async fn test_result_router_creation() {
        let (network_tx, network_rx) = mpsc::channel(100);
        let (session_tx, _session_rx) = mpsc::channel(100);

        let router = ResultRouter::new(network_rx, session_tx);

        // Test router creation
        drop(router);
        drop(network_tx);
    }

    #[tokio::test]
    async fn test_result_routing() {
        let (network_tx, network_rx) = mpsc::channel(100);
        let (session_tx, mut session_rx) = mpsc::channel(100);

        let router = ResultRouter::new(network_rx, session_tx);

        // Spawn the router
        tokio::spawn(async move {
            router.run().await;
        });

        // Test basic routing infrastructure
        drop(network_tx);

        // Give time for router to process and close
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Ensure session channel is empty (no routing yet)
        assert!(session_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_single_result_delivery() {
        let (network_tx, network_rx) = mpsc::channel(100);
        let (session_tx, mut session_rx) = mpsc::channel(100);

        let router = ResultRouter::new(network_rx, session_tx);

        // Spawn the router
        tokio::spawn(async move {
            router.run().await;
        });

        let tx = create_test_transaction();
        let result = create_test_host_result();

        // Send a result through the network channel
        network_tx
            .send((tx, result.clone()))
            .await
            .expect("Failed to send result");

        // Verify the result is delivered to session actor
        let received =
            tokio::time::timeout(tokio::time::Duration::from_millis(100), session_rx.recv())
                .await
                .expect("Timeout waiting for session message")
                .expect("Channel closed unexpectedly");

        match received {
            SessionMessage::DeliverHostResponse {
                tx: received_tx,
                response,
            } => {
                assert_eq!(received_tx, tx);
                assert!(response.is_ok(), "Expected success result");
            }
            _ => panic!("Unexpected message type"),
        }
    }

    #[tokio::test]
    async fn test_multiple_results_delivery() {
        let (network_tx, network_rx) = mpsc::channel(100);
        let (session_tx, mut session_rx) = mpsc::channel(100);

        let router = ResultRouter::new(network_rx, session_tx);

        tokio::spawn(async move {
            router.run().await;
        });

        // Send 3 different results
        let tx1 = create_test_transaction();
        let tx2 = create_test_transaction();
        let tx3 = create_test_transaction();

        let result = create_test_host_result();

        network_tx.send((tx1, result.clone())).await.unwrap();
        network_tx.send((tx2, result.clone())).await.unwrap();
        network_tx.send((tx3, result.clone())).await.unwrap();

        // Verify all 3 results are delivered in order
        for expected_tx in [tx1, tx2, tx3] {
            let received =
                tokio::time::timeout(tokio::time::Duration::from_millis(100), session_rx.recv())
                    .await
                    .expect("Timeout waiting for session message")
                    .expect("Channel closed unexpectedly");

            match received {
                SessionMessage::DeliverHostResponse {
                    tx: received_tx,
                    response,
                } => {
                    assert_eq!(received_tx, expected_tx);
                    assert!(response.is_ok(), "Expected success result");
                }
                _ => panic!("Unexpected message type"),
            }
        }
    }

    #[tokio::test]
    async fn test_session_actor_channel_closed() {
        let (network_tx, network_rx) = mpsc::channel(100);
        let (session_tx, session_rx) = mpsc::channel(100);

        let router = ResultRouter::new(network_rx, session_tx);

        let router_handle = tokio::spawn(async move {
            router.run().await;
        });

        // Close session actor channel (simulating session actor crash)
        drop(session_rx);

        // Send a result - router should detect closed channel and exit
        let tx = create_test_transaction();
        let result = create_test_host_result();
        network_tx.send((tx, result)).await.unwrap();

        // Router should exit gracefully
        tokio::time::timeout(tokio::time::Duration::from_millis(100), router_handle)
            .await
            .expect("Router should exit when session actor channel closes")
            .expect("Router task should complete successfully");
    }

    #[tokio::test]
    async fn test_concurrent_result_delivery() {
        let (network_tx, network_rx) = mpsc::channel(100);
        let (session_tx, mut session_rx) = mpsc::channel(100);

        let router = ResultRouter::new(network_rx, session_tx);

        tokio::spawn(async move {
            router.run().await;
        });

        // Send 10 results concurrently
        let mut handles = vec![];
        for _ in 0..10 {
            let tx_clone = network_tx.clone();
            let handle = tokio::spawn(async move {
                let tx = create_test_transaction();
                let result = create_test_host_result();
                tx_clone.send((tx, result)).await.unwrap();
            });
            handles.push(handle);
        }

        // Wait for all sends to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all 10 results are delivered
        for _ in 0..10 {
            let received =
                tokio::time::timeout(tokio::time::Duration::from_millis(100), session_rx.recv())
                    .await
                    .expect("Timeout waiting for session message")
                    .expect("Channel closed unexpectedly");

            assert!(
                matches!(received, SessionMessage::DeliverHostResponse { .. }),
                "Expected DeliverHostResponse message"
            );
        }

        // Ensure no extra messages
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert!(
            session_rx.try_recv().is_err(),
            "No extra messages should be present"
        );
    }

    #[tokio::test]
    async fn test_network_channel_closed() {
        let (network_tx, network_rx) = mpsc::channel(100);
        let (session_tx, mut session_rx) = mpsc::channel(100);

        let router = ResultRouter::new(network_rx, session_tx);

        let router_handle = tokio::spawn(async move {
            router.run().await;
        });

        // Send a result, then close network channel
        let tx = create_test_transaction();
        let result = create_test_host_result();
        network_tx.send((tx, result.clone())).await.unwrap();
        drop(network_tx);

        // Verify result is delivered
        let received =
            tokio::time::timeout(tokio::time::Duration::from_millis(100), session_rx.recv())
                .await
                .expect("Timeout waiting for session message")
                .expect("Channel closed unexpectedly");

        match received {
            SessionMessage::DeliverHostResponse {
                tx: received_tx,
                response,
            } => {
                assert_eq!(received_tx, tx);
                assert!(response.is_ok(), "Expected success result");
            }
            _ => panic!("Unexpected message type"),
        }

        // Router should exit gracefully after network channel closes
        tokio::time::timeout(tokio::time::Duration::from_millis(100), router_handle)
            .await
            .expect("Router should exit when network channel closes")
            .expect("Router task should complete successfully");
    }
}
