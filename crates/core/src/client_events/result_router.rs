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
            let msg = SessionMessage::DeliverHostResponse {
                tx,
                response: std::sync::Arc::new(host_result),
            };
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
    // use crate::client_events::ClientId;  // Future use

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
}
