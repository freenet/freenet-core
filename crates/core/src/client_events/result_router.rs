//! Result router for transaction results
//!
//! This module provides infrastructure for routing transaction results
//! from the network layer to the session actor.

use crate::contract::SessionMessage;
use crate::message::{QueryResult, Transaction};
use tokio::sync::mpsc;

/// Result router for transaction results
#[allow(dead_code)]
pub struct ResultRouter {
    network_results: mpsc::Receiver<(Transaction, QueryResult)>,
    session_actor_tx: mpsc::Sender<SessionMessage>,
}

impl ResultRouter {
    #[allow(dead_code)]
    pub fn new(
        network_results: mpsc::Receiver<(Transaction, QueryResult)>,
        session_actor_tx: mpsc::Sender<SessionMessage>,
    ) -> Self {
        Self {
            network_results,
            session_actor_tx,
        }
    }

    /// Main routing loop
    #[allow(dead_code)]
    pub async fn run(mut self) {
        while let Some((tx, result)) = self.network_results.recv().await {
            let msg = SessionMessage::DeliverResult {
                tx,
                result: Box::new(result),
            };
            if let Err(e) = self.session_actor_tx.try_send(msg) {
                tracing::warn!("Failed to route result to session actor: {}", e);
            }
        }
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
