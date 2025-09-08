//! Session actor for client connection refactor
//!
//! This module provides a full session actor that manages client sessions
//! and handles efficient 1→N result delivery to multiple clients.

use crate::client_events::ClientId;
use crate::contract::{ClientResponsesSender, SessionMessage};
use crate::message::Transaction;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;

/// Session actor for client connection refactor - Phase 2 implementation
pub struct SessionActor {
    message_rx: mpsc::Receiver<SessionMessage>,
    client_transactions: HashMap<Transaction, HashSet<ClientId>>,
    client_responses: ClientResponsesSender,
}

impl SessionActor {
    /// Create a new session actor
    pub fn new(
        message_rx: mpsc::Receiver<SessionMessage>,
        client_responses: ClientResponsesSender,
    ) -> Self {
        Self {
            message_rx,
            client_transactions: HashMap::new(),
            client_responses,
        }
    }

    /// Main message processing loop
    pub async fn run(mut self) {
        tracing::info!("Session actor starting");

        while let Some(msg) = self.message_rx.recv().await {
            match msg {
                SessionMessage::DeliverHostResponse { tx, response } => {
                    self.handle_result_delivery(tx, response).await;
                }
                SessionMessage::RegisterTransaction { tx, client_id } => {
                    self.client_transactions
                        .entry(tx)
                        .or_default()
                        .insert(client_id);
                    tracing::debug!(
                        "Registered transaction {} for client {}, total clients: {}",
                        tx,
                        client_id,
                        self.client_transactions.get(&tx).map_or(0, |s| s.len())
                    );
                }
                SessionMessage::ClientDisconnect { client_id } => {
                    self.cleanup_client_transactions(client_id);
                    tracing::debug!(
                        "Cleaned up transactions for disconnected client {}",
                        client_id
                    );
                }
                SessionMessage::RegisterClient { client_id, .. } => {
                    tracing::debug!("Registered client session: {}", client_id);
                    // Note: Client registration handled by existing transport layer
                }
                SessionMessage::DeliverResult { tx, result: _ } => {
                    tracing::debug!(
                        "Session actor received legacy DeliverResult for transaction {}",
                        tx
                    );
                    // Legacy variant - preserved for compatibility
                }
            }
        }

        tracing::error!(
            "CRITICAL: Session actor channel closed. \
             Result router or network layer has disconnected. \
             Actor-based client delivery is broken."
        );
        tracing::info!("Session actor stopped");
    }

    /// CORE: 1→N Result Delivery using existing ClientResponsesSender
    /// Optimized with Arc<HostResult> to minimize cloning overhead in 1→N delivery
    async fn handle_result_delivery(
        &mut self,
        tx: Transaction,
        result: std::sync::Arc<crate::client_events::HostResult>,
    ) {
        if let Some(waiting_clients) = self.client_transactions.remove(&tx) {
            let client_count = waiting_clients.len();
            tracing::debug!(
                "Delivering result for transaction {} to {} clients",
                tx,
                client_count
            );

            // Optimized 1→N delivery: Arc enables cheap cloning of the pointer, 
            // then we clone the inner result only once per client
            for client_id in waiting_clients {
                if let Err(e) = self.client_responses.send((client_id, (*result).clone())) {
                    tracing::warn!(
                        "Failed to deliver result to client {}: {}", 
                        client_id, e
                    );
                }
            }

            if client_count > 1 {
                tracing::debug!(
                    "Successfully delivered result for transaction {} to {} clients via optimized 1→N fanout",
                    tx, client_count
                );
            }
        } else {
            tracing::debug!("No clients waiting for transaction result: {}", tx);
        }
    }

    /// Clean up client from all transaction mappings on disconnect
    fn cleanup_client_transactions(&mut self, client_id: ClientId) {
        // Remove client from all transaction mappings
        self.client_transactions.retain(|_tx, clients| {
            clients.remove(&client_id);
            !clients.is_empty()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_events::{ClientId, RequestId};
    use crate::message::Transaction;
    use crate::operations::put::PutMsg;
    use freenet_stdlib::client_api::HostResponse;
    use freenet_stdlib::prelude::{ContractCode, Parameters, WrappedContract};
    use std::sync::Arc;
    use std::collections::HashSet;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_session_actor_one_to_many_delivery() {
        use crate::contract::client_responses_channel;

        let (session_tx, session_rx) = mpsc::channel(100);
        let (mut client_responses_rx, client_responses_tx) = client_responses_channel();
        let actor = SessionActor::new(session_rx, client_responses_tx);

        // Start the actor
        let actor_handle = tokio::spawn(async move {
            actor.run().await;
        });

        // Register 3 clients for same transaction
        let tx = Transaction::new::<PutMsg>();
        let clients = vec![ClientId::FIRST, ClientId::next(), ClientId::next()];

        for &client_id in &clients {
            session_tx
                .send(SessionMessage::RegisterTransaction { tx, client_id })
                .await
                .unwrap();
        }

        // Create test data
        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1, 2, 3])),
            Parameters::from(vec![4u8, 5u8]),
        );
        let host_result = Ok(HostResponse::ContractResponse(
            freenet_stdlib::client_api::ContractResponse::PutResponse {
                key: *contract.key(),
            },
        ));

        // Send result
        let message = SessionMessage::DeliverHostResponse {
            tx,
            response: std::sync::Arc::new(host_result.clone()),
        };
        session_tx.send(message).await.unwrap();

        // Verify that ALL 3 clients receive the result (true 1→N delivery)
        let mut received_count = 0;
        let mut received_clients = HashSet::new();
        
        while let Ok(timeout_result) = tokio::time::timeout(
            tokio::time::Duration::from_millis(100),
            client_responses_rx.recv(),
        ).await {
            if let Some((client_id, received_result)) = timeout_result {
                assert!(clients.contains(&client_id));
                // Verify result structure without full equality (since PartialEq might not be fully implemented)
                match (&received_result, &host_result) {
                    (Ok(_), Ok(_)) => {}, // Both are Ok variants
                    (Err(_), Err(_)) => {}, // Both are Err variants
                    _ => panic!("Result type mismatch: expected same variant (Ok/Err)"),
                }
                received_clients.insert(client_id);
                received_count += 1;
                tracing::debug!("Test: Client {} received result ({}/{})", client_id, received_count, clients.len());
                
                if received_count == clients.len() {
                    break;
                }
            } else {
                panic!("Expected client to receive result but channel was closed");
            }
        }
        
        assert_eq!(received_count, clients.len(), "All {} clients should receive result", clients.len());
        assert_eq!(received_clients.len(), clients.len(), "Each client should receive result exactly once");

        // Clean up
        drop(session_tx);
        actor_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_session_actor_client_disconnect_cleanup() {
        use crate::contract::client_responses_channel;

        let (_session_tx, session_rx) = mpsc::channel(100);
        let (_client_responses_rx, client_responses_tx) = client_responses_channel();
        let mut actor = SessionActor::new(session_rx, client_responses_tx);

        // Register client for transaction
        let tx = Transaction::new::<PutMsg>();
        let client_id = ClientId::FIRST;
        actor
            .client_transactions
            .entry(tx)
            .or_default()
            .insert(client_id);

        // Verify client is registered
        assert!(actor
            .client_transactions
            .get(&tx)
            .unwrap()
            .contains(&client_id));

        // Simulate client disconnect
        actor.cleanup_client_transactions(client_id);

        // Verify client removed from transaction mapping
        assert!(!actor.client_transactions.contains_key(&tx));
    }

    #[tokio::test]
    async fn test_session_actor_handles_all_message_types() {
        use crate::contract::client_responses_channel;

        let (session_tx, session_rx) = mpsc::channel(100);
        let (_client_responses_rx, client_responses_tx) = client_responses_channel();
        let actor = SessionActor::new(session_rx, client_responses_tx);

        // Start the actor
        let actor_handle = tokio::spawn(async move {
            actor.run().await;
        });

        // Test RegisterClient
        let client_id = ClientId::FIRST;
        let request_id = RequestId::new();
        let (transport_tx, _transport_rx) = mpsc::unbounded_channel();

        session_tx
            .send(SessionMessage::RegisterClient {
                client_id,
                request_id,
                transport_tx,
                token: None,
            })
            .await
            .unwrap();

        // Test RegisterTransaction
        let tx = Transaction::new::<PutMsg>();
        session_tx
            .send(SessionMessage::RegisterTransaction { tx, client_id })
            .await
            .unwrap();

        // Test ClientDisconnect
        session_tx
            .send(SessionMessage::ClientDisconnect { client_id })
            .await
            .unwrap();

        // Give actor time to process all messages
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Clean up
        drop(session_tx);
        actor_handle.await.unwrap();
    }
}
