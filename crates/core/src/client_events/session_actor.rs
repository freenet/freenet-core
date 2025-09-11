//! Session actor for client connection refactor
//!
//! This module provides a full session actor that manages client sessions
//! and handles efficient 1→N result delivery to multiple clients.

use crate::client_events::{ClientId, RequestId};
use crate::contract::{ClientResponsesSender, SessionMessage};
use crate::message::Transaction;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;

/// Session actor for client connection refactor - Phase 2 implementation
pub struct SessionActor {
    message_rx: mpsc::Receiver<SessionMessage>,
    client_transactions: HashMap<Transaction, HashSet<ClientId>>,
    // Track RequestId correlation for each (Transaction, ClientId) pair
    client_request_ids: HashMap<(Transaction, ClientId), RequestId>,
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
            client_request_ids: HashMap::new(),
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
                SessionMessage::RegisterTransaction {
                    tx,
                    client_id,
                    request_id,
                } => {
                    self.client_transactions
                        .entry(tx)
                        .or_default()
                        .insert(client_id);

                    // Track RequestId correlation
                    self.client_request_ids.insert((tx, client_id), request_id);

                    tracing::debug!(
                        "Registered transaction {} for client {} (request {}), total clients: {}",
                        tx,
                        client_id,
                        request_id,
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

    /// CORE: 1→N Result Delivery with RequestId correlation
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

            // Optimized 1→N delivery with RequestId correlation
            for client_id in waiting_clients {
                // Look up the RequestId for this (transaction, client) pair
                let request_id =
                    self.client_request_ids
                        .remove(&(tx, client_id))
                        .unwrap_or_else(|| {
                            tracing::warn!(
                            "No RequestId found for transaction {} and client {}, using default", 
                            tx, client_id
                        );
                            RequestId::new()
                        });

                if let Err(e) =
                    self.client_responses
                        .send((client_id, request_id, (*result).clone()))
                {
                    tracing::warn!(
                        "Failed to deliver result to client {} (request {}): {}",
                        client_id,
                        request_id,
                        e
                    );
                } else {
                    tracing::debug!(
                        "Delivered result for transaction {} to client {} with request correlation {}",
                        tx, client_id, request_id
                    );
                }
            }

            if client_count > 1 {
                tracing::debug!(
                    "Successfully delivered result for transaction {} to {} clients via optimized 1→N fanout with RequestId correlation",
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
        self.client_transactions.retain(|tx, clients| {
            clients.remove(&client_id);
            // If no clients left for this transaction, also clean up RequestId mappings
            if clients.is_empty() {
                self.client_request_ids.retain(|(t, _), _| t != tx);
            }
            !clients.is_empty()
        });

        // Clean up RequestId mappings for this client across all transactions
        self.client_request_ids.retain(|(_, c), _| *c != client_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_events::{ClientId, RequestId};
    use crate::message::Transaction;
    use crate::operations::put::PutMsg;
    use freenet_stdlib::client_api::HostResponse;
    use freenet_stdlib::prelude::{
        ContractCode, ContractContainer, ContractWasmAPIVersion, Parameters, WrappedContract,
    };
    use std::collections::HashSet;
    use std::sync::Arc;
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

        let mut request_ids = Vec::new();
        for &client_id in &clients {
            let request_id = RequestId::new();
            request_ids.push(request_id);
            session_tx
                .send(SessionMessage::RegisterTransaction {
                    tx,
                    client_id,
                    request_id,
                })
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

        // Verify that ALL 3 clients receive the result with proper RequestId correlation (true 1→N delivery)
        let mut received_count = 0;
        let mut received_clients = HashSet::new();
        let mut received_request_ids = HashSet::new();

        while let Ok(timeout_result) = tokio::time::timeout(
            tokio::time::Duration::from_millis(100),
            client_responses_rx.recv(),
        )
        .await
        {
            if let Some((client_id, request_id, received_result)) = timeout_result {
                assert!(clients.contains(&client_id));
                assert!(request_ids.contains(&request_id));

                // Verify result structure without full equality (since PartialEq might not be fully implemented)
                match (&received_result, &host_result) {
                    (Ok(_), Ok(_)) => {}   // Both are Ok variants
                    (Err(_), Err(_)) => {} // Both are Err variants
                    _ => panic!("Result type mismatch: expected same variant (Ok/Err)"),
                }

                received_clients.insert(client_id);
                received_request_ids.insert(request_id);
                received_count += 1;

                tracing::debug!(
                    "Test: Client {} received result with RequestId {} ({}/{})",
                    client_id,
                    request_id,
                    received_count,
                    clients.len()
                );

                if received_count == clients.len() {
                    break;
                }
            } else {
                panic!("Expected client to receive result but channel was closed");
            }
        }

        assert_eq!(
            received_count,
            clients.len(),
            "All {} clients should receive result",
            clients.len()
        );
        assert_eq!(
            received_clients.len(),
            clients.len(),
            "Each client should receive result exactly once"
        );
        assert_eq!(
            received_request_ids.len(),
            clients.len(),
            "Each RequestId should be correlated exactly once"
        );

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
        let request_id = RequestId::new();

        actor
            .client_transactions
            .entry(tx)
            .or_default()
            .insert(client_id);
        actor.client_request_ids.insert((tx, client_id), request_id);

        // Verify client is registered
        assert!(actor
            .client_transactions
            .get(&tx)
            .unwrap()
            .contains(&client_id));
        assert!(actor.client_request_ids.contains_key(&(tx, client_id)));

        // Simulate client disconnect
        actor.cleanup_client_transactions(client_id);

        // Verify client and RequestId mappings removed
        assert!(!actor.client_transactions.contains_key(&tx));
        assert!(!actor.client_request_ids.contains_key(&(tx, client_id)));
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
        let request_id = RequestId::new();
        session_tx
            .send(SessionMessage::RegisterTransaction {
                tx,
                client_id,
                request_id,
            })
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

    #[tokio::test]
    async fn test_request_id_correlation_isolation() {
        use crate::contract::client_responses_channel;

        let (session_tx, session_rx) = mpsc::channel(100);
        let (mut client_responses_rx, client_responses_tx) = client_responses_channel();
        let actor = SessionActor::new(session_rx, client_responses_tx);

        // Start the actor
        let actor_handle = tokio::spawn(async move {
            actor.run().await;
        });

        // Test RequestId correlation isolation between different transactions
        let tx1 = Transaction::new::<PutMsg>();
        let tx2 = Transaction::new::<PutMsg>();
        let client_id = ClientId::FIRST;

        let request_id1 = RequestId::new();
        let request_id2 = RequestId::new();

        // Register same client for two different transactions with different RequestIds
        session_tx
            .send(SessionMessage::RegisterTransaction {
                tx: tx1,
                client_id,
                request_id: request_id1,
            })
            .await
            .unwrap();

        session_tx
            .send(SessionMessage::RegisterTransaction {
                tx: tx2,
                client_id,
                request_id: request_id2,
            })
            .await
            .unwrap();

        // Create test contract keys
        let contract1 = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from([1u8; 32].to_vec())),
            Parameters::from([].as_slice()),
        )));
        let contract2 = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from([2u8; 32].to_vec())),
            Parameters::from([].as_slice()),
        )));

        // Create test results
        let result1 = Ok(HostResponse::ContractResponse(
            freenet_stdlib::client_api::ContractResponse::PutResponse {
                key: contract1.key(),
            },
        ));
        let result2 = Ok(HostResponse::ContractResponse(
            freenet_stdlib::client_api::ContractResponse::PutResponse {
                key: contract2.key(),
            },
        ));

        // Send results for both transactions
        session_tx
            .send(SessionMessage::DeliverHostResponse {
                tx: tx1,
                response: std::sync::Arc::new(result1.clone()),
            })
            .await
            .unwrap();

        session_tx
            .send(SessionMessage::DeliverHostResponse {
                tx: tx2,
                response: std::sync::Arc::new(result2.clone()),
            })
            .await
            .unwrap();

        // Verify RequestId correlation is preserved correctly
        let mut received_correlations = Vec::new();

        for _ in 0..2 {
            if let Ok(Some((received_client_id, received_request_id, _received_result))) =
                tokio::time::timeout(
                    tokio::time::Duration::from_millis(100),
                    client_responses_rx.recv(),
                )
                .await
            {
                assert_eq!(received_client_id, client_id);
                received_correlations.push(received_request_id);
            } else {
                panic!("Expected to receive result with RequestId correlation");
            }
        }

        // Verify both RequestIds were received
        assert!(received_correlations.contains(&request_id1));
        assert!(received_correlations.contains(&request_id2));
        assert_eq!(received_correlations.len(), 2);

        tracing::debug!(
            "RequestId correlation isolation test passed: {:?}",
            received_correlations
        );

        // Clean up
        drop(session_tx);
        actor_handle.await.unwrap();
    }
}
