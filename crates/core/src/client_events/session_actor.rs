//! Session actor stub for client connection refactor
//!
//! This module provides a basic session actor that logs received messages
//! and serves as the foundation for future client session management.

use crate::contract::SessionMessage;
use tokio::sync::mpsc;

/// Session actor stub for client connection refactor
pub struct SessionActorStub {
    message_rx: mpsc::Receiver<SessionMessage>,
}

impl SessionActorStub {
    /// Create a new session actor stub
    pub fn new(message_rx: mpsc::Receiver<SessionMessage>) -> Self {
        Self { message_rx }
    }

    /// Main message processing loop
    pub async fn run(mut self) {
        tracing::info!("Session actor stub starting");

        while let Some(message) = self.message_rx.recv().await {
            match message {
                SessionMessage::DeliverHostResponse { tx, response } => {
                    tracing::debug!(
                        "Session actor received DeliverHostResponse for transaction {}: {:?}",
                        tx,
                        match *response {
                            Ok(_) => "Success",
                            Err(_) => "Error",
                        }
                    );
                    // Future: This will route to client sessions
                }
                SessionMessage::RegisterClient {
                    client_id,
                    request_id,
                    ..
                } => {
                    tracing::debug!(
                        "Session actor received RegisterClient: client={}, request={}",
                        client_id,
                        request_id
                    );
                    // Future: This will create client sessions
                }
                SessionMessage::RegisterTransaction { tx, client_id } => {
                    tracing::debug!(
                        "Session actor received RegisterTransaction: tx={}, client={}",
                        tx,
                        client_id
                    );
                    // Future: This will track client-transaction relationships
                }
                SessionMessage::ClientDisconnect { client_id } => {
                    tracing::debug!(
                        "Session actor received ClientDisconnect: client={}",
                        client_id
                    );
                    // Future: This will clean up client sessions
                }
                SessionMessage::DeliverResult { tx, result: _ } => {
                    tracing::debug!(
                        "Session actor received legacy DeliverResult for transaction {}: {:?}",
                        tx,
                        "QueryResult" // Don't log full QueryResult as it doesn't implement Display
                    );
                    // Legacy variant - will be removed in future refactoring
                }
            }
        }

        tracing::info!("Session actor stub stopped");
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
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_session_actor_receives_messages() {
        let (session_tx, session_rx) = mpsc::channel(100);
        let actor = SessionActorStub::new(session_rx);

        // Start the actor
        let actor_handle = tokio::spawn(async move {
            actor.run().await;
        });

        // Create test data
        let tx = Transaction::new::<PutMsg>();
        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1, 2, 3])),
            Parameters::from(vec![4u8, 5u8]),
        );
        let host_result = Ok(HostResponse::ContractResponse(
            freenet_stdlib::client_api::ContractResponse::PutResponse {
                key: *contract.key(),
            },
        ));

        // Send test message
        let message = SessionMessage::DeliverHostResponse {
            tx,
            response: Box::new(host_result),
        };

        session_tx.send(message).await.unwrap();

        // Give actor time to process
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Clean up
        drop(session_tx);
        actor_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_session_actor_handles_all_message_types() {
        let (session_tx, session_rx) = mpsc::channel(100);
        let actor = SessionActorStub::new(session_rx);

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
