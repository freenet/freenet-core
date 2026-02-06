//! Message processor for clean client handling separation
//!
//! This module provides a MessageProcessor enum that cleanly separates
//! network message processing from client notification logic, enabling
//! pure network processing when the actor system is enabled.

use crate::client_events::HostResult;
use crate::contract::SessionMessage;
use crate::message::Transaction;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::debug;

/// Errors that can occur during message processing
#[derive(Debug, thiserror::Error)]
pub enum ProcessingError {
    #[error("Actor communication error: {0}")]
    ActorCommunication(#[from] mpsc::error::SendError<SessionMessage>),
}

/// MessageProcessor for pure network-to-actor routing
///
/// Routes network operation results to the session actor for client notification
pub struct MessageProcessor {
    result_tx: mpsc::Sender<SessionMessage>,
}

impl MessageProcessor {
    /// Create a new MessageProcessor
    pub fn new(result_tx: mpsc::Sender<SessionMessage>) -> Self {
        Self { result_tx }
    }

    /// Handle network result with pure separation - no client types in network layer
    /// This method is called from the pure network layer and routes results to SessionActor
    pub async fn handle_network_result(
        &self,
        tx: Transaction,
        host_result: Option<HostResult>,
    ) -> Result<(), ProcessingError> {
        // Pure result forwarding to SessionActor
        self.route_to_session_actor(tx, host_result).await
    }

    /// Route network result to SessionActor - no client parameters needed
    async fn route_to_session_actor(
        &self,
        tx: Transaction,
        host_result: Option<HostResult>,
    ) -> Result<(), ProcessingError> {
        let status = match &host_result {
            Some(Ok(_)) => "op_result",
            Some(Err(_)) => "error",
            None => "no_result",
        };
        tracing::debug!(%tx, status, "Routing network result to SessionActor");

        let Some(host_result) = host_result else {
            debug!("No result to forward for transaction {}", tx);
            return Ok(());
        };

        // Create session message for pure actor routing
        // The SessionActor will handle all client correlation internally
        let session_msg = SessionMessage::DeliverHostResponse {
            tx,
            response: Arc::new(host_result),
        };

        // Send to SessionActor - it handles all client concerns
        if let Err(e) = self.result_tx.send(session_msg).await {
            tracing::error!(
                "Failed to send result to SessionActor for transaction {}: {}",
                tx,
                e
            );
            return Err(ProcessingError::ActorCommunication(e));
        }

        debug!(
            "Pure network result routed to SessionActor for transaction {}",
            tx
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::get;
    use crate::operations::OpEnum;
    use tokio::sync::mpsc;

    fn create_test_transaction() -> Transaction {
        Transaction::new::<crate::operations::get::GetMsg>()
    }

    fn create_success_host_result() -> Option<HostResult> {
        use freenet_stdlib::prelude::ContractInstanceId;
        let instance_id = ContractInstanceId::new([1u8; 32]);
        let get_op = get::start_op(instance_id, false, false, false);
        Some(OpEnum::Get(get_op).to_host_result())
    }

    fn create_none_host_result() -> Option<HostResult> {
        None
    }

    fn create_error_host_result() -> Option<HostResult> {
        use freenet_stdlib::client_api::{ClientError, ErrorKind};
        Some(Err(ClientError::from(ErrorKind::OperationError {
            cause: "test_state".into(),
        })))
    }

    #[tokio::test]
    async fn test_message_processor_creation() {
        let (session_tx, _session_rx) = mpsc::channel(100);
        let processor = MessageProcessor::new(session_tx);

        // Verify the processor can be created
        assert!(std::ptr::addr_of!(processor).is_aligned());
    }

    #[tokio::test]
    async fn test_handle_network_result_success() {
        let (session_tx, mut session_rx) = mpsc::channel(100);
        let processor = MessageProcessor::new(session_tx);
        let tx = create_test_transaction();

        let result = processor
            .handle_network_result(tx, create_success_host_result())
            .await;

        assert!(result.is_ok());

        // Verify message was sent to SessionActor
        let received_msg = session_rx.recv().await.expect("Should receive message");
        match received_msg {
            SessionMessage::DeliverHostResponse {
                tx: received_tx,
                response: _,
            } => {
                assert_eq!(received_tx, tx);
            }
            _ => panic!("Expected DeliverHostResponse message"),
        }
    }

    #[tokio::test]
    async fn test_handle_network_result_none() {
        let (session_tx, mut session_rx) = mpsc::channel(100);
        let processor = MessageProcessor::new(session_tx);
        let tx = create_test_transaction();

        let result = processor
            .handle_network_result(tx, create_none_host_result())
            .await;

        assert!(result.is_ok());

        // Verify no message was sent for None result
        tokio::select! {
            _ = session_rx.recv() => {
                panic!("Should not receive message for None result");
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)) => {
                // Expected - no message sent
            }
        }
    }

    #[tokio::test]
    async fn test_handle_network_result_error() {
        let (session_tx, mut session_rx) = mpsc::channel(100);
        let processor = MessageProcessor::new(session_tx);
        let tx = create_test_transaction();

        let result = processor
            .handle_network_result(tx, create_error_host_result())
            .await;

        assert!(result.is_ok());

        // Verify error was converted to ClientError and sent
        let received_msg = session_rx.recv().await.expect("Should receive message");
        match received_msg {
            SessionMessage::DeliverHostResponse {
                tx: received_tx,
                response,
            } => {
                assert_eq!(received_tx, tx);
                // Response should be an error
                assert!(response.is_err());
            }
            _ => panic!("Expected DeliverHostResponse message"),
        }
    }

    #[tokio::test]
    async fn test_handle_network_result_channel_closed() {
        let (session_tx, session_rx) = mpsc::channel(100);
        let processor = MessageProcessor::new(session_tx);
        let tx = create_test_transaction();

        // Close the receiver to simulate SessionActor being down
        drop(session_rx);

        let result = processor
            .handle_network_result(tx, create_success_host_result())
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ProcessingError::ActorCommunication(_) => {
                // Expected error type
            }
        }
    }
}
