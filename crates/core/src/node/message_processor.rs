//! Message processor for clean client handling separation
//!
//! This module provides a MessageProcessor enum that cleanly separates
//! network message processing from client notification logic, enabling
//! pure network processing when the actor system is enabled.

use crate::contract::SessionMessage;
use crate::message::Transaction;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error};

/// Errors that can occur during message processing
#[derive(Debug, thiserror::Error)]
pub enum ProcessingError {
    #[error("Actor communication error: {0}")]
    ActorCommunication(#[from] mpsc::error::SendError<SessionMessage>),
}

/// MessageProcessor for pure network-to-actor routing
///
/// This processor only handles actor mode - legacy mode bypasses this entirely
/// and uses the original direct client notification path.
pub struct MessageProcessor {
    result_tx: mpsc::Sender<SessionMessage>,
}

impl MessageProcessor {
    /// Create a new MessageProcessor for actor mode
    /// Legacy mode doesn't use MessageProcessor at all
    pub fn new(result_tx: mpsc::Sender<SessionMessage>) -> Self {
        Self { result_tx }
    }

    /// Handle network result with pure separation - no client types in network layer
    /// This method is called from the pure network layer and routes results to SessionActor
    pub async fn handle_network_result(
        &self,
        tx: Transaction,
        op_result: Result<Option<crate::operations::OpEnum>, crate::node::OpError>,
    ) -> Result<(), ProcessingError> {
        // Pure result forwarding to SessionActor
        self.route_to_session_actor(tx, op_result).await
    }

    /// Route network result to SessionActor - no client parameters needed
    async fn route_to_session_actor(
        &self,
        tx: Transaction,
        op_result: Result<Option<crate::operations::OpEnum>, crate::node::OpError>,
    ) -> Result<(), ProcessingError> {
        // Convert operation result to host result
        let host_result = match op_result {
            Ok(Some(op_res)) => {
                debug!(
                    "Actor mode: converting network result for transaction {}",
                    tx
                );
                Arc::new(op_res.to_host_result())
            }
            Ok(None) => {
                debug!("Actor mode: no result to forward for transaction {}", tx);
                return Ok(()); // No result to forward
            }
            Err(e) => {
                error!(
                    "Actor mode: network operation error for transaction {}: {}",
                    tx, e
                );
                // Create a generic client error for operation failures
                use freenet_stdlib::client_api::{ClientError, ErrorKind};
                Arc::new(Err(ClientError::from(ErrorKind::OperationError {
                    cause: e.to_string().into(),
                })))
            }
        };

        // Create session message for pure actor routing
        // The SessionActor will handle all client correlation internally
        let session_msg = SessionMessage::DeliverHostResponse {
            tx,
            response: host_result,
        };

        // Send to SessionActor - it handles all client concerns
        if let Err(e) = self.result_tx.send(session_msg).await {
            error!(
                "Failed to send result to SessionActor for transaction {}: {}",
                tx, e
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

// TODO: Add comprehensive tests for MessageProcessor functionality
// Tests temporarily removed to focus on core functionality compilation
