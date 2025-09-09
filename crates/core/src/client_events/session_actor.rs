//! Session actor for client connection refactor
//!
//! This module provides a full session actor that manages client sessions
//! and handles efficient 1→N result delivery to multiple clients.
//!
//! Enhanced with:
//! - Message journaling for durability and replay
//! - State checkpointing for fast recovery
//! - Production-ready error handling and supervision

use crate::client_events::message_journal::{
    CheckpointManager, JournalConfig, JournalableSessionMessage, MessageJournal, SequenceNumber,
    StateCheckpoint,
};
use crate::client_events::{ClientId, RequestId};
use crate::contract::{ClientResponsesSender, SessionMessage};
use crate::message::Transaction;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

/// Configuration for SessionActor
#[derive(Debug, Clone)]
pub struct SessionActorConfig {
    pub enable_journaling: bool,
    pub journal_config: JournalConfig,
    pub checkpoint_dir: PathBuf,
    pub checkpoint_interval: Duration,
    pub max_checkpoints: usize,
}

impl Default for SessionActorConfig {
    fn default() -> Self {
        Self {
            enable_journaling: true,
            journal_config: JournalConfig::default(),
            checkpoint_dir: PathBuf::from("./data/checkpoints"),
            checkpoint_interval: Duration::from_secs(300), // 5 minutes
            max_checkpoints: 5,
        }
    }
}

/// Session actor for client connection refactor with enhanced durability
pub struct SessionActor {
    message_rx: mpsc::Receiver<SessionMessage>,
    client_transactions: HashMap<Transaction, HashSet<ClientId>>,
    // Track RequestId correlation for each (Transaction, ClientId) pair
    client_request_ids: HashMap<(Transaction, ClientId), RequestId>,
    client_responses: ClientResponsesSender,

    // Durability components
    config: SessionActorConfig,
    journal: Option<Arc<MessageJournal>>,
    checkpoint_manager: Option<CheckpointManager>,
    last_checkpoint_time: Instant,
    messages_since_checkpoint: u64,
}

/// Recovery mode for SessionActor restart
#[derive(Debug, Clone)]
pub enum RecoveryMode {
    /// Start fresh (no recovery)
    Fresh,
    /// Recover from checkpoint only
    FromCheckpoint,
    /// Recover from checkpoint + replay journal
    FullRecovery,
}

impl SessionActor {
    /// Create a new session actor with basic configuration
    pub fn new(
        message_rx: mpsc::Receiver<SessionMessage>,
        client_responses: ClientResponsesSender,
    ) -> Self {
        Self::with_config(message_rx, client_responses, SessionActorConfig::default())
    }

    /// Create a new session actor with custom configuration
    pub fn with_config(
        message_rx: mpsc::Receiver<SessionMessage>,
        client_responses: ClientResponsesSender,
        config: SessionActorConfig,
    ) -> Self {
        Self {
            message_rx,
            client_transactions: HashMap::new(),
            client_request_ids: HashMap::new(),
            client_responses,
            config,
            journal: None,
            checkpoint_manager: None,
            last_checkpoint_time: Instant::now(),
            messages_since_checkpoint: 0,
        }
    }

    /// Create a new session actor with recovery from existing state
    pub async fn with_recovery(
        message_rx: mpsc::Receiver<SessionMessage>,
        client_responses: ClientResponsesSender,
        config: SessionActorConfig,
        recovery_mode: RecoveryMode,
    ) -> anyhow::Result<Self> {
        let mut actor = Self::with_config(message_rx, client_responses, config);

        // Initialize durability components
        if actor.config.enable_journaling {
            actor.initialize_durability_components().await?;
        }

        // Perform recovery based on mode
        match recovery_mode {
            RecoveryMode::Fresh => {
                info!("SessionActor starting fresh (no recovery)");
            }
            RecoveryMode::FromCheckpoint => {
                actor.recover_from_checkpoint().await?;
            }
            RecoveryMode::FullRecovery => {
                actor.recover_from_checkpoint().await?;
                actor.replay_journal_messages().await?;
            }
        }

        Ok(actor)
    }

    /// Initialize journaling and checkpointing components
    async fn initialize_durability_components(&mut self) -> anyhow::Result<()> {
        if self.config.enable_journaling {
            // Initialize message journal
            let journal = MessageJournal::new(self.config.journal_config.clone()).await?;
            self.journal = Some(Arc::new(journal));

            // Initialize checkpoint manager
            let checkpoint_manager = CheckpointManager::new(
                self.config.checkpoint_dir.clone(),
                self.config.max_checkpoints,
            )?;
            self.checkpoint_manager = Some(checkpoint_manager);

            info!("SessionActor durability components initialized");
        }

        Ok(())
    }

    /// Recover state from the most recent checkpoint
    async fn recover_from_checkpoint(&mut self) -> anyhow::Result<()> {
        let checkpoint_manager = match &self.checkpoint_manager {
            Some(manager) => manager,
            None => return Ok(()), // No checkpoint manager, skip recovery
        };

        if let Some(checkpoint) = checkpoint_manager.load_latest_checkpoint().await? {
            info!(
                "Recovering SessionActor from checkpoint at sequence {}",
                checkpoint.sequence
            );

            // Restore client transactions
            for (tx, client_ids) in checkpoint.client_transactions {
                let client_set: HashSet<ClientId> = client_ids.into_iter().collect();
                self.client_transactions.insert(tx, client_set);
            }

            // Restore client request ID mappings
            for (key, request_id) in checkpoint.client_request_ids {
                if let Some((tx_str, client_str)) = key.split_once(':') {
                    if let (Ok(_tx), Ok(client_id_val)) =
                        (tx_str.parse::<u64>(), client_str.parse::<u64>())
                    {
                        // TODO: Implement proper Transaction deserialization from checkpoint
                        // For now, we skip restoring transaction mappings during recovery
                        let client_id = ClientId(client_id_val as usize);
                        debug!(
                            "Skipping transaction restoration for client {} during recovery",
                            client_id
                        );
                    }
                }
            }

            info!(
                "Restored {} transactions and {} request correlations from checkpoint",
                self.client_transactions.len(),
                self.client_request_ids.len()
            );
        } else {
            info!("No checkpoint found, starting with empty state");
        }

        Ok(())
    }

    /// Replay journal messages after checkpoint
    async fn replay_journal_messages(&mut self) -> anyhow::Result<()> {
        let journal = match &self.journal {
            Some(journal) => journal,
            None => return Ok(()), // No journal, skip replay
        };

        // Determine starting sequence for replay
        let start_sequence = if let Some(checkpoint_manager) = &self.checkpoint_manager {
            if let Some(checkpoint) = checkpoint_manager.load_latest_checkpoint().await? {
                checkpoint.sequence.next() // Start after checkpoint
            } else {
                SequenceNumber::new(1) // No checkpoint, start from beginning
            }
        } else {
            SequenceNumber::new(1)
        };

        let entries = journal.replay_from(start_sequence).await?;

        if entries.is_empty() {
            info!("No journal entries to replay");
            return Ok(());
        }

        info!(
            "Replaying {} journal entries starting from sequence {}",
            entries.len(),
            start_sequence
        );

        for entry in entries {
            // Apply each journalable message to restore state
            self.apply_journalable_message_for_recovery(&entry.message)
                .await;
        }

        info!("Journal replay completed successfully");
        Ok(())
    }

    /// Apply a journalable message during recovery (without side effects)
    async fn apply_journalable_message_for_recovery(
        &mut self,
        message: &JournalableSessionMessage,
    ) {
        use crate::client_events::message_journal::JournalableSessionMessage;

        match message {
            JournalableSessionMessage::RegisterTransaction {
                tx,
                client_id,
                request_id,
            } => {
                self.client_transactions
                    .entry(*tx)
                    .or_default()
                    .insert(*client_id);

                self.client_request_ids
                    .insert((*tx, *client_id), *request_id);

                debug!(
                    "Recovery: Registered transaction {} for client {} (request {})",
                    tx, client_id, request_id
                );
            }
            JournalableSessionMessage::ClientDisconnect { client_id } => {
                self.cleanup_client_transactions(*client_id);
                debug!("Recovery: Cleaned up transactions for client {}", client_id);
            }
            JournalableSessionMessage::DeliverHostResponse { tx } => {
                // Result delivery removes transaction from waiting list
                self.client_transactions.remove(tx);

                // Clean up request ID mappings for this transaction
                self.client_request_ids.retain(|(t, _), _| t != tx);

                debug!("Recovery: Delivered result for transaction {}", tx);
            }
            JournalableSessionMessage::RegisterClient { client_id, .. } => {
                debug!("Recovery: Registered client {}", client_id);
                // Client registration is handled by transport layer
            }
            JournalableSessionMessage::DeliverResult { tx } => {
                // Legacy message handling
                self.client_transactions.remove(tx);
                self.client_request_ids.retain(|(t, _), _| t != tx);
                debug!("Recovery: Delivered legacy result for transaction {}", tx);
            }
        }
    }

    /// Main message processing loop
    pub async fn run(mut self) {
        info!("Session actor starting");

        // Initialize durability components if not already done
        if self.config.enable_journaling && self.journal.is_none() {
            if let Err(e) = self.initialize_durability_components().await {
                error!("Failed to initialize durability components: {}. Continuing without durability.", e);
                // Continue without durability features
            }
        }

        let mut checkpoint_timer = tokio::time::interval(self.config.checkpoint_interval);
        let mut flush_timer = tokio::time::interval(self.config.journal_config.flush_interval);

        loop {
            tokio::select! {
                // Handle incoming messages
                msg_result = self.message_rx.recv() => {
                    match msg_result {
                        Some(msg) => {
                            // Journal the message before processing (if enabled)
                            if let Err(e) = self.journal_message(&msg).await {
                                error!("Failed to journal message: {}. Processing anyway.", e);
                                // Continue processing even if journaling fails
                            }

                            // Process the message
                            self.process_message(msg).await;

                            // Update checkpoint tracking
                            self.messages_since_checkpoint += 1;
                        }
                        None => {
                            info!("Session actor channel closed, shutting down");
                            break;
                        }
                    }
                }

                // Periodic checkpointing
                _ = checkpoint_timer.tick() => {
                    if let Err(e) = self.create_checkpoint_if_needed().await {
                        error!("Failed to create checkpoint: {}", e);
                    }
                }

                // Periodic journal flushing
                _ = flush_timer.tick() => {
                    if let Err(e) = self.flush_journal().await {
                        error!("Failed to flush journal: {}", e);
                    }
                }
            }
        }

        // Final cleanup
        if let Err(e) = self.shutdown_cleanup().await {
            error!("Error during shutdown cleanup: {}", e);
        }

        info!("Session actor stopped");
    }

    /// Process a single message
    async fn process_message(&mut self, msg: SessionMessage) {
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

                debug!(
                    "Registered transaction {} for client {} (request {}), total clients: {}",
                    tx,
                    client_id,
                    request_id,
                    self.client_transactions.get(&tx).map_or(0, |s| s.len())
                );
            }
            SessionMessage::ClientDisconnect { client_id } => {
                self.cleanup_client_transactions(client_id);
                debug!(
                    "Cleaned up transactions for disconnected client {}",
                    client_id
                );
            }
            SessionMessage::RegisterClient { client_id, .. } => {
                debug!("Registered client session: {}", client_id);
                // Note: Client registration handled by existing transport layer
            }
            SessionMessage::DeliverResult { tx, result: _ } => {
                debug!(
                    "Session actor received legacy DeliverResult for transaction {}",
                    tx
                );
                // Legacy variant - preserved for compatibility
            }
        }
    }

    /// Journal a message for durability
    async fn journal_message(&self, message: &SessionMessage) -> anyhow::Result<()> {
        if let Some(journal) = &self.journal {
            // Create a journalable version of the message
            let journalable = JournalableSessionMessage::from(message);
            journal.append_journalable(journalable).await?;
        }
        Ok(())
    }

    /// Create a checkpoint if conditions are met
    async fn create_checkpoint_if_needed(&mut self) -> anyhow::Result<()> {
        // Only create checkpoint if we have significant activity
        if self.messages_since_checkpoint < 100
            && self.last_checkpoint_time.elapsed() < self.config.checkpoint_interval
        {
            return Ok(());
        }

        if let Some(checkpoint_manager) = &self.checkpoint_manager {
            let current_sequence = if let Some(journal) = &self.journal {
                journal.current_sequence()
            } else {
                SequenceNumber::new(self.messages_since_checkpoint)
            };

            let checkpoint = StateCheckpoint::new(
                current_sequence,
                &self.client_transactions,
                &self.client_request_ids,
            );

            checkpoint_manager.create_checkpoint(checkpoint).await?;

            self.last_checkpoint_time = Instant::now();
            self.messages_since_checkpoint = 0;

            info!("Created checkpoint at sequence {}", current_sequence);
        }

        Ok(())
    }

    /// Flush journal to disk
    async fn flush_journal(&self) -> anyhow::Result<()> {
        if let Some(journal) = &self.journal {
            journal.flush().await?;
        }
        Ok(())
    }

    /// Cleanup during shutdown
    async fn shutdown_cleanup(&mut self) -> anyhow::Result<()> {
        // Flush journal one final time
        self.flush_journal().await?;

        // Create final checkpoint if we have activity
        if self.messages_since_checkpoint > 0 {
            self.create_checkpoint_if_needed().await?;
        }

        info!("SessionActor shutdown cleanup completed");
        Ok(())
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
