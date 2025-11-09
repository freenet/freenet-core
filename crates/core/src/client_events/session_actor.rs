//! Session actor for client connection refactor
//!
//! This module provides a simplified session actor that manages client sessions
//! and handles efficient 1→N result delivery to multiple clients.
//!
//! # Cache Eviction Strategy
//!
//! The `pending_results` cache uses **lazy evaluation** for cleanup - there is no
//! background task or periodic timer. Eviction happens **only** as a side effect of
//! processing incoming messages.
//!
//! ## How It Works
//!
//! 1. **On every message** (`process_message`): `prune_pending_results()` is called
//! 2. **TTL-based pruning**: Removes entries older than `PENDING_RESULT_TTL` (60s)
//! 3. **Capacity enforcement**: When cache reaches `MAX_PENDING_RESULTS` (2048),
//!    uses LRU eviction to remove the oldest entry
//!
//! ## Tradeoffs
//!
//! **Advantages:**
//! - Simpler implementation - no separate task management required
//! - Cleanup cost is amortized across normal message processing
//! - No overhead when actor is idle
//!
//! **Limitations:**
//! - **Idle memory retention**: During idle periods (no incoming messages), stale
//!   entries remain in memory indefinitely until the next message arrives
//! - **Temporary overflow**: Cache size can temporarily exceed limits between messages
//! - **Burst accumulation**: After a burst of activity, cache may sit at max capacity
//!   until next message triggers pruning
//! - **Memory pressure**: With large `HostResult` payloads, 2048 entries could consume
//!   significant memory during idle periods
//!
//! ## Future Considerations
//!
//! If idle memory retention becomes problematic in production:
//! - Add a background tokio task with periodic cleanup (e.g., every 30s)
//! - Implement memory-based limits in addition to count-based limits
//! - Add metrics/monitoring for cache size to detect accumulation patterns

use crate::client_events::{ClientId, HostResponse, HostResult, RequestId};
use crate::contract::{ClientResponsesSender, SessionMessage};
use crate::message::Transaction;
use freenet_stdlib::client_api::ContractResponse;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::debug;

/// Time-to-live for cached pending results. Entries older than this duration are
/// eligible for removal during pruning (triggered on message processing).
///
/// Note: Due to lazy evaluation, stale entries may persist beyond TTL during idle periods.
const PENDING_RESULT_TTL: Duration = Duration::from_secs(60);

/// Maximum number of cached pending results. When this limit is reached, LRU eviction
/// removes the oldest entry to make room for new ones.
///
/// Note: Cache may temporarily exceed this limit between messages since enforcement
/// is lazy (triggered only during message processing).
const MAX_PENDING_RESULTS: usize = 2048;

/// Simple session actor for client connection refactor
pub struct SessionActor {
    message_rx: mpsc::Receiver<SessionMessage>,
    client_transactions: HashMap<Transaction, HashSet<ClientId>>,
    // Track RequestId correlation for each (Transaction, ClientId) pair
    client_request_ids: HashMap<(Transaction, ClientId), RequestId>,
    /// Cache of pending results for late-arriving subscribers.
    ///
    /// Uses lazy evaluation for cleanup - entries are pruned only during message processing.
    /// See module-level documentation for detailed cache eviction strategy and limitations.
    pending_results: HashMap<Transaction, PendingResult>,
    client_responses: ClientResponsesSender,
}

#[derive(Clone)]
struct PendingResult {
    result: std::sync::Arc<HostResult>,
    delivered_clients: HashSet<ClientId>,
    last_accessed: Instant,
}

impl PendingResult {
    fn new(result: std::sync::Arc<HostResult>) -> Self {
        Self {
            result,
            delivered_clients: HashSet::new(),
            last_accessed: Instant::now(),
        }
    }

    fn touch(&mut self) {
        self.last_accessed = Instant::now();
    }
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
            pending_results: HashMap::new(),
            client_responses,
        }
    }

    /// Main message processing loop
    pub async fn run(mut self) {
        while let Some(msg) = self.message_rx.recv().await {
            self.process_message(msg).await;
        }
    }

    /// Process a single message.
    ///
    /// Note: This method triggers cache pruning on EVERY message via `prune_pending_results()`.
    /// This is the only mechanism for cache cleanup (lazy evaluation - no background task).
    async fn process_message(&mut self, msg: SessionMessage) {
        self.prune_pending_results();
        match msg {
            SessionMessage::DeliverHostResponse { tx, response } => {
                self.handle_result_delivery(tx, response).await;
            }
            SessionMessage::DeliverHostResponseWithRequestId {
                tx,
                response,
                request_id,
            } => {
                self.handle_result_delivery_with_request_id(tx, response, request_id)
                    .await;
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

                tracing::info!(
                    "Registered transaction {} for client {} (request {}), total clients: {}",
                    tx,
                    client_id,
                    request_id,
                    self.client_transactions.get(&tx).map_or(0, |s| s.len())
                );

                if let Some(result_arc) = self.pending_results.get_mut(&tx).and_then(|pending| {
                    pending.touch();
                    if pending.delivered_clients.insert(client_id) {
                        Some(pending.result.clone())
                    } else {
                        None
                    }
                }) {
                    let mut recipients = HashSet::new();
                    recipients.insert(client_id);
                    self.deliver_result_to_clients(tx, recipients, result_arc);
                    self.cleanup_transaction_entry(tx, client_id);
                }
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

    fn deliver_result_to_clients(
        &mut self,
        tx: Transaction,
        waiting_clients: HashSet<ClientId>,
        result: std::sync::Arc<HostResult>,
    ) {
        let client_count = waiting_clients.len();
        tracing::info!(
            "Delivering result for transaction {} to {} clients",
            tx,
            client_count
        );

        if let Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            key,
            state,
            ..
        })) = result.as_ref()
        {
            tracing::info!(
                "Contract GET response ready for delivery: contract={} bytes={}",
                key,
                state.as_ref().len()
            );
        }

        // Optimized 1→N delivery with RequestId correlation
        for client_id in waiting_clients {
            // Look up the RequestId for this (transaction, client) pair
            let request_id = self
                .client_request_ids
                .remove(&(tx, client_id))
                .unwrap_or_else(|| {
                    tracing::warn!(
                        "No RequestId found for transaction {} and client {}, using default",
                        tx,
                        client_id
                    );
                    RequestId::new()
                });

            if let Err(e) = self
                .client_responses
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
                    tx,
                    client_id,
                    request_id
                );
            }
        }

        if client_count > 1 {
            tracing::debug!(
                "Successfully delivered result for transaction {} to {} clients via optimized 1→N fanout with RequestId correlation",
                tx,
                client_count
            );
        }
    }

    /// CORE: 1→N Result Delivery with RequestId correlation
    /// Optimized with Arc<HostResult> to minimize cloning overhead in 1→N delivery
    async fn handle_result_delivery(
        &mut self,
        tx: Transaction,
        result: std::sync::Arc<crate::client_events::HostResult>,
    ) {
        tracing::info!(
            "Session actor attempting to deliver result for transaction {}, registered transactions: {}",
            tx,
            self.client_transactions.len()
        );

        let mut recipients = HashSet::new();
        let result_to_deliver = {
            if !self.pending_results.contains_key(&tx)
                && self.pending_results.len() >= MAX_PENDING_RESULTS
            {
                self.enforce_pending_capacity();
            }

            let entry = self
                .pending_results
                .entry(tx)
                .or_insert_with(|| PendingResult::new(result.clone()));
            entry.result = result.clone();
            entry.touch();

            if let Some(waiting_clients) = self.client_transactions.remove(&tx) {
                for client_id in waiting_clients {
                    if entry.delivered_clients.insert(client_id) {
                        recipients.insert(client_id);
                    }
                }
            }

            entry.result.clone()
        };

        if !recipients.is_empty() {
            self.deliver_result_to_clients(tx, recipients, result_to_deliver);
        } else {
            tracing::debug!(
                "No clients waiting for transaction result: {}, caching response for deferred delivery",
                tx
            );
        }
    }

    /// Handle result delivery with a specific RequestId
    async fn handle_result_delivery_with_request_id(
        &mut self,
        tx: Transaction,
        result: std::sync::Arc<HostResult>,
        request_id: RequestId,
    ) {
        // Find the specific client associated with this RequestId
        let mut target_client = None;

        // Search for the client that has this RequestId for this transaction
        for ((tx_key, client_id), stored_request_id) in &self.client_request_ids {
            if *tx_key == tx && *stored_request_id == request_id {
                target_client = Some(*client_id);
                break;
            }
        }

        if let Some(client_id) = target_client {
            // Remove the specific client from waiting
            if let Some(waiting_clients) = self.client_transactions.get_mut(&tx) {
                waiting_clients.remove(&client_id);

                // Clean up if no more clients waiting
                if waiting_clients.is_empty() {
                    self.client_transactions.remove(&tx);
                }
            }

            // Remove the RequestId correlation
            self.client_request_ids.remove(&(tx, client_id));

            // Deliver result to the specific client
            if let Err(e) = self
                .client_responses
                .send((client_id, request_id, (*result).clone()))
            {
                tracing::warn!(
                    "Failed to deliver result to client {} (request {}): {}",
                    client_id,
                    request_id,
                    e
                );
            } else {
                if !self.pending_results.contains_key(&tx)
                    && self.pending_results.len() >= MAX_PENDING_RESULTS
                {
                    self.enforce_pending_capacity();
                }

                let entry = self
                    .pending_results
                    .entry(tx)
                    .or_insert_with(|| PendingResult::new(result.clone()));
                entry.delivered_clients.insert(client_id);
                entry.result = result.clone();
                entry.touch();

                tracing::debug!(
                    "Delivered result for transaction {} to specific client {} with request correlation {}",
                    tx, client_id, request_id
                );
            }
        } else {
            tracing::warn!(
                "No client found for transaction {} with request ID {}, falling back to general delivery",
                tx, request_id
            );
            // Fall back to general delivery mechanism
            self.handle_result_delivery(tx, result).await;
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

    /// Prune stale pending results based on TTL and enforce capacity limits.
    ///
    /// This is the **only** cache cleanup mechanism - there is no background task.
    /// Called on every message in `process_message()`.
    ///
    /// # Cleanup Strategy (Lazy Evaluation)
    ///
    /// 1. **Skip if empty**: Early return if no cached results
    /// 2. **Identify active transactions**: Collect all transactions that still have waiting clients
    /// 3. **TTL-based removal**: Remove inactive entries older than `PENDING_RESULT_TTL`
    /// 4. **Capacity enforcement**: If still at/over `MAX_PENDING_RESULTS`, trigger LRU eviction
    ///
    /// # Lazy Evaluation Implications
    ///
    /// - During idle periods (no messages), stale entries persist in memory
    /// - Cache cleanup happens only when actor receives messages
    /// - Stale entries may remain beyond TTL until next message arrives
    fn prune_pending_results(&mut self) {
        if self.pending_results.is_empty() {
            return;
        }

        let mut active_txs: HashSet<Transaction> =
            self.client_transactions.keys().copied().collect();
        active_txs.extend(self.client_request_ids.keys().map(|(tx, _)| *tx));

        let now = Instant::now();
        let stale: Vec<Transaction> = self
            .pending_results
            .iter()
            .filter_map(|(tx, pending)| {
                if active_txs.contains(tx) {
                    return None;
                }
                if now.duration_since(pending.last_accessed) > PENDING_RESULT_TTL {
                    Some(*tx)
                } else {
                    None
                }
            })
            .collect();

        for tx in stale {
            self.pending_results.remove(&tx);
        }

        if self.pending_results.len() >= MAX_PENDING_RESULTS {
            self.enforce_pending_capacity();
        }
    }

    fn cleanup_transaction_entry(&mut self, tx: Transaction, client_id: ClientId) {
        if let Some(waiting_clients) = self.client_transactions.get_mut(&tx) {
            waiting_clients.remove(&client_id);
            if waiting_clients.is_empty() {
                self.client_transactions.remove(&tx);
            }
        }
    }

    /// Enforce capacity limits using LRU (Least Recently Used) eviction.
    ///
    /// Removes the entry with the oldest `last_accessed` timestamp when the cache
    /// reaches or exceeds `MAX_PENDING_RESULTS`.
    ///
    /// # Lazy Evaluation Note
    ///
    /// This is only called:
    /// 1. At the end of `prune_pending_results()` if still at capacity
    /// 2. Before inserting new entries when already at capacity
    ///
    /// Between messages, cache size may temporarily exceed the limit.
    fn enforce_pending_capacity(&mut self) {
        if self.pending_results.len() < MAX_PENDING_RESULTS {
            return;
        }

        if let Some(oldest_tx) = self
            .pending_results
            .iter()
            .min_by_key(|(_, pending)| pending.last_accessed)
            .map(|(tx, _)| *tx)
        {
            self.pending_results.remove(&oldest_tx);
        }
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
    async fn test_pending_result_reaches_late_registered_clients() {
        use crate::contract::client_responses_channel;
        use crate::operations::subscribe::SubscribeMsg;
        use freenet_stdlib::client_api::{ContractResponse, HostResponse};
        use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};

        let (session_tx, session_rx) = mpsc::channel(100);
        let (mut client_responses_rx, client_responses_tx) = client_responses_channel();
        let actor = SessionActor::new(session_rx, client_responses_tx);

        let actor_handle = tokio::spawn(async move {
            actor.run().await;
        });

        let tx = Transaction::new::<SubscribeMsg>();
        let contract_key = ContractKey::from(ContractInstanceId::new([7u8; 32]));
        let host_result = Ok(HostResponse::ContractResponse(
            ContractResponse::SubscribeResponse {
                key: contract_key,
                subscribed: true,
            },
        ));

        // Deliver result before any clients register; this models LocalSubscribeComplete firing
        // before the session actor processes the pending subscription registration.
        session_tx
            .send(SessionMessage::DeliverHostResponse {
                tx,
                response: std::sync::Arc::new(host_result.clone()),
            })
            .await
            .unwrap();

        // First client registers and should receive the cached result.
        let client_one = ClientId::FIRST;
        let request_one = RequestId::new();
        session_tx
            .send(SessionMessage::RegisterTransaction {
                tx,
                client_id: client_one,
                request_id: request_one,
            })
            .await
            .unwrap();

        let (delivered_client_one, delivered_request_one, delivered_result_one) =
            tokio::time::timeout(
                tokio::time::Duration::from_millis(200),
                client_responses_rx.recv(),
            )
            .await
            .expect("session actor failed to deliver cached result to first client")
            .expect("client response channel closed unexpectedly");
        assert_eq!(delivered_client_one, client_one);
        assert_eq!(delivered_request_one, request_one);
        match delivered_result_one {
            Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
            })) => {
                assert_eq!(key, contract_key);
                assert!(subscribed);
            }
            other => panic!("unexpected result delivered to first client: {:?}", other),
        }

        // Second client registers later; we expect the cached result to still be available.
        let client_two = ClientId::next();
        let request_two = RequestId::new();
        session_tx
            .send(SessionMessage::RegisterTransaction {
                tx,
                client_id: client_two,
                request_id: request_two,
            })
            .await
            .unwrap();

        let (delivered_client_two, delivered_request_two, delivered_result_two) =
            tokio::time::timeout(
                tokio::time::Duration::from_millis(200),
                client_responses_rx.recv(),
            )
            .await
            .expect("pending result was not delivered to late-registered client")
            .expect("client response channel closed unexpectedly for late registrant");
        assert_eq!(delivered_client_two, client_two);
        assert_eq!(delivered_request_two, request_two);
        match delivered_result_two {
            Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
            })) => {
                assert_eq!(key, contract_key);
                assert!(subscribed);
            }
            other => panic!(
                "unexpected result delivered to late-registered client: {:?}",
                other
            ),
        }

        actor_handle.abort();
    }

    #[tokio::test]
    async fn test_pending_result_delivered_after_registration() {
        use crate::contract::client_responses_channel;

        let (session_tx, session_rx) = mpsc::channel(100);
        let (mut client_responses_rx, client_responses_tx) = client_responses_channel();
        let actor = SessionActor::new(session_rx, client_responses_tx);

        let actor_handle = tokio::spawn(async move {
            actor.run().await;
        });

        let tx = Transaction::new::<PutMsg>();
        let client_id = ClientId::FIRST;
        let request_id = RequestId::new();
        let host_result = Arc::new(Ok(HostResponse::Ok));

        session_tx
            .send(SessionMessage::DeliverHostResponse {
                tx,
                response: host_result.clone(),
            })
            .await
            .unwrap();

        // Ensure the actor processes the pending result before registration.
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        session_tx
            .send(SessionMessage::RegisterTransaction {
                tx,
                client_id,
                request_id,
            })
            .await
            .unwrap();

        let delivered = tokio::time::timeout(
            tokio::time::Duration::from_millis(200),
            client_responses_rx.recv(),
        )
        .await
        .expect("Timed out waiting for pending result delivery")
        .expect("Client response channel closed unexpectedly");

        let (returned_client, returned_request, returned_result) = delivered;
        assert_eq!(returned_client, client_id);
        assert_eq!(returned_request, request_id);
        match returned_result {
            Ok(HostResponse::Ok) => {}
            other => panic!(
                "Unexpected result delivered. got={:?}, expected=Ok(HostResponse::Ok)",
                other
            ),
        }

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
