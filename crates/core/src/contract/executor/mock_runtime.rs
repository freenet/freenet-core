use super::*;
use crate::message::NodeEvent;
use crate::node::OpManager;
use crate::wasm_runtime::{InMemoryContractStore, MockStateStorage, StateStorage};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

/// Mock runtime for testing that uses fully in-memory storage.
///
/// Unlike the production runtime which uses disk-based storage with background
/// threads (file watchers, compaction), this runtime keeps everything in memory
/// for deterministic simulation testing.
pub(crate) struct MockRuntime {
    pub contract_store: InMemoryContractStore,
}

/// Executor with MockRuntime using disk-based state storage (for backward compatibility)
impl Executor<MockRuntime, Storage> {
    /// Create a mock executor with disk-based state storage.
    ///
    /// Contract code is stored in memory (InMemoryContractStore) for determinism,
    /// but state is stored on disk (SQLite). For fully in-memory storage, use
    /// `new_mock_in_memory`.
    pub async fn new_mock(
        identifier: &str,
        op_sender: Option<OpRequestSender>,
        op_manager: Option<Arc<OpManager>>,
    ) -> anyhow::Result<Self> {
        let data_dir = Self::test_data_dir(identifier);

        // Use in-memory contract store for deterministic behavior
        let contract_store = InMemoryContractStore::new();

        tracing::debug!("creating state store at path: {data_dir:?}");
        std::fs::create_dir_all(&data_dir).expect("directory created");
        let state_store = StateStore::new(Storage::new(&data_dir).await?, u16::MAX as u32).unwrap();
        tracing::debug!("state store created");

        let executor = Executor::new(
            state_store,
            || Ok(()),
            OperationMode::Local,
            MockRuntime { contract_store },
            op_sender,
            op_manager,
        )
        .await?;
        Ok(executor)
    }
}

/// Executor with MockRuntime using fully in-memory storage (for deterministic simulation)
impl Executor<MockRuntime, MockStateStorage> {
    /// Create a mock executor with fully in-memory storage.
    ///
    /// This is designed for deterministic simulation testing where:
    /// - Both contract code and state are stored in memory
    /// - No disk I/O or background threads (file watchers, compaction)
    /// - State persists across node crash/restart (same MockStateStorage instance)
    /// - State can be inspected/verified through MockStateStorage methods
    ///
    /// # Arguments
    /// * `_identifier` - Unused (kept for API compatibility)
    /// * `shared_storage` - A MockStateStorage instance (clone it to share across restarts)
    /// * `op_sender` - Optional channel for network operations
    /// * `op_manager` - Optional reference to the operation manager
    pub async fn new_mock_in_memory(
        _identifier: &str,
        shared_storage: MockStateStorage,
        op_sender: Option<OpRequestSender>,
        op_manager: Option<Arc<OpManager>>,
    ) -> anyhow::Result<Self> {
        // Use fully in-memory storage with no caching for deterministic simulation:
        // - InMemoryContractStore: no disk I/O, no background threads
        // - StateStore::new_uncached: bypasses stretto cache (non-deterministic TinyLFU)
        let contract_store = InMemoryContractStore::new();
        let state_store = StateStore::new_uncached(shared_storage);
        tracing::debug!("created fully in-memory uncached executor for deterministic simulation");

        let executor = Executor::new(
            state_store,
            || Ok(()),
            OperationMode::Local,
            MockRuntime { contract_store },
            op_sender,
            op_manager,
        )
        .await?;
        Ok(executor)
    }
}

/// Common methods for MockRuntime executors (works with any storage type)
impl<S> Executor<MockRuntime, S>
where
    S: StateStorage + Send + Sync + 'static,
    <S as StateStorage>::Error: Into<anyhow::Error>,
{
    pub async fn handle_request(
        &mut self,
        _id: ClientId,
        _req: ClientRequest<'_>,
        _updates: Option<mpsc::UnboundedSender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        unreachable!("MockRuntime does not handle client requests directly")
    }

    /// Emit BroadcastStateChange to notify network peers of state change.
    /// Called when state is updated or when our state wins a CRDT merge.
    async fn broadcast_state_change(&self, key: ContractKey, state: &WrappedState) {
        if let Some(op_manager) = &self.op_manager {
            tracing::debug!(
                contract = %key,
                state_size = state.size(),
                "MockRuntime: Emitting BroadcastStateChange"
            );
            if let Err(err) = op_manager
                .notify_node_event(NodeEvent::BroadcastStateChange {
                    key,
                    new_state: state.clone(),
                })
                .await
            {
                tracing::warn!(
                    contract = %key,
                    error = %err,
                    "MockRuntime: Failed to broadcast state change"
                );
            }
        }
    }
}

/// ContractExecutor implementation for MockRuntime with any storage type
impl<S> ContractExecutor for Executor<MockRuntime, S>
where
    S: StateStorage + Send + Sync + 'static,
    <S as StateStorage>::Error: Into<anyhow::Error>,
{
    fn lookup_key(&self, instance_id: &ContractInstanceId) -> Option<ContractKey> {
        let code_hash = self.runtime.contract_store.code_hash_from_id(instance_id)?;
        Some(ContractKey::from_id_and_code(*instance_id, code_hash))
    }

    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        return_contract_code: bool,
    ) -> Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError> {
        let Some(parameters) = self
            .state_store
            .get_params(&key)
            .await
            .map_err(ExecutorError::other)?
        else {
            return Err(ExecutorError::other(anyhow::anyhow!(
                "missing state and/or parameters for contract {key}"
            )));
        };
        let contract = if return_contract_code {
            self.runtime
                .contract_store
                .fetch_contract(&key, &parameters)
        } else {
            None
        };
        let Ok(state) = self.state_store.get(&key).await else {
            return Err(ExecutorError::other(anyhow::anyhow!(
                "missing state for contract {key}"
            )));
        };
        Ok((Some(state), contract))
    }

    async fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        state: Either<WrappedState, StateDelta<'static>>,
        _related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<UpsertResult, ExecutorError> {
        // Mock runtime implements a simple CRDT-like merge strategy to ensure
        // deterministic convergence in simulation tests:
        // - Compare incoming state with current state using blake3 hash
        // - The state with the lexicographically larger hash wins
        // - This ensures all peers converge to the same state regardless of
        //   message arrival order (important for delayed broadcasts)
        //
        // CRITICAL: When state changes (Updated) or when our state wins the merge
        // (CurrentWon), we emit BroadcastStateChange to propagate to network peers.
        // This ensures peers with "losing" states get updated.
        let result = match (state, code) {
            (Either::Left(incoming_state), Some(contract)) => {
                // New contract with code - check if we should accept this state
                // First store the contract itself
                self.runtime
                    .contract_store
                    .store_contract(contract.clone())
                    .map_err(ExecutorError::other)?;

                // Check if there's already a state for this contract
                match self.state_store.get(&key).await {
                    Ok(current_state) => {
                        // Compare hashes - larger hash wins (deterministic CRDT merge)
                        let incoming_hash = blake3::hash(incoming_state.as_ref());
                        let current_hash = blake3::hash(current_state.as_ref());

                        if incoming_hash.as_bytes() > current_hash.as_bytes() {
                            // Incoming state wins - update
                            self.state_store
                                .update(&key, incoming_state.clone())
                                .await
                                .map_err(ExecutorError::other)?;
                            Ok(UpsertResult::Updated(incoming_state))
                        } else if incoming_hash.as_bytes() == current_hash.as_bytes() {
                            // Same state - no change
                            Ok(UpsertResult::NoChange)
                        } else {
                            // Current state wins - return it so it can be propagated
                            Ok(UpsertResult::CurrentWon(current_state))
                        }
                    }
                    Err(_) => {
                        // No existing state - store the incoming state
                        self.state_store
                            .store(key, incoming_state.clone(), contract.params().into_owned())
                            .await
                            .map_err(ExecutorError::other)?;
                        Ok(UpsertResult::Updated(incoming_state))
                    }
                }
            }
            (Either::Left(incoming_state), None) => {
                // Update case - must have existing state
                match self.state_store.get(&key).await {
                    Ok(current_state) => {
                        // Compare hashes - larger hash wins (deterministic CRDT merge)
                        let incoming_hash = blake3::hash(incoming_state.as_ref());
                        let current_hash = blake3::hash(current_state.as_ref());

                        if incoming_hash.as_bytes() > current_hash.as_bytes() {
                            // Incoming state wins - update
                            self.state_store
                                .update(&key, incoming_state.clone())
                                .await
                                .map_err(ExecutorError::other)?;
                            Ok(UpsertResult::Updated(incoming_state))
                        } else if incoming_hash.as_bytes() == current_hash.as_bytes() {
                            // Same state - no change
                            Ok(UpsertResult::NoChange)
                        } else {
                            // Current state wins - return it so it can be propagated
                            Ok(UpsertResult::CurrentWon(current_state))
                        }
                    }
                    Err(_) => {
                        // No existing state for update - this shouldn't happen but handle gracefully
                        tracing::warn!(
                            contract = %key,
                            "Update received for non-existent contract state"
                        );
                        Err(ExecutorError::request(StdContractError::MissingContract {
                            key: key.into(),
                        }))
                    }
                }
            }
            (Either::Right(delta), Some(contract)) => {
                // Delta update with contract code - store contract first, then apply delta
                self.runtime
                    .contract_store
                    .store_contract(contract.clone())
                    .map_err(ExecutorError::other)?;

                match self.state_store.get(&key).await {
                    Ok(current_state) => {
                        // Apply delta by concatenating with current state (simple mock behavior)
                        // Then use hash comparison for deterministic convergence
                        let mut new_state_bytes = current_state.as_ref().to_vec();
                        new_state_bytes.extend_from_slice(delta.as_ref());
                        let new_state = WrappedState::new(new_state_bytes);

                        let new_hash = blake3::hash(new_state.as_ref());
                        let current_hash = blake3::hash(current_state.as_ref());

                        if new_hash.as_bytes() > current_hash.as_bytes() {
                            self.state_store
                                .update(&key, new_state.clone())
                                .await
                                .map_err(ExecutorError::other)?;
                            Ok(UpsertResult::Updated(new_state))
                        } else {
                            Ok(UpsertResult::NoChange)
                        }
                    }
                    Err(_) => {
                        // No existing state - treat delta as initial state
                        let initial_state = WrappedState::new(delta.as_ref().to_vec());
                        self.state_store
                            .store(key, initial_state.clone(), contract.params().into_owned())
                            .await
                            .map_err(ExecutorError::other)?;
                        Ok(UpsertResult::Updated(initial_state))
                    }
                }
            }
            (Either::Right(delta), None) => {
                // Delta update without contract code - must have existing state
                match self.state_store.get(&key).await {
                    Ok(current_state) => {
                        // Apply delta by concatenating with current state (simple mock behavior)
                        let mut new_state_bytes = current_state.as_ref().to_vec();
                        new_state_bytes.extend_from_slice(delta.as_ref());
                        let new_state = WrappedState::new(new_state_bytes);

                        let new_hash = blake3::hash(new_state.as_ref());
                        let current_hash = blake3::hash(current_state.as_ref());

                        if new_hash.as_bytes() > current_hash.as_bytes() {
                            self.state_store
                                .update(&key, new_state.clone())
                                .await
                                .map_err(ExecutorError::other)?;
                            Ok(UpsertResult::Updated(new_state))
                        } else {
                            Ok(UpsertResult::NoChange)
                        }
                    }
                    Err(_) => {
                        tracing::warn!(
                            contract = %key,
                            "Delta received for non-existent contract state"
                        );
                        Err(ExecutorError::request(StdContractError::MissingContract {
                            key: key.into(),
                        }))
                    }
                }
            }
        };

        // Emit BroadcastStateChange for Updated and CurrentWon cases.
        // - Updated: Our state changed, notify peers so they can update
        // - CurrentWon: Our state won the merge, notify peers so they update to our state
        if let Ok(ref upsert_result) = result {
            match upsert_result {
                UpsertResult::Updated(state) | UpsertResult::CurrentWon(state) => {
                    self.broadcast_state_change(key, state).await;
                }
                UpsertResult::NoChange => {}
            }
        }

        result
    }

    fn register_contract_notifier(
        &mut self,
        _key: ContractInstanceId,
        _cli_id: ClientId,
        _notification_ch: UnboundedSender<HostResult>,
        _summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>> {
        Ok(())
    }

    fn execute_delegate_request(
        &mut self,
        _req: DelegateRequest<'_>,
        _attested_contract: Option<&ContractInstanceId>,
    ) -> Response {
        Err(ExecutorError::other(anyhow::anyhow!(
            "not supported in mock runtime"
        )))
    }

    fn get_subscription_info(&self) -> Vec<crate::message::SubscriptionInfo> {
        vec![] // Mock implementation returns empty list
    }

    async fn summarize_contract_state(
        &mut self,
        key: ContractKey,
    ) -> Result<StateSummary<'static>, ExecutorError> {
        // MockRuntime doesn't have actual contract code to execute summarize_state,
        // so we return the full state as a fallback summary
        let state = self
            .state_store
            .get(&key)
            .await
            .map_err(ExecutorError::other)?;
        Ok(StateSummary::from(state.as_ref().to_vec()))
    }

    async fn get_contract_state_delta(
        &mut self,
        key: ContractKey,
        _their_summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ExecutorError> {
        // MockRuntime doesn't have actual contract code to execute get_state_delta,
        // so we return the full state as the "delta" (fallback behavior)
        let state = self
            .state_store
            .get(&key)
            .await
            .map_err(ExecutorError::other)?;
        Ok(StateDelta::from(state.as_ref().to_vec()))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn local_node_handle() -> Result<(), Box<dyn std::error::Error>> {
        const MAX_MEM_CACHE: u32 = 10_000_000;
        let tmp_dir = tempfile::tempdir()?;
        let state_store_path = tmp_dir.path().join("state_store");
        std::fs::create_dir_all(&state_store_path)?;
        // Use in-memory contract store for deterministic behavior
        let contract_store = InMemoryContractStore::new();
        let state_store =
            StateStore::new(Storage::new(&state_store_path).await?, MAX_MEM_CACHE).unwrap();
        let mut counter = 0;
        Executor::new(
            state_store,
            || {
                counter += 1;
                Ok(())
            },
            OperationMode::Local,
            MockRuntime { contract_store },
            None,
            None,
        )
        .await
        .expect("local node with handle");

        assert_eq!(counter, 1);
        Ok(())
    }

    /// Helper to create a mock executor with in-memory storage for testing
    async fn create_test_executor() -> Executor<MockRuntime, MockStateStorage> {
        let shared_storage = MockStateStorage::new();
        Executor::new_mock_in_memory("test", shared_storage, None, None)
            .await
            .expect("create test executor")
    }

    /// Helper to create a test contract with given code bytes
    fn create_test_contract(code_bytes: &[u8]) -> ContractContainer {
        use freenet_stdlib::prelude::*;

        let code = ContractCode::from(code_bytes.to_vec());
        let params = Parameters::from(vec![]);
        ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(code),
            params,
        )))
    }

    /// Test that CRDT merge accepts state with larger hash
    #[tokio::test(flavor = "current_thread")]
    async fn crdt_merge_larger_hash_wins() {
        let mut executor = create_test_executor().await;

        // Create two states where we know their hash ordering
        let state_a = WrappedState::new(vec![1, 2, 3]);
        let state_b = WrappedState::new(vec![4, 5, 6]);

        let hash_a = blake3::hash(state_a.as_ref());
        let hash_b = blake3::hash(state_b.as_ref());

        // Determine which has larger hash
        let (smaller_state, larger_state) = if hash_a.as_bytes() < hash_b.as_bytes() {
            (state_a, state_b)
        } else {
            (state_b, state_a)
        };

        // Create contract
        let contract = create_test_contract(b"test_contract_code_1");
        let key = contract.key();

        // First, store the smaller-hash state
        let result = executor
            .upsert_contract_state(
                key,
                Either::Left(smaller_state.clone()),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .expect("initial store should succeed");
        assert!(matches!(result, UpsertResult::Updated(_)));

        // Now try to update with larger-hash state - should succeed
        let result = executor
            .upsert_contract_state(
                key,
                Either::Left(larger_state.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .expect("update with larger hash should succeed");
        assert!(
            matches!(result, UpsertResult::Updated(_)),
            "Larger hash should win and update"
        );

        // Verify the stored state is the larger-hash one
        let (stored, _) = executor
            .fetch_contract(key, false)
            .await
            .expect("fetch should succeed");
        assert_eq!(
            stored.unwrap().as_ref(),
            larger_state.as_ref(),
            "Stored state should be the larger-hash state"
        );
    }

    /// Test that CRDT merge rejects state with smaller hash
    #[tokio::test(flavor = "current_thread")]
    async fn crdt_merge_smaller_hash_rejected() {
        let mut executor = create_test_executor().await;

        // Create two states where we know their hash ordering
        let state_a = WrappedState::new(vec![1, 2, 3]);
        let state_b = WrappedState::new(vec![4, 5, 6]);

        let hash_a = blake3::hash(state_a.as_ref());
        let hash_b = blake3::hash(state_b.as_ref());

        // Determine which has larger hash
        let (smaller_state, larger_state) = if hash_a.as_bytes() < hash_b.as_bytes() {
            (state_a, state_b)
        } else {
            (state_b, state_a)
        };

        // Create contract
        let contract = create_test_contract(b"test_contract_code_2");
        let key = contract.key();

        // First, store the larger-hash state
        let result = executor
            .upsert_contract_state(
                key,
                Either::Left(larger_state.clone()),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .expect("initial store should succeed");
        assert!(matches!(result, UpsertResult::Updated(_)));

        // Now try to update with smaller-hash state - should be rejected
        let result = executor
            .upsert_contract_state(
                key,
                Either::Left(smaller_state.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .expect("update attempt should not error");
        assert!(
            matches!(result, UpsertResult::CurrentWon(_)),
            "Smaller hash should be rejected (CurrentWon indicating local state won)"
        );

        // Verify the stored state is still the larger-hash one
        let (stored, _) = executor
            .fetch_contract(key, false)
            .await
            .expect("fetch should succeed");
        assert_eq!(
            stored.unwrap().as_ref(),
            larger_state.as_ref(),
            "Stored state should still be the larger-hash state"
        );
    }

    /// Test that equal hash results in NoChange
    #[tokio::test(flavor = "current_thread")]
    async fn crdt_merge_equal_hash_no_change() {
        let mut executor = create_test_executor().await;

        let state = WrappedState::new(vec![1, 2, 3, 4, 5]);
        let contract = create_test_contract(b"test_contract_code_3");
        let key = contract.key();

        // Store initial state
        let result = executor
            .upsert_contract_state(
                key,
                Either::Left(state.clone()),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .expect("initial store should succeed");
        assert!(matches!(result, UpsertResult::Updated(_)));

        // Try to update with same state - should be NoChange
        let result = executor
            .upsert_contract_state(
                key,
                Either::Left(state.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .expect("update attempt should not error");
        assert!(
            matches!(result, UpsertResult::NoChange),
            "Same state should result in NoChange"
        );
    }

    /// Test that multiple "peers" converge to same state regardless of update order
    /// This simulates the core convergence property we need for DST
    #[tokio::test(flavor = "current_thread")]
    async fn crdt_merge_peers_converge() {
        // Create three distinct states
        let state_1 = WrappedState::new(vec![1, 1, 1]);
        let state_2 = WrappedState::new(vec![2, 2, 2]);
        let state_3 = WrappedState::new(vec![3, 3, 3]);

        // Compute hashes and find the "winner" (largest hash)
        let states = [
            (state_1.clone(), blake3::hash(state_1.as_ref())),
            (state_2.clone(), blake3::hash(state_2.as_ref())),
            (state_3.clone(), blake3::hash(state_3.as_ref())),
        ];

        let winner = states
            .iter()
            .max_by_key(|(_, hash)| hash.as_bytes())
            .map(|(state, _)| state.clone())
            .unwrap();

        // Simulate 3 peers that receive updates in different orders
        // All should converge to the same final state (the one with largest hash)

        // Peer A: receives updates in order 1, 2, 3
        let mut peer_a = create_test_executor().await;
        let contract = create_test_contract(b"convergence_test_contract");
        let key = contract.key();

        // Initialize with state_1
        peer_a
            .upsert_contract_state(
                key,
                Either::Left(state_1.clone()),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .unwrap();
        // Update with state_2
        peer_a
            .upsert_contract_state(
                key,
                Either::Left(state_2.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();
        // Update with state_3
        peer_a
            .upsert_contract_state(
                key,
                Either::Left(state_3.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();

        // Peer B: receives updates in order 3, 1, 2
        let mut peer_b = create_test_executor().await;
        peer_b
            .upsert_contract_state(
                key,
                Either::Left(state_3.clone()),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .unwrap();
        peer_b
            .upsert_contract_state(
                key,
                Either::Left(state_1.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();
        peer_b
            .upsert_contract_state(
                key,
                Either::Left(state_2.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();

        // Peer C: receives updates in order 2, 3, 1
        let mut peer_c = create_test_executor().await;
        peer_c
            .upsert_contract_state(
                key,
                Either::Left(state_2.clone()),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .unwrap();
        peer_c
            .upsert_contract_state(
                key,
                Either::Left(state_3.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();
        peer_c
            .upsert_contract_state(
                key,
                Either::Left(state_1.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();

        // All peers should have converged to the same state (largest hash)
        let (state_a, _) = peer_a.fetch_contract(key, false).await.unwrap();
        let (state_b, _) = peer_b.fetch_contract(key, false).await.unwrap();
        let (state_c, _) = peer_c.fetch_contract(key, false).await.unwrap();

        let state_a = state_a.expect("peer A should have state");
        let state_b = state_b.expect("peer B should have state");
        let state_c = state_c.expect("peer C should have state");

        // All peers should have the winner state
        assert_eq!(
            state_a.as_ref(),
            winner.as_ref(),
            "Peer A should converge to winner"
        );
        assert_eq!(
            state_b.as_ref(),
            winner.as_ref(),
            "Peer B should converge to winner"
        );
        assert_eq!(
            state_c.as_ref(),
            winner.as_ref(),
            "Peer C should converge to winner"
        );

        // All peers should have the same state
        assert_eq!(
            state_a.as_ref(),
            state_b.as_ref(),
            "Peer A and B should have same state"
        );
        assert_eq!(
            state_b.as_ref(),
            state_c.as_ref(),
            "Peer B and C should have same state"
        );
    }

    /// Test that delayed "old" broadcast doesn't overwrite newer state
    /// This is the specific scenario from issue #2634
    #[tokio::test(flavor = "current_thread")]
    async fn crdt_merge_delayed_broadcast_rejected() {
        let mut executor = create_test_executor().await;

        // Scenario from issue #2634:
        // 1. Peer has state S3 (from local update)
        // 2. Delayed broadcast of old state S1 arrives
        // 3. S1 should be rejected, S3 should remain

        // Create states where S3 has larger hash than S1
        // We'll try different byte patterns until we find the right ordering
        let mut s1 = WrappedState::new(vec![1, 0, 0, 0]);
        let mut s3 = WrappedState::new(vec![3, 0, 0, 0]);

        // Ensure S3 has larger hash (swap if needed)
        let hash_s1 = blake3::hash(s1.as_ref());
        let hash_s3 = blake3::hash(s3.as_ref());

        if hash_s1.as_bytes() > hash_s3.as_bytes() {
            std::mem::swap(&mut s1, &mut s3);
        }

        let contract = create_test_contract(b"delayed_broadcast_test");
        let key = contract.key();

        // Peer already has S3 (the newer state with larger hash)
        executor
            .upsert_contract_state(
                key,
                Either::Left(s3.clone()),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .unwrap();

        // Delayed broadcast of S1 arrives (older state with smaller hash)
        let result = executor
            .upsert_contract_state(
                key,
                Either::Left(s1.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();

        // S1 should be rejected (local state S3 won)
        assert!(
            matches!(result, UpsertResult::CurrentWon(_)),
            "Delayed old broadcast should be rejected (CurrentWon indicating local state won)"
        );

        // S3 should still be stored
        let (stored, _) = executor.fetch_contract(key, false).await.unwrap();
        assert_eq!(
            stored.unwrap().as_ref(),
            s3.as_ref(),
            "Newer state S3 should still be stored"
        );
    }
}
