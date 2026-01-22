use super::*;
use crate::message::NodeEvent;
use crate::node::OpManager;
use crate::wasm_runtime::{InMemoryContractStore, MockStateStorage, StateStorage};
use dashmap::DashSet;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

// =============================================================================
// CRDT Emulation Mode
// =============================================================================
//
// The CRDT emulation mode allows testing the delta-based sync protocol with
// contracts that behave like real CRDTs. When a contract is registered as CRDT:
//
// 1. State format: [version: u64 LE][data bytes]
// 2. Summary: [version: u64 LE][blake3 hash of data]
// 3. Delta: [from_version: u64 LE][to_version: u64 LE][new data]
// 4. Delta application: FAILS if current version != from_version
//
// This enables testing PR #2763's fix: if sender incorrectly caches their summary
// as recipient's summary, the next delta will have wrong from_version, causing
// delta application to fail and trigger ResyncRequest.

/// Global registry of contracts using CRDT emulation mode.
/// Thread-safe for use across multiple simulation nodes.
static CRDT_CONTRACTS: std::sync::LazyLock<DashSet<ContractInstanceId>> =
    std::sync::LazyLock::new(DashSet::new);

/// Register a contract to use CRDT emulation mode.
///
/// CRDT mode enables version-aware delta computation and application,
/// which can trigger ResyncRequests when summary caching is incorrect.
pub fn register_crdt_contract(contract_id: ContractInstanceId) {
    CRDT_CONTRACTS.insert(contract_id);
    tracing::debug!(contract = %contract_id, "Registered contract for CRDT emulation mode");
}

/// Check if a contract uses CRDT emulation mode.
pub fn is_crdt_contract(contract_id: &ContractInstanceId) -> bool {
    CRDT_CONTRACTS.contains(contract_id)
}

/// Clear all CRDT contract registrations (for test cleanup).
pub fn clear_crdt_contracts() {
    CRDT_CONTRACTS.clear();
}

/// CRDT state encoding: [version: u64 LE][data]
mod crdt_encoding {
    use super::*;

    /// Minimum state size for CRDT mode (8 bytes for version)
    pub const MIN_STATE_SIZE: usize = 8;

    /// Encode state with version prefix.
    pub fn encode_state(version: u64, data: &[u8]) -> Vec<u8> {
        let mut result = Vec::with_capacity(8 + data.len());
        result.extend_from_slice(&version.to_le_bytes());
        result.extend_from_slice(data);
        result
    }

    /// Decode version and data from state.
    pub fn decode_state(state: &[u8]) -> Option<(u64, &[u8])> {
        if state.len() < MIN_STATE_SIZE {
            return None;
        }
        let version = u64::from_le_bytes(state[0..8].try_into().ok()?);
        let data = &state[8..];
        Some((version, data))
    }

    /// Get version from state without copying data.
    pub fn get_version(state: &[u8]) -> Option<u64> {
        if state.len() < MIN_STATE_SIZE {
            return None;
        }
        Some(u64::from_le_bytes(state[0..8].try_into().ok()?))
    }

    /// Summary format: [version: u64 LE][blake3 hash: 32 bytes] = 40 bytes
    pub fn create_summary(state: &[u8]) -> Option<Vec<u8>> {
        let (version, data) = decode_state(state)?;
        let hash = blake3::hash(data);
        let mut summary = Vec::with_capacity(40);
        summary.extend_from_slice(&version.to_le_bytes());
        summary.extend_from_slice(hash.as_bytes());
        Some(summary)
    }

    /// Extract version from summary.
    pub fn summary_version(summary: &[u8]) -> Option<u64> {
        if summary.len() < 8 {
            return None;
        }
        Some(u64::from_le_bytes(summary[0..8].try_into().ok()?))
    }

    /// Delta format: [from_version: u64 LE][to_version: u64 LE][new data]
    pub fn create_delta(from_version: u64, to_version: u64, new_data: &[u8]) -> Vec<u8> {
        let mut delta = Vec::with_capacity(16 + new_data.len());
        delta.extend_from_slice(&from_version.to_le_bytes());
        delta.extend_from_slice(&to_version.to_le_bytes());
        delta.extend_from_slice(new_data);
        delta
    }

    /// Decode delta: returns (from_version, to_version, new_data).
    pub fn decode_delta(delta: &[u8]) -> Option<(u64, u64, &[u8])> {
        if delta.len() < 16 {
            return None;
        }
        let from_version = u64::from_le_bytes(delta[0..8].try_into().ok()?);
        let to_version = u64::from_le_bytes(delta[8..16].try_into().ok()?);
        let new_data = &delta[16..];
        Some((from_version, to_version, new_data))
    }
}

/// Mock runtime for testing that uses fully in-memory storage.
///
/// Unlike the production runtime which uses disk-based storage with background
/// threads (file watchers, compaction), this runtime keeps everything in memory
/// for deterministic simulation testing.
///
/// ## CRDT Emulation Mode
///
/// Contracts can be registered for CRDT emulation using `register_crdt_contract()`.
/// In CRDT mode:
/// - State includes a version number
/// - Summary includes version for delta computation
/// - Delta application fails if version mismatch (triggers ResyncRequest)
///
/// This enables testing of PR #2763's summary caching fix.
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

    /// Apply a CRDT-mode delta using LWW-Register (Last-Writer-Wins) semantics.
    ///
    /// This implements a proper CRDT with convergent merge semantics:
    /// - Higher version always wins (version acts as logical timestamp)
    /// - Equal versions use hash comparison as deterministic tiebreaker
    /// - Lower versions are rejected (we already have newer data)
    ///
    /// This ensures all nodes converge to the same state regardless of message
    /// delivery order, which is essential for simulation test correctness.
    ///
    /// Delta format: [from_version: u64][to_version: u64][new_data]
    async fn apply_crdt_delta(
        &mut self,
        key: &ContractKey,
        current_state: &WrappedState,
        delta: &StateDelta<'_>,
        _has_contract_code: bool,
    ) -> Result<UpsertResult, ExecutorError> {
        // Decode the delta
        let (_from_version, to_version, new_data) = crdt_encoding::decode_delta(delta.as_ref())
            .ok_or_else(|| ExecutorError::other(anyhow::anyhow!("Invalid CRDT delta format")))?;

        // Get current version and data
        let (current_version, current_data) = crdt_encoding::decode_state(current_state.as_ref())
            .ok_or_else(|| {
            ExecutorError::other(anyhow::anyhow!("Invalid CRDT state format"))
        })?;

        tracing::debug!(
            contract = %key,
            current_version = current_version,
            delta_to_version = to_version,
            "CRDT mode: applying delta with LWW semantics"
        );

        // LWW-Register CRDT merge logic:
        // 1. Higher version wins
        // 2. Equal versions use hash comparison as tiebreaker
        // 3. Lower versions are rejected
        let should_update = if to_version > current_version {
            tracing::debug!(
                contract = %key,
                "CRDT: incoming version {} > current version {} - accepting",
                to_version, current_version
            );
            true
        } else if to_version == current_version {
            // Tiebreaker: compare hashes of the data (not the full state)
            let incoming_hash = blake3::hash(new_data);
            let current_hash = blake3::hash(current_data);
            let accept = incoming_hash.as_bytes() > current_hash.as_bytes();
            tracing::debug!(
                contract = %key,
                "CRDT: equal versions, hash tiebreaker - {}",
                if accept { "accepting incoming" } else { "keeping current" }
            );
            accept
        } else {
            tracing::debug!(
                contract = %key,
                "CRDT: incoming version {} < current version {} - rejecting",
                to_version, current_version
            );
            false
        };

        if should_update {
            let new_state_bytes = crdt_encoding::encode_state(to_version, new_data);
            let new_state = WrappedState::new(new_state_bytes);

            self.state_store
                .update(key, new_state.clone())
                .await
                .map_err(ExecutorError::other)?;

            tracing::debug!(
                contract = %key,
                old_version = current_version,
                new_version = to_version,
                "CRDT mode: delta applied successfully"
            );

            Ok(UpsertResult::Updated(new_state))
        } else {
            Ok(UpsertResult::NoChange)
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
                        // Check for CRDT mode - version-aware delta application
                        if is_crdt_contract(key.id()) {
                            return self
                                .apply_crdt_delta(&key, &current_state, &delta, true)
                                .await;
                        }

                        // Default mode: Apply delta by concatenating with current state
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
                        // Check for CRDT mode - version-aware delta application
                        if is_crdt_contract(key.id()) {
                            return self
                                .apply_crdt_delta(&key, &current_state, &delta, false)
                                .await;
                        }

                        // Default mode: Apply delta by concatenating with current state
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
        // Echo-back prevention is handled by summary comparison in p2p_protoc.
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
        let state = self
            .state_store
            .get(&key)
            .await
            .map_err(ExecutorError::other)?;

        // Check if this contract uses CRDT emulation mode
        if is_crdt_contract(key.id()) {
            // CRDT mode: summary includes version + hash
            if let Some(summary) = crdt_encoding::create_summary(state.as_ref()) {
                tracing::trace!(
                    contract = %key,
                    version = crdt_encoding::get_version(state.as_ref()),
                    "CRDT mode: created versioned summary"
                );
                return Ok(StateSummary::from(summary));
            }
            // Fall through to default if state format is invalid
            tracing::warn!(
                contract = %key,
                "CRDT mode: invalid state format, falling back to hash summary"
            );
        }

        // Default mode: hash-based summary (32 bytes) to enable delta efficiency.
        // With hash summaries, is_delta_efficient(32, state_size) = true when state > 64 bytes.
        let hash = blake3::hash(state.as_ref());
        Ok(StateSummary::from(hash.as_bytes().to_vec()))
    }

    async fn get_contract_state_delta(
        &mut self,
        key: ContractKey,
        their_summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ExecutorError> {
        // Check if this contract uses CRDT emulation mode
        if is_crdt_contract(key.id()) {
            // CRDT mode: compute version-aware delta
            let state = self
                .state_store
                .get(&key)
                .await
                .map_err(ExecutorError::other)?;

            let their_version =
                crdt_encoding::summary_version(their_summary.as_ref()).ok_or_else(|| {
                    ExecutorError::other(anyhow::anyhow!("Invalid CRDT summary format"))
                })?;

            let (our_version, our_data) =
                crdt_encoding::decode_state(state.as_ref()).ok_or_else(|| {
                    ExecutorError::other(anyhow::anyhow!("Invalid CRDT state format"))
                })?;

            tracing::trace!(
                contract = %key,
                their_version = their_version,
                our_version = our_version,
                "CRDT mode: computing delta"
            );

            // Create delta that transforms their state to ours
            let delta = crdt_encoding::create_delta(their_version, our_version, our_data);
            return Ok(StateDelta::from(delta));
        }

        // Default mode: cannot compute real deltas
        // By returning an error, we trigger the full state fallback path with
        // sent_delta=false, which correctly avoids caching the sender's summary.
        Err(ExecutorError::other(anyhow::anyhow!(
            "MockRuntime cannot compute deltas - use full state sync"
        )))
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

    // =========================================================================
    // CRDT Emulation Mode Tests (LWW-Register semantics via delta application)
    // =========================================================================
    //
    // These tests verify the CRDT emulation mode that uses version-based
    // Last-Writer-Wins (LWW) semantics for delta application. This is the
    // mode used by simulation tests via `register_crdt_contract()`.

    /// Helper to create initial CRDT state with version
    fn create_crdt_state(version: u64, data: &[u8]) -> WrappedState {
        WrappedState::new(crdt_encoding::encode_state(version, data))
    }

    /// Helper to create a CRDT delta
    fn create_crdt_delta(from_version: u64, to_version: u64, data: &[u8]) -> StateDelta<'static> {
        StateDelta::from(crdt_encoding::create_delta(from_version, to_version, data))
    }

    /// Test that higher version wins in CRDT mode delta application
    #[tokio::test(flavor = "current_thread")]
    async fn crdt_emulation_higher_version_wins() {
        let mut executor = create_test_executor().await;

        let contract = create_test_contract(b"crdt_emulation_test_1");
        let key = contract.key();

        // Register for CRDT mode
        register_crdt_contract(*key.id());

        // Initialize with version 1
        let initial_state = create_crdt_state(1, b"initial data");
        executor
            .upsert_contract_state(
                key,
                Either::Left(initial_state),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .unwrap();

        // Apply delta with higher version (1 -> 2)
        let delta = create_crdt_delta(1, 2, b"updated data v2");
        let result = executor
            .upsert_contract_state(key, Either::Right(delta), RelatedContracts::default(), None)
            .await
            .unwrap();

        assert!(
            matches!(result, UpsertResult::Updated(_)),
            "Higher version delta should be accepted"
        );

        // Verify state was updated
        let (stored, _) = executor.fetch_contract(key, false).await.unwrap();
        let stored = stored.unwrap();
        let (version, data) = crdt_encoding::decode_state(stored.as_ref()).unwrap();
        assert_eq!(version, 2);
        assert_eq!(data, b"updated data v2");
        // Note: Don't call clear_crdt_contracts() - tests run in parallel and share the global registry
    }

    /// Test that lower version is rejected in CRDT mode delta application
    #[tokio::test(flavor = "current_thread")]
    async fn crdt_emulation_lower_version_rejected() {
        let mut executor = create_test_executor().await;

        let contract = create_test_contract(b"crdt_emulation_test_2");
        let key = contract.key();

        // Register for CRDT mode
        register_crdt_contract(*key.id());

        // Initialize with version 5
        let initial_state = create_crdt_state(5, b"version 5 data");
        executor
            .upsert_contract_state(
                key,
                Either::Left(initial_state),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .unwrap();

        // Apply delta with lower version (2 -> 3) - should be rejected
        let delta = create_crdt_delta(2, 3, b"old data v3");
        let result = executor
            .upsert_contract_state(key, Either::Right(delta), RelatedContracts::default(), None)
            .await
            .unwrap();

        assert!(
            matches!(result, UpsertResult::NoChange),
            "Lower version delta should be rejected with NoChange"
        );

        // Verify state was NOT updated
        let (stored, _) = executor.fetch_contract(key, false).await.unwrap();
        let stored = stored.unwrap();
        let (version, data) = crdt_encoding::decode_state(stored.as_ref()).unwrap();
        assert_eq!(version, 5);
        assert_eq!(data, b"version 5 data");
        // Note: Don't call clear_crdt_contracts() - tests run in parallel and share the global registry
    }

    /// Test that equal versions use hash comparison as tiebreaker
    #[tokio::test(flavor = "current_thread")]
    async fn crdt_emulation_equal_version_hash_tiebreaker() {
        let mut executor = create_test_executor().await;

        let contract = create_test_contract(b"crdt_emulation_test_3");
        let key = contract.key();

        // Register for CRDT mode
        register_crdt_contract(*key.id());

        // Create two data values with known hash ordering at same version
        let data_a = b"aaaa";
        let data_b = b"bbbb";
        let hash_a = blake3::hash(data_a);
        let hash_b = blake3::hash(data_b);

        let (smaller_data, larger_data) = if hash_a.as_bytes() < hash_b.as_bytes() {
            (data_a.as_slice(), data_b.as_slice())
        } else {
            (data_b.as_slice(), data_a.as_slice())
        };

        // Initialize with smaller hash data at version 5
        let initial_state = create_crdt_state(5, smaller_data);
        executor
            .upsert_contract_state(
                key,
                Either::Left(initial_state),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .unwrap();

        // Apply delta with same version but larger hash data - should win
        let delta = create_crdt_delta(5, 5, larger_data);
        let result = executor
            .upsert_contract_state(key, Either::Right(delta), RelatedContracts::default(), None)
            .await
            .unwrap();

        assert!(
            matches!(result, UpsertResult::Updated(_)),
            "Equal version with larger hash should win"
        );

        // Verify state has larger hash data
        let (stored, _) = executor.fetch_contract(key, false).await.unwrap();
        let stored = stored.unwrap();
        let (version, data) = crdt_encoding::decode_state(stored.as_ref()).unwrap();
        assert_eq!(version, 5);
        assert_eq!(data, larger_data);
        // Note: Don't call clear_crdt_contracts() - tests run in parallel and share the global registry
    }

    /// Test that equal version with smaller hash is rejected
    #[tokio::test(flavor = "current_thread")]
    async fn crdt_emulation_equal_version_smaller_hash_rejected() {
        let mut executor = create_test_executor().await;

        let contract = create_test_contract(b"crdt_emulation_test_4");
        let key = contract.key();

        // Register for CRDT mode
        register_crdt_contract(*key.id());

        // Create two data values with known hash ordering at same version
        let data_a = b"aaaa";
        let data_b = b"bbbb";
        let hash_a = blake3::hash(data_a);
        let hash_b = blake3::hash(data_b);

        let (smaller_data, larger_data) = if hash_a.as_bytes() < hash_b.as_bytes() {
            (data_a.as_slice(), data_b.as_slice())
        } else {
            (data_b.as_slice(), data_a.as_slice())
        };

        // Initialize with larger hash data at version 5
        let initial_state = create_crdt_state(5, larger_data);
        executor
            .upsert_contract_state(
                key,
                Either::Left(initial_state),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .unwrap();

        // Apply delta with same version but smaller hash data - should be rejected
        let delta = create_crdt_delta(5, 5, smaller_data);
        let result = executor
            .upsert_contract_state(key, Either::Right(delta), RelatedContracts::default(), None)
            .await
            .unwrap();

        assert!(
            matches!(result, UpsertResult::NoChange),
            "Equal version with smaller hash should be rejected"
        );

        // Verify state still has larger hash data
        let (stored, _) = executor.fetch_contract(key, false).await.unwrap();
        let stored = stored.unwrap();
        let (version, data) = crdt_encoding::decode_state(stored.as_ref()).unwrap();
        assert_eq!(version, 5);
        assert_eq!(data, larger_data);
        // Note: Don't call clear_crdt_contracts() - tests run in parallel and share the global registry
    }

    /// Test that CRDT emulation mode converges regardless of delta arrival order
    /// This is the key property ensuring simulation tests work correctly
    #[tokio::test(flavor = "current_thread")]
    async fn crdt_emulation_convergence_any_order() {
        // Simulate 3 peers receiving the same deltas in different orders
        // All should converge to version 3 with the same data

        let contract = create_test_contract(b"crdt_convergence_test");
        let key = contract.key();
        let contract_id = *key.id();

        // Register ONCE at the beginning - don't clear during test to avoid race conditions
        // with parallel tests that share the global CRDT_CONTRACTS registry
        register_crdt_contract(contract_id);

        // Define 3 deltas representing updates v1->v2, v2->v3, and a "late" v1->v2
        let delta_1_to_2 = create_crdt_delta(1, 2, b"data at version 2");
        let delta_2_to_3 = create_crdt_delta(2, 3, b"data at version 3");
        let delta_1_to_2_late = create_crdt_delta(1, 2, b"late update to v2"); // Different data, same target version

        let initial = create_crdt_state(1, b"initial");

        // Peer A: receives deltas in order 1->2, 2->3, late 1->2
        let mut peer_a = create_test_executor().await;
        peer_a
            .upsert_contract_state(
                key,
                Either::Left(initial.clone()),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .unwrap();
        peer_a
            .upsert_contract_state(
                key,
                Either::Right(delta_1_to_2.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();
        peer_a
            .upsert_contract_state(
                key,
                Either::Right(delta_2_to_3.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();
        peer_a
            .upsert_contract_state(
                key,
                Either::Right(delta_1_to_2_late.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();

        // Peer B: receives deltas in order 2->3, 1->2, late 1->2
        let mut peer_b = create_test_executor().await;
        peer_b
            .upsert_contract_state(
                key,
                Either::Left(initial.clone()),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .unwrap();
        peer_b
            .upsert_contract_state(
                key,
                Either::Right(delta_2_to_3.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();
        peer_b
            .upsert_contract_state(
                key,
                Either::Right(delta_1_to_2.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();
        peer_b
            .upsert_contract_state(
                key,
                Either::Right(delta_1_to_2_late.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();

        // Peer C: receives deltas in order late 1->2, 2->3, 1->2
        let mut peer_c = create_test_executor().await;
        peer_c
            .upsert_contract_state(
                key,
                Either::Left(initial.clone()),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .unwrap();
        peer_c
            .upsert_contract_state(
                key,
                Either::Right(delta_1_to_2_late.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();
        peer_c
            .upsert_contract_state(
                key,
                Either::Right(delta_2_to_3.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();
        peer_c
            .upsert_contract_state(
                key,
                Either::Right(delta_1_to_2.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();

        // All peers should have converged to version 3
        let (state_a, _) = peer_a.fetch_contract(key, false).await.unwrap();
        let (state_b, _) = peer_b.fetch_contract(key, false).await.unwrap();
        let (state_c, _) = peer_c.fetch_contract(key, false).await.unwrap();

        let state_a = state_a.unwrap();
        let state_b = state_b.unwrap();
        let state_c = state_c.unwrap();

        let (ver_a, data_a) = crdt_encoding::decode_state(state_a.as_ref()).unwrap();
        let (ver_b, data_b) = crdt_encoding::decode_state(state_b.as_ref()).unwrap();
        let (ver_c, data_c) = crdt_encoding::decode_state(state_c.as_ref()).unwrap();

        // All should be at version 3 with the same data
        assert_eq!(ver_a, 3, "Peer A should be at version 3");
        assert_eq!(ver_b, 3, "Peer B should be at version 3");
        assert_eq!(ver_c, 3, "Peer C should be at version 3");

        assert_eq!(data_a, data_b, "Peer A and B should have same data");
        assert_eq!(data_b, data_c, "Peer B and C should have same data");
        assert_eq!(data_a, b"data at version 3", "All should have v3 data");
    }

    /// Test idempotency: applying the same delta twice has no effect
    #[tokio::test(flavor = "current_thread")]
    async fn crdt_emulation_idempotent() {
        let mut executor = create_test_executor().await;

        let contract = create_test_contract(b"crdt_idempotent_test");
        let key = contract.key();

        register_crdt_contract(*key.id());

        // Initialize with version 1
        let initial = create_crdt_state(1, b"initial");
        executor
            .upsert_contract_state(
                key,
                Either::Left(initial),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .unwrap();

        // Apply delta 1->2
        let delta = create_crdt_delta(1, 2, b"version 2 data");
        let result1 = executor
            .upsert_contract_state(
                key,
                Either::Right(delta.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();
        assert!(matches!(result1, UpsertResult::Updated(_)));

        // Apply same delta again - should be NoChange (idempotent)
        let result2 = executor
            .upsert_contract_state(
                key,
                Either::Right(delta.clone()),
                RelatedContracts::default(),
                None,
            )
            .await
            .unwrap();
        assert!(
            matches!(result2, UpsertResult::NoChange),
            "Applying same delta twice should be idempotent (NoChange)"
        );

        // Verify state is correct
        let (stored, _) = executor.fetch_contract(key, false).await.unwrap();
        let stored = stored.unwrap();
        let (version, data) = crdt_encoding::decode_state(stored.as_ref()).unwrap();
        assert_eq!(version, 2);
        assert_eq!(data, b"version 2 data");
        // Note: Don't call clear_crdt_contracts() - tests run in parallel and share the global registry
    }
}
