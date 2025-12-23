use super::*;
use super::{
    ContractExecutor, ContractRequest, ContractResponse, ExecutorError, InitCheckResult,
    OpRequestSender, RequestError, Response, StateStoreError, SLOW_INIT_THRESHOLD,
    STALE_INIT_THRESHOLD,
};
use crate::node::OpManager;
use freenet_stdlib::prelude::RelatedContract;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;

// ============================================================================
// RuntimePool - Pool of executors for concurrent contract execution
// ============================================================================

/// Health status information for the RuntimePool.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PoolHealthStatus {
    /// Number of executors currently available in the pool
    pub available: usize,
    /// Number of executors currently checked out (in use)
    pub checked_out: usize,
    /// Total pool size (should equal available + checked_out)
    pub total: usize,
    /// Number of executors that were replaced due to failures
    pub replacements: usize,
}

/// A pool of executors that enables concurrent contract execution.
///
/// This pool manages multiple `Executor<Runtime>` instances, allowing multiple
/// contract operations to run in parallel. The pool uses a semaphore to control
/// access to executors and ensures thread-safe executor borrowing and returning.
///
/// # Architecture
///
/// The pool maintains a fixed number of executors (typically CPU count) and uses
/// a semaphore to gate access. When an operation needs an executor:
/// 1. It acquires a semaphore permit (blocking if all executors are busy)
/// 2. Takes an available executor from the pool
/// 3. Executes the operation
/// 4. Returns the executor to the pool and releases the permit
///
/// This design ensures:
/// - Bounded parallelism (no unbounded thread spawning)
/// - Fair access to executors
/// - Proper cleanup on errors
///
/// # Pool Health Tracking
///
/// The pool tracks actual executor count separately from the semaphore to detect
/// capacity degradation. If executors are lost due to panics or other issues,
/// the pool can detect and report this mismatch.
pub struct RuntimePool {
    /// Pool of available executors. `None` slots indicate executors currently in use.
    runtimes: Vec<Option<Executor<Runtime>>>,
    /// Semaphore controlling access to executors (permits = available executors)
    available: Semaphore,
    /// Configuration for creating new executors
    config: Arc<Config>,
    /// Channel to send operation requests to the event loop (cloneable, shared by all executors)
    op_sender: OpRequestSender,
    /// Reference to the operation manager (cloneable, shared by all executors)
    op_manager: Arc<OpManager>,
    /// Total pool size (for health checking)
    pool_size: usize,
    /// Count of executors currently checked out (for health checking)
    checked_out: AtomicUsize,
    /// Count of executors that were lost/replaced (for monitoring)
    replacements_count: AtomicUsize,
}

impl RuntimePool {
    /// Create a new pool with the specified number of executors.
    ///
    /// # Arguments
    /// * `config` - Configuration for executors
    /// * `op_sender` - Channel to send operation requests to the event loop (cloneable)
    /// * `op_manager` - Reference to the operation manager
    /// * `pool_size` - Number of executors to create (typically CPU count)
    pub async fn new(
        config: Arc<Config>,
        op_sender: OpRequestSender,
        op_manager: Arc<OpManager>,
        pool_size: NonZeroUsize,
    ) -> anyhow::Result<Self> {
        let pool_size_usize: usize = pool_size.into();
        let mut runtimes = Vec::with_capacity(pool_size_usize);

        // Create all executors with clones of op_sender and op_manager
        // Each executor can independently send requests to the event loop
        for _ in 0..pool_size_usize {
            let executor = Executor::from_config(
                config.clone(),
                Some(op_sender.clone()),
                Some(op_manager.clone()),
            )
            .await?;
            runtimes.push(Some(executor));
        }

        tracing::info!(
            pool_size = pool_size_usize,
            "Created RuntimePool with {} executors",
            pool_size_usize
        );

        Ok(Self {
            runtimes,
            available: Semaphore::new(pool_size_usize),
            config,
            op_sender,
            op_manager,
            pool_size: pool_size_usize,
            checked_out: AtomicUsize::new(0),
            replacements_count: AtomicUsize::new(0),
        })
    }

    /// Get the current health status of the pool.
    ///
    /// Returns a tuple of:
    /// - `available`: Number of executors currently available
    /// - `checked_out`: Number of executors currently in use
    /// - `total`: Total pool size
    /// - `replacements`: Number of executors that were replaced due to failures
    pub fn health_status(&self) -> PoolHealthStatus {
        let available = self.runtimes.iter().filter(|s| s.is_some()).count();
        let checked_out = self.checked_out.load(Ordering::SeqCst);
        let replacements = self.replacements_count.load(Ordering::SeqCst);

        // Check for pool degradation
        let expected_total = available + checked_out;
        if expected_total != self.pool_size {
            tracing::warn!(
                available = available,
                checked_out = checked_out,
                expected = self.pool_size,
                actual = expected_total,
                "Pool capacity mismatch detected - possible semaphore drift"
            );
        }

        PoolHealthStatus {
            available,
            checked_out,
            total: self.pool_size,
            replacements,
        }
    }

    /// Log the pool health status at debug level.
    /// Called periodically when there have been replacements to help diagnose issues.
    fn log_health_if_degraded(&self) {
        let status = self.health_status();
        if status.replacements > 0 {
            tracing::info!(
                available = status.available,
                checked_out = status.checked_out,
                total = status.total,
                replacements = status.replacements,
                "RuntimePool health status (degraded - {} replacements)",
                status.replacements
            );
        }
    }

    /// Pop an executor from the pool, blocking until one is available.
    ///
    /// The caller MUST return the executor via `return_executor` after use.
    async fn pop_executor(&mut self) -> Executor<Runtime> {
        // Wait for an available permit
        let permit = self
            .available
            .acquire()
            .await
            .expect("Semaphore should not be closed");

        // Consume the permit without returning it to the semaphore.
        // The permit will be restored in `return_executor` via `add_permits(1)`.
        permit.forget();

        // Track that we're checking out an executor
        self.checked_out.fetch_add(1, Ordering::SeqCst);

        // Find the first available executor
        for slot in &mut self.runtimes {
            if let Some(executor) = slot.take() {
                return executor;
            }
        }

        // This should never happen because of the semaphore
        // But if it does, we need to restore the checked_out count
        self.checked_out.fetch_sub(1, Ordering::SeqCst);
        unreachable!("No executors available despite semaphore permit")
    }

    /// Return an executor to the pool after use.
    fn return_executor(&mut self, executor: Executor<Runtime>) {
        // Track that we're returning an executor
        self.checked_out.fetch_sub(1, Ordering::SeqCst);

        // Find an empty slot
        if let Some(empty_slot) = self.runtimes.iter_mut().find(|slot| slot.is_none()) {
            *empty_slot = Some(executor);
            self.available.add_permits(1);
        } else {
            // This should never happen, but log it if it does
            tracing::error!(
                pool_size = self.pool_size,
                checked_out = self.checked_out.load(Ordering::SeqCst),
                "No empty slot found when returning executor - pool may be corrupted"
            );
            // Still add the permit back to avoid deadlock
            self.available.add_permits(1);
        }
    }

    /// Check if an executor is healthy and can be reused.
    /// An executor is unhealthy if its wasm_store is None (lost due to panic).
    fn is_executor_healthy(executor: &Executor<Runtime>) -> bool {
        executor.runtime.is_healthy()
    }

    /// Create a new executor to replace a broken one.
    async fn create_replacement_executor(&self) -> anyhow::Result<Executor<Runtime>> {
        tracing::warn!("Creating replacement executor due to previous failure");
        Executor::from_config(
            self.config.clone(),
            Some(self.op_sender.clone()),
            Some(self.op_manager.clone()),
        )
        .await
    }
}

impl ContractExecutor for RuntimePool {
    fn lookup_key(&self, instance_id: &ContractInstanceId) -> Option<ContractKey> {
        // Try to find the key in any available executor
        self.runtimes.iter().flatten().find_map(|executor| {
            executor
                .runtime
                .contract_store
                .code_hash_from_id(instance_id)
                .map(|key| ContractKey::from_id_and_code(*instance_id, key))
        })
    }

    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        return_contract_code: bool,
    ) -> Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError> {
        let mut executor = self.pop_executor().await;
        let result = executor.fetch_contract(key, return_contract_code).await;

        // Check if executor is still healthy after the operation
        // If the WASM execution panicked, the store is lost and we need a new executor
        if !Self::is_executor_healthy(&executor) {
            let replacement_num = self.replacements_count.fetch_add(1, Ordering::SeqCst) + 1;
            tracing::warn!(
                contract = %key,
                replacement_number = replacement_num,
                "Executor became unhealthy after fetch_contract, creating replacement"
            );
            match self.create_replacement_executor().await {
                Ok(new_executor) => {
                    self.return_executor(new_executor);
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to create replacement executor");
                    // Return the broken executor anyway - next operation will fail
                    // but at least the pool isn't depleted
                    self.return_executor(executor);
                }
            }
            // Log health status after replacement
            self.log_health_if_degraded();
        } else {
            self.return_executor(executor);
        }
        result
    }

    async fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<UpsertResult, ExecutorError> {
        let mut executor = self.pop_executor().await;
        let result = executor
            .upsert_contract_state(key, update, related_contracts, code)
            .await;

        // Check if executor is still healthy after the operation
        if !Self::is_executor_healthy(&executor) {
            let replacement_num = self.replacements_count.fetch_add(1, Ordering::SeqCst) + 1;
            tracing::warn!(
                contract = %key,
                replacement_number = replacement_num,
                "Executor became unhealthy after upsert_contract_state, creating replacement"
            );
            match self.create_replacement_executor().await {
                Ok(new_executor) => {
                    self.return_executor(new_executor);
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to create replacement executor");
                    self.return_executor(executor);
                }
            }
            // Log health status after replacement
            self.log_health_if_degraded();
        } else {
            self.return_executor(executor);
        }
        result
    }

    fn register_contract_notifier(
        &mut self,
        instance_id: ContractInstanceId,
        cli_id: ClientId,
        notification_ch: tokio::sync::mpsc::UnboundedSender<HostResult>,
        summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>> {
        // Register with all available executors to ensure notifications work
        // regardless of which executor handles subsequent operations
        let owned_summary = summary.map(StateSummary::into_owned);

        let last_error = self
            .runtimes
            .iter_mut()
            .flatten()
            .filter_map(|executor| {
                executor
                    .register_contract_notifier(
                        instance_id,
                        cli_id,
                        notification_ch.clone(),
                        owned_summary.clone(),
                    )
                    .err()
            })
            .last();

        last_error.map_or(Ok(()), Err)
    }

    fn execute_delegate_request(
        &mut self,
        req: DelegateRequest<'_>,
        attested_contract: Option<&ContractInstanceId>,
    ) -> Response {
        // For delegate requests, use the first available executor synchronously
        // This is acceptable because delegate operations are typically quick
        // Find the first available executor's index
        let executor_idx = self.runtimes.iter().position(|slot| slot.is_some());

        match executor_idx {
            Some(idx) => {
                let executor = self.runtimes[idx].as_mut().unwrap();
                executor.execute_delegate_request(req, attested_contract)
            }
            None => Err(ExecutorError::other(anyhow::anyhow!(
                "No executors available for delegate request"
            ))),
        }
    }

    fn get_subscription_info(&self) -> Vec<crate::message::SubscriptionInfo> {
        // Collect subscription info from all executors
        self.runtimes
            .iter()
            .flatten()
            .flat_map(|executor| executor.get_subscription_info())
            .collect()
    }
}

// ============================================================================
// Single Executor Implementation
// ============================================================================

/// Result of computing a state update without committing.
/// Used in network mode to separate state computation from commit,
/// allowing the network operation to handle the commit and properly
/// detect changes for broadcasting.
enum ComputedStateUpdate {
    /// State changed - contains the new state to commit
    Changed(WrappedState),
    /// No change detected - contains the current state
    NoChange(WrappedState),
    /// Missing related contracts - contains the list of required contracts
    MissingRelated(Vec<RelatedContract>),
}

impl ContractExecutor for Executor<Runtime> {
    fn lookup_key(&self, instance_id: &ContractInstanceId) -> Option<ContractKey> {
        let code_hash = self.runtime.contract_store.code_hash_from_id(instance_id)?;
        Some(ContractKey::from_id_and_code(*instance_id, code_hash))
    }

    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        return_contract_code: bool,
    ) -> Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError> {
        tracing::debug!(
            contract = %key,
            return_code = return_contract_code,
            "fetching contract"
        );
        let result = self.perform_contract_get(return_contract_code, key).await;
        if let Ok((Some(ref state), ref code)) = result {
            let hash = blake3::hash(state.as_ref());
            tracing::debug!(
                contract = %key,
                state_size = state.as_ref().len(),
                state_hash = %hash,
                has_code = code.is_some(),
                "fetched contract state"
            );
        }
        match result {
            Ok((state, code)) => Ok((state, code)),
            Err(err) => Err(err),
        }
    }

    async fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<UpsertResult, ExecutorError> {
        // CRITICAL: When a ContractContainer is provided, use its key instead of the passed-in key.
        // The container's key is authoritative and includes the code hash, which is needed for
        // proper contract store lookups. The passed-in key may have code=None (e.g., from GET
        // responses where only the ContractInstanceId was preserved in the message).
        // See issue #2306 and related debugging.
        let key = if let Some(ref container) = code {
            let container_key = container.key();
            // Validate that the instance IDs match - if they don't, something is seriously wrong
            // (corrupted message, bug, or malicious peer). The code hashes may differ (that's
            // expected - the container has the authoritative one), but instance IDs must match.
            if key.id() != container_key.id() {
                tracing::error!(
                    passed_key = %key,
                    container_key = %container_key,
                    "CRITICAL: Contract key instance ID mismatch - passed key doesn't match container"
                );
                return Err(ExecutorError::other(anyhow::anyhow!(
                    "contract key instance ID mismatch"
                )));
            }
            container_key
        } else {
            key
        };

        // Opportunistically clean up any stale initializations to prevent resource leaks
        let stale = self
            .init_tracker
            .cleanup_stale_initializations(STALE_INIT_THRESHOLD);
        for info in stale {
            tracing::warn!(
                contract = %info.key,
                age_secs = info.age.as_secs(),
                dropped_ops = info.dropped_ops,
                "Cleaned up stale contract initialization (possible bug or timeout)"
            );
        }

        // Check if this contract is currently being initialized
        match self.init_tracker.check_and_maybe_queue(
            &key,
            code.is_some(),
            update.clone(),
            related_contracts.clone(),
        ) {
            InitCheckResult::NotInitializing => {
                // Continue with normal processing below
            }
            InitCheckResult::PutDuringInit => {
                return Err(ExecutorError::request(StdContractError::Put {
                    key,
                    cause: "contract is already being initialized".into(),
                }));
            }
            InitCheckResult::Queued { queue_size } => {
                tracing::info!(
                    contract = %key,
                    queue_size,
                    "Operation queued during contract initialization"
                );
                // Return NoChange to indicate the operation didn't fail but also didn't complete
                // The caller should retry later once initialization is complete
                return Ok(UpsertResult::NoChange);
            }
        }
        if let Either::Left(ref state) = update {
            let hash = blake3::hash(state.as_ref());
            tracing::debug!(
                contract = %key,
                state_size = state.as_ref().len(),
                state_hash = %hash,
                phase = "upsert_start",
                "Upserting contract state"
            );
        }
        let params = if let Some(code) = &code {
            code.params()
        } else {
            // Contract not provided, need to get params from state_store
            self.state_store
                .get_params(&key)
                .await
                .map_err(ExecutorError::other)?
                .ok_or_else(|| {
                    // This error occurs when an UPDATE arrives for a contract whose
                    // parameters haven't been stored yet (race condition)
                    tracing::warn!(
                        contract = %key,
                        is_delta = matches!(update, Either::Right(_)),
                        "Contract parameters not found in state_store"
                    );
                    ExecutorError::request(StdContractError::Put {
                        key,
                        cause: "missing contract parameters".into(),
                    })
                })?
        };

        // Track if we stored a new contract
        let (remove_if_fail, contract_was_provided) = if self
            .runtime
            .contract_store
            .fetch_contract(&key, &params)
            .is_none()
        {
            if let Some(ref contract_code) = code {
                tracing::debug!(
                    contract = %key,
                    phase = "store_contract",
                    "Storing new contract"
                );

                self.runtime
                    .contract_store
                    .store_contract(contract_code.clone())
                    .map_err(ExecutorError::other)?;
                (true, true)
            } else {
                // Bug #2306: This should never happen for PUT operations because they
                // always provide the contract code. If we hit this path during a PUT,
                // it indicates the contract code was lost somewhere in the flow.
                tracing::error!(
                    contract = %key,
                    key_code_hash = ?key.code_hash(),
                    code_provided = code.is_some(),
                    is_delta = matches!(update, Either::Right(_)),
                    phase = "missing_contract_error",
                    "Contract not in store and no code provided - this is a bug if this is a PUT operation"
                );
                return Err(ExecutorError::request(StdContractError::MissingContract {
                    key: key.into(),
                }));
            }
        } else {
            // fetch_contract succeeded - the contract code is already cached.
            // However, we still need to ensure the key_to_code_part mapping exists
            // for THIS specific ContractInstanceId. This is critical for contracts
            // that reuse the same WASM code with different parameters (e.g., different
            // River rooms). Without this, lookup_key() fails for the new instance_id.
            // See issue #2380.
            //
            // We only index when code was provided in this request (code.is_some()).
            // When code is None, this is a state-only update to an existing contract
            // that should already be indexed.
            if code.is_some() {
                self.runtime
                    .contract_store
                    .ensure_key_indexed(&key)
                    .map_err(ExecutorError::other)?;
            }
            (false, code.is_some())
        };

        let is_new_contract = self.state_store.get(&key).await.is_err();

        // If this is a new contract being stored, mark it as initializing
        if remove_if_fail && is_new_contract && contract_was_provided {
            tracing::debug!(
                contract = %key,
                "Starting contract initialization - queueing subsequent operations"
            );
            self.init_tracker.start_initialization(key);
        }

        let mut updates = match update {
            Either::Left(incoming_state) => {
                let result = self
                    .runtime
                    .validate_state(&key, &params, &incoming_state, &related_contracts)
                    .map_err(|err| {
                        if remove_if_fail {
                            let _ = self.runtime.contract_store.remove_contract(&key);
                        }
                        ExecutorError::execution(err, None)
                    })?;
                match result {
                    ValidateResult::Valid => {
                        tracing::debug!(
                            contract = %key,
                            phase = "validation_complete",
                            "Incoming state is valid"
                        );

                        // If the contract is new, we store the incoming state as the initial state avoiding the update
                        if is_new_contract {
                            tracing::debug!(
                                contract = %key,
                                phase = "store_initial_state",
                                "Contract is new, storing initial state"
                            );
                            let state_to_store = incoming_state.clone();
                            self.state_store
                                .store(key, state_to_store, params.clone())
                                .await
                                .map_err(ExecutorError::other)?;

                            // Contract initialization complete - mark as ready and get queued ops
                            if let Some(completion_info) =
                                self.init_tracker.complete_initialization(&key)
                            {
                                let init_duration = completion_info.init_duration;
                                if init_duration > SLOW_INIT_THRESHOLD {
                                    tracing::warn!(
                                        contract = %key,
                                        queued_operations = completion_info.queued_ops.len(),
                                        init_duration_ms = init_duration.as_millis(),
                                        threshold_ms = SLOW_INIT_THRESHOLD.as_millis(),
                                        "Contract initialization took longer than expected"
                                    );
                                } else {
                                    tracing::info!(
                                        contract = %key,
                                        queued_operations = completion_info.queued_ops.len(),
                                        init_duration_ms = init_duration.as_millis(),
                                        "Contract initialization complete"
                                    );
                                }

                                // Log details about queued operations
                                for op in &completion_info.queued_ops {
                                    let queue_time = op.queued_at.elapsed();
                                    tracing::info!(
                                        contract = %key,
                                        queue_time_ms = queue_time.as_millis(),
                                        is_delta = matches!(op.update, Either::Right(_)),
                                        has_related = op.related_contracts.states().next().is_some(),
                                        "Queued operation ready for retry after initialization"
                                    );
                                }
                                // The queued operations will be handled when the sender retries them
                                // Now that initialization is complete, they will succeed
                            }

                            return Ok(UpsertResult::Updated(incoming_state));
                        }
                    }
                    ValidateResult::Invalid => {
                        // Validation failed - clear any queued operations
                        if let Some(dropped_count) = self.init_tracker.fail_initialization(&key) {
                            tracing::warn!(
                                contract = %key,
                                dropped_operations = dropped_count,
                                "Contract validation failed, dropping queued operations"
                            );
                        }
                        return Err(ExecutorError::request(StdContractError::invalid_put(key)));
                    }
                    ValidateResult::RequestRelated(mut related) => {
                        // Clear any queued operations since we're missing related contracts
                        if let Some(dropped_count) = self.init_tracker.fail_initialization(&key) {
                            tracing::warn!(
                                contract = %key,
                                dropped_operations = dropped_count,
                                "Missing related contracts, dropping queued operations"
                            );
                        }
                        if let Some(key) = related.pop() {
                            return Err(ExecutorError::request(StdContractError::MissingRelated {
                                key,
                            }));
                        } else {
                            return Err(ExecutorError::internal_error());
                        }
                    }
                }

                vec![UpdateData::State(incoming_state.clone().into())]
            }
            Either::Right(delta) => {
                vec![UpdateData::Delta(delta)]
            }
        };

        let current_state = match self.state_store.get(&key).await {
            Ok(s) => s,
            Err(StateStoreError::MissingContract(_)) => {
                tracing::warn!(
                    contract = %key,
                    phase = "upsert_failed",
                    "Missing contract for upsert"
                );
                return Err(ExecutorError::request(StdContractError::MissingContract {
                    key: key.into(),
                }));
            }
            Err(StateStoreError::Any(err)) => return Err(ExecutorError::other(err)),
        };

        for (id, state) in related_contracts
            .states()
            .filter_map(|(id, c)| c.as_ref().map(|c| (id, c)))
        {
            updates.push(UpdateData::RelatedState {
                related_to: *id,
                state: state.clone(),
            });
        }

        let updated_state = match self
            .attempt_state_update(&params, &current_state, &key, &updates)
            .await?
        {
            Either::Left(s) => s,
            Either::Right(mut r) => {
                let Some(c) = r.pop() else {
                    // this branch should be unreachable since attempt_state_update should only
                    return Err(ExecutorError::internal_error());
                };
                return Err(ExecutorError::request(StdContractError::MissingRelated {
                    key: c.contract_instance_id,
                }));
            }
        };
        match self
            .runtime
            .validate_state(&key, &params, &updated_state, &related_contracts)
            .map_err(|e| ExecutorError::execution(e, None))?
        {
            ValidateResult::Valid => {
                if updated_state.as_ref() == current_state.as_ref() {
                    Ok(UpsertResult::NoChange)
                } else {
                    // Persist the updated state before returning
                    self.state_store
                        .update(&key, updated_state.clone())
                        .await
                        .map_err(ExecutorError::other)?;

                    // todo: forward delta like we are doing with puts
                    tracing::warn!(
                        contract = %key,
                        "Delta updates are not yet supported"
                    );
                    Ok(UpsertResult::Updated(updated_state))
                }
            }
            ValidateResult::Invalid => Err(ExecutorError::request(
                freenet_stdlib::client_api::ContractError::Update {
                    key,
                    cause: "invalid outcome state".into(),
                },
            )),
            ValidateResult::RequestRelated(_) => todo!(),
        }
    }

    fn register_contract_notifier(
        &mut self,
        instance_id: ContractInstanceId,
        cli_id: ClientId,
        notification_ch: tokio::sync::mpsc::UnboundedSender<HostResult>,
        summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>> {
        let channels = self.update_notifications.entry(instance_id).or_default();
        if let Ok(i) = channels.binary_search_by_key(&&cli_id, |(p, _)| p) {
            let (_, existing_ch) = &channels[i];
            if !existing_ch.same_channel(&notification_ch) {
                // Client is already subscribed with a different channel - update to the new channel
                // This can happen when a client reconnects
                tracing::info!(
                    contract = %instance_id,
                    client = %cli_id,
                    "Client already subscribed, updating notification channel"
                );
                channels[i] = (cli_id, notification_ch);
            }
        } else {
            channels.push((cli_id, notification_ch));
        }

        if self
            .subscriber_summaries
            .entry(instance_id)
            .or_default()
            .insert(cli_id, summary.map(StateSummary::into_owned))
            .is_some()
        {
            tracing::debug!(
                contract = %instance_id,
                client = %cli_id,
                "Contract already registered for client, replaced summary"
            );
        }
        Ok(())
    }

    fn execute_delegate_request(
        &mut self,
        req: DelegateRequest<'_>,
        attested_contract: Option<&ContractInstanceId>,
    ) -> Response {
        tracing::debug!(
            attested_contract = ?attested_contract,
            "received delegate request"
        );
        match req {
            DelegateRequest::RegisterDelegate {
                delegate,
                cipher,
                nonce,
            } => {
                use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
                let key = delegate.key().clone();
                let arr = (&cipher).into();
                let cipher = XChaCha20Poly1305::new(arr);
                let nonce = nonce.into();
                if let Some(contract) = attested_contract {
                    self.delegate_attested_ids
                        .entry(key.clone())
                        .or_default()
                        .push(*contract);
                }
                match self.runtime.register_delegate(delegate, cipher, nonce) {
                    Ok(_) => Ok(DelegateResponse {
                        key,
                        values: Vec::new(),
                    }),
                    Err(err) => {
                        tracing::warn!(
                            delegate_key = %key,
                            error = %err,
                            phase = "register_failed",
                            "Failed to register delegate"
                        );
                        Err(ExecutorError::other(StdDelegateError::RegisterError(key)))
                    }
                }
            }
            DelegateRequest::UnregisterDelegate(key) => {
                self.delegate_attested_ids.remove(&key);
                match self.runtime.unregister_delegate(&key) {
                    Ok(_) => Ok(HostResponse::Ok),
                    Err(err) => {
                        tracing::warn!(
                            delegate_key = %key,
                            error = %err,
                            phase = "unregister_failed",
                            "Failed to unregister delegate"
                        );
                        Ok(HostResponse::Ok)
                    }
                }
            }
            DelegateRequest::GetSecretRequest {
                key,
                params,
                get_request,
            } => {
                tracing::debug!(
                    delegate_key = %key,
                    params_size = params.as_ref().len(),
                    attested_contract = ?attested_contract,
                    "Handling GetSecretRequest for delegate"
                );
                let attested = attested_contract.and_then(|contract| {
                    self.delegate_attested_ids
                        .get(&key)
                        .and_then(|contracts| contracts.iter().find(|c| *c == contract))
                });
                match self.runtime.inbound_app_message(
                    &key,
                    &params,
                    attested.map(|c| c.as_bytes()),
                    vec![InboundDelegateMsg::GetSecretRequest(get_request)],
                ) {
                    Ok(values) => Ok(DelegateResponse { key, values }),
                    Err(err) => Err(ExecutorError::execution(
                        err,
                        Some(InnerOpError::Delegate(key.clone())),
                    )),
                }
            }
            DelegateRequest::ApplicationMessages {
                key,
                inbound,
                params,
            } => {
                // Use the attested_contract directly instead of looking it up in delegate_attested_ids
                let attested_bytes = attested_contract.map(|c| c.as_bytes());
                match self.runtime.inbound_app_message(
                    &key,
                    &params,
                    attested_bytes,
                    inbound
                        .into_iter()
                        .map(InboundDelegateMsg::into_owned)
                        .collect(),
                ) {
                    Ok(values) => Ok(DelegateResponse { key, values }),
                    Err(err) => {
                        tracing::error!(
                            delegate_key = %key,
                            error = %err,
                            phase = "execution_failed",
                            "Failed executing delegate"
                        );
                        Err(ExecutorError::execution(
                            err,
                            Some(InnerOpError::Delegate(key)),
                        ))
                    }
                }
            }
            _ => Err(ExecutorError::other(anyhow::anyhow!("not supported"))),
        }
    }

    fn get_subscription_info(&self) -> Vec<crate::message::SubscriptionInfo> {
        self.get_subscription_info()
    }
}

impl Executor<Runtime> {
    // Private implementation methods
}

impl Executor<Runtime> {
    /// Create an Executor for local-only mode (no network operations).
    /// Use this from the binary for local mode execution.
    pub async fn from_config_local(config: Arc<Config>) -> anyhow::Result<Self> {
        Self::from_config(config, None, None).await
    }

    /// Create an Executor with optional network operation support.
    /// This is `pub(crate)` because the parameters involve crate-internal types.
    pub(crate) async fn from_config(
        config: Arc<Config>,
        op_sender: Option<OpRequestSender>,
        op_manager: Option<Arc<OpManager>>,
    ) -> anyhow::Result<Self> {
        let (contract_store, delegate_store, secret_store, state_store) =
            Self::get_stores(&config).await?;
        let rt = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();
        Executor::new(
            state_store,
            move || {
                let _ =
                    crate::util::set_cleanup_on_exit(config.paths().clone()).inspect_err(|error| {
                        tracing::error!("Failed to set cleanup on exit: {error}");
                    });
                Ok(())
            },
            OperationMode::Local,
            rt,
            op_sender,
            op_manager,
        )
        .await
    }

    pub async fn preload(
        &mut self,
        cli_id: ClientId,
        contract: ContractContainer,
        state: WrappedState,
        related_contracts: RelatedContracts<'static>,
    ) {
        if let Err(err) = self
            .contract_requests(
                ContractRequest::Put {
                    contract,
                    state,
                    related_contracts,
                    subscribe: false,
                },
                cli_id,
                None,
            )
            .await
        {
            match err.inner {
                Either::Left(err) => tracing::error!("req error: {err}"),
                Either::Right(err) => tracing::error!("other error: {err}"),
            }
        }
    }

    pub async fn handle_request(
        &mut self,
        id: ClientId,
        req: ClientRequest<'_>,
        updates: Option<mpsc::UnboundedSender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        match req {
            ClientRequest::ContractOp(op) => self.contract_requests(op, id, updates).await,
            ClientRequest::DelegateOp(op) => self.delegate_request(op, None),
            ClientRequest::Disconnect { cause } => {
                if let Some(cause) = cause {
                    tracing::info!("disconnecting cause: {cause}");
                }
                Err(RequestError::Disconnect.into())
            }
            other => {
                tracing::warn!(
                    client = %id,
                    request = ?other,
                    "unsupported client request"
                );
                Err(ExecutorError::other(anyhow::anyhow!("not supported")))
            }
        }
    }

    /// Respond to requests made through any API's from client applications in local mode.
    pub async fn contract_requests(
        &mut self,
        req: ContractRequest<'_>,
        cli_id: ClientId,
        updates: Option<mpsc::UnboundedSender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        tracing::debug!(
            client = %cli_id,
            "received contract request"
        );
        let result = match req {
            ContractRequest::Put {
                contract,
                state,
                related_contracts,
                ..
            } => {
                tracing::debug!(
                    client = %cli_id,
                    contract = %contract.key(),
                    state_size = state.as_ref().len(),
                    "putting contract"
                );
                self.perform_contract_put(contract, state, related_contracts)
                    .await
            }
            ContractRequest::Update { key, data } => self.perform_contract_update(key, data).await,
            // Handle Get requests by returning the contract state and optionally the contract code
            ContractRequest::Get {
                key: instance_id,
                return_contract_code,
                ..
            } => {
                // Look up the full key from the instance_id
                let full_key = self.lookup_key(&instance_id).ok_or_else(|| {
                    tracing::debug!(
                        contract = %instance_id,
                        phase = "key_lookup_failed",
                        "Contract not found during get request"
                    );
                    ExecutorError::request(StdContractError::MissingContract { key: instance_id })
                })?;

                match self
                    .perform_contract_get(return_contract_code, full_key)
                    .await
                {
                    Ok((state, contract)) => Ok(ContractResponse::GetResponse {
                        key: full_key,
                        state: state.ok_or_else(|| {
                            tracing::debug!(
                                contract = %full_key,
                                phase = "get_failed",
                                "Contract state not found during get request"
                            );
                            ExecutorError::request(StdContractError::Get {
                                key: full_key,
                                cause: "contract state not found".into(),
                            })
                        })?,
                        contract,
                    }
                    .into()),
                    Err(err) => Err(err),
                }
            }
            ContractRequest::Subscribe {
                key: instance_id,
                summary,
            } => {
                tracing::debug!(
                    client = %cli_id,
                    contract = %instance_id,
                    has_summary = summary.is_some(),
                    "subscribing to contract"
                );
                let updates = updates.ok_or_else(|| {
                    ExecutorError::other(anyhow::anyhow!("missing update channel"))
                })?;
                self.register_contract_notifier(instance_id, cli_id, updates, summary)?;

                // Look up the full key for storage operations
                let full_key = self.lookup_key(&instance_id).ok_or_else(|| {
                    tracing::debug!(
                        contract = %instance_id,
                        phase = "key_lookup_failed",
                        "Contract not found during subscribe request"
                    );
                    ExecutorError::request(StdContractError::MissingContract { key: instance_id })
                })?;

                // by default a subscribe op has an implicit get
                let _res = self.perform_contract_get(false, full_key).await?;
                self.subscribe(full_key).await?;
                Ok(ContractResponse::SubscribeResponse {
                    key: full_key,
                    subscribed: true,
                }
                .into())
            }
            other => {
                tracing::warn!(
                    client = %cli_id,
                    request = ?other,
                    "unsupported contract request"
                );
                Err(ExecutorError::other(anyhow::anyhow!("not supported")))
            }
        };

        if let Err(ref e) = result {
            tracing::error!(
                client = %cli_id,
                error = %e,
                phase = "request_failed",
                "Contract request failed"
            );
        }

        result
    }

    pub fn delegate_request(
        &mut self,
        req: DelegateRequest<'_>,
        attested_contract: Option<&ContractInstanceId>,
    ) -> Response {
        tracing::debug!(
            attested_contract = ?attested_contract,
            "received delegate request"
        );
        match req {
            DelegateRequest::RegisterDelegate {
                delegate,
                cipher,
                nonce,
            } => {
                use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
                let key = delegate.key().clone();
                let arr = (&cipher).into();
                let cipher = XChaCha20Poly1305::new(arr);
                let nonce = nonce.into();
                if let Some(contract) = attested_contract {
                    self.delegate_attested_ids
                        .entry(key.clone())
                        .or_default()
                        .push(*contract);
                }
                match self.runtime.register_delegate(delegate, cipher, nonce) {
                    Ok(_) => Ok(DelegateResponse {
                        key,
                        values: Vec::new(),
                    }),
                    Err(err) => {
                        tracing::warn!(
                            delegate_key = %key,
                            error = %err,
                            phase = "register_failed",
                            "Failed to register delegate"
                        );
                        Err(ExecutorError::other(StdDelegateError::RegisterError(key)))
                    }
                }
            }
            DelegateRequest::UnregisterDelegate(key) => {
                self.delegate_attested_ids.remove(&key);
                match self.runtime.unregister_delegate(&key) {
                    Ok(_) => Ok(HostResponse::Ok),
                    Err(err) => {
                        tracing::warn!(
                            delegate_key = %key,
                            error = %err,
                            phase = "unregister_failed",
                            "Failed to unregister delegate"
                        );
                        Ok(HostResponse::Ok)
                    }
                }
            }
            DelegateRequest::GetSecretRequest {
                key,
                params,
                get_request,
            } => {
                tracing::debug!(
                    delegate_key = %key,
                    params_size = params.as_ref().len(),
                    attested_contract = ?attested_contract,
                    "Handling GetSecretRequest for delegate"
                );
                let attested = attested_contract.and_then(|contract| {
                    self.delegate_attested_ids
                        .get(&key)
                        .and_then(|contracts| contracts.iter().find(|c| *c == contract))
                });
                match self.runtime.inbound_app_message(
                    &key,
                    &params,
                    attested.map(|c| c.as_bytes()),
                    vec![InboundDelegateMsg::GetSecretRequest(get_request)],
                ) {
                    Ok(values) => Ok(DelegateResponse { key, values }),
                    Err(err) => Err(ExecutorError::execution(
                        err,
                        Some(InnerOpError::Delegate(key.clone())),
                    )),
                }
            }
            DelegateRequest::ApplicationMessages {
                key,
                inbound,
                params,
            } => {
                // Use the attested_contract directly instead of looking it up in delegate_attested_ids
                let attested_bytes = attested_contract.map(|c| c.as_bytes());
                match self.runtime.inbound_app_message(
                    &key,
                    &params,
                    attested_bytes,
                    inbound
                        .into_iter()
                        .map(InboundDelegateMsg::into_owned)
                        .collect(),
                ) {
                    Ok(values) => Ok(DelegateResponse { key, values }),
                    Err(err) => {
                        tracing::error!(
                            delegate_key = %key,
                            error = %err,
                            phase = "execution_failed",
                            "Failed executing delegate"
                        );
                        Err(ExecutorError::execution(
                            err,
                            Some(InnerOpError::Delegate(key)),
                        ))
                    }
                }
            }
            _ => Err(ExecutorError::other(anyhow::anyhow!("not supported"))),
        }
    }

    async fn perform_contract_put(
        &mut self,
        contract: ContractContainer,
        state: WrappedState,
        related_contracts: RelatedContracts<'_>,
    ) -> Response {
        let key = contract.key();
        let params = contract.params();

        if self.get_local_contract(key.id()).await.is_ok() {
            // already existing contract, just try to merge states
            return self
                .perform_contract_update(key, UpdateData::State(state.into()))
                .await;
        }

        self.verify_and_store_contract(state.clone(), contract, related_contracts)
            .await?;

        self.send_update_notification(&key, &params, &state)
            .await
            .map_err(|_| {
                ExecutorError::request(StdContractError::Put {
                    key,
                    cause: "failed while sending notifications".into(),
                })
            })?;
        Ok(ContractResponse::PutResponse { key }.into())
    }

    async fn perform_contract_update(
        &mut self,
        key: ContractKey,
        update: UpdateData<'_>,
    ) -> Response {
        let parameters = {
            self.state_store
                .get_params(&key)
                .await
                .map_err(ExecutorError::other)?
                .ok_or_else(|| {
                    RequestError::ContractError(StdContractError::Update {
                        cause: "missing contract parameters".into(),
                        key,
                    })
                })?
        };

        let current_state = self
            .state_store
            .get(&key)
            .await
            .map_err(ExecutorError::other)?
            .clone();

        let updates = vec![update];

        // In local mode, we handle the full update locally (compute, commit, notify)
        if self.mode == OperationMode::Local {
            let new_state = self
                .get_updated_state(&parameters, current_state, key, updates)
                .await?;
            let summary = self
                .runtime
                .summarize_state(&key, &parameters, &new_state)
                .map_err(|e| ExecutorError::execution(e, None))?;
            // Note: notification is sent by attempt_state_update, no need to send again
            return Ok(ContractResponse::UpdateResponse { key, summary }.into());
        }

        // In network mode, compute the state WITHOUT committing.
        // The network operation will handle the commit and broadcast.
        // This fixes issue #2301: previously we committed here, causing the network
        // operation to see no change and skip broadcasting.
        //
        // If related contracts are needed, we fetch them and retry compute_state_update
        // in a loop (similar to get_updated_state, but without committing).
        let mut updates = updates;
        let start = Instant::now();
        let new_state = loop {
            let computed = self
                .compute_state_update(&parameters, &current_state, &key, &updates)
                .await?;

            match computed {
                ComputedStateUpdate::NoChange(state) => {
                    // No change detected, return early without starting network operation
                    tracing::debug!(
                        contract = %key,
                        phase = "update_no_change",
                        "Update resulted in no change, skipping network operation"
                    );
                    let summary = self
                        .runtime
                        .summarize_state(&key, &parameters, &state)
                        .map_err(|e| ExecutorError::execution(e, None))?;
                    return Ok(ContractResponse::UpdateResponse { key, summary }.into());
                }
                ComputedStateUpdate::MissingRelated(missing) => {
                    // Fetch missing related contracts WITHOUT committing.
                    // This mirrors the logic in get_updated_state but avoids the commit
                    // that would cause the network operation to see NoChange.
                    tracing::debug!(
                        contract = %key,
                        missing_count = missing.len(),
                        phase = "update_fetching_related",
                        "Fetching missing related contracts"
                    );

                    let required_contracts = missing.len() + 1;
                    for RelatedContract {
                        contract_instance_id: id,
                        mode,
                    } in missing
                    {
                        // Try to look up the full key; if not found, treat as missing
                        let local_state = if let Some(related_key) = self.lookup_key(&id) {
                            self.state_store.get(&related_key).await.ok()
                        } else {
                            None
                        };

                        match local_state {
                            Some(state) => {
                                // Already have this contract locally
                                updates.push(UpdateData::RelatedState {
                                    related_to: id,
                                    state: state.into(),
                                });
                            }
                            None => {
                                // Fetch from network
                                let state =
                                    match self.local_state_or_from_network(&id, false).await? {
                                        Either::Left(state) => state,
                                        Either::Right(GetResult {
                                            state, contract, ..
                                        }) => {
                                            let Some(contract) = contract else {
                                                return Err(ExecutorError::request(
                                                    RequestError::ContractError(
                                                        StdContractError::Get {
                                                            key,
                                                            cause: "Missing contract".into(),
                                                        },
                                                    ),
                                                ));
                                            };
                                            // Store the related contract (this is necessary for future lookups)
                                            // but does NOT commit the main contract's state update
                                            self.verify_and_store_contract(
                                                state.clone(),
                                                contract.clone(),
                                                RelatedContracts::default(),
                                            )
                                            .await?;
                                            state
                                        }
                                    };
                                updates.push(UpdateData::State(state.into()));
                                match mode {
                                    RelatedMode::StateOnce => {}
                                    RelatedMode::StateThenSubscribe => {
                                        // After storing, we should be able to look up the key
                                        if let Some(related_key) = self.lookup_key(&id) {
                                            self.subscribe(related_key).await?;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Check if we have enough related contracts to retry
                    if updates.len() + 1 >= required_contracts {
                        // Retry with related contracts
                        continue;
                    } else if start.elapsed() > Duration::from_secs(10) {
                        tracing::error!(
                            contract = %key,
                            elapsed_secs = start.elapsed().as_secs(),
                            phase = "update_timeout",
                            "Timeout fetching related contracts for update"
                        );
                        return Err(ExecutorError::request(RequestError::Timeout));
                    }
                }
                ComputedStateUpdate::Changed(new_state) => {
                    break new_state;
                }
            }
        };

        // State changed - start network operation which will commit and broadcast.
        // We pass the computed new_state so the network operation can detect the change.
        //
        // Notification flow in this path:
        // 1. compute_state_update does NOT send notifications (by design)
        // 2. Network operation calls update_contract -> UpdateQuery -> upsert_contract_state
        // 3. upsert_contract_state -> attempt_state_update sends the notification
        tracing::debug!(
            contract = %key,
            new_size_bytes = new_state.as_ref().len(),
            phase = "update_starting_network_op",
            "State changed, starting network operation for commit and broadcast"
        );
        let summary = self
            .runtime
            .summarize_state(&key, &parameters, &new_state)
            .map_err(|e| ExecutorError::execution(e, None))?;
        let request = UpdateContract { key, new_state };
        let _op: operations::update::UpdateResult = self.op_request(request).await?;
        Ok(ContractResponse::UpdateResponse { key, summary }.into())
    }

    /// Computes the updated state WITHOUT committing it to storage.
    ///
    /// This is used in network mode to prepare the state for the network operation
    /// which will handle the commit. This separation fixes issue #2301 where
    /// committing before the network operation caused change detection to fail.
    async fn compute_state_update(
        &mut self,
        parameters: &Parameters<'_>,
        current_state: &WrappedState,
        key: &ContractKey,
        updates: &[UpdateData<'_>],
    ) -> Result<ComputedStateUpdate, ExecutorError> {
        let update_modification =
            match self
                .runtime
                .update_state(key, parameters, current_state, updates)
            {
                Ok(result) => result,
                Err(err) => {
                    return Err(ExecutorError::execution(
                        err,
                        Some(InnerOpError::Upsert(*key)),
                    ))
                }
            };

        let UpdateModification {
            new_state, related, ..
        } = update_modification;

        let Some(new_state) = new_state else {
            if related.is_empty() {
                // No updates were made, return current state
                return Ok(ComputedStateUpdate::NoChange(current_state.clone()));
            } else {
                // Missing related contracts
                return Ok(ComputedStateUpdate::MissingRelated(related));
            }
        };

        let new_state = WrappedState::new(new_state.into_bytes());

        // Compare bytes to determine if state actually changed.
        // Note: Even though runtime.update_state returned Some(new_state), we still check bytes
        // because the contract may have "processed" the update but produced identical output
        // (e.g., idempotent merge operations in CRDTs). In such cases, we should NOT broadcast
        // since nothing actually changed from the subscribers' perspective.
        let changed = new_state.as_ref() != current_state.as_ref();

        if changed {
            Ok(ComputedStateUpdate::Changed(new_state))
        } else {
            Ok(ComputedStateUpdate::NoChange(current_state.clone()))
        }
    }

    /// Attempts to update the state with the provided updates.
    /// If there were no updates, it will return the current state.
    async fn attempt_state_update(
        &mut self,
        parameters: &Parameters<'_>,
        current_state: &WrappedState,
        key: &ContractKey,
        updates: &[UpdateData<'_>],
    ) -> Result<Either<WrappedState, Vec<RelatedContract>>, ExecutorError> {
        let update_modification =
            match self
                .runtime
                .update_state(key, parameters, current_state, updates)
            {
                Ok(result) => result,
                Err(err) => {
                    return Err(ExecutorError::execution(
                        err,
                        Some(InnerOpError::Upsert(*key)),
                    ))
                }
            };
        let UpdateModification {
            new_state, related, ..
        } = update_modification;
        let Some(new_state) = new_state else {
            if related.is_empty() {
                // no updates were made, just return old state
                return Ok(Either::Left(current_state.clone()));
            } else {
                return Ok(Either::Right(related));
            }
        };
        let new_state = WrappedState::new(new_state.into_bytes());

        if new_state.as_ref() == current_state.as_ref() {
            tracing::debug!(
                contract = %key,
                phase = "update_skipped",
                "No changes in state, avoiding update"
            );
            return Ok(Either::Left(current_state.clone()));
        }

        self.state_store
            .update(key, new_state.clone())
            .await
            .map_err(ExecutorError::other)?;

        tracing::info!(
            contract = %key,
            new_size_bytes = new_state.as_ref().len(),
            phase = "update_complete",
            "Contract state updated"
        );

        if let Err(err) = self
            .send_update_notification(key, parameters, &new_state)
            .await
        {
            tracing::error!(
                contract = %key,
                error = %err,
                phase = "notification_failed",
                "Failed to send update notification"
            );
        }
        Ok(Either::Left(new_state))
    }

    /// Given a contract and a series of delta updates, it will try to perform an update
    /// to the contract state and return the new state. If it fails to update the state,
    /// it will return an error.
    ///
    /// If there are missing updates for related contracts, it will try to fetch them from the network.
    async fn get_updated_state(
        &mut self,
        parameters: &Parameters<'_>,
        current_state: WrappedState,
        key: ContractKey,
        mut updates: Vec<UpdateData<'_>>,
    ) -> Result<WrappedState, ExecutorError> {
        let new_state = {
            let start = Instant::now();
            loop {
                let state_update_res = self
                    .attempt_state_update(parameters, &current_state, &key, &updates)
                    .await?;
                let missing = match state_update_res {
                    Either::Left(new_state) => {
                        // Note: attempt_state_update already commits the state to storage,
                        // so we don't need to call state_store.update again here.
                        break new_state;
                    }
                    Either::Right(missing) => missing,
                };
                // some required contracts are missing
                let required_contracts = missing.len() + 1;
                for RelatedContract {
                    contract_instance_id: id,
                    mode,
                } in missing
                {
                    // Try to look up the full key; if not found, treat as missing
                    let local_state = if let Some(related_key) = self.lookup_key(&id) {
                        self.state_store.get(&related_key).await.ok()
                    } else {
                        None
                    };

                    match local_state {
                        Some(state) => {
                            // in this case we are already subscribed to and are updating this contract,
                            // we can try first with the existing value
                            updates.push(UpdateData::RelatedState {
                                related_to: id,
                                state: state.into(),
                            });
                        }
                        None => {
                            let state = match self.local_state_or_from_network(&id, false).await? {
                                Either::Left(state) => state,
                                Either::Right(GetResult {
                                    state, contract, ..
                                }) => {
                                    let Some(contract) = contract else {
                                        return Err(ExecutorError::request(
                                            RequestError::ContractError(StdContractError::Get {
                                                key,
                                                cause: "Missing contract".into(),
                                            }),
                                        ));
                                    };
                                    self.verify_and_store_contract(
                                        state.clone(),
                                        contract.clone(),
                                        RelatedContracts::default(),
                                    )
                                    .await?;
                                    state
                                }
                            };
                            updates.push(UpdateData::State(state.into()));
                            match mode {
                                RelatedMode::StateOnce => {}
                                RelatedMode::StateThenSubscribe => {
                                    // After storing, we should be able to look up the key
                                    if let Some(related_key) = self.lookup_key(&id) {
                                        self.subscribe(related_key).await?;
                                    }
                                }
                            }
                        }
                    }
                }
                if updates.len() + 1 /* includes the original contract being updated update */ >= required_contracts
                {
                    // try running again with all the related contracts retrieved
                    continue;
                } else if start.elapsed() > Duration::from_secs(10) {
                    /* make this timeout configurable, and anyway should be controlled globally*/
                    return Err(RequestError::Timeout.into());
                }
            }
        };
        Ok(new_state)
    }

    async fn perform_contract_get(
        &mut self,
        return_contract_code: bool,
        key: ContractKey,
    ) -> Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError> {
        tracing::debug!(
            contract = %key,
            return_code = return_contract_code,
            "Getting contract"
        );
        let mut got_contract: Option<ContractContainer> = None;

        if return_contract_code {
            if let Some(contract) = self.get_contract_locally(&key).await? {
                got_contract = Some(contract);
            }
        }

        let state_result = self.state_store.get(&key).await;
        tracing::debug!(
            contract = %key,
            state_found = state_result.is_ok(),
            has_contract = got_contract.is_some(),
            "Contract get result"
        );
        match state_result {
            Ok(state) => Ok((Some(state), got_contract)),
            Err(StateStoreError::MissingContract(_)) => {
                tracing::warn!(contract = %key, "Contract state not found in store");
                Ok((None, got_contract))
            }
            Err(err) => {
                tracing::error!(contract = %key, error = %err, "Failed to get contract state");
                Err(ExecutorError::request(RequestError::from(
                    StdContractError::Get {
                        key,
                        cause: format!("{err}").into(),
                    },
                )))
            }
        }
    }

    async fn get_local_contract(
        &self,
        id: &ContractInstanceId,
    ) -> Result<State<'static>, Either<Box<RequestError>, anyhow::Error>> {
        let Some(full_key) = self.lookup_key(id) else {
            return Err(Either::Right(
                StdContractError::MissingRelated { key: *id }.into(),
            ));
        };
        let Ok(contract) = self.state_store.get(&full_key).await else {
            return Err(Either::Right(
                StdContractError::MissingRelated { key: *id }.into(),
            ));
        };
        let state: &[u8] = unsafe {
            // Safety: this is fine since this will never scape this scope
            std::mem::transmute::<&[u8], &'_ [u8]>(contract.as_ref())
        };
        Ok(State::from(state))
    }

    async fn verify_and_store_contract(
        &mut self,
        state: WrappedState,
        trying_container: ContractContainer,
        mut related_contracts: RelatedContracts<'_>,
    ) -> Result<(), ExecutorError> {
        let key = trying_container.key();
        let params = trying_container.params();
        let state_hash = blake3::hash(state.as_ref());

        tracing::debug!(
            contract = %key,
            state_size = state.as_ref().len(),
            state_hash = %state_hash,
            params_size = params.as_ref().len(),
            "starting contract verification and storage"
        );

        const DEPENDENCY_CYCLE_LIMIT_GUARD: usize = 100;
        let mut iterations = 0;

        let original_key = key;
        let original_state = state.clone();
        let original_params = params.clone();
        let mut trying_key = key;
        let mut trying_state = state;
        let mut trying_params = params;
        let mut trying_contract = Some(trying_container);

        while iterations < DEPENDENCY_CYCLE_LIMIT_GUARD {
            if let Some(contract) = trying_contract.take() {
                tracing::debug!(
                    contract = %trying_key,
                    "storing contract in runtime store"
                );
                // DEBUG: Log contract details before storing
                tracing::debug!(
                    "DEBUG PUT: verify_and_store_contract - key={}, key.code_hash={:?}, contract.key={}, contract.key.code_hash={:?}",
                    key,
                    key.code_hash(),
                    contract.key(),
                    contract.key().code_hash()
                );

                self.runtime
                    .contract_store
                    .store_contract(contract)
                    .map_err(|e| {
                        tracing::error!(
                            contract = %trying_key,
                            error = %e,
                            "failed to store contract in runtime"
                        );
                        ExecutorError::other(e)
                    })?;
            }

            let result = self
                .runtime
                .validate_state(
                    &trying_key,
                    &trying_params,
                    &trying_state,
                    &related_contracts,
                )
                .map_err(|err| {
                    let _ = self.runtime.contract_store.remove_contract(&trying_key);
                    ExecutorError::execution(err, None)
                })?;

            let is_valid = match result {
                ValidateResult::Valid => true,
                ValidateResult::Invalid => false,
                ValidateResult::RequestRelated(related) => {
                    iterations += 1;
                    related_contracts.missing(related);
                    for (id, related) in related_contracts.update() {
                        if related.is_none() {
                            match self.local_state_or_from_network(id, false).await? {
                                Either::Left(state) => {
                                    *related = Some(state.into());
                                }
                                Either::Right(result) => {
                                    let Some(contract) = result.contract else {
                                        return Err(ExecutorError::request(
                                            RequestError::ContractError(
                                                StdContractError::MissingRelated { key: *id },
                                            ),
                                        ));
                                    };
                                    trying_key = contract.key();
                                    trying_params = contract.params();
                                    trying_state = result.state;
                                    trying_contract = Some(contract);
                                    continue;
                                }
                            }
                        }
                    }
                    continue;
                }
            };

            if !is_valid {
                return Err(ExecutorError::request(StdContractError::Put {
                    key: trying_key,
                    cause: "not valid".into(),
                }));
            }

            tracing::debug!(
                contract = %trying_key,
                state_size = trying_state.as_ref().len(),
                "storing contract state"
            );
            self.state_store
                .store(trying_key, trying_state.clone(), trying_params.clone())
                .await
                .map_err(|e| {
                    tracing::error!(
                        contract = %trying_key,
                        error = %e,
                        "failed to store contract state"
                    );
                    ExecutorError::other(e)
                })?;
            if trying_key != original_key {
                trying_key = original_key;
                trying_params = original_params.clone();
                trying_state = original_state.clone();
                continue;
            }
            break;
        }
        if iterations == DEPENDENCY_CYCLE_LIMIT_GUARD {
            return Err(ExecutorError::request(StdContractError::MissingRelated {
                key: *original_key.id(),
            }));
        }
        Ok(())
    }

    /// Delivers update notifications to LOCAL client subscriptions via websocket channels.
    ///
    /// # Architecture: Local vs Network Subscriptions
    ///
    /// Freenet uses two distinct subscription delivery mechanisms:
    ///
    /// 1. **Local subscriptions** (handled here): Clients connected to this node's websocket
    ///    API register via `register_contract_notifier()`. Updates are delivered directly
    ///    through executor notification channels stored in `self.update_notifications`.
    ///    This path is triggered by `LocalSubscribeComplete` events (see `message.rs`).
    ///
    /// 2. **Network subscriptions** (handled in `operations/update.rs`): Remote peers
    ///    subscribe via `ring.seeding_manager.subscribers`. Updates propagate through
    ///    the network via `get_broadcast_targets_update()` which filters network peers
    ///    to receive UPDATE messages.
    ///
    /// This separation allows local clients to receive updates immediately without
    /// network round-trips, while network subscriptions follow the peer-to-peer protocol.
    ///
    /// See also:
    /// - `operations/subscribe.rs::complete_local_subscription()` - triggers LocalSubscribeComplete
    /// - `operations/update.rs::get_broadcast_targets_update()` - network subscription routing
    async fn send_update_notification(
        &mut self,
        key: &ContractKey,
        params: &Parameters<'_>,
        new_state: &WrappedState,
    ) -> Result<(), ExecutorError> {
        tracing::debug!(contract = %key, "notify of contract update");
        let key = *key;
        let instance_id = *key.id();
        if let Some(notifiers) = self.update_notifications.get_mut(&instance_id) {
            let summaries = self.subscriber_summaries.get_mut(&instance_id).unwrap();
            // in general there should be less than 32 failures
            let mut failures = Vec::with_capacity(32);
            for (peer_key, notifier) in notifiers.iter() {
                let peer_summary = summaries.get_mut(peer_key).unwrap();
                let update = match peer_summary {
                    Some(summary) => self
                        .runtime
                        .get_state_delta(&key, params, new_state, &*summary)
                        .map_err(|err| {
                            tracing::error!("{err}");
                            ExecutorError::execution(err, Some(InnerOpError::Upsert(key)))
                        })?
                        .to_owned()
                        .into(),
                    None => UpdateData::State(State::from(new_state.as_ref()).into_owned()),
                };
                if let Err(err) =
                    notifier.send(Ok(
                        ContractResponse::UpdateNotification { key, update }.into()
                    ))
                {
                    failures.push(*peer_key);
                    tracing::error!(
                        client = %peer_key,
                        contract = %key,
                        error = %err,
                        phase = "notification_send_failed",
                        "Failed to send update notification to client"
                    );
                } else {
                    tracing::debug!(
                        client = %peer_key,
                        contract = %key,
                        phase = "notification_sent",
                        "Sent update notification to client"
                    );
                }
            }
            if !failures.is_empty() {
                notifiers.retain(|(c, _)| !failures.contains(c));
            }
        }
        Ok(())
    }

    async fn get_contract_locally(
        &self,
        key: &ContractKey,
    ) -> Result<Option<ContractContainer>, ExecutorError> {
        let Some(parameters) = self
            .state_store
            .get_params(key)
            .await
            .map_err(ExecutorError::other)?
        else {
            // Parameters not in state_store yet
            // This can happen when a contract was just stored but state hasn't been stored yet
            // In this case, we can't fetch the contract because fetch_contract requires params
            tracing::debug!(
                contract = %key,
                "Contract parameters not in state_store, cannot fetch contract"
            );
            return Ok(None);
        };

        let Some(contract) = self.runtime.contract_store.fetch_contract(key, &parameters) else {
            return Ok(None);
        };
        Ok(Some(contract))
    }
}

impl Executor<Runtime> {
    async fn subscribe(&mut self, key: ContractKey) -> Result<(), ExecutorError> {
        if self.mode == OperationMode::Local {
            return Ok(());
        }
        let request = SubscribeContract {
            instance_id: *key.id(),
        };
        let _sub: operations::subscribe::SubscribeResult = self.op_request(request).await?;
        Ok(())
    }

    #[inline]
    async fn local_state_or_from_network(
        &mut self,
        id: &ContractInstanceId,
        return_contract_code: bool,
    ) -> Result<Either<WrappedState, operations::get::GetResult>, ExecutorError> {
        // Try to get locally if we have the full key
        if let Some(full_key) = self.lookup_key(id) {
            if let Ok(state) = self.state_store.get(&full_key).await {
                return Ok(Either::Left(state));
            }
        }
        // Fetch from network
        let request: GetContract = GetContract {
            instance_id: *id,
            return_contract_code,
        };
        let get_result: operations::get::GetResult = self.op_request(request).await?;
        Ok(Either::Right(get_result))
    }
}
