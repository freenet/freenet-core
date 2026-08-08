use super::*;

// ============================================================================
// Single Executor Implementation
// ============================================================================

// ============================================================================
// Bridged methods - shared production logic for Runtime and MockWasmRuntime
// ============================================================================

#[allow(private_bounds)]
impl<R, S> Executor<R, S>
where
    R: crate::wasm_runtime::ContractRuntimeBridge + Send + Sync,
    S: crate::wasm_runtime::StateStorage + Send + Sync + 'static,
    <S as crate::wasm_runtime::StateStorage>::Error: Into<anyhow::Error>,
{
    /// This node's contract-exec WASM counters, or `None` for an executor with
    /// no `OpManager` (unit-test and local-only executors have no `Ring` to
    /// attribute the work to, and nothing reads the counters there).
    ///
    /// Returns a borrow rather than an `Arc` clone so a counter bump on the
    /// contract-handling loop costs one `Relaxed` `fetch_add` and nothing else —
    /// see `ring::contract_exec_metrics` for the cost note and for why an
    /// undifferentiated handler-entry span could not answer the storm question.
    #[inline]
    pub(super) fn contract_exec_metrics(
        &self,
    ) -> Option<&crate::ring::contract_exec_metrics::ContractExecMetrics> {
        self.op_manager
            .as_ref()
            .map(|om| om.ring.contract_exec_metrics())
    }

    /// Grow the summary/delta caches' COUNT target to cover the node's live
    /// hosted-contract count before a summarize/delta computation, so the
    /// interest-heartbeat's hosted working set stays cached across cycles (no
    /// cold-module recompile). Tied to the real count via
    /// `Ring::hosting_contracts_count()`, clamped `[COUNT_MIN, COUNT_MAX]`. The
    /// byte budget (fixed at construction) is the independent hard RAM backstop —
    /// this only moves the COUNT target.
    ///
    /// This covers the hosted set; the small tail of contracts that are in use but
    /// already evicted from the hosted set (evicted-but-in-use, #4610) can make the
    /// true summarize working set — `(is_hosting || contract_in_use) &&
    /// state_present` — slightly exceed `hosting_contracts_count()`. The
    /// `COUNT_MARGIN` slack absorbs the common case; any residual simply recomputes
    /// harmlessly on the slow path.
    ///
    /// O(1) when already sized (the common case after warm-up): a count read plus a
    /// compare. `LruCache::resize` grows lazily (no preallocation), so memory
    /// tracks actual entries, not the cap.
    fn ensure_cache_covers_hosted_set(&mut self) {
        use std::num::NonZeroUsize;

        let Some(om) = self.op_manager.as_ref() else {
            return;
        };
        let hosted = om.ring.hosting_contracts_count();
        let needed = crate::contract::executor::summary_cache_count_target(hosted);
        // needed >= SUMMARY_CACHE_COUNT_MIN > 0 by construction (summary_cache_count_target clamps); unwrap_or is just panic-proofing.
        if needed > self.summary_cache.cap().get() {
            self.summary_cache
                .grow(NonZeroUsize::new(needed).unwrap_or(NonZeroUsize::MIN));
        }
        if needed > self.delta_cache.cap().get() {
            self.delta_cache
                .grow(NonZeroUsize::new(needed).unwrap_or(NonZeroUsize::MIN));
        }
    }

    pub(in crate::contract::executor) fn bridged_lookup_key(
        &self,
        instance_id: &ContractInstanceId,
    ) -> Option<ContractKey> {
        let code_hash = self.runtime.code_hash_from_id(instance_id)?;
        Some(ContractKey::from_id_and_code(*instance_id, code_hash))
    }

    pub(in crate::contract::executor) async fn bridged_fetch_contract(
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

    #[allow(clippy::too_many_lines)]
    pub(in crate::contract::executor) async fn bridged_upsert_contract_state(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<UpsertResult, ExecutorError> {
        self.bridged_upsert_contract_state_inner(key, update, related_contracts, code, false)
            .await
    }

    /// Inner implementation of [`bridged_upsert_contract_state`].
    ///
    /// `defer_related_fetch` controls what happens when validation/merge needs
    /// a related contract that is not held locally:
    /// - `false` (the default, used by `upsert_contract_state`): fetch it from
    ///   the network inline, awaiting the GET (legacy behavior).
    /// - `true` (used by `upsert_contract_state_deferrable`): do NOT fetch
    ///   inline. Roll back any partial work via the normal error path and
    ///   return [`ExecutorError::defer_related_fetch`] carrying the missing ids,
    ///   so the caller can off-load the GET from the serial event loop and
    ///   re-run the upsert with the states supplied. See issue #4391.
    pub(in crate::contract::executor) async fn bridged_upsert_contract_state_inner(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
        defer_related_fetch: bool,
    ) -> Result<UpsertResult, ExecutorError> {
        // CRITICAL: When a ContractContainer is provided, use its key instead of the passed-in key.
        let key = if let Some(ref container) = code {
            let container_key = container.key();
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
        let now = now_nanos();
        let stale = self
            .init_tracker
            .cleanup_stale_initializations(STALE_INIT_THRESHOLD, now);
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
            now,
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
            InitCheckResult::QueueFull => {
                tracing::warn!(
                    contract = %key,
                    limit = MAX_QUEUED_OPS_PER_CONTRACT,
                    "Contract initialization queue full, rejecting operation"
                );
                // Use the typed `ContractQueueFull` marker so the same
                // amplification suppression and DEBUG log severity that
                // the per-contract fair queue gets via
                // `send_queue_full_response` also applies here. Without
                // the typed marker, an init-queue-full looks like a
                // generic StdContractError::Update to callers and
                // re-enters the `try_auto_fetch_contract` /
                // `ResyncRequest` paths. Issue #4251.
                return Err(ExecutorError::other(super::ContractQueueFull));
            }
            InitCheckResult::Queued { queue_size } => {
                tracing::info!(
                    contract = %key,
                    queue_size,
                    "Operation queued during contract initialization"
                );
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
            let p = code.params();
            // Ensure params are persisted to state_store so they survive restarts.
            // The code path (PUT via GET) always provides params in the container,
            // but state_store.store() is only called for new contracts. If the contract
            // already exists (merge path), commit_state_update() calls state_store.update()
            // which doesn't write params. Persisting here covers all cases.
            if let Err(e) = self.state_store.ensure_params(key, p.clone()).await {
                tracing::warn!(
                    contract = %key,
                    error = %e,
                    "Failed to persist contract parameters to state_store"
                );
            }
            p
        } else {
            self.state_store
                .get_params(&key)
                .await
                .map_err(ExecutorError::other)?
                .ok_or_else(|| {
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

        // Track if we stored a new contract. `charged_wasm` carries the blob
        // length charged to the disk tracker (#4683) so every rollback site that
        // removes the just-stored contract also reverses the wasm charge.
        let (remove_if_fail, contract_was_provided, charged_wasm): (bool, bool, Option<usize>) =
            // Dedup probe keyed by CODE HASH, not instance id (#4218): a new
            // instance of already-stored code (same code hash, different params)
            // must take the "already in store" branch below so it is only
            // indexed — never re-stored and never charged against the disk
            // budget a second time. The old `fetch_contract_code` probe was
            // instance-keyed and reported such a second instance as absent,
            // double-counting the shared blob (visible across pool executors,
            // whose instance indexes previously diverged).
            if !self.runtime.code_blob_stored(key.code_hash()) {
                if let Some(ref contract_code) = code {
                    tracing::debug!(
                        contract = %key,
                        phase = "store_contract",
                        "Storing new contract"
                    );

                    // Disk-budget admission gate for the NEW (deduped) code blob
                    // (#4683): the code was not already on disk
                    // (`fetch_contract_code` returned None), so charge its bytes.
                    // Reject before the store so nothing lands; no rollback of the
                    // blob is needed since it was never written.
                    let blob_len = contract_code.data().len();
                    if let Some(op_manager) = &self.op_manager {
                        if let Err(over) = op_manager.ring.admit_wasm_write(blob_len) {
                            tracing::warn!(
                                contract = %key,
                                %over,
                                "Rejecting PUT: disk budget exceeded (contract code)"
                            );
                            return Err(ExecutorError::request(StdContractError::Put {
                                key,
                                cause: over.to_string().into(),
                            }));
                        }
                    }

                    self.runtime
                        .store_contract(contract_code.clone())
                        .map_err(ExecutorError::other)?;
                    // Charge the newly-written blob to the disk tracker NOW
                    // (#4683), before the state gate runs later in this same
                    // PUT. This (a) makes the state gate's aggregate include the
                    // wasm just stored, so a single PUT can't pass both the wasm
                    // and state gates independently and overshoot the budget, and
                    // (b) makes a burst of distinct-code PUTs visible to each
                    // other's admission check within one 60s du-walk window. The
                    // next `refresh_wasm` reconciles the counter against ground
                    // truth. Rolled back below if the PUT later fails to persist.
                    let charged = if let Some(op_manager) = &self.op_manager {
                        op_manager.ring.record_wasm_write(blob_len);
                        Some(blob_len)
                    } else {
                        None
                    };
                    (true, true, charged)
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
                        "Contract not in store and no code provided"
                    );
                    return Err(ExecutorError::request(StdContractError::MissingContract {
                        key: key.into(),
                    }));
                }
            } else if let Some(ref contract_code) = code {
                // Contract code already on disk, but this may be a NEW instance of
                // it: instances that reuse the same WASM with different parameters
                // (e.g. different River rooms) each need their own
                // instance→code row, or `lookup_key()` fails for the new
                // instance id. See issue #2380.
                //
                // This goes through `store_contract`, NOT through a bare
                // index-write helper, and that is the point. `store_contract` is
                // the store's ONE guarded ingress: it verifies that the key is
                // derived from the code and parameters in hand (see
                // `ContractStore::verify_contract_identity`) before it writes
                // anything, and its own fast paths then do exactly the
                // "just index this instance" work this branch needs — the blob is
                // already on disk, so no blob is rewritten and no byte is
                // re-charged against the disk budget.
                //
                // It used to call `ContractStore::ensure_key_indexed` directly,
                // which wrote the durable instance→code row with no derivation
                // check at all. That made this — the COMMON path, since any
                // contract reusing an already-stored binary lands here — an
                // unverified second ingress to the same index that
                // `store_contract` guards. Two writers of one durable row, one
                // guarded and one not, is the structure to avoid; do NOT
                // reintroduce a direct index write here.
                //
                // Only reached when code was provided in this request. With no
                // code this is a state-only update to a contract that is already
                // indexed, and there is nothing to verify against.
                self.runtime
                    .store_contract(contract_code.clone())
                    .map_err(ExecutorError::other)?;
                (false, true, None)
            } else {
                (false, false, None)
            };

        let is_new_contract = self.state_store.get(&key).await.is_err();

        // Save the incoming full state (if any) for potential corrupted-state recovery.
        // When the stored state is corrupted, WASM merge fails. If we have a validated
        // incoming full state, we can replace the corrupted state with it.
        let incoming_full_state = match &update {
            Either::Left(state) => Some(state.clone()),
            Either::Right(_) => None,
        };

        // If this is a new contract being stored, mark it as initializing
        if remove_if_fail && is_new_contract && contract_was_provided {
            tracing::debug!(
                contract = %key,
                "Starting contract initialization - queueing subsequent operations"
            );
            if let Err(e) = self.init_tracker.start_initialization(key, now_nanos()) {
                tracing::warn!(
                    contract = %key,
                    error = %e,
                    limit = MAX_CONCURRENT_INITIALIZATIONS,
                    "Too many concurrent initializations, rejecting PUT"
                );
                if let Err(re) = self.runtime.remove_contract(&key) {
                    tracing::warn!(
                        contract = %key,
                        error = %re,
                        "Failed to remove contract after init tracker rejection"
                    );
                }
                // Reverse the wasm charge (#4683): the blob is being removed.
                if let (Some(blob_len), Some(op_manager)) = (charged_wasm, &self.op_manager) {
                    op_manager.ring.record_wasm_removed(blob_len);
                }
                return Err(ExecutorError::request(StdContractError::Put {
                    key,
                    cause: "node is too busy: too many contracts initializing simultaneously, try again later".into(),
                }));
            }
        }

        let mut updates = match update {
            Either::Left(incoming_state) => {
                // Fast-reject oversized state before expensive WASM validation.
                // A malicious contract could return `Valid` for any state, so this check must
                // happen at the node level before any WASM execution.
                let incoming_size = incoming_state.as_ref().len();
                if incoming_size > MAX_STATE_SIZE {
                    crate::contract::record_state_size_rejection(
                        crate::contract::StateSizeRejectionStage::PreWasmFullState,
                        incoming_size,
                    );
                    tracing::warn!(
                        contract = %key,
                        size_bytes = incoming_size,
                        limit_bytes = MAX_STATE_SIZE,
                        "Rejecting oversized state before WASM validation"
                    );
                    if remove_if_fail {
                        if let Err(e) = self.runtime.remove_contract(&key) {
                            tracing::warn!(
                                contract = %key,
                                error = %e,
                                "failed to remove contract after size rejection"
                            );
                        }
                        // Reverse the wasm charge (#4683): the blob is removed.
                        if let (Some(blob_len), Some(op_manager)) = (charged_wasm, &self.op_manager)
                        {
                            op_manager.ring.record_wasm_removed(blob_len);
                        }
                    }
                    return Err(ExecutorError::request(StdContractError::Put {
                        key,
                        cause: format!(
                            "state size {incoming_size} bytes exceeds maximum allowed {MAX_STATE_SIZE} bytes"
                        )
                        .into(),
                    }));
                }

                let result = self
                    .fetch_related_for_validation(
                        &key,
                        &params,
                        &incoming_state,
                        &related_contracts,
                        defer_related_fetch,
                    )
                    .await
                    .inspect_err(|_| {
                        if remove_if_fail {
                            if let Err(e) = self.runtime.remove_contract(&key) {
                                tracing::warn!(contract = %key, error = %e, "failed to remove contract after validation failure");
                            }
                            // Reverse the wasm charge (#4683): the blob is removed.
                            if let (Some(blob_len), Some(op_manager)) =
                                (charged_wasm, &self.op_manager)
                            {
                                op_manager.ring.record_wasm_removed(blob_len);
                            }
                        }
                        // Clean up init_tracker so queued operations aren't left dangling
                        if let Some(dropped_count) = self.init_tracker.fail_initialization(&key) {
                            tracing::warn!(
                                contract = %key,
                                dropped_operations = dropped_count,
                                "Related contract validation failed, dropping queued operations"
                            );
                        }
                    })?;
                match result {
                    ValidateResult::Valid => {
                        tracing::debug!(
                            contract = %key,
                            phase = "validation_complete",
                            "Incoming state is valid"
                        );

                        if is_new_contract {
                            tracing::debug!(
                                contract = %key,
                                phase = "store_initial_state",
                                "Contract is new, storing initial state"
                            );
                            let state_to_store = incoming_state.clone();
                            let written_bytes = state_to_store.as_ref().len();
                            // Disk-budget admission gate (#4683): reject BEFORE
                            // the store if this write would push aggregate disk
                            // past the budget. No bytes have landed yet, so we
                            // only roll back the contract code we just stored and
                            // the init tracker; the rejection rides `PutMsg::Error`
                            // to the client and network via the non-fatal
                            // `StdContractError::Put`.
                            if let Some(op_manager) = &self.op_manager {
                                if let Err(over) =
                                    op_manager.ring.admit_state_write(&key, written_bytes)
                                {
                                    tracing::warn!(
                                        contract = %key,
                                        %over,
                                        "Rejecting PUT: disk budget exceeded"
                                    );
                                    if remove_if_fail {
                                        if let Err(e) = self.runtime.remove_contract(&key) {
                                            tracing::warn!(
                                                contract = %key,
                                                error = %e,
                                                "failed to remove contract after disk-budget rejection"
                                            );
                                        }
                                        // Reverse the wasm charge (#4683).
                                        if let Some(blob_len) = charged_wasm {
                                            op_manager.ring.record_wasm_removed(blob_len);
                                        }
                                    }
                                    if let Some(dropped) =
                                        self.init_tracker.fail_initialization(&key)
                                    {
                                        tracing::warn!(
                                            contract = %key,
                                            dropped_operations = dropped,
                                            "Disk-budget rejection dropped queued operations"
                                        );
                                    }
                                    return Err(ExecutorError::request(StdContractError::Put {
                                        key,
                                        cause: over.to_string().into(),
                                    }));
                                }
                            }
                            self.state_store
                                .store(key, state_to_store, params.clone())
                                .await
                                .map_err(ExecutorError::other)?;
                            // State-write chokepoint: delegate the three
                            // mandatory side effects (bump generation,
                            // refresh hosting-cache snapshot, report
                            // StateBytesWritten) to `Ring::commit_state_write`.
                            // See its rustdoc and `RuntimePool::remove_contract`
                            // for the EvictContract re-host race this closes.
                            if let Some(op_manager) = &self.op_manager {
                                op_manager.ring.commit_state_write(&key, written_bytes);
                            }

                            let completion_now = now_nanos();
                            if let Some(completion_info) = self
                                .init_tracker
                                .complete_initialization(&key, completion_now)
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

                                // Replay queued operations that arrived during initialization.
                                // These were UPDATE operations that couldn't proceed while the
                                // contract was being initialized. Now that initialization is
                                // complete, we apply them in order to the stored state.
                                let mut current = incoming_state.clone();
                                for op in completion_info.queued_ops {
                                    let queue_time = ContractInitTracker::queue_wait_duration(
                                        &op,
                                        completion_now,
                                    );
                                    tracing::info!(
                                        contract = %key,
                                        queue_time_ms = queue_time.as_millis(),
                                        is_delta = matches!(op.update, Either::Right(_)),
                                        has_related = op.related_contracts.states().next().is_some(),
                                        "Replaying queued operation after initialization"
                                    );

                                    let replay_updates = match op.update {
                                        Either::Left(state) => {
                                            vec![UpdateData::State(state.into())]
                                        }
                                        Either::Right(delta) => {
                                            vec![UpdateData::Delta(delta)]
                                        }
                                    };

                                    match self
                                        .attempt_state_update(
                                            &params,
                                            &current,
                                            &key,
                                            &replay_updates,
                                        )
                                        .await
                                    {
                                        Ok(Either::Left(new_state)) => {
                                            // Validate before accepting
                                            let valid = self
                                                .runtime
                                                .validate_state(
                                                    &key,
                                                    &params,
                                                    &new_state,
                                                    &op.related_contracts,
                                                )
                                                .map(|r| r == ValidateResult::Valid)
                                                .unwrap_or(false);

                                            if valid && new_state.as_ref() != current.as_ref() {
                                                if let Err(e) = self
                                                    .commit_state_update(&key, &params, &new_state)
                                                    .await
                                                {
                                                    tracing::warn!(
                                                        contract = %key,
                                                        error = %e,
                                                        "Failed to commit replayed queued operation"
                                                    );
                                                } else {
                                                    current = new_state;
                                                }
                                            } else if !valid {
                                                tracing::warn!(
                                                    contract = %key,
                                                    "Queued operation produced invalid state, skipping"
                                                );
                                            }
                                        }
                                        Ok(Either::Right(_missing)) => {
                                            tracing::warn!(
                                                contract = %key,
                                                "Queued operation needs related contracts, skipping"
                                            );
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                contract = %key,
                                                error = %e,
                                                "Failed to replay queued operation, skipping"
                                            );
                                        }
                                    }
                                }
                            }

                            self.broadcast_state_change(key, incoming_state.clone())
                                .await;

                            // Notify locally-subscribed WS clients of the
                            // new state. Without this, the very first state
                            // install for a contract on this node never
                            // reaches `register_contract_notifier` consumers
                            // — only the merge path at the end of this
                            // function calls `commit_state_update`, which
                            // is the only other site that fans out to the
                            // local notifier map. ResyncResponse-driven
                            // applies hit this branch when the state_store
                            // entry is missing, so subscribers would miss
                            // every cross-node delivery that recovers via
                            // resync.
                            tracing::info!(
                                contract = %key,
                                new_size_bytes = incoming_state.as_ref().len(),
                                phase = "update_complete",
                                event = "initial_state_installed",
                                "Contract initial state installed"
                            );
                            // Dashboard "last updated" telemetry; no-op if
                            // we're not subscribed to this contract.
                            if let Some(op_manager) = &self.op_manager {
                                op_manager.ring.record_contract_update(&key);
                            }
                            if let Err(err) = self
                                .send_update_notification(&key, &params, &incoming_state)
                                .await
                            {
                                tracing::error!(
                                    contract = %key,
                                    error = %err,
                                    phase = "notification_failed",
                                    "Failed to send initial-state notification"
                                );
                            }

                            return Ok(UpsertResult::Updated(incoming_state));
                        }
                    }
                    ValidateResult::Invalid => {
                        if let Some(dropped_count) = self.init_tracker.fail_initialization(&key) {
                            tracing::warn!(
                                contract = %key,
                                dropped_operations = dropped_count,
                                "Contract validation failed, dropping queued operations"
                            );
                        }
                        return Err(ExecutorError::request(StdContractError::invalid_put(key)));
                    }
                    ValidateResult::RequestRelated(_) => {
                        // fetch_related_for_validation resolves RequestRelated internally.
                        // If this is reached, it indicates a logic error in the helper.
                        if let Some(dropped_count) = self.init_tracker.fail_initialization(&key) {
                            tracing::warn!(
                                contract = %key,
                                dropped_operations = dropped_count,
                                "Unexpected RequestRelated after fetch, dropping queued operations"
                            );
                        }
                        return Err(ExecutorError::request(StdContractError::Put {
                            key,
                            cause: "unexpected RequestRelated after related contract resolution"
                                .into(),
                        }));
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
            Err(err @ StateStoreError::StateTooLarge { .. }) => {
                return Err(ExecutorError::other(err));
            }
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

        // Short-circuit: if the incoming state is byte-identical to the stored
        // state, there is nothing to merge and no WASM call is needed.  This
        // is the dominant case for idempotent re-broadcasts (a peer re-pushes
        // the state it already received) and avoids the spurious
        // `merge_rejected_valid_local` INFO log that was firing every time the
        // dedup cache missed an already-current state.  See issue #4151.
        if let Some(ref full_incoming) = incoming_full_state {
            if full_incoming.as_ref() == current_state.as_ref() {
                // Deterministic (zero-sampling) idempotency check on the
                // identical-input case, cooldown-bounded — see the method's
                // rustdoc. Runs BEFORE the NoChange fast-path return so a
                // non-idempotent contract is flagged on the first identical
                // re-push instead of waiting for the 1/32 sampled probe.
                self.probe_identical_input_idempotency(&key, &params, &current_state);
                tracing::debug!(
                    contract = %key,
                    state_size = current_state.size(),
                    event = "merge_skipped_identical",
                    "Incoming state is byte-identical to stored state — skipping WASM update_state"
                );
                return Ok(UpsertResult::NoChange);
            }
        }

        let mut recovery_performed = false;
        let updated_state = match self
            .attempt_state_update(&params, &current_state, &key, &updates)
            .await
        {
            Ok(Either::Left(s)) => s,
            Ok(Either::Right(missing_related)) => {
                // Contract's `update_state` returned `UpdateModification::requires(...)`,
                // listing related contracts it needs to apply the delta. Try to
                // fetch each one (local first, then network when op_manager is
                // wired) and re-attempt the update with the fetched states
                // surfaced as `UpdateData::RelatedState` entries — the same
                // pattern the validate-side `fetch_related_for_validation`
                // helper uses. Without this, every cross-node UPDATE that
                // references a related contract not yet cached locally would
                // fail with `MissingRelated` even though we could resolve it.
                //
                // Apply the same abuse-prevention guards the validate-side
                // path enforces (see `contracts.md` Abuse prevention):
                //   * Reject empty list (a contract MUST signal Valid via
                //     a populated state, not an empty `requires`).
                //   * Reject self-reference (no contract may ask for its
                //     own state through this path).
                //   * Cap at MAX_RELATED_CONTRACTS_PER_REQUEST so a
                //     misbehaving contract can't fan out 50 network GETs.
                //   * Dedup IDs so repeated declarations don't multiply
                //     the fetch count.
                if missing_related.is_empty() {
                    tracing::warn!(
                        contract = %key,
                        "update_state returned requires() with empty list"
                    );
                    return Err(ExecutorError::request(StdContractError::Update {
                        key,
                        cause: "contract requested related contracts but provided empty list"
                            .into(),
                    }));
                }
                let self_id = key.id();
                if missing_related
                    .iter()
                    .any(|c| &c.contract_instance_id == self_id)
                {
                    tracing::warn!(
                        contract = %key,
                        "update_state requires() included self-reference"
                    );
                    return Err(ExecutorError::request(StdContractError::Update {
                        key,
                        cause: "contract cannot request itself as a related contract".into(),
                    }));
                }
                let unique_ids: HashSet<ContractInstanceId> = missing_related
                    .iter()
                    .map(|c| c.contract_instance_id)
                    .collect();
                if unique_ids.len() > MAX_RELATED_CONTRACTS_PER_REQUEST {
                    tracing::warn!(
                        contract = %key,
                        requested = unique_ids.len(),
                        limit = MAX_RELATED_CONTRACTS_PER_REQUEST,
                        "update_state requires() exceeded MAX_RELATED_CONTRACTS_PER_REQUEST"
                    );
                    return Err(ExecutorError::request(StdContractError::Update {
                        key,
                        cause: format!(
                            "contract requested {} related contracts, limit is {}",
                            unique_ids.len(),
                            MAX_RELATED_CONTRACTS_PER_REQUEST
                        )
                        .into(),
                    }));
                }

                let mut fetched_updates = updates.clone();

                if defer_related_fetch {
                    // DEFERRABLE mode (serial `contract_handling` loop): resolve
                    // LOCAL-ONLY. This path MUST NEVER call
                    // `fetch_related_via_network` (an inline network GET on the
                    // serial loop). Resolve each id from the local state_store;
                    // anything missing is surfaced via `DeferRelated` so the
                    // caller off-loads the fetch. On resume this re-enters with
                    // the state supplied as a `RelatedState` update entry (so
                    // `requires()` no longer lists it), OR — if a misbehaving
                    // contract keeps requiring it — hits the one-deferral cap →
                    // MissingRelated, never an inline network GET. See #4391.
                    //
                    // Asymmetry with the validate-side deferrable block (which
                    // ALSO consults the caller-supplied `initial_related`) is
                    // INTENTIONAL: here, any related state the caller supplied
                    // was already folded into `updates` as `UpdateData::RelatedState`
                    // before `update_state` ran, so a well-behaved contract's
                    // `requires()` never lists a supplied id. A misbehaving one
                    // that re-requires it defers once, then the one-deferral cap
                    // converts the second `DeferRelated` to `MissingRelated`.
                    // Either way the no-inline-fetch invariant holds, so checking
                    // only the state_store here is sufficient.
                    let mut missing = Vec::new();
                    for id in &unique_ids {
                        let resolved = if let Some(full_key) = self.bridged_lookup_key(id) {
                            self.state_store.get(&full_key).await.ok()
                        } else {
                            None
                        };
                        match resolved {
                            Some(state) => fetched_updates.push(UpdateData::RelatedState {
                                related_to: *id,
                                state: State::from(state.as_ref().to_vec()),
                            }),
                            None => missing.push(*id),
                        }
                    }
                    if !missing.is_empty() {
                        return Err(ExecutorError::defer_related_fetch(missing));
                    }
                } else {
                    // NON-deferrable mode: parallel fetch — each related contract
                    // goes through its own GET sub-op concurrently. Previously
                    // serial under a single 10s wall-clock budget, so a contract
                    // requesting N>1 related ids could time out at ~10s/N
                    // effective per fetch. Fan-out via `join_all` turns the
                    // budget back into 10s _per id_ in the common case (network
                    // bandwidth, not CPU, is the constraint). See
                    // freenet/freenet-core#4077.
                    let fetch_results: Vec<(
                        ContractInstanceId,
                        Result<State<'static>, ExecutorError>,
                    )> = {
                        // Reborrow as `&Self` so the per-id futures all share
                        // an immutable borrow; this releases the outer
                        // `&mut self` only for the duration of `fetch_all`,
                        // which is fully awaited before the next `&mut self`
                        // call (`attempt_state_update` below).
                        let this: &Self = &*self;
                        futures::future::join_all(unique_ids.iter().map(|id| {
                            let id = *id;
                            async move {
                                if let Some(full_key) = this.bridged_lookup_key(&id) {
                                    if let Ok(state) = this.state_store.get(&full_key).await {
                                        return (id, Ok(State::from(state.as_ref().to_vec())));
                                    }
                                }
                                let outcome =
                                    fetch_related_via_network(this.op_manager.as_ref(), &id)
                                        .await
                                        .map(|state| State::from(state.as_ref().to_vec()));
                                (id, outcome)
                            }
                        }))
                        .await
                    };
                    let mut failed_id: Option<ContractInstanceId> = None;
                    for (id, res) in fetch_results {
                        match res {
                            Ok(state) => fetched_updates.push(UpdateData::RelatedState {
                                related_to: id,
                                state,
                            }),
                            Err(err) => {
                                tracing::warn!(
                                    contract = %key,
                                    related_id = %id,
                                    error = %err,
                                    "Failed to fetch related contract for update_state requires()"
                                );
                                failed_id.get_or_insert(id);
                            }
                        }
                    }
                    if let Some(id) = failed_id {
                        return Err(ExecutorError::request(StdContractError::MissingRelated {
                            key: id,
                        }));
                    }
                }
                match self
                    .attempt_state_update(&params, &current_state, &key, &fetched_updates)
                    .await
                {
                    Ok(Either::Left(s)) => s,
                    Ok(Either::Right(mut r)) => {
                        // Contract still demanding more after one round → reject
                        // (depth limit, matching the validate-side behavior).
                        let Some(c) = r.pop() else {
                            return Err(ExecutorError::internal_error());
                        };
                        tracing::warn!(
                            contract = %key,
                            related_id = %c.contract_instance_id,
                            "update_state still requires() after first fetch round (depth>1 not supported)"
                        );
                        return Err(ExecutorError::request(StdContractError::MissingRelated {
                            key: c.contract_instance_id,
                        }));
                    }
                    Err(retry_err) => return Err(retry_err),
                }
            }
            Err(merge_err) => {
                // Merge failed. If we have a validated full incoming state, try to recover
                // by replacing the (likely corrupted) local state. The incoming state was
                // already validated at entry to this function.
                let Some(ref valid_incoming) = incoming_full_state else {
                    // Delta update failed and we don't have a full state to recover with.
                    // Propagate the error (the caller may send a ResyncRequest).
                    return Err(merge_err);
                };

                // Before assuming local state is corrupted, validate it. If the local
                // state is valid, the merge failure is legitimate (e.g., the incoming
                // state is older and the contract's merge function correctly rejected
                // it). Only trigger recovery when the local state itself fails
                // validation. See issue #3109.
                let local_valid = self
                    .runtime
                    .validate_state(&key, &params, &current_state, &related_contracts)
                    .map(|r| r == ValidateResult::Valid)
                    .unwrap_or(false);

                // Local state is valid — the merge failure is legitimate, not corruption.
                if local_valid {
                    // Downgrade to DEBUG for idempotent re-pushes where the contract's
                    // merge function correctly rejected the incoming state because its
                    // version is not newer (e.g. "New state version X must be higher
                    // than current version X"). These fire on every re-broadcast that
                    // misses the dedup cache and are not operator-actionable. Any other
                    // merge failure (OOG, WASM trap, etc.) keeps the INFO level because
                    // it may indicate a real problem. See issue #4151.
                    if merge_err.is_invalid_update_rejection() {
                        tracing::debug!(
                            contract = %key,
                            error = %merge_err,
                            local_state_size = current_state.size(),
                            incoming_state_size = valid_incoming.size(),
                            event = "merge_rejected_valid_local",
                            "Merge rejected incoming state (idempotent re-push, \
                             incoming version not newer) - not replacing"
                        );
                    } else {
                        tracing::info!(
                            contract = %key,
                            error = %merge_err,
                            local_state_size = current_state.size(),
                            incoming_state_size = valid_incoming.size(),
                            event = "merge_rejected_valid_local",
                            "Merge rejected incoming state but local state is valid - \
                             not replacing (incoming state may be stale)"
                        );
                    }
                    return Err(merge_err);
                }

                // Local state failed validation — it's likely corrupted.

                // Check and mark recovery in a single lock acquisition.
                let already_recovered = {
                    let mut guard = self
                        .recovery_guard
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    if guard.contains(&key) {
                        true
                    } else {
                        guard.insert(key);
                        false
                    }
                };

                if already_recovered {
                    tracing::error!(
                        contract = %key,
                        error = %merge_err,
                        event = "corrupted_state_recovery_exhausted",
                        "State recovery already attempted, contract is broken - not retrying"
                    );
                    return Err(merge_err);
                }

                tracing::warn!(
                    contract = %key,
                    error = %merge_err,
                    incoming_state_size = valid_incoming.size(),
                    event = "corrupted_state_recovery",
                    "Merge failed with validated incoming state and local state is invalid - \
                     replacing corrupted local state with incoming state"
                );

                recovery_performed = true;
                valid_incoming.clone()
            }
        };

        // Clear the recovery guard for this contract on a successful merge
        // (NOT on the same call that performed recovery — the guard must persist
        // so a subsequent failure is detected as a broken contract).
        if incoming_full_state.is_some() && !recovery_performed {
            self.recovery_guard
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key);
        }

        let result = self
            .fetch_related_for_validation(
                &key,
                &params,
                &updated_state,
                &related_contracts,
                defer_related_fetch,
            )
            .await?;

        if result != ValidateResult::Valid {
            return Err(Self::validation_error(key, result));
        }
        if updated_state.as_ref() == current_state.as_ref() {
            Ok(UpsertResult::NoChange)
        } else {
            // CRDT-invariant idempotency probe. With low probability, re-run
            // `update_state` with `updated_state` as the current state and
            // the same `updates`. A correct CRDT must satisfy
            // `update_state(update_state(S, U), U) == update_state(S, U)` —
            // a violation indicates a contract bug (timestamp/RNG/position-
            // dependent signing/etc.) that produces an infinite broadcast
            // storm in the network. See `crate::ring::broken_invariants`.
            //
            // `RelatedState` inputs are skipped: re-applying a cross-
            // contract state hint isn't required to be a no-op by the
            // contract ABI, so probing it can produce false positives.
            self.maybe_probe_idempotency(&key, &params, &updated_state, &updates)
                .await;

            if self
                .op_manager
                .as_ref()
                .map(|m| m.ring.is_contract_broken(&key))
                .unwrap_or(false)
            {
                // Probe just flagged this contract (or it was already
                // flagged). Skip the commit so we don't extend the
                // problematic state, and surface the suppression to
                // callers via NoChange — there is no state change the
                // network should observe.
                tracing::debug!(
                    contract = %key,
                    event = "merge_suppressed_broken_contract",
                    "Skipping commit_state_update for contract flagged as broken"
                );
                return Ok(UpsertResult::NoChange);
            }

            self.commit_state_update(&key, &params, &updated_state)
                .await?;
            Ok(UpsertResult::Updated(updated_state))
        }
    }

    pub(in crate::contract::executor) fn bridged_register_contract_notifier(
        &mut self,
        instance_id: ContractInstanceId,
        cli_id: ClientId,
        notification_ch: tokio::sync::mpsc::Sender<HostResult>,
        summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>> {
        // Check if already registered (immutable borrow)
        let already_registered = self
            .update_notifications
            .get(&instance_id)
            .and_then(|channels| {
                channels
                    .binary_search_by_key(&&cli_id, |(p, _)| p)
                    .ok()
                    .map(|i| (i, channels[i].1.same_channel(&notification_ch)))
            });

        if let Some((idx, same_channel)) = already_registered {
            if !same_channel {
                tracing::info!(
                    contract = %instance_id,
                    client = %cli_id,
                    "Client already subscribed, updating notification channel"
                );
                // Safety: `already_registered` was derived from `self.update_notifications.get(&instance_id)`
                // succeeding, so the entry is guaranteed to exist.
                //
                // This keeps the read-index / re-acquire / write shape that the
                // `RuntimePool` twin deliberately collapsed (#5040). It is
                // exempt, not overlooked: this map is a plain `HashMap` behind
                // `&mut self` with no `.await` between the two lookups, so no
                // other code can run in the window and the entry cannot vanish.
                // The pool twin works through `Arc<DashMap>`, where that
                // guarantee does not hold. Do NOT copy this shape there.
                if let Some(channels) = self.update_notifications.get_mut(&instance_id) {
                    channels[idx] = (cli_id, notification_ch);
                }
            }
        } else {
            // New subscriber: enforce per-contract limit
            let contract_sub_count = self
                .update_notifications
                .get(&instance_id)
                .map_or(0, |ch| ch.len());
            if contract_sub_count >= super::MAX_SUBSCRIBERS_PER_CONTRACT {
                tracing::warn!(
                    client = %cli_id,
                    contract = %instance_id,
                    limit = super::MAX_SUBSCRIBERS_PER_CONTRACT,
                    "Subscriber limit reached for contract, rejecting registration"
                );
                let key = self
                    .bridged_lookup_key(&instance_id)
                    .unwrap_or_else(|| synthetic_key(instance_id));
                return Err(subscriber_limit_error(
                    key,
                    &format!(
                        "subscriber limit ({}) reached for contract",
                        super::MAX_SUBSCRIBERS_PER_CONTRACT
                    ),
                ));
            }

            // Enforce per-client subscription limit using O(1) counter
            let client_sub_count = self
                .client_subscription_counts
                .get(&cli_id)
                .copied()
                .unwrap_or(0);
            if client_sub_count >= super::MAX_SUBSCRIPTIONS_PER_CLIENT {
                tracing::warn!(
                    client = %cli_id,
                    contract = %instance_id,
                    limit = super::MAX_SUBSCRIPTIONS_PER_CLIENT,
                    current = client_sub_count,
                    "Per-client subscription limit reached, rejecting registration"
                );
                let key = self
                    .bridged_lookup_key(&instance_id)
                    .unwrap_or_else(|| synthetic_key(instance_id));
                return Err(subscriber_limit_error(
                    key,
                    &format!(
                        "per-client subscription limit ({}) reached",
                        super::MAX_SUBSCRIPTIONS_PER_CLIENT
                    ),
                ));
            }

            // Insert in sorted order for efficient lookup (matches pool path)
            let channels = self.update_notifications.entry(instance_id).or_default();
            let insert_pos = channels.partition_point(|(id, _)| id < &cli_id);
            channels.insert(insert_pos, (cli_id, notification_ch));
            *self.client_subscription_counts.entry(cli_id).or_insert(0) += 1;
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

    pub(in crate::contract::executor) async fn bridged_summarize_contract_state(
        &mut self,
        key: ContractKey,
    ) -> Result<StateSummary<'static>, ExecutorError> {
        // Size the caches' count target to the live hosted set before any
        // lookup/insert, so the interest-heartbeat's working set stays cached
        // across cycles (the byte budget stays fixed as the RAM backstop).
        self.ensure_cache_covers_hosted_set();

        // Fast path (the hot path on a busy node — tens/sec over MB-scale
        // states): if the state store holds a cheap change-detector hash for
        // this contract's CURRENT state AND we already have a summary cached
        // against that exact hash, return it WITHOUT loading the full state,
        // hashing it, or running the WASM `summarize_state`. The detector hash is
        // invalidated by every state write and (re)populated only from a
        // freshly-loaded state (see `StateStore::state_hash_cache`), so a hit
        // proves the state is byte-identical to the one that produced the cached
        // summary — the summary is therefore fresh, never stale.
        //
        // SERIALIZATION INVARIANT: the no-stale-populate guarantee depends on
        // ALL summarize/delta reads (this one) AND ALL contract-state writes
        // running on the single `&mut RuntimePool` contract-handling loop, so no
        // write can land between the slow path's state load and its detector
        // populate. Any off-loop work that holds an executor (e.g. the hosted
        // secret export) MUST stay read-only w.r.t. contract state, or it could
        // populate the detector against a state a concurrent write has changed.
        if let Some(detector_hash) = self.state_store.cached_state_hash(&key) {
            // Resolve the hit to an owned value BEFORE recording: `LruCache::get`
            // borrows the executor mutably (it reorders the recency list), so the
            // counter read cannot overlap it. The clone is the same one the
            // return did before; it just moves ahead of the borrow's end.
            let hit = self
                .summary_cache
                .get(&key)
                .and_then(|(hash, summary)| (*hash == detector_hash).then(|| summary.clone()));
            if let Some(cached_summary) = hit {
                // Field-visible cache-HIT count. Without this, the only
                // production signal for this function was a handler-entry span
                // that fires identically here and on the WASM path below, so
                // every storm rate ever quoted conflated the two. See
                // `ring::contract_exec_metrics`.
                if let Some(m) = self.contract_exec_metrics() {
                    m.record_summarize_fast_hit();
                }
                return Ok(cached_summary);
            }
        }

        // Slow path: state changed, never summarized, or detector cold (after
        // restart / eviction). Load the state and recompute as before.
        let (state, _) = self.bridged_fetch_contract(key, false).await?;

        let state = state.ok_or_else(|| {
            ExecutorError::request(StdContractError::Get {
                key,
                cause: "contract state not found".into(),
            })
        })?;

        let state_hash = crate::wasm_runtime::state_hash(&state);

        // Repopulate the change-detector so the NEXT summarize of an unchanged
        // state takes the fast path. Safe under the serialized contract loop: no
        // write can interleave between the load above and here, so this hash
        // matches the state currently on disk.
        self.state_store.cache_state_hash(key, state_hash);

        // The summary may already be cached under this exact hash even when the
        // detector was cold (the summary cache is per-executor; the detector is
        // shared). Reuse it to skip the WASM call.
        let reload_hit = self
            .summary_cache
            .get(&key)
            .and_then(|(hash, summary)| (*hash == state_hash).then(|| summary.clone()));
        if let Some(cached_summary) = reload_hit {
            // Reached the slow path (loaded + hashed the state) but still elided
            // the WASM call. Counted separately from the fast hit so a cold
            // detector is distinguishable from a cold cache.
            if let Some(m) = self.contract_exec_metrics() {
                m.record_summarize_reload_hit();
            }
            return Ok(cached_summary);
        }

        let params = self
            .state_store
            .get_params(&key)
            .await
            .map_err(ExecutorError::other)?
            .ok_or_else(|| {
                ExecutorError::request(StdContractError::Get {
                    key,
                    cause: "contract parameters not found".into(),
                })
            })?;

        // Summarize-storm falsifier (spec step 8 / #4440): count the actual WASM
        // `summarize_state` invocation — the SLOW-path miss the state-hash cache
        // above exists to elide. A cache HIT (the fast path near the top of this
        // fn) does NOT reach here, so this counter measures exactly the expensive
        // work whose per-heartbeat × per-neighbor multiplication was the storm.
        // Under the working cache it scales with the STATE-CHANGE rate, not with
        // hosted-set size or neighbor overlap — every-hop placement's "summarize
        // load stays flat vs hosted-set size" invariant.
        //
        // TWO sinks, deliberately, because they answer to different readers and
        // neither can serve the other:
        //   * `ring.contract_exec_metrics()` is the PRODUCTION counter, read on
        //     the `router_snapshot` cadence. Always live.
        //   * `topology_registry` is the SIMULATION counter, keyed by peer
        //     address so `SimNetwork` can aggregate across nodes; it is a no-op
        //     outside a sim (the record fn reads the sim-only network-name
        //     thread-local) and its `get_own_addr()` lookup is deliberately kept
        //     off the hot path by living here on the WASM slow path. This runs on
        //     the contract-handling loop thread, not a `spawn_blocking` closure,
        //     so the thread-local is set.
        // `summarize_wasm_call_records_both_sinks` pins that they cannot drift
        // apart.
        if let Some(op_manager) = &self.op_manager {
            op_manager
                .ring
                .contract_exec_metrics()
                .record_summarize_wasm_call();
            if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
                crate::ring::topology_registry::record_summarize_wasm_call(own_addr);
            }
        }

        let summary = self
            .runtime
            .summarize_state(&key, &params, &state)
            .map_err(|e| ExecutorError::execution(e, None))?;

        self.summary_cache.put(key, (state_hash, summary.clone()));
        Ok(summary)
    }

    pub(in crate::contract::executor) async fn bridged_get_contract_state_delta(
        &mut self,
        key: ContractKey,
        their_summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ExecutorError> {
        // Size the caches' count target to the live hosted set before any
        // lookup/insert, so the interest-heartbeat's working set stays cached
        // across cycles (the byte budget stays fixed as the RAM backstop).
        self.ensure_cache_covers_hosted_set();

        // Hash the peer's summary up front — it's a small digest, so this is
        // cheap (unlike the full state). Only the STATE component of the cache
        // key gets the cheap change-detector treatment below.
        let summary_hash = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            their_summary.as_ref().hash(&mut h);
            h.finish()
        };

        // Fast path: this runs per-subscriber during broadcast fan-out, so it is
        // potentially even hotter than summarize. If the state store holds a
        // change-detector hash for this contract's CURRENT state AND we already
        // cached the delta for that exact (state, their_summary) pair, return it
        // WITHOUT loading or hashing the full state, or running the WASM
        // `get_state_delta`. The detector guarantees the state is unchanged, so a
        // cached delta computed against the same peer-summary is fresh — a stale
        // delta would diverge the peer just like a stale summary.
        //
        // SERIALIZATION INVARIANT: as in `bridged_summarize_contract_state`, the
        // no-stale-populate guarantee depends on all summarize/delta reads and
        // all contract-state writes running on the single `&mut RuntimePool`
        // loop; off-loop work holding an executor must stay read-only w.r.t.
        // contract state.
        if let Some(detector_hash) = self.state_store.cached_state_hash(&key) {
            let cache_key = (key, detector_hash, summary_hash);
            // Owned before recording: `LruCache::get` borrows mutably (see the
            // summarize twin above).
            let hit = self.delta_cache.get(&cache_key).cloned();
            if let Some(cached_delta) = hit {
                // Field-visible cache-HIT count; see the summarize twin above
                // and `ring::contract_exec_metrics`. This arm runs per-SUBSCRIBER
                // during broadcast fan-out, so it is the hotter of the two.
                if let Some(m) = self.contract_exec_metrics() {
                    m.record_delta_fast_hit();
                }
                return Ok(cached_delta);
            }
        }

        // Slow path: state changed, this peer-summary not seen for the current
        // state, or detector cold. Load the state and recompute as before.
        let (state, _) = self.bridged_fetch_contract(key, false).await?;

        let state = state.ok_or_else(|| {
            ExecutorError::request(StdContractError::Get {
                key,
                cause: "contract state not found".into(),
            })
        })?;

        let state_hash = crate::wasm_runtime::state_hash(&state);

        // Repopulate the change-detector so the next delta/summarize of an
        // unchanged state takes the fast path. Safe under the serialized contract
        // loop (see `bridged_summarize_contract_state`).
        self.state_store.cache_state_hash(key, state_hash);

        let cache_key = (key, state_hash, summary_hash);
        let reload_hit = self.delta_cache.get(&cache_key).cloned();
        if let Some(cached_delta) = reload_hit {
            // Slow path reached (state loaded + hashed) but the WASM call was
            // still elided; see the summarize twin above.
            if let Some(m) = self.contract_exec_metrics() {
                m.record_delta_reload_hit();
            }
            return Ok(cached_delta);
        }

        let params = self
            .state_store
            .get_params(&key)
            .await
            .map_err(ExecutorError::other)?
            .ok_or_else(|| {
                ExecutorError::request(StdContractError::Get {
                    key,
                    cause: "contract parameters not found".into(),
                })
            })?;

        // The delta twin of the summarize slow-path counter: a true cache miss
        // that actually runs the contract's WASM `get_state_delta`. Recorded at
        // the decision, immediately before the call it describes.
        if let Some(m) = self.contract_exec_metrics() {
            m.record_delta_wasm_call();
        }

        let delta = self
            .runtime
            .get_state_delta(&key, &params, &state, &their_summary)
            .map_err(|e| ExecutorError::execution(e, None))?;

        self.delta_cache.put(cache_key, delta.clone());
        Ok(delta)
    }

    // --- Helper methods ---

    pub(super) async fn perform_contract_get(
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
            tracing::debug!(
                contract = %key,
                "Contract parameters not in state_store, cannot fetch contract"
            );
            return Ok(None);
        };

        let Some(contract) = self.runtime.fetch_contract_code(key, &parameters) else {
            return Ok(None);
        };
        Ok(Some(contract))
    }

    pub(super) async fn attempt_state_update(
        &mut self,
        parameters: &Parameters<'_>,
        current_state: &WrappedState,
        key: &ContractKey,
        updates: &[UpdateData<'_>],
    ) -> Result<Either<WrappedState, Vec<RelatedContract>>, ExecutorError> {
        // Cost telemetry (cost-aware eviction, #4861): measure the elapsed
        // time of the blocking WASM `update_state` call and attribute it to
        // the contract on the `ExecCpuMicros` meter axis — INCLUDING applies
        // whose commit is later suppressed (NoChange / byte-identical merge)
        // and applies that ERROR (trap/timeout), since the CPU is burned
        // regardless of whether anything commits. Feeds the hosting sweep's
        // cost-pressure trigger so a zero-subscriber contract that dominates
        // update CPU becomes an eviction candidate. Uses the ring's injected
        // `TimeSource` (deterministic-sim discipline; under paused sim time
        // the elapsed reads 0, and sim tests inject cost via
        // `report_contract_resource_usage` directly). The report path is
        // deadlock-safe by design (brief sync lock, no channels — see
        // `Ring::report_contract_resource_usage`).
        let cost_clock = self
            .op_manager
            .as_ref()
            .map(|op_manager| op_manager.ring.time_source.clone());
        let update_started = cost_clock.as_ref().map(|clock| clock.now());
        let update_result = self
            .runtime
            .update_state(key, parameters, current_state, updates);
        if let (Some(op_manager), Some(clock), Some(started)) = (
            self.op_manager.as_ref(),
            cost_clock.as_ref(),
            update_started,
        ) {
            let elapsed = clock.now().saturating_duration_since(started);
            op_manager.ring.report_contract_resource_usage(
                *key.id(),
                crate::topology::meter::ResourceType::ExecCpuMicros,
                elapsed.as_micros() as f64,
            );
        }
        let update_modification = match update_result {
            Ok(result) => result,
            Err(err) => {
                return Err(ExecutorError::execution(
                    err,
                    Some(InnerOpError::Upsert(*key)),
                ));
            }
        };
        let UpdateModification {
            new_state, related, ..
        } = update_modification;
        let Some(new_state) = new_state else {
            if related.is_empty() {
                return Ok(Either::Left(current_state.clone()));
            } else {
                return Ok(Either::Right(related));
            }
        };
        let new_state = WrappedState::new(new_state.into_bytes());

        // Conformance capture (RFC #5320), off unless an operator sets
        // FREENET_CONFORMANCE_CAPTURE_DIR. This is the seam where the base state,
        // the update that was applied and the resulting state are all in hand,
        // which is exactly the transition an offline replay needs.
        //
        // Cost when disabled is one atomic load. When enabled it is a `try_send` on
        // a bounded channel that drops rather than waits, so a slow writer can never
        // stall a merge — capture losing observations is always preferable to
        // synchronization queueing behind it.
        if let Some(capture) = crate::conformance::capture::global() {
            // Measure first, copy later.
            //
            // Everything below this point that costs an allocation happens inside the
            // `observe_with` closure, which runs only after a queue slot and byte
            // budget are secured. An earlier version computed the incoming state,
            // delta and related payloads BEFORE the budget check and then claimed in
            // a comment that no byte was copied before it — which was false for
            // exactly the three largest fields, and worst for related state, since
            // that carries another contract's whole state. Under sustained load with
            // a full queue, that made the drop path pay full allocate-and-copy on the
            // merge path, which is the cost this ordering exists to avoid.
            let mut incoming_len = 0usize;
            let mut delta_len = 0usize;
            let mut related_len = 0usize;
            for update in updates {
                match update {
                    UpdateData::State(state) => incoming_len = state.as_ref().len(),
                    UpdateData::Delta(delta) => delta_len = delta.as_ref().len(),
                    UpdateData::StateAndDelta { state, delta } => {
                        incoming_len = state.as_ref().len();
                        delta_len = delta.as_ref().len();
                    }
                    UpdateData::RelatedState { state, .. }
                    | UpdateData::RelatedStateAndDelta { state, .. } => {
                        related_len += state.as_ref().len();
                    }
                    UpdateData::RelatedDelta { .. } => {}
                    // `UpdateData` is `#[non_exhaustive]`. A future variant carrying
                    // related state would be missed here and in the closure below;
                    // both sites are marked so the pair is updated together.
                    // AUDIT: new Related* variant -> update both match sites.
                    _ => {}
                }
            }

            let size_hint = parameters.as_ref().len()
                + current_state.as_ref().len()
                + new_state.as_ref().len()
                + incoming_len
                + delta_len
                + related_len;

            capture.observe_with(size_hint, || {
                let (incoming_state, delta) = updates.iter().fold((None, None), |acc, update| {
                    match update {
                        UpdateData::State(state) => (Some(state.as_ref().to_vec()), acc.1),
                        UpdateData::Delta(delta) => (acc.0, Some(delta.as_ref().to_vec())),
                        UpdateData::StateAndDelta { state, delta } => {
                            (Some(state.as_ref().to_vec()), Some(delta.as_ref().to_vec()))
                        }
                        // Related payloads are not part of THIS transition; they are
                        // collected separately below as the context the contract needs
                        // to execute at all.
                        UpdateData::RelatedState { .. }
                        | UpdateData::RelatedDelta { .. }
                        | UpdateData::RelatedStateAndDelta { .. }
                        | _ => acc,
                    }
                });

                // Only full states: a related DELTA cannot be applied without the
                // state it is relative to, which this peer may not hold.
                // AUDIT: new Related* variant -> update both match sites.
                let related: Vec<(ContractInstanceId, Vec<u8>)> = updates
                    .iter()
                    .filter_map(|update| match update {
                        UpdateData::RelatedState { related_to, state }
                        | UpdateData::RelatedStateAndDelta {
                            related_to, state, ..
                        } => Some((*related_to, state.as_ref().to_vec())),
                        UpdateData::State(_)
                        | UpdateData::Delta(_)
                        | UpdateData::StateAndDelta { .. }
                        | UpdateData::RelatedDelta { .. }
                        | _ => None,
                    })
                    .collect();

                crate::conformance::capture::Observation {
                    contract: *key.id(),
                    code_hash: crate::conformance::capture::code_hash_of(key),
                    parameters: parameters.as_ref().to_vec(),
                    base_state: current_state.as_ref().to_vec(),
                    incoming_state,
                    delta,
                    result_state: new_state.as_ref().to_vec(),
                    related,
                }
            });
        }

        if new_state.as_ref() == current_state.as_ref() {
            tracing::debug!(
                contract = %key,
                phase = "update_skipped",
                "No changes in state, avoiding update"
            );
            return Ok(Either::Left(current_state.clone()));
        }

        Ok(Either::Left(new_state))
    }

    /// Probe-sampled idempotency check. Re-runs `update_state` with the just-
    /// produced state as the current state and the same updates; flags the
    /// contract as broken if the re-applied state's byte MULTISET differs from
    /// the original (a genuine content change), but NOT if it is merely a
    /// reordering of the same bytes (benign serialization nondeterminism such
    /// as `HashMap` key order — the #4295 false-positive case). See
    /// [`byte_multiset_eq`].
    ///
    /// Costs one extra WASM invocation per sampled merge. With the
    /// configured probability [`IDEMPOTENCY_PROBE_PROBABILITY`] (currently
    /// 1/32) this is a few percent overhead on active contracts and
    /// effectively zero on quiet ones. Sample selection uses `GlobalRng`
    /// so simulation builds remain deterministic under a fixed seed.
    ///
    /// Only probes update batches whose every entry is
    /// `UpdateData::State(_)`. `Delta`/`StateAndDelta` are exempted
    /// because operation-based CRDTs (counters, append-logs) legitimately
    /// violate the byte-equality property on re-apply; `RelatedState` is
    /// exempted because it's a cross-contract hint, not a CRDT op over
    /// this contract's state. Probe traps (timeout, OOG, panic) are
    /// logged but not treated as positive signals — distinguishing
    /// "buggy on re-apply" from "buggy in some other way" requires a
    /// separate detector. See `crate::ring::broken_invariants`.
    async fn maybe_probe_idempotency(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        post_merge_state: &WrappedState,
        updates: &[UpdateData<'_>],
    ) {
        // Cheap precheck: if already flagged, no value in probing again.
        if let Some(op_manager) = &self.op_manager {
            if op_manager.ring.is_contract_broken(key) {
                return;
            }
        }

        // Probe ONLY when every input is `UpdateData::State(...)`. Three
        // reasons for this conservative gating:
        //
        // 1. `Delta` inputs are NOT contractually required to be
        //    idempotent. An "increment by 1" delta produces S+1 then S+2
        //    on re-apply — that's a CmRDT-shaped contract, perfectly
        //    valid, but `update_state(update_state(S, U), U) != update_state(S, U)`
        //    in the exact byte sense the probe checks. Probing Delta
        //    inputs would mass-flag legitimate counter / append-log
        //    contracts. Skip them entirely.
        //
        // 2. `StateAndDelta` carries a delta too, with the same risk.
        //
        // 3. `RelatedState` is a cross-contract hint, not a CRDT op
        //    over this contract's state — re-applying isn't required
        //    to be a no-op even for a correct contract.
        //
        // State-only batches are the unambiguous case: receiving the
        // same full state twice MUST produce the same merged result by
        // CvRDT lattice-join semantics. That's the invariant the probe
        // tests.
        let all_state =
            !updates.is_empty() && updates.iter().all(|u| matches!(u, UpdateData::State(_)));
        if !all_state {
            return;
        }

        // Sampling. `GlobalRng` honors a fixed seed in simulation tests
        // (see `crate::config::GlobalRng`), so determinism is preserved
        // under the existing test harness — but the call ordering would
        // shift if existing tests don't expect a new RNG consumer on the
        // merge path. The probe runs only on pure-State batches (above),
        // which the simulation harness drives explicitly and rarely, so
        // the RNG stream perturbation is bounded.
        if !crate::config::GlobalRng::random_bool(IDEMPOTENCY_PROBE_PROBABILITY) {
            return;
        }

        // Cost telemetry (cost-aware eviction, #4861): the probe is a full
        // extra WASM `update_state` invocation, and it fires precisely on the
        // storm-relevant class (frequently-merging contracts — including
        // non-idempotent ones). Attribute its elapsed on the same
        // `ExecCpuMicros` axis as the main apply in `attempt_state_update`.
        let probe_clock = self
            .op_manager
            .as_ref()
            .map(|op_manager| op_manager.ring.time_source.clone());
        let probe_started = probe_clock.as_ref().map(|clock| clock.now());
        let probe_result = self
            .runtime
            .update_state(key, parameters, post_merge_state, updates);
        if let (Some(op_manager), Some(clock), Some(started)) = (
            self.op_manager.as_ref(),
            probe_clock.as_ref(),
            probe_started,
        ) {
            let elapsed = clock.now().saturating_duration_since(started);
            op_manager.ring.report_contract_resource_usage(
                *key.id(),
                crate::topology::meter::ResourceType::ExecCpuMicros,
                elapsed.as_micros() as f64,
            );
        }
        let probe_outcome = match probe_result {
            Ok(modification) => modification,
            Err(err) => {
                // The probe failing (timeout, trap, etc.) is not a positive
                // signal — the contract is exercising some other failure
                // mode that doesn't necessarily imply non-idempotency.
                // Log at DEBUG and bail without flagging.
                tracing::debug!(
                    contract = %key,
                    error = %err,
                    event = "idempotency_probe_error",
                    "Idempotency probe failed to execute; skipping detection"
                );
                return;
            }
        };
        let UpdateModification {
            new_state: probe_state,
            ..
        } = probe_outcome;
        let Some(probe_state) = probe_state else {
            // No state output from probe (e.g. contract returned only
            // `requires(...)`). Inconclusive — bail.
            return;
        };
        let probe_state = WrappedState::new(probe_state.into_bytes());

        // Byte-identical re-application: definitively idempotent. Fast path
        // for contracts with canonical serialization.
        if probe_state.as_ref() == post_merge_state.as_ref() {
            return;
        }

        // Bytes differ — but byte inequality alone does NOT prove a CRDT
        // violation. A correct, logically-idempotent merge can still emit
        // byte-different output for the SAME logical state when the contract's
        // serialization is non-canonical (HashMap/HashSet iteration order):
        // the re-serialized state is a REORDERING of the same bytes. Flagging
        // on that byte flutter false-positives correct contracts and — because
        // the broken-invariant flag gates ALL propagation — silently bricks
        // them. That was the root cause of #4295: the ping contract's
        // `HashMap`-backed state re-serialized in non-deterministic key order.
        //
        // Distinguish a benign reordering from a genuine content change by
        // comparing the byte MULTISET (not the bytes, not just the size).
        // Reordering preserves the multiset; a real non-idempotent merge
        // changes content (a counter, timestamp, signature, added/removed
        // entry), which changes the multiset — INCLUDING the fixed-size
        // byte-churn shape of the #4251/#4279 production violator that a
        // size-only check would miss.
        if byte_multiset_eq(post_merge_state.as_ref(), probe_state.as_ref()) {
            tracing::debug!(
                contract = %key,
                size = post_merge_state.size(),
                event = "idempotency_probe_byte_flutter_ignored",
                "Idempotency probe saw byte-different but same-multiset re-application \
                 (serialization reordering); treating as benign, not a violation"
            );
            return;
        }

        if let Some(op_manager) = &self.op_manager {
            tracing::warn!(
                contract = %key,
                post_merge_size = post_merge_state.size(),
                probe_size = probe_state.size(),
                event = "non_idempotent_merge_detected",
                "Contract violates update_state idempotency: re-application changes \
                 state content (different byte multiset, not a reordering). \
                 Flagging contract; outbound BroadcastStateChange will be suppressed."
            );
            op_manager
                .ring
                .record_broken_invariant(*key, crate::ring::BrokenInvariant::NonIdempotent);
        }
    }

    /// Deterministic identical-input idempotency check — the zero-sampling
    /// complement to [`Self::maybe_probe_idempotency`].
    ///
    /// Precondition (enforced by the caller): the incoming full-`State`
    /// payload is byte-identical to the stored state, so the #4151 fast
    /// path in `bridged_upsert_contract_state` is about to return
    /// `NoChange` without invoking WASM. For a CORRECT contract,
    /// `update_state(S, State(S))` must reach a FIXPOINT (CvRDT lattice
    /// join: `S ⊔ S = S`) — possibly after one canonicalization step, see
    /// below. If re-applying the contract's own output to itself keeps
    /// changing the byte MULTISET across [`IDENTITY_PROBE_MAX_APPLIES`]
    /// successive merges, the contract is PROVEN non-idempotent — no
    /// sampling, and no staleness ambiguity, because every merge input is
    /// the contract's own state. This is the self-echo shape of the
    /// production broadcast storm (a junk contract that mutates on every
    /// apply — #4251/#4279); the sampled probe only catches it at 1/32 per
    /// merge, which across many co-hosts leaves the echo alive.
    ///
    /// Why a fixpoint SEQUENCE and not a single re-apply: a correct
    /// CANONICALIZING contract normalizes a raw non-canonical state once —
    /// e.g. the stored state came from a fresh PUT (the install path stores
    /// the client's raw bytes without running `update_state`), and the
    /// first merge rewrites it into canonical form. That first re-apply is
    /// a genuine content change, but the contract then STABILIZES:
    /// re-applying the canonical output yields itself. Flagging on the
    /// first change alone would false-flag every such contract. So the
    /// probe iterates: it only flags when the output NEVER stabilizes
    /// (each of the successive re-applies changes the multiset again).
    ///
    /// Cost control: the #4151 short-circuit exists precisely because
    /// identical re-pushes are the DOMINANT dedup-miss case, so re-running
    /// the merge on EVERY one would reintroduce the per-push WASM cost
    /// that fix removed. Instead the check claims a per-contract cooldown
    /// slot (`Ring::try_claim_identity_probe`,
    /// `IDENTITY_PROBE_COOLDOWN` = 60 s): detection stays DETERMINISTIC —
    /// a violating contract is caught on the first identical apply after
    /// each cooldown, not probabilistically. Per cooldown window a healthy
    /// contract pays one extra merge (its first re-apply is already a
    /// fixpoint), a canonicalizing contract two, and only a genuinely
    /// churning contract the full [`IDENTITY_PROBE_MAX_APPLIES`].
    ///
    /// The #4295 reorder exemption applies unchanged at every step:
    /// byte-different but same-multiset output (serialization-order
    /// flutter) counts as stabilized, NOT a violation. A merge error is
    /// inconclusive, not a positive signal — a correct contract may
    /// legitimately REJECT a same-version push (`InvalidUpdateWithInfo`,
    /// the #4151 log-spam case).
    fn probe_identical_input_idempotency(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        current_state: &WrappedState,
    ) {
        let Some(op_manager) = self.op_manager.clone() else {
            // No ring wired (mock/local harness without an OpManager):
            // nothing to flag against.
            return;
        };
        if op_manager.ring.is_contract_broken(key) {
            // Already flagged; the NoChange fast path is itself the
            // suppression. Nothing new to learn.
            return;
        }
        if !op_manager.ring.try_claim_identity_probe(key) {
            return; // within cooldown — bounded cost
        }

        let mut cur = current_state.clone();
        for step in 0..IDENTITY_PROBE_MAX_APPLIES {
            let updates = [UpdateData::State(State::from(cur.as_ref().to_vec()))];
            let probe_result = self.runtime.update_state(key, parameters, &cur, &updates);
            let modification = match probe_result {
                Ok(m) => m,
                Err(err) => {
                    tracing::debug!(
                        contract = %key,
                        step,
                        error = %err,
                        event = "identity_probe_error",
                        "Identical-input idempotency probe failed to execute \
                         (e.g. same-version push rejected); inconclusive, not flagging"
                    );
                    return;
                }
            };
            let UpdateModification {
                new_state: probe_state,
                ..
            } = modification;
            let Some(probe_state) = probe_state else {
                // No state output (e.g. contract returned only
                // `requires(...)`). Inconclusive — bail.
                return;
            };
            let probe_state = WrappedState::new(probe_state.into_bytes());

            if probe_state.as_ref() == cur.as_ref() {
                // Fixpoint reached: idempotent on its own state. step > 0
                // means the contract canonicalized first — legitimate.
                if step > 0 {
                    tracing::debug!(
                        contract = %key,
                        steps_to_fixpoint = step,
                        event = "identity_probe_canonicalization_stabilized",
                        "Identical-input probe stabilized after canonicalization; \
                         benign, not a violation"
                    );
                }
                return;
            }
            if byte_multiset_eq(cur.as_ref(), probe_state.as_ref()) {
                tracing::debug!(
                    contract = %key,
                    step,
                    size = cur.size(),
                    event = "identity_probe_byte_flutter_ignored",
                    "Identical-input probe saw byte-different but same-multiset \
                     re-application (serialization reordering); benign, not a violation"
                );
                return;
            }
            cur = probe_state;
        }

        tracing::warn!(
            contract = %key,
            state_size = current_state.size(),
            final_probe_size = cur.size(),
            applies = IDENTITY_PROBE_MAX_APPLIES,
            event = "non_idempotent_identity_merge_detected",
            "Contract violates update_state idempotency on its OWN state: \
             every one of the successive identical-input re-applies changed \
             the byte multiset (no fixpoint) — deterministic proof, no \
             sampling. Flagging contract; commit, broadcast, and full-state \
             egress will be suppressed."
        );
        op_manager
            .ring
            .record_broken_invariant(*key, crate::ring::BrokenInvariant::NonIdempotent);
    }

    /// Persist an updated contract state via `state_store.update`.
    ///
    /// This is the canonical chokepoint for UPDATE-shaped writes: every
    /// in-place state update funnels through here. Bumping the per-contract
    /// state-write generation immediately after the store succeeds is what
    /// closes the EvictContract re-host race for UPDATE — see
    /// `RuntimePool::remove_contract`.
    pub(super) async fn commit_state_update(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        new_state: &WrappedState,
    ) -> Result<(), ExecutorError> {
        // Blanket gate: a contract flagged as violating a CRDT invariant
        // (e.g. non-idempotent merge) must not have its state extended
        // OR broadcast from this node. The merge in
        // `bridged_upsert_contract_state` already short-circuits before
        // reaching here on the probe-positive path, but
        // `commit_state_update` has other call sites (related-contract
        // retry, validation re-attempt) that don't run the probe first.
        // Gating here covers all of them with one check. See
        // `crate::ring::broken_invariants` for the tracker and #4279
        // for the storm shape this defends against.
        if let Some(op_manager) = &self.op_manager {
            if op_manager.ring.is_contract_broken(key) {
                tracing::debug!(
                    contract = %key,
                    event = "commit_suppressed_broken_contract",
                    "Skipping commit_state_update for contract flagged as broken"
                );
                return Ok(());
            }
        }

        let state_size = new_state.as_ref().len();
        if state_size > MAX_STATE_SIZE {
            crate::contract::record_state_size_rejection(
                crate::contract::StateSizeRejectionStage::PostMergeCommit,
                state_size,
            );
            tracing::warn!(
                contract = %key,
                size_bytes = state_size,
                limit_bytes = MAX_STATE_SIZE,
                "Rejecting oversized contract state at executor layer"
            );
            return Err(ExecutorError::request(StdContractError::Update {
                key: *key,
                cause: format!(
                    "state size {state_size} bytes exceeds maximum allowed {MAX_STATE_SIZE} bytes"
                )
                .into(),
            }));
        }

        // Disk-budget admission gate (#4683): UPDATE is a mutation of an
        // already-hosted, already-counted footprint, NOT a new admission. Use the
        // GROWTH-ONLY check: a shrinking or size-holding CRDT merge (`delta <= 0`)
        // is admitted unconditionally, even when the aggregate is over budget —
        // rejecting it would stall convergence without freeing any bytes, and a
        // relayed UPDATE rejection is silently dropped (fire-and-forget, no
        // `UpdateMsg::Error`), so no one would learn of the stall. Only genuine
        // growth is subjected to the aggregate bound. Fresh PUTs keep the hard
        // `admit_state_write` gate (they are where new footprints enter and carry
        // `PutMsg::Error` propagation). Nothing has landed, so no rollback needed.
        if let Some(op_manager) = &self.op_manager {
            if let Err(over) = op_manager.ring.admit_state_update(key, state_size) {
                tracing::warn!(
                    contract = %key,
                    %over,
                    "Rejecting UPDATE: disk budget exceeded (growth over budget)"
                );
                return Err(ExecutorError::request(StdContractError::Update {
                    key: *key,
                    cause: over.to_string().into(),
                }));
            }
        }

        self.state_store
            .update(key, new_state.clone())
            .await
            .map_err(ExecutorError::other)?;
        // State-write chokepoint (UPDATE): delegate the bump + refresh +
        // report side effects to `Ring::commit_state_write`. See its
        // rustdoc and `RuntimePool::remove_contract` for the EvictContract
        // re-host race this closes; the report leg feeds the governance
        // scoring layer (`docs/design/contract-hardening.md` — Phase 3).
        if let Some(op_manager) = &self.op_manager {
            op_manager.ring.commit_state_write(key, state_size);
        }

        tracing::debug!(
            contract = %key,
            new_size_bytes = new_state.as_ref().len(),
            phase = "update_complete",
            "Contract state updated"
        );

        // Record update timestamp for dashboard display. No-op if we're
        // not subscribed (e.g., a relay forwarding an UPDATE for a
        // contract this peer doesn't track).
        if let Some(op_manager) = &self.op_manager {
            op_manager.ring.record_contract_update(key);
        }

        if let Err(err) = self
            .send_update_notification(key, parameters, new_state)
            .await
        {
            tracing::error!(
                contract = %key,
                error = %err,
                phase = "notification_failed",
                "Failed to send update notification"
            );
        }

        // Notify subscribed delegates about the state change
        self.send_delegate_contract_notifications(key, new_state);

        if let Some(op_manager) = &self.op_manager {
            // Skip the broadcast entirely if this contract has been flagged
            // as violating a CRDT invariant (e.g. non-idempotent
            // `update_state`). The idempotency probe in
            // `bridged_upsert_contract_state` sets this flag when it
            // catches `update_state(update_state(S, U), U) != update_state(S, U)`.
            // Once flagged, propagating this contract's state changes
            // re-engages the broadcast storm we are trying to suppress.
            // See `crate::ring::broken_invariants`.
            if op_manager.ring.is_contract_broken(key) {
                tracing::debug!(
                    contract = %key,
                    event = "broadcast_suppressed_broken_contract",
                    "Skipping BroadcastStateChange for contract flagged as broken"
                );
            } else if let Err(err) =
                op_manager.try_notify_node_event(crate::message::NodeEvent::BroadcastStateChange {
                    key: *key,
                    new_state: new_state.clone(),
                    is_retry: false,
                    is_reemit: false,
                })
            {
                // Non-blocking emit: a 30-second `notify_node_event(...).await`
                // on this commit path was the primary back-pressure source
                // that wedged both gateways on 2026-05-24 (#4145). Missed
                // broadcasts heal via the next UPDATE or via summary-mismatch
                // SyncStateToPeer rounds — the executor must not stall here.
                //
                // Best-effort by design (see comment block above and
                // #4145): a missed broadcast heals via the next UPDATE
                // or summary-mismatch SyncStateToPeer round. Per-
                // occurrence WARN here flooded gateways under fan-out
                // at the same rate as the helper-internal log it
                // mirrored (#4238). The rate-limited `notify_node_event:
                // Notification channel full for too long` ERROR in
                // op_state_manager.rs is the sustained-back-pressure
                // alert operators should grep for.
                tracing::debug!(
                    contract = %key,
                    error = %err,
                    "Failed to broadcast state change to network peers (best-effort)"
                );
            }
        }

        Ok(())
    }

    /// Send notifications to delegates subscribed to a contract's state changes.
    ///
    /// Checks the global `DELEGATE_SUBSCRIPTIONS` registry and sends a
    /// `DelegateNotification` for each subscribed delegate through the channel.
    ///
    /// This is a **best-effort, lossy** notification path: if the bounded channel
    /// is full, notifications are dropped rather than blocking the commit path.
    /// Delegates that require guaranteed delivery should poll contract state
    /// periodically as a fallback.
    fn send_delegate_contract_notifications(&self, key: &ContractKey, new_state: &WrappedState) {
        let tx = match &self.delegate_notification_tx {
            Some(tx) => tx,
            None => return,
        };

        let instance_id = *key.id();
        // Snapshot subscribers and release the DashMap read-lock before sending
        let subscribers: Vec<DelegateKey> = {
            let entry = crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS.get(&instance_id);
            match entry {
                Some(ref s) if !s.is_empty() => s.iter().cloned().collect(),
                _ => return,
            }
        };

        tracing::debug!(
            contract = %key,
            subscriber_count = subscribers.len(),
            "Sending delegate contract notifications"
        );

        // Share one Arc allocation across all subscribers
        let shared_state = Arc::new(new_state.clone());

        for delegate_key in subscribers {
            match tx.try_send(super::DelegateNotification {
                delegate_key: delegate_key.clone(),
                contract_id: instance_id,
                new_state: Arc::clone(&shared_state),
            }) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    static DROPPED: AtomicUsize = AtomicUsize::new(0);
                    let total = DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
                    tracing::warn!(
                        contract = %key,
                        delegate = %delegate_key,
                        total_dropped = total,
                        "Delegate notification channel full — notification dropped"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    tracing::warn!(
                        contract = %key,
                        "Delegate notification channel closed — removing stale subscriptions"
                    );
                    // Receiver is gone; clean up all subscriptions for this contract
                    // to prevent repeated failed sends on future state updates.
                    crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS.remove(&instance_id);
                    return;
                }
            }
        }
    }

    /// Validate a contract's state, automatically fetching related contracts if requested.
    ///
    /// Depth=1 only: the original contract may request related contract states via
    /// `RequestRelated`. Those related contracts are fetched locally and
    /// `validate_state` is called exactly once more. If the second call also returns
    /// `RequestRelated`, that's an error — contracts must declare all dependencies in
    /// one round.
    ///
    /// # Return value
    /// On success, returns only `Valid` or `Invalid`, never `RequestRelated`.
    /// `RequestRelated` is resolved internally or converted to an error.
    ///
    /// # Safety limits
    /// - At most `MAX_RELATED_CONTRACTS_PER_REQUEST` (10) related contracts
    /// - Self-reference (requesting own ID) is rejected
    /// - Empty `RequestRelated` is rejected
    /// - Overall timeout of `RELATED_FETCH_TIMEOUT` (10s)
    async fn fetch_related_for_validation(
        &mut self,
        key: &ContractKey,
        params: &Parameters<'_>,
        state: &WrappedState,
        initial_related: &RelatedContracts<'_>,
        defer_related_fetch: bool,
    ) -> Result<ValidateResult, ExecutorError> {
        let result = self
            .runtime
            .validate_state(key, params, state, initial_related)
            // #4864 round-7 (Codex P1): a validation-phase WASM error (a runaway
            // `validate_state` that blows the deadline, or a queue-saturation
            // scheduler timeout) must classify exactly like a merge-phase error.
            // With `op = Some(Upsert(*key))` it routes through `update_exec_error`
            // → `Update{cause: "execution error: ..."}`, so `is_wasm_timeout` /
            // `is_scheduler_timeout` / `is_contract_exec_rejection` all match and
            // the UPDATE driver records the backoff. `op = None` would route it to
            // `ExecutorError::other` and the driver would record NOTHING, letting a
            // contract whose runaway half is `validate_state` burn the full budget
            // per broadcast without ever backing off. `key` is in scope here, so
            // the fix is trivially correct.
            .map_err(|e| ExecutorError::execution(e, Some(InnerOpError::Upsert(*key))))?;

        let requested_ids = match result {
            ValidateResult::Valid | ValidateResult::Invalid => return Ok(result),
            ValidateResult::RequestRelated(ids) => ids,
        };

        // Reject empty requests
        if requested_ids.is_empty() {
            tracing::warn!(
                contract = %key,
                "Contract returned RequestRelated with empty list"
            );
            return Err(ExecutorError::request(StdContractError::Put {
                key: *key,
                cause: "contract requested related contracts but provided empty list".into(),
            }));
        }

        // Reject self-reference
        let self_id = key.id();
        if requested_ids.iter().any(|id| id == self_id) {
            tracing::warn!(
                contract = %key,
                "Contract requested its own state as a related contract"
            );
            return Err(ExecutorError::request(StdContractError::Put {
                key: *key,
                cause: "contract cannot request itself as a related contract".into(),
            }));
        }

        // Dedup
        let unique_ids: HashSet<ContractInstanceId> = requested_ids.into_iter().collect();

        // Reject too many
        if unique_ids.len() > MAX_RELATED_CONTRACTS_PER_REQUEST {
            tracing::warn!(
                contract = %key,
                requested = unique_ids.len(),
                limit = MAX_RELATED_CONTRACTS_PER_REQUEST,
                "Contract requested too many related contracts"
            );
            return Err(ExecutorError::request(StdContractError::Put {
                key: *key,
                cause: format!(
                    "contract requested {} related contracts, limit is {}",
                    unique_ids.len(),
                    MAX_RELATED_CONTRACTS_PER_REQUEST
                )
                .into(),
            }));
        }

        let initial_owned = initial_related.clone().into_owned();

        // `related_map` is the populated set fed to the second validate_state
        // call. It is built differently per mode (see below), but both modes
        // end at the same `populated_related` / re-validate.
        let mut related_map: HashMap<ContractInstanceId, Option<State<'static>>> =
            HashMap::with_capacity(unique_ids.len());

        if defer_related_fetch {
            // DEFERRABLE mode (serial `contract_handling` loop): resolve
            // LOCAL-ONLY — caller-supplied `initial_related` states OR the local
            // `state_store`. This path MUST NEVER call `fetch_related_via_network`
            // (which awaits a network GET inline on the serial loop). Anything
            // still unresolved is surfaced via `DeferRelated` so the caller
            // off-loads the fetch; on resume that re-enters here with the state
            // supplied, OR (if a misbehaving contract re-requests something never
            // supplied) hits the one-deferral cap → MissingRelated — never an
            // inline network GET. See #4391.
            let mut missing = Vec::new();
            for id in &unique_ids {
                if let Some(s) = initial_owned
                    .states()
                    .find_map(|(rid, s)| if rid == id { s.as_ref() } else { None })
                {
                    related_map.insert(*id, Some(s.clone().into_owned()));
                    continue;
                }
                if let Some(full_key) = self.bridged_lookup_key(id) {
                    if let Ok(state) = self.state_store.get(&full_key).await {
                        related_map.insert(*id, Some(State::from(state.as_ref().to_vec())));
                        continue;
                    }
                }
                missing.push(*id);
            }
            if !missing.is_empty() {
                return Err(ExecutorError::defer_related_fetch(missing));
            }
        } else {
            tracing::debug!(
                contract = %key,
                related_count = unique_ids.len(),
                "Fetching related contracts for validation"
            );

            // NON-deferrable mode (delegate-driven PUTs, direct callers): fetch
            // each related contract — try the local state_store first, escalate
            // to a network GET when the executor has an `op_manager` attached.
            // The previous version was local-only, which silently failed
            // cross-node UPDATE flows where the validating node was a fresh
            // receiver that hadn't yet cached the related contract (see
            // freenet/mail#80 — the recipient's inbox UPDATE always carried
            // `RequestRelated` for the sender's AFT record, which the receiver
            // hadn't seen before).
            //
            // Parallel fetch via `join_all`: previously serial under a single
            // 10s wall-clock budget, so N related ids each got ~10s/N effective.
            // Each id now races its own sub-op GET, so the budget is per-id in
            // the common case. See freenet/freenet-core#4077.
            //
            // Reborrow as `&Self` so the per-id futures share an immutable
            // borrow; the outer `&mut self` is reclaimed once `fetch_all`
            // is awaited.
            let this: &Self = &*self;
            let fetch_all = async {
                let results: Vec<(ContractInstanceId, Result<State<'static>, ExecutorError>)> =
                    futures::future::join_all(unique_ids.iter().map(|id| {
                        let id = *id;
                        async move {
                            if let Some(full_key) = this.bridged_lookup_key(&id) {
                                if let Ok(state) = this.state_store.get(&full_key).await {
                                    return (id, Ok(State::from(state.as_ref().to_vec())));
                                }
                            }
                            // Local lookup miss → escalate via the
                            // network-fallback helper (factored out so the
                            // per-id branch logic is testable with a stubbed
                            // fetcher). Mock executors that lack an
                            // `op_manager` get the legacy MissingRelated.
                            let outcome = fetch_related_via_network(this.op_manager.as_ref(), &id)
                                .await
                                .map(|state| State::from(state.as_ref().to_vec()));
                            (id, outcome)
                        }
                    }))
                    .await;
                for (id, res) in results {
                    related_map.insert(id, Some(res?));
                }
                Ok::<(), ExecutorError>(())
            };

            match tokio::time::timeout(RELATED_FETCH_TIMEOUT, fetch_all).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(
                        contract = %key,
                        error = %e,
                        "Failed to fetch related contracts"
                    );
                    return Err(e);
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        contract = %key,
                        timeout_secs = RELATED_FETCH_TIMEOUT.as_secs(),
                        fetched = related_map.len(),
                        total = unique_ids.len(),
                        "Timed out fetching related contracts"
                    );
                    return Err(ExecutorError::request(StdContractError::Put {
                        key: *key,
                        cause: "timed out fetching related contracts".into(),
                    }));
                }
            }
        }

        // Merge initial_related (caller-provided) with the resolved states.
        // The contract's first call saw initial_related; the second call should
        // see both the original entries and the resolved ones. (Deferrable mode
        // already inserted supplied states above; `or_insert` makes this a no-op
        // there and only fills gaps for the non-deferrable fetch path.)
        for (id, state) in initial_owned.states() {
            if let Some(s) = state {
                related_map
                    .entry(*id)
                    .or_insert_with(|| Some(s.clone().into_owned()));
            }
        }
        // Conformance capture (#5376), production path.
        //
        // Related state reaches this executor by two routes and capture used to see
        // only one. When `update_state` returns `RequestRelated`, the retry loop
        // resolves each contract and pushes it into `updates` as
        // `UpdateData::RelatedState`, so it travels inside the transition and the
        // observation in `attempt_state_update` records it. When `validate_state`
        // asks, it is answered HERE instead, used for the retry validation below, and
        // dropped — and the transition for this same operation has already been
        // observed by the time we get here, so it could not carry this.
        //
        // Missing it left contracts whose VALIDITY depends on another contract
        // unjudgeable: every replayed case dead-ends at
        // `Inconclusive::RelatedRequired`, which reads exactly like a clean result.
        //
        // There is a SECOND implementation of this same resolution,
        // `fetch_related_for_validation_network` in `contract_ops.rs`, reached only
        // from `run_local_node` — i.e. `OperationMode::Local`, which never joins the
        // ring. It carries the same call for local-mode and `fdev` runs. Both are
        // instrumented on purpose; THIS one is the path a network peer takes, and an
        // earlier version of this fix patched only the other one, which would have
        // changed nothing for any real capture.
        //
        // Costs no fetch: these states were resolved for this node's own validation,
        // local-store-first, and are already in hand. Byte-budgeted and dropped rather
        // than blocking, like every other capture path.
        if let Some(capture) = crate::conformance::capture::global() {
            let size_hint: usize = related_map
                .values()
                .flatten()
                .map(|state| state.as_ref().len())
                .sum();
            capture.observe_related_with(*key.id(), size_hint, || {
                related_map
                    .iter()
                    .filter_map(|(id, state)| state.as_ref().map(|s| (*id, s.as_ref().to_vec())))
                    .collect()
            });
        }

        let populated_related = RelatedContracts::from(related_map);
        let retry_result = self
            .runtime
            .validate_state(key, params, state, &populated_related)
            // #4864 round-7: classify the second-round validation error too (see
            // the first `validate_state` call above for the rationale).
            .map_err(|e| ExecutorError::execution(e, Some(InnerOpError::Upsert(*key))))?;

        // If the contract requests more related contracts, that's depth>1 — reject
        if let ValidateResult::RequestRelated(_) = &retry_result {
            tracing::warn!(
                contract = %key,
                "Contract returned RequestRelated after related contracts were provided (depth>1 not supported)"
            );
            return Err(ExecutorError::request(StdContractError::Put {
                key: *key,
                cause: "contract requested additional related contracts after first round (depth=1 limit exceeded)".into(),
            }));
        }

        Ok(retry_result)
    }

    /// Build an Update error for a non-Valid validation result.
    ///
    /// Used by UPDATE code paths. For PUT paths, use `validation_error_put`.
    pub(super) fn validation_error(key: ContractKey, result: ValidateResult) -> ExecutorError {
        match result {
            ValidateResult::Invalid => {
                ExecutorError::request(freenet_stdlib::client_api::ContractError::Update {
                    key,
                    cause: "invalid outcome state".into(),
                })
            }
            ValidateResult::RequestRelated(_) => {
                tracing::error!(
                    contract = %key,
                    "validation_error called with RequestRelated — expected only Invalid"
                );
                ExecutorError::request(freenet_stdlib::client_api::ContractError::Update {
                    key,
                    cause: "missing related contracts for validation".into(),
                })
            }
            ValidateResult::Valid => {
                tracing::error!(
                    contract = %key,
                    "validation_error called with Valid result — this is a bug"
                );
                ExecutorError::internal_error()
            }
        }
    }

    /// Build a Put error for a non-Valid validation result.
    ///
    /// Used by PUT code paths to preserve correct error semantics for callers.
    pub(super) fn validation_error_put(key: ContractKey, result: ValidateResult) -> ExecutorError {
        match result {
            ValidateResult::Invalid => ExecutorError::request(StdContractError::Put {
                key,
                cause: "invalid outcome state after merge".into(),
            }),
            ValidateResult::RequestRelated(_) => {
                tracing::error!(
                    contract = %key,
                    "validation_error_put called with RequestRelated — expected only Invalid"
                );
                ExecutorError::request(StdContractError::Put {
                    key,
                    cause: "missing related contracts for validation".into(),
                })
            }
            ValidateResult::Valid => {
                tracing::error!(
                    contract = %key,
                    "validation_error_put called with Valid result — this is a bug"
                );
                ExecutorError::internal_error()
            }
        }
    }

    pub(super) async fn broadcast_state_change(&self, key: ContractKey, new_state: WrappedState) {
        if let Some(op_manager) = &self.op_manager {
            // Mirror the broken-invariant gate in `commit_state_update`
            // above. Same rationale: a contract flagged as non-idempotent
            // must not be propagated.
            if op_manager.ring.is_contract_broken(&key) {
                tracing::debug!(
                    contract = %key,
                    event = "broadcast_suppressed_broken_contract",
                    "Skipping BroadcastStateChange for contract flagged as broken"
                );
                return;
            }
            // Non-blocking emit — see comment in the update path above
            // and #4145 for the wedge this prevents.
            if let Err(err) =
                op_manager.try_notify_node_event(crate::message::NodeEvent::BroadcastStateChange {
                    key,
                    new_state,
                    is_retry: false,
                    is_reemit: false,
                })
            {
                // Best-effort by design — see #4145 and the sibling
                // commit path above. Per-occurrence WARN here re-
                // introduced the #4238 spam at the caller layer even
                // after the helper-internal downgrade.
                tracing::debug!(
                    contract = %key,
                    error = %err,
                    "Failed to broadcast state change to network peers (best-effort)"
                );
            }
        }
    }

    pub(super) async fn send_update_notification(
        &mut self,
        key: &ContractKey,
        params: &Parameters<'_>,
        new_state: &WrappedState,
    ) -> Result<(), ExecutorError> {
        tracing::debug!(contract = %key, "notify of contract update");
        let key = *key;
        let instance_id = *key.id();

        // Set by the LOCAL fan-out arm when its channel-closed cleanup empties
        // the subscriber vec. The removal itself has to happen after the
        // if/else chain, because that arm holds `&mut` borrows of both maps for
        // its whole body. The shared arm does the equivalent inline (it works
        // through `Arc<DashMap>`, not `&mut self`), and leaves this false.
        let mut local_entry_became_empty = false;

        // Resolved before either fan-out arm takes its `&mut` borrows: both arms
        // hold two executor maps mutably for the whole loop, so the
        // `self.contract_exec_metrics()` accessor (which borrows ALL of `self`)
        // is unusable inside them.
        //
        // Deliberately the FIELD, not that accessor and not an `Arc` clone: this
        // borrows only `self.op_manager`, which is disjoint from
        // `self.update_notifications`, `self.subscriber_summaries` and
        // `self.runtime`, so the borrow checker allows it and the cost is a
        // pointer. An `Arc` clone here would instead charge two atomic RMWs to
        // every committed update on the zero-local-subscriber path — the
        // overwhelmingly common one for a contract this node hosts for the
        // network, which returns below without recording anything.
        //
        // Neither delta below has a cache in front of it, so they land on the
        // `uncached` arm — see `ring::contract_exec_metrics` for why the cached
        // and uncached WASM totals are separate counters.
        let exec_metrics = self
            .op_manager
            .as_ref()
            .map(|om| om.ring.contract_exec_metrics());

        if let (Some(shared_notifications), Some(shared_summaries)) = (
            self.shared_notifications.as_ref(),
            self.shared_summaries.as_ref(),
        ) {
            // Snapshot subscribers and release the DashMap read-lock before sending.
            // Kept as `Option` rather than collapsed to an empty Vec: ABSENT and
            // PRESENT-but-EMPTY need different handling below, and distinguishing
            // them here keeps the ABSENT path — the overwhelmingly common one —
            // a pure READ. Collapsing first and re-deriving the distinction from
            // a `remove_if` would take a shard WRITE lock on every committed
            // update for every contract with no local subscriber.
            let notifiers_snapshot: Option<Vec<(ClientId, mpsc::Sender<HostResult>)>> =
                shared_notifications
                    .get(&instance_id)
                    .map(|notifiers| notifiers.value().clone());

            // #4681, re-scoped by #5040: an empty snapshot is TWO distinct
            // states, and only one of them is an anomaly.
            //
            // * ABSENT entry — no local client is subscribed: either none ever
            //   was, or the last one disconnected cleanly (`RuntimePool::
            //   remove_client` drops the entry outright). This is the NORMAL
            //   steady state for a contract this node hosts for the NETWORK:
            //   the network mesh is fed by `broadcast_state_change`, not by
            //   this local fan-out, so nothing is undelivered and nothing is
            //   lost. Warning per occurrence made a routine fact read as a
            //   dropped notification and buried the real signal below — one
            //   production node logged 22,413 of these in a single day, 96.6%
            //   of them for one zero-subscriber contract (#5040). PR #4773's
            //   own reviewer note predicted exactly this ("fires on the commit
            //   path for contracts with zero local subscribers (common on
            //   relays/gateways) ... a follow-up could rate-limit it if it
            //   proves noisy").
            //
            // * PRESENT but EMPTY — a subscriber's channel closed without a
            //   Disconnect reaching the handler, so the failure `retain` below
            //   emptied the vec in place. That loss is already reported at the
            //   point of loss (one ERROR per closed channel, naming the client),
            //   so this is a backstop: it is reported ONCE and the stale entry
            //   (with its already-emptied summaries sibling) is dropped, rather
            //   than re-warning on every committed update forever. The `retain`
            //   below now also removes the entry as it empties, which is the
            //   real source fix; this arm catches anything that still slips
            //   through. Both adopt the "drop the entry when it goes empty"
            //   convention the interest/subscriber maps already use
            //   (`interest.rs`, `hosting.rs`: `remove_if(.., |_, v| v.is_empty())`).
            //
            // NOTE ON VISIBILITY: `debug!` is compiled OUT of release builds by
            // `release_max_level_info` (see crates/core/Cargo.toml), so the
            // ABSENT case below is deliberately INVISIBLE in production, not
            // merely quieter. That is intended — it is the steady state and
            // carries no operator action — but it does mean a log grep can no
            // longer distinguish "no subscriber registered" from "never
            // reached", which #4681 had relied on. See #5040.
            let notifiers_snapshot = match notifiers_snapshot {
                None => {
                    if let Some(op_manager) = self.op_manager.as_ref() {
                        op_manager.ring.record_notification_no_local_subscriber();
                    }
                    tracing::debug!(
                        %instance_id,
                        registered_contracts = shared_notifications.len(),
                        "send_update_notification: no local subscriber for contract \
                         (shared storage); nothing to deliver locally"
                    );
                    return Ok(());
                }
                Some(notifiers) if notifiers.is_empty() => {
                    // Captured BEFORE the removal below, so the count describes
                    // the moment of observation rather than the moment after
                    // cleanup (which would undercount by exactly this entry).
                    let registered_contracts = shared_notifications.len();
                    // Gate the summaries removal on the notifications removal
                    // actually happening: `remove_if` re-checks emptiness under
                    // the shard lock, so if a client registered between the
                    // snapshot above and here the entry is no longer empty, the
                    // removal declines — and we must NOT then drop that new
                    // client's summary. The next committed update serves them.
                    if shared_notifications
                        .remove_if(&instance_id, |_, notifiers| notifiers.is_empty())
                        .is_some()
                    {
                        // Unconditional, NOT `remove_if(.., is_empty)`: with no
                        // channels left under this key, every summary beneath it
                        // belongs to a client that can no longer be notified. A
                        // conditional removal would orphan exactly the realistic
                        // case, because a summaries sibling is typically still
                        // NON-empty at this point.
                        shared_summaries.remove(&instance_id);
                        tracing::warn!(
                            %instance_id,
                            registered_contracts,
                            "send_update_notification: no subscriber snapshot for contract \
                             (shared storage); stale entry dropped — the subscriber was \
                             lost on an earlier update (see the prior channel-closed ERROR)"
                        );
                    } else {
                        // The removal DECLINED: a client registered between the
                        // snapshot above and this call, so the entry is no
                        // longer empty. This update is not delivered to them
                        // (the snapshot predates their arrival); the next
                        // committed update is.
                        //
                        // Logged rather than left silent, though note this is a
                        // DEBUG-BUILD aid only: `release_max_level_info`
                        // compiles it out, so in production this path is as
                        // silent as it was. It is recorded because a registered
                        // subscriber, a committed update, and zero evidence is
                        // the #4681 shape, and a debug-build trace is better
                        // than nothing while the window stays unreachable.
                        tracing::debug!(
                            %instance_id,
                            "send_update_notification: subscriber registered concurrently with \
                             the stale-entry cleanup; this update was not delivered to it, the \
                             next one will be"
                        );
                    }
                    return Ok(());
                }
                Some(notifiers) => notifiers,
            };

            let summaries_snapshot: HashMap<ClientId, Option<StateSummary<'static>>> =
                shared_summaries
                    .get(&instance_id)
                    .map_or_else(HashMap::new, |s| s.value().clone());

            if notifiers_snapshot.len() > super::FANOUT_WARNING_THRESHOLD {
                tracing::warn!(
                    contract = %key,
                    subscriber_count = notifiers_snapshot.len(),
                    "High subscriber count for notification fan-out"
                );
            }

            let mut failures = Vec::with_capacity(32);
            // Clients whose notification was dropped because their channel was
            // full. Their cached summary is invalidated below so the NEXT
            // notification is sent as full state — see the `Full` arm (#4681).
            let mut resync_clients: Vec<ClientId> = Vec::new();
            let mut delta_computations = 0usize;
            // Pre-allocate full state once for subscribers that don't get deltas
            let full_state = State::from(new_state.as_ref()).into_owned();

            for (peer_key, notifier) in &notifiers_snapshot {
                let peer_summary = summaries_snapshot.get(peer_key).and_then(|s| s.as_ref());

                let update = match peer_summary {
                    Some(summary)
                        if delta_computations < super::MAX_DELTA_COMPUTATIONS_PER_FANOUT =>
                    {
                        delta_computations += 1;
                        if let Some(m) = exec_metrics {
                            m.record_delta_wasm_uncached();
                        }
                        self.runtime
                            .get_state_delta(&key, params, new_state, summary)
                            .map_err(|err| {
                                tracing::error!("{err}");
                                ExecutorError::execution(err, Some(InnerOpError::Upsert(key)))
                            })?
                            .to_owned()
                            .into()
                    }
                    Some(_) => {
                        // Delta computation cap reached: send full state instead of
                        // running another WASM get_state_delta() call
                        tracing::debug!(
                            client = %peer_key,
                            contract = %key,
                            "Delta computation cap reached, sending full state"
                        );
                        UpdateData::State(full_state.clone())
                    }
                    None => UpdateData::State(full_state.clone()),
                };

                match notifier.try_send(Ok(
                    ContractResponse::UpdateNotification { key, update }.into()
                )) {
                    Ok(()) => {
                        tracing::debug!(
                            client = %peer_key,
                            contract = %key,
                            phase = "notification_sent_shared",
                            "Sent update notification to client (shared storage)"
                        );
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // #4681 mechanism 2. Dropping this notification is
                        // recoverable ONLY if the client is later resent the
                        // whole state. If what we just dropped was a DELTA, the
                        // client has permanently diverged: deltas are
                        // incremental, so a missed one is never made up by
                        // subsequent deltas, and the subscriber stays silently
                        // wrong rather than merely stale.
                        //
                        // So invalidate its cached summary below. An absent or
                        // `None` summary makes the next notification full state
                        // (see the `peer_summary` match above), which resyncs
                        // the client. Deliberately NOT a blocking `send`/
                        // `send_timeout`: this runs on the serial
                        // contract-handling loop, and blocking here is the
                        // #4145 class of wedge that channel-safety.md forbids.
                        resync_clients.push(*peer_key);
                        if let Some(op_manager) = self.op_manager.as_ref() {
                            op_manager.ring.record_notification_dropped_channel_full();
                        }
                        tracing::warn!(
                            client = %peer_key,
                            contract = %key,
                            "Subscriber notification channel full — notification dropped; \
                             invalidating cached summary so the next update resyncs this \
                             client with full state"
                        );
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        failures.push(*peer_key);
                        if let Some(op_manager) = self.op_manager.as_ref() {
                            op_manager.ring.record_notification_dropped_channel_closed();
                        }
                        tracing::error!(
                            client = %peer_key,
                            contract = %key,
                            phase = "notification_send_failed_shared",
                            "Failed to send update notification to client (channel closed)"
                        );
                    }
                }
            }

            // Force a full-state resync for every client whose notification was
            // dropped by a full channel (#4681). The summary is set to `None`
            // rather than removed so the client stays registered; the
            // delta/full-state match above reads `None` as "send full state".
            if !resync_clients.is_empty() {
                if let Some(mut contract_summaries) = shared_summaries.get_mut(&instance_id) {
                    for client in &resync_clients {
                        if let Some(summary) = contract_summaries.get_mut(client) {
                            *summary = None;
                        }
                    }
                }
            }

            if !failures.is_empty() {
                if let Some(mut notifiers) = shared_notifications.get_mut(&instance_id) {
                    notifiers.retain(|(c, _)| !failures.contains(c));
                }
                if let Some(mut contract_summaries) = shared_summaries.get_mut(&instance_id) {
                    for failed_client in &failures {
                        contract_summaries.remove(failed_client);
                    }
                }
                // Drop the entry AS it empties rather than leaving `Some([])`
                // behind (#5040). The stale empty entry was otherwise pruned
                // only opportunistically (by the next `RuntimePool::
                // remove_client` from any client), so until then every
                // committed update re-warned on it and it inflated the
                // `registered_contracts` count in that warning. The loss
                // itself is already reported above, one ERROR per closed
                // channel. Separate statement so the `get_mut` guards above are
                // released before `remove_if` takes the same shard lock.
                //
                // Same gating as the check-site arm: the summaries sibling is
                // dropped only when the notifications removal actually happened,
                // and then unconditionally (any summary under a channel-less key
                // is dead).
                //
                // The gate covers a registration whose notifications-insert
                // lands BEFORE the `remove_if`. It does NOT cover one landing
                // entirely between the successful `remove_if` and the
                // `shared_summaries.remove` below — registration writes the two
                // maps in that order, so such a client ends up with a live
                // channel and no summary. That degrades to a full-state send
                // instead of a delta, never a fault, and it is unreachable
                // while both paths run on the serial contract loop.
                if shared_notifications
                    .remove_if(&instance_id, |_, notifiers| notifiers.is_empty())
                    .is_some()
                {
                    shared_summaries.remove(&instance_id);
                }

                // Decrement per-client subscription counters for failed clients
                if let Some(shared_client_counts) = &self.shared_client_counts {
                    for failed_client in &failures {
                        let remove = shared_client_counts
                            .get_mut(failed_client)
                            .map(|mut count| {
                                *count = count.saturating_sub(1);
                                *count == 0
                            })
                            .unwrap_or(false);
                        if remove {
                            shared_client_counts.remove(failed_client);
                        }
                    }
                }
            }
        } else if let Some(notifiers) = self
            .update_notifications
            .get_mut(&instance_id)
            .filter(|notifiers| !notifiers.is_empty())
        {
            // #4681: only take the fan-out path when a LIVE subscriber remains.
            // A present-but-EMPTY entry (left by the channel-closed cleanup
            // below) is routed to the `else` WARN, not silently skipped.
            let summaries = self.subscriber_summaries.get_mut(&instance_id).unwrap();

            if notifiers.len() > super::FANOUT_WARNING_THRESHOLD {
                tracing::warn!(
                    contract = %key,
                    subscriber_count = notifiers.len(),
                    "High subscriber count for notification fan-out"
                );
            }

            let mut failures = Vec::with_capacity(32);
            // See the shared arm: clients whose notification a full channel
            // dropped, whose cached summary is invalidated so the next update
            // resyncs them with full state (#4681).
            let mut resync_clients: Vec<ClientId> = Vec::new();
            let mut delta_computations = 0usize;
            // Pre-allocate full state once for subscribers that don't get deltas
            let full_state = State::from(new_state.as_ref()).into_owned();

            for (peer_key, notifier) in notifiers.iter() {
                let peer_summary = summaries.get_mut(peer_key).unwrap();
                let update = match peer_summary {
                    Some(summary)
                        if delta_computations < super::MAX_DELTA_COMPUTATIONS_PER_FANOUT =>
                    {
                        delta_computations += 1;
                        if let Some(m) = exec_metrics {
                            m.record_delta_wasm_uncached();
                        }
                        self.runtime
                            .get_state_delta(&key, params, new_state, &*summary)
                            .map_err(|err| {
                                tracing::error!("{err}");
                                ExecutorError::execution(err, Some(InnerOpError::Upsert(key)))
                            })?
                            .to_owned()
                            .into()
                    }
                    Some(_) => {
                        tracing::debug!(
                            client = %peer_key,
                            contract = %key,
                            "Delta computation cap reached, sending full state"
                        );
                        UpdateData::State(full_state.clone())
                    }
                    None => UpdateData::State(full_state.clone()),
                };

                match notifier.try_send(Ok(
                    ContractResponse::UpdateNotification { key, update }.into()
                )) {
                    Ok(()) => {
                        tracing::debug!(
                            client = %peer_key,
                            contract = %key,
                            phase = "notification_sent",
                            "Sent update notification to client"
                        );
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // #4681 mechanism 2 — see the shared arm for the full
                        // rationale. A dropped DELTA diverges the client
                        // permanently, so invalidate its summary and resync
                        // with full state on the next update.
                        resync_clients.push(*peer_key);
                        if let Some(op_manager) = self.op_manager.as_ref() {
                            op_manager.ring.record_notification_dropped_channel_full();
                        }
                        tracing::warn!(
                            client = %peer_key,
                            contract = %key,
                            "Subscriber notification channel full — notification dropped; \
                             invalidating cached summary so the next update resyncs this \
                             client with full state"
                        );
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        failures.push(*peer_key);
                        if let Some(op_manager) = self.op_manager.as_ref() {
                            op_manager.ring.record_notification_dropped_channel_closed();
                        }
                        tracing::error!(
                            client = %peer_key,
                            contract = %key,
                            phase = "notification_send_failed",
                            "Failed to send update notification to client (channel closed)"
                        );
                    }
                }
            }

            // Full-channel resync, local twin of the shared arm (#4681).
            for client in &resync_clients {
                if let Some(summary) = summaries.get_mut(client) {
                    *summary = None;
                }
            }

            if !failures.is_empty() {
                notifiers.retain(|(c, _)| !failures.contains(c));
                // Prune the dead clients' summaries too, mirroring the shared
                // arm. Without this the local path leaked one summary per lost
                // subscriber until that client's `remove_client` ran (#5040
                // review): the shared arm did it, the local arm did not, while
                // the comments claimed the two mirrored each other.
                for failed_client in &failures {
                    summaries.remove(failed_client);
                }
                // Defer the map-level removal: `notifiers` and `summaries` are
                // live `&mut` borrows here. See the flag's declaration.
                local_entry_became_empty = notifiers.is_empty();
                // Decrement per-client subscription counters for failed clients
                for failed_client in &failures {
                    if let Some(count) = self.client_subscription_counts.get_mut(failed_client) {
                        *count = count.saturating_sub(1);
                        if *count == 0 {
                            self.client_subscription_counts.remove(failed_client);
                        }
                    }
                }
            }
        } else {
            // #4681, re-scoped by #5040: mirror the shared-storage branch's
            // absent-vs-empty split (see the rationale there). Reached when the
            // entry is absent (no local subscriber — the ordinary case, and not
            // a drop) OR present-but-empty (a subscriber was lost, already
            // reported per-client as an ERROR above). The emptied entry and its
            // summaries sibling are removed together — the fan-out arm above
            // indexes `subscriber_summaries` on the strength of
            // `update_notifications` having an entry, so the two must stay
            // paired.
            let lost_subscriber_entry = self
                .update_notifications
                .get(&instance_id)
                .is_some_and(|notifiers| notifiers.is_empty());
            if lost_subscriber_entry {
                // Captured BEFORE the removal, matching the shared arm — both
                // branches must report the same thing under the same field name
                // (the count at the moment of observation, INCLUDING the entry
                // being dropped).
                let registered_contracts = self.update_notifications.len();
                self.update_notifications.remove(&instance_id);
                self.subscriber_summaries.remove(&instance_id);
                tracing::warn!(
                    %instance_id,
                    registered_contracts,
                    "send_update_notification: no subscriber snapshot for contract \
                     (local storage); update notification not delivered"
                );
            } else {
                if let Some(op_manager) = self.op_manager.as_ref() {
                    op_manager.ring.record_notification_no_local_subscriber();
                }
                tracing::debug!(
                    %instance_id,
                    registered_contracts = self.update_notifications.len(),
                    "send_update_notification: no local subscriber for contract \
                     (local storage); nothing to deliver locally"
                );
            }
        }

        // Deferred local-arm cleanup (see the flag's declaration): drop the
        // entry AS it empties, so no later update finds a stale empty vec to
        // warn about. This is the local counterpart of the shared arm's
        // in-place removal; without it the local path relied entirely on the
        // check-site backstop and warned once more than the shared path for the
        // identical sequence (#5040 review). Both maps go together — the local
        // fan-out arm indexes `subscriber_summaries` on the strength of
        // `update_notifications` having an entry.
        if local_entry_became_empty {
            self.update_notifications.remove(&instance_id);
            self.subscriber_summaries.remove(&instance_id);
        }
        Ok(())
    }
}

/// Source-scrape pins for the full-state version-gate invariant
/// (HQk7 fork investigation).
///
/// Production observed a fork-oscillation storm where a contract's stored
/// state flip-flopped between two divergent full states (2180 ↔ 1952 bytes)
/// on every resync apply. The investigation verified that the CORE side is
/// correct: every full-state install over EXISTING state routes through the
/// contract's own `update_state` (via `attempt_state_update`), so a contract
/// WITH a version gate keeps its higher-version state — the observed flips
/// were the contract itself accepting both forks (its `update_state` predates
/// the version gate). These pins keep that invariant true: a future refactor
/// must not add a blind `state_store` write for the resync/upsert path that
/// would bypass a well-behaved contract's version acceptance.
#[cfg(test)]
mod full_state_version_gate_pins {
    /// Slice `bridged_upsert_contract_state_inner`'s body (its signature to
    /// the next method's signature).
    fn upsert_body() -> &'static str {
        let src = include_str!("executor_impl.rs");
        let start = src
            .find("pub(in crate::contract::executor) async fn bridged_upsert_contract_state_inner(")
            .expect("bridged_upsert_contract_state_inner not found");
        let after = &src[start..];
        let end = after
            .find("pub(in crate::contract::executor) async fn bridged_summarize_contract_state(")
            .expect("next method after bridged_upsert_contract_state_inner not found");
        &after[..end]
    }

    /// `upsert_body` with `//` comment lines removed, so a pin can assert that a
    /// name does not appear in the CODE without tripping over prose that
    /// deliberately names it (the removed index writer is discussed in a comment
    /// right where it used to be called).
    ///
    /// This strips whole-line `//` comments only — not block comments, not trailing
    /// comments, not `//` inside a string literal. That narrowness is deliberate
    /// rather than an oversight: every gap in it produces a false FAILURE, never a
    /// false pass, because a real call's identifier can never sit on a line whose
    /// `trim_start()` begins with `//`. So the filter is sound in the direction that
    /// matters and does not need to become a lexer. There are no block or trailing
    /// comments in the scraped region today; if someone adds one naming a forbidden
    /// symbol, the pin fails with a confusing message, which is why the assertion
    /// text says how to word it.
    fn upsert_code_only() -> String {
        upsert_body()
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// A full state over EXISTING state must reach the contract's
    /// `update_state` (the contract's version acceptance is the ONLY version
    /// oracle core has), and the only WASM-merge bypass writing an initial
    /// state must be gated on the contract being NEW.
    #[test]
    fn full_state_over_existing_state_runs_contract_update_state() {
        let body = upsert_body();

        // The incoming full state becomes an UpdateData::State merge input…
        let updates_pos = body
            .find("vec![UpdateData::State(incoming_state.clone().into())]")
            .expect("full state must be wrapped as UpdateData::State for the WASM merge");
        // …consumed by attempt_state_update (which invokes update_state).
        let merge_pos = body
            .find(".attempt_state_update(&params, &current_state, &key, &updates)")
            .expect("bridged upsert must merge via attempt_state_update");
        assert!(
            updates_pos < merge_pos,
            "the full-state update input must feed attempt_state_update \
             (updates {updates_pos} < merge {merge_pos})"
        );

        // The direct initial-state store must be inside the is_new_contract
        // branch: the gate check must precede the single `.store(` call.
        let is_new_pos = body
            .find("if is_new_contract {")
            .expect("is_new_contract gate not found");
        let store_pos = body
            .find(".store(key, state_to_store, params.clone())")
            .expect("initial-state store call not found");
        assert!(
            is_new_pos < store_pos,
            "the direct state_store write must be gated on is_new_contract \
             (gate {is_new_pos} < store {store_pos}) — a blind store for an \
             existing contract would bypass the contract's version gate \
             (the HQk7 fork-oscillation failure class)"
        );
        assert_eq!(
            body.matches(".store(key,").count(),
            1,
            "exactly one direct state_store.store call is allowed in the \
             upsert path (the is_new_contract initial install)"
        );
    }

    /// The "code already stored" branch must route through `store_contract`,
    /// the store's one guarded ingress, and must not write the durable
    /// instance→code index by any other means.
    ///
    /// This branch is the COMMON path, not an edge case: any contract reusing an
    /// already-stored binary lands here, which is every River room after the
    /// first. It used to call `ContractStore::ensure_key_indexed`, which wrote
    /// that durable row from a bare `&ContractKey` — no code, no parameters — so
    /// it could verify neither the identity it was filing nor that the blob it
    /// pointed at existed. Both gaps were found the same day
    /// (`verify_contract_identity` for the first, #5280 for the second), which is
    /// what makes this structural rather than two coincidences.
    ///
    /// Reverting the call site alone no longer compiles, since
    /// `ContractStoreBridge` has no index-writing method any more — this pin
    /// covers the case where someone restores the trait method too.
    #[test]
    fn already_stored_branch_routes_through_the_guarded_store_ingress() {
        let body = upsert_code_only();

        assert!(
            !body.contains("ensure_key_indexed"),
            "the upsert path must not write the instance→code index directly; \
             route through store_contract, which verifies the key against the \
             code and parameters first (see ContractStore::verify_contract_identity). \
             If you are seeing this because you MENTIONED the old helper in prose \
             rather than called it: use a `//` line — only whole-line `//` comments \
             are stripped, so block comments and string literals still match."
        );

        // Slice the branch's OWN region — anchor to its `else if`, and stop at the
        // `} else {` that closes it. Searching from the anchor to the end of the
        // function would only prove "a store_contract call exists somewhere at or
        // after this branch", which an unconditional call hoisted out of the
        // branch satisfies just as well. That is not the property being pinned.
        const ANCHOR: &str = "} else if let Some(ref contract_code) = code {";
        let branch_start = body
            .find(ANCHOR)
            .expect("the 'code already stored' branch is not where this pin expects it");
        let after_anchor = branch_start + ANCHOR.len();
        let branch_end = body[after_anchor..]
            .find("} else {")
            .map(|offset| after_anchor + offset)
            .expect("the already-stored branch must be closed by an else arm");
        let branch = &body[branch_start..branch_end];

        assert!(
            branch.contains(".store_contract(contract_code.clone())"),
            "the already-stored branch must index the new instance by calling \
             store_contract INSIDE the branch — its fast paths do exactly that \
             work once the identity is verified"
        );
        // Both `store_contract` calls in this body are legitimate: the new-code
        // branch and this one. A third needs thought, and a call hoisted out of
        // the branch would push this to three.
        assert_eq!(
            body.matches(".store_contract(").count(),
            2,
            "expected exactly two store_contract calls in the upsert path: the \
             new-code branch and the already-stored branch"
        );
    }

    /// The corrupted-state recovery path (the ONE branch that replaces
    /// existing state without a successful WASM merge) must stay gated on the
    /// LOCAL state failing `validate_state` (#3109). Without that gate, a
    /// merge rejection of a stale incoming full state would "recover" by
    /// installing it — exactly the lower-version-overwrites-higher bypass
    /// this module pins against.
    #[test]
    fn corrupted_state_recovery_requires_invalid_local_state() {
        let body = upsert_body();

        let local_valid_pos = body
            .find("let local_valid = self")
            .expect("recovery path must validate the LOCAL state (#3109)");
        let keep_local_pos = body
            .find("if local_valid {")
            .expect("recovery must return the merge error when local state is valid");
        let recovery_pos = body
            .find("recovery_performed = true;")
            .expect("corrupted-state recovery marker not found");
        assert!(
            local_valid_pos < keep_local_pos && keep_local_pos < recovery_pos,
            "recovery ordering must be: validate local ({local_valid_pos}) < \
             keep-local-when-valid ({keep_local_pos}) < recovery ({recovery_pos}) — \
             recovery may only replace state the contract itself calls invalid"
        );
    }
}

/// Source-scrape pins for the conformance capture hook (RFC #5320).
///
/// Capture sits on the merge path, which is the hottest path a contract touches.
/// Two properties keep it safe to run on a live node, and neither is visible from
/// the capture module's own tests, because both are facts about the CALL SITE:
///
/// 1. it observes where the transition actually is, inside `attempt_state_update`;
/// 2. it never blocks the executor.
///
/// A refactor that "tidied" the `observe` call into an `await`, or moved it to a
/// path that cannot see the base state, would leave every capture-module test green
/// while making the node liable to stall behind a diagnostic writer. That is the
/// #4145 / #4466 shape, and `.claude/rules/channel-safety.md` exists because it has
/// happened repeatedly.
#[cfg(test)]
mod conformance_capture_pins {
    /// Slice `attempt_state_update`'s body, from its signature to the next
    /// function's. A missing anchor panics rather than silently widening the region
    /// to the rest of the file, which is how a source pin quietly stops testing
    /// anything (see the `include_str!` note in
    /// `.claude/rules/bug-prevention-patterns.md`).
    fn attempt_state_update_body() -> &'static str {
        let src = include_str!("executor_impl.rs");
        let start = src
            .find("    pub(super) async fn attempt_state_update(")
            .expect("attempt_state_update not found");
        let after = &src[start..];
        let end = after
            .find("    async fn maybe_probe_idempotency(")
            .expect("maybe_probe_idempotency no longer follows attempt_state_update");
        &after[..end]
    }

    /// Capture must read the transition from the merge path itself. Recording it
    /// anywhere else means recording something other than what the contract did.
    #[test]
    fn capture_observes_from_the_merge_path() {
        let body = attempt_state_update_body();
        assert!(
            body.contains("capture.observe_with("),
            "conformance capture is no longer invoked from attempt_state_update, so \
             captured corpora would no longer reflect the merges the node performs"
        );

        // Bound the field check to the `Observation` literal itself.
        //
        // Searching the whole function body was vacuous for `incoming_state`: the
        // name also appears in the `let (incoming_state, delta) = ...` binding that
        // computes it, so deleting the FIELD left the assertion green while the
        // bundle silently lost half the transition. This is the failure mode
        // `AGENTS.md` warns about for source-scrape pins, and it is why the region
        // has to be bounded to the thing being pinned rather than to its file.
        let literal = body
            .split_once("Observation {")
            .expect("the capture hook no longer constructs an Observation literal")
            .1;
        let literal = literal
            .split_once("});")
            .expect("could not find the end of the Observation literal")
            .0;

        // `related,` included deliberately: without it, a refactor that drops the
        // field or hardcodes an empty vec reverts related-contract capture entirely
        // while every pin stays green — the same failure this pin's own history
        // records for `incoming_state`.
        for field in [
            "base_state:",
            "result_state:",
            "incoming_state,",
            "delta,",
            "related,",
        ] {
            assert!(
                literal.contains(field),
                "capture no longer records `{field}` from the merge path; a replay \
                 bundle missing part of the transition cannot reproduce it"
            );
        }
    }

    /// Nothing may be copied before the byte budget is checked.
    ///
    /// The sampler side of this is already pinned: `observe_with` provably does not
    /// invoke its builder once the queue or byte budget is exhausted
    /// (`a_full_queue_skips_building_the_observation_entirely`). What that test cannot
    /// see is the CALL SITE. An earlier version of this hook computed the incoming
    /// state, delta and related payloads BEFORE calling `observe_with`, so the copies
    /// happened unconditionally whenever capture was enabled — queue full or not —
    /// while the comment above them claimed the opposite. Moving them back would
    /// restore that bug with every existing test green, including both pins in this
    /// module, because the `Observation` literal would be unchanged and the hook would
    /// still not await.
    ///
    /// Related state is the reason this matters more since related-contract capture:
    /// it can carry another contract's entire state, so the drop path would pay the
    /// largest copy of the three, on the merge path, under exactly the load that
    /// causes drops.
    #[test]
    fn nothing_is_copied_before_the_budget_check() {
        let body = attempt_state_update_body();
        let hook_start = body
            .find("if let Some(capture) =")
            .expect("capture hook not found in attempt_state_update");
        let hook = &body[hook_start..];
        let (before_budget, inside_closure) = hook
            .split_once("capture.observe_with(")
            .expect("the capture hook no longer routes through observe_with");

        for alloc in [".to_vec()", ".to_owned()", ".clone()"] {
            assert!(
                !before_budget.contains(alloc),
                "the capture hook calls `{alloc}` before `observe_with`, so the copy \
                 happens whether or not a queue slot and byte budget are secured. \
                 Measure lengths from the slices already held, and do every \
                 allocation inside the `observe_with` closure, which runs only after \
                 the budget check"
            );
        }

        // Guard against passing vacuously: if the copies were deleted outright rather
        // than moved, the loop above would also be satisfied. Both spellings count,
        // so a refactor from one to the other does not fail here claiming the copies
        // are gone — which is what the first version of this guard did.
        assert!(
            inside_closure.contains(".to_vec()") || inside_closure.contains(".to_owned()"),
            "no copies remain inside the `observe_with` closure, so this pin would \
             pass for a hook that records nothing at all"
        );
    }

    /// The executor must never wait on capture. A slow or stuck writer would
    /// otherwise stall contract synchronization, which is the one thing this path
    /// is required never to do.
    #[test]
    fn capture_never_awaits_on_the_merge_path() {
        let body = attempt_state_update_body();
        let hook_start = body
            .find("if let Some(capture) =")
            .expect("capture hook not found in attempt_state_update");
        let rest = &body[hook_start..];
        let hook_end = rest
            .find("\n        }")
            .expect("capture hook block not delimited as expected");
        let hook = &rest[..hook_end];
        assert!(
            !hook.contains(".await"),
            "the conformance capture hook awaits on the merge path. It must not: a \
             slow or stuck writer would then stall contract synchronization. Use \
             `try_send` and drop on full, per .claude/rules/channel-safety.md"
        );
    }
}

/// Source-scrape pin for the contract-exec WASM counters' PRODUCTION liveness.
///
/// The failure this guards against already happened once, and is the whole
/// reason this instrumentation exists: a counter sat on exactly the right line
/// for a year and was a no-op in the field, because the only sink it wrote to
/// (`topology_registry`) keys on a thread-local that `SimNetwork` sets and a
/// production node never does. The simulation asserted the invariant held, the
/// field suggested it did not, and the counter that would have adjudicated was
/// switched off precisely where it mattered.
///
/// Two sinks now share the site. A runtime test cannot cover both — the unit
/// fixtures have no bound listener, so `get_own_addr()` returns `None` and the
/// simulation sink never fires there — so the ordering invariant is pinned from
/// source instead: the PRODUCTION record must not be nested inside the
/// `get_own_addr()` guard, which is exactly the shape that would re-create the
/// original bug.
#[cfg(test)]
mod contract_exec_counter_pins {
    /// Slice `bridged_summarize_contract_state`'s CODE (its signature to the
    /// next method's signature), with comment lines removed.
    ///
    /// Both bounds matter. The slice stops a needle matching a later occurrence
    /// elsewhere in the file — including this test module's own assertion
    /// strings, which `include_str!` also pulls in. Dropping comment lines stops
    /// a needle matching the PROSE that describes the code: the first draft of
    /// `summarize_wasm_call_records_both_sinks` failed because `get_own_addr()`
    /// appears in the block comment above the call as well as in the call, and
    /// the comment came first. A pin that matches its own explanation is not
    /// pinning anything.
    fn summarize_body() -> String {
        let src = include_str!("executor_impl.rs");
        let start = src
            .find("pub(in crate::contract::executor) async fn bridged_summarize_contract_state(")
            .expect("bridged_summarize_contract_state not found");
        let after = &src[start..];
        let end = after
            .find("pub(in crate::contract::executor) async fn bridged_get_contract_state_delta(")
            .expect("next method after bridged_summarize_contract_state not found");
        after[..end]
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn summarize_wasm_call_records_both_sinks() {
        let body = summarize_body();

        let production_pos = body
            .find(".record_summarize_wasm_call();")
            .expect("the production ContractExecMetrics counter must be recorded here");
        let own_addr_pos = body
            .find("if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr()")
            .expect("the simulation sink's own-address guard must still be here");
        let sim_pos = body
            .find("topology_registry::record_summarize_wasm_call(own_addr)")
            .expect("the simulation topology_registry sink must still be recorded here");

        assert!(
            production_pos < own_addr_pos,
            "the PRODUCTION counter ({production_pos}) must be recorded BEFORE the \
             get_own_addr() lookup ({own_addr_pos}), i.e. outside its `if let Some` \
             guard. Nesting it inside would make the production counter conditional \
             on a lookup that returns None on a node with no bound listener, \
             recreating the sim-only-counter bug this instrumentation exists to fix."
        );
        assert!(
            own_addr_pos < sim_pos,
            "the simulation sink ({sim_pos}) still takes its peer address from the \
             get_own_addr() guard ({own_addr_pos})"
        );
    }

    /// The counter must sit on the WASM SLOW path: after both cache-hit early
    /// returns, and immediately before the `summarize_state` call it describes.
    /// A counter that drifted above the cache checks would count cache hits as
    /// WASM work — the exact conflation the handler-entry span already makes,
    /// and the reason its numbers could not be acted on.
    #[test]
    fn summarize_wasm_counter_sits_after_both_cache_hit_returns() {
        let body = summarize_body();

        let fast_hit_pos = body
            .find(".record_summarize_fast_hit();")
            .expect("fast-path cache-hit counter not found");
        let reload_hit_pos = body
            .find(".record_summarize_reload_hit();")
            .expect("reload-path cache-hit counter not found");
        let wasm_pos = body
            .find(".record_summarize_wasm_call();")
            .expect("WASM-call counter not found");
        let summarize_state_pos = body
            .find(".summarize_state(&key, &params, &state)")
            .expect("the WASM summarize_state call not found");

        assert!(
            fast_hit_pos < reload_hit_pos && reload_hit_pos < wasm_pos,
            "counter order must follow the path order: fast hit ({fast_hit_pos}) < \
             reload hit ({reload_hit_pos}) < WASM call ({wasm_pos})"
        );
        assert!(
            wasm_pos < summarize_state_pos,
            "the WASM counter ({wasm_pos}) must be recorded at the decision, \
             immediately before the summarize_state call it describes \
             ({summarize_state_pos})"
        );
    }
}
