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
            } else {
                // Contract already in store. ensure_key_indexed handles the case of contracts
                // that reuse the same WASM code with different parameters (e.g., different
                // River rooms). Without this, lookup_key() fails for the new instance_id.
                // See issue #2380.
                //
                // We only index when code was provided in this request (code.is_some()).
                // When code is None, this is a state-only update to an existing contract
                // that should already be indexed.
                if code.is_some() {
                    self.runtime
                        .ensure_key_indexed(&key)
                        .map_err(ExecutorError::other)?;
                }
                (false, code.is_some(), None)
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
                return Err(subscriber_limit_error(
                    instance_id,
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
                return Err(subscriber_limit_error(
                    instance_id,
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
            if let Some((cached_hash, cached_summary)) = self.summary_cache.get(&key) {
                if *cached_hash == detector_hash {
                    return Ok(cached_summary.clone());
                }
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
        if let Some((cached_hash, cached_summary)) = self.summary_cache.get(&key) {
            if *cached_hash == state_hash {
                return Ok(cached_summary.clone());
            }
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
            if let Some(cached_delta) = self.delta_cache.get(&cache_key) {
                return Ok(cached_delta.clone());
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
        if let Some(cached_delta) = self.delta_cache.get(&cache_key) {
            return Ok(cached_delta.clone());
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

        let probe_result = self
            .runtime
            .update_state(key, parameters, post_merge_state, updates);
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
            .map_err(|e| ExecutorError::execution(e, None))?;

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
        let populated_related = RelatedContracts::from(related_map);
        let retry_result = self
            .runtime
            .validate_state(key, params, state, &populated_related)
            .map_err(|e| ExecutorError::execution(e, None))?;

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

        if let (Some(shared_notifications), Some(shared_summaries)) = (
            self.shared_notifications.as_ref(),
            self.shared_summaries.as_ref(),
        ) {
            // Snapshot subscribers and release the DashMap read-lock before sending
            let notifiers_snapshot: Vec<(ClientId, mpsc::Sender<HostResult>)> =
                shared_notifications
                    .get(&instance_id)
                    .map_or_else(Vec::new, |notifiers| notifiers.value().clone());

            // #4681: a committed update found no LIVE subscriber for this
            // contract — either no map entry at all, OR an empty subscriber vec
            // left behind by the channel-closed cleanup below (`retain` removes
            // the last dead subscriber but leaves the emptied entry in the map,
            // so the next committed update snapshots `Some([])`). Both cases are
            // fully silent drops for a contract that may have had a registered
            // subscriber, so surface them with a WARN (instance + total
            // registered-contract count) instead of returning silently — a
            // notification dropped for a registered subscriber is never invisible.
            if notifiers_snapshot.is_empty() {
                tracing::warn!(
                    %instance_id,
                    registered_contracts = shared_notifications.len(),
                    "send_update_notification: no subscriber snapshot for contract \
                     (shared storage); update notification not delivered"
                );
                return Ok(());
            }

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
                        tracing::warn!(
                            client = %peer_key,
                            contract = %key,
                            "Subscriber notification channel full — notification dropped"
                        );
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        failures.push(*peer_key);
                        tracing::error!(
                            client = %peer_key,
                            contract = %key,
                            phase = "notification_send_failed_shared",
                            "Failed to send update notification to client (channel closed)"
                        );
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
                        tracing::warn!(
                            client = %peer_key,
                            contract = %key,
                            "Subscriber notification channel full — notification dropped"
                        );
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        failures.push(*peer_key);
                        tracing::error!(
                            client = %peer_key,
                            contract = %key,
                            phase = "notification_send_failed",
                            "Failed to send update notification to client (channel closed)"
                        );
                    }
                }
            }

            if !failures.is_empty() {
                notifiers.retain(|(c, _)| !failures.contains(c));
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
            // #4681: mirror the shared-storage observability for the local
            // (non-pool) executor — a committed update with no LIVE subscriber
            // for this contract (no map entry at all, OR an empty subscriber vec
            // left behind by the channel-closed cleanup above) must not vanish
            // silently.
            tracing::warn!(
                %instance_id,
                registered_contracts = self.update_notifications.len(),
                "send_update_notification: no subscriber snapshot for contract \
                 (local storage); update notification not delivered"
            );
        }
        Ok(())
    }
}
