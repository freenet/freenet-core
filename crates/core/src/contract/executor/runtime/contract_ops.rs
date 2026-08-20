//! Concrete contract operations for `Executor<Runtime>`.
//!
//! These are the local-mode PUT/UPDATE/GET/verify/reclaim implementations
//! the request dispatcher and the `ContractExecutor` trait impl delegate
//! into, plus the network-aware related-contract validation helper. The
//! generic, runtime-agnostic `bridged_*` methods live in `executor_impl.rs`;
//! this module holds the `Runtime`-specialized logic.

use super::*;

impl Executor<Runtime> {
    pub(super) async fn perform_contract_put(
        &mut self,
        contract: ContractContainer,
        state: WrappedState,
        related_contracts: RelatedContracts<'_>,
    ) -> Response {
        let key = contract.key();
        let params = contract.params();

        if self.get_local_contract(key.id()).await.is_ok() {
            // Contract already exists — merge states locally and broadcast async.
            //
            // We intentionally do NOT delegate to perform_contract_update here because
            // its network mode path uses op_request() which blocks waiting for the
            // network operation to complete (120s timeout). For client-initiated puts
            // (e.g. fdev publish), the client needs a timely response. The network
            // broadcast is fire-and-forget — if it fails, subscribers will still get
            // the update via their next sync.
            //
            // NOTE: This simplified path does not handle contracts that require related
            // contracts for update_state or validate_state. If update_state returns
            // MissingRelated (new_state=None with non-empty related), it is treated as
            // "no change". This is acceptable because no current contracts use related
            // contracts (see issue #2870 for completing that mechanism).
            let current_state = self
                .state_store
                .get(&key)
                .await
                .map_err(ExecutorError::other)?
                .clone();

            let update = UpdateData::State(state.into());
            let update_result = self
                .runtime
                .update_state(&key, &params, &current_state, &[update])
                .map_err(|err| ExecutorError::execution(err, Some(InnerOpError::Upsert(key))))?;

            // If update_state produced no new state, or the merged state is identical
            // to current, return early with the current summary (no work to persist).
            let new_state = match update_result.new_state {
                Some(s) if s.as_ref() != current_state.as_ref() => {
                    WrappedState::new(s.into_bytes())
                }
                _ => {
                    let summary = self
                        .runtime
                        .summarize_state(&key, &params, &current_state)
                        .map_err(|e| ExecutorError::execution(e, None))?;
                    return Ok(ContractResponse::UpdateResponse { key, summary }.into());
                }
            };

            // Validate before persisting (fetch related contracts from network if needed).
            // This is an existing-contract merge, not a fresh store — we don't
            // have a `ContractContainer` in hand, so fall back to the
            // fetch-from-store path (`None`).
            let validate_result = self
                .fetch_related_for_validation_network(
                    &key,
                    &params,
                    &new_state,
                    &related_contracts,
                    None,
                    // Re-PUT into an existing contract returns an UpdateResponse
                    // (this branch), so classify a validation failure as Update.
                    ValidationOpKind::Update,
                )
                .await?;
            if validate_result != ValidateResult::Valid {
                return Err(Self::validation_error_put(key, validate_result));
            }

            // Commit locally
            let written_bytes = new_state.as_ref().len();
            // Disk-budget admission gate (#4683): a re-PUT into an ALREADY-hosted
            // contract is a CRDT merge — a mutation of an already-counted
            // footprint, not a new admission. Use the GROWTH-ONLY check so a
            // shrinking/holding merge (`delta <= 0`) always admits even over
            // budget (rejecting would stall convergence without freeing bytes);
            // only genuine growth is bounded. Nothing has landed → no rollback.
            if let Some(op_manager) = &self.op_manager {
                if let Err(over) = op_manager.ring.admit_state_update(&key, written_bytes) {
                    tracing::warn!(
                        contract = %key,
                        %over,
                        "Rejecting re-PUT: disk budget exceeded (growth over budget)"
                    );
                    return Err(ExecutorError::request(StdContractError::Put {
                        key,
                        cause: over.to_string().into(),
                    }));
                }
            }
            self.state_store
                .update(&key, new_state.clone())
                .await
                .map_err(ExecutorError::other)?;
            // State-write chokepoint (re-PUT): delegate to
            // `Ring::commit_state_write` for bump + refresh + report. See
            // its rustdoc and `RuntimePool::remove_contract` for the
            // EvictContract re-host race this closes.
            if let Some(op_manager) = &self.op_manager {
                op_manager.ring.commit_state_write(&key, written_bytes);
            }

            self.send_update_notification(&key, &params, &new_state)
                .await
                .map_err(|_| {
                    ExecutorError::request(StdContractError::Put {
                        key,
                        cause: "failed while sending notifications".into(),
                    })
                })?;

            self.broadcast_state_change(key, new_state.clone()).await;

            let summary = self
                .runtime
                .summarize_state(&key, &params, &new_state)
                .map_err(|e| ExecutorError::execution(e, None))?;
            return Ok(ContractResponse::UpdateResponse { key, summary }.into());
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

        self.broadcast_state_change(key, state.clone()).await;

        Ok(ContractResponse::PutResponse { key }.into())
    }

    pub(super) async fn perform_contract_update(
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
            .map_err(ExecutorError::other)?;

        let updates = vec![update];

        // `Executor::contract_requests` is only invoked from `run_local_node`
        // (HTTP/WS local-only entry points). Network-mode UPDATEs from
        // clients arrive through `client_event_handling` →
        // `start_client_update` and never reach this function.
        if self.mode == OperationMode::Local {
            let new_state = self
                .get_updated_state(&parameters, current_state, key, updates)
                .await?;
            let summary = self
                .runtime
                .summarize_state(&key, &parameters, &new_state)
                .map_err(|e| ExecutorError::execution(e, None))?;
            return Ok(ContractResponse::UpdateResponse { key, summary }.into());
        }

        Err(ExecutorError::other(anyhow::anyhow!(
            "network UPDATE must dispatch via `start_client_update` (client_events.rs); \
             `perform_contract_update` is reachable only in local mode"
        )))
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

        // Validate before persisting or broadcasting (fetch related contracts from network if needed).
        // This is an UPDATE on an already-hosted contract, not a fresh store —
        // no `ContractContainer` in hand, so fall back to fetch-from-store (`None`).
        let result = self
            .fetch_related_for_validation_network(
                &key,
                parameters,
                &new_state,
                &RelatedContracts::default(),
                None,
                // Local UPDATE path — classify a validation failure as Update
                // (keeps the merge-failure backoff classification).
                ValidationOpKind::Update,
            )
            .await?;

        if result != ValidateResult::Valid {
            return Err(Self::validation_error(key, result));
        }
        if new_state.as_ref() != current_state.as_ref() {
            self.commit_state_update(&key, parameters, &new_state)
                .await?;
        }
        Ok(new_state)
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
        // SAFETY: `contract` is alive for the remainder of this function,
        // and `state` does not escape this scope, so the reborrowed slice
        // remains valid for its entire use.
        let state: &[u8] = unsafe { std::mem::transmute::<&[u8], &'_ [u8]>(contract.as_ref()) };
        Ok(State::from(state))
    }

    /// Verify and store a contract with depth=1 related contract resolution.
    ///
    /// 1. Store the contract code in the runtime store
    /// 2. Validate state (fetching related contracts if requested, one round only)
    /// 3. If valid, persist to state_store
    async fn verify_and_store_contract(
        &mut self,
        state: WrappedState,
        contract: ContractContainer,
        related_contracts: RelatedContracts<'_>,
    ) -> Result<(), ExecutorError> {
        let key = contract.key();
        let params = contract.params();
        let state_hash = blake3::hash(state.as_ref());

        tracing::debug!(
            contract = %key,
            state_size = state.as_ref().len(),
            state_hash = %state_hash,
            params_size = params.as_ref().len(),
            "starting contract verification and storage"
        );

        // Disk-budget admission gate for the code blob (#4683, PR 3): charge the
        // blob only if it is not already on disk (dedup — a re-PUT of existing
        // code adds nothing). Reject before storing; nothing has landed.
        //
        // Probe by CODE HASH (#4218), not by instance id via `fetch_contract`: a
        // new instance of already-stored code (same code hash, different params)
        // shares the one on-disk `.wasm` blob, so it must NOT be charged again.
        // An instance-keyed probe reported such a second instance as absent and
        // double-counted the shared blob — divergently across pool executors,
        // whose instance indexes were previously per-executor.
        let code_already_stored = self
            .runtime
            .contract_store
            .code_blob_stored(key.code_hash());
        let blob_len = contract.data().len();
        if !code_already_stored {
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
        }

        // Keep a cheap (Arc-backed) clone in hand so validation below can use
        // it directly instead of re-fetching from `contract_store` on a
        // module-cache miss (issue #2216: avoids the store→fetch round-trip).
        let contract_for_validation = contract.clone();

        // Store contract code in runtime store
        self.runtime
            .contract_store
            .store_contract(contract)
            .map_err(|e| {
                tracing::error!(
                    contract = %key,
                    error = %e,
                    "failed to store contract in runtime"
                );
                ExecutorError::other(e)
            })?;

        // Charge the newly-written blob to the disk tracker NOW (#4683), before
        // the state gate below, so the state gate sees the wasm just stored (no
        // per-PUT double-count overshoot) and a burst of distinct-code PUTs stays
        // bounded within a du-walk window. Reversed at each removal site below if
        // the PUT fails. Only when the code was newly stored (deduped away above).
        let charged_wasm: Option<usize> = if !code_already_stored {
            if let Some(op_manager) = &self.op_manager {
                op_manager.ring.record_wasm_write(blob_len);
                Some(blob_len)
            } else {
                None
            }
        } else {
            None
        };

        // Validate with depth=1 related contract resolution.
        //
        // DEPTH PROTECTION: fetch_related_for_validation enforces depth=1 —
        // if validate_state returns RequestRelated(ids), we fetch those contracts
        // and retry exactly once. A second RequestRelated is rejected as an error.
        // This prevents:
        //   - Recursive depth (related contracts requesting their own related contracts)
        //   - Amplification attacks (contract requesting new contracts on every retry)
        //   - Self-reference (contract requesting its own state)
        //   - Excessive fan-out (max 10 related contracts per request)
        // See MAX_RELATED_CONTRACTS_PER_REQUEST and RELATED_FETCH_TIMEOUT constants.
        let result = self
            .fetch_related_for_validation_network(
                &key,
                &params,
                &state,
                &related_contracts,
                Some(&contract_for_validation),
                // Fresh PUT (verify_and_store_contract) returns a PutResponse, so a
                // validation failure must surface as a Put variant, not Update
                // (#4864 round-9 item 4).
                ValidationOpKind::Put,
            )
            .await
            .inspect_err(|_| {
                if let Err(e) = self.runtime.contract_store.remove_contract(&key) {
                    tracing::warn!(contract = %key, error = %e, "failed to remove contract after validation failure");
                }
                // Reverse the wasm charge (#4683): the blob is removed.
                if let (Some(len), Some(op_manager)) = (charged_wasm, &self.op_manager) {
                    op_manager.ring.record_wasm_removed(len);
                }
            })?;

        // fetch_related_for_validation resolves RequestRelated internally,
        // so only Valid or Invalid are possible here.
        if result != ValidateResult::Valid {
            if let Err(e) = self.runtime.contract_store.remove_contract(&key) {
                tracing::warn!(contract = %key, error = %e, "failed to remove contract after invalid validation");
            }
            // Reverse the wasm charge (#4683): the blob is removed.
            if let (Some(len), Some(op_manager)) = (charged_wasm, &self.op_manager) {
                op_manager.ring.record_wasm_removed(len);
            }
            return Err(ExecutorError::request(StdContractError::Put {
                key,
                cause: "not valid".into(),
            }));
        }

        tracing::debug!(
            contract = %key,
            state_size = state.as_ref().len(),
            "storing contract state"
        );
        let written_bytes = state.as_ref().len();
        // Disk-budget admission gate for the state (#4683, PR 3): reject before
        // the store. Roll back the contract code we stored above (reuse the same
        // `remove_contract` rollback the validation-failure paths use) so a
        // rejected PUT leaves no partial state on disk.
        if let Some(op_manager) = &self.op_manager {
            if let Err(over) = op_manager.ring.admit_state_write(&key, written_bytes) {
                tracing::warn!(
                    contract = %key,
                    %over,
                    "Rejecting PUT: disk budget exceeded"
                );
                if let Err(e) = self.runtime.contract_store.remove_contract(&key) {
                    tracing::warn!(contract = %key, error = %e, "failed to remove contract after disk-budget rejection");
                }
                // Reverse the wasm charge (#4683): the blob is removed.
                if let Some(len) = charged_wasm {
                    op_manager.ring.record_wasm_removed(len);
                }
                return Err(ExecutorError::request(StdContractError::Put {
                    key,
                    cause: over.to_string().into(),
                }));
            }
        }
        self.state_store
            .store(key, state, params)
            .await
            .map_err(|e| {
                tracing::error!(
                    contract = %key,
                    error = %e,
                    "failed to store contract state"
                );
                ExecutorError::other(e)
            })?;
        // State-write chokepoint (verify_and_store PUT): delegate to
        // `Ring::commit_state_write` for bump + refresh + report. See
        // its rustdoc and `RuntimePool::remove_contract` for the
        // EvictContract re-host race this closes.
        if let Some(op_manager) = &self.op_manager {
            op_manager.ring.commit_state_write(&key, written_bytes);
        }

        Ok(())
    }

    /// Reclaim a contract's on-disk storage after it was evicted from the
    /// hosting cache.
    ///
    /// Deletes (1) the persisted state and parameters from the `StateStore`
    /// and (2) the WASM code blob from the `ContractStore`. The contract-store
    /// removal is code-hash refcount-safe: the shared `.wasm` blob is only
    /// deleted once no other contract instance references the same code.
    ///
    /// Both steps are best-effort and independent: if one fails, the other is
    /// still attempted so a partial reclaim is achieved rather than none. The
    /// method is idempotent — both `StateStore::delete` and
    /// `ContractStore::remove_contract` tolerate already-missing entries — so a
    /// double eviction is harmless.
    ///
    /// Return value:
    ///   - `Ok(ReclaimOutcome::Full)` — both halves are absent at end (either
    ///     both deleted in this call, or one was already missing and the other
    ///     was deleted, or both were already missing).
    ///   - `Ok(ReclaimOutcome::Partial)` — exactly one half failed with a real
    ///     error while the other succeeded. The caller MUST retain the
    ///     pending-reclamation entry so a future sweep retries the remaining
    ///     work. Closes the disk-leak edge case where a transient DB/FS
    ///     error in one half leaves the other half permanently leaked. See
    ///     PR #4212 review round 8.
    ///   - `Err` — BOTH halves failed; surfaced so the caller can log/retry.
    ///
    /// This is the inherent implementation; the `ContractExecutor::remove_contract`
    /// trait method delegates to it and translates the outcome into
    /// pending-reclamation management.
    pub(super) async fn reclaim_contract_storage(
        &mut self,
        key: &ContractKey,
    ) -> Result<ReclaimOutcome, ExecutorError> {
        let state_result = match self.state_store.delete(key).await {
            Ok(()) => {
                // Disk-usage accounting (#4683): drop this contract's state
                // contribution from the aggregate on-disk total. Observational
                // only in this PR. No-op until the tracker is seeded.
                if let Some(op_manager) = &self.op_manager {
                    op_manager.ring.record_state_removed(key);
                }
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    contract = %key,
                    error = %e,
                    "failed to delete persisted state while reclaiming evicted contract"
                );
                Err(())
            }
        };
        let code_result = match self.runtime.contract_store.remove_contract(key) {
            Ok(()) => {
                // Reclaim the compiled module eagerly too, not just on LRU
                // pressure. See remove_contract_module (issue #4754).
                self.runtime.remove_contract_module(key);
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    contract = %key,
                    error = %e,
                    "failed to delete WASM code while reclaiming evicted contract"
                );
                Err(())
            }
        };

        let state_ok = state_result.is_ok();
        let code_ok = code_result.is_ok();
        if !state_ok && !code_ok {
            return Err(ExecutorError::other(anyhow::anyhow!(
                "failed to reclaim any on-disk storage for contract {key}"
            )));
        }

        let outcome = if state_ok && code_ok {
            ReclaimOutcome::Full
        } else {
            ReclaimOutcome::Partial
        };
        tracing::info!(
            contract = %key,
            state_deleted = state_ok,
            code_deleted = code_ok,
            ?outcome,
            "reclaimed on-disk storage for evicted contract"
        );
        Ok(outcome)
    }
}

/// Outcome of [`Executor::reclaim_contract_storage`].
///
/// The split exists so the caller (`RuntimePool::remove_contract`) can decide
/// whether to clear the pending-reclamation entry (on `Full`) or leave it for
/// a future retry (on `Partial`). See PR #4212 review round 8.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReclaimOutcome {
    /// Both state and code are absent at end of the reclaim call.
    Full,
    /// Exactly one half failed with a real error; the other succeeded (or was
    /// already absent). The pending-reclamation entry should be retained so a
    /// future sweep retries the remaining work.
    Partial,
}

impl Executor<Runtime> {
    /// Network-aware variant retained for the PUT path.
    ///
    /// The base `fetch_related_for_validation` on the bridged impl now also
    /// escalates to network GET via `op_manager` when the local state_store
    /// lookup misses, so the two implementations are functionally equivalent
    /// for `Executor<Runtime>`. This variant is kept because it exposes the
    /// `local_state_or_from_network` helper directly and several PUT call
    /// sites already wire through it; collapsing the two would touch more
    /// surface area than the bug fix needs.
    ///
    /// `already_fetched_contract`: when the caller already holds the
    /// `ContractContainer` being validated (e.g. `verify_and_store_contract`,
    /// which just handed it to `contract_store.store_contract`), pass it here
    /// so a module-cache miss compiles directly from it instead of re-fetching
    /// the same bytes from `contract_store` (issue #2216). `None` for callers
    /// that don't have it in hand — behaves exactly as before.
    async fn fetch_related_for_validation_network(
        &mut self,
        key: &ContractKey,
        params: &Parameters<'_>,
        state: &WrappedState,
        initial_related: &RelatedContracts<'_>,
        already_fetched_contract: Option<&ContractContainer>,
        // #4864 round-9 item 4: which op this validation is running for, so a
        // validate_state exec failure classifies as the RIGHT client error —
        // `Update` for the UPDATE / re-PUT callers, `Put` for the fresh-PUT
        // `verify_and_store_contract` path (PutResponse semantics).
        validation_op: ValidationOpKind,
    ) -> Result<ValidateResult, ExecutorError> {
        // #4864 round-7 (Codex P1): classify validation-phase WASM errors (route
        // through execution so a runaway/timed-out `validate_state` reaches the
        // merge-failure backoff and carries the typed timeout provenance).
        // Round-9 item 4: the op KIND is now caller-supplied (Update vs Put) rather
        // than hardcoded Upsert, so a fresh PUT's validation error surfaces as a
        // Put variant instead of masquerading as an UPDATE error.
        let result = match already_fetched_contract {
            Some(contract) => self
                .runtime
                .validate_state_with_contract(key, params, state, initial_related, contract)
                .map_err(|e| ExecutorError::execution(e, Some(validation_op.op_for(*key))))?,
            None => self
                .runtime
                .validate_state(key, params, state, initial_related)
                .map_err(|e| ExecutorError::execution(e, Some(validation_op.op_for(*key))))?,
        };

        let requested_ids = match result {
            ValidateResult::Valid | ValidateResult::Invalid => return Ok(result),
            ValidateResult::RequestRelated(ids) => ids,
        };

        // Apply the same safety checks as the base helper
        if requested_ids.is_empty() {
            return Err(ExecutorError::request(StdContractError::Put {
                key: *key,
                cause: "contract requested related contracts but provided empty list".into(),
            }));
        }
        let self_id = key.id();
        if requested_ids.iter().any(|id| id == self_id) {
            return Err(ExecutorError::request(StdContractError::Put {
                key: *key,
                cause: "contract cannot request itself as a related contract".into(),
            }));
        }
        let unique_ids: HashSet<ContractInstanceId> = requested_ids.into_iter().collect();
        if unique_ids.len() > MAX_RELATED_CONTRACTS_PER_REQUEST {
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

        tracing::debug!(
            contract = %key,
            related_count = unique_ids.len(),
            "Fetching related contracts (with network fallback) for validation"
        );

        let mut related_map: HashMap<ContractInstanceId, Option<State<'static>>> =
            HashMap::with_capacity(unique_ids.len());

        // Parallel fetch — see fetch_related_for_validation for rationale
        // (freenet/freenet-core#4077). The serial loop here had the same
        // 10s/N effective per fetch problem.
        //
        // We can't reuse the `&mut self` `local_state_or_from_network`
        // helper across multiple concurrent futures, so the per-id body
        // is inlined: try the local state_store first, escalate to
        // `fetch_related_via_network` (which only borrows
        // `&Option<Arc<OpManager>>`). Reborrow as `&Self` so the
        // per-id futures share an immutable borrow; the outer
        // `&mut self` is reclaimed once `fetch_all` is awaited.
        let this: &Self = &*self;
        let fetch_all = async {
            let results: Vec<(ContractInstanceId, Result<State<'static>, ExecutorError>)> =
                futures::future::join_all(unique_ids.iter().map(|id| {
                    let id = *id;
                    async move {
                        if let Some(full_key) = this.lookup_key(&id) {
                            if let Ok(state) = this.state_store.get(&full_key).await {
                                return (id, Ok(State::from(state.as_ref().to_vec())));
                            }
                        }
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
            Ok(Err(e)) => return Err(e),
            Err(_elapsed) => {
                return Err(ExecutorError::request(StdContractError::Put {
                    key: *key,
                    cause: "timed out fetching related contracts".into(),
                }));
            }
        }

        // Merge initial_related with newly fetched states
        let initial_owned = initial_related.clone().into_owned();
        for (id, state) in initial_owned.states() {
            if let Some(s) = state {
                related_map
                    .entry(*id)
                    .or_insert_with(|| Some(s.clone().into_owned()));
            }
        }

        let populated_related = RelatedContracts::from(related_map);
        // #4864 round-7/9: classify the second-round validation error too, with the
        // caller-supplied op kind (see the first `validate_state` match above).
        let retry_result = match already_fetched_contract {
            Some(contract) => self
                .runtime
                .validate_state_with_contract(key, params, state, &populated_related, contract)
                .map_err(|e| ExecutorError::execution(e, Some(validation_op.op_for(*key))))?,
            None => self
                .runtime
                .validate_state(key, params, state, &populated_related)
                .map_err(|e| ExecutorError::execution(e, Some(validation_op.op_for(*key))))?,
        };

        if let ValidateResult::RequestRelated(_) = &retry_result {
            return Err(ExecutorError::request(StdContractError::Put {
                key: *key,
                cause: "contract requested additional related contracts after first round (depth=1 limit exceeded)".into(),
            }));
        }

        Ok(retry_result)
    }
}
