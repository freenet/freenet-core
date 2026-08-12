use std::{fs::File, io::Write, path::PathBuf, sync::Arc};

use dashmap::DashMap;
use freenet_stdlib::prelude::*;
use moka::sync::Cache as MokaCache;

use crate::contract::storages::Storage;

use super::RuntimeResult;

/// Shared in-memory contract instance index: `ContractInstanceId -> CodeHash`.
///
/// One `Arc` is owned by `RuntimePool` and cloned into every pool executor's
/// [`ContractStore`], so an instance stored / indexed / removed via one
/// executor is immediately visible to all the others. Before #4218 each
/// `ContractStore::new` built its OWN `Arc<DashMap>` and loaded it once from
/// ReDb, so pool executors diverged: `code_hash_from_id` / `fetch_contract`
/// on executor B could miss an instance stored via executor A, and a
/// removal on A left a "ghost" instance live on B until B was rebuilt.
pub type SharedContractIndex = Arc<DashMap<ContractInstanceId, CodeHash>>;

/// Handle contract blob storage on the file system.
pub struct ContractStore {
    contracts_dir: PathBuf,
    contract_cache: MokaCache<CodeHash, Arc<ContractCode<'static>>>,
    /// In-memory index: ContractInstanceId -> CodeHash.
    /// Shared across all pool executors (see [`SharedContractIndex`]); loaded
    /// from ReDb once on first construction and kept in sync by every
    /// `store_contract` / `ensure_key_indexed` / `remove_contract`.
    key_to_code_part: SharedContractIndex,
    /// ReDb storage for persistent index
    db: Storage,
    /// Test-only hook invoked inside `store_contract` AFTER the `.wasm` blob is
    /// written and synced but BEFORE the ReDb index entry is committed. Lets the
    /// issue #4216 regression test drive a concurrent `remove_contract` into
    /// exactly that window. `None` in every non-test build.
    #[cfg(test)]
    after_blob_write_hook: std::sync::Mutex<Option<Box<dyn FnMut() + Send>>>,
}
// Eviction-driven reclamation of unused contracts now exists: the hosting
// cache evicts least-valuable contracts past its budget and the resulting
// `EvictContract` event drives `Executor::reclaim_contract_storage`, which
// calls `ContractStore::remove_contract` to delete the on-disk `.wasm` blob.

impl ContractStore {
    /// # Arguments
    /// - contracts_dir: directory where contract WASM files are stored
    /// - max_size: max size in bytes of the contracts being cached
    /// - db: ReDb storage for persistent index
    ///
    /// Builds a store with its OWN fresh (unshared) instance index. Use this
    /// for standalone / single-executor stores and tests. Pool executors that
    /// must share one live index across the pool use
    /// [`ContractStore::new_with_shared_index`] (#4218).
    pub fn new(contracts_dir: PathBuf, max_size: u64, db: Storage) -> RuntimeResult<Self> {
        Self::new_with_shared_index(contracts_dir, max_size, db, Arc::new(DashMap::new()))
    }

    /// Like [`ContractStore::new`] but wires in a caller-owned
    /// [`SharedContractIndex`] so every pool executor sees the same live
    /// `ContractInstanceId -> CodeHash` map (#4218).
    ///
    /// The ReDb index is loaded into the shared map only on FIRST construction
    /// (when the map is still empty). Subsequent pool executors and
    /// replacements pass the SAME already-populated `Arc`, so they inherit the
    /// live map (including instances stored since startup) instead of paying a
    /// redundant ReDb scan and racing the on-disk state.
    pub fn new_with_shared_index(
        contracts_dir: PathBuf,
        max_size: u64,
        db: Storage,
        key_to_code_part: SharedContractIndex,
    ) -> RuntimeResult<Self> {
        std::fs::create_dir_all(&contracts_dir).map_err(|err| {
            tracing::error!("error creating contract dir: {err}");
            err
        })?;

        // Load the index from ReDb only if this shared map hasn't been
        // populated yet (first executor in the pool). Later executors share the
        // same live `Arc` and must not clobber / re-scan it.
        if key_to_code_part.is_empty() {
            match db.load_all_contract_index() {
                Ok(entries) => {
                    for (instance_id, code_hash) in entries {
                        key_to_code_part.insert(instance_id, code_hash);
                    }
                    tracing::debug!(
                        "Loaded {} contract index entries from ReDb",
                        key_to_code_part.len()
                    );
                }
                Err(e) => {
                    tracing::warn!("Failed to load contract index from ReDb: {e}");
                }
            }

            // Migrate any contract WASM files written under the legacy lowercased
            // Base58 name to the canonical mixed-case name (issue #4214) so the
            // fetch paths below, which use `code_hash.encode()`, still find code
            // persisted before the stdlib CodeHash::encode case-fix.
            for entry in key_to_code_part.iter() {
                super::migrate_legacy_lowercased_code_file(
                    &contracts_dir,
                    &entry.value().encode(),
                    "wasm",
                );
            }
        }

        Ok(Self {
            contract_cache: MokaCache::builder()
                .max_capacity(max_size)
                .weigher(
                    |key: &CodeHash, value: &Arc<ContractCode<'static>>| -> u32 {
                        // Saturate to u32::MAX on overflow as moka recommends.
                        // A contract WASM module larger than 4 GiB would indicate
                        // a bug in upstream size validation — log it loudly.
                        let len = value.data().len();
                        u32::try_from(len).unwrap_or_else(|_| {
                            tracing::warn!(
                                code_hash = %key,
                                size_bytes = len,
                                "Contract code exceeds u32::MAX in cache weigher; \
                                 saturating. This should be impossible."
                            );
                            u32::MAX
                        })
                    },
                )
                .build(),
            contracts_dir,
            key_to_code_part,
            db,
            #[cfg(test)]
            after_blob_write_hook: std::sync::Mutex::new(None),
        })
    }

    /// Install the test-only post-blob-write hook (issue #4216 regression test).
    #[cfg(test)]
    fn set_after_blob_write_hook(&self, hook: Box<dyn FnMut() + Send>) {
        *self.after_blob_write_hook.lock().unwrap() = Some(hook);
    }

    /// Returns a copy of the contract bytes if available, none otherwise.
    // todo: instead return Result<Option<_>, _> to handle IO errors upstream
    //
    // The `key_to_code_part` index is now SHARED across every pool executor
    // (see [`SharedContractIndex`] / #4218), so a contract stored via executor
    // A is visible to `fetch_contract` on executor B. That fixes the old
    // "stored on A, missing on B" divergence.
    //
    // The lookup is GATED on the shared index: the per-executor `contract_cache`
    // fast path is served ONLY when this instance id is still present in the
    // shared index. Without this gate, after a sibling executor's
    // `remove_contract` cleared the shared index entry, THIS executor's
    // still-warm `contract_cache` (keyed by the shared code hash) would keep
    // serving the removed instance as a "ghost". Consulting the shared index
    // first makes a removal on any executor immediately authoritative on all of
    // them (#4218 problem 2).
    pub fn fetch_contract(
        &self,
        key: &ContractKey,
        params: &Parameters<'_>,
    ) -> Option<ContractContainer> {
        // Resolve the code hash from the shared INDEX, never from the `code`
        // field the caller's `ContractKey` carries. Two reasons, and the second
        // is why this is not merely a tidiness point:
        //
        // 1. It gates on the index, so an instance that is no longer indexed
        //    (e.g. removed via a sibling executor) is not served from a stale
        //    per-executor cache entry (#4218 problem 2).
        // 2. That `code` field is an unverified serde field (see
        //    `verify_contract_identity`), and `ContractKey`'s `Eq`/`Hash` ignore
        //    it, so it does not have to correspond to this instance at all.
        //    Keying the cache lookup off it let a caller name an instance it is
        //    entitled to while choosing WHICH cached code came back for it. The
        //    index is what this node itself recorded, so resolving through it
        //    makes the answer a function of the instance alone.
        let code_hash = self.key_to_code_part.get(key.id()).map(|e| *e.value())?;

        if let Some(data) = self.contract_cache.get(&code_hash) {
            return Some(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
                WrappedContract::new(data, params.clone().into_owned()),
            )));
        }

        let key_path = self
            .contracts_dir
            .join(code_hash.encode())
            .with_extension("wasm");
        // Load with version prefix stripping (fixes #2924)
        // Files are stored with to_bytes_versioned() which adds a version prefix.
        // Must use load_versioned_from_path() to strip it before compilation.
        let (code, _ver) = ContractCode::load_versioned_from_path(&key_path)
            .map_err(|err| {
                tracing::debug!("contract not found: {err}");
                err
            })
            .ok()?;
        let params = params.clone().into_owned();
        // add back the contract part to the mem store
        self.contract_cache
            .insert(code_hash, Arc::new(code.clone()));
        Some(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            WrappedContract::new(Arc::new(code), params),
        )))
    }

    /// Check that a contract's key is actually derived from the bytes it ships
    /// with, and refuse it otherwise.
    ///
    /// # Why this has to be checked here
    ///
    /// A `ContractKey` is two fields, `instance` and `code`, and BOTH are
    /// ordinary serde fields that survive a wire round-trip exactly as sent:
    /// nothing recomputes them on deserialization, and `ContractKey`'s `Hash` /
    /// `Eq` compare `instance` alone, so the `code` half is never even consulted
    /// for identity. `ContractCode` carries the same shape: `ContractCode::hash()`
    /// returns a stored field rather than hashing the bytes, and its `PartialEq`
    /// compares only that field. So a container's claimed identity and its actual
    /// content are independent until something derives one from the other.
    ///
    /// [`ContractStore`] is where that stops being merely untidy: `store_contract`
    /// uses the claimed code hash as the blob FILENAME and as the value written
    /// into the instance→code index, both in memory and durably in ReDb, and the
    /// index is what `fetch_contract` and the compiled-module cache later resolve
    /// through. An identity the node never derived would therefore decide, durably,
    /// which bytes it believes are a given contract's code.
    ///
    /// # The two checks, and why the order matters
    ///
    /// 1. `CodeHash::from_code(code.data())` must equal the code hash the
    ///    container claims (on the key AND on the code itself). This is the only
    ///    check that touches the bytes, so it has to come first.
    /// 2. `instance` must equal `ContractInstanceId::from_params_and_code(params,
    ///    code)`. That derivation is `BLAKE3(code.hash() ‖ params)` and it reads
    ///    `code.hash()`, i.e. the stored field, so it is only meaningful ONCE
    ///    check 1 has established that the field matches the bytes. Doing 2 alone
    ///    would verify a claim against another claim.
    ///
    /// Both together are what bind instance, code hash and bytes into one
    /// identity. Check 1 alone would still allow a well-formed container to be
    /// filed under an unrelated instance id; check 2 alone is circular.
    ///
    /// Two distinct `(code, params)` pairs cannot legitimately derive the same
    /// instance id by a length-shifting trick, only by a genuine BLAKE3
    /// collision: the first operand of the concatenation is a fixed-length
    /// 32-byte hash, so `BLAKE3(code_hash ‖ params)` is unambiguously parseable
    /// and `(code_hash, params)` is uniquely recoverable from the hashed string.
    /// (Were the first operand variable-length, `a ‖ b` and `a' ‖ b'` could agree
    /// while the pairs differed, and a cheap preimage would exist without
    /// breaking the hash.) That is what makes it sound for
    /// `ensure_key_indexed_locked` to treat an index row disagreeing with a
    /// verified key as WRONG rather than as an alternative mapping.
    ///
    /// # Cost, and where it is paid
    ///
    /// One BLAKE3 pass over the WASM per call. Since #5268's follow-up this runs
    /// on the COMMON path, not just on first store: the executor's "code already
    /// on disk" branch routes here so that a new instance of an existing binary
    /// is verified before it is indexed, and on that path there is no module
    /// compile to be negligible against — the blob is cached and nothing is
    /// rewritten. It is still small beside the `validate_state` / `update_state`
    /// WASM call the same PUT performs, which is the honest comparison. Do not
    /// move the check later to save it: the whole point is that it precedes every
    /// durable effect.
    ///
    /// Deriving the id via the stdlib helper rather than re-implementing
    /// `BLAKE3(hash ‖ params)` here is deliberate: a local copy of that formula
    /// would silently diverge if the derivation ever changed.
    ///
    /// # Note for a future fuzz or property test
    ///
    /// A container built with `Arbitrary` fails check 1 BY CONSTRUCTION and is
    /// not a bug here: `ContractCode` derives `Arbitrary` with `code_hash` as
    /// random bytes unrelated to `data`, and `WrappedContract::arbitrary` then
    /// derives the key from that unrelated hash — so such a container satisfies
    /// check 2 and fails check 1. Nothing in core or fdev builds contract
    /// containers that way today, so this is not reachable; but feeding
    /// `WrappedContract::arbitrary()` to `store_contract` would fail in a way
    /// that looks like a real defect. Hash the bytes you want the container to
    /// carry, or go through `WrappedContract::new`.
    fn verify_contract_identity(
        key: &ContractKey,
        code: &ContractCode<'_>,
        params: &Parameters<'_>,
    ) -> RuntimeResult<()> {
        let actual_code_hash = CodeHash::from_code(code.data());

        // Check 1: the claimed code hash(es) must be the hash of these bytes.
        // Both the key's copy and the code's own copy are checked, because they
        // are separate fields and either could be the one a later reader trusts.
        if actual_code_hash != *key.code_hash() || actual_code_hash != *code.hash() {
            return Err(crate::wasm_runtime::RuntimeInnerError::ContractIdentityMismatch {
                key: Box::new(*key),
                detail: format!(
                    "code hashes to {actual_code_hash} but the key claims {} and the code claims {}",
                    key.code_hash(),
                    code.hash()
                ),
            }
            .into());
        }

        // Check 2: sound only now that check 1 passed (see rustdoc).
        let derived_instance = ContractInstanceId::from_params_and_code(params, code);
        if derived_instance != *key.id() {
            return Err(
                crate::wasm_runtime::RuntimeInnerError::ContractIdentityMismatch {
                    key: Box::new(*key),
                    detail: format!(
                        "instance {} is not derived from this code and these {} parameter byte(s) \
                     (derivation gives {derived_instance})",
                        key.id(),
                        params.as_ref().len()
                    ),
                }
                .into(),
            );
        }

        Ok(())
    }

    /// Store a copy of the contract in the local store, in case it hasn't been stored previously.
    pub fn store_contract(&mut self, contract: ContractContainer) -> RuntimeResult<()> {
        let (key, code, params) = match contract.clone() {
            ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract_v1)) => (
                *contract_v1.key(),
                contract_v1.code().clone(),
                contract_v1.params().clone(),
            ),
            // Return, don't `unimplemented!()`. Unreachable today (V1 is the only
            // variant), but `ContractWasmAPIVersion` is `#[non_exhaustive]`, so a
            // future variant would otherwise panic — and since this function moved
            // onto the COMMON PUT path (it now handles new instances of
            // already-stored code, not just first stores), that panic would be
            // reachable from ordinary traffic rather than from a first store. The
            // delegate store already answers this case with an error; match it.
            ContractContainer::Wasm(_) | _ => {
                return Err(anyhow::anyhow!("unsupported contract container version").into());
            }
        };

        // Verify the identity BEFORE anything durable happens, and before taking
        // the blob lock: this is pure computation (one BLAKE3 pass over the WASM),
        // so there is no reason to hold a process-wide lock across it, and nothing
        // below should run at all for a container whose key we cannot derive.
        // See `verify_contract_identity`, including what that pass costs on the
        // already-stored path — where there is no module compile to hide behind.
        if let Err(err) = Self::verify_contract_identity(&key, &code, &params) {
            // WARN, not debug: this is the node declining to file bytes under an
            // identity it did not derive. It should be visible in an operator's
            // log without a rebuild (`debug!` compiles out in release builds).
            tracing::warn!(
                contract = %key,
                code_bytes = code.data().len(),
                "refusing to store contract: {err}"
            );
            return Err(err);
        }

        // Serialize against concurrent store/remove on the SAME shared code
        // hash across sibling-executor `ContractStore`s (issue #4216). Without
        // this, a `remove_contract` on another executor can run its
        // `load_all_contract_index()` scan in the window after we write the
        // `.wasm` blob but before we commit our index entry, see no remaining
        // reference to the code hash, and delete the blob we just wrote —
        // leaving this instance indexed but blobless. Holding the shared lock
        // from before the cache/disk checks through the index commit closes
        // that window. All sections below are synchronous (no `.await`), so
        // holding a std `Mutex` across the ReDb + filesystem ops is
        // deadlock-safe.
        let blob_lock = self.db.contract_blob_lock();
        let _blob_guard = blob_lock.lock().unwrap_or_else(|e| e.into_inner());

        let code_hash = key.code_hash();
        let key_path = code_hash.encode();
        let key_path = self.contracts_dir.join(key_path).with_extension("wasm");
        if self.contract_cache.get(code_hash).is_some() && key_path.exists() {
            // WASM code is cached AND the blob is still on disk: fast path. We
            // still need to ensure this instance_id is indexed (different
            // ContractInstanceIds with the same code each need their own
            // mapping — see issue #2380).
            //
            // We MUST also verify the blob is on disk: this `contract_cache` is
            // per-`ContractStore` (per pool executor), and a `remove_contract`
            // on a sibling executor can delete the shared blob without
            // invalidating our cache. Falling through to the disk-write branch
            // below restores the blob in that case so the new instance is
            // durably stored. See issue #4218 for the underlying per-executor
            // map divergence and Codex's round-4 finding.
            //
            // Use the `_locked` helper: we already hold `blob_lock` and std
            // `Mutex` is not reentrant.
            self.ensure_key_indexed_locked(&key)?;
            return Ok(());
        }
        if let Ok((code, _ver)) = ContractCode::load_versioned_from_path(&key_path) {
            // WASM file exists on disk. Add to cache AND ensure the index is updated.
            // See issue #2344 for why this is critical after crash recovery.
            self.ensure_key_indexed_locked(&key)?;
            self.contract_cache.insert(*code_hash, Arc::new(code));
            return Ok(());
        }

        // CRITICAL ORDER: Write disk first, then index, then cache.
        // This ensures fetch_contract() can always fall back to disk lookup.

        // Step 1: Save to disk first (ensures data is persisted)
        let version = APIVersion::from(contract);
        let output: Vec<u8> = code
            .to_bytes_versioned(version)
            .map_err(|e| anyhow::anyhow!(e))?;
        let mut file = File::create(&key_path)?;
        file.write_all(output.as_slice())?;
        file.sync_all()?; // Ensure durability before updating index

        // Test-only injection point for the issue #4216 race: the blob is now
        // observable on disk but the index entry below is not yet committed.
        #[cfg(test)]
        if let Some(hook) = self.after_blob_write_hook.lock().unwrap().as_mut() {
            hook();
        }

        // Step 2: Update index in ReDb (persistent, crash-safe)
        self.db
            .store_contract_index(key.id(), code_hash)
            .map_err(|e| anyhow::anyhow!("Failed to store contract index: {e}"))?;

        // Step 3: Update in-memory index
        self.key_to_code_part.insert(*key.id(), *code_hash);

        // Step 4: Insert into memory cache
        let data = code.data().to_vec();
        self.contract_cache
            .insert(*code_hash, Arc::new(ContractCode::from(data)));

        Ok(())
    }

    pub fn get_contract_path(&mut self, key: &ContractKey) -> RuntimeResult<PathBuf> {
        let contract_hash = *key.code_hash();
        let key_path = contract_hash.encode();
        Ok(self.contracts_dir.join(key_path).with_extension("wasm"))
    }

    /// Remove a contract instance from the store.
    ///
    /// Removes this instance's index entries (ReDb + in-memory) and any
    /// delegate subscriptions unconditionally. The on-disk `.wasm` blob and
    /// the code cache entry are keyed by `code_hash`, which is shared across
    /// every `ContractInstanceId` using the same code, so they are removed
    /// only once no remaining instance references that code hash. File
    /// removal is idempotent — an already-missing `.wasm` is not an error.
    ///
    /// The "still referenced?" decision is made against the **persistent
    /// ReDb `contract_index`**, not the in-memory `key_to_code_part` map.
    /// Pool executors now share one in-memory index (see
    /// [`SharedContractIndex`] / `new_with_shared_index`), but ReDb stays
    /// the authority here: it is the durable source of truth that also
    /// covers standalone stores built via [`ContractStore::new`] (which
    /// get a private index), and deciding from a possibly-incomplete
    /// in-memory view would wrongly delete a `.wasm` blob still
    /// referenced by another instance — corrupting every surviving
    /// contract that shares the code (e.g. every River room shares one
    /// room-contract WASM, see issue #2380).
    pub fn remove_contract(&mut self, key: &ContractKey) -> RuntimeResult<()> {
        let contract_hash = *key.code_hash();

        // Serialize against concurrent `store_contract` on the SAME shared code
        // hash across sibling-executor `ContractStore`s (issue #4216). Holding
        // the shared lock from before the ReDb index removal through the blob
        // delete guarantees our "is this code still referenced?" scan observes
        // any concurrent store's committed index entry (that store commits its
        // entry while holding this same lock), so we never delete a blob a
        // sibling executor just wrote for a new instance of the same code. All
        // sections below are synchronous (no `.await`), so holding a std
        // `Mutex` across the ReDb + filesystem ops is deadlock-safe.
        let blob_lock = self.db.contract_blob_lock();
        let _blob_guard = blob_lock.lock().unwrap_or_else(|e| e.into_inner());

        // Remove this instance's index entries first. The ReDb removal must
        // happen before the `load_all_contract_index()` scan below so this
        // instance's own entry is not counted as a remaining reference.
        self.db
            .remove_contract_index(key.id())
            .map_err(|e| anyhow::anyhow!("Failed to remove contract index: {e}"))?;
        self.key_to_code_part.remove(key.id());

        // Clean up any delegate subscriptions for this contract instance.
        super::DELEGATE_SUBSCRIPTIONS.remove(key.id());

        // The WASM blob on disk is keyed by code hash and shared by every
        // contract instance with the same code (e.g. all River rooms share
        // one room-contract WASM — see issue #2380). Only delete the blob
        // and its cache entry once no remaining instance references this
        // code hash, otherwise the surviving instances break on cache miss.
        //
        // Decide from the shared persistent index, NOT the per-executor
        // in-memory map (see the doc comment above for why).
        let code_still_referenced = match self.db.load_all_contract_index() {
            Ok(entries) => entries
                .iter()
                .any(|(_, code_hash)| code_hash == &contract_hash),
            Err(e) => {
                // If we cannot read the shared index we cannot prove the
                // blob is unreferenced. A leaked blob is recoverable disk
                // space; deleting a still-referenced shared blob corrupts
                // every surviving contract using that code. Fail safe:
                // keep the blob — and return an error so the caller knows
                // the code half was NOT fully reclaimed and will requeue
                // for retry rather than clearing the pending entry.
                tracing::warn!(
                    code_hash = %contract_hash,
                    error = %e,
                    "Could not load shared contract index while removing a \
                     contract; keeping the .wasm blob to avoid corrupting \
                     contracts that may still reference this code"
                );
                return Err(anyhow::anyhow!(
                    "kept WASM blob for {contract_hash}: shared contract \
                     index read failed: {e}"
                )
                .into());
            }
        };
        if !code_still_referenced {
            // Invalidate the code cache so a removed contract is never served
            // as a "ghost" (issue #3487).
            self.contract_cache.invalidate(&contract_hash);
            let key_path = self
                .contracts_dir
                .join(contract_hash.encode())
                .with_extension("wasm");
            match std::fs::remove_file(&key_path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Returns true if the WASM code blob for `code_hash` is already present on
    /// the shared on-disk contract store.
    ///
    /// This is the disk-budget DEDUP probe (#4218 / #4683). It answers exactly
    /// the question the budget gate and the store-vs-index routing in the
    /// executor need: "will `store_contract` write NEW blob bytes to disk?" —
    /// which is true iff the blob is not already on disk.
    ///
    /// It is keyed by CODE HASH and checks the shared filesystem (one `.wasm`
    /// blob per code hash, shared by every instance across every pool executor),
    /// NOT `fetch_contract`, which is keyed by INSTANCE id. A second instance of
    /// already-stored code (same code hash, different params → a NEW instance
    /// id) is absent from the instance index, so an instance-keyed probe would
    /// wrongly report "not stored" and (a) charge the shared blob against the
    /// disk budget a second time (double-count) and (b) route the PUT down the
    /// store path instead of the ensure-indexed path. Disk existence is the
    /// single shared source of truth for on-disk blob occupancy, is O(1), and is
    /// consistent across executors without relying on any per-executor cache.
    ///
    /// The shared instance index is deliberately NOT consulted here: it can
    /// disagree with disk (a sibling executor deleted the blob, or a crash left
    /// an index entry without a blob), and trusting it would skip the
    /// blob-rewrite safety in `store_contract`. Disk is authoritative for "are
    /// the bytes on disk right now".
    pub fn code_blob_stored(&self, code_hash: &CodeHash) -> bool {
        self.contracts_dir
            .join(code_hash.encode())
            .with_extension("wasm")
            .exists()
    }

    pub fn code_hash_from_key(&self, key: &ContractKey) -> Option<CodeHash> {
        self.key_to_code_part.get(key.id()).map(|r| *r.value())
    }

    /// Look up the code hash for a contract given only its instance ID.
    /// Used when clients request contracts without knowing the code hash.
    pub fn code_hash_from_id(&self, id: &ContractInstanceId) -> Option<CodeHash> {
        self.key_to_code_part.get(id).map(|r| *r.value())
    }

    /// Index a contract instance from a bare key. **Test fixtures only.**
    ///
    /// Indexing a new instance of already-stored code (different parameters =
    /// different rooms, issue #2380) is real and necessary, but it is
    /// [`Self::store_contract`]'s job: its fast paths do exactly this work when
    /// the blob is already present, and they do it AFTER
    /// [`Self::verify_contract_identity`] has established that the key is
    /// derived from the code and parameters in hand.
    ///
    /// This entry point cannot do that, and that is why it is no longer
    /// production-reachable. It receives only a `&ContractKey` — no code bytes,
    /// no parameters — so it can neither derive the identity it is filing nor
    /// confirm the blob it points at exists. It used to be called directly from
    /// `bridged_upsert_contract_state_inner` on the common "code already stored"
    /// path, which made it a second, unguarded writer of the durable
    /// instance→code row, and it was implicated twice over for two DIFFERENT
    /// missing preconditions: no derivation check (the hole
    /// `verify_contract_identity` closes) and no blob-existence check (the
    /// candidate mechanism for the dangling index entries in #5280). One helper,
    /// two invariants missed, because a store with real invariants had an ingress
    /// that could not check them.
    ///
    /// It survives `#[cfg(test)]`-gated because several fixtures legitimately
    /// want "index this key and nothing else". The gate is the enforcement: a
    /// future production caller does not compile. If you need this from
    /// production code, you need `store_contract`.
    #[cfg(test)]
    pub fn ensure_key_indexed(&mut self, key: &ContractKey) -> RuntimeResult<()> {
        // Public entry point: take the shared store/remove lock (issue #4216)
        // so an external caller indexing a new instance is serialized against a
        // concurrent `remove_contract` on the same code hash, just like
        // `store_contract`. Callers already holding the lock (i.e.
        // `store_contract`) must use `ensure_key_indexed_locked` instead — std
        // `Mutex` is not reentrant.
        let blob_lock = self.db.contract_blob_lock();
        let _blob_guard = blob_lock.lock().unwrap_or_else(|e| e.into_inner());
        self.ensure_key_indexed_locked(key)
    }

    /// Unlocked body of [`Self::ensure_key_indexed`]. The caller MUST hold the
    /// shared `contract_blob_lock` (issue #4216). In production this is reached
    /// only from `store_contract`, which already holds the lock and has already
    /// verified `key` against the code and parameters it arrived with.
    ///
    /// # Why a disagreeing row is corrected rather than left alone
    ///
    /// This used to insert only when the instance was ABSENT, which made a wrong
    /// row STICKY: once `instance -> X` existed, a later honest store of that
    /// instance could not correct it, because the only unconditional overwrite is
    /// in `store_contract`'s slow path and that path runs only when the
    /// instance's own blob is missing. So a row written before the derivation
    /// check existed would outlive it indefinitely.
    ///
    /// An instance id is `BLAKE3(code_hash ‖ params)`, so a verified key's
    /// instance determines its code hash uniquely: a stored row that disagrees
    /// with a key the caller has just verified is wrong, not an alternative
    /// mapping. Overwriting it is therefore both safe and the only self-healing
    /// path for rows that predate the check. It logs at WARN — not `debug!`,
    /// which compiles out in release — because a disagreement means the node held
    /// a mapping it could not have derived, which an operator should see once.
    fn ensure_key_indexed_locked(&mut self, key: &ContractKey) -> RuntimeResult<()> {
        let code_hash = key.code_hash();
        let existing = self.key_to_code_part.get(key.id()).map(|e| *e.value());
        match existing {
            Some(recorded) if recorded == *code_hash => return Ok(()),
            Some(recorded) => {
                // WHAT THIS TRADE ACTUALLY IS. Correcting the row can leave
                // `recorded`'s blob referenced by no index row, if this was its last
                // referent. What the correction BUYS is much larger than what it
                // costs: without it, instance `key.id()` resolves durably and
                // forever to code it was never derived from — and the
                // compiled-module cache is keyed off that resolution — whereas with
                // it the residue is one unreferenced file on disk.
                //
                // The file is never served, because `fetch_contract` resolves code
                // through this index. But do not read the leak as self-clearing:
                // NOTHING in the tree frees it. `refresh_wasm` is a `du`-walk that
                // re-measures bytes for the disk-budget counter and deletes nothing,
                // and `remove_contract` — the only blob GC — is driven off live
                // instances. So the bytes stay CORRECTLY counted forever, which
                // means the orphan permanently consumes admission headroom via
                // `admit_wasm_write`'s `total_bytes() + blob_len > budget_bytes`
                // gate. Honest accounting, genuinely spent capacity. Orphan
                // reconciliation in both directions is #5281 ("orphans of both are
                // permanent"); a one-off sweep at this call site would be a second,
                // partial mechanism.
                //
                // Nor is a disagreeing row only possible on binaries predating this
                // check. ReDb rows outlive the binary, and crash-loop auto-rollback
                // (#4073) can reinstall a pre-check binary during probation, which
                // can mint fresh disagreeing rows that the next upgrade corrects
                // here. So this WARN recurring is not evidence of a bug in the
                // check.
                tracing::warn!(
                    contract = %key,
                    instance_id = %key.id(),
                    recorded = %recorded,
                    derived = %code_hash,
                    "Correcting an instance→code index row that disagrees with the \
                     contract's own derived identity"
                );
            }
            None => {}
        }

        // Store in ReDb
        self.db
            .store_contract_index(key.id(), code_hash)
            .map_err(|e| anyhow::anyhow!("Failed to store contract index: {e}"))?;

        // Update in-memory map
        self.key_to_code_part.insert(*key.id(), *code_hash);

        tracing::debug!(
            contract = %key,
            instance_id = %key.id(),
            code_hash = %code_hash,
            "Indexed contract instance (same code, different params)"
        );
        Ok(())
    }
}

#[cfg(test)]
mod test {
    //! Tests for ContractStore
    //!
    //! Key invariant: For every contract stored, `code_hash_from_id(instance_id)`
    //! must return the correct CodeHash. This is critical because `lookup_key()`
    //! uses this to reconstruct ContractKey from just an instance ID.
    use super::*;

    async fn create_test_db(path: &std::path::Path) -> Storage {
        Storage::new(path).await.expect("failed to create test db")
    }

    #[tokio::test]
    async fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;
        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![0, 1, 2])),
            [0, 1].as_ref().into(),
        );
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract.clone()));
        store.store_contract(container)?;
        let f = store.fetch_contract(contract.key(), &[0, 1].as_ref().into());
        assert!(f.is_some());
        Ok(())
    }

    /// Test that simulates the actual contract store flow to see if
    /// contracts can be "lost" between store and fetch
    #[tokio::test]
    async fn test_contract_store_fetch_reliability() -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        // Use realistic-ish cache size
        let mut store = ContractStore::new(contract_dir.path().into(), 100_000, db)?;

        // Store multiple contracts with varying sizes, track their keys
        let mut keys = Vec::new();
        for i in 0..10u8 {
            // Create contracts of different sizes
            let size = ((i as usize) + 1) * 1000;
            let code = vec![i; size];
            let params = Parameters::from(vec![i, i + 1]);
            let contract = WrappedContract::new(Arc::new(ContractCode::from(code)), params.clone());
            let key = *contract.key();
            keys.push((key, params));
            let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract));
            store.store_contract(container)?;
        }

        // Immediately try to fetch all contracts - this is the critical path
        // where issue #2306 manifests
        let mut fetch_failures = 0;
        for (key, params) in &keys {
            let fetched = store.fetch_contract(key, params);
            if fetched.is_none() {
                eprintln!("FETCH FAILED for contract {key} immediately after store!");
                fetch_failures += 1;
            }
        }

        assert_eq!(
            fetch_failures, 0,
            "Contracts should be fetchable immediately after store"
        );

        Ok(())
    }

    /// Test for issue #2344: Contract store index must be persisted to disk.
    /// This test simulates a node restart by creating a new ContractStore from
    /// the same directory, then verifies contracts are still fetchable.
    #[tokio::test]
    async fn test_index_persistence_after_restart() -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1, 2, 3, 4, 5])),
            [10, 20].as_ref().into(),
        );
        let key = *contract.key();
        let params: Parameters = [10, 20].as_ref().into();

        // Store the contract
        {
            let db = create_test_db(contract_dir.path()).await;
            let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;
            let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract));
            store.store_contract(container)?;

            // Verify it's fetchable in the same instance
            assert!(
                store.fetch_contract(&key, &params).is_some(),
                "Contract should be fetchable immediately after store"
            );
        }
        // ContractStore dropped here - simulates process exit

        // Create a NEW ContractStore from the same directory - simulates node restart
        {
            let db = create_test_db(contract_dir.path()).await;
            let store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

            // The contract should be fetchable because both:
            // 1. The WASM file was persisted to disk
            // 2. The index (ReDb) was persisted to disk
            // Issue #2344: Before the fix, the index wasn't synced, so the contract
            // would not be found after restart.
            let fetched = store.fetch_contract(&key, &params);
            assert!(
                fetched.is_some(),
                "Contract should be fetchable after simulated restart - index must be persisted"
            );
        }

        Ok(())
    }

    /// Test for issue #2344: When WASM file exists but index entry is missing
    /// (e.g., after a crash), store_contract should add the missing index entry.
    #[tokio::test]
    async fn test_wasm_exists_but_index_missing() -> Result<(), Box<dyn std::error::Error>> {
        use std::io::Write;

        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![7, 8, 9])),
            [30, 40].as_ref().into(),
        );
        let key = *contract.key();
        let code_hash = key.code_hash();
        let params: Parameters = [30, 40].as_ref().into();

        // Manually create the WASM file on disk (simulating a crash scenario
        // where WASM was synced but index wasn't)
        let wasm_path = contract_dir
            .path()
            .join(code_hash.encode())
            .with_extension("wasm");
        {
            let code_bytes = contract
                .code()
                .to_bytes_versioned(freenet_stdlib::prelude::APIVersion::Version0_0_1)
                .unwrap();
            let mut file = std::fs::File::create(&wasm_path)?;
            file.write_all(&code_bytes)?;
            file.sync_all()?;
        }

        // Create a ContractStore - the ReDb will be empty (no index entries)
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        // The contract is NOT fetchable yet because the index doesn't have the entry
        // and it's not in cache
        assert!(
            store.fetch_contract(&key, &params).is_none(),
            "Contract should NOT be fetchable when WASM exists but index entry is missing"
        );

        // Now call store_contract - this should detect the WASM file exists,
        // add the missing index entry, and add to cache
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract));
        store.store_contract(container)?;

        // Drop the store and create a new one to verify the index was persisted
        drop(store);
        let db = create_test_db(contract_dir.path()).await;
        let store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        // Now the contract should be fetchable because the fix adds the index entry
        let fetched = store.fetch_contract(&key, &params);
        assert!(
            fetched.is_some(),
            "Contract should be fetchable after store_contract adds missing index entry"
        );

        Ok(())
    }

    /// Regression test for issue #2380: Multiple contracts with same WASM code
    /// but different parameters must all be indexed correctly.
    ///
    /// This bug manifested in River when creating multiple chat rooms:
    /// - All rooms use the same room-contract WASM (same code_hash)
    /// - Different parameters (owner key) create different ContractInstanceIds
    /// - Only the first room's instance_id was indexed
    /// - Subscribe to 2nd+ rooms failed because lookup_key() returned None
    #[tokio::test]
    async fn test_multiple_contracts_same_code_different_params()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        // Same WASM code for all contracts (like River's room-contract)
        let shared_code = vec![1, 2, 3, 4, 5];

        // Create multiple contracts with SAME code but DIFFERENT parameters
        // This simulates creating multiple River rooms
        let mut contracts = Vec::new();
        for i in 0..5u8 {
            let params = Parameters::from(vec![i, i + 10, i + 20]); // Different params each time
            let contract = WrappedContract::new(
                Arc::new(ContractCode::from(shared_code.clone())),
                params.clone(),
            );
            contracts.push((contract.clone(), params));

            let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract));
            store.store_contract(container)?;
        }

        // All contracts share the same code_hash
        let expected_code_hash = contracts[0].0.key().code_hash();
        for (contract, _) in &contracts {
            assert_eq!(
                contract.key().code_hash(),
                expected_code_hash,
                "All contracts should have the same code_hash"
            );
        }

        // Critical assertion: code_hash_from_id must work for ALL instance IDs
        // This is what lookup_key() uses, and what failed before the fix
        for (i, (contract, _)) in contracts.iter().enumerate() {
            let instance_id = contract.key().id();
            let lookup_result = store.code_hash_from_id(instance_id);
            assert!(
                lookup_result.is_some(),
                "code_hash_from_id() failed for contract {i} (instance_id: {instance_id}) - \
                 this would cause Subscribe to fail!"
            );
            assert_eq!(
                lookup_result.unwrap(),
                *expected_code_hash,
                "code_hash_from_id() returned wrong hash for contract {i}"
            );
        }

        // Also verify fetch_contract works for all
        for (i, (contract, params)) in contracts.iter().enumerate() {
            let fetched = store.fetch_contract(contract.key(), params);
            assert!(
                fetched.is_some(),
                "fetch_contract() failed for contract {i}"
            );
        }

        Ok(())
    }

    /// Regression test for issue #2924: Versioned contract files must be
    /// properly loaded without the version prefix before compilation.
    ///
    /// The bug:
    /// - Contracts are stored with to_bytes_versioned() which adds a version prefix
    /// - fetch_contract() was using ContractContainer::try_from which read raw bytes
    /// - The prefix caused wasmtime to fail auto-detection (no WASM magic number)
    /// - Module::new tried to parse as WAT, failed with "input bytes aren't valid utf-8"
    ///
    /// The fix:
    /// - Use ContractCode::load_versioned_from_path() which strips the prefix
    /// - The WASM magic number is now at offset 0, so Module::new works correctly
    #[tokio::test]
    async fn test_versioned_contract_loading_issue_2924() -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::wasm_runtime::engine::{Engine, WasmEngine};
        use crate::wasm_runtime::runtime::RuntimeConfig;

        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        // Valid WASM binary (exports "memory" and "answer" function that returns 42)
        // WAT equivalent:
        // (module
        //   (memory 1)
        //   (export "memory" (memory 0))
        //   (func (export "answer") (result i32)
        //     i32.const 42
        //   )
        // )
        const VALID_WASM: &[u8] = &[
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x05, 0x01, 0x60, 0x00, 0x01,
            0x7f, 0x03, 0x02, 0x01, 0x00, 0x05, 0x03, 0x01, 0x00, 0x01, 0x07, 0x13, 0x02, 0x06,
            0x6d, 0x65, 0x6d, 0x6f, 0x72, 0x79, 0x02, 0x00, 0x06, 0x61, 0x6e, 0x73, 0x77, 0x65,
            0x72, 0x00, 0x00, 0x0a, 0x06, 0x01, 0x04, 0x00, 0x41, 0x2a, 0x0b,
        ];

        // Verify the WASM starts with magic number
        assert_eq!(
            &VALID_WASM[0..4],
            &[0x00, 0x61, 0x73, 0x6d],
            "Test WASM should start with magic number"
        );

        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;
        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(VALID_WASM.to_vec())),
            [0, 1].as_ref().into(),
        );
        let key = *contract.key();
        let params: Parameters = [0, 1].as_ref().into();
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract.clone()));

        // Store the contract (this adds version prefix to disk file)
        store.store_contract(container)?;

        // Drop the store to clear the cache, forcing fetch_contract to read from disk
        drop(store);

        // Create a new store and fetch the contract from disk
        let db = create_test_db(contract_dir.path()).await;
        let store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;
        let fetched = store
            .fetch_contract(&key, &params)
            .expect("Contract should be fetchable after store");

        // Extract the code bytes from the fetched contract
        let ContractContainer::Wasm(ContractWasmAPIVersion::V1(fetched_contract)) = fetched else {
            panic!("Expected WASM V1 contract");
        };

        // Verify the fetched code matches the original WASM (without version prefix)
        let fetched_bytes = fetched_contract.code().data();
        assert_eq!(
            fetched_bytes, VALID_WASM,
            "Fetched contract bytes should match original WASM without version prefix"
        );

        // Verify the WASM magic number is at the start (issue #2924 would fail here)
        assert_eq!(
            &fetched_bytes[0..4],
            &[0x00, 0x61, 0x73, 0x6d],
            "Fetched WASM should start with magic number (no version prefix)"
        );

        // Critical test: Verify the fetched contract can be compiled
        // Before the fix, this would fail with "Error when converting wat: input bytes aren't valid utf-8"
        let mut engine = Engine::new(&RuntimeConfig::default(), false)?;
        let compile_result = engine.compile(fetched_bytes);
        assert!(
            compile_result.is_ok(),
            "Contract should compile successfully without 'converting wat' error. Error: {:?}",
            compile_result.err()
        );

        Ok(())
    }

    /// Regression test for issue #3487: removed contracts must not be served from cache.
    ///
    /// Before the fix, remove_contract() deleted from disk and index but did not
    /// invalidate the cache, so fetch_contract() could still return "ghost" contracts.
    #[tokio::test]
    async fn test_remove_contract_invalidates_cache() -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![5, 6, 7])),
            [0, 1].as_ref().into(),
        );
        let key = *contract.key();
        let params: Parameters = [0, 1].as_ref().into();
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract));

        // Store and verify fetchable
        store.store_contract(container)?;
        assert!(
            store.fetch_contract(&key, &params).is_some(),
            "Contract should be fetchable after store"
        );

        // Remove contract
        store.remove_contract(&key)?;

        // Must NOT be fetchable from cache
        assert!(
            store.fetch_contract(&key, &params).is_none(),
            "Removed contract must not be served from cache (ghost contract)"
        );

        Ok(())
    }

    /// A contract whose key IS derived from its own code and parameters stores
    /// normally. This is the control for the three rejection tests below: without
    /// it, they would all still pass if `verify_contract_identity` simply rejected
    /// everything, and a check that cannot come out clean is not evidence.
    #[tokio::test]
    async fn store_contract_accepts_a_key_derived_from_its_own_code()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let params: Parameters = [0u8, 1].as_ref().into();
        // `WrappedContract::new` derives the key from (params, code), so this is
        // exactly what an honest publisher produces.
        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1u8, 2, 3, 4])),
            params.clone(),
        );
        let key = *contract.key();

        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract,
        )))?;
        assert!(
            store.fetch_contract(&key, &params).is_some(),
            "an honestly-derived contract must still be storable and fetchable"
        );
        Ok(())
    }

    /// The claimed code hash must be the hash of the code actually supplied.
    ///
    /// `ContractKey`'s `code` field is an unverified serde field, so a container
    /// can name a code hash unrelated to its bytes. `store_contract` uses that
    /// value as the blob filename and as the index value, so accepting it would
    /// let bytes be filed under a code hash they do not hash to.
    #[tokio::test]
    async fn store_contract_rejects_code_hash_that_is_not_the_hash_of_the_code()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let params: Parameters = [0u8, 1].as_ref().into();
        let mut contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1u8, 2, 3, 4])),
            params.clone(),
        );
        // Keep the instance, claim a code hash that is not this code's hash.
        let bogus_code_hash = CodeHash::new([7u8; 32]);
        contract.key = ContractKey::from_id_and_code(*contract.key.id(), bogus_code_hash);
        let forged_key = contract.key;

        let err = store
            .store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
                contract,
            )))
            .expect_err("a code hash that does not match the code must be refused");
        assert!(
            matches!(
                err.deref(),
                crate::wasm_runtime::RuntimeInnerError::ContractIdentityMismatch { .. }
            ),
            "expected ContractIdentityMismatch, got: {err}"
        );

        // Nothing durable may have happened: no blob under the claimed hash, and
        // no index entry for the instance.
        let claimed_path = contract_dir
            .path()
            .join(bogus_code_hash.encode())
            .with_extension("wasm");
        assert!(
            !claimed_path.exists(),
            "no blob may be written under an unverified code hash"
        );
        assert!(
            store.code_hash_from_id(forged_key.id()).is_none(),
            "no index entry may be written for a refused contract"
        );
        Ok(())
    }

    /// The instance id must be derived from the code and parameters supplied.
    ///
    /// This is the case an internally-consistent container can still forge: the
    /// code hash genuinely matches the bytes, and only the instance id is
    /// unrelated to them. Checking the code hash alone would accept this and file
    /// well-formed code under an arbitrary instance id.
    #[tokio::test]
    async fn store_contract_rejects_instance_not_derived_from_code_and_params()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let code = Arc::new(ContractCode::from(vec![1u8, 2, 3, 4]));
        let params_a: Parameters = [0u8].as_ref().into();
        let params_b: Parameters = [9u8].as_ref().into();

        let instance_a = *WrappedContract::new(code.clone(), params_a).key().id();
        let mut forged = WrappedContract::new(code.clone(), params_b);
        // Correct code hash for these bytes, but another instance's id.
        forged.key = ContractKey::from_id_and_code(instance_a, *forged.key.code_hash());

        let err = store
            .store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(forged)))
            .expect_err("an instance id not derived from this code and params must be refused");
        assert!(
            matches!(
                err.deref(),
                crate::wasm_runtime::RuntimeInnerError::ContractIdentityMismatch { .. }
            ),
            "expected ContractIdentityMismatch, got: {err}"
        );
        assert!(
            store.code_hash_from_id(&instance_a).is_none(),
            "a refused contract must not create an index entry"
        );
        Ok(())
    }

    /// A refused contract must not re-point an EXISTING instance's code.
    ///
    /// `store_contract`'s slow path overwrites the instance→code index
    /// unconditionally (both the in-memory mirror and the durable ReDb row), and
    /// `fetch_contract` plus the compiled-module cache resolve through that index.
    /// So an accepted forgery would not merely add a bad entry, it would change
    /// which code an already-stored, legitimate contract resolves to.
    #[tokio::test]
    async fn store_contract_refusal_leaves_an_existing_instance_pointing_at_its_own_code()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let params: Parameters = [0u8, 1].as_ref().into();
        let honest_code = vec![1u8, 2, 3, 4];
        let honest = WrappedContract::new(
            Arc::new(ContractCode::from(honest_code.clone())),
            params.clone(),
        );
        let honest_key = *honest.key();
        let honest_hash = *honest_key.code_hash();
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(honest)))?;
        assert_eq!(store.code_hash_from_id(honest_key.id()), Some(honest_hash));

        // Different code, internally consistent, but claiming the honest
        // contract's instance id.
        let other_code = Arc::new(ContractCode::from(vec![9u8, 9, 9, 9, 9]));
        let mut forged = WrappedContract::new(other_code.clone(), params.clone());
        let other_hash = *forged.key.code_hash();
        assert_ne!(
            other_hash, honest_hash,
            "fixture must use genuinely different code"
        );
        forged.key = ContractKey::from_id_and_code(*honest_key.id(), other_hash);

        store
            .store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(forged)))
            .expect_err("a forged instance id must be refused");

        // The honest instance still resolves to its own code, and still serves it.
        assert_eq!(
            store.code_hash_from_id(honest_key.id()),
            Some(honest_hash),
            "a refused store must not re-point an existing instance's code"
        );
        let fetched = store
            .fetch_contract(&honest_key, &params)
            .expect("the honest contract must still be served");
        match fetched {
            ContractContainer::Wasm(ContractWasmAPIVersion::V1(c)) => {
                assert_eq!(c.code().data(), honest_code.as_slice());
            }
            _ => panic!("unexpected container version"),
        }
        Ok(())
    }

    /// `fetch_contract` must resolve code through the node's own index, not
    /// through the `code` field on the caller's key.
    ///
    /// The cache fast path used to look up `key.code_hash()` directly, gated only
    /// on the instance being indexed at all. Since that field is unverified and
    /// excluded from `ContractKey`'s `Eq`/`Hash`, a caller could name an instance
    /// while choosing which cached code came back for it.
    #[tokio::test]
    async fn fetch_contract_resolves_code_through_the_index_not_the_callers_key()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let params: Parameters = [0u8, 1].as_ref().into();
        let code_a = vec![1u8, 2, 3, 4];
        let code_b = vec![9u8, 9, 9, 9, 9];

        let a = WrappedContract::new(Arc::new(ContractCode::from(code_a.clone())), params.clone());
        let key_a = *a.key();
        let b = WrappedContract::new(Arc::new(ContractCode::from(code_b.clone())), params.clone());
        let hash_b = *b.key().code_hash();

        // Store both, so B's code is warm in the per-executor cache.
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(a)))?;
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(b)))?;

        // Name instance A, but claim B's code hash.
        let mixed_key = ContractKey::from_id_and_code(*key_a.id(), hash_b);
        let fetched = store
            .fetch_contract(&mixed_key, &params)
            .expect("instance A is indexed, so a lookup for it must resolve");
        match fetched {
            ContractContainer::Wasm(ContractWasmAPIVersion::V1(c)) => {
                assert_eq!(
                    c.code().data(),
                    code_a.as_slice(),
                    "must serve the code the INDEX records for this instance, not the code hash the caller named"
                );
            }
            _ => panic!("unexpected container version"),
        }
        Ok(())
    }

    /// Build a `ContractCode` carrying `data` but claiming `claimed_hash` as its
    /// own stored hash.
    ///
    /// `ContractCode::code_hash` is `pub(crate)` to the stdlib, so an in-memory
    /// value cannot be edited — but it is an ordinary serde field, which is the
    /// whole reason check 1 tests it. `ContractCode` is `data` then `code_hash`,
    /// so the hash is the final 32 bytes of the encoding; the assertion pins that
    /// layout so a stdlib field reorder fails this fixture loudly instead of
    /// quietly patching the wrong bytes.
    fn code_claiming_hash(data: Vec<u8>, claimed_hash: CodeHash) -> ContractCode<'static> {
        let honest = ContractCode::from(data);
        let mut bytes = bincode::serialize(&honest).expect("code must serialize");
        let len = bytes.len();
        assert!(len > 32, "encoding too short to carry a 32-byte hash");
        assert_eq!(
            &bytes[len - 32..],
            honest.hash().as_ref(),
            "fixture is stale: ContractCode's stored hash is not the final 32 bytes"
        );
        bytes[len - 32..].copy_from_slice(claimed_hash.as_ref());
        bincode::deserialize::<ContractCode>(&bytes)
            .expect("a patched code must still deserialize")
            .into_owned()
    }

    /// `ContractCode`'s OWN stored hash must be checked, not just the key's copy.
    ///
    /// Check 1 asserts the recomputed hash against BOTH `key.code_hash()` and
    /// `code.hash()`. The second clause is the one holding check 2 up, and
    /// dropping it re-opens the hole on its own: `ContractInstanceId::
    /// from_params_and_code` derives from `code.hash()`, i.e. the code's STORED
    /// field (stdlib `key.rs::generate_id`). So a container that claims a correct
    /// hash on the KEY while carrying a bogus one on the CODE satisfies the
    /// key-side clause, and then check 2 derives the instance from the bogus
    /// value and agrees with itself — leaving the instance id arbitrary.
    ///
    /// The three fixtures above all forge the key's copy and leave `code.hash()`
    /// correct, so the key-side clause alone fails them and none of them would
    /// notice the second clause being deleted. This one fails without it.
    #[tokio::test]
    async fn store_contract_rejects_a_code_whose_own_stored_hash_is_not_its_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let params: Parameters = [0u8, 1].as_ref().into();
        let data = vec![1u8, 2, 3, 4];
        let honest_hash = CodeHash::from_code(&data);
        let bogus_hash = CodeHash::new([7u8; 32]);

        let forged_code = Arc::new(code_claiming_hash(data.clone(), bogus_hash));
        assert_eq!(
            forged_code.data(),
            data.as_slice(),
            "the fixture must keep the real bytes"
        );
        assert_eq!(
            *forged_code.hash(),
            bogus_hash,
            "the fixture must claim the bogus hash on the CODE"
        );

        // The instance is derived from the BOGUS hash (that is what
        // `from_params_and_code` reads), while the key's own code hash is the
        // CORRECT one — so the key-side clause of check 1 passes, and check 2
        // agrees with itself.
        let instance = ContractInstanceId::from_params_and_code(&params, &*forged_code);
        let key = ContractKey::from_id_and_code(instance, honest_hash);
        assert_eq!(
            *key.code_hash(),
            honest_hash,
            "the key's copy must be correct, or this tests the wrong clause"
        );
        assert_eq!(
            ContractInstanceId::from_params_and_code(&params, &*forged_code),
            *key.id(),
            "check 2 must be satisfiable by this fixture, or it proves nothing \
             about the clause under test"
        );

        // `WrappedContract` is `#[non_exhaustive]`, so build it through `new` and
        // then set the two public fields this fixture needs to control.
        let mut forged = WrappedContract::new(forged_code.clone(), params.clone().into_owned());
        forged.data = forged_code;
        forged.key = key;
        let err = store
            .store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(forged)))
            .expect_err("a code whose own stored hash is not its bytes must be refused");
        assert!(
            matches!(
                err.deref(),
                crate::wasm_runtime::RuntimeInnerError::ContractIdentityMismatch { .. }
            ),
            "expected ContractIdentityMismatch, got: {err}"
        );
        assert!(
            store.code_hash_from_id(&instance).is_none(),
            "no index entry may be written for a refused contract"
        );
        Ok(())
    }

    /// Verification must hold on the "code blob already stored" path, because
    /// that is the path a new instance of existing code takes.
    ///
    /// Several instances share one code blob (every River room shares one room
    /// contract, #2380), so the common case is: blob already on disk, cache warm,
    /// only a new instance→code row to write. `store_contract` reaches that
    /// through its fast paths, and the check runs before them — this pins that,
    /// because a check placed after them would miss the majority of real PUTs.
    /// The executor's equivalent branch routes here for exactly this reason (see
    /// `bridged_upsert_contract_state_inner`).
    #[tokio::test]
    async fn store_contract_verifies_even_when_the_code_blob_is_already_stored()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let code = Arc::new(ContractCode::from(vec![1u8, 2, 3, 4]));
        let params_a: Parameters = [0u8].as_ref().into();
        let params_b: Parameters = [9u8].as_ref().into();

        // First instance: stores the blob, warms the cache.
        let first = WrappedContract::new(code.clone(), params_a.clone().into_owned());
        let key_a = *first.key();
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(first)))?;
        assert!(
            store.code_blob_stored(key_a.code_hash()),
            "fixture must leave the code blob on disk"
        );

        // Control: an HONEST second instance of that same blob is still accepted
        // and indexed. Without this the refusal below would also pass if the fast
        // paths refused every second instance.
        let honest_b = WrappedContract::new(code.clone(), params_b.clone().into_owned());
        let key_b = *honest_b.key();
        assert_ne!(key_a.id(), key_b.id(), "fixture must be a NEW instance");
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            honest_b,
        )))?;
        assert_eq!(
            store.code_hash_from_id(key_b.id()),
            Some(*code.hash()),
            "an honest new instance of already-stored code must be indexed"
        );

        // Now the same shape with an instance id that is not derived from this
        // code and these params.
        let params_c: Parameters = [5u8].as_ref().into();
        let unrelated_instance = *WrappedContract::new(code.clone(), params_c.into_owned())
            .key()
            .id();
        let mut forged = WrappedContract::new(code.clone(), params_b.clone().into_owned());
        forged.key = ContractKey::from_id_and_code(unrelated_instance, *code.hash());

        let err = store
            .store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(forged)))
            .expect_err("an underived instance must be refused even when the blob is present");
        assert!(
            matches!(
                err.deref(),
                crate::wasm_runtime::RuntimeInnerError::ContractIdentityMismatch { .. }
            ),
            "expected ContractIdentityMismatch, got: {err}"
        );
        assert!(
            store.code_hash_from_id(&unrelated_instance).is_none(),
            "the already-stored fast path must not index an unverified instance"
        );
        Ok(())
    }

    /// An instance→code row that disagrees with a verified key is corrected.
    ///
    /// `ensure_key_indexed_locked` used to write only when the instance was
    /// ABSENT, which made a wrong row permanent: the sole unconditional overwrite
    /// is `store_contract`'s slow path, reached only when the instance's blob is
    /// missing, so an honest store of that instance would find the blob present,
    /// take a fast path, see the instance "already indexed", and leave the wrong
    /// row in place. Rows written before verification existed would therefore
    /// outlive it. An instance id is `BLAKE3(code_hash ‖ params)`, so a row that
    /// disagrees with a verified key is wrong rather than an alternative mapping,
    /// and correcting it is the self-healing path.
    #[tokio::test]
    async fn store_contract_corrects_an_index_row_that_disagrees_with_the_derived_key()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let params: Parameters = [0u8, 1].as_ref().into();
        let code_a = vec![1u8, 2, 3, 4];
        let honest = WrappedContract::new(
            Arc::new(ContractCode::from(code_a.clone())),
            params.clone().into_owned(),
        );
        let key_a = *honest.key();
        let hash_a = *key_a.code_hash();

        // Plant a wrong row for A's instance, the way one written before
        // verification existed would look. `ensure_key_indexed` is the test-only
        // bare writer that used to be production-reachable.
        let hash_b = *WrappedContract::new(
            Arc::new(ContractCode::from(vec![9u8, 9, 9, 9, 9])),
            params.clone().into_owned(),
        )
        .key()
        .code_hash();
        assert_ne!(hash_a, hash_b, "fixture must plant a DIFFERENT code hash");
        store.ensure_key_indexed(&ContractKey::from_id_and_code(*key_a.id(), hash_b))?;
        assert_eq!(
            store.code_hash_from_id(key_a.id()),
            Some(hash_b),
            "fixture must leave a disagreeing row in place"
        );

        // A now stores honestly. Its blob is absent, so this takes the slow path…
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            honest.clone(),
        )))?;
        assert_eq!(
            store.code_hash_from_id(key_a.id()),
            Some(hash_a),
            "an honest store must correct a disagreeing row"
        );

        // …and it is corrected on the already-stored fast paths too, which is
        // where the stickiness actually bit: re-plant and re-store with the blob
        // now present.
        store.ensure_key_indexed(&ContractKey::from_id_and_code(*key_a.id(), hash_b))?;
        assert_eq!(store.code_hash_from_id(key_a.id()), Some(hash_b));
        assert!(
            store.code_blob_stored(&hash_a),
            "the blob must now be present, so this exercises a fast path"
        );
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(honest)))?;
        assert_eq!(
            store.code_hash_from_id(key_a.id()),
            Some(hash_a),
            "the fast paths must correct a disagreeing row, not skip it"
        );

        // Everything above reads `code_hash_from_id`, which consults the in-memory
        // `DashMap` and never the durable ReDb row. So drop the store and rebuild
        // from the same directory, forcing the index to load from ReDb alone: that
        // is what proves the correction was made DURABLE rather than only in
        // memory. Not reachable today, because `ensure_key_indexed_locked` writes
        // ReDb before updating the map — but it would break silently under a
        // refactor that reordered them, and an in-memory-only correction would be
        // undone by the next restart. Same shape as
        // `test_index_persistence_after_restart`.
        drop(store);
        let reopened_db = create_test_db(contract_dir.path()).await;
        let reopened = ContractStore::new(contract_dir.path().into(), 10_000, reopened_db)?;
        assert_eq!(
            reopened.code_hash_from_id(key_a.id()),
            Some(hash_a),
            "the correction must be durable: an index rebuilt from ReDb must not \
             resurrect the disagreeing row"
        );
        Ok(())
    }

    /// There must be exactly ONE production ingress to the durable
    /// instance→code index.
    ///
    /// `store_contract_index` has two call sites in this file — `store_contract`'s
    /// slow path and `ensure_key_indexed_locked` — and that is fine only because
    /// the second is reachable in production solely THROUGH the first, after
    /// verification. The bare public wrapper is `#[cfg(test)]`, so a production
    /// caller does not compile.
    ///
    /// This pin exists because that helper was implicated twice, hours apart, for
    /// two different missing preconditions (no derivation check; no blob-existence
    /// check, the candidate mechanism for #5280). A third writer appearing
    /// unnoticed is the failure mode to prevent.
    #[test]
    fn the_durable_index_has_one_production_writer() {
        let src = include_str!("contract_store.rs");

        // Ignore this test module itself: its assertions quote these names.
        let production = &src[..src
            .find("#[cfg(test)]\nmod test {")
            .expect("test module marker not found")];

        assert_eq!(
            production.matches(".store_contract_index(").count(),
            2,
            "exactly two call sites are expected: store_contract's slow path and \
             ensure_key_indexed_locked. A new one needs its own verification, or \
             it must route through store_contract."
        );

        // The index has TWO halves, and counting only the durable one leaves the
        // nastier writer class uncovered: a helper that inserts into
        // `key_to_code_part` WITHOUT calling `store_contract_index` passes the
        // assertion above while creating a memory-versus-ReDb divergence that
        // survives until restart — and `remove_contract` decides blob liveness from
        // ReDb, so the two halves would then disagree about what is referenced.
        // Three sites are expected: the startup load from ReDb, `store_contract`'s
        // slow path, and `ensure_key_indexed_locked`. The startup loader is counted
        // rather than excluded, because a change there is worth noticing too.
        assert_eq!(
            production.matches("key_to_code_part.insert(").count(),
            3,
            "exactly three in-memory index writes are expected: the startup ReDb \
             load, store_contract's slow path, and ensure_key_indexed_locked. A new \
             one that does not also write ReDb would diverge the in-memory index \
             from the durable one until the next restart."
        );

        production
            .find("#[cfg(test)]\n    pub fn ensure_key_indexed(")
            .expect(
                "the bare index writer must stay #[cfg(test)]-gated — it takes only a \
                 &ContractKey, so it can verify neither the identity it files nor that \
                 the blob exists",
            );
    }

    /// Regression test for the latent shared-WASM deletion bug: removing one
    /// contract instance must NOT delete the `.wasm` blob while another
    /// instance still references the same code hash.
    ///
    /// Multiple `ContractInstanceId`s can share one code hash (e.g. every
    /// River chat room shares one room-contract WASM — see issue #2380).
    /// The on-disk blob is keyed by code hash, so an unconditional delete
    /// would break the surviving instances after a cache miss / restart.
    #[tokio::test]
    async fn test_remove_contract_keeps_shared_wasm() -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        // Two instances: SAME code, DIFFERENT params -> same code_hash,
        // different ContractInstanceIds.
        let shared_code = vec![1, 2, 3, 4, 5];
        let params1 = Parameters::from(vec![1, 1, 1]);
        let params2 = Parameters::from(vec![2, 2, 2]);
        let contract1 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params1.clone(),
        );
        let contract2 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params2.clone(),
        );
        let key1 = *contract1.key();
        let key2 = *contract2.key();

        // Both instances share one code hash.
        assert_eq!(key1.code_hash(), key2.code_hash());

        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract1,
        )))?;
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract2,
        )))?;

        let wasm_path = contract_dir
            .path()
            .join(key1.code_hash().encode())
            .with_extension("wasm");
        assert!(wasm_path.exists(), "WASM file should exist after store");

        // Remove the first instance — the shared WASM must survive because
        // the second instance still references the code hash.
        store.remove_contract(&key1)?;
        assert!(
            wasm_path.exists(),
            "Shared WASM file must NOT be deleted while another instance references it"
        );

        // The second instance must still be fetchable.
        assert!(
            store.fetch_contract(&key2, &params2).is_some(),
            "Surviving instance must still be fetchable after the other is removed"
        );

        Ok(())
    }

    /// Regression test for the cross-executor shared-WASM deletion bug.
    ///
    /// Each runtime-pool executor owns a SEPARATE `ContractStore` with its
    /// own in-memory `key_to_code_part` map, but they all share one ReDb
    /// `contract_index`. If `remove_contract` decided "is this code still
    /// referenced?" from its own in-memory map, an instance stored via a
    /// different executor would be invisible — and removing the locally
    /// known instance would wrongly delete the shared `.wasm` blob,
    /// corrupting the instance owned by the other executor.
    ///
    /// This test reproduces that: two `ContractStore`s sharing ONE `db`,
    /// each storing a different instance of the SAME code. Removing one
    /// instance via store A must NOT delete the blob, and the instance
    /// stored via store B must remain fetchable.
    #[tokio::test]
    async fn test_remove_contract_keeps_shared_wasm_across_executors()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        // Two separate ContractStores (simulating two runtime-pool
        // executors) sharing ONE db / ReDb contract_index.
        let mut store_a = ContractStore::new(contract_dir.path().into(), 10_000, db.clone())?;
        let mut store_b = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        // Same code, different params -> same code_hash, different
        // ContractInstanceIds.
        let shared_code = vec![10, 20, 30, 40, 50];
        let params1 = Parameters::from(vec![1, 1, 1]);
        let params2 = Parameters::from(vec![2, 2, 2]);
        let contract1 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params1.clone(),
        );
        let contract2 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params2.clone(),
        );
        let key1 = *contract1.key();
        let key2 = *contract2.key();
        assert_eq!(key1.code_hash(), key2.code_hash());

        // Instance X1 stored via executor A, instance X2 via executor B.
        store_a.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract1,
        )))?;
        store_b.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract2,
        )))?;

        let wasm_path = contract_dir
            .path()
            .join(key1.code_hash().encode())
            .with_extension("wasm");
        assert!(wasm_path.exists(), "WASM file should exist after store");

        // Remove X1 via store A. Store A's in-memory map never saw X2, so
        // the OLD code would delete the shared blob here.
        store_a.remove_contract(&key1)?;

        assert!(
            wasm_path.exists(),
            "Shared WASM file must NOT be deleted while another executor's \
             ContractStore still references the code hash"
        );

        // X2, owned by executor B, must still be fetchable.
        assert!(
            store_b.fetch_contract(&key2, &params2).is_some(),
            "Instance stored via another executor must survive removal of a \
             different instance sharing the same code"
        );

        Ok(())
    }

    /// Removing the last instance referencing a code hash must delete the
    /// `.wasm` blob from disk to reclaim space.
    #[tokio::test]
    async fn test_remove_contract_deletes_last_instance_wasm()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![9, 8, 7, 6])),
            [4, 2].as_ref().into(),
        );
        let key = *contract.key();
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract,
        )))?;

        let wasm_path = contract_dir
            .path()
            .join(key.code_hash().encode())
            .with_extension("wasm");
        assert!(wasm_path.exists(), "WASM file should exist after store");

        // Removing the only instance must delete the blob.
        store.remove_contract(&key)?;
        assert!(
            !wasm_path.exists(),
            "WASM file must be deleted when the last instance is removed"
        );

        Ok(())
    }

    /// `remove_contract` must tolerate an already-missing `.wasm` file
    /// (idempotent file removal).
    #[tokio::test]
    async fn test_remove_contract_idempotent_when_file_missing()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![3, 3, 3])),
            [7, 7].as_ref().into(),
        );
        let key = *contract.key();
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract,
        )))?;

        // Manually delete the WASM file out from under the store.
        let wasm_path = contract_dir
            .path()
            .join(key.code_hash().encode())
            .with_extension("wasm");
        std::fs::remove_file(&wasm_path)?;

        // remove_contract must still succeed despite the missing file.
        store
            .remove_contract(&key)
            .expect("remove_contract must be Ok when the WASM file is already gone");

        Ok(())
    }

    /// Regression test for Codex's round-4 finding: the `contract_cache` is
    /// per-`ContractStore` (one per pool executor), but the `.wasm` blob on
    /// disk is shared. A sibling executor's `remove_contract` can delete the
    /// blob without invalidating this store's cache. A subsequent
    /// `store_contract` for a new instance with the same code hash must NOT
    /// take the cache-hit fast path silently — it must verify the blob still
    /// exists on disk and re-write it if not. Otherwise the new instance is
    /// indexed but blobless until the cache evicts and a fetch fails.
    #[tokio::test]
    async fn test_store_contract_rewrites_blob_when_cache_hit_but_disk_missing()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let shared_code = vec![9, 9, 9, 9];

        // Store instance X1: caches the code AND writes the blob.
        let x1 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            [1, 1].as_ref().into(),
        );
        let x1_code_hash = *x1.key().code_hash();
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(x1)))?;

        let wasm_path = contract_dir
            .path()
            .join(x1_code_hash.encode())
            .with_extension("wasm");
        assert!(wasm_path.exists(), "blob must exist after first store");

        // Simulate a sibling executor's `remove_contract` deleting the shared
        // blob WITHOUT invalidating this store's `contract_cache`.
        std::fs::remove_file(&wasm_path)?;
        assert!(!wasm_path.exists());
        assert!(
            store.contract_cache.get(&x1_code_hash).is_some(),
            "this store's cache still has the code (sibling did not invalidate it)"
        );

        // Store a NEW instance X2 with the SAME code hash. Pre-fix, the
        // cache-hit fast path returned Ok without re-writing the blob,
        // leaving X2 indexed but blobless. Post-fix, the disk-existence
        // check forces a re-write.
        let x2 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code)),
            [2, 2].as_ref().into(),
        );
        let x2_key = *x2.key();
        let params: Parameters = [2, 2].as_ref().into();
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(x2)))?;

        assert!(
            wasm_path.exists(),
            "blob must be re-written after store_contract for a new instance \
             whose shared code was in cache but missing from disk"
        );
        // And the new instance is fetchable end-to-end.
        assert!(
            store.fetch_contract(&x2_key, &params).is_some(),
            "new instance must be fetchable after rewrite"
        );

        Ok(())
    }

    /// Regression test for issue #4216: a concurrent `store_contract` and
    /// `remove_contract` on the SAME shared code hash, running on two
    /// sibling-executor `ContractStore`s over one shared ReDb, must not delete
    /// the freshly-written `.wasm` blob.
    ///
    /// The race: executor B stores a new instance X2 (writes the `.wasm` blob,
    /// then commits its index entry). Executor A removes instance X1, which
    /// scans the shared ReDb index to decide whether the blob is still
    /// referenced. If A's scan runs after B wrote the blob but before B
    /// committed X2's index entry, A sees no remaining reference and deletes
    /// the blob B just wrote — corrupting X2 on the next cache miss / restart.
    ///
    /// This test drives A's `remove_contract` into exactly that window via the
    /// test-only `after_blob_write_hook`, which fires inside B's
    /// `store_contract` after the blob write/sync but before the index commit.
    /// Pre-fix, A's remove completes inside the window and deletes the blob;
    /// post-fix, the shared blob lock makes A block until B's index commit
    /// lands, so A's scan sees X2 and keeps the blob.
    #[tokio::test]
    async fn test_concurrent_store_remove_keeps_freshly_written_wasm()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        // Two ContractStores (two runtime-pool executors) sharing ONE db.
        let mut store_a = ContractStore::new(contract_dir.path().into(), 10_000, db.clone())?;
        let mut store_b = ContractStore::new(contract_dir.path().into(), 10_000, db.clone())?;

        // Same code, different params -> same code_hash, different instances.
        let shared_code = vec![11, 22, 33, 44, 55];
        let params1 = Parameters::from(vec![1, 1, 1]);
        let params2 = Parameters::from(vec![2, 2, 2]);
        let contract1 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params1.clone(),
        );
        let contract2 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params2.clone(),
        );
        let key1 = *contract1.key();
        let key2 = *contract2.key();
        assert_eq!(key1.code_hash(), key2.code_hash());

        // Store X1 (blob + index) via executor A.
        store_a.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract1,
        )))?;

        let wasm_path = contract_dir
            .path()
            .join(key1.code_hash().encode())
            .with_extension("wasm");
        assert!(
            wasm_path.exists(),
            "WASM file should exist after first store"
        );

        // Remove the on-disk blob so B is forced down the blob-WRITE path
        // (otherwise B would take the "blob already on disk" fast path and
        // never reach the write->commit window this race lives in). X1's index
        // entry is intentionally left in place, exactly as it would be during a
        // concurrent eviction of X1.
        std::fs::remove_file(&wasm_path)?;

        // When B writes X2's blob (before committing X2's index), spawn A's
        // remove_contract(X1) on another thread and hold the window open long
        // enough for it to reach — and, pre-fix, complete — its delete
        // decision. We do NOT join inside the hook: post-fix the remove blocks
        // on the shared lock B still holds, so joining here would deadlock.
        let join_slot = Arc::new(std::sync::Mutex::new(None));
        let join_slot_hook = Arc::clone(&join_slot);
        let mut store_a_opt = Some(store_a);
        store_b.set_after_blob_write_hook(Box::new(move || {
            let mut store_a = store_a_opt.take().expect("hook is invoked exactly once");
            let handle = std::thread::spawn(move || {
                // Must succeed: a failing remove would leave the blob on disk for
                // the WRONG reason and the post-condition below would pass
                // vacuously. `handle.join()` surfaces this panic.
                store_a
                    .remove_contract(&key1)
                    .expect("concurrent remove_contract(X1) must succeed");
            });
            // Real wall-clock sleep: this is a genuine cross-thread race test,
            // so a deterministic TimeSource cannot model the interleaving.
            std::thread::sleep(std::time::Duration::from_millis(300));
            *join_slot_hook.lock().unwrap() = Some(handle);
        }));

        // Store X2 via executor B. The hook fires mid-store, after the blob is
        // on disk and before X2's index is committed.
        store_b.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract2,
        )))?;

        // B released the lock on return; let A's remove finish.
        let handle = join_slot
            .lock()
            .unwrap()
            .take()
            .expect("hook must have spawned the remove thread");
        handle.join().expect("remove thread panicked");

        // Post-fix: A's scan observed X2's committed reference and kept the blob.
        assert!(
            wasm_path.exists(),
            "shared WASM blob must survive a concurrent store/remove race (#4216)"
        );

        // And X2 is fetchable from a FRESH cold-cache store over the same db,
        // forcing the disk read that pre-fix corruption breaks.
        let fresh = ContractStore::new(contract_dir.path().into(), 10_000, db)?;
        assert!(
            fresh.fetch_contract(&key2, &params2).is_some(),
            "newly stored instance must be fetchable from disk after the race (#4216)"
        );

        Ok(())
    }

    /// Regression test for issue #4218: pool executors must share ONE contract
    /// instance index. A contract stored via executor A must be visible to
    /// `code_hash_from_id` / `fetch_contract` on executor B without rebuilding
    /// B's store.
    ///
    /// Two `ContractStore`s built with `new_with_shared_index` over the SAME
    /// shared `Arc<DashMap>` (as `RuntimePool` now wires them) model executors A
    /// and B. Pre-fix, each `ContractStore::new` built its own `Arc<DashMap>`,
    /// so B's index never saw A's instance and both lookups returned `None`.
    #[tokio::test]
    async fn test_shared_index_visible_across_executors() -> Result<(), Box<dyn std::error::Error>>
    {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        let shared_index: SharedContractIndex = Arc::new(DashMap::new());
        let mut store_a = ContractStore::new_with_shared_index(
            contract_dir.path().into(),
            10_000,
            db.clone(),
            shared_index.clone(),
        )?;
        let store_b = ContractStore::new_with_shared_index(
            contract_dir.path().into(),
            10_000,
            db,
            shared_index,
        )?;

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![3, 1, 4, 1, 5])),
            [9, 9].as_ref().into(),
        );
        let key = *contract.key();
        let params: Parameters = [9, 9].as_ref().into();

        // Stored via executor A only.
        store_a.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract,
        )))?;

        // Executor B must resolve the instance and fetch it via the SHARED index.
        assert_eq!(
            store_b.code_hash_from_id(key.id()),
            Some(*key.code_hash()),
            "instance stored via executor A must be resolvable on executor B \
             through the shared index (#4218 problem 1)"
        );
        assert!(
            store_b.fetch_contract(&key, &params).is_some(),
            "contract stored via executor A must be fetchable on executor B \
             through the shared index (#4218 problem 1)"
        );

        Ok(())
    }

    /// Regression test for issue #4218: a removal on executor A must be
    /// immediately authoritative on executor B — B must NOT serve the removed
    /// instance as a "ghost" from its own still-warm code cache.
    ///
    /// We warm B's `contract_cache` for the shared code hash (by fetching a
    /// SECOND instance of the same code through B), then remove the first
    /// instance via A. With the shared index + the `fetch_contract` index gate,
    /// B's fetch of the removed instance returns `None` even though the code
    /// hash is still cached on B (still referenced by the surviving instance).
    #[tokio::test]
    async fn test_shared_index_remove_closes_ghost_across_executors()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        let shared_index: SharedContractIndex = Arc::new(DashMap::new());
        let mut store_a = ContractStore::new_with_shared_index(
            contract_dir.path().into(),
            10_000,
            db.clone(),
            shared_index.clone(),
        )?;
        let store_b = ContractStore::new_with_shared_index(
            contract_dir.path().into(),
            10_000,
            db,
            shared_index,
        )?;

        // Two instances of the SAME code (same code hash, different params).
        let shared_code = vec![7, 7, 7, 7];
        let params1 = Parameters::from(vec![1, 0, 0]);
        let params2 = Parameters::from(vec![2, 0, 0]);
        let contract1 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params1.clone(),
        );
        let contract2 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params2.clone(),
        );
        let key1 = *contract1.key();
        let key2 = *contract2.key();
        assert_eq!(key1.code_hash(), key2.code_hash());

        store_a.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract1,
        )))?;
        store_a.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract2,
        )))?;

        // Warm B's per-executor code cache for the shared code hash by fetching
        // the SURVIVING instance through B.
        assert!(
            store_b.fetch_contract(&key2, &params2).is_some(),
            "surviving instance must be fetchable on B (warms B's code cache)"
        );

        // Remove instance 1 via executor A. The shared blob survives (instance 2
        // still references the code hash), but instance 1 is gone from the index.
        store_a.remove_contract(&key1)?;

        // B must NOT serve instance 1 as a ghost even though the shared code hash
        // is warm in B's cache.
        assert!(
            store_b.fetch_contract(&key1, &params1).is_none(),
            "instance removed via executor A must not be served as a ghost by B \
             from a stale warm cache (#4218 problem 2)"
        );
        // The surviving instance is unaffected.
        assert!(
            store_b.fetch_contract(&key2, &params2).is_some(),
            "surviving instance must remain fetchable on B after the other is removed"
        );

        Ok(())
    }

    /// Regression test for issue #4218 (disk-budget double-count): the dedup
    /// probe that gates the wasm disk-budget charge must be keyed by CODE HASH,
    /// so a NEW instance of already-stored code (same code hash, different
    /// params) is recognised as "already stored" and not charged a second time
    /// — even on a DIFFERENT pool executor whose code cache is cold.
    ///
    /// This pins `code_blob_stored` (the probe used by the executor gate sites)
    /// against the exact cross-executor scenario. The commented contrast shows
    /// why the old instance-keyed `fetch_contract` probe double-counted: it
    /// returns `None` here for the second instance, so the gate would charge the
    /// shared blob twice.
    #[tokio::test]
    async fn test_dedup_probe_shared_code_not_double_charged_across_executors()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        let shared_index: SharedContractIndex = Arc::new(DashMap::new());
        let mut store_a = ContractStore::new_with_shared_index(
            contract_dir.path().into(),
            10_000,
            db.clone(),
            shared_index.clone(),
        )?;
        let store_b = ContractStore::new_with_shared_index(
            contract_dir.path().into(),
            10_000,
            db,
            shared_index,
        )?;

        let shared_code = vec![5, 5, 5, 5, 5];
        let params1 = Parameters::from(vec![1, 2, 3]);
        let params2 = Parameters::from(vec![4, 5, 6]);
        let contract1 =
            WrappedContract::new(Arc::new(ContractCode::from(shared_code.clone())), params1);
        let contract2 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params2.clone(),
        );
        let key1 = *contract1.key();
        let key2 = *contract2.key();
        let code_hash = *key1.code_hash();
        assert_eq!(key1.code_hash(), key2.code_hash());

        // Instance 1 stored (and blob charged) via executor A.
        store_a.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract1,
        )))?;

        // The dedup probe on executor B (cold code cache) must report the shared
        // blob as ALREADY stored, so PUTting instance 2 charges nothing more.
        assert!(
            store_b.code_blob_stored(&code_hash),
            "the shared code blob must be recognised as stored on executor B \
             (dedup — no double-count of the disk budget) (#4218)"
        );

        // Contrast: the OLD instance-keyed probe returns None for the never-yet-
        // stored second instance, which is exactly what made the gate charge the
        // shared blob twice before this fix.
        assert!(
            store_b.fetch_contract(&key2, &params2).is_none(),
            "instance-keyed fetch_contract returns None for a new instance — the \
             old dedup probe would have double-charged the shared blob"
        );

        Ok(())
    }
}
