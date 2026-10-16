use dashmap::DashMap;
use freenet_stdlib::prelude::{
    APIVersion, CodeHash, Delegate, DelegateCode, DelegateContainer, DelegateKey,
    DelegateWasmAPIVersion, Parameters,
};
use moka::sync::Cache as MokaCache;
use std::{
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    sync::Arc,
};

use crate::contract::delegate_capabilities::MAX_STORED_PARAMS_BYTES;
use crate::contract::storages::Storage;

use super::RuntimeResult;

/// Registration record version for .reg files
const REG_FILE_VERSION: u8 = 1;

/// In the name of a temporary `.reg` record (`ensure_reg_file`), which never
/// ends in `.reg` itself.
const REG_TMP_MARKER: &str = ".reg.tmp-";

/// Delegate instance index (`DelegateKey -> CodeHash`) shared across every pool
/// executor's [`DelegateStore`], so a delegate registered or removed via one
/// executor is immediately visible to all of them.
///
/// The contract side has had this since #4218; the delegate side did not, so each
/// executor's index froze at whatever ReDb held when it was constructed. That was
/// survivable only because a warm (shared) compiled-module cache let a sibling
/// executor run a delegate its own index had never heard of — which stops being
/// true once the module cache is keyed by an index-resolved code hash (#5268), so
/// the divergence has to be closed rather than relied upon.
pub type SharedDelegateIndex = Arc<DashMap<DelegateKey, CodeHash>>;

/// Source-WASM byte cache shared across every pool executor's [`DelegateStore`].
/// See [`crate::wasm_runtime::SharedCodeCache`] for why these are shared rather
/// than built per executor.
pub type SharedDelegateCodeCache = MokaCache<CodeHash, Arc<DelegateCode<'static>>>;

/// Build a source-WASM byte cache bounded by `max_size` bytes.
pub fn new_code_cache(max_size: u64) -> SharedDelegateCodeCache {
    MokaCache::builder()
        .max_capacity(max_size)
        .weigher(
            |key: &CodeHash, value: &Arc<DelegateCode<'static>>| -> u32 {
                // Saturate to u32::MAX on overflow as moka recommends. A delegate
                // WASM module larger than 4 GiB would indicate a bug in upstream
                // size validation — log it loudly.
                let len = value.as_ref().as_ref().len();
                u32::try_from(len).unwrap_or_else(|_| {
                    tracing::warn!(
                        code_hash = %key,
                        size_bytes = len,
                        "Delegate code exceeds u32::MAX in cache weigher; \
                         saturating. This should be impossible."
                    );
                    u32::MAX
                })
            },
        )
        .build()
}

pub struct DelegateStore {
    delegates_dir: PathBuf,
    delegate_cache: MokaCache<CodeHash, Arc<DelegateCode<'static>>>,
    /// In-memory index: DelegateKey -> CodeHash
    /// Populated from .reg files + ReDb on startup and kept in sync.
    key_to_code_part: Arc<DashMap<DelegateKey, CodeHash>>,
    /// ReDb storage for persistent index (primary runtime store)
    db: Storage,
}

impl DelegateStore {
    /// # Arguments
    /// - delegates_dir: directory where delegate WASM files and .reg records are stored
    /// - max_size: max size in bytes of the delegates being cached
    /// - db: ReDb storage for persistent index
    pub fn new(delegates_dir: PathBuf, max_size: u64, db: Storage) -> RuntimeResult<Self> {
        Self::new_with_shared(
            delegates_dir,
            db,
            SharedDelegateIndex::default(),
            new_code_cache(max_size),
        )
    }

    /// Like [`Self::new`] but adopting a caller-owned index and source-byte cache
    /// so every pool executor's store shares one of each. Both are populated
    /// idempotently, so it does not matter which executor is constructed first.
    /// See [`SharedDelegateIndex`] and [`SharedDelegateCodeCache`].
    pub fn new_with_shared(
        delegates_dir: PathBuf,
        db: Storage,
        shared_index: SharedDelegateIndex,
        shared_code_cache: SharedDelegateCodeCache,
    ) -> RuntimeResult<Self> {
        std::fs::create_dir_all(&delegates_dir).map_err(|err| {
            tracing::error!("error creating delegate dir: {err}");
            err
        })?;

        let key_to_code_part = shared_index;

        // Phase 1: Load index from ReDb (primary store)
        match db.load_all_delegate_index() {
            Ok(entries) => {
                for (delegate_key, code_hash) in entries {
                    key_to_code_part.insert(delegate_key, code_hash);
                }
                tracing::debug!(
                    "Loaded {} delegate index entries from ReDb",
                    key_to_code_part.len()
                );
            }
            Err(e) => {
                tracing::warn!("Failed to load delegate index from ReDb: {e}");
            }
        }

        // Phase 2: Restore any .reg entries missing from ReDb (crash recovery).
        let mut reg_count = 0u32;
        let mut restored_count = 0u32;

        if let Ok(dir) = std::fs::read_dir(&delegates_dir) {
            for entry in dir.flatten() {
                let path = entry.path();
                // A temporary record left by a crash mid-repair
                // (`ensure_reg_file`). Never a record itself. Deleting it
                // unconditionally relies on no `ensure_reg_file` being in
                // flight while a store is built over this directory: both run
                // on the contract loop (registration, replacement executors)
                // or before it starts (pool start-up), one at a time.
                if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.contains(REG_TMP_MARKER))
                {
                    drop(std::fs::remove_file(&path));
                    continue;
                }
                if path.extension().is_none_or(|e| e != "reg") {
                    continue;
                }
                let Some(dk_encoded) = path.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                let data = match std::fs::read(&path) {
                    Ok(d) => d,
                    Err(e) => {
                        tracing::warn!("Failed to read .reg file {}: {e}", path.display());
                        continue;
                    }
                };
                let Some((code_hash, _params)) = parse_reg_file(&data) else {
                    tracing::warn!(
                        "Failed to parse .reg file {} (corrupt or unsupported version)",
                        path.display()
                    );
                    continue;
                };

                let dk_bytes: [u8; 32] = match bs58::decode(dk_encoded)
                    .with_alphabet(bs58::Alphabet::BITCOIN)
                    .into_vec()
                    .ok()
                    .and_then(|b| b.try_into().ok())
                {
                    Some(arr) => arr,
                    None => {
                        tracing::warn!("Invalid delegate key encoding in filename: {dk_encoded}");
                        continue;
                    }
                };

                let delegate_key = DelegateKey::new(dk_bytes, code_hash);
                reg_count += 1;

                // Restore to ReDb + DashMap if missing
                if !key_to_code_part.contains_key(&delegate_key) {
                    if let Err(e) = db.store_delegate_index(&delegate_key, &code_hash) {
                        tracing::warn!("Failed to restore .reg entry to ReDb: {e}");
                    }
                    key_to_code_part.insert(delegate_key, code_hash);
                    restored_count += 1;
                }
            }
        }

        if restored_count > 0 {
            tracing::info!(
                "Restored {restored_count} delegate index entries from .reg files ({reg_count} total .reg files)"
            );
        }

        tracing::debug!("Total delegate index entries: {}", key_to_code_part.len());

        // Migrate any delegate WASM files written under the legacy lowercased
        // Base58 name to the canonical mixed-case name (issue #4214). The .reg
        // records are keyed by DelegateKey::encode(), which never lowercased, so
        // only the code_hash-named .wasm files need migrating to stay reachable
        // across the stdlib CodeHash::encode case-fix.
        for entry in key_to_code_part.iter() {
            super::migrate_legacy_lowercased_code_file(
                &delegates_dir,
                &entry.value().encode(),
                "wasm",
            );
        }

        Ok(Self {
            delegate_cache: shared_code_cache,
            delegates_dir,
            key_to_code_part,
            db,
        })
    }

    /// Whether any delegate this node still has indexed runs `code_hash`.
    ///
    /// Mirrors the reference check [`Self::remove_delegate`] already applies
    /// before deleting a shared `.wasm` blob; callers use it to decide whether a
    /// compiled module keyed by that code hash is still needed.
    pub fn code_still_referenced(&self, code_hash: &CodeHash) -> bool {
        self.key_to_code_part
            .iter()
            .any(|entry| entry.value() == code_hash)
    }

    // Returns a copy of the delegate bytes if available, none otherwise.
    pub fn fetch_delegate(
        &self,
        key: &DelegateKey,
        params: &Parameters<'_>,
    ) -> Option<Delegate<'static>> {
        // Resolve the code hash from the shared INDEX, never from the `code_hash`
        // field the caller's `DelegateKey` carries, and gate on the index being
        // populated for this key at all.
        //
        // The disk path below was already index-gated; the in-memory cache fast
        // path was not, and it looked the code up by `key.code_hash()`. Since the
        // cache is keyed by code hash rather than by delegate key, that served
        // whatever code was warm under the hash the caller NAMED, for a key this
        // node had never recorded — so whether an unregistered key resolved at
        // all depended only on the cache being warm. Resolving through the index
        // makes the answer a function of what this node itself registered, and
        // `store_delegate` verifies that identity on the way in (see
        // `verify_delegate_identity`).
        let code_hash = *self.key_to_code_part.get(key)?.value();

        if let Some(delegate_code) = self.delegate_cache.get(&code_hash) {
            return Some(Delegate::from((&*delegate_code, params)).into_owned());
        }

        let delegate_code_path = self
            .delegates_dir
            .join(code_hash.encode())
            .with_extension("wasm");
        tracing::debug!("loading delegate `{key}` from {delegate_code_path:?}");
        let DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate {
            data: delegate_code,
            ..
        })) = DelegateContainer::try_from((
            delegate_code_path.as_path(),
            params.clone().into_owned(),
        ))
        .ok()?
        else {
            tracing::warn!("unsupported delegate container version for key `{key}`");
            return None;
        };
        tracing::debug!("loaded `{key}` from path");
        let delegate = Delegate::from((&delegate_code, &params.clone().into_owned()));
        self.delegate_cache
            .insert(code_hash, Arc::new(delegate_code));
        Some(delegate)
    }

    /// Ensures the index mapping and .reg backup exist for a key, repairing if missing.
    fn ensure_index_entry(
        &mut self,
        key: &DelegateKey,
        code_hash: &CodeHash,
        params: &Parameters<'_>,
    ) -> RuntimeResult<()> {
        // Ensure the .reg record holds this registration: a backup for crash
        // recovery, and where node-started runs read their parameters (#5616).
        ensure_reg_file(&self.delegates_dir, key, code_hash, params)?;

        if !self.key_to_code_part.contains_key(key) {
            self.db
                .store_delegate_index(key, code_hash)
                .map_err(|e| anyhow::anyhow!("Failed to store delegate index: {e}"))?;
            self.key_to_code_part.insert(key.clone(), *code_hash);
        }
        Ok(())
    }

    #[allow(clippy::wildcard_enum_match_arm)] // DelegateContainer is #[non_exhaustive]
    fn extract_params(delegate: &DelegateContainer) -> Option<Parameters<'static>> {
        match delegate {
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(d)) => {
                Some(d.params().clone().into_owned())
            }
            _ => {
                tracing::warn!("unsupported delegate container version");
                None
            }
        }
    }

    /// Check that a delegate's key is actually derived from the bytes it ships
    /// with, and refuse it otherwise.
    ///
    /// # Why this has to be checked here
    ///
    /// A `DelegateKey` is two fields, the 32-byte `key` and a `code_hash`, and
    /// both are ordinary serde fields that survive a wire round-trip exactly as
    /// sent: `Delegate` derives `Deserialize`, so its generated impl fills `key`
    /// in field-by-field and nothing recomputes it. `DelegateCode` has the same
    /// shape — `DelegateCode::hash()` returns a stored field rather than hashing
    /// the bytes (it still carries a `// todo: skip serializing and instead
    /// compute it`), and its `PartialEq` compares only that field. So a
    /// container's claimed identity and its actual content are independent until
    /// something derives one from the other.
    ///
    /// [`DelegateStore`] is where that stops being merely untidy. `store_delegate`
    /// uses the claimed code hash as the blob FILENAME, and the delegate
    /// directory is content-addressed by that name: `fetch_delegate` and the
    /// `store_delegate` early-return paths both load `<code_hash>.wasm` and hand
    /// back whatever it contains. An identity the node never derived would
    /// therefore decide, durably, which bytes it believes are a given delegate's
    /// code — including for a legitimate delegate registered later whose code
    /// genuinely does hash to that name, which would pick up the bytes already
    /// sitting there.
    ///
    /// # The two checks, and why the order matters
    ///
    /// 1. `CodeHash::from_code(code.data())` must equal the code hash the
    ///    container claims (on the key AND on the code itself). This is the only
    ///    check that touches the bytes, so it has to come first.
    /// 2. The 32-byte key half must be derived from that code hash and these
    ///    parameters. That derivation is `BLAKE3(code.hash() ‖ params)` and it
    ///    reads `code.hash()`, i.e. the stored field, so it is only meaningful
    ///    ONCE check 1 has established that the field matches the bytes. Doing 2
    ///    alone would verify a claim against another claim.
    ///
    /// Both together are what bind key, code hash and bytes into one identity.
    /// Check 1 alone would still allow well-formed code to be filed under an
    /// unrelated key; check 2 alone is circular.
    ///
    /// Deriving the key via the stdlib's own `Delegate::from((code, params))`
    /// rather than re-implementing `BLAKE3(hash ‖ params)` here is deliberate: a
    /// local copy of that formula would silently diverge if the derivation ever
    /// changed. (`DelegateKey::from_params_and_code`, which that `From` impl
    /// calls, is private to the stdlib, so the `From` impl is the way to reach
    /// it.)
    ///
    /// # How this differs from the contract-store check
    ///
    /// `ContractStore::verify_contract_identity` also protects an instance→code
    /// INDEX from being re-pointed, because `ContractKey`'s `Hash`/`Eq` compare
    /// its `instance` alone, so a container claiming an existing instance with
    /// different code overwrites that instance's index row. `DelegateKey`'s
    /// derived `Hash`/`Eq` compare BOTH fields, and the ReDb index row key is the
    /// full 64 bytes, so a container with an unrelated key half addresses a
    /// different row and cannot re-point an existing one. The index half of that
    /// problem therefore does not arise here; the content-addressed blob name
    /// does, and that is what these checks cover.
    fn verify_delegate_identity(
        key: &DelegateKey,
        code: &DelegateCode<'_>,
        params: &Parameters<'_>,
    ) -> RuntimeResult<()> {
        // `DelegateCode::from(&[u8])` hashes the slice it is handed and BORROWS
        // it, so this is one BLAKE3 pass and no copy of the module. It also
        // doubles as the input to check 2, which needs a `DelegateCode` whose
        // stored hash is known-good.
        let recomputed = DelegateCode::from(code.data());
        let actual_code_hash = *recomputed.hash();

        // Check 1: the claimed code hash(es) must be the hash of these bytes.
        // Both the key's copy and the code's own copy are checked, because they
        // are separate fields and either could be the one a later reader trusts.
        if actual_code_hash != *key.code_hash() || actual_code_hash != *code.hash() {
            return Err(super::RuntimeInnerError::DelegateIdentityMismatch {
                key: Box::new(key.clone()),
                detail: format!(
                    "code hashes to {actual_code_hash} but the key claims {} and the code claims {}",
                    key.code_hash(),
                    code.hash()
                ),
            }
            .into());
        }

        // Check 2: sound only now that check 1 passed (see rustdoc).
        let derived = Delegate::from((&recomputed, params));
        if derived.key() != key {
            return Err(super::RuntimeInnerError::DelegateIdentityMismatch {
                key: Box::new(key.clone()),
                detail: format!(
                    "key {key} is not derived from this code and these {} parameter byte(s) \
                     (derivation gives {})",
                    params.as_ref().len(),
                    derived.key()
                ),
            }
            .into());
        }

        Ok(())
    }

    pub fn store_delegate(&mut self, delegate: DelegateContainer) -> RuntimeResult<()> {
        let code_hash = delegate.code_hash();
        let key = delegate.key();
        let Some(params) = Self::extract_params(&delegate) else {
            return Err(anyhow::anyhow!("unsupported delegate container version").into());
        };

        // Verify the identity BEFORE anything durable happens — before the .reg
        // write and the ReDb row that the cache/disk early-return paths below
        // perform via `ensure_index_entry`, and before the blob write further
        // down. This is pure computation (one BLAKE3 pass over the WASM) on a path
        // that only runs at registration, so its cost is not on any hot path. A
        // refusal therefore leaves no blob, no .reg record, no index row and no
        // commit, so it is idempotent under retry. See `verify_delegate_identity`.
        if let Err(err) = Self::verify_delegate_identity(key, delegate.code(), &params) {
            // WARN, not debug: this is the node declining to file bytes under an
            // identity it did not derive. It should be visible in an operator's
            // log without a rebuild (`debug!` compiles out in release builds).
            tracing::warn!(
                delegate = %key,
                code_bytes = delegate.code().data().len(),
                "refusing to store delegate: {err}"
            );
            return Err(err);
        }

        // Early return if already in cache - but ensure index and .reg are updated
        if self.delegate_cache.get(code_hash).is_some() {
            self.ensure_index_entry(key, code_hash, &params)?;
            return Ok(());
        }

        let key_path = code_hash.encode();
        let delegate_path = self.delegates_dir.join(key_path).with_extension("wasm");

        // Early return if file exists on disk - but ensure index and .reg are updated
        if let Ok((code, _ver)) = DelegateCode::load_versioned_from_path(delegate_path.as_path()) {
            self.ensure_index_entry(key, code_hash, &params)?;
            self.delegate_cache.insert(*code_hash, Arc::new(code));
            return Ok(());
        }

        // Write order: WASM -> .reg -> ReDb -> in-memory -> cache.
        // .reg files ensure the index can be rebuilt if ReDb entries are ever lost.

        let version = APIVersion::from(delegate.clone());
        let output: Vec<u8> = delegate
            .code()
            .to_bytes_versioned(version)
            .map_err(|e| anyhow::anyhow!(e))?;
        let mut file = File::create(&delegate_path)?;
        file.write_all(output.as_slice())?;
        file.sync_all()?;

        ensure_reg_file(&self.delegates_dir, key, code_hash, &params)?;

        self.db
            .store_delegate_index(key, code_hash)
            .map_err(|e| anyhow::anyhow!("Failed to store delegate index: {e}"))?;

        self.key_to_code_part.insert(key.clone(), *code_hash);

        self.delegate_cache
            .insert(*code_hash, Arc::new(delegate.code().clone().into_owned()));

        Ok(())
    }

    pub fn remove_delegate(&mut self, key: &DelegateKey) -> RuntimeResult<()> {
        let code_hash = *key.code_hash();
        self.delegate_cache.invalidate(&code_hash);

        // Remove .reg file FIRST to prevent resurrection on crash.
        // If we crash after ReDb removal but before .reg removal, startup
        // reconciliation would restore the deleted delegate from the stale .reg.
        let reg_path = self.delegates_dir.join(key.encode()).with_extension("reg");
        if let Err(err) = std::fs::remove_file(&reg_path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(err.into());
            }
        }

        // Remove from ReDb index
        self.db
            .remove_delegate_index(key)
            .map_err(|e| anyhow::anyhow!("Failed to remove delegate index: {e}"))?;

        // Remove from in-memory index
        self.key_to_code_part.remove(key);

        // Remove .wasm file (keyed by code_hash) only if no other delegate uses it
        let other_delegates_use_code = self
            .key_to_code_part
            .iter()
            .any(|entry| *entry.value() == code_hash);
        if !other_delegates_use_code {
            let wasm_path = self
                .delegates_dir
                .join(code_hash.encode())
                .with_extension("wasm");
            if let Err(err) = std::fs::remove_file(&wasm_path) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    return Err(err.into());
                }
            }
        }

        Ok(())
    }

    pub fn get_delegate_path(&mut self, key: &DelegateKey) -> RuntimeResult<PathBuf> {
        let code_hash = key.code_hash();
        Ok(self
            .delegates_dir
            .join(code_hash.encode())
            .with_extension("wasm"))
    }

    /// The code hash this node recorded for `key`, or `None` when it never
    /// registered that delegate.
    ///
    /// The authoritative resolution the compiled-module cache keys on (#5268),
    /// for exactly the reason [`Self::fetch_delegate`] documents: the `code_hash`
    /// a caller's `DelegateKey` carries is unverified serde data, so trusting it
    /// would let a caller name a delegate while choosing which cached module ran
    /// for it.
    pub fn code_hash_from_key(&self, key: &DelegateKey) -> Option<CodeHash> {
        self.key_to_code_part.get(key).map(|r| *r.value())
    }

    /// The parameters `key` was registered with on this node. See
    /// [`registered_params`].
    pub fn registered_params(
        &self,
        key: &DelegateKey,
    ) -> Result<Parameters<'static>, ParamsUnavailable> {
        registered_params(&self.delegates_dir, &self.key_to_code_part, key)
    }
}

/// Why [`registered_params`] could not establish a delegate's parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamsUnavailable {
    /// This node has not registered the key.
    NotRegistered,
    /// No registration record, and the key is not derived from empty
    /// parameters, so they cannot be recovered.
    NoRecord,
    /// The registration record exists but could not be read.
    Unreadable(std::io::ErrorKind),
    /// The record holds more than `MAX_STORED_PARAMS_BYTES` of parameters.
    TooLarge,
    /// The record is corrupt, names different code than the index, or its
    /// parameters do not derive the key.
    Invalid,
    /// The executor has no delegate store to ask.
    NoStore,
}

impl std::fmt::Display for ParamsUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NotRegistered => "the delegate is not registered on this node",
            Self::NoRecord => {
                "the delegate has no registration record and its key is not derived \
                 from empty parameters (registering it again restores the record)"
            }
            Self::Unreadable(kind) => {
                return write!(
                    f,
                    "the delegate's registration record could not be read ({kind})"
                );
            }
            Self::TooLarge => "the delegate's registered parameters exceed the size limit",
            Self::Invalid => {
                "the delegate's registration record is corrupt or does not match its \
                 key (registering it again normally repairs the record)"
            }
            Self::NoStore => "this executor has no delegate store",
        })
    }
}

/// Header of a `.reg` record: version, code hash, parameter length.
const REG_HEADER_LEN: usize = 1 + 32 + 4;

/// The parameters `key` was registered with on this node, for a run the NODE
/// starts (a contract notification, an inter-delegate hop) and so has no
/// caller to supply them (#5616). An error means this node cannot establish
/// them, and the caller must not run the delegate: running it with other
/// parameters (it used to get empty ones) runs a different instance of it,
/// silently.
///
/// # Source
///
/// - **The key itself, first.** A delegate key is `BLAKE3(code_hash ‖ params)`,
///   so a key that derives from EMPTY parameters proves they were empty. That
///   is the common case and needs no IO, and it holds whatever state the
///   record is in.
/// - **Otherwise the `.reg` record** [`DelegateStore::store_delegate`] writes
///   for every registration (#3344), which holds the parameters verbatim. The
///   index is not enough: it maps a key to its code hash only.
///
/// # What is checked
///
/// - **Index-gated**, for the trust reason `fetch_delegate` gives: the answer
///   is a function of what this node registered, not of which files exist.
/// - **The record's code hash must match the index**, and **the key must be
///   derived from that code hash and the record's parameters**. The key is a
///   commitment to the parameters, so a record that does not reproduce it is
///   refused rather than used.
/// - **Bounded read**: at most `MAX_STORED_PARAMS_BYTES` (64 KiB) of
///   parameters, since this runs on the contract loop. The limit is borrowed
///   from the capability record, which applies it to the parameters it keeps
///   for lifecycle and wake-up runs; for notifications and hops it is new
///   with #5616. A delegate over it gets no node-started runs.
///
/// A missing or damaged record is normally repaired by the next registration
/// of the delegate (`ensure_reg_file`, which logs a record it cannot replace).
///
/// Every run, client-started or not, is also checked against the key in
/// `Runtime::prepare_delegate_call`.
pub fn registered_params(
    delegates_dir: &std::path::Path,
    index: &DashMap<DelegateKey, CodeHash>,
    key: &DelegateKey,
) -> Result<Parameters<'static>, ParamsUnavailable> {
    let code_hash = *index
        .get(key)
        .ok_or(ParamsUnavailable::NotRegistered)?
        .value();
    let empty = Parameters::from(Vec::<u8>::new());
    if key_derives_from(key, &code_hash, &empty) {
        return Ok(empty);
    }

    let reg_path = delegates_dir.join(key.encode()).with_extension("reg");
    let file = match File::open(&reg_path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(ParamsUnavailable::NoRecord);
        }
        Err(err) => return Err(ParamsUnavailable::Unreadable(err.kind())),
    };
    // One byte past the limit, so an oversized record is detected rather than
    // silently truncated into a parse failure.
    let limit = (REG_HEADER_LEN + MAX_STORED_PARAMS_BYTES + 1) as u64;
    let mut data = Vec::new();
    if let Err(err) = file.take(limit).read_to_end(&mut data) {
        return Err(ParamsUnavailable::Unreadable(err.kind()));
    }
    if data.len() > REG_HEADER_LEN + MAX_STORED_PARAMS_BYTES {
        return Err(ParamsUnavailable::TooLarge);
    }
    let (reg_code_hash, params) = parse_reg_file(&data).ok_or(ParamsUnavailable::Invalid)?;
    if reg_code_hash != code_hash || !key_derives_from(key, &code_hash, &params) {
        return Err(ParamsUnavailable::Invalid);
    }
    Ok(params)
}

/// Whether `key` is the delegate key of `code_hash` under `params`.
///
/// Pure computation, no IO. `code_hash` must come from this node's index
/// (`DelegateStore::code_hash_from_key`), never from `key.code_hash()`, which
/// is unverified serde data.
pub(crate) fn key_derives_from(
    key: &DelegateKey,
    code_hash: &CodeHash,
    params: &Parameters<'_>,
) -> bool {
    DelegateKey::from_params(code_hash.encode(), params).is_ok_and(|derived| derived == *key)
}

/// Make the `.reg` record for `key` hold exactly this registration, writing it
/// if it is missing and REPLACING it if it holds anything else.
///
/// Replacing matters since #5616: node-started runs read their parameters
/// from this record, so a record left truncated by a crash or a full disk
/// mid-write would otherwise silence the delegate's notifications on this
/// node for good, because re-registering used to keep any existing record.
/// The caller has verified the registration's identity
/// (`verify_delegate_identity`), so its parameters are the authoritative
/// ones. The write goes through a temporary file and a rename, so a record is
/// never observed half-written.
///
/// Errors when no record can be seen (missing, or its existence cannot be
/// established) and one cannot be written, which is what the old
/// `create_new` write did. A record that exists but cannot be read or
/// replaced is logged and left: failing the registration over it would turn
/// "this delegate's node-started runs are skipped" into "the app cannot
/// register its delegate", and would also block the very re-registration
/// that repairs it once the cause clears. Whatever is left in place,
/// `registered_params` re-derives the key from it, so a stale record can only
/// cause skipped runs, never a run with the wrong parameters.
fn ensure_reg_file(
    delegates_dir: &std::path::Path,
    key: &DelegateKey,
    code_hash: &CodeHash,
    params: &Parameters<'_>,
) -> RuntimeResult<()> {
    let reg_path = delegates_dir.join(key.encode()).with_extension("reg");

    let params_bytes = params.as_ref();
    let mut reg = Vec::with_capacity(REG_HEADER_LEN + params_bytes.len());
    reg.push(REG_FILE_VERSION);
    reg.extend_from_slice(code_hash.as_ref());
    reg.extend_from_slice(&(params_bytes.len() as u32).to_le_bytes());
    reg.extend_from_slice(params_bytes);

    // Whether a record exists is decided by its metadata, which needs no file
    // descriptor: an open can fail for reasons that say nothing about the
    // record (out of descriptors, the directory itself unusable), and reading
    // those as "a record exists" would let a registration succeed with no
    // record written at all.
    let record_exists = std::fs::symlink_metadata(&reg_path).is_ok();
    if record_exists {
        // Read one byte more than the expected record, which is all it takes
        // to tell "identical" from "different".
        let existing = File::open(&reg_path).and_then(|file| {
            let mut existing = Vec::new();
            file.take(reg.len() as u64 + 1).read_to_end(&mut existing)?;
            Ok(existing)
        });
        match existing {
            Ok(existing) if existing == reg => return Ok(()),
            Ok(_) => tracing::warn!(
                delegate = %key,
                path = %reg_path.display(),
                "Replacing a delegate registration record that does not match its \
                 registration"
            ),
            Err(e) => tracing::warn!(
                delegate = %key,
                path = %reg_path.display(),
                error = %e,
                "Cannot read a delegate registration record; replacing it"
            ),
        }
    }

    // A name no other writer uses, so two concurrent registrations cannot
    // interleave in one temporary file. It does not end in `.reg`, so the
    // start-up scan never reads one left behind by a crash; it deletes it.
    static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let tmp_path = delegates_dir.join(format!(
        "{}{REG_TMP_MARKER}{}-{}",
        key.encode(),
        std::process::id(),
        TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let write = || -> std::io::Result<()> {
        let mut file = File::create(&tmp_path)?;
        file.write_all(&reg)?;
        file.sync_all()?;
        std::fs::rename(&tmp_path, &reg_path)
    };
    if let Err(e) = write() {
        // Best effort: do not leave the temporary file behind.
        drop(std::fs::remove_file(&tmp_path));
        if !record_exists {
            return Err(e.into());
        }
        tracing::warn!(
            delegate = %key,
            path = %reg_path.display(),
            error = %e,
            "Failed to repair a delegate registration record; its notifications \
             and inter-delegate messages stay skipped until a registration can \
             repair it"
        );
        return Ok(());
    }

    tracing::debug!("Wrote .reg file: {}", reg_path.display());
    Ok(())
}

/// Parse a .reg registration record file.
/// Returns (code_hash, params) if valid, None if corrupt/unsupported.
fn parse_reg_file(data: &[u8]) -> Option<(CodeHash, Parameters<'static>)> {
    // Minimum: 1 (version) + 32 (hash) + 4 (params len) = 37 bytes
    if data.len() < 37 || data[0] != REG_FILE_VERSION {
        return None;
    }
    let mut code_hash_bytes = [0u8; 32];
    code_hash_bytes.copy_from_slice(&data[1..33]);
    let params_len = u32::from_le_bytes(data[33..37].try_into().ok()?) as usize;
    if data.len() < 37 + params_len {
        return None;
    }
    let params = Parameters::from(data[37..37 + params_len].to_vec());
    Some((CodeHash::new(code_hash_bytes), params))
}

#[cfg(test)]
mod test {
    use super::*;

    async fn create_test_db(path: &std::path::Path) -> Storage {
        Storage::new(path).await.expect("failed to create test db")
    }

    /// An honestly-built delegate: `Delegate::from((code, params))` derives the
    /// key, so this is exactly what a well-behaved publisher produces.
    fn honest_delegate(code: Vec<u8>, params: Vec<u8>) -> DelegateContainer {
        let delegate = Delegate::from((&code.into(), &params.into()));
        DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
    }

    /// Rebuild a `DelegateContainer` with its three CLAIMED identity fields
    /// replaced, by round-tripping through the same bincode encoding that
    /// `EncodingProtocol::Native` decodes a `RegisterDelegate` request with.
    ///
    /// `Delegate`'s `key` field and `DelegateCode`'s `code_hash` field are both
    /// private to the stdlib, so an in-memory container cannot be edited — but
    /// they are ordinary serde fields, which is the entire reason
    /// `verify_delegate_identity` exists, so a SERIALIZED container can. Going
    /// through serde is also the more faithful fixture: it is the shape a
    /// container genuinely arrives in.
    ///
    /// The three 32-byte fields are the last bytes of the encoding, in the order
    /// `code.code_hash`, `key.key`, `key.code_hash` (`Delegate` is
    /// `parameters, data, key` and `DelegateCode` is `data, code_hash`). The
    /// assertions pin that layout, so if the stdlib ever reorders these fields
    /// this fixture fails loudly instead of quietly patching the wrong bytes and
    /// leaving the tests below asserting nothing.
    fn with_claimed_identity(
        delegate: &DelegateContainer,
        claimed_code_hash_on_code: CodeHash,
        claimed_key_bytes: [u8; 32],
        claimed_code_hash_on_key: CodeHash,
    ) -> DelegateContainer {
        let mut bytes = bincode::serialize(delegate).expect("container must serialize");
        let len = bytes.len();
        assert!(
            len > 96,
            "encoding is too short to carry three 32-byte identity fields"
        );
        let (code_hash_at, key_at, key_code_hash_at) = (len - 96, len - 64, len - 32);
        assert_eq!(
            &bytes[code_hash_at..key_at],
            delegate.code_hash().as_ref(),
            "fixture is stale: the code's own code_hash is not where it was"
        );
        assert_eq!(
            &bytes[key_at..key_code_hash_at],
            delegate.key().bytes(),
            "fixture is stale: the key's 32-byte half is not where it was"
        );
        assert_eq!(
            &bytes[key_code_hash_at..],
            delegate.key().code_hash().as_ref(),
            "fixture is stale: the key's code_hash is not where it was"
        );

        bytes[code_hash_at..key_at].copy_from_slice(claimed_code_hash_on_code.as_ref());
        bytes[key_at..key_code_hash_at].copy_from_slice(&claimed_key_bytes);
        bytes[key_code_hash_at..].copy_from_slice(claimed_code_hash_on_key.as_ref());

        bincode::deserialize(&bytes).expect("a patched container must still deserialize")
    }

    fn is_identity_mismatch(err: &crate::wasm_runtime::ContractError) -> bool {
        matches!(
            err.deref(),
            crate::wasm_runtime::RuntimeInnerError::DelegateIdentityMismatch { .. }
        )
    }

    /// A delegate whose key IS derived from its own code and parameters stores
    /// normally, and so does one that has been through
    /// [`with_claimed_identity`] without any of its claims changed.
    ///
    /// This is the control for the rejection tests below. Without it they would
    /// all still pass if `verify_delegate_identity` simply refused everything,
    /// and a check that cannot come out clean is not evidence. The unchanged
    /// round-trip is the second half of the control: it shows the fixture's
    /// re-encoding is not itself what the rejections are detecting.
    #[tokio::test]
    async fn store_delegate_accepts_a_key_derived_from_its_own_code()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-derived-key-test");
        std::fs::create_dir_all(&delegate_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        let params = vec![0u8, 1];
        let delegate = honest_delegate(vec![1u8, 2, 3, 4], params.clone());
        let key = delegate.key().clone();

        store.store_delegate(delegate.clone())?;
        assert!(
            store.fetch_delegate(&key, &params.clone().into()).is_some(),
            "an honestly-derived delegate must still be storable and fetchable"
        );

        // Same container, re-encoded but with every claim left as it was.
        let untouched = with_claimed_identity(
            &delegate,
            *delegate.code_hash(),
            delegate
                .key()
                .bytes()
                .try_into()
                .expect("a delegate key half is 32 bytes"),
            *delegate.key().code_hash(),
        );
        assert_eq!(
            untouched.key(),
            &key,
            "the round-trip must preserve the key"
        );
        store.store_delegate(untouched)?;

        Ok(())
    }

    /// The claimed code hash must be the hash of the code actually supplied.
    ///
    /// The delegate directory is content-addressed by that claimed value:
    /// `store_delegate` writes `<code_hash>.wasm`, and both `fetch_delegate` and
    /// `store_delegate`'s own early-return paths read it back by the same name.
    /// Accepting an unverified claim would file bytes under a name they do not
    /// hash to.
    #[tokio::test]
    async fn store_delegate_rejects_code_hash_that_is_not_the_hash_of_the_code()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-bad-code-hash-test");
        std::fs::create_dir_all(&delegate_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        let params = vec![0u8, 1];
        let honest = honest_delegate(vec![1u8, 2, 3, 4], params.clone());

        // Claim a code hash, on both copies, that is not this code's hash.
        let bogus = CodeHash::new([7u8; 32]);
        let forged = with_claimed_identity(
            &honest,
            bogus,
            honest
                .key()
                .bytes()
                .try_into()
                .expect("a delegate key half is 32 bytes"),
            bogus,
        );
        let forged_key = forged.key().clone();

        let err = store
            .store_delegate(forged)
            .expect_err("a code hash that does not match the code must be refused");
        assert!(
            is_identity_mismatch(&err),
            "expected DelegateIdentityMismatch, got: {err}"
        );

        // Nothing durable may have happened: no blob under the claimed hash, no
        // .reg record, and no index entry.
        assert!(
            !delegate_dir
                .join(bogus.encode())
                .with_extension("wasm")
                .exists(),
            "no blob may be written under an unverified code hash"
        );
        assert!(
            !delegate_dir
                .join(forged_key.encode())
                .with_extension("reg")
                .exists(),
            "no .reg record may be written for a refused delegate"
        );
        assert!(
            store.code_hash_from_key(&forged_key).is_none(),
            "no index entry may be written for a refused delegate"
        );
        Ok(())
    }

    /// The 32-byte key half must be derived from the code hash and parameters
    /// supplied.
    ///
    /// This is the case an internally-consistent container can still make: the
    /// code hash genuinely matches the bytes, and only the key half is unrelated
    /// to them. Checking the code hash alone would accept it and register
    /// well-formed code under an arbitrary delegate identity — and a delegate's
    /// identity is what its secret namespace and its context are keyed by.
    #[tokio::test]
    async fn store_delegate_rejects_key_not_derived_from_code_and_params()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-bad-key-test");
        std::fs::create_dir_all(&delegate_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        let code = vec![1u8, 2, 3, 4];
        // Two honest delegates over the SAME code but different parameters, so
        // their key halves differ while their code hashes agree.
        let victim = honest_delegate(code.clone(), vec![0u8]);
        let params_b = vec![9u8];
        let honest_b = honest_delegate(code.clone(), params_b.clone());
        assert_eq!(
            victim.code_hash(),
            honest_b.code_hash(),
            "fixture must share code so only the key half differs"
        );
        assert_ne!(
            victim.key().bytes(),
            honest_b.key().bytes(),
            "fixture must use genuinely different parameters"
        );

        // Correct code hashes for these bytes, but the OTHER delegate's key half.
        let forged = with_claimed_identity(
            &honest_b,
            *honest_b.code_hash(),
            victim
                .key()
                .bytes()
                .try_into()
                .expect("a delegate key half is 32 bytes"),
            *honest_b.key().code_hash(),
        );
        let forged_key = forged.key().clone();

        let err = store
            .store_delegate(forged)
            .expect_err("a key half not derived from this code and params must be refused");
        assert!(
            is_identity_mismatch(&err),
            "expected DelegateIdentityMismatch, got: {err}"
        );
        assert!(
            store.code_hash_from_key(&forged_key).is_none(),
            "a refused delegate must not create an index entry"
        );
        assert!(
            !delegate_dir
                .join(forged_key.encode())
                .with_extension("reg")
                .exists(),
            "a refused delegate must not create a .reg record"
        );
        Ok(())
    }

    /// A refused delegate must not leave code sitting in the content-addressed
    /// blob namespace for a LATER, legitimate delegate to pick up.
    ///
    /// `store_delegate`'s early-return paths take the blob (or the cache entry)
    /// at `<code_hash>.wasm` as authoritative for that code hash: a delegate
    /// whose bytes genuinely hash to it is registered against whatever is already
    /// there without re-reading its own code. So an accepted mis-hashed container
    /// would not merely add a bad file, it would decide which code a legitimate
    /// delegate resolves to.
    #[tokio::test]
    async fn store_delegate_refusal_leaves_the_blob_namespace_holding_its_own_code()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-blob-namespace-test");
        std::fs::create_dir_all(&delegate_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        let params = vec![0u8, 1];
        let code_a = vec![1u8, 2, 3, 4];
        let honest_a = honest_delegate(code_a.clone(), params.clone());
        let key_a = honest_a.key().clone();
        let hash_a = *honest_a.code_hash();

        // Different code, claiming A's whole identity. Registered BEFORE A, so
        // if it were accepted it would be sitting in A's slot when A arrives.
        let other_code = vec![9u8, 9, 9, 9, 9];
        let forged = with_claimed_identity(
            &honest_delegate(other_code, params.clone()),
            hash_a,
            key_a
                .bytes()
                .try_into()
                .expect("a delegate key half is 32 bytes"),
            hash_a,
        );
        assert_eq!(
            forged.key(),
            &key_a,
            "the forged container must claim A's key"
        );

        // The refusal itself is what the two tests above pin, so hold it and
        // check the CONSEQUENCE first. Otherwise removing the verification makes
        // this test fail at the `expect_err` and it never reaches the outcome it
        // exists to describe — a legitimate delegate resolving to code that is
        // not its own — leaving that half unpinned.
        let refusal = store.store_delegate(forged);

        // A now registers honestly and must resolve to its OWN code.
        store.store_delegate(honest_a)?;
        let fetched = store
            .fetch_delegate(&key_a, &params.clone().into())
            .expect("the honest delegate must be served");
        assert_eq!(
            fetched.code().data(),
            code_a.as_slice(),
            "a legitimate delegate must resolve to its own code, whatever was \
             offered for its content-addressed name earlier"
        );
        assert!(
            refusal.is_err_and(|err| is_identity_mismatch(&err)),
            "code that does not hash to the claimed name must be refused"
        );
        Ok(())
    }

    /// `fetch_delegate` must resolve code through the node's own index, not
    /// through the `code_hash` field on the caller's key.
    ///
    /// The cache fast path used to look up `key.code_hash()` with no index check
    /// at all, while the disk path below it was index-gated. Because the cache is
    /// keyed by code hash and not by delegate key, that served warm code for a
    /// key this node had never registered — so whether an unregistered key
    /// resolved depended only on whether some other delegate sharing the code was
    /// still cached.
    #[tokio::test]
    async fn fetch_delegate_resolves_code_through_the_index_not_the_callers_key()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-fetch-index-test");
        std::fs::create_dir_all(&delegate_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        let params = vec![0u8, 1];
        let honest = honest_delegate(vec![1u8, 2, 3, 4], params.clone());
        let key = honest.key().clone();
        let code_hash = *honest.code_hash();
        store.store_delegate(honest)?;

        // Warm: the registered key still resolves.
        assert!(
            store.fetch_delegate(&key, &params.clone().into()).is_some(),
            "the registered delegate must still be fetchable from the cache"
        );

        // A key this node never registered, naming the code hash that IS cached.
        let unregistered = DelegateKey::new([3u8; 32], code_hash);
        assert_ne!(unregistered, key, "fixture must use a different key");
        assert!(
            store
                .fetch_delegate(&unregistered, &params.clone().into())
                .is_none(),
            "a key the node never registered must not be served warm code"
        );
        Ok(())
    }

    #[tokio::test]
    async fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let cdelegate_dir = temp_dir.path().join("delegates-store-test");
        std::fs::create_dir_all(&cdelegate_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(cdelegate_dir.clone(), 10_000, db)?;
        let delegate = {
            let delegate = Delegate::from((&vec![0, 1, 2].into(), &vec![].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        store.store_delegate(delegate.clone())?;
        let f = store.fetch_delegate(delegate.key(), &vec![].into());
        assert!(f.is_some());
        let _cleanup = std::fs::remove_dir_all(&cdelegate_dir);
        Ok(())
    }

    /// Regression test for issue #2845: store_delegate returns Ok but fetch_delegate
    /// fails with "not found" because index wasn't updated in early return paths.
    #[tokio::test]
    async fn store_repairs_missing_index_when_file_exists() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-index-repair-test");
        std::fs::create_dir_all(&delegate_dir)?;

        let delegate = {
            let delegate = Delegate::from((&vec![10, 20, 30].into(), &vec![].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key = delegate.key().clone();
        let code_hash = *delegate.code_hash();

        // Write delegate file directly to disk (simulating previous registration)
        let key_path = code_hash.encode();
        let delegate_path = delegate_dir.join(key_path).with_extension("wasm");
        let version = APIVersion::from(delegate.clone());
        let output: Vec<u8> = delegate
            .code()
            .to_bytes_versioned(version)
            .map_err(|e| anyhow::anyhow!(e))?;
        let mut file = File::create(&delegate_path)?;
        file.write_all(output.as_slice())?;
        file.sync_all()?;

        // Create a fresh store with empty index (simulating lost index)
        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        assert!(delegate_path.exists(), "Delegate file should exist on disk");
        assert!(
            store.key_to_code_part.is_empty(),
            "Index should be empty initially"
        );

        let fetch_before = store.fetch_delegate(&key, &vec![].into());
        assert!(
            fetch_before.is_none(),
            "Fetch should fail before re-registration"
        );

        store.store_delegate(delegate.clone())?;

        assert!(store.key_to_code_part.contains_key(&key));

        let fetch_after = store.fetch_delegate(&key, &vec![].into());
        assert!(
            fetch_after.is_some(),
            "Fetch should succeed after re-registration"
        );

        Ok(())
    }

    /// Regression test for issue #2845: Two delegates with same WASM code but different
    /// parameters should both be fetchable.
    #[tokio::test]
    async fn store_handles_same_code_different_params() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-same-code-test");
        std::fs::create_dir_all(&delegate_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        let wasm_code: Vec<u8> = vec![100, 101, 102, 103];

        let delegate1 = {
            let params1: Vec<u8> = vec![];
            let delegate = Delegate::from((&wasm_code.clone().into(), &params1.into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key1 = delegate1.key().clone();

        let delegate2 = {
            let params2: Vec<u8> = vec![1, 2, 3];
            let delegate = Delegate::from((&wasm_code.clone().into(), &params2.into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key2 = delegate2.key().clone();

        assert_ne!(key1, key2, "Keys should differ when params differ");
        assert_eq!(delegate1.code_hash(), delegate2.code_hash());

        store.store_delegate(delegate1.clone())?;
        assert!(store.key_to_code_part.contains_key(&key1));

        store.store_delegate(delegate2.clone())?;
        assert!(store.key_to_code_part.contains_key(&key2));

        assert!(store.fetch_delegate(&key1, &vec![].into()).is_some());
        assert!(store.fetch_delegate(&key2, &vec![1, 2, 3].into()).is_some());

        Ok(())
    }

    /// A store in a fresh temp dir, plus the dir and its delegates dir.
    async fn params_store() -> (DelegateStore, PathBuf, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let delegate_dir = temp_dir.path().join("delegates");
        let db = create_test_db(temp_dir.path()).await;
        let store = DelegateStore::new(delegate_dir.clone(), 10_000, db).expect("store");
        (store, delegate_dir, temp_dir)
    }

    fn reg_path_of(dir: &std::path::Path, d: &DelegateContainer) -> PathBuf {
        dir.join(d.key().encode()).with_extension("reg")
    }

    fn params_of(
        store: &DelegateStore,
        d: &DelegateContainer,
    ) -> Result<Vec<u8>, ParamsUnavailable> {
        store
            .registered_params(d.key())
            .map(|p| p.as_ref().to_vec())
    }

    /// #5616: `registered_params` recovers exactly the parameters a delegate
    /// was registered with, so a node-started run (contract notification,
    /// inter-delegate hop) can run the SAME instance a client run does, and
    /// keeps two instances of one code apart.
    #[tokio::test]
    async fn registered_params_returns_the_registration_params()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut store, _dir, _tmp) = params_store().await;
        let code: Vec<u8> = vec![7, 7, 7, 7];
        let room_a = honest_delegate(code.clone(), b"room-a".to_vec());
        let room_b = honest_delegate(code.clone(), b"room-b".to_vec());
        let empty = honest_delegate(code, vec![]);
        for delegate in [&room_a, &room_b, &empty] {
            store.store_delegate(delegate.clone())?;
        }

        assert_eq!(params_of(&store, &room_a), Ok(b"room-a".to_vec()));
        assert_eq!(params_of(&store, &room_b), Ok(b"room-b".to_vec()));
        assert_eq!(params_of(&store, &empty), Ok(vec![]));
        Ok(())
    }

    /// Index-gated: a key this node never registered, or has removed, has no
    /// parameters, even when a `.reg` record for it sits in the directory.
    #[tokio::test]
    async fn registered_params_is_index_gated() -> Result<(), Box<dyn std::error::Error>> {
        let (mut store, dir, _tmp) = params_store().await;

        let never = honest_delegate(vec![1, 2, 3], b"p".to_vec());
        ensure_reg_file(
            &dir,
            never.key(),
            never.code_hash(),
            &Parameters::from(b"p".to_vec()),
        )?;
        assert_eq!(
            params_of(&store, &never),
            Err(ParamsUnavailable::NotRegistered)
        );

        let removed = honest_delegate(vec![1, 2, 4], b"q".to_vec());
        store.store_delegate(removed.clone())?;
        assert_eq!(params_of(&store, &removed), Ok(b"q".to_vec()));
        store.remove_delegate(removed.key())?;
        assert_eq!(
            params_of(&store, &removed),
            Err(ParamsUnavailable::NotRegistered)
        );
        Ok(())
    }

    /// The key proves EMPTY parameters without any record, whatever state the
    /// record is in; non-empty parameters cannot be recovered without one.
    #[tokio::test]
    async fn registered_params_without_a_record_only_vouches_for_empty_params()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut store, dir, _tmp) = params_store().await;
        let empty = honest_delegate(vec![4, 4], vec![]);
        let empty_corrupt = honest_delegate(vec![4, 5], vec![]);
        let parameterized = honest_delegate(vec![4, 4], b"room".to_vec());
        for delegate in [&empty, &empty_corrupt, &parameterized] {
            store.store_delegate(delegate.clone())?;
        }
        std::fs::remove_file(reg_path_of(&dir, &empty))?;
        std::fs::write(reg_path_of(&dir, &empty_corrupt), [0xFF, 1, 2])?;
        std::fs::remove_file(reg_path_of(&dir, &parameterized))?;

        assert_eq!(params_of(&store, &empty), Ok(vec![]));
        assert_eq!(
            params_of(&store, &empty_corrupt),
            Ok(vec![]),
            "a damaged record must not cost an empty-params delegate its runs"
        );
        assert_eq!(
            params_of(&store, &parameterized),
            Err(ParamsUnavailable::NoRecord),
            "non-empty params cannot be recovered without a record, and must not \
             be guessed as empty"
        );
        Ok(())
    }

    /// A record that is corrupt, names other code, or whose parameters do not
    /// derive the key is refused rather than used: running with it would be
    /// the #5616 bug. So is one over the size limit.
    #[tokio::test]
    async fn registered_params_refuses_a_record_that_does_not_match_the_key()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut store, dir, _tmp) = params_store().await;
        let tampered = honest_delegate(vec![9, 9], b"real".to_vec());
        let corrupt = honest_delegate(vec![9, 9], b"other".to_vec());
        let other_code = honest_delegate(vec![9, 9], b"third".to_vec());
        let big = vec![0xAB; MAX_STORED_PARAMS_BYTES + 1];
        let oversized = honest_delegate(vec![9, 9], big.clone());
        for delegate in [&tampered, &corrupt, &other_code, &oversized] {
            store.store_delegate(delegate.clone())?;
        }

        ensure_reg_file(
            &dir,
            tampered.key(),
            tampered.code_hash(),
            &Parameters::from(b"fake".to_vec()),
        )?;
        assert_eq!(
            params_of(&store, &tampered),
            Err(ParamsUnavailable::Invalid)
        );

        std::fs::write(reg_path_of(&dir, &corrupt), [REG_FILE_VERSION, 1, 2, 3])?;
        assert_eq!(params_of(&store, &corrupt), Err(ParamsUnavailable::Invalid));

        ensure_reg_file(
            &dir,
            other_code.key(),
            &CodeHash::new([0x42; 32]),
            &Parameters::from(b"third".to_vec()),
        )?;
        assert_eq!(
            params_of(&store, &other_code),
            Err(ParamsUnavailable::Invalid)
        );

        assert_eq!(
            params_of(&store, &oversized),
            Err(ParamsUnavailable::TooLarge)
        );
        Ok(())
    }

    /// Registering again repairs a missing OR damaged record. Before #5616 an
    /// existing record was never rewritten, which was harmless while it was
    /// only a crash-recovery backup; now that node-started runs read their
    /// parameters from it, a record torn by a crash would have silenced the
    /// delegate's notifications on this node for good.
    #[tokio::test]
    async fn registering_again_repairs_a_missing_or_damaged_record()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut store, dir, _tmp) = params_store().await;
        let missing = honest_delegate(vec![3, 3], b"missing".to_vec());
        let torn = honest_delegate(vec![3, 3], b"torn".to_vec());
        // A damaged version byte: any guard that leaves "unknown versions"
        // alone would leave this one unrepaired.
        let garbled = honest_delegate(vec![3, 3], b"garbled".to_vec());
        for delegate in [&missing, &torn, &garbled] {
            store.store_delegate(delegate.clone())?;
        }
        std::fs::remove_file(reg_path_of(&dir, &missing))?;
        std::fs::write(reg_path_of(&dir, &torn), [REG_FILE_VERSION, 1])?;
        std::fs::write(reg_path_of(&dir, &garbled), [0xFF, 9, 9])?;
        for delegate in [&missing, &torn, &garbled] {
            assert!(params_of(&store, delegate).is_err());
        }

        for delegate in [&missing, &torn, &garbled] {
            store.store_delegate(delegate.clone())?;
        }
        assert_eq!(params_of(&store, &missing), Ok(b"missing".to_vec()));
        assert_eq!(params_of(&store, &torn), Ok(b"torn".to_vec()));
        assert_eq!(params_of(&store, &garbled), Ok(b"garbled".to_vec()));
        let leftovers: Vec<_> = std::fs::read_dir(&dir)?
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(REG_TMP_MARKER))
            .collect();
        assert!(leftovers.is_empty(), "no temporary record left behind");
        Ok(())
    }

    /// The size limit is inclusive: params of exactly `MAX_STORED_PARAMS_BYTES`
    /// are returned, one byte more is `TooLarge`.
    #[tokio::test]
    async fn registered_params_size_limit_is_inclusive() -> Result<(), Box<dyn std::error::Error>> {
        let (mut store, _dir, _tmp) = params_store().await;
        let at_limit = vec![0xCD; MAX_STORED_PARAMS_BYTES];
        let at = honest_delegate(vec![6, 6], at_limit.clone());
        let past = honest_delegate(vec![6, 6], vec![0xCD; MAX_STORED_PARAMS_BYTES + 1]);
        store.store_delegate(at.clone())?;
        store.store_delegate(past.clone())?;
        assert_eq!(params_of(&store, &at), Ok(at_limit));
        assert_eq!(params_of(&store, &past), Err(ParamsUnavailable::TooLarge));
        Ok(())
    }

    /// A record that exists but cannot be read is `Unreadable` (with the
    /// reason), and does NOT make a registration fail: the old `create_new`
    /// write never failed over an existing record, and failing would also
    /// block the re-registration that repairs it.
    #[tokio::test]
    async fn an_unreadable_record_is_reported_and_does_not_fail_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut store, dir, _tmp) = params_store().await;
        let delegate = honest_delegate(vec![8, 8], b"room".to_vec());
        store.store_delegate(delegate.clone())?;
        let reg = reg_path_of(&dir, &delegate);
        std::fs::remove_file(&reg)?;
        // A directory where the record should be: opening or reading it fails,
        // and so does renaming a file over it.
        std::fs::create_dir(&reg)?;
        assert!(matches!(
            params_of(&store, &delegate),
            Err(ParamsUnavailable::Unreadable(_))
        ));

        store
            .store_delegate(delegate.clone())
            .expect("an unrepairable existing record must not fail registration");
        let leftovers: Vec<_> = std::fs::read_dir(&dir)?
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(REG_TMP_MARKER))
            .collect();
        assert!(leftovers.is_empty(), "no temporary record left behind");
        Ok(())
    }

    /// A record that cannot be SEEN and cannot be written still fails, as the
    /// old `create_new` write did: reporting success with no record would
    /// leave the delegate with no crash-recovery backup and no node-started
    /// runs, silently. The directory is replaced by a plain file, so opening
    /// the record fails with `NotADirectory` rather than `NotFound`: an open
    /// error that says nothing about the record must not be read as "a record
    /// exists".
    #[cfg(unix)]
    #[tokio::test]
    async fn a_record_that_cannot_be_seen_or_written_fails()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let dir = temp_dir.path().join("delegates");
        std::fs::write(&dir, b"not a directory")?;
        let delegate = honest_delegate(vec![1, 9], b"room".to_vec());
        assert!(
            ensure_reg_file(
                &dir,
                delegate.key(),
                delegate.code_hash(),
                &Parameters::from(b"room".to_vec()),
            )
            .is_err(),
            "a record that cannot be seen or written must fail registration"
        );
        Ok(())
    }

    /// A temporary record left by a crash mid-repair is deleted at start-up
    /// and never read as a record.
    #[tokio::test]
    async fn startup_deletes_temporary_records() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates");
        std::fs::create_dir_all(&delegate_dir)?;
        let delegate = honest_delegate(vec![5, 5], b"room".to_vec());
        let tmp = delegate_dir.join(format!("{}{REG_TMP_MARKER}1-0", delegate.key().encode()));
        // A valid record body, so restoring it would be observable.
        let mut body = vec![REG_FILE_VERSION];
        body.extend_from_slice(delegate.code_hash().as_ref());
        body.extend_from_slice(&4u32.to_le_bytes());
        body.extend_from_slice(b"room");
        std::fs::write(&tmp, body)?;

        let db = create_test_db(temp_dir.path()).await;
        let store = DelegateStore::new(delegate_dir, 10_000, db)?;
        assert!(!tmp.exists(), "the temporary record must be deleted");
        // Also held by the extension filter alone; kept so a change to the
        // scan that reads temporary records as records fails here.
        assert!(
            store.code_hash_from_key(delegate.key()).is_none(),
            "a temporary record must never be restored into the index"
        );
        Ok(())
    }

    /// .reg files enable index recovery when ReDb entries are lost
    #[tokio::test]
    async fn reg_files_restore_lost_redb_entries() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-reg-restore-test");
        std::fs::create_dir_all(&delegate_dir)?;

        let delegate = {
            let delegate = Delegate::from((&vec![42, 43, 44].into(), &vec![7, 8].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key = delegate.key().clone();

        // Store delegate (creates .wasm, .reg, and ReDb entry)
        let db_path = temp_dir.path().join("db1");
        std::fs::create_dir_all(&db_path)?;
        let db = create_test_db(&db_path).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;
        store.store_delegate(delegate.clone())?;

        let reg_path = delegate_dir.join(key.encode()).with_extension("reg");
        assert!(reg_path.exists(), ".reg file should exist after store");

        // Create a NEW store with a fresh (empty) ReDb — simulates lost database.
        // The .reg file should restore the missing entry.
        let db_path2 = temp_dir.path().join("db2");
        std::fs::create_dir_all(&db_path2)?;
        let db2 = create_test_db(&db_path2).await;
        let store2 = DelegateStore::new(delegate_dir.clone(), 10_000, db2)?;

        assert!(
            store2.key_to_code_part.contains_key(&key),
            "Index should be restored from .reg file"
        );

        let fetched = store2.fetch_delegate(&key, &vec![7, 8].into());
        assert!(
            fetched.is_some(),
            "Delegate should be fetchable after .reg restore"
        );

        Ok(())
    }

    /// remove_delegate removes .reg file alongside WASM and index entries
    #[tokio::test]
    async fn remove_delegate_cleans_reg_file() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-remove-reg-test");
        std::fs::create_dir_all(&delegate_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        let delegate = {
            let delegate = Delegate::from((&vec![50, 51, 52].into(), &vec![].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key = delegate.key().clone();
        store.store_delegate(delegate)?;

        let reg_path = delegate_dir.join(key.encode()).with_extension("reg");
        assert!(reg_path.exists());

        store.remove_delegate(&key)?;

        assert!(!reg_path.exists(), ".reg file should be removed");
        assert!(store.fetch_delegate(&key, &vec![].into()).is_none());

        Ok(())
    }

    /// Storing same delegate twice is idempotent
    #[tokio::test]
    async fn idempotent_store_preserves_reg() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-idempotent-test");
        std::fs::create_dir_all(&delegate_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        let delegate = {
            let delegate = Delegate::from((&vec![60, 61].into(), &vec![].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key = delegate.key().clone();

        store.store_delegate(delegate.clone())?;
        let reg_path = delegate_dir.join(key.encode()).with_extension("reg");
        let mtime1 = std::fs::metadata(&reg_path)?.modified()?;

        store.store_delegate(delegate)?;
        let mtime2 = std::fs::metadata(&reg_path)?.modified()?;

        assert_eq!(mtime1, mtime2, ".reg file should not be rewritten");

        Ok(())
    }

    /// remove_delegate actually deletes the .wasm file (keyed by code_hash, not delegate_key)
    #[tokio::test]
    async fn remove_delegate_cleans_wasm_file() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-remove-wasm-test");
        std::fs::create_dir_all(&delegate_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        let delegate = {
            let delegate = Delegate::from((&vec![70, 71, 72].into(), &vec![].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key = delegate.key().clone();
        let code_hash = *delegate.code_hash();
        store.store_delegate(delegate)?;

        let wasm_path = delegate_dir.join(code_hash.encode()).with_extension("wasm");
        assert!(wasm_path.exists(), ".wasm file should exist after store");

        store.remove_delegate(&key)?;
        assert!(!wasm_path.exists(), ".wasm file should be removed");

        Ok(())
    }

    /// Removing one delegate with shared WASM does not break the other
    #[tokio::test]
    async fn remove_shared_wasm_preserves_other_delegate() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-shared-wasm-test");
        std::fs::create_dir_all(&delegate_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        let wasm_code: Vec<u8> = vec![80, 81, 82];

        let delegate1 = {
            let delegate = Delegate::from((&wasm_code.clone().into(), &vec![].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key1 = delegate1.key().clone();

        let delegate2 = {
            let delegate = Delegate::from((&wasm_code.clone().into(), &vec![9, 10].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key2 = delegate2.key().clone();
        let code_hash = *delegate2.code_hash();

        store.store_delegate(delegate1)?;
        store.store_delegate(delegate2)?;

        let wasm_path = delegate_dir.join(code_hash.encode()).with_extension("wasm");
        assert!(wasm_path.exists());

        // Remove delegate1 — shared .wasm should NOT be deleted
        store.remove_delegate(&key1)?;

        assert!(
            wasm_path.exists(),
            "shared .wasm should survive when another delegate uses it"
        );
        assert!(
            store.fetch_delegate(&key2, &vec![9, 10].into()).is_some(),
            "other delegate should still be fetchable"
        );

        // Now remove delegate2 — .wasm should be deleted (no more users)
        store.remove_delegate(&key2)?;
        assert!(
            !wasm_path.exists(),
            ".wasm should be removed when last delegate is removed"
        );

        Ok(())
    }

    /// Corrupt .reg files are skipped during startup without affecting valid ones
    #[tokio::test]
    async fn startup_skips_corrupt_reg_files() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-corrupt-reg-test");
        std::fs::create_dir_all(&delegate_dir)?;

        // Store a valid delegate first
        let db_path = temp_dir.path().join("db1");
        std::fs::create_dir_all(&db_path)?;
        let db = create_test_db(&db_path).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        let delegate = {
            let delegate = Delegate::from((&vec![90, 91].into(), &vec![].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key = delegate.key().clone();
        store.store_delegate(delegate)?;
        drop(store);

        // Write a corrupt .reg file alongside the valid one
        let corrupt_path = delegate_dir.join("CorruptFileNoRealKey.reg");
        std::fs::write(&corrupt_path, b"garbage data")?;

        // Create a new store with fresh ReDb — should recover valid entry and skip corrupt
        let db_path2 = temp_dir.path().join("db2");
        std::fs::create_dir_all(&db_path2)?;
        let db2 = create_test_db(&db_path2).await;
        let store2 = DelegateStore::new(delegate_dir.clone(), 10_000, db2)?;

        assert!(
            store2.key_to_code_part.contains_key(&key),
            "valid delegate should be restored"
        );
        assert!(
            store2.fetch_delegate(&key, &vec![].into()).is_some(),
            "valid delegate should be fetchable"
        );

        Ok(())
    }

    /// parse_reg_file handles valid and invalid data correctly
    #[test]
    fn parse_reg_file_validation() -> Result<(), Box<dyn std::error::Error>> {
        // Valid: version 1, 32-byte hash, 0-length params
        let mut valid = vec![1u8];
        valid.extend_from_slice(&[0u8; 32]);
        valid.extend_from_slice(&0u32.to_le_bytes());
        assert!(parse_reg_file(&valid).is_some());

        // Valid with params
        let mut valid_with_params = vec![1u8];
        valid_with_params.extend_from_slice(&[1u8; 32]);
        valid_with_params.extend_from_slice(&3u32.to_le_bytes());
        valid_with_params.extend_from_slice(&[10, 20, 30]);
        let (_, params) = parse_reg_file(&valid_with_params).unwrap();
        assert_eq!(params.as_ref(), &[10, 20, 30]);

        // Too short
        assert!(parse_reg_file(&[1u8; 10]).is_none());

        // Wrong version
        let mut wrong_version = vec![99u8];
        wrong_version.extend_from_slice(&[0u8; 32]);
        wrong_version.extend_from_slice(&0u32.to_le_bytes());
        assert!(parse_reg_file(&wrong_version).is_none());

        // Truncated params
        let mut truncated = vec![1u8];
        truncated.extend_from_slice(&[0u8; 32]);
        truncated.extend_from_slice(&10u32.to_le_bytes());
        truncated.extend_from_slice(&[1, 2, 3]);
        assert!(parse_reg_file(&truncated).is_none());

        Ok(())
    }
}
