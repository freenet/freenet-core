use dashmap::DashMap;
use freenet_stdlib::prelude::{
    APIVersion, CodeHash, Delegate, DelegateCode, DelegateContainer, DelegateKey,
    DelegateWasmAPIVersion, Parameters,
};
use moka::sync::Cache as MokaCache;
use std::{fs::File, fs::OpenOptions, io::Write, path::PathBuf, sync::Arc};

use crate::contract::storages::Storage;

use super::RuntimeResult;

/// Registration record version for .reg files
const REG_FILE_VERSION: u8 = 1;

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
        std::fs::create_dir_all(&delegates_dir).map_err(|err| {
            tracing::error!("error creating delegate dir: {err}");
            err
        })?;

        let key_to_code_part = Arc::new(DashMap::new());

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
            delegate_cache: MokaCache::builder()
                .max_capacity(max_size)
                .weigher(
                    |key: &CodeHash, value: &Arc<DelegateCode<'static>>| -> u32 {
                        // Saturate to u32::MAX on overflow as moka recommends.
                        // A delegate WASM module larger than 4 GiB would indicate
                        // a bug in upstream size validation — log it loudly.
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
                .build(),
            delegates_dir,
            key_to_code_part,
            db,
        })
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
        // Ensure .reg file exists (supplementary backup for crash recovery)
        write_reg_file_if_missing(&self.delegates_dir, key, code_hash, params)?;

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
        // down. This is pure computation (one BLAKE3 pass over the WASM,
        // negligible against the compile it precedes), and a refusal therefore
        // leaves no blob, no .reg record, no index row and no commit, so it is
        // idempotent under retry. See `verify_delegate_identity`.
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

        write_reg_file_if_missing(&self.delegates_dir, key, code_hash, &params)?;

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

    pub fn code_hash_from_key(&self, key: &DelegateKey) -> Option<CodeHash> {
        self.key_to_code_part.get(key).map(|r| *r.value())
    }
}

/// Write a .reg registration record file if it doesn't already exist.
fn write_reg_file_if_missing(
    delegates_dir: &std::path::Path,
    key: &DelegateKey,
    code_hash: &CodeHash,
    params: &Parameters<'_>,
) -> RuntimeResult<()> {
    let reg_path = delegates_dir.join(key.encode()).with_extension("reg");

    let params_bytes = params.as_ref();
    let mut reg = Vec::with_capacity(1 + 32 + 4 + params_bytes.len());
    reg.push(REG_FILE_VERSION);
    reg.extend_from_slice(code_hash.as_ref());
    reg.extend_from_slice(&(params_bytes.len() as u32).to_le_bytes());
    reg.extend_from_slice(params_bytes);

    // Atomic create: create_new(true) fails with AlreadyExists if file exists,
    // avoiding TOCTOU race between exists() check and File::create().
    let mut file = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&reg_path)
    {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    file.write_all(&reg)?;
    file.sync_all()?;

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
