use std::path::Path;
use std::sync::Arc;

use freenet_stdlib::prelude::*;
use redb::{Database, DatabaseError, ReadableDatabase, ReadableTable, TableDefinition};

use crate::wasm_runtime::StateStorage;

const CONTRACT_PARAMS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("contract_params");
const STATE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("state");

/// Table for persisting hosting metadata across restarts.
/// Key: ContractKey bytes
/// Value: HostingMetadata serialized (last_access_ms, access_type, size_bytes)
const HOSTING_METADATA_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("hosting_metadata");

/// Index table mapping ContractInstanceId to CodeHash.
/// This replaces the legacy KEY_DATA file in the contracts directory.
/// Key: ContractInstanceId (32 bytes)
/// Value: CodeHash (32 bytes)
pub(crate) const CONTRACT_INDEX_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("contract_index");

/// Index table mapping DelegateKey to CodeHash.
/// This replaces the legacy KEY_DATA file in the delegates directory.
/// Key: DelegateKey (32 bytes key + 32 bytes code_hash = 64 bytes)
/// Value: CodeHash (32 bytes)
pub(crate) const DELEGATE_INDEX_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("delegate_index");

/// Per-contract record of detected CRDT-invariant violations (e.g. a
/// non-idempotent `update_state`). One row per offending contract; presence
/// alone is the gate signal. See `ring::broken_invariants` for the in-memory
/// tracker that hydrates from this table at startup.
///
/// Key: ContractInstanceId (32 bytes)
/// Value: single byte encoding [`BrokenInvariant`] (currently 0 = NonIdempotent)
pub(crate) const BROKEN_INVARIANTS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("broken_invariants");

/// Index table mapping DelegateKey to secret key hashes.
/// This replaces the legacy KEY_DATA file in the secrets directory.
/// Key: DelegateKey (64 bytes)
/// Value: Concatenated secret key hashes (N * 32 bytes)
pub(crate) const SECRETS_INDEX_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("secrets_index");

/// Index table for the per-user secret dimension (P1 of #4381). SEPARATE
/// from [`SECRETS_INDEX_TABLE`] so the existing single-user index keeps its
/// exact schema and a pre-#4381 database opens unchanged (this table is
/// simply absent → created empty on first open).
///
/// Key: DelegateKey (64 bytes) || UserId (32 bytes) = 96 bytes
/// Value: Concatenated secret key hashes (N * 32 bytes)
pub(crate) const USER_SECRETS_INDEX_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("user_secrets_index");

/// Metadata about a hosted contract, persisted to survive restarts.
#[derive(Debug, Clone, Copy)]
pub struct HostingMetadata {
    /// Milliseconds since UNIX epoch when contract was last accessed
    pub last_access_ms: u64,
    /// How the contract was accessed (0=Get, 1=Put, 2=Subscribe)
    pub access_type: u8,
    /// Size of the contract state in bytes
    pub size_bytes: u64,
    /// Code hash of the contract (needed to reconstruct ContractKey)
    pub code_hash: [u8; 32],
    /// Whether this contract was accessed by a local client (HTTP/WebSocket).
    pub local_client_access: bool,
}

impl HostingMetadata {
    pub fn new(
        last_access_ms: u64,
        access_type: u8,
        size_bytes: u64,
        code_hash: [u8; 32],
        local_client_access: bool,
    ) -> Self {
        Self {
            last_access_ms,
            access_type,
            size_bytes,
            code_hash,
            local_client_access,
        }
    }

    /// Serialize to bytes: [last_access_ms: 8][access_type: 1][size_bytes: 8][code_hash: 32][local_client_access: 1] = 50 bytes
    pub fn to_bytes(&self) -> [u8; 50] {
        let mut buf = [0u8; 50];
        buf[0..8].copy_from_slice(&self.last_access_ms.to_le_bytes());
        buf[8] = self.access_type;
        buf[9..17].copy_from_slice(&self.size_bytes.to_le_bytes());
        buf[17..49].copy_from_slice(&self.code_hash);
        buf[49] = u8::from(self.local_client_access);
        buf
    }

    /// Deserialize from bytes. Backward-compatible: 49-byte entries
    /// from before the local_client_access field default to false.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 49 {
            return None;
        }
        let last_access_ms = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
        let access_type = bytes[8];
        let size_bytes = u64::from_le_bytes(bytes[9..17].try_into().ok()?);
        let code_hash: [u8; 32] = bytes[17..49].try_into().ok()?;
        let local_client_access = bytes.get(49).copied().unwrap_or(0) != 0;
        Some(Self {
            last_access_ms,
            access_type,
            size_bytes,
            code_hash,
            local_client_access,
        })
    }
}

/// ReDb wraps a redb Database in Arc for thread-safe sharing.
/// redb supports MVCC (multiple concurrent readers, single writer) internally,
/// so multiple clones of ReDb can safely access the same database.
#[derive(Clone)]
pub struct ReDb(Arc<Database>);

impl ReDb {
    pub async fn new(data_dir: &Path) -> Result<Self, redb::Error> {
        let db_path = data_dir.join("db");
        tracing::info!(
            db_path = ?db_path,
            phase = "store_init",
            "Loading contract store"
        );

        match Database::create(&db_path) {
            Ok(db) => Self::initialize_database(db),
            Err(e) if Self::is_version_mismatch(&e) => {
                tracing::warn!(
                    db_path = ?db_path,
                    error = %e,
                    phase = "version_mismatch",
                    "Database format mismatch detected, automatically migrating"
                );

                // Attempt to back up the old database
                Self::backup_and_remove_database(&db_path)?;

                // Retry with fresh database
                tracing::info!(
                    db_path = ?db_path,
                    phase = "create_new_db",
                    "Creating new database"
                );
                let db = Database::create(&db_path)?;
                Self::initialize_database(db)
            }
            Err(e) => {
                tracing::error!(
                    db_path = ?db_path,
                    error = %e,
                    phase = "store_init_failed",
                    "Failed to load contract store"
                );
                Err(e.into())
            }
        }
    }

    fn initialize_database(db: Database) -> Result<Self, redb::Error> {
        let db = Self(Arc::new(db));
        let txn = db.0.begin_write()?;
        {
            txn.open_table(STATE_TABLE).map_err(|e| {
                tracing::error!(
                    error = %e,
                    table = "STATE_TABLE",
                    phase = "table_init_failed",
                    "Failed to open STATE_TABLE"
                );
                e
            })?;

            txn.open_table(CONTRACT_PARAMS_TABLE).map_err(|e| {
                tracing::error!(
                    error = %e,
                    table = "CONTRACT_PARAMS_TABLE",
                    phase = "table_init_failed",
                    "Failed to open CONTRACT_PARAMS_TABLE"
                );
                e
            })?;

            txn.open_table(HOSTING_METADATA_TABLE).map_err(|e| {
                tracing::error!(
                    error = %e,
                    table = "HOSTING_METADATA_TABLE",
                    phase = "table_init_failed",
                    "Failed to open HOSTING_METADATA_TABLE"
                );
                e
            })?;

            // Index tables for contract/delegate/secrets stores
            // These replace the legacy KEY_DATA files
            txn.open_table(CONTRACT_INDEX_TABLE).map_err(|e| {
                tracing::error!(
                    error = %e,
                    table = "CONTRACT_INDEX_TABLE",
                    phase = "table_init_failed",
                    "Failed to open CONTRACT_INDEX_TABLE"
                );
                e
            })?;

            txn.open_table(DELEGATE_INDEX_TABLE).map_err(|e| {
                tracing::error!(
                    error = %e,
                    table = "DELEGATE_INDEX_TABLE",
                    phase = "table_init_failed",
                    "Failed to open DELEGATE_INDEX_TABLE"
                );
                e
            })?;

            txn.open_table(SECRETS_INDEX_TABLE).map_err(|e| {
                tracing::error!(
                    error = %e,
                    table = "SECRETS_INDEX_TABLE",
                    phase = "table_init_failed",
                    "Failed to open SECRETS_INDEX_TABLE"
                );
                e
            })?;

            // Per-user secrets index (P1 of #4381). Created on first open of
            // upgraded databases too — redb creates missing tables inside the
            // same write txn that opens them, so old DBs gain an empty table
            // without disturbing the single-user SECRETS_INDEX_TABLE above.
            txn.open_table(USER_SECRETS_INDEX_TABLE).map_err(|e| {
                tracing::error!(
                    error = %e,
                    table = "USER_SECRETS_INDEX_TABLE",
                    phase = "table_init_failed",
                    "Failed to open USER_SECRETS_INDEX_TABLE"
                );
                e
            })?;

            // Created on first open of upgraded databases too — redb creates
            // missing tables inside the same write txn that opens them.
            txn.open_table(BROKEN_INVARIANTS_TABLE).map_err(|e| {
                tracing::error!(
                    error = %e,
                    table = "BROKEN_INVARIANTS_TABLE",
                    phase = "table_init_failed",
                    "Failed to open BROKEN_INVARIANTS_TABLE"
                );
                e
            })?;
        }
        txn.commit()?;
        Ok(db)
    }

    fn is_version_mismatch(error: &DatabaseError) -> bool {
        // Match on the specific UpgradeRequired error variant in redb 3.x
        // This is more robust than string matching on error messages
        matches!(error, DatabaseError::UpgradeRequired(..))
    }

    fn backup_and_remove_database(db_path: &Path) -> Result<(), redb::Error> {
        use std::io::ErrorKind;

        // Generate timestamped backup path
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let backup_path = db_path.with_extension(format!("db.backup.{}", timestamp));

        // Attempt to backup before removing
        match std::fs::rename(db_path, &backup_path) {
            Ok(_) => {
                tracing::info!(
                    backup_path = ?backup_path,
                    phase = "backup_complete",
                    "Old database backed up - you can safely delete this backup after verifying the new database works correctly"
                );
                Ok(())
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                // Database doesn't exist, nothing to backup
                tracing::debug!(
                    db_path = ?db_path,
                    "No existing database to backup"
                );
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    db_path = ?db_path,
                    error = %e,
                    phase = "backup_failed",
                    "Failed to backup old database, attempting to remove it directly"
                );

                // If backup fails, try to remove directly
                std::fs::remove_file(db_path).map_err(|remove_err| {
                    tracing::error!(
                        db_path = ?db_path,
                        error = %remove_err,
                        phase = "remove_failed",
                        "Failed to remove incompatible database"
                    );
                    redb::Error::Io(remove_err)
                })?;

                tracing::info!(
                    db_path = ?db_path,
                    phase = "db_removed",
                    "Removed incompatible database (backup failed)"
                );
                Ok(())
            }
        }
    }

    // ==================== Hosting Metadata Methods ====================

    /// Store hosting metadata for a contract.
    pub fn store_hosting_metadata(
        &self,
        key: &ContractKey,
        metadata: HostingMetadata,
    ) -> Result<(), redb::Error> {
        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(HOSTING_METADATA_TABLE)?;
            tbl.insert(key.as_bytes(), metadata.to_bytes().as_slice())?;
        }
        txn.commit().map_err(Into::into)
    }

    /// Get hosting metadata for a contract.
    pub fn get_hosting_metadata(
        &self,
        key: &ContractKey,
    ) -> Result<Option<HostingMetadata>, redb::Error> {
        let txn = self.0.begin_read()?;
        let tbl = txn.open_table(HOSTING_METADATA_TABLE)?;
        match tbl.get(key.as_bytes())? {
            Some(v) => Ok(HostingMetadata::from_bytes(v.value())),
            None => Ok(None),
        }
    }

    /// Remove hosting metadata for a contract.
    pub fn remove_hosting_metadata(&self, key: &ContractKey) -> Result<(), redb::Error> {
        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(HOSTING_METADATA_TABLE)?;
            tbl.remove(key.as_bytes())?;
        }
        txn.commit().map_err(Into::into)
    }

    /// Load all hosting metadata from the database.
    /// Returns a vector of (ContractKey bytes, HostingMetadata) pairs.
    /// The caller must reconstruct ContractKey from the bytes.
    pub fn load_all_hosting_metadata(
        &self,
    ) -> Result<Vec<(Vec<u8>, HostingMetadata)>, redb::Error> {
        let txn = self.0.begin_read()?;
        let tbl = txn.open_table(HOSTING_METADATA_TABLE)?;

        let mut result = Vec::new();
        for entry in tbl.iter()? {
            let (key, value) = entry?;
            if let Some(metadata) = HostingMetadata::from_bytes(value.value()) {
                result.push((key.value().to_vec(), metadata));
            }
        }
        Ok(result)
    }

    /// Get the size of a contract's state (for populating hosting cache).
    pub fn get_state_size(&self, key: &ContractKey) -> Result<Option<u64>, redb::Error> {
        let txn = self.0.begin_read()?;
        let tbl = txn.open_table(STATE_TABLE)?;
        match tbl.get(key.as_bytes())? {
            Some(v) => Ok(Some(v.value().len() as u64)),
            None => Ok(None),
        }
    }

    /// Store a contract's state synchronously.
    ///
    /// This is the same as `StateStorage::store` but without the async wrapper
    /// and **without hosting metadata updates**. States written through this path
    /// will not have `last_access_ms`, `access_type`, `state_size`, or `code_hash`
    /// metadata tracked, meaning they won't be part of the hosting cache on restart.
    ///
    /// Used by V2 delegate host functions that need synchronous writes during
    /// WASM `process()` execution. Hosting metadata integration is a follow-up.
    pub fn store_state_sync(
        &self,
        key: &ContractKey,
        state: WrappedState,
    ) -> Result<(), redb::Error> {
        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(STATE_TABLE)?;
            tbl.insert(key.as_bytes(), state.as_ref())?;
        }
        txn.commit().map_err(Into::into)
    }

    /// Atomically update a contract's state, failing if no prior state exists.
    ///
    /// Performs the existence check and write in a single write transaction to
    /// eliminate the TOCTOU window that would exist with separate read + write.
    /// Used by V2 delegate UPDATE host function.
    ///
    /// **Does not update hosting metadata** (same caveat as `store_state_sync`).
    pub fn update_state_sync(
        &self,
        key: &ContractKey,
        state: WrappedState,
    ) -> Result<bool, redb::Error> {
        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(STATE_TABLE)?;
            // Check existence within the same write transaction
            let exists = tbl.get(key.as_bytes())?.is_some();
            if !exists {
                return Ok(false);
            }
            tbl.insert(key.as_bytes(), state.as_ref())?;
        }
        txn.commit()?;
        Ok(true)
    }

    /// Read a contract's state synchronously.
    ///
    /// This is the same as `StateStorage::get` but without the async wrapper.
    /// Used by V2 delegate host functions that need synchronous access during
    /// WASM `process()` execution.
    pub fn get_state_sync(&self, key: &ContractKey) -> Result<Option<WrappedState>, redb::Error> {
        let txn = self.0.begin_read()?;
        let val = {
            let tbl = txn.open_table(STATE_TABLE)?;
            tbl.get(key.as_bytes())?
        };
        match val {
            Some(v) => Ok(Some(WrappedState::new(v.value().to_vec()))),
            None => Ok(None),
        }
    }

    /// Iterate all contract keys that have stored state.
    /// Returns the raw key bytes - caller must reconstruct ContractKey.
    pub fn iter_all_state_keys(&self) -> Result<Vec<Vec<u8>>, redb::Error> {
        let txn = self.0.begin_read()?;
        let tbl = txn.open_table(STATE_TABLE)?;

        let mut result = Vec::new();
        for entry in tbl.iter()? {
            let (key, _) = entry?;
            result.push(key.value().to_vec());
        }
        Ok(result)
    }

    // ==================== Contract Index Methods ====================
    // These replace the legacy KEY_DATA file in contracts directory

    /// Store a contract index entry: ContractInstanceId → CodeHash
    pub fn store_contract_index(
        &self,
        instance_id: &ContractInstanceId,
        code_hash: &CodeHash,
    ) -> Result<(), redb::Error> {
        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(CONTRACT_INDEX_TABLE)?;
            tbl.insert(instance_id.as_ref(), code_hash.as_ref())?;
        }
        txn.commit().map_err(Into::into)
    }

    /// Get the CodeHash for a ContractInstanceId
    pub fn get_contract_index(
        &self,
        instance_id: &ContractInstanceId,
    ) -> Result<Option<CodeHash>, redb::Error> {
        let txn = self.0.begin_read()?;
        let tbl = txn.open_table(CONTRACT_INDEX_TABLE)?;
        match tbl.get(instance_id.as_ref())? {
            Some(v) => {
                let bytes: [u8; 32] = v.value().try_into().map_err(|_| {
                    redb::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid CodeHash length",
                    ))
                })?;
                Ok(Some(CodeHash::from(&bytes)))
            }
            None => Ok(None),
        }
    }

    /// Remove a contract index entry
    pub fn remove_contract_index(
        &self,
        instance_id: &ContractInstanceId,
    ) -> Result<(), redb::Error> {
        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(CONTRACT_INDEX_TABLE)?;
            tbl.remove(instance_id.as_ref())?;
        }
        txn.commit().map_err(Into::into)
    }

    /// Load all contract index entries
    pub fn load_all_contract_index(
        &self,
    ) -> Result<Vec<(ContractInstanceId, CodeHash)>, redb::Error> {
        let txn = self.0.begin_read()?;
        let tbl = txn.open_table(CONTRACT_INDEX_TABLE)?;

        let mut result = Vec::new();
        for entry in tbl.iter()? {
            let (key, value) = entry?;
            let key_bytes: [u8; 32] = key.value().try_into().map_err(|_| {
                redb::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid ContractInstanceId length",
                ))
            })?;
            let value_bytes: [u8; 32] = value.value().try_into().map_err(|_| {
                redb::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid CodeHash length",
                ))
            })?;
            result.push((
                ContractInstanceId::new(key_bytes),
                CodeHash::from(&value_bytes),
            ));
        }
        Ok(result)
    }

    // ==================== Delegate Index Methods ====================
    // These replace the legacy KEY_DATA file in delegates directory

    /// Store a delegate index entry: DelegateKey → CodeHash
    /// DelegateKey is serialized as 64 bytes (32 byte key + 32 byte code_hash)
    pub fn store_delegate_index(
        &self,
        delegate_key: &DelegateKey,
        code_hash: &CodeHash,
    ) -> Result<(), redb::Error> {
        let mut key_bytes = [0u8; 64];
        key_bytes[..32].copy_from_slice(delegate_key.as_ref());
        key_bytes[32..].copy_from_slice(delegate_key.code_hash().as_ref());

        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(DELEGATE_INDEX_TABLE)?;
            tbl.insert(key_bytes.as_slice(), code_hash.as_ref())?;
        }
        txn.commit().map_err(Into::into)
    }

    /// Get the CodeHash for a DelegateKey
    pub fn get_delegate_index(
        &self,
        delegate_key: &DelegateKey,
    ) -> Result<Option<CodeHash>, redb::Error> {
        let mut key_bytes = [0u8; 64];
        key_bytes[..32].copy_from_slice(delegate_key.as_ref());
        key_bytes[32..].copy_from_slice(delegate_key.code_hash().as_ref());

        let txn = self.0.begin_read()?;
        let tbl = txn.open_table(DELEGATE_INDEX_TABLE)?;
        match tbl.get(key_bytes.as_slice())? {
            Some(v) => {
                let bytes: [u8; 32] = v.value().try_into().map_err(|_| {
                    redb::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid CodeHash length",
                    ))
                })?;
                Ok(Some(CodeHash::from(&bytes)))
            }
            None => Ok(None),
        }
    }

    /// Remove a delegate index entry
    pub fn remove_delegate_index(&self, delegate_key: &DelegateKey) -> Result<(), redb::Error> {
        let mut key_bytes = [0u8; 64];
        key_bytes[..32].copy_from_slice(delegate_key.as_ref());
        key_bytes[32..].copy_from_slice(delegate_key.code_hash().as_ref());

        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(DELEGATE_INDEX_TABLE)?;
            tbl.remove(key_bytes.as_slice())?;
        }
        txn.commit().map_err(Into::into)
    }

    /// Load all delegate index entries
    pub fn load_all_delegate_index(&self) -> Result<Vec<(DelegateKey, CodeHash)>, redb::Error> {
        let txn = self.0.begin_read()?;
        let tbl = txn.open_table(DELEGATE_INDEX_TABLE)?;

        let mut result = Vec::new();
        for entry in tbl.iter()? {
            let (key, value) = entry?;
            let key_bytes = key.value();
            if key_bytes.len() != 64 {
                continue; // Skip malformed entries
            }
            let delegate_key_bytes: [u8; 32] = key_bytes[..32].try_into().unwrap();
            let code_hash_bytes: [u8; 32] = key_bytes[32..].try_into().unwrap();
            let value_bytes: [u8; 32] = value.value().try_into().map_err(|_| {
                redb::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid CodeHash length",
                ))
            })?;

            let delegate_key =
                DelegateKey::new(delegate_key_bytes, CodeHash::from(&code_hash_bytes));
            result.push((delegate_key, CodeHash::from(&value_bytes)));
        }
        Ok(result)
    }

    // ==================== Secrets Index Methods ====================
    // These replace the legacy KEY_DATA file in secrets directory

    /// Store a secrets index entry: DelegateKey → concatenated secret key hashes
    pub fn store_secrets_index(
        &self,
        delegate_key: &DelegateKey,
        secret_keys: &[[u8; 32]],
    ) -> Result<(), redb::Error> {
        let mut key_bytes = [0u8; 64];
        key_bytes[..32].copy_from_slice(delegate_key.as_ref());
        key_bytes[32..].copy_from_slice(delegate_key.code_hash().as_ref());

        // Concatenate all secret keys
        let mut value_bytes = Vec::with_capacity(secret_keys.len() * 32);
        for sk in secret_keys {
            value_bytes.extend_from_slice(sk);
        }

        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(SECRETS_INDEX_TABLE)?;
            tbl.insert(key_bytes.as_slice(), value_bytes.as_slice())?;
        }
        txn.commit().map_err(Into::into)
    }

    /// Get the secret key hashes for a DelegateKey
    pub fn get_secrets_index(
        &self,
        delegate_key: &DelegateKey,
    ) -> Result<Option<Vec<[u8; 32]>>, redb::Error> {
        let mut key_bytes = [0u8; 64];
        key_bytes[..32].copy_from_slice(delegate_key.as_ref());
        key_bytes[32..].copy_from_slice(delegate_key.code_hash().as_ref());

        let txn = self.0.begin_read()?;
        let tbl = txn.open_table(SECRETS_INDEX_TABLE)?;
        match tbl.get(key_bytes.as_slice())? {
            Some(v) => {
                let value = v.value();
                if value.len() % 32 != 0 {
                    return Err(redb::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid secrets index value length",
                    )));
                }
                let mut result = Vec::with_capacity(value.len() / 32);
                for chunk in value.chunks(32) {
                    let arr: [u8; 32] = chunk.try_into().unwrap();
                    result.push(arr);
                }
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    /// Remove a secrets index entry
    pub fn remove_secrets_index(&self, delegate_key: &DelegateKey) -> Result<(), redb::Error> {
        let mut key_bytes = [0u8; 64];
        key_bytes[..32].copy_from_slice(delegate_key.as_ref());
        key_bytes[32..].copy_from_slice(delegate_key.code_hash().as_ref());

        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(SECRETS_INDEX_TABLE)?;
            tbl.remove(key_bytes.as_slice())?;
        }
        txn.commit().map_err(Into::into)
    }

    /// Load all secrets index entries
    #[allow(clippy::type_complexity)]
    pub fn load_all_secrets_index(&self) -> Result<Vec<(DelegateKey, Vec<[u8; 32]>)>, redb::Error> {
        let txn = self.0.begin_read()?;
        let tbl = txn.open_table(SECRETS_INDEX_TABLE)?;

        let mut result = Vec::new();
        for entry in tbl.iter()? {
            let (key, value) = entry?;
            let key_bytes = key.value();
            if key_bytes.len() != 64 {
                continue; // Skip malformed entries
            }
            let delegate_key_bytes: [u8; 32] = key_bytes[..32].try_into().unwrap();
            let code_hash_bytes: [u8; 32] = key_bytes[32..].try_into().unwrap();

            let value_bytes = value.value();
            if value_bytes.len() % 32 != 0 {
                continue; // Skip malformed entries
            }
            let mut secret_keys = Vec::with_capacity(value_bytes.len() / 32);
            for chunk in value_bytes.chunks(32) {
                let arr: [u8; 32] = chunk.try_into().unwrap();
                secret_keys.push(arr);
            }

            let delegate_key =
                DelegateKey::new(delegate_key_bytes, CodeHash::from(&code_hash_bytes));
            result.push((delegate_key, secret_keys));
        }
        Ok(result)
    }

    // ============== Per-User Secrets Index Methods (P1 of #4381) ==============
    // SEPARATE table from the single-user index above; the DelegateKey is
    // suffixed with the 32-byte UserId so each user's set is independent. The
    // single-user SECRETS_INDEX_TABLE is never touched by these methods.

    /// Build the 96-byte composite key `DelegateKey(64) || UserId(32)`.
    fn user_index_key(delegate_key: &DelegateKey, user_id: &[u8; 32]) -> [u8; 96] {
        let mut key_bytes = [0u8; 96];
        key_bytes[..32].copy_from_slice(delegate_key.as_ref());
        key_bytes[32..64].copy_from_slice(delegate_key.code_hash().as_ref());
        key_bytes[64..].copy_from_slice(user_id);
        key_bytes
    }

    /// Store a per-user secrets index entry:
    /// `(DelegateKey, UserId) → concatenated secret key hashes`.
    pub fn store_user_secrets_index(
        &self,
        delegate_key: &DelegateKey,
        user_id: &[u8; 32],
        secret_keys: &[[u8; 32]],
    ) -> Result<(), redb::Error> {
        let key_bytes = Self::user_index_key(delegate_key, user_id);

        let mut value_bytes = Vec::with_capacity(secret_keys.len() * 32);
        for sk in secret_keys {
            value_bytes.extend_from_slice(sk);
        }

        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(USER_SECRETS_INDEX_TABLE)?;
            tbl.insert(key_bytes.as_slice(), value_bytes.as_slice())?;
        }
        txn.commit().map_err(Into::into)
    }

    /// Get the secret key hashes for a `(DelegateKey, UserId)` pair.
    /// Test-only today; the runtime hydrates the whole table at startup via
    /// [`Self::load_all_user_secrets_index`] and keeps an in-memory mirror,
    /// so it never point-queries. Kept for parity with the single-user
    /// `get_secrets_index` and for tests asserting on the durable row.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn get_user_secrets_index(
        &self,
        delegate_key: &DelegateKey,
        user_id: &[u8; 32],
    ) -> Result<Option<Vec<[u8; 32]>>, redb::Error> {
        let key_bytes = Self::user_index_key(delegate_key, user_id);

        let txn = self.0.begin_read()?;
        let tbl = txn.open_table(USER_SECRETS_INDEX_TABLE)?;
        match tbl.get(key_bytes.as_slice())? {
            Some(v) => {
                let value = v.value();
                if value.len() % 32 != 0 {
                    return Err(redb::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid user secrets index value length",
                    )));
                }
                let mut result = Vec::with_capacity(value.len() / 32);
                for chunk in value.chunks(32) {
                    let arr: [u8; 32] = chunk.try_into().unwrap();
                    result.push(arr);
                }
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    /// Remove a per-user secrets index entry.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn remove_user_secrets_index(
        &self,
        delegate_key: &DelegateKey,
        user_id: &[u8; 32],
    ) -> Result<(), redb::Error> {
        let key_bytes = Self::user_index_key(delegate_key, user_id);

        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(USER_SECRETS_INDEX_TABLE)?;
            tbl.remove(key_bytes.as_slice())?;
        }
        txn.commit().map_err(Into::into)
    }

    /// Load all per-user secrets index entries as
    /// `((DelegateKey, UserId bytes), secret key hashes)`.
    #[allow(clippy::type_complexity)]
    pub fn load_all_user_secrets_index(
        &self,
    ) -> Result<Vec<((DelegateKey, [u8; 32]), Vec<[u8; 32]>)>, redb::Error> {
        let txn = self.0.begin_read()?;
        let tbl = txn.open_table(USER_SECRETS_INDEX_TABLE)?;

        let mut result = Vec::new();
        for entry in tbl.iter()? {
            let (key, value) = entry?;
            let key_bytes = key.value();
            if key_bytes.len() != 96 {
                continue; // Skip malformed entries
            }
            let delegate_key_bytes: [u8; 32] = key_bytes[..32].try_into().unwrap();
            let code_hash_bytes: [u8; 32] = key_bytes[32..64].try_into().unwrap();
            let user_id_bytes: [u8; 32] = key_bytes[64..].try_into().unwrap();

            let value_bytes = value.value();
            if value_bytes.len() % 32 != 0 {
                continue; // Skip malformed entries
            }
            let mut secret_keys = Vec::with_capacity(value_bytes.len() / 32);
            for chunk in value_bytes.chunks(32) {
                let arr: [u8; 32] = chunk.try_into().unwrap();
                secret_keys.push(arr);
            }

            let delegate_key =
                DelegateKey::new(delegate_key_bytes, CodeHash::from(&code_hash_bytes));
            result.push(((delegate_key, user_id_bytes), secret_keys));
        }
        Ok(result)
    }

    // ==================== Broken Invariants Methods ====================
    // Per-contract record of detected CRDT-invariant violations. See
    // `ring::broken_invariants` for the in-memory tracker.

    /// Persist a broken-invariant flag for the given contract instance.
    /// `kind_byte` is the single-byte encoding produced by
    /// `BrokenInvariant::to_byte`. Repeated calls overwrite — the tracker's
    /// in-memory layer suppresses redundant writes for already-flagged
    /// contracts, but we don't depend on that here.
    pub fn store_broken_invariant(
        &self,
        instance_id: &ContractInstanceId,
        kind_byte: u8,
    ) -> Result<(), redb::Error> {
        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(BROKEN_INVARIANTS_TABLE)?;
            tbl.insert(instance_id.as_ref(), &[kind_byte][..])?;
        }
        txn.commit().map_err(Into::into)
    }

    /// Remove a persisted broken-invariant flag. Paired with
    /// `BrokenInvariantsTracker::clear` — without on-disk removal, an
    /// operator's unflag would be undone on the next restart's
    /// `set_storage` rehydration.
    pub fn remove_broken_invariant(
        &self,
        instance_id: &ContractInstanceId,
    ) -> Result<(), redb::Error> {
        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(BROKEN_INVARIANTS_TABLE)?;
            tbl.remove(instance_id.as_ref())?;
        }
        txn.commit().map_err(Into::into)
    }

    /// Load all persisted broken-invariant flags. Malformed rows (wrong
    /// key length, wrong value length) are skipped with a warning rather
    /// than failing the entire load — a corrupted entry should not block
    /// startup, and the worst case is we lose a flag and re-detect it.
    pub fn load_all_broken_invariants(&self) -> Result<Vec<(ContractInstanceId, u8)>, redb::Error> {
        let txn = self.0.begin_read()?;
        let tbl = txn.open_table(BROKEN_INVARIANTS_TABLE)?;

        let mut result = Vec::new();
        for entry in tbl.iter()? {
            let (key, value) = entry?;
            let key_bytes: [u8; 32] = match key.value().try_into() {
                Ok(b) => b,
                Err(_) => {
                    tracing::warn!(
                        len = key.value().len(),
                        "Skipping malformed broken-invariants row (key length)"
                    );
                    continue;
                }
            };
            let v = value.value();
            if v.len() != 1 {
                tracing::warn!(
                    len = v.len(),
                    "Skipping malformed broken-invariants row (value length)"
                );
                continue;
            }
            result.push((ContractInstanceId::new(key_bytes), v[0]));
        }
        Ok(result)
    }
}

impl StateStorage for ReDb {
    type Error = redb::Error;

    async fn store(&self, key: ContractKey, state: WrappedState) -> Result<(), Self::Error> {
        let state_size = state.size() as u64;
        let txn = self.0.begin_write()?;

        {
            let mut tbl = txn.open_table(STATE_TABLE)?;
            tbl.insert(key.as_bytes(), state.as_ref())?;
        }

        // Also update hosting metadata to track this contract
        // This ensures the contract is reloaded into hosting cache on restart
        {
            let mut tbl = txn.open_table(HOSTING_METADATA_TABLE)?;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            // Default to PUT access type (1) since we're storing state
            // Store the code hash so we can reconstruct ContractKey on load
            let code_hash: [u8; 32] = **key.code_hash();
            // Preserve existing local_client_access flag on update
            let existing_local = tbl
                .get(key.as_bytes())
                .ok()
                .flatten()
                .and_then(|v| HostingMetadata::from_bytes(v.value()))
                .map(|m| m.local_client_access)
                .unwrap_or(false);
            let metadata = HostingMetadata::new(now_ms, 1, state_size, code_hash, existing_local);
            tbl.insert(key.as_bytes(), metadata.to_bytes().as_slice())?;
        }

        txn.commit().map_err(Into::into)
    }

    async fn get(&self, key: &ContractKey) -> Result<Option<WrappedState>, Self::Error> {
        let txn = self.0.begin_read()?;

        let val = {
            let tbl = txn.open_table(STATE_TABLE)?;
            tbl.get(key.as_bytes())?
        };

        match val {
            Some(v) => Ok(Some(WrappedState::new(v.value().to_vec()))),
            None => Ok(None),
        }
    }

    async fn store_params(
        &self,
        key: ContractKey,
        params: Parameters<'static>,
    ) -> Result<(), Self::Error> {
        let txn = self.0.begin_write()?;

        {
            let mut tbl = txn.open_table(CONTRACT_PARAMS_TABLE)?;
            tbl.insert(key.as_bytes(), params.as_ref())?;
        }
        txn.commit().map_err(Into::into)
    }

    async fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> Result<Option<Parameters<'static>>, Self::Error> {
        let txn = self.0.begin_read()?;

        let val = {
            let tbl = txn.open_table(CONTRACT_PARAMS_TABLE)?;
            tbl.get(key.as_bytes())?
        };

        match val {
            Some(v) => Ok(Some(Parameters::from(v.value().to_vec()))),
            None => Ok(None),
        }
    }

    async fn remove(&self, key: &ContractKey) -> Result<(), Self::Error> {
        // Delete from all three per-key tables in a single write transaction
        // so the removal is atomic. `redb`'s `Table::remove` does not error
        // when the key is absent, so this is naturally idempotent.
        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(STATE_TABLE)?;
            tbl.remove(key.as_bytes())?;
        }
        {
            let mut tbl = txn.open_table(CONTRACT_PARAMS_TABLE)?;
            tbl.remove(key.as_bytes())?;
        }
        {
            let mut tbl = txn.open_table(HOSTING_METADATA_TABLE)?;
            tbl.remove(key.as_bytes())?;
        }
        txn.commit().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    // Note: Direct unit testing of is_version_mismatch is difficult because
    // DatabaseError::UpgradeRequired is created internally by redb and cannot
    // be easily constructed in tests. The real validation happens via:
    // 1. The backup tests below (verify backup logic works)
    // 2. Integration tests with actual v2 databases (verify migration works)
    // 3. Manual testing with actual version mismatches

    #[tokio::test]
    async fn test_backup_nonexistent_database() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("nonexistent_db");

        // Should succeed even if database doesn't exist
        let result = ReDb::backup_and_remove_database(&db_path);
        assert!(
            result.is_ok(),
            "Should handle nonexistent database gracefully"
        );
    }

    #[tokio::test]
    async fn test_backup_creates_timestamped_backup() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");

        // Create a dummy database file (use "db" like the real code does)
        let mut file = std::fs::File::create(&db_path).unwrap();
        file.write_all(b"dummy database content").unwrap();
        drop(file);

        // Backup the database
        ReDb::backup_and_remove_database(&db_path).unwrap();

        // Original should be gone
        assert!(!db_path.exists(), "Original database should be removed");

        // Backup should exist with timestamp format like "db.backup.{timestamp}"
        let backups: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                name.starts_with("db.backup.") || name.starts_with("db.db.backup.")
            })
            .collect();

        assert!(
            !backups.is_empty(),
            "Should create at least one backup. Found files: {:?}",
            std::fs::read_dir(temp_dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name())
                .collect::<Vec<_>>()
        );

        // Verify backup has the same content
        let backup_path = backups[0].path();
        let backup_content = std::fs::read_to_string(&backup_path).unwrap();
        assert_eq!(
            backup_content, "dummy database content",
            "Backup should preserve original content"
        );
    }

    #[tokio::test]
    async fn test_migration_with_fresh_database() {
        let temp_dir = TempDir::new().unwrap();

        // This should succeed and create a new database
        let result = ReDb::new(temp_dir.path()).await;
        assert!(result.is_ok(), "Should successfully create fresh database");

        // Verify database file was created
        let db_path = temp_dir.path().join("db");
        assert!(db_path.exists(), "Database file should exist");
    }

    /// Round-trip test: to_bytes -> from_bytes preserves all fields.
    #[test]
    fn test_hosting_metadata_roundtrip() {
        let metadata = HostingMetadata::new(1234567890, 1, 4096, [0xAB; 32], true);
        let bytes = metadata.to_bytes();
        let restored = HostingMetadata::from_bytes(&bytes).unwrap();
        assert_eq!(restored.last_access_ms, 1234567890);
        assert_eq!(restored.access_type, 1);
        assert_eq!(restored.size_bytes, 4096);
        assert_eq!(restored.code_hash, [0xAB; 32]);
        assert!(restored.local_client_access);

        // Also test with local_client_access = false
        let metadata2 = HostingMetadata::new(9999, 0, 100, [0x01; 32], false);
        let restored2 = HostingMetadata::from_bytes(&metadata2.to_bytes()).unwrap();
        assert!(!restored2.local_client_access);
    }

    /// Backward compatibility: 49-byte legacy entries (pre-local_client_access)
    /// should deserialize with local_client_access = false.
    #[test]
    fn test_hosting_metadata_legacy_49_byte_compat() {
        // Build a legacy 49-byte entry manually
        let mut legacy = [0u8; 49];
        legacy[0..8].copy_from_slice(&1000u64.to_le_bytes());
        legacy[8] = 0; // GET
        legacy[9..17].copy_from_slice(&512u64.to_le_bytes());
        legacy[17..49].copy_from_slice(&[0xCC; 32]);

        let restored = HostingMetadata::from_bytes(&legacy).unwrap();
        assert_eq!(restored.last_access_ms, 1000);
        assert_eq!(restored.access_type, 0);
        assert_eq!(restored.size_bytes, 512);
        assert_eq!(restored.code_hash, [0xCC; 32]);
        assert!(
            !restored.local_client_access,
            "Legacy 49-byte entries must default to local_client_access=false"
        );
    }

    /// Entries shorter than 49 bytes should fail to deserialize.
    #[test]
    fn test_hosting_metadata_too_short() {
        assert!(HostingMetadata::from_bytes(&[0u8; 48]).is_none());
        assert!(HostingMetadata::from_bytes(&[]).is_none());
    }

    fn make_test_key() -> ContractKey {
        let code = ContractCode::from(vec![1, 2, 3, 4]);
        let params = Parameters::from(vec![5, 6, 7, 8]);
        ContractKey::from_params_and_code(&params, &code)
    }

    /// `remove` deletes both the state and the params for a contract.
    #[tokio::test]
    async fn test_remove_deletes_state_and_params() {
        let temp_dir = TempDir::new().unwrap();
        let db = ReDb::new(temp_dir.path()).await.unwrap();

        let key = make_test_key();
        let state = WrappedState::new(vec![1, 2, 3]);
        let params = Parameters::from(vec![10, 20, 30]);

        // Store state + params and confirm they are present.
        db.store(key, state.clone()).await.unwrap();
        db.store_params(key, params.clone()).await.unwrap();
        assert_eq!(db.get(&key).await.unwrap(), Some(state));
        assert_eq!(db.get_params(&key).await.unwrap(), Some(params));

        // Remove and confirm both are gone.
        db.remove(&key).await.unwrap();
        assert_eq!(db.get(&key).await.unwrap(), None);
        assert_eq!(db.get_params(&key).await.unwrap(), None);
    }

    /// `remove` on a contract that was never stored is a no-op (idempotent).
    #[tokio::test]
    async fn test_remove_never_stored_is_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let db = ReDb::new(temp_dir.path()).await.unwrap();

        let key = make_test_key();
        db.remove(&key)
            .await
            .expect("removing a never-stored contract should be Ok");
    }

    // ==================== Broken Invariants Persistence ====================

    fn fake_instance_id(seed: u8) -> ContractInstanceId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        ContractInstanceId::new(bytes)
    }

    /// Full store → reopen → load round trip for the broken-invariants
    /// table. This is the load-bearing guarantee for the
    /// "node that detected the bug stays gated after restart" claim in
    /// PR #4279. A regression that swapped key/value order, dropped the
    /// commit, or wrote to the wrong table would ship green without this.
    #[tokio::test]
    async fn broken_invariants_persistence_round_trip() {
        let temp_dir = TempDir::new().unwrap();

        let id_a = fake_instance_id(0xA1);
        let id_b = fake_instance_id(0xB2);

        // Initial open + write.
        {
            let db = ReDb::new(temp_dir.path()).await.unwrap();
            db.store_broken_invariant(&id_a, 0).expect("store id_a");
            db.store_broken_invariant(&id_b, 0).expect("store id_b");
        }

        // Reopen. The exact instance must come back through
        // `load_all_broken_invariants` — this is what
        // `BrokenInvariantsTracker::set_storage` calls at executor wire-up
        // to hydrate the in-memory flag map.
        let db = ReDb::new(temp_dir.path()).await.unwrap();
        let mut loaded = db.load_all_broken_invariants().expect("load");
        loaded.sort_by_key(|(id, _)| id.as_bytes().to_vec());

        let mut expected = vec![(id_a, 0u8), (id_b, 0u8)];
        expected.sort_by_key(|(id, _)| id.as_bytes().to_vec());

        assert_eq!(
            loaded, expected,
            "broken-invariants table must survive close-and-reopen exactly"
        );
    }

    /// `remove_broken_invariant` deletes the on-disk row, so a clear()
    /// in `BrokenInvariantsTracker` followed by a process restart
    /// genuinely keeps the contract unflagged. Without this, `set_storage`
    /// would re-hydrate from the stale row and the unflag would be undone.
    #[tokio::test]
    async fn broken_invariants_remove_makes_load_empty() {
        let temp_dir = TempDir::new().unwrap();
        let id = fake_instance_id(0x42);

        {
            let db = ReDb::new(temp_dir.path()).await.unwrap();
            db.store_broken_invariant(&id, 0).unwrap();
            assert_eq!(db.load_all_broken_invariants().unwrap().len(), 1);
            db.remove_broken_invariant(&id).unwrap();
            assert!(db.load_all_broken_invariants().unwrap().is_empty());
        }

        // Round-trip across a close/reopen — the removal must persist.
        let db = ReDb::new(temp_dir.path()).await.unwrap();
        assert!(
            db.load_all_broken_invariants().unwrap().is_empty(),
            "removal must survive a close/reopen"
        );
    }

    /// `store_broken_invariant` is treated as upsert (single in-memory
    /// flag → single on-disk row). Repeated stores with the same key
    /// must collapse to one row, not produce duplicates.
    #[tokio::test]
    async fn broken_invariants_store_is_upsert_not_append() {
        let temp_dir = TempDir::new().unwrap();
        let db = ReDb::new(temp_dir.path()).await.unwrap();
        let id = fake_instance_id(0x77);

        db.store_broken_invariant(&id, 0).unwrap();
        db.store_broken_invariant(&id, 0).unwrap();
        db.store_broken_invariant(&id, 0).unwrap();

        let rows = db.load_all_broken_invariants().unwrap();
        assert_eq!(rows.len(), 1, "repeated stores must collapse to one row");
        assert_eq!(rows[0].0, id);
    }

    /// Malformed value-length rows must be skipped (not panic, not abort
    /// load). The current write path always writes 1 byte; this pins the
    /// forward-compat behavior so future format extensions can roll out
    /// without bricking startup for older nodes' on-disk state.
    #[tokio::test]
    async fn broken_invariants_load_skips_malformed_value() {
        use redb::Database;
        let temp_dir = TempDir::new().unwrap();
        let db = ReDb::new(temp_dir.path()).await.unwrap();
        // Write a deliberately-too-long value bypassing the helper.
        let db_path = temp_dir.path().join("db");
        // Drop the wrapper so the raw redb file lock is released before
        // we open it directly to inject the malformed row.
        drop(db);
        let raw = Database::open(&db_path).unwrap();
        {
            let txn = raw.begin_write().unwrap();
            {
                let mut tbl = txn.open_table(BROKEN_INVARIANTS_TABLE).unwrap();
                let id = fake_instance_id(0xCC);
                let bogus: [u8; 4] = [1, 2, 3, 4];
                tbl.insert(id.as_ref(), &bogus[..]).unwrap();
            }
            txn.commit().unwrap();
        }
        drop(raw);

        let db = ReDb::new(temp_dir.path()).await.unwrap();
        let rows = db.load_all_broken_invariants().unwrap();
        assert!(
            rows.is_empty(),
            "malformed row must be silently skipped; got: {:?}",
            rows
        );
    }
}
