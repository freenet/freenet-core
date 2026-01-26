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

/// Index table mapping DelegateKey to secret key hashes.
/// This replaces the legacy KEY_DATA file in the secrets directory.
/// Key: DelegateKey (64 bytes)
/// Value: Concatenated secret key hashes (N * 32 bytes)
pub(crate) const SECRETS_INDEX_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("secrets_index");

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
}

impl HostingMetadata {
    pub fn new(last_access_ms: u64, access_type: u8, size_bytes: u64, code_hash: [u8; 32]) -> Self {
        Self {
            last_access_ms,
            access_type,
            size_bytes,
            code_hash,
        }
    }

    /// Serialize to bytes: [last_access_ms: 8][access_type: 1][size_bytes: 8][code_hash: 32] = 49 bytes
    pub fn to_bytes(&self) -> [u8; 49] {
        let mut buf = [0u8; 49];
        buf[0..8].copy_from_slice(&self.last_access_ms.to_le_bytes());
        buf[8] = self.access_type;
        buf[9..17].copy_from_slice(&self.size_bytes.to_le_bytes());
        buf[17..49].copy_from_slice(&self.code_hash);
        buf
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 49 {
            return None;
        }
        let last_access_ms = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
        let access_type = bytes[8];
        let size_bytes = u64::from_le_bytes(bytes[9..17].try_into().ok()?);
        let code_hash: [u8; 32] = bytes[17..49].try_into().ok()?;
        Some(Self {
            last_access_ms,
            access_type,
            size_bytes,
            code_hash,
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

    /// Store multiple contract index entries in a single transaction.
    /// More efficient than calling store_contract_index repeatedly.
    pub fn store_contract_index_batch(
        &self,
        entries: &[(ContractInstanceId, CodeHash)],
    ) -> Result<(), redb::Error> {
        if entries.is_empty() {
            return Ok(());
        }
        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(CONTRACT_INDEX_TABLE)?;
            for (instance_id, code_hash) in entries {
                tbl.insert(instance_id.as_ref(), code_hash.as_ref())?;
            }
        }
        txn.commit().map_err(Into::into)
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

    /// Store multiple delegate index entries in a single transaction.
    /// More efficient than calling store_delegate_index repeatedly.
    pub fn store_delegate_index_batch(
        &self,
        entries: &[(DelegateKey, CodeHash)],
    ) -> Result<(), redb::Error> {
        if entries.is_empty() {
            return Ok(());
        }
        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(DELEGATE_INDEX_TABLE)?;
            for (delegate_key, code_hash) in entries {
                let mut key_bytes = [0u8; 64];
                key_bytes[..32].copy_from_slice(delegate_key.as_ref());
                key_bytes[32..].copy_from_slice(delegate_key.code_hash().as_ref());
                tbl.insert(key_bytes.as_slice(), code_hash.as_ref())?;
            }
        }
        txn.commit().map_err(Into::into)
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

    /// Store multiple secrets index entries in a single transaction.
    /// More efficient than calling store_secrets_index repeatedly.
    pub fn store_secrets_index_batch(
        &self,
        entries: &[(DelegateKey, Vec<[u8; 32]>)],
    ) -> Result<(), redb::Error> {
        if entries.is_empty() {
            return Ok(());
        }
        let txn = self.0.begin_write()?;
        {
            let mut tbl = txn.open_table(SECRETS_INDEX_TABLE)?;
            for (delegate_key, secret_keys) in entries {
                let mut key_bytes = [0u8; 64];
                key_bytes[..32].copy_from_slice(delegate_key.as_ref());
                key_bytes[32..].copy_from_slice(delegate_key.code_hash().as_ref());

                let mut value_bytes = Vec::with_capacity(secret_keys.len() * 32);
                for sk in secret_keys {
                    value_bytes.extend_from_slice(sk);
                }
                tbl.insert(key_bytes.as_slice(), value_bytes.as_slice())?;
            }
        }
        txn.commit().map_err(Into::into)
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
            let metadata = HostingMetadata::new(now_ms, 1, state_size, code_hash);
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
}
