use std::path::Path;
use std::sync::{Arc, Mutex};

use freenet_stdlib::prelude::*;
use redb::{
    Database, DatabaseError, ReadTransaction, ReadableDatabase, ReadableTable, StorageError,
    TableDefinition, TransactionError, WriteTransaction,
};

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

/// One-shot idempotence / anti-resurrection marker for delegate secret
/// copy-forward (`SecretsStore::migrate_secrets`, #4117). One row per
/// `(predecessor, successor)` delegate pair. Presence alone gates the copy:
/// once a `(predecessor, successor)` migration has run, it is NEVER re-run, so
/// a secret the user deleted from the successor after migration cannot be
/// resurrected by a later re-registration. The row value is a small AUDIT FACT
/// (schema version, the originating contract when known, and the copied/skipped
/// counts) — see `SecretsStore::migrate_secrets`. Created on first open of
/// upgraded databases too (redb materializes a missing table inside the same
/// write txn that opens it), so a pre-#4117 database gains an empty table
/// without disturbing any existing table.
///
/// Key: predecessor DelegateKey (64 bytes) || successor DelegateKey (64 bytes) = 128 bytes
/// Value: versioned marker blob (see `migration_marker` codec in the secrets store)
pub(crate) const MIGRATION_MARKER_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("delegate_secret_migration_marker");

/// Durable record of the web-app contract origins under which each delegate has
/// been registered (#4117 H1 same-origin gate). Written on EVERY successful
/// delegate registration (both `RegisterDelegate` and
/// `RegisterDelegateWithPredecessors`). Copy-forward consults it: a predecessor's
/// Local secrets are copied into a successor ONLY when the registering request's
/// origin is among the predecessor's recorded origins (or both are the Admin/None
/// class), so a malicious web-app cannot register a delegate that names an
/// unrelated victim delegate as a predecessor to exfiltrate its secrets
/// (predecessor keys are public-derivable). See `SecretsStore::delegate_origins`.
///
/// Key: DelegateKey (64 bytes)
/// Value: `[has_admin_none: 1][N × ContractInstanceId(32)]` — `has_admin_none`
///        records that the delegate was at least once registered with no contract
///        origin (loopback / CLI / admin).
pub(crate) const DELEGATE_ORIGINS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("delegate_registration_origins");

/// Durable, UNCAPPED set of the secret hashes each delegate holds in the reserved
/// `\0freenet-migrate/` coordination namespace (#4117 finding 4a). Recorded at
/// `store_secret` time whenever a Local secret's raw key is under that namespace
/// (covers both this node's `pred-done:` markers and the app-side `pred-wip:`
/// markers, since both are written via the registering `set_secret`/`store_secret`
/// path). Copy-forward excludes these hashes from BOTH the value copy and the
/// enumeration copy. It exists because the advisory `.keys` enumeration registry
/// is CAPPED (`MAX_REGISTERED_KEYS_PER_SCOPE`) and may be unreadable — at/above
/// the cap or with a missing registry, a marker's raw key would be invisible to a
/// registry-based check and could chain-copy as user data, poisoning `had_data`
/// and falsely gating a later migration. This table is registry-independent.
///
/// Key: DelegateKey (64 bytes)
/// Value: concatenated 32-byte secret hashes (N × 32 bytes)
pub(crate) const RESERVED_MARKER_HASHES_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("delegate_reserved_marker_hashes");

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

/// Does this redb [`StorageError`] indicate the database is *poisoned* by an
/// underlying I/O failure — i.e. unusable until closed and reopened — rather than a
/// benign, app-level condition (a missing key returns `Ok(None)`; a missing/mismatched
/// table is a `TableError`, never one of these)? See issue #4604.
///
/// redb sets an in-memory poison flag after any I/O error: the triggering op returns
/// [`StorageError::Io`], and EVERY subsequent transaction then returns
/// [`StorageError::PreviousIo`] ("Previous I/O error occurred. Please close and
/// re-open the database.") until the `Database` is dropped and re-created.
/// [`StorageError::LockPoisoned`] (a redb-internal mutex poisoned by a panic) and
/// [`StorageError::DatabaseClosed`] are likewise unrecoverable for the live handle.
///
/// Matched against the typed variants (not the message string) so it cannot drift
/// with a redb wording change. `StorageError` is `#[non_exhaustive]`; `matches!`
/// keeps every OTHER (benign / app-level) error off the restart path — exactly the
/// precise-detection requirement of #4604. Notably `Corrupted`, `ValueTooLarge`,
/// and the table/type errors are NOT treated as poison.
fn storage_error_is_poison(e: &StorageError) -> bool {
    matches!(
        e,
        StorageError::PreviousIo
            | StorageError::Io(_)
            | StorageError::LockPoisoned(_)
            | StorageError::DatabaseClosed
    )
}

/// True if a transaction-begin error (`begin_read` / `begin_write`) signals a
/// poisoned database. Once poisoned, EVERY `begin_write` returns
/// `TransactionError::Storage(StorageError::PreviousIo)`, so this is the universal
/// post-poison choke point the #4604 fix keys off. (`ReadTransactionStillInUse` and
/// any future non-storage variant are benign usage errors, not a poison.)
fn transaction_error_is_poison(e: &TransactionError) -> bool {
    matches!(e, TransactionError::Storage(s) if storage_error_is_poison(s))
}

/// True if the umbrella [`redb::Error`] signals a poisoned database. Used to catch
/// poison that surfaces on the READ path AFTER `begin_read` has already succeeded
/// (redb's `begin_read` does not check the poison flag, so a poisoned read fails
/// later at `open_table` / `get` / iteration as a `StorageError` flattened into this
/// umbrella type).
///
/// IMPORTANT — this deliberately does NOT match `redb::Error::Io`, unlike
/// [`storage_error_is_poison`]. Several read methods in this file SYNTHESIZE
/// `redb::Error::Io(ErrorKind::InvalidData)` for a benign malformed-data row (e.g. a
/// wrong-length `CodeHash`). Treating that as poison would exit-and-restart the node
/// on a single bad row → a crash loop. A genuine backend I/O poison is safe to skip
/// here regardless: redb latches its poison flag on the first backend `Io`, so the
/// very next backend read returns `PreviousIo` (caught here) and the next
/// `begin_write` returns `PreviousIo` (caught by [`storage_error_is_poison`]). So
/// `PreviousIo` (plus the unambiguous `LockPoisoned` / `DatabaseClosed`) is the
/// precise, false-positive-free read-path signal.
fn redb_error_is_poison(e: &redb::Error) -> bool {
    matches!(
        e,
        redb::Error::PreviousIo | redb::Error::LockPoisoned(_) | redb::Error::DatabaseClosed
    )
}

/// Test-only observable proof that the storage layer routed a detected poison to
/// the recovery (process-exit) path. The real handler ([`abort_process_on_redb_poison`])
/// exits the process, which a unit test cannot observe; this counter lets the test
/// assert the wrapper recognised poison (and would have exited in production) without
/// killing the test process.
#[cfg(test)]
static POISON_RECOVERY_TRIGGERED: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// ReDb wraps a redb Database in Arc for thread-safe sharing.
/// redb supports MVCC (multiple concurrent readers, single writer) internally,
/// so multiple clones of ReDb can safely access the same database.
#[derive(Clone)]
pub struct ReDb {
    db: Arc<Database>,
    /// Cross-`ContractStore` mutual-exclusion lock for the shared-WASM
    /// store/remove race (issue #4216).
    ///
    /// Each runtime-pool executor owns a *separate* `ContractStore` but they
    /// all share one `ReDb` handle (cloned into each store). redb's MVCC gives
    /// no cross-transaction locking, so without this lock a
    /// `store_contract(X2)` on one executor and a `remove_contract(X1)` on
    /// another — where X1 and X2 share a code hash — can interleave: the
    /// remover's `load_all_contract_index()` scan runs after the storer wrote
    /// the `.wasm` blob but before it committed the index entry, sees no
    /// remaining reference, and deletes the blob the storer just wrote. The
    /// storer then commits an index entry pointing at a deleted blob.
    ///
    /// This lock serializes those two critical sections. It travels with the
    /// database handle (cloned with every `ReDb::clone`), so all
    /// `ContractStore`s built over the same database share it automatically
    /// with no constructor plumbing, while stores over *different* databases
    /// (per-test isolation) get independent locks.
    contract_blob_lock: Arc<Mutex<()>>,
}

impl ReDb {
    /// Clone the shared cross-`ContractStore` blob lock (issue #4216). The
    /// `ContractStore` store/remove paths lock this clone for the duration of
    /// their blob-vs-index critical section; see the field docs above.
    pub fn contract_blob_lock(&self) -> Arc<Mutex<()>> {
        self.contract_blob_lock.clone()
    }
}

impl ReDb {
    /// Begin a write transaction, routing a *poisoned*-database error to the #4604
    /// recovery path (process exit for a supervised restart with a fresh handle) so
    /// the node does not fail every contract op forever while looking "running". A
    /// benign error is returned unchanged.
    ///
    /// This is the RELIABLE post-poison choke point: redb latches an in-memory poison
    /// flag (`io_failed`) on ANY backend read OR write error, and `begin_write` checks
    /// it on every call (returning `PreviousIo`). Because the node writes hosting
    /// metadata on essentially every contract access, a poisoned database is detected
    /// here within one write — whatever the original error was a read or a write.
    fn begin_write(&self) -> Result<WriteTransaction, TransactionError> {
        self.db.begin_write().map_err(Self::route_txn_error)
    }

    /// Begin a read transaction with the same poison-recovery routing as
    /// [`ReDb::begin_write`]. Note redb's `begin_read` does NOT itself check the
    /// poison flag (it serves the last committed snapshot from cache), so it only
    /// surfaces poison when the read transaction registration itself does I/O; a
    /// poisoned read that reaches the backend fails later inside the transaction.
    /// Either way the next `begin_write` (above) catches the poison promptly.
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError> {
        self.db.begin_read().map_err(Self::route_txn_error)
    }

    /// If `e` indicates a poisoned database, trigger the #4604 recovery path
    /// ([`abort_process_on_redb_poison`], a no-op outside the real node binary).
    /// Always returns the error untouched so callers still propagate it.
    fn route_txn_error(e: TransactionError) -> TransactionError {
        if transaction_error_is_poison(&e) {
            #[cfg(test)]
            POISON_RECOVERY_TRIGGERED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            crate::node::abort_process_on_redb_poison(&e.to_string());
        }
        e
    }

    /// Umbrella-error counterpart of [`ReDb::route_txn_error`], for poison that
    /// surfaces on the read path AFTER `begin_read` succeeded (at `open_table` /
    /// `get` / iteration). Returns the error untouched.
    fn route_redb_error(e: redb::Error) -> redb::Error {
        if redb_error_is_poison(&e) {
            #[cfg(test)]
            POISON_RECOVERY_TRIGGERED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            crate::node::abort_process_on_redb_poison(&e.to_string());
        }
        e
    }

    /// Run a read-transaction body, routing ANY poison error to the #4604 recovery
    /// path — not just a poison at `begin_read`, but one that surfaces later at
    /// `open_table` / `get` / iteration (redb's `begin_read` does not check the
    /// poison flag, so a poisoned read can fail mid-transaction). This gives
    /// read-only workloads the same prompt exit-for-restart that `begin_write`
    /// already gives write workloads, instead of failing every read until the next
    /// write happens to hit `begin_write`.
    fn read_guarded<T>(
        &self,
        f: impl FnOnce(&ReadTransaction) -> Result<T, redb::Error>,
    ) -> Result<T, redb::Error> {
        // begin_read already routes a poison at transaction start.
        let txn = self.begin_read()?;
        f(&txn).map_err(Self::route_redb_error)
    }

    /// Commit a write transaction, routing a poison error to the #4604 recovery
    /// path. The FIRST backend I/O failure usually surfaces HERE (redb reports it as
    /// `CommitError::Storage(StorageError::Io)`), on the very op that poisons the
    /// handle — so catching it at commit triggers the restart immediately instead of
    /// waiting for the next `begin_write` to trip `PreviousIo`. Unlike the read path,
    /// commit/begin errors come straight from redb and are never the synthetic
    /// `Io(InvalidData)` of a malformed row, so it is safe to match `Io` here via
    /// [`storage_error_is_poison`].
    fn commit_guarded(txn: WriteTransaction) -> Result<(), redb::Error> {
        match txn.commit() {
            Ok(()) => Ok(()),
            Err(e) => {
                if let redb::CommitError::Storage(s) = &e {
                    if storage_error_is_poison(s) {
                        #[cfg(test)]
                        POISON_RECOVERY_TRIGGERED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        crate::node::abort_process_on_redb_poison(&e.to_string());
                    }
                }
                Err(e.into())
            }
        }
    }

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
        let db = Self {
            db: Arc::new(db),
            contract_blob_lock: Arc::new(Mutex::new(())),
        };
        let txn = db.db.begin_write()?;
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

            // Delegate secret copy-forward marker (#4117). Created on first open
            // of upgraded databases too (same missing-table-in-write-txn
            // materialization as the tables above), so a pre-#4117 database
            // gains an empty table without disturbing any existing one.
            txn.open_table(MIGRATION_MARKER_TABLE).map_err(|e| {
                tracing::error!(
                    error = %e,
                    table = "MIGRATION_MARKER_TABLE",
                    phase = "table_init_failed",
                    "Failed to open MIGRATION_MARKER_TABLE"
                );
                e
            })?;

            // Delegate registration origins (#4117 H1) + reserved-marker hashes
            // (#4117 4a). Both created on first open of upgraded databases too.
            txn.open_table(DELEGATE_ORIGINS_TABLE).map_err(|e| {
                tracing::error!(
                    error = %e,
                    table = "DELEGATE_ORIGINS_TABLE",
                    phase = "table_init_failed",
                    "Failed to open DELEGATE_ORIGINS_TABLE"
                );
                e
            })?;

            txn.open_table(RESERVED_MARKER_HASHES_TABLE).map_err(|e| {
                tracing::error!(
                    error = %e,
                    table = "RESERVED_MARKER_HASHES_TABLE",
                    phase = "table_init_failed",
                    "Failed to open RESERVED_MARKER_HASHES_TABLE"
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
        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(HOSTING_METADATA_TABLE)?;
            tbl.insert(key.as_bytes(), metadata.to_bytes().as_slice())?;
        }
        Self::commit_guarded(txn)
    }

    /// Get hosting metadata for a contract.
    pub fn get_hosting_metadata(
        &self,
        key: &ContractKey,
    ) -> Result<Option<HostingMetadata>, redb::Error> {
        self.read_guarded(|txn| {
            let tbl = txn.open_table(HOSTING_METADATA_TABLE)?;
            Ok(match tbl.get(key.as_bytes())? {
                Some(v) => HostingMetadata::from_bytes(v.value()),
                None => None,
            })
        })
    }

    /// Remove hosting metadata for a contract.
    pub fn remove_hosting_metadata(&self, key: &ContractKey) -> Result<(), redb::Error> {
        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(HOSTING_METADATA_TABLE)?;
            tbl.remove(key.as_bytes())?;
        }
        Self::commit_guarded(txn)
    }

    /// Load all hosting metadata from the database.
    /// Returns a vector of (ContractKey bytes, HostingMetadata) pairs.
    /// The caller must reconstruct ContractKey from the bytes.
    pub fn load_all_hosting_metadata(
        &self,
    ) -> Result<Vec<(Vec<u8>, HostingMetadata)>, redb::Error> {
        self.read_guarded(|txn| {
            let tbl = txn.open_table(HOSTING_METADATA_TABLE)?;

            let mut result = Vec::new();
            for entry in tbl.iter()? {
                let (key, value) = entry?;
                if let Some(metadata) = HostingMetadata::from_bytes(value.value()) {
                    result.push((key.value().to_vec(), metadata));
                }
            }
            Ok(result)
        })
    }

    /// Get the size of a contract's state (for populating hosting cache).
    pub fn get_state_size(&self, key: &ContractKey) -> Result<Option<u64>, redb::Error> {
        self.read_guarded(|txn| {
            let tbl = txn.open_table(STATE_TABLE)?;
            Ok(tbl.get(key.as_bytes())?.map(|v| v.value().len() as u64))
        })
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
    ///
    /// CHANGE-DETECTOR INVARIANT (future writers, read before using this): any
    /// contract-state write that BYPASSES `StateStore` (as this raw sync write
    /// does) MUST invalidate `StateStore`'s change-detector via
    /// `StateCacheInvalidator` (and the moka state-bytes cache), or the
    /// summarize/delta fast path can serve a STALE summary/delta against the
    /// new state → peer state divergence (#4621). The V2 delegate callers
    /// (`put_contract_state_sync` / `update_contract_state_sync`) do this via
    /// the runtime's `state_write_callback`. A new caller of this method (e.g.
    /// the #4592 live-import work) must wire the same invalidation.
    pub fn store_state_sync(
        &self,
        key: &ContractKey,
        state: WrappedState,
    ) -> Result<(), redb::Error> {
        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(STATE_TABLE)?;
            tbl.insert(key.as_bytes(), state.as_ref())?;
        }
        Self::commit_guarded(txn)
    }

    /// Atomically update a contract's state, failing if no prior state exists.
    ///
    /// Performs the existence check and write in a single write transaction to
    /// eliminate the TOCTOU window that would exist with separate read + write.
    /// Used by V2 delegate UPDATE host function.
    ///
    /// **Does not update hosting metadata** (same caveat as `store_state_sync`).
    ///
    /// CHANGE-DETECTOR INVARIANT: like `store_state_sync`, this bypasses
    /// `StateStore`, so any caller MUST invalidate the `StateStore`
    /// change-detector via `StateCacheInvalidator` or summarize/delta can serve
    /// a stale result → peer state divergence (#4621). See `store_state_sync`.
    pub fn update_state_sync(
        &self,
        key: &ContractKey,
        state: WrappedState,
    ) -> Result<bool, redb::Error> {
        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(STATE_TABLE)?;
            // Check existence within the same write transaction
            let exists = tbl.get(key.as_bytes())?.is_some();
            if !exists {
                return Ok(false);
            }
            tbl.insert(key.as_bytes(), state.as_ref())?;
        }
        Self::commit_guarded(txn)?;
        Ok(true)
    }

    /// Read a contract's state synchronously.
    ///
    /// This is the same as `StateStorage::get` but without the async wrapper.
    /// Used by V2 delegate host functions that need synchronous access during
    /// WASM `process()` execution.
    pub fn get_state_sync(&self, key: &ContractKey) -> Result<Option<WrappedState>, redb::Error> {
        self.read_guarded(|txn| {
            let tbl = txn.open_table(STATE_TABLE)?;
            Ok(tbl
                .get(key.as_bytes())?
                .map(|v| WrappedState::new(v.value().to_vec())))
        })
    }

    /// Iterate all contract keys that have stored state.
    /// Returns the raw key bytes - caller must reconstruct ContractKey.
    pub fn iter_all_state_keys(&self) -> Result<Vec<Vec<u8>>, redb::Error> {
        self.read_guarded(|txn| {
            let tbl = txn.open_table(STATE_TABLE)?;

            let mut result = Vec::new();
            for entry in tbl.iter()? {
                let (key, _) = entry?;
                result.push(key.value().to_vec());
            }
            Ok(result)
        })
    }

    // ==================== Contract Index Methods ====================
    // These replace the legacy KEY_DATA file in contracts directory

    /// Store a contract index entry: ContractInstanceId → CodeHash
    pub fn store_contract_index(
        &self,
        instance_id: &ContractInstanceId,
        code_hash: &CodeHash,
    ) -> Result<(), redb::Error> {
        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(CONTRACT_INDEX_TABLE)?;
            tbl.insert(instance_id.as_ref(), code_hash.as_ref())?;
        }
        Self::commit_guarded(txn)
    }

    /// Get the CodeHash for a ContractInstanceId
    pub fn get_contract_index(
        &self,
        instance_id: &ContractInstanceId,
    ) -> Result<Option<CodeHash>, redb::Error> {
        self.read_guarded(|txn| {
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
        })
    }

    /// Remove a contract index entry
    pub fn remove_contract_index(
        &self,
        instance_id: &ContractInstanceId,
    ) -> Result<(), redb::Error> {
        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(CONTRACT_INDEX_TABLE)?;
            tbl.remove(instance_id.as_ref())?;
        }
        Self::commit_guarded(txn)
    }

    /// Load all contract index entries
    pub fn load_all_contract_index(
        &self,
    ) -> Result<Vec<(ContractInstanceId, CodeHash)>, redb::Error> {
        self.read_guarded(|txn| {
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
        })
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

        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(DELEGATE_INDEX_TABLE)?;
            tbl.insert(key_bytes.as_slice(), code_hash.as_ref())?;
        }
        Self::commit_guarded(txn)
    }

    /// Get the CodeHash for a DelegateKey
    pub fn get_delegate_index(
        &self,
        delegate_key: &DelegateKey,
    ) -> Result<Option<CodeHash>, redb::Error> {
        let mut key_bytes = [0u8; 64];
        key_bytes[..32].copy_from_slice(delegate_key.as_ref());
        key_bytes[32..].copy_from_slice(delegate_key.code_hash().as_ref());

        self.read_guarded(|txn| {
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
        })
    }

    /// Remove a delegate index entry
    pub fn remove_delegate_index(&self, delegate_key: &DelegateKey) -> Result<(), redb::Error> {
        let mut key_bytes = [0u8; 64];
        key_bytes[..32].copy_from_slice(delegate_key.as_ref());
        key_bytes[32..].copy_from_slice(delegate_key.code_hash().as_ref());

        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(DELEGATE_INDEX_TABLE)?;
            tbl.remove(key_bytes.as_slice())?;
        }
        Self::commit_guarded(txn)
    }

    /// Load all delegate index entries
    pub fn load_all_delegate_index(&self) -> Result<Vec<(DelegateKey, CodeHash)>, redb::Error> {
        self.read_guarded(|txn| {
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
        })
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

        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(SECRETS_INDEX_TABLE)?;
            tbl.insert(key_bytes.as_slice(), value_bytes.as_slice())?;
        }
        Self::commit_guarded(txn)
    }

    /// Get the secret key hashes for a DelegateKey
    pub fn get_secrets_index(
        &self,
        delegate_key: &DelegateKey,
    ) -> Result<Option<Vec<[u8; 32]>>, redb::Error> {
        let mut key_bytes = [0u8; 64];
        key_bytes[..32].copy_from_slice(delegate_key.as_ref());
        key_bytes[32..].copy_from_slice(delegate_key.code_hash().as_ref());

        self.read_guarded(|txn| {
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
        })
    }

    /// Remove a secrets index entry
    pub fn remove_secrets_index(&self, delegate_key: &DelegateKey) -> Result<(), redb::Error> {
        let mut key_bytes = [0u8; 64];
        key_bytes[..32].copy_from_slice(delegate_key.as_ref());
        key_bytes[32..].copy_from_slice(delegate_key.code_hash().as_ref());

        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(SECRETS_INDEX_TABLE)?;
            tbl.remove(key_bytes.as_slice())?;
        }
        Self::commit_guarded(txn)
    }

    /// Load all secrets index entries
    #[allow(clippy::type_complexity)]
    pub fn load_all_secrets_index(&self) -> Result<Vec<(DelegateKey, Vec<[u8; 32]>)>, redb::Error> {
        self.read_guarded(|txn| {
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
        })
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

        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(USER_SECRETS_INDEX_TABLE)?;
            tbl.insert(key_bytes.as_slice(), value_bytes.as_slice())?;
        }
        Self::commit_guarded(txn)
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

        self.read_guarded(|txn| {
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
        })
    }

    /// Remove a per-user secrets index entry. Called by the inactive-user TTL
    /// reclaim (#4561, P5 of #4381) in production, and by index tests.
    pub fn remove_user_secrets_index(
        &self,
        delegate_key: &DelegateKey,
        user_id: &[u8; 32],
    ) -> Result<(), redb::Error> {
        let key_bytes = Self::user_index_key(delegate_key, user_id);

        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(USER_SECRETS_INDEX_TABLE)?;
            tbl.remove(key_bytes.as_slice())?;
        }
        Self::commit_guarded(txn)
    }

    /// Load all per-user secrets index entries as
    /// `((DelegateKey, UserId bytes), secret key hashes)`.
    #[allow(clippy::type_complexity)]
    pub fn load_all_user_secrets_index(
        &self,
    ) -> Result<Vec<((DelegateKey, [u8; 32]), Vec<[u8; 32]>)>, redb::Error> {
        self.read_guarded(|txn| {
            let tbl = txn.open_table(USER_SECRETS_INDEX_TABLE)?;

            let mut result = Vec::new();
            for entry in tbl.iter()? {
                let (key, value) = entry?;
                let key_bytes = key.value();
                if key_bytes.len() != 96 {
                    // Skip malformed entries. The write path always emits a
                    // 96-byte composite key (DelegateKey(64) || UserId(32)); a
                    // wrong length means an externally-corrupted or
                    // future-format row. Drop it rather than panic on the
                    // fixed-size `try_into`s below, and warn so the corruption
                    // is visible in monitoring.
                    tracing::warn!(
                        len = key_bytes.len(),
                        "Skipping malformed user-secrets-index row (key length != 96)"
                    );
                    continue;
                }
                let delegate_key_bytes: [u8; 32] = key_bytes[..32].try_into().unwrap();
                let code_hash_bytes: [u8; 32] = key_bytes[32..64].try_into().unwrap();
                let user_id_bytes: [u8; 32] = key_bytes[64..].try_into().unwrap();

                let value_bytes = value.value();
                if value_bytes.len() % 32 != 0 {
                    // Skip malformed entries. The value is a concatenation of
                    // 32-byte secret-key hashes, so a length not divisible by
                    // 32 is corruption. Warn and drop rather than splitting a
                    // partial hash.
                    tracing::warn!(
                        len = value_bytes.len(),
                        "Skipping malformed user-secrets-index row (value length not a multiple of 32)"
                    );
                    continue;
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
        })
    }

    // ============ Delegate Secret Copy-Forward Marker (#4117) ============
    // One-shot idempotence / anti-resurrection marker keyed on the
    // `(predecessor, successor)` delegate pair. See `MIGRATION_MARKER_TABLE`
    // and `SecretsStore::migrate_secrets`.

    /// Build the 128-byte composite key `predecessor(64) || successor(64)`,
    /// where each 64-byte half is `DelegateKey.key(32) || code_hash(32)` — the
    /// same DelegateKey encoding the secrets-index tables use.
    fn migration_marker_key(predecessor: &DelegateKey, successor: &DelegateKey) -> [u8; 128] {
        let mut key_bytes = [0u8; 128];
        key_bytes[..32].copy_from_slice(predecessor.as_ref());
        key_bytes[32..64].copy_from_slice(predecessor.code_hash().as_ref());
        key_bytes[64..96].copy_from_slice(successor.as_ref());
        key_bytes[96..].copy_from_slice(successor.code_hash().as_ref());
        key_bytes
    }

    /// Persist the copy-forward marker for `(predecessor, successor)`. The
    /// `value` is the opaque, versioned audit blob built by the secrets store.
    /// Idempotent: re-writing the same pair overwrites in place.
    pub fn store_migration_marker(
        &self,
        predecessor: &DelegateKey,
        successor: &DelegateKey,
        value: &[u8],
    ) -> Result<(), redb::Error> {
        let key_bytes = Self::migration_marker_key(predecessor, successor);
        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(MIGRATION_MARKER_TABLE)?;
            tbl.insert(key_bytes.as_slice(), value)?;
        }
        Self::commit_guarded(txn)
    }

    /// Fetch the copy-forward marker for `(predecessor, successor)`, or `None`
    /// if this pair has never been migrated. The returned bytes are the opaque
    /// audit blob the secrets store wrote; only the store interprets them.
    pub fn get_migration_marker(
        &self,
        predecessor: &DelegateKey,
        successor: &DelegateKey,
    ) -> Result<Option<Vec<u8>>, redb::Error> {
        let key_bytes = Self::migration_marker_key(predecessor, successor);
        self.read_guarded(|txn| {
            let tbl = txn.open_table(MIGRATION_MARKER_TABLE)?;
            match tbl.get(key_bytes.as_slice())? {
                Some(v) => Ok(Some(v.value().to_vec())),
                None => Ok(None),
            }
        })
    }

    // ========== Delegate registration origins (#4117 H1) ==========

    /// 64-byte delegate key `DelegateKey.key(32) || code_hash(32)` — the same
    /// encoding the secrets-index tables use.
    ///
    /// NOTE (#4117 L3): this redb row key includes the `code_hash`, whereas the
    /// copy-forward's on-disk namespace and the migration gate are keyed on the
    /// 32-byte delegate KEY only (`DelegateKey::encode()`). This is harmless:
    /// `key = BLAKE3(code_hash || params)`, so key and code_hash move together —
    /// two `DelegateKey`s that share a key but differ in code_hash cannot occur
    /// for a real delegate. The wider row key just matches the sibling
    /// secrets-index tables' convention.
    fn delegate_key64(delegate: &DelegateKey) -> [u8; 64] {
        let mut b = [0u8; 64];
        b[..32].copy_from_slice(delegate.as_ref());
        b[32..].copy_from_slice(delegate.code_hash().as_ref());
        b
    }

    /// Record a web-app contract origin for `delegate` (idempotent). `origin =
    /// None` records the Admin/None class (loopback / CLI registration). Grows
    /// the row's origin set; a repeated origin is a no-op.
    pub fn record_delegate_origin(
        &self,
        delegate: &DelegateKey,
        origin: Option<[u8; 32]>,
    ) -> Result<(), redb::Error> {
        let key = Self::delegate_key64(delegate);
        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(DELEGATE_ORIGINS_TABLE)?;
            let (mut has_admin_none, mut origins) = match tbl.get(key.as_slice())? {
                Some(v) => Self::decode_origins(v.value()),
                None => (false, Vec::new()),
            };
            match origin {
                None => has_admin_none = true,
                Some(o) => {
                    if !origins.iter().any(|e| e == &o) {
                        origins.push(o);
                    }
                }
            }
            let mut value = Vec::with_capacity(1 + origins.len() * 32);
            value.push(u8::from(has_admin_none));
            for o in &origins {
                value.extend_from_slice(o);
            }
            tbl.insert(key.as_slice(), value.as_slice())?;
        }
        Self::commit_guarded(txn)
    }

    /// Fetch `delegate`'s recorded origins as `(has_admin_none, origins)`, or
    /// `None` if the delegate has never been registered on this node (the
    /// grandfather case for copy-forward).
    #[allow(clippy::type_complexity)]
    pub fn get_delegate_origins(
        &self,
        delegate: &DelegateKey,
    ) -> Result<Option<(bool, Vec<[u8; 32]>)>, redb::Error> {
        let key = Self::delegate_key64(delegate);
        self.read_guarded(|txn| {
            let tbl = txn.open_table(DELEGATE_ORIGINS_TABLE)?;
            match tbl.get(key.as_slice())? {
                Some(v) => Ok(Some(Self::decode_origins(v.value()))),
                None => Ok(None),
            }
        })
    }

    fn decode_origins(bytes: &[u8]) -> (bool, Vec<[u8; 32]>) {
        if bytes.is_empty() {
            return (false, Vec::new());
        }
        let has_admin_none = bytes[0] != 0;
        let rest = &bytes[1..];
        let mut origins = Vec::with_capacity(rest.len() / 32);
        for chunk in rest.chunks_exact(32) {
            let arr: [u8; 32] = chunk.try_into().unwrap();
            origins.push(arr);
        }
        (has_admin_none, origins)
    }

    // ========== Reserved-marker hashes (#4117 finding 4a) ==========

    /// Record that `delegate` holds a reserved-namespace coordination secret
    /// with hash `hash` (idempotent). Uncapped and registry-independent.
    pub fn add_reserved_marker_hash(
        &self,
        delegate: &DelegateKey,
        hash: &[u8; 32],
    ) -> Result<(), redb::Error> {
        let key = Self::delegate_key64(delegate);
        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(RESERVED_MARKER_HASHES_TABLE)?;
            let mut hashes: Vec<u8> = match tbl.get(key.as_slice())? {
                Some(v) => v.value().to_vec(),
                None => Vec::new(),
            };
            // Idempotent: skip if already present.
            if !hashes.chunks_exact(32).any(|c| c == hash.as_slice()) {
                hashes.extend_from_slice(hash);
                tbl.insert(key.as_slice(), hashes.as_slice())?;
            }
        }
        Self::commit_guarded(txn)
    }

    /// All reserved-namespace secret hashes recorded for `delegate`.
    pub fn get_reserved_marker_hashes(
        &self,
        delegate: &DelegateKey,
    ) -> Result<Vec<[u8; 32]>, redb::Error> {
        let key = Self::delegate_key64(delegate);
        self.read_guarded(|txn| {
            let tbl = txn.open_table(RESERVED_MARKER_HASHES_TABLE)?;
            match tbl.get(key.as_slice())? {
                Some(v) => {
                    let bytes = v.value();
                    let mut out = Vec::with_capacity(bytes.len() / 32);
                    for chunk in bytes.chunks_exact(32) {
                        let arr: [u8; 32] = chunk.try_into().unwrap();
                        out.push(arr);
                    }
                    Ok(out)
                }
                None => Ok(Vec::new()),
            }
        })
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
        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(BROKEN_INVARIANTS_TABLE)?;
            tbl.insert(instance_id.as_ref(), &[kind_byte][..])?;
        }
        Self::commit_guarded(txn)
    }

    /// Remove a persisted broken-invariant flag. Paired with
    /// `BrokenInvariantsTracker::clear` — without on-disk removal, an
    /// operator's unflag would be undone on the next restart's
    /// `set_storage` rehydration.
    pub fn remove_broken_invariant(
        &self,
        instance_id: &ContractInstanceId,
    ) -> Result<(), redb::Error> {
        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(BROKEN_INVARIANTS_TABLE)?;
            tbl.remove(instance_id.as_ref())?;
        }
        Self::commit_guarded(txn)
    }

    /// Load all persisted broken-invariant flags. Malformed rows (wrong
    /// key length, wrong value length) are skipped with a warning rather
    /// than failing the entire load — a corrupted entry should not block
    /// startup, and the worst case is we lose a flag and re-detect it.
    pub fn load_all_broken_invariants(&self) -> Result<Vec<(ContractInstanceId, u8)>, redb::Error> {
        self.read_guarded(|txn| {
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
        })
    }
}

impl StateStorage for ReDb {
    type Error = redb::Error;

    async fn store(&self, key: ContractKey, state: WrappedState) -> Result<(), Self::Error> {
        let state_size = state.size() as u64;
        let txn = self.begin_write()?;

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

        Self::commit_guarded(txn)
    }

    async fn get(&self, key: &ContractKey) -> Result<Option<WrappedState>, Self::Error> {
        self.read_guarded(|txn| {
            let tbl = txn.open_table(STATE_TABLE)?;
            Ok(tbl
                .get(key.as_bytes())?
                .map(|v| WrappedState::new(v.value().to_vec())))
        })
    }

    async fn store_params(
        &self,
        key: ContractKey,
        params: Parameters<'static>,
    ) -> Result<(), Self::Error> {
        let txn = self.begin_write()?;

        {
            let mut tbl = txn.open_table(CONTRACT_PARAMS_TABLE)?;
            tbl.insert(key.as_bytes(), params.as_ref())?;
        }
        Self::commit_guarded(txn)
    }

    async fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> Result<Option<Parameters<'static>>, Self::Error> {
        self.read_guarded(|txn| {
            let tbl = txn.open_table(CONTRACT_PARAMS_TABLE)?;
            Ok(tbl
                .get(key.as_bytes())?
                .map(|v| Parameters::from(v.value().to_vec())))
        })
    }

    async fn remove(&self, key: &ContractKey) -> Result<(), Self::Error> {
        // Delete from all three per-key tables in a single write transaction
        // so the removal is atomic. `redb`'s `Table::remove` does not error
        // when the key is absent, so this is naturally idempotent.
        let txn = self.begin_write()?;
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
        Self::commit_guarded(txn)
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

    // ==================== Per-User Secrets Index (P1 of #4381) ====================

    /// Build a deterministic `DelegateKey` from two seed bytes (one for the
    /// instance key, one for the code hash) for the per-user index tests.
    fn fake_delegate_key(key_seed: u8, code_seed: u8) -> DelegateKey {
        DelegateKey::new([key_seed; 32], CodeHash::from(&[code_seed; 32]))
    }

    /// Full store → get → remove → load round trip for the per-user secrets
    /// index, exercising `store_user_secrets_index`, `get_user_secrets_index`,
    /// `remove_user_secrets_index` (otherwise uncalled in non-test builds),
    /// and `load_all_user_secrets_index`. Pins that the composite
    /// `(DelegateKey, UserId)` key round-trips and that two users under the
    /// same delegate are independent.
    #[tokio::test]
    async fn user_secrets_index_store_get_remove_load_round_trip() {
        let temp_dir = TempDir::new().unwrap();
        let db = ReDb::new(temp_dir.path()).await.unwrap();

        let delegate = fake_delegate_key(0x11, 0x22);
        let alice: [u8; 32] = [0xAA; 32];
        let bob: [u8; 32] = [0xBB; 32];
        let alice_secrets = vec![[1u8; 32], [2u8; 32]];
        let bob_secrets = vec![[3u8; 32]];

        // Store under two distinct users of the same delegate.
        db.store_user_secrets_index(&delegate, &alice, &alice_secrets)
            .unwrap();
        db.store_user_secrets_index(&delegate, &bob, &bob_secrets)
            .unwrap();

        // Point-query each user back.
        assert_eq!(
            db.get_user_secrets_index(&delegate, &alice).unwrap(),
            Some(alice_secrets.clone()),
            "alice's secret set must round-trip"
        );
        assert_eq!(
            db.get_user_secrets_index(&delegate, &bob).unwrap(),
            Some(bob_secrets.clone()),
            "bob's secret set must round-trip independently"
        );

        // load_all returns both rows.
        let mut loaded = db.load_all_user_secrets_index().unwrap();
        loaded.sort_by_key(|((_, user), _)| *user);
        let mut expected = vec![
            ((delegate.clone(), alice), alice_secrets.clone()),
            ((delegate.clone(), bob), bob_secrets.clone()),
        ];
        expected.sort_by_key(|((_, user), _)| *user);
        assert_eq!(loaded, expected, "load_all must return both users' rows");

        // Remove alice; bob is untouched, alice point-query is now None.
        db.remove_user_secrets_index(&delegate, &alice).unwrap();
        assert_eq!(
            db.get_user_secrets_index(&delegate, &alice).unwrap(),
            None,
            "removed user must read back as None"
        );
        assert_eq!(
            db.get_user_secrets_index(&delegate, &bob).unwrap(),
            Some(bob_secrets.clone()),
            "removing alice must not touch bob"
        );
        let remaining = db.load_all_user_secrets_index().unwrap();
        assert_eq!(
            remaining,
            vec![((delegate, bob), bob_secrets)],
            "only bob's row must remain after removing alice"
        );
    }

    /// Malformed rows in `USER_SECRETS_INDEX_TABLE` must be skipped (not
    /// panic, not abort the whole load), mirroring
    /// `broken_invariants_load_skips_malformed_value`. We inject (a) a 95-byte
    /// key (composite key must be 96 bytes) and (b) a value whose length is
    /// not a multiple of 32, then assert a well-formed row still loads and the
    /// malformed ones are dropped.
    #[tokio::test]
    async fn user_secrets_index_load_skips_malformed_rows() {
        use redb::Database;
        let temp_dir = TempDir::new().unwrap();
        let db = ReDb::new(temp_dir.path()).await.unwrap();

        // Seed one well-formed row through the public API so we can confirm
        // the good row survives alongside the malformed injections.
        let good_delegate = fake_delegate_key(0x01, 0x02);
        let good_user: [u8; 32] = [0x03; 32];
        let good_secrets = vec![[0x44u8; 32]];
        db.store_user_secrets_index(&good_delegate, &good_user, &good_secrets)
            .unwrap();

        let db_path = temp_dir.path().join("db");
        // Drop the wrapper so the raw redb file lock is released before we
        // open it directly to inject the malformed rows.
        drop(db);
        let raw = Database::open(&db_path).unwrap();
        {
            let txn = raw.begin_write().unwrap();
            {
                let mut tbl = txn.open_table(USER_SECRETS_INDEX_TABLE).unwrap();
                // (a) 95-byte key (one short of the required 96) with an
                // otherwise valid value.
                let short_key = [0xEE_u8; 95];
                let valid_value = [0x55_u8; 32];
                tbl.insert(short_key.as_slice(), valid_value.as_slice())
                    .unwrap();
                // (b) valid 96-byte key but a value whose length is not a
                // multiple of 32 (33 bytes).
                let valid_key = [0xCD_u8; 96];
                let bogus_value = [0x66_u8; 33];
                tbl.insert(valid_key.as_slice(), bogus_value.as_slice())
                    .unwrap();
            }
            txn.commit().unwrap();
        }
        drop(raw);

        let db = ReDb::new(temp_dir.path()).await.unwrap();
        let loaded = db.load_all_user_secrets_index().unwrap();
        assert_eq!(
            loaded,
            vec![((good_delegate, good_user), good_secrets)],
            "malformed key/value rows must be skipped, leaving only the good row"
        );
    }

    // ==================== #4604: redb poison-recovery ====================

    /// A redb [`redb::StorageBackend`] over an in-memory buffer that can be flipped
    /// to return `io::Error` from every I/O method. Used to produce a REAL redb
    /// poison deterministically (a genuine `StorageError::Io` that makes redb set its
    /// in-memory poison flag, after which every transaction returns
    /// `StorageError::PreviousIo`) so the poison-detection and recovery path can be
    /// exercised without relying on the error message string.
    #[derive(Debug, Clone)]
    struct FailingBackend {
        inner: Arc<redb::backends::InMemoryBackend>,
        fail: Arc<std::sync::atomic::AtomicBool>,
    }

    impl FailingBackend {
        fn new() -> Self {
            Self {
                inner: Arc::new(redb::backends::InMemoryBackend::new()),
                fail: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            }
        }

        /// Make every subsequent I/O call fail, simulating a disk EIO / csum failure.
        fn start_failing(&self) {
            self.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn check(&self) -> std::io::Result<()> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                Err(std::io::Error::other(
                    "injected I/O failure (#4604 redb-poison test)",
                ))
            } else {
                Ok(())
            }
        }
    }

    impl redb::StorageBackend for FailingBackend {
        fn len(&self) -> std::io::Result<u64> {
            self.check()?;
            self.inner.len()
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
            self.check()?;
            self.inner.read(offset, out)
        }
        fn set_len(&self, len: u64) -> std::io::Result<()> {
            self.check()?;
            self.inner.set_len(len)
        }
        fn sync_data(&self) -> std::io::Result<()> {
            self.check()?;
            self.inner.sync_data()
        }
        fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
            self.check()?;
            self.inner.write(offset, data)
        }
    }

    /// Open a fully-initialised [`ReDb`] over an arbitrary backend (test-only).
    fn open_redb_with_backend<B: redb::StorageBackend>(backend: B) -> ReDb {
        let db = Database::builder()
            .create_with_backend(backend)
            .expect("create_with_backend");
        ReDb::initialize_database(db).expect("initialize_database")
    }

    /// Poison detection must be PRECISE (issue #4604, requirement 1): it must fire on
    /// the real underlying-I/O / poison errors and NOT on benign app-level errors.
    /// Uses REAL redb errors produced via the fault-injecting backend, so it is
    /// resilient to redb wording changes (we match variants, not strings).
    #[test]
    fn redb_poison_classifier_is_precise() {
        let backend = FailingBackend::new();
        let db = Database::builder()
            .create_with_backend(backend.clone())
            .unwrap();
        {
            let w = db.begin_write().unwrap();
            w.open_table(STATE_TABLE).unwrap();
            w.commit().unwrap();
        }

        // Benign: opening a non-existent table is a TableError, never an I/O poison.
        // (TableDoesNotExist is not a `Storage(..)` variant at all, so it can never
        // classify as poison; a `Storage(_)` here would still have to be non-poison.)
        {
            let r = db.begin_read().unwrap();
            let missing: TableDefinition<&[u8], &[u8]> = TableDefinition::new("nope");
            if let redb::TableError::Storage(s) = r.open_table(missing).unwrap_err() {
                assert!(
                    !storage_error_is_poison(&s),
                    "a benign table-open storage error must not classify as poison"
                );
            }
        }

        // Trigger a REAL I/O failure → the triggering op returns StorageError::Io,
        // which must classify as poison (the "underlying-I/O-error class").
        backend.start_failing();
        // The injected I/O error surfaces either when `begin_write` does I/O or, more
        // commonly, at `commit` — capture whichever StorageError it is.
        let storage_err: StorageError = match db.begin_write() {
            // begin itself may hit the injected I/O error first.
            Err(TransactionError::Storage(s)) => s,
            Err(other) => panic!("unexpected begin error: {other:?}"),
            Ok(w) => {
                {
                    let mut t = w.open_table(STATE_TABLE).unwrap();
                    // Buffered write; the backend failure surfaces at commit.
                    let _insert = t.insert([1u8, 2, 3].as_slice(), [4u8, 5, 6].as_slice());
                }
                match w.commit() {
                    Ok(()) => {
                        panic!("commit unexpectedly succeeded while backend was failing")
                    }
                    Err(redb::CommitError::Storage(s)) => s,
                    Err(other) => panic!("unexpected commit error: {other:?}"),
                }
            }
        };
        assert!(
            storage_error_is_poison(&storage_err),
            "the underlying I/O error (StorageError::Io) must classify as poison"
        );

        // redb is now poisoned: every begin returns PreviousIo, which the universal
        // begin_* choke point classifies as poison.
        let begin_err = match db.begin_write() {
            Ok(_) => panic!("a poisoned database must reject begin_write"),
            Err(e) => e,
        };
        assert!(
            transaction_error_is_poison(&begin_err),
            "PreviousIo from a poisoned database's begin_write must classify as poison"
        );

        // The umbrella read-path classifier must also flag the real PreviousIo...
        let umbrella: redb::Error = begin_err.into();
        assert!(
            redb_error_is_poison(&umbrella),
            "PreviousIo must classify as poison on the umbrella read path too"
        );

        // ...but must NOT flag the synthetic `Io(InvalidData)` that several read
        // methods produce for a benign malformed-data row. Misclassifying it would
        // exit-and-restart the node on a single bad row (a crash loop) — the
        // false-positive this asymmetry exists to prevent (#4604).
        let malformed_row = redb::Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid CodeHash length",
        ));
        assert!(
            !redb_error_is_poison(&malformed_row),
            "a synthetic Io(InvalidData) malformed-row error must NOT be treated as poison"
        );
    }

    /// End-to-end (issue #4604, requirement 3): a poisoned database routes contract
    /// ops to the recovery path (process-exit-for-restart in production) rather than
    /// failing forever, while a benign not-found does NOT. The recovery handler is
    /// opt-in and OFF in tests, so it returns instead of exiting; the test-only
    /// counter proves the `begin_*` wrapper recognised the poison and would have
    /// exited under the real node binary.
    #[test]
    fn poisoned_redb_takes_recovery_path_benign_does_not() {
        use std::sync::atomic::Ordering;

        let backend = FailingBackend::new();
        let db = open_redb_with_backend(backend.clone());
        let key = make_test_key();

        // Benign not-found: Ok(None), recovery path NOT taken.
        POISON_RECOVERY_TRIGGERED.store(0, Ordering::SeqCst);
        assert!(db.get_state_sync(&key).unwrap().is_none());
        db.store_state_sync(&key, WrappedState::new(vec![1, 2, 3]))
            .unwrap();
        assert_eq!(
            POISON_RECOVERY_TRIGGERED.load(Ordering::SeqCst),
            0,
            "benign not-found / normal ops must NOT take the poison-recovery path"
        );

        // Poison the backend; the write that triggers the FIRST backend I/O error
        // (usually at commit) must take the recovery path on the very op that
        // poisons the handle — not wait for a later op. redb also latches its
        // in-memory poison flag (io_failed) here, set on ANY backend read/write error.
        backend.start_failing();
        POISON_RECOVERY_TRIGGERED.store(0, Ordering::SeqCst);
        assert!(
            db.store_state_sync(&key, WrappedState::new(vec![4, 5, 6]))
                .is_err(),
            "the injected I/O failure must surface as an error"
        );
        assert!(
            POISON_RECOVERY_TRIGGERED.load(Ordering::SeqCst) >= 1,
            "the poisoning write (commit-time I/O error) must take the recovery path \
             on the same op, not only on a later one"
        );

        // The database stays poisoned: redb's poison flag is checked by every
        // `begin_write` (the node writes hosting metadata on essentially every
        // contract op), so the next write returns PreviousIo, which the begin_write
        // wrapper also routes to the recovery (exit-for-restart) path instead of the
        // old fail-forever behaviour.
        POISON_RECOVERY_TRIGGERED.store(0, Ordering::SeqCst);
        assert!(
            db.store_state_sync(&key, WrappedState::new(vec![7, 8, 9]))
                .is_err(),
            "a poisoned write must return an error, not silently no-op"
        );
        assert!(
            POISON_RECOVERY_TRIGGERED.load(Ordering::SeqCst) >= 1,
            "a poisoned database write must take the recovery (exit-for-restart) path \
             rather than failing forever"
        );

        // Read path: redb's `begin_read` does not check the poison flag, so a poison
        // surfaces inside the read body (at open_table/get/iter) as a `PreviousIo`.
        // Feed `read_guarded` a real `PreviousIo` (obtained from the now-poisoned
        // handle's begin_write) to prove the read path routes to recovery too, not
        // just the write path. (A poisoned read served from cache would succeed and
        // is not a failure; this exercises the failing-read body deterministically.)
        let previous_io: redb::Error = match db.begin_write() {
            Ok(_) => panic!("database should still be poisoned"),
            Err(e) => e.into(),
        };
        assert!(
            redb_error_is_poison(&previous_io),
            "a poisoned handle's PreviousIo must classify as poison on the read path"
        );
        POISON_RECOVERY_TRIGGERED.store(0, Ordering::SeqCst);
        let routed: Result<(), redb::Error> = db.read_guarded(|_txn| Err(previous_io));
        assert!(routed.is_err());
        assert!(
            POISON_RECOVERY_TRIGGERED.load(Ordering::SeqCst) >= 1,
            "read_guarded must route a poison surfacing inside the read body to recovery"
        );
    }
}
