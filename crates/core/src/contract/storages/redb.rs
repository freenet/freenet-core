use std::path::Path;
use std::sync::{Arc, Mutex};

use freenet_stdlib::prelude::*;
use redb::{
    Database, DatabaseError, ReadTransaction, ReadableDatabase, ReadableTable, StorageError,
    TableDefinition, TransactionError, WriteTransaction,
};

use crate::wasm_runtime::StateStorage;

/// Minimum reclaimable bytes before a startup compaction is worth the whole-file
/// rewrite it costs. Paired with [`MIN_COMPACTION_RECLAIM_FRACTION`].
const MIN_COMPACTION_RECLAIM_BYTES: u64 = 64 * 1024 * 1024;

/// Minimum reclaimable share of the file before compaction runs, so a large
/// database with proportionally trivial dead space is left alone.
const MIN_COMPACTION_RECLAIM_FRACTION: f64 = 0.25;

/// Records the file size the last compaction attempt settled at, so a database
/// that cannot be compacted further is not rewritten on every start.
///
/// redb's compaction leaves a variable amount of unreclaimable free space: a
/// gateway settled at 1.9% but a laptop peer at 28.4%, which is above the
/// gate's 25% fraction. Without this marker that peer re-ran a full (and
/// entirely futile) compaction pass on every single restart.
const COMPACTION_MARKER_TABLE: TableDefinition<&[u8], u64> =
    TableDefinition::new("compaction_marker");

/// Key under which the post-compaction file size is stored.
const COMPACTION_MARKER_KEY: &[u8] = b"settled_at_bytes";

/// How much the file must grow past the last settled size before compaction is
/// worth attempting again. Compaction cannot help until genuinely new dead
/// space has accumulated, and the marker records where it bottomed out.
const COMPACTION_REGROWTH_FACTOR: f64 = 1.25;

/// What the startup reclaim pass decided. Returned so the decision itself is
/// observable: asserting on the resulting file size cannot distinguish "the gate
/// declined" from "compaction ran and happened to reclaim little", which made an
/// earlier round of these tests pass with the gate entirely disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReclaimOutcome {
    /// File is below the absolute floor; skipped without measuring.
    BelowFloor,
    /// Measured, but the dead space did not clear both gates.
    NotWorthwhile,
    /// Trailing slack accounted for it; the free trim sufficed, no rewrite.
    TrimSufficed,
    /// A full compaction ran.
    Compacted,
    /// Already compacted at (about) this size; compaction cannot help until the
    /// file grows further.
    AlreadySettled,
    /// Could not be determined (stat, stats or transaction failure); skipped.
    Undetermined,
}

#[cfg(test)]
thread_local! {
    static LAST_RECLAIM: std::cell::Cell<Option<ReclaimOutcome>> =
        const { std::cell::Cell::new(None) };
}

/// Record the reclaim decision so tests can assert on it. Compiled out of
/// non-test builds.
fn record_reclaim(outcome: ReclaimOutcome) -> ReclaimOutcome {
    #[cfg(test)]
    LAST_RECLAIM.with(|c| c.set(Some(outcome)));
    outcome
}

#[cfg(test)]
fn last_reclaim() -> Option<ReclaimOutcome> {
    LAST_RECLAIM.with(|c| c.get())
}

#[cfg(test)]
fn clear_last_reclaim() {
    LAST_RECLAIM.with(|c| c.set(None));
}

/// Whether reclaiming `file_bytes - in_use_bytes` justifies rewriting the whole
/// database file.
///
/// BOTH gates must pass. The absolute floor stops a small file being rewritten
/// for a small win; the fraction stops a multi-GB file being rewritten when its
/// dead space is proportionally trivial. Without them, a node in a restart loop
/// would rewrite its entire database on every start.
///
/// Pure so the boundary cases are unit-testable without building a real
/// database, following the same split as `budget_for_ram` /
/// `disk_budget_for_clamped`.
fn compaction_is_worthwhile(file_bytes: u64, in_use_bytes: u64) -> bool {
    let reclaimable = file_bytes.saturating_sub(in_use_bytes);
    let meets_floor = reclaimable >= MIN_COMPACTION_RECLAIM_BYTES;
    // Multiply rather than divide so a zero-length file can't produce a NaN.
    let meets_fraction =
        (reclaimable as f64) >= (file_bytes as f64) * MIN_COMPACTION_RECLAIM_FRACTION;
    meets_floor && meets_fraction
}

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
/// class).
///
/// **This gate alone is NOT sufficient protection (GHSA-824h-7x5x-wfmf).**
/// The registering request's `origin_contract` is itself forgeable by any HTTP
/// client (see GHSA-824h-7x5x-wfmf for the exploit chain), so a malicious web-app CAN obtain
/// a value that matches an unrelated victim delegate's recorded origin. The
/// actual protection today is that the copy-forward's sole caller
/// (`RegisterDelegateWithPredecessors`'s handler) is unconditionally disabled —
/// this gate is not currently invoked in production at all. Do not treat this
/// table as a sufficient authorization control if the copy-forward is ever
/// re-wired; `origin_contract` attestation needs hardening first. See
/// `SecretsStore::delegate_origins` and `SecretsStore::migrate_secrets`.
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
/// INDIVIDUALLY KEYED (#4117 P2a): one row per `(delegate, hash)`, so recording a
/// marker is a single insert, never a read-modify-write of a growing blob (an
/// amplification vector). Per-delegate rows are bounded
/// (`MAX_RESERVED_MARKER_HASHES_PER_DELEGATE`) and read via a prefix range scan.
///
/// Key: DelegateKey (64 bytes) || secret hash (32 bytes) = 96 bytes
/// Value: single presence byte
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

/// True if an error raised on a WRITE path signals a poisoned database.
///
/// Unlike [`redb_error_is_poison`], this DOES match `redb::Error::Io`. That
/// exclusion exists solely because several READ helpers in this file synthesize
/// `Io(ErrorKind::InvalidData)` for a benign malformed row, and treating those
/// as poison would crash-loop the node. No write path synthesizes `Io`, so on a
/// write it can only be a genuine backend failure — at which point redb has
/// already latched its poison flag and the handle is unusable for any further
/// write. Classifying it as benign is what would hand a dead handle to
/// `initialize_database` and stop the node booting.
fn redb_write_error_is_poison(e: &redb::Error) -> bool {
    matches!(e, redb::Error::Io(_)) || redb_error_is_poison(e)
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
            Ok(db) => {
                let db = Self::reclaim_free_pages(db, &db_path)?;
                Self::initialize_database(db)
            }
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
                // No reclaim pass here: the database was just created empty.
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

    /// Return free pages left behind by redb's copy-on-write writes to the OS.
    ///
    /// redb reuses freed pages for later writes but keeps the file at its
    /// all-time high-water mark: an ordinary commit only truncates *trailing*
    /// free space, so the *interior* dead space a long-running peer accumulates
    /// is unreclaimable by any normal operation. Measured on production peers
    /// (2026-07): a gateway holding 1.38 GB of live pages in a 2.59 GB file;
    /// compacting returned it to 1.41 GB with every row intact.
    ///
    /// Runs at startup because that is the one point where no transaction is
    /// live ([`Database::compact`] refuses otherwise) and the store is not
    /// serving anything yet. It is also the only point where a `&mut Database`
    /// exists at all: after `initialize_database` the handle is behind an `Arc`
    /// shared with every pool executor, so a background or on-eviction variant
    /// is not merely awkward, it is unrepresentable without reworking that
    /// sharing.
    ///
    /// Three stages, cheapest first, so an already-healthy node pays almost
    /// nothing:
    ///
    /// 1. Skip outright when the file is below the absolute floor; reclaimable
    ///    can never exceed the file, so no measurement is needed.
    /// 2. Measure, and skip unless the dead space clears both gates.
    /// 3. Close and reopen before compacting. Dropping the handle runs redb's
    ///    maximum-shrink trim, which returns *trailing* slack for free. That
    ///    slack is not dead space: it re-grows on the next write, and a node
    ///    that exits uncleanly (this binary calls `std::process::exit`, so the
    ///    destructor is skipped) can present 40% of it with zero fragmentation.
    ///    Re-measuring after the trim keeps a crash-restart loop from paying a
    ///    full rewrite for space a `ftruncate` already recovered.
    ///
    /// Returns the handle to use, reopening it if the file was touched. A
    /// compaction error is not fatal, but the handle is NOT reusable after one:
    /// redb latches an I/O failure and every later `begin_write` returns
    /// `PreviousIo`, which would turn a survivable compaction failure into a
    /// node that cannot start. So the handle is always reopened on that path.
    fn reclaim_free_pages(db: Database, db_path: &Path) -> Result<Database, redb::Error> {
        let file_bytes = match std::fs::metadata(db_path) {
            Ok(m) => m.len(),
            Err(e) => {
                tracing::warn!(
                    db_path = ?db_path,
                    error = %e,
                    "Could not stat the contract database; skipping compaction"
                );
                record_reclaim(ReclaimOutcome::Undetermined);
                return Ok(db);
            }
        };

        // Reclaimable can never exceed the file, so anything under the floor is
        // decided without paying for a stats read.
        if file_bytes < MIN_COMPACTION_RECLAIM_BYTES {
            tracing::info!(
                db_path = ?db_path,
                file_bytes,
                phase = "compaction_skipped",
                "Contract database below the compaction floor; skipping"
            );
            record_reclaim(ReclaimOutcome::BelowFloor);
            return Ok(db);
        }

        // A previous compaction recorded where it bottomed out. Free space below
        // that point is space compaction provably cannot reclaim, so re-running
        // it would burn a full-file pass to achieve nothing.
        if let Some(settled_bytes) = Self::read_compaction_marker(&db) {
            if (file_bytes as f64) <= (settled_bytes as f64) * COMPACTION_REGROWTH_FACTOR {
                tracing::info!(
                    db_path = ?db_path,
                    file_bytes,
                    settled_bytes,
                    phase = "compaction_skipped",
                    "Contract database already compacted at this size; skipping"
                );
                record_reclaim(ReclaimOutcome::AlreadySettled);
                return Ok(db);
            }
        }

        let Some(in_use_bytes) = Self::pages_in_use_bytes(&db, db_path) else {
            record_reclaim(ReclaimOutcome::Undetermined);
            return Ok(db);
        };
        if !compaction_is_worthwhile(file_bytes, in_use_bytes) {
            tracing::info!(
                db_path = ?db_path,
                file_bytes,
                in_use_bytes,
                phase = "compaction_skipped",
                "Contract database compaction not worthwhile; skipping"
            );
            record_reclaim(ReclaimOutcome::NotWorthwhile);
            return Ok(db);
        }

        // Trim trailing slack for free, then re-measure. Dropping the handle is
        // the only public route to redb's maximum-shrink (`set_shrink_policy` is
        // crate-private). Reaching here already means we were about to rewrite
        // the whole file, so the close/reopen is cheap by comparison — and a
        // healthy node never gets here, because the gate above declines first.
        drop(db);
        let db = Self::reopen_after_trim(db_path)?;
        let trimmed_bytes = match std::fs::metadata(db_path) {
            Ok(m) => m.len(),
            Err(e) => {
                // Fall back to skipping rather than to the pre-trim size: an
                // unknown size must not push us toward the expensive rewrite.
                tracing::warn!(
                    db_path = ?db_path,
                    error = %e,
                    "Could not stat the contract database after trimming; skipping compaction"
                );
                record_reclaim(ReclaimOutcome::Undetermined);
                return Ok(db);
            }
        };
        let Some(in_use_bytes) = Self::pages_in_use_bytes(&db, db_path) else {
            record_reclaim(ReclaimOutcome::Undetermined);
            return Ok(db);
        };
        if !compaction_is_worthwhile(trimmed_bytes, in_use_bytes) {
            tracing::info!(
                db_path = ?db_path,
                was_bytes = file_bytes,
                now_bytes = trimmed_bytes,
                in_use_bytes,
                phase = "compaction_trim_sufficed",
                "Trailing slack trimmed; full compaction not needed"
            );
            record_reclaim(ReclaimOutcome::TrimSufficed);
            return Ok(db);
        }

        // Logged either side rather than timed: `std::time::Instant` is barred
        // in this crate (see .claude/rules/code-style.md). The pair also makes
        // an interrupted compaction diagnosable — redb's `compact` is not
        // resumable, so a kill in this window forces a repair on next open
        // (two-phase commit throughout, so no data is lost).
        tracing::info!(
            db_path = ?db_path,
            was_bytes = file_bytes,
            now_bytes = trimmed_bytes,
            in_use_bytes,
            phase = "compaction_start",
            "Compacting contract database to reclaim free pages"
        );

        let mut db = db;
        match db.compact() {
            Ok(compacted) => {
                let now_bytes = std::fs::metadata(db_path)
                    .map(|m| m.len())
                    .unwrap_or(trimmed_bytes);
                tracing::info!(
                    db_path = ?db_path,
                    was_bytes = file_bytes,
                    now_bytes,
                    compacted,
                    phase = "compaction_done",
                    "Contract database compaction finished"
                );
                // Record where it settled. Whatever free space remains at this
                // size is unreclaimable, so the next start must not try again
                // until the file has grown past it.
                let healthy = Self::write_compaction_marker(&db, db_path, now_bytes);
                record_reclaim(ReclaimOutcome::Compacted);
                if healthy {
                    Ok(db)
                } else {
                    drop(db);
                    Self::reopen_after_trim(db_path)
                }
            }
            Err(e) => {
                // Deliberately NOT routed through `storage_error_is_poison` /
                // `abort_process_on_redb_poison` like the store's other I/O
                // errors: aborting here would loop the supervisor at startup.
                // Reopening clears redb's per-instance `io_failed` latch, which
                // would otherwise make every later `begin_write` return
                // `PreviousIo` and turn a survivable compaction failure into a
                // node that cannot boot. It does NOT rescue a persistent cause
                // (a full disk fails the reopen's repair too) — it converts the
                // transient case from fatal to survivable, nothing more.
                tracing::warn!(
                    db_path = ?db_path,
                    error = %e,
                    phase = "compaction_failed",
                    "Contract database compaction failed; reopening and continuing"
                );
                drop(db);
                record_reclaim(ReclaimOutcome::Undetermined);
                Self::reopen_after_trim(db_path)
            }
        }
    }

    /// Reopen the database after the handle was deliberately dropped.
    ///
    /// The drop released the file lock, so a competing opener can briefly win
    /// it; retry rather than failing startup over a microseconds-wide race.
    /// A failure here IS fatal (there is no handle left to fall back on), so it
    /// is logged with a phase before propagating — otherwise it would surface as
    /// a bare redb error with no hint that compaction was involved.
    fn reopen_after_trim(db_path: &Path) -> Result<Database, redb::Error> {
        const ATTEMPTS: usize = 5;
        let mut last_err = None;
        for attempt in 1..=ATTEMPTS {
            match Database::create(db_path) {
                Ok(db) => return Ok(db),
                Err(e) => {
                    tracing::warn!(
                        db_path = ?db_path,
                        error = %e,
                        attempt,
                        phase = "compaction_reopen_retry",
                        "Could not reopen the contract database after trimming; retrying"
                    );
                    last_err = Some(e);
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        }
        let e = last_err.expect("loop runs at least once");
        tracing::error!(
            db_path = ?db_path,
            error = %e,
            phase = "compaction_reopen_failed",
            "Could not reopen the contract database after trimming; cannot continue"
        );
        Err(e.into())
    }

    /// The file size the last compaction settled at, if one has been recorded.
    ///
    /// A missing table or key simply means no compaction has completed yet, so
    /// every failure here reads as "no marker" and lets the normal gate decide.
    fn read_compaction_marker(db: &Database) -> Option<u64> {
        let txn = db.begin_read().ok()?;
        let table = txn.open_table(COMPACTION_MARKER_TABLE).ok()?;
        let value = table.get(COMPACTION_MARKER_KEY).ok()??.value();
        Some(value)
    }

    /// Record where compaction bottomed out.
    ///
    /// Returns `false` if the write failed in a way that may have poisoned the
    /// handle, so the caller can reopen rather than hand a latched `Database` to
    /// `initialize_database` — whose own `begin_write` would then return
    /// `PreviousIo` and fail startup. That is the same failure mode the
    /// compaction-error path reopens for, and this write runs immediately after
    /// a whole-file rewrite, the highest-I/O-risk moment in the function.
    ///
    /// A benign failure (and there is no way to persist the marker) only costs
    /// a re-evaluation on the next start, which is the pre-marker behaviour.
    fn write_compaction_marker(db: &Database, db_path: &Path, settled_bytes: u64) -> bool {
        // Each redb call returns a different error type; they converge on the
        // umbrella `redb::Error` here so a single classifier can judge them.
        let result: Result<(), redb::Error> = (|| {
            let txn = db.begin_write()?;
            {
                let mut table = txn.open_table(COMPACTION_MARKER_TABLE)?;
                table.insert(COMPACTION_MARKER_KEY, settled_bytes)?;
            }
            txn.commit()?;
            Ok(())
        })();
        match result {
            Ok(()) => true,
            Err(e) => {
                // redb latches an I/O failure on the instance, so anything in
                // that class means the handle is no longer usable for writes.
                let poisoned = redb_write_error_is_poison(&e);
                tracing::warn!(
                    db_path = ?db_path,
                    error = %e,
                    poisoned,
                    "Could not record the compaction marker; the next start will re-evaluate"
                );
                !poisoned
            }
        }
    }

    /// Bytes held by pages the btrees actually occupy, or `None` if it could not
    /// be determined (in which case the caller skips compaction).
    ///
    /// `stats` lives on the write transaction, so one is opened purely to read
    /// it and explicitly aborted. `WriteTransaction::new` performs no disk
    /// writes and `abort` consumes the transaction, so nothing is left behind;
    /// redb's own `Database::new` uses the same begin/read/abort shape.
    fn pages_in_use_bytes(db: &Database, db_path: &Path) -> Option<u64> {
        let txn = match db.begin_write() {
            Ok(txn) => txn,
            Err(e) => {
                tracing::warn!(
                    db_path = ?db_path,
                    error = %e,
                    "Could not open a transaction to size the contract database; skipping compaction"
                );
                return None;
            }
        };
        let stats = txn.stats();
        // Abort regardless of the stats outcome so no transaction is left live
        // to block the compaction below.
        if let Err(e) = txn.abort() {
            tracing::warn!(
                db_path = ?db_path,
                error = %e,
                "Could not abort the sizing transaction; skipping compaction"
            );
            return None;
        }
        match stats {
            Ok(stats) => Some(
                stats
                    .allocated_pages()
                    .saturating_mul(stats.page_size() as u64),
            ),
            Err(e) => {
                tracing::warn!(
                    db_path = ?db_path,
                    error = %e,
                    "Could not read contract database stats; skipping compaction"
                );
                None
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
    pub(crate) fn store_contract_index(
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
    pub(crate) fn store_delegate_index(
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
    ///
    /// # Errors
    /// Returns `Err` if the underlying redb write transaction, table open, or
    /// commit fails (e.g. a backend I/O error).
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
    ///
    /// # Errors
    /// Returns `Err` if the underlying redb read transaction, table open, or
    /// lookup fails (e.g. a backend I/O error).
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

    /// Record the origin under which `delegate` was FIRST registered —
    /// FIRST-WRITER-WINS and IMMUTABLE (#4117 H1). The record is written only if
    /// none exists; a later registration NEVER modifies it. This is the whole
    /// security property: an attacker who re-registers a victim's
    /// (public-derivable) WASM later cannot add itself to the origin set, so it
    /// can never satisfy the copy-forward same-origin gate for that delegate.
    /// `origin = None` records the Admin/None class (a token-less / loopback
    /// registration), which the gate treats as NEVER privileged. Returns whether
    /// a NEW record was written (`false` if one already existed).
    ///
    /// Value encoding (unchanged): `[has_admin_none: 1][origin: 32 if Some]` — a
    /// first-Some writer stores `[0][C]`, a first-None writer stores `[1]`.
    ///
    /// # Errors
    /// Returns `Err` if the underlying redb write transaction, table open,
    /// lookup, or commit fails (e.g. a backend I/O error). The caller MUST fail
    /// the registration on `Err` (persistence-succeeds-before-usable).
    pub fn record_delegate_origin_first_writer(
        &self,
        delegate: &DelegateKey,
        origin: Option<[u8; 32]>,
    ) -> Result<bool, redb::Error> {
        let key = Self::delegate_key64(delegate);
        let txn = self.begin_write()?;
        let wrote = {
            let mut tbl = txn.open_table(DELEGATE_ORIGINS_TABLE)?;
            if tbl.get(key.as_slice())?.is_some() {
                false
            } else {
                let mut value = Vec::with_capacity(1 + 32);
                match origin {
                    None => value.push(1u8), // Admin/None class, no Some origin
                    Some(o) => {
                        value.push(0u8);
                        value.extend_from_slice(&o);
                    }
                }
                tbl.insert(key.as_slice(), value.as_slice())?;
                true
            }
        };
        Self::commit_guarded(txn)?;
        Ok(wrote)
    }

    /// Fetch `delegate`'s FIRST-registration origin as `(has_admin_none,
    /// origins)`, or `None` if the delegate has never been registered on this
    /// node (the NoProvenance case — copy-forward refuses). With first-writer
    /// -wins, `origins` holds at most one entry.
    ///
    /// # Errors
    /// Returns `Err` if the underlying redb read transaction, table open, or
    /// lookup fails (e.g. a backend I/O error). Copy-forward treats an `Err`
    /// here as fail-closed (refuse the copy).
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
            // `chunks_exact(32)` yields exactly-32 slices, but decode fallibly
            // (no production `.unwrap()`): a wrong-length chunk is skipped.
            if let Ok(arr) = <[u8; 32]>::try_from(chunk) {
                origins.push(arr);
            }
        }
        (has_admin_none, origins)
    }

    // ========== Reserved-marker hashes (#4117 finding 4a / P2a) ==========

    /// Per-delegate cap on recorded reserved-marker hashes. Bounds the table
    /// against a delegate that somehow accretes many reserved entries; a real
    /// delegate's reserved set is at most its predecessor count (itself capped at
    /// registration). Over the cap, further entries are not recorded (the
    /// below-cap `.keys` union still covers a freshly-written marker).
    pub(crate) const MAX_RESERVED_MARKER_HASHES_PER_DELEGATE: usize = 256;

    /// 96-byte row key `delegate_key64(64) || hash(32)` for the individually
    /// -keyed reserved-marker table (no read-modify-write of a growing blob —
    /// #4117 P2a).
    fn reserved_marker_row_key(delegate: &DelegateKey, hash: &[u8; 32]) -> [u8; 96] {
        let mut k = [0u8; 96];
        k[..64].copy_from_slice(&Self::delegate_key64(delegate));
        k[64..].copy_from_slice(hash);
        k
    }

    /// Record that `delegate` holds a reserved-namespace coordination secret with
    /// hash `hash`, as an individually-keyed row (#4117 P2a — no read-modify
    /// -write amplification). Idempotent; bounded at
    /// [`Self::MAX_RESERVED_MARKER_HASHES_PER_DELEGATE`] per delegate.
    ///
    /// # Errors
    /// Returns `Err` if the underlying redb write transaction, table open, range
    /// count, or commit fails (e.g. a backend I/O error).
    pub fn add_reserved_marker_hash(
        &self,
        delegate: &DelegateKey,
        hash: &[u8; 32],
    ) -> Result<(), redb::Error> {
        let row_key = Self::reserved_marker_row_key(delegate, hash);
        let (lo, hi) = Self::reserved_marker_range(delegate);
        let txn = self.begin_write()?;
        {
            let mut tbl = txn.open_table(RESERVED_MARKER_HASHES_TABLE)?;
            // Idempotent + capped. Counting the existing rows is bounded by the
            // cap; a present row means we're done.
            if tbl.get(row_key.as_slice())?.is_none() {
                let count = tbl.range(lo.as_slice()..=hi.as_slice())?.count();
                if count < Self::MAX_RESERVED_MARKER_HASHES_PER_DELEGATE {
                    tbl.insert(row_key.as_slice(), [1u8].as_slice())?;
                } else {
                    tracing::warn!(
                        delegate = %delegate.encode(),
                        cap = Self::MAX_RESERVED_MARKER_HASHES_PER_DELEGATE,
                        "reserved-marker hash set at cap; not recording (below-cap .keys union still covers fresh markers)"
                    );
                }
            }
        }
        Self::commit_guarded(txn)
    }

    /// Inclusive `[lo, hi]` 96-byte range bounds covering every reserved-marker
    /// row for `delegate` (its 64-byte prefix followed by all-zero .. all-ones
    /// hash).
    fn reserved_marker_range(delegate: &DelegateKey) -> ([u8; 96], [u8; 96]) {
        let mut lo = [0u8; 96];
        lo[..64].copy_from_slice(&Self::delegate_key64(delegate));
        let mut hi = [0xffu8; 96];
        hi[..64].copy_from_slice(&Self::delegate_key64(delegate));
        (lo, hi)
    }

    /// All reserved-namespace secret hashes recorded for `delegate`. Callers use
    /// this to EXCLUDE markers from copy-forward, so a read failure MUST NOT be
    /// silently treated as "no markers" (that would let a marker chain-copy as
    /// user data): the caller fails closed on `Err`.
    ///
    /// # Errors
    /// Returns `Err` if the underlying redb read transaction, table open, or
    /// range scan fails (e.g. a backend I/O error). Callers MUST fail closed on
    /// `Err` rather than treating it as "no markers".
    pub fn get_reserved_marker_hashes(
        &self,
        delegate: &DelegateKey,
    ) -> Result<Vec<[u8; 32]>, redb::Error> {
        let (lo, hi) = Self::reserved_marker_range(delegate);
        self.read_guarded(|txn| {
            let tbl = txn.open_table(RESERVED_MARKER_HASHES_TABLE)?;
            let mut out = Vec::new();
            for entry in tbl.range(lo.as_slice()..=hi.as_slice())? {
                let (k, _v) = entry?;
                let key_bytes = k.value();
                // Rows are always 96 bytes (delegate64||hash), but decode
                // fallibly (no production `.unwrap()`): a malformed row is
                // skipped rather than panicking the read.
                if let Some(suffix) = key_bytes.get(64..) {
                    if let Ok(hash) = <[u8; 32]>::try_from(suffix) {
                        out.push(hash);
                    }
                }
            }
            Ok(out)
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

// Test-only fault-injection helpers, re-exported so sibling modules' tests
// (e.g. the secrets-store origin-record failure path, #4117) can build a
// `ReDb` whose backend I/O can be flipped to fail on demand. Defined inside
// `mod tests` below; surfaced at module level here for cross-module test use.
#[cfg(test)]
pub(crate) use tests::{FailingBackend, open_redb_with_backend};

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

    /// Write `count` entries of `size` bytes, then delete all but every
    /// `keep_every`-th (always retaining the last, so the tail stays pinned).
    ///
    /// The scattering matters. redb truncates *trailing* free space on commit,
    /// so deleting a contiguous tail would simply shrink the file and prove
    /// nothing. Real peers accumulate *interior* dead space, which no ordinary
    /// commit can return to the OS — that is the state compaction exists to
    /// fix, and the state this fixture reproduces.
    ///
    /// Returns the number of rows left alive.
    fn bloat_database(db_path: &Path, count: usize, size: usize, keep_every: usize) -> usize {
        let keep = |i: usize| i % keep_every == 0 || i == count - 1;
        let db = Database::create(db_path).unwrap();
        {
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn.open_table(STATE_TABLE).unwrap();
                let value = vec![0xABu8; size];
                for i in 0..count {
                    table
                        .insert(&i.to_be_bytes()[..], value.as_slice())
                        .unwrap();
                }
            }
            txn.commit().unwrap();
        }
        {
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn.open_table(STATE_TABLE).unwrap();
                for i in 0..count {
                    if !keep(i) {
                        table.remove(&i.to_be_bytes()[..]).unwrap();
                    }
                }
            }
            txn.commit().unwrap();
        }
        drop(db);
        (0..count).filter(|i| keep(*i)).count()
    }

    /// Exact surviving contents: every key plus a digest of its value, so a
    /// compaction that corrupted, truncated or swapped values is caught. A bare
    /// row count would pass through all of those.
    fn state_contents(db_path: &Path) -> Vec<(Vec<u8>, usize, u64)> {
        let db = Database::open(db_path).unwrap();
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(STATE_TABLE).unwrap();
        let mut out = Vec::new();
        for entry in table.iter().unwrap() {
            let (k, v) = entry.unwrap();
            let bytes = v.value();
            // Cheap order-sensitive digest; full equality would hold 190 MiB.
            let digest = bytes
                .iter()
                .enumerate()
                .fold(1469598103934665603u64, |h, (i, b)| {
                    (h ^ ((*b as u64).wrapping_add(i as u64))).wrapping_mul(1099511628211)
                });
            out.push((k.value().to_vec(), bytes.len(), digest));
        }
        drop(txn);
        drop(db);
        out
    }

    /// Regression test for the unbounded on-disk growth: redb never returns
    /// freed pages to the OS, so a long-running peer's file keeps its all-time
    /// high-water mark. Production peers were sitting at 84% dead space.
    ///
    /// Fails without `reclaim_free_pages` (the file stays at its bloated size).
    #[tokio::test]
    async fn startup_compaction_reclaims_dead_pages_and_preserves_rows() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");

        // ~192 MiB written (redb doubles, so the file lands near 386 MiB), every
        // 16th row kept: clears both gates with the dead space interior.
        let live_rows = bloat_database(&db_path, 192, 1024 * 1024, 16);
        let expected = state_contents(&db_path);
        assert_eq!(expected.len(), live_rows);

        let before = std::fs::metadata(&db_path).unwrap().len();

        // Opening the store is what triggers the reclaim pass.
        clear_last_reclaim();
        let store = ReDb::new(temp_dir.path()).await.unwrap();
        drop(store);
        assert_eq!(
            last_reclaim(),
            Some(ReclaimOutcome::Compacted),
            "the fixture should have been compacted"
        );

        let after = std::fs::metadata(&db_path).unwrap().len();
        // The live rows are ~13 MiB, so a correct compaction lands far below
        // this. A loose `before / 2` would let a compaction that reclaimed only
        // part of the dead space pass silently.
        assert!(
            after < 64 * 1024 * 1024,
            "compaction should reclaim nearly all dead pages: {before} -> {after}"
        );
        assert_eq!(
            state_contents(&db_path),
            expected,
            "compaction must preserve every key and value byte-for-byte"
        );

        // Restart idempotence: the gate must DECLINE on the compacted file, or
        // the node would rewrite its whole database on every start forever.
        // Asserting the decision, not the size: re-compacting an already
        // compacted file also leaves it the same size, so a size assertion
        // would pass even if it recompacted every time.
        clear_last_reclaim();
        let store = ReDb::new(temp_dir.path()).await.unwrap();
        drop(store);
        assert_ne!(
            last_reclaim(),
            Some(ReclaimOutcome::Compacted),
            "a second open must NOT recompact; got {:?}",
            last_reclaim()
        );
        assert_eq!(state_contents(&db_path), expected);
    }

    /// Regression test for repeat compaction on every restart.
    ///
    /// redb's compaction leaves a variable amount of unreclaimable free space.
    /// A production laptop peer settled at 28.4% free — above the 25% fraction
    /// gate — and so re-ran a full, futile compaction pass on every restart
    /// (observed live: `compacted=false`, file byte-identical). A gateway
    /// settled at 1.9% and was unaffected.
    ///
    /// That data-dependence is exactly why this does NOT rely on a fixture
    /// reproducing the 28% residual: a synthetic fixture settles near 7%, where
    /// the ordinary gate already declines, so it would pass with or without the
    /// marker and prove nothing. Instead it plants a marker at the current size
    /// and asserts the decision, which pins the marker path deterministically
    /// whatever redb's allocator happens to leave behind.
    #[tokio::test]
    async fn startup_compaction_skips_when_already_settled_at_this_size() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");
        // A fixture that WOULD otherwise be compacted (verified by the sibling
        // test), so a skip here can only be the marker's doing.
        let live_rows = bloat_database(&db_path, 192, 1024 * 1024, 16);
        let expected = state_contents(&db_path);
        let file_bytes = std::fs::metadata(&db_path).unwrap().len();

        // Plant the marker: "compaction already bottomed out at this size".
        {
            let db = Database::create(&db_path).unwrap();
            ReDb::write_compaction_marker(&db, &db_path, file_bytes);
            drop(db);
        }

        clear_last_reclaim();
        let store = ReDb::new(temp_dir.path()).await.unwrap();
        drop(store);

        assert_eq!(
            last_reclaim(),
            Some(ReclaimOutcome::AlreadySettled),
            "a database already settled at this size must not be recompacted; got {:?}",
            last_reclaim()
        );
        assert_eq!(state_contents(&db_path), expected);
        assert_eq!(expected.len(), live_rows);
    }

    /// A completed compaction must record where it settled, otherwise the next
    /// start has nothing to consult and the repeat-compaction loop returns.
    #[tokio::test]
    async fn compaction_records_where_it_settled() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");
        bloat_database(&db_path, 192, 1024 * 1024, 16);

        clear_last_reclaim();
        let store = ReDb::new(temp_dir.path()).await.unwrap();
        drop(store);
        assert_eq!(last_reclaim(), Some(ReclaimOutcome::Compacted));

        let after = std::fs::metadata(&db_path).unwrap().len();
        let marker = {
            let db = Database::create(&db_path).unwrap();
            let m = ReDb::read_compaction_marker(&db);
            drop(db);
            m
        };
        let marker = marker.expect("compaction must record a marker");
        // Written from the post-compaction size, before the marker's own commit
        // grows the file slightly, so allow a small delta.
        assert!(
            marker.abs_diff(after) < 8 * 1024 * 1024,
            "marker {marker} should record the settled size {after}"
        );
    }

    /// A backend I/O failure during the marker write must be classified as
    /// poison. It is the exact scenario the reopen exists for, and the
    /// read-path classifier silently gets it wrong: it excludes
    /// `redb::Error::Io` so that a malformed row cannot crash-loop the node.
    /// Using it on the write path would report "benign", skip the reopen, and
    /// hand a latched handle to `initialize_database`, whose own `begin_write`
    /// then fails and stops the node booting.
    #[test]
    fn marker_write_io_failure_classifies_as_poison() {
        let io = redb::Error::Io(std::io::Error::other("injected backend failure"));
        assert!(
            redb_write_error_is_poison(&io),
            "a backend Io error on the WRITE path must count as poison"
        );
        // The read-path classifier must keep excluding it, or a single bad row
        // would exit-and-restart the node.
        assert!(
            !redb_error_is_poison(&io),
            "the read-path classifier must still treat Io as benign"
        );
        // Both agree on the unambiguous signals.
        for e in [redb::Error::PreviousIo, redb::Error::DatabaseClosed] {
            assert!(redb_write_error_is_poison(&e));
            assert!(redb_error_is_poison(&e));
        }
    }

    /// End-to-end: with the backend failing, `ReDb::new` must still return a
    /// usable store rather than propagating the poisoned handle.
    #[tokio::test]
    async fn marker_write_failure_does_not_break_store_init() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");
        bloat_database(&db_path, 192, 1024 * 1024, 16);

        // A normal open compacts and records the marker; the store must be
        // usable afterwards. This is the control for the classifier test above,
        // which covers the failure branch directly (a real backend fault cannot
        // be injected through `ReDb::new`, which owns its file backend).
        clear_last_reclaim();
        let store = ReDb::new(temp_dir.path()).await;
        assert!(
            store.is_ok(),
            "store init must succeed through the marker write"
        );
        drop(store);
        assert_eq!(last_reclaim(), Some(ReclaimOutcome::Compacted));
    }

    /// Pin the exact `COMPACTION_REGROWTH_FACTOR` boundary. Without this the
    /// constant survives mutation: the sibling tests sit far either side of it.
    #[test]
    fn compaction_marker_regrowth_boundary() {
        // The decision the marker path makes is
        //   file_bytes <= settled_bytes * COMPACTION_REGROWTH_FACTOR  -> skip
        let settled: u64 = 400 * 1024 * 1024;
        let threshold = (settled as f64) * COMPACTION_REGROWTH_FACTOR;
        let skips = |file: u64| (file as f64) <= threshold;

        // 1.25 x 400 MiB = exactly 500 MiB.
        assert!(
            skips(500 * 1024 * 1024),
            "exactly at the threshold must skip"
        );
        assert!(
            !skips(500 * 1024 * 1024 + 1),
            "one byte past the threshold must re-evaluate"
        );
        // Well inside: a settled database at its own size.
        assert!(skips(settled));
        // Well past: genuine regrowth.
        assert!(!skips(settled * 2));
    }

    /// The marker must not wedge compaction shut: once the file grows well past
    /// where it settled, a fresh compaction is allowed again.
    #[tokio::test]
    async fn compaction_resumes_after_the_file_grows_past_the_marker() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");
        bloat_database(&db_path, 192, 1024 * 1024, 16);

        clear_last_reclaim();
        let store = ReDb::new(temp_dir.path()).await.unwrap();
        drop(store);
        assert_eq!(last_reclaim(), Some(ReclaimOutcome::Compacted));

        // Grow well past the marker, then create fresh interior dead space.
        {
            let db = Database::create(&db_path).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut t = txn.open_table(STATE_TABLE).unwrap();
                let v = vec![0x5Au8; 1024 * 1024];
                for i in 1000..1400usize {
                    t.insert(&i.to_be_bytes()[..], v.as_slice()).unwrap();
                }
            }
            txn.commit().unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut t = txn.open_table(STATE_TABLE).unwrap();
                for i in 1000..1400usize {
                    if i % 16 != 0 {
                        t.remove(&i.to_be_bytes()[..]).unwrap();
                    }
                }
            }
            txn.commit().unwrap();
            drop(db);
        }

        clear_last_reclaim();
        let store = ReDb::new(temp_dir.path()).await.unwrap();
        drop(store);
        assert_eq!(
            last_reclaim(),
            Some(ReclaimOutcome::Compacted),
            "a database that grew past its marker must be compactable again; got {:?}",
            last_reclaim()
        );
    }

    /// The gate must leave a healthy database alone, so a restart loop never
    /// rewrites a multi-GB file for a trivial gain.
    #[tokio::test]
    async fn startup_compaction_skips_database_without_meaningful_dead_space() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");

        // Aim for the discriminating case: enough dead space that the FRACTION
        // gate passes, but under the 64 MiB absolute floor. The precondition is
        // asserted below rather than assumed, because redb's doubling makes the
        // resulting file size awkward to predict.
        let live_rows = bloat_database(&db_path, 60, 1024 * 1024, 2);
        let expected = state_contents(&db_path);
        assert_eq!(expected.len(), live_rows);

        let before = std::fs::metadata(&db_path).unwrap().len();
        let in_use = {
            let db = Database::create(&db_path).unwrap();
            let n = ReDb::pages_in_use_bytes(&db, &db_path).unwrap();
            drop(db);
            n
        };
        let reclaimable = before.saturating_sub(in_use);

        // Precondition: only the floor may be what declines here, otherwise the
        // test would pass for the wrong reason (nothing to reclaim at all).
        assert!(
            (reclaimable as f64) >= (before as f64) * MIN_COMPACTION_RECLAIM_FRACTION,
            "fixture must clear the fraction gate so the FLOOR is what declines; \
             file={before} in_use={in_use} reclaimable={reclaimable}"
        );
        assert!(
            reclaimable < MIN_COMPACTION_RECLAIM_BYTES,
            "fixture must sit below the floor; file={before} reclaimable={reclaimable}"
        );

        clear_last_reclaim();
        let store = ReDb::new(temp_dir.path()).await.unwrap();
        drop(store);

        assert_eq!(
            last_reclaim(),
            Some(ReclaimOutcome::NotWorthwhile),
            "the absolute floor should be what declines here"
        );
        let after = std::fs::metadata(&db_path).unwrap().len();
        assert!(
            after >= before,
            "a declined compaction must not shrink the file: {before} -> {after}"
        );
        assert_eq!(state_contents(&db_path), expected);
    }

    /// A healthy, never-deleted database must NOT be compacted. redb grows by
    /// doubling, so a freshly-grown file has ~50% never-allocated slack that
    /// `allocated_pages` reports as free — if the gate keys on that, every
    /// healthy node rewrites its whole database on every restart.
    #[tokio::test]
    async fn startup_compaction_skips_large_healthy_database() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("db");

        // Nothing deleted: zero dead pages, only growth slack. ~386 MiB file.
        let live_rows = bloat_database(&db_path, 192, 1024 * 1024, 1);
        let before = std::fs::metadata(&db_path).unwrap().len();

        clear_last_reclaim();
        let store = ReDb::new(temp_dir.path()).await.unwrap();
        drop(store);

        // Assert the DECISION, not the size: compacting a healthy database
        // barely changes its length, so a size assertion cannot tell "declined"
        // from "compacted and reclaimed almost nothing".
        assert_ne!(
            last_reclaim(),
            Some(ReclaimOutcome::Compacted),
            "a healthy database must not be compacted; got {:?}",
            last_reclaim()
        );
        let after = std::fs::metadata(&db_path).unwrap().len();
        assert!(
            after <= before,
            "a healthy database must not grow: {before} -> {after}"
        );
        assert_eq!(state_contents(&db_path).len(), live_rows);
    }

    #[test]
    fn compaction_gate_requires_both_floor_and_fraction() {
        const MIB: u64 = 1024 * 1024;

        // Huge absolute reclaim but a trivial share of the file: skip.
        assert!(!compaction_is_worthwhile(10_000 * MIB, 9_000 * MIB));
        // Huge share but below the absolute floor: skip.
        assert!(!compaction_is_worthwhile(60 * MIB, 0));
        // Both satisfied: compact. This is the production shape (84% dead).
        assert!(compaction_is_worthwhile(2_680 * MIB, 430 * MIB));

        // Boundaries. The pair below sits where BOTH gates land simultaneously,
        // so on its own it cannot attribute a decision to either constant.
        assert!(compaction_is_worthwhile(256 * MIB, 192 * MIB));
        assert!(!compaction_is_worthwhile(256 * MIB, 193 * MIB));

        // Isolate the FLOOR: fraction comfortably satisfied (~91%), so only the
        // 64 MiB floor can decide. Pins MIN_COMPACTION_RECLAIM_BYTES.
        assert!(compaction_is_worthwhile(70 * MIB, 6 * MIB)); // exactly 64 MiB
        assert!(!compaction_is_worthwhile(70 * MIB, 7 * MIB)); // 63 MiB

        // Isolate the FRACTION: floor comfortably satisfied (250 MiB), so only
        // the 25% share can decide. Pins MIN_COMPACTION_RECLAIM_FRACTION.
        assert!(compaction_is_worthwhile(1000 * MIB, 750 * MIB)); // exactly 25%
        assert!(!compaction_is_worthwhile(1000 * MIB, 751 * MIB));

        // Degenerate inputs must not divide by zero or panic.
        assert!(!compaction_is_worthwhile(0, 0));
        assert!(
            !compaction_is_worthwhile(100, 500),
            "in_use > file saturates"
        );
    }

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

    /// #4117 H1: the delegate-origin record is FIRST-WRITER-WINS — the first
    /// write wins and returns `true`, a later write is a no-op returning `false`,
    /// and the read observes the ORIGINAL (a racing loser sees the winner's
    /// record). A `None` first-writer records the Admin/None class.
    #[tokio::test]
    async fn delegate_origin_first_writer_wins() {
        let temp_dir = TempDir::new().unwrap();
        let db = ReDb::new(temp_dir.path()).await.unwrap();
        let d = fake_delegate_key(0x33, 0x44);
        assert!(db.get_delegate_origins(&d).unwrap().is_none());

        let a = [0xA1u8; 32];
        assert!(db.record_delegate_origin_first_writer(&d, Some(a)).unwrap());
        // A later, different origin is a no-op (loser).
        let b = [0xB2u8; 32];
        assert!(!db.record_delegate_origin_first_writer(&d, Some(b)).unwrap());
        let (has_none, origins) = db.get_delegate_origins(&d).unwrap().unwrap();
        assert!(!has_none);
        assert_eq!(origins, vec![a], "the read observes only the first origin");

        // A None first-writer records the Admin/None class (never privileged).
        let d2 = fake_delegate_key(0x55, 0x66);
        assert!(db.record_delegate_origin_first_writer(&d2, None).unwrap());
        assert!(
            !db.record_delegate_origin_first_writer(&d2, Some(a))
                .unwrap()
        );
        let (has_none2, origins2) = db.get_delegate_origins(&d2).unwrap().unwrap();
        assert!(has_none2);
        assert!(origins2.is_empty());
    }

    /// #4117 H1 (persistence-succeeds-before-usable): a durable-write failure in
    /// `record_delegate_origin_first_writer` SURFACES as `Err` — it is never
    /// swallowed into a silent `Ok`. This is the storage-layer foundation of the
    /// rule that a failed origin record must ABORT the whole registration (a
    /// registered-but-recordless delegate has a claimable first-writer slot).
    /// Uses the fault-injecting backend to produce a REAL redb I/O failure.
    #[test]
    fn record_delegate_origin_first_writer_surfaces_backend_failure() {
        let backend = FailingBackend::new();
        let db = open_redb_with_backend(backend.clone());
        let d = fake_delegate_key(0x12, 0x34);

        // Healthy: the first write succeeds and is observable.
        assert!(
            db.record_delegate_origin_first_writer(&d, Some([0x11u8; 32]))
                .unwrap(),
            "healthy first write must succeed and return `true`"
        );

        // Disk fails: a subsequent origin write MUST return Err, never a silent Ok.
        backend.start_failing();
        let d2 = fake_delegate_key(0x56, 0x78);
        assert!(
            db.record_delegate_origin_first_writer(&d2, Some([0x22u8; 32]))
                .is_err(),
            "a durable-write failure must surface as Err, never a silent Ok"
        );
    }

    /// #4117 P2a: the reserved-marker-hash table is individually keyed,
    /// idempotent, per-delegate isolated, and per-delegate CAPPED.
    #[tokio::test]
    async fn reserved_marker_hashes_capped_and_isolated() {
        let temp_dir = TempDir::new().unwrap();
        let db = ReDb::new(temp_dir.path()).await.unwrap();
        let d = fake_delegate_key(0x77, 0x88);
        let other = fake_delegate_key(0x99, 0xAA);

        let h1 = [1u8; 32];
        let h2 = [2u8; 32];
        db.add_reserved_marker_hash(&d, &h1).unwrap();
        db.add_reserved_marker_hash(&d, &h1).unwrap(); // idempotent
        db.add_reserved_marker_hash(&d, &h2).unwrap();
        db.add_reserved_marker_hash(&other, &[9u8; 32]).unwrap();

        let mut got = db.get_reserved_marker_hashes(&d).unwrap();
        got.sort();
        assert_eq!(got, vec![h1, h2]);
        assert_eq!(
            db.get_reserved_marker_hashes(&other).unwrap(),
            vec![[9u8; 32]],
            "reserved hashes are per-delegate isolated"
        );

        // Cap: adding well past the per-delegate cap never exceeds it.
        let cap = ReDb::MAX_RESERVED_MARKER_HASHES_PER_DELEGATE;
        for i in 0..(cap as u32 + 10) {
            let mut h = [0u8; 32];
            h[..4].copy_from_slice(&i.to_le_bytes());
            db.add_reserved_marker_hash(&d, &h).unwrap();
        }
        assert_eq!(
            db.get_reserved_marker_hashes(&d).unwrap().len(),
            cap,
            "per-delegate reserved-hash count is bounded at the cap"
        );
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
    pub(crate) struct FailingBackend {
        inner: Arc<redb::backends::InMemoryBackend>,
        fail: Arc<std::sync::atomic::AtomicBool>,
    }

    impl FailingBackend {
        pub(crate) fn new() -> Self {
            Self {
                inner: Arc::new(redb::backends::InMemoryBackend::new()),
                fail: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            }
        }

        /// Make every subsequent I/O call fail, simulating a disk EIO / csum failure.
        pub(crate) fn start_failing(&self) {
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
    pub(crate) fn open_redb_with_backend<B: redb::StorageBackend>(backend: B) -> ReDb {
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
