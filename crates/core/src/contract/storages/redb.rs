use std::path::Path;

use freenet_stdlib::prelude::*;
use redb::{Database, DatabaseError, ReadableDatabase, TableDefinition};

use crate::wasm_runtime::StateStorage;

const CONTRACT_PARAMS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("contract_params");
const STATE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("state");

pub struct ReDb(Database);

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
        let db = Self(db);
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
}

impl StateStorage for ReDb {
    type Error = redb::Error;

    async fn store(&mut self, key: ContractKey, state: WrappedState) -> Result<(), Self::Error> {
        let txn = self.0.begin_write()?;

        {
            let mut tbl = txn.open_table(STATE_TABLE)?;
            tbl.insert(key.as_bytes(), state.as_ref())?;
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
        &mut self,
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
