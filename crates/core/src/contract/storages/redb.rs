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
        tracing::info!("loading contract store from {db_path:?}");

        match Database::create(&db_path) {
            Ok(db) => Self::initialize_database(db),
            Err(e) if Self::is_version_mismatch(&e) => {
                tracing::warn!(
                    "Database format mismatch detected: {e}\n\
                     Automatically migrating incompatible database at {db_path:?}"
                );

                // Attempt to back up the old database
                Self::backup_and_remove_database(&db_path)?;

                // Retry with fresh database
                tracing::info!("Creating new database at {db_path:?}");
                let db = Database::create(&db_path)?;
                Self::initialize_database(db)
            }
            Err(e) => {
                tracing::error!("failed to load contract store: {e}");
                Err(e.into())
            }
        }
    }

    fn initialize_database(db: Database) -> Result<Self, redb::Error> {
        let db = Self(db);
        let txn = db.0.begin_write()?;
        {
            txn.open_table(STATE_TABLE).map_err(|e| {
                tracing::error!(error = %e, "failed to open STATE_TABLE");
                e
            })?;

            txn.open_table(CONTRACT_PARAMS_TABLE).map_err(|e| {
                tracing::error!(error = %e, "failed to open CONTRACT_PARAMS_TABLE");
                e
            })?;
        }
        txn.commit()?;
        Ok(db)
    }

    fn is_version_mismatch(error: &DatabaseError) -> bool {
        let msg = error.to_string();
        msg.contains("Manual upgrade required")
            || msg.contains("file format version")
            || msg.contains("Expected file format version")
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
                    "Old database backed up to {backup_path:?}\n\
                     You can safely delete this backup after verifying the new database works correctly."
                );
                Ok(())
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                // Database doesn't exist, nothing to backup
                tracing::debug!("No existing database to backup");
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to backup old database: {e}\n\
                     Attempting to remove it directly..."
                );

                // If backup fails, try to remove directly
                std::fs::remove_file(db_path).map_err(|remove_err| {
                    tracing::error!(
                        "Failed to remove incompatible database at {db_path:?}: {remove_err}"
                    );
                    redb::Error::Io(remove_err)
                })?;

                tracing::info!("Removed incompatible database (backup failed)");
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
