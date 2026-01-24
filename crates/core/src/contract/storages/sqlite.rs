use std::{path::Path, str::FromStr};

use freenet_stdlib::prelude::*;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteRow},
    ConnectOptions, Row, SqlitePool,
};

use crate::wasm_runtime::{ContractError, StateStorage, StateStoreError};

/// Metadata about a hosted contract, persisted to survive restarts.
/// (Duplicated from redb.rs for sqlite feature-only builds)
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
}

async fn create_contracts_table(pool: &SqlitePool) -> Result<(), SqlDbError> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS states (
            contract        BLOB PRIMARY KEY,
                    state           BLOB,
                    params          BLOB
                )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_hosting_metadata_table(pool: &SqlitePool) -> Result<(), SqlDbError> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS hosting_metadata (
            contract        BLOB PRIMARY KEY,
            last_access_ms  INTEGER NOT NULL,
            access_type     INTEGER NOT NULL,
            size_bytes      INTEGER NOT NULL,
            code_hash       BLOB NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Clone)]
pub struct Pool(SqlitePool);

impl Pool {
    #[cfg_attr(feature = "redb", allow(unused))]
    pub async fn new(db_dir: Option<&Path>) -> Result<Self, SqlDbError> {
        let opts = if let Some(db_dir) = db_dir {
            let file = db_dir.join("freenet.db");
            tracing::info!(
                db_file = ?file,
                phase = "store_init",
                "Loading contract store"
            );
            SqliteConnectOptions::new()
                .create_if_missing(true)
                .filename(file)
        } else {
            SqliteConnectOptions::from_str("sqlite::memory:").unwrap()
        };
        let opts = opts.log_statements(tracing::log::LevelFilter::Debug);
        let pool = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async move { SqlitePool::connect_with(opts).await })
        })
        .unwrap();
        create_contracts_table(&pool).await?;
        create_hosting_metadata_table(&pool).await?;
        Ok(Self(pool.clone()))
    }

    // ==================== Hosting Metadata Methods ====================

    /// Store hosting metadata for a contract.
    pub async fn store_hosting_metadata(
        &self,
        key: &ContractKey,
        metadata: HostingMetadata,
    ) -> Result<(), SqlDbError> {
        sqlx::query(
            "INSERT INTO hosting_metadata (contract, last_access_ms, access_type, size_bytes, code_hash)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT(contract) DO UPDATE SET
                last_access_ms = excluded.last_access_ms,
                access_type = excluded.access_type,
                size_bytes = excluded.size_bytes,
                code_hash = excluded.code_hash",
        )
        .bind(key.as_bytes())
        .bind(metadata.last_access_ms as i64)
        .bind(metadata.access_type as i32)
        .bind(metadata.size_bytes as i64)
        .bind(metadata.code_hash.as_slice())
        .execute(&self.0)
        .await?;
        Ok(())
    }

    /// Get hosting metadata for a contract.
    pub async fn get_hosting_metadata(
        &self,
        key: &ContractKey,
    ) -> Result<Option<HostingMetadata>, SqlDbError> {
        match sqlx::query(
            "SELECT last_access_ms, access_type, size_bytes, code_hash FROM hosting_metadata WHERE contract = ?",
        )
        .bind(key.as_bytes())
        .map(|row: SqliteRow| {
            let code_hash_vec: Vec<u8> = row.get("code_hash");
            let mut code_hash = [0u8; 32];
            if code_hash_vec.len() >= 32 {
                code_hash.copy_from_slice(&code_hash_vec[..32]);
            }
            HostingMetadata::new(
                row.get::<i64, _>("last_access_ms") as u64,
                row.get::<i32, _>("access_type") as u8,
                row.get::<i64, _>("size_bytes") as u64,
                code_hash,
            )
        })
        .fetch_one(&self.0)
        .await
        {
            Ok(metadata) => Ok(Some(metadata)),
            Err(sqlx::Error::RowNotFound) => Ok(None),
            Err(e) => Err(SqlDbError::SqliteError(e)),
        }
    }

    /// Remove hosting metadata for a contract.
    pub async fn remove_hosting_metadata(&self, key: &ContractKey) -> Result<(), SqlDbError> {
        sqlx::query("DELETE FROM hosting_metadata WHERE contract = ?")
            .bind(key.as_bytes())
            .execute(&self.0)
            .await?;
        Ok(())
    }

    /// Load all hosting metadata from the database.
    pub async fn load_all_hosting_metadata(
        &self,
    ) -> Result<Vec<(Vec<u8>, HostingMetadata)>, SqlDbError> {
        let rows = sqlx::query(
            "SELECT contract, last_access_ms, access_type, size_bytes, code_hash FROM hosting_metadata",
        )
        .map(|row: SqliteRow| {
            let contract: Vec<u8> = row.get("contract");
            let code_hash_vec: Vec<u8> = row.get("code_hash");
            let mut code_hash = [0u8; 32];
            if code_hash_vec.len() >= 32 {
                code_hash.copy_from_slice(&code_hash_vec[..32]);
            }
            let metadata = HostingMetadata::new(
                row.get::<i64, _>("last_access_ms") as u64,
                row.get::<i32, _>("access_type") as u8,
                row.get::<i64, _>("size_bytes") as u64,
                code_hash,
            );
            (contract, metadata)
        })
        .fetch_all(&self.0)
        .await?;
        Ok(rows)
    }

    /// Get the size of a contract's state.
    pub async fn get_state_size(&self, key: &ContractKey) -> Result<Option<u64>, SqlDbError> {
        match sqlx::query("SELECT LENGTH(state) as size FROM states WHERE contract = ?")
            .bind(key.as_bytes())
            .map(|row: SqliteRow| row.get::<i64, _>("size") as u64)
            .fetch_one(&self.0)
            .await
        {
            Ok(size) => Ok(Some(size)),
            Err(sqlx::Error::RowNotFound) => Ok(None),
            Err(e) => Err(SqlDbError::SqliteError(e)),
        }
    }

    /// Iterate all contract keys that have stored state.
    pub async fn iter_all_state_keys(&self) -> Result<Vec<Vec<u8>>, SqlDbError> {
        let rows = sqlx::query("SELECT contract FROM states")
            .map(|row: SqliteRow| row.get::<Vec<u8>, _>("contract"))
            .fetch_all(&self.0)
            .await?;
        Ok(rows)
    }
}

impl StateStorage for Pool {
    type Error = SqlDbError;

    async fn store(&self, key: ContractKey, state: WrappedState) -> Result<(), Self::Error> {
        let state_size = state.size() as u64;

        sqlx::query(
            "INSERT INTO states (contract, state)
                     VALUES ($1, $2)
                     ON CONFLICT(contract) DO UPDATE SET state = excluded.state
                     ",
        )
        .bind(key.as_bytes())
        .bind(state.as_ref())
        .execute(&self.0)
        .await?;

        // Also update hosting metadata to track this contract
        // This ensures the contract is reloaded into hosting cache on restart
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        // Default to PUT access type (1) since we're storing state
        // Store the code hash so we can reconstruct ContractKey on load
        let code_hash: [u8; 32] = **key.code_hash();
        let metadata = HostingMetadata::new(now_ms, 1, state_size, code_hash);
        self.store_hosting_metadata(&key, metadata).await?;

        Ok(())
    }

    async fn get(&self, key: &ContractKey) -> Result<Option<WrappedState>, Self::Error> {
        match sqlx::query("SELECT state FROM states WHERE contract = ?")
            .bind(key.as_bytes())
            .map(|row: SqliteRow| Some(WrappedState::new(row.get("state"))))
            .fetch_one(&self.0)
            .await
        {
            Ok(result) => Ok(result),
            Err(sqlx::Error::RowNotFound) => Ok(None),
            Err(_) => Err(SqlDbError::ContractNotFound),
        }
    }

    async fn store_params(
        &self,
        key: ContractKey,
        params: Parameters<'static>,
    ) -> Result<(), Self::Error> {
        sqlx::query(
            "INSERT OR REPLACE INTO states (contract, params)
                     VALUES ($1, $2)
                     ON CONFLICT(contract) DO UPDATE SET params = excluded.params
                     ",
        )
        .bind(key.as_bytes())
        .bind(params.as_ref())
        .execute(&self.0)
        .await?;
        Ok(())
    }

    async fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> Result<Option<Parameters<'static>>, Self::Error> {
        match sqlx::query("SELECT params FROM states WHERE contract = ?")
            .bind(key.as_bytes())
            .map(|row: SqliteRow| Some(Parameters::from(row.get::<Vec<u8>, _>("params"))))
            .fetch_one(&self.0)
            .await
        {
            Ok(result) => Ok(result),
            Err(sqlx::Error::RowNotFound) => Ok(None),
            Err(_) => Err(SqlDbError::ContractNotFound),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SqlDbError {
    #[error("Contract not found")]
    ContractNotFound,
    #[error(transparent)]
    SqliteError(#[from] sqlx::Error),
    #[error(transparent)]
    RuntimeError(#[from] ContractError),
    #[error("{0}")]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    StateStore(#[from] StateStoreError),
}
