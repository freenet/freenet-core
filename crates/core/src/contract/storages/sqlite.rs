use std::{path::Path, str::FromStr};

use freenet_stdlib::prelude::*;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteRow},
    ConnectOptions, Row, SqlitePool,
};

use crate::wasm_runtime::{ContractError, StateStorage, StateStoreError};

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

#[derive(Clone)]
pub struct Pool(SqlitePool);

impl Pool {
    #[cfg_attr(feature = "redb", allow(unused))]
    pub async fn new(db_dir: Option<&Path>) -> Result<Self, SqlDbError> {
        let opts = if let Some(db_dir) = db_dir {
            let file = db_dir.join("freenet.db");
            tracing::info!("loading contract store from {file:?}");
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
        Ok(Self(pool.clone()))
    }
}

impl StateStorage for Pool {
    type Error = SqlDbError;

    async fn store(&mut self, key: ContractKey, state: WrappedState) -> Result<(), Self::Error> {
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
        &mut self,
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
