use super::ContractError;

#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::{Pool as SqlitePool, SQLiteContractHandler, SqlDbError};

#[cfg(feature = "sqlite")]
pub type Storage = SqlitePool;
#[cfg(feature = "sqlite")]
pub type StorageContractHandler<R> = SQLiteContractHandler<R>;
#[cfg(feature = "sqlite")]
pub type StorageDbError = SqlDbError;

#[cfg(feature = "sqlite")]
impl From<SqlDbError> for ContractError {
    fn from(err: SqlDbError) -> Self {
        Self::StorageError(err.into())
    }
}

#[cfg(feature = "rocks_db")]
pub mod rocks_db;
#[cfg(all(feature = "rocks_db", not(feature = "sqlite")))]
use self::rocks_db::{RocksDb, RocksDbContractHandler, RocksDbError};

#[cfg(all(feature = "rocks_db", not(feature = "sqlite")))]
pub type Storage = RocksDb;
#[cfg(all(feature = "rocks_db", not(feature = "sqlite")))]
pub type StorageContractHandler<R> = RocksDbContractHandler<R>;
#[cfg(all(feature = "rocks_db", not(feature = "sqlite")))]
pub type StorageDbError = RocksDbError;

#[cfg(feature = "rocks_db")]
impl From<rocks_db::RocksDbError> for ContractError {
    fn from(err: rocks_db::RocksDbError) -> Self {
        Self::StorageError(err.into())
    }
}
