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
impl From<SqlDbError> for ContractError<SqlDbError> {
    fn from(err: SqlDbError) -> Self {
        Self::StorageError(err)
    }
}

#[cfg(feature = "rocks_db")]
pub mod rocks_db;
#[cfg(feature = "rocks_db")]
use self::rocks_db::{RocksDb, RocksDbContractHandler, RocksDbError};

#[cfg(all(feature = "rocks_db", not(feature = "sqlite")))]
pub type Storage = RocksDb;
#[cfg(all(feature = "rocks_db", not(feature = "sqlite")))]
pub type StorageContractHandler<R> = RocksDbContractHandler<R>;
#[cfg(all(feature = "rocks_db", not(feature = "sqlite")))]
pub type StorageDbError = RocksDbError;

#[cfg(feature = "rocks_db")]
impl From<RocksDbError> for ContractError<RocksDbError> {
    fn from(err: RocksDbError) -> Self {
        Self::StorageError(err)
    }
}
