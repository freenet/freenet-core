#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::{Pool as SqlitePool, SqlDbError};

#[cfg(feature = "sqlite")]
pub type Storage = SqlitePool;

#[cfg(feature = "rocks_db")]
pub mod rocks_db;
#[cfg(all(feature = "rocks_db", not(feature = "sqlite")))]
use self::rocks_db::RocksDb;

#[cfg(all(feature = "rocks_db", not(feature = "sqlite")))]
pub type Storage = RocksDb;
