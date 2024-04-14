/// State storage implementation based on the `sqlite`
#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(all(feature = "sqlite", not(feature = "redb")))]
pub use sqlite::Pool as SqlitePool;

#[cfg(all(feature = "sqlite", not(feature = "redb")))]
pub type Storage = SqlitePool;

/// State storage implementation based on the [`rocksdb`]
#[cfg(feature = "rocksdb")]
pub mod rocksdb;
#[cfg(all(feature = "rocksdb", not(any(feature = "sqlite", feature = "redb"))))]
use self::rocksdb::RocksDb;
#[cfg(all(feature = "rocksdb", not(any(feature = "sqlite", feature = "redb"))))]
pub type Storage = RocksDb;

/// State storage implementation based on the [`redb`]
#[cfg(feature = "redb")]
pub mod redb;
#[cfg(feature = "redb")]
use self::redb::ReDb;

#[cfg(feature = "redb")]
pub type Storage = ReDb;
