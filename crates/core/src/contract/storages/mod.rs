/// State storage implementation based on the `sqlite`
#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(all(feature = "sqlite", not(feature = "redb")))]
pub use sqlite::Pool as SqlitePool;

#[cfg(all(feature = "sqlite", not(feature = "redb")))]
pub type Storage = SqlitePool;

/// State storage implementation based on the [`redb`]
#[cfg(feature = "redb")]
pub mod redb;
#[cfg(feature = "redb")]
use self::redb::ReDb;

#[cfg(feature = "redb")]
pub type Storage = ReDb;
