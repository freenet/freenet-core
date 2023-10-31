use notify::Watcher;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::Write;
use std::path::Path;
use std::{
    fs::{self, File},
    io::Read,
    thread,
    time::Duration,
};

use super::{error::RuntimeInnerError, RuntimeResult};
use crate::DynError;

pub(crate) trait StoreEntriesContainer: Serialize + DeserializeOwned + Default {
    type MemContainer: Send + Sync + 'static;
    type Key;
    type Value;

    fn update(self, container: &mut Self::MemContainer);
    fn replace(container: &Self::MemContainer) -> Self;
    fn insert(container: &mut Self::MemContainer, key: Self::Key, value: Self::Value);
}

pub(crate) trait StoreFsManagement<C>
where
    C: StoreEntriesContainer,
{
    fn watch_changes(
        mut container: C::MemContainer,
        key_file_path: &Path,
        lock_file_path: &Path,
    ) -> Result<(), DynError> {
        let key_path = key_file_path.to_path_buf();
        let lock_path = lock_file_path.to_path_buf();
        let mut watcher = notify::recommended_watcher(
            move |res: Result<notify::Event, notify::Error>| match res {
                Ok(ev) => {
                    if let notify::EventKind::Modify(notify::event::ModifyKind::Data(_)) = ev.kind {
                        match Self::load_from_file(key_path.as_path(), lock_path.as_path()) {
                            Err(err) => tracing::error!("{err}"),
                            Ok(map) => {
                                map.update(&mut container);
                            }
                        }
                    }
                }
                Err(err) => tracing::error!("{err}"),
            },
        )?;
        watcher.watch(key_file_path, notify::RecursiveMode::NonRecursive)?;
        Ok(())
    }

    fn update(
        mem_containter: &mut C::MemContainer,
        key: C::Key,
        value: C::Value,
        key_file_path: &Path,
        lock_file_path: &Path,
    ) -> RuntimeResult<()> {
        Self::acquire_ls_lock(lock_file_path)?;
        C::insert(mem_containter, key, value);
        let container = C::replace(mem_containter);
        let serialized = bincode::serialize(&container).map_err(|e| RuntimeInnerError::Any(e))?;
        // FIXME: make this more reliable, append to the file instead of truncating it
        let mut f = File::create(key_file_path)?;
        f.write_all(&serialized)?;
        Self::release_ls_lock(lock_file_path)?;
        Ok(())
    }

    fn load_from_file(key_file_path: &Path, lock_file_path: &Path) -> RuntimeResult<C> {
        let mut buf = vec![];
        Self::acquire_ls_lock(lock_file_path)?;
        let mut f = File::open(key_file_path)?;
        f.read_to_end(&mut buf)?;
        Self::release_ls_lock(lock_file_path)?;
        let value = if buf.is_empty() {
            C::default()
        } else {
            bincode::deserialize(&buf).map_err(|e| RuntimeInnerError::Any(e))?
        };
        Ok(value)
    }

    fn acquire_ls_lock(lock_file_path: &Path) -> RuntimeResult<()> {
        while lock_file_path.exists() {
            thread::sleep(Duration::from_micros(5));
        }
        File::create(lock_file_path)?;
        Ok(())
    }

    fn release_ls_lock(lock_file_path: &Path) -> RuntimeResult<()> {
        match fs::remove_file(lock_file_path) {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(other) => Err(other.into()),
        }
    }
}
