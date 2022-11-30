use notify::Watcher;
use serde::de::DeserializeOwned;
use std::path::Path;
use std::{
    fs::{self, File},
    io::Read,
    thread,
    time::Duration,
};

use crate::{error::RuntimeInnerError, DynError, RuntimeResult};

pub(crate) trait StoreContainer: DeserializeOwned + Default {
    type Container: Clone + Send + Sync + 'static;
    fn update(self, container: &Self::Container);
}

pub(crate) trait StoreManagement<C>
where
    C: StoreContainer,
{
    fn watch_changes(
        container: &C::Container,
        key_file_path: &Path,
        lock_file_path: &Path,
    ) -> Result<(), DynError> {
        let key_path = key_file_path.to_path_buf();
        let lock_path = lock_file_path.to_path_buf();
        let container_2 = container.clone();
        let mut watcher = notify::recommended_watcher(
            move |res: Result<notify::Event, notify::Error>| match res {
                Ok(ev) => {
                    if let notify::EventKind::Modify(notify::event::ModifyKind::Data(_)) = ev.kind {
                        match Self::load_from_file(key_path.as_path(), lock_path.as_path()) {
                            Err(err) => tracing::error!("{err}"),
                            Ok(map) => {
                                map.update(&container_2);
                            }
                        }
                    }
                }
                Err(e) => tracing::error!("{e}"),
            },
        )?;
        watcher.watch(key_file_path, notify::RecursiveMode::NonRecursive)?;
        Ok(())
    }

    fn load_from_file(key_file_path: &Path, lock_file_path: &Path) -> RuntimeResult<C> {
        let mut buf = vec![];
        Self::acquire_ls_lock(key_file_path)?;
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
