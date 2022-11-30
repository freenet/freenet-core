use dashmap::DashMap;
use notify::Watcher;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::{Display};
use std::hash::Hash;
use std::{
    fs::{self, File},
    io::Read,
    path::PathBuf,
    sync::Arc,
    thread,
    time::Duration,
};

use crate::{DynError, error::RuntimeInnerError, RuntimeResult};

pub trait StoreManagement {
    fn watch_changes<K, V, I>(
        ori_map: Arc<DashMap<K, V>>,
        key_file_path: &once_cell::sync::OnceCell<PathBuf>,
        lock_file_path: &once_cell::sync::OnceCell<PathBuf>,
    ) -> Result<(), DynError>
    where
        K: Hash + Eq,
        I: IntoIterator + DeserializeOwned + Default
    {
        let mut watcher = notify::recommended_watcher(
            move |res: Result<notify::Event, notify::Error>| match res {
                Ok(ev) => {
                    if let notify::EventKind::Modify(notify::event::ModifyKind::Data(_)) = ev.kind {
                        match Self::load_from_file::<I>(key_file_path, lock_file_path) {
                            Err(err) => tracing::error!("{err}"),
                            Ok(map) => {
                                for (k, v) in map.0 {
                                    ori_map.insert(k, v);
                                }
                            }
                        }
                    }
                }
                Err(e) => tracing::error!("{e}"),
            },
        )?;

        watcher.watch(
            key_file_path.get().unwrap(),
            notify::RecursiveMode::NonRecursive,
        )?;
        Ok(())
    }

    fn load_from_file<T>(
        key_file_path: &once_cell::sync::OnceCell<PathBuf>,
        lock_file_path: &once_cell::sync::OnceCell<PathBuf>,
    ) -> RuntimeResult<T>
    where
        T: DeserializeOwned + Default,
    {
        let mut buf = vec![];
        Self::acquire_ls_lock(key_file_path)?;
        let mut f = File::open(key_file_path.get().unwrap())?;
        f.read_to_end(&mut buf)?;
        Self::release_ls_lock(lock_file_path)?;
        let value = if buf.is_empty() {
            T::default()
        } else {
            bincode::deserialize(&buf).map_err(|e| RuntimeInnerError::Any(e))?
        };
        Ok(value)
    }

    fn acquire_ls_lock(
        lock_file_path: &once_cell::sync::OnceCell<PathBuf>,
    ) -> RuntimeResult<()> {
        let lock = lock_file_path.get().unwrap();
        while lock.exists() {
            thread::sleep(Duration::from_micros(5));
        }
        File::create(lock)?;
        Ok(())
    }

    fn release_ls_lock(
        lock_file_path: &once_cell::sync::OnceCell<PathBuf>,
    ) -> RuntimeResult<()> {
        match fs::remove_file(lock_file_path.get().unwrap()) {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(other) => Err(other.into()),
        }
    }
}
