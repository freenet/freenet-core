use dashmap::DashMap;
use notify::Watcher;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::{
    fs::{self, File},
    io::Read,
    iter::FromIterator,
    path::PathBuf,
    sync::Arc,
    thread,
    time::Duration,
};

use crate::{ContractError, DynError, RuntimeInnerError, RuntimeResult};

trait Store {
    fn get_path<const N: usize, T>(
        &mut self,
        base_dir: PathBuf,
        key_hash: T,
    ) -> RuntimeResult<PathBuf>
    where
        T: Clone + Display,
    {
        let contract_hash: [u8; N] = match self.get_key_hash(key_hash) {
            Some(k) => k,
            None => self.code_hash_from_key(key_hash.clone()).ok_or_else(|| {
                tracing::warn!("trying to store partially unspecified contract `{key_hash}`");
                RuntimeInnerError::UnwrapContract
            })?,
        };

        let key_path = bs58::encode(contract_hash)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
            .to_lowercase();
        Ok(base_dir.join(key_path).with_extension("wasm"))
    }

    fn get_key_hash<const N: usize, T>(&self, key: T) -> Option<[u8; N]>;

    fn code_hash_from_key<T, R>(&self, key: T) -> Option<R>;

    fn watch_changes<K, V, I>(
        &self,
        ori_map: Arc<DashMap<K, V>>,
        key_file_path: once_cell::sync::OnceCell<PathBuf>,
        lock_file_path: once_cell::sync::OnceCell<PathBuf>,
    ) -> Result<(), DynError>
    where
        K: Hash + Eq,
        I: IntoIterator<Item = (K, V)> + DeserializeOwned + Default,
    {
        let mut watcher = notify::recommended_watcher(
            move |res: Result<notify::Event, notify::Error>| match res {
                Ok(ev) => {
                    if let notify::EventKind::Modify(notify::event::ModifyKind::Data(_)) = ev.kind {
                        match Self::load_from_file::<I>(&self, key_file_path, lock_file_path) {
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
        &self,
        key_file_path: once_cell::sync::OnceCell<PathBuf>,
        lock_file_path: once_cell::sync::OnceCell<PathBuf>,
    ) -> RuntimeResult<T>
    where
        T: DeserializeOwned + Default,
    {
        let mut buf = vec![];
        Self::acquire_component_ls_lock(&self, key_file_path)?;
        let mut f = File::open(key_file_path.get().unwrap())?;
        f.read_to_end(&mut buf)?;
        Self::release_component_ls_lock(&self, lock_file_path)?;
        let value = if buf.is_empty() {
            T::default()
        } else {
            bincode::deserialize(&buf).map_err(|e| RuntimeInnerError::Any(e))?
        };
        Ok(value)
    }

    fn acquire_component_ls_lock(
        &self,
        lock_file_path: once_cell::sync::OnceCell<PathBuf>,
    ) -> RuntimeResult<()> {
        let lock = lock_file_path.get().unwrap();
        while lock.exists() {
            thread::sleep(Duration::from_micros(5));
        }
        File::create(lock)?;
        Ok(())
    }

    fn release_component_ls_lock(
        &self,
        lock_file_path: once_cell::sync::OnceCell<PathBuf>,
    ) -> RuntimeResult<()> {
        match fs::remove_file(lock_file_path.get().unwrap()) {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(other) => Err(other.into()),
        }
    }
}
