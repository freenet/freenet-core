use dashmap::DashMap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::Read,
    path::PathBuf,
    sync::Arc,
    thread,
    time::Duration,
};
use stretto::Cache;

use crate::{error::RuntimeInnerError, DynError, RuntimeResult};
use locutus_stdlib::prelude::{Component, ComponentKey};

const DEFAULT_MAX_SIZE: i64 = 10 * 1024 * 1024 * 20;

type ComponentCodeKey = [u8; 32];

#[derive(Serialize, Deserialize)]
struct KeyToCodeMap(Vec<(ComponentKey, ComponentCodeKey)>);

impl From<&DashMap<ComponentKey, ComponentCodeKey>> for KeyToCodeMap {
    fn from(vals: &DashMap<ComponentKey, ComponentCodeKey>) -> Self {
        let mut map = vec![];
        for r in vals.iter() {
            map.push((r.key().clone(), *r.value()));
        }
        Self(map)
    }
}

pub struct ComponentStore {
    components_dir: PathBuf,
    component_cache: Cache<ComponentCodeKey, Component<'static>>,
    key_to_component_part: Arc<DashMap<ComponentKey, ComponentCodeKey>>,
}

static LOCK_FILE_PATH: once_cell::sync::OnceCell<PathBuf> = once_cell::sync::OnceCell::new();
static KEY_FILE_PATH: once_cell::sync::OnceCell<PathBuf> = once_cell::sync::OnceCell::new();

impl ComponentStore {
    /// # Arguments
    /// - max_size: max size in bytes of the components being cached
    pub fn new(components_dir: PathBuf, max_size: i64) -> RuntimeResult<Self> {
        // const ERR: &str = "failed to build mem cache";
        // let key_to_component_part;
        // let _ = LOCK_FILE_PATH.try_insert(components_dir.join("__LOCK"));
        // let key_file = match KEY_FILE_PATH
        //     .try_insert(components_dir.join("KEY_DATA"))
        //     .map_err(|(e, _)| e)
        // {
        //     Ok(f) => f,
        //     Err(f) => f,
        // };
        // if !key_file.exists() {
        //     std::fs::create_dir_all(&components_dir).map_err(|err| {
        //         tracing::error!("error creating component dir: {err}");
        //         err
        //     })?;
        //     key_to_component_part = Arc::new(DashMap::new());
        //     File::create(components_dir.join("KEY_DATA"))?;
        // } else {
        //     let map = Self::load_from_file::<KeyToCodeMap>()?;
        //     key_to_component_part = Arc::new(DashMap::from_iter(map.0));
        // }
        // Self::watch_changes(key_to_component_part.clone())?;
        // Ok(Self {
        //     component_cache: Cache::new(100, max_size).expect(ERR),
        //     components_dir,
        //     key_to_component_part,
        // })
        todo!()
    }

    // Returns a copy of the component bytes if available, none otherwise.
    pub fn fetch_component(&self, key: &ComponentKey) -> Option<Component<'static>> {
        if let Some(component) = self.component_cache.get(key.code_hash()) {
            return Some(component.value().clone());
        }
        let cmp_path = self
            .components_dir
            .join(key.encode())
            .with_extension("wasm");
        let cmp = Component::try_from(cmp_path.as_path()).ok()?;
        let cloned = cmp.clone();
        self.component_cache
            .insert(*key.code_hash(), cmp, cloned.as_ref().len() as i64);
        Some(cloned)
    }

    pub fn store_component(&mut self, component: Component<'_>) -> RuntimeResult<()> {
        let key = component.key();
        let cmp_path = self
            .components_dir
            .join(key.encode())
            .with_extension("wasm");
        std::fs::write(cmp_path, component.as_ref())?;
        let cost = component.as_ref().len();
        self.component_cache
            .insert(*key.code_hash(), component.into_owned(), cost as i64);
        Ok(())
    }

    pub fn remove_component(&mut self, key: &ComponentKey) -> RuntimeResult<()> {
        self.component_cache.remove(key.code_hash());
        let cmp_path = self
            .components_dir
            .join(key.encode())
            .with_extension("wasm");
        match std::fs::remove_file(cmp_path) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
            Ok(_) => Ok(()),
        }
    }

    pub fn get_component_path(&mut self, key: &ComponentKey) -> RuntimeResult<PathBuf> {
        let key_path = key.encode().to_lowercase();
        Ok(self.components_dir.join(key_path).with_extension("wasm"))
    }

    pub fn code_hash_from_key(&self, key: &ComponentKey) -> Option<ComponentCodeKey> {
        self.key_to_component_part.get(key).map(|r| *r.value())
    }

    fn watch_changes(
        ori_map: Arc<DashMap<ComponentKey, ComponentCodeKey>>,
    ) -> Result<(), DynError> {
        // let mut watcher = notify::recommended_watcher(
        //     move |res: Result<notify::Event, notify::Error>| match res {
        //         Ok(ev) => {
        //             if let notify::EventKind::Modify(notify::event::ModifyKind::Data(_)) = ev.kind {
        //                 match Self::load_from_file() {
        //                     Err(err) => tracing::error!("{err}"),
        //                     Ok(map) => {
        //                         for (k, v) in map.0 {
        //                             ori_map.insert(k, v);
        //                         }
        //                     }
        //                 }
        //             }
        //         }
        //         Err(e) => tracing::error!("{e}"),
        //     },
        // )?;
        //
        // watcher.watch(
        //     KEY_FILE_PATH.get().unwrap(),
        //     notify::RecursiveMode::NonRecursive,
        // )?;
        Ok(())
    }
}

impl Default for ComponentStore {
    fn default() -> Self {
        Self {
            components_dir: Default::default(),
            component_cache: Cache::new(100, DEFAULT_MAX_SIZE).unwrap(),
            key_to_component_part: Arc::new(DashMap::new()),
        }
    }
}
