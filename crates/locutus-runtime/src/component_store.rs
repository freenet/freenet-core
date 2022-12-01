use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::Write,
    path::PathBuf,
    sync::Arc
};
use stretto::Cache;

use crate::RuntimeResult;
use locutus_stdlib::prelude::{Component, ComponentKey};
use crate::store::{StoreEntriesContainer, StoreFsManagement};

const DEFAULT_MAX_SIZE: i64 = 10 * 1024 * 1024 * 20;

type ComponentCodeKey = [u8; 32];

#[derive(Serialize, Deserialize, Default)]
struct KeyToCodeMap(Vec<(ComponentKey, ComponentCodeKey)>);

impl StoreEntriesContainer for KeyToCodeMap {
    type MemContainer = Arc<DashMap<ComponentKey, ComponentCodeKey>>;
    type Key = ComponentKey;
    type Value = ComponentCodeKey;

    fn update(self, container: &mut Self::MemContainer) {
        for (k, v) in self.0 {
            container.insert(k, v);
        }
    }

    fn replace(container: &Self::MemContainer) -> Self {
        KeyToCodeMap::from(&**container)
    }

    fn insert(container: &mut Self::MemContainer, key: Self::Key, value: Self::Value) {
        container.insert(key, value);
    }
}

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

impl StoreFsManagement<KeyToCodeMap> for ComponentStore {}

impl ComponentStore {
    /// # Arguments
    /// - max_size: max size in bytes of the components being cached
    pub fn new(components_dir: PathBuf, max_size: i64) -> RuntimeResult<Self> {
        const ERR: &str = "failed to build mem cache";
        let key_to_component_part;
        let _ = LOCK_FILE_PATH.try_insert(components_dir.join("__LOCK"));
        let key_file = match KEY_FILE_PATH
            .try_insert(components_dir.join("KEY_DATA"))
            .map_err(|(e, _)| e)
        {
            Ok(f) => f,
            Err(f) => f,
        };
        if !key_file.exists() {
            std::fs::create_dir_all(&components_dir).map_err(|err| {
                tracing::error!("error creating component dir: {err}");
                err
            })?;
            key_to_component_part = Arc::new(DashMap::new());
            File::create(components_dir.join("KEY_DATA"))?;
        } else {
            let map = Self::load_from_file(
                KEY_FILE_PATH.get().unwrap().as_path(),
                LOCK_FILE_PATH.get().unwrap().as_path(),
            )?;
            key_to_component_part = Arc::new(DashMap::from_iter(map.0));
        }
        Self::watch_changes(
            key_to_component_part.clone(),
            KEY_FILE_PATH.get().unwrap().as_path(),
            LOCK_FILE_PATH.get().unwrap().as_path(),
        )?;
        Ok(Self {
            component_cache: Cache::new(100, max_size).expect(ERR),
            components_dir,
            key_to_component_part,
        })
    }

    // Returns a copy of the component bytes if available, none otherwise.
    pub fn fetch_component(&self, key: &ComponentKey) -> Option<Component<'static>> {
        if let Some(component) = self.component_cache.get(key.code_hash()) {
            return Some(component.value().clone());
        }
        self.key_to_component_part.get(key).and_then(|_|{
            let component_path = self
                .components_dir
                .join(key.encode())
                .with_extension("wasm");
            let component = Component::try_from(component_path.as_path()).ok()?;
            let size = component.as_ref().len() as i64;
            self.component_cache
                .insert(*key.code_hash(), component.clone(), size);
            Some(component)
        })
    }

    pub fn store_component(&mut self, component: Component<'_>) -> RuntimeResult<()> {
        let key = component.key();
        let component_hash = key.code_hash();

        if self.component_cache.get(component_hash).is_some() {
            return Ok(());
        }

        Self::update(
            &mut self.key_to_component_part,
            key.clone(),
            *component_hash,
            KEY_FILE_PATH.get().unwrap(),
            LOCK_FILE_PATH.get().unwrap().as_path(),
        )?;

        let component_path = self
            .components_dir
            .join(key.encode())
            .with_extension("wasm");
        if let Ok(component) = Component::try_from(component_path.as_path()) {
            let size = component.as_ref().len() as i64;
            self.component_cache
                .insert(*component_hash, component.clone(), size);
            return Ok(());
        }

        // insert in the memory cache
        let data = component.as_ref();
        let code_size = data.len() as i64;
        self.component_cache
            .insert(*component_hash, Component::from(data.to_vec()), code_size);

        let mut output: Vec<u8> = Vec::with_capacity(
            code_size as usize,
        );
        output.append(&mut component.as_ref().to_vec());
        let mut file = File::create(component_path)?;
        file.write_all(output.as_slice())?;

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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let component_dir = std::env::temp_dir().join("locutus-test").join("store-test");
        std::fs::create_dir_all(&component_dir)?;
        let mut store = ComponentStore::new(component_dir, 10_000)?;
        let component = Component::from(vec![0, 1, 2]);
        store.store_component(component.clone())?;
        let f = store.fetch_component(component.key());
        assert!(f.is_some());
        Ok(())
    }
}
