use std::path::PathBuf;

use locutus_stdlib::prelude::{Component, ComponentKey};
use stretto::Cache;

use crate::RuntimeResult;

const DEFAULT_MAX_SIZE: i64 = 10 * 1024 * 1024 * 20;

pub(crate) struct ComponentStore {
    components_dir: PathBuf,
    component_cache: Cache<ComponentKey, Component<'static>>,
}

impl ComponentStore {
    pub fn fetch_component(&self, key: &ComponentKey) -> Option<Component<'static>> {
        if let Some(component) = self.component_cache.get(key) {
            return Some(component.value().clone());
        }
        let cmp_path = self
            .components_dir
            .join(key.encode())
            .with_extension("wasm");
        let cmp = Component::try_from(cmp_path.as_path()).ok()?;
        let cloned = cmp.clone();
        self.component_cache
            .insert(key.clone(), cmp, cloned.as_ref().len() as i64);
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
            .insert(key.clone(), component.into_owned(), cost as i64);
        Ok(())
    }

    pub fn remove_component(&mut self, key: &ComponentKey) -> RuntimeResult<()> {
        self.component_cache.remove(key);
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
}

impl Default for ComponentStore {
    fn default() -> Self {
        Self {
            components_dir: Default::default(),
            component_cache: Cache::new(100, DEFAULT_MAX_SIZE).unwrap(),
        }
    }
}
