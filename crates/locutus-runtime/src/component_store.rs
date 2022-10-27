use locutus_stdlib::prelude::ComponentKey;

#[derive(Default)]
pub(crate) struct ComponentStore;

impl ComponentStore {
    pub fn fetch_component(&self, key: &ComponentKey) -> Option<Vec<u8>> {
        todo!()
    }
}
