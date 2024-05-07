use std::collections::HashMap;

use chrono::{DateTime, Utc};

pub use chrono;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Ping {
    from: HashMap<String, DateTime<Utc>>,
}

impl core::ops::Deref for Ping {
    type Target = HashMap<String, DateTime<Utc>>;

    fn deref(&self) -> &Self::Target {
        &self.from
    }
}

impl core::ops::DerefMut for Ping {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.from
    }
}

impl Ping {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(feature = "std")]
    pub fn insert(&mut self, name: String) {
        self.from.insert(name, Utc::now());
    }

    pub fn merge(&mut self, other: Self) {
        self.from.extend(other.from);
    }
}
