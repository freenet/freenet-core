use std::collections::HashSet;

use chrono::{DateTime, Utc};

pub use chrono;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Ping {
    from: HashSet<String>,
    timestamp: DateTime<Utc>,
}

#[cfg(feature = "std")]
impl Default for Ping {
    fn default() -> Self {
        Self {
            from: HashSet::new(),
            timestamp: Utc::now(),
        }
    }
}

impl core::ops::Deref for Ping {
    type Target = HashSet<String>;

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
    #[cfg(feature = "std")]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    #[cfg(feature = "std")]
    pub fn is_expired(&self) -> bool {
        self.timestamp + chrono::Duration::hours(1) < Utc::now()
    }

    pub fn merge(&mut self, other: Self) {
        self.from.extend(other.from);
        #[cfg(feature = "std")]
        {
            self.timestamp = Utc::now();
        }

        #[cfg(not(feature = "std"))]
        {
            self.timestamp = other.timestamp;
        }
    }
}
