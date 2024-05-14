use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};

pub use chrono;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "clap", derive(clap::Parser))]
pub struct PingContractOptions {
    /// Time to live for the ping record.
    #[serde(with = "humantime_serde")]
    #[cfg_attr(feature = "clap", clap(long, value_parser = duration_parser, default_value = "5s"))]
    pub ttl: Duration,

    /// The frequency to send ping record.
    #[serde(with = "humantime_serde")]
    #[cfg_attr(feature = "clap", clap(long, value_parser = duration_parser, default_value = "1s"))]
    pub frequency: Duration,

    /// The tag of the ping contract
    #[serde(default = "freenet_ping")]
    #[cfg_attr(feature = "clap", clap(long, default_value = "freenet-ping"))]
    pub tag: String,
}

#[inline]
fn freenet_ping() -> String {
    "freenet-ping".to_string()
}

#[cfg(feature = "clap")]
#[inline]
fn duration_parser(s: &str) -> Result<Duration, humantime::DurationError> {
    humantime::parse_duration(s)
}

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

    pub fn merge(&mut self, other: Self, ttl: Duration) {
        #[cfg(feature = "std")]
        let now = Utc::now();
        #[cfg(not(feature = "std"))]
        let now = freenet_stdlib::time::now();

        for (name, created_time) in other.from.into_iter() {
            if now <= created_time + ttl {
                self.from.insert(name, created_time);
            }
        }

        self.from.retain(|_, v| now <= *v + ttl);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_expired() {
        let mut ping = Ping::new();
        ping.insert("Alice".to_string());
        ping.insert("Bob".to_string());

        let mut other = Ping::new();
        other.from.insert("Alice".to_string(), Utc::now() - Duration::from_secs(6));
        other.from.insert("Charlie".to_string(), Utc::now() - Duration::from_secs(6));

        ping.merge(other, Duration::from_secs(5));

        assert_eq!(ping.len(), 2);
        assert!(ping.contains_key("Alice"));
        assert!(ping.contains_key("Bob"));
        assert!(!ping.contains_key("Charlie"));
    }

    #[test]
    fn test_merge_ok() {
        let mut ping = Ping::new();
        ping.insert("Alice".to_string());
        ping.insert("Bob".to_string());

        let mut other = Ping::new();
        other.from.insert("Alice".to_string(), Utc::now() - Duration::from_secs(4));
        other.from.insert("Charlie".to_string(), Utc::now() - Duration::from_secs(4));

        ping.merge(other, Duration::from_secs(5));

        assert_eq!(ping.len(), 3);
        assert!(ping.contains_key("Alice"));
        assert!(ping.contains_key("Bob"));
        assert!(ping.contains_key("Charlie"));
    }
}
