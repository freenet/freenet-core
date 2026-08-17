use std::{collections::BTreeMap, fmt::Display, time::Duration};

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

    /// The tag of the ping contract subscriber.
    #[cfg_attr(feature = "clap", clap(long))]
    pub tag: String,

    /// Code hash of the ping contract.
    #[cfg_attr(feature = "clap", clap(long))]
    pub code_key: String,
}

#[cfg(feature = "clap")]
#[inline]
fn duration_parser(s: &str) -> Result<Duration, humantime::DurationError> {
    humantime::parse_duration(s)
}

/// Maximum number of ping entries to keep per peer
const MAX_HISTORY_PER_PEER: usize = 10;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, Clone)]
pub struct Ping {
    /// BTreeMap, not HashMap: contract state is compared byte-for-byte across
    /// peers to decide whether they have converged, and a HashMap serializes in
    /// iteration order, so two peers holding the same logical state encode it
    /// differently and never agree. Canonical encoding is a platform requirement
    /// (freenet-core #5320).
    from: BTreeMap<String, Vec<DateTime<Utc>>>,
    /// Optional padding to inflate serialized size for streaming tests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub padding: Option<Vec<u8>>,
}

impl core::ops::Deref for Ping {
    type Target = BTreeMap<String, Vec<DateTime<Utc>>>;

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

    /// Create a Ping with padding of the given size in bytes.
    pub fn with_padding(size: usize) -> Self {
        Self {
            from: BTreeMap::new(),
            padding: Some(vec![0xAB; size]),
        }
    }

    #[cfg(feature = "std")]
    pub fn insert(&mut self, name: String) {
        let now = Utc::now();
        self.from.entry(name.clone()).or_default().push(now);

        // Keep only the last MAX_HISTORY_PER_PEER entries
        if let Some(entries) = self.from.get_mut(&name)
            && entries.len() > MAX_HISTORY_PER_PEER
        {
            // Sort in descending order (newest first)
            entries.sort_by(|a, b| b.cmp(a));
            // Keep only the newest MAX_HISTORY_PER_PEER entries
            entries.truncate(MAX_HISTORY_PER_PEER);
        }
    }

    /// Merge another peer's ping state into this one.
    ///
    /// The retention rule is applied to the UNION of both sides, not to the
    /// incoming side alone. Filtering only `other` made the merge order-dependent:
    /// the same expired timestamp survived if this peer already held it and vanished
    /// if it arrived from a peer, so `merge(A, B)` and `merge(B, A)` disagreed
    /// permanently. That is a broken merge law (freenet-core #5320), and it was
    /// found by running the conformance verifier against this contract.
    ///
    /// The retention policy itself is unchanged and deliberate: keep the newest
    /// `MAX_HISTORY_PER_PEER` entries regardless of age, plus any older entries
    /// still within TTL. Applied to the union it is a pure function of (union, now),
    /// so it is commutative, associative and idempotent at a given instant.
    pub fn merge(&mut self, other: Self, ttl: Duration) -> BTreeMap<String, Vec<DateTime<Utc>>> {
        #[cfg(feature = "std")]
        let now = Utc::now();
        #[cfg(not(feature = "std"))]
        let now = freenet_stdlib::time::now();

        let before = self.from.clone();

        // Union first. Nothing is judged before both sides are in one place, which
        // is what keeps the rule symmetric.
        for (name, incoming) in other.from {
            self.from.entry(name).or_default().extend(incoming);
        }

        for timestamps in self.from.values_mut() {
            Self::retain_history(timestamps, now, ttl);
        }

        // Report whatever actually changed, so callers still learn about new pings.
        let mut updates = BTreeMap::new();
        for (name, timestamps) in &self.from {
            if before.get(name) != Some(timestamps) {
                updates.insert(name.clone(), timestamps.clone());
            }
        }
        updates
    }

    /// Newest first, deduplicated, keeping `MAX_HISTORY_PER_PEER` regardless of age
    /// plus any older entries still within TTL.
    ///
    /// Sorting is by timestamp descending, a total order over the entries' own
    /// content, so the surviving set does not depend on the order they arrived in.
    fn retain_history(timestamps: &mut Vec<DateTime<Utc>>, now: DateTime<Utc>, ttl: Duration) {
        timestamps.sort_by(|a, b| b.cmp(a));
        timestamps.dedup();

        if timestamps.len() > MAX_HISTORY_PER_PEER {
            let mut keep = timestamps[..MAX_HISTORY_PER_PEER].to_vec();
            keep.extend(
                timestamps[MAX_HISTORY_PER_PEER..]
                    .iter()
                    .filter(|t| now <= **t + ttl)
                    .copied(),
            );
            *timestamps = keep;
        }
    }

    pub fn last_timestamp(&self, name: &str) -> Option<&DateTime<Utc>> {
        self.from
            .get(name)
            .and_then(|timestamps| timestamps.first())
    }

    /// Checks if a peer has any ping entries
    pub fn contains_key(&self, name: &str) -> bool {
        self.from.get(name).is_some_and(|v| !v.is_empty())
    }

    /// Returns the number of peers with ping entries
    pub fn len(&self) -> usize {
        self.from.len()
    }

    /// Returns whether there are no ping entries
    pub fn is_empty(&self) -> bool {
        self.from.is_empty()
    }
}

impl Display for Ping {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut entries: Vec<_> = self.from.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        write!(
            f,
            "Ping {{ {} }}",
            entries
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}: [{}]",
                        k,
                        v.iter()
                            .map(|dt| dt.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;

    /// Expired entries are retained on BOTH sides, or on neither.
    ///
    /// Updated for the #5320 conformance fix. This previously asserted that an
    /// incoming peer whose only entry was expired got dropped, while
    /// `test_keep_newest_entries_regardless_of_ttl` and
    /// `test_preserve_max_history_when_all_expired` assert that this peer's OWN
    /// expired entries are kept. Those two expectations are the same rule applied
    /// asymmetrically, and the asymmetry is what broke commutativity: the same
    /// expired timestamp survived if you held it and vanished if a peer sent it, so
    /// `merge(A, B)` and `merge(B, A)` disagreed permanently.
    ///
    /// The retention policy is unchanged — newest `MAX_HISTORY_PER_PEER` regardless
    /// of age — and is now applied to the union, so an incoming expired entry is
    /// kept exactly as an own expired entry is.
    #[test]
    fn test_merge_expired_is_symmetric() {
        let old_time = Utc::now() - Duration::from_secs(6);
        let ttl = Duration::from_secs(5);

        // Build the two inputs ONCE and clone them per direction. Constructing them
        // separately stamps `Utc::now()` again, so the two orders would be fed
        // different inputs and the comparison would say nothing about the merge.
        let mut a = Ping::new();
        a.insert("Alice".to_string());
        a.insert("Bob".to_string());
        let mut b = Ping::new();
        b.from.insert("Alice".to_string(), vec![old_time]);
        b.from.insert("Charlie".to_string(), vec![old_time]);

        let mut a_then_b = a.clone();
        a_then_b.merge(b.clone(), ttl);

        // Charlie is kept, on the same rule that keeps our own expired entries.
        assert!(a_then_b.contains_key("Alice"));
        assert!(a_then_b.contains_key("Bob"));
        assert!(
            a_then_b.contains_key("Charlie"),
            "an incoming expired entry must be retained on the same terms as an own \
             expired entry, or the merge is order-dependent"
        );

        // The property that matters: merging the other way round agrees.
        let mut b_then_a = b.clone();
        b_then_a.merge(a.clone(), ttl);
        assert_eq!(
            *a_then_b, *b_then_a,
            "merge must be commutative; this is the defect the conformance verifier \
             found against the deployed ping contract"
        );
    }

    #[test]
    fn test_merge_ok() {
        let mut ping = Ping::new();
        ping.insert("Alice".to_string());
        ping.insert("Bob".to_string());

        let mut other = Ping::new();
        let recent_time = Utc::now() - Duration::from_secs(4);
        other.from.insert("Alice".to_string(), vec![recent_time]);
        other.from.insert("Charlie".to_string(), vec![recent_time]);

        ping.merge(other, Duration::from_secs(5));

        assert_eq!(ping.len(), 3);
        assert!(ping.contains_key("Alice"));
        assert!(ping.contains_key("Bob"));
        assert!(ping.contains_key("Charlie"));
    }

    #[test]
    fn test_history_limit() {
        let mut ping = Ping::new();
        let name = "Alice".to_string();

        // Insert more than MAX_HISTORY_PER_PEER entries
        for _ in 0..MAX_HISTORY_PER_PEER + 5 {
            ping.insert(name.clone());
            // Add a small delay to ensure different timestamps
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Verify we only kept the maximum number of entries
        assert_eq!(ping.from.get(&name).unwrap().len(), MAX_HISTORY_PER_PEER);

        // Verify they're sorted newest first
        let timestamps = ping.from.get(&name).unwrap();
        for i in 0..timestamps.len() - 1 {
            assert!(timestamps[i] > timestamps[i + 1]);
        }
    }

    #[test]
    fn test_merge_preserves_history() {
        let mut ping1 = Ping::new();
        let mut ping2 = Ping::new();
        let name = "Alice".to_string();

        // Insert 5 entries in ping1
        for _ in 0..5 {
            ping1.insert(name.clone());
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Insert 5 different entries in ping2
        for _ in 0..5 {
            ping2.insert(name.clone());
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Merge ping2 into ping1
        ping1.merge(ping2, Duration::from_secs(30));

        // Should have 10 entries for Alice now
        assert_eq!(ping1.from.get(&name).unwrap().len(), 10);

        // Verify they're sorted newest first
        let timestamps = ping1.from.get(&name).unwrap();
        for i in 0..timestamps.len() - 1 {
            assert!(timestamps[i] > timestamps[i + 1]);
        }
    }

    #[test]
    fn test_preserve_max_history_when_all_expired() {
        // Create a ping with expired entries
        let mut ping = Ping::new();
        let name = "Alice".to_string();

        // Insert MAX_HISTORY_PER_PEER entries, all expired
        let expired_time = Utc::now() - Duration::from_secs(10);
        for i in 0..MAX_HISTORY_PER_PEER {
            let timestamp = expired_time - Duration::from_secs(i as u64); // Make different timestamps
            ping.from.entry(name.clone()).or_default().push(timestamp);
        }

        // Ensure entries are sorted newest first
        ping.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));

        // Use a short TTL so all entries would normally be expired
        let ttl = Duration::from_secs(5);

        // Create an empty ping to merge with
        let other = Ping::default();

        // Merge - this should preserve all entries despite being expired
        ping.merge(other, ttl);

        // Verify all entries are still there
        assert_eq!(ping.from.get(&name).unwrap().len(), MAX_HISTORY_PER_PEER);
    }

    #[test]
    fn test_remove_only_expired_entries_beyond_max() {
        let mut ping = Ping::new();
        let name = "Alice".to_string();
        let now = Utc::now();

        // Insert 5 fresh entries
        for i in 0..5 {
            ping.from
                .entry(name.clone())
                .or_default()
                .push(now - Duration::from_secs(i));
        }

        // Insert 10 expired entries
        let expired_time = now - Duration::from_secs(20); // well beyond TTL
        for i in 0..10 {
            ping.from
                .entry(name.clone())
                .or_default()
                .push(expired_time - Duration::from_secs(i));
        }

        // Sort entries (newest first)
        ping.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));

        // Use a TTL of 10 seconds
        let ttl = Duration::from_secs(10);

        // Create an empty ping to merge with
        let other = Ping::default();

        // Merge - should keep all fresh entries and enough expired ones to reach MAX_HISTORY_PER_PEER
        ping.merge(other, ttl);

        // Verify we have MAX_HISTORY_PER_PEER entries
        assert_eq!(ping.from.get(&name).unwrap().len(), MAX_HISTORY_PER_PEER);

        // Verify the first 5 entries are the fresh ones
        let entries = ping.from.get(&name).unwrap();
        for entry in entries.iter().take(5) {
            assert!(now - entry < chrono::TimeDelta::seconds(10)); // These should be fresh
        }
    }

    #[test]
    fn test_keep_newest_entries_regardless_of_ttl() {
        let mut ping1 = Ping::new();
        let mut ping2 = Ping::new();
        let name = "Alice".to_string();
        let now = Utc::now();

        // Add 5 fresh entries to ping1
        for i in 0..5 {
            let timestamp = now - Duration::from_secs(i);
            ping1.from.entry(name.clone()).or_default().push(timestamp);
        }

        // Add 5 expired entries to ping2, but newer than ping1's entries
        // These should be kept despite being expired because they're the newest
        let expired_but_newer = now + Duration::from_secs(10); // in the future (newer)
        for i in 0..5 {
            let timestamp = expired_but_newer - Duration::from_secs(i);
            ping2.from.entry(name.clone()).or_default().push(timestamp);
        }

        // Sort both sets
        ping1.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));
        ping2.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));

        // Use a very short TTL so basically everything is expired except the very newest
        let ttl = Duration::from_secs(1);

        // Merge ping2 into ping1
        ping1.merge(ping2, ttl);

        // Verify the result has MAX_HISTORY_PER_PEER entries
        assert_eq!(ping1.from.get(&name).unwrap().len(), MAX_HISTORY_PER_PEER);

        // The first 5 entries should be the ones from ping2 (they're newer)
        let entries = ping1.from.get(&name).unwrap();
        for entry in entries.iter().take(5) {
            assert!(*entry > now); // These should be the future timestamps
        }
    }

    #[test]
    fn test_consistent_history_after_multiple_merges() {
        let mut ping_main = Ping::new();
        let name = "Alice".to_string();
        let now = Utc::now();

        // Create several pings with different timestamps, ensuring they are clearly distinct
        let mut ping1 = Ping::new();
        let mut ping2 = Ping::new();
        let mut ping3 = Ping::new();

        // Use more explicit timestamps to avoid any potential overlap issues
        let timestamps_ping1: Vec<DateTime<Utc>> = (0..4)
            .map(|i| now - Duration::from_secs(30 + i * 2))
            .collect();
        let timestamps_ping2: Vec<DateTime<Utc>> = (0..4)
            .map(|i| now - Duration::from_secs(20 + i * 2))
            .collect();
        let timestamps_ping3: Vec<DateTime<Utc>> = (0..4)
            .map(|i| now - Duration::from_secs(10 + i * 2))
            .collect();

        // Add entries to each ping
        for timestamp in &timestamps_ping1 {
            ping1.from.entry(name.clone()).or_default().push(*timestamp);
        }
        for timestamp in &timestamps_ping2 {
            ping2.from.entry(name.clone()).or_default().push(*timestamp);
        }
        for timestamp in &timestamps_ping3 {
            ping3.from.entry(name.clone()).or_default().push(*timestamp);
        }

        // Sort all sets
        ping1.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));
        ping2.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));
        ping3.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));

        // Use a TTL that would expire some but not all entries
        let ttl = Duration::from_secs(25);

        // Merge in random order to test consistency
        ping_main.merge(ping2, ttl); // Middle
        ping_main.merge(ping1, ttl); // Oldest
        ping_main.merge(ping3, ttl); // Newest

        // Define the time range boundaries for classifying entries
        let ping3_min = now - Duration::from_secs(18);
        let ping2_min = now - Duration::from_secs(28);

        // Get the final entries
        let entries = ping_main.from.get(&name).unwrap();

        // We should have at most MAX_HISTORY_PER_PEER entries after merging
        assert!(entries.len() <= MAX_HISTORY_PER_PEER);

        // The entries should be sorted newest first
        for i in 0..entries.len() - 1 {
            assert!(
                entries[i] > entries[i + 1],
                "Entries not correctly sorted at positions {} and {}",
                i,
                i + 1
            );
        }

        // Verify the newest entries are from ping3
        assert!(
            entries[0] >= now - Duration::from_secs(18),
            "Expected newest entry to be from ping3"
        );

        // Count entries by source time range
        let mut ping3_count = 0;
        let mut ping2_count = 0;
        let mut ping1_count = 0;

        for entry in entries {
            if *entry >= ping3_min {
                ping3_count += 1;
            } else if *entry >= ping2_min {
                ping2_count += 1;
            } else {
                ping1_count += 1;
            }
        }

        // Since TTL is 25s, all ping3 entries (4) and most ping2 entries should be included
        assert_eq!(
            ping3_count, 4,
            "Expected all 4 entries from ping3 (newest), but found {ping3_count}"
        );

        // Check that we have at least 3 entries from ping2
        assert!(
            ping2_count >= 3,
            "Expected at least 3 entries from ping2 (middle), but found {ping2_count}"
        );

        // Due to TTL, we expect at most 3 entries from ping1
        assert!(
            ping1_count <= 3,
            "Expected at most 3 entries from ping1 (oldest), but got {ping1_count}"
        );

        // Verify total count matches what we found
        let total_classified = ping3_count + ping2_count + ping1_count;
        assert_eq!(entries.len(), total_classified, "Entry count mismatch");
    }

    #[test]
    fn test_empty_after_merge_if_all_expired() {
        let mut ping = Ping::new();
        let name = "Alice".to_string();

        // Add some entries but all expired
        let expired_time = Utc::now() - Duration::from_secs(20);
        for i in 0..MAX_HISTORY_PER_PEER - 1 {
            // Less than MAX_HISTORY_PER_PEER entries
            let timestamp = expired_time - Duration::from_secs(i as u64);
            ping.from.entry(name.clone()).or_default().push(timestamp);
        }

        // Sort entries
        ping.from.get_mut(&name).unwrap().sort_by(|a, b| b.cmp(a));

        // Use a TTL shorter than the age of entries
        let ttl = Duration::from_secs(10);

        // Create an empty ping to merge with
        let other = Ping::default();

        // This should keep all entries despite being expired since we have less than MAX_HISTORY_PER_PEER
        ping.merge(other, ttl);

        // Verify all entries are kept
        assert_eq!(
            ping.from.get(&name).unwrap().len(),
            MAX_HISTORY_PER_PEER - 1
        );
    }
}
