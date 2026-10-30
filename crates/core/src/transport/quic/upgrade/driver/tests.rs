use super::*;
use crate::transport::crypto::TransportKeypair;

fn seed(failures: u32, retry_secs: u64) -> BackoffSeed {
    BackoffSeed {
        failures,
        retry_at: Duration::from_secs(retry_secs),
    }
}

fn store(entries: usize) -> SeedStore {
    SeedStore::new(NonZeroUsize::new(entries).unwrap())
}

/// The newer session's seed stands, whichever files first. A max merge would
/// keep the old session's count and undo the clear a stable path made.
#[test]
fn an_older_session_never_replaces_a_newer_sessions_seed() {
    let key = TransportKeypair::new().public().clone();
    let mut seeds = store(4);
    seeds.file(&key, 2, seed(0, 0));
    seeds.file(&key, 1, seed(5, 3000));
    assert_eq!(seeds.seed_for(&key), seed(0, 0));
    seeds.file(&key, 3, seed(1, 60));
    assert_eq!(seeds.seed_for(&key), seed(1, 60));
    seeds.file(&key, 3, seed(2, 120));
    assert_eq!(seeds.seed_for(&key), seed(2, 120), "a session may refile");
}

/// The store never holds more than its limit, and the least recently used key
/// goes first. A key it lost reads as no backoff.
#[test]
fn the_store_is_bounded_and_evicts_the_least_recently_used() {
    let keys: Vec<_> = (0..4)
        .map(|_| TransportKeypair::new().public().clone())
        .collect();
    let mut seeds = store(2);
    seeds.file(&keys[0], 1, seed(1, 1));
    seeds.file(&keys[1], 2, seed(2, 2));
    assert_eq!(seeds.seed_for(&keys[0]), seed(1, 1), "refreshes keys[0]");
    seeds.file(&keys[2], 3, seed(3, 3));
    assert_eq!(seeds.filed.len(), 2);
    assert_eq!(seeds.seed_for(&keys[1]), BackoffSeed::default());
    assert_eq!(seeds.seed_for(&keys[0]), seed(1, 1));
    assert_eq!(seeds.seed_for(&keys[2]), seed(3, 3));
    for key in &keys {
        seeds.file(key, 9, seed(9, 9));
        assert!(seeds.filed.len() <= 2);
    }
}
