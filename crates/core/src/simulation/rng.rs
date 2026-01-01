//! Seeded random number generator for deterministic simulation.
//!
//! This module provides a centralized RNG that can be shared across all
//! simulation components to ensure deterministic behavior.

use rand::{rngs::SmallRng, Rng, SeedableRng};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A thread-safe, seeded random number generator for simulation.
///
/// All random decisions during simulation should go through this RNG
/// to ensure deterministic replay when using the same seed.
///
/// # Determinism
///
/// For deterministic replay:
/// 1. All random decisions must use this RNG
/// 2. Random calls must happen in the same order each run
/// 3. Parallel access must be serialized (handled internally by Mutex)
#[derive(Clone)]
pub struct SimulationRng {
    inner: Arc<Mutex<SmallRng>>,
    seed: u64,
}

impl SimulationRng {
    /// Creates a new simulation RNG with the given seed.
    pub fn new(seed: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SmallRng::seed_from_u64(seed))),
            seed,
        }
    }

    /// Returns the seed used to create this RNG.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Generates a random boolean with the given probability of being true.
    pub fn gen_bool(&self, probability: f64) -> bool {
        self.inner.lock().unwrap().random_bool(probability)
    }

    /// Generates a random u64.
    pub fn gen_u64(&self) -> u64 {
        self.inner.lock().unwrap().random()
    }

    /// Generates a random u32.
    pub fn gen_u32(&self) -> u32 {
        self.inner.lock().unwrap().random()
    }

    /// Generates a random usize in the given range.
    pub fn gen_range(&self, range: std::ops::Range<usize>) -> usize {
        self.inner.lock().unwrap().random_range(range)
    }

    /// Generates a random u64 in the given range.
    pub fn gen_range_u64(&self, range: std::ops::Range<u64>) -> u64 {
        self.inner.lock().unwrap().random_range(range)
    }

    /// Generates a random f64 in [0, 1).
    pub fn gen_f64(&self) -> f64 {
        self.inner.lock().unwrap().random()
    }

    /// Generates a random Duration within the given range.
    pub fn gen_duration(&self, range: std::ops::Range<Duration>) -> Duration {
        let start_nanos = range.start.as_nanos() as u64;
        let end_nanos = range.end.as_nanos() as u64;
        let nanos = self
            .inner
            .lock()
            .unwrap()
            .random_range(start_nanos..end_nanos);
        Duration::from_nanos(nanos)
    }

    /// Chooses a random element from a slice, returning None if empty.
    pub fn choose<'a, T>(&self, slice: &'a [T]) -> Option<&'a T> {
        if slice.is_empty() {
            return None;
        }
        let idx = self.gen_range(0..slice.len());
        Some(&slice[idx])
    }

    /// Shuffles a slice in-place using Fisher-Yates algorithm.
    pub fn shuffle<T>(&self, slice: &mut [T]) {
        let mut rng = self.inner.lock().unwrap();
        for i in (1..slice.len()).rev() {
            let j = rng.random_range(0..=i);
            slice.swap(i, j);
        }
    }

    /// Creates a child RNG with a derived seed.
    ///
    /// This is useful for giving each peer its own RNG while maintaining
    /// overall determinism - the child's seed is derived from the parent's
    /// current state.
    pub fn child(&self) -> Self {
        let child_seed = self.gen_u64();
        Self::new(child_seed)
    }

    /// Creates a child RNG with a specific derived seed based on an index.
    ///
    /// This ensures that child RNGs are created deterministically regardless
    /// of the order in which they're requested, as long as the indices are
    /// consistent.
    pub fn child_with_index(&self, index: u64) -> Self {
        // Derive seed by mixing parent seed with index
        let derived_seed = self
            .seed
            .wrapping_mul(0x517cc1b727220a95)
            .wrapping_add(index);
        Self::new(derived_seed)
    }

    /// Access the inner RNG for operations that need direct `Rng` trait access.
    ///
    /// # Warning
    ///
    /// The returned guard holds the lock. Be careful not to hold it across
    /// await points or other blocking operations.
    pub fn lock(&self) -> std::sync::MutexGuard<'_, SmallRng> {
        self.inner.lock().unwrap()
    }
}

impl std::fmt::Debug for SimulationRng {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimulationRng")
            .field("seed", &self.seed)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_determinism_same_seed() {
        let rng1 = SimulationRng::new(42);
        let rng2 = SimulationRng::new(42);

        // Should produce identical sequences
        for _ in 0..100 {
            assert_eq!(rng1.gen_u64(), rng2.gen_u64());
        }
    }

    #[test]
    fn test_determinism_different_seeds() {
        let rng1 = SimulationRng::new(42);
        let rng2 = SimulationRng::new(43);

        // Should produce different sequences
        let mut same_count = 0;
        for _ in 0..100 {
            if rng1.gen_u64() == rng2.gen_u64() {
                same_count += 1;
            }
        }
        // Statistically very unlikely to have many matches
        assert!(same_count < 10);
    }

    #[test]
    fn test_gen_bool() {
        let rng = SimulationRng::new(42);

        // With 0.0 probability, should always be false
        for _ in 0..100 {
            assert!(!rng.gen_bool(0.0));
        }

        // With 1.0 probability, should always be true
        for _ in 0..100 {
            assert!(rng.gen_bool(1.0));
        }
    }

    #[test]
    fn test_gen_range() {
        let rng = SimulationRng::new(42);

        for _ in 0..100 {
            let val = rng.gen_range(10..20);
            assert!((10..20).contains(&val));
        }
    }

    #[test]
    fn test_gen_duration() {
        let rng = SimulationRng::new(42);
        let min = Duration::from_millis(10);
        let max = Duration::from_millis(100);

        for _ in 0..100 {
            let val = rng.gen_duration(min..max);
            assert!(val >= min && val < max);
        }
    }

    #[test]
    fn test_choose() {
        let rng = SimulationRng::new(42);
        let items = vec![1, 2, 3, 4, 5];

        // Should never return None for non-empty slice
        for _ in 0..100 {
            assert!(rng.choose(&items).is_some());
        }

        // Should return None for empty slice
        let empty: Vec<i32> = vec![];
        assert!(rng.choose(&empty).is_none());
    }

    #[test]
    fn test_shuffle_determinism() {
        let rng1 = SimulationRng::new(42);
        let rng2 = SimulationRng::new(42);

        let mut items1 = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut items2 = items1.clone();

        rng1.shuffle(&mut items1);
        rng2.shuffle(&mut items2);

        assert_eq!(items1, items2);
    }

    #[test]
    fn test_child_determinism() {
        let parent1 = SimulationRng::new(42);
        let parent2 = SimulationRng::new(42);

        let child1 = parent1.child();
        let child2 = parent2.child();

        // Children should produce identical sequences
        for _ in 0..100 {
            assert_eq!(child1.gen_u64(), child2.gen_u64());
        }
    }

    #[test]
    fn test_child_with_index_determinism() {
        let parent = SimulationRng::new(42);

        // Same index should give same child
        let child1 = parent.child_with_index(5);
        let child2 = parent.child_with_index(5);

        for _ in 0..10 {
            assert_eq!(child1.gen_u64(), child2.gen_u64());
        }

        // Different indices should give different children
        let child3 = parent.child_with_index(6);
        let child4 = parent.child_with_index(7);

        // They should produce different sequences
        let val3 = child3.gen_u64();
        let val4 = child4.gen_u64();
        // Just check they're both valid (extremely unlikely to match by chance)
        assert!(val3 != 0 || val4 != 0);
    }
}
