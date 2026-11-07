//! A `BuildHasher` for maps whose iteration order feeds a decision.
//!
//! std `HashMap`/`HashSet` and `DashMap` default to `RandomState`, which takes
//! its keys from the OS once per thread and increments them for every new map.
//! Under simulation that makes one seed take different paths from one run to
//! the next, between processes and between runs in one process (#6132): a
//! capped loop over a map, a tie-break, or a walk that sends or wakes in map
//! order visits the entries in an order that changes with every map instance.
//!
//! Under the `simulation_tests` feature the hasher is SipHash with fixed keys,
//! so iteration order depends only on the keys and the insertion history.
//! Every other build keeps `RandomState`, so production behaviour is unchanged.
//!
//! Construct these maps with `Default::default()` (`HashMap::default()`,
//! `DashMap::default()`): `new()` exists only for `RandomState`.
//!
//! A fixed order is still an arbitrary order. A capped loop over one of these
//! maps that must reach every entry over time needs its own rotation, not a
//! sort; see #6123.
//!
//! Which maps: those whose iteration order can change what a node does (a
//! capped or early-break loop, a first-match or eviction tie, messages sent,
//! tasks woken or RNG drawn in map order), from an audit of the crate during
//! #6123 and #6132. A map only looked up, or iterated into a sum or a sorted
//! list, does not need it.

#[cfg(not(feature = "simulation_tests"))]
pub(crate) type DecisionHasher = std::collections::hash_map::RandomState;

#[cfg(feature = "simulation_tests")]
pub(crate) type DecisionHasher =
    std::hash::BuildHasherDefault<std::collections::hash_map::DefaultHasher>;

pub(crate) type DecisionHashMap<K, V> = std::collections::HashMap<K, V, DecisionHasher>;
pub(crate) type DecisionHashSet<K> = std::collections::HashSet<K, DecisionHasher>;
pub(crate) type DecisionDashMap<K, V> = dashmap::DashMap<K, V, DecisionHasher>;

#[cfg(all(test, feature = "simulation_tests"))]
mod tests {
    use super::DecisionHashMap;

    /// Two maps with the same inserts iterate in the same order. With std
    /// `RandomState` every map gets its own keys, so this differs between
    /// instances (and between processes).
    #[test]
    fn two_maps_with_the_same_inserts_iterate_in_the_same_order() {
        let build = || {
            let mut map: DecisionHashMap<u64, ()> = Default::default();
            for key in 0..64u64 {
                map.insert(key.wrapping_mul(0x9E37_79B9_7F4A_7C15), ());
            }
            map.keys().copied().collect::<Vec<_>>()
        };
        let first = build();
        for _ in 0..8 {
            assert_eq!(build(), first);
        }
    }
}
