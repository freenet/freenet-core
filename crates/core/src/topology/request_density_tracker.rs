use crate::ring::{Connection, Location};
use std::collections::{BTreeMap, VecDeque};
use thiserror::Error;

/// Tracks requests sent by a node to its neighbors and creates a density map, which
/// is useful for determining which new neighbors to connect to based on their
/// location.
pub(crate) struct RequestDensityTracker {
    /// Amount of requests done to an specific location.
    request_locations: BTreeMap<Location, usize>,
    /// Request locations sorted by order of execution.
    request_list: VecDeque<Location>,
    window_size: usize,
    samples: usize,
}

impl RequestDensityTracker {
    pub(crate) fn new(window_size: usize) -> Self {
        Self {
            request_locations: BTreeMap::new(),
            request_list: VecDeque::with_capacity(window_size),
            window_size,
            samples: 0,
        }
    }

    pub(crate) fn sample(&mut self, value: Location) {
        self.samples += 1;

        self.request_list.push_back(value);
        *self.request_locations.entry(value).or_insert(0) += 1;

        if self.request_list.len() > self.window_size {
            if let Some(oldest) = self.request_list.pop_front() {
                if let Some(count) = self.request_locations.get_mut(&oldest) {
                    *count -= 1;
                    if *count == 0 {
                        self.request_locations.remove(&oldest);
                    }
                }
            }
        }
    }

    pub(crate) fn create_density_map(
        &self,
        neighbor_locations: &BTreeMap<Location, Vec<Connection>>,
    ) -> Result<DensityMap, DensityMapError> {
        if neighbor_locations.is_empty() {
            return Err(DensityMapError::EmptyNeighbors);
        }

        let mut neighbor_request_counts = BTreeMap::new();

        for (sample_location, sample_count) in self.request_locations.iter() {
            let previous_neighbor = neighbor_locations
                .range(..*sample_location)
                .next_back()
                .or_else(|| neighbor_locations.iter().next_back());
            let next_neighbor = neighbor_locations
                .range(*sample_location..)
                .next()
                .or_else(|| neighbor_locations.iter().next());

            match (previous_neighbor, next_neighbor) {
                (Some((previous_neighbor_location, _)), Some((next_neighbor_location, _))) => {
                    if sample_location.distance(previous_neighbor_location)
                        < sample_location.distance(next_neighbor_location)
                    {
                        *neighbor_request_counts
                            .entry(*previous_neighbor_location)
                            .or_insert(0) += sample_count;
                    } else {
                        *neighbor_request_counts
                            .entry(*next_neighbor_location)
                            .or_insert(0) += sample_count;
                    }
                }
                _ => unreachable!("previous_neighbor and next_neighbor should always be Some if neighbor_locations is not empty"),
            }
        }

        Ok(DensityMap {
            neighbor_request_counts,
        })
    }
}

pub(crate) struct DensityMap {
    neighbor_request_counts: BTreeMap<Location, usize>,
}

impl DensityMap {
    pub fn get_density_at(&self, location: Location) -> Result<f64, DensityMapError> {
        if self.neighbor_request_counts.is_empty() {
            return Err(DensityMapError::EmptyNeighbors);
        }

        // Determine the locations below and above the given location
        let previous_neighbor = self
            .neighbor_request_counts
            .range(..location)
            .next_back()
            .or_else(|| self.neighbor_request_counts.iter().next_back());

        let next_neighbor = self
            .neighbor_request_counts
            .range(location..)
            .next()
            .or_else(|| self.neighbor_request_counts.iter().next());

        // Determine the value proportionate to the distance to the previous and next neighbor
        let count_estimate = match (previous_neighbor, next_neighbor) {
            (
                Some((previous_neighbor_location, previous_neighbor_count)),
                Some((next_neighbor_location, next_neighbor_count)),
            ) => {
                let previous_neighbor_dist =
                    location.distance(*previous_neighbor_location).as_f64();
                let next_neighbor_dist = location.distance(*next_neighbor_location).as_f64();
                let total_dist = previous_neighbor_dist + next_neighbor_dist;
                let previous_neighbor_prop = previous_neighbor_dist / total_dist;
                let next_neighbor_prop = next_neighbor_dist / total_dist;
                next_neighbor_prop * *previous_neighbor_count as f64
                    + previous_neighbor_prop * *next_neighbor_count as f64
            }
            // The None cases have been removed as they should not occur given the new logic
            _ => unreachable!("previous_neighbor and next_neighbor should always be Some if neighbor_request_counts is not empty"),
        };

        Ok(count_estimate)
    }

    pub fn get_max_density(&self) -> Result<Location, DensityMapError> {
        tracing::debug!("get_max_density called");

        if self.neighbor_request_counts.is_empty() {
            tracing::debug!("No neighbors to get max density from");
            return Err(DensityMapError::EmptyNeighbors);
        }

        // Identify the midpoint Location between the pair of neighbors
        // with the highest combined request count
        let mut max_density_location = Location::new(0.0);
        let mut max_density = 0;

        tracing::debug!("Starting to iterate over neighbor pairs");

        for (
            (previous_neighbor_location, previous_neighbor_count),
            (next_neighbor_location, next_neighbor_count),
        ) in self
            .neighbor_request_counts
            .iter()
            .zip(self.neighbor_request_counts.iter().skip(1))
        {
            // tracing span with location of first and last neighbor locations
            let span = tracing::debug_span!(
                "neighbor_pair",
                previous_neighbor_location = previous_neighbor_location.as_f64(),
                next_neighbor_location = next_neighbor_location.as_f64()
            );
            let _enter = span.enter();
            let combined_count = previous_neighbor_count + next_neighbor_count;
            tracing::debug!(combined_count, "Combined count for neighbor pair");

            if combined_count > max_density {
                tracing::debug!(
                    max_density = combined_count,
                    location = %max_density_location,
                    "New max density found"
                );
                max_density = combined_count;
                max_density_location = Location::new(
                    (previous_neighbor_location.as_f64() + next_neighbor_location.as_f64()) / 2.0,
                );
            }
        }

        tracing::debug!("Checking first and last neighbors");

        // We need to also check the first and last neighbors as locations are circular
        let first_neighbor = self.neighbor_request_counts.iter().next();
        let last_neighbor = self.neighbor_request_counts.iter().next_back();
        if let (
            Some((first_neighbor_location, first_neighbor_count)),
            Some((last_neighbor_location, last_neighbor_count)),
        ) = (first_neighbor, last_neighbor)
        {
            let combined_count = first_neighbor_count + last_neighbor_count;
            tracing::debug!(combined_count, "Combined count for first and last neighbor");

            if combined_count > max_density {
                // max_density = combined_count; Not needed as this is the last check
                let distance = first_neighbor_location.distance(*last_neighbor_location);
                let mut mp = first_neighbor_location.as_f64() - (distance.as_f64() / 2.0);
                if mp < 0.0 {
                    mp += 1.0;
                }
                max_density_location = Location::new(mp);
                tracing::debug!(
                    location = %max_density_location,
                    "New max density found at the edge"
                );
            }
        }

        tracing::debug!(location = %max_density_location, "Returning max density location");
        Ok(max_density_location)
    }

    /// Like `get_max_density()`, but biases toward locations closer to `my_location`
    /// using distance-weighted scoring: score = density / distance.
    ///
    /// This favors nearby high-density areas over distant ones, encouraging
    /// connections in the local neighborhood while still allowing sufficiently
    /// high-demand distant locations to win.
    ///
    /// Note: this is a deterministic argmax, not stochastic sampling. All peers
    /// with similar density maps will select the same target. A future improvement
    /// could sample from the score distribution to diversify connection targets.
    ///
    /// MIN_DISTANCE (0.001) prevents division-by-near-zero for candidates very close
    /// to `my_location`.
    pub fn get_max_density_weighted(
        &self,
        my_location: Location,
    ) -> Result<Location, DensityMapError> {
        const MIN_DISTANCE: f64 = 0.001;

        if self.neighbor_request_counts.is_empty() {
            return Err(DensityMapError::EmptyNeighbors);
        }

        // Single entry: no adjacent pairs exist, return the only location we have
        if self.neighbor_request_counts.len() == 1 {
            return Ok(*self.neighbor_request_counts.keys().next().unwrap());
        }

        let mut best_location = Location::new(0.0);
        let mut best_score: f64 = -1.0;

        // Score each adjacent-pair midpoint by density / distance_to_me
        for ((prev_loc, prev_count), (next_loc, next_count)) in self
            .neighbor_request_counts
            .iter()
            .zip(self.neighbor_request_counts.iter().skip(1))
        {
            let combined_density = (prev_count + next_count) as f64;
            let midpoint = Location::new((prev_loc.as_f64() + next_loc.as_f64()) / 2.0);
            let distance = my_location.distance(midpoint).as_f64().max(MIN_DISTANCE);
            let score = combined_density / distance;

            if score > best_score {
                best_score = score;
                best_location = midpoint;
            }
        }

        // Wrap-around: check first and last neighbor pair
        let first = self.neighbor_request_counts.iter().next();
        let last = self.neighbor_request_counts.iter().next_back();
        if let (Some((first_loc, first_count)), Some((last_loc, last_count))) = (first, last) {
            // Only meaningful if there are at least 2 entries (otherwise first == last)
            if first_loc != last_loc {
                let combined_density = (first_count + last_count) as f64;
                let wrap_distance = first_loc.distance(*last_loc);
                let mut mp = first_loc.as_f64() - (wrap_distance.as_f64() / 2.0);
                if mp < 0.0 {
                    mp += 1.0;
                }
                let midpoint = Location::new(mp);
                let distance = my_location.distance(midpoint).as_f64().max(MIN_DISTANCE);
                let score = combined_density / distance;

                if score > best_score {
                    best_location = midpoint;
                }
            }
        }

        Ok(best_location)
    }
}

/// Struct to handle caching of DensityMap
pub(in crate::topology) struct CachedDensityMap {
    density_map: Option<DensityMap>,
}

impl CachedDensityMap {
    pub fn new() -> Self {
        CachedDensityMap { density_map: None }
    }

    pub fn set(
        &mut self,
        tracker: &RequestDensityTracker,
        neighbor_locations: &BTreeMap<Location, Vec<Connection>>,
    ) -> Result<(), DensityMapError> {
        let density_map = tracker.create_density_map(neighbor_locations)?;
        self.density_map = Some(density_map);
        Ok(())
    }

    pub fn get(&self) -> Option<&DensityMap> {
        self.density_map.as_ref()
    }
}

#[derive(Error, Debug)]
pub(crate) enum DensityMapError {
    #[error("The neighbors BTreeMap is empty.")]
    EmptyNeighbors,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::RwLock;
    use tracing::debug;

    #[test]
    fn test_create_density_map() {
        let neighbors = RwLock::new(BTreeMap::new());
        neighbors
            .write()
            .unwrap()
            .insert(Location::new(0.2), vec![]);
        neighbors
            .write()
            .unwrap()
            .insert(Location::new(0.6), vec![]);

        let neighbors = neighbors.read();

        let mut sw = RequestDensityTracker::new(5);
        sw.sample(Location::new(0.21));
        sw.sample(Location::new(0.22));
        sw.sample(Location::new(0.23));
        sw.sample(Location::new(0.61));
        sw.sample(Location::new(0.62));

        let result = sw.create_density_map(&neighbors.unwrap());
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(
            result.neighbor_request_counts.get(&Location::new(0.2)),
            Some(&3)
        );
        assert_eq!(
            result.neighbor_request_counts.get(&Location::new(0.6)),
            Some(&2)
        );
    }

    #[test]
    fn test_wrap_around() {
        let mut sw = RequestDensityTracker::new(5);
        sw.sample(Location::new(0.21));
        sw.sample(Location::new(0.22));
        sw.sample(Location::new(0.23));
        sw.sample(Location::new(0.61));
        sw.sample(Location::new(0.62));

        let mut neighbors = BTreeMap::new();
        neighbors.insert(Location::new(0.6), vec![]);
        neighbors.insert(Location::new(0.9), vec![]);

        let result = sw.create_density_map(&neighbors);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(
            result.neighbor_request_counts.get(&Location::new(0.9)),
            Some(&3)
        );
        assert_eq!(
            result.neighbor_request_counts.get(&Location::new(0.6)),
            Some(&2)
        );
    }

    #[test]
    fn test_interpolate() {
        let mut sw = RequestDensityTracker::new(10);
        sw.sample(Location::new(0.19));
        sw.sample(Location::new(0.20));
        sw.sample(Location::new(0.21));
        sw.sample(Location::new(0.59));
        sw.sample(Location::new(0.60));

        let mut neighbors = BTreeMap::new();
        neighbors.insert(Location::new(0.2), vec![]);
        neighbors.insert(Location::new(0.6), vec![]);

        let result = sw.create_density_map(&neighbors);
        assert!(result.is_ok());
        let result = result.unwrap();

        // Scan and dumb densities 0.0 to 1.0 at 0.01 intervals
        debug!("Location\tDensity");
        for i in 0..100 {
            let location = Location::new(i as f64 / 100.0);
            let density = result.get_density_at(location).unwrap();
            // Print and round density to 2 decimals
            debug!(
                "{}\t{}",
                location.as_f64(),
                (density * 100.0).round() / 100.0
            );
        }

        assert_eq!(result.get_density_at(Location::new(0.2)).unwrap(), 3.0);
        assert_eq!(result.get_density_at(Location::new(0.6)).unwrap(), 2.0);
        assert_eq!(result.get_density_at(Location::new(0.4)).unwrap(), 2.5);
        assert_eq!(result.get_density_at(Location::new(0.5)).unwrap(), 2.25);
    }

    #[test]
    fn test_drop() {
        let mut sw = RequestDensityTracker::new(4);
        sw.sample(Location::new(0.21));
        sw.sample(Location::new(0.22));
        sw.sample(Location::new(0.23));
        sw.sample(Location::new(0.61));
        sw.sample(Location::new(0.62));

        let mut neighbors = BTreeMap::new();
        neighbors.insert(Location::new(0.2), vec![]);
        neighbors.insert(Location::new(0.6), vec![]);

        let result = sw.create_density_map(&neighbors);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(
            result.neighbor_request_counts.get(&Location::new(0.2)),
            Some(&2)
        );
        assert_eq!(
            result.neighbor_request_counts.get(&Location::new(0.6)),
            Some(&2)
        );
    }

    #[test]
    fn test_empty_neighbors_error() {
        let sw = RequestDensityTracker::new(10);
        let empty_neighbors = BTreeMap::new();
        matches!(
            sw.create_density_map(&empty_neighbors),
            Err(DensityMapError::EmptyNeighbors)
        );
    }

    #[test]
    fn test_get_max_density() {
        let mut density_map = DensityMap {
            neighbor_request_counts: BTreeMap::new(),
        };

        density_map
            .neighbor_request_counts
            .insert(Location::new(0.2), 1);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.6), 2);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.8), 2);

        let result = density_map.get_max_density();
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result, Location::new(0.7));
    }

    #[test]
    fn test_get_max_density_2() {
        let mut density_map = DensityMap {
            neighbor_request_counts: BTreeMap::new(),
        };

        density_map
            .neighbor_request_counts
            .insert(Location::new(0.2), 1);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.6), 2);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.8), 2);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.9), 1);

        let result = density_map.get_max_density();
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result, Location::new(0.7));
    }

    #[test]
    fn test_get_max_density_first_last() {
        let mut density_map = DensityMap {
            neighbor_request_counts: BTreeMap::new(),
        };

        density_map
            .neighbor_request_counts
            .insert(Location::new(0.0), 2);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.2), 1);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.6), 1);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.8), 1);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.9), 2);

        let result = density_map.get_max_density();
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result, Location::new(0.95));
    }

    #[test]
    fn test_get_max_density_first_last_2() {
        // Verify the other case in max_density_location calculation
        let mut density_map = DensityMap {
            neighbor_request_counts: BTreeMap::new(),
        };

        density_map
            .neighbor_request_counts
            .insert(Location::new(0.3), 2);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.4), 1);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.6), 1);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.8), 1);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.9), 2);

        let result = density_map.get_max_density();
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result, Location::new(0.1));
    }

    #[test]
    fn test_get_max_density_first_last_3() {
        // Verify the other case in max_density_location calculation
        let mut density_map = DensityMap {
            neighbor_request_counts: BTreeMap::new(),
        };

        density_map
            .neighbor_request_counts
            .insert(Location::new(0.1), 2);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.2), 1);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.3), 1);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.4), 1);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.7), 2);

        let result = density_map.get_max_density();
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result, Location::new(0.9));
    }

    #[test]
    fn test_get_max_density_empty_neighbors_error() {
        let density_map = DensityMap {
            neighbor_request_counts: BTreeMap::new(),
        };

        let result = density_map.get_max_density();
        assert!(matches!(result, Err(DensityMapError::EmptyNeighbors)));
    }

    #[test]
    fn test_weighted_density_closer_wins_at_equal_density() {
        // Two pairs with equal density — the one closer to my_location should win
        let mut density_map = DensityMap {
            neighbor_request_counts: BTreeMap::new(),
        };

        density_map
            .neighbor_request_counts
            .insert(Location::new(0.2), 3);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.4), 3); // midpoint 0.3
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.7), 3);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.9), 3); // midpoint 0.8

        let my_location = Location::new(0.25);
        let result = density_map.get_max_density_weighted(my_location).unwrap();
        // Midpoint 0.3 is at distance 0.05, midpoint 0.8 is at distance 0.45
        // Equal density (6 each), so closer one (0.3) should win
        assert_eq!(result, Location::new(0.3));
    }

    #[test]
    fn test_weighted_density_high_density_overcomes_distance() {
        // A distant pair with much higher density should beat a close pair with low density.
        // Carefully constructed so the wrap-around pair doesn't interfere:
        // first=0.1(1) and last=0.95(100) wrap midpoint is at 0.025, far from my_location=0.4.
        let mut density_map = DensityMap {
            neighbor_request_counts: BTreeMap::new(),
        };

        density_map
            .neighbor_request_counts
            .insert(Location::new(0.1), 1);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.45), 1); // midpoint 0.275, density=2
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.55), 1); // midpoint 0.5, density=2
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.95), 100); // midpoint 0.75, density=101

        let my_location = Location::new(0.4);
        let result = density_map.get_max_density_weighted(my_location).unwrap();
        // Close midpoint 0.5: score = 2/0.1 = 20
        // Distant midpoint 0.75: score = 101/0.35 ≈ 289
        // Wrap midpoint 0.025: score = 101/0.375 ≈ 269
        // High density at distance wins
        assert_eq!(result, Location::new(0.75));
    }

    #[test]
    fn test_weighted_density_empty_error() {
        let density_map = DensityMap {
            neighbor_request_counts: BTreeMap::new(),
        };

        let result = density_map.get_max_density_weighted(Location::new(0.5));
        assert!(matches!(result, Err(DensityMapError::EmptyNeighbors)));
    }

    #[test]
    fn test_weighted_density_single_entry() {
        // Single entry should return that entry's location (no pairs to compute midpoints)
        let mut density_map = DensityMap {
            neighbor_request_counts: BTreeMap::new(),
        };
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.7), 5);

        let result = density_map
            .get_max_density_weighted(Location::new(0.3))
            .unwrap();
        assert_eq!(result, Location::new(0.7));
    }

    #[test]
    fn test_weighted_density_wrap_around_wins() {
        // The wrap-around pair (first + last) should win when it has the best
        // density/distance ratio relative to my_location near the wrap boundary
        let mut density_map = DensityMap {
            neighbor_request_counts: BTreeMap::new(),
        };

        // Wrap-around pair: 0.05(50) and 0.95(50) → midpoint ≈ 0.0, density=100
        // Interior pair: 0.4(1) and 0.6(1) → midpoint 0.5, density=2
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.05), 50);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.4), 1);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.6), 1);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.95), 50);

        // my_location=0.0 is right at the wrap midpoint
        let my_location = Location::new(0.0);
        let result = density_map.get_max_density_weighted(my_location).unwrap();
        // Wrap midpoint is at ~0.0 (or 1.0 due to float precision at the boundary).
        // Both represent the same ring position. Score = 100/0.001 = 100000, dominates.
        assert!(
            my_location.distance(result).as_f64() < 0.01,
            "Expected wrap midpoint near 0.0, got {result}"
        );
    }

    #[test]
    fn test_weighted_density_min_distance_clamp() {
        // When my_location is extremely close to a midpoint, MIN_DISTANCE (0.001)
        // should clamp the distance, preventing infinite scores
        let mut density_map = DensityMap {
            neighbor_request_counts: BTreeMap::new(),
        };

        density_map
            .neighbor_request_counts
            .insert(Location::new(0.499), 1);
        density_map
            .neighbor_request_counts
            .insert(Location::new(0.501), 1); // midpoint = 0.5

        // my_location is essentially at the midpoint (distance < MIN_DISTANCE)
        let my_location = Location::new(0.5);
        let result = density_map.get_max_density_weighted(my_location);
        // Should succeed without panic or overflow
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Location::new(0.5));
    }
}
