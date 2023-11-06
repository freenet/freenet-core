pub mod cached_density_map;

#[cfg(test)]
mod tests;

use crate::ring::Location;
use std::collections::{BTreeMap, LinkedList};
use thiserror::Error;

/// Tracks requests sent by a node to its neighbors and creates a density map, which
/// is useful for determining which new neighbors to connect to based on their
/// location.
pub(super) struct RequestDensityTracker {
    ordered_map: BTreeMap<Location, usize>,
    list: LinkedList<Location>,
    window_size: usize,
    samples: usize,
}

impl RequestDensityTracker {
    pub fn new(window_size: usize) -> Self {
        Self {
            ordered_map: BTreeMap::new(),
            list: LinkedList::new(),
            window_size,
            samples: 0,
        }
    }

    pub fn sample(&mut self, value: Location) {
        self.samples += 1;

        self.list.push_back(value);
        *self.ordered_map.entry(value).or_insert(0) += 1;

        if self.list.len() > self.window_size {
            if let Some(oldest) = self.list.pop_front() {
                if let Some(count) = self.ordered_map.get_mut(&oldest) {
                    *count -= 1;
                    if *count == 0 {
                        self.ordered_map.remove(&oldest);
                    }
                }
            }
        }
    }

    pub fn create_density_map(
        &self,
        neighbors: &BTreeMap<Location, usize>,
    ) -> Result<DensityMap, DensityMapError> {
        debug_assert!(!neighbors.is_empty());
        if neighbors.is_empty() {
            return Err(DensityMapError::EmptyNeighbors);
        }

        let mut density_map = DensityMap {
            neighbor_request_counts: BTreeMap::new(),
        };

        for (sample_location, sample_count) in self.ordered_map.iter() {
            let previous_neighbor = neighbors
                .range(..*sample_location)
                .next_back()
                .or_else(|| neighbors.iter().next_back());
            let next_neighbor = neighbors
                .range(*sample_location..)
                .next()
                .or_else(|| neighbors.iter().next());

            match (previous_neighbor, next_neighbor) {
                (Some((previous_neighbor_location, _)), Some((next_neighbor_location, _))) => {
                    if sample_location.distance(*previous_neighbor_location)
                        < sample_location.distance(*next_neighbor_location)
                    {
                        *density_map
                            .neighbor_request_counts
                            .entry(*previous_neighbor_location)
                            .or_insert(0) += sample_count;
                    } else {
                        *density_map
                            .neighbor_request_counts
                            .entry(*next_neighbor_location)
                            .or_insert(0) += sample_count;
                    }
                }
                // The None cases have been removed as they should not occur given the new logic
                _ => unreachable!(
                    "This shouldn't be possible given that we verify neighbors is not empty"
                ),
            }
        }

        Ok(density_map)
    }
}

pub(super) struct DensityMap {
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
            _ => unreachable!(
                "This shouldn't be possible given that we verify neighbors is not empty"
            ),
        };

        Ok(count_estimate)
    }

    pub fn get_max_density(&self) -> Result<Location, DensityMapError> {
        if self.neighbor_request_counts.is_empty() {
            return Err(DensityMapError::EmptyNeighbors);
        }

        // Identify the midpoint Location between the pair of neighbors
        // with the highest combined request count
        let mut max_density_location = Location::new(0.0);
        let mut max_density = 0;

        for (
            (previous_neighbor_location, previous_neighbor_count),
            (next_neighbor_location, next_neighbor_count),
        ) in self
            .neighbor_request_counts
            .iter()
            .zip(self.neighbor_request_counts.iter().skip(1))
        {
            let combined_count = previous_neighbor_count + next_neighbor_count;
            if combined_count > max_density {
                max_density = combined_count;
                max_density_location = Location::new(
                    (previous_neighbor_location.as_f64() + next_neighbor_location.as_f64()) / 2.0,
                );
            }
        }

        // We need to also check the first and last neighbors as locations are circular
        let first_neighbor = self.neighbor_request_counts.iter().next();
        let last_neighbor = self.neighbor_request_counts.iter().next_back();
        if let (
            Some((first_neighbor_location, first_neighbor_count)),
            Some((last_neighbor_location, last_neighbor_count)),
        ) = (first_neighbor, last_neighbor)
        {
            let combined_count = first_neighbor_count + last_neighbor_count;
            if combined_count > max_density {
                // max_density = combined_count; Not needed as this is the last check
                let distance = first_neighbor_location.distance(*last_neighbor_location);
                let mut mp = first_neighbor_location.as_f64() - (distance.as_f64() / 2.0);
                if mp < 0.0 {
                    mp += 1.0;
                }
                max_density_location = Location::new(mp);
            }
        }

        Ok(max_density_location)
    }
}

#[derive(Error, Debug)]
pub(crate) enum DensityMapError {
    #[error("The neighbors BTreeMap is empty.")]
    EmptyNeighbors,
}
