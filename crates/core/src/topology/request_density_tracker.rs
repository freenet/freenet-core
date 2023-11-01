use std::collections::{BTreeMap, LinkedList};
use thiserror::Error;
use crate::ring::{Location, Distance};

/// Tracks requests sent by a node to its neighbors and creates a density map, which
/// is useful for determining which new neighbors to connect to based on their
/// location.
pub(crate) struct RequestDensityTracker {
    ordered_map: BTreeMap<Location, usize>,
    list: LinkedList<Location>,
    window_size: usize,
    samples: usize,
}

impl RequestDensityTracker {
    pub(crate) fn new(window_size: usize) -> Self {
        Self {
            ordered_map: BTreeMap::new(),
            list: LinkedList::new(),
            window_size,
            samples: 0,
        }
    }

    pub(crate) fn sample(&mut self, value: Location) {
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

    pub(crate) fn create_density_map(&self, neighbors: &BTreeMap<Location, usize>) -> Result<DensityMap, DensityMapError> {
        if neighbors.is_empty() {
            return Err(DensityMapError::EmptyNeighbors);
        }
    
        let smoothing_radius = 2;
        let mut density_map = DensityMap {
            neighbor_request_counts: BTreeMap::new(),
        };
    
        for (sample_location, sample_count) in self.ordered_map.iter() {
            let previous_neighbor = neighbors.range(..*sample_location).rev().next()
                .or_else(|| neighbors.iter().rev().next());
            let next_neighbor = neighbors.range(*sample_location..).next()
                .or_else(|| neighbors.iter().next());
    
                match (previous_neighbor, next_neighbor) {
                    (Some((previous_neighbor_location, _)), Some((next_neighbor_location, _))) => {
                        if sample_location.distance(*previous_neighbor_location) < sample_location.distance(*next_neighbor_location) {
                            *density_map.neighbor_request_counts.entry(*previous_neighbor_location).or_insert(0) += sample_count;
                        } else {
                            *density_map.neighbor_request_counts.entry(*next_neighbor_location).or_insert(0) += sample_count;
                        }
                    },
                    // The None cases have been removed as they should not occur given the new logic
                    _ => unreachable!("This shouldn't be possible given that we verify neighbors is not empty"),
                }
        }
    
        Ok(density_map)
    }
}

pub(crate) struct DensityMap {
    neighbor_request_counts: BTreeMap<Location, usize>,
}

impl DensityMap {
    pub(crate) fn get_density_at(&self, location: Location) -> Result<f64, DensityMapError> {
        if self.neighbor_request_counts.is_empty() {
            return Err(DensityMapError::EmptyNeighbors);
        }

        // Determine the locations below and above the given location
        let previous_neighbor = self.neighbor_request_counts.range(..location).rev().next()
            .or_else(|| self.neighbor_request_counts.iter().rev().next());
        
        let next_neighbor = self.neighbor_request_counts.range(location..).next()
            .or_else(|| self.neighbor_request_counts.iter().next());

        // Determine the value proportionate to the distance to the previous and next neighbor
        let count_estimate = match (previous_neighbor, next_neighbor) {
            (Some((previous_neighbor_location, previous_neighbor_count)), Some((next_neighbor_location, next_neighbor_count))) => {
                let previous_neighbor_dist = location.distance(*previous_neighbor_location);
                let next_neighbor_dist = location.distance(*next_neighbor_location);
                let total_dist = Distance::new(previous_neighbor_dist.as_f64() + next_neighbor_dist.as_f64());
                let previous_neighbor_prop = previous_neighbor_dist.as_f64() / total_dist.as_f64();
                let next_neighbor_prop = next_neighbor_dist.as_f64() / total_dist.as_f64();
                previous_neighbor_prop * *previous_neighbor_count as f64 + next_neighbor_prop * *next_neighbor_count as f64
            },
            // The None cases have been removed as they should not occur given the new logic
            _ => unreachable!("This shouldn't be possible given that we verify neighbors is not empty"),
        };

        Ok(count_estimate)
    }

    pub(crate) fn get_max_density(&self) -> Result<Location, DensityMapError> {
        if self.neighbor_request_counts.is_empty() {
            return Err(DensityMapError::EmptyNeighbors);
        }

        // Identify the midpoint Location between the pair of neighbors
        // with the highest combined request count
        let mut max_density_location = Location::new(0.0);
        let mut max_density = 0;

        for (
            (previous_neighbor_location, previous_neighbor_count), (next_neighbor_location, next_neighbor_count)) 
            in 
            self.neighbor_request_counts.iter().zip(self.neighbor_request_counts.iter().skip(1)) {
            let combined_count = previous_neighbor_count + next_neighbor_count;
            if combined_count > max_density {
                max_density = combined_count;
                max_density_location = Location::new((previous_neighbor_location.as_f64() + next_neighbor_location.as_f64()) / 2.0);
            }
        }

        // We need to also check the first and last neighbors as locations are circular
        let first_neighbor = self.neighbor_request_counts.iter().next();
        let last_neighbor = self.neighbor_request_counts.iter().rev().next();
        if let (Some((first_neighbor_location, first_neighbor_count)), Some((last_neighbor_location, last_neighbor_count))) = (first_neighbor, last_neighbor) {
            let combined_count = first_neighbor_count + last_neighbor_count;
            if combined_count > max_density {
                max_density = combined_count;
                let distance = first_neighbor_location.distance(*last_neighbor_location);
                let mut mp = first_neighbor_location.as_f64() - (distance.as_f64()/2.0);
                if mp < 0.0 {
                    mp += 1.0;
                }
                max_density_location = Location::new(mp);
            }
        }

        Ok(max_density_location)
    }
}

// Define the custom error type using thiserror
#[derive(Error, Debug)]
pub enum DensityError {
    #[error("Not enough samples to determine lower and upper bounds")]
    CantFindBounds,

    #[error("Window radius too big. Window radius should be <= 50% of the number of samples ({samples}) and window size ({window_size}).")]
    WindowTooBig {
        samples: usize,
        window_size: usize,
    },
}

#[derive(Error, Debug)]
pub enum DensityMapError {
    #[error("The neighbors BTreeMap is empty.")]
    EmptyNeighbors,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_density_map() {
        let mut sw = RequestDensityTracker::new(5);
        sw.sample(Location::new(0.21));
        sw.sample(Location::new(0.22));
        sw.sample(Location::new(0.23));
        sw.sample(Location::new(0.61));
        sw.sample(Location::new(0.62));

        let mut neighbors = BTreeMap::new();
        neighbors.insert(Location::new(0.2), 1);
        neighbors.insert(Location::new(0.6), 1);

        let result = sw.create_density_map(&neighbors);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.neighbor_request_counts.get(&Location::new(0.2)), Some(&3));
        assert_eq!(result.neighbor_request_counts.get(&Location::new(0.6)), Some(&2));
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
        neighbors.insert(Location::new(0.6), 1);
        neighbors.insert(Location::new(0.9), 1);

        let result = sw.create_density_map(&neighbors);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.neighbor_request_counts.get(&Location::new(0.9)), Some(&3));
        assert_eq!(result.neighbor_request_counts.get(&Location::new(0.6)), Some(&2));
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
        neighbors.insert(Location::new(0.2), 1);
        neighbors.insert(Location::new(0.6), 1);

        let result = sw.create_density_map(&neighbors);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.neighbor_request_counts.get(&Location::new(0.2)), Some(&2));
        assert_eq!(result.neighbor_request_counts.get(&Location::new(0.6)), Some(&2));
    }

    #[test]
    fn test_empty_neighbors_error() {
        let sw = RequestDensityTracker::new(10);
        let empty_neighbors = BTreeMap::new();
        let result = sw.create_density_map(&empty_neighbors);
        assert!(matches!(result, Err(DensityMapError::EmptyNeighbors)));
    }

    #[test]
    fn test_get_max_density() {
        let mut density_map = DensityMap {
            neighbor_request_counts: BTreeMap::new(),
        };

        density_map.neighbor_request_counts.insert(Location::new(0.2), 1);
        density_map.neighbor_request_counts.insert(Location::new(0.6), 2);
        density_map.neighbor_request_counts.insert(Location::new(0.8), 2);

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

        density_map.neighbor_request_counts.insert(Location::new(0.2), 1);
        density_map.neighbor_request_counts.insert(Location::new(0.6), 2);
        density_map.neighbor_request_counts.insert(Location::new(0.8), 2);
        density_map.neighbor_request_counts.insert(Location::new(0.9), 1);

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

        density_map.neighbor_request_counts.insert(Location::new(0.0), 2);
        density_map.neighbor_request_counts.insert(Location::new(0.2), 1);
        density_map.neighbor_request_counts.insert(Location::new(0.6), 1);
        density_map.neighbor_request_counts.insert(Location::new(0.8), 1);
        density_map.neighbor_request_counts.insert(Location::new(0.9), 2);

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

        density_map.neighbor_request_counts.insert(Location::new(0.3), 2);
        density_map.neighbor_request_counts.insert(Location::new(0.4), 1);
        density_map.neighbor_request_counts.insert(Location::new(0.6), 1);
        density_map.neighbor_request_counts.insert(Location::new(0.8), 1);
        density_map.neighbor_request_counts.insert(Location::new(0.9), 2);

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

        density_map.neighbor_request_counts.insert(Location::new(0.1), 2);
        density_map.neighbor_request_counts.insert(Location::new(0.2), 1);
        density_map.neighbor_request_counts.insert(Location::new(0.3), 1);
        density_map.neighbor_request_counts.insert(Location::new(0.4), 1);
        density_map.neighbor_request_counts.insert(Location::new(0.7), 2);

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
}
