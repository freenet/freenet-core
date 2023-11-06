use crate::ring::Location;
use crate::topology::request_density_tracker::{self, DensityMapError};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

/// Struct to handle caching of DensityMap
pub(in crate::topology) struct CachedDensityMap {
    pub density_map: Option<(request_density_tracker::DensityMap, Instant)>,
    regenerate_interval: Duration,
}

impl CachedDensityMap {
    pub fn new(regenerate_interval: Duration) -> Self {
        CachedDensityMap {
            density_map: None,
            regenerate_interval,
        }
    }

    pub fn create(
        &mut self,
        tracker: &request_density_tracker::RequestDensityTracker,
        current_neighbors: &BTreeMap<Location, usize>,
    ) -> Result<(), DensityMapError> {
        let now = Instant::now();
        let density_map = tracker.create_density_map(current_neighbors)?;
        self.density_map = Some((density_map, now));
        Ok(())
    }

    pub fn get(&self) -> Option<&request_density_tracker::DensityMap> {
        let now = Instant::now();
        self.density_map
            .as_ref()
            .and_then(|(density_map, last_update)| {
                if now.duration_since(*last_update) < self.regenerate_interval {
                    Some(density_map)
                } else {
                    None
                }
            })
    }
}
