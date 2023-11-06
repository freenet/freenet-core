use crate::ring::Location;
use crate::topology::request_density_tracker::{self, DensityMapError};
use std::collections::BTreeMap;

/// Struct to handle caching of DensityMap
pub(in crate::topology) struct CachedDensityMap {
    pub density_map: Option<request_density_tracker::DensityMap>,
}

impl CachedDensityMap {
    pub fn new() -> Self {
        CachedDensityMap { density_map: None }
    }

    pub fn set(
        &mut self,
        tracker: &request_density_tracker::RequestDensityTracker,
        current_neighbors: &BTreeMap<Location, usize>,
    ) -> Result<(), DensityMapError> {
        let density_map = tracker.create_density_map(current_neighbors)?;
        self.density_map = Some(density_map);
        Ok(())
    }

    pub fn get(&self) -> Option<&request_density_tracker::DensityMap> {
        self.density_map.as_ref()
    }
}
