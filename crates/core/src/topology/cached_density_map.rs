use std::{time::{Duration, Instant}, collections::BTreeMap, rc::Rc};
use crate::ring::Location;
use super::request_density_tracker::{self, DensityMapError};

/// Struct to handle caching of DensityMap
pub(super) struct CachedDensityMap {
    density_map: Option<(Rc<request_density_tracker::DensityMap>, Instant)>,
    regenerate_interval: Duration,
}

impl CachedDensityMap {
    pub(super) fn new(regenerate_interval: Duration) -> Self {
        CachedDensityMap {
            density_map: None,
            regenerate_interval,
        }
    }

    pub(super) fn get_or_create(&mut self, tracker: &request_density_tracker::RequestDensityTracker, current_neighbors: &BTreeMap<Location, usize>) -> Result<Rc<request_density_tracker::DensityMap>, DensityMapError> {
        let now = Instant::now();
        if let Some((density_map, last_update)) = &self.density_map {
            if now.duration_since(*last_update) < self.regenerate_interval {
                return Ok(density_map.clone());
            }
        }

        let density_map = Rc::new(tracker.create_density_map(current_neighbors)?);
        self.density_map = Some((density_map.clone(), now));

        Ok(density_map)
    }
}
