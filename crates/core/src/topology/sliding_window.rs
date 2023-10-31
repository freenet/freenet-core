use std::collections::{BTreeMap, LinkedList};

use crate::ring::{Location, Distance};

struct SlidingWindow {
    ordered_map: BTreeMap<Location, usize>,
    list: LinkedList<Location>,
    window_size: usize,
}

impl SlidingWindow {
    pub fn new(window_size: usize) -> Self {
        Self {
            ordered_map: BTreeMap::new(),
            list: LinkedList::new(),
            window_size,
        }
    }

    pub fn sample(&mut self, value: Location) {
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

    pub fn density(&self, position: Location, window_radius: usize) -> Option<Distance> {
        // Create ranges for lower and upper bounds
        let lower_range = (std::ops::Bound::Unbounded::<Location>, std::ops::Bound::Included(position));
        let upper_range = (std::ops::Bound::Excluded(position), std::ops::Bound::Unbounded::<Location>);

        let lower_iter = self.ordered_map.range(lower_range).rev();
        // Lower iter size is the total of the values in lower_iter
        let lower_iter_sz = lower_iter.map(|(_, v)| v).sum::<usize>();

        let lower_iter = self.ordered_map.range(lower_range).rev();

        let lower_bound : Option<Location> = if lower_iter_sz >= window_radius {
            // Fold over lower_iter through window_radius sample Locations to obtain the lower bound sample Location,
            // bearing in mind that the map entry values are the number of times that the sample Location has been sampled
    
            let mut count = 0;
            let mut lower_bound = None;
            for (sample_location, sample_count) in lower_iter {
                count += sample_count;
                if count >= window_radius {
                    lower_bound = Some(*sample_location);
                    break;
                }
            }
            lower_bound
        } else {
            // Iterate down from the top of the map accumulating the count to obtain the lower bound after window_radius-lower_iter_sz samples
            let mut count = lower_iter_sz;
            let mut lower_bound = None;
            for (sample_location, sample_count) in self.ordered_map.iter().rev() {
                count += sample_count;
                if count >= window_radius {
                    lower_bound = Some(*sample_location);
                    break;
                }
            }
            lower_bound
        };

        // Now determine the upper_bound in the same way but moving forward rather than reverse
        let upper_iter = self.ordered_map.range(upper_range);
        let upper_iter_sz = upper_iter.map(|(_, v)| v).sum::<usize>();
        let upper_iter = self.ordered_map.range(upper_range);

        let upper_bound : Option<Location> = if upper_iter_sz >= window_radius {
            // Fold over upper_iter through window_radius sample Locations to obtain the upper bound sample Location,
            // bearing in mind that the map entry values are the number of times that the sample Location has been sampled
    
            let mut count = 0;
            let mut upper_bound = None;
            for (sample_location, sample_count) in upper_iter {
                count += sample_count;
                if count >= window_radius {
                    upper_bound = Some(*sample_location);
                    break;
                }
            }
            upper_bound
        } else {
            // Iterate down from the top of the map accumulating the count to obtain the upper bound after window_radius-upper_iter_sz samples
            let mut count = upper_iter_sz;
            let mut upper_bound = None;
            for (sample_location, sample_count) in self.ordered_map.iter() {
                count += sample_count;
                if count >= window_radius {
                    upper_bound = Some(*sample_location);
                    break;
                }
            }
            upper_bound
        };

        // Now we have the lower and upper bounds, we can calculate the density which is
        // the distance between them
        return match (lower_bound, upper_bound) {
            (Some(lower), Some(upper)) => Some(lower.distance(upper)),
            _ => None,
        }

    }    
    
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sliding_window() {
        let mut sw = SlidingWindow::new(5);

        sw.sample(Location::new(0.0));
        sw.sample(Location::new(0.1));
        sw.sample(Location::new(0.2));
        sw.sample(Location::new(0.3));
        sw.sample(Location::new(0.4));
        // Previous samples should have been evicted
        sw.sample(Location::new(0.5));
        sw.sample(Location::new(0.6)); // <-- bottom of range
                                       // <-- position
        sw.sample(Location::new(0.7)); // <-- top of range
        sw.sample(Location::new(0.8));
        sw.sample(Location::new(0.9));

        // Verify that there are now only 5 samples
        assert_eq!(sw.ordered_map.len(), 5);

        // Verify that 0.4 was evicted
        assert_eq!(sw.ordered_map.contains_key(&Location::new(0.4)), false);

        // Verify density
        assert_eq!(sw.density(Location::new(0.65), 1), Some(Location::new(0.6).distance(Location::new(0.7))));
    }

    #[test]
    fn test_sliding_window_overlap() {
        let mut sw = SlidingWindow::new(5);

        sw.sample(Location::new(0.0));
        sw.sample(Location::new(0.1));
        sw.sample(Location::new(0.2));
        sw.sample(Location::new(0.3));
        sw.sample(Location::new(0.4));
        // Previous samples should have been evicted
        sw.sample(Location::new(0.5));
                                       // <-- position
        sw.sample(Location::new(0.6));
        sw.sample(Location::new(0.7)); // <-- top of range
        sw.sample(Location::new(0.8));
        sw.sample(Location::new(0.9)); // <-- bottom of range

        assert_eq!(sw.density(Location::new(0.55), 2), Some(Location::new(0.9).distance(Location::new(0.7))));
    }

    #[test]
    fn test_duplicate_locations() {
        let mut sw = SlidingWindow::new(5);

        sw.sample(Location::new(0.0));
        sw.sample(Location::new(0.1));
        sw.sample(Location::new(0.1));
        sw.sample(Location::new(0.3));
        sw.sample(Location::new(0.4));
        // Previous samples should have been evicted
        sw.sample(Location::new(0.6)); // <-- bottom of range
        sw.sample(Location::new(0.6));
                                       // <-- position
        sw.sample(Location::new(0.7));
        sw.sample(Location::new(0.8)); // <-- top of range
        sw.sample(Location::new(0.9));

        // Verify 0.4 was evicted
        assert_eq!(sw.ordered_map.contains_key(&Location::new(0.4)), false);

        // Verify density
        assert_eq!(sw.density(Location::new(0.65), 2), Some(Location::new(0.6).distance(Location::new(0.8))));
    }
}