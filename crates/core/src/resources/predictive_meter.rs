use std::collections::VecDeque;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use dashmap::DashMap;
use pav_regression::pav::{IsotonicRegression, Point};
use crate::resources::meter::{AttributionSource, Meter, ResourceType};

/// `PredictiveMeter` is used to predict the future usage of a resource based on its
/// current usage. It assumes that for new connections or other resources - the resource
/// usage will start at 0.0 and increase over time. It also assumes that the resource
/// usage will not decrease over time. This allows us to make decisions based on the
/// expected long-term usage rather than the current usage which may be misleading.
pub(super) struct PredictiveMeter {
    meter : RwLock<Meter>,
    samples : DashMap<ResourceType, DashMap<AttributionSource, UsageSamples>>,
    attribution_source_creation_time : DashMap<AttributionSource, Instant>,
    regression_curve_cache: RwLock<Option<(Instant, IsotonicRegression)>>,
}

impl PredictiveMeter {
    pub(super) fn new(meter : RwLock<Meter>) -> Self {
       todo!()
    }

    pub fn get_iso_regression(&mut self) {

    }
}

pub(super) struct UsageSamples {
    samples : VecDeque<Sample>,
    regression_curve_cache: RwLock<Option<(Instant, IsotonicRegression)>>,
}

pub struct Sample {
    pub time : Instant,
    pub value : f64,
    pub weight : f64,
}

impl UsageSamples {
    fn add_sample(&mut self, sample: Sample, max_samples: usize) {
        self.samples.push_back(sample);
        while self.samples.len() >= max_samples {
            self.samples.pop_front();
        }
    }

    fn get_iso_regression(&self, attribution_source_creation_time: Instant, current_time: Instant) -> IsotonicRegression {
        const MAX_CACHE_AGE: Duration = Duration::from_secs(10 * 60); // 10 minutes
        let old_curve_weight = 100.0;

        let mut cache = self.regression_curve_cache.write().unwrap();
        // If there is no cache or cache is too old, create a new one
        if cache.is_none() || cache.as_ref().unwrap().0.elapsed() > MAX_CACHE_AGE {
            let mut points: Vec<Point> =
                self.samples.iter()
                    .map(|sample| {
                        let time = sample.time.duration_since(attribution_source_creation_time).as_secs_f64();
                        let value = sample.value;
                        let weight = sample.weight;
                        Point::new_with_weight(time, value, weight)
                    }).collect();

            // If there is a cache, add the old curve to the new one with a high weight
            if let Some((_, regression)) = cache.as_ref() {
                for point in regression.get_points() {
                    points.push(Point::new_with_weight(point.x(), point.y(), old_curve_weight));
                }
                let mut regression = IsotonicRegression::new_ascending(points.as_slice());
                *cache = Some((current_time, regression.clone()));
            }
        }
        cache.unwrap().1
    }
}