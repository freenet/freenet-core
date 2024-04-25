use crate::topology::rate::Rate;
use std::time::Duration;
use std::{collections::VecDeque, time::Instant};

#[derive(Clone, Debug)]
pub(crate) struct RunningAverage {
    max_samples: usize,
    samples: VecDeque<(Instant, f64)>,
    sum_samples: f64,
    total_sample_count: usize,
}

impl RunningAverage {
    pub fn new(max_samples: usize) -> Self {
        RunningAverage {
            max_samples,
            samples: VecDeque::with_capacity(max_samples),
            sum_samples: 0.0,
            total_sample_count: 0,
        }
    }

    pub(crate) fn insert_with_time(&mut self, now: Instant, value: f64) {
        // Require that now is after the last sample time
        if let Some((last_sample_time, _)) = self.samples.back() {
            debug_assert!(now >= *last_sample_time);
        }
        self.total_sample_count += 1;
        if self.samples.len() < self.max_samples {
            self.samples.push_back((now, value));
            self.sum_samples += value;
        } else if let Some((_, old_value)) = self.samples.pop_front() {
            self.samples.push_back((now, value));
            self.sum_samples += value - old_value;
        }
    }

    pub(crate) fn get_rate_at_time(&self, now: Instant) -> Option<Rate> {
        if self.samples.is_empty() {
            return None;
        }
        let oldest_sample_time = self.samples.front().unwrap().0;
        let sample_duration = now - oldest_sample_time;
        const MINIMUM_TIME_WINDOW: Duration = Duration::from_secs(1);
        let divisor = sample_duration.max(MINIMUM_TIME_WINDOW);
        Some(Rate::new(self.sum_samples, divisor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert() {
        let max_samples = 3;
        let mut running_avg = RunningAverage::new(max_samples);
        let now = Instant::now();

        running_avg.insert_with_time(now, 2.0);
        assert_eq!(running_avg.samples.len(), 1);
        assert_eq!(running_avg.sum_samples, 2.0);

        running_avg.insert_with_time(now + Duration::from_secs(1), 4.0);
        assert_eq!(running_avg.samples.len(), 2);
        assert_eq!(running_avg.sum_samples, 6.0);

        running_avg.insert_with_time(now + Duration::from_secs(2), 6.0);
        assert_eq!(running_avg.samples.len(), 3);
        assert_eq!(running_avg.sum_samples, 12.0);

        // Test that the oldest value is removed when max_samples is exceeded
        running_avg.insert_with_time(now + Duration::from_secs(3), 8.0);
        assert_eq!(running_avg.samples.len(), 3);
        assert_eq!(running_avg.sum_samples, 18.0);
    }

    #[test]
    fn test_per_second_measurement() {
        let max_samples = 3;
        let mut running_avg = RunningAverage::new(max_samples);
        let now = Instant::now();

        // Test with no samples
        assert!(running_avg.get_rate_at_time(now).is_none());

        // Test with one sample
        running_avg.insert_with_time(now, 2.0);
        assert_eq!(
            running_avg
                .get_rate_at_time(now + Duration::from_secs(1))
                .unwrap()
                .per_second(),
            2.0
        );

        // Test with multiple samples
        running_avg.insert_with_time(now + Duration::from_secs(1), 4.0);
        running_avg.insert_with_time(now + Duration::from_secs(2), 6.0);
        assert_eq!(
            running_avg
                .get_rate_at_time(now + Duration::from_secs(3))
                .unwrap()
                .per_second(),
            4.0
        );

        // Test with max_samples exceeded
        running_avg.insert_with_time(now + Duration::from_secs(3), 8.0);
        assert_eq!(
            running_avg
                .get_rate_at_time(now + Duration::from_secs(4))
                .unwrap()
                .per_second(),
            6.0
        );
    }
}
