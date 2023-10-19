use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// `ConnectionEvaluator` is used to evaluate connection scores within a specified time window.
///
/// The evaluator records scores and determines whether a given score is better (i.e., lower) than
/// any other scores within a predefined time window. A score is considered better if it's lower
/// than all other scores in the time window, or if no scores were recorded within the window's
/// duration.
/// 
/// In the Freenet context, this will be used to titrate the rate of new connection requests accepted
/// by a node. The node will only accept a new connection if the score of the connection is better
/// than all other scores within the time window. 
struct ConnectionEvaluator {
    scores: VecDeque<(Instant, f64)>,
    window_duration: Duration,
}

impl ConnectionEvaluator {
    pub fn new(window_duration: Duration) -> Self {
        ConnectionEvaluator {
            scores: VecDeque::new(),
            window_duration,
        }
    }

    pub fn record(&mut self, score: f64) -> bool {
        self.record_with_current_time(score, Instant::now())
    }

    fn record_with_current_time(&mut self, score: f64, current_time: Instant) -> bool {
        self.remove_outdated_scores(current_time);

        let is_better = self.scores.is_empty() || self.scores.iter().all(|&(_, s)| score < s);
        self.scores.push_back((current_time, score));

        is_better
    }

    fn remove_outdated_scores(&mut self, current_time: Instant) {
        while let Some(&(time, _)) = self.scores.front() {
            if current_time.duration_since(time) > self.window_duration {
                self.scores.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_first_score() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));
        assert_eq!(evaluator.record(5.0), true);
    }

    #[test]
    fn test_record_within_window_duration() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));
        
        let start_time = Instant::now();
        evaluator.record_with_current_time(5.0, start_time);
        assert_eq!(evaluator.record_with_current_time(6.0, start_time + Duration::from_secs(5)), false);
    }

    #[test]
    fn test_record_outside_window_duration() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));
        
        let start_time = Instant::now();
        evaluator.record_with_current_time(5.0, start_time);
        assert_eq!(evaluator.record_with_current_time(6.0, start_time + Duration::from_secs(11)), true);
    }

    #[test]
    fn test_remove_outdated_scores() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));
        
        let start_time = Instant::now();
        evaluator.record_with_current_time(5.0, start_time);
        evaluator.record_with_current_time(6.0, start_time + Duration::from_secs(5));
        evaluator.record_with_current_time(4.5, start_time + Duration::from_secs(11));
        assert_eq!(evaluator.scores.len(), 2);
    }
}
