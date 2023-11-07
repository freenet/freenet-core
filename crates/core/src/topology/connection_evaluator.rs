use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// `ConnectionEvaluator` is used to evaluate connection scores within a specified time window.
///
/// The evaluator records scores and determines whether a given score is better (higher) than
/// any other scores within a predefined time window. A score is considered better if it's higher
/// than all other scores in the time window, or if no scores were recorded within the window's
/// duration.
///
/// In the Freenet context, this will be used to titrate the rate of new connection requests accepted
/// by a node. The node will only accept a new connection if the score of the connection is better
/// than all other scores within the time window.
pub(super) struct ConnectionEvaluator {
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

    pub fn record_only_with_current_time(&mut self, score: f64, current_time: Instant) {
        self.remove_outdated_scores(current_time);
        self.scores.push_back((current_time, score));
    }

    pub fn record_and_eval_with_current_time(&mut self, score: f64, current_time: Instant) -> bool {
        self.remove_outdated_scores(current_time);

        let is_better = self.scores.is_empty() || self.scores.iter().all(|&(_, s)| score > s);

        // Important to add new score *after* checking if it's better than all other scores
        self.record_only_with_current_time(score, current_time);

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
        let current_time = Instant::now();
        assert!(evaluator.record_and_eval_with_current_time(5.0, current_time));
        // Assert evaluator.scores contains the new score
        assert_eq!(evaluator.scores.len(), 1);
        assert_eq!(evaluator.scores[0].1, 5.0);
        assert_eq!(evaluator.scores[0].0, current_time);
    }

    #[test]
    fn test_not_best_in_time_window() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));

        let start_time = Instant::now();
        evaluator.record_and_eval_with_current_time(5.0, start_time);
        assert!(
            !evaluator.record_and_eval_with_current_time(4.0, start_time + Duration::from_secs(5)),
        );
    }

    #[test]
    fn test_best_in_time_window() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));

        let start_time = Instant::now();
        evaluator.record_and_eval_with_current_time(5.0, start_time);
        assert!(
            evaluator.record_and_eval_with_current_time(4.0, start_time + Duration::from_secs(11)),
        );
    }

    #[test]
    fn test_remove_outdated_scores() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));

        let start_time = Instant::now();
        evaluator.record_and_eval_with_current_time(5.0, start_time);
        evaluator.record_and_eval_with_current_time(6.0, start_time + Duration::from_secs(5));
        evaluator.record_and_eval_with_current_time(4.5, start_time + Duration::from_secs(11));
        assert_eq!(evaluator.scores.len(), 2);
    }

    #[test]
    fn test_empty_window_duration() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(0));
        let current_time = Instant::now();
        assert!(evaluator.record_and_eval_with_current_time(5.0, current_time));
        assert!(!evaluator.record_and_eval_with_current_time(4.0, current_time));
    }

    #[test]
    fn test_multiple_scores_same_timestamp() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));
        let current_time = Instant::now();
        evaluator.record_only_with_current_time(5.0, current_time);
        evaluator.record_only_with_current_time(6.0, current_time);
        assert_eq!(evaluator.scores.len(), 2);
        assert!(!evaluator
            .record_and_eval_with_current_time(4.0, current_time + Duration::from_secs(5)),);
    }

    #[test]
    fn test_negative_scores() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));
        let start_time = Instant::now();
        assert!(evaluator.record_and_eval_with_current_time(-5.0, start_time),);
        assert!(
            evaluator.record_and_eval_with_current_time(-4.0, start_time + Duration::from_secs(5)),
        );
        assert!(
            !evaluator.record_and_eval_with_current_time(-6.0, start_time + Duration::from_secs(5)),
        );
    }

    #[test]
    fn test_large_number_of_scores() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));
        let start_time = Instant::now();
        for i in 0..1000 {
            evaluator.record_only_with_current_time(i as f64, start_time + Duration::from_secs(i));
        }
        assert!(evaluator
            .record_and_eval_with_current_time(1000.0, start_time + Duration::from_secs(1001)),);
    }
}
