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
pub(crate) struct ConnectionEvaluator {
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

    pub fn record_only(&mut self, score: f64) {
        self.record_only_with_current_time(score, Instant::now());
    }

    pub fn record_only_with_current_time(&mut self, score: f64, current_time: Instant) {
        self.remove_outdated_scores(current_time);
        self.scores.push_back((current_time, score));
    }

    pub fn record_and_eval(&mut self, score: f64) -> bool {
        self.record_and_eval_with_current_time(score, Instant::now())
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
mod tests;
