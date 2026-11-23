use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::Instant;

/// `ConnectionEvaluator` is used to evaluate connection scores within a specified time window.
///
/// The evaluator records scores and determines whether a given score is better (higher) than
/// the other candidates' scores within a predefined time window. A score is considered better if
/// it's higher than every other candidate's score in the time window, or if no other candidate
/// was recorded within the window's duration.
///
/// In the Freenet context, this will be used to titrate the rate of new connection requests accepted
/// by a node. The node will only accept a new connection if the score of the connection is better
/// than all other candidates' scores within the time window.
///
/// Each candidate (by address) holds at most one entry, its latest score. A
/// candidate is compared only with the OTHER candidates' entries, and its
/// retry replaces its own. Before this, a rejected candidate's score stayed in
/// the window, and its retry, carrying the same score, could never be strictly
/// greater than it: a joiner retrying one node every 25-30 s was refused there
/// for as long as it kept retrying, with no other candidate involved.
pub(super) struct ConnectionEvaluator {
    /// Oldest first, at most one entry per candidate, at most
    /// [`MAX_TRACKED_CANDIDATES`] entries.
    scores: VecDeque<(Instant, SocketAddr, f64)>,
    window_duration: Duration,
}

/// Most candidates the window holds. Candidates are peer addresses, which a
/// remote party can vary, so the window is bounded by count as well as by
/// time. Past the cap the LOWEST-scoring entry is dropped: the bar a
/// candidate must beat is the highest other entry, and dropping the lowest
/// never lowers it, so a flood of low-scoring evaluations cannot switch the
/// rate limit off. (Dropping the oldest instead would let such a flood push
/// the strong entries out.)
pub(super) const MAX_TRACKED_CANDIDATES: usize = 1024;

impl ConnectionEvaluator {
    pub fn new(window_duration: Duration) -> Self {
        ConnectionEvaluator {
            scores: VecDeque::new(),
            window_duration,
        }
    }

    pub fn record_only_with_current_time(
        &mut self,
        candidate: SocketAddr,
        score: f64,
        current_time: Instant,
    ) {
        self.remove_outdated_scores(current_time);
        // The new entry is the latest, so replacing at the back keeps the
        // order `remove_outdated_scores` relies on.
        self.scores.retain(|(_, addr, _)| *addr != candidate);
        self.scores.push_back((current_time, candidate, score));
        while self.scores.len() > MAX_TRACKED_CANDIDATES {
            // The first of the lowest scores, so ties drop the oldest.
            // Removing from the middle keeps the time order.
            let lowest = self
                .scores
                .iter()
                .enumerate()
                .fold(None::<(usize, f64)>, |low, (i, &(_, _, s))| match low {
                    Some((_, l)) if l <= s => low,
                    _ => Some((i, s)),
                })
                .map(|(i, _)| i);
            match lowest {
                Some(i) => {
                    self.scores.remove(i);
                    crate::transport::metrics::TRANSPORT_METRICS.record_connect_evaluator(
                        crate::transport::metrics::EvaluatorEvent::CapEviction,
                    );
                }
                None => break,
            }
        }
    }

    pub fn record_and_eval_with_current_time(
        &mut self,
        candidate: SocketAddr,
        score: f64,
        current_time: Instant,
    ) -> bool {
        self.remove_outdated_scores(current_time);

        let is_better = self
            .scores
            .iter()
            .filter(|(_, addr, _)| *addr != candidate)
            .all(|&(_, _, s)| score > s);
        if is_better
            && self
                .scores
                .iter()
                .any(|&(_, addr, s)| addr == candidate && score <= s)
        {
            // Before #6347 this candidate's own earlier entry refused it.
            crate::transport::metrics::TRANSPORT_METRICS.record_connect_evaluator(
                crate::transport::metrics::EvaluatorEvent::AcceptedOverOwnEntry,
            );
        }

        // Important to add new score *after* checking if it's better than all other scores
        self.record_only_with_current_time(candidate, score, current_time);

        is_better
    }

    fn remove_outdated_scores(&mut self, current_time: Instant) {
        while let Some(&(time, _, _)) = self.scores.front() {
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

    fn peer(n: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], n))
    }

    #[test]
    fn test_record_first_score() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));
        let current_time = Instant::now();
        assert!(evaluator.record_and_eval_with_current_time(peer(1), 5.0, current_time));
        // Assert evaluator.scores contains the new score
        assert_eq!(evaluator.scores.len(), 1);
        assert_eq!(evaluator.scores[0].2, 5.0);
        assert_eq!(evaluator.scores[0].0, current_time);
    }

    #[test]
    fn test_not_best_in_time_window() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));

        let start_time = Instant::now();
        evaluator.record_and_eval_with_current_time(peer(1), 5.0, start_time);
        assert!(!evaluator.record_and_eval_with_current_time(
            peer(2),
            4.0,
            start_time + Duration::from_secs(5)
        ));
    }

    #[test]
    fn test_best_in_time_window() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));

        let start_time = Instant::now();
        evaluator.record_and_eval_with_current_time(peer(1), 5.0, start_time);
        assert!(evaluator.record_and_eval_with_current_time(
            peer(2),
            4.0,
            start_time + Duration::from_secs(11)
        ));
    }

    #[test]
    fn test_remove_outdated_scores() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));

        let start_time = Instant::now();
        evaluator.record_and_eval_with_current_time(peer(1), 5.0, start_time);
        evaluator.record_and_eval_with_current_time(
            peer(2),
            6.0,
            start_time + Duration::from_secs(5),
        );
        evaluator.record_and_eval_with_current_time(
            peer(3),
            4.5,
            start_time + Duration::from_secs(11),
        );
        assert_eq!(evaluator.scores.len(), 2);
    }

    #[test]
    fn test_empty_window_duration() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(0));
        let current_time = Instant::now();
        assert!(evaluator.record_and_eval_with_current_time(peer(1), 5.0, current_time));
        assert!(!evaluator.record_and_eval_with_current_time(peer(2), 4.0, current_time));
    }

    #[test]
    fn test_multiple_scores_same_timestamp() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));
        let current_time = Instant::now();
        evaluator.record_only_with_current_time(peer(1), 5.0, current_time);
        evaluator.record_only_with_current_time(peer(2), 6.0, current_time);
        assert_eq!(evaluator.scores.len(), 2);
        assert!(!evaluator.record_and_eval_with_current_time(
            peer(3),
            4.0,
            current_time + Duration::from_secs(5)
        ));
    }

    #[test]
    fn test_negative_scores() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));
        let start_time = Instant::now();
        assert!(evaluator.record_and_eval_with_current_time(peer(1), -5.0, start_time));
        assert!(evaluator.record_and_eval_with_current_time(
            peer(2),
            -4.0,
            start_time + Duration::from_secs(5)
        ));
        assert!(!evaluator.record_and_eval_with_current_time(
            peer(3),
            -6.0,
            start_time + Duration::from_secs(5)
        ));
    }

    #[test]
    fn test_large_number_of_scores() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));
        let start_time = Instant::now();
        for i in 0..1000u16 {
            evaluator.record_only_with_current_time(
                peer(i),
                i as f64,
                start_time + Duration::from_secs(i as u64),
            );
        }
        assert!(evaluator.record_and_eval_with_current_time(
            peer(1000),
            1000.0,
            start_time + Duration::from_secs(1001)
        ));
    }

    /// The stranded-holder simulation: a joiner refused once retried the same
    /// node every 25-30 s with the same score. Each retry was compared with its
    /// own earlier entry, so it was refused for as long as it kept retrying,
    /// with no other candidate in the window.
    #[test]
    fn a_retry_inside_the_window_is_not_refused_by_its_own_earlier_score() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(60));
        let start = Instant::now();
        // Refused the first time: a better candidate is in the window.
        evaluator.record_only_with_current_time(peer(1), 0.9, start);
        assert!(!evaluator.record_and_eval_with_current_time(
            peer(2),
            0.5,
            start + Duration::from_secs(30)
        ));
        // The better candidate's entry ages out at t=61; the joiner's own
        // (t=30) is still inside the window when it retries at t=62.
        assert!(
            evaluator.record_and_eval_with_current_time(
                peer(2),
                0.5,
                start + Duration::from_secs(62)
            ),
            "the retry ties only with its own earlier score"
        );
    }

    #[test]
    fn a_retry_is_still_compared_with_the_other_candidates() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(60));
        let start = Instant::now();
        evaluator.record_and_eval_with_current_time(peer(2), 0.5, start);
        evaluator.record_only_with_current_time(peer(1), 0.9, start + Duration::from_secs(10));
        assert!(
            !evaluator.record_and_eval_with_current_time(
                peer(2),
                0.5,
                start + Duration::from_secs(20)
            ),
            "a better other candidate in the window still wins"
        );
    }

    #[test]
    fn a_better_candidate_still_wins_and_a_worse_one_still_loses() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(60));
        let start = Instant::now();
        evaluator.record_and_eval_with_current_time(peer(1), 0.5, start);
        assert!(evaluator.record_and_eval_with_current_time(
            peer(2),
            0.6,
            start + Duration::from_secs(1)
        ));
        assert!(!evaluator.record_and_eval_with_current_time(
            peer(3),
            0.55,
            start + Duration::from_secs(2)
        ));
    }

    /// Two distinct candidates with the same score refuse each other: the
    /// comparison is strict. Peers sharing a /24 (or one peer changing ports)
    /// derive the same location and so the same score.
    #[test]
    fn distinct_candidates_with_equal_scores_refuse_each_other() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(60));
        let start = Instant::now();
        assert!(evaluator.record_and_eval_with_current_time(peer(1), 0.5, start));
        assert!(!evaluator.record_and_eval_with_current_time(
            peer(2),
            0.5,
            start + Duration::from_secs(1)
        ));
    }

    /// A flood of more than the cap of low-scoring evaluations inside one
    /// window must not push out the strong entry that sets the bar: the cap
    /// drops the lowest entries, never the highest.
    #[test]
    fn a_flood_of_low_scores_does_not_lower_the_bar() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(300));
        let start = Instant::now();
        evaluator.record_only_with_current_time(peer(1), 0.9, start);
        for i in 0..(MAX_TRACKED_CANDIDATES + 100) {
            evaluator.record_only_with_current_time(
                SocketAddr::from(([10, 1, (i >> 8) as u8, (i & 0xff) as u8], 2)),
                0.1,
                start + Duration::from_millis(1 + i as u64),
            );
        }
        assert_eq!(evaluator.scores.len(), MAX_TRACKED_CANDIDATES);
        assert!(
            !evaluator.record_and_eval_with_current_time(
                peer(3),
                0.5,
                start + Duration::from_secs(10)
            ),
            "0.5 must still lose to the 0.9 entry the flood came after"
        );
    }

    /// Distinct candidates inside one window cannot grow it past the cap; with
    /// equal scores the oldest is the one dropped.
    #[test]
    fn the_window_holds_at_most_the_cap_of_candidates() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(3600));
        let start = Instant::now();
        let extra = 10;
        for i in 0..(MAX_TRACKED_CANDIDATES + extra) {
            evaluator.record_only_with_current_time(
                SocketAddr::from(([10, 0, (i >> 8) as u8, (i & 0xff) as u8], 1)),
                0.1,
                start + Duration::from_millis(i as u64),
            );
        }
        assert_eq!(evaluator.scores.len(), MAX_TRACKED_CANDIDATES);
        let oldest_kept = evaluator.scores.front().map(|(_, a, _)| *a);
        assert_eq!(
            oldest_kept,
            Some(SocketAddr::from(([10, 0, 0, extra as u8], 1))),
            "the first {extra} candidates were dropped, oldest first"
        );
    }

    /// One entry per candidate: a retry replaces its earlier entry, so a
    /// candidate retrying many times holds one slot, and its fresh timestamp
    /// keeps the deque in order for `remove_outdated_scores`.
    #[test]
    fn a_retry_replaces_the_candidate_s_earlier_entry() {
        let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(60));
        let start = Instant::now();
        evaluator.record_and_eval_with_current_time(peer(2), 0.5, start);
        evaluator.record_only_with_current_time(peer(1), 0.9, start + Duration::from_secs(10));
        for k in 2..6 {
            evaluator.record_and_eval_with_current_time(
                peer(2),
                0.5,
                start + Duration::from_secs(10 * k),
            );
        }
        let entries: Vec<_> = evaluator.scores.iter().map(|(_, a, _)| *a).collect();
        assert_eq!(entries, vec![peer(1), peer(2)], "oldest first, one each");
        // peer(1)'s entry (t=10) ages out at t=71 although peer(2) kept
        // retrying after it: the order held.
        evaluator.record_only_with_current_time(peer(3), 0.1, start + Duration::from_secs(71));
        let entries: Vec<_> = evaluator.scores.iter().map(|(_, a, _)| *a).collect();
        assert_eq!(entries, vec![peer(2), peer(3)]);
    }
}
