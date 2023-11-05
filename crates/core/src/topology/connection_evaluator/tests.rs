use super::*;

#[test]
fn test_record_first_score() {
    let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));
    let current_time = Instant::now();
    assert_eq!(evaluator.record_and_eval_with_current_time(5.0, current_time), true);
    // Assert evaluator.scores contains the new score
    assert_eq!(evaluator.scores.len(), 1);
    assert_eq!(evaluator.scores[0].1, 5.0);
    assert_eq!(evaluator.scores[0].0, current_time);

}

#[test]
fn test_record_within_window_duration() {
    let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));

    let start_time = Instant::now();
    evaluator.record_and_eval_with_current_time(5.0, start_time);
    assert_eq!(evaluator.record_and_eval_with_current_time(6.0, start_time + Duration::from_secs(5)), false);
}

#[test]
fn test_record_outside_window_duration() {
    let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));

    let start_time = Instant::now();
    evaluator.record_and_eval_with_current_time(5.0, start_time);
    assert_eq!(evaluator.record_and_eval_with_current_time(6.0, start_time + Duration::from_secs(11)), true);
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
