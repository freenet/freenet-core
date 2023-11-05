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
fn test_not_best_in_time_window() {
    let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));

    let start_time = Instant::now();
    evaluator.record_and_eval_with_current_time(5.0, start_time);
    assert_eq!(evaluator.record_and_eval_with_current_time(4.0, start_time + Duration::from_secs(5)), false);
}

#[test]
fn test_best_in_time_window() {
    let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));

    let start_time = Instant::now();
    evaluator.record_and_eval_with_current_time(5.0, start_time);
    assert_eq!(evaluator.record_and_eval_with_current_time(4.0, start_time + Duration::from_secs(11)), true);
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
    assert_eq!(evaluator.record_and_eval_with_current_time(5.0, current_time), true);
    assert_eq!(evaluator.record_and_eval_with_current_time(4.0, current_time), false);
}

#[test]
fn test_multiple_scores_same_timestamp() {
    let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));
    let current_time = Instant::now();
    evaluator.record_only_with_current_time(5.0, current_time);
    evaluator.record_only_with_current_time(6.0, current_time);
    assert_eq!(evaluator.scores.len(), 2);
    assert_eq!(evaluator.record_and_eval_with_current_time(4.0, current_time + Duration::from_secs(5)), false);
}

#[test]
fn test_negative_scores() {
    let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));
    let start_time = Instant::now();
    assert_eq!(evaluator.record_and_eval_with_current_time(-5.0, start_time), true);
    assert_eq!(evaluator.record_and_eval_with_current_time(-4.0, start_time + Duration::from_secs(5)), true);
    assert_eq!(evaluator.record_and_eval_with_current_time(-6.0, start_time + Duration::from_secs(5)), false);
}

#[test]
fn test_large_number_of_scores() {
    let mut evaluator = ConnectionEvaluator::new(Duration::from_secs(10));
    let start_time = Instant::now();
    for i in 0..1000 {
        evaluator.record_only_with_current_time(i as f64, start_time + Duration::from_secs(i));
    }
    assert_eq!(evaluator.record_and_eval_with_current_time(1000.0, start_time + Duration::from_secs(1001)), true);
}
