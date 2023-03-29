mod routing_outcome_estimator;

use routing_outcome_estimator::RoutingOutcomeEstimator;

struct PeerChooser {
    time_est: RoutingOutcomeEstimator,
    success_est: RoutingOutcomeEstimator,
}