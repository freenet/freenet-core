mod routing_outcome_estimator;

use routing_outcome_estimator::RoutingOutcomeEstimator;

pub struct PeerChooser {
    time_est: RoutingOutcomeEstimator,
    success_est: RoutingOutcomeEstimator,
}