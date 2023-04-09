mod routing_outcome_estimator;

use locutus_core::libp2p::*;
use routing_outcome_estimator::RoutingOutcomeEstimator;

pub struct PeerChooser {
    time_est: RoutingOutcomeEstimator,
    success_est: RoutingOutcomeEstimator,
}

impl PeerChooser {

}

struct RoutingEvent {
    peer_id : PeerId,
    outcome : RoutingOutcome,
}
