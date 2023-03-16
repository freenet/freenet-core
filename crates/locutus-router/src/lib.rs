
use std::{collections::HashMap, time::Duration};

use locutus_core::{libp2p::PeerId, Location};
use pav_regression::{
    pav::{IsotonicRegression, Point},
};

pub struct RetrievalTimeEstimator {
    dist_to_time_regression: IsotonicRegression,

    dist_to_time_regressions_by_peer: HashMap<PeerId, IsotonicRegression>,
}

fn merge(value1 : &[u8], value2 : &[u8]) -> Vec<u8>

impl RetrievalTimeEstimator {
    fn new<I>(history: I) -> Self
    where
        I: IntoIterator<Item = RoutingEvent>,
    {
        let mut all_points : Vec<Point> = vec![];

        let mut points_by_peer: HashMap<PeerId, Vec<Point>> = HashMap::new();

        for event in history {
            if let RoutingOutcome::Success { duration } = event.routing_outcome {
                let x = event.peer_location.distance(&event.contract_location);
                let y = duration.as_millis() as f64;
                let point = Point::new(x.into(), y);

                all_points.push(point);

                points_by_peer.entry(event.peer).or_insert_with(|| {vec![]}).push(point);
            };
        }

        let dist_to_time_regression = IsotonicRegression::new_ascending(all_points.as_slice());

        let dist_to_time_regressions_by_peer = points_by_peer
            .into_iter()
            .map(|(peer, points)| {
                let regression = IsotonicRegression::new_ascending(points.as_slice());
                (peer, regression)
            })
            .collect();

        RetrievalTimeEstimator {
            dist_to_time_regression,
            dist_to_time_regressions_by_peer,
        }
    }
}

pub struct RoutingEvent {
    peer: PeerId,
    peer_location: Location,
    contract_location: Location,
    routing_type: RoutingType,
    routing_outcome: RoutingOutcome,
}

pub enum RoutingType {
    JoinRing,
    Put,
    Get,
    Subscribe,
}

pub enum RoutingOutcome {
    Success { duration: Duration },
    Failure,
}

#[test]
fn test() {
    println!("hello world!");
}
