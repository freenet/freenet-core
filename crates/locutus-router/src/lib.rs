use std::time::Duration;

use locutus_core::{libp2p::PeerId, Location};
use pav_regression::{*, pav::{IsotonicRegression, Point}};

pub struct PAVRouter {
    dist_to_time_regression : IsotonicRegression,
}

impl PAVRouter {
    fn new<I>(history: I) -> Self
    where
        I: IntoIterator<Item = RoutingEvent>,
    {
        let points: Vec<Point> = history
            .into_iter()
            .filter_map(|routing_event| match routing_event.routing_outcome {
                RoutingOutcome::Success { duration } => {
                    let x = routing_event
                        .peer_location
                        .distance(&routing_event.contract_location);
                    let y = duration.as_millis() as f64;
                    Some(Point::new(x.into(), y))
                }
                RoutingOutcome::Failure => None,
            })
            .collect();
    
        let regression = IsotonicRegression::new_ascending(&points);
    
        PAVRouter {
            dist_to_time_regression: regression,
        }
    }
    
}

pub struct RoutingEvent {
    peer : PeerId,
    peer_location : Location,
    contract_location : Location,
    routing_type : RoutingType,
    routing_outcome : RoutingOutcome,
}

pub enum RoutingType {
    JoinRing,
    Put,
    Get,
    Subscribe,
}

pub enum RoutingOutcome {
    Success { duration : Duration },
    Failure,
}


#[test]
fn test() {
    println!("hello world!");
}
