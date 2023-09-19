## Intelligent Routing

Freenet's request routing mechanism plays a crucial role in the efficiency of
the network.

It is responsible for deciding which peer to route a request to when attempting
to read, create, or modify a contract's state. The mechanism is designed to
select the peer that can complete the request the fastest, which may not always
be the peer closest to the contract's location - the traditional approach for
routing in a small-world network, known as [greedy
routing](https://en.wikipedia.org/wiki/Small-world_routing#Greedy_routing).

### Isotonic Regression

Freenet uses [isotonic regression](https://github.com/sanity/pav.rs), a method
for estimating a monotonically increasing or decreasing function given a set of
data, to predict the response time from a peer based on its ring distance from
the target location of the request.

This estimation is then adjusted by the average difference between the isotonic
regression estimate and the actual response time from previous interactions with
the peer. This process enables a form of adaptive routing that selects the peer
with the lowest estimated response time.

### Router Initialization and Event Handling

When a new
[Router](https://github.com/freenet/freenet-core/blob/main/crates/core/src/router.rs)
is created, it's initialized with a history of routing events. These events are
processed to generate the initial state of the isotonic estimators. For example,
failure outcomes and success durations are computed for each event in the
history and used to initialize the respective estimators. The average transfer
size is also computed from the history.

The Router can add new events to its history, updating its estimators in the
process. When a successful routing event occurs, the Router updates its response
start time estimator, failure estimator, and transfer rate estimator based on
the details of the event. If a failure occurs, only the failure estimator is
updated.

### Peer Selection

To select a peer for routing a request, the Router first checks whether it has
sufficient historical data. If not, it selects the peer with the minimum
distance to the contract location. If it does have sufficient data, it predicts
the outcome of routing the request to each available peer and selects the one
with the best predicted outcome.

### Outcome Prediction

To predict the outcome of routing a request to a specific peer, the Router uses
its isotonic estimators to predict the time to the start of the response, the
chance of failure, and the transfer rate. These predictions are used to compute
an expected total time for the request, with the cost of a failure being assumed
as a multiple of the cost of success. The peer with the lowest expected total
time is selected for routing the request.
