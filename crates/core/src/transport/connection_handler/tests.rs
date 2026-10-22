/// Smoke tests pinning the contract the transport layer relies on from
/// `tokio::sync::mpsc`. These tests replace the wrapper-level tests that
/// were deleted along with `fast_channel.rs` in #3960 — they exercise the
/// same scenarios (basic send/recv, backpressure, sender clone, disconnect
/// detection, no-busy-loop wakeup, burst drain, multi-producer concurrency,
/// stress under contention, sender drop while receiver parked) directly
/// against the primitive the transport now uses, so any future regression
/// (e.g. swapping back to a wrapper that wedges) trips a clear unit test.
mod mpsc_invariants;

/// The UDP transport's implementation of the `OutboundConnector` boundary
/// trait forwards each stage of a connect to `OutboundConnectionHandler`.
mod outbound_connector;

/// The UDP transport's implementation of the `InboundAcceptor` boundary trait.
mod inbound_acceptor;

/// The UDP transport's `TransportHandle` and its listener liveness.
mod transport_handle;

/// `ConnectionStateManager::has_nat_traversal_to_ip`, which classifies stray
/// intros.
mod nat_traversal_by_ip;

/// The node-wide egress cap never makes the listener, a handshake or the
/// receive path wait.
mod egress_cap;
