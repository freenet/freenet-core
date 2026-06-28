/// Smoke tests pinning the contract the transport layer relies on from
/// `tokio::sync::mpsc`. These tests replace the wrapper-level tests that
/// were deleted along with `fast_channel.rs` in #3960 — they exercise the
/// same scenarios (basic send/recv, backpressure, sender clone, disconnect
/// detection, no-busy-loop wakeup, burst drain, multi-producer concurrency,
/// stress under contention, sender drop while receiver parked) directly
/// against the primitive the transport now uses, so any future regression
/// (e.g. swapping back to a wrapper that wedges) trips a clear unit test.
mod mpsc_invariants;
