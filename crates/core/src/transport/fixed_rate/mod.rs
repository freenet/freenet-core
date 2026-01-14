//! Fixed-rate congestion controller.
//!
//! A simple, non-adaptive congestion controller that transmits at a constant rate
//! regardless of network feedback. This is a pragmatic alternative to adaptive
//! algorithms (BBR, LEDBAT) when those algorithms have bugs or instabilities.
//!
//! ## Design Rationale
//!
//! Complex congestion control algorithms can enter broken states:
//! - BBR's min_rtt can be reset to infinity, causing cwnd to freeze
//! - LEDBAT can enter "death spirals" from misinterpreted delay signals
//! - State machines can fail to recover from edge cases
//!
//! A fixed-rate approach trades optimal throughput for reliability:
//! - No complex state that can break
//! - Predictable, consistent throughput
//! - Works if the network can sustain the configured rate
//!
//! ## Usage
//!
//! Configure via `CongestionControlConfig`:
//! ```ignore
//! let config = CongestionControlConfig::fixed_rate(12_500_000); // 100 Mbps
//! let controller = config.build();
//! ```
//!
//! ## Caveats
//!
//! - No backpressure: If the path can't sustain the rate, packets will be dropped
//!   and retransmitted, potentially wasting bandwidth
//! - Not optimal: Fast connections won't be fully utilized, slow connections
//!   may be overloaded
//! - Use as a temporary measure while debugging adaptive algorithms

mod controller;

pub use controller::{FixedRateConfig, FixedRateController, DEFAULT_RATE_BYTES_PER_SEC};
