//! Manual Throughput Benchmarks (No Criterion)
//!
//! Custom harness for throughput testing without criterion's warmup phase.
//! Tests sustained throughput, bandwidth saturation, and concurrent streams.
//!
//! ## Usage
//!
//! ```bash
//! # Run all manual benchmarks
//! cargo test --release --bench transport_manual --features bench -- --nocapture
//!
//! # Run specific test
//! cargo test --release --bench transport_manual --features bench manual_bandwidth_saturation -- --nocapture
//! ```
//!
//! ## Why `--features bench` is required
//!
//! These benchmarks use `mock_transport` which is gated behind:
//! ```rust,ignore
//! #[cfg(any(test, feature = "bench"))]
//! pub use connection_handler::mock_transport;
//! ```
//!
//! This prevents test infrastructure from being compiled into production builds.
//!
//! ## Tests Included
//!
//! - `manual_throughput_benchmarks`: Single message latency (1KB-32KB)
//! - `manual_sustained_throughput`: Multiple messages on same connection
//! - `manual_bandwidth_saturation`: Maximum throughput (send as fast as possible)
//! - `manual_concurrent_streams`: Multiple parallel connections

mod transport;
