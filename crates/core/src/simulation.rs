//! Deterministic simulation testing framework for Freenet.
//!
//! This module provides infrastructure for running reproducible network simulations
//! with deterministic time advancement, seeded randomness, and fault injection.
//!
//! # Architecture
//!
//! The simulation framework is built around several key components:
//!
//! - **TimeSource**: Abstraction over time operations (real vs virtual)
//! - **VirtualTime**: Deterministic time that only advances when explicitly stepped
//! - **SimulationRng**: Seeded RNG for all random decisions during simulation
//! - **FaultConfig**: Configuration for fault injection (drops, partitions, latency)
//!
//! # SimNetwork (Production-Ready)
//!
//! Location: `testing_impl.rs`
//!
//! **What it does:**
//! - Runs actual Freenet node code as async tasks
//! - Uses `InMemoryTransport` for fast message passing
//! - Supports fault injection via `FaultConfig`
//! - Provides convergence checking and operation tracking
//! - Uses Turmoil for deterministic task scheduling
//!
//! **Use for:** Integration testing, end-to-end tests, fdev test mode
//!
//! # Usage Example
//!
//! ```ignore
//! use freenet::dev_tool::SimNetwork;
//! use freenet::simulation::FaultConfig;
//!
//! // Create network with deterministic seed
//! let mut sim = SimNetwork::new("test", 1, 3, 10, 7, 10, 5, 0x1234).await;
//!
//! // Enable fault injection
//! sim.with_fault_injection(
//!     FaultConfig::builder()
//!         .message_loss_rate(0.05)
//!         .latency_range(Duration::from_millis(10)..Duration::from_millis(50))
//!         .build()
//! );
//!
//! // Run operations and check results
//! let handles = sim.start_with_rand_gen::<SmallRng>(0x1234, 10, 5).await;
//! sim.await_convergence(Duration::from_secs(30), Duration::from_millis(500), 1).await?;
//!
//! // Verify operation success
//! let summary = sim.get_operation_summary().await;
//! assert!(summary.overall_success_rate() >= 0.9);
//! ```

mod fault;
mod rng;
mod time;

pub use fault::{FaultConfig, FaultConfigBuilder, Partition};
pub use rng::SimulationRng;
pub use time::Wakeup;
pub use time::{RealTime, TimeSource, TimeSourceInterval, VirtualTime, WakeupId};
