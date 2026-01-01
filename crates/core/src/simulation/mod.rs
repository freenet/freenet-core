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
//! - **Scheduler**: Deterministic event scheduler processing events in order
//! - **SimulatedNetwork**: Network layer integrating with the scheduler
//! - **FaultConfig**: Configuration for fault injection (drops, partitions, latency)
//!
//! # Integration with SimNetwork
//!
//! There are two simulation systems in the codebase:
//!
//! 1. **`SimNetwork`** (in `testing_impl.rs`): The existing async-based test network
//!    - Uses tokio async runtime with real time
//!    - Nodes run as actual async tasks with `InMemoryTransport`
//!    - Good for integration testing with realistic concurrency
//!
//! 2. **`SimulatedNetwork`** (this module): Pure deterministic simulation
//!    - Synchronous, event-driven message delivery
//!    - Uses `VirtualTime` for fully controlled time progression
//!    - Good for reproducible bug reproduction and property testing
//!
//! ## Current Integration
//!
//! Seeds flow from `SimNetwork` through to:
//! - Per-peer RNG seeds via `derive_peer_seed()`
//! - `MemoryConnManager` for transport-level decisions
//! - `InMemoryTransport` for deterministic noise mode shuffling
//!
//! ## Future Integration (TODO)
//!
//! To achieve full determinism, `SimNetwork` could be enhanced to:
//! 1. Use `VirtualTime` instead of `tokio::time`
//! 2. Route messages through `SimulatedNetwork` for controlled delivery
//! 3. Use `Scheduler` to order all async events
//!
//! This would require replacing the tokio runtime with a deterministic executor.
//!
//! # Usage
//!
//! ```ignore
//! use freenet::simulation::{Scheduler, FaultConfig, VirtualTime};
//!
//! // Create a deterministic simulation with seed 42
//! let mut scheduler = Scheduler::new(42);
//!
//! // Configure faults
//! let fault_config = FaultConfig::builder()
//!     .message_loss_rate(0.01)
//!     .latency_range(Duration::from_millis(10)..Duration::from_millis(100))
//!     .build();
//!
//! // Run simulation
//! scheduler.run_until(|state| state.all_connected());
//! ```

mod fault;
mod network;
mod rng;
mod scheduler;
mod time;

pub use fault::{FaultConfig, FaultConfigBuilder, Partition};
pub use network::{SimulatedNetwork, SimulatedNetworkConfig};
pub use rng::SimulationRng;
pub use scheduler::{Event, EventId, EventType, Scheduler, SchedulerConfig};
pub use time::{RealTime, TimeSource, VirtualTime, Wakeup, WakeupId};
