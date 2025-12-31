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
