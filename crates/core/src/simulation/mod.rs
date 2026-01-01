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
//! - **FaultConfig**: Configuration for fault injection (drops, partitions, latency)
//!
//! # Two Simulation Systems
//!
//! ## 1. SimNetwork (Production-Ready)
//!
//! Location: `testing_impl.rs`
//!
//! **What it does:**
//! - Runs actual Freenet node code as async tasks
//! - Uses `InMemoryTransport` for fast message passing
//! - Supports fault injection via `FaultInjectorState`
//! - Provides convergence checking and operation tracking
//!
//! **Limitations:**
//! - Uses tokio's async scheduler (non-deterministic ordering)
//! - Uses real wall-clock time (timing varies between runs)
//!
//! **Use for:** Integration testing, end-to-end tests, fdev test mode
//!
//! ## 2. SimulatedNetwork (Experimental - Not Yet Useful)
//!
//! Location: `simulation/network.rs`
//!
//! **⚠️ EXPERIMENTAL STATUS:**
//! This component is a design sketch for future deterministic simulation.
//! It currently does NOT run actual node code - only abstract message passing.
//! It cannot test Freenet protocol logic in its current form.
//!
//! **What it provides:**
//! - Fully deterministic message delivery via `Scheduler`
//! - `VirtualTime` for controlled time progression
//! - A model of what full determinism could look like
//!
//! **What's missing to make it useful:**
//! - Integration with actual node execution
//! - A deterministic executor to replace tokio
//! - Wiring to route node operations through the scheduler
//!
//! # Achieving Full Determinism (Future Work)
//!
//! To run actual node code with deterministic scheduling would require:
//!
//! ## Option 1: Deterministic Executor
//! Replace tokio with a custom executor that processes tasks in deterministic order:
//! ```ignore
//! // Conceptual - not implemented
//! struct DeterministicExecutor {
//!     scheduler: Scheduler,
//!     task_queue: BinaryHeap<(Priority, Task)>,
//! }
//!
//! impl DeterministicExecutor {
//!     fn step(&mut self) {
//!         // Process one task/event in priority order
//!         // Advance VirtualTime as needed
//!     }
//! }
//! ```
//!
//! ## Option 2: Single-Threaded Tokio + Controlled Yields
//! Use `tokio::runtime::Builder::new_current_thread()` with explicit yield points:
//! ```ignore
//! let rt = tokio::runtime::Builder::new_current_thread()
//!     .enable_time()  // Could mock with VirtualTime
//!     .build()?;
//!
//! // Run with controlled task ordering via priority channels
//! ```
//!
//! ## Option 3: Simulation Shim Layer
//! Intercept all I/O and time operations, routing them through the scheduler:
//! ```ignore
//! // Replace tokio::time::sleep with:
//! async fn sim_sleep(duration: Duration) {
//!     let deadline = VIRTUAL_TIME.now() + duration;
//!     SCHEDULER.wait_until(deadline).await;
//! }
//! ```
//!
//! Each option has trade-offs between implementation complexity and fidelity.
//! See `docs/architecture/simulation-testing.md` for detailed analysis.
//!
//! # Current Recommended Usage
//!
//! For testing Freenet logic, use `SimNetwork` with fault injection:
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
mod network;
mod rng;
mod scheduler;
mod time;

pub use fault::{FaultConfig, FaultConfigBuilder, Partition};
pub use network::{SimulatedNetwork, SimulatedNetworkConfig};
pub use rng::SimulationRng;
pub use scheduler::{Event, EventId, EventType, Scheduler, SchedulerConfig};
pub use time::{RealTime, TimeSource, VirtualTime, Wakeup, WakeupId};
