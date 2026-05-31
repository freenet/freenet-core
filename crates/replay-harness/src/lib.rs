//! Offline replay harness for shadow-RTT controllers.
//!
//! This crate provides a deterministic, single-threaded simulator that drives
//! a candidate [`Controller`] against a stream of RTT samples — either
//! synthetic (programmatic scenarios that pin known failure modes from the
//! prior congestion-control attempts in [#4074][issue]) or replayed from the
//! OTLP shadow-RTT telemetry that Phase 1.5 emits (#4292).
//!
//! It is strictly offline tooling. Nothing here is reachable from the
//! production `freenet` binary, and the rolling-stats / event types here
//! are an intentional copy of the production types: we want the harness to
//! pin against a specific iteration of the algorithm rather than silently
//! follow whatever `crates/core` does next.
//!
//! [issue]: https://github.com/freenet/freenet-core/issues/4074
//!
//! # Workflow
//!
//! 1. Author a [`Controller`] in `controllers/`.
//! 2. Author a synthetic [`Scenario`](scenarios::Scenario) that pins the
//!    behaviour you expect on a specific input pattern (idle baseline,
//!    routing event, contention burst, …).
//! 3. `cargo test -p replay-harness` runs every scenario against every
//!    controller — additions to either side compose automatically.
//! 4. Once a controller passes synthetic scenarios, run it against real
//!    OTLP data via the binary:
//!    `replay-harness otlp <log.jsonl> --controller <name>`.

pub mod controllers;
pub mod event;
pub mod replayer;
pub mod rolling;
pub mod scenarios;

pub use controllers::{Controller, ControllerView, PeerObservation, RateDecision};
pub use event::{Event, EventStream, PeerKey};
pub use replayer::{DecisionLog, ReplayReport, Replayer};
pub use rolling::{RollingRttStats, RttSnapshot};
