//! Congestion control state machine.
//!
//! This module defines the unified congestion control state machine that
//! consolidates slow start, congestion avoidance, and periodic slowdown states.

use std::sync::atomic::{AtomicU8, Ordering};

/// Unified congestion control state machine.
///
/// This enum consolidates the previous `in_slow_start` boolean and `SlowdownState`
/// into a single state machine, preventing conflicting state combinations that
/// caused bugs (e.g., PR #2510 where `in_slow_start=true` during `RampingUp`
/// bypassed the slowdown handler).
///
/// ## State Transitions
///
/// There are two paths through the slowdown cycle:
///
/// ### Initial Slowdown (after first slow start exit)
/// ```text
/// SlowStart → WaitingForSlowdown → InSlowdown → Frozen → RampingUp → CongestionAvoidance
/// ```
/// `WaitingForSlowdown` adds a delay (2 RTTs) before the first slowdown to allow
/// the connection to stabilize after slow start exit.
///
/// ### Subsequent Slowdowns (periodic, from CongestionAvoidance)
/// ```text
/// CongestionAvoidance → InSlowdown → Frozen → RampingUp → CongestionAvoidance
/// ```
/// Subsequent slowdowns skip `WaitingForSlowdown` and go directly to `InSlowdown`
/// when the scheduled slowdown time is reached.
///
/// ### Timeout Recovery (from any state)
/// ```text
/// Any State → SlowStart
/// ```
///
/// ### Visual Diagram
/// ```text
/// ┌─────────────┐
/// │  SlowStart  │─────────────────────────────────────────────────┐
/// └──────┬──────┘                                                 │
///        │ (ssthresh or delay exit)                               │
///        ▼                                                        │
/// ┌────────────────────────┐     (first time only)                │
/// │  WaitingForSlowdown    │◄─────────────────────┐               │
/// └──────────┬─────────────┘                      │               │
///            │ (wait complete)                    │               │
///            ▼                                    │               │
/// ┌──────────────┐◄───────────────────────────────│───────────────│───┐
/// │  InSlowdown  │  (transient: immediately       │               │   │
/// └──────┬───────┘   transitions on next ACK)     │               │   │
///        │                                        │               │   │
///        ▼                                        │               │   │
/// ┌──────────┐                                    │               │   │
/// │  Frozen  │  (holds for N RTTs)                │               │   │
/// └────┬─────┘                                    │               │   │
///      │ (freeze duration complete)               │               │   │
///      ▼                                          │               │   │
/// ┌────────────┐                                  │               │   │
/// │  RampingUp │  (exponential growth)            │               │   │
/// └─────┬──────┘                                  │               │   │
///       │ (target reached)                        │               │   │
///       ▼                                         │               │   │
/// ┌──────────────────────┐                        │               │   │
/// │ CongestionAvoidance  │────────────────────────┴───────────────┘   │
/// └──────────┬───────────┘◄───────────────────────────────────────────┘
///            │ (scheduled slowdown time reached)
///            └────────────────────────────────────────────────────────┘
///                        (subsequent slowdowns skip WaitingForSlowdown)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub(crate) enum CongestionState {
    /// Initial slow start phase: exponential cwnd growth until ssthresh
    /// or delay threshold is reached.
    SlowStart = 0,
    /// Normal LEDBAT++ congestion avoidance: delay-based cwnd adjustment.
    CongestionAvoidance = 1,
    /// Waiting N RTTs before starting the *first* periodic slowdown.
    /// Only entered from SlowStart; subsequent slowdowns skip this state.
    WaitingForSlowdown = 2,
    /// Transient state: cwnd has been reduced, immediately transitions to
    /// Frozen on the next ACK. This state exists to separate the cwnd
    /// reduction from the freeze timing logic.
    InSlowdown = 3,
    /// Frozen at reduced cwnd for N RTTs to re-measure base delay.
    Frozen = 4,
    /// Ramping back up using exponential growth after slowdown.
    /// This state uses slow-start-like growth but with different exit
    /// conditions (target cwnd rather than ssthresh/delay).
    RampingUp = 5,
}

impl CongestionState {
    /// Convert from u8, returning None for invalid values.
    pub(crate) fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::SlowStart),
            1 => Some(Self::CongestionAvoidance),
            2 => Some(Self::WaitingForSlowdown),
            3 => Some(Self::InSlowdown),
            4 => Some(Self::Frozen),
            5 => Some(Self::RampingUp),
            _ => None,
        }
    }
}

/// Lock-free atomic wrapper for [`CongestionState`].
///
/// Provides type-safe atomic operations on the congestion state, ensuring
/// that all loads return valid enum variants and all stores use valid values.
/// This encapsulates the raw `AtomicU8` and prevents invalid state values
/// from being stored or read.
pub(crate) struct AtomicCongestionState(AtomicU8);

impl AtomicCongestionState {
    /// Create a new atomic state with the given initial value.
    pub(crate) fn new(state: CongestionState) -> Self {
        Self(AtomicU8::new(state as u8))
    }

    /// Load the current state with Acquire ordering.
    ///
    /// # Panics (debug only)
    /// Debug-asserts if the stored value is not a valid `CongestionState`.
    pub(crate) fn load(&self) -> CongestionState {
        let value = self.0.load(Ordering::Acquire);
        match CongestionState::from_u8(value) {
            Some(state) => state,
            None => {
                // This should never happen - indicates memory corruption or a serious bug
                tracing::error!(
                    value,
                    "CRITICAL: Invalid congestion state value - possible memory corruption"
                );
                debug_assert!(false, "Invalid congestion state value: {}", value);
                // In release, fall back to CongestionAvoidance as safest default
                CongestionState::CongestionAvoidance
            }
        }
    }

    /// Store a new state with Release ordering.
    pub(crate) fn store(&self, state: CongestionState) {
        self.0.store(state as u8, Ordering::Release);
    }

    /// Check if current state is SlowStart.
    pub(crate) fn is_slow_start(&self) -> bool {
        self.load() == CongestionState::SlowStart
    }

    /// Transition to SlowStart state (used for timeout recovery).
    pub(crate) fn enter_slow_start(&self) {
        self.store(CongestionState::SlowStart);
    }

    /// Transition to CongestionAvoidance state.
    pub(crate) fn enter_congestion_avoidance(&self) {
        self.store(CongestionState::CongestionAvoidance);
    }

    /// Transition to WaitingForSlowdown state.
    pub(crate) fn enter_waiting_for_slowdown(&self) {
        self.store(CongestionState::WaitingForSlowdown);
    }

    /// Transition to InSlowdown state.
    pub(crate) fn enter_in_slowdown(&self) {
        self.store(CongestionState::InSlowdown);
    }

    /// Transition to Frozen state.
    pub(crate) fn enter_frozen(&self) {
        self.store(CongestionState::Frozen);
    }

    /// Transition to RampingUp state.
    pub(crate) fn enter_ramping_up(&self) {
        self.store(CongestionState::RampingUp);
    }

    /// Atomically compare and exchange state.
    ///
    /// If current state equals `expected`, sets it to `new` and returns `Ok(expected)`.
    /// Otherwise, returns `Err(current_state)`.
    ///
    /// Uses AcqRel ordering for success and Acquire for failure.
    #[allow(dead_code)] // Available for future use in atomic state transitions
    pub(crate) fn compare_exchange(
        &self,
        expected: CongestionState,
        new: CongestionState,
    ) -> Result<CongestionState, CongestionState> {
        self.0
            .compare_exchange(
                expected as u8,
                new as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|v| CongestionState::from_u8(v).unwrap_or(CongestionState::CongestionAvoidance))
            .map_err(|v| {
                CongestionState::from_u8(v).unwrap_or(CongestionState::CongestionAvoidance)
            })
    }
}

impl std::fmt::Debug for AtomicCongestionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AtomicCongestionState({:?})", self.load())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_congestion_state_values() {
        // Verify enum values for state machine
        assert_eq!(CongestionState::SlowStart as u8, 0);
        assert_eq!(CongestionState::CongestionAvoidance as u8, 1);
        assert_eq!(CongestionState::WaitingForSlowdown as u8, 2);
        assert_eq!(CongestionState::InSlowdown as u8, 3);
        assert_eq!(CongestionState::Frozen as u8, 4);
        assert_eq!(CongestionState::RampingUp as u8, 5);
    }
}
