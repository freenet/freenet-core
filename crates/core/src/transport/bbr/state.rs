//! BBRv3 congestion control state machine.
//!
//! This module defines the BBR state machine with four primary states:
//! - Startup: Exponential probing to find max bandwidth
//! - Drain: Drain queue built during Startup
//! - ProbeBW: Steady-state bandwidth probing with cycling phases
//! - ProbeRTT: Periodic min_rtt measurement

use std::sync::atomic::{AtomicU8, Ordering};

/// Primary BBRv3 state machine states.
///
/// ## State Transitions
///
/// ```text
/// ┌────────────────┐
/// │    Startup     │  (pacing_gain=2.77, cwnd_gain=2.0)
/// │                │  Exponential bandwidth probing
/// └───────┬────────┘
///         │ (bandwidth plateaus for 3 rounds OR 2% loss)
///         ▼
/// ┌────────────────┐
/// │     Drain      │  (pacing_gain=0.36, cwnd_gain=2.0)
/// │                │  Drain queue built during Startup
/// └───────┬────────┘
///         │ (inflight <= BDP)
///         ▼
/// ┌────────────────┐
/// │    ProbeBW     │  Steady-state bandwidth probing
/// │                │  Cycles through: Down → Cruise → Refill → Up
/// └───────┬────────┘
///         │ (every ~5s OR min_rtt expired)
///         ▼
/// ┌────────────────┐
/// │   ProbeRTT     │  (cwnd_gain=0.5)
/// │                │  Drain queue to measure true min_rtt
/// └───────┬────────┘
///         │ (200ms elapsed AND inflight drained)
///         └────────────────────────────────► ProbeBW
///
/// Timeout from any state → Startup
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum BbrState {
    /// Initial phase: exponential growth to find max bandwidth.
    /// Uses high pacing gain (2.77x) to double sending rate each RTT.
    /// Exits when bandwidth plateaus or loss exceeds threshold.
    Startup = 0,

    /// Drain queue built during Startup.
    /// Uses low pacing gain (0.36x) to send below estimated bandwidth.
    /// Exits when inflight drops to estimated BDP.
    Drain = 1,

    /// Steady-state bandwidth probing.
    /// Cycles through sub-phases to probe for more bandwidth while
    /// maintaining low queuing delay. See `ProbeBwPhase` for details.
    ProbeBW = 2,

    /// Periodic RTT measurement phase.
    /// Reduces cwnd to drain queues and get accurate min_rtt sample.
    /// Enters every ~5s or when min_rtt filter expires.
    ProbeRTT = 3,
}

impl BbrState {
    /// Convert from u8, returning None for invalid values.
    pub(crate) fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Startup),
            1 => Some(Self::Drain),
            2 => Some(Self::ProbeBW),
            3 => Some(Self::ProbeRTT),
            _ => None,
        }
    }

    /// Returns true if this state uses exponential cwnd growth.
    pub(crate) fn is_probing_bandwidth(&self) -> bool {
        matches!(self, Self::Startup)
    }
}

/// ProbeBW sub-phases for steady-state bandwidth probing.
///
/// BBRv3 cycles through these phases to maintain high utilization
/// while periodically probing for more bandwidth.
///
/// ## Phase Cycle
///
/// ```text
/// ┌──────────┐      ┌─────────┐      ┌────────┐      ┌──────┐
/// │   Down   │ ───► │ Cruise  │ ───► │ Refill │ ───► │  Up  │
/// │ (0.9x)   │      │ (1.0x)  │      │ (1.0x) │      │(1.25x)│
/// └──────────┘      └─────────┘      └────────┘      └──┬───┘
///      ▲                                                │
///      └────────────────────────────────────────────────┘
/// ```
///
/// - **Down**: Drain any queue by pacing below bandwidth (0.9x)
/// - **Cruise**: Steady-state pacing at estimated bandwidth (1.0x)
/// - **Refill**: Refill the pipe after Down phase (1.0x)
/// - **Up**: Probe for more bandwidth (1.25x)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ProbeBwPhase {
    /// Sending below estimated bandwidth to drain queues.
    /// pacing_gain = 0.9
    Down = 0,

    /// Sending at estimated bandwidth (steady state).
    /// pacing_gain = 1.0
    Cruise = 1,

    /// Refilling the pipe after Down phase.
    /// pacing_gain = 1.0
    Refill = 2,

    /// Probing for additional bandwidth.
    /// pacing_gain = 1.25
    Up = 3,
}

impl ProbeBwPhase {
    /// Convert from u8, returning None for invalid values.
    pub(crate) fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Down),
            1 => Some(Self::Cruise),
            2 => Some(Self::Refill),
            3 => Some(Self::Up),
            _ => None,
        }
    }

    /// Get the next phase in the cycle.
    pub(crate) fn next(self) -> Self {
        match self {
            Self::Down => Self::Cruise,
            Self::Cruise => Self::Refill,
            Self::Refill => Self::Up,
            Self::Up => Self::Down,
        }
    }

    /// Get the pacing gain for this phase.
    pub(crate) fn pacing_gain(self) -> f64 {
        match self {
            Self::Down => super::config::PROBE_BW_DOWN_PACING_GAIN,
            Self::Cruise => super::config::PROBE_BW_CRUISE_PACING_GAIN,
            Self::Refill => super::config::PROBE_BW_CRUISE_PACING_GAIN,
            Self::Up => super::config::PROBE_BW_UP_PACING_GAIN,
        }
    }
}

/// Lock-free atomic wrapper for [`BbrState`].
///
/// Provides type-safe atomic operations on the BBR state, ensuring
/// that all loads return valid enum variants.
pub(crate) struct AtomicBbrState(AtomicU8);

impl AtomicBbrState {
    /// Create a new atomic state with the given initial value.
    pub(crate) fn new(state: BbrState) -> Self {
        Self(AtomicU8::new(state as u8))
    }

    /// Load the current state with Acquire ordering.
    pub(crate) fn load(&self) -> BbrState {
        let value = self.0.load(Ordering::Acquire);
        match BbrState::from_u8(value) {
            Some(state) => state,
            None => {
                tracing::error!(
                    value,
                    "CRITICAL: Invalid BBR state value - possible memory corruption"
                );
                debug_assert!(false, "Invalid BBR state value: {}", value);
                // Fall back to ProbeBW as safest default
                BbrState::ProbeBW
            }
        }
    }

    /// Store a new state with Release ordering.
    pub(crate) fn store(&self, state: BbrState) {
        self.0.store(state as u8, Ordering::Release);
    }

    /// Check if current state is Startup.
    pub(crate) fn is_startup(&self) -> bool {
        self.load() == BbrState::Startup
    }

    /// Check if current state is ProbeRTT.
    pub(crate) fn is_probe_rtt(&self) -> bool {
        self.load() == BbrState::ProbeRTT
    }

    /// Transition to Startup state (used for timeout recovery).
    pub(crate) fn enter_startup(&self) {
        self.store(BbrState::Startup);
    }

    /// Transition to Drain state.
    pub(crate) fn enter_drain(&self) {
        self.store(BbrState::Drain);
    }

    /// Transition to ProbeBW state.
    pub(crate) fn enter_probe_bw(&self) {
        self.store(BbrState::ProbeBW);
    }

    /// Transition to ProbeRTT state.
    pub(crate) fn enter_probe_rtt(&self) {
        self.store(BbrState::ProbeRTT);
    }

    /// Atomically compare and exchange state.
    pub(crate) fn compare_exchange(
        &self,
        expected: BbrState,
        new: BbrState,
    ) -> Result<BbrState, BbrState> {
        self.0
            .compare_exchange(
                expected as u8,
                new as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|v| BbrState::from_u8(v).unwrap_or(BbrState::ProbeBW))
            .map_err(|v| BbrState::from_u8(v).unwrap_or(BbrState::ProbeBW))
    }
}

impl std::fmt::Debug for AtomicBbrState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AtomicBbrState({:?})", self.load())
    }
}

/// Lock-free atomic wrapper for [`ProbeBwPhase`].
pub(crate) struct AtomicProbeBwPhase(AtomicU8);

impl AtomicProbeBwPhase {
    /// Create a new atomic phase with the given initial value.
    pub(crate) fn new(phase: ProbeBwPhase) -> Self {
        Self(AtomicU8::new(phase as u8))
    }

    /// Load the current phase with Acquire ordering.
    pub(crate) fn load(&self) -> ProbeBwPhase {
        let value = self.0.load(Ordering::Acquire);
        ProbeBwPhase::from_u8(value).unwrap_or(ProbeBwPhase::Cruise)
    }

    /// Store a new phase with Release ordering.
    pub(crate) fn store(&self, phase: ProbeBwPhase) {
        self.0.store(phase as u8, Ordering::Release);
    }

    /// Advance to the next phase in the cycle.
    pub(crate) fn advance(&self) -> ProbeBwPhase {
        let current = self.load();
        let next = current.next();
        self.store(next);
        next
    }
}

impl std::fmt::Debug for AtomicProbeBwPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AtomicProbeBwPhase({:?})", self.load())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bbr_state_values() {
        assert_eq!(BbrState::Startup as u8, 0);
        assert_eq!(BbrState::Drain as u8, 1);
        assert_eq!(BbrState::ProbeBW as u8, 2);
        assert_eq!(BbrState::ProbeRTT as u8, 3);
    }

    #[test]
    fn test_probe_bw_phase_values() {
        assert_eq!(ProbeBwPhase::Down as u8, 0);
        assert_eq!(ProbeBwPhase::Cruise as u8, 1);
        assert_eq!(ProbeBwPhase::Refill as u8, 2);
        assert_eq!(ProbeBwPhase::Up as u8, 3);
    }

    #[test]
    fn test_probe_bw_phase_cycle() {
        assert_eq!(ProbeBwPhase::Down.next(), ProbeBwPhase::Cruise);
        assert_eq!(ProbeBwPhase::Cruise.next(), ProbeBwPhase::Refill);
        assert_eq!(ProbeBwPhase::Refill.next(), ProbeBwPhase::Up);
        assert_eq!(ProbeBwPhase::Up.next(), ProbeBwPhase::Down);
    }

    #[test]
    fn test_atomic_bbr_state() {
        let state = AtomicBbrState::new(BbrState::Startup);
        assert!(state.is_startup());

        state.enter_drain();
        assert_eq!(state.load(), BbrState::Drain);

        state.enter_probe_bw();
        assert_eq!(state.load(), BbrState::ProbeBW);

        state.enter_probe_rtt();
        assert!(state.is_probe_rtt());
    }

    #[test]
    fn test_atomic_probe_bw_phase() {
        let phase = AtomicProbeBwPhase::new(ProbeBwPhase::Down);
        assert_eq!(phase.load(), ProbeBwPhase::Down);

        let next = phase.advance();
        assert_eq!(next, ProbeBwPhase::Cruise);
        assert_eq!(phase.load(), ProbeBwPhase::Cruise);
    }
}
