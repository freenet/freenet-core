//! A reactive bound on what quinn holds for a connection's streams in their
//! in-order phase (#5940, buffer-budget design section 1a).
//!
//! The path reads a stream's header and metadata, and a control frame's body,
//! in order: quinn holds what arrives past a missing byte, keeping each
//! datagram alive until the gap fills or its assembler compacts, which can be
//! several times the connection's credit. Everything else is read unordered,
//! and quinn then holds about nothing (`path::inbound`).
//!
//! What bounds the hold: quinn keeps only received datagrams, so every byte it
//! holds for a stream lies in a datagram that arrived after the stream was
//! created, and a stream in its in-order phase holds only datagrams that
//! arrived after that phase began (data from before it was read, or is the
//! between-polls exception). So the connection's hold is at most
//!
//! ```text
//! RETAINED_DATAGRAM_BYTES x (datagrams received since the OLDEST open in-order
//! phase began) + 1.25 x min(those datagrams x 1,200, W + debt)
//! ```
//!
//! The second term is quinn's compaction copy: it copies a stream's unique
//! buffered bytes while the datagrams they came in are still alive, and a
//! connection's unique buffered bytes are at most its credit, W + debt; the
//! copy lands in a jemalloc size class up to a quarter larger. Per-stream state
//! is in the fixed charge (`config::QUIC_OPEN_STREAM_BYTES`).
//!
//! pinned by `quic::memory_tests::quinn_holds_no_more_than_its_received_datagrams`.
//! The socket adapter, which sees every datagram quinn receives, counts each
//! connection's (`QuicSocket`, [`HoldRegistry`]); the reader opens a phase per
//! stream ([`HoldDetector::begin_phase`]) and closes it when the stream
//! switches to unordered reads or ends. When the count passes the connection's
//! threshold the detector trips: every stream still in its in-order phase is
//! stopped (`stop_code::EVICTED`, counted as `QuicInboundOutcome::EvictedHold`),
//! quinn frees what it held for them, and their phases close.
//!
//! An in-order phase spans at most a header and its metadata (18 + 1,120 B)
//! or a control body (at most 4 KiB), so an honest one lasts at most about a
//! round trip, while the first datagram is resent; the threshold
//! ([`HOLD_THRESHOLD_TENTHS`] of W + debt) must exceed the datagrams a busy
//! connection receives meanwhile, or honest streams are stopped (3c measures
//! the rate under loss). A path with a budget meter has its threshold set
//! from its window (`budget::ConnMeter`); a shadow path keeps the one for the
//! endpoint's initial window (`config::INITIAL_RECEIVE_WINDOW_BYTES`).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::transport::metrics::QuicDataCounters;

/// What quinn keeps alive per received datagram it holds: the 1,200-byte
/// datagram buffer in its jemalloc size class (1,280) and the assembler's
/// entry for the chunk it carries (up to 64). Pinned with the copy term by
/// `quic::memory_tests::quinn_holds_no_more_than_its_received_datagrams`
/// (the largest measured was 1,787 B per datagram over 44 datagrams at a
/// 16 KiB window, of which the copy was 16 KiB).
pub(crate) const RETAINED_DATAGRAM_BYTES: u64 = 1344;

/// The trip threshold, in tenths of the connection's credit (W + debt): 2.2.
/// Provisional: the hostile-sender harness (3c) measures honest false stops
/// at 2, 5, 10 and 15% loss with several streams, and the ramp gate needs
/// them near zero at 5%; this value is chosen from that data.
pub(crate) const HOLD_THRESHOLD_TENTHS: u64 = 22;

/// The trip threshold for a connection whose peer may hold `credit` bytes.
pub(crate) const fn hold_threshold(credit: u64) -> u64 {
    credit.saturating_mul(HOLD_THRESHOLD_TENTHS) / 10
}

/// The most quinn holds for a connection under the detector, for the charge:
/// the threshold, plus the compaction copy (at most 1.25 x `credit`), plus
/// the one datagram counted when the trip is seen. About 3.45 x `credit`.
pub(crate) const fn hold_charge(credit: u64) -> u64 {
    hold_threshold(credit)
        .saturating_add(credit.saturating_mul(5) / 4)
        .saturating_add(RETAINED_DATAGRAM_BYTES)
}

/// The largest credit whose [`hold_charge`] is at most `charge` (rounded down,
/// so never over).
pub(crate) const fn credit_for_hold_charge(charge: u64) -> u64 {
    // hold_charge(c) = c x 22/10 + c x 5/4 + slot, about c x 69/20 + slot,
    // with each part rounded down; dividing by a little more is safe.
    charge
        .saturating_sub(RETAINED_DATAGRAM_BYTES)
        .saturating_mul(20)
        / 70
}

/// One connection's datagram count and its open in-order phases.
pub(crate) struct HoldDetector {
    /// Datagrams the socket adapter received from the connection's peer.
    datagrams: AtomicU64,
    /// The datagram count each open phase began at, by phase number.
    phases: Mutex<BTreeMap<u64, u64>>,
    next_phase: AtomicU64,
    /// The smallest start in `phases`, or `u64::MAX` with none open: read on
    /// every datagram without the lock.
    oldest: AtomicU64,
    /// The trip threshold in bytes.
    threshold: AtomicU64,
    /// Raised at each trip; a phase begun under an older value is stopped.
    generation: AtomicU64,
    /// This oldest phase has tripped already: count one trip per oldest phase.
    tripped: AtomicBool,
    notify: tokio::sync::Notify,
    node: &'static QuicDataCounters,
}

impl HoldDetector {
    pub(crate) fn new(threshold: u64, node: &'static QuicDataCounters) -> Arc<Self> {
        Arc::new(Self {
            datagrams: AtomicU64::new(0),
            phases: Mutex::new(BTreeMap::new()),
            next_phase: AtomicU64::new(0),
            oldest: AtomicU64::new(u64::MAX),
            threshold: AtomicU64::new(threshold),
            generation: AtomicU64::new(0),
            tripped: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
            node,
        })
    }

    pub(crate) fn set_threshold(&self, threshold: u64) {
        self.threshold.store(threshold, Ordering::Relaxed);
    }

    /// One datagram from the connection's peer reached quinn.
    pub(crate) fn on_datagram(&self) {
        let count = self.datagrams.fetch_add(1, Ordering::Relaxed) + 1;
        let oldest = self.oldest.load(Ordering::Acquire);
        if oldest == u64::MAX {
            return;
        }
        let held = count
            .saturating_sub(oldest)
            .saturating_mul(RETAINED_DATAGRAM_BYTES);
        let threshold = self.threshold.load(Ordering::Relaxed).max(1);
        self.node
            .record_hold_level(held.saturating_mul(100) / threshold);
        if held > threshold && !self.tripped.swap(true, Ordering::AcqRel) {
            self.generation.fetch_add(1, Ordering::AcqRel);
            self.node.record_hold_trip();
            self.notify.notify_waiters();
        }
    }

    /// A stream entered its in-order phase. The phase ends when the guard is
    /// dropped.
    pub(crate) fn begin_phase(self: &Arc<Self>) -> HoldPhase {
        let id = self.next_phase.fetch_add(1, Ordering::Relaxed);
        let mut phases = self.phases.lock();
        let start = self.datagrams.load(Ordering::Relaxed);
        phases.insert(id, start);
        self.refresh(&phases);
        HoldPhase {
            id,
            generation: self.generation.load(Ordering::Acquire),
            detector: self.clone(),
        }
    }

    fn end_phase(&self, id: u64) {
        let mut phases = self.phases.lock();
        phases.remove(&id);
        self.refresh(&phases);
    }

    fn refresh(&self, phases: &BTreeMap<u64, u64>) {
        let oldest = phases.values().copied().min().unwrap_or(u64::MAX);
        if self.oldest.swap(oldest, Ordering::AcqRel) != oldest {
            // A new oldest phase may trip on its own.
            self.tripped.store(false, Ordering::Release);
        }
    }

    /// Open phases now. Tests only.
    #[cfg(test)]
    pub(crate) fn open_phases(&self) -> usize {
        self.phases.lock().len()
    }
}

/// One stream's in-order phase; dropping it ends the phase.
pub(crate) struct HoldPhase {
    id: u64,
    generation: u64,
    detector: Arc<HoldDetector>,
}

impl HoldPhase {
    /// Completes once the detector tripped after this phase began: the stream
    /// must then be stopped.
    pub(crate) async fn tripped(&self) {
        loop {
            let notified = self.detector.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.detector.generation.load(Ordering::Acquire) > self.generation {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for HoldPhase {
    fn drop(&mut self) {
        self.detector.end_phase(self.id);
    }
}

/// The detectors of an endpoint's connections, by the peer address the socket
/// adapter hands quinn.
#[derive(Default)]
pub(crate) struct HoldRegistry {
    detectors: DashMap<SocketAddr, Weak<HoldDetector>>,
}

impl HoldRegistry {
    /// Count datagrams from `addr` into `detector` while the returned guard
    /// lives.
    pub(crate) fn register(
        self: &Arc<Self>,
        addr: SocketAddr,
        detector: &Arc<HoldDetector>,
    ) -> HoldRegistration {
        self.detectors.insert(addr, Arc::downgrade(detector));
        HoldRegistration {
            registry: self.clone(),
            addr,
            detector: Arc::downgrade(detector),
        }
    }

    /// A datagram from `addr` is handed to quinn.
    pub(crate) fn on_datagram(&self, addr: SocketAddr) {
        let detector = self
            .detectors
            .get(&addr)
            .and_then(|entry| entry.value().upgrade());
        if let Some(detector) = detector {
            detector.on_datagram();
        }
    }
}

/// Removes its detector from the registry when dropped, if still its own.
pub(crate) struct HoldRegistration {
    registry: Arc<HoldRegistry>,
    addr: SocketAddr,
    detector: Weak<HoldDetector>,
}

impl Drop for HoldRegistration {
    fn drop(&mut self) {
        self.registry
            .detectors
            .remove_if(&self.addr, |_, held| held.ptr_eq(&self.detector));
    }
}

#[cfg(test)]
mod tests;
