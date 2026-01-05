//! Transport layer metrics collection for periodic telemetry snapshots.
//!
//! Instead of emitting telemetry events per-transfer (which could flood the server),
//! we accumulate metrics and emit periodic snapshots every N seconds.
//!
//! Metrics always accumulate (negligible overhead from atomic ops). Snapshots
//! are only taken by TelemetryWorker when telemetry is enabled - if telemetry
//! is disabled, no TelemetryWorker runs and `take_snapshot()` is never called.
//!
//! # Usage
//!
//! ```ignore
//! use freenet::transport::metrics::TRANSPORT_METRICS;
//!
//! // Record a completed transfer
//! TRANSPORT_METRICS.record_transfer_completed(&stats);
//!
//! // Periodically take snapshots (done by TelemetryWorker when telemetry enabled)
//! if let Some(snapshot) = TRANSPORT_METRICS.take_snapshot() {
//!     // Send to telemetry
//! }
//! ```

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::LazyLock;
use tokio::sync::mpsc;

use crate::tracing::{TransferDirection, TransferEvent};

/// Global transport metrics instance.
///
/// All peer connections report to this single instance, which aggregates
/// metrics for periodic telemetry snapshots.
pub static TRANSPORT_METRICS: LazyLock<TransportMetrics> = LazyLock::new(TransportMetrics::new);

/// Accumulates transport metrics for periodic reporting.
///
/// All operations are lock-free using atomics. Metrics are accumulated
/// until `take_snapshot()` is called, which resets the counters.
///
/// # Thread Safety
///
/// This struct is designed for concurrent updates from multiple connections.
/// The snapshot operation is not atomic across all fields, but this is
/// acceptable for telemetry purposes.
///
/// # When Telemetry is Disabled
///
/// Metrics still accumulate (negligible overhead), but `take_snapshot()`
/// is never called since TelemetryWorker doesn't run.
#[derive(Debug)]
pub struct TransportMetrics {
    // Transfer counters (reset each snapshot)
    transfers_completed: AtomicU32,
    transfers_failed: AtomicU32,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,

    // Timing accumulators (for computing averages)
    total_transfer_time_ms: AtomicU64,

    // Throughput tracking
    peak_throughput_bps: AtomicU64,

    // LEDBAT stats (peak values during period)
    peak_cwnd_bytes: AtomicU32,
    min_cwnd_bytes: AtomicU32,
    cwnd_sum: AtomicU64,     // For computing average
    cwnd_samples: AtomicU32, // Number of samples

    // Slowdowns during period
    slowdowns_triggered: AtomicU32,

    // RTT tracking (in microseconds for precision)
    min_rtt_us: AtomicU64,
    max_rtt_us: AtomicU64,
    rtt_sum_us: AtomicU64,
    rtt_samples: AtomicU32,
}

impl Default for TransportMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl TransportMetrics {
    /// Create a new metrics collector.
    pub fn new() -> Self {
        Self {
            transfers_completed: AtomicU32::new(0),
            transfers_failed: AtomicU32::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            total_transfer_time_ms: AtomicU64::new(0),
            peak_throughput_bps: AtomicU64::new(0),
            peak_cwnd_bytes: AtomicU32::new(0),
            min_cwnd_bytes: AtomicU32::new(u32::MAX),
            cwnd_sum: AtomicU64::new(0),
            cwnd_samples: AtomicU32::new(0),
            slowdowns_triggered: AtomicU32::new(0),
            min_rtt_us: AtomicU64::new(u64::MAX),
            max_rtt_us: AtomicU64::new(0),
            rtt_sum_us: AtomicU64::new(0),
            rtt_samples: AtomicU32::new(0),
        }
    }

    /// Record a completed outbound transfer.
    pub fn record_transfer_completed(&self, stats: &super::TransferStats) {
        // Use saturating arithmetic to prevent overflow (though extremely unlikely
        // in practice - would require billions of transfers or exabytes of data)
        self.transfers_completed
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(1))
            })
            .ok();
        self.bytes_sent
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(stats.bytes_transferred))
            })
            .ok();
        self.total_transfer_time_ms
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(stats.elapsed.as_millis() as u64))
            })
            .ok();

        // Update throughput peak
        let throughput = stats.avg_throughput_bps();
        self.peak_throughput_bps
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if throughput > current {
                    Some(throughput)
                } else {
                    None
                }
            })
            .ok();

        // Update LEDBAT stats
        self.record_cwnd_sample(stats.final_cwnd_bytes);
        self.peak_cwnd_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if stats.peak_cwnd_bytes > current {
                    Some(stats.peak_cwnd_bytes)
                } else {
                    None
                }
            })
            .ok();

        self.slowdowns_triggered
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(stats.slowdowns_triggered))
            })
            .ok();

        // Record RTT sample from base_delay
        let rtt_us = stats.base_delay.as_micros() as u64;
        if rtt_us > 0 {
            self.record_rtt_sample(rtt_us);
        }
    }

    /// Record a cwnd sample (called periodically or on transfer completion).
    fn record_cwnd_sample(&self, cwnd_bytes: u32) {
        self.cwnd_sum
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(cwnd_bytes as u64))
            })
            .ok();
        self.cwnd_samples
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(1))
            })
            .ok();

        // Update min
        self.min_cwnd_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if cwnd_bytes < current {
                    Some(cwnd_bytes)
                } else {
                    None
                }
            })
            .ok();
    }

    /// Record an RTT sample in microseconds.
    fn record_rtt_sample(&self, rtt_us: u64) {
        self.rtt_sum_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(rtt_us))
            })
            .ok();
        self.rtt_samples
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(1))
            })
            .ok();

        // Update min
        self.min_rtt_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if rtt_us < current {
                    Some(rtt_us)
                } else {
                    None
                }
            })
            .ok();

        // Update max
        self.max_rtt_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if rtt_us > current {
                    Some(rtt_us)
                } else {
                    None
                }
            })
            .ok();
    }

    /// Take a snapshot and reset all counters.
    ///
    /// Returns `None` if there was no activity during the period.
    pub fn take_snapshot(&self) -> Option<TransportSnapshot> {
        // Read and reset counters
        let transfers_completed = self.transfers_completed.swap(0, Ordering::Relaxed);
        let transfers_failed = self.transfers_failed.swap(0, Ordering::Relaxed);

        // No activity = no snapshot
        if transfers_completed == 0 && transfers_failed == 0 {
            // Still reset other counters to prevent stale data
            self.bytes_sent.store(0, Ordering::Relaxed);
            self.bytes_received.store(0, Ordering::Relaxed);
            self.total_transfer_time_ms.store(0, Ordering::Relaxed);
            self.peak_throughput_bps.store(0, Ordering::Relaxed);
            self.peak_cwnd_bytes.store(0, Ordering::Relaxed);
            self.min_cwnd_bytes.store(u32::MAX, Ordering::Relaxed);
            self.cwnd_sum.store(0, Ordering::Relaxed);
            self.cwnd_samples.store(0, Ordering::Relaxed);
            self.slowdowns_triggered.store(0, Ordering::Relaxed);
            self.min_rtt_us.store(u64::MAX, Ordering::Relaxed);
            self.max_rtt_us.store(0, Ordering::Relaxed);
            self.rtt_sum_us.store(0, Ordering::Relaxed);
            self.rtt_samples.store(0, Ordering::Relaxed);
            return None;
        }

        let bytes_sent = self.bytes_sent.swap(0, Ordering::Relaxed);
        let bytes_received = self.bytes_received.swap(0, Ordering::Relaxed);
        let total_transfer_time_ms = self.total_transfer_time_ms.swap(0, Ordering::Relaxed);
        let peak_throughput_bps = self.peak_throughput_bps.swap(0, Ordering::Relaxed);
        let peak_cwnd_bytes = self.peak_cwnd_bytes.swap(0, Ordering::Relaxed);
        let min_cwnd_bytes = self.min_cwnd_bytes.swap(u32::MAX, Ordering::Relaxed);
        let cwnd_sum = self.cwnd_sum.swap(0, Ordering::Relaxed);
        let cwnd_samples = self.cwnd_samples.swap(0, Ordering::Relaxed);
        let slowdowns_triggered = self.slowdowns_triggered.swap(0, Ordering::Relaxed);
        let min_rtt_us = self.min_rtt_us.swap(u64::MAX, Ordering::Relaxed);
        let max_rtt_us = self.max_rtt_us.swap(0, Ordering::Relaxed);
        let rtt_sum_us = self.rtt_sum_us.swap(0, Ordering::Relaxed);
        let rtt_samples = self.rtt_samples.swap(0, Ordering::Relaxed);

        // Compute averages
        let avg_cwnd_bytes = if cwnd_samples > 0 {
            (cwnd_sum / cwnd_samples as u64) as u32
        } else {
            0
        };

        let avg_transfer_time_ms = if transfers_completed > 0 {
            total_transfer_time_ms / transfers_completed as u64
        } else {
            0
        };

        let avg_rtt_us = if rtt_samples > 0 {
            rtt_sum_us / rtt_samples as u64
        } else {
            0
        };

        Some(TransportSnapshot {
            transfers_completed,
            transfers_failed,
            bytes_sent,
            bytes_received,
            avg_transfer_time_ms,
            peak_throughput_bps,
            avg_cwnd_bytes,
            peak_cwnd_bytes,
            min_cwnd_bytes: if min_cwnd_bytes == u32::MAX {
                0
            } else {
                min_cwnd_bytes
            },
            slowdowns_triggered,
            avg_rtt_us,
            min_rtt_us: if min_rtt_us == u64::MAX {
                0
            } else {
                min_rtt_us
            },
            max_rtt_us,
        })
    }
}

/// Periodic transport metrics snapshot.
///
/// Emitted every N seconds to provide aggregate transport layer statistics
/// without flooding the telemetry server with per-transfer events.
///
/// # Sentinel Values
///
/// - `min_cwnd_bytes`: 0 indicates no cwnd samples were recorded
/// - `min_rtt_us`: 0 indicates no RTT samples were recorded
/// - Other fields: 0 is a valid value indicating no activity for that metric
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct TransportSnapshot {
    /// Number of stream transfers completed successfully.
    pub transfers_completed: u32,
    /// Number of stream transfers that failed.
    pub transfers_failed: u32,
    /// Total bytes sent during the period.
    pub bytes_sent: u64,
    /// Total bytes received during the period.
    pub bytes_received: u64,
    /// Average transfer time in milliseconds (0 if no transfers).
    pub avg_transfer_time_ms: u64,
    /// Peak throughput observed (bytes per second).
    pub peak_throughput_bps: u64,
    /// Average congestion window size in bytes (0 if no samples).
    pub avg_cwnd_bytes: u32,
    /// Peak congestion window size (bytes).
    pub peak_cwnd_bytes: u32,
    /// Minimum congestion window size in bytes (0 if no samples recorded).
    pub min_cwnd_bytes: u32,
    /// Number of LEDBAT slowdowns triggered.
    pub slowdowns_triggered: u32,
    /// Average RTT in microseconds (0 if no samples).
    pub avg_rtt_us: u64,
    /// Minimum RTT in microseconds (0 if no samples recorded).
    pub min_rtt_us: u64,
    /// Maximum RTT in microseconds.
    pub max_rtt_us: u64,
}

/// Channel capacity for transfer events.
/// Using a bounded channel to prevent memory issues if telemetry is slow.
const TRANSFER_EVENT_CHANNEL_CAPACITY: usize = 1000;

/// Global sender for transfer events.
/// This is set once during telemetry initialization.
static TRANSFER_EVENT_SENDER: LazyLock<parking_lot::RwLock<Option<mpsc::Sender<TransferEvent>>>> =
    LazyLock::new(|| parking_lot::RwLock::new(None));

/// Initialize the transfer event channel.
/// Returns a receiver that the TelemetryWorker should poll.
/// Should be called once during telemetry initialization.
pub fn init_transfer_event_channel() -> mpsc::Receiver<TransferEvent> {
    let (tx, rx) = mpsc::channel(TRANSFER_EVENT_CHANNEL_CAPACITY);
    *TRANSFER_EVENT_SENDER.write() = Some(tx);
    rx
}

/// Emit a transfer started event.
/// Returns immediately if telemetry is not enabled (sender not initialized).
pub fn emit_transfer_started(
    stream_id: u64,
    peer_addr: SocketAddr,
    expected_bytes: u64,
    direction: TransferDirection,
) {
    let sender_guard = TRANSFER_EVENT_SENDER.read();
    if let Some(sender) = sender_guard.as_ref() {
        let event = TransferEvent::Started {
            stream_id,
            peer_addr,
            expected_bytes,
            direction,
            tx_id: None, // Transaction ID not available at transport layer
            timestamp: crate::tracing::telemetry::current_timestamp_ms(),
        };
        // Use try_send to avoid blocking the transport layer
        let _ = sender.try_send(event);
    }
}

/// Emit a transfer completed event.
/// Returns immediately if telemetry is not enabled (sender not initialized).
#[allow(clippy::too_many_arguments)]
pub fn emit_transfer_completed(
    stream_id: u64,
    peer_addr: SocketAddr,
    bytes_transferred: u64,
    elapsed_ms: u64,
    avg_throughput_bps: u64,
    peak_cwnd_bytes: Option<u32>,
    final_cwnd_bytes: Option<u32>,
    slowdowns_triggered: Option<u32>,
    final_srtt_ms: Option<u32>,
    final_ssthresh_bytes: Option<u32>,
    min_ssthresh_floor_bytes: Option<u32>,
    total_timeouts: Option<u32>,
    direction: TransferDirection,
) {
    let sender_guard = TRANSFER_EVENT_SENDER.read();
    if let Some(sender) = sender_guard.as_ref() {
        let event = TransferEvent::Completed {
            stream_id,
            peer_addr,
            bytes_transferred,
            elapsed_ms,
            avg_throughput_bps,
            peak_cwnd_bytes,
            final_cwnd_bytes,
            slowdowns_triggered,
            final_srtt_ms,
            final_ssthresh_bytes,
            min_ssthresh_floor_bytes,
            total_timeouts,
            direction,
            timestamp: crate::tracing::telemetry::current_timestamp_ms(),
        };
        // Use try_send to avoid blocking the transport layer
        let _ = sender.try_send(event);
    }
}

/// Emit a transfer failed event.
/// Returns immediately if telemetry is not enabled (sender not initialized).
pub fn emit_transfer_failed(
    stream_id: u64,
    peer_addr: SocketAddr,
    bytes_transferred: u64,
    reason: String,
    elapsed_ms: u64,
    direction: TransferDirection,
) {
    let sender_guard = TRANSFER_EVENT_SENDER.read();
    if let Some(sender) = sender_guard.as_ref() {
        let event = TransferEvent::Failed {
            stream_id,
            peer_addr,
            bytes_transferred,
            reason,
            elapsed_ms,
            direction,
            timestamp: crate::tracing::telemetry::current_timestamp_ms(),
        };
        // Use try_send to avoid blocking the transport layer
        let _ = sender.try_send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_empty_snapshot_returns_none() {
        let metrics = TransportMetrics::new();
        assert!(metrics.take_snapshot().is_none());
    }

    #[test]
    fn test_snapshot_after_transfer() {
        let metrics = TransportMetrics::new();

        let stats = crate::transport::TransferStats {
            stream_id: 1,
            remote_addr: "127.0.0.1:8080".parse().unwrap(),
            bytes_transferred: 1024,
            elapsed: Duration::from_millis(100),
            peak_cwnd_bytes: 50000,
            final_cwnd_bytes: 40000,
            slowdowns_triggered: 1,
            base_delay: Duration::from_millis(10),
            final_ssthresh_bytes: 100000,
            min_ssthresh_floor_bytes: 5696,
            total_timeouts: 0,
        };

        metrics.record_transfer_completed(&stats);

        let snapshot = metrics.take_snapshot().expect("should have snapshot");
        assert_eq!(snapshot.transfers_completed, 1);
        assert_eq!(snapshot.bytes_sent, 1024);
        assert_eq!(snapshot.peak_cwnd_bytes, 50000);
        assert_eq!(snapshot.slowdowns_triggered, 1);
    }

    #[test]
    fn test_snapshot_resets_counters() {
        let metrics = TransportMetrics::new();

        let stats = crate::transport::TransferStats {
            stream_id: 1,
            remote_addr: "127.0.0.1:8080".parse().unwrap(),
            bytes_transferred: 1024,
            elapsed: Duration::from_millis(100),
            peak_cwnd_bytes: 50000,
            final_cwnd_bytes: 40000,
            slowdowns_triggered: 1,
            base_delay: Duration::from_millis(10),
            final_ssthresh_bytes: 100000,
            min_ssthresh_floor_bytes: 5696,
            total_timeouts: 0,
        };

        metrics.record_transfer_completed(&stats);
        let _ = metrics.take_snapshot();

        // Second snapshot should be None (no new activity)
        assert!(metrics.take_snapshot().is_none());
    }

    #[test]
    fn test_multiple_transfers_aggregate() {
        let metrics = TransportMetrics::new();

        for i in 0..5 {
            let stats = crate::transport::TransferStats {
                stream_id: i,
                remote_addr: "127.0.0.1:8080".parse().unwrap(),
                bytes_transferred: 1000,
                elapsed: Duration::from_millis(100),
                peak_cwnd_bytes: 40000 + (i as u32 * 1000),
                final_cwnd_bytes: 35000,
                slowdowns_triggered: 1,
                base_delay: Duration::from_millis(10),
                final_ssthresh_bytes: 100000,
                min_ssthresh_floor_bytes: 5696,
                total_timeouts: 0,
            };
            metrics.record_transfer_completed(&stats);
        }

        let snapshot = metrics.take_snapshot().expect("should have snapshot");
        assert_eq!(snapshot.transfers_completed, 5);
        assert_eq!(snapshot.bytes_sent, 5000);
        assert_eq!(snapshot.peak_cwnd_bytes, 44000); // Max of all peaks
        assert_eq!(snapshot.slowdowns_triggered, 5);
    }
}
