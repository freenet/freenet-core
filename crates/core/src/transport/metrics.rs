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

use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
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

    // Cumulative wire-byte counters (never reset, used by the local dashboard).
    //
    // `cumulative_bytes_sent` is updated at the UDP socket layer for every
    // successful `send_to`, so it reflects every byte we put on the wire
    // including keep-alives, ACKs, NAT-traversal probes, and small control
    // messages.
    //
    // `cumulative_bytes_received` is updated post-authentication (see
    // `record_packet_received`), so it counts bytes from packets that pass
    // `try_decrypt_sym` against an established symmetric session key.
    // Counting at the raw socket would let an attacker spoofing UDP source
    // addresses inflate this counter arbitrarily (#3999).
    cumulative_bytes_sent: AtomicU64,
    cumulative_bytes_received: AtomicU64,

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

    // Per-peer wire-byte stats (bounded to MAX_TRACKED_PEERS entries).
    //
    // Receive side is metered post-authentication in
    // `transport::peer_connection::PeerConnection::recv` — only packets that
    // pass `try_decrypt_sym` for an established connection contribute. Send
    // side is metered at the socket layer because outbound targets are
    // controlled by us, not influenced by remote peers.
    //
    // When the table is full a new entry evicts the least-recently-updated
    // entry (LRU on `last_seen_tick`).
    per_peer_stats: DashMap<SocketAddr, PeerTransferStats>,

    // Monotonically increasing tick stamped onto each per-peer entry on
    // update. Used purely for LRU ordering — no wall-clock semantics, so
    // simulation determinism is unaffected.
    per_peer_tick: AtomicU64,

    // RTT tracking (in microseconds for precision)
    min_rtt_us: AtomicU64,
    max_rtt_us: AtomicU64,
    rtt_sum_us: AtomicU64,
    rtt_samples: AtomicU32,
}

/// Maximum number of peers tracked for per-peer transfer stats.
const MAX_TRACKED_PEERS: usize = 256;

/// Per-peer cumulative transfer statistics.
#[derive(Debug)]
pub struct PeerTransferStats {
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    /// Monotonic tick of the most recent update; smallest tick = oldest entry,
    /// used for LRU eviction when the table is full.
    last_seen_tick: AtomicU64,
}

impl Default for PeerTransferStats {
    fn default() -> Self {
        Self {
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            last_seen_tick: AtomicU64::new(0),
        }
    }
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
            cumulative_bytes_sent: AtomicU64::new(0),
            cumulative_bytes_received: AtomicU64::new(0),
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
            per_peer_stats: DashMap::new(),
            per_peer_tick: AtomicU64::new(0),
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
        // Note: `bytes_sent` here aggregates stream-transfer payload sizes for
        // the periodic telemetry snapshot. Wire-byte counters
        // (`cumulative_bytes_sent`, per-peer `bytes_sent`) are updated at the
        // socket layer in `record_packet_sent` and intentionally not touched
        // here.
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
    pub(crate) fn record_cwnd_sample(&self, cwnd_bytes: u32) {
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
    ///
    /// Called both from [`Self::record_transfer_completed`] (LEDBAT
    /// `base_delay` at stream completion) and from the ping/pong keep-alive
    /// cycle in `peer_connection.rs` (round-trip of a keep-alive Ping). The
    /// keep-alive path is what keeps RTT statistics populated for quiet,
    /// long-lived connections that rarely complete a stream transfer (#4000).
    pub(crate) fn record_rtt_sample(&self, rtt_us: u64) {
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
                if rtt_us < current { Some(rtt_us) } else { None }
            })
            .ok();

        // Update max
        self.max_rtt_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if rtt_us > current { Some(rtt_us) } else { None }
            })
            .ok();
    }

    /// Read cumulative bytes uploaded without resetting counters.
    ///
    /// Used by the local dashboard to display lifetime transfer stats
    /// without interfering with the periodic telemetry snapshots.
    pub fn cumulative_bytes_sent(&self) -> u64 {
        self.cumulative_bytes_sent.load(Ordering::Relaxed)
    }

    /// Number of RTT samples accumulated in the current snapshot window.
    ///
    /// Reset to 0 by `take_snapshot`. Exposed so tests can assert that a
    /// code path (e.g. the ping/pong keep-alive cycle) actually recorded a
    /// sample without consuming the snapshot.
    #[cfg(test)]
    pub(crate) fn rtt_sample_count(&self) -> u32 {
        self.rtt_samples.load(Ordering::Relaxed)
    }

    /// Record a completed inbound stream transfer.
    ///
    /// Aggregates stream-payload bytes into the per-snapshot `bytes_received`
    /// counter, which pairs with `transfers_completed` for periodic telemetry.
    /// Wire-byte counters (`cumulative_bytes_received`, per-peer
    /// `bytes_received`) are owned by [`Self::record_packet_received`] at the
    /// socket layer.
    pub fn record_inbound_completed(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record an outbound UDP packet at the socket layer.
    ///
    /// Updates the cumulative dashboard counter and the per-peer counter so
    /// keep-alives, ACKs, and other small control messages all show up on the
    /// local dashboard, not just large stream transfers.
    ///
    /// Outbound is metered at the socket layer because send targets are
    /// controlled by us — there is no spoof vector — and we want to surface
    /// every byte we put on the wire (including NAT-traversal probes that
    /// never produce a stream).
    ///
    /// `remote_addr` MUST be the canonical (un-mapped) form used elsewhere in
    /// the system — the same form `recv_from` returns after
    /// `normalize_mapped_addr`. Mismatched callers (e.g. passing a
    /// `::ffff:x.x.x.x` mapped target on a dual-stack socket) would create a
    /// duplicate per-peer entry that the dashboard cannot join against
    /// `connected_peers[].address`.
    pub fn record_packet_sent(&self, remote_addr: SocketAddr, bytes: u64) {
        self.cumulative_bytes_sent
            .fetch_add(bytes, Ordering::Relaxed);
        self.record_per_peer(remote_addr, bytes, |s| &s.bytes_sent);
    }

    /// Record an inbound UDP packet that has passed authentication.
    ///
    /// MUST only be called for packets that have been successfully decrypted
    /// against an established symmetric session key — see the call site in
    /// [`crate::transport::peer_connection::PeerConnection::recv`]. Calling
    /// this on raw `recv_from` output would let an attacker inflate the
    /// dashboard by spoofing UDP packets from many source IPs (#3999).
    ///
    /// See [`Self::record_packet_sent`] for the canonical-address contract on
    /// `remote_addr`.
    pub fn record_packet_received(&self, remote_addr: SocketAddr, bytes: u64) {
        self.cumulative_bytes_received
            .fetch_add(bytes, Ordering::Relaxed);
        self.record_per_peer(remote_addr, bytes, |s| &s.bytes_received);
    }

    /// Read cumulative bytes downloaded without resetting counters.
    pub fn cumulative_bytes_received(&self) -> u64 {
        self.cumulative_bytes_received.load(Ordering::Relaxed)
    }

    /// Record per-peer bytes for the given direction.
    ///
    /// Bounded to [`MAX_TRACKED_PEERS`] entries with LRU eviction: when the
    /// table is full, the entry whose `last_seen_tick` is smallest is
    /// removed before the new entry is inserted. This keeps the dashboard's
    /// per-peer view current as peers come and go without unbounded growth,
    /// and combined with the post-authentication call site in
    /// [`Self::record_packet_received`] closes the spoof inflation vector
    /// described in #3999.
    fn record_per_peer(
        &self,
        addr: SocketAddr,
        bytes: u64,
        field: impl Fn(&PeerTransferStats) -> &AtomicU64,
    ) {
        let tick = self.per_peer_tick.fetch_add(1, Ordering::Relaxed) + 1;

        if let Some(entry) = self.per_peer_stats.get(&addr) {
            field(&entry).fetch_add(bytes, Ordering::Relaxed);
            entry.last_seen_tick.store(tick, Ordering::Relaxed);
            return;
        }

        // The capacity check / eviction / insert isn't atomic across threads,
        // so the table may transiently hold up to MAX_TRACKED_PEERS + a small
        // burst — acceptable for a dashboard counter.
        if self.per_peer_stats.len() >= MAX_TRACKED_PEERS {
            self.evict_oldest_peer();
        }
        let entry = self.per_peer_stats.entry(addr).or_default();
        field(&entry).fetch_add(bytes, Ordering::Relaxed);
        entry.last_seen_tick.store(tick, Ordering::Relaxed);
    }

    /// Remove the per-peer entry whose `last_seen_tick` is smallest.
    ///
    /// O(MAX_TRACKED_PEERS) but called only on insert into a full table.
    fn evict_oldest_peer(&self) {
        let oldest = self
            .per_peer_stats
            .iter()
            .min_by_key(|entry| entry.value().last_seen_tick.load(Ordering::Relaxed))
            .map(|entry| *entry.key());
        if let Some(addr) = oldest {
            self.per_peer_stats.remove(&addr);
        }
    }

    /// Remove a peer's per-peer stats.
    ///
    /// Called when the connection to `addr` is torn down so the slot becomes
    /// available for a new peer immediately, instead of relying on LRU
    /// eviction to clean up after the connection-level state is gone.
    pub fn remove_peer(&self, addr: SocketAddr) {
        self.per_peer_stats.remove(&addr);
    }

    /// Read-only snapshot for the local dashboard. Does NOT reset counters
    /// (unlike `take_snapshot` which is consumed by the telemetry worker).
    ///
    /// **Telemetry interaction**: `peak_throughput_bps`, `avg_cwnd_bytes`,
    /// `avg_rtt_us`, and `slowdowns_triggered` are period accumulators that
    /// `take_snapshot` resets every `transport_snapshot_interval_secs`
    /// (default 30s).  Between resets these reflect recent activity; the
    /// dashboard sees the current window, not a lifetime aggregate.
    /// `cumulative_bytes_sent/received` are never reset and reflect totals.
    pub fn read_snapshot(&self) -> TransportSnapshot {
        let transfers_completed = self.transfers_completed.load(Ordering::Relaxed);
        let transfers_failed = self.transfers_failed.load(Ordering::Relaxed);
        let total_transfer_time_ms = self.total_transfer_time_ms.load(Ordering::Relaxed);
        let peak_throughput_bps = self.peak_throughput_bps.load(Ordering::Relaxed);
        let peak_cwnd_bytes = self.peak_cwnd_bytes.load(Ordering::Relaxed);
        let min_cwnd_bytes = self.min_cwnd_bytes.load(Ordering::Relaxed);
        let cwnd_sum = self.cwnd_sum.load(Ordering::Relaxed);
        let cwnd_samples = self.cwnd_samples.load(Ordering::Relaxed);
        let slowdowns_triggered = self.slowdowns_triggered.load(Ordering::Relaxed);
        let min_rtt_us = self.min_rtt_us.load(Ordering::Relaxed);
        let max_rtt_us = self.max_rtt_us.load(Ordering::Relaxed);
        let rtt_sum_us = self.rtt_sum_us.load(Ordering::Relaxed);
        let rtt_samples = self.rtt_samples.load(Ordering::Relaxed);

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

        TransportSnapshot {
            transfers_completed,
            transfers_failed,
            // Delta fields not applicable to non-resetting reads —
            // `take_snapshot` computes these from period accumulators,
            // but a read-only snapshot has no meaningful interval.
            bytes_sent: 0,
            bytes_received: 0,
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
        }
    }

    /// Snapshot per-peer transfer stats: `(addr, bytes_sent, bytes_received)`.
    pub fn per_peer_snapshot(&self) -> Vec<(SocketAddr, u64, u64)> {
        self.per_peer_stats
            .iter()
            .map(|entry| {
                let addr = *entry.key();
                let sent = entry.value().bytes_sent.load(Ordering::Relaxed);
                let recv = entry.value().bytes_received.load(Ordering::Relaxed);
                (addr, sent, recv)
            })
            .collect()
    }

    /// Take a snapshot and reset all counters.
    ///
    /// Returns `None` if there was no activity during the period.
    pub fn take_snapshot(&self) -> Option<TransportSnapshot> {
        // Read and reset counters
        let transfers_completed = self.transfers_completed.swap(0, Ordering::Relaxed);
        let transfers_failed = self.transfers_failed.swap(0, Ordering::Relaxed);
        // RTT samples count as activity in their own right: quiet connections
        // that only exchange keep-alive ping/pong traffic record RTT samples
        // but complete no transfers (#4000). Gating snapshot emission solely
        // on transfers would discard (and reset) those samples here, so the
        // telemetry worker would never see RTT data for a quiet node — the
        // exact under-counting this is meant to fix.
        let rtt_samples = self.rtt_samples.swap(0, Ordering::Relaxed);

        // No activity = no snapshot
        if transfers_completed == 0 && transfers_failed == 0 && rtt_samples == 0 {
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
            // Deliberately DO NOT reset the RTT accumulators (`rtt_sum_us`,
            // `min_rtt_us`, `max_rtt_us`) here. `rtt_samples` was already
            // swapped to 0; if a concurrent `record_rtt_sample` lands between
            // that swap and this point, it bumps `rtt_sum_us`/min/max *and*
            // `rtt_samples` (back to 1). Zeroing the accumulators here would
            // then leave a nonzero sample count paired with a zeroed
            // sum/min/max, so the next snapshot would emit a bogus
            // avg/min/max = 0. Since these accumulators are only ever advanced
            // alongside a sample, they are already at their identity values
            // (0 / u64::MAX / 0) in the genuine no-activity case, making the
            // stores redundant anyway. Leaving them keeps the count and the
            // accumulators mutually consistent under concurrency.
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
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
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
        // Fire-and-forget: try_send avoids blocking the transport layer; channel full means telemetry drops
        #[allow(clippy::let_underscore_must_use)]
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
        // Fire-and-forget: try_send avoids blocking the transport layer; channel full means telemetry drops
        #[allow(clippy::let_underscore_must_use)]
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
        // Fire-and-forget: try_send avoids blocking the transport layer; channel full means telemetry drops
        #[allow(clippy::let_underscore_must_use)]
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
            final_flightsize: 0,
            configured_rate: 0,
        };

        metrics.record_transfer_completed(&stats);

        let snapshot = metrics.take_snapshot().expect("should have snapshot");
        assert_eq!(snapshot.transfers_completed, 1);
        assert_eq!(snapshot.bytes_sent, 1024);
        assert_eq!(snapshot.peak_cwnd_bytes, 50000);
        assert_eq!(snapshot.slowdowns_triggered, 1);
    }

    /// Regression test for #4000: a quiet connection that records RTT samples
    /// from the ping/pong keep-alive cycle but completes no transfers must
    /// still produce a telemetry snapshot. Without counting RTT samples as
    /// activity, `take_snapshot` would early-return `None` and reset the RTT
    /// counters, silently discarding exactly the quiet-connection RTT data the
    /// keep-alive sampling path is meant to surface.
    #[test]
    fn test_snapshot_emitted_for_rtt_only_activity() {
        let metrics = TransportMetrics::new();

        // No transfers — only a keep-alive RTT sample, as a quiet connection
        // would produce.
        metrics.record_rtt_sample(42_000);

        let snapshot = metrics
            .take_snapshot()
            .expect("RTT-only activity must still produce a snapshot");
        assert_eq!(snapshot.transfers_completed, 0);
        assert_eq!(snapshot.transfers_failed, 0);
        assert_eq!(snapshot.avg_rtt_us, 42_000);
        assert_eq!(snapshot.min_rtt_us, 42_000);
        assert_eq!(snapshot.max_rtt_us, 42_000);

        // Counters reset: a subsequent snapshot with no new activity is None.
        assert!(metrics.take_snapshot().is_none());
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
            final_flightsize: 0,
            configured_rate: 0,
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
                final_flightsize: 0,
                configured_rate: 0,
            };
            metrics.record_transfer_completed(&stats);
        }

        let snapshot = metrics.take_snapshot().expect("should have snapshot");
        assert_eq!(snapshot.transfers_completed, 5);
        assert_eq!(snapshot.bytes_sent, 5000);
        assert_eq!(snapshot.peak_cwnd_bytes, 44000); // Max of all peaks
        assert_eq!(snapshot.slowdowns_triggered, 5);
    }

    #[test]
    fn test_inbound_completed_updates_only_snapshot_bytes() {
        // record_inbound_completed must NOT touch the cumulative or per-peer
        // wire-byte counters — those are owned by record_packet_received and
        // updated at the socket layer. This test pins that contract so the
        // double-counting bug (counted both at socket layer and at stream
        // completion) cannot regress.
        let metrics = TransportMetrics::new();

        metrics.record_inbound_completed(2048);
        metrics.record_inbound_completed(1024);

        assert_eq!(
            metrics.cumulative_bytes_received(),
            0,
            "cumulative is wire-byte only; stream completion must not touch it"
        );
        assert!(
            metrics.per_peer_snapshot().is_empty(),
            "per-peer is wire-byte only; stream completion must not touch it"
        );

        // The snapshot bytes_received field still aggregates transfer payloads
        // alongside transfers_completed for telemetry.
        let stats = crate::transport::TransferStats {
            stream_id: 1,
            remote_addr: "10.0.0.1:5000".parse().unwrap(),
            bytes_transferred: 100,
            elapsed: Duration::from_millis(10),
            peak_cwnd_bytes: 1000,
            final_cwnd_bytes: 1000,
            slowdowns_triggered: 0,
            base_delay: Duration::from_millis(5),
            final_ssthresh_bytes: 100000,
            min_ssthresh_floor_bytes: 5696,
            total_timeouts: 0,
            final_flightsize: 0,
            configured_rate: 0,
        };
        metrics.record_transfer_completed(&stats);
        let snapshot = metrics.take_snapshot().unwrap();
        assert_eq!(snapshot.bytes_received, 3072);
    }

    #[test]
    fn test_packet_sent_updates_cumulative_and_per_peer() {
        let metrics = TransportMetrics::new();
        let addr: std::net::SocketAddr = "10.0.0.1:5000".parse().unwrap();

        // Simulate small control packets that never trigger streaming —
        // historically these were invisible to the dashboard. After the fix
        // they must show up.
        metrics.record_packet_sent(addr, 64);
        metrics.record_packet_sent(addr, 200);

        assert_eq!(metrics.cumulative_bytes_sent(), 264);

        let peers = metrics.per_peer_snapshot();
        let peer = peers.iter().find(|(a, _, _)| *a == addr).expect("tracked");
        assert_eq!(peer.1, 264, "per-peer sent must reflect packet bytes");
        assert_eq!(peer.2, 0);
    }

    #[test]
    fn test_packet_received_updates_cumulative_and_per_peer() {
        let metrics = TransportMetrics::new();
        let addr: std::net::SocketAddr = "10.0.0.1:5000".parse().unwrap();

        metrics.record_packet_received(addr, 128);
        metrics.record_packet_received(addr, 256);

        assert_eq!(metrics.cumulative_bytes_received(), 384);

        let peers = metrics.per_peer_snapshot();
        let peer = peers.iter().find(|(a, _, _)| *a == addr).expect("tracked");
        assert_eq!(peer.1, 0);
        assert_eq!(peer.2, 384);
    }

    #[test]
    fn test_packet_metrics_survive_snapshot_reset() {
        let metrics = TransportMetrics::new();
        let addr: std::net::SocketAddr = "10.0.0.1:5000".parse().unwrap();

        metrics.record_packet_sent(addr, 500);
        metrics.record_packet_received(addr, 700);

        // Force a snapshot via a transfer completion.
        let stats = crate::transport::TransferStats {
            stream_id: 1,
            remote_addr: addr,
            bytes_transferred: 100,
            elapsed: Duration::from_millis(10),
            peak_cwnd_bytes: 1000,
            final_cwnd_bytes: 1000,
            slowdowns_triggered: 0,
            base_delay: Duration::from_millis(5),
            final_ssthresh_bytes: 100000,
            min_ssthresh_floor_bytes: 5696,
            total_timeouts: 0,
            final_flightsize: 0,
            configured_rate: 0,
        };
        metrics.record_transfer_completed(&stats);
        let _ = metrics.take_snapshot();

        // Cumulative + per-peer wire bytes survive the snapshot reset.
        assert_eq!(metrics.cumulative_bytes_sent(), 500);
        assert_eq!(metrics.cumulative_bytes_received(), 700);
        let peers = metrics.per_peer_snapshot();
        let peer = peers.iter().find(|(a, _, _)| *a == addr).expect("tracked");
        assert_eq!(peer.1, 500);
        assert_eq!(peer.2, 700);
    }

    #[test]
    fn test_per_peer_capacity_bound() {
        let metrics = TransportMetrics::new();

        // Fill to capacity
        for i in 0..MAX_TRACKED_PEERS {
            let addr: std::net::SocketAddr = format!("10.0.{}.{}:{}", i / 256, i % 256, 5000 + i)
                .parse()
                .unwrap();
            metrics.record_packet_received(addr, 100);
        }

        assert_eq!(metrics.per_peer_snapshot().len(), MAX_TRACKED_PEERS);

        // Beyond capacity: cumulative still counts AND the new peer is now
        // tracked because LRU evicts the oldest entry — that's the #3999
        // behavior change. Pre-fix the new peer would have been silently
        // dropped from the per-peer view.
        let extra: std::net::SocketAddr = "192.168.1.1:9999".parse().unwrap();
        metrics.record_packet_received(extra, 500);

        assert_eq!(metrics.per_peer_snapshot().len(), MAX_TRACKED_PEERS);
        assert!(
            metrics
                .per_peer_snapshot()
                .iter()
                .any(|(a, _, _)| *a == extra),
            "new peer must be tracked after LRU evicts the oldest entry"
        );

        let total: u64 = (MAX_TRACKED_PEERS as u64 * 100) + 500;
        assert_eq!(metrics.cumulative_bytes_received(), total);
    }

    #[test]
    fn test_per_peer_lru_evicts_oldest() {
        // Fill the table, then record activity on the second-oldest entry to
        // refresh its tick. The first-inserted entry must be the one evicted
        // when a brand-new peer is recorded. This pins the LRU ordering
        // (least-recently-updated, not least-recently-inserted).
        let metrics = TransportMetrics::new();

        let oldest: std::net::SocketAddr = "10.0.0.1:1000".parse().unwrap();
        metrics.record_packet_received(oldest, 1);

        let next_oldest: std::net::SocketAddr = "10.0.0.2:1000".parse().unwrap();
        metrics.record_packet_received(next_oldest, 1);

        // Fill the rest of the table.
        for i in 2..MAX_TRACKED_PEERS {
            let addr: std::net::SocketAddr = format!("10.0.{}.{}:{}", i / 256, i % 256, 5000 + i)
                .parse()
                .unwrap();
            metrics.record_packet_received(addr, 1);
        }
        assert_eq!(metrics.per_peer_snapshot().len(), MAX_TRACKED_PEERS);

        // Refresh `next_oldest` so it is no longer the LRU candidate.
        metrics.record_packet_received(next_oldest, 1);

        // Insert a new peer. LRU eviction should remove `oldest`.
        let newcomer: std::net::SocketAddr = "192.168.7.7:9999".parse().unwrap();
        metrics.record_packet_received(newcomer, 1);

        let peers = metrics.per_peer_snapshot();
        assert_eq!(peers.len(), MAX_TRACKED_PEERS);
        assert!(
            peers.iter().all(|(a, _, _)| *a != oldest),
            "the least-recently-updated entry must be evicted"
        );
        assert!(
            peers.iter().any(|(a, _, _)| *a == next_oldest),
            "the recently-refreshed entry must survive eviction"
        );
        assert!(
            peers.iter().any(|(a, _, _)| *a == newcomer),
            "the new entry must be tracked after eviction"
        );
    }

    #[test]
    fn test_remove_peer_frees_slot() {
        // Disconnecting a peer must immediately free its per-peer slot so
        // the bounded table doesn't accumulate stale entries on long-running
        // gateways.
        let metrics = TransportMetrics::new();

        let addr: std::net::SocketAddr = "10.0.0.5:5000".parse().unwrap();
        metrics.record_packet_received(addr, 100);
        assert_eq!(metrics.per_peer_snapshot().len(), 1);

        metrics.remove_peer(addr);
        assert!(
            metrics.per_peer_snapshot().is_empty(),
            "remove_peer must drop the entry"
        );

        // Cumulative counters are wire-byte history and intentionally do
        // NOT roll back when a peer entry is removed.
        assert_eq!(metrics.cumulative_bytes_received(), 100);
    }

    #[test]
    fn test_remove_peer_is_idempotent() {
        // Removing a non-existent peer is a no-op (e.g. when
        // record_peer_disconnected fires for a peer whose stats slot was
        // already evicted by LRU).
        let metrics = TransportMetrics::new();
        let addr: std::net::SocketAddr = "10.0.0.99:9999".parse().unwrap();
        metrics.remove_peer(addr); // must not panic / corrupt state
        assert!(metrics.per_peer_snapshot().is_empty());

        metrics.record_packet_received(addr, 50);
        assert_eq!(metrics.per_peer_snapshot().len(), 1);
        metrics.remove_peer(addr);
        metrics.remove_peer(addr); // second remove must be a no-op
        assert!(metrics.per_peer_snapshot().is_empty());
    }

    /// Sentinel values (`u32::MAX` / `u64::MAX`) must be mapped to 0 in the
    /// read-snapshot — the dashboard displays them as "0ms" / "0 B", not
    /// garbage. No samples are ever recorded, so the sentinels survive.
    #[test]
    fn read_snapshot_sentinels_map_to_zero() {
        let metrics = TransportMetrics::new();
        let snap = metrics.read_snapshot();
        assert_eq!(snap.min_cwnd_bytes, 0, "no cwnd samples → min should be 0");
        assert_eq!(snap.min_rtt_us, 0, "no RTT samples → min should be 0");
    }

    /// Happy-path: record known cwnd samples, verify read_snapshot
    /// returns correct average.
    #[test]
    fn read_snapshot_happy_path() {
        let metrics = TransportMetrics::new();
        metrics.record_cwnd_sample(4000);
        metrics.record_cwnd_sample(2000);
        metrics.record_cwnd_sample(6000);
        let snap = metrics.read_snapshot();
        assert_eq!(snap.avg_cwnd_bytes, 4000);
    }
}
