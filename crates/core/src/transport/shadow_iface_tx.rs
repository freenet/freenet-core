//! Phase 1.6 OS-interface tx-bytes shadow telemetry for the outer-loop
//! rate controller (issue #4074).
//!
//! Cross-connection RTT (Phase 1) and reference-ping (Phase 1.5) can tell
//! us *that* the local uplink is contended, but not *who* is contending.
//! The one signal that disambiguates "Freenet is saturating its own link"
//! from "the operator's other apps are competing" is the aggregate OS
//! interface transmit counter:
//!
//! ```text
//! op = total_interface_tx − freenet_own_tx
//! ```
//!
//! where `freenet_own_tx` is `TRANSPORT_METRICS.cumulative_bytes_sent`
//! (every byte Freenet put on the wire) and `total_interface_tx` is the
//! sum of `tx_bytes` across all non-loopback interfaces from Linux
//! `/proc/net/dev`. A large `op` while the uplink is saturated means the
//! operator's own traffic is the cause; a small `op` means Freenet is.
//!
//! **Best-effort and opt-in.** This probe is gated behind the bare
//! `telemetry.iface-tx-enabled` flag (default `false`), the same way
//! reference-ping is gated, and only spawned when telemetry is enabled and
//! the node is not a test environment. If `/proc/net/dev` is unavailable
//! (non-Linux, sandbox, restricted), each read returns `None` and that
//! tick is silently omitted — the loop never blocks and never crashes.
//!
//! **Observation only.** Like every other #4074 shadow signal, nothing in
//! the production data path reads this; the rule in
//! `.claude/rules/transport.md` applies.
//!
//! ## Accounting caveat
//!
//! `cumulative_bytes_sent` counts UDP *payload* bytes, whereas the
//! interface counter includes the IP + UDP headers (28 bytes/packet) that
//! Freenet's own packets also carry. So `op = total − own` slightly
//! over-attributes Freenet's own header overhead to "other" traffic — a
//! small, roughly-constant offset that does not affect the
//! saturation-attribution question. `saturating_sub` keeps `op` at 0 in
//! the rare case the two counters are read across a skewed window and
//! `own` momentarily exceeds `total`.

use std::time::Duration;

use crate::node::background_task_monitor::BackgroundTaskMonitor;
use crate::transport::TRANSPORT_METRICS;

/// 1 Hz cadence, matching the other shadow aggregators so the streams
/// align at the collector.
const MONITOR_INTERVAL: Duration = Duration::from_secs(1);

/// Linux per-interface byte/packet counters.
const PROC_NET_DEV: &str = "/proc/net/dev";

/// Spawn the `shadow_iface_tx` monitor and register it with the
/// `BackgroundTaskMonitor`. Call once at node startup *only when the
/// iface-tx flag is enabled* (the caller in `p2p_impl.rs` gates this the
/// same way as reference-ping).
///
/// The task takes a baseline reading, then once per second reads the
/// interface counter again and emits the per-interval deltas. A failed
/// read omits that tick and leaves the baseline untouched, so a transient
/// failure cannot produce a spurious huge delta on recovery.
pub(crate) fn spawn_iface_tx_monitor(local_peer_id: String, monitor: &BackgroundTaskMonitor) {
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(MONITOR_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the immediate first tick, then take the baseline aligned to
        // the cadence. `prev_total` and `prev_own` are sampled together so
        // their deltas cover the same window.
        ticker.tick().await;
        let mut prev_total = read_total_tx_bytes().await;
        let mut prev_own = TRANSPORT_METRICS.cumulative_bytes_sent();

        loop {
            ticker.tick().await;
            let now_total = read_total_tx_bytes().await;
            let now_own = TRANSPORT_METRICS.cumulative_bytes_sent();

            if let (Some(prev), Some(now)) = (prev_total, now_total) {
                let total_delta = now.saturating_sub(prev);
                let own_delta = now_own.saturating_sub(prev_own);
                let op_delta = total_delta.saturating_sub(own_delta);
                emit_iface_snapshot(&local_peer_id, total_delta, own_delta, op_delta);
            }
            // Advance the baseline only on a successful read so that a
            // failed tick is bridged (the next success measures over the
            // longer gap for BOTH counters consistently) rather than
            // producing a misattributed delta.
            if now_total.is_some() {
                prev_total = now_total;
                prev_own = now_own;
            }
        }
    });
    monitor.register("shadow_iface_tx_monitor", handle);
}

/// Read `/proc/net/dev` and sum `tx_bytes` across non-loopback
/// interfaces. Returns `None` if the file cannot be read or no interface
/// line parses (non-Linux, sandboxed, malformed).
///
/// Uses `tokio::fs` so the (tiny, ~1 KB) procfs read is offloaded to the
/// blocking pool and never stalls the reactor.
async fn read_total_tx_bytes() -> Option<u64> {
    let contents = tokio::fs::read_to_string(PROC_NET_DEV).await.ok()?;
    parse_total_tx_bytes(&contents)
}

/// Parse `/proc/net/dev` contents and sum `tx_bytes` across all
/// non-loopback interfaces.
///
/// Line format (RFC-less kernel format) after the two header lines:
/// ```text
///   eth0: <rx_bytes> <rx_packets> <rx_errs> <rx_drop> <rx_fifo> \
///         <rx_frame> <rx_compressed> <rx_multicast> \
///         <tx_bytes> <tx_packets> ...
/// ```
/// The interface name precedes a `:`; the receive block is 8 fields, so
/// `tx_bytes` is field index 8 in the whitespace-split remainder.
fn parse_total_tx_bytes(contents: &str) -> Option<u64> {
    const TX_BYTES_FIELD: usize = 8;
    let mut total: u64 = 0;
    let mut found = false;
    for line in contents.lines() {
        let Some((iface, rest)) = line.split_once(':') else {
            // Header lines have no ':' in the name position — skip.
            continue;
        };
        let iface = iface.trim();
        // Loopback never leaves the host; exclude it from "uplink" tx.
        if iface.is_empty() || iface == "lo" {
            continue;
        }
        if let Some(tx) = rest
            .split_whitespace()
            .nth(TX_BYTES_FIELD)
            .and_then(|s| s.parse::<u64>().ok())
        {
            total = total.saturating_add(tx);
            found = true;
        }
    }
    found.then_some(total)
}

fn emit_iface_snapshot(
    local_peer_id: &str,
    total_tx_bytes: u64,
    own_tx_bytes: u64,
    op_tx_bytes: u64,
) {
    tracing::debug!(
        target: "freenet::transport::shadow_iface_tx",
        total_tx_bytes,
        own_tx_bytes,
        op_tx_bytes,
        "shadow_iface_tx"
    );
    crate::tracing::telemetry::send_standalone_event_with_peer_id(
        "shadow_iface_tx",
        local_peer_id,
        serde_json::json!({
            "total_tx_bytes_per_sec": total_tx_bytes,
            "freenet_own_tx_bytes_per_sec": own_tx_bytes,
            "op_tx_bytes_per_sec": op_tx_bytes,
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "Inter-|   Receive                                                |  Transmit\n\
         face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n\
    lo:  1000     10    0    0    0     0          0         0    1000      10    0    0    0     0       0          0\n\
  eth0: 500000   1000    0    0    0     0          0         0  250000     800    0    0    0     0       0          0\n\
  wlan0: 10     1    0    0    0     0          0         0    7500       5    0    0    0     0       0          0\n";

    #[test]
    fn parse_sums_tx_excluding_loopback() {
        // eth0 tx 250000 + wlan0 tx 7500 = 257500; lo (1000) excluded.
        assert_eq!(parse_total_tx_bytes(SAMPLE), Some(257_500));
    }

    #[test]
    fn parse_returns_none_when_no_interface_lines() {
        // Header only — no data lines means no signal.
        let headers = "Inter-|   Receive  |  Transmit\n face |bytes ... |bytes ...\n";
        assert_eq!(parse_total_tx_bytes(headers), None);
        assert_eq!(parse_total_tx_bytes(""), None);
    }

    #[test]
    fn parse_skips_only_loopback_named_lo() {
        // An interface whose name merely contains "lo" (e.g. "flannel")
        // must NOT be excluded — only the exact "lo" device is loopback.
        let input = "  lodev: 1 2 3 4 5 6 7 8 999 10\n";
        assert_eq!(parse_total_tx_bytes(input), Some(999));
    }

    #[test]
    fn parse_tolerates_short_or_garbage_lines() {
        // A line with fewer than 9 post-colon fields contributes nothing
        // but must not crash or poison the sum.
        let input = "  eth0: 1 2 3\n  eth1: 1 2 3 4 5 6 7 8 4242 9\n";
        assert_eq!(parse_total_tx_bytes(input), Some(4242));
    }

    /// `spawn_iface_tx_monitor` must keep its task alive across ticks even
    /// when `/proc/net/dev` is read every second. On non-Linux CI the read
    /// returns `None` and the tick is omitted, which also exercises the
    /// "omit, never block" path. Mirror of the reference-ping survival pin.
    #[tokio::test(start_paused = true)]
    async fn monitor_survives_multiple_ticks() {
        let monitor = BackgroundTaskMonitor::new();
        spawn_iface_tx_monitor("test-peer".to_string(), &monitor);

        tokio::time::advance(MONITOR_INTERVAL * 4 + Duration::from_millis(100)).await;
        tokio::task::yield_now().await;

        let exit = monitor.wait_for_any_exit();
        tokio::pin!(exit);
        let still_running = tokio::time::timeout(Duration::from_millis(50), &mut exit)
            .await
            .is_err();
        assert!(
            still_running,
            "iface-tx monitor task should still be alive after a few ticks"
        );
    }
}
