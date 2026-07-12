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
//!
//! The probe also sums tx across *all* non-loopback interfaces, including
//! virtual ones (docker/veth/bridge/bonded). On a container host a packet
//! can traverse `docker0` → `eno1` and be counted on both, inflating
//! `total` (and therefore `op`). The intended targets are bare-metal
//! gateways where this is a non-issue; on container hosts treat `op` as an
//! upper bound on competing traffic, not an exact figure.

use std::time::Duration;

use crate::node::background_task_monitor::BackgroundTaskMonitor;
use crate::simulation::{RealTime, TimeSource};
use crate::transport::TRANSPORT_METRICS;
use crate::transport::shadow_stats::{SHADOW_ROLLUP_WINDOW_SECS, WindowedStat};

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
        // Monotonic clock (tokio-backed, so it advances with paused time in
        // tests and reflects true wall-clock — including executor stalls — in
        // production). The elapsed interval used to normalize a delta to a
        // per-second rate MUST come from this clock, NOT a tick counter: under
        // `MissedTickBehavior::Delay` a stalled executor makes one `tick()`
        // span several wall-clock seconds, so a tick counter would divide by
        // too few seconds and still fake a burst.
        let time = RealTime::new();
        let mut prev_total = read_total_tx_bytes().await;
        let mut prev_own = TRANSPORT_METRICS.cumulative_bytes_sent();
        // Wall-clock nanos when the baseline (`prev_*`) was last taken. A failed
        // `/proc/net/dev` read does NOT advance the baseline, so a gap of failed
        // reads (or a stall) makes the next successful interval span several
        // seconds; dividing the delta by the real elapsed interval normalizes
        // it back to a per-second rate rather than a single one-second burst.
        let mut baseline_at_nanos = time.now_nanos();
        let mut window = IfaceWindow::default();

        loop {
            ticker.tick().await;
            let now_total = read_total_tx_bytes().await;
            let now_own = TRANSPORT_METRICS.cumulative_bytes_sent();
            let now_nanos = time.now_nanos();

            if let (Some(prev), Some(now)) = (prev_total, now_total) {
                // Bytes over the (possibly multi-second) interval, divided down
                // to a per-second rate. Both counters share the same interval,
                // so `op` stays consistent.
                let interval_secs = elapsed_secs(baseline_at_nanos, now_nanos);
                let total_delta = now.saturating_sub(prev);
                let own_delta = now_own.saturating_sub(prev_own);
                let op_delta = iface_op(total_delta, own_delta);
                window.record(
                    per_sec(total_delta, interval_secs),
                    per_sec(own_delta, interval_secs),
                    per_sec(op_delta, interval_secs),
                );
            }
            window.ticks += 1;
            if window.ticks >= SHADOW_ROLLUP_WINDOW_SECS {
                emit_iface_rollup(&local_peer_id, &window);
                window = IfaceWindow::default();
            }
            // Advance the baseline only on a successful read so that a
            // failed tick is bridged (the next success measures over the
            // longer gap for BOTH counters consistently) rather than
            // producing a misattributed delta.
            if now_total.is_some() {
                prev_total = now_total;
                prev_own = now_own;
                baseline_at_nanos = now_nanos;
            }
        }
    });
    monitor.register("shadow_iface_tx_monitor", handle);
}

/// Windowed rollup accumulator for the `shadow_iface_tx` stream.
#[derive(Default)]
struct IfaceWindow {
    /// Total 1 Hz ticks in this window (including failed reads), used only to
    /// close the [`SHADOW_ROLLUP_WINDOW_SECS`] window.
    ticks: u32,
    /// Ticks that produced a sample (a successful `/proc/net/dev` read).
    samples: u32,
    total_tx_bytes: WindowedStat,
    own_tx_bytes: WindowedStat,
    op_tx_bytes: WindowedStat,
}

impl IfaceWindow {
    fn record(&mut self, total: u64, own: u64, op: u64) {
        self.samples += 1;
        self.total_tx_bytes.record(total);
        self.own_tx_bytes.record(own);
        self.op_tx_bytes.record(op);
    }
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

/// `op = total_tx − freenet_own_tx`, the bytes attributable to traffic
/// other than Freenet. `saturating_sub` keeps it at 0 when `own` exceeds
/// `total` — which can happen across a skewed read window or because
/// `own` counts UDP payload while the interface counter and Freenet's own
/// header overhead interact (see the module-level accounting caveat).
fn iface_op(total_delta: u64, own_delta: u64) -> u64 {
    total_delta.saturating_sub(own_delta)
}

/// Normalize an interval byte delta to a per-second rate.
///
/// A failed `/proc/net/dev` read does not advance the baseline, so after a gap
/// of failed reads (or an executor stall) the next successful interval spans
/// several seconds. Dividing the delta by that elapsed interval keeps the
/// recorded value a true per-second rate: without it, a whole multi-second byte
/// count would be folded into a single one-second sample and reported as a fake
/// burst (inflating both the window mean and its `max`). `interval_secs` is
/// clamped to at least 1 to avoid a divide-by-zero on the normal every-second
/// path (and when [`elapsed_secs`] rounds a sub-second interval to 0).
fn per_sec(delta: u64, interval_secs: u64) -> u64 {
    delta / interval_secs.max(1)
}

/// Whole seconds elapsed between two monotonic-clock nanosecond readings,
/// rounded to nearest.
///
/// This is the interval [`per_sec`] divides by. It is derived from the
/// [`TimeSource`] clock, NOT the tick count, on purpose: under
/// `MissedTickBehavior::Delay` a stalled executor can make one ticker `tick()`
/// span several wall-clock seconds, so a tick counter would report `1` and
/// [`per_sec`] would divide by too few seconds — still faking a burst. The
/// monotonic clock reflects the true elapsed interval, so the stall normalizes
/// correctly. A sub-second interval rounds to 0; `per_sec`'s `.max(1)` then
/// treats it as one second.
fn elapsed_secs(baseline_nanos: u64, now_nanos: u64) -> u64 {
    let elapsed = now_nanos.saturating_sub(baseline_nanos);
    // Round to nearest second (add half a second before the integer divide).
    (elapsed + 500_000_000) / 1_000_000_000
}

/// Emit one `shadow_iface_tx` rollup covering the closed window. Skips
/// emission when no `/proc/net/dev` read succeeded in the whole window
/// (mirroring the original per-tick "omit on failed read"). Each field keeps
/// the window mean per-second rate under its original name; `*_p50` (robust
/// central tendency for a bursty rate) and `*_max` (the busiest second's
/// transmit / competing-traffic peak) are the additive distribution fields the
/// #4074 saturation-attribution analysis consumes.
fn emit_iface_rollup(local_peer_id: &str, window: &IfaceWindow) {
    if window.samples == 0 {
        return;
    }
    // Record the Option<u64> means directly (NOT via `?`): tracing renders a
    // `Some(n)` as the bare number and omits the field for `None`, matching the
    // OTLP number-or-null. `?` would emit the literal `Some(n)` and break
    // structured local-log parsers.
    tracing::debug!(
        target: "freenet::transport::shadow_iface_tx",
        total_tx_bytes = window.total_tx_bytes.mean(),
        own_tx_bytes = window.own_tx_bytes.mean(),
        op_tx_bytes = window.op_tx_bytes.mean(),
        window_secs = SHADOW_ROLLUP_WINDOW_SECS,
        "shadow_iface_tx"
    );
    crate::tracing::telemetry::send_standalone_shadow_event_with_peer_id(
        "shadow_iface_tx",
        local_peer_id,
        iface_rollup_json(window),
    );
}

/// Build the `shadow_iface_tx` rollup JSON. Pure so the schema (compat field =
/// window mean per-second rate, additive `*_p50` / `*_max` distribution fields)
/// is unit-testable without the telemetry sender.
fn iface_rollup_json(window: &IfaceWindow) -> serde_json::Value {
    serde_json::json!({
        "total_tx_bytes_per_sec": window.total_tx_bytes.mean(),
        "total_tx_bytes_per_sec_p50": window.total_tx_bytes.p50(),
        "total_tx_bytes_per_sec_max": window.total_tx_bytes.max(),
        "freenet_own_tx_bytes_per_sec": window.own_tx_bytes.mean(),
        "freenet_own_tx_bytes_per_sec_p50": window.own_tx_bytes.p50(),
        "freenet_own_tx_bytes_per_sec_max": window.own_tx_bytes.max(),
        "op_tx_bytes_per_sec": window.op_tx_bytes.mean(),
        "op_tx_bytes_per_sec_p50": window.op_tx_bytes.p50(),
        "op_tx_bytes_per_sec_max": window.op_tx_bytes.max(),
        "window_secs": SHADOW_ROLLUP_WINDOW_SECS,
        "samples": window.samples,
    })
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

    #[test]
    fn parse_handles_colon_glued_to_first_counter() {
        // On busy interfaces the kernel does not pad, so a large rx_bytes
        // can abut the colon with no separating space, e.g.
        // `eth0:123456789 ...`. `split_once(':')` removes the colon and
        // `rest` then starts with rx_bytes, so field index 8 still lands
        // on tx_bytes. Pin that the no-space-after-colon form parses
        // identically to the spaced form.
        let glued = "eth0:100 1 2 3 4 5 6 7 9999 10\n";
        assert_eq!(parse_total_tx_bytes(glued), Some(9999));
    }

    #[test]
    fn parse_handles_huge_counters_without_overflow() {
        // 64-bit interface counters near u64::MAX must parse and sum
        // without panicking (saturating_add guards the sum).
        let big = u64::MAX - 1;
        let input = format!("eth0: 1 2 3 4 5 6 7 8 {big} 9\neth1: 1 2 3 4 5 6 7 8 5 9\n");
        assert_eq!(parse_total_tx_bytes(&input), Some(u64::MAX));
    }

    #[test]
    fn iface_op_is_total_minus_own() {
        assert_eq!(iface_op(10_000, 3_000), 7_000);
        assert_eq!(iface_op(0, 0), 0);
    }

    #[test]
    fn iface_op_saturates_when_own_exceeds_total() {
        // The skewed-window case the module rustdoc promises to handle:
        // own > total must clamp op to 0, never wrap.
        assert_eq!(iface_op(1_000, 4_000), 0);
    }

    #[test]
    fn per_sec_normalizes_bridged_gap_to_a_rate() {
        // Normal every-second path: delta is already a per-second rate.
        assert_eq!(per_sec(9_000, 1), 9_000);
        // A gap: 29 failed reads then one success bridges a 30 s interval, so
        // the 30 s byte count must be divided back to a per-second rate rather
        // than recorded as a single one-second burst.
        assert_eq!(per_sec(300_000, 30), 10_000);
        // Divide-by-zero guard (interval clamped to >= 1).
        assert_eq!(per_sec(500, 0), 500);
    }

    #[test]
    fn elapsed_secs_uses_wall_clock_not_tick_count() {
        // Normal ~1 s interval.
        assert_eq!(elapsed_secs(0, 1_000_000_000), 1);
        // Executor stall: one tick spanning ~5 wall-clock seconds must yield 5,
        // NOT 1 (which a tick counter would give), so per_sec divides correctly
        // and a 5 s byte delta is not reported as a single 1 s burst.
        assert_eq!(elapsed_secs(0, 5_000_000_000), 5);
        let stalled_delta = 500_000u64; // ~5 s of bytes
        assert_eq!(
            per_sec(stalled_delta, elapsed_secs(0, 5_000_000_000)),
            100_000,
            "a 5 s stall normalizes to the real per-second rate, not a 5x burst"
        );
        // A 30 s bridged gap (baseline offset to prove it is a difference).
        assert_eq!(elapsed_secs(1_000_000_000, 31_000_000_000), 30);
        // Rounds to nearest second.
        assert_eq!(elapsed_secs(0, 1_400_000_000), 1);
        assert_eq!(elapsed_secs(0, 1_600_000_000), 2);
        // Sub-second rounds to 0; per_sec's .max(1) then treats it as 1 s.
        assert_eq!(elapsed_secs(0, 200_000_000), 0);
        assert_eq!(per_sec(500, elapsed_secs(0, 200_000_000)), 500);
    }

    #[test]
    fn bridged_gap_does_not_fake_a_burst_in_the_rollup() {
        // Regression: before the per-second normalization, a window with 29
        // failed reads and one success recorded the whole ~30 s byte count as a
        // single 1 s sample, so mean == max == the full multi-second burst.
        // With normalization the single bridged sample is a true per-second
        // rate, so no fake burst appears.
        let mut window = IfaceWindow::default();
        // 29 failed reads contribute no sample but count as elapsed ticks.
        for _ in 0..29 {
            window.ticks += 1;
        }
        // One success bridging a 30 s interval carrying 300_000 total bytes.
        let interval = 30u64;
        window.record(per_sec(300_000, interval), per_sec(60_000, interval), 0);
        window.ticks += 1;

        let json = iface_rollup_json(&window);
        // 300_000 / 30 = 10_000 bytes/sec, NOT the raw 300_000 burst.
        assert_eq!(json["total_tx_bytes_per_sec"], 10_000);
        assert_eq!(json["total_tx_bytes_per_sec_max"], 10_000);
        assert_eq!(json["freenet_own_tx_bytes_per_sec"], 2_000);
        // Only the one successful read counts as a sample; the 29 gaps do not.
        assert_eq!(json["samples"], 1);
        assert_eq!(json["window_secs"], SHADOW_ROLLUP_WINDOW_SECS);
    }

    #[test]
    fn iface_rollup_json_includes_p50_for_byte_rates() {
        let mut window = IfaceWindow::default();
        window.record(10_000, 4_000, 6_000);
        window.record(30_000, 4_000, 26_000);
        window.ticks += 2;
        let json = iface_rollup_json(&window);
        assert_eq!(json["total_tx_bytes_per_sec"], 20_000); // mean
        // Upper-middle median of [10_000, 30_000] = sorted[1] = 30_000.
        assert_eq!(json["total_tx_bytes_per_sec_p50"], 30_000);
        assert_eq!(json["total_tx_bytes_per_sec_max"], 30_000);
        assert_eq!(json["op_tx_bytes_per_sec_p50"], 26_000);
        assert_eq!(json["samples"], 2);
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
