//! Node self-resource-utilization sampling for the internal telemetry path.
//!
//! Foundational piece (A1) of the demand-driven hosting redesign
//! (freenet/freenet-core#4642 — see `docs/design/hosting-eviction.md`). The
//! capability-relative hosting budget and its eviction policy (pieces A2/A3)
//! must be validated against a node's REAL scarcest-resource headroom, but the
//! node currently surfaces no resource-utilization metrics at all. This module
//! samples the node's own memory / CPU / bandwidth usage from cheap local
//! sources and exposes it so it can be emitted on the INTERNAL telemetry stream
//! the dashboard consumes.
//!
//! Scope guard (#4642 A1): this is deliberately NOT on the client-facing wire
//! protocol. It must never be added to `SystemMetrics`, `HostResponse`, or any
//! other `freenet-stdlib` bincode type — doing so would be a wire-format break
//! requiring separate coordination. All sampling here reads process-local
//! sources (`/proc`, in-process transport counters); nothing crosses the peer
//! wire.

use crate::transport::TRANSPORT_METRICS;

/// A point-in-time sample of the node's own resource utilization.
///
/// Every field is best-effort: the memory / CPU fields are `Option` because
/// their `/proc` sources are Linux-only and can fail to parse; the bandwidth
/// counters are always available from the in-process transport metrics.
///
/// Serialized to JSON for the telemetry stream (`resource_utilization` event).
/// It is NOT a wire-protocol type and must not be embedded in one.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub(crate) struct ResourceUtilization {
    /// Resident set size (bytes): the node process's current physical-memory
    /// footprint. `None` off Linux or if `/proc/self/statm` is unreadable.
    pub memory_rss_bytes: Option<u64>,
    /// Memory ceiling the node may use (bytes): `min(host RAM, cgroup limit)`.
    /// This is the SAME source that sizes the module cache and (piece A2) the
    /// hosting budget — [`crate::wasm_runtime::read_total_ram_bytes`] — so the
    /// dashboard can compute real headroom (`rss / limit`). `None` if it can't
    /// be determined (e.g. non-unix).
    pub memory_limit_bytes: Option<u64>,
    /// Cumulative process CPU time (user + system) in seconds since start.
    /// `None` off Linux or if `/proc/self/stat` is unreadable. Deltas between
    /// samples give CPU utilization; the raw counter is monotonic.
    pub cpu_time_seconds: Option<f64>,
    /// Cumulative bytes sent at the UDP socket layer since start (never reset).
    pub cumulative_bytes_sent: u64,
    /// Cumulative bytes received at the UDP socket layer since start (never
    /// reset).
    pub cumulative_bytes_received: u64,
}

impl ResourceUtilization {
    /// Serialize to a JSON value for the `resource_utilization` telemetry
    /// event. Infallible: a serialization failure (should never happen for
    /// this plain struct) degrades to `Value::Null` rather than panicking.
    pub(crate) fn to_telemetry_value(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

/// Sample the node's current resource utilization from cheap local sources.
///
/// Reads `/proc/self/statm` (RSS), `/proc/self/stat` (CPU time),
/// [`crate::wasm_runtime::read_total_ram_bytes`] (memory ceiling) and the
/// in-process [`TRANSPORT_METRICS`] cumulative byte counters. Never blocks on
/// the network and never panics.
pub(crate) fn sample() -> ResourceUtilization {
    ResourceUtilization {
        memory_rss_bytes: rss_bytes(),
        memory_limit_bytes: crate::wasm_runtime::read_total_ram_bytes().map(|v| v as u64),
        cpu_time_seconds: cpu_time_seconds(),
        cumulative_bytes_sent: TRANSPORT_METRICS.cumulative_bytes_sent(),
        cumulative_bytes_received: TRANSPORT_METRICS.cumulative_bytes_received(),
    }
}

/// Returns the process RSS (Resident Set Size) in bytes by reading
/// `/proc/self/statm`. Returns `None` on non-Linux platforms or if the read
/// fails.
pub(crate) fn rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let rss_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        // SAFETY: `sysconf(_SC_PAGESIZE)` is a POSIX-defined, signal-safe call
        // that reads a system constant and has no preconditions.
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size > 0 {
            Some(rss_pages * page_size as u64)
        } else {
            None
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Returns cumulative process CPU time (user + system) in seconds by reading
/// `/proc/self/stat`. Returns `None` on non-Linux platforms or if the read /
/// parse fails.
pub(crate) fn cpu_time_seconds() -> Option<f64> {
    #[cfg(target_os = "linux")]
    {
        let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
        let ticks = parse_proc_stat_cpu_ticks(&stat)?;
        // SAFETY: `sysconf(_SC_CLK_TCK)` is a POSIX-defined, signal-safe call
        // reading a system constant; no preconditions, no pointers.
        let clk_tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        if clk_tck > 0 {
            Some(ticks as f64 / clk_tck as f64)
        } else {
            None
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Pure parse of `utime + stime` (CPU ticks) from `/proc/self/stat` contents.
///
/// The `comm` field (field 2) is wrapped in parentheses and may itself contain
/// spaces and `)` characters, so the reliable anchor is the LAST `)` in the
/// line: after it, whitespace-separated fields resume at field 3 (`state`).
/// `utime` is field 14 and `stime` field 15 (1-indexed), i.e. the 12th and
/// 13th tokens after the last `)`.
#[cfg(target_os = "linux")]
fn parse_proc_stat_cpu_ticks(stat: &str) -> Option<u64> {
    let after_comm = stat.rsplit_once(')').map(|(_, rest)| rest.trim_start())?;
    let mut fields = after_comm.split_whitespace();
    // Field 3 (state) is index 0 here, so utime (field 14) is index 11.
    let utime: u64 = fields.nth(11)?.parse().ok()?;
    let stime: u64 = fields.next()?.parse().ok()?;
    Some(utime + stime)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The sampler populates the memory and CPU axes on Linux (CI + prod) and
    /// always populates the bandwidth counters. This is the piece-A1 guarantee:
    /// a node's resource headroom is observable rather than absent.
    #[test]
    fn sample_populates_resource_axes() {
        let sample = sample();

        // Bandwidth counters are always available (in-process, all platforms).
        // They are cumulative u64s; asserting the type is populated is enough
        // (a fresh test node may legitimately have sent zero bytes).
        let _ = sample.cumulative_bytes_sent;
        let _ = sample.cumulative_bytes_received;

        #[cfg(target_os = "linux")]
        {
            assert!(
                sample.memory_rss_bytes.is_some_and(|rss| rss > 0),
                "RSS must be sampled and non-zero on Linux, got {:?}",
                sample.memory_rss_bytes
            );
            assert!(
                sample.memory_limit_bytes.is_some_and(|limit| limit > 0),
                "memory limit (min RAM/cgroup) must be sampled on Linux, got {:?}",
                sample.memory_limit_bytes
            );
            assert!(
                sample.cpu_time_seconds.is_some_and(|cpu| cpu >= 0.0),
                "CPU time must be sampled on Linux, got {:?}",
                sample.cpu_time_seconds
            );
        }
    }

    /// The telemetry JSON carries every axis as a populated key, so the
    /// dashboard sees non-absent fields it can chart. Guards against the field
    /// silently dropping out of the serialized event.
    #[test]
    fn telemetry_value_contains_all_axes() {
        let value = sample().to_telemetry_value();
        let obj = value
            .as_object()
            .expect("sample serializes to a JSON object");

        for key in [
            "memory_rss_bytes",
            "memory_limit_bytes",
            "cpu_time_seconds",
            "cumulative_bytes_sent",
            "cumulative_bytes_received",
        ] {
            assert!(obj.contains_key(key), "telemetry value missing key `{key}`");
        }

        // On Linux the memory/CPU axes must serialize to concrete numbers, not
        // JSON null — i.e. the sample actually resolved them.
        #[cfg(target_os = "linux")]
        {
            assert!(
                obj["memory_rss_bytes"].is_number(),
                "memory_rss_bytes should be a number on Linux, got {}",
                obj["memory_rss_bytes"]
            );
            assert!(
                obj["memory_limit_bytes"].is_number(),
                "memory_limit_bytes should be a number on Linux, got {}",
                obj["memory_limit_bytes"]
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_proc_stat_cpu_ticks_handles_parens_in_comm() {
        // comm = "(freenet)"; utime = field 14 = 100, stime = field 15 = 55.
        let line = "1234 (freenet) S 1 1234 1234 0 -1 4194304 100 0 0 0 \
                    100 55 0 0 20 0 8 0 999 12345 678 rest ignored";
        assert_eq!(parse_proc_stat_cpu_ticks(line), Some(155));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_proc_stat_cpu_ticks_handles_spaces_and_parens_in_comm() {
        // Pathological comm containing a space AND a close-paren; the parser
        // must anchor on the LAST ')', not the first.
        let line = "42 (weird )name) R 1 42 42 0 -1 0 7 3 0 0 \
                    12 8 0 0 20 0 1 0 100 200 300 tail";
        assert_eq!(parse_proc_stat_cpu_ticks(line), Some(20));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_proc_stat_cpu_ticks_rejects_garbage() {
        assert_eq!(parse_proc_stat_cpu_ticks("not a stat line"), None);
        assert_eq!(parse_proc_stat_cpu_ticks(""), None);
    }
}
