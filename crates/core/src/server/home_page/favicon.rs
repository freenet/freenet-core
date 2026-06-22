//! Favicon builder for the local-peer dashboard.
//!
//! `RABBIT_SVG_PATH` lives in the parent module (`home_page.rs`) so that
//! `path_handlers.rs` can reach it without a re-export dance.

use super::*;

/// Build a `data:` URI for an SVG favicon colored by connection status.
///
/// Colors follow issue #3287 (match order = priority):
/// 1. Grey: starting up (no snapshot yet)
/// 2. Blue: connected (any open connections — healthy state wins)
/// 3. Dark red: NAT traversal failing (all attempts failed)
/// 4. Red: connection failures present
/// 5. Amber: attempting to connect (fallback)
pub fn build_favicon_data_uri(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
    // Color is pre-encoded for data URI (# → %23) to avoid scanning the entire SVG.
    let color = match snap {
        None => "%239e9e9e",                              // grey — starting up
        Some(s) if s.open_connections > 0 => "%230abab5", // teal — connected
        Some(s) if s.nat_stats.attempts > 0 && s.nat_stats.successes == 0 => "%238b0000", // dark red — NAT problems
        Some(s) if !s.failures.is_empty() => "%23f44336", // red — connection issues
        Some(_) => "%23fbbf24",                           // amber — connecting
    };

    format!(
        "data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 640 471'>\
         <path d='{path}' fill='{color}' fill-rule='evenodd'/></svg>",
        path = RABBIT_SVG_PATH,
        color = color,
    )
}
