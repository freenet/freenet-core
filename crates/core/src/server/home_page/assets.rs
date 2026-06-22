pub const CSS: &str = include_str!("assets/style.css");

/// Inline JavaScript for the dark/light mode toggle.
/// Dark mode is the default (matching the global telemetry dashboard).
/// A plain (non-module) script so it runs synchronously before first paint
/// and so toggleTheme() is reachable from the button's onclick attribute.
/// The page auto-refreshes every 5 s, so restoring the saved theme before
/// render is essential to avoid a flash of the wrong theme on each refresh.
pub const JS: &str = include_str!("assets/dashboard.js");

pub const PEER_CSS: &str = include_str!("assets/peer.css");
