//! Homepage served at `/` when a user navigates to their local Freenet node.
//!
//! Renders a card-based dashboard showing connection status, peers, subscriptions,
//! and operation stats. Styled to match the global telemetry dashboard.

use std::fmt::Write;

use axum::response::{Html, IntoResponse};

use crate::node::network_status::{self, format_ago, format_duration, html_escape};

/// Handler for `GET /` — returns a self-contained HTML dashboard.
pub(super) async fn homepage() -> impl IntoResponse {
    Html(homepage_html())
}

fn homepage_html() -> String {
    let snap = network_status::get_snapshot();

    let (version, uptime) = match &snap {
        Some(s) => (s.version.as_str(), format_duration(s.elapsed_secs)),
        None => ("?", "0s".to_string()),
    };

    let favicon = build_favicon_data_uri(&snap);

    let status_card = build_status_card(&snap);
    let peers_card = build_peers_card(&snap);
    let contracts_card = build_contracts_card(&snap);
    let ops_card = build_ops_card(&snap);

    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="5">
    <title>Freenet — Local Peer</title>
    <link rel="icon" type="image/svg+xml" href="{favicon}">
    <style>{CSS}</style>
    <script>{JS}</script>
</head>
<body>
    <header>
        <div class="header-left">
            <img src="https://freenet.org/freenet_logo.svg" alt="Freenet" class="logo">
            <span class="header-title">FREENET</span>
            <span class="header-scope">Local Peer</span>
            <span class="badge">v{version}</span>
        </div>
        <div class="header-right">
            <span class="uptime">Up {uptime}</span>
            <button class="theme-btn" id="theme-btn" onclick="toggleTheme()" title="Toggle dark/light mode">
                <span id="theme-icon">☀️</span>
            </button>
        </div>
    </header>

    <main>
        {status_card}
        {peers_card}
        {contracts_card}
        {ops_card}

        <div class="card">
            <h2>Freenet Apps</h2>
            <ul class="app-list">
                <li>
                    <a href="/v1/contract/web/raAqMhMG7KUpXBU2SxgCQ3Vh4PYjttxdSWd9ftV7RLv/">River Chat</a>
                    <p class="note">You'll need an <a href="https://freenet.org/quickstart/" target="_blank" rel="noopener noreferrer">invite</a> to join the "Freenet Official" room.</p>
                </li>
            </ul>
        </div>

        <div class="card card-muted">
            <h2>Links</h2>
            <ul class="link-list">
                <li><a href="https://freenet.org" target="_blank" rel="noopener noreferrer">freenet.org</a></li>
                <li><a href="https://matrix.to/#/#freenet-locutus:matrix.org" target="_blank" rel="noopener noreferrer">Freenet Matrix channel</a></li>
                <li><a href="https://github.com/freenet/freenet-core" target="_blank" rel="noopener noreferrer">GitHub</a></li>
            </ul>
        </div>
    </main>
</body>
</html>"##,
        CSS = CSS,
        JS = JS,
        favicon = favicon,
        version = html_escape(version),
        uptime = uptime,
        status_card = status_card,
        peers_card = peers_card,
        contracts_card = contracts_card,
        ops_card = ops_card,
    )
}

/// Freenet rabbit silhouette SVG path, derived from freenet_logo.svg.
/// Used for the favicon with a solid color fill (no gradient) so the
/// connection status color is immediately visible at favicon size.
const RABBIT_SVG_PATH: &str = concat!(
    "M358.864 40.470C358.605 40.728 354.143 42.467 348.947 44.334",
    "C284.621 67.446 232.573 113.729 201.443 175.500",
    "C193.895 190.478 184.375 213.708 185.375 214.708",
    "C185.621 214.954 187.715 211.857 190.030 207.827",
    "C211.190 170.984 229.863 146.093 255.968 119.933",
    "C274.854 101.008 282.998 94.207 302.034 81.466",
    "C334.671 59.621 367.531 47.376 401.250 44.492",
    "L409.000 43.829 408.984 47.165",
    "C408.958 52.704 405.255 68.515 401.010 81.213",
    "C382.392 136.898 338.799 184.709 277.000 217.224",
    "C271.225 220.263 263.913 223.788 260.750 225.058",
    "C254.629 227.517 254.126 228.307 256.511 231.712",
    "C258.282 234.241 258.484 234.089 249.500 237.002",
    "C226.868 244.341 200.420 256.771 183.918 267.825",
    "C173.918 274.522 156.961 289.225 158.000 290.296",
    "C158.275 290.579 163.450 287.694 169.500 283.883",
    "C175.550 280.073 186.125 274.083 193.000 270.573",
    "C264.905 233.856 345.414 226.155 422.387 248.633",
    "C434.634 252.210 468.194 264.823 465.830 264.961",
    "C465.461 264.982 459.741 263.440 453.118 261.534",
    "C422.666 252.769 376.068 246.967 347.500 248.384",
    "C320.590 249.719 284.052 255.527 283.798 258.510",
    "C283.694 259.727 291.796 264.541 298.477 267.231",
    "C306.163 270.326 319.360 270.612 338.812 268.103",
    "C385.602 262.070 433.627 269.250 469.963 287.712",
    "C475.721 290.638 480.874 293.474 481.416 294.016",
    "C482.061 294.661 476.225 295.004 464.450 295.010",
    "C407.520 295.043 349.853 308.084 300.500 332.086",
    "C290.412 336.992 272.833 346.834 271.147 348.519",
    "C269.278 350.389 273.301 349.263 283.966 344.933",
    "C302.548 337.389 332.479 327.629 351.000 323.074",
    "C386.266 314.400 413.893 311.393 450.000 312.298",
    "C474.559 312.914 491.602 315.535 509.306 321.421",
    "C520.784 325.236 519.954 325.640 504.924 323.552",
    "C419.615 311.701 330.506 332.225 238.000 385.033",
    "C224.991 392.460 221.855 394.386 200.762 407.913",
    "C184.591 418.282 178.817 420.978 172.750 420.990",
    "C167.060 421.002 164.441 418.869 163.413 413.387",
    "C160.912 400.055 178.394 366.762 202.136 339.644",
    "L206.387 334.788 197.443 335.644",
    "C183.073 337.019 158.519 336.519 150.152 334.681",
    "C132.833 330.876 120.785 321.947 117.439 310.437",
    "C112.326 292.850 123.492 270.717 146.912 252.015",
    "C154.528 245.934 155.702 244.562 156.798 240.464",
    "C158.983 232.296 168.599 206.900 174.208 194.482",
    "C184.044 172.710 197.989 150.083 213.332 131.000",
    "C229.597 110.770 255.612 87.415 277.277 73.590",
    "C286.990 67.393 310.855 55.436 323.062 50.653",
    "C335.623 45.730 360.750 38.583 358.864 40.470",
    "M375.000 209.596",
    "C430.712 216.655 477.541 237.609 509.241 269.661",
    "C514.049 274.523 518.765 279.625 519.721 281.000",
    "C521.404 283.419 521.347 283.408 517.980 280.669",
    "C500.484 266.434 486.556 257.516 468.000 248.668",
    "C452.323 241.193 438.766 236.261 424.000 232.663",
    "C395.297 225.667 374.955 223.024 349.705 223.010",
    "C333.212 223.000 332.865 222.956 330.455 220.545",
    "C329.105 219.195 328.000 217.046 328.000 215.768",
    "C328.000 212.845 330.558 209.125 333.357 207.978",
    "C336.189 206.817 360.536 207.763 375.000 209.596",
);

/// Build a `data:` URI for an SVG favicon colored by connection status.
///
/// Colors follow issue #3287 (match order = priority):
/// 1. Grey: starting up (no snapshot yet)
/// 2. Blue: connected (any open connections — healthy state wins)
/// 3. Dark red: NAT traversal failing (all attempts failed)
/// 4. Red: connection failures present
/// 5. Amber: attempting to connect (fallback)
fn build_favicon_data_uri(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
    // Color is pre-encoded for data URI (# → %23) to avoid scanning the entire SVG.
    let color = match snap {
        None => "%239e9e9e",                              // grey — starting up
        Some(s) if s.open_connections > 0 => "%23007FFF", // blue — connected
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

fn build_status_card(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
    let Some(snap) = snap else {
        return r#"<div class="card">
            <h2>Connection Status</h2>
            <div class="status-row"><span class="dot dot-yellow"></span> Starting up...</div>
            <div class="spinner"></div>
        </div>"#
            .to_string();
    };

    let (dot_class, status_text) = if snap.open_connections > 0 {
        let n = snap.open_connections;
        let label = if n == 1 { "peer" } else { "peers" };
        ("dot-green", format!("Connected ({n} {label})"))
    } else {
        ("dot-yellow", "Connecting...".to_string())
    };

    let spinner = if snap.open_connections == 0 {
        r#"<div class="spinner"></div>"#
    } else {
        ""
    };

    // Gateway-only warning
    let gateway_warning = if snap.gateway_only {
        format!(
            r#"<div class="warning">
                <strong>Only connected to gateway servers</strong> — no peer-to-peer connections.
                This usually means a firewall is blocking incoming UDP connections on port <code>{port}</code>.
                <ul>
                    <li>Configure your router to forward UDP port <code>{port}</code> to this computer.</li>
                    <li>Check that no software firewall (ufw, iptables, Windows Defender) is blocking Freenet.</li>
                    <li>We plan to improve support for restrictive firewalls in a future release.</li>
                </ul>
            </div>"#,
            port = snap.listening_port
        )
    } else {
        String::new()
    };

    // NAT stats (suppress the detailed advice when the gateway warning already covers it)
    let nat_html = if snap.nat_stats.attempts > 0 {
        let all_failed = snap.nat_stats.successes == 0;
        let class = if all_failed { " nat-fail" } else { "" };
        // Only show the "try forwarding" advice if the gateway_only warning isn't already visible
        let extra = if all_failed && !snap.gateway_only {
            format!(
                r#"<p class="nat-advice">All NAT traversal attempts have failed. Your firewall or router is likely blocking UDP connections. Try forwarding UDP port <code>{}</code> on your router.</p>"#,
                snap.listening_port
            )
        } else {
            String::new()
        };
        format!(
            r#"<p class="nat-stat{class}">NAT hole punching: {s}/{a} successful</p>{extra}"#,
            class = class,
            s = snap.nat_stats.successes,
            a = snap.nat_stats.attempts,
            extra = extra,
        )
    } else if snap.open_connections == 0 {
        r#"<p class="nat-stat">No NAT traversal attempts yet</p>"#.to_string()
    } else {
        String::new()
    };

    // Failure diagnostics
    let failures_html = if !snap.failures.is_empty() {
        let mut items = String::new();
        for f in &snap.failures {
            items.push_str(&format!(
                "<li><code>{}</code>: {}</li>",
                f.address, f.reason_html
            ));
        }
        format!(
            r#"<div class="diagnostics">
                <h3>Connection Issues</h3>
                <ul>{items}</ul>
                <p class="attempts">Attempted {attempts} connection(s) over {elapsed}. Retrying...</p>
            </div>"#,
            items = items,
            attempts = snap.connection_attempts,
            elapsed = format_duration(snap.elapsed_secs),
        )
    } else if snap.open_connections == 0 && snap.connection_attempts > 0 {
        format!(
            r#"<p class="attempts">Attempted {} connection(s) over {}. Retrying...</p>"#,
            snap.connection_attempts,
            format_duration(snap.elapsed_secs),
        )
    } else {
        String::new()
    };

    format!(
        r#"<div class="card">
            <h2>Connection Status</h2>
            <div class="status-row"><span class="dot {dot_class}"></span> {status_text}</div>
            {spinner}
            {gateway_warning}
            {nat_html}
            {failures_html}
        </div>"#,
        dot_class = dot_class,
        status_text = status_text,
        spinner = spinner,
        gateway_warning = gateway_warning,
        nat_html = nat_html,
        failures_html = failures_html,
    )
}

fn build_peers_card(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
    let Some(snap) = snap else {
        return String::new();
    };
    if snap.peers.is_empty() && snap.open_connections == 0 {
        return String::new();
    }

    let own_loc = snap
        .own_location
        .map(|l| format!(r#"<span class="own-loc">Your location: {:.4}</span>"#, l))
        .unwrap_or_default();

    if snap.peers.is_empty() {
        return format!(
            r#"<div class="card">
                <div class="card-header"><h2>Network Peers</h2>{own_loc}</div>
                <p class="empty">No peers connected</p>
            </div>"#,
            own_loc = own_loc,
        );
    }

    let ring_svg = build_ring_svg(snap.own_location, &snap.peers);

    let mut rows = String::new();
    for p in &snap.peers {
        let peer_type = if p.is_gateway { "Gateway" } else { "Peer" };
        let loc = p
            .location
            .map(|l| format!("{:.4}", l))
            .unwrap_or_else(|| "—".to_string());
        rows.push_str(&format!(
            r#"<tr><td><code>{addr}</code></td><td>{loc}</td><td>{ptype}</td><td>{connected}</td></tr>"#,
            addr = p.address,
            loc = loc,
            ptype = peer_type,
            connected = format_duration(p.connected_secs),
        ));
    }

    format!(
        r#"<div class="card">
            <div class="card-header"><h2>Network Peers</h2>{own_loc}</div>
            {ring_svg}
            <div class="table-wrap">
                <table>
                    <thead><tr><th>Address</th><th>Location</th><th>Type</th><th>Connected</th></tr></thead>
                    <tbody>{rows}</tbody>
                </table>
            </div>
        </div>"#,
        own_loc = own_loc,
        ring_svg = ring_svg,
        rows = rows,
    )
}

/// Build an SVG ring visualization showing this node and connected peers.
/// Locations are 0.0–1.0, mapped to angles around a circle.
fn build_ring_svg(own_location: Option<f64>, peers: &[network_status::PeerSnapshot]) -> String {
    // Only render if we have location data for at least one peer
    let has_any_location = own_location.is_some() || peers.iter().any(|p| p.location.is_some());
    if !has_any_location {
        return String::new();
    }

    let cx: f64 = 120.0;
    let cy: f64 = 120.0;
    let r: f64 = 95.0;
    let size = 240;

    let mut svg = format!(
        "<div class=\"ring-wrap\"><svg viewBox=\"0 0 {size} {size}\" width=\"{size}\" height=\"{size}\" class=\"ring-svg\">"
    );

    // Ring circle
    write!(
        svg,
        "<circle cx=\"{cx}\" cy=\"{cy}\" r=\"{r}\" fill=\"none\" stroke=\"#e0e0e0\" stroke-width=\"2\"/>"
    )
    .ok();

    // Helper: location (0.0-1.0) to (x, y) on the ring.
    // 0.0 is at the top, increasing clockwise.
    let loc_to_xy = |loc: f64| -> (f64, f64) {
        let angle = loc * std::f64::consts::TAU - std::f64::consts::FRAC_PI_2;
        (cx + r * angle.cos(), cy + r * angle.sin())
    };

    // Draw connection lines from own location to each peer
    if let Some(own_loc) = own_location {
        let (ox, oy) = loc_to_xy(own_loc);
        for p in peers {
            if let Some(ploc) = p.location {
                let (px, py) = loc_to_xy(ploc);
                let stroke = if p.is_gateway { "#fbbf24" } else { "#007FFF" };
                write!(
                    svg,
                    "<line x1=\"{ox:.1}\" y1=\"{oy:.1}\" x2=\"{px:.1}\" y2=\"{py:.1}\" stroke=\"{stroke}\" stroke-width=\"1\" opacity=\"0.4\"/>"
                )
                .ok();
            }
        }
    }

    // Peer dots
    for p in peers {
        if let Some(loc) = p.location {
            let (px, py) = loc_to_xy(loc);
            let fill = if p.is_gateway { "#fbbf24" } else { "#007FFF" };
            write!(
                svg,
                "<circle cx=\"{px:.1}\" cy=\"{py:.1}\" r=\"5\" fill=\"{fill}\"/>"
            )
            .ok();
        }
    }

    // Own location (drawn last so it's on top)
    if let Some(own_loc) = own_location {
        let (ox, oy) = loc_to_xy(own_loc);
        write!(
            svg,
            "<circle cx=\"{ox:.1}\" cy=\"{oy:.1}\" r=\"7\" fill=\"#34d399\" stroke=\"#fff\" stroke-width=\"2\"/>"
        )
        .ok();
    }

    svg.push_str("</svg>");

    // Legend
    svg.push_str(concat!(
        "<div class=\"ring-legend\">",
        "<span class=\"ring-key\"><span class=\"ring-dot ring-dot-self\"></span> You</span>",
        "<span class=\"ring-key\"><span class=\"ring-dot ring-dot-peer\"></span> Peer</span>",
        "<span class=\"ring-key\"><span class=\"ring-dot ring-dot-gw\"></span> Gateway</span>",
        "</div></div>",
    ));

    svg
}

fn build_contracts_card(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
    let Some(snap) = snap else {
        return String::new();
    };
    if snap.contracts.is_empty() {
        if snap.open_connections > 0 {
            return r#"<div class="card">
                <h2>Subscribed Contracts</h2>
                <p class="empty">No active subscriptions</p>
            </div>"#
                .to_string();
        }
        return String::new();
    }

    let mut rows = String::new();
    for c in &snap.contracts {
        let last_update = c
            .last_updated_secs
            .map(format_ago)
            .unwrap_or_else(|| "—".to_string());
        rows.push_str(&format!(
            r#"<tr><td title="{full}"><code>{short}</code></td><td>{subscribed}</td><td>{last_update}</td></tr>"#,
            full = html_escape(&c.key_full),
            short = html_escape(&c.key_short),
            subscribed = format_ago(c.subscribed_secs),
            last_update = last_update,
        ));
    }

    format!(
        r#"<div class="card">
            <h2>Subscribed Contracts</h2>
            <div class="table-wrap">
                <table>
                    <thead><tr><th>Contract</th><th>Subscribed</th><th>Last Update</th></tr></thead>
                    <tbody>{rows}</tbody>
                </table>
            </div>
        </div>"#,
        rows = rows,
    )
}

fn build_ops_card(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
    let Some(snap) = snap else {
        return String::new();
    };
    let ops = &snap.op_stats;
    if ops.total() == 0 && snap.open_connections == 0 {
        return String::new();
    }

    fn op_cell(name: &str, ok: u32, fail: u32) -> String {
        format!(
            r#"<div class="op-cell">
                <div class="op-name">{name}</div>
                <div><span class="op-ok">{ok}</span> <span class="op-fail">{fail}</span></div>
            </div>"#,
            name = name,
            ok = ok,
            fail = fail,
        )
    }

    format!(
        r#"<div class="card">
            <h2>Operations</h2>
            <div class="op-grid">
                {get}{put}{update}{subscribe}
            </div>
        </div>"#,
        get = op_cell("GET", ops.gets.0, ops.gets.1),
        put = op_cell("PUT", ops.puts.0, ops.puts.1),
        update = op_cell("UPDATE", ops.updates.0, ops.updates.1),
        subscribe = op_cell("SUBSCRIBE", ops.subscribes.0, ops.subscribes.1),
    )
}

const CSS: &str = r##"
/* ── CSS Variables (dark mode default, matching global telemetry dashboard) ── */
:root {
    --bg-primary: #06080c;
    --bg-secondary: #0d1117;
    --bg-tertiary: #161b22;
    --bg-panel: rgba(13, 17, 23, 0.8);
    --border-color: rgba(48, 54, 61, 0.6);
    --text-primary: #e6edf3;
    --text-secondary: #8b949e;
    --text-muted: #484f58;
    --accent-primary: #007FFF;
    --accent-light: #7ecfef;
    --accent-dark: #0052cc;
    --font-mono: 'SF Mono', Monaco, 'Cascadia Code', monospace;
    --font-sans: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
}
/* Light mode overrides */
[data-theme="light"] {
    --bg-primary: #f0f4f8;
    --bg-secondary: #e2e8f0;
    --bg-tertiary: #cbd5e1;
    --bg-panel: rgba(255, 255, 255, 0.85);
    --border-color: rgba(148, 163, 184, 0.5);
    --text-primary: #0f172a;
    --text-secondary: #475569;
    --text-muted: #94a3b8;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: var(--font-sans);
    background: var(--bg-primary);
    color: var(--text-primary);
    line-height: 1.5;
}
/* Atmospheric background (matching telemetry dashboard) */
body::before {
    content: '';
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background:
        radial-gradient(ellipse at 50% 0%, rgba(0, 127, 255, 0.08) 0%, transparent 50%),
        radial-gradient(ellipse at 80% 50%, rgba(126, 207, 239, 0.05) 0%, transparent 40%),
        radial-gradient(ellipse at 20% 80%, rgba(0, 82, 204, 0.05) 0%, transparent 40%);
    pointer-events: none;
    z-index: 0;
}
[data-theme="light"] body::before {
    background:
        radial-gradient(ellipse at 50% 0%, rgba(0, 127, 255, 0.06) 0%, transparent 50%),
        radial-gradient(ellipse at 80% 50%, rgba(126, 207, 239, 0.04) 0%, transparent 40%),
        radial-gradient(ellipse at 20% 80%, rgba(0, 82, 204, 0.04) 0%, transparent 40%);
}
header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.75rem 1.5rem;
    background: var(--bg-panel);
    border-bottom: 1px solid var(--border-color);
    backdrop-filter: blur(10px);
    position: relative;
    z-index: 1;
}
.header-left {
    display: flex;
    align-items: center;
    gap: 0.5rem;
}
.header-right {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    color: var(--text-secondary);
    font-size: 0.9rem;
}
.logo { width: 28px; height: 28px; }
.header-title {
    font-family: var(--font-mono);
    font-weight: 600;
    font-size: 1.1rem;
    letter-spacing: -0.02em;
    background: linear-gradient(135deg, var(--accent-light) 0%, var(--accent-primary) 50%, var(--accent-dark) 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}
.header-scope {
    font-family: var(--font-mono);
    font-size: 0.75rem;
    font-weight: 500;
    color: var(--accent-light);
    background: rgba(126, 207, 239, 0.1);
    padding: 0.15rem 0.5rem;
    border-radius: 4px;
    border: 1px solid rgba(126, 207, 239, 0.2);
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
.badge {
    background: rgba(0, 127, 255, 0.15);
    color: var(--accent-light);
    padding: 0.15rem 0.5rem;
    border-radius: 12px;
    font-size: 0.75rem;
    font-weight: 500;
    font-family: var(--font-mono);
}
.uptime { font-family: var(--font-mono); font-size: 0.85rem; }
main {
    max-width: 800px;
    margin: 1.5rem auto;
    padding: 0 1rem;
    display: flex;
    flex-direction: column;
    gap: 1rem;
    position: relative;
    z-index: 1;
}
.card {
    background: var(--bg-panel);
    border: 1px solid var(--border-color);
    border-radius: 10px;
    padding: 1rem 1.25rem;
    backdrop-filter: blur(10px);
}
.card-muted { background: var(--bg-secondary); }
.card h2 {
    font-size: 0.85rem;
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 0.75rem;
    font-family: var(--font-mono);
    font-weight: 500;
}
.card-header {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    margin-bottom: 0.75rem;
}
.card-header h2 { margin-bottom: 0; }
.own-loc {
    font-size: 0.8rem;
    color: var(--text-secondary);
    font-family: var(--font-mono);
}
.status-row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 1rem;
    font-weight: 500;
    margin-bottom: 0.5rem;
}
.dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    display: inline-block;
    flex-shrink: 0;
}
.dot-green { background: #34d399; box-shadow: 0 0 8px rgba(52, 211, 153, 0.5); }
.dot-yellow { background: #fbbf24; box-shadow: 0 0 8px rgba(251, 191, 36, 0.5); }
.spinner {
    width: 20px;
    height: 20px;
    border: 2px solid var(--border-color);
    border-top-color: var(--accent-primary);
    border-radius: 50%;
    animation: spin 1s linear infinite;
    margin: 0.5rem 0;
}
@keyframes spin { to { transform: rotate(360deg); } }
.warning {
    background: rgba(251, 191, 36, 0.1);
    border: 1px solid rgba(251, 191, 36, 0.3);
    border-radius: 6px;
    padding: 0.75rem 1rem;
    margin: 0.75rem 0;
    font-size: 0.9rem;
    color: #fbbf24;
}
[data-theme="light"] .warning { color: #92400e; background: rgba(251, 191, 36, 0.12); border-color: rgba(251, 191, 36, 0.4); }
.warning ul {
    margin: 0.5rem 0 0 1.25rem;
    font-size: 0.85rem;
}
.warning li { margin-bottom: 0.25rem; }
.nat-stat {
    font-size: 0.85rem;
    color: var(--text-secondary);
    margin-top: 0.5rem;
}
.nat-fail { color: #f87171; font-weight: 500; }
[data-theme="light"] .nat-fail { color: #dc2626; }
.nat-advice {
    background: rgba(248, 113, 113, 0.1);
    border: 1px solid rgba(248, 113, 113, 0.3);
    border-radius: 6px;
    padding: 0.5rem 0.75rem;
    margin-top: 0.5rem;
    font-size: 0.85rem;
    color: #f87171;
}
[data-theme="light"] .nat-advice { color: #b91c1c; background: rgba(248, 113, 113, 0.08); }
.diagnostics {
    background: rgba(251, 191, 36, 0.1);
    border: 1px solid rgba(251, 191, 36, 0.3);
    border-radius: 6px;
    padding: 0.75rem 1rem;
    margin-top: 0.75rem;
}
.diagnostics h3 {
    color: #fbbf24;
    font-size: 0.9rem;
    margin-bottom: 0.4rem;
}
[data-theme="light"] .diagnostics h3 { color: #92400e; }
[data-theme="light"] .diagnostics { color: #78350f; background: rgba(251, 191, 36, 0.12); }
.diagnostics ul {
    padding-left: 1.2rem;
    margin: 0.4rem 0;
    list-style: disc;
}
.diagnostics li {
    color: var(--text-secondary);
    margin-bottom: 0.35rem;
    font-size: 0.85rem;
}
.attempts {
    color: var(--text-muted);
    font-size: 0.8rem;
    margin-top: 0.4rem;
}
.table-wrap { overflow-x: auto; }
table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.85rem;
    font-family: var(--font-mono);
}
thead th {
    text-align: left;
    padding: 0.4rem 0.6rem;
    border-bottom: 2px solid var(--border-color);
    color: var(--text-secondary);
    font-weight: 500;
    font-size: 0.8rem;
    text-transform: uppercase;
    letter-spacing: 0.03em;
}
tbody td {
    padding: 0.4rem 0.6rem;
    border-bottom: 1px solid var(--border-color);
}
code {
    background: var(--bg-tertiary);
    color: var(--text-primary);
    padding: 0.1rem 0.3rem;
    border-radius: 3px;
    font-size: 0.85em;
    font-family: var(--font-mono);
}
.empty {
    color: var(--text-muted);
    font-size: 0.9rem;
    font-style: italic;
}
.op-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 0.75rem;
}
.op-cell {
    text-align: center;
    padding: 0.5rem;
    background: var(--bg-secondary);
    border-radius: 6px;
    border: 1px solid var(--border-color);
}
.op-name {
    font-size: 0.75rem;
    color: var(--text-secondary);
    font-weight: 500;
    text-transform: uppercase;
    margin-bottom: 0.25rem;
    font-family: var(--font-mono);
}
.op-ok { color: #34d399; font-weight: 600; }
.op-ok::before { content: "\2713 "; }
.op-fail { color: #f87171; font-weight: 600; margin-left: 0.5rem; }
.op-fail::before { content: "\2717 "; }
[data-theme="light"] .op-ok { color: #059669; }
[data-theme="light"] .op-fail { color: #dc2626; }
.app-list, .link-list {
    list-style: none;
    padding: 0;
}
.app-list li, .link-list li { margin-bottom: 0.4rem; }
.app-list a, .link-list a {
    color: var(--accent-light);
    text-decoration: none;
}
.app-list a:hover, .link-list a:hover { text-decoration: underline; }
[data-theme="light"] .app-list a, [data-theme="light"] .link-list a { color: var(--accent-primary); }
.note {
    color: var(--text-secondary);
    font-size: 0.8rem;
    margin-top: 0.15rem;
}
.note a { color: var(--accent-light); }
[data-theme="light"] .note a { color: var(--accent-primary); }
p { margin-bottom: 0.5rem; }
p:last-child { margin-bottom: 0; }
.ring-wrap {
    display: flex;
    flex-direction: column;
    align-items: center;
    margin-bottom: 0.75rem;
}
.ring-svg { display: block; }
.ring-svg circle:first-child { stroke: var(--text-muted) !important; }
.ring-legend {
    display: flex;
    gap: 1rem;
    margin-top: 0.4rem;
    font-size: 0.75rem;
    color: var(--text-secondary);
}
.ring-key {
    display: flex;
    align-items: center;
    gap: 0.3rem;
}
.ring-dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    display: inline-block;
}
.ring-dot-self { background: #34d399; }
.ring-dot-peer { background: var(--accent-primary); }
.ring-dot-gw { background: #fbbf24; }
.theme-btn {
    background: none;
    border: 1px solid var(--border-color);
    border-radius: 6px;
    padding: 0.2rem 0.5rem;
    cursor: pointer;
    font-size: 1rem;
    line-height: 1;
    color: inherit;
    transition: border-color 0.15s;
}
.theme-btn:hover { border-color: var(--text-secondary); }
@media (max-width: 600px) {
    .op-grid { grid-template-columns: repeat(2, 1fr); }
    header { padding: 0.5rem 1rem; }
    main { margin: 1rem auto; }
    .ring-svg { width: 180px; height: 180px; }
}
"##;

/// Inline JavaScript for the dark/light mode toggle.
/// Dark mode is the default (matching the global telemetry dashboard).
/// A plain (non-module) script so it runs synchronously before first paint
/// and so toggleTheme() is reachable from the button's onclick attribute.
/// The page auto-refreshes every 5 s, so restoring the saved theme before
/// render is essential to avoid a flash of the wrong theme on each refresh.
const JS: &str = r##"
(function() {
    try {
        if (localStorage.getItem('theme') === 'light') {
            document.documentElement.setAttribute('data-theme', 'light');
        }
    } catch (e) { /* localStorage unavailable — default to dark */ }
})();

function toggleTheme() {
    var isLight = document.documentElement.getAttribute('data-theme') === 'light';
    var icon = document.getElementById('theme-icon');
    if (isLight) {
        document.documentElement.removeAttribute('data-theme');
        if (icon) icon.textContent = '\u2600\uFE0F'; /* sun = click to switch to light */
        try { localStorage.removeItem('theme'); } catch (e) {}
    } else {
        document.documentElement.setAttribute('data-theme', 'light');
        if (icon) icon.textContent = '\uD83C\uDF19'; /* moon = click to switch to dark */
        try { localStorage.setItem('theme', 'light'); } catch (e) {}
    }
}

document.addEventListener('DOMContentLoaded', function() {
    var icon = document.getElementById('theme-icon');
    if (icon && document.documentElement.getAttribute('data-theme') === 'light') {
        icon.textContent = '\uD83C\uDF19'; /* moon = click to switch to dark */
    }
});
"##;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::network_status::{
        FailureSnapshot, NatStatsSnapshot, NetworkStatusSnapshot, OpStatsSnapshot,
    };
    use std::net::SocketAddr;

    fn base_snapshot() -> NetworkStatusSnapshot {
        NetworkStatusSnapshot {
            failures: Vec::new(),
            connection_attempts: 0,
            open_connections: 0,
            elapsed_secs: 10,
            listening_port: 31337,
            version: "0.1.0".to_string(),
            own_location: None,
            peers: Vec::new(),
            contracts: Vec::new(),
            op_stats: OpStatsSnapshot::default(),
            nat_stats: NatStatsSnapshot::default(),
            gateway_only: false,
        }
    }

    #[test]
    fn favicon_grey_when_no_snapshot() {
        let uri = build_favicon_data_uri(&None);
        assert!(uri.starts_with("data:image/svg+xml,"));
        assert!(uri.contains("%239e9e9e"), "expected grey color");
    }

    #[test]
    fn favicon_blue_when_connected() {
        let mut snap = base_snapshot();
        snap.open_connections = 3;
        let uri = build_favicon_data_uri(&Some(snap));
        assert!(uri.contains("%23007FFF"), "expected blue color");
    }

    #[test]
    fn favicon_dark_red_when_nat_fails() {
        let mut snap = base_snapshot();
        snap.nat_stats.attempts = 5;
        snap.nat_stats.successes = 0;
        let uri = build_favicon_data_uri(&Some(snap));
        assert!(uri.contains("%238b0000"), "expected dark red color");
    }

    #[test]
    fn favicon_red_when_failures_present() {
        let mut snap = base_snapshot();
        snap.failures.push(FailureSnapshot {
            address: "1.2.3.4:1234".parse::<SocketAddr>().unwrap(),
            reason_html: "timeout".to_string(),
        });
        let uri = build_favicon_data_uri(&Some(snap));
        assert!(uri.contains("%23f44336"), "expected red color");
    }

    #[test]
    fn favicon_amber_when_connecting() {
        let snap = base_snapshot();
        let uri = build_favicon_data_uri(&Some(snap));
        assert!(uri.contains("%23fbbf24"), "expected amber color");
    }

    #[test]
    fn favicon_blue_takes_priority_over_failures() {
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.nat_stats.attempts = 5;
        snap.nat_stats.successes = 0;
        snap.failures.push(FailureSnapshot {
            address: "1.2.3.4:1234".parse().unwrap(),
            reason_html: "timeout".to_string(),
        });
        let uri = build_favicon_data_uri(&Some(snap));
        assert!(
            uri.contains("%23007FFF"),
            "connected should override failure colors"
        );
    }

    #[test]
    fn favicon_amber_when_nat_partially_succeeds() {
        let mut snap = base_snapshot();
        snap.nat_stats.attempts = 10;
        snap.nat_stats.successes = 3;
        let uri = build_favicon_data_uri(&Some(snap));
        assert!(
            uri.contains("%23fbbf24"),
            "partial NAT success should show amber, not dark red"
        );
    }

    #[test]
    fn favicon_dark_red_over_red_when_both_present() {
        let mut snap = base_snapshot();
        snap.nat_stats.attempts = 5;
        snap.nat_stats.successes = 0;
        snap.failures.push(FailureSnapshot {
            address: "1.2.3.4:1234".parse().unwrap(),
            reason_html: "timeout".to_string(),
        });
        let uri = build_favicon_data_uri(&Some(snap));
        assert!(
            uri.contains("%238b0000"),
            "NAT failure should take priority over connection failures"
        );
    }

    #[test]
    fn favicon_embedded_in_homepage() {
        let html = homepage_html();
        assert!(
            html.contains(r#"rel="icon" type="image/svg+xml" href="data:image/svg+xml,"#),
            "homepage should contain favicon data URI"
        );
    }

    #[test]
    fn external_links_open_in_new_tab() {
        // Verify all external links in the template have target="_blank"
        let html = homepage_html();
        for line in html.lines() {
            if line.contains("href=\"https://") {
                assert!(
                    line.contains("target=\"_blank\""),
                    "external link missing target=\"_blank\": {line}"
                );
                assert!(
                    line.contains("rel=\"noopener noreferrer\""),
                    "external link missing rel=\"noopener noreferrer\": {line}"
                );
            }
        }
    }
}
