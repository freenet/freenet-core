//! Homepage served at `/` when a user navigates to their local Freenet node.
//!
//! Renders a card-based dashboard showing connection status, peers, subscriptions,
//! and operation stats. Styled to match the global telemetry dashboard.

use std::fmt::Write;

use axum::extract::Path;
use axum::response::{Html, IntoResponse};

use crate::node::network_status::{self, format_ago, format_duration, html_escape};

/// Handler for `GET /` — returns a self-contained HTML dashboard.
pub(super) async fn homepage() -> impl IntoResponse {
    Html(homepage_html())
}

/// Handler for `GET /peer/{address}` — returns a detail page for a single peer.
pub(super) async fn peer_detail(Path(address): Path<String>) -> impl IntoResponse {
    Html(peer_detail_html(&address))
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
    let transfer_card = build_transfer_card(&snap);

    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
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
        {transfer_card}
        {contracts_card}
        {ops_card}

        <div class="card">
            <h2>Freenet Apps</h2>
            <ul class="app-list">
                <li>
                    <a href="/v1/contract/web/raAqMhMG7KUpXBU2SxgCQ3Vh4PYjttxdSWd9ftV7RLv/">River Chat</a>
                    <p class="note">You'll need an <a href="https://freenet.org/quickstart#invite-form" target="_blank" rel="noopener noreferrer">invite</a> to join the "Freenet Official" room.</p>
                </li>
                <li>
                    <a href="/v1/contract/web/EqJ5YpEEV3XLqEvKWLQHFhGAac2qXzSUoE6k2zbdnXBr/">Delta</a>
                    <p class="note">A website builder for Freenet.</p>
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
        transfer_card = transfer_card,
        contracts_card = contracts_card,
        ops_card = ops_card,
    )
}

/// Freenet rabbit silhouette SVG path, derived from freenet_logo.svg.
/// Used for the favicon with a solid color fill (no gradient) so the
/// connection status color is immediately visible at favicon size.
pub(super) const RABBIT_SVG_PATH: &str = concat!(
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

    // Health banner — the primary "everything looks good" indicator
    let health_banner = match snap.health {
        network_status::HealthLevel::Healthy => {
            let n = snap.open_connections;
            let label = if n == 1 { "peer" } else { "peers" };
            format!(
                r#"<div class="health-banner health-good">
                    <span class="health-icon">&#x2714;</span>
                    <span>Node is healthy — connected to {n} {label}</span>
                </div>"#,
            )
        }
        network_status::HealthLevel::Degraded => {
            let detail = if snap.gateway_only {
                "Only connected to gateways — no peer-to-peer connections yet"
            } else {
                "Connected but NAT traversal is failing"
            };
            format!(
                r#"<div class="health-banner health-degraded">
                    <span class="health-icon">&#x26A0;</span>
                    <span>{detail}</span>
                </div>"#,
            )
        }
        network_status::HealthLevel::Connecting => r#"<div class="health-banner health-connecting">
                <span class="health-icon">&#x231B;</span>
                <span>Connecting to the network...</span>
            </div>"#
            .to_string(),
        network_status::HealthLevel::Trouble => {
            let has_version_mismatch = snap
                .failures
                .iter()
                .any(|f| f.reason_html.contains("Version mismatch"));
            let detail = if has_version_mismatch {
                "Version mismatch — update required"
            } else {
                "Unable to connect — check firewall and network settings"
            };
            format!(
                r#"<div class="health-banner health-trouble">
                    <span class="health-icon">&#x2716;</span>
                    <span>{detail}</span>
                </div>"#,
            )
        }
    };

    // External address info (shown once discovered via NAT traversal)
    let external_addr_html = if let Some(addr) = snap.external_address {
        format!(
            r#"<p class="external-addr">External address: <code>{ip}</code> &mdash; UDP port: <code>{port}</code></p>"#,
            ip = addr.ip(),
            port = addr.port(),
        )
    } else if snap.open_connections > 0 {
        r#"<p class="external-addr muted">External address: discovering...</p>"#.to_string()
    } else {
        String::new()
    };

    let spinner = if snap.open_connections == 0 {
        r#"<div class="spinner"></div>"#
    } else {
        ""
    };

    // Gateway-only warning (only when not connected to any peers)
    let gateway_warning = if snap.gateway_only {
        format!(
            r#"<div class="warning">
                <strong>Firewall likely blocking incoming connections</strong> on UDP port <code>{port}</code>.
                <ul>
                    <li>Configure your router to forward UDP port <code>{port}</code> to this computer.</li>
                    <li>Check that no software firewall (ufw, iptables, Windows Defender) is blocking Freenet.</li>
                </ul>
            </div>"#,
            port = snap.listening_port
        )
    } else {
        String::new()
    };

    // NAT stats with rolling trend
    let nat_html = if snap.nat_stats.attempts > 0 {
        let all_failed = snap.nat_stats.successes == 0;
        let class = if all_failed { " nat-fail" } else { "" };
        let extra = if all_failed && !snap.gateway_only {
            format!(
                r#"<p class="nat-advice">All NAT traversal attempts have failed. Try forwarding UDP port <code>{}</code> on your router.</p>"#,
                snap.listening_port
            )
        } else {
            String::new()
        };

        // Rolling trend: recent window stats; only show verdict when truly blocked
        let (recent, verdict) = if snap.nat_stats.recent_attempts > 0 {
            let rs = snap.nat_stats.recent_successes;
            let ra = snap.nat_stats.recent_attempts;
            let verdict = if rs == 0 && snap.nat_stats.successes == 0 {
                r#" <span class="nat-verdict nat-verdict-bad">Port may be blocked</span>"#
                    .to_string()
            } else {
                String::new()
            };
            (
                format!(r#" <span class="nat-recent">({rs}/{ra} recent)</span>"#),
                verdict,
            )
        } else {
            (String::new(), String::new())
        };

        format!(
            r#"<p class="nat-stat{class}">NAT hole punching: {s}/{a} successful{recent} {verdict}</p>{extra}"#,
            class = class,
            s = snap.nat_stats.successes,
            a = snap.nat_stats.attempts,
            recent = recent,
            verdict = verdict,
            extra = extra,
        )
    } else if snap.open_connections == 0 {
        r#"<p class="nat-stat">No NAT traversal attempts yet</p>"#.to_string()
    } else {
        String::new()
    };

    // Failure diagnostics — demoted when connected (muted style, collapsed)
    let failures_html = if !snap.failures.is_empty() {
        let mut items = String::new();
        for f in &snap.failures {
            items.push_str(&format!(
                "<li><code>{}</code>: {}</li>",
                f.address, f.reason_html
            ));
        }
        if snap.open_connections > 0 {
            // Demoted: muted style when node is otherwise connected
            format!(
                r#"<details class="diagnostics-muted">
                    <summary>{n} recent connection attempt(s) failed <span class="muted-hint">(normal)</span></summary>
                    <ul>{items}</ul>
                </details>"#,
                n = snap.failures.len(),
                items = items,
            )
        } else {
            // Prominent: when not connected, failures are actionable
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
        }
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
            {health_banner}
            {external_addr_html}
            {spinner}
            {gateway_warning}
            {nat_html}
            {failures_html}
        </div>"#,
        health_banner = health_banner,
        external_addr_html = external_addr_html,
        spinner = spinner,
        gateway_warning = gateway_warning,
        nat_html = nat_html,
        failures_html = failures_html,
    )
}

/// Format bytes as a human-readable string (e.g., "1.2 MB").
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn build_transfer_card(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
    let Some(snap) = snap else {
        return String::new();
    };
    if snap.bytes_uploaded == 0 && snap.bytes_downloaded == 0 && snap.open_connections == 0 {
        return String::new();
    }

    format!(
        r#"<div class="card">
            <h2>Data Transfer</h2>
            <div class="transfer-stat">
                <span class="transfer-label">Uploaded</span>
                <span class="transfer-value">{uploaded}</span>
            </div>
            <div class="transfer-stat">
                <span class="transfer-label">Downloaded</span>
                <span class="transfer-value">{downloaded}</span>
            </div>
        </div>"#,
        uploaded = format_bytes(snap.bytes_uploaded),
        downloaded = format_bytes(snap.bytes_downloaded),
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
        let sent = if p.bytes_sent > 0 {
            format_bytes(p.bytes_sent)
        } else {
            "—".to_string()
        };
        let recv = if p.bytes_received > 0 {
            format_bytes(p.bytes_received)
        } else {
            "—".to_string()
        };
        rows.push_str(&format!(
            r#"<tr class="peer-row" onclick="window.location='/peer/{addr_enc}'"><td><code>{addr}</code></td><td>{loc}</td><td>{ptype}</td><td>{sent}</td><td>{recv}</td><td>{connected}</td></tr>"#,
            addr_enc = html_escape(&p.address.to_string()),
            addr = p.address,
            loc = loc,
            ptype = peer_type,
            sent = sent,
            recv = recv,
            connected = format_duration(p.connected_secs),
        ));
    }

    format!(
        r#"<div class="card">
            <div class="card-header"><h2>Network Peers</h2>{own_loc}</div>
            {ring_svg}
            <div class="table-wrap">
                <table>
                    <thead><tr><th>Address</th><th>Location</th><th>Type</th><th>Sent</th><th>Recv</th><th>Connected</th></tr></thead>
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

    // UPDATE cell: show received broadcast count (single number) since
    // subscription-streamed updates are push-based and don't have success/failure.
    // If there are also routed updates (with success/fail), show both.
    let update_cell = {
        let routed = ops.updates.0 + ops.updates.1;
        let received = ops.updates_received;
        if routed > 0 {
            // Both routed and received
            format!(
                r#"<div class="op-cell">
                    <div class="op-name">UPDATE</div>
                    <div><span class="op-ok">{ok}</span> <span class="op-fail">{fail}</span></div>
                    <div class="op-received">{recv} received</div>
                </div>"#,
                ok = ops.updates.0,
                fail = ops.updates.1,
                recv = received,
            )
        } else {
            // Only received (common for subscriber nodes)
            format!(
                r#"<div class="op-cell">
                    <div class="op-name">UPDATE</div>
                    <div class="op-count">{recv}</div>
                </div>"#,
                recv = received,
            )
        }
    };

    format!(
        r#"<div class="card">
            <h2>Operations</h2>
            <div class="op-grid">
                {get}{put}{update}{subscribe}
            </div>
        </div>"#,
        get = op_cell("GET", ops.gets.0, ops.gets.1),
        put = op_cell("PUT", ops.puts.0, ops.puts.1),
        update = update_cell,
        subscribe = {
            // Show active subscription count as primary metric since the cumulative
            // operation count includes periodic lease renewals (every 2 min per contract)
            // which inflates the number and confuses users.
            let active = snap.contracts.len() as u32;
            let total_ops = ops.subscribes.0 + ops.subscribes.1;
            if total_ops > 0 {
                format!(
                    r#"<div class="op-cell">
                        <div class="op-name">SUBSCRIBE</div>
                        <div class="op-count">{active} active</div>
                        <div class="op-received">{total_ops} ops</div>
                    </div>"#,
                )
            } else {
                format!(
                    r#"<div class="op-cell">
                        <div class="op-name">SUBSCRIBE</div>
                        <div class="op-count">{active} active</div>
                    </div>"#,
                )
            }
        },
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
.external-addr {
    font-size: 0.85rem;
    color: var(--text-secondary);
    margin-top: 0.5rem;
}
.external-addr.muted { opacity: 0.6; font-style: italic; }
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
.nat-recent { color: var(--text-muted); font-size: 0.8rem; }
.nat-verdict {
    display: inline-block;
    font-size: 0.75rem;
    font-weight: 600;
    padding: 0.1rem 0.4rem;
    border-radius: 4px;
    margin-left: 0.3rem;
    vertical-align: middle;
}
.nat-verdict-good { background: rgba(52, 211, 153, 0.15); color: #34d399; }
.nat-verdict-bad { background: rgba(248, 113, 113, 0.15); color: #f87171; }
.nat-verdict-warn { background: rgba(251, 191, 36, 0.15); color: #fbbf24; }
[data-theme="light"] .nat-verdict-good { color: #059669; background: rgba(5, 150, 105, 0.1); }
[data-theme="light"] .nat-verdict-bad { color: #dc2626; background: rgba(220, 38, 38, 0.1); }
[data-theme="light"] .nat-verdict-warn { color: #d97706; background: rgba(217, 119, 6, 0.1); }
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
.peer-row { cursor: pointer; transition: background 0.15s; }
.peer-row:hover { background: var(--bg-tertiary); }
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
.op-count { color: var(--text-primary); font-weight: 600; font-size: 1.1rem; }
.op-received { color: var(--text-secondary); font-size: 0.7rem; margin-top: 0.15rem; }
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
/* ── Health banner ── */
.health-banner {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.6rem 0.9rem;
    border-radius: 6px;
    font-weight: 500;
    font-size: 0.9rem;
    margin-bottom: 0.75rem;
}
.health-icon { font-size: 1.1rem; flex-shrink: 0; }
.health-good {
    background: rgba(52, 211, 153, 0.12);
    border: 1px solid rgba(52, 211, 153, 0.3);
    color: #34d399;
}
[data-theme="light"] .health-good { color: #059669; background: rgba(52, 211, 153, 0.1); }
.health-degraded {
    background: rgba(251, 191, 36, 0.1);
    border: 1px solid rgba(251, 191, 36, 0.3);
    color: #fbbf24;
}
[data-theme="light"] .health-degraded { color: #92400e; }
.health-connecting {
    background: rgba(0, 127, 255, 0.08);
    border: 1px solid rgba(0, 127, 255, 0.2);
    color: var(--accent-light);
}
[data-theme="light"] .health-connecting { color: var(--accent-dark); }
.health-trouble {
    background: rgba(248, 113, 113, 0.1);
    border: 1px solid rgba(248, 113, 113, 0.3);
    color: #f87171;
}
[data-theme="light"] .health-trouble { color: #b91c1c; }
/* ── Demoted diagnostics (when connected) ── */
.diagnostics-muted {
    margin-top: 0.5rem;
    font-size: 0.8rem;
    color: var(--text-muted);
}
.diagnostics-muted summary {
    cursor: pointer;
    font-family: var(--font-mono);
}
.diagnostics-muted summary:hover { color: var(--text-secondary); }
.diagnostics-muted ul {
    padding-left: 1.2rem;
    margin: 0.4rem 0;
    list-style: disc;
    color: var(--text-secondary);
    font-size: 0.8rem;
}
.diagnostics-muted li { margin-bottom: 0.25rem; }
.muted-hint { opacity: 0.6; font-style: italic; }
/* ── Transfer stats ── */
.transfer-stat {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.5rem;
    background: var(--bg-secondary);
    border-radius: 6px;
    border: 1px solid var(--border-color);
}
.transfer-label {
    font-size: 0.75rem;
    color: var(--text-secondary);
    font-weight: 500;
    text-transform: uppercase;
    font-family: var(--font-mono);
}
.transfer-value {
    font-size: 1.1rem;
    font-weight: 600;
    font-family: var(--font-mono);
    color: var(--text-primary);
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

/* Tab switching for per-operation-type charts */
function switchTab(el) {
    var tabId = el.getAttribute('data-tab');
    /* Deactivate all tabs and panels in this group */
    var group = el.closest('.tab-group');
    if (!group) return;
    group.querySelectorAll('.tab-label').forEach(function(t) { t.classList.remove('tab-active'); });
    group.querySelectorAll('.tab-panel').forEach(function(p) { p.classList.remove('tab-panel-active'); });
    /* Activate selected */
    el.classList.add('tab-active');
    var panel = group.querySelector('#panel-' + tabId);
    if (panel) panel.classList.add('tab-panel-active');
    /* Remember active tab for auto-refresh persistence */
    try { sessionStorage.setItem('activeOpTab', tabId); } catch(e) {}
}

document.addEventListener('DOMContentLoaded', function() {
    var icon = document.getElementById('theme-icon');
    if (icon && document.documentElement.getAttribute('data-theme') === 'light') {
        icon.textContent = '\uD83C\uDF19'; /* moon = click to switch to dark */
    }

    /* Restore active tab after page load / auto-refresh */
    function restoreTab() {
        try {
            var saved = sessionStorage.getItem('activeOpTab');
            if (saved) {
                var tab = document.querySelector('.tab-label[data-tab="' + saved + '"]');
                if (tab) switchTab(tab);
            }
        } catch(e) {}
    }
    restoreTab();

    /* Auto-refresh: fetch the page and swap dynamic content without a full reload.
       Uses setTimeout chaining (not setInterval) so slow responses don't overlap. */
    function scheduleRefresh() {
        setTimeout(function() {
            fetch(window.location.href).then(function(r) { return r.text(); }).then(function(html) {
                var parser = new DOMParser();
                var doc = parser.parseFromString(html, 'text/html');
                var newMain = doc.querySelector('main');
                var oldMain = document.querySelector('main');
                if (newMain && oldMain) oldMain.innerHTML = newMain.innerHTML;
                /* Update header elements (outside <main>) */
                var newUp = doc.querySelector('.uptime');
                var oldUp = document.querySelector('.uptime');
                if (newUp && oldUp) oldUp.textContent = newUp.textContent;
                var newBadge = doc.querySelector('.badge');
                var oldBadge = document.querySelector('.badge');
                if (newBadge && oldBadge) oldBadge.textContent = newBadge.textContent;
                var newIcon = doc.querySelector('link[rel="icon"]');
                var oldIcon = document.querySelector('link[rel="icon"]');
                if (newIcon && oldIcon) oldIcon.setAttribute('href', newIcon.getAttribute('href'));
                /* Restore tab selection after content swap */
                restoreTab();
            }).catch(function(e) { console.warn('Dashboard refresh failed:', e); })
              .finally(scheduleRefresh);
        }, 5000);
    }
    scheduleRefresh();
});
"##;

// ─── Peer detail page ────────────────────────────────────────────────────────

fn peer_detail_html(address_str: &str) -> String {
    let snap = network_status::get_snapshot();

    let peer = snap.as_ref().and_then(|s| {
        s.peers
            .iter()
            .find(|p| p.address.to_string() == address_str)
    });

    let Some(peer) = peer else {
        return format!(
            r##"<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Peer Not Found — Freenet</title>
<style>{CSS}{PEER_CSS}</style><script>{JS}</script>
</head><body>
<header>
    <div class="header-left">
        <a href="/" class="logo-link"><img src="https://freenet.org/freenet_logo.svg" alt="Freenet" class="logo"></a>
        <a href="/" class="header-title">FREENET</a>
        <span class="header-sep">/</span>
        <span class="header-scope">Peer</span>
    </div>
    <div class="header-right">
        <button class="theme-btn" id="theme-btn" onclick="toggleTheme()" title="Toggle dark/light mode">
            <span id="theme-icon">☀️</span>
        </button>
    </div>
</header>
<main>
    <div class="card"><h2>Peer Not Found</h2><p class="empty">No connected peer with address <code>{addr}</code>. The peer may have disconnected.</p>
    <p style="margin-top:0.75rem"><a href="/" style="color:var(--accent-light);font-family:var(--font-mono);font-size:0.85rem">&larr; Back to dashboard</a></p></div>
</main></body></html>"##,
            CSS = CSS,
            PEER_CSS = PEER_CSS,
            JS = JS,
            addr = html_escape(address_str),
        );
    };

    let peer_type = if peer.is_gateway { "Gateway" } else { "Peer" };
    let loc_str = peer
        .location
        .map(|l| format!("{:.6}", l))
        .unwrap_or_else(|| "—".to_string());

    // Try to get router data
    let router_lock = network_status::get_router();
    let router_guard = router_lock.as_ref().map(|r| r.read());

    let (router_snapshot, peer_routing) = match (&router_guard, &peer.peer_key_location) {
        (Some(router), Some(pkl)) => (Some(router.snapshot()), Some(router.peer_snapshot(pkl))),
        (Some(router), None) => (Some(router.snapshot()), None),
        _ => (None, None),
    };

    // Build info card
    let info_card = format!(
        r#"<div class="card">
            <h2>Peer Info</h2>
            <div class="info-grid">
                <div class="info-label">Address</div><div class="info-value"><code>{addr}</code></div>
                <div class="info-label">Location</div><div class="info-value">{loc}</div>
                <div class="info-label">Type</div><div class="info-value">{ptype}</div>
                <div class="info-label">Connected</div><div class="info-value">{connected}</div>
                <div class="info-label">Sent</div><div class="info-value">{sent}</div>
                <div class="info-label">Received</div><div class="info-value">{recv}</div>
            </div>
        </div>"#,
        addr = html_escape(&peer.address.to_string()),
        loc = loc_str,
        ptype = peer_type,
        connected = format_duration(peer.connected_secs),
        sent = format_bytes(peer.bytes_sent),
        recv = format_bytes(peer.bytes_received),
    );

    // Build routing model status card
    let model_card = if let Some(ref rs) = router_snapshot {
        let total_events = rs.failure_events + rs.success_events;
        let peer_failure_events = peer_routing
            .as_ref()
            .and_then(|pr| pr.failure_adjustment.map(|(_, c)| c))
            .unwrap_or(0);
        let peer_response_events = peer_routing
            .as_ref()
            .and_then(|pr| pr.response_time_adjustment.map(|(_, c)| c))
            .unwrap_or(0);
        let peer_transfer_events = peer_routing
            .as_ref()
            .and_then(|pr| pr.transfer_rate_adjustment.map(|(_, c)| c))
            .unwrap_or(0);
        format!(
            r#"<div class="card">
                <h2>Routing Model</h2>
                <div class="info-grid">
                    <div class="info-label">Prediction active</div><div class="info-value">{active}</div>
                    <div class="info-label">Global events</div><div class="info-value">{total}</div>
                    <div class="info-label">This peer: failure</div><div class="info-value">{pf} events</div>
                    <div class="info-label">This peer: response time</div><div class="info-value">{pr} events</div>
                    <div class="info-label">This peer: transfer rate</div><div class="info-value">{pt} events</div>
                </div>
                <h3 style="margin-top: 1em;">Renegade ML Predictor</h3>
                <div class="info-grid">
                    <div class="info-label">Failure observations</div><div class="info-value">{rf}</div>
                    <div class="info-label">Response time observations</div><div class="info-value">{rr}</div>
                    <div class="info-label">Transfer speed observations</div><div class="info-value">{rt}</div>
                    <div class="info-label">Known peers</div><div class="info-value">{rp}</div>
                    <div class="info-label">Predictions evaluated</div><div class="info-value">{n_eval}</div>
                    <div class="info-label">Brier score (overall)</div><div class="info-value">{brier}</div>
                    <div class="info-label">Brier score (recent)</div><div class="info-value">{recent_brier}</div>
                </div>
                <p class="empty" style="font-size: 0.8em; margin-top: 0.5em;">Brier score: 0 = perfect · &lt;0.05 excellent · &lt;0.1 good · &lt;0.15 fair · 0.25 = random coin flip</p>
            </div>"#,
            active = if rs.prediction_active { "Yes" } else { "No" },
            total = total_events,
            pf = peer_failure_events,
            pr = peer_response_events,
            pt = peer_transfer_events,
            rf = rs.renegade_failure_events,
            rr = rs.renegade_response_time_events,
            rt = rs.renegade_transfer_speed_events,
            rp = rs.renegade_known_peers,
            brier = rs
                .renegade_brier_score
                .map(|b| format!("{:.4}", b))
                .unwrap_or_else(|| "—".to_string()),
            recent_brier = rs
                .renegade_recent_brier_score
                .map(|b| format!("{:.4}", b))
                .unwrap_or_else(|| "—".to_string()),
            n_eval = rs.renegade_predictions_evaluated,
        )
    } else {
        r#"<div class="card"><h2>Routing Model</h2><p class="empty">Router data not available</p></div>"#.to_string()
    };

    // Build SVG charts with per-operation-type tabs
    let charts = if let Some(ref rs) = router_snapshot {
        let fail_adj = peer_routing
            .as_ref()
            .and_then(|pr| pr.failure_adjustment.map(|(m, _)| m));
        let rt_adj = peer_routing
            .as_ref()
            .and_then(|pr| pr.response_time_adjustment.map(|(m, _)| m));
        let xfer_adj = peer_routing
            .as_ref()
            .and_then(|pr| pr.transfer_rate_adjustment.map(|(m, _)| m));
        let ploc = peer.location;

        // Build tab content for each operation type
        let tab_names = ["All", "GET", "PUT", "UPDATE", "SUBSCRIBE"];
        let mut tab_labels = String::new();
        let mut tab_panels = String::new();

        for (i, &tab_name) in tab_names.iter().enumerate() {
            let tab_id = tab_name.to_lowercase().replace(' ', "-");

            // Get curves for this tab
            let (f_curve, f_range, rt_curve, rt_range, xfer_curve, xfer_range, event_count) =
                if tab_name == "All" {
                    (
                        rs.failure_curve.as_slice(),
                        rs.failure_data_range,
                        rs.response_time_curve.as_slice(),
                        rs.response_time_data_range,
                        rs.transfer_rate_curve.as_slice(),
                        rs.transfer_rate_data_range,
                        rs.failure_events,
                    )
                } else if let Some(c) = rs.per_op_curves.get(tab_name) {
                    (
                        c.failure_curve.as_slice(),
                        c.failure_data_range,
                        c.response_time_curve.as_slice(),
                        c.response_time_data_range,
                        c.transfer_rate_curve.as_slice(),
                        c.transfer_rate_data_range,
                        c.failure_events,
                    )
                } else {
                    (
                        &[][..],
                        (0.0, 0.0),
                        &[][..],
                        (0.0, 0.0),
                        &[][..],
                        (0.0, 0.0),
                        0,
                    )
                };

            // Tab label with event count badge
            let count_badge = if event_count > 0 {
                format!(r#" <span class="tab-count">{event_count}</span>"#)
            } else {
                String::new()
            };
            let dim_class = if event_count == 0 && tab_name != "All" {
                " tab-dim"
            } else {
                ""
            };
            let active_class = if i == 0 { " tab-active" } else { "" };
            write!(
                tab_labels,
                r#"<span class="tab-label{dim}{active}" data-tab="{id}" onclick="switchTab(this)">{name}{badge}</span>"#,
                id = tab_id, dim = dim_class, active = active_class, name = tab_name, badge = count_badge,
            ).ok();

            // Tab panel content
            let mut panel_content = String::new();
            if event_count == 0 && tab_name != "All" {
                write!(
                    panel_content,
                    r#"<div class="empty-chart">No routing events for {name} operations yet</div>"#,
                    name = tab_name,
                )
                .ok();
            } else {
                // Only show charts that have data
                if !f_curve.is_empty() {
                    panel_content.push_str(&build_estimator_chart(
                        "Failure Probability",
                        f_curve,
                        f_range,
                        if tab_name == "All" { fail_adj } else { None },
                        ploc,
                        "0.0",
                        "1.0",
                    ));
                }
                if !rt_curve.is_empty() {
                    panel_content.push_str(&build_estimator_chart(
                        "Response Time (s)",
                        rt_curve,
                        rt_range,
                        if tab_name == "All" { rt_adj } else { None },
                        ploc,
                        "0",
                        "auto",
                    ));
                }
                if !xfer_curve.is_empty() {
                    panel_content.push_str(&build_estimator_chart(
                        "Transfer Rate (B/s)",
                        xfer_curve,
                        xfer_range,
                        if tab_name == "All" { xfer_adj } else { None },
                        ploc,
                        "auto",
                        "0",
                    ));
                }
                if panel_content.is_empty() {
                    write!(
                        panel_content,
                        r#"<div class="empty-chart">Awaiting routing events</div>"#,
                    )
                    .ok();
                }
            }

            let panel_active = if i == 0 { " tab-panel-active" } else { "" };
            write!(
                tab_panels,
                r#"<div class="tab-panel{active}" id="panel-{id}">{content}</div>"#,
                active = panel_active,
                id = tab_id,
                content = panel_content,
            )
            .ok();
        }

        format!(
            r#"<div class="card">
                <h2>Routing Predictions</h2>
                <p class="chart-legend">
                    <span class="chart-key"><span class="chart-dot chart-dot-global"></span> Global model</span>
                    <span class="chart-key"><span class="chart-dot chart-dot-peer"></span> Peer-adjusted</span>
                    <span class="chart-key"><span class="chart-dot chart-dot-loc"></span> Peer location</span>
                    <span class="chart-key"><span class="chart-dot chart-dot-ext"></span> Extrapolated</span>
                </p>
                <div class="tab-group">
                    <div class="tab-bar">{tab_labels}</div>
                    {tab_panels}
                </div>
            </div>"#,
            tab_labels = tab_labels,
            tab_panels = tab_panels,
        )
    } else {
        String::new()
    };

    // Build renegade accuracy chart
    let renegade_chart = if let Some(ref rs) = router_snapshot {
        if rs.renegade_accuracy_pairs.is_empty() {
            String::new()
        } else {
            build_renegade_accuracy_chart(&rs.renegade_accuracy_pairs)
        }
    } else {
        String::new()
    };

    // Build prediction summary card
    let prediction_card = if let Some(ref pr) = peer_routing {
        if let Some(ref pred) = pr.prediction_at_own_location {
            format!(
                r#"<div class="card">
                    <h2>Prediction at Peer Location</h2>
                    <div class="info-grid">
                        <div class="info-label">Failure probability</div><div class="info-value">{fp:.4}</div>
                        <div class="info-label">Response time</div><div class="info-value">{rt:.3}s</div>
                        <div class="info-label">Expected total time</div><div class="info-value">{ett:.3}s</div>
                        <div class="info-label">Transfer speed</div><div class="info-value">{ts:.0} B/s</div>
                    </div>
                </div>"#,
                fp = pred.failure_probability,
                rt = pred.time_to_response_start,
                ett = pred.expected_total_time,
                ts = pred.transfer_speed_bps,
            )
        } else {
            r#"<div class="card"><h2>Prediction</h2><p class="empty">Insufficient data for prediction at this peer's location</p></div>"#.to_string()
        }
    } else {
        String::new()
    };

    let snap_ref = snap.as_ref();
    let version = snap_ref.map(|s| s.version.as_str()).unwrap_or("?");

    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Peer {addr} — Freenet</title>
    <style>{CSS}{PEER_CSS}</style>
    <script>{JS}</script>
</head>
<body>
    <header>
        <div class="header-left">
            <a href="/" class="logo-link"><img src="https://freenet.org/freenet_logo.svg" alt="Freenet" class="logo"></a>
            <a href="/" class="header-title">FREENET</a>
            <span class="header-sep">/</span>
            <span class="header-scope">Peer</span>
            <span class="header-addr">{addr}</span>
            <span class="badge">v{version}</span>
        </div>
        <div class="header-right">
            <button class="theme-btn" id="theme-btn" onclick="toggleTheme()" title="Toggle dark/light mode">
                <span id="theme-icon">☀️</span>
            </button>
        </div>
    </header>
    <main>
        {info_card}
        {model_card}
        {charts}
        {renegade_chart}
        {prediction_card}
    </main>
</body>
</html>"##,
        addr = html_escape(&peer.address.to_string()),
        CSS = CSS,
        PEER_CSS = PEER_CSS,
        JS = JS,
        version = html_escape(version),
        info_card = info_card,
        model_card = model_card,
        charts = charts,
        renegade_chart = renegade_chart,
        prediction_card = prediction_card,
    )
}

/// Build an SVG chart showing a PAV regression curve with optional per-peer adjustment.
///
/// `data_range` is `(data_x_min, data_x_max)` -- the x-range of actual regression data.
/// Points outside this range are extrapolated by the PAV crate and drawn as dashed lines.
fn build_estimator_chart(
    title: &str,
    curve_points: &[(f64, f64)],
    data_range: (f64, f64),
    peer_adjustment: Option<f64>,
    peer_location: Option<f64>,
    y_min_hint: &str,
    y_max_hint: &str,
) -> String {
    if curve_points.is_empty() {
        return format!(
            r#"<div class="chart-section"><h3>{title}</h3><div class="empty-chart">Awaiting routing events</div></div>"#,
            title = title,
        );
    }

    let w: f64 = 560.0;
    let h: f64 = 200.0;
    let pad_l: f64 = 50.0;
    let pad_r: f64 = 10.0;
    let pad_t: f64 = 10.0;
    let pad_b: f64 = 30.0;
    let plot_w = w - pad_l - pad_r;
    let plot_h = h - pad_t - pad_b;

    // Determine Y range: use fixed bounds if provided, otherwise auto-scale from data
    let fixed_y_min = y_min_hint.parse::<f64>().ok();
    let fixed_y_max = y_max_hint.parse::<f64>().ok();

    let mut y_min;
    let mut y_max;

    if let (Some(lo), Some(hi)) = (fixed_y_min, fixed_y_max) {
        y_min = lo;
        y_max = hi;
    } else {
        let y_vals: Vec<f64> = curve_points.iter().map(|(_, y)| *y).collect();
        y_min = y_vals.iter().cloned().fold(f64::INFINITY, f64::min);
        y_max = y_vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        // Include peer-adjusted values in range if present
        if let Some(adj) = peer_adjustment {
            for (_, y) in curve_points {
                let adjusted = y + adj;
                y_min = y_min.min(adjusted);
                y_max = y_max.max(adjusted);
            }
        }

        // Override individual bounds if a fixed hint was given
        if let Some(lo) = fixed_y_min {
            y_min = lo;
        }
        if let Some(hi) = fixed_y_max {
            y_max = hi;
        }

        // Add 10% padding and avoid zero-range (only for auto-scaled bounds)
        let range = y_max - y_min;
        if range < 1e-10 {
            y_min -= 0.5;
            y_max += 0.5;
        } else {
            if fixed_y_min.is_none() {
                y_min -= range * 0.1;
            }
            if fixed_y_max.is_none() {
                y_max += range * 0.1;
            }
        }
    }
    let y_range = y_max - y_min;

    // X is always distance [0.0, 0.5]
    let x_min: f64 = 0.0;
    let x_max: f64 = 0.5;
    let x_range = x_max - x_min;

    let to_svg_x = |x: f64| -> f64 { pad_l + ((x - x_min) / x_range) * plot_w };
    let to_svg_y = |y: f64| -> f64 { pad_t + plot_h - ((y - y_min) / y_range) * plot_h };

    let mut svg = format!(
        r#"<div class="chart-section"><h3>{title}</h3>
        <svg viewBox="0 0 {w} {h}" width="{w}" height="{h}" class="chart-svg">"#,
        title = title,
        w = w as u32,
        h = h as u32,
    );

    // Axes
    write!(
        svg,
        r#"<line x1="{lx}" y1="{ty}" x2="{lx}" y2="{by}" stroke="var(--text-muted)" stroke-width="1"/>"#,
        lx = pad_l,
        ty = pad_t,
        by = pad_t + plot_h,
    )
    .ok();
    write!(
        svg,
        r#"<line x1="{lx}" y1="{by}" x2="{rx}" y2="{by}" stroke="var(--text-muted)" stroke-width="1"/>"#,
        lx = pad_l,
        by = pad_t + plot_h,
        rx = pad_l + plot_w,
    )
    .ok();

    // X-axis labels
    for &x_tick in &[0.0, 0.1, 0.2, 0.3, 0.4, 0.5] {
        let sx = to_svg_x(x_tick);
        write!(
            svg,
            r#"<text x="{sx:.0}" y="{y}" text-anchor="middle" class="axis-label">{v:.1}</text>"#,
            sx = sx,
            y = pad_t + plot_h + 18.0,
            v = x_tick,
        )
        .ok();
    }

    // Y-axis labels (3 ticks)
    for i in 0..=2 {
        let frac = i as f64 / 2.0;
        let y_val = y_min + frac * y_range;
        let sy = to_svg_y(y_val);
        let label = if y_val.abs() < 1e-3 && y_range < 10.0 {
            format!("{:.3}", y_val)
        } else if y_range <= 1.0 {
            format!("{:.2}", y_val)
        } else {
            format!("{:.0}", y_val)
        };
        write!(
            svg,
            r#"<text x="{x}" y="{sy:.0}" text-anchor="end" class="axis-label">{label}</text>"#,
            x = pad_l - 4.0,
            sy = sy,
            label = label,
        )
        .ok();
    }

    // Helper: draw a curve with solid line in data range and dashed outside.
    // `points` are (x, y) pairs; `adj` is an optional y-offset for peer adjustment.
    let draw_curve = |svg: &mut String, points: &[(f64, f64)], adj: f64, color: &str| {
        if points.len() < 2 {
            return;
        }
        let (data_lo, data_hi) = data_range;

        // Split points into segments: extrapolated-left, data, extrapolated-right
        let mut left_ext = Vec::new();
        let mut data_seg = Vec::new();
        let mut right_ext = Vec::new();

        for &(x, y) in points {
            let y = y + adj;
            if x < data_lo - 0.001 {
                left_ext.push((x, y));
            } else if x > data_hi + 0.001 {
                right_ext.push((x, y));
            } else {
                data_seg.push((x, y));
            }
        }

        // Draw left extrapolation (dashed) -- include first data point for continuity
        if !left_ext.is_empty() {
            if let Some(&first_data) = data_seg.first() {
                left_ext.push(first_data);
            }
            let mut path = String::new();
            for (i, (x, y)) in left_ext.iter().enumerate() {
                let sx = to_svg_x(*x);
                let sy = to_svg_y(*y);
                if i == 0 {
                    write!(path, "M{sx:.1},{sy:.1}").ok();
                } else {
                    write!(path, " L{sx:.1},{sy:.1}").ok();
                }
            }
            write!(
                svg,
                r#"<path d="{path}" fill="none" stroke="{color}" stroke-width="1.5" stroke-dasharray="4,3" opacity="0.5"/>"#,
                path = path, color = color,
            ).ok();
        }

        // Draw data range (solid)
        if data_seg.len() >= 2 {
            let mut path = String::new();
            for (i, (x, y)) in data_seg.iter().enumerate() {
                let sx = to_svg_x(*x);
                let sy = to_svg_y(*y);
                if i == 0 {
                    write!(path, "M{sx:.1},{sy:.1}").ok();
                } else {
                    write!(path, " L{sx:.1},{sy:.1}").ok();
                }
            }
            write!(
                svg,
                r#"<path d="{path}" fill="none" stroke="{color}" stroke-width="2" opacity="0.8"/>"#,
                path = path,
                color = color,
            )
            .ok();
        } else if data_seg.len() == 1 {
            // Single data point -- draw as a dot
            let (x, y) = data_seg[0];
            write!(
                svg,
                r#"<circle cx="{cx:.1}" cy="{cy:.1}" r="3" fill="{color}" opacity="0.8"/>"#,
                cx = to_svg_x(x),
                cy = to_svg_y(y),
                color = color,
            )
            .ok();
        }

        // Draw right extrapolation (dashed) -- include last data point for continuity
        if !right_ext.is_empty() {
            if let Some(&last_data) = data_seg.last() {
                right_ext.insert(0, last_data);
            }
            let mut path = String::new();
            for (i, (x, y)) in right_ext.iter().enumerate() {
                let sx = to_svg_x(*x);
                let sy = to_svg_y(*y);
                if i == 0 {
                    write!(path, "M{sx:.1},{sy:.1}").ok();
                } else {
                    write!(path, " L{sx:.1},{sy:.1}").ok();
                }
            }
            write!(
                svg,
                r#"<path d="{path}" fill="none" stroke="{color}" stroke-width="1.5" stroke-dasharray="4,3" opacity="0.5"/>"#,
                path = path, color = color,
            ).ok();
        }
    };

    // Global curve (blue)
    draw_curve(&mut svg, curve_points, 0.0, "var(--accent-primary)");

    // Peer-adjusted curve (green)
    if let Some(adj) = peer_adjustment {
        draw_curve(&mut svg, curve_points, adj, "#34d399");
    }

    // Peer location marker (vertical dashed line)
    if let Some(loc) = peer_location {
        // Distance from peer to itself is 0, but contracts near the peer have small distances.
        // Mark the peer's ring location on the x-axis as distance=0 (leftmost).
        let _ = loc; // The peer itself is at distance 0 from contracts at its own location
        let sx = to_svg_x(0.0);
        write!(
            svg,
            "<line x1=\"{sx:.1}\" y1=\"{ty}\" x2=\"{sx:.1}\" y2=\"{by}\" stroke=\"#fbbf24\" stroke-width=\"1.5\" stroke-dasharray=\"4,3\" opacity=\"0.7\"/>",
            sx = sx,
            ty = pad_t,
            by = pad_t + plot_h,
        )
        .ok();
    }

    svg.push_str("</svg></div>");
    svg
}

/// Build an SVG strip chart showing predicted failure probability vs actual outcome.
/// X-axis: predicted probability [0, 1].
/// Y-axis: actual outcome (0 = success at bottom, 1 = failure at top).
/// Good calibration: successes cluster left, failures cluster right.
fn build_renegade_accuracy_chart(pairs: &[(f64, f64)]) -> String {
    use std::fmt::Write;

    let w = 400.0_f64;
    let h = 160.0_f64;
    let pad_l = 50.0;
    let pad_r = 20.0;
    let pad_t = 30.0;
    let pad_b = 30.0;
    let plot_w = w - pad_l - pad_r;
    let plot_h = h - pad_t - pad_b;

    let to_x = |predicted: f64| -> f64 { pad_l + predicted.clamp(0.0, 1.0) * plot_w };
    let to_y = |actual: f64| -> f64 {
        if actual > 0.5 {
            pad_t + 10.0 // failure near top
        } else {
            pad_t + plot_h - 10.0 // success near bottom
        }
    };

    let mut svg = format!(
        r#"<div class="card">
        <h2>Renegade Prediction Accuracy</h2>
        <p style="font-size:0.8em;color:var(--text-muted);">
            Each dot is a routing event. X = predicted failure probability.
            Top row = actual failures, bottom = actual successes.
            Well-calibrated: failures cluster right, successes cluster left.
        </p>
        <svg viewBox="0 0 {w} {h}" width="{w}" height="{h}" class="chart-svg">"#,
        w = w as u32,
        h = h as u32,
    );

    // Background
    write!(
        svg,
        r#"<rect x="{lx}" y="{ty}" width="{pw}" height="{ph}" fill="var(--bg-secondary)" rx="2"/>"#,
        lx = pad_l,
        ty = pad_t,
        pw = plot_w,
        ph = plot_h,
    )
    .ok();

    // Axes
    write!(
        svg,
        r#"<line x1="{lx}" y1="{by}" x2="{rx}" y2="{by}" stroke="var(--text-muted)" stroke-width="1"/>"#,
        lx = pad_l,
        by = pad_t + plot_h,
        rx = pad_l + plot_w,
    )
    .ok();

    // X-axis labels
    for &v in &[0.0, 0.25, 0.5, 0.75, 1.0] {
        let x = to_x(v);
        write!(
            svg,
            r#"<text x="{x}" y="{y}" text-anchor="middle" font-size="9" fill="var(--text-muted)">{v}</text>"#,
            x = x,
            y = pad_t + plot_h + 15.0,
            v = v,
        )
        .ok();
        // Grid line
        write!(
            svg,
            r#"<line x1="{x}" y1="{ty}" x2="{x}" y2="{by}" stroke="var(--text-muted)" stroke-width="0.3" stroke-dasharray="3"/>"#,
            x = x,
            ty = pad_t,
            by = pad_t + plot_h,
        )
        .ok();
    }

    // Y-axis labels
    write!(
        svg,
        r#"<text x="{x}" y="{y}" text-anchor="end" font-size="9" fill="var(--text-muted)">Failure</text>"#,
        x = pad_l - 4.0,
        y = pad_t + 14.0,
    )
    .ok();
    write!(
        svg,
        r#"<text x="{x}" y="{y}" text-anchor="end" font-size="9" fill="var(--text-muted)">Success</text>"#,
        x = pad_l - 4.0,
        y = pad_t + plot_h - 6.0,
    )
    .ok();

    // X-axis title
    write!(
        svg,
        r#"<text x="{x}" y="{y}" text-anchor="middle" font-size="9" fill="var(--text-muted)">Predicted failure probability</text>"#,
        x = pad_l + plot_w / 2.0,
        y = h - 2.0,
    )
    .ok();

    // Plot points with jitter to avoid overlap
    let mut jitter_seed = 0u32;
    for &(predicted, actual) in pairs {
        // Simple deterministic jitter
        jitter_seed = jitter_seed.wrapping_mul(1103515245).wrapping_add(12345);
        let jitter = ((jitter_seed >> 16) as f64 / 65535.0 - 0.5) * (plot_h * 0.3);

        let x = to_x(predicted);
        let y = to_y(actual) + jitter;
        let y = y.clamp(pad_t + 2.0, pad_t + plot_h - 2.0);

        let color = if actual > 0.5 {
            "var(--accent-danger, #f85149)"
        } else {
            "var(--accent-success, #3fb950)"
        };

        write!(
            svg,
            r#"<circle cx="{x:.1}" cy="{y:.1}" r="2.5" fill="{color}" opacity="0.6"/>"#,
            x = x,
            y = y,
            color = color,
        )
        .ok();
    }

    // Count stats
    let n_success = pairs.iter().filter(|(_, a)| *a < 0.5).count();
    let n_failure = pairs.len() - n_success;

    write!(
        svg,
        r#"<text x="{x}" y="{y}" text-anchor="start" font-size="9" fill="var(--text-muted)">{n} pts ({s} ok, {f} fail)</text>"#,
        x = pad_l + 2.0,
        y = pad_t - 5.0,
        n = pairs.len(),
        s = n_success,
        f = n_failure,
    )
    .ok();

    svg.push_str("</svg></div>");
    svg
}

const PEER_CSS: &str = r##"
.logo-link, .header-title { text-decoration: none; }
.logo-link:hover, a.header-title:hover { opacity: 0.8; }
a.header-title {
    background: linear-gradient(135deg, var(--accent-light) 0%, var(--accent-primary) 50%, var(--accent-dark) 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    font-family: var(--font-mono);
    font-weight: 600;
    font-size: 1.1rem;
    letter-spacing: -0.02em;
}
.header-sep {
    color: var(--text-muted);
    font-size: 1rem;
    margin: 0 0.15rem;
}
.header-addr {
    font-family: var(--font-mono);
    font-size: 0.8rem;
    color: var(--text-secondary);
    background: var(--bg-tertiary);
    padding: 0.1rem 0.45rem;
    border-radius: 4px;
}
.info-grid {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 0.4rem 1.25rem;
    font-size: 0.85rem;
}
.info-label {
    color: var(--text-secondary);
    font-family: var(--font-mono);
    font-weight: 500;
    text-transform: uppercase;
    font-size: 0.75rem;
    align-self: center;
}
.info-value { font-family: var(--font-mono); }
.chart-section { margin: 1rem 0; }
.chart-section h3 {
    font-size: 0.8rem;
    color: var(--text-secondary);
    font-family: var(--font-mono);
    font-weight: 500;
    text-transform: uppercase;
    margin-bottom: 0.5rem;
}
.chart-svg {
    display: block;
    width: 100%;
    max-width: 600px;
}
.chart-svg .axis-label {
    font-family: var(--font-mono);
    font-size: 10px;
    fill: var(--text-muted);
}
.chart-legend {
    display: flex;
    gap: 1rem;
    margin-bottom: 0.75rem;
    font-size: 0.75rem;
    color: var(--text-secondary);
}
.chart-key {
    display: flex;
    align-items: center;
    gap: 0.3rem;
}
.chart-dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    display: inline-block;
}
.chart-dot-global { background: var(--accent-primary); }
.chart-dot-peer { background: #34d399; }
.chart-dot-loc { background: #fbbf24; }
.chart-dot-ext {
    background: transparent;
    border: 2px dashed var(--accent-primary);
    width: 8px;
    height: 8px;
}
/* Tabs */
.tab-group { margin-top: 0.5rem; }
.tab-bar {
    display: flex;
    gap: 0;
    border-bottom: 1px solid var(--border-color);
    margin-bottom: 0.75rem;
    overflow-x: auto;
}
.tab-label {
    font-family: var(--font-mono);
    font-size: 0.75rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    color: var(--text-secondary);
    padding: 0.4rem 0.75rem;
    cursor: pointer;
    border-bottom: 2px solid transparent;
    transition: color 0.15s, border-color 0.15s;
    white-space: nowrap;
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
}
.tab-label:hover { color: var(--text-primary); }
.tab-label.tab-dim { color: var(--text-muted); }
.tab-label.tab-dim:hover { color: var(--text-secondary); }
.tab-label.tab-active {
    color: var(--accent-primary);
    border-bottom-color: var(--accent-primary);
}
.tab-count {
    font-size: 0.65rem;
    background: var(--bg-tertiary);
    color: var(--text-muted);
    padding: 0.05rem 0.35rem;
    border-radius: 8px;
    min-width: 1.2em;
    text-align: center;
}
.tab-label.tab-active .tab-count {
    background: rgba(0, 127, 255, 0.15);
    color: var(--accent-primary);
}
.tab-panel { display: none; }
.tab-panel.tab-panel-active { display: block; }
.empty-chart {
    display: flex;
    align-items: center;
    justify-content: center;
    height: 80px;
    background: var(--bg-secondary);
    border: 1px dashed var(--border-color);
    border-radius: 6px;
    color: var(--text-muted);
    font-size: 0.8rem;
    font-family: var(--font-mono);
}
"##;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::network_status::{
        FailureSnapshot, HealthLevel, NatStatsSnapshot, NetworkStatusSnapshot, OpStatsSnapshot,
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
            external_address: None,
            peers: Vec::new(),
            contracts: Vec::new(),
            op_stats: OpStatsSnapshot::default(),
            nat_stats: NatStatsSnapshot::default(),
            gateway_only: false,
            bytes_uploaded: 0,
            bytes_downloaded: 0,
            health: HealthLevel::Connecting,
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
    fn health_banner_shown_for_each_level() {
        // Healthy
        let mut snap = base_snapshot();
        snap.health = HealthLevel::Healthy;
        snap.open_connections = 3;
        let html = build_status_card(&Some(snap));
        assert!(html.contains("health-good"), "healthy banner missing");
        assert!(html.contains("Node is healthy"));

        // Degraded
        let mut snap = base_snapshot();
        snap.health = HealthLevel::Degraded;
        snap.gateway_only = true;
        snap.open_connections = 1;
        let html = build_status_card(&Some(snap));
        assert!(html.contains("health-degraded"), "degraded banner missing");

        // Connecting
        let snap = base_snapshot(); // default is Connecting
        let html = build_status_card(&Some(snap));
        assert!(
            html.contains("health-connecting"),
            "connecting banner missing"
        );

        // Trouble
        let mut snap = base_snapshot();
        snap.health = HealthLevel::Trouble;
        let html = build_status_card(&Some(snap));
        assert!(html.contains("health-trouble"), "trouble banner missing");
    }

    #[test]
    fn failures_demoted_when_connected() {
        let mut snap = base_snapshot();
        snap.open_connections = 3;
        snap.health = HealthLevel::Healthy;
        snap.failures.push(FailureSnapshot {
            address: "1.2.3.4:1234".parse().unwrap(),
            reason_html: "NAT traversal failed".to_string(),
        });
        let html = build_status_card(&Some(snap));
        // Should use <details> (collapsed) instead of prominent .diagnostics
        assert!(
            html.contains("diagnostics-muted"),
            "failures should be demoted when connected"
        );
        assert!(
            !html.contains(r#"class="diagnostics""#),
            "should not use prominent diagnostics style"
        );
        assert!(
            html.contains("(normal)"),
            "should indicate failures are normal"
        );
    }

    #[test]
    fn failures_prominent_when_not_connected() {
        let mut snap = base_snapshot();
        snap.open_connections = 0;
        snap.health = HealthLevel::Connecting;
        snap.failures.push(FailureSnapshot {
            address: "1.2.3.4:1234".parse().unwrap(),
            reason_html: "timeout".to_string(),
        });
        let html = build_status_card(&Some(snap));
        assert!(
            html.contains(r#"class="diagnostics""#),
            "failures should be prominent when not connected"
        );
        assert!(!html.contains("diagnostics-muted"), "should not be demoted");
    }

    #[test]
    fn transfer_card_shows_bytes() {
        let mut snap = base_snapshot();
        snap.bytes_uploaded = 1024 * 1024 * 5; // 5 MB
        snap.open_connections = 1;
        let html = build_transfer_card(&Some(snap));
        assert!(html.contains("5.0 MB"), "should show formatted bytes");
        assert!(html.contains("Uploaded"), "should show upload label");
    }

    #[test]
    fn transfer_card_hidden_when_no_data() {
        let snap = base_snapshot();
        let html = build_transfer_card(&Some(snap));
        assert!(
            html.is_empty(),
            "transfer card should be hidden with no data"
        );
    }

    #[test]
    fn format_bytes_units() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
        assert_eq!(format_bytes(1024 * 1024 * 1024 * 3 / 2), "1.5 GB");
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

    #[test]
    fn no_meta_refresh_in_homepage() {
        let html = homepage_html();
        assert!(
            !html.contains("http-equiv=\"refresh\""),
            "meta refresh must not be present — JS partial update is used instead"
        );
    }

    #[test]
    fn subscribe_cell_shows_active_count() {
        use crate::node::network_status::ContractSnapshot;

        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.op_stats.subscribes = (250, 3);
        snap.contracts = vec![
            ContractSnapshot {
                key_short: "ABC1...".to_string(),
                key_full: "ABC123".to_string(),
                subscribed_secs: 100,
                last_updated_secs: Some(5),
            },
            ContractSnapshot {
                key_short: "DEF4...".to_string(),
                key_full: "DEF456".to_string(),
                subscribed_secs: 50,
                last_updated_secs: None,
            },
        ];
        let html = build_ops_card(&Some(snap));
        assert!(
            html.contains("2 active"),
            "should show active subscription count, got: {html}"
        );
        assert!(
            html.contains("253 ops"),
            "should show total ops as secondary info, got: {html}"
        );
        assert!(
            !html.contains("\u{2713} 250"),
            "should not show raw success/fail format for subscribes"
        );
    }

    #[test]
    fn subscribe_cell_zero_ops_shows_active_only() {
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.op_stats.gets = (1, 0); // need some ops so card renders
        let html = build_ops_card(&Some(snap));
        assert!(
            html.contains("0 active"),
            "should show 0 active when no subscriptions, got: {html}"
        );
        assert!(!html.contains("0 ops"), "should hide ops line when zero");
    }

    #[test]
    fn no_meta_refresh_in_peer_detail() {
        let html = peer_detail_html("127.0.0.1:31337");
        assert!(
            !html.contains("http-equiv=\"refresh\""),
            "meta refresh must not be present — JS partial update is used instead"
        );
    }

    #[test]
    fn js_contains_auto_refresh() {
        assert!(
            JS.contains("scheduleRefresh"),
            "JS constant must contain the auto-refresh scheduler"
        );
    }
}
