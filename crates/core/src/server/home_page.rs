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
    let governance_card = build_governance_card(&snap);
    let contracts_card = build_contracts_card(&snap);
    let ops_card = build_ops_card(&snap);
    let transfer_card = build_transfer_card(&snap);

    let peer_id = snap
        .as_ref()
        .and_then(|s| {
            if s.ring_stats.peer_id.is_empty() {
                None
            } else {
                Some(s.ring_stats.peer_id.as_str())
            }
        })
        .unwrap_or("?");
    let pub_key = snap
        .as_ref()
        .and_then(|s| {
            if s.ring_stats.own_pub_key.is_empty() {
                None
            } else {
                Some(s.ring_stats.own_pub_key.as_str())
            }
        })
        .unwrap_or("?");
    let peer_copy_btn = if peer_id == "?" {
        String::new()
    } else {
        r#"<button class="copy-btn" onclick="copyToClipboard(document.getElementById('peer-id').textContent).then(function(){showToast('Peer ID copied')})" title="Copy peer ID">&#x2398;</button>"#
            .to_string()
    };
    let pub_copy_btn = if pub_key == "?" {
        String::new()
    } else {
        r#"<button class="copy-btn" onclick="copyToClipboard(document.getElementById('pub-key').textContent).then(function(){showToast('Pub key copied')})" title="Copy public key">&#x2398;</button>"#
            .to_string()
    };

    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FN Peer</title>
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
            <span class="badge" id="version-badge" data-version="{version}">v{version}</span>
            <a class="update-badge" id="update-badge" href="https://github.com/freenet/freenet-core/releases/latest" target="_blank" rel="noopener noreferrer" hidden>Update available</a>
            <span class="pub-key-label">Peer ID</span>
            <code class="pub-key" id="peer-id" title="Click to copy">{peer_id}</code>{peer_copy_btn}
            <span class="pub-key-label">Pub key</span>
            <code class="pub-key" id="pub-key" title="Click to copy">{pub_key}</code>{pub_copy_btn}
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
        {governance_card}
        {contracts_card}
        {ops_card}

        <div class="card">
            <h2>Freenet Links</h2>
            <ul class="app-list">
                <li>
                    <a href="/v1/contract/web/raAqMhMG7KUpXBU2SxgCQ3Vh4PYjttxdSWd9ftV7RLv/">River Chat</a>
                    <p class="note">You'll need an <a href="https://freenet.org/quickstart#invite-form" target="_blank" rel="noopener noreferrer">invite</a> to join the "Freenet Official" room.</p>
                </li>
                <li>
                    <a href="/v1/contract/web/122f6AR7PyF8d8mhczuNQM4xrLtBf5t8g2iB7PEVT7KC/">Freenet Mail</a>
                    <p class="note">Decentralized email built on Freenet. <a href="https://github.com/freenet/mail" target="_blank" rel="noopener noreferrer">Source</a>.</p>
                </li>
                <li>
                    <a href="/v1/contract/web/EqJ5YpEEV3XLqEvKWLQHFhGAac2qXzSUoE6k2zbdnXBr/">Delta</a>
                    <p class="note">A website builder for Freenet.</p>
                </li>
                <li>
                    <a href="/v1/contract/web/DLog47hEsrtuGT4N5XCeMBG45m4n1aWM89tBZXue2E1N/">Ghostkey Identity Vault</a>
                    <p class="note">Manage Ghostkey identities on Freenet.</p>
                </li>
                <li>
                    <a href="/v1/contract/web/E4m5WbaC4cdbpDjL82WfYUnrM9iMnfaaN8Tsn7UHPMjZ/">freenet.org</a>
                    <p class="note">The freenet.org website, mirrored on Freenet.</p>
                </li>
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
        peer_id = html_escape(peer_id),
        peer_copy_btn = peer_copy_btn,
        pub_key = html_escape(pub_key),
        pub_copy_btn = pub_copy_btn,
        status_card = status_card,
        peers_card = peers_card,
        transfer_card = transfer_card,
        governance_card = governance_card,
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

    // Ring stats row: connection count, hosted contracts, connection attempts
    let ring_stats_html = format!(
        r#"<div class="metrics-row">
            <div class="metric-tile">
                <span class="metric-value">{conns}</span>
                <span class="metric-label">Ring peers</span>
            </div>
            <div class="metric-tile">
                <span class="metric-value">{hosted}</span>
                <span class="metric-label">Hosted contracts</span>
            </div>
            <div class="metric-tile">
                <span class="metric-value">{attempts}</span>
                <span class="metric-label">Conn attempts</span>
            </div>
        </div>"#,
        conns = snap.ring_stats.connection_count,
        hosted = snap.ring_stats.hosted_contracts,
        attempts = snap.connection_attempts,
    );

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
            {ring_stats_html}
            {external_addr_html}
            {spinner}
            {gateway_warning}
            {nat_html}
            {failures_html}
        </div>"#,
        health_banner = health_banner,
        ring_stats_html = ring_stats_html,
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
    // Always show the card once the node has been up for more than a few
    // seconds — initial connection flapping shouldn't make the panel
    // appear/disappear rhythmically during auto-refresh.
    if snap.bytes_uploaded == 0 && snap.bytes_downloaded == 0 && snap.elapsed_secs < 10 {
        return String::new();
    }

    let ts = &snap.transport_snapshot;

    let peak_tput = if ts.peak_throughput_bps > 0 {
        format!(
            " <span class=\"transfer-detail\">(peak {}/s)</span>",
            format_bytes(ts.peak_throughput_bps)
        )
    } else {
        String::new()
    };

    let rtt_str = if ts.avg_rtt_us > 0 {
        format!(
            r#"<div class="transfer-stat">
                <span class="transfer-label">RTT (avg/min/max)</span>
                <span class="transfer-value">{avg}ms / {min}ms / {max}ms</span>
            </div>"#,
            avg = format_args!("{:.1}", ts.avg_rtt_us as f64 / 1000.0),
            min = format_args!("{:.1}", ts.min_rtt_us as f64 / 1000.0),
            max = format_args!("{:.1}", ts.max_rtt_us as f64 / 1000.0),
        )
    } else {
        String::new()
    };

    let cwnd_str = if ts.avg_cwnd_bytes > 0 {
        format!(
            r#"<div class="transfer-stat">
                <span class="transfer-label">cwnd (avg/peak/min)</span>
                <span class="transfer-value">{avg} / {peak} / {min}</span>
            </div>"#,
            avg = format_bytes(ts.avg_cwnd_bytes as u64),
            peak = format_bytes(ts.peak_cwnd_bytes as u64),
            min = format_bytes(ts.min_cwnd_bytes as u64),
        )
    } else {
        String::new()
    };

    let slowdown_str = if ts.slowdowns_triggered > 0 {
        format!(
            r#"<div class="transfer-stat">
                <span class="transfer-label">LEDBAT slowdowns</span>
                <span class="transfer-value">{s}</span>
            </div>"#,
            s = ts.slowdowns_triggered,
        )
    } else {
        String::new()
    };

    let xfer_str = if ts.transfers_completed > 0 || ts.transfers_failed > 0 {
        format!(
            r#"<div class="transfer-stat">
                <span class="transfer-label">Transfers (avg time)</span>
                <span class="transfer-value">{ok} ok / {fail} fail <span class="transfer-detail">({avg}s avg)</span></span>
            </div>"#,
            ok = ts.transfers_completed,
            fail = ts.transfers_failed,
            avg = format_args!("{:.3}", ts.avg_transfer_time_ms as f64 / 1000.0),
        )
    } else {
        String::new()
    };

    format!(
        r#"<div class="card">
            <h2>Data Transfer</h2>
            <div class="transfer-stat">
                <span class="transfer-label">Uploaded</span>
                <span class="transfer-value">{uploaded}{peak_tput}</span>
            </div>
            <div class="transfer-stat">
                <span class="transfer-label">Downloaded</span>
                <span class="transfer-value">{downloaded}</span>
            </div>
            {xfer_str}
            {rtt_str}
            {cwnd_str}
            {slowdown_str}
        </div>"#,
        uploaded = format_bytes(snap.bytes_uploaded),
        peak_tput = peak_tput,
        downloaded = format_bytes(snap.bytes_downloaded),
        xfer_str = xfer_str,
        rtt_str = rtt_str,
        cwnd_str = cwnd_str,
        slowdown_str = slowdown_str,
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

    let ring_svg = build_ring_svg(
        snap.own_location,
        &snap.peers,
        Some(&snap.governance),
        &snap.contracts,
    );

    let mut rows = String::new();
    for p in &snap.peers {
        let peer_type = if p.is_gateway { "Gateway" } else { "Peer" };
        let loc_sort = p.location.map(|l| l.to_string()).unwrap_or_default();
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
            r#"<tr class="peer-row" onclick="window.location='/peer/{addr_enc}'"><td data-sort="{addr_enc}"><code>{addr}</code></td><td data-sort="{loc_sort}">{loc}</td><td data-sort="{ptype}">{ptype}</td><td data-sort="{bytes_sent}">{sent}</td><td data-sort="{bytes_recv}">{recv}</td><td data-sort="{conn_secs}">{connected}</td></tr>"#,
            addr_enc = html_escape(&p.address.to_string()),
            addr = p.address,
            loc_sort = loc_sort,
            loc = loc,
            ptype = peer_type,
            bytes_sent = p.bytes_sent,
            sent = sent,
            bytes_recv = p.bytes_received,
            recv = recv,
            conn_secs = p.connected_secs,
            connected = format_duration(p.connected_secs),
        ));
    }

    format!(
        r#"<div class="card">
            <div class="card-header"><h2>Network Peers</h2>{own_loc}</div>
            {ring_svg}
            <div class="table-wrap">
                <table class="sortable" data-table-id="peers">
                    <thead><tr><th data-sort-type="text">Address</th><th data-sort-type="num">Location</th><th data-sort-type="text">Type</th><th data-sort-type="num">Sent</th><th data-sort-type="num">Recv</th><th data-sort-type="num">Connected</th></tr></thead>
                    <tbody>{rows}</tbody>
                </table>
            </div>
        </div>"#,
        own_loc = own_loc,
        ring_svg = ring_svg,
        rows = rows,
    )
}

/// Build an SVG ring visualization. Two concentric rings:
///
/// - **Outer ring**: peers we're connected to, placed by their
///   ring-location (Kleinberg topology position).
/// - **Inner ring**: contracts the governance manager is tracking,
///   placed by a deterministic hash of the instance id. Colored by
///   governance state.
///
/// Lines (Bezier curves through the interior) go YOU → peer (outer)
/// and YOU → contract (inner). All curves originate from or terminate
/// at YOU — a node only sees its own traffic, so anything else would
/// be fabricated.
///
/// Falls back to outer-ring-only rendering when no governance
/// snapshot is supplied (e.g. tests that pre-date the upgrade).
fn build_ring_svg(
    own_location: Option<f64>,
    peers: &[network_status::PeerSnapshot],
    governance: Option<&network_status::GovernanceSnapshot>,
    hosted_contracts: &[network_status::ContractSnapshot],
) -> String {
    // Render when there's *something* to show. The historical guard
    // (at least one peer with a location) still applies.
    let has_any_location = own_location.is_some() || peers.iter().any(|p| p.location.is_some());
    if !has_any_location {
        return String::new();
    }

    // Larger viewBox than the original 240×240 so the inner ring +
    // labels have room. Use a viewBox-only sizing (no fixed width)
    // so the SVG scales with the card.
    let size: f64 = 480.0;
    let cx: f64 = size / 2.0;
    let cy: f64 = size / 2.0;
    let r_outer: f64 = 195.0;
    let r_inner: f64 = 120.0;

    let mut svg = format!(
        "<div class=\"ring-wrap\"><svg viewBox=\"0 0 {size:.0} {size:.0}\" class=\"ring-svg\" preserveAspectRatio=\"xMidYMid meet\">"
    );

    // Show the inner ring when there are *any* contracts to display
    // (governance-flagged OR hosted).  Without this the hosted dots
    // render orphaned on an invisible ring.
    let has_inner_ring =
        governance.is_some_and(|g| !g.contracts.is_empty()) || !hosted_contracts.is_empty();

    // === Background rings ===
    write!(
        svg,
        "<circle cx=\"{cx}\" cy=\"{cy}\" r=\"{r_outer}\" fill=\"none\" stroke=\"#363c4a\" stroke-width=\"1\"/>"
    )
    .ok();
    if has_inner_ring {
        write!(
            svg,
            "<circle cx=\"{cx}\" cy=\"{cy}\" r=\"{r_inner}\" fill=\"none\" stroke=\"#363c4a\" stroke-width=\"1\"/>"
        )
        .ok();
    }

    // Helper: location (0.0..1.0) → (x, y) on a ring of given radius.
    // 0.0 is at the top, increasing clockwise.
    let loc_to_xy = |loc: f64, r: f64| -> (f64, f64) {
        let angle = loc * std::f64::consts::TAU - std::f64::consts::FRAC_PI_2;
        (cx + r * angle.cos(), cy + r * angle.sin())
    };

    // Curved chord path generator. Quadratic Bezier from (x1,y1) to
    // (x2,y2) with the control point pulled toward the SVG centre.
    // `bend_base` is the maximum bend for antipodal points; chord
    // length scales bend so short hops look flat and long hops arc
    // through the interior. Matches the nova.locut.us:3133 ring
    // routing visualization.
    let curve_path = |x1: f64, y1: f64, x2: f64, y2: f64, bend_base: f64| -> String {
        let dx = x2 - x1;
        let dy = y2 - y1;
        let chord = (dx * dx + dy * dy).sqrt();
        let ratio = (chord / (2.0 * r_outer)).min(1.0);
        let bend = bend_base * ratio;
        let mx = (x1 + x2) / 2.0;
        let my = (y1 + y2) / 2.0;
        let pcx = cx + (mx - cx) * (1.0 - bend);
        let pcy = cy + (my - cy) * (1.0 - bend);
        format!("M {x1:.1},{y1:.1} Q {pcx:.1},{pcy:.1} {x2:.1},{y2:.1}")
    };

    // === Ring labels at top ===
    write!(
        svg,
        "<text x=\"{cx}\" y=\"{y:.1}\" text-anchor=\"middle\" fill=\"#6b7280\" font-family=\"monospace\" font-size=\"9\" letter-spacing=\"0.18em\">PEERS</text>",
        y = cy - r_outer - 8.0,
    )
    .ok();
    if has_inner_ring {
        write!(
            svg,
            "<text x=\"{cx}\" y=\"{y:.1}\" text-anchor=\"middle\" fill=\"#6b7280\" font-family=\"monospace\" font-size=\"9\" letter-spacing=\"0.18em\">CONTRACTS</text>",
            y = cy - r_inner - 8.0,
        )
        .ok();
    }

    let own_xy = own_location.map(|loc| loc_to_xy(loc, r_outer));

    // === Connection curves: YOU → peers ===
    // Stroke width scales with total bytes transferred to this peer,
    // giving a visual "data flow" indication.  Floor at 0.6 so even
    // idle peers are visible; ceiling at 3.0 for the busiest peer.
    // Gateways get a warm amber arc, regular peers a teal one.
    if let Some((ox, oy)) = own_xy {
        // Find max transfer for relative scaling
        let max_xfer = peers
            .iter()
            .map(|p| p.bytes_sent.saturating_add(p.bytes_received))
            .max()
            .unwrap_or(1)
            .max(1);
        for p in peers {
            if let Some(ploc) = p.location {
                let (px, py) = loc_to_xy(ploc, r_outer);
                let total = p.bytes_sent.saturating_add(p.bytes_received);
                let sw = 0.6 + 2.4 * (total as f64 / max_xfer as f64);
                let (stroke, opacity) = if p.is_gateway {
                    ("#f0a030", 0.55)
                } else {
                    ("#0abab5", 0.45)
                };
                let path = curve_path(ox, oy, px, py, 0.55);
                write!(
                    svg,
                    "<path d=\"{path}\" fill=\"none\" stroke=\"{stroke}\" stroke-width=\"{sw:.1}\" stroke-opacity=\"{opacity}\" stroke-linecap=\"round\"/>"
                )
                .ok();
            }
        }
    }

    // Deterministic hash → ring location for placing contracts.
    // The instance id is a 32-byte content hash; we fold the
    // string form to a u64 and modulo onto [0, 1). Position is
    // stable across refreshes (same id → same dot location).
    let hash_to_loc = |s: &str| -> f64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        s.hash(&mut h);
        (h.finish() % 10_000) as f64 / 10_000.0
    };

    // === Hosted contracts on the inner ring (faint dots) ===
    //
    // Before drawing flagged-contract markers, draw ALL hosted
    // contracts as faint dim dots so the inner ring isn't visually
    // empty in healthy state. Hosted-contract data comes from the
    // Subscribed Contracts list (different source from governance,
    // which only carries flagged entries). A contract that's also
    // flagged will get a brighter overlay drawn on top below.
    //
    // This addresses the "inner ring looks broken" finding from the
    // UI feedback doc — operators saw the CONTRACTS wordmark with
    // no dots beneath it and assumed the renderer was unfinished.
    let governance_ids: std::collections::HashSet<&str> = governance
        .map(|g| g.contracts.iter().map(|c| c.instance_id.as_str()).collect())
        .unwrap_or_default();
    for c in hosted_contracts {
        // Skip contracts that are flagged — they get a more visible
        // marker in the flagged-rendering loop below. Drawing them
        // here would just be overlapped.
        if governance_ids.contains(c.instance_id.as_str()) {
            continue;
        }
        // Hash on the instance_id so the hosted-dot position matches
        // the same contract's flagged-dot position (which also hashes
        // on instance_id) — Codex review caught the previous
        // key_full vs instance_id mismatch.
        let loc = hash_to_loc(&c.instance_id);
        let (kx, ky) = loc_to_xy(loc, r_inner);
        // Dim teal dot — same brand color as YOU but smaller and
        // translucent so flagged dots stand out by contrast.
        write!(
            svg,
            "<circle cx=\"{x:.1}\" cy=\"{y:.1}\" r=\"2.5\" fill=\"#43c178\" fill-opacity=\"0.45\"><title>{title}</title></circle>",
            x = kx,
            y = ky,
            title = html_escape(&format!("{} (hosted)", &c.key_short)),
        )
        .ok();
    }

    // === Contract dots (and YOU → flagged-contract curves) ===
    if let Some(gov) = governance {
        for c in &gov.contracts {
            let loc = hash_to_loc(&c.instance_id);
            let (kx, ky) = loc_to_xy(loc, r_inner);
            let (fill, glow) = match c.state {
                network_status::GovernanceStateSnapshot::Normal => ("#43c178", false),
                network_status::GovernanceStateSnapshot::Borderline => ("#ffb610", false),
                network_status::GovernanceStateSnapshot::WouldEvict => ("#ff8a3d", true),
                network_status::GovernanceStateSnapshot::Evicted => ("#ff667a", true),
                network_status::GovernanceStateSnapshot::Banned => ("#d33682", true),
            };
            // Flagged contracts get a curve from YOU into the inner
            // ring + a small label of the short instance id. Normal
            // ones get a dot only — they'd otherwise overwhelm the
            // visual with curves.
            let is_flagged = !matches!(c.state, network_status::GovernanceStateSnapshot::Normal);
            if is_flagged {
                if let Some((ox, oy)) = own_xy {
                    let path = curve_path(ox, oy, kx, ky, 0.6);
                    write!(
                        svg,
                        "<path d=\"{path}\" fill=\"none\" stroke=\"{fill}\" stroke-width=\"1.4\" stroke-opacity=\"0.7\"/>"
                    )
                    .ok();
                }
            }
            // Use a more distinctive shape for flagged (rect) than
            // normal (smaller, dimmer dot) so glance-scanning the
            // ring surfaces flagged contracts first.
            let size_px = if is_flagged { 6.0 } else { 3.0 };
            let opacity = if is_flagged { "1.0" } else { "0.55" };
            let glow_attr = if glow {
                "filter=\"drop-shadow(0 0 3px currentColor)\""
            } else {
                ""
            };
            write!(
                svg,
                "<rect x=\"{x:.1}\" y=\"{y:.1}\" width=\"{size_px:.1}\" height=\"{size_px:.1}\" fill=\"{fill}\" fill-opacity=\"{opacity}\" {glow_attr}><title>{title}</title></rect>",
                x = kx - size_px / 2.0,
                y = ky - size_px / 2.0,
                title = html_escape(&format!("{} ({:?})", c.instance_id_short, c.state)),
            )
            .ok();
            // Short label next to flagged contracts so the ring view
            // matches the table without clicking.
            if is_flagged {
                let label_loc_r = r_inner - 12.0;
                let (lx, ly) = loc_to_xy(loc, label_loc_r);
                write!(
                    svg,
                    "<text x=\"{lx:.1}\" y=\"{ly:.1}\" text-anchor=\"middle\" dominant-baseline=\"middle\" fill=\"{fill}\" font-family=\"monospace\" font-size=\"9\" font-weight=\"500\">{label}</text>",
                    label = html_escape(&c.instance_id_short),
                )
                .ok();
            }
        }
    }

    // === Peer dots on the outer ring ===
    for p in peers {
        if let Some(loc) = p.location {
            let (px, py) = loc_to_xy(loc, r_outer);
            let fill = if p.is_gateway { "#ffb610" } else { "#66d9ff" };
            let kind = if p.is_gateway { "Gateway" } else { "Peer" };
            let addr = p.address.to_string();
            let title = format!("{kind} {addr} (loc {loc:.4})");
            write!(
                svg,
                "<a href=\"/peer/{href}\" class=\"ring-peer-link\"><title>{title}</title><circle cx=\"{px:.1}\" cy=\"{py:.1}\" r=\"4\" fill=\"{fill}\"/></a>",
                href = html_escape(&addr),
                title = html_escape(&title),
            )
            .ok();
        }
    }

    // === YOU marker — drawn last so it sits above everything ===
    if let Some(own_loc) = own_location {
        let (ox, oy) = loc_to_xy(own_loc, r_outer);
        write!(
            svg,
            "<g class=\"ring-self\"><title>You (loc {own_loc:.4})</title><circle cx=\"{ox:.1}\" cy=\"{oy:.1}\" r=\"6\" fill=\"#43c178\" stroke=\"#ebecf0\" stroke-width=\"1.5\" class=\"you-dot\"/><text x=\"{lx:.1}\" y=\"{ly:.1}\" text-anchor=\"middle\" fill=\"#43c178\" font-family=\"monospace\" font-size=\"9\" font-weight=\"500\" letter-spacing=\"0.05em\">YOU</text></g>",
            lx = ox,
            ly = oy + 18.0,
        )
        .ok();
    }

    svg.push_str("</svg>");

    // Legend below the ring.
    svg.push_str(concat!(
        "<div class=\"ring-legend\">",
        "<span class=\"ring-key\"><span class=\"ring-dot ring-dot-self\"></span> You</span>",
        "<span class=\"ring-key\"><span class=\"ring-dot ring-dot-peer\"></span> Peer</span>",
        "<span class=\"ring-key\"><span class=\"ring-dot ring-dot-gw\"></span> Gateway</span>",
        "<span class=\"ring-key\"><span class=\"ring-dot ring-dot-contract-normal\"></span> Hosted</span>",
        "<span class=\"ring-key\"><span class=\"ring-dot ring-dot-contract-flagged\"></span> Flagged</span>",
        "</div></div>",
    ));

    svg
}

/// Build the governance card. Reads `snap.governance` (sourced from
/// `Ring::dashboard_governance_snapshot` → `GovernanceManager`). Every
/// field rendered here came from the back-end's computation —
/// nothing is invented at render time.
///
/// Layout follows the prototype: verdict block on the left (big
/// number + headline), mini-strip with network-norms on the right,
/// then the per-contract table below. The histogram and ring inner-
/// ring renderings are deliberately separate commits — this commit
/// proves the data path end-to-end with the simplest visualisations.
fn build_governance_card(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
    let Some(snap) = snap else {
        return String::new();
    };
    let g = &snap.governance;

    let mode_txt = match g.mode {
        network_status::GovernanceModeSnapshot::Off => "off",
        network_status::GovernanceModeSnapshot::DryRun => "dry-run",
        network_status::GovernanceModeSnapshot::Enforce => "enforce",
    };

    let last_tick_footer = match g.last_tick_at {
        Some(at) => {
            // Compute "Ns ago" using TOKIO instant arithmetic via
            // a wall-clock comparison. The snapshot was built
            // moments ago so we can approximate "now" inline.
            let now = tokio::time::Instant::now();
            let secs = now.saturating_duration_since(at).as_secs();
            format!("Last evaluated {} ago", format_ago(secs))
        }
        None => "Reaper has not yet ticked".to_string(),
    };

    // Empty state: no FLAGGED contracts. Render the structural
    // skeleton (mode pill, 5-tile mini-strip with em-dashes if data
    // is unavailable, observed/required progress) rather than just a
    // paragraph — teaches operators what data will appear and what
    // mode is active. The previous empty state hid this and made
    // an operator think the dashboard was half-implemented.
    if g.contracts.is_empty() {
        let observed = g.observed_count;
        let needed = g.min_samples;
        // Tiny pluralization helper so the user-facing messages
        // don't read "1 contracts" — Codex review nit.
        let plural = |n: usize| if n == 1 { "contract" } else { "contracts" };
        let progress_msg = if needed == 0 {
            "Governance manager is not yet wired.".to_string()
        } else if observed == 0 {
            format!(
                "No contracts observed yet. Scoring activates after {needed} {n_word} \
                 have accumulated cost.",
                n_word = plural(needed),
            )
        } else if observed < needed {
            let remaining = needed - observed;
            let verb = if remaining == 1 {
                "accumulates"
            } else {
                "accumulate"
            };
            format!(
                "Observed {observed} / {needed} {n_word} needed for statistical scoring. \
                 Scoring activates once {remaining} more {r_word} {verb} cost.",
                n_word = plural(needed),
                r_word = plural(remaining),
            )
        } else {
            // Enough samples observed but none flagged — that's the
            // healthy steady state.
            format!(
                "All {observed} tracked {n_word} within normal range. \
                 (Scored against the network's own observed distribution.)",
                n_word = plural(observed),
            )
        };
        let verdict_main = if observed >= needed {
            format!(
                r#"<div class="verdict-num">✓</div>
                   <div class="verdict-headline">{observed} contracts within normal range</div>
                   <div class="verdict-detail">No flags raised.</div>"#
            )
        } else {
            format!(
                r#"<div class="verdict-num">{observed}<span class="verdict-num-denom">/{needed}</span></div>
                   <div class="verdict-headline">contracts observed</div>
                   <div class="verdict-detail">Scoring activates at {needed}.</div>"#
            )
        };

        // 5-tile skeleton — render even with no data, using em-dashes
        // for missing values. This shows operators what fields will
        // populate as the reaper ticks.
        let median_txt = g
            .norms
            .median_log_ratio
            .map(|v| format!("{:.2}", v))
            .unwrap_or_else(|| "—".to_string());
        let mad_txt = g
            .norms
            .mad
            .map(|v| format!("{:.2}", v))
            .unwrap_or_else(|| "—".to_string());
        let threshold_txt = g
            .norms
            .threshold
            .map(|v| format!("{:.2}", v))
            .unwrap_or_else(|| "—".to_string());
        let sample_size_txt = if g.norms.sample_size == 0 {
            "—".to_string()
        } else {
            g.norms.sample_size.to_string()
        };

        return format!(
            r##"<div class="card">
                <div class="card-header"><h2>Contract Governance</h2><span class="g-mode g-mode-{mode}">{mode}</span></div>
                <div class="g-verdict-row">
                    <div class="g-verdict verdict-ok">{verdict_main}</div>
                    <div class="g-norms">
                        <div class="g-norm"><div class="g-norm-label">Tracked</div><div class="g-norm-value">{observed}</div></div>
                        <div class="g-norm"><div class="g-norm-label">Sample size</div><div class="g-norm-value">{sample_size}</div></div>
                        <div class="g-norm"><div class="g-norm-label">Median log-ratio</div><div class="g-norm-value">{median}</div></div>
                        <div class="g-norm"><div class="g-norm-label">MAD spread</div><div class="g-norm-value">{mad}</div></div>
                        <div class="g-norm"><div class="g-norm-label">Eviction threshold</div><div class="g-norm-value">{threshold}</div></div>
                    </div>
                </div>
                <p class="empty" style="margin: 0.6rem 0.9rem 0.2rem; font-size: 0.9rem;">{progress}</p>
                <p class="empty" style="margin: 0 0.9rem 0.6rem; font-size: 0.78rem; color: var(--text-muted, #888);">{tick_footer}</p>
            </div>"##,
            mode = mode_txt,
            verdict_main = verdict_main,
            observed = observed,
            sample_size = sample_size_txt,
            median = median_txt,
            mad = mad_txt,
            threshold = threshold_txt,
            progress = progress_msg,
            tick_footer = last_tick_footer,
        );
    }

    // Verdict counts. Mirror state-snapshot enum → string.
    let mut counts = [0u32; 5];
    for c in &g.contracts {
        let idx = match c.state {
            network_status::GovernanceStateSnapshot::Normal => 0,
            network_status::GovernanceStateSnapshot::Borderline => 1,
            network_status::GovernanceStateSnapshot::WouldEvict => 2,
            network_status::GovernanceStateSnapshot::Evicted => 3,
            network_status::GovernanceStateSnapshot::Banned => 4,
        };
        counts[idx] = counts[idx].saturating_add(1);
    }
    let borderline = counts[1];
    let would_evict = counts[2];
    let evicted = counts[3];
    let banned = counts[4];
    let flagged = borderline + would_evict + evicted + banned;
    let total = g.contracts.len();

    let verdict_class = if flagged == 0 {
        "verdict-ok"
    } else {
        "verdict-alert"
    };
    let verdict_main = if flagged == 0 {
        format!(
            r#"<div class="verdict-num">✓</div>
               <div class="verdict-headline">All {total} contracts within normal range</div>"#,
        )
    } else {
        let mut detail_parts: Vec<String> = Vec::new();
        if would_evict > 0 {
            detail_parts.push(format!(
                r#"<span class="sw sw-wouldevict"></span>{would_evict} would be evicted"#
            ));
        }
        if borderline > 0 {
            detail_parts.push(format!(
                r#"<span class="sw sw-borderline"></span>{borderline} borderline"#
            ));
        }
        if evicted > 0 {
            detail_parts.push(format!(
                r#"<span class="sw sw-evicted"></span>{evicted} evicted"#
            ));
        }
        if banned > 0 {
            detail_parts.push(format!(
                r#"<span class="sw sw-banned"></span>{banned} banned"#
            ));
        }
        format!(
            r#"<div class="verdict-num">{flagged}</div>
               <div class="verdict-headline">contracts flagged on this node</div>
               <div class="verdict-detail">{detail}</div>"#,
            detail = detail_parts.join(" &nbsp;·&nbsp; "),
        )
    };

    // Network-norms mini-strip — sourced from the last reaper-tick
    // result. Empty if no tick has run yet (cold start; sample size
    // didn't reach min_samples; MAD collapsed).
    let median_txt = g
        .norms
        .median_log_ratio
        .map(|v| format!("{:.2}", v))
        .unwrap_or_else(|| "—".to_string());
    let mad_txt = g
        .norms
        .mad
        .map(|v| format!("{:.2}", v))
        .unwrap_or_else(|| "—".to_string());
    let threshold_txt = g
        .norms
        .threshold
        .map(|v| format!("{:.2}", v))
        .unwrap_or_else(|| "—".to_string());
    let sample_size_txt = g.norms.sample_size.to_string();
    // `mode_txt` and `last_tick_footer` are in scope from the top
    // of this function (defined once, reused in both empty and
    // populated branches).

    // Per-contract table — flagged-only by default; an "all" link
    // could come later. Honest principle: this table reflects the
    // governance manager's `iter_scores()`, nothing else.
    let mut rows = String::new();
    let mut shown_count = 0;
    // Sort: most-flagged first (Banned > Evicted > WouldEvict >
    // Borderline > Normal), then by highest log-ratio descending so
    // the worst offenders sit at the top.
    let mut sorted: Vec<&network_status::ContractGovernanceEntry> = g.contracts.iter().collect();
    sorted.sort_by(|a, b| {
        let rank = |s: network_status::GovernanceStateSnapshot| match s {
            network_status::GovernanceStateSnapshot::Banned => 0,
            network_status::GovernanceStateSnapshot::Evicted => 1,
            network_status::GovernanceStateSnapshot::WouldEvict => 2,
            network_status::GovernanceStateSnapshot::Borderline => 3,
            network_status::GovernanceStateSnapshot::Normal => 4,
        };
        rank(a.state).cmp(&rank(b.state)).then_with(|| {
            b.log_ratio
                .partial_cmp(&a.log_ratio)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    });
    for c in sorted.iter() {
        // Default: only show flagged contracts. An "all" toggle could
        // come later; for now keeping the table digestible.
        if matches!(c.state, network_status::GovernanceStateSnapshot::Normal) {
            continue;
        }
        shown_count += 1;
        let state_label = match c.state {
            network_status::GovernanceStateSnapshot::Normal => "normal",
            network_status::GovernanceStateSnapshot::Borderline => "borderline",
            network_status::GovernanceStateSnapshot::WouldEvict => "would evict",
            network_status::GovernanceStateSnapshot::Evicted => "evicted",
            network_status::GovernanceStateSnapshot::Banned => "banned",
        };
        let state_class = match c.state {
            network_status::GovernanceStateSnapshot::Normal => "g-normal",
            network_status::GovernanceStateSnapshot::Borderline => "g-borderline",
            network_status::GovernanceStateSnapshot::WouldEvict => "g-wouldevict",
            network_status::GovernanceStateSnapshot::Evicted => "g-evicted",
            network_status::GovernanceStateSnapshot::Banned => "g-banned",
        };
        let log_ratio_txt = c
            .log_ratio
            .map(|v| format!("{:+.2}", v))
            .unwrap_or_else(|| "—".to_string());
        let age = format_ago(c.age_secs);
        let state_rank = match c.state {
            network_status::GovernanceStateSnapshot::Banned => 0u8,
            network_status::GovernanceStateSnapshot::Evicted => 1,
            network_status::GovernanceStateSnapshot::WouldEvict => 2,
            network_status::GovernanceStateSnapshot::Borderline => 3,
            network_status::GovernanceStateSnapshot::Normal => 4,
        };
        let log_ratio_sort = c.log_ratio.map(|v| format!("{v:.6}")).unwrap_or_default();
        rows.push_str(&format!(
            r#"<tr><td title="{full}" data-sort="{full}"><code>{short}</code><button type="button" class="copy-key" data-copy="{full}" title="Copy contract key" aria-label="Copy contract key">⧉</button></td><td data-sort="{state_rank}"><span class="g-badge {state_class}">{state_label}</span></td><td class="right" data-sort="{log_ratio_sort}">{log_ratio}</td><td class="right" data-sort="{cost:.6}">{cost:.2}</td><td class="right" data-sort="{benefit:.6}">{benefit:.2}</td><td class="right" data-sort="{age_secs}">{age}</td></tr>"#,
            full = html_escape(&c.instance_id),
            short = html_escape(&c.instance_id_short),
            state_class = state_class,
            state_label = state_label,
            state_rank = state_rank,
            log_ratio = log_ratio_txt,
            log_ratio_sort = log_ratio_sort,
            cost = c.cost_used,
            benefit = c.benefit_score,
            age = age,
            age_secs = c.age_secs,
        ));
    }
    if shown_count == 0 {
        rows = r#"<tr class="sort-disabled"><td colspan="6" class="empty" style="padding: 0.5rem 0.9rem">All contracts within normal range.</td></tr>"#.to_string();
    }

    let tracked_total = g.observed_count.max(total);
    format!(
        r##"<div class="card">
            <div class="card-header"><h2>Contract Governance</h2><span class="g-mode g-mode-{mode}">{mode}</span></div>
            <div class="g-verdict-row">
                <div class="g-verdict {verdict_class}">{verdict_main}</div>
                <div class="g-norms">
                    <div class="g-norm"><div class="g-norm-label">Tracked</div><div class="g-norm-value">{tracked_total}</div></div>
                    <div class="g-norm"><div class="g-norm-label">Sample size</div><div class="g-norm-value">{sample_size}</div></div>
                    <div class="g-norm"><div class="g-norm-label">Median log-ratio</div><div class="g-norm-value">{median}</div></div>
                    <div class="g-norm"><div class="g-norm-label">MAD spread</div><div class="g-norm-value">{mad}</div></div>
                    <div class="g-norm"><div class="g-norm-label">Eviction threshold</div><div class="g-norm-value">{threshold}</div></div>
                </div>
            </div>
            <div class="table-wrap">
                <table class="sortable" data-table-id="governance">
                    <thead><tr><th data-sort-type="text">Contract</th><th data-sort-type="num">State</th><th class="right" data-sort-type="num">log-ratio</th><th class="right" data-sort-type="num">Cost</th><th class="right" data-sort-type="num">Benefit</th><th class="right" data-sort-type="num">Age</th></tr></thead>
                    <tbody>{rows}</tbody>
                </table>
            </div>
            <p class="empty" style="margin: 0.4rem 0.9rem 0.6rem; font-size: 0.78rem; color: var(--text-muted, #888);">{tick_footer}</p>
        </div>"##,
        mode = mode_txt,
        verdict_class = verdict_class,
        verdict_main = verdict_main,
        tracked_total = tracked_total,
        tick_footer = last_tick_footer,
        sample_size = sample_size_txt,
        median = median_txt,
        mad = mad_txt,
        threshold = threshold_txt,
        rows = rows,
    )
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

    let state_by_id = &snap.governance.state_by_id;
    let mut rows = String::new();
    for c in &snap.contracts {
        let last_update = c
            .last_updated_secs
            .map(format_ago)
            .unwrap_or_else(|| "—".to_string());
        let last_update_sort = c.last_updated_secs.unwrap_or(u64::MAX);
        // Governance state cell. A contract may appear in the
        // Subscribed Contracts table without being flagged by
        // governance — in that case the state isn't in `state_by_id`
        // (the snapshot only carries flagged contracts) and we
        // render it as "ok". Absence from the table specifically
        // means "not flagged"; we trust the back-end's `iter_flagged_
        // scores` filter.
        let (gov_class, gov_label, gov_sort) = match state_by_id.get(&c.instance_id) {
            None => ("gov-ok", "ok", 0u8),
            Some(network_status::GovernanceStateSnapshot::Normal) => ("gov-ok", "ok", 0),
            Some(network_status::GovernanceStateSnapshot::Borderline) => {
                ("gov-borderline", "borderline", 1)
            }
            Some(network_status::GovernanceStateSnapshot::WouldEvict) => {
                ("gov-wouldevict", "would evict", 2)
            }
            Some(network_status::GovernanceStateSnapshot::Evicted) => ("gov-evicted", "evicted", 3),
            Some(network_status::GovernanceStateSnapshot::Banned) => ("gov-banned", "banned", 4),
        };
        rows.push_str(&format!(
            r#"<tr><td title="{full}" data-sort="{full}"><code>{short}</code><button type="button" class="copy-key" data-copy="{full}" title="Copy contract key" aria-label="Copy contract key">⧉</button></td><td data-sort="{gov_sort}"><span class="gov-pill {gov_class}">{gov_label}</span></td><td data-sort="{sub_secs}">{subscribed}</td><td data-sort="{last_sort}">{last_update}</td></tr>"#,
            full = html_escape(&c.key_full),
            short = html_escape(&c.key_short),
            gov_sort = gov_sort,
            gov_class = gov_class,
            gov_label = gov_label,
            sub_secs = c.subscribed_secs,
            subscribed = format_ago(c.subscribed_secs),
            last_sort = last_update_sort,
            last_update = last_update,
        ));
    }

    format!(
        r#"<div class="card">
            <h2>Subscribed Contracts</h2>
            <div class="table-wrap">
                <table class="sortable" data-table-id="contracts">
                    <thead><tr><th data-sort-type="text">Contract</th><th data-sort-type="num">Gov</th><th data-sort-type="num">Subscribed</th><th data-sort-type="num">Last Update</th></tr></thead>
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
            let total_ops = ops.subscribes.0.saturating_add(ops.subscribes.1);
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
/* ── CSS Variables (dark mode default) ── */
:root {
    --bg-primary: #0c0d0f;
    --bg-secondary: #141518;
    --bg-tertiary: #1a1c20;
    --bg-panel: rgba(20, 21, 24, 0.85);
    --border-color: rgba(64, 66, 72, 0.35);
    --text-primary: #edeeef;
    --text-secondary: #94969a;
    --text-muted: #585a5e;
    --accent-primary: #0abab5;
    --accent-light: #5eead4;
    --accent-dark: #0d9488;
    --font-mono: 'Iosevka', 'JetBrains Mono', 'Cascadia Code', 'Fira Code', 'SF Mono', monospace;
    --font-sans: 'Space Grotesk', 'DM Sans', system-ui, -apple-system, sans-serif;
}
/* Light mode overrides */
[data-theme="light"] {
    --bg-primary: #f7f5f2;
    --bg-secondary: #eeebe6;
    --bg-tertiary: #e5e0d9;
    --bg-panel: rgba(255, 255, 255, 0.88);
    --border-color: rgba(180, 175, 168, 0.4);
    --text-primary: #1a1816;
    --text-secondary: #5c5955;
    --text-muted: #8c8985;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: var(--font-sans);
    background: var(--bg-primary);
    color: var(--text-primary);
    line-height: 1.5;
}
/* Subtle warm background with dot-grid texture */
body::before {
    content: '';
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background:
        radial-gradient(ellipse at 30% 20%, rgba(224, 220, 210, 0.03) 0%, transparent 55%),
        radial-gradient(ellipse at 70% 60%, rgba(10, 186, 181, 0.04) 0%, transparent 50%);
    pointer-events: none;
    z-index: 0;
}
body::after {
    content: '';
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background-image: radial-gradient(rgba(255,255,255,0.015) 1px, transparent 1px);
    background-size: 24px 24px;
    pointer-events: none;
    z-index: 0;
    opacity: 0.6;
}
[data-theme="light"] body::before {
    background:
        radial-gradient(ellipse at 30% 20%, rgba(180, 175, 165, 0.06) 0%, transparent 55%),
        radial-gradient(ellipse at 70% 60%, rgba(10, 186, 181, 0.04) 0%, transparent 50%);
}
[data-theme="light"] body::after {
    background-image: radial-gradient(rgba(0,0,0,0.025) 1px, transparent 1px);
    opacity: 0.5;
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
    background: rgba(94, 234, 212, 0.1);
    padding: 0.15rem 0.5rem;
    border-radius: 4px;
    border: 1px solid rgba(94, 234, 212, 0.2);
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
.pub-key-label {
    font-size: 0.65rem;
    color: var(--text-muted);
    text-transform: uppercase;
    font-family: var(--font-mono);
    margin-right: 0.3rem;
}
.pub-key {
    font-family: var(--font-mono);
    font-size: 0.7rem;
    color: var(--text-secondary);
    background: rgba(139, 148, 158, 0.08);
    padding: 0.15rem 0.5rem;
    border-radius: 4px 0 0 4px;
    border: 1px solid var(--border-color);
    max-width: 24ch;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    cursor: pointer;
}
.pub-key:hover { color: var(--accent-light); }
.copy-btn {
    font-family: var(--font-mono);
    font-size: 0.7rem;
    color: var(--text-secondary);
    background: rgba(139, 148, 158, 0.08);
    border: 1px solid var(--border-color);
    border-left: none;
    border-radius: 0 4px 4px 0;
    padding: 0.15rem 0.4rem;
    cursor: pointer;
    line-height: 1;
}
.copy-btn:hover { color: var(--accent-light); background: rgba(94, 234, 212, 0.1); }
.badge {
    background: rgba(10, 186, 181, 0.15);
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
    border-radius: 12px;
    padding: 1rem 1.25rem;
    backdrop-filter: blur(10px);
    box-shadow: 0 1px 3px rgba(0,0,0,0.12);
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
@keyframes peer-fade-in {
    from { opacity: 0; transform: translateY(-6px); }
    to   { opacity: 1; transform: translateY(0); }
}
@keyframes you-pulse {
    0%, 100% { r: 6; }
    50%      { r: 8.5; }
}
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
    background: rgba(10, 186, 181, 0.08);
    border: 1px solid rgba(10, 186, 181, 0.2);
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
/* ── Ring stats metric row ── */
.metrics-row {
    display: flex;
    gap: 0.5rem;
    margin: 0.75rem 0;
}
.metric-tile {
    flex: 1;
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 6px;
    padding: 0.5rem;
    text-align: center;
}
.metric-value {
    display: block;
    font-size: 1.3rem;
    font-weight: 700;
    font-family: var(--font-mono);
    color: var(--accent-light);
}
[data-theme="light"] .metric-value { color: var(--accent-primary); }
.metric-label {
    display: block;
    font-size: 0.65rem;
    color: var(--text-secondary);
    text-transform: uppercase;
    font-family: var(--font-mono);
    margin-top: 0.2rem;
}
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
.transfer-detail {
    font-size: 0.75rem;
    font-weight: 400;
    color: var(--text-secondary);
    margin-left: 0.4rem;
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
.ring-dot-self { background: #43c178; }
.ring-dot-peer { background: #66d9ff; }
.ring-dot-gw { background: #ffb610; }
.ring-dot-contract-normal { background: #43c178; opacity: 0.55; }
.ring-dot-contract-flagged { background: #ff667a; }
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

/* ── Mobile responsive ── */
@media (max-width: 768px) {
    main { margin: 0.75rem auto; padding: 0 0.5rem; }
    header {
        flex-wrap: wrap; gap: 0.4rem; padding: 0.5rem 0.75rem;
    }
    .header-left { flex-wrap: wrap; gap: 0.3rem; }
    .header-right { margin-left: auto; }
    .pub-key-label { display: none; }
    .pub-key { max-width: 10ch; font-size: 0.65rem; }
    .copy-btn { font-size: 0.65rem; padding: 0.15rem 0.3rem; }
    .uptime { font-size: 0.7rem; }
    .badge, .update-badge { font-size: 0.65rem; }

    .card { padding: 0.75rem 1rem; border-radius: 8px; }

    .metrics-row { flex-wrap: wrap; gap: 0.35rem; }
    .metric-tile { flex: 1 1 30%; min-width: 5rem; padding: 0.4rem; }
    .metric-value { font-size: 1.1rem; }
    .metric-label { font-size: 0.6rem; }

    .peer-table { font-size: 0.72rem; }
    .peer-table th, .peer-table td { padding: 0.3rem 0.4rem; }
    .peer-table th:nth-child(4), .peer-table td:nth-child(4),
    .peer-table th:nth-child(5), .peer-table td:nth-child(5) { display: none; }

    .transfer-stat { flex-direction: column; align-items: flex-start; gap: 0.2rem; }
    .transfer-value { font-size: 0.95rem; }

    .op-grid { grid-template-columns: repeat(2, 1fr); gap: 0.35rem; }
    .op-card { padding: 0.5rem; }
    .op-label { font-size: 0.6rem; }
    .op-value { font-size: 0.9rem; }

    .ring-wrap { margin-bottom: 0.5rem; }
    .ring-svg { width: 160px; height: 160px; }

    /* Governance: stack verdict + norms vertically, let table scroll */
    .g-verdict-row { grid-template-columns: 1fr; }
    .g-verdict { min-width: auto; }
    .g-norms { grid-template-columns: repeat(3, 1fr); }
    .app-list li { padding: 0.4rem 0; }
}
@media (max-width: 400px) {
    .g-norms { grid-template-columns: repeat(2, 1fr); }
    .header-title { font-size: 0.9rem; }
    .header-scope { font-size: 0.65rem; padding: 0.1rem 0.35rem; }
    .metric-tile { flex: 1 1 45%; }
    .peer-table { font-size: 0.68rem; }
    .op-grid { grid-template-columns: 1fr 1fr; }
    .metrics-row { gap: 0.25rem; }
}
/* ── Ring SVG peer links ── */
.ring-svg a.ring-peer-link { cursor: pointer; }
.ring-svg a.ring-peer-link circle {
    transition: r 0.12s ease, stroke-width 0.12s ease;
}
.ring-svg a.ring-peer-link:hover circle,
.ring-svg a.ring-peer-link:focus circle {
    r: 7;
    stroke: var(--text-primary);
    stroke-width: 1.5;
}
.ring-svg a.ring-peer-link:focus { outline: none; }

/* "You" marker: subtle pulse */
.ring-svg .you-dot {
    animation: you-pulse 3s ease-in-out infinite;
    transform-origin: center;
}

/* Peer table: fade-in rows */
.peer-table tbody tr {
    animation: peer-fade-in 0.35s ease-out both;
}
.peer-table tbody tr:nth-child(1) { animation-delay: 0.02s; }
.peer-table tbody tr:nth-child(2) { animation-delay: 0.06s; }
.peer-table tbody tr:nth-child(3) { animation-delay: 0.10s; }
.peer-table tbody tr:nth-child(4) { animation-delay: 0.14s; }
.peer-table tbody tr:nth-child(5) { animation-delay: 0.18s; }
.peer-table tbody tr:nth-child(n+6) { animation-delay: 0.22s; }

.copy-btn-inline {
    font-family: var(--font-mono);
    font-size: 0.65rem;
    color: var(--text-muted);
    background: none;
    border: 1px solid var(--border-color);
    border-radius: 3px;
    padding: 0 0.35rem;
    cursor: pointer;
    margin-left: 0.4rem;
    vertical-align: middle;
}
.copy-btn-inline:hover { color: var(--accent-light); border-color: var(--accent-light); }
/* ── Sortable tables ── */
table.sortable thead th {
    cursor: pointer;
    user-select: none;
    position: relative;
    padding-right: 1.1rem;
}
table.sortable thead th:hover { color: var(--text-primary); }
table.sortable thead th::after {
    content: "";
    position: absolute;
    right: 0.4rem;
    top: 50%;
    transform: translateY(-50%);
    opacity: 0.35;
    font-size: 0.7rem;
}
table.sortable thead th.sort-asc::after { content: "▲"; opacity: 1; color: var(--accent-light); }
table.sortable thead th.sort-desc::after { content: "▼"; opacity: 1; color: var(--accent-light); }
/* ── Copy-to-clipboard contract button ── */
.copy-key {
    background: none;
    border: none;
    padding: 0 0.25rem;
    margin-left: 0.35rem;
    cursor: copy;
    font: inherit;
    font-size: 0.85em;
    color: var(--text-muted);
    transition: color 0.15s;
}
.copy-key:hover { color: var(--accent-light); }
.copy-key:focus { outline: 1px dashed var(--accent-light); outline-offset: 2px; }
.copy-key.copied { color: #34d399; }
[data-theme="light"] .copy-key.copied { color: #059669; }
/* ── Update-available badge in header ── */
.update-badge {
    background: rgba(52, 211, 153, 0.15);
    color: #34d399;
    border: 1px solid rgba(52, 211, 153, 0.35);
    padding: 0.15rem 0.5rem;
    border-radius: 12px;
    font-size: 0.75rem;
    font-weight: 500;
    font-family: var(--font-mono);
    text-decoration: none;
    margin-left: 0.25rem;
}
.update-badge:hover { background: rgba(52, 211, 153, 0.25); text-decoration: none; }
[data-theme="light"] .update-badge { color: #059669; }
.update-badge[hidden] { display: none; }
/* ── Toast (clipboard feedback) ── */
.toast-container {
    position: fixed;
    bottom: 1rem;
    left: 50%;
    transform: translateX(-50%);
    z-index: 100;
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    pointer-events: none;
}
.toast {
    background: var(--bg-panel);
    border: 1px solid var(--border-color);
    color: var(--text-primary);
    padding: 0.5rem 0.9rem;
    border-radius: 6px;
    font-size: 0.85rem;
    font-family: var(--font-mono);
    backdrop-filter: blur(10px);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.25);
    animation: toast-in 0.18s ease-out;
}
.toast.toast-error { color: #f87171; border-color: rgba(248, 113, 113, 0.4); }
@keyframes toast-in {
    from { opacity: 0; transform: translateY(8px); }
    to   { opacity: 1; transform: translateY(0); }
}

/* ── Governance card (Phase 4.5) ───────────────────────────────────── */
.g-mode {
    font-family: var(--font-mono);
    font-size: 0.7rem;
    padding: 0.15rem 0.5rem;
    border-radius: 3px;
    letter-spacing: 0.03em;
    text-transform: uppercase;
    margin-left: auto;
}
.g-mode-dry-run {
    background: rgba(255, 182, 16, 0.15);
    color: #ffb610;
    border: 1px solid rgba(255, 182, 16, 0.35);
}
.g-mode-enforce {
    background: rgba(255, 102, 122, 0.15);
    color: #ff667a;
    border: 1px solid rgba(255, 102, 122, 0.35);
}
.g-mode-off {
    background: rgba(148, 163, 184, 0.15);
    color: var(--text-secondary);
    border: 1px solid rgba(148, 163, 184, 0.35);
}

.g-verdict-row {
    display: grid;
    grid-template-columns: minmax(280px, 0.85fr) 2fr;
    gap: 0.75rem;
    margin: 0.5rem 0 0.75rem;
}
.g-verdict {
    background: var(--bg-tertiary);
    border: 1px solid var(--border-color);
    border-left: 3px solid #ff8a3d;
    border-radius: 6px;
    padding: 0.85rem 1rem 0.85rem 1.1rem;
    display: grid;
    grid-template-columns: auto 1fr;
    grid-template-rows: auto auto;
    column-gap: 1rem;
    align-items: center;
}
.verdict-ok { border-left-color: #43c178; }
.verdict-alert { border-left-color: #ff8a3d; }
.g-verdict .verdict-num {
    grid-row: 1 / 3;
    font-family: var(--font-mono);
    font-size: 3.2rem;
    font-weight: 500;
    line-height: 0.95;
    color: #ff8a3d;
    letter-spacing: -0.04em;
}
.verdict-ok .verdict-num { color: #43c178; }
.g-verdict .verdict-headline {
    font-size: 0.95rem;
    font-weight: 500;
    color: var(--text-primary);
}
.g-verdict .verdict-detail {
    font-family: var(--font-mono);
    font-size: 0.78rem;
    color: var(--text-secondary);
    margin-top: 0.15rem;
}
.g-verdict .sw {
    display: inline-block;
    width: 6px;
    height: 6px;
    border-radius: 50%;
    margin-right: 0.3rem;
    vertical-align: 0.07rem;
}
.sw-borderline { background: #ffb610; }
.sw-wouldevict { background: #ff8a3d; }
.sw-evicted    { background: #ff667a; }
.sw-banned     { background: #d33682; }

.g-norms {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 0.5rem;
}
.g-norm {
    background: var(--bg-tertiary);
    border: 1px solid var(--border-color);
    border-radius: 5px;
    padding: 0.55rem 0.75rem;
}
.g-norm-label {
    font-family: var(--font-mono);
    font-size: 0.66rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-muted);
}
.g-norm-value {
    font-family: var(--font-mono);
    font-size: 1rem;
    font-weight: 500;
    margin-top: 0.1rem;
    color: var(--text-primary);
}

.g-badge {
    display: inline-block;
    font-family: var(--font-mono);
    font-size: 0.7rem;
    padding: 0.1rem 0.45rem;
    border-radius: 3px;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    font-weight: 500;
}
.g-normal     { background: rgba(67,193,120,0.16);  color: #43c178; }
.g-borderline { background: rgba(255,182,16,0.16);  color: #ffb610; }
.g-wouldevict { background: rgba(255,138,61,0.18);  color: #ff8a3d; }
.g-evicted    { background: rgba(255,102,122,0.18); color: #ff667a; }
.g-banned     { background: rgba(211,54,130,0.18);  color: #d33682; }

/* Compact governance-state pill rendered inline in the Subscribed
 * Contracts table's Gov column. Smaller than the `.g-badge` used in
 * the main Contract Governance table so it doesn't dominate the
 * row, while still being a single visual cue scanning down the
 * column. */
.gov-pill {
    display: inline-block;
    padding: 0.1rem 0.45rem;
    font-size: 0.72rem;
    border-radius: 3px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    font-weight: 500;
    line-height: 1.2;
}
.gov-ok         { background: rgba(67,193,120,0.12);  color: #6b8a76; }
.gov-borderline { background: rgba(255,182,16,0.18);  color: #ffb610; }
.gov-wouldevict { background: rgba(255,138,61,0.20);  color: #ff8a3d; }
.gov-evicted    { background: rgba(255,102,122,0.22); color: #ff667a; }
.gov-banned     { background: rgba(211,54,130,0.22);  color: #d33682; }

/* Denominator in the empty-state verdict ("3/30"). Smaller than the
 * numerator so the eye reads the count first and the threshold
 * second. */
.verdict-num-denom {
    font-size: 0.55em;
    color: var(--text-muted, #888);
    margin-left: 0.1em;
    vertical-align: middle;
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

/* ── Toast notifications ── */
function showToast(msg, opts) {
    var container = document.getElementById('toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toast-container';
        container.className = 'toast-container';
        document.body.appendChild(container);
    }
    var t = document.createElement('div');
    t.className = 'toast' + ((opts && opts.error) ? ' toast-error' : '');
    t.textContent = msg;
    container.appendChild(t);
    setTimeout(function() {
        t.style.transition = 'opacity 0.25s';
        t.style.opacity = '0';
        setTimeout(function() { if (t.parentNode) t.parentNode.removeChild(t); }, 260);
    }, (opts && opts.duration) || 1600);
}

/* ── Copy contract key to clipboard ── */
function copyToClipboard(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
        return navigator.clipboard.writeText(text);
    }
    /* Fallback for older browsers / non-secure contexts */
    return new Promise(function(resolve, reject) {
        try {
            var ta = document.createElement('textarea');
            ta.value = text;
            ta.style.position = 'fixed';
            ta.style.opacity = '0';
            document.body.appendChild(ta);
            ta.select();
            var ok = document.execCommand('copy');
            document.body.removeChild(ta);
            ok ? resolve() : reject(new Error('execCommand failed'));
        } catch (e) { reject(e); }
    });
}

/* ── Sortable tables ── */
function compareCells(a, b, type) {
    if (type === 'num') {
        var na = parseFloat(a);
        var nb = parseFloat(b);
        var aBad = isNaN(na), bBad = isNaN(nb);
        if (aBad && bBad) return 0;
        if (aBad) return 1;   /* missing values sort to bottom */
        if (bBad) return -1;
        return na - nb;
    }
    return a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' });
}

function applySort(table, colIndex, dir) {
    var tbody = table.querySelector('tbody');
    if (!tbody) return;
    var ths = table.querySelectorAll('thead th');
    var th = ths[colIndex];
    if (!th) return;
    var type = th.getAttribute('data-sort-type') || 'text';
    var rows = Array.prototype.slice.call(tbody.querySelectorAll('tr'));
    rows.sort(function(r1, r2) {
        var c1 = r1.children[colIndex];
        var c2 = r2.children[colIndex];
        var v1 = c1 ? (c1.getAttribute('data-sort') || c1.textContent) : '';
        var v2 = c2 ? (c2.getAttribute('data-sort') || c2.textContent) : '';
        var cmp = compareCells(v1, v2, type);
        return dir === 'desc' ? -cmp : cmp;
    });
    rows.forEach(function(r) { tbody.appendChild(r); });
    ths.forEach(function(h) { h.classList.remove('sort-asc', 'sort-desc'); });
    th.classList.add(dir === 'desc' ? 'sort-desc' : 'sort-asc');
}

function sortKey(table) {
    return 'sort:' + (table.getAttribute('data-table-id') || 'tbl');
}

function handleHeaderClick(th) {
    var table = th.closest('table.sortable');
    if (!table) return;
    var ths = Array.prototype.slice.call(table.querySelectorAll('thead th'));
    var idx = ths.indexOf(th);
    if (idx < 0) return;
    var current = th.classList.contains('sort-asc') ? 'asc'
                : th.classList.contains('sort-desc') ? 'desc'
                : null;
    var dir = current === 'asc' ? 'desc' : 'asc';
    applySort(table, idx, dir);
    try { sessionStorage.setItem(sortKey(table), idx + ':' + dir); } catch (e) {}
}

function restoreSort() {
    document.querySelectorAll('table.sortable').forEach(function(table) {
        try {
            var saved = sessionStorage.getItem(sortKey(table));
            if (!saved) return;
            var parts = saved.split(':');
            var idx = parseInt(parts[0], 10);
            var dir = parts[1] === 'desc' ? 'desc' : 'asc';
            if (!isNaN(idx)) applySort(table, idx, dir);
        } catch (e) {}
    });
}

/* ── Update-available check (GitHub releases, cached 12h) ── */
function compareSemver(a, b) {
    var pa = String(a).replace(/^v/, '').split(/[.\-+]/);
    var pb = String(b).replace(/^v/, '').split(/[.\-+]/);
    var n = Math.max(pa.length, pb.length);
    for (var i = 0; i < n; i++) {
        var na = parseInt(pa[i], 10);
        var nb = parseInt(pb[i], 10);
        if (isNaN(na) && isNaN(nb)) {
            var s = (pa[i] || '').localeCompare(pb[i] || '');
            if (s !== 0) return s;
            continue;
        }
        if (isNaN(na)) return -1;
        if (isNaN(nb)) return 1;
        if (na !== nb) return na - nb;
    }
    return 0;
}

function showUpdateBadge(latestTag) {
    var el = document.getElementById('update-badge');
    if (!el) return;
    el.textContent = 'Update: v' + String(latestTag).replace(/^v/, '');
    el.title = 'A newer Freenet release is available — click to view release notes';
    el.hidden = false;
}

function checkForUpdate() {
    var badge = document.getElementById('version-badge');
    if (!badge) return;
    var current = badge.getAttribute('data-version') || '';
    if (!current || current === '?') return;
    var TTL_MS = 12 * 60 * 60 * 1000;
    var now = Date.now();
    var cached = null;
    try {
        var raw = localStorage.getItem('freenet-update-check');
        if (raw) cached = JSON.parse(raw);
    } catch (e) {}
    if (cached && cached.tag && cached.checkedAt && (now - cached.checkedAt) < TTL_MS) {
        if (compareSemver(cached.tag, current) > 0) showUpdateBadge(cached.tag);
        return;
    }
    fetch('https://api.github.com/repos/freenet/freenet-core/releases/latest', {
        headers: { 'Accept': 'application/vnd.github+json' },
    }).then(function(r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
    }).then(function(data) {
        var tag = data && data.tag_name;
        if (!tag) return;
        try { localStorage.setItem('freenet-update-check', JSON.stringify({ tag: tag, checkedAt: now })); } catch (e) {}
        if (compareSemver(tag, current) > 0) showUpdateBadge(tag);
    }).catch(function(e) {
        /* Network blocked / GitHub rate-limited — silently skip */
        console.debug('Update check failed:', e);
    });
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
    restoreSort();
    checkForUpdate();

    /* Delegated click handler \u2014 survives <main> innerHTML swaps from auto-refresh,
       so we don't need to re-bind after each refresh. */
    document.addEventListener('click', function(ev) {
        var copy = ev.target.closest && ev.target.closest('.copy-key');
        if (copy) {
            ev.preventDefault();
            ev.stopPropagation();
            var text = copy.getAttribute('data-copy') || copy.textContent.trim();
            copyToClipboard(text).then(function() {
                showToast('Contract key copied');
                copy.classList.add('copied');
                setTimeout(function() { copy.classList.remove('copied'); }, 900);
            }).catch(function() {
                showToast('Copy failed', { error: true });
            });
            return;
        }
        var th = ev.target.closest && ev.target.closest('table.sortable thead th');
        if (th) { handleHeaderClick(th); return; }
    });

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
                var newBadge = doc.querySelector('#version-badge');
                var oldBadge = document.getElementById('version-badge');
                if (newBadge && oldBadge) {
                    oldBadge.textContent = newBadge.textContent;
                    var nv = newBadge.getAttribute('data-version');
                    if (nv) oldBadge.setAttribute('data-version', nv);
                }
                var newIcon = doc.querySelector('link[rel="icon"]');
                var oldIcon = document.querySelector('link[rel="icon"]');
                if (newIcon && oldIcon) oldIcon.setAttribute('href', newIcon.getAttribute('href'));
                /* Restore tab selection and table sort after content swap */
                restoreTab();
                restoreSort();
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
    let addr_enc = html_escape(&peer.address.to_string());
    let info_card = format!(
        r#"<div class="card">
            <h2>Peer Info</h2>
            <div><strong>{ptype}</strong> <code>{addr}</code><button type="button" class="copy-btn-inline" data-addr="{addr_enc}" onclick="copyToClipboard(this.getAttribute('data-addr')).then(function(){{showToast('Address copied')}})" title="Copy address">⎘</button></div>
            <div class="info-grid">
                <div class="info-label">Location</div><div class="info-value">{loc}</div>
                <div class="info-label">Connected</div><div class="info-value">{connected}</div>
                <div class="info-label">Sent</div><div class="info-value">{sent}</div>
                <div class="info-label">Received</div><div class="info-value">{recv}</div>
            </div>
        </div>"#,
        ptype = peer_type,
        addr = addr_enc,
        addr_enc = addr_enc,
        loc = loc_str,
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
                    r#"<div class="empty-chart">No {name} operations have routed through this peer yet. The chart will populate as the network sends or relays {name}s through this connection.</div>"#,
                    name = tab_name,
                )
                .ok();
            } else {
                // Always render all three prediction-component slots so the
                // user can see every dimension the router models. Each slot
                // either contains the rendered curve or a per-metric
                // "awaiting data" placeholder — empty slots are NOT hidden,
                // because hiding them silently rotted unobserved when the
                // migration stopped feeding the response-time
                // and transfer-rate estimators (the `Failure Probability`-only
                // dashboard regression that surfaced this code path).
                panel_content.push_str(&build_estimator_chart_or_placeholder(
                    "Failure Probability",
                    f_curve,
                    f_range,
                    if tab_name == "All" { fail_adj } else { None },
                    ploc,
                    "0.0",
                    "1.0",
                    "No success/failure observations have routed through this peer yet.",
                ));
                panel_content.push_str(&build_estimator_chart_or_placeholder(
                    "Response Time (s)",
                    rt_curve,
                    rt_range,
                    if tab_name == "All" { rt_adj } else { None },
                    ploc,
                    "0",
                    "auto",
                    "No timed responses have been observed from this peer yet.",
                ));
                panel_content.push_str(&build_estimator_chart_or_placeholder(
                    "Transfer Rate (B/s)",
                    xfer_curve,
                    xfer_range,
                    if tab_name == "All" { xfer_adj } else { None },
                    ploc,
                    "auto",
                    "0",
                    "No payload transfers have been observed from this peer yet.",
                ));
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
                        <div class="info-label">Failure probability</div><div class="info-value">{fp}</div>
                        <div class="info-label">Response time</div><div class="info-value">{rt}</div>
                        <div class="info-label">Expected total time</div><div class="info-value">{ett}</div>
                        <div class="info-label">Transfer speed</div><div class="info-value">{ts}</div>
                    </div>
                </div>"#,
                fp = fmt_prediction_prob(pred.failure_probability),
                rt = fmt_prediction_time(pred.time_to_response_start),
                ett = fmt_prediction_time(pred.expected_total_time),
                ts = fmt_prediction_speed(pred.transfer_speed_bps),
            )
        } else {
            r#"<div class="card"><h2>Prediction</h2><p class="empty">Not enough routing data yet to predict this peer's behavior. The card fills in as operations are routed through it.</p></div>"#.to_string()
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

/// Render the named estimator chart, or — when no data has been observed
/// yet — a titled placeholder. The placeholder keeps the slot visible so
/// users can always see every component of a routing prediction even when
/// some estimators have not yet received feedback. Hiding empty charts
/// masked the data-collection regression in the migration
/// that fed only `failure_estimator` and left `response_start_time` and
/// `transfer_rate` permanently empty.
#[allow(clippy::too_many_arguments)]
fn build_estimator_chart_or_placeholder(
    title: &str,
    curve_points: &[(f64, f64)],
    data_range: (f64, f64),
    peer_adjustment: Option<f64>,
    peer_location: Option<f64>,
    y_min_hint: &str,
    y_max_hint: &str,
    empty_message: &str,
) -> String {
    if curve_points.is_empty() {
        return format!(
            r#"<div class="chart-section"><h3>{title}</h3><div class="empty-chart">{msg}</div></div>"#,
            title = title,
            msg = empty_message,
        );
    }
    build_estimator_chart(
        title,
        curve_points,
        data_range,
        peer_adjustment,
        peer_location,
        y_min_hint,
        y_max_hint,
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
            r#"<div class="chart-section"><h3>{title}</h3><div class="empty-chart">No data yet. Populates as operations route through this peer.</div></div>"#,
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

/// Sentinel values (f64::MAX / 2.0 ~ 9e307) indicate insufficient transfer data.
/// Cap at ~31 years in seconds -- anything above is clearly not a real prediction.
const REASONABLE_TIME_LIMIT: f64 = 1.0e9;

fn fmt_prediction_time(v: f64) -> String {
    if v.is_finite() && (0.0..REASONABLE_TIME_LIMIT).contains(&v) {
        format!("{v:.3}s")
    } else {
        "N/A".to_string()
    }
}

fn fmt_prediction_speed(v: f64) -> String {
    if v.is_finite() && v > 0.0 {
        format!("{v:.0} B/s")
    } else {
        "N/A".to_string()
    }
}

fn fmt_prediction_prob(v: f64) -> String {
    if v.is_finite() && (0.0..=1.0).contains(&v) {
        format!("{v:.4}")
    } else {
        "N/A".to_string()
    }
}

/// Build an SVG strip chart showing predicted failure probability vs actual outcome.
/// X-axis: predicted probability [0, 1].
/// Y-axis: actual outcome (0 = success at bottom, 1 = failure at top).
/// Good calibration: successes cluster left, failures cluster right.
fn build_renegade_accuracy_chart(pairs: &[(f64, f64)]) -> String {
    use std::fmt::Write;

    // Filter out NaN/non-finite values before processing
    let valid_pairs: Vec<(f64, f64)> = pairs
        .iter()
        .copied()
        .filter(|(p, a)| p.is_finite() && a.is_finite())
        .collect();
    let total_count = valid_pairs.len();

    let successes: Vec<f64> = valid_pairs
        .iter()
        .filter(|(_, a)| *a < 0.5)
        .map(|(p, _)| p.clamp(0.0, 1.0))
        .collect();
    let failures: Vec<f64> = valid_pairs
        .iter()
        .filter(|(_, a)| *a >= 0.5)
        .map(|(p, _)| p.clamp(0.0, 1.0))
        .collect();

    let w = 400.0_f64;
    let h = 200.0_f64;
    let pad_l = 50.0;
    let pad_r = 20.0;
    let pad_t = 30.0;
    let pad_b = 30.0;
    let plot_w = w - pad_l - pad_r;
    let plot_h = h - pad_t - pad_b;
    let mid_y = pad_t + plot_h / 2.0; // dividing line between failure (top) and success (bottom)
    let half_h = plot_h / 2.0;

    // Use dots for very small datasets, bars for larger ones
    let use_dots = total_count < 15;

    // Adaptive bin count: fewer bins when less data
    let n_bins = if total_count < 30 {
        5
    } else if total_count < 100 {
        8
    } else {
        10
    };
    let bin_width = 1.0 / n_bins as f64;

    let to_x = |v: f64| -> f64 { pad_l + v.clamp(0.0, 1.0) * plot_w };

    let mut svg = format!(
        r#"<div class="card">
        <h2>Renegade Prediction Accuracy</h2>
        <p style="font-size:0.8em;color:var(--text-muted);">
            Distribution of predicted failure probability, split by outcome.
            <span style="color:var(--accent-danger, #f85149);">Failures</span> above,
            <span style="color:var(--accent-success, #3fb950);">successes</span> below.
            Well-calibrated: failures skew right, successes skew left.
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

    // Center dividing line
    write!(
        svg,
        r#"<line x1="{lx}" y1="{my}" x2="{rx}" y2="{my}" stroke="var(--text-muted)" stroke-width="1"/>"#,
        lx = pad_l,
        my = mid_y,
        rx = pad_l + plot_w,
    )
    .ok();

    // X-axis labels and grid
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
        r#"<text x="{x}" y="{y}" text-anchor="end" font-size="9" fill="var(--accent-danger, #f85149)">Fail</text>"#,
        x = pad_l - 4.0,
        y = pad_t + 14.0,
    )
    .ok();
    write!(
        svg,
        r#"<text x="{x}" y="{y}" text-anchor="end" font-size="9" fill="var(--accent-success, #3fb950)">OK</text>"#,
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

    if use_dots {
        // Dot strip mode: stack dots vertically from the center line outward
        let dot_r = 3.5;
        let dot_spacing = dot_r * 2.2;

        // Bin dots to stack them
        let mut fail_bins: Vec<Vec<f64>> = vec![Vec::new(); n_bins];
        let mut ok_bins: Vec<Vec<f64>> = vec![Vec::new(); n_bins];

        for &p in &failures {
            let bin = ((p / bin_width) as usize).min(n_bins - 1);
            fail_bins[bin].push(p);
        }
        for &p in &successes {
            let bin = ((p / bin_width) as usize).min(n_bins - 1);
            ok_bins[bin].push(p);
        }

        // Draw failure dots (grow upward from center), with overflow indicator
        for (bin_idx, dots) in fail_bins.iter().enumerate() {
            let cx = pad_l + (bin_idx as f64 + 0.5) * bin_width * plot_w;
            let mut drawn = 0;
            for (i, _) in dots.iter().enumerate() {
                let cy = mid_y - dot_spacing * (i as f64 + 0.5);
                if cy < pad_t + dot_r {
                    break;
                }
                drawn += 1;
                write!(
                    svg,
                    r#"<circle cx="{cx:.1}" cy="{cy:.1}" r="{r}" fill="var(--accent-danger, #f85149)" opacity="0.7"/>"#,
                    cx = cx,
                    cy = cy,
                    r = dot_r,
                )
                .ok();
            }
            if drawn < dots.len() {
                write!(
                    svg,
                    r#"<text x="{cx:.1}" y="{y:.1}" text-anchor="middle" font-size="8" fill="var(--accent-danger, #f85149)">+{n}</text>"#,
                    cx = cx,
                    y = pad_t - 1.0,
                    n = dots.len() - drawn,
                )
                .ok();
            }
        }

        // Draw success dots (grow downward from center), with overflow indicator
        for (bin_idx, dots) in ok_bins.iter().enumerate() {
            let cx = pad_l + (bin_idx as f64 + 0.5) * bin_width * plot_w;
            let mut drawn = 0;
            for (i, _) in dots.iter().enumerate() {
                let cy = mid_y + dot_spacing * (i as f64 + 0.5);
                if cy > pad_t + plot_h - dot_r {
                    break;
                }
                drawn += 1;
                write!(
                    svg,
                    r#"<circle cx="{cx:.1}" cy="{cy:.1}" r="{r}" fill="var(--accent-success, #3fb950)" opacity="0.7"/>"#,
                    cx = cx,
                    cy = cy,
                    r = dot_r,
                )
                .ok();
            }
            if drawn < dots.len() {
                write!(
                    svg,
                    r#"<text x="{cx:.1}" y="{y:.1}" text-anchor="middle" font-size="8" fill="var(--accent-success, #3fb950)">+{n}</text>"#,
                    cx = cx,
                    y = pad_t + plot_h + 10.0,
                    n = dots.len() - drawn,
                )
                .ok();
            }
        }
    } else {
        // Histogram bar mode
        let mut fail_counts = vec![0usize; n_bins];
        let mut ok_counts = vec![0usize; n_bins];

        for &p in &failures {
            let bin = ((p / bin_width) as usize).min(n_bins - 1);
            fail_counts[bin] += 1;
        }
        for &p in &successes {
            let bin = ((p / bin_width) as usize).min(n_bins - 1);
            ok_counts[bin] += 1;
        }

        let max_count = fail_counts
            .iter()
            .chain(ok_counts.iter())
            .copied()
            .max()
            .unwrap_or(1)
            .max(1);

        let bar_w = plot_w / n_bins as f64;
        let bar_pad = bar_w * 0.1;

        for i in 0..n_bins {
            let x = pad_l + i as f64 * bar_w + bar_pad;
            let bw = bar_w - 2.0 * bar_pad;

            // Failure bars grow upward from center
            if fail_counts[i] > 0 {
                let bar_h = (fail_counts[i] as f64 / max_count as f64) * (half_h - 4.0);
                let y = mid_y - bar_h;
                write!(
                    svg,
                    r#"<rect x="{x:.1}" y="{y:.1}" width="{bw:.1}" height="{bh:.1}" fill="var(--accent-danger, #f85149)" opacity="0.7" rx="1"/>"#,
                    x = x,
                    y = y,
                    bw = bw,
                    bh = bar_h,
                )
                .ok();
                // Count label on bar
                write!(
                    svg,
                    r#"<text x="{tx:.1}" y="{ty:.1}" text-anchor="middle" font-size="8" fill="var(--text-muted)">{n}</text>"#,
                    tx = x + bw / 2.0,
                    ty = y - 2.0,
                    n = fail_counts[i],
                )
                .ok();
            }

            // Success bars grow downward from center
            if ok_counts[i] > 0 {
                let bar_h = (ok_counts[i] as f64 / max_count as f64) * (half_h - 4.0);
                write!(
                    svg,
                    r#"<rect x="{x:.1}" y="{my:.1}" width="{bw:.1}" height="{bh:.1}" fill="var(--accent-success, #3fb950)" opacity="0.7" rx="1"/>"#,
                    x = x,
                    my = mid_y,
                    bw = bw,
                    bh = bar_h,
                )
                .ok();
                // Count label on bar
                write!(
                    svg,
                    r#"<text x="{tx:.1}" y="{ty:.1}" text-anchor="middle" font-size="8" fill="var(--text-muted)">{n}</text>"#,
                    tx = x + bw / 2.0,
                    ty = mid_y + bar_h + 10.0,
                    n = ok_counts[i],
                )
                .ok();
            }
        }
    }

    // Stats summary
    write!(
        svg,
        r#"<text x="{x}" y="{y}" text-anchor="start" font-size="9" fill="var(--text-muted)">{n} events ({s} ok, {f} fail)</text>"#,
        x = pad_l + 2.0,
        y = pad_t - 5.0,
        n = total_count,
        s = successes.len(),
        f = failures.len(),
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
    background: rgba(10, 186, 181, 0.15);
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
        RingStatsSnapshot,
    };
    use crate::transport::metrics::TransportSnapshot;
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
            ring_stats: RingStatsSnapshot::default(),
            transport_snapshot: TransportSnapshot::default(),
            governance: Default::default(),
        }
    }

    #[test]
    fn favicon_grey_when_no_snapshot() {
        let uri = build_favicon_data_uri(&None);
        assert!(uri.starts_with("data:image/svg+xml,"));
        assert!(uri.contains("%239e9e9e"), "expected grey color");
    }

    #[test]
    fn favicon_teal_when_connected() {
        let mut snap = base_snapshot();
        snap.open_connections = 3;
        let uri = build_favicon_data_uri(&Some(snap));
        assert!(uri.contains("%230abab5"), "expected teal color");
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
    fn favicon_connected_overrides_failures() {
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
            uri.contains("%230abab5"),
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
    fn transfer_card_hidden_when_fresh_start() {
        let mut snap = base_snapshot();
        snap.elapsed_secs = 3; // first few seconds, no traffic yet
        let html = build_transfer_card(&Some(snap));
        assert!(
            html.is_empty(),
            "transfer card should be hidden in first 10s with no data"
        );
    }

    // Superseded: the 10s grace period replaced the unconditional
    // hide-on-zero-open-connections logic (#3507).
    #[ignore]
    #[test]
    fn transfer_card_hidden_when_no_data() {
        let mut snap = base_snapshot();
        snap.elapsed_secs = 100; // well past grace, still no data
        // Old behaviour: hidden. New behaviour: shown (tested above).
    }

    #[test]
    fn transfer_card_shown_after_grace_period() {
        let snap = base_snapshot(); // elapsed_secs=10, no traffic → still show
        let html = build_transfer_card(&Some(snap));
        assert!(
            !html.is_empty(),
            "transfer card should render after grace period even without data"
        );
    }

    /// Non-zero transport metrics must render RTT, cwnd, slowdown, and
    /// transfer sub-sections — currently every test uses default zeros,
    /// which skips those branches entirely.
    #[test]
    fn transfer_card_renders_subsections_with_data() {
        let mut snap = base_snapshot();
        snap.bytes_uploaded = 5000;
        snap.open_connections = 1;
        snap.transport_snapshot.avg_rtt_us = 12500; // 12.5ms
        snap.transport_snapshot.min_rtt_us = 8000;
        snap.transport_snapshot.max_rtt_us = 45000;
        snap.transport_snapshot.avg_cwnd_bytes = 32768;
        snap.transport_snapshot.peak_cwnd_bytes = 98304;
        snap.transport_snapshot.min_cwnd_bytes = 11264;
        snap.transport_snapshot.slowdowns_triggered = 23;
        snap.transport_snapshot.transfers_completed = 847;
        snap.transport_snapshot.transfers_failed = 3;
        snap.transport_snapshot.avg_transfer_time_ms = 1200;
        let html = build_transfer_card(&Some(snap));
        assert!(html.contains("RTT"), "RTT row missing");
        assert!(html.contains("12.5ms"), "avg RTT missing");
        assert!(html.contains("8.0ms"), "min RTT missing");
        assert!(html.contains("45.0ms"), "max RTT missing");
        assert!(html.contains("cwnd"), "cwnd row missing");
        assert!(html.contains("LEDBAT"), "slowdown row missing");
        assert!(html.contains("23</span>"), "slowdown count missing");
        assert!(html.contains("847"), "transfers completed missing");
        assert!(html.contains("3"), "transfers failed missing");
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
    fn title_is_fn_peer() {
        let html = homepage_html();
        assert!(
            html.contains("<title>FN Peer</title>"),
            "dashboard title must be 'FN Peer' — do not rename without updating this test"
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
                instance_id: "ABC123".to_string(),
                subscribed_secs: 100,
                last_updated_secs: Some(5),
            },
            ContractSnapshot {
                key_short: "DEF4...".to_string(),
                key_full: "DEF456".to_string(),
                instance_id: "DEF456".to_string(),
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

    #[test]
    fn renegade_chart_empty_input() {
        let svg = build_renegade_accuracy_chart(&[]);
        assert!(svg.contains("0 events (0 ok, 0 fail)"));
        assert!(svg.contains("<svg"));
    }

    #[test]
    fn renegade_chart_dot_mode_sparse_data() {
        // < 15 points should use dot mode (circles, not rects for bars)
        let pairs: Vec<(f64, f64)> = vec![
            (0.1, 0.0), // success, low predicted
            (0.9, 1.0), // failure, high predicted
            (0.5, 0.0), // success, mid predicted
        ];
        let svg = build_renegade_accuracy_chart(&pairs);
        assert!(svg.contains("<circle"), "dot mode should render circles");
        assert!(svg.contains("3 events (2 ok, 1 fail)"));
    }

    #[test]
    fn renegade_chart_bar_mode_dense_data() {
        // >= 15 points should use histogram bars
        let pairs: Vec<(f64, f64)> = (0..20)
            .map(|i| (i as f64 / 20.0, if i > 10 { 1.0 } else { 0.0 }))
            .collect();
        let svg = build_renegade_accuracy_chart(&pairs);
        assert!(svg.contains("<rect"), "bar mode should render rects");
        assert!(svg.contains("20 events"));
    }

    #[test]
    fn renegade_chart_filters_nan_values() {
        let pairs = vec![
            (0.5, 0.0),           // valid success
            (f64::NAN, 1.0),      // NaN predicted -- filtered
            (0.3, f64::NAN),      // NaN actual -- filtered
            (f64::INFINITY, 0.0), // infinite predicted -- filtered
            (0.7, 1.0),           // valid failure
        ];
        let svg = build_renegade_accuracy_chart(&pairs);
        // Only the 2 valid pairs should be counted
        assert!(svg.contains("2 events (1 ok, 1 fail)"));
    }

    #[test]
    fn renegade_chart_all_successes() {
        let pairs: Vec<(f64, f64)> = vec![(0.1, 0.0), (0.2, 0.0), (0.3, 0.0)];
        let svg = build_renegade_accuracy_chart(&pairs);
        assert!(svg.contains("3 events (3 ok, 0 fail)"));
    }

    #[test]
    fn renegade_chart_boundary_predicted_value() {
        // p=1.0 should not panic (bin clamping)
        let pairs = vec![(1.0, 0.0), (0.0, 1.0)];
        let svg = build_renegade_accuracy_chart(&pairs);
        assert!(svg.contains("2 events"));
    }

    #[test]
    fn fmt_prediction_time_sentinel_values() {
        assert_eq!(fmt_prediction_time(f64::MAX / 2.0), "N/A");
        assert_eq!(fmt_prediction_time(f64::INFINITY), "N/A");
        assert_eq!(fmt_prediction_time(f64::NAN), "N/A");
        assert_eq!(fmt_prediction_time(-1.0), "N/A");
        assert_eq!(fmt_prediction_time(0.0), "0.000s");
        assert_eq!(fmt_prediction_time(1.5), "1.500s");
        assert_eq!(fmt_prediction_time(1.0e9), "N/A"); // at the limit
        assert_eq!(fmt_prediction_time(999_999_999.0), "999999999.000s");
    }

    #[test]
    fn fmt_prediction_speed_sentinel_values() {
        assert_eq!(fmt_prediction_speed(0.0), "N/A");
        assert_eq!(fmt_prediction_speed(-5.0), "N/A");
        assert_eq!(fmt_prediction_speed(f64::NAN), "N/A");
        assert_eq!(fmt_prediction_speed(f64::INFINITY), "N/A");
        assert_eq!(fmt_prediction_speed(1024.0), "1024 B/s");
    }

    /// Regression: with no data the helper must still emit a titled
    /// placeholder so all three prediction-component slots
    /// (Failure Probability, Response Time, Transfer Rate) stay
    /// visible on the peer dashboard. The previous behaviour hid the
    /// chart entirely, which masked the driver data-collection
    /// regression that left response-time/transfer-rate estimators
    /// permanently empty (Failure-Probability-only dashboard).
    #[test]
    fn build_estimator_chart_or_placeholder_empty_renders_titled_placeholder() {
        let html = build_estimator_chart_or_placeholder(
            "Response Time (s)",
            &[],
            (0.0, 0.0),
            None,
            None,
            "0",
            "auto",
            "No timed responses have been observed from this peer yet.",
        );
        assert!(
            html.contains("<h3>Response Time (s)</h3>"),
            "empty placeholder must still display the chart title so the user sees the slot is part of the model, got: {html}"
        );
        assert!(
            html.contains("No timed responses have been observed"),
            "empty placeholder must explain why no curve is shown, got: {html}"
        );
        assert!(
            !html.contains("<svg"),
            "empty placeholder must not render an SVG, got: {html}"
        );
    }

    /// Regression: the per-tab "Routing Predictions" panel must call
    /// `build_estimator_chart_or_placeholder` for all three
    /// prediction-component slots (Failure Probability, Response Time,
    /// Transfer Rate). Hiding empty slots previously masked the
    /// driver data-collection regression for months — keeping every
    /// slot visible makes future regressions detectable on sight.
    /// Source-scrape rather than HTML-grep because the visible-when-empty
    /// behaviour depends on a router_snapshot being present, and the
    /// `home_page.rs::tests` module does not have a snapshot fixture
    /// builder.
    #[test]
    fn peer_detail_panel_calls_estimator_helper_for_all_three_components() {
        let src = include_str!("home_page.rs");
        // Truncate at the test marker so the assertion below doesn't
        // match against this very test's source.
        let cutoff = src
            .find("#[cfg(test)]")
            .expect("home_page.rs must have a #[cfg(test)] section");
        let prod = &src[..cutoff];
        for title in [
            "Failure Probability",
            "Response Time (s)",
            "Transfer Rate (B/s)",
        ] {
            // Find the helper call site and walk forward up to 200 bytes
            // for the title literal. Whitespace-tolerant so rustfmt
            // doesn't churn this pin.
            let mut found = false;
            let mut cursor = 0;
            while let Some(call) = prod[cursor..].find("build_estimator_chart_or_placeholder(") {
                let abs = cursor + call;
                let tail_end = (abs + 400).min(prod.len());
                let needle = format!("\"{title}\"");
                if prod[abs..tail_end].contains(&needle) {
                    found = true;
                    break;
                }
                cursor = abs + 1;
            }
            assert!(
                found,
                "peer-detail panel builder must call \
                 build_estimator_chart_or_placeholder with title {title:?} so the slot is \
                 always visible. Without this every prediction-component \
                 slot can silently disappear when its estimator has no \
                 data — the original regression."
            );
        }
    }

    #[test]
    fn build_estimator_chart_or_placeholder_renders_chart_when_data_present() {
        let curve = vec![(0.0, 0.1), (0.25, 0.5), (0.5, 0.9)];
        let html = build_estimator_chart_or_placeholder(
            "Failure Probability",
            &curve,
            (0.0, 0.5),
            None,
            None,
            "0.0",
            "1.0",
            "should not see this",
        );
        assert!(html.contains("<svg"), "non-empty curve must render an SVG");
        assert!(
            !html.contains("should not see this"),
            "non-empty curve must not show the empty-message text"
        );
    }

    #[test]
    fn fmt_prediction_prob_sentinel_values() {
        assert_eq!(fmt_prediction_prob(f64::NAN), "N/A");
        assert_eq!(fmt_prediction_prob(f64::INFINITY), "N/A");
        assert_eq!(fmt_prediction_prob(-0.1), "N/A");
        assert_eq!(fmt_prediction_prob(1.1), "N/A");
        assert_eq!(fmt_prediction_prob(0.0), "0.0000");
        assert_eq!(fmt_prediction_prob(1.0), "1.0000");
        assert_eq!(fmt_prediction_prob(0.5), "0.5000");
    }

    fn sample_peer(addr: &str, location: f64) -> crate::node::network_status::PeerSnapshot {
        use crate::node::network_status::PeerSnapshot;
        PeerSnapshot {
            address: addr.parse().unwrap(),
            is_gateway: false,
            location: Some(location),
            connected_secs: 60,
            peer_key_location: None,
            bytes_sent: 1024,
            bytes_received: 2048,
        }
    }

    #[test]
    fn ring_svg_dots_link_to_peer_pages() {
        let peers = vec![sample_peer("127.0.0.1:31337", 0.25)];
        let svg = build_ring_svg(Some(0.5), &peers, None, &[]);
        assert!(
            svg.contains("<a href=\"/peer/127.0.0.1:31337\""),
            "ring peer dot must be wrapped in a link to /peer/{{addr}}, got: {svg}"
        );
        assert!(
            svg.contains("<title>"),
            "ring peer dot must include a <title> tooltip, got: {svg}"
        );
    }

    #[test]
    fn peers_table_is_sortable_with_raw_sort_values() {
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.peers = vec![sample_peer("127.0.0.1:31337", 0.25)];
        let html = build_peers_card(&Some(snap));
        assert!(
            html.contains("class=\"sortable\""),
            "peers table must be sortable"
        );
        assert!(
            html.contains("data-sort-type=\"num\""),
            "peers table must declare numeric sort columns"
        );
        // Bytes are formatted (e.g. "1.0 KB") but must sort by raw byte counts.
        assert!(
            html.contains("data-sort=\"1024\""),
            "sent bytes cell must carry raw byte count for sorting"
        );
        assert!(
            html.contains("data-sort=\"2048\""),
            "recv bytes cell must carry raw byte count for sorting"
        );
    }

    #[test]
    fn contracts_table_has_copy_button() {
        use crate::node::network_status::ContractSnapshot;
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.contracts = vec![ContractSnapshot {
            key_short: "ABC1...".to_string(),
            key_full: "ABC123XYZ".to_string(),
            instance_id: "ABC123XYZ".to_string(),
            subscribed_secs: 100,
            last_updated_secs: Some(5),
        }];
        let html = build_contracts_card(&Some(snap));
        assert!(
            html.contains("class=\"copy-key\""),
            "contract cell must render a copy button"
        );
        assert!(
            html.contains("data-copy=\"ABC123XYZ\""),
            "copy button must carry the full contract key, got: {html}"
        );
        assert!(
            html.contains("class=\"sortable\""),
            "contracts table must be sortable"
        );
    }

    #[test]
    fn header_includes_update_badge() {
        let html = homepage_html();
        assert!(
            html.contains("id=\"update-badge\""),
            "homepage header must expose an update-badge slot for the JS update check"
        );
        assert!(
            html.contains("id=\"version-badge\""),
            "homepage header must tag the version badge with an id so the JS check can read data-version"
        );
        assert!(
            html.contains("data-version="),
            "version badge must expose data-version for the update comparison"
        );
    }

    #[test]
    fn js_contains_update_check_and_sort() {
        // Lightweight contract: the dashboard JS must keep the new
        // helpers wired up. If you rename them, update both sides.
        assert!(
            JS.contains("checkForUpdate"),
            "JS must include the update check function"
        );
        assert!(
            JS.contains("applySort"),
            "JS must include the table sort function"
        );
        assert!(
            JS.contains("copyToClipboard"),
            "JS must include the clipboard copy helper"
        );
    }

    // ── Edge cases for the ring SVG ─────────────────────────────────────────

    #[test]
    fn ring_svg_self_dot_has_title_but_no_link() {
        // The local node has no /peer/{addr} page, so the self circle
        // must NOT be wrapped in an <a>, but it should still expose a
        // <title> for parity with the peer dots.
        let svg = build_ring_svg(Some(0.42), &[], None, &[]);
        assert!(svg.contains("<svg"), "ring SVG should still render");
        assert!(
            !svg.contains("<a "),
            "self-only ring must not contain any <a> wrappers, got: {svg}"
        );
        assert!(
            svg.contains("<g class=\"ring-self\">"),
            "self dot must be wrapped in a <g> with the ring-self class"
        );
        assert!(
            svg.contains("<title>You "),
            "self dot must include a 'You' title, got: {svg}"
        );
    }

    #[test]
    fn ring_svg_distinguishes_gateway_in_link_title() {
        // Gateway peers and regular peers route to the same /peer/{addr}
        // page, but the SVG <title> tooltip should label them differently
        // so users can identify gateways without hovering each one.
        use crate::node::network_status::PeerSnapshot;
        let gw = PeerSnapshot {
            address: "10.0.0.1:31337".parse().unwrap(),
            is_gateway: true,
            location: Some(0.10),
            connected_secs: 0,
            peer_key_location: None,
            bytes_sent: 0,
            bytes_received: 0,
        };
        let peer = sample_peer("10.0.0.2:31338", 0.90);
        let svg = build_ring_svg(Some(0.5), &[gw, peer], None, &[]);
        assert!(
            svg.contains("<title>Gateway 10.0.0.1:31337"),
            "gateway dot title must say 'Gateway', got: {svg}"
        );
        assert!(
            svg.contains("<title>Peer 10.0.0.2:31338"),
            "regular peer title must say 'Peer', got: {svg}"
        );
        assert!(
            svg.contains("href=\"/peer/10.0.0.1:31337\""),
            "gateway dot must still link to /peer/{{addr}}"
        );
    }

    #[test]
    fn ring_svg_omitted_when_no_locations() {
        // If neither own_location nor any peer has a location, there's
        // nothing to plot — return empty so the card just shows the table.
        use crate::node::network_status::PeerSnapshot;
        let no_loc_peer = PeerSnapshot {
            address: "10.0.0.3:1".parse().unwrap(),
            is_gateway: false,
            location: None,
            connected_secs: 0,
            peer_key_location: None,
            bytes_sent: 0,
            bytes_received: 0,
        };
        assert!(build_ring_svg(None, &[no_loc_peer], None, &[]).is_empty());
        assert!(build_ring_svg(None, &[], None, &[]).is_empty());
    }

    /// Pin: build_ring_svg renders hosted contracts as faint dim
    /// teal dots on the inner ring (non-flagged ones). Without this
    /// the inner ring was visually empty in healthy state and made
    /// the renderer look unfinished.
    ///
    /// Rule-review of #4298 caught that the new `hosted_contracts`
    /// rendering loop was untested — every test site passed `&[]`.
    /// This test exercises the happy path (at least one dot
    /// rendered) and the skip-flagged path (a contract present in
    /// BOTH hosted and flagged sets gets the flagged marker only,
    /// not a duplicate hosted dot).
    #[test]
    fn ring_svg_renders_hosted_contracts_on_inner_ring() {
        use crate::node::network_status::{
            ContractGovernanceEntry, ContractSnapshot, GovernanceSnapshot, GovernanceStateSnapshot,
            NetworkNorms,
        };

        let hosted = vec![
            ContractSnapshot {
                key_short: "HOST1...".to_string(),
                key_full: "HOST1_with_params".to_string(),
                instance_id: "HOST1".to_string(),
                subscribed_secs: 60,
                last_updated_secs: Some(5),
            },
            ContractSnapshot {
                key_short: "HOST2...".to_string(),
                key_full: "HOST2_with_params".to_string(),
                instance_id: "HOST2".to_string(),
                subscribed_secs: 60,
                last_updated_secs: Some(5),
            },
            // This one is ALSO flagged — should be skipped in the
            // hosted dim-dot loop to avoid a duplicate marker.
            ContractSnapshot {
                key_short: "FLAG1...".to_string(),
                key_full: "FLAG1_with_params".to_string(),
                instance_id: "FLAG1".to_string(),
                subscribed_secs: 60,
                last_updated_secs: Some(5),
            },
        ];

        let governance = GovernanceSnapshot {
            mode: crate::node::network_status::GovernanceModeSnapshot::DryRun,
            contracts: vec![ContractGovernanceEntry {
                instance_id: "FLAG1".to_string(),
                instance_id_short: "FLAG1".to_string(),
                state: GovernanceStateSnapshot::WouldEvict,
                cost_used: 1.0,
                benefit_score: 1.0,
                log_ratio: Some(0.0),
                age_secs: 100,
                last_transition_secs_ago: 1,
                history: Vec::new(),
            }],
            observed_count: 3,
            min_samples: 30,
            norms: NetworkNorms::default(),
            last_tick_at: None,
            state_by_id: std::collections::HashMap::new(),
        };

        let svg = build_ring_svg(Some(0.5), &[], Some(&governance), &hosted);

        // The faint hosted-contract dot uses the brand teal at 0.45
        // opacity. Pin both attributes so a future refactor that
        // changes the style triggers the test, AND count the dots so
        // we know flagged-skipping worked.
        let hosted_dot_count = svg
            .matches("fill=\"#43c178\" fill-opacity=\"0.45\"")
            .count();
        assert_eq!(
            hosted_dot_count, 2,
            "expected exactly 2 hosted-contract dim dots (HOST1, HOST2). \
             FLAG1 should be skipped because it's already in the flagged set. \
             Got {hosted_dot_count} hosted dots in SVG:\n{svg}"
        );

        // FLAG1 must still appear, just via the brighter flagged-
        // dot rendering (the WouldEvict color, with glow).
        assert!(
            svg.contains("#ff8a3d"),
            "FLAG1 should render with its WouldEvict color regardless of being in hosted set"
        );
    }

    /// Pin: when the hosted-contracts slice is empty, the inner ring
    /// emits no hosted-dot circles. Sanity check that the
    /// `&[] → no rendering` path still works as before.
    #[test]
    fn ring_svg_no_hosted_contracts_means_no_dim_dots() {
        let svg = build_ring_svg(Some(0.5), &[], None, &[]);
        assert!(
            !svg.contains("fill-opacity=\"0.45\""),
            "empty hosted slice must produce no dim-dot fill-opacity attribute, got:\n{svg}"
        );
    }

    // ── Sort attribute coverage for both tables ─────────────────────────────

    #[test]
    fn peers_table_handles_missing_location_in_sort() {
        // A peer with no known location should still be sortable: emit
        // an empty data-sort so the JS comparator treats it as the
        // largest value (sinks to the bottom in ascending order).
        use crate::node::network_status::PeerSnapshot;
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.peers = vec![PeerSnapshot {
            address: "10.0.0.4:1".parse().unwrap(),
            is_gateway: false,
            location: None,
            connected_secs: 5,
            peer_key_location: None,
            bytes_sent: 0,
            bytes_received: 0,
        }];
        let html = build_peers_card(&Some(snap));
        assert!(
            html.contains("data-sort=\"\">—"),
            "peer row with unknown location must emit empty data-sort, got: {html}"
        );
    }

    #[test]
    fn contracts_table_preserves_full_key_tooltip_and_code_markup() {
        // The full-key tooltip on the cell predates the copy button and
        // must NOT be lost when we add the button — that tooltip is
        // still useful for hover-only users (e.g. read-only screenshots).
        // Likewise, <code>{short}</code> must stay outside the button so
        // the abbreviated key keeps its monospace styling without a
        // hover state on the contract text itself.
        use crate::node::network_status::ContractSnapshot;
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.contracts = vec![ContractSnapshot {
            key_short: "DEAD...".to_string(),
            key_full: "DEADBEEF".to_string(),
            instance_id: "DEADBEEF".to_string(),
            subscribed_secs: 60,
            last_updated_secs: Some(2),
        }];
        let html = build_contracts_card(&Some(snap));
        assert!(
            html.contains("title=\"DEADBEEF\""),
            "the original full-key cell tooltip must be preserved, got: {html}"
        );
        assert!(
            html.contains("<code>DEAD...</code>"),
            "the abbreviated key must stay as a plain <code> sibling of the button, got: {html}"
        );
        // Lock in the simplified markup: the <code> element is a sibling
        // of the button, NOT wrapped inside it.
        assert!(
            !html.contains("class=\"copy-key\" data-copy=\"DEADBEEF\" title=\"Copy contract key\" aria-label=\"Copy contract key\">⧉</button><code>"),
            "<code> must come BEFORE the copy button"
        );
        assert!(
            html.contains("</code><button type=\"button\" class=\"copy-key\""),
            "<code> must directly precede the <button> sibling, got: {html}"
        );
    }

    #[test]
    fn contracts_table_never_updated_sorts_last() {
        // Contracts that have never been updated (last_updated_secs = None)
        // should sink to the bottom in ascending order — represented as
        // u64::MAX in the data-sort attribute. If we emitted "—" or 0,
        // the row would jump to the top and look "freshest".
        use crate::node::network_status::ContractSnapshot;
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.contracts = vec![
            ContractSnapshot {
                key_short: "FRESH..".to_string(),
                key_full: "FRESH".to_string(),
                instance_id: "FRESH".to_string(),
                subscribed_secs: 1,
                last_updated_secs: Some(1),
            },
            ContractSnapshot {
                key_short: "NEVER..".to_string(),
                key_full: "NEVER".to_string(),
                instance_id: "NEVER".to_string(),
                subscribed_secs: 1,
                last_updated_secs: None,
            },
        ];
        let html = build_contracts_card(&Some(snap));
        let sentinel = format!("data-sort=\"{}\">—", u64::MAX);
        assert!(
            html.contains(&sentinel),
            "never-updated contract must emit data-sort=\"{}\" so it sorts last in ascending order, got: {html}",
            u64::MAX
        );
    }

    /// Regression test for the ContractKey/ContractInstanceId
    /// id-vs-key string mismatch that Codex review of #4298 caught:
    /// `GovernanceSnapshot.state_by_id` is keyed by
    /// `ContractInstanceId::to_string()` (the 32-byte content hash),
    /// while `ContractSnapshot.key_full` is the full `ContractKey`
    /// encoding (including parameters / code-hash bookkeeping). The
    /// two strings are NOT equal, so a lookup keyed on `key_full`
    /// would silently miss every flagged contract.
    ///
    /// Pin: with a contract whose `instance_id` differs from
    /// `key_full` and whose state in `state_by_id` is Banned, the
    /// rendered row MUST show "banned" (not "ok"). Pre-fix the
    /// assertion would have failed because the lookup never found
    /// the entry.
    #[test]
    fn contracts_table_gov_column_uses_instance_id_not_key_full() {
        use crate::node::network_status::{ContractSnapshot, GovernanceStateSnapshot};
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        // The critical part: instance_id ≠ key_full. In production
        // key_full includes the params / code hash so this is the
        // common case, not an edge.
        snap.contracts = vec![ContractSnapshot {
            key_short: "FOO1234...".to_string(),
            key_full: "FOO1234WITH_PARAMS_AND_CODE_HASH".to_string(),
            instance_id: "FOO1234".to_string(),
            subscribed_secs: 60,
            last_updated_secs: Some(10),
        }];
        snap.governance
            .state_by_id
            .insert("FOO1234".to_string(), GovernanceStateSnapshot::Banned);
        let html = build_contracts_card(&Some(snap));
        assert!(
            html.contains(r#"<span class="gov-pill gov-banned">banned</span>"#),
            "Gov column lookup must use `instance_id` (not `key_full`) so \
             state_by_id keys match — got:\n{html}"
        );
    }

    // ── Header / version badge guarantees ───────────────────────────────────

    /// Helper for header-element tests: pull a specific HTML element line
    /// out of the rendered homepage. We can't just take the first line
    /// containing the id, because the CSS block and JS bundle also
    /// reference these ids by name.
    fn extract_element_line(html: &str, anchor: &str) -> String {
        html.lines()
            .find(|l| l.contains(anchor) && l.trim_start().starts_with('<'))
            .unwrap_or_else(|| panic!("no HTML line containing {anchor:?} in homepage"))
            .to_string()
    }

    #[test]
    fn version_badge_data_attribute_matches_visible_text() {
        // The JS update check reads `data-version` and compares it
        // against the GitHub `tag_name`. If the attribute drifted away
        // from the visible "v{version}" text, the user would see one
        // version on the chip and the comparator would use another —
        // so lock that they always agree.
        //
        // Note: in unit tests there is no live NetworkStatusSnapshot,
        // so the rendered version is the "?" placeholder; the contract
        // we want is "attribute == visible text minus the leading v",
        // which holds for both the placeholder and the runtime value.
        let html = homepage_html();
        let line = extract_element_line(&html, "id=\"version-badge\"");
        let data_ver = line
            .split("data-version=\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .expect("version badge must declare data-version");
        let visible = line
            .split('>')
            .nth(1)
            .and_then(|s| s.split('<').next())
            .expect("version badge must contain visible text");
        assert!(
            visible.starts_with('v'),
            "version chip text must start with 'v', got: {visible:?}"
        );
        assert_eq!(
            &visible[1..],
            data_ver,
            "data-version ({data_ver:?}) must equal the visible chip text without the leading 'v' ({visible:?})"
        );
    }

    #[test]
    fn update_badge_links_to_releases_and_starts_hidden() {
        // The badge must:
        //   1. Link to the GitHub releases page (so users land somewhere
        //      sensible when they click it).
        //   2. Open in a new tab with rel=noopener (we don't want the
        //      release page to navigate the dashboard frame).
        //   3. Start hidden — we surface it only after the JS update
        //      check confirms a newer version exists.
        let html = homepage_html();
        let line = extract_element_line(&html, "id=\"update-badge\"");
        assert!(
            line.contains("href=\"https://github.com/freenet/freenet-core/releases/latest\""),
            "update badge must link to the releases page, got: {line}"
        );
        assert!(
            line.contains("target=\"_blank\""),
            "update badge must open in a new tab"
        );
        assert!(
            line.contains("rel=\"noopener noreferrer\""),
            "update badge must use rel=noopener noreferrer for safety"
        );
        assert!(
            line.contains(" hidden"),
            "update badge must start hidden — JS unhides it only when an update is found"
        );
    }

    #[test]
    fn js_update_check_uses_localstorage_cache() {
        // Two guarantees the JS check makes about resource use:
        //   1. We compare semver, not string equality — otherwise
        //      "0.2.10" would look "older" than "0.2.9".
        //   2. We cache the GitHub response in localStorage so we don't
        //      hit the api.github.com rate limit on every refresh.
        // Pin both as substrings.
        assert!(
            JS.contains("compareSemver"),
            "JS must include the semver comparator the update check relies on"
        );
        assert!(
            JS.contains("localStorage") && JS.contains("freenet-update-check"),
            "JS must persist the update check in localStorage under the freenet-update-check key"
        );
    }

    // ─── Governance card (Phase 4.5) ───────────────────────────────

    use crate::node::network_status::{
        ContractGovernanceEntry, GovernanceModeSnapshot, GovernanceSnapshot,
        GovernanceStateSnapshot, NetworkNorms,
    };

    fn mk_entry(state: GovernanceStateSnapshot, instance_id: &str) -> ContractGovernanceEntry {
        ContractGovernanceEntry {
            instance_id: instance_id.to_string(),
            instance_id_short: instance_id.chars().take(12).collect::<String>(),
            state,
            cost_used: 12.5,
            benefit_score: 1.0,
            log_ratio: Some(1.1),
            age_secs: 3600,
            last_transition_secs_ago: 60,
            history: Vec::new(),
        }
    }

    #[test]
    fn governance_card_empty_state_when_no_contracts() {
        // Empty state = "no FLAGGED contracts." Post-polish slice 2 the
        // card still renders structural skeleton (mode pill, 5-tile
        // mini-strip with em-dashes, observed/needed progress) so an
        // operator can see what fields will appear once data arrives.
        // Pin: the skeleton tiles + mode pill are visible.
        let snap = base_snapshot();
        let html = build_governance_card(&Some(snap));
        assert!(
            html.contains(r#"g-mode g-mode-dry-run">dry-run<"#),
            "empty state must show the mode pill — got:\n{html}"
        );
        assert!(
            html.contains("Eviction threshold"),
            "empty state must render the 5-tile skeleton — got:\n{html}"
        );
        assert!(
            html.contains("—"),
            "empty state should use em-dashes for missing tile values"
        );
        assert!(
            !html.contains("verdict-alert"),
            "empty state must not render the alert verdict block"
        );
    }

    #[test]
    fn governance_card_empty_state_shows_observed_progress() {
        // Pin: the empty state surfaces "N / min_samples" + the
        // remaining count, using the exact phrases the user sees —
        // not just digit substrings (Codex review nit).
        let mut snap = base_snapshot();
        snap.governance = GovernanceSnapshot {
            mode: GovernanceModeSnapshot::DryRun,
            contracts: Vec::new(), // none flagged
            norms: NetworkNorms::default(),
            observed_count: 12,
            min_samples: 30,
            last_tick_at: None,
            state_by_id: std::collections::HashMap::new(),
        };
        let html = build_governance_card(&Some(snap));
        assert!(
            html.contains("Observed 12 / 30 contracts needed"),
            "empty state should pin the 'Observed X / Y contracts needed' phrase — got:\n{html}"
        );
        assert!(
            html.contains("once 18 more contracts accumulate"),
            "empty state should name the remaining count by name — got:\n{html}"
        );
    }

    #[test]
    fn governance_card_empty_state_pluralizes_singular_count() {
        // Pin: when remaining == 1, the message uses "1 more contract"
        // not "1 more contracts". Codex review nit on pluralization.
        let mut snap = base_snapshot();
        snap.governance = GovernanceSnapshot {
            mode: GovernanceModeSnapshot::DryRun,
            contracts: Vec::new(),
            norms: NetworkNorms::default(),
            observed_count: 29,
            min_samples: 30,
            last_tick_at: None,
            state_by_id: std::collections::HashMap::new(),
        };
        let html = build_governance_card(&Some(snap));
        assert!(
            html.contains("once 1 more contract accumulates"),
            "with remaining=1 message must use singular 'contract accumulates' — got:\n{html}"
        );
    }

    #[test]
    fn governance_card_empty_state_healthy_when_enough_samples() {
        // Pin: once observed_count >= min_samples but nothing is
        // flagged, the empty state should read "all N within normal
        // range" — the healthy-steady-state message — not the
        // ramp-up-progress one.
        let mut snap = base_snapshot();
        snap.governance = GovernanceSnapshot {
            mode: GovernanceModeSnapshot::DryRun,
            contracts: Vec::new(),
            norms: NetworkNorms::default(),
            observed_count: 50,
            min_samples: 30,
            last_tick_at: None,
            state_by_id: std::collections::HashMap::new(),
        };
        let html = build_governance_card(&Some(snap));
        assert!(
            html.contains("All 50 tracked contracts within normal range")
                || html.contains("50 contracts within normal range"),
            "healthy-steady-state empty card should declare 'normal range' — got:\n{html}"
        );
    }

    #[test]
    fn governance_card_verdict_ok_when_all_normal() {
        let mut snap = base_snapshot();
        snap.governance = GovernanceSnapshot {
            mode: GovernanceModeSnapshot::DryRun,
            contracts: (0..5)
                .map(|i| mk_entry(GovernanceStateSnapshot::Normal, &format!("aaa{i}")))
                .collect(),
            norms: NetworkNorms::default(),
            observed_count: 0,
            min_samples: 30,
            last_tick_at: None,
            state_by_id: std::collections::HashMap::new(),
        };
        let html = build_governance_card(&Some(snap));
        assert!(
            html.contains("verdict-ok"),
            "5 normal contracts should produce verdict-ok styling — got:\n{html}"
        );
        assert!(
            html.contains("All 5 contracts within normal range"),
            "verdict should name the total — got:\n{html}"
        );
    }

    #[test]
    fn governance_card_verdict_alert_with_breakdown_when_flagged() {
        let mut snap = base_snapshot();
        snap.governance = GovernanceSnapshot {
            mode: GovernanceModeSnapshot::DryRun,
            contracts: vec![
                mk_entry(GovernanceStateSnapshot::WouldEvict, "abuser1"),
                mk_entry(GovernanceStateSnapshot::Borderline, "warn1"),
                mk_entry(GovernanceStateSnapshot::Borderline, "warn2"),
                mk_entry(GovernanceStateSnapshot::Normal, "ok1"),
            ],
            norms: NetworkNorms::default(),
            observed_count: 0,
            min_samples: 30,
            last_tick_at: None,
            state_by_id: std::collections::HashMap::new(),
        };
        let html = build_governance_card(&Some(snap));
        assert!(html.contains("verdict-alert"));
        // Total flagged = 1 + 2 = 3
        assert!(
            html.contains(">3<"),
            "verdict number should be the flagged count — got:\n{html}"
        );
        assert!(html.contains("1 would be evicted"));
        assert!(html.contains("2 borderline"));
        // Table renders the flagged ones, not the normal one.
        assert!(html.contains("abuser1"));
        assert!(html.contains("warn1"));
        assert!(!html.contains(r#"<code>ok1</code>"#));
    }

    #[test]
    fn governance_card_mode_pill_reflects_snapshot_mode() {
        for (mode, label) in [
            (GovernanceModeSnapshot::Off, "off"),
            (GovernanceModeSnapshot::DryRun, "dry-run"),
            (GovernanceModeSnapshot::Enforce, "enforce"),
        ] {
            let mut snap = base_snapshot();
            snap.governance = GovernanceSnapshot {
                mode,
                contracts: vec![mk_entry(GovernanceStateSnapshot::Normal, "ok")],
                norms: NetworkNorms::default(),
                observed_count: 0,
                min_samples: 30,
                last_tick_at: None,
                state_by_id: std::collections::HashMap::new(),
            };
            let html = build_governance_card(&Some(snap));
            assert!(
                html.contains(&format!(r#"g-mode g-mode-{label}">{label}<"#)),
                "mode pill should reflect {label} — got:\n{html}"
            );
        }
    }

    #[test]
    fn governance_card_omits_when_snap_is_none() {
        let html = build_governance_card(&None);
        assert!(html.is_empty());
    }
}
