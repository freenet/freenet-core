use super::*;

pub fn build_status_card(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
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

    // UPDATE rate-limiter stats: only shown once the limiter has seen
    // traffic, so idle nodes stay uncluttered. A non-zero "Rate-limited"
    // or "Capacity-dropped" count is the operator's signal that the
    // per-(sender, contract) UPDATE limiter is dropping relayed traffic.
    let rate_limit_html = if snap.ring_stats.updates_accepted > 0
        || snap.ring_stats.updates_rate_limited > 0
        || snap.ring_stats.updates_capacity_dropped > 0
    {
        format!(
            r#"<div class="metrics-row">
            <div class="metric-tile">
                <span class="metric-value">{accepted}</span>
                <span class="metric-label">UPDATEs relayed</span>
            </div>
            <div class="metric-tile">
                <span class="metric-value">{rate_limited}</span>
                <span class="metric-label">Rate-limited</span>
            </div>
            <div class="metric-tile">
                <span class="metric-value">{capacity_dropped}</span>
                <span class="metric-label">Capacity-dropped</span>
            </div>
        </div>"#,
            accepted = snap.ring_stats.updates_accepted,
            rate_limited = snap.ring_stats.updates_rate_limited,
            capacity_dropped = snap.ring_stats.updates_capacity_dropped,
        )
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
            {ring_stats_html}
            {rate_limit_html}
            {external_addr_html}
            {spinner}
            {gateway_warning}
            {nat_html}
            {failures_html}
        </div>"#,
        health_banner = health_banner,
        ring_stats_html = ring_stats_html,
        rate_limit_html = rate_limit_html,
        external_addr_html = external_addr_html,
        spinner = spinner,
        gateway_warning = gateway_warning,
        nat_html = nat_html,
        failures_html = failures_html,
    )
}

/// Format bytes as a human-readable string (e.g., "1.2 MB").
pub fn format_bytes(bytes: u64) -> String {
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

pub fn build_transfer_card(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
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

pub fn build_peers_card(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
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
pub fn build_ring_svg(
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

/// Format the governance card's "last evaluated" footer from the number
/// of seconds since the reaper last ticked. `format_ago` already appends
/// " ago" (or returns "just now"), so the template must NOT add a second
/// "ago" — doing so rendered "Last evaluated 18s ago ago".
pub fn format_last_evaluated(secs: u64) -> String {
    format!("Last evaluated {}", format_ago(secs))
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
pub fn build_governance_card(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
    let Some(snap) = snap else {
        return String::new();
    };
    let g = &snap.governance;

    // Retire the dormant MAD-governance card on default nodes. The MAD-based
    // `GovernanceManager` is default-Off (see `GovernanceConfig` in
    // contract/governance.rs) and is being replaced by demand-driven eviction
    // (#4296, #4642); on a default node it only ever renders "Governance is
    // off", so hide the card entirely unless an operator has explicitly enabled
    // governance (DryRun/Enforce). Live retention is surfaced by the
    // demand-driven eviction card instead. See .claude/rules/hosting-invariants.md
    // (retention is now demand-driven, not MAD cost/benefit outlier detection).
    if matches!(g.mode, network_status::GovernanceModeSnapshot::Off) {
        return String::new();
    }

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
            format_last_evaluated(secs)
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
        // NOTE: this branch is only reachable when governance is explicitly
        // enabled (DryRun/Enforce) — the default `Off` mode returns an empty
        // card above, so the ramp-up / "scoring activates" copy below is only
        // shown to an operator who opted in.
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

/// Build the demand-driven eviction card (piece A, #4642). Surfaces the
/// capability-relative RAM budget and the per-contract Greedy-Dual
/// `keep_score` that ACTUALLY governs retention today — the mechanism that
/// replaced the dormant MAD `GovernanceManager` (#4296), whose dashboard card
/// is now hidden on default nodes. Every value comes from
/// `Ring::dashboard_hosting_snapshot` reading the canonical hosting cache;
/// nothing is invented at render time. See `.claude/rules/hosting-invariants.md`.
///
/// Hidden when the node hosts nothing (a fresh/idle node) — the budget still
/// exists but there's nothing to retain, so the panel would just be noise.
pub fn build_hosting_card(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
    let Some(snap) = snap else {
        return String::new();
    };
    let h = &snap.hosting;
    if h.contract_count == 0 {
        return String::new();
    }

    let budget = h.budget_bytes.max(1);
    let used_pct = (h.used_bytes as f64 / budget as f64 * 100.0).min(100.0);
    let headroom = h.budget_bytes.saturating_sub(h.used_bytes);

    // Recently-read evictions are the miscalibration alarm (#4338): evicting a
    // repeatedly-requested contract means the demand estimate is mis-ordering
    // the working set. Color it when non-zero so an operator notices.
    let recently_read_value = if h.evictions_of_recently_read_total > 0 {
        format!(
            r#"<span style="color: var(--danger, #c0392b);">{}</span>"#,
            h.evictions_of_recently_read_total
        )
    } else {
        "0".to_string()
    };

    // Per-contract table, bounded. Rows arrive in EVICTION order (lowest
    // keep-score first); show at most MAX_ROWS. The tile carries the full count.
    //
    // The "next to evict" badge marks the first EVICTION-ELIGIBLE row, not
    // simply the lowest keep-score row: the over-budget sweep SKIPS contracts
    // that are within `min_ttl` or still in use (`should_retain`), so the
    // lowest-score row may be one the cache would never actually evict. Badging
    // it would mislabel an eviction-exempt contract — exactly the "dashboard
    // misleads the operator" problem this card exists to fix. When nothing is
    // currently eligible (every hosted contract is within-TTL / in use), no row
    // is badged.
    const MAX_ROWS: usize = 20;
    let next_victim_idx = h.contracts.iter().position(|c| c.eviction_eligible);
    let mut rows = String::new();
    for (i, c) in h.contracts.iter().take(MAX_ROWS).enumerate() {
        let next_badge = if Some(i) == next_victim_idx {
            r#" <span class="hz-badge hz-next" title="Lowest keep-score among eviction-eligible contracts (past min-TTL and not in use) — the over-budget sweep would evict this one first.">next to evict</span>"#
        } else {
            ""
        };
        rows.push_str(&format!(
            r#"<tr><td title="{full}" data-sort="{full}"><code>{short}</code><button type="button" class="copy-key" data-copy="{full}" title="Copy contract key" aria-label="Copy contract key">⧉</button>{next}</td><td class="right" data-sort="{keep:.6}">{keep:.3}</td><td class="right" data-sort="{demand:.6}">{demand:.3}</td><td class="right" data-sort="{size}">{size_fmt}</td><td class="right" data-sort="{reads}">{reads}</td></tr>"#,
            full = html_escape(&c.key_full),
            short = html_escape(&c.key_short),
            next = next_badge,
            keep = c.keep_score,
            demand = c.predicted_demand,
            size = c.size_bytes,
            size_fmt = format_bytes(c.size_bytes),
            reads = c.read_count,
        ));
    }
    let shown = (h.contracts.len()).min(MAX_ROWS);
    let footer = if (h.contract_count as usize) > shown {
        format!(
            r#"<p class="empty" style="margin: 0.4rem 0.9rem 0.6rem; font-size: 0.78rem; color: var(--text-muted, #888);">Showing the {shown} most-evictable of {total} hosted contracts (lowest keep-score first).</p>"#,
            shown = shown,
            total = h.contract_count,
        )
    } else {
        String::new()
    };

    format!(
        r##"<div class="card">
            <div class="card-header"><h2>Demand-driven eviction</h2><span class="g-mode g-mode-enforce">piece A</span></div>
            <p class="empty" style="margin: 0.2rem 0.9rem 0.4rem; font-size: 0.82rem; color: var(--text-muted, #888);">Retention is demand-driven: contracts with the lowest predicted read-demand (keep-score) are evicted first when over the RAM budget.</p>
            <div class="g-verdict-row">
                <div class="g-norms">
                    <div class="g-norm"><div class="g-norm-label">RAM used</div><div class="g-norm-value">{used} / {budget} ({pct:.0}%)</div></div>
                    <div class="g-norm"><div class="g-norm-label">Headroom</div><div class="g-norm-value">{headroom}</div></div>
                    <div class="g-norm"><div class="g-norm-label">Hosted</div><div class="g-norm-value">{count}</div></div>
                    <div class="g-norm"><div class="g-norm-label">Budget evictions</div><div class="g-norm-value">{budget_evictions}</div></div>
                    <div class="g-norm"><div class="g-norm-label">Evicted w/ demand</div><div class="g-norm-value">{recently_read}</div></div>
                </div>
            </div>
            <div class="table-wrap">
                <table class="sortable" data-table-id="hosting">
                    <thead><tr><th data-sort-type="text">Contract</th><th class="right" data-sort-type="num">Keep-score</th><th class="right" data-sort-type="num">Demand</th><th class="right" data-sort-type="num">Size</th><th class="right" data-sort-type="num">Reads</th></tr></thead>
                    <tbody>{rows}</tbody>
                </table>
            </div>
            {footer}
        </div>"##,
        used = format_bytes(h.used_bytes),
        budget = format_bytes(h.budget_bytes),
        pct = used_pct,
        headroom = format_bytes(headroom),
        count = h.contract_count,
        budget_evictions = h.budget_evictions_total,
        recently_read = recently_read_value,
        rows = rows,
        footer = footer,
    )
}

/// Contract ban-list card (#4302). Surfaces the canonical
/// `Ring::contract_ban_list` so operators can see whether the Phase 7
/// hardening mechanism is catching abusers or sitting idle.
///
/// Two concrete asks from the issue: a count tile ("N contracts on ban
/// list") and a per-entry list (key, reason, time remaining). The
/// governance-state-machine drill-down is deferred (the ban list stores
/// only the current entry, not the transition history that led to it).
///
/// The card is rendered whenever a snapshot exists — including when the
/// list is empty — so an idle-but-active mechanism is distinguishable
/// from one that isn't wired. Every value here comes from the ban list's
/// own accessors via the provider closure; nothing is invented at render
/// time.
pub fn build_ban_list_card(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
    let Some(snap) = snap else {
        return String::new();
    };
    let b = &snap.ban_list;

    let plural = |n: usize| if n == 1 { "contract" } else { "contracts" };

    // Capacity-rejection note: only shown when non-zero, so the common
    // case stays uncluttered. A non-zero value is the operator's signal
    // that the bounded list (MAX_BANNED_CONTRACTS) is overflowing.
    let capacity_note = if b.capacity_rejected_total > 0 {
        format!(
            r#"<p class="empty" style="margin: 0 0.9rem 0.6rem; font-size: 0.82rem; color: var(--danger, #c0392b);">{n} ban{s} rejected — list at capacity.</p>"#,
            n = b.capacity_rejected_total,
            s = if b.capacity_rejected_total == 1 {
                ""
            } else {
                "s"
            },
        )
    } else {
        String::new()
    };

    let body = if b.entries.is_empty() {
        r#"<p class="empty" style="margin: 0.6rem 0.9rem;">No contracts banned. The mechanism is active and currently idle.</p>"#
            .to_string()
    } else {
        let mut rows = String::new();
        for e in &b.entries {
            let reason_txt = match e.reason {
                // The MAD auto-ban path is dormant (governance default-Off,
                // being replaced by demand-driven eviction — #4296/#4642), so
                // in practice only the operator path produces live entries.
                network_status::BanReasonSnapshot::AutoMad => "auto (legacy governance, dormant)",
                network_status::BanReasonSnapshot::Operator => "operator",
            };
            let remaining = if e.expires_in_secs == 0 {
                "lifting".to_string()
            } else {
                format!("{} left", format_duration(e.expires_in_secs))
            };
            // html_escape the contract id even though instance ids are
            // base58 (no HTML metacharacters today): defense-in-depth so
            // the row stays safe if the id format ever changes.
            write!(
                rows,
                r#"<tr><td class="mono">{id}</td><td>{reason}</td><td>{remaining}</td></tr>"#,
                id = html_escape(&e.instance_id),
                reason = reason_txt,
                remaining = html_escape(&remaining),
            )
            .ok();
        }
        format!(
            r#"<table class="data-table">
                <thead><tr><th>Contract</th><th>Reason</th><th>Expires</th></tr></thead>
                <tbody>{rows}</tbody>
            </table>"#
        )
    };

    format!(
        r##"<div class="card">
            <div class="card-header"><h2>Contract Ban List</h2></div>
            <div class="g-verdict-row">
                <div class="g-norms">
                    <div class="g-norm"><div class="g-norm-label">On ban list</div><div class="g-norm-value">{count}</div></div>
                </div>
                <p class="empty" style="margin: 0; padding: 0.4rem 0.9rem; font-size: 0.9rem;">{count} {n_word} currently banned at this node.</p>
            </div>
            {capacity_note}
            {body}
        </div>"##,
        count = b.count,
        n_word = plural(b.count),
        capacity_note = capacity_note,
        body = body,
    )
}

pub fn build_contracts_card(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
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
        let last_update_sort = c.last_updated_secs.unwrap_or(u64::MAX);
        // Freshness / demand cell (replaces the retired MAD-governance
        // column). `is_receiving_updates` is the REAL per-contract freshness
        // signal — only an active subscription keeps the cached state current
        // (is_hosting is NOT a freshness signal, PR #3699). `in_use` shows
        // whether real demand (a local client or a downstream subscriber)
        // pins the contract, vs. a network-only subscription with no reader.
        let (fresh_class, fresh_label) = if c.is_receiving_updates {
            ("fresh-ok", "fresh")
        } else {
            ("fresh-stale", "stale")
        };
        let (use_class, use_label) = if c.in_use {
            ("use-active", "in use")
        } else {
            ("use-idle", "idle")
        };
        // Sort healthiest-first: freshness weighs more than in-use demand.
        let fresh_sort = (c.is_receiving_updates as u8) * 2 + (c.in_use as u8);
        rows.push_str(&format!(
            r#"<tr><td title="{full}" data-sort="{full}"><code>{short}</code><button type="button" class="copy-key" data-copy="{full}" title="Copy contract key" aria-label="Copy contract key">⧉</button></td><td data-sort="{fresh_sort}"><span class="fresh-pill {fresh_class}">{fresh_label}</span> <span class="fresh-pill {use_class}">{use_label}</span></td><td data-sort="{sub_secs}">{subscribed}</td><td data-sort="{last_sort}">{last_update}</td></tr>"#,
            full = html_escape(&c.key_full),
            short = html_escape(&c.key_short),
            fresh_sort = fresh_sort,
            fresh_class = fresh_class,
            fresh_label = fresh_label,
            use_class = use_class,
            use_label = use_label,
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
                    <thead><tr><th data-sort-type="text">Contract</th><th data-sort-type="num">Freshness</th><th data-sort-type="num">Subscribed</th><th data-sort-type="num">Last Update</th></tr></thead>
                    <tbody>{rows}</tbody>
                </table>
            </div>
        </div>"#,
        rows = rows,
    )
}

pub fn build_ops_card(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
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
