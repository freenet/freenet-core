use super::*;

/// Format a success share so the ROUNDING never asserts something false.
///
/// `{:.0}` alone renders 199/200 as "100%" and 1/200 as "0%". Both are lies of
/// exactly the kind this panel exists to remove: "100% answered" when a
/// request failed is the same shape of absolute, unearned claim as "Node is
/// healthy" was. An operator reading 100% will stop looking.
///
/// So the two absolute values are reserved for the cases that genuinely earn
/// them, and the bands next to them say which side of the boundary they are
/// on rather than rounding across it.
fn answered_share(ok: u32, total: u32) -> String {
    // Defence in depth. The only caller gates on `total >= MIN_SAMPLE`, so
    // zero cannot reach here today — but without this the `ok == total` arm
    // below would answer "100%" for nothing at all, which is the worst
    // possible wrong answer from a panel whose entire purpose is not
    // overstating success. A future caller that forgets the guard should get
    // an honest dash, not a perfect score.
    if total == 0 {
        return "—".to_string();
    }
    if ok == total {
        return "100%".to_string();
    }
    if ok == 0 {
        return "0%".to_string();
    }
    // Inspect what would ACTUALLY be displayed rather than reasoning about
    // where the boundary falls. Comparing against 99.5 / 0.5 by hand gets the
    // exact-half case wrong — 1 of 200 is precisely 0.5%, and Rust's `{:.0}`
    // rounds half to even, so it renders "0" while a hand-written `pct < 0.5`
    // does not catch it. Formatting first removes the second guess.
    let pct = (ok as f64 / total as f64) * 100.0;
    let rendered = format!("{pct:.0}");
    match rendered.as_str() {
        // Would display as an absolute, but the counts say otherwise.
        "100" => ">99%".to_string(),
        "0" => "<1%".to_string(),
        other => format!("{other}%"),
    }
}

/// Contracts read successfully, as a measured rate rather than a verdict.
///
/// Deliberately reports the LIFETIME rate with the period it covers, not a
/// recent window, and that is a data constraint rather than a shortcut.
/// Measured on three live nodes: a hosted-mode peer did 101 GETs in 17h55m
/// (~5.6/hour), a gateway 7 in 28m (~15/hour), and a third none at all. A
/// fifteen-minute window would hold one to four requests, and even an hour
/// holds about six, where a single failure moves the number seventeen points.
/// A window short enough to mean "now" is empty almost all the time, so it
/// would report nothing far more often than it reported anything.
///
/// The cost of using lifetime is real and worth naming: a node broken early
/// and fine since shows a blended figure. The uptime is printed alongside so
/// the reader can see what the number covers, and `record_op_result` keeps no
/// history that would allow better.
fn build_get_success_line(snap: &network_status::NetworkStatusSnapshot) -> String {
    let (ok, failed) = snap.op_stats.gets;
    let total = ok.saturating_add(failed);
    let period = format_duration(snap.elapsed_secs);

    // Below this, a percentage is theatre: at one or two requests it swings by
    // fifty points per outcome. Show the counts and say why there is no rate,
    // rather than printing a number that looks like a measurement.
    const MIN_SAMPLE: u32 = 20;

    let body = if total == 0 {
        "<span class=\"gsr-none\">none yet</span>".to_string()
    } else if total < MIN_SAMPLE {
        format!(
            r#"<span class="gsr-none">too few to rate</span> <span class="gsr-detail">{ok} of {total} answered in {period}</span>"#
        )
    } else {
        format!(
            r#"<span class="gsr-value">{share} answered</span> <span class="gsr-detail">{ok} of {total} &middot; since start {period}</span>"#,
            share = answered_share(ok, total),
        )
    };

    // A caveat that is always shown, never conditional on the number being
    // low. Two reasons it has to be visible rather than a tooltip.
    //
    // First, an unanswered GET is frequently the NETWORK failing to route,
    // not this node failing to serve — dead-ends dominate the not-found mode
    // today. A bare "GET success 1%" invites the operator to conclude their
    // own node is broken and report it, which is a support burden built out
    // of our own phrasing.
    //
    // Second, showing the caveat only when the figure looks bad would be a
    // threshold in disguise, and choosing that threshold is exactly the
    // judgement this panel exists to avoid making. So it is unconditional,
    // and it says "not by itself" rather than "not your fault", because on a
    // node with no connections it genuinely is this node.
    // Plain text, not a <span>. An earlier draft wrapped this in
    // `class="gsr-caveat"`, which no stylesheet rule ever matched — a dead
    // class name reads as if it carries styling and invites someone to
    // "restore" formatting that never existed. The wrapping <p> is styled.
    let caveat = "Unanswered includes requests the network could not route, so a low share does not by itself mean this node is faulty.";

    format!(
        r#"<p class="get-success-rate" title="Of the GET requests this node has issued, the share that came back with contract state, over the whole time it has been running. Not a recent window: peers issue only a handful of GETs an hour, so a short window would nearly always be empty. A request that dead-ends in the network counts as unanswered.">
            <span class="gsr-label">GET requests</span> {body}
        </p>
        <p class="get-success-caveat">{caveat}</p>"#
    )
}

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
            // Deliberately NOT a verdict. This used to read "Node is healthy",
            // with a tick, on the strength of four connectivity inputs that
            // say nothing about whether the node can actually serve reads —
            // four live v0.2.128 peers displayed it while answering between
            // 1.3% and 89% of their GETs (#5370).
            //
            // A verdict is an assertion, and asserting things that are not
            // true is this page's recurring failure. State the connection
            // count, which is a fact, and let the measured GET rate below
            // speak for whether the node is working.
            let n = snap.open_connections;
            let label = if n == 1 { "peer" } else { "peers" };
            format!(
                r#"<div class="health-banner health-good">
                    <span class="health-icon">&#x25CF;</span>
                    <span>Connected to {n} {label}</span>
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

    let get_success = build_get_success_line(snap);

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
        || snap.ring_stats.updates_capacity_evicted > 0
        || snap.ring_stats.updates_sender_budget_dropped > 0
        || snap.ring_stats.updates_sender_budget_unmetered > 0
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
            <div class="metric-tile">
                <span class="metric-value">{capacity_evicted}</span>
                <span class="metric-label">Capacity-evicted</span>
            </div>
            <div class="metric-tile">
                <span class="metric-value">{sender_budget_dropped}</span>
                <span class="metric-label">Fresh-id-dropped</span>
            </div>
            <div class="metric-tile">
                <span class="metric-value">{sender_budget_unmetered}</span>
                <span class="metric-label">Fresh-id-unmetered</span>
            </div>
        </div>"#,
            accepted = snap.ring_stats.updates_accepted,
            rate_limited = snap.ring_stats.updates_rate_limited,
            capacity_dropped = snap.ring_stats.updates_capacity_dropped,
            capacity_evicted = snap.ring_stats.updates_capacity_evicted,
            sender_budget_dropped = snap.ring_stats.updates_sender_budget_dropped,
            sender_budget_unmetered = snap.ring_stats.updates_sender_budget_unmetered,
        )
    } else {
        String::new()
    };

    // Contract-handler queue occupancy (#4917). Every client operation passes
    // through this queue on its way to the single-threaded WASM executor, so a
    // sustained backlog here is what a user experiences as a slow or
    // timed-out request — the #4912 failure mode, where a client PUT waited
    // ~2 minutes behind it with no way to see the queue at all.
    //
    // Hidden while the queue has never been used AND has never rejected
    // anything, so an idle node shows nothing. `high_water` is the load-bearing
    // number: this page is polled, and a burst between two polls would leave
    // no trace in the instantaneous depth.
    let fair_queue_html = if snap.fair_queue.high_water > 0
        || snap.fair_queue.rejected_global_capacity > 0
        || snap.fair_queue.rejected_per_contract > 0
    {
        format!(
            r#"<div class="metrics-row">
            <div class="metric-tile">
                <span class="metric-value">{depth}</span>
                <span class="metric-label">Queue depth</span>
            </div>
            <div class="metric-tile">
                <span class="metric-value">{high_water}</span>
                <span class="metric-label">Peak depth</span>
            </div>
            <div class="metric-tile">
                <span class="metric-value">{rejected_capacity}</span>
                <span class="metric-label">Queue-full rejects</span>
            </div>
            <div class="metric-tile">
                <span class="metric-value">{rejected_contract}</span>
                <span class="metric-label">Contract-cap rejects</span>
            </div>
            <div class="metric-tile">
                <span class="metric-value">{shed}</span>
                <span class="metric-label">Background shed</span>
            </div>
        </div>"#,
            depth = snap.fair_queue.depth_total,
            high_water = snap.fair_queue.high_water,
            // Shown as its own tile rather than folded into the total: the
            // whole point of splitting the two causes is that node-wide
            // backpressure and one contract hitting its own cap call for
            // different responses. Summing them would put the misdiagnosis
            // this split exists to prevent back on the dashboard.
            rejected_capacity = snap.fair_queue.rejected_global_capacity,
            rejected_contract = snap.fair_queue.rejected_per_contract,
            shed = snap.fair_queue.background_shed,
        )
    } else {
        String::new()
    };

    // Nearest-neighbor ring lattice completeness. A peer with BOTH a successor
    // (closest-higher) and predecessor (closest-lower) ring edge has the base
    // lattice greedy routing needs. Shown once the node has any connections.
    let lattice_html = if snap.open_connections > 0 {
        let mark = |held: bool, dist: Option<f64>| -> String {
            match (held, dist) {
                (true, Some(d)) => format!("yes ({d:.4})"),
                (true, None) => "yes".to_string(),
                (false, _) => "no".to_string(),
            }
        };
        let (probes, improvements) = (
            snap.ring_stats.lattice_probes_issued,
            snap.ring_stats.lattice_probe_improvements,
        );
        format!(
            r#"<div class="metrics-row">
            <div class="metric-tile">
                <span class="metric-value">{succ}</span>
                <span class="metric-label">Lattice successor</span>
            </div>
            <div class="metric-tile">
                <span class="metric-value">{pred}</span>
                <span class="metric-label">Lattice predecessor</span>
            </div>
            <div class="metric-tile">
                <span class="metric-value">{improvements}/{probes}</span>
                <span class="metric-label">Discovery hits/probes</span>
            </div>
        </div>"#,
            succ = mark(
                snap.ring_stats.lattice_has_successor,
                snap.ring_stats.lattice_successor_distance
            ),
            pred = mark(
                snap.ring_stats.lattice_has_predecessor,
                snap.ring_stats.lattice_predecessor_distance
            ),
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
            {get_success}
            {ring_stats_html}
            {lattice_html}
            {rate_limit_html}
            {fair_queue_html}
            {external_addr_html}
            {spinner}
            {gateway_warning}
            {nat_html}
            {failures_html}
        </div>"#,
        health_banner = health_banner,
        get_success = get_success,
        ring_stats_html = ring_stats_html,
        lattice_html = lattice_html,
        rate_limit_html = rate_limit_html,
        fair_queue_html = fair_queue_html,
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

    // Only render the average when something actually completed.
    // `avg_transfer_time_ms` is a sentinel 0 when `transfers_completed == 0`
    // (metrics.rs guards the division), so an all-failures window would
    // otherwise render "0 ok / 3 fail (0.000s avg)" — presenting the sentinel
    // as a measured timing. That window became reachable when #4827 gave
    // `transfers_failed` a writer: before that the counter was structurally 0,
    // so this row only ever rendered with >=1 completion and the avg was always
    // real.
    let avg_str = if ts.transfers_completed > 0 {
        format!(
            r#" <span class="transfer-detail">({avg}s avg)</span>"#,
            avg = format_args!("{:.3}", ts.avg_transfer_time_ms as f64 / 1000.0),
        )
    } else {
        String::new()
    };

    let xfer_str = if ts.transfers_completed > 0 || ts.transfers_failed > 0 {
        format!(
            r#"<div class="transfer-stat">
                <span class="transfer-label">Transfers (avg time)</span>
                <span class="transfer-value">{ok} ok / {fail} fail{avg_str}</span>
            </div>"#,
            ok = ts.transfers_completed,
            fail = ts.transfers_failed,
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
            {filter_controls}
            <div class="table-wrap">
                <table class="sortable" data-table-id="peers">
                    <thead><tr><th data-sort-type="text">Address</th><th data-sort-type="num">Location</th><th data-sort-type="text">Type</th><th data-sort-type="num">Sent</th><th data-sort-type="num">Recv</th><th data-sort-type="num">Connected</th></tr></thead>
                    <tbody>{rows}</tbody>
                </table>
            </div>
        </div>"#,
        own_loc = own_loc,
        ring_svg = ring_svg,
        filter_controls = table_filter_controls("peers", "peers"),
        rows = rows,
    )
}

/// Filter + collapse controls for a long table.
///
/// The peers and subscribed-contracts tables are unbounded — a production
/// gateway rendered 210 peer rows, 70% of an 11,780px page, with no way to
/// locate one row. The rows are still all rendered (truncating server-side
/// would make the filter lie about what it searched); `dashboard.js`
/// collapses the table to a readable default and expands it while a query is
/// active.
///
/// Rendered unconditionally rather than only past a threshold, so the control
/// does not appear and disappear as a node's peer count crosses a boundary.
/// The JS hides the toggle itself when there is nothing to collapse.
fn table_filter_controls(table_id: &str, label: &str) -> String {
    format!(
        r#"<div class="table-filter" data-filter-for="{id}">
            <input type="search" class="tf-input" placeholder="Filter {label}…" aria-label="Filter {label}" autocomplete="off" spellcheck="false">
            <span class="tf-status" role="status" aria-live="polite"></span>
            <button type="button" class="tf-toggle" hidden aria-expanded="false"></button>
        </div>"#,
        id = html_escape(table_id),
        label = html_escape(label),
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
    // through the interior. Matches the telemetry.freenet.org ring
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

/// Build the demand-driven eviction card (#4642). Surfaces the
/// capability-relative budgets and the per-contract rows that the
/// subscriber-primary eviction sweep orders. Every value comes from
/// `Ring::dashboard_hosting_snapshot` reading the canonical hosting cache;
/// nothing is invented at render time. See `.claude/rules/hosting-invariants.md`.
///
/// The per-contract table shows `recency_seq`, NOT `keep_score` /
/// `predicted_demand`. The latter two are the demoted telemetry-only
/// Greedy-Dual estimator, which eviction does not read; presenting them as the
/// eviction score described a mechanism retired by the subscriber-primary
/// rework, and left the table sorted by a column it did not display (#4830).
/// `recency_seq` is the ordering input the cache actually sorts these rows by.
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
    // NOT clamped to 100%. Over budget is the eviction trigger itself, so it
    // is a state the operator most needs to see accurately; clamping rendered
    // "150 B / 100 B (100%)", which contradicts its own numerator and hides
    // how far over the node is. (Found when the binding strip below started
    // reporting the true figure and the two disagreed — the clamp here
    // predates this change.)
    let used_pct = h.used_bytes as f64 / budget as f64 * 100.0;
    let headroom = h.budget_bytes.saturating_sub(h.used_bytes);

    // Which limit is actually closest? The card shows three ceilings, and
    // before this they were three flat rows of identical tiles with nothing
    // saying which one binds. On a real low-RAM peer that is not academic: a
    // measured framework node sat at 34% of its state-byte budget, 1% of its
    // disk budget, and 99.2% of its contract-slot ceiling — the one number
    // that mattered, rendered in the same muted grey as the two that had room
    // to spare.
    //
    // Utilisation is computed per axis and the highest wins. A budget of 0 is
    // "not configured / not yet measured", not "completely full", so those
    // axes are skipped rather than reported as 100%. The disk budget is the
    // real case: it is an `Option` because the tracker is unseeded early in a
    // node's life, and ranking an absent denominator as full would report a
    // phantom emergency on every fresh start.
    //
    // For the slot axis this is defensive rather than load-bearing, though the
    // reason is narrower than "the budget is floored": the SETTER
    // (`HostingCache::set_resident_overhead_budget_bytes`) does not clamp, but
    // its only production caller
    // (`HostingManager::recompute_resident_overhead_budget`) passes the output
    // of `resident_overhead_budget_for`, which ends in
    // `.max(MIN_RESIDENT_OVERHEAD_BUDGET_BYTES)` — 128 MiB, i.e. 128 slots.
    // Every other caller is a test. So 0 slots is unreachable in production
    // TODAY, by call-site convention rather than by construction; a future
    // caller that set the budget directly could break that. If a node ever can
    // reach 0 slots, the honest fix is to say so explicitly rather than let
    // this branch quietly imply "fine".
    let axis_utilisation = |used: u64, budget: u64| -> Option<f64> {
        (budget > 0).then(|| used as f64 / budget as f64)
    };
    // Each axis carries a note saying what CROSSING it actually does, because
    // the three are not the same kind of ceiling and "closest limit" alone
    // would imply they are:
    //
    //   - contract state: the sweep's own condition (`current_bytes >
    //     budget_bytes`), so crossing fires it directly.
    //   - contract slots: also a sweep condition, but only once the breach has
    //     been SUSTAINED (`resident_overhead_over_budget` requires half of
    //     RESIDENT_OVERHEAD_SUSTAINED_WINDOW, ~2.5 min). A transient spike to
    //     99% here self-resolves without evicting anything, so the strip must
    //     not read as though eviction is imminent.
    //   - disk: not a sweep condition at all. Its direct consequence is
    //     ADMISSION: `admit_state_write` / `admit_state_update` /
    //     `admit_wasm_write` (disk_usage.rs) reject new growth once the
    //     projected total exceeds the budget. It can additionally tighten the
    //     contract-state limit via `effective = ram.min(disk_budget)`, but
    //     only when the disk budget is the SMALLER of the two — on a typical
    //     node it is not (measured: 1.0 GB RAM budget against 32.0 GB disk),
    //     so saying "filling disk tightens the state limit" would be wrong in
    //     the ordinary case and wrong precisely when writes start failing.
    let mut axes: Vec<(&str, f64, String, &str)> = Vec::new();
    if let Some(u) = axis_utilisation(h.used_bytes, h.budget_bytes) {
        axes.push((
            "contract state",
            u,
            format!(
                "{} of {}",
                format_bytes(h.used_bytes),
                format_bytes(h.budget_bytes)
            ),
            "Crossing this triggers an eviction sweep.",
        ));
    }
    if let (Some(used), Some(disk_budget)) = (h.disk_total_bytes, h.disk_budget_bytes) {
        if let Some(u) = axis_utilisation(used, disk_budget) {
            axes.push((
                "disk",
                u,
                format!("{} of {}", format_bytes(used), format_bytes(disk_budget)),
                "Filling this rejects new writes — the disk admission gates refuse \
                 state and WASM growth once the projected total would exceed the \
                 budget. It does not by itself trigger an eviction sweep. It can \
                 also tighten the contract-state limit, but only when the disk \
                 budget is the smaller of the two, since that limit is \
                 min(RAM budget, disk budget).",
            ));
        }
    }
    if let Some(u) = axis_utilisation(h.contract_count, h.contract_slot_budget) {
        axes.push((
            "contract slots",
            u,
            format!("{} of {}", h.contract_count, h.contract_slot_budget),
            "Crossing this triggers a sweep only if it stays over for a few minutes, \
             so a brief spike here resolves on its own.",
        ));
    }
    // Cost pressure (#4861) is deliberately absent: it is a sustained-rate
    // condition on a single offending contract, not a utilisation ratio, so it
    // has no comparable denominator to rank against these three.
    let binding = axes
        .iter()
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .map(|(name, util, detail, note)| {
            // Over budget is a REAL, reachable state, not an error: exceeding
            // the contract-state budget is the eviction trigger itself, and
            // the slot axis sits over its ceiling for the whole ~2.5 min
            // sustained window before anything is shed. Clamping the
            // percentage rendered that as "150 of 100 (100%)" — a line that
            // contradicts itself and hides the breach magnitude at exactly the
            // moment an operator needs it. So the percentage is unclamped and
            // only the BAR WIDTH is capped, since a bar cannot overflow its
            // track.
            let pct = util * 100.0;
            let bar_pct = pct.min(100.0);
            // Threshold on the DISPLAYED figure, not the raw one. Colouring
            // from `pct` while printing `{pct:.0}` makes the two disagree at
            // the boundary: 89.6% prints as "90%" but renders amber, and 74.6%
            // prints as "75%" but renders grey. An operator seeing a red 90%
            // beside an amber 90% has no way to tell them apart, so round
            // first and threshold on what they can actually read.
            let shown_pct = pct.round();
            // Colour only near the ceiling: an operator should be able to
            // ignore this strip until it means something.
            let tone = if shown_pct >= 90.0 {
                "var(--danger, #c0392b)"
            } else if shown_pct >= 75.0 {
                "var(--warn, #b8860b)"
            } else {
                "var(--text-muted, #888)"
            };
            format!(
                r#"<div class="hz-binding" title="{note}"><div class="hz-binding-head">Closest limit: <strong>{name}</strong> — {detail} ({shown:.0}%)</div><div class="hz-bar" role="img" aria-label="{name} at {shown:.0} percent of its limit"><span class="hz-bar-fill" style="width: {bar_pct:.1}%; background: {tone};"></span></div></div>"#,
                name = html_escape(name),
                detail = html_escape(detail),
                note = html_escape(note),
                shown = shown_pct,
                bar_pct = bar_pct,
                tone = tone,
            )
        })
        .unwrap_or_default();

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

    // Per-contract table, bounded. Rows arrive from the cache already sorted
    // ascending by `(recency_seq, key)` — so `recency_seq` is the column that
    // explains each row's position, and it is the one shown. `keep_score` and
    // `predicted_demand` are the DEMOTED telemetry-only estimator: eviction
    // reads neither, so rendering them here as though they ranked anything
    // described a mechanism that was retired by the subscriber-primary rework
    // (#4830). They are gone from this projection entirely — they survive only
    // on the cache-side `HostedContract`, where they still drive the
    // Greedy-Dual `eviction_floor` ratchet.
    //
    // Caveat the footer states rather than hides: the cache-side sort is only
    // the ordering WITHIN the zero-subscriber tier. `victim_order` ranks
    // local-subscription count and downstream-subscriber count above recency,
    // and those counts are computed transiently during the sweep — the cache
    // cannot see them, so they are not available to render.
    //
    // The "next to evict" badge marks the first EVICTION-ELIGIBLE row. A
    // contract still in use (a local client / downstream subscriber) is ordered
    // LAST by the sweep — shed only as a last resort when nothing with fewer
    // subscribers is eligible — so it is not the common-case next victim.
    // `is_eviction_eligible` reflects that (not in use; there is no longer a
    // `min_ttl` age gate). When nothing is currently eligible (every hosted
    // contract is in use), no row is badged.
    const MAX_ROWS: usize = 20;
    let next_victim_idx = h.contracts.iter().position(|c| c.eviction_eligible);
    let mut rows = String::new();
    for (i, c) in h.contracts.iter().take(MAX_ROWS).enumerate() {
        let next_badge = if Some(i) == next_victim_idx {
            r#" <span class="hz-badge hz-next" title="First eviction-eligible contract (not pinned by a local client or downstream subscriber) in the sweep's order — the over-budget sweep would evict this one first.">next to evict</span>"#
        } else {
            ""
        };
        // `recency_seq` is a per-run monotonic sequence, not a timestamp, and
        // it is the EVICTION recency clock rather than a pure last-read time:
        // `record_abandonment` also stamps a fresh seq when a contract loses
        // its last subscriber, deliberately granting a grace period so a
        // just-unsubscribed contract is not evicted on a stale read accrued
        // while it sat in the subscription tier. Labelling this column "last
        // access" would therefore be wrong. 0 means the clock has not been set
        // since this process started (including every entry reloaded from disk
        // at startup), which is why so many rows read "never" after a restart.
        let recency = if c.recency_seq == 0 {
            r#"<span style="color: var(--text-muted, #888);">never</span>"#.to_string()
        } else {
            c.recency_seq.to_string()
        };
        // A contract pinned by a local client or downstream subscriber is
        // ordered LAST by the real sweep, but the cache-side sort this table
        // shows cannot see subscriber counts — so a pinned, never-read contract
        // lands at the top reading "never", looking exactly like the most
        // evictable row. Mark it, or the ordering caveat in the footer is the
        // only thing standing between the operator and the wrong conclusion.
        let pin_badge = if c.eviction_eligible {
            ""
        } else {
            r#" <span class="fresh-pill use-active" title="Pinned by a local client or downstream subscriber. The sweep evicts these last, regardless of this row's position.">in use</span>"#
        };
        rows.push_str(&format!(
            r#"<tr><td title="{full}" data-sort="{full}"><a href="/contract/{full}" class="key-link"><code>{short}</code></a><button type="button" class="copy-key" data-copy="{full}" title="Copy contract key" aria-label="Copy contract key">⧉</button>{next}{pin}</td><td class="right" data-sort="{seq}">{recency}</td><td class="right" data-sort="{size}">{size_fmt}</td><td class="right" data-sort="{reads}">{reads}</td></tr>"#,
            full = html_escape(&c.key_full),
            short = html_escape(&c.key_short),
            next = next_badge,
            pin = pin_badge,
            seq = c.recency_seq,
            recency = recency,
            size = c.size_bytes,
            size_fmt = format_bytes(c.size_bytes),
            reads = c.read_count,
        ));
    }
    let shown = (h.contracts.len()).min(MAX_ROWS);
    let footer = if (h.contract_count as usize) > shown {
        format!(
            r#"<p class="empty" style="margin: 0.4rem 0.9rem 0.6rem; font-size: 0.78rem; color: var(--text-muted, #888);">Showing {shown} of {total} hosted contracts, lowest eviction-recency first. That clock is set by a real GET or PUT and also when a contract loses its last subscriber, so it is not purely a last-read time. Contracts with a local client or downstream subscriber outrank recency and are evicted last, but those counts aren't available here — so this is the eviction order only among contracts with no subscribers.</p>"#,
            shown = shown,
            total = h.contract_count,
        )
    } else {
        String::new()
    };

    // On-disk usage tiles (#4683/#4702 follow-up): `None` renders "measuring…"
    // so an unseeded disk tracker (early startup) is visually distinct from
    // genuine zero usage, mirroring the `hosting_disk_*` telemetry gate.
    const MEASURING: &str = "measuring…";
    let disk_used_value = match h.disk_total_bytes {
        Some(total) => format_bytes(total),
        None => MEASURING.to_string(),
    };
    let disk_breakdown_title = match (
        h.disk_state_bytes,
        h.disk_wasm_bytes,
        h.disk_compile_cache_bytes,
    ) {
        (Some(state), Some(wasm), Some(cache)) => format!(
            "State: {state} · WASM: {wasm} · Compile cache: {cache}",
            state = format_bytes(state),
            wasm = format_bytes(wasm),
            cache = format_bytes(cache),
        ),
        _ => "Disk tracker not yet seeded".to_string(),
    };
    let disk_budget_value = match h.disk_budget_bytes {
        Some(budget) => format_bytes(budget),
        None => MEASURING.to_string(),
    };
    let disk_headroom_value = match (h.disk_total_bytes, h.disk_budget_bytes) {
        (Some(used), Some(budget)) => format_bytes(budget.saturating_sub(used)),
        _ => MEASURING.to_string(),
    };

    // The tile shows slots because that is the unit the ceiling constrains;
    // the tooltip keeps the RAM derivation available, so an operator who wants
    // to know WHY the ceiling is where it is can still get there. Estimated,
    // never measured — say so, since the whole failure this replaces was a
    // derived count reading as a memory measurement.
    let slot_tooltip = format!(
        "A contract-count ceiling, not a memory measurement. Each hosted contract is \
         charged a flat estimate for per-contract bookkeeping (subscriptions, redb/index \
         entries) that the contract-state budget does not count, so the RAM-scaled byte \
         budget behind this (#5325) works out to a maximum number of contracts. \
         Currently {used} estimated against a {budget} ceiling.",
        used = format_bytes(h.estimated_resident_overhead_bytes),
        budget = format_bytes(h.resident_overhead_budget_bytes),
    );

    // Contract-slot tiles (#5325): the state-byte budget above bounds contract
    // STATE bytes only. This axis is the count-derived one that closes the gap
    // where many small-state contracts (negligible impact on the state-byte
    // tile) still exhaust a peer's real resident memory via per-contract
    // subscription/index bookkeeping.
    //
    // Rendered as SLOTS rather than the underlying bytes. The byte pair was
    // `contract_count * 1 MiB` against a RAM-scaled ceiling, which prints as
    // e.g. "520.0 MB / 524.0 MB" and reads as measured memory — it is not
    // measured, and what it constrains is a number of contracts. The tooltip
    // keeps the RAM derivation available for anyone who needs it.

    format!(
        r##"<div class="card">
            <div class="card-header"><h2>Demand-driven eviction</h2></div>
            <p class="empty" style="margin: 0.2rem 0.9rem 0.4rem; font-size: 0.82rem; color: var(--text-muted, #888);">Retention is demand-driven. When over budget the node sheds contracts with the fewest subscribers first — a local client subscription outranks a downstream one, and among contracts with neither, the one with the lowest eviction-recency goes first. A sweep can be triggered by any of several independent pressures: contract state bytes, disk usage, the resident-overhead ceiling that scales with hosted-contract count (#5325), or a single zero-demand contract taking a sustained share of the node's update work (#4861).</p>
            {binding}
            <div class="g-verdict-row">
                <div class="g-norms">
                    <div class="g-norm"><div class="g-norm-label">RAM used</div><div class="g-norm-value">{used} / {budget} ({pct:.0}%)</div></div>
                    <div class="g-norm"><div class="g-norm-label">Headroom</div><div class="g-norm-value">{headroom}</div></div>
                    <div class="g-norm"><div class="g-norm-label">Hosted</div><div class="g-norm-value">{count}</div></div>
                    <div class="g-norm"><div class="g-norm-label">Budget evictions</div><div class="g-norm-value">{budget_evictions}</div></div>
                    <div class="g-norm"><div class="g-norm-label">Evicted w/ demand</div><div class="g-norm-value">{recently_read}</div></div>
                </div>
            </div>
            <div class="g-verdict-row">
                <div class="g-norms">
                    <div class="g-norm" title="{disk_breakdown_title}"><div class="g-norm-label">Disk used</div><div class="g-norm-value">{disk_used}</div></div>
                    <div class="g-norm"><div class="g-norm-label">Disk budget</div><div class="g-norm-value">{disk_budget}</div></div>
                    <div class="g-norm"><div class="g-norm-label">Disk headroom</div><div class="g-norm-value">{disk_headroom}</div></div>
                </div>
            </div>
            <div class="g-verdict-row">
                <div class="g-norms">
                    <div class="g-norm" title="{slot_tooltip}"><div class="g-norm-label">Contract slots used</div><div class="g-norm-value">{slots_used} / {slots_budget}</div></div>
                    <div class="g-norm"><div class="g-norm-label">Slots free</div><div class="g-norm-value">{slots_free}</div></div>
                    <div class="g-norm" title="Evictions where the contract-slot ceiling was the active pressure (#5325). May overlap with budget evictions."><div class="g-norm-label">Slot-pressure evictions</div><div class="g-norm-value">{resident_overhead_evictions}</div></div>
                </div>
            </div>
            <div class="table-wrap">
                <table class="sortable" data-table-id="hosting">
                    <thead><tr><th data-sort-type="text">Contract</th><th class="right" data-sort-type="num" title="The eviction recency clock: a per-run sequence, higher is more recent. Reset by a real GET or PUT, and also when a contract loses its last subscriber — the sweep deliberately gives a just-unsubscribed contract a grace period, so this is NOT purely a last-read time. &quot;never&quot; means the clock has not been set since this node started, which includes everything reloaded from disk at startup. This is the column the eviction sweep orders by among contracts with no subscribers.">Recency</th><th class="right" data-sort-type="num">Size</th><th class="right" data-sort-type="num">Reads</th></tr></thead>
                    <tbody>{rows}</tbody>
                </table>
            </div>
            {footer}
        </div>"##,
        binding = binding,
        used = format_bytes(h.used_bytes),
        budget = format_bytes(h.budget_bytes),
        pct = used_pct,
        headroom = format_bytes(headroom),
        count = h.contract_count,
        budget_evictions = h.budget_evictions_total,
        recently_read = recently_read_value,
        disk_breakdown_title = html_escape(&disk_breakdown_title),
        disk_used = disk_used_value,
        disk_budget = disk_budget_value,
        disk_headroom = disk_headroom_value,
        slot_tooltip = html_escape(&slot_tooltip),
        slots_used = h.contract_count,
        slots_budget = h.contract_slot_budget,
        slots_free = h.contract_slot_budget.saturating_sub(h.contract_count),
        resident_overhead_evictions = h.resident_overhead_evictions_total,
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
            r#"<tr><td title="{full}" data-sort="{full}"><a href="/contract/{full}" class="key-link"><code>{short}</code></a><button type="button" class="copy-key" data-copy="{full}" title="Copy contract key" aria-label="Copy contract key">⧉</button></td><td data-sort="{fresh_sort}"><span class="fresh-pill {fresh_class}">{fresh_label}</span> <span class="fresh-pill {use_class}">{use_label}</span></td><td data-sort="{sub_secs}">{subscribed}</td><td data-sort="{last_sort}">{last_update}</td></tr>"#,
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
            {filter_controls}
            <div class="table-wrap">
                <table class="sortable" data-table-id="contracts">
                    <thead><tr><th data-sort-type="text">Contract</th><th data-sort-type="num">Freshness</th><th data-sort-type="num">Subscribed</th><th data-sort-type="num">Last Update</th></tr></thead>
                    <tbody>{rows}</tbody>
                </table>
            </div>
        </div>"#,
        filter_controls = table_filter_controls("contracts", "contracts"),
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

/// Per-delegate observability card (#5467 Phase 0).
///
/// There was previously ZERO delegate information anywhere on this dashboard.
/// The gap is what let #4669 survive unnoticed: a delegate subscribes, the call
/// returns success, notification delivery works, and nothing anywhere reports
/// that the subscription did not actually pin the contract.
///
/// Three states, deliberately kept distinguishable — the whole point of the
/// panel is that "nothing to report" and "nothing is reporting" must not look
/// the same (the Contract Ban List card, `build_ban_list_card`, is the template
/// for this, and its explicit empty state is the part that was worth copying):
///
/// 1. **Provider unregistered** (`snap.delegates == None`) — the card is not
///    rendered at all, matching every other unwired panel.
/// 2. **Wired, no delegates** — the card renders with an explicit empty state.
/// 3. **Wired, delegates present** — the table.
///
/// Every value comes from the provider closure reading canonical state. The
/// only mirrored numbers are the execution counters, which have no canonical
/// source (see `delegate_observability`), and their write sites are source-pinned.
pub fn build_delegates_card(snap: &Option<network_status::NetworkStatusSnapshot>) -> String {
    let Some(snap) = snap else {
        return String::new();
    };
    // `None` = the provider was never registered. Render nothing rather than an
    // empty table, which would claim this node has no delegates.
    let Some(d) = snap.delegates.as_ref() else {
        return String::new();
    };

    // The headline warning. A non-zero count here IS the #4669 bug, visible.
    let unpinned_note = if d.subscriptions_without_demand > 0 {
        format!(
            r#"<p class="empty" style="margin: 0 0.9rem 0.6rem; font-size: 0.82rem; color: var(--danger, #c0392b);">{n} delegate subscription{s} did not register demand — the contract is not pinned by it, so nothing keeps it hosted or renewed here. Until #4669 lands this is expected for every delegate subscription: <code>contract_in_use</code> has no delegate term, so a subscription only reads as pinned when some other route (usually the app's own WebSocket subscription) happens to pin the same contract.</p>"#,
            n = d.subscriptions_without_demand,
            s = if d.subscriptions_without_demand == 1 {
                ""
            } else {
                "s"
            },
        )
    } else {
        String::new()
    };

    // A runaway delegate that spins until the loop gives up returns Ok, so it
    // never appears in the error count. This is the only trace it leaves.
    let cap_note = if d.iteration_cap_hits_total > 0 {
        format!(
            r#"<p class="empty" style="margin: 0 0.9rem 0.6rem; font-size: 0.82rem; color: var(--danger, #c0392b);">{n} request{s} hit the contract-request iteration cap and returned truncated results. That path returns success, so these do NOT appear in the error column — a delegate spinning until the loop gives up otherwise reads as healthy (#5454).</p>"#,
            n = d.iteration_cap_hits_total,
            s = if d.iteration_cap_hits_total == 1 {
                ""
            } else {
                "s"
            },
        )
    } else {
        String::new()
    };

    // Phase 2 (#3972 scheduled execution) is not built. Say so explicitly: a
    // "0 pending wakeups" tile would read as "none pending", which is a
    // fabricated answer to a question this node cannot answer at all.
    let wakeups_value = if d.wakeup_scheduling_available {
        // Unreachable today; kept so wiring Phase 2 is a one-line change here
        // rather than a rewrite that has to rediscover why the tile said this.
        "—".to_string()
    } else {
        "not built".to_string()
    };

    let body = if d.delegates.is_empty() {
        r#"<p class="empty" style="margin: 0.6rem 0.9rem;">No delegates have run or subscribed on this node. The panel is wired and reporting; this is a genuine zero, not a missing data source.</p>"#
            .to_string()
    } else {
        let mut rows = String::new();
        for e in &d.delegates {
            // Unknown stays "—", never 0. A delegate we have never seen execute
            // has no rate and no last-active time; printing 0 would invent one.
            let dash = "—".to_string();
            let last_active = e
                .last_active_secs_ago
                .map(format_ago)
                .unwrap_or_else(|| dash.clone());
            let avg_call = if e.invocations > 0 {
                format!("{} µs", e.total_exec_micros / e.invocations)
            } else {
                dash.clone()
            };
            let avg_request = if e.requests > 0 {
                format!("{} ms", e.total_request_micros / e.requests / 1000)
            } else {
                dash.clone()
            };
            let max_request = if e.requests > 0 {
                format!("{} ms", e.max_request_micros / 1000)
            } else {
                dash.clone()
            };
            let subs_cell = if e.subscriptions.is_empty() {
                "0".to_string()
            } else {
                format!(
                    "{pinned} / {total}",
                    pinned = e.subscriptions_registering_demand,
                    total = e.subscriptions.len(),
                )
            };
            // A subscription that did not pin is the finding, so colour it.
            let subs_class = if e.subscriptions.is_empty()
                || e.subscriptions_registering_demand == e.subscriptions.len()
            {
                "use-active"
            } else {
                "fresh-stale"
            };
            let error_cell = match (&e.last_error, e.last_error_secs_ago) {
                (Some(msg), Some(secs)) => format!(
                    r#"<span title="{full}">{n} · {ago}</span>"#,
                    full = html_escape(msg),
                    n = e.errors,
                    ago = html_escape(&format_ago(secs)),
                ),
                _ => e.errors.to_string(),
            };
            let cap_cell = if e.iteration_cap_hits > 0 {
                format!(
                    r#"<span class="fresh-stale" title="Requests that exhausted MAX_CONTRACT_REQUEST_ITERATIONS. That arm returns Ok, so these are not counted as errors.">{}</span>"#,
                    e.iteration_cap_hits
                )
            } else {
                "0".to_string()
            };
            write!(
                rows,
                r#"<tr><td class="mono">{key}</td><td class="{subs_class}">{subs}</td><td class="right">{requests}</td><td class="right">{avg_req}</td><td class="right">{max_req}</td><td class="right">{max_iters}</td><td class="right">{cap}</td><td class="right">{invocations}</td><td class="right">{avg_call}</td><td class="right">{errors}</td><td class="right">{last_active}</td></tr>"#,
                key = html_escape(&e.key),
                subs_class = subs_class,
                subs = html_escape(&subs_cell),
                requests = e.requests,
                avg_req = html_escape(&avg_request),
                max_req = html_escape(&max_request),
                max_iters = e.max_iterations,
                cap = cap_cell,
                invocations = e.invocations,
                avg_call = html_escape(&avg_call),
                errors = error_cell,
                last_active = html_escape(&last_active),
            )
            .ok();
        }
        format!(
            r#"<div class="table-wrap">
                <table class="sortable" data-table-id="delegates">
                    <thead><tr>
                        <th data-sort-type="text">Delegate</th>
                        <th title="Subscriptions that actually registered demand, over subscriptions held. A subscription that did not register demand does NOT pin the contract — see the note above.">Pinned / subs</th>
                        <th class="right" data-sort-type="num" title="Whole client requests. One request drives up to 100 delegate invocations, so this is the unit that matches what a user waited for.">Requests</th>
                        <th class="right" data-sort-type="num">Avg req</th>
                        <th class="right" data-sort-type="num">Max req</th>
                        <th class="right" data-sort-type="num" title="Most contract-request loop iterations any single request needed. Approaching 100 means it is close to the cap.">Max iters</th>
                        <th class="right" data-sort-type="num" title="Requests that hit the iteration cap. These return Ok, so they are NOT errors.">Cap hits</th>
                        <th class="right" data-sort-type="num" title="Individual delegate executions (process() calls).">Invocations</th>
                        <th class="right" data-sort-type="num">Avg call</th>
                        <th class="right" data-sort-type="num" title="Failed invocations. Hover for the most recent error message.">Errors</th>
                        <th class="right" data-sort-type="num">Last active</th>
                    </tr></thead>
                    <tbody>{rows}</tbody>
                </table>
            </div>"#
        )
    };

    format!(
        r##"<div class="card">
            <div class="card-header"><h2>Delegates</h2></div>
            <p class="empty" style="margin: 0.2rem 0.9rem 0.4rem; font-size: 0.82rem; color: var(--text-muted, #888);">Per-delegate activity on this node (#5467 Phase 0). Measurement only — nothing here throttles or suspends a delegate. Two units are shown because they differ by up to 100x: a REQUEST is one client call, which drives up to 100 delegate INVOCATIONS, so a healthy per-call time is consistent with a request that took seconds.</p>
            <div class="g-verdict-row">
                <div class="g-norms">
                    <div class="g-norm"><div class="g-norm-label">Delegates</div><div class="g-norm-value">{count}</div></div>
                    <div class="g-norm" title="Subscriptions that actually registered demand, over subscriptions held across all delegates."><div class="g-norm-label">Pinned / subs</div><div class="g-norm-value">{pinned} / {subs}</div></div>
                    <div class="g-norm"><div class="g-norm-label">With unpinned subs</div><div class="g-norm-value">{unpinned_delegates}</div></div>
                    <div class="g-norm" title="Scheduled execution (#3972 / Phase 2) is not merged, so this node cannot have pending wakeups. Shown as 'not built' rather than 0, which would read as 'none pending'."><div class="g-norm-label">Pending wakeups</div><div class="g-norm-value">{wakeups}</div></div>
                </div>
            </div>
            <div class="g-verdict-row">
                <div class="g-norms">
                    <div class="g-norm" title="Compiled delegate WASM modules held in the module cache."><div class="g-norm-label">Module cache</div><div class="g-norm-value">{mc_entries}</div></div>
                    <div class="g-norm"><div class="g-norm-label">Cache used</div><div class="g-norm-value">{mc_used} / {mc_budget}</div></div>
                    <div class="g-norm"><div class="g-norm-label">Cache evictions</div><div class="g-norm-value">{mc_evictions}</div></div>
                </div>
            </div>
            {unpinned_note}
            {cap_note}
            {body}
        </div>"##,
        count = d.delegates.len(),
        pinned = d.subscriptions_total - d.subscriptions_without_demand,
        subs = d.subscriptions_total,
        unpinned_delegates = d.delegates_with_unpinned_subscriptions,
        wakeups = wakeups_value,
        mc_entries = d.module_cache_entries,
        mc_used = format_bytes(d.module_cache_total_bytes),
        mc_budget = format_bytes(d.module_cache_budget_bytes),
        mc_evictions = d.module_cache_evictions_total,
        unpinned_note = unpinned_note,
        cap_note = cap_note,
        body = body,
    )
}
