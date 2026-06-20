use super::assets::{CSS, JS, PEER_CSS};
use super::cards::format_bytes;
use super::estimator::{
    build_estimator_chart_or_placeholder, build_renegade_accuracy_panel, failure_chart_y_max,
    fmt_prediction_prob, fmt_prediction_speed, fmt_prediction_time,
};
use super::*;

// ─── Peer detail page ────────────────────────────────────────────────────────

pub fn peer_detail_html(address_str: &str) -> String {
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
                <p class="empty" style="font-size: 0.8em; margin-top: 0.25em;"><a href="https://github.com/sanity/renegade" target="_blank" rel="noopener noreferrer" class="ext-link">Renegade</a> is a zero-configuration k-nearest-neighbours model (it auto-selects K and learns which features matter). It predicts per-peer, per-contract outcomes from four features (peer, contract location, distance, time); the router blends its prediction with the distance-based estimate (a weighted average, Renegade's share growing with data up to half) to catch patterns distance alone misses, such as a peer that drops requests for specific contracts.</p>
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

            // Get curves + raw scatter points for this tab
            #[allow(clippy::type_complexity)]
            let (
                f_curve,
                f_range,
                f_points,
                rt_curve,
                rt_range,
                rt_points,
                xfer_curve,
                xfer_range,
                xfer_points,
                event_count,
            ): (
                &[(f64, f64)],
                (f64, f64),
                &[(f64, f64)],
                &[(f64, f64)],
                (f64, f64),
                &[(f64, f64)],
                &[(f64, f64)],
                (f64, f64),
                &[(f64, f64)],
                usize,
            ) = if tab_name == "All" {
                (
                    rs.failure_curve.as_slice(),
                    rs.failure_data_range,
                    rs.failure_points.as_slice(),
                    rs.response_time_curve.as_slice(),
                    rs.response_time_data_range,
                    rs.response_time_points.as_slice(),
                    rs.transfer_rate_curve.as_slice(),
                    rs.transfer_rate_data_range,
                    rs.transfer_rate_points.as_slice(),
                    rs.failure_events,
                )
            } else if let Some(c) = rs.per_op_curves.get(tab_name) {
                (
                    c.failure_curve.as_slice(),
                    c.failure_data_range,
                    c.failure_points.as_slice(),
                    c.response_time_curve.as_slice(),
                    c.response_time_data_range,
                    c.response_time_points.as_slice(),
                    c.transfer_rate_curve.as_slice(),
                    c.transfer_rate_data_range,
                    c.transfer_rate_points.as_slice(),
                    c.failure_events,
                )
            } else {
                (
                    &[][..],
                    (0.0, 0.0),
                    &[][..],
                    &[][..],
                    (0.0, 0.0),
                    &[][..],
                    &[][..],
                    (0.0, 0.0),
                    &[][..],
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
                // Failure probabilities are tiny, so a fixed 0.0–1.0 axis
                // squashes the curve flat against the bottom. Zoom the top of
                // the axis to 2x the curve's value at the right edge of the
                // plot so the line is legible. See failure_chart_y_max.
                let fail_y_max =
                    failure_chart_y_max(f_curve, if tab_name == "All" { fail_adj } else { None })
                        .to_string();
                panel_content.push_str(&build_estimator_chart_or_placeholder(
                    "Failure Probability",
                    f_curve,
                    f_points,
                    f_range,
                    if tab_name == "All" { fail_adj } else { None },
                    ploc,
                    "0.0",
                    &fail_y_max,
                    "No success/failure observations have routed through this peer yet.",
                ));
                panel_content.push_str(&build_estimator_chart_or_placeholder(
                    "Response Time (s)",
                    rt_curve,
                    rt_points,
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
                    xfer_points,
                    xfer_range,
                    if tab_name == "All" { xfer_adj } else { None },
                    ploc,
                    // Transfer rate is a positive B/s value: floor at 0, auto-scale
                    // the top. (Was "auto"/"0", which clamped the max to 0 and gave a
                    // degenerate inverted range that also hid the scatter overlay.)
                    "0",
                    "auto",
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
                <h2>Outcomes vs Distance</h2>
                <p style="font-size:0.8em;color:var(--text-muted);">
                    Actual observed outcomes (dots) against ring distance to the contract, with the
                    isotonic fit overlaid (the All tab is the aggregate the router uses; per-op tabs
                    just break it down and are not consulted separately). "Peer-adjusted" adds this
                    peer's running EWMA correction to that fit. How tightly the dots hug a monotonic
                    curve shows how well distance alone predicts the outcome. A separate Renegade
                    model is blended into the final estimate; its accuracy is in the Prediction
                    Accuracy panel below.
                </p>
                <p class="chart-legend">
                    <span class="chart-key"><span class="chart-dot chart-dot-actual"></span> Actual outcomes</span>
                    <span class="chart-key"><span class="chart-dot chart-dot-global"></span> Isotonic fit</span>
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

    // Build the renegade prediction-accuracy panel (failure + timing models)
    let renegade_chart = if let Some(ref rs) = router_snapshot {
        build_renegade_accuracy_panel(
            &rs.renegade_accuracy_pairs,
            rs.renegade_brier_score,
            &rs.renegade_response_time_pairs,
            &rs.renegade_transfer_speed_pairs,
        )
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
