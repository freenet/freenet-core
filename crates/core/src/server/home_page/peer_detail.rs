use super::assets::{CSS, JS, PEER_CSS};
use super::cards::format_bytes;
use super::estimator::{
    build_estimator_chart_or_placeholder, build_renegade_accuracy_panel, failure_chart_y_max,
    fmt_prediction_prob, fmt_prediction_speed, fmt_prediction_time,
};
use super::*;
use crate::router::AdjustmentMode;

// ─── Peer detail page ────────────────────────────────────────────────────────

/// Render a Brier skill score.
///
/// Skill is the reading that matters and the sign is the whole point, so a
/// negative value is labelled rather than left for the reader to interpret:
/// "worse than assuming nothing" is a specific, actionable statement, and a
/// bare `-0.43` is not.
fn fmt_skill(skill: Option<f64>) -> String {
    match skill {
        Some(value) if value.is_finite() => {
            let label = if value < -0.01 {
                " (worse than assuming nothing)"
            } else if value < 0.01 {
                " (no better than assuming nothing)"
            } else {
                ""
            };
            format!("{value:+.3}{label}")
        }
        // Undefined rather than zero: an all-success window has no variation to
        // score against, which is a different statement from "no skill".
        _ => "&mdash; (no failures yet to score against)".to_string(),
    }
}

/// Share of full-window decisions from the farthest quarter above which the
/// candidate window is worth investigating as too narrow.
///
/// A quarter of the window is what you would expect by chance if the predictor
/// ignored distance entirely, so "a quarter of full-window decisions came from
/// the farthest quarter" is roughly the point where the ordering stops looking
/// distance-dominated and starts looking truncated.
const WINDOW_TOO_NARROW_SHARE: f64 = 0.25;

/// Turn the far-quarter share into the sentence an operator actually acts on.
///
/// Extracted from the template rather than left inline because this is the most
/// consequential piece of new user-facing logic on the page — it is the line
/// that says whether to go and widen the routing window — and inline it had no
/// way to be tested at its own threshold.
fn fmt_window_reading(share: Option<f64>) -> String {
    match share {
        None => "Not enough decisions against a full window to say yet.".to_string(),
        Some(share) if share >= WINDOW_TOO_NARROW_SHARE => format!(
            "<strong>Worth investigating:</strong> {:.0}% of full-window decisions chose from              the farthest quarter, so the better peer may often be one the router never scored.",
            share * 100.0
        ),
        Some(share) => format!(
            "The limit looks comfortable: only {:.0}% of full-window decisions chose from the              farthest quarter, so widening it would rarely change the outcome.",
            share * 100.0
        ),
    }
}

/// Render a stage's observation count, saying so when the stage is inactive.
///
/// A stage below the prediction floor rendered a bare `0` and an empty chart,
/// which is indistinguishable from a broken stage. Both nova gateways sit here
/// permanently for transfer speed, so this is the normal case, not an edge one.
fn fmt_stage_events(count: usize) -> String {
    const PREDICTION_FLOOR: usize = 10;
    if count < PREDICTION_FLOOR {
        format!("{count} &mdash; inactive, needs {PREDICTION_FLOOR}")
    } else {
        count.to_string()
    }
}

pub fn peer_detail_html(address_str: &str) -> String {
    let snap = network_status::get_snapshot();

    let peer = snap.as_ref().and_then(|s| {
        s.peers
            .iter()
            .find(|p| p.address.to_string() == address_str)
    });

    let Some(peer) = peer else {
        return format!(
            include_str!("assets/peer_not_found.html"),
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
                <p class="empty" style="font-size: 0.8em; margin-top: 0.25em;"><a href="https://github.com/sanity/renegade" target="_blank" rel="noopener noreferrer" class="ext-link">Renegade</a> is a zero-configuration k-nearest-neighbours model (it auto-selects K and learns which features matter). It learns from four features (peer, contract location, distance, time) what the distance-based estimate gets <em>wrong</em> for a particular peer and contract, and corrects it &mdash; catching patterns distance alone misses, such as a peer that drops requests for specific contracts. It corrects the <em>distance-only</em> estimate directly, taking over from the simpler per-peer offset rather than adding to it. How much of the correction is applied depends on how much nearby evidence supports it, so a query the model knows nothing about leaves the distance-only estimate untouched.</p>
                <div class="info-grid">
                    <div class="info-label">Failure observations</div><div class="info-value">{rf}</div>
                    <div class="info-label">Response time observations</div><div class="info-value">{rr}</div>
                    <div class="info-label">Transfer speed observations</div><div class="info-value">{rt}</div>
                    <div class="info-label">Known peers</div><div class="info-value">{rp}</div>
                    <div class="info-label">Predictions evaluated</div><div class="info-value">{n_eval}</div>
                    <div class="info-label">Brier score (overall)</div><div class="info-value">{brier}</div>
                    <div class="info-label">Brier score (recent)</div><div class="info-value">{recent_brier}</div>
                </div>

                <h3 style="margin-top: 1em;">Which layer is doing the work?</h3>
                <p class="empty" style="font-size: 0.8em; margin-top: 0.25em;">Each layer&rsquo;s <strong>skill</strong> against simply assuming the average failure rate. <strong>0 means no better than that assumption; negative means worse.</strong> Skill rather than a raw score because failures are rare, and on a rare event a raw score mostly measures the rarity: at a {base_rate} failure rate, a forecast that never predicts failure at all scores {clim_brier} and looks excellent.</p>
                <p class="empty" style="font-size: 0.8em; margin-top: 0.25em;">These are <strong>two routes from the same starting point</strong>, not one running total. Both begin at the distance-only estimate. The established route adds a per-peer offset and then the Renegade blend; the correction route instead learns what the distance-only estimate gets wrong for this exact peer and contract, and <strong>replaces</strong> the per-peer offset rather than stacking on it. Compare the two end points, not the rows in order.</p>
                <div class="info-grid">
                    <div class="info-label">Both routes start at: distance only</div><div class="info-value">{skill_global}</div>
                    <div class="info-label">&#8627; established: + per-peer offset</div><div class="info-value">{skill_adjusted}</div>
                    <div class="info-label">&#8627; established: + Renegade blend{blend_note}</div><div class="info-value">{skill_blended}</div>
                    <div class="info-label">&#8627; correction: distance only + residual{corrected_note}</div><div class="info-value">{skill_corrected}</div>
                    <div class="info-label">Scored predictions</div><div class="info-value">{layers_eval}</div>
                </div>

                <h3 style="margin-top: 1em;">Is the candidate window too narrow?</h3>
                <p class="empty" style="font-size: 0.8em; margin-top: 0.25em;">Routing only scores the <strong>{window} closest</strong> peers to the contract; anything further away is invisible for that hop. This measures whether that limit is costing anything. If the chosen peer is usually one of the nearest few, the limit is comfortably wide and removing it would change nothing. If choices pile up against the far edge <em>while the window was full</em>, the ordering is being cut off where the better peer plausibly sits.</p>
                <div class="info-grid">
                    <div class="info-label">Decisions measured</div><div class="info-value">{rank_total}</div>
                    <div class="info-label">Mean position of the chosen peer</div><div class="info-value">{rank_mean}</div>
                    <div class="info-label">Decisions against a full window</div><div class="info-value">{rank_saturated}</div>
                    <div class="info-label">&#8627; chose from the farthest quarter</div><div class="info-value">{rank_far}</div>
                </div>
                <p class="empty" style="font-size: 0.8em; margin-top: 0.5em;">{rank_reading}</p>

                <h3 style="margin-top: 1em;">Correction state</h3>
                <p class="empty" style="font-size: 0.8em; margin-top: 0.25em;">These are learned from the data, not configured &mdash; both are chosen by scoring candidate values against what actually happened, and both will change as the network does. <em>&kappa;</em> is how much evidence the correction demands before it applies half of itself; the bandwidth is the distance in feature space beyond which neighbouring observations stop counting as nearby.</p>
                <div class="info-grid">
                    <div class="info-label">Reaching routing decisions</div><div class="info-value">{corr_enabled}</div>
                    <div class="info-label">Selected &kappa;</div><div class="info-value">{kappa}</div>
                    <div class="info-label">Kernel bandwidth</div><div class="info-value">{bandwidth}</div>
                    <div class="info-label">Residual observations: failure</div><div class="info-value">{res_f}</div>
                    <div class="info-label">Residual observations: response time</div><div class="info-value">{res_r}</div>
                    <div class="info-label">Residual observations: transfer speed</div><div class="info-value">{res_t}</div>
                    <div class="info-label">Corrections scored</div><div class="info-value">{res_scored}</div>
                </div>
            </div>"#,
            active = if rs.prediction_active { "Yes" } else { "No" },
            total = total_events,
            pf = peer_failure_events,
            pr = peer_response_events,
            pt = peer_transfer_events,
            rf = rs.renegade_failure_events,
            rr = fmt_stage_events(rs.renegade_response_time_events),
            rt = fmt_stage_events(rs.renegade_transfer_speed_events),
            rp = rs.renegade_known_peers,
            base_rate = rs
                .failure_base_rate
                .map(|b| format!("{:.2}%", b * 100.0))
                .unwrap_or_else(|| "the observed".to_string()),
            clim_brier = rs
                .failure_climatology_brier
                .map(|b| format!("{:.4}", b))
                .unwrap_or_else(|| "\u{2014}".to_string()),
            skill_global = fmt_skill(rs.failure_skill_global),
            skill_adjusted = fmt_skill(rs.failure_skill_adjusted),
            skill_blended = fmt_skill(rs.failure_skill_blended),
            skill_corrected = fmt_skill(rs.failure_skill_corrected),
            blend_note = if rs.residual_correction_enabled {
                " &mdash; superseded"
            } else {
                " &mdash; in use"
            },
            corrected_note = if rs.residual_correction_enabled {
                " &mdash; in use"
            } else {
                " &mdash; measured, not applied"
            },
            layers_eval = rs.failure_layers_evaluated,
            corr_enabled = if rs.residual_correction_enabled {
                "Yes"
            } else {
                "No \u{2014} measuring only"
            },
            kappa = rs
                .residual_kappa
                .map(|k| format!("{:.1}", k))
                .unwrap_or_else(|| "\u{2014}".to_string()),
            bandwidth = rs
                .residual_bandwidth
                .map(|b| format!("{:.4}", b))
                .unwrap_or_else(|| "not yet estimated".to_string()),
            res_f = rs.residual_failure_events,
            res_r = rs.residual_response_time_events,
            res_t = rs.residual_transfer_speed_events,
            res_scored = rs.residual_scored,
            window = rs.consider_n_closest_peers,
            rank_total = rs.selection_ranks.total,
            rank_mean = rs
                .selection_ranks
                .mean_rank()
                .map(|mean| format!("{mean:.1} (0 = closest)"))
                .unwrap_or_else(|| "&mdash;".to_string()),
            rank_saturated = rs.selection_ranks.saturated,
            rank_far = rs
                .selection_ranks
                .far_quarter_share()
                .map(|share| format!("{:.1}%", share * 100.0))
                .unwrap_or_else(|| "&mdash;".to_string()),
            rank_reading = fmt_window_reading(rs.selection_ranks.far_quarter_share()),
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
                // The adjustment mode per chart MUST match the router's choice for
                // that estimator (see `Router::new`): failure + transfer are
                // additive, response time is multiplicative. The dashboard renders
                // the peer-adjusted curve with this mode, so a wrong mode would draw
                // a curve of the wrong SHAPE vs the router's prediction. (The curve
                // is intentionally a *preview*: the dashboard draws it whenever an
                // adjustment exists, while the router only applies it once the peer
                // has `MIN_POINTS_FOR_REGRESSION` effective observations.) When
                // #4547 flips transfer rate to multiplicative, update its mode here
                // too — the mode is mirrored, not read from the snapshot.
                panel_content.push_str(&build_estimator_chart_or_placeholder(
                    "Failure Probability",
                    f_curve,
                    f_points,
                    f_range,
                    if tab_name == "All" { fail_adj } else { None },
                    AdjustmentMode::Additive,
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
                    AdjustmentMode::Multiplicative,
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
                    AdjustmentMode::Additive,
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
                    just break it down and are not consulted separately). "Peer-adjusted" applies this
                    peer's running EWMA correction to that fit — a multiplicative factor for response
                    time, an additive offset for failure and transfer rate. How tightly the dots hug a monotonic
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
            // The chart derives its own score from these pairs. Passing one in
            // was the bug: every score available to pass describes a different
            // window from the one drawn.
            &rs.renegade_accuracy_pairs,
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
        include_str!("assets/peer.html"),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_rendering_names_the_sign_rather_than_leaving_it_to_the_reader() {
        // The whole point of showing skill instead of a raw score is that the
        // SIGN is the finding. A bare "-0.430" is exactly as easy to skim past
        // as the "excellent" grade it replaces.
        let worse = fmt_skill(Some(-0.43));
        assert!(worse.contains("-0.430"), "got {worse}");
        assert!(
            worse.contains("worse than assuming nothing"),
            "a negative skill must say so in words, got {worse}"
        );

        let none = fmt_skill(Some(0.0));
        assert!(
            none.contains("no better than assuming nothing"),
            "zero skill must say so in words, got {none}"
        );

        let good = fmt_skill(Some(0.42));
        assert!(good.contains("+0.420"), "got {good}");
        assert!(
            !good.contains("assuming nothing"),
            "positive skill needs no caveat, got {good}"
        );
    }

    #[test]
    fn skill_rendering_distinguishes_undefined_from_zero() {
        // An all-success window has no variation to score against, which is a
        // different statement from "this model has no skill".
        for undefined in [None, Some(f64::NAN), Some(f64::INFINITY)] {
            let rendered = fmt_skill(undefined);
            assert!(
                rendered.contains("no failures yet"),
                "undefined skill must not render as a number, got {rendered}"
            );
        }
    }

    #[test]
    fn skill_rendering_switches_wording_at_its_own_thresholds() {
        // The +/-0.01 band is where "no better than assuming nothing" gives way
        // to a real verdict. The other tests sit far from it, so a wrong
        // threshold there would go unnoticed.
        assert!(
            fmt_skill(Some(-0.011)).contains("worse than assuming nothing"),
            "just below -0.01 must read as worse"
        );
        assert!(
            fmt_skill(Some(-0.009)).contains("no better than assuming nothing"),
            "just above -0.01 must read as neutral, not worse"
        );
        assert!(
            fmt_skill(Some(0.009)).contains("no better than assuming nothing"),
            "just below +0.01 must still read as neutral"
        );
        let just_above = fmt_skill(Some(0.011));
        assert!(
            !just_above.contains("assuming nothing"),
            "just above +0.01 must read as a real gain, got {just_above}"
        );
    }

    #[test]
    fn window_reading_switches_verdict_at_its_threshold() {
        // This sentence is what an operator acts on, so its boundary is pinned
        // in both directions rather than only at comfortable distances from it.
        assert!(fmt_window_reading(Some(0.249)).contains("looks comfortable"));
        assert!(fmt_window_reading(Some(0.25)).contains("Worth investigating"));
        assert!(fmt_window_reading(Some(0.251)).contains("Worth investigating"));
        assert!(fmt_window_reading(Some(0.0)).contains("looks comfortable"));
        assert!(fmt_window_reading(Some(1.0)).contains("Worth investigating"));
    }

    #[test]
    fn window_reading_says_so_when_there_is_no_evidence_yet() {
        let reading = fmt_window_reading(None);
        assert!(
            reading.contains("Not enough decisions"),
            "with no full-window decisions the panel must not imply a verdict, got {reading}"
        );
        assert!(
            !reading.contains("comfortable") && !reading.contains("Worth investigating"),
            "absence of evidence must not render as either verdict, got {reading}"
        );
    }

    #[test]
    fn window_reading_reports_the_share_it_judged() {
        // A verdict without its number is not checkable by the reader.
        assert!(fmt_window_reading(Some(0.42)).contains("42%"));
        assert!(fmt_window_reading(Some(0.05)).contains("5%"));
    }

    #[test]
    fn stage_events_marks_an_inactive_stage_at_the_boundary() {
        // Both nova gateways sit below the floor for transfer speed
        // permanently, so this is the normal case rather than an edge one.
        assert!(fmt_stage_events(0).contains("inactive"));
        assert!(fmt_stage_events(9).contains("inactive"));
        assert!(
            fmt_stage_events(9).contains("10"),
            "an inactive stage must name the threshold it needs"
        );
        // Exactly at the floor the stage IS active — an off-by-one here would
        // label a working stage broken.
        assert_eq!(fmt_stage_events(10), "10");
        assert_eq!(fmt_stage_events(4155), "4155");
    }

    /// Pins that the layer panel presents the correction as a BRANCH off the
    /// distance-only estimate, not as another term stacked on the per-peer
    /// offset.
    ///
    /// It is stacked in neither the code nor the copy, but it was in the copy
    /// until the B5 change flipped the composition and this panel was not
    /// updated with it. The label read "+ residual correction" directly beneath
    /// "+ per-peer adjustment", which invites exactly the wrong comparison —
    /// reading down the rows as a running total when the last row branches off
    /// the first.
    #[test]
    fn layer_panel_does_not_present_the_correction_as_stacking() {
        let source = include_str!("peer_detail.rs");
        // Scope to the RENDERING FUNCTION before searching for anything, so the
        // scrape cannot anchor on this test's own literals, on a second panel
        // introduced earlier in the file, or on text that is never rendered.
        // Proving the match merely precedes the test module is weaker: it still
        // permits the pin to validate dead copy while the real panel regresses.
        let render_start = source
            .find("pub fn peer_detail_html")
            .expect("the rendering function must exist");
        let render_end = source[render_start..]
            .find("\n#[cfg(test)]")
            .map(|offset| render_start + offset)
            .expect("the test module must follow the rendering function");
        let source = &source[render_start..render_end];

        let panel_start = source
            .find("Which layer is doing the work?")
            .expect("the layer panel heading must exist inside peer_detail_html");
        let panel_end = source[panel_start..]
            .find("Correction state")
            .map(|offset| panel_start + offset)
            .expect("the correction-state heading must follow the layer panel");
        let panel = &source[panel_start..panel_end];

        assert!(
            panel.contains("two routes from the same starting point"),
            "the panel must say the rows are alternative routes, not a running total"
        );
        assert!(
            panel.contains("replaces"),
            "the panel must say the correction REPLACES the per-peer offset"
        );
        assert!(
            !panel.contains(r#"<div class="info-label">+ residual correction"#),
            "the corrected row must not be labelled with a leading '+', which \
             reads as another term added to the row above it"
        );
    }
}
