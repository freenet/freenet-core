use super::assets::{CSS, JS, PEER_CSS};
use super::cards::format_bytes;
use super::estimator::{
    PeerLine, build_accuracy_panel, build_estimator_chart_or_placeholder, failure_chart_y_max,
    fmt_prediction_prob, fmt_prediction_speed, fmt_prediction_time,
};
use super::*;
use crate::router::{AdjustmentMode, Breakdown};

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

/// A duration readable at any scale: µs below a millisecond, ms below a second.
fn fmt_duration_secs(seconds: f64) -> String {
    if seconds < 1e-3 {
        format!("{:.0} &micro;s", seconds * 1e6)
    } else if seconds < 1.0 {
        format!("{:.1} ms", seconds * 1e3)
    } else {
        format!("{seconds:.2} s")
    }
}

/// Render the RMS error in seconds of the router's estimate and of the
/// isotonic fallback's, on the same events.
///
/// It names which is ahead rather than leaving two bare numbers. Below
/// [`crate::router::MIN_WEIGHT_FOR_VERDICT`] of FORGOTTEN event weight it says
/// so instead of naming a winner: a handful of recent timed events settles
/// nothing, however many were scored long ago.
fn fmt_seconds_error(
    isotonic: Option<f64>,
    hierarchical: Option<f64>,
    scored: u64,
    weight: f64,
) -> String {
    let (Some(isotonic), Some(hierarchical)) = (isotonic, hierarchical) else {
        return "&mdash; no event both models forecast yet".to_string();
    };
    let numbers = format!(
        "hierarchical {}, isotonic fallback {} (n={scored}, recent weight {weight:.0})",
        fmt_duration_secs(hierarchical),
        fmt_duration_secs(isotonic)
    );
    if weight < crate::router::MIN_WEIGHT_FOR_VERDICT {
        return format!(
            "{numbers} &mdash; insufficient recent data (needs weight {:.0})",
            crate::router::MIN_WEIGHT_FOR_VERDICT
        );
    }
    let verdict = if hierarchical <= isotonic {
        "hierarchical no worse"
    } else {
        "hierarchical worse"
    };
    format!("{numbers} &mdash; {verdict}")
}

/// Render the hierarchical peer-table eviction count against its capacity.
///
/// Evictions mean churn is exceeding the headroom derived from
/// `max_connections`, which an operator can act on, so a non-zero count says so.
fn fmt_evictions(evictions: u64, capacity: usize) -> String {
    if evictions == 0 {
        format!("0 (capacity {capacity} per stage)")
    } else {
        format!(
            "{evictions} (capacity {capacity} per stage) &mdash; churn exceeds the table's headroom"
        )
    }
}

/// Skewness or excess kurtosis beyond this reads as a departure from the
/// lognormal assumption. For a normal sample of a few hundred the sampling
/// standard error of either is about 0.1-0.3, so 1.0 is well clear of noise.
const LOG_SHAPE_WARNING: f64 = 1.0;

/// Render a timing stage's lognormality check.
///
/// Expectation timing is exact only for normal log residuals, so the reading an
/// operator needs is whether that holds, said in words, with the numbers.
fn fmt_log_shape(shape: &crate::router::LogResidualShape) -> String {
    let (Some(sigma2), Some(skew), Some(kurtosis)) =
        (shape.sigma2, shape.skewness, shape.excess_kurtosis)
    else {
        return format!("&mdash; not enough data yet ({} residuals)", shape.events);
    };
    let verdict = if skew.abs() > LOG_SHAPE_WARNING || kurtosis.abs() > LOG_SHAPE_WARNING {
        " &mdash; <strong>not lognormal</strong>: expected times may be inaccurate"
    } else {
        " &mdash; consistent with lognormal"
    };
    format!(
        "&sigma;&sup2; {sigma2:.3}, skew {skew:+.2}, excess kurtosis {kurtosis:+.2} (n={}){verdict}",
        shape.events
    )
}

/// Render the hierarchical estimator's selected forgetting horizon.
///
/// `None` means "forgets nothing inside its window" only once the stage is
/// active (has a curve). Before that it predicts nothing, and no horizon has
/// been selected at all.
fn fmt_horizon(active: bool, events: usize, hours: Option<f64>) -> String {
    match (events, active, hours) {
        (0, _, _) => "&mdash; (no events yet)".to_string(),
        (_, false, _) => format!("&mdash; not active yet ({events} events, no curve)"),
        (_, true, None) => "none &mdash; remembers its whole window".to_string(),
        (_, true, Some(hours)) => format!("{hours} h"),
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

/// The three things the router estimates for a candidate, in display order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StageKind {
    Failure,
    ResponseTime,
    TransferSpeed,
}

impl StageKind {
    const ALL: [StageKind; 3] = [
        StageKind::Failure,
        StageKind::ResponseTime,
        StageKind::TransferSpeed,
    ];

    fn title(self) -> &'static str {
        match self {
            StageKind::Failure => "Failure probability",
            StageKind::ResponseTime => "Time to first response",
            StageKind::TransferSpeed => "Transfer speed",
        }
    }

    /// Outcomes a stage needs before it has a curve (the hierarchical
    /// estimator's `MIN_CURVE_POINTS_*`).
    fn needs(self) -> usize {
        match self {
            StageKind::Failure => 5,
            StageKind::ResponseTime | StageKind::TransferSpeed => 30,
        }
    }

    /// A value on the stage's own scale (a probability, or natural-log seconds
    /// or bytes/s) in readable units.
    fn render(self, value: f64) -> String {
        match self {
            StageKind::Failure => format!("{:.2}%", value * 100.0),
            StageKind::ResponseTime => fmt_duration_secs(value.exp()),
            StageKind::TransferSpeed => fmt_prediction_speed(value.exp()),
        }
    }

    /// The change a level made, on the stage's own scale: percentage points
    /// for a probability, a factor for a log-scale quantity.
    fn change(self, before: f64, after: f64) -> String {
        match self {
            StageKind::Failure => format!("{:+.2} pt", (after - before) * 100.0),
            StageKind::ResponseTime | StageKind::TransferSpeed => {
                format!("&times;{:.2}", (after - before).exp())
            }
        }
    }

    /// An estimate in the router's own units (probability, seconds, bytes/s).
    fn render_estimate(self, estimate: f64) -> String {
        match self {
            StageKind::Failure => format!("{:.2}%", estimate * 100.0),
            StageKind::ResponseTime => fmt_duration_secs(estimate),
            StageKind::TransferSpeed => fmt_prediction_speed(estimate),
        }
    }
}

/// Render a stage's data: how many outcomes its window holds, and whether it
/// has a curve yet. A timing stage without one is estimated by the isotonic
/// fallback, which the row says, because it is what routing uses meanwhile.
fn fmt_stage_status(stage: StageKind, events: usize, active: bool) -> String {
    if active {
        return format!("{events} outcomes in the window");
    }
    let fallback = match stage {
        StageKind::Failure => "",
        StageKind::ResponseTime | StageKind::TransferSpeed => {
            "; routing uses the per-distance fit meanwhile"
        }
    };
    format!(
        "{events} &mdash; no curve yet, needs {}{fallback}",
        stage.needs()
    )
}

/// Render the evidence behind a level's own average and how much of it the
/// estimate adopted.
fn fmt_evidence(evidence: f64, weight: f64) -> String {
    if evidence <= 0.0 {
        "no record".to_string()
    } else {
        format!(
            "{evidence:.1} effective outcomes, {:.0}% adopted",
            weight * 100.0
        )
    }
}

/// Render how much the router knows about this peer for one stage.
fn fmt_record(breakdown: Option<&Breakdown>) -> String {
    match breakdown {
        None => "&mdash; no curve yet".to_string(),
        Some(breakdown) if breakdown.peer_evidence <= 0.0 => "no record yet".to_string(),
        Some(breakdown) => format!("{:.1} effective outcomes", breakdown.peer_evidence),
    }
}

/// The rows explaining one stage's estimate for this peer.
fn breakdown_rows(stage: StageKind, breakdown: Option<&Breakdown>) -> String {
    let row = |label: &str, value: &str| {
        format!(r#"<div class="info-label">{label}</div><div class="info-value">{value}</div>"#)
    };
    let Some(b) = breakdown else {
        let fallback = match stage {
            StageKind::Failure => String::new(),
            StageKind::ResponseTime | StageKind::TransferSpeed => {
                " Until then routing uses the per-distance fit with this peer's running \
                 correction (the charts below)."
                    .to_string()
            }
        };
        return row(
            "Estimate",
            &format!(
                "&mdash; no curve yet (needs {} outcomes).{fallback}",
                stage.needs()
            ),
        );
    };
    let mut rows = row("Distance curve at distance 0", &stage.render(b.curve));
    let step = |before: f64, after: Option<f64>, note: String| match after {
        Some(after) => format!(
            "{} ({}){note}",
            stage.render(after),
            stage.change(before, after)
        ),
        None => "&mdash; no spread between peers measured yet".to_string(),
    };
    rows.push_str(&row(
        "+ what every peer has in common",
        &step(b.curve, b.after_all_peers, String::new()),
    ));
    let before_peer = b.after_all_peers.unwrap_or(b.curve);
    rows.push_str(&row(
        "+ this peer",
        &step(
            before_peer,
            b.after_peer,
            format!("; {}", fmt_evidence(b.peer_evidence, b.peer_weight)),
        ),
    ));
    let before_band = b.after_peer.unwrap_or(before_peer);
    rows.push_str(&row(
        &format!(
            "+ this peer on ring band {} ({:.3}&ndash;{:.3})",
            b.band,
            b.band as f64 / 8.0,
            (b.band + 1) as f64 / 8.0
        ),
        &step(
            before_band,
            b.after_band,
            format!("; {}", fmt_evidence(b.band_evidence, b.band_weight)),
        ),
    ));
    match stage {
        StageKind::Failure => {}
        StageKind::ResponseTime | StageKind::TransferSpeed => {
            // The router acts on the expectation, not the median: half the
            // predictive log variance, added for time and subtracted for speed.
            let half = b.spread / 2.0;
            let factor = if stage == StageKind::ResponseTime {
                half.exp()
            } else {
                (-half).exp()
            };
            rows.push_str(&row(
                "Allowance for uncertainty",
                &format!("&times;{factor:.2} (an average, not a typical case)"),
            ));
        }
    }
    let horizon = match b.horizon_hours {
        Some(hours) => format!("forgets over {hours} h"),
        None => "remembers its whole window".to_string(),
    };
    rows.push_str(&row(
        "Estimate routing uses",
        &format!(
            "<strong>{}</strong> ({horizon})",
            stage.render_estimate(b.estimate)
        ),
    ));
    rows
}

/// Shown when `FREENET_ROUTING_FALLBACK_ISOTONIC` has routing on the emergency
/// fallback, so no reading on the page is mistaken for the live algorithm.
const FALLBACK_NOTE: &str = "<strong>Routing is on the emergency fallback</strong> \
     (FREENET_ROUTING_FALLBACK_ISOTONIC is set): every estimate comes from the per-distance \
     fit with a running per-peer correction. The estimates below are still computed, but \
     routing does not use them.";

/// The card that shows how the router builds its estimate for this peer.
fn build_breakdown_card(breakdown: &[Option<Breakdown>; 3], fallback: bool) -> String {
    let mut sections = String::new();
    for (stage, breakdown) in StageKind::ALL.iter().zip(breakdown) {
        write!(
            sections,
            r#"<h3 style="margin-top: 1em;">{title}</h3><div class="info-grid">{rows}</div>"#,
            title = stage.title(),
            rows = breakdown_rows(*stage, breakdown.as_ref()),
        )
        .ok();
    }
    let fallback_note = if fallback {
        format!(
            r#"<p class="empty" style="font-size: 0.8em; margin-top: 0.25em;">{FALLBACK_NOTE}</p>"#
        )
    } else {
        String::new()
    };
    format!(
        r#"<div class="card">
            <h2>How the Router Sees This Peer</h2>
            <p class="empty" style="font-size: 0.8em; margin-top: 0.25em;">The router&rsquo;s estimates for a contract at this peer&rsquo;s own location (distance 0), built up one level at a time. Each level adds an offset learned from outcomes, weighted by how much evidence stands behind it; &ldquo;adopted&rdquo; is how much of that level&rsquo;s own average the estimate took on. The last row of each is the number routing uses.</p>
            {fallback_note}
            {sections}
        </div>"#
    )
}

/// Top of the failure chart's y-axis, zoomed so every drawn line stays on
/// screen. See [`failure_chart_y_max`].
fn failure_axis_top(curve: &[(f64, f64)], line: PeerLine<'_>) -> f64 {
    match line {
        PeerLine::None => failure_chart_y_max(curve, None),
        PeerLine::Adjustment(adjustment, _) => failure_chart_y_max(curve, Some(adjustment)),
        PeerLine::Curve(points) => {
            // Lift the axis by how far the peer's right edge sits above the
            // curve's, exactly as an upward adjustment would.
            let right_edge = |points: &[(f64, f64)]| {
                points
                    .iter()
                    .max_by(|a, b| a.0.total_cmp(&b.0))
                    .map_or(0.0, |&(_, y)| y)
            };
            failure_chart_y_max(curve, Some(right_edge(points) - right_edge(curve)))
        }
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
        let record = |stage: usize| {
            fmt_record(
                peer_routing
                    .as_ref()
                    .and_then(|pr| pr.breakdown[stage].as_ref()),
            )
        };
        format!(
            r#"<div class="card">
                <h2>Routing Model</h2>
                <div class="info-grid">
                    <div class="info-label">Prediction active</div><div class="info-value">{active}</div>
                    <div class="info-label">Routing estimator</div><div class="info-value">{estimator}</div>
                    <div class="info-label">Global events</div><div class="info-value">{total}</div>
                    <div class="info-label">This peer: failure record</div><div class="info-value">{pf}</div>
                    <div class="info-label">This peer: response-time record</div><div class="info-value">{pr}</div>
                    <div class="info-label">This peer: transfer-speed record</div><div class="info-value">{pt}</div>
                </div>

                <h3 style="margin-top: 1em;">How the router estimates</h3>
                <p class="empty" style="font-size: 0.8em; margin-top: 0.25em;">For each candidate peer the router estimates three things: the chance the request fails, the time to the first response, and the transfer speed. Each estimate starts from a <strong>distance curve</strong> fitted over the node&rsquo;s recent traffic, then adds an offset for the peer, and for the peer on this part of the ring, each weighted by how much evidence stands behind it: a peer seen a handful of times barely moves the estimate, and a peer seen many times moves it only as far as the measured spread between peers justifies &mdash; if peers turn out not to differ, not at all. It forgets old evidence at whichever rate has predicted best recently. The timing estimates need 30 timed responses before they have a curve; until then routing uses a simpler per-distance fit with a running per-peer correction.</p>
                <div class="info-grid">
                    <div class="info-label">Failure data</div><div class="info-value">{stage_failure}</div>
                    <div class="info-label">Response-time data</div><div class="info-value">{stage_response}</div>
                    <div class="info-label">Transfer-speed data</div><div class="info-value">{stage_transfer}</div>
                    <div class="info-label">Forgetting horizon (failure)</div><div class="info-value">{horizon}</div>
                    <div class="info-label">Peer-table evictions</div><div class="info-value">{evictions}</div>
                </div>

                <h3 style="margin-top: 1em;">How good are the estimates?</h3>
                <p class="empty" style="font-size: 0.8em; margin-top: 0.25em;">The failure estimate&rsquo;s <strong>skill</strong> against simply assuming the average failure rate. <strong>0 means no better than that assumption; negative means worse.</strong> Skill rather than a raw score because failures are rare, and on a rare event a raw score mostly measures the rarity: at a {base_rate} failure rate, a forecast that never predicts failure at all scores {clim_brier} and looks excellent. The timing rows compare the router&rsquo;s estimates, in seconds, with the per-distance fit&rsquo;s on the same responses.</p>
                <div class="info-grid">
                    <div class="info-label">Failure-estimate skill</div><div class="info-value">{skill}</div>
                    <div class="info-label">Brier score</div><div class="info-value">{brier}</div>
                    <div class="info-label">Scored predictions</div><div class="info-value">{evaluated}</div>
                    <div class="info-label">Response-time error, RMS seconds</div><div class="info-value">{timing_error}</div>
                    <div class="info-label">Transfer-time error, RMS seconds</div><div class="info-value">{transfer_error}</div>
                    <div class="info-label">Response-time log residuals</div><div class="info-value">{shape_response}</div>
                    <div class="info-label">Transfer-speed log residuals</div><div class="info-value">{shape_transfer}</div>
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
            </div>"#,
            active = if rs.prediction_active { "Yes" } else { "No" },
            estimator = if rs.isotonic_fallback_enabled {
                "<strong>emergency fallback</strong>: the per-distance fit with a running \
                 per-peer correction (FREENET_ROUTING_FALLBACK_ISOTONIC is set); the estimates \
                 below are computed but not used"
            } else {
                "hierarchical: evidence-weighted, described below"
            },
            total = total_events,
            pf = record(0),
            pr = record(1),
            pt = record(2),
            stage_failure = fmt_stage_status(
                StageKind::Failure,
                rs.hierarchical_failure_events,
                rs.hierarchical_failure_active
            ),
            stage_response = fmt_stage_status(
                StageKind::ResponseTime,
                rs.hierarchical_response_time_events,
                rs.hierarchical_response_time_active
            ),
            stage_transfer = fmt_stage_status(
                StageKind::TransferSpeed,
                rs.hierarchical_transfer_speed_events,
                rs.hierarchical_transfer_speed_active
            ),
            horizon = fmt_horizon(
                rs.hierarchical_failure_active,
                rs.hierarchical_failure_events,
                rs.hierarchical_failure_horizon_hours,
            ),
            evictions = fmt_evictions(
                rs.hierarchical_peer_evictions,
                rs.hierarchical_peer_capacity
            ),
            base_rate = rs
                .failure_base_rate
                .map(|b| format!("{:.2}%", b * 100.0))
                .unwrap_or_else(|| "the observed".to_string()),
            clim_brier = rs
                .failure_climatology_brier
                .map(|b| format!("{:.4}", b))
                .unwrap_or_else(|| "\u{2014}".to_string()),
            skill = fmt_skill(rs.failure_skill_hierarchical),
            brier = match (rs.failure_brier, rs.failure_climatology_brier) {
                (Some(brier), Some(climatology)) => {
                    format!("{brier:.4} (assuming the average scores {climatology:.4})")
                }
                (Some(brier), None) => format!("{brier:.4}"),
                _ => "\u{2014}".to_string(),
            },
            evaluated = rs.hierarchical_failure_evaluated,
            timing_error = fmt_seconds_error(
                rs.response_time_rmse_secs_isotonic,
                rs.response_time_rmse_secs_hierarchical,
                rs.response_time_scored,
                rs.response_time_weight
            ),
            transfer_error = fmt_seconds_error(
                rs.transfer_time_rmse_secs_isotonic,
                rs.transfer_time_rmse_secs_hierarchical,
                rs.transfer_time_scored,
                rs.transfer_time_weight
            ),
            shape_response = fmt_log_shape(&rs.hierarchical_response_time_log_shape),
            shape_transfer = fmt_log_shape(&rs.hierarchical_transfer_speed_log_shape),
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
        )
    } else {
        r#"<div class="card"><h2>Routing Model</h2><p class="empty">Router data not available</p></div>"#.to_string()
    };

    // Whether routing is on the emergency isotonic fallback.
    let fallback = router_snapshot
        .as_ref()
        .is_some_and(|rs| rs.isotonic_fallback_enabled);

    // How the router builds its estimate for this peer, level by level.
    let breakdown_card = match &peer_routing {
        Some(pr) if pr.breakdown.iter().any(Option::is_some) => {
            build_breakdown_card(&pr.breakdown, fallback)
        }
        _ => String::new(),
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
        let peer_curves = peer_routing.as_ref().map(|pr| &pr.peer_curves);
        let ploc = peer.location;

        // What the All tab draws for one stage: the hierarchical curves once the
        // stage has one (they are what routing uses), otherwise the isotonic fit
        // with this peer's EWMA adjustment (which routing uses meanwhile). The
        // mode per chart MUST match the router's (`Router::new`): failure and
        // transfer additive, response time multiplicative.
        let all_tab = |stage: usize,
                       hierarchical: &[(f64, f64)],
                       adjustment: Option<f64>,
                       mode: AdjustmentMode| {
            // On the emergency fallback routing reads the isotonic fit for
            // every stage, so that is what the chart shows.
            if hierarchical.is_empty() || rs.isotonic_fallback_enabled {
                (
                    false,
                    adjustment.map_or(PeerLine::None, |adj| PeerLine::Adjustment(adj, mode)),
                )
            } else {
                let own = peer_curves.map_or(&[][..], |curves| curves[stage].as_slice());
                (
                    true,
                    if own.is_empty() {
                        PeerLine::None
                    } else {
                        PeerLine::Curve(own)
                    },
                )
            }
        };

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

            // Per stage: the curve to draw and this peer's line. The per-op tabs
            // break the isotonic fit down by operation type and draw no peer line.
            let hierarchical = &rs.hierarchical_curves;
            let (f_curve, f_line) = if tab_name == "All" {
                match all_tab(0, &hierarchical.failure, fail_adj, AdjustmentMode::Additive) {
                    (true, line) => (hierarchical.failure.as_slice(), line),
                    (false, line) => (f_curve, line),
                }
            } else {
                (f_curve, PeerLine::None)
            };
            let (rt_curve, rt_line) = if tab_name == "All" {
                match all_tab(
                    1,
                    &hierarchical.response_time,
                    rt_adj,
                    AdjustmentMode::Multiplicative,
                ) {
                    (true, line) => (hierarchical.response_time.as_slice(), line),
                    (false, line) => (rt_curve, line),
                }
            } else {
                (rt_curve, PeerLine::None)
            };
            let (xfer_curve, xfer_line) = if tab_name == "All" {
                match all_tab(
                    2,
                    &hierarchical.transfer_speed,
                    xfer_adj,
                    AdjustmentMode::Additive,
                ) {
                    (true, line) => (hierarchical.transfer_speed.as_slice(), line),
                    (false, line) => (xfer_curve, line),
                }
            } else {
                (xfer_curve, PeerLine::None)
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
                // the axis to 2x the drawn lines' value at the right edge of the
                // plot so they are legible. See failure_chart_y_max.
                let fail_y_max = failure_axis_top(f_curve, f_line).to_string();
                panel_content.push_str(&build_estimator_chart_or_placeholder(
                    "Failure Probability",
                    f_curve,
                    f_points,
                    f_range,
                    f_line,
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
                    rt_line,
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
                    xfer_line,
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
                    Actual observed outcomes (dots) against ring distance to the contract. On the
                    All tab the lines are what routing uses: the <strong>distance curve</strong> is
                    the estimate for a peer the router has no record of, and <strong>this peer</strong>
                    adds what it has learned about this one (ring-band effects left out; they are in
                    the card above). A timing estimate needs 30 timed responses before it has such a
                    curve; until then its chart shows the simpler per-distance fit and this peer&rsquo;s
                    running correction, which is what routing uses meanwhile. The per-operation tabs
                    break the per-distance fit down by operation type for comparison; routing does
                    not consult them separately.
                </p>
                <p class="chart-legend">
                    <span class="chart-key"><span class="chart-dot chart-dot-actual"></span> Actual outcomes</span>
                    <span class="chart-key"><span class="chart-dot chart-dot-global"></span> Distance curve</span>
                    <span class="chart-key"><span class="chart-dot chart-dot-peer"></span> This peer</span>
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

    // How the router's recent estimates matched what happened.
    let accuracy_chart = if let Some(ref rs) = router_snapshot {
        build_accuracy_panel(
            // The chart derives its own score from these pairs, so the score
            // always describes the window drawn.
            &rs.hierarchical_failure_pairs,
            &rs.hierarchical_response_time_pairs,
            &rs.hierarchical_transfer_speed_pairs,
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
        prediction_card = prediction_card,
        breakdown_card = breakdown_card,
        charts = charts,
        accuracy_chart = accuracy_chart,
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
    fn eviction_rendering_flags_churn() {
        assert_eq!(fmt_evictions(0, 400), "0 (capacity 400 per stage)");
        let churning = fmt_evictions(12, 400);
        assert!(
            churning.contains("12") && churning.contains("headroom"),
            "{churning}"
        );
    }

    /// The page source from the rendering function to the test module, so a
    /// scrape cannot match this module's own literals.
    fn rendering_source() -> &'static str {
        let source = include_str!("peer_detail.rs");
        let start = source.find("pub fn peer_detail_html").unwrap();
        let end = start + source[start..].find("\n#[cfg(test)]").unwrap();
        &source[start..end]
    }

    /// The copy must not promise that evidence alone moves the estimate: with
    /// no measured between-peer spread, a peer seen thousands of times does not
    /// move it at all.
    #[test]
    fn model_panel_does_not_overstate_what_evidence_buys() {
        let render = rendering_source();
        assert!(!render.contains("moves it fully"));
        assert!(render.contains("if peers turn out not to differ, not at all"));
    }

    /// The page shows the live algorithm: nothing on it may describe the
    /// removed legacy stack as if it still ran (#4485).
    #[test]
    fn peer_page_describes_only_the_live_estimator() {
        let source = include_str!("peer_detail.rs");
        let production = &source[..source.find("\n#[cfg(test)]").unwrap()];
        for removed in [
            "Renegade",
            "renegade",
            "residual correction",
            "Correction state",
            "Which layer is doing the work",
            "FREENET_ROUTING_HIERARCHICAL",
        ] {
            assert!(
                !production.contains(removed),
                "the peer page still mentions {removed:?}, which no longer routes"
            );
        }
    }

    #[test]
    fn seconds_error_rendering_names_the_verdict() {
        assert!(
            fmt_seconds_error(Some(0.2), Some(0.1), 400, 400.0).contains("hierarchical no worse")
        );
        assert!(
            fmt_seconds_error(Some(0.2), Some(0.2), 400, 400.0).contains("hierarchical no worse")
        );
        assert!(fmt_seconds_error(Some(0.1), Some(0.2), 400, 400.0).contains("hierarchical worse"));
        assert!(fmt_seconds_error(None, Some(0.2), 0, 0.0).contains("no event"));
        let numbers = fmt_seconds_error(Some(0.2), Some(0.1), 400, 400.0);
        assert!(
            numbers.contains("hierarchical 100.0 ms")
                && numbers.contains("isotonic fallback 200.0 ms"),
            "each figure must sit beside its model's name: {numbers}"
        );
    }

    /// The verdict needs recent evidence: a large lifetime count whose weight
    /// has been forgotten is not enough.
    #[test]
    fn seconds_error_needs_enough_recent_weight_for_a_verdict() {
        let min = crate::router::MIN_WEIGHT_FOR_VERDICT;
        let stale = fmt_seconds_error(Some(0.2), Some(0.1), 10_000, min - 1.0);
        assert!(
            stale.contains("insufficient recent data") && !stale.contains("no worse"),
            "10,000 events long forgotten must not produce a verdict: {stale}"
        );
        let enough = fmt_seconds_error(Some(0.2), Some(0.1), 150, min);
        assert!(enough.contains("hierarchical no worse"), "{enough}");
    }

    #[test]
    fn lognormality_warning_does_not_assert_a_direction() {
        let shape = crate::router::LogResidualShape {
            sigma2: Some(0.25),
            skewness: Some(-2.0),
            excess_kurtosis: Some(0.0),
            events: 400,
        };
        let text = fmt_log_shape(&shape);
        assert!(text.contains("not lognormal") && text.contains("inaccurate"));
        assert!(!text.contains("understated") && !text.contains("overstated"));
    }

    #[test]
    fn durations_render_at_a_readable_scale() {
        assert_eq!(fmt_duration_secs(0.000_42), "420 &micro;s");
        assert_eq!(fmt_duration_secs(0.042), "42.0 ms");
        assert_eq!(fmt_duration_secs(4.2), "4.20 s");
    }

    #[test]
    fn log_shape_rendering_names_the_verdict_at_its_threshold() {
        use crate::router::LogResidualShape;
        let shape = |skew: f64, kurtosis: f64| LogResidualShape {
            sigma2: Some(0.25),
            skewness: Some(skew),
            excess_kurtosis: Some(kurtosis),
            events: 400,
        };
        assert!(fmt_log_shape(&shape(0.1, -0.2)).contains("consistent with lognormal"));
        assert!(fmt_log_shape(&shape(0.99, 0.99)).contains("consistent with lognormal"));
        assert!(fmt_log_shape(&shape(1.01, 0.0)).contains("not lognormal"));
        assert!(fmt_log_shape(&shape(0.0, 1.01)).contains("not lognormal"));
        assert!(fmt_log_shape(&shape(-1.5, 0.0)).contains("not lognormal"));
        assert!(fmt_log_shape(&LogResidualShape::default()).contains("not enough data"));
    }

    #[test]
    fn horizon_rendering_distinguishes_no_events_from_no_forgetting() {
        assert!(fmt_horizon(false, 0, None).contains("no events yet"));
        assert!(fmt_horizon(true, 0, Some(6.0)).contains("no events yet"));
        let inactive = fmt_horizon(false, 3, None);
        assert!(
            inactive.contains("not active yet") && !inactive.contains("whole window"),
            "a stage with no curve must not read as forgetting nothing: {inactive}"
        );
        let whole = fmt_horizon(true, 120, None);
        assert!(
            whole.contains("whole window"),
            "an active stage with no forgetting must say so, got {whole}"
        );
        assert_eq!(fmt_horizon(true, 120, Some(1.5)), "1.5 h");
        assert_eq!(fmt_horizon(true, 120, Some(24.0)), "24 h");
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

    /// A timing stage without a curve is estimated by the isotonic fallback,
    /// and the row must say so rather than read as broken. The threshold named
    /// is the one the estimator uses.
    #[test]
    fn stage_status_names_the_threshold_and_the_fallback() {
        let cold = fmt_stage_status(StageKind::ResponseTime, 29, false);
        assert!(
            cold.contains("needs 30") && cold.contains("per-distance fit"),
            "{cold}"
        );
        let cold_failure = fmt_stage_status(StageKind::Failure, 3, false);
        assert!(
            cold_failure.contains("needs 5") && !cold_failure.contains("per-distance fit"),
            "the failure stage has no fallback to name: {cold_failure}"
        );
        assert_eq!(
            fmt_stage_status(StageKind::TransferSpeed, 30, true),
            "30 outcomes in the window"
        );
    }

    #[test]
    fn evidence_rendering_distinguishes_no_record_from_little() {
        assert_eq!(fmt_evidence(0.0, 0.0), "no record");
        assert_eq!(
            fmt_evidence(12.34, 0.65),
            "12.3 effective outcomes, 65% adopted"
        );
    }

    fn breakdown(estimate: f64) -> Breakdown {
        Breakdown {
            curve: (0.1f64).ln(),
            after_all_peers: Some((0.1f64).ln()),
            after_peer: Some((0.2f64).ln()),
            after_band: Some((0.2f64).ln()),
            peer_evidence: 12.0,
            peer_weight: 0.5,
            band_evidence: 0.0,
            band_weight: 0.0,
            band: 3,
            spread: 0.5,
            horizon_hours: Some(6.0),
            estimate,
        }
    }

    /// The breakdown shows each level as a readable value with the change it
    /// made, the evidence behind it, and ends on the number routing uses.
    #[test]
    fn breakdown_rows_walk_the_levels_to_the_routing_estimate() {
        let rows = breakdown_rows(StageKind::ResponseTime, Some(&breakdown(0.257)));
        assert!(rows.contains("100.0 ms"), "the curve in ms: {rows}");
        assert!(rows.contains("&times;2.00"), "this peer doubles it: {rows}");
        assert!(
            rows.contains("12.0 effective outcomes, 50% adopted"),
            "{rows}"
        );
        assert!(rows.contains("ring band 3 (0.375&ndash;0.500)"), "{rows}");
        assert!(
            rows.contains("&times;1.28"),
            "exp(0.25) uncertainty: {rows}"
        );
        assert!(rows.contains("<strong>257.0 ms</strong>"), "{rows}");
        assert!(rows.contains("forgets over 6 h"), "{rows}");

        let failure = Breakdown {
            curve: 0.02,
            after_all_peers: None,
            after_peer: None,
            after_band: None,
            spread: 0.0,
            ..breakdown(0.02)
        };
        let rows = breakdown_rows(StageKind::Failure, Some(&failure));
        assert!(rows.contains("2.00%"), "{rows}");
        assert!(
            rows.contains("no spread between peers measured yet"),
            "{rows}"
        );
        assert!(
            !rows.contains("Allowance for uncertainty"),
            "a probability carries no expectation allowance: {rows}"
        );
    }

    #[test]
    fn breakdown_rows_name_the_fallback_for_a_cold_timing_stage() {
        let cold = breakdown_rows(StageKind::TransferSpeed, None);
        assert!(
            cold.contains("needs 30") && cold.contains("per-distance fit"),
            "{cold}"
        );
    }

    /// On the emergency fallback the breakdown card says its numbers are not
    /// what routing uses; otherwise it says nothing of the kind.
    #[test]
    fn breakdown_card_says_when_routing_is_on_the_fallback() {
        let on = build_breakdown_card(&[None, None, None], true);
        assert!(
            on.contains("emergency fallback") && on.contains("does not use them"),
            "{on}"
        );
        let off = build_breakdown_card(&[None, None, None], false);
        assert!(!off.contains("emergency fallback"), "{off}");
    }

    /// The failure axis stays zoomed to the drawn lines: a peer curve above
    /// the distance curve lifts it, one below does not lower it.
    #[test]
    fn failure_axis_keeps_the_peer_curve_on_screen() {
        let curve = [(0.0, 0.001), (0.5, 0.02)];
        let above = [(0.0, 0.002), (0.5, 0.05)];
        let below = [(0.0, 0.0), (0.5, 0.01)];
        assert!((failure_axis_top(&curve, PeerLine::Curve(&above)) - 0.10).abs() < 1e-9);
        assert!((failure_axis_top(&curve, PeerLine::Curve(&below)) - 0.04).abs() < 1e-9);
        assert!((failure_axis_top(&curve, PeerLine::None) - 0.04).abs() < 1e-9);
    }
}
