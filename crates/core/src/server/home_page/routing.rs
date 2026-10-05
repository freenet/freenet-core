//! The network-wide routing model page, served at `/routing`.
//!
//! Everything here describes the router as a whole: how much it has learned,
//! how good its predictions are, and the deeper diagnostics an operator reads
//! when something looks wrong. What the router has learned about ONE peer is
//! on that peer's page (`peer_detail.rs`), which links here.

use super::assets::{CSS, JS, PEER_CSS};
use super::estimator::{
    build_accuracy_panel, build_estimator_chart_or_placeholder, failure_chart_y_max,
};
use super::*;
use crate::router::RouterSnapshotInfo;

/// What the page reads from the router, taken under its read lock and
/// rendered after the lock is released.
pub(super) struct RoutingInputs {
    pub snapshot: RouterSnapshotInfo,
    /// Peers evicted from the router's per-peer selection table.
    pub selection_evictions: u64,
}

/// Render `/routing` from the live router.
pub fn routing_html() -> String {
    let snap = network_status::get_snapshot();
    let version = snap
        .as_ref()
        .map_or_else(|| "?".to_string(), |s| s.version.clone());
    let inputs = network_status::get_router().map(|lock| {
        let router = lock.read();
        RoutingInputs {
            snapshot: router.snapshot(),
            selection_evictions: router.peer_selection_evictions(),
        }
    });
    render_routing_page(&version, inputs.as_ref())
}

/// [`routing_html`] from explicit inputs, so tests need no global state.
pub(super) fn render_routing_page(version: &str, inputs: Option<&RoutingInputs>) -> String {
    let body = match inputs {
        Some(inputs) => format!(
            "{model}{accuracy}{outcomes}{diagnostics}",
            model = model_card(&inputs.snapshot),
            accuracy = accuracy_card(&inputs.snapshot),
            outcomes = outcomes_card(&inputs.snapshot),
            diagnostics = diagnostics_card(&inputs.snapshot, inputs.selection_evictions),
        ),
        None => r#"<div class="card"><h2>Routing model</h2><p class="empty">Router data not available yet.</p></div>"#
            .to_string(),
    };
    format!(
        include_str!("assets/routing.html"),
        CSS = CSS,
        PEER_CSS = PEER_CSS,
        JS = JS,
        version = html_escape(version),
        body = body,
    )
}

fn row(label: &str, value: &str) -> String {
    format!(r#"<div class="info-label">{label}</div><div class="info-value">{value}</div>"#)
}

/// What the model is built on: which estimator routes, whether it predicts
/// yet, and how much data each stage holds.
fn model_card(rs: &RouterSnapshotInfo) -> String {
    let estimator = if rs.isotonic_fallback_enabled {
        "<strong>emergency fallback</strong> (FREENET_ROUTING_FALLBACK_ISOTONIC is set): \
         per-distance fit with a per-peer correction"
    } else {
        "hierarchical (default)"
    };
    let predicting = if rs.prediction_active {
        "yes".to_string()
    } else {
        format!(
            "no, routing by distance until 50 requests ({} so far)",
            rs.failure_events
        )
    };
    let rows = [
        row("Estimator", estimator),
        row("Predicting", &predicting),
        row(
            "Failure data",
            &fmt_stage_status(
                StageKind::Failure,
                rs.hierarchical_failure_events,
                rs.hierarchical_failure_active,
            ),
        ),
        row(
            "Response-time data",
            &fmt_stage_status(
                StageKind::ResponseTime,
                rs.hierarchical_response_time_events,
                rs.hierarchical_response_time_active,
            ),
        ),
        row(
            "Transfer-speed data",
            &fmt_stage_status(
                StageKind::TransferSpeed,
                rs.hierarchical_transfer_speed_events,
                rs.hierarchical_transfer_speed_active,
            ),
        ),
        row(
            "Forgetting horizon",
            &fmt_horizon(
                rs.hierarchical_failure_active,
                rs.hierarchical_failure_events,
                rs.hierarchical_failure_horizon_hours,
            ),
        ),
    ]
    .concat();
    format!(
        r#"<div class="card">
            <h2>Routing model</h2>
            <p class="caption">What your node has learned from its recent requests, across all peers.</p>
            <div class="info-grid">{rows}</div>
        </div>"#
    )
}

/// How good the predictions are: the scores, then the charts they summarise.
fn accuracy_card(rs: &RouterSnapshotInfo) -> String {
    let brier = match (rs.failure_brier, rs.failure_climatology_brier) {
        (Some(brier), Some(climatology)) => {
            format!("{brier:.4} &middot; average-rate guess {climatology:.4}")
        }
        (Some(brier), None) => format!("{brier:.4}"),
        _ => "&mdash;".to_string(),
    };
    let rows = [
        row(
            "Failure skill",
            &format!(
                "{} &middot; n={}",
                fmt_skill(rs.failure_skill_hierarchical),
                rs.hierarchical_failure_evaluated
            ),
        ),
        row("Brier score", &brier),
        row(
            "Response time, RMS",
            &fmt_seconds_error(
                rs.response_time_rmse_secs_isotonic,
                rs.response_time_rmse_secs_hierarchical,
                rs.response_time_scored,
                rs.response_time_weight,
            ),
        ),
        row(
            "Transfer time, RMS",
            &fmt_seconds_error(
                rs.transfer_time_rmse_secs_isotonic,
                rs.transfer_time_rmse_secs_hierarchical,
                rs.transfer_time_scored,
                rs.transfer_time_weight,
            ),
        ),
    ]
    .concat();
    let charts = build_accuracy_panel(
        // The charts derive their own scores from these pairs, so each score
        // describes the window drawn.
        &rs.hierarchical_failure_pairs,
        &rs.hierarchical_response_time_pairs,
        &rs.hierarchical_transfer_speed_pairs,
    );
    format!(
        r#"<div class="card">
            <h2>Prediction accuracy</h2>
            <p class="caption">Each dot is a forecast made before its outcome was known; on the dashed diagonal it was exact.</p>
            <div class="info-grid">{rows}</div>
            {charts}
        </div>"#
    )
}

/// Every recent outcome against ring distance, all peers, per operation type.
fn outcomes_card(rs: &RouterSnapshotInfo) -> String {
    let tab_names = ["All", "GET", "PUT", "UPDATE", "SUBSCRIBE"];
    let mut tab_labels = String::new();
    let mut tab_panels = String::new();
    let hierarchical = &rs.hierarchical_curves;

    for (i, &tab_name) in tab_names.iter().enumerate() {
        let tab_id = tab_name.to_lowercase();
        let all = tab_name == "All";
        let per_op = rs.per_op_curves.get(tab_name);
        // The All tab draws what routing predicts for a peer it has no record
        // of: the hierarchical curve once a stage has one, the isotonic fit
        // before (which is what routing uses meanwhile), and the isotonic fit
        // throughout on the emergency fallback. The per-operation tabs break
        // the isotonic fit down by operation; routing does not consult them.
        let pick = |hierarchical: &'_ [(f64, f64)], isotonic: &'_ [(f64, f64)]| {
            if hierarchical.is_empty() || rs.isotonic_fallback_enabled {
                isotonic.to_vec()
            } else {
                hierarchical.to_vec()
            }
        };
        let (
            f_curve,
            f_range,
            f_points,
            rt_curve,
            rt_range,
            rt_points,
            x_curve,
            x_range,
            x_points,
            count,
        ) = if all {
            (
                pick(&hierarchical.failure, &rs.failure_curve),
                rs.failure_data_range,
                rs.failure_points.clone(),
                pick(&hierarchical.response_time, &rs.response_time_curve),
                rs.response_time_data_range,
                rs.response_time_points.clone(),
                pick(&hierarchical.transfer_speed, &rs.transfer_rate_curve),
                rs.transfer_rate_data_range,
                rs.transfer_rate_points.clone(),
                rs.failure_events,
            )
        } else if let Some(c) = per_op {
            (
                c.failure_curve.clone(),
                c.failure_data_range,
                c.failure_points.clone(),
                c.response_time_curve.clone(),
                c.response_time_data_range,
                c.response_time_points.clone(),
                c.transfer_rate_curve.clone(),
                c.transfer_rate_data_range,
                c.transfer_rate_points.clone(),
                c.failure_events,
            )
        } else {
            Default::default()
        };

        let badge = if count > 0 {
            format!(r#" <span class="tab-count">{count}</span>"#)
        } else {
            String::new()
        };
        let dim = if count == 0 && !all { " tab-dim" } else { "" };
        let active = if i == 0 { " tab-active" } else { "" };
        write!(
            tab_labels,
            r#"<span class="tab-label{dim}{active}" data-tab="{tab_id}" onclick="switchTab(this)">{tab_name}{badge}</span>"#,
        )
        .ok();

        let content = if count == 0 && !all {
            format!(r#"<div class="empty-chart">No {tab_name} requests yet.</div>"#)
        } else {
            let fail_y_max = failure_chart_y_max(&f_curve).to_string();
            [
                build_estimator_chart_or_placeholder(
                    "Failure probability",
                    &f_curve,
                    &f_points,
                    f_range,
                    "0.0",
                    &fail_y_max,
                    "No outcomes yet.",
                ),
                build_estimator_chart_or_placeholder(
                    "Response time (s)",
                    &rt_curve,
                    &rt_points,
                    rt_range,
                    "0",
                    "auto",
                    "No timed replies yet.",
                ),
                build_estimator_chart_or_placeholder(
                    "Transfer speed (B/s)",
                    &x_curve,
                    &x_points,
                    x_range,
                    "0",
                    "auto",
                    "No data transfers yet.",
                ),
            ]
            .concat()
        };
        let panel_active = if i == 0 { " tab-panel-active" } else { "" };
        write!(
            tab_panels,
            r#"<div class="tab-panel{panel_active}" id="panel-{tab_id}">{content}</div>"#,
        )
        .ok();
    }

    format!(
        r#"<div class="card">
            <h2>Outcomes by ring distance</h2>
            <p class="caption">Recent outcomes across all peers. On All, the line is what your node predicts for a peer from distance alone.</p>
            <div class="tab-group">
                <div class="tab-bar">{tab_labels}</div>
                {tab_panels}
            </div>
        </div>"#
    )
}

/// The readings an operator needs only when something looks wrong, collapsed.
fn diagnostics_card(rs: &RouterSnapshotInfo, selection_evictions: u64) -> String {
    let ranks = &rs.selection_ranks;
    let rows = [
        row(
            "Events without a peer location",
            &fmt_unlocated_discards(rs.route_events_discarded_unlocated),
        ),
        row(
            "Peer-table evictions",
            &fmt_evictions(
                rs.hierarchical_peer_evictions,
                rs.hierarchical_peer_capacity,
            ),
        ),
        row(
            "Selection-table evictions",
            &selection_evictions.to_string(),
        ),
        row("Contract term", &fmt_contract_term(rs)),
        row(
            "Response-time log residuals",
            &fmt_log_shape(&rs.hierarchical_response_time_log_shape),
        ),
        row(
            "Transfer-speed log residuals",
            &fmt_log_shape(&rs.hierarchical_transfer_speed_log_shape),
        ),
        row(
            "Candidate window",
            &format!("{} closest peers", rs.consider_n_closest_peers),
        ),
        row("Decisions measured", &ranks.total.to_string()),
        row(
            "Mean chosen position",
            &ranks.mean_rank().map_or_else(
                || "&mdash;".to_string(),
                |m| format!("{m:.1} (0 = closest)"),
            ),
        ),
        row("Against a full window", &ranks.saturated.to_string()),
        row(
            "&#8627; from the farthest quarter",
            &ranks
                .far_quarter_share()
                .map_or_else(|| "&mdash;".to_string(), |s| format!("{:.1}%", s * 100.0)),
        ),
        row(
            "Window reading",
            &fmt_window_reading(ranks.far_quarter_share()),
        ),
    ]
    .concat();
    format!(
        r#"<div class="card">
            <details class="diag">
                <summary>Diagnostics</summary>
                <div class="info-grid">{rows}</div>
            </details>
        </div>"#
    )
}

/// Render a Brier skill score.
///
/// A skill within 0.01 of zero is "≈ 0": at that size the sign is noise, and
/// printing `-0.007` invites a reading the number cannot support. A clearly
/// negative skill says what it means, because worse than assuming the average
/// failure rate is a finding in itself.
fn fmt_skill(skill: Option<f64>) -> String {
    match skill {
        Some(value) if value.is_finite() => {
            if value.abs() < 0.01 {
                "&asymp; 0".to_string()
            } else if value < 0.0 {
                format!("{value:+.2} (worse than the average rate)")
            } else {
                format!("{value:+.2}")
            }
        }
        // Undefined rather than zero: an all-success window has no variation to
        // score against, which is a different statement from "no skill".
        _ => "&mdash; (no failures yet)".to_string(),
    }
}

/// The contract term's activation, in the order a reader needs it: whether it
/// could produce an effect at all, and then whether it actually did.
///
/// The estimable-refit count on its own does NOT answer "is the term doing
/// anything": the effect is also refused per query when a contract has too few
/// present peers, and on the recorded soak most failures are on contracts that
/// never reach that bar. So the applied counts come first in the sentence.
/// `tau2_contract` is shown with the number of contracts it rests on, because
/// it has no minimum group count and one contract reads the same as eighty.
fn fmt_contract_term(rs: &RouterSnapshotInfo) -> String {
    if rs.hierarchical_contract_estimable_refits == 0 {
        return format!(
            "never estimable ({} contracts tracked, {} evicted)",
            rs.hierarchical_contracts, rs.hierarchical_contract_evictions
        );
    }
    let tau2 = match rs.hierarchical_contract_tau2 {
        Some(value) => format!(
            "between-contract variance {value:.4} over {} contracts, {} entries",
            rs.hierarchical_contract_qualifying_contracts,
            rs.hierarchical_contract_qualifying_entries
        ),
        None => "no components at the last refit".to_string(),
    };
    // The floor's binding frequency: on the recorded gateway streams it bound
    // on every estimable refit, because cells there are mostly unanimous and
    // mostly tiny. A reader whose traffic differs needs to see that here.
    let floor = format!(
        "; evidence floor bound at {} of {} estimable refits",
        rs.hierarchical_contract_floor_bound_refits, rs.hierarchical_contract_estimable_refits
    );
    // The den gate's own frequency. An estimable refit on which fewer than two
    // contracts qualified produced NOTHING, so without this the refit count
    // above overstates what the term did.
    let den = match rs.hierarchical_contract_den_below_two_refits {
        0 => String::new(),
        below => format!(
            "; {below} of those refits had fewer than two qualifying contracts, so the term \
             was off"
        ),
    };
    format!(
        "{} residuals adjusted, {} forecasts offset; estimable at {} refits; \
         {tau2}{floor}{den}",
        rs.hierarchical_contract_effects_applied,
        rs.hierarchical_contract_forecast_offsets,
        rs.hierarchical_contract_estimable_refits,
    )
}

/// Route events the router discarded because their peer had no known
/// location. Zero is the expected value; anything else is a producer breaking
/// an invariant, and those events taught the estimators nothing.
fn fmt_unlocated_discards(count: u64) -> String {
    match count {
        0 => "0".to_string(),
        count => format!("<strong>{count}</strong>, not learned (a bug: please report it)"),
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
        return "&mdash; nothing both models forecast yet".to_string();
    };
    let numbers = format!(
        "hierarchical {} &middot; isotonic fallback {} &middot; n={scored}",
        fmt_duration_secs(hierarchical),
        fmt_duration_secs(isotonic)
    );
    if weight < crate::router::MIN_WEIGHT_FOR_VERDICT {
        return format!("{numbers} &middot; too little recent data to compare");
    }
    let verdict = if hierarchical <= isotonic {
        "hierarchical no worse"
    } else {
        "hierarchical worse"
    };
    format!("{numbers} &middot; {verdict}")
}

/// Render the hierarchical peer-table eviction count against its capacity.
///
/// Evictions mean churn is exceeding the headroom derived from
/// `max_connections`, which an operator can act on, so a non-zero count says so.
fn fmt_evictions(evictions: u64, capacity: usize) -> String {
    if evictions == 0 {
        format!("0 (capacity {capacity} per stage)")
    } else {
        format!("{evictions} (capacity {capacity} per stage): churn exceeds the table's headroom")
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
        " &middot; <strong>not lognormal</strong>: expected times may be inaccurate"
    } else {
        " &middot; consistent with lognormal"
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
        (_, true, None) => "none, remembers its whole window".to_string(),
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

/// Turn the far-quarter share into the sentence an operator actually acts on:
/// whether to go and widen the routing window. Its own function so the
/// threshold can be tested.
fn fmt_window_reading(share: Option<f64>) -> String {
    match share {
        None => "Not enough decisions against a full window to say yet.".to_string(),
        Some(share) if share >= WINDOW_TOO_NARROW_SHARE => format!(
            "<strong>Worth investigating:</strong> {:.0}% of full-window decisions chose from \
             the farthest quarter, so the better peer may often be one the router never scored.",
            share * 100.0
        ),
        Some(share) => format!(
            "The limit looks comfortable: only {:.0}% of full-window decisions chose from the \
             farthest quarter.",
            share * 100.0
        ),
    }
}

/// The three things the router estimates for a candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StageKind {
    Failure,
    ResponseTime,
    TransferSpeed,
}

impl StageKind {
    /// Outcomes a stage needs before it has a curve.
    fn needs(self) -> usize {
        match self {
            StageKind::Failure => crate::router::MIN_CURVE_POINTS_FAILURE,
            StageKind::ResponseTime | StageKind::TransferSpeed => {
                crate::router::MIN_CURVE_POINTS_LOG
            }
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
    format!("{events}, no curve yet (needs {}){fallback}", stage.needs())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> RouterSnapshotInfo {
        crate::router::Router::new(&[]).snapshot()
    }

    #[test]
    fn skill_near_zero_reads_as_zero_once_not_as_a_signed_number() {
        for near in [-0.009, -0.0001, 0.0, 0.007] {
            assert_eq!(fmt_skill(Some(near)), "&asymp; 0", "{near}");
        }
        let worse = fmt_skill(Some(-0.43));
        assert!(
            worse.contains("-0.43") && worse.contains("worse than the average rate"),
            "a clearly negative skill must say so in words, got {worse}"
        );
        assert_eq!(fmt_skill(Some(0.42)), "+0.42");
        // Just past the band on each side is a real reading.
        assert!(fmt_skill(Some(-0.011)).contains("worse"));
        assert_eq!(fmt_skill(Some(0.011)), "+0.01");
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
    fn eviction_rendering_flags_churn() {
        assert_eq!(fmt_evictions(0, 400), "0 (capacity 400 per stage)");
        let churning = fmt_evictions(12, 400);
        assert!(
            churning.contains("12") && churning.contains("headroom"),
            "{churning}"
        );
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
        assert!(fmt_seconds_error(None, Some(0.2), 0, 0.0).contains("nothing both models"));
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
            stale.contains("too little recent data") && !stale.contains("no worse"),
            "10,000 events long forgotten must not produce a verdict: {stale}"
        );
        let enough = fmt_seconds_error(Some(0.2), Some(0.1), 150, min);
        assert!(enough.contains("hierarchical no worse"), "{enough}");
    }

    /// The discard count is a should-be-zero invariant: zero renders plainly,
    /// anything else is flagged with the count.
    #[test]
    fn unlocated_discards_render_as_a_clean_zero_or_as_a_flagged_count() {
        assert_eq!(fmt_unlocated_discards(0), "0");
        let some = fmt_unlocated_discards(7);
        assert!(
            some.contains("<strong>7</strong>") && some.contains("not learned"),
            "{some}"
        );
    }

    /// `fmt_contract_term`'s branches. The floor-versus-estimable clause is the
    /// one that matters: a transposition of the two counts renders a sentence
    /// claiming the floor bound MORE often than there were refits to bind on.
    #[test]
    fn the_contract_term_row_reports_each_regime_distinctly() {
        let base = || {
            let mut info = snapshot();
            info.hierarchical_contracts = 12;
            info.hierarchical_contract_evictions = 3;
            info
        };

        // (a) never estimable: the row must say so and must not print counts
        // that would read as activity.
        let never = fmt_contract_term(&base());
        assert!(
            never.contains("never estimable") && never.contains("12 contracts tracked"),
            "{never}"
        );

        // (b) estimable with no components at the last refit.
        let mut info = base();
        info.hierarchical_contract_estimable_refits = 40;
        info.hierarchical_contract_floor_bound_refits = 40;
        info.hierarchical_contract_effects_applied = 7;
        info.hierarchical_contract_forecast_offsets = 9;
        let no_components = fmt_contract_term(&info);
        assert!(
            no_components.contains("no components at the last refit"),
            "{no_components}"
        );

        // (c) components present, the floor binding on every estimable refit,
        // and the den gate silent.
        info.hierarchical_contract_tau2 = Some(0.125);
        info.hierarchical_contract_qualifying_contracts = 41;
        info.hierarchical_contract_qualifying_entries = 323;
        let full = fmt_contract_term(&info);
        assert!(
            full.contains("evidence floor bound at 40 of 40 estimable refits"),
            "{full}"
        );
        assert!(
            full.contains("between-contract variance 0.1250 over 41 contracts"),
            "{full}"
        );
        assert!(
            !full.contains("fewer than two qualifying contracts"),
            "the den gate did not fire, so the row must not mention it: {full}"
        );

        // The transposition, stated as its own assertion so the failure names
        // the defect rather than a missing substring.
        let mut swapped = info.clone();
        swapped.hierarchical_contract_floor_bound_refits = 40;
        swapped.hierarchical_contract_estimable_refits = 11;
        assert!(
            fmt_contract_term(&swapped).contains("bound at 40 of 11"),
            "the row prints floor-bound BEFORE estimable, so a swap is visible"
        );

        // (d) the den gate firing on some of the estimable refits.
        info.hierarchical_contract_den_below_two_refits = 12;
        let gated = fmt_contract_term(&info);
        assert!(
            gated.contains("12 of those refits had fewer than two qualifying contracts"),
            "{gated}"
        );
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
        // A verdict without its number is not checkable by the reader.
        assert!(fmt_window_reading(Some(0.42)).contains("42%"));
        assert!(fmt_window_reading(Some(0.05)).contains("5%"));
        // The format string's line continuations must not leak whitespace.
        assert!(!fmt_window_reading(Some(0.42)).contains("  "));
    }

    #[test]
    fn window_reading_says_so_when_there_is_no_evidence_yet() {
        let reading = fmt_window_reading(None);
        assert!(reading.contains("Not enough decisions"), "{reading}");
        assert!(
            !reading.contains("comfortable") && !reading.contains("Worth investigating"),
            "absence of evidence must not render as either verdict, got {reading}"
        );
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

    /// The page renders every section from a real router, keeps the deep
    /// diagnostics collapsed, links back home, and refreshes by JS only.
    #[test]
    fn routing_page_renders_every_section_with_diagnostics_collapsed() {
        let inputs = RoutingInputs {
            snapshot: snapshot(),
            selection_evictions: 3,
        };
        let html = render_routing_page("0.2.141", Some(&inputs));
        for section in [
            "Routing model",
            "Prediction accuracy",
            "Outcomes by ring distance",
            "<summary>Diagnostics</summary>",
            "Selection-table evictions",
        ] {
            assert!(html.contains(section), "missing {section:?}");
        }
        let details = html.find("<details").expect("diagnostics are collapsed");
        assert!(
            html.find("Contract term").is_some_and(|at| at > details),
            "the contract term is a deep diagnostic and sits inside <details>"
        );
        assert!(
            !html.contains("<details open"),
            "diagnostics start collapsed"
        );
        assert!(html.contains(r#"href="/""#), "links back to the dashboard");
        assert!(!html.contains("http-equiv=\"refresh\""));
        assert!(
            html.matches(r#"<p class="caption">"#).count() <= 3,
            "at most one short caption per card"
        );
    }

    #[test]
    fn routing_page_without_a_router_says_so() {
        let html = render_routing_page("?", None);
        assert!(html.contains("Router data not available yet."));
    }
}
