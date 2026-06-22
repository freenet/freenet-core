use super::*;
use crate::router::AdjustmentMode;

/// Choose the top of the failure-probability chart's y-axis.
///
/// Failure probabilities for a healthy peer are tiny (often well under 0.1),
/// so a fixed 0.0–1.0 axis squashes the fitted curve flat against the bottom
/// and its shape is unreadable. Instead, scale the top of the axis to twice the
/// curve's value at the right edge of the plot (the largest distance shown,
/// 0.5), so the line occupies roughly the lower half of the chart. The PAV
/// failure estimator is monotonically increasing in distance, so the right edge
/// is the curve's maximum and 2x leaves headroom above the whole line.
///
/// Returns 1.0 (the original full-range axis) when there is no failure signal at
/// the right edge, avoiding a degenerate zero-height axis. The result is capped
/// at 1.0 since a probability can never exceed 1.0.
pub fn failure_chart_y_max(curve_points: &[(f64, f64)], peer_adjustment: Option<f64>) -> f64 {
    // y-value at the largest sampled distance (the right edge of the chart).
    let right_edge = curve_points
        .iter()
        .max_by(|a, b| a.0.total_cmp(&b.0))
        .map(|&(_, y)| y)
        .unwrap_or(0.0);
    // Keep the peer-adjusted line on-screen too: only an upward (positive)
    // adjustment can push its right edge above the global curve's.
    let right_edge = right_edge + peer_adjustment.unwrap_or(0.0).max(0.0);
    if right_edge <= 1e-9 {
        return 1.0;
    }
    (2.0 * right_edge).min(1.0)
}

/// Render the named estimator chart, or — when no data has been observed
/// yet — a titled placeholder. The placeholder keeps the slot visible so
/// users can always see every component of a routing prediction even when
/// some estimators have not yet received feedback. Hiding empty charts
/// masked the data-collection regression in the migration
/// that fed only `failure_estimator` and left `response_start_time` and
/// `transfer_rate` permanently empty.
#[allow(clippy::too_many_arguments)]
pub fn build_estimator_chart_or_placeholder(
    title: &str,
    curve_points: &[(f64, f64)],
    scatter_points: &[(f64, f64)],
    data_range: (f64, f64),
    peer_adjustment: Option<f64>,
    adjustment_mode: AdjustmentMode,
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
        scatter_points,
        data_range,
        peer_adjustment,
        adjustment_mode,
        peer_location,
        y_min_hint,
        y_max_hint,
    )
}

/// Build an SVG chart showing a PAV regression curve with optional per-peer adjustment.
///
/// `data_range` is `(data_x_min, data_x_max)` -- the x-range of actual regression data.
/// Points outside this range are extrapolated by the PAV crate and drawn as dashed lines.
#[allow(clippy::too_many_arguments)]
pub fn build_estimator_chart(
    title: &str,
    curve_points: &[(f64, f64)],
    scatter_points: &[(f64, f64)],
    data_range: (f64, f64),
    peer_adjustment: Option<f64>,
    adjustment_mode: AdjustmentMode,
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
    // Bottom padding leaves room for both the distance tick numbers and the
    // "Distance" axis title below them; plot height stays 160px.
    let h: f64 = 210.0;
    let pad_l: f64 = 50.0;
    let pad_r: f64 = 10.0;
    let pad_t: f64 = 10.0;
    let pad_b: f64 = 40.0;
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

        // Include the raw scatter so observed outliers aren't clipped.
        for (_, y) in scatter_points {
            if y.is_finite() {
                y_min = y_min.min(*y);
                y_max = y_max.max(*y);
            }
        }

        // Include peer-adjusted values in range if present
        if let Some(adj) = peer_adjustment {
            for (_, y) in curve_points {
                let adjusted = adjustment_mode.apply(*y, adj);
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

    // X-axis title: the x-axis is always ring distance (peer ↔ contract).
    write!(
        svg,
        r#"<text x="{x:.0}" y="{y:.0}" text-anchor="middle" class="axis-label">Distance</text>"#,
        x = pad_l + plot_w / 2.0,
        y = h - 6.0,
    )
    .ok();

    // Y-axis labels (3 ticks).
    //
    // Pick decimal places from the tick step so adjacent ticks stay
    // distinguishable. The failure chart now zooms to a very small range
    // (probabilities are tiny), where a fixed 2-decimal format would collapse
    // every tick to "0.00".
    let step = y_range / 2.0;
    let decimals: usize = if step <= 0.0 {
        3
    } else if step >= 10.0 {
        0
    } else if step >= 1.0 {
        1
    } else {
        // step in (0, 1): enough places for ~2 significant figures of the step.
        ((-step.log10()).ceil() as usize).saturating_add(1).min(9)
    };
    for i in 0..=2 {
        let frac = i as f64 / 2.0;
        let y_val = y_min + frac * y_range;
        let sy = to_svg_y(y_val);
        let label = format!("{y_val:.decimals$}");
        write!(
            svg,
            r#"<text x="{x}" y="{sy:.0}" text-anchor="end" class="axis-label">{label}</text>"#,
            x = pad_l - 4.0,
            sy = sy,
            label = label,
        )
        .ok();
    }

    // Raw observed outcomes (drawn first, under the isotonic fit). Each dot is one
    // actual event at its (distance, outcome); the spread shows how isotonic the
    // relationship really is.
    //
    // The failure chart zooms its y-axis to the tiny fitted probabilities (see
    // failure_chart_y_max), which would push the binary failure outcomes (y = 1.0)
    // off the top of the plot. Rather than dropping off-scale points — which would
    // hide every failure precisely on the healthy, low-probability peers the zoom
    // is meant to illuminate — clamp them to the nearest edge so they remain
    // visible as a row of dots at the boundary. (Auto-scaled charts always size
    // their range to include the scatter, so the clamp is a no-op there.)
    for &(x, y) in scatter_points {
        if !(x.is_finite() && y.is_finite()) {
            continue;
        }
        if !(x_min..=x_max).contains(&x) {
            continue;
        }
        write!(
            svg,
            r#"<circle cx="{cx:.1}" cy="{cy:.1}" r="1.8" fill="var(--text-muted)" opacity="0.35"/>"#,
            cx = to_svg_x(x),
            cy = to_svg_y(y.clamp(y_min, y_max)),
        )
        .ok();
    }

    // Helper: draw a curve with solid line in data range and dashed outside.
    // `points` are (x, y) pairs; `adj` is the peer adjustment, combined with each
    // y via `adjustment_mode.apply` (an additive offset or a multiplicative factor
    // depending on the mode). `adj == 0.0` is the neutral global curve.
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
            // Apply the per-peer adjustment via the same `AdjustmentMode` the
            // router uses (additive `y + adj`, or multiplicative `y * exp(adj)`),
            // so the drawn curve matches the router's prediction exactly. For the
            // un-adjusted global curve `adj == 0.0`, which is the neutral value in
            // both modes (`y + 0` / `y * e^0`), so it is drawn unchanged.
            //
            // Then clamp to the visible axis floor. In additive mode a negative
            // adjustment (a peer faster / more reliable than the global fit) can
            // drive the value below the axis — e.g. a negative "response time",
            // which is physically meaningless. The router clamps the same per-peer
            // estimate to >= 0 (`IsotonicEstimator::estimate_retrieval_time`);
            // clamping to `y_min` here keeps the drawn curve on-axis and consistent
            // with that, mirroring the scatter-point clamp above. (`y_min` is 0 for
            // all of these charts.)
            let y = adjustment_mode.apply(y, adj).max(y_min);
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

    // Global curve (teal, the brand accent)
    draw_curve(&mut svg, curve_points, 0.0, "var(--accent-primary)");

    // Peer-adjusted curve (violet — deliberately off the teal/green family so it
    // is not confused with the teal global curve)
    if let Some(adj) = peer_adjustment {
        draw_curve(&mut svg, curve_points, adj, "#8b5cf6");
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

pub fn fmt_prediction_time(v: f64) -> String {
    if v.is_finite() && (0.0..REASONABLE_TIME_LIMIT).contains(&v) {
        format!("{v:.3}s")
    } else {
        "N/A".to_string()
    }
}

pub fn fmt_prediction_speed(v: f64) -> String {
    if v.is_finite() && v > 0.0 {
        format!("{v:.0} B/s")
    } else {
        "N/A".to_string()
    }
}

pub fn fmt_prediction_prob(v: f64) -> String {
    if v.is_finite() && (0.0..=1.0).contains(&v) {
        format!("{v:.4}")
    } else {
        "N/A".to_string()
    }
}

/// Which kind of regression model a scatter chart is rendering, controlling the
/// axis unit formatting (durations vs throughput).
#[derive(Clone, Copy)]
pub enum RegKind {
    Time,
    Speed,
}

/// Build the renegade prediction-accuracy panel: one reliability diagram for the
/// binary failure model plus predicted-vs-actual scatters for the two regression
/// models (response time, transfer speed). Returns an empty string when no model
/// has scored any predictions yet, so a fresh node shows nothing rather than an
/// empty card.
pub fn build_renegade_accuracy_panel(
    failure_pairs: &[(f64, f64)],
    brier: Option<f64>,
    response_time_pairs: &[(f64, f64)],
    transfer_speed_pairs: &[(f64, f64)],
) -> String {
    if failure_pairs.is_empty() && response_time_pairs.is_empty() && transfer_speed_pairs.is_empty()
    {
        return String::new();
    }

    let failure = build_reliability_chart(failure_pairs, brier);
    let response = build_regression_chart("Response time", RegKind::Time, response_time_pairs);
    let transfer = build_regression_chart("Transfer speed", RegKind::Speed, transfer_speed_pairs);

    format!(
        r#"<div class="card">
        <h2>Prediction Accuracy</h2>
        <p style="font-size:0.8em;color:var(--text-muted);">
            How well the Renegade predictor's recent predictions matched reality (this scores
            the Renegade k-NN layer, not the distance-only fit in Outcomes vs Distance above).
            On the dashed diagonal predictions are perfect: for failure, predicted probability
            equals the observed failure rate (calibration); for the timing models, predicted
            equals actual.
        </p>
        <div style="display:flex;flex-wrap:wrap;gap:1rem;justify-content:flex-start;">
            {failure}
            {response}
            {transfer}
        </div>
    </div>"#,
        failure = failure,
        response = response,
        transfer = transfer,
    )
}

/// Reliability (calibration) diagram for the binary failure model.
/// X = predicted failure probability, Y = observed failure rate within each
/// predicted-probability bin. Dots on the diagonal mean the model is calibrated;
/// above the line it under-predicts failure, below it over-predicts.
pub fn build_reliability_chart(pairs: &[(f64, f64)], brier: Option<f64>) -> String {
    use std::fmt::Write;

    let valid: Vec<(f64, f64)> = pairs
        .iter()
        .copied()
        .filter(|(p, a)| p.is_finite() && a.is_finite())
        .map(|(p, a)| (p.clamp(0.0, 1.0), a))
        .collect();

    if valid.is_empty() {
        return mini_chart_placeholder("Failure (calibration)", "predicted prob vs observed rate");
    }

    let n = valid.len();
    let n_bins = 10usize;
    let mut bin_pred_sum = vec![0.0f64; n_bins];
    let mut bin_fail = vec![0usize; n_bins];
    let mut bin_total = vec![0usize; n_bins];
    for (p, a) in &valid {
        let bin = ((p * n_bins as f64) as usize).min(n_bins - 1);
        bin_pred_sum[bin] += p;
        bin_total[bin] += 1;
        if *a >= 0.5 {
            bin_fail[bin] += 1;
        }
    }

    let (w, h) = (260.0f64, 220.0f64);
    let (pad_l, pad_r, pad_t, pad_b) = (38.0f64, 12.0f64, 30.0f64, 30.0f64);
    let plot_w = w - pad_l - pad_r;
    let plot_h = h - pad_t - pad_b;
    let to_x = |v: f64| pad_l + v.clamp(0.0, 1.0) * plot_w;
    let to_y = |v: f64| pad_t + (1.0 - v.clamp(0.0, 1.0)) * plot_h;

    let mut svg = format!(
        r#"<svg viewBox="0 0 {w} {h}" width="{w}" height="{h}" class="accuracy-chart">"#,
        w = w as u32,
        h = h as u32,
    );

    write!(
        svg,
        r#"<text x="{x}" y="14" font-size="10" font-weight="600" fill="var(--text-secondary)">Failure (calibration)</text>"#,
        x = pad_l,
    )
    .ok();
    let headline = match brier {
        Some(b) if b.is_finite() => format!("Brier {b:.3} · n={n}"),
        _ => format!("n={n}"),
    };
    write!(
        svg,
        r#"<text x="{x}" y="26" font-size="9" fill="var(--text-muted)">{headline}</text>"#,
        x = pad_l,
    )
    .ok();

    write!(
        svg,
        r#"<rect x="{lx}" y="{ty}" width="{pw}" height="{ph}" fill="var(--bg-secondary)" rx="2"/>"#,
        lx = pad_l,
        ty = pad_t,
        pw = plot_w,
        ph = plot_h,
    )
    .ok();

    // perfect-calibration diagonal
    write!(
        svg,
        r#"<line x1="{x1:.1}" y1="{y1:.1}" x2="{x2:.1}" y2="{y2:.1}" stroke="var(--text-muted)" stroke-width="1" stroke-dasharray="4"/>"#,
        x1 = to_x(0.0),
        y1 = to_y(0.0),
        x2 = to_x(1.0),
        y2 = to_y(1.0),
    )
    .ok();

    // axis ticks at 0 / 0.5 / 1 on both axes
    for &v in &[0.0_f64, 0.5, 1.0] {
        write!(
            svg,
            r#"<text x="{x:.1}" y="{y:.1}" text-anchor="middle" font-size="8" fill="var(--text-muted)">{v}</text>"#,
            x = to_x(v),
            y = pad_t + plot_h + 12.0,
        )
        .ok();
        write!(
            svg,
            r#"<text x="{x:.1}" y="{y:.1}" text-anchor="end" font-size="8" fill="var(--text-muted)">{v}</text>"#,
            x = pad_l - 4.0,
            y = to_y(v) + 3.0,
        )
        .ok();
    }

    // calibration curve through non-empty bins (in predicted-probability order)
    let mut pts: Vec<(f64, f64, usize)> = Vec::new();
    for b in 0..n_bins {
        if bin_total[b] == 0 {
            continue;
        }
        let mean_pred = bin_pred_sum[b] / bin_total[b] as f64;
        let obs_rate = bin_fail[b] as f64 / bin_total[b] as f64;
        pts.push((mean_pred, obs_rate, bin_total[b]));
    }
    if pts.len() >= 2 {
        let path = pts
            .iter()
            .map(|(px, py, _)| format!("{:.1},{:.1}", to_x(*px), to_y(*py)))
            .collect::<Vec<_>>()
            .join(" ");
        write!(
            svg,
            r#"<polyline points="{path}" fill="none" stroke="var(--accent-primary, #58a6ff)" stroke-width="1.5" opacity="0.8"/>"#,
        )
        .ok();
    }
    let max_bin = bin_total.iter().copied().max().unwrap_or(1).max(1);
    for (px, py, count) in &pts {
        let r = 2.5 + 3.5 * (*count as f64 / max_bin as f64).sqrt();
        write!(
            svg,
            r#"<circle cx="{cx:.1}" cy="{cy:.1}" r="{r:.1}" fill="var(--accent-primary, #58a6ff)" opacity="0.85"/>"#,
            cx = to_x(*px),
            cy = to_y(*py),
        )
        .ok();
    }

    write!(
        svg,
        r#"<text x="{x:.1}" y="{y:.1}" text-anchor="middle" font-size="8" fill="var(--text-muted)">predicted fail prob</text>"#,
        x = pad_l + plot_w / 2.0,
        y = h - 1.0,
    )
    .ok();

    svg.push_str("</svg>");
    svg
}

/// Predicted-vs-actual scatter for a regression model (response time, transfer
/// speed) on log-log axes, since both targets span orders of magnitude. Points
/// on the dashed diagonal mean predicted == actual; the headline is the median
/// absolute percentage error over the retained window.
pub fn build_regression_chart(label: &str, kind: RegKind, pairs: &[(f64, f64)]) -> String {
    use std::fmt::Write;

    // Log axes require strictly-positive, finite values.
    let valid: Vec<(f64, f64)> = pairs
        .iter()
        .copied()
        .filter(|(p, a)| p.is_finite() && a.is_finite() && *p > 0.0 && *a > 0.0)
        .collect();

    if valid.len() < 2 {
        return mini_chart_placeholder(label, "predicted vs actual");
    }

    let n = valid.len();

    // Median absolute percentage error (robust to the heavy tails of latency /
    // throughput) as the single headline number.
    let mut apes: Vec<f64> = valid.iter().map(|(p, a)| ((p - a) / a).abs()).collect();
    apes.sort_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal));
    let mid = apes.len() / 2;
    let mdape = if apes.len() % 2 == 0 {
        (apes[mid - 1] + apes[mid]) / 2.0
    } else {
        apes[mid]
    };

    // Shared log range across predicted and actual so the diagonal is 45°.
    let mut lo = f64::INFINITY;
    let mut hi = f64::NEG_INFINITY;
    for (p, a) in &valid {
        lo = lo.min(p.min(*a));
        hi = hi.max(p.max(*a));
    }
    let mut log_lo = lo.log10();
    let mut log_hi = hi.log10();
    if (log_hi - log_lo) < 0.5 {
        // Pad a near-flat range so points don't all sit on one edge.
        let center = (log_hi + log_lo) / 2.0;
        log_lo = center - 0.5;
        log_hi = center + 0.5;
    } else {
        let pad = (log_hi - log_lo) * 0.08;
        log_lo -= pad;
        log_hi += pad;
    }
    let span = (log_hi - log_lo).max(1e-9);

    let (w, h) = (260.0f64, 220.0f64);
    let (pad_l, pad_r, pad_t, pad_b) = (38.0f64, 12.0f64, 30.0f64, 30.0f64);
    let plot_w = w - pad_l - pad_r;
    let plot_h = h - pad_t - pad_b;
    let to_x = |v: f64| pad_l + ((v.log10() - log_lo) / span) * plot_w;
    let to_y = |v: f64| pad_t + (1.0 - (v.log10() - log_lo) / span) * plot_h;

    let mut svg = format!(
        r#"<svg viewBox="0 0 {w} {h}" width="{w}" height="{h}" class="accuracy-chart">"#,
        w = w as u32,
        h = h as u32,
    );
    write!(
        svg,
        r#"<text x="{x}" y="14" font-size="10" font-weight="600" fill="var(--text-secondary)">{label}</text>"#,
        x = pad_l,
    )
    .ok();
    write!(
        svg,
        r#"<text x="{x}" y="26" font-size="9" fill="var(--text-muted)">median err {pct:.0}% · n={n}</text>"#,
        x = pad_l,
        pct = mdape * 100.0,
    )
    .ok();

    write!(
        svg,
        r#"<rect x="{lx}" y="{ty}" width="{pw}" height="{ph}" fill="var(--bg-secondary)" rx="2"/>"#,
        lx = pad_l,
        ty = pad_t,
        pw = plot_w,
        ph = plot_h,
    )
    .ok();

    // Power-of-ten gridlines + axis labels. When all points fall within a single
    // decade (the common steady-state case) there is no power-of-ten boundary in
    // range, so fall back to labelling the axis endpoints rather than rendering an
    // unlabelled axis.
    let first_decade = log_lo.ceil() as i32;
    let last_decade = log_hi.floor() as i32;
    let tick_vals: Vec<f64> = if first_decade <= last_decade {
        (first_decade..=last_decade)
            .map(|d| 10f64.powi(d))
            .collect()
    } else {
        vec![10f64.powf(log_lo), 10f64.powf(log_hi)]
    };
    for val in tick_vals {
        let gx = to_x(val);
        let gy = to_y(val);
        write!(
            svg,
            r#"<line x1="{gx:.1}" y1="{ty:.1}" x2="{gx:.1}" y2="{by:.1}" stroke="var(--text-muted)" stroke-width="0.3" stroke-dasharray="3"/>"#,
            ty = pad_t,
            by = pad_t + plot_h,
        )
        .ok();
        write!(
            svg,
            r#"<text x="{gx:.1}" y="{y:.1}" text-anchor="middle" font-size="8" fill="var(--text-muted)">{lbl}</text>"#,
            y = pad_t + plot_h + 12.0,
            lbl = fmt_reg_axis(kind, val),
        )
        .ok();
        write!(
            svg,
            r#"<text x="{x:.1}" y="{gy:.1}" text-anchor="end" font-size="8" fill="var(--text-muted)">{lbl}</text>"#,
            x = pad_l - 4.0,
            lbl = fmt_reg_axis(kind, val),
        )
        .ok();
    }

    // Perfect diagonal (predicted == actual).
    write!(
        svg,
        r#"<line x1="{x1:.1}" y1="{y1:.1}" x2="{x2:.1}" y2="{y2:.1}" stroke="var(--text-muted)" stroke-width="1" stroke-dasharray="4"/>"#,
        x1 = to_x(10f64.powf(log_lo)),
        y1 = to_y(10f64.powf(log_lo)),
        x2 = to_x(10f64.powf(log_hi)),
        y2 = to_y(10f64.powf(log_hi)),
    )
    .ok();

    for (p, a) in &valid {
        write!(
            svg,
            r#"<circle cx="{cx:.1}" cy="{cy:.1}" r="2.2" fill="var(--accent-primary, #58a6ff)" opacity="0.45"/>"#,
            cx = to_x(*p),
            cy = to_y(*a),
        )
        .ok();
    }

    write!(
        svg,
        r#"<text x="{x:.1}" y="{y:.1}" text-anchor="middle" font-size="8" fill="var(--text-muted)">predicted (x) vs actual (y)</text>"#,
        x = pad_l + plot_w / 2.0,
        y = h - 1.0,
    )
    .ok();

    svg.push_str("</svg>");
    svg
}

/// Compact axis label for a regression value: durations as s/ms/µs, throughput
/// as B/KB/MB/GB per second.
fn fmt_reg_axis(kind: RegKind, v: f64) -> String {
    match kind {
        RegKind::Time => {
            if v >= 1.0 {
                format!("{v:.0}s")
            } else if v >= 0.001 {
                format!("{:.0}ms", v * 1000.0)
            } else {
                format!("{:.0}µs", v * 1_000_000.0)
            }
        }
        RegKind::Speed => {
            if v >= 1e9 {
                format!("{:.0}GB/s", v / 1e9)
            } else if v >= 1e6 {
                format!("{:.0}MB/s", v / 1e6)
            } else if v >= 1e3 {
                format!("{:.0}KB/s", v / 1e3)
            } else {
                format!("{v:.0}B/s")
            }
        }
    }
}

/// A small placeholder chart shown while a model has too little data to plot.
fn mini_chart_placeholder(label: &str, sub: &str) -> String {
    let (w, h) = (260.0f64, 220.0f64);
    format!(
        r#"<svg viewBox="0 0 {w} {h}" width="{w}" height="{h}" class="accuracy-chart">
        <text x="38" y="14" font-size="10" font-weight="600" fill="var(--text-secondary)">{label}</text>
        <text x="{cx}" y="{cy}" text-anchor="middle" font-size="10" fill="var(--text-muted)">collecting data…</text>
        <text x="{cx}" y="{cy2}" text-anchor="middle" font-size="8" fill="var(--text-muted)">{sub}</text>
    </svg>"#,
        w = w as u32,
        h = h as u32,
        cx = w / 2.0,
        cy = h / 2.0,
        cy2 = h / 2.0 + 14.0,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // The plot's bottom edge (x-axis) in SVG user units, matching the geometry in
    // `build_estimator_chart`: pad_t (10) + plot_h (h 210 - pad_t 10 - pad_b 40 = 160).
    const PLOT_BOTTOM_Y: f64 = 170.0;

    // Extract the y-coordinates from every path drawn in the given stroke color.
    fn path_y_coords_for_color(svg: &str, color: &str) -> Vec<f64> {
        let mut ys = Vec::new();
        for seg in svg.split("<path") {
            if !seg.contains(color) {
                continue;
            }
            // The `d="..."` attribute precedes the stroke color in the same element.
            let Some(start) = seg.find("d=\"") else {
                continue;
            };
            let rest = &seg[start + 3..];
            let Some(end) = rest.find('"') else {
                continue;
            };
            for token in rest[..end].split_whitespace() {
                // Tokens look like `M50.0,170.0` or `L256.0,131.4`.
                let cleaned = token.trim_start_matches(['M', 'L']);
                if let Some((_, y)) = cleaned.split_once(',') {
                    if let Ok(v) = y.parse::<f64>() {
                        ys.push(v);
                    }
                }
            }
        }
        ys
    }

    #[test]
    fn peer_adjusted_curve_additive_never_renders_below_x_axis() {
        // Additive mode: small positive base curve with a strongly-negative peer
        // adjustment. The raw `y + adj` would be deeply negative and, on a y-axis
        // floored at 0, would draw the violet "Peer-adjusted" curve below the
        // x-axis without the clamp.
        let curve = [(0.0, 0.10), (0.25, 0.30), (0.50, 0.50)];
        let svg = build_estimator_chart(
            "Failure Probability",
            &curve,
            &[],           // no scatter
            (0.0, 0.5),    // entire range is data (solid line)
            Some(-1000.0), // strongly-negative peer adjustment
            AdjustmentMode::Additive,
            None,
            "0.0", // y floor
            "auto",
        );

        // "#8b5cf6" is the violet stroke used only for the peer-adjusted curve.
        let ys = path_y_coords_for_color(&svg, "#8b5cf6");
        assert!(
            !ys.is_empty(),
            "expected a peer-adjusted curve to be drawn; svg: {svg}"
        );
        for y in ys {
            assert!(
                y <= PLOT_BOTTOM_Y + 0.05,
                "peer-adjusted curve drawn below the x-axis (y={y} > {PLOT_BOTTOM_Y})"
            );
        }
    }

    #[test]
    fn peer_adjusted_curve_multiplicative_stays_non_negative() {
        // Multiplicative mode: a strongly-negative log-adjustment scales the curve
        // toward zero (`y * exp(-1000) ≈ 0`). It must never render below the axis.
        let curve = [(0.0, 0.10), (0.25, 0.30), (0.50, 0.50)];
        let svg = build_estimator_chart(
            "Response Time (s)",
            &curve,
            &[],
            (0.0, 0.5),
            Some(-1000.0), // log-ratio: exp(-1000) ≈ 0
            AdjustmentMode::Multiplicative,
            None,
            "0", // y floor as used for the response-time chart
            "auto",
        );
        let ys = path_y_coords_for_color(&svg, "#8b5cf6");
        assert!(!ys.is_empty(), "expected a peer-adjusted curve; svg: {svg}");
        for y in ys {
            assert!(
                y <= PLOT_BOTTOM_Y + 0.05,
                "multiplicative peer-adjusted curve drawn below the x-axis (y={y})"
            );
        }
    }

    #[test]
    fn peer_adjusted_curve_multiplicative_scales_above_global() {
        // A positive log-adjustment (ln 2) scales the curve UP by 2x, so the violet
        // peer-adjusted curve must sit ABOVE the teal global curve — i.e. at a
        // SMALLER SVG y (the y-axis points down). This confirms the chart applies
        // the multiplicative transform, not an additive offset, and renders it.
        let curve = [(0.0, 1.0), (0.5, 1.0)]; // flat global at 1.0
        let svg = build_estimator_chart(
            "Response Time (s)",
            &curve,
            &[],
            (0.0, 0.5),
            Some(std::f64::consts::LN_2), // factor exp(ln 2) = 2.0
            AdjustmentMode::Multiplicative,
            None,
            "0",
            "auto",
        );
        let violet = path_y_coords_for_color(&svg, "#8b5cf6");
        let teal = path_y_coords_for_color(&svg, "var(--accent-primary)");
        let violet_top = violet.iter().cloned().fold(f64::INFINITY, f64::min);
        let teal_top = teal.iter().cloned().fold(f64::INFINITY, f64::min);
        assert!(violet.iter().all(|y| *y <= PLOT_BOTTOM_Y + 0.05));
        assert!(
            violet_top < teal_top - 1.0,
            "multiplicative peer-adjusted curve (factor 2x) should sit above the \
             global curve: violet_top={violet_top} should be < teal_top={teal_top}"
        );
    }
}
