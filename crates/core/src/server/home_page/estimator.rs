use super::*;

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
pub fn failure_chart_y_max(curve_points: &[(f64, f64)]) -> f64 {
    // y-value at the largest sampled distance (the right edge of the chart).
    let right_edge = curve_points
        .iter()
        .max_by(|a, b| a.0.total_cmp(&b.0))
        .map(|&(_, y)| y)
        .unwrap_or(0.0);
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
    unit: ChartUnit,
    curve_points: &[(f64, f64)],
    scatter_points: &[(f64, f64)],
    data_range: (f64, f64),
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
        unit,
        curve_points,
        scatter_points,
        data_range,
        y_min_hint,
        y_max_hint,
    )
}

/// Build an SVG chart showing a distance curve over the raw outcomes behind it.
///
/// `data_range` is `(data_x_min, data_x_max)` -- the x-range of actual regression data.
/// Points outside this range are extrapolated by the PAV crate and drawn as dashed lines.
pub fn build_estimator_chart(
    title: &str,
    unit: ChartUnit,
    curve_points: &[(f64, f64)],
    scatter_points: &[(f64, f64)],
    data_range: (f64, f64),
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
    // ring-distance axis title below them; plot height stays 160px.
    let h: f64 = 210.0;
    let pad_l: f64 = 58.0;
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
        r#"<text x="{x:.0}" y="{y:.0}" text-anchor="middle" class="axis-label">ring distance between peer and contract (0 = same spot, 0.5 = opposite side)</text>"#,
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
        let label = match unit {
            ChartUnit::Probability => format!("{y_val:.decimals$}"),
            ChartUnit::Seconds => fmt_reg_axis(RegKind::Time, y_val),
            ChartUnit::BytesPerSecond => fmt_reg_axis(RegKind::Speed, y_val),
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
    //
    // All dots are ONE path of zero-length round-capped segments: a fifth of
    // the bytes of a <circle> each, on a page re-fetched every five seconds.
    let mut dots = String::new();
    for &(x, y) in scatter_points {
        if !(x.is_finite() && y.is_finite()) {
            continue;
        }
        if !(x_min..=x_max).contains(&x) {
            continue;
        }
        write!(
            dots,
            "M{:.0} {:.0}h0",
            to_svg_x(x),
            to_svg_y(y.clamp(y_min, y_max)),
        )
        .ok();
    }
    if !dots.is_empty() {
        write!(
            svg,
            r#"<path d="{dots}" class="scatter" fill="none" stroke="var(--text-muted)" stroke-width="3.6" stroke-linecap="round" opacity="0.35"/>"#,
        )
        .ok();
    }

    // Helper: draw a curve with solid line in data range and dashed outside.
    let draw_curve = |svg: &mut String, points: &[(f64, f64)], color: &str| {
        if points.len() < 2 {
            return;
        }
        let (data_lo, data_hi) = data_range;

        // Split points into segments: extrapolated-left, data, extrapolated-right
        let mut left_ext = Vec::new();
        let mut data_seg = Vec::new();
        let mut right_ext = Vec::new();

        for &(x, y) in points {
            // Clamp to the visible axis floor, mirroring the scatter-point
            // clamp above. (`y_min` is 0 for all of these charts.)
            let y = y.max(y_min);
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

    draw_curve(&mut svg, curve_points, "var(--accent-primary)");

    svg.push_str("</svg></div>");
    svg
}

/// Whether a transfer speed is the router's floor under a degenerate isotonic
/// estimate rather than a measurement: such a peer sorts after every working
/// one, and its speed and cost are placeholders. The peer page says so
/// instead of showing either.
pub(super) fn is_floored_speed(v: f64) -> bool {
    v.is_finite() && v > 0.0 && v <= crate::router::DEGENERATE_SPEED_FLOOR_BPS
}

/// What a distance chart's y-axis measures, for its tick labels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChartUnit {
    Probability,
    Seconds,
    BytesPerSecond,
}

/// Which kind of regression model a scatter chart is rendering, controlling the
/// axis unit formatting (durations vs throughput).
#[derive(Clone, Copy)]
pub enum RegKind {
    Time,
    Speed,
}

/// The predicted-vs-actual charts for the timing estimates routing acts on,
/// response time and transfer speed, from the hierarchical estimator's recent
/// forecasts, in one wrapping row. Returns an empty string when nothing has
/// been scored yet, so a fresh node shows no empty charts. (The failure
/// forecast is summarised in words: at a failure rate of about 1%, a
/// calibration chart of it is one dot.)
pub fn build_accuracy_panel(
    response_time_pairs: &[(f64, f64)],
    transfer_speed_pairs: &[(f64, f64)],
) -> String {
    if response_time_pairs.is_empty() && transfer_speed_pairs.is_empty() {
        return String::new();
    }
    let response = build_regression_chart("Response time", RegKind::Time, response_time_pairs);
    let transfer = build_regression_chart("Transfer speed", RegKind::Speed, transfer_speed_pairs);
    format!(r#"<div class="accuracy-row">{response}{transfer}</div>"#)
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

    // The factor within which half the outcomes landed, either way: the same
    // reading the peer page gives (robust to the heavy tails of latency and
    // throughput).
    let mut misses: Vec<f64> = valid.iter().map(|(p, a)| (p / a).max(a / p)).collect();
    misses.sort_by(f64::total_cmp);
    let typical_miss = misses[misses.len() / 2];

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
        r#"<text x="{x}" y="26" font-size="9" fill="var(--text-muted)">typically within &#215;{typical_miss:.1} · last {n}</text>"#,
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
        r#"<text x="{x:.1}" y="{y:.1}" text-anchor="middle" font-size="8" fill="var(--text-muted)">predicted (across) against actual (up)</text>"#,
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
                format!("{} s", trim_one(v))
            } else if v >= 0.001 {
                format!("{:.0} ms", v * 1000.0)
            } else if v > 0.0 {
                format!("{:.0} µs", v * 1_000_000.0)
            } else {
                "0".to_string()
            }
        }
        RegKind::Speed => {
            if v >= 1e9 {
                format!("{} GB/s", trim_one(v / 1e9))
            } else if v >= 1e6 {
                format!("{} MB/s", trim_one(v / 1e6))
            } else if v >= 1e3 {
                format!("{:.0} KB/s", v / 1e3)
            } else if v > 0.0 {
                format!("{v:.0} B/s")
            } else {
                "0".to_string()
            }
        }
    }
}

/// A value with one decimal, or none when it is whole.
fn trim_one(v: f64) -> String {
    if (v - v.round()).abs() < 0.05 {
        format!("{v:.0}")
    } else {
        format!("{v:.1}")
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
