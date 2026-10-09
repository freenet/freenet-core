//! Chart and number helpers shared by the dashboard's peer page and its
//! `/routing` page, so both read in the same units and the same scales.

use super::*;

/// A chart rendered at a desktop and a phone width; CSS shows the one that fits.
pub(super) fn responsive(render: impl Fn(f64) -> String) -> String {
    format!(
        r#"<div class="w-wide">{}</div><div class="w-narrow">{}</div>"#,
        render(600.0),
        render(340.0)
    )
}

/// A log axis: its ends in `log10` units, and the values to tick.
pub(super) struct LogAxis {
    pub lo: f64,
    pub hi: f64,
    pub ticks: Vec<f64>,
}

impl LogAxis {
    /// Covers every positive finite value with a little room either side, at
    /// least one decade wide. Ticks at the decades inside it, or at 1-2-5
    /// steps when fewer than two decades fall inside.
    pub(super) fn covering(values: impl Iterator<Item = f64>) -> Self {
        let (mut min, mut max) = (f64::INFINITY, f64::NEG_INFINITY);
        for value in values.filter(|v| v.is_finite() && *v > 0.0) {
            min = min.min(value);
            max = max.max(value);
        }
        if !min.is_finite() {
            (min, max) = (0.1, 1.0);
        }
        let (mut lo, mut hi) = ((min / 1.3).log10(), (max * 1.3).log10());
        if hi - lo < 1.0 {
            let grow = (1.0 - (hi - lo)) / 2.0;
            lo -= grow;
            hi += grow;
        }
        let inside = |v: f64| v.log10() >= lo - 1e-9 && v.log10() <= hi + 1e-9;
        let decades: Vec<f64> = (lo.floor() as i32..=hi.ceil() as i32)
            .map(|d| 10f64.powi(d))
            .filter(|v| inside(*v))
            .collect();
        let ticks = if decades.len() >= 2 {
            decades
        } else {
            (lo.floor() as i32..=hi.ceil() as i32)
                .flat_map(|d| [1.0, 2.0, 5.0].map(|m| m * 10f64.powi(d)))
                .filter(|v| inside(*v))
                .collect()
        };
        LogAxis { lo, hi, ticks }
    }

    /// Position of `value` along the axis, 0 at the low end and 1 at the high.
    pub(super) fn fraction(&self, value: f64) -> f64 {
        ((value.max(1e-300).log10() - self.lo) / (self.hi - self.lo)).clamp(0.0, 1.0)
    }
}

/// Every other peer's dots as ONE path of zero-length round-capped segments,
/// about a fifth of the bytes of a `<circle>` each. These dots carry no
/// tooltip, and the page is re-fetched every five seconds.
pub(super) fn faint_dots(points: impl Iterator<Item = (f64, f64)>) -> String {
    let mut path = String::new();
    for (x, y) in points {
        write!(path, "M{x:.0} {y:.0}h0").ok();
    }
    if path.is_empty() {
        path
    } else {
        format!(r#"<path d="{path}" class="dots-faint"/>"#)
    }
}

/// A `<title>` tooltip, or nothing at phone width, where no pointer can hover.
pub(super) fn tooltip(text: &str, narrow: bool) -> String {
    if narrow || text.is_empty() {
        String::new()
    } else {
        format!("<title>{text}</title>")
    }
}

/// At most `max` of `points`, evenly strided, keeping their order.
pub(super) fn downsample(points: &[(f64, f64)], max: usize) -> Vec<(f64, f64)> {
    if max == 0 {
        return Vec::new();
    }
    if points.len() <= max {
        return points.to_vec();
    }
    let stride = points.len() as f64 / max as f64;
    (0..max)
        .map(|i| points[((i as f64 * stride) as usize).min(points.len() - 1)])
        .collect()
}

/// Linear interpolation on a curve sorted by distance.
pub(super) fn interpolate(curve: &[(f64, f64)], d: f64) -> Option<f64> {
    let first = curve.first()?;
    if d <= first.0 {
        return Some(first.1);
    }
    for pair in curve.windows(2) {
        let ((d0, v0), (d1, v1)) = (pair[0], pair[1]);
        if d <= d1 {
            let t = if d1 > d0 { (d - d0) / (d1 - d0) } else { 0.0 };
            return Some(v0 + t * (v1 - v0));
        }
    }
    curve.last().map(|last| last.1)
}

/// A whole number without decimals, otherwise one.
pub(super) fn fmt_trim(value: f64) -> String {
    if (value - value.round()).abs() < 1e-9 {
        format!("{value:.0}")
    } else {
        format!("{value:.1}")
    }
}

/// A duration for an axis or a tooltip.
pub(super) fn fmt_time(seconds: f64) -> String {
    if seconds < 0.001 {
        format!("{:.0} &micro;s", seconds * 1e6)
    } else if seconds < 1.0 {
        format!("{} ms", fmt_trim_sig(seconds * 1e3))
    } else {
        format!("{} s", fmt_trim_sig(seconds))
    }
}

/// Up to three significant figures, without trailing zeros: 100, 25, 2.5, 0.25.
pub(super) fn fmt_trim_sig(value: f64) -> String {
    if value >= 100.0 {
        format!("{value:.0}")
    } else if value >= 10.0 {
        fmt_trim(value)
    } else {
        let text = format!("{value:.2}");
        text.trim_end_matches('0').trim_end_matches('.').to_string()
    }
}

/// A transfer speed for an axis or a tooltip.
pub(super) fn fmt_speed(bytes_per_second: f64) -> String {
    let (value, unit) = if bytes_per_second >= 1e9 {
        (bytes_per_second / 1e9, "GB/s")
    } else if bytes_per_second >= 1e6 {
        (bytes_per_second / 1e6, "MB/s")
    } else if bytes_per_second >= 1e3 {
        (bytes_per_second / 1e3, "KB/s")
    } else {
        (bytes_per_second, "B/s")
    };
    format!("{} {unit}", fmt_trim_sig(value))
}
