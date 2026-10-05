use super::assets::{CSS, JS, PEER_CSS};
use super::cards::format_bytes;
use super::estimator::{
    fmt_expected_total_time, fmt_prediction_prob, fmt_prediction_speed, fmt_prediction_time,
};
use super::*;
use crate::router::Breakdown;

// ─── Peer detail page ────────────────────────────────────────────────────────

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

    /// Outcomes a stage needs before it has a curve.
    fn needs(self) -> usize {
        match self {
            StageKind::Failure => crate::router::MIN_CURVE_POINTS_FAILURE,
            StageKind::ResponseTime | StageKind::TransferSpeed => {
                crate::router::MIN_CURVE_POINTS_LOG
            }
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
    if let Some(after_contract) = b.after_contract {
        rows.push_str(&row(
            "+ this contract, from every peer asked for it",
            &format!(
                "{} ({})",
                stage.render(after_contract),
                stage.change(b.curve, after_contract)
            ),
        ));
    }
    let before_all = b.after_contract.unwrap_or(b.curve);
    rows.push_str(&row(
        "+ what every peer has in common",
        &step(before_all, b.after_all_peers, String::new()),
    ));
    let before_peer = b.after_all_peers.unwrap_or(before_all);
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

    let peer_routing = match (&router_guard, &peer.peer_key_location) {
        (Some(router), Some(pkl)) => Some(router.peer_snapshot(pkl)),
        _ => None,
    };
    drop(router_guard);

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

    // Whether routing is on the emergency isotonic fallback.
    let fallback = crate::router::isotonic_fallback_enabled();

    // How the router builds its estimate for this peer, level by level.
    let breakdown_card = match &peer_routing {
        Some(pr) if pr.breakdown.iter().any(Option::is_some) => {
            build_breakdown_card(&pr.breakdown, fallback)
        }
        _ => String::new(),
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
                ett = fmt_expected_total_time(pred.expected_total_time, pred.transfer_speed_bps),
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
        prediction_card = prediction_card,
        breakdown_card = breakdown_card,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn durations_render_at_a_readable_scale() {
        assert_eq!(fmt_duration_secs(0.000_42), "420 &micro;s");
        assert_eq!(fmt_duration_secs(0.042), "42.0 ms");
        assert_eq!(fmt_duration_secs(4.2), "4.20 s");
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
            after_contract: None,
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
}
