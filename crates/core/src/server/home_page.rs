//! Homepage served at `/` when a user navigates to their local Freenet node.
//!
//! Renders a card-based dashboard showing connection status, peers, subscriptions,
//! and operation stats. Styled to match the global telemetry dashboard.

mod assets;
mod cards;
mod estimator;
mod favicon;
mod peer_detail;

use axum::extract::Path;
use axum::response::{Html, IntoResponse};

// Re-exported so child submodules can reach them via `use super::*;`.
// Safe to re-export at pub(super) because they are pub in their origin.
pub(super) use crate::node::network_status::{self, format_ago, format_duration, html_escape};
pub(super) use std::fmt::Write;

use assets::{CSS, JS};
use cards::{
    build_ban_list_card, build_contracts_card, build_governance_card, build_hosting_card,
    build_ops_card, build_peers_card, build_status_card, build_transfer_card,
};
use favicon::build_favicon_data_uri;
use peer_detail::peer_detail_html;

/// Freenet rabbit silhouette SVG path, derived from freenet_logo.svg.
/// Used for the favicon with a solid color fill (no gradient) so the
/// connection status color is immediately visible at favicon size.
/// Kept in root (not favicon.rs) so `path_handlers.rs` can reach it as
/// `super::home_page::RABBIT_SVG_PATH` without an extra re-export.
pub(super) const RABBIT_SVG_PATH: &str = concat!(
    "M358.864 40.470C358.605 40.728 354.143 42.467 348.947 44.334",
    "C284.621 67.446 232.573 113.729 201.443 175.500",
    "C193.895 190.478 184.375 213.708 185.375 214.708",
    "C185.621 214.954 187.715 211.857 190.030 207.827",
    "C211.190 170.984 229.863 146.093 255.968 119.933",
    "C274.854 101.008 282.998 94.207 302.034 81.466",
    "C334.671 59.621 367.531 47.376 401.250 44.492",
    "L409.000 43.829 408.984 47.165",
    "C408.958 52.704 405.255 68.515 401.010 81.213",
    "C382.392 136.898 338.799 184.709 277.000 217.224",
    "C271.225 220.263 263.913 223.788 260.750 225.058",
    "C254.629 227.517 254.126 228.307 256.511 231.712",
    "C258.282 234.241 258.484 234.089 249.500 237.002",
    "C226.868 244.341 200.420 256.771 183.918 267.825",
    "C173.918 274.522 156.961 289.225 158.000 290.296",
    "C158.275 290.579 163.450 287.694 169.500 283.883",
    "C175.550 280.073 186.125 274.083 193.000 270.573",
    "C264.905 233.856 345.414 226.155 422.387 248.633",
    "C434.634 252.210 468.194 264.823 465.830 264.961",
    "C465.461 264.982 459.741 263.440 453.118 261.534",
    "C422.666 252.769 376.068 246.967 347.500 248.384",
    "C320.590 249.719 284.052 255.527 283.798 258.510",
    "C283.694 259.727 291.796 264.541 298.477 267.231",
    "C306.163 270.326 319.360 270.612 338.812 268.103",
    "C385.602 262.070 433.627 269.250 469.963 287.712",
    "C475.721 290.638 480.874 293.474 481.416 294.016",
    "C482.061 294.661 476.225 295.004 464.450 295.010",
    "C407.520 295.043 349.853 308.084 300.500 332.086",
    "C290.412 336.992 272.833 346.834 271.147 348.519",
    "C269.278 350.389 273.301 349.263 283.966 344.933",
    "C302.548 337.389 332.479 327.629 351.000 323.074",
    "C386.266 314.400 413.893 311.393 450.000 312.298",
    "C474.559 312.914 491.602 315.535 509.306 321.421",
    "C520.784 325.236 519.954 325.640 504.924 323.552",
    "C419.615 311.701 330.506 332.225 238.000 385.033",
    "C224.991 392.460 221.855 394.386 200.762 407.913",
    "C184.591 418.282 178.817 420.978 172.750 420.990",
    "C167.060 421.002 164.441 418.869 163.413 413.387",
    "C160.912 400.055 178.394 366.762 202.136 339.644",
    "L206.387 334.788 197.443 335.644",
    "C183.073 337.019 158.519 336.519 150.152 334.681",
    "C132.833 330.876 120.785 321.947 117.439 310.437",
    "C112.326 292.850 123.492 270.717 146.912 252.015",
    "C154.528 245.934 155.702 244.562 156.798 240.464",
    "C158.983 232.296 168.599 206.900 174.208 194.482",
    "C184.044 172.710 197.989 150.083 213.332 131.000",
    "C229.597 110.770 255.612 87.415 277.277 73.590",
    "C286.990 67.393 310.855 55.436 323.062 50.653",
    "C335.623 45.730 360.750 38.583 358.864 40.470",
    "M375.000 209.596",
    "C430.712 216.655 477.541 237.609 509.241 269.661",
    "C514.049 274.523 518.765 279.625 519.721 281.000",
    "C521.404 283.419 521.347 283.408 517.980 280.669",
    "C500.484 266.434 486.556 257.516 468.000 248.668",
    "C452.323 241.193 438.766 236.261 424.000 232.663",
    "C395.297 225.667 374.955 223.024 349.705 223.010",
    "C333.212 223.000 332.865 222.956 330.455 220.545",
    "C329.105 219.195 328.000 217.046 328.000 215.768",
    "C328.000 212.845 330.558 209.125 333.357 207.978",
    "C336.189 206.817 360.536 207.763 375.000 209.596",
);

/// Handler for `GET /` — returns a self-contained HTML dashboard.
pub(super) async fn homepage() -> impl IntoResponse {
    Html(homepage_html())
}

/// Handler for `GET /peer/{address}` — returns a detail page for a single peer.
pub(super) async fn peer_detail(Path(address): Path<String>) -> impl IntoResponse {
    Html(peer_detail_html(&address))
}

fn homepage_html() -> String {
    let snap = network_status::get_snapshot();

    let (version, uptime) = match &snap {
        Some(s) => (s.version.as_str(), format_duration(s.elapsed_secs)),
        None => ("?", "0s".to_string()),
    };

    let favicon = build_favicon_data_uri(&snap);

    let status_card = build_status_card(&snap);
    let peers_card = build_peers_card(&snap);
    let hosting_card = build_hosting_card(&snap);
    let governance_card = build_governance_card(&snap);
    let ban_list_card = build_ban_list_card(&snap);
    let contracts_card = build_contracts_card(&snap);
    let ops_card = build_ops_card(&snap);
    let transfer_card = build_transfer_card(&snap);

    let peer_id = snap
        .as_ref()
        .and_then(|s| {
            if s.ring_stats.peer_id.is_empty() {
                None
            } else {
                Some(s.ring_stats.peer_id.as_str())
            }
        })
        .unwrap_or("?");
    let pub_key = snap
        .as_ref()
        .and_then(|s| {
            if s.ring_stats.own_pub_key.is_empty() {
                None
            } else {
                Some(s.ring_stats.own_pub_key.as_str())
            }
        })
        .unwrap_or("?");
    let peer_copy_btn = if peer_id == "?" {
        String::new()
    } else {
        r#"<button class="copy-btn" onclick="copyToClipboard(document.getElementById('peer-id').textContent).then(function(){showToast('Peer ID copied')})" title="Copy peer ID">&#x2398;</button>"#
            .to_string()
    };
    let pub_copy_btn = if pub_key == "?" {
        String::new()
    } else {
        r#"<button class="copy-btn" onclick="copyToClipboard(document.getElementById('pub-key').textContent).then(function(){showToast('Pub key copied')})" title="Copy public key">&#x2398;</button>"#
            .to_string()
    };

    format!(
        include_str!("home_page/assets/home.html"),
        CSS = CSS,
        JS = JS,
        favicon = favicon,
        version = html_escape(version),
        // Asset version = the build that compiled THIS served page. Baked in at
        // compile time so the JS can compare it against the live runtime version
        // fetched from /v1/version and warn when a cached page is stale (#4289).
        asset_version = html_escape(crate::config::PCK_VERSION),
        uptime = uptime,
        peer_id = html_escape(peer_id),
        peer_copy_btn = peer_copy_btn,
        pub_key = html_escape(pub_key),
        pub_copy_btn = pub_copy_btn,
        status_card = status_card,
        peers_card = peers_card,
        transfer_card = transfer_card,
        hosting_card = hosting_card,
        governance_card = governance_card,
        ban_list_card = ban_list_card,
        contracts_card = contracts_card,
        ops_card = ops_card,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    // Explicit imports from submodules (not re-exported from root).
    use super::assets::JS;
    use super::cards::{
        build_ban_list_card, build_contracts_card, build_governance_card, build_ops_card,
        build_peers_card, build_ring_svg, build_status_card, build_transfer_card, format_bytes,
        format_last_evaluated,
    };
    use super::estimator::{
        RegKind, build_estimator_chart, build_estimator_chart_or_placeholder,
        build_regression_chart, build_reliability_chart, build_renegade_accuracy_panel,
        failure_chart_y_max, fmt_prediction_prob, fmt_prediction_speed, fmt_prediction_time,
    };
    use super::favicon::build_favicon_data_uri;
    use super::peer_detail::peer_detail_html;
    use crate::node::network_status::{
        FailureSnapshot, HealthLevel, NatStatsSnapshot, NetworkStatusSnapshot, OpStatsSnapshot,
        RingStatsSnapshot,
    };
    use crate::router::AdjustmentMode;
    use crate::transport::metrics::TransportSnapshot;
    use std::net::SocketAddr;

    fn base_snapshot() -> NetworkStatusSnapshot {
        NetworkStatusSnapshot {
            failures: Vec::new(),
            connection_attempts: 0,
            open_connections: 0,
            elapsed_secs: 10,
            listening_port: 31337,
            version: "0.1.0".to_string(),
            own_location: None,
            external_address: None,
            peers: Vec::new(),
            contracts: Vec::new(),
            op_stats: OpStatsSnapshot::default(),
            nat_stats: NatStatsSnapshot::default(),
            gateway_only: false,
            bytes_uploaded: 0,
            bytes_downloaded: 0,
            health: HealthLevel::Connecting,
            ring_stats: RingStatsSnapshot::default(),
            transport_snapshot: TransportSnapshot::default(),
            governance: Default::default(),
            ban_list: Default::default(),
            hosting: Default::default(),
        }
    }

    // ── Asset/runtime version mismatch banner (#4289) ──────────────────────

    /// Reference specification of the "stale assets" banner rule, used to test
    /// the logic the homepage JS implements client-side.
    ///
    /// The actual mismatch detection runs in the browser (see
    /// `checkVersionMismatch` in [`JS`]): the served page bakes in its
    /// `asset_version` (the build that generated the HTML/JS the browser is
    /// running) and fetches the live `runtime_version` from `/v1/version` at page
    /// load. This Rust function is the canonical, unit-testable statement of when
    /// the banner should appear; the JS mirrors it exactly. Keeping it here (and
    /// tested) makes the rule's edge cases explicit and guards against the JS
    /// drifting from the intended behaviour. It lives in the test module (not as
    /// production code) because the production decision is made in JS, not in the
    /// server-side render — and keeping it inside the single `#[cfg(test)]`
    /// boundary preserves the source-scrape pin invariant relied on by
    /// `peer_detail_panel_calls_estimator_helper_for_all_three_components` (the
    /// first `#[cfg(test)]` marker must be the production/test boundary).
    ///
    /// The mismatch is meaningful in the #3967 / #4289 scenario: a browser is
    /// still holding a cached homepage emitted by an old binary while a newer
    /// binary is now the process actually answering requests. In that case the
    /// asset version (frozen in the cached page) differs from the live runtime
    /// version, and the page is genuinely stale — the user should refresh.
    ///
    /// The banner is shown **only** when both versions are known and they
    /// differ. A missing/unknown version on either side (`""` or `"?"`, e.g. the
    /// node is mid-startup and `network_status` has no snapshot yet) is treated
    /// as "can't tell" and never triggers the banner, so the warning cannot fire
    /// spuriously during startup. The comparison is an exact string match: any
    /// difference in the published version string (including pre-release suffixes
    /// like `0.2.68-rc1`) is a real asset/runtime divergence worth surfacing.
    fn should_show_version_banner(asset_version: &str, runtime_version: &str) -> bool {
        if !version_is_known(asset_version) || !version_is_known(runtime_version) {
            return false;
        }
        asset_version != runtime_version
    }

    /// A version string is "known" when it is non-empty and not the `"?"`
    /// placeholder used by the homepage when no `network_status` snapshot exists.
    /// Reference for the JS `versionIsKnown`; see [`should_show_version_banner`].
    fn version_is_known(version: &str) -> bool {
        !version.is_empty() && version != "?"
    }

    #[test]
    fn version_banner_hidden_when_versions_match() {
        assert!(
            !should_show_version_banner("0.2.49", "0.2.49"),
            "identical asset and runtime versions must not show the banner"
        );
    }

    #[test]
    fn version_banner_shown_when_versions_differ() {
        assert!(
            should_show_version_banner("0.2.37", "0.2.49"),
            "a stale cached asset version vs a newer runtime must show the banner"
        );
        // Direction is irrelevant: any divergence is worth surfacing.
        assert!(
            should_show_version_banner("0.2.49", "0.2.37"),
            "asset newer than runtime is still a mismatch worth surfacing"
        );
    }

    #[test]
    fn version_banner_treats_prerelease_suffixes_as_distinct() {
        // Pre-release / build-metadata suffixes are part of the published
        // version string; an exact mismatch there is a real divergence.
        assert!(
            should_show_version_banner("0.2.68-rc1", "0.2.68"),
            "0.2.68-rc1 and 0.2.68 are different builds and must mismatch"
        );
        assert!(
            !should_show_version_banner("0.2.68-rc1", "0.2.68-rc1"),
            "identical pre-release strings must not show the banner"
        );
    }

    #[test]
    fn version_banner_hidden_when_either_version_unknown() {
        // Startup race: node has no snapshot yet, so the runtime version is
        // the "?" placeholder or empty. The banner must not fire spuriously.
        assert!(
            !should_show_version_banner("0.2.49", "?"),
            "unknown runtime version (?) must not trigger the banner"
        );
        assert!(
            !should_show_version_banner("0.2.49", ""),
            "empty runtime version must not trigger the banner"
        );
        assert!(
            !should_show_version_banner("?", "0.2.49"),
            "unknown asset version (?) must not trigger the banner"
        );
        assert!(
            !should_show_version_banner("", "0.2.49"),
            "empty asset version must not trigger the banner"
        );
        assert!(
            !should_show_version_banner("?", "?"),
            "both unknown must not trigger the banner"
        );
    }

    #[test]
    fn homepage_renders_version_mismatch_banner_slot() {
        let html = homepage_html();
        assert!(
            html.contains("id=\"version-mismatch-banner\""),
            "homepage must render a hidden banner slot the JS can reveal on mismatch"
        );
        assert!(
            html.contains("data-asset-version="),
            "banner slot must carry the compile-time asset version for the JS comparison"
        );
        // The slot must ship hidden so it never flashes before the live check.
        let slot = extract_element_line(&html, "id=\"version-mismatch-banner\"");
        assert!(
            slot.contains("hidden"),
            "version mismatch banner must be hidden by default, got: {slot}"
        );
    }

    #[test]
    fn js_contains_version_mismatch_check() {
        assert!(
            JS.contains("checkVersionMismatch"),
            "JS must include the asset/runtime version mismatch check"
        );
        assert!(
            JS.contains("/v1/version"),
            "JS must query the runtime version endpoint"
        );
        // The JS must mirror should_show_version_banner: skip unknown ('?'/'')
        // versions so the banner can't fire during startup.
        assert!(
            JS.contains("data-asset-version"),
            "JS must read the baked-in asset version from the banner slot"
        );
    }

    /// Pins the import-data UI wiring (#4592, file-upload path): the homepage
    /// must render the import card's open button plus the modal form (file +
    /// key inputs), and the modal must ship hidden. A source-scrape guard so a
    /// refactor that drops the wiring fails CI rather than silently shipping a
    /// dead "Import data file" button.
    #[test]
    fn homepage_renders_import_ui_wiring() {
        let html = homepage_html();
        assert!(
            html.contains("import-open-btn"),
            "homepage must render the import-data open button"
        );
        for id in [
            "id=\"import-modal\"",
            "id=\"import-file\"",
            "id=\"import-key\"",
            "id=\"import-submit\"",
        ] {
            assert!(
                html.contains(id),
                "homepage must render import modal element `{id}`"
            );
        }
        // The modal ships hidden so it never flashes before the user opens it.
        let modal = extract_element_line(&html, "id=\"import-modal\"");
        assert!(
            modal.contains("hidden"),
            "import modal must be hidden by default, got: {modal}"
        );
    }

    /// The served dashboard JS must POST the upload to `/v1/import` with the
    /// bundle-key header the endpoint requires — the load-bearing half of the
    /// export→import round-trip. Pinned so a JS refactor can't silently break
    /// it while leaving the button in place.
    #[test]
    fn js_contains_import_upload_wiring() {
        assert!(
            JS.contains("/v1/import"),
            "JS must POST the bundle to the /v1/import endpoint"
        );
        assert!(
            JS.contains("X-Freenet-Bundle-Key"),
            "JS must send the bundle decryption key in the X-Freenet-Bundle-Key header"
        );
        assert!(
            JS.contains("runImport") && JS.contains("openImportModal"),
            "JS must define the import submit + modal-open handlers"
        );
    }

    #[test]
    fn favicon_grey_when_no_snapshot() {
        let uri = build_favicon_data_uri(&None);
        assert!(uri.starts_with("data:image/svg+xml,"));
        assert!(uri.contains("%239e9e9e"), "expected grey color");
    }

    #[test]
    fn favicon_teal_when_connected() {
        let mut snap = base_snapshot();
        snap.open_connections = 3;
        let uri = build_favicon_data_uri(&Some(snap));
        assert!(uri.contains("%230abab5"), "expected teal color");
    }

    #[test]
    fn favicon_dark_red_when_nat_fails() {
        let mut snap = base_snapshot();
        snap.nat_stats.attempts = 5;
        snap.nat_stats.successes = 0;
        let uri = build_favicon_data_uri(&Some(snap));
        assert!(uri.contains("%238b0000"), "expected dark red color");
    }

    #[test]
    fn favicon_red_when_failures_present() {
        let mut snap = base_snapshot();
        snap.failures.push(FailureSnapshot {
            address: "1.2.3.4:1234".parse::<SocketAddr>().unwrap(),
            reason_html: "timeout".to_string(),
        });
        let uri = build_favicon_data_uri(&Some(snap));
        assert!(uri.contains("%23f44336"), "expected red color");
    }

    #[test]
    fn favicon_amber_when_connecting() {
        let snap = base_snapshot();
        let uri = build_favicon_data_uri(&Some(snap));
        assert!(uri.contains("%23fbbf24"), "expected amber color");
    }

    #[test]
    fn last_evaluated_footer_has_single_ago() {
        // Regression: format_ago already appends " ago", so the footer
        // template must not add a second one. Previously rendered
        // "Last evaluated 18s ago ago".
        assert_eq!(format_last_evaluated(18), "Last evaluated 18s ago");
        assert!(!format_last_evaluated(18).contains("ago ago"));
        // Under 5s, format_ago returns "just now" (no "ago" suffix at all).
        assert_eq!(format_last_evaluated(2), "Last evaluated just now");
    }

    #[test]
    fn favicon_connected_overrides_failures() {
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.nat_stats.attempts = 5;
        snap.nat_stats.successes = 0;
        snap.failures.push(FailureSnapshot {
            address: "1.2.3.4:1234".parse().unwrap(),
            reason_html: "timeout".to_string(),
        });
        let uri = build_favicon_data_uri(&Some(snap));
        assert!(
            uri.contains("%230abab5"),
            "connected should override failure colors"
        );
    }

    #[test]
    fn favicon_amber_when_nat_partially_succeeds() {
        let mut snap = base_snapshot();
        snap.nat_stats.attempts = 10;
        snap.nat_stats.successes = 3;
        let uri = build_favicon_data_uri(&Some(snap));
        assert!(
            uri.contains("%23fbbf24"),
            "partial NAT success should show amber, not dark red"
        );
    }

    #[test]
    fn favicon_dark_red_over_red_when_both_present() {
        let mut snap = base_snapshot();
        snap.nat_stats.attempts = 5;
        snap.nat_stats.successes = 0;
        snap.failures.push(FailureSnapshot {
            address: "1.2.3.4:1234".parse().unwrap(),
            reason_html: "timeout".to_string(),
        });
        let uri = build_favicon_data_uri(&Some(snap));
        assert!(
            uri.contains("%238b0000"),
            "NAT failure should take priority over connection failures"
        );
    }

    #[test]
    fn favicon_embedded_in_homepage() {
        let html = homepage_html();
        assert!(
            html.contains(r#"rel="icon" type="image/svg+xml" href="data:image/svg+xml,"#),
            "homepage should contain favicon data URI"
        );
    }

    #[test]
    fn health_banner_shown_for_each_level() {
        // Healthy
        let mut snap = base_snapshot();
        snap.health = HealthLevel::Healthy;
        snap.open_connections = 3;
        let html = build_status_card(&Some(snap));
        assert!(html.contains("health-good"), "healthy banner missing");
        assert!(html.contains("Node is healthy"));

        // Degraded
        let mut snap = base_snapshot();
        snap.health = HealthLevel::Degraded;
        snap.gateway_only = true;
        snap.open_connections = 1;
        let html = build_status_card(&Some(snap));
        assert!(html.contains("health-degraded"), "degraded banner missing");

        // Connecting
        let snap = base_snapshot(); // default is Connecting
        let html = build_status_card(&Some(snap));
        assert!(
            html.contains("health-connecting"),
            "connecting banner missing"
        );

        // Trouble
        let mut snap = base_snapshot();
        snap.health = HealthLevel::Trouble;
        let html = build_status_card(&Some(snap));
        assert!(html.contains("health-trouble"), "trouble banner missing");
    }

    #[test]
    fn failures_demoted_when_connected() {
        let mut snap = base_snapshot();
        snap.open_connections = 3;
        snap.health = HealthLevel::Healthy;
        snap.failures.push(FailureSnapshot {
            address: "1.2.3.4:1234".parse().unwrap(),
            reason_html: "NAT traversal failed".to_string(),
        });
        let html = build_status_card(&Some(snap));
        // Should use <details> (collapsed) instead of prominent .diagnostics
        assert!(
            html.contains("diagnostics-muted"),
            "failures should be demoted when connected"
        );
        assert!(
            !html.contains(r#"class="diagnostics""#),
            "should not use prominent diagnostics style"
        );
        assert!(
            html.contains("(normal)"),
            "should indicate failures are normal"
        );
    }

    #[test]
    fn rate_limit_stats_hidden_when_no_traffic() {
        // A fresh node (no relayed UPDATEs yet) must not render the
        // UPDATE rate-limiter row — keeps the idle dashboard uncluttered.
        let snap = base_snapshot();
        assert_eq!(snap.ring_stats.updates_accepted, 0);
        let html = build_status_card(&Some(snap));
        assert!(
            !html.contains("Rate-limited"),
            "rate-limit row should be hidden when the limiter has seen no traffic"
        );
    }

    #[test]
    fn rate_limit_stats_rendered_when_active() {
        // Once the limiter has dropped traffic, the operator must see the
        // accepted / rate-limited / capacity-dropped counts on the card.
        let mut snap = base_snapshot();
        snap.ring_stats.updates_accepted = 1234;
        snap.ring_stats.updates_rate_limited = 56;
        snap.ring_stats.updates_capacity_dropped = 7;
        let html = build_status_card(&Some(snap));
        assert!(html.contains("UPDATEs relayed"), "accepted label missing");
        assert!(html.contains("1234</span>"), "accepted count missing");
        assert!(html.contains("Rate-limited"), "rate-limited label missing");
        assert!(html.contains("56</span>"), "rate-limited count missing");
        assert!(
            html.contains("Capacity-dropped"),
            "capacity-dropped label missing"
        );
        assert!(html.contains("7</span>"), "capacity-dropped count missing");
    }

    #[test]
    fn rate_limit_stats_shown_when_only_accepted() {
        // Boundary: accepted>0 but zero drops should still render the row
        // (operators want to see the limiter is active and healthy).
        let mut snap = base_snapshot();
        snap.ring_stats.updates_accepted = 10;
        let html = build_status_card(&Some(snap));
        assert!(
            html.contains("UPDATEs relayed"),
            "row should render once any UPDATE has been accepted"
        );
    }

    #[test]
    fn rate_limit_stats_shown_when_only_rate_limited() {
        // Boundary: the row's most operator-critical trigger. Independently
        // exercises the SECOND term of the OR guard so a `||`->`&&` change,
        // a dropped term, or a field-name swap is caught. A node that has
        // ONLY ever rate-limited (accepted/capacity both zero) must still
        // surface the signal.
        let mut snap = base_snapshot();
        snap.ring_stats.updates_rate_limited = 1;
        let html = build_status_card(&Some(snap));
        assert!(
            html.contains("Rate-limited"),
            "row must render when the limiter has rate-limited traffic"
        );
        assert!(html.contains("1</span>"), "rate-limited count missing");
    }

    #[test]
    fn rate_limit_stats_shown_when_only_capacity_dropped() {
        // Boundary: independently exercises the THIRD term of the OR guard
        // (capacity drops signal identity churn / admission pressure). A
        // node that has ONLY ever capacity-dropped must still surface it.
        let mut snap = base_snapshot();
        snap.ring_stats.updates_capacity_dropped = 1;
        let html = build_status_card(&Some(snap));
        assert!(
            html.contains("Capacity-dropped"),
            "row must render when the limiter has capacity-dropped traffic"
        );
        assert!(html.contains("1</span>"), "capacity-dropped count missing");
    }

    #[test]
    fn failures_prominent_when_not_connected() {
        let mut snap = base_snapshot();
        snap.open_connections = 0;
        snap.health = HealthLevel::Connecting;
        snap.failures.push(FailureSnapshot {
            address: "1.2.3.4:1234".parse().unwrap(),
            reason_html: "timeout".to_string(),
        });
        let html = build_status_card(&Some(snap));
        assert!(
            html.contains(r#"class="diagnostics""#),
            "failures should be prominent when not connected"
        );
        assert!(!html.contains("diagnostics-muted"), "should not be demoted");
    }

    #[test]
    fn transfer_card_shows_bytes() {
        let mut snap = base_snapshot();
        snap.bytes_uploaded = 1024 * 1024 * 5; // 5 MB
        snap.open_connections = 1;
        let html = build_transfer_card(&Some(snap));
        assert!(html.contains("5.0 MB"), "should show formatted bytes");
        assert!(html.contains("Uploaded"), "should show upload label");
    }

    #[test]
    fn transfer_card_hidden_when_fresh_start() {
        let mut snap = base_snapshot();
        snap.elapsed_secs = 3; // first few seconds, no traffic yet
        let html = build_transfer_card(&Some(snap));
        assert!(
            html.is_empty(),
            "transfer card should be hidden in first 10s with no data"
        );
    }

    // Superseded: the 10s grace period replaced the unconditional
    // hide-on-zero-open-connections logic (#3507).
    #[ignore]
    #[test]
    fn transfer_card_hidden_when_no_data() {
        let mut snap = base_snapshot();
        snap.elapsed_secs = 100; // well past grace, still no data
        // Old behaviour: hidden. New behaviour: shown (tested above).
    }

    #[test]
    fn transfer_card_shown_after_grace_period() {
        let snap = base_snapshot(); // elapsed_secs=10, no traffic → still show
        let html = build_transfer_card(&Some(snap));
        assert!(
            !html.is_empty(),
            "transfer card should render after grace period even without data"
        );
    }

    /// Non-zero transport metrics must render RTT, cwnd, slowdown, and
    /// transfer sub-sections — currently every test uses default zeros,
    /// which skips those branches entirely.
    #[test]
    fn transfer_card_renders_subsections_with_data() {
        let mut snap = base_snapshot();
        snap.bytes_uploaded = 5000;
        snap.open_connections = 1;
        snap.transport_snapshot.avg_rtt_us = 12500; // 12.5ms
        snap.transport_snapshot.min_rtt_us = 8000;
        snap.transport_snapshot.max_rtt_us = 45000;
        snap.transport_snapshot.avg_cwnd_bytes = 32768;
        snap.transport_snapshot.peak_cwnd_bytes = 98304;
        snap.transport_snapshot.min_cwnd_bytes = 11264;
        snap.transport_snapshot.slowdowns_triggered = 23;
        snap.transport_snapshot.transfers_completed = 847;
        snap.transport_snapshot.transfers_failed = 3;
        snap.transport_snapshot.avg_transfer_time_ms = 1200;
        let html = build_transfer_card(&Some(snap));
        assert!(html.contains("RTT"), "RTT row missing");
        assert!(html.contains("12.5ms"), "avg RTT missing");
        assert!(html.contains("8.0ms"), "min RTT missing");
        assert!(html.contains("45.0ms"), "max RTT missing");
        assert!(html.contains("cwnd"), "cwnd row missing");
        assert!(html.contains("LEDBAT"), "slowdown row missing");
        assert!(html.contains("23</span>"), "slowdown count missing");
        assert!(html.contains("847"), "transfers completed missing");
        assert!(html.contains("3"), "transfers failed missing");
    }

    #[test]
    fn format_bytes_units() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
        assert_eq!(format_bytes(1024 * 1024 * 1024 * 3 / 2), "1.5 GB");
    }

    #[test]
    fn external_links_open_in_new_tab() {
        // Verify all external links in the template have target="_blank"
        let html = homepage_html();
        for line in html.lines() {
            if line.contains("href=\"https://") {
                assert!(
                    line.contains("target=\"_blank\""),
                    "external link missing target=\"_blank\": {line}"
                );
                assert!(
                    line.contains("rel=\"noopener noreferrer\""),
                    "external link missing rel=\"noopener noreferrer\": {line}"
                );
            }
        }
    }

    #[test]
    fn no_meta_refresh_in_homepage() {
        let html = homepage_html();
        assert!(
            !html.contains("http-equiv=\"refresh\""),
            "meta refresh must not be present — JS partial update is used instead"
        );
    }

    #[test]
    fn title_is_fn_peer() {
        let html = homepage_html();
        assert!(
            html.contains("<title>FN Peer</title>"),
            "dashboard title must be 'FN Peer' — do not rename without updating this test"
        );
    }

    #[test]
    fn subscribe_cell_shows_active_count() {
        use crate::node::network_status::ContractSnapshot;

        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.op_stats.subscribes = (250, 3);
        snap.contracts = vec![
            ContractSnapshot {
                key_short: "ABC1...".to_string(),
                key_full: "ABC123".to_string(),
                instance_id: "ABC123".to_string(),
                subscribed_secs: 100,
                last_updated_secs: Some(5),
                is_receiving_updates: true,
                in_use: true,
            },
            ContractSnapshot {
                key_short: "DEF4...".to_string(),
                key_full: "DEF456".to_string(),
                instance_id: "DEF456".to_string(),
                subscribed_secs: 50,
                last_updated_secs: None,
                is_receiving_updates: true,
                in_use: true,
            },
        ];
        let html = build_ops_card(&Some(snap));
        assert!(
            html.contains("2 active"),
            "should show active subscription count, got: {html}"
        );
        assert!(
            html.contains("253 ops"),
            "should show total ops as secondary info, got: {html}"
        );
        assert!(
            !html.contains("\u{2713} 250"),
            "should not show raw success/fail format for subscribes"
        );
    }

    #[test]
    fn subscribe_cell_zero_ops_shows_active_only() {
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.op_stats.gets = (1, 0); // need some ops so card renders
        let html = build_ops_card(&Some(snap));
        assert!(
            html.contains("0 active"),
            "should show 0 active when no subscriptions, got: {html}"
        );
        assert!(!html.contains("0 ops"), "should hide ops line when zero");
    }

    #[test]
    fn no_meta_refresh_in_peer_detail() {
        let html = peer_detail_html("127.0.0.1:31337");
        assert!(
            !html.contains("http-equiv=\"refresh\""),
            "meta refresh must not be present — JS partial update is used instead"
        );
    }

    #[test]
    fn js_contains_auto_refresh() {
        assert!(
            JS.contains("scheduleRefresh"),
            "JS constant must contain the auto-refresh scheduler"
        );
    }

    #[test]
    fn reliability_chart_empty_is_placeholder() {
        let svg = build_reliability_chart(&[], None);
        assert!(svg.contains("collecting data"));
        assert!(svg.contains("<svg"));
    }

    #[test]
    fn reliability_chart_renders_points_and_brier() {
        // Well-separated: low predicted -> success, high predicted -> failure.
        let pairs: Vec<(f64, f64)> = (0..20)
            .map(|i| (i as f64 / 20.0, if i > 10 { 1.0 } else { 0.0 }))
            .collect();
        let svg = build_reliability_chart(&pairs, Some(0.042));
        assert!(svg.contains("Failure (calibration)"));
        assert!(svg.contains("Brier 0.042"));
        assert!(svg.contains("n=20"));
        assert!(svg.contains("<circle"), "bins should render as points");
    }

    #[test]
    fn reliability_chart_filters_nonfinite() {
        let pairs = vec![
            (0.5, 0.0),
            (f64::NAN, 1.0),
            (0.3, f64::NAN),
            (f64::INFINITY, 0.0),
            (0.7, 1.0),
        ];
        // Only 2 valid pairs survive the finite filter.
        let svg = build_reliability_chart(&pairs, None);
        assert!(svg.contains("n=2"));
    }

    #[test]
    fn reliability_chart_boundary_values_no_panic() {
        // p == 1.0 and p == 0.0 must clamp into a bin without panicking.
        let svg = build_reliability_chart(&[(1.0, 0.0), (0.0, 1.0)], Some(0.5));
        assert!(svg.contains("<svg"));
        assert!(svg.contains("n=2"));
    }

    #[test]
    fn regression_chart_sparse_is_placeholder() {
        // Fewer than 2 valid (positive, finite) points -> placeholder.
        let svg = build_regression_chart("Response time", RegKind::Time, &[(0.5, 0.4)]);
        assert!(svg.contains("collecting data"));
        assert!(svg.contains("Response time"));
    }

    #[test]
    fn regression_chart_time_renders_scatter_and_metric() {
        // predicted ~= actual, sub-second range -> ms axis labels, low error.
        let pairs: Vec<(f64, f64)> = (1..=20)
            .map(|i| {
                let actual = i as f64 * 0.01; // 10ms..200ms
                (actual * 1.1, actual) // 10% over-prediction
            })
            .collect();
        let svg = build_regression_chart("Response time", RegKind::Time, &pairs);
        assert!(svg.contains("Response time"));
        assert!(svg.contains("median err"));
        assert!(svg.contains("<circle"));
        assert!(
            svg.contains("ms"),
            "sub-second axis should be labelled in ms"
        );
    }

    #[test]
    fn regression_chart_speed_uses_byte_units() {
        let pairs: Vec<(f64, f64)> = (1..=20)
            .map(|i| {
                let actual = i as f64 * 100_000.0; // up to ~2 MB/s
                (actual, actual)
            })
            .collect();
        let svg = build_regression_chart("Transfer speed", RegKind::Speed, &pairs);
        assert!(svg.contains("Transfer speed"));
        assert!(
            svg.contains("KB/s") || svg.contains("MB/s"),
            "throughput axis should use byte-rate units"
        );
    }

    #[test]
    fn regression_chart_filters_nonpositive() {
        // Zero/negative/non-finite values can't be plotted on a log axis and are
        // dropped, leaving too few points -> placeholder.
        let pairs = vec![(0.0, 1.0), (-1.0, 2.0), (f64::NAN, 3.0), (5.0, 0.0)];
        let svg = build_regression_chart("Response time", RegKind::Time, &pairs);
        assert!(svg.contains("collecting data"));
    }

    #[test]
    fn regression_chart_all_equal_points_render_finite() {
        // Every point identical -> degenerate log range. Must pad the range and
        // produce finite coordinates (no NaN) rather than dividing by a zero span.
        let pairs = vec![(1000.0, 1000.0), (1000.0, 1000.0), (1000.0, 1000.0)];
        let svg = build_regression_chart("Transfer speed", RegKind::Speed, &pairs);
        assert!(svg.contains("<svg"));
        assert!(
            svg.contains("<circle"),
            "should plot the overlapping points"
        );
        assert!(!svg.contains("NaN"), "no NaN coordinates, got: {svg}");
        // Single-decade fallback must still produce axis labels.
        assert!(svg.contains("B/s"), "endpoint axis labels should render");
    }

    #[test]
    fn regression_chart_single_decade_labels_endpoints() {
        // All points within one decade (no power-of-ten boundary lands in the
        // padded range): the chart must fall back to labelling the axis endpoints
        // rather than rendering an unlabelled axis.
        let pairs: Vec<(f64, f64)> = (0..12)
            .map(|i| {
                let a = 0.16 + (i as f64) * 0.04; // 0.16..0.60 s, within one decade
                (a, a)
            })
            .collect();
        let svg = build_regression_chart("Response time", RegKind::Time, &pairs);
        assert!(svg.contains("<svg"));
        assert!(!svg.contains("NaN"), "no NaN coords, got: {svg}");
        assert!(
            svg.contains("ms"),
            "endpoint axis labels should render in the single-decade case, got: {svg}"
        );
    }

    #[test]
    fn accuracy_panel_empty_when_no_data() {
        assert_eq!(
            build_renegade_accuracy_panel(&[], None, &[], &[]),
            String::new()
        );
    }

    #[test]
    fn accuracy_panel_renders_all_three_models() {
        let failure: Vec<(f64, f64)> = (0..20)
            .map(|i| (i as f64 / 20.0, if i > 10 { 1.0 } else { 0.0 }))
            .collect();
        let svg = build_renegade_accuracy_panel(&failure, Some(0.05), &[], &[]);
        assert!(svg.contains("Prediction Accuracy"));
        assert!(svg.contains("Failure (calibration)"));
        // Timing models have no data yet -> their placeholders still appear.
        assert!(svg.contains("Response time"));
        assert!(svg.contains("Transfer speed"));
    }

    #[test]
    fn fmt_prediction_time_sentinel_values() {
        assert_eq!(fmt_prediction_time(f64::MAX / 2.0), "N/A");
        assert_eq!(fmt_prediction_time(f64::INFINITY), "N/A");
        assert_eq!(fmt_prediction_time(f64::NAN), "N/A");
        assert_eq!(fmt_prediction_time(-1.0), "N/A");
        assert_eq!(fmt_prediction_time(0.0), "0.000s");
        assert_eq!(fmt_prediction_time(1.5), "1.500s");
        assert_eq!(fmt_prediction_time(1.0e9), "N/A"); // at the limit
        assert_eq!(fmt_prediction_time(999_999_999.0), "999999999.000s");
    }

    #[test]
    fn fmt_prediction_speed_sentinel_values() {
        assert_eq!(fmt_prediction_speed(0.0), "N/A");
        assert_eq!(fmt_prediction_speed(-5.0), "N/A");
        assert_eq!(fmt_prediction_speed(f64::NAN), "N/A");
        assert_eq!(fmt_prediction_speed(f64::INFINITY), "N/A");
        assert_eq!(fmt_prediction_speed(1024.0), "1024 B/s");
    }

    /// Regression: with no data the helper must still emit a titled
    /// placeholder so all three prediction-component slots
    /// (Failure Probability, Response Time, Transfer Rate) stay
    /// visible on the peer dashboard. The previous behaviour hid the
    /// chart entirely, which masked the driver data-collection
    /// regression that left response-time/transfer-rate estimators
    /// permanently empty (Failure-Probability-only dashboard).
    #[test]
    fn build_estimator_chart_or_placeholder_empty_renders_titled_placeholder() {
        let html = build_estimator_chart_or_placeholder(
            "Response Time (s)",
            &[],
            &[],
            (0.0, 0.0),
            None,
            AdjustmentMode::Additive,
            None,
            "0",
            "auto",
            "No timed responses have been observed from this peer yet.",
        );
        assert!(
            html.contains("<h3>Response Time (s)</h3>"),
            "empty placeholder must still display the chart title so the user sees the slot is part of the model, got: {html}"
        );
        assert!(
            html.contains("No timed responses have been observed"),
            "empty placeholder must explain why no curve is shown, got: {html}"
        );
        assert!(
            !html.contains("<svg"),
            "empty placeholder must not render an SVG, got: {html}"
        );
    }

    #[test]
    fn estimator_chart_overlays_raw_scatter() {
        // A fit curve plus raw observations: the chart must draw the scatter dots
        // (circles) behind the isotonic curve so the spread is visible.
        let curve = vec![(0.0, 0.1), (0.25, 0.5), (0.5, 0.9)];
        let scatter = vec![(0.05, 0.0), (0.1, 1.0), (0.3, 0.0), (0.4, 1.0)];
        let html = build_estimator_chart_or_placeholder(
            "Failure Probability",
            &curve,
            &scatter,
            (0.0, 0.5),
            None,
            AdjustmentMode::Additive,
            None,
            "0.0",
            "1.0",
            "no data",
        );
        assert!(html.contains("<svg"), "should render an SVG, got: {html}");
        assert!(
            html.contains("<circle"),
            "raw observations should render as scatter circles, got: {html}"
        );
    }

    /// Regression: the per-tab "Outcomes vs Distance" panel must call
    /// `build_estimator_chart_or_placeholder` for all three
    /// prediction-component slots (Failure Probability, Response Time,
    /// Transfer Rate). Hiding empty slots previously masked the
    /// driver data-collection regression for months — keeping every
    /// slot visible makes future regressions detectable on sight.
    /// Source-scrape rather than HTML-grep because the visible-when-empty
    /// behaviour depends on a router_snapshot being present, and the
    /// `home_page.rs::tests` module does not have a snapshot fixture
    /// builder.
    #[test]
    fn peer_detail_panel_calls_estimator_helper_for_all_three_components() {
        let src = include_str!("home_page/peer_detail.rs");
        let prod = src;
        for title in [
            "Failure Probability",
            "Response Time (s)",
            "Transfer Rate (B/s)",
        ] {
            // Find the helper call site and walk forward up to 200 bytes
            // for the title literal. Whitespace-tolerant so rustfmt
            // doesn't churn this pin.
            let mut found = false;
            let mut cursor = 0;
            while let Some(call) = prod[cursor..].find("build_estimator_chart_or_placeholder(") {
                let abs = cursor + call;
                let tail_end = (abs + 400).min(prod.len());
                let needle = format!("\"{title}\"");
                if prod[abs..tail_end].contains(&needle) {
                    found = true;
                    break;
                }
                cursor = abs + 1;
            }
            assert!(
                found,
                "peer-detail panel builder must call \
                 build_estimator_chart_or_placeholder with title {title:?} so the slot is \
                 always visible. Without this every prediction-component \
                 slot can silently disappear when its estimator has no \
                 data — the original regression."
            );
        }
    }

    #[test]
    fn build_estimator_chart_or_placeholder_renders_chart_when_data_present() {
        let curve = vec![(0.0, 0.1), (0.25, 0.5), (0.5, 0.9)];
        let html = build_estimator_chart_or_placeholder(
            "Failure Probability",
            &curve,
            &[],
            (0.0, 0.5),
            None,
            AdjustmentMode::Additive,
            None,
            "0.0",
            "1.0",
            "should not see this",
        );
        assert!(html.contains("<svg"), "non-empty curve must render an SVG");
        assert!(
            !html.contains("should not see this"),
            "non-empty curve must not show the empty-message text"
        );
    }

    #[test]
    fn failure_chart_y_max_zooms_to_twice_right_edge() {
        // Monotonic, tiny failure curve: right edge is 0.04 → axis top 0.08, so
        // the line sits around mid-height instead of hugging y=0.
        let curve = vec![(0.0, 0.001), (0.25, 0.02), (0.5, 0.04)];
        let y_max = failure_chart_y_max(&curve, None);
        assert!((y_max - 0.08).abs() < 1e-9, "expected 0.08, got {y_max}");
    }

    #[test]
    fn failure_chart_y_max_falls_back_to_full_range_without_signal() {
        // No failures observed (all-zero curve) → keep the original 0..1 axis
        // rather than collapsing to a degenerate zero-height range.
        let curve = vec![(0.0, 0.0), (0.5, 0.0)];
        assert_eq!(failure_chart_y_max(&curve, None), 1.0);
        // An empty curve also falls back to the full range.
        assert_eq!(failure_chart_y_max(&[], None), 1.0);
    }

    #[test]
    fn failure_chart_y_max_capped_at_one() {
        // A large right edge would give 2x > 1; a probability axis can't exceed 1.
        let curve = vec![(0.0, 0.2), (0.5, 0.7)];
        assert_eq!(failure_chart_y_max(&curve, None), 1.0);
    }

    #[test]
    fn failure_chart_y_max_accounts_for_peer_adjustment() {
        let curve = vec![(0.0, 0.001), (0.5, 0.02)];
        // Upward adjustment lifts the peer-adjusted line, so the axis must grow
        // to keep it on-screen: (0.02 + 0.03) * 2 = 0.10.
        let up = failure_chart_y_max(&curve, Some(0.03));
        assert!((up - 0.10).abs() < 1e-9, "expected 0.10, got {up}");
        // A downward adjustment must not shrink the axis below the global edge.
        let down = failure_chart_y_max(&curve, Some(-0.01));
        assert!((down - 0.04).abs() < 1e-9, "expected 0.04, got {down}");
    }

    #[test]
    fn estimator_chart_labels_x_axis_as_distance() {
        // The x-axis is always ring distance; it must carry a "Distance" title.
        let curve = vec![(0.0, 0.1), (0.25, 0.5), (0.5, 0.9)];
        let html = build_estimator_chart(
            "Failure Probability",
            &curve,
            &[],
            (0.0, 0.5),
            None,
            AdjustmentMode::Additive,
            None,
            "0.0",
            "1.0",
        );
        assert!(
            html.contains(">Distance<"),
            "estimator chart must label its x-axis 'Distance', got: {html}"
        );
    }

    #[test]
    fn estimator_chart_small_range_y_labels_stay_distinct() {
        // Zoomed failure axis (0.0 .. 0.01): a fixed 2-decimal format collapsed
        // the middle and top ticks to "0.00"/"0.01". Adaptive decimals must keep
        // all three ticks readable and distinct.
        let curve = vec![(0.0, 0.0), (0.25, 0.003), (0.5, 0.005)];
        let html = build_estimator_chart(
            "Failure Probability",
            &curve,
            &[],
            (0.0, 0.5),
            None,
            AdjustmentMode::Additive,
            None,
            "0.0",
            "0.01",
        );
        assert!(
            html.contains(">0.0050<"),
            "middle y-tick must remain readable at a small range, got: {html}"
        );
        assert!(
            html.contains(">0.0100<"),
            "top y-tick must remain readable at a small range, got: {html}"
        );
    }

    #[test]
    fn estimator_chart_keeps_offscale_failure_dots_when_zoomed() {
        // With a zoomed failure axis (0.0 .. 0.08), raw failure outcomes at
        // y=1.0 are off-scale-high. They must still render (clamped to the top
        // edge) instead of vanishing, so failures stay visible on the
        // low-probability peers the zoom targets. The fitted curve renders as a
        // <path>, so every <circle> here is a scatter dot.
        let curve = vec![(0.0, 0.001), (0.25, 0.02), (0.5, 0.04)];
        let scatter = vec![(0.1, 0.0), (0.2, 1.0), (0.3, 1.0)];
        let html = build_estimator_chart(
            "Failure Probability",
            &curve,
            &scatter,
            (0.0, 0.5),
            None,
            AdjustmentMode::Additive,
            None,
            "0.0",
            "0.08",
        );
        let circles = html.matches("<circle").count();
        assert!(
            circles >= 3,
            "all 3 scatter dots (incl. the 2 off-scale failures) must render, got {circles}: {html}"
        );
    }

    #[test]
    fn peer_detail_links_renegade_to_repo() {
        // The "Renegade" label on the Routing Model card links to the project repo.
        let src = include_str!("home_page/peer_detail.rs");
        assert!(
            src.contains(r#"href="https://github.com/sanity/renegade""#),
            "the Renegade label must link to https://github.com/sanity/renegade"
        );
    }

    #[test]
    fn fmt_prediction_prob_sentinel_values() {
        assert_eq!(fmt_prediction_prob(f64::NAN), "N/A");
        assert_eq!(fmt_prediction_prob(f64::INFINITY), "N/A");
        assert_eq!(fmt_prediction_prob(-0.1), "N/A");
        assert_eq!(fmt_prediction_prob(1.1), "N/A");
        assert_eq!(fmt_prediction_prob(0.0), "0.0000");
        assert_eq!(fmt_prediction_prob(1.0), "1.0000");
        assert_eq!(fmt_prediction_prob(0.5), "0.5000");
    }

    fn sample_peer(addr: &str, location: f64) -> crate::node::network_status::PeerSnapshot {
        use crate::node::network_status::PeerSnapshot;
        PeerSnapshot {
            address: addr.parse().unwrap(),
            is_gateway: false,
            location: Some(location),
            connected_secs: 60,
            peer_key_location: None,
            bytes_sent: 1024,
            bytes_received: 2048,
        }
    }

    #[test]
    fn ring_svg_dots_link_to_peer_pages() {
        let peers = vec![sample_peer("127.0.0.1:31337", 0.25)];
        let svg = build_ring_svg(Some(0.5), &peers, None, &[]);
        assert!(
            svg.contains("<a href=\"/peer/127.0.0.1:31337\""),
            "ring peer dot must be wrapped in a link to /peer/{{addr}}, got: {svg}"
        );
        assert!(
            svg.contains("<title>"),
            "ring peer dot must include a <title> tooltip, got: {svg}"
        );
    }

    #[test]
    fn peers_table_is_sortable_with_raw_sort_values() {
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.peers = vec![sample_peer("127.0.0.1:31337", 0.25)];
        let html = build_peers_card(&Some(snap));
        assert!(
            html.contains("class=\"sortable\""),
            "peers table must be sortable"
        );
        assert!(
            html.contains("data-sort-type=\"num\""),
            "peers table must declare numeric sort columns"
        );
        // Bytes are formatted (e.g. "1.0 KB") but must sort by raw byte counts.
        assert!(
            html.contains("data-sort=\"1024\""),
            "sent bytes cell must carry raw byte count for sorting"
        );
        assert!(
            html.contains("data-sort=\"2048\""),
            "recv bytes cell must carry raw byte count for sorting"
        );
    }

    #[test]
    fn contracts_table_has_copy_button() {
        use crate::node::network_status::ContractSnapshot;
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.contracts = vec![ContractSnapshot {
            key_short: "ABC1...".to_string(),
            key_full: "ABC123XYZ".to_string(),
            instance_id: "ABC123XYZ".to_string(),
            subscribed_secs: 100,
            last_updated_secs: Some(5),
            is_receiving_updates: true,
            in_use: true,
        }];
        let html = build_contracts_card(&Some(snap));
        assert!(
            html.contains("class=\"copy-key\""),
            "contract cell must render a copy button"
        );
        assert!(
            html.contains("data-copy=\"ABC123XYZ\""),
            "copy button must carry the full contract key, got: {html}"
        );
        assert!(
            html.contains("class=\"sortable\""),
            "contracts table must be sortable"
        );
    }

    #[test]
    fn header_includes_update_badge() {
        let html = homepage_html();
        assert!(
            html.contains("id=\"update-badge\""),
            "homepage header must expose an update-badge slot for the JS update check"
        );
        assert!(
            html.contains("id=\"version-badge\""),
            "homepage header must tag the version badge with an id so the JS check can read data-version"
        );
        assert!(
            html.contains("data-version="),
            "version badge must expose data-version for the update comparison"
        );
    }

    #[test]
    fn js_contains_update_check_and_sort() {
        // Lightweight contract: the dashboard JS must keep the new
        // helpers wired up. If you rename them, update both sides.
        assert!(
            JS.contains("checkForUpdate"),
            "JS must include the update check function"
        );
        assert!(
            JS.contains("applySort"),
            "JS must include the table sort function"
        );
        assert!(
            JS.contains("copyToClipboard"),
            "JS must include the clipboard copy helper"
        );
    }

    // ── Edge cases for the ring SVG ─────────────────────────────────────────

    #[test]
    fn ring_svg_self_dot_has_title_but_no_link() {
        // The local node has no /peer/{addr} page, so the self circle
        // must NOT be wrapped in an <a>, but it should still expose a
        // <title> for parity with the peer dots.
        let svg = build_ring_svg(Some(0.42), &[], None, &[]);
        assert!(svg.contains("<svg"), "ring SVG should still render");
        assert!(
            !svg.contains("<a "),
            "self-only ring must not contain any <a> wrappers, got: {svg}"
        );
        assert!(
            svg.contains("<g class=\"ring-self\">"),
            "self dot must be wrapped in a <g> with the ring-self class"
        );
        assert!(
            svg.contains("<title>You "),
            "self dot must include a 'You' title, got: {svg}"
        );
    }

    #[test]
    fn ring_svg_distinguishes_gateway_in_link_title() {
        // Gateway peers and regular peers route to the same /peer/{addr}
        // page, but the SVG <title> tooltip should label them differently
        // so users can identify gateways without hovering each one.
        use crate::node::network_status::PeerSnapshot;
        let gw = PeerSnapshot {
            address: "10.0.0.1:31337".parse().unwrap(),
            is_gateway: true,
            location: Some(0.10),
            connected_secs: 0,
            peer_key_location: None,
            bytes_sent: 0,
            bytes_received: 0,
        };
        let peer = sample_peer("10.0.0.2:31338", 0.90);
        let svg = build_ring_svg(Some(0.5), &[gw, peer], None, &[]);
        assert!(
            svg.contains("<title>Gateway 10.0.0.1:31337"),
            "gateway dot title must say 'Gateway', got: {svg}"
        );
        assert!(
            svg.contains("<title>Peer 10.0.0.2:31338"),
            "regular peer title must say 'Peer', got: {svg}"
        );
        assert!(
            svg.contains("href=\"/peer/10.0.0.1:31337\""),
            "gateway dot must still link to /peer/{{addr}}"
        );
    }

    #[test]
    fn ring_svg_omitted_when_no_locations() {
        // If neither own_location nor any peer has a location, there's
        // nothing to plot — return empty so the card just shows the table.
        use crate::node::network_status::PeerSnapshot;
        let no_loc_peer = PeerSnapshot {
            address: "10.0.0.3:1".parse().unwrap(),
            is_gateway: false,
            location: None,
            connected_secs: 0,
            peer_key_location: None,
            bytes_sent: 0,
            bytes_received: 0,
        };
        assert!(build_ring_svg(None, &[no_loc_peer], None, &[]).is_empty());
        assert!(build_ring_svg(None, &[], None, &[]).is_empty());
    }

    /// Pin: build_ring_svg renders hosted contracts as faint dim
    /// teal dots on the inner ring (non-flagged ones). Without this
    /// the inner ring was visually empty in healthy state and made
    /// the renderer look unfinished.
    ///
    /// Rule-review of #4298 caught that the new `hosted_contracts`
    /// rendering loop was untested — every test site passed `&[]`.
    /// This test exercises the happy path (at least one dot
    /// rendered) and the skip-flagged path (a contract present in
    /// BOTH hosted and flagged sets gets the flagged marker only,
    /// not a duplicate hosted dot).
    #[test]
    fn ring_svg_renders_hosted_contracts_on_inner_ring() {
        use crate::node::network_status::{
            ContractGovernanceEntry, ContractSnapshot, GovernanceSnapshot, GovernanceStateSnapshot,
            NetworkNorms,
        };

        let hosted = vec![
            ContractSnapshot {
                key_short: "HOST1...".to_string(),
                key_full: "HOST1_with_params".to_string(),
                instance_id: "HOST1".to_string(),
                subscribed_secs: 60,
                last_updated_secs: Some(5),
                is_receiving_updates: true,
                in_use: true,
            },
            ContractSnapshot {
                key_short: "HOST2...".to_string(),
                key_full: "HOST2_with_params".to_string(),
                instance_id: "HOST2".to_string(),
                subscribed_secs: 60,
                last_updated_secs: Some(5),
                is_receiving_updates: true,
                in_use: true,
            },
            // This one is ALSO flagged — should be skipped in the
            // hosted dim-dot loop to avoid a duplicate marker.
            ContractSnapshot {
                key_short: "FLAG1...".to_string(),
                key_full: "FLAG1_with_params".to_string(),
                instance_id: "FLAG1".to_string(),
                subscribed_secs: 60,
                last_updated_secs: Some(5),
                is_receiving_updates: true,
                in_use: true,
            },
        ];

        let governance = GovernanceSnapshot {
            mode: crate::node::network_status::GovernanceModeSnapshot::DryRun,
            contracts: vec![ContractGovernanceEntry {
                instance_id: "FLAG1".to_string(),
                instance_id_short: "FLAG1".to_string(),
                state: GovernanceStateSnapshot::WouldEvict,
                cost_used: 1.0,
                benefit_score: 1.0,
                log_ratio: Some(0.0),
                age_secs: 100,
                last_transition_secs_ago: 1,
                history: Vec::new(),
            }],
            observed_count: 3,
            min_samples: 30,
            norms: NetworkNorms::default(),
            last_tick_at: None,
            state_by_id: std::collections::HashMap::new(),
        };

        let svg = build_ring_svg(Some(0.5), &[], Some(&governance), &hosted);

        // The faint hosted-contract dot uses the brand teal at 0.45
        // opacity. Pin both attributes so a future refactor that
        // changes the style triggers the test, AND count the dots so
        // we know flagged-skipping worked.
        let hosted_dot_count = svg
            .matches("fill=\"#43c178\" fill-opacity=\"0.45\"")
            .count();
        assert_eq!(
            hosted_dot_count, 2,
            "expected exactly 2 hosted-contract dim dots (HOST1, HOST2). \
             FLAG1 should be skipped because it's already in the flagged set. \
             Got {hosted_dot_count} hosted dots in SVG:\n{svg}"
        );

        // FLAG1 must still appear, just via the brighter flagged-
        // dot rendering (the WouldEvict color, with glow).
        assert!(
            svg.contains("#ff8a3d"),
            "FLAG1 should render with its WouldEvict color regardless of being in hosted set"
        );
    }

    /// Pin: when the hosted-contracts slice is empty, the inner ring
    /// emits no hosted-dot circles. Sanity check that the
    /// `&[] → no rendering` path still works as before.
    #[test]
    fn ring_svg_no_hosted_contracts_means_no_dim_dots() {
        let svg = build_ring_svg(Some(0.5), &[], None, &[]);
        assert!(
            !svg.contains("fill-opacity=\"0.45\""),
            "empty hosted slice must produce no dim-dot fill-opacity attribute, got:\n{svg}"
        );
    }

    // ── Sort attribute coverage for both tables ─────────────────────────────

    #[test]
    fn peers_table_handles_missing_location_in_sort() {
        // A peer with no known location should still be sortable: emit
        // an empty data-sort so the JS comparator treats it as the
        // largest value (sinks to the bottom in ascending order).
        use crate::node::network_status::PeerSnapshot;
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.peers = vec![PeerSnapshot {
            address: "10.0.0.4:1".parse().unwrap(),
            is_gateway: false,
            location: None,
            connected_secs: 5,
            peer_key_location: None,
            bytes_sent: 0,
            bytes_received: 0,
        }];
        let html = build_peers_card(&Some(snap));
        assert!(
            html.contains("data-sort=\"\">—"),
            "peer row with unknown location must emit empty data-sort, got: {html}"
        );
    }

    #[test]
    fn contracts_table_preserves_full_key_tooltip_and_code_markup() {
        // The full-key tooltip on the cell predates the copy button and
        // must NOT be lost when we add the button — that tooltip is
        // still useful for hover-only users (e.g. read-only screenshots).
        // Likewise, <code>{short}</code> must stay outside the button so
        // the abbreviated key keeps its monospace styling without a
        // hover state on the contract text itself.
        use crate::node::network_status::ContractSnapshot;
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.contracts = vec![ContractSnapshot {
            key_short: "DEAD...".to_string(),
            key_full: "DEADBEEF".to_string(),
            instance_id: "DEADBEEF".to_string(),
            subscribed_secs: 60,
            last_updated_secs: Some(2),
            is_receiving_updates: true,
            in_use: true,
        }];
        let html = build_contracts_card(&Some(snap));
        assert!(
            html.contains("title=\"DEADBEEF\""),
            "the original full-key cell tooltip must be preserved, got: {html}"
        );
        assert!(
            html.contains("<code>DEAD...</code>"),
            "the abbreviated key must stay as a plain <code> sibling of the button, got: {html}"
        );
        // Lock in the simplified markup: the <code> element is a sibling
        // of the button, NOT wrapped inside it.
        assert!(
            !html.contains("class=\"copy-key\" data-copy=\"DEADBEEF\" title=\"Copy contract key\" aria-label=\"Copy contract key\">⧉</button><code>"),
            "<code> must come BEFORE the copy button"
        );
        assert!(
            html.contains("</code><button type=\"button\" class=\"copy-key\""),
            "<code> must directly precede the <button> sibling, got: {html}"
        );
    }

    #[test]
    fn contracts_table_never_updated_sorts_last() {
        // Contracts that have never been updated (last_updated_secs = None)
        // should sink to the bottom in ascending order — represented as
        // u64::MAX in the data-sort attribute. If we emitted "—" or 0,
        // the row would jump to the top and look "freshest".
        use crate::node::network_status::ContractSnapshot;
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.contracts = vec![
            ContractSnapshot {
                key_short: "FRESH..".to_string(),
                key_full: "FRESH".to_string(),
                instance_id: "FRESH".to_string(),
                subscribed_secs: 1,
                last_updated_secs: Some(1),
                is_receiving_updates: true,
                in_use: true,
            },
            ContractSnapshot {
                key_short: "NEVER..".to_string(),
                key_full: "NEVER".to_string(),
                instance_id: "NEVER".to_string(),
                subscribed_secs: 1,
                last_updated_secs: None,
                is_receiving_updates: true,
                in_use: true,
            },
        ];
        let html = build_contracts_card(&Some(snap));
        let sentinel = format!("data-sort=\"{}\">—", u64::MAX);
        assert!(
            html.contains(&sentinel),
            "never-updated contract must emit data-sort=\"{}\" so it sorts last in ascending order, got: {html}",
            u64::MAX
        );
    }

    /// The Subscribed Contracts table's second column shows the REAL
    /// per-contract freshness/demand signal, not the retired MAD-governance
    /// state. `is_receiving_updates` drives the fresh/stale pill (only an
    /// active subscription keeps the cache current — `is_hosting` is NOT a
    /// freshness signal, PR #3699); `in_use` drives the in-use/idle pill
    /// (real demand: a local client or a downstream subscriber).
    #[test]
    fn contracts_table_shows_freshness_and_demand_pills() {
        use crate::node::network_status::ContractSnapshot;
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.contracts = vec![
            // Fresh + genuinely in use.
            ContractSnapshot {
                key_short: "FRESH...".to_string(),
                key_full: "FRESH_IN_USE".to_string(),
                instance_id: "FRESH_IN_USE".to_string(),
                subscribed_secs: 60,
                last_updated_secs: Some(10),
                is_receiving_updates: true,
                in_use: true,
            },
            // Not receiving updates and no demand → stale + idle.
            ContractSnapshot {
                key_short: "STALE...".to_string(),
                key_full: "STALE_IDLE".to_string(),
                instance_id: "STALE_IDLE".to_string(),
                subscribed_secs: 60,
                last_updated_secs: None,
                is_receiving_updates: false,
                in_use: false,
            },
        ];
        let html = build_contracts_card(&Some(snap));
        assert!(
            html.contains(r#"<span class="fresh-pill fresh-ok">fresh</span>"#),
            "receiving-updates contract must render the fresh pill — got:\n{html}"
        );
        assert!(
            html.contains(r#"<span class="fresh-pill use-active">in use</span>"#),
            "in-use contract must render the in-use pill — got:\n{html}"
        );
        assert!(
            html.contains(r#"<span class="fresh-pill fresh-stale">stale</span>"#),
            "non-receiving contract must render the stale pill — got:\n{html}"
        );
        assert!(
            html.contains(r#"<span class="fresh-pill use-idle">idle</span>"#),
            "no-demand contract must render the idle pill — got:\n{html}"
        );
        // The retired governance column must be gone.
        assert!(
            !html.contains("gov-pill"),
            "the retired MAD-governance column must not render — got:\n{html}"
        );
        assert!(
            html.contains(">Freshness<"),
            "the second column header should now be 'Freshness' — got:\n{html}"
        );
    }

    // ── Header / version badge guarantees ───────────────────────────────────

    /// Helper for header-element tests: pull a specific HTML element line
    /// out of the rendered homepage. We can't just take the first line
    /// containing the id, because the CSS block and JS bundle also
    /// reference these ids by name.
    fn extract_element_line(html: &str, anchor: &str) -> String {
        html.lines()
            .find(|l| l.contains(anchor) && l.trim_start().starts_with('<'))
            .unwrap_or_else(|| panic!("no HTML line containing {anchor:?} in homepage"))
            .to_string()
    }

    #[test]
    fn version_badge_data_attribute_matches_visible_text() {
        // The JS update check reads `data-version` and compares it
        // against the GitHub `tag_name`. If the attribute drifted away
        // from the visible "v{version}" text, the user would see one
        // version on the chip and the comparator would use another —
        // so lock that they always agree.
        //
        // Note: in unit tests there is no live NetworkStatusSnapshot,
        // so the rendered version is the "?" placeholder; the contract
        // we want is "attribute == visible text minus the leading v",
        // which holds for both the placeholder and the runtime value.
        let html = homepage_html();
        let line = extract_element_line(&html, "id=\"version-badge\"");
        let data_ver = line
            .split("data-version=\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .expect("version badge must declare data-version");
        let visible = line
            .split('>')
            .nth(1)
            .and_then(|s| s.split('<').next())
            .expect("version badge must contain visible text");
        assert!(
            visible.starts_with('v'),
            "version chip text must start with 'v', got: {visible:?}"
        );
        assert_eq!(
            &visible[1..],
            data_ver,
            "data-version ({data_ver:?}) must equal the visible chip text without the leading 'v' ({visible:?})"
        );
    }

    #[test]
    fn update_badge_links_to_releases_and_starts_hidden() {
        // The badge must:
        //   1. Link to the GitHub releases page (so users land somewhere
        //      sensible when they click it).
        //   2. Open in a new tab with rel=noopener (we don't want the
        //      release page to navigate the dashboard frame).
        //   3. Start hidden — we surface it only after the JS update
        //      check confirms a newer version exists.
        let html = homepage_html();
        let line = extract_element_line(&html, "id=\"update-badge\"");
        assert!(
            line.contains("href=\"https://github.com/freenet/freenet-core/releases/latest\""),
            "update badge must link to the releases page, got: {line}"
        );
        assert!(
            line.contains("target=\"_blank\""),
            "update badge must open in a new tab"
        );
        assert!(
            line.contains("rel=\"noopener noreferrer\""),
            "update badge must use rel=noopener noreferrer for safety"
        );
        assert!(
            line.contains(" hidden"),
            "update badge must start hidden — JS unhides it only when an update is found"
        );
    }

    #[test]
    fn js_update_check_uses_localstorage_cache() {
        // Two guarantees the JS check makes about resource use:
        //   1. We compare semver, not string equality — otherwise
        //      "0.2.10" would look "older" than "0.2.9".
        //   2. We cache the GitHub response in localStorage so we don't
        //      hit the api.github.com rate limit on every refresh.
        // Pin both as substrings.
        assert!(
            JS.contains("compareSemver"),
            "JS must include the semver comparator the update check relies on"
        );
        assert!(
            JS.contains("localStorage") && JS.contains("freenet-update-check"),
            "JS must persist the update check in localStorage under the freenet-update-check key"
        );
    }

    // ─── Governance card (Phase 4.5) ───────────────────────────────

    use crate::node::network_status::{
        ContractGovernanceEntry, GovernanceModeSnapshot, GovernanceSnapshot,
        GovernanceStateSnapshot, NetworkNorms,
    };

    fn mk_entry(state: GovernanceStateSnapshot, instance_id: &str) -> ContractGovernanceEntry {
        ContractGovernanceEntry {
            instance_id: instance_id.to_string(),
            instance_id_short: instance_id.chars().take(12).collect::<String>(),
            state,
            cost_used: 12.5,
            benefit_score: 1.0,
            log_ratio: Some(1.1),
            age_secs: 3600,
            last_transition_secs_ago: 60,
            history: Vec::new(),
        }
    }

    #[test]
    fn governance_card_empty_state_when_no_contracts() {
        // Empty state = "no FLAGGED contracts." Post-polish slice 2 the
        // card still renders structural skeleton (mode pill, 5-tile
        // mini-strip with em-dashes, observed/needed progress) so an
        // operator can see what fields will appear once data arrives.
        // Pin: the skeleton tiles + mode pill are visible.
        let snap = base_snapshot();
        let html = build_governance_card(&Some(snap));
        assert!(
            html.contains(r#"g-mode g-mode-dry-run">dry-run<"#),
            "empty state must show the mode pill — got:\n{html}"
        );
        assert!(
            html.contains("Eviction threshold"),
            "empty state must render the 5-tile skeleton — got:\n{html}"
        );
        assert!(
            html.contains("—"),
            "empty state should use em-dashes for missing tile values"
        );
        assert!(
            !html.contains("verdict-alert"),
            "empty state must not render the alert verdict block"
        );
    }

    #[test]
    fn governance_card_empty_state_shows_observed_progress() {
        // Pin: the empty state surfaces "N / min_samples" + the
        // remaining count, using the exact phrases the user sees —
        // not just digit substrings (Codex review nit).
        let mut snap = base_snapshot();
        snap.governance = GovernanceSnapshot {
            mode: GovernanceModeSnapshot::DryRun,
            contracts: Vec::new(), // none flagged
            norms: NetworkNorms::default(),
            observed_count: 12,
            min_samples: 30,
            last_tick_at: None,
            state_by_id: std::collections::HashMap::new(),
        };
        let html = build_governance_card(&Some(snap));
        assert!(
            html.contains("Observed 12 / 30 contracts needed"),
            "empty state should pin the 'Observed X / Y contracts needed' phrase — got:\n{html}"
        );
        assert!(
            html.contains("once 18 more contracts accumulate"),
            "empty state should name the remaining count by name — got:\n{html}"
        );
    }

    #[test]
    fn governance_card_empty_state_pluralizes_singular_count() {
        // Pin: when remaining == 1, the message uses "1 more contract"
        // not "1 more contracts". Codex review nit on pluralization.
        let mut snap = base_snapshot();
        snap.governance = GovernanceSnapshot {
            mode: GovernanceModeSnapshot::DryRun,
            contracts: Vec::new(),
            norms: NetworkNorms::default(),
            observed_count: 29,
            min_samples: 30,
            last_tick_at: None,
            state_by_id: std::collections::HashMap::new(),
        };
        let html = build_governance_card(&Some(snap));
        assert!(
            html.contains("once 1 more contract accumulates"),
            "with remaining=1 message must use singular 'contract accumulates' — got:\n{html}"
        );
    }

    #[test]
    fn governance_card_hidden_when_off() {
        // The MAD governance detector is default-Off and is being replaced by
        // demand-driven eviction (#4296, #4642). On a default (Off) node the
        // dashboard must NOT render the dormant, superseded governance card at
        // all — it only ever said "Governance is off" and misled operators.
        // The demand-driven eviction card surfaces live retention instead.
        let mut snap = base_snapshot();
        snap.governance = GovernanceSnapshot {
            mode: GovernanceModeSnapshot::Off,
            contracts: Vec::new(),
            norms: NetworkNorms::default(),
            observed_count: 0,
            min_samples: 30,
            last_tick_at: None,
            state_by_id: std::collections::HashMap::new(),
        };
        let html = build_governance_card(&Some(snap));
        assert!(
            html.is_empty(),
            "Off-mode governance card must be hidden entirely — got:\n{html}"
        );
    }

    #[test]
    fn governance_card_empty_state_healthy_when_enough_samples() {
        // Pin: once observed_count >= min_samples but nothing is
        // flagged, the empty state should read "all N within normal
        // range" — the healthy-steady-state message — not the
        // ramp-up-progress one.
        let mut snap = base_snapshot();
        snap.governance = GovernanceSnapshot {
            mode: GovernanceModeSnapshot::DryRun,
            contracts: Vec::new(),
            norms: NetworkNorms::default(),
            observed_count: 50,
            min_samples: 30,
            last_tick_at: None,
            state_by_id: std::collections::HashMap::new(),
        };
        let html = build_governance_card(&Some(snap));
        assert!(
            html.contains("All 50 tracked contracts within normal range")
                || html.contains("50 contracts within normal range"),
            "healthy-steady-state empty card should declare 'normal range' — got:\n{html}"
        );
    }

    #[test]
    fn governance_card_verdict_ok_when_all_normal() {
        let mut snap = base_snapshot();
        snap.governance = GovernanceSnapshot {
            mode: GovernanceModeSnapshot::DryRun,
            contracts: (0..5)
                .map(|i| mk_entry(GovernanceStateSnapshot::Normal, &format!("aaa{i}")))
                .collect(),
            norms: NetworkNorms::default(),
            observed_count: 0,
            min_samples: 30,
            last_tick_at: None,
            state_by_id: std::collections::HashMap::new(),
        };
        let html = build_governance_card(&Some(snap));
        assert!(
            html.contains("verdict-ok"),
            "5 normal contracts should produce verdict-ok styling — got:\n{html}"
        );
        assert!(
            html.contains("All 5 contracts within normal range"),
            "verdict should name the total — got:\n{html}"
        );
    }

    #[test]
    fn governance_card_verdict_alert_with_breakdown_when_flagged() {
        let mut snap = base_snapshot();
        snap.governance = GovernanceSnapshot {
            mode: GovernanceModeSnapshot::DryRun,
            contracts: vec![
                mk_entry(GovernanceStateSnapshot::WouldEvict, "abuser1"),
                mk_entry(GovernanceStateSnapshot::Borderline, "warn1"),
                mk_entry(GovernanceStateSnapshot::Borderline, "warn2"),
                mk_entry(GovernanceStateSnapshot::Normal, "ok1"),
            ],
            norms: NetworkNorms::default(),
            observed_count: 0,
            min_samples: 30,
            last_tick_at: None,
            state_by_id: std::collections::HashMap::new(),
        };
        let html = build_governance_card(&Some(snap));
        assert!(html.contains("verdict-alert"));
        // Total flagged = 1 + 2 = 3
        assert!(
            html.contains(">3<"),
            "verdict number should be the flagged count — got:\n{html}"
        );
        assert!(html.contains("1 would be evicted"));
        assert!(html.contains("2 borderline"));
        // Table renders the flagged ones, not the normal one.
        assert!(html.contains("abuser1"));
        assert!(html.contains("warn1"));
        assert!(!html.contains(r#"<code>ok1</code>"#));
    }

    #[test]
    fn governance_card_mode_pill_reflects_snapshot_mode() {
        // Off is hidden entirely (see governance_card_hidden_when_off); only
        // the explicitly-enabled modes render, and when they do the pill must
        // reflect the mode.
        for (mode, label) in [
            (GovernanceModeSnapshot::DryRun, "dry-run"),
            (GovernanceModeSnapshot::Enforce, "enforce"),
        ] {
            let mut snap = base_snapshot();
            snap.governance = GovernanceSnapshot {
                mode,
                contracts: vec![mk_entry(GovernanceStateSnapshot::Normal, "ok")],
                norms: NetworkNorms::default(),
                observed_count: 0,
                min_samples: 30,
                last_tick_at: None,
                state_by_id: std::collections::HashMap::new(),
            };
            let html = build_governance_card(&Some(snap));
            assert!(
                html.contains(&format!(r#"g-mode g-mode-{label}">{label}<"#)),
                "mode pill should reflect {label} — got:\n{html}"
            );
        }

        // And the Off mode renders nothing at all.
        let mut off = base_snapshot();
        off.governance = GovernanceSnapshot {
            mode: GovernanceModeSnapshot::Off,
            contracts: vec![mk_entry(GovernanceStateSnapshot::Normal, "ok")],
            norms: NetworkNorms::default(),
            observed_count: 0,
            min_samples: 30,
            last_tick_at: None,
            state_by_id: std::collections::HashMap::new(),
        };
        assert!(
            build_governance_card(&Some(off)).is_empty(),
            "Off mode must render no governance card"
        );
    }

    #[test]
    fn governance_card_omits_when_snap_is_none() {
        let html = build_governance_card(&None);
        assert!(html.is_empty());
    }

    // ─── Demand-driven eviction card (piece A, #4642) ───────────────

    #[test]
    fn hosting_card_hidden_when_nothing_hosted() {
        // A fresh/idle node hosts nothing → the card is noise, hide it.
        let snap = base_snapshot(); // default hosting: contract_count == 0
        assert!(build_hosting_card(&Some(snap)).is_empty());
        assert!(build_hosting_card(&None).is_empty());
    }

    #[test]
    fn hosting_card_renders_budget_and_eviction_order() {
        use crate::node::network_status::{HostedContractEntry, HostingSnapshot};
        let mut snap = base_snapshot();
        snap.hosting = HostingSnapshot {
            budget_bytes: 256 * 1024 * 1024,
            used_bytes: 64 * 1024 * 1024,
            contract_count: 2,
            budget_evictions_total: 3,
            evictions_of_recently_read_total: 1,
            // Provider emits rows in eviction order (lowest keep_score first).
            contracts: vec![
                HostedContractEntry {
                    key_full: "VICTIM_FULL".to_string(),
                    key_short: "VICTIM...".to_string(),
                    keep_score: 0.10,
                    predicted_demand: 0.0,
                    size_bytes: 1024,
                    read_count: 0,
                    eviction_eligible: true,
                },
                HostedContractEntry {
                    key_full: "HOT_FULL".to_string(),
                    key_short: "HOT...".to_string(),
                    keep_score: 5.0,
                    predicted_demand: 4.0,
                    size_bytes: 2048,
                    read_count: 42,
                    eviction_eligible: false,
                },
            ],
        };
        let html = build_hosting_card(&Some(snap));
        assert!(
            html.contains("Demand-driven eviction"),
            "card title — got:\n{html}"
        );
        assert!(
            html.contains("64.0 MB / 256.0 MB"),
            "RAM used/budget tile — got:\n{html}"
        );
        // Non-zero recently-read evictions are the miscalibration alarm: colored.
        assert!(
            html.contains("var(--danger"),
            "recently-read eviction count should be highlighted — got:\n{html}"
        );
        // The next-to-evict badge attaches to the first (lowest keep_score) row.
        let victim_idx = html.find("VICTIM_FULL").expect("victim row present");
        let hot_idx = html.find("HOT_FULL").expect("hot row present");
        let badge_idx = html.find("next to evict").expect("next-to-evict badge");
        assert!(
            victim_idx < hot_idx,
            "rows must be in eviction order (lowest keep_score first) — got:\n{html}"
        );
        assert!(
            badge_idx > victim_idx && badge_idx < hot_idx,
            "the next-to-evict badge must be on the victim (first) row — got:\n{html}"
        );
    }

    fn mk_hosted_entry(
        key: &str,
        keep_score: f64,
        eviction_eligible: bool,
    ) -> crate::node::network_status::HostedContractEntry {
        use crate::node::network_status::HostedContractEntry;
        HostedContractEntry {
            key_full: format!("{key}_FULL"),
            key_short: format!("{key}..."),
            keep_score,
            predicted_demand: 0.0,
            size_bytes: 1024,
            read_count: 0,
            eviction_eligible,
        }
    }

    #[test]
    fn hosting_card_badges_first_eligible_row_not_lowest_score() {
        use crate::node::network_status::HostingSnapshot;
        // The lowest-keep-score row is eviction-INELIGIBLE (within min_ttl or
        // in use), so the real over-budget sweep would SKIP it. The badge must
        // land on the first ELIGIBLE row instead — badging the lowest-score row
        // would mislabel an eviction-exempt contract (the finding this fixes).
        let mut snap = base_snapshot();
        snap.hosting = HostingSnapshot {
            budget_bytes: 256 * 1024 * 1024,
            used_bytes: 300 * 1024 * 1024, // over budget
            contract_count: 2,
            budget_evictions_total: 0,
            evictions_of_recently_read_total: 0,
            contracts: vec![
                // Lowest score, but pinned (in use / within TTL) → not eligible.
                mk_hosted_entry("PINNED", 0.10, false),
                // Higher score, but actually eligible → this is the real victim.
                mk_hosted_entry("EVICTABLE", 2.0, true),
            ],
        };
        let html = build_hosting_card(&Some(snap));
        let pinned_idx = html.find("PINNED_FULL").expect("pinned row present");
        let evictable_idx = html.find("EVICTABLE_FULL").expect("evictable row present");
        let badge_idx = html
            .find("next to evict")
            .expect("next-to-evict badge present");
        assert!(
            badge_idx > evictable_idx,
            "badge must be on the EVICTABLE (eligible) row, not the pinned \
             lowest-score row — got:\n{html}"
        );
        // The pinned row (rendered first) must NOT carry the badge.
        assert!(
            !(badge_idx > pinned_idx && badge_idx < evictable_idx),
            "the pinned lowest-score row must not be badged — got:\n{html}"
        );
    }

    #[test]
    fn hosting_card_no_badge_when_nothing_eligible() {
        use crate::node::network_status::HostingSnapshot;
        // Every hosted contract is within-TTL / in use → the sweep can evict
        // none of them right now, so NO row is labelled "next to evict".
        let mut snap = base_snapshot();
        snap.hosting = HostingSnapshot {
            budget_bytes: 256 * 1024 * 1024,
            used_bytes: 300 * 1024 * 1024,
            contract_count: 2,
            budget_evictions_total: 0,
            evictions_of_recently_read_total: 0,
            contracts: vec![
                mk_hosted_entry("A", 0.10, false),
                mk_hosted_entry("B", 2.0, false),
            ],
        };
        let html = build_hosting_card(&Some(snap));
        assert!(
            html.contains("Demand-driven eviction"),
            "card still renders (hosting > 0) — got:\n{html}"
        );
        assert!(
            !html.contains("next to evict"),
            "no row may be badged when nothing is eviction-eligible — got:\n{html}"
        );
    }

    // ─── Contract ban-list card (#4302) ────────────────────────────

    use crate::node::network_status::{BanListEntry, BanListSnapshot, BanReasonSnapshot};

    fn mk_ban_entry(id: &str, reason: BanReasonSnapshot, expires_in_secs: u64) -> BanListEntry {
        BanListEntry {
            instance_id: id.to_string(),
            reason,
            expires_in_secs,
        }
    }

    #[test]
    fn ban_list_card_empty_state_renders_count_zero_and_idle_message() {
        // Empty list still renders the card so operators can tell the
        // mechanism is active-but-idle (the issue's core motivation),
        // not unwired. Count tile shows 0.
        let snap = base_snapshot();
        let html = build_ban_list_card(&Some(snap));
        assert!(
            html.contains("Contract Ban List"),
            "card must render its heading even when empty — got:\n{html}"
        );
        assert!(
            html.contains(
                r#"<div class="g-norm-label">On ban list</div><div class="g-norm-value">0</div>"#
            ),
            "empty state must show a 0 count tile — got:\n{html}"
        );
        assert!(
            html.contains("0 contracts currently banned"),
            "empty state must say 0 contracts banned — got:\n{html}"
        );
        assert!(
            html.contains("active and currently idle"),
            "empty state must distinguish idle-but-active from unwired — got:\n{html}"
        );
        assert!(
            !html.contains("<table"),
            "empty state must not render an entry table — got:\n{html}"
        );
    }

    #[test]
    fn ban_list_card_lists_entries_with_key_reason_and_expiry() {
        // The two concrete asks: count tile + entry list (key, reason,
        // expiry remaining). Pin all three columns for both reasons.
        let mut snap = base_snapshot();
        snap.ban_list = BanListSnapshot {
            count: 2,
            capacity_rejected_total: 0,
            entries: vec![
                mk_ban_entry("AutoBannedContract11111", BanReasonSnapshot::AutoMad, 1800),
                mk_ban_entry("OperatorBannedContract22", BanReasonSnapshot::Operator, 90),
            ],
        };
        let html = build_ban_list_card(&Some(snap));
        // Count tile.
        assert!(
            html.contains(r#"<div class="g-norm-value">2</div>"#),
            "count tile must show 2 — got:\n{html}"
        );
        // Keys appear.
        assert!(
            html.contains("AutoBannedContract11111"),
            "auto-banned contract id must appear — got:\n{html}"
        );
        assert!(
            html.contains("OperatorBannedContract22"),
            "operator-banned contract id must appear — got:\n{html}"
        );
        // Reasons distinguish AutoMad vs Operator. The AutoMad path is
        // dormant (governance default-Off, being replaced by demand-driven
        // eviction), so its label is de-emphasized as legacy/dormant.
        assert!(
            html.contains("auto (legacy governance, dormant)"),
            "AutoMad ban must render the de-emphasized legacy-governance reason — got:\n{html}"
        );
        assert!(
            html.contains(">operator<"),
            "Operator ban must render an 'operator' reason — got:\n{html}"
        );
        // Expiry remaining: 90s formatted, and 1800s as 30m.
        assert!(
            html.contains("30m left") || html.contains("30m 0s left"),
            "expiry remaining must render the 1800s ban as ~30m — got:\n{html}"
        );
        assert!(
            html.contains("1m 30s left") || html.contains("90s left"),
            "expiry remaining must render the 90s ban — got:\n{html}"
        );
    }

    #[test]
    fn ban_list_card_singular_count_pluralization() {
        // Boundary: count == 1 must read "1 contract", not "1 contracts".
        let mut snap = base_snapshot();
        snap.ban_list = BanListSnapshot {
            count: 1,
            capacity_rejected_total: 0,
            entries: vec![mk_ban_entry("OnlyBanned1", BanReasonSnapshot::AutoMad, 600)],
        };
        let html = build_ban_list_card(&Some(snap));
        assert!(
            html.contains("1 contract currently banned"),
            "count==1 must use singular 'contract' — got:\n{html}"
        );
        assert!(
            !html.contains("1 contracts currently banned"),
            "count==1 must not say '1 contracts' — got:\n{html}"
        );
    }

    #[test]
    fn ban_list_card_shows_capacity_rejection_note_when_nonzero() {
        // The capacity-rejection counter is the operator's signal that
        // the bounded list is overflowing — surface it only when > 0.
        let mut snap = base_snapshot();
        snap.ban_list = BanListSnapshot {
            count: 1,
            capacity_rejected_total: 5,
            entries: vec![mk_ban_entry("AtCapacity1", BanReasonSnapshot::AutoMad, 300)],
        };
        let html = build_ban_list_card(&Some(snap));
        assert!(
            html.contains("5 bans rejected"),
            "non-zero capacity rejection must surface a note — got:\n{html}"
        );
        assert!(
            html.contains("list at capacity"),
            "capacity note must explain the cause — got:\n{html}"
        );
    }

    #[test]
    fn ban_list_card_hides_capacity_note_when_zero() {
        // Common case: no capacity pressure → no clutter.
        let mut snap = base_snapshot();
        snap.ban_list = BanListSnapshot {
            count: 1,
            capacity_rejected_total: 0,
            entries: vec![mk_ban_entry("Normal1", BanReasonSnapshot::Operator, 300)],
        };
        let html = build_ban_list_card(&Some(snap));
        assert!(
            !html.contains("rejected"),
            "zero capacity rejections must not render the note — got:\n{html}"
        );
    }

    #[test]
    fn ban_list_card_omits_when_snap_is_none() {
        let html = build_ban_list_card(&None);
        assert!(html.is_empty());
    }
}
