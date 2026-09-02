//! Homepage served at `/` when a user navigates to their local Freenet node.
//!
//! Renders a card-based dashboard showing connection status, peers, subscriptions,
//! and operation stats. Styled to match the global telemetry dashboard.

mod assets;
mod cards;
mod contract_detail;
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
use contract_detail::contract_detail_html;
use favicon::{build_dashboard_title, build_favicon_data_uri};
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

/// Per-contract detail page (#5369). Accepts either the full `ContractKey`
/// encoding or the `ContractInstanceId`, because the dashboard's own cards key
/// on different ones and an operator pasting either should land somewhere.
pub(super) async fn contract_detail(Path(key): Path<String>) -> impl IntoResponse {
    Html(contract_detail_html(&key))
}

fn homepage_html() -> String {
    let snap = network_status::get_snapshot();

    let (version, uptime) = match &snap {
        Some(s) => (s.version.as_str(), format_duration(s.elapsed_secs)),
        None => ("?", "0s".to_string()),
    };

    let favicon = build_favicon_data_uri(&snap);
    let title = build_dashboard_title(&snap);

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
        title = html_escape(&title),
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
    use super::contract_detail::contract_detail_html_from;
    use super::estimator::{
        RegKind, build_estimator_chart, build_estimator_chart_or_placeholder,
        build_regression_chart, build_reliability_chart, build_renegade_accuracy_panel,
        failure_chart_y_max, fmt_prediction_prob, fmt_prediction_speed, fmt_prediction_time,
    };
    use super::favicon::{build_dashboard_title, build_favicon_data_uri};
    use super::peer_detail::peer_detail_html;
    use crate::conformance::property::Severity;
    use crate::conformance::status::{
        CheckedContract, MergeCheckStatus, MergeCheckView, MergeFinding,
    };
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
            fair_queue: Default::default(),
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
        // Superseded by #5370: the banner no longer declares the node healthy.
        // Four live v0.2.128 peers showed "Node is healthy" while answering
        // between 1.3% and 89% of their GETs, because the four inputs behind
        // the verdict are all connectivity and none of them can see whether
        // the node serves reads. The banner now states the connection count,
        // which is a fact, and the measured GET rate carries the rest.
        assert!(
            html.contains("Connected to 3 peers"),
            "the healthy state must still state its connection count, got: {html}"
        );
        assert!(
            !html.contains("Node is healthy"),
            "the verdict must not come back, got: {html}"
        );

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
    fn fair_queue_stats_hidden_when_queue_never_used() {
        // An idle node must not render the queue row — same
        // uncluttered-dashboard rule as the rate limiter above.
        let snap = base_snapshot();
        assert_eq!(snap.fair_queue.high_water, 0);
        let html = build_status_card(&Some(snap));
        assert!(
            !html.contains("Queue depth"),
            "queue row should be hidden until the queue has been used"
        );
    }

    #[test]
    fn fair_queue_stats_rendered_after_backlog_even_once_drained() {
        // The #4912 case: a burst that has since drained. Instantaneous depth
        // is back to 0, so ONLY `high_water` reveals that the executor was
        // ever backed up — a polled dashboard would otherwise show nothing.
        let mut snap = base_snapshot();
        snap.fair_queue.depth_total = 0;
        snap.fair_queue.high_water = 4096;
        snap.fair_queue.rejected_global_capacity = 17;
        let html = build_status_card(&Some(snap));
        assert!(html.contains("Peak depth"), "peak-depth label missing");
        assert!(
            html.contains("4096</span>"),
            "peak depth must be shown even though the queue has since drained"
        );
        assert!(
            html.contains("Queue-full rejects") && html.contains("17</span>"),
            "queue-full reject count missing"
        );
    }

    #[test]
    fn fair_queue_per_contract_rejects_are_not_reported_as_zero() {
        // One hot contract hits its own per-tier cap while the node is
        // nowhere near global capacity. The row renders because rejections
        // happened, so it must not then show "0" for them — a card that
        // appears *because* of an event and reports zero of it is worse than
        // not rendering at all.
        let mut snap = base_snapshot();
        snap.fair_queue.high_water = 100;
        snap.fair_queue.rejected_per_contract = 42;
        snap.fair_queue.rejected_global_capacity = 0;
        let html = build_status_card(&Some(snap));
        assert!(
            html.contains("Contract-cap rejects"),
            "per-contract rejections need their own label"
        );
        assert!(
            html.contains("42</span>"),
            "per-contract reject count must be displayed, not silently dropped"
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
        assert!(
            html.contains("1.200s avg"),
            "a real average must still render when transfers completed"
        );
    }

    /// An all-failures window must NOT render an average.
    ///
    /// `avg_transfer_time_ms` is a sentinel 0 when nothing completed
    /// (metrics.rs guards the division), so rendering it would claim a measured
    /// "0.000s avg" for transfers that produced no timing at all. The window is
    /// reachable only since #4827 gave `transfers_failed` a writer: before that
    /// the counter was structurally 0, so this row required >=1 completion and
    /// the average was always real. A 30s all-failures window is exactly the
    /// sick-node case #4827 exists to surface, so the first thing it renders
    /// must not be a fabricated timing.
    #[test]
    fn transfer_card_omits_avg_when_nothing_completed() {
        let mut snap = base_snapshot();
        snap.bytes_uploaded = 5000;
        snap.open_connections = 1;
        snap.transport_snapshot.transfers_completed = 0;
        snap.transport_snapshot.transfers_failed = 3;
        // The sentinel metrics.rs produces when transfers_completed == 0.
        snap.transport_snapshot.avg_transfer_time_ms = 0;
        let html = build_transfer_card(&Some(snap));

        assert!(
            html.contains("0 ok / 3 fail"),
            "the failure row must still render — surfacing it is the point of #4827"
        );
        assert!(
            !html.contains("avg)"),
            "must not present the sentinel 0 as a measured average; got: {html}"
        );
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

    // ── Tab title surfaces connection state + count (#3509) ────────────────

    #[test]
    fn title_shows_trying_to_connect_when_no_snapshot() {
        // Before the first `network_status` snapshot exists (node mid-startup)
        // the title must fall back to the "trying to connect" indicator, not
        // crash or render a stale placeholder.
        assert_eq!(build_dashboard_title(&None), "\u{26A1} Dashboard");
    }

    #[test]
    fn title_shows_connection_count_when_connected() {
        let mut snap = base_snapshot();
        snap.open_connections = 4;
        assert_eq!(
            build_dashboard_title(&Some(snap)),
            "(4) Dashboard",
            "connected state must show the open connection count"
        );
    }

    #[test]
    fn title_shows_connection_count_boundary_one() {
        // Boundary: exactly one connection still uses the count form, not a
        // singular/plural variant — the format is purely numeric.
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        assert_eq!(build_dashboard_title(&Some(snap)), "(1) Dashboard");
    }

    #[test]
    fn title_shows_trouble_icon_on_health_trouble() {
        let mut snap = base_snapshot();
        snap.health = HealthLevel::Trouble;
        snap.open_connections = 0;
        assert_eq!(
            build_dashboard_title(&Some(snap)),
            "\u{26A0} Dashboard",
            "HealthLevel::Trouble must surface the warning icon"
        );
    }

    #[test]
    fn title_shows_trouble_icon_on_nat_failure() {
        // Even without HealthLevel::Trouble, an all-failed NAT traversal
        // history is a major connectivity error worth the same icon (mirrors
        // the favicon's dark-red NAT-failure precedence).
        let mut snap = base_snapshot();
        snap.nat_stats.attempts = 5;
        snap.nat_stats.successes = 0;
        snap.open_connections = 0;
        assert_eq!(build_dashboard_title(&Some(snap)), "\u{26A0} Dashboard");
    }

    #[test]
    fn title_shows_warning_icon_on_connection_failures() {
        // Mirrors the favicon's red case (build_favicon_data_uri's
        // `!s.failures.is_empty()` branch): a node can accumulate gateway
        // connection failures (e.g. Timeout) within the first 60s while
        // health == Connecting and nat_stats.attempts == 0 — no NAT history
        // yet, so this must not fall through to the default "connecting"
        // icon. Previously the title omitted this branch entirely and
        // showed the ⚡ default here, disagreeing with the favicon's red
        // "connection issues" icon for the same snapshot.
        let mut snap = base_snapshot();
        snap.open_connections = 0;
        snap.failures.push(FailureSnapshot {
            address: "1.2.3.4:1234".parse().unwrap(),
            reason_html: "Timeout".to_string(),
        });
        assert_eq!(build_dashboard_title(&Some(snap)), "\u{26A0} Dashboard");
    }

    #[test]
    fn title_shows_connecting_icon_by_default() {
        // Default base_snapshot(): HealthLevel::Connecting, zero connections,
        // no NAT attempts yet — the "still trying" fallback.
        let snap = base_snapshot();
        assert_eq!(build_dashboard_title(&Some(snap)), "\u{26A1} Dashboard");
    }

    #[test]
    fn title_connected_overrides_trouble_and_nat_failure() {
        // Connection count takes top priority even if failures/NAT problems
        // are still present in the snapshot (mirrors the favicon: "connected
        // wins" over lingering failure signals from before the connection
        // was established).
        let mut snap = base_snapshot();
        snap.open_connections = 2;
        snap.health = HealthLevel::Trouble;
        snap.nat_stats.attempts = 5;
        snap.nat_stats.successes = 0;
        assert_eq!(build_dashboard_title(&Some(snap)), "(2) Dashboard");
    }

    #[test]
    fn title_connected_overrides_failures() {
        // Same "connected wins" precedence as
        // title_connected_overrides_trouble_and_nat_failure, but exercised
        // against a bare `failures` signal with no Trouble/NAT-failure
        // present — connected must still win (mirrors
        // favicon_connected_overrides_failures).
        let mut snap = base_snapshot();
        snap.open_connections = 3;
        snap.failures.push(FailureSnapshot {
            address: "1.2.3.4:1234".parse().unwrap(),
            reason_html: "Timeout".to_string(),
        });
        assert_eq!(build_dashboard_title(&Some(snap)), "(3) Dashboard");
    }

    #[test]
    fn homepage_renders_dynamic_title() {
        // The rendered page must carry the derived title, not a static
        // placeholder — the JS refresh path re-reads it from `doc.title`.
        let html = homepage_html();
        assert!(
            html.contains("<title>\u{26A1} Dashboard</title>"),
            "no snapshot exists in the unit-test process, so the homepage \
             must render the 'trying to connect' title, got: {html}"
        );
    }

    #[test]
    fn js_syncs_title_on_refresh() {
        // Source-scrape guard: the auto-refresh handler must copy the
        // fetched document's title onto the live page so a backgrounded tab
        // reflects the current connection state without a full reload.
        assert!(
            JS.contains("document.title = doc.title"),
            "JS auto-refresh must sync document.title from the refreshed page"
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
    fn js_slows_refresh_when_tab_hidden() {
        // #3353: a hidden/backgrounded tab must back off to a much longer
        // refresh interval instead of polling every 5s while nobody is
        // watching. Pin both constants and the document.hidden branch.
        assert!(
            JS.contains("document.hidden"),
            "JS must branch the refresh interval on document.hidden"
        );
        assert!(
            JS.contains("HIDDEN_REFRESH_MS"),
            "JS must define a distinct, longer interval for hidden tabs"
        );
        assert!(
            JS.contains("60000"),
            "hidden-tab interval should be a much longer backoff (e.g. 60s)"
        );
    }

    #[test]
    fn js_refreshes_immediately_on_tab_visible() {
        // #3353: returning to a hidden tab must not wait out the stale
        // 60s hidden-tab timer — it should refresh right away so the user
        // sees current data as soon as they look at it.
        assert!(
            JS.contains("visibilitychange"),
            "JS must listen for visibilitychange to react to tab focus changes"
        );
        assert!(
            JS.contains("refreshDashboard()"),
            "JS must expose a refreshDashboard function callable outside the timer chain"
        );
    }

    #[test]
    fn js_auto_refresh_has_no_setinterval() {
        // Auto-refresh must keep using setTimeout chaining (so slow responses
        // don't overlap) even after adding the visibility-aware cadence —
        // setInterval would let a hung fetch pile up parallel requests.
        assert!(
            !JS.contains("setInterval("),
            "auto-refresh must not switch to setInterval; keep setTimeout chaining"
        );
    }

    #[test]
    fn js_guards_against_concurrent_refresh_chains() {
        // Rule-review finding on #4777's visibility-aware refresh: without
        // resetting refreshTimer once its setTimeout fires, a visibilitychange
        // racing an in-flight timer-triggered fetch would clearTimeout() an
        // already-fired id (a no-op) and fork a second concurrent
        // refreshDashboard().finally(scheduleRefresh) chain, breaking the
        // documented "one fetch at a time" invariant. Pin both the in-flight
        // guard flag and the refreshTimer = null reset.
        assert!(
            JS.contains("refreshInFlight"),
            "JS must track an in-flight guard flag so refreshDashboard() \
             is a no-op while a fetch is already running"
        );
        assert!(
            JS.contains("refreshTimer = null"),
            "JS must reset refreshTimer to null once its setTimeout fires, \
             so a later clearTimeout(refreshTimer) can't silently no-op \
             against an already-fired timer id"
        );
        // The BEHAVIORAL coverage for this state machine lives in
        // dashboard_refresh.test.mjs (run via `npm test` in
        // crates/core/src/server, wired into the lint-assets CI job), which
        // extracts createRefreshScheduler between these markers and drives it
        // under Node with fake timers. Pin the markers here so a Rust-side
        // refactor can't silently strip the extraction points that test
        // depends on.
        assert!(
            JS.contains("refresh-scheduler:BEGIN") && JS.contains("refresh-scheduler:END"),
            "JS must keep the refresh-scheduler:BEGIN/END markers — \
             dashboard_refresh.test.mjs extracts the scheduler between them"
        );
        assert!(
            JS.contains("function createRefreshScheduler("),
            "JS must keep the injectable createRefreshScheduler factory \
             that dashboard_refresh.test.mjs tests behaviorally"
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
        // the abbreviated key keeps its monospace styling.
        //
        // Amended for #5369: the <code> is now wrapped in an <a> to the
        // per-contract detail page, so it no longer DIRECTLY precedes the
        // button. That is a deliberate reversal of one clause of the
        // original intent, which asked for no hover state on the contract
        // text — written when there was nowhere for the key to link TO.
        // Now there is, and a link on the key is how the page is reached.
        // What still holds, and is what these assertions protect, is that
        // <code> stays OUTSIDE the copy button: nesting it would make the
        // key text a click target for "copy" rather than for "open", and
        // would put the monospace styling inside a button's hover state.
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
        // Lock in the markup: the <code> element is a sibling of the
        // button, NOT wrapped inside it.
        assert!(
            !html.contains("class=\"copy-key\" data-copy=\"DEADBEEF\" title=\"Copy contract key\" aria-label=\"Copy contract key\">⧉</button><code>"),
            "<code> must come BEFORE the copy button"
        );
        assert!(
            html.contains("</code></a><button type=\"button\" class=\"copy-key\""),
            "<code> must sit inside the detail link and directly precede the \
             copy button, got: {html}"
        );
        // The link must target the detail page with the FULL key, not the
        // abbreviation — the abbreviation is ambiguous and the page looks up
        // by full key or instance id.
        assert!(
            html.contains("href=\"/contract/DEADBEEF\""),
            "the key must link to its detail page by full key, got: {html}"
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

    #[test]
    fn js_update_check_core_stays_extractable_for_its_behavioral_test() {
        // #5102: caching only SUCCESS meant a rate-limited browser re-requested
        // on every single page load — the dashboard knocked hardest exactly
        // while the IP was already being refused. This check cannot move off
        // api.github.com the way the node's poll did (it runs in a browser, and
        // the quota-free github.com redirect sends no CORS headers), so backing
        // off is the only lever available here.
        //
        // The back-off is a small state machine, and substring pins cannot catch
        // a wiring error in one — so the real coverage is the executable
        // `update_check.test.mjs`, which extracts `createUpdateChecker` between
        // these markers and drives it under Node (mutation-verified: removing
        // the back-off fails it). What this Rust test guards is only that the
        // markers and the injectable factory survive, since silently losing them
        // would strand that test on code it can no longer extract.
        assert!(
            JS.contains("update-check:BEGIN") && JS.contains("update-check:END"),
            "the update-check core must stay bracketed by the update-check:BEGIN/END \
             markers so update_check.test.mjs can extract it (#5102)"
        );
        assert!(
            JS.contains("function createUpdateChecker(deps)"),
            "the update-check core must stay a dependency-injected factory so it can \
             be driven under Node without a browser (#5102)"
        );

        // Wiring, not just existence. update_check.test.mjs drives the factory in
        // ISOLATION, so a refactor that inlined a broken check into
        // checkForUpdate and orphaned the factory would keep both that test and
        // the assertions above green while shipping the bug. Pin that the
        // production entry point actually goes through the tested code.
        let (_, body) = JS
            .split_once("function checkForUpdate() {")
            .expect("checkForUpdate not found");
        let (body, _) = body
            .split_once("\n}\n")
            .expect("could not locate end of checkForUpdate");
        assert!(
            body.contains("createUpdateChecker({") && body.contains("checker.check("),
            "checkForUpdate must delegate to the tested createUpdateChecker factory, \
             not re-implement the check inline (#5102)"
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
            // Provider emits rows in eviction order (least-recently accessed
            // first — ascending `recency_seq`).
            contracts: vec![
                HostedContractEntry {
                    key_full: "VICTIM_FULL".to_string(),
                    key_short: "VICTIM...".to_string(),
                    size_bytes: 1024,
                    read_count: 0,
                    recency_seq: 0,
                    eviction_eligible: true,
                },
                HostedContractEntry {
                    key_full: "HOT_FULL".to_string(),
                    key_short: "HOT...".to_string(),
                    size_bytes: 2048,
                    read_count: 42,
                    recency_seq: 99,
                    eviction_eligible: false,
                },
            ],
            ..Default::default()
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
        // The next-to-evict badge attaches to the first eligible row.
        let victim_idx = html.find("VICTIM_FULL").expect("victim row present");
        let hot_idx = html.find("HOT_FULL").expect("hot row present");
        let badge_idx = html.find("next to evict").expect("next-to-evict badge");
        assert!(
            victim_idx < hot_idx,
            "rows must be in eviction order (least-recently accessed first) — got:\n{html}"
        );
        assert!(
            badge_idx > victim_idx && badge_idx < hot_idx,
            "the next-to-evict badge must be on the victim (first) row — got:\n{html}"
        );
    }

    /// The eviction table must be ordered by a column it actually displays.
    ///
    /// Regression for #4830: the card rendered `Keep-score` and `Demand` — the
    /// demoted telemetry-only estimator that eviction does not read — while the
    /// rows arrived sorted by `recency_seq`, which was dropped in the dashboard
    /// mapping and never shown. The table was therefore sorted by an invisible
    /// column and labelled as sorted by one that had no effect on the order.
    ///
    /// Mutation-checked: re-adding a `Keep-score` header or dropping the
    /// `Recency` column fails this test.
    #[test]
    fn hosting_card_shows_the_column_it_is_sorted_by() {
        use crate::node::network_status::HostingSnapshot;
        let mut snap = base_snapshot();
        snap.hosting = HostingSnapshot {
            budget_bytes: 256 * 1024 * 1024,
            used_bytes: 64 * 1024 * 1024,
            contract_count: 2,
            contracts: vec![
                mk_hosted_entry_seq("COLD", 0, true),
                mk_hosted_entry_seq("WARM", 7, false),
            ],
            ..Default::default()
        };
        let html = build_hosting_card(&Some(snap));

        assert!(
            html.contains(">Recency</th>"),
            "the ordering column must be a visible header — got:\n{html}"
        );
        // The dormant estimator must not be presented as a ranking column.
        assert!(
            !html.contains(">Keep-score</th>") && !html.contains(">Demand</th>"),
            "keep-score/demand are telemetry-only and must not be shown as \
             eviction ranking columns — got:\n{html}"
        );
        // `recency_seq == 0` is "not accessed since startup", not a real
        // sequence number, so it must not render as a bare 0.
        assert!(
            html.contains(">never<"),
            "recency_seq 0 must render as 'never', not 0 — got:\n{html}"
        );
        assert!(
            html.contains(">7<"),
            "a non-zero recency_seq must render its value — got:\n{html}"
        );
        // Displayed order must BE the recency order, not merely be labelled as
        // it. Without this the test would pass on an arbitrary row order.
        let cold_idx = html.find("COLD_FULL").expect("cold row present");
        let warm_idx = html.find("WARM_FULL").expect("warm row present");
        assert!(
            cold_idx < warm_idx,
            "rows must render least-recently-accessed first — got:\n{html}"
        );
    }

    /// The card must not carry the "piece A" badge, which was internal epic
    /// jargon rendered in `.g-mode-enforce` — the Governance card's
    /// *enforcement* red — so a decorative label wore the alarm colour.
    ///
    /// Without this pin, restoring the span leaves the suite green.
    #[test]
    fn hosting_card_has_no_piece_a_badge() {
        use crate::node::network_status::HostingSnapshot;
        let mut snap = base_snapshot();
        snap.hosting = HostingSnapshot {
            budget_bytes: 256 * 1024 * 1024,
            used_bytes: 64 * 1024 * 1024,
            contract_count: 1,
            contracts: vec![mk_hosted_entry_seq("A", 0, true)],
            ..Default::default()
        };
        let html = build_hosting_card(&Some(snap));
        assert!(
            !html.contains("piece A"),
            "the internal epic label must not appear on an operator surface — got:\n{html}"
        );
        assert!(
            !html.contains("g-mode-enforce"),
            "the eviction card must not borrow the governance enforcement-red \
             badge style — got:\n{html}"
        );
    }

    /// A contract pinned by a local client or downstream subscriber sorts to
    /// the top of this table when it has never been read, because the
    /// cache-side sort cannot see subscriber counts — yet the real sweep
    /// evicts it LAST. Without a marker it is indistinguishable from the most
    /// evictable row, which is the confusion the "in use" badge exists to
    /// prevent.
    #[test]
    fn hosting_card_marks_pinned_rows_as_in_use() {
        use crate::node::network_status::HostingSnapshot;
        let mut snap = base_snapshot();
        snap.hosting = HostingSnapshot {
            budget_bytes: 256 * 1024 * 1024,
            used_bytes: 64 * 1024 * 1024,
            contract_count: 2,
            contracts: vec![
                // Never read AND pinned: sorts first, but is not the victim.
                mk_hosted_entry_seq("PINNED", 0, false),
                mk_hosted_entry_seq("EVICTABLE", 5, true),
            ],
            ..Default::default()
        };
        let html = build_hosting_card(&Some(snap));
        let pinned_idx = html.find("PINNED_FULL").expect("pinned row present");
        let evictable_idx = html.find("EVICTABLE_FULL").expect("evictable row present");
        let badge_idx = html.find(">in use<").expect("in-use badge present");
        assert!(
            badge_idx > pinned_idx && badge_idx < evictable_idx,
            "the in-use badge must sit on the pinned row — got:\n{html}"
        );
        // Exactly one row is pinned, so exactly one badge.
        assert_eq!(
            html.matches(">in use<").count(),
            1,
            "only the pinned row may carry the in-use badge — got:\n{html}"
        );
    }

    /// The footer must not claim a ranking the card cannot actually show.
    ///
    /// Regression for #4830: it read "the N most-evictable … (lowest keep-score
    /// first)", which was wrong twice over — keep-score does not order the
    /// sweep, and the cache-side sort covers only the zero-subscriber tier.
    #[test]
    fn hosting_card_footer_does_not_claim_keep_score_ordering() {
        use crate::node::network_status::HostingSnapshot;
        let mut snap = base_snapshot();
        snap.hosting = HostingSnapshot {
            budget_bytes: 256 * 1024 * 1024,
            used_bytes: 64 * 1024 * 1024,
            // More hosted than rendered, so the footer appears.
            contract_count: 500,
            contracts: vec![mk_hosted_entry_seq("A", 0, true)],
            ..Default::default()
        };
        let html = build_hosting_card(&Some(snap));
        assert!(
            html.contains("of 500 hosted contracts"),
            "footer should report the full hosted count — got:\n{html}"
        );
        assert!(
            !html.to_lowercase().contains("keep-score first"),
            "footer must not claim keep-score ordering — got:\n{html}"
        );
        // The subscriber-tier caveat is the honest part; keep it pinned so a
        // future copy edit cannot quietly drop it.
        assert!(
            html.contains("no subscribers"),
            "footer must state that this is only the zero-subscriber ordering \
             — got:\n{html}"
        );
        // `recency_seq` is ALSO advanced by `record_abandonment` when a
        // contract loses its last subscriber, so any copy calling this an
        // access/read ordering misrepresents a just-abandoned contract.
        //
        // This blocklist is ILLUSTRATIVE, NOT EXHAUSTIVE: it catches the
        // phrasings that actually shipped, but an equivalent rewording
        // ("accessed at", "time since last read", "recently accessed") would
        // pass. A green run here is therefore evidence, not proof — if you are
        // editing this copy, re-check the claim against `record_abandonment`
        // rather than trusting the test.
        for banned in [
            "least-recently accessed",
            "least-recently read",
            "last accessed",
            "last read",
        ] {
            assert!(
                !html.contains(banned),
                "user-visible copy must not describe eviction recency as an \
                 access time (found {banned:?}) — abandonment advances it too \
                 — got:\n{html}"
            );
        }
    }

    fn mk_hosted_entry_seq(
        key: &str,
        recency_seq: u64,
        eviction_eligible: bool,
    ) -> crate::node::network_status::HostedContractEntry {
        let mut e = mk_hosted_entry(key, eviction_eligible);
        e.recency_seq = recency_seq;
        e
    }

    /// Ordering is by `recency_seq`; use `mk_hosted_entry_seq` when a test
    /// cares about the order. This helper leaves it at 0 ("never accessed").
    fn mk_hosted_entry(
        key: &str,
        eviction_eligible: bool,
    ) -> crate::node::network_status::HostedContractEntry {
        use crate::node::network_status::HostedContractEntry;
        HostedContractEntry {
            key_full: format!("{key}_FULL"),
            key_short: format!("{key}..."),
            size_bytes: 1024,
            read_count: 0,
            recency_seq: 0,
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
                mk_hosted_entry("PINNED", false),
                // Higher score, but actually eligible → this is the real victim.
                mk_hosted_entry("EVICTABLE", true),
            ],
            ..Default::default()
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
            contracts: vec![mk_hosted_entry("A", false), mk_hosted_entry("B", false)],
            ..Default::default()
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

    // ─── Disk-usage tiles (follow-up to #4683/#4702) ────────────────

    #[test]
    fn hosting_card_disk_tiles_show_measuring_before_seed() {
        use crate::node::network_status::HostingSnapshot;
        // Disk fields default to `None` (tracker not yet seeded / budget not
        // yet recomputed). The panel must render "measuring…" rather than a
        // bogus 0 B or an astronomical u64::MAX byte count.
        let mut snap = base_snapshot();
        snap.hosting = HostingSnapshot {
            budget_bytes: 256 * 1024 * 1024,
            used_bytes: 64 * 1024 * 1024,
            contract_count: 1,
            budget_evictions_total: 0,
            evictions_of_recently_read_total: 0,
            contracts: vec![mk_hosted_entry("A", false)],
            disk_state_bytes: None,
            disk_wasm_bytes: None,
            disk_compile_cache_bytes: None,
            disk_total_bytes: None,
            disk_budget_bytes: None,
            ..Default::default()
        };
        let html = build_hosting_card(&Some(snap));
        assert!(
            html.contains("Disk used"),
            "disk-used tile label present — got:\n{html}"
        );
        assert!(
            html.contains("Disk budget"),
            "disk-budget tile label present — got:\n{html}"
        );
        assert!(
            html.contains("Disk headroom"),
            "disk-headroom tile label present — got:\n{html}"
        );
        let measuring_count = html.matches("measuring…").count();
        assert_eq!(
            measuring_count, 3,
            "all three disk tiles must show 'measuring…' pre-seed — got:\n{html}"
        );
        assert!(
            !html.contains("18446744073709551615"),
            "u64::MAX must never leak into the rendered disk budget — got:\n{html}"
        );
    }

    #[test]
    fn hosting_card_disk_tiles_render_seeded_values() {
        use crate::node::network_status::HostingSnapshot;
        let mut snap = base_snapshot();
        snap.hosting = HostingSnapshot {
            budget_bytes: 256 * 1024 * 1024,
            used_bytes: 64 * 1024 * 1024,
            contract_count: 1,
            budget_evictions_total: 0,
            evictions_of_recently_read_total: 0,
            contracts: vec![mk_hosted_entry("A", false)],
            disk_state_bytes: Some(100 * 1024 * 1024),
            disk_wasm_bytes: Some(20 * 1024 * 1024),
            disk_compile_cache_bytes: Some(5 * 1024 * 1024),
            disk_total_bytes: Some(125 * 1024 * 1024),
            disk_budget_bytes: Some(500 * 1024 * 1024),
            ..Default::default()
        };
        let html = build_hosting_card(&Some(snap));
        assert!(
            !html.contains("measuring…"),
            "seeded snapshot must not show 'measuring…' — got:\n{html}"
        );
        assert!(
            html.contains("125.0 MB"),
            "disk-used total tile — got:\n{html}"
        );
        assert!(html.contains("500.0 MB"), "disk-budget tile — got:\n{html}");
        // Headroom = budget(500) - used(125) = 375 MB.
        assert!(
            html.contains("375.0 MB"),
            "disk-headroom tile (budget - used) — got:\n{html}"
        );
        // Breakdown surfaced in the title tooltip.
        assert!(
            html.contains("State: 100.0 MB")
                && html.contains("WASM: 20.0 MB")
                && html.contains("Compile cache: 5.0 MB"),
            "per-component breakdown in tooltip — got:\n{html}"
        );
        // The explanatory paragraph must name the pressures that can actually
        // trigger a sweep. It used to claim the floor was "min(RAM budget,
        // disk budget)", and this assertion pinned that wording — which is how
        // the claim outlived the two axes added since: the count-derived
        // resident-overhead ceiling (#5325, the one that binds first on a
        // real low-RAM peer) and cost pressure (#4861). Pin the axes, not the
        // phrasing, so adding a fifth fails here instead of going unnoticed.
        for axis in ["state bytes", "disk", "resident-overhead", "update work"] {
            assert!(
                html.contains(axis),
                "explanatory paragraph must name the {axis:?} eviction pressure \
                 — got:\n{html}"
            );
        }
    }

    /// The count-derived pressure axis (#5325) must render as contract SLOTS,
    /// not as bytes.
    ///
    /// The underlying pair is `contract_count * 1 MiB` against a RAM-scaled
    /// ceiling, so printing it as "30.0 MB / 100.0 MB" reads as measured
    /// memory. It is not measured, and what it constrains is a number of
    /// contracts — a low-RAM peer showed "520.0 MB / 524.0 MB" when the honest
    /// statement was "520 of 524 contract slots used".
    #[test]
    fn hosting_card_renders_slot_axis_as_counts_not_bytes() {
        use crate::node::network_status::HostingSnapshot;
        let mut snap = base_snapshot();
        snap.hosting = HostingSnapshot {
            budget_bytes: 256 * 1024 * 1024,
            used_bytes: 1024,
            contract_count: 30,
            contracts: vec![mk_hosted_entry("A", false)],
            resident_overhead_budget_bytes: 100 * 1024 * 1024,
            estimated_resident_overhead_bytes: 30 * 1024 * 1024,
            contract_slot_budget: 100,
            resident_overhead_evictions_total: 7,
            ..Default::default()
        };
        let html = build_hosting_card(&Some(snap));
        assert!(
            html.contains("Contract slots used") && html.contains("30 / 100"),
            "slot axis must render as counts — got:\n{html}"
        );
        assert!(
            html.contains(">70<"),
            "slots free = budget(100) - used(30) — got:\n{html}"
        );
        assert!(
            html.contains(">7<"),
            "slot-pressure eviction counter renders the snapshot value — got:\n{html}"
        );
        // The byte framing must be gone: it is what made this read as RAM.
        assert!(
            !html.contains("Resident overhead (est.)")
                && !html.contains("Resident overhead budget"),
            "the byte-denominated resident-overhead tiles must not return — got:\n{html}"
        );
    }

    /// The card shows several independent ceilings; it must say which one is
    /// closest to binding.
    ///
    /// Measured on a live low-RAM peer: 34% of the state-byte budget, 1% of
    /// the disk budget, 99.2% of the contract-slot ceiling. All three rendered
    /// as identical muted tiles, so the only number that mattered was
    /// indistinguishable from the two with room to spare.
    #[test]
    fn hosting_card_names_the_closest_limit() {
        use crate::node::network_status::HostingSnapshot;
        let mut snap = base_snapshot();
        snap.hosting = HostingSnapshot {
            // State bytes: 34% used.
            budget_bytes: 1000,
            used_bytes: 340,
            // Slots: 99% used — this is the binding axis.
            contract_count: 99,
            contract_slot_budget: 100,
            // Disk: 1% used.
            disk_total_bytes: Some(10),
            disk_budget_bytes: Some(1000),
            contracts: vec![mk_hosted_entry("A", true)],
            ..Default::default()
        };
        let html = build_hosting_card(&Some(snap));
        assert!(
            html.contains("Closest limit:") && html.contains("contract slots"),
            "the binding axis must be named — got:\n{html}"
        );
        assert!(
            html.contains("99 of 100"),
            "the binding axis detail must show its own units — got:\n{html}"
        );
        // At 99% it must be flagged, not left in the same muted grey as an
        // axis with room to spare.
        assert!(
            html.contains("var(--danger"),
            "a near-full binding axis must be coloured — got:\n{html}"
        );
    }

    /// The lowest-utilisation axis must NOT be the one reported.
    #[test]
    fn hosting_card_picks_the_highest_utilisation_axis() {
        use crate::node::network_status::HostingSnapshot;
        let mut snap = base_snapshot();
        snap.hosting = HostingSnapshot {
            // State bytes 90% — the binding axis here.
            budget_bytes: 1000,
            used_bytes: 900,
            // Slots only 10%.
            contract_count: 10,
            contract_slot_budget: 100,
            contracts: vec![mk_hosted_entry("A", true)],
            ..Default::default()
        };
        let html = build_hosting_card(&Some(snap));
        assert!(
            html.contains("Closest limit:") && html.contains("contract state"),
            "state bytes at 90% must outrank slots at 10% — got:\n{html}"
        );
        assert!(
            !html.contains("Closest limit: <strong>contract slots"),
            "the slack axis must not be reported as closest — got:\n{html}"
        );
    }

    /// Over budget must render as over budget, not as a clamped 100%.
    ///
    /// This state is reachable and important, not an error case: exceeding the
    /// contract-state budget IS the eviction trigger, and the slot axis sits
    /// over its ceiling for the whole ~2.5 minute sustained window before
    /// anything is shed. Clamping the percentage produced "150 of 100 (100%)"
    /// — a line that contradicts its own detail text and hides how far over
    /// the node is at exactly the moment that matters.
    #[test]
    fn hosting_card_shows_true_percentage_when_over_budget() {
        use crate::node::network_status::HostingSnapshot;
        let mut snap = base_snapshot();
        snap.hosting = HostingSnapshot {
            budget_bytes: 100,
            used_bytes: 150,
            contract_count: 1,
            contracts: vec![mk_hosted_entry("A", true)],
            ..Default::default()
        };
        let html = build_hosting_card(&Some(snap));
        assert!(
            html.contains("150%"),
            "an over-budget axis must report its true percentage — got:\n{html}"
        );
        assert!(
            !html.contains("(100%)"),
            "no percentage on the card may be clamped to 100% — the RAM-used \
             tile had the same clamp and disagreed with the strip — got:\n{html}"
        );
        // The RAM-used tile must agree with the strip, not clamp separately.
        assert!(
            html.contains("150 B / 100 B (150%)"),
            "the RAM-used tile must report the true percentage too — got:\n{html}"
        );
        // The bar itself is still capped: a fill cannot overflow its track.
        assert!(
            html.contains("width: 100.0%"),
            "the bar width must stay clamped at 100% — got:\n{html}"
        );
    }

    /// An unconfigured or not-yet-measured budget is not "100% full".
    ///
    /// The disk budget is an `Option` precisely because the tracker is
    /// unseeded early in a node's life; treating an absent or zero
    /// denominator as full would report a phantom emergency on every fresh
    /// start.
    #[test]
    fn hosting_card_skips_unconfigured_axes_when_picking_closest_limit() {
        use crate::node::network_status::HostingSnapshot;
        let mut snap = base_snapshot();
        snap.hosting = HostingSnapshot {
            budget_bytes: 1000,
            used_bytes: 100,
            contract_count: 5,
            // A slot budget of 0 means "not configured", NOT "no slots left".
            contract_slot_budget: 0,
            // Disk tracker unseeded.
            disk_total_bytes: None,
            disk_budget_bytes: None,
            contracts: vec![mk_hosted_entry("A", true)],
            ..Default::default()
        };
        let html = build_hosting_card(&Some(snap));
        assert!(
            html.contains("Closest limit:") && html.contains("contract state"),
            "the one configured axis must be reported — got:\n{html}"
        );
        assert!(
            !html.contains("contract slots</strong>"),
            "an unconfigured axis must not be ranked at all — got:\n{html}"
        );
    }

    // ─── Long-table filter controls ────────────────────────────────
    //
    // SCOPE WARNING: these tests assert the emitted MARKUP only. They do not
    // execute `dashboard.js`, have no DOM, and cannot tell you whether the
    // filter actually filters, whether the collapse collapses, or whether
    // either survives the 5s `<main>` swap. A green run here is compatible
    // with the feature being completely broken in a browser. The behaviour is
    // covered by driving a real node with Playwright; see the PR.

    // ─── GET success rate (#5370) ───────────────────────────────────────

    /// The banner must not call the node healthy.
    ///
    /// It did, on four connectivity inputs that say nothing about whether the
    /// node can serve reads: four live v0.2.128 peers displayed "Node is
    /// healthy" while answering between 1.3% and 89% of their GETs. A verdict
    /// is an assertion, and unsupported assertions are what this page keeps
    /// getting wrong. The connection COUNT is a fact and stays.
    #[test]
    fn status_card_states_connections_instead_of_declaring_health() {
        let mut snap = base_snapshot();
        snap.open_connections = 5;
        snap.health = crate::node::network_status::HealthLevel::Healthy;
        let html = build_status_card(&Some(snap));

        assert!(
            !html.contains("is healthy"),
            "the banner must not declare the node healthy — got:\n{html}"
        );
        assert!(
            html.contains("Connected to 5 peers"),
            "the connection count is a fact and must survive — got:\n{html}"
        );
    }

    /// A percentage over a handful of requests is theatre.
    ///
    /// Peers issue only a few GETs an hour, so a fresh node sits at a tiny
    /// denominator for a long time. At two requests one outcome moves the
    /// figure fifty points, which looks like a measurement and is not.
    #[test]
    fn get_success_rate_refuses_to_rate_a_tiny_sample() {
        let mut snap = base_snapshot();
        snap.open_connections = 3;
        snap.health = crate::node::network_status::HealthLevel::Healthy;
        snap.elapsed_secs = 600;
        snap.op_stats.gets = (1, 1);
        let html = build_status_card(&Some(snap));

        assert!(
            html.contains("too few to rate"),
            "a 2-request sample must not be rendered as a percentage — \
             got:\n{html}"
        );
        assert!(
            !html.contains("50%"),
            "and specifically not as 50% — got:\n{html}"
        );
        assert!(
            html.contains("1 of 2"),
            "the counts are still worth showing — got:\n{html}"
        );
        assert!(
            html.contains("does not by itself"),
            "the caveat must appear even when there is no rate to qualify — \
             got:\n{html}"
        );
    }

    /// The number the whole change exists to surface.
    ///
    /// The production gateway in #5370 answered 2 of 153 GETs and displayed
    /// "Node is healthy". It must now read 1%.
    #[test]
    fn get_success_rate_reports_the_measured_share() {
        let mut snap = base_snapshot();
        snap.open_connections = 12;
        snap.health = crate::node::network_status::HealthLevel::Healthy;
        snap.elapsed_secs = 3600 * 5;
        snap.op_stats.gets = (2, 151);
        let html = build_status_card(&Some(snap));

        assert!(
            html.contains("1% answered"),
            "2 of 153 is 1% and must be shown as such — got:\n{html}"
        );
        assert!(
            html.contains("2 of 153"),
            "the sample size must accompany the percentage, so the reader can \
             tell 1% of 153 from 1% of 3 — got:\n{html}"
        );
        assert!(
            html.contains("since start"),
            "the figure is lifetime, not a recent window, and must say so — \
             got:\n{html}"
        );
    }

    /// Rounding must never assert something that did not happen.
    ///
    /// `{:.0}` alone renders 199/200 as "100%" and 1/200 as "0%". Both are
    /// false in the way this panel exists to prevent: "100% answered" when a
    /// request failed is the same unearned absolute as "Node is healthy" was,
    /// and an operator who reads 100% stops looking.
    ///
    /// 100% and 0% are therefore reserved for the cases that earn them, and
    /// the bands beside them say which side of the boundary they are on
    /// instead of rounding across it.
    #[test]
    fn answered_share_never_rounds_across_an_absolute() {
        let render = |ok: u32, total: u32| {
            let mut snap = base_snapshot();
            snap.open_connections = 4;
            snap.health = crate::node::network_status::HealthLevel::Healthy;
            snap.elapsed_secs = 3600;
            snap.op_stats.gets = (ok, total - ok);
            build_status_card(&Some(snap))
        };

        // A single failure must not render as a perfect score.
        let near_perfect = render(199, 200);
        assert!(
            near_perfect.contains("&gt;99% answered") || near_perfect.contains(">99% answered"),
            "199 of 200 must not claim 100% — got:\n{near_perfect}"
        );
        assert!(
            !near_perfect.contains("100% answered"),
            "199 of 200 rounds to 100 and must be caught — got:\n{near_perfect}"
        );

        // A single success must not render as total failure.
        let near_zero = render(1, 200);
        assert!(
            near_zero.contains("&lt;1% answered") || near_zero.contains("<1% answered"),
            "1 of 200 must not claim 0% — got:\n{near_zero}"
        );

        // The absolutes are still available when genuinely earned.
        let perfect = render(50, 50);
        assert!(
            perfect.contains("100% answered"),
            "50 of 50 really is 100% — got:\n{perfect}"
        );
        let zero = render(0, 50);
        assert!(
            zero.contains("0% answered"),
            "0 of 50 really is 0% — got:\n{zero}"
        );

        // Exactly at MIN_SAMPLE. The gate is `total < MIN_SAMPLE`, so 20 must
        // take the rate branch — the one boundary value the other cases do not
        // pin, and an off-by-one here would silently withhold the number from
        // every node sitting at the threshold.
        let at_min = render(10, 20);
        assert!(
            at_min.contains("50% answered"),
            "20 requests is exactly the minimum sample, so it must be rated — \
             got:\n{at_min}"
        );
        assert!(
            !at_min.contains("too few to rate"),
            "and must not be refused — got:\n{at_min}"
        );
        let below_min = render(9, 19);
        assert!(
            below_min.contains("too few to rate"),
            "19 requests is below the minimum and must be refused — \
             got:\n{below_min}"
        );

        // And an ordinary value is unaffected: the 1.3% gateway from #5370.
        let gateway = render(2, 153);
        assert!(
            gateway.contains("1% answered"),
            "2 of 153 is 1.3%, which rounds honestly to 1% — got:\n{gateway}"
        );
    }

    /// The caveat must be UNCONDITIONAL, at every rate.
    ///
    /// An unanswered GET is frequently the network failing to route rather
    /// than this node failing to serve — dead-ends dominate the not-found
    /// mode. Without the caveat, "GET requests 1% answered" invites the
    /// operator to conclude their own node is broken and report it, and the
    /// support burden would be built out of our own phrasing.
    ///
    /// Showing it only when the number looks bad would be a threshold in
    /// disguise, and picking that threshold is precisely the judgement this
    /// panel exists to avoid. So it is pinned at a healthy rate too: if a
    /// future change makes it conditional, this fails.
    #[test]
    fn get_success_caveat_is_shown_at_every_rate() {
        // Includes the total == 0 branch. Review noted it was the one case
        // the caveat's own test never exercised — and it is the state a
        // freshly-started node sits in, so it is the branch most operators
        // see first.
        for (ok, failed, label) in [
            (2u32, 151u32, "very low"),
            (150, 3, "very high"),
            (0, 0, "no requests yet"),
        ] {
            let mut snap = base_snapshot();
            snap.open_connections = 6;
            snap.health = crate::node::network_status::HealthLevel::Healthy;
            snap.elapsed_secs = 3600 * 4;
            snap.op_stats.gets = (ok, failed);
            let html = build_status_card(&Some(snap));
            assert!(
                html.contains("could not route"),
                "the caveat must appear at a {label} rate too, or it becomes a \
                 threshold in disguise — got:\n{html}"
            );
        }
    }

    /// The rate must not be styled as a verdict.
    ///
    /// Colouring it green or red would reintroduce through CSS exactly the
    /// judgement the change removed from the markup — and the threshold for
    /// that colour is the number nobody could justify picking, which is why
    /// the verdict went in the first place.
    #[test]
    fn get_success_rate_carries_no_pass_fail_styling() {
        let mut snap = base_snapshot();
        snap.open_connections = 4;
        snap.health = crate::node::network_status::HealthLevel::Healthy;
        snap.elapsed_secs = 3600;
        snap.op_stats.gets = (10, 90);
        let html = build_status_card(&Some(snap));

        let line_start = html
            .find("get-success-rate")
            .expect("the rate line must be rendered");
        let line = &html[line_start..html[line_start..].find("</p>").unwrap() + line_start];
        for verdict_class in [
            "health-good",
            "health-trouble",
            "health-degraded",
            "op-ok",
            "op-fail",
        ] {
            assert!(
                !line.contains(verdict_class),
                "the rate line must not carry the pass/fail class \
                 `{verdict_class}` — got:\n{line}"
            );
        }
    }

    // ─── Contract detail page (#5369) ───────────────────────────────────

    /// A key that reaches this page came straight out of a URL path, so it is
    /// attacker-controlled text rendered into HTML.
    ///
    /// The not-found branch is the dangerous one: it echoes the key back
    /// verbatim to say what was not found, and it is reachable by ANYONE who
    /// can load the page with any path at all — no node state required. An
    /// unescaped echo there is a reflected-XSS hole on the operator's own
    /// dashboard, which is the origin that also serves every locally-hosted
    /// app.
    #[test]
    fn contract_detail_escapes_a_key_from_the_url() {
        let payload = "<script>alert(1)</script>";
        let html = contract_detail_html_from(&None, payload, false, None);
        assert!(
            !html.contains("<script>alert(1)"),
            "an unescaped key from the URL is reflected XSS — got:\n{html}"
        );
        assert!(
            html.contains("&lt;script&gt;alert(1)"),
            "the key should still be shown, escaped, so the operator can see \
             what was not found — got:\n{html}"
        );
    }

    /// Absence on this node is not absence from the network, and the page must
    /// not imply otherwise.
    ///
    /// A node holds what demand routed to it; it has no directory and cannot
    /// know whether a contract exists elsewhere. "Not found" phrased as a
    /// global fact would be the same class of falsehood as the eviction card
    /// describing a ranking the code does not implement.
    #[test]
    fn contract_not_found_is_scoped_to_this_node() {
        let html = contract_detail_html_from(&None, "NOSUCHCONTRACTKEY", false, None);
        assert!(
            html.contains("Contract Not Found"),
            "expected the not-found page — got:\n{html}"
        );
        assert!(
            html.contains("THIS node"),
            "the page must scope its claim to this node rather than implying \
             the contract is absent from the network — got:\n{html}"
        );
    }

    /// `ContractKey::to_string()` and `ContractKey::id().to_string()` produce
    /// the SAME string, and this pins it against a real key rather than a
    /// fixture that assumes it.
    ///
    /// This exists because the opposite belief is written down in this
    /// codebase and has now misled three separate readers. The rustdoc on
    /// `ContractSnapshot::instance_id` says it is "Distinct from `key_full`
    /// which carries the full ContractKey encoding (instance id + parameters /
    /// code-hash bookkeeping)". That is wrong: `impl Display for ContractKey`
    /// delegates to `self.instance`, and `id()` returns `&self.instance`, so
    /// the code-hash half never reaches either string.
    ///
    /// The consequences of believing otherwise are concrete. Two reviews of
    /// the contract detail page independently reported a blocking bug — that a
    /// hosted-only contract cannot be joined to governance, because
    /// `HostedContractEntry` carries no `instance_id` field — and I acted on it
    /// once, adding a "cannot be cross-referenced" branch for a case that does
    /// not exist. The join works precisely because these two strings are the
    /// same.
    ///
    /// If a future stdlib gives `ContractKey` a Display that includes the code
    /// hash, this fails, and the detail page's governance lookup genuinely
    /// does need the separate id.
    #[test]
    fn contract_key_display_equals_its_instance_id() {
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};

        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([7u8; 32]),
            CodeHash::new([9u8; 32]),
        );
        assert_eq!(
            key.to_string(),
            key.id().to_string(),
            "the dashboard joins hosting/subscription records (keyed by \
             key.to_string()) against governance records (keyed by \
             key.id().to_string()); if these ever diverge the contract detail \
             page silently stops finding governance data"
        );
    }

    /// The subscribed path — the one the page exists for — rendered end to
    /// end.
    ///
    /// Every other test here drives `subscribed == None` (no snapshot, an
    /// unknown key, or a hosted-only contract), so the Subscription card's
    /// actual logic was unexercised: the freshness pill, the in-use flag, the
    /// never-vs-ago branch on `last_updated_secs`, and the Identity card's
    /// instance-id row, which only renders when a subscription supplies one.
    /// Review caught that the primary lookup path had no coverage while four
    /// secondary paths did.
    #[test]
    fn contract_detail_renders_the_subscribed_path() {
        use crate::node::network_status::ContractSnapshot;

        // Slice to the Subscription card before asserting. The page embeds the
        // whole stylesheet inline, so `html.contains("fresh-ok")` is true of
        // every render — the CSS defines `.fresh-ok` whether or not the pill
        // is emitted. The first version of this test asserted against the full
        // document and passed vacuously on the positive case; only the
        // negative case ("stale must NOT contain fresh-ok") exposed it.
        fn subscription_panel(html: &str) -> String {
            let start = html
                .find("<h2>Subscription</h2>")
                .expect("the Subscription card must be rendered");
            let end = html[start..]
                .find("<h2>Hosting</h2>")
                .map(|i| start + i)
                .unwrap_or(html.len());
            html[start..end].to_string()
        }

        let base = |is_fresh: bool, in_use: bool, last: Option<u64>| {
            let mut snap = base_snapshot();
            snap.open_connections = 3;
            snap.contracts = vec![ContractSnapshot {
                key_short: "SUBB...".to_string(),
                key_full: "SUBBED1".to_string(),
                instance_id: "SUBBED1".to_string(),
                subscribed_secs: 3600,
                last_updated_secs: last,
                is_receiving_updates: is_fresh,
                in_use,
            }];
            contract_detail_html_from(&Some(snap), "SUBBED1", false, None)
        };

        // Fresh, in use, updated recently.
        let fresh_html = base(true, true, Some(30));
        let fresh = subscription_panel(&fresh_html);
        assert!(
            fresh.contains("fresh-ok") && fresh.contains("receiving updates"),
            "a contract in the update mesh must show the fresh pill — \
             got:\n{fresh}"
        );
        assert!(
            fresh.contains("30s ago"),
            "a known last-update time must be rendered as an age — got:\n{fresh}"
        );
        assert!(
            fresh_html.contains("Instance id"),
            "the instance-id row renders only when a subscription supplies \
             one, and this is that case — got:\n{fresh}"
        );

        // Not receiving updates, not pinned by demand, never updated.
        let stale_html = base(false, false, None);
        let stale = subscription_panel(&stale_html);
        assert!(
            stale.contains("fresh-stale") && stale.contains("not receiving updates"),
            "a contract outside the update mesh must NOT show as fresh — \
             serving a stale copy is the failure invariant 1 forbids, so the \
             page must not imply freshness it does not have. got:\n{stale}"
        );
        assert!(
            stale.contains("never"),
            "an absent last-update must read as 'never', not as an age of \
             zero — got:\n{stale}"
        );
        // The two states must be distinguishable, or the pill is decoration.
        assert!(
            fresh.contains("fresh-ok") && !stale.contains("fresh-ok"),
            "the freshness pill must differ between the two states"
        );
    }

    /// The abbreviating branch, which nothing else reaches.
    ///
    /// `abbreviate()` only runs when a contract has neither a subscription nor
    /// a hosting record but DOES have a governance one — an Evicted, Banned or
    /// WouldEvict contract this node no longer holds. That is a real and
    /// expected state, and every other test supplies a short `key_short` from
    /// a subscription or hosting entry instead, so the truncation arithmetic
    /// was never executed.
    ///
    /// The multi-byte case is the reason the function uses `.chars()` rather
    /// than byte slicing: a `&key[..12]` regression would panic on a
    /// non-ASCII boundary rather than fail politely. Contract keys are base58
    /// today, so this is defensive — which is exactly why it needs a test
    /// rather than a reader's confidence.
    #[test]
    fn contract_detail_abbreviates_a_long_governance_only_key() {
        use crate::node::network_status::{ContractGovernanceEntry, GovernanceStateSnapshot};

        let gov_only = |key: &str| {
            let mut snap = base_snapshot();
            snap.open_connections = 2;
            snap.governance.contracts = vec![ContractGovernanceEntry {
                instance_id: key.to_string(),
                instance_id_short: key.to_string(),
                state: GovernanceStateSnapshot::Banned,
                cost_used: 9.0,
                benefit_score: 0.1,
                log_ratio: Some(-2.0),
                age_secs: 120,
                last_transition_secs_ago: 30,
                history: Vec::new(),
            }];
            contract_detail_html_from(&Some(snap), key, false, None)
        };

        // 44 base58 characters, the real shape of a contract key.
        let long = "7WSdxLxjPvKgGZBqDpRuPMuoprnQBmXtnkHkDpTPTdcJ";
        let html = gov_only(long);
        assert!(
            html.contains("7WSdxLxjPvKg…"),
            "a governance-only contract has no short form to borrow, so the \
             page must abbreviate the key itself — got:\n{html}"
        );
        assert!(
            html.contains(long),
            "and must still show the full key, which is what the copy button \
             and the filter search on — got:\n{html}"
        );

        // Exactly at the boundary: 12 chars must NOT be truncated.
        let twelve = "123456789012";
        let at_boundary = gov_only(twelve);
        assert!(
            !at_boundary.contains("123456789012…"),
            "a key exactly at the cap is not longer than the cap, so it must \
             not gain an ellipsis — got:\n{at_boundary}"
        );

        // Multi-byte, to pin that truncation counts CHARACTERS not bytes. A
        // byte-slicing regression panics here rather than returning something
        // wrong, which is the failure mode worth catching early.
        let wide = "ααααααααααααααα";
        let multibyte = gov_only(wide);
        let expected: String = "α".repeat(12);
        assert!(
            multibyte.contains(&format!("{expected}…")),
            "a multi-byte key must abbreviate to 12 CHARACTERS, not 12 bytes. \
             The first version of this assertion was `contains(\"…\")` behind \
             an `||`, which is true either way — byte slicing yields 6 of \
             these 2-byte chars and sailed through it. Counting the characters \
             is what distinguishes the two — got:\n{multibyte}"
        );
    }

    /// A hosted-only contract CAN be cross-referenced against governance,
    /// and this pins the non-obvious reason why.
    ///
    /// `HostedContractEntry` carries no `instance_id` field, and governance is
    /// keyed by `ContractInstanceId`, so the join looks impossible. It is not:
    /// `impl Display for ContractKey` delegates to `self.instance` and
    /// `ContractKey::id()` returns `&self.instance`, so `key.to_string()` and
    /// `key.id().to_string()` are THE SAME STRING. The requested key is always
    /// a valid governance lookup value.
    ///
    /// This needs a test because the rustdoc on `ContractSnapshot::instance_id`
    /// asserts the opposite — "Distinct from `key_full` which carries the full
    /// ContractKey encoding" — which is wrong, and reading it caused a wrong
    /// turn on this page: a "cannot be cross-referenced" branch was added for
    /// a case that does not exist. If the stdlib ever makes the two encodings
    /// genuinely differ, this fails and says where to look.
    #[test]
    fn hosted_only_contract_still_joins_to_governance() {
        use crate::node::network_status::{ContractGovernanceEntry, GovernanceStateSnapshot};

        let key = "HOSTEDONLYKEY";
        let mut snap = base_snapshot();
        snap.open_connections = 2;
        // Hosted, with NO subscription entry to supply an instance id.
        snap.hosting.contracts = vec![crate::node::network_status::HostedContractEntry {
            key_full: key.to_string(),
            key_short: key.to_string(),
            size_bytes: 1024,
            read_count: 0,
            recency_seq: 0,
            eviction_eligible: true,
        }];
        // Governance knows it under the same string, because that string IS
        // the instance id.
        snap.governance.contracts = vec![ContractGovernanceEntry {
            instance_id: key.to_string(),
            instance_id_short: key.to_string(),
            state: GovernanceStateSnapshot::Borderline,
            cost_used: 3.0,
            benefit_score: 1.0,
            log_ratio: Some(-0.4),
            age_secs: 600,
            last_transition_secs_ago: 60,
            history: Vec::new(),
        }];

        let html = contract_detail_html_from(&Some(snap), key, false, None);
        assert!(
            html.contains("Borderline"),
            "a hosted-only contract must still show its governance state — \
             the requested key is a valid instance id. got:\n{html}"
        );
        assert!(
            !html.contains("Not flagged by the governance manager"),
            "and must not report it as unflagged when a record was found — \
             got:\n{html}"
        );
    }

    /// Governance history must show the NEWEST transitions.
    ///
    /// `GovernanceSnapshot::history` is documented as "newest last", so a
    /// plain `.take(10)` renders the ten OLDEST and hides everything recent —
    /// exactly inverted from what someone opening the page wants. The bug is
    /// invisible until a contract accumulates more than ten transitions, which
    /// is why it needs a test rather than a look.
    #[test]
    fn contract_detail_shows_the_newest_governance_transitions() {
        use crate::node::network_status::{
            ContractGovernanceEntry, GovernanceStateSnapshot, GovernanceTransitionEntry,
            GovernanceTransitionReasonSnapshot,
        };

        // 14 transitions, oldest first, distinguishable by their age.
        let history: Vec<GovernanceTransitionEntry> = (0..14)
            .map(|i| GovernanceTransitionEntry {
                secs_ago: (14 - i) * 60,
                from: GovernanceStateSnapshot::Normal,
                to: GovernanceStateSnapshot::Borderline,
                reason: GovernanceTransitionReasonSnapshot::ThresholdCrossed,
            })
            .collect();
        let oldest_secs = history[0].secs_ago;
        let newest_secs = history[13].secs_ago;

        let mut snap = base_snapshot();
        snap.open_connections = 2;
        snap.governance.contracts = vec![ContractGovernanceEntry {
            instance_id: "GOVKEY1".to_string(),
            instance_id_short: "GOVKEY1".to_string(),
            state: GovernanceStateSnapshot::Borderline,
            cost_used: 1.0,
            benefit_score: 2.0,
            log_ratio: Some(0.5),
            age_secs: 900,
            last_transition_secs_ago: newest_secs,
            history,
        }];

        let html = contract_detail_html_from(&Some(snap), "GOVKEY1", false, None);
        let newest = format_duration(newest_secs);
        let oldest = format_duration(oldest_secs);
        assert!(
            html.contains(&format!("{newest} ago")),
            "the most recent transition ({newest} ago) must be shown — \
             got:\n{html}"
        );
        assert!(
            !html.contains(&format!("{oldest} ago")),
            "the oldest transition ({oldest} ago) must have been dropped by \
             the cap, not the newest — got:\n{html}"
        );
    }

    /// The hosting panel must NOT imply it is showing the eviction ranking.
    ///
    /// Invariant 3 ranks by local subscriptions, then downstream subscribers,
    /// then recency. Only recency is in the snapshot; the two counts that
    /// OUTRANK it are computed during the sweep and are unavailable here. A
    /// panel that showed recency alone, unqualified, would read as "this is
    /// why the contract is kept" — which is exactly the falsehood PR #5371
    /// removed from the eviction card, reintroduced on a new page.
    #[test]
    fn contract_detail_says_the_eviction_ranking_is_not_shown() {
        let mut snap = base_snapshot();
        snap.open_connections = 2;
        snap.hosting.contracts = vec![crate::node::network_status::HostedContractEntry {
            key_full: "TESTKEY1".to_string(),
            key_short: "TESTKEY1".to_string(),
            size_bytes: 4096,
            read_count: 3,
            recency_seq: 42,
            eviction_eligible: true,
        }];
        let html = contract_detail_html_from(&Some(snap), "TESTKEY1", false, None);
        assert!(
            html.contains("not shown"),
            "the hosting panel must say the ranking keys are missing — \
             got:\n{html}"
        );
        assert!(
            html.contains("5372"),
            "and point at the issue tracking them, so the gap is followable \
             rather than a dead end — got:\n{html}"
        );
    }

    // ─── Merge-law card (#5397) ──────────────────────────────────────────

    /// Extracts the Merge laws card. The whole stylesheet is inlined into
    /// every render, so a bare `html.contains("fresh-ok")` is true whether or
    /// not this card actually used that class — see `subscription_panel`
    /// above, which exists for the identical reason.
    fn merge_panel(html: &str) -> String {
        let start = html
            .find("<h2>Merge laws</h2>")
            .expect("the Merge laws card must be rendered");
        let end = html[start..]
            .find("<h2>Governance</h2>")
            .map(|i| start + i)
            .unwrap_or(html.len());
        html[start..end].to_string()
    }

    /// One checked-contract record, the shape `conformance::status` stores.
    ///
    /// Built through `CheckedContract::new` + `note_finding`, the only route
    /// production has and — since `findings` became private — the only route
    /// anything has. A struct literal here would let a test construct a record
    /// production could not, which is precisely what hid `record`'s
    /// un-deduplicating insert arm.
    ///
    /// Note what `note_finding` does to `findings` on the way in: it inserts at the
    /// FRONT and drops a property already present. So the record comes back with the
    /// list reversed and any duplicate property gone — the signature reads like "these
    /// findings, in this order" and it is not. Today's callers assert presence, not
    /// order; do not write an order-dependent assertion against this helper without
    /// reading that.
    fn checked_record(
        contract: freenet_stdlib::prelude::ContractInstanceId,
        verdicts: usize,
        inconclusive: usize,
        findings: Vec<MergeFinding>,
    ) -> CheckedContract {
        // The timestamp is overwritten by `record` with the tick's publish time,
        // exactly as `status::checked_contracts` does in production.
        let mut record = CheckedContract::new(
            contract,
            verdicts,
            inconclusive,
            tokio::time::Instant::now(),
        );
        for finding in findings {
            record.note_finding(finding);
        }
        record
    }

    /// What the page reads: one contract's record plus the node-wide numbers.
    ///
    /// Goes through `MergeCheckStatus::view_for`, the same call the wrapper
    /// makes, rather than building a `MergeCheckView` by hand — a hand-built
    /// view would let a test assert about a record the real lookup would never
    /// have returned.
    fn merge_view(
        status: &MergeCheckStatus,
        contract: &freenet_stdlib::prelude::ContractInstanceId,
    ) -> MergeCheckView {
        status.view_for(Some(contract), tokio::time::Instant::now())
    }

    /// Text with the markup stripped, so "no digit in the visible content"
    /// assertions aren't defeated by the `2` in every `<h2>` tag.
    fn visible_text(html: &str) -> String {
        let mut out = String::new();
        let mut in_tag = false;
        for c in html.chars() {
            match c {
                '<' => in_tag = true,
                '>' => in_tag = false,
                _ if !in_tag => out.push(c),
                _ => {}
            }
        }
        out
    }

    /// A hosted-only contract: the cheapest fixture that reaches any card at
    /// all. A key with no subscription, hosting, or governance record
    /// short-circuits to the "not found" page before the Merge laws card (or
    /// any other card) ever renders.
    fn hosted_snap(key: &str) -> NetworkStatusSnapshot {
        let mut snap = base_snapshot();
        snap.open_connections = 2;
        snap.hosting.contracts = vec![crate::node::network_status::HostedContractEntry {
            key_full: key.to_string(),
            key_short: key.to_string(),
            size_bytes: 1024,
            read_count: 0,
            recency_seq: 0,
            eviction_eligible: true,
        }];
        snap
    }

    /// State 1: merge-law checking is not running on this node at all.
    ///
    /// Must say so plainly, and must render NO count at all — not even a
    /// zero. A rendered "0" here would read as "0 violations found", which is
    /// indistinguishable from a clean bill of health and is exactly the
    /// conflation `conformance::status`'s module doc exists to prevent.
    #[test]
    fn merge_card_reports_checking_disabled_with_no_digits() {
        use freenet_stdlib::prelude::ContractInstanceId;

        let key = ContractInstanceId::new([1u8; 32]).to_string();
        let snap = hosted_snap(&key);
        let html = contract_detail_html_from(&Some(snap), &key, false, None);
        let panel = merge_panel(&html);
        assert!(
            panel.to_ascii_lowercase().contains("not enabled"),
            "must say plainly that checking is off — got:\n{panel}"
        );
        assert!(
            panel.to_ascii_lowercase().contains("default"),
            "must say this is the default state, not a fault — got:\n{panel}"
        );
        assert!(
            !visible_text(&panel).chars().any(|c| c.is_ascii_digit()),
            "no digit may appear in the VISIBLE text when checking is off — \
             a rendered count (even a zero) would read as a clean result \
             rather than as unmeasured. got:\n{panel}"
        );
    }

    /// State 2a: checking is running, but this contract falls outside the
    /// bounded recently-checked window.
    ///
    /// Must NOT be read as clean — the window is bounded, so absence here is
    /// absence of knowledge, not a clean bill of health.
    #[test]
    fn merge_card_reports_not_recently_checked_as_unknown_not_clean() {
        use freenet_stdlib::prelude::ContractInstanceId;

        let key_id = ContractInstanceId::new([2u8; 32]);
        let key = key_id.to_string();
        // A DIFFERENT contract was checked; this one was not.
        let other = ContractInstanceId::new([9u8; 32]);
        let mut status = MergeCheckStatus::default();
        status.record(
            [checked_record(other, 5, 0, vec![])],
            1,
            0,
            tokio::time::Instant::now(),
        );

        let snap = hosted_snap(&key);
        let view = merge_view(&status, &key_id);
        let html = contract_detail_html_from(&Some(snap), &key, true, Some(&view));
        let panel = merge_panel(&html);
        assert!(
            panel.contains("has not been checked recently"),
            "must say the contract fell outside the checked window — \
             got:\n{panel}"
        );
        assert!(
            !panel
                .to_ascii_lowercase()
                .contains("no merge-law violation"),
            "must not claim a clean result for a contract the checker never \
             looked at — got:\n{panel}"
        );
    }

    /// State 2b: checking is running, but has not published its first tick
    /// yet (`snapshot()` is still `None`).
    ///
    /// This must render the same "not checked recently" message as 2a, not
    /// "not enabled" — the checker IS on, it simply has nothing to report
    /// yet. Conflating this with "off" would misreport a starting node as one
    /// with checking disabled.
    #[test]
    fn merge_card_reports_not_checked_before_first_publish() {
        use freenet_stdlib::prelude::ContractInstanceId;

        let key = ContractInstanceId::new([3u8; 32]).to_string();
        let snap = hosted_snap(&key);
        let html = contract_detail_html_from(&Some(snap), &key, true, None);
        let panel = merge_panel(&html);
        assert!(
            panel.contains("has not been checked recently"),
            "before the first tick publishes, the honest answer is 'on, \
             nothing established yet', which must read the same as the \
             bounded-window case above — got:\n{panel}"
        );
        assert!(
            !panel.to_ascii_lowercase().contains("not enabled"),
            "must not be conflated with checking being off — got:\n{panel}"
        );
    }

    /// State 3: checked, no findings for this contract.
    ///
    /// Must say plainly that no violation was found AND show how many cases
    /// ran, so the reader can judge whether a clean result reflects a
    /// meaningful sample or barely having looked.
    #[test]
    fn merge_card_reports_clean_result_with_case_count() {
        use freenet_stdlib::prelude::ContractInstanceId;

        let key_id = ContractInstanceId::new([4u8; 32]);
        let key = key_id.to_string();
        let mut status = MergeCheckStatus::default();
        status.record(
            [checked_record(key_id, 250, 4, vec![])],
            1,
            0,
            tokio::time::Instant::now(),
        );

        let snap = hosted_snap(&key);
        let view = merge_view(&status, &key_id);
        let html = contract_detail_html_from(&Some(snap), &key, true, Some(&view));
        let panel = merge_panel(&html);
        assert!(
            panel
                .to_ascii_lowercase()
                .contains("no merge-law violation"),
            "a clean, checked contract must say so plainly — got:\n{panel}"
        );
        assert!(
            panel.contains("250 reached a verdict"),
            "must show how many of THIS contract's cases reached a verdict, so a \
             clean result can be judged for how meaningful it is — got:\n{panel}"
        );
        assert!(
            panel.contains("4 inconclusive"),
            "must show this contract's inconclusive count too: a contract with one \
             verdict and 199 inconclusive cases must not render like one with 200 \
             verdicts — got:\n{panel}"
        );
        assert!(
            !panel
                .to_ascii_lowercase()
                .contains("could not reach a verdict"),
            "the fleet-wide unjudged note must NOT appear when the count is \
             zero — got:\n{panel}"
        );
    }

    /// A converging idempotence finding must not read the same as a
    /// non-convergent one.
    ///
    /// Severity cannot separate them — since #5462 both are `Severity::Violation`,
    /// deliberately — so the panel has to read `settling`. For a whole review round
    /// `MergeFinding` carried that field while the only code that tells an operator
    /// what a row means ignored it, and the field's own rustdoc said to consult it.
    /// A test asserting the field is populated would have passed throughout; this
    /// asserts what the operator actually sees.
    #[test]
    fn the_panel_separates_a_settling_finding_from_a_non_convergent_one() {
        use crate::conformance::property::IdempotenceSettling;
        use freenet_stdlib::prelude::ContractInstanceId;

        let rendered = |settling| {
            let key_id = ContractInstanceId::new([7u8; 32]);
            let key = key_id.to_string();
            let mut status = MergeCheckStatus::default();
            status.record(
                [checked_record(
                    key_id,
                    40,
                    0,
                    vec![MergeFinding {
                        contract: key_id,
                        property: "state_idempotence",
                        severity: Severity::Violation,
                        settling,
                        would_remove: true,
                    }],
                )],
                1,
                0,
                tokio::time::Instant::now(),
            );
            let snap = hosted_snap(&key);
            let view = merge_view(&status, &key_id);
            let html = contract_detail_html_from(&Some(snap), &key, true, Some(&view));
            merge_panel(&html)
        };

        let settled = rendered(Some(IdempotenceSettling::SettledAfter(1)));
        let never = rendered(Some(IdempotenceSettling::NeverSettled));

        assert_ne!(
            settled, never,
            "a contract that converges after one rewrite and one that never settles \
             must not render identically — that is the misreading #5462 exists to \
             prevent:\nsettled:\n{settled}\nnever:\n{never}"
        );
        assert!(
            settled.contains("converges"),
            "the settling case must say so in words the operator reads:\n{settled}"
        );
    }

    /// State 4: checked, with findings — the state the whole card exists for.
    ///
    /// Each finding must show its property, and a `Violation` must read as
    /// visibly more serious than a `Diagnostic`: the former is removal-eligible
    /// under enforcement, the latter is legal but wasteful. A panel that only
    /// printed the bare enum name would satisfy "shows severity" without making
    /// the distinction an operator actually needs.
    ///
    /// This asserted the panel said "cannot converge" until #5462. That stopped
    /// being true of every `Violation`: `state_idempotence` now reports a
    /// canonicalizing contract, which breaks idempotence and still converges. The
    /// assertion is retargeted rather than dropped, because the property it
    /// guards — an explanation rather than a bare enum name — is unchanged.
    #[test]
    fn merge_card_lists_findings_with_severity_and_removal_distinguished() {
        use freenet_stdlib::prelude::ContractInstanceId;

        let key_id = ContractInstanceId::new([5u8; 32]);
        let key = key_id.to_string();
        let mut status = MergeCheckStatus::default();
        status.record(
            [checked_record(
                key_id,
                40,
                0,
                vec![
                    MergeFinding {
                        contract: key_id,
                        property: "state_commutativity",
                        severity: Severity::Violation,
                        settling: None,
                        would_remove: true,
                    },
                    MergeFinding {
                        contract: key_id,
                        property: "self_delta_empty",
                        severity: Severity::Diagnostic,
                        settling: None,
                        would_remove: false,
                    },
                ],
            )],
            1,
            0,
            tokio::time::Instant::now(),
        );

        let snap = hosted_snap(&key);
        let view = merge_view(&status, &key_id);
        let html = contract_detail_html_from(&Some(snap), &key, true, Some(&view));
        let panel = merge_panel(&html);
        assert!(
            panel.contains("state_commutativity") && panel.contains("self_delta_empty"),
            "both findings for this contract must be listed — got:\n{panel}"
        );
        assert!(
            panel.contains("removal-eligible"),
            "a Violation must be explained, not just labelled with the bare \
             enum name — got:\n{panel}"
        );
        assert!(
            panel.contains("legal but wasteful"),
            "a Diagnostic must be visibly distinguished from a Violation, \
             not merely a different word for the same thing — got:\n{panel}"
        );
    }

    /// The fleet-wide "could not judge" count must be surfaced when non-zero,
    /// independent of this contract's own state — it is information the
    /// operator needs regardless of which contract's page they are viewing.
    #[test]
    fn merge_card_surfaces_contracts_without_verdict_fleet_wide() {
        use freenet_stdlib::prelude::ContractInstanceId;

        let key_id = ContractInstanceId::new([6u8; 32]);
        let key = key_id.to_string();
        let mut status = MergeCheckStatus::default();
        status.record(
            [checked_record(key_id, 400, 0, vec![])],
            10,
            3,
            tokio::time::Instant::now(),
        );

        let snap = hosted_snap(&key);
        let view = merge_view(&status, &key_id);
        let html = contract_detail_html_from(&Some(snap), &key, true, Some(&view));
        let panel = merge_panel(&html);
        assert!(
            panel.contains('3'),
            "the unjudged count must be surfaced when non-zero — \
             got:\n{panel}"
        );
        assert!(
            panel
                .to_ascii_lowercase()
                .contains("could not reach a verdict"),
            "must be phrased as an inability to judge, not folded silently \
             into the clean result above it — got:\n{panel}"
        );
        assert!(
            panel.contains("most recent tick"),
            "the unjudged count is a per-TICK number and must be phrased as one. \
             The earlier wording ('on this node') read as a standing property of \
             the peer — got:\n{panel}"
        );
    }

    /// #5403 H1: a contract with a finding must never render the green pill,
    /// however many other contracts have been checked since.
    ///
    /// The two-window version kept a 256-entry checked list and a 64-entry
    /// findings list. `merge_law_card` picked its branch from the first and
    /// read the second, so a contract inside one and evicted from the other
    /// rendered "no merge-law violation was found for this contract" — for a
    /// contract the checker had positively found violating. Worse, that was
    /// the steady state: a re-detected finding was deduplicated rather than
    /// moved to the front, so the contract caught on every tick was the one
    /// likeliest to lose its finding while its checked entry was refreshed.
    ///
    /// A hundred intervening contracts is well past the old findings cap and
    /// well short of the checked cap, which is exactly the gap the bug lived
    /// in. This test fails against the two-list code.
    #[test]
    fn merge_card_keeps_a_finding_after_the_old_findings_cap_would_have_evicted_it() {
        use freenet_stdlib::prelude::ContractInstanceId;

        let key_id = ContractInstanceId::new([7u8; 32]);
        let key = key_id.to_string();
        let mut status = MergeCheckStatus::default();
        status.record(
            [checked_record(
                key_id,
                12,
                0,
                vec![MergeFinding {
                    contract: key_id,
                    property: "state_commutativity",
                    severity: Severity::Violation,
                    settling: None,
                    would_remove: true,
                }],
            )],
            1,
            0,
            tokio::time::Instant::now(),
        );
        for i in 0..100u8 {
            let other = ContractInstanceId::new([100u8.wrapping_add(i); 32]);
            status.record(
                [checked_record(
                    other,
                    3,
                    0,
                    vec![MergeFinding {
                        contract: other,
                        property: "state_idempotence",
                        severity: Severity::Violation,
                        settling: None,
                        would_remove: true,
                    }],
                )],
                1,
                0,
                tokio::time::Instant::now(),
            );
        }

        let snap = hosted_snap(&key);
        let view = merge_view(&status, &key_id);
        let html = contract_detail_html_from(&Some(snap), &key, true, Some(&view));
        let panel = merge_panel(&html);
        assert!(
            !panel
                .to_ascii_lowercase()
                .contains("no merge-law violation"),
            "a contract the checker FOUND violating rendered a clean result \
             because later contracts pushed its finding out of a separately \
             capped list — got:\n{panel}"
        );
        assert!(
            panel.contains("state_commutativity"),
            "the finding must still be listed while the contract is still in \
             the checked window — got:\n{panel}"
        );
    }

    /// #5403 H1, at the surface an operator reads: the card shows one row per
    /// broken law, not one per violating case.
    ///
    /// Driven end to end through the production assembly — a real probe of the
    /// deliberately-defective fixture contract, then `status::checked_contracts`,
    /// `MergeCheckStatus::record`, `view_for`, and the real page function. Every
    /// other merge-card test above hand-builds a `CheckedContract`, so none of them
    /// could see a defect in the code that BUILDS one; three review rounds each
    /// found a defect on that path and no test failed.
    ///
    /// Mode 6 (NEVER_SETTLES) breaks two merge laws on most generated cases, and one
    /// probe returns thirteen findings across those two. Before the fix the card
    /// rendered thirteen rows, eleven of them byte-identical duplicates, which then
    /// persisted for the record's whole residency in the window because later ticks
    /// deduplicate AGAINST them.
    #[tokio::test(flavor = "multi_thread")]
    async fn merge_card_renders_one_row_per_broken_law_from_a_real_probe()
    -> Result<(), Box<dyn std::error::Error>> {
        // Keep in sync with the mode constants in
        // `tests/test-contract-conformance/src/lib.rs`.
        const NEVER_SETTLES: u8 = 6;

        let (id, report, findings) =
            crate::conformance::shadow::probe_fixture_contract(NEVER_SETTLES).await?;
        let distinct: std::collections::BTreeSet<&str> = findings
            .iter()
            .map(|f| f.violation.property.as_str())
            .collect();
        assert!(
            findings.len() > distinct.len() && distinct.len() > 1,
            "the probe returned {} findings across {} properties, so this test can \
             no longer tell one row per LAW from one row per CASE",
            findings.len(),
            distinct.len()
        );

        let mut status = MergeCheckStatus::default();
        status.record(
            crate::conformance::status::checked_contracts(&report.judged, &findings),
            report.judged.len(),
            report.without_verdict,
            tokio::time::Instant::now(),
        );

        let key = id.to_string();
        let snap = hosted_snap(&key);
        let view = merge_view(&status, &id);
        let html = contract_detail_html_from(&Some(snap), &key, true, Some(&view));
        let panel = merge_panel(&html);

        let rows = panel.matches("<tr><td>").count();
        assert_eq!(
            rows,
            distinct.len(),
            "the card rendered {rows} finding rows for {} broken laws — a single \
             broken law repeated across a probe's cases becomes a wall of identical \
             rows. got:\n{panel}",
            distinct.len()
        );
        for property in &distinct {
            assert_eq!(
                panel.matches(property).count(),
                1,
                "{property} appears more than once on the card:\n{panel}"
            );
        }
        Ok(())
    }

    /// A checker that has stopped publishing must say so, not present its last
    /// tick as a current result.
    ///
    /// `status::publish` is reached from two places in `capture::run_writer`, and
    /// a peer can stop reaching either indefinitely while the old snapshot
    /// stands: the probe task can panic, a probe can hang so `in_flight` never
    /// clears and no further tick starts, or the writer task can be gone. A peer
    /// whose probe has been dead for a week would otherwise keep serving that
    /// week-old "no violation found" with nothing on the page to say when it was
    /// established.
    #[test]
    fn merge_card_reports_a_frozen_checker_rather_than_a_current_clean_result() {
        use freenet_stdlib::prelude::ContractInstanceId;

        let key_id = ContractInstanceId::new([8u8; 32]);
        let key = key_id.to_string();
        let mut status = MergeCheckStatus::default();
        status.record(
            [checked_record(key_id, 30, 0, vec![])],
            1,
            0,
            tokio::time::Instant::now(),
        );

        let snap = hosted_snap(&key);
        // A week later. `view_for` takes `now` so the staleness branch does not
        // need a week of wall clock to reach.
        let a_week = std::time::Duration::from_secs(7 * 24 * 60 * 60);
        let view = status.view_for(Some(&key_id), tokio::time::Instant::now() + a_week);
        let html = contract_detail_html_from(&Some(snap), &key, true, Some(&view));
        let panel = merge_panel(&html);
        assert!(
            panel.contains("has not published"),
            "a week-old snapshot rendered as a current result, with nothing on \
             the card saying when the checker last ran — got:\n{panel}"
        );
        assert!(
            panel.contains("Last checker tick"),
            "every state with a snapshot must show how old that snapshot is — \
             got:\n{panel}"
        );
    }

    /// #5403 M1: a contract the checker has FOUND VIOLATING must not render as one
    /// this node has never heard of.
    ///
    /// The page short-circuited to `contract_not_found.html` — "This node knows
    /// nothing about &lt;key&gt;" — whenever the contract was absent from the
    /// subscribed, hosted and governance sections of the snapshot, and threw the
    /// already-computed merge view away. Those three empty and a merge record present
    /// is not a corner: the checked window holds ~32 hours while the hosting cache
    /// evicts under budget pressure well inside that, and
    /// `network_status::get_snapshot()` returning `None` empties all three at once.
    /// So the state the page denied all knowledge of is exactly the state where it
    /// had the most alarming thing to say.
    #[test]
    fn a_contract_absent_from_the_snapshot_but_found_violating_is_not_reported_unknown() {
        use freenet_stdlib::prelude::ContractInstanceId;

        let key_id = ContractInstanceId::new([21u8; 32]);
        let key = key_id.to_string();
        let mut status = MergeCheckStatus::default();
        status.record(
            [checked_record(
                key_id,
                8,
                0,
                vec![MergeFinding {
                    contract: key_id,
                    property: "state_commutativity",
                    severity: Severity::Violation,
                    settling: None,
                    would_remove: true,
                }],
            )],
            1,
            0,
            tokio::time::Instant::now(),
        );
        let view = merge_view(&status, &key_id);

        // No snapshot at all: the harshest form of the case, and one a live node
        // reaches whenever `get_snapshot()` returns None.
        let html = contract_detail_html_from(&None, &key, true, Some(&view));
        assert!(
            !html.contains("knows nothing about"),
            "the page denied all knowledge of a contract it had positively found \
             violating a merge law — got:\n{html}"
        );
        let panel = merge_panel(&html);
        assert!(
            panel.contains("state_commutativity"),
            "the finding must still be shown when the contract is absent from the \
             snapshot's other sections — got:\n{panel}"
        );
    }

    /// The not-found page must still be reachable, or the fix above would simply
    /// have deleted a state.
    ///
    /// A key nothing knows anything about — no snapshot entry AND no merge record —
    /// is genuinely unknown and must say so, rather than rendering an empty detail
    /// page that reads as a contract with nothing wrong with it.
    #[test]
    fn a_contract_nothing_knows_about_is_still_reported_unknown() {
        use freenet_stdlib::prelude::ContractInstanceId;

        let key_id = ContractInstanceId::new([22u8; 32]);
        let other = ContractInstanceId::new([23u8; 32]);
        let key = key_id.to_string();
        // The checker HAS published, and has a record — for a different contract.
        let mut status = MergeCheckStatus::default();
        status.record(
            [checked_record(other, 5, 0, vec![])],
            1,
            0,
            tokio::time::Instant::now(),
        );
        let view = merge_view(&status, &key_id);
        assert!(
            view.contract.is_none(),
            "this test only means something while the requested contract has no record"
        );

        let html = contract_detail_html_from(&None, &key, true, Some(&view));
        assert!(
            html.contains("knows nothing about"),
            "a genuinely unknown contract stopped being reported as unknown, so the \
             merge-record exemption swallowed the not-found state entirely — \
             got:\n{html}"
        );
    }

    /// #5403 M2: "when was THIS contract last checked?" must be answered with this
    /// contract's own record, not with the node's most recent tick.
    ///
    /// The card put node-wide `published_secs_ago` in the same info-grid as the
    /// per-contract case counts, immediately beside "No merge-law violation was found
    /// for this contract the last time it was checked". The checker probes at most two
    /// contracts per fifteen-minute tick against a window holding 256, so a contract
    /// judged twenty hours ago rendered as checked three minutes ago — a confident
    /// freshness claim about a stale result. It is the same misattribution the PR had
    /// already fixed one row up for the COUNTS; see `status::judged_last_tick`.
    #[test]
    fn merge_card_ages_this_contract_from_its_own_record_not_the_last_node_tick() {
        use freenet_stdlib::prelude::ContractInstanceId;

        let key_id = ContractInstanceId::new([24u8; 32]);
        let key = key_id.to_string();
        let judged_at = tokio::time::Instant::now();
        // Added rather than subtracted: `Instant` is monotonic from boot, so
        // `now - 20h` panics on a host that has been up for less than that.
        let twenty_hours = std::time::Duration::from_secs(20 * 60 * 60);

        let mut status = MergeCheckStatus::default();
        status.record([checked_record(key_id, 12, 0, vec![])], 1, 0, judged_at);
        // Twenty hours of ticks that never touched this contract. The node is
        // perfectly healthy; the record is not fresh.
        status.record([], 2, 0, judged_at + twenty_hours);

        let snap = hosted_snap(&key);
        let view = status.view_for(Some(&key_id), judged_at + twenty_hours);
        let html = contract_detail_html_from(&Some(snap), &key, true, Some(&view));
        let panel = merge_panel(&html);

        assert!(
            panel.contains("This contract last checked"),
            "the card gives no per-contract age at all, so the only age on it is the \
             node's — got:\n{panel}"
        );
        assert!(
            panel.contains("20h"),
            "a contract judged twenty hours ago is rendered as checked at the node's \
             most recent tick, beside a sentence about what was found 'the last time \
             it was checked' — got:\n{panel}"
        );
        // And the node-wide number is still there, still labelled as node-wide.
        assert!(
            panel.contains("Last checker tick"),
            "the node-wide tick age must remain: a frozen checker is a different \
             fault from a stale record, and the card has to be able to show both — \
             got:\n{panel}"
        );
    }

    /// The value rendered against one `info-label` in the merge card.
    ///
    /// Both ages sit in the same `info-grid` and both end in " ago", so a
    /// `panel.contains("5m")` cannot tell which of them it matched — and the whole
    /// question these two tests ask is which clock answered which label.
    fn info_value(panel: &str, label: &str) -> String {
        let anchor = format!(r#">{label}</div><div class="info-value">"#);
        let start = panel
            .find(&anchor)
            .unwrap_or_else(|| panic!("the card renders no `{label}` row:\n{panel}"))
            + anchor.len();
        let end = panel[start..]
            .find("</div>")
            .expect("an info-value must be closed");
        panel[start..start + end].to_string()
    }

    /// #5403 L3: re-checking a contract must refresh its per-contract age.
    ///
    /// `MergeCheckStatus::record` stamps `checked_at` in BOTH of its arms. Confining
    /// that assignment to the insert (`None`) arm compiles, and the test above cannot
    /// see it: that test records the contract ONCE, so it only ever drives the insert
    /// arm. A contract the checker looks at every tick would then render with the age
    /// of the first time it was ever seen, growing without bound while the checker
    /// was in fact judging it every fifteen minutes — M2's mirror image (there the
    /// per-contract age was too fresh; here it would be too stale), and the same
    /// fault of the card answering one question with another clock.
    #[test]
    fn re_checking_a_contract_refreshes_its_per_contract_age() {
        use freenet_stdlib::prelude::ContractInstanceId;

        let key_id = ContractInstanceId::new([26u8; 32]);
        let key = key_id.to_string();
        let first_seen = tokio::time::Instant::now();
        let twenty_hours = std::time::Duration::from_secs(20 * 60 * 60);
        let five_minutes = std::time::Duration::from_secs(5 * 60);

        let mut status = MergeCheckStatus::default();
        // First sight: the insert arm.
        status.record([checked_record(key_id, 12, 0, vec![])], 1, 0, first_seen);
        // Twenty hours later the checker comes back to the SAME contract: the merge
        // arm, which is the arm nothing covered.
        status.record(
            [checked_record(key_id, 3, 0, vec![])],
            1,
            0,
            first_seen + twenty_hours,
        );

        let snap = hosted_snap(&key);
        let view = status.view_for(Some(&key_id), first_seen + twenty_hours + five_minutes);
        let html = contract_detail_html_from(&Some(snap), &key, true, Some(&view));
        let panel = merge_panel(&html);

        let per_contract = info_value(&panel, "This contract last checked");
        let node_wide = info_value(&panel, "Last checker tick");
        assert_eq!(
            per_contract, node_wide,
            "the contract was re-checked by the most recent tick, so its own age and \
             the node's must agree. They do not, which means the merge arm left \
             `checked_at` at first sight: a contract judged every tick renders as one \
             last looked at hours or days ago — got {per_contract:?} against \
             {node_wide:?} in:\n{panel}"
        );
        assert!(
            !per_contract.contains("20h"),
            "the re-checked contract is aged from when it was FIRST seen, not from \
             the tick that last judged it — got {per_contract:?} in:\n{panel}"
        );
        // The accumulation the merge arm also owns, so this test fails loudly rather
        // than vacuously if a refactor stops merging records at all.
        assert!(
            panel.contains("15 reached a verdict"),
            "the two ticks' case counts did not accumulate, so the merge arm did not \
             run and this test proves nothing about it — got:\n{panel}"
        );
    }

    /// #5403 L2: a contract with no record must still show how old the snapshot is.
    ///
    /// The card's own doc says every state that has a snapshot renders when that
    /// snapshot was published. The no-record branch did not: the publish age reached
    /// it only through the staleness note, which fires at three missed ticks. Below
    /// that threshold — a checker that died fourteen minutes ago — "this contract has
    /// not been checked recently" carried no age at all, and a busy checker that has
    /// simply not got round to this contract rendered identically to one that had
    /// stopped. That is the same absence-reads-as-fine conflation this subsystem
    /// exists to stop, in the one state where the page has nothing else to say.
    #[test]
    fn a_contract_with_no_record_still_shows_how_old_the_snapshot_is() {
        use freenet_stdlib::prelude::ContractInstanceId;

        let key_id = ContractInstanceId::new([27u8; 32]);
        let other = ContractInstanceId::new([28u8; 32]);
        let key = key_id.to_string();
        let published = tokio::time::Instant::now();

        // The checker HAS published — for a different contract, so the requested one
        // has no record.
        let mut status = MergeCheckStatus::default();
        status.record([checked_record(other, 5, 0, vec![])], 1, 0, published);

        // Well inside `STALE_AFTER` (45 minutes), so the staleness note does NOT
        // fire and cannot supply the age on this branch's behalf. That is the whole
        // window the branch was blind in.
        let view = status.view_for(
            Some(&key_id),
            published + std::time::Duration::from_secs(840),
        );
        assert!(
            view.contract.is_none() && !view.stale,
            "this test only means anything for a fresh snapshot with no record for \
             the requested contract"
        );

        let snap = hosted_snap(&key);
        let html = contract_detail_html_from(&Some(snap), &key, true, Some(&view));
        let panel = merge_panel(&html);

        assert!(
            panel.contains("has not been checked recently"),
            "this test must be reading the no-record branch — got:\n{panel}"
        );
        assert_eq!(
            info_value(&panel, "Last checker tick"),
            "14m ago",
            "the no-record state renders no snapshot age, so an operator cannot tell \
             a busy checker that has not reached this contract from one that stopped \
             fourteen minutes ago — got:\n{panel}"
        );
    }

    /// #5403 M4: the unjudged count's explanation must name what it actually counts.
    ///
    /// It was explained as "every case it tried was inconclusive, or related state
    /// exceeded its budget", which names the two rarest entries on the list
    /// `shadow::ShadowReport::without_verdict` accumulates. That list also holds: no
    /// contract store, an empty corpus, code that would not resolve, an oracle that
    /// would not build, setup that exhausted the time budget before one case ran, a
    /// dead probe task, and `awaiting_samples` — for most of which ZERO cases were
    /// tried, and on a warming-up peer `awaiting_samples` dominates outright. An
    /// operator reading the old sentence would go looking for contracts whose cases
    /// were all inconclusive and find none.
    #[test]
    fn merge_card_explains_the_unjudged_count_by_what_it_counts() {
        use freenet_stdlib::prelude::ContractInstanceId;

        let key_id = ContractInstanceId::new([25u8; 32]);
        let key = key_id.to_string();
        let mut status = MergeCheckStatus::default();
        status.record(
            [checked_record(key_id, 40, 0, vec![])],
            1,
            4,
            tokio::time::Instant::now(),
        );

        let snap = hosted_snap(&key);
        let view = merge_view(&status, &key_id);
        let html = contract_detail_html_from(&Some(snap), &key, true, Some(&view));
        let panel = merge_panel(&html);

        assert!(
            panel.contains("no samples collected"),
            "the dominant cause on a warming-up peer — focus picked a contract the \
             sampler holds nothing for — is not named at all — got:\n{panel}"
        );
        assert!(
            !panel.contains("every case it tried was inconclusive"),
            "the explanation still asserts that every unjudged contract ran cases, \
             which is false for most of them and for all of the common ones — \
             got:\n{panel}"
        );
    }

    /// #5403 L6: every property name these card tests plant must be one the checker
    /// can actually emit.
    ///
    /// `merge_card_lists_findings_with_severity_and_removal_distinguished` asserted on
    /// `self_delta_size`, which `ConformanceProperty::as_str` cannot produce — the
    /// real name is `self_delta_empty`. The test was internally consistent (it planted
    /// the string and then found it), so it passed while asserting about a card state
    /// no node will ever render, and the property it claimed to cover — that a
    /// Diagnostic renders distinguishably from a Violation — was never exercised
    /// against a real Diagnostic property at all.
    ///
    /// Fixing the one literal would leave the class open, so this reads every
    /// `MergeFinding` property literal planted in this file and checks it against
    /// `ConformanceProperty::ALL`. A future fixture invented out of thin air fails
    /// here instead of quietly testing nothing.
    ///
    /// Nothing in this doc comment may spell the scrape's needle, or the scrape finds
    /// its own prose — which it did on the first run, and failed loudly rather than
    /// silently, because an unreal name is exactly what it rejects.
    #[test]
    fn merge_card_fixtures_only_use_property_names_the_checker_can_emit() {
        use crate::conformance::property::ConformanceProperty;

        let real: std::collections::BTreeSet<&str> = ConformanceProperty::ALL
            .iter()
            .map(|p| p.as_str())
            .collect();

        let src = include_str!("home_page.rs");
        let needle = "property: \"";
        let mut planted: Vec<&str> = Vec::new();
        let mut from = 0usize;
        while let Some(found) = src[from..].find(needle) {
            let start = from + found + needle.len();
            let end = start
                + src[start..]
                    .find('"')
                    .expect("an unterminated string literal in this file's own source");
            planted.push(&src[start..end]);
            from = end;
        }

        assert!(
            !planted.is_empty(),
            "no `property: \"…\"` fixture was found in this file, so this test has \
             stopped reading what it claims to read — the fixtures were probably \
             renamed or moved, and it is now vacuous"
        );
        for name in &planted {
            assert!(
                real.contains(name),
                "the merge-card fixtures plant `{name}`, which no \
                 ConformanceProperty produces. A card test built on a name the \
                 checker cannot emit asserts about a state no node will ever render. \
                 Real names: {real:?}"
            );
        }
    }

    /// The Playwright fixture's markup must stay in step with what the server
    /// actually emits.
    ///
    /// `dashboard-table-filter.spec.ts` builds its own filter controls and
    /// table, because BOTH cards that carry them short-circuit to an empty
    /// variant when they have no rows — and the Playwright harness node is a
    /// single isolated peer with neither peers nor subscribed contracts, so on
    /// CI the real controls are never on the page. That was found the hard
    /// way: an earlier version of the spec asserted the controls were
    /// unconditional and failed in CI.
    ///
    /// A hand-built fixture is only safe while it matches reality, so this
    /// test pins every hook the spec selects on. If you change the markup in
    /// `table_filter_controls`, this fails and tells you to change the spec
    /// too — which is the whole point, because the spec would otherwise keep
    /// passing against markup the server no longer produces.
    #[test]
    fn filter_fixture_markup_matches_table_filter_controls() {
        let mut snap = base_snapshot();
        snap.open_connections = 2;
        snap.peers = vec![sample_peer("10.0.0.1:31337", 0.25)];
        let html = build_peers_card(&Some(snap));

        // Every selector `dashboard-table-filter.spec.ts` depends on. Keep
        // this list and the spec's `fixtureCardHtml` in lockstep.
        for hook in [
            r#"class="table-filter""#,
            r#"data-filter-for="#,
            r#"class="tf-input""#,
            r#"type="search""#,
            r#"class="tf-status""#,
            r#"class="tf-toggle""#,
            r#"aria-expanded="false""#,
            r#"class="table-wrap""#,
            r#"class="sortable""#,
            r#"data-table-id="#,
            r#"data-sort-type="#,
        ] {
            assert!(
                html.contains(hook),
                "the Playwright fixture selects on `{hook}`, which the server \
                 no longer emits. Update `fixtureCardHtml` in \
                 crates/core/tests/playwright/tests/dashboard-table-filter.spec.ts \
                 to match, or the spec will pass against markup that does not \
                 exist — got:\n{html}"
            );
        }
    }

    /// Both long tables must carry filter controls wired to their own table.
    ///
    /// The peers table rendered 210 rows on a production gateway — 70% of an
    /// 11,780px page — with no way to locate a single row.
    #[test]
    fn long_tables_render_filter_controls_bound_to_their_table() {
        let mut snap = base_snapshot();
        snap.open_connections = 2;
        snap.peers = vec![sample_peer("10.0.0.1:31337", 0.25)];
        let peers_html = build_peers_card(&Some(snap));
        assert!(
            peers_html.contains(r#"data-filter-for="peers""#),
            "peers filter must target the peers table — got:\n{peers_html}"
        );
        assert!(
            peers_html.contains(r#"data-table-id="peers""#),
            "the targeted table id must exist on the page — got:\n{peers_html}"
        );

        let mut snap2 = base_snapshot();
        snap2.open_connections = 2;
        snap2.contracts = vec![crate::node::network_status::ContractSnapshot {
            key_short: "AAA1...".to_string(),
            key_full: "AAA123XYZ".to_string(),
            instance_id: "AAA123XYZ".to_string(),
            subscribed_secs: 100,
            last_updated_secs: Some(5),
            is_receiving_updates: true,
            in_use: true,
        }];
        let contracts_html = build_contracts_card(&Some(snap2));
        assert!(
            contracts_html.contains(r#"data-filter-for="contracts""#),
            "contracts filter must target the contracts table — got:\n{contracts_html}"
        );
    }

    /// The controls must be reachable without a mouse and announce changes.
    ///
    /// The status line updates as you type, so it needs a live region or a
    /// screen-reader user gets no feedback that the table changed under them.
    #[test]
    fn filter_controls_are_labelled_and_announced() {
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.peers = vec![sample_peer("10.0.0.1:31337", 0.25)];
        let html = build_peers_card(&Some(snap));
        assert!(
            html.contains(r#"aria-label="Filter peers""#),
            "the input needs an accessible name — got:\n{html}"
        );
        assert!(
            html.contains(r#"aria-live="polite""#),
            "the row-count status must be announced as it changes — got:\n{html}"
        );
        assert!(
            html.contains(r#"aria-expanded="false""#),
            "the collapse toggle must expose its state — got:\n{html}"
        );
    }

    /// The toggle ships hidden.
    ///
    /// It is the JS that decides whether there is anything to collapse; a
    /// toggle visible before that decision would flash on every refresh, and
    /// on a small node would offer to expand a table that is already whole.
    #[test]
    fn filter_toggle_starts_hidden_for_the_js_to_reveal() {
        let mut snap = base_snapshot();
        snap.open_connections = 1;
        snap.peers = vec![sample_peer("10.0.0.1:31337", 0.25)];
        let html = build_peers_card(&Some(snap));
        assert!(
            html.contains(r#"class="tf-toggle" hidden"#),
            "the toggle must start hidden — got:\n{html}"
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
