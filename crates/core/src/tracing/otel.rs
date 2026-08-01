//! Standards-configured OpenTelemetry SDK metrics pipeline.
//!
//! Strictly isolated from [`super::telemetry`]: nothing here reads
//! `TelemetryConfig`, and the endpoint never falls back to
//! `DEFAULT_TELEMETRY_ENDPOINT`. The two features are independent by design —
//! see `docs/design/otel-metrics-exporter.md`.

use crate::config::OtelConfig;

/// Why the OTel metrics exporter was not started.
///
/// Mirrors `telemetry::TelemetrySuppression` so both pipelines refuse to ship
/// data from a test process, but the decision is computed from `OtelConfig`
/// alone — the two flags never consult each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum OtelSuppression {
    /// Operator left `otel-telemetry-enabled` off (the default).
    Disabled,
    /// `--id` test environment (integration/CLI harness sets `is_test_environment`).
    TestEnvironmentFlag,
    /// A `cfg(test)` build or a binary running under a cargo test/bench harness.
    TestHarness,
}

/// Decide whether the metrics exporter should be suppressed.
///
/// Pure and side-effect free: callers pass `cfg!(test)` and the result of
/// `telemetry::running_under_cargo_test()` so this is testable for a
/// production release binary (must NOT suppress) from inside a test process,
/// which by construction trips both test signals.
///
/// Suppression is keyed only on signals a real release binary never matches,
/// and deliberately NOT on `cfg!(feature = "testing")` — that flag leaks onto
/// the shipped binary through Cargo feature unification with `fdev` and
/// silently disabled telemetry across the fleet once already (#4366, the
/// 0.2.81 blackout). See `telemetry::telemetry_suppression_reason`.
#[allow(dead_code)]
pub(crate) fn otel_suppression_reason(
    config: &OtelConfig,
    is_test_build: bool,
    running_under_cargo_test: bool,
) -> Option<OtelSuppression> {
    if !config.enabled {
        return Some(OtelSuppression::Disabled);
    }
    if config.is_test_environment {
        return Some(OtelSuppression::TestEnvironmentFlag);
    }
    if is_test_build || running_under_cargo_test {
        return Some(OtelSuppression::TestHarness);
    }
    None
}

/// Endpoint to hand to `MetricExporter`'s builder, or `None` to let the SDK
/// resolve it.
///
/// Required precedence is env > config file > SDK default, but
/// `opentelemetry-otlp` 0.32 inverts the first two: `resolve_http_endpoint`
/// (`src/exporter/http/mod.rs`) checks the programmatic value FIRST and only
/// then `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` / `OTEL_EXPORTER_OTLP_ENDPOINT`.
/// So whenever either variable is set we return `None` and stay out of the
/// way. It also appends the `/v1/metrics` signal path only on the env-var
/// path, so a config-file value gets the path appended here.
#[allow(dead_code)]
pub(crate) fn resolve_metrics_endpoint(
    cfg_endpoint: Option<&str>,
    metrics_env: Option<&str>,
    generic_env: Option<&str>,
) -> Option<String> {
    let is_set = |v: Option<&str>| v.is_some_and(|s| !s.trim().is_empty());
    if is_set(metrics_env) || is_set(generic_env) {
        return None;
    }
    let base = cfg_endpoint.map(str::trim).filter(|s| !s.is_empty())?;
    Some(format!("{}/v1/metrics", base.trim_end_matches('/')))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OtelConfig;

    fn enabled_config() -> OtelConfig {
        OtelConfig {
            enabled: true,
            endpoint: None,
            is_test_environment: false,
        }
    }

    #[test]
    fn production_shaped_input_is_not_suppressed() {
        assert_eq!(
            otel_suppression_reason(&enabled_config(), false, false),
            None,
            "a real release binary with the flag on must export"
        );
    }

    #[test]
    fn every_test_signal_suppresses() {
        let disabled = OtelConfig {
            enabled: false,
            ..enabled_config()
        };
        assert_eq!(
            otel_suppression_reason(&disabled, false, false),
            Some(OtelSuppression::Disabled)
        );

        let test_env = OtelConfig {
            is_test_environment: true,
            ..enabled_config()
        };
        assert_eq!(
            otel_suppression_reason(&test_env, false, false),
            Some(OtelSuppression::TestEnvironmentFlag)
        );

        assert_eq!(
            otel_suppression_reason(&enabled_config(), true, false),
            Some(OtelSuppression::TestHarness),
            "cfg(test) build"
        );
        assert_eq!(
            otel_suppression_reason(&enabled_config(), false, true),
            Some(OtelSuppression::TestHarness),
            "running from a cargo deps/ harness"
        );
    }

    #[test]
    fn metrics_env_wins_over_generic_env_and_config() {
        // Both env forms mean "let the SDK resolve it", because
        // opentelemetry-otlp gives a programmatic endpoint priority over the
        // env vars — passing one would invert the required precedence.
        assert_eq!(
            resolve_metrics_endpoint(
                Some("http://from-config:4318"),
                Some("http://from-metrics-env:4318/v1/metrics"),
                Some("http://from-generic-env:4318"),
            ),
            None
        );
        assert_eq!(
            resolve_metrics_endpoint(
                Some("http://from-config:4318"),
                None,
                Some("http://from-generic-env:4318"),
            ),
            None
        );
    }

    #[test]
    fn config_endpoint_gets_the_signal_path_appended() {
        // The SDK appends /v1/metrics only on the env-var path; a programmatic
        // endpoint is used verbatim, so we append it ourselves.
        assert_eq!(
            resolve_metrics_endpoint(Some("http://collector:4318"), None, None),
            Some("http://collector:4318/v1/metrics".to_string())
        );
        assert_eq!(
            resolve_metrics_endpoint(Some("http://collector:4318/"), None, None),
            Some("http://collector:4318/v1/metrics".to_string()),
            "trailing slash must not double up"
        );
    }

    #[test]
    fn nothing_configured_defers_to_the_sdk_default() {
        assert_eq!(resolve_metrics_endpoint(None, None, None), None);
        assert_eq!(
            resolve_metrics_endpoint(Some("   "), None, None),
            None,
            "a blank endpoint is not a configuration"
        );
    }
}
