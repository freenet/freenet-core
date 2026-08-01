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

use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{ExporterBuildError, MetricExporter, WithExportConfig};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};

/// Instrumentation scope name for every instrument this crate registers.
const METER_NAME: &str = "freenet";

/// Start the OpenTelemetry SDK metrics pipeline and install it as the
/// process-global meter provider.
///
/// No-op when suppressed (see [`otel_suppression_reason`]) and best-effort
/// otherwise: an exporter that cannot be built logs a warning and the node
/// starts anyway. Metrics export must never be a startup dependency.
///
/// After this returns, instrumentation anywhere in the crate is just
/// `opentelemetry::global::meter("freenet")` — there is deliberately no wrapper
/// type or registry to keep in sync.
pub fn init(config: &OtelConfig, local_peer_id: String) {
    if let Some(reason) = otel_suppression_reason(
        config,
        cfg!(test),
        super::telemetry::running_under_cargo_test(),
    ) {
        tracing::debug!(?reason, "OTel metrics exporter not started");
        return;
    }

    let endpoint = resolve_metrics_endpoint(
        config.endpoint.as_deref(),
        std::env::var(opentelemetry_otlp::OTEL_EXPORTER_OTLP_METRICS_ENDPOINT)
            .ok()
            .as_deref(),
        std::env::var(opentelemetry_otlp::OTEL_EXPORTER_OTLP_ENDPOINT)
            .ok()
            .as_deref(),
    );

    match build_provider(endpoint.as_deref(), local_peer_id) {
        Ok(provider) => {
            // ponytail: no shutdown hook. `set_meter_provider` holds a
            // reference for the process lifetime and PeriodicReader exports
            // every 60s (OTEL_METRIC_EXPORT_INTERVAL), so at most one partial
            // interval is lost at exit. If that tail ever matters, keep the
            // provider in a OnceLock and call `shutdown()` from the graceful
            // shutdown path in `bin/freenet.rs`.
            global::set_meter_provider(provider);
            register_process_metrics();
            tracing::info!(
                endpoint = endpoint
                    .as_deref()
                    .unwrap_or("<resolved by OTEL_* env or SDK default>"),
                "OTel metrics exporter started"
            );
        }
        Err(error) => {
            tracing::warn!(
                %error,
                "OTel metrics exporter failed to start; node continues without metrics"
            );
        }
    }
}

/// Build the OTLP/HTTP exporter and meter provider.
///
/// `endpoint` is `None` when the standard env vars should win — see
/// [`resolve_metrics_endpoint`] for why calling `with_endpoint` at all would
/// override them.
pub(crate) fn build_provider(
    endpoint: Option<&str>,
    local_peer_id: String,
) -> Result<SdkMeterProvider, ExporterBuildError> {
    let mut builder = MetricExporter::builder().with_http();
    if let Some(endpoint) = endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    let exporter = builder.build()?;

    // `service.name` is overridden by OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES
    // when the operator sets them; the SDK reads those itself.
    let resource = Resource::builder()
        .with_service_name("freenet-peer")
        .with_attribute(KeyValue::new("peer.id", local_peer_id))
        .build();

    Ok(SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(resource)
        .build())
}

/// Register the instruments this crate owns.
///
/// Must run AFTER `global::set_meter_provider`: `global::meter` binds to
/// whatever provider is installed at call time.
fn register_process_metrics() {
    let meter = global::meter(METER_NAME);
    // The handle is dropped on purpose — the callback is registered into the
    // pipeline at `build()` and observed on every collection cycle regardless.
    let _rss = meter
        .u64_observable_gauge("freenet.process.memory.rss")
        .with_unit("By")
        .with_description("Resident set size of the freenet process")
        .with_callback(|observer| {
            if let Some(rss) = crate::node::resource_metrics::rss_bytes() {
                observer.observe(rss, &[]);
            }
        })
        .build();
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

    #[tokio::test]
    async fn provider_builds_inside_a_tokio_runtime() {
        // Two things under test. First, exporter construction must not panic
        // when it happens inside an async context — the OTLP HTTP exporter uses
        // reqwest's BLOCKING client because PeriodicReader exports from its own
        // thread. Second, an unreachable collector must not surface as a build
        // error: export failures are asynchronous and must never fail node
        // startup. Port 1 is chosen because nothing can be listening there.
        let provider = build_provider(
            Some("http://127.0.0.1:1/v1/metrics"),
            "peer-under-test".to_string(),
        )
        .expect("exporter build must succeed against an unreachable collector");
        provider.shutdown().expect("clean shutdown");
    }
}
