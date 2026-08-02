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

use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{ExporterBuildError, MetricExporter, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    metrics::{Aggregation, Instrument, InstrumentKind, SdkMeterProvider, Stream},
};
use std::sync::OnceLock;

/// Instrumentation scope name for every instrument this crate registers.
const METER_NAME: &str = "freenet";

/// Start the OpenTelemetry SDK metrics pipeline and install it as the
/// process-global meter provider.
///
/// No-op when suppressed (see [`otel_suppression_reason`]) and best-effort
/// otherwise: an exporter that cannot be built logs a warning and the node
/// starts anyway. Metrics export must never be a startup dependency.
///
/// `instance_id` identifies THIS node and must not contain a network address:
/// see [`build_provider`].
pub fn init(config: &OtelConfig, instance_id: String) {
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

    match build_provider(endpoint.as_deref(), instance_id) {
        Ok(provider) => {
            // ponytail: no shutdown hook. `set_meter_provider` holds a
            // reference for the process lifetime and PeriodicReader exports
            // every 60s (OTEL_METRIC_EXPORT_INTERVAL), so at most one partial
            // interval is lost at exit. If that tail ever matters, keep the
            // provider in a OnceLock and call `shutdown()` from the graceful
            // shutdown path in `bin/freenet.rs`.
            global::set_meter_provider(provider);
            register_metrics();
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
///
/// `instance_id` MUST be the transport public key fingerprint, never a
/// `PeerId`: `PeerId`'s `Display` is `{pub_key}@{addr}`, so using it would put
/// this node's socket address in every exported batch AND make the identity
/// churn on every address change. The fingerprint is public by construction
/// (peers learn it on connect) and stable for the life of the keypair.
pub(crate) fn build_provider(
    endpoint: Option<&str>,
    instance_id: String,
) -> Result<SdkMeterProvider, ExporterBuildError> {
    let mut builder = MetricExporter::builder().with_http();
    if let Some(endpoint) = endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    let exporter = builder.build()?;

    // `service.name` is overridden by OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES
    // when the operator sets them; the SDK reads those itself.
    //
    // Resource attributes ride once per export batch, not per datapoint, so
    // identifying THIS node here costs nothing per series — unlike a
    // per-datapoint attribute, which is why no instrument below carries one
    // identifying the remote end of a connection.
    let resource = Resource::builder()
        .with_service_name("freenet-node")
        .with_attribute(KeyValue::new("service.instance.id", instance_id))
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .with_attribute(KeyValue::new("os.type", std::env::consts::OS))
        .with_attribute(KeyValue::new("host.arch", std::env::consts::ARCH))
        .build();

    Ok(SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(resource)
        // Every histogram this crate records is base-2 exponential rather than
        // explicit-bucket: the SDK's default boundaries are tuned for
        // millisecond latency and are useless for byte-scale instruments, and
        // exponential buckets self-adjust instead of needing a hand-picked
        // boundary set per instrument.
        .with_view(|instrument: &Instrument| {
            (instrument.kind() == InstrumentKind::Histogram)
                .then(|| {
                    Stream::builder()
                        .with_aggregation(Aggregation::Base2ExponentialHistogram {
                            max_size: 160,
                            max_scale: 20,
                            record_min_max: true,
                        })
                        .build()
                        .ok()
                })
                .flatten()
        })
        .build())
}

/// Synchronous instruments, recorded from the code paths they measure.
///
/// These need a handle held somewhere, unlike the observable instruments below
/// whose callbacks the pipeline owns. Kept behind a `OnceLock` set at the end
/// of [`init`] for two reasons: instruments built before
/// `global::set_meter_provider` would bind to the no-op provider forever, and
/// when the exporter is disabled the record helpers collapse to one relaxed
/// atomic load and a branch.
struct Instruments {
    rtt: Histogram<f64>,
    cwnd: Histogram<u64>,
    transfers: Counter<u64>,
    nat_traversal: Counter<u64>,
    operations: Counter<u64>,
}

static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();

/// Record a transport RTT sample. No-op until [`init`] installs the pipeline.
pub(crate) fn record_rtt_ms(rtt_ms: f64) {
    if let Some(i) = INSTRUMENTS.get() {
        i.rtt.record(rtt_ms, &[]);
    }
}

/// Record a congestion-window sample.
pub(crate) fn record_cwnd(cwnd_bytes: u64) {
    if let Some(i) = INSTRUMENTS.get() {
        i.cwnd.record(cwnd_bytes, &[]);
    }
}

/// Record a stream transfer outcome (`completed` / `failed`).
pub(crate) fn record_transfer(result: &'static str) {
    if let Some(i) = INSTRUMENTS.get() {
        i.transfers.add(1, &[KeyValue::new("result", result)]);
    }
}

/// Record a NAT traversal outcome (`attempt` / `established` /
/// `failed_error` / `failed_version`).
pub(crate) fn record_nat_traversal(result: &'static str) {
    if let Some(i) = INSTRUMENTS.get() {
        i.nat_traversal.add(1, &[KeyValue::new("result", result)]);
    }
}

/// Record an operation outcome. `op` is one of get/put/update/subscribe.
///
/// ponytail: outcome only, no duration histogram — no driver measures its own
/// elapsed time today, and adding one means threading `TimeSource` through
/// every `op_ctx_task` (raw `Instant::now()` is banned in this crate). Add the
/// histogram when someone needs operation latency percentiles.
pub(crate) fn record_op_result(op: &'static str, success: bool) {
    if let Some(i) = INSTRUMENTS.get() {
        i.operations.add(
            1,
            &[
                KeyValue::new("op", op),
                KeyValue::new("result", if success { "success" } else { "failure" }),
            ],
        );
    }
}

/// Register the instruments this crate owns.
///
/// Must run AFTER `global::set_meter_provider`: `global::meter` binds to
/// whatever provider is installed at call time.
///
/// Observable handles are dropped on purpose — the callback is registered into
/// the pipeline at `build()` and observed on every collection cycle regardless.
/// The SDK has no batch-callback API, so each one reads
/// [`network_status::otel_metrics_snapshot`] independently; that is why the
/// accessor is a cheap scalar read rather than the dashboard's `get_snapshot`.
fn register_metrics() {
    let meter = global::meter(METER_NAME);

    let registered = INSTRUMENTS.set(Instruments {
        rtt: meter
            .f64_histogram("freenet.transport.rtt")
            .with_unit("ms")
            .with_description("Round-trip time observed on transport connections")
            .build(),
        cwnd: meter
            .u64_histogram("freenet.transport.cwnd")
            .with_unit("By")
            .with_description("Congestion window samples")
            .build(),
        transfers: meter
            .u64_counter("freenet.transport.transfers")
            .with_description("Stream transfers by outcome")
            .build(),
        nat_traversal: meter
            .u64_counter("freenet.transport.nat_traversal")
            .with_description("Outbound NAT traversal attempts by outcome")
            .build(),
        operations: meter
            .u64_counter("freenet.operation.results")
            .with_description("Completed operations by type and outcome")
            .build(),
    });
    if registered.is_err() {
        // A second `init` would leave the sync instruments bound to the first
        // provider while the observable ones move to the new one — loud rather
        // than silently half-migrated.
        tracing::warn!("OTel instruments already registered; keeping the first set");
    }

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

    register_transport_metrics(&meter);
    register_ring_metrics(&meter);
    register_queue_metrics(&meter);
}

/// Wire-level counters, read from the cumulative (never-reset) transport
/// totals.
///
/// Deliberately NOT read from `TransportSnapshot`: those fields are period
/// accumulators that `take_snapshot` zeroes for the legacy telemetry worker, so
/// observing them as counters would report a non-monotonic series whenever
/// `telemetry-enabled` is also on.
fn register_transport_metrics(meter: &opentelemetry::metrics::Meter) {
    use crate::transport::TRANSPORT_METRICS;

    let _bytes = meter
        .u64_observable_counter("freenet.transport.bytes")
        .with_unit("By")
        .with_description(
            "Wire bytes. Sent is metered at the socket (includes keep-alives, ACKs and \
             NAT probes); received is metered post-authentication, so the two directions \
             are deliberately not symmetric.",
        )
        .with_callback(|observer| {
            observer.observe(
                TRANSPORT_METRICS.cumulative_bytes_sent(),
                &[KeyValue::new("direction", "sent")],
            );
            observer.observe(
                TRANSPORT_METRICS.cumulative_bytes_received(),
                &[KeyValue::new("direction", "received")],
            );
        })
        .build();

    let _packets = meter
        .u64_observable_counter("freenet.transport.packets")
        .with_description("UDP datagrams, metered at the same sites as freenet.transport.bytes")
        .with_callback(|observer| {
            let (sent, received) = TRANSPORT_METRICS.cumulative_packets();
            observer.observe(sent, &[KeyValue::new("direction", "sent")]);
            observer.observe(received, &[KeyValue::new("direction", "received")]);
        })
        .build();
}

/// Ring / topology state, mirroring the dashboard's connection-status tiles.
fn register_ring_metrics(meter: &opentelemetry::metrics::Meter) {
    use crate::node::network_status::otel_metrics_snapshot as snapshot;

    let _connections = meter
        .u64_observable_gauge("freenet.ring.connections")
        .with_description("Active ring connections")
        .with_callback(|observer| {
            if let Some(s) = snapshot() {
                observer.observe(s.ring.connection_count as u64, &[]);
            }
        })
        .build();

    let _hosted = meter
        .u64_observable_gauge("freenet.node.contracts.hosted")
        .with_description("Contracts currently hosted by this node")
        .with_callback(|observer| {
            if let Some(s) = snapshot() {
                observer.observe(s.ring.hosted_contracts as u64, &[]);
            }
        })
        .build();

    let _attempts = meter
        .u64_observable_counter("freenet.connect.attempts")
        .with_description("Connection attempts made since startup")
        .with_callback(|observer| {
            if let Some(s) = snapshot() {
                observer.observe(s.connection_attempts as u64, &[]);
            }
        })
        .build();

    let _lattice = meter
        .u64_observable_gauge("freenet.ring.lattice.neighbor")
        .with_description(
            "1 when this node holds its closest connected ring neighbor on that side. \
             Held does not mean tight — compare distances across nodes.",
        )
        .with_callback(|observer| {
            if let Some(s) = snapshot() {
                observer.observe(
                    s.ring.lattice_has_successor as u64,
                    &[KeyValue::new("position", "successor")],
                );
                observer.observe(
                    s.ring.lattice_has_predecessor as u64,
                    &[KeyValue::new("position", "predecessor")],
                );
            }
        })
        .build();

    let _distance = meter
        .f64_observable_gauge("freenet.ring.lattice.neighbor.distance")
        .with_description("Ring distance to each held lattice edge; absent when unheld")
        .with_callback(|observer| {
            if let Some(s) = snapshot() {
                if let Some(d) = s.ring.lattice_successor_distance {
                    observer.observe(d, &[KeyValue::new("position", "successor")]);
                }
                if let Some(d) = s.ring.lattice_predecessor_distance {
                    observer.observe(d, &[KeyValue::new("position", "predecessor")]);
                }
            }
        })
        .build();

    let _probes = meter
        .u64_observable_counter("freenet.ring.lattice.probes")
        .with_description(
            "Route-to-self probes fired, and lattice improvements observed. Counted \
             independently — an improvement lands some ticks after the probe that caused \
             it, so the ratio is a convergence gauge, not a success rate.",
        )
        .with_callback(|observer| {
            if let Some(s) = snapshot() {
                observer.observe(
                    s.ring.lattice_probes_issued,
                    &[KeyValue::new("result", "issued")],
                );
                observer.observe(
                    s.ring.lattice_probe_improvements,
                    &[KeyValue::new("result", "improvement")],
                );
            }
        })
        .build();

    let _updates = meter
        .u64_observable_counter("freenet.contract.updates")
        .with_description("Relayed UPDATEs by admission outcome")
        .with_callback(|observer| {
            if let Some(s) = snapshot() {
                observer.observe(
                    s.ring.updates_accepted,
                    &[KeyValue::new("result", "accepted")],
                );
                observer.observe(
                    s.ring.updates_rate_limited,
                    &[KeyValue::new("result", "rate_limited")],
                );
                observer.observe(
                    s.ring.updates_capacity_dropped,
                    &[KeyValue::new("result", "capacity_dropped")],
                );
            }
        })
        .build();
}

/// Executor fair-queue occupancy and admission outcomes.
fn register_queue_metrics(meter: &opentelemetry::metrics::Meter) {
    use crate::node::network_status::otel_metrics_snapshot as snapshot;

    let _depth = meter
        .u64_observable_gauge("freenet.contract.queue.depth")
        .with_description("Current fair-queue occupancy")
        .with_callback(|observer| {
            if let Some(s) = snapshot() {
                let q = &s.fair_queue;
                for (tier, depth) in [
                    ("total", q.depth_total),
                    ("client_local", q.depth_client_local),
                    ("network_relay", q.depth_network_relay),
                    ("background", q.depth_background),
                ] {
                    observer.observe(depth as u64, &[KeyValue::new("queue", tier)]);
                }
            }
        })
        .build();

    // A gauge, not a counter: `high_water` is a running maximum, and a
    // collector that saw it as a counter would read a plateau as "no traffic".
    // It exists because a burst between two 60s collections leaves no trace in
    // the instantaneous depth.
    let _high_water = meter
        .u64_observable_gauge("freenet.contract.queue.depth.high_water")
        .with_description("Highest fair-queue occupancy reached since startup")
        .with_callback(|observer| {
            if let Some(s) = snapshot() {
                observer.observe(s.fair_queue.high_water as u64, &[]);
            }
        })
        .build();

    let _rejected = meter
        .u64_observable_counter("freenet.contract.queue.rejected")
        .with_description(
            "Fair-queue admission rejections. global_capacity is node-wide saturation; \
             per_contract is one noisy contract hitting its own cap.",
        )
        .with_callback(|observer| {
            if let Some(s) = snapshot() {
                observer.observe(
                    s.fair_queue.rejected_global_capacity,
                    &[KeyValue::new("reason", "global_capacity")],
                );
                observer.observe(
                    s.fair_queue.rejected_per_contract,
                    &[KeyValue::new("reason", "per_contract")],
                );
            }
        })
        .build();

    let _shed = meter
        .u64_observable_counter("freenet.contract.queue.background_shed")
        .with_description("Background events shed to make room for higher-priority work")
        .with_callback(|observer| {
            if let Some(s) = snapshot() {
                observer.observe(s.fair_queue.background_shed, &[]);
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

    #[test]
    fn instance_id_carries_no_network_address() {
        // The exporter identifies this node by its transport public key
        // fingerprint. `PeerId` renders as `{pub_key}@{addr}`, so using it — as
        // the exporter originally did — leaks our socket address into every
        // batch and re-identifies the node whenever the address changes.
        let keypair = crate::transport::TransportKeypair::new();
        let instance_id = keypair.public().to_string();

        assert!(!instance_id.is_empty());
        assert!(
            !instance_id.contains('@') && !instance_id.contains(':'),
            "instance id must not embed an address, got {instance_id}"
        );

        let peer_id = crate::node::PeerId::new(
            keypair.public().clone(),
            "203.0.113.7:31337".parse().expect("valid addr"),
        );
        assert!(
            peer_id.to_string().contains("203.0.113.7"),
            "guard is meaningless if PeerId stops embedding the address"
        );
    }

    #[test]
    fn record_helpers_are_inert_without_a_pipeline() {
        // Every record helper is called from production paths that run whether
        // or not the exporter is enabled, so an unset OnceLock must be a no-op
        // rather than a panic or an implicit no-op-provider binding.
        record_rtt_ms(12.5);
        record_cwnd(4096);
        record_transfer("completed");
        record_nat_traversal("attempt");
        record_op_result("get", true);
        assert!(
            INSTRUMENTS.get().is_none(),
            "recording must not lazily bind instruments to the no-op provider"
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
