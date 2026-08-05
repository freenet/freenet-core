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
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};
use opentelemetry_otlp::{ExporterBuildError, MetricExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    Resource,
    metrics::{Aggregation, Instrument, InstrumentKind, SdkMeterProvider, Stream},
};
use std::sync::OnceLock;

/// Build one `freenet`-mode bearer token:
/// `freenet/<pubkey>/<timestamp>/<signature>`, where `<signature>` is
/// the XEdDSA signature over `freenet/<pubkey>/<timestamp>`.
///
/// `<pubkey>` is the base58 full x25519 transport public key — the node's one
/// real identity, the same key peers see and whose truncated fingerprint UIs
/// display. `<timestamp>` is seconds since the Unix epoch, `<signature>` is
/// base58 too.
/// Freshly built per export request so the timestamp stays current.
///
/// Collector-side verification needs no exotic library: convert the
/// Montgomery pubkey to Edwards (sign bit 0), then standard Ed25519 verify —
/// see `node_pubkey_is_verifiable_with_stock_ed25519` below.
pub(crate) fn bearer_token(signer: &xeddsa::xed25519::PrivateKey, pubkey_b58: &str) -> String {
    use xeddsa::xeddsa::Sign;
    // Wall-clock epoch seconds on purpose: the collector checks it against
    // ITS clock, so simulation time would be meaningless here.
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let signed_payload = format!("freenet/{pubkey_b58}/{timestamp}");
    // OS entropy (SysRng), not GlobalRng: XEdDSA's Z randomness hedges the
    // signature nonce, which is cryptographic material — the same exception
    // documented in .claude/rules/code-style.md for keys/nonces. UnwrapErr is
    // required because xeddsa's bound is the infallible rand 0.10 CryptoRng.
    let signature: [u8; 64] = signer.sign(
        signed_payload.as_bytes(),
        rand_core10::UnwrapErr(rand10::rngs::SysRng),
    );
    let signature = bs58::encode(signature).into_string();
    format!("{signed_payload}/{signature}")
}

/// OTLP HTTP client that injects a fresh `Authorization: Bearer` token
/// (see [`bearer_token`]) into every export request, delegating the actual
/// send to the same blocking reqwest client the exporter would use anyway.
struct FreenetAuthClient {
    inner: reqwest::blocking::Client,
    signer: xeddsa::xed25519::PrivateKey,
    pubkey_b58: String,
}

// Manual impl: never print the signing key.
impl std::fmt::Debug for FreenetAuthClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FreenetAuthClient").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl HttpClient for FreenetAuthClient {
    async fn send_bytes(&self, mut request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let token = bearer_token(&self.signer, &self.pubkey_b58);
        request.headers_mut().insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_str(&format!("Bearer {token}"))?,
        );
        // Hand-rolled send, mirroring opentelemetry-http's blocking impl:
        // that impl is on reqwest 0.13's client (opentelemetry-http's own
        // dep), while the workspace is on 0.12, so we can't delegate to it.
        // Blocking inside async is fine here for the same reason the SDK's
        // default client is blocking: PeriodicReader exports via block_on on
        // a dedicated thread. Fold both in when the workspace moves to 0.13.
        let request: reqwest::blocking::Request = request.map(|body| body.to_vec()).try_into()?;
        let mut response = self.inner.execute(request)?.error_for_status()?;
        let headers = std::mem::take(response.headers_mut());
        let mut http_response = Response::builder()
            .status(response.status())
            .body(response.bytes()?)?;
        *http_response.headers_mut() = headers;
        Ok(http_response)
    }
}

/// Instrumentation scope name for every instrument this crate registers.
const METER_NAME: &str = "freenet";

/// Start the OpenTelemetry SDK metrics pipeline and install it as the
/// process-global meter provider.
///
/// No-op when suppressed (see [`otel_suppression_reason`]) and best-effort
/// otherwise: an exporter that cannot be built logs a warning and the node
/// starts anyway. Metrics export must never be a startup dependency.
///
/// `keypair` is the node's transport keypair: it yields the
/// `freenet.node.pubkey` / `freenet.node.fingerprint` resource attributes
/// (see [`build_provider`]) and, when `otel-auth-mode = "freenet"`, its
/// derived signing key authenticates every export request (see
/// [`bearer_token`]).
pub fn init(config: &OtelConfig, keypair: &crate::transport::TransportKeypair) {
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

    // `service.instance.id` IS the auth identity: the same base58 ed25519
    // verifying key the bearer token carries as `<pubkey>`, so the collector
    // self-validates the node id against the signing key by string equality
    // after verifying the signature. Derived from the keypair even when auth
    // is disabled, so the id is stable across auth-mode changes.
    let pubkey = bs58::encode(keypair.public_key_bytes()).into_string();
    let fingerprint = keypair.public().to_string();
    let auth_signer = match config.auth_mode {
        crate::config::OtelAuthMode::Freenet => Some(keypair.auth_token_signer()),
        crate::config::OtelAuthMode::Disabled => None,
    };

    match build_provider(endpoint.as_deref(), pubkey, fingerprint, auth_signer) {
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
/// Two identity resource attributes, both computed by [`init`] from the one
/// transport keypair:
///
/// - `freenet.node.pubkey` — the base58 full x25519 transport public key,
///   byte-equal to the bearer token's `<pubkey>` field. The collector
///   verifies the token's XEdDSA signature against this key, so the identity
///   is self-validating and unforgeable.
/// - `freenet.node.fingerprint` — base58 of the FIRST 12 BYTES of the same
///   key (`TransportPublicKey::Display`, what UIs show). A pure public
///   function of `pubkey`, so the collector recomputes and checks it rather
///   than trusting it.
///
/// Neither may ever be a `PeerId`: its `Display` is `{pub_key}@{addr}`, so
/// using it would put this node's socket address in every exported batch AND
/// make the identity churn on every address change.
pub(crate) fn build_provider(
    endpoint: Option<&str>,
    pubkey: String,
    fingerprint: String,
    auth_signer: Option<xeddsa::xed25519::PrivateKey>,
) -> Result<SdkMeterProvider, ExporterBuildError> {
    // The blocking reqwest clients below (ours and the exporter's default)
    // each own a private tokio runtime. Creating one — or dropping one on the
    // error path — inside an async context panics with "Cannot drop a runtime
    // in a context where blocking is not allowed", and `init` runs inside the
    // node's async build path. Hop to a plain thread so the whole build is
    // async-context-free regardless of the caller.
    std::thread::scope(|scope| {
        scope
            .spawn(move || build_provider_blocking(endpoint, pubkey, fingerprint, auth_signer))
            .join()
            .expect("otel provider build thread panicked")
    })
}

fn build_provider_blocking(
    endpoint: Option<&str>,
    pubkey: String,
    fingerprint: String,
    auth_signer: Option<xeddsa::xed25519::PrivateKey>,
) -> Result<SdkMeterProvider, ExporterBuildError> {
    let mut builder = MetricExporter::builder().with_http();
    if let Some(endpoint) = endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    if let Some(signer) = auth_signer {
        // Same blocking client the exporter defaults to (PeriodicReader
        // exports off-runtime — see Cargo.toml), wrapped to sign each request.
        builder = builder.with_http_client(FreenetAuthClient {
            inner: reqwest::blocking::Client::new(),
            signer,
            pubkey_b58: pubkey.clone(),
        });
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
        .with_attribute(KeyValue::new("freenet.node.pubkey", pubkey))
        .with_attribute(KeyValue::new("freenet.node.fingerprint", fingerprint))
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
            auth_mode: Default::default(),
            is_test_environment: false,
        }
    }

    /// One keypair plus its token, pre-split, for the verification tests.
    fn token_fixture() -> (crate::transport::TransportKeypair, String) {
        let keypair = crate::transport::TransportKeypair::new();
        let pubkey_b58 = bs58::encode(keypair.public_key_bytes()).into_string();
        let token = bearer_token(&keypair.auth_token_signer(), &pubkey_b58);
        (keypair, token)
    }

    #[test]
    fn bearer_token_has_the_documented_shape_and_verifies() {
        use xeddsa::xeddsa::Verify;

        let (keypair, token) = token_fixture();

        let parts: Vec<&str> = token.split('/').collect();
        let [scheme, pubkey, timestamp, signature] = parts[..] else {
            panic!("expected 4 /-separated parts, got {token}");
        };
        assert_eq!(scheme, "freenet");
        assert_eq!(
            pubkey,
            bs58::encode(keypair.public_key_bytes()).into_string(),
            "pubkey part must be the full base58 x25519 transport public key"
        );
        let ts: u64 = timestamp.parse().expect("timestamp is epoch seconds");
        assert!(
            ts > 1_700_000_000,
            "timestamp must be current epoch seconds"
        );
        // The signature covers everything before its own slash, and verifies
        // against the token's OWN pubkey — the transport key itself.
        let signed_payload = format!("freenet/{pubkey}/{timestamp}");
        let sig_bytes: [u8; 64] = bs58::decode(signature)
            .into_vec()
            .unwrap()
            .try_into()
            .expect("64-byte signature");
        xeddsa::xed25519::PublicKey(keypair.public_key_bytes())
            .verify(signed_payload.as_bytes(), &sig_bytes)
            .expect("XEdDSA signature must verify against the transport pubkey");

        // A forged payload with the same signature must fail.
        assert!(
            xeddsa::xed25519::PublicKey(keypair.public_key_bytes())
                .verify(b"freenet/forged", &sig_bytes)
                .is_err()
        );
    }

    #[test]
    fn node_pubkey_is_verifiable_with_stock_ed25519() {
        // The collector-side contract, spelled out: no xeddsa dependency
        // needed there. Convert the Montgomery (x25519) pubkey to an Edwards
        // point with sign bit 0, then run ordinary Ed25519 verification.
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};

        let (keypair, token) = token_fixture();
        let (payload, sig_b58) = token.rsplit_once('/').unwrap();
        let sig_bytes: [u8; 64] = bs58::decode(sig_b58)
            .into_vec()
            .unwrap()
            .try_into()
            .unwrap();

        let edwards = curve25519_dalek::montgomery::MontgomeryPoint(keypair.public_key_bytes())
            .to_edwards(0)
            .expect("transport pubkey must map to Edwards")
            .compress()
            .to_bytes();
        VerifyingKey::from_bytes(&edwards)
            .unwrap()
            .verify(payload.as_bytes(), &Signature::from_bytes(&sig_bytes))
            .expect("stock ed25519 verify after Montgomery->Edwards conversion");
    }

    #[test]
    fn fingerprint_attr_is_recomputable_from_the_pubkey_attr() {
        // Requirement: a node cannot fake the UI-facing fingerprint. The
        // collector derives it from the verified pubkey instead of trusting
        // it: b58-decode pubkey, take the first 12 bytes, b58-encode.
        let keypair = crate::transport::TransportKeypair::new();
        let pubkey_attr = bs58::encode(keypair.public_key_bytes()).into_string();
        let fingerprint_attr = keypair.public().to_string();

        let decoded = bs58::decode(&pubkey_attr).into_vec().unwrap();
        assert_eq!(
            bs58::encode(&decoded[..12]).into_string(),
            fingerprint_attr,
            "fingerprint must be a pure function of pubkey, or the collector \
             cannot validate the UI-facing id"
        );
    }

    #[test]
    fn bearer_tokens_are_unique_per_request() {
        // XEdDSA's random Z makes each signature distinct even over an
        // identical payload (same pubkey, same second).
        let keypair = crate::transport::TransportKeypair::new();
        let pubkey = bs58::encode(keypair.public_key_bytes()).into_string();
        let signer = keypair.auth_token_signer();
        assert_ne!(
            bearer_token(&signer, &pubkey),
            bearer_token(&signer, &pubkey)
        );
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
    fn node_pubkey_attr_matches_the_bearer_token_pubkey() {
        // The collector's self-validation contract: after verifying the token
        // signature, `<pubkey>` must equal `freenet.node.pubkey` exactly.
        let (keypair, token) = token_fixture();
        let pubkey_attr = bs58::encode(keypair.public_key_bytes()).into_string();
        assert_eq!(
            token.split('/').nth(1),
            Some(pubkey_attr.as_str()),
            "token <pubkey> must equal freenet.node.pubkey, or the collector \
             cannot self-validate the node id against the signing key"
        );
    }

    #[test]
    fn instance_id_carries_no_network_address() {
        // `PeerId` renders as `{pub_key}@{addr}`, so using it — as the
        // exporter originally did — leaks our socket address into every
        // batch and re-identifies the node whenever the address changes.
        // Both identity attributes must stay address-free.
        let keypair = crate::transport::TransportKeypair::new();
        for instance_id in [
            bs58::encode(keypair.public_key_bytes()).into_string(),
            keypair.public().to_string(),
        ] {
            assert!(!instance_id.is_empty());
            assert!(
                !instance_id.contains('@') && !instance_id.contains(':'),
                "identity attribute must not embed an address, got {instance_id}"
            );
        }

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
        // when invoked from an async context — the blocking reqwest client
        // owns a private tokio runtime, so `build_provider` hops to a plain
        // thread internally; this asserts that hop works (on Linux, building
        // inline panics with "Cannot drop a runtime in a context where
        // blocking is not allowed"). Second, an unreachable collector must not
        // surface as a build error: export failures are asynchronous and must
        // never fail node startup. Port 1 is chosen because nothing can be
        // listening there.
        let provider = build_provider(
            Some("http://127.0.0.1:1/v1/metrics"),
            "pubkey-under-test".to_string(),
            "fingerprint-under-test".to_string(),
            // Auth on: the signing client path must not panic in async
            // context either.
            Some(crate::transport::TransportKeypair::new().auth_token_signer()),
        )
        .expect("exporter build must succeed against an unreachable collector");
        // Shutdown drops the exporter's blocking client and with it that
        // private runtime — same hazard as construction, so it must happen
        // where blocking is allowed, not on this async test thread.
        tokio::task::spawn_blocking(move || provider.shutdown().expect("clean shutdown"))
            .await
            .expect("shutdown thread panicked");
    }
}
