//! Standards-configured OpenTelemetry SDK metrics pipeline.
//!
//! Strictly isolated from [`super::telemetry`]: nothing here reads
//! `TelemetryConfig`, and the endpoint never falls back to
//! `DEFAULT_TELEMETRY_ENDPOINT`. The two features are independent by design —
//! see `docs/design/otel-metrics-exporter.md`.

use std::sync::OnceLock;

use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{KeyValue, global};
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};
use opentelemetry_otlp::{ExporterBuildError, MetricExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    Resource,
    metrics::{Aggregation, Instrument, InstrumentKind, SdkMeterProvider, Stream},
};

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

/// Build one `freenet`-mode bearer token:
/// `freenet/<pubkey>/<audience>/<timestamp>/<signature>`, where `<signature>`
/// is the XEdDSA signature over everything preceding it.
///
/// `<pubkey>` is the base58 full x25519 transport public key — the node's one
/// real identity, the same key peers see and whose truncated fingerprint UIs
/// display. `<audience>` names the collector the request is going to (see
/// [`audience_of`]), so a token is only valid there: without it, any collector
/// we export to could replay the token to any other collector accepting this
/// scheme and impersonate this node. `<timestamp>` is seconds since the Unix
/// epoch, `<signature>` is base58 too.
/// Freshly built per export request so the timestamp stays current.
///
/// Collector-side verification needs no exotic library: convert the
/// Montgomery pubkey to Edwards (sign bit 0), then standard Ed25519 verify —
/// see `node_pubkey_is_verifiable_with_stock_ed25519` below. The collector
/// must additionally check `<audience>` against the hash of each URL it
/// answers at (see [`audience_of`]) and `<timestamp>` against its own clock.
pub(crate) fn bearer_token(
    signer: &xeddsa::xed25519::PrivateKey,
    pubkey_b58: &str,
    audience: &str,
) -> String {
    use xeddsa::xeddsa::Sign;
    // Wall-clock epoch seconds on purpose: the collector checks it against
    // ITS clock, so simulation time would be meaningless here.
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let signed_payload = format!("freenet/{pubkey_b58}/{audience}/{timestamp}");
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

/// The `<audience>` field of a bearer token: base58 of the first 16 bytes of
/// `SHA-256(canonical target URL)`.
///
/// A hash rather than the URL itself for two reasons: a URL contains `/`,
/// which is the token's field separator, and the full URL is longer than the
/// binding needs to be. 16 bytes is 128 bits — an attacker looking to reuse a
/// token elsewhere needs a *second meaningful collector URL* colliding with
/// the first, which this is far past sufficient for.
///
/// The collector recomputes this from the URL(s) it expects to be reached at
/// and compares, so both sides must canonicalize identically. The rules,
/// exactly:
///
/// - `{scheme}://{host}:{port}{path}`, e.g.
///   `http://collector.example:4318/v1/metrics`.
/// - scheme and host lowercased (both are case-insensitive).
/// - port always explicit, defaulting to 80 for `http` and 443 for `https`,
///   so `https://c.example/x` and `https://c.example:443/x` agree.
/// - path verbatim — no trailing-slash or dot-segment normalization.
/// - **userinfo stripped**, and query/fragment dropped (an OTLP export URL has
///   neither). Stripping userinfo is not cosmetic: hashing an endpoint of
///   `https://user:secret@collector/` would make the value unreproducible for
///   a collector that does not know the password, and it keeps credentials out
///   of the signed input entirely.
///
/// Cost of hashing: a rejected token tells the collector nothing about what
/// the sender aimed at. The node logs its resolved endpoint at startup, and
/// `docs/otel-metrics.md` documents this computation so a mismatch can be
/// worked out by hand.
fn audience_of(uri: &http::Uri) -> String {
    use sha2::{Digest, Sha256};

    let Some(host) = uri.host() else {
        return String::new();
    };
    let scheme = uri.scheme_str().unwrap_or("http").to_ascii_lowercase();
    let port = uri.port_u16().unwrap_or(match scheme.as_str() {
        "https" => 443,
        _ => 80,
    });
    let canonical = format!(
        "{scheme}://{}:{port}{}",
        host.to_ascii_lowercase(),
        uri.path()
    );
    bs58::encode(&Sha256::digest(canonical.as_bytes())[..16]).into_string()
}

/// The exporter's only HTTP transport, in every auth mode.
///
/// Always installed, so `opentelemetry-otlp` never builds a client of its own
/// and none of its `reqwest-*` features have to be enabled — see the comment
/// on the dependency in `crates/core/Cargo.toml` for the dependency-graph
/// reason that matters.
///
/// With `signer` set (`otel-auth-mode = "freenet"`) it adds a fresh
/// `Authorization: Bearer` token (see [`bearer_token`]) to each request;
/// with it unset it is a plain sender.
struct OtlpHttpClient {
    inner: reqwest::blocking::Client,
    /// `None` in `disabled` auth mode.
    signer: Option<xeddsa::xed25519::PrivateKey>,
    pubkey_b58: String,
}

// Manual impl: never print the signing key.
impl std::fmt::Debug for OtlpHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtlpHttpClient").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl HttpClient for OtlpHttpClient {
    async fn send_bytes(&self, mut request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        // Never clobber an operator-supplied header: the exporter applies
        // OTEL_EXPORTER_OTLP_HEADERS before calling us, so a hosted collector
        // configured with `Authorization: Basic ...` there must win. Our
        // bearer token is one auth scheme among several, not the only one.
        if let Some(signer) = self.signer.as_ref() {
            if !request.headers().contains_key(http::header::AUTHORIZATION) {
                let token = bearer_token(signer, &self.pubkey_b58, &audience_of(request.uri()));
                request.headers_mut().insert(
                    http::header::AUTHORIZATION,
                    http::HeaderValue::from_str(&format!("Bearer {token}"))?,
                );
            }
        }
        // Hand-rolled send, mirroring opentelemetry-http's blocking impl:
        // that impl is on reqwest 0.13's client (opentelemetry-http's own
        // dep), while the workspace is on 0.12, so we can't delegate to it.
        // Blocking inside async is fine here for the same reason the SDK's
        // default client is blocking: PeriodicReader exports via block_on on
        // a dedicated thread. Fold both in when the workspace moves to 0.13.
        let request: reqwest::blocking::Request = request.map(|body| body.to_vec()).try_into()?;
        // Deliberately no `error_for_status()`: it discards the body, which is
        // where OTLP puts rejection detail. Hand the whole response back and
        // the SDK turns a non-2xx into an export error itself, logging the
        // status, the URL and the body (`HttpClient.StatusError`).
        let mut response = self.inner.execute(request)?;
        let status = response.status();
        let headers = std::mem::take(response.headers_mut());
        let mut http_response = Response::builder().status(status).body(response.bytes()?)?;
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
///
/// Returns the reason it did NOT start, or `None` when the pipeline is
/// installed. Callers ignore it; it exists so a test can assert that `init`
/// consults [`otel_suppression_reason`] and returns before building anything —
/// deleting the check would make an enabled config return `None` under
/// `cfg(test)`, i.e. ship a test network's metrics to a collector.
pub(crate) fn init(
    config: &OtelConfig,
    keypair: &crate::transport::TransportKeypair,
) -> Option<OtelSuppression> {
    if let Some(reason) = otel_suppression_reason(
        config,
        cfg!(test),
        super::telemetry::running_under_cargo_test(),
    ) {
        tracing::debug!(?reason, "OTel metrics exporter not started");
        return Some(reason);
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

    let (pubkey, fingerprint) = identity_attributes(keypair);
    let auth_signer = match config.auth_mode {
        crate::config::OtelAuthMode::Freenet => Some(keypair.auth_token_signer()),
        crate::config::OtelAuthMode::Disabled => None,
    };

    match build_provider(endpoint.as_deref(), pubkey, fingerprint, auth_signer) {
        Ok(provider) => {
            // NOTE: no shutdown hook. `set_meter_provider` holds a
            // reference for the process lifetime and PeriodicReader exports
            // every 60s (OTEL_METRIC_EXPORT_INTERVAL), so at most one partial
            // interval is lost at exit. If that tail ever matters, keep the
            // provider in a OnceLock and call `shutdown()` from the graceful
            // shutdown path in `bin/freenet.rs`.
            global::set_meter_provider(provider);
            register_metrics();
            // Log where this node's signed identity is actually going,
            // including when an inherited OTEL_* variable overrode the
            // configured endpoint — an operator who cannot see that from the
            // logs cannot tell their collector was bypassed.
            let env_endpoint =
                std::env::var(opentelemetry_otlp::OTEL_EXPORTER_OTLP_METRICS_ENDPOINT)
                    .or_else(|_| std::env::var(opentelemetry_otlp::OTEL_EXPORTER_OTLP_ENDPOINT))
                    .ok();
            if let (Some(env_endpoint), Some(cfg_endpoint)) =
                (env_endpoint.as_deref(), config.endpoint.as_deref())
            {
                tracing::warn!(
                    %env_endpoint,
                    %cfg_endpoint,
                    "OTEL_EXPORTER_OTLP_* overrides the configured otel-endpoint"
                );
            }
            tracing::info!(
                endpoint = endpoint
                    .as_deref()
                    .or(env_endpoint.as_deref())
                    .unwrap_or("http://localhost:4318 (SDK default)"),
                auth_mode = ?config.auth_mode,
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
    None
}

/// The two resource attributes that identify THIS node, both derived from the
/// one transport keypair: `(freenet.node.pubkey, freenet.node.fingerprint)`.
///
/// A function rather than two inline expressions in [`init`] so the guards in
/// this module's tests assert on what production actually attaches — building
/// the same strings in a test body would pass no matter what `init` does.
fn identity_attributes(keypair: &crate::transport::TransportKeypair) -> (String, String) {
    // The full base58 x25519 transport public key. Byte-equal to the bearer
    // token's `<pubkey>` field, so the collector self-validates the node id
    // against the signing key after verifying the signature. Derived from the
    // keypair even when auth is disabled, so the id is stable across auth-mode
    // changes. NEVER a `PeerId`: its Display is `{pub_key}@{addr}`.
    (
        bs58::encode(keypair.public_key_bytes()).into_string(),
        keypair.public().to_string(),
    )
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
            // A panic in the builder must surface as a build error, not
            // propagate: `init` runs on node startup and metrics export must
            // never be a startup dependency.
            .unwrap_or_else(|panic| {
                let msg = panic
                    .downcast_ref::<&str>()
                    .map(ToString::to_string)
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "non-string panic payload".to_owned());
                Err(ExporterBuildError::InternalFailure(format!(
                    "otel provider build thread panicked: {msg}"
                )))
            })
    })
}

/// Per-export HTTP timeout, resolved exactly like `opentelemetry-otlp` would
/// (`OTEL_EXPORTER_OTLP_METRICS_TIMEOUT` > `OTEL_EXPORTER_OTLP_TIMEOUT` >
/// 10s, in milliseconds). The SDK applies its own resolution only to a client
/// it builds itself, and we always supply one, so it has to happen here.
fn export_timeout() -> std::time::Duration {
    [
        opentelemetry_otlp::OTEL_EXPORTER_OTLP_METRICS_TIMEOUT,
        opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT,
    ]
    .iter()
    .find_map(|var| std::env::var(var).ok()?.trim().parse::<u64>().ok())
    .map(std::time::Duration::from_millis)
    .unwrap_or(opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT)
}

/// Resource attribute keys the operator declared through the environment,
/// which must not be overwritten — see the merge note in
/// [`build_provider_blocking`].
fn env_declared_resource_keys() -> Vec<String> {
    let mut keys = Vec::new();
    if std::env::var("OTEL_SERVICE_NAME").is_ok_and(|v| !v.trim().is_empty()) {
        keys.push("service.name".to_owned());
    }
    if let Ok(attrs) = std::env::var("OTEL_RESOURCE_ATTRIBUTES") {
        keys.extend(attrs.split(',').filter_map(|pair| {
            let key = pair.split('=').next()?.trim();
            (!key.is_empty()).then(|| key.to_owned())
        }));
    }
    keys
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
    // Always ours, in every auth mode — see `OtlpHttpClient`. Blocking on
    // purpose: PeriodicReader exports off-runtime (see Cargo.toml). The
    // timeout is the one the SDK would have applied to its own client, which
    // it does not apply to a supplied one.
    builder = builder.with_http_client(OtlpHttpClient {
        inner: reqwest::blocking::Client::builder()
            .timeout(export_timeout())
            .build()
            .unwrap_or_else(|_| reqwest::blocking::Client::new()),
        signer: auth_signer,
        pubkey_b58: pubkey.clone(),
    });
    let exporter = builder.build()?;

    // Resource attributes ride once per export batch, not per datapoint, so
    // identifying THIS node here costs nothing per series — unlike a
    // per-datapoint attribute, which is why no instrument below carries one
    // identifying the remote end of a connection.
    //
    // `Resource::builder` seeds from OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES
    // and then `with_attribute`/`with_service_name` MERGE OVER that seed, so
    // setting a literal unconditionally silently discards the operator's
    // value: two nodes on one host with distinct OTEL_SERVICE_NAMEs would both
    // export `service.name=freenet-node`. Only fill in what the environment
    // did not declare.
    let declared = env_declared_resource_keys();
    let mut resource = Resource::builder();
    if !declared.iter().any(|k| k == "service.name") {
        resource = resource.with_service_name("freenet-node");
    }
    for (key, value) in [
        ("freenet.node.pubkey", pubkey),
        ("freenet.node.fingerprint", fingerprint),
        ("service.version", env!("CARGO_PKG_VERSION").to_owned()),
        ("os.type", std::env::consts::OS.to_owned()),
        ("host.arch", std::env::consts::ARCH.to_owned()),
    ] {
        if !declared.iter().any(|k| k == key) {
            resource = resource.with_attribute(KeyValue::new(key, value));
        }
    }
    let resource = resource.build();

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
/// NOTE: outcome only, no duration histogram — no driver measures its own
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
        .with_description(
            "Current fair-queue occupancy, per tier. No `total` series: it would \
             double-count under `sum by (queue)` — sum the tiers instead.",
        )
        .with_callback(|observer| {
            if let Some(s) = snapshot() {
                let q = &s.fair_queue;
                for (tier, depth) in [
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

    /// The collector this module's tests mint tokens for, as the audience
    /// hash of `http://collector.example:4318/v1/metrics`.
    fn test_audience() -> String {
        audience_of(
            &"http://collector.example:4318/v1/metrics"
                .parse::<http::Uri>()
                .unwrap(),
        )
    }

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
        let token = bearer_token(&keypair.auth_token_signer(), &pubkey_b58, &test_audience());
        (keypair, token)
    }

    #[test]
    fn bearer_token_has_the_documented_shape_and_verifies() {
        use xeddsa::xeddsa::Verify;

        let (keypair, token) = token_fixture();

        let parts: Vec<&str> = token.split('/').collect();
        let [scheme, pubkey, audience, timestamp, signature] = parts[..] else {
            panic!("expected 5 /-separated parts, got {token}");
        };
        assert_eq!(scheme, "freenet");
        assert_eq!(
            pubkey,
            bs58::encode(keypair.public_key_bytes()).into_string(),
            "pubkey part must be the full base58 x25519 transport public key"
        );
        assert_eq!(
            audience,
            test_audience(),
            "the token must name the collector it was minted for, or it can be \
             replayed to any other collector accepting this scheme"
        );
        let ts: u64 = timestamp.parse().expect("timestamp is epoch seconds");
        assert!(
            ts > 1_700_000_000,
            "timestamp must be current epoch seconds"
        );
        // The signature covers everything before its own slash, and verifies
        // against the token's OWN pubkey — the transport key itself.
        let signed_payload = format!("freenet/{pubkey}/{audience}/{timestamp}");
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

    /// One-shot collector on a real socket: returns its address and a handle
    /// yielding the raw request text it received.
    fn oneshot_collector() -> (std::net::SocketAddr, std::thread::JoinHandle<String>) {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut raw = Vec::new();
            let mut buf = [0u8; 1024];
            // Read until the body payload arrives; the client writes the
            // whole request before waiting on the response.
            while !raw.windows(14).any(|w| w == b"export-payload") {
                let n = stream.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&buf[..n]);
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .unwrap();
            String::from_utf8_lossy(&raw).into_owned()
        });
        (addr, server)
    }

    /// The `Authorization` header value as the collector saw it, if any.
    fn wire_auth_header(raw: &str) -> Option<String> {
        raw.lines()
            .find(|l| l.to_ascii_lowercase().starts_with("authorization:"))
            .map(|l| l.split_once(':').unwrap().1.trim().to_owned())
    }

    fn post_export(client: &OtlpHttpClient, addr: std::net::SocketAddr, auth: Option<&str>) {
        let mut request = Request::builder()
            .method("POST")
            .uri(format!("http://{addr}/v1/metrics"));
        if let Some(auth) = auth {
            request = request.header(http::header::AUTHORIZATION, auth);
        }
        let request = request.body(Bytes::from_static(b"export-payload")).unwrap();
        // futures' executor, not tokio: send_bytes blocks internally.
        let response = futures::executor::block_on(client.send_bytes(request)).unwrap();
        assert!(response.status().is_success());
    }

    fn test_client(keypair: &crate::transport::TransportKeypair, signed: bool) -> OtlpHttpClient {
        OtlpHttpClient {
            inner: reqwest::blocking::Client::new(),
            signer: signed.then(|| keypair.auth_token_signer()),
            pubkey_b58: bs58::encode(keypair.public_key_bytes()).into_string(),
        }
    }

    #[test]
    fn send_bytes_puts_a_verifiable_bearer_header_on_the_wire() {
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};

        // The full auth transport path: header injection plus the
        // http::Request -> reqwest::blocking::Request conversion, observed
        // from the collector's side of a real socket. Plain #[test], not
        // tokio: the blocking client must stay out of async contexts.
        let (addr, server) = oneshot_collector();
        let keypair = crate::transport::TransportKeypair::new();
        let pubkey_b58 = bs58::encode(keypair.public_key_bytes()).into_string();
        post_export(&test_client(&keypair, true), addr, None);

        let raw = server.join().unwrap();
        let token = wire_auth_header(&raw)
            .expect("Authorization header must reach the wire")
            .strip_prefix("Bearer ")
            .expect("Bearer scheme")
            .to_owned();
        let (payload, sig_b58) = token.rsplit_once('/').unwrap();
        // What the collector computes from the URL it answers at.
        let audience = audience_of(
            &format!("http://{addr}/v1/metrics")
                .parse::<http::Uri>()
                .unwrap(),
        );
        assert!(
            payload.starts_with(&format!("freenet/{pubkey_b58}/{audience}/")),
            "wire token must carry this node's pubkey and the audience hash of \
             the URL it was actually sent to: {token}"
        );
        // Verify exactly like a collector would — see
        // node_pubkey_is_verifiable_with_stock_ed25519.
        let sig_bytes: [u8; 64] = bs58::decode(sig_b58)
            .into_vec()
            .unwrap()
            .try_into()
            .unwrap();
        let edwards = curve25519_dalek::montgomery::MontgomeryPoint(keypair.public_key_bytes())
            .to_edwards(0)
            .unwrap()
            .compress()
            .to_bytes();
        VerifyingKey::from_bytes(&edwards)
            .unwrap()
            .verify(payload.as_bytes(), &Signature::from_bytes(&sig_bytes))
            .expect("wire bearer token must verify with stock ed25519");
        assert!(
            raw.contains("export-payload"),
            "body must survive the http -> reqwest conversion"
        );
    }

    #[test]
    fn an_operator_supplied_authorization_header_is_never_overwritten() {
        // OTEL_EXPORTER_OTLP_HEADERS is applied by the exporter before it
        // calls us. An operator pointing at a hosted collector that wants
        // `Authorization: Basic ...` would otherwise get a 401 on every
        // export with no way to turn our token off but `otel-auth-mode`.
        let (addr, server) = oneshot_collector();
        let keypair = crate::transport::TransportKeypair::new();
        post_export(
            &test_client(&keypair, true),
            addr,
            Some("Basic b3BlcmF0b3I="),
        );
        assert_eq!(
            wire_auth_header(&server.join().unwrap()).as_deref(),
            Some("Basic b3BlcmF0b3I="),
            "the operator's own credentials must reach the collector"
        );
    }

    #[test]
    fn disabled_auth_mode_sends_no_authorization_header() {
        // The default mode: exporting to your own collector must not ship a
        // signed assertion of this node's identity there.
        let (addr, server) = oneshot_collector();
        let keypair = crate::transport::TransportKeypair::new();
        post_export(&test_client(&keypair, false), addr, None);
        assert_eq!(
            wire_auth_header(&server.join().unwrap()),
            None,
            "auth-mode disabled must put no Authorization header on the wire"
        );
    }

    #[test]
    fn fingerprint_attr_is_recomputable_from_the_pubkey_attr() {
        // Requirement: a node cannot fake the UI-facing fingerprint. The
        // collector derives it from the verified pubkey instead of trusting
        // it: b58-decode pubkey, take the first 12 bytes, b58-encode.
        // Asserts on `identity_attributes` — the function production uses —
        // not on strings rebuilt here, which would pass whatever init does.
        let keypair = crate::transport::TransportKeypair::new();
        let (pubkey_attr, fingerprint_attr) = identity_attributes(&keypair);

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
            bearer_token(&signer, &pubkey, &test_audience()),
            bearer_token(&signer, &pubkey, &test_audience())
        );
    }

    #[test]
    fn a_token_for_one_collector_does_not_verify_at_another() {
        // The replay bound: a collector we export to must not be able to
        // present our token at a different collector and impersonate us.
        use xeddsa::xeddsa::Verify;

        let keypair = crate::transport::TransportKeypair::new();
        let (pubkey, _) = identity_attributes(&keypair);
        let token = bearer_token(&keypair.auth_token_signer(), &pubkey, &test_audience());
        let (payload, sig_b58) = token.rsplit_once('/').unwrap();
        let sig_bytes: [u8; 64] = bs58::decode(sig_b58)
            .into_vec()
            .unwrap()
            .try_into()
            .unwrap();
        let other = audience_of(
            &"http://other-collector:4318/v1/metrics"
                .parse::<http::Uri>()
                .unwrap(),
        );
        let replayed = payload.replace(&test_audience(), &other);
        assert_ne!(replayed, payload, "audience must appear in the payload");
        assert!(
            xeddsa::xed25519::PublicKey(keypair.public_key_bytes())
                .verify(replayed.as_bytes(), &sig_bytes)
                .is_err(),
            "a token re-aimed at another collector must fail verification"
        );
    }

    #[test]
    fn the_audience_hash_is_reproducible_from_the_documented_canonical_url() {
        // The collector recomputes this from the URL it expects, so the
        // canonicalization is a wire contract. Recompute it here the way the
        // doc comment (and docs/otel-metrics.md) describe, independently of
        // audience_of's own string building.
        use sha2::{Digest, Sha256};
        let expect = |canonical: &str| {
            bs58::encode(&Sha256::digest(canonical.as_bytes())[..16]).into_string()
        };

        for (uri, canonical) in [
            (
                "http://collector.example:4318/v1/metrics",
                "http://collector.example:4318/v1/metrics",
            ),
            // Default port filled in, scheme and host lowercased.
            (
                "https://Collector.Example/v1/metrics",
                "https://collector.example:443/v1/metrics",
            ),
            (
                "http://collector.example/v1/metrics",
                "http://collector.example:80/v1/metrics",
            ),
            // Credentials stripped: a collector that does not know the
            // password must still be able to reproduce the hash.
            (
                "https://user:secret@collector.example:4318/v1/metrics",
                "https://collector.example:4318/v1/metrics",
            ),
            (
                "http://[::1]:4318/v1/metrics",
                "http://[::1]:4318/v1/metrics",
            ),
            // Everything at once: credentials, port, multi-segment path.
            (
                "http://user:pass@host:1234/path/here",
                "http://host:1234/path/here",
            ),
        ] {
            let audience = audience_of(&uri.parse::<http::Uri>().unwrap());
            assert_eq!(audience, expect(canonical), "audience of {uri}");
            assert!(
                !audience.contains('/'),
                "audience must not contain the token's field separator"
            );
        }

        // The binding has to be sensitive to the parts it claims to cover.
        let of = |u: &str| audience_of(&u.parse::<http::Uri>().unwrap());
        assert_ne!(
            of("https://c.example:4318/v1/metrics"),
            of("http://c.example:4318/v1/metrics"),
            "scheme is bound"
        );
        assert_ne!(
            of("http://c.example:4318/v1/metrics"),
            of("http://c.example:4319/v1/metrics"),
            "port is bound"
        );
        assert_ne!(
            of("http://c.example:4318/tenant-a/v1/metrics"),
            of("http://c.example:4318/tenant-b/v1/metrics"),
            "path is bound — two collectors behind one host must differ"
        );
    }

    #[test]
    fn init_refuses_to_start_from_a_test_process() {
        // The suppression check lives in `init`, and `init` is unreachable
        // under cfg(test) — so without this, deleting the whole block leaves
        // every test green and a `--id` test network ships to a collector.
        // Under cfg(test) an ENABLED config must still come back suppressed;
        // if the check were gone, init would build a pipeline and return None.
        assert_eq!(
            init(
                &enabled_config(),
                &crate::transport::TransportKeypair::new()
            ),
            Some(OtelSuppression::TestHarness),
            "init must consult otel_suppression_reason and return before \
             building anything"
        );
        assert!(
            INSTRUMENTS.get().is_none(),
            "a suppressed init must not have registered instruments"
        );
    }

    /// Cross-file pin (a same-file scrape can be satisfied by its own literal
    /// — see .claude/rules/bug-prevention-patterns.md): every hot-path mirror
    /// that feeds a synchronous instrument must still exist. Delete one and
    /// its counter reports zero forever with nothing else failing.
    ///
    /// Ceiling: presence in the file, not in the right function. A mirror
    /// moved to the wrong call site still passes; a deleted one does not.
    #[test]
    fn every_sync_instrument_still_has_its_hot_path_mirror() {
        for (source, call) in [
            (
                include_str!("../transport/metrics.rs"),
                "crate::tracing::otel::record_rtt_ms(",
            ),
            (
                include_str!("../transport/metrics.rs"),
                "crate::tracing::otel::record_cwnd(",
            ),
            (
                include_str!("../transport/metrics.rs"),
                "crate::tracing::otel::record_transfer(\"completed\")",
            ),
            (
                include_str!("../transport/metrics.rs"),
                "crate::tracing::otel::record_transfer(\"failed\")",
            ),
            (
                include_str!("../transport/metrics.rs"),
                "crate::tracing::otel::record_nat_traversal(\"attempt\")",
            ),
            (
                include_str!("../transport/metrics.rs"),
                "crate::tracing::otel::record_nat_traversal(\"established\")",
            ),
            (
                include_str!("../transport/metrics.rs"),
                "crate::tracing::otel::record_nat_traversal(\"failed_error\")",
            ),
            (
                include_str!("../transport/metrics.rs"),
                "crate::tracing::otel::record_nat_traversal(\"failed_version\")",
            ),
            (
                include_str!("../node/network_status.rs"),
                "crate::tracing::otel::record_op_result(",
            ),
        ] {
            assert!(
                source.contains(call),
                "missing hot-path mirror `{call}`: the instrument it feeds \
                 would report zero forever"
            );
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
    async fn provider_builds_with_auth_disabled() {
        // The default auth mode, and the only path where no signer is
        // installed. It still gets our HttpClient — the exporter has no
        // reqwest feature enabled and would fail with NoHttpClient otherwise.
        let provider = build_provider(
            Some("http://127.0.0.1:1/v1/metrics"),
            "pubkey-under-test".to_string(),
            "fingerprint-under-test".to_string(),
            None,
        )
        .expect("exporter build must succeed with auth disabled");
        tokio::task::spawn_blocking(move || provider.shutdown().expect("clean shutdown"))
            .await
            .expect("shutdown thread panicked");
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
