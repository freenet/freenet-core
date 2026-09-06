//! Standards-configured OpenTelemetry SDK metrics pipeline.
//!
//! Strictly isolated from [`super::telemetry`]: nothing here reads
//! `TelemetryConfig`, and the endpoint never falls back to
//! `DEFAULT_TELEMETRY_ENDPOINT`. The two features are independent by design —
//! see `docs/design/otel-metrics-exporter.md`.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use opentelemetry::metrics::Histogram;
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

/// The endpoint the standard environment declares, in SDK precedence order,
/// or `None` when neither variable is set to a non-blank value.
///
/// Blank-filtered on purpose: `env::var` returns `Ok("")` for a variable set
/// to the empty string, which [`resolve_metrics_endpoint`] correctly treats as
/// unset — so reading it raw would report an override that is not happening.
fn env_endpoint() -> Option<String> {
    [
        opentelemetry_otlp::OTEL_EXPORTER_OTLP_METRICS_ENDPOINT,
        opentelemetry_otlp::OTEL_EXPORTER_OTLP_ENDPOINT,
    ]
    .iter()
    .find_map(|var| {
        let value = std::env::var(var).ok()?;
        let value = value.trim();
        (!value.is_empty()).then(|| value.to_owned())
    })
}

/// The raw `OTEL_EXPORTER_OTLP_*HEADERS` value, in SDK precedence order.
fn env_otlp_headers() -> Option<String> {
    [
        "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
        "OTEL_EXPORTER_OTLP_HEADERS",
    ]
    .iter()
    .find_map(|var| {
        let value = std::env::var(var).ok()?;
        let value = value.trim();
        (!value.is_empty()).then(|| value.to_owned())
    })
}

/// `(lowercased name, value)` for every header the operator declared through
/// `OTEL_EXPORTER_OTLP_HEADERS`.
///
/// Every one is treated as a credential. That variable exists to carry them,
/// and `Authorization` is only the most common spelling: `x-honeycomb-team`,
/// `api-key` and `dd-api-key` are all header-credential schemes in ordinary
/// use with OTLP. Keying the cleartext guard on `Authorization` alone would
/// protect one spelling and quietly leak the others.
///
/// Parsed loosely on purpose — the SDK owns the authoritative parse, and this
/// only decides what to guard and what to redact. Over-inclusion costs a
/// refused export on a plaintext remote endpoint, which is the safe direction.
fn parse_otlp_headers(raw: &str) -> Vec<(String, String)> {
    raw.split(',')
        .filter_map(|pair| pair.split_once('='))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        .filter(|(name, value)| !name.is_empty() && !value.is_empty())
        .collect()
}

/// Whether a credential may ride a request to this URI without going over the
/// wire in cleartext.
///
/// `https` always qualifies. Plaintext `http` qualifies only for loopback,
/// which is the overwhelmingly common OTLP deployment — a collector sidecar on
/// the same host — where there is no network path to intercept.
///
/// This inspects the URI the exporter AIMED at, which is only the real
/// destination because [`export_http_client`] disables redirects and ambient
/// proxies. Re-enable either and this check stops meaning anything.
///
/// One divergence is knowingly accepted: `localhost` is a NAME, resolved by
/// the system resolver, so on a host whose `/etc/hosts` lacks the loopback
/// entry it could resolve off-machine while this returns `true`. Requiring a
/// literal loopback IP would close it and would also break the sidecar
/// configuration the exemption exists for — `http://localhost:4318` is the
/// SDK's own default. Judged the better trade, and recorded rather than
/// silently relied upon.
fn credential_safe(uri: &http::Uri) -> bool {
    if uri.scheme_str() == Some("https") {
        return true;
    }
    // Bracketed IPv6 authority: `http::Uri::host` keeps the brackets.
    let Some(host) = uri
        .host()
        .map(|h| h.trim_start_matches('[').trim_end_matches(']'))
    else {
        return false;
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// An endpoint STRING with any `user:password@` removed, for logging.
///
/// The `Uri`-typed [`redact_uri`] does not cover the startup and validation
/// log lines: those carry the operator's raw config or env value, before it is
/// ever parsed. `otel-endpoint = "http://u:p@collector:4318"` would otherwise
/// put the password in the node log at INFO on every start, and
/// `freenet service report` uploads node logs wholesale.
fn redact_endpoint(endpoint: &str) -> std::borrow::Cow<'_, str> {
    // A missing scheme is NOT a case to skip: it is precisely what the
    // "endpoint is unusable" WARN fires on, so `u:secret@collector:4318`
    // would otherwise be logged verbatim by the one line most likely to see
    // a malformed endpoint.
    let (prefix, rest) = match endpoint.split_once("://") {
        Some((scheme, rest)) => (format!("{scheme}://"), rest),
        None => (String::new(), endpoint),
    };
    // Only an `@` before the first `/` is userinfo; one in a path is not.
    let authority_end = rest.find('/').unwrap_or(rest.len());
    match rest[..authority_end].rfind('@') {
        None => std::borrow::Cow::Borrowed(endpoint),
        Some(at) => std::borrow::Cow::Owned(format!("{prefix}[redacted]@{}", &rest[at + 1..])),
    }
}

/// `uri` with any `user:password@` removed, for logging.
///
/// `http::Uri`'s `Display` renders the authority verbatim, userinfo included,
/// and both the startup line and every export failure log the endpoint — so
/// `otel-endpoint = "http://u:p@collector:4318"` would otherwise put the
/// password in the node log, which `freenet service report` uploads wholesale.
fn redact_uri(uri: &http::Uri) -> String {
    let Some(authority) = uri.authority() else {
        return uri.to_string();
    };
    let Some((_, host_port)) = authority.as_str().split_once('@') else {
        return uri.to_string();
    };
    let scheme = uri
        .scheme_str()
        .map(|s| format!("{s}://"))
        .unwrap_or_default();
    format!("{scheme}[redacted]@{host_port}{}", uri.path())
}

/// Which credential will be refused at export time for `endpoint`, or `None`.
///
/// Covers both sources: `otel-auth-mode = "freenet"` mints our own signed
/// token, and `OTEL_EXPORTER_OTLP_HEADERS` may carry the operator's. `None`
/// for `endpoint` means the SDK default, `http://localhost:4318`, which is
/// loopback and therefore fine.
fn credential_that_would_be_refused(
    auth_mode: crate::config::OtelAuthMode,
    headers: Option<&str>,
    endpoint: Option<&str>,
) -> Option<String> {
    let endpoint = endpoint?;
    let unsafe_endpoint = endpoint
        .parse::<http::Uri>()
        .is_ok_and(|uri| !credential_safe(&uri));
    if !unsafe_endpoint {
        return None;
    }
    if matches!(auth_mode, crate::config::OtelAuthMode::Freenet) {
        return Some("the freenet bearer token (otel-auth-mode)".to_owned());
    }
    headers
        .map(parse_otlp_headers)
        .and_then(|declared| declared.into_iter().next())
        .map(|(name, _)| format!("the `{name}` header from OTEL_EXPORTER_OTLP_HEADERS"))
}

/// Why an OTLP endpoint will not work, or `None` when it is usable.
///
/// Both failure modes are otherwise near-invisible. `http::Uri` accepts
/// `collector:4318` as an authority with no scheme, so the exporter builds and
/// then every export dies converting to a `reqwest::Request` ("relative URL");
/// and an endpoint the SDK cannot parse at all is swallowed with `.ok()`,
/// falling back to `http://localhost:4318` while the startup log still names
/// the operator's URL.
fn endpoint_problem(endpoint: &str) -> Option<&'static str> {
    match endpoint.parse::<http::Uri>() {
        Err(_) => Some(
            "not a valid URL; include the scheme, e.g. http://collector:4318. \
             From the config file this fails the exporter build; from \
             OTEL_EXPORTER_OTLP_* the SDK swallows it and exports to \
             http://localhost:4318 instead",
        ),
        Ok(uri) if !matches!(uri.scheme_str(), Some("http" | "https")) => {
            Some("missing an http:// or https:// scheme; every export will fail to build a request")
        }
        Ok(_) => None,
    }
}

/// How far a `freenet` bearer token's `<timestamp>` may sit from the
/// collector's clock before the collector must reject it.
///
/// Five minutes: wide enough for ordinary NTP drift and a slow export, narrow
/// enough that a captured token is not usefully replayable. Declared here
/// because it is a wire contract — the node stamps the timestamp, the
/// collector enforces the window, and the two have to agree on the number.
pub(crate) const REPLAY_WINDOW: std::time::Duration = std::time::Duration::from_secs(300);

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
///
/// # The signature covers the body, which is not on the wire
///
/// The signing input is the token prefix PLUS `/<base58 SHA-256 of the request
/// body>`, while the transmitted token stops at the prefix. The collector has
/// the body, so it recomputes that hash and verifies against it.
///
/// Without this a token authenticates only "this node addressed this
/// collector at this second" — anyone who obtained one could attach it to a
/// body of their own invention and have it accepted as this node's metrics,
/// which is precisely the spoofing the scheme exists to stop. Keeping the hash
/// off the wire costs nothing and leaves the token format at five fields.
///
/// # Replay window (a collector obligation)
///
/// The node cannot enforce this; the collector must. `<timestamp>` is epoch
/// seconds, and a collector MUST reject a token whose timestamp is outside
/// [`REPLAY_WINDOW`] of its own clock, and SHOULD refuse a
/// `(pubkey, timestamp, body hash)` triple it has already accepted inside that
/// window. Without both, a token captured in flight is replayable for as long
/// as the collector will take it. `docs/otel-metrics.md` states the same rules
/// so a collector implementer has them without reading this.
pub(crate) fn bearer_token(
    signer: &xeddsa::xed25519::PrivateKey,
    pubkey_b58: &str,
    audience: &str,
    body: &[u8],
) -> String {
    use sha2::{Digest, Sha256};
    use xeddsa::xeddsa::Sign;
    // Wall-clock epoch seconds on purpose: the collector checks it against
    // ITS clock, so simulation time would be meaningless here.
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let signed_payload = format!("freenet/{pubkey_b58}/{audience}/{timestamp}");
    let body_hash = bs58::encode(Sha256::digest(body)).into_string();
    let signed_payload_with_body = format!("{signed_payload}/{body_hash}");
    // OS entropy (SysRng), not GlobalRng: XEdDSA's Z randomness hedges the
    // signature nonce, which is cryptographic material — the same exception
    // documented in .claude/rules/code-style.md for keys/nonces. UnwrapErr is
    // required because xeddsa's bound is the infallible rand 0.10 CryptoRng.
    let signature: [u8; 64] = signer.sign(
        signed_payload_with_body.as_bytes(),
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
/// - `{host}:{port}{path}`, e.g. `collector.example:4318/v1/metrics`.
/// - host lowercased (case-insensitive).
/// - port always explicit, defaulting to 80 for an `http` URL and 443 for an
///   `https` one, so `https://c.example/x` and `https://c.example:443/x` agree.
/// - path verbatim — no trailing-slash or dot-segment normalization.
/// - **userinfo stripped**, and query/fragment dropped (an OTLP export URL has
///   neither). Stripping userinfo is not cosmetic: hashing an endpoint of
///   `https://user:secret@collector/` would make the value unreproducible for
///   a collector that does not know the password, and it keeps credentials out
///   of the signed input entirely.
///
/// The scheme is deliberately NOT part of the hashed string. It identifies a
/// transport, not a party, so binding it would not narrow "which collector may
/// use this token" at all — while forcing every collector reachable over both
/// http and https to configure the same URL twice. Note it still leaks in
/// indirectly through the default-port rule above, which is why
/// `http://c.example/x` and `https://c.example/x` do not collide.
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
    let port = uri.port_u16().unwrap_or(match uri.scheme_str() {
        Some("https") => 443,
        _ => 80,
    });
    let canonical = format!("{}:{port}{}", host.to_ascii_lowercase(), uri.path());
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
    /// Every header the operator declared through `OTEL_EXPORTER_OTLP_HEADERS`,
    /// lowercased name and value. Held for two jobs: deciding whether a
    /// request carries a credential at all (the guard below is not
    /// `Authorization`-only), and redacting those values out of anything a
    /// collector echoes back into the log. See [`parse_otlp_headers`].
    operator_credentials: Vec<(String, String)>,
}

impl OtlpHttpClient {
    /// The name of a credential header on this request, if any.
    ///
    /// `Authorization` always counts — ours or the operator's. Beyond it,
    /// every header the operator declared through `OTEL_EXPORTER_OTLP_HEADERS`
    /// counts too, because that is where OTLP credentials live and
    /// `Authorization` is only the most common spelling of one.
    fn credential_header_name(&self, headers: &http::HeaderMap) -> Option<String> {
        if headers.contains_key(http::header::AUTHORIZATION) {
            return Some("Authorization".to_owned());
        }
        self.operator_credentials
            .iter()
            .map(|(name, _)| name)
            .find(|name| headers.keys().any(|h| h.as_str() == name.as_str()))
            .cloned()
    }

    /// Values worth searching an error body for.
    ///
    /// Each declared header contributes its whole value AND, when that value
    /// carries a scheme prefix, the token after it. A collector rejecting a
    /// request names the token alone far more often than the header value —
    /// `unauthorized: key 'sk-abc123' rejected` — so registering only
    /// `Bearer sk-abc123` would miss the commonest spelling of the leak this
    /// exists to stop.
    ///
    /// Matching is exact and case-sensitive. A collector that percent-encodes,
    /// base64s or JSON-escapes the value defeats it; that is a known limit,
    /// not an oversight. The keyword pass in [`redact_keyword_lines`] is the
    /// backstop for anything named alongside a scheme or header word.
    fn redaction_needles(&self) -> Vec<&str> {
        let mut needles = Vec::new();
        for (_, value) in &self.operator_credentials {
            for candidate in [
                value.as_str(),
                value
                    .split_once(' ')
                    .filter(|(scheme, _)| {
                        scheme.eq_ignore_ascii_case("bearer")
                            || scheme.eq_ignore_ascii_case("basic")
                    })
                    .map(|(_, token)| token)
                    .unwrap_or_default(),
            ] {
                if candidate.len() >= MIN_REDACTION_NEEDLE {
                    needles.push(candidate);
                }
            }
        }
        // Longest first, so redacting a token does not leave the surrounding
        // header value unmatched.
        needles.sort_by_key(|n| std::cmp::Reverse(n.len()));
        needles
    }

    /// An error body, safe to log: credentials removed, then truncated.
    ///
    /// The order matters and was wrong once. Truncating first cuts a
    /// credential that straddles the boundary, so the value no longer matches
    /// and its surviving prefix is logged — a body of 240 bytes of prose
    /// followed by `key sk-LIVE-abc123` would put the first characters of a
    /// live key at WARN.
    fn safe_body(&self, body: &[u8]) -> String {
        let text = String::from_utf8_lossy(body);
        let mut text = redact_keyword_lines(&text);
        for needle in self.redaction_needles() {
            if text.contains(needle) {
                text = text.replace(needle, "[redacted]");
            }
        }
        truncate_for_log(&text)
    }
}

// Manual impl: never print the signing key.
impl std::fmt::Debug for OtlpHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtlpHttpClient").finish_non_exhaustive()
    }
}

/// Whether the last export attempt failed, so failures log on the first
/// occurrence and on each failing<->recovering transition rather than every
/// 60s forever.
static EXPORT_FAILING: AtomicBool = AtomicBool::new(false);

/// Report a failed export at WARN, once per failing streak.
///
/// The SDK will not do this for us, despite `opentelemetry-otlp` logging both
/// network errors and non-2xx responses at DEBUG on the stated grounds that
/// "PeriodicReader already logs the returned error via `otel_error!`". That is
/// true for the batch log/span processors and FALSE for metrics: the metrics
/// `PeriodicReader` logs its export result via `otel_debug!`
/// ("PeriodReaderInvokedExport"), and the only `otel_error!` in that file is
/// thread-creation failure. Without this, a collector that is down, rejecting
/// our token, or 413-ing our batches produces NO output at the default log
/// level while startup still says "OTel metrics exporter started".
fn report_export_failure(uri: &http::Uri, detail: &str) {
    if !EXPORT_FAILING.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            uri = redact_uri(uri),
            detail,
            "OTel metrics export is failing; metrics are not reaching the collector"
        );
    }
}

/// Report a successful export, logging only the failing -> recovered edge.
fn report_export_success() {
    if EXPORT_FAILING.swap(false, Ordering::Relaxed) {
        tracing::info!("OTel metrics export recovered");
    }
}

/// Shortest credential value worth searching an error body for.
///
/// `OTEL_EXPORTER_OTLP_HEADERS` carries things that are not secret —
/// `x-scope-orgid=prod` is a Mimir/Loki tenant id — and replacing every
/// occurrence of a four-character value would corrupt the very diagnostics
/// this log exists for. Over-inclusion is still right for the cleartext GUARD,
/// where the cost of a false positive is a refused export; it is wrong here,
/// where the cost is an unreadable log.
const MIN_REDACTION_NEEDLE: usize = 8;

/// Largest response body read from a collector.
///
/// A bound is wanted: a collector answering an OTLP POST with an endless body
/// would otherwise be read until the export timeout. But it must not truncate
/// a LEGITIMATE response — a 200 carries an `ExportMetricsServiceResponse`,
/// normally empty, whose one large field is a `partial_success.error_message`
/// naming rejected datapoints, and the SDK needs the whole protobuf to decode
/// it. Truncating that would leave the SDK unable to report a partial failure
/// while this client called the export a success. Hence a cap far above any
/// plausible real response rather than at the 256 bytes we log.
const MAX_RESPONSE_BODY: u64 = 1024 * 1024;

/// Whether the response-body cap has already been reported, so an oversized
/// collector warns once rather than every 60s forever.
static BODY_CAP_REPORTED: std::sync::Once = std::sync::Once::new();

/// Cut a redacted body down to what is worth logging.
///
/// By CHARACTERS, not bytes, so a multi-byte sequence is never split. Applied
/// AFTER redaction on purpose — see [`OtlpHttpClient::safe_body`].
fn truncate_for_log(text: &str) -> String {
    match text.char_indices().nth(256) {
        None => text.to_owned(),
        Some((at, _)) => text[..at].to_owned(),
    }
}

/// Redact any line naming a credential scheme or header.
fn redact_keyword_lines(text: &str) -> String {
    text.split_inclusive('\n')
        .map(|line| {
            let lower = line.to_ascii_lowercase();
            match ["authorization", "bearer ", "freenet/"]
                .iter()
                .filter_map(|needle| lower.find(needle))
                .min()
            {
                None => line.to_owned(),
                // The WHOLE line, not from the marker onwards: a collector
                // answering `invalid token abc123 in authorization header`
                // puts the credential BEFORE the keyword, and redacting
                // forwards would leave it in the log. Losing one line of
                // rejection detail is the cheaper mistake.
                Some(_) => {
                    let mut redacted = String::from("[redacted]");
                    if line.ends_with('\n') {
                        redacted.push('\n');
                    }
                    redacted
                }
            }
        })
        .collect()
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
                // An audience is only empty when the URI has no host, which
                // cannot reach the wire anyway. Refuse rather than mint
                // `freenet/<pubkey>//<ts>/<sig>`: a collector splitting on
                // '/' with empty-filtering instead of `splitn(5, '/')` would
                // misparse it, and signing an empty audience binds the token
                // to nothing.
                let audience = audience_of(request.uri());
                if audience.is_empty() {
                    report_export_failure(
                        request.uri(),
                        "endpoint has no host, so no collector audience can be \
                         derived; check otel-endpoint",
                    );
                    return Err(
                        "cannot derive a collector audience from an endpoint with no host".into(),
                    );
                }
                let token = bearer_token(
                    signer,
                    &self.pubkey_b58,
                    &audience,
                    // Binds the signature to THIS batch; see `bearer_token`.
                    request.body(),
                );
                request.headers_mut().insert(
                    http::header::AUTHORIZATION,
                    http::HeaderValue::from_str(&format!("Bearer {token}"))?,
                );
            }
        }
        // Whoever the credential belongs to — our signed token or the
        // operator's own header — it does not go out in cleartext to anything
        // but loopback. A stolen static token is reusable until it is rotated;
        // a stolen `freenet` token is replayable at that same collector for
        // `REPLAY_WINDOW`. Failing the export is deliberate: stripping the
        // header silently would surface as a collector-side auth error and
        // send the operator hunting in the wrong place, and sending it anyway
        // is the thing being prevented.
        // Userinfo counts too. NOT because reqwest promotes it to a Basic
        // header — it does that only on the `RequestBuilder` path, and this
        // goes through `TryFrom<http::Request>`, which does not. The reason is
        // simpler: `http://user:secret@collector/` puts a credential in the
        // request line itself, `Uri::host()` strips it so `credential_safe`
        // never sees it, and sending that to a plaintext remote endpoint hands
        // the password to anyone on the path.
        let credential = self.credential_header_name(request.headers()).or_else(|| {
            request
                .uri()
                .authority()
                .filter(|a| a.as_str().contains('@'))
                .map(|_| "endpoint userinfo".to_owned())
        });
        if let Some(name) = credential {
            if !credential_safe(request.uri()) {
                let uri = redact_uri(request.uri());
                report_export_failure(
                    request.uri(),
                    &format!(
                        "refusing to send the `{name}` credential over plaintext http to a \
                         non-loopback collector; use an https:// endpoint or a collector on \
                         loopback (see docs/otel-metrics.md)"
                    ),
                );
                return Err(format!("refusing to send a credential in cleartext to {uri}").into());
            }
        }
        let uri = request.uri().clone();
        // Hand-rolled send, mirroring opentelemetry-http's blocking impl:
        // that impl is on reqwest 0.13's client (opentelemetry-http's own
        // dep), while the workspace is on 0.12, so we can't delegate to it.
        // Blocking inside async is fine here for the same reason the SDK's
        // default client is blocking: PeriodicReader exports via block_on on
        // a dedicated thread. Fold both in when the workspace moves to 0.13.
        //
        // Every outcome is reported through `report_export_*`: the SDK logs
        // them at DEBUG only, so this is the only place a failing export
        // becomes visible to an operator.
        let request: reqwest::blocking::Request = request
            .map(|body| body.to_vec())
            .try_into()
            .inspect_err(|error| {
                // Reached when the endpoint has no scheme: reqwest requires an
                // absolute URL, while `http::Uri` accepts `host:port` as an
                // authority. `init` warns about that at startup too.
                report_export_failure(&uri, &format!("invalid request URL: {error}"))
            })?;
        // Deliberately no `error_for_status()`: it discards the body, which is
        // where OTLP puts rejection detail. Hand the whole response back and
        // the SDK turns a non-2xx into an export error itself.
        let mut response = self
            .inner
            .execute(request)
            .inspect_err(|error| report_export_failure(&uri, &format!("{error}")))?;
        let status = response.status();
        let headers = std::mem::take(response.headers_mut());
        let mut buf = Vec::new();
        std::io::Read::read_to_end(
            &mut std::io::Read::take(&mut response, MAX_RESPONSE_BODY),
            &mut buf,
        )
        .inspect_err(|error| report_export_failure(&uri, &format!("{error}")))?;
        if buf.len() as u64 > MAX_RESPONSE_BODY {
            // Unreachable via `take`, but the cap is the invariant, not the
            // reader: assert it rather than assume it.
            buf.truncate(MAX_RESPONSE_BODY as usize);
        }
        if buf.len() as u64 == MAX_RESPONSE_BODY {
            // Warned once, not per export: a consistently oversized collector
            // would otherwise emit this every interval forever. Reaching the
            // cap means the SDK may be unable to decode a partial-success
            // message, so it is worth saying at all.
            BODY_CAP_REPORTED.call_once(|| {
                tracing::warn!(
                    uri = redact_uri(&uri),
                    cap = MAX_RESPONSE_BODY,
                    "collector response reached the read cap; if it was larger, \
                     partial-success detail for this batch is incomplete"
                );
            });
        }
        let body = Bytes::from(buf);
        if status.is_success() {
            report_export_success();
        } else {
            report_export_failure(&uri, &format!("HTTP {status}: {}", self.safe_body(&body)));
        }
        let mut http_response = Response::builder().status(status).body(body)?;
        *http_response.headers_mut() = headers;
        Ok(http_response)
    }
}

/// The HTTP client every export goes through.
///
/// A named function rather than inline builder calls so the tests exercise the
/// SAME construction production uses. Both policies below are credential
/// safety controls, and a test that built its own client would let either be
/// deleted in silence — which is how a security control rots.
///
/// - **No redirects.** reqwest's default policy replays the request at the new
///   location with the original headers attached, so an https endpoint
///   answering 30x with an http `Location` would carry the credential there in
///   cleartext, past [`credential_safe`], which only ever sees the endpoint
///   the exporter aimed at. A collector has no business redirecting an OTLP
///   POST; a 30x now surfaces as an export failure.
/// - **No ambient proxy.** reqwest defaults to `auto_sys_proxy` and neither it
///   nor hyper-util's matcher has a loopback exemption, so an `HTTP_PROXY` in
///   the environment — routine on corporate networks — would send an export
///   aimed at `http://localhost:4318` to that proxy instead: straight through
///   the loopback exemption, credential attached, off the machine. Same defeat
///   as the redirect and easier to hit, since it is ambient environment rather
///   than something the collector does.
///
/// The timeout is the one the SDK would have applied to its own client, which
/// it does not apply to a supplied one.
fn export_http_client() -> Result<reqwest::blocking::Client, reqwest::Error> {
    reqwest::blocking::Client::builder()
        .timeout(export_timeout())
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
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
/// Returns `Some(reason)` when the exporter was SUPPRESSED and `None`
/// otherwise — including when the pipeline failed to build, which is logged
/// but is not a suppression. Callers ignore the value; it exists so a test can
/// assert that `init` consults [`otel_suppression_reason`] and returns before
/// building anything — deleting the check would make an enabled config return
/// `None` under `cfg(test)`, i.e. ship a test network's metrics to a collector.
pub(crate) fn init(
    config: &OtelConfig,
    keypair: &crate::transport::TransportKeypair,
) -> Option<OtelSuppression> {
    if let Some(reason) = otel_suppression_reason(
        config,
        cfg!(test),
        super::telemetry::running_under_cargo_test(),
    ) {
        // Being off by default is unremarkable; being switched ON and then
        // suppressed anyway is something the operator has to be told about,
        // or the exporter looks enabled and silently ships nothing.
        if config.enabled && reason != OtelSuppression::Disabled {
            tracing::warn!(
                ?reason,
                "otel-telemetry-enabled is set but the OTel metrics exporter was suppressed"
            );
        } else {
            tracing::debug!(?reason, "OTel metrics exporter not started");
        }
        return Some(reason);
    }

    let env_endpoint = env_endpoint();
    let endpoint = resolve_metrics_endpoint(
        config.endpoint.as_deref(),
        std::env::var(opentelemetry_otlp::OTEL_EXPORTER_OTLP_METRICS_ENDPOINT)
            .ok()
            .as_deref(),
        std::env::var(opentelemetry_otlp::OTEL_EXPORTER_OTLP_ENDPOINT)
            .ok()
            .as_deref(),
    );

    // Log where this node's signed identity is actually going, including when
    // an inherited OTEL_* variable overrode the configured endpoint — an
    // operator who cannot see that from the logs cannot tell their collector
    // was bypassed.
    if let (Some(env_endpoint), Some(cfg_endpoint)) =
        (env_endpoint.as_deref(), config.endpoint.as_deref())
    {
        tracing::warn!(
            env_endpoint = %redact_endpoint(env_endpoint),
            cfg_endpoint = %redact_endpoint(cfg_endpoint),
            "OTEL_EXPORTER_OTLP_* overrides the configured otel-endpoint"
        );
    }
    // Validate before building: an endpoint the SDK accepts but reqwest does
    // not produces a per-export failure rather than a build error, and an
    // unparseable env value is swallowed by the SDK, which then silently
    // exports to localhost while the log line below names the operator's URL.
    let effective_endpoint = endpoint.as_deref().or(env_endpoint.as_deref());
    if let Some(effective) = effective_endpoint {
        if let Some(problem) = endpoint_problem(effective) {
            tracing::warn!(
                endpoint = %redact_endpoint(effective),
                problem,
                "OTLP endpoint is unusable"
            );
        }
    }
    // Say it at startup, where an operator is looking. The refusal itself
    // happens per export and latches, so otherwise a plain `http://` typo
    // produces a cheerful "exporter started" line and then, an interval later,
    // one WARN that is easy to miss. This is a configuration error.
    if let Some(credential) = credential_that_would_be_refused(
        config.auth_mode,
        env_otlp_headers().as_deref(),
        effective_endpoint,
    ) {
        tracing::warn!(
            endpoint = %redact_endpoint(effective_endpoint.unwrap_or_default()),
            credential,
            "the collector endpoint is plaintext http and not loopback, so every \
             export will be refused rather than send this credential in cleartext; \
             use an https:// endpoint or a collector on loopback (see \
             docs/otel-metrics.md)"
        );
    }

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
            // shutdown path in `bin/freenet.rs` — but NOT directly from an
            // async fn: `shutdown` drops the blocking reqwest client and its
            // private tokio runtime, which panics with "Cannot drop a runtime
            // in a context where blocking is not allowed". It has to go
            // through `spawn_blocking` (or a plain thread), the same hop
            // `build_provider` makes on the way in and this module's two
            // provider tests make on the way out.
            global::set_meter_provider(provider);
            register_metrics();
            tracing::info!(
                endpoint = %redact_endpoint(
                    endpoint
                        .as_deref()
                        .or(env_endpoint.as_deref())
                        .unwrap_or("http://localhost:4318 (SDK default)")
                ),
                auth_mode = ?config.auth_mode,
                // Named at startup only when it applies. A collector verifying
                // our tokens has to enforce this window, and an operator
                // standing one up should not have to read the source for the
                // number.
                replay_window_secs = matches!(
                    config.auth_mode,
                    crate::config::OtelAuthMode::Freenet
                )
                .then(|| REPLAY_WINDOW.as_secs()),
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
    for var in [
        opentelemetry_otlp::OTEL_EXPORTER_OTLP_METRICS_TIMEOUT,
        opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT,
    ] {
        let Ok(raw) = std::env::var(var) else {
            continue;
        };
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        match raw.parse::<u64>() {
            Ok(ms) => {
                // The spec unit is MILLISECONDS, so `=10` meaning "10 seconds"
                // yields a 10ms timeout and every export times out instead.
                if ms < 100 {
                    tracing::warn!(
                        var,
                        ms,
                        "OTLP export timeout is in MILLISECONDS and this value is very small; \
                         a seconds value was probably intended"
                    );
                }
                return std::time::Duration::from_millis(ms);
            }
            // Fall through to the next variable, as the SDK's own `.ok()`
            // resolution would — but say so rather than ignoring it silently.
            Err(error) => tracing::warn!(
                var,
                raw,
                %error,
                "ignoring unparseable OTLP export timeout (expected whole milliseconds)"
            ),
        }
    }
    opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT
}

/// Resource attribute keys the operator declared through the environment —
/// see [`resource_attributes`] for which of them win.
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

/// The resource attributes to attach, given the keys the operator declared
/// through `OTEL_SERVICE_NAME` / `OTEL_RESOURCE_ATTRIBUTES`.
///
/// Descriptive attributes defer to the environment: `Resource::builder` seeds
/// from those variables and `with_attribute` merges OVER that seed, so setting
/// a literal unconditionally would silently discard the operator's value — two
/// nodes on one host with distinct `OTEL_SERVICE_NAME`s would both export
/// `service.name=freenet-node`.
///
/// The two `freenet.node.*` identity attributes are the exception and are
/// ALWAYS emitted. They are not description, they are the collector's proof of
/// which node sent the batch: `freenet.node.pubkey` must equal the bearer
/// token's `<pubkey>` field, which is verified against the signature. Letting
/// `OTEL_RESOURCE_ATTRIBUTES=freenet.node.pubkey=...` shadow them would export
/// an identity that does not match the key everything was signed with, and
/// break that self-validation silently on both sides.
fn resource_attributes(
    pubkey: String,
    fingerprint: String,
    declared: &[String],
) -> Vec<(&'static str, String)> {
    let mut attributes = vec![
        ("freenet.node.pubkey", pubkey),
        ("freenet.node.fingerprint", fingerprint),
    ];
    for (key, _) in &attributes {
        if declared.iter().any(|declared| declared == key) {
            tracing::warn!(
                key,
                "the node identity resource attribute cannot be overridden by \
                 OTEL_RESOURCE_ATTRIBUTES; the signed value is exported instead"
            );
        }
    }
    attributes.extend(
        [
            ("service.name", "freenet-node".to_owned()),
            ("service.version", env!("CARGO_PKG_VERSION").to_owned()),
            ("os.type", std::env::consts::OS.to_owned()),
            ("host.arch", std::env::consts::ARCH.to_owned()),
        ]
        .into_iter()
        .filter(|(key, _)| !declared.iter().any(|declared| declared == key)),
    );
    attributes
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
    //
    // A build failure is propagated rather than falling back to
    // `Client::new()`: that fallback has NO timeout, so a collector that
    // accepts the connection and never answers would block the PeriodicReader
    // thread indefinitely and stop all metric collection.
    let http_client = export_http_client().map_err(|error| {
        ExporterBuildError::InternalFailure(format!("otel http client build failed: {error}"))
    })?;
    builder = builder.with_http_client(OtlpHttpClient {
        inner: http_client,
        signer: auth_signer,
        pubkey_b58: pubkey.clone(),
        operator_credentials: env_otlp_headers()
            .as_deref()
            .map(parse_otlp_headers)
            .unwrap_or_default(),
    });
    let exporter = builder.build()?;

    // Resource attributes ride once per export batch, not per datapoint, so
    // identifying THIS node here costs nothing per series — unlike a
    // per-datapoint attribute, which is why no instrument below carries one
    // identifying the remote end of a connection.
    // Which of these defer to the environment and which do not is
    // [`resource_attributes`]'s decision.
    let mut resource = Resource::builder();
    for (key, value) in resource_attributes(pubkey, fingerprint, &env_declared_resource_keys()) {
        resource = resource.with_attribute(KeyValue::new(key, value));
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
///
/// ONLY histograms belong here. Anything that is a monotonic count is exported
/// as an observable counter reading the cumulative atomic the measured code
/// already keeps, which cannot drift from the thing it measures; a
/// hand-maintained mirror call at the same site can, and has (#4009, #4010 —
/// `.claude/rules/bug-prevention-patterns.md`). A histogram has no such
/// atomic to read: sum-and-count cannot reconstruct a distribution.
struct Instruments {
    rtt: Histogram<f64>,
    cwnd: Histogram<u64>,
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

/// Register the instruments this crate owns against the process-global meter.
///
/// Must run AFTER `global::set_meter_provider`: `global::meter` binds to
/// whatever provider is installed at call time.
fn register_metrics() {
    let meter = global::meter(METER_NAME);

    if INSTRUMENTS.set(build_instruments(&meter)).is_err() {
        // A second `init` would leave the sync instruments bound to the first
        // provider while the observable ones move to the new one — loud rather
        // than silently half-migrated.
        tracing::warn!("OTel instruments already registered; keeping the first set");
    }
    register_observables(&meter, RingSources::live());
}

/// The synchronous instruments, built against `meter`.
///
/// Separate from [`register_metrics`] so a test can build them against its own
/// provider without setting the `INSTRUMENTS` OnceLock, which is process-global
/// and would leak into every other test in the binary under plain `cargo test`.
fn build_instruments(meter: &opentelemetry::metrics::Meter) -> Instruments {
    Instruments {
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
    }
}

/// Register every observable instrument against `meter`.
///
/// Observable handles are dropped on purpose — the callback is registered into
/// the pipeline at `build()` and observed on every collection cycle regardless.
/// The SDK has no batch-callback API, so each one reads its source
/// independently; that is why those accessors are cheap scalar reads rather
/// than the dashboard's `get_snapshot`.
///
/// Takes the meter rather than reaching for `global::meter` so
/// `instrument_callbacks_export_named_datapoints` can drive all of them against
/// an in-memory exporter. A panic in any callback kills the `PeriodicReader`
/// thread and stops ALL metrics permanently, and no export-side signal reports
/// it, so "the callbacks run at all" needs a test.
fn register_observables(meter: &opentelemetry::metrics::Meter, sources: RingSources) {
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

    register_transport_metrics(meter);
    register_ring_metrics(meter, sources);
    register_queue_metrics(meter);
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

    let _transfers = meter
        .u64_observable_counter("freenet.transport.transfers")
        .with_description("Stream transfers by outcome")
        .with_callback(|observer| {
            let (completed, failed) = TRANSPORT_METRICS.cumulative_transfers();
            observer.observe(completed, &[KeyValue::new("result", "completed")]);
            observer.observe(failed, &[KeyValue::new("result", "failed")]);
        })
        .build();

    let _nat = meter
        .u64_observable_counter("freenet.transport.nat_traversal")
        .with_description("Outbound NAT traversal attempts by outcome")
        .with_callback(|observer| {
            let (attempt, established, failed_error, failed_version) =
                TRANSPORT_METRICS.cumulative_nat_traversal();
            for (result, value) in [
                ("attempt", attempt),
                ("established", established),
                ("failed_error", failed_error),
                ("failed_version", failed_version),
            ] {
                observer.observe(value, &[KeyValue::new("result", result)]);
            }
        })
        .build();
}

/// Ring / topology state, mirroring the dashboard's connection-status tiles.
/// The three accessors every ring- and status-sourced callback reads, behind
/// function pointers so a test can drive them.
///
/// Not indirection for its own sake. Each of those callback bodies is guarded
/// by `if let Some(x) = <source>()`, and in a fresh test process no provider is
/// registered, so the guarded body — which is where the bucket indexing and
/// the `as u64` casts live — never executes. Half the instruments were
/// consequently unasserted by the export test: renaming one, or dropping a
/// `reason=` / `op=` / `result=` attribute, failed nothing, even though
/// `HostingReason::as_str`'s own rustdoc calls those values "a metrics
/// contract" where "renaming one silently empties a panel".
#[derive(Clone, Copy)]
struct RingSources {
    ring_stats: fn() -> Option<crate::node::network_status::RingStatsSnapshot>,
    hosting_reasons: fn() -> Option<crate::ring::HostingReasonStats>,
    status_scalars: fn() -> Option<crate::node::network_status::OtelStatusScalars>,
}

impl RingSources {
    /// What production reads: the live provider-backed accessors.
    fn live() -> Self {
        use crate::node::network_status::{
            otel_hosting_reasons, otel_ring_stats, otel_status_scalars,
        };
        Self {
            ring_stats: otel_ring_stats,
            hosting_reasons: otel_hosting_reasons,
            status_scalars: otel_status_scalars,
        }
    }
}

fn register_ring_metrics(meter: &opentelemetry::metrics::Meter, sources: RingSources) {
    use crate::ring::HostingReason;

    let _connections = meter
        .u64_observable_gauge("freenet.ring.connections")
        .with_description("Active ring connections")
        .with_callback(move |observer| {
            if let Some(ring) = (sources.ring_stats)() {
                observer.observe(ring.connection_count as u64, &[]);
            }
        })
        .build();

    // Both hosted-contract gauges are attributed by `reason` and carry NO
    // un-attributed total: emitting both on one instrument would make
    // `sum by (reason)` double-count. `HostingReason` partitions the hosted
    // set, so the total is `sum(freenet.node.contracts.hosted)`.
    let _hosted = meter
        .u64_observable_gauge("freenet.node.contracts.hosted")
        .with_description(
            "Contracts currently hosted by this node, partitioned by why each one is held",
        )
        .with_callback(move |observer| {
            if let Some(reasons) = (sources.hosting_reasons)() {
                for reason in HostingReason::ALL {
                    observer.observe(
                        reasons.count(reason),
                        &[KeyValue::new("reason", reason.as_str())],
                    );
                }
            }
        })
        .build();

    let _hosted_bytes = meter
        .u64_observable_gauge("freenet.node.contracts.hosted.bytes")
        .with_unit("By")
        .with_description(
            "Contract state bytes hosted by this node, partitioned by why each contract is \
             held. State only — WASM code blobs and database overhead are excluded, matching \
             what the hosting cache's byte budget measures.",
        )
        .with_callback(move |observer| {
            if let Some(reasons) = (sources.hosting_reasons)() {
                for reason in HostingReason::ALL {
                    observer.observe(
                        reasons.bytes(reason),
                        &[KeyValue::new("reason", reason.as_str())],
                    );
                }
            }
        })
        .build();

    // Named for what it counts, not for the field it reads.
    // `NetworkStatus::connection_attempts` has exactly one writer in the tree,
    // inside `record_gateway_failure`, so nothing increments it on an
    // initiated or successful connection. The local dashboard inherits the
    // same mislabel; an instrument name is an external contract operators
    // alert on, and "attempts near zero" reads as the opposite of "failures
    // near zero".
    let _gateway_failures = meter
        .u64_observable_counter("freenet.connect.gateway_failures")
        .with_description("Gateway connection failures since startup")
        .with_callback(move |observer| {
            if let Some(status) = (sources.status_scalars)() {
                observer.observe(status.connection_attempts as u64, &[]);
            }
        })
        .build();

    // Bootstrap-acceptance-churn counters (#4787): a restarted node's
    // gateway connection lingers as transient, expires, and is reaped as a
    // zombie before the onward CONNECT promotes it to the ring, cycling the
    // joiner through repeated reconnects. Instrumentation only (the issue's
    // "before a fix" step) — no acceptance behavior changes here. ACCEPTOR
    // side: `event` distinguishes the transient lifecycle sites; a sustained
    // high `transient_expired`:`promoted_to_ring` ratio is the churn
    // signature. `promoted_to_ring` covers BOTH promotion paths and counts
    // only promotions the ring actually accepted.
    let _bootstrap_churn = meter
        .u64_observable_counter("freenet.bootstrap.churn")
        .with_description(
            "Acceptor-side transient connection registration/expiry/promotion \
             totals since startup (bootstrap-acceptance-churn instrumentation, #4787)",
        )
        .with_callback(move |observer| {
            if let Some(status) = (sources.status_scalars)() {
                observer.observe(
                    status.bootstrap_transient_registered,
                    &[KeyValue::new("event", "transient_registered")],
                );
                observer.observe(
                    status.bootstrap_transient_expired,
                    &[KeyValue::new("event", "transient_expired")],
                );
                observer.observe(
                    status.bootstrap_promoted_to_ring,
                    &[KeyValue::new("event", "promoted_to_ring")],
                );
            }
        })
        .build();

    // JOINER side (#4787): time from process start to first reaching
    // `min_connections`. The clock is anchored at
    // `network_status::mark_process_start()`, called on the first line of the
    // `freenet` binary's `main`, so this genuinely includes config load,
    // storage open and the cached-peer fast-reconnect path — the startup work
    // that can itself delay CONNECT. (A library embedding that never calls it
    // gets the anchor at `network_status::init()`, i.e. node start.)
    //
    // `None` until reached, so this gauge has no datapoint for a node that has
    // not bootstrapped. `freenet.bootstrap.completed` below is what makes that
    // state visible rather than merely absent.
    let _bootstrap_time_to_min_connections = meter
        .f64_observable_gauge("freenet.bootstrap.time_to_min_connections_seconds")
        .with_description(
            "Seconds from process start to first reaching min_connections \
             (bootstrap-acceptance-churn instrumentation, #4787)",
        )
        .with_callback(move |observer| {
            if let Some(status) = (sources.status_scalars)() {
                if let Some(elapsed) = status.bootstrap_time_to_min_connections {
                    observer.observe(elapsed.as_secs_f64(), &[]);
                }
            }
        })
        .build();

    // 1 once this process has reached `min_connections`, 0 before that (#4787
    // finding 3). Without it a permanently-stuck joiner is indistinguishable
    // from an old build or a dropped collector: both simply have no
    // time_to_min_connections datapoint.
    let _bootstrap_completed = meter
        .u64_observable_gauge("freenet.bootstrap.completed")
        .with_description(
            "1 if this process has reached min_connections at least once, 0 if it \
             never has (bootstrap-acceptance-churn instrumentation, #4787)",
        )
        .with_callback(move |observer| {
            if let Some(status) = (sources.status_scalars)() {
                let completed = u64::from(status.bootstrap_time_to_min_connections.is_some());
                observer.observe(completed, &[]);
            }
        })
        .build();

    // Below-threshold join-loop rounds, split by what each round actually did
    // (#4787). A node stuck below `min_connections` increments one of these
    // every ~4s forever, so an unsplit total degrades into a process-uptime
    // proxy; the split is the measurement. `connect_issued` = actively
    // retrying and being refused, `backoff_blocked` = every gateway in
    // exponential backoff, `no_target` = gateway transports look
    // connected/pending while no real peers are acquired (the #4787 stall
    // signature). Read alongside `freenet.bootstrap.completed`.
    let _bootstrap_startup_rounds = meter
        .u64_observable_counter("freenet.bootstrap.startup_rounds")
        .with_description(
            "Below-min_connections join-loop rounds since startup, by outcome \
             (bootstrap-acceptance-churn instrumentation, #4787)",
        )
        .with_callback(move |observer| {
            if let Some(status) = (sources.status_scalars)() {
                observer.observe(
                    status.bootstrap_startup_rounds_connect_issued,
                    &[KeyValue::new("outcome", "connect_issued")],
                );
                observer.observe(
                    status.bootstrap_startup_rounds_backoff_blocked,
                    &[KeyValue::new("outcome", "backoff_blocked")],
                );
                observer.observe(
                    status.bootstrap_startup_rounds_no_target,
                    &[KeyValue::new("outcome", "no_target")],
                );
            }
        })
        .build();

    // Read from the dashboard's own cumulative op counters rather than
    // incremented at `record_op_result`: same instrument, one fewer thing that
    // can be forgotten at a new call site. `record_op_result` is itself a
    // manually-mirrored counter with a documented required-call-site list, and
    // one mirror per fact is the most this can be reduced to.
    let _operations = meter
        .u64_observable_counter("freenet.operation.results")
        .with_description("Completed operations by type and outcome")
        .with_callback(move |observer| {
            let Some(status) = (sources.status_scalars)() else {
                return;
            };
            for (op, (success, failure)) in [
                ("get", status.op_stats.gets),
                ("put", status.op_stats.puts),
                ("update", status.op_stats.updates),
                ("subscribe", status.op_stats.subscribes),
            ] {
                observer.observe(
                    success as u64,
                    &[KeyValue::new("op", op), KeyValue::new("result", "success")],
                );
                observer.observe(
                    failure as u64,
                    &[KeyValue::new("op", op), KeyValue::new("result", "failure")],
                );
            }
        })
        .build();

    let _lattice = meter
        .u64_observable_gauge("freenet.ring.lattice.neighbor")
        .with_description(
            "1 when this node holds its closest connected ring neighbor on that side. \
             Held does not mean tight — compare distances across nodes.",
        )
        .with_callback(move |observer| {
            if let Some(ring) = (sources.ring_stats)() {
                observer.observe(
                    ring.lattice_has_successor as u64,
                    &[KeyValue::new("position", "successor")],
                );
                observer.observe(
                    ring.lattice_has_predecessor as u64,
                    &[KeyValue::new("position", "predecessor")],
                );
            }
        })
        .build();

    let _distance = meter
        .f64_observable_gauge("freenet.ring.lattice.neighbor.distance")
        .with_description("Ring distance to each held lattice edge; absent when unheld")
        .with_callback(move |observer| {
            if let Some(ring) = (sources.ring_stats)() {
                if let Some(d) = ring.lattice_successor_distance {
                    observer.observe(d, &[KeyValue::new("position", "successor")]);
                }
                if let Some(d) = ring.lattice_predecessor_distance {
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
        .with_callback(move |observer| {
            if let Some(ring) = (sources.ring_stats)() {
                observer.observe(
                    ring.lattice_probes_issued,
                    &[KeyValue::new("result", "issued")],
                );
                observer.observe(
                    ring.lattice_probe_improvements,
                    &[KeyValue::new("result", "improvement")],
                );
            }
        })
        .build();

    let _updates = meter
        .u64_observable_counter("freenet.contract.updates")
        .with_description("Relayed UPDATEs by admission outcome")
        .with_callback(move |observer| {
            if let Some(ring) = (sources.ring_stats)() {
                observer.observe(
                    ring.updates_accepted,
                    &[KeyValue::new("result", "accepted")],
                );
                observer.observe(
                    ring.updates_rate_limited,
                    &[KeyValue::new("result", "rate_limited")],
                );
                observer.observe(
                    ring.updates_capacity_dropped,
                    &[KeyValue::new("result", "capacity_dropped")],
                );
            }
        })
        .build();
}

/// Executor fair-queue occupancy and admission outcomes.
///
/// These read `contract::fair_queue_stats()` directly rather than going
/// through the node-status snapshot: it is a free function over always-present
/// statics, so routing it through a snapshot that can be partly unavailable
/// only creates a way for an unrelated missing provider to zero these.
fn register_queue_metrics(meter: &opentelemetry::metrics::Meter) {
    use crate::contract::fair_queue_stats as snapshot;

    let _depth = meter
        .u64_observable_gauge("freenet.contract.queue.depth")
        .with_description(
            "Current fair-queue occupancy, per tier. No `total` series: it would \
             double-count under `sum by (queue)` — sum the tiers instead.",
        )
        .with_callback(|observer| {
            let q = snapshot();
            for (tier, depth) in [
                ("client_local", q.depth_client_local),
                ("network_relay", q.depth_network_relay),
                ("background", q.depth_background),
            ] {
                observer.observe(depth as u64, &[KeyValue::new("queue", tier)]);
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
        .with_callback(|observer| observer.observe(snapshot().high_water as u64, &[]))
        .build();

    let _rejected = meter
        .u64_observable_counter("freenet.contract.queue.rejected")
        .with_description(
            "Fair-queue admission rejections. global_capacity is node-wide saturation; \
             per_contract is one noisy contract hitting its own cap.",
        )
        .with_callback(|observer| {
            let q = snapshot();
            observer.observe(
                q.rejected_global_capacity,
                &[KeyValue::new("reason", "global_capacity")],
            );
            observer.observe(
                q.rejected_per_contract,
                &[KeyValue::new("reason", "per_contract")],
            );
        })
        .build();

    let _shed = meter
        .u64_observable_counter("freenet.contract.queue.background_shed")
        .with_description("Background events shed to make room for higher-priority work")
        .with_callback(|observer| observer.observe(snapshot().background_shed, &[]))
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

    /// The body every token in these tests is signed over. The signature
    /// covers it (see `bearer_token`), so verification must use the same bytes.
    const TEST_BODY: &[u8] = b"export-payload";

    /// What a collector verifies the signature over: the transmitted token
    /// prefix plus the hash of the body it received. The hash is deliberately
    /// NOT on the wire — see [`bearer_token`].
    fn signing_input(payload: &str, body: &[u8]) -> String {
        use sha2::Digest;
        format!(
            "{payload}/{}",
            bs58::encode(sha2::Sha256::digest(body)).into_string()
        )
    }

    /// One keypair plus its token, pre-split, for the verification tests.
    fn token_fixture() -> (crate::transport::TransportKeypair, String) {
        let keypair = crate::transport::TransportKeypair::new();
        let pubkey_b58 = bs58::encode(keypair.public_key_bytes()).into_string();
        let token = bearer_token(
            &keypair.auth_token_signer(),
            &pubkey_b58,
            &test_audience(),
            TEST_BODY,
        );
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
        // The signature covers everything before its own slash PLUS the body
        // hash, and verifies against the token's OWN pubkey — the transport
        // key itself.
        use sha2::Digest as _;
        let body_hash = bs58::encode(sha2::Sha256::digest(TEST_BODY)).into_string();
        let signed_payload = format!("freenet/{pubkey}/{audience}/{timestamp}/{body_hash}");
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
            .verify(
                signing_input(payload, TEST_BODY).as_bytes(),
                &Signature::from_bytes(&sig_bytes),
            )
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
            // Production's client, not a bare one: the redirect and proxy
            // policies it carries are credential-safety controls, and a
            // hand-built client here would let either be deleted in silence.
            inner: export_http_client().expect("export client must build"),
            signer: signed.then(|| keypair.auth_token_signer()),
            pubkey_b58: bs58::encode(keypair.public_key_bytes()).into_string(),
            operator_credentials: Vec::new(),
        }
    }

    /// Ring/status sources that always answer, so every guarded callback body
    /// actually executes. With the live accessors no provider is registered in
    /// a fresh test process, every one of those bodies is skipped, and half the
    /// instrument names below assert nothing at all.
    fn fixture_sources() -> RingSources {
        use crate::node::network_status::{OtelStatusScalars, RingStatsSnapshot};
        fn ring() -> Option<RingStatsSnapshot> {
            Some(RingStatsSnapshot {
                connection_count: 3,
                // `Some`, or the distance gauge emits no datapoint at all and
                // both its name and its `position=` attributes go unasserted.
                lattice_successor_distance: Some(0.25),
                lattice_predecessor_distance: Some(0.5),
                ..Default::default()
            })
        }
        fn reasons() -> Option<crate::ring::HostingReasonStats> {
            Some(crate::ring::HostingReasonStats::default())
        }
        fn scalars() -> Option<OtelStatusScalars> {
            Some(OtelStatusScalars {
                // `Some`, or the time-to-min-connections gauge emits no
                // datapoint at all and its name goes unasserted — same
                // reasoning as `lattice_successor_distance` above.
                bootstrap_time_to_min_connections: Some(std::time::Duration::from_secs(7)),
                bootstrap_transient_registered: 10,
                bootstrap_transient_expired: 9,
                bootstrap_promoted_to_ring: 1,
                bootstrap_startup_rounds_connect_issued: 4,
                bootstrap_startup_rounds_backoff_blocked: 3,
                bootstrap_startup_rounds_no_target: 2,
                ..Default::default()
            })
        }
        RingSources {
            ring_stats: ring,
            hosting_reasons: reasons,
            status_scalars: scalars,
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
            .verify(
                signing_input(payload, b"export-payload").as_bytes(),
                &Signature::from_bytes(&sig_bytes),
            )
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
            bearer_token(&signer, &pubkey, &test_audience(), TEST_BODY),
            bearer_token(&signer, &pubkey, &test_audience(), TEST_BODY)
        );
    }

    #[test]
    fn a_token_for_one_collector_does_not_verify_at_another() {
        // The replay bound: a collector we export to must not be able to
        // present our token at a different collector and impersonate us.
        use xeddsa::xeddsa::Verify;

        let keypair = crate::transport::TransportKeypair::new();
        let (pubkey, _) = identity_attributes(&keypair);
        let token = bearer_token(
            &keypair.auth_token_signer(),
            &pubkey,
            &test_audience(),
            TEST_BODY,
        );
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
                .verify(signing_input(&replayed, TEST_BODY).as_bytes(), &sig_bytes)
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
                "collector.example:4318/v1/metrics",
            ),
            // Default port filled in from the scheme, host lowercased.
            (
                "https://Collector.Example/v1/metrics",
                "collector.example:443/v1/metrics",
            ),
            (
                "http://collector.example/v1/metrics",
                "collector.example:80/v1/metrics",
            ),
            // Credentials stripped: a collector that does not know the
            // password must still be able to reproduce the hash.
            (
                "https://user:secret@collector.example:4318/v1/metrics",
                "collector.example:4318/v1/metrics",
            ),
            ("http://[::1]:4318/v1/metrics", "[::1]:4318/v1/metrics"),
            // Everything at once: credentials, port, multi-segment path.
            (
                "http://user:pass@host:1234/path/here",
                "host:1234/path/here",
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
            of("http://c.example:4318/v1/metrics"),
            of("http://c.example:4319/v1/metrics"),
            "port is bound"
        );
        assert_ne!(
            of("http://c.example:4318/tenant-a/v1/metrics"),
            of("http://c.example:4318/tenant-b/v1/metrics"),
            "path is bound — two collectors behind one host must differ"
        );
        // ...and NOT to the scheme, which names a transport rather than a
        // party: one collector reachable both ways must not need two
        // audience entries. Deliberate, so pin it rather than leave it to
        // be "fixed" later.
        assert_eq!(
            of("https://c.example:4318/v1/metrics"),
            of("http://c.example:4318/v1/metrics"),
            "scheme is deliberately not bound"
        );
        // It does still reach the hash through the default-port rule, so
        // these two do not collide.
        assert_ne!(
            of("https://c.example/v1/metrics"),
            of("http://c.example/v1/metrics"),
            "an omitted port defaults per scheme: 443 vs 80"
        );
    }

    #[test]
    fn a_token_does_not_verify_against_a_different_body() {
        // The point of binding the signature to the body. Without it a token
        // authenticates only "this node addressed this collector at this
        // second", so anyone holding one could attach it to metrics of their
        // own invention and have them accepted as this node's — the exact
        // spoofing the scheme exists to stop.
        use xeddsa::xeddsa::Verify;

        let (keypair, token) = token_fixture();
        let (payload, sig_b58) = token.rsplit_once('/').unwrap();
        let sig_bytes: [u8; 64] = bs58::decode(sig_b58)
            .into_vec()
            .unwrap()
            .try_into()
            .unwrap();
        let pubkey = xeddsa::xed25519::PublicKey(keypair.public_key_bytes());

        assert!(
            pubkey
                .verify(signing_input(payload, TEST_BODY).as_bytes(), &sig_bytes)
                .is_ok(),
            "the token must verify against the body it was minted for"
        );
        assert!(
            pubkey
                .verify(
                    signing_input(payload, b"substituted-metrics").as_bytes(),
                    &sig_bytes
                )
                .is_err(),
            "the same token must NOT verify against a substituted body"
        );
    }

    #[test]
    fn credential_safe_permits_https_and_loopback_only() {
        let of = |u: &str| credential_safe(&u.parse::<http::Uri>().unwrap());
        assert!(of("https://collector.example:4318/v1/metrics"), "https");
        assert!(of("https://collector.example/v1/metrics"), "https, no port");
        // The sidecar deployment, which is the common one and must keep working.
        assert!(of("http://localhost:4318/v1/metrics"), "loopback by name");
        assert!(of("http://127.0.0.1:4318/v1/metrics"), "loopback v4");
        assert!(
            of("http://127.9.9.9:4318/v1/metrics"),
            "all of 127/8 is loopback"
        );
        assert!(of("http://[::1]:4318/v1/metrics"), "loopback v6, bracketed");
        // The cases the guard exists for.
        assert!(
            !of("http://collector.example:4318/v1/metrics"),
            "plaintext remote"
        );
        assert!(
            !of("http://10.0.0.5:4318/v1/metrics"),
            "a LAN address is not loopback"
        );
        assert!(!of("http://[2001:db8::1]:4318/v1/metrics"), "remote v6");
    }

    /// A client whose only credential is the operator's, under a header name
    /// that is not `Authorization`.
    fn client_with_operator_header(name: &str, value: &str) -> OtlpHttpClient {
        OtlpHttpClient {
            inner: export_http_client().expect("export client must build"),
            signer: None,
            pubkey_b58: String::new(),
            operator_credentials: vec![(name.to_ascii_lowercase(), value.to_owned())],
        }
    }

    fn post_to(client: &OtlpHttpClient, uri: &str, header: Option<(&str, &str)>) -> String {
        let mut request = Request::builder().method("POST").uri(uri);
        if let Some((name, value)) = header {
            request = request.header(name, value);
        }
        let request = request.body(Bytes::from_static(TEST_BODY)).unwrap();
        futures::executor::block_on(client.send_bytes(request))
            .expect_err("a credential over plaintext http must not be sent")
            .to_string()
    }

    #[test]
    fn a_credential_is_refused_over_plaintext_http_to_a_remote_collector() {
        // A stolen static token is reusable until rotated and a stolen freenet
        // token is replayable at that collector for REPLAY_WINDOW, so the
        // export fails rather than putting either on the wire. Bound to a host
        // that does not resolve: the assertion is that we never connect.
        let keypair = crate::transport::TransportKeypair::new();
        let signed = test_client(&keypair, true);
        let error = post_to(
            &signed,
            "http://collector.example:4318/v1/metrics",
            Some(("authorization", "Bearer operator-secret")),
        );
        assert!(
            error.contains("cleartext"),
            "the error must say why it refused, got: {error}"
        );
    }

    #[test]
    fn an_operator_credential_that_is_not_authorization_is_still_guarded() {
        // Honeycomb (`x-honeycomb-team`), New Relic (`api-key`) and Datadog
        // (`dd-api-key`) all carry their keys in custom headers through the
        // same OTEL_EXPORTER_OTLP_HEADERS variable. Guarding `Authorization`
        // alone would protect one spelling and leak every other.
        let client = client_with_operator_header("x-honeycomb-team", "hcaik_secret");
        let error = post_to(
            &client,
            "http://collector.example:4318/v1/metrics",
            Some(("x-honeycomb-team", "hcaik_secret")),
        );
        assert!(
            error.contains("cleartext"),
            "a non-Authorization credential must be guarded too, got: {error}"
        );
    }

    #[test]
    fn safe_body_redacts_a_credential_the_collector_names_without_a_keyword() {
        // Keyword matching alone misses this: `unauthorized: key '...'` holds
        // neither `authorization` nor `bearer `, so the key would reach the
        // node log verbatim — and `freenet service report` uploads logs whole.
        let client = client_with_operator_header("x-api-key", "sk-abc123");
        let redacted =
            client.safe_body(b"{\"message\":\"unauthorized: key 'sk-abc123' rejected\"}");
        assert!(
            !redacted.contains("sk-abc123"),
            "the credential value must not reach the log: {redacted}"
        );
        assert!(
            redacted.contains("unauthorized"),
            "the rejection detail this body exists for must survive: {redacted}"
        );
    }

    #[test]
    fn redact_keyword_lines_removes_echoed_credentials() {
        let redacted = redact_keyword_lines(
            "rejected\nauthorization: Bearer operator-secret\ntrailing detail\n",
        );
        assert!(
            !redacted.contains("operator-secret"),
            "an echoed credential must not reach the log: {redacted}"
        );
        assert!(
            redacted.contains("rejected") && redacted.contains("trailing detail"),
            "the rejection detail must survive: {redacted}"
        );
        // Our own token, echoed back without a header name in front of it.
        // Our own token, echoed back without a header name in front of it.
        assert!(!redact_keyword_lines("bad token freenet/aaa/bbb/1/ccc").contains("ccc"));
        assert_eq!(
            redact_keyword_lines("quota exceeded"),
            "quota exceeded",
            "an ordinary body must pass through untouched"
        );
        // Truncation is by CHARACTER, so a multi-byte sequence at the
        // boundary cannot split and panic.
        let wide = "é".repeat(400);
        assert_eq!(truncate_for_log(&wide).chars().count(), 256);
    }

    #[test]
    fn safe_body_redacts_a_credential_that_straddles_the_truncation_boundary() {
        // Ordering bug, found in review: truncating BEFORE redacting cuts the
        // value, so `contains` no longer matches and the surviving prefix is
        // logged. 240 characters of prose then a live key put its first
        // characters at WARN.
        let client = client_with_operator_header("x-api-key", "sk-LIVE-abcdefghijklmnop");
        let mut body = "x".repeat(240).into_bytes();
        body.extend_from_slice(b" key sk-LIVE-abcdefghijklmnop rejected");
        let redacted = client.safe_body(&body);
        assert!(
            !redacted.contains("sk-LIVE"),
            "no part of the credential may survive truncation: {redacted}"
        );
    }

    #[test]
    fn safe_body_redacts_the_token_inside_a_scheme_prefixed_header_value() {
        // `OTEL_EXPORTER_OTLP_HEADERS=authorization=Bearer sk-abc12345` stores
        // `Bearer sk-abc12345`, but a collector rejecting it names the token
        // alone. Registering only the whole value would miss the commonest
        // spelling of exactly the leak this exists to stop.
        let client = client_with_operator_header("authorization", "Bearer sk-abc12345");
        let redacted = client.safe_body(b"{\"message\":\"rejected key 'sk-abc12345'\"}");
        assert!(
            !redacted.contains("sk-abc12345"),
            "the token alone must be redacted, not just the whole header value: {redacted}"
        );
    }

    #[test]
    fn safe_body_leaves_short_non_secret_values_alone() {
        // `x-scope-orgid=prod` is a Mimir/Loki tenant id, not a secret.
        // Replacing every "prod" would corrupt the diagnostics this log
        // exists for, so the redaction pass has a minimum needle length even
        // though the cleartext GUARD still treats the header as a credential.
        let client = client_with_operator_header("x-scope-orgid", "prod");
        let body = client.safe_body(b"tenant prod rejected: bad timestamp");
        assert!(
            body.contains("prod rejected"),
            "a short, non-secret value must not be scrubbed out of the detail: {body}"
        );
    }

    #[test]
    fn redact_uri_strips_userinfo() {
        // Both the startup line and every export failure log the endpoint, and
        // `http::Uri`'s Display renders the authority verbatim.
        let of = |u: &str| redact_uri(&u.parse::<http::Uri>().unwrap());
        assert_eq!(
            of("http://user:pass@collector.example:4318/v1/metrics"),
            "http://[redacted]@collector.example:4318/v1/metrics"
        );
        assert!(!of("http://user:pass@c.example/x").contains("pass"));
        assert_eq!(
            of("https://collector.example:4318/v1/metrics"),
            "https://collector.example:4318/v1/metrics",
            "a URL with no userinfo must be unchanged"
        );

        // The startup INFO line and the endpoint-validation WARN log the
        // operator's RAW config string, before it is ever parsed into a Uri,
        // so the string form has to redact too.
        let of = |e: &str| redact_endpoint(e).into_owned();
        assert_eq!(
            of("http://user:pass@collector.example:4318"),
            "http://[redacted]@collector.example:4318"
        );
        assert!(!of("https://user:hunter2@c.example/v1/metrics").contains("hunter2"));
        assert_eq!(
            of("http://collector.example:4318"),
            "http://collector.example:4318",
            "no userinfo, unchanged"
        );
        assert_eq!(
            of("http://localhost:4318 (SDK default)"),
            "http://localhost:4318 (SDK default)",
            "the default placeholder must survive verbatim"
        );
        // An `@` in the PATH is not userinfo and must not trigger redaction.
        assert_eq!(
            of("http://collector.example/v1/a@b"),
            "http://collector.example/v1/a@b"
        );
        // A MISSING scheme is exactly what the "endpoint is unusable" WARN
        // fires on, so it is the line most likely to see a malformed endpoint
        // — and must still redact.
        assert_eq!(of("u:secret@collector:4318"), "[redacted]@collector:4318");
        assert!(!of("user:hunter2@collector:4318/v1/metrics").contains("hunter2"));
    }

    #[test]
    fn nothing_is_exported_anywhere_until_an_operator_asks() {
        // Freenet is a privacy-focused network: shipping node telemetry to a
        // third party by default would be unacceptable. Three independent
        // defaults carry that promise and each is one careless line from being
        // reversed, so pin all three rather than trusting review to notice.
        let cfg = OtelConfig::default();
        assert!(!cfg.enabled, "the exporter must be OFF unless enabled");
        assert_eq!(cfg.endpoint, None, "no endpoint may be baked in");
        assert_eq!(
            cfg.auth_mode,
            crate::config::OtelAuthMode::Disabled,
            "a node must not assert a signed identity to a collector unasked"
        );
        // With nothing configured the SDK default applies, and it is loopback.
        assert_eq!(resolve_metrics_endpoint(None, None, None), None);
    }

    #[test]
    fn a_credential_bound_for_a_plaintext_remote_endpoint_is_diagnosed_at_startup() {
        use crate::config::OtelAuthMode;
        let remote = Some("http://collector.example:4318");
        // Our own token.
        assert!(
            credential_that_would_be_refused(OtelAuthMode::Freenet, None, remote).is_some(),
            "auth-mode=freenet over plaintext remote must be diagnosed"
        );
        // The operator's, under any header name.
        assert!(
            credential_that_would_be_refused(
                OtelAuthMode::Disabled,
                Some("x-api-key=secret"),
                remote
            )
            .is_some()
        );
        // Not diagnosed: safe endpoint, loopback, or no credential at all.
        assert!(
            credential_that_would_be_refused(
                OtelAuthMode::Freenet,
                None,
                Some("https://collector.example:4318")
            )
            .is_none()
        );
        assert!(
            credential_that_would_be_refused(
                OtelAuthMode::Freenet,
                None,
                Some("http://127.0.0.1:4318")
            )
            .is_none()
        );
        assert!(credential_that_would_be_refused(OtelAuthMode::Disabled, None, remote).is_none());
        // No endpoint means the SDK default, http://localhost:4318 — loopback.
        assert!(credential_that_would_be_refused(OtelAuthMode::Freenet, None, None).is_none());
    }

    #[test]
    fn the_export_client_keeps_its_credential_safety_policies() {
        // Both lines are security controls with no observable behaviour in a
        // unit test — no redirect is followed and no proxy is consulted when
        // neither is configured — so deleting either passes every other test
        // in this file. `credential_safe` can only inspect the URI we aimed
        // at, and each of these is what makes that the real destination.
        let body = top_level_fn_body(include_str!("otel.rs"), "fn export_http_client()");
        assert!(
            body.contains("Client::builder()"),
            "the pin must have scoped to the builder, or it proves nothing: {body}"
        );
        assert!(
            body.contains("redirect(reqwest::redirect::Policy::none())"),
            "redirects must stay disabled: reqwest replays a 30x with the \
             original headers, carrying a credential to an http Location"
        );
        assert!(
            body.contains(".no_proxy()"),
            "ambient proxies must stay disabled: reqwest defaults to \
             auto_sys_proxy with no loopback exemption, so HTTP_PROXY would \
             take an export aimed at localhost off the machine"
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

    /// Body of the METHOD whose signature line is `signature`, bounded to that
    /// method.
    ///
    /// The free-function `fn_body` in `bin/commands/auto_update.rs` refuses
    /// indented definitions, because its `\n}\n` end-anchor would slice to the
    /// end of the enclosing `impl`. Every site pinned below is a method, so
    /// this variant anchors on the matching 4-space-indented `\n    }\n`
    /// instead. Same reason both exist at all: an unbounded
    /// `source.contains(call)` does not fail when the call MOVES — it matches
    /// a later occurrence, typically the pin's own assertion string, and
    /// passes vacuously (AGENTS.md, #5103).
    /// [`method_body`]'s sibling for a TOP-LEVEL fn, which closes on an
    /// unindented `}`. Using `method_body` here would run past the end of the
    /// function to the next indented closer it found, so the region pinned
    /// would not be the one named — a pin that is wrong in the direction of
    /// passing.
    fn top_level_fn_body<'a>(src: &'a str, signature: &str) -> &'a str {
        let at = src
            .find(signature)
            .unwrap_or_else(|| panic!("definition not found: {signature}"));
        let tests_at = src
            .find("\n#[cfg(test)]\nmod ")
            .map(|i| i + 1)
            .expect("test module not located — this guard cannot verify anything");
        assert!(
            at < tests_at,
            "`{signature}` matched inside the test module — this pin is \
             scraping its own source and would pass vacuously"
        );
        let after = &src[at + signature.len()..];
        let (body, _) = after
            .split_once("\n}\n")
            .unwrap_or_else(|| panic!("could not locate end of: {signature}"));
        body
    }

    fn method_body<'a>(src: &'a str, signature: &str) -> &'a str {
        let at = src
            .find(signature)
            .unwrap_or_else(|| panic!("definition not found: {signature}"));
        let tests_at = src
            .find("\n#[cfg(test)]\nmod ")
            .map(|i| i + 1)
            .expect("test module not located — this guard cannot verify anything");
        assert!(
            at < tests_at,
            "`{signature}` matched inside the test module — this pin is \
             scraping its own source and would pass vacuously"
        );
        let after = &src[at + signature.len()..];
        let (body, _) = after
            .split_once("\n    }\n")
            .unwrap_or_else(|| panic!("could not locate end of: {signature}"));
        assert!(
            !body.contains("\n#[cfg(test)]\nmod "),
            "scoped region for `{signature}` escaped into the test module — \
             this pin would pass vacuously"
        );
        body
    }

    /// Cross-file pin (a same-file scrape can be satisfied by its own literal
    /// — see .claude/rules/bug-prevention-patterns.md): the two histogram
    /// mirrors must still sit IN the function that takes the sample. Delete or
    /// move one and its histogram reports nothing forever with nothing else
    /// failing.
    ///
    /// Only histograms are pinned because only histograms are mirrored: every
    /// counter this crate exports is an observable reading a cumulative atomic
    /// the measured code already keeps, so there is no second call site to
    /// forget. See [`Instruments`].
    ///
    /// Mutation-tested when written: moving `record_rtt_ms` out of
    /// `record_rtt_sample` (into, say, `record_transfer_completed`) fails
    /// here, where the previous unbounded `source.contains` stayed green.
    #[test]
    fn every_sync_instrument_still_has_its_hot_path_mirror() {
        let source = include_str!("../transport/metrics.rs");
        for (signature, call) in [
            (
                "    pub(crate) fn record_cwnd_sample(&self, cwnd_bytes: u32) {",
                "crate::tracing::otel::record_cwnd(",
            ),
            (
                "    pub(crate) fn record_rtt_sample(&self, rtt_us: u64) {",
                "crate::tracing::otel::record_rtt_ms(",
            ),
        ] {
            assert!(
                method_body(source, signature).contains(call),
                "`{signature}` no longer calls `{call}`: the histogram it \
                 feeds would report nothing forever"
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
        // Both identity attributes must stay address-free. Asserts on
        // `identity_attributes` — the function production uses — because
        // rebuilding the strings here would pass whatever `init` actually
        // attaches, including a `PeerId`.
        let keypair = crate::transport::TransportKeypair::new();
        let (pubkey_attr, fingerprint_attr) = identity_attributes(&keypair);
        for instance_id in [pubkey_attr, fingerprint_attr] {
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
        assert!(
            INSTRUMENTS.get().is_none(),
            "recording must not lazily bind instruments to the no-op provider"
        );
    }

    #[test]
    fn env_declared_attributes_cannot_shadow_the_node_identity() {
        // The collector verifies the bearer token's signature and then trusts
        // `freenet.node.pubkey` as the sender's identity. If
        // OTEL_RESOURCE_ATTRIBUTES could override that attribute, a node would
        // export an identity that does not match the key it signed with, and
        // neither side would notice.
        let declared = [
            "freenet.node.pubkey".to_owned(),
            "freenet.node.fingerprint".to_owned(),
            "service.name".to_owned(),
        ];
        let attributes = resource_attributes("REAL-PK".into(), "REAL-FP".into(), &declared);
        let value = |key: &str| {
            attributes
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.as_str())
        };

        assert_eq!(
            value("freenet.node.pubkey"),
            Some("REAL-PK"),
            "the signed pubkey must be exported even when the environment declares it"
        );
        assert_eq!(value("freenet.node.fingerprint"), Some("REAL-FP"));
        // Descriptive attributes still defer: the operator's own service.name
        // has to survive, or two nodes on one host collapse into one series.
        assert_eq!(
            value("service.name"),
            None,
            "a declared service.name must be left to the environment"
        );
        assert!(
            value("service.version").is_some(),
            "undeclared descriptive attributes are still filled in"
        );
    }

    #[test]
    fn unusable_endpoints_are_diagnosed() {
        // `http::Uri` accepts `host:port` as an authority with no scheme, so
        // the exporter builds and every export then dies converting to a
        // reqwest request — the failure is per-export, not at startup.
        assert!(
            endpoint_problem("collector.example:4318").is_some(),
            "a schemeless authority must be reported"
        );
        // Not parseable at all: the SDK swallows this and falls back to
        // localhost while the startup log names the operator's URL.
        assert!(endpoint_problem("collector.example:4318/v1/metrics").is_some());
        assert!(endpoint_problem("not a url").is_some());
        // A non-HTTP scheme parses fine but reqwest cannot send it.
        assert!(endpoint_problem("ftp://collector.example:4318/v1/metrics").is_some());

        assert_eq!(
            endpoint_problem("http://collector.example:4318/v1/metrics"),
            None
        );
        assert_eq!(
            endpoint_problem("https://collector.example/v1/metrics"),
            None
        );
    }

    /// Every observable callback runs, and the instruments carry the names and
    /// attributes the collector-side dashboards filter on.
    ///
    /// This is the only test that executes an instrument at all: `init` returns
    /// early under `cfg(test)` (see `init_refuses_to_start_from_a_test_process`),
    /// so without this the eighteen callbacks, every instrument name, every unit
    /// and every attribute are unexecuted in CI. That matters beyond naming — a
    /// panic in one callback kills the `PeriodicReader` thread and silently
    /// stops ALL metrics, and `report_export_failure` cannot see it because it
    /// is an export-side signal.
    ///
    /// Deliberately built against a LOCAL provider: neither
    /// `global::set_meter_provider` nor the `INSTRUMENTS` OnceLock is touched,
    /// because both are process-global and would leak into every other test in
    /// this binary under plain `cargo test` (which, unlike nextest, shares one
    /// process — see .claude/rules/testing.md).
    #[test]
    fn instrument_callbacks_export_named_datapoints() {
        use opentelemetry::metrics::MeterProvider;
        use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader};

        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let meter = provider.meter(METER_NAME);

        // Same two calls `register_metrics` makes, minus the global bindings.
        let instruments = build_instruments(&meter);
        register_observables(&meter, fixture_sources());
        instruments.rtt.record(12.5, &[]);
        instruments.cwnd.record(4096, &[]);

        provider.force_flush().expect("collection must not fail");
        let exported = exporter.get_finished_metrics().expect("exported batches");

        let mut seen: Vec<(String, Vec<String>)> = Vec::new();
        for resource_metric in &exported {
            for scope in resource_metric.scope_metrics() {
                for metric in scope.metrics() {
                    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
                    let render = |kv: &KeyValue| format!("{}={}", kv.key, kv.value.as_str());
                    // Only the u64 sums and gauges carry attributes this test
                    // asserts on; the histograms are checked by name alone.
                    // A wildcard rather than an exhaustive listing so a future
                    // SDK variant does not break the build here — and if an
                    // instrument's aggregation ever moves, its attribute
                    // assertions below fail loudly rather than silently pass.
                    #[allow(clippy::wildcard_enum_match_arm)]
                    let attributes: Vec<String> = match metric.data() {
                        AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                            .data_points()
                            .flat_map(|p| p.attributes())
                            .map(render)
                            .collect(),
                        AggregatedMetrics::U64(MetricData::Gauge(gauge)) => gauge
                            .data_points()
                            .flat_map(|p| p.attributes())
                            .map(render)
                            .collect(),
                        // `freenet.ring.lattice.neighbor.distance` is the one
                        // f64 observable. Without this arm its attribute
                        // vector is always empty, so every assertion on its
                        // `position=` values passes vacuously no matter what
                        // the callback emits.
                        AggregatedMetrics::F64(MetricData::Gauge(gauge)) => gauge
                            .data_points()
                            .flat_map(|p| p.attributes())
                            .map(render)
                            .collect(),
                        _ => Vec::new(),
                    };
                    seen.push((metric.name().to_string(), attributes));
                }
            }
        }

        let names: Vec<&str> = seen.iter().map(|(name, _)| name.as_str()).collect();
        // The histograms prove the synchronous path exports; the rest prove
        // each observable callback ran and produced a datapoint.
        for expected in [
            "freenet.transport.rtt",
            "freenet.transport.cwnd",
            "freenet.transport.bytes",
            "freenet.transport.packets",
            "freenet.transport.transfers",
            "freenet.transport.nat_traversal",
            "freenet.contract.queue.depth",
            "freenet.contract.queue.depth.high_water",
            "freenet.contract.queue.rejected",
            "freenet.contract.queue.background_shed",
            // Ring- and status-sourced: unasserted before `RingSources` made
            // their sources injectable, because the guarded bodies never ran.
            "freenet.ring.connections",
            "freenet.node.contracts.hosted",
            "freenet.node.contracts.hosted.bytes",
            "freenet.connect.gateway_failures",
            "freenet.operation.results",
            "freenet.ring.lattice.neighbor",
            "freenet.ring.lattice.neighbor.distance",
            "freenet.ring.lattice.probes",
            "freenet.contract.updates",
            // Bootstrap-acceptance-churn instrumentation (#4787).
            "freenet.bootstrap.churn",
            "freenet.bootstrap.time_to_min_connections_seconds",
            "freenet.bootstrap.completed",
            "freenet.bootstrap.startup_rounds",
        ] {
            assert!(
                names.contains(&expected),
                "instrument `{expected}` produced no datapoint; exported: {names:?}"
            );
        }

        // ...and by attribute, since a collector-side dashboard filters on
        // these strings: renaming one silently empties a panel.
        let attributes_of = |name: &str| {
            seen.iter()
                .find(|(n, _)| n == name)
                .map(|(_, a)| a.clone())
                .unwrap_or_default()
        };
        for (name, attribute) in [
            ("freenet.transport.bytes", "direction=sent"),
            ("freenet.transport.bytes", "direction=received"),
            ("freenet.transport.transfers", "result=completed"),
            ("freenet.transport.transfers", "result=failed"),
            ("freenet.transport.nat_traversal", "result=attempt"),
            ("freenet.transport.nat_traversal", "result=failed_version"),
            ("freenet.contract.queue.depth", "queue=client_local"),
            ("freenet.contract.queue.depth", "queue=network_relay"),
            ("freenet.contract.queue.depth", "queue=background"),
            ("freenet.contract.queue.rejected", "reason=per_contract"),
            ("freenet.contract.queue.rejected", "reason=global_capacity"),
            ("freenet.transport.packets", "direction=sent"),
            ("freenet.transport.packets", "direction=received"),
            ("freenet.transport.nat_traversal", "result=established"),
            ("freenet.transport.nat_traversal", "result=failed_error"),
            // Every `HostingReason::as_str` value: its own rustdoc calls these
            // a metrics contract where renaming one empties a panel.
            ("freenet.node.contracts.hosted", "reason=local_client"),
            ("freenet.node.contracts.hosted", "reason=downstream"),
            ("freenet.node.contracts.hosted", "reason=subscribed"),
            ("freenet.node.contracts.hosted", "reason=local_access"),
            ("freenet.node.contracts.hosted", "reason=abandoned"),
            ("freenet.node.contracts.hosted", "reason=restored"),
            ("freenet.node.contracts.hosted", "reason=routed"),
            ("freenet.node.contracts.hosted.bytes", "reason=local_client"),
            ("freenet.node.contracts.hosted.bytes", "reason=downstream"),
            ("freenet.node.contracts.hosted.bytes", "reason=subscribed"),
            ("freenet.node.contracts.hosted.bytes", "reason=local_access"),
            ("freenet.node.contracts.hosted.bytes", "reason=abandoned"),
            ("freenet.node.contracts.hosted.bytes", "reason=restored"),
            ("freenet.node.contracts.hosted.bytes", "reason=routed"),
            ("freenet.operation.results", "op=get"),
            ("freenet.operation.results", "op=put"),
            ("freenet.operation.results", "op=update"),
            ("freenet.operation.results", "op=subscribe"),
            ("freenet.operation.results", "result=success"),
            ("freenet.operation.results", "result=failure"),
            ("freenet.ring.lattice.neighbor", "position=successor"),
            ("freenet.ring.lattice.neighbor", "position=predecessor"),
            (
                "freenet.ring.lattice.neighbor.distance",
                "position=successor",
            ),
            (
                "freenet.ring.lattice.neighbor.distance",
                "position=predecessor",
            ),
            ("freenet.ring.lattice.probes", "result=issued"),
            ("freenet.ring.lattice.probes", "result=improvement"),
            ("freenet.bootstrap.churn", "event=transient_registered"),
            ("freenet.bootstrap.churn", "event=transient_expired"),
            ("freenet.bootstrap.churn", "event=promoted_to_ring"),
            ("freenet.bootstrap.startup_rounds", "outcome=connect_issued"),
            (
                "freenet.bootstrap.startup_rounds",
                "outcome=backoff_blocked",
            ),
            ("freenet.bootstrap.startup_rounds", "outcome=no_target"),
            ("freenet.contract.updates", "result=accepted"),
            ("freenet.contract.updates", "result=rate_limited"),
            ("freenet.contract.updates", "result=capacity_dropped"),
        ] {
            assert!(
                attributes_of(name).iter().any(|a| a == attribute),
                "`{name}` is missing attribute `{attribute}`; has {:?}",
                attributes_of(name)
            );
        }

        // NOT asserted here: that the source-gated instruments (ring, hosted,
        // operations) export NOTHING when their source is unregistered. That
        // is real behaviour — an observable with no source must skip the cycle
        // rather than report a zero, because a zero is a real datapoint — but
        // it cannot be asserted from this process. `NETWORK_STATUS` is a
        // process-global OnceLock and `RING_STATS_PROVIDER` a global static,
        // and other tests in this binary set both; under plain `cargo test`
        // (one process, unlike nextest) the assertion's outcome depends on
        // test order. What this test does guard about those instruments is
        // the part that matters: their callbacks RAN, without panicking, which
        // is what keeps the PeriodicReader thread alive.
        //
        // That the queue instruments above export regardless is the visible
        // half of the same property — one unregistered provider no longer
        // zeroes unrelated metrics.
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
