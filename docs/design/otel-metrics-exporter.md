# Design: OpenTelemetry metrics exporter (isolated from existing telemetry)

Status: proposed. Implementation plan:
[`docs/superpowers/plans/2026-08-01-otel-metrics-exporter.md`](../superpowers/plans/2026-08-01-otel-metrics-exporter.md).

## Problem

`TelemetryReporter` (`crates/core/src/tracing/telemetry.rs`) hand-builds OTLP-JSON
log records and POSTs them to `{telemetry-endpoint}/v1/logs` with `reqwest`
(`telemetry.rs:1415-1500`). It is the feed for the project's central dashboard and
it works. It is not an OpenTelemetry SDK pipeline: no meter provider, no
instruments, no standard `OTEL_*` env-var handling, no metrics or traces — so
there is nowhere to hang actual metrics.

## Goal

Add a real OpenTelemetry SDK metrics pipeline that can be pointed at any OTLP
collector and configured the standard way. The existing telemetry path is
untouched.

## Non-goals

- Any change to `TelemetryReporter`, `to_otlp_logs`, the `/v1/logs` wire format,
  `telemetry-enabled`, or `telemetry-endpoint`.
- Migrating existing events onto the new pipeline.
- Logs and traces exporters. The endpoint resolution is designed to serve all
  three signals; only metrics ships now.
- Any instrumentation beyond the single proof-of-life gauge.

## Isolation requirement (hard)

`otel-telemetry-enabled` and `telemetry-enabled` are strictly independent
features. The two pipelines are not expected to share a backend. Concretely:

- Separate config structs. `OtelConfig` is a sibling of `TelemetryConfig`, never a
  field on it.
- `otel::init` takes `&OtelConfig` only and must never read `TelemetryConfig`.
- No endpoint fallback between them. `otel-endpoint` never defaults to
  `DEFAULT_TELEMETRY_ENDPOINT` (nova).
- Enabling or disabling one has no effect on the other.
- The only shared code is the test-harness detection helper, a free function.

## Configuration

New `OtelArgs` (clap + serde) and `OtelConfig` (resolved), beside
`TelemetryArgs`/`TelemetryConfig` in `crates/core/src/config.rs`:

| Key | Env | Default | Meaning |
|---|---|---|---|
| `otel-telemetry-enabled` | `FREENET_OTEL_TELEMETRY_ENABLED` | `false` | Enable the SDK metrics exporter |
| `otel-endpoint` | (see below) | none | OTLP/HTTP collector base URL |

Default is `false`: nothing is exported yet, so off is the no-behavior-change
default. Operators opt in.

### Endpoint precedence

`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` > `OTEL_EXPORTER_OTLP_ENDPOINT` >
`otel-endpoint` in config.toml > `http://localhost:4318` (the SDK's own default).

This is deliberately *not* what the SDK does by itself. In
`opentelemetry-otlp` 0.32, `resolve_http_endpoint`
(`src/exporter/http/mod.rs:719-749`) gives a programmatic `with_endpoint` value
**priority over both env vars**, and uses it **verbatim** — `build_endpoint_uri`
appends `/v1/metrics` only on the env-var path. So to get env-wins precedence the
code must:

- call `with_endpoint` only when neither env var is set, and
- append `/v1/metrics` itself when passing the config-file value.

`otel-endpoint` therefore has no clap `env =` binding — binding it would merge the
standard variable into the config layer and invert the precedence.

Every other standard variable — `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`,
`OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_TIMEOUT`,
`OTEL_EXPORTER_OTLP_COMPRESSION`, `OTEL_METRIC_EXPORT_INTERVAL` (default 60s,
`opentelemetry_sdk/src/metrics/periodic_reader.rs:24-43`) — is read by the SDK.
No code for them.

## Dependencies

`opentelemetry` is already non-optional in `crates/core/Cargo.toml`.
`opentelemetry_sdk` and `opentelemetry-otlp` are optional and reachable only
through the `trace-ot` feature; both are already in `Cargo.lock`. Making them
non-optional is the whole dependency change — their default features already
cover what is needed:

- `opentelemetry-otlp` defaults: `http-proto`, `reqwest-blocking-client`,
  `metrics`, `trace`, `logs`, `internal-logs`.
- `opentelemetry_sdk` defaults: `metrics`, `trace`, `logs` (workspace decl adds
  `rt-tokio`).

`reqwest-blocking-client` is load-bearing, not incidental: `PeriodicReader` runs
exports on a dedicated thread through `futures_executor::block_on`
(`periodic_reader.rs:419`). An async reqwest client on that thread has no tokio
reactor and would fail at export time.

Because a feature array may not name a non-optional dependency, `trace-ot` drops
`"opentelemetry-otlp"` from its list.

## Suppression

`otel::init` returns `None` — no provider, no exporter, no global registration —
when any of these hold, mirroring `telemetry_suppression_reason`
(`telemetry.rs:660-679`):

1. `!cfg.enabled`
2. `cfg.is_test_environment` (the `--id` flag)
3. `cfg!(test)`
4. `running_under_cargo_test()` (executable's parent dir is `deps/`)

Same rationale as #4366: keyed only on signals a real release binary never trips,
and deliberately not on `cfg!(feature = "testing")`, which leaks onto the shipped
binary via Cargo feature unification with `fdev`.

The decision lives in a pure function so both directions are unit-testable from
inside a test process, which by construction trips signals 3 and 4.

## Pipeline

`crates/core/src/tracing/otel.rs`:

```
MetricExporter::builder().with_http()[.with_endpoint(resolved)].build()
  → SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(Resource::builder()
            .with_service_name("freenet-peer")
            .with_attribute(KeyValue::new("peer.id", local_peer_id))
            .build())
        .build()
  → opentelemetry::global::set_meter_provider(provider.clone())
```

Exporter build failure logs a WARN and returns `None`. Metrics export must never
fail node startup.

No wrapper type, no registry, no facade. Future instrumentation is
`opentelemetry::global::meter("freenet").u64_counter(…)` at the call site.

### Proof-of-life metric

One observable gauge, `freenet.process.memory.rss`, over
`crate::node::resource_metrics::rss_bytes()` (already exists,
`node/resource_metrics.rs:79`). A real datapoint end to end, and the thing to look
at in a collector to confirm the pipeline works.

### Shutdown

Not wired. `global::set_meter_provider` holds a reference for the process
lifetime, and `PeriodicReader` exports every 60s, so at most one partial interval
is lost at exit. Flushing on the signal path is plumbing this does not need yet;
the code carries a `ponytail:` comment naming the ceiling and the upgrade path.

## Wire-up

`crates/core/src/node.rs`, in `build_with_flush_handle` beside the
`TelemetryReporter::new` call (`node.rs:754`):

```rust
otel::init(&self.config.otel, self.local_peer_id_string());
```

The provider is registered globally; nothing is stored on `Node`.

## Testing

- Endpoint precedence: metrics env > generic env > config > default. Pure
  function, no network.
- Suppression: the pure decision function returns "export" only for a
  production-shaped input, and a reason for each of disabled / `--id` /
  `cfg(test)` / cargo-`deps` harness. Both directions.
- Isolation: enforced structurally — the otel decision function takes
  `&OtelConfig` and cannot reach `TelemetryConfig` — plus a review check that
  the diff touches `telemetry.rs` in exactly one line (the `pub(crate)`
  widening of the harness detector).
- CLI/env parsing: `--otel-telemetry-enabled=false` parses as false. A bare
  clap flag with an `env` binding treats any value of the variable as true,
  which would turn the exporter on for an operator trying to turn it off.
- Provider construction succeeds inside a tokio runtime against an unreachable
  endpoint (guards the reqwest-blocking-in-async-context concern; export failure
  is asynchronous and must not surface at build time).
- Config round-trip through `ConfigArgs::build()` — mandatory per
  `.claude/rules/code-style.md`.

## Risks

- Making the two crates non-optional grows the default build. They do NOT
  share reqwest with the existing HTTP client: `opentelemetry-otlp` pulls
  reqwest 0.13, a separate major version from the workspace's reqwest 0.12.
  `opentelemetry-otlp` is trimmed to `default-features = false` plus exactly
  the metrics/http-proto/reqwest-blocking-client/reqwest-rustls/internal-logs
  features, so the trace/logs exporters (explicit non-goals) and their
  prost/opentelemetry-proto deps stay out of the default build. The bounded
  remaining cost is a second reqwest major version plus the metrics-only OTLP
  exporter — confirm cross-compile targets still build
  (`.github/workflows/cross-compile.yml`,
  `crates/core/tests/cross_compile_feature_split.rs`).
- `global::set_meter_provider` is process-global. A `trace-ot` build also sets
  OTel globals (tracer provider, not meter provider) — confirm no conflict.
- `reqwest/blocking` gets enabled workspace-wide by feature unification, which
  pulls in a background runtime thread for blocking clients.

## Process note

This is a feature, not a bug fix. Per
[CONTRIBUTING.md](../../CONTRIBUTING.md) it needs a maintainer-approved issue
before implementation starts.
