# Design: OpenTelemetry metrics exporter (isolated from existing telemetry)

Status: implemented (`crates/core/src/tracing/otel.rs`).
Operator-facing configuration: [`docs/otel-metrics.md`](../otel-metrics.md).

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
- Per-connection metrics. Identifying the remote end of a connection is a
  per-datapoint attribute, multiplied by bucket count on histograms; the
  aggregate signals below answer the operational questions without it. Transfer
  and connection *events*, if wanted, belong on a log pipeline rather than as
  metric series.
- Operation and contract-execution latency histograms. No driver measures its
  own elapsed time today, and raw `Instant::now()` is banned in `crates/core/`
  (`.claude/rules/testing.md`), so adding them means threading `TimeSource`
  through every `op_ctx_task`. `freenet.operation.results` ships the outcome
  counter now; the duration histogram is deferred until someone needs
  percentiles.

## Instruments

Registered in `tracing/otel.rs::register_metrics`.

| Instrument | Kind | Attributes | Source |
|---|---|---|---|
| `freenet.process.memory.rss` | gauge | — | `node::resource_metrics::rss_bytes` (Linux only) |
| `freenet.transport.bytes` | counter | `direction` | `cumulative_bytes_{sent,received}` |
| `freenet.transport.packets` | counter | `direction` | `cumulative_packets_{sent,received}` |
| `freenet.transport.transfers` | counter | `result` | `cumulative_transfers` |
| `freenet.transport.nat_traversal` | counter | `result` | `cumulative_nat_traversal` |
| `freenet.transport.rtt` | histogram | — | `record_rtt_sample` |
| `freenet.transport.cwnd` | histogram | — | `record_cwnd_sample` |
| `freenet.operation.results` | counter | `op`, `result` | `NetworkStatus::op_stats` |
| `freenet.ring.connections` | gauge | — | `RingStatsSnapshot` |
| `freenet.node.contracts.hosted` | gauge | `reason` | `HostingReasonStats` |
| `freenet.node.contracts.hosted.bytes` | gauge | `reason` | `HostingReasonStats` |
| `freenet.connect.gateway_failures` | counter | — | `NetworkStatus` |
| `freenet.ring.lattice.neighbor` | gauge | `position` | `RingStatsSnapshot` |
| `freenet.ring.lattice.neighbor.distance` | gauge | `position` | `RingStatsSnapshot` |
| `freenet.ring.lattice.probes` | counter | `result` | `RingStatsSnapshot` |
| `freenet.contract.updates` | counter | `result` | `RingStatsSnapshot` |
| `freenet.contract.queue.depth` | gauge | `queue` | `FairQueueStats` |
| `freenet.contract.queue.depth.high_water` | gauge | — | `FairQueueStats` |
| `freenet.contract.queue.rejected` | counter | `reason` | `FairQueueStats` |
| `freenet.contract.queue.background_shed` | counter | — | `FairQueueStats` |

Everything but the two histograms is an observable callback over state that
already existed for the local dashboard.

The two histograms are the ONLY synchronous instruments, and that is a rule
rather than an accident: a counter is always exported as an observable reading
the cumulative atomic the measured code already keeps, so there is no second
call site to forget when a new one is added. A hand-placed `otel::record_*`
mirror next to an existing `fetch_add` is the failure class of #4009 / #4010,
where mirrored counters rotted and read zero for months
(`.claude/rules/bug-prevention-patterns.md`). A histogram gets no such
treatment because a sum-and-count atomic cannot reconstruct a distribution;
those two call sites are pinned, bounded to their function, by
`every_sync_instrument_still_has_its_hot_path_mirror`.

Where a period counter already existed (`transfers_*`, `nat_traversal_*`),
the observable reads a NEW cumulative counter incremented in the same
function, not the period one: `take_snapshot` swaps the period counters to
zero for the legacy telemetry worker, so observing those as counters would
produce a non-monotonic series on any node running both pipelines. Pinned by
`cumulative_outcome_counters_survive_a_snapshot_reset`.

### `reason` on the hosted-contract gauges

`freenet.node.contracts.hosted` and `.hosted.bytes` answer "how much are we
holding, and *why*". The `reason` values come from `ring::HostingReason` and
are a **partition**: the classifier assigns each hosted contract to the first
matching bucket in priority order, so `sum by (reason)` is the hosted-contract
count and the byte gauge sums to the hosting cache's used bytes. Neither gauge
emits an un-attributed total — that would double-count under `sum`.

| `reason` | held because |
|---|---|
| `local_client` | a local client (WebSocket/HTTP) holds a subscription |
| `downstream` | a downstream peer subscribes to us — we relay its updates |
| `subscribed` | unexpired network subscription, no local or downstream reader |
| `local_access` | no subscription, but a local client GET/PUT touched it *recently* |
| `abandoned` | was in use and no longer is — the eviction-candidate pool |
| `restored` | reloaded from persisted metadata at startup, unread since |
| `routed` | residual: arrived via a routed GET/PUT, no demand signal |

The strings are a metrics contract — collector-side dashboards filter on them,
so add variants rather than repurpose existing values.

Two of these are easy to get subtly wrong, so they are spelled out:

- `local_access` is gated on the same age window the hosting policy uses
  (`has_recent_local_client_access`, `SUBSCRIPTION_LEASE_DURATION`), NOT on the
  sticky `local_client_access` flag, which is set once and never cleared.
  Classifying on the flag would make the bucket monotonically absorb every
  contract a client ever touched over a node's uptime, while `routed` drained
  into it — and would disagree with the policy the gauge exists to describe.
- `restored` exists because the restore path resets `abandoned_at` to `None`.
  Without it, a restart silently empties `abandoned` into `routed` and every
  reloaded contract claims to have arrived through a routed GET/PUT. It is the
  demand-side view of `HostingCause::StartupRestore`.

`HostingReason` (this table) and `HostingCause` (`host_begin` in the local
telemetry pipeline) are different questions in different tenses — current
demand, re-derived every collection, versus provenance frozen at admission.
A contract admitted as `TransitGet` shows up here as `local_client` the moment
a local client subscribes. Both rustdocs cross-reference each other.

The breakdown has its own provider (`set_hosting_reason_provider`) rather than
riding `RingStatsSnapshot`: that provider runs on every dashboard HTTP request,
and this is an O(hosted) walk under the hosting-cache read lock. Its own
provider confines the cost to the OTel collection cadence. The bytes gauge
counts contract **state** only — no WASM blobs, no database overhead — matching
what the hosting cache's byte budget measures, so it is comparable against the
budget but not against on-disk usage.

Resource attributes: `freenet.node.pubkey` (the full base58 **x25519** transport
public key — byte-equal to the bearer token's `<pubkey>` field, so a collector
that verified the signature has also verified the node id),
`freenet.node.fingerprint` (the truncated form UIs show, recomputable from
`freenet.node.pubkey`, so the collector derives rather than trusts it),
`service.name`, `service.version`, `os.type`, `host.arch`. Never a `PeerId`,
which renders as `{pub_key}@{addr}` and would export our socket address.

The literals are applied only for keys the operator did **not** declare through
`OTEL_SERVICE_NAME` / `OTEL_RESOURCE_ATTRIBUTES`: `ResourceBuilder` seeds from
the environment and then merges `with_attribute` over that seed, so setting one
unconditionally would silently discard the operator's value. This deference
applies to the descriptive attributes only — the two `freenet.node.*` identity
attributes are always emitted, because they are what the collector checks the
bearer-token signature against, and an operator-supplied override would export
an identity that does not match the signing key.

Not instrumented: `TransportMetrics::slowdowns_triggered` is a period
accumulator that `take_snapshot` zeroes for the legacy telemetry worker, so
observing it as a counter would report a non-monotonic series whenever
`telemetry-enabled` is also on — the same hazard as the transport byte and
packet counters, which is why those are read from the cumulative totals
instead.

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
| `otel-auth-mode` | — | `disabled` | `freenet` (bearer token) or `disabled` (no header) |

Default is `false`: nothing is exported yet, so off is the no-behavior-change
default. Operators opt in.

`otel-telemetry-enabled` is `Option<bool>` in `OtelArgs` and has no clap
`default_value`, unlike its `telemetry-*` siblings. With a default, "unset" and
"explicitly false" are indistinguishable after parsing, so
`--otel-telemetry-enabled=false` could not override a `config.toml` that says
`true` — the off switch would not work.

### Collector authentication

`otel-auth-mode = "freenet"` puts a per-request

```
Authorization: Bearer freenet/<pubkey>/<audience>/<timestamp>/<signature>
```

on every export. `<signature>` is an XEdDSA (Signal construction, `xeddsa`
crate) signature over everything preceding it, made with the x25519 transport
secret itself (`TransportKeypair::auth_token_signer`) — the node's one
identity, no second key to cross-certify. `<pubkey>` and `<signature>` are
base58, `<timestamp>` is epoch seconds.

`<audience>` binds the token to the collector it was minted for: without it,
any collector we export to could replay our token at any other collector
accepting this scheme and impersonate the node. It is
**base58 of the first 16 bytes of `SHA-256(canonical target URL)`**, hashed
rather than sent literally because a URL contains `/`, the token's field
separator. The canonical form both sides must agree on:

- `{host}:{port}{path}`, e.g. `collector.example:4318/v1/metrics`
- host lowercased
- port always explicit, defaulting to 80 for an `http` URL and 443 for an `https` one
- path verbatim, no normalization
- userinfo stripped, query and fragment dropped
- **scheme not included**: it names a transport, not a party, so binding it
  would not narrow which collector may use the token, while forcing every
  collector reachable over both http and https to be configured twice. It does
  still reach the hash through the default-port rule, so `http://c/x` and
  `https://c/x` differ.

Stripping userinfo is load-bearing twice over: it keeps operator credentials
out of a signed, wire-visible field, and a collector that does not know the
password could not otherwise reproduce the hash.

Hashing the full URL rather than just the authority means two collectors
behind one hostname on different paths (`/tenant-a` vs `/tenant-b`) get
distinct audiences. The cost is diagnosability — a rejected token tells the
collector nothing about where the sender thought it was pointing — so the node
logs its resolved endpoint at startup and `docs/otel-metrics.md` documents the
computation for hand-checking a mismatch.

A collector verifies with a stock Ed25519 library after converting the
Montgomery public key to Edwards with sign bit 0, then checks `<audience>`
against the hash of each URL it answers at and `<timestamp>` against its own
clock.

**The signature covers the request body, which is not transmitted.** The
signing input is the token prefix plus `/<base58 SHA-256 of the body>`; the
token on the wire stops at the prefix, and the collector recomputes the hash
from the body it received. Without it a token authenticates only "this node
addressed this collector at this second", so anyone holding one could attach it
to a body of their own and have it accepted as this node's metrics — the exact
spoofing the scheme exists to stop. Keeping the hash off the wire leaves the
token at five fields and costs the collector one hash it was already able to
compute.

**Replay is bounded by the collector, and the bound is `REPLAY_WINDOW` (300s).**
The node cannot enforce it: it stamps `<timestamp>` and the collector checks it
against its own clock, rejecting anything outside the window and refusing a
`(pubkey, timestamp, body hash)` triple already accepted inside it. The
constant is declared in `tracing/otel.rs` and restated in `docs/otel-metrics.md`
because it is a wire contract — both sides have to agree on the number. This
was previously deferred as "specify a replay bound"; it is specified here.

**No credential goes out in cleartext, ours or the operator's.** A request
carrying either fails rather than being sent when the endpoint is plaintext
`http` to a non-loopback host, and this is also diagnosed at startup instead of
one latched WARN an export interval later. Two ambient mechanisms would
otherwise defeat that check, since it can only inspect the URI the exporter
aimed at: reqwest replays a redirect with the original headers, and defaults to
`auto_sys_proxy` with no loopback exemption of its own — so an `HTTP_PROXY` in
the environment would send an export aimed at `http://localhost:4318` to that
proxy, straight through the exemption. `export_http_client` disables both.

The guard is not `Authorization`-only. Every header the operator declared
through `OTEL_EXPORTER_OTLP_HEADERS` counts as a credential, because that is
where OTLP credentials live and `Authorization` is merely the most common
spelling — `x-honeycomb-team`, `api-key` and `dd-api-key` are all in ordinary
use. Those same values are redacted out of any error body before it is logged,
by value rather than by keyword: a collector answering
`unauthorized: key 'sk-abc123' rejected` contains neither `authorization` nor
`bearer `, and `freenet service report` uploads node logs wholesale.

The default is `disabled` — no `Authorization` header. Pointing the exporter at
your own collector must not ship a signed assertion of this node's identity
somewhere that never asked for one; `freenet` mode is for collectors that
actually verify these tokens. Either way, an `Authorization` header supplied
through `OTEL_EXPORTER_OTLP_HEADERS` is never overwritten — which is how static
header auth, the only other scheme in common use with OTLP, is configured
today, and the reason a second built-in auth mode is a separate change rather
than a blocker for this one.

To be precise about what `disabled` withholds: the SIGNATURE, not the identity.
`freenet.node.pubkey` / `.fingerprint` are resource attributes on every export
batch in both modes (`identity_attributes` is called before the auth-mode
match), because metrics with no node id to group by are not useful. `disabled`
means "make no cryptographic assertion of this identity to this collector", not
"conceal which node this is".

### Endpoint precedence

`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` > `OTEL_EXPORTER_OTLP_ENDPOINT` >
`otel-endpoint` in config.toml > `http://localhost:4318` (the SDK's own default).

This is deliberately *not* what the SDK does by itself. In
`opentelemetry-otlp` 0.32, `resolve_http_endpoint`
(`src/exporter/http/mod.rs:720-750`) gives a programmatic `with_endpoint` value
**priority over both env vars**, and uses it **verbatim** — `build_endpoint_uri`
appends `/v1/metrics` only on the env-var path. So to get env-wins precedence the
code must:

- call `with_endpoint` only when neither env var is set, and
- append `/v1/metrics` itself when passing the config-file value.

`otel-endpoint` therefore has no clap `env =` binding — binding it would merge the
standard variable into the config layer and invert the precedence.

Note the two endpoint variables are NOT interchangeable. `resolve_http_endpoint`
uses the signal-specific `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` **verbatim**
(`// per signal env var is not modified`); only the generic
`OTEL_EXPORTER_OTLP_ENDPOINT` and the built-in default go through
`build_endpoint_uri`. So the signal-specific variable must carry the full
`/v1/metrics` path and the generic one must not.

Most other standard variables — `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`,
`OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_METRIC_EXPORT_INTERVAL` (default 60s,
`opentelemetry_sdk/src/metrics/periodic_reader.rs:24-43`) — are read by the SDK.
No code for them. Two exceptions:

- `OTEL_EXPORTER_OTLP_TIMEOUT` / `OTEL_EXPORTER_OTLP_METRICS_TIMEOUT` are
  resolved by `otel::export_timeout`, not the SDK: the SDK applies its resolved
  timeout only to a client it builds itself, and we always supply one.
- `OTEL_EXPORTER_OTLP_COMPRESSION` is **not supported**. The exporter validates
  it at build time and hard-errors unless the `gzip-http` / `zstd-http` feature
  is enabled, which we deliberately do not enable. Setting it therefore fails
  the exporter build and the node runs with no metrics (with a WARN naming the
  cause). Enable the matching feature if compression is ever wanted.

## Dependencies

`opentelemetry` is already non-optional in `crates/core/Cargo.toml`.
`opentelemetry_sdk` and `opentelemetry-otlp` were optional and reachable only
through the `trace-ot` feature; both were already in `Cargo.lock`. Making them
non-optional, with `opentelemetry-otlp` trimmed to `http-proto` + `metrics` +
`internal-logs`, is the dependency change. Net new packages in `Cargo.lock`:
`xeddsa` and its `convert_case`.

**No `reqwest-*-client` and no `reqwest-rustls` feature.** `tracing::otel`
always supplies its own `HttpClient`, and `opentelemetry-otlp` builds a client
only when none was given, so those features would be dead weight with a real
cost: they pull `opentelemetry-http`'s reqwest **0.13**, whose `rustls` feature
is hardwired to `aws-lc-rs` + `rustls-platform-verifier`. That is a second
reqwest major, a second TLS root store, a C/assembly `aws-lc-sys` build (via
`cc`+`cmake`, on musl and Windows release targets that install neither), and
two crypto providers inside one rustls — which makes
`CryptoProvider::get_default_or_install_from_crate_features` return `None` and
panic for any caller that does not pass a provider explicitly. Note that
switching to `reqwest-rustls-webpki-roots` does **not** avoid this: it still
enables `reqwest/default-tls`, i.e. the same aws-lc stack, and merely adds
webpki roots on top.

Our client is blocking on purpose: `PeriodicReader` runs exports on a dedicated
thread through `futures_executor::block_on` (`periodic_reader.rs:419`), where
an async reqwest client has no tokio reactor. It rides the workspace reqwest
0.12, which already carries `rustls-tls`, so `https://` endpoints work with
nothing new in the graph. It also applies the export timeout
(`OTEL_EXPORTER_OTLP_METRICS_TIMEOUT` > `OTEL_EXPORTER_OTLP_TIMEOUT` > 10s)
itself, because the SDK only resolves that for a client it built.

Because a feature array may not name a non-optional dependency, `trace-ot` drops
`"opentelemetry-otlp"` from its list.

## Suppression

`otel::init` returns `Some(reason)` — no provider, no exporter, no global
registration — when any of these hold, mirroring `telemetry_suppression_reason`
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
MetricExporter::builder().with_http()[.with_endpoint(resolved)]
    .with_http_client(OtlpHttpClient { .. })     // always ours; signs when auth is on
    .build()
  → SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(Resource::builder()
            .with_service_name("freenet-node")   // only if OTEL_* did not set it
            .with_attribute(KeyValue::new("freenet.node.pubkey", pubkey))
            .with_attribute(KeyValue::new("freenet.node.fingerprint", fingerprint))
            .build())
        .with_view(/* base-2 exponential for every histogram */)
        .build()
  → opentelemetry::global::set_meter_provider(provider)
```

Exporter build failure logs a WARN and the node starts anyway. Metrics export
must never fail node startup.

The whole build runs on a plain `std::thread`: `reqwest::blocking::Client` owns
a private tokio runtime, and creating or dropping one inside an async context
panics with "Cannot drop a runtime in a context where blocking is not allowed".
`init` is called from the node's async build path.

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
the code carries a `NOTE:` comment naming the ceiling and the upgrade path —
including the trap on that path: `shutdown()` drops the blocking reqwest client
and its private tokio runtime, so calling it directly from an async fn panics
with "Cannot drop a runtime in a context where blocking is not allowed". It has
to go through `spawn_blocking`, the same hop `build_provider` makes inbound.

## Wire-up

`crates/core/src/node.rs`, in `build_with_flush_handle` beside the
`TelemetryReporter::new` call (`node.rs:815`):

```rust
crate::tracing::otel::init(&self.config.otel, &self.key_pair);
```

The transport keypair, not a `PeerId`: it yields both identity resource
attributes and, in `freenet` auth mode, the token signing key. The provider is
registered globally; nothing is stored on `Node`. `init` returns the
suppression reason (or `None` when it started) so a test can prove it consults
that check before building anything — `init` is otherwise unreachable under
`cfg(test)`.

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
  which would turn the exporter on for an operator trying to turn it off. It
  also overrides a `config.toml` that says `true`, asserted through
  `ConfigArgs::build()` rather than at parse level.
- Auth: token shape and stock-Ed25519 verification; a token re-aimed at another
  collector fails to verify; the header reaches a real socket in `freenet`
  mode, is absent in `disabled` mode, and never replaces an operator-supplied
  `Authorization`.
- Instruments actually run: `instrument_callbacks_export_named_datapoints`
  builds a provider over `InMemoryMetricExporter`, registers every instrument
  against it, collects once, and asserts the exported datapoints by name and by
  attribute. `init` returns early under `cfg(test)`, so without this no
  callback, name, unit or attribute is ever executed in CI — and a panic in any
  callback kills the `PeriodicReader` thread and stops all metrics permanently,
  with no export-side signal to report it. It deliberately uses a LOCAL
  provider: neither `global::set_meter_provider` nor the `INSTRUMENTS` OnceLock
  is touched, both being process-global state that would leak into every other
  test in the binary under plain `cargo test`.
- Pins that would otherwise be vacuous: `init` returns a suppression reason
  under `cfg(test)` (deleting the check makes it build a pipeline instead), and
  a cross-file scrape asserts each histogram's `record_*` mirror still sits
  inside the function that takes the sample. The scrape is BOUNDED to that
  function (a `method_body` helper, the indented sibling of `fn_body` in
  `bin/commands/auto_update.rs`): an unbounded `contains` does not fail when
  the call MOVES, it matches a later occurrence — typically the pin's own
  assertion string — and passes vacuously. Mutation-tested when written.
- Provider construction succeeds inside a tokio runtime against an unreachable
  endpoint (guards the reqwest-blocking-in-async-context concern; export failure
  is asynchronous and must not surface at build time).
- Config round-trip through `ConfigArgs::build()` — mandatory per
  `.claude/rules/code-style.md`.

## Risks

- Making the two crates non-optional grows the default build: the trim drops
  the logs exporter only, since `http-proto` mandates `trace`, `prost` and
  `opentelemetry-proto`. No new native toolchain requirement — see
  Dependencies — so the musl/Windows release targets in
  `.github/workflows/cross-compile.yml` (which never runs on PRs) build the
  same way they did before.
- `global::set_meter_provider` is process-global. A `trace-ot` build also sets
  OTel globals (tracer provider, not meter provider).
- `reqwest/blocking` gets enabled workspace-wide by feature unification, which
  pulls in a background runtime thread for blocking clients.
- Six new never-reset `AtomicU64`s on `TransportMetrics` shadow existing period
  counters. That is the deliberate trade against a hot-path `otel::record_*`
  mirror at each site: 48 bytes on one process-global struct, in exchange for
  removing a class of silent drift.

## Process note

This is a feature, not a bug fix. Per
[CONTRIBUTING.md](../../CONTRIBUTING.md) it needs a maintainer-approved issue
before implementation starts — see #5046.
