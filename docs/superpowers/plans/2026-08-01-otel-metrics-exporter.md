# OpenTelemetry Metrics Exporter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a standards-configured OpenTelemetry SDK metrics pipeline to `freenet`, strictly isolated from the existing `telemetry-enabled` reporter, so real metrics can be exported to any OTLP collector.

**Architecture:** A new `OtelArgs`/`OtelConfig` config pair (sibling of, never nested in, `TelemetryArgs`/`TelemetryConfig`) gates a new `crates/core/src/tracing/otel.rs` module. That module builds an OTLP/HTTP `MetricExporter` and an `SdkMeterProvider`, registers it as the process-global meter provider, and registers one observable RSS gauge as proof of life. The endpoint is resolved env-first, which requires working *around* `opentelemetry-otlp`'s own precedence. Everything in `telemetry.rs` is untouched.

**Tech Stack:** Rust, clap + serde config, `opentelemetry` / `opentelemetry_sdk` / `opentelemetry-otlp` 0.32, OTLP over HTTP/protobuf with the blocking reqwest client.

**Design spec:** [`docs/design/otel-metrics-exporter.md`](../../design/otel-metrics-exporter.md)

## Global Constraints

- **This is a feature, not a bug fix.** Per `CONTRIBUTING.md` a maintainer-approved issue MUST exist before implementation starts. Do not open a PR without it.
- Branch name: `feat/otel-metrics-exporter`.
- Conventional-commit subjects, under 72 chars, body explains WHY (`.claude/rules/git-workflow.md`).
- Before every commit: `cargo fmt` and `cargo clippy -p freenet -- -D warnings`. CI treats any warning as failure.
- No behavior change to `TelemetryReporter`, `to_otlp_logs`, the `/v1/logs` path, `telemetry-enabled`, or `telemetry-endpoint`. If a diff touches those, it is wrong.
- `otel::init` and everything it calls must never read `TelemetryConfig`.
- `otel-endpoint` must NEVER fall back to `DEFAULT_TELEMETRY_ENDPOINT` (`http://nova.locut.us:4318`). Its default is `http://localhost:4318`.
- Default of `otel-telemetry-enabled` is `false`.
- Crate under test is `freenet` (`crates/core`). Unit tests run with `cargo test -p freenet --lib <path>`.
- Production code in `crates/core/` uses `TimeSource` for time and `GlobalRng` for randomness (`.claude/rules/code-style.md`). Neither is needed here — do not introduce `Instant::now()` or `rand::random()`.
- Any deliberate simplification gets a `ponytail:` comment naming the ceiling and the upgrade path.

---

### Task 1: Config — `OtelArgs` / `OtelConfig`

**Files:**
- Modify: `crates/core/src/config.rs` (add structs after `TelemetryConfig`'s helpers ~line 2237; add `ConfigArgs` field ~line 236; add `ConfigArgs::default()` entry ~line 294; add merge block in `build()` ~line 629; add `Config` field ~line 1312; add `Config` construction in `build()` ~line 1109)
- Test: `crates/core/src/config.rs` (the `mod tests` block — new tests plus the existing guard at line 5241)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `pub const DEFAULT_OTEL_ENDPOINT: &str = "http://localhost:4318";`
  - `pub struct OtelArgs { pub enabled: bool, pub endpoint: Option<String> }`
  - `pub struct OtelConfig { pub enabled: bool, pub endpoint: Option<String>, pub is_test_environment: bool }`
  - `Config::otel: OtelConfig`, `ConfigArgs::otel: OtelArgs`

- [ ] **Step 1: Write the failing tests**

Add to the `mod tests` block in `crates/core/src/config.rs`:

```rust
#[test]
fn otel_args_default_is_off_and_endpointless() {
    // The new pipeline exports nothing yet, so shipping it on would be a
    // behavior change. Operators opt in explicitly.
    let args = OtelArgs::default();
    assert!(!args.enabled, "otel-telemetry-enabled must default to false");
    assert_eq!(args.endpoint, None, "no implicit collector");
}

#[test]
fn otel_flag_parses_from_cli() {
    use clap::Parser;
    let none = ConfigArgs::try_parse_from(["freenet"]).expect("bare parse");
    assert!(!none.otel.enabled, "no flag -> off");
    let set = ConfigArgs::try_parse_from(["freenet", "--otel-telemetry-enabled"])
        .expect("flag parse");
    assert!(set.otel.enabled, "--otel-telemetry-enabled -> on");
    // Explicit `=false` must parse and mean false. Without this form the flag
    // would be a bare ArgAction::SetTrue, and clap turns ANY value of the bound
    // env var — including "false" — into true.
    let off = ConfigArgs::try_parse_from(["freenet", "--otel-telemetry-enabled=false"])
        .expect("explicit false parse");
    assert!(!off.otel.enabled, "--otel-telemetry-enabled=false -> off");
    let with_ep = ConfigArgs::try_parse_from([
        "freenet",
        "--otel-endpoint",
        "http://collector.example:4318",
    ])
    .expect("endpoint parse");
    assert_eq!(
        with_ep.otel.endpoint.as_deref(),
        Some("http://collector.example:4318")
    );
}

#[test]
fn otel_endpoint_never_defaults_to_the_dashboard_collector() {
    // Hard isolation requirement: the two pipelines share no backend.
    assert_ne!(
        DEFAULT_OTEL_ENDPOINT, DEFAULT_TELEMETRY_ENDPOINT,
        "otel must not default to the central dashboard collector"
    );
    assert_eq!(DEFAULT_OTEL_ENDPOINT, "http://localhost:4318");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p freenet --lib config::tests::otel_ -- --nocapture`
Expected: FAIL — `cannot find type OtelArgs in this scope`, `cannot find value DEFAULT_OTEL_ENDPOINT`.

- [ ] **Step 3: Add the config types**

Insert after `fn default_iface_tx_enabled()` (~line 2237) in `crates/core/src/config.rs`:

```rust
/// Default OTLP/HTTP endpoint for the SDK metrics pipeline, used when neither
/// the standard `OTEL_EXPORTER_OTLP_*` env vars nor `otel-endpoint` are set.
///
/// Deliberately NOT `DEFAULT_TELEMETRY_ENDPOINT`: `otel-telemetry-enabled` and
/// `telemetry-enabled` are strictly isolated features that are not expected to
/// share a backend. Pointing this pipeline at the central dashboard collector
/// must always be an explicit operator choice.
pub const DEFAULT_OTEL_ENDPOINT: &str = "http://localhost:4318";

/// CLI/file args for the OpenTelemetry SDK metrics exporter.
///
/// Strictly independent of [`TelemetryArgs`]: no shared field, no shared
/// default, no fallback in either direction.
#[derive(clap::Parser, Debug, Clone, Default, Serialize, Deserialize)]
pub struct OtelArgs {
    /// Enable the OpenTelemetry SDK metrics exporter. Independent of
    /// `telemetry-enabled`; enabling or disabling one has no effect on the
    /// other.
    ///
    /// `num_args`/`default_missing_value` rather than a bare flag: with an
    /// `env` binding, clap's `SetTrue` action treats ANY value of the variable
    /// as true, so `FREENET_OTEL_TELEMETRY_ENABLED=false` would silently turn
    /// the exporter ON. This form accepts `--otel-telemetry-enabled`,
    /// `--otel-telemetry-enabled=false`, and a properly parsed env value.
    #[arg(
        long = "otel-telemetry-enabled",
        env = "FREENET_OTEL_TELEMETRY_ENABLED",
        num_args = 0..=1,
        default_value = "false",
        default_missing_value = "true",
        action = clap::ArgAction::Set
    )]
    #[serde(rename = "otel-telemetry-enabled", default)]
    pub enabled: bool,

    /// OTLP/HTTP collector base URL (e.g. `http://collector:4318`).
    ///
    /// No clap `env =` binding on purpose. The standard
    /// `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`
    /// variables must take priority over this file-level value, and binding
    /// them here would merge them into the config layer and invert that
    /// precedence. They are resolved in `tracing::otel` instead.
    #[arg(long = "otel-endpoint")]
    #[serde(rename = "otel-endpoint", skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

/// Resolved configuration for the OpenTelemetry SDK metrics exporter.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OtelConfig {
    /// Whether the SDK metrics exporter is enabled.
    #[serde(default, rename = "otel-telemetry-enabled")]
    pub enabled: bool,

    /// Operator-configured OTLP/HTTP collector base URL, if any. `None` means
    /// "let the SDK resolve it" — see `tracing::otel::resolve_metrics_endpoint`.
    #[serde(default, rename = "otel-endpoint", skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// Whether this is a test environment (detected via `--id`). Mirrors
    /// [`TelemetryConfig::is_test_environment`]; suppresses export so test
    /// networks can't ship data to a collector.
    #[serde(skip)]
    pub is_test_environment: bool,
}
```

- [ ] **Step 4: Hang the args off `ConfigArgs`**

In `crates/core/src/config.rs`, after the `pub telemetry: TelemetryArgs,` field (~line 236):

```rust
    #[command(flatten)]
    pub otel: OtelArgs,
```

And in `impl Default for ConfigArgs`, after `telemetry: Default::default(),` (~line 294):

```rust
            otel: Default::default(),
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p freenet --lib config::tests::otel_ -- --nocapture`
Expected: PASS (3 tests).

- [ ] **Step 6: Write the failing round-trip test**

In `crates/core/src/config.rs`, extend the existing guard test
`all_persisted_config_fields_round_trip_through_build` (line 5241).

In the `seed` literal, after the `telemetry: TelemetryConfig { … },` block (~line 5323):

```rust
            otel: OtelConfig {
                enabled: true,
                endpoint: Some("http://example.invalid:4319".to_string()),
                is_test_environment: false, // #[serde(skip)] — derived from --id
            },
```

In the exhaustive `let Config { … } = rebuilt;` destructure, after `telemetry,` (~line 5357):

```rust
            otel,
```

And after the `shutdown_drain_secs` assertion (~line 5403):

```rust
        assert_eq!(otel.enabled, seed.otel.enabled, "otel.enabled");
        assert_eq!(
            otel.endpoint, seed.otel.endpoint,
            "otel.endpoint — an operator's collector URL must survive the \
             config.toml merge"
        );
```

- [ ] **Step 7: Run it to verify it fails**

Run: `cargo test -p freenet --lib config::tests::all_persisted_config_fields_round_trip_through_build`
Expected: FAIL to COMPILE — `struct Config has no field named otel`.

- [ ] **Step 8: Add the `Config` field, the merge, and the construction**

Three edits in `crates/core/src/config.rs`.

(a) On `pub struct Config`, after `pub telemetry: TelemetryConfig,` (~line 1312):

```rust
    /// OpenTelemetry SDK metrics exporter settings. Strictly isolated from
    /// `telemetry` above — see `docs/design/otel-metrics-exporter.md`.
    #[serde(default)]
    pub otel: OtelConfig,
```

(b) In `ConfigArgs::build()`, inside the `if let Some(cfg) = …` merge block, after the `iface_tx_enabled` merge (~line 629):

```rust
            // otel-telemetry-enabled defaults to false via clap, so only the
            // file-says-true direction needs handling — same one-directional
            // override as reference-ping/iface-tx above. Kept separate from the
            // telemetry merge on purpose: the two features are independent.
            if cfg.otel.enabled {
                self.otel.enabled = true;
            }
            if let Some(endpoint) = cfg.otel.endpoint {
                self.otel.endpoint.get_or_insert(endpoint);
            }
```

(c) In the `Config { … }` literal in `build()`, after the `telemetry: TelemetryConfig { … },` block (~line 1109):

```rust
            otel: OtelConfig {
                enabled: self.otel.enabled,
                endpoint: self.otel.endpoint,
                // Same --id rule as telemetry: simulated networks and
                // integration tests must not ship data to a collector.
                is_test_environment: self.id.is_some(),
            },
```

- [ ] **Step 9: Run the round-trip test to verify it passes**

Run: `cargo test -p freenet --lib config::tests::all_persisted_config_fields_round_trip_through_build`
Expected: PASS.

- [ ] **Step 10: Run the whole config module and lint**

Run: `cargo test -p freenet --lib config::`
Expected: PASS, no regressions.

Run: `cargo fmt && cargo clippy -p freenet -- -D warnings`
Expected: clean.

- [ ] **Step 11: Commit**

```bash
git add crates/core/src/config.rs
git commit -m "feat(otel): add isolated otel-telemetry config

New OtelArgs/OtelConfig sit beside TelemetryArgs/TelemetryConfig rather
than inside them: the SDK metrics pipeline and the dashboard reporter are
independent features that are not expected to share a backend, so neither
enable-flag nor endpoint may fall back to the other."
```

---

### Task 2: Pure decision functions in `tracing::otel`

**Files:**
- Create: `crates/core/src/tracing/otel.rs`
- Modify: `crates/core/src/tracing.rs` (add module declaration next to `pub mod telemetry;`, ~line 42)
- Modify: `crates/core/src/tracing/telemetry.rs:615` (widen `running_under_cargo_test` to `pub(crate)`)
- Test: `crates/core/src/tracing/otel.rs` (inline `mod tests`)

**Interfaces:**
- Consumes: `crate::config::OtelConfig` from Task 1.
- Produces:
  - `pub(crate) enum OtelSuppression { Disabled, TestEnvironmentFlag, TestHarness }`
  - `pub(crate) fn otel_suppression_reason(cfg: &OtelConfig, is_test_build: bool, running_under_cargo_test: bool) -> Option<OtelSuppression>`
  - `pub(crate) fn resolve_metrics_endpoint(cfg_endpoint: Option<&str>, metrics_env: Option<&str>, generic_env: Option<&str>) -> Option<String>` — `None` means "let the SDK resolve it".

- [ ] **Step 1: Write the failing tests**

Create `crates/core/src/tracing/otel.rs` containing ONLY this test module for now:

```rust
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
```

- [ ] **Step 2: Declare the module and run the tests to verify they fail**

In `crates/core/src/tracing.rs`, after `pub use telemetry::TelemetryReporter;` (~line 43):

```rust
/// Standards-configured OpenTelemetry SDK metrics pipeline. Strictly isolated
/// from `telemetry` above — see `docs/design/otel-metrics-exporter.md`.
pub mod otel;
```

Run: `cargo test -p freenet --lib tracing::otel`
Expected: FAIL — `cannot find function otel_suppression_reason in this scope`.

- [ ] **Step 3: Write the implementation**

Prepend to `crates/core/src/tracing/otel.rs`, above the test module:

```rust
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p freenet --lib tracing::otel`
Expected: PASS (5 tests).

- [ ] **Step 5: Widen the shared harness detector**

In `crates/core/src/tracing/telemetry.rs`, change line 615 from:

```rust
fn running_under_cargo_test() -> bool {
```

to:

```rust
pub(crate) fn running_under_cargo_test() -> bool {
```

Leave its doc comment unchanged. This is the only code shared between the two
pipelines — a free function with no config in it.

- [ ] **Step 6: Verify the crate still builds and lints**

Run: `cargo test -p freenet --lib tracing:: && cargo fmt && cargo clippy -p freenet -- -D warnings`
Expected: PASS, clean.

- [ ] **Step 7: Commit**

```bash
git add crates/core/src/tracing.rs crates/core/src/tracing/otel.rs crates/core/src/tracing/telemetry.rs
git commit -m "feat(otel): add suppression and endpoint-precedence logic

Both decisions are pure functions so the production direction is testable
from inside a test process. Endpoint resolution deliberately returns None
when a standard OTEL_* var is set: opentelemetry-otlp gives a programmatic
endpoint priority over the env vars, which is the opposite of the
precedence operators expect."
```

---

### Task 3: Exporter, meter provider, RSS gauge, and node wire-up

**Files:**
- Modify: `crates/core/Cargo.toml` (deps ~lines 129-130, `trace-ot` feature ~line 233)
- Modify: `crates/core/src/tracing/otel.rs` (add `init`, `build_provider`, `register_process_metrics`)
- Modify: `crates/core/src/node.rs` (~line 754, beside the `TelemetryReporter::new` call)
- Test: `crates/core/src/tracing/otel.rs` (inline `mod tests`)

**Interfaces:**
- Consumes: `otel_suppression_reason`, `resolve_metrics_endpoint` (Task 2); `OtelConfig` (Task 1); `crate::node::resource_metrics::rss_bytes() -> Option<u64>` (existing, `node/resource_metrics.rs:79`); `NodeConfig::local_peer_id_string() -> String` (existing, `node.rs:441`).
- Produces:
  - `pub fn init(config: &OtelConfig, local_peer_id: String)`
  - `pub(crate) fn build_provider(endpoint: Option<&str>, local_peer_id: String) -> Result<SdkMeterProvider, opentelemetry_otlp::ExporterBuildError>`

- [ ] **Step 1: Make the OTel crates non-optional**

In `crates/core/Cargo.toml`, change lines 129-130 from:

```toml
opentelemetry-otlp = { workspace = true, optional = true }
opentelemetry_sdk = { workspace = true, optional = true }
```

to:

```toml
# Non-optional: the SDK metrics pipeline (tracing::otel) ships in every build.
# Default features already give us metrics + http-proto + reqwest-blocking-client.
# The blocking client is load-bearing, not incidental: PeriodicReader exports
# from a dedicated thread via futures_executor::block_on, where an async reqwest
# client would have no tokio reactor.
opentelemetry-otlp = { workspace = true }
opentelemetry_sdk = { workspace = true }
```

And on line 233, drop `"opentelemetry-otlp"` from the feature list (a feature
array may not name a non-optional dependency):

```toml
trace-ot = ["opentelemetry-jaeger", "trace", "tracing-opentelemetry"]
```

- [ ] **Step 2: Verify the crate still builds**

Run: `cargo build -p freenet`
Expected: SUCCESS.

Run: `cargo test -p freenet --test cross_compile_feature_split`
Expected: PASS.

- [ ] **Step 3: Write the failing test**

Add to the `mod tests` block in `crates/core/src/tracing/otel.rs`:

```rust
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
```

- [ ] **Step 4: Run it to verify it fails**

Run: `cargo test -p freenet --lib tracing::otel::tests::provider_builds_inside_a_tokio_runtime`
Expected: FAIL — `cannot find function build_provider in this scope`.

- [ ] **Step 5: Write the implementation**

Add to `crates/core/src/tracing/otel.rs`, after `resolve_metrics_endpoint` and
before the test module:

```rust
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
                endpoint = endpoint.as_deref().unwrap_or("<resolved by OTEL_* env or SDK default>"),
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
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cargo test -p freenet --lib tracing::otel`
Expected: PASS (6 tests).

- [ ] **Step 7: Wire it into node startup**

In `crates/core/src/node.rs`, in `build_with_flush_handle`, immediately after the
`if let Some(telemetry) = TelemetryReporter::new(…) { … }` block (~line 758) and
before `(DynamicRegister::new(registers), flush_handle)`:

```rust
            // Independent of the TelemetryReporter above: a separate opt-in
            // (`otel-telemetry-enabled`), a separate endpoint, and a separate
            // collector. It is not a NetEventRegister — it installs a global
            // meter provider that instrumentation reaches via
            // `opentelemetry::global::meter`.
            crate::tracing::otel::init(&self.config.otel, self.local_peer_id_string());
```

- [ ] **Step 8: Verify the node still builds and its tests pass**

Run: `cargo build -p freenet && cargo test -p freenet --lib node::`
Expected: SUCCESS, PASS.

- [ ] **Step 9: Lint**

Run: `cargo fmt && cargo clippy -p freenet -- -D warnings`
Expected: clean.

- [ ] **Step 10: Commit**

```bash
git add crates/core/Cargo.toml crates/core/src/tracing/otel.rs crates/core/src/node.rs
git commit -m "feat(otel): export metrics through the OpenTelemetry SDK

Installs a global meter provider backed by an OTLP/HTTP exporter, plus one
RSS gauge so the pipeline carries a real datapoint end to end. Future
instrumentation is a global::meter call at the site, with no registry to
keep in sync. The OTel crates become non-optional because the pipeline
ships in every build, not just trace-ot ones."
```

---

### Task 4: Operator documentation

**Files:**
- Modify: `AGENTS.md` (new section after "Delegate secrets-at-rest")
- Reference: `docs/design/otel-metrics-exporter.md` (already written; do not restate it)

**Interfaces:**
- Consumes: the config keys from Task 1 and the env-var precedence from Task 2.
- Produces: nothing code depends on.

- [ ] **Step 1: Add the section**

In `AGENTS.md`, after the `## Delegate secrets-at-rest` section, insert:

```markdown
## Two independent telemetry pipelines

`telemetry-enabled` / `telemetry-endpoint` feed the project's central
dashboard through a hand-rolled OTLP-JSON log POST (`tracing/telemetry.rs`).

`otel-telemetry-enabled` / `otel-endpoint` are a **separate, unrelated**
OpenTelemetry SDK metrics pipeline (`tracing/otel.rs`). The two share no
config, no endpoint, and no fallback in either direction — enabling or
disabling one has no effect on the other, and `otel-endpoint` must never
default to the dashboard collector.

The otel pipeline honors the standard variables, which take priority over
`otel-endpoint` in `config.toml`:
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`, `OTEL_EXPORTER_OTLP_ENDPOINT`,
`OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_SERVICE_NAME`,
`OTEL_RESOURCE_ATTRIBUTES`, `OTEL_METRIC_EXPORT_INTERVAL`. Without any of
them it exports to `http://localhost:4318`.

Adding an instrument is one call at the site — no registry, no wrapper:

    opentelemetry::global::meter("freenet").u64_counter("freenet.some.thing").build()

Design: [`docs/design/otel-metrics-exporter.md`](docs/design/otel-metrics-exporter.md).
```

- [ ] **Step 2: Verify the design doc links resolve**

Run: `ls docs/design/otel-metrics-exporter.md docs/superpowers/plans/2026-08-01-otel-metrics-exporter.md`
Expected: both listed.

- [ ] **Step 3: Commit**

```bash
git add AGENTS.md
git commit -m "docs(otel): document the two independent telemetry pipelines

The isolation between telemetry-enabled and otel-telemetry-enabled is a
design constraint, not an accident, so it belongs where the next person
reads before touching either."
```

---

## Verification

After Task 4, before opening the PR:

- [ ] `cargo fmt --check` — clean
- [ ] `cargo clippy -p freenet -- -D warnings` — clean
- [ ] `cargo test -p freenet --lib config:: tracing::` — pass
- [ ] `cargo test -p freenet --test cross_compile_feature_split` — pass
- [ ] `cargo build -p freenet --features trace-ot` — the feature-list edit in Task 3 did not break the jaeger path
- [ ] `git diff main --stat` — `tracing/telemetry.rs` shows exactly one changed line (the `pub(crate)` widening). Anything more means the isolation constraint was violated.

Manual smoke check (optional, needs a collector):

```bash
docker run --rm -p 4318:4318 otel/opentelemetry-collector:latest
FREENET_OTEL_TELEMETRY_ENABLED=true cargo run -p freenet --bin freenet -- network
# collector log should show freenet.process.memory.rss within ~60s
```
