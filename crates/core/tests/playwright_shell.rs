//! Rust harness that drives the Playwright shell smoke tests
//! (freenet/freenet-core#3856).
//!
//! ## Why this exists
//!
//! Four PRs (#3842, #3849, #3852, #3854) fixed bugs in the shell bridge, the
//! navigation interceptor, and the CSP — code that lives as JavaScript strings
//! in `crates/core/src/server/path_handlers.rs` and `client_api.rs`. Each was
//! only guarded by a Rust-level substring assertion on the emitted JS, and
//! each review note said "this should really be a browser test". This harness
//! closes that gap: it boots a real node, publishes a fixture webapp, and runs
//! the injected shell/iframe JavaScript against a headless Chromium via
//! Playwright.
//!
//! ## Architecture
//!
//! - Node lifecycle stays on the well-tested `#[freenet_test]` in-process path
//!   (same model as `fdev_publish_e2e.rs`). A single gateway node is enough:
//!   it stores the contract it publishes locally, so serving the shell needs
//!   no network propagation.
//! - The fixture webapp (`tests/playwright/fixture-webapp/`) is published as a
//!   Freenet *website contract* using the same `fdev website` path real users
//!   take. We drive `fdev` as a child process with a temp `XDG_CONFIG_HOME`,
//!   so the signing key never touches the developer's real config dir.
//! - Fixture WASM choice: we reuse the **prebuilt** website-container WASM that
//!   ships embedded in `fdev` (`crates/fdev/resources/website_contract.wasm`).
//!   That keeps the test free of a `wasm32-unknown-unknown` build step and
//!   makes the contract key deterministic per fdev version. The tradeoff (the
//!   WASM is a committed binary that must be rebuilt by hand when its source
//!   changes) is already accepted project-wide — see
//!   `crates/website-contract/README.md`.
//! - The browser layer (`tests/playwright/`) is a thin Playwright project; the
//!   Rust side passes it the ready-to-load shell URL via `FREENET_SHELL_URL`.
//!
//! ## Running
//!
//! The Playwright step is opt-in via `FREENET_PLAYWRIGHT=1` so the default
//! `cargo nextest` run (and the unit-test CI job) does not require Node /
//! browsers and never goes red for a missing toolchain. The dedicated
//! `.github/workflows/playwright-shell.yml` job installs the browsers and sets
//! the flag.
//!
//! ```text
//! # one-time, in crates/core/tests/playwright/:
//! npm ci && npx playwright install --with-deps chromium
//! # then, from the workspace root:
//! cargo build --bin fdev
//! FREENET_PLAYWRIGHT=1 cargo nextest run -p freenet \
//!   --features testing --test playwright_shell
//! ```

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use freenet::test_utils::TestContext;
use freenet_macros::freenet_test;
use testresult::TestResult;

/// Name of the website signing key created in the temp config dir.
const FIXTURE_KEY_NAME: &str = "smoke-fixture";

/// Env flag that opts in to actually launching Playwright. Unset (the default,
/// including a plain `cargo test`) makes the test publish + reach the shell and
/// then return early, so the absence of Node/browsers is never a failure.
const PLAYWRIGHT_ENV: &str = "FREENET_PLAYWRIGHT";

// ---------------------------------------------------------------------------
// Path resolution (mirrors fdev_publish_e2e.rs)
// ---------------------------------------------------------------------------

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace layout: crates/core/../../ should resolve")
        .to_path_buf()
}

fn target_dir() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("target"))
}

/// Path to the `fdev` binary. CI builds it with `cargo build` before tests;
/// local devs need `cargo build --bin fdev` first.
fn fdev_bin() -> PathBuf {
    let debug = target_dir().join("debug").join("fdev");
    if debug.exists() {
        return debug;
    }
    let release = target_dir().join("release").join("fdev");
    assert!(
        release.exists(),
        "fdev binary not found at {debug:?} or {release:?}. Build it first: \
         `cargo build --bin fdev`."
    );
    release
}

fn playwright_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("playwright")
}

fn fixture_webapp_dir() -> PathBuf {
    playwright_dir().join("fixture-webapp")
}

// ---------------------------------------------------------------------------
// fdev website publish helpers
// ---------------------------------------------------------------------------

/// Run `fdev website init <FIXTURE_KEY_NAME>` against an isolated config dir
/// (`XDG_CONFIG_HOME = config_home`) and return the contract key printed on the
/// "Your website contract key: <key>" line.
fn website_init(config_home: &Path) -> anyhow::Result<String> {
    let output = Command::new(fdev_bin())
        .env("XDG_CONFIG_HOME", config_home)
        .args(["website", "init", FIXTURE_KEY_NAME])
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() {
        anyhow::bail!(
            "fdev website init failed: {:?}\nstdout: {stdout}\nstderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    let key = stdout
        .lines()
        .find_map(|l| l.strip_prefix("Your website contract key: "))
        .map(|k| k.trim().to_string())
        .ok_or_else(|| anyhow::anyhow!("could not parse contract key from fdev output:\n{stdout}"))?;
    anyhow::ensure!(!key.is_empty(), "fdev printed an empty contract key");
    Ok(key)
}

/// Run `fdev --node-url <ws_url> website publish <fixture> --key <name>`.
///
/// Like `fdev_publish_observed` in `fdev_publish_e2e.rs`, a non-zero exit is
/// not treated as fatal: the freshly-spun fixture's Put driver can report a
/// timeout while the insert still lands. We confirm the real outcome by
/// polling the HTTP shell route instead.
fn website_publish_observed(config_home: &Path, ws_url: &str) {
    let output = Command::new(fdev_bin())
        .env("XDG_CONFIG_HOME", config_home)
        .args(["--node-url", ws_url, "website", "publish"])
        .arg(fixture_webapp_dir())
        .args(["--key", FIXTURE_KEY_NAME])
        .output()
        .expect("spawn fdev website publish");
    if output.status.success() {
        tracing::info!(
            "fdev website publish exited 0: {}",
            String::from_utf8_lossy(&output.stdout).trim_end()
        );
    } else {
        tracing::warn!(
            "fdev website publish reported error (may still have landed): exit={:?}\nstderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim_end(),
        );
    }
}

/// Poll the shell route (`GET /v1/contract/web/{key}/`) until it returns 200
/// and the body looks like the shell (contains the sandboxed iframe), or the
/// deadline expires. A 200 shell means the node fetched + unpacked the contract
/// it stored locally and `shell_page` rendered — exactly what the browser test
/// then loads.
async fn wait_for_shell(shell_url: &str, within: Duration) -> anyhow::Result<bool> {
    let deadline = std::time::Instant::now() + within;
    // reqwest is already a (non-dev) dependency of the freenet crate, so this
    // adds no new dependency. Short per-request timeout so a hung connection
    // doesn't eat the whole budget on one attempt.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    while std::time::Instant::now() < deadline {
        match client.get(shell_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let body = resp.text().await.unwrap_or_default();
                if body.contains("id=\"app\"") && body.contains("freenetBridge") {
                    return Ok(true);
                }
                tracing::debug!("shell 200 but body not ready yet ({} bytes)", body.len());
            }
            Ok(resp) => {
                tracing::debug!("shell not ready: HTTP {}", resp.status());
            }
            Err(e) => {
                tracing::debug!("shell request errored (node still warming?): {e}");
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Ok(false)
}

// ---------------------------------------------------------------------------
// Playwright invocation
// ---------------------------------------------------------------------------

/// Whether the caller asked us to actually launch the browser suite.
fn playwright_enabled() -> bool {
    matches!(
        std::env::var(PLAYWRIGHT_ENV).ok().as_deref(),
        Some("1") | Some("true")
    )
}

/// Run `npx playwright test` against the shell URL. Assumes deps + browsers are
/// already installed (the CI workflow / local README handle that). Returns an
/// error if the suite fails so the Rust test fails too.
fn run_playwright(shell_url: &str) -> anyhow::Result<()> {
    let dir = playwright_dir();
    anyhow::ensure!(
        dir.join("node_modules").exists(),
        "Playwright deps not installed. Run `npm ci` in {dir:?} (CI does this) \
         before setting {PLAYWRIGHT_ENV}=1."
    );
    let status = Command::new("npx")
        .current_dir(&dir)
        .env("FREENET_SHELL_URL", shell_url)
        .args(["playwright", "test"])
        .status()
        .map_err(|e| anyhow::anyhow!("failed to spawn `npx playwright test` in {dir:?}: {e}"))?;
    anyhow::ensure!(status.success(), "Playwright suite failed (exit {status:?})");
    Ok(())
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

/// Boot a node, publish the fixture webapp, confirm the shell serves over HTTP,
/// then (when `FREENET_PLAYWRIGHT=1`) run the Playwright browser suite against
/// it.
///
/// A single gateway node suffices: it stores its own published contract, so
/// `contract_home` fetches it locally and renders the shell without needing
/// network propagation.
#[freenet_test(
    health_check_readiness = true,
    nodes = ["gateway"],
    timeout_secs = 300,
    startup_wait_secs = 30,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4,
)]
async fn shell_smoke_via_playwright(ctx: &mut TestContext) -> TestResult {
    let node = ctx.node("gateway")?;

    // Isolate the website signing key in a temp config dir so we never touch
    // the developer's real ~/.config/freenet.
    let config_home = tempfile::tempdir()?;
    let key = website_init(config_home.path())?;
    tracing::info!("fixture website contract key: {key}");

    // Publish the fixture via the real `fdev website publish` path.
    let ws_url = node.ws_url();
    website_publish_observed(config_home.path(), &ws_url);

    // The HTTP API server shares the node's WS port. Build the shell route.
    let shell_url = format!(
        "http://{}:{}/v1/contract/web/{}/",
        node.ip, node.ws_port, key
    );

    assert!(
        wait_for_shell(&shell_url, Duration::from_secs(120)).await?,
        "shell never became ready at {shell_url} — the fixture contract did not \
         publish + unpack within the deadline"
    );
    tracing::info!("shell ready at {shell_url}");

    if !playwright_enabled() {
        tracing::warn!(
            "{PLAYWRIGHT_ENV} not set: published the fixture and confirmed the \
             shell serves, but skipping the browser suite. Set {PLAYWRIGHT_ENV}=1 \
             (and install Playwright deps) to run it."
        );
        return Ok(());
    }

    // tokio is multi-threaded here; the blocking `npx` child is fine on a
    // worker thread for a one-shot suite run.
    let shell_url_for_pw = shell_url.clone();
    tokio::task::spawn_blocking(move || run_playwright(&shell_url_for_pw)).await??;

    Ok(())
}
