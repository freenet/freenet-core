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
// NOTE: the `#[freenet_test]` macro rewrites the wrapped function's return type
// to `freenet::test_utils::TestResult`, so the `-> TestResult` annotation below
// is parsed but discarded — no `TestResult` import is needed (and importing one
// trips `unused_imports`). This matches the other `#[freenet_test]` suites
// (e.g. error_notification.rs), which also write `-> TestResult` with no import.

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

/// Strip ANSI CSI escape sequences (`ESC [ ... <final byte>`) from a line.
/// Small hand-rolled stripper so the harness needs no extra dependency; it
/// only has to handle the colour/SGR sequences a CLI might emit.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            // ESC: skip an optional '[' then everything up to and including the
            // final byte in the range 0x40..=0x7e.
            if chars.peek() == Some(&'[') {
                chars.next();
            }
            for d in chars.by_ref() {
                if ('\u{40}'..='\u{7e}').contains(&d) {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// fdev website publish helpers
// ---------------------------------------------------------------------------

/// An `fdev` command whose signing-key store is redirected into `config_home`.
///
/// `fdev website` resolves its key directory through `dirs::config_dir()`, which
/// honours `XDG_CONFIG_HOME` only on Linux — on macOS it is `$HOME/Library/
/// Application Support`, and on Windows `%APPDATA%`. Setting `XDG_CONFIG_HOME`
/// alone therefore left this test writing `smoke-fixture.toml` into the
/// developer's REAL config dir on macOS: the run polluted a directory this
/// harness promises not to touch, and the NEXT run failed with "Key
/// 'smoke-fixture' already exists" because `website init` refuses to overwrite a
/// signing key. Redirect the home/appdata vars too, so the isolation holds
/// wherever the test runs and the suite stays re-runnable.
///
/// Safe to redirect wholesale because `fdev website` shells out to nothing —
/// no cargo, no toolchain lookup — so a synthetic HOME can't break a child
/// build.
fn isolated_fdev(config_home: &Path) -> Command {
    let mut cmd = Command::new(fdev_bin());
    cmd.env("XDG_CONFIG_HOME", config_home) // Linux
        .env("HOME", config_home) // macOS (and the XDG fallback)
        .env("APPDATA", config_home); // Windows
    cmd
}

/// Run `fdev website init <FIXTURE_KEY_NAME>` against an isolated config dir
/// and return the contract key printed on the "Your website contract key:
/// <key>" line.
fn website_init(config_home: &Path) -> anyhow::Result<String> {
    let output = isolated_fdev(config_home)
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
    // Parse the "Your website contract key: <key>" line. `fdev` writes it with
    // a plain `println!` and we capture its stdout via a pipe (not a TTY), so
    // no ANSI colouring is emitted; we still strip CSI escape sequences and any
    // trailing CR/whitespace defensively so a future colourised fdev, or a
    // Windows CRLF, doesn't silently corrupt the key.
    let key = stdout
        .lines()
        .find_map(|l| {
            let stripped = strip_ansi(l);
            stripped
                .strip_prefix("Your website contract key: ")
                .map(|k| k.trim().to_string())
        })
        .ok_or_else(|| {
            anyhow::anyhow!("could not parse contract key from fdev output:\n{stdout}")
        })?;
    // Contract keys are base58 (alphanumeric, no punctuation/whitespace). A
    // value that fails this shape check means our parse picked up the wrong
    // text — fail loudly rather than feed garbage into the shell URL.
    anyhow::ensure!(
        !key.is_empty() && key.chars().all(|c| c.is_ascii_alphanumeric()),
        "parsed contract key has unexpected shape (ANSI/format drift?): {key:?}"
    );
    Ok(key)
}

/// Run `fdev --node-url <ws_url> website publish <fixture> --key <name>`.
///
/// Like `fdev_publish_observed` in `fdev_publish_e2e.rs`, a non-zero exit is
/// not treated as fatal: the freshly-spun fixture's Put driver can report a
/// timeout while the insert still lands. We confirm the real outcome by
/// polling the HTTP shell route instead.
///
/// And for the same reason as there, fdev's client-side wait is capped well
/// below its 300 s default: waiting it out for an outcome we discard is dead
/// time, and it silently couples this test to the node's stall watchdog
/// (`operations::stream_progress::STREAM_OP_INACTIVITY_TIMEOUT`). Measured
/// during #5432: raising that window pushed this test from ~36 s straight to
/// nextest's 240 s cap, three tries running, even though the shell still came
/// up fine and the assertion this test makes was unaffected.
fn website_publish_observed(config_home: &Path, ws_url: &str) {
    let output = isolated_fdev(config_home)
        .env("FDEV_RESPONSE_TIMEOUT", "30")
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

/// Which specs a Playwright pass runs. Specs tagged `@hosted` need a hosted
/// node behind TLS and run only in [`hosted_shell_permission_token_via_playwright`];
/// every other spec runs against the ordinary node.
#[derive(Clone, Copy)]
enum SpecSet {
    NotHosted,
    Hosted,
}

/// Run `npx playwright test` against the shell URL. Assumes deps + browsers are
/// already installed (the CI workflow / local README handle that). Returns an
/// error if the suite fails so the Rust test fails too.
fn run_playwright(shell_url: &str, specs: SpecSet) -> anyhow::Result<()> {
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
        .args(match specs {
            SpecSet::NotHosted => ["--grep-invert", "@hosted"],
            SpecSet::Hosted => ["--grep", "@hosted"],
        })
        .status()
        .map_err(|e| anyhow::anyhow!("failed to spawn `npx playwright test` in {dir:?}: {e}"))?;
    anyhow::ensure!(
        status.success(),
        "Playwright suite failed (exit {status:?})"
    );
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
// Budget: the harness times the WHOLE test, including the `fdev website
// publish` step (now capped at 30s of client wait by `website_publish_observed`
// — it was previously bounded only by fdev's 300s default, which is what made
// this test hit nextest's cap when #5432 widened the node-side stall window),
// the shell-readiness poll (up to 120s), and the Playwright suite (~90s for the
// 16 specs in shell.spec.ts, plus every other suite under tests/playwright/tests,
// connecting-recovery.spec.ts among them). An observed local
// run finished in ~282s BEFORE that cap; it is well under that now. 600s gives
// comfortable headroom without masking a genuine hang (the individual sub-steps
// have their own tighter internal deadlines).
#[freenet_test(
    health_check_readiness = true,
    nodes = ["gateway"],
    timeout_secs = 600,
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
    tokio::task::spawn_blocking(move || run_playwright(&shell_url_for_pw, SpecSet::NotHosted))
        .await??;

    Ok(())
}

// ---------------------------------------------------------------------------
// Hosted-mode pass
// ---------------------------------------------------------------------------

/// Reserve an ephemeral TCP port by binding then freeing it.
fn reserve_port() -> anyhow::Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

/// A single self-contained gateway rooted at `dir`, its client API on
/// loopback `ws_port`. `hosted_mode` is the only difference between the two
/// boots of [`hosted_shell_permission_token_via_playwright`].
fn standalone_node_config(
    dir: &Path,
    ws_port: u16,
    network_port: u16,
    keypair_path: &Path,
    hosted_mode: bool,
) -> freenet::config::ConfigArgs {
    use std::net::Ipv4Addr;
    freenet::config::ConfigArgs {
        ws_api: freenet::config::WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(ws_port),
            hosted_mode: Some(hosted_mode),
            ..Default::default()
        },
        network_api: freenet::config::NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: Some(network_port),
            is_gateway: true,
            skip_load_from_network: true,
            gateways: Some(vec![]),
            location: Some(0.5),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: Some(network_port),
            ..Default::default()
        },
        config_paths: freenet::config::ConfigPathsArgs {
            config_dir: Some(dir.to_path_buf()),
            data_dir: Some(dir.to_path_buf()),
            log_dir: Some(dir.to_path_buf()),
        },
        secrets: freenet::config::SecretArgs {
            transport_keypair: Some(keypair_path.to_path_buf()),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// A running standalone node.
struct StandaloneNode {
    ws_port: u16,
    shutdown: freenet::ShutdownHandle,
    run: tokio::task::JoinHandle<Result<std::convert::Infallible, anyhow::Error>>,
}

impl StandaloneNode {
    async fn start(dir: &Path, keypair_path: &Path, hosted_mode: bool) -> anyhow::Result<Self> {
        let ws_port = reserve_port()?;
        let net_port = reserve_port()?;
        let cfg = standalone_node_config(dir, ws_port, net_port, keypair_path, hosted_mode)
            .build()
            .await?;
        let node = freenet::local_node::NodeConfig::new(cfg.clone())
            .await?
            .build(freenet::server::serve_client_api(cfg.ws_api.clone()).await?)
            .await?;
        let shutdown = node.shutdown_handle();
        let run = tokio::spawn(async move { node.run().await });
        Ok(Self {
            ws_port,
            shutdown,
            run,
        })
    }

    async fn stop(self) {
        self.shutdown.shutdown().await;
        if tokio::time::timeout(Duration::from_secs(30), self.run)
            .await
            .is_err()
        {
            tracing::info!("node run loop did not exit within 30s of shutdown");
        }
    }
}

/// How long the TLS proxy has to report the port it listens on.
const TLS_PROXY_START_TIMEOUT: Duration = Duration::from_secs(30);

/// The TLS proxy child (`hosted-tls-proxy.mjs`), killed on drop.
struct TlsProxy {
    child: std::process::Child,
    port: u16,
}

impl TlsProxy {
    fn start(node_port: u16) -> anyhow::Result<Self> {
        use std::io::BufRead;
        let mut child = Command::new("node")
            .arg(playwright_dir().join("hosted-tls-proxy.mjs"))
            .arg(node_port.to_string())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| {
                anyhow::anyhow!("failed to spawn the TLS proxy (is node on PATH?): {e}")
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("TLS proxy has no stdout"))?;
        // Read the port line on a separate thread so a proxy that never
        // writes it cannot hang the test. Killing the child ends that read.
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut line = String::new();
            let read = std::io::BufReader::new(stdout)
                .read_line(&mut line)
                .map(|_| line);
            drop(tx.send(read));
        });
        let line = match rx.recv_timeout(TLS_PROXY_START_TIMEOUT) {
            Ok(Ok(line)) => line,
            Ok(Err(e)) => {
                drop(child.kill());
                drop(child.wait());
                anyhow::bail!("reading the TLS proxy's port failed: {e}")
            }
            Err(_) => {
                drop(child.kill());
                drop(child.wait());
                anyhow::bail!(
                    "TLS proxy did not report its port within {TLS_PROXY_START_TIMEOUT:?}"
                )
            }
        };
        let port = line
            .trim()
            .strip_prefix("LISTENING ")
            .and_then(|p| p.parse::<u16>().ok());
        match port {
            Some(port) => Ok(Self { child, port }),
            None => {
                drop(child.kill());
                drop(child.wait());
                anyhow::bail!("TLS proxy did not report its port, got {line:?}")
            }
        }
    }
}

impl Drop for TlsProxy {
    fn drop(&mut self) {
        drop(self.child.kill());
        drop(self.child.wait());
    }
}

/// The shell's permission routes on a HOSTED node, in a real browser.
///
/// A hosted node requires the user's token on the permission routes and
/// shows each user only their own prompts; the `@hosted` specs in
/// `tests/permission-user-token.spec.ts` check that the shell sends that
/// token. Such a node cannot be prepared through `#[freenet_test]`, and a
/// hosted node refuses the tokenless client-API connection `fdev website
/// publish` would use, so this test:
///
/// 1. boots a standalone node that is NOT hosted and publishes the fixture,
/// 2. restarts the same data directory in hosted mode,
/// 3. puts `hosted-tls-proxy.mjs` in front of it, which serves https and adds
///    `X-Forwarded-Proto: https` as a same-host TLS proxy does, and
/// 4. runs `npx playwright test --grep @hosted` against the proxied shell.
///
/// Without `FREENET_PLAYWRIGHT=1` it returns before booting any node, so an
/// ordinary test run pays nothing for it and a checkout with no Node or
/// browsers never fails. Each node is shut down on every return path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hosted_shell_permission_token_via_playwright() -> anyhow::Result<()> {
    if !playwright_enabled() {
        tracing::warn!(
            "{PLAYWRIGHT_ENV} not set: skipping the hosted pass without booting \
             a node. Set {PLAYWRIGHT_ENV}=1 (and install Playwright deps) to run it."
        );
        return Ok(());
    }
    let data_dir = tempfile::tempdir()?;
    let key = freenet::dev_tool::TransportKeypair::new();
    let keypair_path = data_dir.path().join("private.pem");
    key.save(&keypair_path)?;
    key.public().save(data_dir.path().join("public.pem"))?;

    // 1. Publish on a node that is not hosted.
    let config_home = tempfile::tempdir()?;
    let contract_key = website_init(config_home.path())?;
    let node = StandaloneNode::start(data_dir.path(), &keypair_path, false).await?;
    let ws_url = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        node.ws_port
    );
    // The node's API may still be coming up; retry the publish until the
    // shell serves.
    let first_shell = format!(
        "http://127.0.0.1:{}/v1/contract/web/{contract_key}/",
        node.ws_port
    );
    let published = async {
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        loop {
            website_publish_observed(config_home.path(), &ws_url);
            if wait_for_shell(&first_shell, Duration::from_secs(20)).await? {
                return Ok(());
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "the fixture never published on the node that is not hosted"
            );
        }
    }
    .await;
    node.stop().await;
    published?;

    // 2. Same data directory, hosted mode. The first node's store is released
    // a little after its run loop exits, so the reopen is retried while the
    // database is still locked, for a bounded time.
    let reopen_deadline = std::time::Instant::now() + Duration::from_secs(60);
    let node = loop {
        match StandaloneNode::start(data_dir.path(), &keypair_path, true).await {
            Ok(node) => break node,
            Err(e)
                if e.to_string().contains("already open")
                    && std::time::Instant::now() < reopen_deadline =>
            {
                tracing::info!("data directory still locked by the first node, retrying: {e}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => return Err(e),
        }
    };
    let hosted_shell = format!(
        "http://127.0.0.1:{}/v1/contract/web/{contract_key}/",
        node.ws_port
    );

    let result = async {
        let served = wait_for_shell(&hosted_shell, Duration::from_secs(120)).await;
        anyhow::ensure!(
            matches!(served, Ok(true)),
            "the hosted node never served the published shell: {served:?}"
        );

        // 3. The TLS proxy, and a check that the shell is reachable through it.
        let proxy = TlsProxy::start(node.ws_port)?;
        let shell_url = format!(
            "https://127.0.0.1:{}/v1/contract/web/{contract_key}/",
            proxy.port
        );
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(10))
            .build()?;
        let body = client.get(&shell_url).send().await?.text().await?;
        anyhow::ensure!(
            body.contains("freenetBridge") && body.contains("__freenet_user_token"),
            "the hosted shell is not served through the TLS proxy"
        );

        // 4. The hosted specs.
        let url = shell_url.clone();
        tokio::task::spawn_blocking(move || run_playwright(&url, SpecSet::Hosted)).await??;
        drop(proxy);
        Ok(())
    }
    .await;
    node.stop().await;
    result
}
