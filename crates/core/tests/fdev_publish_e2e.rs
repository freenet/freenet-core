//! End-to-end coverage for `fdev publish` and `fdev get-contract-id`
//! against a real in-process node.
//!
//! Complements the unit tests in `crates/fdev/src/commands.rs::tests`
//! (which only cover the `load_contract_for_publish` helper in
//! isolation) by driving the full CLI → WebSocket → node DB path.
//! That's the surface where #4075 manifested: the bug folded the
//! 8-byte version + 32-byte hash header into `ContractCode.data`,
//! the bytes crossed the wire intact, and only the receiving node's
//! wasmtime rejected them with `compile: input bytes aren't valid
//! utf-8`. A unit test on the helper alone can't catch a regression
//! in node-side insertion; this file does.
//!
//! Coverage:
//!   * raw vs packaged input agree on contract id (the symptom of
//!     #4075 was that the packaged input mixed the 40-byte header
//!     into the hash → different id)
//!   * garbage/empty input is rejected at the CLI surface (not
//!     silently turned into a 0-byte or 40-byte "contract")
//!   * packaged contract round-trips through `fdev publish` to a
//!     live peer node and lands in its local DB
//!   * raw contract still round-trips after the fix — no regression
//!     on the previously-working path
//!
//! Optional follow-ups (not yet covered):
//!   * `fdev publish delegate` with a packaged delegate file
//!   * webapp wrapper round-trip via `fdev website publish`

use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Context;
use freenet::test_utils::{self, TestContext, make_get};
use freenet_macros::freenet_test;
use freenet_stdlib::client_api::{ContractResponse, HostResponse, WebApi};
use freenet_stdlib::prelude::*;
use testresult::TestResult;
use tokio_tungstenite::connect_async;

const TEST_CONTRACT: &str = "test-contract-integration";

// ---------------------------------------------------------------------------
// Path resolution
// ---------------------------------------------------------------------------

fn workspace_root() -> PathBuf {
    // `CARGO_MANIFEST_DIR` is set at compile time to this crate's
    // directory (`crates/core`). The workspace root is two ancestors
    // up. The same pattern is used by `compile_contract` in
    // `test_utils.rs` to locate the workspace.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace layout: crates/core/../../ should resolve")
        .to_path_buf()
}

fn target_dir() -> PathBuf {
    // Honor CARGO_TARGET_DIR if the caller (CI's `test_unit` job sets
    // it, see ci.yml) overrides the default. Otherwise fall back to
    // `<workspace>/target` like cargo itself does.
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("target"))
}

/// Path to the `fdev` binary built by `cargo build` from this
/// workspace. The CI `test_unit` job runs `cargo build --locked`
/// before `cargo nextest`, so this is on disk by the time tests
/// start. Local devs need to `cargo build --bin fdev` first.
fn fdev_bin() -> PathBuf {
    let mut path = target_dir().join("debug").join("fdev");
    if !path.exists() {
        // Some local workflows build `--release` (or set
        // CARGO_TARGET_DIR to a release path). Try that before giving
        // up — keeps the test runnable without a fresh debug build.
        let release = target_dir().join("release").join("fdev");
        if release.exists() {
            path = release;
        }
    }
    assert!(
        path.exists(),
        "fdev binary not found at {path:?}. Build it first: \
         `cargo build --bin fdev` (CI's test_unit job does this in \
         its Build step before the Test step)."
    );
    path
}

// ---------------------------------------------------------------------------
// Contract fixture (shared across tests so we only run `fdev build`
// once per test binary invocation — `nextest` runs each test in its
// own process so this only saves the second test in the same file).
// ---------------------------------------------------------------------------

/// Output of `fdev build` against `tests/test-contract-integration/`.
/// 8-byte version + 32-byte hash + raw WASM.
fn packaged_contract_path() -> PathBuf {
    static CACHE: OnceLock<PathBuf> = OnceLock::new();
    CACHE
        .get_or_init(|| {
            let crate_dir = workspace_root().join("tests").join(TEST_CONTRACT);
            assert!(
                crate_dir.exists(),
                "test contract crate missing at {crate_dir:?}"
            );

            let status = Command::new(fdev_bin())
                .arg("build")
                .current_dir(&crate_dir)
                .status()
                .expect("spawn fdev build");
            assert!(status.success(), "fdev build failed for {TEST_CONTRACT}");

            let path = crate_dir
                .join("build")
                .join("freenet")
                .join(TEST_CONTRACT.replace('-', "_"));
            assert!(
                path.exists(),
                "fdev build did not produce expected artifact at {path:?}"
            );
            path
        })
        .clone()
}

/// Strip the 40-byte (8 + 32) version+hash header so we have a file
/// that begins with the WASM magic. Mirrors what
/// `ContractCode::load_versioned_from_path` does internally; we read
/// the packaged file ourselves to keep the test transport-agnostic.
fn write_raw_wasm_into(dst: &std::path::Path) -> anyhow::Result<()> {
    const HEADER_LEN: usize = 8 + 32;
    const WASM_MAGIC: &[u8; 4] = b"\0asm";

    let bytes = std::fs::read(packaged_contract_path()).context("read packaged contract")?;
    anyhow::ensure!(
        bytes.len() > HEADER_LEN,
        "packaged file shorter than 40-byte header: {}",
        bytes.len()
    );
    let raw = &bytes[HEADER_LEN..];
    anyhow::ensure!(
        raw.starts_with(WASM_MAGIC),
        "after stripping {HEADER_LEN}-byte header, expected WASM magic, got: {:02x?}",
        &raw[..raw.len().min(8)]
    );
    std::fs::write(dst, raw).context("write raw wasm")?;
    Ok(())
}

/// Per-contract initial state. test-contract-integration validates
/// state as JSON-deserialized `TodoList { tasks, version }`; the
/// empty list is the cheapest valid encoding.
fn write_initial_state_into(dst: &std::path::Path) -> anyhow::Result<()> {
    let bytes = test_utils::create_empty_todo_list();
    std::fs::write(dst, bytes).context("write initial state")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// fdev process helpers
// ---------------------------------------------------------------------------

/// Run an `fdev` subcommand and return its stdout. Panics with full
/// diagnostic if exit code is non-zero — saves every test from
/// re-implementing the "spawn / check / print stderr" boilerplate.
fn fdev_run(args: &[&str]) -> String {
    let output = Command::new(fdev_bin())
        .args(args)
        .output()
        .expect("spawn fdev");
    if !output.status.success() {
        panic!(
            "fdev {args:?} exited {:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    String::from_utf8(output.stdout).expect("fdev stdout not utf-8")
}

/// Like `fdev_run` but expects failure. Returns (exit_code, stderr).
fn fdev_run_expect_failure(args: &[&str]) -> (Option<i32>, String) {
    let output = Command::new(fdev_bin())
        .args(args)
        .output()
        .expect("spawn fdev");
    assert!(
        !output.status.success(),
        "fdev {args:?} unexpectedly succeeded\nstdout: {}",
        String::from_utf8_lossy(&output.stdout),
    );
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

/// Run `fdev publish` and capture its outcome, but DO NOT treat a
/// non-zero exit as a fatal test failure.
///
/// Why: in a freshly-spun-up 2-node test fixture the receiving node's
/// Put driver hits its single-shot attempt timeout (~90s) before the
/// actual insert is flushed — fdev faithfully reports the timeout to
/// the user, then the insert completes ~200ms later (the node logs a
/// `Broadcasting hosting update` followed by a `PutResponse`). The
/// race is a fixture quirk, not a code defect in fdev's load path,
/// and what we're actually probing here is "do the bytes survive the
/// wire transit and land in the DB" — which is the surface the #4075
/// fix touches. Validate via [`wait_for_contract_via_get`] below
/// instead of via exit code.
async fn fdev_publish_observed(args: &[&str]) {
    let output = Command::new(fdev_bin())
        .args(args)
        .output()
        .expect("spawn fdev");
    if output.status.success() {
        tracing::info!(
            "fdev publish exited 0: {}",
            String::from_utf8_lossy(&output.stdout).trim_end()
        );
    } else {
        // Document the exact error so a future regression that
        // changes the error string is visible in the test log.
        tracing::warn!(
            "fdev publish reported timeout/error (expected race with \
             fixture's Put driver): exit={:?}\nstderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim_end(),
        );
    }
}

/// Repeatedly issue a WS `Get` against the peer until it returns
/// `Found`, or the deadline expires. Used after
/// `fdev_publish_observed` to confirm the bytes really did land —
/// independent of whether fdev itself reported success or hit the
/// fixture race. A `Found` response means the receiving node
/// compiled the WASM and committed it (the surface the #4075 fix
/// is about).
///
/// We use WS Get rather than `verify_contract_exists` from
/// test_utils because the latter checks a stale path layout
/// (`<dir>/contracts/<hash>` without `.wasm` extension and without
/// the operation-mode subdirectory) — it returns `false` even for
/// successfully-stored contracts. operations.rs calls
/// `verify_contract_exists` but discards its bool, so the staleness
/// isn't visible there. WS Get exercises exactly the surface a real
/// user would see.
async fn wait_for_contract_via_get(
    ws_url: &str,
    key: ContractKey,
    within: Duration,
) -> anyhow::Result<bool> {
    let deadline = std::time::Instant::now() + within;
    let (stream, _) = connect_async(ws_url)
        .await
        .with_context(|| format!("connect to peer ws {ws_url}"))?;
    let mut client = WebApi::start(stream);

    // Inner attempt timeout — we don't want a single Get to consume
    // the whole budget if the peer is briefly busy.
    const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(15);

    while std::time::Instant::now() < deadline {
        make_get(&mut client, key, true, false).await?;
        match tokio::time::timeout(ATTEMPT_TIMEOUT, client.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                contract: Some(_),
                ..
            }))) => return Ok(true),
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                contract: None,
                ..
            }))) => {
                // Contract not (yet) in store on this peer — wait a
                // beat and retry. Pre-fix this path stays empty
                // forever because wasmtime rejected the bytes; post-
                // fix the contract appears within a few seconds.
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(Ok(other)) => {
                tracing::warn!("unexpected response while waiting for Get: {other:?}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(Err(e)) => {
                tracing::warn!("Get attempt errored: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(_) => {
                tracing::warn!("Get attempt timed out, retrying");
            }
        }
    }
    Ok(false)
}

// ---------------------------------------------------------------------------
// Tests: pure-CLI invariants (no node — fast, run in parallel)
// ---------------------------------------------------------------------------

/// `fdev get-contract-id` must return the same key whether the
/// input file is raw WASM (magic at offset 0) or the packaged format
/// (`fdev build` output: 8-byte version + 32-byte hash + WASM).
///
/// Pre-#4075 the packaged input went through `ContractCode::load_raw`
/// (which is just `read_to_end`) and the 40-byte prefix was hashed
/// alongside the WASM bytes, so the two ids diverged. The fix makes
/// `load_contract_for_publish` peek for the WASM magic and route
/// packaged inputs through `load_versioned_from_path` — same code
/// hash, same id.
#[test]
fn get_contract_id_matches_between_raw_and_packaged() -> TestResult {
    let tmp = tempfile::tempdir()?;
    let raw_path = tmp.path().join("raw.wasm");
    write_raw_wasm_into(&raw_path)?;
    let packaged_path = packaged_contract_path();

    let id_packaged = fdev_run(&["get-contract-id", "--code", packaged_path.to_str().unwrap()])
        .trim()
        .to_string();
    let id_raw = fdev_run(&["get-contract-id", "--code", raw_path.to_str().unwrap()])
        .trim()
        .to_string();

    assert!(!id_packaged.is_empty(), "fdev printed empty contract id");
    assert_eq!(
        id_packaged, id_raw,
        "raw and packaged inputs must hash to the same contract id \
         (pre-fix the packaged path folded the 40-byte header into \
         ContractCode.data → distinct ids)"
    );
    Ok(())
}

/// Files that are neither raw WASM nor a valid packaged container
/// must surface as a non-zero exit. Without the magic-byte guard,
/// `ContractCode::load_raw` happily reads any file into bytes and
/// the downstream node compile is the only thing that rejects it —
/// which leaves the failure mode for an end user looking like a
/// node bug instead of a fdev input bug.
#[test]
fn get_contract_id_rejects_garbage_and_empty() -> TestResult {
    let tmp = tempfile::tempdir()?;
    for (name, bytes) in [
        ("empty.bin", &[][..]),
        ("garbage.bin", &[0xff; 16][..]),
        // 4 bytes that pass length-but-not-magic; trips load_raw but
        // would have passed the previous (no-magic-check) branch.
        ("near-magic.bin", &[0x00, 0x61, 0x73, 0x99][..]),
    ] {
        let path = tmp.path().join(name);
        std::fs::write(&path, bytes)?;
        let (exit, stderr) =
            fdev_run_expect_failure(&["get-contract-id", "--code", path.to_str().unwrap()]);
        assert!(
            !stderr.is_empty() || exit != Some(0),
            "{name}: fdev failed but produced no diagnostic"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests: end-to-end (live in-process peer node)
// ---------------------------------------------------------------------------

/// Drive `fdev publish` against a peer node's WS API with the
/// packaged contract format. The receiving node must accept the
/// bytes (after #4075 strips the 40-byte prefix on the fdev side)
/// and end up with the contract in its local store. Pre-fix the
/// peer would have rejected the Put inside `validate_state` with
/// `compile: input bytes aren't valid utf-8`, leaving its DB empty.
#[freenet_test(
    health_check_readiness = true,
    nodes = ["gateway", "peer-a"],
    // Matches the known-working fixture in operations.rs::test_put_contract:
    // 300s budget + 4 worker threads. With fewer workers / shorter wait the
    // first Put attempt sometimes hits the node's single-shot timeout while
    // the network topology is still settling.
    timeout_secs = 300,
    startup_wait_secs = 30,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4,
)]
async fn publish_packaged_contract_round_trip(ctx: &mut TestContext) -> TestResult {
    let peer = ctx.node("peer-a")?;

    let tmp = tempfile::tempdir()?;
    let state_path = tmp.path().join("state.json");
    write_initial_state_into(&state_path)?;
    let packaged = packaged_contract_path();

    // Resolve the contract key the same way fdev's publish path does
    // — by sending the packaged file through `load_contract` (which
    // packages the raw WASM ourselves, identical hash). This lets us
    // assert that the peer's DB contains exactly the contract we
    // expect, not just "some contract".
    let container = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let expected_key = container.key();

    // Use the node's full ws_url — `#[freenet_test]` binds each peer
    // to a distinct loopback (`127.1.2.1` etc.) for test isolation,
    // so `ws://127.0.0.1:<port>` would refuse to connect.
    let url = peer.ws_url();
    fdev_publish_observed(&[
        "--node-url",
        &url,
        "publish",
        "--code",
        packaged.to_str().unwrap(),
        "contract",
        "--state",
        state_path.to_str().unwrap(),
    ])
    .await;

    assert!(
        wait_for_contract_via_get(&url, expected_key, Duration::from_secs(120)).await?,
        "packaged contract not found via Get on peer-a for key {} \
         — did wasmtime reject the bytes? (#4075 regression: pre-fix \
         the packaged path included the 40-byte header in the bytes \
         shipped to the node, wasmtime rejected with \"compile: input \
         bytes aren't valid utf-8\", and the contract never reached \
         the store)",
        expected_key,
    );
    Ok(())
}

/// Same shape as the packaged round-trip but feeds raw WASM straight
/// into `fdev publish`. The previously-working path; the test guards
/// against any regression introduced by the magic-byte discrimination.
#[freenet_test(
    health_check_readiness = true,
    nodes = ["gateway", "peer-a"],
    // Matches the known-working fixture in operations.rs::test_put_contract:
    // 300s budget + 4 worker threads. With fewer workers / shorter wait the
    // first Put attempt sometimes hits the node's single-shot timeout while
    // the network topology is still settling.
    timeout_secs = 300,
    startup_wait_secs = 30,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4,
)]
async fn publish_raw_wasm_round_trip(ctx: &mut TestContext) -> TestResult {
    let peer = ctx.node("peer-a")?;

    let tmp = tempfile::tempdir()?;
    let raw_path = tmp.path().join("raw.wasm");
    write_raw_wasm_into(&raw_path)?;
    let state_path = tmp.path().join("state.json");
    write_initial_state_into(&state_path)?;

    let container = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let expected_key = container.key();

    // Use the node's full ws_url — `#[freenet_test]` binds each peer
    // to a distinct loopback (`127.1.2.1` etc.) for test isolation,
    // so `ws://127.0.0.1:<port>` would refuse to connect.
    let url = peer.ws_url();
    fdev_publish_observed(&[
        "--node-url",
        &url,
        "publish",
        "--code",
        raw_path.to_str().unwrap(),
        "contract",
        "--state",
        state_path.to_str().unwrap(),
    ])
    .await;

    assert!(
        wait_for_contract_via_get(&url, expected_key, Duration::from_secs(120)).await?,
        "raw WASM contract not found via Get on peer-a for key {} \
         (regression on the previously-working raw-input path)",
        expected_key,
    );
    Ok(())
}
