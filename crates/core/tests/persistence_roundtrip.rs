//! Disk-backed crash/recover persistence round-trip test.
//!
//! Issue #3367 (Gap 1): SimNetwork nodes use in-memory stores, so existing
//! crash/recover tests never exercise the real redb-backed `ContractStore`
//! on disk. Nothing PUTs a contract WITH parameters, stops the node so state
//! flushes to redb, restarts from the SAME on-disk data dir, and verifies a
//! GET returns the correct state AND parameters and that a subscription can be
//! re-established afterwards.
//!
//! This regression test closes that gap. It guards:
//! - #3350: parameters lost on restart
//! - #3352: subscriptions broken on startup
//!
//! Out of scope here (this test PUTs a single contract and exercises neither
//! eviction nor a delegate, so it would NOT have caught these — they need
//! their own targeted repros):
//! - #3341: stale cache after LRU eviction
//! - #3344: delegate index unrecoverable
//!
//! Topology: a single gateway node with a persistent (disk-backed) data dir.
//! It PUTs the contract, is stopped, then a fresh node is started against the
//! *same* on-disk directory. Persistence is local to the node that hosts the
//! contract, so a single node is the simplest faithful repro for the redb
//! flush/reload of state + parameters + the subscription-registration path.
//!
//! Why two OS PROCESSES, not an in-process restart: the contract executor that
//! owns the redb `Database` and the WebSocket API server are spawned on the
//! process-wide `GlobalExecutor` (see `node/p2p_impl.rs`) and are NOT torn down
//! by a node's graceful shutdown — they keep `op_manager` clones alive, so the
//! redb file lock is never released and the WS server keeps answering within
//! the same process. An in-process "restart" therefore can't actually reload
//! from disk: node 2 fails to open the locked redb and node 1's still-live
//! server silently answers the GET, validating nothing. Running node 1 and
//! node 2 as separate `freenet network` subprocesses makes node 1's exit
//! release the OS file lock and kill its server, so the post-restart GET is
//! genuinely served by node 2 reading the contract back from disk.
//!
//! Scope note: UPDATE delivery (the notification half of a subscription) routes
//! through the network layer and so cannot complete on an isolated single node.
//! Reliable 2-node connectivity in turn is only provided by the `#[freenet_test]`
//! macro, which has no stop/restart-against-the-same-dir hook. This test
//! therefore verifies the on-disk round-trip and that a GET-with-subscribe is
//! *accepted* on the restarted node against the reloaded contract store — which
//! is exactly the #3352 startup failure surface (a broken store rejects the
//! request) — but does not assert listener registration or cross-node UPDATE
//! propagation, neither of which is observable from a single isolated node.

use freenet::test_utils::{create_empty_todo_list, load_contract};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tracing::info;

const TEST_CONTRACT: &str = "test-contract-integration";

/// Non-empty parameters so the on-disk round-trip actually carries something
/// to lose. The contract key is `hash(code || params)`, so these bytes are
/// baked into the key and MUST survive a restart for a GET to resolve.
const CONTRACT_PARAMS: &[u8] = b"persistence-roundtrip-params-3367";

// ---------------------------------------------------------------------------
// Binary / path resolution (mirrors fdev_publish_e2e.rs)
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

/// Path to the `freenet` node binary built by `cargo build` from this
/// workspace. CI's `test_unit` job runs `cargo build --locked` before the
/// tests, so this is on disk by then. Local devs need `cargo build --bin
/// freenet` first.
fn freenet_bin() -> PathBuf {
    let debug = target_dir().join("debug").join("freenet");
    if debug.exists() {
        return debug;
    }
    let release = target_dir().join("release").join("freenet");
    assert!(
        release.exists(),
        "freenet binary not found at {debug:?} or {release:?}. Build it first: \
         `cargo build --bin freenet` (CI's test_unit job builds it before tests)."
    );
    release
}

// ---------------------------------------------------------------------------
// Subprocess node lifecycle
// ---------------------------------------------------------------------------

/// A `freenet network` gateway subprocess. Killed and reaped on drop so a
/// panicking assertion never leaks a node process.
struct NodeProcess {
    child: Child,
    ws_port: u16,
}

impl NodeProcess {
    /// Spawn a single self-contained gateway node whose config/data/log all
    /// live under `dir`. Starting another node against the same `dir` reuses
    /// the persisted redb stores. `transport_keypair` is reused across the
    /// restart so the node keeps a stable identity.
    fn spawn(
        dir: &Path,
        ws_port: u16,
        network_port: u16,
        transport_keypair: &Path,
    ) -> anyhow::Result<Self> {
        let child = Command::new(freenet_bin())
            .arg("network")
            // This spawns the REAL release-style `freenet` binary as a separate
            // process. Unlike in-process `#[freenet_test]` nodes (which run from
            // the cargo `deps/` harness and are auto-suppressed by
            // `running_under_cargo_test()`), this subprocess runs from
            // `target/<profile>/freenet` with no `--id`, so neither test signal
            // fires and telemetry would default ON — POSTing release-tagged
            // events to the production OTLP collector (#4366 contamination
            // class). Pin it OFF via the same env the binary reads for the
            // `telemetry-enabled` flag (config.rs: env = "FREENET_TELEMETRY_ENABLED"),
            // which is also how freenet-test-network disables it for spawned nodes.
            .env("FREENET_TELEMETRY_ENABLED", "false")
            .args(["--ws-api-address", "127.0.0.1"])
            .args(["--ws-api-port", &ws_port.to_string()])
            .args(["--network-address", "127.0.0.1"])
            .args(["--network-port", &network_port.to_string()])
            .args(["--public-network-address", "127.0.0.1"])
            .args(["--public-network-port", &network_port.to_string()])
            .arg("--is-gateway")
            .arg("--skip-load-from-network")
            .arg("--ignore-protocol-checking")
            .args(["--location", "0.5"])
            .args(["--config-dir", &dir.to_string_lossy()])
            .args(["--data-dir", &dir.to_string_lossy()])
            .args(["--log-dir", &dir.to_string_lossy()])
            .args(["--transport-keypair", &transport_keypair.to_string_lossy()])
            // Inherit stdio so node logs interleave with the test output under
            // `--nocapture`; harmless when captured.
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;
        Ok(Self { child, ws_port })
    }

    /// True once the node has exited (so the WS port will never come up).
    fn has_exited(&mut self) -> anyhow::Result<bool> {
        Ok(self.child.try_wait()?.is_some())
    }

    /// Stop the node: SIGKILL + reap. Process exit is what releases the redb
    /// file lock and tears down the WS server, so the next node can open the
    /// same data dir cleanly. (SIGKILL rather than a graceful stop keeps the
    /// teardown deterministic; redb commits synchronously per write, so the
    /// PUT is already durable on disk well before this point.)
    fn stop(mut self) -> anyhow::Result<()> {
        self.child.kill()?;
        self.child.wait()?;
        Ok(())
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        // Best-effort cleanup if `stop` wasn't called (e.g. a panic unwound
        // past it). Ignore errors — the process may already be gone.
        if self.child.kill().is_ok() {
            let _reaped = self.child.wait().is_ok();
        }
    }
}

/// Reserve an ephemeral port by binding then immediately freeing it. The kernel
/// won't hand the same port back on the next `:0` request, so this races far
/// less than a fixed port; the node binds it a moment later.
fn reserve_port() -> anyhow::Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

async fn connect(ws_port: u16) -> anyhow::Result<WebApi> {
    let url = format!("ws://127.0.0.1:{ws_port}/v1/contract/command?encodingProtocol=native");
    let (ws_stream, _) = connect_async(&url).await?;
    Ok(WebApi::start(ws_stream))
}

/// Poll the node's WS API until it accepts a connection, up to ~60s. Replaces a
/// fixed sleep: a freshly spawned subprocess (compile cache cold, redb reload on
/// the restart) can take a while on a loaded self-hosted runner, while on an
/// idle box it's ready in a second or two. The node process is checked on each
/// miss so a node that died during startup surfaces as a clear error instead of
/// a generic connect timeout. Returns a connected client once ready.
async fn wait_for_ws(node: &mut NodeProcess) -> anyhow::Result<WebApi> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if node.has_exited()? {
            anyhow::bail!(
                "freenet node process exited before its WS API on port {} came up",
                node.ws_port
            );
        }
        if let Ok(client) = connect(node.ws_port).await {
            return Ok(client);
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("WS API on port {} did not come up within 60s", node.ws_port);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Await a specific `HostResponse` variant, ignoring (logging) any others, up
/// to `secs` seconds.
macro_rules! recv_until {
    ($client:expr, $secs:expr, $pat:pat => $body:expr) => {{
        let deadline = tokio::time::Instant::now() + Duration::from_secs($secs);
        loop {
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for expected response");
            }
            match timeout(Duration::from_secs(5), $client.recv()).await {
                Ok(Ok($pat)) => break $body,
                Ok(Ok(other)) => info!("ignoring response while waiting: {other:?}"),
                // A protocol/transport error from the client won't fix itself by
                // re-looping; surface it immediately with context instead of
                // spinning until the deadline and reporting a generic timeout.
                Ok(Err(e)) => anyhow::bail!("client error while waiting for response: {e}"),
                Err(_) => {}
            }
        }
    }};
}

/// PUT a contract WITH parameters, stop the node so the on-disk redb store is
/// the only surviving copy, restart from the same data dir, then
/// GET-with-subscribe and assert the state AND parameters round-tripped and
/// that subscribe-on-restart is accepted (#3352).
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn test_persistence_roundtrip_state_and_params() -> anyhow::Result<()> {
    // Pre-compile the contract before the timed section so cargo build time
    // doesn't eat into the test budget.
    freenet::test_utils::ensure_contract_compiled(TEST_CONTRACT)?;

    // Data dir we own and reuse across the restart — this is the whole point:
    // the first node flushes to it, the second node reloads from it.
    let data_dir = tempfile::tempdir()?;
    let data_path = data_dir.path().to_path_buf();

    // Stable transport identity reused across the restart.
    let key = freenet::dev_tool::TransportKeypair::new();
    let transport_keypair = data_path.join("private.pem");
    key.save(&transport_keypair)?;
    key.public().save(data_path.join("public.pem"))?;

    // Distinct ephemeral ports per phase. The processes don't overlap in time
    // (node 1 is killed before node 2 starts), but using fresh ports avoids any
    // TIME_WAIT bind contention on the restart. The DATA DIR is what must be
    // shared across the restart; the ports must not.
    let (ws_port, network_port) = (reserve_port()?, reserve_port()?);
    let (ws_port2, network_port2) = (reserve_port()?, reserve_port()?);

    let contract = load_contract(TEST_CONTRACT, Parameters::from(CONTRACT_PARAMS.to_vec()))?;
    let contract_key = contract.key();
    info!(%contract_key, "loaded contract with non-empty parameters");

    // The contract key must embed the parameters; a zero-param load would have
    // a different key. Guard the premise of the test.
    let empty_param_key = load_contract(TEST_CONTRACT, Parameters::from(vec![]))?.key();
    assert_ne!(
        contract_key, empty_param_key,
        "parameters must be part of the contract key for this test to be meaningful"
    );

    // Initial state: a todo list with one task so we can detect state loss.
    let mut todo: freenet::test_utils::TodoList =
        serde_json::from_slice(&create_empty_todo_list())?;
    todo.tasks.push(freenet::test_utils::Task {
        id: 7,
        title: "survive a restart".to_string(),
        description: "state and params must round-trip through redb".to_string(),
        completed: false,
        priority: 3,
    });
    let initial_state = WrappedState::from(serde_json::to_vec(&todo)?);

    // ---- Phase 1: first node — PUT with params, then stop ----
    let mut node1 = NodeProcess::spawn(&data_path, ws_port, network_port, &transport_keypair)?;
    let mut client = wait_for_ws(&mut node1).await?;
    client
        .send(ClientRequest::ContractOp(ContractRequest::Put {
            contract: contract.clone(),
            state: initial_state.clone(),
            related_contracts: Default::default(),
            subscribe: false,
            blocking_subscribe: false,
        }))
        .await?;
    let put_key = recv_until!(client, 30,
        HostResponse::ContractResponse(ContractResponse::PutResponse { key }) => key);
    assert_eq!(put_key, contract_key, "PUT acknowledged a different key");
    info!("PUT acknowledged; killing node 1 to release the redb store");

    // Drop the client, then kill node 1. redb commits synchronously on each
    // write (`txn.commit()`), so the contract is already durable on disk at
    // PUT-ack time — killing the process here releases the file lock and the WS
    // server so the restart below proves the data is read back from a *fresh*
    // store, not handed over by a still-live in-process server.
    drop(client);
    node1.stop()?;

    // ---- Phase 2: restart from the SAME data dir, fresh ports ----
    let mut node2 = NodeProcess::spawn(&data_path, ws_port2, network_port2, &transport_keypair)?;
    let mut client = wait_for_ws(&mut node2).await?;

    // ---- GET-with-subscribe: assert persisted state AND parameters,
    //      and that subscribe-on-restart is accepted ----
    // `subscribe: true` drives the node's subscribe-on-GET path for the
    // locally-hosted contract (the same path River/HTTP clients use). On a
    // freshly restarted node this is exactly the #3352 startup failure surface:
    // the request must be accepted against the reloaded redb-backed contract
    // store rather than rejected because the store failed to reload. (Listener
    // registration itself isn't observable from a single isolated node, so we
    // assert acceptance, not delivery.)
    client
        .send(ClientRequest::ContractOp(ContractRequest::Get {
            key: *contract_key.id(),
            return_contract_code: true,
            subscribe: true,
            blocking_subscribe: false,
        }))
        .await?;
    let (got_contract, got_state) = recv_until!(client, 30,
        HostResponse::ContractResponse(ContractResponse::GetResponse {
            contract: Some(c), state, ..
        }) => (c, state));

    // Parameters survived the on-disk round-trip (#3350).
    assert_eq!(
        got_contract.params().as_ref(),
        CONTRACT_PARAMS,
        "parameters were lost across restart (#3350)"
    );
    // Key is derived from code+params, so it must match too.
    assert_eq!(
        got_contract.key(),
        contract_key,
        "contract key changed across restart"
    );
    // State survived the on-disk round-trip.
    let recovered: freenet::test_utils::TodoList = serde_json::from_slice(got_state.as_ref())?;
    assert_eq!(
        recovered.tasks.len(),
        1,
        "persisted state lost its task across restart"
    );
    assert_eq!(recovered.tasks[0].id, 7, "persisted task id changed");
    assert_eq!(
        recovered.tasks[0].title, "survive a restart",
        "persisted task title changed"
    );
    info!("GET-with-subscribe after restart returned correct state and parameters");

    // A second GET (no subscribe) must still resolve from the reloaded store —
    // confirms the contract remains durably hosted, not a one-shot artifact of
    // the first read.
    client
        .send(ClientRequest::ContractOp(ContractRequest::Get {
            key: *contract_key.id(),
            return_contract_code: true,
            subscribe: false,
            blocking_subscribe: false,
        }))
        .await?;
    let second = recv_until!(client, 30,
        HostResponse::ContractResponse(ContractResponse::GetResponse {
            contract: Some(c), ..
        }) => c);
    assert_eq!(
        second.params().as_ref(),
        CONTRACT_PARAMS,
        "second GET after restart returned wrong parameters"
    );
    info!("second GET after restart still returns the persisted contract");

    drop(client);
    node2.stop()?;

    Ok(())
}
