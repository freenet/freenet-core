//! In-process graceful-shutdown / restart round-trip test.
//!
//! Issue #4401: a node's graceful shutdown (`ShutdownHandle::shutdown()`) tore
//! down the network event loop but left the detached contract-executor and
//! WebSocket-server tasks (spawned on the process-wide `GlobalExecutor`)
//! running. Those tasks kept holding the redb on-disk file lock and kept
//! serving on the node's bound ports after the node had "shut down". As a
//! result an in-process restart against the same data dir would either:
//!   1. fail — node 2's redb open returns `DatabaseAlreadyOpen` in-process (the
//!      surviving process still holds the lock); cross-process the same condition
//!      surfaces as an OS file-lock block. Either way the restart can't proceed, OR
//!   2. silently pass — node 1's still-live WS server answers the GET, so the
//!      "restart" validated nothing.
//!
//! This is the exact scenario the process-based `persistence_roundtrip.rs`
//! (PR #4398) had to work around by running the two nodes as separate OS
//! processes. With the fix, shutdown aborts the detached tasks and drops the
//! redb `Database` clones, so node 2 can open the same data dir IN THE SAME
//! PROCESS.
//!
//! Negative control: after node 1's `run()` future returns we assert the redb
//! lock is released within a tight bound by opening the database directly.
//! Without the fix the executor task still holds the lock, so this open blocks
//! and the bounded wait fails — proving the test is load-bearing.
//!
//! Binds `127.0.0.1` (a single loopback address, not the full 127/8 range), so
//! unlike the connectivity tests it runs on macOS as well as Linux CI.

use freenet::{
    local_node::NodeConfig,
    server::serve_client_api,
    test_utils::{load_contract, make_get, make_put},
};
use freenet_stdlib::{
    client_api::{ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use std::{
    net::{Ipv4Addr, TcpListener},
    path::Path,
    time::{Duration, Instant},
};
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tracing::info;

const TEST_CONTRACT: &str = "test-contract-integration";

/// Reserve an ephemeral port by binding then freeing it, so the node can rebind
/// a moment later. Distinct ports per phase avoid TIME_WAIT contention; only the
/// DATA DIR is shared across the restart.
fn reserve_port() -> anyhow::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

/// Build a single self-contained gateway node config rooted at `dir`, serving
/// its WS API on `ws_port` and network on `network_port`. Reusing `dir` across a
/// restart reuses the persisted redb stores; reusing `keypair_path` keeps a
/// stable node identity.
fn node_config(
    dir: &Path,
    ws_port: u16,
    network_port: u16,
    keypair_path: &Path,
) -> freenet::config::ConfigArgs {
    freenet::config::ConfigArgs {
        ws_api: freenet::config::WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(ws_port),
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

/// Connect to the node's WS API, retrying until it accepts a connection or the
/// deadline passes. The deadline is what makes the negative control bite: if
/// node 2's redb open blocks on a lock node 1 never released, its WS API never
/// comes up and this returns an error rather than hanging the whole test.
async fn connect_ws(port: u16, within: Duration) -> anyhow::Result<WebApi> {
    let url = format!("ws://127.0.0.1:{port}/v1/contract/command?encodingProtocol=native");
    let deadline = Instant::now() + within;
    loop {
        match connect_async(&url).await {
            Ok((ws_stream, _)) => return Ok(WebApi::start(ws_stream)),
            Err(e) => {
                if Instant::now() >= deadline {
                    anyhow::bail!("WS API on port {port} did not come up within {within:?}: {e}");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Await a `PutResponse`/`GetResponse`, ignoring other responses, up to `secs`.
macro_rules! recv_until {
    ($client:expr, $secs:expr, $pat:pat => $body:expr) => {{
        let deadline = Instant::now() + Duration::from_secs($secs);
        loop {
            if Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for expected response");
            }
            match timeout(Duration::from_secs(5), $client.recv()).await {
                Ok(Ok($pat)) => break $body,
                Ok(Ok(other)) => info!("ignoring response while waiting: {other:?}"),
                Ok(Err(e)) => anyhow::bail!("client error while waiting: {e}"),
                Err(_) => {}
            }
        }
    }};
}

// Runtime-ignore under sqlite (mirrors `tests/redb_migration.rs`): this test
// probes the redb on-disk layout (`<data_dir>/db/db`) and opens `redb::Database`
// directly. Under `--no-default-features --features sqlite` the node writes a
// sqlite DB instead, so the probe would mis-target. Kept as a runtime `#[ignore]`
// (not a file-level `#![cfg]`) so the file still compiles under sqlite.
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
#[cfg_attr(not(feature = "redb"), ignore)]
async fn test_in_process_restart_releases_redb_lock() -> anyhow::Result<()> {
    freenet::test_utils::ensure_contract_compiled(TEST_CONTRACT)?;

    // Data dir reused across the in-process restart — the whole point of the
    // test: node 1 flushes to it, node 2 reloads from it WITHOUT a process exit
    // in between.
    let data_dir = tempfile::tempdir()?;
    let data_path = data_dir.path().to_path_buf();
    // The node lays out its redb file at `<data_dir>/db/db`: `Config::db_dir()`
    // is `<data_dir>/db`, and `ReDb::new` joins `"db"` to that. This is the file
    // whose OS lock the negative control probes.
    let db_path = data_path.join("db").join("db");

    // Stable transport identity reused across the restart.
    let key = freenet::dev_tool::TransportKeypair::new();
    let keypair_path = data_path.join("private.pem");
    key.save(&keypair_path)?;
    key.public().save(data_path.join("public.pem"))?;

    let (ws_port1, net_port1) = (reserve_port()?, reserve_port()?);
    let (ws_port2, net_port2) = (reserve_port()?, reserve_port()?);

    let contract = load_contract(TEST_CONTRACT, Parameters::from(vec![]))?;
    let contract_key = contract.key();
    let initial_state = WrappedState::from(freenet::test_utils::create_empty_todo_list());

    // ---- Phase 1: node 1 — PUT a contract, then graceful shutdown ----
    let cfg1 = node_config(&data_path, ws_port1, net_port1, &keypair_path)
        .build()
        .await?;
    let node1 = NodeConfig::new(cfg1.clone())
        .await?
        .build(serve_client_api(cfg1.ws_api.clone()).await?)
        .await?;
    let shutdown1 = node1.shutdown_handle();
    // Drive the node on its own task; `run()` returns (with an Err carrying the
    // shutdown cause) once the event loop tears down, at which point the node —
    // and every redb `Database` clone it owns, now that the detached tasks are
    // aborted — has been dropped.
    let run1 = tokio::spawn(async move { node1.run().await });

    {
        let mut client = connect_ws(ws_port1, Duration::from_secs(30)).await?;
        make_put(&mut client, initial_state.clone(), contract.clone(), false).await?;
        let put_key = recv_until!(client, 30,
            HostResponse::ContractResponse(ContractResponse::PutResponse { key }) => key);
        assert_eq!(put_key, contract_key, "PUT acknowledged a different key");
        info!("node 1 PUT acknowledged; triggering graceful shutdown");
        // Drop the client so its WS connection closes before we shut down.
    }

    // Graceful shutdown, then wait for the node's run loop to actually return.
    shutdown1.shutdown().await;
    let run1_result = timeout(Duration::from_secs(30), run1)
        .await
        .map_err(|_| anyhow::anyhow!("node 1 run loop did not exit within 30s of shutdown()"))?
        .map_err(|e| anyhow::anyhow!("node 1 run task panicked: {e}"))?;
    info!(?run1_result, "node 1 run loop exited after shutdown");

    // ---- Negative control: the redb lock must be released promptly ----
    //
    // Before the fix the detached executor task survives shutdown and keeps the
    // redb `Database` open, so this direct open returns `DatabaseAlreadyOpen`
    // in-process (and the bounded loop below fails). With the fix the lock is
    // released shortly after the run loop returns — "shortly" rather than
    // "instantly" because abort only signals the executor task; its redb clones
    // drop when the runtime next polls it to completion, hence the 10s bound.
    let lock_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match try_open_redb(&db_path) {
            Ok(()) => {
                info!(
                    elapsed_ms = lock_deadline
                        .saturating_duration_since(Instant::now())
                        .as_millis(),
                    "redb lock released after shutdown"
                );
                break;
            }
            Err(e) => {
                assert!(
                    Instant::now() < lock_deadline,
                    "redb file lock was not released within 10s of node 1 shutdown \
                     (detached executor task still holding it?): {e}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    // ---- Phase 2: node 2 — restart IN THE SAME PROCESS, same data dir ----
    let cfg2 = node_config(&data_path, ws_port2, net_port2, &keypair_path)
        .build()
        .await?;
    let node2 = NodeConfig::new(cfg2.clone())
        .await?
        .build(serve_client_api(cfg2.ws_api.clone()).await?)
        .await?;
    let shutdown2 = node2.shutdown_handle();
    let run2 = tokio::spawn(async move { node2.run().await });

    // If node 2 could not open the locked redb (pre-fix), its WS API never comes
    // up and this fails within the bound rather than hanging forever.
    let mut client = connect_ws(ws_port2, Duration::from_secs(30)).await?;
    make_get(&mut client, contract_key, true, false).await?;
    let got_state = recv_until!(client, 30,
        HostResponse::ContractResponse(ContractResponse::GetResponse { state, .. }) => state);
    assert_eq!(
        got_state.as_ref(),
        initial_state.as_ref(),
        "node 2 served different state than node 1 persisted (stale server or bad reload)"
    );
    info!("node 2 GET after in-process restart returned the persisted state");

    drop(client);
    shutdown2.shutdown().await;
    // Best-effort cleanup of node 2's run loop; the assertions above are what
    // the test proves, so a slow teardown here should not fail it.
    if timeout(Duration::from_secs(30), run2).await.is_err() {
        info!("node 2 run loop did not exit within 30s of shutdown (cleanup only)");
    }

    Ok(())
}

/// Open the redb database file directly, returning Ok only if the OS file lock
/// is currently free. Used as the negative control: a still-running executor
/// task from node 1 would hold this lock and make the open block/fail.
fn try_open_redb(db_path: &Path) -> anyhow::Result<()> {
    // `Database::create` acquires the same exclusive file lock the node's
    // contract store takes, so a successful open here proves the lock is free.
    let db = redb::Database::create(db_path)?;
    drop(db);
    Ok(())
}
