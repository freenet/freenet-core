#![cfg(feature = "nightly_tests")]

use anyhow::{anyhow, bail, ensure};
use freenet::{
    config::{ConfigArgs, ConfigPathsArgs, InlineGwConfig, NetworkArgs, SecretArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_client_api_with_listener_and_contracts,
    test_utils::{
        self, NodeInfo, TestContext, create_large_todo_list, load_contract, make_get, make_put,
    },
};
use freenet_macros::freenet_test;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractResponse, HostResponse, NodeQuery, QueryResponse, WebApi},
    prelude::*,
};
use std::time::Duration;
use tokio_tungstenite::connect_async;

const NUM_SMALL_CONTRACTS: usize = 3;
/// Settle time after the PUTs before booting the late-joining getter node.
const PUT_SETTLE_SECS: u64 = 5;
/// Retention gap between the two GET phases. Must be strictly greater than
/// `SUBSCRIPTION_LEASE_DURATION` (480s, `crates/core/src/ring/hosting.rs`) so
/// phase 2 exercises the lease-renewal machinery — every shorter timer
/// (renewal interval 120s) is crossed as well. A short gap would make phase 2
/// a duplicate of phase 1: no retention/eviction path fires below 120s.
const RETENTION_DELAY_SECS: u64 = 540;
/// Single overall deadline per GET. There is deliberately NO retry loop: the
/// nightly nextest profile sets retries=0 because a GET that only succeeds on
/// a retry is exactly the findability regression this test exists to surface.
const GET_TIMEOUT_SECS: u64 = 120;
const JOIN_TIMEOUT_SECS: u64 = 60;
/// Deterministic ring location for the late-joining getter node, away from
/// the macro nodes' locations (gateway 0.1, peer-a 0.5).
const PEER_B_LOCATION: f64 = 0.9;

/// One PUT contract tracked through both GET phases.
struct PutContract {
    key: ContractKey,
    label: String,
    contract: ContractContainer,
    state: WrappedState,
}

/// Discard any queued responses so a stale message from a previous
/// interaction cannot be mis-attributed to the next request.
async fn drain_stray_responses(client: &mut WebApi) {
    loop {
        match tokio::time::timeout(Duration::from_millis(200), client.recv()).await {
            Ok(Ok(resp)) => tracing::warn!("Discarding stray response: {:?}", resp),
            Ok(Err(err)) => tracing::warn!("Discarding stray error: {}", err),
            Err(_) => break,
        }
    }
}

/// Issue a single GET and wait for the matching response within one overall
/// deadline. Responses for other keys (or non-GET responses) are skipped;
/// only a `GetResponse` whose key matches is accepted.
async fn get_contract(
    client: &mut WebApi,
    key: ContractKey,
    label: &str,
) -> anyhow::Result<(ContractContainer, WrappedState)> {
    drain_stray_responses(client).await;
    make_get(client, key, true, false).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(GET_TIMEOUT_SECS);
    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or_else(|| anyhow!("GET {label} ({key}) timed out after {GET_TIMEOUT_SECS}s"))?;
        match tokio::time::timeout(remaining, client.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key: resp_key,
                contract,
                state,
            }))) => {
                if resp_key != key {
                    tracing::warn!(
                        "GetResponse for different key {resp_key} while waiting for {key}, ignoring"
                    );
                    continue;
                }
                let contract = contract
                    .ok_or_else(|| anyhow!("GET {label} ({key}) returned no contract code"))?;
                ensure!(
                    contract.key() == key,
                    "GET {label}: contract key mismatch (got {}, want {key})",
                    contract.key()
                );
                return Ok((contract, state));
            }
            Ok(Ok(other)) => {
                tracing::warn!(
                    "Unexpected response while waiting for GET {label}: {:?}",
                    other
                );
            }
            Ok(Err(e)) => bail!("GET {label} ({key}): websocket error: {e}"),
            Err(_) => bail!("GET {label} ({key}) timed out after {GET_TIMEOUT_SECS}s"),
        }
    }
}

async fn put_and_wait(
    client: &mut WebApi,
    state: WrappedState,
    contract: ContractContainer,
    description: &str,
) -> anyhow::Result<ContractKey> {
    let expected_key = contract.key();
    tracing::info!(
        "PUTting {description} (state size: {} bytes)",
        state.as_ref().len()
    );
    make_put(client, state, contract, false).await?;

    match tokio::time::timeout(Duration::from_secs(GET_TIMEOUT_SECS), client.recv()).await {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            ensure!(key == expected_key, "PUT key mismatch for {description}");
            tracing::info!("PUT {description} succeeded (key: {key})");
            Ok(key)
        }
        Ok(Ok(other)) => bail!("PUT {description}: unexpected response: {:?}", other),
        Ok(Err(e)) => bail!("PUT {description}: error receiving response: {e}"),
        Err(_) => bail!("PUT {description}: timed out after {GET_TIMEOUT_SECS}s"),
    }
}

/// GET every contract via `client` and verify the returned contract and state
/// are byte-identical to what was PUT.
async fn get_and_verify_all(
    client: &mut WebApi,
    contracts: &[PutContract],
    phase: &str,
) -> anyhow::Result<()> {
    for c in contracts {
        tracing::info!("{phase}: GET {} ({}) via peer-b...", c.label, c.key);
        let (resp_contract, resp_state) = get_contract(client, c.key, &c.label).await?;
        ensure!(
            resp_contract == c.contract,
            "{phase}: GET {} returned a different contract container",
            c.label
        );
        ensure!(
            resp_state == c.state,
            "{phase}: GET {} returned different state (got {} bytes, want {} bytes)",
            c.label,
            resp_state.as_ref().len(),
            c.state.as_ref().len()
        );
        tracing::info!(
            "{phase}: GET {} OK (state size: {} bytes)",
            c.label,
            resp_state.as_ref().len()
        );
    }
    Ok(())
}

/// Build (but do not run) a fresh peer node that bootstraps via the test
/// gateway, mirroring the node construction `#[freenet_test]` generates. The
/// caller drives the returned run-future. Booting this node AFTER the PUTs is
/// the crux of the test: a late joiner cannot hold any replica, so its GETs
/// must route over the network — a GET served from a local copy (which is
/// what happens on the gateway itself, since every PUT hop stores locally)
/// would not detect a broken network-GET path.
async fn build_late_peer(
    gateway: &NodeInfo,
) -> anyhow::Result<(
    impl std::future::Future<Output = anyhow::Result<std::convert::Infallible>>,
    String,
    tempfile::TempDir,
)> {
    // ponytail: IP may collide with a macro node if tasks migrated threads
    // (the allocator is thread-local); distinct reserved ports keep that safe.
    let base_idx = test_utils::allocate_test_node_block(1);
    let ip = test_utils::test_ip_for_node(base_idx);
    let network_port = test_utils::reserve_local_port_on_ip(ip)?;
    let ws_port = test_utils::reserve_local_port_on_ip(ip)?;

    let temp_dir = tempfile::tempdir()?;
    let keypair = TransportKeypair::new();
    let transport_keypair_path = temp_dir.path().join("private.pem");
    keypair.save(&transport_keypair_path)?;
    keypair.public().save(temp_dir.path().join("public.pem"))?;

    let gateway_info = InlineGwConfig {
        address: (
            std::net::IpAddr::V4(gateway.ip),
            gateway
                .network_port
                .ok_or_else(|| anyhow!("gateway node has no network port"))?,
        )
            .into(),
        location: Some(gateway.location),
        public_key_path: gateway.temp_dir_path.join("public.pem"),
    };

    let config = ConfigArgs {
        ws_api: freenet::config::WebsocketApiArgs {
            address: Some(ip.into()),
            ws_api_port: Some(ws_port),
            ..Default::default()
        },
        network_api: NetworkArgs {
            public_address: Some(ip.into()),
            public_port: Some(network_port),
            is_gateway: false,
            skip_load_from_network: true,
            gateways: Some(vec![serde_json::to_string(&gateway_info)?]),
            location: Some(PEER_B_LOCATION),
            ignore_protocol_checking: true,
            address: Some(ip.into()),
            network_port: Some(network_port),
            ..Default::default()
        },
        config_paths: ConfigPathsArgs {
            config_dir: Some(temp_dir.path().to_path_buf()),
            data_dir: Some(temp_dir.path().to_path_buf()),
            log_dir: Some(temp_dir.path().to_path_buf()),
        },
        secrets: SecretArgs {
            transport_keypair: Some(transport_keypair_path),
            ..Default::default()
        },
        ..Default::default()
    };

    test_utils::release_local_port(network_port);
    let ws_listener = test_utils::take_reserved_tcp_listener(ws_port)
        .ok_or_else(|| anyhow!("ws port {ws_port} was not reserved"))?;
    let built = config.build().await?;
    let mut node_config = NodeConfig::new(built.clone()).await?;
    node_config.relay_ready_connections(Some(0));
    node_config.min_number_of_connections(1);
    node_config.max_number_of_connections(4);
    let (api_clients, _origin_contracts) =
        serve_client_api_with_listener_and_contracts(built.ws_api, ws_listener).await?;
    // The flush handle only forces event-log flushes on drop; this extra
    // node's log is not part of the macro's aggregation, so it can go.
    let (node, _flush_handle) = node_config.build_with_flush_handle(api_clients).await?;

    let ws_url = format!("ws://{ip}:{ws_port}/v1/contract/command?encodingProtocol=native");
    Ok((node.run(), ws_url, temp_dir))
}

/// Connect to a freshly booted node's websocket API, retrying while the
/// server comes up.
async fn connect_ws_with_retry(ws_url: &str) -> anyhow::Result<WebApi> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(JOIN_TIMEOUT_SECS);
    loop {
        match connect_async(ws_url).await {
            Ok((stream, _)) => return Ok(WebApi::start(stream)),
            Err(e) if tokio::time::Instant::now() < deadline => {
                tracing::info!("peer-b ws not ready yet ({e}), retrying...");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => bail!("could not connect to peer-b ws api at {ws_url}: {e}"),
        }
    }
}

/// Wait until the node reports at least one ring connection.
async fn wait_for_ring_join(client: &mut WebApi) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(JOIN_TIMEOUT_SECS);
    loop {
        client
            .send(ClientRequest::NodeQueries(NodeQuery::ConnectedPeers))
            .await?;
        match tokio::time::timeout(Duration::from_secs(5), client.recv()).await {
            Ok(Ok(HostResponse::QueryResponse(QueryResponse::ConnectedPeers { peers }))) => {
                if !peers.is_empty() {
                    tracing::info!("peer-b joined the ring ({} connection(s))", peers.len());
                    return Ok(());
                }
            }
            Ok(Ok(other)) => tracing::warn!("unexpected response to ConnectedPeers: {:?}", other),
            Ok(Err(e)) => tracing::warn!("ConnectedPeers query error: {e}"),
            Err(_) => tracing::warn!("ConnectedPeers query timed out"),
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "peer-b did not join the ring within {JOIN_TIMEOUT_SECS}s"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Automated daily network test: PUT contracts via one peer, GET them via another.
///
/// Exercises the core end-to-end invariant of issue #4665: one peer PUTs
/// several contracts (including a ~1 MB payload) and a DIFFERENT peer — one
/// that joins the network only AFTER the PUTs, so it cannot hold any replica
/// and its GETs must actually route over the network — retrieves them,
/// byte-identical, both right away and again after a retention gap that
/// crosses the subscription-lease boundary (480s).
///
/// Ref: <https://github.com/freenet/freenet-core/issues/4665>
#[freenet_test(
    health_check_readiness = true,
    nodes = ["gateway", "peer-a"],
    node_locations = [0.1, 0.5],
    timeout_secs = 1200,
    startup_wait_secs = 30,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4,
    aggregate_events = "always"
)]
async fn test_cross_peer_put_get_retention(ctx: &mut TestContext) -> TestResult {
    test_utils::ensure_contract_compiled("test-contract-integration")?;

    let peer_a = ctx.node("peer-a")?;
    let gateway = ctx.node("gateway")?;
    tracing::info!("peer-a ws_port: {}", peer_a.ws_port);
    tracing::info!("gateway ws_port: {}", gateway.ws_port);

    // ---- PUT phase: everything goes in via peer-a ----
    let (stream_a, _) = connect_async(&peer_a.ws_url()).await?;
    let mut client_a = WebApi::start(stream_a);

    let mut contracts: Vec<PutContract> = Vec::new();

    for i in 0..NUM_SMALL_CONTRACTS {
        let params = Parameters::from(format!("daily-test-small-{i}").into_bytes());
        let contract = load_contract("test-contract-integration", params)?;
        let state = WrappedState::from(test_utils::create_todo_list_with_item(&format!(
            "small-contract-{i}"
        )));
        let key = put_and_wait(
            &mut client_a,
            state.clone(),
            contract.clone(),
            &format!("small contract {i}/{NUM_SMALL_CONTRACTS}"),
        )
        .await?;
        contracts.push(PutContract {
            key,
            label: format!("small-{i}"),
            contract,
            state,
        });
    }

    let large_params = Parameters::from(b"daily-test-large-payload".to_vec());
    let large_contract = load_contract("test-contract-integration", large_params)?;
    let large_state = WrappedState::from(create_large_todo_list());
    let large_key = put_and_wait(
        &mut client_a,
        large_state.clone(),
        large_contract.clone(),
        "large (~1MB) contract",
    )
    .await?;
    contracts.push(PutContract {
        key: large_key,
        label: "large-1MB".to_string(),
        contract: large_contract,
        state: large_state,
    });

    client_a
        .send(ClientRequest::Disconnect { cause: None })
        .await?;

    tracing::info!(
        "All {} contracts PUT via peer-a. Settling {PUT_SETTLE_SECS}s before booting peer-b...",
        contracts.len()
    );
    tokio::time::sleep(Duration::from_secs(PUT_SETTLE_SECS)).await;

    // ---- Boot the late-joining getter node ----
    let (node_b_fut, peer_b_ws_url, _peer_b_tmp) = build_late_peer(gateway).await?;
    tokio::pin!(node_b_fut);

    let phases = async {
        let mut client_b = connect_ws_with_retry(&peer_b_ws_url).await?;
        wait_for_ring_join(&mut client_b).await?;

        tracing::info!("Phase 1: GET each contract via late-joining peer-b (network GET)");
        get_and_verify_all(&mut client_b, &contracts, "phase-1").await?;

        tracing::info!(
            "Phase 1 complete. Waiting {RETENTION_DELAY_SECS}s (past the 480s \
             subscription-lease boundary) for the retention check..."
        );
        tokio::time::sleep(Duration::from_secs(RETENTION_DELAY_SECS)).await;

        tracing::info!("Phase 2: GET each contract again (retention over time)");
        get_and_verify_all(&mut client_b, &contracts, "phase-2").await?;

        tracing::info!(
            "All {} contracts retrievable cross-peer after {RETENTION_DELAY_SECS}s retention gap",
            contracts.len()
        );

        client_b
            .send(ClientRequest::Disconnect { cause: None })
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        anyhow::Ok(())
    };

    tokio::select! {
        res = &mut node_b_fut => bail!("peer-b node exited prematurely: {res:?}"),
        res = phases => res?,
    }

    Ok(())
}
