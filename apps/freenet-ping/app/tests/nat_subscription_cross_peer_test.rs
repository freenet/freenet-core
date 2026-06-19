//! Docker NAT integration test for cross-peer subscription delivery (issue #2199).
//!
//! This complements `docker_nat_test::test_contract_operations_via_docker_nat`
//! (and the core-level loopback test added in PR #4390) by closing the specific
//! gap #2199 cares about: a peer behind NAT subscribes through a gateway, and an
//! update originating from a *different* NAT peer must route back to the
//! subscriber.
//!
//! ## Why this is distinct from `test_contract_operations_via_docker_nat`
//!
//! The existing test updates the contract FROM THE GATEWAY. When the gateway is
//! both the update origin and the node holding the subscriber registration, the
//! subscriber can receive the new state via the gateway's *local* broadcast —
//! that path does not, on its own, exercise the cross-node
//! registration-by-observed-address mechanism. A green result there does not
//! prove that a remote-origin update can find a NAT subscriber.
//!
//! ## What a green result here proves (assertion -> mechanism)
//!
//! Topology: 1 gateway + 2 peers, BOTH behind NAT.
//!   - peer(0) is the subscriber.
//!   - peer(1) is the updater (a *different* NAT peer).
//!
//! peer(0) is behind NAT: its outbound `SubscribeMsg::Request` carries no
//! address of its own (a NAT peer cannot know/announce its public mapping). The
//! gateway is therefore the only node that knows how to reach peer(0) — it must
//! have:
//!   (a) filled the *observed* transport `source_addr` from the inbound packet
//!       and registered peer(0) as a downstream subscriber by that address
//!       (`register_downstream_subscriber`), and
//!   (b) relayed peer(1)'s remote-origin update back down to peer(0) through
//!       that registration.
//!
//! peer(1)'s update never transits the subscriber's own client. The ONLY route
//! by which peer(0) can observe peer(1)'s key is the gateway-held downstream
//! registration keyed on peer(0)'s NAT-observed address. So the final
//! `assert!` that peer(0)'s state contains the key peer(1) set is contingent on
//! the NAT address-filling path working end to end. If address-filling
//! regressed, peer(0) would never be registered reachable and the update would
//! not arrive — the assertion would fail.
//!
//! ## Contract hosting note
//!
//! The contract is deployed (PUT) from the gateway, mirroring the existing
//! Docker NAT test. The freenet-ping helpers do not expose deterministic
//! control over which node ends up *hosting* the contract, so we do not assert
//! on hosting location. The two-NAT-peer topology is what makes the
//! gateway-relayed downstream-registration path the delivery route: peer(0)
//! subscribes through the gateway from behind NAT, and the update originates on
//! a *separate* NAT peer, so a purely-local broadcast on the subscriber cannot
//! account for delivery.
//!
//! Requires: FREENET_TEST_DOCKER_NAT=1, FREENET_BINARY_PATH, Docker daemon.

mod common;

use anyhow::Result;
use common::{
    APP_TAG, PACKAGE_DIR, PATH_TO_CONTRACT, connect_ws_with_retry, get_contract_state,
    load_contract, subscribe_to_contract, update_contract_state,
};
use freenet_ping_app::ping_client::wait_for_put_response;
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest},
    prelude::*,
};
use freenet_test_network::{FreenetBinary, NetworkBuilder, TestNetwork};
use std::{path::PathBuf, time::Duration};

/// Cross-peer NAT subscription delivery: a NAT peer subscribes through the
/// gateway, a *different* NAT peer updates, and the subscriber receives it.
///
/// See the module doc-comment for the full assertion->mechanism trace and why
/// this is distinct from `test_contract_operations_via_docker_nat`.
#[ignore] // Only runs when FREENET_TEST_DOCKER_NAT=1
#[tokio::test(flavor = "multi_thread")] // Docker backend uses block_in_place()
async fn test_nat_subscription_cross_peer_update_via_docker_nat() -> Result<()> {
    // Require Docker NAT env var — fail loudly if someone removes #[ignore]
    // and runs this without Docker.
    if std::env::var("FREENET_TEST_DOCKER_NAT").is_err() {
        panic!(
            "FREENET_TEST_DOCKER_NAT must be set. \
             This test requires Docker for NAT simulation."
        );
    }

    let binary_path = std::env::var("FREENET_BINARY_PATH")
        .expect("FREENET_BINARY_PATH must point to the freenet binary");

    // Build a 1-gateway + 2-peer Docker NAT network. Both peers are behind NAT.
    // Backend::default() reads FREENET_TEST_DOCKER_NAT and creates DockerNat.
    let network: TestNetwork = NetworkBuilder::new()
        .gateways(1)
        .peers(2)
        .binary(FreenetBinary::Path(binary_path.into()))
        .connectivity_timeout(Duration::from_secs(120))
        .build()
        .await
        .expect("Failed to build Docker NAT network");

    // --- Connect WebSocket clients to gateway, subscriber peer, updater peer ---
    let gw_url = format!("{}?encodingProtocol=native", network.gateway(0).ws_url());
    let subscriber_url = format!("{}?encodingProtocol=native", network.peer(0).ws_url());
    let updater_url = format!("{}?encodingProtocol=native", network.peer(1).ws_url());

    let mut gw_client = connect_ws_with_retry(&gw_url, "gateway", 30).await?;
    let mut subscriber_client =
        connect_ws_with_retry(&subscriber_url, "peer0-subscriber", 30).await?;
    let mut updater_client = connect_ws_with_retry(&updater_url, "peer1-updater", 30).await?;

    // --- Load and deploy contract from gateway (PUT) ---
    let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);

    // Compile contract WASM from source, get code hash for proper options.
    let temp_options = PingContractOptions {
        frequency: Duration::from_secs(5),
        ttl: Duration::from_secs(30),
        tag: APP_TAG.to_string(),
        code_key: String::new(),
    };
    let temp_params = Parameters::from(serde_json::to_vec(&temp_options)?);
    let container = load_contract(&path_to_code, temp_params)?;
    let code_hash = CodeHash::from_code(container.data());

    let options = PingContractOptions {
        frequency: Duration::from_secs(5),
        ttl: Duration::from_secs(30),
        tag: APP_TAG.to_string(),
        code_key: code_hash.to_string(),
    };
    let params = Parameters::from(serde_json::to_vec(&options)?);
    let container = load_contract(&path_to_code, params)?;
    let contract_key = container.key();

    let initial_state = WrappedState::new(serde_json::to_vec(&Ping::default())?);
    gw_client
        .send(ClientRequest::ContractOp(ContractRequest::Put {
            contract: container,
            state: initial_state,
            related_contracts: RelatedContracts::new(),
            subscribe: true,
            blocking_subscribe: false,
        }))
        .await?;
    wait_for_put_response(&mut gw_client, &contract_key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to deploy contract: {}", e))?;
    tracing::info!("Contract deployed from gateway: {contract_key}");

    // --- Fetch contract on the subscriber peer before subscribing ---
    // PutResponse is sent after local upsert on the gateway; the contract may
    // not have propagated to the peer yet. A GET with return_contract_code=true
    // ensures the peer caches the WASM before subscribing.
    get_contract_state(&mut subscriber_client, contract_key, true).await?;
    tracing::info!("Subscriber peer (peer0) fetched contract");

    // --- peer(0) SUBSCRIBE through the gateway (from behind NAT) ---
    // peer(0)'s SubscribeMsg::Request carries no address of its own; the gateway
    // must register it by the NAT-observed transport source_addr.
    subscribe_to_contract(&mut subscriber_client, contract_key).await?;
    tracing::info!("Subscriber peer (peer0) subscribed to contract");

    // Allow subscription propagation across the network.
    tokio::time::sleep(Duration::from_secs(15)).await;

    // --- Fetch contract on the updater peer so it can host/update it ---
    get_contract_state(&mut updater_client, contract_key, true).await?;
    tracing::info!("Updater peer (peer1) fetched contract");

    // --- peer(1) (a DIFFERENT NAT peer) UPDATE the contract ---
    // This update's origin is NOT the gateway and NOT the subscriber. For
    // peer(0) to observe it, the gateway must relay the broadcast down to
    // peer(0) via the downstream registration keyed on peer(0)'s observed addr.
    let mut update = Ping::default();
    update.insert("cross-peer-nat-node".to_string());
    update_contract_state(&mut updater_client, contract_key, update).await?;
    tracing::info!("State update sent from updater peer (peer1)");

    // Allow update propagation.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // --- Verify the subscriber peer received the cross-peer update ---
    let subscriber_state = get_contract_state(&mut subscriber_client, contract_key, false).await?;
    assert!(
        subscriber_state.contains_key("cross-peer-nat-node"),
        "Subscriber peer (peer0, behind NAT) should have received the \
         'cross-peer-nat-node' key set by a DIFFERENT NAT peer (peer1) via \
         gateway-relayed downstream subscription. Delivery requires the gateway \
         to have registered peer0 by its NAT-observed source_addr. \
         Got keys: {:?}",
        subscriber_state.keys().collect::<Vec<_>>()
    );

    tracing::info!("Docker NAT cross-peer subscription test passed");
    Ok(())
}
