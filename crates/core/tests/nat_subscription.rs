//! Integration test for NAT peer subscription (issue #2199).
//!
//! This test exercises the gateway-side address-filling code in the
//! SUBSCRIBE path that was previously only covered by unit / structural-pin
//! tests:
//!
//! - `crates/core/src/operations/subscribe.rs::register_downstream_subscriber`
//! - the SUBSCRIBE dispatch site in `crates/core/src/node.rs`, which derives
//!   the subscriber's `upstream_addr` from the inbound transport's observed
//!   `source_addr` (the `SubscribeMsg::Request` wire variant carries no peer
//!   address — see `subscribe.rs` "Uses hop-by-hop routing: each node stores
//!   `requester_addr` from the transport layer").
//!
//! ## Why this models the NAT case, and why it is NOT a duplicate
//!
//! A peer behind NAT does not know (and cannot put on the wire) its own
//! externally-visible address. The `SubscribeMsg::Request` it sends carries
//! only the `instance_id` / `htl` / `visited` bloom — no `PeerKeyLocation`,
//! no source address. Each relaying node observes only the *previous hop's*
//! real (NAT-translated) source address, via the transport layer's
//! `source_addr`. The dispatch site fills this in as `upstream_addr` and
//! `register_downstream_subscriber` registers the previous hop by that
//! observed address. This hop-by-hop, observed-address registration is the
//! "gateway fills in observed address from `source_addr`" flow from #2199.
//!
//! The existing suite already routes a remote SUBSCRIBE
//! (`operations.rs::test_multiple_clients_subscription` has a client on a
//! different node than the PUT-originator). What it does NOT do is *pin* the
//! topology: with random node locations, nothing guarantees the subscribing
//! node is not the contract's hosting node. If the subscriber happens to be
//! the host (or shares a node with the updater), the SUBSCRIBE can resolve as
//! a local hit and the self-update is delivered entirely in-process — the
//! gateway-observed-address registration never runs, yet the test still goes
//! green (a false positive; see the original #2199 test #1).
//!
//! This test removes that escape hatch by **pinning ring locations** (the
//! same technique as `operations.rs::test_put_contract_three_hop_*`):
//!
//! - `host-peer` is placed exactly at the contract's location → it is the
//!   provable host (distance 0 from the contract key).
//! - the updater operates on `host-peer`, so the updater and the subscriber
//!   are on different nodes.
//! - `nat-peer` (the subscriber) is placed half a ring away from the contract
//!   → it is provably NOT the host and is not co-located with the updater.
//!
//! With that topology, the ONLY way an UPDATE made on `host-peer` can reach
//! `nat-peer`'s client is for the hop-by-hop downstream-subscriber chain to
//! have been built from observed `source_addr`s while `nat-peer`'s SUBSCRIBE
//! relayed `nat-peer → gateway → host-peer`. `nat-peer` cannot be the host
//! and cannot self-deliver, so a successful notification is contingent on the
//! observed-address fill having worked at every hop: had it failed, the
//! relaying node would log "could not find peer to register interest", the
//! broadcast target list would omit the downstream hop, and the assertion
//! would time out. That is the strongest contingency available without an
//! introspection hook into the gateway's subscriber set (there is none — see
//! below).
//!
//! ## Why `nat-peer` GETs (with `subscribe=true`) instead of a bare SUBSCRIBE (#4524)
//!
//! NOTE (#4642): as of the demand-driven combined get+subscribe, a bare
//! `ContractRequest::Subscribe` on an uncached contract NO LONGER rejects — it
//! routes as a get+subscribe (the GET fetches state toward the key). This test
//! keeps the explicit `GET+subscribe=true` because it exercises the SAME relay
//! path and makes the WASM-acquisition precondition explicit; the reject
//! described below is historical (the #3757 guard, removed by #4642).
//!
//! A bare `ContractRequest::Subscribe` was rejected by the client-event handler
//! unless the contract WASM/state is already cached on the *local* node
//! (`client_events.rs`: "Rejecting SUBSCRIBE: contract WASM not cached
//! locally" — the guard added in #3757 so a node can't be "subscribed but
//! can't apply updates"). That guard explicitly exempts `GET+subscribe=true`
//! and `PUT+subscribe=true` because those operations inherently fetch/provide
//! the WASM ("PUT the contract or GET the contract before subscribing").
//!
//! `nat-peer` is the *provable non-host* (half a ring away) and never PUTs the
//! contract itself, so the only way it can hold the contract locally is to
//! fetch it. It cannot rely on PUT replication: the originator PUT on
//! `host-peer` finalizes at `host-peer` (already at the contract location) and
//! replicates exactly **one hop** toward the contract neighbourhood
//! (`relay_put_replicate_forward`, #4509), which lands on the *gateway* (closer
//! to the contract than `nat-peer`) — not on `nat-peer`. So a bare SUBSCRIBE on
//! `nat-peer` raced contract presence that, for the farthest peer, often never
//! arrives at all; that is the flake tracked in #4524 (a "contract not cached
//! locally" rejection ~7-9s in).
//!
//! The deterministic fix is to use the supported non-host path: `nat-peer`
//! issues `GET` with `subscribe=true`. The GET routes *toward* the contract
//! (so it reliably resolves on `gateway`/`host-peer`), fetches and caches the
//! body on `nat-peer`, and the explicit `subscribe=true` flag then registers
//! the subscription (`maybe_subscribe_child` -> `run_client_subscribe`) through
//! the SAME relay path a bare SUBSCRIBE
//! would use — the wire `SubscribeMsg::Request` still carries no address, so
//! each hop still fills `upstream_addr` from the observed transport
//! `source_addr` and registers the previous hop as a downstream subscriber.
//! The NAT observed-address registration the test exists to prove is therefore
//! still exercised end-to-end; only the unreliable "wait for PUT to replicate
//! to the farthest peer" precondition is removed. No timeout was bumped and no
//! assertion was weakened.
//!
//! ## Why we assert on the routed notification rather than the subscriber set
//!
//! There is no test-visible way to read a node's downstream-subscriber set.
//! The `NodeDiagnostics` response field `subscriber_peer_ids` is hardcoded
//! empty (lease-based subscriptions are tracked internally in
//! `HostingManager`, not surfaced), and `Ring::downstream_subscriber_count`
//! is not exported to integration tests. So the routed update notification is
//! the only observable proof, and we make it load-bearing by forcing the
//! topology so that proof can only be produced via the observed-address
//! registration path.
//!
//! ## Loopback / platform note
//!
//! Like every other `#[freenet_test]` multi-node test in this crate, each
//! node binds on a varied loopback IP (`127.x.y.1`, see
//! `freenet::test_utils::test_ip_for_node`). macOS cannot bind the full
//! `127.0.0.0/8` range and fails with "Can't assign requested address";
//! this test must be validated on Linux (`/linux-test` / Docker). It is NOT
//! gated off — it shares the platform requirement of the existing
//! `operations.rs` integration suite and runs unmodified in CI on Linux.

use anyhow::bail;
use freenet::dev_tool::Location;
use freenet::test_utils::{self, TestContext, make_get_with_blocking, make_put, make_update};
use freenet_macros::freenet_test;
use freenet_stdlib::{
    client_api::{ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use std::sync::LazyLock;
use std::time::Duration;
use tokio_tungstenite::connect_async;

const TEST_CONTRACT: &str = "test-contract-integration";

/// Contract container + its ring location, computed once. The location is
/// derived from the contract key (`Location::from(&ContractKey)`), so pinning
/// node locations *relative to* this value yields a deterministic host.
static CONTRACT: LazyLock<(ContractContainer, Location)> = LazyLock::new(|| {
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into()).expect("load contract");
    let location = Location::from(&contract.key());
    (contract, location)
});

fn contract_location() -> Location {
    CONTRACT.1
}

/// `host-peer` sits exactly at the contract location → it is the provable
/// host (distance 0 from the contract key).
fn host_peer_location() -> f64 {
    contract_location().as_f64()
}

/// Gateway sits a fifth of the ring away from the contract — close enough to
/// be on the routing path between `nat-peer` and `host-peer`, but not the
/// host.
fn gateway_location() -> f64 {
    Location::new_rounded(contract_location().as_f64() + 0.2).as_f64()
}

/// `nat-peer` (the subscriber) sits half a ring away from the contract — the
/// maximum possible ring distance, so it is provably NOT the host and cannot
/// resolve the SUBSCRIBE as a local hit.
fn nat_peer_location() -> f64 {
    Location::new_rounded(contract_location().as_f64() + 0.5).as_f64()
}

/// Drain `client` until a `PutResponse` for `contract_key` arrives or the
/// deadline elapses. Tolerates interleaved unrelated responses.
async fn await_put_response(
    client: &mut WebApi,
    contract_key: ContractKey,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            bail!("timeout waiting for PUT response");
        }
        match tokio::time::timeout(remaining, client.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key, "PUT response key mismatch");
                return Ok(());
            }
            Ok(Ok(other)) => {
                tracing::debug!("await_put_response: ignoring {:?}", other);
            }
            Ok(Err(e)) => bail!("error waiting for PUT response: {e}"),
            Err(_) => bail!("timeout waiting for PUT response"),
        }
    }
}

/// Drain `client` until a `GetResponse` for `contract_key` arrives, asserting
/// the contract code came back (`contract: Some(_)`, since the caller requests
/// `return_contract_code = true`). A GET with `subscribe = true` both fetches
/// the contract body to the local node and registers the subscription via the
/// GET driver's `maybe_subscribe_child`; because the caller also passes
/// `blocking_subscribe = true`, that subscribe is awaited inline before the
/// `GetResponse` is published, so this response is the non-host-peer's
/// deterministic "I now hold the contract AND the downstream chain is
/// registered" signal (issue #4524 — see module docs). Tolerates interleaved
/// unrelated responses.
async fn await_get_response(
    client: &mut WebApi,
    contract_key: ContractKey,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            bail!("timeout waiting for GET response");
        }
        match tokio::time::timeout(remaining, client.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                contract,
                ..
            }))) => {
                assert_eq!(key, contract_key, "GET response key mismatch");
                assert!(
                    contract.is_some(),
                    "NAT peer GET must return the contract code (return_contract_code=true) — \
                     without the WASM cached locally the follow-up subscription would be \
                     rejected 'not cached locally' (issue #4524)"
                );
                return Ok(());
            }
            Ok(Ok(other)) => {
                tracing::debug!("await_get_response: ignoring {:?}", other);
            }
            Ok(Err(e)) => bail!("error waiting for GET response: {e}"),
            Err(_) => bail!("timeout waiting for GET response"),
        }
    }
}

/// Drain `client` until an `UpdateNotification` for `contract_key` arrives,
/// returning the single-task title from the delivered state. Tolerates
/// interleaved `UpdateResponse` / other responses.
async fn await_update_notification(
    client: &mut WebApi,
    contract_key: ContractKey,
    timeout: Duration,
) -> anyhow::Result<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            bail!("timeout waiting for UpdateNotification");
        }
        match tokio::time::timeout(remaining, client.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                key,
                update,
            }))) => {
                assert_eq!(key, contract_key, "UpdateNotification key mismatch");
                match update {
                    UpdateData::State(state) => {
                        let todo: test_utils::TodoList = serde_json::from_slice(state.as_ref())
                            .expect("deserialize state from update notification");
                        let title = todo
                            .tasks
                            .first()
                            .map(|t| t.title.clone())
                            .unwrap_or_default();
                        return Ok(title);
                    }
                    // For this test we only PUT/UPDATE full State, so any
                    // other variant is unexpected; keep waiting in case a
                    // full-State notification is still in flight. Known
                    // variants are listed explicitly; the trailing wildcard
                    // exists ONLY to satisfy `UpdateData`'s `#[non_exhaustive]`
                    // (stdlib 0.6.0+) — mirrors operations.rs.
                    UpdateData::Delta(_)
                    | UpdateData::StateAndDelta { .. }
                    | UpdateData::RelatedState { .. }
                    | UpdateData::RelatedDelta { .. }
                    | UpdateData::RelatedStateAndDelta { .. }
                    | _ => {
                        tracing::warn!("await_update_notification: ignoring non-State update");
                    }
                }
            }
            Ok(Ok(other)) => {
                tracing::debug!("await_update_notification: ignoring {:?}", other);
            }
            Ok(Err(e)) => bail!("error waiting for UpdateNotification: {e}"),
            Err(_) => bail!("timeout waiting for UpdateNotification"),
        }
    }
}

/// Build a single-task todo-list state with the given title.
fn todo_state_with_title(title: &str) -> WrappedState {
    let todo = test_utils::TodoList {
        tasks: vec![test_utils::Task {
            id: 1,
            title: title.to_string(),
            description: "nat-subscription integration".to_string(),
            completed: false,
            priority: 1,
        }],
        version: 0,
    };
    WrappedState::from(serde_json::to_vec(&todo).expect("serialize todo state"))
}

/// Issue #2199: **A NAT peer that is provably NOT the contract host receives
/// updates, proving the relay chain registered it by its observed address.**
///
/// Topology is pinned (see module docs) so the contract hosts on `host-peer`
/// and the subscribing `nat-peer` sits half a ring away — it cannot be the
/// host and cannot self-deliver. The updater operates on `host-peer`, so the
/// updater and subscriber are on different nodes. Delivery of the
/// `host-peer` UPDATE to `nat-peer` therefore requires the hop-by-hop
/// downstream-subscriber chain (`nat-peer → gateway → host-peer`) to have
/// been built from observed `source_addr`s during the SUBSCRIBE relay — the
/// wire `Request` carries no address, so each hop must fill it from the
/// transport layer. A received notification is contingent on that fill: any
/// hop failing to resolve the observed address would drop the downstream
/// registration and the assertion would time out.
///
/// NOTE: the `source_addr == None` conflation flagged during #2199 review
/// (a dropped source address would map to the node's own address) is tracked
/// separately as issue #4389; it is a latent production concern, not
/// exercised here, and intentionally left untouched (this PR is test-only).
#[freenet_test(
    health_check_readiness = true,
    nodes = ["gateway", "host-peer", "nat-peer"],
    gateways = ["gateway"],
    node_configs = {
        "gateway": { location: gateway_location() },
        "host-peer": { location: host_peer_location() },
        "nat-peer": { location: nat_peer_location() },
    },
    timeout_secs = 600,
    startup_wait_secs = 40,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_nat_peer_remote_subscription_receives_update(ctx: &mut TestContext) -> TestResult {
    let (contract, contract_loc) = {
        let (contract, loc) = &*CONTRACT;
        (contract.clone(), *loc)
    };
    let contract_key = contract.key();

    let host_peer = ctx.node("host-peer")?;
    let nat_peer = ctx.node("nat-peer")?;
    let gateway = ctx.node("gateway")?;

    // Verify the topology was pinned as designed: host-peer at the contract
    // location, nat-peer half a ring away. This guards against a refactor of
    // the location helpers silently collapsing the host/subscriber separation
    // that the whole test relies on.
    assert_eq!(
        host_peer.location,
        host_peer_location(),
        "host-peer must be pinned at the contract location"
    );
    assert_eq!(
        nat_peer.location,
        nat_peer_location(),
        "nat-peer must be pinned half a ring from the contract"
    );
    let host_dist = Location::new_rounded(host_peer.location).distance(contract_loc);
    let nat_dist = Location::new_rounded(nat_peer.location).distance(contract_loc);
    assert!(
        nat_dist > host_dist,
        "nat-peer ({nat_dist:?}) must be farther from the contract than host-peer \
         ({host_dist:?}) — otherwise the subscriber could be the host and the test \
         would not exercise gateway-observed-address registration"
    );

    tracing::info!(
        "gateway: {:?} (loc {}), host-peer: {:?} (loc {}), nat-peer: {:?} (loc {}); contract loc {}",
        gateway.temp_dir_path,
        gateway.location,
        host_peer.temp_dir_path,
        host_peer.location,
        nat_peer.temp_dir_path,
        nat_peer.location,
        contract_loc.as_f64(),
    );

    // Updater client on host-peer (the hosting node, distinct from the
    // subscriber's node).
    let uri_host = host_peer.ws_url();
    let (stream_host, _) = connect_async(&uri_host).await?;
    let mut client_host = WebApi::start(stream_host);

    // Subscriber client on the NAT peer.
    let uri_nat = nat_peer.ws_url();
    let (stream_nat, _) = connect_async(&uri_nat).await?;
    let mut client_nat = WebApi::start(stream_nat);

    // PUT initial state from host-peer (subscribe=false so host-peer is not
    // itself a client subscriber; we want the broadcast to reach the NAT peer
    // via the downstream-subscriber chain). Retry under CI resource pressure,
    // mirroring operations.rs.
    const PUT_MAX_ATTEMPTS: usize = 3;
    let initial = todo_state_with_title("initial");
    let mut put_ok = false;
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 1..=PUT_MAX_ATTEMPTS {
        tracing::info!("host-peer PUT attempt {attempt}/{PUT_MAX_ATTEMPTS}");
        if let Err(e) = make_put(&mut client_host, initial.clone(), contract.clone(), false).await {
            last_err = Some(e);
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        match await_put_response(&mut client_host, contract_key, Duration::from_secs(120)).await {
            Ok(()) => {
                put_ok = true;
                break;
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    if !put_ok {
        bail!(
            "host-peer PUT failed after {PUT_MAX_ATTEMPTS} attempts: {:?}",
            last_err
        );
    }
    tracing::info!("host-peer PUT succeeded");

    // NAT peer fetches the contract (with BLOCKING auto-subscribe) instead of
    // issuing a bare SUBSCRIBE. A bare `Subscribe` would be rejected "contract
    // WASM not cached locally" because nat-peer is the provable non-host and
    // does not reliably receive the contract via PUT replication (the PUT
    // replicates one hop toward the contract neighbourhood — onto the closer
    // gateway, not nat-peer). `GET` routes *toward* the contract, so it reliably
    // resolves on gateway/host-peer, fetches and caches the body on nat-peer,
    // and the GET driver registers the subscription through the SAME relay path:
    // the wire `SubscribeMsg::Request` carries no address, so each hop
    // (`nat-peer → gateway → host-peer`) must fill the observed address from its
    // transport `source_addr` and register the previous hop as a downstream
    // subscriber. This is the deterministic, non-host subscribe path for issue
    // #4524 (see module docs) — not a sleep, not a bumped timeout.
    //
    // `blocking_subscribe = true` is load-bearing: the GET driver awaits the
    // subscribe child inline (`get::op_ctx_task::maybe_subscribe_child`) before
    // publishing the `GetResponse`, so by the time the response arrives the
    // downstream-subscriber chain is fully registered. With the non-blocking
    // GET the subscribe is spawned fire-and-forget and the `GetResponse` can
    // arrive *before* registration completes — then the UPDATE below could be
    // sent too early and the notification would be missed (the original race,
    // just relocated). Retry under CI resource pressure, mirroring the PUT.
    const GET_MAX_ATTEMPTS: usize = 3;
    let mut get_ok = false;
    let mut last_get_err: Option<anyhow::Error> = None;
    for attempt in 1..=GET_MAX_ATTEMPTS {
        tracing::info!("nat-peer GET+subscribe attempt {attempt}/{GET_MAX_ATTEMPTS}");
        if let Err(e) = make_get_with_blocking(
            &mut client_nat,
            contract_key,
            /* return_contract_code */ true,
            /* subscribe */ true,
            /* blocking_subscribe */ true,
        )
        .await
        {
            last_get_err = Some(e);
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        match await_get_response(&mut client_nat, contract_key, Duration::from_secs(120)).await {
            Ok(()) => {
                get_ok = true;
                break;
            }
            Err(e) => {
                last_get_err = Some(e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    if !get_ok {
        bail!(
            "nat-peer GET+subscribe failed after {GET_MAX_ATTEMPTS} attempts: {:?}",
            last_get_err
        );
    }
    tracing::info!(
        "nat-peer GET+blocking-subscribe succeeded (relay registered observed addresses)"
    );

    // UPDATE the contract from host-peer (the host, a different node than the
    // subscriber). The NAT peer cannot self-deliver — it is provably not the
    // host — so the notification can only arrive via the downstream chain.
    let updated = todo_state_with_title("update-from-host");
    make_update(&mut client_host, contract_key, updated).await?;
    tracing::info!("host-peer UPDATE sent; waiting for notification to route back to nat-peer");

    let title =
        await_update_notification(&mut client_nat, contract_key, Duration::from_secs(120)).await?;
    assert_eq!(
        title, "update-from-host",
        "NAT peer must receive the update made on host-peer — a missing/wrong title means the \
         broadcast did not route back through the downstream chain built from observed \
         addresses (NAT address-filling regression)"
    );
    tracing::info!("nat-peer received update notification — NAT subscription end-to-end OK");

    Ok(())
}
