//! Thin wrapper over the freenet-stdlib websocket client: single-attempt
//! PUT/GET with one overall deadline each, plus ring-join polling.

use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail, ensure};
use freenet_stdlib::client_api::{
    ClientRequest, ContractRequest, ContractResponse, HostResponse, NodeQuery, QueryResponse,
    WebApi,
};
use freenet_stdlib::prelude::*;
use tokio_tungstenite::connect_async;

pub fn command_url(base_ws: &str) -> String {
    format!(
        "{}/v1/contract/command?encodingProtocol=native",
        base_ws.trim_end_matches('/')
    )
}

/// Connect to a node's websocket API, retrying while it comes up.
pub async fn connect(base_ws: &str, timeout: Duration) -> Result<WebApi> {
    let url = command_url(base_ws);
    let deadline = Instant::now() + timeout;
    loop {
        match connect_async(&url).await {
            Ok((stream, _)) => return Ok(WebApi::start(stream)),
            Err(e) if Instant::now() < deadline => {
                eprintln!("ws at {base_ws} not ready yet ({e}), retrying...");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => bail!("could not connect to websocket API at {url}: {e}"),
        }
    }
}

pub async fn disconnect(client: &mut WebApi) {
    let _ = client.send(ClientRequest::Disconnect { cause: None }).await;
}

/// Discard queued responses so a stale message from a previous interaction
/// cannot be mis-attributed to the next request.
async fn drain_stray_responses(client: &mut WebApi) {
    loop {
        match tokio::time::timeout(Duration::from_millis(200), client.recv()).await {
            Ok(Ok(resp)) => eprintln!("discarding stray response: {resp:?}"),
            Ok(Err(err)) => eprintln!("discarding stray error: {err}"),
            Err(_) => break,
        }
    }
}

/// Issue a single PUT and wait for the matching response. Returns latency.
pub async fn put(
    client: &mut WebApi,
    contract: ContractContainer,
    state: WrappedState,
    label: &str,
    timeout: Duration,
) -> Result<Duration> {
    let expected_key = contract.key();
    drain_stray_responses(client).await;
    let started = Instant::now();
    client
        .send(ClientRequest::ContractOp(ContractRequest::Put {
            contract,
            state,
            related_contracts: RelatedContracts::default(),
            subscribe: false,
            blocking_subscribe: false,
        }))
        .await?;

    let deadline = started + timeout;
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| anyhow!("PUT {label} timed out after {timeout:?}"))?;
        match tokio::time::timeout(remaining, client.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                ensure!(
                    key == expected_key,
                    "PUT {label}: key mismatch (got {key}, want {expected_key})"
                );
                return Ok(started.elapsed());
            }
            Ok(Ok(other)) => eprintln!("PUT {label}: skipping unexpected response: {other:?}"),
            Ok(Err(e)) => bail!("PUT {label}: websocket error: {e}"),
            Err(_) => bail!("PUT {label} timed out after {timeout:?}"),
        }
    }
}

/// Issue a single GET and wait for the matching response within one overall
/// deadline. Responses for other keys are skipped.
pub async fn get(
    client: &mut WebApi,
    id: ContractInstanceId,
    label: &str,
    timeout: Duration,
) -> Result<(ContractContainer, WrappedState, Duration)> {
    drain_stray_responses(client).await;
    let started = Instant::now();
    client
        .send(ClientRequest::ContractOp(ContractRequest::Get {
            key: id,
            return_contract_code: true,
            subscribe: false,
            blocking_subscribe: false,
        }))
        .await?;

    let deadline = started + timeout;
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| anyhow!("GET {label} ({id}) timed out after {timeout:?}"))?;
        match tokio::time::timeout(remaining, client.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                contract,
                state,
            }))) => {
                if *key.id() != id {
                    eprintln!("GET {label}: response for different key {key}, ignoring");
                    continue;
                }
                let contract =
                    contract.ok_or_else(|| anyhow!("GET {label} ({id}) returned no contract"))?;
                return Ok((contract, state, started.elapsed()));
            }
            Ok(Ok(other)) => eprintln!("GET {label}: skipping unexpected response: {other:?}"),
            Ok(Err(e)) => bail!("GET {label} ({id}): websocket error: {e}"),
            Err(_) => bail!("GET {label} ({id}) timed out after {timeout:?}"),
        }
    }
}

/// Wait until the node reports at least one ring connection.
pub async fn wait_for_ring_join(client: &mut WebApi, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        client
            .send(ClientRequest::NodeQueries(NodeQuery::ConnectedPeers))
            .await?;
        match tokio::time::timeout(Duration::from_secs(5), client.recv()).await {
            Ok(Ok(HostResponse::QueryResponse(QueryResponse::ConnectedPeers { peers }))) => {
                if !peers.is_empty() {
                    eprintln!(
                        "ephemeral node joined the ring ({} connection(s))",
                        peers.len()
                    );
                    return Ok(());
                }
            }
            Ok(Ok(other)) => eprintln!("unexpected response to ConnectedPeers: {other:?}"),
            Ok(Err(e)) => eprintln!("ConnectedPeers query error: {e}"),
            Err(_) => eprintln!("ConnectedPeers query timed out"),
        }
        ensure!(
            Instant::now() < deadline,
            "ephemeral node did not join the ring within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
