//! Thin wrapper over the freenet-stdlib websocket client: single-attempt
//! PUT/GET with one overall deadline each, plus ring-join polling.

use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail, ensure};
use freenet_stdlib::client_api::{
    ClientError, ClientRequest, ContractError, ContractRequest, ContractResponse, ErrorKind,
    HostResponse, NodeQuery, QueryResponse, RequestError, WebApi,
};
use freenet_stdlib::prelude::*;
use tokio_tungstenite::connect_async;

/// URL of a node's client command endpoint.
pub fn command_url(base_ws: &str) -> String {
    format!(
        "{}/v1/contract/command?encodingProtocol=native",
        base_ws.trim_end_matches('/')
    )
}

/// A poll interval with ±20% jitter. One check polling its own node has no
/// herd to stampede, but several vantage points polling the same gateway
/// would, and a nightly is exactly the kind of thing that ends up running
/// from more than one place.
fn jittered(base: Duration) -> Duration {
    base.mul_f64(0.8 + rand::random::<f64>() * 0.4)
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
                tokio::time::sleep(jittered(Duration::from_secs(1))).await;
            }
            Err(e) => bail!("could not connect to websocket API at {url}: {e}"),
        }
    }
}

/// Close the client session. Best effort: a node that already went away needs
/// no goodbye.
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

/// What an incoming message means for the request currently outstanding.
enum Reaction<T> {
    /// The response this request was waiting for.
    Done(T),
    /// Not this request's: log the reason and keep waiting.
    Ignore(String),
    /// This request's, and it ends it.
    Fatal(anyhow::Error),
}

/// Which contract an error is about, when the API says.
///
/// `None` means the error names no contract, so it cannot be attributed to any
/// one request and the caller has to treat it as its own (see the `Err` arms of
/// [`classify_put`] / [`classify_get`]).
fn error_contract_id(err: &ClientError) -> Option<ContractInstanceId> {
    match err.kind() {
        ErrorKind::RequestError(RequestError::ContractError(contract_err)) => {
            Some(match contract_err {
                ContractError::Get { key, .. }
                | ContractError::Put { key, .. }
                | ContractError::Update { key, .. }
                | ContractError::Subscribe { key, .. } => *key.id(),
                ContractError::ContractStackOverflow { key }
                | ContractError::MissingRelated { key }
                | ContractError::MissingContract { key } => *key,
                _ => return None,
            })
        }
        ErrorKind::IncorrectState(key) => Some(*key.id()),
        _ => None,
    }
}

/// Decide what an incoming message means for the PUT of `expected_key`.
fn classify_put(
    expected_key: &ContractKey,
    label: &str,
    incoming: Result<HostResponse, ClientError>,
) -> Reaction<()> {
    match incoming {
        Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key })) => {
            if key != *expected_key {
                // Late reply to an earlier PUT that already timed out.
                // Failing this one on it would cascade bogus failures
                // through every remaining PUT.
                Reaction::Ignore(format!(
                    "PUT {label}: response for different key {key}, ignoring"
                ))
            } else {
                Reaction::Done(())
            }
        }
        Ok(other) => Reaction::Ignore(format!(
            "PUT {label}: skipping unexpected response: {other:?}"
        )),
        Err(e) => match error_contract_id(&e) {
            Some(other) if other != *expected_key.id() => Reaction::Ignore(format!(
                "PUT {label}: error for different key {other}, ignoring: {e}"
            )),
            _ => Reaction::Fatal(anyhow!("PUT {label}: websocket error: {e}")),
        },
    }
}

/// Decide what an incoming message means for the GET of `id`.
///
/// Errors are filtered by contract key exactly as successful responses are: a
/// server-side error for a *previous* op that already hit its own client-side
/// deadline arrives during the *next* op's wait window, and attributing it to
/// that op stamps the wrong contract's key into the reported error text.
fn classify_get(
    id: ContractInstanceId,
    label: &str,
    incoming: Result<HostResponse, ClientError>,
) -> Reaction<(ContractContainer, WrappedState)> {
    match incoming {
        Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            key,
            contract,
            state,
        })) => {
            if *key.id() != id {
                return Reaction::Ignore(format!(
                    "GET {label}: response for different key {key}, ignoring"
                ));
            }
            match contract {
                Some(contract) => Reaction::Done((contract, state)),
                None => Reaction::Fatal(anyhow!("GET {label} ({id}) returned no contract")),
            }
        }
        Ok(other) => Reaction::Ignore(format!(
            "GET {label}: skipping unexpected response: {other:?}"
        )),
        Err(e) => match error_contract_id(&e) {
            Some(other) if other != id => Reaction::Ignore(format!(
                "GET {label}: error for different key {other}, ignoring: {e}"
            )),
            _ => Reaction::Fatal(anyhow!("GET {label} ({id}): websocket error: {e}")),
        },
    }
}

/// Issue a single PUT and wait for the matching response.
///
/// Returns the outcome together with how long the operation actually took.
/// The duration is measured on **both** arms: a failure's latency has to be a
/// measurement, not the configured timeout echoed back (see the doc on
/// [`get`]).
pub async fn put(
    client: &mut WebApi,
    contract: ContractContainer,
    state: WrappedState,
    label: &str,
    timeout: Duration,
) -> (Result<()>, Duration) {
    drain_stray_responses(client).await;
    let started = Instant::now();
    let outcome = put_inner(client, contract, state, label, timeout, started).await;
    (outcome, started.elapsed())
}

async fn put_inner(
    client: &mut WebApi,
    contract: ContractContainer,
    state: WrappedState,
    label: &str,
    timeout: Duration,
    started: Instant,
) -> Result<()> {
    let expected_key = contract.key();
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
            Ok(incoming) => match classify_put(&expected_key, label, incoming) {
                Reaction::Done(()) => return Ok(()),
                Reaction::Ignore(why) => eprintln!("{why}"),
                Reaction::Fatal(e) => return Err(e),
            },
            Err(_) => bail!("PUT {label} timed out after {timeout:?}"),
        }
    }
}

/// Issue a single GET and wait for the matching response within one overall
/// deadline. Responses and errors for other keys are skipped.
///
/// Returns the outcome together with how long the operation actually took.
/// The duration is measured on **both** arms: reporting the configured timeout
/// for every failure makes a fast terminal failure indistinguishable from the
/// client giving up, which is the distinction the latency field exists to draw.
pub async fn get(
    client: &mut WebApi,
    id: ContractInstanceId,
    label: &str,
    timeout: Duration,
) -> (Result<(ContractContainer, WrappedState)>, Duration) {
    drain_stray_responses(client).await;
    let started = Instant::now();
    let outcome = get_inner(client, id, label, timeout, started).await;
    (outcome, started.elapsed())
}

async fn get_inner(
    client: &mut WebApi,
    id: ContractInstanceId,
    label: &str,
    timeout: Duration,
    started: Instant,
) -> Result<(ContractContainer, WrappedState)> {
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
            Ok(incoming) => match classify_get(id, label, incoming) {
                Reaction::Done(found) => return Ok(found),
                Reaction::Ignore(why) => eprintln!("{why}"),
                Reaction::Fatal(e) => return Err(e),
            },
            Err(_) => bail!("GET {label} ({id}) timed out after {timeout:?}"),
        }
    }
}

/// Wait until the node joins the ring; returns the addresses it connected to.
/// The report carries them: they are the only evidence of whether a GET could
/// have been answered by the node that stored the PUTs.
pub async fn wait_for_ring_join(client: &mut WebApi, timeout: Duration) -> Result<Vec<String>> {
    let deadline = Instant::now() + timeout;
    loop {
        client
            .send(ClientRequest::NodeQueries(NodeQuery::ConnectedPeers))
            .await?;
        match tokio::time::timeout(Duration::from_secs(5), client.recv()).await {
            Ok(Ok(HostResponse::QueryResponse(QueryResponse::ConnectedPeers { peers }))) => {
                if !peers.is_empty() {
                    let addrs: Vec<String> =
                        peers.iter().map(|(_, addr)| addr.to_string()).collect();
                    eprintln!(
                        "ephemeral node joined the ring ({} connection(s): {})",
                        addrs.len(),
                        addrs.join(", ")
                    );
                    return Ok(addrs);
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
        tokio::time::sleep(jittered(Duration::from_secs(2))).await;
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::sync::Arc;

    use freenet_stdlib::prelude::bincode;
    use futures::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::Message;

    use super::*;

    /// A message as it appears on the wire between node and client.
    type HostResult = Result<HostResponse, ClientError>;

    fn id_of(byte: u8) -> ContractInstanceId {
        ContractInstanceId::new([byte; 32])
    }

    /// The error shape the live network actually produced in the runs that
    /// motivated the key filter: a terminal GET failure that names its
    /// contract.
    fn get_error_for(id: ContractInstanceId) -> ClientError {
        ErrorKind::RequestError(RequestError::ContractError(ContractError::Get {
            key: ContractKey::from_id_and_code(id, CodeHash::new([0; 32])),
            cause: "stream assembly: no fragments received within inactivity timeout".into(),
        }))
        .into()
    }

    /// A contract a fake node can hand back, plus its state.
    fn test_contract() -> (ContractContainer, WrappedState) {
        let code = ContractCode::from(vec![1u8, 2, 3, 4]);
        let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(code),
            Parameters::from(vec![]),
        )));
        (contract, WrappedState::new(vec![7u8; 32]))
    }

    /// Accept one websocket connection, wait for the client's request, then
    /// send `responses` in order. Stays open afterwards so the client sees the
    /// responses rather than a connection close.
    async fn fake_node(listener: TcpListener, responses: Vec<HostResult>) {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut ws = tokio_tungstenite::accept_async(stream)
            .await
            .expect("ws handshake");
        ws.next().await.expect("client request").expect("ws frame");
        for response in responses {
            let bytes = bincode::serialize(&response).expect("serialize response");
            ws.send(Message::Binary(bytes.into())).await.expect("send");
        }
        // Hold the socket open; the test finishes long before this elapses.
        tokio::time::sleep(Duration::from_secs(60)).await;
    }

    async fn start_fake_node(responses: Vec<HostResult>) -> WebApi {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0u16))
            .await
            .expect("bind");
        let port = listener.local_addr().expect("local addr").port();
        tokio::spawn(fake_node(listener, responses));
        connect(
            &format!("ws://{}:{port}", Ipv4Addr::LOCALHOST),
            TEST_TIMEOUT,
        )
        .await
        .expect("connect to fake node")
    }

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    // ---- defect 3: errors must be key-filtered like successes ----

    #[test]
    fn an_error_naming_another_contract_is_ignored_not_attributed() {
        let waiting_on = id_of(1);
        let stale = id_of(2);
        match classify_get(waiting_on, "7d run/small-2", Err(get_error_for(stale))) {
            Reaction::Ignore(why) => assert!(
                why.contains(&stale.to_string()),
                "the log line should name the contract the error was really about, got: {why}"
            ),
            Reaction::Fatal(e) => panic!(
                "a late error for {stale} was attributed to the op waiting on {waiting_on}: {e:#}"
            ),
            Reaction::Done(_) => panic!("an error cannot complete a GET"),
        }
    }

    #[test]
    fn an_error_naming_the_waiting_contract_still_fails_it() {
        let waiting_on = id_of(1);
        match classify_get(waiting_on, "0h small-0", Err(get_error_for(waiting_on))) {
            Reaction::Fatal(e) => assert!(
                format!("{e:#}").contains("stream assembly"),
                "the real cause must survive into the report, got: {e:#}"
            ),
            _ => panic!("an error for this op's own contract must fail it"),
        }
    }

    #[test]
    fn an_error_naming_no_contract_still_fails_the_waiting_op() {
        // Unattributable errors (a dropped channel, a disconnect) keep the old
        // conservative behaviour: they belong to whoever is waiting. Silently
        // ignoring them would hang the op until its deadline.
        match classify_get(id_of(1), "0h small-0", Err(ErrorKind::Disconnect.into())) {
            Reaction::Fatal(_) => {}
            _ => panic!("an error with no contract key must still fail the waiting op"),
        }
    }

    #[test]
    fn put_errors_are_key_filtered_the_same_way() {
        let mine = ContractKey::from_id_and_code(id_of(1), CodeHash::new([0; 32]));
        match classify_put(&mine, "small-0", Err(get_error_for(id_of(2)))) {
            Reaction::Ignore(_) => {}
            _ => panic!("a late error for another contract must not fail this PUT"),
        }
        match classify_put(&mine, "small-0", Err(get_error_for(id_of(1)))) {
            Reaction::Fatal(_) => {}
            _ => panic!("an error for this PUT's own contract must fail it"),
        }
    }

    #[tokio::test]
    async fn a_late_error_during_a_get_does_not_fail_that_get() {
        // The race from the nightly logs: a previous op's server-side error
        // lands inside the *next* op's wait window, ahead of that op's own
        // response.
        let (contract, state) = test_contract();
        let key = contract.key();
        let mut client = start_fake_node(vec![
            Err(get_error_for(id_of(9))),
            Ok(HostResponse::ContractResponse(
                ContractResponse::GetResponse {
                    key,
                    contract: Some(contract.clone()),
                    state: state.clone(),
                },
            )),
        ])
        .await;

        let (outcome, _elapsed) = get(&mut client, *key.id(), "0h small-0", TEST_TIMEOUT).await;
        let (got_contract, got_state) =
            outcome.expect("the stale error must not fail this GET, whose response did arrive");
        assert_eq!(got_contract, contract);
        assert_eq!(got_state, state);
    }

    // ---- defect 2: a failure's latency is measured, not the timeout ----

    #[tokio::test]
    async fn a_fast_failure_reports_the_time_it_took_not_the_op_timeout() {
        // A terminal error from the node, well inside the 120s deadline the
        // nightly run configures.
        let op_timeout = Duration::from_secs(120);
        let id = id_of(1);
        let mut client = start_fake_node(vec![Err(get_error_for(id))]).await;

        let (outcome, elapsed) = get(&mut client, id, "7d run/small-2", op_timeout).await;
        assert!(outcome.is_err(), "the node returned a terminal error");
        assert!(
            elapsed < Duration::from_secs(5),
            "a failure that resolved immediately reported {elapsed:?}; the whole point is that \
             this is a measurement and not the {op_timeout:?} deadline"
        );
    }
}
