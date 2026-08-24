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
    /// An error belonging to a *different* contract: log it, keep waiting, and
    /// count it. Separate from [`Reaction::Ignore`] so the report can carry how
    /// often the key filter fired, which is what makes the filter's own
    /// behaviour auditable from the artifact rather than only from stderr.
    IgnoredError(String),
    /// This request's, and it ends it.
    Fatal(anyhow::Error),
}

/// Base58 length bounds for a 32-byte contract id: 32 zero bytes encode to 32
/// `1`s, and the largest 32-byte value needs `ceil(256 / log2(58))` = 44
/// characters. A token outside this range cannot be one.
const CONTRACT_ID_B58_LEN: std::ops::RangeInclusive<usize> = 32..=44;

/// A character of the bitcoin base58 alphabet, which is what
/// [`ContractInstanceId`] encodes to.
fn is_base58(c: char) -> bool {
    c.is_ascii_alphanumeric() && !matches!(c, '0' | 'O' | 'I' | 'l')
}

/// Contract ids named in free text.
///
/// Terminal network failures reach a client as [`ErrorKind::OperationError`],
/// which has **no key field**: the id is written into the human-readable
/// `cause` by the node (`synthesized_get_error_cause` in freenet-core). Reading
/// it back out of the prose is the only way the key filter reaches the failures
/// netcheck actually receives.
///
/// Parsing prose is only acceptable if it cannot yield a *wrong* id, because a
/// wrong id that is not ours makes the filter discard a real failure. Two
/// bounds rule that out:
///
/// * the token must round-trip — decode then re-encode must reproduce it
///   exactly. `from_base58` zero-pads short input, so without this any
///   base58-spelled English word (`after`, `for`, `retries`) would decode to a
///   well-formed id that is definitely not ours;
/// * a token shorter or longer than a 32-byte id is rejected before decoding.
///
/// Whatever fails these is simply not found, which leaves the error
/// unattributed and therefore fatal for the waiting op. That is the
/// conservative direction: see [`error_about_another_contract`].
fn contract_ids_in_text(text: &str) -> Vec<ContractInstanceId> {
    text.split(|c: char| !is_base58(c))
        .filter(|token| CONTRACT_ID_B58_LEN.contains(&token.len()))
        .filter_map(|token| {
            let id = ContractInstanceId::from_base58(token).ok()?;
            (id.encode() == token).then_some(id)
        })
        .collect()
}

/// Every contract an error definitively names as its own subject.
///
/// Empty means the error names no contract that can be trusted to be its
/// subject, so it cannot be attributed to any one request.
fn error_contract_ids(err: &ClientError) -> Vec<ContractInstanceId> {
    match err.kind() {
        ErrorKind::RequestError(RequestError::ContractError(contract_err)) => match contract_err {
            ContractError::Get { key, .. }
            | ContractError::Put { key, .. }
            | ContractError::Update { key, .. }
            | ContractError::Subscribe { key, .. } => vec![*key.id()],
            ContractError::MissingContract { key } => vec![*key],
            // These name the RELATED contract — the dependency the failing
            // operation could not resolve — not the operation's own. Treating
            // that key as the error's subject would ignore a real failure of
            // whatever contract we are actually waiting on.
            ContractError::MissingRelated { .. } | ContractError::ContractStackOverflow { .. } => {
                Vec::new()
            }
            // `ContractError` is `#[non_exhaustive]`. A future variant that
            // carries a key lands here and stays unattributed, i.e. fatal for
            // the waiting op: the safe direction, but it does mean a new
            // key-carrying variant needs an arm above to be filtered on.
            _ => Vec::new(),
        },
        ErrorKind::IncorrectState(key) => vec![*key.id()],
        // The shape terminal network failures actually arrive in: no key
        // field, the id written into the prose. See `contract_ids_in_text`.
        //
        // `Unhandled` is the same shape and matters for the same reason: it is
        // what `ClientError::from(String)` produces, which is the sink stdlib's
        // own stream-assembly failure path uses -- and stream assembly is the
        // motivating failure class here, so leaving it unscanned would exempt
        // the very errors this filter exists to attribute.
        ErrorKind::OperationError { cause } | ErrorKind::Unhandled { cause } => {
            contract_ids_in_text(cause.as_ref())
        }
        // `ErrorKind` is `#[non_exhaustive]` too, and the same reasoning
        // applies: unrecognised means unattributed means fatal.
        _ => Vec::new(),
    }
}

/// Whether `err` definitively belongs to some contract other than `id`.
///
/// `Some(named)` — it names contracts and none of them is `id`, so it is a late
/// reply to an operation that already gave up; `named` is what to log.
///
/// `None` — it names `id`, names `id` among others, or names nothing
/// recognisable. All three stay fatal for the waiting op. Ignoring an error
/// nobody can attribute would hang that op to its deadline and then report the
/// deadline instead of the real cause, which is worse than blaming the wrong op
/// loudly.
fn error_about_another_contract(err: &ClientError, id: &ContractInstanceId) -> Option<String> {
    let named = error_contract_ids(err);
    if named.is_empty() || named.contains(id) {
        return None;
    }
    Some(
        named
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", "),
    )
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
        Err(e) => match error_about_another_contract(&e, expected_key.id()) {
            Some(other) => Reaction::IgnoredError(format!(
                "PUT {label}: error for different key {other}, ignoring: {e}"
            )),
            None => Reaction::Fatal(anyhow!("PUT {label}: websocket error: {e}")),
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
        // A dead-ended GET does NOT arrive as `Err`. The node answers
        // `Ok(NotFound)` on purpose, so a client can tell "contract genuinely
        // absent" from "operation failed" -- see the comment on the arm that
        // publishes it in `operations/get/op_ctx_task.rs`, which calls this
        // "the dominant production `not_found` mode".
        //
        // Falling through to `Ignore` here is what made defect 2 survive its
        // own fix: netcheck would discard the node's terminal answer, keep
        // waiting, and report `latency_ms: 120000` -- the configured deadline
        // again, for the single most common failure, now dressed as a
        // measurement instead of an acknowledged constant. `NotFound` carries
        // `instance_id`, so it is key-filtered exactly like every other
        // terminal reply.
        Ok(HostResponse::ContractResponse(ContractResponse::NotFound { instance_id })) => {
            if instance_id != id {
                Reaction::Ignore(format!(
                    "GET {label}: NotFound for different key {instance_id}, ignoring"
                ))
            } else {
                Reaction::Fatal(anyhow!(
                    "GET {label} ({id}): node answered NotFound (contract not located)"
                ))
            }
        }
        Ok(other) => Reaction::Ignore(format!(
            "GET {label}: skipping unexpected response: {other:?}"
        )),
        Err(e) => match error_about_another_contract(&e, &id) {
            Some(other) => Reaction::IgnoredError(format!(
                "GET {label}: error for different key {other}, ignoring: {e}"
            )),
            None => Reaction::Fatal(anyhow!("GET {label} ({id}): websocket error: {e}")),
        },
    }
}

/// One completed operation: what it produced, and what was observed while it
/// ran.
///
/// The measurements travel with the outcome rather than being reconstructed by
/// the caller, because both of them are things only this module sees: the
/// caller knows the configured timeout, not the elapsed time, and knows nothing
/// at all about errors that arrived for other contracts.
pub struct Attempt<T> {
    pub outcome: Result<T>,
    /// How long the operation actually took, measured on **both** arms. A
    /// failure's latency has to be a measurement, not the configured timeout
    /// echoed back (see the doc on [`get`]).
    pub latency: Duration,
    /// Incoming errors this operation attributed to another contract and
    /// skipped. Zero is the ordinary case; a non-zero count is how a run
    /// records that the key filter fired.
    pub errors_ignored: usize,
}

impl<T> Attempt<T> {
    /// Check a successful outcome, keeping the measurements. Used for the
    /// GET verification step, which can turn a delivered response into a
    /// failure without changing how long it took to arrive.
    pub fn and_then<U>(self, check: impl FnOnce(T) -> Result<U>) -> Attempt<U> {
        Attempt {
            outcome: self.outcome.and_then(check),
            latency: self.latency,
            errors_ignored: self.errors_ignored,
        }
    }
}

/// Issue a single PUT and wait for the matching response.
pub async fn put(
    client: &mut WebApi,
    contract: ContractContainer,
    state: WrappedState,
    label: &str,
    timeout: Duration,
) -> Attempt<()> {
    drain_stray_responses(client).await;
    let started = Instant::now();
    let mut errors_ignored = 0;
    let outcome = put_inner(
        client,
        contract,
        state,
        label,
        timeout,
        started,
        &mut errors_ignored,
    )
    .await;
    Attempt {
        outcome,
        latency: started.elapsed(),
        errors_ignored,
    }
}

async fn put_inner(
    client: &mut WebApi,
    contract: ContractContainer,
    state: WrappedState,
    label: &str,
    timeout: Duration,
    started: Instant,
    errors_ignored: &mut usize,
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
                Reaction::IgnoredError(why) => {
                    *errors_ignored += 1;
                    eprintln!("{why}");
                }
                Reaction::Fatal(e) => return Err(e),
            },
            Err(_) => bail!("PUT {label} timed out after {timeout:?}"),
        }
    }
}

/// Issue a single GET and wait for the matching response within one overall
/// deadline. Responses and errors for other keys are skipped.
///
/// The reported latency is measured on **both** arms: reporting the configured
/// timeout for every failure makes a fast terminal failure indistinguishable
/// from the client giving up, which is the distinction the latency field exists
/// to draw.
pub async fn get(
    client: &mut WebApi,
    id: ContractInstanceId,
    label: &str,
    timeout: Duration,
) -> Attempt<(ContractContainer, WrappedState)> {
    drain_stray_responses(client).await;
    let started = Instant::now();
    let mut errors_ignored = 0;
    let outcome = get_inner(client, id, label, timeout, started, &mut errors_ignored).await;
    Attempt {
        outcome,
        latency: started.elapsed(),
        errors_ignored,
    }
}

async fn get_inner(
    client: &mut WebApi,
    id: ContractInstanceId,
    label: &str,
    timeout: Duration,
    started: Instant,
    errors_ignored: &mut usize,
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
                Reaction::IgnoredError(why) => {
                    *errors_ignored += 1;
                    eprintln!("{why}");
                }
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

    fn reaction_name<T>(r: &Reaction<T>) -> &'static str {
        match r {
            Reaction::Done(_) => "Done",
            Reaction::Ignore(_) => "Ignore",
            Reaction::IgnoredError(_) => "IgnoredError",
            Reaction::Fatal(_) => "Fatal",
        }
    }

    /// A structured terminal GET failure: the API names the contract in a key
    /// field. Real, but not what a terminal *network* failure looks like — for
    /// that see [`network_error_for`].
    fn get_error_for(id: ContractInstanceId) -> ClientError {
        ErrorKind::RequestError(RequestError::ContractError(ContractError::Get {
            key: ContractKey::from_id_and_code(id, CodeHash::new([0; 32])),
            cause: "stream assembly: no fragments received within inactivity timeout".into(),
        }))
        .into()
    }

    /// The error shape the live network actually produces, and the one every
    /// failure in the nightly runs that motivated the key filter arrived as:
    /// `ErrorKind::OperationError`, which has no key field at all. The cause is
    /// the text `synthesized_get_error_cause` writes in freenet-core, verbatim
    /// apart from the id, which is substituted so a test can name a specific
    /// contract.
    ///
    /// Rendered whole, this is the failure from run 31148314225:
    ///
    /// ```text
    /// GET .../large-1MB (5fSGSjA8...): websocket error: client error: error while
    /// executing operation in the network: GET response stream assembly failed for
    /// 5fSGSjA8... after exhausting retries: stream assembly: no fragments received
    /// within inactivity timeout
    /// ```
    fn network_error_for(id: ContractInstanceId) -> ClientError {
        network_error_because(format!(
            "GET response stream assembly failed for {id} after exhausting retries: \
             stream assembly: no fragments received within inactivity timeout"
        ))
    }

    /// A terminal network failure carrying an arbitrary cause.
    fn network_error_because(cause: impl Into<std::borrow::Cow<'static, str>>) -> ClientError {
        ErrorKind::OperationError {
            cause: cause.into(),
        }
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
            Reaction::IgnoredError(why) => assert!(
                why.contains(&stale.to_string()),
                "the log line should name the contract the error was really about, got: {why}"
            ),
            Reaction::Fatal(e) => panic!(
                "a late error for {stale} was attributed to the op waiting on {waiting_on}: {e:#}"
            ),
            _ => panic!("an error cannot complete a GET"),
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
            Reaction::IgnoredError(_) => {}
            _ => panic!("a late error for another contract must not fail this PUT"),
        }
        match classify_put(&mine, "small-0", Err(get_error_for(id_of(1)))) {
            Reaction::Fatal(_) => {}
            _ => panic!("an error for this PUT's own contract must fail it"),
        }
    }

    // ---- defect 3, continued: the shape the network actually sends ----

    #[test]
    fn a_terminal_network_error_naming_another_contract_is_ignored() {
        // The one that matters. Every failure in the nightly runs this filter
        // was built from arrived as OperationError, whose contract id is prose
        // rather than a field — so a filter that reads only the structured
        // variants never fires in production, however green its tests are.
        let waiting_on = id_of(1);
        let stale = id_of(2);
        match classify_get(
            waiting_on,
            "7d run/large-1MB",
            Err(network_error_for(stale)),
        ) {
            Reaction::IgnoredError(why) => assert!(
                why.contains(&stale.to_string()),
                "the log line should name the contract the error was really about, got: {why}"
            ),
            Reaction::Fatal(e) => panic!(
                "the failure shape the live network actually sends was attributed to the op \
                 waiting on {waiting_on}, which is the misreport this filter exists to stop: {e:#}"
            ),
            _ => panic!("an error cannot complete a GET"),
        }
    }

    #[test]
    fn a_terminal_network_error_naming_the_waiting_contract_still_fails_it() {
        let waiting_on = id_of(1);
        match classify_get(
            waiting_on,
            "0h large-1MB",
            Err(network_error_for(waiting_on)),
        ) {
            Reaction::Fatal(e) => assert!(
                format!("{e:#}").contains("no fragments received"),
                "the real cause must survive into the report, got: {e:#}"
            ),
            _ => panic!("an error naming this op's own contract must fail it"),
        }
    }

    #[test]
    fn put_errors_from_the_network_are_key_filtered_the_same_way() {
        let mine = ContractKey::from_id_and_code(id_of(1), CodeHash::new([0; 32]));
        match classify_put(&mine, "small-0", Err(network_error_for(id_of(2)))) {
            Reaction::IgnoredError(_) => {}
            _ => panic!("a late network error for another contract must not fail this PUT"),
        }
        match classify_put(&mine, "small-0", Err(network_error_for(id_of(1)))) {
            Reaction::Fatal(_) => {}
            _ => panic!("a network error naming this PUT's own contract must fail it"),
        }
    }

    #[test]
    fn a_network_error_naming_several_contracts_including_ours_is_ours() {
        // Conservative on ambiguity: if we are named at all, the error is
        // ours. Ignoring it would hang this op to its deadline and then
        // report the deadline instead of what actually happened.
        let mine = id_of(1);
        let cause = format!(
            "PUT failed for {} while resolving related contract {mine}",
            id_of(2)
        );
        match classify_get(mine, "0h small-0", Err(network_error_because(cause))) {
            Reaction::Fatal(_) => {}
            _ => panic!("an error that names us among others must still fail us"),
        }
    }

    #[test]
    fn a_network_error_naming_nothing_parseable_stays_fatal() {
        // Fail safe. An error nobody can attribute belongs to whoever is
        // waiting; discarding it would trade a wrong attribution for a
        // silently hung op, which is worse.
        for cause in [
            "operation timed out before any peer answered",
            // Base58-spelled words. `from_base58` zero-pads short input, so a
            // parser without the round-trip check would decode these into
            // well-formed ids that are definitely not ours, and quietly
            // discard a real failure.
            "GET failed for after exhausting retries",
            // A truncated id: right alphabet, wrong length.
            "GET response stream assembly failed for 5fSGSjA8 after exhausting retries",
        ] {
            match classify_get(id_of(1), "0h small-0", Err(network_error_because(cause))) {
                Reaction::Fatal(_) => {}
                _ => panic!(
                    "an error naming no usable contract id must fail the waiting op: {cause}"
                ),
            }
        }
    }

    #[test]
    fn only_a_token_that_round_trips_is_read_as_a_contract_id() {
        let id = id_of(7);
        let encoded = id.to_string();
        assert_eq!(
            contract_ids_in_text(&format!("assembly failed for {encoded} after retries")),
            vec![id],
            "an id sitting in prose must be found without relying on its position"
        );
        assert_eq!(
            contract_ids_in_text(&format!("failed for {encoded}x after retries")),
            Vec::new(),
            "a 44-character token that does not round-trip is not an id (this case is caught \
             by the round-trip check, NOT by the length bound: it is inside 32..=44)"
        );
        assert_eq!(
            contract_ids_in_text("failed for 11111111111111111111111111111112 after"),
            vec![ContractInstanceId::new({
                let mut bytes = [0u8; 32];
                bytes[31] = 1;
                bytes
            })],
            "a canonical encoding with leading '1's is still a canonical encoding"
        );
        assert_eq!(
            contract_ids_in_text("1111111111111111111111111111111"),
            Vec::new(),
            "31 characters cannot encode a 32-byte id"
        );

        // The upper end of CONTRACT_ID_B58_LEN, which nothing else reaches:
        // every other id in this module is [n; 32] for a small n and encodes
        // to 43 characters, so narrowing the range to `32..=43` would leave
        // the whole suite green while silently dropping every id at or above
        // 58^43 (~6% of the id space) out of attribution. Those errors would
        // then be charged to whichever op happened to be waiting, which is
        // precisely the defect this filter exists to fix.
        let widest = id_of(255);
        let encoded = widest.to_string();
        assert_eq!(
            encoded.len(),
            44,
            "the all-ones id must exercise the upper bound"
        );
        assert_eq!(
            contract_ids_in_text(&format!("assembly failed for {encoded} after retries")),
            vec![widest],
            "the longest id a contract key can have must still be found"
        );
    }

    #[test]
    fn an_error_about_a_related_contract_is_not_about_that_contract() {
        // `MissingRelated` names the DEPENDENCY the operation could not
        // resolve, not the operation's own contract. Reading it as the
        // subject would ignore a real failure of whatever we are waiting on.
        let waiting_on = id_of(1);
        let dependency = id_of(2);
        for err in [
            ContractError::MissingRelated { key: dependency },
            ContractError::ContractStackOverflow { key: dependency },
        ] {
            let err: ClientError = ErrorKind::RequestError(RequestError::ContractError(err)).into();
            match classify_get(waiting_on, "0h small-0", Err(err)) {
                Reaction::Fatal(_) => {}
                _ => panic!("a related-contract key must not be read as the error's subject"),
            }
        }
    }

    #[test]
    fn a_notfound_for_this_contract_fails_the_get_immediately() {
        // The dominant production failure shape. Before this arm existed it
        // fell through to `Ok(other) => Ignore`, so netcheck discarded the
        // node's terminal answer and burned the full op timeout — reporting
        // `latency_ms: 120000` for the most common failure there is, which is
        // exactly the constant-latency defect this PR set out to remove.
        let id = id_of(3);
        match classify_get(
            id,
            "0h small-0",
            Ok(HostResponse::ContractResponse(ContractResponse::NotFound {
                instance_id: id,
            })),
        ) {
            Reaction::Fatal(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("NotFound"),
                    "the reported cause must say the node answered NotFound, got: {msg}"
                );
            }
            other => panic!(
                "a NotFound naming this contract is a terminal answer and must fail the GET                  fast, not be ignored into a synthetic timeout: {}",
                reaction_name(&other)
            ),
        }
    }

    #[test]
    fn a_notfound_for_another_contract_is_ignored() {
        // Key-filtered exactly like every other terminal reply: a NotFound
        // answering an op that already gave up must not kill this one.
        match classify_get(
            id_of(3),
            "0h small-0",
            Ok(HostResponse::ContractResponse(ContractResponse::NotFound {
                instance_id: id_of(9),
            })),
        ) {
            Reaction::Ignore(_) => {}
            other => panic!(
                "a NotFound naming a different contract must be ignored: {}",
                reaction_name(&other)
            ),
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

        let attempt = get(&mut client, *key.id(), "0h small-0", TEST_TIMEOUT).await;
        let (got_contract, got_state) = attempt
            .outcome
            .expect("the stale error must not fail this GET, whose response did arrive");
        assert_eq!(got_contract, contract);
        assert_eq!(got_state, state);
        assert_eq!(
            attempt.errors_ignored, 1,
            "the run must be able to see from the report that the filter fired"
        );
    }

    // ---- defect 2: a failure's latency is measured, not the timeout ----

    #[tokio::test]
    async fn a_fast_failure_reports_the_time_it_took_not_the_op_timeout() {
        // A terminal error from the node, well inside the 120s deadline the
        // nightly run configures.
        let op_timeout = Duration::from_secs(120);
        let id = id_of(1);
        let mut client = start_fake_node(vec![Err(get_error_for(id))]).await;

        let attempt = get(&mut client, id, "7d run/small-2", op_timeout).await;
        assert!(
            attempt.outcome.is_err(),
            "the node returned a terminal error"
        );
        assert!(
            attempt.latency < Duration::from_secs(5),
            "a failure that resolved immediately reported {:?}; the whole point is that this is a \
             measurement and not the {op_timeout:?} deadline",
            attempt.latency
        );
    }

    #[tokio::test]
    async fn a_fast_put_failure_also_reports_the_time_it_took() {
        // The PUT path had the identical defect and is fixed the same way, but
        // nothing pinned it: PUT failures land in the same report under the
        // same field, so an unmeasured one is just as misleading.
        let op_timeout = Duration::from_secs(120);
        let (contract, state) = test_contract();
        let key = contract.key();
        let mut client = start_fake_node(vec![Err(get_error_for(*key.id()))]).await;

        let attempt = put(&mut client, contract, state, "small-0", op_timeout).await;
        assert!(
            attempt.outcome.is_err(),
            "the node returned a terminal error"
        );
        assert!(
            attempt.latency < Duration::from_secs(5),
            "a PUT failure that resolved immediately reported {:?}; that is the configured \
             {op_timeout:?} deadline, not a measurement",
            attempt.latency
        );
    }
}
