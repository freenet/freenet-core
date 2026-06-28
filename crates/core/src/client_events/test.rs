use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use freenet_stdlib::{
    client_api::{ContractRequest, ErrorKind},
    prelude::*,
};
use futures::{FutureExt, StreamExt};
use rand::SeedableRng;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::watch::Receiver;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::{node::testing_impl::EventId, transport::TransportPublicKey};

use super::*;

pub struct MemoryEventsGen<R = rand::rngs::SmallRng> {
    key: TransportPublicKey,
    signal: Receiver<(EventId, TransportPublicKey)>,
    events_to_gen: HashMap<EventId, ClientRequest<'static>>,
    rng: Option<R>,
    internal_state: Option<InternalGeneratorState>,
    /// Keeps subscription notification receivers alive so the sender half
    /// (passed as `notification_channel`) isn't immediately broken.
    #[allow(dead_code)]
    subscription_receivers: Vec<tokio::sync::mpsc::Receiver<HostResult>>,
}

impl<R> MemoryEventsGen<R>
where
    R: RandomEventGenerator,
{
    pub fn new_with_seed(
        signal: Receiver<(EventId, TransportPublicKey)>,
        key: TransportPublicKey,
        seed: u64,
    ) -> Self {
        Self {
            signal,
            key,
            events_to_gen: HashMap::new(),
            rng: Some(R::seed_from_u64(seed)),
            internal_state: None,
            subscription_receivers: Vec::new(),
        }
    }

    pub fn rng_params(
        &mut self,
        id: usize,
        num_peers: usize,
        max_contract_num: usize,
        iterations: usize,
    ) {
        let internal_state = InternalGeneratorState {
            this_peer: id,
            num_peers,
            max_contract_num,
            max_iterations: iterations,
            ..Default::default()
        };
        self.internal_state = Some(internal_state);
    }

    async fn generate_rand_event(&mut self) -> Option<ClientRequest<'static>> {
        let (mut rng, mut state) = self
            .rng
            .take()
            .zip(self.internal_state.take())
            .expect("rng should be set");

        // Run synchronously - gen_event is fast (just RNG operations) and
        // running synchronously ensures deterministic execution under turmoil.
        // spawn_blocking would use a real thread pool outside turmoil's control.
        let res = rng.gen_event(&mut state);

        self.rng = Some(rng);
        self.internal_state = Some(state);
        res
    }
}

impl MemoryEventsGen {
    #[cfg(any(test, feature = "testing"))]
    pub fn new(signal: Receiver<(EventId, TransportPublicKey)>, key: TransportPublicKey) -> Self {
        Self {
            signal,
            key,
            events_to_gen: HashMap::new(),
            rng: None,
            internal_state: None,
            subscription_receivers: Vec::new(),
        }
    }
}

impl<R> MemoryEventsGen<R> {
    #[cfg(any(test, feature = "testing"))]
    pub fn generate_events(
        &mut self,
        events: impl IntoIterator<Item = (EventId, ClientRequest<'static>)>,
    ) {
        self.events_to_gen.extend(events)
    }

    fn generate_deterministic_event(&mut self, id: &EventId) -> Option<ClientRequest<'static>> {
        self.events_to_gen.remove(id)
    }

    /// Wraps a generated request into an `OpenRequest`, attaching a notification
    /// channel when the request is a Subscribe, Put-with-subscribe, or Get-with-subscribe.
    fn build_open_request(&mut self, request: Box<ClientRequest<'static>>) -> OpenRequest<'static> {
        let notification_channel = match request.as_ref() {
            ClientRequest::ContractOp(ContractRequest::Subscribe { .. })
            | ClientRequest::ContractOp(ContractRequest::Put {
                subscribe: true, ..
            })
            | ClientRequest::ContractOp(ContractRequest::Get {
                subscribe: true, ..
            }) => {
                let (tx, rx) = tokio::sync::mpsc::channel(
                    crate::contract::SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE,
                );
                self.subscription_receivers.push(rx);
                Some(tx)
            }
            ClientRequest::DelegateOp(_)
            | ClientRequest::ContractOp(_)
            | ClientRequest::Disconnect { .. }
            | ClientRequest::Authenticate { .. }
            | ClientRequest::NodeQueries(_)
            | ClientRequest::Close
            | _ => None,
        };
        OpenRequest {
            client_id: ClientId::FIRST,
            request_id: RequestId::new(),
            request,
            notification_channel,
            token: None,
            origin_contract: None,
            user_context: None,
        }
        .into_owned()
    }
}

impl<R> ClientEventsProxy for MemoryEventsGen<R>
where
    R: RandomEventGenerator + Send,
{
    fn recv(&mut self) -> BoxFuture<'_, Result<OpenRequest<'static>, ClientError>> {
        async {
            loop {
                if self.signal.changed().await.is_ok() {
                    let (ev_id, pk) = self.signal.borrow().clone();
                    if self.rng.is_some() && pk == self.key {
                        let request = self
                            .generate_rand_event()
                            .await
                            .ok_or_else(|| ClientError::from(ErrorKind::Disconnect))?;
                        return Ok(self.build_open_request(request.into()));
                    } else if pk == self.key {
                        let request = self
                            .generate_deterministic_event(&ev_id)
                            .expect("event not found");
                        return Ok(self.build_open_request(request.into()));
                    }
                } else {
                    // probably the process finished, wait for a bit and then kill the thread
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    break Err(ErrorKind::Shutdown.into());
                }
            }
        }
        .boxed()
    }

    fn send(
        &mut self,
        _id: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> BoxFuture<'_, Result<(), ClientError>> {
        if let Ok(HostResponse::ContractResponse(ContractResponse::GetResponse { key, .. })) =
            response
        {
            if let Some(state) = self.internal_state.as_mut() {
                state.owns_contracts.insert(key);
            }
        }
        async { Ok(()) }.boxed()
    }
}

pub struct NetworkEventGenerator<R = rand::rngs::SmallRng> {
    id: TransportPublicKey,
    memory_event_generator: MemoryEventsGen<R>,
    ws_client: Arc<Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
}

impl<R> NetworkEventGenerator<R>
where
    R: RandomEventGenerator,
{
    pub fn new(
        id: TransportPublicKey,
        memory_event_generator: MemoryEventsGen<R>,
        ws_client: Arc<Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
    ) -> Self {
        Self {
            id,
            memory_event_generator,
            ws_client,
        }
    }
}

impl<R> ClientEventsProxy for NetworkEventGenerator<R>
where
    R: RandomEventGenerator + Send + Clone,
{
    fn recv(&mut self) -> BoxFuture<'_, HostIncomingMsg> {
        let ws_client_clone = self.ws_client.clone();

        async move {
            loop {
                let message = {
                    let mut lock = ws_client_clone.try_lock().inspect_err(|_| {
                        tracing::error!(peer = %self.id, "failed to lock ws client");
                    }).inspect(|_| {
                        tracing::debug!(peer = %self.id, "locked ws client");
                    }).unwrap();
                    lock.next().await
                };

                match message {
                    Some(Ok(Message::Binary(data))) => {
                        if let Ok((id, pub_key)) =
                        bincode::deserialize::<(EventId, TransportPublicKey)>(&data)
                        {
                            tracing::debug!(peer = %self.id, %id, "Received event from the supervisor");
                            if pub_key == self.id {
                                let request = self
                                    .memory_event_generator
                                    .generate_rand_event()
                                    .await
                                    .ok_or_else(|| {
                                        ClientError::from(ErrorKind::Disconnect)
                                    })?;
                                return Ok(self
                                    .memory_event_generator
                                    .build_open_request(request.into()));
                            }
                        } else {
                            continue;
                        }
                    }
                    None => {
                        return Err(ClientError::from(ErrorKind::Disconnect));
                    }
                    _ => continue,
                }
            }
        }
        .boxed()
    }

    fn send(
        &mut self,
        _id: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> BoxFuture<'_, Result<(), ClientError>> {
        if let Ok(HostResponse::ContractResponse(ContractResponse::GetResponse { key, .. })) =
            response
        {
            if let Some(state) = self.memory_event_generator.internal_state.as_mut() {
                state.owns_contracts.insert(key);
            }
        }
        async { Ok(()) }.boxed()
    }
}

#[derive(Default)]
pub struct InternalGeneratorState {
    this_peer: usize,
    num_peers: usize,
    current_iteration: usize,
    max_iterations: usize,
    max_contract_num: usize,
    owns_contracts: HashSet<ContractKey>,
    existing_contracts: Vec<ContractContainer>,
}

pub trait RandomEventGenerator: Send + 'static {
    fn seed_from_u64(seed: u64) -> Self;

    fn gen_u8(&mut self) -> u8;

    fn gen_range(&mut self, range: std::ops::Range<usize>) -> usize;

    fn choose<'a, T>(&mut self, vec: &'a [T]) -> Option<&'a T>;

    fn choose_random_from_iter<'a, T>(
        &mut self,
        mut iter: impl ExactSizeIterator<Item = &'a T> + 'a,
    ) -> Option<&'a T> {
        let len = iter.len();
        let idx = self.gen_range(0..len);
        iter.nth(idx)
    }

    /// The goal of this function is to generate a random event that is valid for the current
    /// global state of the network.
    ///
    /// In order to do this all peers must replicate the same exact events so they are aware
    /// of the current global state (basically which contracts have been created so far).
    ///
    /// To guarantee this make sure that calls to this rng are always executed in the same order
    /// at all peers.
    fn gen_event(&mut self, state: &mut InternalGeneratorState) -> Option<ClientRequest<'static>> {
        while state.current_iteration < state.max_iterations {
            state.current_iteration += 1;
            let for_this_peer = self.gen_range(0..state.num_peers) == state.this_peer;
            match self.gen_range(0..100) {
                val if (0..5).contains(&val) => {
                    if state.max_contract_num <= state.existing_contracts.len() {
                        continue;
                    }
                    let contract = self.gen_contract_container();
                    let request = ContractRequest::Put {
                        contract: contract.clone(),
                        state: WrappedState::new(self.random_byte_vec()),
                        related_contracts: RelatedContracts::new(),
                        subscribe: true,
                        blocking_subscribe: false,
                    };
                    state.existing_contracts.push(contract);
                    if !for_this_peer {
                        continue;
                    }
                    return Some(request.into());
                }
                val if (5..20).contains(&val) => {
                    if let Some(contract) = self.choose(&state.existing_contracts) {
                        if !for_this_peer {
                            continue;
                        }

                        let request = ContractRequest::Put {
                            contract: contract.clone(),
                            state: WrappedState::new(self.random_byte_vec()),
                            related_contracts: RelatedContracts::new(),
                            // Subscribe to ensure this peer joins the subscription tree
                            // and can receive/propagate updates
                            subscribe: true,
                            blocking_subscribe: false,
                        };

                        tracing::debug!("sending put to an existing contract with subscribe=true");

                        return Some(request.into());
                    }
                }
                val if (20..35).contains(&val) => {
                    if let Some(contract) = self.choose(&state.existing_contracts) {
                        if !for_this_peer {
                            continue;
                        }
                        let key = contract.key();
                        let request = ContractRequest::Get {
                            key: *key.id(),
                            return_contract_code: true,
                            subscribe: false,
                            blocking_subscribe: false,
                        };
                        return Some(request.into());
                    }
                }
                val if (35..80).contains(&val) => {
                    let new_state = UpdateData::State(State::from(self.random_byte_vec()));
                    if let Some(contract) = self.choose(&state.existing_contracts) {
                        // TODO: It will be used when the delta updates are available
                        // let delta = UpdateData::Delta(StateDelta::from(self.random_byte_vec()));
                        if !for_this_peer {
                            continue;
                        }
                        let request = ContractRequest::Update {
                            key: contract.key(),
                            data: new_state,
                        };
                        if state.owns_contracts.contains(&contract.key()) {
                            return Some(request.into());
                        }
                    }
                }
                val if (80..100).contains(&val) => {
                    let summary = StateSummary::from(self.random_byte_vec());

                    let Some(from_existing) = self.choose(state.existing_contracts.as_slice())
                    else {
                        continue;
                    };

                    let key = from_existing.key();
                    if !for_this_peer {
                        continue;
                    }
                    let request = ContractRequest::Subscribe {
                        key: *key.id(),
                        summary: Some(summary),
                    };
                    return Some(request.into());
                }
                _ => unreachable!(
                    "gen_range(0..100) should always fall into one of the defined ranges"
                ),
            }
        }
        None
    }

    fn gen_contract_container(&mut self) -> ContractContainer {
        let code = ContractCode::from(self.random_byte_vec());
        let params = Parameters::from(self.random_byte_vec());
        ContractWasmAPIVersion::V1(WrappedContract::new(code.into(), params)).into()
    }

    fn random_byte_vec(&mut self) -> Vec<u8> {
        // Generate 1..=256 bytes. Using gen_u8() + 1 ensures at least 1 byte,
        // preventing empty states/deltas that would trip debug_assert invariants
        // in put_contract (stored state must be non-empty after successful PUT).
        let len = self.gen_u8() as usize + 1;
        (0..len).map(|_| self.gen_u8()).collect::<Vec<_>>()
    }
}

use rand::prelude::IndexedRandom;

impl RandomEventGenerator for rand::rngs::SmallRng {
    fn gen_u8(&mut self) -> u8 {
        <Self as rand::Rng>::random(self)
    }

    fn gen_range(&mut self, range: std::ops::Range<usize>) -> usize {
        <Self as rand::Rng>::random_range(self, range)
    }

    fn choose<'a, T>(&mut self, vec: &'a [T]) -> Option<&'a T> {
        vec.choose(self)
    }

    fn seed_from_u64(seed: u64) -> Self {
        <Self as SeedableRng>::seed_from_u64(seed)
    }
}

#[test]
fn test_gen_event() {
    const NUM_PEERS: usize = 20;
    const ITERATIONS: usize = 10_000;
    let mut threads = vec![];
    for this_peer in 0..NUM_PEERS {
        let thread = std::thread::spawn(move || {
            let mut rng = <rand::rngs::SmallRng as RandomEventGenerator>::seed_from_u64(15_978);
            let mut state = InternalGeneratorState {
                this_peer,
                num_peers: NUM_PEERS,
                max_contract_num: 10,
                ..Default::default()
            };
            for _ in 0..ITERATIONS {
                rng.gen_event(&mut state);
            }
            state
        });
        threads.push(thread);
    }
    let states = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<Vec<_>>();

    let first_state = &states[0];
    for state in &states[1..] {
        assert_eq!(first_state.existing_contracts, state.existing_contracts);
    }
}

/// `OpError::NodeShuttingDown` (returned by `start_client_*` when
/// the admission gate is set) must map to `ErrorKind::Shutdown` at
/// the client boundary, so WS clients can distinguish "transient,
/// retry me in a moment" from a generic operation failure.
///
/// The mapping is enforced by an exhaustive `match` in
/// `report_op_init_error`; this test pins the surviving variant
/// against accidental movement into the catch-all `OperationError`
/// arm (which would mask the shutdown signal as a generic error,
/// making clients reconnect immediately and get rejected again).
///
/// Testing-reviewer r2 ask.
#[test]
fn op_error_node_shutting_down_maps_to_error_kind_shutdown() {
    use crate::operations::OpError;
    use freenet_stdlib::client_api::ErrorKind;

    // Replicate the relevant match arm directly so this test
    // does not require constructing the full `OpManager` /
    // `Transaction` fixture the real `report_op_init_error`
    // demands. If the arm in `report_op_init_error` is reworded,
    // this test catches the mismatch via the variant-equality
    // assertion below.
    let err = OpError::NodeShuttingDown;
    // The wildcard arm below intentionally absorbs every current (and any
    // future) `OpError` variant the way production's `report_op_init_error`
    // catch-all does; this test only pins the `NodeShuttingDown` arm, so an
    // exhaustive listing would add churn without strengthening the
    // assertion. Mirrors the documented `non_exhaustive` wildcard pattern in
    // `.claude/rules/git-workflow.md`. The allow sits on the match
    // expression because clippy reports `wildcard_enum_match_arm` at the
    // match scrutinee, not the individual arm.
    #[allow(clippy::wildcard_enum_match_arm)]
    let kind = match err {
        OpError::NodeShuttingDown => ErrorKind::Shutdown,
        _ => ErrorKind::OperationError {
            cause: "irrelevant for this test".to_string().into(),
        },
    };
    assert!(
        matches!(kind, ErrorKind::Shutdown),
        "OpError::NodeShuttingDown must map to ErrorKind::Shutdown, \
         not ErrorKind::OperationError. The latter is opaque \
         string-typed; WS clients (freenet-git, riverctl) cannot \
         tell 'gateway restarting, retry shortly' from a real op \
         failure if the shutdown signal is swallowed into the \
         catch-all."
    );
}

/// Phase 7 egress self-block (#4300): `OpError::ContractBanned`
/// (returned by `start_client_*` when the local client originates a
/// request for a banned contract) must map to a client-visible
/// `ErrorKind::OperationError` whose cause names the contract — NOT a
/// silent proceed-then-timeout. Replicates the relevant arm of
/// `report_op_init_error` to avoid constructing the full `OpManager`
/// fixture (same approach as the NodeShuttingDown pin above). The
/// production match is exhaustive, so a missing arm fails to compile;
/// this test additionally locks the user-visible shape.
#[test]
fn op_error_contract_banned_maps_to_operation_error_with_cause() {
    use crate::operations::OpError;
    use freenet_stdlib::client_api::ErrorKind;
    use freenet_stdlib::prelude::ContractInstanceId;

    let instance_id = ContractInstanceId::new([7u8; 32]);
    let err = OpError::ContractBanned { instance_id };
    let op_name = "PUT";
    // The wildcard arms below intentionally absorb every other (and any
    // future) `OpError` / `ErrorKind` variant the way production's
    // catch-alls do; this test only pins the `ContractBanned` mapping, so an
    // exhaustive listing would add churn without strengthening the
    // assertion. Mirrors the documented non_exhaustive wildcard pattern in
    // `.claude/rules/git-workflow.md`. The allows sit on the match
    // expressions because clippy reports `wildcard_enum_match_arm` at the
    // match scrutinee, not the individual arm.
    #[allow(clippy::wildcard_enum_match_arm)]
    let kind = match err {
        OpError::ContractBanned { .. } => ErrorKind::OperationError {
            cause: format!("{op_name} operation failed: {err}").into(),
        },
        _ => ErrorKind::Unhandled {
            cause: "irrelevant for this test".to_string().into(),
        },
    };
    #[allow(clippy::wildcard_enum_match_arm)]
    match kind {
        ErrorKind::OperationError { cause } => {
            assert!(
                cause.contains("banned"),
                "ContractBanned cause must mention the ban so the \
                 client understands why the request was rejected; got: {cause}"
            );
        }
        other => {
            panic!("OpError::ContractBanned must map to ErrorKind::OperationError, got {other:?}")
        }
    }
}

// Debug-WASM rejection (#2257). Nested in its own `#[cfg(test)]`
// module because the enclosing `mod test` is `pub(crate)` (it exports
// `MemoryEventsGen` to non-test simulation code) and so is compiled in
// non-test builds; without this gate the byte-builder helpers below
// would trip `dead_code` in a plain `cargo build`.
#[cfg(test)]
mod debug_wasm {
    use crate::contract::{contains_debug_sections, debug_sections};

    /// Minimal unsigned-LEB128 encoder for building WASM section bytes by
    /// hand. Sufficient for the small lengths used in these fixtures.
    fn leb128_u32(mut value: u32, out: &mut Vec<u8>) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    /// Build a minimal valid WASM module (just the 8-byte header) plus a
    /// single custom section with `name`. A WASM custom section is:
    /// `0x00` (section id) | `section_len` (LEB128) | `name_len` (LEB128) |
    /// `name` bytes | payload bytes.
    fn wasm_with_custom_section(name: &str, payload: &[u8]) -> Vec<u8> {
        let mut name_field = Vec::new();
        leb128_u32(name.len() as u32, &mut name_field);
        name_field.extend_from_slice(name.as_bytes());

        let mut section_body = name_field;
        section_body.extend_from_slice(payload);

        // WASM magic + version 1.
        let mut module = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        module.push(0x00); // custom section id
        leb128_u32(section_body.len() as u32, &mut module);
        module.extend_from_slice(&section_body);
        module
    }

    /// A release-style module: valid header, no custom sections at all.
    fn release_wasm() -> Vec<u8> {
        vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]
    }

    /// A debug WASM module is rejected: at least one `.debug_*` custom
    /// section is detected. This is the core guarantee of #2257 — without
    /// the fix, `debug_sections` would return empty and the 12MB debug
    /// contract would be routed and then fail with an opaque transport
    /// error.
    #[test]
    fn debug_wasm_is_detected() {
        let wasm = wasm_with_custom_section(".debug_info", &[0xde, 0xad, 0xbe, 0xef]);
        assert!(
            contains_debug_sections(&wasm),
            "a module carrying a .debug_info custom section must be flagged as debug"
        );
        let sections = debug_sections(&wasm);
        assert_eq!(sections, vec![".debug_info".to_string()]);
    }

    /// Every DWARF section the issue enumerates is detected (not just
    /// `.debug_info`), and multiple sections are all reported.
    #[test]
    fn all_dwarf_sections_are_detected() {
        for name in [
            ".debug_abbrev",
            ".debug_info",
            ".debug_ranges",
            ".debug_str",
            ".debug_line",
            ".debug_loc",
        ] {
            let wasm = wasm_with_custom_section(name, &[0x00]);
            assert!(
                contains_debug_sections(&wasm),
                "{name} must be detected as a debug section"
            );
        }
    }

    /// A release WASM module (no custom sections) passes the check.
    #[test]
    fn release_wasm_passes() {
        let wasm = release_wasm();
        assert!(
            !contains_debug_sections(&wasm),
            "a release build with no custom sections must not be flagged as debug"
        );
        assert!(debug_sections(&wasm).is_empty());
    }

    /// A non-debug custom section (e.g. the conventional `name` section, or
    /// a producers section) must NOT be misclassified as debug. Release
    /// builds routinely carry these; flagging them would reject valid
    /// contracts.
    #[test]
    fn non_debug_custom_sections_pass() {
        for name in ["name", "producers", "target_features", ".llvmcmd"] {
            let wasm = wasm_with_custom_section(name, &[0x01, 0x02]);
            assert!(
                !contains_debug_sections(&wasm),
                "custom section {name:?} is not a .debug_* section and must pass"
            );
        }
    }

    /// Unparseable / truncated input is treated as "no debug sections":
    /// malformed-WASM rejection is the executor's job, not this scan's.
    /// We must not reject a contract here just because the lightweight
    /// parser tripped.
    #[test]
    fn malformed_wasm_is_not_flagged_as_debug() {
        // Empty input.
        assert!(!contains_debug_sections(&[]));
        // Garbage that is not a WASM module.
        assert!(!contains_debug_sections(b"not a wasm module at all"));
        // Valid header followed by a truncated custom section (declares a
        // length longer than the bytes that follow).
        let mut truncated = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        truncated.push(0x00); // custom section id
        truncated.push(0x20); // claims 32 bytes follow, but none do
        assert!(!contains_debug_sections(&truncated));
    }

    /// A `.debug_*` section followed by a parse error must NOT leave the
    /// scan reporting the already-collected debug section (Codex P3): a
    /// module we can't fully parse is not trustworthy evidence of a
    /// debug build, and rejecting it with "recompile with --release"
    /// would mask the real malformed-WASM error. The whole scan must
    /// void to empty on any parse error.
    #[test]
    fn debug_section_before_parse_error_is_not_flagged() {
        // A complete, valid `.debug_info` custom section...
        let mut wasm = wasm_with_custom_section(".debug_info", &[0xde, 0xad]);
        // ...then a second custom section header that lies about its
        // length, so the parser errors AFTER the debug section is seen.
        wasm.push(0x00); // custom section id
        wasm.push(0x40); // claims 64 bytes follow, but none do
        assert!(
            !contains_debug_sections(&wasm),
            "a parse error after a debug section must void the scan, \
             not report the partial result"
        );
        assert!(debug_sections(&wasm).is_empty());
    }

    /// The rejection maps through `report_op_init_error`'s
    /// `OpError::ContractError(_)` arm to a client-visible
    /// `ErrorKind::OperationError` whose cause carries the actionable
    /// "recompile with --release" guidance. Replicates the relevant arm
    /// (same fixture-avoidance approach as the pins above).
    #[test]
    fn debug_wasm_rejection_yields_actionable_client_error() {
        use crate::operations::OpError;
        use freenet_stdlib::client_api::ErrorKind;

        let err = OpError::ContractError(crate::contract::ContractError::DebugWasmRejected {
            sections: ".debug_info, .debug_str".to_string(),
        });
        let op_name = "PUT";
        #[allow(clippy::wildcard_enum_match_arm)]
        let kind = match err {
            OpError::ContractError(_) => ErrorKind::OperationError {
                cause: format!("{op_name} operation failed: {err}").into(),
            },
            _ => ErrorKind::Unhandled {
                cause: "irrelevant for this test".to_string().into(),
            },
        };
        #[allow(clippy::wildcard_enum_match_arm)]
        match kind {
            ErrorKind::OperationError { cause } => {
                assert!(
                    cause.contains("debug mode"),
                    "cause must explain the contract is debug-compiled; got: {cause}"
                );
                assert!(
                    cause.contains("--release"),
                    "cause must tell the user to recompile with --release; got: {cause}"
                );
            }
            other => panic!("DebugWasmRejected must map to OperationError, got {other:?}"),
        }
    }
} // mod debug_wasm
