# Freenet Core Crate

## Trigger-Action Rules

### BEFORE writing any new code in this crate

```
Does this code need current time?
  → DO NOT use: std::time::Instant::now(), tokio::time::sleep()
  → USE: TimeSource trait from src/simulation/

Does this code need randomness?
  → DO NOT use: rand::random(), rand::thread_rng()
  → USE: GlobalRng from src/config.rs

Is this network code for tests?
  → DO NOT use: tokio::net::UdpSocket
  → USE: SimulationSocket from src/transport/in_memory_socket.rs
```

### WHEN adding a test

```
Is it testing network behavior?
  → Use #[freenet_test] macro for SimNetwork

Is it a unit test?
  → Use mocks: MockNetworkBridge, MockRing

Need deterministic results?
  → Use GlobalRng seeding (see src/config.rs)
```

### WHEN modifying transport/

```
Read first: docs/architecture/transport/README.md
Key decision points:
  - Connection state: peer_connection.rs
  - Rate limiting: rate_limiter.rs
  - Congestion control: congestion/ (LEDBAT++, BBR, FixedRate)
```

### WHEN modifying operations/

```
Each file is a state machine:
  connect.rs    → CONNECT operation
  get.rs        → GET operation
  put.rs        → PUT operation
  update.rs     → UPDATE operation
  subscribe.rs  → SUBSCRIBE operation (legacy state-machine path)

Plus task-per-transaction drivers from #1454 Phase 2b onwards:
  subscribe/op_ctx_task.rs → client-initiated SUBSCRIBE driver
                             + fresh inbound relay SUBSCRIBE driver
                             (Phase 5 follow-up slice A, `start_relay_subscribe`).
                             **Phase 5 final (SUBSCRIBE slice)** retired the
                             legacy carrier: `OpEnum::Subscribe`,
                             `OpManager.ops.subscribe` DashMap, `impl
                             Operation for SubscribeOp`, `has_subscribe_op`,
                             `remove_subscribe_and_notify_timeout`, the
                             `OpEnum::Subscribe` arm in
                             `IsOperationCompleted for OpEnum`, the
                             `OpEnum::Subscribe` arm in the abort
                             handler, the `try_from_op_enum!(OpEnum::Subscribe, ...)`
                             macro entry, `OpError::StatePushed` (no
                             remaining live constructor),
                             `OpEnum::is_subscription_renewal`, the
                             `ContractHandlerEvent::NotifySubscriptionError`
                             / `...ErrorResponse` chain (executor
                             trait method, bridged helper,
                             send_subscription_error_to_clients), and
                             the `handle_op_request<SubscribeOp>`
                             fallthrough in node.rs are all gone.
                             Every SUBSCRIBE wire variant dispatches
                             unconditionally: Request → `start_relay_subscribe`,
                             Unsubscribe → `handle_unsubscribe_inbound`,
                             Response → bypass to originator waiter,
                             ForwardingAck → no-op. The
                             surviving `OpManager::send_unsubscribe_upstream`
                             sends Unsubscribe through
                             `OpCtx::send_fire_and_forget` directly
                             (bypasses `ops.subscribe`). **Phase 6
                             (SUBSCRIBE slice)** retired the surviving
                             scaffolding: `SubscribeOp`,
                             `SubscribeState`, `SubscribeStats`,
                             `AwaitingResponseData`,
                             `PrepareRequestData`, `CompletedData`,
                             `SubscribeResult`, the `start_op` /
                             `start_op_with_id` test fixtures, the
                             dead `fetch_contract_if_missing` helper
                             plus the inline state-machine / retry
                             matrix / outcome / lifecycle pin tests
                             are all gone. The surviving wire-format
                             types, `handle_unsubscribe_inbound` (the
                             post-#1454 inbound handler),
                             `register_downstream_subscriber`,
                             `prepare_initial_request` /
                             `InitialRequest` (shared by all driver
                             entry points), and
                             `complete_local_subscription` remain
                             because the task-per-tx drivers consume
                             them.
  put/op_ctx_task.rs       → client-initiated PUT driver (Phase 3a)
                             + fresh inbound non-streaming relay PUT
                             driver (Phase 5 follow-up slice A, PR #3917,
                             `start_relay_put`)
                             + fresh inbound streaming relay PUT
                             driver (Phase 5 follow-up slice B,
                             `start_relay_put_streaming`: claims
                             inbound stream via orphan_stream_registry,
                             pipes fragments downstream via
                             NetworkBridge::pipe_stream, bubbles a
                             downgraded non-streaming Response upstream).
                             **Phase 5 final (PUT slice)** retired the
                             legacy carrier: `OpEnum::Put`,
                             `OpManager.ops.put` DashMap, `impl Operation
                             for PutOp`, `has_put_op`,
                             `remove_put_and_report_failure`, and the
                             `handle_op_request<PutOp>` fallthrough in
                             node.rs are all gone. Every PUT wire variant
                             dispatches unconditionally to a task-per-tx
                             driver. **Phase 6 (PUT slice)** retired
                             the surviving scaffolding: `PutOp`,
                             `PutState`, `PutStats`,
                             `AwaitingResponseData`, `FinishedData`,
                             the 22 inline outcome / failure-routing /
                             type-state pin tests, and the four
                             anti-regression pins for retired
                             speculative-retry fields (#3964) are all
                             gone. The surviving wire-format types,
                             the `op_state_manager.rs` GC pin tests,
                             the `put_forwarding_ack_senders` source
                             grep, and the originator finalization
                             helpers (`finalize_put_at_originator` /
                             `PutFinalizationData`) + the local-store
                             helper (`put_contract`) remain because
                             the task-per-tx drivers consume them. The
                             legacy upgrade-on-forward branch (non-stream
                             `Request` whose serialized payload exceeds
                             `streaming_threshold`) now dispatches
                             through `start_relay_put`; the driver itself
                             owns the upgrade decision.
  get/op_ctx_task.rs       → client-initiated GET driver (Phase 3b)
                             + fresh inbound relay GET driver (Phase 5,
                             PR #3896, `start_relay_get`)
                             + sub-op GET driver (`start_sub_op_get`)
                             + targeted sub-op GET driver
                             (`start_targeted_sub_op_get`, replaces
                             legacy `start_targeted_op`).
                             **Phase 5 final (GET slice)** retired the
                             legacy carrier: `OpEnum::Get`,
                             `OpManager.ops.get` DashMap, `impl Operation
                             for GetOp`, `start_op` / `start_op_with_id`
                             / `start_targeted_op`, `request_get`,
                             `has_get_op`, `remove_get_and_report_failure`,
                             and the `handle_op_request<GetOp>`
                             fallthrough in node.rs are all gone.
                             Every GET wire variant dispatches
                             unconditionally to a task-per-tx driver.
                             **Phase 6 (GET slice)** retired the
                             surviving scaffolding: `GetOp`,
                             `GetState`, `GetStats`,
                             `AwaitingResponseData`,
                             `PrepareRequestData`, `FinishedData`,
                             `TryFrom<GetOp> for GetResult`, the
                             inline outcome / failure-routing /
                             type-state / relay-cache-decision pin
                             tests, and the unused `GetResult.key`
                             field are all gone. The surviving
                             wire-format types and the originator
                             result envelope `GetResult` (state +
                             contract only) remain because the
                             task-per-tx drivers and the executor
                             consume them.
  update/op_ctx_task.rs    → client-initiated UPDATE driver (Phase 4,
                             fire-and-forget) + fresh inbound non-streaming
                             relay UPDATE drivers (Phase 5 follow-up slice
                             A, PR #3910: `start_relay_request_update`,
                             `start_relay_broadcast_to`) + fresh inbound
                             streaming relay UPDATE drivers (Phase 5
                             follow-up slice C:
                             `start_relay_request_update_streaming`,
                             `start_relay_broadcast_to_streaming` —
                             claim inbound stream, assemble, apply
                             locally; BroadcastStateChange propagates).
                             **Phase 5 final** retired the legacy carrier:
                             `OpEnum::Update`, `OpManager.ops.update`
                             DashMap, `impl Operation for UpdateOp`,
                             `request_update`, `start_op`,
                             `has_update_op`, `remove_update_and_report_failure`,
                             and the `handle_op_request<UpdateOp>`
                             fallthrough in node.rs are all gone. Every
                             UPDATE wire variant now dispatches
                             unconditionally to a task-per-tx driver.
                             **Phase 6 (UPDATE slice)** retired the
                             surviving scaffolding: `UpdateOp`,
                             `UpdateState`, `UpdateStats`,
                             `FinishedData`, the inline outcome /
                             failure-routing pin tests, and the two
                             stale `process_message`-anchored pin
                             tests (driver-side pin tests in
                             `update/op_ctx_task.rs:2063` /
                             `:2345` cover the same
                             `send_summary_back_on_rejection`
                             invariant). The surviving wire-format
                             types, `BroadcastDedupCache`,
                             `BroadcastTargetResult`,
                             `OpManager::try_auto_fetch_contract`,
                             `update_contract` + `UpdateExecution` +
                             the log helpers, and the
                             post-merge propagation helpers
                             (`send_proactive_summary_notification` /
                             `send_summary_back_on_rejection`) +
                             the `log_severity` regression suite (#3914)
                             + the `summary_back_helper_gates_on_summary_equality`
                             pin remain because the task-per-tx
                             drivers consume them.
  connect/op_ctx_task.rs   → client-initiated CONNECT driver (Phase 2c
                             slice 2, `start_client_connect`) + fresh
                             inbound relay CONNECT driver (Phase 2c
                             slice 1, `start_relay_connect`: full
                             state machine in task locals — initial
                             `handle_request` decision, downstream
                             forward, upstream emits, plus re-entry
                             handling for Response/Rejected/
                             ObservedAddress/ConnectFailed via
                             `OpCtx::send_to_and_collect_replies`)
    All five relay drivers share the per-node dedup gate pattern
    (`active_relay_{get,update,put,subscribe,connect}_txs`) and the
    `Relay*InflightGuard` RAII. PUT and UPDATE streaming relays now run
    on the task-per-tx path. The deprecated UPDATE `Broadcasting` wire
    variant has been removed (gated by a `min-compatible-version`
    bump). SUBSCRIBE has no streaming variants. CONNECT relay's
    admission RAII guard is scoped to the initial-actions block (NOT
    held across the recv loop) to avoid admission slot starvation
    under load. Renewals migrated to
    `subscribe::run_renewal_subscribe` (reuses the client-initiated
    driver with `is_renewal=true`, returns the outcome to the renewal
    task instead of through `result_router_tx`). PUT sub-op subscribes
    are dispatched via `subscribe::run_client_subscribe`. Executor
    auto-subscribe migrated to `subscribe::run_executor_subscribe`
    (returns `Result<(), OpError>` directly; bypasses the `op_request`
    mediator). `Unsubscribe` now routes to
    `subscribe::handle_unsubscribe_inbound` (free function — removes
    downstream subscriber + propagates upstream when interest hits
    zero; replaces the legacy `process_message::Unsubscribe` arm
    retired in #1454 phase 5 final SUBSCRIBE slice). `ForwardingAck`
    is a no-op telemetry hook with no state mutation.
```

## Module Map

| Module | Entry Point | What It Does |
|--------|-------------|--------------|
| `node/` | `node.rs` | Event loop (start here for data flow) |
| `operations/` | `operations.rs` | Transaction state machines |
| `contract/` | `handler.rs` | WASM execution |
| `transport/` | `connection_handler.rs` | Networking layer |
| `ring/` | `ring.rs` | DHT routing |
| `server/` | `server.rs` | Client API |
| `simulation/` | `time.rs`, `rng.rs` | DST abstractions |

## Commands

```bash
cargo test -p freenet                           # All tests
cargo bench --bench transport_perf -- level0    # Quick benchmark
```
