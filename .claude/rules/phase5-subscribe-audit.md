# Phase 5 final — SUBSCRIBE slice audit (next-up after GET)

**Status:** scoping doc. Written alongside the GET slice (#1454
phase 5 final, GET slice). The SUBSCRIBE slice has not started; this
file records what the GET slice's reachability audit found about the
SUBSCRIBE side and what the migration will need.

**Premise:** the GET-slice pattern is reusable. SUBSCRIBE has the
same shape:

1. Client-initiated SUBSCRIBE → migrated in Phase 2b (PR #3806,
   `subscribe::op_ctx_task::run_client_subscribe`).
2. Fresh inbound relay SUBSCRIBE Request → migrated in Phase 5
   follow-up slice A (PR #3932, `start_relay_subscribe`).
3. Renewals → migrated in the SUBSCRIBE renewal slice
   (`run_renewal_subscribe`, reuses `drive_client_subscribe_inner`).
4. Executor auto-subscribe → migrated to
   `subscribe::run_executor_subscribe` (bypasses `op_request`
   mediator).
5. PUT/GET sub-op subscribes → spawn `run_client_subscribe` directly
   from the corresponding op_ctx_task driver via `maybe_subscribe_child`.

What remains: the legacy `OpEnum::Subscribe` carrier, `ops.subscribe`
DashMap, `impl Operation for SubscribeOp`, the
`handle_op_request<SubscribeOp>` fallthrough in `node.rs`,
`SubscribeMsg::Unsubscribe` routing entries created by
`request_unsubscribe` / `create_unsubscribe_op`, and the no-op
`SubscribeMsg::ForwardingAck` handler.

## Per-file audit checklist

Mirror the GET slice's checklist; sections that read "VERIFY"
must be re-confirmed before deletion.

### `crates/core/src/operations/subscribe.rs`

- VERIFY: locate `impl Operation for SubscribeOp` block (analogous
  to `get.rs:910` before deletion).
- VERIFY: `start_op` / `start_op_with_id` constructors. After GET
  slice retirement these are kept by SUBSCRIBE for test fixtures
  only (per existing operations.md note); confirm by greppting for
  callers.
- VERIFY: `request_unsubscribe` / `create_unsubscribe_op`. These are
  the surviving legacy writers into `ops.subscribe` (entries created
  to route `SubscribeMsg::Unsubscribe` through `process_message`).
  Decision: migrate to a task-per-tx adapter OR keep on legacy and
  bypass DashMap deletion.
- COUNT: `#[test]` functions in subscribe/tests.rs. PUT had ~25
  outcome / failure-routing / wire-format / pin tests that survived
  under `#[allow(dead_code)]`; GET had similar counts. SUBSCRIBE
  should be in the same range.

### `crates/core/src/contract/executor.rs`

- VERIFY: any `SubscribeContract` / `impl
  ComposeNetworkMessage<SubscribeOp>` impl. After the executor
  auto-subscribe migration this should be retired; confirm by
  greppting.

### `crates/core/src/node.rs`

- `NetMessageV1::Subscribe(ref op) => { ... }` arm. After GET-slice
  pattern, the bypass dispatches `start_relay_subscribe` for relay
  hops; remaining variants (`Unsubscribe`, `ForwardingAck`) fall
  through to `handle_op_request::<subscribe::SubscribeOp, _>`.
  - Originator-loopback for SUBSCRIBE Request (e.g.
    `SubscribeMsg::Subscribed`) is forwarded to the awaiting task
    via the bypass at `pending_op_results`. With Phase 2b migration,
    no client SUBSCRIBE pushes a `SubscribeOp` into `ops.subscribe`.
    Confirm by reading the bypass branch in
    `handle_pure_network_message_v1`.
  - `SubscribeMsg::ForwardingAck` (no-op handler) — decide whether
    to delete or keep as a no-op match arm (mirrors GET/PUT/UPDATE).
  - `SubscribeMsg::Unsubscribe` — needs migration OR explicit
    legacy-fallthrough preservation.
- `OpEnum::Subscribe` arms in `report_result`, `handle_aborted_op`
  (GET/PUT/UPDATE no-op arm — SUBSCRIBE would join), and the
  `IsOperationCompleted for OpEnum` impl.
- String-grep regression tests in `node.rs::tests` that mention
  `handle_op_request::<subscribe::SubscribeOp, _>` or
  `has_subscribe_op`. Rewrite to anchor on the next NetMessageV1
  variant (GET slice's pattern: collapse multiple tests into one
  with runtime-composed needles for negative pins).

### `crates/core/src/node/op_state_manager.rs`

- `Ops.subscribe: DashMap<Transaction, SubscribeOp>` field.
- `OpManager::push` SUBSCRIBE match arm.
- `peek_next_hop_addr`, `pop` SUBSCRIBE arms.
- `OpManager::has_subscribe_op` — VERIFY it's still consulted by
  node.rs bypass match. After the slice it can go.
- `OpEnum::Subscribe` arm in `completed()`.
- `pending_op_counts()` SUBSCRIBE slot — set to 0 like GET/PUT/UPDATE.
- `remove_subscribe_and_notify_timeout` and its GC sweep callers.
  GET slice deleted both. SUBSCRIBE would join.
- Inline tests: any `has_subscribe_op_*` cases. Delete with the
  `has_subscribe_op` API.

### `crates/core/src/operations.rs`

- `OpEnum::Subscribe` variant + the `delegate!` arm.
- `try_from_op_enum!(OpEnum::Subscribe, subscribe::SubscribeOp,
  TransactionType::Subscribe)`.

### Side-effects

- `request_unsubscribe` / `create_unsubscribe_op` are the only
  surviving legacy entry points. Decide:
  - Option A: keep on legacy (smallest scope; preserves existing
    behavior verbatim). Requires keeping `ops.subscribe` DashMap and
    `impl Operation for SubscribeOp`. Defeats the point of the slice.
  - Option B: migrate `Unsubscribe` to a task-per-tx adapter (e.g.
    `start_unsubscribe`). Most likely choice. The driver fires the
    Unsubscribe wire message, awaits no reply (fire-and-forget).

## Risks specific to SUBSCRIBE (vs. GET/PUT)

1. **`Unsubscribe` migration is mandatory.** Unlike GET's PUT/UPDATE
   slice, SUBSCRIBE has a non-trivial `Unsubscribe` wire variant
   that the legacy state machine routes via `process_message`. A
   "drop-with-warning" stub is not viable: clients rely on
   Unsubscribe to end subscriptions. Plan the migration as part of
   this slice, not a follow-up.

2. **`ForwardingAck` no-op handler.** GET slice kept it as a
   no-op match arm for backward compat with pre-#3964 peers.
   SUBSCRIBE has the same pattern; preserve it.

3. **`SubscribeMsg::Subscribed` originator-loopback.** GET bubbles
   `Response{Found}` through the bypass and the driver finalizes.
   SUBSCRIBE has `Subscribed` / `NotSubscribed` reply variants.
   Confirm both bypass correctly. The existing operations.md note
   already documents this.

4. **No streaming variants.** SUBSCRIBE is small-payload only; no
   parallel to PUT slice B's streaming relay migration.

## Estimated size

- Code deletion: ~1500 LoC (SUBSCRIBE's legacy state machine is
  smaller than GET's; SUBSCRIBE has fewer retry helpers and no
  streaming variants).
- Test deletion: 20–30 inline tests (PUT kept 25; GET kept similar).
- Files touched: same 6 (executor.rs, executor/runtime.rs,
  subscribe.rs, node.rs, op_state_manager.rs, operations.rs) + this
  file + CLAUDE.md + operations.md + Unsubscribe migration site.

## Sequencing recommendation

1. Pre-implementation: re-run the "executor mode constructor"
   audit. Confirm `OperationMode::Network` is unreachable for any
   surviving SUBSCRIBE entry point.
2. Pre-implementation: write `start_unsubscribe` task-per-tx
   adapter for `request_unsubscribe`.
3. Commit 1: migrate `Unsubscribe` to task-per-tx; mark the legacy
   helpers `#[allow(dead_code)]`.
4. Commit 2: delete `impl Operation for SubscribeOp`. Merge with
   commit 3 if compilation pressure forces it.
5. Commit 3: delete `handle_op_request<SubscribeOp>` fallthrough +
   OpEnum::Subscribe match arms in node.rs. Update string-grep pins.
6. Commit 4: delete `ops.subscribe` DashMap, `has_subscribe_op`,
   `remove_subscribe_and_notify_timeout`, `OpEnum::Subscribe`
   variant, `try_from_op_enum!` macro entry.
7. Commit 5: docs sweep (this file gets superseded by
   `phase5-connect-audit.md` once SUBSCRIBE is done — or by phase 6
   if SUBSCRIBE is the last slice before CONNECT relay tightening).
