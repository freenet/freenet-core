# Phase 5 final — GET slice audit (next-up after PUT)

**Status:** scoping doc. Written alongside the PUT slice (#1454
phase 5 final, PUT slice). The GET slice has not started; this file
records what the PUT slice's reachability audit found about the
GET side and what the migration will need.

**Premise:** the PUT slice's pattern is reusable. GET has the
same shape:

1. Client-initiated GET → migrated in Phase 3b
   (`start_client_get`).
2. Fresh inbound non-streaming relay GET → migrated in Phase 5
   (PR #3896, `start_relay_get`).
3. GET sub-op (subscribe / executor) → migrated in
   `start_sub_op_get`.
4. `start_targeted_op` (UPDATE auto-fetch invokes a GET legacy
   entry that spawns `OpEnum::Get`) → still on legacy.

What remains: the legacy `OpEnum::Get` carrier, `ops.get` DashMap,
`impl Operation for GetOp`, the `handle_op_request<GetOp>`
fallthrough in `node.rs`, GC speculative-retry block (already
retired per existing operations.md note), and `start_targeted_op`.

## Per-file audit checklist

Mirror the PUT slice's checklist; sections that read "VERIFY"
must be re-confirmed before deletion.

### `crates/core/src/operations/get.rs`

- VERIFY: locate `impl Operation for GetOp` block (analogous to
  `put.rs:185-1302` before deletion).
- VERIFY: `start_targeted_op` (a GET legacy entry that UPDATE's
  auto-fetch spawns) — count callers in `update.rs:343-349` and
  any other module. Decision: keep on legacy (preserves UPDATE
  auto-fetch semantics) OR migrate to a task-per-tx variant of
  `start_sub_op_get`. Operations.md still references it as a
  surviving legacy entry.
- VERIFY: GC speculative-retry block already retired (per
  existing operations.md note). No `get_retried` HashMap, no
  `GetOp::ack_received` / `GetOp::speculative_paths` fields.
- COUNT: `#[test]` functions. PUT had 25 outcome / failure-routing
  / wire-format / pin tests that survived under
  `#[allow(dead_code)]`; GET should have similar counts.

### `crates/core/src/contract/executor.rs`

- VERIFY: does `GetContract` / `impl ComposeNetworkMessage<GetOp>`
  exist? Per existing operations.md note, "the executor's
  `GetContract` / `ComposeNetworkMessage<GetOp>` impl is deleted"
  (sub-op GET migration retired it). So this section likely
  empty.

### `crates/core/src/contract/executor/runtime.rs`

- VERIFY: any `perform_contract_get` Network branch. UPDATE's
  Network branch was dead (no `OperationMode::Network` constructor).
  GET likely shares that property.

### `crates/core/src/node.rs`

- `NetMessageV1::Get(ref op) => { ... }` arm. After PUT slice
  pattern, the bypass dispatches `start_relay_get` for relay
  hops; remaining variants fall through to
  `handle_op_request::<get::GetOp, _>`.
  - Originator-loopback for GET (e.g. `GetMsg::Response{Found}`)
    is forwarded to the awaiting task via the bypass at
    `pending_op_results`. With Phase 3b migration, no client GET
    pushes a `GetOp` into `ops.get`. Confirm by reading the bypass
    branch in `handle_pure_network_message_v1`.
  - `GetMsg::ForwardingAck` (no-op handler) — decide whether to
    delete or keep as a no-op match arm (mirrors PUT/SUBSCRIBE).
- `OpEnum::Get` arms in `report_result` (~line 540 region),
  `handle_aborted_op` (PUT slice replaced its abort arm with
  unified `Put | Update => no-op`; GET would join), and the
  `IsOperationCompleted for OpEnum` impl.
- String-grep regression tests in `node.rs::tests` that mention
  `handle_op_request::<get::GetOp, _>` or `has_get_op`. Rewrite
  to anchor on the next NetMessageV1 variant (PUT's slice
  collapsed 5 tests into 1 with runtime-composed needles for
  negative pins; GET can mirror that).

### `crates/core/src/node/op_state_manager.rs`

- `Ops.get: DashMap<Transaction, GetOp>` field.
- `OpManager::push` GET match arm.
- `peek_next_hop_addr`, `get_current_hop`, `pop` GET arms.
- `OpManager::has_get_op` — VERIFY it's still consulted by node.rs
  bypass match. After this slice it can go.
- `OpEnum::Get` arm in `completed()`.
- `pending_op_counts()` GET slot — set to 0 like PUT/UPDATE.
- `remove_get_and_report_failure` and its GC sweep callers. PUT
  slice deleted both PUT and UPDATE handlers; GET would join.
- Inline tests: `has_get_op_returns_*`. Delete with the
  `has_get_op` API.

### `crates/core/src/operations.rs`

- `OpEnum::Get` variant + the `delegate!` arm.
- `try_from_op_enum!(OpEnum::Get, get::GetOp, TransactionType::Get)`.

### Side-effects

- `start_targeted_op` still owns UPDATE auto-fetch's GET
  spawning. Decide:
  - Option A: keep on legacy (smallest scope; preserves existing
    behavior verbatim). The driver returns an `OpEnum::Get` that
    the legacy GC sweep eventually cleans up. After this slice
    deletes `OpEnum::Get`, this option is no longer viable.
  - Option B: migrate to a task-per-tx variant of
    `start_sub_op_get`. UPDATE auto-fetch becomes a fire-and-forget
    spawn. Most likely choice; mirrors the executor's
    `local_state_or_from_network` pattern.

## Risks specific to GET (vs. PUT)

1. **`start_targeted_op` migration is mandatory.** Unlike PUT's
   upgrade-on-forward edge case (which can drop with a warning),
   UPDATE auto-fetch is a load-bearing path: contracts that
   trigger `RequestRelated` during validation depend on the GET
   to return state. A "drop with warning" stub here would break
   contract validation. Plan the migration as part of this slice,
   not a follow-up.

2. **GC speculative retry block already retired.** PUT slice
   confirmed PUT's was retired in PR #3964. Existing operations.md
   note confirms GET's retired in "phase 5-final retired that
   block" (parallel pass). VERIFY by greppting for
   `get_retry_candidates` in `op_state_manager.rs` — should be
   absent.

3. **`GetMsg::Response{Found}` originator-loopback.** PUT bubbles
   `Response` through the bypass and the driver finalizes. GET
   has Response{Found, NotFound} variants. Confirm both bypass
   correctly. The existing operations.md note already documents
   this for GET.

4. **`GetMsg::ResponseStreaming` upgrade-on-forward.** PUT slice
   surfaced this as a real edge case (slice A driver does not
   upgrade). GET's relay driver (`start_relay_get`) — VERIFY
   whether it upgrades. If not, decide: drop-with-warning (PUT
   slice's choice) or teach the driver.

## Estimated size

- Code deletion: ~1500–2000 LoC (GET's legacy state machine is
  comparable to PUT's; PUT deleted ~1100 in `impl Operation` plus
  ~150 in `node.rs` plus ~150 in `op_state_manager.rs` =
  ~1400 net).
- Test deletion: 25–35 inline tests (PUT kept 25 under allow;
  GET counts to be verified).
- Files touched: same 6 (executor.rs, executor/runtime.rs, get.rs,
  node.rs, op_state_manager.rs, operations.rs) + this file +
  CLAUDE.md + operations.md + start_targeted_op migration site
  (`update.rs`).

## Sequencing recommendation

1. Pre-implementation: re-run the "executor mode constructor"
   audit. Confirm `OperationMode::Network` is unreachable.
2. Pre-implementation: write `start_sub_op_get`-style migration
   for `start_targeted_op` (or extend `start_sub_op_get` to
   accept the UPDATE-auto-fetch shape).
3. Commit 1: retire `start_targeted_op` legacy → migrate UPDATE
   auto-fetch to the task-per-tx adapter.
4. Commit 2: delete `impl Operation for GetOp`. Merge with
   commit 3 if compilation pressure forces it.
5. Commit 3: delete `handle_op_request<GetOp>` fallthrough +
   OpEnum::Get match arms in node.rs. Update string-grep pins.
6. Commit 4: delete `ops.get` DashMap, `has_get_op`,
   `remove_get_and_report_failure`, `OpEnum::Get` variant,
   `try_from_op_enum!` macro entry.
7. Commit 5: docs sweep (this file gets superseded by
   `phase5-subscribe-audit.md` once GET is done).
