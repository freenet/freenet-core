# Phase 5 final — PUT slice audit (next-up after UPDATE)

**Status:** scoping doc. Written alongside the UPDATE slice (#1454
phase 5 final commit 5). The PUT slice has not started; this file
records what the UPDATE slice's reachability audit found about the
PUT side and what the migration will need.

**Premise:** the UPDATE slice's pattern is reusable. PUT has the
same shape:

1. Client-initiated PUT → migrated in Phase 3a (`start_client_put`).
2. Fresh inbound non-streaming relay PUT → migrated in Phase 5
   follow-up slice A (PR #3917, `start_relay_put`).
3. Fresh inbound streaming relay PUT → migrated in Phase 5
   follow-up slice B (`start_relay_put_streaming`).

What remains: the legacy `OpEnum::Put` carrier, `ops.put` DashMap,
`impl Operation for PutOp`, `request_put` / `start_op` (if PUT has
them), and the `handle_op_request<PutOp, _>` fallthrough in
`node.rs`.

## Per-file audit checklist

Mirror the UPDATE slice's checklist; sections that read "VERIFY"
must be re-confirmed before deletion.

### `crates/core/src/operations/put.rs`

- VERIFY: locate `impl Operation for PutOp` block (analogous to
  `update.rs:284-1361` before deletion). Likely at a similar
  position. Includes `load_or_init`, `process_message`, `id`.
- VERIFY: locate the originator-side helpers analogous to
  UPDATE's `request_update` / `start_op` / `start_op_with_id` /
  `deliver_update_result`. PUT has originator-loopback paths the
  UPDATE didn't (per #1454 phase 5 follow-up "PUT GC speculative
  retries... retired (PR #3964)"). Those retries may already be
  gone, simplifying this slice.
- VERIFY: which `OpEnum::Put(...)` constructions remain inside
  `process_message`. The match arms that produced them are
  candidates for deletion.
- COUNT: `#[test]` functions. UPDATE had 17 outcome /
  failure-routing / wire-format tests that survived under
  `#[allow(dead_code)]`; PUT may have similar counts.

### `crates/core/src/contract/executor.rs`

- VERIFY: does `PutContract` / `impl ComposeNetworkMessage<PutOp>`
  exist? UPDATE had `UpdateContract` here. If so, the network
  branch of `perform_contract_put` (analogous to
  `perform_contract_update`) is dead because the executor is always
  `OperationMode::Local`. Same retirement pattern.
- The `op_request` mediator + `ComposeNetworkMessage` trait stay
  under `#[allow(dead_code)]` for phase 6 (orphan after the last
  ComposeNetworkMessage impl is gone, but threaded through builders).

### `crates/core/src/contract/executor/runtime.rs`

- `perform_contract_put` body: VERIFY whether it has a Network
  branch analogous to UPDATE's lines 3107–3249. If so, retire it
  and add a parallel pin test
  (`perform_contract_put_does_not_use_legacy_network_path`).

### `crates/core/src/node.rs`

- `NetMessageV1::Put(ref op) => { ... }` arm. After the slice-A
  bypass match dispatches `start_relay_put` (and slice-B
  `start_relay_put_streaming`), verify what variants still fall
  through to `handle_op_request::<put::PutOp, _>`.
  - Originator-loopback for PUT (e.g. `PutMsg::Response` /
    `PutMsg::SuccessfulPut`) lands here for ops still in `ops.put`
    on the originator. Phase 3a's `start_client_put` doesn't
    push to `ops.put`, so these should already be unreachable on
    the client-initiated path. Confirm before deletion.
- `OpEnum::Put` arms in `report_result` (~line 540 region),
  `handle_aborted_op` (~line 2770 region), and the
  `IsOperationCompleted for OpEnum` impl (~line 2961 region).
- String-grep regression tests in `node.rs::tests` that mention
  `handle_op_request::<put::PutOp, _>` or `has_put_op`. Rewrite
  to anchor on the next NetMessageV1 variant (UPDATE's slice
  collapsed 4 tests into 2 and used runtime-composed needles for
  negative pins; PUT can mirror that).

### `crates/core/src/node/op_state_manager.rs`

- `Ops.put: DashMap<Transaction, PutOp>` field.
- `OpManager::push` PUT match arm (~line 720).
- `peek_next_hop_addr`, `peek_target_peer`, `get_current_hop`,
  `pop` PUT arms (~lines 757, 783, 795, 819).
- `OpManager::has_put_op` — VERIFY it's still consulted by node.rs
  bypass match. After this slice it can go.
- `OpEnum::Put` arm in `completed()` (~line 967).
- `pending_op_counts()` PUT slot — set to 0 like UPDATE.
- `remove_put_and_report_failure` (~line 1290 region) and its GC
  sweep callers.
- Inline tests: `has_put_op_returns_*`. Delete with the
  `has_put_op` API.

### `crates/core/src/operations.rs`

- `OpEnum::Put` variant + the `delegate!` arm.
- `try_from_op_enum!(OpEnum::Put, put::PutOp, TransactionType::Put)`.

### Side-effects

- `BroadcastDedupCache` analog for PUT (if any). UPDATE has one;
  PUT may share it via the executor's broadcast machinery. VERIFY.
- `OrphanStreamRegistry` interactions: PUT streaming relay (slice B)
  claims orphan streams. Make sure deletion of the legacy state
  machine doesn't strand a stream-claim path.
- Subscribe sub-op spawning from PUT (`maybe_subscribe_child` in
  `put/op_ctx_task.rs`) is on the task-per-tx side already and
  shouldn't be affected.

## Risks specific to PUT (vs. UPDATE)

1. **Originator-loopback for streaming PUT.** UPDATE is fire-and-forget
   end-to-end; PUT has a Response that the originator awaits.
   `start_client_put` already handles this via
   `OpCtx::send_and_await`, but verify no path still funnels a
   PUT Response through the legacy fallthrough.

2. **Speculative retry remnants.** PR #3964 retired PUT GC
   speculative retries. VERIFY there are no leftover
   `ack_received` / `speculative_paths` field reads in the
   legacy `process_message` that would crash if the fields are
   already gone.

3. **`PutMsg::ForwardingAck` consumer.** Per existing
   operations.md note: "only the upgrade-on-forward without
   inbound `StreamId` and a no-op `ForwardingAck` receiver remain
   on legacy." Decide whether the no-op receiver moves into the
   task-per-tx driver or is simply deleted (mirrors GET +
   SUBSCRIBE which already deleted theirs to no-op handlers).

4. **Streaming upgrade-on-forward.** The legacy path handles
   `PutMsg::Request` whose forwarded payload would exceed
   `streaming_threshold` — the relay needs to upgrade the
   downstream send to a `RequestStreaming`. Slice B left this on
   legacy. Verify whether the slice-B driver can absorb it (or
   the slice should retire that legacy edge first).

## Estimated size

- Code deletion: ~1500–2000 LoC (PUT's legacy state machine is
  comparable to UPDATE's; UPDATE deleted ~1900).
- Test deletion: 20–30 inline tests (UPDATE deleted 4, kept 17
  under allow; PUT counts to be verified).
- Files touched: same 6 (executor.rs, executor/runtime.rs, put.rs,
  node.rs, op_state_manager.rs, operations.rs) + this file +
  CLAUDE.md + operations.md.

## Sequencing recommendation

1. Pre-implementation: re-run the "executor mode constructor"
   audit. UPDATE's `OperationMode::Network` is unreachable; PUT
   may share that property — confirm.
2. Commit 1: retire executor PUT network branch (if dead).
3. Commit 2: delete `impl Operation for PutOp`, `request_put`,
   `start_op` family. Merge with commit 3 if compilation pressure
   forces it (UPDATE had to merge 2+3).
4. Commit 3: delete `handle_op_request<PutOp>` fallthrough +
   OpEnum::Put match arms in node.rs. Update string-grep pins.
5. Commit 4: delete `ops.put` DashMap, `has_put_op`,
   `remove_put_and_report_failure`, `OpEnum::Put` variant,
   `try_from_op_enum!` macro entry.
6. Commit 5: docs sweep (this file gets superseded by a parallel
   `phase5-get-audit.md` once PUT is done).
