# Relay UPDATE → task-per-tx port plan (#1454 phase 5 follow-up)

**Branch:** `phase-5-relay-update`
**Baseline:** main @ `bc8ee9d` (PR #3896 — relay GET task-per-tx) merged.
**Predecessor patterns:** PR #3806 (SUBSCRIBE 2b), #3843 (PUT 3a), #3884 (GET 3b),
#3891 (UPDATE 4 client-init), #3896 (relay GET phase 5).

## 1. Problem statement

Today the **client-initiated** UPDATE at an originator runs through
`operations/update/op_ctx_task.rs::start_client_update` (Phase 4). But every
**relay hop** that receives any of the five `UpdateMsg` wire variants still
hits the legacy `process_message` re-entry loop:

```
node.rs:1191  NetMessageV1::Update
  → handle_op_request::<update::UpdateOp, _>
    → UpdateOp::process_message
      → match UpdateMsg { RequestUpdate
                        | BroadcastTo
                        | Broadcasting (deprecated)
                        | RequestUpdateStreaming
                        | BroadcastToStreaming }
```

Relay state lives in an `UpdateOp` stored in `OpManager.ops.update` indexed
by tx, mutated by each incoming message. Same state-machine pattern the
task-per-tx refactor is eliminating.

**Scope of this port:** migrate the relay-hop side of UPDATE to a task-per-tx
driver so a relay receiving `UpdateMsg::RequestUpdate` or
`UpdateMsg::BroadcastTo` spawns a driver that owns the local-apply +
forward-or-broadcast decision in task locals, with **no `UpdateOp` in
`OpManager.ops.update` for any relay hop**.

## 2. Architectural map (UPDATE differs from GET)

UPDATE is **not** request/response. There is no upstream reply on success
or failure for `RequestUpdate` or `BroadcastTo`. Relay state transitions
are terminal (`Finished` or `None`). The op survives only because GC needs
something to time-out for telemetry.

Wire variants and their relay roles:

| Variant | Relay role | Reply? | Side effects |
|---|---|---|---|
| `RequestUpdate` | Forward inbound update toward closest peer that hosts contract | None | If has-contract: apply locally → BroadcastStateChange (automatic). If not: forward downstream once. |
| `BroadcastTo` | Apply broadcast payload locally; auto-fanout via BroadcastStateChange | None | Dedup cache check, WASM merge, ResyncRequest on delta failure, try_auto_fetch_contract on missing-params, proactive summary notify. |
| `Broadcasting` | DEPRECATED no-op | None | None (logging only). |
| `RequestUpdateStreaming` | Same as RequestUpdate but assembles streamed value first | None | Stream claim, assemble, deserialize, then RequestUpdate logic. |
| `BroadcastToStreaming` | Same as BroadcastTo but streamed | None | Stream claim, assemble, then BroadcastTo logic. |

**Loop-back consideration:** Phase 4's client driver does NOT call
`send_to_and_await(target=None)` like GET does. It calls
`send_fire_and_forget(target_addr, msg)` for the remote case and never
loops back through pure-network-message handler. So **no loop-back path
to preserve** for UPDATE — this is simpler than GET.

**Per-node dedup:** GET relay added `active_relay_get_txs` to drop
duplicate inbound Requests. UPDATE needs a similar gate but the
amplification risk is lower because there's no retry loop and no waited
response. Still add `active_relay_update_txs` for parity + safety against
GC-spawned re-entries.

## 3. Scope decision: which variants migrate

**This PR — slice A:**
- ✅ `RequestUpdate` (non-streaming)
- ✅ `BroadcastTo` (non-streaming)
- ✅ `Broadcasting` (deprecated — fall through to legacy no-op handler;
   not worth migrating)

**Deferred — slice B (follow-up PR):**
- ⏳ `RequestUpdateStreaming`
- ⏳ `BroadcastToStreaming`

Streaming variants share the same orphan-stream infrastructure as GET's
deferred slice 4. Migrate both ops' streaming together so the orphan-claim
+ assembly pattern lands once.

**Slice C (cleanup):**
- ⏳ Remove relay branches from `update.rs::process_message` once both
  slices A + B above prove stable.

## 4. Code map

### 4.1 Legacy relay regions to bypass (slice A)

| Region | Lines | Role |
|---|---|---|
| `UpdateMsg::RequestUpdate` arm | 347-594 | Local apply or forward to next hop |
| `UpdateMsg::BroadcastTo` arm | 595-825 | Apply broadcast, dedup, ResyncRequest, auto-fetch |
| `UpdateMsg::Broadcasting` arm | 826-844 | Deprecated logging-only |

Streaming branches (847-1054 and beyond) stay legacy in slice A.

### 4.2 New driver entry points — `operations/update/op_ctx_task.rs`

```rust
pub(crate) async fn start_relay_request_update(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    related_contracts: RelatedContracts<'static>,
    value: WrappedState,
    sender_addr: SocketAddr,
) -> Result<(), OpError>;

pub(crate) async fn start_relay_broadcast_to(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,
    key: ContractKey,
    payload: DeltaOrFullState,
    sender_summary_bytes: Vec<u8>,
    sender_addr: SocketAddr,
) -> Result<(), OpError>;
```

Driver bodies mirror legacy `process_message` arms but:
- No `OpManager.ops.update` push.
- No `return_msg` / `new_state` bookkeeping.
- All side effects (dedup cache, WASM merge, ResyncRequest, auto-fetch,
  proactive summary) reproduced verbatim.
- Forward-downstream uses `OpCtx::send_fire_and_forget` (no
  `send_to_and_await` — UPDATE has no reply to await).
- Per-node dedup via `OpManager::active_relay_update_txs` (DashSet).

### 4.3 Dispatch wiring — `node.rs::NetMessageV1::Update` branch

Replaces `handle_op_request::<update::UpdateOp, _>` for fresh inbound
`RequestUpdate` / `BroadcastTo` with `source_addr.is_some()` and no
existing `UpdateOp`. All other paths fall through to legacy:

- `RequestUpdateStreaming` / `BroadcastToStreaming` → legacy (slice B).
- `Broadcasting` → legacy (deprecated no-op).
- Existing `UpdateOp` (GC-spawned retries, originator loop-back) → legacy.

Add `OpManager::has_update_op(tx) -> bool` mirroring `has_get_op`.

### 4.4 Telemetry parity

Each relay arm emits NetEventLog events (`update_request`,
`update_success`, `update_broadcast_received`, `update_broadcast_applied`,
`update_failure`). Driver replicates every emission point with same
arguments and ordering.

## 5. Tricky cases

### 5.1 BroadcastTo dedup cache (#5: amplification risk)

`broadcast_dedup_cache.check_and_insert` MUST fire BEFORE the WASM
merge. If we skip dedup and run merge for every duplicate broadcast, a
gossiping fanout multiplies WASM cost N-fold. Driver inserts the dedup
check at the same point legacy does (after telemetry, before merge).

### 5.2 ResyncRequest on delta-merge failure

Legacy `update.rs:759-758` sends `InterestMessage::ResyncRequest` via
`notify_node_event` when delta application fails. Driver MUST replicate
this — it's the primary recovery path for state divergence between peers.

### 5.3 try_auto_fetch_contract on missing params

`update.rs:764` triggers self-healing GET when full-state update fails
with non-rejection error. Replicate in driver.

### 5.4 Proactive summary notification

`update.rs:806-819` spawns background task to notify interested peers
when state changes (so they can update their cached summary of us).
Driver MUST also spawn this — without it, peers re-broadcast stale data
to us repeatedly.

### 5.5 No upstream reply

UPDATE relay never sends a response back to upstream. The originator's
client driver completes IMMEDIATELY after `send_fire_and_forget`
returns (Phase 4 design). So the new relay driver:
- Does NOT call `send_and_await` anywhere.
- Does NOT install a `pending_op_results` slot.
- Does NOT need any reply-routing bypass in `node.rs`.

This is simpler than GET relay and means **no per-node dedup
amplification risk** of the kind that hit phase 5 GET. We still add
the dedup gate for tx-collision robustness against GC-spawned retries.

### 5.6 Originator loopback non-issue

Phase 4 client driver (`update/op_ctx_task.rs:264-273`) builds
`UpdateMsg::RequestUpdate` and sends via `send_fire_and_forget` to a
remote peer. It never sends to itself. There's no Phase-4-equivalent of
GET's `send_and_await(target=None)` loop-back. So we don't need a
`source_addr.is_none()` legacy fall-through for Phase 4 compatibility.

The dispatch gate still gates on `source_addr.is_some()` defensively —
internal callers (e.g. `start_op_with_id` from `start_targeted_op`)
that pre-register an `UpdateOp` will be caught by `has_update_op(tx)
-> true`.

### 5.7 Apply-amplification-fixes preemptively

Mitigate the four amplifiers from PR #3896 even though most don't
apply to UPDATE:
1. ✅ No retry loop at relay (UPDATE forwards once unconditionally).
2. ✅ No fresh attempt_tx (UPDATE uses incoming_tx directly).
3. ✅ No ForwardingAck (UPDATE never had one).
4. ✅ Per-node dedup via `active_relay_update_txs` DashSet.

## 6. Commit split

1. **`refactor(ops): add relay UPDATE task-per-tx driver scaffold`**
   Add `start_relay_request_update` + `start_relay_broadcast_to` +
   helpers to `operations/update/op_ctx_task.rs`. Add
   `OpManager::has_update_op` + `active_relay_update_txs` field.
   `#[allow(dead_code)]` on driver — not yet wired. Unit tests for:
   - Dispatch decision tree (has-contract vs no-contract for RequestUpdate)
   - Dedup cache hit on duplicate BroadcastTo
   - ResyncRequest emission on delta failure
   - try_auto_fetch_contract emission on full-state failure
   - Per-node dedup gate rejects duplicate tx
   `cargo test -p freenet --lib` green.

2. **`refactor(ops): route relay UPDATE through task-per-tx driver`**
   Switch dispatch in `node.rs::NetMessageV1::Update` branch to call
   `start_relay_request_update` / `start_relay_broadcast_to` for fresh
   inbound non-streaming variants with `source_addr.is_some()` and no
   existing `UpdateOp`. Lift `dead_code` allows. Add structural pin
   tests + behavioral invariants:
   - T1: dispatch routes RequestUpdate to driver
   - T2: dispatch routes BroadcastTo to driver
   - T3: dispatch falls through for Streaming variants
   - T4: dispatch falls through for Broadcasting (deprecated)
   - T5: dispatch falls through when has_update_op returns true
   - T6: dispatch falls through when source_addr is None
   - T7: counter increments on driver entry
   - T8: relay driver does NOT call send_and_await
   - T9: relay driver MUST call dedup cache before merge

3. **`refactor(ops): remove unreachable relay arms from update.rs`**
   Delete the `RequestUpdate` and `BroadcastTo` arms of
   `process_message` once tests confirm they're unreachable for fresh
   inbound traffic. Keep streaming arms (slice B). Verify with grep.

## 7. Validation gates

### 7.1 Per-commit

```
cargo fmt
cargo clippy -- -D warnings
cargo test -p freenet --lib
```

### 7.2 Commit 2 (behavior flip)

Sim integration tests on Linux via `/linux-test`:
- `test_update_basic_propagation`
- `test_update_two_node`
- `test_update_three_node`
- `test_update_propagates_through_relay`
- `test_update_fanout`
- `test_update_subscribe_after_update_propagates`
- `test_concurrent_updates_from_multiple_clients`
- `test_update_delta_merge_recovery`
- `test_update_resync_on_delta_failure`
- `test_update_streaming_large_payload` (must stay green — slice A doesn't
  touch streaming, but verify dispatch fallthrough works)

### 7.3 Memory regression watch

Run `simulation-debug.yml` ci-fault-loss workflow with RSS sampler.
Compare against post-#3896 baseline (2.6 GB peak). Driver has no retry
loop and no `send_and_await` — expected RSS impact: minimal. Any
regression is suspicious.

## 8. Risks

- **Dedup cache consistency:** if driver misses the dedup check, a
  duplicate BroadcastTo runs WASM merge twice, doubling executor cost.
  Mitigation: T9 structural test pins the call order.

- **ResyncRequest semantics:** if the driver doesn't clear cached peer
  summary before sending ResyncRequest, the peer sends back the same
  delta and the loop continues. Mitigation: replicate clear-then-send
  ordering exactly.

- **Auto-fetch contract:** `try_auto_fetch_contract` spawns a GET
  internally via `start_targeted_op`. Verify spawn succeeds and the
  fetched contract is properly stored.

- **Proactive summary task:** `tokio::spawn`ed background task. Per
  `code-style.md` rule "no fire-and-forget for critical tasks" — but
  this matches legacy. Replicate without registering with
  `BackgroundTaskMonitor` to preserve parity. Document why in driver
  comment.

- **Telemetry coverage gaps:** if any `NetEventLog::*` emission is
  dropped, downstream observability tooling sees gaps in update
  traces. T-series structural tests grep for each emission name in
  driver source.

## 9. Out of scope (slice B + slice C follow-ups)

- `RequestUpdateStreaming` / `BroadcastToStreaming` migration.
- Removal of `UpdateOp::process_message` relay arms.
- `Broadcasting` (deprecated) handler removal.
- Final `OpManager.ops.update` DashMap deletion (Phase 5 close-out).
