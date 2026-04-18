# Relay GET → task-per-tx port plan (#3883, #1454 phase 5)

**Branch:** `issue-3883-relay-get-task` (rebased on origin/main @ `c3413946`)
**Baseline:** phases 2b (SUBSCRIBE), 3a (PUT client), 3b (GET client), 3c
(decouple SubOperationTracker), 4 (UPDATE client) all merged.

## 1. Problem statement

Today the **client-initiated** GET at an originator node is driven by
`operations/get/op_ctx_task.rs::drive_client_get_inner` (phase 3b). But
every **relay hop** that receives a `GetMsg::Request` on the wire still
hits the legacy path:

```
node.rs:1073  NetMessageV1::Get
  → handle_op_request::<get::GetOp, _>        (when not a terminal reply
                                               for an active client driver)
    → Op::process_message                     (get.rs ~line 1210)
      → match GetMsg { Request | Response{Found} | Response{NotFound}
                     | ResponseStreaming | ForwardingAck | ... }
```

The relay state (candidate list, HTL, upstream_addr, retry attempts,
sub-operation tracking) lives in a `GetOp` stored in `OpManager.ops.get`.
That `GetOp` is indexed by transaction ID and mutated by each incoming
`GetMsg` through the `process_message` re-entry loop — the same pattern
the task-per-tx refactor is eliminating.

**Scope of this port:** migrate the relay-hop side of GET to a
task-per-tx driver so that a relay that receives `GetMsg::Request` spawns
a driver which owns routing / forwarding / NotFound-retry / Found-
response-bubble-up in task locals, with **no `GetOp` in `OpManager.ops.get`
for any relay hop of the transaction**.

## 2. Architectural insight (end-to-end trace complete)

Traced the originator loop-back path end-to-end. Findings:

**Loop-back mechanism (phase-3b client driver):**
1. `drive_client_get_inner` calls `send_and_await(Request, target=None)`
   (`get/op_ctx_task.rs:535-544` builds the Request; `op_ctx.rs:155-156`
   passes `target_addr=None`).
2. `handle_op_execution` (`p2p_protoc.rs:3870-3891`): `None` target →
   `ConnEvent::InboundMessage(msg)` — a pure local loop-back, no wire I/O.
3. `handle_inbound_message` → `process_message_decoupled` →
   `handle_pure_network_message_v1` with `source_addr=None`.
4. `NetMessageV1::Get(Request)` bypass filter at `node.rs:1082-1091`
   skips (Request not terminal) → `handle_op_request::<GetOp>` at
   line 1093.
5. `GetOp::load_or_init` (`get.rs:1170-1203`): no existing op for this
   tx, message is `Request` not `Response`, so creates a **fresh GetOp**
   with `GetState::ReceivedRequest` and `upstream_addr=None`. This is
   identical to the real-relay entry with `source_addr=peer_addr`,
   except `upstream_addr` is `None`.
6. `GetOp::process_message(Request)` runs the relay state machine:
   HTL check, local-cache lookup (`get.rs:1366-1428`). Cache hit →
   returns `Ok(Some(op))` with `OperationResult { return_msg: None, ... }`
   → `is_operation_completed` true →
   `forward_pending_op_result_if_completed` (`node.rs:1102-1106`) echoes
   the **original Request** into the driver's `pending_op_results`
   callback.
7. Driver's `classify` (`get/op_ctx_task.rs:505-507`) matches
   `GetMsg::Request` → `Terminal::LocalCompletion` → driver assembles
   HostResponse from local store.

**Key discovery:** originator loop-back IS a relay call with
`source_addr=None`. The same code path that handles true relay handles
loop-back, distinguished only by `upstream_addr=None`.

**Consequence for this port:**

  **Option C — dispatch split** is the cleanest fit:
  - `NetMessageV1::Get(Request)` with `source_addr.is_some()` → new
    `start_relay_get` task-per-tx driver (true relay case).
  - `NetMessageV1::Get(Request)` with `source_addr.is_none()` →
    continue through `handle_op_request` → legacy `process_message`
    relay arm (originator loop-back only).

This preserves the phase-3b client driver's Request-echo contract
without any phase-3b changes, and isolates the new driver to the true
relay case. Commit 4 (dead-code removal) can safely delete only the
real-relay code paths; the HTL check + local-cache-check branches
remain live for loop-back but can eventually be inlined into the
client driver in a follow-up.

Alternative — **Option B** (migrate loop-back into client driver,
delete legacy `GetMsg::Request` arm entirely) — is strictly cleaner
long-term but requires rewriting phase 3b's local-completion
plumbing. Out of scope for this PR.

**This plan uses Option C.**

## 3. Code map — what moves where

### 3.1 Legacy relay code to remove (from `operations/get.rs`)

| Region | Lines (current main) | Role |
|---|---|---|
| `GetState::ReceivedRequest` variant + construction | 504, 1316-1325 | Relay entry state |
| `GetMsg::Request` match arm | 1239-1668 | HTL check, local cache check, forward-or-respond decision |
| `GetMsg::Response{NotFound}` relay arm | 1672-1799 | Retry to next alternative peer, or upstream NotFound |
| `GetMsg::Response{Found}` relay arm | 1811-2418 (relay side only — ~1800-2050 approx; originator-side kept) | Cache locally, forward upstream |
| `GetMsg::ForwardingAck` handling | ~1175 (pattern), send at 1628-1652 | Fire-and-forget ack timing |
| Relay-side `request_get()` call sites | 184-390 (partial — originator side kept, relay-side forwarding removed) | Initial forward from relay |

(Line numbers to be re-verified before each commit; main moves.)

The originator-path pieces in `operations/get.rs` that phase 3b left
in place (because `process_message` loop-back uses them for local
completion) remain. We're not rewriting phase 3b.

### 3.2 New relay driver — `operations/get/op_ctx_task.rs` (same file)

Add a second entry point + driver loop alongside `start_client_get`:

```rust
pub(crate) async fn start_relay_get(
    op_manager: Arc<OpManager>,
    incoming_tx: Transaction,                 // reused, not a new tx
    instance_id: ContractInstanceId,
    htl: u8,                                   // already-decremented HTL
    upstream_addr: SocketAddr,                 // who to bubble response to
    skip_list: VisitedPeers,                   // peers upstream already tried
    return_contract_code: bool,
    subscribe: bool,
) -> Result<(), OpError>
```

Driver structure mirrors `drive_client_get_inner` (get/op_ctx_task.rs:186-~410)
but with different terminal semantics:

- **No `HostResult`** (relay doesn't publish to a client).
- **Terminal Found**: cache locally, send `GetMsg::Response{Found}` upstream,
  send `ForwardingAck` if required, spawn subscription child-op only if
  this node was the original requester (it isn't, for relay — so never).
- **Terminal NotFound after exhaustion**: send `GetMsg::Response{NotFound}`
  upstream.
- **Local cache hit at relay entry**: answer immediately without spawning
  a downstream attempt (preserving today's relay behavior in get.rs:1366-1428).

Reuse infrastructure:
- `RetryDriver` trait + `drive_retry_loop` from `operations/op_ctx.rs`.
- `advance_to_next_peer` pattern from `put/op_ctx_task.rs:412` — copy/adapt
  (peer-type differs: relay has `upstream_addr`, client doesn't).

### 3.3 Dispatch wiring (`node.rs:1073-1134`)

Currently `NetMessageV1::Get(op)` routes any non-terminal message to
`handle_op_request::<get::GetOp, _>`. Change:

```
NetMessageV1::Get(op) =>
  if terminal-for-active-client-driver:
      try_forward_task_per_tx_reply (existing phase-3b bypass)
  else if GetMsg::Request && source != self:
      start_relay_get(...)                    // NEW
      return Ok(None);
  else:
      handle_op_request::<get::GetOp, _>(...) // fallback for
                                              // originator loop-back,
                                              // GC-spawned retries,
                                              // Response/Ack to legacy
                                              // relay ops still in
                                              // OpManager during
                                              // rollout
```

**Rollout:** no feature flag. Matches phases 3b/4 which shipped the
behavior change unconditionally. Rollback = revert the PR.

### 3.4 Terminal-reply bypass (originator side)

When the relay driver sends `GetMsg::Response{Found|NotFound}` back to
the upstream addr, that upstream peer's `node.rs` sees the reply. If
the upstream is the **originator** running a phase-3b client driver,
the existing bypass at `node.rs:1082-1091` catches it — no change
needed. If the upstream is **another relay node still running under
the new driver**, the reply arrives as a non-terminal message for
that node's relay driver — the per-tx reply channel established by
`start_relay_get`'s `send_and_await` call must receive it.

This is the same wiring `OpCtx::send_and_await` already implements for
phases 2b/3a/3b/4. Reuse without modification.

### 3.5 Auto-subscribe handoff (`operations.rs:637-649`)

`auto_subscribe_on_get_response()` is called from the legacy
`process_message` Response{Found} branch (get.rs:2306-2320) and is
gated on `is_original_requester` (get.rs:2258, 2307). Relay nodes
never satisfy that gate, so the relay driver does **not** need to
call this helper. The phase-3b client driver (`drive_client_get_inner`)
already calls it on terminal Found. Leave as-is.

### 3.6 SubOperationTracker (phase 3c)

Phase 3c decoupled drivers from `SubOperationTracker` for SUBSCRIBE,
PUT, GET. The relay driver inherits that decoupling: it will register
no sub-operations (the "subscribe-on-get" sub-op is an originator
concern, not a relay concern).

## 4. Tricky cases & how they're handled

### 4.1 NotFound retry to next hop

Legacy: `GetMsg::Response{NotFound}` at relay mutates `GetOp` to pop
next candidate from `AwaitingResponseData` and re-forwards.

Driver: same semantics, but candidate list lives in driver locals.
`advance_to_next_peer` (adapt from PUT) returns either (a) next peer
to forward to, or (b) `Exhausted` → send NotFound to upstream_addr
and exit.

### 4.2 ForwardingAck timing

Legacy sends ForwardingAck at `get.rs:1628-1652` **before** forwarding
the Request to the downstream peer. This is a fire-and-forget signal
to upstream that "I'm taking responsibility for this hop, don't retry
me." The driver must preserve this send-before-forward ordering (the
upstream's retry timer uses ForwardingAck as the signal to extend).

In the driver: after selecting the next peer but before calling
`send_and_await`, synchronously send ForwardingAck to `upstream_addr`
via the conn_manager. No await on ack — it's fire-and-forget.

### 4.3 Local cache hit at relay entry

Legacy: `get.rs:1366-1428` — if the relay has the contract state
locally, it constructs a `GetMsg::Response{Found}` and sends it
upstream without forwarding. HTL is irrelevant in this case.

Driver: same check at driver entry, before the retry loop. If local
store has the contract, short-circuit to response assembly + send +
return.

### 4.4 HTL = 0 handling

Legacy: `get.rs:1291-1325` — HTL exhausted → NotFound to upstream.

Driver: if `htl == 0`, short-circuit to NotFound + send + return.
(HTL is passed in already decremented by `OpCtx::send_and_await`'s
wire-layer convention — double-check during implementation.)

### 4.5 Originator loop-back

Phase-3b client driver calls `send_and_await` with a `GetMsg::Request`
targeting itself for the local-completion cache-hit check
(`get/op_ctx_task.rs:18-26`). `process_message` today echoes the
Request as the terminal reply. Under this port, that code path is
preserved because the dispatch wiring (§3.3) falls through to the
legacy `handle_op_request` when `source == self`. No change needed.

### 4.6 Streaming payloads

Phase-3b left streamed-payload assembly and caching on the legacy
path (`operations/get/op_ctx_task.rs:38-42`). Relay-side streaming
assembly (where a relay forwards `ResponseStreaming` chunks upstream)
is **out of scope for this port** — it stays on the legacy
`process_message` path. Dispatch wiring (§3.3) routes
`GetMsg::ResponseStreaming` and its Ack to `handle_op_request` as
before. This matches phase-3b's scope boundary and keeps this PR's
LOC controllable.

Follow-up issue after this PR: migrate streaming-chunk forwarding.

### 4.7 GC-spawned retries & `start_targeted_op`

Per `get/op_ctx_task.rs:10-13`, GC-spawned retries and
`start_targeted_op()` (UPDATE-triggered auto-fetch) stay on the legacy
re-entry loop. This port does not change that. They will continue to
produce `GetOp` entries in `OpManager.ops.get` and flow through
`handle_op_request`. The dispatch wiring (§3.3) falls through to the
legacy path whenever the incoming Request's transaction already has a
`GetOp` registered, so GC-spawned retries keep working.

## 5. Commit split (one PR, three commits)

1. **`refactor(ops): add relay GET task-per-tx driver + helpers (#3883 scaffold)`**
   Extract HTL-check / local-cache-check / peer-selection subroutines
   from `GetMsg::Request` arm into free functions, AND add
   `start_relay_get` + `drive_relay_get_inner` to
   `operations/get/op_ctx_task.rs`. Driver reuses the extracted
   helpers. Not yet wired to dispatch. Unit tests for driver using
   the same `OpCtx` test harness phase 3b used. `cargo test -p freenet`
   green — driver is dead code in integration tests (only unit tests
   exercise it).

2. **`refactor(ops): route relay GET requests through task-per-tx driver (#1454 phase 5)`**
   Switch dispatch in `node.rs:1073-1134` to call `start_relay_get`
   for relay-hop `GetMsg::Request` when `source_addr.is_some()`.
   Preserve `source_addr.is_none()` loop-back → legacy `handle_op_request`
   path (phase-3b client driver contract). Legacy `GetMsg::Request`
   relay arm in `get.rs` becomes unreachable for true relay. Run
   relay sim tests:
   - `test_get_routing_coverage_low_htl`
   - `test_get_retry_with_alternatives_sparse_topology`
   - `test_get_reliability_diagnostic`
   - `test_get_reliability_with_latency`
   - `test_get_reliability_with_churn`

3. **`refactor(ops): remove dead relay branches from get.rs (#3883 cleanup)`**
   Delete `GetState::ReceivedRequest`-based code paths and relay arms
   of `GetMsg::Request/Response{Found}/Response{NotFound}/ForwardingAck`
   that are unreachable when `source_addr.is_some()`. Keep
   loop-back-reachable code (local-cache-check branch) intact. Verify
   nothing else references removed code (grep + compile).

Each commit individually: `cargo fmt && cargo clippy -- -D warnings &&
cargo test -p freenet` green. Commit 1 is strictly non-behavioral
(new module + helper extraction). Commit 2 is the behavior flip.
Commit 3 is unreachable-code removal.

## 6. Validation gates

### 6.1 Per-commit (all four)

```
cargo fmt
cargo clippy -- -D warnings
cargo test -p freenet
```

### 6.2 Commit 3 (behavior flip) — additional

Integration / sim tests that exercise relay forwarding explicitly:
- `tests/simulation_integration.rs::test_get_routing_coverage_low_htl`
- `tests/simulation_integration.rs::test_get_retry_with_alternatives_sparse_topology`
- `tests/simulation_integration.rs::test_get_reliability_diagnostic`
- `tests/simulation_integration.rs::test_get_reliability_with_latency`
- `tests/simulation_integration.rs::test_get_reliability_with_churn`

### 6.3 macOS 127.x.x.x binding

Integration tests that fail on macOS with "Can't assign requested
address" must be validated via `/linux-test` (Docker). This is a
known infrastructure blocker from phases 3b/4, not a code issue.

## 7. Out of scope (defer to follow-ups)

- Relay-side streaming-chunk forwarding (§4.6).
- GC-spawned retry migration (§4.7).
- `start_targeted_op` (UPDATE-triggered fetch) migration.
- Removal of `GetState` / `GetOp` entirely (only possible once all
  the above plus originator loop-back are migrated).

## 8. Risks

- **Thin-driver assumption (§2):** if the phase-3b client driver's
  loop-back is more tangled with the legacy relay `GetMsg::Request`
  arm than documented, commit 4's dead-code removal could break the
  originator's local-completion path. Mitigation: commit 3's dispatch
  change is gated so legacy path still runs for `source == self`;
  commit 4 only removes relay-side code, preserving originator arms.

- **ForwardingAck timing (§4.2):** if send-before-forward ordering is
  violated, upstream peers will prematurely time out and retry,
  causing duplicate GETs. Tests `test_get_reliability_with_latency`
  and `test_get_retry_with_alternatives_sparse_topology` will catch
  this if it regresses.

- **NotFound-retry peer selection (§4.1):** the `VisitedPeers` /
  `skip_list` semantics must match between relay driver and phase-3b
  client driver (they share the trait but populate differently). A
  divergence causes a peer to be re-tried or a valid peer to be
  skipped. Unit tests in commit 2 must cover skip-list propagation
  across retry attempts.
