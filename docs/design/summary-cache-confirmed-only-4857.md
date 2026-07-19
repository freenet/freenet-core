# RFC: Confirmed-only neighbor-summary caching (issue #4857)

**Status:** Proposal for review. No code beyond a repro test
(`queue_full_drop_at_receiver_poisons_neighbor_summary_cache_4857` in
`crates/core/src/node/network_bridge/broadcast_queue.rs`).

**Review tier:** Full. Touches broadcast/update state machines, wire-adjacent
summary exchange, and three prior-incident surfaces (#4251, #4442, #4145/#4466).
Multi-model + Codex before merge.

---

## TL;DR

- **Bug:** a sender caches "my neighbor now has my current state" the moment it
  *sends* a delta, not when the neighbor *confirms* it. On a silent
  queue-full drop the cache is now a lie, so every later delta is diffed against
  that false baseline and omits the dropped change. A rarely-changing field
  (River `member_info`, a ban, a config) is stranded until the ~5-min
  InterestSync heartbeat heals it.
- **Root cause is broader than the drop.** Caching *our* summary as the
  neighbor's summary assumes "after you apply my delta, you equal me." That is
  also false under a concurrent merge from another peer. Issue #2764 already
  wrote this down ("we can't predict their resulting summary").
- **Fix:** the sender's cached summary of a neighbor must only ever be set from a
  summary the neighbor *actually reported*. The `Summaries` handler
  (`node.rs:2703`) already does exactly this; the work is to (a) stop the
  sender-side optimistic write, (b) make receivers report their real summary
  promptly instead of only every 5 min, and (c) add an in-flight/confirmed split
  so the report round-trip doesn't cause redundant resends.
- **Must not** re-introduce the #4442 full-state storm, and **must not** push a
  full-state resync onto a saturated queue (#4251).
- **The genuinely-permanent bug is a separate finding** (over-disk-budget peers,
  secondary-2 below), not this poisoning path. It should get its own issue and
  is arguably higher priority.

---

## Background: how replica sync works today

A contract's state lives on many nodes. When one node's copy changes it tells
its neighbors, sending a **delta** (the difference) rather than full state. To
compute a delta it needs to know what the neighbor already has, so each node
keeps, per neighbor, a cached **summary** of that neighbor's state (a
contract-defined fingerprint). Broadcast then computes
`delta = diff(cached_summary_of_neighbor, my_current_summary)` and sends only
that (`broadcast_queue.rs::broadcast_to_single_peer`, ~L556-646). If the cache
has no summary for the neighbor it sends full state instead.

Summaries are **memoized** (`executor.rs:1251`,
`summary_cache: ByteBoundedLruCache<ContractKey, (u64, StateSummary)>`,
"memoize the (expensive) WASM `summarize_state`"), and the node that just
committed an update already computed its current summary to build the broadcast
diff — so a summary is cheap to produce and usually already in hand.

## The bug (#4857)

1. **The sender advances its cache on send, not on ack.** After sending a delta
   the sender sets its cached summary of the neighbor to *its own* current
   summary — `record_delivery_to_interest` → `update_peer_summary(our_summary)`
   (`broadcast_queue.rs:543-544`). This fires on a purely sender-side signal: for
   the delta path, `bridge.send(...)` returning `Ok`, which only means the
   message was *enqueued onto the event-loop channel* (`:819`, `:823`); for the
   streaming/full-state path, a sender-side stream completion
   (`BroadcastDeliveryOutcome::Delivered`). Neither is a receiver acknowledgement
   — the code comment says so explicitly.
2. **The receiver can silently drop the delta.** Under load the receiver's
   contract queue fills (`ContractQueueFull`) and the update is dropped with
   **nothing sent back** to the sender (`update/op_ctx_task.rs`
   `drive_relay_broadcast_to`, ~L1183-1246). This silence is deliberate
   anti-amplification (#4251/#4253), pinned by
   `broadcast_to_suppresses_amplification_on_queue_full`: a full-state resync onto
   an already-saturated queue would make the overload worse.
3. **The cache now lies, and stays lying.** The sender believes the neighbor has
   `S1`; the neighbor actually still has `S0`. Every later delta is
   `diff(S1, S_new)` and omits the `S0→S1` change forever. A frequently-changing
   field self-masks (the next update re-ships it); a **rarely-changing** field
   stays missing until the ~5-min heartbeat.

**Trigger reality:** the transport is reliable (ACK/retransmit), so this is *not*
packet loss. The only ACK-invisible drops are receiver contract-queue saturation
and connection churn. A rarely-changing contract can't fill its own 100-deep
queue, so in practice this bites as *collateral* on a node overloaded by other
hot contracts (a loaded gateway, a #4534-style River hot room).

## Root cause

The cache write `update_peer_summary(our_summary)` encodes the assumption *"once
you apply my delta you are identical to me."* That is wrong in two ways:

- **Drop (permanent loss):** the neighbor never applied the delta, so it is
  missing the field entirely. This is the #4857 headline.
- **Concurrent merge (softer):** the neighbor also applied another peer's delta,
  so its post-merge summary is not ours. Issue #2764 documented exactly this in
  the still-present comment on `test_full_state_send_no_incorrect_caching`
  (`simulation_integration.rs`, ~L1976-2020): *"the recipient's state depends on
  CRDT merge with their existing state — we can't predict their resulting
  summary … potential divergence in race conditions."* Here the neighbor still
  holds a valid merged value, so this usually degrades to inefficiency /
  race-window divergence rather than clean loss — but it means the optimistic
  write is wrong-in-principle on success too, not only on drop.

The only node that knows a neighbor's true post-merge summary is the neighbor.

## History this fix must respect

This exact bug class has been round-tripped before; the constraints below are why
the naive fix is wrong.

- **#2764 / #2763** — recognized the wrong-cached-summary divergence; fixed it by
  caching *only* after a delta send (`if sent_delta`).
- **#4145 / #4442** — that `if sent_delta` gate caused a **full-state broadcast
  storm**: a summary-less new subscriber never got a cached summary, so it was
  trapped receiving full state forever. Fixed by caching on *any* delivered
  broadcast (delta or full state) — which knowingly re-accepted the
  wrong-cached-summary risk, relying on two mitigations: **(a)** the periodic
  InterestSync summary exchange, and **(b)** the delta-apply-failure →
  `ResyncRequest` path that clears the sender's cached summary. #4857 is the
  corner where *neither* mitigation fires: queue-full is not an apply-failure (no
  ResyncRequest), and a rarely-changing field means the 5-min exchange is the only
  heal.
- **#4251 / #4253** — the silent queue-full drop. **Any fix must not push a
  full-state resync onto a saturated queue.** A rate-limited *cache invalidation*
  or *summary report* is fine; a full-state resend is not.
- **#4145 / #4466** — channel-safety. Anything added to the broadcast/receive path
  must be best-effort `try_send` (droppable), never a blocking `.send().await` on
  an event-loop-reachable sender.
- **#4440** — the `summarize_contract_state` storm (~70-80/sec) tamed by
  rate-limiting + the `summary_cache`. A fix must reuse the memoized / already-
  computed summary, never trigger a fresh WASM summarize just to report.

## Existing machinery we build on

The corrected behavior mostly already exists on the *receiving-a-summary* side:

- **`node.rs:2644-2773` — the `Summaries` handler.** For each contract it sets
  the cached peer summary to the peer's *reported* summary
  (`update_peer_summary(their_summary)`, L2703) **unconditionally**, then if
  `our != theirs` it emits a **targeted** `SyncStateToPeer` (full state) to that
  one peer, capped at `MAX_STALE_SYNCS_PER_SUMMARIES = 32` per message with random
  rotation (#3798). The comment (L2660-2662) already declares mutual concurrent
  divergence safe.
- **Wire types already exist:** `InterestMessage::Summaries { entries }` and
  `SummaryEntry { hash, summary_bytes: Option<..> }` (`message.rs`) already carry
  summaries between peers on the 5-min heartbeat. Reusing them means **no
  wire-format change and no version gate**.
- **`summary_cache` memoization** (`executor.rs:1251`) makes a report cheap.

So the fix is largely: *stop writing the optimistic value, and make the
receiver report its real summary more promptly* — routing through the cache-update
that `node.rs:2703` already performs.

## Proposed design

### 1. Remove the sender-side optimistic write

`record_delivery_to_interest` (`broadcast_queue.rs:543-544`) no longer calls
`update_peer_summary(our_summary)`. Delivery still refreshes the interest TTL;
it just stops asserting the neighbor's *content*. This alone kills the poisoning
and the concurrency mis-cache. **But on its own it re-opens the #4442 storm**
(new subscribers never get a cached summary), so it cannot ship without step 2.

### 2. Confirmed source = the receiver reports its real summary

The cache is populated only from receiver-reported summaries, via the existing
`Summaries` path (`node.rs:2703`). Receivers report promptly, two tiers:

- **On queue-full drop (the direct #4857 fix):** the receiver already knows it
  dropped an update it was interested in. It sends a **rate-limited, targeted
  `Summaries`** to the originating sender carrying its *current* (still-`S0`)
  summary. The sender un-poisons its cache to `S0`. This is a cheap summary, not
  a full-state resync, so it stays within #4251. Rate-limit per
  (contract, neighbor) via `TrackedBackoff` so a saturated contract can't
  amplify.
- **On apply (closes the concurrency gap + replaces the storm mitigation):** when
  a receiver applies an update that changes its summary, it advertises its new
  summary. Freenet already advertises post-apply summaries to *interested peers* —
  the change is to **include the originating sender**, which is currently excluded
  as #2764 "echo-back prevention." *(Open question: understand exactly what echo
  #2764 was preventing before un-excluding — see Open Questions.)*

Reusing the memoized/just-computed summary keeps this off the #4440 storm path.

### 3. In-flight / confirmed split (prevents redundant resends)

Separate the two things the current code conflates:

- **Confirmed summary** — advances only on a receiver report (step 2). This is the
  delta baseline; it never lies, so no poisoning.
- **In-flight marker** per (contract, neighbor) — "I've sent up to `S1`, not yet
  confirmed." Before broadcasting, if an identical delta is already in-flight and
  not timed out, **skip the resend**. On a confirming report, promote in-flight →
  confirmed. On timeout or a drop-report, clear in-flight so a resend is allowed.

This gives correctness from the confirmed baseline *and* suppresses the duplicate
sends during the report round-trip that plain "only-confirm" caching would cause.
The InterestManager already has a delta-memoization cache (`ring/interest.rs:284`)
to build on.

### 4. #4251-safe healing

On a drop-triggered report, the sender **un-poisons the cache but does not emit an
immediate full-state `SyncStateToPeer`** onto the (still-saturated) queue. The
correction rides the *next organic broadcast* as a small delta
(`diff(S0, S_current)` includes the missing field). For an active contract that is
seconds away; the heartbeat covers a genuinely idle one. Concretely: gate the
`is_stale → SyncStateToPeer` emit in the `Summaries` handler so a drop-origin
report updates the cache without triggering a full-state push (the heartbeat's
capped full-state heal is unchanged).

### 5. Keep the heartbeat as the backstop

Reports can be dropped too (queue full, churn), so no report scheme is complete on
its own. The ~5-min InterestSync stays the guaranteed backstop. Its known holes:
the 32-cap rotation is fine for eventual consistency, but the **over-disk-budget
rejection defeats it entirely** — that is secondary-2, tracked separately.

## Why this doesn't re-open the #4442 storm

The storm came from new subscribers *never* getting a cached summary. Here the
cache still gets populated — from the receiver's own report (step 2) instead of
the sender's guess — so the sender still collapses to small deltas after the first
report. The difference is the cached value is now *true* (the neighbor's real
post-merge summary) rather than an optimistic assumption.

## Secondary findings — split into their own issues

- **Secondary-1 (perf, own issue):** `is_stale` is a raw byte-compare of
  contract-produced summaries (`node.rs:2698-2701`). If a contract's
  `summarize_state` is order-nondeterministic (e.g. HashMap iteration), two
  identical states compare unequal, so heals fire spuriously. This *over*-fires
  the heal (wasted CPU + summarize volume, consistent with the production
  journal), it does **not** cause missed heals — so it is a performance drain, not
  part of the data-loss mechanism. Fix: canonicalize/compare semantically, and/or
  have contracts use deterministic maps in their Summary type.
- **Secondary-2 (correctness, own issue, arguably higher priority):** an
  over-disk-budget peer silently rejects growth-only updates
  (`executor_impl.rs:1696-1708` → `admit_state_update`), and the follow-up
  `ResyncResponse` (full state) is even larger and also rejected
  (`node.rs:3029-3061`) — a genuinely permanent divergence loop independent of the
  poisoning story. No heartbeat rescues it because the rescue is refused.

## Testing plan

- **Committed:** `queue_full_drop_at_receiver_poisons_neighbor_summary_cache_4857`
  pins the poisoning deterministically at the `InterestManager` level (passes on
  buggy main; would fail once step 1 lands). It is a *characterization* test of
  the current bug — when the fix lands it must be **inverted** to assert the fixed
  behavior, not merged as-is.
- **Needed for the fix — per-field CRDT behavior.** The mock runtimes carry full
  state as the "delta," so per-field omission *cannot be expressed* with them;
  the codebase already documents this (`simulation_integration.rs:2011-2020`). A
  faithful end-to-end test needs either a purpose-built per-field-diff mock
  contract or a `#[freenet_test]` with a small real CRDT contract. Assertions:
  the changed field converges at the receiver **without** waiting for the 5-min
  heartbeat; **no** redundant resends of an in-flight delta; **no** full-state
  send onto the saturated queue. Use `StateVerifier::stale_peers()` /
  `divergences()` for the network-level check.

## Rollout / wire-compat

Reusing `InterestMessage::Summaries` means **no wire-format change**; old peers
already handle `Summaries`, so a mixed-version network is safe (hosting-invariants
H). The behavior change (confirmed-only caching + prompt reports) is
backward-compatible: an old sender simply keeps its old caching, and a new
receiver's extra report is a message the old sender already knows how to process.

## Open questions / decisions for review

1. **Report-on-drop only (cheap, narrow) vs report-on-every-apply (complete,
   more traffic)?** Recommendation: do both tiers — drop-report for the direct
   fix, apply-report (piggybacked on the existing advertisement) for the
   concurrency gap — with the heartbeat covering reports that are themselves
   dropped. Memoization makes the apply-report cheap; the residual cost is message
   count on hot contracts, tunable by piggybacking rather than a separate message.
2. **Does un-excluding the sender from the apply-advertisement re-open #2764's
   echo problem?** Must understand #2764's exact echo mechanism first; the
   exclusion was itself a deliberate fix.
3. **Delta vs full-state for any fast heal.** Prefer letting the next organic
   delta carry it (step 4); only the heartbeat sends full state, and stays capped.
4. **Interaction with secondary-1:** nondeterministic summaries make `is_stale`
   over-fire, so more reports classify as "stale" and add heal traffic. Worth
   fixing secondary-1 alongside to keep the report path quiet.
5. **Sequencing:** step 1 (remove optimistic write) and step 2 (receiver reports)
   are coupled and must ship together, or the #4442 storm returns. Steps 3–4 are
   the efficiency/amplification guards. Consider landing secondary-2 first — it is
   a smaller, self-contained, genuinely-permanent fix.

---

*Investigated + drafted 2026-07-19. [AI-assisted - Claude]*
