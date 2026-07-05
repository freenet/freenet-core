# Event dispatch: targeted vs broadcast fan-out

Several node paths react to a contract's state changing by pushing that
state to other peers. There are two fundamentally different dispatch
shapes, and choosing the wrong one is both a **correctness** and a
**performance** bug. This document is the reference for that distinction;
the review checklist that enforces it lives in
[`.claude/rules/operations.md`](../../.claude/rules/operations.md) under
"Event emission review".

## The two `NodeEvent` shapes

| Event | Recipients | Cost | Used for |
|-------|-----------|------|----------|
| `BroadcastStateChange { key, new_state, .. }` | **All** subscribers/hosts of `key` | O(fan-out) per emit | Propagating a real PUT/UPDATE outward so every subscriber converges |
| `SyncStateToPeer { key, new_state, target }` | **Exactly one** peer (`target`) | O(1) per emit | Healing a single peer that is known to be behind (summary mismatch, proximity-cache overlap) |

- `BroadcastStateChange` is handled in
  `node/network_bridge/p2p_protoc/broadcast.rs` via
  `get_broadcast_targets_update`, which resolves **every** subscriber of
  the contract and sends to each. On a popular contract that is ~28–80
  peers per emit.
- `SyncStateToPeer` is handled in the same module but sends to the single
  `target` socket address only.

## When each is correct

Use `BroadcastStateChange` when the state change is **new information the
whole subscriber set must learn** — the normal PUT/UPDATE propagation
path. One update, fanned out once, is the intended O(fan-out).

Use `SyncStateToPeer` when you have **already identified the specific
peer(s) that are behind** and only they need catching up:

- **Summary-mismatch correction** (interest sync). A peer reports a stale
  summary; only that peer is behind, so only that peer is healed. See
  `node.rs::handle_interest_sync_message` (`Summaries` arm) and the
  `stale_peer_sync_event` builder.
- **Proximity-cache overlap.** When a neighbor is discovered to be
  missing a contract we host, the heal is targeted at that neighbor.

## Why mixing them is a bug

The failure mode is emitting `BroadcastStateChange` from a path that has
**already narrowed the problem to a single peer**. Instead of one targeted
send you get a full fan-out, and if N peers each independently detect a
mismatch within the same heartbeat cycle you get **O(N × fan-out)**
transmissions where O(N) would suffice.

### Concrete incident: #3791

For six weeks the summary-mismatch handler emitted
`BroadcastStateChange` instead of `SyncStateToPeer`. Production impact
(gateway, 2026-04-07, one hour):

- 4,820 `UPDATE_PROPAGATION` events; **88% triggered by summary
  mismatches**.
- Average fan-out **28.7 peers** per broadcast; peak 144 broadcasts/minute
  for a single contract.
- ~138,000 peer-transmissions/hour from one gateway.
- Observed **19:1** and user-reported **163:1** upload/download ratios
  (1.1 GB uploaded in 45 minutes).

A misleading comment — *"Emitting BroadcastStateChange triggers the
existing broadcast path which computes deltas and sends state only to
peers with stale summaries"* — described the *intended* effect, not the
*actual* all-subscriber fan-out, so the wrong event survived review.

## Guardrails in the tree

- **Builder isolates the decision.** `node.rs::stale_peer_sync_event`
  builds the targeted `SyncStateToPeer`. The `Summaries`-arm emit site
  routes through it rather than constructing a `NodeEvent` inline, so the
  variant-and-target choice is unit-testable without a full `OpManager`.
- **Regression tests** (`node.rs`, module `fanout_regression_guard`):
  - `stale_peer_sync_event_is_targeted_not_broadcast` — the heal event is
    a targeted `SyncStateToPeer`, never a `BroadcastStateChange`.
  - `stale_peer_sync_event_targets_the_reporting_peer` — the sole target
    is the reporting peer.
  - `emit_site_uses_targeted_builder` — source-scrape pin: the production
    emit site still routes through the builder and never names
    `BroadcastStateChange`. This closes the #3791 test-gap where a
    data-layer-only test stayed green after the dispatch was reverted.
- **Review checklist.** Every `NodeEvent`-emitting site must be reviewed
  for (1) which peers receive the event and (2) whether the comment names
  the actual dispatch path. See `.claude/rules/operations.md`.

## References

- Bug: #3791 · Fix PR: #3793 · Process/test tracking: #3796
- Emit budget cap on stale-sync fan-out: #3798 (see
  `MAX_STALE_SYNCS_PER_SUMMARIES` in `node.rs`)
