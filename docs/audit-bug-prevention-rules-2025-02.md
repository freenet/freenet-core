# Audit Results — Bug-Prevention Rules (Feb 2025)

Companion to issue #3271. Audit performed against commit `0b889454`.

## Rule 1: `biased;` Select Audit

**11 sites found** across 9 files.

| Site | File | Status | Issue |
|------|------|--------|-------|
| UDP listen monitor | `node/network_bridge/p2p_protoc.rs` | OK | Fatal condition priority — well documented |
| Message loss test | `node/network_bridge/p2p_protoc.rs` | OK | Regression test — intentional |
| WebSocket handler | `client_events/websocket.rs` | OK | Explicit anti-pattern comment — no `biased;` |
| Response priority | `client_events/combinator.rs` | OK | Response priority — well documented |
| VirtualTime timeout | `simulation/time.rs` | Needs docs | Missing WHY documentation |
| Infra task monitor | `node/p2p_impl.rs` | Needs docs | Missing DST determinism explanation |
| **Op request mediator** | **`contract/executor.rs`** | **CRITICAL** | **Missing per-iteration caps on high-throughput arms** |
| Cancel-safe permit | `transport/peer_connection/piped_stream.rs` | OK | Well documented cancellation safety |
| Event loop test | `node/network_bridge.rs` | Needs docs | Missing test purpose documentation |

**Action needed:** `executor.rs` mediator needs per-iteration caps on `op_request_receiver` and `from_event_loop_rx` arms.

## Rule 2: `GlobalExecutor::spawn` & `try_send` Audit

### Spawn Sites (15 production)

| Risk | Count | Details |
|------|-------|---------|
| Critical | 1 | `tracing/telemetry.rs:170` — TelemetryWorker dropped without monitoring |
| Medium | 3 | WebSocket event server, test context |
| OK | 11 | Handle stored and monitored |

### try_send Sites (21 total)

| Risk | File:Line | Issue |
|------|-----------|-------|
| **HIGH** | `node/op_state_manager.rs:566,1171` | Operation notifications silently dropped on channel full |
| Medium-Low | `client_events/combinator.rs:201` | Intentional backpressure (documented, prevents deadlock) |
| Low | Transport packet drops | Expected for UDP |
| OK | Telemetry sites (6) | Fire-and-forget acceptable |

### Catch-all `_ =>` in Metrics (node/mod.rs)

| Risk | Line | Issue |
|------|------|-------|
| **HIGH** | 2196 | `get_op_type_and_is_irrelevant()` — non-exhaustive match on OpOutcome |
| Medium | 1802,1816,1829,1842,1855 | Transaction abort handlers silently swallow errors |
| Medium | 2034 | DelegateRequest type logging — future variants map to "Unknown" |

## Rule 3: Cleanup/Prune Audit

**All cleanup paths are properly implemented.** Key findings:

- `prune_connection` cleans ALL 6 related maps (location_for_peer, connections_by_location, pending_reservations, ready_peers, connected_since, peer_health)
- Orphaned operations are extracted and retried via `handle_orphaned_transactions()`
- All `retain()` calls have TTL or age bounds
- Lock ordering is documented and correct
- `cleanup_stale_reservations` runs two-phase sweep (Phase 1: TTL on pending, Phase 2: orphan detection)

**No critical issues found.** The cleanup architecture from the befb0bd/0b88945 fixes is solid.

## Rule 4: Sleep/Backoff/Jitter Audit

### Critical Issues

| Component | Location | Issue |
|-----------|----------|-------|
| `op_retry_backoff()` | `node/mod.rs:770` (5 call sites) | No jitter on 5ms–1s exponential backoff |
| `BROADCAST_RETRY_BASE_DELAY` | `p2p_protoc.rs:3773` | No jitter on 1s/2s/3s linear backoff |
| ReadyState send | `p2p_protoc.rs:3759` | Fire-and-forget UDP, relies on 30s periodic refresh |

### Compliant Sites

| Component | Jitter | Cancellable |
|-----------|--------|-------------|
| Gateway backoff | Managed (TrackedBackoff) | Yes (Notify signal) |
| Connection pool backoff | ±2s jitter | No (short durations OK) |
| Subscription jitter | 0–15s spread | Yes (spawned) |
| Telemetry backoff | ±25% jitter | Yes (spawned) |

## Rule 5: Unused Dependencies (cargo machete)

**Core crates with potentially unused dependencies:**

| Crate | Dependencies |
|-------|-------------|
| **freenet** (core) | opentelemetry-jaeger, opentelemetry-otlp, opentelemetry_sdk, pin-project, tracing-opentelemetry |
| **fdev** | prettytable-rs, reqwest, tracing-subscriber |
| **freenet-macros** | darling |

Note: cargo-machete may report false positives for feature-gated or proc-macro dependencies. Manual verification recommended before removal.

## Summary

| Rule | Audit Result |
|------|-------------|
| 1. `select!` fairness | 1 critical (executor.rs), 3 need docs |
| 2. Task monitoring | 1 critical spawn, 2 critical try_send, 1 non-exhaustive match |
| 3. State consistency | **All clear** — cleanup architecture is solid |
| 4. Backoff + jitter | 3 critical (op_retry, broadcast retry, ReadyState) |
| 5. Deployment | Several potentially unused deps to verify |
