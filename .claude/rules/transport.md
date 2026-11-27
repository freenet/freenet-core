---
paths:
  - "crates/core/src/transport/**"
---

# Transport Module Rules

This file holds the rules, invariants and prohibitions, each with its reason. The mechanism detail behind each section is in `docs/architecture/transport/rules-reference.md` (pointers below). When a change alters a mechanism or a rule, update BOTH files in the same change, and fix whichever one is wrong when they disagree.

## Cryptographic Rules

### WHEN handling encryption

```
Handshake phase:
  → USE: X25519 key exchange + ChaCha20Poly1305
  → Keys: Ephemeral X25519 keypair per connection

Data phase:
  → USE: AES-128-GCM for all data packets
  → Nonces: Counter-based (4-byte random prefix + 8-byte atomic counter)
  → NEVER reuse (key, nonce) pair
```

### WHEN generating keys

```
Static node keys:
  → Generated once per node lifetime
  → Stored in configuration directory
  → 32-byte X25519 keypair

Session keys:
  → Generated per connection during handshake
  → 16-byte AES-128 keys
  → Independent keys for each direction
  → Cleared on connection close
```

### WHEN handling decryption failures

```
→ Drop packet silently (may be malicious)
→ Log at debug level
→ Do NOT propagate error to caller
→ Do NOT disconnect (could be transient)
```

## Congestion Control Rules

### WHEN using LEDBAT++

```
Key constants (do not change without understanding impact):
  TARGET: 60ms (queuing delay threshold)
  SLOWDOWN_REDUCTION_FACTOR: 4 (cwnd drops to 25%)
  SLOWDOWN_INTERVAL: 9x slowdown duration

MUST:
  - Exit slow start at 75% of TARGET (45ms)
  - Cap multiplicative decrease at -W/2 per RTT
  - Implement periodic slowdown for fairness
```

### WHEN configuring bandwidth

```
Per-connection mode (default):
  → Each connection is paced independently; no node-wide cap
  → Default: 10 Mbps (1,250,000 bytes/sec); see DEFAULT_RATE_BYTES_PER_SEC in fixed_rate/controller.rs
  → bandwidth-limit caps each connection's rate (unset by default)

Global pool mode:
  → total-bandwidth-limit enables this
  → Formula: rate = max(total/connections, min_per_connection)
  → min_per_connection prevents starvation; clamped to the total
  → total-bandwidth-limit is ALSO a node-wide egress cap (egress_cap.rs)
```

### Node-wide egress cap (#5940 decision 3)

- `DemuxSocket` owns one `EgressCap` only when `total-bandwidth-limit` is set; no setting, no cap. Only stream fragment tasks WAIT for it (`outbound_stream.rs::capped_attempt`); all else is charged once sent, into debt.
- NEVER add a wait to the listener loop, handshake tasks, keepalive, `outbound_short_message`, or anything in `PeerConnection::recv`: recv is a cancellable `select!` that changes state before it sends.
- NEVER put a reservation on the NODE cap (leaked capacity); nothing is reserved there while a sender waits, it is charged only at send. Random-race waiters starve (#6112), so waiters queue.
- Per-connection admission is IN ORDER: pay the bucket ONCE via `TokenBucket::reserve`; a reservation that would reach the stream deadline is not made (`reserve_within`). A send error after the take does NOT refund.
- Node-cap admission is IN ORDER, one queue shared by streams and QUIC (`StreamPacing::wait_for_admission` over `EgressCap::poll_admit`); admit only when the bucket holds the waiter's datagram plus one per waiter ahead.
- cwnd check and `egress_cap::try_take` run back to back with NO await before the send; a held or failed-take stream requeues AT ITS PLACE (`held_place`).
- No take ever uses an admission it does not name (`AdmissionWaiter::named`; QUIC via `HANDED_ADMISSION`, which relies on freenet-quinn 0.11.13 call order: a quinn bump MUST re-check).
- NEVER evict a waiter; none is admitted with more than `MAX_ADMISSION_WAKERS` ahead (`a_waiter_past_the_count_bound_is_not_admitted_by_a_large_burst`).
- `StreamPacing` checks the gap BEFORE every take, after every sleep and every cwnd-wait poll; at `PACED_WAIT_DEADLINE` the stream fails stream-scoped, flight released (first fragment too).
- Unpaced debt is capped at `DEBT_LIMIT`. `poll_admit` is `Ready` exactly when `try_take` would succeed (`admits`) and takes nothing.
- Every take is at most one datagram within the burst (QUIC: one segment per transmit, debug-asserted); the node sends at most burst + rate x interval + bytes forgiven.
- A deadline death signals `StreamSendOutcome::EgressAbandoned`, not `Dropped`; every consumer treats anything but `Delivered` as not delivered; keep it that way when adding a variant (#6143).
- The QUIC endpoint MUST run on the cap's clock. Tests assert on per-cap `EgressCap::stats()`, never the global counters.

Mechanism detail: docs/architecture/transport/rules-reference.md § Node-wide egress cap (#5940 decision 3)

### Retransmit cap (legacy UDP, #6147)

- Resends and probes go through `EgressCap::admit_retransmit` with NO await and NO lock (atomics only; nothing under the `sent_tracker` lock takes a lock). Nothing retransmitted is forgiven; abandonment never asks the cap.
- A refused RTO resend leaves state UNCHANGED. Do NOT remove the deferral limit `RETX_DEFER_LIMIT_RTOS`: without it a packet is never abandoned and pins flight (#4345).
- Book a slot only on share refusals, NOT bucket refusals (`share_waiters_wake_once_per_slot_whatever_their_number`).

Mechanism detail: docs/architecture/transport/rules-reference.md § Retransmit cap (legacy UDP, #6147)

### WHEN modifying congestion control

```
BEFORE changing LEDBAT++ parameters:
  1. Understand RFC 6817 and draft-irtf-iccrg-ledbat-plus-plus
  2. Test at multiple RTT values (10ms, 50ms, 100ms, 200ms)
  3. Test inter-flow fairness (2+ concurrent connections)
  4. Document reasoning in commit message
```

### Flight-size release invariant (issue #4345)

- Flight size has THREE release paths: ACK, ABANDON (`release_flightsize`), STREAM ABORT (`release_aborted_stream_flightsize` via `SentPacketTracker::drop_stream`). A stream with no ACKs must not pin flight at cwnd.
- Each in-flight packet is released EXACTLY ONCE: each path REMOVES it from `pending_receipts`.
- `on_timeout()` MUST NOT change flight size. Do NOT reintroduce a decrement-on-RTO + re-add-on-resend pair (net-zero, never drains a never-ACKed packet).
- `get_resend` KEEPS the packet in `pending_receipts` so a concurrent `drop_stream` sees it.
- Resend re-registration MUST use `refresh_sent_packet` (update-only), NOT `report_sent_packet` (resurrects a released packet).
- Retransmissions MUST stay bounded; never make retransmit infinite.

Mechanism detail: docs/architecture/transport/rules-reference.md § Flight-size release invariant (issue #4345)

### RTO backoff: once per round, reset on ACK (#5795)

```
SentPacketTracker::rto_backoff doubles at most ONCE per round:
  → the first RTO expiry doubles it and sets backoff_round_end_nanos =
    now + (new) effective RTO;
  → other packets timing out before that are retransmitted WITHOUT
    doubling again (RFC 6298 has one timer per connection; a burst of
    losses is one expiry);
  → any ACK that resets rto_backoff to 1 also resets
    backoff_round_end_nanos to 0, so the next outage backs off at once.

DO NOT go back to doubling per lost packet: a 10-packet burst then hits
the 60 s cap immediately and the rest of the burst stalls for minutes.
That was masked until #5795 by the ack-only NoOp chatter, which reset
the backoff every ~200 ms. DO NOT reset rto_backoff without also
clearing the round end: a stale round end (up to 60 s ahead) turns the
next outage into a base-RTO retransmit burst that exhausts
MAX_PACKET_RETRANSMITS in seconds.
```

### Shadow per-peer RTT registry (issue #4074, Phases 1 + 1.5)

- Observation only; see the "NEVER read" rule under Phase 1.6. A new periodic emitter goes in `telemetry::PERIODIC_EVENT_TYPES` only if a fixed timer bounds it (#6001).
- Reference-ping is opt-in (`telemetry.reference-ping-enabled`, default `false`); dev machines and integration tests MUST leave it off (DNS traffic perturbs timing tests).

Mechanism detail: docs/architecture/transport/rules-reference.md § Shadow per-peer RTT registry (issue #4074, Phases 1 + 1.5)

### Transport-redesign baseline telemetry (`transport_snapshot`)

- Observation only (single relaxed atomics on the hot path). `CONN_RTT_REGISTRY` is read only by telemetry, NEVER by the data path.
- Declare each label set once in a `labelled_counters!` list (a new variant needs a snapshot field). The never-reset OTel twin must NOT observe the period counters.
- `observed_addr_*` counts only, never the address.
- `inbound_concurrency_*` may justify LOWERING `MAX_UNI_STREAMS`, NEVER raising it; keep its counts under ONE mutex.
- Phase D's gate reads the SUM of `connect_native_fallback_connected_{joiner,acceptor}`, not the rescue counter (`op_ctx_task::native_path_failed`).
- Pass the real `DisconnectReason`; `Pruned` only for local topology management.

Mechanism detail: docs/architecture/transport/rules-reference.md § Transport-redesign baseline telemetry (`transport_snapshot`)

### Release census (`join_versions`, #5940)

- Emit only when telemetry is on. `gateway_joins` counts at `complete_gateway_handshake` success, NEVER at intro receipt (spoofable).
- Labels stay in fixed arrays; no keys, no addresses; in `gateway_joins` (not neighbours) counts 1 to 4 report as `"<5"`; zero counts omitted in both maps.

Mechanism detail: docs/architecture/transport/rules-reference.md § Release census (`join_versions`, #5940)

### Dashboard bandwidth sampler (#5152)

`transport/bandwidth_history.rs` runs one more always-on 1 Hz task
(`bandwidth_history_sampler`), separate from the shadow telemetry
above: it reads `TRANSPORT_METRICS.cumulative_bytes_sent/received` into
a fixed-size, one-minute rate buffer that only the local dashboard's
Data Transfer chart reads. It does not emit telemetry and nothing on the
data path reads it. It is registered with `BackgroundTaskMonitor`, and the
history (`BANDWIDTH_HISTORY`) is process-global like the counters, so
several nodes in one test process share it: `MIN_SAMPLE_SPACING` and
reading the counters under the history lock keep two samplers from
double-counting or wiping the series.

### Shadow demand / queue / OS-counter / class telemetry (Phase 1.6)

- `record_outbound` is the only Phase 1.6 hot-path hook: one relaxed atomic add, no allocation, lock or blocking. The broadcast-queue gauge updates under its own lock.
- `shadow_iface_tx` is opt-in (`telemetry.iface-tx-enabled`).

```
NEVER read SHADOW_RTT_REGISTRY, cross_connection_median_inflation,
the reference_ping stats, OR the Phase 1.6 shadow_demand counters /
broadcast-depth gauge / registered bandwidth handle from the
production data path (rate limiter, retry, congestion control). The
hot path only WRITES the cheap class counters; all reads happen in
the aggregator tasks. They exist only for the staged rollout in
#4074:
  Phases 1, 1.5, 1.6 → observation only
  Phase 2 → shadow controller, still no behaviour change
  Phase 3 → opt-in flag
  Phase 4 → default switch only after Phase 3 shows improvement
```

Mechanism detail: docs/architecture/transport/rules-reference.md § Shadow demand / queue / OS-counter / class telemetry (Phase 1.6)

## Connection Lifecycle Rules

### WHEN establishing connections

- Intro rate limit 1 packet/second per source IP; gateway ramp-up 5/s for 30s, 20/s for 2min, unlimited after.
- Send multiple intro packets (`NAT_TRAVERSAL_MAX_ATTEMPTS`, 3 s overall deadline), backoff 50ms->300ms->1s->5s; the first relay observes our address (`ObservedAddress`).
- A failed intro `send_to` counts as a lost datagram, NOT an abort; one-shot acks fail via `SendFailed` (#5871). NEVER map a send error to `ChannelClosed`.
- Hole-punch completion (`traverse_nat` RemoteInbound) only on a packet under this attempt's inbound session key; drop others (`handshake_unauthenticated_dropped`) WITHOUT sending.
- Restart detection (all peers, not just gateways): an intro on an established connection is decrypted (1/sec per IP); if valid, tear down the stale session and accept a fresh handshake (#3671).

Mechanism detail: docs/architecture/transport/rules-reference.md § WHEN establishing connections

### WHEN maintaining connections

```
Keep-alive:
  → Ping every 5 seconds initially
  → After 5 pings unanswered SINCE THE LAST AUTHENTICATED INBOUND PACKET,
    interval backs off: 10s → 15s cap (MAX_KEEPALIVE_INTERVAL). Any packet
    from the peer resets the backoff, so lost Pongs on a live link do not
    stretch it. The 15 s cap is the NAT-keepalive guarantee (#5795): do NOT
    raise it; the NoOp chatter that used to mask a 60 s cap is gone.
  → Idle timeout: 120s (RealTime), 24h (VirtualTime/simulation)
  → On idle-timeout closure, per-peer backoff is recorded to prevent
    rapid reconnection cycles to dead peers (#3252)
```

### WHEN changing receipts / acks (#5795)

- Ping / Pong are never acked. `AckConnection*` is acked on every arrival but NOT recorded for dedup; `ack_ok` answered at most ONCE per connection (else loops).
- DO NOT start data at 0 again: data starts at `FIRST_DATA_PACKET_ID` (1) so an id-0 receipt is never credited to our first data packet.
- `ShortMessage` / `StreamFragment` / `Upgrade` are acked within `ACK_CHECK_INTERVAL` and RE-ACKED on duplicate; never delivered twice. DO NOT stop re-acking duplicates: the only recovery for a lost receipt.
- DO NOT make ack-only NoOps tracked again (`PeerConnection::noop`), and DO NOT ack NoOps from a remote at/above `UNTRACKED_ACK_NOOP_MIN_VERSION`: restores the ack-of-ack ping-pong.
- NoOp is acked ONLY for a remote below `UNTRACKED_ACK_NOOP_MIN_VERSION` or of unknown version. Receipts that do not fit beside a payload go in separate untracked NoOps with FRESH ids.
- `report_received_receipts` emits ack-info ONLY for ids still in `pending_receipts`, so receipts for untracked ids never touch flight size.

Mechanism detail: docs/architecture/transport/rules-reference.md § WHEN changing receipts / acks (#5795)

### QUIC upgrade control message (#5940 step e1)

- `SymmetricMessagePayload::Upgrade` is bincode index 7; APPEND ONLY at both levels (`pre_upgrade_variants_keep_their_wire_bytes`). NEVER append a field to an existing variant (e1 silently ignores trailing bytes); new data goes in a new caps-gated variant.
- Send only when `may_send_upgrade(remote_version)` (unknown fails closed; the decoder ships a release first, since peers <= 0.2.147 panic on an undecodable payload). An Offer also needs `may_offer_upgrade`. A reply to a received Offer needs no gate. The floor follows the wire-gated-floor rule in docs/RELEASING.md (`QUIC_UPGRADE_SHIPPED_IN`).
- NEVER close the connection over an Upgrade message. At most one Decline per connection per `DECLINE_INTERVAL` (`DeclineGate`).
- `DeclineReason` wire values: 1 Disabled, 2 Backoff, 3 Busy, 4 NotReady; any other value is a decline with no stated reason. Append only.
- With QUIC off, send the Decline from recv's loop via `pending_decline` (cleared after the send), NOT in `process_inbound` (a cancelled send loses the Offer).
- Do NOT send Upgrade, or any new payload, on the handshake path or before the connection is acknowledged (`traverse_nat` joiner decodes with `?`).

Mechanism detail: docs/architecture/transport/rules-reference.md § QUIC upgrade control message (#5940 step e1)

### WHEN closing connections

```
Current behavior (no wire-level close message):
  → Local side: drop_connection_by_addr tears down local state and
    exits the per-connection task. No packet sent to remote.
  → Remote side: detects closure via keepalive timeout (120s idle).
  → SymmetricMessagePayload has no Close/Goodbye variant.

NOTE: The transport has no graceful close protocol. When one side
drops a connection, the remote peer must rely on the 120s idle
timeout to detect it. This is a known limitation (#3545).
```

## Rate Limiting Rules

### WHEN implementing DoS protection

```
Intro packet rate limiting:
  → 1 second minimum between decryption attempts per IP
  → Why: X25519 operations are expensive
  → Cleanup: Expire entries every 60 seconds
```

### WHEN using token bucket

- `paced_rate`: `min(cc_rate, global_pool_rate)` with `total-bandwidth-limit`, else `min(cc_rate, bandwidth_limit)`, else `cc_rate`. A limit only lowers the rate, never raises it above `cc_rate` (#6047).
- The bucket's INITIAL rate (`initial_paced_rate`) fixes its capacity for the connection's life, so a `bandwidth-limit` above `cc_rate` must NOT raise it (#3702). Only stream fragments draw on the bucket.
- A `total-bandwidth-limit` of 0 is unset for BOTH transports (`a_total_of_zero_paces_neither_transport`).
- A configured `bandwidth-limit` is raised to `MIN_BANDWIDTH_LIMIT` (64 KiB/s): near the receiver's age-limit rate long streams fail. `total-bandwidth-limit` has no such floor.
- QUIC window cap (`recycle::CappedCubic`): window = min(Cubic, max(rate x RTT, `window_floor`)); `window_floor` = 2401 B (two datagrams + 1 byte; quinn refuses a send at in_flight + bytes >= window, so 2400 would admit one datagram), NEVER lower. No limit or before the first ack = Cubic's window.
- QUIC path pacer (`LimitPacer`): NOT the UDP stream bucket (FixedRate would pace QUIC below a higher limit). One-packet Shorts and control frames are not charged.
- A pacer write uses `reserve_within` bounded by the send's current deadline (`SendProgress::time_left`, not the connection window); a refusal ends the send `NotOpened(Paced)` / `Failed(Paced)`.
- A pacer refusal is never a stall-count timeout and a pacer wait is NOT progress: a dark paced path still goes suspect (`pacer_refusals_never_count_and_a_dark_paced_path_still_closes`).
- A message the pacer refused that UDP then sent is charged to the pacer as debt, bounded by `FALLBACK_DEBT`.
- Reserve BEFORE sending; block if insufficient.

Mechanism detail: docs/architecture/transport/rules-reference.md § WHEN using token bucket

### WHEN admitting native joiners at a gateway (#5940, A7b; on since the floor release, `GATEWAY_NATIVE_ADMISSION`)

- A joiner is charged against the QUIC path cap from admission (`JoinerCharge`). Do not add a parallel joiner counter: paths and joiners share ONE unit total in `QuicPathPermits`.
- Joiners together hold at most 1/`JOINER_SHARE_DIVISOR` of the units; the join window counts from the hub queueing the join.
- Until the hub attaches the cap (`attach_joiner_cap`) every attempt is ignored: fails closed.
- Order in `EndpointCore::admit`: Retry, per-address, global, THEN cap charge, THEN allow-set; a refusal drops the ticket and charge (`with_admission_off_no_joiner_is_charged`).
- Unpromoted joiners share one slice of the ingress queue (`QUIC_INGRESS_JOINER_DATAGRAMS`), not per-source or per-IP; record the slot per datagram (`joiner_slot`).
- Never `std::mem::forget` a charge or a mark: a leaked charge shrinks the cap for the process life.

Mechanism detail: docs/architecture/transport/rules-reference.md § WHEN admitting native joiners at a gateway (#5940, A7b; on since the floor release, `GATEWAY_NATIVE_ADMISSION`)

### WHEN a joiner dials a gateway natively (#5940, phase B; on since the floor release, `GATEWAY_NATIVE_JOIN_PERCENT`)

- The joiner alone decides, in the handshake driver (`plan_gateway_join`); not in the connect op or event loop.
- Fallback to the ordinary UDP dial is unconditional and immediate; nothing is sticky. Gateway join failures only start that gateway's own cooldown.
- `setup::join_native` is always the client and never punches; count its ends as `gateway_native_join_*`, never `connect_native_setup_*`.
- A gateway hands a completed native join to the node (`QuicUpgrades::hand_off_join`): a full queue or no node closes it (`quic_gateway_admission_handoff_full`), and so does a join that finished after an endpoint rebuild replaced its stack (`stack_gone`). Every stack the listener builds, the first and each rebuilt one, gets the same hand-off. The handed-over connection holds the joiner's charge until the node promotes it under the proven key (`set_peer_identity` calls `JoinerCharge::promote`) and drops it with the connection; never leak it. Do not mark a gateway in the index before it runs a release with this hand-off: an older gateway closes a completed join after the Hello, and a joiner whose setup succeeded has no fallback.
- The node binds a native join to its proven key WITHOUT promoting it (`connection_lifecycle::accept_native_join`): the connect op promotes it at ConnectPeer, and a ConnectRequest naming another key closes it. Never pass a native join to `handle_successful_connection` with `Some(peer)` (on a gateway that promotes it into the ring before CONNECT accepts it). A proven native join skips the UDP survivor rule (a newer native join for the same key replaces the older; one beside a UDP entry is held, `hold_native`), and one that sends no request within `NATIVE_JOIN_REQUEST_WITHIN` (5 s, a one-shot timer: the zombie sweep's 30 s tick is too coarse) is closed.

Mechanism detail: docs/architecture/transport/rules-reference.md § WHEN a joiner dials a gateway natively (#5940, phase B; on since the floor release, `GATEWAY_NATIVE_JOIN_PERCENT`)

## Backoff, Jitter, and Recovery Rules

### WHEN implementing retry/backoff

```
All retry/backoff loops MUST apply random jitter:
  → At least ±20% of the interval
  → WHY: Prevents thundering herd after gateway restart (all peers reconnect simultaneously)

Backoff sleeps MUST be interruptible:
  → Use tokio::select! to race sleep against cancellation signal (Notify, CancellationToken)
  → Plain tokio::time::sleep() in retry loop is PROHIBITED unless <1s
  → WHY: Uninterruptible sleeps prevent recovery when conditions change (e.g., isolation cleared)
```

### WHEN node has zero connections

```
MUST have an explicit gateway re-bootstrap path:
  → Cannot depend on routing through existing connections (there are none)
  → Direct gateway contact must be a fallback

WHY: Normal recovery mechanisms (acquire_new) route through existing connections.
With zero connections, the node gets stuck permanently.
```

### WHEN sending critical control messages (ReadyState, etc.)

```
Single fire-and-forget UDP sends for state that affects routing decisions are PROHIBITED.

MUST:
  → Implement retry with backoff for critical control messages
  → Consider optimistic timeout (e.g., treat peer as ready after N seconds even without ACK)

WHY: In lossy environments, a single lost packet can permanently prevent a peer
from being seen as ready. ReadyState bug required re-broadcast every 30s + 60s fallback.
```

## Socket Abstraction Rules

### WHEN writing socket code

```
NEVER use: tokio::net::UdpSocket directly in crates/core
ALWAYS use: Socket trait (crates/core/src/transport.rs)

Why: Enables SimulationSocket for deterministic testing
```

#### Exception: `DefaultSocket` for non-peer-to-peer external probes

There is one documented exception, used by `transport/reference_ping.rs`:
the `DefaultSocket` type alias in `transport.rs` resolves to
`tokio::net::UdpSocket`, but referring to it by the alias keeps the
rule-lint check on the literal `tokio::net::UdpSocket` path satisfied
without re-introducing the raw path. The reference-ping probe uses
`DefaultSocket` deliberately to call the **inherent** `UdpSocket`
methods (NOT the `Socket` trait impl), because the trait's `send_to`
calls `TRANSPORT_METRICS.record_packet_sent` — which would pollute the
per-peer dashboard LRU with the reference target IP (`1.1.1.1:53` by
default), occupying one of the `MAX_TRACKED_PEERS = 256` slots
permanently.

`DefaultSocket` is acceptable ONLY for:
- Out-of-band probes whose target is not a Freenet peer (so the
  `SimulationSocket` substitution is meaningless).
- Code that explicitly needs to skip the trait's metering /
  buffer-tuning / dual-stack setup.

For ALL peer-to-peer transport code paths, continue using the
`Socket` trait so the simulation harness can substitute
`SimulationSocket`. If you are tempted to add another `DefaultSocket`
use site, justify it in a load-bearing comment at the call site and
update this section.

#### Inbound packets pass through `DemuxSocket`

- `DemuxSocket` (`transport/demux.rs`) splits by first byte: `0x01`/`0x02` pass, `0xC0` bit set is QUIC-shaped, rest dropped. Exact ONLY with QUIC bit greasing (RFC 9287) off.
- The listener stays the ONLY reader of the UDP socket; never add a second QUIC reader (`enable_quic_ingress` queue exists for that).
- Fill the ingress queue with `try_reserve`, never a waiting send, so a slow endpoint cannot stall the listener; keep `QUIC_INGRESS_QUEUE_BYTES` in `declared_cache_ceiling`.
- Queue only allow-set sources (`allow_quic_source`) so probers get no QUIC reply; per-source cap and `QUIC_INGRESS_QUIET_RESERVE` keep one source from starving others; native joiners use `QUIC_INGRESS_JOINER_DATAGRAMS`, never the reserve.
- `QuicSourcePermit` keeps an address allowed; set capped at `QUIC_ALLOWED_SOURCES_MAX`; `allow_existing_quic_source` only for an address allowed now, so `accept` cannot resurrect a dropped one. The endpoint holds one permit per connection until quinn reports it closed (#5922).
- Gateway native admission (`quic/admission.rs`, #5940 A7b) is ON since the floor release (`GATEWAY_NATIVE_ADMISSION` true, no runtime kill switch): call `enable_gateway_admission` ONLY inside `admission::enabled_at` (`the_node_enables_admission_only_behind_the_switch`). EVERY gateway configured `prefer` admits from release day, in the index or not (a `shadow` or `off` gateway does not: native joins carry data, `shadow` keeps data on UDP; `admit_native_joins_if_on` takes the mode from `quic_policy`): `enabled_at` reads only `is_gateway` and the constant, and no gateway reads its own `GatewayConfig.native`. The index mark only steers which gateways default joiners dial natively. Operator lever for a quinn advisory: `transport-quic = "off"` on that gateway (no QUIC endpoint is built), available only while `breaker::UDP_FALLBACK` is true.
- `EndpointCore::accept_attempt`: Retry first, then rate limits, then charge the joiner to the path cap (`try_acquire_joiner`), then `allow_joiner`. A ticket dropped before handshake MUST be `refunded`; a panicking join is counted, never kills `quic_gateway_joins`.
- The listener calls `RecvErrorTracker::on_success` for `Diverted` datagrams too (`Socket::recv_classified`), or diverted QUIC traffic would end it.
- The wrapper charges outbound to the egress cap; a new send site must not wait unless it is a pacing point.

Mechanism detail: docs/architecture/transport/rules-reference.md § Inbound packets pass through `DemuxSocket`

#### QUIC endpoint (`transport/quic.rs`, #5940 step c2)

Modes and config
- The `transport-quic` default (`QuicMode::default_mode`) is read, NEVER written to config.toml, so unset nodes follow their release (default-off release = kill switch).
- An unknown `transport-quic` value is off with a warning, NEVER an error: a crash-loop rollback must start with a later release's value. `off` is deprecated (WARN).
- Emergency stop (RELEASING.md): `QUIC_DATA_RAMP_START = None` plus default mode back to shadow (Linux/macOS) / off; edit `QuicMode::default_mode` and `the_default_mode_is_prefer`. It does not reach a node that ever ran with `--transport-quic` or `FREENET_TRANSPORT_QUIC` (that value is saved to config.toml).
- `shadow`/`prefer` start with an EMPTY allow-set until an upgrade adds a peer; a failed endpoint build runs QUIC off.
- `quic-max-paths` is stored as text (`lenient_count`); a bad value is ignored, never stops the node.

Upgrade state machine (`quic/upgrade/machine.rs`)
- `UpgradeMachine::step` does NO I/O and reads NO clock (`now` from TimeSource, `sample` from GlobalRng).
- Only the higher key (server) offers; roles come only from `Event::Promoted`; equal keys = Off.
- `allow_peer` runs BEFORE the Offer, then the caller reports `Event::OfferSent`; at the path limit the server does not offer and is not charged.
- Frames for another attempt id change nothing; repeated/older Offers get no reply; a client never tears a path down for an Offer; a server receiving one records `role_conflict`.
- Backoff uses `ExponentialBackoff`, NOT `TrackedBackoff` (reads the clock); a longer pending backoff is never shortened; only a path lasting `STABLE_PATH` clears the failure count.
- `SessionEnded` takes normal backoff unless already charged (never twice); endpoint Down charges nothing; session-end shut-off closes with `SESSION_ENDED` (6), others DOWNGRADE.
- Disabled and an unknown decline reason take the NORMAL backoff. Busy/NotReady/Backoff declines and a full allow-set are uncharged; failures (close codes, lost connection, idle, deadline) mean fallback + backoff, NEVER a permanent downgrade. Codes 9/10 are native-only.
- Off is FINAL for the session: no re-offer, no charge except from a session end.
- `BackoffSeed` (`backoff_seed` after `SessionEnded`, taken by `Promoted`) carries backoff across replaced sessions; clamp `retry_at` (`SEED_WAIT_MAX`, `BACKOFF_MAX`).
- Caller contracts: report `OfferSent` before any reply; end/file any older session for the key BEFORE `Promoted` on the new one and never let an older seed replace a newer; use the listener's clock clone (not `RealTime::new()`); give both machines the session's lifetime.
- Pin: `negotiation_floor_stays_above_every_release_that_ignores_an_offer` (`QUIC_UPGRADE_NEGOTIATES_IN` must not go to 0.2.152 or below).

Data slice (`upgrade/slice.rs`)
- The `bucket` hash is pinned by a test vector; do not change it. `QUIC_DATA_RAMP_START` (`upgrade/policy.rs`) is set in its own reviewed PR at publish time and left set; only an emergency stop resets it to `None`.
- `QuicPolicy::own_caps` gives `caps::DATA` only for `prefer` and a pair in the slice.
- `quic-data-percent` replaces the ramp on one node, only `prefer` uses it; a value that is not a whole number 0-100 reads as unset, with a WARN when QUIC is on.
- A path carries data ONLY with both bits; the peer's bit is held in `State::Active` for that attempt. Accepting inbound data is set at build from the own bit alone (peer's first stream can overtake its bit); a data send without both bits is a debug assertion (`QuicPath::send_data`). Offer caps stay 0.
- `RECYCLE_FOR_DATA` (7) recycle applies only to paths without the bit, to peers advertising `caps::RECYCLES`, at most once per flip; it is NOT a fault, backoff, fallback or DOWNGRADE. Set the bit only when data-capable.
- QUIC inbound must NEVER write `last_received_nanos`/`last_inbound_nanos` (keepalive and idle timeout stay UDP-only); `quic_receipt_nanos` is written ONLY by `watch_acknowledgements` once established, and only `identity::key_holder` reads `ReceiptStamp`.

Upgrade telemetry
- `Session::count` matches every `Outcome`; `QuicUpgradeOutcome` is full at 32, so new counts need their own labelled set.
- `quic_close_received_<label>`: `unknown_code` is ALWAYS the last slot; new codes go before it.
- `quic_churned_fallback_*` is a separate prefix so `quic_fallback_*` sums exclude it; `FALLBACK_CHURN_WINDOW` must outlast a UDP idle timeout (`the_churn_window_outlasts_a_udp_idle_timeout`).
- Record each data count where the path decides it, into `QuicPathStats` and `QuicDataCounters` together; tests never read the global `TRANSPORT_METRICS`. No key may share a family prefix (`no_key_is_under_another_family`).
- Shadow = total minus carried loss is recorded in one `record_path_loss` call under one lock (bug-prevention exception).
- The dark-path rule closes ONLY data-carrying paths with no UDP session; others only count (`a_path_without_data_is_not_closed_by_a_dark_spell`).

Upgrade driver (`quic/upgrade/driver.rs`)
- With QUIC off there must be no task, select arm or GlobalRng draw. `recv` only `try_send`s Upgrades to the connection's task; no negotiation inside `recv`.
- `Promoted` is stepped from `set_peer_identity` on Set/Same ONLY. Ending an older session uses its lock (never held across an await); compare and write are one step in `by_key.entry()`. Lock order: `by_key` shard, then session state; never the reverse.
- Only an attempt that returned a Retry token takes the live slot (`Session::on_incoming`); an attempt with no slot is ignored BEFORE that check. `RETRY_TOKEN_LIFETIME` must stay above `CONNECT_TIMEOUT`; a token proves return-routability only, the key pin still applies. Any other connection closes with DOWNGRADE.
- Ending a session removes its route/by-key entry only if its own, also on driver panic. `admit()` `Ok` does not prove the endpoint is up; `QuicPathPermits` release on drop.

QuicSocket / config
- `QuicSocket::receive` reads the ingress queue only and NEVER returns an error (quinn's driver stops on one). Send: with a cap `try_take` first (short -> WouldBlock, nothing charged; socket WouldBlock returns the take); other send errors are counted and reported as sent. `UdpPoller` is Ready only if the socket can send AND `poll_admit(1200)`.
- `config.rs` pins every quinn/rustls value. Must hold: `grease_quic_bit(false)` (demux depends on it), version 1, MTU 1200, no MTU discovery/GSO/migration/NEW_TOKEN (Retry only), one ALPN per attempt (`freenet-quic/1` upgrade, `/2` native), Cubic behind `recycle::loss_gate::LossGate` (`without_loss_gate` is test-only).
- Reset, token and CID keys come from OS entropy, NEVER GlobalRng; quinn's `rng_seed` and CID nonces stay on GlobalRng. CIDs (`quic/cid.rs`) carry a keyed BLAKE3 MAC; do not use quinn's hashed generator (spoofable).
- `accept` never awaits the handshake (`QuicIncoming::establish`); QUIC integration tests use the `bounded` watchdog.

QUIC loss gate (`quic/recycle/loss_gate.rs`)
- Min RTT is NEVER forgotten (a windowed minimum makes the gate's own queue the baseline). Do NOT raise the queue threshold, add a cap or a windowed min RTT without lab cells for jitter, AQM and shallow queues (#6185, #6183): each was tried and broke one.

QUIC memory (`quic/config.rs`, `quic::memory_tests`)
- Keep initial windows small (quinn cannot take a grant back). Windows grow only on CONTIGUOUS delivery (`RangeMirror::prefix`), at most doubling, within `budget::MAX_WINDOW_BYTES` and `MAX_INBOUND_STREAM_BYTES_PER_CONNECTION` (`windows_never_exceed_the_reassembly_allowance`); room comes from others' debt, NEVER by taking credit back.
- Every window change and connection read MUST happen in that connection's receive task; tell quinn after releasing the budget lock, or the debt mirror under-estimates. Evaluation stays a running total.
- Bound: `memory <= data paths x (QUIC_CONNECTION_CHARGE_BYTES + floor grant) + budget`; the floor grant is fixed charge, the budget holds only growth above it. Data paths are capped by `quic_connection_cap` while `may_carry_data` (the clamp NEVER goes below min-number-of-connections, and the configured value is kept so config.toml is unchanged; the staging cap is never set below bytes staged); with data off the shadow charge derives from what a path GRANTS the peer (`a_shadow_path_is_charged_for_what_it_grants`).
- New QUIC buffers must be summed in `quic_memory_ceiling` (`declared_caches_plus_hosting_budget_leave_room_for_the_runtime`); `INCOMING_BUFFER_TOTAL_BYTES` and `QUIC_INGRESS_QUEUE_BYTES` count whatever `transport-quic` says.
- Staging (`ConnStaging`) is charged BEFORE the buffer grows; past the cap the stream is stopped, a second time within 30 s closes with OVER_BUDGET.
- quinn assumptions (PANICS-class) to re-check before bumping the quinn pin: `QUIC_ACK_TAIL_BYTES` assumes quinn-proto's cap of 1000 ack-only packets (M2); loss telemetry assumes quinn-proto 0.11.19 `PathStats` semantics (sent includes ack-only, lost never taken back, lost PLPMTUD probes left out of `lost` while sent ones are in `sent`, MTU discovery off).

Simulation (`SimNetwork`)
- Harness writes `prefer` at a 100% data share and pins native connect / native gateway join at 0%, unless `with_transport_quic` / `with_node_transport_quic` (`"off"` is the legacy-UDP opt-out), `with_release_quic_data_share` (the release's own share) or `with_native_connect` set otherwise. The endpoint runs on VirtualTime only in simulation (`the_transport_endpoint_runs_on_virtual_time_only_in_simulation`); the bind-time draw uses `GlobalRng::with_side_stream`; every runner must move VirtualTime (`run_turmoil_on_virtual_time`) or no QUIC timer fires.

Mechanism detail: docs/architecture/transport/rules-reference.md § QUIC endpoint (`transport/quic.rs`, #5940 step c2)

#### QUIC rollout (#5940)

- No opt-in stages; releases do the rollout. Every step watches peer disappearance per version, endpoint-down, panics, breaker trips, RSS vs `quic_paths_active`, CPU/egress, NAT success.
- Windows is shadow by default (breaker confirmed on a real machine, #6031). The release watch reads Windows separately (`quic_adapter_panicked`, `quic_breaker_trips`, `quic_breaker_start`); read any Windows QUIC panic before the next release.
- Step 0: first negotiating release ships QUIC off; nodes answer offers `Decline{disabled}`. Step 1: nova gateways and framework in shadow 24-48 h; rollback = key off and restart.
- Step 2: shadow default (`QuicMode::default_mode`) per platform only once that platform has a breaker. Gate: >= 95% upgrade success (`established_as_offerer` / accepted), < 1 fallback per path-day EXCLUDING `endpoint_down` and `session_ended_rx`, each class net of `quic_churned_fallback_*`, read only over windows much longer than 150 s; memory within measured terms; an abort spike stops the step.
- Data slice (step 3, e5b): data on a slice of pairs chosen by a hash of their two keys (both sides agree with no extra message). The ramp runs within ONE release over 24 h from its start: 5% for 6 h, 25% at 12 h, 100% at 24 h. Pairs outside the slice are the control. QUIC memory is bounded by the buffer budget and summed in the node aggregate at the QUIC connection cap, to which the connection limit is lowered while the node may carry data. Started by 0.2.158 (#6224), with the release watch and the emergency-stop branch beside it. Keep the stream limits only while permit waits plus `quic_open_uni_waited` stay below 0.5% of sends and ge_48 below 0.2% of path-windows; re-derive those bars from real data, never loosen them.
- Going to 100% is default-on only if, per slice vs control: fewer RTO transfers, timeout disconnects no worse, p90 queueing (transfers >= 1 MiB) <= 262 ms, CPU/byte <= 1.5x, egress <= cap + 5%, memory <= UDP, zero QUIC panics/endpoint-downs, and the kill switch rehearsed once.
- Rollback at any step is a release through the 0-4 h auto-update stagger (data ramp: no ramp, 0.2.156 defaults; QUIC at fault: default-off). Per-path UDP fallback and the breaker work without a release.

Mechanism detail: docs/architecture/transport/rules-reference.md § QUIC rollout (#5940)

#### QUIC identity (`transport/quic/identity.rs`, #5940 step c3)

- Identity is a raw public key (RFC 7250) from the node's EXISTING X25519 transport key: no new key, file, message or exporter proof.
- Signing is XEdDSA as Ed25519; nonce randomness from the OS (SysRng), NEVER GlobalRng (code-style.md OsRng exception). Debug output shows the public key only.
- Presented key is the Edwards form with SIGN BIT 0 (XEdDSA convention). `PeerKey::of` refuses (UnusableKey) non-canonical, small-order, torsion and u = -1 keys (forgeable, or one secret as many keys).
- Verification is PINNED to the expected key: SPKI must equal it exactly, no chain, scheme Ed25519, signature checked against the EXPECTED key; anything else fails the handshake.
- `allow_peer` records key AND kind (QuicPeerPermit); a second key per address or the same key for the other kind is KeyConflict. `accept()` ignores an attempt with no recorded key.
- The endpoint's own server config (needed so quinn passes Initials to accept) refuses EVERY client.
- The expected key comes from the node's own record, NEVER from an upgrade message. A transport holds the key it dialled (outbound) or, only with a QUIC endpoint, the key sent by `ConnEvent::SetPeerIdentity` at ring promotion.
- `conn_identity_promotion_key_mismatch` and `transport_key_refused` MUST stay 0 (`test_quic_prefer_forms_a_mesh_of_the_same_peers_as_quic_off`).
- The upgrade (e5a-5b) MUST treat a transport with no key as not eligible, never "any key"; an inbound transport's key is the peer's claim, checked only by the QUIC handshake.
- `ProvenClientKey` (gateway native join, A7b): accept only exact Ed25519 SPKI, sign bit 0, a key `PeerKey::of` accepts. Read `proven_client_key` only on a fully established connection, NEVER from `into_0rtt()`. `server_tls_proven` offers `freenet-quic/2` only.

Mechanism detail: docs/architecture/transport/rules-reference.md § QUIC identity (`transport/quic/identity.rs`, #5940 step c3)

#### QUIC-native setup (`quic/native/`, #5940 QUIC-native connect, A4)

- `setup::connect_native`/`accept_native`/`join_native` are `pub(in crate::transport)`, called only by `QuicUpgrades::setup_native`; the endpoint's native side stays `pub(in crate::transport::quic::native)`. Since the floor release the connect op asks for a setup for every ready pair (`QUIC_NATIVE_PERCENT` 100; 25 in A+ step 1), gated on `NATIVE_DIAL_READY` (true), and every join to a gateway marked native tries native first (`GATEWAY_NATIVE_JOIN_PERCENT` 100). There is no runtime kill switch: the acceptor decides on `min(offered, own)`, so a reversal reaches a pair only once either side has updated.
- ALPN: native-allowed addresses get `freenet-quic/2` only, others `/1`; one kind per address. Attempts from a native-allowed address go only to `native::waiters`, never an upgrade session; the only registration is in `accept_native`.
- Hello layout (`native/hello.rs`) is frozen. Malformed closes HELLO_REFUSED; incompatible version closes VERSION_INCOMPATIBLE with this node's 8 version bytes; rules are `connection_handler::native_version`.
- Every punch/Initial is charged to `PunchBudget` BEFORE it is sent; a destination slot is replaced only once refilled, so forgetting never refunds early.
- Proven peers (`PunchBudget`): only a completed native setup or a successful gateway hand-off proves a peer, never a UDP connection (source-scrape pin `only_native_completions_prove_a_peer`). A proof is keyed on (IPv4 or IPv6 /64, peer key), not the port. Proven setups draw on per-peer, per-address and node allowances, falling back to the shared budgets when spent (never worse than unproven). Joined proofs are a separate bounded partition. Forgetting or migrating a proof MUST NOT refill an allowance. Figures are provisional.
- Fairness between requesters steering setups is NOT decided (A5): it MUST be fair before UDP is removed. The proven-peer allowance changes who is starved, not whether.
- `QuicPathLimits::NATIVE` (udp_fallback off, data on): a send that would go to UDP is Dropped (after the `NoCredit` retries, below); a quiet timeout NEVER makes a native path suspect; a peer refusing data (NOT_ACCEPTING_DATA) closes it (`stop_sending_data`).
- `breaker::UDP_FALLBACK` (D-3, dark, true today) is read by every path through `QuicPathLimits::effective_udp_fallback` (one reader; a source-scan pin in `breaker.rs` fails on any other). With it false an upgraded path gives up what it cannot carry, never turns suspect, a route whose path ended drops its queue, and a short the queue cannot take (full or closed) is dropped and counted instead of overflowing to UDP. This is the PATH-level fallback only: a connection with no route still sends on its UDP session, which D-8 deletes, so a sim or soak with the constant false is a partial check and cannot show what a no-UDP network costs.
- `queue_native_short` returns `SendQueueFull` (Transient, connection survives, no ShortOverflowUdp, counted `quic_data_event_native_short_queue_full`, not Dropped) on a full queue. A native connection counts in `GlobalBandwidthManager`; its drop closes NODE_DROPPED_PEER.
- A Short is counted dropped ONLY by `QuicPathStats::record_short_dropped(cause)` (`quic_short_dropped_<cause>` sums to `quic_data_send_short_dropped`); `record_send` asserts a dropped Short never takes the uncaused path, and `only_record_short_dropped_counts_a_dropped_short` pins the call sites. Choose the cause where the drop is decided; never derive it by subtraction.
- Native connect gates on this node's OWN endpoint availability (per-node `QuicAvailabilityWatch`, not the process-wide metric), on the configured `transport-quic` mode being `prefer` (a shadow or off node is never ready in any role, gateway joins included). The joiner's offer reads `readiness()`; the relay's decision reads `acceptor_readiness()` (the same, except that a simulation node with its joiner role off still decides). Dial: higher key serves; setups capped by `NATIVE_SETUPS_MAX`; setup errors counted by an EXHAUSTIVE match (`connect_native_setup_<label>`).
- Every accept site MUST pass the response's decision to `dispatch_expect_connection_from` (source-scrape pin).
- A failed native setup NEVER costs the join until UDP is removed (`test_quic_native_failed_setups_fall_back_to_udp`). Both sides fall back `NATIVE_FALLBACK_AFTER` after their OWN reference, never earlier, so hole punches overlap.
- The client makes OVERLAPPING ATTEMPTS: a second dial `OVERLAP_BEFORE_END` before the window ends, neither cancelled; an attempt = dial + Hello, so a client-only success does not end the race. A first attempt that ERRORS before the overlap point ends the setup with that error and starts no second.
- The joiner dials native only while `NATIVE_MIN_WINDOW_LEFT` of the ACCEPTOR's window remains, else UDP. A UDP dial beside a pending native dial runs BESIDE it (`udp_beside_native`), never merged. Never answer Ok without a kept connection.
- SURVIVOR RULE (design note 10.2 item 18): with a native and a UDP connection to one peer key, keep the native one while it is LIVE (`identity::HOLD_WINDOW`), whichever came first; native wins only after carrying a frame (#6294, `NativeFirstFrame`, held via `hold_native`, `HELD_NATIVE_WINDOW`).
- `promote_held_native` replaces only the transport and keeps identity, ring membership, ReadyState and health; `record_success` runs before any keep/swap/drop.
- A native failure marks no failed address and records no peer backoff (a UDP backoff would refuse the native dial's own fallback); for an already-connected address it records nothing. A UDP dial that fails beside a pending native dial must not back off or mark anything; once the native dial ended without a connection it takes the ordinary failure path.
- A RESCUE is a native setup that failed on the path (`timed_out`, `handshake_failed`, `hello_lost`: `says_native_failed_on_path`; a refusal by the node's own limits or a Hello the peer answered says nothing about the path) and then UDP to the same peer connected. It is only COUNTED (`connect_native_rescued_by_udp_{joiner,acceptor}`, `connect_native_fallback_connected_*`); nothing acts on it. Native-connect demotion (a per-role strike streak that turned a node's offer or decisions off for 6 h) was removed: a node with repeated native failures keeps offering its percent and keeps deciding Native, and each failed setup costs only its UDP fallback. Do NOT re-add per-node state that lowers the offer or the decision on failures: at 100% it demoted ~6% of acceptors for hours, and after phase D a demoted acceptor could not fall back to UDP. `connect_native_demotions` and `connect_native_acceptor_demotions` stay in the snapshot JSON as always 0 (the JSON is additive; older nodes still report them). Pinned by `no_native_demotion_state_remains_in_the_crate`. Known residuals of the rescue counters, each an unearned rescue: a joiner whose plan for the acceptor was UDP at fallback time (`UdpAt`), and a joiner whose own native setup was refused locally (the acceptor's setup times out and its UDP fallback connects).
- Setup admission: before deciding Native the acceptor admits the setup (`NativeConnectState::admit_acceptor_setup`), by joiner ADDRESS: at most `acceptor_pending_setups_max(max_connections)` addresses pending (half the native setup slots, so the node's own joiner dials keep the rest; another CONNECT to a pending address shares its entry), at most `ACCEPTOR_PENDING_SETUPS_PER_SOURCE` (4) per source (an IPv4 address or an IPv6 /64), and only with a known address. A refusal decides Udp AT ONCE (`connect_native_admission_refused`), so a joiner never waits out the native fallback because the acceptor had no room; a control pair dropped for lack of room is counted (`connect_native_admission_control_dropped`). The `SetupTicket` rides the `AcceptOutcome` into `dispatch_expect_connection_from` and drops at the acceptor's native dial deadline (`NATIVE_CONNECT_WITHIN`), NOT at the dial's answer; an accept never dispatched, or a failed dispatch, drops it. The per-source cap bounds honest crowding, not forged addresses: the node cap is what holds. The cap bounds distinct pending addresses, not decisions, and the node's own native dials as joiner take setup slots uncapped. After phase D a refusal cannot "decide UDP" and will need another answer (a Rejected).
- A native dial does not drop an existing connection to that peer; success binds `IdentitySource::Proven`.
- Determinism: `bounded_send` and the native setup selects are `biased`, NOT `deterministic_select!` (it shifts GlobalRng for later tests; `test_quic_native_connect_is_deterministic`).

Mechanism detail: docs/architecture/transport/rules-reference.md § QUIC-native setup (`quic/native/`, #5940 QUIC-native connect, A4)

#### QUIC framing and path (`quic/framing.rs`, `quic/path.rs`, #5940 step e3)

The upgrade driver builds a `QuicPath` and sends control frames on it. Data goes on it by "QUIC send route" below.

**Frame header** (one message per uni stream: 18-byte header, metadata, payload, end)
- `FrameHeader::decode` refuses, never panics: unknown version or kind; `total_len` 0 or > `MAX_STREAM_TOTAL_BYTES` (Short, Stream); Control 0 or > `MAX_CONTROL_BYTES` (4 KiB); fewer than 18 bytes.
- Also refused: `metadata_len` > `MAX_METADATA_BYTES` (1120; larger metadata is left out, as on UDP); metadata or nonzero `stream_id` on Short/Control; a Stream id not caller-named.
- The sender builds headers through the same checks. A new kind or field needs a new version, sent only to a peer that said it reads it.

**Sending** (`send_short_bounded` / `send_stream_bounded` / `send_stream_chunks_bounded` / `send_control_bounded`)
- Sent is NOT delivered: a refusal or a drop for a full channel can read as success at the sender.
- Every node send MUST use a `send_*_bounded` method (`path/bounded.rs`, `SendDeadlines`, `QuicPath::stream_deadlines`), or it can wait as long as the connection lives.
- `send_stream_chunks_bounded` resets the stream if the source fails or delivers a different length; it takes no permit or stream before the first chunk.
- Streamed sends and multi-packet Shorts hold a `STREAMED_SEND_LIMIT` (48) permit until acked, stopped or reset; short/control stay within the reserve (`a_short_still_sends_while_streamed_messages_are_blocked`).
- Do NOT cap permits to the peer's limit (it starts at `DATA_START_UNI_STREAMS`, raises only when busy); a send past it ends `NotOpened(NoCredit)`, never `MaybeDelivered`. On an upgraded path that goes on UDP; on a native path (no UDP session) it is sent again on the same path up to `NOT_OPENED_RETRIES` (2) times (`QuicPath::retry_not_opened`, only while the path still takes data; a pipe does not retry), then dropped. A wait on this side's own resources ends `NotOpened(TimedOut)` and is not retried.
- A one-packet Short's background ack wait is bounded (`ACK_WAITS_WITHOUT_PERMIT`, `SHORT_ACK_WAIT`, then reset), so a peer withholding acks cannot make the path hold a task each.
- The driver reserves a ReadyAck's stream before data (`QuicPath::control_first`).
- Bounded-send timers MUST use `TimeSource::sleep_cancelable`; plain `sleep` keeps its VirtualTime wakeup until the deadline.
- A streamed send past `STREAMED_SEND_LIMIT` waits for a permit without opening a stream, and that wait MUST race `connection.closed()`: the semaphore never closes, so it would hang on a closed connection.
- Every write and source-chunk wait MUST race the stream's `stopped()` and reset on a stop: quinn-proto does not report STOP_SENDING to a writer blocked on connection flow control.
- Dropping a send future before finish resets its stream (`Drop` on `Outgoing`); quinn's drop would FINISH it, read as an early end. One dropped while waiting for acks after finish is NOT reset and may still be delivered, except when its own deadline ended the wait or a Short/control frame's background wait hits `SHORT_ACK_WAIT`: those are reset.
- `MaybeDelivered(..)` means the whole message was written and the peer may have it: not a Failed. `Failed(NotOpened(..))` can go another way. `Failed(TimedOut)` counts toward `SEND_ABORT_LIMIT`, `NotOpened` does not. `MaybeDelivered(TimedOut)` counts toward the stall close, `NotOpened(..)` (a wait to open, TimedOut or NoCredit) does not.
- After `MaybeDelivered` or any Failed but `NotOpened` a resend needs a FRESH id: the receiver drops UDP fragments under the old id (still acking them) and cancels a QUIC stream.
- A `NOT_ACCEPTING_DATA` stop means the sender's gating is wrong: log, send on UDP, do not retry on this path (a native path closes instead).

**Send deadlines and gap**
- The gap is judged on CONNECTION progress (`bounded::ConnectionWatch`): out only after its own gap AND no new ack for `STALL_PROGRESS_WINDOW`; never past total. Cap holds count only within `route::hold_credit` of the last ack.
- A stalled source is bounded by `SendDeadlines::source_gap`; control sends have no gap. A pipe whose sink is UDP keeps 5 s (its receiver gives up then); a QUIC pipe takes min(source gap, `receiver_gap`).
- A streamed send yields before each piece but the first while another holds a permit (`Outgoing::take_turns`); NEVER yield after the last piece, or the end takes a datagram of its own.

**Shadow mode** (`QuicPathLimits::SHADOW`, `accept_data = false`; DEFAULT accepts)
- Short/Stream frames are stopped with `NOT_ACCEPTING_DATA` right after the header decodes, before the budget lock or any body read; nothing is charged, so shadow QUIC cannot take UDP's room. Control is read as usual.

**Receiving**
- One task per connection reads every stream and only uses `try_send`; a panic closes with `DOWNGRADE`. Short/Control channels are bounded (full: dropped, counted); Stream goes through `path/inbound.rs`.
- NEVER read a stream in order (`read_chunk(_, true)`) on a path, or let a stream switch from in-order to unordered reads: every bound assumes unordered reads.
- NEVER keep bytes read before the header outside the staging cap: an unordered read returns the peer's credit for every byte.
- A reader never stops reading, even waiting for channel room: a byte past the declared end is `Overlong` at once, and reading the end frees quinn's slot.
- NEVER charge or refuse a one-packet Short after its body is read: its sender may already read Sent, and a refusal loses it.
- A connection's streams may hold at most `budget::gap_cap` gap ranges, past which the stream is stopped `EVICTED`; a second trip in `STAGED_TRIP_WINDOW` closes with `OVER_BUDGET`.
- GRO stays off (`max_receive_segments` 1, pinned); quinn-proto must be 0.11.17 or later.

**Inbound adapter**
- A Stream payload goes into an ordinary `StreamHandle` (UDP fragment shape), announced (`InboundStreamArrival`) before the payload.
- Before allocating: header checks, then the `InboundStreamBudget` check and charge under one lock; Shorts are charged too. Refusal is STOP_SENDING (`stop_code`).
- Completion is by BYTES: exactly `total_len`, then the end. Early end (`Truncated`), extra bytes (`Overlong`) or sender reset cancels the handle (`Cancelled`).
- The charge is released only when all state is gone (`Weak`). No quinn chunk is kept (it may pin a larger allocation): reads copy into buffers. Buffers grow to the highest offset received (doubling, never past the declared length, exact at the end); NEVER allocate the declared length up front. A throttled age-limit expiry is counted `ThrottledPastAge`, never `EvictedIdle`/`EvictedAge`.
- While `ConnMeter::throttled_since` holds, a progressing stream's idle timer is paused (age limit still applies); a stream with no byte is never paused.
- A relay pipe (`pipe_stream_on_path`) does NOT wait out its upstream's throttle. Refused ids are NOT remembered; an id already held is refused (`StreamIdInUse`).

**One budget per peer** (QUIC memory gate M3)
- `QuicPath::with_budget` charges the peer's UDP `SharedInboundBudget`: ONE allowance for both paths. Node paths MUST use `with_budget`; `new` / `with_limits` are `#[cfg(test)]`.
- Charges are keyed by `InboundKey` (`Udp`, `Quic`, `QuicShort`): never charge, test or release one path's stream under another's variant.
- A gone UDP charge is released ONLY by UDP code (`Releaser::Udp`), keeping UDP's release timing exactly main's; QUIC releases only QUIC charges; UDP's check and sweep also release gone QUIC ones (`udp_and_quic_paths_share_one_inbound_allowance`).
- The budget lock is `parking_lot`, never held across `.await`, not reentrant; it may be held while taking `StreamRegistry` locks, never the reverse.

**Path recycling and liveness** (`quic/recycle.rs`, #5940 M6; all close with `PEER_NOT_ACKNOWLEDGING`)
- Guard 2 is inactive while the egress cap holds THIS connection's transmits. `socket::CapHolds` is per connection (#6128), NEVER node-wide (a capped node always has one held); `fallback_pollers` must stay 0. Do NOT narrow `CapHolds` to sends counted since the hold: a held transmit is also in quinn's bytes in flight.
- Dark: outages shorter than the window must survive any probe backoff (`the_dark_rule_waits_out_the_probe_backoff_of_an_ack_only_peer`). The dark close applies only to a data path with no UDP session (`DARK_CLOSES_WITH_UDP` false); quinn's idle timeout ends the rest, since closing dropped Shorts a recovering path delivers.
- Credit-starved (`CreditWatch`): a path limited by its congestion window, shared uplink or egress cap NEVER counts (`a_congestion_limited_path_under_the_floor_rate_is_never_closed`).
- Stream credit withheld (`StreamCreditWatch`) closes after `STREAM_CREDIT_WINDOW` (60 s, not 10: honest receivers lower the limit per waiting Short).
- A send timeout before its stream opened (source, permit or stream credit wait) is back-pressure and does NOT count toward the stall close (`route::timed_out`).
- A stream's end ack is awaited a bounded time (`SendDeadlines::ack_wait`), then `MaybeDelivered(TimedOut)` and reset. `SHORT_ACK_WAIT` must stay above `UDP_PATH_DARK_END_MAX` + keepalive (compile-time assertion).
- Late ends (`LateEndCheck`; `quic_late_end_<kind>` counts them, `quic_late_end_judged_<kind>` counts ends that reached the proof point, once per end, the denominator): three episodes (late ends within 60 s of each other are one) in 30 min count a would-close on every path; closing a native path is behind `LATE_ENDS_CLOSE_NATIVE` (false, ships dark until the D-3 flag day) and a path with a UDP session is never closed (`LATE_ENDS_CLOSES_WITH_UDP` false); no field positive exists. Ack waits expired (#6205): closes a native path only (`ACK_WAITS_EXPIRED_CLOSES_WITH_UDP` false); #6205 gates UDP removal. A reset at an expired wait is a STRIKE only with evidence the peer is alive and the end went out (`StrikeCheck`).
- No strike while the cap holds a transmit, after a late timer wake (`LATE_WAKE_SLACK`), after a long-dark spell, or on a slow path. `ACK_WAIT_EXPIRY_LIMIT` strikes in a window is a would-close; acks NEVER reset the count (`ack_wait_expiries_count_a_would_close_whatever_else_is_acknowledged`).
- Stream age (`BodyReader::set_stream_age`): past `inbound_stream_age_limit(len)` a stream stays only while its CONNECTION reads at least 32 KiB/s (per connection: quinn shares round-robin), up to `STREAM_AGE_LIMIT`.
- The path closes past `recycle::SEND_ABORT_LIMIT` (4096) of its own resets. e5 constraint: `SEND_ABORT_LIMIT` and `PEER_NOT_ACKNOWLEDGING` ask the peer to fall back to UDP for THIS connection only: e5 must allow a new upgrade after its normal backoff.

**Control payload** (`quic/control.rs`): `version u8 (CONTROL_VERSION 1) | type u8 (0 Ready, 1 ReadyAck) | attempt u32 | caps u32`, exactly `CONTROL_LEN` (10) bytes.
- `QuicControl::decode` checks the version first (a later version is `UnknownVersion` at any length), refuses an unknown type and any other v1 length (appended bytes too), and clears caps bits outside `caps::KNOWN`.
- Control refusals are counted per path only through `QuicPath::decode_control` (`control_unknown_version`, `control_unknown_type`, `control_malformed`), not by `QuicControl::decode`; do not count them in both.
- A new type or field needs a new version (`UpgradeControl` instead grows by caps-gated variants). Ready stays at version 1 until the sender knows the peer reads newer by another route (Offer caps, release floor).

**Send route** (#5940 e5b-6, `quic/upgrade/data_route.rs`, `peer_connection/quic_send.rs`, `quic/path/route.rs`)
- A message has one route, chosen when sent (`QuicPathState::data_route`): held only once both sides set the data bit (`Action::EnableData`), unless the peer refused. None with QUIC off (no task or `GlobalRng` draw added), in shadow, after teardown.
- NOTHING HERE WAITS IN THE CONNECTION'S TASK: one-packet messages `try_send` into a per-path queue (`DataRoute::queue_short`; full or closed: UDP at once); larger messages, Streams and pipes run as tasks in `outbound_stream_futures`.
- One worker per path holds at most `MAX_UNI_STREAMS` `send_short_bounded` calls (or the queue never fills); on `connection.closed()` it hands what it holds to UDP.
- A message goes on UDP only when the outcome proves the receiver cannot deliver it (`route::next_step`). `MaybeDelivered`: NEVER resent; a stream reports `Dropped` (`Delivered` would let the broadcast queue record state the peer may lack).
- `Failed(NotOpened)` (after the native-path retries, where there is no UDP), unbuildable frame, `NOT_ACCEPTING_DATA`: UDP. `Failed(TimedOut)` / connection lost, `BUDGET_REFUSED`: Short on UDP, Stream `Dropped`. Other stops, Source, `LengthMismatch`: `Dropped`.
- A stream that opened never goes again under its id (the operation layer retries with a fresh id, `OutboundStreamFailed`). A source that ends or fails before its first chunk is `Dropped`.
- Stall close: `STALLED_SENDS_LIMIT` timeouts in a row close with `DOWNGRADE` (`route::StallCount`), counting only while nothing was newly acked within `STALL_PROGRESS_WINDOW`; a pipe timing out on its source does not count (`WatchedSource`). The window MUST stay: without it a busy relay or slow consumer is closed and loses what readers hold.
- Suspect path (`is_suspect`): after the first counted send timeout `data_route()` gives no route and new data goes on UDP; sends already on the path stay (a resend could deliver twice). It takes data again (`suspect_left`) after the ack count moved.
- `NOT_ACCEPTING_DATA` at the sender: log once per path, count every time (`data_refused`), new sends go on UDP. `send_data` has three states so a send racing the refusal does not trip `debug_assert_carries_data`; keep all three.
- A path whose data bit is 0 stops the first data frame with `NOT_ACCEPTING_DATA` and closes with `DOWNGRADE` after `REFUSAL_CLOSE_DELAY` (`closed_refusing_data`); without the close an acked one-packet Short is lost.

**QUIC receive flow** (#5940 e5b-4)
- The session owns one pair of data channels (`data_channels`); paths deliver with `try_send`. Two guarded `recv` select arms read them: nothing waits in the connection's task, arm bodies have no await. With QUIC off poll order and `GlobalRng` draws must match the old 7-branch select (`nine_branches_with_the_extra_guards_off_poll_as_seven`).
- Short arm: hash into `dispatched_msg_hashes`, release the charge at once (`release_if_gone`). A Short is NEVER suppressed; the same embedded metadata dispatches twice, as on UDP.
- Stream arm: an id held as Udp or in `completed_inbound_streams` is cancelled and counted; else it goes into `completed_inbound_streams` AT ARRIVAL, not completion, so a later UDP fragment is `AlreadyCompleted`. The handle goes to the `InboundStreamRegistry` (not `streaming_handles`).
- A Short or Stream with a full channel waits in its own reader, stopped `EVICTED` past the age limit; Control is dropped and counted. A waiting Stream keeps reading so it does not hold the sender's credit.
- A Short waits BEFORE its end is read (reading the end frees the stream), so waiters stay capped by the stream limit. A one-packet Short is not charged.

Mechanism detail: docs/architecture/transport/rules-reference.md § QUIC framing and path (`quic/framing.rs`, `quic/path.rs`, #5940 step e3)

#### QUIC errors, close codes and liveness (`quic/error.rs`, `quic/liveness.rs`, #5940 step e4)

- `quic/error.rs` maps every quinn error to one `QuicErrorKind` by EXHAUSTIVE match; do NOT match quinn errors elsewhere, convert and match the kind.
- Reset/Stopped are TRANSIENT except codes refusing the message itself: HEADER_REFUSED, OVERLONG, FRAGMENT_REFUSED, NOT_ACCEPTING_DATA, LENGTH_MISMATCH (`stop_is_retryable`/`reset_is_retryable`).
- Use `QuicEndpoint::error_kind` (`error::attribute`), not the bare mapping: connection-scope kinds become EndpointDown while Down, EXCEPT `Refused{IdentityMismatch}` (security signal). e5 MUST NOT act irreversibly on `Protocol{0x1}` without re-checking `availability().not_up()`.
- A connection-scope QUIC error ends the QUIC path only: e5 falls back inside the transport and NEVER surfaces it to operations.
- Close/stop/reset codes are WIRE VALUES: NEVER renumber; add at the next free value (0x0B). Ranges: 0x00-0x0f CONNECTION_CLOSE, 0x10-0x1f STOP_SENDING, 0x20-0x2f RESET_STREAM; each in its module's `ALL` (compile-time and source-scrape checked). Unknown codes read as ConnectionLost.
- Close codes: NODE_DROPPED_PEER 0, DOWNGRADE 1, IDENTITY_MISMATCH 2, SEND_ABORT_LIMIT 3, PEER_NOT_ACKNOWLEDGING 4, CONTROL_REFUSED 5, SESSION_ENDED 6, RECYCLE_FOR_DATA 7 (sent ONLY to a peer with `caps::RECYCLES`; see "Data slice"), OVER_BUDGET 8, VERSION_INCOMPATIBLE 9 and HELLO_REFUSED 10 (native connections ONLY).
- Every close goes through `QuicCloser` (one closer wins, once); NEVER call quinn's close with a bare number.
- NEVER feed endpoint death to `ListenerExit` (it exits the node). Sole exception: a gateway with `FREENET_SUPERVISED=1` (`gateway_quic_restarts`; `INVOCATION_ID` alone does not count) exits `QuicEndpointDown`/`QuicCooldownEnded`, code 42, NEVER fast-crash 45; recorded first via `breaker::note_endpoint_down_exit`, unrecordable or `quic-ignore-crash-breaker` deaths do not exit.
- The node needs `panic = "unwind"`; `QuicEndpoint::bind` refuses `panic = "abort"` (`require_unwinding`) and QUIC runs off.
- Socket-adapter panics (`poll_recv`, `try_send`, `poll_writable`) are caught before quinn; `adapter_panicked` marks Down; the call returns an io error, NEVER `Ready(Ok)`/`Ok`, and later calls return it first.
- After an adapter panic NOTHING more leaves the endpoint (no CONNECTION_CLOSE; `an_adapter_panic_ends_every_connection_and_counts_once`); on every Down each watcher closes locally with DOWNGRADE.
- `connect` refuses when Down or the adapter flag is set (`refuses_connects`); the `try_send` flag check is the only guard on quinn's `respond()` path.
- Quinn is PINNED `freenet-quinn = "=0.11.13"` (+ `freenet-quinn-proto`) because liveness relies on quinn internals: re-check before bumping; return to upstream once quinn#2987 ships.
- A panic in quinn's own driver code is an ENDPOINT DEATH, a controlled exit (#6184): `EndpointHealth::quinn_panicked`; supervised nodes exit via `QuicEndpointDown`, others move paths to UDP.
- Do NOT add per-call guards or catch-and-leak around quinn. Stop calling quinn once the endpoint is not Up (`refuses_connects`, `biased` accept select, router DROPS attempts). Nothing run during an unwind may call quinn (a Drop close/reset is a second panic and aborts).
- Do NOT skip closes on the endpoint-wide `quinn_may_be_poisoned` flag (skips the watcher's close of sound connections).
- Breaker (`quic/breaker.rs`): an uncontained panic trips it next start. Gateways on Linux/macOS: ANY abnormal end of a QUIC-enabled run trips the next start (cooldown 15 min doubling to 4 h; peer 24 h). Peers and Windows/boot-identity-less gateways use SHORT RUNS (only a run under 10 min that recorded a QUIC panic counts, each start within 24 h of the previous): peer trips after two (below the rollback's three; `peer_breaker_trips_before_the_crash_loop_rollback`), gateway after three. Ordinary stops never count. The `legacy_verdict` shadow NEVER changes whether QUIC runs.
- With no UDP fallback (`breaker::UDP_FALLBACK`, set by the UDP-removal release), a peer trips at three and its trip keeps QUIC on with start backoff and an immediate update check. Flipping the constant also turns on `quic/fatal_exit.rs` (dark today, no runtime switch): before a transport-fatal exit (endpoint death, driver death, planned cooldown restart) a supervised node runs the binary's registered update check, bounded at 20 s (`FATAL_EXIT_UPDATE_CHECK_TIMEOUT`, under the unit's 45 s `TimeoutStopSec`), which only pre-downloads (`ExecStopPost` installs); a stop during it exits 0. A QUIC endpoint that cannot be built then ends the node too (exit 45 when the supervisor opted in to fast-crash, else 42; the opt-in MUST be registered BEFORE the node is built, the exit runs inside `build`); an unsupervised node exits at once without the check. A hook panic is contained (exit code unchanged); a failed check never blocks the exit.
- Availability is per endpoint: Disabled, Down and Released are final for an endpoint (a node that rebuilds its endpoint holds its own settable watch that goes Down then Up; each dead endpoint's watch keeps its Down); `spawn_accept_loop` is called once per endpoint, holds no owner share and PARKS instead of returning (the monitor treats any task end as node failure).
- QUIC endpoint rebuild (`quic/rebuild.rs`, #6272; dark: `ENDPOINT_REBUILD` false unless a test turns it on, so no production node rebuilds and a supervised node exits for a restart): an unsupervised node whose endpoint died rebuilds it on its one socket after a backoff (1 s doubling to 5 min, +-20% jitter; reset only after `REBUILD_STABLE`). The rebuild loop OWNS the accept loop and attempt router (`spawn_accept_loop_with`, `spawn_attempt_router_with`) as the task `quic_endpoint_rebuild`; a task that ends while the endpoint stays up still ends the node.
- Declaration order is load-bearing: `QuicEndpoint`'s owner field MUST stay declared before the endpoint (so Released is stored first); the `QuicPathState` field MUST stay declared before `inbound_budget` in `PeerConnection`.
- Register tasks started from the transport open path with `BackgroundTaskMonitor::late_registrar()`, NOT `register()` (the monitor drains before the transport opens, so `register()` tasks go unmonitored).

Mechanism detail: docs/architecture/transport/rules-reference.md § QUIC errors, close codes and liveness (`quic/error.rs`, `quic/liveness.rs`, #5940 step e4)

### WHEN testing transport code

```
USE: SimulationSocket::bind(addr).await
This provides:
  - Deterministic packet delivery
  - Fault injection (loss, latency, reordering)
  - Time control via VirtualTime
```

## Streaming Rules

### WHEN handling large payloads

```
Threshold: Check should_use_streaming(payload_size)
Default: 64KB

Streaming path:
  1. Send metadata first (stream_id, total_size)
  2. Fragment into chunks
  3. Send chunks with sequence numbers
  4. Reassemble at receiver
```

### WHEN implementing fragment reassembly

- USE `LockFreeStreamBuffer`. Completion is in BYTES (received == `total_size`, contiguous); NEVER a fragment count.
- Reject inserts past `total_size` and empty fragments other than #1.
- Three stream types, NEVER mixed (never feed one stream to both reassembly paths): `InboundStream` and `StreamHandle` are transport-internal; above it use only `boundary::IncomingStream`.
- Match a stream to its connection by `StreamIdentity` (`IncomingStream::is`), never by buffer. `chunks()` has NO inactivity timer, ends after its first Err, and its end does not prove the byte total: count bytes yourself and bring your own inactivity timer (#5445).
- `pipe_stream` MUST re-chunk (`Rechunk`): every fragment but #1 and the last exactly full, since the receiver refuses empty fragments other than #1 and allots ceil(total / `FRAGMENT_PAYLOAD_SIZE`) + 1 slots.

Mechanism detail: docs/architecture/transport/rules-reference.md § WHEN implementing fragment reassembly

### WHEN admitting or sending a stream

- Every new stream passes `check_declared_stream_length()` (0 and > `MAX_STREAM_TOTAL_BYTES` invalid) and the per-peer `InboundStreamBudget` (shared with a QUIC path to the same peer) BEFORE any state is created from its declared length; admission and charge use the same cost.
- A refused stream id is remembered (`RefusedStreams`) so later fragments drop uncounted; do not use a forgetting cache; if a refusal cannot be remembered, close the connection. Completed streams are remembered (`completed_inbound_streams`).
- Release a charge only when ALL reassembly state is gone (via `Weak`), never on a single map removal. Evicting a legacy stream cancels it via `LegacyStreamToken`.
- Evict idle or older-than-`max_inbound_stream_age(declared)` streams: scales with length, not refreshed by traffic, never a flat constant.
- Remove a dropped connection's unclaimed operations streams from the orphan registry. Count every refusal/eviction in the `InboundStreamOutcome` list.
- Sender: `send_stream` / `pipe_stream` return `OutboundStreamInvalidLength` (stream-scoped, NOT transient) for a length the receiver would refuse; no fragment is ever empty.

Mechanism detail: docs/architecture/transport/rules-reference.md § WHEN admitting or sending a stream

## Testing Checklist

```
□ Test encryption round-trip
□ Test tampering detection (modify encrypted packet)
□ Test wrong key rejection
□ Test NAT traversal (SimNetwork with NAT config)
□ Test keep-alive and timeout
□ Test congestion control at various RTTs
□ Test streaming with packet loss
```

## Pitfalls to Avoid

```
DON'T: Use std::time for timeouts in production code
WHY: Breaks deterministic simulation testing
USE: TimeSource trait

DON'T: Assume packets arrive in order
WHY: UDP provides no ordering guarantees
HANDLE: Reordering, duplicates, loss

DON'T: Trust packet contents before decryption
WHY: Attacker can forge UDP packets
VERIFY: Decrypt and validate AEAD tag first

DON'T: Block on socket operations
WHY: Deadlocks event loop
USE: Async socket operations with timeout
```

## Documentation

- Architecture: `docs/architecture/transport/README.md`
- Security: `docs/architecture/transport/security.md`
- Connection lifecycle: `docs/architecture/transport/connection-lifecycle.md`
- LEDBAT++: `docs/architecture/transport/design/ledbat-plus-plus.md`
- Bandwidth config: `docs/architecture/transport/configuration/bandwidth-configuration.md`
- Mechanism detail and history behind these rules: `docs/architecture/transport/rules-reference.md`
