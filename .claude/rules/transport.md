---
paths:
  - "crates/core/src/transport/**"
---

# Transport Module Rules

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
  → Each connection gets independent limit
  → Default: 10 Mbps (1,250,000 bytes/sec); see DEFAULT_RATE_BYTES_PER_SEC in fixed_rate/controller.rs

Global pool mode:
  → total-bandwidth-limit enables this
  → Formula: rate = max(total/connections, min_per_connection)
  → min_per_connection prevents starvation; clamped to the total
  → total-bandwidth-limit is ALSO a node-wide egress cap (egress_cap.rs)
```

### Node-wide egress cap (#5940 decision 3)

```
DemuxSocket owns one EgressCap (token bucket, allowed to go into debt) when
total-bandwidth-limit is set, and charges EVERY datagram it sends. No setting
→ no cap → one Option check per send.

Only stream fragment tasks WAIT for the bucket (send_stream, pipe_stream;
outbound_stream.rs::capped_attempt). With a cap, a fragment pays its
connection's bucket ONCE with TokenBucket::reserve (debt, then sleep the
exact deficit, no jitter), as the uncapped path does: PER-CONNECTION
ADMISSION IS IN ORDER. A reservation whose wait would reach the stream's
deadline is not made (TokenBucket::reserve_within, one lock) and the
stream fails at once, blamed like a deadline failure with the would-be
wait counted as connection time: a refused reservation takes no place, so
it cannot delay the fragments behind it (otherwise above
~5 x rate / 1200 streams on a connection nearly every stream fails).
Refusals are counted apart, egress_streams_refused_<cause> (connection or
other; never cap, nothing has slept for the cap before a reservation), with
gap + would-be wait as the gap, and their own WARN;
egress_streams_abandoned_<cause> means only streams that died at the
deadline. A reservation that is made and later refunded (cancel, a later
failure) does NOT move its followers up: queued waits are not recomputed.
An admitted newcomer CAN delay streams already in progress (as uncapped
and in 0.2.147); a per-connection scheduler is future work for both.
egress_long_waits_<cause> does not count refusals; read it together with
egress_streams_refused_<cause>. Only the
connection wait is checked: a
fragment can still fail on the node cap at the deadline. Otherwise the
fragment keeps the payment (ConnectionPayment) through every later wait,
on cwnd and on the cap; if the fragment is not sent it is given back,
clamped to capacity (a late refund credits at most one datagram the
refill also covered: capacity moves between connections, the node cap
holds; so no timer-slack margin is needed), and a send error after the
take does not refund it (as uncapped). Rate updates every RTT can
reorder waiters slightly, as on the uncapped reserve path; order is not
a correctness property of the cap. Then each attempt is:
cwnd check, node-wide egress_cap::try_take, back to back with no await
before the send; if the cap is short, sleep through StreamPacing (deficit x
U[1.0, 1.4) + U[0, 2 ms)) and start again from the cwnd check. NODE-CAP
ADMISSION IS A RACE: nothing is held on the cap while waiting, the order
among waiters is random, so connections share the cap by their number of
waiting streams (no isolation between connections). The kept connection
payment can be stale after a cwnd or cap wait (the bucket refills under
it): a connection may then exceed its bucket by one datagram per waiting
stream. That moves capacity between connections only; the node cap is
unaffected.
NEVER put a reservation on the NODE cap (earlier reservation/hold models
there leaked capacity). 0.2.148 used try_take + jittered retry on
the per-connection bucket too; the race starved streams on busy connections
past 5 s (every abandonment on capped peers was `connection`, failures
doubled on the busiest capped peer). StreamPacing (one per stream, only with
a cap) measures the gap since the stream's previous fragment was sent, the
gap the receiver's STREAM_INACTIVITY_TIMEOUT times, and is checked BEFORE
every take and after every sleep, and on every poll of the cwnd wait: at
PACED_WAIT_DEADLINE (5 s) the stream fails, stream-scoped, with its flight
released, and the fragment is not sent. The deadline also applies to the
first fragment, which the receiver does not time (its 5 s timer starts when
fragment #1 arrives; before that it waits up to STREAM_CLAIM_TIMEOUT, 60 s).
That is deliberate admission control: a stream that cannot get one slot in
5 s fails fast and the operation retries elsewhere. The overload drop-off
has the same shape as 0.2.147 (the receiver's timeout) but fails fast.
In pipe_stream the first gap starts when the first upstream chunk arrives
(pipe_stream's own 5 s upstream inactivity select bounds that wait), and
from fragment #2 on, time waiting on the upstream stream counts toward the
gap and is attributed to `other`. On capped nodes a stream that hits the
paced deadline while cwnd-limited is counted in egress_streams_abandoned_*,
not in the cwnd-abort counter (stream_send_abort_cwnd); a cwnd timeout
(CWND_WAIT_TIMEOUT, 3 s) that comes first is still counted there. The take is PACED_TAKE_BYTES (MAX_PACKET_SIZE); the socket
returns what the datagram did not use (Socket::send_to_paced), and all of it
on a failed send. Without a cap the per-connection bucket keeps its old
reserve-and-sleep path. Everything else goes through plain send_to and is
charged once sent (a failed send is not charged), into debt.

NEVER add a wait to: the listener loop, the handshake tasks
(gateway_connection / traverse_nat), the keepalive task, outbound_short_message
(it runs inline in peer_connection_listener, whose drain loop would stop
polling recv), or anything inside PeerConnection::recv (ACKs, Pong, receipts,
retransmits, ack_ok). recv is a cancellable select! branch that changes state
before it sends (answered_connection_ack, receipts taken before the NoOp).

Unpaced debt is capped at DEBT_LIMIT (1 s at the cap); the rest is forgiven
(sent uncharged) and counted. Over any interval ending at a paced send the
node sends at most burst + rate x interval + bytes forgiven in the
interval. Invariant: nothing is reserved on the NODE cap; a paced sender
holds no node-wide capacity while it waits, and is charged there only at
its send (it does hold its per-connection reservation, see above). Gaps
that reach LONG_WAIT
(2.5 s) and abandoned streams are counted by what took most of the gap
(WaitCause: cap, connection = the connection's own bucket, other = cwnd, the
upstream stream for relayed streams, and the rest): egress_long_waits_<cause>,
egress_streams_abandoned_<cause>, egress_streams_refused_<cause>. Each has
its own rate-limited WARN with one line per cause seen since the last, each
with its count and remedy; the per-stream abandonment line is at debug. The
cap counts UDP payload, not IP/UDP headers.
EgressCap::poll_admit is the poll form for a sender that is polled (the QUIC
endpoint's UdpPoller, #5940): Ready exactly when try_take would succeed at
that instant (one shared predicate, `admits`), else Pending with a timer on
the cap's TimeSource, kept in the caller's AdmissionWaiter. The timer is
STAGGERED: deficit plus one WAKE_DATAGRAM_BYTES of refill per waiter queued
ahead, so N waiters with one deficit wake one per datagram of refill, oldest
first, not all N at once. While Pending the cap also holds the caller's
waker (at most MAX_ADMISSION_WAKERS; past it an entry is dropped WITHOUT
waking, its waiter keeps its timer); a waiter keeps its queue place across
polls, and dropping an AdmissionWaiter removes its entry, so a closed
connection cannot absorb a wake. Bytes given back (a take that sent less, or a failed send)
wake one held waker per WAKE_DATAGRAM_BYTES (1200, the QUIC datagram) the
bucket then holds, and none if it holds less, so legacy give-backs cost N
waiting QUIC connections nothing until a datagram fits; wakers are woken
after the lock is released, oldest registration first (a FIFO; overflow
drops the oldest). An admitted waiter took the slot reserved for it, so
nobody behind it moves; a waiter dropped while queued wakes the next
MAX_FOLLOWERS_WOKEN_PER_LEAVE (8) behind it so each re-polls to its new
place (AdmissionQueue::leave), so N closes cost at most 8 N wakes. Waiters
further back stay up to one datagram late per such drop until they next
poll. An entry evicted at MAX_ADMISSION_WAKERS wakes nobody. No waiter wakes before
the bucket can admit it. It takes
nothing; the QUIC socket takes with
try_take and sends with DemuxSocket::try_send_to_paced, so its datagram is
charged once, by the take. Every take is at most one datagram, within the
burst (QUIC: one segment per transmit, debug-asserted), so the bound above
holds exactly.
The cap is built on RealTime even in simulation, because the listener and
its connections run on RealTime there: a VirtualTime cap would compute
deficits on another clock than its waiters sleep and time out on. The QUIC
endpoint runs on the cap's clock for the same reason (bind_udp_listener
builds both on RealTime; tests build both on one VirtualTime).
Tests assert on per-cap EgressCap::stats(), never the global counters.
```

### WHEN modifying congestion control

```
BEFORE changing LEDBAT++ parameters:
  1. Understand RFC 6817 and draft-irtf-iccrg-ledbat-plus-plus
  2. Test at multiple RTT values (10ms, 50ms, 100ms, 200ms)
  3. Test inter-flow fairness (2+ concurrent connections)
  4. Document reasoning in commit message
```

### Flight-size release invariant (issue #4345)

```
Flight size has THREE release paths. ACK is the normal one; the other two
exist so a stream whose ACKs never arrive (lossy path / starved reverse-ACK
channel) cannot pin flight size at cwnd and stall every subsequent stream
(the #4345 "cwnd wait timeout" / "no fragments received" failure):

  1. ACK — report_received_receipts returns the packet size; the recv loop
     calls on_ack* which decrements flight size.
  2. ABANDON — after MAX_PACKET_RETRANSMITS with no ACK, SentPacketTracker
     returns ResendAction::Abandon and the recv loop calls
     CongestionControl::release_flightsize(len). Bounds the worst case for a
     black-holed individual packet.
  3. STREAM ABORT (drop_stream) — when an outbound stream aborts (cwnd-wait
     timeout, upstream stall/error, or mid-send failure in
     outbound_stream.rs), release_aborted_stream_flightsize() calls
     SentPacketTracker::drop_stream(stream_id), which removes ALL of that
     stream's still-tracked packets and returns their byte total; the abort
     then release_flightsize(total). This frees the stranded bytes in one
     shot instead of waiting ~6s for each fragment to abandon individually.

Each in-flight packet is released EXACTLY ONCE across these paths, because
drop_stream / Abandon / ACK each REMOVE the packet from pending_receipts; a
packet absent from pending_receipts yields no ack-info and cannot abandon.

  - on_timeout() applies the cwnd loss response only; it MUST NOT change
    flight size (the timed-out packet is immediately re-sent and stays in
    flight). Do NOT reintroduce a decrement-on-RTO + re-add-on-resend pair —
    the recv loop always re-sends on RTO, so that is net-zero and does not
    drain a never-ACKed packet.
  - get_resend KEEPS a resent packet in pending_receipts (clones the payload,
    refreshes send-time in place). This is what lets a concurrent drop_stream
    (spawned abort task) still see and release an in-flight-being-resent
    packet. Pure tracker bookkeeping — it does NOT touch flight size.
  - The recv loop's resend re-registration MUST use refresh_sent_packet
    (update-only, no-op if the packet was already removed), NOT
    report_sent_packet (insert-capable). Using the insert path would
    RESURRECT a packet that a concurrent drop_stream just removed+released,
    and a later ACK/abandon of that zombie would release its bytes a SECOND
    time (flight-size under-count). See #4345 stage-2 review.
  - Retransmissions MUST stay bounded. Do not make retransmit infinite again.
```

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

`transport/rolling_rtt_stats.rs` maintains a process-wide
`SHADOW_RTT_REGISTRY` (`DashMap<SocketAddr, Arc<dyn RttSnapshotProvider>>`)
populated by `RollingRttStatsHandle` in every `RemoteConnection`. A
1Hz aggregator spawned from `p2p_impl.rs` (registered with
`BackgroundTaskMonitor` as `shadow_rtt_aggregator`) emits a
`shadow_rtt_aggregate` event both as `tracing::debug!` (file-log
mirror; visible via `RUST_LOG=…=debug` in debug builds, compiled
out entirely in release builds via the `release_max_level_info`
feature in `crates/core/Cargo.toml`) and via
`send_standalone_shadow_event_with_peer_id` so it reaches the OTLP
collector regardless of log level, tagged with the local node id
so the collector can disaggregate samples per reporting node. All
five shadow emitters use the `_shadow_` variant so the telemetry
rate limiter admits them under a low-priority sub-budget and they
cannot starve operational telemetry (#4380).

`transport/reference_ping.rs` runs an analogous 1Hz loop
(`reference_ping` background task) that probes a fixed external
target (default `1.1.1.1:53`) over UDP with a synthetic DNS query
and feeds the RTT into a parallel `RollingRttStats`. It emits
`shadow_reference_ping` events with the same shape and the same
local-peer-id tag. The point is to separate "overlay multi-hop
queueing baseline" (visible only in the per-peer signal) from
"local uplink contention" (visible in both signals simultaneously),
which the Phase 1 analysis posted on #4074 showed Phase 1 alone
cannot answer.

The reference-ping spawn is **opt-in**: gated by
`telemetry.reference-ping-enabled` (default `false`). Production
gateway configs set it to `true`; developer machines and
integration tests leave it off so they don't fire DNS traffic
from CI runners (which would perturb timing-sensitive multi-node
tests — see PR #4292 root-cause). The shadow aggregator is
always-on; only reference-ping is gated.

### Transport-redesign baseline telemetry (`transport_snapshot`)

`TRANSPORT_METRICS` carries period counters for loss and ACK overhead
(`packets_sent`, `packets_retransmitted`, `packets_abandoned`,
`rto_expiries`, `tlp_probes`, `standalone_ack_*`), NAT failure causes
(`nat_fail_*`, which sum to `nat_traversal_failed_error + _version` with a
different version/non-version split, plus up to 8 per-attempt
`nat_fail_samples` per period with the sub-reason and what the attempt saw;
OUTBOUND traversals only), an endpoint-dependent-mapping signal
(`observed_addr_*`: each handshake ack's view of our address, compared only
across DIFFERENT reporters in the same address family, IPv4-mapped v6
treated as v4; counts only, never the address; per period, so it resets; a
NAT rebinding between two reporters' reports is indistinguishable from
EDM), final-ACK timing (`transfers_acked`, `*_transfer_ack_time_ms`) and
the listener-to-connection queue (`inbound_queue_depth_hwm`, `_drops`) and
inbound stream refusals / evictions by reason (`inbound_stream_<label>`, one
per `InboundStreamOutcome`; the never-reset twin feeds the opt-in OTel
`freenet.transport.inbound_stream_limits`, which must not observe the period
counters), NAT traversals by why they were started (`nat_attempts_<label>`,
`nat_established_<label>`, one per `NatAttemptOrigin`, and `origin` on each
`nat_fail_samples` entry; the origin is threaded internally from each
`NodeEvent::ConnectPeer` through `OutboundConnector::connect` and
`ConnectionStart`, never on the wire; a dial to a configured gateway is
always `gateway`, and `ring_peer_no_transport` is the event loop's dial to a
ring peer whose transport was lost, since cached-peer joins never dial
(#5954)), intro packets a non-gateway drops as unknown (`stray_intro_<label>`:
the exact address's traversal timed out (no_response, undecryptable or
incomplete; not other failures) in the last 10 s,
a traversal to another port of the IP is running (endpoint-dependent mapping
OR another peer behind the same IP, e.g. CGNAT; it cannot tell them apart), or
neither; the recent failures are a listener-owned table bounded by count and
TTL, and early evictions are counted in `stray_intro_failures_evicted`) and CONNECT
joiner dial-queue waits (`dial_queue_wait_<label>`, coarse buckets around the
3 s punch window) and inbound datagrams by first byte (`demux_<label>`, one per
`DemuxClass`: `legacy` passes through, `quic` and `other` are counted and
dropped or, for `quic`, queued for the QUIC ingress; each `quic` one is also
counted once by what happened to it, `quic_ingress_<label>`, one per
`QuicIngressOutcome`; see "Socket Abstraction Rules") and outbound datagrams by how the
node-wide egress cap paced them (`egress_<label>`, one per `EgressDecision`,
plus `egress_bytes`, `egress_bytes_waited`, `egress_wait_us_sum` / `_max`,
`egress_long_waits_<cause>`, `egress_streams_abandoned_<cause>`,
`egress_streams_refused_<cause>`, `egress_debt_hwm_bytes`,
`egress_debt_forgiven_bytes`; all zero without
`total-bandwidth-limit`), QUIC upgrade control messages received and
ignored (`upgrade_control_received`) and hole-punch handshake packets dropped,
one count per packet, because they were not encrypted under the attempt's
inbound session key (`handshake_unauthenticated_dropped`), QUIC endpoint deaths
(`quic_endpoint_down`) and QUIC connections this node closed, by close code
(`quic_close_sent_<label>`, one per `QuicCloseCode`). Each label set is declared once in a
`labelled_counters!` list, so a new variant needs a snapshot field. Hot
path writes are single relaxed atomics; `SentPacketTracker` keeps an
`rto_expiries` count and a bounded per-stream final-ACK watch, both
observation only.

Per-connection inbound concurrency (`inbound_concurrency_*`,
`peer_connection/inbound_concurrency.rs`) estimates, at each inbound short
message and each newly admitted inbound stream, the QUIC uni streams the
connection would hold: open inbound streams plus packets still queued for it
plus short messages that arrived within SRTT + 25 ms. Arrival is when the
listener queued the packet (`InboundPacket::queued_nanos`, on the listener's
clock, which its connections share), stamped at two sites:
`try_send_established`, and `forward_handshake_buffered` for packets a gateway
handshake buffered (those share one stamp, which raises the estimate for a new
gateway connection). Arrivals are remembered for a fixed 2 s horizon (at most
1024), so a growing SRTT can count earlier ones again. Per snapshot window
(`transport_snapshot_interval_secs`, default 30 s) it reports the connections
sampled (only those that received a short message or had a stream admitted in
the window), how many peaked at 8 / 16 / 32 / 64 / 128 / 256 or more, and the
largest estimate; keys are uploaded only when non-zero, and `conns_sampled` is
the only activity term. It is an estimate built to err high, not a strict
upper bound; the known undercounts (a sample skipped at a window boundary,
arrivals older than the horizon or past the 1024 cap, streams the 1024-stream
budget refused) are listed in the module doc. So `ge_T / conns_sampled` can
support LOWERING `MAX_UNI_STREAMS` (`quic/config.rs`), never raising it; a
limit that is too low costs only sender back-pressure, and QUIC shadow mode
(e5a) is expected to confirm the value on real QUIC before any default
changes. It says nothing about the memory a peer opening streams on purpose
can hold (the per-path ceiling's job). `inbound_concurrency_max` is diagnostic
only (one peer can drive it to the cap with a burst or an inflated RTT). The
counts sit under one mutex taken only on a connection's first sample of a
window or a new peak, and by the snapshot, which advances the window and
drains them together; do not split them back into separate atomics, which
let a sample racing the snapshot produce rows with `ge_T > conns_sampled`.

`CONN_RTT_REGISTRY` (`rolling_rtt_stats.rs`) holds one `ConnRttGauge` per live
connection (owned by `RollingRttStatsHandle`) for the snapshot's
cross-connection SRTT / min-RTT percentiles. A connection's snapshot SRTT may
be an unloaded keep-alive value; for bufferbloat analysis use the per-transfer
`final_srtt_ms` vs `min_rtt_ms` on `transfer_completed`. Same rule as the shadow
registry: written from the recv loop, read only by telemetry, NEVER by the
data path.

Transfer events take `final_srtt_ms` and `total_timeouts` from the
connection's sent-packet tracker, not the congestion controller (FixedRate
reports zero RTT/loss), add `min_rtt_ms` and `peak_flightsize_bytes`
(queueing delay), and report cwnd as null when the controller has no
bounded window. `Ring::prune_connection` and `NodeEvent::DropConnection`
take a `DisconnectReason`: pass the real cause (`Timeout` for the idle
timeout, `NetworkError` for transport failures, `Pruned` only for local
topology management; the full list is on `DisconnectReason`). Disconnect
byte totals are this connection's: the ring `Connection` records the
per-peer totals when it joins and the event reports the difference.

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

`transport/shadow_demand.rs` adds the *demand* side of the floor
analysis. Three process-wide observation hooks are written from the
hot send path with a single relaxed atomic add each, and read only
by the 1Hz aggregator tasks:

- **Outbound class counters** (`record_outbound`): cumulative bytes
  split into must-flow / short / bulk. `classify()` keys off the
  `SymmetricMessagePayload` variant — `StreamFragment` is `Bulk`
  (the only class the token bucket meters, i.e. the only slowable
  traffic), `ShortMessage` is `Short` (opaque serialized `NetMessage`
  — small contract op or control plane, unsplittable at the transport
  layer), and `Ping`/`Pong`/`NoOp`/`AckConnection`/`Upgrade` are
  `MustFlow`.
  Tagged at `packet_sending` (covers `ShortMessage` /
  `StreamFragment`) plus the bypass sites `send_pong`, the
  keep-alive `Ping`, and the ack-only `NoOp` (`PeerConnection::noop`,
  untracked since #5795). The split is a classified *subset* of the node
  total: it excludes retransmits, handshake/intro `AckConnection`, and
  standalone connection ACKs, which are still counted in
  `cumulative_bytes_sent` (the `shadow_rate_demand` total) but not the
  per-class breakdown.
- **Broadcast-queue depth gauge** (`record_broadcast_queue_depth`):
  updated by `node/network_bridge/broadcast_queue.rs` under its own
  queue lock, so the shadow reader never contends on the queue mutex.
- **Global-bandwidth handle** (`register_global_bandwidth`): a `Weak`
  to the node's `GlobalBandwidthManager`, registered at construction
  in `p2p_protoc.rs`, so the demand aggregator can read the effective
  aggregate rate `R` — present only in global-pool mode
  (`total_bandwidth_limit` set); null under the default per-connection
  FixedRate mode.

Two always-on 1Hz tasks (spawned from `p2p_impl.rs`, registered with
`BackgroundTaskMonitor` as `shadow_demand_aggregator` /
`shadow_outbound_class_aggregator`) emit `shadow_rate_demand`
(achieved throughput vs `R`, active-connection count, broadcast-queue
depth) and `shadow_outbound_class` (the must-flow / short / bulk byte
split). Same OTLP shape and peer-id tagging as `shadow_rtt_aggregate`.

`transport/shadow_iface_tx.rs` adds the OS-interface counter: a 1Hz
read of Linux `/proc/net/dev` summing non-loopback `tx_bytes`, emitting
`shadow_iface_tx` with the derived `op = total_tx − freenet_own_tx`
(`freenet_own_tx` = `TRANSPORT_METRICS.cumulative_bytes_sent`). It is
**opt-in and best-effort**: gated by `telemetry.iface-tx-enabled`
(default `false`, same gating as reference-ping); if `/proc/net/dev`
is unavailable the tick is silently omitted, never blocking.

The class tagging is the only Phase 1.6 hook that touches the hot send
path; it is a single relaxed atomic add per packet (no allocation, no
lock, no blocking). Everything else is read from the separate
aggregator tasks.

```
NEVER read SHADOW_RTT_REGISTRY, cross_connection_median_inflation,
the reference_ping stats, OR the Phase 1.6 shadow_demand counters /
broadcast-depth gauge / registered bandwidth handle from the
production data path (rate limiter, retry, congestion control). The
hot path only WRITES the cheap class counters; all reads happen in
the aggregator tasks. They exist only for the staged rollout in
#4074:
  Phase 1   → observation only — per-peer overlay RTT (current)
  Phase 1.5 → observation only — adds reference-path RTT + peer_id
              tagging so signals can be disaggregated per node and
              the overlay-vs-uplink confound can be tested
  Phase 1.6 → observation only — adds outbound demand vs R, broadcast
              queue depth, OS interface tx (op = total − own), and the
              must-flow-vs-bulk outbound split
  Phase 2   → shadow controller, still no behaviour change
  Phase 3   → opt-in flag
  Phase 4   → default switch only after Phase 3 shows improvement
```

## Connection Lifecycle Rules

### WHEN establishing connections

```
NAT traversal:
  1. Send multiple intro packets — `NAT_TRAVERSAL_MAX_ATTEMPTS` (40 in release builds, 10 under `cfg(test)`) at a ~250 ms cadence (200 ms receive wait + 50 ms `INITIAL_INTERVAL`), capped by a 3 s `overall_deadline` (so ~12 attempts at production settings)
  2. Use exponential backoff: 50ms → 300ms → 1s → 5s
  3. First relay observes external address (ObservedAddress msg)
  4. Rate limit: 1 intro packet/second per source IP
  5. Gateway ramp-up: 5/s for 30s, 20/s for 2min, unlimited after
     (prevents thundering herd after gateway restart)

Handshake send failures (#5871):
  → In the intro resend loop a failed `send_to` counts as a lost datagram,
    not an abort. When the attempt runs to its deadline, it ends with
    `SendFailed` if the last send failed, and the final warn carries
    `failed_sends` and the last OS error either way.
  → The gateway success ack and the joiner's final ack are one-shot and fail
    via `SendFailed` at once. The best-effort version-mismatch ack only
    logs, so the peer is still recorded as outdated.
  → Never map a send error to `ChannelClosed`.

Hole-punch completion (traverse_nat's RemoteInbound arm, the side that
answered the remote's intro):
  → Completes only on a packet encrypted under this attempt's inbound
    session key. The success ack is unchanged here; a follow-up changes it.
  → Any other packet is dropped and counted once per packet
    (`handshake_unauthenticated_dropped`). The attempt goes back to
    receiving without sending, within the current receive window, so
    dropped packets cause no extra acks and do not delay the paced ones.
    It stays pending until a packet under the key arrives or the
    deadline passes.
  → Every release encrypts the packets that complete this arm under that
    key, so the check needs no version gate and no wire change. Tests run
    a peer with the earlier rule (complete on any non-intro packet)
    through LEGACY_HANDSHAKE_COMPLETION to check interop both ways, and
    REMOTE_INBOUND_COMPLETIONS records which peer completed in this arm.
  → Its logs, like every traverse_nat log, carry direction = "outbound"
    even though this side is answering; the drop log adds
    arm = "remote_inbound".

Restart detection (all peers, not just gateways):
  → If an intro packet arrives on an established connection, attempt
    asymmetric decryption (rate-limited to 1/sec per IP).
  → If valid: the remote peer restarted. Tear down stale session and
    accept a fresh handshake (server-side, same as gateway_connection).
  → This lets restarted peers reconnect via existing NAT holes without
    full gateway re-bootstrap (#3671).
```

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

```
Which packets earn a receipt is decided by `receipt_policy` in
peer_connection.rs. A receipt is only useful for a packet the SENDER
tracks for retransmission:
  → Ping / Pong: never acked (every release sends them untracked).
  → AckConnection*: acked on every arrival but NOT recorded for dedup.
    The handshake's completion ack is tracked at packet id 0 and must be
    acked; id 0 is also the remote's first data packet, so recording it
    would drop that packet as a duplicate. Answering a connection ack with
    `ack_ok` happens at most ONCE per connection: the reply is itself an
    AckConnection, so answering every one loops forever between two peers.
  → ShortMessage / StreamFragment / Upgrade: acked within
    ACK_CHECK_INTERVAL (100 ms), and RE-ACKED when a duplicate arrives (a
    duplicate means our receipt was lost). A duplicate is never delivered
    (or, for Upgrade, processed) twice.
  → NoOp: acked only for a remote below UNTRACKED_ACK_NOOP_MIN_VERSION
    or of unknown version (old releases track their ack-only NoOps).
    From a remote at/above the floor it is fire-and-forget.
  → Receipts that do not fit beside a payload go in separate untracked
    NoOps with FRESH ids; only the payload is tracked under its id.
  → Our own ack-only NoOps (`PeerConnection::noop`) are NEVER tracked
    and never touch flight size. Receipt reliability comes from the
    remote retransmitting + our duplicate re-ack, as TCP does for a
    lost pure ACK.

DO NOT make ack-only NoOps tracked again, and DO NOT ack NoOps from a
capable remote: either restores the ack-of-ack ping-pong that cost
~5 pps per direction on every connection, idle ones included.
DO NOT stop re-acking duplicates: it is the only recovery for a lost
receipt now that acks are untracked.
A receipt for an id we no longer track (an old peer acking our
untracked NoOp, a re-ack of an already-released packet) is routine.
report_received_receipts must keep emitting ack-info ONLY for ids still
in pending_receipts, so such receipts never touch flight size.
NAT keepalive does NOT depend on any of this: the keepalive task's
5 s Ping (and the remote's Pong) keeps every mapping open.
```

### QUIC upgrade control message (#5940 step e1)

```
SymmetricMessagePayload::Upgrade(UpgradeControl) is bincode index 7,
appended after AckConnectionV2. UpgradeControl is Offer { attempt, caps },
Accept { attempt } or Decline { attempt, reason }.

Decode-only until step e5: nothing sends it. A node that receives one acks
it like a ShortMessage, counts it (upgrade_control_received in
transport_snapshot) and ignores it. It never replies and never closes the
connection.

Version gate: send it only when may_send_upgrade(remote_version) is true,
i.e. the remote is at or above QUIC_UPGRADE_MIN_VERSION (0.2.149). None
(unknown) fails closed. Peers at or below 0.2.147 panic on a payload they
cannot decode and 0.2.148 drops it, so the decoder must ship in a release
before any release sends the message. A reply to a received Offer needs no
gate. The floor follows the wire-gated-floor rule in docs/RELEASING.md
(QUIC_UPGRADE_SHIPPED_IN marker).

Append only, at both levels: a new SymmetricMessagePayload variant goes
after the last one, and existing variants keep their bytes
(pre_upgrade_variants_keep_their_wire_bytes). A new UpgradeControl variant
cannot be decoded by an e1 node, so it may be sent only behind a caps bit
the receiver advertised; receivers ignore caps bits they do not know.
Never append a field to an existing variant: bincode accepts trailing
bytes, so an e1 decoder silently ignores it. New data goes in a new
caps-gated variant.

Not on the handshake path: in traverse_nat (connection_handler.rs) the
joiner decodes with `?`, so an undecodable packet there still aborts the
traversal. A sender must not send Upgrade, or any other new payload, before
the connection is acknowledged (a constraint for e5).
```

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

```
Rate calculation:
  final_rate = min(ledbat_rate, global_pool_rate)

Token reservation:
  → Reserve BEFORE sending
  → Block if insufficient tokens
  → RTT-adaptive update interval
  → With the node-wide egress cap, capped stream senders also reserve (in
    order) and keep the reservation until the node-wide take succeeds,
    giving it back if the fragment is not sent; see "Node-wide egress cap"
```

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

`bind_udp_listener` (`connection_handler.rs`) binds `DemuxSocket<S>`
(`transport/demux.rs`), so every socket the listener reads, real or
`SimulationSocket`, splits inbound datagrams by first byte: `0x01` / `0x02`
(the UDP transport's own type bytes) pass through unchanged, a byte with a
`0xC0` bit set is QUIC-shaped, anything else (including an empty datagram) is
other. QUIC-shaped and other datagrams are counted (`demux_<label>`) and
dropped. The QUIC rule is exact only with QUIC bit greasing (RFC 9287)
disabled. Tests that inject a hand-written socket straight into the listener
bypass the wrapper.

For a QUIC endpoint on the same port (#5940), `enable_quic_ingress` gives the
wrapper a bounded queue (`QUIC_INGRESS_QUEUE_DATAGRAMS`, a declared byte
budget `QUIC_INGRESS_QUEUE_BYTES` in `declared_cache_ceiling`) of QUIC-class
datagrams with their source. The listener stays the ONLY reader of the UDP
socket and fills the queue with `try_reserve`, never a waiting send, so a slow
endpoint costs dropped QUIC datagrams and never stalls the listener. Only
sources on the allow-set (`allow_quic_source`, normalized addresses) are
queued; the rest are dropped, so a prober gets no QUIC reply. One source
holds at most QUIC_INGRESS_PER_SOURCE_DATAGRAMS (a quarter) of the queue, so
one allowed or spoofed-allowed source cannot starve the other connections.
The trade-off: a lone bulk peer, with no other connection competing, still
gets only a quarter of the queue, so a burst larger than 256 datagrams that
the endpoint has not yet drained is dropped (`_source_full`) even when the
rest of the queue is empty.
`allow_quic_source` returns a QuicSourcePermit: the address stays allowed
while a permit for it is alive, and the set holds at most
QUIC_ALLOWED_SOURCES_MAX addresses (past it, `allow_quic_source` refuses; a
new address reserves its place atomically). `allow_existing_quic_source`
grants another permit only for an address allowed at that moment, in one
step, so `accept` cannot bring back an address whose last permit was just
dropped.
The QUIC endpoint holds one permit per connection until quinn reports the
connection closed, whatever closed it, and dropping its connection handle
closes it; an address on the set is bounded by its connection's life, which
inherits the #5922 gap for a connection held open indefinitely. Each
QUIC-class datagram is counted as `quic_ingress_forwarded`, `_not_allowed`,
`_queue_full`, `_source_full` or `_no_ingress` (no ingress enabled, its
receiver gone, or disabled; every node with `transport-quic` off). The QUIC
endpoint enables the ingress through `with_quic_ingress`, which disables it
again (allow-set emptied and closed) if the endpoint cannot be built, so a
failed `prefer` bind leaves the socket as with QUIC off.

Socket-health signal: the listener reads through `Socket::recv_classified`,
which `DemuxSocket` answers with `Datagram` for a legacy datagram and
`Diverted` for one it consumed. The listener calls
`RecvErrorTracker::on_success` for both, so any datagram the socket delivered
resets the unclassified-error run that would end the listener. Do not move the
QUIC read onto a second reader of the socket; the ingress queue exists so
there is one.

The same wrapper charges outbound datagrams to the node-wide egress cap (see
"Node-wide egress cap" above). A new send site needs nothing to be charged;
it must not wait unless it is a pacing point.

#### QUIC endpoint (`transport/quic.rs`, #5940 step c2)

`transport-quic` (config key, `off` by default, or `prefer`) decides whether
`bind_udp_listener` builds a quinn endpoint over the DemuxSocket. Off: no
endpoint, no QUIC ingress, nothing changes. Prefer: the endpoint is built
with the node's QUIC identity (below) and an EMPTY allow-set, so nothing
reaches it and it sends nothing; the listener holds it. Upgrading a UDP
session to QUIC comes later, so only tests open QUIC connections. An unknown
value is off with a warning, never an error: a crash-loop rollback
reinstalls an older binary, which must start with a value a later release
wrote, and keeps it in config.toml.

```
QuicSocket (quic/socket.rs) is quinn's AsyncUdpSocket:
  → receive: reads the ingress queue only, one datagram per call, NEVER an
    error (quinn's endpoint driver stops on one); an IPv4 source is given
    as ::ffff:a.b.c.d on an IPv6 socket, the form quinn connects to there
  → send: one datagram per transmit (max_transmit_segments 1, no GSO);
    with a cap, try_take(len) first: short → WouldBlock, nothing charged;
    socket WouldBlock → the take is returned; any other send error is
    counted and reported as sent (quinn's connection driver stops on one)
  → UdpPoller: Ready when the socket can send AND poll_admit(1200) is
    Ready; its AdmissionWaiter sits in a Mutex only so the poller is Sync
QuicRuntime (quic/runtime.rs): now/new_timer on the TimeSource, spawn on
  GlobalExecutor. A timer reset to a later deadline keeps its armed sleep
  (VirtualSleep has no Drop deregistration) and re-arms when it fires.
config.rs sets every quinn and rustls value; tests pin each one quinn
exposes (its module doc lists the few it does not):
  grease_quic_bit(false) (the demux rule depends on it), versions [1],
  max_udp_payload / initial and min MTU 1200, no MTU discovery, no GSO,
  keepalive 15 s (None when the TimeSource disables keepalive), idle = the
  TimeSource's connection idle timeout, Cubic, 256 uni / 1 bidi streams,
  stream / receive / send window = clamp(5% of memory / (2 x
  max-number-of-connections), 128 KiB, 2 MiB), no unreliable datagrams, no
  spin bit, migration off, max_incoming 64, incoming buffers 64 KiB / 1 MiB,
  no validation tokens, rustls ring, TLS 1.3, fixed ALPN, no SNI, no
  resumption, no early data. Keys and seeds (reset key, token key, CID key,
  quinn's rng_seed, initial destination CIDs): keys from the OS, seeds and
  CIDs from GlobalRng (see below).
Connection IDs (quic/cid.rs) carry a keyed BLAKE3 MAC, so quinn drops a
  short-header packet with a forged ID silently; quinn's own hashed
  generator can be spoofed and would answer it with a stateless reset.
```

QUIC memory: the flow-control windows (connection receive, stream receive,
send; all one value from `connection_window_bytes`) are clamped and capped
at `MAX_INBOUND_STREAM_BYTES_PER_CONNECTION`; the stream window never
exceeds the connection window. The cap does not bind today (2 MiB clamp);
`windows_never_exceed_the_reassembly_allowance` only guards future constant
changes. The windows are the same class of per-connection exception as
inbound stream reassembly and are in `NOT_SUMMED` in
`declared_cache_ceiling`, but they are NOT covered by it: they are in
addition to reassembly, the send window is outbound, and quinn's own
per-connection state (up to 256 uni streams, packet and ACK bookkeeping,
assembler, crypto buffers) is not bounded by them. That memory, about twice
the window per connection plus quinn state, is NOT accounted in the node
aggregate and must be before QUIC is on by default (rollout gate). The fixed
node-wide endpoint buffers are summed whatever `transport-quic` says:
`INCOMING_BUFFER_TOTAL_BYTES` (quinn's pending attempts) and
`QUIC_INGRESS_QUEUE_BYTES`.

The cap's `poll_admit` and `give_back` publish through Release/Acquire
atomics, not SeqCst, so a give-back racing a waiter's registration can miss
its wake (the store-buffer pattern); the waiter's own timer bounds the delay
to its deficit plus its stagger (one datagram's refill per waiter ahead of
it). Known and accepted.

The dual-stack test skips (printing SKIPPED) on a host without dual-stack
IPv6; set REQUIRE_DUAL_STACK=1 to make that a failure. Nothing sets it in
CI, so CI does not check that the test ran.

Key material (stateless-reset key, token key, CID MAC key) comes from the OS
entropy pool, never GlobalRng; quinn's rng_seed and the CID nonces stay on
GlobalRng. With `prefer`, a failed endpoint build logs a WARN and the node
runs with QUIC off. `accept` returns the attempt without awaiting its
handshake (QuicIncoming::establish), so one stalled handshake does not hold
back the next. QUIC integration tests run under a watchdog thread
(`bounded`), so a quinn driver that spins fails the test instead of hanging
`cargo test`.

In tests, two endpoints on SimulationSocket with one VirtualTime and a
seeded GlobalRng repeat the same event trace; the bytes differ, because
rustls's key exchange and the XEdDSA nonce use the OS RNG.
`quic/test_tls.rs` holds only a plain Ed25519 identity for tests where a
peer presents a key that is not its transport key.

#### QUIC identity (`transport/quic/identity.rs`, #5940 step c3)

```
A node authenticates in the TLS 1.3 handshake with a raw public key
(RFC 7250) made from its EXISTING X25519 transport key. No new key, no file,
no extra message, no exporter proof.
  → Signing: XEdDSA (Signal's spec, the `xeddsa` crate) with the X25519
    secret, as Ed25519 (scheme 0x0807). Nonce randomness from the OS
    (SysRng), NEVER GlobalRng (code-style.md OsRng exception). Debug of the
    signing key, signer and identity shows the public key only.
  → Presented key: the Edwards form of the X25519 public key with SIGN BIT 0,
    in an Ed25519 SubjectPublicKeyInfo. Sign bit 0 is XEdDSA's convention:
    the signer negates its scalar when its natural Edwards key has sign 1.
    PeerKey::of refuses (UnusableKey) any key no node can hold: a
    non-canonical encoding (bit 255 set or u >= p), a small-order Edwards
    point (forgeable without a secret, since Ed25519 verification does not
    check the key's order), a point with a torsion component (would let one
    secret authenticate as up to 8 keys), and u = -1 (no Edwards form).
  → Verification is PINNED to the key the caller expects: the presented
    SPKI must equal the expected key's exactly (public bytes; ct_eq only
    for uniformity; no chain), the scheme must be Ed25519, and the
    signature is checked with ring against the EXPECTED key. Anything else fails the handshake.
  → Client: connect(addr, &expected_server_key) builds TLS pinned to it.
  → Server: allow_peer(addr, &expected_client_key) puts the address on the
    allow-set AND records the key (QuicPeerPermit; a second key for an
    address already held is KeyConflict). accept() looks the key up by the
    attempt's (normalized) address and accepts with a per-attempt server
    config whose TLS is pinned to it (Incoming::accept_with); an attempt
    with no recorded key is ignored. The endpoint's own server config
    (needed so quinn passes Initials to accept) refuses every client.
  → When the SERVER refuses the client, the client may see connect()
    succeed: TLS 1.3 lets it finish before the server's verdict, and the
    server's CONNECTION_CLOSE ends the connection at once. The server never
    gets a connection from an unexpected key.
Tests: conversion vs curve25519-dalek 5 (random keys, bit 255 set, u = -1),
signer key == verifier key over many keypairs with both natural sign bits,
signatures verified by ed25519-dalek and refused with any bit flipped,
nonce differs under one GlobalRng seed, handshakes in both directions, and
refusal of a wrong server key, a wrong client key, a plain Ed25519 key on
either side, and a man in the middle with its own key on either leg.
```

#### QUIC framing and path (`quic/framing.rs`, `quic/path.rs`, #5940 step e3)

Dead code until e5: nothing in the node builds a `QuicPath`.

```
One message per QUIC uni stream: an 18-byte header, then metadata, then
payload, then the stream's end.
  version u8 (1) | kind u8 (0 Short, 1 Stream, 2 Control) |
  stream_id u32 BE | total_len u64 BE (payload) | metadata_len u32 BE
FrameHeader::decode refuses, never panics: unknown version or kind;
  total_len 0 or > MAX_STREAM_TOTAL_BYTES (check_declared_stream_length)
  for Short and Stream; Control 0 or > MAX_CONTROL_BYTES (4 KiB);
  metadata_len > MAX_METADATA_BYTES (1120, what UDP embeds in fragment
  #1, the shared embeds_metadata predicate; the sender leaves larger
  metadata out of the frame, and the caller sends the announcing message
  separately as a short message, as on UDP; empty metadata arrives as
  None, deliberately); metadata or a nonzero
  stream_id on Short or Control; a Stream id that is not caller-named;
  fewer than 18 bytes. The sender builds headers through the same
  checks. A new kind or field needs a new version, sent only to a peer
  that said it reads it.
Sending (QuicPath::send_short / send_stream / send_stream_chunks /
  send_control): returns when the peer's QUIC stack acked every byte, or
  Stopped(code). A message that fits in the flow window can be acked
  before the receiver reads its header, so a refusal may read as success
  at the sender, as on UDP, and so may one dropped for a full channel:
  sent is NOT delivered. send_stream_chunks resets the stream if the
  source fails or delivers a different length. quinn's 256 uni streams
  are the back-pressure (open_uni waits; quinn returns credit only after
  more than 32 streams are freed).
  Sends have no deadline of their own: open_uni, a flow-blocked write
  and the final wait for acks can wait as long as the connection lives
  against a peer that does not cooperate. e5 callers MUST bound every
  send with a timeout. Dropping a send future resets its stream (Drop on
  Outgoing; quinn's own drop would FINISH it, which the receiver reads as
  a stream that ended early). A send dropped while waiting for acks after
  finish is not reset and may still be delivered: a timeout there means
  "maybe delivered".
  Each locally reset stream leaves one Arc<Notify> in quinn's per-
  connection `stopped` map until the connection closes (quinn 0.11.12:
  stopped() registers it, and a local reset produces neither the Finished
  nor the Stopped event that removes it). Bounded by the sends aborted on
  the connection; e5 must bound that or recycle the connection.
  Every write, and the wait for a source's next chunk, races the
  stream's stopped() future and resets on a stop:
  quinn-proto 0.11.19 does not report STOP_SENDING to a writer blocked on
  CONNECTION flow control, so without the race a stopped stream keeps the
  connection's window and blocks every other stream on it. Do not write
  to a quinn SendStream without that race.
Receiving: one task per connection reads every stream (FuturesUnordered,
  no task per stream) and only uses try_send. If that task panics it is
  counted (receive_task_panicked) and the connection is closed with
  close code DOWNGRADE (the peer falls back to UDP). Short
  and Control go to bounded channels (full: dropped and counted); Stream
  goes through the INBOUND ADAPTER (path/inbound.rs).
Inbound adapter: a Stream's payload goes into an ordinary StreamHandle,
  re-chunked to the UDP fragment shape (fragment i = bytes
  [(i-1)F, iF), F = FRAGMENT_PAYLOAD_SIZE), so fork / assemble / pipe /
  StreamIdentity / registry are unchanged. The handle is announced
  (InboundStreamArrival, with the metadata) before the payload.
  → Before allocating: header checks, then the path's
    InboundStreamBudget check and charge (same cost and limits as UDP).
    Short messages are charged too (a token rides with the message until
    its payload is taken). Refusal: STOP_SENDING (stop_code), counted in
    the shared InboundStreamOutcome counters, path and process-wide.
  → Completion by BYTES: exactly total_len, then the stream's end. The
    fragment that completes the buffer is held until the end is seen.
    Ends early (Truncated), more bytes (Overlong), reset by the sender:
    the handle is cancelled (consumers get Cancelled) and it is counted.
  → Charge released only when all state is gone (Weak), as on UDP. The
    reader's guard owns its only clone, so the charge goes when the
    reader ends if no consumer holds one; a consumer that drops later is
    not observed, and the charge goes at the next check() that needs
    room or when a stream with the same id arrives (release_if_gone
    before the StreamIdInUse test). An id is held from admission until
    then, and is free again afterwards (completed ids are not
    remembered).
  → No chunk is kept: quinn's chunks are slices of the ~1200-byte
    datagram, so header, metadata, short and control reads copy into one
    buffer that grows as data arrives (from 64 KiB, never past the
    declared length, exact at the end; a declared length is not allocated
    up front). Stream payload goes into fragments, each a fresh buffer of
    exactly its length; only a chunk of a whole fragment or more is kept
    as a slice (const assert: MAX_UDP_PAYLOAD <= 1.2 x fragment), except
    after quinn-proto's peer-triggerable Assembler::defragment merges data
    into one allocation of up to the stream window, which such slices then
    share (inside the charge while the inbound state lives). e5: a
    fragment Bytes from an inbound StreamHandle must not outlive the
    inbound charge (e.g. in an outbound pipe's retransmit state) without
    being copied. Streams
    with no charge (header phase, control frames) have an age limit too
    (inbound_stream_age_limit(0)).
  → Idle and age eviction: streaming_handle_idle_timeout and
    inbound_stream_age_limit, counted EvictedIdle / EvictedAge, checked by
    one timer per stream on the path's clock (not the timeout_check
    tick). Every QUIC chunk is new bytes, so every chunk is progress.
    In simulation each stream's last armed timer stays in VirtualTime's
    pending wakeups until its deadline (VirtualSleep has no drop
    deregistration); the timer is replaced only when it fires.
  → Refused ids are NOT remembered: a stopped QUIC stream cannot deliver
    more bytes, and a new QUIC stream with that id carries the whole
    stream. A Stream id already held on the path is refused
    (StreamIdInUse). Dropping the path cancels streams in progress.
Not here: dedup (e5: is_duplicate_dispatch, completed_inbound_streams),
  export of the QUIC-only counters (per path, QuicPath::stats), and one
  budget shared with the UDP session. The path has its own budget, so a
  peer with both may hold twice the per-connection allowance until e5
  resolves it (QUIC memory gate). Short-message budget keys are picked by
  the path and could equal a UDP transport-internal id: do not share the
  budget without separating the keys.
```

#### QUIC errors, close codes and liveness (`quic/error.rs`, `quic/liveness.rs`, #5940 step e4)

Dead code in production while QUIC carries no traffic: with `transport-quic`
off no endpoint is built and the transport reports `Disabled`.

```
Errors: TransportError::Quic(QuicError), beside TransportError::Udp and
  opaque like it (classify with scope()). quic/error.rs maps every quinn
  ConnectionError / ConnectError / WriteError / ReadError and the e3
  QuicSendError / HeaderError / QuicConnectError / QuicAllowError to one
  QuicErrorKind, with exhaustive matches (a new quinn variant does not
  compile until it has a kind); path/inbound.rs reads through the same
  mapping. Do not match quinn errors elsewhere to decide what to do;
  convert and match the kind.
  → Reset{code} (the peer reset a stream we read, or we reset our own
    send: SENDER_ABORTED when its source failed, LENGTH_MISMATCH when it
    delivered the wrong length) and Stopped{code} (the peer sent
    STOP_SENDING) are TRANSIENT, like UDP's OutboundStreamFailed, except
    the codes that refuse the message itself: HEADER_REFUSED, OVERLONG,
    FRAGMENT_REFUSED (the receiver's buffer refused a fragment; distinct
    from CANCELLED, which means the consumer is gone) and LENGTH_MISMATCH
    (stop_is_retryable, reset_is_retryable: classified per namespace, so
    a reset carrying a deterministic stop value, or a stop carrying
    LENGTH_MISMATCH's value, is retryable).
  → Stream scope (fails the same way again): those codes, InvalidFrame,
    StreamClosed (a local misuse of quinn's stream API).
  → Connection scope: ConnectionLost (peer closed with NODE_DROPPED_PEER
    or an unknown code, closed here, NO_ERROR, stateless reset),
    TimedOut, EndpointDown, Protocol{code} (any other transport error,
    including a LOCAL INTERNAL_ERROR: quinn raises it for a dropped
    endpoint driver, but quinn-proto 0.11.19 also raises it on a healthy
    endpoint for a peer leaving too many gaps in a stream or crypto
    buffer, so the code alone does not blame the endpoint),
    Refused{reason}:
    Handshake (a TLS alert, codes 0x100 to 0x1ff), Version,
    ConnectionRefused (CONNECTION_REFUSED), Downgrade and
    IdentityMismatch (the peer's close codes), AllowSetFull, KeyConflict,
    UnusableKey, Local.
  → Attribution: error::attribute(availability, kind) turns every
    connection-scope kind into EndpointDown while the endpoint is Down,
    whichever error it came from (a connection error, a read or write or
    path send that lost its connection, a connect), except
    Refused{IdentityMismatch}, a security signal kept visible;
    stream-scope kinds are unchanged. QuicEndpoint::error_kind applies it
    with the endpoint's availability: use it, not the bare mapping, to
    decide what to do (after endpoint death quinn reports an
    INTERNAL_ERROR or, once its implicit close ran, LocallyClosed).
    Window: quinn's EndpointDriver::drop ends the connections before the
    watcher records Down, so on a multi-thread runtime a connection can
    end as Protocol{0x1} while the availability still reads Up. e5 must
    not take an irreversible per-peer action on Protocol{0x1} without
    re-checking after a short timeout on availability().not_up() (which
    never returns while the endpoint is healthy).
  → A connection-scope QUIC error ends the QUIC path only. e5 handles it
    inside the transport (fallback to the UDP session, which stays up)
    and never surfaces it to operations.
Application error codes, one range per QUIC frame type, all one-byte
  varints; each module lists its codes in ALL, the compile-time check in
  quic/error.rs refuses an out-of-range or duplicated code, and a
  source-scrape test refuses a code constant missing from ALL:
    0x00..=0x0f CONNECTION_CLOSE: close_code::NODE_DROPPED_PEER 0,
                DOWNGRADE 1 (fall back to UDP), IDENTITY_MISMATCH 2
    0x10..=0x1f STOP_SENDING: path::stop_code (receiver refusals)
    0x20..=0x2f RESET_STREAM: path::reset_code (sender gave up)
  The scrape test checks every constant by name against ALL, and
  close_code::ALL against the QuicCloseCode variants.
  The close codes are wire values a peer on another release reads: never
  renumber them; add a new code at the next free value. A code the peer
  does not know reads as ConnectionLost. Every close the QUIC code sends
  goes through the connection's QuicCloser (QuicConnection::close and
  Drop, which sends NODE_DROPPED_PEER, and the path's receive
  supervisor, which shares the closer): a flag swapped once per
  connection lets exactly one caller, on any thread, close it, and only
  if it is still open, so a second or concurrent close neither replaces
  the peer's reason nor counts again. Never call quinn's close with a
  bare number.
Liveness ("alive" = the endpoint driver has not ENDED; it does not mean
  the endpoint is making progress): quinn spawns its endpoint driver
  itself and keeps no handle. QuicRuntime wraps the FIRST task quinn
  spawns, which is the endpoint driver (Endpoint::new_with_abstract_socket
  spawns it before returning; later spawns are connection drivers). When
  that driver's future RETURNS while the node holds the endpoint, the
  endpoint's QuicAvailability turns Down for good, once, with a WARN and
  quic_endpoint_down. Its connections end, accept() returns None and
  connect() fails as EndpointDown. The UDP transport is untouched.
  → NEVER feed endpoint death to ListenerExit: that exits the node.
  → What this covers is narrow. quinn's driver returns only on a receive
    I/O error and QuicSocket never returns one, so in production today it
    cannot return at all; tests inject the error (socket::ReceiveFault).
    The one real way it ends is a PANIC, which is NOT contained: quinn
    0.11.12 polls the driver holding its endpoint state mutex
    (endpoint.rs line 370), so a panic poisons it and every later lock
    panics too, in EndpointDriver::drop (line 403), Accept::poll (line
    651, the accept loop), connect (line 214) and the connection drivers;
    one of those running during the unwind aborts the process. A panic is
    never reported as Down. Containing a quinn panic (catch the unwind,
    leak the driver instead of dropping it, guard the accept loop) is a
    separate PREREQUISITE before QUIC is on by default.
  → When the node drops its last handle, the availability is Released
    (final; the first final state stored wins, so a release and a
    driver return racing never overwrite each other) and the driver's
    later return is not reported (QuicEndpoint's
    owner field is declared before the endpoint, so the release is stored
    first; quinn's handle count is Relaxed, but the last drop and the
    driver's return both take quinn's state mutex, which orders them).
    A driver task dropped unfinished (runtime shutdown) is not reported.
  → QuicAvailabilityWatch (QuicEndpoint::availability, and
    OutboundConnectionHandler::quic_availability, which is Disabled with
    no endpoint): get() any time, not_up() to wait. Disabled, Down and
    Released are final, and not_up() returns them at once every time.
    Availability is per endpoint, one per node (so per process in
    production, per node in simulation).
  → QuicEndpoint::spawn_accept_loop registers the accept loop with
    BackgroundTaskMonitor as quic_accept_loop. It holds an EndpointCore
    (no owner share), so it does not keep the endpoint alive: when the
    node drops its last handle it sees Released, drops the core (quinn's
    driver ends, the socket is let go) and stops. It also stops when
    accept() returns None or the consumer is gone (WARN). It PARKS
    instead of returning, because the monitor treats any end of a task as
    a node failure, and drops its Sender first, so the consumer's recv()
    returns None once drained. Attempts still queued each hold a quinn
    Incoming, which holds the endpoint: they keep quinn's driver and the
    socket alive until drained or dropped. Call it once per endpoint (a
    second call warns and starts nothing). It hands attempts on with
    try_send; a full queue ignores the attempt and counts it
    (accept_queue_full, WARN on the first and every 1024th). Cloning an
    EndpointCore keeps quinn's endpoint, driver and socket alive WITHOUT
    an owner share; only the accept loop holds one.
  → run_node drains the monitor (wait_for_any_exit) BEFORE the event
    listener opens the transport, so a task registered from the
    transport's open path is not monitored. register() now logs that at
    ERROR. e5 must register the accept loop through a monitor that is
    still live (not yet waited on) at that point.
Tests: one row per quinn variant and per stop/reset code in
  quic/error/tests.rs; each close code sent by either side and seen by
  the peer as that code and kind; the receive-task panic seen as
  DOWNGRADE; a driver ended by an injected receive error turns Down,
  counts once, attributes its connections' errors to EndpointDown,
  leaves the accept loop parked and the UDP side working; dropping the
  endpoint with the accept loop running releases the socket and reads
  Released, not Down; a full accept queue counts; the transport (on
  simulation sockets) reports Disabled with the key off, and Down with
  its listener still running after a death.
```

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

```
USE: LockFreeStreamBuffer (AtomicPtr<Bytes> slots, 256-slot chunks
     allocated on first insert; construction allocates only the chunk table)
  - contiguous_fragments: AtomicU32 for frontier tracking
  - Completion is measured in BYTES: received == total_size and every
    inserted fragment contiguous. Never use a fragment count.
  - Inserts past total_size are rejected, and so are empty fragments
    other than #1: older senders leave fragment #1 empty when
    embedded metadata fills the packet, and those peers can still connect.
  - A taken slot holds the TAKEN marker: empty to readers, a duplicate to
    inserts, filled to the frontier.
  - Lock-free insertion
  - Ordered delivery to consumer

Three stream types, never mixed:
  - inbound_stream::InboundStream: the UDP transport's reassembler for its
    OWN streams (oversized messages, StreamId::next_transport_internal).
    Transport-internal.
  - StreamHandle (with StreamRegistry): the UDP transport's handle for
    caller-named streams (StreamId::next). Transport-internal.
  - boundary::IncomingStream: what the registry, operations and node see.
    Above the transport, use only this; it wraps a StreamHandle today.
Never feed one stream to both reassembly paths.

Above the transport:
  - Match a stream to the connection that received it by its opaque
    boundary::StreamIdentity (IncomingStream::is), never by its buffer.
  - Read a stream incrementally with IncomingStream::chunks(). It has NO
    inactivity timer (bring your own), ends after its first Err, and its end
    does not prove the byte total was reached (#5445): count bytes yourself.

WHEN piping a stream onward (pipe_stream):
  Re-chunk the input (Rechunk in outbound_stream.rs). The receiver gives a
  stream ceil(total / FRAGMENT_PAYLOAD_SIZE) + 1 slots and refuses empty
  fragments other than #1, so every fragment but #1 and the last must be
  exactly full. Fragment #1 is the first chunk as received when it fits (a
  UDP-received stream keeps its exact boundaries); a longer first chunk is
  cut to leave room for this hop's metadata, as send_stream does.
```

### WHEN admitting or sending a stream

```
Stream length limits (streaming.rs):
  MAX_STREAM_TOTAL_BYTES = MAX_STATE_SIZE + WEBSOCKET_MAX_MESSAGE_SIZE
                           + envelope margin
  check_declared_stream_length(): 0 and > MAX_STREAM_TOTAL_BYTES are invalid

Receiver: every new stream passes check_declared_stream_length() and the
per-connection InboundStreamBudget (inbound_budget.rs) BEFORE any state is
created from its declared length.
  - Limits: MAX_INBOUND_STREAMS_PER_CONNECTION streams and
    MAX_INBOUND_STREAM_BYTES_PER_CONNECTION bytes (one maximum-size
    stream's cost plus headroom for ordinary streams).
  - A stream's cost is ChargeOverhead::cost(declared): its length, a fixed
    per-stream amount, and PER_FRAGMENT_OVERHEAD for every fragment number
    its length allows. Admission (check) and the charge use the same cost.
  - Both reassembly paths hold fragments as received and assemble once on
    completion: the memory held is the fragments received.
  - The bound is per connection; node-wide fairness is deferred to the
    transport redesign (exception recorded in bug-prevention-patterns.md;
    the flat constants are in executor.rs NOT_SUMMED).
  - A refused stream id is remembered (inbound_budget::RefusedStreams) until
    the age limit for its length, and its later fragments are dropped
    without being counted again. Do not use a forgetting cache. If a
    refusal cannot be remembered, the connection is closed. Refusals do not
    change ACK or dedup behaviour.
  - Completed streams are remembered (completed_inbound_streams) so a late
    retransmission does not start them again.
  - A charge is released only when ALL reassembly state is gone (observed
    via Weak: buffer clones for operations streams, the recv_stream task
    for legacy ones). Do not release on a single map removal.
  - Evicting a legacy stream cancels it through its LegacyStreamToken;
    dropping the sender alone is not enough.
  - Incomplete streams are evicted when idle or older than
    max_inbound_stream_age(declared), which scales with the length and is
    not refreshed by traffic; both paths, on the timeout_check tick, before
    the idle streaming-handle sweep. Never make the age limit a flat
    constant. "Complete" means all bytes contiguous, not a byte count. Only
    newly stored bytes refresh idleness.
  - On connection drop, its unclaimed operations streams are removed from
    the orphan registry.
  - Every refusal, eviction and refusal-related close is counted by reason
    in TRANSPORT_METRICS (cumulative_inbound_stream_outcomes, OTel
    freenet.transport.inbound_stream_limits) and per period in the
    transport_snapshot inbound_stream_<label> fields. A new reason is added
    once, in the InboundStreamOutcome labelled_counters! list; it then needs a
    snapshot field.
  - A complete operations stream is retired from streaming_handles and the
    registry at once, so only its consumers hold the charge.
  - LockFreeStreamBuffer::insert claims the slot BEFORE reserving bytes, so
    concurrent duplicates return Ok(false) without touching the reservation;
    a taken slot holds TAKEN, so fragments at or below the contiguous
    frontier are duplicates even after a reader has taken them.

Sender: send_stream / pipe_stream return OutboundStreamInvalidLength
(stream-scoped, NOT transient) for a length the receiver would refuse.
send_stream does not embed metadata in fragment #1 unless at least one
payload byte still fits; no fragment is ever empty.
```

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
