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
a correctness property of the cap. Then each attempt is: wait in the
node-wide cap's admission queue (StreamPacing::wait_for_admission over
EgressCap::poll_admit, the SAME queue the QUIC endpoint waits in); if it had
to wait, start again from the cwnd check, which the queue then admits at
once; then cwnd check and egress_cap::try_take back to back with no await
before the send (a take that still fails, because another sender took the
refill first, queues again). NODE-CAP ADMISSION IS IN ORDER: each waiting
stream and each waiting QUIC connection is one entry, and poll_admit admits
a waiter only when the bucket holds its datagram PLUS one for every waiter
queued ahead of it (counted up to MAX_ADMISSION_WAKERS), so a waiter that
polls first cannot take another's turn. Nothing is set aside: a waiter
ahead that has not polled yet takes its datagram from the same level when
it does. Timers (staggered by place) decide only when a waiter looks; a late
timer delays that waiter, never reorders. Without the level check, waiters
due on one tick were admitted in poll order and a re-armed waiter lost every
tie (one stream in three sent nothing in 2 s at 1524 streams). After
Admission::Waited the stream re-checks cwnd and calls again; that call
takes its turn without queueing within ADMISSION_PASS (1 ms). Without the
pass it re-queued at the back on every turn and never sent. A stream held
by cwnd past the pass, or whose take failed (an unpaced send took the
datagram first), queues again AT ITS PLACE (AdmissionWaiter::held_place, the
ticket it was admitted from, kept until a take succeeds), so neither costs
it a turn of the queue (streams_sharing_a_window_keep_their_turn_under_a_binding_cap:
worst gap one turn, 0.86 s, against two turns, 1.61 s, when it went to the
back). At most MAX_ADMISSION_WAKERS admissions are live at once; the next
waiter waits, and no live one is dropped. Every admission has a number its
waiter keeps; a waiter that polls again (a QUIC poller whose take failed, a
stream past the pass) withdraws its own, and a take never withdraws one it
did not use, so a failed send on quinn's respond path (no poll_writable)
leaves every live admission alone. A named waiter admitted on its first poll
keeps a place too (its admission's number). A withdrawn admission wakes the
next held waker, and a waiter's timer is set no later than the oldest
outstanding admission lapses, so a datagram counted for an admission that
goes unused does not leave the bucket idle. An admission stays on the queue
(QueuedWaiters::admitted) until its take or for ADMISSION_PASS, whichever
comes first: it counts ahead of every waiter that polls, and every take
leaves one datagram per outstanding admission but its own, so on a
multi-thread runtime nobody can use a datagram admitted to another sender
between its Ready and its take. Admission times are read under the queue
lock. A stream's admission is NAMED (AdmissionWaiter::named, taken with
try_take_admitted through StreamPacing::take): its take finds its own
admission or, once that lapsed, takes as a newcomer that leaves the
datagrams owed to the rest. Without names, a stream descheduled past the
pass took the datagram of a newer admission and cleared that admission as
its own (2 to 37 of 3200 takes lost in
an_admitted_take_is_not_lost_to_another_thread). A QUIC take is named too:
QuicPoller::poll_ready hands its admission number to QuicSocket::try_send
through a thread-local (quic/socket.rs HANDED_ADMISSION), relying on quinn
0.11.12 calling poll_writable then try_send back to back on one thread
(connection.rs:1048-1057; a quinn bump must re-check it,
a_poller_hands_its_admission_to_the_transmit_that_follows). quinn's
endpoint respond (endpoint.rs:584) sends with no poll, finds no handoff and
leaves every admission alone. No take ever uses an admission it does not
name. The queue holds a waker
and a place, never bytes. So connections share the cap by their number of
waiting senders (no isolation between connections). The wait races the
stream's deadline (cut at LONG_WAIT and at PACED_WAIT_DEADLINE, check()
after each cut), so abandonment and its counters mean what they did. Past
MAX_ADMISSION_WAKERS (1024) queued waiters, newer ones hold no waker but
keep their place and their staggered timer; no waiter is evicted (evicting
the oldest sent the longest waiters to the back, and 500 of 1524 streams
sent once in 2 s). A waiter's place is counted under the queue lock up to
MAX_ADMISSION_WAKERS + 1 (so at most 1025 steps per poll, however long the
queue), and its timer's stagger is clamped to MAX_ADMISSION_WAKERS so
quic::socket::cap_hold_validity still bounds a QUIC transmit's next poll; a
waiter further back wakes early and waits again: it is never admitted while
more than MAX_ADMISSION_WAKERS are ahead of it, whatever the level (the
clamp is for the timer only, pinned by
a_waiter_past_the_count_bound_is_not_admitted_by_a_large_burst). The kept connection
payment can be stale after a cwnd or cap wait (the bucket refills under
it): a connection may then exceed its bucket by one datagram per waiting
stream. That moves capacity between connections only; the node cap is
unaffected.
NEVER put a reservation on the NODE cap (earlier reservation/hold models
there leaked capacity). 0.2.148 used try_take + jittered retry on
the per-connection bucket too; the race starved streams on busy connections
past 5 s (every abandonment on capped peers was `connection`, failures
doubled on the busiest capped peer). The node cap kept that race (try_take
+ jittered retry) until #6112, on the reasoning that its waiters were all
streams, equally unordered. It stopped being neutral once QUIC shared the
cap: QUIC waits in the queue, so it won contended refills, and with the
default minimum (below) a QUIC path took about 462 kB/s and a UDP stream
38 kB/s of a 500 kB/s cap (40 seeds). The 0.2.148 lesson applies: a random
race among waiters starves the unlucky ones, so stream waiters now queue
too (249 and 251 kB/s measured the same way). StreamPacing (one per stream, only with
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
interval. Invariant: nothing is reserved on the NODE cap while a sender
waits; it holds no node-wide capacity in the queue, and is charged there only
at its send (it does hold its per-connection reservation, see above). Between
its admission and its take an admitted sender holds one datagram, for at most
ADMISSION_PASS (1 ms): every other take leaves it in the bucket. Gaps
that reach LONG_WAIT
(2.5 s) and abandoned streams are counted by what took most of the gap
(WaitCause: cap, connection = the connection's own bucket, other = cwnd, the
upstream stream for relayed streams, and the rest): egress_long_waits_<cause>,
egress_streams_abandoned_<cause>, egress_streams_refused_<cause>. Each has
its own rate-limited WARN with one line per cause seen since the last, each
with its count and remedy; the per-stream abandonment line is at debug. A
stream abandoned at the deadline (the cwnd-wait check or a capped Abandon)
signals StreamSendOutcome::EgressAbandoned on its completion_tx, not
Dropped, so the UPDATE broadcast counters can name the cause per contract
(#6143). Every consumer treats any outcome other than Delivered as not
delivered; keep it that way when adding a variant. The
cap counts UDP payload, not IP/UDP headers.
EgressCap::poll_admit is the poll form for a sender that is polled (the QUIC
endpoint's UdpPoller, #5940): Ready exactly when try_take would succeed at
that instant (one shared predicate, `admits`), else Pending with a timer on
the cap's TimeSource, kept in the caller's AdmissionWaiter. The timer is
STAGGERED: deficit plus one WAKE_DATAGRAM_BYTES of refill per waiter queued
ahead, so N waiters with one deficit wake one per datagram of refill, oldest
first, not all N at once. While Pending the cap also holds the caller's
waker (at most MAX_ADMISSION_WAKERS; past it a newer waiter is queued with
no waker and relies on its timer, and no entry is dropped); a waiter keeps its queue place across
polls, and dropping an AdmissionWaiter removes its entry, so a closed
connection cannot absorb a wake. Bytes given back (a take that sent less, or a failed send)
wake one held waker per WAKE_DATAGRAM_BYTES (1200, the QUIC datagram) the
bucket then holds, and none if it holds less, so legacy give-backs cost N
waiting QUIC connections nothing until a datagram fits; wakers are woken
after the lock is released, oldest holder first; the woken keep their
places. An admission wakes the oldest waiter still holding a waker (the
next in line, AdmissionQueue::try_admit), one wake per admission: a waiter's
timer is set from its place when it last polled, and waiters that left
ahead of it since (streams failing at their deadline) make it late. Without
that wake, 300 streams on a 64 KiB/s cap, failing at their deadline and
retrying, used 18% of the refill; with it, 100%. An admitted waiter took the slot reserved for it, so
nobody behind it moves; a waiter dropped while queued wakes the next
MAX_FOLLOWERS_WOKEN_PER_LEAVE (8) behind it so each re-polls to its new
place (AdmissionQueue::leave), so N closes cost at most 8 N wakes. Waiters
further back stay up to one datagram late per such drop until they next
poll. No waiter wakes before
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
builds both on RealTime in production; tests build both on one
VirtualTime). In a simulation the endpoint runs on the network's
VirtualTime, so bind_udp_listener refuses a simulated node with both
transport-quic and total-bandwidth-limit.
Tests assert on per-cap EgressCap::stats(), never the global counters.
```

### Retransmit cap (legacy UDP, #6147)

```
With a cap, RTO resends and TLP probes go through EgressCap::admit_retransmit
from the resend arm of PeerConnection::recv: NO await, NO lock (share meter
CAS, then bucket CAS; a bucket refusal only refunds the share). Takes only
while retransmits < RETX_SHARE_PERCENT (50%) of the rate over
RETX_SHARE_WINDOW (1 s, at least one datagram) AND bucket debt after the take
<= RETX_DEBT_LIMIT (100 ms). Nothing retransmitted is forgiven.
Refused RTO resend: state UNCHANGED (count, backoff, send time), timer
re-armed at max(wait, RETX_DEFER_MIN = 2 ms), capped at the deferral limit:
once the CONNECTION's resends have been refused for RETX_DEFER_LIMIT_RTOS (1)
effective RTO in a row (RetxDeferral: from the first refusal, not the RTO
deadline, since a backoff reset moves every deadline; per connection, not
per packet, since only the queue head is asked) every due resend goes
UNCAPPED (charged unpaced) until the cap takes one or nothing due is
refused. Do NOT remove that limit: the count moves only when a
resend goes, so an unbounded deferral (unpaced debt > 100 ms, or a tiny cap)
never abandons the packet and pins flight (#4345). Refused probe: skipped,
marked probed. Abandonment never asks the cap. No cap -> nothing asked.
A SHARE refusal BOOKS the next slot in the share (RetxDeferral,
at most one booking per connection; refunded on run end, which an ack leads
to, on a dropped take and on going uncapped; a closed connection loses at
most that one datagram), so waiting connections wake one per slot; a BUCKET
refusal waits for the next free datagram time among bucket-refused
retransmits, two slots per datagram time (retx_bucket_next_ns, RETX_BUCKET_SPACING_DIV = 2; a timing hint, nothing paid; its lead is clamped to half the deferral limit, since slots are never handed back). Booking on
bucket refusals too was tried and cost 1 MB/s storm streams (383 vs 550):
it paced bucket-limited repairs at the share rate. Without
it they all woke on the same deficit and node-wide wakes grew with N (97k/s
at 256 connections); pinned by
share_waiters_wake_once_per_slot_whatever_their_number. Booking, take and
refund are atomics only: nothing under the sent_tracker lock takes a lock.
Counters: egress_retransmits_deferred (refusals, not packets),
egress_retransmits_uncapped(_bytes), egress_probes_skipped.
The share is NODE-WIDE: one lossy peer can use it; the deferral limit bounds
how long another connection's resend is held (one RTO).
Flight bound: (M+1) x (60 s RTO cap + PTO + floor + yield) + min(all
remaining retransmits at the share, (M+1) x (one RTO + uncapped drain));
derivation term by term in the flight_drain_bound rustdoc; pinned by
link_model::storm_tests::flight_drain_bound and
sent_packet_tracker::held_cap_debt_cannot_stop_abandonment.
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
`send_standalone_event_with_peer_id` so it reaches the OTLP
collector regardless of log level, tagged with the local node id
so the collector can disaggregate samples per reporting node. All
five shadow rollups are listed in `telemetry::PERIODIC_EVENT_TYPES`,
so the telemetry rate limiter admits them without consuming the
operational cap (#6001). A new emitter belongs on that list only if
a fixed timer bounds it.

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
`total-bandwidth-limit`), QUIC upgrade control messages received
(`upgrade_control_received`), Decline replies held back
(`upgrade_decline_suppressed`) and hole-punch handshake packets dropped,
one count per packet, because they were not encrypted under the attempt's
inbound session key (`handshake_unauthenticated_dropped`), QUIC endpoint deaths
(`quic_endpoint_down`), panics the QUIC socket adapter caught
(`quic_adapter_panicked`, once per endpoint; the process keeps running
and no breaker state is left), QUIC crash-loop breaker trips at startup
(`quic_breaker_trips`, on unix counted for any abnormal end of a QUIC-enabled
run, not only a QUIC fault, and on Windows for three runs in a row that ended
within ten minutes after recording a panic in QUIC code (quinn, quinn-proto or
transport/quic, by panic location), each starting within 24 h of the one before;
see `quic/breaker.rs`) and whether the breaker keeps QUIC
off in this process (`quic_breaker_suppressed`, a state that is never reset and
is not activity by itself) and the endpoint's state (`quic_endpoint`: `up`, or
`off_config`, `off_platform`, `off_abort`, `off_bind`, `off_breaker`, or `died`;
`unknown` before the node reaches the bind; set at startup after the breaker
gate, at the bind, and at the endpoint's death, which is final for the process;
a state that is never reset and is not activity by itself) and QUIC connections this node closed, by close code
(`quic_close_sent_<label>`, one per `QuicCloseCode`), the QUIC-native connect
offer and decision (`connect_native_offered_{zero,nonzero}` built as joiner,
`connect_native_decided_{native,udp}` made as acceptor for an offered request,
`connect_native_decision_{without_offer,native_unsupported}` for a `Native`
decision a joiner could not use, `connect_native_decision_over_cap` for one
dropped unanswered past the per-CONNECT cap on refused acceptors, and the last
offer as states
`connect_native_offer_{level,percent,ready}`; see
`operations/connect/native.rs`) and the shadow-mode upgrade
telemetry (below, "Upgrade telemetry"). Each label set is declared once in a
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
largest estimate (the levels are fixed so the series stays comparable across
releases; `MAX_UNI_STREAMS` is 64 since 2026-10-26, so `ge_64` counts the
connections that would reach it, and `ge_128` / `ge_256` are kept for
continuity); keys are uploaded only when non-zero, and `conns_sampled` is
the only activity term. It is an estimate built to err high, not a strict
upper bound; the known undercounts (a sample skipped at a window boundary,
arrivals older than the horizon or past the 1024 cap, streams the 1024-stream
budget refused) are listed in the module doc. So `ge_T / conns_sampled` can
support LOWERING `MAX_UNI_STREAMS` (`quic/config.rs`), never raising it; a
limit that is too low costs only sender back-pressure. Shadow mode carries no
data, so it measures nothing about the value; rollout step 3 (data on a slice
of pairs, see "QUIC rollout") is the first measurement on real QUIC. The gauge says nothing about the memory
a peer opening streams on purpose can hold (the per-path ceiling's job). `inbound_concurrency_max` is diagnostic
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

### Release census (`join_versions`, #5940)

`transport/join_versions.rs` emits `join_versions` once per
`JOIN_VERSIONS_INTERVAL` (1 h, listed in `PERIODIC_EVENT_TYPES`), only when
telemetry is on, so the QUIC-only plan can see which releases are out there,
including nodes whose own telemetry is off. `gateway_joins` (gateways only)
counts each SUCCESSFUL inbound gateway handshake by transport and the joiner's
version, at the listener's `complete_gateway_handshake` success arm, never at
intro receipt (an intro is spoofable); it is process-wide (`GATEWAY_JOINS`) and
resets at each emission. `neighbours` (every node) is a snapshot of live ring
connections by peer version (`ConnectionManager::live_neighbour_versions`), so
reconnects do not skew it. A version is bucketed relative to this build
(`OWN_VERSION`, `OWN_FLOOR`, the values `PROTOC_VERSION` encodes): the exact
version for this major.minor and a patch in `own_patch - 31 ..= own_patch`;
when the floor is in an earlier series (format 2, #6189), also the exact
version for the floor's major.minor and a patch in `floor_patch ..=
floor_patch + 31`; else `older`, `newer` or `unknown` (no version). That is at
most 67 labels whatever a peer sends (35 while the floor shares this
series), held in fixed arrays. No keys,
no addresses; in `gateway_joins` a count of 1 to 4 is reported as `"<5"`, and
zero counts are omitted in both maps. Transport `legacy_intro` is today's UDP
handshake; `native` is reserved for QUIC-native joins, which do not exist yet,
and is never counted. It is not a `KnownShadowRollup`: its admission and drop
counts are in the per-type `tel_t` row like any other event.

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
    Earlier releases track a gateway's success ack at packet id 0 and need
    the receipt; they also give their first data packet id 0, so recording
    it would drop that packet as a duplicate. Our own data starts at
    FIRST_DATA_PACKET_ID (1) on every path and no handshake ack is tracked
    on a connection, because a receipt for id 0 (sent for every connection
    ack, including a handshake ack resent across our completion) would
    otherwise be credited to our first data packet: a false RTT sample and
    a lost packet never retransmitted. DO NOT start data at 0 again.
    Answering a connection ack with `ack_ok` happens at most ONCE per
    connection: the reply is itself an AckConnection, so answering every
    one loops forever between two peers.
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

A node that receives one acks it like a ShortMessage, counts it
(upgrade_control_received in transport_snapshot) and never closes the
connection over it. With upgrades (see "Upgrade driver") it goes to the
session's driver. Without them an Offer gets Decline{Disabled} and an Accept
or a Decline is dropped.

At most one Decline per connection per DECLINE_INTERVAL (45 s, below the shortest uncharged retry of 48 s), on the
connection's clock (quic/upgrade/driver.rs DeclineGate; the same type on
PeerConnection with QUIC off and in the driver with QUIC on). A Decline is
tracked and retransmitted, and each Offer under a new packet id would otherwise
get its own. One held back is counted by the gate (upgrade_decline_suppressed)
and costs the offerer NoReply after OFFER_TIMEOUT, which takes a normal backoff,
including where the reason would have been uncharged (Busy, NotReady, Backoff).
With QUIC off the reply is not sent in process_inbound: the Offer is already in
the dedup window, so a cancelled send would lose it. pending_decline holds the
attempt, the top of recv's loop sends it, and it is cleared only once that send
has finished, so a cancelled recv sends it on its next call. A failed send is
not tried again.

Version gate: send it only when may_send_upgrade(remote_version) is true,
i.e. the remote is at or above QUIC_UPGRADE_MIN_VERSION (0.2.149). None
(unknown) fails closed. Peers at or below 0.2.147 panic on a payload they
cannot decode and 0.2.148 drops it, so the decoder must ship in a release
before any release sends the message. A reply to a received Offer needs no
gate. The floor follows the wire-gated-floor rule in docs/RELEASING.md
(QUIC_UPGRADE_SHIPPED_IN marker).

An Offer also needs may_offer_upgrade(remote_version): the remote is at or
above QUIC_UPGRADE_NEGOTIATES_IN, the first release that answers one (a peer
below it decodes an Offer and ignores it). It is (0, 2, 154), recorded in
QUIC_UPGRADE_NEGOTIATES_SHIPPED_IN. Nodes below it report a version below the
floor and do not
offer to each other.

Decline reasons (DeclineReason, wire values): 1 Disabled, 2 Backoff, 3 Busy,
4 NotReady (no peer key from a promotion yet, or an attempt in progress). Any
other value is a decline with no stated reason.

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
Rate calculation on every rate update (peer_connection::paced_rate; #6047):
  final_rate = min(cc_rate, global_pool_rate)        with total-bandwidth-limit
  final_rate = min(cc_rate, bandwidth_limit)         else, if bandwidth-limit set
  final_rate = cc_rate                               otherwise
  A configured limit only lowers the rate, never raises it above cc_rate.
  bandwidth_limit is the config after NetworkApiConfig::transport_bandwidth_limit
  (effective_bandwidth_limit, called once in p2p_protoc and pinned there by a
  config.rs test): 0 = unset, ignored under any non-zero total-bandwidth-limit (a total of 0 is unset), and
  raised to MIN_BANDWIDTH_LIMIT (2 x MIN_INBOUND_STREAM_RATE_BYTES_PER_SEC,
  64 KiB/s) if lower: near the receiver's age-limit rate, long streams fail.
  The bucket's INITIAL rate comes from peer_connection::initial_paced_rate at
  the three connection setup sites (global share as is, else cc_rate lowered
  to bandwidth_limit) and fixes the bucket's capacity (initial_rate / 10) for
  the connection's life, so a bandwidth-limit above cc_rate must NOT raise it
  (#3702 burst). The global share is still taken as is, as before #6047.
  Only stream fragments draw on the bucket; short messages, ACKs and
  retransmits are unpaced.
  QUIC has no share of this bucket. With a limit set its data is paced by a
  bucket of its own at the limit (the path pacer, below), and the same rate
  (QuicRateLimit::rate, which calls paced_rate) caps each QUIC connection's
  congestion window:
  recycle::CappedCubic::window = min(Cubic's window, max(rate x RTT,
  window_floor)): the cap never goes below window_floor (two full datagrams
  and one byte, 2401 B: quinn refuses
  a send at in_flight + bytes >= window, so 2400 would admit one datagram, and
  a lone packet waits for the peer's 25 ms delayed ack). With no limit, or
  before the first acknowledgement, it is Cubic's window; for the first batch
  of acknowledgements the RTT is still quinn's 333 ms initial estimate, so the
  cap is loose for that batch. quinn's in-flight limit reads the window, which
  holds a path to about the rate (a_path_under_a_bandwidth_limit_keeps_to_it:
  within 10% at 400 kB/s over a 40 ms round trip); the pacer only spreads the
  sends. While the cap is below Cubic's window, on_ack reports the path as
  app-limited to Cubic, so Cubic does not grow behind the cap and still backs
  off on loss once the cap rises above the capacity (RFC 7661 idea). It
  covers everything the path sends, shorts and control frames included. It
  does not hold where rate x RTT is under the floor: there a path keeps two
  datagrams in flight, about 2400 B per round trip (at the 64 KiB/s minimum,
  any round trip under about 37 ms; pinned by
  the_floor_keeps_two_datagrams_in_flight_both_ways). On loopback a path ran
  at 3.4 to 34 MB/s with 500 kB/s set.
  Path pacer (quic/path/pacer.rs): with bandwidth-limit or
  total-bandwidth-limit set (PeerConnection::path_rate_limit), the session
  builds a LimitPacer on the path's clock (the one the send deadlines run
  on), and every path of the connection draws its data writes from it: a
  token bucket of the connection's QUIC side at QuicRateLimit::rate (the
  limit, or the share, read again at every reservation; capacity rate / 10,
  at least MIN_BUCKET_CAPACITY_BYTES, recomputed when the rate changes), so
  the limit holds at any round trip. NOT the UDP stream bucket: that runs at
  min(UDP controller rate, limit), and FixedRate's 1.25 MB/s would pace QUIC
  below a higher limit, slower than no limit at all. Charged: each write of
  a Stream or pipe (header, metadata, payload) and of a Short past one
  packet. Not charged, as on UDP: one-packet Shorts and control frames,
  which never wait behind data. With no limit nothing is paced. A write
  reserves with reserve_within, limited to what the send's current deadline
  leaves for its stage (with the stream open, its own gap, never the
  connection's window: bounded::SendProgress::time_left; the window would let
  the queue hold about 10 s of the rate instead of 5 s, refusing more opens;
  a wait for a pipe's source is not a step, so after a slow chunk the next
  write may be offered little and refused mid-stream, as in #6126),
  and waits in the send's own task; a refused reservation takes
  nothing and ends the send NotOpened(Paced) before the stream opened
  (header and metadata are taken before open_uni) or Failed(Paced) after
  (the stream is reset). A message the pacer refused that UDP then sent is
  charged to the pacer in full as debt (QuicPath::charge_fallback, after the
  UDP send succeeds), up to FALLBACK_DEBT (half a second at the rate): an
  unbounded debt refused the next fragment of every stream already open, and
  a refused open stream is dropped. Each refusal counts
  quic_data_event_pacer_refused. A total-bandwidth-limit of 0 is unset for
  both transports (NetworkApiConfig::effective_total_bandwidth_limit: no
  global pool, so no UDP share, no QUIC pacer, no window cap, and a
  bandwidth-limit beside it applies to both). Bytes taken for a write not
  made go back. A refusal is never a timeout for the stall count (route.rs
  timed_out); a pacer wait is NOT progress, so a dark paced path still goes
  suspect and closes. Residual, accepted (lead, 2026-11-06; the design-B
  residual): a peer whose data is on both transports at once can get up to
  twice its share, for as long as that lasts. That includes a message the
  pacer refused: the charge is booked when its UDP transfer ends and is
  capped at half a second, so for the whole transfer QUIC keeps sending at
  the pacer's rate beside it (a 10 MB fallback at a 100 kB/s bandwidth-limit
  runs about 100 s on the UDP bucket, then 50 kB is charged). Data that falls
  back for any other reason (no stream credit, a deadline) is paced by the
  UDP bucket only. Only a budget shared by both transports would close it;
  the egress cap still bounds the node under total-bandwidth-limit. Pinned by quic::path::tests::pacer,
  pacer_refusals_never_count_and_a_dark_paced_path_still_closes,
  a_paced_send_to_a_dark_peer_times_out_and_the_path_goes_suspect,
  messages_the_pacer_refuses_go_on_udp_and_are_charged_to_it,
  a_session_paces_its_data_only_under_a_limit,
  a_sessions_pacer_runs_on_the_path_clock,
  config::tests::a_total_of_zero_paces_neither_transport,
  a_data_route_is_paced_only_when_the_session_has_a_pacer and
  quic_data_is_paced_only_under_a_limit_and_at_that_limit.
  Under a saturated egress cap, the window cap apportions it only when the
  per-connection share is below the cap. With the minimum per connection set
  below the share (two connections, one flow each, equal RTT),
  a_quic_path_and_udp_streams_each_get_their_fair_share_under_a_saturated_
  egress_cap measured QUIC about 239 kB/s and a UDP stream sender about
  250 kB/s of a 250 kB/s share, over 40 seeds (491 and 12 kB/s with the
  window cap removed). With the DEFAULT minimum (DEFAULT_MIN_PER_CONNECTION,
  1 MB/s, which GlobalBandwidthManager clamps to the total), a total below
  n x 1 MB/s, so nearly every residential cap, gives every connection a share
  of min(1 MB/s, total): each QUIC window cap is total x RTT and apportions
  nothing. The egress cap's admission queue apportions it instead: QUIC and
  UDP stream senders wait in the same FIFO (see "Node-wide egress cap"), so
  each waiting sender gets a turn. Measured the same way with the default
  minimum: QUIC 249362 to 249363 B/s, one UDP stream 250568 B/s (40 seeds;
  before #6112 QUIC 461 to 463 kB/s, UDP 37 to 39 kB/s). With four UDP
  streams on the other connection: QUIC about 100 kB/s, the streams about
  400 kB/s, since the queue serves senders, not connections (with no QUIC
  path, one stream against four on another connection: 100 and 400 kB/s).
  With the minimum below the share the connection buckets bound each side:
  QUIC 239332 to 240057 B/s, UDP 249943 to 250106 B/s, with one or four
  streams. No fragment reached the stream deadline in any case.
  KNOWN LIMITATION on capped nodes: a QUIC connection is one sender in that
  queue, so its ACKs wait their turn behind queued UDP stream fragments. With
  100 UDP streams queued on a 500 kB/s cap, the peer sending to this node over
  QUIC measured srtt 123 ms (41 ms on main, where QUIC won nearly every refill)
  and a 300 kB transfer took 0.74 s (0.21 s); no losses or congestion events.
  No ACK priority: quinn does not mark ACK-only transmits and the poller does
  not know a transmit's size when it polls, so a size rule would be a guess.
  Only nodes with total-bandwidth-limit are affected.
  CappedCubic::metrics reports the capped window as congestion_window.
  The transport sets the rate when it binds (bind_udp_listener, pinned by
  the_transport_gives_the_endpoint_its_bandwidth_settings); QuicSettings
  from the config carries none.

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
rest of the queue is empty. The last QUIC_INGRESS_QUIET_RESERVE (128) slots
are for sources holding fewer than QUIC_INGRESS_QUIET_SOURCE_DATAGRAMS (4):
once only those are free, any other source's datagram is `_reserve_full`, so
a quiet connection's loss probes and acknowledgements still get in. Without
it, a full queue dropped a quiet connection's probes one after another, quinn
doubled its probe timeout each time, and the connection went silent past the
consumers' 5 s gap (256 connections into one node, debug build). The cost:
sources holding 4 or more queued datagrams have 12.5% less room between them
for a burst (896 slots rather than 1024). The 896 is momentary, not a quota:
a source that drains below 4 counts as quiet again. Many sources spoofing
allowed addresses can fill the reserve; the queue then holds 1024, as
without it. Limitation: the reserve helps only while most sources are busy;
with about 256 or more connections each holding a few datagrams, most
traffic counts as quiet and the queue behaves close to a plain FIFO.
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
`_queue_full`, `_source_full`, `_reserve_full` (refused at the quiet
reserve, below) or `_no_ingress` (no ingress enabled, its
receiver gone, or disabled; every node with `transport-quic` off). The QUIC
endpoint enables the ingress through `with_quic_ingress`, which disables it
again (allow-set emptied and closed) if the endpoint cannot be built, so a
failed `shadow`/`prefer` bind leaves the socket as with QUIC off.

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

`transport-quic` (config key: `off`, `shadow` or `prefer`; unset is
`QuicMode::default_mode`, shadow on Linux, macOS and Windows and off
elsewhere, since the breaker cannot trip without a boot identity) decides whether `bind_udp_listener` builds a quinn endpoint over the DemuxSocket.
`prefer` negotiates data with the release's share of pairs ("Data slice" below;
`upgrade::policy::log_startup` reports the share at INFO once per node), and a pair
with both bits sends its data on the path ("QUIC send route" below). At a share of
0, with no `quic-data-percent`, `prefer` differs from `shadow` only by the bit, which
stays 0. The release that raises the share above 0 must say in its notes that
existing `prefer` configs start carrying data. Off: no endpoint, no QUIC ingress, nothing
changes, except that `off` set in the config is deprecated and logs a WARN at
startup (`upgrade::policy::log_startup`); the endpoint state telemetry reports it
as `off_config`. Windows is shadow by default too since its crash-loop breaker was
confirmed on a real Windows machine (#6031). The default is
read, never written to config.toml: a node that did not set the key follows the
release it runs, so a default-off release is the kill switch. It does not reach
a node that ever ran with `--transport-quic` or `FREENET_TRANSPORT_QUIC`: those
are saved to config.toml like any setting (the Step 1 gateways are such nodes). Shadow or prefer: the endpoint is built
with the node's QUIC identity (below) and an EMPTY allow-set, so nothing
reaches it and it sends nothing; the listener holds it, with the queue of
its accept loop (below), which hands on no attempt while the allow-set is
empty. An upgrade adds a peer to the allow-set ("Upgrade driver" below).

Upgrade state machine (`quic/upgrade/machine.rs`, #5940 step e5a-5a):
UpgradeMachine::step(Inputs { now, sample, policy }, Event) returns
the Actions for the caller to carry out. It does no I/O and reads no clock:
now comes from the connection's TimeSource, sample is a GlobalRng draw in
0..1. The caller sends Event::Tick at UpgradeMachine::deadline(). The module
doc has the transition table.
  Roles: the higher key is the server and the only side that offers. A role
  comes only from Event::Promoted, so a transport holding no peer key, or one
  whose promotion sent no key, is not eligible (it answers an Offer with
  NotReady). Equal keys have no role and the machine is Off.
  Offer: after promotion plus 2 to 10 s, only if may_offer_upgrade, and only
  with a permit from QuicPolicy::admit. allow_peer runs before the Offer is
  sent, and the caller then reports Event::OfferSent, which is what records
  Outcome::Offered, so a refused allow_peer is not counted as an offer. A
  permit is held inside the Setup and Active states, so leaving them returns
  it. At the path limit the server does not offer and is not charged (retry
  after 60 s, jittered); a client answers Busy.
  Timeouts: Offer 10 s, connecting 10 s, waiting for the other side's control
  frame 10 s (two Ready/ReadyAck sends of 5 s each), Protocol{0x1} recheck 1 s.
  Established: the client on Ready, the server on ReadyAck. Ready carries the
  attempt id; a frame or report for any other attempt changes nothing.
  Backoff: 60 s doubling to 1 h, 1 h after an identity mismatch, all +/-20%
  (util::backoff::ExponentialBackoff; TrackedBackoff reads the clock, so it is
  not used here). A longer pending backoff is not shortened. Only a path that
  lasted STABLE_PATH (10 min) clears the failure count, also when the session
  or endpoint ends it, so a path that keeps dying soon after it comes up is
  retried less often.
  Session end: from Setup, Rechecking or an Active path younger than
  STABLE_PATH, SessionEnded takes a normal backoff, because the session of a
  restarted peer is replaced before its old QUIC connection reports anything.
  A failure that already charged left the machine in Idle, so it is not
  charged twice. An endpoint ending, or a SessionEnded with the endpoint read
  as Down, charges nothing. A machine shut off by a session end closes its
  path with SESSION_ENDED (code 6); every other shut-off closes with DOWNGRADE.
  Uncharged: a Decline of Busy, NotReady or Backoff and a full allow-set retry
  after 60 s (jittered) and leave the failure count. So a peer that stays
  NotReady or in its own backoff is offered to once per 60 s while that lasts.
  Disabled and an unknown reason take the normal backoff.
  Across sessions: BackoffSeed { failures, retry_at } carries the count and the
  next attempt's time. UpgradeMachine::backoff_seed reads it, after stepping
  SessionEnded, and Event::Promoted takes it, since an inbound session has no
  key until promotion and its machine must exist before that to answer an
  early Offer. A later promotion's seed is ignored. retry_at is cut to
  SEED_WAIT_MAX (72 min, the longest delay with the most jitter) and the count
  is dropped once retry_at is more than BACKOFF_MAX in the past, since the
  seed may come off a clock with another epoch. A promotion's first offer waits
  for the later of the seed and its own delay. e5a-5b must carry it per peer key
  across a replaced session, or a session that flaps retries at session rate.
  Failures: close codes 1, 3, 4, 5, 6 and 8, a lost connection, an idle timeout and a
  deadline each mean fallback plus normal backoff, then a new offer. Codes 9
  (VERSION_INCOMPATIBLE) and 10 (HELLO_REFUSED) are sent only on native
  connections; on an upgrade path they read as a lost connection. Never a
  permanent downgrade. Identity: close code 2 received, or Event::IdentityMismatch
  (closes with IDENTITY_MISMATCH). A TLS failure during the handshake
  (Refused{Handshake}) takes the normal backoff.
  A failed Ready or ReadyAck send: a lost connection takes the path of
  Event::PathFailed, attributed against the endpoint first (the kind in
  BoundedSend is not attributed; a PathFailed's is), and a stop from the peer
  is one stream's and changes nothing. A send that timed out or never left the
  node is a normal failure.
  Protocol{0x1}: the attempt's resources go at once, then up to 1 s for the
  endpoint to read Down. If it does, fallback_endpoint_down and no backoff; if
  not, a normal backoff.
  Control frames refused during setup: UnknownVersion and UnknownType close at
  once with CONTROL_REFUSED and a normal backoff, so the sender does not wait
  for its setup timeout. WrongLength and Empty wait for the deadline. Once
  active, a refused frame leaves the path alone.

  Off is final for the session, also for a policy that later clears (endpoint
  Down, Released or Disabled, session ended, policy refusing, below the version
  floor, equal keys): no re-offer, and no charge to the peer except from a
  session end (above). A client or a machine with no role answers an Offer in
  Off with Disabled; a server records role_conflict and does not reply. A later
  Promoted changes and records nothing.

  An Offer repeating or older than the highest seen gets no reply. An Offer
  while an attempt is in progress gets NotReady; a client never tears a path
  down because of an Offer, so offers cannot make it dial faster than its
  attempts finish. A server that receives an Offer records role_conflict and
  does not reply.

  Caller contracts (e5a-5b): report OfferSent before stepping any reply to that
  Offer; drop an Offer still deferred behind allow_peer once a TearDown names
  its attempt; read backoff_seed after stepping SessionEnded (None when the machine took no seed: file nothing); end and file any older session for the peer key BEFORE stepping Promoted on the new one, and never let an older session's seed replace a newer one's (a max merge undoes a stable path's clear, so track session order); give every session the listener's clock clone (RealTime::new() starts its own epoch); give the client
  and server machines of a session the session's lifetime, since attempt ids
  restart per session and a client ignores an Offer at or below the highest it
  has seen, with no reply.
  Action::Record(Outcome) carries the outcomes for e5a-6. Local recycle closes
  reach the machine as ConnectionLost; their counts stay in quic_close_sent_*.
  Tests (machine/tests.rs, no simulation): every (role, state, event) against
  a table where anything unlisted must change nothing, including failures,
  retry_at, last_attempt and last_offer, the same for stale attempts, plus
  backoff, jitter bounds, deadlines, each close code through the real error
  mapping, Down as terminal, and the permit count after every step. A harness
  (Pair) runs two machines on one clock and loses each message kind once, and
  asserts both end up active, or both idle with no permits, within a bound for
  that kind (30 s, or 125 s for a lost close, which waits for the idle timeout).
  timeouts_and_windows_have_these_values pins the constants as literals, and
  negotiation_floor_stays_above_every_release_that_ignores_an_offer stops
  QUIC_UPGRADE_NEGOTIATES_IN going down to 0.2.152 or below.

Data slice (#5940 e5b-3): `upgrade/slice.rs`. bucket(a, b) = first 8 bytes of
BLAKE3("freenet-quic-data-slice-v1" || min(a, b) || max(a, b)), big-endian, mod
100, over the two transport public keys, the peer's from the node's record. A
pair is in the slice when its bucket is below the percentage, so 0 holds none,
100 holds all, and slices nest. A test vector pins the function.
  The percentage: `QUIC_DATA_PERCENT` in `upgrade/policy.rs`, one value per
  release, 0 today; the config key `quic-data-percent` (lenient text like
  `quic-max-paths`; anything but a whole number from 0 to 100 reads as unset, with
  a WARN when QUIC is on) replaces it on one node. Only `prefer` uses it.
  `QuicPolicy::own_caps` gives `caps::DATA` when the mode is `prefer` and the pair
  is in the slice, otherwise 0, and the driver passes that in `Event::Promoted`.
  `QUIC_DATA_RECEIVER_READY` is true: `recv` reads the path's data ("QUIC receive
  flow" below).
  The machine reads no key to decide it.
  A path carries data only with both bits. `Ready` and `ReadyAck` carry the
  sender's own bit. The peer's bit is read from the frame that activates the
  attempt and held in `State::Active` for that attempt, so a later frame changes
  nothing and the next attempt negotiates again. With both set the machine emits
  `Action::EnableData` and `Outcome::DataEnabled`, which has no counter yet:
  `QuicUpgradeOutcome` is at 32, the longest array serde and `Default` take, so a
  new upgrade count needs a labelled set of its own.
    own 0, peer any: refuses inbound data with NOT_ACCEPTING_DATA, may not send.
    own 1, peer 0: accepts inbound data, may not send.
    own 1, peer 1: accepts inbound data, may send (`QuicPath::enable_data`).
  Accepting is set when the path is built, from the own bit alone
  (`QuicPathLimits::SHADOW` or `DEFAULT`), because the peer's first stream can
  overtake the frame carrying its bit. Sending is `QuicPath::send_data`, which
  `debug_assert_carries_data` reads. The Offer's caps stay 0. A release before the
  bit clears it on decode and sends 0, so its pairs stay in shadow.
  With both bits the connection sends on the path ("QUIC send route" below). Frames
  a peer sends to a node with its own bit set reach `recv` ("QUIC receive flow"
  below). A sender needs no version check: 0.2.154 is the first release that can
  set the bit and the first that reads, every earlier one clears the bit on decode
  and sends 0, and a node rolled back has restarted and negotiates again.

Receipt stamp (#6028, e5b-3): `ReceiptStamp` reads the later of two values,
`last_inbound_nanos` (UDP) and `quic_receipt_nanos`, on the connection's clock. The
second is written only by the path's `watch_acknowledgements`, at a tick
(ACK_WATCH_TICK, 2 s) that saw `frames_received` grow, so PINGs count, and only
once the machine reports the path established (`PeerHeard` stays empty before
that). Before any path has been active, as always with QUIC off, it is 0 and the stamp
reads as the UDP value alone; after a path ends it keeps its last write and ages
out. Only `identity::key_holder` reads the stamp: a holder heard
from on its active path within HOLD_WINDOW (30 s) keeps its key against a newer
claim. A holder can read as heard from up to one tick longer than it really was.
`last_inbound_nanos` and `last_received_nanos` stay UDP-only, so the keepalive
backoff and the idle timeout are unchanged, and a holder whose UDP is dead is still
closed by the idle timeout at 120 s.

Upgrade telemetry (#5940 e5a-6). The driver turns each machine `Outcome` into a
counter at `Session::count`, which matches every `Outcome` variant, so a new
outcome does not compile until it has one. All are per-period and reset with the
snapshot except the two gauges.
  `quic_upgrade_<label>` (`QuicUpgradeOutcome`): offered, accepted (the peer
  accepted this node's Offer, by its Accept or, if that was lost, by its
  connection, counted once either way), accept_sent, established (either role),
  established_as_offerer, no_reply, timeout, handshake_failed, identity_mismatch,
  role_conflict, allow_set_full, key_conflict, control_refused,
  ineligible_version, declined_<reason> (the peer's Decline), declined_uncharged
  (the same Decline when the machine spared the peer the penalty; the flag comes
  from `on_decline`), decline_sent_<reason> (counted after the decline rate gate;
  `upgrade_decline_suppressed` counts the held-back ones), setup_downgrade_rx and
  setup_endpoint_down (the same causes as the fallbacks below, before the path
  was active), session_ended_setup and session_ended_active (the UDP session
  ended under an attempt or a path), inbox_overflow (a full inbox only),
  attempt_unrouted (attempts that reached this node and found no session or no
  attempt waiting; a client's retransmitted Initial can count again),
  attempt_retried (attempts answered with a Retry; one per upgrade with no loss,
  and every unvalidated Initial with the address as its source counts), and
  task_panicked (a driver or pump panic, re-raised after counting).
  `identity_mismatch` counts on both ends: a peer's close with code 2 ends this
  node's attempt as `identity_mismatch` too.
  Step 2 gate denominator: established_as_offerer over accepted, reported beside
  offered and declined_uncharged. Both sides count `established`; only the
  offerer's is the numerator. Counts are node-wide; there is no per-pair series.
  `quic_fallback_<label>` (`QuicFallback`): idle, reset, protocol, downgrade_rx,
  endpoint_down, session_ended_rx, over_budget_rx. Paths that had been active only; an attempt
  that ends in setup is counted in `quic_upgrade_*`. A close this node sent for
  send_abort_limit or peer_not_acknowledging reaches the machine as a lost
  connection, so it counts as `reset`; its count is `quic_close_sent_*`. A key
  mismatch is `quic_upgrade_identity_mismatch`.
  session_ended_rx is the peer closing with SESSION_ENDED (6): its UDP session
  to this node ended (a prune or a restart). It is charged like downgrade_rx.
  In setup this node counts the same close as `setup_downgrade_rx`, since
  `QuicUpgradeOutcome` is full. A peer on a release before the code closes
  with DOWNGRADE in that case (downgrade_rx). A node on a release before the
  code reads 6 as an unknown code, a lost connection: `reset` for an active
  path and `quic_upgrade_handshake_failed` in setup. So during a mixed
  rollout, old nodes show more `reset` and `unknown_code` and fewer
  `downgrade_rx` for the same closes, with nothing broken.
  `quic_churned_fallback_<label>`, the same labels: fallbacks the UDP session's
  end followed within FALLBACK_CHURN_WINDOW (150 s). The window is longer than
  a UDP session takes to end once its peer goes silent (120 s idle timeout,
  plus a keepalive interval, plus a timeout check), pinned by
  the_churn_window_outlasts_a_udp_idle_timeout. The machine remembers each
  fallback's time and cause (at most RECENT_FALLBACKS_MAX, 8; each is followed
  by a backoff of at least 48 s, so at most 4 fit the window) and at
  SessionEnded records Outcome::ChurnedFallback once for each within the
  window, boundary included; the list is then empty, so a later SessionEnded
  counts none again. It is a separate prefix so a sum over `quic_fallback_*`
  does not add it in.
  Reading them: a churned fallback is counted at the session's end, which can
  come up to 150 s after the fallback, so with the default 30 s snapshot a
  per-snapshot `quic_fallback_<c>` minus `quic_churned_fallback_<c>` can be
  negative. Subtract only over windows much longer than 150 s (an hour or a
  day). The Step 2 gate, fallbacks per path-day excluding endpoint and
  restart causes, is then
    (sum over c in idle, reset, protocol, downgrade_rx, over_budget_rx of
       quic_fallback_<c> - quic_churned_fallback_<c>) / path-days,
  with endpoint_down and session_ended_rx left out entirely (session_ended_rx
  is churn by definition). over_budget_rx is in the sum: it is a close that
  counted as downgrade_rx before OVER_BUDGET existed (a peer's second trip
  past the gap cap), and a release before the code counts it as reset. It is a lower bound on fallbacks where QUIC failed
  while UDP kept working: any session end within the window counts, including
  one that the QUIC failure itself caused.
  `quic_close_received_<label>`: one per QUIC connection quinn reports as closed
  by a peer application close, by raw code (counted in `QuicConnection::new`'s
  watcher, because `QuicErrorKind` merges codes 1, 3, 4 and 5); a code with no
  name is `unknown_code`, always the last slot. A new code goes in before it,
  so a reader must take the last slot as unknown and not use a fixed index
  (SESSION_ENDED moved `unknown_code` from index 6 to 7, and
  VERSION_INCOMPATIBLE and HELLO_REFUSED from 8 to 10). A local close is not
  counted.
  `quic_path_age_at_close_<lt_1m|lt_10m|lt_1h|lt_1d|ge_1d>`: age of an active
  path at the teardown that ends it. `quic_establish_ms_p50`/`_p90`/`_samples`:
  from this node's Offer or Accept to active, on the session clock, first 256
  samples per period (`_samples` counts all). Both roles are mixed: a server
  times to its `ReadyAck`, a client to its `Ready`.
  Gauges `quic_paths_offered_or_dialing` and `quic_paths_active` are held by a
  `PathGauge` guard inside the attempt, so dropping the attempt leaves the gauge.
  `quic_paths_active` is also in `resource_utilization` beside
  `memory_rss_bytes`.
  `quic_flow_control_window_bytes`: the window every QUIC connection of this
  node starts with (the floor, per direction), set when the endpoint is built,
  0 without one; a state, not reset and not activity. A data path's windows
  then move within the buffer budget (`quic_window_*`, `quic_budget_*`).
  Not built: the `transport=legacy|quic` attribute on connection, transfer and
  disconnect events; quinn's RTT, min RTT and lost bytes on a QUIC transfer's
  events (those fields are None today); the per-peer byte counters and the
  dashboard for QUIC traffic; a per-path memory estimate.

QUIC data telemetry (#5940 e5b-8, `transport/metrics/quic_data.rs`): node-wide
counts of what paths' data sends and receives did, in one nested
`TransportSnapshot::quic_data` and uploaded flat, a key per non-zero count and the
gauge every period, 0 included (`QuicDataSnapshot::upload_keys`). Each is
recorded where the path decides it, into the path's `QuicPathStats` and the node's
`QuicDataCounters` together (`QuicPathLimits::counters`, `TRANSPORT_METRICS` unless
a test passes its own; tests read their own, never the global).
  quic_data_send_<short|large_short|stream|pipe>_<sent|maybe_delivered|
  not_opened_udp|failed_udp|dropped>; quic_data_event_<short_overflow_udp|
  data_refused|stalled_closed|udp_send_failed|suspect_entered|suspect_left|
  pacer_refused|ack_wait_released|stream_limit_full>; quic_data_stop_<code> (stops a
  peer sent to data sends); quic_inbound_<QuicInboundOutcome> (every receive
  outcome, refusals by frame kind as short_refused_budget and
  stream_refused_budget); quic_enabled_data_paths; quic_live_data_paths (a gauge of
  paths with a route the peer has not refused data on, not reset, not activity;
  lowered once, at the refusal or when the attempt goes); quic_sends and quic_open_uni_waited (opens a
  path tried, data and control, and those that waited for stream credit);
  quic_streamed_sends and quic_stream_permit_waited; quic_bytes_sent and
  quic_bytes_received (quinn's UDP payload counts, reported by change at each 2 s
  tick of the acknowledgement watch and once more when the path's task ends);
  quic_sent_packets, quic_lost_packets, quic_lost_bytes and
  quic_congestion_events (quinn's PathStats, every path including shadow ones, by
  the same tick and once more when the path's task ends, so a path's last tick
  before it closes is counted, sums of the period). A CONNECTION_CLOSE quinn sends
  again after that report is in neither the byte nor the packet counts. A panic
  in the watch, which the path survives, ends nothing: the report is the
  receive supervisor's (recycle::PathReport); quic_loss_rate_<zero|lt_0_5pct|lt_1pct|
  lt_2pct|lt_5pct|lt_10pct|lt_20pct|ge_20pct> (a gauge of live paths that have sent
  at least 200 packets since they started, each in the one bucket of its own
  lost/sent over its life, moved as it changes, out when the path ends; every
  bucket uploaded every period, 0 included; no per-path values) and
  quic_ended_loss_rate_<bucket> (paths that ended in the period, by their final
  bucket, so paths that fail lossy still show) and quic_ended_paths_below_floor
  (paths that ended in the period before 200 packets). Packets in flight at close
  are dropped by quinn, not declared lost, so a final bucket can understate the
  loss at the end. The rate is quinn's lost over
  sent (quinn-proto 0.11.19), not a measurement of the link: sent counts every
  packet quinn built, acknowledgement-only ones included; lost counts quinn's
  loss declarations, and a packet declared lost and later acknowledged is not
  taken back; lost PLPMTUD probes are left out of lost while sent ones are in
  sent, and MTU discovery is off, so no path sends any. A shadow path's packets
  are mostly keepalives and acknowledgements, so its rate may not hold for
  data-sized packets. The sums and the buckets are updated and reset under one
  lock;
  quic_session_ended_with_data_path_<age> (sessions that ended while their
  path's route still carried data); quic_streams_sampled_paths,
  quic_streams_open_ge_<8|16|32|48> and quic_streams_peak_max (per path and period,
  the peak of inbound streams read at once, counted as inbound_concurrency counts
  connections); quic_send_resets (sends a path reset on this side, counted by
  recycle::SendAborts while the connection is open) and quic_path_ms (time QUIC
  paths were up, shadow ones included, added at each tick of the acknowledgement
  watch; the last tick to a path's end is not counted), whose ratio over a long
  window is resets per connection-hour (a path whose acknowledgement watch is
  off or panicked stops adding time but not resets, so the ratio errs high;
  each is logged, at WARN and ERROR); quic_send_resets_reached_<64|512|2048|4096>
  and quic_send_resets_path_max (paths whose lifetime reset count reached each
  level in the period, and the highest count a reset in the period took a path
  to). They are for the fixed charge, which takes quinn's stopped map at
  SEND_ABORT_LIMIT x QUIC_STOPPED_ENTRY_BYTES until field data justifies less.
  quic_gate_loss_<queue|share|persistent|ecn|no_sample|ignored> (congestion
  events by the loss gate's verdict, recycle::loss_gate, reported by the path's
  report at each tick; over a path's life they sum to its quic_congestion_events,
  since quinn counts an event where it calls the controller, while one period's
  sums can differ by the events between a tick's two reads) and quic_gate_ended_queue_<lt_10pct|lt_50pct|
  ge_50pct> (paths that ended in the period after at least 20 verdicts, by the
  share passed as a queue). A path whose losses mostly read as a queue runs as
  plain Cubic, from a real queue or from jitter; with the path's loss rate this
  sizes how often jitter defeats the gate in the field.
  A family is the keys of one label list under one prefix
  (`QuicDataSnapshot::keys_by_family`). No other key starts with a family's
  prefix or with the prefix sibling families share (`quic_data_` for the send,
  event and stop families, `quic_streams_open_` for the levels), and no key starts
  with a prefix two of the snapshot's other keys share (`quic_send_`,
  `quic_paths_` and the rest), so a prefix sum never adds two kinds of count. The
  tests read the families from the keys and the other keys from the snapshot's
  serde fields, not from a list (`no_key_is_under_another_family`). The step-3
  stream-limit bars read quic_open_uni_waited over quic_sends,
  quic_stream_permit_waited over quic_streamed_sends (both counts of attempts),
  and quic_streams_open_ge_48 over quic_streams_sampled_paths.

Upgrade driver (`quic/upgrade/driver.rs`, #5940 step e5a-5b): QuicUpgrades is
per node (endpoint, policy, routes by address, live sessions by peer key, backoff
store), and exists only with an endpoint, a policy and a task registrar.
QuicPathState is a PeerConnection field; with none, as with QUIC off, there is no
task, no select arm and no GlobalRng draw. It owns one task per connection, which
runs the machine, so nothing of a negotiation runs inside `recv`, which only
`try_send`s a received Upgrade to it.
  Sends: UDP Upgrade messages are tracked packets, sent by the driver through
  UdpSender, which holds only what a send needs. It charges flight size and
  tracks the packet only after the socket took it, with no await between, so a
  send that fails or is dropped leaves nothing in flight. Control frames go through
  send_control_bounded in a task per send. Data goes by "QUIC send route" below.
  Paths: QuicPath::with_budget(.., SHADOW or DEFAULT by this node's data bit, the
  connection's inbound budget, the session's data channels), on the clock quinn runs on (PathClock: RealTime, or
  the network's VirtualTime in simulation). The machine reads the connection's
  time_source, the listener's clock. A data send on a path without both data bits
  is a debug assertion. The field is declared before inbound_budget.
  Liveness: nothing on the QUIC side, the two data arms of `recv` included, writes
  last_received_nanos or last_inbound_nanos, so QUIC inbound does not refresh the
  UDP session. An active
  path writes only quic_receipt_nanos ("Receipt stamp" above).
  With QUIC on, a UDP session differs by those Upgrade packets: they use packet
  ids, enter the sent tracker and flight size, are retransmitted and earn
  receipts.
  Promotion: the machine's Promoted is stepped from set_peer_identity on Set or
  Same only, so a dialled key alone does not start an upgrade. It first ends any
  older session of the key, through that session's lock (never held across an
  await): its path closes and its seed is filed. An older session arriving after
  a newer one ends itself. The compare and the write are one step inside
  by_key.entry(), after checking that this session has not ended, so an ended
  session adds no entry and an ended session's entry is replaced. Lock order: a
  by_key shard, then a session's state; nothing holding a session's state takes
  by_key. Session numbers order sessions in BackoffSeed filing.
  Backoff store: an LRU of SEED_STORE_ENTRIES (4096) peer keys with fixed-size
  values; the least recently used goes first.
  Attempts: the listener's attempt queue goes to a registered task
  (quic_attempt_router), which holds the upgrades weakly and routes by normalized
  address with try_send. An attempt nobody waits for is ignored and counted. The
  machine's deadline bounds a dial or an establish: its TearDown aborts the task.
  Address validation: only an attempt that returned a Retry token takes the live
  attempt's slot (Session::on_incoming, establishing). An attempt without one is
  answered with a Retry (QuicIncoming::retry), counted (attempt_retried) and given
  up with its permit and key hold; the session's own permit keeps the address on
  the allow-set. An attempt with no slot to take is ignored before this check, so
  no Retry goes out for it. Each upgrade costs one more round trip on the same
  OFFER_TIMEOUT and CONNECT_TIMEOUT, and quic_establish_ms includes it. The Retry
  and the token-bearing Initial are QUIC-class datagrams from an allowed source on
  both sides, the client having allowed the server's address when it dialled.
  Clients handle a Retry inside quinn, so any release can dial a node that sends
  one. The token is sealed with the endpoint config's token_key and bound to the
  source address, and is good for RETRY_TOKEN_LIFETIME (15 s, above
  CONNECT_TIMEOUT); past that quinn answers with a close and no attempt appears.
  quinn sends a Retry with try_send, so with the socket or the egress cap short it
  is lost like any datagram and the client's retransmitted Initial is answered
  again. A token proves the sender reads datagrams sent to that address; a sender
  that does can still take the slot, and fails the key pin.
  A connection for anything but the live attempt is closed with DOWNGRADE.
  Cleanup: ending a session removes its route and its by-key entry, each only if
  it is that session's, and drops its attempt. It runs at connection drop and if
  the driver panics. e5a-6 reads the machine's Outcome where Action::Record lands.

Upgrade policy (`quic/upgrade/policy.rs`):
`QuicPolicy::from_config(QuicPolicyConfig::new(network_api, breaker), availability)`
takes the config, the breaker's decision (`breaker::gate` returns it beside the settings, as
`Gated`) and the endpoint's availability. `disabled_by()` names the first
rule that stops upgrades: `Mode` (off), `Breaker`, `NoPaths`
(`quic-max-paths` = 0) or `Endpoint(availability)` (not `Up`); `allowed()` is
true when none does. `admit()` returns a `QuicPathPermit` or
`PathRefusal::{Disabled(cause), Busy}`; `Ok` does not prove the endpoint is
still up. `QuicPathPermits` holds at most `quic-max-paths` paths, released on
drop.

`quic-max-paths` is stored as text (`lenient_count`), like `transport-quic`,
so any value loads: an integer or string is kept as written, a bool or float
as its text, and an array, table or datetime as a placeholder. A value that is
not a whole number is ignored with a WARN at startup (only when
`transport-quic` is not off) and reads as the default, so a typo or a later
release's value cannot stop the node. On the command line a negative value
needs `=` (`--quic-max-paths=-1`): with a space clap reads `-1` as a flag and
refuses the command line, as for every numeric option.
The default is `max-number-of-connections`, and a larger value is lowered to
it.
At the default the limit rarely binds (a permit is held from the offer, so a
path still closing can briefly hold a slot). A rollback to a release before
the key drops it: the older binary rewrites `config.toml` without it. It is
kebab-case from the start, so it has no `CONFIG_KEY_SPELLINGS` entry.

An unknown `transport-quic` value is off with a warning, never an error: a
crash-loop rollback
reinstalls an older binary, which must start with a value a later release
wrote, and keeps it in config.toml. `off` itself is deprecated: it still turns
QUIC off and logs a deprecation WARN at startup, since QUIC is to replace the
UDP transport (#5940).

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
  TimeSource's connection idle timeout, Cubic behind the loss gate
  (recycle::loss_gate::LossGate, see below) with its window capped to the
  configured bandwidth (recycle::CappedCubic; QuicRateLimit::without_loss_gate,
  test-only, gives plain capped Cubic), 4 uni / 0 bidi streams and a
  16 KiB receive window until a path takes data (a data path raises the
  streams to 16, then toward 64 with load, and its meter the windows), send window the floor (128 KiB,
  64 KiB under 2 GiB), stream receive window 8 MiB, no unreliable datagrams, no
  spin bit, migration off, max_incoming 64, incoming buffers 64 KiB / 1 MiB,
  no NEW_TOKEN tokens (Retry only), rustls ring, TLS 1.3, fixed ALPN, no SNI, no
  resumption, no early data. Keys and seeds (reset key, token key, CID key,
  quinn's rng_seed, initial destination CIDs): keys from the OS, seeds and
  CIDs from GlobalRng (see below).
Connection IDs (quic/cid.rs) carry a keyed BLAKE3 MAC, so quinn drops a
  short-header packet with a forged ID silently; quinn's own hashed
  generator can be spoofed and would answer it with a stateless reset.
```

QUIC loss gate (quic/recycle/loss_gate.rs, every QUIC path by default): Cubic
cuts its window on every loss, so under a few percent of random loss (Wi-Fi,
cheap routers) its window collapses and large messages take minutes. The gate
passes a congestion event to Cubic only when: quinn declares persistent
congestion; it is an ECN mark (lost_bytes 0; the socket reads no ECN today);
there is no RTT sample yet; the queue estimated around the lost packet's send
time is at least queue_threshold = max(10% of min RTT, 2 ms) (QUEUE_FRACTION,
QUEUE_FLOOR); or the smoothed per-round loss share is at least 0.25
(LOSS_SHARE_CEILING). Otherwise the loss is ignored (quinn still retransmits,
the window stays). Limits, all known and measured in the e5b-9 lab (tables in
#6185 and #6183, which tracks the follow-up):
- min RTT is never forgotten (deliberately: a windowed minimum would let the
  gate's own standing queue become the baseline). A route that grows longer
  reads the extra delay as a queue and the path runs as plain Cubic.
- On a drop-tail queue deeper than the threshold, with no random loss, it is
  exactly Cubic. On a queue shallower than the threshold (or an AQM such as
  CoDel/PIE whose target is below it, the common case for 100-250 ms node
  RTTs) congestion losses look random and only the 0.25 share ceiling stops
  the flow, so it holds a standing queue near the threshold and a competing
  flow keeps less of the link than next to Cubic (still far more than next
  to legacy UDP).
- Jitter above the threshold reads as a queue, so the gate passes random
  losses and the path behaves as Cubic (30 ms of jitter on a 100 ms path;
  10 ms of jitter there is still absorbed).
Verdicts are exported as quic_gate_loss_<verdict> and
quic_gate_ended_queue_<bucket> (see the telemetry keys above). Do not raise
the threshold, add a threshold cap or a windowed min RTT without lab cells
for jitter, AQM and shallow queues: each was tried and broke one of them.

QUIC memory: every connection starts with a 16 KiB receive window and 4 uni
streams (`config::INITIAL_RECEIVE_WINDOW_BYTES`, `INITIAL_UNI_STREAMS`): quinn
cannot take a grant back. A path that takes data raises
the streams to 16 at build (`config::DATA_START_UNI_STREAMS`, in tests and
simulations too), doubling toward 64 once the peer has had every stream busy
at once (holding a slot with its end not yet read; a stream whole and
waiting on this node's consumer is not busy) and as many again have
completed with no gap (a hole between received runs, or a first packet
read after a later one; a lost LAST packet leaves no hole and is not
counted), with none failing (any end but a completion, a reset by the
sender, the connection ending, or an end of this node's own doing: a
consumer that cancelled, or a consumer of new streams that is gone; through
the reader's `inbound::LimitTicket`), the connection not throttled and the buffer budget
charged at most 3/4 (`budget::QuicBudget::has_room`) at the completion that
would raise (`inbound::StreamLimit`, never lowered; a waiting one-packet
Short takes one off while it waits), and its meter raises the receive window to the
floor (128 KiB, or 64 KiB under 2 GiB, `budget::floor_window_bytes`); the
send window starts at the floor, and the stream receive window is the
largest a connection grows to (`budget::MAX_WINDOW_BYTES`, 8 MiB); all are capped at
`MAX_INBOUND_STREAM_BYTES_PER_CONNECTION`, which does not bind today
(`windows_never_exceed_the_reassembly_allowance` guards future constant
changes). A shadow path keeps the initial window. A data path registers a
`budget::ConnMeter` with the node's `budget::QuicBudget` (memory / 16,
`quic_buffer_budget_for`), which charges it (W + debt) + max(staging_cap(W +
debt), the staging cap as set) (3 x (W + debt) when the cap follows the
credit; the cap is never set below the bytes staged) + send x 9/8 + R_conn x
70 B + CONTROL_BUFFER_BYTES (144 KiB) and sets its windows at runtime
(`quinn::Connection::set_receive_window` / `set_send_window`). The charge at
the floor windows (`budget::floor_grant`) is part of the data path's fixed
charge, not of the budget: the budget holds only each path's growth above
it, so a path that arrives never pushes the budget over, whatever the
others hold. The bound is
memory <= data paths x (QUIC_CONNECTION_CHARGE_BYTES + floor grant) + budget.
Until the aggregate step (PR 5) caps data paths, their number is bounded
only by the connection limit (default 200). PROVISIONAL figures at 200,
re-sized by PR 5: 1 GiB: 64 MiB + 200 x 1313.5 KiB = 320.5 MiB; 2 GiB:
128 MiB + 200 x 1577.5 KiB = 436.1 MiB; 7600 MiB gateway: 475 MiB +
200 x 1577.5 KiB = 783.1 MiB. No data runs on them: QUIC_DATA_PERCENT stays 0
until PR 5 (`no_data_share_before_the_quic_budget_is_summed`).
  → W + debt: quinn records a shrink as debt paid by later credits; the
    meter mirrors it from the bytes the reader reads (where it can differ,
    it over-estimates). Every window change and every read
    of a connection happens in its receive task: a shrink another path asks
    for is applied in the shrunk path's own evaluation, after its pending
    reads are counted, and quinn is told after the budget's lock is
    released. Keep it so, or the mirror under-estimates.
  → Growth only when window-limited on CONTIGUOUS delivery (the in-order
    prefix of each stream, `RangeMirror::prefix`, at least 75% of a window
    per round trip), at most doubling, up to 8 MiB, while the path's charge
    stays within its share C (floor grant plus the budget split over the
    paths growing) and the node's growth within the budget. Bytes read above a gap are credit, never growth.
  → A grow that does not fit asks the paths furthest above C to shrink
    toward it; the room comes as their debt is read, never by taking credit
    back.
  → A path delivering under a quarter of its window per round trip for
    budget::IDLE_DECAY (10 s) halves its receive window toward the floor,
    once per period. Credit already granted stays charged until the peer
    uses it, so a peer that grew and went silent keeps its charge while it
    stays silent: that limits other paths' growth, not memory.
  → The send window follows 2 x cwnd within the same share, lowered only at
    half. A path whose sends filled at least 75% of its send window per round
    trip while 2 x cwnd is above it counts as growing for C, and a send grow
    held back by the budget below C asks others to shrink as a receive grow
    does (budget::tests::a_later_sender_reaches_its_share). A window lowered while streams opened before it are open stays
    charged until they end, at the largest window set while they are open
    (the stream's epoch, held until its data is acknowledged). Streams
    opened before the lower window is applied to the connection take the
    old epoch.
  → The gap cap R_conn is max(256, (W + debt) / 1 KiB), set as the window
    changes (`ConnRanges::set_cap`) and never below the gaps held; it is
    charged at the cap; a path without a meter keeps 256.
  → The staging cap (below) is 2 x (W + debt), set with it.
  → Evaluated from the receive task at most once per round trip (10 ms at
    least): on reads, at each 2 s acknowledgement-watch tick, and when the
    path's sends progress (`ConnMeter::note_send` wakes the receive task).
  → A running total: an evaluation does no work proportional to the number
    of paths, except counting the paths growing (at most once per 10 ms
    node-wide) and asking others to shrink.
  Telemetry: quic_budget_bytes, quic_budget_charged_bytes and
  quic_budget_charged_peak_bytes (growth above the floor grants only: a node
  with every path at the floor reports 0, so uploads neither),
  quic_window_grew, _grow_refused, _shrank, quic_staged_peak_pct.
  Staging (path/inbound.rs, `ConnStaging`): every stream is read unordered
  from its first byte, so quinn holds nothing for a stream beyond the chunks
  its reader has not polled yet. Bytes read for a stream before its header,
  metadata or control body has been taken are staged in one buffer per
  stream, from the read position to the highest byte staged, charged at its
  allocation size against the connection's staging cap (2 x (W + debt); 2 x
  the initial window on a shadow path), checked before the buffer grows. A
  read that would pass the cap stops its stream (EVICTED, evicted_staged); a
  second within 30 s closes the path with OVER_BUDGET
  (closed_staged_over_cap). A control body stays charged to staging until
  its stream's end arrives. The factor 2 is provisional until the 3c harness
  measures honest stops under loss.
  Not metered: unread chunks between two polls of a reader (bounded only by
  how long the reader task waits to run; 3c measures it under CPU load), and
  the fixed per-connection terms (`QUIC_CONNECTION_CHARGE_BYTES`).
`quic/config.rs` pins MEASUREMENTS of named terms under stated conditions
(`quic::memory_tests`, jemalloc-rounded on linux-gnu), not a ceiling on what
a connection can hold: `QUIC_CONNECTION_FIXED_BYTES` (64 KiB, an idle
connection with its path) and `QUIC_ACK_TAIL_BYTES` (240 KiB, the
acknowledgement-only packets tracked after the last ack-eliciting send, up
to quinn-proto's cap of 1000; M2 must re-check this term against
quinn-proto's behaviour). Not measured: datagram sizes other than 1200 B,
quinn's endpoint-to-connection datagram queue, send-side packet records and
the buffers behind sent data, allocators other than jemalloc, CRYPTO-frame
reassembly (`crypto_buffer_size` 16 KiB, charged in the fixed charge). Some quinn terms depend on
traffic rate and time rather than configuration, as the UDP transport's
dedup window and sent-packet tracker do, so the per-path ceiling (memory
gate M2, #5940) cannot be the sum of these measurements alone. This
memory is NOT accounted in the node aggregate and must be before QUIC carries
data by default (before `QUIC_DATA_PERCENT` > 0, pinned by
`no_data_share_before_the_quic_budget_is_summed`). Shadow paths carry no data, so
what they hold is the fixed per-path terms, and Step 2's gate watches that as
RSS against `quic_paths_active`. The fixed
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
GlobalRng. With `shadow` or `prefer`, a failed endpoint build logs a WARN and the node
runs with QUIC off. `accept` returns the attempt without awaiting its
handshake (QuicIncoming::establish), so one stalled handshake does not hold
back the next. QUIC integration tests run under a watchdog thread
(`bounded`), so a quinn driver that spins fails the test instead of hanging
`cargo test`.

In tests, two endpoints on SimulationSocket with one VirtualTime and a
seeded GlobalRng repeat the same event trace; the bytes differ, because
rustls's key exchange and the XEdDSA nonce use the OS RNG.

In SimNetwork: `with_transport_quic` / `with_node_transport_quic` set the
config value per node (off unless set, whatever the node default: the
harness writes `off` into every node's config; an unknown value panics;
tests that start nodes outside SimNetwork, in `crates/core/tests` and through
freenet-test-network, get the platform default on purpose, as production does,
except where a restart after SIGKILL would trip the breaker), and the
endpoint runs on the network's VirtualTime (`get_socket_virtual_time`;
RealTime in production, pinned by
`the_transport_endpoint_runs_on_virtual_time_only_in_simulation`). Its
bind-time GlobalRng draw (quinn's rng_seed) runs on
`GlobalRng::with_side_stream`, so an endpoint that is bound and idle does not
shift the run's main random stream. That is all the determinism tests cover:
once connections open, quinn's connection-ID draws
(`initial_dst_cid_provider`, the CID generator) take from the main stream
inside quinn's tasks. With endpoints idle, the one difference from QUIC off
is scheduling (one endpoint driver task per node), which changes some
connection choices, so a QUIC-on run is compared with QUIC off by the peers
that join, not by the exact edge set. Every runner moves that VirtualTime: the
direct runner from tokio's clock, the turmoil runners after each step
(`run_turmoil_on_virtual_time`); before that the turmoil runners left it at 0, so
no QUIC timer fired there. A record's `at` is on its host's own clock; under
turmoil each host's starts when its runtime is built. `probe_quic_endpoints_at` sends each
node a stranger's QUIC datagram during a direct-runner run, so a test can
check every endpoint was live (`not_allowed`) rather than failed to bind.
`SimNetwork::quic_upgrade_trace` returns a handle (`quic/upgrade/sim_trace.rs`,
tests only) with what each node's upgrades did, readable after the run,
including what arrived on the path and on UDP (`received a short on the path`,
`data arrived on UDP`, noted at the receiver whichever code sent it) and what each
send ended as (`sent a short on the path: ...`). It also
injects: `send_initial_on_each_attempt` (the node receives an Initial with the
peer's address as its source each time it allows that address, and records
`injected an Initial`), `make_record_wrong` (the key compared after the
handshake disagrees, so the path closes with IDENTITY_MISMATCH),
`fail_endpoint_at` (the endpoint
driver dies, recorded as `failed the endpoint at <ms> ms` on VirtualTime) and `run_as_release` (a peer reporting the crate's version reads
as a given release, since this tree reports a version below the negotiation
floor until the release bump). The first offer is 2-10 s after promotion, so give a run tens of
virtual seconds.
Other knobs: `with_node_advertised_version` (the version a node advertises
in its intro and AckConnectionV2, to emulate an older peer of the same
major.minor; compatibility checks keep the real version),
`with_quic_idle_timeout`, and `drop_quic_datagrams_between` (drops
QUIC-class datagrams, by the demux classifier, for one pair from a virtual
time; `simulation::QuicDatagramDrop`, kept across `with_fault_injection`).
`quic/test_tls.rs` holds only a plain Ed25519 identity for tests where a
peer presents a key that is not its transport key.

#### QUIC rollout (#5940)

There are no opt-in stages; releases do the rollout. Windows is shadow by
default since its breaker (a short run counts only if it panicked in QUIC code)
was confirmed on a real Windows machine (#6031: Windows Server 2022, kills, a
reboot, a logoff, `service restart`/`stop` never counted; injected aborts and a
crash loop under the run wrapper tripped). Not run on a machine, covered by the
rule that only a QUIC panic counts: Fast Startup, sleep, tray Restart/Stop/Quit
and an update exit. What it never catches: a crash not preceded by a QUIC
panic, a crash loop with runs of 10 minutes or more, and a fault that crashes
once a day. Every step watches: peers disappearing per version
against the previous rollout (a node that dies before reporting is absent from
every rate), endpoint-down, panics and breaker trips, RSS against
`quic_paths_active`, CPU and egress, and NAT success, connections and transfers.
From the first release with the Windows default, the release watch also reads
Windows separately: `quic_adapter_panicked` and `quic_breaker_trips` by version
against the previous release, and peers with `quic_breaker_suppressed`. A
Windows QUIC panic in the field is read (location, version) before the next
release goes out.

- Step 0: the first negotiating release ships with QUIC off. Its nodes answer
  offers (`Decline{disabled}` with the key off); `upgrade_control_received`
  stays near 0 and UDP metrics do not change.
- Step 1: nova gateways and framework in shadow for 24-48 h. The 24 h gateway
  A/B runs only if something looks odd. Rollback: set the key to off and restart.
- Step 2: shadow on by default on Linux and macOS, then Windows once its
  breaker was confirmed on a real machine (`QuicMode::default_mode`). Gate: at least 95% upgrade
  success by `established_as_offerer / accepted`; fewer than 1 fallback per
  path-day excluding endpoint and restart causes; memory within the measured
  terms (about 0.3 to 0.5 MiB per idle path). An abort spike stops the step.
  Shadow carries no data, so it does not measure `MAX_UNI_STREAMS`.
- Step 3 (e5b): data on a slice of pairs chosen by a hash of their two keys, so
  both sides agree with no extra message: about 5%, then 25%, then 100%, one
  release each. Pairs outside the slice are the control. Needs identity PR3, the
  quinn pin decision and #6027. Keep the stream limits if permit waits plus
  `quic_open_uni_waited` stay below 0.5% of sends and ge_48 below 0.2% of
  path-windows.
- Going to 100% is default-on. Per slice against its control: fewer transfers
  that hit an RTO; p90 queueing at 1 MiB or more no worse than 262 ms; timeout
  disconnects no worse; CPU per byte at most 1.5x; OS egress at most cap + 5%;
  memory no worse than UDP; zero panics or endpoint-downs attributable to QUIC;
  over the slice's release; and the kill switch rehearsed once (a default-off
  release through the stagger). The stream-limit bars are re-derived from real
  data, not loosened.
- Rollback at any step is a default-off release through auto-update's 0-4 h
  stagger. Per-path fallback to UDP and the breaker work without a release. A
  node in a broken state may not self-update.

Simulation scenarios (`tests/simulation_integration.rs`, `test_quic_*`): two
nodes established with traffic on UDP; data bits on each side, where only both
enable data; data at a test percentage, where messages go on the path, come out of
`recv` and decode as real messages, with no data on UDP once the path carries it; a
state past the streaming threshold as a Stream end to end, stored by the receiver;
QUIC-off peers decline `Disabled`; peers
below the version floor are never offered; blackholed and idle paths fall back
and are re-offered; a wrong key closes with IDENTITY_MISMATCH; endpoint-down
closes every path; an Initial the peer did not send is retried and the real dial
establishes; a replaced or restarted session closes its path and keeps the
backoff; a mixed shadow / off / below-floor network forms paths only between
shadow nodes; `quic-max-paths` = 1 declines `Busy` without a charge.

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
  → The expected key comes from the node's own record, never from an
    upgrade message. A connection's transport (PeerConnection) holds its
    peer's key: the key it dialled, for an outbound connection, and, only
    when the transport has a QUIC endpoint (TransportHandle::quic_endpoint),
    the key the node promoted the connection into the ring under. The
    bridge sends that as ConnEvent::SetPeerIdentity when the ring takes the
    connection (P2pConnManager::tell_transport_peer_identity: after
    handle_successful_connection adds it, and when
    promote_existing_connection returns Promoted, once the connection is
    bound), with the key the node bound to it (identity::bind). With QUIC
    off nothing is sent. Counters (conn_identity_*): transport_key_sent
    (handed to the connection's task), transport_key_undelivered (not handed
    over within the 1 s fallback; not re-sent), transport_key_set / _same
    (the transport took it), and promotion_key_mismatch (promoted under a key
    that is not the bound one; nothing sent) and transport_key_refused (the
    transport holds another key and keeps it), which must stay at 0 (the
    simulation tripwire and test_quic_prefer_forms_a_mesh_of_the_same_peers_as_quic_off
    assert it).
    A ring member's transport can still hold no key: a key not handed
    over within the fallback is not re-sent, and a promotion that finds
    the address already in the ring (AlreadyInRing) sends none. The
    upgrade (e5a-5b) must treat a transport with no key as not eligible,
    never as "any key". An inbound transport's key is the key the node
    bound from the peer's claim, not one the transport authenticated; the
    QUIC handshake is what checks it.
Tests: conversion vs curve25519-dalek 5 (random keys, bit 255 set, u = -1),
signer key == verifier key over many keypairs with both natural sign bits,
signatures verified by ed25519-dalek and refused with any bit flipped,
nonce differs under one GlobalRng seed, handshakes in both directions, and
refusal of a wrong server key, a wrong client key, a plain Ed25519 key on
either side, and a man in the middle with its own key on either leg.
```

#### QUIC framing and path (`quic/framing.rs`, `quic/path.rs`, #5940 step e3)

The upgrade driver builds a `QuicPath` and sends control frames on it. Data goes on
it by "QUIC send route" below.

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
Sending (QuicPath::send_short_bounded / send_stream_bounded /
  send_stream_chunks_bounded / send_control_bounded; the forms without a
  deadline are #[cfg(test)]): returns when the peer's QUIC stack acked every byte, or
  Stopped(code). A message that fits in the flow window can be acked
  before the receiver reads its header, so a refusal may read as success
  at the sender, as on UDP, and so may one dropped for a full channel:
  sent is NOT delivered. send_stream_chunks_bounded resets the stream if the
  source fails or delivers a different length, and takes nothing before the
  source's first chunk (no permit, no stream; a source that ends or fails first
  opens no stream). The peer's current uni-stream limit (16 on a data path,
  rising to 64, MAX_UNI_STREAMS) is the back-pressure (open_uni waits; quinn
  returns credit only after more than an eighth of the limit is freed).
  Slot reservation: send_stream_bounded / send_stream_chunks_bounded, and
  send_short_bounded for a message past one packet (fits_one_packet), each
  take one of the path's STREAMED_SEND_LIMIT semaphore permits before
  open_uni. The stream holds it until it is acknowledged, stopped or reset,
  also past a deadline that reported MaybeDelivered: Outgoing's drop hands
  the wait to a small task (at most 48 per path, each ending with the
  connection). A one-packet Short holds no permit but its send epoch, and
  waits the same way, at most ACK_WAITS_WITHOUT_PERMIT (64) per path; past
  that its epoch is released at once and counted
  (quic_data_event_ack_wait_released) and its stream is reset, so quinn
  discards the unacknowledged packet with the epoch (the reset counts toward
  SEND_ABORT_LIMIT, for its stopped-map entry), and a peer withholding some
  acknowledgements cannot make the path hold a task or data per Short. A streamed send past the limit waits for a permit without
  opening a stream, and that wait races connection.closed(), because the
  semaphore never closes. One-packet shorts and control sends take no
  permit. The limit divides as STREAMED_SEND_LIMIT 48 + UNANNOUNCED_CREDIT_SLACK 8 +
  RESERVED_UNI_STREAMS 8 = MAX_UNI_STREAMS 64 (quinn leaves up to
  MAX_UNI_STREAMS / 8 freed slots with no MAX_STREAMS frame written). The
  reserve holds on a steady path: every streamed send still holds its permit
  or its stream was freed at the receiver, no MAX_STREAMS frame is lost,
  queued or in flight, and short and
  control messages hold no more than the reserve (pinned by
  a_short_still_sends_while_streamed_messages_are_blocked).
  Constraints on a path that carries data (none arises in shadow, which sends only
  control frames):
  → A permit can return before the slot does: on a reset or a stop from the
    peer (about one round trip), and on an ack, which is about receipt and can
    come before the receiver has read the stream to its end. That is short
    while the consumer keeps up. With a full channel the readers wait for room,
    up to the age limit, each holding a slot, so a slow consumer can use up the
    reserve: one-packet messages then fail to open and go another way. Accepted;
    quic_open_uni_waited measures it.
  → Credit the sender does not know about can exceed 8 while a MAX_STREAMS
    frame is lost, queued or in flight: quinn counts credit as announced
    when it writes the frame (sent_max_remote), so 9 freed slots on their way
    plus 8 freed since are 17 the sender cannot use, with nothing lost.
  → One-packet shorts are uncapped, so many at once can use up the reserve.
    Each holds a slot for about one round trip, and SendDeadlines::short
    gives one second to open.
  → A slot is not send capacity: a control frame with a slot still waits on
    the connection's shared flow control and send window (quinn-proto
    write_limit).
  → The division uses this node's MAX_UNI_STREAMS, not the limit the peer
    advertised (quinn 0.11.12 does not expose it). A data path's peer starts
    at DATA_START_UNI_STREAMS (16) and raises toward 64 only once its streams
    are all busy, so the sender keeps its 48 permits: capping it below the
    peer's limit would stop the raise. Until the peer has raised, the reserve
    does not hold: a send past the peer's limit, streamed, one-packet or
    control, waits in open_uni until its open deadline (2 s streamed, 1 s a
    one-packet Short) and ends NotOpened, which goes on UDP; it never ends
    MaybeDelivered, since nothing was written. Each is counted
    quic_data_event_stream_limit_full when its open deadline ends the wait
    (a send cancelled earlier, by a teardown or shutdown, is not counted;
    pinned by sends_past_a_fresh_peers_stream_limit_end_not_opened). Control
    frames are sent only during setup, but a client's ReadyAck goes out in
    the step that enables its data, so it would race its own data for the
    peer's slots: the driver reserves the frame's stream before the send
    runs (QuicPath::control_first), and no data send opens a stream until
    the control frame has one or its send ended (pinned by
    a_reserved_control_frame_opens_ahead_of_data_at_a_full_limit).
  → Once the UDP transport is removed there is no UDP to go to: a send that
    finds the peer's limit full then waits for a slot, bounded by its send
    deadline, and fails NotOpened at the deadline. Not built; the
    stream_limit_full count is what sizes that wait.
  → Each bounded send arms TimeSource::sleep_cancelable, which VirtualTime
    removes (wakeup and waker) when the future is dropped; plain sleep
    keeps its wakeup until the deadline (VirtualSleep has no drop
    deregistration, and advance_to_next_wakeup relies on that). The trait
    default is sleep; PathClock forwards it.
  → Failed(TimedOut) is a reset and counts toward SEND_ABORT_LIMIT, so
    repeated timeouts close the path. NotOpened opened nothing, so it does not
    count.
  → A NOT_ACCEPTING_DATA stop means the sender's gating is wrong: log it and
    send the message on UDP, do not retry on this path. A sender cannot rely
    on seeing it (see the stop code).
  Sends have no deadline of their own: open_uni, a flow-blocked write
  and the final wait for acks can wait as long as the connection lives
  against a peer that does not cooperate. Every node send MUST go through
  a QuicPath::send_*_bounded method (short, control, stream, stream_chunks;
  path/bounded.rs), under SendDeadlines: open (from the start of the send to
  the stream being open, permit included), gap (between two writes or chunks
  while the stream is open, and not during the ack wait, which includes a send
  window of backlog) and total. SendDeadlines::short(len) is 1 s and 5 s, or
  a stream's for a message past one packet; ::stream(len) is 2 s, 5 s
  (STREAM_INACTIVITY_TIMEOUT) and inbound_stream_age_limit(len); a plain
  Duration is one deadline for all of it. A send from a source waits for the
  first chunk for the gap, counted in total, and open runs from when it arrives.
  The gap is judged on CONNECTION progress (bounded::ConnectionWatch): a node
  send runs out of gap only once its own gap has passed AND nothing on the
  connection was newly acknowledged (recycle::AckProgress) for
  STALL_PROGRESS_WINDOW (10 s), the stall count's rule. An egress cap hold of
  this connection's transmit (recycle::cap_held, its own record) also counts,
  but only within route::hold_credit of the connection's last acknowledgement:
  the window, or socket::cap_transmit_wait (the longest one transmit can wait
  at the cap, the derivation cap_hold_validity also uses) if longer. That is
  10 s at 1 MB/s, about 13.3 s at 100 kB/s, about 19.8 s at 64 KiB/s, about
  124 s at 10 kB/s and about 6.8 h at 50 B/s. The last acknowledgement is the
  path's record (QuicPath::last_acked), kept by the stall count at settles and
  by every send's watch at its wakes, so a new send whose first transmits wait
  past its gap in a deep cap queue on a live path is credited. The record
  holds the count with the time (bounded::AckRecord), so a count it has not
  seen (an acknowledgement after the last settle or wake) is dated at the new
  send's start rather than missed. A connection
  nothing has acknowledged for the credit gets nothing from holds: a dark
  connection's own probes wait for a saturated cap too, and counted without
  that limit they kept a dark send going for 32 s. For the
  credit after its last ack a dark path's own held probes do still count (on
  main they counted for as long as they were held). Under random loss that reaches Cubic (plain Cubic before
  the loss gate, or a loss the gate passes: a queue, jitter, a loss share at
  or above 0.25) a send window (384 KiB when this was measured; set per path
  from the buffer budget since) drains at Cubic's window per round trip, so
  writes wait past 5 s while acks still arrive; failing them reset streams the
  peer was receiving (E5B9-LAB, plain Cubic, 8 MiB at 5% loss and 200 ms
  failed). The connection is read at the timer's wakes, so a send that stops
  moving ends at its own gap or within 2 x STALL_PROGRESS_WINDOW of the last
  move, never past total. It gives nothing until a wake during the send has
  seen a move, so a path dark before the send fails it at 5 s with no cap
  holding its transmits; where the cap holds them, at most at the record's
  last acknowledgement plus hold_credit plus one window (after an idle
  spell, a capped path that went dark gets one such credit from its first
  new send, whose start dates the keepalive acknowledgements from before it
  went dark); a path that
  goes dark mid-send fails it at 12 to 22 s (its own early acknowledgements
  are a move; 22 s when one lands just after the 2 s first wake; pinned at
  12 s by
  a_path_dark_mid_send_fails_within_two_windows_and_goes_suspect). Nothing
  while the send waits for its own source (the connection is not what holds
  it; a stalled source is bounded by the send's source gap,
  SendDeadlines::source_gap, which a QUIC pipe sets from the source's
  fragment_gap_limit: 5 s for a UDP-fed source, the source path's gap for a
  QUIC-fed one, below. Two SeqCst flag handshakes wake the loop to re-arm:
  when a source wait starts and drops an extended deadline (once: a timer
  already armed at the own gap is not re-armed), and when a source wait ends
  while the timer is armed at a source gap longer than the own gap, so a
  write that stalls after a slow chunk is caught at the own gap), and nothing
  without a CappedCubic controller.
  QUIC-fed streams (streaming::StreamHandle::new_quic, QuicSource): the
  reader sets the stream's path (its PathRtt, kept current at each stream
  admitted and each ack-watch tick, and its own idle limit) and records why
  it stopped (QuicSourceEnd: Idle, Age, Ended), which wakes every clone and
  fork. Consumers wait quic_fragment_gap between pieces:
  QUIC_GAP_RTT_MULTIPLE (24) round trips, at least 5 s, at most the reader's
  idle limit (30 s in production); 5 s before the reader set the path.
  assemble() applies it as its timer, read at each wait, and also ends at
  once on a recorded end; chunks() yields the end as an Err. A QUIC pipe
  takes min(the source's gap, QuicPath::receiver_gap: the gap the next hop's
  receiver allows, estimated from below from this path's minimum round trip,
  since this end's srtt includes its own queueing) once, when it starts, so a
  slow upstream feeding a fast downstream still fails at about the downstream
  gap and rarely pipes into a stream its receiver gave up on; the GET drain window
  also takes the gap once. A pipe whose sink is UDP (pipe_stream, and a QUIC
  pipe's fallback to UDP) keeps 5 s whatever its source, because its UDP
  receiver gives up at 5 s. End to end, a fast last hop still fails a slow
  chain at its own gap: the receiver does not learn the upstream gap. Control sends have no gap. A receiver reads every announced
  stream to its end whatever its consumer does (push_fragment does not wait);
  its one wait is announcing a stream into a full channel, bounded by
  inbound_stream_age_limit(len), the sender's total, so the two give up
  together. With keepalive PINGs (15 s apart) as its only acknowledgements, at
  most two wakes in a row (10 s apart) can each see a new one, so they alone
  extend a stopped send by at most two wakes. Argued, not tested.
  One timer per send, re-armed when it fires, as BodyReader. Messages are
  written in pieces of at most FRAGMENT_PAYLOAD_SIZE, each returning when quinn
  has buffered it, and each is a step of the gap and, for a stream, one
  StreamProgressObserver tick, so a slow upload does not read as stalled. A
  message that fits the send
  window is buffered at once, so its gap never applies and only total ends its
  wait. quinn gives a write all the free credit without yielding, and send
  fairness orders packets, not writes, so a streamed send yields before each
  piece but the first while another streamed send holds a permit
  (Outgoing::take_turns). Without it streams run one after another and one
  waits past the 5 s the receiver allows. Never yield after the last piece:
  the driver would send the data before the end is queued, and the end would
  take a datagram of its own. Pinned on one thread by
  streams_on_a_path_take_turns_with_their_fragments, for streams and for shorts
  past one packet; a multi-thread runtime is unmeasured, and more writers than
  a step of credit has fragments can lag (quinn wakes blocked writers last in,
  first out; about 14 fragments a step at the smallest window). A send returns
  Sent (every byte acked), MaybeDelivered(TimedOut | ConnectionLost) (the
  whole message was written and FIN queued, SendProgress set in
  Outgoing::finish, then the deadline passed or the connection was lost
  during the ack wait: the peer may have it, do not treat it as Failed),
  Failed(NotOpened(TimedOut | ConnectionLost)) (no stream ever existed, so the
  message can go another way), or Failed(TimedOut | Error) (the stream did not
  finish, or a stop from the peer, or a source that ended or failed before its
  first chunk). After MaybeDelivered or any Failed but NotOpened the receiver
  may remember the id and may have dispatched the metadata, so a resend needs a
  fresh id: under the same id it drops UDP fragments while still acking them,
  and cancels a QUIC stream.
  Dropping a send future resets its stream (Drop on Outgoing; quinn's own
  drop would FINISH it, which the receiver reads as a stream that ended
  early). A send dropped while waiting for acks after finish is not reset
  and may still be delivered.
  Each locally reset stream leaves one Arc<Notify> in quinn's per-
  connection `stopped` map until the connection closes (quinn 0.11.12:
  stopped() registers it, and a local reset produces neither the Finished
  nor the Stopped event that removes it). Bounded by the sends aborted on
  the connection: the path counts its own resets (a stop the peer sent,
  surfaced through a write or through stopped(), is not one; nor is a reset
  on a closed connection) and closes the connection past
  recycle::SEND_ABORT_LIMIT (4096) with close code SEND_ABORT_LIMIT (M6).
  The fixed charge per connection (config::QUIC_CONNECTION_CHARGE_BYTES,
  888 KiB) takes the map at that worst case, 4096 x QUIC_STOPPED_ENTRY_BYTES
  (104 B, measured 88 B). Polling stopped() only after finish() would not
  avoid the entries: a writer blocked on connection credit must still race
  stopped() (below), so the poll stays on every wait and the common resets
  register anyway.
  Every write, and the wait for a source's next chunk, races the
  stream's stopped() future and resets on a stop:
  quinn-proto 0.11.19 does not report STOP_SENDING to a writer blocked on
  CONNECTION flow control, so without the race a stopped stream keeps the
  connection's window and blocks every other stream on it. Do not write
  to a quinn SendStream without that race.
Shadow mode (QuicPathLimits::SHADOW, accept_data = false; DEFAULT
  accepts): a Short or Stream frame is stopped with NOT_ACCEPTING_DATA
  right after its header decodes, before the budget lock and before any
  metadata or payload is read, and counted (refused_not_accepting_data,
  per path). Nothing is charged to the shared inbound budget and no budget
  refusal is recorded, so QUIC in shadow can neither take UDP's room nor
  leave UDP a sticky refusal. Control frames are read as usual.
Receiving: one task per connection reads every stream (FuturesUnordered,
  no task per stream) and only uses try_send. If that task panics it is
  counted (receive_task_panicked) and the connection is closed with
  close code DOWNGRADE (the peer falls back to UDP). Short
  and Control go to bounded channels (full: dropped and counted); Stream
  goes through the INBOUND ADAPTER (path/inbound.rs).
Unordered reads: every stream is read with read_chunk(_, false) from its
  first byte, so quinn hands over each piece as it arrives, gap or not, and
  each byte once. Until the header (and metadata, or control body) has been
  taken, pieces are staged under the staging cap (above); then a Short's body
  goes into one buffer at each piece's offset (growing to the highest offset
  received, exact at the end), and a Stream's payload into the charged buffer
  at its offset (the Rechunker stages a fragment until it is whole and holds
  the one that completes the stream until its end).
  → NEVER read a QUIC stream in order (read_chunk(_, true)) on a path: every
    bound here assumes unordered reads, and a stream must not switch from
    in-order to unordered reads.
  → NEVER keep bytes read before the header outside the staging cap: an
    unordered read returns the peer's credit for every byte read.
  → A reader never stops reading, including while it waits for channel room:
    a byte past the declared end is Overlong at once, and the end may be read
    before the stream is announced, which frees quinn's slot. A charged
    message that waits is bounded by its charge; an uncharged one-packet
    Short lowers the peer's stream limit by one while it waits
    (inbound::StreamLimit), so at most the peer's current stream limit
    wait.
  → NEVER charge or refuse a one-packet Short after its body is read: its
    sender may already read it as Sent, and a refusal then loses it.
  A gap is charged as one received range (~70 B, budget::GAP_BYTES);
  gaps are counted per stream as ranges past the first, and a connection's
  streams may hold at most R_conn (budget::gap_cap, max(256, (W + debt) / 1 KiB); 256 without a meter), past
  which the stream is stopped EVICTED and counted too_many_ranges. A stopped
  stream's gaps stay counted (inbound::ConnRanges::keep), since nothing tells
  us when the peer ends it; it gains no gaps after the stop, and at most
  MAX_UNI_STREAMS stopped streams count at once: the count takes
  the sum of the MAX_UNI_STREAMS largest frozen gap counts, not of all of
  them, so it does not grow with the connection's age; after KEPT_GAPS_GRACE
  (30 s) each counts 32 less (an honest peer ends a stopped stream well
  within it). A second trip past the cap within 30 s
  (inbound::STAGED_TRIP_WINDOW) closes the path with OVER_BUDGET
  (closed_too_many_ranges). A Stream waiting for room in the
  delivery channel reads on, its end included (its charge bounds how many
  wait); one that ends short fails Truncated at once. While it waits it
  is evicted idle if its payload stops arriving; once the payload is all
  here only the age limit applies. A byte past the
  declared end is Overlong as soon as it arrives, so an overlong stream can
  fail before it is announced. GRO stays off (QuicSocket::max_receive_segments
  is 1, pinned): with GRO a 1-byte chunk would keep a whole batch. quinn-proto
  must be 0.11.17 or later within 0.11 (pinned from Cargo.lock).
Inbound adapter: a Stream's payload goes into an ordinary StreamHandle,
  re-chunked to the UDP fragment shape (fragment i = bytes
  [(i-1)F, iF), F = FRAGMENT_PAYLOAD_SIZE), so fork / assemble / pipe /
  StreamIdentity / registry are unchanged. The handle is announced
  (InboundStreamArrival, with the metadata) before the payload.
  → Before allocating: header checks, then the path's
    InboundStreamBudget check and charge (same cost and limits as UDP;
    one lock from check to charge).
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
  → No chunk is kept: a chunk may share an allocation larger than itself,
    so header, metadata, short and control reads copy into one buffer that
    grows to the highest offset received (doubling, never past the declared
    length, exact at the end; a declared length is not allocated up
    front). Stream payload goes into fragments, each a fresh buffer of
    exactly its length (Rechunker::feed always copies; no fragment shares
    an allocation with a quinn chunk, so a fragment Bytes keeps nothing
    larger than itself alive). Streams
    with no charge have an age limit too: inbound::HEADER_PHASE_AGE (30 s,
    at least the idle timeout) until the header is read, then
    inbound_stream_age_limit(0) (control frames, one-packet Shorts).
  → Idle and age eviction: streaming_handle_idle_timeout and
    inbound_stream_age_limit, counted EvictedIdle / EvictedAge, checked by
    one timer per stream on the path's clock (not the timeout_check
    tick). Every QUIC chunk is new bytes, so every chunk is progress.
    While the buffer budget throttles the connection
    (budget::ConnMeter::throttled_since), a stream that made progress in
    the idle period before the throttle began has its idle timer paused
    (the slowdown is ours); its age limit still applies and is counted
    ThrottledPastAge, never EvictedIdle / EvictedAge. A stream with no
    byte received is never paused, and a pause ends with the throttle, so
    an age limit reached after it lapsed is EvictedAge. A consumer in
    assemble() waits another gap while the throttle lasts
    (streaming::ThrottleProbe on QuicSourcePath), so a throttled stream
    does not fail at its consumer either; the reader still ends it if the
    peer is idle. The probe holds the connection's meter weakly: a handle
    that outlives its connection (orphan registry, an op, a pipe fork) does
    not keep the connection charged in the budget, and reads it as not
    throttled. A relay pipe (pipe_stream_on_path) does NOT wait out a
    throttle of its upstream path: it fails at its source gap as before.
    The pipe's source gap is already capped at the downstream receiver's
    gap (QuicPath::receiver_gap), and the downstream node cannot see this
    node's throttle, so it would give up on the stream at that gap anyway;
    waiting longer would write into a stream nobody assembles. The throttle cannot tell why a connection was held back:
    a peer sending flat out into the window it was given marks its own
    connection throttled and pauses its other active streams' idle timers,
    which gives it nothing a byte per idle period would not (each stream
    still ends at its age limit), and such holds are counted
    ThrottledPastAge too.
    In simulation each stream's last armed timer stays in VirtualTime's
    pending wakeups until its deadline (VirtualSleep has no drop
    deregistration); the timer is replaced only when it fires.
  → Refused ids are NOT remembered: a stopped QUIC stream cannot deliver
    more bytes, and a new QUIC stream with that id carries the whole
    stream. A Stream id already held on the path is refused
    (StreamIdInUse). Dropping the path cancels streams in progress.
One budget per peer (QUIC memory gate M3): QuicPath::with_budget charges
  the SharedInboundBudget of the peer's UDP connection
  (PeerConnection::inbound_budget), so a peer with both paths has ONE
  allowance; with_limits gives a path its own budget and is for tests.
  QuicPath::new and with_limits are #[cfg(test)]; with_budget ignores
  limits.max_streams/max_bytes. Charges are keyed by InboundKey:
  Udp(StreamId), Quic(StreamId), QuicShort(u32). The same numeric id on
  both paths is two charges; never charge, test or release one path's
  stream under another path's variant. UDP's take_expired sees only Udp keys
  (QUIC evicts by its own timers). live_buffer_streams yields Udp and Quic keys,
  so connection drop cancels unclaimed streams of both; the same id can appear
  twice, and StreamIdentity tells them apart.
  A gone UDP charge is released ONLY by UDP code (its check and sweep,
  Releaser::Udp), so UDP's release timing is exactly main's; a UDP-only
  node releases as before. (The dedup against restarting a stream is
  completed_inbound_streams, not the held charge.) A QUIC admission
  (Releaser::Quic) releases gone QUIC charges only. UDP's check and sweep
  also release gone QUIC charges, since QUIC reads its own gone charge as
  free; otherwise one would hold UDP's room until the path next needed
  room, or for good once the path is gone. Pinned at the call sites by
  udp_and_quic_paths_share_one_inbound_allowance (read_stream and
  read_short refused on room a gone UDP charge holds),
  a_quic_guard_releases_only_its_own_charge,
  udp_admission_releases_its_own_gone_charges_for_room,
  the_udp_sweep_releases_a_gone_quic_charge and
  quic_admission_holds_one_budget_lock_from_check_to_charge (which also
  pins Releaser::Quic in both QUIC checks). The lock is parking_lot,
  never held across .await, and not reentrant (do not call the handle's
  stream_count/charged_bytes while holding lock()); it may be held while
  taking the StreamRegistry (DashMap) locks, never the reverse. UDP
  admission holds it from check to charge, so a QUIC charge cannot take
  the room between (pinned by
  udp_admission_holds_one_budget_lock_from_check_to_charge).
  Node paths are built with with_budget. What sharing costs, each accepted as
  stated:
  → If the UDP PeerConnection is replaced (reconnect or fallback), its session
    ends and closes the path, so the path never outlives its connection's
    budget handle.
  → A UDP stream refused because QUIC filled the budget is remembered as
    refused (sticky), and MAX_REMEMBERED_REFUSALS (4096) outstanding
    refusals close the UDP connection. Same as UDP's own overload, but the
    other path can now cause it. The operation retries with a fresh id.
  → A QUIC charge whose consumer never releases it blocks UDP admissions
    too: the UDP sweep does not evict QUIC streams. An incomplete stream ends
    at the path's own timers. A complete one held by a stuck consumer also
    blocks on UDP. inbound_stream_complete_charge_past_age counts those, either
    path, once per stream. It is not a refusal, an eviction or a limit being hit:
    it counts consumers slow to take what they were given.
  → A gone UDP charge holds room against QUIC until UDP's next 5 s
    timeout_check sweep (or UDP's own admission under pressure), so QUIC
    can see BUDGET_REFUSED for up to about 5 s on room that is free.
  → The shared Arc can keep UDP charges (and their room) alive after the
    PeerConnection drops, and no QUIC code releases them. Its drop ends the
    session first, so the path and the handle go with it.
  Telemetry and logs that read the budget (sample_inbound_concurrency's
  outstanding count, the refusal warning's open_streams/charged_bytes)
  are peer-wide once shared and include QUIC charges.
  With one allowance, a large UDP stream still draining can make a QUIC
  stream refuse (BUDGET_REFUSED) where two allowances admitted it; that
  is the intended semantics, retried like any transfer failure.
Path recycling (quic/recycle.rs, #5940 step M6): besides the send-reset
  limit above, the path's receive task samples the connection every
  ACK_WATCH_TICK (2 s) and closes it with PEER_NOT_ACKNOWLEDGING when, for
  ACK_WATCH_WINDOW (10 s), this side had ack-eliciting packets outstanding,
  the peer acknowledged none of them, and the peer kept sending (no two
  samples in a row without anything from it). A peer that keeps sending but
  acknowledges none of our packets is not a conforming QUIC peer (RFC 9000 13.2.1). Newly acknowledged packets
  are counted by CappedCubic, which wraps Cubic in transport_config;
  ConnectionStats has no such counter. Two samples in a row with nothing
  from the peer end the window, and so does a sample with nothing
  outstanding (in flight after the latest acknowledgements, minus losses,
  or a send since), so recovery from an outage does not close the path.
  quinn counts a transmit's frames, and its bytes in flight, when it builds
  it, before the egress cap admits it. Guard 2 is therefore inactive on a
  connection while the egress cap holds its transmits: while the cap holds
  one of this connection's (or held one since the previous sample;
  socket::CapHolds, one record per connection since #6128, handed to the
  poller quinn creates inside connect_with / accept_with by
  socket::with_poller_holds, which relies on quinn 0.11.12 building that
  poller synchronously on the calling thread; a poller built elsewhere is
  counted per endpoint, CapHolds::fallback_pollers, and the path tests
  assert 0) no window runs on that path (pinned per connection by
  a_held_connection_does_not_pause_another_connections_ack_watch). Never node-wide:
  on a node whose cap binds, some connection is nearly always held, and a
  node-wide record kept every path's guard 2, stall count and send gap from
  firing. Do not narrow this to the sends counted since the hold:
  a held transmit is also in quinn's bytes in flight. A hold whose poller
  is not polled again within socket::cap_hold_validity of the cap's rate
  (a poller left by an ended connection) stops counting.
  e5 constraint: SEND_ABORT_LIMIT and PEER_NOT_ACKNOWLEDGING ask the peer to
  fall back to UDP for THIS connection only. They are not a permanent
  downgrade: e5 must allow a new upgrade after its normal backoff.
Control payload (quic/control.rs), the body of a Control frame, one per
  uni stream:
    version u8 (CONTROL_VERSION 1) | type u8 (0 Ready, 1 ReadyAck) |
    attempt u32 BE (the UpgradeControl attempt) | caps u32 BE
  Exactly CONTROL_LEN (10) bytes. QuicControl::decode checks the version
  first (a later version is UnknownVersion at any length), refuses an
  unknown type and a v1 payload of any other length (bytes appended at the
  end too, so a field cannot be added without a version), and clears caps
  bits outside caps::KNOWN (caps::DATA = 1, the only bit, see "Data slice").
  The Offer's caps field is separate and stays 0.
  A new type or field needs a new version. UpgradeControl differs: bincode
  tolerates trailing bytes there, so it grows by caps-gated variants. A
  sender cannot learn from these frames that the peer reads a new version,
  because caps travels in Ready and ReadyAck themselves: Ready stays at
  version 1 until the sender knows by another route, the caps of the Offer
  it received (client side only) or a release floor on the peer's version,
  as may_send_upgrade is. A refused frame reports Sent at its sender.
  QuicPath::decode_control decodes a payload from
  QuicInbound::controls and counts refusals per path
  (control_unknown_version, control_unknown_type, control_malformed); they
  are counted only through it, not by QuicControl::decode.
Counts: per path (QuicPath::stats) and node-wide ("QUIC data telemetry" above).
QUIC send route (#5940 e5b-6, quic/upgrade/data_route.rs,
  peer_connection/quic_send.rs, quic/path/route.rs):
  A message has one route, chosen when it is sent, from
  QuicPathState::data_route: the live attempt's DataRoute, held only once both
  sides set the data bit (Action::EnableData), unless the peer refused data. None
  with QUIC off, in shadow and after a teardown, which takes the attempt under the
  same lock. With QUIC off no task, arm or GlobalRng draw is added. Streams already
  running on UDP when a path comes up finish on UDP.
  NOTHING HERE WAITS IN THE CONNECTION'S TASK. A message of one packet goes through
  DataRoute::queue_short, a try_send into a per-path queue of 256 (full: on UDP at
  once, counted short_overflow_udp; closed: on UDP). One worker per path holds a
  FuturesUnordered of at most MAX_UNI_STREAMS send_short_bounded calls: without
  that cap the queue would never fill. A larger message, a Stream and a pipe each
  run as a task in outbound_stream_futures, the arm that accounts for a UDP stream.
  A larger message is a Short on the path and a stream of its own on UDP, under an
  id taken when it is sent.
  Fallback: QuicPath::settle counts an outcome and path::route::next_step says what
  happens next. A message goes on UDP only when the outcome proves the receiver
  cannot deliver it:
    (StreamSendOutcome is Delivered, Dropped, or EgressAbandoned, which only
    a UDP stream at the egress deadline reports; see "Node-wide egress cap".)
    Sent: done (StreamSendOutcome::Delivered).
    MaybeDelivered: never resent, the peer may hold it. A stream reports
      StreamSendOutcome::Dropped: quinn may still have held part of it unsent, and
      Delivered would make the broadcast queue record a state the peer may lack,
      with only anti-entropy to correct it. Dropped costs one spare full-state send.
    Failed(NotOpened), a frame that cannot be built: UDP, a stream under its id with
      its completion_tx. A pipe reads its stream again from the start.
    Failed(TimedOut), a connection lost before the end: a Short goes on UDP, as it
      needs its end to be delivered. A Stream is Dropped.
    NOT_ACCEPTING_DATA: UDP for both.
    BUDGET_REFUSED: a Short goes on UDP, which charges none. A Stream is Dropped.
    Any other stop, Source, LengthMismatch: Dropped, counted (stops by code).
  A stream that opened never goes again under its id: the receiver may remember it
  and drops UDP fragments under it while still acking them. The operation layer
  retries with a fresh id, as after a UDP stream failure, so a Dropped stream ends
  as OutboundStreamFailed, which is stream-scoped. A source that ends or fails before
  its first chunk is Dropped too: it has nothing to send on any route.
  UdpSender sends a Short from the worker, and UdpStreamSender is what send_stream
  and pipe_stream take of a connection, so flight size is charged by the same code
  and rules as on UDP (#4345). A send that stays on the path charges none.
  The worker is not one of the attempt's tasks, which are aborted. It watches
  connection.closed(), closes the queue and hands what it holds to UDP, so a path
  that ends costs only what may already have arrived. A message taken in the gap
  before the close is seen fails to open and goes the same way. The UDP send of a
  session that has already ended goes nowhere, as on a dropped connection.
  A path that stops moving: STALLED_SENDS_LIMIT (8) timeouts in a row close it with
  DOWNGRADE (stalled_closed), but only timeouts while nothing on the connection was
  newly acknowledged within STALL_PROGRESS_WINDOW (ACK_WATCH_WINDOW, 10 s), read
  from CappedCubic's record (recycle::AckProgress) when a send settles
  (route::StallCount). Any acknowledgement or any Sent starts the count again. All
  three kinds count: Failed(NotOpened(TimedOut)), Failed(TimedOut),
  MaybeDelivered(TimedOut). NotOpened must count: a path whose permits are all
  held by sends stuck waiting for acknowledgements is recycled only by this close.
  The window must stay: without it a busy relay whose 48 permits hold transfers
  that are being acknowledged, or a receiver whose consumer is slow while its QUIC
  stack acknowledges, is closed and loses what the receiver's readers hold. Known
  gap: a consumer stalled past the window while nothing else on the path is
  acknowledged reads as a stalled path. An egress cap hold of THIS connection's
  transmit counts as progress, by the ack watch's rule (recycle::cap_held,
  shared), while total-bandwidth-limit holds them nothing can be acknowledged;
  but only for route::hold_credit after the connection's last acknowledgement
  (as seen at a settle): the window, or the longest one transmit can wait at
  the cap if that is longer (10 s at 1 MB/s, about 13.3 s at 100 kB/s, about
  19.8 s at 64 KiB/s). A live path whose transmit the cap holds is acknowledged
  again within that, so it is not stall-closed (pinned at 50 B/s and at
  100 kB/s); a dark connection whose own probes the cap holds stops counting
  them after it. total-bandwidth-limit has no floor (MIN_BANDWIDTH_LIMIT only
  floors the per-connection bandwidth-limit), so the credit grows without bound
  as the cap falls. The real bound on a busy capped node: a path gone dark goes
  suspect at a send timeout at least hold_credit after the last acknowledgement
  the path recorded (dated when a settle or a wake saw it, and moved back to a
  quiet proof's since when one applies), so one credit plus one gap. The credit
  covers this node's cap only: a peer whose own cap is saturated delays its
  acknowledgements, and below about 137 kB/s (where cap_transmit_wait reaches
  the window) that delay can pass a window,
  which reads as a quiet path here (not new with this rule); until then new streams that time
  out after opening are dropped (as before), a short that times out before its
  end is written goes to UDP, and a one-packet short (written whole at once)
  ends MaybeDelivered and is not resent, so on a dark path it can be lost in
  that time, as before suspect existed. A send that
  ran out of its gap leaves a QuietProof (the acked count and hold count, and
  since when they stood still); while the counts still match, the stall count
  dates the progress it sees from then, so the first timeout after a path goes
  dark mid-send counts and the path goes suspect at once. The path has one
  slot, read (not taken) by every settle; a later timeout's proof of the same
  counts is merged into it (earlier since kept), so it cannot push an earlier
  send's date forward. Both counts only grow, which is what makes a matching
  proof sound. A pipe that timed out
  waiting for its source does not count (WatchedSource). It costs a normal
  backoff.
  Fast path: Session::routed is an atomic set only while the live attempt holds a
  route, so a connection with no route (shadow, the default on Linux, macOS and Windows)
  sends without taking the session's lock. enable_data runs once per activation
  (debug assertion); a second route would orphan the first one's worker.
  A path gone dark: the first send timeout that counts toward the stall close
  (nothing acknowledged for the window, no cap hold) makes the path suspect
  (QuicPath::is_suspect, suspect_entered). data_route() then gives no route and
  the worker sends what it takes on UDP, so new data goes on UDP while sends
  already on the path stay there (a resend could deliver twice). The first time
  it is asked after the acknowledgement count moved (quinn's keepalive brings one
  within 15 s on a live path), the path takes data again (suspect_left). Without
  it every short into a dark path ends MaybeDelivered(TimedOut), is never resent
  and is lost, and an operation's retry lands in the same place until quinn's
  idle timeout. A path that stays dark ends on quinn's idle timeout (120 s in
  production, Fallback(Idle)), not on the stall close: once suspect no new send
  reaches it, so the stall close fires only when STALLED_SENDS_LIMIT sends were
  already on it. New data goes on UDP meanwhile, so nothing is lost. Entering
  and leaving are under one lock. Fallback is a testing-period crutch: this
  routing goes with it when the legacy transport is removed.
  Receiver side: a path whose own data bit is 0 stops the first data frame with
  NOT_ACCEPTING_DATA and closes the path with DOWNGRADE REFUSAL_CLOSE_DELAY (2 s)
  later (closed_refusing_data, a WARN once per path). The delay lets the stops
  reach the sender first: a sender that sees the close before a stream's stop
  drops the stream, where the stop sends it on UDP. Without the close, a Short of
  one packet is acknowledged before its stop arrives, so its sender reads Sent,
  never clears send_data and loses every short on the path. The pair then
  negotiates again after the normal backoff. Residual: a one-packet short sent
  in the 2 s window is acknowledged before its stop is seen and is lost.
  NOT_ACCEPTING_DATA at the sender: this node's gating or the peer's is wrong.
  Logged once per path, counted (data_refused, and every time), and new sends go
  on UDP; the receiver closes the path. send_data has three states so a send racing the refusal does not trip
  debug_assert_carries_data. A small send acked before the stop is lost, and that
  count is the only sign of it.
  Telemetry: a transfer on a path emits transfer_started, _completed and _failed
  and reports through the same arm. What comes from the UDP congestion controller
  and sent tracker does not exist there: those fields are None or 0. A send that
  falls back reports two starts under one id. The counts are per path
  (QuicPathStats::send_count, send_event_count, stop_count).
  Clocks: a send's deadlines are on the path's clock (DataRoute::clock), a transfer's
  duration on the connection's. They differ in simulation.
QUIC receive flow (#5940 e5b-4):
  The session owns one pair of data channels for its lifetime (data_channels; 256
  each) and every path it builds delivers to them, always with try_send. The
  connection holds the receivers (quic_shorts, quic_streams), read by two guarded
  arms of recv's select, so nothing waits in the connection's task. Each channel's
  recv is cancel-safe and each arm's body runs to its end with no await. With QUIC
  off the guards are false: the 9-branch select gives a disabled branch no place in
  the order and no part in the random draw, so the poll order and the GlobalRng
  draws are those of the 7-branch select it replaced
  (nine_branches_with_the_extra_guards_off_poll_as_seven).
  A message already in a channel when its path ends is still delivered; a reader
  still waiting for room in a full channel is dropped with the receive task and
  counted (short_abandoned_waiting, stream_abandoned_waiting).
  Short arm: put the hash in dispatched_msg_hashes, release the charge at once
  (release_if_gone on the key the short carries), return the bytes. A Short is never
  suppressed. Short after the same embedded metadata dispatches twice, as on UDP.
  Stream arm: an id the budget holds as Udp, or in completed_inbound_streams, is
  cancelled and counted (inbound_stream_stream_id_on_both_paths). Otherwise the id
  goes in completed_inbound_streams, so a later UDP fragment is dropped as
  AlreadyCompleted, and quietly: only the arm counts. A handle already cancelled (it
  ended while it waited in the channel) is dropped with its charge. Otherwise it goes
  to the InboundStreamRegistry and its embedded metadata through
  is_duplicate_dispatch. The handle is not in streaming_handles: the reader task
  pushes fragments and evicts by its own timer. The id is put in
  completed_inbound_streams at arrival, not at completion: that LRU holds
  COMPLETED_STREAM_MEMORY (MAX_INBOUND_STREAMS_PER_CONNECTION) ids, so an id is
  forgotten after that many newer ones, and an eviction means a late UDP fragment
  or a reused QUIC id with that id is taken for a new stream instead of refused.
  Full channels: a Short or Stream whose channel is full waits in its own reader
  future (reserve, raced with the stream's age limit) and is stopped EVICTED past
  it; Control is still dropped and counted. A waiting Stream keeps reading its
  payload into its charged buffer (evicted idle if the payload stops
  arriving; once it is all here only the age limit applies), so the wait does
  not hold the sender's flow-control credit; its end stays unread until it is
  announced. A Short waits BEFORE its end is read
  (reading the end frees the stream and returns its credit), so waiters are
  capped by this node's current stream limit for the peer (16 on a data
  path, rising to 64). A Short of one packet or less
  (fits_one_packet) is not charged, as on UDP, and is bounded by that cap and
  the channel. A Stream arm with no registry
  cancels the handle and releases its charge.
  When a stream's charge goes:
  → Reader ends with no consumer holding the buffer: at once (StreamGuard).
  → Cancelled and registered, unclaimed: the orphan GC drops a cancelled row within
    its 5 s interval, then the UDP sweep (5 s) or the next admission short of room.
  → Consumer lets go later: the UDP sweep or the next admission short of room.
  → Complete and unclaimed with the path gone and the connection alive: the orphan
    timeout (60 s), as for UDP.
  → Connection drop: PeerConnection::drop ends the session and cancels every
    unclaimed stream of both paths. Nothing needs the room after that.
  A UDP session's timers keep running: nothing here writes last_received_nanos or
  last_inbound_nanos. sample_inbound_concurrency does not run on these arms.
  record_inbound_completed does: a Short counts its payload at the arm, a Stream
  counts its declared length at arrival (UDP counts at completion, so a QUIC stream
  that later fails is counted too).
  take_complete_past_age compares the sweep's clock with each charge's admission
  clock. Production shares one (the connection's) across both paths; in
  simulation the QUIC path charges on the virtual clock and a UDP sweep may pass
  another, so there the comparison is across two clocks.
```

#### QUIC errors, close codes and liveness (`quic/error.rs`, `quic/liveness.rs`, #5940 step e4)

Live in production on Linux, macOS and Windows, where shadow is the default: the
paths carry control frames only, but endpoints, connections and their errors
are real. With `transport-quic` off no endpoint is built and the transport
reports `Disabled`.

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
    from CANCELLED, which means the consumer is gone), NOT_ACCEPTING_DATA
    (the path accepts no data for as long as it lives) and LENGTH_MISMATCH
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
    ConnectionRefused (CONNECTION_REFUSED), Downgrade, IdentityMismatch,
    SessionEnded, OverBudget, VersionIncompatible and HelloRefused (the
    peer's close codes), AllowSetFull, KeyConflict, UnusableKey, Local.
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
                DOWNGRADE 1 (fall back to UDP), IDENTITY_MISMATCH 2,
                SEND_ABORT_LIMIT 3 and PEER_NOT_ACKNOWLEDGING 4 (path
                recycling, M6; the peer reads both as Downgrade),
                CONTROL_REFUSED 5 (a control frame of an unknown version or
                type arrived during path setup; the peer reads it as
                Downgrade, a node without this code as ConnectionLost),
                SESSION_ENDED 6 (the UDP session ended; sent by the upgrade
                machine's session-end shut-off and for a dial that connects
                after the session ended; the peer reads it as
                Refused{SessionEnded}, falls back and charges as for Downgrade,
                and counts quic_fallback_session_ended_rx, or
                setup_downgrade_rx in setup; a node without this code reads
                ConnectionLost: quic_fallback_reset, or
                quic_upgrade_handshake_failed in setup), 0x07 left to the
                ramp branch's RECYCLE_FOR_DATA, OVER_BUDGET 8 (the peer held
                more of this node's QUIC buffer budget than it allows: its
                gaps or staged bytes past their cap a second time within 30 s; the peer reads it as
                Refused{OverBudget}, falls back, charges as for Downgrade and
                counts quic_fallback_over_budget_rx, or setup_downgrade_rx in
                setup; a node without this code reads ConnectionLost),
                VERSION_INCOMPATIBLE 9 (the peer's native Hello carried a
                version this node cannot talk to; the reason is this node's
                8 version bytes, sent with QuicCloser::close_with_reason) and
                HELLO_REFUSED 10 (the peer's native Hello was malformed or
                past HELLO_MAX). Both are sent only on native connections
                (#5940 QUIC-native connect); this node reads them as
                Refused{VersionIncompatible} and Refused{HelloRefused}, and a
                node without them as ConnectionLost. On an upgrade path, where
                nothing sends them, the upgrade machine treats either as a
                lost connection (handshake_failed in setup, reset once
                active), as a node without the codes would.
    0x10..=0x1f STOP_SENDING: path::stop_code (receiver refusals;
                NOT_ACCEPTING_DATA 0x19 is a shadow path refusing data. Data
                may be sent only after caps negotiation, so it is a counter
                and not a fallback signal a sender can rely on: a small
                message can be fully acked before the stop arrives)
    0x20..=0x2f RESET_STREAM: path::reset_code (sender gave up)
  The scrape test checks every constant by name against ALL, and
  close_code::ALL against the QuicCloseCode variants.
  The close codes are wire values a peer on another release reads: never
  renumber them; add a new code at the next free value (0x0B; 0x07 is
  taken by the ramp branch). A code the peer does not know reads as
  ConnectionLost there; what this node does with each code it knows is
  listed above. Every close the QUIC code sends
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
  → quinn's driver returns only on a receive I/O error. QuicSocket returns
    one only after it caught a panic (below); tests inject one
    (socket::SocketFaults::fail_receive).
  → PANICS. The node relies on panic = "unwind". A build with
    panic = "abort" still compiles, but QuicEndpoint::bind refuses
    (require_unwinding, through bind_checked so tests drive it) and the
    node runs with QUIC off; log_startup says so instead of describing the
    configured mode. This covers a profile-level abort only.
    CONTAINED: a panic in the socket adapter (QuicSocket::poll_recv,
    try_send, QuicPoller::poll_writable), which quinn calls holding its
    endpoint or connection state lock. The adapter catches it before it
    unwinds into quinn (no lock poisoned), and
    EndpointHealth::adapter_panicked turns the endpoint Down, counts
    quic_adapter_panicked once per endpoint and logs ERROR once (quinn
    also logs its own ERROR "I/O error" for each of its drivers that ends
    with the error, so expect up to one such line per connection). The call
    returns an io error (never Ready(Ok)/Ok, which would make a lasting
    fault repeat on every transmit), ending that driver. From then on
    every adapter call returns that error before the adapter's own code
    runs (one AtomicBool load), so a lasting fault is hit once per
    endpoint, and the adapter wakes the endpoint driver so it ends
    promptly.
  → After an adapter panic NOTHING more leaves the endpoint: no
    CONNECTION_CLOSE reaches any peer, and peers find out from their own
    idle timeout. Sending one would not work anyway: EndpointDriver::drop
    drops each connection's event channel, and a connection driver that
    sees that ends with INTERNAL_ERROR before it transmits (quinn 0.11.12
    ConnectionDriver::poll). Pinned by
    an_adapter_panic_ends_every_connection_and_counts_once.
  → On EVERY Down (a returned driver too) each connection's watcher closes
    it locally with DOWNGRADE; that is what fails calls waiting on a
    driver that ended. QuicCloser counts a close in quic_close_sent_* only
    while it can be sent (EndpointHealth::closes_can_be_sent: not Down,
    no adapter panic), so the watcher's close and any close after an
    adapter panic are not counted. Released alone closes nothing. An
    adapter panic after Released is counted and logged but leaves it
    Released; since every send of the endpoint then fails (which can end
    any connection driver without closing its connection), each watcher
    also closes its connection when EndpointHealth::adapter_panic_watch
    turns true (pinned by an_adapter_panic_after_release_closes_its_connections).
  → connect refuses before quinn sees the attempt when the endpoint is
    Down or the adapter flag is set (EndpointHealth::refuses_connects).
    After a driver return quinn already refuses; after an adapter panic it
    refuses only once the woken endpoint driver has run again and been
    dropped, and on a multi-thread runtime a connect in that window would
    send an Initial (pinned by
    after_down_nothing_reaches_the_wire_before_quinn_notices).
  → The try_send flag check is the only guard on quinn's endpoint-driver
    respond() path (stateless reset, version negotiation, Retry, a close
    for a refused attempt), which calls try_send with no poll_writable
    (pinned by the_adapter_refuses_sends_after_a_panic_without_running).
  → These claims depend on quinn 0.11.12 internals, so quinn is pinned
    "=0.11.12" in crates/core/Cargo.toml; re-check them before bumping it.
  → A handshake in flight ends with an error that reads as EndpointDown,
    through quinn: an adapter panic in that connection's driver ends the
    driver with an error, and dropping it drops the second-to-last
    connection reference, so ConnectionRef::drop closes it; a panic
    elsewhere ends the endpoint driver, whose drop ends the connection.
    connect/establish release their permit. Pinned by the
    a_panic_during_*_ends_the_handshake_* and
    a_*_panic_during_connect_ends_the_handshake tests.
  → QUIC stays off until the node restarts. Also contained, per
    connection: a panic in the path's receive task (DOWNGRADE that
    connection, receive_task_panicked, not quic_adapter_panicked).
  → NOT CONTAINED: a panic in quinn's own endpoint- or connection-driver
    code, or in the node's runtime adapter that quinn calls under the same
    lock (QuicRuntime::now, QuicTimer::reset/poll and the TimeSource's
    sleep_until; they have no error to return a panic as). quinn 0.11.12
    polls each driver holding a std Mutex it locks with .lock().unwrap(),
    so the panic poisons it, and quinn's destructors unwrap the poisoned
    lock. quinn spawns the endpoint driver inside its own async block, the
    unwind drops it there (before any catch around the spawned future),
    and EndpointDriver::drop (endpoint.rs:403) then EndpointRef::drop
    (:724) lock the poisoned mutex: always an abort, pinned by
    a_panic_unwinding_through_the_endpoint_driver_still_aborts. SendStream,
    RecvStream and the last Connection handle lock in Drop, so a
    connection-driver panic can abort when another task drops one. The
    fix is upstream in quinn (tolerate a poisoned lock in those
    destructors, both :403 and :724 for the endpoint driver). Do not add
    catch-and-leak machinery around quinn here: it cannot cover the
    stream and connection destructors, which run in the node's own tasks.
  → Telling them apart: a caught adapter panic leaves the process running
    (ERROR log, quic_adapter_panicked, quic_endpoint_down) and leaves no
    crash-loop breaker state, so QUIC comes back on the next start, and a
    deterministic fault recurs once per start. A panic that is not
    contained aborts the process. On unix the next start trips the breaker
    (quic_breaker_trips, see quic/breaker.rs). On Windows the breaker does
    not look at how a run ended: a panic hook records a panic located in QUIC
    code in the run's marker, and it counts runs that lasted under 10 minutes
    and recorded one, so the fourth start after three such aborts in a row
    trips it. An ordinary stop records no panic and never counts; an abort
    after 10 minutes or more never does, nor do aborts a day apart
    (quic/breaker/short_runs.rs). A panic counts as QUIC's when the thread
    is inside a QuicCode guard (quinn's driver polls in runtime.rs, the socket
    adapter's catch_unwind calls) or, failing that, by location. The hook is
    process-global: only a child-process test installs it (the arming tests
    pass a no-op installer).
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
  → QuicEndpoint::spawn_accept_loop registers the accept loop through a
    LateRegistrar as quic_accept_loop. The node starts it when it opens the
    transport with an endpoint (UdpTransportConfig::task_registrar; tools
    and tests that pass none start no loop). It holds an EndpointCore
    (no owner share), so it does not keep the endpoint alive: when the
    node drops its last handle it sees Released, drops the core (quinn's
    driver ends, the socket is let go) and stops. It also stops when
    accept() returns None or the consumer is gone (WARN). It PARKS
    instead of returning, because the monitor treats any end of a task as
    a node failure, and drops its Sender first, so the consumer's recv()
    returns None once drained. The parked task is aborted when the node
    stops: when the monitor's wait future resolves or is dropped, it aborts
    every task registered through a LateRegistrar (a JoinHandle dropped
    alone only detaches its task), and one registered after that is
    aborted at once. A monitor never waited on (in-memory simulation
    nodes) aborts them when it and its last registrar are dropped. Attempts still queued each hold a quinn
    Incoming, which holds the endpoint: they keep quinn's driver and the
    socket alive until drained or dropped. Call it once per endpoint (a
    second call warns and starts nothing). It hands attempts on with
    try_send; a full queue ignores the attempt and counts it
    (accept_queue_full, WARN on the first and every 1024th). Cloning an
    EndpointCore keeps quinn's endpoint, driver and socket alive WITHOUT
    an owner share; only the accept loop holds one.
  → run_node drains the monitor (wait_for_any_exit) BEFORE the event
    listener opens the transport, so a task registered from the
    transport's open path with register() is not monitored (logged at
    ERROR). BackgroundTaskMonitor::late_registrar() gives a handle whose
    registrations, before or after the drain, the future returned by
    wait_for_any_exit also watches; use it for any task started from the
    transport's open path.
Tests: one row per quinn variant and per stop/reset code in
  quic/error/tests.rs; each close code sent by either side and seen by
  the peer as that code and kind; the receive-task panic seen as
  DOWNGRADE; a panic in the socket adapter (receive; and a lasting one in
  send or write readiness, hit exactly once) turns Down, stops upgrades
  (QuicPolicy) and leaves the process running (child process, so an
  abort fails one test); with two connections both end, the fault is
  counted once and nothing more leaves the endpoint; in the window before
  quinn notices Down, sends and connects put nothing on the wire; a panic
  during a handshake (write readiness, send, receive) ends it; a panic
  through quinn's endpoint driver still aborts (pinned); a driver ended by
  an injected receive error turns Down,
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
per-peer InboundStreamBudget (inbound_budget.rs) BEFORE any state is
created from its declared length.
  - Limits: MAX_INBOUND_STREAMS_PER_CONNECTION streams and
    MAX_INBOUND_STREAM_BYTES_PER_CONNECTION bytes (one maximum-size
    stream's cost plus headroom for ordinary streams).
  - A stream's cost is ChargeOverhead::cost(declared): its length, a fixed
    per-stream amount, and PER_FRAGMENT_OVERHEAD for every fragment number
    its length allows. Admission (check) and the charge use the same cost.
  - Both reassembly paths hold fragments as received and assemble once on
    completion: the memory held is the fragments received.
  - The bound is per peer: the UDP connection's budget, shared with a QUIC
    path to the same peer (SharedInboundBudget, keyed by InboundKey).
    Node-wide fairness is deferred to the transport redesign (exception
    recorded in bug-prevention-patterns.md;
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
