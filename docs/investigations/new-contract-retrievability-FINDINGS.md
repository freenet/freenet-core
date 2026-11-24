# New-contract retrievability investigation (branch inv/new-contract-retrievability)

## Status
- Worktree: freenet-core/main/.claude/worktrees/retrievability-inv (base e9613f905)
- Sim harness added: investigate_new_contract_retrievability in crates/core/tests/simulation_integration.rs (#[ignore]).
  Run: cargo test -p freenet --features simulation_tests,testing --test simulation_integration investigate_new_contract_retrievability -- --ignored --nocapture
  Env: RV_NODES RV_MAXCONN RV_SEEDS RV_DELAYS RV_PUT_AT_NODE

## Code reading so far
- GET client: advance_to_next_peer takes k=1 per attempt, MAX_RETRIES=3 => max 4 distinct first hops (get/op_ctx_task.rs:2551,2605).
- Each relay forwards ONCE (MAX_RELAY_RETRIES=1, relay_advance_to_next_peer 3775), then consults neighbor ADVERTISEMENTS (<=2 hosts).
- GET next-hop = router select_k_best_peers over ALL unvisited connections (no strictly-closer filter on GET), distance-sorted or prediction-sorted.
- No gateway exclusion in k_closest_potentially_hosting (ring.rs:4406) => hypothesis (a) not supported by code.

## Real-network repro (scratch nodes A/B on nova, ports 7701/7702, 20-26 conns each; throwaway unlisted River rooms via riverctl on A, fdev get on B)
Release 0.2.164 nodes, INFO logs (results in multi.log): GET from B, 45s client timeout, delay after create:
 3s FAIL, 3s FAIL, 30s OK, 30s OK, 90s OK, 90s OK, 180s OK, 180s FAIL(45s; succeeded at 74s via retry after "stream assembly failed").
Debug build of main (6032eba26) nodes, 150s timeout, 6 rooms at 3s delay (multi2.log): all OK but 63s, 25s, 11s, 10s, 1s, 1s.
Debug log evidence (nodeB/log, key 24MhrKNn..., key loc 0.0614):
 - 20:47:32 B forwards GET to closest peer (loc 0.0036); no reply for 60s (attempt timeout), retry to next peer 98.97.x gets ResponseStreaming in 0.6s. One silent first hop = 60s.
 - key 6Qwa (loc 0.314): attempts 1,2 went to peers at 0.352, 0.241 and returned NotFound after 2-4s; attempt 3 to 0.927 NotFound; attempt 4 to 0.924 (distance 0.39 from key!) returned the state. Greedy walks from the nearest first hops do NOT reach a holder; a copy was found only at a peer near the PUT ORIGIN, not near the key.
 - A's PUT for 6Qwa: client put at 20:49:04, "summary-first" probe returned only at 20:49:18 (14s) and the real PUT was forwarded at 20:49:18. B's GET (20:49:07) therefore ran before the PUT was on the wire. (put/op_ctx_task.rs ~960-1060; riverctl returns/hosts locally before the PUT is forwarded.)
## Sim (investigate_new_contract_retrievability, 25 nodes, gateway PUT originator, max_conn 8)
 delay 3s: requester ranks 8 and 16 find (2/2 seeds each); rank 12 FAILS 2/2 seeds. delay 120s rank 8 2/2, rank 12 seed0 OK. PUT copies land near key (nearest holder dist 0.016-0.02, 10-13 holders), so (b)/(d) not the sim cause; the failure is a requester whose greedy walks miss holders and is time-dependent (resolves later) => reproduces the "not findable at first, findable later" shape.
## Hypotheses
 (a) gateway exclusion: killed by code (ring.rs:4406 no gateway filter).
 (b) PUT copies far from key: not in sim; real net unconfirmed (6Qwa suggests holders near origin were what GET found).
 (c) retry budget burned: CONFIRMED mechanism: 4 first hops max, each gets one greedy forward + <=2 advertised-host consults; B-type nodes burn attempts on NotFound; a silent hop costs 60s.
 (d) PUT not propagated yet: CONFIRMED contributor: summary-first probe delays the real PUT by 0.6-14s+ (probe_round_trip_timeout may be longer) while the client already sees success.
 Not explained: 19-31 min on try's node (deterministic first hops + no hosting advertisement seen until later). Needs gateway-side debug or try-node debug logs.

## 19-31 min window: which peers did the failed GETs try?
NOT determinable from the logs. The try node's INFO log (freenet.2026-11-24-19.log) has, per failed GET, only the client tx, "ring_connections=38-40", and "stopped advancing after 4 peer attempt(s) ... last peer replied retry" (op_ctx.rs:1003); the per-attempt peer is logged only at DEBUG ("GET relay: forwarding request to downstream peer"). The gateway log (/home/freenet/.local/state/freenet/) is not readable by this account (permission denied). So "same 4 peers every time" is unverified. Each failure took ~6s (19:06:10 -> 19:06:16), i.e. 4 quick NotFounds, no 60s timeouts. Success came at 19:34:08 with the state 2278 bytes. Next step: run the try node (or a copy) at debug, or read the gateway log with sudo.
