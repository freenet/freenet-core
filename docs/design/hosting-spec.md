# Demand-Driven Hosting — Build Specification

> Detailed build-spec companion to [`demand-driven-hosting.md`](demand-driven-hosting.md) (the design-intent WHY-and-HOW doc). This spec adds the mechanism-level detail, constants, and `file:line` cites. Where this spec and `.claude/rules/hosting-invariants.md` disagree, the invariants file is the most-current synthesis and wins.

*This is the build-level spec for Freenet's demand-driven hosting redesign. All the design decisions are locked; the job here is to state each mechanism so a human can review it and an implementer can build it. Every section leads with plain language and keeps the exact constants and `file:line` cites in a short* Grounding *note at the end. File cites are branch `df1ac453` unless noted; main offsets differ by a small amount.*

---

## 1. Overview

**One idea:** a peer either **hosts** a contract or it doesn't. Hosting means it has the current data, keeps it up to date, and answers reads for it. There is no in-between tier — no "sort of cached," no "held just in case." A contract lives on a peer only because real demand put it there.

Everything else follows from four stable pillars:

- **Hosting-or-not.** No middle category. "cache," "relay," "seed" are *actions*, never *states*.
- **Demand-driven.** Nothing is hosted speculatively. A peer hosts because a read or subscription routed to it, or because it sits on the path a write took toward the key.
- **Freshness-by-proximity.** Two connected peers that both host a contract keep each other current, automatically. That neighbor-to-neighbor spread is the *only* way updates travel — there is no authoritative master copy. A peer is "fresh" when it is wired into that web through a link to some peer **closer to the contract's key** than itself. Follow those closer-and-closer links and you reach the single closest peer, where new writes first land.
- **Find-by-connectivity.** To find a contract, a request just heads toward its key. It arrives as long as peers are connected to their nearest neighbors. Finding is a question about *connectivity*, not about scattering copies around.

The state of a contract is a **commutative monoid**: any two copies merge order-independently, so there is never a fork to resolve and no notion of one copy being globally "ahead" of another. That property is what lets propagation be proximity-only and lets a peer serve its best copy immediately while it re-links in the background (**serve-DURING**). There is deliberately **no freshness flag** on the wire: the store is eventually consistent, and apps that need a hard freshness guarantee build it at the application layer.

The mechanism sections below specify: how freshness spreads, where contracts get placed, how a full peer makes room, how a peer re-finds its place, how writes work, how "I'm full" is signalled, how a protected contract degrades under load, the single controller that drives maintenance, the retirement of the standalone SUBSCRIBE op, and the removal of a redundant old fan-out path.

---

## 2. Mechanisms

### Freshness & Propagation

**What it does.** Keeps every hosted copy current without any peer holding an authoritative master. Two connected peers that both host a contract heal each other automatically, and that neighbor-to-neighbor spread is the only way updates move. "Fresh" is not a timestamp — it is being wired into the co-host web through a link to a peer closer to the key.

**The rule.**
- A peer **hosts** a contract if and only if it has the state AND one of: an **upstream** (a live connected co-host strictly closer to the key), or it is **actively acquiring** one, or it is the **locally-verified root** (the closest peer, see Re-rooting).
- Updates propagate two ways, both demand-independent:
  - **Live fan-out.** A committed update is sent to ALL connected advertised co-hosts, with no demand or interest check.
  - **Periodic anti-entropy (InterestSync, ~5-min heartbeat).** Peers exchange their full set of interest hashes, unfiltered. On a hash mismatch, the newer state heals the staler via a targeted `SyncStateToPeer`, bidirectionally. A cached-only copy registers as "interested" (`hosting = true`), so demandless copies participate in anti-entropy just like subscribed ones.
- A peer serves a read from its **best local copy immediately** (serve-DURING). It never blocks a read to confirm freshness or to re-root.
- There is **no freshness flag** on `GetResponse`. Do not add one.

**Why it's shaped this way.** Freshness-by-connection makes the stale-host failure class structural rather than policed: a served copy is current as long as the peer is linked into the mesh, so there is no flag to keep accurate and no way for a durable unsubscribed copy to answer a read as authoritative. Proximity-only propagation needs no global coordinator — the monoid merge means any two copies converge, so "near-key peers just host more from routing gravity" is enough. Serve-during avoids paying a re-root round-trip on every read; a copy that was fresh a moment ago is at most slightly behind while it re-links.

**Where it can fail, and what we do about it.**
- *An isolated holder with no closer co-host can silently go stale.* Anti-entropy is neighbor-scoped, so it only converges once the holder re-links. Mitigation: background re-root keeps trying to re-establish an upstream; until then the peer serves best-effort. This is accepted eventual consistency.
- *Over-claiming "bounded freshness."* The heal path is capped (32 stale contracts per summary cycle), emits are best-effort (`try_send`), and it only runs between connected co-hosts. So convergence is bounded ONLY under stable connectivity with a co-host neighbor; a partition, backpressure, or a topology miss is unbounded until the peer re-links or the app supplies its own freshness. Mitigation (Codex B2 / Fable M8): state the bound only under those assumptions, and **segment the catch-up telemetry by has-co-host-neighbor vs. not** — the isolated cohort converges only via re-root at the ~5-9% near-miss floor, so it must be measured separately.
- *A stale co-host advertisement poisons fan-out and upstream selection.* The advertisement ID exchange is best-effort `try_send` and its retraction is dead code. Mitigation: Fix 1 makes the on-connect full-set exchange reliable (retry / periodic full-set re-request) and wires retraction. This is separate from the InterestSync STATE layer, which already works and is untouched.
- *Summarize storm from every-hop placement.* Covered under Placement (it is a consequence of every-hop hosting); the mitigation ships with that work.

**Still to pin down.** The periodic full-set re-request cadence (which also sets the reconcile TOCTOU bound); whether the isolated-holder cohort needs any catch-up bound beyond re-root.

*Grounding.* Live fan-out `operations/update.rs:294` → `neighbors_with_contract` `node/neighbor_hosting.rs:334` (no interest check). Cached copy registers interested: `register_local_hosting` GET `get/op_ctx_task.rs:1308`, PUT `put/op_ctx_task.rs:1936`; `is_interested()` counts `hosting` `ring/interest.rs:167-168`. Heartbeat unfiltered: `get_all_interest_hashes` call `ring.rs:1363`, `INTEREST_HEARTBEAT_INTERVAL` (~5 min) `interest.rs:60`. Summary gate `should_summarize_or_broadcast` `ring/hosting.rs:1546` via `summary_if_hosted_or_in_use` `node.rs:3104`; mismatch `node.rs:2518`; heal `SyncStateToPeer` `broadcast.rs:536-544`; cap `MAX_STALE_SYNCS_PER_SUMMARIES=32` `node.rs:2095`. Advertisement `try_send` sites `connection_lifecycle.rs:375`/`:1198`; dead retraction `on_contract_unhosted` `neighbor_hosting.rs:131`. Hosting order `most_keyward_among` `ring.rs:398` (`*dist < my_distance`).

---

### Placement

**What it does.** Decides which peers end up holding a contract. Every peer a PUT or GET passes through starts hosting it; ordinary capacity-based eviction (an LRU biased by a distance prior) then decides what to keep. Under load, copies concentrate near the key and thin out where nobody reads — no fixed replica count, the size is emergent.

**The rule.**
- **Every hop hosts.** Every hop on a PUT route hosts the contract and retains it under the normal cache. Every return hop on a GET does the same (host-on-GET is the identical channel).
- **Retention** is `keep_score = eviction_floor + predicted_demand`. At cold start, `predicted_demand` is a **distance prior** (closer to the key scores higher); as reads accumulate it slides toward the contract's own observed read rate.
- **Eviction fires only over-budget.** Under budget, nothing evicts. When over budget, the lowest-scoring contracts drop — so far, unread copies go first, concentrating hosting keyward.
- **Reconcile-before-announce.** A hop must acquire the POST-MERGE body before it advertises hosting, because the pre-merge state forwarded to it can diverge.

**Why it's shaped this way.** Every-hop placement gives a fresh contract many nearby landing spots for free, which is both redundancy and findability. The LRU + distance prior is exactly what separates this from the old **relay-caching anti-pattern**: copies are not durable-regardless-of-demand; they are the lowest-value eviction candidates the instant capacity binds. Concentration is emergent from routing gravity plus the prior, not a globally-coordinated count. A demandless-copy expiry was considered and **dropped**: per-peer update fanout is bounded by a peer's co-host-neighbor degree, not by the total copy count (see Where it can fail), so cheap extra copies never overload any one peer — the expiry would have dropped the cheap copies without touching the real, demanded load.

**Where it can fail, and what we do about it.**
- *Fanout on an under-full fleet.* Eviction only runs over-budget, so on a fleet with spare capacity nothing evicts and every hop-copy is retained. This does NOT overload any peer: per-peer update fanout is bounded by a peer's **co-host-neighbor degree** — a peer sends each committed update only to its connected advertised co-hosts (~5-20), regardless of whether the contract lives on 10 or 1000 peers — and updates are small (<1KB). So the total copy count does not drive any single peer's send load. What CAN overload a peer is hosting too many **busy** (actively-demanded) contracts at once, and that is handled by the **uplink-aware admission budget** (reject/shed when uplink is the scarce resource at its limit; see Admission & Shedding), NOT by dropping cheap copies. Mitigation (Codex B1 / Fable M3): keep a cheap fanout **measurement** in telemetry — **holders-per-contract** and **sends-per-committed-update**, per contract — as a check on this model, with NO switch or expiry attached.
- *Summarize storm (re-arms #4440 / #4473).* Every-hop maximizes neighbor hosted-set overlap by construction (same routes → same contracts), and a naive path would run a WASM `summarize_state` per shared contract on both the summary build and the comparator each 5-min heartbeat. At ~20 neighbors × a few-hundred shared contracts that returns to the ~70-80 summarize-calls/sec that melted the network. Mitigation (Codex #3 / Fable): the branch **already has** a state-hash-validated summary cache that returns without loading state or running WASM on a cache hit, keyed by the per-contract `state_generation` counter — so a contract is summarized once per change, not once per neighbor per heartbeat. The work is to **VERIFY the existing cache covers every-hop load**, NOT to build a new one; add a per-heartbeat entry-cap + rotation (mirroring the existing 32-heal cap) ONLY if cache-hit telemetry still scales with hosted-set overlap. Telemetry falsifier: **summarize-calls/sec vs. hosted-set size** must stay flat as the hosted set grows. Ships with every-hop (Fix 8).
- *Reconcile-before-announce gap.* If a hop advertises before acquiring the merged body, a co-host or reader picks up a stale/partial copy. Mitigation: a directed sub-op fetch of the post-merge body gates the advertise; the branch's fetch-if-missing short-circuits on a present-but-stale body and must be replaced by this path.
- *Distance prior mis-scores a hot far contract.* With only the distance prior (A4 deferred), a heavily-read FAR contract scores low and can thrash. Mitigation: keep `min_ttl` as the anti-thrash floor until the distance prior + op-pinning + A4 are all live; only then drop it.

**Still to pin down.** The A4 blend function and the `g0(distance)` prior shape (only ratios matter).

*Grounding.* Every-hop PUT `put.rs:296`; host-on-GET comment `client_events.rs:1121`, caching `get/op_ctx_task.rs::cache_contract_locally`. Over-budget-only eviction early-return `ring/hosting/cache.rs:402`. `keep_score` `demand.rs:5`; cold-start `MIN_POINTS_FOR_PRIOR=5` `demand.rs:36`, `NEUTRAL_DEMAND=1.0` `:49`; A4 deferred `:13-15`. `min_ttl` `cache.rs:159`/gate `:417`. Seed-lease to delete: `SEED_LEASE_DURATION` `hosting.rs:131`, `SEED_CHAIN_LENGTH=3` `:114`, `MAX_ACTIVE_SEED_LEASES=256` `:142`, `install_seed_lease` `:1157`, `maybe_install_put_seed_lease` `put/op_ctx_task.rs:2035`, stale short-circuit `:2076-2086`. Summarize WASM per entry `node.rs:2440-2463`, comparator `:2543`; existing state-hash summary cache `contract/executor/runtime/executor_impl.rs:1059`, populated `:1128`, keyed by the per-contract `state_generation` counter `ring/hosting.rs:363` / `bump_state_generation:452`.

---

### Admission & Shedding

**What it does.** When a full peer is asked to host a new subscription, it decides whether to make room or refuse — keeping the contracts most worth keeping, near the peers they belong to, without thrashing.

**The rule.**
- A peer hosts what fits its budget, sized by its **scarcest resource** (memory, CPU, or uplink, whichever binds). "Full" = that resource at its limit.
- **Budget composition (uplink is greenfield).** The budget starts as **memory + CPU** (both measurable today). **Uplink** joins the scarcest-resource set only once an egress meter exists — a greenfield build, none is in-tree today (`update_rate_limit.rs` is inbound only). Its first consumers are the **admission uplink-check** and graceful-degradation **coalescing**; interim proxy while the real meter is built = **sends-per-update × mean payload size**. With the demandless-copy expiry dropped (see Placement), **uplink-aware admission is the designated aggregate fanout bound** — it is what sheds when a peer holds too many *busy* contracts, which copy count alone never triggers. Until the egress meter ships, aggregate egress is unmetered — but this is a **pre-existing greenfield gap, not a regression** (the expiry never bounded aggregate egress either; it only dropped cheap copies), and per-peer fanout stays degree-bounded meanwhile, so no single peer is overloaded in the interim.
- To admit a new subscription when full, it may evict **one** contract, and only one that is **both farther from the key than the newcomer AND single-subscriber**.
- Contracts with **2+ subscribers are not evicted** in normal operation. **Zero-subscriber cached copies are evicted first**, by distance.
- If nothing qualifies, **refuse** with an explicit "node at capacity" NACK.
- The same refusal governs a **local client's own subscription** and a **conversion** (a cached, zero-sub contract gaining its first subscriber). Both count as admissions and are subject to full→reject.
- **Displacement chains are capped at depth `K_displacement_depth`** (D1). A refused newcomer's downstream re-subscribe can itself trigger an eviction at the next peer; that cascade is bounded by `K_displacement_depth`. Enforcing the cap requires a **depth counter carried on the subscribe/re-subscribe wire message** (a new field, version-gated) — each chain step is a different peer seeing an ordinary 1-sub subscribe, so no peer can locally know the accumulated depth without it. The counter is **increment-only**, with an **anti-spoof clamp**: a missing or absurd value is treated as depth ≥ `K_displacement_depth`−1, so a peer understating it to extend the chain gains nothing.
- **Under genuine saturation the NEWCOMER is refused** (explicit "at capacity"), never a settled incumbent silently demoted (D1).
- **Last-resort valve** (D2): when memory would genuinely overflow, the peer sheds its least-valuable contract even if it has 2+ subscribers (they re-root). This fires ONLY at real OOM risk.

**Why it's shaped this way.** "Farther AND single-sub" makes every eviction a strict local improvement — a better-placed contract replaces a worse-placed one, single-for-single, so the readers served at the admitting peer don't drop. Never-evict-2+-sub protects the popular contracts. Refuse-when-nothing-qualifies is the backpressure that bounds a peer's load. Applying the rule to conversions and client subs keeps a gateway's pinned set bounded (closing the "50/client × unbounded clients" hole). Capping the chain and refusing the newcomer, rather than an incumbent, keeps churn bounded and keeps the degradation *visible* to the party that caused it. The OOM valve exists because otherwise a fully-pinned node has no way back under its memory budget — conversions and monotonic state growth push it over with nothing sheddable, which is exactly the #4565 OOM piece A shipped to fix.

**Where it can fail, and what we do about it.**
- *Displacement chains* (Fable B1 / F3). A victim's re-subscribe is itself a 1-sub subscription that can evict at the next peer → cascade, each step a re-root plus a body fetch, and a DoS lever (keys crafted just inside a region's threshold). Mitigation: **cap chain depth at `K_displacement_depth`**, account each step **via the wire-carried, version-gated, increment-only depth counter (anti-spoof clamp: missing/absurd → depth ≥ `K_displacement_depth`−1)**, refuse the newcomer at the cap (D1, Ian-decided).
- *Stranded party is a peer, not a client* (Fable F7). When a displacement victim's re-subscribe hits the placement-failure cap, the originator is a peer, so no client directly sees the "region at capacity" error. Mitigation: **propagate it via the renewal-failure path** to the downstream clients that depend on that peer, so the capacity signal reaches a client rather than being silently lost (chosen over accepting silent best-effort + telemetry).
- *Evict-to-admit victim goes dark* (Fable F8). The admitting peer must not drop the victim before its downstream can re-home. Mitigation: the handoff is **make-before-break** — the admitting peer does not drop the victim until its downstream has had a chance to re-root.
- *Convergence is NOT strictly monotonic* (Fable B1). "Only evict farther → each contract lands closer" is false under saturation: a victim can re-land FARTHER because every closer peer is also full. Honest guarantee instead: **each admission improves the admitting peer's placement; the capped chain terminates; long-run churn motion is bounded only empirically** (sim, not proof) (D1, Ian-decided).
- *Over-budget-all-pinned with no recovery* (Fable B2). A node drifts over its memory budget via conversions (byte-neutral pin growth) and monotonic state growth, with 2+-sub contracts normally unsheddable → OOM re-arm. Mitigation: the **last-resort OOM valve** sheds a 2+-sub contract at real overflow risk, and a conversion is refused at capacity (D2, Ian-decided).
- *`min_ttl` blocks the valve* (Fable M9). The shed/valve picks a victim, but the `min_ttl` candidate filter could exclude a young entry, leaving a full node unable to make room. Mitigation: **the shed and the OOM valve bypass the `min_ttl` filter**.
- *Client starvation on a keyward gateway* (Fable M6 / Codex #2). A gateway full of network pins could refuse its own users while serving strangers. Mitigation: a **reserved client-pin budget slice**; a client sub MAY evict demandless copies (yes) and MAY evict-to-admit farther single-sub network pins. This is **the client-pin exception** — an *explicit* carve-out from the strict-improvement rule: a local client outranks a remote single subscriber even at **equal-or-worse** placement. Equal placement is not a strict improvement, so it is named here as an exception rather than read as contradicting "farther AND single-sub, strict improvement." Because state size is unknown until fetched, the admission bytes-check runs **post-fetch** (accepting the wasted transfer on a refusal), or is size-blind against the reserved slice; the reserved slice is the simplest and is the recommendation.

**Still to pin down.** The exact NACK message (see The "I'm full" backpressure); whether a refused re-subscribe routes onward — **yes**, specified in the backpressure section; the `K_displacement_depth` and `K_placement_failures` values (now split); the OOM-valve trigger point (what memory headroom counts as "real overflow"); the reserved client-pin slice size, and post-fetch vs. size-blind checking.

*Grounding.* Pinned exemption is absolute today: `cache.rs:415-418` (`!should_retain`, `should_retain = contract_in_use`) — evict-to-admit must pierce it for exactly the farther-single-sub class. `contract_in_use` OR-terms `hosting.rs:1110-1113` (`has_client_subscriptions` `:1111`). Unbounded caps today: `MAX_DOWNSTREAM_SUBSCRIBERS_PER_CONTRACT=512` `hosting.rs:99`, `MAX_SUBSCRIPTIONS_PER_CLIENT=50` `contract/executor.rs:66`. `min_ttl` gate `cache.rs:417`. OOM precedent: #4565 (piece A).

---

### Re-rooting & Root-verification

**What it does.** When a peer loses its upstream (its link to a closer co-host), it finds a new place in the mesh without going dark; and the peer closest to a key confirms it really is the root. Both use the same lightweight **search** that can ask "who actually holds this?" at its terminus — not a heavy state-dragging operation, which routes by proximity alone and mis-targets a sparse key.

**The rule.**
- Losing the upstream marks the contract for re-root. The peer keeps **serving its best local copy** the whole time (serve-DURING) and re-roots in the **background**.
- Re-root = the **consult-equipped search** (the subscribe find with `first_hop = None` plus the terminal host-advertisement consult), **NOT a state-carrying PUT**.
- Re-root is rate-limited by the existing keyward-refind backoff, so a failing search doesn't hammer.
- The trigger is primarily **connection-drop** (event-driven); advertisement retraction is the backstop.
- A **locally-verified root** is the peer for which a bounded search finds no strictly-closer host. It is a LOCAL claim atop the accepted ~5-9% near-miss floor, not a global-freshness invariant. It **re-verifies periodically** (piggybacking the anti-entropy re-request cadence) and on demand (a read re-checks).
- **Duplicate-root reconciliation:** a root that receives a hosting announcement from a **strictly-closer** peer immediately re-verifies and yields root to it.

**Why it's shaped this way.** Serve-during keeps a peer with intact data from ever going permanently dark, which matches the ruling that a stored, not-being-updated copy is fine to serve. The search finds the true holder far more reliably than a PUT because it consults host advertisements at the terminus rather than trusting greedy routing alone. Making the root a local, re-verified claim keeps it honest about the near-miss floor while still collapsing duplicate roots the moment a closer one appears.

**Where it can fail, and what we do about it.**
- *False local root.* A bounded search can miss a closer holder (the ~5-9% floor) and maintain a false root. Mitigation: periodic + on-read re-verify, and immediate yield on a closer announcement (duplicate-root reconciliation). Residual: the near-miss floor is a known, separately-tracked findability limit, not something this mechanism closes.
- *Re-root can't place (every candidate NACKs).* Mitigation: `K_placement_failures` consecutive placement failures surface a client-visible "region at capacity" error — but this applies to PLACEMENT only; the bytes-holding peer keeps serving regardless.
- *Re-root landing depends on routing.* Serve-during is safe without good routing (the peer always serves its bytes), but where re-root *lands* depends on the findability workstream. Mitigation: couple re-root delivery to the findability fixes; accept the floor until they land.
- *A stale advertisement mis-routes the search.* Mitigation: Fix 1 advertisement reliability (see Freshness & Propagation).

**Still to pin down.** The root re-verify cadence value; whether re-root and root-verify share one search path or two; the `K_placement_failures` value (shared with the NACK).

*Grounding.* Consult-equipped search `drive_client_subscribe_inner` `subscribe/op_ctx_task.rs:496` (`first_hop=None` `:501`), consult `:476-480`, backoff `is_in_keyward_refind_backoff` `:457`, FAIL-FINDABLY `:448-523`. Strict-closer order `most_keyward_among` `ring.rs:398`. Terminal-consult caps `MAX_TERMINAL_CONSULT_HOSTS=2` (GET `get/op_ctx_task.rs:1445`), `TERMINAL_CONSULT_HOSTS=1` (subscribe `:56`); PUT has none. Last-resort stale fallback `get/op_ctx_task.rs:2749-2764`. Renewal classifier `RenewalOutcome` `:301`, exhaustive arm `:519-521`.

---

### The PUT protocol (summary-first)

**What it does.** Publishes or updates a contract without re-shipping full state across a mesh that already holds it. A PUT probes toward the key carrying only a state summary; the response reveals whether the contract is genuinely new (needs full state) or already held (needs only a reconcile).

**The rule.**
- **Phase 1 — probe.** The PUT routes toward the key carrying a state **summary/hash**, not the full state. Each hop reveals whether a holder exists; the initiator does not need to know in advance.
- **Phase 2 — dispatch**, two cases:
  - **No holder found** → treat as a genuinely new contract → ship **full state hop-by-hop** along the route. These hops become the new hosts. This is exactly where every-hop placement gets its bytes, so summary-first PUT and every-hop hosting are coherent.
  - **Holder found** → **bidirectional summary/delta reconcile**: the two copies exchange summaries and each pulls the other's delta on mismatch. This is NOT "ahead/behind" — divergent copies are neither, they merge via the commutative monoid. Full state does not re-travel an existing mesh.
- Each hosting hop acquires the **post-merge body** before advertising (shared with Placement's reconcile-before-announce).
- Client completion: the PUT completes when the probe has reached a terminus and the seed/merge is acknowledged along the path (exact ack point to be pinned).
- The summary and delta are the contract's existing opaque `StateSummary` / `StateDelta` — Freenet moves the bytes, the *contract* defines them (via `summarize_state` / `get_state_delta` / `update_state`), and these are the **same primitives the InterestSync anti-entropy already uses** to heal. So there is nothing to encode. What is new is only the PUT *message* carrying a summary field first, plus the probe→dispatch handshake — that message shape is version-gated against the fleet-minimum version.

**Why it's shaped this way.** Summary-first avoids paying full-state bandwidth to update a contract the mesh already holds, which is the common case. Shipping full state only in the genuinely-new case is what makes it coherent with every-hop placement — fresh seeds need the body, existing meshes don't. Bidirectional reconcile is the only primitive consistent with a monoid state that has no global order.

**Where it can fail, and what we do about it.**
- *"No holder found" ≠ "genuinely new"* (Fable M7 / Codex M5). PUT has no terminal consult, so an off-path holder can be missed → a false-fresh second copy is created. Mitigation: accept it as eventual consistency — the two copies reconcile via anti-entropy / duplicate-root once they share a neighbor. Say this explicitly; do not claim discovery is exact.
- *Latency change on the busiest op.* A fresh PUT becomes two passes (probe to terminus, then full-state along the path). Mitigation: only the fresh-seed case pays it; existing-contract PUTs ship only a delta. Track PUT completion latency by case.
- *Per-hop body-acquisition amplification* (Fable F5). The summary-first savings accrue **only for route hops that already host** — those reconcile with a delta and re-ship no full state. For a holder-found PUT through hops that do NOT yet host, making each one host-and-fetch the merged body would cost ~N directed full-body fetches, an N-body amplification aggregate-comparable to the full-state PUT it "replaces." Mitigation: hosting at those non-hosting hops is **lazy — host-on-next-read, not host-and-fetch-every-hop**. The existing-contract PUT reconciles with the holders it finds and does not oblige every probe hop to fetch the body; a non-hosting hop starts hosting only when a later read routes through it. Track **PUT-bytes-by-case** in telemetry.
- *"Reverse transfer if initiator is ahead" is wrong* (Fable M7). A hash can't establish an order a monoid doesn't have. Mitigation: **replaced by bidirectional reconcile** above (Ian-aligned) — remove any "ahead/behind" framing.

**Still to pin down.** The new PUT *message* shape (a probe carrying a `StateSummary`; the dispatch signal for new-vs-existing) and its version gate — NOT the summary/delta encoding, which is contract-defined opaque bytes reused from the existing anti-entropy path; the exact client completion semantics (ack at probe / at seed / at merge); the per-hop body-fetch source in the existing-contract case.

*Grounding.* Full-state PUT today `put.rs:296`, `PutStreamingPayload.value` `:271`. `state_hash` is telemetry-only `put.rs:100`, `put/op_ctx_task.rs:1985` — today's PUT ships full `WrappedState`. The summary/delta payloads reuse the contract's existing `StateSummary` / `StateDelta` (opaque `[u8]`, contract-defined, already used by the InterestSync anti-entropy); the only new wire is the PUT *message* carrying a summary field. Version gate `remote_version()` `transport/peer_connection.rs:1524`, consumed `connection_lifecycle.rs:1016`.

---

### The "I'm full" backpressure (the NACK)

**What it does.** Gives the admission refusal a concrete wire message and defines what a refused subscriber does next — so a full region pushes back instead of overloading, and a refused request keeps looking for room instead of hammering or stranding its downstream.

**The rule.**
- A new refusal variant (propose `SubscribeRefused`, or a refusal case on the repurposed renewal-result channel) carries: **which resource is saturated** (memory / CPU / uplink) and a **retry-after** hint.
- On receiving a NACK, the refused peer **routes onward** to the next-closest keyward candidate it has already computed. **Routing is separated from registration** — a NACK rejects *hosting*, not the *search*.
- Receiving a NACK **arms the keyward-refind backoff**, so the peer doesn't hammer the same saturated region.
- Each NACK increments the **placement-failure count**, carried on the subscribe/re-subscribe wire message (the same version-gated, increment-only counter that carries `K_displacement_depth`, with the anti-spoof clamp: missing/absurd value → treat as already at the cap). After **`K_placement_failures`** consecutive placement failures across candidates, the originating client gets a visible "region at capacity" error.
- The variant is **version-gated**; old peers that don't understand it fall back to the existing renewal-result handling.

**Why it's shaped this way.** Separating routing from registration lets a refused subscribe keep descending toward the key past a full peer — which is what bounds the displacement chain to real capacity rather than stranding a downstream at the first full hop. Carrying the resource + retry-after lets the caller back off intelligently instead of blindly. The `K_placement_failures` cap turns an otherwise-unbounded retry into a bounded, client-visible failure.

**Where it can fail, and what we do about it.**
- *NACK storms under regional saturation.* Every candidate NACKs → churn. Mitigation: backoff arming + retry-after + the `K_placement_failures` cap bound the retries; the `K_displacement_depth` cap (D1) bounds the cascade.
- *A new variant breaks old peers* (the wire-format incident class). Mitigation: version-gate the variant; keep the renewal-result classifier exhaustive so a new variant forces a re-triage at compile time.
- *Onward-routing loops.* A NACK'd peer could route back toward where it came from. Mitigation: route only to strictly-closer, not-yet-tried candidates; the strict-closer filter prevents backtracking.

**Still to pin down.** The exact variant name and payload layout; the retry-after derivation; the `K_placement_failures` value (shared with re-root placement-failure); tie-breaks when two candidates are equidistant.

*Grounding.* Forcing point `RenewalOutcome` enum `subscribe/op_ctx_task.rs:301`, exhaustive arm `:519-521` (an added variant forces re-triage here), `classify_renewal_result` `~:571`. Backoff `is_in_keyward_refind_backoff` `:457`. Version gate `remote_version` `transport/peer_connection.rs:1524`.

---

### Graceful degradation

**What it does.** Keeps a 2+-subscriber contract — which the admission rule refuses to shed — serving even when its own update fanout outgrows the peer's uplink, by sending updates *less often* rather than by blocking anything.

**The rule.**
- Each contract has an **update coalescing interval**. Under uplink pressure the interval **stretches**: the peer sends the latest merged state less frequently, collapsing intermediate updates into one.
- A coalesced send **rides the existing delta path** (`DeltaOrFullState`): it ships a **delta to the receiver when that receiver's summary is known**, and full state only as a fallback — NOT a literal full-state send, which would *increase* per-send bytes (a large state can be ~650KB) under the very uplink pressure coalescing exists to relieve.
- It **never applies backpressure** to the update pipeline (no `send().await` stalling an event loop). It drops intermediate states, keeping only the latest — safe because the state is a mergeable monoid, so skipping a generation loses nothing.
- The coalescing interval is **observable** via a per-contract counter.

**Why it's shaped this way.** A protected contract can't be shed, so the only relief valve is temporal — send the same convergent state less often. Keep-latest coalescing is safe precisely because the state merges. Backpressure is explicitly ruled out: a blocking send on a bounded channel inside an event loop deadlocks the node (the #3519 / result-router class of incident, 4+ production precedents).

**Where it can fail, and what we do about it.**
- *Mistaken for a stall.* A stretched interval can look like a silent hang. Mitigation: the coalescing-interval counter makes it observable and distinguishable from a deadlock; alert if the interval grows without bound.
- *Starving a hot contract.* Aggressive coalescing could make a popular contract laggy. Mitigation: the interval stretches proportionally to uplink pressure and recovers when pressure drops; bound the maximum interval.
- *Confused with backpressure in implementation.* Mitigation: implement strictly as keep-latest drop, never a blocking send; add a guard comment at the site referencing #3519.

**Still to pin down.** The coalescing-interval function (uplink pressure → interval); the maximum interval; whether the pressure signal is per-contract or per-peer.

*Grounding.* Anti-pattern precedent #3519 (`.send().await` in event/recv loops, bug-prevention-patterns). Coalesced-send delta machinery already exists: `BroadcastTo` ships `DeltaOrFullState` `operations/update.rs:946-953`. This is otherwise a NEW mechanism — there is no current coalescing.

---

### The maintenance / reconcile loop

**What it does.** Puts all per-contract maintenance under one level-triggered controller that decides actions (subscribe, renew, collapse, announce, retract, evict-to-admit, re-root) from current state — instead of scattering the logic across event handlers and a stored upstream flag. Events mark a contract dirty; one driver recomputes and applies.

**The rule.**
- `reconcile(contract) -> Vec<Action>` reads current inputs and returns actions: **Subscribe / Unsubscribe / Collapse / Announce / Retract / Evict-to-admit / Re-root-search**.
- **Upstream is computed everywhere** (the closest connected co-host strictly closer to the key), never stored — this removes the stored `is_upstream` flag and its drift.
- Events **mark-dirty**; a periodic tick is the backstop; one driver applies actions.
- **Renewal is interest-gated:** a lease renews only while the contract is in use (a local client OR a registered downstream subscriber strictly farther from the key OR recent real GET/PUT access). When the last interest goes, the lease lapses and the chain collapses inward. This is the #3763 storm fix — renewal tracks active demand, not cache size. A read-only or PUT-only contract that is never subscribed (the River UI and web/UI container contracts) is kept renewed by its GET/PUT recency alone, consistent with invariant 3.
- Required hardening: **make-before-break** (acquire the new upstream before releasing the old); demand-loss **hysteresis**; **freshness-reconcile-before-announce** as an action dependency; formation-aware re-root damping; distinguish **partition from collapse**; a **deadline-driven dirty source** at lease-expiry-minus-margin; **at-emission revalidation** of destructive actions against re-read inputs; a **per-contract wire-action rate cap**.

**Why it's shaped this way.** A single level-triggered controller is idempotent and self-correcting — a missed event is caught by the next tick, and computing upstream removes a whole class of stale-flag bugs. Interest-gated renewal is the load-bearing change that makes subscriptions proportional to active interest rather than to cache size, so chains collapse when demand ends (the earlier "the eviction budget will bound the storm" framing was wrong — the budget can't touch in-use, self-renewing leases). The hardening exists because the inputs live in separate concurrent maps: without at-emission revalidation, N reconciles over N stale reads agree N times and a last holder can collapse on a stale snapshot.

**Where it can fail, and what we do about it.**
- *TOCTOU across separate maps.* Inputs read at reconcile time can be stale at emission. Mitigation: destructive actions (Collapse / Unsubscribe / Retract / Evict-to-admit) re-read inputs at emission and re-validate; the staleness bound is Fix 1's advertisement re-request cadence — size the tick against it.
- *Slow tick + missed event → lease-lapse cascade.* Mitigation: the deadline-driven dirty source fires at lease-expiry-minus-margin, so a renewal isn't missed even if the triggering event was dropped.
- *Break-before-make goes dark.* Mitigation: make-before-break — the new upstream is acquired before the old is released.
- *Action storms from a flapping input.* Mitigation: per-contract wire-action rate cap; demand-loss hysteresis.
- *Partition read as collapse.* A partitioned peer could tear down a still-wanted chain. Mitigation: distinguish partition (unreachable → keep serving, re-root) from collapse (demand gone → tear down).

**Still to pin down.** The tick period vs. the advertisement re-request cadence (the TOCTOU bound); the hysteresis window; the per-contract rate-cap value; the formation-window damping parameters.

*Grounding.* Interest-gated renewal `ring/hosting.rs:1834` (`... && self.contract_in_use(&key)`). Controller home spans `node/op_state_manager.rs`, `ring/hosting.rs`, `ring.rs`, `ring/interest.rs`; kills the stored `is_upstream` flag. Storm precedent: #3763.

---

### SUBSCRIBE retirement & client API

**What it does.** Stops sending a standalone SUBSCRIBE wire message for initial subscribes (folding the subscribe into GET/PUT-with-subscribe), while keeping the client-facing `ContractRequest::Subscribe` API working through a deprecation adapter, and keeping the wire enum variant for rollout compatibility.

**The rule.**
- New peers no longer **emit** a standalone SUBSCRIBE for an initial subscribe; the subscribe rides on a GET (or PUT) with a subscribe flag.
- The standalone SUBSCRIBE wire variant is **repurposed as `Renewal`** (payload bincode-identical) and **kept in the enum** — removing it would shift bincode indices, which is itself the cross-version break.
- Incoming standalone-SUBSCRIBE handling is kept for the rollout window so older peers still interoperate.
- The client `ContractRequest::Subscribe` **stays, deprecated**, translated by an adapter into GET-with-subscribe; the adapter synthesizes the `ContractResponse::SubscribeResponse` the client expects.
- Note the behavior change: a subscribe now pulls full state — a latency change on a live API. Smoke-test riverctl before release.

**Why it's shaped this way.** A standalone subscribe is redundant once a GET already routes toward the key and can carry a subscribe flag; folding them removes a wire round-trip and a duplicate code path. Keeping the variant and the client API preserves wire and client compatibility across a mixed-version fleet — the redesign is behavioral, with no wire-format break.

**Where it can fail, and what we do about it.**
- *Removing the variant breaks bincode layout* (known incident class). Mitigation: KEEP the variant; repurpose, don't delete. Delete only as a separate later cleanup, once the fleet is fully past it.
- *Old clients / tools deserialize the new response shape wrong.* Mitigation: the adapter synthesizes the exact `SubscribeResponse`; smoke-test `riverctl member list` against the updated gateway (the v0.2.11 streaming-default incident class).
- *Latency regression surprises a live app.* Mitigation: document the full-state-pull latency change; verify against River before release.

**Still to pin down.** The deprecation timeline for finally removing the standalone SUBSCRIBE variant. (PUT-with-subscribe IS in scope, not an open question: Ian named GET AND PUT explicitly, 2026-07-02 and 07-08.)

*Grounding.* Client `ContractRequest::Subscribe` key-extraction `client_events/websocket.rs:598`, handler `client_events.rs:1071`. `GetResponse` fields (no freshness field — keep it that way) `client_api/client_events.rs:1706-1711`. Wire-compat precedent: v0.2.11 streaming / bincode variant-index.

---

### Removing the old fan-out (Source-2)

**What it does.** Removes a redundant second update fan-out path (the interest-manager fan-out) that duplicates the proximity fan-out — without touching the InterestSync anti-entropy, which is a separate layer that must stay.

**The rule.**
- Drop the **Source-2** block in `get_broadcast_targets_update` (the interest-manager targets); keep **Source-1** (proximity fan-out to advertised co-hosts).
- This does **NOT** remove the InterestSync state anti-entropy — that is a distinct layer and stays.
- Gate the removal: **version-gate Source-2 emission** to fleet-minimum version so a new peer keeps updating an old-vintage downstream that still relies on it; and **cover the formation window** for registered-but-unannounced hosts (reconcile-before-announce widens the window).

**Why it's shaped this way.** Source-2 is redundant with Source-1 — proximity fan-out already reaches all advertised co-hosts — so it is pure duplicate fan-out, and removing it cuts update traffic. The version gate and formation-window coverage prevent dropping updates to peers that haven't yet advertised or are on an old version during rollout.

**Where it can fail, and what we do about it.**
- *Confusing Source-2 with the anti-entropy* (this was the v4 blocker). Mitigation: they are separate layers — removing Source-2 leaves InterestSync intact. State this explicitly and cite both.
- *Dropping updates to an old-vintage downstream.* Mitigation: version-gate emission so Source-2 keeps running toward peers that still need it through the rollout.
- *Formation-window gap.* A host registered but not yet advertised misses Source-1. Mitigation: reconcile-before-announce widens the advertise window; the anti-entropy heals the residual.

**Still to pin down.** How long to keep Source-2 emission version-gated before final removal.

*Grounding.* Source-1 `operations/update.rs:294`; Source-2 `:323-324` → `get_interested_peers` `:324` (def `ring/interest.rs:496`). Version gate `remote_version`. Removing Source-2 leaves the InterestSync layer (`node.rs:2518`/`broadcast.rs:536-544`) intact.

---

## 3. Build sequencing

Ordered, with gates. Independently-safe PRs where possible; shapes are proposals, targets are settled. The summarize-storm mitigation (verify the existing summary cache covers every-hop load) ships WITH the every-hop placement work (step 8). (There is no demandless-copy expiry to ship — it was dropped; per-peer fanout is bounded by co-host-neighbor degree, and busy-contract load by the uplink-aware admission budget.)

1. **Advertisement-layer reliability + retraction (Fix 1).** Make the on-connect full-set ID exchange reliable (retry / periodic full-set re-request — this cadence also sets the reconcile TOCTOU bound) at both `try_send` sites; wire retraction into eviction/collapse/evict-to-admit. Scope: the advertisement layer ONLY, not the InterestSync state anti-entropy (which already works). **Gates:** every-hop placement (8), Source-2 removal (9), re-root correctness.

2. **Reconcile core (the keystone refactor).** Level-triggered controller; compute-upstream-everywhere; kill stored `is_upstream`; add `Evict-to-admit` + `Re-root-search` actions; ALL hardening (deadline-dirty, at-emission revalidation, per-contract rate cap) + the TOCTOU bound. Behavior-preserving refactor first, then flip re-root to change-detection. Sim: no-storm under load, chain-collapses-on-client-leave, no-cycle, last-holder-not-collapsed-on-stale-snapshot, ≥3 seeds. Full multi-model review. **Gates:** admission (7-bis), serve-during re-root (3).

3. **Hosting definition + serve-DURING + background search re-root (R3).** Restore "strictly-closer OR actively-acquiring OR locally-verified root." Serve the best local copy immediately, kick a background re-root/reconcile (backoff-gated), never block a read. **No `GetResponse` freshness flag.** Re-root = the consult-equipped search, not a state-carrying PUT. Coupled to findability (11) for re-root *landing*; serve-during itself is safe without it. **Gates:** admission (a NACK'd re-subscribe re-roots via this).
   - **3-bis. Summary-first PUT.** Probe → dispatch; full state only for the genuinely-new case; bidirectional reconcile for existing; version-gate; riverctl smoke-test. Full-tier protocol change.

4. **Distance-based cold-start prior.** Gates the `min_ttl` drop, the seed-lease delete, and admission's distance term.
5. **Op-scoped pinning.** Gates the `min_ttl` drop.
6. **A4 — per-contract observed-demand term.** Gates the `min_ttl` drop AND is the spam bound for every-hop placement. (Encode **6→8**.)
7. **Drop `min_ttl`** (gated on 4 + 5 + 6). Re-run the eviction/thrash sim (#4441, #4565).
   - **7-bis. Strict-improvement admission-by-eviction + client-pin bound + chain cap (D1) + OOM valve (D2) + the NACK.** Evict-to-admit pierces the pinned exemption for the farther-single-sub class only; reject → NACK when nothing qualifies; "full" = capability-relative scarcest resource; client-pin bound with a reserved slice; graceful-degradation coalescing for over-fanned protected contracts. The shed/valve bypasses the `min_ttl` filter. **Gates on 2 + 3.** Sim: bounded evict-count per contract, one-for-one readers-served, saturated-region NACK, capped displacement chain.

8. **Delete the seed lease + build every-hop LRU placement (gated on 4).** Remove the seed-lease + chain constants and the `contract_in_use` seed-lease OR-clause; build the new at-hop path (host + retain under LRU, acquiring the post-merge body before advertising). Same treatment for host-on-GET. **Verify the existing summary cache covers every-hop load in this PR** (the summarize-storm mitigation belongs with every-hop; no expiry to build). Sim: PUT-then-GET-days-later under near-K churn; idle-fleet fanout **measured** (holders-per-contract, sends-per-committed-update) as a check, no switch; summarize-calls/sec flat vs. hosted-set size.

9. **Remove Source-2 fan-out (gated on Fix 1).** Version-gate emission; formation-window coverage. Does NOT remove InterestSync.

10. **SUBSCRIBE retirement + client deprecation adapter.** Keep the variant, repurpose as `Renewal`, synthesize `SubscribeResponse`, smoke-test riverctl.

11. **Findability workstream (separate / parallel).** L2 acceptance-gate relax (`ring/connection_manager.rs:658`), L1 explicit short-range target, L7 directional fallback. Validate against a routing-last-hop metric in `run_simulation_direct`. Ships accepting the ~5-9% near-miss floor.

**Gating summary:** {4,5,6} → 7. **6→8**, 4→8. 1 → {8, 9}. **{2,3} → 7-bis**. 3 (re-root) ↔ 11. 2 is the keystone refactor.

---

## 4. Telemetry — each invariant ships with its falsifier

Each falsifier below is **dual-purpose** (see §5): a sim assertion that gates the release in CI *and* a wild-telemetry check that gates deployment — the sim asserts it before merge, telemetry watches the same signal in the field after.

- **Hosting is demand-driven (no renewal storm)** → renewal-message rate tracks ACTIVE DEMAND, not cache size. Falsifier: renewal rate rising with cache size.
- **Dissolution concentrates keyward** → distance-to-key of DROPPED contracts skews far-first under forced capacity pressure (idle-network eviction is vacuously silent).
- **Freshness holds, honestly (serve-during)** → PUT-then-GET-days-later served + eventually-current under churn; InterestSync catch-up latency **segmented by has-co-host-neighbor vs. not** (the isolated cohort converges only via re-root at the ~5-9% floor); duplicate-root / split-brain rate vs. a target. (Codex B2 / Fable M8)
- **Fanout measured (no switch)** → **holders-per-contract** AND **sends-per-committed-update**, per contract, as a **check** on the per-peer-degree fanout bound — NOT wired to any switch or expiry (the demandless-copy expiry was dropped; the real fanout defense is the uplink-aware admission budget). (Codex B1 / Fable M3)
- **Summary-first PUT bandwidth honest** → **PUT-bytes-by-case** (fresh-seed full-state along the route vs. existing-contract delta vs. lazy host-on-read) — savings should appear only on hops that already host. (Fable F5)
- **Summarize load bounded** → **summarize-calls/sec vs. hosted-set size** must stay flat as the hosted set grows (the #4440 falsifier). (Fable M4)
- **Admission bounded** → pinned-set size per peer; evict-to-admit events; shed-victim distance-AND-sub-count distribution (victims farther AND single-sub, except the OOM valve); NACK rate (peer + client); client-pin refusal rate; **displacement-chain length distribution vs. K**. (D1)
- **OOM valve fires only at real risk** → count of 2+-sub sheds and the memory headroom at each (should be near-overflow only). (D2)
- **Graceful degradation observable** → per-contract coalescing-interval counter; must be distinguishable from a stall. (Fable M10)
- **Re-root works** → re-root-via-search success + consult-hit rate; state-carrying PUT (divergence-heal) tracked separately.
- **Findability** → GET/subscribe dead-end rate; "holder existed but unreached" vs. "no holder"; closest-neighbor-edge coverage as the driver.
- **Advertisement layer reliable (Fix 1)** → on-connect `HostingStateRequest` sent-vs-delivered; periodic full-set re-request phantom-retraction lag (also the TOCTOU bound).
- **Distance-prior retention** → keep-score vs. distance; retention-time vs. distance.

---

## 5. Testing & Validation

Behavioral changes to routing, hosting, and subscriptions default to **simulation tests** (`run_simulation_direct` — deterministic, fast, runs in CI). The harness must grow faster than the core, and a logic error must be reproducible in a sim before it is ever discovered in production. The unifying idea ties this section to §4: **every telemetry falsifier is dual-purpose — a sim assertion that gates the release (in CI, before merge) AND a wild-telemetry check that gates deployment (in the field, after merge).** Sims catch what they can at small scale and deterministic timing; telemetry is the real-scale, real-timing safety net for the residual a sim can't reach — partitions, backpressure, genuine OOM, mixed-version fleets.

### Harness extensions (an early-foundation PR)

`run_simulation_direct` today can't drive the scenarios the redesign turns on, so extending it is a **prerequisite, not an afterthought** — an early PR someone can start in parallel with R1 (the reconcile core), since the per-mechanism tests below depend on it. It needs to:

- **Inject capacity pressure.** The eviction / admission / shedding / OOM-valve paths fire only *over budget*, so a test must be able to force a peer over its budget on demand. On an idle fleet these paths are vacuously silent (§4's "idle-network eviction is vacuously silent").
- **Inject churn.** Re-root, chain collapse, and displacement chains only appear when upstreams drop and peers leave, so the harness must drive connection churn deterministically.
- **Measure the new signals** — the same ones §4 names: fanout-vs-holders, summarize-calls/sec vs. hosted-set size, displacement-chain length, shed-victim distance-and-sub-count distribution, anti-entropy catch-up latency **segmented by has-co-host-neighbor vs. not**, and the exact-consecutive-edge-formation metric for findability.

### Per-mechanism sim tests

Each mechanism gets a **write-test-first** sim — it must fail without the fix and pass with it — and each asserts the standard simulation-health metrics: subscribe success rate, GET success rate, subscription-tree formation, and time-to-first-successful-op. The review blockers map directly onto test cases, grouped by the release they gate:

- **Reconcile core + demand-driven eviction (R1 + R2):** no-storm-under-load; chain-collapses-on-client-leave; no-cycle; last-holder-not-collapsed-on-stale-snapshot; bounded-evict-count-per-contract; OOM-valve-fires-only-at-real-risk; co-host-convergence (anti-entropy heals a stale demandless copy); serve-during-never-dark.
- **Placement + admission + summary-first PUT (R3):** one-for-one-readers-served; saturated-region-NACK; capped-displacement-chain; PUT-then-GET-days-later (durability under near-K churn); no-full-state-to-existing-mesh (summary-first PUT ships a delta, not full state, to a hop that already hosts).
- **Re-rooting & root-verification (R4):** re-root-via-search-success; locally-verified-root + duplicate-root-reconciliation.
- **Findability (separate / parallel):** re-root lands via search against a sparse target, measured on the exact-consecutive-edge-formation / routing-last-hop metric.

Each test case has a matching §4 falsifier — the sim asserts it before release, telemetry watches it in the wild after.

### Wire-format compatibility tests

Three protocol changes carry wire risk — the summary-first PUT message, the NACK refusal variant, and the SUBSCRIBE-retirement / `Renewal` repurpose — so each gets the wire-format-incident discipline (the v0.2.11 / bincode-variant-index class):

- **bincode variant-index tests** pinning the enum layout, so an added or reordered variant fails a test rather than a live peer.
- **A mixed-version simulation** proving old + new peers interoperate — the redesign is behavioral with no wire break, and this is the assertion that keeps it that way.
- **The riverctl / fdev consumer smoke-test against the actually-built release binary**: verify `which <tool>` resolves to the freshly-built binary AND `strings $(which <tool>) | grep freenet-stdlib-` shows the expected version *before* diagnosing any variant-index error (the 2026-04-15 PATH-shadowing false alarm).

### The per-release gate

Each release runs the same loop, tying sims and telemetry together:

1. CI runs the release's relevant falsifiers **as sim assertions** — green is required to merge.
2. Merge, cut the release.
3. Watch the **same falsifiers in wild telemetry** through the soak window.
4. Only then does the next release proceed.

The **R2-before-R3 ordering is enforced at the telemetry gate**: eviction (R2) must be *observed working in the wild* — not merely merged — before the placement bundle (R3) ships, because R3's safety depends on R2's bound already being live (host-on-GET without the demand gauge is the relay-caching anti-pattern; host-chains without the budget re-arm the #3763 storm). The two high-risk PRs — the reconcile-core keystone refactor and the placement bundle — additionally require the full multi-model PR review (§3).

### Test-level guidance

Most of this is **simulation**, because it is all behavioral. **Unit tests** cover the isolated logic that needs no network: the `keep_score` / distance-prior curves, the chain-depth counter clamp, the admission predicate. **Docker-NAT** is used only if a transport-layer question arises — mostly N/A here, since this is hosting / routing logic, not transport. **Multi-machine** is reserved for final real-geography validation, not routine per-PR work.

---

## 6. Open questions (consolidated "still to pin down")

Genuine open items across all mechanisms. Most are parameter/encoding choices, not design gaps; the two that carry the most risk are marked ⚠.

1. **Advertisement full-set re-request cadence** — also sets the reconcile TOCTOU / tick bound.
2. ⚠ **Uplink egress meter dependency (F4)** — no in-tree egress meter exists today (`update_rate_limit.rs` is inbound only). Uplink is load-bearing in three places: the admission budget (now the **primary fanout bound**), the NACK's saturated-resource field, and coalescing. Interim proxy = sends-per-update × mean payload size; the real meter is a greenfield build that must ship before uplink-aware admission is real.
3. **A4 blend function** and the `g0(distance)` prior shape (only ratios matter).
4. **NACK variant name + payload layout** (which resource + retry-after) and the **retry-after derivation**.
5. ⚠ **Chain-cap K accounting (F3)** — the `K_displacement_depth` and `K_placement_failures` values (now split, one each), AND the **wire-carried depth/attempt counter** that enforces both: a new subscribe/re-subscribe field, version-gated, increment-only, with an anti-spoof clamp (missing/absurd → treat as at the cap).
6. **OOM-valve trigger point** — what memory headroom counts as "real overflow."
7. **Reserved client-pin slice size**, and post-fetch vs. size-blind admission checking (state size is unknown until fetched).
8. **Root re-verify cadence**, and whether re-root and root-verify share one search path or two.
9. **Summary-first PUT message shape** — the probe carrying a `StateSummary`, the new-vs-existing dispatch signal, and its version gate. (The summary/delta *encoding* is NOT open: `StateSummary` / `StateDelta` are contract-defined opaque bytes, reused from the existing anti-entropy path.)
10. **PUT client completion semantics** (ack at probe / seed / merge) and the per-hop body-fetch source in the existing-contract case.
11. **Equidistant candidate tie-breaks** for NACK onward-routing.
12. **Coalescing-interval function** (uplink pressure → interval), the maximum interval, and whether the pressure signal is per-contract or per-peer.
13. **Reconcile parameters** — tick period vs. re-request cadence, hysteresis window, per-contract rate-cap value, formation-window damping.
14. ~~PUT-with-subscribe vs. GET-with-subscribe coverage~~ RESOLVED: both in scope (Ian named GET AND PUT explicitly, 2026-07-02 / 07-08).
15. **Timelines** — final removal of the repurposed SUBSCRIBE variant, and how long to keep Source-2 emission version-gated.
16. **Isolated-holder catch-up** — whether the no-co-host cohort needs any bound beyond background re-root.
17. **Findability L1/L2/L7** sim-metric and landing order.

Settled by reference (NOT open): the locally-verified-root criteria and duplicate-root reconciliation; interest-gated renewal; the re-root search primitive; the Source-2 version/formation gates; the four pillars and decisions D1/D2.
