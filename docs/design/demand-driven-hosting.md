# Demand-driven hosting and subscription

**Status: canonical design-intent doc for the demand-driven hosting/subscription model. Converged with Ian on 2026-07-01. This is the WHY-and-HOW reference the invariants file points at; the code does not yet fully implement it (that is the redesign, tracked in the epic below).**

- Tracker: [freenet/freenet-core#4642](https://github.com/freenet/freenet-core/issues/4642)
- Invariants (the short, load-bearing rules this doc explains): `.claude/rules/hosting-invariants.md`
- Sibling design doc for the already-shipped eviction/budget piece: `docs/design/hosting-eviction.md`

This document explains how contract hosting and subscription are *supposed* to work and, for each design choice, *why* it is that way rather than some plausible alternative. It is written for humans reviewing the design and for future agents who need to change hosting/subscription code without re-deriving the reasoning (or re-introducing a fixed bug). If you are about to touch hosting, placement, eviction, subscription, or the GET/PUT/SUBSCRIBE/UPDATE paths, read this and the invariants file first.

---

## 1. Core principle: hosting is demand-driven

A peer hosts a contract because **real demand routed to it**. "Real demand" means exactly one of two things:

- a **local client** on that peer is reading or subscribing to the contract, or
- a **downstream subscriber** (another peer, farther from the contract's key, that has active demand of its own) routed a subscription through it.

A peer does **not** host a contract because it forwarded a request for it, because a neighbor pushed it, or because it happened to relay traffic for it. Replication degree is not a tunable global constant; it emerges from where demand exists and how requests route.

### There is no "relay" category

This is the single most important framing point, and it is easy to lose. A peer either **hosts** a contract (it holds the state, is subscribed, and stays fresh in the update mesh) or it **does not**. Forwarding a request one hop closer to the contract's key is just **routing**. It is not a persistent role, not a category, not a state a peer can be "stuck" in.

The current code is full of `relay` naming (`drive_relay_subscribe`, `relay_subscribe_does_not_install_lease`, "a relay is not itself a subscriber" comments, hundreds of occurrences under `operations/`). Read all of it as a **fossil**. It is the residue of the "hollow relay" firefight described in section 9, and it is being removed. When you see `relay` in the code, translate it in your head to either "a peer forwarding a request (routing)" or "the legacy hollow-relay that piece D deletes." Do not treat relaying as a design concept, and do not name new code `relay`.

**Why insist on this?** Every past hosting bug traces back to a copy that exists for a reason other than demand: a cache left on a return path, a contract pushed toward a key because someone held it, a subscription record that outlived the interest that created it. Collapsing the ontology to "hosting or not, driven only by demand" removes the categories where those phantom copies used to hide.

---

## 2. Consistency and the update model

### No authoritative copy

There is no primary, no leader, no authoritative replica. Every copy of a contract's state is equal. Contract state is a **commutative monoid**: updates are merged with an operation that is associative and commutative, so any set of updates applied in any order converges to the same state. Mergeability is a requirement the contract model places on contract authors, not an assumption the network makes and hopes holds.

**Why no authority?** An authoritative copy would need a fixed home, a way to elect or locate that home, and a way to recover it after the hosting peer churns out. All three are expensive and fragile in a peer-to-peer network with constant membership change. A commutative merge sidesteps the whole problem: correctness no longer depends on *which* copy you talk to or *what order* updates arrive in. Peers near the contract's key location host it more often, but only because requests route toward the key (gravity), not because those peers hold any special status.

### Updates propagate by proximity

Any two peers that are **connected** and **both host the same contract** automatically exchange that contract's updates. That is the entire propagation rule. There is no separate "forward this update to my list of registered subscribers" step.

It follows that a contract's **update mesh is just the connected sub-graph of the peers that host it**. An update injected anywhere in that sub-graph floods to every peer in it (merging commutatively at each hop) and converges.

**Why proximity rather than an explicit subscriber list?** An explicit forwarding list is a second source of truth that has to be kept in sync with the actual set of hosts. Every past subscription bug was some version of "the forwarding list and the real hosting state disagreed": a subscriber that stopped hosting but stayed on someone's list, or a host that never got added to the list and silently went stale. Deriving propagation directly from "are we connected and do we both host it" removes that second source of truth. There is nothing to get out of sync.

### The consequence for a lone reader

A peer that holds a contract's state but is **not connected to any other host** is isolated from the update mesh. It receives no updates, so its copy silently goes stale and becomes useless (worse than useless, if it answers reads with stale state). This is the crucial constraint that motivates subscription: to stay fresh, a peer must not merely hold the state, it must be **connected into the mesh**.

---

## 3. What a subscription is for

Put the last two points together. For a client's peer to serve a contract correctly it must satisfy **both** conditions:

1. it **hosts** the contract (holds the state, so it can answer reads locally), and
2. it is **in the update mesh** (connected, transitively, to at least one other host, so its state stays current).

If the client's peer hosts the contract but none of its neighbors host it, condition 2 fails: it is an island, and it goes stale.

So the job of a **SUBSCRIBE** is to build a **connected path of co-hosts** from the client's peer toward the contract's key, where every peer on the path hosts the contract, until the path joins the existing update mesh. Concretely, a subscribe routes toward the key; each hop it passes through becomes a real host (it fetches the state if it does not have it, calls `ring.subscribe`, and advertises via `announce_contract_hosted`); the walk stops when it reaches a peer that is already a host (already in the mesh) or reaches the key region. After the path exists, **proximity (section 2) handles all further update propagation for free**, because there is no ongoing "push updates down the chain" machinery: adjacent path peers are connected co-hosts and therefore already exchange updates automatically.

**Why build a chain of real hosts instead of a chain of lightweight forwarders?** Because a forwarder that does not host cannot stay fresh and cannot answer reads, which makes it a dead end for both GET and update propagation. That dead-end pattern (the "hollow relay") is exactly the production failure section 9 recounts. In this model there is no lightweight middle tier: every peer on a live subscription path is a first-class host and a first-class member of the update mesh.

---

## 4. Upstream is computed, not stored

Every hosting peer has, for each contract it hosts, an **upstream**: the direction of the contract's key. The upstream is defined as

> among this peer's connected neighbors that host the contract and are **strictly closer to the contract's key** than this peer is, the upstream is the one **closest to the contract's key** (the most keyward such neighbor).

The word "closest" here means closest *to the contract's key*, not closest to this peer: of all the eligible neighbors (connected hosts that are more keyward than you), your upstream is the single most keyward one, the next real host on the path toward the key.

It is **computed from current state** every time it is needed, from two inputs the peer already has: the neighbor-hosting advertisements it has collected (which of my connected neighbors host this contract), and ring distance (how far each of us is from the contract's key). It is **not** a flag written down when the subscription first formed.

**Why computed rather than stored?** Two reasons, and they are the load-bearing part of the whole design.

1. **A derived-from-current-state property is self-correcting; a formation flag rots.** If "who is my upstream" is recomputed from the live set of hosting neighbors, then when the network changes (a neighbor churns out, a host closer to the contract's key appears), the answer updates itself the next time it is read. A flag set at formation time records a fact about a network that may no longer exist. After enough churn, a stored-upstream/stored-downstream scheme drifts into states where two peers each believe the other is downstream of them, a reciprocal inconsistency with no ground truth to correct it. That mutual belief is precisely the self-perpetuation loop section 7 exists to prevent.

2. **"Strictly closer" is a total order, so the upstream relation is acyclic by construction.** Distance-to-key is a total order over peers (ties broken as in section 6). "My upstream is strictly closer to the key than me" therefore means the upstream relation only ever points in the direction of decreasing distance. You cannot build a cycle out of strictly-decreasing steps. A mutual-upstream cycle, the pathological "A keeps B alive, B keeps A alive, forever, with no real demand," is **structurally impossible**. A formation flag gives you no such guarantee: nothing stops two peers from recording each other.

This is why the doc keeps saying *computed* upstream and *strict distance*: those two properties are what make the maintenance mechanism in the next section safe.

---

## 5. Renewal: the single maintenance primitive

A hosting peer keeps its place in the chain by periodically sending a **subscribe renewal** to its (computed) upstream. That single act is the only maintenance mechanism, and three important behaviors fall out of it without any additional machinery.

### 5a. Renewal is gated by demand

A peer sends renewals toward its upstream **only while it is itself `contract_in_use`**, where `contract_in_use` means: it has a local client interested in the contract, **or** it has a registered downstream subscriber (a peer strictly farther from the key than itself). This is the *same* predicate that decides whether the peer keeps hosting (section 6): one condition drives both.

When a peer's demand dries up (its client leaves and its last downstream lapses), it stops renewing. Its registration at the upstream then lapses on its own (downstream registrations lease-expire, see section 6). That removes one unit of demand from the upstream, which may in turn cause the upstream to fall out of `contract_in_use`, stop renewing, and lapse at *its* upstream. The effect **propagates inward toward the key**, collapsing exactly the part of the chain that no longer has demand behind it.

**Why gate renewal on demand?** This is the fix for the storm failure mode (section 7). If leases renew unconditionally, the number of live subscriptions grows with the amount of state a peer has ever cached and never shrinks, because nothing ever stops renewing. Gating renewal on active interest makes the live-subscription count track **active demand**, not accumulated cache. Idle contracts stop being renewed, their chains collapse, and they evaporate, which is the intended behavior for zero-demand contracts per invariant 3 and the "idle-persistence" anti-pattern.

### 5b. Renewal re-targets automatically (re-rooting)

Renewals always go to the **current computed upstream** (section 4). So when the network changes, a host closer to the contract's key appears or the current upstream disconnects, the peer's computed upstream changes, and its very next renewal simply goes to the new upstream instead. The old upstream, no longer receiving renewals from this peer, lets the registration lapse (and if this peer was its only downstream and it has no client of its own, the old upstream collapses per section 6).

**Re-rooting is therefore not a separate operation.** It is just the renewal stream following the computed upstream as that computation changes. Registering as the *new* upstream's downstream is nothing special either: it is simply the first renewal this peer sends to it. There is no distinct "migrate subscription" code path to get wrong.

**Why fold re-rooting into renewal instead of building a dedicated migration mechanism?** Because a dedicated mechanism is a second place where the chain's shape is decided, and it would have to agree with the computed-upstream logic in every corner case. Making re-rooting a consequence of "renew toward whoever the current upstream is" means there is only one rule, and it is self-consistent by definition.

### 5c. The frontier (boundary) case

A peer with active demand but **no connected co-host strictly closer to the key** has no upstream to renew toward. It sits at the frontier of the mesh. Instead of renewing, it **routes a fresh subscribe toward the key** to establish a link into (or extend) the mesh. This is how a chain first reaches toward the key, and how a stranded sub-tree re-attaches after its upstream vanishes. Piece F (section 9) makes this event-driven: the peer reacts to detecting the upstream's disappearance rather than waiting for a lease timer, so the window of staleness is short. Section 5d refines this: a peer with no upstream is in one of two structurally different situations, a genuine keyward terminus or a stranded host that must re-root, and the correct response differs between them.

### 5d. Boundary cases: no valid upstream

A peer can end up with no computed upstream (section 4) for two structurally different reasons. Both are handled, neither is a stuck state, and the distinction matters because the right response is different.

**(a) The keyward terminus (local root).** A peer that hosts the contract but has **no connected peers closer to the contract's key** is the root of its chain (of its local mesh component). It has no upstream, and that is **correct, not a stuck state**: it is the keyward end of everything it can see, so there is simply nothing more keyward to renew toward. It is *not* the case-(b) frontier that must re-subscribe, because there is no connected peer closer to the contract's key to route a subscribe through. It keeps hosting exactly as the stop-hosting rule (section 6) dictates: while it has a **local client** or a **downstream** (a connected co-host farther from the contract's key that depends on it). If that demand vanishes it stops hosting like any other peer, and the contract evaporates from it. Being keyward-most is not itself demand; nothing special keeps a root alive.

*Caveat (why "no closer peers" is a local statement).* "No closer connected peers" is relative to this peer's own connection set. A genuinely-closer peer that also hosts the contract but that this peer is **not connected to** forms a **separate mesh component**. The two components reconcile when they become connected, via the contract's commutative merge (piece G, section 9). That is a topology concern (get the components connected), not a hosting-design gap: once connectivity exists, the merge folds the divergent copies order-independently and losslessly.

**(b) The stranded / mis-rooted host (re-rooting).** A peer that hosts the contract and **does** have connected peers closer to the contract's key, but **none of those closer peers host it**. This is typically produced by churn: the near-key hosts dropped out, leaving this peer as the keyward-most survivor of what used to be a longer chain. Its computed upstream is empty not because it is the true terminus but because the hosts that used to be its upstream are gone. If the peer still has demand, this triggers **re-rooting**.

The mechanism is the same primitive as section 5c: with demand, the peer routes a fresh **SUBSCRIBE** toward the contract's key. The closest-to-the-key peer the subscribe reaches, call it **C**, receives the demand and becomes a host, so C is now this peer's upstream and the chain toward the key is re-established. The process recurses: C, now a fresh host, either reaches a terminus of its own (case (a)) or joins an already-existing keyward mesh, and the chain is whole again.

The point worth stating explicitly is **how C gets the state**. C bootstraps its copy **backward** from the stranded holder using the ordinary, direction-agnostic summary/delta sync that any two connecting co-hosts run: C sends its (empty) state summary, and the holder returns the full state as a delta. The commutative-monoid state and the summary sync **do not care that the holder is farther from the contract's key than C**, so re-rooting needs **no bespoke "push state keyward" path**; it is just the normal sync running in whichever direction demand happens to require. (This is the same reason there is no authoritative-copy problem: state flows to wherever a host needs it, independent of key-distance.)

One freshness guard: C hosts (joins the mesh) but **must not serve reads until it has synced**. This is exactly invariant 1's single permitted transient, "state in flight during a GET or resync": a just-joined host is briefly not-yet-fresh and must not answer reads until it is, or it would reintroduce the stale-read failure invariant 1 exists to forbid.

**Why this is not the placement-push anti-pattern.** Re-rooting is **demand-gated**. It fires because real demand (a local client or a downstream on the stranded holder) needs the contract kept fresh, and it works by a SUBSCRIBE/pull toward the key, not by a peer pushing a contract outward because it happens to hold one. That demand-gating is precisely the line between this legitimate recovery and the holding-driven placement-push anti-pattern the invariants forbid (section 9): the same keyward movement of state, but the opposite trigger.

**Degradation, no deadlock.** If the peers closer to the contract's key refuse the subscribe because they are over budget (piece B, admission), there is no deadlock. The subscribe diversifies to other peers closer to the contract's key, or, failing that, the peer simply remains a **valid stranded host serving its own mesh component** until some peer closer to the contract's key can take it, with terminal-advertisement consult (piece C, section 9) keeping the contract findable in the meantime. The worst case is **suboptimal placement** (the contract is hosted farther from its key than ideal), never unavailability.

---

## 6. The stop-hosting / collapse rule

A peer **keeps hosting** a contract **iff** it has either:

- a **local client** interested in the contract, **or**
- a registered subscriber **strictly farther** from the contract's key than itself (a downstream).

It **stops** hosting when it has neither. Critically, the **upstream (any neighbor closer to the key) is explicitly excluded** from what can keep it hosting. Note this is the same `contract_in_use` predicate as the renewal gate in section 5a: a peer keeps hosting exactly as long as it keeps renewing, and stops both together.

### Why the upstream must be excluded

This exclusion is the anti-self-perpetuation rule, and it is subtle enough to be worth spelling out.

Co-hosting is symmetric at the update-mesh level: two connected peers that both host a contract exchange updates in both directions, and each is aware the other hosts it. If "keep hosting because a neighbor also hosts it and wants updates" counted **any** co-hosting neighbor, then two connected co-hosts would each count the other as a reason to stay, and the pair would keep each other alive **forever, with no real demand behind either of them**. That is mutual self-perpetuation: a two-peer chain that never dies.

Requiring the sustaining subscriber to be **strictly farther** from the key breaks the symmetry. Of any two connected co-hosts, exactly one is farther from the key. Only that farther one is a "downstream" from the closer one's perspective; the closer one is the farther one's **upstream** and does **not** count as its downstream. So when demand disappears, the **farther** peer (which has no one farther still, and no client) is the first to find itself with no downstream, so it collapses first. Its collapse removes it as the closer peer's downstream, so the closer peer collapses next, and so on. The collapse runs **leaf-inward** (farthest peer first, toward the key) and is guaranteed to terminate.

### Why this terminates (and why it needs the section-4 definitions)

The termination guarantee rests on three properties, and each is why an earlier, simpler-looking choice was rejected:

1. **"Farther" is strict distance to the contract's key**, which is a **total order**, so the downstream relation is **acyclic**: it always points away from the key. A collapse walking "leaf-inward" along an acyclic relation cannot loop and must reach the frontier. *This is the concrete reason upstream/downstream must be defined by computed distance (section 4) and not by a formation flag: the formation flag has no ordering property, so it cannot guarantee acyclicity, so collapse under it can fail to terminate.*
2. **Ties are handled by strict `<`.** If two peers are exactly equidistant from the key, neither is strictly farther than the other, so neither counts the other as a downstream. The worst case is that both stop hosting, which is safe (it never wrongly keeps a copy alive). An optional peer-id tiebreak can keep one of them if that turns out to matter in practice, but the default strict comparison already fails safe.
3. **Downstream registrations lease-expire.** A registration created by a renewal has a TTL; if the downstream stops renewing (because it collapsed, or churned out), the registration disappears on its own. This is what guarantees a collapsed or vanished downstream stops counting as demand: there is no phantom keep-alive holding the upstream up after the real subscriber is gone. (This mirrors the general project rule that every GC exemption must be time-bounded.)

---

## 7. The two failure modes this design guards against

The whole design is shaped by two specific, historically real failure modes. Everything above is in service of making both impossible.

### Self-perpetuation: a chain that will not die

A subscription chain stays alive after all real demand behind it is gone, holding hosting slots and update-fanout cost hostage to nothing. **Prevented by:** the strict-distance total order (which makes the downstream relation acyclic, so any collapse terminates), plus excluding the upstream from what keeps a peer hosting (so co-hosts cannot prop each other up), plus lease-expiring downstream registrations (so vanished demand actually stops counting). Remove any one of the three and mutual perpetuation becomes reachable again.

### Storm: subscriptions proportional to accumulated cache

The number of live subscriptions grows with how much state a peer has ever cached, rather than with how much is actively wanted, because leases renew regardless of interest. This is the [#3763](https://github.com/freenet/freenet-core/issues/3763) class of incident: renewal traffic and subscription state scale with cache size, which only ever grows, producing a renewal storm. **Prevented by:** interest-gated renewal (section 5a). A peer renews only while `contract_in_use`, so live subscriptions scale with **active demand**, which rises and falls, instead of with cache, which only accumulates.

**Note on scope: the eviction budget does not fix the storm.** It is tempting to think piece A's memory/CPU budget and demand-ordered eviction gauge (`docs/design/hosting-eviction.md`) already bound the storm. They do not, and this is a load-bearing distinction. The eviction gauge bounds **bytes/slots** by evicting low-demand contracts, but an in-use, actively-subscribed contract is **eviction-exempt** (it is pinned by demand) *and* self-renewing. So the set of contracts responsible for the renewal storm is exactly the set the budget is forbidden to touch. Interest-gated renewal is a **separate** change to the shared renewal predicate (`contracts_needing_renewal`), and it is the only thing that bounds the storm. Do not conflate the two mechanisms; an earlier "just bound it with the budget" framing was wrong.

---

## 8. A worked example

A concrete trace, to make the mechanics tangible. Peers are named by increasing distance from the contract's key: **K** (closest, in the key region), **M** (middle), **C** (the client's peer, farthest). All three are connected in a line K–M–C.

**Formation.** A client on C subscribes. None of C's neighbors host the contract, so C routes a fresh subscribe toward the key (section 5c). The subscribe walks C to M to K; each hop fetches state, calls `ring.subscribe`, advertises hosting, and becomes a real host. K is in the key region (or already in the mesh), so the walk stops. Now K, M, C all host and are connected, so proximity (section 2) keeps all three current. Computed upstreams: C's upstream is M, M's upstream is K, K has none (it is the most keyward, closest to the contract's key). ("Closest" throughout means closest to the contract's key, not to any particular peer.) Downstreams: M is registered as K's downstream; C is registered as M's downstream. All held by renewals.

**Steady state.** C renews to M (C is `contract_in_use` via its local client). M renews to K (M is `contract_in_use` via its downstream C). K renews to no one but keeps hosting because it has a downstream M. Updates injected anywhere flood the K–M–C sub-graph and converge.

**The client leaves.** C loses its local client and has no downstream, so C is no longer `contract_in_use`. C stops renewing to M and stops hosting. Its registration at M lease-expires. Now M has no local client and no downstream (C is gone), so M stops being `contract_in_use`, stops renewing to K, and stops hosting; its registration at K lapses. Now K has no downstream and no client, so K stops hosting too. The contract, which now has zero demand anywhere, evaporates from the network, exactly as intended for a zero-demand contract. The collapse ran C to M to K (leaf-inward) and terminated at the key.

**Re-rooting variant.** Suppose instead that while C, M, K are all hosting, a new peer N joins, connects to C, and starts hosting the contract, and N is strictly closer to the key than M. Then C's computed upstream changes from M to N. C's next renewal goes to N (registering C as N's downstream); C stops renewing to M. M's registration of C lapses; if C was M's only downstream and M has no client, M collapses. The chain has re-rooted from C–M–K to C–N–(toward K), with no dedicated migration step, just the renewal stream following the computed upstream.

---

## 9. Relation to the redesign pieces

This model is the design content of several pieces of the epic ([#4642](https://github.com/freenet/freenet-core/issues/4642); the task map with per-piece status lives in `.claude/rules/hosting-invariants.md`). In short:

- **Piece A, eviction/retention and the capability budget.** **Shipped in 0.2.90** (telemetry #4643, RAM-sized budget #4644, demand-ordered eviction #4650). Documented in `docs/design/hosting-eviction.md`. A bounds **bytes/slots**; it deliberately does **not** touch the renewal storm (section 7). A had to ship and be proven in the field first, because the pieces below assume its bound is already live (host-on-GET without A's gauge would be relay-caching; host-chains without A's budget would be the storm).
  - **The retention/eviction/admission demand signal is SUBSCRIBER COUNT** (subscriber-primary rework, PR-1). Eviction orders victims by ascending `(subscriber_count, last_get_seq)`: the **fewest-subscriber** contract first, ties broken by **least-recent real GET read**. `subscriber_count` is genuine demand — local client subscriptions plus downstream subscribers, the same two sources as `contract_in_use`, so a subscribed contract is pinned in normal operation and retention agrees with the section-6 collapse rule by construction. **Distance is removed from the eviction ranking**: its causal effect on demand already flows through subscriber count (subscriptions route toward the key, so near-key peers accumulate more subscribers — counting both would double-count), and network locality is delivered by **routing** (sections 2–4), not by eviction. The earlier "predicted read-demand / proximity-prior" estimator (A3/#4650/#4688) is **demoted to telemetry** — still trained and dashboarded, no longer part of the eviction key — and slated for deletion once subscriber-primary is field-validated. The subscriber-count *gradation* (one vs many subscribers) bites only at two places: **admission** (a full peer displaces a strictly-fewer-subscriber incumbent, or refuses — piece B) and the **OOM valve** (under genuine RAM overflow the peer sheds its fewest-subscriber contract, piercing the in-use pin, so it survives rather than OOMs; the Overflow trigger is deliberately unwired until the RSS signal is plumbed). In **normal** over-budget operation every subscribed contract is pinned and only zero-subscriber contracts are dropped, by real-GET recency.
- **Piece D, the subscription model in this document.** Computed upstream + interest-gated renewal + real co-hosting chains that collapse when demand ends. This is the keystone piece: it deletes the hollow-relay code and replaces the unconditional `contracts_needing_renewal` behavior with the interest gate. It needs simulation proof (no-storm under load, and chain-collapses-on-client-leave, across several seeds) because it re-enables the very path the #3763 firefight disabled.
- **Piece F, event-driven re-subscribe.** Makes re-rooting (section 5b) and frontier re-attachment (section 5c) **prompt**: a peer re-subscribes on *detecting* its upstream drop rather than waiting for a lease-expiry timer, which shrinks the staleness window. It is the same computed-upstream logic, triggered by an event instead of a timer.
- **Pieces B and E, admission and cache/auto-subscribe cleanup, fold into this model.** B (an over-committed peer refuses new subscriptions; host-on-first-GET under the gauge) pairs with A's budget. E (remove relay-caching and GET-auto-subscribe) is gated on D landing, because D is what makes the removed behavior unnecessary.
- **Piece C, terminal advertisement consult.** Additive and separable (the terminus-consult landed in #4646). At the terminus of a routed request, the peer consults host advertisements to find who hosts the contract, rather than relying on state having been pre-scattered toward the key. This is what keeps a contract findable without speculative replication (invariant 5), including a stranded host still serving its own mesh component in the section-5d degradation case.
- **Piece G, partition/fork reconciliation.** Divergent copies of a contract's state (for example the separate mesh components of section 5d case (a), or the two sides of a network partition) reconcile via the contract's commutative-monoid merge once connectivity between them is restored. Because the merge is order-independent and lossless, this is a topology concern (get the components connected) rather than a hosting-design gap; the residual work is a simulation check that reconnection actually happens on heal, not a new mechanism.
- **Piece H, mixed-version rollout.** The change is behavioral, not a wire-format change, so old and new nodes interoperate. The `NetMessageV1::SubscribeHint` enum variant is **kept** for wire compatibility even though it is no longer emitted; do **not** remove it as part of this work (removing an enum variant shifts bincode indices and *is* the cross-version break). Retire it only as a separate later cleanup once the whole fleet is past it.

---

## 10. The one-paragraph summary

Hosting is demand-driven: a peer hosts a contract only because a local client or a farther-out subscriber routed demand to it, and forwarding a request is just routing, not a role. State is a commutative monoid with no authoritative copy, and updates flow automatically between any two connected co-hosts, so a contract's update mesh is simply the connected sub-graph of its hosts. A subscribe builds a connected path of real hosts from the client's peer toward the key so that peer is both able to serve reads and kept fresh by proximity. Each peer maintains its place with one primitive, a renewal sent to its **computed** upstream (of its connected hosts that are strictly nearer the contract's key than itself, the most keyward one), gated by whether it still has demand, and both collapse-on-idle and re-rooting fall out of that one primitive. A peer keeps hosting only while it has a local client or a strictly-farther downstream, never because of a closer neighbor; that strict-distance rule makes the chain acyclic and its collapse terminating, which (with lease-expiring registrations) kills self-perpetuation, while interest-gated renewal kills the cache-proportional storm.

---

*[AI-assisted - Claude]*
