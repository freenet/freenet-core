//! Registry of which delegates want to hear about which contracts' state
//! changes, and the bound on how much of that one delegate may hold.
//!
//! This is the sibling of [`crate::contract::delegate_app_registry`], and the
//! two are deliberately shaped alike: that one maps `delegate -> apps` (where a
//! delegate's output goes), this one maps `contract -> delegates` (which
//! delegates hear about a contract). Both are process-global, both are written
//! from paths an external actor influences, and both therefore carry caps
//! enforced at insertion. Read `delegate_app_registry`'s header alongside this
//! one; where this module departs from its shape, it says so.
//!
//! # Why this module exists rather than a bare `DashMap`
//!
//! Until this module the registry was a single public `DashMap` mutated from
//! five call sites across `wasm_runtime` and `contract`. Bounding it requires a
//! second, reverse index (`delegate -> contracts`) so the per-delegate cap can
//! be checked in O(1) rather than by scanning every contract. Two maps that must
//! agree are only safe if nothing outside can desynchronise them, so both are
//! private here and every mutation goes through a function in this file. That is
//! the same reason `delegate_app_registry` owns both `DELEGATE_APPS` and
//! `CLIENT_REGISTRATION_COUNTS` rather than exporting them.
//!
//! # The registration path
//!
//! A delegate subscribes by emitting
//! `OutboundDelegateMsg::SubscribeContractRequest`, handled in
//! `contract::handle_delegate_with_contract_requests`, which reaches
//! [`subscribe`]. There used to be a second path, the `subscribe_contract` host
//! function, and a bound applied to only one of the two would have been an
//! opt-out; that host function was removed in #5637.
//!
//! # Durability (#5493)
//!
//! The registry also has a DURABLE half, one redb row per pair
//! (`DELEGATE_SUBSCRIPTIONS_TABLE`), so a delegate's subscriptions survive a
//! node restart. Without it a restart silently dropped every delegate
//! subscription, and nothing re-established them: a delegate only runs when
//! something invokes it, and the thing that would invoke it is a notification
//! on the contract whose subscription just vanished.
//!
//! The durable half is written ONLY by the writers in this file, each of which
//! takes a [`Durability`] argument, so a call site cannot mutate one half and
//! forget the other: it has to say, at the call, whether the mutation is
//! mirrored to disk. The storage handle is a parameter rather than a module
//! global because the registry is process-global while a `Storage` belongs to
//! one node, and several nodes share one process in every in-process test; a
//! global handle would send one node's durable writes to another node's
//! database.
//!
//! [`restore_from_storage`] replays the durable set into memory at startup
//! (`NetworkContractHandler::build`), through the SAME admission path a live
//! subscribe uses, so the per-delegate cap applies on restore exactly as on
//! add. Re-establishing the interest and network subscription a live subscribe
//! takes is done off-loop by `contract::delegate_restore`.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractInstanceId, DelegateKey};

use crate::contract::storages::Storage;

/// Maximum number of distinct contracts one delegate may hold a subscription to.
///
/// # What one subscription costs
///
/// A subscription is a standing claim: every commit to the contract wakes this
/// delegate with the full new state (`Executor::send_delegate_contract_notifications`),
/// and delegate execution is globally serialised on the contract-handling loop,
/// so the cost is paid in that loop's time. The claim also outlives the app that
/// asked for it — there is no unsubscribe (#5600) and, deliberately, no TTL (see
/// [`subscribe`]).
///
/// # Derivation
///
/// The directly analogous constant is
/// `contract::executor::MAX_SUBSCRIPTIONS_PER_CLIENT` (500) — "maximum total
/// subscriptions a single client may hold across all contracts". Its history is
/// the useful part: it was **raised from 50 to 500** on 2026-08-22 because 50
/// "was hit almost immediately" by apps that subscribe to one contract per
/// discoverable peer. A cap that real apps hit is worse than no cap, because it
/// converts an amplification bound into an app-compatibility bug, so this one is
/// set well above any plausible legitimate use rather than as tight as the
/// arithmetic would allow.
///
/// Legitimate use, measured rather than guessed: River's chat delegate holds one
/// subscription per room **it owns the signing key for** (`EnsureRoomSubscription`
/// fires only for owner-mode rooms). That is single digits for an ordinary user
/// and tens for a heavy one. 256 leaves an order of magnitude of headroom over
/// the heavy case.
///
/// # What it bounds, and what it does not
///
/// **This bounds one delegate. It does NOT give the node an aggregate ceiling,
/// and an earlier version of this comment claimed it did.** That claim said the
/// reachable total was `MAX_CREATED_DELEGATES_PER_NODE` (1024) x this, and it is
/// wrong: that counter is incremented only by the `create_delegate` host
/// function (`native_api.rs`), and `delegates.rs` states outright that
/// "delegates registered directly by apps were never counted". So an app that
/// registers delegates over its own WebSocket increments nothing, each distinct
/// code+params pair is a distinct `DelegateKey`, and each such key gets a fresh
/// budget of its own. The number of delegates is not bounded on that path, so
/// neither is the product.
///
/// What IS bounded is the delegate-spawns-delegate composition: a delegate
/// created by another delegate does pass through the counter, so that path is
/// capped at 1024 x this.
///
/// State the residual rather than leaving the reader to find it: against an app
/// that will register unlimited distinct delegates, this constant limits the
/// blast radius of any ONE of them and nothing more. That is still worth having
/// — it is what stops a single delegate accumulating an unbounded standing
/// claim — but it is a per-principal bound, not a node-wide one, and the
/// node-wide ceiling has to come from bounding delegate registration, which is
/// not this change. The comparison with `MAX_SUBSCRIPTIONS_PER_CLIENT` (per
/// `ClientId`, and a client mints a fresh one by opening another WebSocket) is
/// therefore a similarity and not a contrast, which is the opposite of what this
/// paragraph used to say.
///
/// **A count is not a byte budget** (`.claude/rules/code-style.md`, clause 4), so
/// state the byte story explicitly rather than leaving a count to imply one. Two
/// separate mechanisms carry it, and NEITHER is this constant:
///
///  * **In flight**: a notification does not copy the state per subscriber —
///    `send_delegate_contract_notifications` clones it into one `Arc` shared
///    across every subscriber, sent over a bounded, lossy channel
///    (`DELEGATE_NOTIFICATION_CHANNEL_SIZE`, dropped when full rather than
///    queued). So in-flight bytes are bounded by channel depth, not by how many
///    subscriptions exist.
///  * **At rest**: a held subscription raises hosting demand, but it does not
///    pin residency. `HostingCache::evict_over_budget`
///    (`ring/hosting/cache.rs`) treats subscriber count as the eviction
///    ORDERING, not as a filter — a subscribed contract sorts last and is still
///    evicted when nothing cheaper is eligible and the node is over either the
///    state-byte budget or the resident-overhead budget. Both budgets are
///    enforced regardless of local demand.
///
/// What this constant adds on top is a bound on how much of those SHARED
/// node-wide pools one delegate can claim before that reactive eviction has to
/// pull the node back under budget — eviction is a last resort, and reaching it
/// costs churn borne by every other subscriber on the node, not just the
/// delegate that caused it.
///
/// **If you are about to conclude 256 is too generous, this is the paragraph you
/// want.** The pessimistic arithmetic — 256 x `MAX_STATE_SIZE` = 12.8 GiB per
/// delegate — is the calculation to do when a count cap IS the only byte bound,
/// and that is not the case here: a subscription ORDERS its contract last for
/// eviction, it does not pin it, so the byte budgets above still reclaim it.
/// This was checked against `evict_over_budget` rather than assumed, because the
/// two readings differ by an order of magnitude in what cap they justify, and
/// the wrong one produces a cap real apps hit. If a future change ever makes a
/// held subscription genuinely un-evictable, that check lapses and this number
/// has to be re-derived against bytes rather than against River's room count.
///
/// # Interaction with in-flight work
///
/// PR #5615 removes the "the contract must already be local" precondition on the
/// V1 subscribe path, letting a delegate name any instance id and have the node
/// fetch it from the network. That widens the reachable set from "contracts this
/// node happens to hold" to "any contract that exists", which makes this bound
/// more load-bearing than it is today, not less. #5615's own
/// `MAX_NETWORK_CONTRACT_OPS_PER_PARK` throttles concurrent fetches in flight;
/// it does not bound the steady-state count, which is this constant's job.
///
/// # Not configurable, deliberately
///
/// Same reason as `MAX_SUBSCRIPTIONS_PER_CLIENT`, where making it a per-node
/// option was proposed and explicitly rejected (Ian, 2026-08-22): a configurable
/// cap means a dApp works on some peers and not others, which is exactly the
/// non-uniformity Freenet must avoid.
pub(crate) const MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE: usize = 256;

/// `contract -> delegates`. The fan-out direction, read on every state commit.
static BY_CONTRACT: LazyLock<DashMap<ContractInstanceId, HashSet<DelegateKey>>> =
    LazyLock::new(DashMap::default);

/// `delegate -> its subscribed contracts, each stamped with the last time a
/// notification was delivered for it`.
///
/// The reverse index of [`BY_CONTRACT`], maintained so the per-delegate cap is
/// an O(1) lookup instead of a scan over every subscribed contract — the same
/// role `CLIENT_REGISTRATION_COUNTS` plays in `delegate_app_registry`. It holds
/// stamps rather than a bare count because the cap evicts rather than refuses,
/// and eviction needs to know which entry is coldest.
///
/// `tokio::time::Instant` (not `std::time::Instant`) so tests using
/// `tokio::time::pause` / `advance` can drive eviction order deterministically,
/// matching `AppRegistration::last_seen` and `DelegateContextEntry`.
static BY_DELEGATE: LazyLock<
    DashMap<DelegateKey, HashMap<ContractInstanceId, tokio::time::Instant>>,
> = LazyLock::new(DashMap::default);

/// Total cap evictions since process start.
///
/// An operator needs the aggregate, not one log line per event: a node shedding
/// subscriptions steadily looks identical, line by line, to one that shed a
/// single subscription an hour ago. Reported as a field on the `warn!` in
/// [`subscribe`] rather than only through the test accessor, so the running
/// total is in RELEASE builds where saturation has to be visible
/// (`.claude/rules/code-style.md`) — the same shape as the `total_dropped`
/// counter on the delegate-notification drop path.
///
/// Surfacing it through `HostingCacheStats`-style telemetry alongside
/// `subscribed_evictions_total` is worth doing and is deliberately NOT done
/// here: this registry is a process-global with no stats struct to hang it on,
/// and inventing one belongs in its own change rather than on this bound.
static CAP_EVICTIONS: AtomicU64 = AtomicU64::new(0);

/// Whether a registry mutation is mirrored to the node's durable store.
///
/// Every writer takes one, so each call site states its choice instead of
/// inheriting a default. `InMemoryOnly` is correct in exactly two places:
///
///  * the delegate-notification channel closing, which happens when the node is
///    shutting down, so erasing the durable rows there would erase every
///    delegate's subscriptions on every clean shutdown;
///  * executors with no durable store (mock and simulation runtimes, unit
///    tests).
#[derive(Clone, Copy)]
pub(crate) enum Durability<'a> {
    /// Apply the mutation to the durable table as well.
    Persist(&'a Storage),
    /// Mutate the in-memory registry only.
    InMemoryOnly,
}

impl<'a> Durability<'a> {
    pub(crate) fn from_store(store: Option<&'a Storage>) -> Self {
        match store {
            Some(db) => Durability::Persist(db),
            None => Durability::InMemoryOnly,
        }
    }
}

/// Serialises every PERSISTED mutation across its in-memory and durable halves.
///
/// Without it the two halves can be applied in different orders by two
/// threads: a `subscribe` on the contract loop and a `remove_contract` on an
/// executor thread can interleave so that memory ends without the pair while
/// disk ends with it, and the next boot resurrects a subscription that was
/// removed. Holding one lock across both halves makes the durable table a
/// linearisation of the in-memory one.
///
/// A plain mutex is enough: persisted mutations are rare (a NEW subscription,
/// a delegate unregistration, a contract removal) and none of them awaits
/// while holding it. It is taken OUTSIDE every DashMap guard, and nothing that
/// holds a DashMap guard in this module takes it, so it cannot invert against
/// them. `ContractStore::remove_contract` calls in holding its blob lock, which
/// is a strict outer lock (nothing here takes the blob lock).
static DURABLE_WRITE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn durable_write_guard() -> std::sync::MutexGuard<'static, ()> {
    // A poisoned lock only means a previous holder panicked; the data it
    // guards is `()`, so there is nothing to be inconsistent.
    DURABLE_WRITE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// What [`subscribe`] did.
///
/// Deliberately NOT `#[must_use]`. The obligation that would justify it —
/// releasing the evicted pair's interest hold — is discharged by [`subscribe`]
/// itself, precisely because relying on callers to notice an enum arm is what
/// this design avoids. Two of the three registration paths correctly ignore the
/// return value, so `#[must_use]` would buy nothing and cost a `let _ =` at
/// those sites, which reads as an obligation being waived when none exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SubscribeOutcome {
    /// A new (contract, delegate) pair was recorded.
    Registered,
    /// The pair was already present; the call was a no-op. Both registration
    /// paths are documented as idempotent, so this is an ordinary result and not
    /// a failure.
    AlreadySubscribed,
    /// A new pair was recorded, and this contract's subscription was dropped to
    /// make room for it because the delegate was at
    /// [`MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE`].
    ///
    /// **A caller that took a resource when it registered the subscription MUST
    /// release it for the evicted contract here.** This variant exists so the
    /// registry cannot silently drop a subscription that something else is still
    /// accounting for, and two in-flight changes make that concrete:
    ///
    ///  * #5493 registers hosting demand alongside the subscription. Demand
    ///    released nowhere would outlive the subscription that justified it —
    ///    the durable claim this cap exists to bound.
    ///  * #5615 routes the V1 path through `add_local_client`, a **refcount**
    ///    rather than the set-insert this registry used to be. A refcount
    ///    incremented per subscribe and never decremented on eviction leaks, and
    ///    a leaked count never falls back to zero.
    ///
    /// **The `delegate_interest` hold is discharged by [`subscribe`] itself**, so
    /// a caller does not have to remember to do it — ignoring this variant
    /// compiles, it is one arm of an enum, and two of the three registration
    /// paths ignore the return value entirely. Any OTHER per-pair resource a
    /// future caller takes at registration must still be released here.
    RegisteredEvicting(ContractInstanceId),
}

/// A test-only pause between the two index writes in [`subscribe`], used to make
/// the guard OBSERVABLE rather than to catch a race by luck.
///
/// The window this exists for is a handful of instructions wide, and whether it
/// opens depends on whether the thread happens to be descheduled inside it. That
/// makes an unaided concurrency test a property of machine load rather than of
/// the code: measured against a deliberately broken build it caught the defect
/// on a heavily loaded box and **0 times in 5 on a quiet one**. A test that
/// passes on an idle CI runner and fails on a busy one is indistinguishable
/// from flakiness and gets "fixed" with a retry.
///
/// Pausing at the exact point turns that into a decision rather than a race.
/// The call sits between the reverse-index write and the forward-index write,
/// so its position RELATIVE TO THE GUARD is the thing under test:
///
///  * guard held across both writes (correct): the pause happens while holding
///    it, every other subscriber for that delegate blocks on the guard, and
///    nothing can interleave. The test passes.
///  * guard dropped before the forward write (the defect): the pause happens
///    with nothing held, other subscribers proceed, fill the cap and evict this
///    very contract before its forward entry is written. The test fails, every
///    time.
///
/// So the hook is live in BOTH versions — it is not scaffolding that only means
/// something against a reverted fix.
///
/// A pause and not a barrier, deliberately: a rendezvous here deadlocks the
/// correct version, because the guard holder would wait for a task the guard is
/// blocking. Armed for ONE designated contract so a test pays a single delay
/// rather than one per subscribe.
#[cfg(test)]
pub(crate) mod race_hook {
    use super::ContractInstanceId;
    use std::sync::Mutex;

    static ARMED: Mutex<Option<ContractInstanceId>> = Mutex::new(None);

    /// Pause the next `subscribe` for `contract` inside the window.
    pub(crate) fn arm(contract: ContractInstanceId) {
        *ARMED.lock().unwrap() = Some(contract);
    }

    pub(crate) fn disarm() {
        *ARMED.lock().unwrap() = None;
    }

    pub(super) fn maybe_pause(contract: &ContractInstanceId) {
        let armed = *ARMED.lock().unwrap() == Some(*contract);
        if armed {
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    }
}

/// Record `delegate`'s interest in `contract`, enforcing the per-delegate cap.
///
/// # Why this evicts instead of refusing at the cap
///
/// `.claude/rules/code-style.md` allows refuse-at-cap only for entries that age
/// out on their own, "so a newcomer's wait is bounded". **These entries never age
/// out.** There is no TTL (below) and no unsubscribe (#5600); a subscription is
/// removed only when the delegate is unregistered, the contract is removed from
/// the store, or the notification channel closes. So refusing at the cap would
/// hand the whole budget permanently to whichever contracts a delegate happened
/// to subscribe to first, with no recovery path for any later one — the exact
/// permanent-starvation failure that rule describes, in a stronger form than the
/// refreshed-entry case it was written for (there, an idle incumbent at least
/// eventually rolls off; here it never does).
///
/// So the coldest entry is evicted instead. Eviction is contained to the
/// delegate that exceeded its own budget — one delegate can never evict
/// another's subscription — so this cannot be turned into a way to silence a
/// victim.
///
/// **The cost, stated plainly: eviction is silent to the delegate.** It has no
/// way to learn that a subscription it believes it holds has been dropped, and
/// will simply stop receiving notifications for that contract. That is the price
/// of not having refusal starve it instead, and it is why the cap is set far
/// above legitimate use rather than tightly: no well-behaved delegate should
/// ever reach it. #5600 (unsubscribe) is what would let a delegate manage its
/// own budget and make this avoidable; until then the eviction is logged at
/// `warn!` so saturation is visible in release builds, as that rule requires.
///
/// # Why there is deliberately no TTL
///
/// **Do not "fix" the missing TTL here.** An idle-expiry would silently break
/// River. Its UI fires `EnsureRoomSubscription` once per session behind a dedup
/// and retries only "on next cold load"
/// (`ui/src/components/app/freenet_api/response_handler.rs`), so a subscription
/// that expired under a long-lived tab would never be re-established, and the
/// delegate's private-room secret rotation — which is driven entirely by these
/// notifications — would stop for that room with no error anywhere. A room quiet
/// for longer than any plausible TTL is precisely the case where the next
/// membership change most needs to be seen.
///
/// The bound that AGENTS.md asks for is supplied by the cap instead: the map
/// cannot grow without limit, and the entries it holds are removed by
/// [`remove_delegate`] and [`remove_contract`].
///
/// # Durability
///
/// With [`Durability::Persist`], a NEW pair is written to the durable table and
/// a cap-evicted pair is removed from it, in one transaction. A repeat
/// subscribe ([`SubscribeOutcome::AlreadySubscribed`]) writes nothing: this
/// runs synchronously on the contract-handling loop, and a delegate
/// re-subscribing in a loop must not cost a write transaction per repeat.
pub(crate) fn subscribe(
    contract: ContractInstanceId,
    delegate: &DelegateKey,
    durability: Durability<'_>,
) -> SubscribeOutcome {
    let Durability::Persist(db) = durability else {
        return subscribe_in_memory(contract, delegate);
    };
    let _guard = durable_write_guard();
    let outcome = subscribe_in_memory(contract, delegate);
    let evicted = match &outcome {
        SubscribeOutcome::AlreadySubscribed => return outcome,
        SubscribeOutcome::Registered => None,
        SubscribeOutcome::RegisteredEvicting(id) => Some(*id),
    };
    durable::record(db, &contract, delegate, evicted.as_ref());
    outcome
}

/// The in-memory admission: cap check, coldest-eviction and both index writes.
/// Shared by [`subscribe`] and [`restore_from_storage`], so the cap is applied
/// identically on add and on restore.
fn subscribe_in_memory(contract: ContractInstanceId, delegate: &DelegateKey) -> SubscribeOutcome {
    let now = tokio::time::Instant::now();

    // Take the reverse index first and hold it across the forward-map write.
    // The two maps are only ever locked in this order, here and in every
    // removal below, so they cannot deadlock against each other.
    let mut owned = BY_DELEGATE.entry(delegate.clone()).or_default();

    if owned.contains_key(&contract) {
        // Idempotent re-subscribe. Deliberately does NOT restamp: the stamp
        // means "last notification delivered", and letting a delegate refresh it
        // by re-subscribing in a loop would let it pin an entry it never hears
        // from, defeating the eviction order.
        //
        // It DOES re-assert the forward-map half, which is not redundant. This
        // function dedups on the reverse index while `is_subscribed` answers
        // from the forward map, and while ONE map existed those were the same
        // question by construction. They are no longer, so a state where the
        // two disagree is now representable, and without this line it would be
        // PERMANENT: the early return above happens before the forward-map
        // insert below, so no later subscribe would ever repair it, and
        // `remove_contract` early-returns on a missing forward entry so no
        // removal would clean it either. The subscription would be dead —
        // silently receiving nothing — while still holding a unit of cap budget
        // that nothing ages out. A `HashSet` insert is idempotent and the
        // reverse index is untouched here, so this cannot admit past the cap or
        // double-count; it only makes every subscribe self-healing.
        BY_CONTRACT
            .entry(contract)
            .or_default()
            .insert(delegate.clone());
        return SubscribeOutcome::AlreadySubscribed;
    }

    let mut evicted = None;
    if owned.len() >= MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE {
        // Evict the coldest entry. One, not a batch: the rule's batching advice
        // is aimed at a full scan per admission "on the receive path", and this
        // is not that — subscribing happens inside delegate WASM execution,
        // which already costs milliseconds, and the scan is bounded by the cap.
        // Evicting more than strictly necessary would also discard legitimate
        // standing interest that nothing will re-establish.
        //
        // The scan runs UNDER the reverse-index write guard, which the same
        // rule warns about and which the paragraph above does not answer. It is
        // deliberate and it is what makes the cap exact: check, evict and
        // insert have to be one atomic step, or two concurrent subscribes for
        // the same delegate both observe `len() == cap - 1` and both insert.
        // The cost is bounded and small — at most
        // `MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE` (256) `Instant` comparisons
        // over an in-memory map, holding ONE DashMap shard, contended only by
        // other operations on delegates hashing to that same shard. Releasing
        // the guard to scan would trade an exact bound for a lock-free one, and
        // an off-by-a-few cap is not worth a re-entrancy hazard here.
        let coldest = owned
            .iter()
            .min_by_key(|(_, stamp)| **stamp)
            .map(|(id, _)| *id);
        if let Some(coldest) = coldest {
            // Through the shared writer, not inline: eviction is a removal that
            // does not look like one. Its durable half is discharged by
            // `subscribe` from the returned `RegisteredEvicting`, in the same
            // transaction that records the new pair. See `forget_one`.
            forget_one(Some(owned.value_mut()), &coldest, delegate);
            let total = CAP_EVICTIONS.fetch_add(1, Ordering::Relaxed) + 1;
            // Throttled, and the throttle is the point rather than tidiness. A
            // delegate parked at its cap evicts on EVERY subscribe, so an
            // unthrottled line here is an amplifier: the cheapest possible
            // remote action produces one log write each, which is the shape a
            // refusal path must never have. The first eviction is always
            // reported so a node that sheds once is not silent, and the running
            // total on every line it does emit carries the rate, so nothing is
            // lost by dropping the ones in between.
            if total == 1 || total % 64 == 0 {
                tracing::warn!(
                    %delegate,
                    evicted_contract = %coldest,
                    new_contract = %contract,
                    cap = MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE,
                    cap_evictions_total = total,
                    "Delegate at its contract-subscription cap; evicted its coldest \
                     subscription to admit a new one. The delegate is NOT told, and \
                     will stop receiving notifications for the evicted contract."
                );
            }
            evicted = Some(coldest);
        }
    }

    owned.insert(contract, now);
    // Between the two writes, so a test can prove the guard actually spans them.
    // A no-op unless armed; see `race_hook`.
    #[cfg(test)]
    race_hook::maybe_pause(&contract);
    // Under the SAME guard as the reverse-index insert above, not after it.
    //
    // Dropping the guard first leaves a window in which another task
    // subscribing for this delegate can reach the cap, pick this very contract
    // as its coldest victim, and call `drop_from_contract_map` for a forward
    // entry that has not been written yet — a no-op — before this task writes
    // it. The reverse index would then have lost the contract while the forward
    // index gained it: the split-brain state again, in a third direction, and
    // one that leaves the delegate on the delivery path for a subscription it
    // is not counted as holding. This is not hypothetical; the concurrency test
    // below caught it on its first run.
    BY_CONTRACT
        .entry(contract)
        .or_default()
        .insert(delegate.clone());
    drop(owned);

    match evicted {
        Some(id) => {
            // Give back the local interest the evicted subscription took, now
            // that every DashMap guard above is released — a release closure
            // reaches into `InterestManager`, which takes its own locks.
            //
            // Done HERE rather than left to the caller. The obligation is real
            // for one of the three registration paths (the network-resolved
            // one, which is the only path that takes an `add_local_client`
            // refcount), but a caller that ignores this outcome compiles fine,
            // and two of the three do exactly that. The leak it would cause is
            // invisible in testing: the symptom is a refcount that never
            // returns to zero, so `cleanup_contract_if_no_interest` silently
            // never fires. A no-op for a pair holding nothing, which is most of
            // them.
            crate::wasm_runtime::delegate_interest::release_pair(&id, delegate);
            SubscribeOutcome::RegisteredEvicting(id)
        }
        None => SubscribeOutcome::Registered,
    }
}

/// Snapshot the delegates subscribed to `contract`.
///
/// Returns an owned `Vec` so the caller does not hold a shard lock while
/// sending on a channel.
pub(crate) fn subscribers_of(contract: &ContractInstanceId) -> Vec<DelegateKey> {
    BY_CONTRACT
        .get(contract)
        .map(|s| s.iter().cloned().collect())
        .unwrap_or_default()
}

/// Record that a notification for `contract` was delivered to `delegate`.
///
/// This is the "ordinary use" that orders eviction: a subscription that is
/// actually producing notifications stays warm, and the one evicted under
/// pressure is whichever the delegate has heard about least recently. Cheap
/// enough for the commit path — one map write per subscribed delegate, on a path
/// that is already cloning and sending the new state.
pub(crate) fn note_notified(contract: &ContractInstanceId, delegate: &DelegateKey) {
    if let Some(mut owned) = BY_DELEGATE.get_mut(delegate) {
        if let Some(stamp) = owned.get_mut(contract) {
            *stamp = tokio::time::Instant::now();
        }
    }
}

/// Drop every subscription held by `delegate` (delegate unregistered).
///
/// With [`Durability::Persist`] its durable rows go too. That matters beyond
/// tidiness: a row left behind would be restored at the next boot, for a
/// delegate nothing will ever unregister again. (Restore also drops rows whose
/// delegate is no longer registered, so this is the primary path and that the
/// backstop.)
pub(crate) fn remove_delegate(delegate: &DelegateKey, durability: Durability<'_>) {
    let Durability::Persist(db) = durability else {
        remove_delegate_in_memory(delegate);
        return;
    };
    let _guard = durable_write_guard();
    remove_delegate_in_memory(delegate);
    durable::remove_delegate(db, delegate);
}

fn remove_delegate_in_memory(delegate: &DelegateKey) {
    // Hold the reverse-index guard across the forward-map cleanup, rather than
    // removing the entry and then walking a detached snapshot.
    //
    // Executors are a pool, so `UnregisterDelegate` does not run on the same
    // thread as `apply_resolved_contract_op`. A `subscribe`
    // for this delegate completing inside a snapshot walk would recreate the
    // reverse-index entry and insert into the forward map, and the walk would
    // then strip the forward half back out — leaving exactly the disagreement
    // the re-subscribe path above has to heal. Holding the guard makes such a
    // `subscribe` wait instead, so the window does not exist in this direction
    // at all. Same lock order as `subscribe` (reverse index, then forward), so
    // it cannot deadlock against it.
    let Some(mut owned) = BY_DELEGATE.get_mut(delegate) else {
        return;
    };
    let contracts: Vec<ContractInstanceId> = owned.keys().copied().collect();
    owned.clear();
    for contract in &contracts {
        drop_from_contract_map(contract, delegate);
    }
    drop(owned);
    // Re-checked under the removal guard: a `subscribe` that was waiting on the
    // guard above may have refilled the entry, and removing a non-empty one
    // would silently unsubscribe it.
    BY_DELEGATE.remove_if(delegate, |_, owned| owned.is_empty());
}

/// Drop every subscription to `contract` (contract removed, or its notification
/// channel closed).
///
/// Contract removal passes [`Durability::Persist`]: the contract is gone from
/// this node, and a surviving row would re-subscribe to it (and re-fetch it
/// from the network) at every boot. The notification-channel-closed path
/// passes [`Durability::InMemoryOnly`], because that channel closes on node
/// shutdown, and the subscriptions must survive exactly that.
pub(crate) fn remove_contract(contract: &ContractInstanceId, durability: Durability<'_>) {
    let Durability::Persist(db) = durability else {
        remove_contract_in_memory(contract);
        return;
    };
    let _guard = durable_write_guard();
    remove_contract_in_memory(contract);
    durable::remove_contract(db, contract);
}

fn remove_contract_in_memory(contract: &ContractInstanceId) {
    let Some((_, delegates)) = BY_CONTRACT.remove(contract) else {
        return;
    };
    for delegate in &delegates {
        let now_empty = match BY_DELEGATE.get_mut(delegate) {
            Some(mut owned) => {
                owned.remove(contract);
                owned.is_empty()
            }
            None => false,
        };
        // Only the delegates this contract actually touched, and each re-checked
        // under the removal guard. A blanket `BY_DELEGATE.retain(..)` here would
        // scan every delegate on the node on every contract removal, and would
        // race a concurrent `subscribe` that had just refilled an entry.
        if now_empty {
            BY_DELEGATE.remove_if(delegate, |_, owned| owned.is_empty());
        }
    }
}

/// The ONE place a single `(contract, delegate)` subscription is dropped, and
/// therefore the only place anything that must happen when a subscription goes
/// away belongs.
///
/// Takes the caller's ALREADY-HELD reverse-index guard as `owned` rather than
/// acquiring its own. That is forced, not stylistic: the cap is exact only
/// because `subscribe` holds that guard across the whole check-evict-insert
/// sequence, so a writer that re-acquired it would deadlock against its own
/// caller. `None` is for callers that hold no guard because the delegate has no
/// reverse entry at all, which is reachable — the forward index can still list
/// the pair after a `remove_contract` divergence, and clearing that is still a
/// drop.
///
/// **Both removal callers go through here on purpose.** Cap eviction and
/// `unsubscribe` drop exactly one pair, and eviction is the one that gets
/// written separately and forgotten, because it does not look like a removal
/// path — it looks like part of admission. Its durable half (#5493) is
/// discharged by this function's callers (`subscribe` from
/// `RegisteredEvicting`, `unsubscribe` directly), which hold the durable write
/// lock this function cannot take without re-entering. An eviction that cleared
/// the in-memory indexes and left a durable row would have the row replayed at
/// the next boot; [`restore_from_storage`] re-applies the cap, so that would not
/// leave the delegate over it, but it would let an old subscription displace a
/// newer one.
///
/// Returns whether anything was actually held, so a caller can tell a real drop
/// from a no-op.
fn forget_one(
    owned: Option<&mut HashMap<ContractInstanceId, tokio::time::Instant>>,
    contract: &ContractInstanceId,
    delegate: &DelegateKey,
) -> bool {
    let removed = owned.is_some_and(|o| o.remove(contract).is_some());
    // Checked independently of the reverse half: the two can disagree, and a
    // drop that honoured only one would leave the other behind.
    let listed = is_subscribed(contract, delegate);
    if removed || listed {
        drop_from_contract_map(contract, delegate);
    }
    removed || listed
}

/// Remove `delegate` from `contract`'s subscriber set, dropping the contract's
/// entry entirely once nothing is subscribed to it.
///
/// Forward-map half only — the caller owns the reverse-index half. Private so
/// the two halves cannot be applied separately from outside.
fn drop_from_contract_map(contract: &ContractInstanceId, delegate: &DelegateKey) {
    let now_empty = match BY_CONTRACT.get_mut(contract) {
        Some(mut delegates) => {
            delegates.remove(delegate);
            delegates.is_empty()
        }
        None => false,
    };
    if now_empty {
        // Re-check under the removal guard: another delegate may have subscribed
        // between the two locks, and removing a non-empty entry would silently
        // unsubscribe it.
        BY_CONTRACT.remove_if(contract, |_, delegates| delegates.is_empty());
    }
}

/// Drop exactly one (contract, delegate) subscription, leaving every other
/// subscriber of `contract` and every other subscription of `delegate` alone.
///
/// Currently reachable only from tests, which use it to clean up a registration
/// without disturbing a concurrent test's entry in this process-global map. It
/// is deliberately written as the general operation rather than a test helper,
/// because it is also the operation #5600 needs: a delegate that could
/// unsubscribe would be able to manage its own budget under
/// [`MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE`] instead of relying on eviction.
/// Promote it out of `cfg(test)` when that lands.
#[cfg(test)]
pub(crate) fn unsubscribe(
    contract: &ContractInstanceId,
    delegate: &DelegateKey,
    durability: Durability<'_>,
) -> bool {
    let Durability::Persist(db) = durability else {
        return unsubscribe_in_memory(contract, delegate);
    };
    let _guard = durable_write_guard();
    let dropped = unsubscribe_in_memory(contract, delegate);
    // Unconditionally, not only when `dropped`: a durable row with no memory
    // half (e.g. one not yet restored) is still this pair's to clear.
    durable::remove_one(db, contract, delegate);
    dropped
}

#[cfg(test)]
fn unsubscribe_in_memory(contract: &ContractInstanceId, delegate: &DelegateKey) -> bool {
    // Through the same writer cap eviction uses, so the two cannot drift: they
    // are the only two paths that drop a single pair. Returns whether anything
    // was held, which is what #5600 needs to answer "not subscribed" rather than
    // reporting a silent success.
    let dropped = match BY_DELEGATE.get_mut(delegate) {
        Some(mut owned) => {
            let dropped = forget_one(Some(owned.value_mut()), contract, delegate);
            drop(owned);
            BY_DELEGATE.remove_if(delegate, |_, owned| owned.is_empty());
            dropped
        }
        // No reverse entry: the forward index may still list the pair after a
        // `remove_contract` divergence, and clearing that is still a drop.
        None => forget_one(None, contract, delegate),
    };
    if dropped {
        // Same obligation as the eviction branch of `subscribe`: this drops one
        // pair, so one pair's interest hold has to come back.
        //
        // Wired now even though this function is `cfg(test)` and its callers
        // record no holds, so it discharges nothing today. When #5600 promotes
        // it to the real unsubscribe path the pairs WILL hold refcounts, and
        // the diff that promotes it is an attribute change — a reviewer reading
        // that diff sees a `#[cfg(test)]` come off and no reason to re-audit a
        // function that already existed. Closing it here costs a line; closing
        // it later means finding it as a production leak.
        crate::wasm_runtime::delegate_interest::release_pair(contract, delegate);
    }
    dropped
}

/// Cap evictions since process start. See [`CAP_EVICTIONS`].
#[cfg(test)]
pub(crate) fn cap_evictions_total() -> u64 {
    CAP_EVICTIONS.load(Ordering::Relaxed)
}

/// Number of contracts `delegate` is currently subscribed to.
#[cfg(test)]
pub(crate) fn subscription_count(delegate: &DelegateKey) -> usize {
    BY_DELEGATE.get(delegate).map_or(0, |owned| owned.len())
}

/// When a notification for `contract` was last delivered to `delegate`.
///
/// Exists so a test can pin the PRODUCTION call to [`note_notified`] rather than
/// only the helper. The helper being correct says nothing about it being wired
/// up, and the wiring is the half that rots: delete the call in
/// `send_delegate_contract_notifications` and every stamp stays at its
/// registration time, so eviction starts targeting the delegate's BUSIEST
/// subscription instead of its coldest, silently and with the suite still green.
#[cfg(test)]
pub(crate) fn last_notified(
    contract: &ContractInstanceId,
    delegate: &DelegateKey,
) -> Option<tokio::time::Instant> {
    BY_DELEGATE
        .get(delegate)
        .and_then(|owned| owned.get(contract).copied())
}

/// Whether `delegate` is subscribed to `contract`.
///
/// Production use is the V1 SUBSCRIBE arm's pre-branch guard: it asks this
/// BEFORE deciding whether to reach the network, because
/// `InterestManager::add_local_client` is a refcount and a repeat subscribe
/// that fell through to the network path would take a second one for a single
/// logical subscriber. That is a different question from
/// [`SubscribeOutcome::AlreadySubscribed`], which reports what an admission
/// that already happened did, so the two are not interchangeable.
pub(crate) fn is_subscribed(contract: &ContractInstanceId, delegate: &DelegateKey) -> bool {
    BY_CONTRACT
        .get(contract)
        .is_some_and(|delegates| delegates.contains(delegate))
}

/// Replay the persisted subscriptions into the in-memory registry at startup,
/// and return the pairs that were restored, for the caller to re-establish the
/// interest and network subscription a live subscribe takes
/// (`contract::delegate_restore`).
///
/// Runs once, from `NetworkContractHandler::build`, before the node's event
/// loop exists, so no delegate can run (and no subscribe can race it) while it
/// does.
///
/// # What is dropped, and why
///
/// | Row | Outcome |
/// |---|---|
/// | malformed (bad length or format byte) | deleted from disk, skipped |
/// | delegate no longer registered | deleted from disk, skipped: nothing is left to unregister it, so it would be replayed at every boot |
/// | beyond [`MAX_DURABLE_DELEGATE_SUBSCRIPTIONS`] | deleted from disk, skipped |
/// | delegate over its per-delegate cap | admitted through [`subscribe_in_memory`], which evicts exactly as a live subscribe would; the evicted row is deleted from disk |
///
/// A contract that is not in the local store is deliberately KEPT: a live
/// subscribe for a contract this node has never seen is the primary use case
/// (the node fetches it from the network), and re-establishing it is the same
/// operation.
///
/// # Failure is never fatal
///
/// A table that cannot be read logs at ERROR and restores nothing, and deletes
/// nothing: treating a transient read error as "no subscriptions" would erase
/// every delegate's subscriptions from disk. A delegate-index lookup that fails
/// keeps the row (it is only dropped on a definite "not registered").
///
/// [`MAX_DURABLE_DELEGATE_SUBSCRIPTIONS`]: crate::contract::storages::redb::MAX_DURABLE_DELEGATE_SUBSCRIPTIONS
pub(crate) fn restore_from_storage(db: &Storage) -> Vec<(ContractInstanceId, DelegateKey)> {
    let _guard = durable_write_guard();
    durable::restore(db)
}

/// The durable half's I/O, kept in one place so the registry logic above does
/// not carry backend `cfg`s. Every function logs and continues on error: a
/// durability failure must never take down the in-memory subscription, which
/// is still correct for the life of the process.
#[cfg(feature = "redb")]
mod durable {
    use super::*;
    use crate::contract::storages::redb::MAX_DURABLE_DELEGATE_SUBSCRIPTIONS;

    /// Warnings on the write path are throttled like the cap-eviction warning,
    /// for the same reason: a delegate subscribing in a loop at the node-wide
    /// ceiling would otherwise produce one log write per subscribe.
    static NOT_PERSISTED: AtomicU64 = AtomicU64::new(0);

    pub(super) fn record(
        db: &Storage,
        contract: &ContractInstanceId,
        delegate: &DelegateKey,
        evicted: Option<&ContractInstanceId>,
    ) {
        match db.record_delegate_subscription(
            contract,
            delegate,
            evicted,
            MAX_DURABLE_DELEGATE_SUBSCRIPTIONS,
        ) {
            Ok(true) => {}
            Ok(false) => {
                let total = NOT_PERSISTED.fetch_add(1, Ordering::Relaxed) + 1;
                if total == 1 || total % 64 == 0 {
                    tracing::warn!(
                        %delegate,
                        %contract,
                        ceiling = MAX_DURABLE_DELEGATE_SUBSCRIPTIONS,
                        not_persisted_total = total,
                        "Durable delegate-subscription table is at its node-wide ceiling; \
                         this subscription works now but will NOT survive a restart"
                    );
                }
            }
            Err(e) => tracing::warn!(
                %delegate,
                %contract,
                error = %e,
                "Failed to persist a delegate subscription; it works now but will \
                 not survive a restart"
            ),
        }
    }

    #[cfg(test)]
    pub(super) fn remove_one(db: &Storage, contract: &ContractInstanceId, delegate: &DelegateKey) {
        if let Err(e) = db.remove_delegate_subscription(contract, delegate) {
            tracing::warn!(%delegate, %contract, error = %e,
                "Failed to remove a persisted delegate subscription");
        }
    }

    pub(super) fn remove_delegate(db: &Storage, delegate: &DelegateKey) {
        if let Err(e) = db.remove_delegate_subscriptions_for_delegate(delegate) {
            tracing::warn!(%delegate, error = %e,
                "Failed to remove a delegate's persisted subscriptions; restore will \
                 drop them at the next boot if the delegate is no longer registered");
        }
    }

    pub(super) fn remove_contract(db: &Storage, contract: &ContractInstanceId) {
        if let Err(e) = db.remove_delegate_subscriptions_for_contract(contract) {
            tracing::warn!(%contract, error = %e,
                "Failed to remove a contract's persisted delegate subscriptions");
        }
    }

    pub(super) fn restore(db: &Storage) -> Vec<(ContractInstanceId, DelegateKey)> {
        let loaded =
            match db.load_delegate_subscriptions(MAX_DURABLE_DELEGATE_SUBSCRIPTIONS as usize) {
                Ok(loaded) => loaded,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "Could not read persisted delegate subscriptions; restoring none \
                         this boot (the persisted set is left untouched)"
                    );
                    return Vec::new();
                }
            };

        let mut to_delete = loaded.malformed;
        let malformed = to_delete.len();
        let excess = loaded.excess.len();
        to_delete.extend(loaded.excess);

        let mut restored = Vec::new();
        let mut unregistered = 0usize;
        let mut cap_evicted = 0usize;
        for (contract, delegate) in loaded.valid {
            match db.get_delegate_index(&delegate) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    unregistered += 1;
                    to_delete.push(row_key(&contract, &delegate));
                    continue;
                }
                Err(e) => {
                    // Unknown is not "unregistered": keep the row and the
                    // subscription rather than erase it on a read error.
                    tracing::warn!(%delegate, error = %e,
                        "Could not check whether a delegate is still registered; \
                         restoring its subscription anyway");
                }
            }
            // The SAME admission a live subscribe uses, so the per-delegate cap
            // holds on restore exactly as on add.
            match subscribe_in_memory(contract, &delegate) {
                SubscribeOutcome::Registered | SubscribeOutcome::AlreadySubscribed => {}
                SubscribeOutcome::RegisteredEvicting(evicted) => {
                    cap_evicted += 1;
                    to_delete.push(row_key(&evicted, &delegate));
                    restored.retain(|(c, d): &(ContractInstanceId, DelegateKey)| {
                        !(c == &evicted && d == &delegate)
                    });
                }
            }
            restored.push((contract, delegate));
        }

        if let Err(e) = db.remove_delegate_subscription_rows(&to_delete) {
            tracing::warn!(error = %e, rows = to_delete.len(),
                "Failed to delete stale persisted delegate subscriptions; they will be \
                 re-examined at the next boot");
        }

        if !restored.is_empty() || !to_delete.is_empty() {
            tracing::info!(
                restored = restored.len(),
                dropped_unregistered_delegate = unregistered,
                dropped_malformed = malformed,
                dropped_over_ceiling = excess,
                dropped_over_delegate_cap = cap_evicted,
                "Restored persisted delegate subscriptions"
            );
        }
        restored
    }

    fn row_key(contract: &ContractInstanceId, delegate: &DelegateKey) -> Vec<u8> {
        let mut key = Vec::with_capacity(96);
        key.extend_from_slice(contract.as_ref());
        key.extend_from_slice(delegate.as_ref());
        key.extend_from_slice(delegate.code_hash().as_ref());
        key
    }
}

/// No durable backend: every durable operation is a no-op, so the registry
/// degrades to in-memory only (today's behaviour) rather than claiming a
/// durability it does not have.
#[cfg(not(feature = "redb"))]
mod durable {
    use super::*;

    pub(super) fn record(
        _: &Storage,
        _: &ContractInstanceId,
        _: &DelegateKey,
        _: Option<&ContractInstanceId>,
    ) {
    }
    #[cfg(test)]
    pub(super) fn remove_one(_: &Storage, _: &ContractInstanceId, _: &DelegateKey) {}
    pub(super) fn remove_delegate(_: &Storage, _: &DelegateKey) {}
    pub(super) fn remove_contract(_: &Storage, _: &ContractInstanceId) {}
    pub(super) fn restore(_: &Storage) -> Vec<(ContractInstanceId, DelegateKey)> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::prelude::CodeHash;

    fn dkey(seed: u8) -> DelegateKey {
        let mut bytes = [seed; 32];
        bytes[31] = TEST_ID_NAMESPACE;
        DelegateKey::new(bytes, CodeHash::new([seed; 32]))
    }

    /// Namespace marker in the last id byte, keeping this module's contract ids
    /// away from every other test in the process: the registry is a
    /// process-global and CI runs tests in threads, so colliding ids would make
    /// these tests see each other's entries (the hazard #4824 records for this
    /// same map). The delegate keys are namespaced by construction — `dkey`
    /// seeds all 32 bytes — so only the contract ids need this.
    const TEST_ID_NAMESPACE: u8 = 0xD5;

    fn cid(seed: u16) -> ContractInstanceId {
        let mut bytes = [0u8; 32];
        bytes[0..2].copy_from_slice(&seed.to_le_bytes());
        bytes[31] = TEST_ID_NAMESPACE;
        ContractInstanceId::new(bytes)
    }

    /// Every test cleans up after itself, for the same reason `cid` namespaces:
    /// the registry outlives any one test.
    fn cleanup(delegate: &DelegateKey) {
        remove_delegate(delegate, Durability::InMemoryOnly);
    }

    #[tokio::test]
    async fn subscribe_is_idempotent() {
        let d = dkey(1);
        let c = cid(1);
        assert_eq!(
            subscribe(c, &d, Durability::InMemoryOnly),
            SubscribeOutcome::Registered
        );
        assert_eq!(
            subscribe(c, &d, Durability::InMemoryOnly),
            SubscribeOutcome::AlreadySubscribed
        );
        assert_eq!(subscription_count(&d), 1);
        assert_eq!(subscribers_of(&c), vec![d.clone()]);
        cleanup(&d);
    }

    /// The bound itself: a delegate cannot accumulate subscriptions without
    /// limit. Without the cap this reaches `cap + 50`.
    #[tokio::test]
    async fn a_delegate_cannot_exceed_its_subscription_cap() {
        let d = dkey(2);
        for i in 0..(MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE + 50) {
            subscribe(cid(1000 + i as u16), &d, Durability::InMemoryOnly);
        }
        assert_eq!(
            subscription_count(&d),
            MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE,
            "a delegate must not hold more than its cap"
        );
        cleanup(&d);
    }

    /// Admission past the cap must EVICT, not refuse — a refusal would starve
    /// every later subscription permanently, since these entries never age out.
    ///
    /// Time is paused and advanced explicitly between subscribes. Without that
    /// the stamps can tie, and `min_by_key` over a `HashMap` breaks ties in
    /// iteration order, which is not deterministic — the identity assertion
    /// below would then pass or fail by luck.
    #[tokio::test(start_paused = true)]
    async fn subscribing_at_the_cap_admits_the_newcomer_and_evicts() {
        let d = dkey(3);
        let first = cid(2000);
        subscribe(first, &d, Durability::InMemoryOnly);
        for i in 1..MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE {
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
            subscribe(cid(2000 + i as u16), &d, Durability::InMemoryOnly);
        }
        tokio::time::advance(std::time::Duration::from_millis(1)).await;

        let newcomer = cid(9000);
        // The counter is process-global and other tests in this binary evict
        // too, so assert it ADVANCED rather than asserting an absolute value.
        let evictions_before = cap_evictions_total();
        let outcome = subscribe(newcomer, &d, Durability::InMemoryOnly);
        assert!(
            cap_evictions_total() > evictions_before,
            "an eviction must be counted, or an operator sees only isolated log \
             lines and cannot tell steady shedding from a one-off"
        );
        assert_eq!(
            outcome,
            SubscribeOutcome::RegisteredEvicting(first),
            "the coldest (never-notified, oldest) subscription must be the victim"
        );
        assert!(
            is_subscribed(&newcomer, &d),
            "the newcomer must be admitted, not refused"
        );
        assert!(
            !is_subscribed(&first, &d),
            "the evicted subscription must be gone from BOTH indexes"
        );
        cleanup(&d);
    }

    /// The eviction victim is the least recently NOTIFIED, not the oldest —
    /// otherwise a delegate's busiest subscription would be evicted first.
    #[tokio::test(start_paused = true)]
    async fn eviction_picks_the_least_recently_notified() {
        let d = dkey(4);
        let oldest = cid(3000);
        subscribe(oldest, &d, Durability::InMemoryOnly);
        for i in 1..MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE {
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
            subscribe(cid(3000 + i as u16), &d, Durability::InMemoryOnly);
        }

        // The oldest subscription is the busy one: it has just been notified,
        // while the second-oldest has never been.
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        note_notified(&oldest, &d);
        let expected_victim = cid(3001);

        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        assert_eq!(
            subscribe(cid(9001), &d, Durability::InMemoryOnly),
            SubscribeOutcome::RegisteredEvicting(expected_victim),
            "a subscription that is actively delivering notifications must not \
             be the eviction victim merely for being old"
        );
        assert!(
            is_subscribed(&oldest, &d),
            "the busy subscription must survive"
        );
        cleanup(&d);
    }

    /// A re-subscribe must not count as use. Otherwise a delegate could pin an
    /// entry it never hears from by re-subscribing in a loop.
    #[tokio::test(start_paused = true)]
    async fn resubscribing_does_not_refresh_the_eviction_stamp() {
        let d = dkey(5);
        let a = cid(4000);
        let b = cid(4001);
        subscribe(a, &d, Durability::InMemoryOnly);
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        subscribe(b, &d, Durability::InMemoryOnly);

        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        subscribe(a, &d, Durability::InMemoryOnly); // idempotent re-subscribe, must not restamp

        for i in 2..MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE {
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
            subscribe(cid(4000 + i as u16), &d, Durability::InMemoryOnly);
        }
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        assert_eq!(
            subscribe(cid(9002), &d, Durability::InMemoryOnly),
            SubscribeOutcome::RegisteredEvicting(a),
            "re-subscribing must not refresh a subscription's eviction stamp"
        );
        cleanup(&d);
    }

    /// One delegate hitting its cap must never evict another delegate's
    /// subscription — otherwise the cap becomes a way to silence a victim.
    #[tokio::test]
    async fn eviction_never_crosses_delegates() {
        let victim = dkey(6);
        let hog = dkey(7);
        let shared = cid(5000);
        subscribe(shared, &victim, Durability::InMemoryOnly);

        subscribe(shared, &hog, Durability::InMemoryOnly);
        for i in 1..(MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE + 20) {
            subscribe(cid(5000 + i as u16), &hog, Durability::InMemoryOnly);
        }

        assert!(
            is_subscribed(&shared, &victim),
            "a delegate at its cap must not evict a DIFFERENT delegate's \
             subscription to the same contract"
        );
        assert_eq!(subscription_count(&victim), 1);
        cleanup(&victim);
        cleanup(&hog);
    }

    /// The cap must hold under genuine concurrent contention, which is the one
    /// thing every other test in this file cannot show.
    ///
    /// `subscribe` holds the `BY_DELEGATE` entry write guard across the whole
    /// check-evict-insert sequence, and the comment at that site names the race
    /// it exists to prevent: two concurrent subscribes for the same delegate
    /// both observing `len() == cap - 1` and both inserting. Every other test
    /// here calls `subscribe` sequentially from one task, so **the guard could
    /// be deleted and all of them would still pass** — the mechanism the
    /// implementation was built to survive would be the one nothing drives.
    ///
    /// Multi-threaded on purpose: a current-thread runtime interleaves at await
    /// points, and `subscribe` is synchronous, so on one worker the sequence is
    /// atomic for free and the test would prove nothing. The barrier makes the
    /// tasks start together rather than trickling in.
    ///
    /// Deliberately over-subscribes by 2x the cap so eviction, which is the part
    /// that reads and mutates under the same guard, is contended rather than
    /// incidental.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_subscribes_never_exceed_the_cap() {
        use std::sync::Arc;

        const FILLERS: u16 = 8;
        const PER_FILLER: u16 = 40;
        const BASE: u16 = 20000;
        const VICTIM: u16 = 19999;

        // DETERMINISTIC, not probabilistic, and the difference is the whole
        // value of this test. Racing `subscribe` unaided caught the real
        // admission defect 0 times in 5 against a deliberately broken build on
        // an idle machine, and once on a loaded one — detection was a property
        // of machine load, not of the code, which is the kind of test that goes
        // green on a quiet CI runner and gets a retry bolted on when it is not.
        //
        // `race_hook` pauses ONE subscribe between the reverse-index write and
        // the forward-index write. That makes the guard's SPAN observable: with
        // both writes under one guard the fillers below block and cannot
        // interleave, and with the forward write outside it they proceed, fill
        // the cap, and evict the victim before its forward entry exists.
        let d = dkey(16);
        let victim = cid(VICTIM);
        race_hook::arm(victim);

        let start = Arc::new(tokio::sync::Barrier::new(2));
        let victim_task = {
            let delegate = d.clone();
            let start = Arc::clone(&start);
            tokio::spawn(async move {
                start.wait().await;
                tokio::task::spawn_blocking(move || {
                    subscribe(victim, &delegate, Durability::InMemoryOnly)
                })
                .await
                .expect("victim subscribe must not panic")
            })
        };

        // Enough subscribes to fill the cap and then keep evicting, so the
        // victim ages into being the coldest entry while it is paused.
        let fillers = {
            let delegate = d.clone();
            let start = Arc::clone(&start);
            tokio::spawn(async move {
                start.wait().await;
                let mut handles = Vec::new();
                for t in 0..FILLERS {
                    let delegate = delegate.clone();
                    handles.push(tokio::task::spawn_blocking(move || {
                        for i in 0..PER_FILLER {
                            subscribe(
                                cid(BASE + t * PER_FILLER + i),
                                &delegate,
                                Durability::InMemoryOnly,
                            );
                        }
                    }));
                }
                for h in handles {
                    h.await.expect("no filler task may panic");
                }
            })
        };

        victim_task.await.expect("victim task joined");
        fillers.await.expect("filler task joined");
        race_hook::disarm();

        let held = subscription_count(&d);
        assert!(
            held <= MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE,
            "the cap must hold under concurrency: {held} held against a cap of \
             {MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE}"
        );

        // The assertion that matters. A pair present in the forward index but
        // absent from the reverse one leaves the delegate on the delivery path
        // for a subscription that occupies none of its budget and that cap
        // eviction can never choose as a victim, because eviction picks from the
        // index that lost it.
        assert!(
            !(is_subscribed(&victim, &d) && last_notified(&victim, &d).is_none()),
            "the victim is listed in the forward index but absent from the \
             reverse one: the two index writes did not happen under one guard, \
             so a concurrent subscriber evicted it in the window between them"
        );
        let mut listed = 0;
        for t in 0..FILLERS {
            for i in 0..PER_FILLER {
                if is_subscribed(&cid(BASE + t * PER_FILLER + i), &d) {
                    listed += 1;
                }
            }
        }
        if is_subscribed(&victim, &d) {
            listed += 1;
        }
        assert_eq!(
            listed, held,
            "every contract the forward index lists must also be counted in the \
             reverse index"
        );

        cleanup(&d);
    }

    /// Cap eviction must discharge EVERY node's hold under the evicted pair,
    /// not just the first.
    ///
    /// #5615 keys the hold map by `(contract, delegate)` with node identity in
    /// the VALUE, because two nodes in an in-process run each take their own
    /// refcount on their own `InterestManager`. So `release_pair` has to drain
    /// the whole per-node map. Discharging one and dropping the entry leaves the
    /// other node's interest standing with nothing left to release it, which is
    /// the leak `delegate_interest` exists to close, reintroduced through
    /// eviction.
    ///
    /// This fails against a `release_pair` that takes the first hold and
    /// returns, which is the shape the single-`Hold` value type before #5615's
    /// M3 fix would naturally have produced.
    #[tokio::test(start_paused = true)]
    async fn cap_eviction_discharges_every_node_holding_the_evicted_pair() {
        use crate::wasm_runtime::delegate_interest;
        use std::sync::{Arc, Mutex};

        const NODE_A: delegate_interest::NodeIdentity = 0xA1;
        const NODE_B: delegate_interest::NodeIdentity = 0xB1;

        let d = dkey(17);
        let victim = cid(8800);

        let released: Arc<Mutex<Vec<delegate_interest::NodeIdentity>>> = Default::default();
        let make_release = |node| {
            let sink = released.clone();
            let release: delegate_interest::InterestRelease = Arc::new(move |_k| {
                sink.lock().unwrap().push(node);
            });
            release
        };
        let ckey = freenet_stdlib::prelude::ContractKey::from_id_and_code(
            victim,
            CodeHash::new([0xD5; 32]),
        );

        subscribe(victim, &d, Durability::InMemoryOnly);
        // Two nodes each took their own refcount for the same pair.
        delegate_interest::record(victim, d.clone(), ckey, make_release(NODE_A), NODE_A);
        delegate_interest::record(victim, d.clone(), ckey, make_release(NODE_B), NODE_B);

        for i in 1..MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE {
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
            subscribe(cid(8800 + i as u16), &d, Durability::InMemoryOnly);
        }
        tokio::time::advance(std::time::Duration::from_millis(1)).await;

        assert_eq!(
            subscribe(cid(9200), &d, Durability::InMemoryOnly),
            SubscribeOutcome::RegisteredEvicting(victim),
            "the victim must be the eviction target, or this test proves nothing"
        );

        let mut seen = released.lock().unwrap().clone();
        seen.sort_unstable();
        assert_eq!(
            seen,
            vec![NODE_A, NODE_B],
            "evicting a pair must give back EVERY node's refcount for it; \
             discharging only the first leaves the second node's interest \
             standing with nothing able to release it"
        );

        cleanup(&d);
    }

    /// The two indexes can disagree, and a re-subscribe must repair it rather
    /// than dedup against the half that survived.
    ///
    /// `subscribe` dedups on the reverse index; `is_subscribed` answers from
    /// the forward one. While there was a single map those were the same
    /// question by construction — splitting the registry to enforce the cap
    /// turned a structural property into one nothing enforces, so it is pinned
    /// here instead.
    ///
    /// The disagreement is reachable: `remove_contract` snapshots the forward
    /// entry and then walks it clearing reverse entries, and a `subscribe`
    /// completing inside that walk is re-stripped from the forward map. The
    /// consequence is what makes this worth a test rather than a comment —
    /// without the repair, the early return happens BEFORE the forward-map
    /// insert, so no later subscribe fixes it, `remove_contract` early-returns
    /// on the missing forward entry so no removal cleans it, and the
    /// subscription is permanently dead while still holding cap budget.
    #[tokio::test]
    async fn a_resubscribe_repairs_a_half_lost_registration() {
        let d = dkey(12);
        let c = cid(8500);
        subscribe(c, &d, Durability::InMemoryOnly);

        // Exactly what a `subscribe` racing a removal walk leaves behind: the
        // reverse index still claims the subscription, the forward index has
        // lost it.
        drop_from_contract_map(&c, &d);
        assert!(
            !is_subscribed(&c, &d),
            "precondition: the forward half must be missing, or this test is \
             not exercising the repair"
        );
        assert_eq!(subscription_count(&d), 1, "the reverse half must survive");

        assert_eq!(
            subscribe(c, &d, Durability::InMemoryOnly),
            SubscribeOutcome::AlreadySubscribed,
            "the reverse index still holds it, so this is a re-subscribe"
        );
        assert!(
            is_subscribed(&c, &d),
            "a re-subscribe must re-assert the forward half; without it the \
             subscription is permanently dead and permanently holds cap budget, \
             because nothing else writes that entry and nothing ages it out"
        );
        assert_eq!(
            subscription_count(&d),
            1,
            "repairing must not double-count against the cap"
        );
        cleanup(&d);
    }

    /// The mirror disagreement — forward half present, reverse half lost, which
    /// is what a `subscribe` racing `remove_contract`'s walk leaves — must also
    /// converge, and must not double-count.
    #[tokio::test]
    async fn a_resubscribe_repairs_a_lost_reverse_half() {
        let d = dkey(13);
        let c = cid(8600);
        subscribe(c, &d, Durability::InMemoryOnly);

        if let Some(mut owned) = BY_DELEGATE.get_mut(&d) {
            owned.remove(&c);
        }
        assert_eq!(subscription_count(&d), 0, "precondition: reverse half gone");
        assert!(is_subscribed(&c, &d), "precondition: forward half survives");

        assert_eq!(
            subscribe(c, &d, Durability::InMemoryOnly),
            SubscribeOutcome::Registered,
            "the reverse index lost it, so this registers rather than dedups"
        );
        assert!(is_subscribed(&c, &d));
        assert_eq!(
            subscription_count(&d),
            1,
            "the forward half was already present; re-registering must not \
             leave the contract counted twice"
        );
        assert_eq!(
            subscribers_of(&c),
            vec![d.clone()],
            "and must not duplicate the subscriber"
        );
        cleanup(&d);
    }

    /// Removing a delegate must leave the two indexes agreeing, including for a
    /// delegate that still holds many subscriptions — the walk clears the
    /// forward half for every one of them.
    #[tokio::test]
    async fn remove_delegate_leaves_both_indexes_agreeing() {
        let d = dkey(14);
        let others = dkey(15);
        let shared = cid(8700);
        subscribe(shared, &others, Durability::InMemoryOnly);
        for i in 0..8u16 {
            subscribe(cid(8700 + i), &d, Durability::InMemoryOnly);
        }

        remove_delegate(&d, Durability::InMemoryOnly);

        assert_eq!(subscription_count(&d), 0);
        for i in 0..8u16 {
            assert!(
                !is_subscribed(&cid(8700 + i), &d),
                "every forward entry must be cleared, not just the first"
            );
        }
        assert!(
            is_subscribed(&shared, &others),
            "another delegate's subscription to the same contract must survive"
        );
        cleanup(&others);
    }

    /// Cap eviction drops ONE (contract, delegate) pair, and the local-interest
    /// refcount that pair took must come back with it.
    ///
    /// This is the only per-pair subscription drop in production, and neither
    /// of `delegate_interest`'s bulk release functions fits it: `release_contract`
    /// would discharge other delegates' holds on the same contract and
    /// `release_delegate` would discharge this delegate's holds on contracts it
    /// still subscribes to. So the assertion is two-sided on purpose — the
    /// evicted pair released EXACTLY once (catching the under-release, which is
    /// the leak) and the sibling pair not at all (catching the over-release,
    /// which is worse, because the decrement lands on interest a real
    /// subscriber holds). A test that checked only the evicted pair would pass
    /// against a fix that reached for `release_delegate`.
    ///
    /// The victim is given a REAL recorded hold rather than relying on the
    /// registry alone: `release_pair` is a no-op for a pair that holds nothing,
    /// so a victim with no hold would make this pass without exercising
    /// anything.
    #[tokio::test(start_paused = true)]
    async fn cap_eviction_releases_the_evicted_pair_and_only_that_pair() {
        use crate::wasm_runtime::delegate_interest;
        use std::sync::{Arc, Mutex};

        let d = dkey(11);
        let victim = cid(8000);
        let sibling = cid(8001);

        let released: Arc<Mutex<Vec<ContractInstanceId>>> = Default::default();
        let make_release = |id: ContractInstanceId| {
            let sink = released.clone();
            let release: delegate_interest::InterestRelease = Arc::new(move |_k| {
                sink.lock().unwrap().push(id);
            });
            release
        };
        let ckey = |id: ContractInstanceId| {
            freenet_stdlib::prelude::ContractKey::from_id_and_code(id, CodeHash::new([0xD5; 32]))
        };

        // The victim subscribes first, so it is the coldest and therefore the
        // eviction target.
        subscribe(victim, &d, Durability::InMemoryOnly);
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        subscribe(sibling, &d, Durability::InMemoryOnly);

        // Both took an interest refcount, as the network-resolved subscribe
        // path does.
        // One node; the multi-node case is the test below.
        const NODE: delegate_interest::NodeIdentity = 0xC1;
        delegate_interest::record(victim, d.clone(), ckey(victim), make_release(victim), NODE);
        delegate_interest::record(
            sibling,
            d.clone(),
            ckey(sibling),
            make_release(sibling),
            NODE,
        );

        for i in 2..MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE {
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
            subscribe(cid(8000 + i as u16), &d, Durability::InMemoryOnly);
        }
        tokio::time::advance(std::time::Duration::from_millis(1)).await;

        assert_eq!(
            subscribe(cid(9100), &d, Durability::InMemoryOnly),
            SubscribeOutcome::RegisteredEvicting(victim),
            "the coldest subscription must be the victim, or this test is not \
             exercising what it claims to"
        );

        assert_eq!(
            *released.lock().unwrap(),
            vec![victim],
            "the evicted pair's interest refcount must be given back exactly \
             once; left standing, `local_interests` never returns to zero for \
             that contract and `cleanup_contract_if_no_interest` never fires"
        );

        // The hold must also be GONE, not merely discharged: a later bulk
        // release must not discharge it a second time. Releasing the delegate
        // now should therefore report the sibling and nothing else.
        delegate_interest::release_delegate(&d);
        assert_eq!(
            *released.lock().unwrap(),
            vec![victim, sibling],
            "the sibling's hold must survive the eviction and discharge exactly \
             once afterwards; a second release of the evicted pair would \
             decrement interest that nothing holds"
        );

        cleanup(&d);
    }

    /// Both indexes must be cleared together, or a stale reverse-index entry
    /// would hold budget a delegate no longer uses — permanently, since these
    /// entries never age out.
    #[tokio::test]
    async fn removal_paths_clear_both_indexes() {
        let d = dkey(8);
        let c = cid(6000);

        subscribe(c, &d, Durability::InMemoryOnly);
        remove_delegate(&d, Durability::InMemoryOnly);
        assert_eq!(subscription_count(&d), 0);
        assert!(subscribers_of(&c).is_empty());

        subscribe(c, &d, Durability::InMemoryOnly);
        remove_contract(&c, Durability::InMemoryOnly);
        assert_eq!(
            subscription_count(&d),
            0,
            "removing a contract must free the budget it held in the reverse index"
        );
        assert!(subscribers_of(&c).is_empty());
        cleanup(&d);
    }

    /// Removing one delegate's subscription must not disturb another's to the
    /// same contract.
    #[tokio::test]
    async fn removing_one_delegate_leaves_other_subscribers() {
        let a = dkey(9);
        let b = dkey(10);
        let c = cid(7000);
        subscribe(c, &a, Durability::InMemoryOnly);
        subscribe(c, &b, Durability::InMemoryOnly);

        remove_delegate(&a, Durability::InMemoryOnly);
        assert_eq!(subscribers_of(&c), vec![b.clone()]);
        assert_eq!(subscription_count(&b), 1);
        cleanup(&b);
    }

    /// Durable half (#5493). Every test opens a real redb in a temp dir, so the
    /// assertions are about what is actually on disk, and "restart" means what
    /// it means in production: the process-global registry is emptied and the
    /// durable table is all that is left.
    #[cfg(feature = "redb")]
    mod durability {
        use super::*;

        async fn open_db() -> (Storage, tempfile::TempDir) {
            let dir = tempfile::tempdir().expect("tempdir");
            let db = Storage::new(dir.path()).await.expect("open redb");
            (db, dir)
        }

        /// Register `d` in the delegate index, which is what restore consults
        /// to decide a delegate still exists.
        fn register(db: &Storage, d: &DelegateKey) {
            db.store_delegate_index(d, d.code_hash())
                .expect("store delegate index");
        }

        /// Simulate the process exiting: the in-memory registry is lost, the
        /// durable table is untouched.
        fn lose_memory(d: &DelegateKey) {
            remove_delegate(d, Durability::InMemoryOnly);
            assert_eq!(subscription_count(d), 0);
        }

        fn rows(db: &Storage) -> Vec<(ContractInstanceId, DelegateKey)> {
            db.load_delegate_subscriptions(usize::MAX)
                .expect("load")
                .valid
        }

        #[tokio::test]
        async fn a_persisted_subscription_is_restored_after_a_restart() {
            let (db, _dir) = open_db().await;
            let d = dkey(100);
            let c = cid(20000);
            register(&db, &d);

            assert_eq!(
                subscribe(c, &d, Durability::Persist(&db)),
                SubscribeOutcome::Registered
            );
            assert_eq!(rows(&db), vec![(c, d.clone())]);

            lose_memory(&d);
            assert!(!is_subscribed(&c, &d));

            let restored = restore_from_storage(&db);
            assert_eq!(restored, vec![(c, d.clone())]);
            assert!(
                is_subscribed(&c, &d),
                "restore must put the pair back where notification delivery reads it"
            );
            assert_eq!(subscribers_of(&c), vec![d.clone()]);
            // Restore does not rewrite what it read.
            assert_eq!(rows(&db), vec![(c, d.clone())]);
            cleanup(&d);
        }

        #[tokio::test]
        async fn restore_drops_the_rows_of_a_delegate_that_is_no_longer_registered() {
            let (db, _dir) = open_db().await;
            let gone = dkey(101);
            let kept = dkey(102);
            register(&db, &kept);
            subscribe(cid(20010), &gone, Durability::Persist(&db));
            subscribe(cid(20011), &kept, Durability::Persist(&db));
            lose_memory(&gone);
            lose_memory(&kept);

            let restored = restore_from_storage(&db);
            assert_eq!(restored, vec![(cid(20011), kept.clone())]);
            assert!(!is_subscribed(&cid(20010), &gone));
            assert_eq!(
                rows(&db),
                vec![(cid(20011), kept.clone())],
                "a row nothing can ever unregister must be deleted, not replayed every boot"
            );
            cleanup(&kept);
        }

        #[tokio::test]
        async fn unregistering_a_delegate_clears_its_persisted_rows() {
            let (db, _dir) = open_db().await;
            let d = dkey(103);
            let other = dkey(104);
            register(&db, &d);
            register(&db, &other);
            subscribe(cid(20020), &d, Durability::Persist(&db));
            subscribe(cid(20021), &d, Durability::Persist(&db));
            subscribe(cid(20020), &other, Durability::Persist(&db));

            remove_delegate(&d, Durability::Persist(&db));
            assert_eq!(rows(&db), vec![(cid(20020), other.clone())]);

            lose_memory(&other);
            assert_eq!(restore_from_storage(&db), vec![(cid(20020), other.clone())]);
            assert_eq!(subscription_count(&d), 0);
            cleanup(&other);
        }

        #[tokio::test]
        async fn contract_removal_clears_rows_but_a_closed_notification_channel_does_not() {
            let (db, _dir) = open_db().await;
            let a = dkey(105);
            let b = dkey(106);
            register(&db, &a);
            register(&db, &b);
            let removed = cid(20030);
            let shutdown = cid(20031);
            subscribe(removed, &a, Durability::Persist(&db));
            subscribe(removed, &b, Durability::Persist(&db));
            subscribe(shutdown, &a, Durability::Persist(&db));

            remove_contract(&removed, Durability::Persist(&db));
            // The channel-closed path, which runs on node shutdown.
            remove_contract(&shutdown, Durability::InMemoryOnly);

            assert_eq!(
                rows(&db),
                vec![(shutdown, a.clone())],
                "contract removal must clear every delegate's row for it; a shutdown must clear none"
            );
            assert!(!is_subscribed(&shutdown, &a));
            assert_eq!(restore_from_storage(&db), vec![(shutdown, a.clone())]);
            assert!(is_subscribed(&shutdown, &a));
            cleanup(&a);
            cleanup(&b);
        }

        #[tokio::test]
        async fn a_repeat_subscribe_does_not_write() {
            let (db, _dir) = open_db().await;
            let d = dkey(107);
            let c = cid(20040);
            subscribe(c, &d, Durability::Persist(&db));
            // Delete the row behind the registry's back. A repeat subscribe
            // that wrote would put it back; one that does not, will not.
            db.remove_delegate_subscriptions_for_contract(&c)
                .expect("remove");
            assert_eq!(
                subscribe(c, &d, Durability::Persist(&db)),
                SubscribeOutcome::AlreadySubscribed
            );
            assert!(
                rows(&db).is_empty(),
                "AlreadySubscribed must not take a write"
            );
            cleanup(&d);
        }

        #[tokio::test]
        async fn cap_eviction_removes_the_evicted_row_in_the_same_write() {
            let (db, _dir) = open_db().await;
            let d = dkey(108);
            register(&db, &d);
            for i in 0..MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE {
                subscribe(cid(21000 + i as u16), &d, Durability::Persist(&db));
            }
            let outcome = subscribe(cid(22000), &d, Durability::Persist(&db));
            let SubscribeOutcome::RegisteredEvicting(evicted) = outcome else {
                panic!("expected an eviction at the cap, got {outcome:?}");
            };
            let on_disk = rows(&db);
            assert_eq!(on_disk.len(), MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE);
            assert!(on_disk.contains(&(cid(22000), d.clone())));
            assert!(
                !on_disk.contains(&(evicted, d.clone())),
                "an evicted subscription left on disk would displace a newer one at the next boot"
            );
            cleanup(&d);
        }

        /// The per-delegate cap holds on RESTORE, not only on add: a table
        /// holding more rows for one delegate than the cap allows (only
        /// reachable by corruption or an older writer) restores at most the
        /// cap, and the surplus is deleted so the table converges.
        #[tokio::test]
        async fn restore_reapplies_the_per_delegate_cap() {
            let (db, _dir) = open_db().await;
            let d = dkey(109);
            register(&db, &d);
            let over = MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE + 10;
            for i in 0..over {
                let c = cid(23000 + i as u16);
                let mut key = Vec::new();
                key.extend_from_slice(c.as_ref());
                key.extend_from_slice(d.as_ref());
                key.extend_from_slice(d.code_hash().as_ref());
                db.insert_raw_delegate_subscription_row(
                    &key,
                    &[crate::contract::storages::redb::DELEGATE_SUBSCRIPTION_ROW_V1],
                )
                .expect("plant row");
            }

            let restored = restore_from_storage(&db);
            assert_eq!(restored.len(), MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE);
            assert_eq!(
                subscription_count(&d),
                MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE
            );
            assert_eq!(
                db.delegate_subscription_row_count(),
                MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE as u64
            );
            // What was restored is exactly what is left on disk.
            let mut on_disk = rows(&db);
            let mut restored_sorted = restored.clone();
            on_disk.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
            restored_sorted.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
            assert_eq!(on_disk, restored_sorted);
            cleanup(&d);
        }

        /// Corrupt rows never crash restore and never block the valid ones;
        /// they are deleted so they are not re-read at every boot.
        #[tokio::test]
        async fn restore_skips_and_deletes_malformed_rows() {
            let (db, _dir) = open_db().await;
            let d = dkey(110);
            register(&db, &d);
            subscribe(cid(24000), &d, Durability::Persist(&db));
            lose_memory(&d);
            db.insert_raw_delegate_subscription_row(&[1, 2, 3], &[1])
                .expect("short key");
            let mut unknown_version = vec![0xEE; 96];
            unknown_version[31] = TEST_ID_NAMESPACE;
            db.insert_raw_delegate_subscription_row(&unknown_version, &[99])
                .expect("unknown format byte");
            assert_eq!(db.delegate_subscription_row_count(), 3);

            let restored = restore_from_storage(&db);
            assert_eq!(restored, vec![(cid(24000), d.clone())]);
            assert_eq!(db.delegate_subscription_row_count(), 1);
            cleanup(&d);
        }

        #[tokio::test]
        async fn restore_on_a_fresh_database_is_inert() {
            let (db, _dir) = open_db().await;
            assert!(restore_from_storage(&db).is_empty());
            assert_eq!(db.delegate_subscription_row_count(), 0);
        }

        /// An upgraded delegate is a NEW key. Its predecessor's rows belong to
        /// the predecessor: kept while it is registered, dropped once it is not.
        /// Nothing migrates them.
        #[tokio::test]
        async fn an_upgraded_delegate_does_not_inherit_its_predecessors_rows() {
            let (db, _dir) = open_db().await;
            let old = dkey(111);
            let new = dkey(112);
            register(&db, &new);
            subscribe(cid(25000), &old, Durability::Persist(&db));
            lose_memory(&old);

            assert!(restore_from_storage(&db).is_empty());
            assert!(!is_subscribed(&cid(25000), &new));
            assert!(rows(&db).is_empty());
        }

        /// The node-wide ceiling: past it a new subscription still works in
        /// memory but is not written, and an eviction in the same call is still
        /// applied.
        #[tokio::test]
        async fn the_node_wide_ceiling_bounds_what_is_persisted() {
            let (db, _dir) = open_db().await;
            let d = dkey(113);
            assert!(
                db.record_delegate_subscription(&cid(26000), &d, None, 2)
                    .unwrap()
            );
            assert!(
                db.record_delegate_subscription(&cid(26001), &d, None, 2)
                    .unwrap()
            );
            assert!(
                !db.record_delegate_subscription(&cid(26002), &d, None, 2)
                    .unwrap(),
                "a third row must be refused at a ceiling of 2"
            );
            assert_eq!(db.delegate_subscription_row_count(), 2);
            // Re-recording an existing row at the ceiling is not a refusal.
            assert!(
                db.record_delegate_subscription(&cid(26000), &d, None, 2)
                    .unwrap()
            );
            // An eviction frees its slot in the same transaction.
            assert!(
                db.record_delegate_subscription(&cid(26002), &d, Some(&cid(26000)), 2)
                    .unwrap()
            );
            let loaded = db.load_delegate_subscriptions(1).unwrap();
            assert_eq!(loaded.valid.len(), 1);
            assert_eq!(
                loaded.excess.len(),
                1,
                "rows past max_rows are reported, not dropped silently"
            );
        }
    }
}
