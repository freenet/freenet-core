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
//! # The two registration paths
//!
//! Both converge here, and a bound that missed either would be free to bypass:
//!
//!  1. **V2** delegates call the `subscribe_contract` host function, which
//!     reaches [`subscribe`] via `native_api::DelegateCallEnv::subscribe_contract_sync`.
//!  2. **V1** delegates emit `OutboundDelegateMsg::SubscribeContractRequest`,
//!     handled in `contract::handle_delegate_with_contract_requests`.
//!
//! A delegate chooses which of the two it gets: `Runtime::prepare_delegate_call`
//! selects `DelegateApiVersion::V2` iff the module imports the async host
//! functions, and `V1` otherwise. So the version is not a property the node
//! assigns — it is a property of the guest WASM, and a bound applied to only one
//! path is an opt-out rather than a bound.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractInstanceId, DelegateKey};

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

/// What [`subscribe`] did.
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
pub(crate) fn subscribe(contract: ContractInstanceId, delegate: &DelegateKey) -> SubscribeOutcome {
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
        let coldest = owned
            .iter()
            .min_by_key(|(_, stamp)| **stamp)
            .map(|(id, _)| *id);
        if let Some(coldest) = coldest {
            owned.remove(&coldest);
            drop_from_contract_map(&coldest, delegate);
            let total = CAP_EVICTIONS.fetch_add(1, Ordering::Relaxed) + 1;
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
            evicted = Some(coldest);
        }
    }

    owned.insert(contract, now);
    drop(owned);

    BY_CONTRACT
        .entry(contract)
        .or_default()
        .insert(delegate.clone());

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
pub(crate) fn remove_delegate(delegate: &DelegateKey) {
    // Hold the reverse-index guard across the forward-map cleanup, rather than
    // removing the entry and then walking a detached snapshot.
    //
    // Executors are a pool, so `UnregisterDelegate` does not run on the same
    // thread as the V2 host call or `apply_resolved_contract_op`. A `subscribe`
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
pub(crate) fn remove_contract(contract: &ContractInstanceId) {
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
pub(crate) fn unsubscribe(contract: &ContractInstanceId, delegate: &DelegateKey) {
    let removed = match BY_DELEGATE.get_mut(delegate) {
        Some(mut owned) => owned.remove(contract).is_some(),
        None => false,
    };
    if removed {
        drop_from_contract_map(contract, delegate);
        BY_DELEGATE.remove_if(delegate, |_, owned| owned.is_empty());
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
        remove_delegate(delegate);
    }

    #[tokio::test]
    async fn subscribe_is_idempotent() {
        let d = dkey(1);
        let c = cid(1);
        assert_eq!(subscribe(c, &d), SubscribeOutcome::Registered);
        assert_eq!(subscribe(c, &d), SubscribeOutcome::AlreadySubscribed);
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
            subscribe(cid(1000 + i as u16), &d);
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
        subscribe(first, &d);
        for i in 1..MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE {
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
            subscribe(cid(2000 + i as u16), &d);
        }
        tokio::time::advance(std::time::Duration::from_millis(1)).await;

        let newcomer = cid(9000);
        // The counter is process-global and other tests in this binary evict
        // too, so assert it ADVANCED rather than asserting an absolute value.
        let evictions_before = cap_evictions_total();
        let outcome = subscribe(newcomer, &d);
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
        subscribe(oldest, &d);
        for i in 1..MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE {
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
            subscribe(cid(3000 + i as u16), &d);
        }

        // The oldest subscription is the busy one: it has just been notified,
        // while the second-oldest has never been.
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        note_notified(&oldest, &d);
        let expected_victim = cid(3001);

        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        assert_eq!(
            subscribe(cid(9001), &d),
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
        subscribe(a, &d);
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        subscribe(b, &d);

        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        subscribe(a, &d); // idempotent re-subscribe, must not restamp

        for i in 2..MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE {
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
            subscribe(cid(4000 + i as u16), &d);
        }
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        assert_eq!(
            subscribe(cid(9002), &d),
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
        subscribe(shared, &victim);

        subscribe(shared, &hog);
        for i in 1..(MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE + 20) {
            subscribe(cid(5000 + i as u16), &hog);
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
        subscribe(c, &d);

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
            subscribe(c, &d),
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
        subscribe(c, &d);

        if let Some(mut owned) = BY_DELEGATE.get_mut(&d) {
            owned.remove(&c);
        }
        assert_eq!(subscription_count(&d), 0, "precondition: reverse half gone");
        assert!(is_subscribed(&c, &d), "precondition: forward half survives");

        assert_eq!(
            subscribe(c, &d),
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
        subscribe(shared, &others);
        for i in 0..8u16 {
            subscribe(cid(8700 + i), &d);
        }

        remove_delegate(&d);

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
        subscribe(victim, &d);
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        subscribe(sibling, &d);

        // Both took an interest refcount, as the network-resolved subscribe
        // path does.
        delegate_interest::record(victim, d.clone(), ckey(victim), make_release(victim));
        delegate_interest::record(sibling, d.clone(), ckey(sibling), make_release(sibling));

        for i in 2..MAX_CONTRACT_SUBSCRIPTIONS_PER_DELEGATE {
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
            subscribe(cid(8000 + i as u16), &d);
        }
        tokio::time::advance(std::time::Duration::from_millis(1)).await;

        assert_eq!(
            subscribe(cid(9100), &d),
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

        subscribe(c, &d);
        remove_delegate(&d);
        assert_eq!(subscription_count(&d), 0);
        assert!(subscribers_of(&c).is_empty());

        subscribe(c, &d);
        remove_contract(&c);
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
        subscribe(c, &a);
        subscribe(c, &b);

        remove_delegate(&a);
        assert_eq!(subscribers_of(&c), vec![b.clone()]);
        assert_eq!(subscription_count(&b), 1);
        cleanup(&b);
    }
}
