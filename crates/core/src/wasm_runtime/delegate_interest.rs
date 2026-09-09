//! Interest a delegate subscription took, and the obligation to give it back
//! (#5542).
//!
//! # Why this exists
//!
//! Before #5542 the V1 delegate SUBSCRIBE arm inserted into
//! [`DELEGATE_SUBSCRIPTIONS`](super::DELEGATE_SUBSCRIPTIONS) and did nothing
//! else. The registry is a `HashSet`, so a re-subscribe was idempotent for free
//! and there was no accounting to get wrong.
//!
//! #5542 routes a subscribe for a contract this node does not hold through
//! `run_executor_subscribe`, which ends in
//! `InterestManager::add_local_client` — a per-CONTRACT **refcount**, taking no
//! client id, explicitly non-idempotent. That is the demand registration the
//! feature needs: without it the contract is evicted out from under the
//! subscription. But it introduced an asymmetry, because **nothing on any
//! delegate path can decrement it**. The only decrement outside
//! `ring/interest.rs` is driven by `remove_client_from_all_subscriptions`
//! on a WebSocket `Disconnect`, keyed by `ClientId`, and a delegate has no
//! `ClientId` in this tree.
//!
//! The plain happy path was never the problem: a delegate subscription is
//! permanent, so a permanent local interest matching it is what we want. What
//! was missing is that subscription **removal** was not wired to interest
//! **release**, and three routine paths drop a delegate subscription —
//! `UnregisterDelegate`, contract removal, and notification-channel-closed
//! cleanup. `UnregisterDelegate` is an everyday operation, not an edge case.
//! Left unwired, `local_interests` never returns to zero for that contract and
//! `cleanup_contract_if_no_interest` never fires: demand nothing can retire, on
//! the interest path #5467 exists to make trustworthy.
//!
//! # Shape, and why it is this one
//!
//! Each hold is **self-discharging**: it stores the `ContractKey` to release
//! and a type-erased closure that performs the release. Two consequences, both
//! deliberate:
//!
//! * **No `crate::ring` dependency here.** The removal sites live in
//!   `wasm_runtime`, `InterestManager` lives in `ring`, and the closure is
//!   built where an `OpManager` is already in hand. Same trick as the
//!   `state_write_callback` / `state_admit_callback` hooks on `Runtime`.
//! * **Per-node, not process-global behaviour.** The map itself is a global,
//!   matching the registry it shadows, but each hold carries ITS OWN node's
//!   release closure. A single global callback bound to one node's `OpManager`
//!   would release the wrong node's interest under the in-process multi-node
//!   test harness, where these globals are already shared (#4824).
//!
//! The closure captures a `Weak<OpManager>`, so a hold never keeps a shut-down
//! node's `OpManager` alive; a hold that cannot upgrade has nothing left to
//! release and is simply dropped.
//!
//! # What is recorded, and what deliberately is not
//!
//! **Only acquisitions this node actually made.** A subscribe answered from the
//! local store takes no refcount, and neither does the V2
//! `subscribe_contract_sync` host function — both only insert the registry
//! hook. So this map is a strict subset of `DELEGATE_SUBSCRIPTIONS`, keyed by
//! the same pair, and releasing is driven from HERE rather than from the
//! registry. That is what makes over-release impossible: an entry exists if and
//! only if `add_local_client` ran for that pair, so a decrement can never fall
//! on interest some other subscriber holds — which would be strictly worse than
//! the leak it was trying to fix.
//!
//! This is not a mirror of the subscription set that has to be kept in step
//! with it; it is a record of outstanding obligations, and it may legitimately
//! be smaller. Anything that removes a subscription may call the release
//! functions unconditionally: a pair with no hold is a no-op.

use std::sync::Arc;

use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey, DelegateKey};

/// Performs the actual `InterestManager::remove_local_client`. Type-erased so
/// this module needs no `crate::ring` or `crate::node` dependency.
pub type InterestRelease = Arc<dyn Fn(&ContractKey) + Send + Sync>;

struct Hold {
    key: ContractKey,
    release: InterestRelease,
}

/// Outstanding local-interest refcounts taken on behalf of delegate
/// subscriptions, keyed by the `(contract, delegate)` pair that owns them.
static DELEGATE_INTEREST_HOLDS: std::sync::LazyLock<
    DashMap<(ContractInstanceId, DelegateKey), Hold>,
> = std::sync::LazyLock::new(DashMap::new);

/// Record that one local-interest refcount was taken for
/// `(contract, delegate)`, and how to give it back.
///
/// Idempotent per pair: recording twice keeps ONE obligation, matching the
/// caller's own guarantee that it increments at most once per pair (the
/// `already_subscribed` gate plus the in-round de-duplication). If that
/// guarantee were ever broken, holding one obligation for two increments leaks
/// — which is the safe direction, since the alternative is releasing interest
/// that was never taken.
pub(crate) fn record(
    contract: ContractInstanceId,
    delegate: DelegateKey,
    key: ContractKey,
    release: InterestRelease,
) {
    DELEGATE_INTEREST_HOLDS
        .entry((contract, delegate))
        .or_insert(Hold { key, release });
}

/// Release every hold taken for `delegate`, across all contracts.
///
/// Called from `UnregisterDelegate`, which drops the delegate from every
/// subscription entry.
pub(crate) fn release_delegate(delegate: &DelegateKey) {
    // Collect first: `retain` holds shard locks, and a release closure reaches
    // into `InterestManager`, which takes its own locks. Doing that under a
    // DashMap shard guard is how lock-order inversions get built.
    let mut discharged = Vec::new();
    DELEGATE_INTEREST_HOLDS.retain(|(_, holder), hold| {
        if holder == delegate {
            discharged.push((hold.key, hold.release.clone()));
            false
        } else {
            true
        }
    });
    for (key, release) in discharged {
        release(&key);
    }
}

/// Release every hold taken for `contract`, across all delegates.
///
/// Called from contract removal and from notification-channel-closed cleanup,
/// both of which drop every delegate subscription for one contract.
pub(crate) fn release_contract(contract: &ContractInstanceId) {
    let mut discharged = Vec::new();
    DELEGATE_INTEREST_HOLDS.retain(|(id, _), hold| {
        if id == contract {
            discharged.push((hold.key, hold.release.clone()));
            false
        } else {
            true
        }
    });
    for (key, release) in discharged {
        release(&key);
    }
}

#[cfg(test)]
mod tests {
    //! ISOLATION CONTRACT for anything added here: `DELEGATE_INTEREST_HOLDS` is
    //! a process global and `cargo test` runs these in parallel, so a test must
    //! (a) use key bytes no sibling uses, and (b) NEVER clear the map or assert
    //! on its total size. An earlier version of these tests ended with
    //! `DELEGATE_INTEREST_HOLDS.clear()`, which wiped siblings' entries
    //! mid-run and made all three fail against correct code. Assert on the
    //! specific pairs you created.
    use super::*;
    use freenet_stdlib::prelude::CodeHash;
    use std::sync::Mutex;

    fn key(byte: u8) -> ContractKey {
        ContractKey::from_id_and_code(
            ContractInstanceId::new([byte; 32]),
            CodeHash::new([byte; 32]),
        )
    }

    fn delegate(byte: u8) -> DelegateKey {
        DelegateKey::new([byte; 32], CodeHash::new([byte; 32]))
    }

    /// The three sites that DROP a delegate subscription must each release the
    /// interest it held. The module tests above prove the mechanism; these
    /// prove it is actually reached, which is the half that silently rots — a
    /// release function nobody calls is indistinguishable from the leak it was
    /// written to close.
    ///
    /// Source-level rather than behavioural because two of the three sites sit
    /// inside `UnregisterDelegate` / notification-dispatch paths that need a
    /// live executor and a live notification channel to reach. If you make
    /// those reachable in a unit test, replace these.
    #[test]
    fn every_subscription_removal_site_releases_its_interest() {
        // `UnregisterDelegate` — an ORDINARY operation, not an edge case.
        let delegates = include_str!("../contract/executor/runtime/delegates.rs");
        let unregister = delegates
            .split("DelegateRequest::UnregisterDelegate(key) => {")
            .nth(1)
            .expect("the UnregisterDelegate arm must exist");
        assert!(
            unregister.contains("delegate_interest::release_delegate(&key)"),
            "UnregisterDelegate drops the delegate from every subscription entry, \
             so it must also give back the local interest those subscriptions \
             took; without it `local_interests` never returns to zero and \
             `cleanup_contract_if_no_interest` never fires (#5542)"
        );

        // Contract removal.
        let store = include_str!("contract_store.rs");
        assert!(
            store.contains("delegate_interest::release_contract(key.id())"),
            "removing a contract drops its delegate subscriptions, so it must \
             release the interest they took (#5542)"
        );

        // Notification-channel-closed cleanup.
        let executor = include_str!("../contract/executor/runtime/executor_impl.rs");
        assert!(
            executor.contains("delegate_interest::release_contract(&instance_id)"),
            "channel-closed cleanup removes every subscription for the contract, \
             so it must release the interest they took (#5542)"
        );
    }

    /// The obligation must be RECORDED where the refcount is taken, or the
    /// release sites above have nothing to discharge and the whole mechanism is
    /// inert while looking present.
    #[test]
    fn the_subscribe_path_records_the_obligation_it_incurs() {
        let contract = include_str!("../contract.rs");
        let body = contract
            .split("fn apply_resolved_contract_op<CH>(")
            .nth(1)
            .expect("apply_resolved_contract_op must exist");
        let body = &body[..body.find("\nfn ").unwrap_or(body.len())];
        assert!(
            body.contains("delegate_interest::record("),
            "the site that installs the DELEGATE_SUBSCRIPTIONS hook after a \
             successful network subscribe is the site that took the \
             `add_local_client` refcount, so it must record the obligation to \
             release it (#5542)"
        );
        assert!(
            body.contains("remove_local_client"),
            "the recorded release closure must actually call \
             `remove_local_client`; recording an obligation that discharges to \
             nothing is the same leak with more code"
        );
    }

    /// A release closure that records what it was asked to release.
    fn recorder() -> (InterestRelease, Arc<Mutex<Vec<ContractKey>>>) {
        let seen: Arc<Mutex<Vec<ContractKey>>> = Default::default();
        let sink = seen.clone();
        let release: InterestRelease = Arc::new(move |k: &ContractKey| {
            sink.lock().unwrap().push(*k);
        });
        (release, seen)
    }

    /// `UnregisterDelegate` is an ordinary operation, and it must give back
    /// every refcount that delegate's subscriptions took. Before #5542 wired
    /// `run_executor_subscribe` in there was nothing to give back; after it,
    /// skipping this leaves `local_interests` permanently above zero for the
    /// contract, so `cleanup_contract_if_no_interest` never fires.
    #[test]
    fn unregistering_a_delegate_releases_every_interest_it_held() {
        let (release, seen) = recorder();
        let d = delegate(200);
        record(*key(201).id(), d.clone(), key(201), release.clone());
        record(*key(202).id(), d.clone(), key(202), release.clone());
        // A different delegate's hold on one of the same contracts must survive.
        let other = delegate(203);
        record(*key(201).id(), other.clone(), key(201), release);

        release_delegate(&d);

        let mut released: Vec<_> = seen.lock().unwrap().clone();
        released.sort_by_key(|k| k.to_string());
        assert_eq!(
            released,
            vec![key(201), key(202)],
            "every contract the delegate held interest in must be released"
        );
        assert!(
            DELEGATE_INTEREST_HOLDS.contains_key(&(*key(201).id(), other)),
            "another delegate's hold on the same contract must NOT be discharged \
             — releasing interest a different subscriber holds is worse than the \
             leak this closes"
        );
    }

    /// Contract removal and channel-closed cleanup both drop every delegate
    /// subscription for one contract, so both must discharge every hold on it.
    #[test]
    fn removing_a_contract_releases_every_delegate_hold_on_it() {
        let (release, seen) = recorder();
        let target = key(210);
        record(*target.id(), delegate(211), target, release.clone());
        record(*target.id(), delegate(212), target, release.clone());
        record(*key(213).id(), delegate(211), key(213), release);

        release_contract(target.id());

        assert_eq!(
            seen.lock().unwrap().len(),
            2,
            "both delegates' holds on the removed contract must be released"
        );
        assert!(
            DELEGATE_INTEREST_HOLDS.contains_key(&(*key(213).id(), delegate(211))),
            "a hold on a DIFFERENT contract must survive"
        );
    }

    /// Releasing a pair that never took a refcount must be a no-op, not a
    /// decrement. A subscribe answered from the local store takes none, and
    /// neither does the V2 `subscribe_contract_sync` host function — so the
    /// removal paths call these functions for pairs that hold nothing, and a
    /// decrement there would fall on interest a real client holds.
    #[test]
    fn releasing_a_pair_that_holds_nothing_does_not_decrement() {
        let (release, seen) = recorder();
        record(*key(220).id(), delegate(221), key(220), release);

        release_delegate(&delegate(222));
        release_contract(key(223).id());

        assert!(
            seen.lock().unwrap().is_empty(),
            "no hold matched, so nothing may be released"
        );
        assert!(
            DELEGATE_INTEREST_HOLDS.contains_key(&(*key(220).id(), delegate(221))),
            "the unrelated hold must be untouched"
        );
    }

    /// Recording the same pair twice keeps ONE obligation. The caller
    /// increments at most once per pair; if that ever broke, leaking one
    /// refcount is the safe direction, because the alternative is releasing
    /// interest that was never taken.
    #[test]
    fn a_pair_recorded_twice_holds_one_obligation() {
        let (release, seen) = recorder();
        record(*key(230).id(), delegate(231), key(230), release.clone());
        record(*key(230).id(), delegate(231), key(230), release);

        release_delegate(&delegate(231));

        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "one obligation must produce exactly one release"
        );
    }
}
