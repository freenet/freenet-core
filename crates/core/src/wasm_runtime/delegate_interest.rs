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

/// In-process identity of the node that took a hold: the address of its
/// `Arc<OpManager>` (#5542 finding M3/F4).
///
/// The map is process-global, so its key has to be too, and "which node" is
/// exactly "which `OpManager` instance" — there is no stable node id on
/// `OpManager` to use instead. In production this is always a single value,
/// because production runs one node per process; it earns its place in the
/// in-process multi-node harness, which is the environment the module's own
/// per-hold-closure design was written for.
pub(crate) type NodeIdentity = usize;

/// Outstanding local-interest refcounts taken on behalf of delegate
/// subscriptions, keyed by the `(contract, delegate, node)` triple that owns
/// them.
///
/// The NODE component is load-bearing and was missing. Keyed on the pair alone,
/// `record`'s `or_insert` silently discarded a second node's obligation when two
/// nodes in one process subscribed the same delegate to the same contract: both
/// incremented, one hold existed, so release discharged one and leaked the
/// other permanently. The module already argued that per-node behaviour matters
/// here — it is why each hold carries its own release closure rather than a
/// single global callback — but the key did not carry that intent.
static DELEGATE_INTEREST_HOLDS: std::sync::LazyLock<
    DashMap<(ContractInstanceId, DelegateKey, NodeIdentity), Hold>,
> = std::sync::LazyLock::new(DashMap::new);

/// Record that one local-interest refcount was taken for
/// `(contract, delegate)`, and how to give it back.
///
/// Idempotent per (pair, node): recording twice for the SAME node keeps ONE
/// obligation, matching that caller's own guarantee that it increments at most
/// once per pair (the `already_subscribed` gate plus the in-round
/// de-duplication). A DIFFERENT node recording the same pair gets its own
/// obligation, because it took its own refcount on its own `InterestManager` —
/// collapsing the two is how the second one leaked. If that
/// guarantee were ever broken, holding one obligation for two increments leaks
/// — which is the safe direction, since the alternative is releasing interest
/// that was never taken.
pub(crate) fn record(
    contract: ContractInstanceId,
    delegate: DelegateKey,
    key: ContractKey,
    release: InterestRelease,
    node: NodeIdentity,
) {
    DELEGATE_INTEREST_HOLDS
        .entry((contract, delegate, node))
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
    DELEGATE_INTEREST_HOLDS.retain(|(_, holder, _), hold| {
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
    DELEGATE_INTEREST_HOLDS.retain(|(id, _, _), hold| {
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

    /// Two distinct in-process node identities. In production there is only
    /// ever one; these stand for two `OpManager`s in one test process.
    const NODE_A: NodeIdentity = 0xA;
    const NODE_B: NodeIdentity = 0xB;

    /// Two NODES in one process, subscribing the same delegate to the same
    /// contract, each hold their own obligation (#5542 finding M3/F4).
    ///
    /// Keyed on `(contract, delegate)` alone, `record`'s `or_insert` made the
    /// second node's call a silent no-op. Both nodes had incremented their own
    /// `InterestManager`, one hold existed, so release discharged one node's
    /// refcount and leaked the other's permanently. Exposure is the in-process
    /// multi-node harness — which is exactly the environment this module's
    /// per-hold release closure was designed for, so the key had to carry the
    /// same intent the closure already did.
    #[test]
    fn two_nodes_holding_the_same_pair_each_keep_their_obligation() {
        let (release_a, seen_a) = recorder();
        let (release_b, seen_b) = recorder();
        let d = delegate(240);
        let k = key(241);

        record(*k.id(), d.clone(), k, release_a, NODE_A);
        record(*k.id(), d.clone(), k, release_b, NODE_B);

        release_delegate(&d);

        assert_eq!(
            seen_a.lock().unwrap().len(),
            1,
            "node A's refcount must be released"
        );
        assert_eq!(
            seen_b.lock().unwrap().len(),
            1,
            "node B took its own refcount on its own InterestManager, so it must \
             get its own release; collapsing the two leaks whichever recorded \
             second"
        );
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
    ///
    /// TWO VACUITY TRAPS this pin walked into and now avoids, both of which
    /// `contract.rs`'s pin helpers were written for after the same defects
    /// shipped there (#5450 and the #5554-era pins):
    ///
    /// * **A commented-out call is still text.** Without stripping comments a
    ///   scrape cannot tell `foo();` from `// foo();`, so it stays green over a
    ///   call that no longer runs — and commenting a line out is exactly what
    ///   someone does while debugging, which is precisely when the pin is the
    ///   only thing still watching.
    /// * **An unbounded search passes if the call merely MOVES.** Searching a
    ///   whole file, or the entire suffix after a match arm's opener, is
    ///   satisfied by the call existing anywhere later in the file — including
    ///   in a function that never runs on this path. Each search is now bounded
    ///   to the region that has to contain it.
    #[test]
    fn every_subscription_removal_site_releases_its_interest() {
        /// Comment-stripped text between `start` and the next `end` after it.
        /// Panics rather than returning empty if either anchor is missing, so a
        /// rename fails the pin loudly instead of vacuously passing it.
        fn region(src: &str, start: &str, end: &str) -> String {
            let stripped = crate::contract::source_pin_util::strip_comments(src);
            let from = stripped
                .find(start)
                .unwrap_or_else(|| panic!("anchor `{start}` must exist"));
            let rest = &stripped[from + start.len()..];
            let to = rest
                .find(end)
                .unwrap_or_else(|| panic!("closing anchor `{end}` must follow `{start}`"));
            rest[..to].to_string()
        }

        // `UnregisterDelegate` — an ORDINARY operation, not an edge case.
        // Bounded to that match arm: the next arm's opener ends it.
        let unregister = region(
            include_str!("../contract/executor/runtime/delegates.rs"),
            "DelegateRequest::UnregisterDelegate(key) => {",
            "DelegateRequest::ApplicationMessages {",
        );
        assert!(
            unregister.contains("delegate_interest::release_delegate(&key)"),
            "UnregisterDelegate drops the delegate from every subscription entry, \
             so it must also give back the local interest those subscriptions \
             took; without it `local_interests` never returns to zero and \
             `cleanup_contract_if_no_interest` never fires (#5542)"
        );

        // Contract removal, bounded to `ContractStore::remove_contract`.
        let remove_contract = region(
            include_str!("contract_store.rs"),
            "pub fn remove_contract(&mut self, key: &ContractKey) -> RuntimeResult<()> {",
            "\n    pub fn ",
        );
        assert!(
            remove_contract.contains("delegate_interest::release_contract(key.id())"),
            "removing a contract drops its delegate subscriptions, so it must \
             release the interest they took (#5542)"
        );

        // The simulation/mock backend must agree with the production store, or
        // a simulation models a node that behaves differently from the shipped
        // one. It diverged from #3251 until #5542 found it.
        let in_memory_remove = region(
            include_str!("simulation_runtime.rs"),
            "pub fn remove_contract(&self, key: &ContractKey) -> Result<(), anyhow::Error> {",
            "\n    pub fn ",
        );
        assert!(
            in_memory_remove.contains("delegate_interest::release_contract(key.id())"),
            "`InMemoryContractStore::remove_contract` must release delegate \
             interest exactly as the production `ContractStore` does; a backend \
             that diverges here makes every simulation model a node that behaves \
             differently from the one that ships (#5542)"
        );

        // Notification-channel-closed cleanup, bounded to its own function.
        let notify = region(
            include_str!("../contract/executor/runtime/executor_impl.rs"),
            "fn send_delegate_contract_notifications(&self, key: &ContractKey, new_state: &WrappedState) {",
            "async fn fetch_related_for_validation(",
        );
        assert!(
            notify.contains("delegate_interest::release_contract(&instance_id)"),
            "channel-closed cleanup removes every subscription for the contract, \
             so it must release the interest they took (#5542)"
        );
    }

    /// The obligation must be RECORDED where the refcount is taken, or the
    /// release sites above have nothing to discharge and the whole mechanism is
    /// inert while looking present.
    #[test]
    fn the_subscribe_path_records_the_obligation_it_incurs() {
        let contract =
            crate::contract::source_pin_util::strip_comments(include_str!("../contract.rs"));
        let body = contract
            .split("fn apply_resolved_contract_op<CH>(")
            .nth(1)
            .expect("apply_resolved_contract_op must exist");
        let body = &body[..body
            .find("\nfn ")
            .expect("apply_resolved_contract_op must be followed by another fn")];
        assert!(
            body.contains("delegate_interest::record("),
            "the site that installs the DELEGATE_SUBSCRIPTIONS hook after a \
             successful network subscribe is the site that took the \
             `add_local_client` refcount, so it must record the obligation to \
             release it (#5542)"
        );
        assert!(
            body.contains("delegate_interest_release_closure("),
            "the recorded obligation must be built by the shared release-closure \
             constructor; there are two sites that take this refcount now, and a \
             second hand-written closure is where the next divergence lives"
        );
        // ...and that constructor must actually decrement. Following the
        // extraction rather than dropping the assertion: the property being
        // pinned is "the obligation discharges to a real decrement", and it is
        // one function further away, not gone.
        let closure = contract
            .split("fn delegate_interest_release_closure(")
            .nth(1)
            .expect("delegate_interest_release_closure must exist");
        let closure = &closure[..closure
            .find("\nfn ")
            .expect("delegate_interest_release_closure must be followed by another fn")];
        assert!(
            closure.contains("remove_local_client"),
            "the release closure must actually call `remove_local_client`; \
             recording an obligation that discharges to nothing is the same leak \
             with more code"
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
        record(*key(201).id(), d.clone(), key(201), release.clone(), NODE_A);
        record(*key(202).id(), d.clone(), key(202), release.clone(), NODE_A);
        // A different delegate's hold on one of the same contracts must survive.
        let other = delegate(203);
        record(*key(201).id(), other.clone(), key(201), release, NODE_A);

        release_delegate(&d);

        let mut released: Vec<_> = seen.lock().unwrap().clone();
        released.sort_by_key(|k| k.to_string());
        assert_eq!(
            released,
            vec![key(201), key(202)],
            "every contract the delegate held interest in must be released"
        );
        assert!(
            DELEGATE_INTEREST_HOLDS.contains_key(&(*key(201).id(), other, NODE_A)),
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
        record(*target.id(), delegate(211), target, release.clone(), NODE_A);
        record(*target.id(), delegate(212), target, release.clone(), NODE_A);
        record(*key(213).id(), delegate(211), key(213), release, NODE_A);

        release_contract(target.id());

        assert_eq!(
            seen.lock().unwrap().len(),
            2,
            "both delegates' holds on the removed contract must be released"
        );
        assert!(
            DELEGATE_INTEREST_HOLDS.contains_key(&(*key(213).id(), delegate(211), NODE_A)),
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
        record(*key(220).id(), delegate(221), key(220), release, NODE_A);

        release_delegate(&delegate(222));
        release_contract(key(223).id());

        assert!(
            seen.lock().unwrap().is_empty(),
            "no hold matched, so nothing may be released"
        );
        assert!(
            DELEGATE_INTEREST_HOLDS.contains_key(&(*key(220).id(), delegate(221), NODE_A)),
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
        record(
            *key(230).id(),
            delegate(231),
            key(230),
            release.clone(),
            NODE_A,
        );
        record(*key(230).id(), delegate(231), key(230), release, NODE_A);

        release_delegate(&delegate(231));

        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "one obligation must produce exactly one release"
        );
    }
}
