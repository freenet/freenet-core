//! The one place a delegate's contract subscription is written or cleared.
//!
//! A delegate subscription has **two representations** and they must never
//! disagree:
//!
//! 1. the in-memory registry read by `ContractNotification` delivery
//!    (`Executor::send_delegate_contract_notifications`), and
//! 2. a durable row in redb's `DELEGATE_SUBSCRIPTIONS_TABLE`, so the
//!    subscription — and therefore the hosting PIN it now carries — survives a
//!    node restart (#4669 part 2 / #5467).
//!
//! # Why durability is not optional any more
//!
//! Before #4669 part 1 the registry drove notification delivery and nothing
//! else, and losing it on restart cost a delegate one notification round. Since
//! part 1 a subscription also registers hosting demand, and **a delegate only
//! runs when something invokes it**. The thing that would invoke it is a
//! notification on the contract whose pin the restart just dropped, so an
//! in-memory-only subscription cannot re-arm itself: the practical recovery is
//! "the user reopens the app", which is exactly the case the delegate exists to
//! cover. That reproduces the failure shape #5467 objects to — the subscribe
//! succeeded, notifications work, and nothing reports that the pin is gone.
//!
//! # Why this module exists rather than three mirrored call sites
//!
//! Three separate paths clear the in-memory registry — `UnregisterDelegate`
//! (`contract/executor/runtime/delegates.rs`), the notification channel closing
//! (`contract/executor/runtime/executor_impl.rs`) and contract removal
//! (`wasm_runtime/contract_store.rs`) — and two register it (the V1
//! `SubscribeContractRequest` arm in `contract.rs` and the V2
//! `subscribe_contract()` host function). A durable copy written beside each of
//! those is the "manually-mirrored state" shape in
//! `.claude/rules/bug-prevention-patterns.md`: the cleanup path that forgot it
//! leaves a durable row that is restored on every subsequent boot, pinning a
//! contract for a delegate that no longer exists — **a pin nothing will ever
//! release**, and one that is silent because the in-memory half looks correct.
//!
//! So the registry is private to this module and every mutation goes through
//! one of the four functions below, each of which writes both representations.
//! Adding a sixth call site cannot omit the durable half, because there is no
//! way to reach the registry without it.
//!
//! # The storage handle is a parameter, not a global
//!
//! Every caller passes its own `&Storage`. That is deliberate: the registry is
//! a process-global `static` (#4824) while a `Storage` belongs to one node, and
//! several nodes share one process in every `#[freenet_test]`. A global storage
//! handle here would send one node's durable writes to another node's database.
//! The parameter is `Option` only because the V2 host function's
//! `DelegateCallEnv` may have no state store at all (local-only and mock
//! runtimes); `None` degrades to exactly the pre-#4669-part-2 behaviour —
//! notifications work, the pin works for this process, and nothing survives a
//! restart.
//!
//! # Backend scope
//!
//! The durable half is **redb only**, like ten of redb's twelve tables. Under
//! `--no-default-features --features sqlite` every function below degrades to
//! the in-memory registry alone, i.e. to the behaviour this module exists to
//! fix. redb is the default feature and what ships.

use std::collections::HashSet;
use std::sync::LazyLock;

use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractInstanceId, DelegateKey};

/// Global registry of delegate subscriptions to contracts.
///
/// **Private on purpose.** See the module docs: every mutation must also reach
/// the durable copy, and the only way to guarantee that is for there to be no
/// other way in. Reads go through [`subscribers`]; tests that need to inspect
/// or reset it use [`test_support`].
static REGISTRY: LazyLock<DashMap<ContractInstanceId, HashSet<DelegateKey>>> =
    LazyLock::new(DashMap::default);

/// Record that `delegate` is subscribed to `contract`, durably where a storage
/// handle is available.
///
/// Returns whether the subscription is registered after this call. `false` only
/// when the per-contract cap
/// ([`crate::contract::storages::ReDb::MAX_DELEGATE_SUBSCRIPTIONS_PER_CONTRACT`])
/// refused it; in that case NEITHER representation records it, so the two
/// cannot disagree at the cap.
///
/// A durable write that fails for any other reason is logged and the in-memory
/// registration proceeds. Refusing the subscribe outright would be a
/// regression — notification delivery and the pin both still work for the life
/// of this process — and redb's own poison detection (`commit_guarded`) already
/// aborts the process for the failure class that must not be continued through.
pub(crate) fn register<S: DelegateSubscriptionPersistence + ?Sized>(
    db: Option<&S>,
    contract: &ContractInstanceId,
    delegate: &DelegateKey,
) -> bool {
    if !persist_add(db, contract, delegate) {
        return false;
    }
    REGISTRY
        .entry(*contract)
        .or_default()
        .insert(delegate.clone());
    true
}

/// The delegates subscribed to `contract`, or `None` if there are none.
///
/// Reads the in-memory registry, which boot restore has already reconciled
/// against the durable set — so this never has to touch disk on the
/// notification path.
pub(crate) fn subscribers(contract: &ContractInstanceId) -> Option<HashSet<DelegateKey>> {
    REGISTRY.get(contract).map(|entry| entry.value().clone())
}

/// Forget every delegate subscribed to `contract`, in both representations.
///
/// Returns the delegates that were subscribed, so the caller can retire the
/// matching demand without re-reading a map it has just emptied.
///
/// Called when the contract goes away (eviction, PUT rollback) and when the
/// delegate-notification channel closes. The durable half is a prefix range
/// scan — the row key is contract-major precisely so this teardown is cheap.
pub(crate) fn forget_contract<S: DelegateSubscriptionPersistence + ?Sized>(
    db: Option<&S>,
    contract: &ContractInstanceId,
) -> HashSet<DelegateKey> {
    persist_remove_contract(db, contract);
    REGISTRY
        .remove(contract)
        .map(|(_, subscribers)| subscribers)
        .unwrap_or_default()
}

/// Forget every subscription `delegate` holds, in both representations.
///
/// Returns the contracts it was subscribed to.
///
/// The durable rows are found through the in-memory registry rather than by
/// scanning the table: the registry is authoritative at runtime (boot restore
/// seeded it from the same table), so this costs one point delete per contract
/// the delegate actually held instead of a full table scan on every
/// `UnregisterDelegate`.
pub(crate) fn forget_delegate<S: DelegateSubscriptionPersistence + ?Sized>(
    db: Option<&S>,
    delegate: &DelegateKey,
) -> Vec<ContractInstanceId> {
    let mut affected = Vec::new();
    REGISTRY.retain(|contract, subscribers| {
        if subscribers.remove(delegate) {
            affected.push(*contract);
        }
        !subscribers.is_empty()
    });
    for contract in &affected {
        persist_remove_one(db, contract, delegate);
    }
    affected
}

/// Forget ONE `(contract, delegate)` subscription, in both representations.
///
/// Used by boot reconciliation to drop a durable row whose delegate or contract
/// no longer exists. Kept distinct from [`forget_delegate`] because
/// reconciliation runs before the registry is seeded, so there is nothing to
/// walk.
pub(crate) fn forget_one<S: DelegateSubscriptionPersistence + ?Sized>(
    db: Option<&S>,
    contract: &ContractInstanceId,
    delegate: &DelegateKey,
) {
    persist_remove_one(db, contract, delegate);
    if let Some(mut entry) = REGISTRY.get_mut(contract) {
        entry.remove(delegate);
    }
    REGISTRY.remove_if(contract, |_, subscribers| subscribers.is_empty());
}

/// Every `(contract, delegate)` subscription recorded on disk.
///
/// Read once, at boot, by [`crate::contract::handler`]'s restore step.
///
/// `Err` means the table could not be read, and the caller **must not** treat
/// that as "no subscriptions": doing so would silently drop every delegate's
/// pin on a transient read failure, which is the exact outcome this table
/// exists to prevent.
pub(crate) fn load_persisted<S: DelegateSubscriptionPersistence + ?Sized>(
    db: &S,
) -> Result<Vec<(ContractInstanceId, DelegateKey)>, String> {
    db.load_delegate_subscriptions()
}

/// Put a subscription back in the in-memory registry from a durable row that
/// has just been read, WITHOUT re-affirming that row.
///
/// The counterpart to [`register`], and the difference is the reason it exists.
/// `register` writes the durable row and stamps it as affirmed now; that is
/// right for a delegate calling subscribe and wrong for the node replaying its
/// own disk at boot. A stamp that boot restore refreshed would be re-affirmed
/// by restarts alone, so the row could never age out and the cleanup exemption
/// would be permanently refreshable, which `AGENTS.md` forbids.
///
/// Returns whether the subscription is registered after this call. `false` only
/// when the durable row has gone since it was read, which is not reachable at
/// boot; the caller warns rather than registering demand for a subscription the
/// store no longer holds (the same ordering rule `register` enforces at the
/// cap: no pin without a subscription record).
pub(crate) fn restore_registration<S: DelegateSubscriptionPersistence + ?Sized>(
    db: Option<&S>,
    contract: &ContractInstanceId,
    delegate: &DelegateKey,
) -> bool {
    let recorded = match db {
        Some(db) => db.delegate_subscription_is_recorded(contract, delegate),
        // No store means no durable row to contradict; the in-memory registry
        // is the whole record for this run.
        None => true,
    };
    if !recorded {
        return false;
    }
    REGISTRY
        .entry(*contract)
        .or_default()
        .insert(delegate.clone());
    true
}

// ---------------------------------------------------------------------------
// Durable half.
// ---------------------------------------------------------------------------

/// How a storage backend records delegate subscriptions durably.
///
/// A supertrait of [`crate::wasm_runtime::StateStorage`], so every state
/// backend answers the question and the functions above accept any of them —
/// a concrete `&ReDb`, a generic `&S`, or a `&dyn` — without a caller needing
/// to name a backend. **Every method defaults to a no-op**, which is the
/// honest answer for the sqlite backend and the mock: they degrade to the
/// in-memory registry alone, i.e. to the pre-#4669-part-2 behaviour. redb — the
/// default feature and what ships — overrides all four.
///
/// Object-safe on purpose: `ContractExecutor::delegate_subscription_store`
/// returns one of these through a trait object, because the serial
/// contract-handling loop reaches its executor generically.
pub trait DelegateSubscriptionPersistence: Send + Sync {
    /// Record `(contract, delegate)`. Returns whether it is recorded after the
    /// call; `false` means a cap refused it — per-contract or node-wide, and
    /// the caller does not need to know which, only that no row exists.
    fn persist_delegate_subscription(
        &self,
        _contract: &ContractInstanceId,
        _delegate: &DelegateKey,
    ) -> bool {
        true
    }

    /// Forget one `(contract, delegate)` row. Idempotent.
    fn forget_delegate_subscription(
        &self,
        _contract: &ContractInstanceId,
        _delegate: &DelegateKey,
    ) {
    }

    /// Forget every row for `contract`. Idempotent.
    fn forget_delegate_subscriptions_for_contract(&self, _contract: &ContractInstanceId) {}

    /// Every recorded `(contract, delegate)` pair, for boot restore.
    ///
    /// `Err` means the store could not be read. A caller must NOT treat that as
    /// "no subscriptions" — see [`load_persisted`].
    fn load_delegate_subscriptions(
        &self,
    ) -> Result<Vec<(ContractInstanceId, DelegateKey)>, String> {
        Ok(Vec::new())
    }

    /// Whether `(contract, delegate)` is recorded, WITHOUT affirming it.
    ///
    /// Boot restore uses this instead of [`Self::persist_delegate_subscription`]
    /// so replaying the durable set does not refresh the rows' last-affirmed
    /// stamps. See [`restore_registration`].
    fn delegate_subscription_is_recorded(
        &self,
        _contract: &ContractInstanceId,
        _delegate: &DelegateKey,
    ) -> bool {
        true
    }
}

fn persist_add<S: DelegateSubscriptionPersistence + ?Sized>(
    db: Option<&S>,
    contract: &ContractInstanceId,
    delegate: &DelegateKey,
) -> bool {
    match db {
        Some(db) => db.persist_delegate_subscription(contract, delegate),
        None => true,
    }
}

fn persist_remove_one<S: DelegateSubscriptionPersistence + ?Sized>(
    db: Option<&S>,
    contract: &ContractInstanceId,
    delegate: &DelegateKey,
) {
    if let Some(db) = db {
        db.forget_delegate_subscription(contract, delegate);
    }
}

fn persist_remove_contract<S: DelegateSubscriptionPersistence + ?Sized>(
    db: Option<&S>,
    contract: &ContractInstanceId,
) {
    if let Some(db) = db {
        db.forget_delegate_subscriptions_for_contract(contract);
    }
}

/// Direct registry access for tests that need to observe or reset it.
///
/// Production code MUST NOT use these — the whole point of the module is that
/// the two representations move together. The registry is process-global
/// (#4824) and the test binary runs cases in one process, so tests that assert
/// on it have to be able to clear it.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    use dashmap::DashMap;

    /// The registry itself, for the pre-existing delegate tests that drive it
    /// directly.
    ///
    /// A `#[cfg(test)]` FUNCTION rather than a wider visibility on the static:
    /// the static stays private in every build, so production code cannot reach
    /// it whatever it imports, which is the property this module exists for.
    pub(crate) fn registry() -> &'static DashMap<ContractInstanceId, HashSet<DelegateKey>> {
        &REGISTRY
    }

    /// Register in the IN-MEMORY registry only, with no durable write.
    ///
    /// This is what the pre-#4669-part-2 code did, and a restart test uses it
    /// to reproduce that behaviour deliberately.
    pub(crate) fn register_in_memory_only(contract: &ContractInstanceId, delegate: &DelegateKey) {
        REGISTRY
            .entry(*contract)
            .or_default()
            .insert(delegate.clone());
    }

    /// Drop every in-memory entry naming `delegate`, leaving disk alone.
    ///
    /// Models a process restart for one delegate: the durable rows survive, the
    /// registry does not.
    pub(crate) fn clear_in_memory_for(delegate: &DelegateKey) {
        REGISTRY.retain(|_, subscribers| {
            subscribers.remove(delegate);
            !subscribers.is_empty()
        });
    }

    /// Whether `delegate` is subscribed to `contract` in the in-memory registry.
    pub(crate) fn is_registered(contract: &ContractInstanceId, delegate: &DelegateKey) -> bool {
        REGISTRY
            .get(contract)
            .is_some_and(|entry| entry.contains(delegate))
    }
}

#[cfg(test)]
mod tests {
    /// Every mutation of `REGISTRY` lives in a writer that also writes the
    /// durable half. This pins the SET of them.
    ///
    /// # WHY A NAMES PIN RATHER THAN A BEHAVIOURAL TEST
    ///
    /// The guarantee this module provides is structural: the `static` is
    /// private, so nothing outside can reach it, and every writer inside writes
    /// both representations. A behavioural test can only exercise the paths
    /// that EXIST. It can never fail because someone ADDED a sixth one that
    /// forgot the durable half. This can.
    ///
    /// That is a live risk rather than a hypothetical. #5623 encapsulates this
    /// same static behind this same module for its own per-delegate cap, and
    /// merges before this branch. If its cap-eviction removes an entry through
    /// a new route instead of calling `forget_one`, the in-memory set shrinks,
    /// the durable row survives, the two diverge with no conflict marker and no
    /// failing test, and the row returns at the next boot restore. A
    /// clean-looking merge is the dangerous outcome here, not a messy one.
    ///
    /// # THE WINDOW FAILS CLOSED IN BOTH DIRECTIONS, AND ONLY ONE IS OBVIOUS
    ///
    /// `SOURCE.split(..).next()` always yields a part, so a missing anchor does
    /// not error, it hands back the WHOLE FILE including this test module. That
    /// is the fail-open shape worth worrying about here, and the
    /// `static REGISTRY:` assertion below does not cover it: that guards
    /// TRUNCATION, not EXPANSION.
    ///
    /// What covers expansion is the exact-set equality. The test module mutates
    /// `REGISTRY` itself, through `test_support`'s helpers, so an expanded
    /// window adds owners like `clear_in_memory_for` and
    /// `register_in_memory_only` and the assertion fails.
    ///
    /// Verified rather than reasoned: replacing the split anchor with one that
    /// cannot match reddens this test with nine owners instead of five. Written
    /// down because `.next()` invites exactly this doubt, and the next reader
    /// should not have to re-derive the answer.
    ///
    /// If this fails after a merge or rebase, do NOT just add the new name.
    /// Check first that the new path writes the durable half, by calling an
    /// existing writer or by persisting itself. Then add it.
    ///
    /// `restore_registration` is deliberately listed while NOT writing the
    /// durable half: it replays rows already on disk, and refreshing their
    /// stamps is exactly what must not happen (see its own docs). It is the one
    /// sanctioned mutation that only reads the durable side, and naming it here
    /// makes that a decision on the record rather than an omission.
    #[test]
    fn every_registry_mutation_lives_in_a_known_writer() {
        const SOURCE: &str = include_str!("delegate_subscriptions.rs");
        let production = SOURCE
            .split("\n#[cfg(test)]")
            .next()
            .expect("split always yields at least one part");
        assert!(
            production.contains("static REGISTRY:"),
            "the production window must contain the static itself. If this fires \
             the window is truncated and every assertion below is vacuous."
        );

        // Offset-based, NOT line-based. The mutations are written as
        //
        //     REGISTRY
        //         .entry(*contract)
        //
        // so a scan requiring `REGISTRY` and the operator on ONE line finds two
        // of the five writers and silently passes on a shorter list. The first
        // version of this test did exactly that, and only the explicit expected
        // set turned it from a green vacuous pin into a red one.
        const MUTATORS: [&str; 7] = [
            ".entry(",
            ".remove(",
            ".remove_if(",
            ".retain(",
            ".get_mut(",
            ".insert(",
            ".clear(",
        ];

        // Every `fn` header with its byte offset, so a mutation can be mapped
        // back to the function containing it. Track the offset explicitly while
        // walking lines; slicing from a newline and taking `.lines().next()`
        // yields an empty string, which is how the previous attempt found zero
        // headers and attributed everything to file scope.
        let mut headers: Vec<(usize, &str)> = Vec::new();
        let mut offset = 0usize;
        for line in production.lines() {
            let t = line.trim_start();
            if let Some(after) = t
                .strip_prefix("pub(crate) fn ")
                .or_else(|| t.strip_prefix("pub fn "))
                .or_else(|| t.strip_prefix("fn "))
            {
                let name = after.split(['(', '<']).next().unwrap_or(after);
                headers.push((offset, name));
            }
            offset += line.len() + 1;
        }

        let mut owners: Vec<&str> = Vec::new();
        let mut search = 0usize;
        while let Some(rel) = production[search..].find("REGISTRY") {
            let at = search + rel;
            search = at + "REGISTRY".len();
            // Bound the lookahead to this statement, so an unrelated later
            // call cannot be attributed to this occurrence.
            let stmt_end = production[at..]
                .find(';')
                .map(|e| at + e)
                .unwrap_or(production.len());
            let stmt = &production[at..stmt_end];
            if !MUTATORS.iter().any(|op| stmt.contains(op)) {
                continue;
            }
            let owner = headers
                .iter()
                .take_while(|(pos, _)| *pos < at)
                .last()
                .map(|(_, name)| *name)
                .unwrap_or("<file scope>");
            owners.push(owner);
        }
        owners.sort_unstable();
        owners.dedup();

        assert_eq!(
            owners,
            [
                "forget_contract",
                "forget_delegate",
                "forget_one",
                "register",
                "restore_registration",
            ],
            "the set of functions mutating REGISTRY changed. Every one must also \
             write the durable half, or the two representations diverge silently \
             and the row returns at the next boot. Verify the new path persists \
             BEFORE adding its name here."
        );
    }
}
