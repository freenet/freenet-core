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
    /// call; `false` means a per-contract cap refused it.
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

    /// The registry itself, for the pre-existing delegate tests that drive it
    /// directly. Named as it used to be (`native_api::DELEGATE_SUBSCRIPTIONS`)
    /// so those tests keep reading the same way; production code cannot reach
    /// it, which is the property that matters.
    pub(crate) use super::REGISTRY as DELEGATE_SUBSCRIPTIONS;

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
