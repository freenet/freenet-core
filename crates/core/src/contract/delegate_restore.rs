//! Re-establish the delegate subscriptions restored from disk at startup
//! (#5493).
//!
//! [`delegate_subscriptions::restore_from_storage`] puts each persisted
//! `(contract, delegate)` pair back into the in-memory registry, which is what
//! routes a contract's state changes to the delegate as `ContractNotification`s.
//! A LIVE subscribe also takes two more things, and a restored one must take
//! exactly the same two, no more:
//!
//!  1. a network subscription, so updates to the contract reach this node;
//!  2. one `InterestManager::add_local_client` refcount, recorded in
//!     [`delegate_interest`] so the ordinary removal paths
//!     (`UnregisterDelegate`, contract removal) give it back.
//!
//! Both come from `run_executor_subscribe`, the same entry point the live
//! network path uses (`contract::run_delegate_contract_op`), which also covers
//! the local-hit case. Nothing here registers demand a live subscribe would
//! not: no synthetic client, no extra pin, nothing on `contract_in_use`.
//!
//! # Pacing, and why there is no thundering herd
//!
//! One background task per node, and it re-establishes pairs ONE AT A TIME,
//! so a restart never has more than one restore subscribe in flight. It waits
//! [`FIRST_ATTEMPT_DELAY`] (jittered) before the first attempt so the node has
//! joined the ring, spaces attempts by [`BETWEEN_ATTEMPTS`], and re-establishes
//! at most [`MAX_REESTABLISH_PER_STARTUP`] pairs per boot. Pairs that fail are
//! retried in later rounds with jittered exponential backoff, at most
//! [`MAX_ROUNDS`] rounds, and then left alone until the next boot. A failure
//! never removes the subscription: the registry entry and the persisted row
//! stay, so local commits still notify the delegate and the next boot tries
//! again.
//!
//! [`delegate_subscriptions::restore_from_storage`]: crate::wasm_runtime::delegate_subscriptions::restore_from_storage

use std::sync::Arc;
use std::time::Duration;

use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey, DelegateKey};
use tokio_util::sync::CancellationToken;

use crate::config::{GlobalExecutor, GlobalRng};
use crate::node::OpManager;
use crate::wasm_runtime::{delegate_interest, delegate_subscriptions};

/// Delay before the first attempt, so the node has had time to join the ring.
/// An attempt before joining fails with `PeerNotJoined` and just burns a round.
pub(crate) const FIRST_ATTEMPT_DELAY: Duration = Duration::from_secs(15);

/// Spacing between consecutive restore subscribes within a round.
pub(crate) const BETWEEN_ATTEMPTS: Duration = Duration::from_millis(250);

/// Upper bound on one restore subscribe, a backstop for a wedged driver. Same
/// order as the live delegate path's `PARK_WORK_BUDGET` (75 s).
pub(crate) const ATTEMPT_BUDGET: Duration = Duration::from_secs(60);

/// Backoff between retry rounds: doubles from `FIRST_ATTEMPT_DELAY` up to this.
pub(crate) const MAX_ROUND_DELAY: Duration = Duration::from_secs(300);

/// Retry rounds per boot before giving up until the next boot. With the
/// delays above that is roughly 40 minutes of attempts.
pub(crate) const MAX_ROUNDS: u32 = 10;

/// Most pairs re-established over the network per boot.
///
/// The persisted set is itself bounded
/// (`MAX_DURABLE_DELEGATE_SUBSCRIPTIONS`); this is the tighter bound on how
/// much network work one restart can cause. Pairs past it are still restored
/// into the registry (local commits notify the delegate) and still persisted,
/// so a later boot or the delegate's own re-subscribe covers them.
pub(crate) const MAX_REESTABLISH_PER_STARTUP: usize = 512;

/// What one attempt concluded.
#[derive(Debug, PartialEq, Eq)]
enum Attempt {
    /// Interest and network subscription are in place (or were already).
    Established,
    /// Nothing to do any more: the subscription was removed since boot, or the
    /// contract is banned.
    Moot,
    /// Failed; try again in a later round.
    Retry,
}

/// Spawn the background task that re-establishes `restored`. A no-op when
/// nothing was restored, so a node with no delegate subscriptions spawns
/// nothing.
pub(crate) fn spawn_reestablish(
    op_manager: Arc<OpManager>,
    mut restored: Vec<(ContractInstanceId, DelegateKey)>,
) {
    if restored.is_empty() {
        return;
    }
    if restored.len() > MAX_REESTABLISH_PER_STARTUP {
        tracing::warn!(
            restored = restored.len(),
            cap = MAX_REESTABLISH_PER_STARTUP,
            "More restored delegate subscriptions than the per-boot re-establish cap; \
             the rest stay registered for local notifications but are not \
             re-subscribed on the network this boot"
        );
        restored.truncate(MAX_REESTABLISH_PER_STARTUP);
    }
    let shutdown = op_manager.ring.shutdown_token();
    // Fire-and-forget is deliberate: the task is bounded (MAX_ROUNDS), holds no
    // lock, and exits promptly on the node's shutdown token, so it cannot
    // outlive the node it serves.
    GlobalExecutor::spawn(async move {
        reestablish(op_manager, restored, shutdown).await;
    });
}

async fn reestablish(
    op_manager: Arc<OpManager>,
    mut pending: Vec<(ContractInstanceId, DelegateKey)>,
    shutdown: CancellationToken,
) {
    let total = pending.len();
    let mut delay = FIRST_ATTEMPT_DELAY;
    for round in 0..MAX_ROUNDS {
        if sleep_or_shutdown(&shutdown, jitter(delay)).await {
            return;
        }
        let mut failed = Vec::new();
        for (i, (contract, delegate)) in pending.into_iter().enumerate() {
            if i > 0 && sleep_or_shutdown(&shutdown, BETWEEN_ATTEMPTS).await {
                return;
            }
            let outcome = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                outcome = reestablish_one(&op_manager, contract, &delegate) => outcome,
            };
            if outcome == Attempt::Retry {
                failed.push((contract, delegate));
            }
        }
        if failed.is_empty() {
            tracing::info!(
                total,
                rounds = round + 1,
                "Re-established restored delegate subscriptions"
            );
            return;
        }
        tracing::debug!(
            round,
            remaining = failed.len(),
            "Some restored delegate subscriptions could not be re-established yet"
        );
        pending = failed;
        delay = (delay * 2).min(MAX_ROUND_DELAY);
    }
    tracing::warn!(
        total,
        remaining = pending.len(),
        rounds = MAX_ROUNDS,
        "Gave up re-establishing some restored delegate subscriptions this boot; \
         they remain registered and persisted, and are retried at the next boot"
    );
}

/// Re-establish one pair: run the same subscribe a live delegate subscribe
/// runs, then record its refcount exactly once.
async fn reestablish_one(
    op_manager: &Arc<OpManager>,
    contract: ContractInstanceId,
    delegate: &DelegateKey,
) -> Attempt {
    if !delegate_subscriptions::is_subscribed(&contract, delegate) {
        // Removed since boot (UnregisterDelegate, contract removal, or evicted
        // by the delegate's own later subscribes). Nothing to re-establish.
        return Attempt::Moot;
    }
    if delegate_interest::holds(&contract, delegate, op_manager.node_identity) {
        // The delegate re-subscribed on its own since boot and the live path
        // already took this node's refcount. Taking another would double it.
        return Attempt::Established;
    }
    if crate::operations::reject_if_contract_banned(op_manager, &contract).is_err() {
        // A live subscribe refuses a banned contract; so does restore. The
        // registry entry and persisted row stay, in case the ban lifts.
        tracing::info!(
            %contract,
            %delegate,
            "Not re-establishing a restored delegate subscription: contract is banned"
        );
        return Attempt::Moot;
    }

    let tx = crate::message::Transaction::new::<crate::operations::subscribe::SubscribeMsg>();
    let result = tokio::time::timeout(
        ATTEMPT_BUDGET,
        crate::operations::subscribe::run_executor_subscribe(op_manager.clone(), contract, tx),
    )
    .await;
    match result {
        Ok(Ok(())) => {
            // `add_local_client` is keyed on the instance id alone
            // (`ContractKey`'s Hash/Eq are instance-only), so an instance-only
            // key releases exactly the entry the subscribe created. Same
            // device as the live network path's fallback key.
            let key = ContractKey::from_id_and_code(contract, CodeHash::new([0u8; 32]));
            if !delegate_interest::record(
                contract,
                delegate.clone(),
                key,
                super::delegate_interest_release_closure(op_manager),
                op_manager.node_identity,
            ) {
                // A live re-subscribe recorded this node's obligation while we
                // were subscribing; ours is the extra refcount.
                op_manager.interest_manager.remove_local_client(&key);
            }
            // Removed while we were subscribing? Its release ran before our
            // record, so discharge ours now. `release_pair` removes the entry
            // atomically, so if the removal's release ran after our record it
            // already took it and this is a no-op: never released twice.
            if !delegate_subscriptions::is_subscribed(&contract, delegate) {
                delegate_interest::release_pair(&contract, delegate);
                return Attempt::Moot;
            }
            Attempt::Established
        }
        Ok(Err(err)) => {
            tracing::debug!(%contract, %delegate, error = %err,
                "Restored delegate subscription: subscribe failed, will retry");
            Attempt::Retry
        }
        Err(_) => {
            tracing::debug!(%contract, %delegate,
                "Restored delegate subscription: subscribe timed out, will retry");
            Attempt::Retry
        }
    }
}

/// `true` if the node is shutting down.
async fn sleep_or_shutdown(shutdown: &CancellationToken, wait: Duration) -> bool {
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => true,
        _ = tokio::time::sleep(wait) => false,
    }
}

/// ±20% jitter, per `.claude/rules/code-style.md`, so nodes restarted together
/// (a release rollout) do not re-subscribe in lockstep.
fn jitter(d: Duration) -> Duration {
    d.mul_f64(GlobalRng::random_range(0.8f64..1.2f64))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Body of the first `async fn build(` in handler.rs, which is
    /// `NetworkContractHandler::build`, with comment lines removed so a
    /// commented-out call cannot satisfy the pin.
    fn network_handler_build_body() -> String {
        let src = include_str!("handler.rs");
        let start = src.find("async fn build(").expect("build() not found");
        let open = start + src[start..].find('{').expect("build() has no body");
        let mut depth = 0i32;
        let mut end = None;
        for (i, b) in src.as_bytes()[open..].iter().enumerate() {
            match b {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(open + i);
                        break;
                    }
                }
                _ => {}
            }
        }
        src[open..end.expect("unbalanced braces in build()")]
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Wiring pin: the restore must actually run at startup, and in the right
    /// place. After `rehydrate_local_hosting_interest` (so a restored subscribe
    /// sees locally hosted contracts as local) and inside `build`, which returns
    /// before the event loop exists (so no delegate runs while its
    /// subscriptions are missing). The behavioural restart test in
    /// `delegate_notification_wasm_tests` proves `restore_from_storage` works;
    /// this proves the node calls it.
    ///
    /// Mutation-checked: deleting or commenting out either call, or moving the
    /// restore above the rehydrate, fails this test.
    #[test]
    fn network_handler_build_restores_delegate_subscriptions() {
        let body = network_handler_build_body();
        let rehydrate = body
            .find("rehydrate_local_hosting_interest()")
            .expect("build() must rehydrate local hosting interest");
        let restore = body
            .find("delegate_subscriptions::restore_from_storage(")
            .expect("build() must restore persisted delegate subscriptions (#5493)");
        let spawn = body
            .find("delegate_restore::spawn_reestablish(")
            .expect("build() must re-establish restored delegate subscriptions (#5493)");
        assert!(
            rehydrate < restore,
            "restore must run after the hosting cache/interest are rehydrated"
        );
        assert!(
            restore < spawn,
            "re-establish what was restored, after restoring it"
        );
    }

    #[test]
    fn jitter_stays_within_twenty_percent() {
        for _ in 0..1000 {
            let j = jitter(Duration::from_secs(100));
            assert!(j >= Duration::from_secs(80) && j <= Duration::from_secs(120));
        }
    }

    /// The per-boot network work is bounded: the round schedule is finite and
    /// the backoff is capped, so a permanently failing pair costs at most
    /// `MAX_ROUNDS` attempts per boot.
    #[test]
    fn retry_schedule_is_bounded() {
        let mut delay = FIRST_ATTEMPT_DELAY;
        let mut total = Duration::ZERO;
        for _ in 0..MAX_ROUNDS {
            total += delay;
            delay = (delay * 2).min(MAX_ROUND_DELAY);
        }
        assert!(delay <= MAX_ROUND_DELAY);
        assert!(total < Duration::from_secs(60 * 60), "schedule {total:?}");
    }
}
