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
//! network path uses (`contract::run_contract_op_off_loop`), which also covers
//! the local-hit case. Nothing here registers demand a live subscribe would
//! not: no synthetic client, no extra pin, nothing on `contract_in_use`.
//!
//! # Pacing, and why there is no thundering herd
//!
//! One background task per node, and it re-establishes pairs ONE AT A TIME,
//! so a restart never has more than one restore subscribe in flight. It waits
//! [`FIRST_ATTEMPT_DELAY`] (jittered) before the first attempt so the node has
//! joined the ring, spaces attempts by [`BETWEEN_ATTEMPTS`], and re-establishes
//! at most [`MAX_REESTABLISH_PER_STARTUP`] pairs per boot, chosen round-robin
//! across delegates in a random order each boot (see [`select_for_reestablish`]),
//! so no one delegate can take the whole budget and no fixed tail is starved
//! boot after boot. Pairs that fail are
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

/// Backoff between retry rounds: doubles from `FIRST_ATTEMPT_DELAY` up to this.
pub(crate) const MAX_ROUND_DELAY: Duration = Duration::from_secs(300);

/// Retry rounds per boot before giving up until the next boot. The delays
/// BETWEEN rounds sum to under an hour; each round additionally takes as long
/// as its attempts do, which is bounded by the subscribe driver's own retry
/// budget per pair, not by a timer here.
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
    }
    restored = select_for_reestablish(restored, MAX_REESTABLISH_PER_STARTUP);
    let shutdown = op_manager.ring.shutdown_token();
    // Fire-and-forget is deliberate: the task is bounded (MAX_ROUNDS), holds no
    // lock, and exits promptly on the node's shutdown token, so it cannot
    // outlive the node it serves.
    GlobalExecutor::spawn(async move {
        reestablish(op_manager, restored, shutdown).await;
    });
}

/// Choose at most `cap` pairs to re-establish: round-robin across delegates,
/// with both the delegate order and each delegate's pair order shuffled per
/// boot. Round-robin means one delegate at its full per-delegate cap cannot
/// take the whole budget from the others; shuffling means that when the budget
/// does bind, a different subset is chosen each boot instead of the same tail
/// (key order) being skipped forever.
fn select_for_reestablish(
    pairs: Vec<(ContractInstanceId, DelegateKey)>,
    cap: usize,
) -> Vec<(ContractInstanceId, DelegateKey)> {
    // A HashMap for lookup, but a Vec for order: HashMap iteration order is
    // per-process random, which would make simulation runs non-deterministic.
    let mut index: std::collections::HashMap<DelegateKey, usize> = std::collections::HashMap::new();
    let mut by_delegate: Vec<(DelegateKey, Vec<ContractInstanceId>)> = Vec::new();
    for (contract, delegate) in pairs {
        match index.get(&delegate) {
            Some(&i) => by_delegate[i].1.push(contract),
            None => {
                index.insert(delegate.clone(), by_delegate.len());
                by_delegate.push((delegate, vec![contract]));
            }
        }
    }
    shuffle(&mut by_delegate);
    for (_, contracts) in &mut by_delegate {
        shuffle(contracts);
    }
    let mut selected = Vec::new();
    let mut round = 0;
    while selected.len() < cap {
        let mut took_any = false;
        for (delegate, contracts) in &by_delegate {
            if let Some(contract) = contracts.get(round) {
                selected.push((*contract, delegate.clone()));
                took_any = true;
                if selected.len() == cap {
                    break;
                }
            }
        }
        if !took_any {
            break;
        }
        round += 1;
    }
    selected
}

/// Fisher-Yates over `GlobalRng`, so simulation runs stay deterministic.
fn shuffle<T>(items: &mut [T]) {
    for i in (1..items.len()).rev() {
        let j = GlobalRng::random_range(0..=i);
        items.swap(i, j);
    }
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
            // `biased;` justification (per `.claude/rules/code-style.md`):
            // shutdown is checked first so a node stopping mid-restore does not
            // start another subscribe. Neither arm can starve the other: each
            // select resolves once and is not in a loop over a stream.
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

    // NO outer timeout, deliberately. `run_executor_subscribe` takes its
    // `add_local_client` refcount part-way through (before an `.await` on the
    // local-hit path), so a timer cancelling it at the wrong moment would leave
    // a refcount no hold records, and the retry would take another. The driver
    // bounds itself (its own retry loop); the only cancellation left is node
    // shutdown, after which the refcount's `InterestManager` is gone anyway.
    let tx = crate::message::Transaction::new::<crate::operations::subscribe::SubscribeMsg>();
    let result =
        crate::operations::subscribe::run_executor_subscribe(op_manager.clone(), contract, tx)
            .await;
    finish_attempt(op_manager, contract, delegate, result)
}

/// Account for one finished restore subscribe. Split from the network call so
/// the hold accounting, which is where a leak or double release would live, is
/// testable without a network.
fn finish_attempt<E: std::fmt::Display>(
    op_manager: &Arc<OpManager>,
    contract: ContractInstanceId,
    delegate: &DelegateKey,
    result: Result<(), E>,
) -> Attempt {
    match result {
        Ok(()) => {
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
        Err(err) => {
            tracing::debug!(%contract, %delegate, error = %err,
                "Restored delegate subscription: subscribe failed, will retry");
            Attempt::Retry
        }
    }
}

/// `true` if the node is shutting down.
///
/// `biased;` justification (per `.claude/rules/code-style.md`): shutdown wins a
/// tie with the timer so a stopping node exits instead of starting one more
/// attempt. Both arms are single-shot futures, so there is nothing to starve.
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

    /// A real `OpManager` in local mode, no network. Mirrors the fixture in
    /// `executor::pool_tests::delegate_notification_wasm_tests`.
    async fn op_manager(id: &str) -> (Arc<OpManager>, Box<dyn std::any::Any>) {
        let config_args = crate::config::ConfigArgs {
            id: Some(id.to_string()),
            mode: Some(crate::contract::executor::OperationMode::Local),
            ..Default::default()
        };
        let node_config =
            crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
                .await
                .expect("build NodeConfig");
        let (notification_rx, notification_tx) = crate::node::event_loop_notification_channel();
        let (ops_ch_channel, ch_channel, wait_for_event) =
            crate::contract::contract_handler_channel();
        let connection_manager = crate::ring::ConnectionManager::new(&node_config);
        let (result_router_tx, result_router_rx) = tokio::sync::mpsc::channel(100);
        let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();
        let op_manager = Arc::new(
            OpManager::new(
                notification_tx,
                ops_ch_channel,
                &node_config,
                crate::tracing::DynamicRegister::new(vec![]),
                connection_manager,
                result_router_tx,
                &task_monitor,
            )
            .expect("build OpManager"),
        );
        op_manager.ring.attach_op_manager(&op_manager);
        let guards: Box<dyn std::any::Any> = Box::new((
            notification_rx,
            ch_channel,
            wait_for_event,
            result_router_rx,
            task_monitor,
        ));
        (op_manager, guards)
    }

    fn pair(seed: u8) -> (ContractInstanceId, DelegateKey, ContractKey) {
        let mut id = [seed; 32];
        id[31] = 0xE7; // namespace away from other tests' ids
        let contract = ContractInstanceId::new(id);
        let delegate = DelegateKey::new([seed; 32], CodeHash::new([0xE7; 32]));
        let key = ContractKey::from_id_and_code(contract, CodeHash::new([0u8; 32]));
        (contract, delegate, key)
    }

    /// How many `add_local_client` refcounts `key` holds, by draining them.
    fn drain_local_clients(op: &OpManager, key: &ContractKey) -> usize {
        let mut n = 0;
        while op.interest_manager.has_local_interest(key) && n < 16 {
            op.interest_manager.remove_local_client(key);
            n += 1;
        }
        n
    }

    fn cleanup(contract: &ContractInstanceId, delegate: &DelegateKey) {
        delegate_interest::release_pair(contract, delegate);
        delegate_subscriptions::remove_delegate(
            delegate,
            delegate_subscriptions::Durability::InMemoryOnly,
        );
    }

    #[tokio::test]
    async fn a_pair_removed_since_boot_is_moot_and_takes_nothing() {
        let (op, _g) = op_manager("restore-moot").await;
        let (contract, delegate, key) = pair(1);
        assert_eq!(
            reestablish_one(&op, contract, &delegate).await,
            Attempt::Moot
        );
        assert_eq!(drain_local_clients(&op, &key), 0);
        assert!(!delegate_interest::holds(
            &contract,
            &delegate,
            op.node_identity
        ));
    }

    #[tokio::test]
    async fn a_pair_the_delegate_already_resubscribed_takes_no_second_refcount() {
        let (op, _g) = op_manager("restore-already-held").await;
        let (contract, delegate, key) = pair(2);
        delegate_subscriptions::subscribe(
            contract,
            &delegate,
            delegate_subscriptions::Durability::InMemoryOnly,
        );
        // The live path took its refcount and recorded it.
        op.interest_manager.add_local_client(&key);
        assert!(delegate_interest::record(
            contract,
            delegate.clone(),
            key,
            crate::contract::delegate_interest_release_closure(&op),
            op.node_identity,
        ));
        assert_eq!(
            reestablish_one(&op, contract, &delegate).await,
            Attempt::Established
        );
        assert_eq!(
            drain_local_clients(&op, &key),
            1,
            "exactly the live path's one"
        );
        cleanup(&contract, &delegate);
    }

    /// A successful restore subscribe (which took one refcount) records exactly
    /// one hold, and the ordinary removal path gives that refcount back.
    #[tokio::test]
    async fn a_successful_restore_records_one_hold_that_removal_releases() {
        let (op, _g) = op_manager("restore-success").await;
        let (contract, delegate, key) = pair(3);
        delegate_subscriptions::subscribe(
            contract,
            &delegate,
            delegate_subscriptions::Durability::InMemoryOnly,
        );
        op.interest_manager.add_local_client(&key); // what the subscribe took
        assert_eq!(
            finish_attempt::<String>(&op, contract, &delegate, Ok(())),
            Attempt::Established
        );
        assert!(delegate_interest::holds(
            &contract,
            &delegate,
            op.node_identity
        ));
        // UnregisterDelegate's release path.
        delegate_interest::release_delegate(&delegate);
        assert!(
            !op.interest_manager.has_local_interest(&key),
            "the restored refcount must be released by the ordinary removal path"
        );
        cleanup(&contract, &delegate);
    }

    /// A live re-subscribe recorded the hold while the restore subscribe was in
    /// flight: the restore's refcount is the extra one and is given back.
    #[tokio::test]
    async fn a_restore_that_loses_the_race_gives_back_its_refcount() {
        let (op, _g) = op_manager("restore-race").await;
        let (contract, delegate, key) = pair(4);
        delegate_subscriptions::subscribe(
            contract,
            &delegate,
            delegate_subscriptions::Durability::InMemoryOnly,
        );
        op.interest_manager.add_local_client(&key); // live path's
        delegate_interest::record(
            contract,
            delegate.clone(),
            key,
            crate::contract::delegate_interest_release_closure(&op),
            op.node_identity,
        );
        op.interest_manager.add_local_client(&key); // restore's
        assert_eq!(
            finish_attempt::<String>(&op, contract, &delegate, Ok(())),
            Attempt::Established
        );
        assert_eq!(
            drain_local_clients(&op, &key),
            1,
            "one subscriber, one refcount"
        );
        cleanup(&contract, &delegate);
    }

    /// Removed (e.g. UnregisterDelegate) while the restore subscribe was in
    /// flight, so the removal's release found nothing: the restore discharges
    /// its own refcount rather than leaving demand nothing can retire.
    #[tokio::test]
    async fn a_restore_for_a_pair_removed_mid_flight_releases_its_refcount() {
        let (op, _g) = op_manager("restore-removed-mid-flight").await;
        let (contract, delegate, key) = pair(5);
        op.interest_manager.add_local_client(&key); // restore's
        assert_eq!(
            finish_attempt::<String>(&op, contract, &delegate, Ok(())),
            Attempt::Moot
        );
        assert!(!op.interest_manager.has_local_interest(&key));
        assert!(!delegate_interest::holds(
            &contract,
            &delegate,
            op.node_identity
        ));
    }

    #[tokio::test]
    async fn a_failed_restore_records_nothing_and_retries() {
        let (op, _g) = op_manager("restore-failed").await;
        let (contract, delegate, key) = pair(6);
        delegate_subscriptions::subscribe(
            contract,
            &delegate,
            delegate_subscriptions::Durability::InMemoryOnly,
        );
        assert_eq!(
            finish_attempt(&op, contract, &delegate, Err("no peers")),
            Attempt::Retry
        );
        assert!(!delegate_interest::holds(
            &contract,
            &delegate,
            op.node_identity
        ));
        assert_eq!(drain_local_clients(&op, &key), 0);
        cleanup(&contract, &delegate);
    }

    /// One delegate at its full cap cannot take the whole per-boot budget.
    #[test]
    fn selection_is_round_robin_across_delegates() {
        let heavy_a = DelegateKey::new([0xA1; 32], CodeHash::new([0xA1; 32]));
        let heavy_b = DelegateKey::new([0xB2; 32], CodeHash::new([0xB2; 32]));
        let light = DelegateKey::new([0xC3; 32], CodeHash::new([0xC3; 32]));
        let mut pairs = Vec::new();
        for i in 0..300u16 {
            let mut id = [0u8; 32];
            id[..2].copy_from_slice(&i.to_le_bytes());
            pairs.push((ContractInstanceId::new(id), heavy_a.clone()));
            id[2] = 1;
            pairs.push((ContractInstanceId::new(id), heavy_b.clone()));
        }
        for i in 0..5u16 {
            let mut id = [0xFF; 32];
            id[..2].copy_from_slice(&i.to_le_bytes());
            pairs.push((ContractInstanceId::new(id), light.clone()));
        }
        let selected = select_for_reestablish(pairs, 512);
        assert_eq!(selected.len(), 512);
        let count = |d: &DelegateKey| selected.iter().filter(|(_, k)| k == d).count();
        assert_eq!(count(&light), 5, "the light delegate must not be starved");
        assert!(count(&heavy_a).abs_diff(count(&heavy_b)) <= 1);
        // Under the cap, everything is selected.
        let small = select_for_reestablish(selected[..10].to_vec(), 512);
        assert_eq!(small.len(), 10);
    }

    #[test]
    fn jitter_stays_within_twenty_percent() {
        for _ in 0..1000 {
            let j = jitter(Duration::from_secs(100));
            assert!(j >= Duration::from_secs(80) && j <= Duration::from_secs(120));
        }
    }

    /// The delay schedule between rounds is finite and capped, so a
    /// permanently failing pair costs at most `MAX_ROUNDS` attempts per boot
    /// (the attempts' own duration is bounded by the subscribe driver).
    #[test]
    fn inter_round_delay_schedule_is_bounded() {
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
