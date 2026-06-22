//! Subscription and network-fetch helpers for `Executor<Runtime>`.
//!
//! `subscribe` drives the executor-side SUBSCRIBE operation, and
//! `local_state_or_from_network` is the local-then-network state lookup the
//! PUT/validation paths use to resolve a contract's current state.

use super::*;

impl Executor<Runtime> {
    pub(super) async fn subscribe(&mut self, key: ContractKey) -> Result<(), ExecutorError> {
        if self.mode == OperationMode::Local {
            return Ok(());
        }
        let op_manager = self
            .op_manager
            .as_ref()
            .ok_or_else(|| ExecutorError::other(anyhow::anyhow!("missing op_manager")))?;
        let executor_tx = crate::message::Transaction::new::<operations::subscribe::SubscribeMsg>();
        // Caps total task lifetime — the inner driver's per-attempt
        // `OPERATION_TTL = 60 s` would otherwise allow multi-attempt
        // waits to compound. Any change here should be checked against
        // the per-attempt budget so `MAX_RETRIES` attempts can complete
        // within the deadline.
        const SUBSCRIBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
        match tokio::time::timeout(
            SUBSCRIBE_TIMEOUT,
            operations::subscribe::run_executor_subscribe(
                op_manager.clone(),
                *key.id(),
                executor_tx,
            ),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(ExecutorError::other(anyhow::anyhow!("{err}"))),
            Err(_) => Err(ExecutorError::other(anyhow::anyhow!(
                "executor subscribe timed out after {}s",
                SUBSCRIBE_TIMEOUT.as_secs()
            ))),
        }
    }

    #[inline]
    pub(super) async fn local_state_or_from_network(
        &mut self,
        id: &ContractInstanceId,
        return_contract_code: bool,
    ) -> Result<Either<WrappedState, operations::get::GetResult>, ExecutorError> {
        // Try to get locally if we have the full key
        if let Some(full_key) = self.lookup_key(id) {
            if let Ok(state) = self.state_store.get(&full_key).await {
                return Ok(Either::Left(state));
            }
        }
        // Fetch from network via the sub-op GET driver. The driver
        // delivers the resolved `GetResult` directly through a oneshot.
        let op_manager = self
            .op_manager
            .as_ref()
            .ok_or_else(|| ExecutorError::other(anyhow::anyhow!("missing op_manager")))?;
        let (_tx, rx) =
            operations::get::op_ctx_task::start_sub_op_get(op_manager, *id, return_contract_code);
        // Outer callers may wrap this with a tighter budget (e.g.,
        // `fetch_related_for_validation_network` uses
        // `RELATED_FETCH_TIMEOUT = 10s`); when that fires first the
        // receiver is dropped silently and the spawned sub-op task
        // continues until OPERATION_TTL exhausts the retry loop. No
        // leak (oneshot send-after-drop is graceful) — just a
        // longer-lived background task.
        const SUB_OP_FETCH_TIMEOUT: Duration = Duration::from_secs(120);
        let outcome = tokio::time::timeout(SUB_OP_FETCH_TIMEOUT, rx)
            .await
            .map_err(|_| {
                tracing::warn!(
                    contract = %id,
                    "sub-op GET timed out at executor"
                );
                ExecutorError::other(anyhow::anyhow!("sub-op GET timed out"))
            })?
            .map_err(|_| ExecutorError::other(anyhow::anyhow!("sub-op GET task dropped")))?;
        match outcome {
            operations::get::op_ctx_task::SubOpGetOutcome::Found(get_result) => {
                Ok(Either::Right(get_result))
            }
            operations::get::op_ctx_task::SubOpGetOutcome::NotFound(cause) => {
                Err(ExecutorError::other(anyhow::anyhow!(cause)))
            }
            operations::get::op_ctx_task::SubOpGetOutcome::Infra(err) => {
                Err(ExecutorError::other(err))
            }
        }
    }
}
