use std::{collections::HashMap, sync::Arc};

use freenet_stdlib::prelude::*;
use tracing::Instrument;

use crate::{
    client_events::ClientEventsProxy,
    config::GlobalExecutor,
    contract::{self, executor_channel, ContractHandler, MemoryContractHandler},
    node::{
        network_bridge::{event_loop_notification_channel, in_memory::MemoryConnManager},
        op_state_manager::OpManager,
        NetEventRegister, NetworkBridge,
    },
    ring::PeerKeyLocation,
};

use super::{Builder, RunnerConfig};

impl<ER> Builder<ER> {
    pub async fn run_node<UsrEv>(
        self,
        user_events: UsrEv,
        parent_span: tracing::Span,
    ) -> Result<(), anyhow::Error>
    where
        UsrEv: ClientEventsProxy + Send + 'static,
        ER: NetEventRegister + Clone,
    {
        let gateways = self.config.get_gateways()?;

        let (notification_channel, notification_tx) = event_loop_notification_channel();
        let (ops_ch_channel, ch_channel, wait_for_event) = contract::contract_handler_channel();

        let _guard = parent_span.enter();
        let op_manager = Arc::new(OpManager::new(
            notification_tx,
            ops_ch_channel,
            &self.config,
            self.event_register.clone(),
        )?);
        std::mem::drop(_guard);
        let (executor_listener, executor_sender) = executor_channel(op_manager.clone());
        let contract_handler =
            MemoryContractHandler::build(ch_channel, executor_sender, self.contract_handler_name)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

        let conn_manager = MemoryConnManager::new(
            self.peer_key.clone(),
            self.event_register.clone(),
            op_manager.clone(),
            self.add_noise,
        );

        GlobalExecutor::spawn(
            contract::contract_handling(contract_handler)
                .instrument(tracing::info_span!(parent: parent_span.clone(), "contract_handling")),
        );

        let mut config = super::RunnerConfig {
            peer_key: self.peer_key,
            gateways,
            parent_span: Some(parent_span),
            op_manager,
            conn_manager,
            user_events: Some(user_events),
            notification_channel,
            event_register: self.event_register.trait_clone(),
            executor_listener,
            client_wait_for_transaction: wait_for_event,
        };
        config
            .append_contracts(self.contracts, self.contract_subscribers)
            .await?;
        super::run_node(config).await
    }

    #[cfg(test)]
    pub fn append_contracts(
        &mut self,
        contracts: Vec<(ContractContainer, WrappedState, bool)>,
        contract_subscribers: std::collections::HashMap<ContractKey, Vec<PeerKeyLocation>>,
    ) {
        self.contracts.extend(contracts);
        self.contract_subscribers.extend(contract_subscribers);
    }
}

impl<NB, UsrEv> RunnerConfig<NB, UsrEv>
where
    NB: NetworkBridge,
    UsrEv: ClientEventsProxy + Send + 'static,
{
    async fn append_contracts(
        &mut self,
        contracts: Vec<(ContractContainer, WrappedState, bool)>,
        contract_subscribers: HashMap<ContractKey, Vec<PeerKeyLocation>>,
    ) -> Result<(), anyhow::Error> {
        use crate::contract::ContractHandlerEvent;
        for (contract, state, subscription) in contracts {
            let key: ContractKey = contract.key();
            self.op_manager
                .notify_contract_handler(ContractHandlerEvent::PutQuery {
                    key: key.clone(),
                    state,
                    related_contracts: RelatedContracts::default(),
                    contract: Some(contract),
                })
                .await?;
            tracing::debug!(
                "Appended contract {} to peer {}",
                key,
                self.op_manager.ring.get_peer_key().unwrap()
            );
            if subscription {
                self.op_manager.ring.seed_contract(key.clone());
            }
            if let Some(subscribers) = contract_subscribers.get(&key) {
                // add contract subscribers
                for subscriber in subscribers {
                    if self
                        .op_manager
                        .ring
                        .add_subscriber(&key, subscriber.clone())
                        .is_err()
                    {
                        tracing::warn!("Max subscribers for contract {} reached", key);
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
