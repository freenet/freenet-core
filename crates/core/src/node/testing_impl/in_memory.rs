use std::{collections::HashMap, sync::Arc};

use freenet_stdlib::prelude::*;
use tracing::Instrument;

use crate::{
    client_events::ClientEventsProxy,
    config::GlobalExecutor,
    contract::{self, executor_channel, ContractHandler, MemoryContractHandler},
    node::{
        network_bridge::{in_memory::MemoryConnManager, EventLoopNotifications},
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
        let is_gateway = self.config.local_ip.zip(self.config.local_port).is_some();

        let (notification_channel, notification_tx) = EventLoopNotifications::channel();
        let (ops_ch_channel, ch_channel) = contract::contract_handler_channel();

        let op_storage = Arc::new(OpManager::new(
            notification_tx,
            ops_ch_channel,
            &self.config,
            &gateways,
            self.event_register.clone(),
        )?);
        let (_executor_listener, executor_sender) = executor_channel(op_storage.clone());
        let contract_handler =
            MemoryContractHandler::build(ch_channel, executor_sender, self.ch_builder)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

        let conn_manager = MemoryConnManager::new(
            self.peer_key,
            self.event_register.trait_clone(),
            op_storage.clone(),
            self.add_noise,
        );

        GlobalExecutor::spawn(
            contract::contract_handling(contract_handler)
                .instrument(tracing::info_span!(parent: parent_span.clone(), "contract_handling")),
        );
        let mut config = super::RunnerConfig {
            peer_key: self.peer_key,
            is_gateway,
            gateways,
            parent_span: Some(parent_span),
            op_storage,
            conn_manager,
            user_events: Some(user_events),
            notification_channel,
            event_register: self.event_register.trait_clone(),
        };
        config
            .append_contracts(self.contracts, self.contract_subscribers)
            .await?;
        super::run_node(config).await
    }

    #[cfg(test)]
    pub fn append_contracts(
        &mut self,
        contracts: Vec<(ContractContainer, WrappedState)>,
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
        contracts: Vec<(ContractContainer, WrappedState)>,
        contract_subscribers: HashMap<ContractKey, Vec<PeerKeyLocation>>,
    ) -> Result<(), anyhow::Error> {
        use crate::contract::ContractHandlerEvent;
        for (contract, state) in contracts {
            let key: ContractKey = contract.key();
            let parameters = contract.params();
            self.op_storage
                .notify_contract_handler(ContractHandlerEvent::Cache(contract.clone()), None)
                .await?;
            self.op_storage
                .notify_contract_handler(
                    ContractHandlerEvent::PutQuery {
                        key: key.clone(),
                        state,
                        related_contracts: RelatedContracts::default(),
                        parameters: Some(parameters),
                    },
                    None,
                )
                .await?;
            tracing::debug!(
                "Appended contract {} to peer {}",
                key,
                self.op_storage.ring.peer_key
            );
            self.op_storage.ring.contract_cached(&key);
            if let Some(subscribers) = contract_subscribers.get(&key) {
                // add contract subscribers
                for subscriber in subscribers {
                    if self
                        .op_storage
                        .ring
                        .add_subscriber(&key, *subscriber)
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
