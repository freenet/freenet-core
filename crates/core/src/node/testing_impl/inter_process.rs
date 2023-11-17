use std::sync::Arc;

use tracing::Instrument;

use crate::{
    config::GlobalExecutor,
    contract::{self, ContractHandler, MemoryContractHandler},
    dev_tool::{ClientEventsProxy, NodeConfig, PeerId},
    node::{
        network_bridge::{inter_process::InterProcessConnManager, EventLoopNotifications},
        NetEventRegister, OpManager,
    },
};

pub struct SimPeer {
    pub(super) config: NodeConfig,
}

impl SimPeer {
    pub fn peer_key(&self) -> PeerId {
        self.config.peer_id
    }
}

impl SimPeer {
    pub(crate) async fn run_node<UsrEv, ER>(
        self,
        user_events: UsrEv,
        event_register: ER,
    ) -> Result<(), anyhow::Error>
    where
        UsrEv: ClientEventsProxy + Send + 'static,
        ER: NetEventRegister + Send + Clone + 'static,
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
            event_register.clone(),
        )?);
        let (_executor_listener, executor_sender) = contract::executor_channel(op_storage.clone());
        let contract_handler = MemoryContractHandler::build(
            ch_channel,
            executor_sender,
            self.config.peer_id.to_string(),
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

        let conn_manager = InterProcessConnManager::new();

        GlobalExecutor::spawn(
            contract::contract_handling(contract_handler)
                .instrument(tracing::info_span!("contract_handling")),
        );

        let running_node = super::RunnerConfig {
            peer_key: self.config.peer_id,
            op_storage,
            gateways,
            notification_channel,
            conn_manager,
            event_register: Box::new(event_register),
            is_gateway,
            user_events: Some(user_events),
            parent_span: None,
        };
        super::run_node(running_node).await
    }
}
