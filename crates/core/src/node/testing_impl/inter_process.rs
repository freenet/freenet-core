use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::Instrument;

use crate::{
    config::GlobalExecutor,
    contract::{self, ContractHandler, MemoryContractHandler},
    dev_tool::{ClientEventsProxy, NodeConfig},
    node::{
        network_bridge::{inter_process::InterProcessConnManager, EventLoopNotifications},
        OpManager,
    },
    tracing::{EventRegister, NetEventRegister},
};

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct SimPeer {
    pub(super) config: NodeConfig,
}

impl SimPeer {
    pub async fn start_child<UsrEv>(self, event_generator: UsrEv) -> Result<(), anyhow::Error>
    where
        UsrEv: ClientEventsProxy + Send + 'static,
    {
        let event_register = {
            #[cfg(feature = "trace-ot")]
            {
                use crate::tracing::{CombinedRegister, OTEventRegister};
                CombinedRegister::new([
                    Box::new(EventRegister::new()),
                    Box::new(OTEventRegister::new()),
                ])
            }
            #[cfg(not(feature = "trace-ot"))]
            {
                EventRegister::new()
            }
        };
        self.run_node(event_generator, event_register).await
    }

    async fn run_node<UsrEv, ER>(
        self,
        event_generator: UsrEv,
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

        let op_manager = Arc::new(OpManager::new(
            notification_tx,
            ops_ch_channel,
            &self.config,
            &gateways,
            event_register.clone(),
        )?);
        let (_executor_listener, executor_sender) = contract::executor_channel(op_manager.clone());
        let contract_handler = MemoryContractHandler::build(
            ch_channel,
            executor_sender,
            self.config.peer_id.to_string(),
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

        let conn_manager = InterProcessConnManager::new(event_register.clone(), op_manager.clone());

        GlobalExecutor::spawn(
            contract::contract_handling(contract_handler)
                .instrument(tracing::info_span!("contract_handling", peer = %self.config.peer_id)),
        );

        let running_node = super::RunnerConfig {
            peer_key: self.config.peer_id,
            op_manager,
            gateways,
            notification_channel,
            conn_manager,
            event_register: Box::new(event_register),
            is_gateway,
            user_events: Some(event_generator),
            parent_span: None,
        };
        super::run_node(running_node).await
    }
}

impl From<NodeConfig> for SimPeer {
    fn from(config: NodeConfig) -> Self {
        Self { config }
    }
}
