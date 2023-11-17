use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::Instrument;

use crate::{
    config::GlobalExecutor,
    contract::{self, ContractHandler, MemoryContractHandler},
    dev_tool::{ClientEventsProxy, NodeConfig},
    node::{
        network_bridge::{inter_process::InterProcessConnManager, EventLoopNotifications},
        EventRegister, NetEventRegister, OpManager,
    },
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
        // disable console logging but enable trace collector (if it's enabled)
        std::env::set_var("FREENET_DISABLE_LOGS", "1");
        crate::config::set_logger();

        let event_register = {
            #[cfg(feature = "trace-ot")]
            {
                use crate::node::network_event_log::OTEventRegister;
                crate::node::CombinedRegister::new([
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
