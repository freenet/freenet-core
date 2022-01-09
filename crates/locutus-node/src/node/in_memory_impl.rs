use std::sync::Arc;

use tokio::sync::mpsc::{self, Receiver};

use super::{
    conn_manager::in_memory::MemoryConnManager, event_listener::EventListener, handle_cancelled_op,
    join_ring_request, op_state::OpManager, process_message, user_event_handling, PeerKey,
};
use crate::{
    config::GlobalExecutor,
    contract::{
        self, Contract, ContractError, ContractHandler, ContractHandlerEvent, ContractValue,
        SimStoreError,
    },
    message::Message,
    operations::OpError,
    ring::{PeerKeyLocation, Ring},
    user_events::UserEventsProxy,
    NodeConfig,
};

pub(super) struct NodeInMemory<CErr = SimStoreError> {
    pub peer_key: PeerKey,
    pub op_storage: Arc<OpManager<CErr>>,
    gateways: Vec<PeerKeyLocation>,
    notification_channel: Receiver<Message>,
    conn_manager: MemoryConnManager,
    event_listener: Option<Box<dyn EventListener + Send + Sync + 'static>>,
    is_gateway: bool,
}

impl<CErr> NodeInMemory<CErr>
where
    CErr: std::error::Error + Send + Sync + 'static,
{
    /// Buils an in-memory node. Does nothing upon construction,
    pub fn build<CH>(
        config: NodeConfig,
        event_listener: Option<Box<dyn EventListener + Send + Sync + 'static>>,
    ) -> Result<NodeInMemory<<CH as ContractHandler>::Error>, anyhow::Error>
    where
        CH: ContractHandler + Send + Sync + 'static,
        <CH as ContractHandler>::Error: std::error::Error + Send + Sync + 'static,
    {
        let peer_key = PeerKey::from(config.local_key.public());
        let conn_manager = MemoryConnManager::new(true, peer_key, None);
        let gateways = config.get_gateways()?;

        let ring = Ring::new(&config, &gateways)?;
        let (notification_tx, notification_channel) = mpsc::channel(100);
        let (ops_ch_channel, ch_channel) = contract::contract_handler_channel();
        let op_storage = Arc::new(OpManager::new(ring, notification_tx, ops_ch_channel));
        let contract_handler = CH::from(ch_channel);

        GlobalExecutor::spawn(contract::contract_handling(contract_handler));

        Ok(NodeInMemory {
            peer_key,
            conn_manager,
            op_storage,
            gateways,
            notification_channel,
            event_listener,
            is_gateway: config.location.is_some(),
        })
    }

    pub async fn run_node<UsrEv>(&mut self, user_events: UsrEv) -> Result<(), anyhow::Error>
    where
        UsrEv: UserEventsProxy + Send + Sync + 'static,
    {
        join_ring_request(
            None,
            self.peer_key,
            self.gateways.iter(),
            &self.op_storage,
            &mut self.conn_manager,
        )
        .await?;
        GlobalExecutor::spawn(user_event_handling(self.op_storage.clone(), user_events));
        self.run_event_listener().await
    }

    pub async fn append_contracts(
        &self,
        contracts: Vec<(Contract, ContractValue)>,
    ) -> Result<(), ContractError<CErr>> {
        for (contract, value) in contracts {
            let key = contract.key();
            self.op_storage
                .notify_contract_handler(ContractHandlerEvent::Cache(contract))
                .await?;
            self.op_storage
                .notify_contract_handler(ContractHandlerEvent::PushQuery { key, value })
                .await?;
            log::debug!(
                "Appended contract {} to peer {}",
                key,
                self.op_storage.ring.peer_key
            );
            self.op_storage.ring.contract_cached(key);
        }
        Ok(())
    }

    /// Starts listening to incoming events. Will attempt to join the ring if any gateways have been provided.
    async fn run_event_listener(&mut self) -> Result<(), anyhow::Error> {
        loop {
            let msg = tokio::select! {
                msg = self.conn_manager.recv() => { msg }
                msg = self.notification_channel.recv() => if let Some(msg) = msg {
                    Ok(msg)
                } else {
                    anyhow::bail!("notification channel shutdown, fatal error");
                }
            };

            if let Ok(Message::Canceled(tx)) = msg {
                handle_cancelled_op(
                    tx,
                    self.peer_key,
                    self.gateways.iter(),
                    &self.op_storage,
                    &mut self.conn_manager,
                )
                .await?;
                continue;
            }

            let op_storage = self.op_storage.clone();
            let conn_manager = self.conn_manager.clone();
            let event_listener = self
                .event_listener
                .as_ref()
                .map(|listener| listener.trait_clone());

            GlobalExecutor::spawn(process_message(
                msg,
                op_storage,
                conn_manager,
                event_listener,
            ));
        }
    }

    #[inline(always)]
    fn report_result(op_result: Result<(), OpError<CErr>>) {
        if let Err(err) = op_result {
            log::debug!("Finished tx w/ error: {}", err)
        }
    }
}
