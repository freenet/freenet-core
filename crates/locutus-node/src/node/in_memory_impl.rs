use std::{collections::HashMap, sync::Arc};

use either::Either;
use locutus_runtime::prelude::ContractKey;
use tokio::sync::mpsc::{self, Receiver};

use super::{
    client_event_handling, conn_manager::in_memory::MemoryConnManager,
    event_listener::EventListener, handle_cancelled_op, join_ring_request, op_state::OpManager,
    process_message, PeerKey,
};
use crate::{
    client_events::ClientEventsProxy,
    config::GlobalExecutor,
    contract::{self, ContractError, ContractHandler, ContractHandlerEvent, SimStoreError},
    message::{Message, NodeEvent, TransactionType},
    operations::OpError,
    ring::{PeerKeyLocation, Ring},
    util::IterExt,
    NodeConfig, WrappedContract, WrappedState,
};

pub(super) struct NodeInMemory<CErr = SimStoreError> {
    pub peer_key: PeerKey,
    pub op_storage: Arc<OpManager<CErr>>,
    gateways: Vec<PeerKeyLocation>,
    notification_channel: Receiver<Either<Message, NodeEvent>>,
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
        config: NodeConfig<1>,
        event_listener: Option<Box<dyn EventListener + Send + Sync + 'static>>,
    ) -> Result<NodeInMemory<<CH as ContractHandler>::Error>, anyhow::Error>
    where
        CH: ContractHandler + Send + Sync + 'static,
        <CH as ContractHandler>::Error:
            std::error::Error + From<std::io::Error> + Send + Sync + 'static,
    {
        let peer_key = PeerKey::from(config.local_key.public());
        let conn_manager = MemoryConnManager::new(peer_key);
        let gateways = config.get_gateways()?;
        let is_gateway = config.local_ip.zip(config.local_port).is_some();

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
            is_gateway,
        })
    }

    pub async fn run_node<UsrEv>(&mut self, user_events: UsrEv) -> Result<(), anyhow::Error>
    where
        UsrEv: ClientEventsProxy + Send + Sync + 'static,
    {
        if !self.is_gateway {
            if let Some(gateway) = self.gateways.iter().shuffle().take(1).next() {
                join_ring_request(
                    None,
                    self.peer_key,
                    gateway,
                    &self.op_storage,
                    &mut self.conn_manager,
                )
                .await?;
            } else {
                anyhow::bail!("requires at least one gateway");
            }
        }
        GlobalExecutor::spawn(client_event_handling(self.op_storage.clone(), user_events));
        self.run_event_listener().await
    }

    pub async fn append_contracts(
        &self,
        contracts: Vec<(WrappedContract, WrappedState)>,
        contract_subscribers: HashMap<ContractKey, Vec<PeerKeyLocation>>,
    ) -> Result<(), ContractError<CErr>> {
        for (contract, value) in contracts {
            let key = *contract.key();
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
            if let Some(subscribers) = contract_subscribers.get(&key) {
                // add contract subscribers
                for subscriber in subscribers {
                    if self
                        .op_storage
                        .ring
                        .add_subscriber(key, *subscriber)
                        .is_err()
                    {
                        log::warn!("Max subscribers for contract {} reached", key);
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Starts listening to incoming events. Will attempt to join the ring if any gateways have been provided.
    async fn run_event_listener(&mut self) -> Result<(), anyhow::Error> {
        loop {
            let msg = tokio::select! {
                msg = self.conn_manager.recv() => { msg.map(Either::Left) }
                msg = self.notification_channel.recv() => if let Some(msg) = msg {
                    Ok(msg)
                } else {
                    anyhow::bail!("notification channel shutdown, fatal error");
                }
            };

            if let Ok(Either::Left(Message::Canceled(tx))) = msg {
                let tx_type = tx.tx_type();
                let res = handle_cancelled_op(
                    tx,
                    self.peer_key,
                    self.gateways.iter(),
                    &self.op_storage,
                    &mut self.conn_manager,
                )
                .await;
                match res {
                    Err(OpError::MaxRetriesExceeded(_, _))
                        if tx_type == TransactionType::JoinRing && !self.is_gateway =>
                    {
                        log::warn!("Retrying joining the ring with an other peer");
                        let gateway = self.gateways.iter().shuffle().next().unwrap();
                        join_ring_request(
                            None,
                            self.peer_key,
                            gateway,
                            &self.op_storage,
                            &mut self.conn_manager,
                        )
                        .await?
                    }
                    Err(err) => return Err(anyhow::anyhow!(err)),
                    Ok(_) => {}
                }
                continue;
            }

            let msg = match msg {
                Ok(Either::Left(msg)) => Ok(msg),
                Ok(Either::Right(action)) => match action {
                    NodeEvent::ShutdownNode => break Ok(()),
                    NodeEvent::ConfirmedInbound => continue,
                    NodeEvent::DropConnection(_) => continue,
                    NodeEvent::AcceptConnection(_) => continue,
                    NodeEvent::Error(err) => {
                        log::error!("Connection error within ops: {err}");
                        continue;
                    }
                },
                Err(err) => Err(err),
            };

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
}
