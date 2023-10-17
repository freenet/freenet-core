use std::{collections::HashMap, sync::Arc};

use either::Either;
use freenet_stdlib::prelude::*;

use super::{
    client_event_handling,
    conn_manager::{in_memory::MemoryConnManager, EventLoopNotifications},
    handle_cancelled_op, join_ring_request,
    op_state::OpManager,
    process_message, EventLogRegister, PeerKey,
};
use crate::{
    client_events::ClientEventsProxy,
    config::GlobalExecutor,
    contract::{
        self, executor_channel, ClientResponsesSender, ContractError, ContractHandler,
        ContractHandlerEvent, ExecutorToEventLoopChannel, MemoryContractHandler,
        NetworkEventListenerHalve,
    },
    message::{Message, NodeEvent, TransactionType},
    node::NodeBuilder,
    operations::OpError,
    ring::{PeerKeyLocation, Ring},
    util::IterExt,
};

pub(super) struct NodeInMemory {
    pub peer_key: PeerKey,
    pub op_storage: Arc<OpManager>,
    gateways: Vec<PeerKeyLocation>,
    notification_channel: EventLoopNotifications,
    conn_manager: MemoryConnManager,
    event_listener: Box<dyn EventLogRegister + Send + Sync + 'static>,
    is_gateway: bool,
    _executor_listener: ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
}

impl NodeInMemory {
    /// Buils an in-memory node. Does nothing upon construction,
    pub async fn build<EL: EventLogRegister>(
        builder: NodeBuilder<1>,
        event_listener: EL,
        ch_builder: String,
    ) -> Result<NodeInMemory, anyhow::Error> {
        let peer_key = PeerKey::from(builder.local_key.public());
        let conn_manager = MemoryConnManager::new(peer_key);
        let gateways = builder.get_gateways()?;
        let is_gateway = builder.local_ip.zip(builder.local_port).is_some();

        let ring = Ring::new::<1, EL>(&builder, &gateways)?;
        let (notification_channel, notification_tx) = EventLoopNotifications::channel();
        let (ops_ch_channel, ch_channel) = contract::contract_handler_channel();
        let op_storage = Arc::new(OpManager::new(ring, notification_tx, ops_ch_channel));
        let (_executor_listener, executor_sender) = executor_channel(op_storage.clone());
        let contract_handler =
            MemoryContractHandler::build(ch_channel, executor_sender, ch_builder)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

        GlobalExecutor::spawn(contract::contract_handling(contract_handler));

        Ok(NodeInMemory {
            peer_key,
            conn_manager,
            op_storage,
            gateways,
            notification_channel,
            event_listener: Box::new(event_listener),
            is_gateway,
            _executor_listener,
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
        let (client_responses, cli_response_sender) = contract::ClientResponses::channel();
        GlobalExecutor::spawn(client_event_handling(
            self.op_storage.clone(),
            user_events,
            client_responses,
        ));
        self.run_event_listener(cli_response_sender).await
    }

    pub async fn append_contracts<'a>(
        &self,
        contracts: Vec<(ContractContainer, WrappedState)>,
        contract_subscribers: HashMap<ContractKey, Vec<PeerKeyLocation>>,
    ) -> Result<(), ContractError> {
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

    /// Starts listening to incoming events. Will attempt to join the ring if any gateways have been provided.
    #[tracing::instrument(name = "memory_event_listener", skip_all)]
    async fn run_event_listener(
        &mut self,
        _client_responses: ClientResponsesSender, // fixme: use this
    ) -> Result<(), anyhow::Error> {
        loop {
            let msg = tokio::select! {
                msg = self.conn_manager.recv() => { msg.map(Either::Left) }
                msg = self.notification_channel.recv() => if let Some(msg) = msg {
                    Ok(msg.map_left(|(msg, _cli_id)| msg))
                } else {
                    anyhow::bail!("notification channel shutdown, fatal error");
                }
            };

            if let Ok(Either::Left(Message::Aborted(tx))) = msg {
                let tx_type = tx.tx_type();
                let res = handle_cancelled_op(
                    tx,
                    self.peer_key,
                    &self.op_storage,
                    &mut self.conn_manager,
                )
                .await;
                match res {
                    Err(OpError::MaxRetriesExceeded(_, _))
                        if tx_type == TransactionType::JoinRing && !self.is_gateway =>
                    {
                        tracing::warn!("Retrying joining the ring with an other peer");
                        if let Some(gateway) = self.gateways.iter().shuffle().next() {
                            join_ring_request(
                                None,
                                self.peer_key,
                                gateway,
                                &self.op_storage,
                                &mut self.conn_manager,
                            )
                            .await?
                        } else {
                            anyhow::bail!("requires at least one gateway");
                        }
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
                        tracing::error!("Connection error within ops: {err}");
                        continue;
                    }
                },
                Err(err) => Err(err),
            };

            let op_storage = self.op_storage.clone();
            let conn_manager = self.conn_manager.clone();
            let event_listener = self.event_listener.trait_clone();

            GlobalExecutor::spawn(process_message(
                msg,
                op_storage,
                conn_manager,
                event_listener,
                None,
                None,
                None,
            ));
        }
    }
}
