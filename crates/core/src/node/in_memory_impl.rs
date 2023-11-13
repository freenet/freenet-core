use std::{collections::HashMap, sync::Arc};

use either::Either;
use freenet_stdlib::prelude::*;
use tracing::Instrument;

use super::{
    client_event_handling, handle_cancelled_op, join_ring_request,
    network_bridge::{in_memory::MemoryConnManager, EventLoopNotifications},
    network_event_log::NetEventLog,
    op_state_manager::OpManager,
    process_message, NetEventRegister, PeerKey,
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
    ring::PeerKeyLocation,
    util::IterExt,
};

pub(super) struct NodeInMemory {
    pub peer_key: PeerKey,
    pub op_storage: Arc<OpManager>,
    gateways: Vec<PeerKeyLocation>,
    notification_channel: EventLoopNotifications,
    conn_manager: MemoryConnManager,
    event_register: Box<dyn NetEventRegister>,
    is_gateway: bool,
    _executor_listener: ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
    /// Span to use for this node for tracing purposes
    pub parent_span: tracing::Span,
}

impl NodeInMemory {
    /// Buils an in-memory node. Does nothing upon construction,
    pub async fn build<ER: NetEventRegister + Clone>(
        builder: NodeBuilder<1>,
        event_register: ER,
        ch_builder: String,
        add_noise: bool,
    ) -> Result<NodeInMemory, anyhow::Error> {
        let peer_key = PeerKey::from(builder.local_key.public());
        let gateways = builder.get_gateways()?;
        let is_gateway = builder.local_ip.zip(builder.local_port).is_some();

        let (notification_channel, notification_tx) = EventLoopNotifications::channel();
        let (ops_ch_channel, ch_channel) = contract::contract_handler_channel();

        let op_storage = Arc::new(OpManager::new(
            notification_tx,
            ops_ch_channel,
            &builder,
            &gateways,
            event_register.clone(),
        )?);
        let (_executor_listener, executor_sender) = executor_channel(op_storage.clone());
        let contract_handler =
            MemoryContractHandler::build(ch_channel, executor_sender, ch_builder)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;

        let event_register = Box::new(event_register);
        let conn_manager = MemoryConnManager::new(
            peer_key,
            event_register.trait_clone(),
            op_storage.clone(),
            add_noise,
        );

        let parent_span = tracing::Span::current();
        GlobalExecutor::spawn(
            contract::contract_handling(contract_handler)
                .instrument(tracing::info_span!(parent: parent_span.clone(), "contract_handling")),
        );

        Ok(NodeInMemory {
            peer_key,
            conn_manager,
            op_storage,
            gateways,
            notification_channel,
            event_register,
            is_gateway,
            _executor_listener,
            parent_span,
        })
    }

    pub async fn run_node<UsrEv>(&mut self, user_events: UsrEv) -> Result<(), anyhow::Error>
    where
        UsrEv: ClientEventsProxy + Send + 'static,
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
        let parent_span = self.parent_span.clone();
        GlobalExecutor::spawn(
            client_event_handling(self.op_storage.clone(), user_events, client_responses)
                .instrument(tracing::info_span!(parent: parent_span, "client_event_handling")),
        );
        let parent_span = self.parent_span.clone();
        self.run_event_listener(cli_response_sender)
            .instrument(parent_span)
            .await
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

    /// Starts listening to incoming events. Will attempt to join the ring if any gateways have been provided.
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
                let tx_type = tx.transaction_type();
                let res = handle_cancelled_op(
                    tx,
                    self.peer_key,
                    &self.op_storage,
                    &mut self.conn_manager,
                )
                .await;
                match res {
                    Err(OpError::MaxRetriesExceeded(_, _))
                        if tx_type == TransactionType::Connect && !self.is_gateway =>
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
                Ok(Either::Left(msg)) => msg,
                Ok(Either::Right(action)) => match action {
                    NodeEvent::ShutdownNode => break Ok(()),
                    NodeEvent::DropConnection(peer) => {
                        tracing::info!("Dropping connection to {peer}");
                        self.event_register
                            .register_events(Either::Left(NetEventLog::disconnected(&peer)));
                        self.op_storage.ring.prune_connection(peer);
                        continue;
                    }
                    other => {
                        unreachable!("event {other:?}, shouldn't happen in the in-memory impl")
                    }
                },
                Err(err) => {
                    super::report_result(
                        None,
                        Err(err.into()),
                        &self.op_storage,
                        None,
                        None,
                        &mut *self.event_register as &mut _,
                    )
                    .await;
                    continue;
                }
            };

            let op_storage = self.op_storage.clone();
            let conn_manager = self.conn_manager.clone();
            let event_listener = self.event_register.trait_clone();

            let parent_span = tracing::Span::current();
            let span = tracing::info_span!(
                parent: parent_span,
                "process_network_message",
                peer = %self.peer_key, transaction = %msg.id(),
                tx_type = %msg.id().transaction_type()
            );
            let msg = process_message(
                msg,
                op_storage,
                conn_manager,
                event_listener,
                None,
                None,
                None,
            )
            .instrument(span);
            GlobalExecutor::spawn(msg);
        }
    }
}
