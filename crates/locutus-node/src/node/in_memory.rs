use std::sync::Arc;

use tokio::sync::mpsc::{self, Receiver};

use crate::{
    conn_manager::{in_memory::MemoryConnManager, ConnectionBridge, PeerKey},
    contract::{self, ContractHandler},
    message::{Message, Transaction, TransactionType, TxType},
    node::event_listener::EventLog,
    operations::{
        get,
        join_ring::{self, JoinRingMsg, JoinRingOp},
        put, subscribe, OpError, Operation,
    },
    ring::{PeerKeyLocation, Ring},
    user_events::UserEventsProxy,
    utils::{ExponentialBackoff, ExtendedIter},
    NodeConfig,
};

use super::{op_state::OpManager, EventListener};

pub(crate) struct NodeInMemory<CErr>
where
    CErr: std::error::Error,
{
    pub peer_key: PeerKey,
    gateways: Vec<PeerKeyLocation>,
    notification_channel: Receiver<Message>,
    pub conn_manager: MemoryConnManager,
    pub op_storage: Arc<OpManager<CErr>>,
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
        let peer = PeerKey::from(config.local_key.public());
        let ev_listener_cp = event_listener
            .as_ref()
            .map(|listener| listener.trait_clone());
        let conn_manager = MemoryConnManager::new(true, peer, None, ev_listener_cp);

        let gateways: Vec<_> = config
            .remote_nodes
            .into_iter()
            .filter_map(|node| {
                if node.addr.is_some() {
                    Some(PeerKeyLocation {
                        peer: PeerKey::from(node.identifier),
                        location: Some(node.location),
                    })
                } else {
                    None
                }
            })
            .filter(|pkloc| pkloc.peer != peer)
            .collect();

        // config.location

        if (config.local_ip.is_none() || config.local_port.is_none()) && gateways.is_empty() {
            return Err(anyhow::anyhow!(
                    "At least one remote gateway is required to join an existing network for non-gateway nodes."
                ));
        }

        let mut ring = Ring::new(peer);
        if let Some(loc) = config.location {
            if config.local_ip.is_none() || config.local_port.is_none() {
                return Err(anyhow::anyhow!("IP and port are required for gateways"));
            }
            ring.update_location(Some(loc));
            for PeerKeyLocation { peer, location } in &gateways {
                // all gateways are aware of each other
                ring.add_connection((*location).unwrap(), *peer);
            }
        }
        if let Some(max_hops_to_live) = config.max_hops_to_live {
            ring.with_max_hops(max_hops_to_live);
        }
        if let Some(rnd_if_htl_above) = config.rnd_if_htl_above {
            ring.with_rnd_walk_above(rnd_if_htl_above);
        }
        if let Some(max_conn) = config.max_number_conn {
            ring.with_max_connections(max_conn);
        }
        if let Some(min_conn) = config.min_number_conn {
            ring.with_min_connections(min_conn);
        }

        let (notification_tx, notification_channel) = mpsc::channel(100);
        let (ops_ch_channel, ch_channel) = contract::contract_handler_channel();
        let op_storage = Arc::new(OpManager::new(ring, notification_tx, ops_ch_channel));
        let contract_handler = CH::from(ch_channel);

        tokio::spawn(contract::contract_handling(contract_handler));

        Ok(NodeInMemory {
            peer_key: peer,
            conn_manager,
            op_storage,
            gateways,
            notification_channel,
            event_listener,
            is_gateway: config.location.is_some(),
        })
    }

    async fn join_ring(
        &mut self,
        backoff: Option<ExponentialBackoff>,
    ) -> Result<(), OpError<CErr>> {
        if self.is_gateway {
            return Ok(());
        }
        if let Some(gateway) = self.gateways.iter().shuffle().take(1).next() {
            let tx_id = Transaction::new(<JoinRingMsg as TxType>::tx_type_id(), &self.peer_key);
            // initiate join action action per each gateway
            let mut op = join_ring::JoinRingOp::initial_request(
                self.peer_key,
                *gateway,
                self.op_storage.ring.max_hops_to_live,
                tx_id,
            );
            if let Some(mut backoff) = backoff {
                // backoff to retry later in case it failed
                log::warn!(
                    "Performing a new join attempt, attempt number: {}",
                    backoff.retries()
                );
                if backoff.sleep_async().await.is_none() {
                    log::error!("Max number of retries reached");
                    return Err(OpError::MaxRetriesExceeded(
                        tx_id,
                        format!("{:?}", tx_id.tx_type()),
                    ));
                }
                op.backoff = Some(backoff);
            }
            join_ring::join_ring_request(tx_id, &self.op_storage, &mut self.conn_manager, op)
                .await?;
        } else {
            log::warn!("No gateways provided, single gateway node setup for this node");
        }
        Ok(())
    }

    /// Starts listening to incoming events. Will attempt to join the ring if any gateways have been provided.
    pub async fn listen_on<UsrEv>(&mut self, user_events: UsrEv) -> Result<(), anyhow::Error>
    where
        UsrEv: UserEventsProxy + Send + Sync + 'static,
    {
        self.join_ring(None).await?;
        tokio::spawn(super::user_event_handling(
            self.op_storage.clone(),
            user_events,
        ));

        // loop for processings messages
        loop {
            let msg = tokio::select! {
                msg = self.conn_manager.recv() => { msg }
                msg = self.notification_channel.recv() => if let Some(msg) = msg {
                    Ok(msg)
                } else {
                    anyhow::bail!("notification channel shutdown, fatal error");
                }
            };

            let op_storage = self.op_storage.clone();
            let mut conn_manager = self.conn_manager.clone();
            let mut event_listener = self
                .event_listener
                .as_ref()
                .map(|listener| listener.trait_clone());

            if let Ok(Message::Canceled(tx)) = msg {
                log::warn!("Failed tx `{}`, potentially attempting a retry", tx);
                match tx.tx_type() {
                    TransactionType::JoinRing => {
                        const MSG: &str = "Fatal error: unable to connect to the network";
                        // the attempt to join the network failed, this could be a fatal error since the node
                        // is useless without connecting to the network, we will retry with exponential backoff
                        match op_storage.pop(&tx) {
                            Some(Operation::JoinRing(JoinRingOp {
                                backoff: Some(backoff),
                                ..
                            })) => {
                                if cfg!(test) {
                                    self.join_ring(None).await?
                                } else {
                                    self.join_ring(Some(backoff)).await?
                                }
                            }
                            None => {
                                log::error!("{}", MSG);
                                self.join_ring(None).await?
                            }
                            _ => {}
                        }
                    }
                    _ => todo!(),
                }
            }

            tokio::spawn(async move {
                match msg {
                    Ok(msg) => {
                        if let Some(listener) = &mut event_listener {
                            listener.event_received(EventLog::new(&msg, &op_storage.ring.peer_key));
                        }
                        match msg {
                            Message::JoinRing(op) => {
                                let op_result =
                                    join_ring::handle_join_ring(&op_storage, &mut conn_manager, op)
                                        .await;
                                Self::report_result(op_result);
                            }
                            Message::Put(op) => {
                                let op_result =
                                    put::handle_put_request(&op_storage, &mut conn_manager, op)
                                        .await;
                                Self::report_result(op_result);
                            }
                            Message::Get(op) => {
                                log::info!("Handling get request");
                                let op_result =
                                    get::handle_get_request(&op_storage, &mut conn_manager, op)
                                        .await;
                                Self::report_result(op_result);
                            }
                            Message::Subscribe(op) => {
                                let op_result = subscribe::handle_subscribe_response(
                                    &op_storage,
                                    &mut conn_manager,
                                    op,
                                )
                                .await;
                                Self::report_result(op_result);
                            }
                            _ => {}
                        }
                    }
                    Err(err) => {
                        Self::report_result(Err(err.into()));
                    }
                }
            });
        }
    }

    #[inline(always)]
    fn report_result(op_result: Result<(), OpError<CErr>>) {
        if let Err(err) = op_result {
            log::debug!("Finished tx w/ error: {}", err)
        }
    }
}
