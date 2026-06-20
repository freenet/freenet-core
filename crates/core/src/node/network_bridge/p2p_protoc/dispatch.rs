//! Event-loop dispatch for [`P2pConnManager`]: priority-select result
//! routing and the per-message / per-event handlers it fans out to.
//!
//! Behavior-preserving extraction from `p2p_protoc.rs`.

use super::*;

impl P2pConnManager {
    /// Process a SelectResult from the priority select stream
    pub(super) async fn process_select_result(
        &mut self,
        result: priority_select::SelectResult,
        state: &mut EventListenerState,
        handshake_commands: &HandshakeCommandSender,
    ) -> anyhow::Result<EventResult> {
        let peer_id = &self.bridge.op_manager.ring.connection_manager.pub_key;

        use priority_select::SelectResult;
        match result {
            SelectResult::Notification(msg) => {
                tracing::debug!(
                    peer = %peer_id,
                    msg_present = msg.is_some(),
                    "PrioritySelect: notifications_receiver READY"
                );
                Ok(self.handle_notification_msg(msg))
            }
            SelectResult::OpExecution(msg) => {
                tracing::debug!(
                    peer = %peer_id,
                    "PrioritySelect: op_execution_receiver READY"
                );
                Ok(self.handle_op_execution(msg, state))
            }
            SelectResult::PeerConnection(msg) => {
                tracing::debug!(
                    peer = %peer_id,
                    "PrioritySelect: connection events READY"
                );
                self.handle_transport_event(msg, state, handshake_commands)
                    .await
            }
            SelectResult::ConnBridge(msg) => {
                tracing::debug!(
                    peer = %peer_id,
                    "PrioritySelect: conn_bridge_rx READY"
                );
                Ok(self.handle_bridge_msg(msg))
            }
            SelectResult::Handshake(result) => {
                tracing::debug!(
                    peer = %peer_id,
                        "PrioritySelect: handshake event READY"
                );
                match result {
                    Some(event) => {
                        self.handle_handshake_action(event, state, handshake_commands)
                            .await?;
                        Ok(EventResult::Continue)
                    }
                    None => {
                        tracing::warn!(
                            "Handshake handler stream closed; notifying pending callbacks"
                        );
                        self.handle_handshake_stream_closed(state).await?;
                        Ok(EventResult::Continue)
                    }
                }
            }
            SelectResult::NodeController(msg) => {
                tracing::debug!(
                    peer = %peer_id,
                    "PrioritySelect: node_controller READY"
                );
                Ok(self.handle_node_controller_msg(msg))
            }
            SelectResult::ClientTransaction(event_id) => {
                tracing::debug!(
                    peer = %peer_id,
                    "PrioritySelect: client_wait_for_transaction READY"
                );
                Ok(self.handle_client_transaction_subscription(event_id, state))
            }
            SelectResult::ExecutorTransaction(id) => {
                tracing::debug!(
                    peer = %peer_id,
                    "PrioritySelect: executor_listener READY"
                );
                Ok(self.handle_executor_transaction(id, state))
            }
        }
    }

    pub(super) async fn handle_inbound_message(
        &self,
        msg: NetMessage,
        source_addr: Option<SocketAddr>,
        op_manager: &Arc<OpManager>,
        state: &mut EventListenerState,
    ) -> anyhow::Result<()> {
        let tx = *msg.id();
        tracing::debug!(
            %tx,
            tx_type = ?tx.transaction_type(),
            ?source_addr,
            "Handling inbound NetMessage at event loop"
        );
        match msg {
            NetMessage::V1(NetMessageV1::Aborted(tx)) => {
                tracing::debug!(
                    %tx,
                    tx_type = ?tx.transaction_type(),
                    "Received Aborted wire message — driver owns cancellation, ignoring"
                );
            }
            msg => {
                self.process_message(msg, source_addr, op_manager, state)
                    .await;
            }
        }
        Ok(())
    }

    async fn process_message(
        &self,
        msg: NetMessage,
        source_addr: Option<SocketAddr>,
        op_manager: &Arc<OpManager>,
        state: &mut EventListenerState,
    ) {
        tracing::debug!(
            tx = %msg.id(),
            tx_type = ?msg.id().transaction_type(),
            msg_type = %msg,
            ?source_addr,
            peer = ?op_manager.ring.connection_manager.get_own_addr(),
            "process_message called - processing network message"
        );

        let span = tracing::info_span!(
            "process_network_message",
            transaction = %msg.id(),
            tx_type = %msg.id().transaction_type()
        );

        let pending_op_result = state.pending_op_results.get(msg.id()).cloned();

        GlobalExecutor::spawn(
            process_message_decoupled(
                msg,
                source_addr,
                op_manager.clone(),
                self.bridge.clone(),
                self.event_listener.trait_clone(),
                pending_op_result,
            )
            .instrument(span),
        );
    }

    /// Looks up a connection by public key using the reverse lookup map.
    /// Returns the socket address and connection entry if found.
    pub(super) fn connection_entry_by_pub_key(
        &self,
        pub_key: &TransportPublicKey,
    ) -> Option<(SocketAddr, &ConnectionEntry)> {
        self.addr_by_pub_key
            .get(pub_key)
            .and_then(|addr| self.connections.get(addr).map(|entry| (*addr, entry)))
    }

    fn handle_notification_msg(&self, msg: Option<Either<NetMessage, NodeEvent>>) -> EventResult {
        match msg {
            Some(Left(msg)) => {
                // With hop-by-hop routing, messages no longer embed target.
                // For initial requests (GET, PUT, Subscribe, Connect), extract next hop from operation state.
                // For other messages, process locally (they'll be routed through network_bridge.send()).
                let tx = *msg.id();

                // Try to get next hop from operation state for initial outbound requests
                // Uses peek methods to avoid pop/push overhead
                if let Some(next_hop_addr) = self.bridge.op_manager.peek_next_hop_addr(&tx) {
                    tracing::debug!(
                        tx = %msg.id(),
                        msg_type = %msg,
                        next_hop = %next_hop_addr,
                        "handle_notification_msg: Found next hop in operation state, routing as OutboundMessage"
                    );
                    // Fire-and-forget ops (Update, Unsubscribe) are completed after routing.
                    // Other ops expect responses and stay in the state map.
                    let is_fire_and_forget = tx.transaction_type() == TransactionType::Update
                        || matches!(
                            &msg,
                            NetMessage::V1(NetMessageV1::Subscribe(
                                crate::operations::subscribe::SubscribeMsg::Unsubscribe { .. }
                            ))
                        );
                    if is_fire_and_forget {
                        self.bridge.op_manager.completed(tx);
                    }
                    return EventResult::Event(
                        ConnEvent::OutboundMessageWithTarget {
                            target_addr: next_hop_addr,
                            msg,
                        }
                        .into(),
                    );
                }

                // Message has no next hop or couldn't get from op state - process locally
                tracing::debug!(
                    tx = %msg.id(),
                    msg_type = %msg,
                    "handle_notification_msg: No next hop found, processing locally"
                );
                EventResult::Event(ConnEvent::InboundMessage(msg.into()).into())
            }
            Some(Right(action)) => {
                tracing::debug!(
                    event = %action,
                    "handle_notification_msg: Received NodeEvent notification"
                );
                EventResult::Event(ConnEvent::NodeAction(action).into())
            }
            None => EventResult::Event(
                ConnEvent::ClosedChannel(ChannelCloseReason::Notification).into(),
            ),
        }
    }

    fn handle_op_execution(
        &self,
        msg: Option<super::super::OpExecutionPayload>,
        state: &mut EventListenerState,
    ) -> EventResult {
        match msg {
            Some((callback, msg, target_addr)) => {
                // The driver task may have been cancelled between
                // `op_execution_sender.send()` and our processing of the
                // event — e.g. simulation teardown drops driver futures.
                // When the response receiver is dropped before we reach
                // here, the callback sender is closed; inserting it
                // would leak `pending_op_results` until the 60s sweep,
                // detectable as imbalance in #3100's regression guard
                // `test_pending_op_results_bounded`. Skipping the
                // insert keeps the HashMap bounded; the outbound
                // request below still dispatches because the receiving
                // peer's view of the operation is independent of the
                // local driver's lifecycle.
                if callback.is_closed() {
                    tracing::debug!(
                        tx = %msg.id(),
                        "handle_op_execution: callback already closed (driver cancelled before insert); skipping"
                    );
                } else {
                    state.pending_op_results.insert(*msg.id(), callback);
                    crate::config::GlobalTestMetrics::record_pending_op_insert();
                    crate::config::GlobalTestMetrics::record_pending_op_size(
                        state.pending_op_results.len() as u64,
                    );
                }
                // When the driver supplied an explicit target, dispatch the
                // message to that peer over the network instead of looping it
                // back as a local InboundMessage. The reply still flows back
                // through the `pending_op_results` callback we just inserted.
                //
                // This is the load-bearing branch for issue #3838:
                // client-initiated SUBSCRIBE with the contract cached locally
                // would otherwise short-circuit in `process_message` and never
                // register as a downstream subscriber on the home node.
                match target_addr {
                    Some(target_addr) => {
                        // Track `tx → target_addr` so disconnect-cancellation in
                        // `prune_connection` / `handle_orphaned_transactions` can
                        // wake the parked driver (#4154). Two cases register here:
                        //   - `send_to_and_await`: this branch just installed a
                        //     waiter into `pending_op_results` above.
                        //   - `send_fire_and_forget` from a relay driver running
                        //     on the originator: the callback is closed (no insert
                        //     above) but the originator's client driver has an
                        //     earlier `pending_op_results[tx]` waiter that needs
                        //     waking.
                        // Skip when no waiter is present (e.g. cancelled-driver
                        // teardown races) — registering with nothing to wake would
                        // leak a live_tx_tracker entry until the peer disconnects.
                        // Same-peer duplicates (e.g. CONNECT's parallel
                        // `Ring::initiate_connect` registration) are tolerated by
                        // `remove_finished_transaction`'s per-peer `retain`. Cross-
                        // peer rebinds (same tx forwarded to two peers in turn) leave
                        // a stale entry on the earlier peer until that peer
                        // disconnects — a known limitation documented in
                        // `LiveTransactionTracker::add_transaction`'s rustdoc.
                        let tx = *msg.id();
                        if state.pending_op_results.contains_key(&tx) {
                            self.bridge
                                .op_manager
                                .ring
                                .live_tx_tracker
                                .add_transaction(target_addr, tx);
                        }
                        EventResult::Event(
                            ConnEvent::OutboundMessageWithTarget { target_addr, msg }.into(),
                        )
                    }
                    None => EventResult::Event(ConnEvent::InboundMessage(msg.into()).into()),
                }
            }
            None => {
                EventResult::Event(ConnEvent::ClosedChannel(ChannelCloseReason::OpExecution).into())
            }
        }
    }

    fn handle_bridge_msg(&self, msg: Option<P2pBridgeEvent>) -> EventResult {
        match msg {
            Some(P2pBridgeEvent::Message(target, msg)) => {
                // Use OutboundMessageWithTarget to preserve the target address from
                // P2pBridge::send(). This is critical for NAT scenarios where
                // the address in the message differs from the actual transport address.
                // The target.socket_addr() contains the address that was used to look up
                // the peer in P2pBridge::send(), which is the correct transport address.
                if let Some(target_addr) = target.socket_addr() {
                    EventResult::Event(
                        ConnEvent::OutboundMessageWithTarget {
                            target_addr,
                            msg: *msg,
                        }
                        .into(),
                    )
                } else {
                    // Fall back to OutboundMessage if no explicit address
                    // (shouldn't happen in normal operation)
                    tracing::warn!(
                        tx = %msg.id(),
                        target_pub_key = %target.pub_key(),
                        "handle_bridge_msg: target has no socket address, falling back to msg.target()"
                    );
                    EventResult::Event(ConnEvent::OutboundMessage(*msg).into())
                }
            }
            Some(P2pBridgeEvent::NodeAction(action)) => {
                EventResult::Event(ConnEvent::NodeAction(action).into())
            }
            Some(P2pBridgeEvent::StreamSend {
                target_addr,
                stream_id,
                data,
                metadata,
                completion_tx,
            }) => EventResult::Event(
                ConnEvent::StreamSend {
                    target_addr,
                    stream_id,
                    data,
                    metadata,
                    completion_tx,
                }
                .into(),
            ),
            Some(P2pBridgeEvent::PipeStream {
                target_addr,
                outbound_stream_id,
                inbound_handle,
                metadata,
            }) => EventResult::Event(
                ConnEvent::PipeStream {
                    target_addr,
                    outbound_stream_id,
                    inbound_handle,
                    metadata,
                }
                .into(),
            ),
            None => EventResult::Event(ConnEvent::ClosedChannel(ChannelCloseReason::Bridge).into()),
        }
    }

    fn handle_node_controller_msg(&self, msg: Option<NodeEvent>) -> EventResult {
        match msg {
            Some(msg) => EventResult::Event(ConnEvent::NodeAction(msg).into()),
            None => {
                EventResult::Event(ConnEvent::ClosedChannel(ChannelCloseReason::Controller).into())
            }
        }
    }

    // Removed handle_handshake_msg as it's integrated into wait_for_event

    fn handle_client_transaction_subscription(
        &self,
        event_id: Result<(ClientId, WaitingTransaction), anyhow::Error>,
        state: &mut EventListenerState,
    ) -> EventResult {
        let Ok((client_id, transaction)) = event_id.inspect_err(|e| {
            tracing::error!("Error while receiving client transaction result: {:?}", e);
        }) else {
            return EventResult::Continue;
        };
        match transaction {
            WaitingTransaction::Transaction(tx) => {
                tracing::debug!(%tx, %client_id, "Subscribing client to transaction results");
                let entry = state.tx_to_client.entry(tx).or_default();
                let inserted = entry.insert(client_id);
                tracing::debug!(
                    "tx_to_client: tx={} client={} inserted={} total_waiting_clients={}",
                    tx,
                    client_id,
                    inserted,
                    entry.len()
                );
            }
            WaitingTransaction::Subscription { contract_key } => {
                tracing::debug!(%client_id, %contract_key, "Client waiting for subscription");
                if let Some(clients) =
                    state
                        .client_waiting_transaction
                        .iter_mut()
                        .find_map(|(tx, clients)| {
                            if let WaitingTransaction::Subscription { contract_key: key } = tx {
                                return (key == &contract_key).then_some(clients);
                            }
                            None
                        })
                {
                    clients.insert(client_id);
                } else {
                    state.client_waiting_transaction.push((
                        WaitingTransaction::Subscription { contract_key },
                        HashSet::from_iter([client_id]),
                    ));
                }
            }
        }
        EventResult::Continue
    }

    fn handle_executor_transaction(
        &self,
        id: Result<Transaction, anyhow::Error>,
        state: &mut EventListenerState,
    ) -> EventResult {
        let Ok(id) = id.map_err(|err| {
            tracing::error!("Error while receiving transaction from executor: {:?}", err);
        }) else {
            return EventResult::Continue;
        };
        state.pending_from_executor.insert(id);
        EventResult::Continue
    }
}
