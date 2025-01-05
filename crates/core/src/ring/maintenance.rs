use std::{collections::{BTreeMap, BTreeSet}, sync::Arc, time::{Duration, Instant}};
use either::Either;
use rand::seq::SliceRandom;
use crate::{message::{NodeEvent, Transaction}, node::{EventLoopNotificationsSender, PeerId}, operations::connect, router::Router};
use super::{LiveTransactionTracker, Location, Ring};

impl Ring {
    pub(crate) async fn connection_maintenance(
        self: Arc<Self>,
        notifier: EventLoopNotificationsSender,
        live_tx_tracker: LiveTransactionTracker,
        mut missing_candidates: sync::mpsc::Receiver<PeerId>,
    ) -> anyhow::Result<()> {
        tracing::debug!("Initializing connection maintenance task");
        #[cfg(not(test))]
        const CONNECTION_AGE_THRESOLD: Duration = Duration::from_secs(60 * 5);
        #[cfg(test)]
        const CONNECTION_AGE_THRESOLD: Duration = Duration::from_secs(5);
        const CHECK_TICK_DURATION: Duration = Duration::from_secs(60);
        const REGENERATE_DENSITY_MAP_INTERVAL: Duration = Duration::from_secs(60);

        let mut check_interval = tokio::time::interval(CHECK_TICK_DURATION);
        check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut refresh_density_map = tokio::time::interval(REGENERATE_DENSITY_MAP_INTERVAL);
        refresh_density_map.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut missing = BTreeMap::new();

        #[cfg(not(test))]
        let retry_peers_missing_candidates_interval = Duration::from_secs(60 * 5) * 2;
        #[cfg(test)]
        let retry_peers_missing_candidates_interval = Duration::from_secs(5);

        // if the peer is just starting wait a bit before
        // we even attempt acquiring more connections
        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut live_tx = None;
        let mut pending_conn_adds = BTreeSet::new();
        loop {
            if self.connection_manager.get_peer_key().is_none() {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            loop {
                match missing_candidates.try_recv() {
                    Ok(missing_candidate) => {
                        missing.insert(Reverse(Instant::now()), missing_candidate);
                    }
                    Err(sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(sync::mpsc::error::TryRecvError::Disconnected) => {
                        tracing::debug!("Shutting down connection maintenance");
                        anyhow::bail!("finished");
                    }
                }
            }

            // eventually peers which failed to return candidates should be retried when enough time has passed
            let retry_missing_candidates_until =
                Instant::now() - retry_peers_missing_candidates_interval;

            // remove all missing candidates which have been retried
            missing.split_off(&Reverse(retry_missing_candidates_until));

            // avoid connecting to the same peer multiple times
            let mut skip_list = missing.values().collect::<Vec<_>>();
            let this_peer = self.connection_manager.get_peer_key().unwrap();
            skip_list.push(&this_peer);

            // if there are no open connections, we need to acquire more
            if let Some(tx) = &live_tx {
                if !live_tx_tracker.still_alive(tx) {
                    let _ = live_tx.take();
                }
            }

            if let Some(ideal_location) = pending_conn_adds.pop_first() {
                if live_tx.is_none() {
                    live_tx = self
                        .acquire_new(ideal_location, &skip_list, &notifier, &live_tx_tracker)
                        .await
                        .map_err(|error| {
                            tracing::debug!(?error, "Shutting down connection maintenance task");
                            error
                        })?;
                } else {
                    pending_conn_adds.insert(ideal_location);
                }
            }

            let neighbor_locations = {
                let peers = self.connection_manager.get_connections_by_location();
                peers
                    .iter()
                    .map(|(loc, conns)| {
                        let conns: Vec<_> = conns
                            .iter()
                            .filter(|conn| {
                                conn.open_at.elapsed() > CONNECTION_AGE_THRESOLD
                                    && !live_tx_tracker.has_live_connection(&conn.location.peer)
                            })
                            .cloned()
                            .collect();
                        (*loc, conns)
                    })
                    .filter(|(_, conns)| !conns.is_empty())
                    .collect()
            };

            let adjustment = self
                .connection_manager
                .topology_manager
                .write()
                .adjust_topology(
                    &neighbor_locations,
                    &self.connection_manager.own_location().location,
                    Instant::now(),
                );
            match adjustment {
                TopologyAdjustment::AddConnections(target_locs) => {
                    pending_conn_adds.extend(target_locs);
                }
                TopologyAdjustment::RemoveConnections(mut should_disconnect_peers) => {
                    for peer in should_disconnect_peers.drain(..) {
                        notifier
                            .send(Either::Right(NodeEvent::DropConnection(
                                peer.peer,
                            )))
                            .await
                            .map_err(|error| {
                                tracing::debug!(
                                    ?error,
                                    "Shutting down connection maintenance task"
                                );
                                error
                            })?;
                    }
                }
                TopologyAdjustment::NoChange => {}
            }

            tokio::select! {
              _ = refresh_density_map.tick() => {
                self.refresh_density_request_cache();
              }
              _ = check_interval.tick() => {}
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self, notifier, live_tx_tracker), fields(peer = %self.connection_manager.pub_key))]
    async fn acquire_new(
        &self,
        ideal_location: Location,
        skip_list: &[&PeerId],
        notifier: &EventLoopNotificationsSender,
        live_tx_tracker: &LiveTransactionTracker,
    ) -> anyhow::Result<Option<Transaction>> {
        let query_target = {
            let router = self.router.read();
            if let Some(t) =
                self.connection_manager
                    .routing(ideal_location, None, skip_list, &router)
            {
                t
            } else {
                return Ok(None);
            }
        };
        let joiner = self.connection_manager.own_location();
        tracing::debug!(
            this_peer = %joiner,
            %query_target,
            %ideal_location,
            "Adding new connections"
        );
        let missing_connections = self.connection_manager.max_connections - self.open_connections();
        let connected = self.connection_manager.connected_peers();
        let id = Transaction::new::<connect::ConnectMsg>();
        live_tx_tracker.add_transaction(query_target.peer.clone(), id);
        let msg = connect::ConnectMsg::Request {
            id,
            target: query_target.clone(),
            msg: connect::ConnectRequest::FindOptimalPeer {
                query_target,
                ideal_location,
                joiner,
                max_hops_to_live: missing_connections,
                skip_list: skip_list
                    .iter()
                    .map(|p| (*p).clone())
                    .chain(connected)
                    .collect(),
            },
        };
        notifier.send(Either::Left(msg.into())).await?;
        Ok(Some(id))
    }
}
