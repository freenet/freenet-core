#![allow(unused)] // FIXME: remove this attr

use std::{
    ops::Deref,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    conn_manager::{self, ConnectionBridge, Transport},
    message::{Message, MsgType, ProbeRequest, ProbeResponse, Transaction, Visit},
    ring_proto::{self, Location, RingProtocol},
};

pub(crate) struct ProbeProtocol {
    /// Add unit to prevent initialization without calling new
    _void_constructor: (),
}

impl ProbeProtocol {
    pub fn new<CM>(ring_proto: Arc<RingProtocol<CM>>, location: Location) -> Self
    where
        CM: ConnectionBridge + Clone + Send + Sync + 'static,
    {
        let ring_cp = ring_proto.clone();
        let listen_fn = move |requestor, msg| -> conn_manager::Result<()> {
            let (tx_id, my_visit) = if let Message::ProbeRequest(
                tx_id,
                ProbeRequest {
                    hops_to_live,
                    target,
                },
            ) = msg
            {
                (
                    tx_id,
                    Visit {
                        hop: hops_to_live,
                        latency: Duration::from_secs(0),
                        location,
                    },
                )
            } else {
                return Err(conn_manager::ConnError::UnexpectedResponseMessage(msg));
            };

            if my_visit.hop > 0 {
                let ring_cp = ring_cp.clone();
                tokio::spawn(async move {
                    let inc_req = ProbeRequest {
                        hops_to_live: my_visit.hop,
                        target: my_visit.location,
                    };
                    let response = Self::probe(ring_cp.clone(), tx_id, inc_req).await.unwrap();
                    ring_cp
                        .conn_manager
                        .send(requestor, tx_id, (tx_id, response).into());
                });
            } else {
                ring_cp.conn_manager.send(
                    requestor,
                    tx_id,
                    (
                        tx_id,
                        ProbeResponse {
                            visits: vec![my_visit],
                        },
                    )
                        .into(),
                );
            }

            Ok(())
        };
        ring_proto
            .conn_manager
            .listen(<ProbeRequest as MsgType>::msg_type_id(), listen_fn);
        ProbeProtocol {
            _void_constructor: (),
        }
    }

    pub async fn probe<CM>(
        ring: Arc<RingProtocol<CM>>,
        tx_id: Transaction,
        req: ProbeRequest,
    ) -> Result<ProbeResponse, ()>
    where
        CM: ConnectionBridge + 'static,
    {
        if ring.ring.connections_by_location.read().is_empty() {
            return Err(());
        }
        let connections = ring.ring.connections_by_distance(&req.target);
        let (_, next_peer) = connections.first().unwrap();
        let (completion_send, completion) = crossbeam::channel::bounded(1);

        let ring_cp = ring.clone();
        let req_cp = req.clone();
        let start = Instant::now();
        let func = move |peer, msg| -> Result<(), conn_manager::ConnError> {
            let (tx_id, mut inbound_res) = if let Message::ProbeResponse(tx_id, res) = msg {
                (tx_id, res)
            } else {
                return Err(conn_manager::ConnError::UnexpectedResponseMessage(msg));
            };
            let time = start.elapsed();
            let loc = ring_cp
                .location
                .read()
                .ok_or(conn_manager::ConnError::LocationUnknown)?;
            let my_visit = Visit {
                hop: req_cp.hops_to_live,
                latency: time,
                location: loc,
            };
            inbound_res.visits.push(my_visit);
            completion_send.send(inbound_res);
            Ok(())
        };

        ring.conn_manager.send_with_callback(
            *next_peer,
            tx_id,
            (
                tx_id,
                ProbeRequest {
                    hops_to_live: 0,
                    target: req.target,
                },
            )
                .into(),
            func,
        );

        loop {
            match completion.try_recv() {
                Ok(res) => return Ok(res),
                Err(crossbeam::channel::TryRecvError::Disconnected) => return Err(()),
                Err(crossbeam::channel::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

mod message {
    use crate::{message::Visit, ring_proto::Location};

    struct ProbeRequest {
        target: Location,
        hops_to_live: usize,
    }

    struct ProbeResponse {
        visits: Vec<Visit>,
    }
}

pub(crate) mod utils {
    use crate::message::ProbeResponse;

    pub(crate) fn plot_probe_responses(probes: impl IntoIterator<Item = ProbeResponse>) {
        for (probe_idx, probe) in probes.into_iter().enumerate() {
            println!("Probe #{}", probe_idx);
            println!("hop\tlocation\tlatency");
            for visit in probe.visits {
                println!(
                    "{}\t{}\t{}",
                    visit.hop,
                    visit.location,
                    visit.latency.as_secs()
                );
            }
        }
    }
}
