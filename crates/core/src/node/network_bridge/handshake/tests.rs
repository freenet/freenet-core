use core::panic;
use std::{fmt::Display, sync::Arc, time::Duration};

use aes_gcm::{Aes128Gcm, KeyInit};
use anyhow::{anyhow, bail};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::{
    dev_tool::TransportKeypair,
    operations::connect::{ConnectMsg, ConnectResponse},
    ring::{Connection, PeerKeyLocation, Ring},
    transport::{
        ConnectionEvent, OutboundConnectionHandler, PacketData, RemoteConnection, SymmetricMessage,
        SymmetricMessagePayload, TransportPublicKey, UnknownEncryption,
    },
};

struct TransportMock {
    inbound_sender: mpsc::Sender<PeerConnection>,
    outbound_recv: mpsc::Receiver<(SocketAddr, ConnectionEvent)>,
    /// Outbount messages to peers
    packet_senders: HashMap<SocketAddr, (Aes128Gcm, mpsc::Sender<PacketData<UnknownEncryption>>)>,
    /// Next packet id to use
    packet_id: u32,
    /// Inbound messages from peers
    packet_receivers: Vec<mpsc::Receiver<(SocketAddr, Arc<[u8]>)>>,
    in_key: Aes128Gcm,
    my_addr: SocketAddr,
}

impl TransportMock {
    async fn new_conn(&mut self, addr: SocketAddr) {
        let out_symm_key = Aes128Gcm::new_from_slice(&[0; 16]).unwrap();
        let in_symm_key = Aes128Gcm::new_from_slice(&[1; 16]).unwrap();
        let (conn, packet_sender, packet_recv) =
            PeerConnection::new_test(addr, self.my_addr, out_symm_key, in_symm_key.clone());
        self.inbound_sender.send(conn).await.unwrap();
        tracing::debug!("New inbound connection established");
        self.packet_senders
            .insert(addr, (in_symm_key, packet_sender));
        self.packet_receivers.push(packet_recv);
    }

    async fn new_outbound_conn(
        &mut self,
        addr: SocketAddr,
        callback: oneshot::Sender<Result<crate::transport::RemoteConnection, TransportError>>,
    ) {
        let out_symm_key = Aes128Gcm::new_from_slice(&[0; 16]).unwrap();
        let in_symm_key = Aes128Gcm::new_from_slice(&[1; 16]).unwrap();
        let (conn, packet_sender, packet_recv) =
            PeerConnection::new_remote_test(addr, self.my_addr, out_symm_key, in_symm_key.clone());
        callback
            .send(Ok(conn))
            .map_err(|_| "Failed to send connection")
            .unwrap();
        tracing::debug!("New outbound connection established");
        self.packet_senders
            .insert(addr, (in_symm_key, packet_sender));
        self.packet_receivers.push(packet_recv);
    }

    /// This would happen when a new unsolicited connection is established with a gateway or
    /// when after initialising a connection with a peer via `outbound_recv`, a connection
    /// is successfully established.
    async fn establish_inbound_conn(
        &mut self,
        addr: SocketAddr,
        pub_key: TransportPublicKey,
        hops_to_live: Option<usize>,
    ) {
        let id = Transaction::new::<ConnectMsg>();
        let target_peer_id = PeerId::new(addr, pub_key.clone());
        let target_peer = PeerKeyLocation::from(target_peer_id);
        let hops_to_live = hops_to_live.unwrap_or(10);
        let initial_join_req = ConnectMsg::Request {
            id,
            target: target_peer,
            msg: ConnectRequest::StartJoinReq {
                joiner: None,
                joiner_key: pub_key,
                joiner_location: None,
                hops_to_live,
                max_hops_to_live: hops_to_live,
                skip_connections: HashSet::new(),
                skip_forwards: HashSet::new(),
            },
        };
        self.inbound_msg(
            addr,
            NetMessage::V1(NetMessageV1::Connect(initial_join_req)),
        )
        .await
    }

    async fn inbound_msg(&mut self, addr: SocketAddr, msg: impl Serialize + Display) {
        tracing::debug!(at=?self.my_addr, to=%addr, "Sending message from peer");
        let msg = bincode::serialize(&msg).unwrap();
        let (out_symm_key, packet_sender) = self.packet_senders.get_mut(&addr).unwrap();
        let sym_msg = SymmetricMessage::serialize_msg_to_packet_data(
            self.packet_id,
            msg,
            out_symm_key,
            vec![],
        )
        .unwrap();
        tracing::trace!(at=?self.my_addr, to=%addr, "Sending message to peer");
        packet_sender.send(sym_msg.into_unknown()).await.unwrap();
        tracing::trace!(at=?self.my_addr, to=%addr, "Message sent");
        self.packet_id += 1;
    }

    async fn recv_outbound_msg(&mut self) -> anyhow::Result<NetMessage> {
        let receiver = &mut self.packet_receivers[0];
        let (_, msg) = receiver
            .recv()
            .await
            .ok_or_else(|| anyhow::Error::msg("Failed to receive packet"))?;
        let packet: PacketData<UnknownEncryption> = PacketData::from_buf(&*msg);
        let packet = packet
            .try_decrypt_sym(&self.in_key)
            .map_err(|_| anyhow!("Failed to decrypt packet"))?;
        let msg: SymmetricMessage = bincode::deserialize(packet.data()).unwrap();
        let payload = match msg {
            SymmetricMessage {
                payload: SymmetricMessagePayload::ShortMessage { payload },
                ..
            } => payload,
            SymmetricMessage {
                payload:
                    SymmetricMessagePayload::StreamFragment {
                        total_length_bytes,
                        mut payload,
                        ..
                    },
                ..
            } => {
                let mut remaining = total_length_bytes as usize - payload.len();
                while remaining > 0 {
                    let (_, msg) = receiver
                        .recv()
                        .await
                        .ok_or_else(|| anyhow::Error::msg("Failed to receive packet"))?;
                    let packet: PacketData<UnknownEncryption> = PacketData::from_buf(&*msg);
                    let packet = packet
                        .try_decrypt_sym(&self.in_key)
                        .map_err(|_| anyhow!("Failed to decrypt packet"))?;
                    let msg: SymmetricMessage = bincode::deserialize(packet.data()).unwrap();
                    match msg {
                        SymmetricMessage {
                            payload: SymmetricMessagePayload::StreamFragment { payload: new, .. },
                            ..
                        } => {
                            payload.extend_from_slice(&new);
                            remaining -= new.len();
                        }
                        _ => panic!("Unexpected message type"),
                    }
                }
                payload
            }
            _ => panic!("Unexpected message type"),
        };
        let msg: NetMessage = bincode::deserialize(&payload).unwrap();
        Ok(msg)
    }
}

struct NodeMock {
    establish_conn: HanshakeHandlerMsg,
    _outbound_msg: OutboundMessage,
}

impl NodeMock {
    /// A request from node internals to establish a connection with a peer.
    async fn establish_conn(&self, remote: PeerId, tx: Transaction, is_gw: bool) {
        self.establish_conn
            .establish_conn(remote, tx, is_gw)
            .await
            .unwrap();
    }
}

struct TestVerifier {
    transport: TransportMock,
    node: NodeMock,
}

fn config_handler(
    addr: impl Into<SocketAddr>,
    existing_connections: Option<Vec<Connection>>,
    is_gateway: bool,
) -> (HandshakeHandler, TestVerifier) {
    let (outbound_sender, outbound_recv) = mpsc::channel(100);
    let outbound_conn_handler = OutboundConnectionHandler::new(outbound_sender);
    let (inbound_sender, inbound_recv) = mpsc::channel(100);
    let inbound_conn_handler = InboundConnectionHandler::new(inbound_recv);
    let addr = addr.into();
    let keypair = TransportKeypair::new();
    let mngr = ConnectionManager::default_with_key(keypair.public().clone());
    mngr.try_set_peer_key(addr);
    let router = Router::new(&[]);

    if let Some(connections) = existing_connections {
        for conn in connections {
            let location = conn.get_location().location.unwrap();
            let peer_id = conn.get_location().peer.clone();
            mngr.add_connection(location, peer_id, false);
        }
    }

    let (handler, establish_conn, _outbound_msg) = HandshakeHandler::new(
        inbound_conn_handler,
        outbound_conn_handler,
        mngr,
        Arc::new(RwLock::new(router)),
        None,
        is_gateway,
        None, // test code doesn't need peer_ready
    );
    (
        handler,
        TestVerifier {
            transport: TransportMock {
                inbound_sender,
                outbound_recv,
                packet_senders: HashMap::new(),
                packet_receivers: Vec::new(),
                in_key: Aes128Gcm::new_from_slice(&[0; 16]).unwrap(),
                packet_id: 0,
                my_addr: addr,
            },
            node: NodeMock {
                establish_conn,
                _outbound_msg,
            },
        },
    )
}

async fn start_conn(
    test: &mut TestVerifier,
    addr: SocketAddr,
    pub_key: TransportPublicKey,
    id: Transaction,
    is_gw: bool,
) -> oneshot::Sender<Result<RemoteConnection, TransportError>> {
    test.node
        .establish_conn(PeerId::new(addr, pub_key.clone()), id, is_gw)
        .await;
    let (
        trying_addr,
        ConnectionEvent::ConnectionStart {
            remote_public_key,
            open_connection,
        },
    ) = test
        .transport
        .outbound_recv
        .recv()
        .await
        .ok_or_else(|| anyhow!("failed to get conn start req"))
        .unwrap();
    assert_eq!(trying_addr, addr);
    assert_eq!(remote_public_key, pub_key);
    tracing::debug!("Received connection event");
    open_connection
}

// ============================================================================
// Stream-based tests for HandshakeEventStream
// ============================================================================

/// Helper to get the next event from a HandshakeEventStream
async fn next_stream_event(stream: &mut HandshakeEventStream) -> Result<Event, HandshakeError> {
    use futures::StreamExt;
    stream.next().await.ok_or(HandshakeError::ChannelClosed)?
}

#[tokio::test]
async fn test_stream_gateway_inbound_conn_success() -> anyhow::Result<()> {
    let addr: SocketAddr = ([127, 0, 0, 1], 10000).into();
    let (handler, mut test) = config_handler(addr, None, true);
    let mut stream = HandshakeEventStream::new(handler);

    let remote_addr = ([127, 0, 0, 1], 10001).into();
    let test_controller = async {
        let pub_key = TransportKeypair::new().public().clone();
        test.transport.new_conn(remote_addr).await;
        test.transport
            .establish_inbound_conn(remote_addr, pub_key, None)
            .await;
        Ok::<_, anyhow::Error>(())
    };

    let gw_inbound = async {
        let event =
            tokio::time::timeout(Duration::from_secs(15), next_stream_event(&mut stream)).await??;
        match event {
            Event::InboundConnection { conn, .. } => {
                assert_eq!(conn.remote_addr(), remote_addr);
                Ok(())
            }
            other => bail!("Unexpected event: {:?}", other),
        }
    };
    futures::try_join!(test_controller, gw_inbound)?;
    Ok(())
}

#[tokio::test]
async fn test_stream_gateway_inbound_conn_rejected() -> anyhow::Result<()> {
    let addr: SocketAddr = ([127, 0, 0, 1], 10000).into();
    let (handler, mut test) = config_handler(addr, None, true);
    let mut stream = HandshakeEventStream::new(handler);

    let remote_addr = ([127, 0, 0, 1], 10001).into();
    let remote_pub_key = TransportKeypair::new().public().clone();
    let test_controller = async {
        test.transport.new_conn(remote_addr).await;
        test.transport
            .establish_inbound_conn(remote_addr, remote_pub_key.clone(), None)
            .await;

        // Reject the connection
        let sender_key = TransportKeypair::new().public().clone();
        let acceptor_key = TransportKeypair::new().public().clone();
        let joiner_key = TransportKeypair::new().public().clone();
        let response = NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
            id: Transaction::new::<ConnectMsg>(),
            sender: PeerKeyLocation {
                peer: PeerId::new(addr, sender_key),
                location: Some(Location::random()),
            },
            target: PeerKeyLocation {
                peer: PeerId::new(remote_addr, remote_pub_key),
                location: Some(Location::random()),
            },
            msg: ConnectResponse::AcceptedBy {
                accepted: false,
                acceptor: PeerKeyLocation {
                    peer: PeerId::new(addr, acceptor_key),
                    location: Some(Location::random()),
                },
                joiner: PeerId::new(remote_addr, joiner_key),
            },
        }));

        test.transport.inbound_msg(remote_addr, response).await;
        Ok::<_, anyhow::Error>(())
    };

    let gw_inbound = async {
        // First event: InboundConnection (may be accepted or rejected depending on routing)
        let event =
            tokio::time::timeout(Duration::from_secs(15), next_stream_event(&mut stream)).await??;
        tracing::info!("Received event: {:?}", event);
        Ok(())
    };
    futures::try_join!(test_controller, gw_inbound)?;
    Ok(())
}

#[tokio::test]
async fn test_stream_peer_to_gw_outbound_conn() -> anyhow::Result<()> {
    let addr: SocketAddr = ([127, 0, 0, 1], 10001).into();
    let (handler, mut test) = config_handler(addr, None, false);
    let mut stream = HandshakeEventStream::new(handler);

    let joiner_key = TransportKeypair::new();
    let pub_key = joiner_key.public().clone();
    let id = Transaction::new::<ConnectMsg>();
    let remote_addr: SocketAddr = ([127, 0, 0, 2], 10002).into();

    let test_controller = async {
        let open_connection = start_conn(&mut test, remote_addr, pub_key.clone(), id, true).await;
        test.transport
            .new_outbound_conn(remote_addr, open_connection)
            .await;
        tracing::debug!("Outbound connection established");

        // Wait for and respond to StartJoinReq
        let msg = test.transport.recv_outbound_msg().await?;
        let msg = match msg {
            NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                id: inbound_id,
                msg: ConnectRequest::StartJoinReq { joiner_key, .. },
                ..
            })) => {
                assert_eq!(id, inbound_id);
                let sender = PeerKeyLocation {
                    peer: PeerId::new(remote_addr, pub_key.clone()),
                    location: Some(Location::from_address(&remote_addr)),
                };
                let joiner_peer_id = PeerId::new(addr, joiner_key.clone());
                let target = PeerKeyLocation {
                    peer: joiner_peer_id.clone(),
                    location: Some(Location::random()),
                };
                NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                    id: inbound_id,
                    sender: sender.clone(),
                    target,
                    msg: ConnectResponse::AcceptedBy {
                        accepted: true,
                        acceptor: sender,
                        joiner: joiner_peer_id,
                    },
                }))
            }
            other => bail!("Unexpected message: {:?}", other),
        };
        test.transport.inbound_msg(remote_addr, msg).await;
        Ok::<_, anyhow::Error>(())
    };

    let peer_outbound = async {
        let event =
            tokio::time::timeout(Duration::from_secs(15), next_stream_event(&mut stream)).await??;
        match event {
            Event::OutboundGatewayConnectionSuccessful {
                peer_id,
                connection,
                ..
            } => {
                assert_eq!(peer_id.addr, remote_addr);
                assert_eq!(peer_id.pub_key, pub_key);
                drop(connection);
                Ok(())
            }
            other => bail!("Unexpected event: {:?}", other),
        }
    };

    futures::try_join!(test_controller, peer_outbound)?;
    Ok(())
}

#[tokio::test]
async fn test_stream_peer_to_peer_outbound_conn_succeeded() -> anyhow::Result<()> {
    let addr: SocketAddr = ([127, 0, 0, 1], 10001).into();
    let (handler, mut test) = config_handler(addr, None, false);
    let mut stream = HandshakeEventStream::new(handler);

    let peer_key = TransportKeypair::new();
    let peer_pub_key = peer_key.public().clone();
    let peer_addr = ([127, 0, 0, 2], 10002).into();

    let tx = Transaction::new::<ConnectMsg>();

    let test_controller = async {
        let open_connection =
            start_conn(&mut test, peer_addr, peer_pub_key.clone(), tx, false).await;
        test.transport
            .new_outbound_conn(peer_addr, open_connection)
            .await;

        Ok::<_, anyhow::Error>(())
    };

    let peer_inbound = async {
        let event =
            tokio::time::timeout(Duration::from_secs(15), next_stream_event(&mut stream)).await??;
        match event {
            Event::OutboundConnectionSuccessful {
                peer_id,
                connection,
            } => {
                assert_eq!(peer_id.addr, peer_addr);
                assert_eq!(peer_id.pub_key, peer_pub_key);
                drop(connection);
                Ok(())
            }
            other => bail!("Unexpected event: {:?}", other),
        }
    };

    futures::try_join!(test_controller, peer_inbound)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stream_peer_to_gw_outbound_conn_rejected() -> anyhow::Result<()> {
    let joiner_addr = ([127, 0, 0, 1], 10001).into();
    let (handler, mut test) = config_handler(joiner_addr, None, false);
    let mut stream = HandshakeEventStream::new(handler);

    let gw_key = TransportKeypair::new();
    let gw_pub_key = gw_key.public().clone();
    let gw_addr = ([127, 0, 0, 1], 10000).into();
    let gw_peer_id = PeerId::new(gw_addr, gw_pub_key.clone());
    let gw_pkloc = PeerKeyLocation {
        location: Some(Location::from_address(&gw_peer_id.addr)),
        peer: gw_peer_id.clone(),
    };

    let joiner_key = TransportKeypair::new();
    let joiner_pub_key = joiner_key.public().clone();
    let joiner_peer_id = PeerId::new(joiner_addr, joiner_pub_key.clone());
    let joiner_pkloc = PeerKeyLocation {
        peer: joiner_peer_id.clone(),
        location: Some(Location::from_address(&joiner_peer_id.addr)),
    };

    let tx = Transaction::new::<ConnectMsg>();

    let test_controller = async {
        let open_connection = start_conn(&mut test, gw_addr, gw_pub_key.clone(), tx, true).await;
        test.transport
            .new_outbound_conn(gw_addr, open_connection)
            .await;

        let msg = test.transport.recv_outbound_msg().await?;
        tracing::info!("Received connect request: {:?}", msg);
        let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
            id,
            msg: ConnectRequest::StartJoinReq { .. },
            ..
        })) = msg
        else {
            panic!("unexpected message");
        };
        assert_eq!(id, tx);

        let initial_join_req = ConnectMsg::Response {
            id: tx,
            sender: gw_pkloc.clone(),
            target: joiner_pkloc.clone(),
            msg: ConnectResponse::AcceptedBy {
                accepted: false,
                acceptor: gw_pkloc.clone(),
                joiner: joiner_peer_id.clone(),
            },
        };
        test.transport
            .inbound_msg(
                gw_addr,
                NetMessage::V1(NetMessageV1::Connect(initial_join_req)),
            )
            .await;
        tracing::debug!("Sent initial gw rejected reply");

        for i in 1..Ring::DEFAULT_MAX_HOPS_TO_LIVE {
            let port = i + 10;
            let addr = ([127, 0, port as u8, 1], port as u16).into();
            let acceptor = PeerKeyLocation {
                location: Some(Location::from_address(&addr)),
                peer: PeerId::new(addr, TransportKeypair::new().public().clone()),
            };
            tracing::info!(%acceptor, "Sending forward reply number {i} with status `{}`", i > 3);
            let forward_response = ConnectMsg::Response {
                id: tx,
                sender: gw_pkloc.clone(),
                target: joiner_pkloc.clone(),
                msg: ConnectResponse::AcceptedBy {
                    accepted: i > 3,
                    acceptor: acceptor.clone(),
                    joiner: joiner_peer_id.clone(),
                },
            };
            test.transport
                .inbound_msg(
                    gw_addr,
                    NetMessage::V1(NetMessageV1::Connect(forward_response.clone())),
                )
                .await;

            if i > 3 {
                // Create the successful connection
                async fn establish_conn(
                    test: &mut TestVerifier,
                    i: usize,
                    joiner_addr: SocketAddr,
                ) -> Result<(), anyhow::Error> {
                    let (remote, ev) = tokio::time::timeout(
                            Duration::from_secs(10),
                            test.transport.outbound_recv.recv(),
                        )
                        .await
                        .inspect_err(|error| {
                            tracing::error!(%error, conn_num = %i, "failed while receiving connection events");
                        })
                        .map_err(|_| anyhow!("time out"))?
                        .ok_or( anyhow!("Failed to receive event"))?;
                    let ConnectionEvent::ConnectionStart {
                        open_connection, ..
                    } = ev;
                    let out_symm_key = Aes128Gcm::new_from_slice(&[0; 16]).unwrap();
                    let in_symm_key = Aes128Gcm::new_from_slice(&[1; 16]).unwrap();
                    let (conn, out, inb) = PeerConnection::new_remote_test(
                        remote,
                        joiner_addr,
                        out_symm_key,
                        in_symm_key.clone(),
                    );
                    test.transport
                        .packet_senders
                        .insert(remote, (in_symm_key, out));
                    test.transport.packet_receivers.push(inb);
                    tracing::info!(conn_num = %i, %remote, "Connection established at remote");
                    open_connection
                        .send(Ok(conn))
                        .map_err(|_| anyhow!("failed to open conn"))?;
                    tracing::info!(conn_num = %i, "Returned open conn");
                    Ok(())
                }

                establish_conn(&mut test, i, joiner_addr).await?;
            }
        }

        Ok::<_, anyhow::Error>(())
    };

    let peer_inbound = async {
        let mut conn_count = 0;
        let mut gw_rejected = false;
        for conn_num in 3..Ring::DEFAULT_MAX_HOPS_TO_LIVE {
            let conn_num = conn_num + 2;
            let event =
                tokio::time::timeout(Duration::from_secs(60), next_stream_event(&mut stream))
                    .await
                    .inspect_err(|_| {
                        tracing::error!(%conn_num, "failed while waiting for events");
                    })?
                    .inspect_err(|error| {
                        tracing::error!(%error, %conn_num, "failed while receiving events");
                    })?;
            match event {
                Event::OutboundConnectionSuccessful { peer_id, .. } => {
                    tracing::info!(%peer_id, %conn_num, "Connection established at peer");
                    conn_count += 1;
                }
                Event::OutboundGatewayConnectionRejected { peer_id } => {
                    tracing::info!(%peer_id, "Gateway connection rejected");
                    assert_eq!(peer_id.addr, gw_addr);
                    gw_rejected = true;
                }
                other => bail!("Unexpected event: {:?}", other),
            }
        }
        tracing::debug!("Completed all checks, connection count: {conn_count}");
        assert!(gw_rejected);
        assert_eq!(conn_count, 6);
        Ok(())
    };
    futures::try_join!(test_controller, peer_inbound)?;
    Ok(())
}
