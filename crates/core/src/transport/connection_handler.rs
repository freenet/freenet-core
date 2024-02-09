use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::vec::Vec;
use std::{borrow::Cow, time::Duration};

use aes_gcm::{Aes128Gcm, KeyInit};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::net::{ToSocketAddrs, UdpSocket};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task;

use crate::transport::received_packet_tracker::ReportResult;
use crate::util::{CachingSystemTimeSrc, TimeSource};

use super::bw;
use super::peer_connection::SenderStreamError;
use super::received_packet_tracker::ReceivedPacketTracker;
use super::sent_packet_tracker::SentPacketTracker;
use super::{
    crypto::{TransportKeypair, TransportPublicKey},
    packet_data::{MAX_DATA_SIZE, MAX_PACKET_SIZE},
    peer_connection::{PeerConnection, SenderStream},
    symmetric_message::{SymmetricMessage, SymmetricMessagePayload},
    BytesPerSecond, PacketData,
};

const PROTOC_VERSION: [u8; 2] = 1u16.to_le_bytes();

// Constants for exponential backoff
const INITIAL_TIMEOUT: Duration = Duration::from_secs(5);
const TIMEOUT_MULTIPLIER: u64 = 2;
const MAX_TIMEOUT: Duration = Duration::from_secs(60); // Maximum timeout limit

// Constants for interval increase
const INITIAL_INTERVAL: Duration = Duration::from_millis(200);
const INTERVAL_INCREASE_FACTOR: u64 = 2;
const MAX_INTERVAL: Duration = Duration::from_millis(5000); // Maximum interval limit

type ConnectionHandlerMessage = (SocketAddr, Vec<u8>);
pub type SerializedMessage = Vec<u8>;
type PeerChannel = (
    mpsc::Sender<SerializedMessage>,
    mpsc::Receiver<SymmetricMessagePayload>,
);

struct OutboundMessage {
    remote_addr: SocketAddr,
    msg: SerializedMessage,
    recv: mpsc::Receiver<SerializedMessage>,
}

pub(crate) struct ConnectionHandler {
    max_upstream_rate: Arc<arc_swap::ArcSwap<BytesPerSecond>>,
    send_queue: mpsc::Sender<(SocketAddr, ConnectionEvent)>,
    new_connection_notifier: mpsc::Receiver<PeerConnection>,
}

impl ConnectionHandler {
    pub async fn new<S: Socket>(
        keypair: TransportKeypair,
        listen_port: u16,
        is_gateway: bool,
        max_upstream_rate: BytesPerSecond,
    ) -> Result<Self, TransportError> {
        // Bind the UDP socket to the specified port
        let socket = Arc::new(S::bind(("0.0.0.0", listen_port)).await?);

        // Channel buffer is one so senders will await until the receiver is ready, important for bandwidth limiting
        let (conn_handler_sender, conn_handler_receiver) = mpsc::channel(1);
        let (new_connection_sender, new_connection_notifier) = mpsc::channel(100);

        let max_upstream_rate = Arc::new(arc_swap::ArcSwap::from_pointee(max_upstream_rate));
        let transport = UdpPacketsListener {
            is_gateway,
            socket,
            this_peer_keypair: keypair,
            remote_connections: BTreeMap::new(),
            inbound_connections: BTreeMap::new(),
            connection_handler: conn_handler_receiver,
            max_upstream_rate: max_upstream_rate.clone(),
            new_connection_notifier: new_connection_sender,
        };
        let connection_handler = ConnectionHandler {
            max_upstream_rate,
            send_queue: conn_handler_sender,
            new_connection_notifier,
        };

        task::spawn(transport.listen());

        Ok(connection_handler)
    }

    pub async fn connect(
        &mut self,
        remote_public_key: TransportPublicKey,
        remote_addr: SocketAddr,
        remote_is_gateway: bool,
    ) -> Result<PeerConnection, TransportError> {
        if !remote_is_gateway {
            let (open_connection, recv_connection) = oneshot::channel();
            self.send_queue
                .send((
                    remote_addr,
                    ConnectionEvent::ConnectionStart {
                        remote_public_key,
                        open_connection,
                    },
                ))
                .await
                .map_err(|_| TransportError::ChannelClosed)?;
            let ((outbound_sender, inbound_recv), inbound_sym_key) =
                recv_connection.await.map_err(|e| anyhow::anyhow!(e))??;
            Ok(PeerConnection {
                inbound_recv,
                outbound_sender,
                inbound_sym_key,
                ongoing_stream: None,
            })
        } else {
            todo!("establish connection with a gateway")
        }
    }

    pub async fn new_connection(&mut self) -> Option<PeerConnection> {
        self.new_connection_notifier.recv().await
    }

    fn update_max_upstream_rate(&mut self, max_upstream_rate: BytesPerSecond) {
        self.max_upstream_rate.store(Arc::new(max_upstream_rate));
    }
}

/// Make connection handler more testable
pub(super) trait Socket: Sized + Send + Sync + 'static {
    fn bind<A: ToSocketAddrs + Send>(addr: A) -> impl Future<Output = io::Result<Self>> + Send;
    fn recv_from(
        &self,
        buf: &mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + Send;
    fn send_to<A: ToSocketAddrs + Send>(
        &self,
        buf: &[u8],
        target: A,
    ) -> impl Future<Output = io::Result<usize>> + Send;
}

impl Socket for UdpSocket {
    async fn bind<A: ToSocketAddrs + Send>(addr: A) -> io::Result<Self> {
        Self::bind(addr).await
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.recv_from(buf).await
    }

    async fn send_to<A: ToSocketAddrs + Send>(&self, buf: &[u8], target: A) -> io::Result<usize> {
        self.send_to(buf, target).await
    }
}

/// Handles UDP transport internally.
struct UdpPacketsListener<S = UdpSocket> {
    socket: Arc<S>,
    remote_connections: BTreeMap<SocketAddr, RemoteConnection>,
    connection_handler: mpsc::Receiver<(SocketAddr, ConnectionEvent)>,
    this_peer_keypair: TransportKeypair,
    max_upstream_rate: Arc<arc_swap::ArcSwap<BytesPerSecond>>,
    is_gateway: bool,
    /// A new inbound connection that we haven't sent an explicit message yet
    inbound_connections:
        BTreeMap<SocketAddr, (PeerChannel, Aes128Gcm, mpsc::Receiver<SerializedMessage>)>,
    new_connection_notifier: mpsc::Sender<PeerConnection>,
}

#[allow(clippy::large_enum_variant)]
enum ConnectionState {
    /// Initial state of the joiner
    StartOutbound {
        remote_public_key: TransportPublicKey,
    },
    /// Initial state of the joinee, at this point NAT has been already traversed
    RemoteInbound {
        outbound_key: Aes128Gcm,
        /// Encrypted intro packet for comparison
        intro_packet: PacketData,
    },
    /// Second state of the joiner, acknowledging their connection
    AckConnectionOutbound,
}

impl<S: Socket> UdpPacketsListener<S> {
    async fn listen(mut self) -> Result<(), TransportError> {
        let mut peer_messages = FuturesUnordered::new();
        const DEFAULT_BW_TRACKER_WINDOW_SIZE: Duration = Duration::from_secs(10);
        let bw_tracker = Arc::new(Mutex::new(super::bw::PacketBWTracker::new(
            DEFAULT_BW_TRACKER_WINDOW_SIZE,
        )));
        // todo: refactor this loop a bit so the code is more readable
        // todo: we probably need to refactor this a bit so we don't block the socket listening
        // with decryption, deserialization, msg handling etc. so we keep the socket getting new packets
        // from multiple peers as fast as possible, we can wrap socket into an arc so this should be easy to do
        loop {
            let mut buf = [0u8; MAX_PACKET_SIZE];
            tokio::select! {
                // Handling of inbound packets
                recv_result = self.socket.recv_from(&mut buf) => {
                    match recv_result {
                        Ok((size, remote_addr)) => {
                            let remote_conn = self.remote_connections.remove(&remote_addr);
                            match remote_conn {
                                Some(mut remote_conn) => {
                                    // todo: in the future optimize this to just take a PacketData only as large as necessary
                                    let packet_data = PacketData::from(&buf[..size]);
                                    if remote_conn.check_inbound_packet(&packet_data) {
                                        // we received a duplicate of the intro packet from the remote
                                        // we can ignore it since we already have the connection established
                                        continue;
                                    }
                                    let decrypted = packet_data.decrypt(&remote_conn.outbound_symmetric_key).map_err(|e| {
                                        tracing::error!(%e, ?remote_addr, "Failed to decrypt packet");
                                    }).unwrap();
                                    let msg = SymmetricMessage::deser(decrypted.data()).unwrap();
                                    if let SymmetricMessagePayload::AckConnection { result } = &msg.payload {
                                        match result {
                                            Ok(_) => {
                                                if let Some(((outbound_sender, inbound_recv), inbound_sym_key, outbound_receiver)) = self.inbound_connections.remove(&remote_addr) {
                                                    if self.new_connection_notifier.send(PeerConnection {
                                                        inbound_recv,
                                                        outbound_sender,
                                                        inbound_sym_key,
                                                        ongoing_stream: None,
                                                    }).await.is_err() {
                                                        return Err(TransportError::ChannelClosed);
                                                    }
                                                    let socket = self.socket.clone();
                                                    // fixme
                                                    // peer_messages.push(tokio::spawn(async move {
                                                    //     outbound_messages(
                                                    //         socket,
                                                    //         outbound_receiver,
                                                    //         remote_addr
                                                    //     ).await
                                                    // }));
                                                }
                                            }
                                            Err(error) => {
                                                tracing::error!(%error, ?remote_addr, "Failed to establish connection");
                                                continue;
                                            }
                                        }
                                    }
                                    match remote_conn.received_tracker.report_received_packet(msg.message_id) {
                                        ReportResult::Ok => {
                                            if remote_conn.inbound_packet_sender.send(msg.payload).await.is_err() {
                                                // dropping this remote connection since we don't care about their messages anymore
                                                tracing::debug!(%remote_addr, "Remote disconnected");
                                            }
                                        },
                                        ReportResult::AlreadyReceived => {
                                            remote_conn.sent_tracker.report_sent_packet(msg.message_id, packet_data.data().into());
                                        }
                                        ReportResult::QueueFull => todo!(),
                                    }
                                }
                                None => {
                                    // if we received a message, it means that a packet reached us and ports were mapped
                                    // so we can successfully receive messages from the remote
                                    let packet_data = PacketData::from(std::mem::replace(&mut buf, [0; MAX_PACKET_SIZE]));
                                    match self.handle_unrecogized_remote(remote_addr, packet_data).await {
                                        Err(error) => {
                                            tracing::error!(%error, ?remote_addr, "Failed to establish connection");
                                        }
                                        Ok((remote_connection, inbound_recv, inbound_sym_key)) => {
                                            let (outbound_sender, outbound_receiver) = mpsc::channel(1);
                                            self.remote_connections.insert(remote_addr, remote_connection);
                                            self.inbound_connections.insert(
                                                remote_addr,
                                                ((outbound_sender, inbound_recv), inbound_sym_key, outbound_receiver)
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            // todo: this should panic and be propagate to the main task or retry and eventually fail
                            tracing::error!("Failed to receive UDP packet: {:?}", e);
                            return Err(e.into());
                        }
                    }
                },
                // Handling of outbound packets
                res = peer_messages.next(), if !peer_messages.is_empty() => {
                    let res: Option<Result<Result<(), SocketAddr>, tokio::task::JoinError>> = res;
                    let Some(res) = res else {
                        // this should be unreachable, but it wouldn't matter either way
                        // the remote conn has been dropped
                        tracing::error!("peer_messages.next() returned None");
                        continue;
                    };
                    let remote_addr = res.map_err(|e| anyhow::anyhow!(e))?.unwrap_err();
                    tracing::debug!(%remote_addr, "Remote disconnected");
                    self.remote_connections.remove(&remote_addr);
                    continue;
                }
                // Handling of connection events
                send_message = self.connection_handler.recv() => {
                    let Some((remote_addr, event)) = send_message else { return Ok(()); };
                    let ConnectionEvent::ConnectionStart { remote_public_key, open_connection } = event;
                    match self.traverse_nat(remote_addr, ConnectionState::StartOutbound { remote_public_key }).await {
                        Err(error) => {
                            tracing::error!(%error, ?remote_addr, "Failed to establish connection");
                            let _ = open_connection.send(Err(error));
                        }
                        Ok((remote_connection, inbound_recv, inbound_sym_key)) => {
                            let (outbound_sender, outbound_receiver) = mpsc::channel(1);
                            // self.remote_connections.insert(remote_addr, remote_connection);
                            let _ = open_connection.send(Ok(((outbound_sender, inbound_recv), inbound_sym_key)));
                            let socket = self.socket.clone();
                            let bw_c = bw_tracker.clone();
                            peer_messages.push(tokio::spawn(async move {
                                outbound_messages(
                                    socket,
                                    outbound_receiver,
                                    remote_addr,
                                    remote_connection,
                                    bw_c
                                ).await
                            }));
                        }
                    }
                },
            }
        }
    }

    const NAT_TRAVERSAL_MAX_ATTEMPS: usize = 20;

    // fixme: there is a problem when establishing conenction since both peers are trying to connect simultaneously
    // they have an attempt where the connection is outbound and the other is inbound 2 different instances
    // of RemoteConnection will be created and the only the last one may be tracked, this is a race condition
    // that might (not sure) lead to weird behaviour for the packet trackers etc. so need to ensure both connections
    // are consolidated into one properly
    async fn traverse_nat(
        &self,
        remote_addr: SocketAddr,
        mut state: ConnectionState,
    ) -> Result<
        (
            RemoteConnection,
            mpsc::Receiver<SymmetricMessagePayload>,
            Aes128Gcm,
        ),
        TransportError,
    > {
        // Initialize timeout and interval
        let mut timeout = INITIAL_TIMEOUT;
        let mut interval_duration = INITIAL_INTERVAL;
        let mut tick = tokio::time::interval(interval_duration);

        let mut failures = 0;
        let mut packet = [0u8; MAX_PACKET_SIZE];

        let inbound_sym_key_bytes = rand::random::<[u8; 16]>();
        let inbound_sym_key = Aes128Gcm::new(&inbound_sym_key_bytes.into());
        let mut outbound_sym_key: Option<Aes128Gcm> = {
            if let ConnectionState::RemoteInbound { outbound_key, .. } = &state {
                Some(outbound_key.clone())
            } else {
                None
            }
        };

        let mut outbound_intro_packet = None;
        let mut update_state: Option<ConnectionState> = None;

        while failures < Self::NAT_TRAVERSAL_MAX_ATTEMPS {
            if let Some(new_state) = update_state.take() {
                state = new_state;
            }
            match state {
                ConnectionState::StartOutbound {
                    ref remote_public_key,
                } => {
                    // todo: refactor so Start and Remote response use the same code
                    if outbound_intro_packet.is_none() {
                        let mut data = [0u8; { 16 + PROTOC_VERSION.len() }];
                        data[..PROTOC_VERSION.len()].copy_from_slice(&PROTOC_VERSION);
                        data[PROTOC_VERSION.len()..].copy_from_slice(&inbound_sym_key_bytes);
                        outbound_intro_packet =
                            Some(PacketData::<MAX_PACKET_SIZE>::encrypted_with_remote(
                                &data,
                                remote_public_key,
                            ));
                    }
                    let data = outbound_intro_packet.as_ref().unwrap();
                    tracing::debug!("Sending protocol version and inbound key to remote");
                    if let Err(error) = self.socket.send_to(data.data(), remote_addr).await {
                        failures += 1;
                        if failures == Self::NAT_TRAVERSAL_MAX_ATTEMPS {
                            return Err(error.into());
                        }
                        tick.tick().await;
                        continue;
                    }
                }
                ConnectionState::AckConnectionOutbound => {
                    let acknowledgment =
                        SymmetricMessage::ack_ok(outbound_sym_key.as_mut().unwrap())?;
                    let _ = self
                        .socket
                        .send_to(acknowledgment.data(), remote_addr)
                        .await;
                    let mut sent_tracker = SentPacketTracker::new();
                    sent_tracker.report_sent_packet(
                        SymmetricMessage::FIRST_MESSAGE_ID,
                        acknowledgment.data().into(),
                    );
                    // we are connected to the remote and we just send the pub key to them
                    // if they fail to receive it, they will re-request the packet through
                    // the regular error control mechanism
                    return Ok((
                        RemoteConnection {
                            outbound_symmetric_key: outbound_sym_key
                                .expect("should be set at this stage"),
                            inbound_packet_sender: mpsc::channel(1).0,
                            remote_is_gateway: false,
                            remote_addr,
                            received_tracker: ReceivedPacketTracker::new(),
                            sent_tracker,
                            inbound_intro_packet: None,
                            inbound_checked_times: 0,
                            last_message_id: 0,
                        },
                        mpsc::channel(1).1,
                        inbound_sym_key,
                    ));
                }
                ConnectionState::RemoteInbound { .. } => {
                    if outbound_intro_packet.is_none() {
                        // if an intro packet hasn't been created yet, create it
                        let mut data = [0u8; { 16 + PROTOC_VERSION.len() }];
                        data[..PROTOC_VERSION.len()].copy_from_slice(&PROTOC_VERSION);
                        data[PROTOC_VERSION.len()..].copy_from_slice(&inbound_sym_key_bytes);
                        outbound_intro_packet =
                            Some(PacketData::<MAX_PACKET_SIZE>::encrypted_with_cipher(
                                &data,
                                outbound_sym_key.as_ref().unwrap(),
                            ));
                    }
                    // the other peer, which is at the Start state, will receive our inbound key (see below)
                    let data = outbound_intro_packet.as_ref().unwrap();
                    tracing::debug!("Sending back protocol version and inbound key to remote");
                    if let Err(error) = self.socket.send_to(data.data(), remote_addr).await {
                        failures += 1;
                        if failures == Self::NAT_TRAVERSAL_MAX_ATTEMPS {
                            return Err(error.into());
                        }
                        tick.tick().await;
                        continue;
                    }
                }
            }
            let next_inbound = {
                // todo: if a message is received from a different remote, reduce the timeout
                // by the passed time since it doesn't count
                tokio::time::timeout(timeout, self.socket.recv_from(&mut packet)).boxed()
            };
            match next_inbound.await {
                Ok(Ok((size, response_remote))) => {
                    if response_remote != remote_addr {
                        todo!("is a different remote, handle this message");
                    }
                    match state {
                        ConnectionState::StartOutbound { .. } => {
                            let data = PacketData::from(&packet[..size]);
                            // the peer initially received our intro packet and encrypted with our inbound_key
                            // see `handle_unrecogized_remote` for details, so decrypting with our key should work
                            // means that at this point the NAT has been traversed and they are already receiving our messages
                            let Ok(decrypted_packet) = data.decrypt(&inbound_sym_key) else {
                                failures += 1;
                                tracing::debug!("Failed to decrypt packet");
                                continue;
                            };
                            let key = Aes128Gcm::new_from_slice(
                                &decrypted_packet.data()[PROTOC_VERSION.len()..],
                            )
                            .map_err(|_| {
                                TransportError::ConnectionEstablishmentFailure {
                                    cause: "invalid symmetric key".into(),
                                }
                            })?;
                            let protocol_version = &decrypted_packet.data()[..PROTOC_VERSION.len()];
                            if protocol_version != PROTOC_VERSION {
                                let packet = SymmetricMessage::ack_error(&key)?;
                                let _ = self.socket.send_to(packet.data(), remote_addr).await;
                                return Err(TransportError::ConnectionEstablishmentFailure {
                                    cause: format!(
                                        "remote is using a different protocol version: {:?}",
                                        String::from_utf8_lossy(protocol_version)
                                    )
                                    .into(),
                                });
                            }
                            outbound_sym_key = Some(key);
                            // now we need to send back a packet with our asymetric pub key for the remote to have
                            // so it can enroute others to us if necessary
                            state = ConnectionState::AckConnectionOutbound;
                            continue;
                        }
                        ConnectionState::RemoteInbound {
                            // this is the packet encrypted with out RSA pub key
                            ref intro_packet,
                            ..
                        } => {
                            // update_state = Some(ConnectionState::AckConnection);
                            // next packet should be an acknowledgement packet, but might also be a repeated
                            // intro packet so we need to handle that
                            let packet = PacketData::from(&packet[..size]);
                            if packet.is_intro_packet(intro_packet) {
                                continue;
                            }
                            // if is not an intro packet, the connection is successful and we can proceed
                            let (inbound_sender, inbound_recv) = mpsc::channel(1);
                            return Ok((
                                RemoteConnection {
                                    outbound_symmetric_key: outbound_sym_key
                                        .expect("should be set at this stage"),
                                    inbound_packet_sender: inbound_sender,
                                    remote_is_gateway: false,
                                    remote_addr,
                                    received_tracker: ReceivedPacketTracker::new(),
                                    sent_tracker: SentPacketTracker::new(),
                                    inbound_intro_packet: None,
                                    inbound_checked_times: 0,
                                    last_message_id: 0,
                                },
                                inbound_recv,
                                inbound_sym_key,
                            ));
                        }
                        ConnectionState::AckConnectionOutbound => {
                            // we never reach this state cause we break out of this function before checking
                            // for more remote packets
                            unreachable!()
                        }
                    }
                }
                Ok(Err(io_error)) => {
                    failures += 1;
                    tracing::debug!(%io_error, "Failed to receive UDP response");
                }
                Err(_) => {
                    failures += 1;
                    tracing::debug!("Failed to receive UDP response, time out");
                }
            }
            // Update timeout using exponential backoff, capped at MAX_TIMEOUT
            timeout = std::cmp::min(
                Duration::from_secs(timeout.as_secs() * TIMEOUT_MULTIPLIER),
                MAX_TIMEOUT,
            );

            // Update interval, capped at MAX_INTERVAL
            if interval_duration < MAX_INTERVAL {
                interval_duration = std::cmp::min(
                    Duration::from_millis(
                        interval_duration.as_millis() as u64 * INTERVAL_INCREASE_FACTOR,
                    ),
                    MAX_INTERVAL,
                );
                tick = tokio::time::interval(interval_duration);
            }

            tick.tick().await;
        }
        Err(TransportError::ConnectionEstablishmentFailure {
            cause: "max connection attempts reached".into(),
        })
    }

    async fn handle_unrecogized_remote(
        &self,
        remote_addr: SocketAddr,
        inbound_intro_packet: PacketData,
    ) -> Result<
        (
            RemoteConnection,
            mpsc::Receiver<SymmetricMessagePayload>,
            Aes128Gcm,
        ),
        TransportError,
    > {
        // logic for gateway should be slightly different cause we don't need to do nat traversal
        let decrypted_intro_packet = match self
            .this_peer_keypair
            .secret
            .decrypt(inbound_intro_packet.data())
        {
            Ok(req) => req,
            Err(error) => {
                tracing::debug!(%error, "Failed to decrypt packet");
                return Err(error.into());
            }
        };
        let protoc = &decrypted_intro_packet[..PROTOC_VERSION.len()];
        if protoc != PROTOC_VERSION {
            todo!("return error");
        }
        let outbound_key_bytes =
            &decrypted_intro_packet[PROTOC_VERSION.len()..PROTOC_VERSION.len() + 16];
        let outbound_key = Aes128Gcm::new_from_slice(outbound_key_bytes).expect("correct length");
        // now we need to attempt punching through the NAT to the remote connection that can reach us
        let mut res = self
            .traverse_nat(
                remote_addr,
                ConnectionState::RemoteInbound {
                    outbound_key,
                    intro_packet: inbound_intro_packet.clone(),
                },
            )
            .await?;
        // store the original intro packet encrypted with the pub key
        res.0.inbound_intro_packet = Some(inbound_intro_packet);
        Ok(res)
    }
}

const BANDWITH_LIMIT: usize = 1024 * 1024 * 10; // 10 MB/s

#[inline]
async fn outbound_messages(
    socket: Arc<impl Socket>,
    mut outbound_receiver: mpsc::Receiver<SerializedMessage>,
    remote_addr: SocketAddr,
    mut remote_conn: RemoteConnection,
    bw_tracker: Arc<Mutex<bw::PacketBWTracker<impl TimeSource>>>,
) -> Result<(), SocketAddr> {
    loop {
        let msg = outbound_receiver.recv().await.ok_or({
            // dropping this remote connection since we don't care about their messages anymore
            // since the other side dropped the PeerConnectin
            remote_addr
        })?;
        if let Some(wait_time) = bw_tracker
            .lock()
            .await
            .can_send_packet(BANDWITH_LIMIT, msg.len())
        {
            tokio::time::sleep(wait_time).await;
        }
        let size = msg.len();
        send_outbound_msg(&*socket, &mut remote_conn, msg, &bw_tracker)
            .await
            .map_err(|_| remote_addr)?;
        bw_tracker.lock().await.add_packet(size);
    }
}

#[inline]
async fn send_outbound_msg(
    socket: &impl Socket,
    remote_conn: &mut RemoteConnection,
    serialized_data: SerializedMessage,
    bw_tracker: &Arc<Mutex<bw::PacketBWTracker<impl TimeSource>>>,
) -> Result<(), TransportError> {
    let receipts = remote_conn.received_tracker.get_receipts();
    if serialized_data.len() > MAX_DATA_SIZE {
        // todo: WIP, this code path is unlikely to ever complete, need to do the refactor commented above,
        // so the outbound traffic is separated from the inboudn packet listener
        // otherwise we will never be getting the receipts back and be able to finishing this task

        // allow some buffering so we don't suspend the listener task while awaiting for sending multiple notifications
        let (receipts_notifier, receipts_notification) = mpsc::channel(10);
        SenderStream::new(
            socket,
            remote_conn,
            serialized_data,
            receipts_notification,
            bw_tracker,
            BANDWITH_LIMIT,
        )
        .await?;
    } else {
        let msg_id = remote_conn.last_message_id.wrapping_add(1);
        let packet = SymmetricMessage::short_message(
            msg_id,
            serialized_data,
            &remote_conn.outbound_symmetric_key,
            receipts,
        )?;
        socket
            .send_to(packet.data(), remote_conn.remote_addr)
            .await?;
        remote_conn
            .sent_tracker
            .report_sent_packet(msg_id, packet.data().into());
    }
    Ok(())
}

enum ConnectionEvent {
    ConnectionStart {
        remote_public_key: TransportPublicKey,
        open_connection: oneshot::Sender<Result<(PeerChannel, Aes128Gcm), TransportError>>,
    },
}

#[must_use]
pub(super) struct RemoteConnection {
    pub outbound_symmetric_key: Aes128Gcm,
    remote_is_gateway: bool,
    pub remote_addr: SocketAddr,
    inbound_packet_sender: mpsc::Sender<SymmetricMessagePayload>,
    pub received_tracker: ReceivedPacketTracker<CachingSystemTimeSrc>,
    pub sent_tracker: SentPacketTracker<CachingSystemTimeSrc>,
    inbound_intro_packet: Option<PacketData>,
    inbound_checked_times: usize,
    pub last_message_id: u32,
}

impl RemoteConnection {
    fn check_inbound_packet(&mut self, packet: &PacketData) -> bool {
        let mut inbound = false;
        if let Some(inbound_intro_packet) = self.inbound_intro_packet.as_ref() {
            if packet.is_intro_packet(inbound_intro_packet) {
                inbound = true;
            }
        }
        self.inbound_checked_times += 1;
        if self.inbound_checked_times >= UdpPacketsListener::<UdpSocket>::NAT_TRAVERSAL_MAX_ATTEMPS
        {
            // no point in checking more than the max attemps since they won't be sending
            // the intro packet more than this amount of times
            self.inbound_intro_packet = None;
        }
        inbound
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TransportError {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("transport handler channel closed")]
    ChannelClosed,
    #[error("connection to remote closed")]
    ConnectionClosed,
    #[error("failed while establishing connection, reason: {cause}")]
    ConnectionEstablishmentFailure { cause: Cow<'static, str> },
    #[error(transparent)]
    PubKeyDecryptionError(#[from] rsa::errors::Error),
    #[error("{0}")]
    PrivateKeyDecryptionError(aes_gcm::aead::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error(transparent)]
    Serialization(#[from] bincode::Error),
    #[error(transparent)]
    StreamingError(#[from] SenderStreamError),
    #[error("received unexpected message from remote: {0}")]
    UnexpectedMessage(Cow<'static, str>),
}

// TODO: add test for establishing a connection between two non-gateways (at localhost)
// it should be already possible to do this with the current code
// (spawn an other thread for the 2nd peer)
