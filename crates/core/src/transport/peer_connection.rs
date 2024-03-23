use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;
use std::vec::Vec;

use crate::transport::packet_data::Unknown;
use aes_gcm::Aes128Gcm;
use futures::stream::FuturesUnordered;
use futures::{Future, FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

mod inbound_stream;
mod outbound_stream;

use super::{
    connection_handler::SerializedMessage,
    packet_data::{self, PacketData},
    received_packet_tracker::ReceivedPacketTracker,
    received_packet_tracker::ReportResult,
    sent_packet_tracker::{ResendAction, SentPacketTracker},
    symmetric_message::{self, SymmetricMessage, SymmetricMessagePayload},
    TransportError,
};
use crate::util::time_source::InstantTimeSrc;

type Result<T = (), E = TransportError> = std::result::Result<T, E>;

// TODO: measure the space overhead of SymmetricMessage::ShortMessage since is likely less than 100
/// The max payload we can send in a single fragment, this MUST be less than packet_data::MAX_DATA_SIZE
/// since we need to account for the space overhead of SymmetricMessage::LongMessage metadata
const MAX_DATA_SIZE: usize = packet_data::MAX_DATA_SIZE - 100;

#[must_use]
pub(super) struct RemoteConnection {
    pub outbound_packets: mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
    pub outbound_symmetric_key: Aes128Gcm,
    pub remote_addr: SocketAddr,
    pub sent_tracker: Arc<parking_lot::Mutex<SentPacketTracker<InstantTimeSrc>>>,
    pub last_packet_id: Arc<AtomicU32>,
    pub inbound_packet_recv: mpsc::Receiver<PacketData<Unknown>>,
    pub inbound_symmetric_key: Aes128Gcm,
    pub my_address: Option<SocketAddr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct StreamId(u32);

impl StreamId {
    pub fn next() -> Self {
        static NEXT_ID: AtomicU32 = AtomicU32::new(0);
        Self(NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Release))
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

type InboundStreamFut =
    Pin<Box<dyn Future<Output = Result<(StreamId, SerializedMessage), StreamId>> + Send>>;

/// The `PeerConnection` struct is responsible for managing the connection with a remote peer.
/// It provides methods for sending and receiving messages to and from the remote peer.
///
/// The `PeerConnection` struct maintains the state of the connection, including the remote
/// connection details, trackers for received and sent packets, and futures for inbound and
/// outbound streams.
///
/// The `send` method is used to send serialized data to the remote peer. If the data size
/// exceeds the maximum allowed size, it is sent as a stream; otherwise, it is sent as a
/// short message.
///
/// The `recv` method is used to receive incoming packets from the remote peer. It listens for
/// incoming packets or receipts, and resends packets if necessary.
///
/// The `process_inbound` method is used to process incoming payloads based on their type.
///
/// The `noop`, `outbound_short_message`, and `outbound_stream` methods are used internally for
/// sending different types of messages.
///
/// The `packet_sending` function is a helper function used to send packets to the remote peer.
#[must_use = "call await on the `recv` function to start listening for incoming messages"]
pub(crate) struct PeerConnection {
    remote_conn: RemoteConnection,
    received_tracker: ReceivedPacketTracker<InstantTimeSrc>,
    inbound_streams: HashMap<StreamId, mpsc::UnboundedSender<(u32, Vec<u8>)>>,
    inbound_stream_futures: FuturesUnordered<InboundStreamFut>,
    outbound_stream_futures: FuturesUnordered<JoinHandle<Result>>,
}

impl PeerConnection {
    pub fn new(remote_conn: RemoteConnection) -> Self {
        Self {
            remote_conn,
            received_tracker: ReceivedPacketTracker::new(),
            inbound_streams: HashMap::new(),
            inbound_stream_futures: FuturesUnordered::new(),
            outbound_stream_futures: FuturesUnordered::new(),
        }
    }

    pub async fn send<T>(&mut self, data: T) -> Result
    where
        T: Serialize + Send + 'static,
    {
        let data = tokio::task::spawn_blocking(move || bincode::serialize(&data).unwrap())
            .await
            .unwrap();
        if data.len() > MAX_DATA_SIZE {
            self.outbound_stream(data).await;
        } else {
            self.outbound_short_message(data).await?;
        }
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>> {
        tracing::trace!(remote = ?self.remote_conn.remote_addr, "waiting for incoming messages");
        // listen for incoming messages or receipts or wait until is time to do anything else again
        let mut resend_check = Some(tokio::time::sleep(tokio::time::Duration::from_secs(1)));
        loop {
            tracing::trace!(remote = ?self.remote_conn.remote_addr, "waiting for incoming messages loop");
            tokio::select! {
                inbound = self.remote_conn.inbound_packet_recv.recv() => {
                    let packet_data = inbound.ok_or(TransportError::ConnectionClosed)?;
                    let Ok(decrypted) = packet_data.try_decrypt_sym(&self.remote_conn.inbound_symmetric_key).map_err(|error| {
                        tracing::debug!(%error, remote = ?self.remote_conn.remote_addr, "Failed to decrypt packet, might be an intro packet or a partial packet");
                    }) else {
                        // just ignore this message
                        // TODO: maybbe check how frequently this happens and decide to drop a connection based on that
                        // if it is partial packets being received too often
                        // TODO: this branch should at much happen UdpPacketsListener::NAT_TRAVERSAL_MAX_ATTEMPTS
                        // for intro packets will be sent than this amount, so we could be checking for that initially
                        tracing::trace!(remote = ?self.remote_conn.remote_addr, "ignoring packet");
                        continue;
                    };
                    let msg = SymmetricMessage::deser(decrypted.data()).unwrap();
                    let SymmetricMessage {
                        packet_id,
                        confirm_receipt,
                        payload,
                    } = msg;
                    #[cfg(test)]
                    {
                        tracing::trace!(
                            remote = %self.remote_conn.remote_addr, %packet_id, %payload, ?confirm_receipt,
                            "received inbound packet"
                        );
                    }
                    self.remote_conn
                        .sent_tracker
                        .lock()
                        .report_received_receipts(&confirm_receipt);
                    tracing::trace!(%packet_id, remote = %self.remote_conn.remote_addr, "received packet");
                    match self.received_tracker.report_received_packet(packet_id) {
                        ReportResult::Ok => {}
                        ReportResult::AlreadyReceived => {
                            tracing::trace!(%packet_id, "already received packet");
                            continue;
                        }
                        ReportResult::QueueFull => {
                            let receipts = self.received_tracker.get_receipts();
                            self.noop(receipts).await?;
                        },
                    }
                    tracing::trace!(%packet_id, "processing inbound packet");
                    if let Some(msg) = self.process_inbound(payload).await.map_err(|error| {
                        tracing::error!(%error, %packet_id, remote = %self.remote_conn.remote_addr, "error processing inbound packet");
                        error
                    })? {
                        tracing::debug!(%packet_id, "returning full stream message");
                        return Ok(msg);
                    }
                }
                inbound_stream = self.inbound_stream_futures.next(), if !self.inbound_stream_futures.is_empty() => {
                    let Some(res) = inbound_stream else {
                        tracing::error!("unexpected no-stream from ongoing_inbound_streams");
                        continue
                    };
                    let Ok((stream_id, msg)) = res else {
                        tracing::error!("unexpected error from ongoing_inbound_streams");
                        // TODO: may leave orphan stream recvs hanging around in this case
                        continue;
                    };
                    self.inbound_streams.remove(&stream_id);
                    // tracing::trace!(%stream_id, "stream finished");
                    return Ok(msg);
                }
                outbound_stream = self.outbound_stream_futures.next(), if !self.outbound_stream_futures.is_empty() => {
                    let Some(res) = outbound_stream else {
                        tracing::error!("unexpected no-stream from ongoing_outbound_streams");
                        continue
                    };
                    res.map_err(|e| TransportError::Other(e.into()))??
                }
                _ = resend_check.take().unwrap_or(tokio::time::sleep(Duration::from_secs(1))) => {
                    loop {
                        tracing::trace!(remote = ?self.remote_conn.remote_addr, "checking for resends");
                        let maybe_resend = self.remote_conn
                            .sent_tracker
                            .lock()
                            .get_resend();
                        match maybe_resend {
                            ResendAction::WaitUntil(wait_until) => {
                                tracing::trace!(
                                    remote = ?self.remote_conn.remote_addr,
                                    wait_time = (std::time::Instant::now() - wait_until).as_secs(),
                                    "waiting for resend"
                                );
                                resend_check = Some(tokio::time::sleep_until(wait_until.into()));
                                break;
                            }
                            ResendAction::Resend(idx, packet) => {
                                tracing::trace!(%idx, remote = ?self.remote_conn.remote_addr, "resending packet");
                                self.remote_conn
                                    .outbound_packets
                                    .send((self.remote_conn.remote_addr, packet.clone()))
                                    .await
                                    .map_err(|_| TransportError::ConnectionClosed)?;
                                tracing::trace!(%idx, remote = ?self.remote_conn.remote_addr, "packet resent");
                                self.remote_conn.sent_tracker.lock().report_sent_packet(idx, packet);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Returns the external address of the peer holding this connection.
    pub fn my_address(&self) -> Option<SocketAddr> {
        self.remote_conn.my_address
    }

    async fn process_inbound(
        &mut self,
        payload: SymmetricMessagePayload,
    ) -> Result<Option<Vec<u8>>> {
        use SymmetricMessagePayload::*;
        match payload {
            ShortMessage { payload } => Ok(Some(payload)),
            AckConnection { .. } => Ok(None),
            GatewayConnection { .. } => Ok(None),
            StreamFragment {
                stream_id,
                total_length_bytes,
                fragment_number,
                payload,
            } => {
                if let Some(sender) = self.inbound_streams.get(&stream_id) {
                    tracing::trace!(%stream_id, %fragment_number, "pushing fragment to existing stream");
                    sender
                        .send((fragment_number, payload))
                        .map_err(|_| TransportError::ConnectionClosed)?;
                    tracing::trace!(%stream_id, %fragment_number, "fragment pushed");
                } else {
                    let (sender, receiver) = mpsc::unbounded_channel();
                    tracing::trace!(%stream_id, %fragment_number, "new stream");
                    self.inbound_streams.insert(stream_id, sender);
                    let mut stream = inbound_stream::InboundStream::new(total_length_bytes);
                    if let Some(msg) = stream.push_fragment(fragment_number, payload) {
                        self.inbound_streams.remove(&stream_id);
                        tracing::trace!(%stream_id, %fragment_number, "stream finished");
                        return Ok(Some(msg));
                    }
                    tracing::trace!(%stream_id, "listening for more fragments");
                    self.inbound_stream_futures
                        .push(inbound_stream::recv_stream(stream_id, receiver, stream).boxed());
                }
                Ok(None)
            }
            NoOp => Ok(None),
        }
    }

    #[inline]
    async fn noop(&mut self, receipts: Vec<u32>) -> Result<()> {
        packet_sending(
            self.remote_conn.remote_addr,
            &self.remote_conn.outbound_packets,
            self.remote_conn
                .last_packet_id
                .fetch_add(1, std::sync::atomic::Ordering::Release),
            &self.remote_conn.outbound_symmetric_key,
            receipts,
            (),
            &self.remote_conn.sent_tracker,
        )
        .await
    }

    #[inline]
    async fn outbound_short_message(&mut self, data: SerializedMessage) -> Result<()> {
        let receipts = self.received_tracker.get_receipts();
        let packet_id = self
            .remote_conn
            .last_packet_id
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        packet_sending(
            self.remote_conn.remote_addr,
            &self.remote_conn.outbound_packets,
            packet_id,
            &self.remote_conn.outbound_symmetric_key,
            receipts,
            symmetric_message::ShortMessage(data),
            &self.remote_conn.sent_tracker,
        )
        .await?;
        Ok(())
    }

    async fn outbound_stream(&mut self, data: SerializedMessage) {
        let stream_id = StreamId::next();
        let task = tokio::spawn(outbound_stream::send_stream(
            stream_id,
            self.remote_conn.last_packet_id.clone(),
            self.remote_conn.outbound_packets.clone(),
            self.remote_conn.remote_addr,
            data,
            self.remote_conn.outbound_symmetric_key.clone(),
            self.remote_conn.sent_tracker.clone(),
        ));
        self.outbound_stream_futures.push(task);
    }
}

async fn packet_sending(
    remote_addr: SocketAddr,
    outbound_packets: &mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
    packet_id: u32,
    outbound_sym_key: &Aes128Gcm,
    confirm_receipt: Vec<u32>,
    payload: impl Into<SymmetricMessagePayload>,
    sent_tracker: &parking_lot::Mutex<SentPacketTracker<InstantTimeSrc>>,
) -> Result<()> {
    // FIXME: here ensure that `confirm_receipt` won't make the packet exceed the max data size
    // if it does, split it to send multiple noop packets with the receipts

    // tracing::trace!(packet_id, "sending packet");
    let packet = SymmetricMessage::serialize_msg_to_packet_data(
        packet_id,
        payload,
        outbound_sym_key,
        confirm_receipt,
    )?;
    outbound_packets
        .send((remote_addr, packet.clone().prepared_send()))
        .await
        .map_err(|_| TransportError::ConnectionClosed)?;
    sent_tracker
        .lock()
        .report_sent_packet(packet_id, packet.prepared_send());
    Ok(())
}

#[cfg(test)]
mod tests {
    use aes_gcm::KeyInit;
    use futures::TryFutureExt;
    use std::net::{Ipv4Addr, SocketAddr};
    use tokio::sync::mpsc;

    use super::{
        inbound_stream::{recv_stream, InboundStream},
        outbound_stream::send_stream,
        *,
    };
    use crate::transport::packet_data::MAX_PACKET_SIZE;

    #[tokio::test]
    async fn test_inbound_outbound_interaction() -> Result<(), Box<dyn std::error::Error>> {
        const MSG_LEN: usize = 1000;
        let (sender, mut receiver) = mpsc::channel(1);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let message: Vec<_> = std::iter::repeat(0)
            .take(MSG_LEN)
            .map(|_| rand::random::<u8>())
            .collect();
        let key = rand::random::<[u8; 16]>();
        let cipher = Aes128Gcm::new(&key.into());
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));

        let stream_id = StreamId::next();
        // Send a long message using the outbound stream
        let outbound = tokio::task::spawn(send_stream(
            stream_id,
            Arc::new(AtomicU32::new(0)),
            sender,
            remote_addr,
            message.clone(),
            cipher.clone(),
            sent_tracker,
        ))
        .map_err(|e| e.into());

        let inbound = async {
            // need to take care of decrypting and deserializing the inbound data before collecting into the message
            let (tx, rx) = mpsc::unbounded_channel();
            let stream = InboundStream::new(MSG_LEN as u64);
            let inbound_msg = tokio::task::spawn(recv_stream(stream_id, rx, stream));
            while let Some((_, network_packet)) = receiver.recv().await {
                let decrypted = PacketData::<_, MAX_PACKET_SIZE>::from_buf(&network_packet)
                    .try_decrypt_sym(&cipher)
                    .map_err(TransportError::PrivateKeyDecryptionError)?;
                let SymmetricMessage {
                    payload:
                        SymmetricMessagePayload::StreamFragment {
                            fragment_number,
                            payload,
                            ..
                        },
                    ..
                } = SymmetricMessage::deser(decrypted.data()).expect("symmetric message")
                else {
                    return Err("unexpected message".into());
                };
                println!("fragment_number: {}", fragment_number);
                tx.send((fragment_number, payload))?;
            }
            let (_, msg) = inbound_msg
                .await?
                .map_err(|_| anyhow::anyhow!("stream failed"))?;
            Ok::<_, Box<dyn std::error::Error>>(msg)
        };

        let (out_res, inbound_msg) = tokio::try_join!(outbound, inbound)?;
        out_res?;
        assert_eq!(message, inbound_msg);
        Ok(())
    }
}
