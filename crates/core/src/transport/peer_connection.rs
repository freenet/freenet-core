use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::vec::Vec;

use crate::transport::packet_data::Unknown;
use aes_gcm::Aes128Gcm;
use futures::stream::FuturesUnordered;
use futures::{Future, FutureExt, StreamExt};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

mod inbound_stream;
mod outbound_stream;

use super::{
    connection_handler::SerializedMessage,
    packet_data::{PacketData, MAX_DATA_SIZE},
    received_packet_tracker::ReceivedPacketTracker,
    received_packet_tracker::ReportResult,
    sent_packet_tracker::{ResendAction, SentPacketTracker},
    symmetric_message::{self, SymmetricMessage, SymmetricMessagePayload},
    TransportError,
};
use crate::util::time_source::InstantTimeSrc;

type Result<T = (), E = TransportError> = std::result::Result<T, E>;

#[must_use]
pub(super) struct RemoteConnection {
    pub outbound_packets: mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
    pub outbound_symmetric_key: Aes128Gcm,
    pub remote_addr: SocketAddr,
    pub sent_tracker: Arc<Mutex<SentPacketTracker<InstantTimeSrc>>>,
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
    inbound_streams: HashMap<StreamId, mpsc::Sender<(u32, Vec<u8>)>>,
    inbound_stream_futures: FuturesUnordered<InboundStreamFut>,
    outbound_stream_futures: FuturesUnordered<Pin<Box<dyn Future<Output = Result> + Send>>>,
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
        // listen for incoming messages or receipts or wait until is time to do anything else again
        let mut resend_check = Some(tokio::time::sleep(tokio::time::Duration::from_secs(1)));
        loop {
            tokio::select! {
                inbound = self.remote_conn.inbound_packet_recv.recv() => {
                    let packet_data = inbound.ok_or(TransportError::ConnectionClosed)?;
                    let Ok(decrypted) = packet_data.try_decrypt_sym(&self.remote_conn.inbound_symmetric_key).map_err(|error| {
                        tracing::debug!(%error, ?self.remote_conn.remote_addr, "Failed to decrypt packet, might be an intro packet or a partial packet");
                    }) else {
                        // just ignore this message
                        // TODO: maybbe check how frequently this happens and decide to drop a connection based on that
                        // if it is partial packets being received too often
                        // TODO: this branch should at much happen UdpPacketsListener::NAT_TRAVERSAL_MAX_ATTEMPTS
                        // for intro packets will be sent than this amount, so we could be checking for that initially
                        continue;
                    };
                    let msg = SymmetricMessage::deser(decrypted.data()).unwrap();
                    let SymmetricMessage {
                        packet_id,
                        confirm_receipt,
                        payload,
                    } = msg;
                    self.remote_conn
                        .sent_tracker
                        .lock()
                        .report_received_receipts(&confirm_receipt);
                    match self.received_tracker.report_received_packet(packet_id) {
                        ReportResult::Ok => {
                            if let Some(msg) = self.process_inbound(payload).await? {
                                return Ok(msg);
                            }
                        }
                        ReportResult::AlreadyReceived => {}
                        ReportResult::QueueFull => {
                            let receipts = self.received_tracker.get_receipts();
                            self.noop(receipts).await?;
                        },
                    }
                }
                inbound_stream = self.inbound_stream_futures.next(), if !self.inbound_stream_futures.is_empty() => {
                    let Some(res) = inbound_stream else {
                        tracing::error!("unexpected no-stream from ongoing_inbound_streams");
                        continue
                    };
                    let Ok((stream_id, msg)) = res else {
                        // TODO: may leave orphan stream recvs hanging around in this case
                        continue;
                    };
                    self.inbound_streams.remove(&stream_id);
                    return Ok(msg);
                }
                outbound_stream = self.outbound_stream_futures.next(), if !self.outbound_stream_futures.is_empty() => {
                    let Some(res) = outbound_stream else {
                        tracing::error!("unexpected no-stream from ongoing_outbound_streams");
                        continue
                    };
                    res?
               }
                _ = resend_check.take().expect("should be set") => {
                    loop {
                        let maybe_resend = self.remote_conn
                            .sent_tracker
                            .lock()
                            .get_resend();
                        match maybe_resend {
                            ResendAction::WaitUntil(wait_until) => {
                                resend_check = Some(tokio::time::sleep_until(wait_until.into()));
                                break;
                            }
                            ResendAction::Resend(idx, packet) => {
                                self.remote_conn
                                    .outbound_packets
                                    .send((self.remote_conn.remote_addr, packet.clone()))
                                    .await
                                    .map_err(|_| TransportError::ConnectionClosed)?;
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
                    sender
                        .send((fragment_number, payload))
                        .await
                        .map_err(|_| TransportError::ConnectionClosed)?;
                } else {
                    let (sender, receiver) = mpsc::channel(1);
                    self.inbound_streams.insert(stream_id, sender);
                    let mut stream = inbound_stream::InboundStream::new(total_length_bytes);
                    if let Some(msg) = stream.push_fragment(fragment_number, payload) {
                        self.inbound_streams.remove(&stream_id);
                        return Ok(Some(msg));
                    }
                    self.inbound_stream_futures.push(
                        inbound_stream::recv_stream(stream_id, total_length_bytes, receiver)
                            .boxed(),
                    );
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
        let task = outbound_stream::send_stream(
            stream_id,
            self.remote_conn.last_packet_id.clone(),
            self.remote_conn.outbound_packets.clone(),
            self.remote_conn.remote_addr,
            data,
            self.remote_conn.outbound_symmetric_key.clone(),
            self.remote_conn.sent_tracker.clone(),
        );
        self.outbound_stream_futures.push(task.boxed());
    }
}

async fn packet_sending(
    remote_addr: SocketAddr,
    outbound_packets: &mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
    packet_id: u32,
    outbound_sym_key: &Aes128Gcm,
    confirm_receipt: Vec<u32>,
    payload: impl Into<SymmetricMessagePayload>,
    sent_tracker: &Mutex<SentPacketTracker<InstantTimeSrc>>,
) -> Result<()> {
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

    use crate::transport::packet_data::MAX_PACKET_SIZE;

    use super::{inbound_stream::recv_stream, outbound_stream::send_stream, *};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_inbound_outbound_interaction() -> Result<(), Box<dyn std::error::Error>> {
        let (sender, mut receiver) = mpsc::channel(1);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let message: Vec<_> = std::iter::repeat(0)
            .take(1_000)
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
            let (tx, rx) = mpsc::channel(1);
            let inbound_msg = tokio::task::spawn(recv_stream(stream_id, message.len() as u64, rx));
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
                tx.send((fragment_number, payload)).await?;
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
