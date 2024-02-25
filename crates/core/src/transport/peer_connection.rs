use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::vec::Vec;

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

type Result<T = ()> = std::result::Result<T, TransportError>;

#[must_use]
pub(super) struct RemoteConnection {
    pub outbound_packets: mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
    pub outbound_symmetric_key: Aes128Gcm,
    pub remote_addr: SocketAddr,
    pub sent_tracker: Arc<Mutex<SentPacketTracker<InstantTimeSrc>>>,
    pub last_packet_id: Arc<AtomicU32>,
    pub inbound_packet_recv: mpsc::Receiver<PacketData>,
    pub inbound_symmetric_key: Aes128Gcm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub(crate) struct StreamId(u32);

impl StreamId {
    fn next() -> Self {
        static NEXT_ID: AtomicU32 = AtomicU32::new(0);
        Self(NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Release))
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Handles the connection with a remote peer.
///
/// Can be awaited for incoming messages or used to send messages to the remote peer.
#[must_use = "call await on the `recv` function to start listening for incoming messages"]
pub(crate) struct PeerConnection {
    remote_conn: RemoteConnection,
    received_tracker: ReceivedPacketTracker<InstantTimeSrc>,
    inbound_streams: HashMap<StreamId, mpsc::Sender<(u32, Vec<u8>)>>,
    ongoing_inbound_streams:
        FuturesUnordered<Pin<Box<dyn Future<Output = Result<SerializedMessage>> + Send>>>,
    ongoing_outbound_streams: FuturesUnordered<Pin<Box<dyn Future<Output = Result> + Send>>>,
    outbound_receipts_notifiers: HashMap<StreamId, mpsc::Sender<u32>>,
}

impl PeerConnection {
    pub fn new(remote_conn: RemoteConnection) -> Self {
        Self {
            remote_conn,
            received_tracker: ReceivedPacketTracker::new(),
            inbound_streams: HashMap::new(),
            ongoing_inbound_streams: FuturesUnordered::new(),
            ongoing_outbound_streams: FuturesUnordered::new(),
            outbound_receipts_notifiers: HashMap::new(),
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
                    let Ok(decrypted) = packet_data.decrypt(&self.remote_conn.inbound_symmetric_key).map_err(|error| {
                        tracing::error!(%error, ?self.remote_conn.remote_addr, "Failed to decrypt packet, might be an intro packet");
                    }) else {
                        // just ignore this message
                        // TODO: this branch should at much happen UdpPacketsListener::NAT_TRAVERSAL_MAX_ATTEMPTS
                        // since never more intro packets will be sent than this amount,
                        // after checking that amount of times we should drop the connection if sending corrupt messages
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
                inbound_stream = self.ongoing_inbound_streams.next(), if !self.ongoing_inbound_streams.is_empty() => {
                    let Some(res) = inbound_stream else {
                        tracing::error!("unexpected no-stream from ongoing_inbound_streams");
                        continue
                    };
                    return res;
                }
                outbound_stream = self.ongoing_outbound_streams.next(), if !self.ongoing_outbound_streams.is_empty() => {
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

    async fn process_inbound(
        &mut self,
        payload: SymmetricMessagePayload,
    ) -> Result<Option<Vec<u8>>> {
        use SymmetricMessagePayload::*;
        match payload {
            ShortMessage { payload } => Ok(Some(payload)),
            AckConnection { .. } => Ok(None),
            GatewayConnection { .. } => Ok(None),
            LongMessageFragment {
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
                    let (sender, mut receiver) = mpsc::channel(1);
                    self.inbound_streams.insert(stream_id, sender);
                    let mut stream = inbound_stream::InboundStream::new(total_length_bytes);
                    if let Some(msg) = stream.push_fragment(fragment_number, payload) {
                        self.inbound_streams.remove(&stream_id);
                        return Ok(Some(msg));
                    }
                    self.ongoing_inbound_streams.push(
                        async move {
                            while let Some((fragment_number, payload)) = receiver.recv().await {
                                if let Some(msg) = stream.push_fragment(fragment_number, payload) {
                                    return Ok(msg);
                                }
                            }
                            Err(TransportError::IncompleteInboundStream(stream_id))
                        }
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
        let (sent_confirm_sender, sent_confirm_recv) = mpsc::channel(1);
        let stream_id = StreamId::next();
        let task = outbound_stream::send_long_message(
            stream_id,
            self.remote_conn.last_packet_id.clone(),
            self.remote_conn.outbound_packets.clone(),
            self.remote_conn.remote_addr,
            data,
            self.remote_conn.outbound_symmetric_key.clone(),
            sent_confirm_recv,
            self.remote_conn.sent_tracker.clone(),
        );
        self.ongoing_outbound_streams.push(task.boxed());
        self.outbound_receipts_notifiers
            .insert(stream_id, sent_confirm_sender);
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
    let payload = SymmetricMessage::serialize_msg_to_packet_data(
        packet_id,
        payload,
        outbound_sym_key,
        confirm_receipt,
    )?;
    let packet: Arc<[u8]> = payload.into();
    outbound_packets
        .send((remote_addr, packet.clone()))
        .await
        .map_err(|_| TransportError::ConnectionClosed)?;
    sent_tracker.lock().report_sent_packet(packet_id, packet);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::peer_connection::inbound_stream::InboundStream;
    use crate::transport::peer_connection::outbound_stream::send_long_message;
    use aes_gcm::KeyInit;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_inbound_outbound_interaction() {
        let (sender, mut receiver) = mpsc::channel(100);
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let message = vec![1, 2, 3, 4, 5];
        let key = rand::random::<[u8; 16]>();
        let cipher = Aes128Gcm::new(&key.into());
        let (sent_confirmed_send, sent_confirmed_recv) = mpsc::channel(100);
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));

        // Send a long message using the outbound stream
        let send_result = send_long_message(
            StreamId::next(),
            Arc::new(AtomicU32::new(0)),
            sender.clone(),
            remote_addr,
            message.clone(),
            cipher.clone(),
            sent_confirmed_recv,
            sent_tracker,
        )
        .await;

        assert!(send_result.is_ok());

        // Create an inbound stream to receive the message
        let mut inbound_stream = InboundStream::new(message.len() as u64);

        // Simulate receiving the message
        while let Some((_, received_message)) = receiver.recv().await {
            let fragment_number = 0; // This is a simplification, in reality you would need to extract the fragment number from the received message
            let received_message = (*received_message).to_vec(); // Convert Arc<[u8]> to Vec<u8>

            let result = inbound_stream.push_fragment(fragment_number, received_message);

            if let Some(complete_message) = result {
                // Check that the received message matches the sent message
                assert_eq!(complete_message, message);
                return;
            }
        }

        panic!("Did not receive complete message");
    }
}
