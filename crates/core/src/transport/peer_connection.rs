use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::vec::Vec;

use aes_gcm::Aes128Gcm;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::mpsc;

mod inbound_stream;
mod outbound_stream;

use inbound_stream::InboundStream;
use tokio::task::JoinHandle;

use super::received_packet_tracker::ReceivedPacketTracker;
use super::sent_packet_tracker::ResendAction;
use super::{
    connection_handler::{SerializedMessage, TransportError},
    packet_data::MAX_DATA_SIZE,
    sent_packet_tracker::SentPacketTracker,
    symmetric_message::{SymmetricMessage, SymmetricMessagePayload},
};
use crate::transport::packet_data::PacketData;
use crate::transport::received_packet_tracker::ReportResult;
use crate::util::time_source::InstantTimeSrc;

type Result<T = ()> = std::result::Result<T, TransportError>;

#[must_use]
pub(super) struct OutboundRemoteConnection {
    pub outbound_packets: mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
    pub outbound_symmetric_key: Aes128Gcm,
    pub remote_is_gateway: bool,
    pub remote_addr: SocketAddr,
    pub sent_tracker: Arc<Mutex<SentPacketTracker<InstantTimeSrc>>>,
    pub last_message_id: Arc<AtomicU32>,
    pub inbound_packet_recv: mpsc::Receiver<SymmetricMessage>,
}

/// Handles the connection with a remote peer.
///
/// Can be awaited for incoming messages or used to send messages to the remote peer.
#[must_use = "call await on the `recv` function to start listening for incoming messages"]
pub(crate) struct PeerConnection {
    outbound_connection: OutboundRemoteConnection,
    // todo: periodically we need to send a noop message with the receipts if
    // a period has passed without reporting the inbound receipts
    inbound_receipts: Vec<u32>,
    received_tracker: ReceivedPacketTracker<InstantTimeSrc>,
    ongoing_inbound_stream: Option<InboundStream>,
    ongoing_outbound_streams: FuturesUnordered<JoinHandle<Result>>,
    outbound_receipts_notifiers: HashMap<u32, mpsc::Sender<u32>>,
}

impl PeerConnection {
    pub fn new(outbound_connection: OutboundRemoteConnection) -> Self {
        Self {
            outbound_connection,
            ongoing_inbound_stream: None,
            inbound_receipts: vec![],
            received_tracker: ReceivedPacketTracker::new(),
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
                inbound = self.outbound_connection.inbound_packet_recv.recv() => {
                    let SymmetricMessage {
                        message_id,
                        confirm_receipt,
                        payload,
                    } = inbound.ok_or(TransportError::ConnectionClosed)?;
                    self.inbound_receipts.push(message_id);
                    self.outbound_connection
                        .sent_tracker
                        .lock()
                        .report_received_receipts(&confirm_receipt);
                    match self.received_tracker.report_received_packet(message_id) {
                        ReportResult::Ok => {
                            if let Some(msg) = self.process_inbound(payload).await? {
                                return Ok(msg);
                            }
                        }
                        ReportResult::AlreadyReceived => {}
                        ReportResult::QueueFull => todo!(),
                    }
                }
                stream = self.ongoing_outbound_streams.next(), if !self.ongoing_outbound_streams.is_empty() => {
                    let Some(stream) = stream else {
                        tracing::error!("unexpected no-stream from ongoing_outbound_streams");
                        continue
                    };
                    let Ok(res) = stream else {
                        tracing::error!("unexpected join error from ongoing_outbound_streams");
                        continue
                    };
                    res?
                }
                _ = resend_check.take().expect("should be set") => {
                    loop {
                        let maybe_resend = self.outbound_connection
                            .sent_tracker
                            .lock()
                            .get_resend();
                        match maybe_resend {
                            ResendAction::WaitUntil(wait_until) => {
                                resend_check = Some(tokio::time::sleep_until(wait_until.into()));
                                break;
                            }
                            ResendAction::Resend(idx, packet) => {
                                self.outbound_connection
                                    .outbound_packets
                                    .send((self.outbound_connection.remote_addr, packet.clone()))
                                    .await
                                    .map_err(|_| TransportError::ConnectionClosed)?;
                                self.outbound_connection.sent_tracker.lock().report_sent_packet(idx, packet);
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
            AckConnection { .. } => Err(TransportError::UnexpectedMessage("AckConnection".into())),
            LongMessageFragment {
                message_id,
                total_length_bytes: total_length,
                fragment_number: index,
                payload: fragment,
            } => {
                // TODO: change to make multiplexing possible, use message_id for identifying the stream which fragment belongs to
                let mut stream = self
                    .ongoing_inbound_stream
                    .take()
                    .unwrap_or_else(|| InboundStream::new(total_length));
                if let Some(msg) = stream.push_fragment(index, fragment) {
                    return Ok(Some(msg));
                }
                self.ongoing_inbound_stream = Some(stream);
                Ok(None)
            }
        }
    }

    async fn outbound_short_message(&mut self, data: SerializedMessage) -> Result {
        let msg_id = self
            .outbound_connection
            .last_message_id
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        let payload = SymmetricMessage::short_message(
            msg_id,
            data,
            &self.outbound_connection.outbound_symmetric_key,
            std::mem::take(&mut self.inbound_receipts),
        )?;
        packet_sending(
            self.outbound_connection.remote_addr,
            &self.outbound_connection.outbound_packets,
            msg_id,
            payload,
            &self.outbound_connection.sent_tracker,
        )
        .await
    }

    async fn outbound_stream(&mut self, data: SerializedMessage) {
        let (sent_confirm_sender, sent_confirm_recv) = mpsc::channel(1);
        let stream_id = self
            .outbound_connection
            .last_message_id
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        let task = outbound_stream::send_long_message(
            stream_id,
            self.outbound_connection.last_message_id.clone(),
            self.outbound_connection.outbound_packets.clone(),
            self.outbound_connection.remote_addr,
            data,
            self.outbound_connection.outbound_symmetric_key.clone(),
            sent_confirm_recv,
            self.outbound_connection.sent_tracker.clone(),
        );
        let task = tokio::spawn(task);
        self.ongoing_outbound_streams.push(task);
        self.outbound_receipts_notifiers
            .insert(stream_id, sent_confirm_sender);
    }
}

async fn packet_sending(
    remote_addr: SocketAddr,
    outbound_packets: &mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
    msg_id: u32,
    payload: PacketData,
    sent_tracker: &Mutex<SentPacketTracker<InstantTimeSrc>>,
) -> Result {
    let packet: Arc<[u8]> = payload.into();
    outbound_packets
        .send((remote_addr, packet.clone()))
        .await
        .map_err(|_| TransportError::ConnectionClosed)?;
    sent_tracker.lock().report_sent_packet(msg_id, packet);
    Ok(())
}
