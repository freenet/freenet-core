use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Instant;
use std::vec::Vec;

use aes_gcm::Aes128Gcm;
use serde::Serialize;
use tokio::sync::{mpsc, Mutex};

mod inbound_stream;
mod outbound_stream;

use inbound_stream::InboundStream;
pub(super) use outbound_stream::SenderStreamError;

use self::outbound_stream::send_long_message;

use super::received_packet_tracker::ReceivedPacketTracker;
use super::{
    bw,
    connection_handler::{SerializedMessage, Socket, TransportError},
    packet_data::MAX_DATA_SIZE,
    sent_packet_tracker::ResendAction,
    sent_packet_tracker::SentPacketTracker,
    symmetric_message::{SymmetricMessage, SymmetricMessagePayload},
};
use crate::transport::received_packet_tracker::ReportResult;
use crate::util::time_source::InstantTimeSrc;

const BANDWITH_LIMIT: usize = 1024 * 1024 * 10; // 10 MB/s

#[must_use]
pub(super) struct OutboundRemoteConnection<S> {
    pub socket: Arc<S>,
    pub outbound_symmetric_key: Aes128Gcm,
    pub remote_is_gateway: bool,
    pub remote_addr: SocketAddr,
    pub sent_tracker: SentPacketTracker<InstantTimeSrc>,
    pub last_message_id: Arc<AtomicU32>,
    pub inbound_packet_recv: mpsc::Receiver<SymmetricMessage>,
}

impl<S: Socket> OutboundRemoteConnection<S> {
    async fn send_packet(&mut self, idx: u32, packet: Arc<[u8]>) -> Result<(), SenderStreamError> {
        self.socket
            .send_to(&packet, self.remote_addr)
            .await
            .map_err(|_| SenderStreamError::Closed)?;
        self.sent_tracker.report_sent_packet(idx, packet);
        Ok(())
    }
}

/// Handles the connection with a remote peer.
///
/// Can be awaited for incoming messages or used to send messages to the remote peer.
#[must_use = "call await on the `recv` function to start listening for incoming messages"]
pub(crate) struct PeerConnection<S = UdpSocket> {
    outbound_connection: OutboundRemoteConnection<S>,
    ongoing_stream: Option<InboundStream>,
    bw_tracker: Arc<Mutex<bw::PacketBWTracker<InstantTimeSrc>>>,
    // todo: periodically we need to send a noop message with the receipts if
    // a period has passed without reporting the inbound receipts
    inbound_receipts: Vec<u32>,
    received_tracker: ReceivedPacketTracker<InstantTimeSrc>,
}

impl<S: Socket> PeerConnection<S> {
    pub fn new(
        outbound_connection: OutboundRemoteConnection<S>,
        bw_tracker: Arc<Mutex<bw::PacketBWTracker<InstantTimeSrc>>>,
    ) -> Self {
        Self {
            outbound_connection,
            ongoing_stream: None,
            bw_tracker,
            inbound_receipts: vec![],
            received_tracker: ReceivedPacketTracker::new(),
        }
    }

    pub async fn send<T>(&mut self, data: T) -> Result<(), TransportError>
    where
        T: Serialize + Send + 'static,
    {
        let msg = tokio::task::spawn_blocking(move || bincode::serialize(&data).unwrap())
            .await
            .unwrap();
        if let Some(wait_time) = self
            .bw_tracker
            .lock()
            .await
            .can_send_packet(BANDWITH_LIMIT, msg.len())
        {
            tokio::time::sleep(wait_time).await;
        }
        let size = msg.len();
        if size > MAX_DATA_SIZE {
            self.send_long(msg).await;
        } else {
            let msg_id = self
                .outbound_connection
                .last_message_id
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            let packet = SymmetricMessage::short_message(
                msg_id,
                msg,
                &self.outbound_connection.outbound_symmetric_key,
                std::mem::take(&mut self.inbound_receipts),
            )?;
            self.outbound_connection
                .socket
                .send_to(packet.data(), self.outbound_connection.remote_addr)
                .await?;
            self.outbound_connection
                .sent_tracker
                .report_sent_packet(msg_id, packet.data().into());
        }
        self.bw_tracker.lock().await.add_packet(size);
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>, TransportError> {
        let mut next_sent_check = Instant::now();
        let mut wait_for_sending_until = Instant::now();
        let mut pending_outbound_packet: Option<(u32, Arc<[u8]>)> = None;
        loop {
            // if there are pending packets just send them out first
            if let Some((idx, pending_packet)) = pending_outbound_packet.take() {
                if Instant::now() >= wait_for_sending_until {
                    self.outbound_connection
                        .send_packet(idx, pending_packet)
                        .await?;
                }
            }

            // if necessary, check if we need to resend any packet and if possible do so
            if next_sent_check <= Instant::now() {
                match self.maybe_resend().await? {
                    SendCheck::NextCheck(next) => {
                        next_sent_check = next;
                    }
                    SendCheck::WaitForBandwith {
                        time,
                        pending_packet: (idx, packet),
                    } => {
                        wait_for_sending_until = time;
                        pending_outbound_packet = Some((idx, packet));
                    }
                    SendCheck::KeepSending => continue,
                }
            }

            // listen for incoming messages or receipts or wait until is time to do anything else again
            tokio::select! {
                inbound = self.outbound_connection.inbound_packet_recv.recv() => {
                    let SymmetricMessage { message_id, confirm_receipt, payload } = inbound.ok_or(TransportError::ConnectionClosed)?;
                    self.inbound_receipts.push(message_id);
                    self.outbound_connection.sent_tracker.report_received_receipts(&confirm_receipt);
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
                _ = tokio::time::sleep_until(next_sent_check.into()) => {}
                _ = tokio::time::sleep_until(wait_for_sending_until.into()), if pending_outbound_packet.is_some() => {}
            }
        }
    }

    async fn maybe_resend(&mut self) -> Result<SendCheck, TransportError> {
        match self.outbound_connection.sent_tracker.get_resend() {
            ResendAction::WaitUntil(wait) => Ok(SendCheck::NextCheck(wait)),
            ResendAction::Resend(idx, packet) => {
                let mut bw_tracker = self.bw_tracker.lock().await;
                if let Some(send_wait) = bw_tracker.can_send_packet(BANDWITH_LIMIT, packet.len()) {
                    let time: Instant = Instant::now() + send_wait;
                    Ok(SendCheck::WaitForBandwith {
                        time,
                        pending_packet: (idx, packet),
                    })
                } else {
                    self.outbound_connection.send_packet(idx, packet).await?;
                    Ok(SendCheck::KeepSending)
                }
            }
        }
    }

    async fn process_inbound(
        &mut self,
        payload: SymmetricMessagePayload,
    ) -> Result<Option<Vec<u8>>, TransportError> {
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
                    .ongoing_stream
                    .take()
                    .unwrap_or_else(|| InboundStream::new(total_length));
                if let Some(msg) = stream.push_fragment(index, fragment) {
                    return Ok(Some(msg));
                }
                self.ongoing_stream = Some(stream);
                Ok(None)
            }
        }
    }

    async fn send_long(&mut self, data: SerializedMessage) {
        // send_long_message(
        //     &mut self.outbound_connection,
        //     serialized_data,
        //     &self.bw_tracker,
        //     BANDWITH_LIMIT,
        // )
        // .await?;
        let task = tokio::spawn(async move {});
    }
}

enum SendCheck {
    NextCheck(Instant),
    WaitForBandwith {
        time: Instant,
        pending_packet: (u32, Arc<[u8]>),
    },
    KeepSending,
}
