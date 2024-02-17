use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::vec::Vec;

use aes_gcm::Aes128Gcm;
use serde::Serialize;
use tokio::sync::mpsc;

mod inbound_stream;
mod outbound_stream;

use inbound_stream::InboundStream;
pub(super) use outbound_stream::SenderStreamError;

use self::outbound_stream::send_long_message;

use super::received_packet_tracker::ReceivedPacketTracker;
use super::{
    connection_handler::{SerializedMessage, TransportError},
    packet_data::MAX_DATA_SIZE,
    sent_packet_tracker::SentPacketTracker,
    symmetric_message::{SymmetricMessage, SymmetricMessagePayload},
};
use crate::transport::received_packet_tracker::ReportResult;
use crate::util::time_source::InstantTimeSrc;

#[must_use]
pub(super) struct OutboundRemoteConnection {
    pub outbound_packets: mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
    pub outbound_symmetric_key: Aes128Gcm,
    pub remote_is_gateway: bool,
    pub remote_addr: SocketAddr,
    pub sent_tracker: SentPacketTracker<InstantTimeSrc>,
    pub last_message_id: Arc<AtomicU32>,
    pub inbound_packet_recv: mpsc::Receiver<SymmetricMessage>,
}

impl OutboundRemoteConnection {
    async fn send_packet(&mut self, idx: u32, packet: Arc<[u8]>) -> Result<(), SenderStreamError> {
        self.outbound_packets
            .send((self.remote_addr, packet.clone()))
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
pub(crate) struct PeerConnection {
    outbound_connection: OutboundRemoteConnection,
    ongoing_stream: Option<InboundStream>,
    // todo: periodically we need to send a noop message with the receipts if
    // a period has passed without reporting the inbound receipts
    inbound_receipts: Vec<u32>,
    received_tracker: ReceivedPacketTracker<InstantTimeSrc>,
}

impl PeerConnection {
    pub fn new(outbound_connection: OutboundRemoteConnection) -> Self {
        Self {
            outbound_connection,
            ongoing_stream: None,
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
        let size = msg.len();
        // todo: refactor so we reuse logic for keeping track of sent packets
        // for both short and long messages
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
            let packet: Arc<[u8]> = packet.data().into();
            self.outbound_connection
                .outbound_packets
                .send((self.outbound_connection.remote_addr, packet.clone()))
                .await
                .unwrap();
            self.outbound_connection
                .sent_tracker
                .report_sent_packet(msg_id, packet);
        }
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>, TransportError> {
        // listen for incoming messages or receipts or wait until is time to do anything else again
        loop {
            let inbound = self.outbound_connection.inbound_packet_recv.recv().await;
            let SymmetricMessage {
                message_id,
                confirm_receipt,
                payload,
            } = inbound.ok_or(TransportError::ConnectionClosed)?;
            self.inbound_receipts.push(message_id);
            self.outbound_connection
                .sent_tracker
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

async fn send_message(payload: SymmetricMessagePayload) {}

// async fn maybe_resend(&mut self) -> Result<SendCheck, TransportError> {
//     match self.outbound_connection.sent_tracker.get_resend() {
//         ResendAction::WaitUntil(wait) => Ok(SendCheck::NextCheck(wait)),
//         ResendAction::Resend(idx, packet) => {
//             let mut bw_tracker = self.bw_tracker.lock().await;
//             if let Some(send_wait) = bw_tracker.can_send_packet(BANDWITH_LIMIT, packet.len()) {
//                 let time: Instant = Instant::now() + send_wait;
//                 Ok(SendCheck::WaitForBandwith {
//                     time,
//                     pending_packet: (idx, packet),
//                 })
//             } else {
//                 self.outbound_connection.send_packet(idx, packet).await?;
//                 Ok(SendCheck::KeepSending)
//             }
//         }
//     }
// }

// enum SendCheck {
//     NextCheck(Instant),
//     WaitForBandwith {
//         time: Instant,
//         pending_packet: (u32, Arc<[u8]>),
//     },
//     KeepSending,
// }

// // if necessary, check if we need to resend any packet and if possible do so
// if next_sent_check <= Instant::now() {
//     match self.maybe_resend().await? {
//         SendCheck::NextCheck(next) => {
//             next_sent_check = next;
//         }
//         SendCheck::WaitForBandwith {
//             time,
//             pending_packet: (idx, packet),
//         } => {
//             wait_for_sending_until = time;
//             pending_outbound_packet = Some((idx, packet));
//         }
//         SendCheck::KeepSending => continue,
//     }
// }
