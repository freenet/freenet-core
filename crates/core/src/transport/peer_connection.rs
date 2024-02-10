use std::future::Future;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::task::Poll;
use std::vec::Vec;

use aes_gcm::Aes128Gcm;
use serde::Serialize;
use tokio::sync::{mpsc, Mutex};

mod receiver_stream;
mod sender_stream;

use self::sender_stream::SenderStream;
use receiver_stream::ReceiverStream;
pub(super) use sender_stream::SenderStreamError;

use super::bw;
use super::sent_packet_tracker::SentPacketTracker;
use super::symmetric_message::SymmetricMessage;
use super::{
    connection_handler::{SerializedMessage, Socket, TransportError},
    packet_data::MAX_DATA_SIZE,
    symmetric_message::SymmetricMessagePayload,
};
use crate::util::CachingSystemTimeSrc;

const BANDWITH_LIMIT: usize = 1024 * 1024 * 10; // 10 MB/s

#[must_use]
pub(super) struct OutboundRemoteConnection<S> {
    pub socket: Arc<S>,
    pub outbound_symmetric_key: Aes128Gcm,
    pub remote_is_gateway: bool,
    pub remote_addr: SocketAddr,
    pub sent_tracker: SentPacketTracker<CachingSystemTimeSrc>,
    pub last_message_id: u32,
    pub receipts_notifier: mpsc::Receiver<Vec<u32>>,
    pub inbound_recv: mpsc::Receiver<SymmetricMessagePayload>,
}

/// Handles the connection with a remote peer.
///
/// Can be awaited for incoming messages or used to send messages to the remote peer.
pub(crate) struct PeerConnection<S = UdpSocket> {
    outbound_connection: OutboundRemoteConnection<S>,
    ongoing_stream: Option<ReceiverStream>,
    bw_tracker: Arc<Mutex<bw::PacketBWTracker<CachingSystemTimeSrc>>>,
}

impl<S: Socket> Future for PeerConnection<S> {
    type Output = Result<Vec<u8>, TransportError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use SymmetricMessagePayload::*;
        let payload = match self.outbound_connection.inbound_recv.poll_recv(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => {
                // connection finished
                return Poll::Ready(Err(TransportError::ConnectionClosed));
            }
            Poll::Ready(Some(packet)) => packet,
        };
        match payload {
            ShortMessage { payload } => Poll::Ready(Ok(payload)),
            AckConnection { .. } => Poll::Ready(Err(TransportError::UnexpectedMessage(
                "AckConnection".into(),
            ))),
            LongMessageFragment {
                total_length_bytes: total_length,
                fragment_number: index,
                payload,
            } => {
                let mut stream = self
                    .ongoing_stream
                    .take()
                    .unwrap_or_else(|| ReceiverStream::new(total_length)); // TODO: Is index used appropriately?
                if let Some(msg) = stream.push_fragment(index, payload) {
                    return Poll::Ready(Ok(msg));
                }
                self.ongoing_stream = Some(stream);
                Poll::Pending
            }
        }
    }
}

impl<S: Socket> PeerConnection<S> {
    pub fn new(
        outbound_connection: OutboundRemoteConnection<S>,
        bw_tracker: Arc<Mutex<bw::PacketBWTracker<CachingSystemTimeSrc>>>,
    ) -> Self {
        Self {
            outbound_connection,
            ongoing_stream: None,
            bw_tracker,
        }
    }

    pub async fn send<T: Serialize>(&mut self, data: &T) -> Result<(), TransportError> {
        // todo: improve: careful with blocking while serializing here
        let serialized_data = bincode::serialize(data).unwrap();
        self.outbound_messages(serialized_data).await?;
        Ok(())
    }

    async fn outbound_messages(&mut self, msg: SerializedMessage) -> Result<(), TransportError> {
        if let Some(wait_time) = self
            .bw_tracker
            .lock()
            .await
            .can_send_packet(BANDWITH_LIMIT, msg.len())
        {
            tokio::time::sleep(wait_time).await;
        }
        let size = msg.len();
        self.send_outbound_msg(msg).await?;
        self.bw_tracker.lock().await.add_packet(size);
        Ok(())
    }

    async fn send_outbound_msg(
        &mut self,
        serialized_data: SerializedMessage,
    ) -> Result<(), TransportError> {
        let receipts = self
            .outbound_connection
            .receipts_notifier
            .try_recv()
            .unwrap_or_default();
        if serialized_data.len() > MAX_DATA_SIZE {
            // todo: WIP, this code path is unlikely to ever complete, need to do the refactor commented above,
            // so the outbound traffic is separated from the inboudn packet listener
            // otherwise we will never be getting the receipts back and be able to finishing this task

            // allow some buffering so we don't suspend the listener task while awaiting for sending multiple notifications
            let stream = SenderStream::new(
                &mut self.outbound_connection,
                serialized_data,
                &self.bw_tracker,
                BANDWITH_LIMIT,
            );
            stream.await?;
        } else {
            let msg_id = self.outbound_connection.last_message_id.wrapping_add(1);
            let packet = SymmetricMessage::short_message(
                msg_id,
                serialized_data,
                &self.outbound_connection.outbound_symmetric_key,
                receipts,
            )?;
            self.outbound_connection
                .socket
                .send_to(packet.data(), self.outbound_connection.remote_addr)
                .await?;
            self.outbound_connection
                .sent_tracker
                .report_sent_packet(msg_id, packet.data().into());
        }
        Ok(())
    }
}
