use std::future::Future;
use std::task::Poll;
use std::vec::Vec;

use aes_gcm::Aes128Gcm;
use futures::Sink;
use serde::Serialize;
use tokio::{net::UdpSocket, sync::mpsc};

use super::{
    connection_handler::{RemoteConnection, SerializedMessage, TransportError},
    packet_data::PacketData,
    symmetric_message::{SymmetricMessage, SymmetricMessagePayload},
};

/// Handles the connection with a remote peer.
///
/// Can be awaited for incoming messages or used to send messages to the remote peer.
pub(crate) struct PeerConnection {
    pub(super) inbound_recv: mpsc::Receiver<PacketData>,
    pub(super) outbound_sender: mpsc::Sender<SerializedMessage>,
    pub(super) inbound_sym_key: Aes128Gcm,
    pub(super) ongoing_stream: Option<ReceiverStream>,
}

impl Future for PeerConnection {
    type Output = Result<Vec<u8>, TransportError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use SymmetricMessagePayload::*;
        let packet_data = match self.inbound_recv.poll_recv(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => {
                // connection finished
                return Poll::Ready(Err(TransportError::ConnectionClosed));
            }
            Poll::Ready(Some(packet)) => packet,
        };
        let decrypted = match packet_data
            .decrypt(&self.inbound_sym_key)
            .map_err(TransportError::PrivateKeyDecryptionError)
        {
            Ok(decrypted) => decrypted,
            Err(e) => return Poll::Ready(Err(e)),
        };
        let SymmetricMessage { payload, .. } = SymmetricMessage::deser(decrypted.data())?;
        // todo: handle out of order messages, repeated messages, missing packets, etc.
        match payload {
            ShortMessage { payload } => Poll::Ready(Ok(payload)),
            AckConnection { .. } => Poll::Ready(Err(TransportError::UnexpectedMessage(
                "AckConnection".into(),
            ))),
            LongMessageFragment {
                total_length,
                index,
                payload,
            } => {
                // todo: IGNORE THIS FOR NOW, needs more work
                let mut stream = self
                    .ongoing_stream
                    .take()
                    .unwrap_or_else(|| ReceiverStream::new(total_length));
                if let Some(msg) = stream.push_fragment(index, payload) {
                    return Poll::Ready(Ok(msg));
                }
                self.ongoing_stream = Some(stream);
                Poll::Pending
            }
        }
    }
}

impl PeerConnection {
    pub async fn send<T: Serialize>(&mut self, data: &T) -> Result<(), TransportError> {
        // todo: improve: careful with blocking while serializing here
        let serialized_data = bincode::serialize(data).unwrap();
        // todo: improve cancel safety just in case, although is unlikely to be an issue
        // when calling this methods because the &mut ref
        self.outbound_sender
            .send(serialized_data)
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;
        Ok(())
    }
}

type StreamBytes = Vec<u8>;

pub(super) struct ReceiverStream {}

impl ReceiverStream {
    fn new(total_length: u64) -> Self {
        todo!()
    }

    /// Returns some if the message has been completely streamed, none otherwise.
    fn push_fragment(&mut self, index: u64, fragment: StreamBytes) -> Option<Vec<u8>> {
        todo!()
    }
}

struct StreamedMessagePart {
    data: PacketData,
    part_start_position: usize,
    message_size: usize,
}

/// Handles breaking a message into parts, encryption, etc.
pub(super) struct SenderStream<'a> {
    socket: &'a UdpSocket,
}

impl<'a> SenderStream<'a> {
    pub fn new(socket: &'a UdpSocket, remote_conn: &mut RemoteConnection) -> Self {
        todo!()
    }
}

impl Sink<StreamBytes> for SenderStream<'_> {
    type Error = SenderStreamError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn start_send(self: std::pin::Pin<&mut Self>, data: StreamBytes) -> Result<(), Self::Error> {
        // we break the message into parts, encrypt them, and send them
        todo!()
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum SenderStreamError {
    #[error("stream closed unexpectedly")]
    Closed,
    #[error("message too big, size: {size}, max size: {max_size}")]
    MessageExceedsLength { size: usize, max_size: usize },
}
