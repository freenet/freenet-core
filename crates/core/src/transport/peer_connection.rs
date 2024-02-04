use std::collections::BTreeMap;
use std::future::Future;
use std::task::Poll;
use std::vec::Vec;

use aes_gcm::Aes128Gcm;
use futures::Sink;
use serde::Serialize;
use tokio::{net::UdpSocket, sync::mpsc};

use super::{
    connection_handler::{RemoteConnection, SerializedMessage, Socket, TransportError},
    crypto::TransportPublicKey,
    packet_data::PacketData,
    symmetric_message::SymmetricMessagePayload,
};

/// Handles the connection with a remote peer.
///
/// Can be awaited for incoming messages or used to send messages to the remote peer.
pub(crate) struct PeerConnection {
    pub(super) inbound_recv: mpsc::Receiver<SymmetricMessagePayload>,
    pub(super) outbound_sender: mpsc::Sender<SerializedMessage>,
    pub(super) inbound_sym_key: Aes128Gcm,
    pub(super) ongoing_stream: Option<ReceiverStream>,
    /// In case the connection is from a joiner they will send back their public key
    /// so we can handle that back to other peers in the network in case they want to connect
    /// with this joiner.
    pub(super) pub_key: Option<TransportPublicKey>,
}

impl Future for PeerConnection {
    type Output = Result<Vec<u8>, TransportError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use SymmetricMessagePayload::*;
        let payload = match self.inbound_recv.poll_recv(cx) {
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
                total_length,
                index,
                payload,
            } => {
                let mut stream = self
                    .ongoing_stream
                    .take()
                    .unwrap_or_else(|| ReceiverStream::new(total_length, index));
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

// todo:  unit test
pub(super) struct ReceiverStream {
    start_index: u64,
    total_length: u64,
    last_contiguous: u64,
    received_fragments: u64,
    fragments: BTreeMap<u64, Vec<u8>>,
    message: Vec<u8>,
}

impl ReceiverStream {
    fn new(total_length: u64, start_index: u64) -> Self {
        Self {
            start_index,
            total_length,
            last_contiguous: start_index,
            received_fragments: 0,
            fragments: BTreeMap::new(),
            message: vec![],
        }
    }

    /// Returns some if the message has been completely streamed, none otherwise.
    fn push_fragment(&mut self, index: u64, mut fragment: StreamBytes) -> Option<Vec<u8>> {
        self.received_fragments += 1;
        if index == self.last_contiguous + 1 {
            self.last_contiguous = index;
            self.message.append(&mut fragment);
            self.get_if_finished()
        } else {
            self.fragments.insert(index, fragment);
            while let Some((idx, mut v)) = self.fragments.pop_first() {
                if idx == self.last_contiguous + 1 {
                    self.last_contiguous += 1;
                    self.message.append(&mut v);
                } else {
                    self.fragments.insert(idx, v);
                    break;
                }
            }
            self.get_if_finished()
        }
    }

    fn get_if_finished(&mut self) -> Option<Vec<u8>> {
        if self.message.len() as u64 == self.total_length {
            Some(std::mem::take(&mut self.message))
        } else {
            None
        }
    }
}

struct StreamedMessagePart {
    data: PacketData,
    part_start_position: usize,
    message_size: usize,
}

/// Handles breaking a message into parts, encryption, etc.
pub(super) struct SenderStream<'a, S = UdpSocket> {
    socket: &'a S,
}

impl<'a> SenderStream<'a> {
    pub fn new<S: Socket>(socket: &'a S, remote_conn: &mut RemoteConnection) -> Self {
        todo!()
    }
}

impl<S> Sink<StreamBytes> for SenderStream<'_, S> {
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
