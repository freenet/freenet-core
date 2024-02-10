use std::future::Future;
use std::task::Poll;
use std::vec::Vec;

use aes_gcm::Aes128Gcm;
use serde::Serialize;
use tokio::sync::mpsc;

use receiver_stream::ReceiverStream;

use super::{
    connection_handler::{SerializedMessage, TransportError},
    symmetric_message::SymmetricMessagePayload,
};

pub(crate) mod receiver_stream;
pub(crate) mod sender_stream;

/// Handles the connection with a remote peer.
///
/// Can be awaited for incoming messages or used to send messages to the remote peer.
pub(crate) struct PeerConnection {
    pub(super) inbound_recv: mpsc::Receiver<SymmetricMessagePayload>,
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
