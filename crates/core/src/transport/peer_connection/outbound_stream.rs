use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use aes_gcm::Aes128Gcm;
use futures::channel::mpsc;
use futures::SinkExt;

use crate::transport::{
    connection_handler::Socket, packet_data, symmetric_message::SymmetricMessage,
};

use super::{OutboundRemoteConnection, PeerConnection};

pub(crate) type StreamBytes = Vec<u8>;

// TODO: measure the space overhead of SymmetricMessage::LongMessage since is likely less than 100
/// The max payload we can send in a single fragment, this MUST be less than packet_data::MAX_DATA_SIZE
/// since we need to account for the space overhead of SymmetricMessage::LongMessage metadata
const MAX_DATA_SIZE: usize = packet_data::MAX_DATA_SIZE - 100;

/*

Remote A:  t0 Vec<u8> ------ -> Stream<(Packet, Socket)>
Remote B:  ------ t1 Vec<u8> -> Stream<(Packet, Socket)>   ----> channel(1) --->  (thread) OutboundTrafficChannel<Packet> udp_socket.send(packet)
Remote C:  ------ t2 Vec<u8> -> Stream<(Packet, Socket)>


async fn sender_spot(...) {
    let bw_tracker;
    // let map: HashMap<SocketAddr, SentPacketTracker> = HashMap::new();
    while let Some((socket_addr, packet, report_sent)) = self.outbound_packet.recv().await {
        if bw_tracker.can_send_packet(packet.size()) {
            self.socket.send(socket_addr, packet).await;
            bw_tracker.report_sent_packet(now, packet_size);
        //    report_sent.await;
        }
    }
}
*/

// todo: unit test
/// Handles sending a long message which is not being streamed,
/// streaming messages will be tackled differently, in the interim time before
/// the necessary changes are done to the codebase we will use this function
pub(super) async fn send_long_message(
    // remote_conn: &mut OutboundRemoteConnection<impl Socket>,
    last_message_id: Arc<AtomicU32>,
    mut sender: mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
    remote_socket: SocketAddr,
    mut message: StreamBytes,
    outbound_symmetric_key: Aes128Gcm,
) -> Result<(), SenderStreamError> {
    let total_length_bytes = message.len() as u32;
    let start_index = last_message_id.fetch_add(1, std::sync::atomic::Ordering::Release);
    let mut total_messages = message.len() / MAX_DATA_SIZE;
    total_messages += if message.len() % MAX_DATA_SIZE == 0 {
        0
    } else {
        1
    };
    let mut sent_not_confirmed = HashSet::new();
    let mut sent_confirmed = 0;
    let mut confirm_receipts = Vec::new();

    loop {
        let sent_so_far = sent_confirmed + sent_not_confirmed.len();
        if sent_so_far < total_messages {
            let mut rest = {
                if message.len() > MAX_DATA_SIZE {
                    message.split_off(MAX_DATA_SIZE)
                } else {
                    std::mem::take(&mut message)
                }
            };
            std::mem::swap(&mut message, &mut rest);
            let idx = start_index + sent_so_far as u32 + 1;
            let fragment: Arc<[u8]> = SymmetricMessage::fragmented_message(
                start_index,
                total_length_bytes as u64,
                sent_so_far as u32 + 1,
                rest,
                &outbound_symmetric_key,
                std::mem::take(&mut confirm_receipts),
            )?
            .into();
            // todo: this is blocking, but hopefully meaningless, measure and improve if necessary
            sent_not_confirmed.insert(idx);
            // send_packet(&connection, remote_socket, fragment).await?;
            sender.send((remote_socket, fragment)).await.unwrap();
            continue;
        }

        if message.is_empty() && sent_not_confirmed.is_empty() {
            break;
        }

        // we sent all messages (self.message is empty) but we still need to confirm all were received
        debug_assert!(message.is_empty());
        debug_assert!(!sent_not_confirmed.is_empty());
    }

    Ok(())
}

async fn send_packet(
    connection: &impl Socket,
    remote_addr: SocketAddr,
    packet: Arc<[u8]>,
) -> Result<(), SenderStreamError> {
    connection
        .send_to(&packet, remote_addr)
        .await
        .map_err(|_| SenderStreamError::Closed)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum SenderStreamError {
    #[error("stream closed unexpectedly")]
    Closed,
    #[error("message too big, size: {size}, max size: {max_size}")]
    MessageExceedsLength { size: usize, max_size: usize },
    #[error(transparent)]
    SerializationError(#[from] bincode::Error),
}

#[cfg(test)]
mod tests {
    // use super::*;
    // use crate::transport::MessagePayload;
    // use crate::util::time_source::MockTimeSource;

    // fn mock_outbound_remote_connection() -> OutboundRemoteConnection<impl Socket> {
    //     let time_source = MockTimeSource::new(Instant::now());
    // }
}
