use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::transport::{
    bw, connection_handler::Socket, packet_data, symmetric_message::SymmetricMessage,
};
use crate::util::time_source::InstantTimeSrc;

use super::OutboundRemoteConnection;

pub(crate) type StreamBytes = Vec<u8>;

// TODO: measure the space overhead of SymmetricMessage::LongMessage since is likely less than 100
/// The max payload we can send in a single fragment, this MUST be less than packet_data::MAX_DATA_SIZE
/// since we need to account for the space overhead of SymmetricMessage::LongMessage metadata
const MAX_DATA_SIZE: usize = packet_data::MAX_DATA_SIZE - 100;

struct OutboundStream {}

// todo: unit test
/// Handles sending a long message which is not being streamed,
/// streaming messages will be tackled differently, in the interim time before
/// the necessary changes are done to the codebase we will use this function
pub(super) async fn send_long_message(
    remote_conn: &mut OutboundRemoteConnection<impl Socket>,
    mut message: StreamBytes,
    bw_tracker: &Mutex<bw::PacketBWTracker<InstantTimeSrc>>,
    bw_limit: usize,
) -> Result<(), SenderStreamError> {
    let total_length_bytes = message.len() as u32;
    let start_index = remote_conn
        .last_message_id
        .fetch_add(1, std::sync::atomic::Ordering::Release);
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
            let fragment = SymmetricMessage::fragmented_message(
                start_index,
                total_length_bytes as u64,
                sent_so_far as u32 + 1,
                rest,
                &remote_conn.outbound_symmetric_key,
                std::mem::take(&mut confirm_receipts),
            )?
            .into();
            // todo: this is blocking, but hopefully meaningless, measure and improve if necessary
            sent_not_confirmed.insert(idx);
            send_packet(remote_conn, idx, fragment).await?;
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
    remote_conn: &mut OutboundRemoteConnection<impl Socket>,
    idx: u32,
    packet: Arc<[u8]>,
) -> Result<(), SenderStreamError> {
    remote_conn
        .socket
        .send_to(&packet, remote_conn.remote_addr)
        .await
        .map_err(|_| SenderStreamError::Closed)?;
    remote_conn.sent_tracker.report_sent_packet(idx, packet);
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
