use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;

use crate::{
    transport::{
        bw, connection_handler::Socket, packet_data, sent_packet_tracker::ResendAction,
        symmetric_message::SymmetricMessage,
    },
    util::{CachingSystemTimeSrc, TimeSource},
};

use super::OutboundRemoteConnection;

pub(crate) type StreamBytes = Vec<u8>;

// TODO: measure the space overhead of SymmetricMessage::LongMessage since is likely less than 100
/// The max payload we can send in a single fragment, this MUST be less than packet_data::MAX_DATA_SIZE
/// since we need to account for the space overhead of SymmetricMessage::LongMessage metadata
const MAX_DATA_SIZE: usize = packet_data::MAX_DATA_SIZE - 100;

// todo: unit test
/// Handles sending a long message which is not being streamed,
/// streaming messages will be tackled differently, in the interim time before
/// the necessary changes are done to the codebase we will use this function
pub(super) async fn send_long_message(
    remote_conn: &mut OutboundRemoteConnection<impl Socket>,
    mut message: StreamBytes,
    bw_tracker: &Mutex<bw::PacketBWTracker<CachingSystemTimeSrc>>,
    bw_limit: usize,
) -> Result<(), SenderStreamError> {
    let total_length_bytes = message.len() as u32;
    let start_index = remote_conn.last_message_id + 1;
    let mut total_messages = message.len() / MAX_DATA_SIZE;
    total_messages += if message.len() % MAX_DATA_SIZE == 0 {
        0
    } else {
        1
    };
    let mut wait_for_sending_until = Instant::now();
    let mut sent_not_confirmed = HashSet::new();
    let mut sent_confirmed = 0;
    let mut pending_outbound_packet: Option<(u32, Arc<[u8]>)> = None;
    let mut next_sent_check = Instant::now();
    let mut confirm_receipts = Vec::new();

    loop {
        match remote_conn.receipts_notifier.try_recv() {
            Ok(mut receipts) => {
                remote_conn.sent_tracker.report_received_receipts(&receipts);
                for receipt in receipts.iter() {
                    if sent_not_confirmed.remove(receipt) {
                        sent_confirmed += 1;
                    }
                }
                std::mem::swap(&mut confirm_receipts, &mut receipts);
            }
            Err(_) => return Err(SenderStreamError::Closed),
        }
        if Instant::now() < wait_for_sending_until {
            tokio::time::sleep_until(wait_for_sending_until.into()).await;
        }
        if let Some((idx, pending_packet)) = pending_outbound_packet.take() {
            send_packet(remote_conn, idx, pending_packet).await?;
        }

        // if necessary, check if we need to resend any packet
        if next_sent_check <= bw_tracker.lock().await.time_source.now() {
            match remote_conn.sent_tracker.get_resend() {
                ResendAction::WaitUntil(wait) => {
                    next_sent_check = wait;
                }
                ResendAction::Resend(idx, packet) => {
                    let mut bw_tracker = bw_tracker.lock().await;
                    if let Some(send_wait) = bw_tracker.can_send_packet(bw_limit, packet.len()) {
                        wait_for_sending_until = bw_tracker.time_source.now() + send_wait;
                        pending_outbound_packet = Some((idx, packet));
                    } else {
                        send_packet(remote_conn, idx, packet).await?;
                        continue;
                    }
                }
            }
        }

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
                idx,
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
