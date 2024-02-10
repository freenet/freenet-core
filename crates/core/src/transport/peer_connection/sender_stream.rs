use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use futures::{pin_mut, FutureExt};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex};

use crate::transport::bw;
use crate::transport::connection_handler::{RemoteConnection, Socket};
use crate::transport::packet_data::{PacketData, MAX_DATA_SIZE, MAX_PACKET_SIZE};
use crate::transport::sent_packet_tracker::ResendAction;
use crate::util::{CachingSystemTimeSrc, TimeSource};

pub(crate) type StreamBytes = Vec<u8>;

// todo: unit test
/// Handles breaking a message into parts, encryption, etc.
pub(crate) struct SenderStream<'a, S = UdpSocket, T: TimeSource = CachingSystemTimeSrc> {
    socket: &'a S,
    remote_conn: &'a mut RemoteConnection,
    message: StreamBytes,
    start_index: u32,
    total_messages: usize,
    sent_confirmed: usize,
    sent_not_confirmed: HashSet<u32>,
    receipts_notification: mpsc::Receiver<Vec<u32>>,
    next_sent_check: Instant,
    bw_tracker: &'a Mutex<bw::PacketBWTracker<T>>,
    bw_limit: usize,
    wait_for_sending_until: Instant,
    pending_outbound_packet: Option<(u32, Arc<[u8]>)>,
}

impl<'a, S: Socket, T: TimeSource> SenderStream<'a, S, T> {
    pub fn new(
        socket: &'a S,
        remote_conn: &'a mut RemoteConnection,
        whole_message: StreamBytes,
        receipts_notification: mpsc::Receiver<Vec<u32>>,
        bw_tracker: &'a Mutex<bw::PacketBWTracker<T>>,
        bw_limit: usize,
    ) -> Self {
        let start_index = remote_conn.last_message_id + 1;
        let mut total_messages = whole_message.len() / MAX_DATA_SIZE;
        total_messages += if whole_message.len() % MAX_DATA_SIZE == 0 {
            0
        } else {
            1
        };
        Self {
            socket,
            remote_conn,
            message: whole_message,
            start_index,
            total_messages,
            sent_confirmed: 0,
            sent_not_confirmed: HashSet::new(),
            receipts_notification,
            next_sent_check: Instant::now(),
            bw_tracker,
            bw_limit,
            wait_for_sending_until: Instant::now(),
            pending_outbound_packet: None,
        }
    }

    fn sent_packets(&self) -> usize {
        self.sent_confirmed + self.sent_not_confirmed.len()
    }

    async fn send_packet(&mut self, idx: u32, packet: Arc<[u8]>) -> Result<(), SenderStreamError> {
        self.socket
            .send_to(&packet, self.remote_conn.remote_addr)
            .await
            .map_err(|_| SenderStreamError::Closed)?;
        // self.start_index + self.sent_packets() as u32
        self.remote_conn
            .sent_tracker
            .report_sent_packet(idx, packet);
        Ok(())
    }

    async fn maybe_resend(&mut self) -> Result<(), SenderStreamError> {
        match self.remote_conn.sent_tracker.get_resend() {
            ResendAction::WaitUntil(wait) => {
                self.next_sent_check = wait;
            }
            ResendAction::Resend(idx, packet) => {
                let mut bw_tracker = self.bw_tracker.lock().await;
                if let Some(send_wait) = bw_tracker.can_send_packet(self.bw_limit, packet.len()) {
                    self.wait_for_sending_until = bw_tracker.time_source.now() + send_wait;
                    self.pending_outbound_packet = Some((idx, packet));
                } else {
                    self.send_packet(idx, packet).await?;
                }
            }
        }
        Ok(())
    }
}

// this really doesn't need to be a sink, is enough with a future and we can merge poll_ready and poll_flush logic
// into poll really
impl<S: Socket, T: TimeSource> Future for SenderStream<'_, S, T> {
    type Output = Result<(), SenderStreamError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.wait_for_sending_until > Instant::now() {
            return Poll::Pending;
        }
        match self.receipts_notification.try_recv() {
            Ok(receipts) => {
                self.remote_conn
                    .sent_tracker
                    .report_received_receipts(&receipts);
                for receipt in receipts.iter() {
                    if self.sent_not_confirmed.remove(receipt) {
                        self.sent_confirmed += 1;
                    }
                }
            }
            Err(_) => return Poll::Ready(Err(SenderStreamError::Closed)),
        }

        if let Some((idx, pending_packet)) = self.pending_outbound_packet.take() {
            let f = self.send_packet(idx, pending_packet);
            pin_mut!(f);
            loop {
                match f.poll_unpin(cx) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(_)) => break,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                }
            }
        }

        if self.next_sent_check <= Instant::now()
        /* todo: call bw_Tracker.time_source instead */
        {
            let f = self.maybe_resend();
            pin_mut!(f);
            loop {
                match f.poll_unpin(cx) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(_)) => break,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                }
            }
        }

        let sent_so_far = self.sent_packets();
        if sent_so_far < self.total_messages {
            let mut rest = {
                if self.message.len() > MAX_DATA_SIZE {
                    self.message.split_off(MAX_DATA_SIZE)
                } else {
                    std::mem::take(&mut self.message)
                }
            };
            std::mem::swap(&mut self.message, &mut rest);
            // todo: this is blocking, but hopefully meaningless, measure and improve if necessary
            let packet: Arc<[u8]> = PacketData::<MAX_PACKET_SIZE>::encrypted_with_cipher(
                &rest[..],
                &self.remote_conn.outbound_symmetric_key,
            )
            .into();
            let idx = self.start_index + sent_so_far as u32 + 1;
            self.sent_not_confirmed.insert(idx);
            let f = self.send_packet(idx, packet);
            pin_mut!(f);
            loop {
                match f.poll_unpin(cx) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(_)) => break,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                }
            }
            Poll::Pending
        } else if self.message.is_empty() && self.sent_not_confirmed.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            // we sent all messages (self.message is empty) but we still need to confirm all were received
            debug_assert!(self.message.is_empty());
            debug_assert!(!self.sent_not_confirmed.is_empty());
            Poll::Pending
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SenderStreamError {
    #[error("stream closed unexpectedly")]
    Closed,
    #[error("message too big, size: {size}, max size: {max_size}")]
    MessageExceedsLength { size: usize, max_size: usize },
}
