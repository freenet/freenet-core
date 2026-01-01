//! Types and definitions to handle all inter-peer communication.
//!
//! This module provides the `NetworkBridge` trait, an abstraction layer for network interactions.
//! Implementations manage peer connections, message serialization, and routing.
//! It receives outgoing messages from the `Node` and `OpManager` event loops and forwards
//! incoming network messages (`NetMessage`) to the `Node`'s event loop via channels.
//!
//! See [`../../architecture.md`](../../architecture.md) for its interactions with event loops and other components.

use std::future::Future;
use std::net::SocketAddr;

use either::Either;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::message::{NetMessage, NodeEvent};

mod handshake;
pub(crate) mod in_memory;
pub(crate) mod p2p_protoc;
pub(crate) mod priority_select;

// Re-export fault injection types and functions for testing
pub use in_memory::{get_fault_injector, set_fault_injector, FaultInjectorState, NetworkStats};

pub(crate) type ConnResult<T> = std::result::Result<T, ConnectionError>;

/// Allows handling of connections to the network as well as sending messages
/// to other peers in the network with whom connection has been established.
///
/// Connections are keyed by socket address since that's what identifies
/// a network connection. The cryptographic identity is handled separately
/// at the transport layer.
pub(crate) trait NetworkBridge: Send + Sync {
    fn drop_connection(
        &mut self,
        peer_addr: SocketAddr,
    ) -> impl Future<Output = ConnResult<()>> + Send;

    fn send(
        &self,
        target_addr: SocketAddr,
        msg: NetMessage,
    ) -> impl Future<Output = ConnResult<()>> + Send;
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub(crate) enum ConnectionError {
    #[error("location unknown for this node")]
    LocationUnknown,
    #[error("unable to send message to {0}")]
    SendNotCompleted(SocketAddr),
    #[error("Unexpected connection req")]
    UnexpectedReq,
    #[error("error while de/serializing message")]
    #[serde(skip)]
    Serialization(#[from] Option<Box<bincode::ErrorKind>>),
    #[error("{0}")]
    TransportError(String),
    #[error("failed connect")]
    FailedConnectOp,
    #[error("unwanted connection")]
    UnwantedConnection,
    #[error("connection to/from address {0} blocked by local policy")]
    AddressBlocked(std::net::SocketAddr),

    // errors produced while handling the connection:
    #[error("IO error: {0}")]
    IOError(String),
    #[error("timeout error while waiting for a message")]
    Timeout,
}

impl From<std::io::Error> for ConnectionError {
    fn from(err: std::io::Error) -> Self {
        Self::IOError(format!("{err}"))
    }
}

impl From<crate::transport::TransportError> for ConnectionError {
    fn from(err: crate::transport::TransportError) -> Self {
        Self::TransportError(err.to_string())
    }
}

impl Clone for ConnectionError {
    fn clone(&self) -> Self {
        match self {
            Self::LocationUnknown => Self::LocationUnknown,
            Self::Serialization(_) => Self::Serialization(None),
            Self::SendNotCompleted(addr) => Self::SendNotCompleted(*addr),
            Self::IOError(err) => Self::IOError(err.clone()),
            Self::Timeout => Self::Timeout,
            Self::UnexpectedReq => Self::UnexpectedReq,
            Self::TransportError(err) => Self::TransportError(err.clone()),
            Self::FailedConnectOp => Self::FailedConnectOp,
            Self::UnwantedConnection => Self::UnwantedConnection,
            Self::AddressBlocked(addr) => Self::AddressBlocked(*addr),
        }
    }
}

pub(crate) fn event_loop_notification_channel(
) -> (EventLoopNotificationsReceiver, EventLoopNotificationsSender) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static CHANNEL_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

    let _channel_id = CHANNEL_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
    let (notification_tx, notification_rx) = mpsc::channel(100);
    let (op_execution_tx, op_execution_rx) = mpsc::channel(100);

    tracing::info!(
        channel_id = _channel_id,
        "Created event loop notification channel pair"
    );

    (
        EventLoopNotificationsReceiver {
            notifications_receiver: notification_rx,
            op_execution_receiver: op_execution_rx,
        },
        EventLoopNotificationsSender {
            notifications_sender: notification_tx,
            op_execution_sender: op_execution_tx,
        },
    )
}

pub(crate) struct EventLoopNotificationsReceiver {
    pub(crate) notifications_receiver: Receiver<Either<NetMessage, NodeEvent>>,
    pub(crate) op_execution_receiver: Receiver<(Sender<NetMessage>, NetMessage)>,
}

#[allow(dead_code)] // FIXME: enable async sub-transactions
impl EventLoopNotificationsReceiver {
    pub(crate) fn notifications_receiver(&self) -> &Receiver<Either<NetMessage, NodeEvent>> {
        &self.notifications_receiver
    }

    pub(crate) fn op_execution_receiver(&self) -> &Receiver<(Sender<NetMessage>, NetMessage)> {
        &self.op_execution_receiver
    }
}

#[derive(Clone)]
pub(crate) struct EventLoopNotificationsSender {
    pub(crate) notifications_sender: Sender<Either<NetMessage, NodeEvent>>,
    pub(crate) op_execution_sender: Sender<(Sender<NetMessage>, NetMessage)>,
}

impl EventLoopNotificationsSender {
    pub(crate) fn notifications_sender(&self) -> &Sender<Either<NetMessage, NodeEvent>> {
        &self.notifications_sender
    }

    #[allow(dead_code)] // FIXME: enable async sub-transactions
    pub(crate) fn op_execution_sender(&self) -> &Sender<(Sender<NetMessage>, NetMessage)> {
        &self.op_execution_sender
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use either::Either;
    use freenet_stdlib::prelude::*;
    use tokio::time::{timeout, Duration};

    /// Test that notification channel works correctly with biased select
    /// This test simulates the event loop scenario where we use biased select
    /// to poll the notification channel along with other futures
    #[tokio::test]
    async fn test_notification_channel_with_biased_select() {
        // Create notification channel
        let (notification_channel, notification_tx) = event_loop_notification_channel();
        let mut rx = notification_channel.notifications_receiver;

        // Create a simple NodeEvent to test the channel
        let test_event = crate::message::NodeEvent::Disconnect { cause: None };

        // Spawn a task to send notification after a delay
        let sender = notification_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            tracing::info!("Sending notification");
            sender
                .notifications_sender()
                .send(Either::Right(test_event))
                .await
                .expect("Failed to send notification");
            tracing::info!("Notification sent successfully");
        });

        // Simulate event loop with biased select
        let (_dummy_tx, mut dummy_rx) = tokio::sync::mpsc::channel::<String>(10);
        let mut received = false;

        tracing::info!("Starting event loop simulation");
        for i in 0..50 {
            tracing::debug!("Loop iteration {}", i);

            let result = timeout(Duration::from_millis(100), async {
                tokio::select! {
                    biased;
                    msg = rx.recv() => {
                        tracing::info!("Received notification: {:?}", msg);
                        Some(msg)
                    }
                    _ = dummy_rx.recv() => {
                        tracing::debug!("Received dummy message");
                        None
                    }
                }
            })
            .await;

            match result {
                Ok(Some(Some(_msg))) => {
                    tracing::info!("Successfully received notification!");
                    received = true;
                    break;
                }
                Ok(Some(None)) => {
                    tracing::error!("Channel closed unexpectedly");
                    break;
                }
                Ok(None) => {
                    tracing::debug!("Dummy channel activity");
                }
                Err(_) => {
                    tracing::debug!("Timeout, continuing...");
                }
            }
        }

        assert!(received, "Notification was never received by event loop");
        tracing::info!("Test passed!");
    }

    /// Test that multiple notifications can be sent and received
    #[tokio::test]
    async fn test_multiple_notifications() {
        let (notification_channel, notification_tx) = event_loop_notification_channel();
        let mut rx = notification_channel.notifications_receiver;

        // Send 3 notifications
        for _i in 0..3 {
            let test_event = crate::message::NodeEvent::Disconnect { cause: None };

            notification_tx
                .notifications_sender()
                .send(Either::Right(test_event))
                .await
                .expect("Failed to send notification");
        }

        // Receive all 3
        let mut count = 0;
        while count < 3 {
            match timeout(Duration::from_secs(1), rx.recv()).await {
                Ok(Some(_)) => count += 1,
                Ok(None) => panic!("Channel closed unexpectedly"),
                Err(_) => panic!("Timeout waiting for notification {}", count + 1),
            }
        }

        assert_eq!(count, 3, "Should receive all 3 notifications");
    }

    /// Test channel behavior when receiver is dropped
    #[tokio::test]
    async fn test_send_fails_when_receiver_dropped() {
        let (notification_channel, notification_tx) = event_loop_notification_channel();

        // Drop the receiver
        drop(notification_channel);

        // Try to send - should fail
        let test_event = crate::message::NodeEvent::Disconnect { cause: None };

        let result = notification_tx
            .notifications_sender()
            .send(Either::Right(test_event))
            .await;

        assert!(result.is_err(), "Send should fail when receiver is dropped");
    }

    // Note: Tests that require NetMessage creation are omitted because
    // constructing valid NetMessage instances requires complex setup with
    // cryptographic keys, proper state management, and specific operation states.
    // The core channel functionality is already well-tested by the NodeEvent tests above.

    /// Test channel capacity doesn't block under normal load
    #[tokio::test]
    async fn test_channel_capacity() {
        let (notification_channel, notification_tx) = event_loop_notification_channel();
        let mut rx = notification_channel.notifications_receiver;

        // Send multiple messages without reading
        for _ in 0..50 {
            let test_event = crate::message::NodeEvent::Disconnect { cause: None };
            notification_tx
                .notifications_sender()
                .send(Either::Right(test_event))
                .await
                .expect("Should not block with capacity of 100");
        }

        // Now read them all
        let mut count = 0;
        while count < 50 {
            match timeout(Duration::from_millis(10), rx.recv()).await {
                Ok(Some(_)) => count += 1,
                _ => break,
            }
        }

        assert_eq!(count, 50, "Should receive all 50 messages");
    }

    /// Test EventLoopNotificationsSender clone works correctly
    #[tokio::test]
    async fn test_sender_clone() {
        let (notification_channel, notification_tx) = event_loop_notification_channel();
        let mut rx = notification_channel.notifications_receiver;

        // Clone the sender
        let cloned_tx = notification_tx.clone();

        // Send from original
        let test_event1 = crate::message::NodeEvent::Disconnect { cause: None };
        notification_tx
            .notifications_sender()
            .send(Either::Right(test_event1))
            .await
            .expect("Should send from original");

        // Send from clone
        let test_event2 = crate::message::NodeEvent::Disconnect {
            cause: Some("cloned".into()),
        };
        cloned_tx
            .notifications_sender()
            .send(Either::Right(test_event2))
            .await
            .expect("Should send from clone");

        // Both should be received
        let mut received = 0;
        for _ in 0..2 {
            if timeout(Duration::from_millis(100), rx.recv()).await.is_ok() {
                received += 1;
            }
        }
        assert_eq!(received, 2, "Should receive both messages");
    }
}

// ConnectionError tests
#[cfg(test)]
mod connection_error_tests {
    use super::*;

    #[test]
    fn test_connection_error_clone() {
        let errors = vec![
            ConnectionError::LocationUnknown,
            ConnectionError::SendNotCompleted("127.0.0.1:8080".parse().unwrap()),
            ConnectionError::UnexpectedReq,
            ConnectionError::Serialization(None),
            ConnectionError::TransportError("test error".to_string()),
            ConnectionError::FailedConnectOp,
            ConnectionError::UnwantedConnection,
            ConnectionError::AddressBlocked("127.0.0.1:8080".parse().unwrap()),
            ConnectionError::IOError("io error".to_string()),
            ConnectionError::Timeout,
        ];

        for error in errors {
            let cloned = error.clone();
            // Verify clone produces equivalent error
            assert_eq!(format!("{}", error), format!("{}", cloned));
        }
    }

    #[test]
    fn test_connection_error_from_io_error() {
        let io_error = std::io::Error::other("test io error");
        let conn_error: ConnectionError = io_error.into();

        match conn_error {
            ConnectionError::IOError(msg) => {
                assert!(msg.contains("test io error"));
            }
            _ => panic!("Expected IOError variant"),
        }
    }

    #[test]
    fn test_connection_error_display() {
        let error = ConnectionError::LocationUnknown;
        let display = format!("{}", error);
        assert!(!display.is_empty());
        assert!(display.contains("location unknown"));

        let error = ConnectionError::SendNotCompleted("127.0.0.1:8080".parse().unwrap());
        let display = format!("{}", error);
        assert!(display.contains("127.0.0.1:8080"));

        let error = ConnectionError::AddressBlocked("192.168.1.1:9000".parse().unwrap());
        let display = format!("{}", error);
        assert!(display.contains("192.168.1.1:9000"));

        let error = ConnectionError::Timeout;
        let display = format!("{}", error);
        assert!(display.contains("timeout"));
    }

    #[test]
    fn test_serialization_error_clone_loses_inner() {
        // When cloning a Serialization error, the inner error is lost
        let inner = Box::new(bincode::ErrorKind::SizeLimit);
        let original = ConnectionError::Serialization(Some(inner));
        let cloned = original.clone();

        match cloned {
            ConnectionError::Serialization(None) => {} // Expected - inner is lost
            _ => panic!("Expected Serialization(None) after clone"),
        }
    }

    #[test]
    fn test_connection_error_debug() {
        let error = ConnectionError::FailedConnectOp;
        let debug = format!("{:?}", error);
        assert!(!debug.is_empty());
        assert!(debug.contains("FailedConnectOp"));
    }
}
