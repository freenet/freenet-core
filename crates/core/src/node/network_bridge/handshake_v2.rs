//! Placeholder implementation for the upcoming ConnectV2 handshake flow.
//!
//! The new handshake pipeline is still under active development. For now we keep a
//! compile-time stub so the surrounding modules that reference this file continue
//! to build while we flesh out the full state machine.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::mpsc;

use crate::dev_tool::{Location, PeerId, Transaction};
use crate::ring::ConnectionManager;
use crate::router::Router;
use crate::transport::{InboundConnectionHandler, OutboundConnectionHandler, PeerConnection};

/// Events that will eventually be emitted by the ConnectV2 handshake handler.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum Event {
    InboundConnection {
        transaction: Transaction,
        connection: PeerConnection,
        joiner: PeerId,
        courtesy: bool,
    },
    OutboundEstablished {
        transaction: Transaction,
        peer: PeerId,
        connection: PeerConnection,
        courtesy: bool,
    },
    OutboundFailed {
        transaction: Transaction,
        peer: PeerId,
        courtesy: bool,
    },
}

/// Commands delivered from the event loop into the handshake handler.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum Command {
    Connect {
        peer: PeerId,
        transaction: Transaction,
        courtesy: bool,
    },
    DropConnection {
        peer: PeerId,
    },
}

#[allow(dead_code)]
pub(crate) struct CommandSender(mpsc::Sender<Command>);

impl CommandSender {
    #[allow(dead_code)]
    pub async fn send(&self, cmd: Command) -> Result<(), mpsc::error::SendError<Command>> {
        self.0.send(cmd).await
    }
}

/// Temporary stub implementation that just keeps channels alive.
#[allow(dead_code)]
pub(crate) struct HandshakeHandler {
    #[allow(dead_code)]
    inbound: InboundConnectionHandler,
    #[allow(dead_code)]
    outbound: OutboundConnectionHandler,
    #[allow(dead_code)]
    connection_manager: ConnectionManager,
    #[allow(dead_code)]
    router: Arc<Router>,
    #[allow(dead_code)]
    this_location: Option<Location>,
    #[allow(dead_code)]
    is_gateway: bool,
    #[allow(dead_code)]
    peer_ready: Option<Arc<std::sync::atomic::AtomicBool>>,
    commands_rx: mpsc::Receiver<Command>,
}

#[allow(clippy::too_many_arguments)]
#[allow(dead_code)]
impl HandshakeHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        inbound: InboundConnectionHandler,
        outbound: OutboundConnectionHandler,
        connection_manager: ConnectionManager,
        router: Arc<Router>,
        this_location: Option<Location>,
        is_gateway: bool,
        peer_ready: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> (Self, CommandSender) {
        let (tx, rx) = mpsc::channel(1);
        (
            HandshakeHandler {
                inbound,
                outbound,
                connection_manager,
                router,
                this_location,
                is_gateway,
                peer_ready,
                commands_rx: rx,
            },
            CommandSender(tx),
        )
    }
}

impl Stream for HandshakeHandler {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.commands_rx).poll_recv(cx) {
            Poll::Ready(Some(_cmd)) => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
