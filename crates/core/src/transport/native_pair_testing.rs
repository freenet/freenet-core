//! A real QUIC-native connection and its peer, for tests outside the
//! transport module that need to see what the peer sees when the node ends
//! the connection (see `zombie_sweep` tests in the node's connection
//! lifecycle).

use std::future::Future;
use std::time::Duration;

use crate::dev_tool::TimeSource;
use crate::transport::boundary::PeerConnectionApi;
use crate::transport::peer_connection::native_connection::NativePeerConnection;
use crate::transport::quic::config::QuicRateLimit;
use crate::transport::quic::upgrade::data_route::RouteNote;
use crate::transport::quic::upgrade::driver::PathClock;
use crate::transport::quic::upgrade::driver::tests::{NativeServer, native_server, run_bounded};

/// The peer's end of a native connection.
pub(crate) struct NativePeerEnd {
    server: NativeServer,
}

impl NativePeerEnd {
    /// Lets `by` of the transport's virtual time pass.
    pub(crate) async fn settle(&self, by: Duration) {
        self.server.time.sleep(by).await;
    }

    /// The application close code the node ended the connection with, if it
    /// has sent one the peer has received.
    pub(crate) fn close_code_from_node(&self) -> Option<u64> {
        match self.server.peer.connection().inner().close_reason() {
            Some(quinn::ConnectionError::ApplicationClosed(close)) => {
                Some(close.error_code.into_inner())
            }
            _ => None,
        }
    }

    /// Whether the connection is still open from the peer's side.
    pub(crate) fn is_open(&self) -> bool {
        self.server
            .peer
            .connection()
            .inner()
            .close_reason()
            .is_none()
    }
}

/// Run `test` on a native connection (the node's end, handed over as the
/// node's transport object) and the peer's end, on a virtual clock.
pub(crate) fn run_native_pair<F, Fut>(network: &'static str, test: F)
where
    F: FnOnce(Box<dyn PeerConnectionApi>, NativePeerEnd) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    run_bounded(move || async move {
        let (peer, server) = native_server(
            network,
            crate::transport::quic::path::QuicPathLimits::DEFAULT,
            None,
        )
        .await;
        let time = peer.time.clone();
        let conn = NativePeerConnection::new(
            server,
            (0, 2, 160),
            PathClock::Virtual(time.clone()),
            None,
            QuicRateLimit::new(None, None),
            RouteNote::default(),
        );
        (time, test(Box::new(conn), NativePeerEnd { server: peer }))
    });
}
