//! A QUIC-native connection set up by the node's upgrades hub (#5940, A5b),
//! which holds what every native setup shares: the endpoint and its clock, the
//! native waiters, the node's one punch budget (both roles charge it), and the
//! cap on setups at once.
//!
//! The role is the key order, as for an upgrade: the higher key is the server
//! (`setup::accept_native`, which waits the longer window), the lower the
//! client (`setup::connect_native`). Each setup's end is counted once, by an
//! exhaustive match on its error, as `connect_native_setup_<label>`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use super::QuicUpgrades;
use crate::simulation::TimeSource;
use crate::transport::TRANSPORT_METRICS;
use crate::transport::crypto::TransportPublicKey;
use crate::transport::metrics::NativeSetupOutcome;
use crate::transport::peer_connection::native_connection::NativePeerConnection;
use crate::transport::quic::native::exchange::HelloFailure;
use crate::transport::quic::native::punch::PunchRefusal;
use crate::transport::quic::native::setup::{
    NativePeer, NativeSetupError, accept_native, connect_native,
};
use crate::transport::quic::upgrade::data_route::RouteNote;
use crate::transport::quic::upgrade::machine::Role;
use crate::transport::quic::{QuicAllowError, QuicConnectError};

/// A native connection the node asks for, from its own record of the peer.
#[derive(Debug, Clone)]
pub(crate) struct NativeDial {
    pub(crate) addr: SocketAddr,
    /// The key the peer must prove in the handshake.
    pub(crate) key: TransportPublicKey,
    /// Whether the peer is a gateway (`setup::NativePeer::is_gateway`).
    pub(crate) is_gateway: bool,
    /// How long the caller can wait; the setup's own window is shorter
    /// (`setup::NATIVE_WINDOW`), and a setup with less than
    /// `setup::MIN_WINDOW_LEFT` of it left is not started.
    pub(crate) within: Duration,
}

impl QuicUpgrades {
    /// Set up a native connection to `dial`'s peer and wrap it for the node,
    /// or say why not. Refused at once, and counted `setups_full`, when the
    /// node already runs `QuicPolicy::native_setups_max` setups.
    pub(in crate::transport) async fn setup_native(
        self: &Arc<Self>,
        dial: NativeDial,
    ) -> Result<NativePeerConnection, NativeSetupOutcome> {
        let result = self.run_native_setup(&dial).await;
        let outcome = match &result {
            Ok(_) => NativeSetupOutcome::Established,
            Err(outcome) => *outcome,
        };
        TRANSPORT_METRICS.record_connect_native_setup(outcome);
        result
    }

    async fn run_native_setup(
        self: &Arc<Self>,
        dial: &NativeDial,
    ) -> Result<NativePeerConnection, NativeSetupOutcome> {
        // Held until the setup ends; the connection is then the node's.
        let Ok(_slot) = Arc::clone(&self.native_setups).try_acquire_owned() else {
            return Err(NativeSetupOutcome::SetupsFull);
        };
        let clock = self.path_clock.clone();
        let deadline = clock.now() + dial.within;
        let peer = NativePeer {
            addr: dial.addr,
            key: &dial.key,
            is_gateway: dial.is_gateway,
        };
        // No capability bits are defined yet.
        let caps = 0;
        let setup = match Role::of(&self.local_key, &dial.key) {
            Some(Role::Server) => {
                accept_native(
                    &self.endpoint,
                    &self.native,
                    &self.punch_budget,
                    peer,
                    caps,
                    &clock,
                    deadline,
                )
                .await
            }
            Some(Role::Client) => {
                connect_native(
                    &self.endpoint,
                    &self.punch_budget,
                    peer,
                    caps,
                    &clock,
                    deadline,
                )
                .await
            }
            // The peer's key is this node's own: there is no one to connect to.
            None => return Err(NativeSetupOutcome::UnusableKey),
        };
        let native = setup.map_err(|error| {
            tracing::debug!(addr = %dial.addr, %error, "A QUIC-native setup gave no connection");
            outcome_of(&error)
        })?;
        Ok(NativePeerConnection::new(
            native.connection,
            native.peer.version,
            clock,
            Some(self.endpoint.buffer_budget()),
            self.native_rate.clone(),
            self.route_note(dial.addr),
        ))
    }

    fn route_note(&self, remote: SocketAddr) -> RouteNote {
        #[cfg(any(test, feature = "testing"))]
        return RouteNote::new(self.trace.clone(), remote, 0);
        #[cfg(not(any(test, feature = "testing")))]
        {
            let _ = remote;
            RouteNote::default()
        }
    }
}

/// The label for a setup that gave no connection. Exhaustive at every level,
/// so a new error does not compile until it has one.
pub(super) fn outcome_of(error: &NativeSetupError) -> NativeSetupOutcome {
    use NativeSetupOutcome as O;
    match error {
        NativeSetupError::TooLate => O::TooLate,
        NativeSetupError::OverBudget(refusal) => match refusal {
            PunchRefusal::Destination => O::OverBudgetDestination,
            PunchRefusal::Node => O::OverBudgetNode,
            PunchRefusal::Table => O::OverBudgetTable,
        },
        NativeSetupError::AlreadyWaiting => O::AlreadyWaiting,
        NativeSetupError::Allow(allow) => match allow {
            QuicAllowError::AllowSetFull => O::AllowSetFull,
            QuicAllowError::KeyConflict => O::KeyConflict,
            QuicAllowError::UnusableKey(_) => O::UnusableKey,
        },
        NativeSetupError::Connect(connect) => match connect {
            QuicConnectError::AllowSetFull => O::AllowSetFull,
            QuicConnectError::UnusableKey(_) => O::UnusableKey,
            QuicConnectError::Tls(_)
            | QuicConnectError::Connect(_)
            | QuicConnectError::Connection(_) => O::HandshakeFailed,
        },
        NativeSetupError::TimedOut => O::TimedOut,
        NativeSetupError::Hello(hello) => match hello {
            HelloFailure::Refused(_) | HelloFailure::PeerRefusedHello => O::HelloRefused,
            HelloFailure::Incompatible(_) | HelloFailure::RejectedVersion => O::HelloVersion,
            HelloFailure::TimedOut | HelloFailure::ConnectionLost(_) => O::HelloLost,
        },
    }
}
