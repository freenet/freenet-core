use freenet_stdlib::client_api::{ClientError, HostResponse};
use futures::future::BoxFuture;

use super::types::{ClientId, HostIncomingMsg};

pub(crate) type BoxedClient = Box<dyn ClientEventsProxy + Send + 'static>;

pub trait ClientEventsProxy {
    /// # Cancellation Safety
    /// This future must be safe to cancel.
    fn recv(&mut self) -> BoxFuture<'_, HostIncomingMsg>;

    /// Sends a response from the host to the client application.
    fn send(
        &mut self,
        id: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> BoxFuture<'_, Result<(), ClientError>>;

    /// Wire this proxy's HTTP layer to the live node, so HTTP-only operations
    /// that must reach the executor (the hosted-mode export endpoint, P3-live of
    /// #4381) can route through the node's `OpManager`. Called ONCE at node
    /// startup from `p2p_impl`, where the live `op_manager` first meets the
    /// client proxies (before the combinator consumes them).
    ///
    /// The argument is `&dyn Any` carrying an `Arc<OpManager>` rather than a
    /// concrete `&Arc<OpManager>` ON PURPOSE: `ClientEventsProxy` is a public
    /// trait but `OpManager` is `pub(crate)`, so naming it in the signature
    /// would leak a more-private type. Implementors that care
    /// (`HttpClientApi`) downcast it; the default no-op ignores it, so external
    /// implementors (e.g. fdev's `StdInput`) need not know about `OpManager` at
    /// all. Stored as a `Weak` by the recipient so it does not extend the
    /// node's lifetime, making this per-node (no process-global singleton that
    /// concurrent in-process nodes would clobber).
    fn set_op_manager(&self, _op_manager: &dyn std::any::Any) {}
}
