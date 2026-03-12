use std::collections::HashMap;

use freenet_stdlib::client_api::{ErrorKind, HostResponse};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use super::{BoxedClient, ClientError, ClientId, HostResult, OpenRequest};
use crate::config::GlobalExecutor;

/// Channel capacity for communication between the combinator and client tasks.
///
/// Bounded channels prevent unbounded memory growth (OOM) when subscription notifications
/// pile up faster than clients consume them (see #2928). The host side uses `try_send()`
/// so it never blocks — if a client's buffer is full, the send fails and the client is
/// treated as disconnected. The client→host direction uses `send().await` which is safe
/// because the host never blocks on sends, so it always makes progress draining requests.
///
/// This avoids both the original `channel(1)` deadlock from #2915 and unbounded growth.
const CHANNEL_CAPACITY: usize = 2048;

/// Maximum consecutive transient errors before `client_fn` gives up on a proxy.
/// Prevents CPU spin if a proxy's `recv()` returns errors without blocking.
/// The counter resets on every successful request, so intermittent errors are fine.
const MAX_CONSECUTIVE_TRANSIENT_ERRORS: usize = 50;

type HostIncomingMsg = Result<OpenRequest<'static>, ClientError>;

type ClientEventsFut =
    BoxFuture<'static, (usize, Receiver<HostIncomingMsg>, Option<HostIncomingMsg>)>;

/// This type allows combining different sources of events into one and interoperation between them.
pub struct ClientEventsCombinator<const N: usize> {
    pending_futs: FuturesUnordered<ClientEventsFut>,
    /// receiving end of the different client applications from the node
    clients: [Sender<(ClientId, HostResult)>; N],
    /// a map of the individual protocols, external, sending client events ids to an internal list of ids
    external_clients: [HashMap<ClientId, ClientId>; N],
    /// a map of the external id to which protocol it belongs (represented by the index in the array)
    /// and the original id (reverse of indexes)
    internal_clients: HashMap<ClientId, (usize, ClientId)>,
    /// tracks which client slots have disconnected so we don't re-poll dead channels
    dead_clients: [bool; N],
    /// human-readable names for each slot (e.g., "http", "websocket") for diagnostics
    slot_names: [&'static str; N],
}

impl<const N: usize> ClientEventsCombinator<N> {
    pub fn new(clients: [BoxedClient; N]) -> Self {
        let pending_futs = FuturesUnordered::new();
        let channels = clients.map(|client| {
            let (tx, rx) = channel(CHANNEL_CAPACITY);
            let (tx_host, rx_host) = channel(CHANNEL_CAPACITY);
            GlobalExecutor::spawn(client_fn(client, rx, tx_host));
            (tx, rx_host)
        });
        let mut clients = [(); N].map(|_| None);
        let mut hosts_rx = [(); N].map(|_| None);
        for (i, (tx, rx_host)) in channels.into_iter().enumerate() {
            clients[i] = Some(tx);
            hosts_rx[i] = Some(rx_host);
        }
        let external_clients = [(); N].map(|_| HashMap::new());

        for (i, rx) in hosts_rx.iter_mut().enumerate() {
            let Some(mut rx) = rx.take() else {
                continue;
            };
            pending_futs.push(
                async move {
                    let res = rx.recv().await;
                    (i, rx, res)
                }
                .boxed(),
            );
        }

        Self {
            clients: clients.map(|c| c.unwrap()),
            external_clients,
            internal_clients: HashMap::new(),
            pending_futs,
            dead_clients: [false; N],
            slot_names: ["unknown"; N],
        }
    }

    /// Set human-readable names for each slot for diagnostic logging.
    ///
    /// Panics if `names.len() != N` (programmer error).
    pub fn with_slot_names(mut self, names: &[&'static str]) -> Self {
        assert_eq!(names.len(), N, "slot names length must match client count");
        for (i, name) in names.iter().enumerate() {
            self.slot_names[i] = name;
        }
        self
    }
}

impl<const N: usize> super::ClientEventsProxy for ClientEventsCombinator<N> {
    fn recv(&mut self) -> BoxFuture<'_, Result<OpenRequest<'static>, ClientError>> {
        async {
            loop {
                if self.pending_futs.is_empty() {
                    // All client slots are dead — nothing left to poll.
                    // Return a shutdown error so client_event_handling can exit gracefully.
                    tracing::warn!("All client transports have disconnected");
                    return Err(ErrorKind::Shutdown.into());
                }

                let Some((idx, mut rx, res)) = self.pending_futs.next().await else {
                    // FuturesUnordered drained mid-poll (shouldn't happen if !is_empty above)
                    tracing::warn!("All client transports have disconnected");
                    return Err(ErrorKind::Shutdown.into());
                };

                match res {
                    Some(msg) => {
                        // Channel is alive — re-enqueue for next poll
                        self.pending_futs.push(
                            async move {
                                let res = rx.recv().await;
                                (idx, rx, res)
                            }
                            .boxed(),
                        );

                        // Map external client IDs to internal ones
                        let mapped = match msg {
                            Ok(OpenRequest {
                                client_id: external,
                                request_id,
                                request,
                                notification_channel,
                                token,
                                origin_contract,
                            }) => {
                                let id = *self.external_clients[idx]
                                    .entry(external)
                                    .or_insert_with(|| {
                                        let internal = ClientId::next();
                                        self.internal_clients.insert(internal, (idx, external));
                                        internal
                                    });
                                tracing::debug!("received request for proxy #{idx}; internal_id={id}; external_id={external}; req={request}");
                                Ok(OpenRequest {
                                    client_id: id,
                                    request_id,
                                    request,
                                    notification_channel,
                                    token,
                                    origin_contract,
                                })
                            }
                            err @ Err(_) => err,
                        };
                        return mapped;
                    }
                    None => {
                        // Channel closed — client_fn exited. Mark slot as dead and clean up
                        // mappings. Do NOT re-push into pending_futs (avoids infinite spin).
                        let slot_name = self.slot_names[idx];
                        self.dead_clients[idx] = true;

                        // Remove internal→external mappings for this slot
                        let dead_internals: Vec<ClientId> = self
                            .internal_clients
                            .iter()
                            .filter(|(_, (slot, _))| *slot == idx)
                            .map(|(id, _)| *id)
                            .collect();
                        for id in &dead_internals {
                            self.internal_clients.remove(id);
                        }
                        self.external_clients[idx].clear();

                        // Log at error level so operators notice degraded state.
                        // If this is the WebSocket slot, the node can't accept new
                        // client connections — a restart is recommended (#3479).
                        tracing::error!(
                            slot_name,
                            proxy_idx = idx,
                            "Client API '{slot_name}' died — node is in degraded state, \
                             restart recommended"
                        );

                        // Loop back to poll remaining live clients
                        continue;
                    }
                }
            }
        }
        .boxed()
    }

    fn send<'a>(
        &mut self,
        internal: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> BoxFuture<'_, Result<(), ClientError>> {
        async move {
            let Some((idx, external)) = self.internal_clients.get(&internal) else {
                // Client disconnected between request and response — this is normal,
                // not fatal. Log and silently drop the response.
                tracing::debug!(
                    client_id = internal.0,
                    "Dropping response for disconnected client (unknown client ID)"
                );
                return Ok(());
            };
            let idx = *idx;
            let external = *external;

            if self.dead_clients[idx] {
                // Client slot is already marked dead — drop the response silently.
                tracing::debug!(
                    client_id = internal.0,
                    proxy_idx = idx,
                    "Dropping response for dead client slot"
                );
                return Ok(());
            }

            // Use try_send so the host event loop never blocks on sending to a slow client.
            // This prevents the channel(1) deadlock from #2915 while keeping memory bounded.
            if let Err(e) = self.clients[idx].try_send((external, response)) {
                if matches!(e, tokio::sync::mpsc::error::TrySendError::Full(_)) {
                    tracing::warn!(
                        client_id = internal.0,
                        capacity = CHANNEL_CAPACITY,
                        "Response channel full — client not consuming fast enough, dropping response"
                    );
                } else {
                    tracing::debug!(
                        client_id = internal.0,
                        "Client channel closed, dropping response"
                    );
                }
                // Don't propagate as error — a slow/dead client should not crash the node.
            }
            Ok(())
        }
        .boxed()
    }
}

/// Handles bidirectional communication between a client and the host.
///
/// Uses `tokio::select! { biased; ... }` to ensure host responses are always
/// processed before client disconnect errors, preventing lost responses.
/// The `biased` modifier ensures futures are polled in declaration order,
/// and if multiple are ready, the first one wins.
///
/// The client→host direction uses `send().await` on a bounded channel, which provides
/// backpressure. This is safe because the host side uses `try_send()` (never blocks),
/// so it always makes progress draining this channel. No circular wait = no deadlock.
async fn client_fn(
    mut client: BoxedClient,
    mut rx: Receiver<(ClientId, HostResult)>,
    tx_host: Sender<Result<OpenRequest<'static>, ClientError>>,
) {
    let mut consecutive_errors: usize = 0;
    loop {
        tokio::select! {
            biased;

            // Priority 1: Host responses (highest priority)
            // Ensures responses are sent to client before handling disconnect
            msg = rx.recv() => {
                match msg {
                    Some((client_id, response)) => {
                        if client.send(client_id, response).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        tracing::debug!("disconnected host");
                        break;
                    }
                }
            }

            // Priority 2: Client requests
            result = client.recv() => {
                match result {
                    Ok(OpenRequest {
                        client_id,
                        request_id,
                        request,
                        notification_channel,
                        token,
                        origin_contract,
                    }) => {
                        tracing::debug!(
                            "received msg @ combinator from external id {client_id}, msg: {request}"
                        );
                        if tx_host
                            .send(Ok(OpenRequest {
                                client_id,
                                request_id,
                                request,
                                notification_channel,
                                token,
                                origin_contract,
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        consecutive_errors = 0;
                    }
                    Err(err) if matches!(
                        err.kind(),
                        ErrorKind::ChannelClosed
                            | ErrorKind::Shutdown
                            | ErrorKind::TransportProtocolDisconnect
                    ) => {
                        // Fatal: client disconnected, node shutting down, or transport dead.
                        tracing::warn!("Fatal client error, shutting down client slot: {err}");
                        if let Err(e) = tx_host.send(Err(err)).await {
                            tracing::debug!(error = %e, "failed to notify host of fatal error");
                        }
                        break;
                    }
                    Err(err) => {
                        // Transient per-connection errors (NodeUnavailable, UnknownClient,
                        // DeserializationError, etc.) should NOT kill the entire client slot.
                        // Forward to host and continue accepting connections.
                        //
                        // Note: ErrorKind is #[non_exhaustive], so new variants added to
                        // freenet-stdlib will land here. This is intentional — new errors
                        // should be treated as transient by default and only promoted to
                        // fatal after explicit review.
                        consecutive_errors += 1;
                        if consecutive_errors >= MAX_CONSECUTIVE_TRANSIENT_ERRORS {
                            tracing::error!(
                                consecutive_errors,
                                "Too many consecutive transient errors, shutting down client slot: {err}"
                            );
                            if let Err(e) = tx_host.send(Err(err)).await {
                                tracing::debug!(error = %e, "failed to notify host of error limit");
                            }
                            break;
                        }
                        tracing::warn!("Transient client error (continuing): {err}");
                        if tx_host.send(Err(err)).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
    }
    tracing::debug!("Peer client interface shut down");
}

#[cfg(test)]
mod test {
    use std::collections::VecDeque;

    use freenet_stdlib::client_api::ClientRequest;
    use futures::try_join;
    use tokio::sync::mpsc::{channel, Receiver, Sender};

    use super::*;
    use crate::client_events::ClientEventsProxy;

    struct SampleProxy {
        id: usize,
        rx: Receiver<usize>,
        tx: Sender<usize>,
    }

    impl SampleProxy {
        fn new(id: usize, rx: Receiver<usize>, tx: Sender<usize>) -> Self {
            Self { id, rx, tx }
        }
    }

    impl ClientEventsProxy for SampleProxy {
        fn recv(&mut self) -> BoxFuture<'_, crate::client_events::HostIncomingMsg> {
            Box::pin(async {
                let id = self
                    .rx
                    .recv()
                    .await
                    .ok_or_else::<ClientError, _>(|| ErrorKind::ChannelClosed.into())?;
                assert_eq!(id, self.id);
                Ok(OpenRequest::new(
                    ClientId::next(),
                    Box::new(ClientRequest::Disconnect { cause: None }),
                ))
            })
        }

        fn send(
            &mut self,
            _id: ClientId,
            _response: Result<HostResponse, ClientError>,
        ) -> BoxFuture<'_, Result<(), ClientError>> {
            async {
                self.tx
                    .send(self.id)
                    .await
                    .map_err(|_| ErrorKind::ChannelClosed.into())
            }
            .boxed()
        }
    }

    fn setup_proxies() -> ([BoxedClient; 3], Vec<Sender<usize>>, Vec<Receiver<usize>>) {
        let mut cnt = 0;
        let mut senders = vec![];
        let mut receivers = vec![];
        let clients = [None::<()>; 3].map(|_| {
            let (tx1, rx1) = channel(1);
            let (tx2, rx2) = channel(1);
            let r = Box::new(SampleProxy::new(cnt, rx1, tx2)) as _;
            senders.push(tx1);
            receivers.push(rx2);
            cnt += 1;
            r
        });
        (clients, senders, receivers)
    }

    #[tokio::test]
    async fn test_recv() {
        let (proxies, mut senders, _) = setup_proxies();
        let mut combinator = ClientEventsCombinator::new(proxies);

        let sending = async {
            for _ in 0..3 {
                for (id, tx) in senders.iter_mut().enumerate() {
                    tx.send(id).await?;
                }
            }
            Ok::<_, Box<dyn std::error::Error>>(senders)
        };

        let receiving = async {
            // Receive 3 rounds of 3 requests each (one per proxy).
            for _ in 0..9 {
                combinator.recv().await?;
            }
            // SampleProxy::recv() creates a new external ClientId each call,
            // so 9 requests produce 9 distinct internal client mappings.
            assert_eq!(combinator.internal_clients.len(), 9);
            Ok::<_, Box<dyn std::error::Error>>(())
        };

        try_join!(sending, receiving).unwrap();
    }

    #[tokio::test]
    async fn test_send() {
        let (proxies, mut senders, mut receivers) = setup_proxies();
        let mut combinator = ClientEventsCombinator::new(proxies);

        // Create the internal client mapping implicitly.
        for (idx, sender) in senders.iter_mut().enumerate() {
            sender.send(idx).await.unwrap();
            combinator.recv().await.unwrap();
        }

        let receiving = async {
            // Test sending a response through the combinator for each proxy.
            for (idx, receiver) in receivers.iter_mut().enumerate() {
                // Assert that the receiver received the expected message.
                let received_id = receiver
                    .recv()
                    .await
                    .ok_or(format!("missing {idx} sender"))?;
                assert_eq!(received_id, idx);
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        };

        let sending = async {
            for (i, cli_id) in combinator
                .internal_clients
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .enumerate()
            {
                // Send a sample response through the combinator.
                combinator
                    .send(cli_id, Ok(HostResponse::Ok))
                    .await
                    .map_err(|err| format!("Send failed for client {i}: {err}",))?;
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        };

        try_join!(sending, receiving).unwrap();
    }

    /// Regression test: concurrent bidirectional traffic must not deadlock.
    ///
    /// With `channel(1)` this would deadlock when `client_fn` blocks on
    /// sending a request while the combinator blocks on sending a response.
    #[tokio::test]
    async fn test_bidirectional_no_deadlock() {
        let (proxies, mut senders, mut receivers) = setup_proxies();
        let mut combinator = ClientEventsCombinator::new(proxies);

        // Populate internal_clients mapping.
        for (idx, sender) in senders.iter_mut().enumerate() {
            sender.send(idx).await.unwrap();
            combinator.recv().await.unwrap();
        }
        let client_ids: Vec<_> = combinator.internal_clients.keys().cloned().collect();

        let rounds = 20;

        // Client side: send requests and drain responses concurrently.
        let client_side = async {
            let send_requests = async {
                for _ in 0..rounds {
                    for (id, tx) in senders.iter_mut().enumerate() {
                        tx.send(id).await.unwrap();
                    }
                }
            };
            let drain_responses = async {
                for _ in 0..rounds {
                    for receiver in receivers.iter_mut() {
                        receiver.recv().await.unwrap();
                    }
                }
            };
            futures::future::join(send_requests, drain_responses).await;
        };

        // Host side: interleave send and recv on the combinator since both
        // require &mut self (can't split into concurrent futures).
        let host_side = async {
            for _ in 0..rounds {
                // Send a response to each client.
                for cli_id in &client_ids {
                    combinator
                        .send(*cli_id, Ok(HostResponse::Ok))
                        .await
                        .unwrap();
                }
                // Drain one request per client.
                for _ in 0..3 {
                    combinator.recv().await.unwrap();
                }
            }
        };

        // With channel(1) this would deadlock; with bounded(2048) + try_send it completes.
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            futures::future::join(client_side, host_side),
        )
        .await
        .expect("bidirectional traffic deadlocked");
    }

    /// Regression test for #3243: a single client disconnecting should not crash the combinator.
    /// Before the fix, a closed channel would produce TransportProtocolDisconnect which was
    /// treated as fatal. Now the dead slot is cleaned up and remaining clients continue working.
    #[tokio::test]
    async fn test_client_disconnect_non_fatal() {
        let (proxies, mut senders, _receivers) = setup_proxies();
        let mut combinator = ClientEventsCombinator::new(proxies);

        // Populate internal_clients mapping for all 3 proxies.
        for (idx, sender) in senders.iter_mut().enumerate() {
            sender.send(idx).await.unwrap();
            combinator.recv().await.unwrap();
        }
        assert_eq!(combinator.internal_clients.len(), 3);

        // Kill proxy #1 by dropping its sender — this closes the channel,
        // causing client_fn to exit. The proxy first sends a ChannelClosed error
        // through tx_host, then tx_host is dropped causing None on the next poll.
        drop(senders.remove(1));

        // First recv may get the ChannelClosed error from the dying proxy.
        // We need to also send data from a live proxy so recv can eventually return it.
        senders[0].send(0).await.unwrap();

        // Drain events until we get a successful request from proxy #0.
        // This exercises the dead-channel cleanup path.
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match combinator.recv().await {
                    Ok(req) => return Ok(req),
                    Err(e) if matches!(e.kind(), ErrorKind::ChannelClosed) => {
                        // Expected — the dying proxy notified us of disconnect
                        continue;
                    }
                    Err(e) if matches!(e.kind(), ErrorKind::TransportProtocolDisconnect) => {
                        // This was the old fatal error — should not happen anymore,
                        // but if the dead channel is detected it's also fine to skip.
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
        })
        .await
        .expect("recv timed out — combinator is stuck on dead channel");
        assert!(result.is_ok(), "recv should succeed for live proxy");

        // Key assertion: we got a successful response from a live proxy despite
        // another proxy disconnecting — the combinator didn't crash or hang.
        // The dead_clients flag may or may not be set yet depending on poll ordering,
        // but the important thing is the combinator is still functional.
    }

    /// Test that send() to a disconnected client doesn't return an error.
    #[tokio::test]
    async fn test_send_to_disconnected_client_ok() {
        let (proxies, mut senders, _receivers) = setup_proxies();
        let mut combinator = ClientEventsCombinator::new(proxies);

        // Populate mappings.
        for (idx, sender) in senders.iter_mut().enumerate() {
            sender.send(idx).await.unwrap();
            combinator.recv().await.unwrap();
        }
        let client_ids: Vec<_> = combinator.internal_clients.keys().cloned().collect();

        // Kill proxy #1.
        drop(senders.remove(1));

        // Drain the dead channel notification by receiving — this will skip the dead one.
        senders[0].send(0).await.unwrap();
        drop(tokio::time::timeout(std::time::Duration::from_secs(5), combinator.recv()).await);

        // Sending to any client (including the now-dead one) should succeed (not panic/error).
        for cli_id in &client_ids {
            let result = combinator.send(*cli_id, Ok(HostResponse::Ok)).await;
            assert!(result.is_ok(), "send should not fail for client {cli_id}");
        }
    }

    /// A proxy that returns N configurable errors before delegating to SampleProxy.
    struct ErrorThenOkProxy {
        errors: VecDeque<ErrorKind>,
        inner: SampleProxy,
    }

    impl ErrorThenOkProxy {
        fn new(id: usize, errors: Vec<ErrorKind>, rx: Receiver<usize>, tx: Sender<usize>) -> Self {
            Self {
                errors: VecDeque::from(errors),
                inner: SampleProxy::new(id, rx, tx),
            }
        }
    }

    impl ClientEventsProxy for ErrorThenOkProxy {
        fn recv(&mut self) -> BoxFuture<'_, crate::client_events::HostIncomingMsg> {
            Box::pin(async {
                if let Some(kind) = self.errors.pop_front() {
                    return Err(kind.into());
                }
                self.inner.recv().await
            })
        }

        fn send(
            &mut self,
            id: ClientId,
            response: Result<HostResponse, ClientError>,
        ) -> BoxFuture<'_, Result<(), ClientError>> {
            self.inner.send(id, response)
        }
    }

    /// Regression test for #3479: a transient error (NodeUnavailable) from client.recv()
    /// should NOT permanently kill the client slot. The error should be forwarded to the
    /// host and the proxy should continue accepting connections.
    #[tokio::test]
    async fn test_transient_error_does_not_kill_client_slot() {
        let (tx_trigger, rx_trigger) = channel(1);
        let (tx_response, _rx_response) = channel(1);

        let proxy = Box::new(ErrorThenOkProxy::new(
            0,
            vec![ErrorKind::NodeUnavailable],
            rx_trigger,
            tx_response,
        )) as BoxedClient;

        let mut combinator = ClientEventsCombinator::new([proxy]);

        // First recv should get the forwarded NodeUnavailable error (not hang forever)
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), combinator.recv())
            .await
            .expect("recv timed out — client_fn died on transient error");
        match result {
            Err(e) => assert!(
                matches!(e.kind(), ErrorKind::NodeUnavailable),
                "error should be NodeUnavailable, got: {e}"
            ),
            Ok(_) => panic!("should receive the transient error, got Ok"),
        }

        // Now send a valid request through the proxy — it should still work
        tx_trigger.send(0).await.unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), combinator.recv())
            .await
            .expect("recv timed out — client slot is dead after transient error");
        assert!(
            result.is_ok(),
            "proxy should still be alive after transient error"
        );
    }

    /// Test that fatal errors (Shutdown) still kill the client slot as expected.
    #[tokio::test]
    async fn test_fatal_error_kills_client_slot() {
        let (_tx_trigger, rx_trigger) = channel(1);
        let (tx_response, _rx_response) = channel(1);

        let proxy = Box::new(ErrorThenOkProxy::new(
            0,
            vec![ErrorKind::Shutdown],
            rx_trigger,
            tx_response,
        )) as BoxedClient;

        let mut combinator = ClientEventsCombinator::new([proxy]);

        // First recv should get the forwarded Shutdown error
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), combinator.recv())
            .await
            .expect("recv timed out");
        match result {
            Err(e) => assert!(
                matches!(e.kind(), ErrorKind::Shutdown),
                "error should be Shutdown, got: {e}"
            ),
            Ok(_) => panic!("should receive the Shutdown error, got Ok"),
        }

        // The client slot is dead — the combinator should report shutdown (all slots dead).
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), combinator.recv())
            .await
            .expect("recv timed out");
        assert!(
            matches!(result, Err(ref e) if matches!(e.kind(), ErrorKind::Shutdown)),
            "combinator should report shutdown after fatal error killed the slot"
        );
    }

    /// Test that exceeding MAX_CONSECUTIVE_TRANSIENT_ERRORS kills the slot.
    #[tokio::test]
    async fn test_consecutive_error_limit_kills_slot() {
        let (_tx_trigger, rx_trigger) = channel(1);
        let (tx_response, _rx_response) = channel(1);

        let proxy = Box::new(ErrorThenOkProxy::new(
            0,
            vec![ErrorKind::NodeUnavailable; MAX_CONSECUTIVE_TRANSIENT_ERRORS],
            rx_trigger,
            tx_response,
        )) as BoxedClient;

        let mut combinator = ClientEventsCombinator::new([proxy]);

        // Drain all transient errors
        for i in 0..MAX_CONSECUTIVE_TRANSIENT_ERRORS {
            let result = tokio::time::timeout(std::time::Duration::from_secs(5), combinator.recv())
                .await
                .unwrap_or_else(|_| panic!("recv timed out on error {i}"));
            assert!(result.is_err(), "iteration {i} should be an error");
        }

        // Slot should now be dead — combinator reports shutdown
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), combinator.recv())
            .await
            .expect("recv timed out — combinator should report shutdown");
        assert!(
            matches!(result, Err(ref e) if matches!(e.kind(), ErrorKind::Shutdown)),
            "combinator should report shutdown after consecutive error limit"
        );
    }

    /// Test that multiple consecutive transient errors don't kill the slot
    /// (as long as they stay under MAX_CONSECUTIVE_TRANSIENT_ERRORS).
    #[tokio::test]
    async fn test_multiple_transient_errors_survive() {
        let (tx_trigger, rx_trigger) = channel(1);
        let (tx_response, _rx_response) = channel(1);

        let proxy = Box::new(ErrorThenOkProxy::new(
            0,
            vec![ErrorKind::NodeUnavailable; 5],
            rx_trigger,
            tx_response,
        )) as BoxedClient;

        let mut combinator = ClientEventsCombinator::new([proxy]);

        // Drain all 5 transient errors
        for i in 0..5 {
            let result = tokio::time::timeout(std::time::Duration::from_secs(5), combinator.recv())
                .await
                .unwrap_or_else(|_| panic!("recv timed out on error {i}"));
            match result {
                Err(e) => assert!(
                    matches!(e.kind(), ErrorKind::NodeUnavailable),
                    "error {i} should be NodeUnavailable, got: {e}"
                ),
                Ok(_) => panic!("error {i} should be an error, got Ok"),
            }
        }

        // Now send a valid request — slot should still be alive
        tx_trigger.send(0).await.unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), combinator.recv())
            .await
            .expect("recv timed out — client slot died after transient errors");
        assert!(
            result.is_ok(),
            "proxy should still be alive after multiple transient errors"
        );
    }

    /// Test that with_slot_names works and dead slots still allow live ones to continue.
    #[tokio::test]
    async fn test_slot_names_and_dead_slot_continues() {
        let (proxies, mut senders, _receivers) = setup_proxies();
        let mut combinator =
            ClientEventsCombinator::new(proxies).with_slot_names(&["http", "websocket", "extra"]);

        for (idx, sender) in senders.iter_mut().enumerate() {
            sender.send(idx).await.unwrap();
            combinator.recv().await.unwrap();
        }

        // Kill slot 1 ("websocket")
        drop(senders.remove(1));

        // Send data from a live proxy
        senders[0].send(0).await.unwrap();

        // Should eventually get a successful request from a live proxy
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match combinator.recv().await {
                    Ok(req) => return Ok(req),
                    Err(e) if matches!(e.kind(), ErrorKind::ChannelClosed) => continue,
                    Err(e) if matches!(e.kind(), ErrorKind::TransportProtocolDisconnect) => {
                        continue
                    }
                    Err(e) => return Err(e),
                }
            }
        })
        .await
        .expect("recv timed out — combinator should continue with live slots");
        assert!(result.is_ok(), "dead slot should not kill combinator");
    }
}
