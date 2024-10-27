use std::collections::HashMap;

use freenet_stdlib::client_api::{ErrorKind, HostResponse};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use super::{BoxedClient, ClientError, ClientId, HostResult, OpenRequest};

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
}

impl<const N: usize> ClientEventsCombinator<N> {
    pub fn new(clients: [BoxedClient; N]) -> Self {
        let pending_futs = FuturesUnordered::new();
        let channels = clients.map(|client| {
            let (tx, rx) = channel(1);
            let (tx_host, rx_host) = channel(1);
            tokio::task::spawn(client_fn(client, rx, tx_host));
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
        }
    }
}

impl<const N: usize> super::ClientEventsProxy for ClientEventsCombinator<N> {
    fn recv(&mut self) -> BoxFuture<'_, Result<OpenRequest<'static>, ClientError>> {
        async {
            let Some((idx, mut rx, res)) = self.pending_futs.next().await else {
                unreachable!();
            };

            let res = res
                .map(|res| {
                    match res {
                        Ok(OpenRequest {
                            client_id: external,
                            request,
                            notification_channel,
                            token,
                        }) => {
                            let id = *self.external_clients[idx]
                                .entry(external)
                                .or_insert_with(|| {
                                    // add a new mapped external client id
                                    let internal = ClientId::next();
                                    self.internal_clients.insert(internal, (idx, external));
                                    internal
                                });
                            tracing::debug!("received request for proxy #{idx}; internal_id={id}; external_id={external}; req={request}");
                            Ok(OpenRequest {
                                client_id: id,
                                request,
                                notification_channel,
                                token,
                            })
                        }
                        err @ Err(_) => err,
                    }
                })
                .unwrap_or_else(|| Err(ErrorKind::TransportProtocolDisconnect.into()));

            self.pending_futs.push(
                async move {
                    let res = rx.recv().await;
                    (idx, rx, res)
                }
                .boxed(),
            );

            res
        }
        .boxed()
    }

    fn send<'a>(
        &mut self,
        internal: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> BoxFuture<'_, Result<(), ClientError>> {
        async move {
            let (idx, external) = self
                .internal_clients
                .get(&internal)
                .ok_or(ErrorKind::UnknownClient(internal.0))?;
            self.clients[*idx]
                .send((*external, response))
                .await
                .map_err(|_| ErrorKind::TransportProtocolDisconnect)?;
            Ok(())
        }
        .boxed()
    }
}

async fn client_fn(
    mut client: BoxedClient,
    mut rx: Receiver<(ClientId, HostResult)>,
    tx_host: Sender<Result<OpenRequest<'static>, ClientError>>,
) {
    loop {
        tokio::select! {
            host_msg = rx.recv() => {
                if let Some((client_id, response)) = host_msg {
                    if client.send(client_id, response).await.is_err() {
                        eprintln!("disconnected host");
                        break;
                    }
                } else {
                    tracing::debug!("disconnected host");
                    break;
                }
            }
            client_msg = client.recv() => {
                match client_msg {
                    Ok(OpenRequest { client_id,  request, notification_channel, token }) => {
                        tracing::debug!("received msg @ combinator from external id {client_id}, msg: {request}");
                        if tx_host.send(Ok(OpenRequest { client_id,  request, notification_channel, token })).await.is_err() {
                            break;
                        }
                    }
                    Err(err) if matches!(err.kind(), ErrorKind::ChannelClosed) =>{
                        tracing::debug!("disconnected client");
                        let _ = tx_host.send(Err(err)).await;
                        break;
                    }
                    Err(err) => {
                        panic!("Error of kind: {err} not handled");
                    }
                }
            }
        }
    }
    tracing::error!("Client shut down");
}

#[cfg(test)]
mod test {
    use freenet_stdlib::client_api::ClientRequest;
    use futures::try_join;

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
                eprintln!("#{}, received msg {id}", self.id);
                Ok(OpenRequest::new(
                    ClientId::new(id),
                    Box::new(ClientRequest::Disconnect { cause: None }),
                ))
            })
        }

        fn send(
            &mut self,
            id: ClientId,
            _response: Result<HostResponse, ClientError>,
        ) -> BoxFuture<'_, Result<(), ClientError>> {
            assert_eq!(id.0, self.id);
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
    async fn combinator_recv() {
        let (proxies, mut senders, _) = setup_proxies();
        let mut combinator = ClientEventsCombinator::new(proxies);

        let _senders: Vec<Sender<usize>> = tokio::task::spawn(async move {
            for _ in 1..4 {
                for (id, tx) in senders.iter_mut().enumerate() {
                    tx.send(id).await.unwrap();
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    eprintln!("sent msg {id}");
                }
            }
            senders
        })
        .await
        .unwrap();

        for _ in 0..3 {
            for i in 1..4 {
                let OpenRequest { client_id: id, .. } = combinator.recv().await.unwrap();
                eprintln!("received {i}: {id:?}");
                assert_eq!(ClientId::new(i), id);
            }
        }
    }

    #[ignore]
    #[tokio::test]
    async fn test_send() {
        let (proxies, _, mut receivers) = setup_proxies();
        let mut combinator = ClientEventsCombinator::new(proxies);

        for idx in 0..3 {
            let client_id = ClientId::new(idx);
            // Insert each client ID mapping into the combinator's internal clients.
            combinator
                .internal_clients
                .insert(client_id, (idx, client_id));
        }

        let receivers = async move {
            // Test sending a response through the combinator for each proxy.
            for (idx, receiver) in receivers.iter_mut().enumerate() {
                // Assert that the receiver received the expected message.
                let received_id = receiver
                    .recv()
                    .await
                    .ok_or(format!("missing {idx} sender"))?;
                assert_eq!(received_id, idx);
                println!(
                    "Receiver {} confirmed send for client ID: {}",
                    idx, received_id
                );
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        };

        let senders = async {
            for idx in 0..3 {
                // Send a sample response through the combinator.
                combinator
                    .send(ClientId::new(idx), Ok(HostResponse::Ok))
                    .await
                    .map_err(|err| format!("Send failed for client {idx}: {err}",))?;
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        };

        try_join!(senders, receivers).unwrap();
    }
}
