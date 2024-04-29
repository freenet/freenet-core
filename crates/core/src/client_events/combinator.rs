use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::{collections::HashMap, task::Poll};

use freenet_stdlib::client_api::{ErrorKind, HostResponse};
use futures::future::BoxFuture;
use futures::task::AtomicWaker;
use futures::FutureExt;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use super::{BoxedClient, ClientError, ClientId, HostResult, OpenRequest};

type HostIncomingMsg = Result<OpenRequest<'static>, ClientError>;

/// This type allows combining different sources of events into one and interoperation between them.
pub struct ClientEventsCombinator<const N: usize> {
    /// receiving end of the different client applications from the node
    clients: [Sender<(ClientId, HostResult)>; N],
    /// receiving end of the host node from the different client applications
    hosts_rx: [Receiver<HostIncomingMsg>; N],
    /// a map of the individual protocols, external, sending client events ids to an internal list of ids
    external_clients: [HashMap<ClientId, ClientId>; N],
    /// a map of the external id to which protocol it belongs (represented by the index in the array)
    /// and the original id (reverse of indexes)
    internal_clients: HashMap<ClientId, (usize, ClientId)>,
    #[allow(clippy::type_complexity)]
    pend_futs:
        [Option<Pin<Box<dyn Future<Output = Option<HostIncomingMsg>> + Sync + Send + 'static>>>; N],
}

impl<const N: usize> ClientEventsCombinator<N> {
    pub fn new(clients: [BoxedClient; N]) -> Self {
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
        let hosts_rx = hosts_rx.map(|h| h.unwrap());
        let external_clients = [(); N].map(|_| HashMap::new());
        Self {
            clients: clients.map(|c| c.unwrap()),
            hosts_rx,
            external_clients,
            internal_clients: HashMap::new(),
            pend_futs: [(); N].map(|_| None),
        }
    }
}

impl<const N: usize> super::ClientEventsProxy for ClientEventsCombinator<N> {
    fn recv<'a>(&'_ mut self) -> BoxFuture<'_, Result<OpenRequest<'static>, ClientError>> {
        Box::pin(async {
            let mut futs_opt = [(); N].map(|_| None);
            let pend_futs = &mut self.pend_futs;
            for (i, pend) in pend_futs.iter_mut().enumerate() {
                let fut = &mut futs_opt[i];
                if let Some(pend_fut) = pend.take() {
                    *fut = Some(pend_fut);
                } else {
                    // this receiver ain't awaiting, queue a new one
                    // SAFETY: is safe here to extend the lifetime since clients are required to be 'static
                    //         and we take ownership, so they will be alive for the duration of the program
                    let new_pend = unsafe {
                        std::mem::transmute(Box::pin(self.hosts_rx[i].recv())
                            as Pin<Box<dyn Future<Output = _> + Send + Sync + '_>>)
                    };
                    *fut = Some(new_pend);
                }
            }
            let (res, idx, mut others) = select_all(futs_opt.map(|f| f.unwrap())).await;
            let res = res
                .map(|res| {
                    match res {
                        Ok(OpenRequest {
                            client_id: external,
                            request,
                            notification_channel,
                            token,
                        }) => {
                            tracing::debug!(
                                "received request; internal_id={external}; req={request}"
                            );
                            let id =
                                *self.external_clients[idx]
                                    .entry(external)
                                    .or_insert_with(|| {
                                        // add a new mapped external client id
                                        let internal = ClientId::next();
                                        self.internal_clients.insert(internal, (idx, external));
                                        internal
                                    });

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
            // place back futs
            debug_assert!(pend_futs.iter().all(|f| f.is_none()));
            debug_assert_eq!(
                others.iter().filter(|a| a.is_some()).count(),
                pend_futs.len() - 1
            );
            std::mem::swap(pend_futs, &mut others);
            res
        })
    }

    fn send<'a>(
        &mut self,
        internal: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> BoxFuture<'_, Result<(), ClientError>> {
        Box::pin(async move {
            let (idx, external) = self
                .internal_clients
                .get(&internal)
                .ok_or(ErrorKind::UnknownClient(internal.0))?;
            self.clients[*idx]
                .send((*external, response))
                .await
                .map_err(|_| ErrorKind::TransportProtocolDisconnect)?;
            Ok(())
        })
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

/// An optimized for the use case version of `futures::select_all` which keeps ordering.
#[must_use = "futures do nothing unless you `.await` or poll them"]
struct SelectAll<Fut, const N: usize> {
    waker: AtomicWaker,
    inner: [Option<Fut>; N],
}

impl<Fut: Unpin, const N: usize> Unpin for SelectAll<Fut, N> {}

impl<Fut: Future + Unpin, const N: usize> Future for SelectAll<Fut, N> {
    type Output = (Fut::Output, usize, [Option<Fut>; N]);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        macro_rules! recv {
            () => {
                let item = self
                    .inner
                    .iter_mut()
                    .enumerate()
                    .find_map(|(i, f)| {
                        f.as_mut().map(|f| match f.poll_unpin(cx) {
                            Poll::Pending => None,
                            Poll::Ready(e) => Some((i, e)),
                        })
                    })
                    .flatten();
                match item {
                    Some((idx, res)) => {
                        eprintln!("polled {idx}");
                        self.inner[idx] = None;
                        let rest = std::mem::replace(&mut self.inner, [(); N].map(|_| None));
                        return Poll::Ready((res, idx, rest));
                    }
                    None => {}
                }
            };
        }
        recv!();
        self.waker.register(cx.waker());
        recv!();
        Poll::Pending
    }
}

fn select_all<F, const N: usize>(iter: [F; N]) -> SelectAll<F, N>
where
    F: Future + Unpin,
{
    SelectAll {
        waker: AtomicWaker::new(),
        inner: iter.map(|f| Some(f)),
    }
}

#[cfg(test)]
mod test {
    use freenet_stdlib::client_api::ClientRequest;

    use super::*;
    use crate::client_events::ClientEventsProxy;

    struct SampleProxy {
        id: usize,
        rx: Receiver<usize>,
    }

    impl SampleProxy {
        fn new(id: usize, rx: Receiver<usize>) -> Self {
            Self { id, rx }
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
            _id: ClientId,
            _response: Result<HostResponse, ClientError>,
        ) -> BoxFuture<'_, Result<(), ClientError>> {
            todo!()
        }
    }

    #[ignore]
    #[tokio::test]
    async fn combinator_recv() {
        let mut cnt = 0;
        let mut senders = vec![];
        let proxies = [None::<()>; 3].map(|_| {
            let (tx, rx) = channel(1);
            senders.push(tx);
            let r = Box::new(SampleProxy::new(cnt, rx)) as _;
            cnt += 1;
            r
        });
        let mut combinator = ClientEventsCombinator::new(proxies);

        let _senders = tokio::task::spawn(async move {
            for (id, tx) in senders.iter_mut().enumerate() {
                tx.send(id).await.unwrap();
                eprintln!("sent msg {id}");
            }
            senders
        })
        .await
        .unwrap();

        for i in 0..3 {
            let OpenRequest { client_id: id, .. } = combinator.recv().await.unwrap();
            eprintln!("received: {id:?}");
            assert_eq!(ClientId::new(i), id);
        }
    }
}
