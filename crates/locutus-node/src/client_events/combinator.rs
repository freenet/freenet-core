use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Context;
use std::{collections::HashMap, task::Poll};

use futures::task::AtomicWaker;
use futures::FutureExt;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use super::{BoxedClient, ClientError, HostResult};
use crate::client_events::ErrorKind;
use crate::{ClientEventsProxy, ClientId, ClientRequest, HostResponse};

type HostIncomingMsg = Result<(ClientId, ClientRequest), ClientError>;

static COMBINATOR_INDEXES: AtomicUsize = AtomicUsize::new(0);

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

#[allow(clippy::needless_lifetimes)]
impl<const N: usize> ClientEventsProxy for ClientEventsCombinator<N> {
    fn recv(
        &mut self,
    ) -> Pin<
        Box<dyn Future<Output = Result<(ClientId, ClientRequest), ClientError>> + Send + Sync + '_>,
    > {
        Box::pin(async {
            let mut futs_opt = [(); N].map(|_| None);
            let pend_futs = &mut self.pend_futs;
            for (i, pend) in pend_futs.iter_mut().enumerate() {
                let f = &mut futs_opt[i];
                if let Some(pend_fut) = pend.take() {
                    *f = Some(pend_fut);
                } else {
                    // this receiver ain't awaiting, queue a new one
                    // SAFETY: is safe here to extend the lifetime since clients are required to be 'static
                    //         and we take ownership, so they will be alive for the duration of the program
                    let new_pend = unsafe {
                        std::mem::transmute(Box::pin(self.hosts_rx[i].recv())
                            as Pin<Box<dyn Future<Output = _> + Send + Sync + '_>>)
                    };
                    *f = Some(new_pend);
                }
            }
            let (res, idx, others) =
                futures::future::select_all(futs_opt.map(|f| f.unwrap())).await;
            if let Some(res) = res {
                match res {
                    Ok((external, r)) => {
                        log::debug!("received request; internal_id={external}; req={r}");
                        let id = *(&mut self.external_clients[idx])
                            .entry(external)
                            .or_insert_with(|| {
                                // add a new mapped external client id
                                let internal =
                                    ClientId(COMBINATOR_INDEXES.fetch_add(1, Ordering::SeqCst));
                                self.internal_clients.insert(internal, (idx, external));
                                internal
                            });

                        // place back futs
                        debug_assert!(pend_futs.iter().all(|f| f.is_none()));
                        debug_assert_eq!(others.len(), pend_futs.len() - 1);
                        for (i, fut) in others.into_iter().enumerate() {
                            let p = &mut pend_futs.get_mut(i);
                            let _ = p.insert(&mut Some(fut));
                        }
                        Ok((id, r))
                    }
                    err @ Err(_) => err,
                }
            } else {
                Err(ErrorKind::TransportProtocolDisconnect.into())
            }
        })
    }

    fn send<'a>(
        &'a mut self,
        internal: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ClientError>> + Send + Sync + '_>> {
        Box::pin(async move {
            let (idx, external) = self
                .internal_clients
                .get(&internal)
                .ok_or(ErrorKind::UnknownClient(internal))?;
            (&self.clients[*idx])
                .send((*external, response))
                .await
                .map_err(|_| ErrorKind::TransportProtocolDisconnect)?;
            Ok(())
        })
    }

    fn cloned(&self) -> BoxedClient {
        todo!()
    }
}

async fn client_fn(
    mut client: BoxedClient,
    mut rx: Receiver<(ClientId, HostResult)>,
    tx_host: Sender<Result<(ClientId, ClientRequest), ClientError>>,
) {
    loop {
        tokio::select! {
            host_msg = rx.recv() => {
                if let Some((client_id, response)) = host_msg {
                    if client.send(client_id, response).await.is_err() {
                        break;
                    }
                } else {
                    log::debug!("disconnected host");
                    break;
                }
            }
            client_msg = client.recv() => {
                match client_msg {
                    Ok((id, msg)) => {
                        log::debug!("received msg @ combinator from external id {id}, msg: {msg}");
                        if tx_host.send(Ok((id, msg))).await.is_err() {
                            break;
                        }
                    }
                    Err(err) if err.kind() == ErrorKind::ChannelClosed =>{
                        log::debug!("disconnected client");
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
    log::error!("client shut down");
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

// FIXME
fn _select_all<F, const N: usize>(iter: [F; N]) -> SelectAll<F, N>
where
    F: Future + Unpin,
{
    SelectAll {
        waker: AtomicWaker::new(),
        inner: iter.map(|f| Some(f)),
    }
}
