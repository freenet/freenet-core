use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Context;
use std::{collections::HashMap, task::Poll};

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
    hosts_rx: Box<[Receiver<HostIncomingMsg>; N]>,
    /// pending client futures from the client applications, if any
    #[allow(clippy::type_complexity)]
    pending_client_futs:
        [Option<Pin<Box<dyn Future<Output = Option<HostIncomingMsg>> + Send + Sync>>>; N],
    /// a map of the individual protocols, external, sending client events ids to an internal list of ids
    external_clients: [HashMap<ClientId, ClientId>; N],
    /// a map of the external id to which protocol it belongs (represented by the index in the array)
    /// and the original id (reverse of indexes)
    internal_clients: HashMap<ClientId, (usize, ClientId)>,
}

impl<const N: usize> ClientEventsCombinator<N> {
    pub fn new(clients: [BoxedClient; N]) -> Self {
        // TODO: here share connection between clients reusing the local HTTP server when it applies
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

        // initialize receiving channels for first time
        // give an stable address on the heap to hosts_rx
        let mut hosts_rx = Box::new(hosts_rx.map(|c| c.unwrap()));
        let mut pending_client_futs = [(); N].map(|_| None);
        // Safety: create a longer living reference, is safe since the futures should always be as long living as the refs
        let h = unsafe {
            &mut *(&mut hosts_rx as *mut _)
                as &mut [Receiver<Result<(ClientId, ClientRequest), ClientError>>; N]
        };
        for (i, rx) in h.iter_mut().enumerate() {
            let a = pending_client_futs.get_mut(i).unwrap();
            let f = Box::pin(rx.recv()) as Pin<Box<dyn Future<Output = _> + Send + Sync>>;
            *a = Some(f);
        }

        let external_clients = [(); N].map(|_| HashMap::new());
        Self {
            clients: clients.map(|c| c.unwrap()),
            hosts_rx,
            pending_client_futs,
            external_clients,
            internal_clients: HashMap::new(),
        }
    }
}

impl<const N: usize> Drop for ClientEventsCombinator<N> {
    fn drop(&mut self) {
        // make sure that the pending futures are not left outstanding with danglign references to self
        let _ = std::mem::replace(&mut self.pending_client_futs, [(); N].map(|_| None));
    }
}

#[async_trait::async_trait]
impl<const N: usize> ClientEventsProxy for ClientEventsCombinator<N> {
    async fn recv(&mut self) -> Result<(ClientId, ClientRequest), ClientError> {
        let mut futs_opt = [(); N].map(|_| None);
        for (i, o) in self.pending_client_futs.iter_mut().enumerate() {
            let f = futs_opt.get_mut(i).unwrap();
            *f = o.take();
        }
        let (res, idx, mut others) = select_all(futs_opt.map(|f| f.unwrap())).await;
        let idle_ch = &mut self.hosts_rx[idx];
        others[idx] = Some(Box::pin(idle_ch.recv()));
        if let Some(res) = res {
            match res {
                Ok((external, r)) => {
                    let id = (&mut self.external_clients[idx])
                        .entry(external)
                        .or_insert_with(|| {
                            // add a new mapped external client id
                            let internal =
                                ClientId(COMBINATOR_INDEXES.fetch_add(1, Ordering::SeqCst));
                            self.internal_clients.insert(internal, (idx, external));
                            internal
                        });
                    Ok((*id, r))
                }
                err @ Err(_) => err,
            }
        } else {
            Err(ErrorKind::TransportProtocolDisconnect.into())
        }
    }

    async fn send(
        &mut self,
        internal: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> Result<(), ClientError> {
        let (idx, external) = self
            .internal_clients
            .get(&internal)
            .ok_or(ErrorKind::UnknownClient(internal))?;
        (&self.clients[*idx])
            .send((*external, response))
            .await
            .map_err(|_| ErrorKind::TransportProtocolDisconnect)?;
        Ok(())
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
                    Ok(msg) => {
                        if tx_host.send(Ok(msg)).await.is_err() {
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
}

/// An optimized for the use case version of `futures::select_all` which keeps ordering.
#[must_use = "futures do nothing unless you `.await` or poll them"]
struct SelectAll<Fut, const N: usize> {
    inner: [Option<Fut>; N],
}

impl<Fut: Unpin, const N: usize> Unpin for SelectAll<Fut, N> {}

impl<Fut: Future + Unpin, const N: usize> Future for SelectAll<Fut, N> {
    type Output = (Fut::Output, usize, [Option<Fut>; N]);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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
                Poll::Ready((res, idx, rest))
            }
            None => Poll::Pending,
        }
    }
}

fn select_all<F, const N: usize>(iter: [F; N]) -> SelectAll<F, N>
where
    F: Future + Unpin,
{
    SelectAll {
        inner: iter.map(|f| Some(f)),
    }
}
