use std::task::Poll;

use futures::{pin_mut, FutureExt, Sink, SinkExt, Stream, StreamExt};
use tokio::{
    net::TcpStream,
    sync::mpsc::{self, Receiver, Sender},
};
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};

use super::{
    client_events::{ClientError, ClientRequest, ErrorKind},
    Error, HostResult,
};

type Connection = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct WebApi {
    request_tx: Sender<ClientRequest<'static>>,
    response_rx: Receiver<HostResult>,
    queue: Vec<ClientRequest<'static>>,
}

impl Stream for WebApi {
    type Item = HostResult;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.response_rx.poll_recv(cx)
    }
}

impl Sink<ClientRequest<'static>> for WebApi {
    type Error = ClientError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if self.queue.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        item: ClientRequest<'static>,
    ) -> Result<(), Self::Error> {
        self.queue.push(item);
        Ok(())
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut queue = vec![];
        std::mem::swap(&mut queue, &mut self.queue);
        let mut error = false;
        while let Some(item) = queue.pop() {
            let f = self.request_tx.send(item);
            pin_mut!(f);
            match f.poll_unpin(cx) {
                Poll::Ready(Ok(_)) => continue,
                Poll::Ready(Err(_err)) => {
                    error = true;
                    break;
                }
                Poll::Pending => {}
            }
        }
        if error {
            self.queue.append(&mut queue);
            Poll::Ready(Err(ErrorKind::ChannelClosed.into()))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

impl WebApi {
    pub fn start(connection: Connection) -> Self {
        let (request_tx, request_rx) = mpsc::channel(1);
        let (response_tx, response_rx) = mpsc::channel(1);
        tokio::spawn(request_handler(request_rx, response_tx, connection));
        Self {
            request_tx,
            response_rx,
            queue: vec![],
        }
    }

    #[inline]
    pub async fn request(
        &mut self,
        request: ClientRequest<'static>,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<ClientRequest<'static>>> {
        self.request_tx.send(request).await
    }

    #[inline]
    pub async fn response(&mut self) -> HostResult {
        let res = self.response_rx.recv().await;
        res.ok_or_else(|| ClientError::from(ErrorKind::ChannelClosed))?
    }
}

async fn request_handler(
    mut request_rx: Receiver<ClientRequest<'static>>,
    mut response_tx: Sender<HostResult>,
    mut conn: Connection,
) {
    let error = loop {
        tokio::select! {
            req = request_rx.recv() => {
                match process_request(&mut conn, req).await {
                    Ok(_) => continue,
                    Err(err) => break err,
                }
            }
            res = conn.next() => {
                match process_response(&mut conn, &mut response_tx, res).await {
                    Ok(_) => continue,
                    Err(err) => break err,
                }
            }
        }
    };
    tracing::error!(?error, "request handler error");
    let error = match error {
        Error::ChannelClosed => ErrorKind::ChannelClosed.into(),
        other => ClientError::from(format!("{other}")),
    };
    let _ = response_tx.send(Err(error)).await;
}

#[inline]
async fn process_request(
    conn: &mut Connection,
    req: Option<ClientRequest<'static>>,
) -> Result<(), Error> {
    let req = req.ok_or(Error::ChannelClosed)?;
    let msg = rmp_serde::to_vec(&req)?;
    conn.send(Message::Binary(msg)).await?;
    Ok(())
}

#[inline]
async fn process_response(
    conn: &mut Connection,
    response_tx: &mut Sender<HostResult>,
    res: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
) -> Result<(), Error> {
    let res = res.ok_or(Error::ConnectionClosed)??;
    match res {
        Message::Text(msg) => {
            let response: HostResult = rmp_serde::from_read(msg.as_bytes())?;
            response_tx
                .send(response)
                .await
                .map_err(|_| Error::ChannelClosed)?;
        }
        Message::Binary(binary) => {
            let response: HostResult = rmp_serde::from_read(binary.as_slice())?;
            response_tx
                .send(response)
                .await
                .map_err(|_| Error::ChannelClosed)?;
        }
        Message::Ping(_) => {
            conn.send(Message::Pong(vec![0, 5, 3, 9])).await?;
        }
        Message::Pong(_) => {}
        Message::Close(_) => return Err(Error::ConnectionClosed),
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{net::Ipv4Addr, sync::atomic::AtomicU16};
    use tokio::net::TcpListener;

    static PORT: AtomicU16 = AtomicU16::new(65495);

    struct Server {
        listener: TcpListener,
    }

    impl Server {
        async fn new() -> Self {
            let listener = tokio::net::TcpListener::bind((
                Ipv4Addr::LOCALHOST,
                PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            ))
            .await
            .unwrap();
            Server { listener }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_send() {
        // let test_conn = WebSocketStream
        todo!();
    }
}
