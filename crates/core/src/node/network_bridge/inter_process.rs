use std::sync::{Arc, OnceLock};

use futures::{future::BoxFuture, FutureExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter, Stdout},
    sync::{
        watch::{Receiver, Sender},
        Mutex,
    },
};

use crate::{
    message::NetMessage,
    node::{testing_impl::NetworkBridgeExt, PeerId},
};

use super::{ConnectionError, NetworkBridge};

type Data = Vec<u8>;

static INCOMING_DATA: OnceLock<Sender<Data>> = OnceLock::new();

#[derive(Clone)]
pub struct InterProcessConnManager {
    recv: Receiver<Data>,
    output: Arc<Mutex<BufWriter<Stdout>>>,
}

impl InterProcessConnManager {
    pub(in crate::node) fn new() -> Self {
        let (sender, recv) = tokio::sync::watch::channel(vec![]);
        INCOMING_DATA.set(sender).expect("shouldn't be set");
        Self {
            recv,
            output: Arc::new(Mutex::new(BufWriter::new(tokio::io::stdout()))),
        }
    }

    pub fn push_msg(data: Vec<u8>) {
        let _ = INCOMING_DATA.get().expect("should be set").send(data);
    }

    pub async fn pull_msg(
        stdout: &mut tokio::process::ChildStdout,
    ) -> std::io::Result<Option<(PeerId, Data)>> {
        let mut msg_len = [0u8; 4];
        let Ok(read_res) = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            stdout.read_exact(&mut msg_len),
        )
        .await
        else {
            return Ok(None);
        };
        read_res?;
        let msg_len = u32::from_le_bytes(msg_len) as usize;
        let buf = &mut vec![0u8; msg_len];
        stdout.read_exact(buf).await?;
        let (target, data) = bincode::deserialize(buf)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other))?;
        Ok(Some((target, data)))
    }
}

impl NetworkBridgeExt for InterProcessConnManager {
    fn recv(&mut self) -> BoxFuture<Result<NetMessage, ConnectionError>> {
        async {
            self.recv
                .changed()
                .await
                .map_err(|_| ConnectionError::Timeout)?;
            let data = &*self.recv.borrow();
            let deser = bincode::deserialize(data)?;
            Ok(deser)
        }
        .boxed()
    }
}

#[async_trait::async_trait]
impl NetworkBridge for InterProcessConnManager {
    async fn send(&self, target: &PeerId, msg: NetMessage) -> super::ConnResult<()> {
        tracing::debug!(%target, ?msg, "sending network message out");
        let data = bincode::serialize(&(*target, msg))?;
        let output = &mut *self.output.lock().await;
        output.write_all(&(data.len() as u32).to_le_bytes()).await?;
        output.write_all(&data).await?;
        output.flush().await?;
        tracing::debug!(%target, bytes = data.len(), "sent network message out");
        Ok(())
    }

    async fn add_connection(&mut self, _peer: PeerId) -> super::ConnResult<()> {
        Ok(())
    }

    async fn drop_connection(&mut self, _peer: &PeerId) -> super::ConnResult<()> {
        Ok(())
    }
}
