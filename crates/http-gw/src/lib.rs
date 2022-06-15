pub(crate) mod contract_web_handling;
pub(crate) mod errors;
mod http_gateway;
pub(crate) mod state_handling;

use std::io::{Cursor, Read};

use byteorder::{BigEndian, ReadBytesExt};
pub use http_gateway::HttpGateway;
use locutus_node::{either::Either, ClientError, ClientId, ClientRequest, ErrorKind, HostResponse};
use locutus_runtime::{ContractKey, WrappedState};
use tar::Archive;
use xz2::bufread::XzDecoder;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;
type HostResult = (ClientId, Result<HostResponse, ClientError>);
type ClientHandlingMessage = (
    ClientRequest,
    Either<tokio::sync::mpsc::UnboundedSender<HostResult>, ClientId>,
);

#[cfg(feature = "local")]
pub mod local_node {
    use std::net::{Ipv4Addr, SocketAddr};

    use locutus_dev::LocalNode;
    use locutus_node::{either, ClientError, ClientEventsProxy, ErrorKind, WebSocketProxy, RequestError};

    use crate::{DynError, HttpGateway};

    pub async fn set_local_node(mut local_node: LocalNode) -> Result<(), DynError> {
        let (mut http_handle, filter) = HttpGateway::as_filter();
        let socket: SocketAddr = (Ipv4Addr::LOCALHOST, 50509).into();
        let _ws_handle = WebSocketProxy::as_upgrade(socket, filter).await?;
        // FIXME: use combinator
        // let mut all_clients =
        //    ClientEventsCombinator::new([Box::new(ws_handle), Box::new(http_handle)]);
        loop {
            let (id, req) = http_handle.recv().await?;
            tracing::debug!("client {id}, req -> {req}");

            match local_node.handle_request(req).await {
                Ok(res) => {
                    http_handle.send(id, Ok(res)).await?;
                }
                Err(either::Left(RequestError::Disconnect)) => {}
                Err(either::Left(err)) => {
                    log::error!("{err}");
                    http_handle
                        .send(id, Err(ClientError::from(ErrorKind::from(err))))
                        .await?;
                }
                Err(either::Right(err)) => {
                    log::error!("{err}");
                    http_handle
                        .send(id, Err(ErrorKind::Unhandled(format!("{err}")).into()))
                        .await?;
                }
            }
        }
    }
}

pub struct UnpackedState<T: Read> {
    pub metadata: Vec<u8>,
    pub web: Archive<T>,
    pub state: WrappedState,
}

pub fn unpack_state(
    state: &[u8],
    key: &ContractKey,
) -> Result<UnpackedState<impl Read>, ClientError> {
    // Decompose the state and extract the compressed web interface
    let mut state = Cursor::new(state);
    let metadata_size = state
        .read_u64::<BigEndian>()
        .map_err(|_| ErrorKind::IncorrectState(*key))?;
    let mut metadata = vec![0; metadata_size as usize];
    state
        .read_exact(&mut metadata)
        .map_err(|_| ErrorKind::IncorrectState(*key))?;
    let web_size = state
        .read_u64::<BigEndian>()
        .map_err(|_| ErrorKind::IncorrectState(*key))?;
    let mut web = vec![0; web_size as usize];
    state
        .read_exact(&mut web)
        .map_err(|_| ErrorKind::IncorrectState(*key))?;
    let state_size = state
        .read_u64::<BigEndian>()
        .map_err(|_| ErrorKind::IncorrectState(*key))?;
    let mut dynamic_state = vec![0; state_size as usize];
    state
        .read_exact(&mut dynamic_state)
        .map_err(|_| ErrorKind::IncorrectState(*key))?;

    // Decode tar.xz and unpack contract web
    let decoder = XzDecoder::new(Cursor::new(web));
    let web = Archive::new(decoder);

    // Decode the dynamic state
    let state = WrappedState::from(dynamic_state);
    Ok(UnpackedState {
        metadata,
        web,
        state,
    })
}
