use warp::Filter;

use crate::{ClientEventsProxy, ClientRequest, HostResponse};

pub(crate) struct WebSocketInterface {}

#[async_trait::async_trait]
impl ClientEventsProxy for WebSocketInterface {
    type Error = String;

    async fn recv(&mut self) -> Result<ClientRequest, Self::Error> {
        todo!()
    }

    /// Sends a response from the host to the client application.
    async fn send(
        &mut self,
        response: Result<HostResponse, Self::Error>,
    ) -> Result<HostResponse, Self::Error> {
        todo!()
    }
}
