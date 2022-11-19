use super::{
	client_events::{ClientRequest}, 
	Error
};

pub struct WebApi {}

type Connection = web_sys::WebSocket;

impl WebApi {
    pub fn start(connection: Connection) -> Self {
        todo!()
    }
}

async fn process_request(
    conn: &mut Connection,
    req: Option<ClientRequest<'static>>,
) -> Result<(), Error> {
    let req = req.ok_or(Error::ChannelClosed)?;
    let _msg = rmp_serde::to_vec(&req)?;
    //     conn.send(Message::Binary(msg)).await?;
    Ok(())
}

async fn process_response(conn: &mut Connection, res: Option<Result<(), ()>>) -> Result<(), Error> {
    todo!()
}
