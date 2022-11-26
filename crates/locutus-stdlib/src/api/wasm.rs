use wasm_bindgen::{prelude::Closure, JsCast};
use web_sys::{ErrorEvent, MessageEvent};

use super::{client_events::ClientRequest, Error, HostResult};

type Connection = web_sys::WebSocket;

pub struct WebApi {
    conn: Connection,
}

impl WebApi {
    pub fn start(
        conn: Connection,
        result_handler: impl FnOnce(HostResult) + Copy + 'static,
        error_handler: impl FnOnce(Error) + Copy + 'static,
        onopen_handler: impl FnOnce() + Copy + 'static,
    ) -> Self {
        let onmessage_callback = Closure::<dyn FnMut(_)>::new(move |e: MessageEvent| {
            let response: HostResult = match serde_wasm_bindgen::from_value(e.data()) {
                Ok(val) => val,
                Err(err) => {
                    tracing::error!(%err, "error deserializing message from host");
                    error_handler(Error::OtherError(format!("{err}").into()));
                    return;
                }
            };
            result_handler(response);
        });
        conn.set_onmessage(Some(onmessage_callback.as_ref().unchecked_ref()));
        onmessage_callback.forget();

        let onerror_callback = Closure::<dyn FnMut(_)>::new(move |e: ErrorEvent| {
            let error = format!(
                "error: {file}:{lineno}: {msg}",
                file = e.filename(),
                lineno = e.lineno(),
                msg = e.message()
            );
            error_handler(Error::OtherError(error.into()));
        });
        conn.set_onerror(Some(onerror_callback.as_ref().unchecked_ref()));
        onerror_callback.forget();

        let onopen_callback = Closure::<dyn FnMut()>::new(move || {
            onopen_handler();
        });
        conn.set_onopen(Some(onopen_callback.as_ref().unchecked_ref()));
        onopen_callback.forget();

        conn.set_binary_type(web_sys::BinaryType::Blob);
        WebApi { conn }
    }

    pub fn send(&mut self, request: ClientRequest<'static>) -> Result<(), Error> {
        let send = rmp_serde::to_vec(&request)?;
        self.conn.send_with_u8_array(&send).map_err(
            |err| match serde_wasm_bindgen::from_value(err) {
                Ok(e) => Error::ConnectionError(e),
                Err(e) => Error::OtherError(format!("{e}").into()),
            },
        )?;
        Ok(())
    }

    pub fn disconnect(self, cause: impl AsRef<str>) {
        let _ = self.conn.close_with_code_and_reason(1000, cause.as_ref());
    }
}
