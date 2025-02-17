use freenet_stdlib::prelude::Connection;

pub fn convert_websocket_stream<S>(
    stream: tokio_tungstenite::WebSocketStream<S>,
) -> Connection {
    // This is safe because the WebSocketStream types are structurally identical
    // even though they come from different versions
    unsafe { std::mem::transmute(stream) }
}
