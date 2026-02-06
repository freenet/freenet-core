//! Integration test verifying fdev properly waits for server responses
//!
//! This test ensures that the WebSocket client doesn't close the connection
//! before receiving a response from the server (fixing issue #2278).

use freenet_stdlib::client_api::{
    ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi,
};
use freenet_stdlib::prelude::*;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;

static PORT: AtomicU16 = AtomicU16::new(54000);

/// Test that the WebSocket client properly waits for a PutResponse
///
/// Before the fix for #2278, fdev would close the connection before
/// receiving the response, causing "Connection reset without closing handshake"
/// errors on the server side.
#[tokio::test]
async fn test_websocket_client_waits_for_put_response() {
    let port = PORT.fetch_add(1, Ordering::SeqCst);

    // Create a mock contract key for the response (base58 encoded)
    let instance_id = ContractInstanceId::try_from("11111111111111111111111111111111".to_string())
        .expect("valid id");
    let mock_key = ContractKey::from_id_and_code(instance_id, CodeHash::new([0u8; 32]));
    let response: HostResponse<WrappedState> =
        HostResponse::ContractResponse(ContractResponse::PutResponse { key: mock_key });

    // Channel to signal when server is ready to accept connections
    let (ready_tx, ready_rx) = oneshot::channel::<()>();

    // Start the mock server
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))
        .await
        .expect("bind");

    let server_response = response.clone();
    let server_handle = tokio::spawn(async move {
        // Signal that we're ready to accept connections
        let _ = ready_tx.send(());

        let (stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("accept timeout")
            .expect("accept");

        let mut ws_stream = tokio_tungstenite::accept_async(stream)
            .await
            .expect("ws accept");

        use futures::{SinkExt, StreamExt};

        // Receive the request
        let msg = tokio::time::timeout(Duration::from_secs(5), ws_stream.next())
            .await
            .expect("receive timeout")
            .expect("stream not empty")
            .expect("receive");

        // Verify we received a binary message (contract requests are binary)
        match msg {
            Message::Binary(_) => {} // Request received successfully
            _ => panic!("expected binary message"),
        };

        // Send back the response
        let response_bytes = bincode::serialize(&Ok::<_, freenet_stdlib::client_api::ClientError>(
            server_response,
        ))
        .expect("serialize");
        ws_stream
            .send(Message::Binary(response_bytes.into()))
            .await
            .expect("send response");
    });

    // Wait for server to be ready before connecting
    ready_rx.await.expect("server ready signal");

    // Connect client
    let url = format!("ws://127.0.0.1:{port}/v1/contract/command?encodingProtocol=native");
    let (stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("connect");
    let mut client = WebApi::start(stream);

    // Create a minimal contract for the request
    let code = ContractCode::from(vec![0u8; 32]);
    let wrapped = WrappedContract::new(Arc::new(code), Parameters::from(vec![]));
    let api_version = ContractWasmAPIVersion::V1(wrapped);
    let contract = ContractContainer::from(api_version);

    // Send a Put request (simulating what fdev does)
    let request = ClientRequest::ContractOp(ContractRequest::Put {
        contract,
        state: WrappedState::new(vec![]),
        related_contracts: RelatedContracts::default(),
        subscribe: false,
        blocking_subscribe: false,
    });

    client.send(request).await.expect("send request");

    // This is the key behavior: we must receive the response before dropping the client.
    // Before the fix, fdev would exit here without waiting, causing connection reset.
    let response = tokio::time::timeout(Duration::from_secs(5), client.recv())
        .await
        .expect("response timeout")
        .expect("receive response");

    // Verify we got the expected response
    match response {
        HostResponse::ContractResponse(ContractResponse::PutResponse { key }) => {
            assert_eq!(key, mock_key);
        }
        other => panic!("unexpected response: {:?}", other),
    }

    // Wait for server to complete
    server_handle.await.expect("server task");
}
