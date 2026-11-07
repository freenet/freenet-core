//! End-to-end check of user-approved app grants for native clients (#5264).
//!
//! Boots a real node (event loop, executor, WASM runtime) with its client API
//! on loopback, and drives the whole flow over real HTTP and WebSocket:
//! request a grant, approve it as the browser page would, collect the token by
//! polling, connect with it, and have a real delegate (`test-delegate-attested`)
//! echo the `MessageOrigin` the runtime handed it. Then revoke the grant and
//! check that a connection held open with it is closed, and that the token is
//! refused afterwards.
//!
//! The attested-origin assertion is what fails if the WebSocket upgrade stops
//! consulting the grant store: the token is not a shell-page token, so without
//! that lookup the connection is refused as carrying an invalid token.

// A `_`/`other` arm treats any unexpected variant as a test failure; listing
// every variant of every non_exhaustive stdlib enum would be brittle noise.
#![allow(clippy::wildcard_enum_match_arm)]

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener},
    path::Path,
    time::{Duration, Instant},
};

use freenet::{
    local_node::NodeConfig, server::serve_client_api_with_listener, test_utils::load_delegate,
};
use freenet_stdlib::{
    client_api::{ClientRequest, DelegateRequest, HostResponse, WebApi},
    prelude::*,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use testresult::TestResult;
use tokio::time::timeout;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

const TEST_DELEGATE: &str = "test-delegate-attested";
const CIPHER: [u8; 32] = [7u8; 32];
const NONCE: [u8; 24] = [9u8; 24];
const AUTH_TOKEN_INVALID_CLOSE_CODE: u16 = 4401;

#[derive(Debug, Serialize, Deserialize)]
enum InboundAppMessage {
    CheckAttested,
}

#[derive(Debug, Deserialize)]
enum OutboundAppMessage {
    Attested(Option<Vec<u8>>),
}

fn node_config(
    dir: &Path,
    ws_port: u16,
    network_port: Option<u16>,
    keypair_path: &Path,
) -> freenet::config::ConfigArgs {
    freenet::config::ConfigArgs {
        ws_api: freenet::config::WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(ws_port),
            ..Default::default()
        },
        network_api: freenet::config::NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: network_port,
            // A gateway must name a public network port; a local-mode node
            // (no network port) is not one.
            is_gateway: network_port.is_some(),
            skip_load_from_network: true,
            gateways: Some(vec![]),
            location: Some(0.5),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port,
            ..Default::default()
        },
        config_paths: freenet::config::ConfigPathsArgs {
            config_dir: Some(dir.to_path_buf()),
            data_dir: Some(dir.to_path_buf()),
            log_dir: Some(dir.to_path_buf()),
        },
        secrets: freenet::config::SecretArgs {
            transport_keypair: Some(keypair_path.to_path_buf()),
            ..Default::default()
        },
        mode: Some(freenet::local_node::OperationMode::Local),
        ..Default::default()
    }
}

fn reserve_port() -> anyhow::Result<u16> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

async fn wait_ws_ready(port: u16, within: Duration) -> anyhow::Result<()> {
    let url = format!("ws://127.0.0.1:{port}/v1/contract/command?encodingProtocol=native");
    let deadline = Instant::now() + within;
    loop {
        match connect_async(&url).await {
            Ok((stream, _)) => {
                drop(stream);
                return Ok(());
            }
            Err(e) => {
                if Instant::now() >= deadline {
                    anyhow::bail!("WS API on port {port} did not come up within {within:?}: {e}");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect_with_token(port: u16, token: &str) -> anyhow::Result<Ws> {
    let url = format!("ws://127.0.0.1:{port}/v1/contract/command?encodingProtocol=native");
    let mut request = url.as_str().into_client_request()?;
    request
        .headers_mut()
        .insert("Authorization", format!("Bearer {token}").parse()?);
    Ok(connect_async(request).await?.0)
}

/// Read frames until the server closes; returns the close code and reason.
async fn close_frame(ws: &mut Ws) -> anyhow::Result<Option<(u16, String)>> {
    timeout(Duration::from_secs(10), async {
        while let Some(frame) = ws.next().await {
            match frame? {
                Message::Close(Some(cf)) => {
                    return Ok(Some((u16::from(cf.code), cf.reason.to_string())));
                }
                Message::Close(None) => return Ok(None),
                _ => continue,
            }
        }
        Ok(None)
    })
    .await?
}

async fn close_code(ws: &mut Ws) -> anyhow::Result<Option<u16>> {
    Ok(close_frame(ws).await?.map(|(code, _)| code))
}

/// Register the delegate if asked, then ask it to echo the `MessageOrigin` it
/// was handed on a connection opened with `token`.
async fn attested_origin(
    port: u16,
    token: &str,
    delegate: &DelegateContainer,
    register: bool,
) -> anyhow::Result<Option<Vec<u8>>> {
    let mut client = WebApi::start(connect_with_token(port, token).await?);
    let delegate_key = delegate.key().clone();
    if register {
        client
            .send(ClientRequest::DelegateOp(
                DelegateRequest::RegisterDelegate {
                    delegate: delegate.clone(),
                    cipher: CIPHER,
                    nonce: NONCE,
                },
            ))
            .await?;
        match timeout(Duration::from_secs(20), client.recv()).await?? {
            HostResponse::DelegateResponse { key, .. } => {
                anyhow::ensure!(key == delegate_key, "register returned the wrong key");
            }
            other => anyhow::bail!("expected DelegateResponse on register, got {other:?}"),
        }
    }
    client
        .send(ClientRequest::DelegateOp(
            DelegateRequest::ApplicationMessages {
                key: delegate_key,
                params: Parameters::from(vec![]),
                inbound: vec![InboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(bincode::serialize(&InboundAppMessage::CheckAttested)?),
                )],
            },
        ))
        .await?;
    match timeout(Duration::from_secs(20), client.recv()).await?? {
        HostResponse::DelegateResponse { values, .. } => {
            let app_msg = match values.first() {
                Some(OutboundDelegateMsg::ApplicationMessage(m)) => m,
                other => anyhow::bail!("expected ApplicationMessage, got {other:?}"),
            };
            match bincode::deserialize::<OutboundAppMessage>(&app_msg.payload)? {
                OutboundAppMessage::Attested(bytes) => Ok(bytes),
            }
        }
        other => anyhow::bail!("expected DelegateResponse, got {other:?}"),
    }
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn native_app_grant_end_to_end() -> TestResult {
    let data_dir = tempfile::tempdir()?;
    let data_path = data_dir.path().to_path_buf();
    let key = freenet::dev_tool::TransportKeypair::new();
    let keypair_path = data_path.join("private.pem");
    key.save(&keypair_path)?;
    key.public().save(data_path.join("public.pem"))?;

    let ws_port = reserve_port()?;
    let net_port = reserve_port()?;
    let cfg = node_config(&data_path, ws_port, Some(net_port), &keypair_path)
        .build()
        .await?;
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, ws_port)))?;
    let clients = serve_client_api_with_listener(cfg.ws_api.clone(), listener).await?;
    let node = NodeConfig::new(cfg.clone()).await?.build(clients).await?;
    let shutdown = node.shutdown_handle();
    let run = tokio::spawn(async move { node.run().await });
    wait_ws_ready(ws_port, Duration::from_secs(60)).await?;

    grant_flow(ws_port).await?;

    shutdown.shutdown().await;
    if timeout(Duration::from_secs(30), run).await.is_err() {
        tracing::info!("node run loop did not exit within 30s of shutdown (cleanup only)");
    }
    Ok(())
}

/// Start a node the way `freenet local` starts one (an
/// `Executor::from_config_local` driven by `run_local_node`), on a client-API
/// listener bound here first, so the port cannot be taken in between and the
/// readiness check can only reach this node. Retries opening the store while a
/// previous node on the same data dir is still releasing it.
async fn start_local_node(
    data_path: &Path,
    keypair_path: &Path,
) -> anyhow::Result<(u16, tokio::task::JoinHandle<anyhow::Result<()>>)> {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let ws_port = listener.local_addr()?.port();
    let deadline = Instant::now() + Duration::from_secs(30);
    let (executor, socket) = loop {
        let cfg = node_config(data_path, ws_port, None, keypair_path)
            .build()
            .await?;
        let socket = cfg.ws_api.clone();
        match freenet::local_node::Executor::from_config_local(std::sync::Arc::new(cfg)).await {
            Ok(executor) => break (executor, socket),
            Err(e) if Instant::now() < deadline => {
                tracing::info!(error = %e, "store not released yet; retrying");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(e),
        }
    };
    let run = tokio::spawn(async move {
        freenet::run_local_node_with_listener(executor, socket, listener).await
    });
    wait_ws_ready(ws_port, Duration::from_secs(60)).await?;
    Ok((ws_port, run))
}

/// The same flow on a local-mode node, which has no `OpManager`: grants must
/// work there too. Then a grant collected on that node still works after the
/// node is stopped and a new one is started on the same data directory, which
/// shows the grant is kept in the redb store and not in memory.
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn native_app_grant_end_to_end_local_mode() -> TestResult {
    let data_dir = tempfile::tempdir()?;
    let data_path = data_dir.path().to_path_buf();
    let key = freenet::dev_tool::TransportKeypair::new();
    let keypair_path = data_path.join("private.pem");
    key.save(&keypair_path)?;
    key.public().save(data_path.join("public.pem"))?;

    let (ws_port, run) = start_local_node(&data_path, &keypair_path).await?;
    grant_flow(ws_port).await?;

    // A second grant, kept across a restart.
    let app = ContractInstanceId::new([42u8; 32]);
    let token = obtain_grant(ws_port, app).await?;
    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;
    expect_attested(ws_port, &token, &delegate, false, app).await?;

    run.abort();
    drop(run.await);

    let (ws_port, run) = start_local_node(&data_path, &keypair_path).await?;
    expect_attested(ws_port, &token, &delegate, true, app).await?;
    run.abort();
    drop(run.await);
    Ok(())
}

/// Request, approve (as the page's script does) and collect a grant for `app`.
async fn obtain_grant(ws_port: u16, app: ContractInstanceId) -> anyhow::Result<String> {
    let base = format!("http://127.0.0.1:{ws_port}");
    let http = reqwest::Client::new();
    let created: serde_json::Value = http
        .post(format!("{base}/v1/app-grants/request"))
        .json(&serde_json::json!({ "app": app.to_string(), "client_name": "e2e client" }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let request_id = created["request_id"].as_str().unwrap().to_string();
    let approve_url = created["approve_url"].as_str().unwrap().to_string();
    let page = http
        .get(&approve_url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let start = page.find("data-nonce=\"").expect("page nonce") + "data-nonce=\"".len();
    let nonce = &page[start..start + 64];
    let resp = http
        .post(&approve_url)
        .header("Origin", &base)
        .json(&serde_json::json!({ "nonce": nonce, "allow": true }))
        .send()
        .await?;
    anyhow::ensure!(resp.status() == 200, "approve failed: {}", resp.status());
    let polled: serde_json::Value = http
        .get(format!("{base}/v1/app-grants/request/{request_id}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    anyhow::ensure!(polled["status"] == "approved", "{polled}");
    Ok(polled["token"].as_str().unwrap().to_string())
}

/// A connection opened with `token` is attested as `app` by a real delegate.
async fn expect_attested(
    ws_port: u16,
    token: &str,
    delegate: &DelegateContainer,
    register: bool,
    app: ContractInstanceId,
) -> TestResult {
    let attested = attested_origin(ws_port, token, delegate, register)
        .await?
        .expect("a grant-token connection must receive an attested identity");
    match bincode::deserialize::<MessageOrigin>(&attested)? {
        MessageOrigin::WebApp(id) => assert_eq!(id, app, "wrong contract attested"),
        other => panic!("expected MessageOrigin::WebApp, got {other:?}"),
    }
    Ok(())
}

/// Request, approve and collect a grant; connect with it and have a real
/// delegate echo the attested origin; check the query-string form is refused;
/// revoke while a connection is open; check the revoked token is refused.
async fn grant_flow(ws_port: u16) -> TestResult {
    let base = format!("http://127.0.0.1:{ws_port}");
    let http = reqwest::Client::new();
    let app = ContractInstanceId::new([42u8; 32]);

    // 1. The native client asks. (`reqwest` sends no Origin.)
    let created: serde_json::Value = http
        .post(format!("{base}/v1/app-grants/request"))
        .json(&serde_json::json!({ "app": app.to_string(), "client_name": "e2e client" }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let request_id = created["request_id"].as_str().unwrap().to_string();
    let approve_url = created["approve_url"].as_str().unwrap().to_string();

    // 2. The user opens the page and clicks Allow (as the page's script does).
    let page = http
        .get(&approve_url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    assert!(page.contains(&app.to_string()), "the page names the app");
    let start = page.find("data-nonce=\"").expect("page nonce") + "data-nonce=\"".len();
    let nonce = &page[start..start + 64];
    let resp = http
        .post(&approve_url)
        .header("Origin", &base)
        .json(&serde_json::json!({ "nonce": nonce, "allow": true }))
        .send()
        .await?;
    assert_eq!(resp.status(), 200, "{}", resp.text().await?);

    // 3. The client collects the token. A repeated poll (a lost response)
    // returns the same token until the token is first used.
    let poll_url = format!("{base}/v1/app-grants/request/{request_id}");
    let mut tokens = Vec::new();
    for _ in 0..2 {
        let polled: serde_json::Value = http
            .get(&poll_url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(polled["status"], "approved");
        tokens.push(polled["token"].as_str().unwrap().to_string());
    }
    assert_eq!(tokens[0], tokens[1], "re-collection returns the same token");
    let token = tokens.remove(0);

    // 4. A connection with the token is attested as the app.
    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;
    let attested = attested_origin(ws_port, &token, &delegate, true)
        .await?
        .expect("a grant-token connection must receive an attested identity");
    match bincode::deserialize::<MessageOrigin>(&attested)? {
        MessageOrigin::WebApp(id) => assert_eq!(id, app, "wrong contract attested"),
        other => panic!("expected MessageOrigin::WebApp, got {other:?}"),
    }
    // Used once, the token can no longer be collected.
    assert_eq!(http.get(&poll_url).send().await?.status(), 404);

    // The same token in the query string is not honoured.
    let url = format!(
        "ws://127.0.0.1:{ws_port}/v1/contract/command?encodingProtocol=native&authToken={token}"
    );
    let (mut via_query, _) = connect_async(&url).await?;
    assert_eq!(
        close_code(&mut via_query).await?,
        Some(AUTH_TOKEN_INVALID_CLOSE_CODE)
    );

    // 5. Revoke from the Apps and permissions page while a connection is open.
    let mut held = connect_with_token(ws_port, &token).await?;
    // Prove the connection is fully set up (and so registered under the grant)
    // before revoking: the client sees the upgrade complete before the node has
    // looked the token up. A request answered on it has passed that point.
    let probe = ClientRequest::DelegateOp(DelegateRequest::ApplicationMessages {
        key: delegate.key().clone(),
        params: Parameters::from(vec![]),
        inbound: vec![InboundDelegateMsg::ApplicationMessage(
            ApplicationMessage::new(bincode::serialize(&InboundAppMessage::CheckAttested)?),
        )],
    });
    held.send(Message::Binary(bincode::serialize(&probe)?.into()))
        .await?;
    timeout(Duration::from_secs(20), async {
        while let Some(frame) = held.next().await {
            if let Message::Binary(_) = frame? {
                return anyhow::Ok(());
            }
        }
        anyhow::bail!("the held connection closed before answering")
    })
    .await??;
    let list: serde_json::Value = http
        .get(format!("{base}/permission/native-grants"))
        .send()
        .await?
        .json()
        .await?;
    assert!(
        !list.to_string().contains(&token),
        "the list never shows the token"
    );
    let id = list["grants"][0]["id"].as_str().unwrap().to_string();
    let resp = http
        .post(format!("{base}/permission/native-grants/revoke"))
        .header("Origin", &base)
        .json(&serde_json::json!({ "id": id }))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    // The reason tells the revocation path apart from the invalid-token path,
    // which uses the same code.
    assert_eq!(
        close_frame(&mut held).await?,
        Some((
            AUTH_TOKEN_INVALID_CLOSE_CODE,
            "app grant revoked".to_string()
        )),
        "the live connection is closed by the revocation"
    );

    // 6. The revoked token is refused.
    let mut after = connect_with_token(ws_port, &token).await?;
    assert_eq!(
        close_code(&mut after).await?,
        Some(AUTH_TOKEN_INVALID_CLOSE_CODE)
    );
    Ok(())
}
