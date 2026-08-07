//! Live end-to-end verification of the connection-scope gate on attested
//! application identity (GHSA-824h-7x5x-wfmf).
//!
//! This boots a REAL node — full event loop, contract executor, WASM runtime —
//! with its client API bound to `0.0.0.0`, and drives it through the real
//! HTTP/WS stack with `tokio-tungstenite`. The delegate is real WASM
//! (`test-delegate-attested`), and it echoes back the `MessageOrigin` the
//! runtime handed it. So the assertion is on the identity a delegate ACTUALLY
//! receives, not on an intermediate value.
//!
//! Three deployment shapes, which is the whole point:
//!
//! 1. **DIRECT LOOPBACK** — a browser or CLI on the node's own host. Must still
//!    receive `MessageOrigin::WebApp(..)`. This is the regression risk: the gate
//!    breaking this would break every local app on the node while a health check
//!    of "process up, /v1/version answers" stayed perfectly green.
//! 2. **COLOCATED REVERSE PROXY** — the node sees the proxy's loopback address
//!    even though the human is remote. This is `try.freenet.org`'s topology
//!    (nginx on the same host, upstream `127.0.0.1`). The gate must be a NO-OP
//!    here; isolation there comes from hosted mode's `user_context` instead.
//!    Exercised with a real TCP forwarder listening off-loopback and dialing
//!    `127.0.0.1`, so the node genuinely observes a loopback peer for a
//!    connection that originated off-host.
//! 3. **DIRECT OFF-HOST** — a LAN browser connecting straight to the node. Must
//!    receive NO attested identity.
//!
//! Shapes 2 and 3 need a non-loopback PRIVATE address on this machine (the
//! client API refuses non-private sources outright, before the gate, so a public
//! one proves nothing about it). Discovery probes the default route and honours
//! `FREENET_TEST_OFFHOST_IPV4` for hosts whose default route is public. Where no
//! address is found the two legs are SKIPPED with a loud log rather than
//! silently passing; shape 1 always runs, and the `Remote` classification itself
//! is pinned deterministically by the `ConnectionScope` unit tests in
//! `client_events::types`. Read a skip as "this environment could not exercise
//! it", never as "it passed".

// A `_`/`other` arm treats any unexpected variant as a test failure; listing
// every variant of every non_exhaustive stdlib enum would be brittle noise.
#![allow(clippy::wildcard_enum_match_arm)]

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener},
    path::Path,
    time::{Duration, Instant},
};

use freenet::{
    dev_tool::AuthToken, local_node::NodeConfig,
    server::serve_client_api_with_listener_and_contracts, test_utils::load_delegate,
};
use freenet_stdlib::{
    client_api::{ClientRequest, DelegateRequest, HostResponse, WebApi},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use testresult::TestResult;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};
use tracing::info;

/// The WebSocket handshake itself failed, so the node never saw a request.
///
/// Distinguished from every other error on purpose: "the connection was refused"
/// says nothing about the attestation gate, while "the connection was accepted
/// and then something went wrong" is a real failure the off-host leg must not
/// swallow. See that leg's `Err` arm.
#[derive(Debug)]
struct ConnectFailed(tokio_tungstenite::tungstenite::Error);

impl std::fmt::Display for ConnectFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "websocket connect failed: {}", self.0)
    }
}

impl std::error::Error for ConnectFailed {}

const TEST_DELEGATE: &str = "test-delegate-attested";
const CIPHER: [u8; 32] = [7u8; 32];
const NONCE: [u8; 24] = [9u8; 24];

#[derive(Debug, Serialize, Deserialize)]
enum InboundAppMessage {
    CheckAttested,
}

#[derive(Debug, Deserialize)]
enum OutboundAppMessage {
    Attested(Option<Vec<u8>>),
}

/// A non-loopback **private** IPv4 address of this host, or `None`.
///
/// Private on purpose. The client API already refuses any connection from a
/// non-private source before request handling
/// (`server::private_network_filter`), so a public source address never reaches
/// the attestation gate at all and would prove nothing about it. The realistic
/// off-host attacker is on the LAN, which is exactly an RFC1918 source.
///
/// Uses the connect-a-UDP-socket trick against several RFC1918 targets: no
/// packet is sent, the kernel just resolves which local address each route
/// would use. Works offline and needs no interface enumeration.
fn private_non_loopback_ipv4() -> Option<Ipv4Addr> {
    // Explicit override first. Route probing finds only the address the DEFAULT
    // route uses, so a host whose default route is public (a colocated server
    // with private addresses on secondary interfaces — wireguard, docker) has a
    // usable address that probing cannot reach. Set
    // `FREENET_TEST_OFFHOST_IPV4` to one of its private addresses to exercise
    // shapes 2 and 3 there.
    if let Ok(explicit) = std::env::var("FREENET_TEST_OFFHOST_IPV4") {
        match explicit.parse::<Ipv4Addr>() {
            Ok(ip) if !ip.is_loopback() && !ip.is_unspecified() => return Some(ip),
            other => tracing::warn!(
                ?other,
                "FREENET_TEST_OFFHOST_IPV4 is set but unusable; falling back to route probing"
            ),
        }
    }
    for target in ["10.0.0.1:9", "172.16.0.1:9", "192.168.0.1:9"] {
        let Ok(sock) = std::net::UdpSocket::bind("0.0.0.0:0") else {
            continue;
        };
        if sock.connect(target).is_err() {
            continue;
        }
        if let Ok(SocketAddr::V4(local)) = sock.local_addr() {
            let ip = *local.ip();
            if ip.is_private() && !ip.is_loopback() {
                return Some(ip);
            }
        }
    }
    None
}

fn node_config(
    dir: &Path,
    ws_port: u16,
    network_port: u16,
    keypair_path: &Path,
) -> freenet::config::ConfigArgs {
    freenet::config::ConfigArgs {
        ws_api: freenet::config::WebsocketApiArgs {
            // 0.0.0.0 on purpose: the point of this test is to accept both a
            // loopback and an off-host connection on the SAME listener, so the
            // only thing that differs between the legs is the peer address the
            // kernel records.
            address: Some(Ipv4Addr::UNSPECIFIED.into()),
            ws_api_port: Some(ws_port),
            ..Default::default()
        },
        network_api: freenet::config::NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: Some(network_port),
            is_gateway: true,
            skip_load_from_network: true,
            gateways: Some(vec![]),
            location: Some(0.5),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: Some(network_port),
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

/// Connect to `authority` carrying `token`, register the delegate if asked, then
/// ask it to echo the `MessageOrigin` it was handed.
///
/// Returns the raw echoed bytes: `None` means the delegate saw no attested
/// identity at all.
async fn attested_origin_via(
    authority: &str,
    token: &AuthToken,
    delegate: &DelegateContainer,
    register: bool,
) -> anyhow::Result<Option<Vec<u8>>> {
    let url = format!("ws://{authority}/v1/contract/command?encodingProtocol=native");
    let mut request = url.as_str().into_client_request()?;
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {}", token.as_str()).parse()?,
    );
    let (stream, _) = connect_async(request)
        .await
        .map_err(|e| anyhow::Error::new(ConnectFailed(e)))?;
    let mut client = WebApi::start(stream);
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
                key: delegate_key.clone(),
                params: Parameters::from(vec![]),
                inbound: vec![InboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(bincode::serialize(&InboundAppMessage::CheckAttested)?),
                )],
            },
        ))
        .await?;

    match timeout(Duration::from_secs(20), client.recv()).await?? {
        HostResponse::DelegateResponse { values, .. } => {
            anyhow::ensure!(!values.is_empty(), "delegate produced no output");
            let app_msg = match &values[0] {
                OutboundDelegateMsg::ApplicationMessage(m) => m,
                other => anyhow::bail!("expected ApplicationMessage, got {other:?}"),
            };
            match bincode::deserialize::<OutboundAppMessage>(&app_msg.payload)? {
                OutboundAppMessage::Attested(bytes) => Ok(bytes),
            }
        }
        other => anyhow::bail!("expected DelegateResponse, got {other:?}"),
    }
}

fn expect_webapp(bytes: Option<Vec<u8>>, expected: ContractInstanceId, shape: &str) -> TestResult {
    let bytes = match bytes {
        Some(b) => b,
        None => panic!(
            "{shape}: the delegate received NO attested identity. This shape must keep \
             working — the gate is only meant to withhold attestation from connections \
             the node cannot prove are local."
        ),
    };
    match bincode::deserialize::<MessageOrigin>(&bytes)? {
        MessageOrigin::WebApp(id) => {
            assert_eq!(id, expected, "{shape}: wrong contract attested");
            Ok(())
        }
        other => panic!("{shape}: expected MessageOrigin::WebApp, got {other:?}"),
    }
}

/// A one-shot TCP forwarder listening on `listen` and dialing `upstream`.
///
/// Stands in for a colocated reverse proxy: the node's peer address is the
/// forwarder's loopback address, while the client connected off-host.
fn spawn_forwarder(listen: SocketAddr, upstream: SocketAddr) -> anyhow::Result<()> {
    let listener = std::net::TcpListener::bind(listen)?;
    listener.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(listener)?;
    tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                if let Ok(mut outbound) = tokio::net::TcpStream::connect(upstream).await {
                    if let Err(e) = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await
                    {
                        tracing::debug!(error = %e, "forwarder connection ended");
                    }
                }
            });
        }
    });
    Ok(())
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn attested_origin_survives_loopback_and_proxy_but_not_off_host() -> TestResult {
    let data_dir = tempfile::tempdir()?;
    let data_path = data_dir.path().to_path_buf();
    let key = freenet::dev_tool::TransportKeypair::new();
    let keypair_path = data_path.join("private.pem");
    key.save(&keypair_path)?;
    key.public().save(data_path.join("public.pem"))?;

    let ws_port = reserve_port()?;
    let net_port = reserve_port()?;
    let cfg = node_config(&data_path, ws_port, net_port, &keypair_path)
        .build()
        .await?;

    // Bind the client API on 0.0.0.0 ourselves so the same listener serves both
    // the loopback and the off-host leg, and so we get the OriginContractMap
    // back to seed a token→contract mapping (what the HTTP shell page would do).
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, ws_port)))?;
    let (clients, origin_contracts) =
        serve_client_api_with_listener_and_contracts(cfg.ws_api.clone(), listener).await?;

    let node = NodeConfig::new(cfg.clone()).await?.build(clients).await?;
    let shutdown = node.shutdown_handle();
    let run = tokio::spawn(async move { node.run().await });
    wait_ws_ready(ws_port, Duration::from_secs(60)).await?;

    let token = AuthToken::from("origin-scope-e2e-token".to_string());
    let contract_id = ContractInstanceId::new([42u8; 32]);
    origin_contracts.insert(
        token.clone(),
        freenet::server::OriginContract::new(contract_id, freenet::dev_tool::ClientId::FIRST),
    );

    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;

    // SHAPE 1 — direct loopback. Also registers the delegate.
    let direct =
        attested_origin_via(&format!("127.0.0.1:{ws_port}"), &token, &delegate, true).await?;
    expect_webapp(direct, contract_id, "direct loopback")?;
    info!("shape 1 (direct loopback): attested, as required");

    // SHAPE 2 — colocated reverse proxy. Only meaningful if we can listen
    // off-loopback; otherwise the forwarder would be loopback→loopback and would
    // prove nothing about the proxy topology.
    let lan_ip = private_non_loopback_ipv4();
    match lan_ip {
        Some(ip) => {
            let fwd_port = reserve_port()?;
            spawn_forwarder(
                SocketAddr::from((ip, fwd_port)),
                SocketAddr::from((Ipv4Addr::LOCALHOST, ws_port)),
            )?;
            let proxied =
                attested_origin_via(&format!("{ip}:{fwd_port}"), &token, &delegate, false).await?;
            expect_webapp(proxied, contract_id, "colocated reverse proxy")?;
            info!(%ip, "shape 2 (colocated proxy): attested — the gate is a no-op here, as designed");

            // SHAPE 3 — direct off-host. Distinguish two ways of not being
            // attested: the connection established and the gate withheld
            // attestation (what this test is for), versus the connection never
            // establishing because `private_network_filter` refused it first
            // (also safe, but no evidence about the gate). Reporting the second
            // as if it were the first is exactly the "verification that cannot
            // fail" trap.
            match attested_origin_via(&format!("{ip}:{ws_port}"), &token, &delegate, false).await {
                Ok(off_host) => {
                    assert!(
                        off_host.is_none(),
                        "direct off-host connection received an attested identity \
                         ({off_host:?}); a token presented from off-host must attest nothing"
                    );
                    info!(%ip, "shape 3 (direct off-host): connection accepted, NOT attested — the gate held");
                }
                Err(e) if e.downcast_ref::<ConnectFailed>().is_some() => {
                    // The handshake was refused (the pre-existing private-network
                    // filter, or a host firewall), so the node never processed a
                    // request and the gate was not exercised. Safe, but not
                    // evidence — and deliberately NOT reported as a pass.
                    tracing::warn!(
                        %ip, error = %e,
                        "shape 3 INCONCLUSIVE: the off-host connection never established, \
                         so the attestation gate itself was not exercised on this host"
                    );
                }
                Err(e) => {
                    // The connection WAS established and then something went
                    // wrong. Never treat this as inconclusive: a regression where
                    // an off-host request errors instead of resolving to no
                    // attestation would otherwise log a warning and pass.
                    panic!(
                        "shape 3 (direct off-host) connected but then failed: {e}. \
                         The gate must resolve a non-local caller to NO attested \
                         origin, not error."
                    );
                }
            }
        }
        None => {
            // In CI this is a HARD FAILURE. These two legs are the most valuable
            // assertions in the PR, and a silent environment-dependent skip is
            // exactly the "verification that cannot fail" shape: the suite would
            // stay green while the thing it exists to check never ran.
            //
            // Locally it stays a warning, so a developer on a loopback-only
            // machine is not blocked. Set `FREENET_TEST_OFFHOST_IPV4` to any
            // private address of the host to run them.
            assert!(
                std::env::var_os("CI").is_none(),
                "shapes 2 and 3 could not run: no non-loopback PRIVATE IPv4 address was \
                 found on this CI host. These legs are the point of this test, so a skip \
                 here is a failure, not a pass. Set FREENET_TEST_OFFHOST_IPV4 in the \
                 workflow to a private address of the runner."
            );
            tracing::warn!(
                "SKIPPED shapes 2 and 3: no non-loopback private IPv4 address on this host, \
                 so the proxy and off-host topologies cannot be exercised here. The Remote \
                 classification itself is pinned by client_events::types unit tests."
            );
        }
    }

    shutdown.shutdown().await;
    if timeout(Duration::from_secs(30), run).await.is_err() {
        info!("node run loop did not exit within 30s of shutdown (cleanup only)");
    }
    Ok(())
}
