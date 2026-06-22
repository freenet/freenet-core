//! Live end-to-end integration test for hosted mode (#4381).
//!
//! This boots a REAL single node — full event loop, contract executor, and
//! WASM runtime — with `hosted_mode = true`, and drives it through the real
//! HTTP/WS stack with `tokio-tungstenite` clients. It exercises the actual
//! `connection_info` gate middleware and the per-connection `userToken`
//! threading, not just the store layer, so it proves the feature works as a
//! whole rather than in isolation.
//!
//! What it proves (one delegate, `test-delegate-2`, which stores/reads secrets
//! via `ctx.set_secret`/`ctx.get_secret`):
//!
//! 1. ISOLATION — two hosted connections presenting DISTINCT `userToken`s store
//!    a secret under the SAME delegate+key and each reads back its OWN value.
//!    The per-user namespace (P1 `SecretScope::User`) keeps them apart.
//! 2. NO-TOKEN = LOCAL — a hosted connection with no `userToken` operates on the
//!    `SecretScope::Local` namespace, invisible to (and not seeing) the per-user
//!    secrets above.
//! 3. GATE ENFORCEMENT — a hosted connection presenting a `userToken` but WITHOUT
//!    `X-Forwarded-Proto: https` is rejected at the WS upgrade with 403
//!    (the refuse-plaintext-token invariant).
//! 4. FLAG GATE — on a node with `hosted_mode = false`, the same `userToken`
//!    (with the XFP header) is IGNORED: the connection lands on `Local`, proving
//!    the whole feature is inert unless the flag is on.
//!
//! Determinism: the node is driven on its own task and we wait on real readiness
//! (the WS API accepting a connection, each response arriving) via bounded
//! `connect`/`recv` loops with clear failure messages — no fixed sleeps gate
//! correctness.
//!
//! Binds a single loopback address (`127.0.0.1`), so the gate sees a loopback
//! source automatically and the test runs on macOS as well as Linux CI.

// The response/message matches below intentionally use a `_`/`other` arm to
// treat ANY unexpected `HostResponse` / `OutboundDelegateMsg` /
// `OutboundAppMessage` variant as a test failure — listing every variant would
// be brittle noise that adds no signal in a test. (CI's `cargo clippy` does not
// lint integration-test targets; this allow keeps `--tests`/local runs clean.)
#![allow(clippy::wildcard_enum_match_arm)]

use std::{
    net::{Ipv4Addr, TcpListener},
    path::Path,
    time::{Duration, Instant},
};

use freenet::{local_node::NodeConfig, server::serve_client_api, test_utils::load_delegate};
use freenet_stdlib::{
    client_api::{ClientRequest, DelegateRequest, HostResponse, WebApi},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, client::IntoClientRequest},
};
use tracing::info;

/// The secret-capable delegate fixture (`tests/test-delegate-2`). Its
/// `StoreSecret { key, value }` calls `ctx.set_secret`, and `GetNonExistentSecret(key)`
/// calls `ctx.get_secret` and returns the value (despite the name, it returns
/// whatever is present — `None` only when truly absent). Both go through the
/// secrets store under the connection's secret scope, so they are exactly the
/// surface that per-user isolation governs.
const TEST_DELEGATE: &str = "test-delegate-2";

/// Arbitrary 32-byte XChaCha20-Poly1305 key for `RegisterDelegate`. Ignored
/// server-side since #4140 (the DEK is HKDF-derived from the node KEK), kept
/// only because the wire variant still carries the field.
const TEST_DELEGATE_CIPHER: [u8; 32] = [
    0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
    0x0f, 0x1e, 0x2d, 0x3c, 0x4b, 0x5a, 0x69, 0x78, 0x87, 0x96, 0xa5, 0xb4, 0xc3, 0xd2, 0xe1, 0xf0,
];
/// Arbitrary 24-byte registration nonce; also ignored for encryption since #4143.
const TEST_DELEGATE_NONCE: [u8; 24] = [0u8; 24];

// ---- The application-message contract of `test-delegate-2`. ----
// Mirrors the `InboundAppMessage` / `OutboundAppMessage` enums in
// `tests/test-delegate-2/src/lib.rs`. We re-declare (rather than import) so the
// test owns the wire contract it asserts against — the variant ORDER must match
// the fixture exactly, since bincode keys on the discriminant.

// Only `StoreSecret` and `GetNonExistentSecret` are constructed by this test;
// the rest exist solely to keep the discriminant numbering aligned with the
// fixture's `InboundAppMessage` so bincode-encoded tags match.
#[allow(dead_code)]
#[derive(Debug, Serialize)]
enum InboundAppMessage {
    CreateInboxRequest,
    PleaseSignMessage(Vec<u8>),
    WriteContext(Vec<u8>),
    ReadContext,
    ClearContext,
    IncrementCounter,
    HasSecret(Vec<u8>),
    GetNonExistentSecret(Vec<u8>),
    StoreSecret { key: Vec<u8>, value: Vec<u8> },
    RemoveSecret(Vec<u8>),
    WriteLargeContext(usize),
    StoreLargeSecret { key: Vec<u8>, size: usize },
}

// The payloads of most variants are never read by this test (we only assert on
// `SecretResult` and `SecretStored`); they exist solely so the discriminant
// numbering matches the fixture's `OutboundAppMessage` for bincode decoding.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
enum OutboundAppMessage {
    CreateInboxResponse(Vec<u8>),
    MessageSigned(Vec<u8>),
    ContextData(Vec<u8>),
    CounterValue(u32),
    SecretExists(bool),
    /// Result of getting a secret (`None` if not found). This is the variant we
    /// assert on for the round-trip and isolation checks.
    SecretResult(Option<Vec<u8>>),
    ContextWritten,
    ContextCleared,
    SecretStored,
    SecretRemoved,
    LargeContextWritten(usize),
    LargeSecretStored(usize),
    SecretStoreFailed,
}

/// Reserve an ephemeral TCP port by binding then freeing it.
fn reserve_port() -> anyhow::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

/// Build a single self-contained gateway node config rooted at `dir`, serving
/// its WS API on `ws_port`. `hosted_mode` is the knob under test.
fn node_config(
    dir: &Path,
    ws_port: u16,
    network_port: u16,
    keypair_path: &Path,
    hosted_mode: bool,
) -> freenet::config::ConfigArgs {
    freenet::config::ConfigArgs {
        ws_api: freenet::config::WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(ws_port),
            // THE flag under test. `Some(true)` => hosted; `Some(false)` =>
            // single-user, userToken ignored entirely.
            hosted_mode: Some(hosted_mode),
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
        ..Default::default()
    }
}

/// A running hosted-mode node with a real executor + WASM runtime, plus the
/// pieces a test needs to talk to it and tear it down.
struct TestNode {
    ws_port: u16,
    shutdown: freenet::ShutdownHandle,
    // `Node::run` never returns `Ok` — it drives the event loop until it exits
    // with an error (e.g. the shutdown cause), hence `Infallible` in the Ok arm.
    run: tokio::task::JoinHandle<Result<std::convert::Infallible, anyhow::Error>>,
    // Held only to keep the temp dir alive for the node's lifetime.
    _data_dir: tempfile::TempDir,
}

impl TestNode {
    /// Boot a node and wait until its WS API is accepting connections.
    async fn start(hosted_mode: bool) -> anyhow::Result<Self> {
        let data_dir = tempfile::tempdir()?;
        let data_path = data_dir.path().to_path_buf();

        let key = freenet::dev_tool::TransportKeypair::new();
        let keypair_path = data_path.join("private.pem");
        key.save(&keypair_path)?;
        key.public().save(data_path.join("public.pem"))?;

        let ws_port = reserve_port()?;
        let net_port = reserve_port()?;

        let cfg = node_config(&data_path, ws_port, net_port, &keypair_path, hosted_mode)
            .build()
            .await?;
        let node = NodeConfig::new(cfg.clone())
            .await?
            .build(serve_client_api(cfg.ws_api.clone()).await?)
            .await?;
        let shutdown = node.shutdown_handle();
        let run = tokio::spawn(async move { node.run().await });

        // Wait for real readiness: the WS API accepting a TCP+WS upgrade. This
        // is what makes the test deterministic without a fixed sleep — if the
        // node never comes up we fail with a clear message instead of hanging.
        wait_ws_ready(ws_port, Duration::from_secs(30)).await?;

        Ok(Self {
            ws_port,
            shutdown,
            run,
            _data_dir: data_dir,
        })
    }

    /// Graceful shutdown; best-effort wait for the run loop to exit.
    async fn shutdown(self) {
        self.shutdown.shutdown().await;
        if timeout(Duration::from_secs(30), self.run).await.is_err() {
            info!("node run loop did not exit within 30s of shutdown (cleanup only)");
        }
    }
}

/// Poll the plain WS endpoint (no token) until it accepts a connection or the
/// deadline passes. Used only as a readiness probe, then dropped.
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

/// Build the WS upgrade request URL with the native encoding protocol and an
/// optional `userToken` query parameter.
fn ws_url(port: u16, user_token: Option<&str>) -> String {
    let base = format!("ws://127.0.0.1:{port}/v1/contract/command?encodingProtocol=native");
    match user_token {
        Some(t) => format!("{base}&userToken={t}"),
        None => base,
    }
}

/// Connect a hosted-mode WS client: `userToken` in the URL query and (when
/// `xfp_https`) the `X-Forwarded-Proto: https` handshake header the gate
/// requires. Loopback source is automatic (we connect to 127.0.0.1).
async fn connect_hosted(
    port: u16,
    user_token: Option<&str>,
    xfp_https: bool,
) -> anyhow::Result<WebApi> {
    let url = ws_url(port, user_token);
    let mut request = url.as_str().into_client_request()?;
    if xfp_https {
        request
            .headers_mut()
            .insert("X-Forwarded-Proto", "https".parse()?);
    }
    let (stream, _) = connect_async(request).await?;
    Ok(WebApi::start(stream))
}

/// Register the delegate over `client` and confirm the node acknowledges it.
async fn register_delegate(
    client: &mut WebApi,
    delegate: &DelegateContainer,
    delegate_key: &DelegateKey,
) -> anyhow::Result<()> {
    client
        .send(ClientRequest::DelegateOp(
            DelegateRequest::RegisterDelegate {
                delegate: delegate.clone(),
                cipher: TEST_DELEGATE_CIPHER,
                nonce: TEST_DELEGATE_NONCE,
            },
        ))
        .await?;
    let resp = timeout(Duration::from_secs(15), client.recv()).await??;
    match resp {
        HostResponse::DelegateResponse { key, .. } => {
            anyhow::ensure!(
                &key == delegate_key,
                "register acked a different delegate key: {key}"
            );
            Ok(())
        }
        other => anyhow::bail!("unexpected response to RegisterDelegate: {other:?}"),
    }
}

/// Send one application message to the delegate and decode the single
/// `OutboundAppMessage` it returns.
async fn delegate_roundtrip(
    client: &mut WebApi,
    delegate_key: &DelegateKey,
    msg: &InboundAppMessage,
) -> anyhow::Result<OutboundAppMessage> {
    let payload = bincode::serialize(msg)?;
    let app_msg = ApplicationMessage::new(payload);
    client
        .send(ClientRequest::DelegateOp(
            DelegateRequest::ApplicationMessages {
                key: delegate_key.clone(),
                params: Parameters::from(vec![]),
                inbound: vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
            },
        ))
        .await?;

    let resp = timeout(Duration::from_secs(15), client.recv()).await??;
    match resp {
        HostResponse::DelegateResponse { key, values } => {
            anyhow::ensure!(
                &key == delegate_key,
                "delegate response carried a different key: {key}"
            );
            anyhow::ensure!(!values.is_empty(), "delegate returned no output messages");
            let out = match &values[0] {
                OutboundDelegateMsg::ApplicationMessage(m) => m,
                other => anyhow::bail!("expected ApplicationMessage, got {other:?}"),
            };
            anyhow::ensure!(out.processed, "delegate did not mark message processed");
            Ok(bincode::deserialize(&out.payload)?)
        }
        other => anyhow::bail!("unexpected response to ApplicationMessages: {other:?}"),
    }
}

/// `StoreSecret { key, value }` → assert `SecretStored`.
async fn store_secret(
    client: &mut WebApi,
    delegate_key: &DelegateKey,
    key: &[u8],
    value: &[u8],
) -> anyhow::Result<()> {
    let out = delegate_roundtrip(
        client,
        delegate_key,
        &InboundAppMessage::StoreSecret {
            key: key.to_vec(),
            value: value.to_vec(),
        },
    )
    .await?;
    match out {
        OutboundAppMessage::SecretStored => Ok(()),
        other => anyhow::bail!("expected SecretStored, got {other:?}"),
    }
}

/// `GetNonExistentSecret(key)` → return the stored value, or `None` if absent.
/// (The fixture name is a misnomer: it returns `ctx.get_secret(key)` verbatim.)
async fn get_secret(
    client: &mut WebApi,
    delegate_key: &DelegateKey,
    key: &[u8],
) -> anyhow::Result<Option<Vec<u8>>> {
    let out = delegate_roundtrip(
        client,
        delegate_key,
        &InboundAppMessage::GetNonExistentSecret(key.to_vec()),
    )
    .await?;
    match out {
        OutboundAppMessage::SecretResult(v) => Ok(v),
        other => anyhow::bail!("expected SecretResult, got {other:?}"),
    }
}

/// Headline test: per-user isolation + no-token=Local, all through the live
/// WS stack against a real executor.
///
/// Token A stores VA, token B stores VB, the no-token (Local) connection stores
/// VL — all under the SAME delegate and SAME secret key. Then each reads back
/// and must see only its own value, and never another scope's value.
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn hosted_mode_per_user_secret_isolation() -> anyhow::Result<()> {
    let node = TestNode::start(/* hosted_mode */ true).await?;
    let port = node.ws_port;

    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;
    let delegate_key = delegate.key().clone();

    const TOKEN_A: &str = "user-token-aaaaaaaaaaaaaaaaaaaaaaaa";
    const TOKEN_B: &str = "user-token-bbbbbbbbbbbbbbbbbbbbbbbb";
    const SECRET_KEY: &[u8] = b"shared-secret-key";
    const VALUE_A: &[u8] = b"value-belonging-to-user-A";
    const VALUE_B: &[u8] = b"value-belonging-to-user-B";
    const VALUE_LOCAL: &[u8] = b"value-on-the-local-namespace";

    // --- Three connections: user A, user B, and a no-token (Local) one. ---
    let mut conn_a = connect_hosted(port, Some(TOKEN_A), true).await?;
    let mut conn_b = connect_hosted(port, Some(TOKEN_B), true).await?;
    let mut conn_local = connect_hosted(port, None, true).await?;

    // RegisterDelegate is keyed by delegate (not by connection) and is
    // idempotent server-side, so registering on each connection is fine.
    register_delegate(&mut conn_a, &delegate, &delegate_key).await?;
    register_delegate(&mut conn_b, &delegate, &delegate_key).await?;
    register_delegate(&mut conn_local, &delegate, &delegate_key).await?;

    // --- Each scope writes a DIFFERENT value under the SAME (delegate, key). ---
    store_secret(&mut conn_a, &delegate_key, SECRET_KEY, VALUE_A).await?;
    store_secret(&mut conn_b, &delegate_key, SECRET_KEY, VALUE_B).await?;
    store_secret(&mut conn_local, &delegate_key, SECRET_KEY, VALUE_LOCAL).await?;

    // --- Each scope reads back its OWN value (isolation). ---
    let read_a = get_secret(&mut conn_a, &delegate_key, SECRET_KEY).await?;
    let read_b = get_secret(&mut conn_b, &delegate_key, SECRET_KEY).await?;
    let read_local = get_secret(&mut conn_local, &delegate_key, SECRET_KEY).await?;

    assert_eq!(
        read_a.as_deref(),
        Some(VALUE_A),
        "user A must read back A's value, not B's or Local's"
    );
    assert_eq!(
        read_b.as_deref(),
        Some(VALUE_B),
        "user B must read back B's value, not A's or Local's"
    );
    assert_eq!(
        read_local.as_deref(),
        Some(VALUE_LOCAL),
        "the no-token (Local) connection must read back the Local value"
    );

    // Cross-checks: the three values are genuinely distinct, so the equalities
    // above prove non-overlap rather than coincidental agreement.
    assert_ne!(read_a, read_b, "A and B must not share a namespace");
    assert_ne!(read_a, read_local, "A and Local must not share a namespace");
    assert_ne!(read_b, read_local, "B and Local must not share a namespace");

    // A fresh connection re-presenting TOKEN_A must see A's value — the per-user
    // namespace is durable across connections, keyed only by the token.
    let mut conn_a2 = connect_hosted(port, Some(TOKEN_A), true).await?;
    register_delegate(&mut conn_a2, &delegate, &delegate_key).await?;
    let read_a2 = get_secret(&mut conn_a2, &delegate_key, SECRET_KEY).await?;
    assert_eq!(
        read_a2.as_deref(),
        Some(VALUE_A),
        "reconnecting with TOKEN_A must see A's durable per-user secret"
    );

    drop((conn_a, conn_b, conn_local, conn_a2));
    node.shutdown().await;
    Ok(())
}

/// Gate enforcement: a hosted connection presenting a `userToken` but WITHOUT
/// `X-Forwarded-Proto: https` must be rejected at the WS upgrade with 403
/// (the refuse-plaintext-token invariant). Loopback alone is not sufficient.
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn hosted_mode_rejects_token_without_https() -> anyhow::Result<()> {
    let node = TestNode::start(/* hosted_mode */ true).await?;
    let port = node.ws_port;

    // Token present, but NO X-Forwarded-Proto header → RejectInsecure (403).
    let url = ws_url(port, Some("some-user-token-value"));
    let request = url.as_str().into_client_request()?;
    let result = connect_async(request).await;

    match result {
        Ok(_) => panic!(
            "hosted connection with a userToken but no X-Forwarded-Proto: https \
             was accepted; the refuse-plaintext-token gate did not fire"
        ),
        Err(tungstenite::Error::Http(resp)) => {
            assert_eq!(
                resp.status(),
                tungstenite::http::StatusCode::FORBIDDEN,
                "expected 403 Forbidden from the gate, got {}",
                resp.status()
            );
            info!("gate correctly rejected plaintext-token upgrade with 403");
        }
        Err(other) => {
            anyhow::bail!("expected an HTTP 403 from the gate, got a different error: {other:?}")
        }
    }

    // Sanity: the SAME token WITH the XFP header is accepted (the only
    // difference is the header the gate keys on), so the rejection above is the
    // gate firing on the missing header, not a broken endpoint.
    let mut ok = connect_hosted(port, Some("some-user-token-value"), true).await?;
    ok.send(ClientRequest::Disconnect { cause: None })
        .await
        .ok();
    drop(ok);

    node.shutdown().await;
    Ok(())
}

/// Flag gate: with `hosted_mode = false`, the same `userToken` (+ XFP header) is
/// IGNORED — the connection lands on the `Local` namespace, sharing state with a
/// no-token connection. Proves the feature is inert unless the flag is on.
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn flag_off_ignores_user_token_and_uses_local() -> anyhow::Result<()> {
    let node = TestNode::start(/* hosted_mode */ false).await?;
    let port = node.ws_port;

    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;
    let delegate_key = delegate.key().clone();

    const SECRET_KEY: &[u8] = b"flag-off-key";
    const VALUE: &[u8] = b"written-by-the-tokened-connection";

    // Connection WITH a userToken (+XFP), but the node's flag is OFF. The token
    // must be ignored, so this writes to Local.
    let mut conn_token = connect_hosted(port, Some("ignored-because-flag-off"), true).await?;
    register_delegate(&mut conn_token, &delegate, &delegate_key).await?;
    store_secret(&mut conn_token, &delegate_key, SECRET_KEY, VALUE).await?;

    // A no-token connection (also Local) must SEE that value — i.e. the tokened
    // write did NOT get its own per-user namespace.
    let mut conn_plain = connect_hosted(port, None, true).await?;
    register_delegate(&mut conn_plain, &delegate, &delegate_key).await?;
    let read = get_secret(&mut conn_plain, &delegate_key, SECRET_KEY).await?;
    assert_eq!(
        read.as_deref(),
        Some(VALUE),
        "with hosted_mode=false the userToken must be ignored: the tokened write \
         should be visible on the shared Local namespace"
    );

    drop((conn_token, conn_plain));
    node.shutdown().await;
    Ok(())
}
