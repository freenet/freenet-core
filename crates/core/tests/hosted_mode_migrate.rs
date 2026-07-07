//! Live end-to-end integration test for the zero-friction "magic-link" secrets
//! MIGRATION (#4592) — the hosted `mint` + public `pull` endpoints plus the
//! local confirmation PAGE, all on ONE running node.
//!
//! The migration turns the hosted → self-host handoff into a link the user
//! clicks on their own peer. The three cooperating HTTP surfaces exercised here
//! (all served by the same binary — a node can be either side of the handoff):
//!
//!   * `POST /v1/hosted/migrate/mint` — HOSTED side. Gated EXACTLY like the live
//!     export (`GET /v1/hosted/export`): hosted mode ON + loopback source +
//!     `X-Forwarded-Proto: https` + a valid `X-Freenet-User-Token`. It exports
//!     the token-user's secret scope under a FRESH EPHEMERAL key (never the
//!     durable token) and buffers it server-side under a single-use pull token,
//!     which it returns as JSON.
//!   * `GET /v1/hosted/migrate/pull?pt=<token>` — PUBLIC. The single-use pull
//!     token is the entire auth. Returns the raw ephemeral bundle bytes plus the
//!     ephemeral key in the `X-Freenet-Bundle-Key` header. A second pull of the
//!     same token — or an unknown token — is a uniform 404.
//!   * `GET /hosted/import` — the LOCAL confirmation page. HTML with an explicit
//!     import button; it never imports by itself (defeats drive-by / CSRF).
//!
//! The headline test walks the whole round-trip on a single hosted node and
//! proves the load-bearing properties:
//!
//!   1. The durable token authenticates the mint but is NOT the bundle key — the
//!      pulled bundle rides a fresh EPHEMERAL key, and that exact key round-trips
//!      through `POST /v1/import` back to the original plaintext.
//!   2. The pull token is SINGLE-USE: the first pull consumes it, a replay 404s.
//!   3. The migrated secret decrypts to user A's original plaintext when read
//!      back through the live delegate stack.
//!
//! The local `POST /v1/hosted/pull-import` endpoint is NOT driven here: its
//! outbound HTTPS fetch is locked to `https://try.freenet.org` (a strict SSRF
//! allowlist), so it cannot be exercised against a loopback-bound test server.
//! Its gate + allowlist are covered by the `hosted_migrate` unit tests. To prove
//! the pulled bundle is a real, importable bundle we feed it into the existing
//! `POST /v1/import` endpoint directly (the same import `pull-import` would drive
//! internally, with the same key headers).

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
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};
use tracing::info;

const TEST_DELEGATE: &str = "test-delegate-2";

const TEST_DELEGATE_CIPHER: [u8; 32] = [
    0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
    0x0f, 0x1e, 0x2d, 0x3c, 0x4b, 0x5a, 0x69, 0x78, 0x87, 0x96, 0xa5, 0xb4, 0xc3, 0xd2, 0xe1, 0xf0,
];
const TEST_DELEGATE_NONCE: [u8; 24] = [0u8; 24];

// Mirror of the fixture's `InboundAppMessage` / `OutboundAppMessage` (variant
// ORDER must match for bincode discriminants — see hosted_mode_export.rs).
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

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
enum OutboundAppMessage {
    CreateInboxResponse(Vec<u8>),
    MessageSigned(Vec<u8>),
    ContextData(Vec<u8>),
    CounterValue(u32),
    SecretExists(bool),
    SecretResult(Option<Vec<u8>>),
    ContextWritten,
    ContextCleared,
    SecretStored,
    SecretRemoved,
    LargeContextWritten(usize),
    LargeSecretStored(usize),
    SecretStoreFailed,
}

fn reserve_port() -> anyhow::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

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
            hosted_mode: Some(hosted_mode),
            // Disable the per-user op rate limit (#4561) for these tests: the
            // migration mint runs a full export and shares the export rate
            // limit, which is unrelated to what we exercise here.
            per_user_op_rate_limit: Some(0),
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

struct TestNode {
    ws_port: u16,
    shutdown: freenet::ShutdownHandle,
    run: tokio::task::JoinHandle<Result<std::convert::Infallible, anyhow::Error>>,
    _data_dir: tempfile::TempDir,
}

impl TestNode {
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

        wait_ws_ready(ws_port, Duration::from_secs(30)).await?;

        Ok(Self {
            ws_port,
            shutdown,
            run,
            _data_dir: data_dir,
        })
    }

    async fn shutdown(self) {
        self.shutdown.shutdown().await;
        if timeout(Duration::from_secs(30), self.run).await.is_err() {
            info!("node run loop did not exit within 30s of shutdown (cleanup only)");
        }
    }
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

/// Hosted WS connection: `user_token` → `userToken` query param; `xfp_https` →
/// `X-Forwarded-Proto: https`. Honors the per-user token (User scope) only when
/// both are set on a loopback source.
async fn connect_hosted(
    port: u16,
    user_token: Option<&str>,
    xfp_https: bool,
) -> anyhow::Result<WebApi> {
    let base = format!("ws://127.0.0.1:{port}/v1/contract/command?encodingProtocol=native");
    let url = match user_token {
        Some(t) => format!("{base}&userToken={t}"),
        None => base,
    };
    let mut request = url.as_str().into_client_request()?;
    if xfp_https {
        request
            .headers_mut()
            .insert("X-Forwarded-Proto", "https".parse()?);
    }
    let (stream, _) = connect_async(request).await?;
    Ok(WebApi::start(stream))
}

/// Plain WS connection (no user token). On a hosted node this maps to
/// `SecretScope::Local` — the scope `POST /v1/import` writes into — so it is how
/// we read the migrated secret back.
async fn connect_plain(port: u16) -> anyhow::Result<WebApi> {
    let url = format!("ws://127.0.0.1:{port}/v1/contract/command?encodingProtocol=native");
    let request = url.as_str().into_client_request()?;
    let (stream, _) = connect_async(request).await?;
    Ok(WebApi::start(stream))
}

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
                "register acked a different key: {key}"
            );
            Ok(())
        }
        other => anyhow::bail!("unexpected response to RegisterDelegate: {other:?}"),
    }
}

async fn store_secret(
    client: &mut WebApi,
    delegate_key: &DelegateKey,
    key: &[u8],
    value: &[u8],
) -> anyhow::Result<()> {
    let payload = bincode::serialize(&InboundAppMessage::StoreSecret {
        key: key.to_vec(),
        value: value.to_vec(),
    })?;
    client
        .send(ClientRequest::DelegateOp(
            DelegateRequest::ApplicationMessages {
                key: delegate_key.clone(),
                params: Parameters::from(vec![]),
                inbound: vec![InboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(payload),
                )],
            },
        ))
        .await?;
    let resp = timeout(Duration::from_secs(15), client.recv()).await??;
    match resp {
        HostResponse::DelegateResponse { values, .. } => {
            let out = match &values[0] {
                OutboundDelegateMsg::ApplicationMessage(m) => m,
                other => anyhow::bail!("expected ApplicationMessage, got {other:?}"),
            };
            match bincode::deserialize::<OutboundAppMessage>(&out.payload)? {
                OutboundAppMessage::SecretStored => Ok(()),
                other => anyhow::bail!("expected SecretStored, got {other:?}"),
            }
        }
        other => anyhow::bail!("unexpected response to ApplicationMessages: {other:?}"),
    }
}

/// Send a single `InboundAppMessage` to the delegate and decode the single
/// `OutboundAppMessage` reply — the live secret read path.
async fn delegate_app_msg(
    client: &mut WebApi,
    delegate_key: &DelegateKey,
    msg: &InboundAppMessage,
) -> anyhow::Result<OutboundAppMessage> {
    let payload = bincode::serialize(msg)?;
    client
        .send(ClientRequest::DelegateOp(
            DelegateRequest::ApplicationMessages {
                key: delegate_key.clone(),
                params: Parameters::from(vec![]),
                inbound: vec![InboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(payload),
                )],
            },
        ))
        .await?;
    let resp = timeout(Duration::from_secs(15), client.recv()).await??;
    match resp {
        HostResponse::DelegateResponse { values, .. } => {
            let out = match &values[0] {
                OutboundDelegateMsg::ApplicationMessage(m) => m,
                other => anyhow::bail!("expected ApplicationMessage, got {other:?}"),
            };
            Ok(bincode::deserialize::<OutboundAppMessage>(&out.payload)?)
        }
        other => anyhow::bail!("unexpected response to ApplicationMessages: {other:?}"),
    }
}

/// Read a secret VALUE back through the live delegate (decrypts via the running
/// node's `SecretsStore`).
async fn get_secret_via_delegate(
    client: &mut WebApi,
    delegate_key: &DelegateKey,
    key: &[u8],
) -> anyhow::Result<Option<Vec<u8>>> {
    match delegate_app_msg(
        client,
        delegate_key,
        &InboundAppMessage::GetNonExistentSecret(key.to_vec()),
    )
    .await?
    {
        OutboundAppMessage::SecretResult(v) => Ok(v),
        other => anyhow::bail!("expected SecretResult, got {other:?}"),
    }
}

/// Whether a secret exists, through the live delegate.
async fn has_secret_via_delegate(
    client: &mut WebApi,
    delegate_key: &DelegateKey,
    key: &[u8],
) -> anyhow::Result<bool> {
    match delegate_app_msg(
        client,
        delegate_key,
        &InboundAppMessage::HasSecret(key.to_vec()),
    )
    .await?
    {
        OutboundAppMessage::SecretExists(b) => Ok(b),
        other => anyhow::bail!("expected SecretExists, got {other:?}"),
    }
}

/// `POST /v1/hosted/migrate/mint`. `token` → `X-Freenet-User-Token`; `xfp_https`
/// → `X-Forwarded-Proto: https`. Returns the raw response so callers assert on
/// both the secure (200 + JSON) and insecure (403) paths.
async fn http_mint(
    port: u16,
    token: Option<&str>,
    xfp_https: bool,
) -> anyhow::Result<reqwest::Response> {
    let url = format!("http://127.0.0.1:{port}/v1/hosted/migrate/mint");
    let mut req = reqwest::Client::new().post(&url);
    if let Some(t) = token {
        req = req.header("X-Freenet-User-Token", t);
    }
    if xfp_https {
        req = req.header("X-Forwarded-Proto", "https");
    }
    Ok(req.send().await?)
}

/// `GET /v1/hosted/migrate/pull?pt=<pt>`. Public: the pull token is the entire
/// auth. Returns the raw response so callers assert on 200 (bundle + key
/// headers) and 404 (unknown / already-used / expired).
async fn http_pull(port: u16, pt: &str) -> anyhow::Result<reqwest::Response> {
    let url = format!("http://127.0.0.1:{port}/v1/hosted/migrate/pull?pt={pt}");
    Ok(reqwest::Client::new().get(&url).send().await?)
}

/// `POST /v1/import`. `origin` → `Origin` header; `key`/`kind` → the bundle
/// headers. Mirrors `hosted_mode_import.rs`'s import helper.
async fn http_import(
    port: u16,
    bundle: Vec<u8>,
    key: Option<&str>,
    kind: Option<&str>,
    overwrite: bool,
    origin: Option<&str>,
) -> anyhow::Result<reqwest::Response> {
    let url = format!("http://127.0.0.1:{port}/v1/import");
    let mut req = reqwest::Client::new()
        .post(&url)
        .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
        .body(bundle);
    if let Some(k) = key {
        req = req.header("X-Freenet-Bundle-Key", k);
    }
    if let Some(kd) = kind {
        req = req.header("X-Freenet-Bundle-Key-Kind", kd);
    }
    if overwrite {
        req = req.header("X-Freenet-Import-Overwrite", "true");
    }
    if let Some(o) = origin {
        req = req.header(reqwest::header::ORIGIN, o);
    }
    Ok(req.send().await?)
}

/// `GET /hosted/import` — the local confirmation page.
async fn http_import_page(port: u16) -> anyhow::Result<reqwest::Response> {
    let url = format!("http://127.0.0.1:{port}/hosted/import");
    Ok(reqwest::Client::new().get(&url).send().await?)
}

#[derive(Deserialize)]
struct MintResponseBody {
    pull_token: String,
    expires_in_secs: u64,
}

#[derive(Deserialize)]
struct ImportResponseBody {
    imported: usize,
    skipped: Vec<(String, String)>,
}

/// Headline: the full magic-link round-trip on one hosted node.
///
/// Proves, in order, the load-bearing properties of #4592's primary path:
///
///  * MINT is gated like the live export (secure request → 200 + a pull token).
///  * The bundle is sealed under a FRESH EPHEMERAL key (the durable token never
///    leaves the node): the plaintext is absent from the bundle, and the key the
///    PULL hands back — NOT the durable token — is what decrypts it via
///    `POST /v1/import`.
///  * The pull token is SINGLE-USE: the first pull consumes it; a replay and an
///    unknown token are both a uniform 404.
///  * The migrated secret decrypts to user A's ORIGINAL plaintext when read back
///    through the live delegate stack (the import lands in the node-local scope,
///    which a no-token connection reads).
#[serial_test::serial]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn migrate_magic_link_round_trip() -> anyhow::Result<()> {
    // Pin a multi-executor pool so the mint's export is admitted regardless of
    // the CI runner's core count (a 1-core runner's default pool of 1 disables
    // exports → 503). Mirrors the export/import live tests.
    let _pool = PoolSizeEnv::set(4);
    let node = TestNode::start(/* hosted_mode */ true).await?;
    let port = node.ws_port;

    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;
    let delegate_key = delegate.key().clone();

    const TOKEN: &str = "migrate-user-token-A-aaaaaaaaaaaaaaaa";
    const SECRET_KEY: &[u8] = b"migrate-secret-key";
    const VALUE: &[u8] = b"user-A-secret-to-migrate-home";

    // (1) As user A over the secure hosted connection: register the delegate and
    // store a secret in user A's per-user scope (what the mint will export).
    let mut conn_a = connect_hosted(port, Some(TOKEN), true).await?;
    register_delegate(&mut conn_a, &delegate, &delegate_key).await?;
    store_secret(&mut conn_a, &delegate_key, SECRET_KEY, VALUE).await?;

    // (2) MINT: the authenticated hosted user mints a one-time migration link.
    let resp = http_mint(port, Some(TOKEN), true).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "a secure mint (hosted + loopback + XFP:https + valid token) must be 200"
    );
    let mint: MintResponseBody = resp.json().await?;
    assert!(
        !mint.pull_token.is_empty(),
        "mint must return a non-empty pull token"
    );
    assert_eq!(
        mint.expires_in_secs, 600,
        "the pull token TTL is the documented 600s window"
    );

    // (3) PULL: the public pull endpoint hands back the ephemeral bundle + key.
    let resp = http_pull(port, &mint.pull_token).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "the first pull of a fresh token must be 200"
    );
    // Read the key headers (owned) BEFORE consuming the body.
    let bundle_key = resp
        .headers()
        .get("x-freenet-bundle-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        !bundle_key.is_empty(),
        "pull must return a non-empty X-Freenet-Bundle-Key header"
    );
    let key_kind = resp
        .headers()
        .get("x-freenet-bundle-key-kind")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert_eq!(
        key_kind, "token",
        "the ephemeral bundle key is a token-kind key (default import kind)"
    );
    let bundle = resp.bytes().await?.to_vec();
    assert!(!bundle.is_empty(), "pull must return the bundle bytes");
    assert_eq!(&bundle[0..4], b"FNSX", "pull must return an FNSX bundle");
    // The ephemeral key is NOT the durable token: the token cannot be what
    // sealed the bundle we just pulled.
    assert_ne!(
        bundle_key, TOKEN,
        "the ephemeral bundle key must not be the durable user token"
    );
    // The plaintext value must NOT appear verbatim (sealed under the key).
    assert!(
        !bundle.windows(VALUE.len()).any(|w| w == VALUE),
        "plaintext leaked into the bundle"
    );

    // (4) SINGLE-USE: the first pull consumed the token, so a replay is a uniform
    // 404 — and an unknown token is the SAME 404 (no existence oracle).
    let resp = http_pull(port, &mint.pull_token).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "a second pull of the consumed token must be 404 (single-use)"
    );
    let resp = http_pull(port, "bogusnonexistenttoken123").await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "an unknown pull token must be 404"
    );

    // (5) IMPORT: feed the pulled bundle into POST /v1/import with the EPHEMERAL
    // key the pull handed back (this is exactly what the local pull-import would
    // forward). This proves the ephemeral key round-trips to a real import.
    let origin = format!("http://127.0.0.1:{port}");
    let resp = http_import(
        port,
        bundle,
        Some(&bundle_key),
        Some(&key_kind),
        false,
        Some(&origin),
    )
    .await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "importing the pulled bundle with its ephemeral key must be 200"
    );
    let report: ImportResponseBody = resp.json().await?;
    assert_eq!(report.imported, 1, "exactly user A's one secret imports");
    assert!(report.skipped.is_empty());

    // (6) READBACK through the live stack: POST /v1/import targets the node-local
    // scope, which a NO-TOKEN connection reads. Confirm the migrated secret
    // decrypts to user A's ORIGINAL plaintext while the node stays up.
    let mut conn_local = connect_plain(port).await?;
    register_delegate(&mut conn_local, &delegate, &delegate_key).await?;
    let read = get_secret_via_delegate(&mut conn_local, &delegate_key, SECRET_KEY).await?;
    assert_eq!(
        read.as_deref(),
        Some(VALUE),
        "the migrated secret must decrypt to user A's original plaintext via the live node"
    );
    assert!(
        has_secret_via_delegate(&mut conn_local, &delegate_key, SECRET_KEY).await?,
        "has_secret must see the migrated secret"
    );

    drop(conn_a);
    drop(conn_local);
    node.shutdown().await;
    Ok(())
}

/// The mint gate is the export gate: every insecure variant is rejected with 403
/// before any export runs (so the pool size is irrelevant here). Mirrors the
/// export handler's truth table — the durable token is worthless without the
/// loopback + `X-Forwarded-Proto: https` proof of a secure (TLS-terminated)
/// request, and an absent/empty token is no token at all.
#[serial_test::serial]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn migrate_mint_rejects_insecure_requests() -> anyhow::Result<()> {
    let node = TestNode::start(/* hosted_mode */ true).await?;
    let port = node.ws_port;

    // Token but NO X-Forwarded-Proto: https (loopback alone is insufficient).
    let resp = http_mint(port, Some("some-token-value"), false).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "a mint without XFP:https must be 403"
    );

    // No token at all.
    let resp = http_mint(port, None, true).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "a mint without a user token must be 403"
    );

    // Empty token (treated as absent).
    let resp = http_mint(port, Some(""), true).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "a mint with an empty token must be 403"
    );

    node.shutdown().await;
    Ok(())
}

/// The mint flag gate: with `hosted_mode = false` the whole migration feature is
/// inert — a token + XFP mint is still 403 (no user namespaces exist).
#[serial_test::serial]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn migrate_mint_flag_off_rejects() -> anyhow::Result<()> {
    let node = TestNode::start(/* hosted_mode */ false).await?;
    let port = node.ws_port;

    let resp = http_mint(port, Some("ignored-because-flag-off"), true).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "with hosted_mode=false the mint endpoint must 403"
    );

    node.shutdown().await;
    Ok(())
}

/// The local confirmation page is served (200 HTML) with the explicit import
/// button and the POST target wired to the local pull-import endpoint — and it
/// does NOT import on its own (a state-changing import needs an explicit click,
/// which the button provides; the page never auto-runs it).
#[serial_test::serial]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn migrate_import_page_is_served_with_confirm_control() -> anyhow::Result<()> {
    let node = TestNode::start(/* hosted_mode */ true).await?;
    let port = node.ws_port;

    let resp = http_import_page(port).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "the confirmation page must be 200"
    );
    let ctype = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        ctype.contains("text/html"),
        "the confirmation page must be HTML, got Content-Type: {ctype}"
    );
    let body = resp.text().await?;
    assert!(
        body.contains("id=\"import-btn\""),
        "the page must render the explicit import button"
    );
    assert!(
        body.contains("/v1/hosted/pull-import"),
        "the page must wire the POST to the local pull-import endpoint"
    );

    node.shutdown().await;
    Ok(())
}

/// RAII guard that sets `FREENET_RUNTIME_POOL_SIZE` for the duration of a
/// (serialized) test and restores it on drop, so a specific pool size can be
/// forced when the node builds its `RuntimePool`. Safe only because these tests
/// are `#[serial]` — no other node start overlaps the env-var window.
struct PoolSizeEnv;
impl PoolSizeEnv {
    fn set(n: usize) -> Self {
        // SAFETY: serialized tests; no concurrent node start reads this var.
        unsafe { std::env::set_var("FREENET_RUNTIME_POOL_SIZE", n.to_string()) };
        Self
    }
}
impl Drop for PoolSizeEnv {
    fn drop(&mut self) {
        // SAFETY: serialized tests; no concurrent node start reads this var.
        unsafe { std::env::remove_var("FREENET_RUNTIME_POOL_SIZE") };
    }
}
