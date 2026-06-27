//! Live end-to-end integration test for the secrets IMPORT endpoint
//! (`POST /v{1,2}/import`, P3-live of #4592, CHUNK 1).
//!
//! Boots a REAL NORMAL single-user node (hosted mode OFF) — full event loop,
//! contract executor, WASM runtime — then proves the durable property the old
//! `freenet secrets import` CLI could NOT do: import delegate secrets into the
//! running node's local `SecretsStore` **while the node stays up** (the CLI
//! opens ReDb directly, single-writer, so it needs the node stopped).
//!
//! 1. Builds an encrypted P3 bundle IN-PROCESS (a throwaway `SecretsStore` +
//!    `export_bundle`) holding one `SecretScope::Local` secret under the test
//!    delegate, sealed with a token — standing in for a `.fnsx` the user brought
//!    from elsewhere.
//! 2. `POST /v1/import` over loopback with the dashboard `Origin` + the bundle
//!    key header, and asserts 200 + an `ImportReport` of `imported=1`.
//! 3. Reads the secret back THROUGH THE LIVE STACK (register the delegate over
//!    WS, then a delegate `get_secret`/`has_secret`) WHILE THE NODE IS STILL
//!    RUNNING, and asserts it decrypts to the original plaintext.
//!
//! Negatives: a wrong key → 4xx with NO write (the secret stays absent); a
//! missing `Origin` (the dashboard-origin gate) → 403. The gate's loopback,
//! null-origin (sandbox iframe) and per-contract-`AuthToken` exclusions are
//! covered by the `hosted_import` unit tests (they can't be produced against a
//! loopback-bound live server). A collision import (`overwrite=false`) is
//! covered too.

#![allow(clippy::wildcard_enum_match_arm)]

use std::{
    net::{Ipv4Addr, TcpListener},
    path::Path,
    time::{Duration, Instant},
};

use freenet::{
    dev_tool::{BundleKeyMaterial, SecretScope, Secrets, SecretsStore, export_bundle},
    local_node::NodeConfig,
    server::serve_client_api,
    test_utils::load_delegate,
};
use freenet_stdlib::{
    client_api::{ClientRequest, DelegateRequest, HostResponse, WebApi},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};
use tracing::info;
use zeroize::Zeroizing;

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
) -> freenet::config::ConfigArgs {
    freenet::config::ConfigArgs {
        ws_api: freenet::config::WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(ws_port),
            // A NORMAL single-user node: hosted mode OFF. Import targets Local.
            hosted_mode: Some(false),
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
    async fn start() -> anyhow::Result<Self> {
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

/// Plain WS connection (no user token — this is a normal single-user node).
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

/// Build an encrypted P3 bundle IN-PROCESS holding one `Local`-scope secret
/// under `delegate_key`, sealed with token-HKDF over `token`. Stands in for a
/// `.fnsx` the user brought from another peer. The throwaway store's node KEK is
/// irrelevant: the bundle carries PLAINTEXT (re-encrypted under `token`), so it
/// decrypts purely from the token on the receiving node.
async fn build_token_bundle(
    delegate_key: &DelegateKey,
    secret_key: &[u8],
    value: &[u8],
    token: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let tmp = tempfile::tempdir()?;
    let secrets_dir = tmp.path().join("secrets");
    std::fs::create_dir_all(&secrets_dir)?;
    let db = freenet::storages::Storage::new(tmp.path()).await?;
    let secrets = Secrets::load_for_secrets_dir(&secrets_dir)?;
    let mut store = SecretsStore::new(secrets_dir, secrets, db)?;
    let secret_id = SecretsId::new(secret_key.to_vec());
    store.store_secret(
        delegate_key,
        &secret_id,
        SecretScope::Local,
        Zeroizing::new(value.to_vec()),
    )?;
    let bundle = export_bundle(&store, SecretScope::Local, &BundleKeyMaterial::Token(token))?;
    Ok(bundle)
}

/// `POST /v1/import`. `origin` → `Origin` header; `key`/`kind`/`overwrite` →
/// the bundle headers. Returns the raw response so callers assert on status.
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

#[derive(Deserialize)]
struct ImportResponseBody {
    imported: usize,
    skipped: Vec<(String, String)>,
}

/// Headline: a bundle imported over `POST /v1/import` lands in the running node's
/// store and is immediately readable through the live delegate stack — WHILE THE
/// NODE STAYS UP (the property the node-stopped CLI import cannot provide).
#[serial_test::serial]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn live_import_round_trip_node_stays_up() -> anyhow::Result<()> {
    // Pin a multi-executor pool so the import is admitted regardless of the CI
    // runner's core count (a 1-core runner's default pool of 1 disables off-loop
    // secret jobs).
    let _pool = PoolSizeEnv::set(4);
    let node = TestNode::start().await?;
    let port = node.ws_port;

    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;
    let delegate_key = delegate.key().clone();

    const TOKEN: &str = "import-bundle-key-aaaaaaaaaaaaaaaaaaaa";
    const SECRET_KEY: &[u8] = b"imported-secret-key";
    const VALUE: &[u8] = b"the-value-the-user-brought-home";

    // Build the bundle out-of-band (as if downloaded from try.freenet.org).
    let bundle = build_token_bundle(&delegate_key, SECRET_KEY, VALUE, TOKEN.as_bytes()).await?;
    assert_eq!(
        &bundle[0..4],
        b"FNSX",
        "fixture must produce an FNSX bundle"
    );

    let origin = format!("http://127.0.0.1:{port}");

    // Sanity: the secret is NOT present before the import.
    let mut conn = connect_plain(port).await?;
    register_delegate(&mut conn, &delegate, &delegate_key).await?;
    assert!(
        !has_secret_via_delegate(&mut conn, &delegate_key, SECRET_KEY).await?,
        "secret must be absent before import"
    );

    // Import over HTTP (loopback + dashboard origin + the bundle key).
    let resp = http_import(
        port,
        bundle,
        Some(TOKEN),
        Some("token"),
        false,
        Some(&origin),
    )
    .await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "live import over loopback must be 200 OK"
    );
    let report: ImportResponseBody = resp.json().await?;
    assert_eq!(report.imported, 1, "exactly the one secret imports");
    assert!(report.skipped.is_empty());

    // Read it back THROUGH THE LIVE STACK while the node is still running.
    let read = get_secret_via_delegate(&mut conn, &delegate_key, SECRET_KEY).await?;
    assert_eq!(
        read.as_deref(),
        Some(VALUE),
        "the imported secret must decrypt to its original plaintext via the live node"
    );
    assert!(
        has_secret_via_delegate(&mut conn, &delegate_key, SECRET_KEY).await?,
        "has_secret must see the imported secret"
    );

    drop(conn);
    node.shutdown().await;
    Ok(())
}

/// A wrong key cannot decrypt the bundle: 4xx, and NOTHING is written (the
/// decrypt fails before any write). The secret stays absent on the live node.
#[serial_test::serial]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn live_import_wrong_key_rejected_no_write() -> anyhow::Result<()> {
    let _pool = PoolSizeEnv::set(4);
    let node = TestNode::start().await?;
    let port = node.ws_port;

    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;
    let delegate_key = delegate.key().clone();

    const TOKEN: &str = "the-real-bundle-key-bbbbbbbbbbbbbbbb";
    const SECRET_KEY: &[u8] = b"wrong-key-secret";
    const VALUE: &[u8] = b"should-never-land";

    let bundle = build_token_bundle(&delegate_key, SECRET_KEY, VALUE, TOKEN.as_bytes()).await?;
    let origin = format!("http://127.0.0.1:{port}");

    // Import with a DIFFERENT key → must fail authentication.
    let resp = http_import(
        port,
        bundle,
        Some("a-completely-different-key"),
        Some("token"),
        false,
        Some(&origin),
    )
    .await?;
    assert!(
        resp.status().is_client_error(),
        "a wrong key must be a 4xx (client fault), got {}",
        resp.status()
    );

    // NO write: the secret must be absent on the live node.
    let mut conn = connect_plain(port).await?;
    register_delegate(&mut conn, &delegate, &delegate_key).await?;
    assert!(
        !has_secret_via_delegate(&mut conn, &delegate_key, SECRET_KEY).await?,
        "a failed (wrong-key) import must write NOTHING"
    );

    drop(conn);
    node.shutdown().await;
    Ok(())
}

/// The dashboard-origin gate: a request with no `Origin` header is rejected
/// (403) by the live server — exercises the gate end-to-end (its loopback /
/// null-origin / per-contract-token branches are unit-tested in `hosted_import`).
#[serial_test::serial]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn live_import_missing_origin_rejected() -> anyhow::Result<()> {
    let _pool = PoolSizeEnv::set(4);
    let node = TestNode::start().await?;
    let port = node.ws_port;

    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;
    let delegate_key = delegate.key().clone();
    let bundle = build_token_bundle(&delegate_key, b"k", b"v", b"key-cccccccccccccccccccc").await?;

    // No Origin header → the gate rejects with 403 before any work.
    let resp = http_import(
        port,
        bundle,
        Some("key-cccccccccccccccccccc"),
        Some("token"),
        false,
        None,
    )
    .await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "an import without a trusted Origin must be 403"
    );

    node.shutdown().await;
    Ok(())
}

/// Collision handling: re-importing the same secret with `overwrite=false` skips
/// it and reports it (idempotent), leaving the value untouched and still
/// readable via the live stack.
#[serial_test::serial]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn live_import_collision_skips_and_reports() -> anyhow::Result<()> {
    let _pool = PoolSizeEnv::set(4);
    let node = TestNode::start().await?;
    let port = node.ws_port;

    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;
    let delegate_key = delegate.key().clone();

    const TOKEN: &str = "collision-key-dddddddddddddddddddd";
    const SECRET_KEY: &[u8] = b"collision-secret";
    const VALUE: &[u8] = b"first-import-value";

    let origin = format!("http://127.0.0.1:{port}");

    // First import lands the secret.
    let bundle = build_token_bundle(&delegate_key, SECRET_KEY, VALUE, TOKEN.as_bytes()).await?;
    let resp = http_import(
        port,
        bundle,
        Some(TOKEN),
        Some("token"),
        false,
        Some(&origin),
    )
    .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let first: ImportResponseBody = resp.json().await?;
    assert_eq!(first.imported, 1);

    // Second import of the SAME secret with overwrite=false → skipped+reported.
    let bundle = build_token_bundle(&delegate_key, SECRET_KEY, VALUE, TOKEN.as_bytes()).await?;
    let resp = http_import(
        port,
        bundle,
        Some(TOKEN),
        Some("token"),
        false,
        Some(&origin),
    )
    .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let second: ImportResponseBody = resp.json().await?;
    assert_eq!(
        second.imported, 0,
        "a colliding secret must not be re-imported"
    );
    assert_eq!(second.skipped.len(), 1, "the collision must be reported");

    // The value is still readable via the live stack.
    let mut conn = connect_plain(port).await?;
    register_delegate(&mut conn, &delegate, &delegate_key).await?;
    assert_eq!(
        get_secret_via_delegate(&mut conn, &delegate_key, SECRET_KEY)
            .await?
            .as_deref(),
        Some(VALUE)
    );

    drop(conn);
    node.shutdown().await;
    Ok(())
}

/// RAII guard that sets `FREENET_RUNTIME_POOL_SIZE` for the duration of a
/// (serialized) test and restores it on drop. Safe only because these tests are
/// `#[serial]` — no other node start overlaps the env-var window.
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
