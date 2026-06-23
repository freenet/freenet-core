//! Live end-to-end integration test for the hosted-mode export endpoint
//! (`GET /v{1,2}/hosted/export`, P3-live of #4381).
//!
//! Boots a REAL single hosted node — full event loop, contract executor, WASM
//! runtime — exactly like `hosted_mode_isolation.rs`, then:
//!
//! 1. Stores a per-user secret as user A over the live WS stack (so the node's
//!    `SecretsStore` actually holds a `SecretScope::User` entry).
//! 2. Hits `GET /v1/hosted/export` over HTTP with the `X-Freenet-User-Token`
//!    header (+ `X-Forwarded-Proto: https`) and asserts the response is a
//!    non-empty `FNSX` bundle that round-trips through the P3 `import_bundle`
//!    back to the original secret. This proves the gate honors a secure request
//!    AND that the bytes are the real, decryptable bundle.
//! 3. Asserts every INSECURE variant is rejected with 403 and yields NO bundle:
//!    no token, token-without-XFP, and (flag-off node) token-but-hosted-off.
//!
//! The node binds loopback `127.0.0.1`, so the export gate sees a loopback
//! source automatically (the trust anchor); the only knob the insecure cases
//! flip is the header / the flag, matching the WS gate's truth table.

#![allow(clippy::wildcard_enum_match_arm)]

use std::{
    net::{Ipv4Addr, TcpListener},
    path::Path,
    time::{Duration, Instant},
};

use freenet::{
    dev_tool::{
        BundleKeyMaterial, ImportReport, Secrets, SecretsStore, TargetScope, import_bundle,
    },
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

const TEST_DELEGATE: &str = "test-delegate-2";

const TEST_DELEGATE_CIPHER: [u8; 32] = [
    0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
    0x0f, 0x1e, 0x2d, 0x3c, 0x4b, 0x5a, 0x69, 0x78, 0x87, 0x96, 0xa5, 0xb4, 0xc3, 0xd2, 0xe1, 0xf0,
];
const TEST_DELEGATE_NONCE: [u8; 24] = [0u8; 24];

// Mirror of the fixture's `InboundAppMessage` / `OutboundAppMessage` (variant
// ORDER must match for bincode discriminants — see hosted_mode_isolation.rs).
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

/// `GET /v1/hosted/export`. `token` → `X-Freenet-User-Token`; `xfp_https`
/// → `X-Forwarded-Proto: https`. Returns the raw response so callers assert
/// on both the secure (200 + bytes) and insecure (403) paths.
async fn http_export(
    port: u16,
    token: Option<&str>,
    xfp_https: bool,
) -> anyhow::Result<reqwest::Response> {
    let url = format!("http://127.0.0.1:{port}/v1/hosted/export");
    let mut req = reqwest::Client::new().get(&url);
    if let Some(t) = token {
        req = req.header("X-Freenet-User-Token", t);
    }
    if xfp_https {
        req = req.header("X-Forwarded-Proto", "https");
    }
    Ok(req.send().await?)
}

/// Open a throwaway `SecretsStore` rooted at `secrets_dir` and import `bundle`
/// at `target`, returning the import report. Used to prove the exported bytes
/// are a real, decryptable P3 bundle (the same round-trip the user does on
/// their own peer).
async fn import_into_fresh_store(
    bundle: &[u8],
    token: &[u8],
    target: &TargetScope,
) -> anyhow::Result<ImportReport> {
    let tmp = tempfile::tempdir()?;
    let secrets_dir = tmp.path().join("secrets");
    std::fs::create_dir_all(&secrets_dir)?;
    let db = freenet::storages::Storage::new(tmp.path()).await?;
    // `Secrets::default()` is `#[cfg(test)]`-only inside freenet-core and so is
    // not visible to this external test crate. Use the public loader, which
    // generates a fresh node `Secrets` for an empty dir — the value only affects
    // how Local-scope blobs are RE-encrypted at rest in this throwaway store;
    // the BUNDLE itself decrypts purely from the token.
    let secrets = Secrets::load_for_secrets_dir(&secrets_dir)?;
    let mut store = SecretsStore::new(secrets_dir, secrets, db)?;
    let report = import_bundle(
        &mut store,
        bundle,
        &BundleKeyMaterial::Token(token),
        target,
        false,
    )?;
    Ok(report)
}

/// Headline: a secure export (hosted on + loopback + XFP:https + a valid token
/// that has stored secrets) returns a non-empty FNSX bundle that round-trips
/// via `import_bundle` back to the original secret.
#[serial_test::serial]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn hosted_export_secure_round_trip() -> anyhow::Result<()> {
    // Pin a multi-executor pool so the export is admitted regardless of the CI
    // runner's core count (a 1-core runner's default pool of 1 disables exports).
    let _pool = PoolSizeEnv::set(4);
    let node = TestNode::start(/* hosted_mode */ true).await?;
    let port = node.ws_port;

    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;
    let delegate_key = delegate.key().clone();

    const TOKEN: &str = "user-token-for-export-aaaaaaaaaaaaaaaa";
    const SECRET_KEY: &[u8] = b"exported-secret-key";
    const VALUE: &[u8] = b"the-value-user-A-wants-to-take-with-them";

    // Store a per-user secret as this user over the live WS stack.
    let mut conn = connect_hosted(port, Some(TOKEN), true).await?;
    register_delegate(&mut conn, &delegate, &delegate_key).await?;
    store_secret(&mut conn, &delegate_key, SECRET_KEY, VALUE).await?;

    // Export over HTTP with the secure header set.
    let resp = http_export(port, Some(TOKEN), true).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "secure export must be 200 OK"
    );
    // Download headers.
    let ctype = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert_eq!(ctype, "application/octet-stream", "wrong Content-Type");
    let cdisp = resp
        .headers()
        .get(reqwest::header::CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        cdisp.contains("attachment") && cdisp.contains("freenet-data.fnsx"),
        "wrong Content-Disposition: {cdisp}"
    );

    let bundle = resp.bytes().await?.to_vec();
    assert!(!bundle.is_empty(), "export bundle must be non-empty");
    // P3 magic: the bundle starts with `FNSX`.
    assert_eq!(&bundle[0..4], b"FNSX", "not an FNSX bundle");
    // The plaintext value must NOT appear verbatim (encrypted at rest).
    assert!(
        !bundle.windows(VALUE.len()).any(|w| w == VALUE),
        "plaintext leaked into the bundle"
    );

    // Round-trip: import into a fresh store at Local scope and read the secret
    // back. This is what the user does on their own single-user peer.
    let report = import_into_fresh_store(&bundle, TOKEN.as_bytes(), &TargetScope::Local).await?;
    assert_eq!(report.imported, 1, "exactly the one stored secret imports");
    assert!(report.skipped.is_empty());

    // And confirm the imported plaintext equals the original by re-importing at
    // the User scope and enumerating the entry.
    let user_report = import_into_fresh_store(
        &bundle,
        TOKEN.as_bytes(),
        &TargetScope::user_from_token(TOKEN.as_bytes()),
    )
    .await?;
    assert_eq!(user_report.imported, 1);

    drop(conn);
    node.shutdown().await;
    Ok(())
}

/// DEFER (#4531 / #4381 P5): an in-flight export must NOT block the contract
/// loop. We store several secrets (so the export does real enumerate+decrypt+
/// seal work), then fire the HTTP export CONCURRENTLY with a WS delegate
/// store-secret op that goes through the SAME contract loop, and require BOTH to
/// complete. With the previous inline design the export ran ON the loop, so the
/// concurrent delegate op could not even start until the export finished; with
/// the deferral the loop stays free and both make progress together. Generous
/// timeouts (this asserts non-blocking liveness + correctness, not tight
/// timing — the strict "arm defers off-loop" guarantee is pinned by the
/// source-scrape unit tests `export_dispatch_arm_defers_off_loop` /
/// `export_job_run_offloads_blocking_work`).
#[serial_test::serial]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn export_does_not_block_concurrent_contract_ops() -> anyhow::Result<()> {
    // Pin a multi-executor pool so the export is admitted (200) deterministically
    // regardless of the CI runner's core count — on a 1-core runner the default
    // pool would be 1, which disables exports (covered separately).
    let _pool = PoolSizeEnv::set(4);
    let node = TestNode::start(/* hosted_mode */ true).await?;
    let port = node.ws_port;

    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;
    let delegate_key = delegate.key().clone();

    const TOKEN: &str = "user-token-for-defer-bbbbbbbbbbbbbbbb";

    let mut conn = connect_hosted(port, Some(TOKEN), true).await?;
    register_delegate(&mut conn, &delegate, &delegate_key).await?;
    // Store enough secrets that the export has real work to do.
    for i in 0..64u32 {
        let key = format!("defer-secret-{i}");
        let value = vec![b'v'; 256];
        store_secret(&mut conn, &delegate_key, key.as_bytes(), &value).await?;
    }

    // Open a SECOND connection for the concurrent op so it doesn't serialize
    // behind the first connection's WS request/response framing.
    let mut conn2 = connect_hosted(port, Some(TOKEN), true).await?;
    register_delegate(&mut conn2, &delegate, &delegate_key).await?;

    // Fire the export and a concurrent delegate store-secret op at the same
    // time. Both route through the single contract-handling loop; both must
    // complete. The concurrent op must NOT be starved for the export's duration.
    let export_fut = http_export(port, Some(TOKEN), true);
    let concurrent_op_fut = store_secret(
        &mut conn2,
        &delegate_key,
        b"stored-while-export-runs",
        b"concurrent-value",
    );

    let (export_resp, concurrent_op) = tokio::join!(
        async { timeout(Duration::from_secs(30), export_fut).await },
        async { timeout(Duration::from_secs(30), concurrent_op_fut).await },
    );

    // The concurrent contract op completed (was not blocked behind the export).
    concurrent_op
        .expect("a contract op concurrent with an export must not be starved (#4531)")
        .expect("the concurrent store-secret op must succeed");

    // The export also completed and returned a valid bundle.
    let export_resp = export_resp
        .expect("export must complete")
        .expect("export HTTP request must not error");
    assert_eq!(
        export_resp.status(),
        reqwest::StatusCode::OK,
        "export must be 200 OK"
    );
    let bundle = export_resp.bytes().await?.to_vec();
    assert_eq!(
        &bundle[0..4],
        b"FNSX",
        "export must return a valid FNSX bundle"
    );

    drop(conn);
    drop(conn2);
    node.shutdown().await;
    Ok(())
}

/// A wrong token cannot decrypt a bundle exported under the real token —
/// confirms the bundle key is genuinely the user's token (not a fixed key).
#[serial_test::serial]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn hosted_export_bundle_rejects_wrong_token() -> anyhow::Result<()> {
    // Pin a multi-executor pool so the export is admitted (this test needs a real
    // bundle to then prove a wrong token can't decrypt it).
    let _pool = PoolSizeEnv::set(4);
    let node = TestNode::start(true).await?;
    let port = node.ws_port;

    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;
    let delegate_key = delegate.key().clone();
    const TOKEN: &str = "the-real-token-bbbbbbbbbbbbbbbbbbbbbb";

    let mut conn = connect_hosted(port, Some(TOKEN), true).await?;
    register_delegate(&mut conn, &delegate, &delegate_key).await?;
    store_secret(&mut conn, &delegate_key, b"k", b"v").await?;

    let bundle = http_export(port, Some(TOKEN), true)
        .await?
        .bytes()
        .await?
        .to_vec();

    // Importing with a DIFFERENT token must fail the AEAD authentication.
    let err = import_into_fresh_store(&bundle, b"a-different-token", &TargetScope::Local)
        .await
        .expect_err("wrong token must not decrypt the bundle");
    assert!(
        err.to_string().to_lowercase().contains("auth")
            || err.to_string().to_lowercase().contains("passphrase")
            || err.to_string().to_lowercase().contains("token"),
        "expected an auth failure, got: {err}"
    );

    drop(conn);
    node.shutdown().await;
    Ok(())
}

/// Insecure variants on a hosted node: every one must 403 and return NO bundle.
#[serial_test::serial]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn hosted_export_rejects_insecure_requests() -> anyhow::Result<()> {
    let node = TestNode::start(/* hosted_mode */ true).await?;
    let port = node.ws_port;

    // (a) No token at all.
    let resp = http_export(port, None, true).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "no-token export must be 403"
    );
    let body = resp.bytes().await?;
    assert_ne!(
        &body[..body.len().min(4)],
        b"FNSX",
        "must not return a bundle"
    );

    // (b) Token but NO X-Forwarded-Proto: https (loopback alone is insufficient).
    let resp = http_export(port, Some("some-token-value"), false).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "token-without-https export must be 403"
    );
    let body = resp.bytes().await?;
    assert_ne!(
        &body[..body.len().min(4)],
        b"FNSX",
        "must not return a bundle"
    );

    // (c) Empty token (treated as no token).
    let resp = http_export(port, Some(""), true).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "empty-token export must be 403"
    );

    node.shutdown().await;
    Ok(())
}

/// Flag gate: with `hosted_mode = false`, a token + XFP export is still 403 —
/// the whole feature is inert unless the flag is on (no user namespaces exist).
#[serial_test::serial]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn hosted_export_flag_off_rejects() -> anyhow::Result<()> {
    let node = TestNode::start(/* hosted_mode */ false).await?;
    let port = node.ws_port;

    let resp = http_export(port, Some("ignored-because-flag-off"), true).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "with hosted_mode=false the export endpoint must 403"
    );
    let body = resp.bytes().await?;
    assert_ne!(
        &body[..body.len().min(4)],
        b"FNSX",
        "must not return a bundle"
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
        unsafe { std::env::remove_var("FREENET_RUNTIME_POOL_SIZE") };
    }
}

/// DEADLOCK REGRESSION (#4531, #4563 re-review): on a 2-executor pool, an export
/// in flight must NOT wedge the contract loop. The effective export-concurrency
/// cap is `min(MAX_CONCURRENT_EXPORTS, pool_size-1) == 1`, so an admitted export
/// holds exactly ONE of the two executors; the loop keeps the other free for
/// normal ops. We fire an HTTP export and a concurrent WS delegate op together
/// and require BOTH to complete — the previous design (blocking executor
/// checkout on the loop, no pool-size clamp) could deadlock here. Exercises the
/// real node loop + pool, not a mock.
#[serial_test::serial]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn export_on_small_pool_does_not_deadlock_the_loop() -> anyhow::Result<()> {
    let _pool = PoolSizeEnv::set(2);
    let node = TestNode::start(/* hosted_mode */ true).await?;
    let port = node.ws_port;

    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;
    let delegate_key = delegate.key().clone();
    const TOKEN: &str = "user-token-small-pool-cccccccccccccccc";

    let mut conn = connect_hosted(port, Some(TOKEN), true).await?;
    register_delegate(&mut conn, &delegate, &delegate_key).await?;
    for i in 0..32u32 {
        store_secret(
            &mut conn,
            &delegate_key,
            format!("sp-secret-{i}").as_bytes(),
            &vec![b'v'; 256],
        )
        .await?;
    }

    // Second connection for the concurrent op (independent WS framing).
    let mut conn2 = connect_hosted(port, Some(TOKEN), true).await?;
    register_delegate(&mut conn2, &delegate, &delegate_key).await?;

    // Export (holds 1 of 2 executors off-loop) + a concurrent contract op. With
    // pool_size=2 the loop must keep the second executor free; both must finish.
    let export_fut = http_export(port, Some(TOKEN), true);
    let op_fut = store_secret(
        &mut conn2,
        &delegate_key,
        b"sp-stored-while-export-runs",
        b"sp-concurrent-value",
    );
    let (export_resp, op) = tokio::join!(
        async { timeout(Duration::from_secs(30), export_fut).await },
        async { timeout(Duration::from_secs(30), op_fut).await },
    );

    op.expect("concurrent op must not be wedged behind the export on a 2-pool (#4531)")
        .expect("the concurrent store-secret op must succeed");
    let export_resp = export_resp
        .expect("export must complete")
        .expect("export HTTP request must not error");
    // On a 2-executor pool exactly one export permit exists, so this single
    // export is admitted and succeeds (200).
    assert_eq!(
        export_resp.status(),
        reqwest::StatusCode::OK,
        "the single export on a 2-pool must be admitted and succeed"
    );

    drop(conn);
    drop(conn2);
    node.shutdown().await;
    Ok(())
}

/// On a 1-executor pool the effective export concurrency is 0, so an export is
/// DISABLED — it returns 503 (Busy) rather than running on the sole executor
/// (which would be the inline-blocking this change removes) or deadlocking. The
/// node must otherwise function normally. Exercises the real node loop + pool.
#[serial_test::serial]
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn export_disabled_on_single_executor_pool() -> anyhow::Result<()> {
    let _pool = PoolSizeEnv::set(1);
    let node = TestNode::start(/* hosted_mode */ true).await?;
    let port = node.ws_port;

    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;
    let delegate_key = delegate.key().clone();
    const TOKEN: &str = "user-token-single-pool-dddddddddddddddd";

    // The node still works on a 1-executor pool: store a secret normally.
    let mut conn = connect_hosted(port, Some(TOKEN), true).await?;
    register_delegate(&mut conn, &delegate, &delegate_key).await?;
    store_secret(&mut conn, &delegate_key, b"sole-secret", b"sole-value").await?;

    // The export is disabled (0 export permits) → 503, NOT a hang and NOT a 200.
    let resp = timeout(
        Duration::from_secs(30),
        http_export(port, Some(TOKEN), true),
    )
    .await
    .expect("export request must return promptly (disabled, not hung)")?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "export must be 503 (disabled) on a single-executor pool"
    );

    // And the node still serves normal ops afterwards (loop not wedged).
    store_secret(&mut conn, &delegate_key, b"after-export", b"after-value")
        .await
        .expect("node must still serve contract ops after a rejected export");

    drop(conn);
    node.shutdown().await;
    Ok(())
}
