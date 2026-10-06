//! Regression test: HTTPS clients must trust CAs installed in the OS trust store.
//!
//! Networks that intercept TLS (corporate proxies) re-sign every certificate
//! with a CA the administrator installed in the OS trust store. With only the
//! bundled webpki roots, `freenet service report` and the update checks failed
//! there with `invalid peer certificate: UnknownIssuer`.
//!
//! A local HTTPS server stands in for the proxy: its certificate chains to a
//! throwaway CA (`tests/data/tls_native_roots/`, valid until 2126) that is
//! "installed" via `SSL_CERT_FILE`, which rustls-native-certs reads in place of
//! the platform store on every OS. This must stay the only test in this binary,
//! because it sets that process-global env var.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{self, pki_types};

const CA_PEM_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/data/tls_native_roots/ca.pem"
);
const LEAF_CERT_DER: &[u8] = include_bytes!("data/tls_native_roots/leaf.der");
const LEAF_KEY_DER: &[u8] = include_bytes!("data/tls_native_roots/leaf.key.der");

/// Serves a fixed `200 ok` over HTTPS on an ephemeral loopback port.
async fn spawn_https_server() -> u16 {
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![pki_types::CertificateDer::from(LEAF_CERT_DER.to_vec())],
        pki_types::PrivateKeyDer::Pkcs8(pki_types::PrivatePkcs8KeyDer::from(LEAF_KEY_DER.to_vec())),
    )
    .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                // A failed handshake is the expected outcome for the negative control.
                let Ok(mut tls) = acceptor.accept(tcp).await else {
                    return;
                };
                let mut buf = [0u8; 4096];
                let _ = tls.read(&mut buf).await;
                let _ = tls
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
                    )
                    .await;
                let _ = tls.shutdown().await;
            });
        }
    });
    port
}

#[test]
fn https_client_trusts_ca_installed_in_os_trust_store() {
    // SAFETY: set before the runtime exists, so no other thread can be reading
    // the environment. This is the only test in this binary.
    unsafe { std::env::set_var("SSL_CERT_FILE", CA_PEM_PATH) };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(check_trust());
}

async fn check_trust() {
    let port = spawn_https_server().await;
    let url = format!("https://localhost:{port}/");

    // Negative control: with the OS store excluded the CA is rejected, so the
    // assertion below cannot pass through the bundled roots or any other path.
    let bundled_roots_only = reqwest::Client::builder()
        .tls_built_in_native_certs(false)
        .build()
        .unwrap();
    let err = bundled_roots_only
        .get(&url)
        .send()
        .await
        .expect_err("the test CA must not be trusted without the OS trust store");
    assert!(
        format!("{err:?}").contains("UnknownIssuer"),
        "expected an UnknownIssuer rejection, got: {err:?}"
    );

    // Built the same way as the clients in `freenet service report` and the
    // update checks: no explicit root configuration.
    let client = reqwest::Client::builder().build().unwrap();
    let response = client
        .get(&url)
        .send()
        .await
        .expect("a CA in the OS trust store must be trusted (TLS-intercepting networks)");
    assert_eq!(response.text().await.unwrap(), "ok");
}
