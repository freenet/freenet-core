//! Opt-in trust of the OS certificate store for HTTPS clients.
//!
//! The workspace reqwest trusts only the bundled webpki (Mozilla) roots, and
//! that is deliberate. Update downloads and other fetches whose integrity rests
//! on TLS must not accept a CA merely because something installed it in the OS
//! store: release signatures are not yet mandatory (`REQUIRE_RELEASE_SIGNATURE`
//! in `bin/commands/update.rs`), and OS stores can hold adware or local
//! development roots whose keys are not private.
//!
//! Requests with nothing to protect opt in here, so they also work behind
//! TLS-intercepting proxies, whose CA the administrator installs in the OS
//! store. Today that is only `freenet service report`.

/// Adds the OS trust store's certificates to `builder`, on top of the bundled
/// roots. When `SSL_CERT_FILE` or `SSL_CERT_DIR` is set, those are read instead
/// of the platform store.
///
/// Never fails: a store that cannot be read, or a certificate rustls cannot use
/// as a trust anchor, is skipped, leaving the bundled roots in place.
pub fn add_os_root_certificates(mut builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    // rustls-native-certs unwraps some platform API results on Windows; a
    // panic there must not take down the command that asked for extra roots.
    let Ok(loaded) = std::panic::catch_unwind(rustls_native_certs::load_native_certs) else {
        tracing::warn!("Loading the OS trust store panicked; using bundled roots only");
        return builder;
    };
    for error in &loaded.errors {
        tracing::debug!(%error, "Error reading the OS trust store");
    }
    for der in loaded.certs {
        // reqwest fails the whole client build on a certificate rustls rejects
        // as a trust anchor, and OS stores do contain such certificates, so
        // probe each one the same way first.
        if rustls::RootCertStore::empty().add(der.clone()).is_err() {
            continue;
        }
        if let Ok(cert) = reqwest::Certificate::from_der(&der) {
            builder = builder.add_root_certificate(cert);
        }
    }
    builder
}
