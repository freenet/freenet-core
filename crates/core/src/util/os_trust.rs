//! Opt-in trust of the OS certificate store for HTTPS clients.
//!
//! The workspace reqwest trusts only the bundled webpki (Mozilla) roots, and
//! that is deliberate. Update downloads and other fetches whose integrity rests
//! on TLS must not accept a CA merely because something installed it in the OS
//! store: release signatures are not yet mandatory (`REQUIRE_RELEASE_SIGNATURE`
//! in `bin/commands/update.rs`), and OS stores can hold adware or local
//! development roots whose keys are not private.
//!
//! Requests with nothing integrity-critical opt in here, so they also work
//! behind TLS-intercepting proxies, whose CA the administrator installs in the
//! OS store. Their contents are then readable by any CA in that store. Today
//! that is only `freenet service report`; `freenet update` and auto-update
//! still fail behind such proxies until signatures are mandatory.

/// Adds the OS trust store's certificates to `builder`, on top of the bundled
/// roots. When `SSL_CERT_FILE` or `SSL_CERT_DIR` is set, those are read instead
/// of the platform store.
///
/// Never fails: a store that cannot be read, or a certificate rustls cannot use
/// as a trust anchor, is skipped, leaving the bundled roots in place.
pub fn add_os_root_certificates(builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    add_root_certificates_from(builder, rustls_native_certs::load_native_certs)
}

fn add_root_certificates_from(
    mut builder: reqwest::ClientBuilder,
    load: impl FnOnce() -> rustls_native_certs::CertificateResult + std::panic::UnwindSafe,
) -> reqwest::ClientBuilder {
    // rustls-native-certs unwraps some platform API results on Windows; a
    // panic there must not take down the command that asked for extra roots.
    let Ok(loaded) = std::panic::catch_unwind(load) else {
        tracing::warn!(
            "Loading the OS trust store panicked (the message above); using bundled roots only"
        );
        return builder;
    };
    // WARN, not debug: this is the only clue a user behind a TLS-intercepting
    // proxy gets when the upload then fails with `UnknownIssuer` (for example a
    // stale SSL_CERT_FILE, which replaces the platform store entirely).
    if let Some(first) = loaded.errors.first() {
        tracing::warn!(
            error = %first,
            count = loaded.errors.len(),
            "Errors reading the OS trust store"
        );
    }
    let found = loaded.certs.len();
    let mut added = 0usize;
    for der in loaded.certs {
        // reqwest fails the whole client build on a certificate rustls rejects
        // as a trust anchor, and OS stores do contain such certificates, so
        // probe each one the same way first.
        if rustls::RootCertStore::empty().add(der.clone()).is_err() {
            continue;
        }
        if let Ok(cert) = reqwest::Certificate::from_der(&der) {
            builder = builder.add_root_certificate(cert);
            added += 1;
        }
    }
    if added == 0 {
        tracing::warn!(
            found,
            "No usable certificates in the OS trust store; using bundled roots only"
        );
    }
    builder
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::pki_types::CertificateDer;

    const USABLE_CERT: &[u8] = include_bytes!("../../tests/data/tls_native_roots/leaf.der");

    #[test]
    fn unusable_os_certificate_is_skipped_rather_than_failing_the_build() {
        let builder = add_root_certificates_from(reqwest::Client::builder(), || {
            let mut loaded = rustls_native_certs::CertificateResult::default();
            loaded.certs = vec![
                // Valid DER framing, but not a certificate rustls can anchor on.
                CertificateDer::from(vec![0x30, 0x03, 0x01, 0x02, 0x03]),
                CertificateDer::from(USABLE_CERT.to_vec()),
            ];
            loaded
        });
        builder
            .build()
            .expect("an unusable OS-store certificate must not fail the client build");
    }

    #[test]
    fn panicking_loader_leaves_a_working_builder() {
        let builder =
            add_root_certificates_from(reqwest::Client::builder(), || panic!("platform loader"));
        builder
            .build()
            .expect("a panicking OS-store loader must leave the bundled roots usable");
    }
}
