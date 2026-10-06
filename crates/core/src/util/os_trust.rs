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

/// What [`add_os_root_certificates`] found. Callers report it when a request
/// later fails: the CLI commands that use this run without a tracing
/// subscriber, so a log line would go nowhere, and this is the only clue a user
/// gets before `UnknownIssuer` (for example from a stale `SSL_CERT_FILE`, which
/// replaces the platform store entirely).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct OsTrustSummary {
    /// Certificates the OS store returned.
    pub found: usize,
    /// Certificates added to the client builder.
    pub added: usize,
    /// The first error reading the store, if any.
    pub first_error: Option<String>,
    /// The platform loader panicked, so only the bundled roots are in use.
    pub panicked: bool,
}

impl std::fmt::Display for OsTrustSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.panicked {
            return f.write_str("loading the OS trust store panicked; used bundled roots only");
        }
        write!(
            f,
            "{} of {} OS trust store certificates added",
            self.added, self.found
        )?;
        if let Some(error) = &self.first_error {
            write!(f, "; error reading the store: {error}")?;
        }
        Ok(())
    }
}

/// Adds the OS trust store's certificates to `builder`, on top of the bundled
/// roots. When `SSL_CERT_FILE` or `SSL_CERT_DIR` is set, those are read instead
/// of the platform store.
///
/// Never fails: a store that cannot be read, or a certificate rustls cannot use
/// as a trust anchor, is skipped, leaving the bundled roots in place.
pub fn add_os_root_certificates(
    builder: reqwest::ClientBuilder,
) -> (reqwest::ClientBuilder, OsTrustSummary) {
    add_root_certificates_from(builder, rustls_native_certs::load_native_certs)
}

fn add_root_certificates_from(
    mut builder: reqwest::ClientBuilder,
    load: impl FnOnce() -> rustls_native_certs::CertificateResult + std::panic::UnwindSafe,
) -> (reqwest::ClientBuilder, OsTrustSummary) {
    // rustls-native-certs unwraps some platform API results on Windows; a
    // panic there must not take down the command that asked for extra roots.
    let Ok(loaded) = std::panic::catch_unwind(load) else {
        let summary = OsTrustSummary {
            panicked: true,
            ..OsTrustSummary::default()
        };
        return (builder, summary);
    };
    let mut summary = OsTrustSummary {
        found: loaded.certs.len(),
        first_error: loaded.errors.first().map(ToString::to_string),
        ..OsTrustSummary::default()
    };
    for der in loaded.certs {
        // reqwest fails the whole client build on a certificate rustls rejects
        // as a trust anchor, and OS stores do contain such certificates, so
        // probe each one the same way first.
        if rustls::RootCertStore::empty().add(der.clone()).is_err() {
            continue;
        }
        if let Ok(cert) = reqwest::Certificate::from_der(&der) {
            builder = builder.add_root_certificate(cert);
            summary.added += 1;
        }
    }
    (builder, summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::pki_types::CertificateDer;

    const USABLE_CERT: &[u8] = include_bytes!("../../tests/data/tls_native_roots/leaf.der");

    #[test]
    fn unusable_os_certificate_is_skipped_rather_than_failing_the_build() {
        let (builder, summary) = add_root_certificates_from(reqwest::Client::builder(), || {
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
        assert_eq!(
            summary,
            OsTrustSummary {
                found: 2,
                added: 1,
                first_error: None,
                panicked: false,
            },
            "the usable certificate must still be added"
        );
    }

    #[test]
    fn panicking_loader_leaves_a_working_builder() {
        let (builder, summary) =
            add_root_certificates_from(reqwest::Client::builder(), || panic!("platform loader"));
        builder
            .build()
            .expect("a panicking OS-store loader must leave the bundled roots usable");
        assert!(summary.panicked);
        assert_eq!(
            summary.to_string(),
            "loading the OS trust store panicked; used bundled roots only"
        );
    }
}
