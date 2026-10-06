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

use std::ffi::OsString;

/// What [`add_os_root_certificates`] found. Callers must show it to the user
/// when a connection later fails: the CLI commands that use this run without a
/// tracing subscriber, so a log line would go nowhere, and this is the only
/// clue a user gets before `UnknownIssuer` (for example from a stale
/// `SSL_CERT_FILE`, which replaces the platform store entirely).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct OsTrustSummary {
    /// Certificates the OS store returned.
    pub found: usize,
    /// Certificates added to the client builder.
    pub added: usize,
    /// The first error reading the store, if any.
    pub first_error: Option<String>,
    /// How many errors reading the store there were in all.
    pub error_count: usize,
    /// The platform loader panicked, so only the bundled roots are in use.
    pub panicked: bool,
    /// `SSL_CERT_FILE` / `SSL_CERT_DIR` settings that replaced the platform
    /// store. A file that exists but lacks the proxy's CA loads cleanly, so
    /// this is the clue in that case.
    pub env_override: Option<String>,
}

impl std::fmt::Display for OsTrustSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.panicked {
            return f.write_str("loading the OS trust store panicked; used bundled roots only");
        }
        match &self.env_override {
            Some(source) => write!(
                f,
                "{} of {} certificates from {source} added, in place of the OS trust store",
                self.added, self.found
            )?,
            None => write!(
                f,
                "{} of {} OS trust store certificates added",
                self.added, self.found
            )?,
        }
        if let Some(error) = &self.first_error {
            write!(f, "; error reading the store: {error}")?;
            if self.error_count > 1 {
                write!(f, " (+{} more)", self.error_count - 1)?;
            }
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
    // Read before loading, so the summary describes what the loader saw.
    let env_override = env_override(|name| std::env::var_os(name));
    let (builder, mut summary) =
        add_root_certificates_from(builder, rustls_native_certs::load_native_certs);
    summary.env_override = env_override;
    (builder, summary)
}

/// The `SSL_CERT_FILE` / `SSL_CERT_DIR` settings that make rustls-native-certs
/// 0.8 skip the platform store, using its rule: the file whenever it is set,
/// the directory list only when it has a non-empty entry.
fn env_override(var: impl Fn(&str) -> Option<OsString>) -> Option<String> {
    let mut overrides = Vec::new();
    if let Some(file) = var("SSL_CERT_FILE") {
        overrides.push(format!("SSL_CERT_FILE={}", file.to_string_lossy()));
    }
    if let Some(dirs) = var("SSL_CERT_DIR") {
        if std::env::split_paths(&dirs).any(|dir| !dir.as_os_str().is_empty()) {
            overrides.push(format!("SSL_CERT_DIR={}", dirs.to_string_lossy()));
        }
    }
    (!overrides.is_empty()).then(|| overrides.join(", "))
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
        error_count: loaded.errors.len(),
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
                ..OsTrustSummary::default()
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

    #[test]
    fn env_override_follows_the_loaders_rule() {
        let lookup = |file: Option<&str>, dir: Option<&str>| {
            env_override(|name| match name {
                "SSL_CERT_FILE" => file.map(OsString::from),
                "SSL_CERT_DIR" => dir.map(OsString::from),
                _ => None,
            })
        };
        assert_eq!(lookup(None, None), None);
        // Empty directory entries are dropped by the loader, which then reads
        // the platform store, so they are not an override.
        assert_eq!(lookup(None, Some("")), None);
        assert_eq!(lookup(None, Some(":")), None);
        assert_eq!(
            lookup(Some("/a.pem"), None).as_deref(),
            Some("SSL_CERT_FILE=/a.pem")
        );
        assert_eq!(
            lookup(None, Some("/certs")).as_deref(),
            Some("SSL_CERT_DIR=/certs")
        );
        assert_eq!(
            lookup(Some("/a.pem"), Some("/certs")).as_deref(),
            Some("SSL_CERT_FILE=/a.pem, SSL_CERT_DIR=/certs")
        );
    }

    #[test]
    fn summary_names_what_was_loaded_and_what_went_wrong() {
        let plain = OsTrustSummary {
            found: 140,
            added: 139,
            ..OsTrustSummary::default()
        };
        assert_eq!(
            plain.to_string(),
            "139 of 140 OS trust store certificates added"
        );

        let stale_override = OsTrustSummary {
            first_error: Some("not found".into()),
            error_count: 3,
            env_override: Some("SSL_CERT_FILE=/gone.pem".into()),
            ..OsTrustSummary::default()
        };
        assert_eq!(
            stale_override.to_string(),
            "0 of 0 certificates from SSL_CERT_FILE=/gone.pem added, in place of the OS \
             trust store; error reading the store: not found (+2 more)"
        );
    }
}
