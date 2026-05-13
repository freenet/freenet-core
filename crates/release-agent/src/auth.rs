use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::HeaderName;
use hmac::{Hmac, Mac};
use sha2::Sha256;

/// HTTP header carrying the request signature. `HeaderMap::get` is
/// case-insensitive over the wire; using a `HeaderName` constant makes the
/// contract explicit and avoids re-parsing on every lookup.
pub const HEADER_SIGNATURE: HeaderName = HeaderName::from_static("x-signature");

#[derive(thiserror::Error, Debug)]
pub enum AuthError {
    #[error("X-Signature header is not valid hex")]
    BadSignatureHeader,
    #[error("signature mismatch")]
    BadSignature,
    #[error("issued_at must be a positive Unix timestamp")]
    NonPositiveIssuedAt,
    #[error("clock skew too large: |now - issued_at| = {0}s")]
    ClockSkew(i64),
    #[error("hmac key error: {0}")]
    KeyError(String),
}

pub fn verify_signature(secret: &[u8], body: &[u8], header_value: &str) -> Result<(), AuthError> {
    let expected = hex::decode(header_value).map_err(|_| AuthError::BadSignatureHeader)?;
    let mut mac =
        <Hmac<Sha256>>::new_from_slice(secret).map_err(|e| AuthError::KeyError(e.to_string()))?;
    mac.update(body);
    // `verify_slice` is constant-time on equal-length inputs and short-
    // circuits on length mismatch. SHA-256 output is fixed-size, so the
    // length-leak gives an attacker no useful information.
    mac.verify_slice(&expected)
        .map_err(|_| AuthError::BadSignature)
}

/// Reject requests whose `issued_at` is non-positive or outside ±tolerance
/// of the agent's wall clock. Uses checked arithmetic so an attacker who
/// produces a valid signature cannot panic the agent with `i64::MIN`.
///
/// `tolerance_seconds` is unsigned so a misconfigured negative value
/// can't accidentally reject every request.
pub fn check_clock_skew(issued_at: i64, tolerance_seconds: u32) -> Result<(), AuthError> {
    if issued_at <= 0 {
        return Err(AuthError::NonPositiveIssuedAt);
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let delta = now
        .checked_sub(issued_at)
        .and_then(i64::checked_abs)
        .unwrap_or(i64::MAX);
    if delta > tolerance_seconds as i64 {
        return Err(AuthError::ClockSkew(delta));
    }
    Ok(())
}

pub fn sign(secret: &[u8], body: &[u8]) -> String {
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_round_trip() {
        let secret = b"super-secret-shared-key-32-bytes!!!";
        let body = br#"{"version":"0.2.56","issued_at":1715300000}"#;
        let sig = sign(secret, body);
        verify_signature(secret, body, &sig).expect("valid signature accepted");
    }

    #[test]
    fn signature_rejects_tamper() {
        let secret = b"super-secret-shared-key-32-bytes!!!";
        let body = br#"{"version":"0.2.56","issued_at":1715300000}"#;
        let sig = sign(secret, body);
        let tampered = br#"{"version":"0.2.99","issued_at":1715300000}"#;
        assert!(matches!(
            verify_signature(secret, tampered, &sig),
            Err(AuthError::BadSignature)
        ));
    }

    #[test]
    fn signature_rejects_wrong_secret() {
        let body = b"payload";
        let sig = sign(b"secret-A-padded-to-thirty-two-byt", body);
        assert!(matches!(
            verify_signature(b"secret-B-padded-to-thirty-two-byt", body, &sig),
            Err(AuthError::BadSignature)
        ));
    }

    #[test]
    fn signature_rejects_malformed_hex() {
        assert!(matches!(
            verify_signature(b"k", b"body", "not-hex!!"),
            Err(AuthError::BadSignatureHeader)
        ));
    }

    #[test]
    fn signature_rejects_wrong_length_hex() {
        assert!(matches!(
            verify_signature(b"k", b"body", "deadbeef"),
            Err(AuthError::BadSignature)
        ));
    }

    #[test]
    fn clock_skew_within_tolerance() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        check_clock_skew(now - 30, 300).unwrap();
        check_clock_skew(now + 30, 300).unwrap();
    }

    #[test]
    fn clock_skew_outside_tolerance_rejected() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        assert!(matches!(
            check_clock_skew(now - 3600, 300),
            Err(AuthError::ClockSkew(_))
        ));
    }

    #[test]
    fn issued_at_zero_rejected() {
        assert!(matches!(
            check_clock_skew(0, 300),
            Err(AuthError::NonPositiveIssuedAt)
        ));
    }

    #[test]
    fn issued_at_negative_rejected() {
        assert!(matches!(
            check_clock_skew(-1, 300),
            Err(AuthError::NonPositiveIssuedAt)
        ));
    }

    #[test]
    fn issued_at_i64_min_does_not_panic() {
        // Regression: previously `.abs()` on i64::MIN panicked.
        assert!(matches!(
            check_clock_skew(i64::MIN, 300),
            Err(AuthError::NonPositiveIssuedAt)
        ));
    }
}
