use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;

pub const HEADER_SIGNATURE: &str = "x-signature";

#[derive(thiserror::Error, Debug)]
pub enum AuthError {
    #[error("X-Signature header is not valid hex")]
    BadSignatureHeader,
    #[error("signature mismatch")]
    BadSignature,
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
    mac.verify_slice(&expected)
        .map_err(|_| AuthError::BadSignature)
}

pub fn check_clock_skew(issued_at: i64, tolerance_seconds: i64) -> Result<(), AuthError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let delta = (now - issued_at).abs();
    if delta > tolerance_seconds {
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
        // valid hex, but wrong number of bytes for SHA-256 output
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
}
