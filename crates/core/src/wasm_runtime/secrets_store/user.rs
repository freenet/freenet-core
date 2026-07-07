//! Per-user identity and secret-scope types for the hosted-mode (P2 of #4381)
//! secret namespace: [`UserId`], [`SecretScope`], [`UserSecretContext`], and
//! the token-to-id/dek-secret derivation helpers.

use zeroize::Zeroizing;

/// Default minimum interval between rewrites of a user's `.last_seen` marker on
/// the hot request path (#4561). A stamp older than this is refreshed; a newer
/// one is left untouched, so the steady-state per-request cost is a single
/// `stat` (no write) — see [`super::sweep::stamp_user_last_seen`]. One hour is far below the
/// 30-day TTL, so the marker can never go more than ~1h stale while a user is
/// active, which is negligible slack against a 30-day reclaim threshold.
pub const DEFAULT_LAST_SEEN_DEBOUNCE_SECS: u64 = 3_600;

/// Domain-separation prefix for [`user_id`]. Distinct from
/// [`USER_DEK_SECRET_DOMAIN`] so the same token cannot yield a user id that
/// collides with a dek-secret (and vice-versa).
const USER_ID_DOMAIN: &[u8] = b"freenet-user-id";

/// Domain-separation prefix for [`user_dek_secret`].
const USER_DEK_SECRET_DOMAIN: &[u8] = b"freenet-user-dek";

/// Identifier for a per-user secret namespace.
///
/// A `UserId` is a 32-byte opaque tag that partitions a delegate's secret
/// storage into independent per-user namespaces. In hosted mode (P2 of #4381)
/// it is derived from a connection's user token via [`user_id`] and carried in
/// a [`UserSecretContext`]; outside hosted mode no `UserId` is constructed and
/// every secret operation stays [`SecretScope::Local`].
///
/// The bytes are not secret on their own (they only name a namespace), so
/// `UserId` does NOT zeroize — unlike the `dek_secret` that travels alongside
/// it in [`SecretScope::User`], which is held in `Zeroizing`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct UserId([u8; 32]);

impl UserId {
    /// Construct a `UserId` from its raw 32 bytes.
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Raw 32-byte tag.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// bs58 (BITCOIN alphabet) encoding of the 32-byte tag, used as the
    /// on-disk path segment under `users/`. Mirrors `DelegateKey::encode`
    /// and `SecretsId::encode` so all three render consistently in paths
    /// and logs.
    pub fn encode(&self) -> String {
        bs58::encode(self.0)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
    }
}

impl std::fmt::Debug for UserId {
    /// Render as the bs58 encoding rather than a raw byte array. The tag is
    /// not secret, but the encoded form is what appears in paths and logs,
    /// so matching it keeps `{:?}` output greppable against the filesystem.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UserId({})", self.encode())
    }
}

/// Scope selector for a secret read/write/remove.
///
/// `Local` is the historical single-user path: it MUST behave byte-for-byte
/// identically to pre-#4381 code (same on-disk path, same node-KEK-derived
/// DEK, same ReDb table). `User` adds an optional per-user dimension whose
/// DEK is derived purely from a caller-provided `dek_secret` (NOT the node
/// KEK), making per-user secrets portable by design (P3 export). The `User`
/// scope is selected only in hosted mode (P2 of #4381), when a connection
/// presents a user token; the borrowed `id`/`dek_secret` come from that
/// connection's [`UserSecretContext`].
///
/// The `dek_secret` is borrowed as `&Zeroizing<[u8; 32]>` so the caller
/// retains ownership and the value is wiped when the caller drops it; the
/// store never copies it into a longer-lived buffer.
pub enum SecretScope<'a> {
    /// Single-user / node-local path. Byte-for-byte identical to pre-#4381.
    Local,
    /// Per-user path keyed by `id`, encrypted under a DEK derived solely
    /// from `dek_secret` (node-KEK-independent).
    User {
        id: &'a UserId,
        dek_secret: &'a Zeroizing<[u8; 32]>,
    },
}

/// A per-connection user secret namespace, derived ONCE at the WebSocket
/// connection boundary from a durable user token (P2 of #4381, hosted mode).
///
/// This is the owned counterpart to [`SecretScope::User`]: it holds the
/// `user_id` tag and the `dek_secret` (the latter in `Zeroizing` so it is
/// wiped on drop), and lends them out as a borrowed [`SecretScope::User`] via
/// [`Self::scope`] for the duration of a single secret operation.
///
/// # Security invariant
///
/// A `UserSecretContext` is constructed in EXACTLY ONE place — at WS
/// connection establishment, from the connection's user token (see
/// [`UserSecretContext::from_token`]). It then travels immutably with the
/// connection. Nothing reachable from a delegate's WASM, a delegate message
/// body, a `ClientRequest`, or the app contract id can construct, mutate, or
/// substitute it: the only public constructor takes the raw token bytes and
/// derives both fields deterministically via the domain-separated [`user_id`]
/// / [`user_dek_secret`] hashes. This is what makes the per-user namespace
/// unforgeable from inside the sandbox.
#[derive(Clone)]
pub struct UserSecretContext {
    user_id: UserId,
    dek_secret: Zeroizing<[u8; 32]>,
}

impl UserSecretContext {
    /// Derive a `UserSecretContext` from a connection's opaque user token.
    ///
    /// This is the ONLY constructor. Both the namespace tag and the DEK
    /// secret come solely from `token` via the domain-separated derivations,
    /// so the resulting scope cannot be influenced by anything other than the
    /// token presented at the connection boundary.
    ///
    /// The token is sensitive; this never logs it or the derived secret.
    pub fn from_token(token: &[u8]) -> Self {
        Self {
            user_id: user_id(token),
            dek_secret: user_dek_secret(token),
        }
    }

    /// The non-secret namespace tag for this user. Safe to log.
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }

    /// Borrow this context as a [`SecretScope::User`] for one secret call.
    ///
    /// The returned scope borrows `self`, so the `dek_secret` is never copied
    /// into a longer-lived buffer — it lives exactly as long as `self`.
    pub fn scope(&self) -> SecretScope<'_> {
        SecretScope::User {
            id: &self.user_id,
            dek_secret: &self.dek_secret,
        }
    }
}

impl std::fmt::Debug for UserSecretContext {
    /// Render only the non-secret `user_id`. The `dek_secret` is NEVER
    /// included so that `{:?}` on any struct that transitively holds a
    /// `UserSecretContext` (e.g. the `DelegateRequest` contract-handler event)
    /// cannot leak key material into logs.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserSecretContext")
            .field("user_id", &self.user_id)
            .field("dek_secret", &"<redacted>")
            .finish()
    }
}

/// Derive a [`UserId`] from an opaque bearer token.
///
/// `user_id(token) = blake3(USER_ID_DOMAIN || token)`. The domain prefix is
/// distinct from [`user_dek_secret`]'s so the two derivations are
/// independent: knowing a user's id reveals nothing about their dek-secret.
///
/// Consumed by [`UserSecretContext::from_token`] at the WS connection boundary
/// (P2 of #4381, hosted mode).
///
/// The token is sensitive; this function never logs it. The returned id is a
/// non-secret namespace tag, so it is not wrapped in `Zeroizing`.
pub fn user_id(token: &[u8]) -> UserId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(USER_ID_DOMAIN);
    hasher.update(token);
    UserId(*hasher.finalize().as_bytes())
}

/// Derive a per-user DEK secret (HKDF IKM) from an opaque bearer token.
///
/// `user_dek_secret(token) = blake3(USER_DEK_SECRET_DOMAIN || token)`,
/// returned in `Zeroizing` so it is wiped on drop. Distinct domain prefix
/// from [`user_id`] guarantees `user_id(token) != user_dek_secret(token)`
/// as byte strings for every token.
///
/// Consumed by [`UserSecretContext::from_token`] (see [`user_id`]). The token
/// is sensitive; this function never logs it or the derived secret.
pub fn user_dek_secret(token: &[u8]) -> Zeroizing<[u8; 32]> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(USER_DEK_SECRET_DOMAIN);
    hasher.update(token);
    Zeroizing::new(*hasher.finalize().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The user token is an OPAQUE bearer string: the namespace is
    /// `blake3(domain || raw_token_bytes)`, with NO hex decode and NO
    /// length/charset validation anywhere on the path. That is the property the
    /// shell's hex -> base58 access-key change relies on — both encodings are
    /// just different UTF-8 strings, so:
    ///
    /// - a NEW base58 token is honored exactly like a hex one (no backend change
    ///   was needed for the switch), and
    /// - an EXISTING user's stored hex token keeps mapping to the same namespace
    ///   (back-compat: their data stays reachable).
    ///
    /// This pins that contract so a future "validate the token is 32-byte hex"
    /// well-meaning tightening would fail CI instead of silently locking every
    /// pre-existing hex user out of their data.
    #[test]
    fn user_token_is_opaque_hex_and_base58_both_accepted() {
        // A representative hex token (what older shell builds minted) and a
        // representative base58 token (what new builds mint, Bitcoin alphabet).
        // Both are just strings as far as the backend is concerned.
        let hex_token = b"0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0";
        let base58_realistic = b"7Nsp3W1kZ2vQmYb8xR4tHgD6cF5aLj9UeKq";

        // Every token yields a stable, non-panicking derivation.
        for tok in [hex_token.as_slice(), base58_realistic.as_slice()] {
            let a = UserSecretContext::from_token(tok);
            let b = UserSecretContext::from_token(tok);
            assert_eq!(
                a.user_id(),
                b.user_id(),
                "same token string must derive the same namespace (existing hex \
                 users must keep their namespace across restarts/versions)"
            );
        }

        // Distinct token strings derive distinct namespaces — a hex token and a
        // base58 token never collide (they are different byte strings).
        let hex_id = user_id(hex_token);
        let b58_id = user_id(base58_realistic);
        assert_ne!(
            hex_id.as_bytes(),
            b58_id.as_bytes(),
            "different token strings must derive different namespaces"
        );

        // The derivation depends ONLY on the raw bytes: a hex string and its
        // uppercased form are DIFFERENT tokens (no normalization), which is the
        // correct behavior for an opaque bearer secret.
        let lower = b"abcdef0123456789";
        let upper = b"ABCDEF0123456789";
        assert_ne!(
            user_id(lower).as_bytes(),
            user_id(upper).as_bytes(),
            "token derivation is over raw bytes with no case/format normalization"
        );
    }
}
