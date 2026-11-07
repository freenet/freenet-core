//! Native app grants: a user-approved, durable, revocable credential that lets
//! a non-browser client (for example a desktop app) act as a given web app's
//! identity when it talks to delegates. This is the first "non-browser client"
//! piece of #5264.
//!
//! A browser tab gets its app identity from the shell page the node serves for
//! that app. A native client has no shell page, so it asks the node over plain
//! HTTP, the user approves in the browser, and the node hands the client a
//! token. A WebSocket connection presenting the token is then treated exactly
//! like a browser connection for that app: delegate requests carry
//! `MessageOrigin::WebApp(<app>)`.
//!
//! What is stored: only the SHA-256 of the token, keyed to the grant record.
//! The token itself is held in the node's memory from approval until the
//! request it answers expires, the token is first used to connect, or the
//! grant is revoked, whichever is first, so a client whose poll response was
//! lost can collect it again. After that it exists only in the client.
//!
//! The approval flow and its gates live in `server::client_api::app_grants`.
//!
//! What this does NOT protect against: a malicious program already running as
//! the same user. The approval step defends against web content and gives the
//! user a visible, revocable record; it is not a boundary between local
//! processes of the same user. Closing that needs the authentication half of
//! #5264.

use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use super::{AppIdentity, DelegateCapabilities};

/// Upper bound on stored native grants. Creating one more is refused until the
/// user revokes one; a real user approves a handful.
pub(crate) const MAX_NATIVE_GRANTS: usize = 64;

/// Longest client name kept, in characters. The name is display-only and is
/// never presented as verified.
pub(crate) const MAX_CLIENT_NAME_CHARS: usize = 64;

/// Longest client name kept, in UTF-8 bytes. Below the record's one-byte
/// length field (255), so a stored name is never cut inside a character.
pub(crate) const MAX_CLIENT_NAME_BYTES: usize = 192;

/// A refresh of `last_used_at` is written at most this often per grant, so a
/// client that reconnects in a loop does not turn every connect into a write.
const LAST_USED_WRITE_INTERVAL_MS: u64 = 60 * 60 * 1000;

const RECORD_VERSION: u8 = 1;

/// The bearer credential handed to a native client.
///
/// 32 bytes from the OS entropy pool, base58-encoded. `Debug` is redacted so the
/// token cannot reach a log through a `?token` field; the buffer is wiped on
/// drop.
#[derive(Clone)]
pub(crate) struct GrantToken(Zeroizing<String>);

impl GrantToken {
    /// Mint a fresh token.
    ///
    /// Uses `OsRng`, not `GlobalRng`: this is a durable credential, so it must
    /// not come from the simulation RNG, whose seed tests fix for
    /// reproducibility (`.claude/rules/code-style.md`, the OsRng exception for
    /// key material). Called once per user approval, never on a hot path.
    pub(crate) fn generate() -> Self {
        use chacha20poly1305::aead::{OsRng, rand_core::RngCore};
        let mut bytes = Zeroizing::new([0u8; 32]);
        OsRng.fill_bytes(bytes.as_mut_slice());
        Self(Zeroizing::new(bs58::encode(bytes.as_slice()).into_string()))
    }

    /// The token as the client must present it (`Authorization: Bearer ...`).
    pub(crate) fn expose(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn hash(&self) -> GrantTokenHash {
        GrantTokenHash::of(self.expose())
    }
}

impl std::fmt::Debug for GrantToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GrantToken(<redacted>)")
    }
}

/// SHA-256 of a grant token: the storage key, and the id the dashboard uses to
/// name a grant for revocation. Not a secret: it cannot be presented in place
/// of the token.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct GrantTokenHash(pub(crate) [u8; 32]);

impl GrantTokenHash {
    pub(crate) fn of(token: &str) -> Self {
        Self(Sha256::digest(token.as_bytes()).into())
    }

    pub(crate) fn to_hex(self) -> String {
        hex::encode(self.0)
    }

    pub(crate) fn from_hex(s: &str) -> Option<Self> {
        let bytes = hex::decode(s).ok()?;
        Some(Self(bytes.try_into().ok()?))
    }
}

/// One stored grant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NativeGrant {
    pub app: AppIdentity,
    /// The name the requesting program gave itself. Display only, never
    /// verified, and always labelled as such.
    pub client_name: String,
    pub created_at_ms: u64,
    pub last_used_at_ms: u64,
}

impl NativeGrant {
    fn encode(&self) -> Vec<u8> {
        // Names come through `sanitize_client_name`, which keeps them under
        // the length field; still never cut inside a UTF-8 character.
        let mut name_len = self.client_name.len().min(u8::MAX as usize);
        while !self.client_name.is_char_boundary(name_len) {
            name_len -= 1;
        }
        let name = self.client_name.as_bytes();
        let mut out = Vec::with_capacity(1 + AppIdentity::ENCODED_LEN + 16 + 1 + name_len);
        out.push(RECORD_VERSION);
        out.extend_from_slice(&self.app.encode());
        out.extend_from_slice(&self.created_at_ms.to_be_bytes());
        out.extend_from_slice(&self.last_used_at_ms.to_be_bytes());
        out.push(name_len as u8);
        out.extend_from_slice(&name[..name_len]);
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let (&version, rest) = bytes.split_first()?;
        if version != RECORD_VERSION {
            return None;
        }
        let (app, rest) = rest.split_at_checked(AppIdentity::ENCODED_LEN)?;
        let app = AppIdentity::decode(app)?;
        let (created, rest) = rest.split_at_checked(8)?;
        let (last_used, rest) = rest.split_at_checked(8)?;
        let (&name_len, rest) = rest.split_first()?;
        let name = rest.get(..name_len as usize)?;
        Some(Self {
            app,
            client_name: String::from_utf8_lossy(name).into_owned(),
            created_at_ms: u64::from_be_bytes(created.try_into().ok()?),
            last_used_at_ms: u64::from_be_bytes(last_used.try_into().ok()?),
        })
    }
}

/// Unicode format characters (general category Cf) that can reorder or hide
/// surrounding text: bidi embeddings, overrides and isolates, zero-width
/// characters, and similar. Stripped from a requester-supplied name so it
/// cannot visually rearrange the "not verified" text displayed beside it.
fn is_format_char(c: char) -> bool {
    matches!(c as u32,
        0x00AD
        | 0x0600..=0x0605
        | 0x061C
        | 0x06DD
        | 0x070F
        | 0x0890..=0x0891
        | 0x08E2
        | 0x180E
        | 0x200B..=0x200F
        | 0x202A..=0x202E
        | 0x2060..=0x2064
        | 0x2066..=0x206F
        | 0xFEFF
        | 0xFFF9..=0xFFFB
        | 0x110BD
        | 0x110CD
        | 0x13430..=0x1343F
        | 0x1BCA0..=0x1BCA3
        | 0x1D173..=0x1D17A
        | 0xE0001
        | 0xE0020..=0xE007F)
}

/// Keep a requester-supplied name short and printable: control and format
/// characters are removed. It is still untrusted text: callers must escape it
/// for HTML.
pub(crate) fn sanitize_client_name(name: &str) -> String {
    let mut out = String::new();
    let mut chars = 0;
    for c in name.chars() {
        // Line and paragraph separators (U+2028, U+2029) are not control
        // characters but break the line the name is shown on.
        if c.is_control() || is_format_char(c) || matches!(c, '\u{2028}' | '\u{2029}') {
            continue;
        }
        if chars == MAX_CLIENT_NAME_CHARS || out.len() + c.len_utf8() > MAX_CLIENT_NAME_BYTES {
            break;
        }
        out.push(c);
        chars += 1;
    }
    out.trim().to_string()
}

impl DelegateCapabilities {
    /// Store a new grant for `token_hash`. `Ok(false)` when
    /// [`MAX_NATIVE_GRANTS`] grants already exist; the count and the insert
    /// are one storage transaction, so concurrent callers cannot exceed it.
    /// Idempotent: a grant already stored under `token_hash` is kept as it is
    /// and reported as stored, whatever the count.
    pub(crate) fn create_native_grant(
        &self,
        token_hash: GrantTokenHash,
        app: AppIdentity,
        client_name: &str,
    ) -> anyhow::Result<bool> {
        let now = self.now_ms();
        let grant = NativeGrant {
            app,
            client_name: sanitize_client_name(client_name),
            created_at_ms: now,
            last_used_at_ms: now,
        };
        self.storage
            .insert_native_grant_capped(&token_hash.0, &grant.encode(), MAX_NATIVE_GRANTS)
    }

    /// Whether a grant is stored for `token_hash`. A plain read: no refresh.
    pub(crate) fn has_native_grant(&self, token_hash: GrantTokenHash) -> anyhow::Result<bool> {
        Ok(self.storage.get_native_grant(&token_hash.0)?.is_some())
    }

    /// Number of stored grants.
    pub(crate) fn native_grant_count(&self) -> anyhow::Result<usize> {
        Ok(self.storage.all_native_grants()?.len())
    }

    /// The grant a presented token names, if any. Refreshes `last_used_at`
    /// at most once per [`LAST_USED_WRITE_INTERVAL_MS`].
    ///
    /// The refresh is a conditional update of the EXISTING row in one write
    /// transaction. It must never be a plain write of the record read above: a
    /// revocation landing between that read and the write would be undone, and
    /// the revoked token would work again. A failed refresh is logged and does
    /// not refuse the connection.
    pub(crate) fn native_grant_for(
        &self,
        token_hash: GrantTokenHash,
    ) -> anyhow::Result<Option<NativeGrant>> {
        let Some(bytes) = self.storage.get_native_grant(&token_hash.0)? else {
            return Ok(None);
        };
        // An unreadable record (corruption, or a newer record version read by
        // an older build) is an ERROR, not "no grant": the token may be valid,
        // and "not found" would tell the client to discard it.
        let Some(grant) = NativeGrant::decode(&bytes) else {
            tracing::warn!("Unreadable native app grant record");
            anyhow::bail!("unreadable native app grant record");
        };
        let now = self.now_ms();
        if now.saturating_sub(grant.last_used_at_ms) >= LAST_USED_WRITE_INTERVAL_MS {
            let refreshed =
                self.storage
                    .update_native_grant_if_present(&token_hash.0, &mut |current| {
                        let mut g = NativeGrant::decode(current)?;
                        g.last_used_at_ms = now;
                        Some(g.encode())
                    });
            if let Err(e) = refreshed {
                tracing::debug!(error = %e, "Could not refresh a native app grant's last use");
            }
        }
        Ok(Some(grant))
    }

    /// Delete a grant. `true` if it existed.
    pub(crate) fn revoke_native_grant(&self, token_hash: GrantTokenHash) -> anyhow::Result<bool> {
        self.storage.remove_native_grant(&token_hash.0)
    }

    /// Every readable stored grant, oldest first (tests). The Apps page uses
    /// [`Self::native_grant_listing`], which also lists unreadable rows.
    #[cfg(test)]
    pub(crate) fn native_grants(&self) -> anyhow::Result<Vec<(GrantTokenHash, NativeGrant)>> {
        let mut out: Vec<_> = self
            .storage
            .all_native_grants()?
            .into_iter()
            .filter_map(|(hash, bytes)| {
                NativeGrant::decode(&bytes).map(|g| (GrantTokenHash(hash), g))
            })
            .collect();
        out.sort_by_key(|(_, g)| g.created_at_ms);
        Ok(out)
    }

    /// Every stored row for the Apps and permissions page, oldest first, with
    /// rows this build cannot decode listed as `None` at the end. Those still
    /// count toward [`MAX_NATIVE_GRANTS`] and still make their token read as
    /// "unavailable" (4503), so the user must be able to see and revoke them;
    /// the hash, the table key, is all a revocation needs.
    pub(crate) fn native_grant_listing(
        &self,
    ) -> anyhow::Result<Vec<(GrantTokenHash, Option<NativeGrant>)>> {
        let mut out: Vec<_> = self
            .storage
            .all_native_grants()?
            .into_iter()
            .map(|(hash, bytes)| (GrantTokenHash(hash), NativeGrant::decode(&bytes)))
            .collect();
        out.sort_by_key(|(_, g)| match g {
            Some(g) => (0, g.created_at_ms),
            None => (1, 0),
        });
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::delegate_capabilities::{
        CapabilityStorage, FlakyCapabilityStorage, MemoryCapabilityStorage,
    };
    use crate::util::time_source::SharedMockTimeSource;
    use freenet_stdlib::prelude::ContractInstanceId;
    use std::sync::Arc;

    fn app(n: u8) -> AppIdentity {
        AppIdentity::WebApp(ContractInstanceId::new([n; 32]))
    }

    #[test]
    fn token_is_32_random_bytes_and_debug_is_redacted() {
        let a = GrantToken::generate();
        let b = GrantToken::generate();
        assert_ne!(a.expose(), b.expose());
        let raw = bs58::decode(a.expose()).into_vec().unwrap();
        assert_eq!(raw.len(), 32);
        let dbg = format!("{a:?} {:?}", Some(&a));
        assert!(!dbg.contains(a.expose()), "Debug leaked the token: {dbg}");
        assert!(dbg.contains("redacted"));
    }

    #[test]
    fn hash_is_sha256_of_the_presented_string() {
        let t = GrantToken::generate();
        let expected: [u8; 32] = Sha256::digest(t.expose().as_bytes()).into();
        assert_eq!(t.hash().0, expected);
        assert_eq!(GrantTokenHash::of(t.expose()), t.hash());
        let hex = t.hash().to_hex();
        assert_eq!(GrantTokenHash::from_hex(&hex), Some(t.hash()));
        assert_eq!(GrantTokenHash::from_hex("abcd"), None);
    }

    #[test]
    fn record_round_trips_and_rejects_garbage() {
        let g = NativeGrant {
            app: app(3),
            client_name: "River desktop".into(),
            created_at_ms: 1,
            last_used_at_ms: 2,
        };
        assert_eq!(NativeGrant::decode(&g.encode()), Some(g.clone()));
        let bytes = g.encode();
        assert_eq!(NativeGrant::decode(&bytes[..bytes.len() - 1]), None);
        assert_eq!(NativeGrant::decode(&[]), None);
        let mut wrong_version = bytes.clone();
        wrong_version[0] = 9;
        assert_eq!(NativeGrant::decode(&wrong_version), None);
    }

    #[test]
    fn client_name_is_trimmed_and_stripped_of_controls() {
        assert_eq!(sanitize_client_name("  a\u{7}b\n "), "ab");
        assert_eq!(
            sanitize_client_name(&"x".repeat(500)).chars().count(),
            MAX_CLIENT_NAME_CHARS
        );
    }

    #[test]
    fn client_name_is_capped_by_bytes_at_a_char_boundary() {
        let wide = "\u{1F600}".repeat(100); // 4 bytes each
        let s = sanitize_client_name(&wide);
        assert!(s.len() <= MAX_CLIENT_NAME_BYTES, "{}", s.len());
        assert_eq!(s.len() % 4, 0, "no partial character");
        assert!(sanitize_client_name("a\u{2028}b\u{2029}c") == "abc");
        // A record whose name exceeds the length field still decodes intact.
        let g = NativeGrant {
            app: app(1),
            client_name: "\u{e9}".repeat(200),
            created_at_ms: 0,
            last_used_at_ms: 0,
        };
        let back = NativeGrant::decode(&g.encode()).unwrap();
        assert!(back.client_name.chars().all(|c| c == '\u{e9}'));
        assert_eq!(back.client_name.len(), 254);
    }

    #[test]
    fn client_name_loses_format_characters() {
        // Right-to-left override, isolates, zero-width space, BOM.
        assert_eq!(
            sanitize_client_name("a\u{202E}b\u{2066}c\u{2069}d\u{200B}e\u{FEFF}"),
            "abcde"
        );
        assert!(is_format_char('\u{E0041}'));
        assert!(!is_format_char('a'));
        assert!(!is_format_char('\u{00E9}'));
    }

    fn mock_caps(
        storage: Arc<dyn CapabilityStorage>,
    ) -> (Arc<DelegateCapabilities>, SharedMockTimeSource) {
        let time = SharedMockTimeSource::new();
        let c = DelegateCapabilities::with_time_source(
            storage,
            Arc::new(time.clone()),
            crate::contract::delegate_capabilities::BudgetLimits::default(),
        );
        (c, time)
    }

    #[test]
    fn last_used_is_refreshed_after_the_interval() {
        let (c, time) = mock_caps(Arc::new(MemoryCapabilityStorage::default()));
        let t = GrantToken::generate();
        assert!(c.create_native_grant(t.hash(), app(1), "x").unwrap());
        let created = c.native_grants().unwrap()[0].1.created_at_ms;
        time.advance_time(std::time::Duration::from_millis(
            LAST_USED_WRITE_INTERVAL_MS + 1,
        ));
        c.native_grant_for(t.hash()).unwrap().unwrap();
        let g = &c.native_grants().unwrap()[0].1;
        assert!(g.last_used_at_ms > created, "{g:?}");
    }

    /// A revocation landing between the lookup's read and its last-used
    /// refresh must stay revoked. With a plain write-back of the record read,
    /// the refresh recreated the row and the revoked token worked again.
    #[test]
    fn revocation_between_read_and_refresh_is_not_undone() {
        let storage = Arc::new(FlakyCapabilityStorage::default());
        let (c, time) = mock_caps(storage.clone());
        let t = GrantToken::generate();
        assert!(c.create_native_grant(t.hash(), app(1), "x").unwrap());
        // Old enough that the lookup will try to refresh it.
        time.advance_time(std::time::Duration::from_millis(
            LAST_USED_WRITE_INTERVAL_MS + 1,
        ));
        let hash = t.hash();
        *storage.after_native_get.lock() = Some(Box::new(move |inner| {
            assert!(inner.remove_native_grant(&hash.0).unwrap());
        }));
        // This lookup read the row before the revocation, so it may still
        // answer with it; what matters is what is left in storage.
        drop(c.native_grant_for(t.hash()).unwrap());
        assert!(storage.after_native_get.lock().is_none(), "the hook ran");
        assert!(c.native_grants().unwrap().is_empty(), "revocation undone");
        assert_eq!(c.native_grant_for(t.hash()).unwrap(), None);
    }

    /// An unreadable record is an error (the caller reports "try again"), not
    /// a missing grant (which would tell the client to discard its token).
    #[test]
    fn unreadable_record_is_an_error_not_absent() {
        let storage = Arc::new(MemoryCapabilityStorage::default());
        let c = DelegateCapabilities::new(storage.clone());
        let t = GrantToken::generate();
        assert!(
            storage
                .insert_native_grant_capped(&t.hash().0, b"\x09not a record", 64)
                .unwrap()
        );
        assert!(c.native_grant_for(t.hash()).is_err());
        assert_eq!(
            c.native_grant_for(GrantToken::generate().hash()).unwrap(),
            None
        );
    }

    #[test]
    fn create_lookup_revoke() {
        let storage = Arc::new(MemoryCapabilityStorage::default());
        let c = DelegateCapabilities::new(storage.clone());
        let t = GrantToken::generate();
        assert!(
            c.create_native_grant(t.hash(), app(1), "River desktop")
                .unwrap()
        );
        let g = c.native_grant_for(t.hash()).unwrap().unwrap();
        assert_eq!(g.app, app(1));
        assert_eq!(g.client_name, "River desktop");
        // Only the hash is a key; the raw token appears nowhere in storage.
        for (k, v) in storage.all_native_grants().unwrap() {
            assert_eq!(k, t.hash().0);
            let needle = t.expose().as_bytes();
            assert!(!v.windows(needle.len()).any(|w| w == needle));
        }
        assert!(c.revoke_native_grant(t.hash()).unwrap());
        assert!(!c.revoke_native_grant(t.hash()).unwrap());
        assert_eq!(c.native_grant_for(t.hash()).unwrap(), None);
    }

    #[test]
    fn creation_is_capped() {
        let c = DelegateCapabilities::new(Arc::new(MemoryCapabilityStorage::default()));
        for _ in 0..MAX_NATIVE_GRANTS {
            assert!(
                c.create_native_grant(GrantToken::generate().hash(), app(1), "x")
                    .unwrap()
            );
        }
        assert!(
            !c.create_native_grant(GrantToken::generate().hash(), app(1), "x")
                .unwrap()
        );
        assert_eq!(c.native_grant_count().unwrap(), MAX_NATIVE_GRANTS);
        assert_eq!(c.native_grants().unwrap().len(), MAX_NATIVE_GRANTS);
    }

    /// The grant (by token hash) survives a node restart: the redb table is
    /// reopened from disk, and the token still resolves to its app.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn grant_survives_a_reopen_and_only_the_hash_is_on_disk() {
        let dir = crate::util::tests::get_temp_dir();
        let token = GrantToken::generate();
        {
            let db = crate::contract::storages::Storage::new(dir.path())
                .await
                .unwrap();
            let c = DelegateCapabilities::new(Arc::new(db));
            c.create_native_grant(token.hash(), app(7), "River desktop")
                .unwrap();
        }
        let db = crate::contract::storages::Storage::new(dir.path())
            .await
            .unwrap();
        let rows = db.load_all_native_app_grants().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, token.hash().0);
        let needle = token.expose().as_bytes();
        assert!(!rows[0].1.windows(needle.len()).any(|w| w == needle));
        let c = DelegateCapabilities::new(Arc::new(db));
        let g = c.native_grant_for(token.hash()).unwrap().unwrap();
        assert_eq!(g.app, app(7));
        // Nothing else resolves.
        assert_eq!(
            c.native_grant_for(GrantToken::generate().hash()).unwrap(),
            None
        );
    }

    /// Storage-level contract of the redb table: the capped insert, the
    /// conditional update, idempotence, and persistence across a reopen.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn redb_capped_insert_and_conditional_update() {
        let dir = crate::util::tests::get_temp_dir();
        let a = [1u8; 32];
        let b = [2u8; 32];
        let c = [3u8; 32];
        {
            let db = crate::contract::storages::Storage::new(dir.path())
                .await
                .unwrap();
            assert!(db.insert_native_app_grant_capped(&a, b"one", 2).unwrap());
            assert!(db.insert_native_app_grant_capped(&b, b"two", 2).unwrap());
            // At the cap: refused, and nothing written.
            assert!(!db.insert_native_app_grant_capped(&c, b"three", 2).unwrap());
            assert_eq!(db.get_native_app_grant(&c).unwrap(), None);
            assert_eq!(db.load_all_native_app_grants().unwrap().len(), 2);
            // A key already present is stored, even at the cap, and kept as is.
            assert!(db.insert_native_app_grant_capped(&a, b"other", 2).unwrap());
            assert_eq!(
                db.get_native_app_grant(&a).unwrap().as_deref(),
                Some(&b"one"[..])
            );
            assert_eq!(db.load_all_native_app_grants().unwrap().len(), 2);
            // Update of an absent key writes nothing.
            let mut called = false;
            db.update_native_app_grant_if_present(&c, &mut |_| {
                called = true;
                Some(b"made up".to_vec())
            })
            .unwrap();
            assert!(!called);
            assert_eq!(db.get_native_app_grant(&c).unwrap(), None);
            // Update of a present key persists.
            db.update_native_app_grant_if_present(&b, &mut |old| {
                assert_eq!(old, b"two");
                Some(b"two v2".to_vec())
            })
            .unwrap();
            assert_eq!(
                db.get_native_app_grant(&b).unwrap().as_deref(),
                Some(&b"two v2"[..])
            );
            // `None` from the closure leaves the row alone.
            db.update_native_app_grant_if_present(&b, &mut |_| None)
                .unwrap();
            assert!(db.remove_native_app_grant(&a).unwrap());
            assert!(!db.remove_native_app_grant(&a).unwrap());
        }
        let db = crate::contract::storages::Storage::new(dir.path())
            .await
            .unwrap();
        let rows = db.load_all_native_app_grants().unwrap();
        assert_eq!(rows, vec![(b, b"two v2".to_vec())]);
    }
}
