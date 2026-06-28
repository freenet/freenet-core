//! Delegate secrets-at-rest storage.
//!
//! Split into focused submodules; this file is the module root and re-exports
//! the complete public API so every external `use super::secrets_store::…`
//! path continues to resolve unchanged.
//!
//! # Submodule layout
//!
//! - `user`: [`UserId`], [`SecretScope`], [`UserSecretContext`], token
//!   derivation helpers (`user_id`, `user_dek_secret`),
//!   `DEFAULT_LAST_SEEN_DEBOUNCE_SECS`
//! - `quota`: per-user byte quota tracking (`QuotaTracker`,
//!   `USER_QUOTA_TRACKER`, `apply_signed_delta`, `QuotaCommit`),
//!   quota consts, `#[cfg(test)]` quota test helpers
//! - `sweep`: filesystem permission helpers (`create_owner_only`,
//!   `ensure_owner_only_dir`), inactive-user TTL reclaim
//!   (`stamp_user_last_seen`, `reclaim_inactive_users`,
//!   `should_spawn_inactive_user_sweep`, `spawn_inactive_user_sweep`,
//!   `SweepOutcome`)
//! - `store`: [`SecretStoreError`], [`SecretsStore`] struct + impl,
//!   [`ExportSecretEntry`], [`ExportScopeError`],
//!   `decrypt_secret_blob`, `log_legacy_decrypt`, and the
//!   `#[cfg(test)]` test module

mod quota;
mod store;
mod sweep;
mod user;

// ── Public re-exports ─────────────────────────────────────────────────────────
// Keep every path that existed before the split resolving identically.

// Intentional public re-exports of the full API surface. Some items are only
// used from external callers or tests; the allow suppresses spurious
// unused-import warnings that the compiler emits for re-exported items with no
// direct use site in this compilation unit.
#[allow(unused_imports)]
pub use user::{
    DEFAULT_LAST_SEEN_DEBOUNCE_SECS, SecretScope, UserId, UserSecretContext, user_dek_secret,
    user_id,
};

#[allow(unused_imports)]
pub use quota::{
    DEFAULT_PER_USER_INACTIVE_TTL_SECS, DEFAULT_PER_USER_SECRET_QUOTA_BYTES,
    MAX_RECLAIMS_PER_SWEEP, SWEEP_MAX_GAP_INTERVAL_MULTIPLE,
};

#[allow(unused_imports)]
pub use sweep::{
    SweepOutcome, reclaim_inactive_users, should_spawn_inactive_user_sweep,
    spawn_inactive_user_sweep, stamp_user_last_seen, wall_clock_unix_secs,
};

pub use store::{ExportScopeError, ExportSecretEntry, SecretStoreError, SecretsStore};

// ── pub(super) re-exports ─────────────────────────────────────────────────────
// `secret_snapshots` and other wasm_runtime siblings import these.

pub(super) use sweep::{create_owner_only, ensure_owner_only_dir};
