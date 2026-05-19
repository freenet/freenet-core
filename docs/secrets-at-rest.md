# Delegate Secrets at Rest

Server-side delegate secret encryption evolved across PRs #4143 / #4144
/ #4146 (tracker #4137). This document describes the current model and
operator-facing migration guidance.

## Model

```
node KEK (32 bytes, in OS keyring / systemd cred / file)
  └── per-delegate DEK = HKDF-SHA256(KEK, salt=delegate_key.encode(),
                                          info="freenet-delegate-dek-v1")
        └── per-write random nonce (24 bytes, OsRng — PR #4143)
```

The KEK is provisioned on first start by the resolver in
`crates/core/src/config/kek.rs`, which tries OS keyring → systemd
credential → file backend in order and persists the choice in
`secrets_dir/kek_backend`. Subsequent starts load STRICTLY from the
recorded backend — transient keyring outage is a hard error, never a
silent demotion.

## Operator migration matrix

| Source release           | Cipher source on disk                  | Status after PR #4146 |
|--------------------------|----------------------------------------|------------------------|
| ≤ 0.2.58 (pre-#4143)     | `LEGACY_DEFAULT_CIPHER` (world-known)  | Decryptable via legacy-migration tier. |
| 0.2.59 (#4143)           | `LEGACY_DEFAULT_CIPHER`, versioned     | Decryptable via legacy-migration tier. |
| 0.2.60+ (#4144)          | per-install random `delegate_cipher`   | Decryptable via `default_encryption` (legacy_chain[0]). |
| post-#4146               | HKDF-derived DEK from node KEK         | Default path. |

## BREAKING semantic change introduced by #4146

`RegisterDelegate { cipher, nonce }` server-side semantics changed:
client-supplied `cipher` and `nonce` are now IGNORED (logged at INFO).
The wire format is unchanged so older clients continue to function,
but **any prior deployment that wrote secrets under a client-supplied
non-default cipher will lose access to those blobs** unless that
cipher coincidentally matches the new HKDF-derived DEK (it won't).

Deployments that only ever used `DelegateRequest::DEFAULT_CIPHER` or
the per-install auto-generated cipher are covered by the legacy chain
and migrate transparently.

If you ran any client that explicitly constructed `RegisterDelegate {
cipher: custom, .. }` against a freenet-core ≤ 0.2.60 node, restore
the secrets dir from a pre-upgrade backup, decrypt with the custom
cipher manually (the on-disk format is documented in
`crates/core/src/wasm_runtime/secrets_store.rs::decrypt_secret_blob`),
and re-upload via the post-#4146 client API. The HKDF derivation will
produce a new DEK and the secrets will be re-encrypted under it on
first write.

## `freenet secrets` CLI

```
freenet secrets kek-status   --secrets-dir <path>
freenet secrets kek-migrate  --secrets-dir <path> --to {keyring|systemd|file} --yes
freenet secrets kek-rotate   --secrets-dir <path> --yes   # NOT YET IMPLEMENTED (#4137)
```

Node MUST be stopped before running migrate. `kek-rotate` currently
bails with a clear error pointing operators at the temporary
kek-migrate workflow; crash-safe two-phase rotation (`.rot` shadow
files + recovery on next start) is tracked as a follow-up under #4137.
