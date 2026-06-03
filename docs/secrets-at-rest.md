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
`crates/core/src/config/kek.rs`. The **auto-resolver intentionally
omits the OS keyring backend** to avoid an unexpected Keychain /
Credential Manager prompt the moment an operator first runs `freenet`.
The auto-chain is therefore:

1. `systemd` credential — only active when the node was started by
   systemd with `LoadCredentialEncrypted=freenet-kek:...` on the unit
   (so the operator already opted in by configuring the unit).
2. `file` — `secrets_dir/node_kek`, 0o600. Always-available fallback;
   logs a WARN so the operator knows the KEK lives on disk.

The OS keyring backend is **opt-in**: operators who want it run
`freenet secrets kek-init --backend keyring --secrets-dir <path> --yes`
**before** the first node start. The act of running that CLI command
is the consent capture — the keyring write (and any platform
permission dialog it triggers) happens during a command the operator
explicitly invoked, not during plain `freenet` startup.

After the resolver runs, the choice is persisted in
`secrets_dir/kek_backend`. Subsequent starts load STRICTLY from the
recorded backend — transient backend outage is a hard error, never a
silent demotion.

### Platform notes (keyring backend)

| Platform | First write | Subsequent reads | After binary upgrade |
|---|---|---|---|
| macOS (signed) | silent | silent | Keychain prompt (signature changed) |
| macOS (unsigned / dev) | silent | prompts each start | prompts each start |
| Windows | silent | silent | silent |
| Linux | **not supported** in this build | — | — |

Linux note: the workspace ships `keyring 3.x` without `linux-native` /
`sync-secret-service` to avoid the `libdbus-1-dev` build dependency.
The crate would otherwise fall back to an in-process mock store that
silently accepts writes and discards them on process exit — orphaning
the `kek_backend` marker and bricking the next node start with no
recovery path. `KeyringKek::new()` therefore refuses on Linux with an
explicit `KekError::Keyring("not supported on Linux in this build…")`.
Linux operators use `--backend systemd` (with
`LoadCredentialEncrypted=`) or `--backend file`. A future release may
add an opt-in cargo feature that pulls in `linux-native` for operators
who have libdbus available.

The macOS / dev-build prompt-per-start behavior is the main reason
keyring is opt-in: every auto-update changes the binary signature, and
without opt-in every release would surface a surprise prompt. A future
PR will let an application (delegate) trigger a consent flow lazily on
first-use via the WS API, so end users can grant per-app and the keyring
becomes the obvious default again.

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
freenet secrets kek-status       --secrets-dir <path>
freenet secrets kek-init          --secrets-dir <path> --backend {keyring|systemd|file} --yes
freenet secrets kek-migrate       --secrets-dir <path> --to     {keyring|systemd|file} --yes
freenet secrets kek-rotate        --secrets-dir <path> --yes   # NOT YET IMPLEMENTED (#4137)
freenet secrets snapshot-list     --secrets-dir <path> [--delegate <key>] [--secret <id>]
freenet secrets snapshot-restore  --secrets-dir <path> --delegate <key> --secret <id> \
                                  --timestamp-ms <ms> --yes
```

`kek-init` opts in to a specific backend BEFORE first start. It refuses
to run if the backend marker already exists — use `kek-migrate` after
first start.

Node MUST be stopped before running migrate. `kek-rotate` currently
bails with a clear error pointing operators at the temporary
kek-migrate workflow; crash-safe two-phase rotation (`.rot` shadow
files + recovery on next start) is tracked as a follow-up under #4137.

### Snapshot inspect / restore (#4036)

`snapshot-list` walks the secrets tree and reports the per-secret
snapshot history created by the snapshot-on-write durability layer
(#4034). With no filter it summarises every delegate; `--delegate`
restricts to one; `--delegate X --secret Y` prints each individual
snapshot's `timestamp_ms`, UTC time, and size. It is **metadata only**
— it never decrypts or prints plaintext secret values (that would
require the node KEK and expose secrets on the terminal), and it is
read-only, so it is safe to run while the node is up (a stopped node
just gives a point-in-time-consistent view).

`snapshot-restore` rolls one secret back to the snapshot identified by
`--timestamp-ms` (copy the value from `snapshot-list`). The current
active value is snapshotted first, so the restore is itself reversible.
Restore is a byte-level copy: the restored ciphertext stays decryptable
by the same KEK-derived DEK that wrote it, so it only makes sense on the
node that owns the secrets — snapshots are **not** portable across nodes
(each node's DEK is HKDF-derived from its own KEK). The node MUST be
stopped, because restore writes the active secret file and a running
node may concurrently write the same secret. Both commands operate
purely on the on-disk secrets tree (no ReDb / KEK access needed) and
share the restore durability core (atomic tmp+fsync+rename) with the
node runtime's `SecretsStore::restore_snapshot`. `--delegate` and
`--secret` must be single path components (the bs58 ids printed by
`snapshot-list`); values containing `/` or `..` are rejected.

After a successful restore the shared core thins the history under the
default retention policy, the same as a normal write. One consequence:
if you restore from a snapshot older than the 2-year `max_age` cap, that
very old source snapshot is pruned from `.snapshots/` afterward. The
restore itself is unaffected (the value is already the active secret by
then) and stays reversible — the prior active value is captured as a
fresh snapshot before the overwrite.

## File permissions (PR #4195 / issue #4141)

The secrets tree is owner-only on Unix:

| Path                                            | Mode  |
|-------------------------------------------------|-------|
| `<secrets_dir>/`                                | 0o700 |
| `<secrets_dir>/transport_keypair`               | 0o600 |
| `<secrets_dir>/node_kek` (file backend)         | 0o600 |
| `<secrets_dir>/delegate_cipher` (legacy)        | 0o600 |
| `<secrets_dir>/kek_backend` (marker)            | 0o600 |
| `<secrets_dir>/<delegate>/`                     | 0o700 |
| `<secrets_dir>/<delegate>/<secret_id>`          | 0o600 |
| `<secrets_dir>/<delegate>/.snapshots/`          | 0o700 |
| `<secrets_dir>/<delegate>/.snapshots/<sec>/`    | 0o700 |
| `<secrets_dir>/<delegate>/.snapshots/<sec>/*`   | 0o600 |

Modes are set in the same syscall as `O_CREAT` (via
`OpenOptions::mode`), so there is no race window where a file is
readable under the process umask.

### Defense-in-depth: startup umask (PR follow-up to #4196)

In addition to the per-call-site `OpenOptions::mode(0o600)`, the
`freenet` binary calls `umask(0o077)` at process startup, before the
tokio runtime spawns any worker thread. This is the belt-and-suspenders
companion: if a future contributor adds a plain `File::create` in the
secrets subsystem and forgets to route it through the
`create_owner_only` helper, the file still lands at `0o600` (and any
created directory at `0o700`) because the umask itself masks off all
group and other bits.

Operators who configured a custom `umask` for the freenet service
(e.g. `UMask=0027` in the systemd unit, or a shell `umask` in a wrapper
script) **will see their setting overridden** — the node always
tightens to `0o077`. The override is logged at INFO on startup with
both the prior and new mask so operators can confirm what happened
without inspecting on-disk file modes. There is currently no knob to
relax this; if you have a concrete need for a different policy (e.g.
`0o007` so a privileged backup group can read the secrets tree), open
an issue under the secrets-at-rest umbrella (#4137).

The tightening MUST happen before any thread is spawned: `umask(2)` is
per-thread on macOS (BSD-derived semantics) and worker threads inherit
their creator's umask at `pthread_create` time, so a runtime built
before the umask call would leave its workers at the operator's
default. The Linux umask is per-process so the constraint is weaker
there, but the same call site is correct for both platforms.

The regression test `umask_persists_into_tokio_worker_thread` in
`crates/core/src/bin/freenet.rs` pins this behavior: a plain
`File::create` issued from a `tokio::spawn`'d task is asserted to land
at `0o600` after `set_secure_umask` runs.

### Migration from pre-#4195 nodes

Nodes upgraded across this PR may have inherited the process umask
(typically `0o022` → directories at `0o755`, files at `0o644`) on the
secrets tree. `SecretsStore::new` chmods the secrets root, every
delegate directory, and every `.snapshots/` directory down to `0o700`
on each restart. A single restart of the new binary is sufficient to
migrate. Operators will see one `tracing::warn!` per restart per
already-tightened directory, e.g.

```
WARN secrets directory was not 0o700; tightening to owner-only
     path="/var/lib/freenet/secrets" existing_mode=755
```

The warn line includes the prior mode so an operator who needed a
non-default mode (e.g. group-readable for a backup tool) can recover
the original policy from logs.

Files written under the pre-#4195 binary are NOT auto-migrated to
`0o600` — the chmod logic only touches directories. Operators with
existing per-secret blobs on disk should run a one-time
`chmod -R u=rwX,go= <secrets_dir>` after upgrading to close that gap.
Files written by the post-#4195 binary land at `0o600` directly.
