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
`crates/core/src/wasm_runtime/secrets_store/store.rs::decrypt_secret_blob`),
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
                                  --timestamp-ms <ms> [--suffix <n>] --yes
freenet secrets export            --secrets-dir <path> --db-dir <path> \
                                  {--local | --user-token [tok]} \
                                  [--passphrase [p] | --use-token-key] --out <file>
freenet secrets import   <file>   --secrets-dir <path> --db-dir <path> \
                                  [--passphrase [p] | --use-token-key [--token tok]] \
                                  [--local | --into-user [tok]] [--overwrite]
```

Secret inputs (passphrase, user token) are read from a SAFE source so they
need not appear on the command line, where they leak via the process table
(`ps`) and shell history. For each secret the resolution order is:
**(1)** an environment variable — `FREENET_SECRET_PASSPHRASE` for the
passphrase, `FREENET_USER_TOKEN` for the user token; **(2)** the
corresponding flag, if given (last-resort convenience; `--help` warns it is
exposed); **(3)** an interactive prompt when stdin is a TTY (no-echo for the
passphrase). In a non-interactive context with neither env nor flag set, the
command errors rather than hanging. The default key method is the passphrase;
`--use-token-key` selects token-keyed crypto instead (export requires
`--user-token`; import reads `FREENET_USER_TOKEN` / `--token` / a prompt).

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
`--timestamp-ms` (copy the value from `snapshot-list`). If two writes
landed in the same millisecond, `snapshot-list` shows multiple rows at
that timestamp with a `suffix` (`-`, `0`, `1`, …); pass `--suffix <n>`
to target the `.n` row (omit it to restore the unsuffixed `-` row, which
is also the only entry when there is no collision). The current active
value is snapshotted first, so the restore is itself reversible.
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

After a successful restore, `snapshot-restore` thins the history under
the default retention policy, the same as a normal write. One
consequence: if you restore from a snapshot older than the 2-year
`max_age` cap, that very old source snapshot is pruned from
`.snapshots/` afterward. The restore itself is unaffected (the value is
already the active secret by then) and stays reversible — the prior
active value is captured as a fresh snapshot before the overwrite.

### Export / import a portable secrets bundle (#4035, P3 of #4381)

Unlike snapshot-restore, `export`/`import` move secrets BETWEEN nodes.
`export` gathers every secret in a scope into a single encrypted file;
`import` re-places them on another node.

The primary use case is the hosted → self-host migration (#4381): a user
who tried Freenet through a hosted gateway exports their per-user
delegate secrets (`--user-token <tok>`) and re-imports them into their
own peer (`import --local`). The same surface also backs up a normal
node's single-user secrets (`export --local`).

Both commands need `--secrets-dir` (the on-disk secret blobs) AND
`--db-dir` (the node's data directory, holding the ReDb secrets index).
The export walks the index to know what to gather, so unlike the
snapshot commands it can't run on the secrets tree alone. The node MUST
be stopped (the ReDb file is opened exclusively).

**Scope selection.** `export` takes exactly one source scope:
`--local` (all single-user secrets) or `--user-token <tok>` (every
per-user secret for that one hosted user, across all delegates).
`import` places secrets at `--local` by default (the self-host target);
`--into-user <tok>` re-files them under a per-user scope instead.

**Bundle format.** The output file is
`[MAGIC "FNSX"][version][kdf_id][16B salt][24B nonce][AEAD]`. The AEAD
plaintext is a CBOR document `{schema_version, source_scope,
created_unix_secs, entries:[{delegate_key, code_hash, secret_hash,
plaintext}]}`. The header is bound as AEAD additional data, so any
tampering with the version / salt / nonce fails authentication cleanly.
Each entry records the delegate key + code hash so the original
`DelegateKey` is reconstructible on the import node — and because a
`DelegateKey` is content-derived from the delegate's wasm+params, a
re-installed webapp shipping the same delegate yields the same key, so
the imported secrets line up without any per-app migration table.

**Encryption at rest.** The bundle file is ALWAYS encrypted. The key is
derived either from a user passphrase (`--passphrase`, Argon2id over the
per-bundle salt) or from the opaque user token (`--use-token-key` on
export / `--token` on import, HKDF-SHA256 — handy for a hosted user who
only has their token). The two methods are not interchangeable: a
passphrase bundle opened with token material (or vice-versa) fails
authentication. A wrong passphrase/token (or a corrupt bundle) fails
during the decrypt phase, before ANY write. The per-entry write loop
itself is not transactional, so a write that fails partway leaves the
already-written entries committed — but re-running the import is safe:
entries are idempotent by their `(delegate, secret_hash)` key (an
existing secret is skipped, or re-written to the same value with
`--overwrite`), so a retry converges.

**Operator sees plaintext during export.** Building the bundle requires
the node to DECRYPT every secret in memory (it can, by construction — it
holds the scope DEK). So an operator running `export` on a hosted node
observes the plaintext for the duration of the export. This is inherent
to the hosted model (the node already runs the delegate) and is flagged
on stderr when the command runs. A user who wants zero operator exposure
should self-host from the start rather than migrate out later. The
bundle FILE, by contrast, is never written in plaintext.

**Collision handling.** On import, a secret that already exists at the
same delegate+id is left untouched and reported as skipped, unless
`--overwrite` is passed (in which case the prior value is snapshotted
first, like a normal write). `--out` on export refuses to overwrite an
existing file.

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
| `<secrets_dir>/<delegate>/.keys`                | 0o600 |
| `<secrets_dir>/<delegate>/users/<user_id>/.keys`| 0o600 |
| `<secrets_dir>/<delegate>/.snapshots/`          | 0o700 |
| `<secrets_dir>/<delegate>/.snapshots/<sec>/`    | 0o700 |
| `<secrets_dir>/<delegate>/.snapshots/<sec>/*`   | 0o600 |

Modes are set in the same syscall as `O_CREAT` (via
`OpenOptions::mode`), so there is no race window where a file is
readable under the process umask.

### Key-enumeration registry: `.keys` (PR #4523 / issue #4355)

Each scope keeps an optional encrypted `.keys` registry that maps its
stored secrets back to the **raw key bytes** the delegate originally
supplied to `store_secret`. It backs the `list_secrets` delegate
hostcall: the durable ReDb index stores only the 32-byte Blake3 hash of
each key and secret files are hash-named, so without this registry the
raw key (e.g. an open-ended `room:<owner_vk>` family) is recoverable
nowhere on disk. One registry lives per scope:
`<delegate>/.keys` for `Local`, `<delegate>/users/<user_id>/.keys` for
each `User` scope.

At-rest properties — identical discipline to the secret values it
describes:

- Encrypted under the **same scope DEK** as that scope's secret values
  (`Local`: the delegate cipher; `User`: `derive_user_dek`), so no new
  key material is introduced.
- Same on-disk layout: `[VERSION_V1][24-byte XChaCha20-Poly1305
  nonce][AEAD ciphertext + tag]`, with a **fresh per-write nonce** from
  the OS entropy pool (no nonce reuse across rewrites).
- Created `0o600` under the owner-only (`0o700`) scope tree, written via
  the same tmp+fsync+rename atomic discipline (`.keys.tmp` sibling).
- Bounded at 4096 keys per scope to cap amplification; over-cap keys are
  still stored and readable as values but are not enumerable.

The registry is **best-effort and independent of the value path**: it is
written strictly AFTER the durable value+index commit, and a failed or
unreadable registry only degrades future enumeration — it never blocks
or loses a secret VALUE. A transient read error or a corrupt/undecryptable
`.keys` blob is FAIL-SAFE: the on-disk registry is left untouched rather
than overwritten from empty, so a momentary IO hiccup cannot permanently
shrink the enumerable key set (it recovers on the next readable write).

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

## Hosted mode: per-user secrets over a secure connection (#4381)

Hosted mode (`--hosted-mode`, OFF by default) lets one node serve many
untrusted web users, each getting their own per-user delegate-secret
namespace derived from a durable `userToken` they present on the
WebSocket upgrade. When hosted mode is OFF, `userToken` is ignored and
every connection is single-user — byte-for-byte the pre-#4381 behavior.

The `userToken` is a durable, node-independent, high-value credential:
it names a per-user secret namespace and is valid against any hosted
node. So the node honors it ONLY over a connection it can prove is
secure.

### The gate

With hosted mode on, the per-user token is honored ONLY over a
**loopback** connection carrying **`X-Forwarded-Proto: https`** — i.e.
the request reached the node from `127.0.0.1` / `[::1]` and arrived with
an `https` forwarded-proto header. That is the shape produced by a
TLS-terminating reverse proxy running on the same host as the node:

- the **loopback source** proves the proxy→node hop is local (the
  plaintext API port was not exposed to the network for this
  connection), and
- the **`https` `X-Forwarded-Proto`** is positive evidence, set by the
  TLS terminator, that the browser→proxy hop used TLS.

Everything else is rejected with `403` (fail-closed):

| source | `X-Forwarded-Proto` | outcome |
|---|---|---|
| loopback | `https` | honored |
| loopback | missing or `http` | **403** |
| non-loopback | any | **403** |
| unknown | any | **403** |

A direct plaintext connection — even from loopback — is refused: without
the `https` attestation the node cannot rule out that the token crossed
the network in cleartext. The request `Host` header is deliberately NOT
consulted: it is proxy-rewritable (nginx's default rewrites it to the
upstream `127.0.0.1:7509`), so it cannot grant trust; only the `https`
`X-Forwarded-Proto` can.

### Operator responsibility (REQUIRED proxy configuration)

The node trusts `X-Forwarded-Proto` only from a loopback source, but it
cannot tell a header the proxy *set* from one the proxy merely *passed
through* from the client. Making the attestation trustworthy is the
operator's job. The proxy fronting a hosted node MUST:

1. **SET / OVERWRITE `X-Forwarded-Proto` itself**, to the real scheme of
   the browser→proxy hop.
2. **STRIP any client-supplied `X-Forwarded-*` headers** before
   forwarding, so a client cannot inject its own
   `X-Forwarded-Proto: https`.

- **Caddy** does both by default — it sets `X-Forwarded-Proto` and does
  not pass through a client-supplied value. No extra configuration
  needed.
- **nginx** requires `proxy_set_header X-Forwarded-Proto $scheme;` (a
  literal `proxy_set_header X-Forwarded-Proto https;` is fine for an
  HTTPS-only server block) AND must NOT forward a client-supplied
  `X-Forwarded-Proto`. nginx forwards unknown client request headers by
  default, so an explicit `proxy_set_header` that overwrites the value is
  the mechanism that also stops pass-through — make sure no `http`/server
  block leaks the client value through.

### Security note (known limitation)

The node trusts `X-Forwarded-Proto` from a loopback source. If the proxy
is **misconfigured to forward a client-supplied `X-Forwarded-Proto:
https` over a plaintext listener**, a client could spoof the TLS
attestation and the token would be honored over cleartext. The node
cannot detect this pass-through misconfiguration — it sees a loopback
source and an `https` header and has no way to know the header came from
the client rather than the proxy.

Correct proxy configuration (set-and-strip, above) is therefore the
operator's responsibility. This is an accepted limitation, not a
shared-secret handshake: the early hosted operators are the Freenet team,
who control their own proxy configuration. A developer testing hosted
mode locally must likewise front the node with a TLS proxy or send the
header manually (`curl -H 'X-Forwarded-Proto: https'` from loopback) — a
plain plaintext loopback request is refused.
