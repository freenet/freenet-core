# Release-agent deployment

Per-gateway install steps for `freenet-release-agent`. Tracked in
[#4073](https://github.com/freenet/freenet-core/issues/4073).
Companion files live in [`scripts/release-agent/`](../scripts/release-agent/).

This doc covers **what's not in `install.sh`**: the parts that need human
judgement (DNS, firewall rules on cloud-hosted gateways, secret rotation,
dry-run validation).

## Architecture recap

```
GitHub Actions runner
        │  HTTPS POST /update
        │  X-Signature: HMAC-SHA256(body, secret)
        ▼
update.<host>.locut.us       ←─── DNS A record to gateway's public IP
        │
        │  TLS termination (Let's Encrypt via Caddy)
        ▼
Caddy on gateway → 127.0.0.1:9876
        │
        ▼
freenet-release-agent (as freenet-update user)
        │  verify HMAC, clock-skew, version-pin, rate-limit
        │  cross-check target == GitHub /releases/latest
        ▼
sudo -n /usr/local/bin/gateway-auto-update.sh --force --target-version vX.Y.Z
        │  (sudoers entry NOPASSWD-allows this exact prefix; the
        │   trailing `*` is a sudoers wildcard for the version value)
        ▼
fetches release binary, replaces /usr/local/bin/freenet, restarts service
```

The privilege boundary is the sudoers entry, not the agent. Even if the
agent is fully compromised, it can only invoke
`gateway-auto-update.sh --force --target-version <value>` where `<value>`
is constrained by **two independent regex checks**: the agent parses it
via `semver::Version::parse` before forwarding, and the script
re-validates with a strict `^[0-9]+\.[0-9]+\.[0-9]+...$` regex on entry.

Note: the sudoers `*` wildcard matches *any* characters including
whitespace (per `man 5 sudoers`); the safety comes from the dual regex
validation in the agent and script, not from sudoers alone. Do NOT
loosen either validation site.

### River announcement path (nova only)

```
GitHub Actions runner (release-announce.yml)
        │  HTTPS POST /announce/river
        │  X-Signature: HMAC-SHA256(body, secret)
        ▼
update.nova.locut.us → Caddy → 127.0.0.1:9876
        │
        ▼
freenet-release-agent (as freenet-update)
        │  verify HMAC, clock-skew; length+NUL check on message
        │  rate-limit: 1 announcement / minute
        ▼
sudo -n -u ian /usr/local/bin/announce-to-river.sh "<message>"
        │  (sudoers: `freenet-update ALL=(ian) NOPASSWD: ...`)
        ▼
cargo run -p riverctl message send <ROOM_VK> "<message>"
   via local Freenet node at http://127.0.0.1:7509/
```

This endpoint is **opt-in via config** (`river_announce_command` and
`river_announce_user` in `config.toml`). Gateways without a local
Freenet node + room signing key (e.g. vega) leave both fields blank;
`POST /announce/river` returns 503 there.

## GitHub Actions secrets

The end-to-end pipeline needs these repository secrets (set with
`gh secret set <name> --repo freenet/freenet-core`):

| Secret | Used by | Source |
|---|---|---|
| `RELEASE_AGENT_HMAC_NOVA` | `gateway-update.yml` + `release-announce.yml` | nova's `/etc/freenet-release-agent/hmac.key` (echoed once by `install.sh`) |
| `RELEASE_AGENT_HMAC_VEGA` | `gateway-update.yml` | vega's `/etc/freenet-release-agent/hmac.key` |
| `MATRIX_HOMESERVER_URL` | `release-announce.yml` | e.g. `https://matrix.org` |
| `MATRIX_ACCESS_TOKEN` | `release-announce.yml` | Matrix bot user's access token (from `Settings → Help & About → Advanced → Access Token`) |

The Matrix announcement job is a no-op (with a workflow warning) if
the Matrix secrets are absent — useful while bootstrapping.

## Prerequisites per gateway

1. **`gateway-auto-update.sh` installed at `/usr/local/bin/`** (or wherever
   the existing release flow puts it), owned by root, perms ≤ 755.
   `install.sh` aborts otherwise.
2. **Caddy** (or another TLS terminator) already running and serving
   `*.locut.us`. Caddyfile snippet in
   [`scripts/release-agent/Caddyfile.snippet`](../scripts/release-agent/Caddyfile.snippet).
3. **DNS A record** for `update.<host>.locut.us` pointing at the
   gateway's public IP. Create this before `install.sh` runs so Caddy can
   acquire the cert.
4. **Inbound TCP/443 reachable** from GitHub Actions runners. See [Firewall
   notes per host](#firewall-notes-per-host) below.

## Install procedure

On the gateway, as root:

```bash
# 1. Copy a freshly cross-compiled freenet-release-agent binary alongside
#    install.sh. Suggested: scp from a workstation where
#    `cargo build --release -p freenet-release-agent` just finished.
scp target/release/freenet-release-agent <gateway>:/tmp/

# 2. Run the installer with the gateway's short tag (used for the Caddy
#    vhost: nova → update.nova.locut.us).
cd /path/to/freenet-core/scripts/release-agent/
cp /tmp/freenet-release-agent ./freenet-release-agent
sudo bash install.sh nova           # or vega, etc.
```

`install.sh` is idempotent — re-running won't overwrite an existing
`/etc/freenet-release-agent/config.toml` or `hmac.key`. To rotate a
secret intentionally, see [Rotating the HMAC secret](#rotating-the-hmac-secret).

After install:

```bash
# Append the Caddyfile snippet printed by the installer into your Caddy
# config (e.g. /etc/caddy/conf.d/freenet-release-agent.conf if your
# Caddyfile imports conf.d/*), then:
sudo systemctl reload caddy

# Verify the agent is up:
curl https://update.<host>.locut.us/healthz
# → "ok"

curl https://update.<host>.locut.us/version
# → {"version":"0.2.56", ...} from the installed freenet binary

# Verify dry-run is on (it should be; that's the install default):
sudo grep ^dry_run /etc/freenet-release-agent/config.toml
# → dry_run = true
```

## Secret handling

The HMAC secret is the only authentication for `/update`. Treat it like an
SSH key.

- Generated by `install.sh` via `openssl rand -hex 32` (256-bit).
- Stored at `/etc/freenet-release-agent/hmac.key`, mode `0640`,
  `root:freenet-update`.
- Echoed once at install time; **copy it immediately** into the matching
  GitHub Actions secret (suggested name: `RELEASE_AGENT_HMAC_<HOST>`,
  e.g. `RELEASE_AGENT_HMAC_NOVA`).
- The agent reads the secret **once at startup** and holds it in process
  memory for its lifetime. Rotation therefore requires a `systemctl
  restart freenet-release-agent` after writing the new key — without
  the restart, the next GitHub Actions run will 401 with the new secret
  while the agent still verifies against the old one.

### Rotating the HMAC secret

1. On the gateway: `sudo openssl rand -hex 32 | sudo tee
   /etc/freenet-release-agent/hmac.key`. Re-set perms if `tee` widened
   them: `sudo chmod 0640 /etc/freenet-release-agent/hmac.key && sudo
   chown root:freenet-update /etc/freenet-release-agent/hmac.key`.
2. `sudo systemctl restart freenet-release-agent` to pick up the new key.
   The agent reads the secret at startup, so the rotation does not take
   effect until restart.
3. Update the GitHub Actions secret immediately. Between steps 2 and 3,
   the next workflow run will 401.
4. Smoke-test with the signing snippet from
   [`scripts/release-agent/README.md`](../scripts/release-agent/README.md).

## Firewall notes per host

### nova (self-hosted)

`ufw allow 443/tcp` if not already open for the existing Caddy vhosts.
No cloud SG to deal with. Recommended source restriction:
**leave open**, rely on HMAC. GitHub Actions IP ranges are too large
(~3000 CIDRs from `https://api.github.com/meta`) and rotate weekly, so
narrowing the SG creates more breakage than it prevents.

### vega (AWS EC2)

Vega differs from nova: `ghostkey-api` (gkapi.freenet.org) already owns
:80 and :443 on this host, so the release-agent uses a **non-standard
port and a per-host vhost**.

- AWS Security Group `sg-0f4b9b9b5bf2a8cbb`: open inbound **TCP/8443**
  from `0.0.0.0/0`. (No need to open :443; Caddy doesn't listen there
  on vega.) Rely on HMAC for authentication; the agent's defense-in-
  depth layers (clock-skew window, rate-limit, version-pin, GitHub-
  latest cross-check) make a public endpoint safe.
- Caddy config: install
  [`scripts/release-agent/Caddyfile.vega.snippet`](../scripts/release-agent/Caddyfile.vega.snippet)
  as `/etc/caddy/Caddyfile`. It uses `auto_https off` so Caddy doesn't
  try to bind :80/:443, and an explicit `tls` directive pointing at
  the dedicated `vega.locut.us` Let's Encrypt cert at
  `/etc/letsencrypt/live/vega.locut.us/{fullchain,privkey}.pem`.
- Cert permissions: the `caddy` system user must be in the `ssl-cert`
  group; the cert files in `/etc/letsencrypt/archive/vega.locut.us/`
  must be chgrp'd to `ssl-cert` with mode `0640` (privkey) / `0644`
  (the rest). Install
  [`scripts/release-agent/reload-caddy.sh`](../scripts/release-agent/reload-caddy.sh)
  as `/etc/letsencrypt/renewal-hooks/deploy/reload-caddy.sh` (mode
  0755) so certbot re-applies the group ownership and reloads Caddy
  after each renewal.
- Smoke test URL: `https://vega.locut.us:8443/release-agent/healthz`
  (not `update.vega.locut.us`). The workflow's vega matrix entry
  matches this.

If you want a tighter SG, the alternative is a fronting Caddy on a
known-IP host that the SG allows in, with the agent's port only
reachable through it. Adds operational complexity; not recommended for
Phase 1.

## Dry-run validation checklist

Before flipping `dry_run = false` on a gateway, complete the following
**against at least one real release** done via the existing
`scripts/release.sh` flow:

- [ ] Agent has been running for ≥ 24h with no restarts
  (`systemctl status freenet-release-agent`)
- [ ] `journalctl -u freenet-release-agent --since "1 day ago" | grep
  -E "ERROR|panic"` is empty
- [ ] At least one signed `POST /update` from a workstation returned
  `200 {"dry_run": true}` and logged `dry-run: would invoke …`
- [ ] At least one **wrong-signature** request returned `401` (verify
  via journal that the failure was logged)
- [ ] At least one **downgrade attempt** (target version < installed)
  returned `403`
- [ ] At least one **already-on-target** request returned `200 {"no_op":
  true}`
- [ ] After a real release: the version in
  `/etc/freenet-release-agent/hmac.key`'s host now matches the new
  freenet binary (via `/version` endpoint), confirming the agent saw
  the actual upgrade event from journald

## Flipping dry-run off

Once the checklist above passes:

```bash
sudo sed -i 's/^dry_run = true$/dry_run = false/' \
    /etc/freenet-release-agent/config.toml
sudo systemctl restart freenet-release-agent
sudo systemctl status freenet-release-agent
```

Verify with a signed request: the response should now return
`202 {"spawned": true}` instead of `200 {"dry_run": true}`, and
`journalctl -u freenet-release-agent -f` should show the sudo
invocation.

**Do nova first, leave vega in dry-run** until nova has driven a
successful production update via the workflow at least once.

## Uninstalling

```bash
sudo systemctl disable --now freenet-release-agent.service
sudo rm -f /etc/systemd/system/freenet-release-agent.service \
           /etc/sudoers.d/freenet-release-agent \
           /usr/local/bin/freenet-release-agent
sudo systemctl daemon-reload
# Optional: also remove the user and config dir
sudo userdel freenet-update
sudo rm -rf /etc/freenet-release-agent
# Remove the Caddyfile vhost block, reload Caddy.
```

The `gateway-auto-update.sh` script is left in place — it predates the
agent and is used by the existing release flow.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `curl /healthz` connection refused | Agent not running. `systemctl status freenet-release-agent` and `journalctl -u freenet-release-agent`. |
| `401` on every request | Secret mismatch between gateway and GitHub Actions. Re-paste the secret. |
| `401` only on slow runners | `clock_skew_tolerance_seconds` too tight. Default 300s should be safe; bump to 600 if the runner's clock drifts. |
| `403` from `/update` with `"reason":"downgrade"` | Workflow targeted an older version than what's installed. Expected if a hotfix landed out-of-order. |
| `403` with `"reason":"not_github_latest"` | GitHub release wasn't tagged `latest` yet (pre-release flag still set). Wait for the `release.yml` workflow to finalize. |
| `409` from `/update` | An update is already in flight on this gateway (overlapping/duplicate POST). The in-flight guard rejected the duplicate to prevent the #4271 double-stop. NOT a failure — the in-progress update is the one that applies; `gateway-update.yml` logs a `::notice::` and proceeds to the verify step. |
| `429` from `/update` | Rate-limited: another update was accepted within `rate_limit_seconds` (default 600). The recent update is in effect; the workflow treats this as non-fatal and proceeds to verify. |
| `502` | GitHub API unreachable from gateway. Transient — workflow will retry. |
| `500` after flipping `dry_run=false` | `sudo -n` rejected the invocation. `sudo -l -U freenet-update` should show the sudoers entry. If not, re-run `visudo -c -f /etc/sudoers.d/freenet-release-agent`. |
| Caddy can't get a cert | DNS A record not propagated yet, or Caddy not reachable on 80/tcp for the ACME HTTP-01 challenge. |

For anything else: `journalctl -u freenet-release-agent --since "10 min ago"`.
