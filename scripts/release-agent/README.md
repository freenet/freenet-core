# freenet-release-agent deploy artifacts

These files install the release-agent on a gateway host. The agent itself lives
in `crates/release-agent/`. Tracked in
[freenet-core#4073](https://github.com/freenet/freenet-core/issues/4073).

## Files

| File | Install path | Purpose |
|---|---|---|
| `freenet-release-agent.service` | `/etc/systemd/system/` | systemd unit running as the dedicated `freenet-update` user |
| `sudoers.freenet-release-agent` | `/etc/sudoers.d/` (mode 0440) | NOPASSWD only for `gateway-auto-update.sh --force` |
| `config.example.toml` | `/etc/freenet-release-agent/config.toml` | Per-gateway config; **dry_run = true** by default |
| `Caddyfile.snippet` | merge into your Caddy config | TLS-terminated public hostname (`update.<host>.locut.us`) |
| `install.sh` | run once on the gateway | Wires all of the above |

## Privilege model

- **Agent process** runs as `freenet-update` (system user, no shell, no home).
- **Update path** is `agent → sudo → /usr/local/bin/gateway-auto-update.sh --force`.
  sudoers matches the literal command line; any flag other than `--force` is
  rejected by sudo before the script runs.
- **HMAC secret** at `/etc/freenet-release-agent/hmac.key` is mode 0640,
  root:freenet-update.

## Phases

- **Phase 1** (this PR): `dry_run = true` everywhere. Agent logs what it
  *would* do but never invokes the update script. Used to validate auth,
  rate-limiting, and the workflow → agent round-trip without risking a real
  release.
- **Phase 2**: flip `dry_run = false` on nova only.
- **Phase 3**: deploy on vega, switch the GitHub workflow to call both agents.

## Adding a new gateway

```bash
# On the gateway:
sudo bash install.sh <hostname-tag>
# Copy the printed HMAC secret into a GitHub Actions secret.
# Append the printed Caddyfile block, reload Caddy.

# From your workstation, smoke test:
curl https://update.<host>.locut.us/healthz
curl https://update.<host>.locut.us/version
```

## Smoke test (signing a request)

```bash
SECRET=$(sudo cat /etc/freenet-release-agent/hmac.key)
BODY='{"version":"0.2.56","issued_at":'"$(date +%s)"'}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$SECRET" | awk '{print $NF}')

curl -X POST https://update.<host>.locut.us/update \
  -H "Content-Type: application/json" \
  -H "X-Signature: $SIG" \
  -d "$BODY"
```

In dry-run mode the response will include `"dry_run": true` and the journal
will show a `dry-run: would invoke …` line. No update is actually performed.
