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
- **Phase 3**: deploy on a second gateway and switch the GitHub workflow to
  call both agents. (The second gateway was vega until it was retired in
  September 2026, #5540; nova is now the only rollout target.)

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

## Response semantics

`GET /version` returns JSON:

```json
{
  "version": "0.2.71",
  "binary_path": "/usr/local/bin/freenet",
  "service_active": true,
  "managed_service": "freenet-gateway"
}
```

- **`version`** — the ON-DISK freenet binary version (`freenet --version`).
  This alone is NOT proof the gateway is running it.
- **`service_active`** — `true` iff `systemctl is-active <managed_service>`
  reports `active`. Surfaces a gateway whose binary was swapped but whose
  service failed to restart (the vega v0.2.71 incident, where `/version`
  reported the new version while the gateway was down for ~5 minutes).
  Consumers polling for a successful update MUST require `service_active == true`.
  **Backward compatibility:** agents built before this field omit it entirely.
  Its ABSENCE means "this agent cannot confirm service health", which is NOT the
  same as success. As of #4492 the `gateway-update.yml` consumer **fails closed**
  when the field is absent (the binary may have swapped while the service stayed
  dead — the 2026-06-18 nova incident); accepting a binary-only check is now an
  explicit, opt-in choice (`allow_binary_only_fallback=true`), not the default.
- **`managed_service`** — the systemd unit name whose state `service_active`
  reflects.

`POST /update` returns:

- **`200` + `no_op: true`** — the gateway is already on the requested version. The agent did NOT spawn the update script. Callers that poll `/version` to confirm the upgrade MUST inspect `no_op` to distinguish "already there" from "just kicked off".
- **`200` + `no_op: false`** — dry-run mode; would have spawned.
- **`202`** — live mode; update spawned, expected to complete within seconds. Poll `/version` (tolerating ~30s of connection-refused during systemd restart) to confirm the new binary.
- **`401`** — missing/invalid signature, stale `issued_at`, or non-positive `issued_at`.
- **`403`** — refusing to downgrade, or requested version doesn't match GitHub `latest`.
- **`429`** — rate-limited (default: one update per 10 min per gateway).
- **`502`** — GitHub `/releases/latest` lookup failed. Rate-limit window is NOT consumed in this case; retry once GitHub recovers.
- **`500`** — `sudo` rejected or the script exited non-zero within 1 second. Rate-limit window also NOT consumed. Check `journalctl -u freenet-release-agent` for the script's stderr.
