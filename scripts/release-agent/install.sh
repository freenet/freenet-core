#!/bin/bash
# Install the freenet-release-agent on a gateway host. Run as root.
#
# Expects the agent binary to be present alongside this script (named
# `freenet-release-agent`). Cross-compile it from CI or copy from a local
# build.
#
# Usage:
#   sudo bash install.sh <hostname-tag>
#
# Where <hostname-tag> is the gateway's short name, used to template the
# Caddy site (e.g. `nova` -> update.nova.locut.us).

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "Must be run as root" >&2
    exit 1
fi
if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <hostname-tag>" >&2
    exit 1
fi

HOSTNAME_TAG="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY_SRC="$SCRIPT_DIR/freenet-release-agent"

if [[ ! -x "$BINARY_SRC" ]]; then
    echo "Expected binary at $BINARY_SRC (cross-compile or copy from local build)." >&2
    exit 1
fi

echo "==> Creating freenet-update system user (idempotent)"
if ! id -u freenet-update >/dev/null 2>&1; then
    useradd --system --no-create-home --shell /usr/sbin/nologin freenet-update
fi

echo "==> Creating config directory"
install -d -m 0755 -o root -g root /etc/freenet-release-agent
# Logs go to journald (StandardOutput=journal in the systemd unit); no
# filesystem log dir needed.

echo "==> Installing binary"
install -m 0755 -o root -g root "$BINARY_SRC" /usr/local/bin/freenet-release-agent

echo "==> Installing config (preserves existing /etc/freenet-release-agent/config.toml)"
if [[ ! -f /etc/freenet-release-agent/config.toml ]]; then
    install -m 0640 -o root -g freenet-update \
        "$SCRIPT_DIR/config.example.toml" /etc/freenet-release-agent/config.toml
fi

echo "==> Generating HMAC secret (preserves existing /etc/freenet-release-agent/hmac.key)"
if [[ ! -f /etc/freenet-release-agent/hmac.key ]]; then
    umask 077
    openssl rand -hex 32 > /etc/freenet-release-agent/hmac.key
    chmod 0640 /etc/freenet-release-agent/hmac.key
    chown root:freenet-update /etc/freenet-release-agent/hmac.key
    echo
    echo "Generated new HMAC secret. Copy it into GitHub Actions secrets as"
    echo "  RELEASE_AGENT_HMAC_${HOSTNAME_TAG^^}"
    echo
    echo "  $(cat /etc/freenet-release-agent/hmac.key)"
    echo
fi

echo "==> Verifying gateway-auto-update.sh privilege boundary"
GATEWAY_UPDATE_SCRIPT=/usr/local/bin/gateway-auto-update.sh
if [[ -e "$GATEWAY_UPDATE_SCRIPT" ]]; then
    script_owner=$(stat -c '%U' "$GATEWAY_UPDATE_SCRIPT")
    script_perms=$(stat -c '%a' "$GATEWAY_UPDATE_SCRIPT")
    if [[ "$script_owner" != "root" ]]; then
        echo "ERROR: $GATEWAY_UPDATE_SCRIPT is owned by $script_owner, must be root" >&2
        echo "  If freenet-update can write this script, the sudoers entry is meaningless." >&2
        exit 1
    fi
    if [[ "$script_perms" -gt 755 ]]; then
        echo "ERROR: $GATEWAY_UPDATE_SCRIPT has perms $script_perms; max safe is 755" >&2
        exit 1
    fi
else
    echo "WARNING: $GATEWAY_UPDATE_SCRIPT not found yet. Install it with the existing gateway-auto-update bootstrap before flipping dry_run=false." >&2
fi

echo "==> Installing announce-to-river.sh (gateways without a Freenet node + signing key should omit this)"
ANNOUNCE_SCRIPT=/usr/local/bin/announce-to-river.sh
install -m 0755 -o root -g root \
    "$SCRIPT_DIR/announce-to-river.sh" \
    "$ANNOUNCE_SCRIPT"
echo "  Installed $ANNOUNCE_SCRIPT (no-op on gateways without river_announce_command set in config)"

echo "==> Installing sudoers entry"
install -m 0440 -o root -g root \
    "$SCRIPT_DIR/sudoers.freenet-release-agent" \
    /etc/sudoers.d/freenet-release-agent
visudo -c -f /etc/sudoers.d/freenet-release-agent

echo "==> Installing systemd unit"
install -m 0644 -o root -g root \
    "$SCRIPT_DIR/freenet-release-agent.service" \
    /etc/systemd/system/freenet-release-agent.service
systemctl daemon-reload
systemctl enable --now freenet-release-agent.service

echo "==> Caddy site config (manual step)"
echo "Drop the following into your Caddy config and reload Caddy:"
echo
sed "s|<HOSTNAME>|$HOSTNAME_TAG|g" "$SCRIPT_DIR/Caddyfile.snippet"
echo

systemctl status --no-pager freenet-release-agent.service || true

echo "==> Smoke test (loopback)"
sleep 1
curl -fsS http://127.0.0.1:9876/healthz || {
    echo "Smoke test failed — check journalctl -u freenet-release-agent" >&2
    exit 1
}
echo
echo "Install complete. Agent is in dry-run mode; flip dry_run=false in"
echo "/etc/freenet-release-agent/config.toml after Phase 2 validation."
