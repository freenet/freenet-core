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

echo "==> Installing gateway-auto-update.sh and deploy-local-gateway.sh"
# Always from this checkout, so re-running install.sh after a merge is how the
# gateway picks up the current scripts. gateway-auto-update.sh verifies each
# release's signed checksums before installing (an older copy does not), and
# runs deploy-local-gateway.sh from its own directory as root, so a stale
# deploy script is as dangerous as a stale updater (#4492). Both are
# refreshed; a changed previous deploy script is kept as .prev.
GATEWAY_BIN_DIR=/usr/local/bin
GATEWAY_UPDATE_SCRIPT=$GATEWAY_BIN_DIR/gateway-auto-update.sh
DEPLOY_SCRIPT=$GATEWAY_BIN_DIR/deploy-local-gateway.sh
install -m 0755 -o root -g root \
    "$SCRIPT_DIR/../gateway-auto-update.sh" \
    "$GATEWAY_UPDATE_SCRIPT"
if [[ -f "$DEPLOY_SCRIPT" ]] && ! cmp -s "$SCRIPT_DIR/../deploy-local-gateway.sh" "$DEPLOY_SCRIPT"; then
    cp -p "$DEPLOY_SCRIPT" "$DEPLOY_SCRIPT.prev"
    echo "  Replacing a different $DEPLOY_SCRIPT (previous copy: $DEPLOY_SCRIPT.prev)"
fi
install -m 0755 -o root -g root \
    "$SCRIPT_DIR/../deploy-local-gateway.sh" \
    "$DEPLOY_SCRIPT"
if ! openssl version 2>/dev/null | grep -qE '^OpenSSL ([3-9]|[1-9][0-9])\.'; then
    echo "WARNING: OpenSSL >= 3.0 not found; gateway-auto-update.sh will refuse every update until it is installed." >&2
fi

echo "==> Verifying the privilege boundary around the update scripts"
# The files were just installed root:root 0755, so checking them proves
# nothing. What can still undo the sudoers boundary is the DIRECTORY: if
# freenet-update (or any non-root user) can write $GATEWAY_BIN_DIR, it can
# replace either script, including the deploy script that
# gateway-auto-update.sh runs as root from that directory.
dir_owner=$(stat -c '%U' "$GATEWAY_BIN_DIR")
dir_perms=$(stat -c '%a' "$GATEWAY_BIN_DIR")
if [[ "$dir_owner" != "root" ]]; then
    echo "ERROR: $GATEWAY_BIN_DIR is owned by $dir_owner, must be root" >&2
    exit 1
fi
if (( 8#$dir_perms & 8#022 )); then
    echo "ERROR: $GATEWAY_BIN_DIR has perms $dir_perms; group/other must not be able to write it" >&2
    exit 1
fi

echo "==> Installing announce-to-river.sh"
# Always install the script — it's harmless when the agent's config
# leaves `river_announce_command` blank (the endpoint returns 503 in
# that case). Set `river_announce_command = "/usr/local/bin/announce-to-river.sh"`
# in config.toml AND make sure ~/.config/freenet-river-official/ +
# the local Freenet node are present on this gateway before enabling.
ANNOUNCE_SCRIPT=/usr/local/bin/announce-to-river.sh
install -m 0755 -o root -g root \
    "$SCRIPT_DIR/announce-to-river.sh" \
    "$ANNOUNCE_SCRIPT"
echo "  Installed $ANNOUNCE_SCRIPT (endpoint disabled until config opts in)"

echo "==> Installing the staggered companion-gateway restart (#6336)"
# gateway-auto-update.sh queues freenet-gateway-stagger.service after a primary
# update; the hook skips hosts where that unit does not exist. Install the
# script everywhere (harmless), and the units only on nova, which runs a second
# gateway process (freenet-gateway-2) that must restart AFTER the primary, not
# with it. The gw2 unit carries nova's addresses and key paths, and expects
# /home/freenet/.ssh/gw2.x25519.pem and the supervised.conf drop-in
# (FREENET_SUPERVISED=1) to be provisioned separately.
install -m 0755 -o root -g root \
    "$SCRIPT_DIR/../gateway-staggered-restart.sh" \
    /usr/local/bin/gateway-staggered-restart.sh
if [[ "$HOSTNAME_TAG" == "nova" ]]; then
    for unit in freenet-gateway-2 freenet-gateway-stagger freenet-gateway-stagger-alert; do
        install -m 0644 -o root -g root \
            "$SCRIPT_DIR/$unit.service" "/etc/systemd/system/$unit.service"
    done
    systemctl daemon-reload
    # The primary must be enabled itself: deploy-local-gateway.sh only re-enables
    # a unit that was enabled when the deploy began, so it never repairs this.
    if systemctl cat freenet-gateway.service >/dev/null 2>&1; then
        systemctl enable freenet-gateway.service
    else
        echo "  WARNING: freenet-gateway.service does not exist yet; enable it once it does" >&2
    fi
    # reenable (not enable) so an upgrade of an install made before #6336 also
    # drops the old freenet-gateway.service.wants/freenet-gateway-2.service link.
    systemctl reenable freenet-gateway-2.service
    echo "  Installed gw2 + stagger units (not started; no gateway is restarted)"
fi

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
