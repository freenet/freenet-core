#!/bin/sh
# Certbot deploy hook for vega.locut.us — fix up cert permissions for the
# `ssl-cert` group (Caddy reads the cert as the `caddy` user, which is in
# that group) and tell Caddy to pick up the renewed cert without dropping
# the listening socket on :8443.
#
# Runs only when at least one cert was actually renewed (certbot only
# invokes deploy hooks on success). RENEWED_LINEAGE is set to the
# /etc/letsencrypt/live/<name> directory by certbot.
#
# Install path on vega: /etc/letsencrypt/renewal-hooks/deploy/reload-caddy.sh
# (mode 0755, owner root:root). Filed as part of issue #4120.

set -eu

case "${RENEWED_LINEAGE:-}" in
  */vega.locut.us)
    ARCHIVE_DIR="/etc/letsencrypt/archive/vega.locut.us"
    if [ -d "$ARCHIVE_DIR" ]; then
      # Re-apply ssl-cert group ownership and group-readable mode on the
      # newly-rotated cert files. certbot writes them as root:root 0600
      # for privkey / 0644 for the chain by default.
      chgrp ssl-cert "$ARCHIVE_DIR"/*.pem
      chmod 0644 "$ARCHIVE_DIR"/cert*.pem "$ARCHIVE_DIR"/chain*.pem "$ARCHIVE_DIR"/fullchain*.pem
      chmod 0640 "$ARCHIVE_DIR"/privkey*.pem
    fi
    # Reload (not restart) Caddy so existing connections drain gracefully.
    systemctl reload caddy
    ;;
  *)
    # Different cert renewed (e.g. gkapi.freenet.org). Nothing to do here.
    ;;
esac
