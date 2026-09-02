# Freenet Node Container

The official image is published to `ghcr.io/freenet/freenet-core` for every
stable Freenet release. Tags are the matching release version (for example,
`v0.2.132`) and `latest` tracks the newest stable release.

## Run it

Create a `compose.yml` file:

```yaml
services:
  freenet-node:
    image: ghcr.io/freenet/freenet-core:latest
    ports:
      - "31338:31338/udp"
    volumes:
      - freenet-data:/data
    restart: unless-stopped

volumes:
  freenet-data:
```

Run `docker compose up -d` and follow startup with `docker compose logs -f`.
The named volume preserves node data across container and image updates. To
update, run `docker compose pull && docker compose up -d`.

The checked-in [docker-compose.yml](docker-compose.yml) additionally builds an
image from the current checkout, which is useful for development.

## Ports and API

UDP port `31338` is the node transport port. The control/client API on port
`7509` is deliberately loopback-only and is not published by the example. It
can read and modify contract state, identities, and key material.

If a trusted local reverse proxy needs the API, add a loopback-only port mapping
and an `FREENET_ALLOWED_HOST` value. For remote access, also set
`FREENET_WS_API_ADDRESS` deliberately and follow the security guidance in
[client-api-exposure.md](../../docs/client-api-exposure.md).

The entrypoint initializes named volumes as root, then runs `freenet` as the
unprivileged `freenet` user. When using `docker run --user`, make the mounted
config and data directories writable by that user first.

The image disables the binary's self-updater. Container images are immutable;
use the normal image pull and restart workflow above to update the node.
