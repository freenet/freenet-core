# Freenet gateway container

Official image: `ghcr.io/freenet/freenet-gateway`, published for every stable
release. Tags are the release version (`v0.2.132`), the minor series
(`0.2`), and `latest` for the newest stable release. Images are built for
`linux/amd64` and `linux/arm64`.

A gateway is a freenet node started with `--is-gateway`: a stable, publicly
reachable bootstrap point that other peers connect through to join the
network. If you just want to run a regular node, see
[docker/freenet-node](../freenet-node/README.md) instead -- this image adds
nothing else on top.

## Run it

```bash
docker run -d --name freenet-gateway --network host \
  -e PUBLIC_NETWORK_ADDRESS=gateway.example.com \
  -v freenet-gateway-data:/data --restart unless-stopped \
  ghcr.io/freenet/freenet-gateway:latest
```

Or with the [compose file](docker-compose.yml) in this directory (set
`PUBLIC_NETWORK_ADDRESS` first):

```bash
PUBLIC_NETWORK_ADDRESS=gateway.example.com docker compose up -d
docker compose logs -f
```

`PUBLIC_NETWORK_ADDRESS` is the one thing you must supply: the hostname or IP
other peers should advertise and connect to. There is no default -- a
container started without it exits immediately with a clear error, rather
than starting a gateway nobody can reach.

## Identity

A gateway needs a *stable* identity: the same transport keypair across
restarts and upgrades, because that is what makes it a recognizable,
reconnectable bootstrap point rather than a new stranger every time the
container recreates.

The keypair lives at `/data/keys/transport-keypair.pem` (override with
`TRANSPORT_KEYPAIR`) and is **generated automatically on first start if it
does not exist yet**. You do not need to pre-create anything; you do need to
make sure `/data` is a real, persistent volume (see the compose file) so the
generated identity survives a container recreate. Back it up like you would
any other private key.

## The gateway keeps itself up to date

Same guarantee as [docker/freenet-node](../freenet-node/README.md#the-node-keeps-itself-up-to-date):
this image self-updates, using the same container entrypoint and the same
systemd-unit-mirroring contract. You do not need Watchtower, a cron job, or a
habit of running `docker compose pull`. See that section for the full
explanation; it applies here unchanged, because a gateway IS a node.

## Networking

The compose file uses `network_mode: host`, which is what you want on Linux,
for the same reasons as the node image (see
[docker/freenet-node/README.md](../freenet-node/README.md#networking)) plus
one gateway-specific reason: a gateway's whole job is being reachable at a
*stable* `address:port` that it advertises to every peer that connects
through it, and bridge NAT rewriting the source port of outbound packets
works against that.

### Bridge fallback

If host networking is unavailable (Docker Desktop, or a policy that forbids
it):

```yaml
services:
  freenet-gateway:
    image: ghcr.io/freenet/freenet-gateway:latest
    environment:
      - PUBLIC_NETWORK_ADDRESS=gateway.example.com
    ports:
      - "31337:31337/udp"
    volumes:
      - freenet-gateway-data:/data
    restart: unless-stopped
    stop_grace_period: 45s

volumes:
  freenet-gateway-data:
```

Read [docs/client-api-exposure.md](../../docs/client-api-exposure.md) before
publishing the client API port. It is fully privileged and is not exposed by
either compose file above.

## Ports

| Port | Protocol | Published | Purpose |
|------|----------|-----------|---------|
| 31337 | UDP | yes | Gateway transport. Peers connect here. |
| 7509 | TCP | loopback only | Client API and dashboard. Fully privileged. |

The transport port is pinned by `NETWORK_PORT`, and the port a gateway
*advertises* to peers is pinned separately by `PUBLIC_NETWORK_PORT` -- see
Configuration below for when these need to differ.

## Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `PUBLIC_NETWORK_ADDRESS` | **required, no default** | Hostname or IP peers should advertise/connect to |
| `PUBLIC_NETWORK_PORT` | `NETWORK_PORT` | Port advertised to peers, if different from the local listen port (e.g. behind a port-forwarding NAT) |
| `NETWORK_PORT` | `31337` | UDP transport port this gateway listens on locally |
| `TRANSPORT_KEYPAIR` | `/data/keys/transport-keypair.pem` | Path to this gateway's identity. Generated automatically if missing -- see Identity above |
| `WS_API_PORT` | `7509` | Client API port |
| `FREENET_CONFIG_DIR` | `/data/config` | Configuration directory, passed to the node as `--config-dir` |
| `FREENET_DATA_DIR` | `/data/node` | Node data directory, passed to the node as `--data-dir` |
| `FREENET_BIN_DIR` | `/data/bin` | Where the running binary lives |
| `FREENET_HOME_DIR` | `/data/home` | `$HOME` for the node, so rollback state persists |
| `FREENET_RESTART_JITTER_PCT` | `20` | Jitter applied to the restart backoff |
| `LOG_DIR` | `/data/logs` | Rotating log files, on the volume |
| `FREENET_LOG_TO_CONSOLE` | `1` | Also log to stdout, so `docker logs` works |
| `RUST_LOG` | unset (info) | Log verbosity |
| `FREENET_DISABLE_AUTO_UPDATE` | unset | Set to disable self-update. Test networks only. |

Arguments passed to `docker run` after the image name are forwarded to
`freenet network`, same as the node image.

## Health

```bash
docker inspect --format '{{.State.Health.Status}}' freenet-gateway
```

Same liveness semantics as the node image -- see
[docker/freenet-node/README.md](../freenet-node/README.md#health). A
self-update window counts as healthy.

## Logs

```bash
docker compose logs -f
docker exec freenet-gateway ls /data/logs
```

Same as the node image: console + rotating files under `/data/logs`, both
persisted on the volume.

## Restart behaviour

Same as the node image -- see
[docker/freenet-node/README.md](../freenet-node/README.md#restart-behaviour).
Exit 43 ("another Freenet instance is already running") most often means a
native Freenet install sharing this host's `NETWORK_PORT`/`WS_API_PORT` under
`network_mode: host`.

## Users and permissions

Same as the node image: the entrypoint initializes the volume as root
(including the transport-keypair directory), then drops to the unprivileged
`freenet` user (uid/gid 1000) for everything else.

## How the image is built

The image does **not** compile Freenet -- it downloads the same
statically-linked musl binary the node image ships, verified the same way.
See [docker/freenet-node/README.md](../freenet-node/README.md#how-the-image-is-built)
for the full explanation.

An earlier version of this Dockerfile *did* build from source, against a
`node:26-bullseye-slim` base image. That approach stopped working once
`bullseye` reached end-of-life (`bullseye-security` is gone from the live
Debian mirror, and even `archive.debian.org`'s snapshot no longer matches the
packages already baked into that base image); see
[#5634](https://github.com/freenet/freenet-core/issues/5634) for the details.
Moving to the same fetch-and-verify approach as the node image fixes that
permanently, rather than chasing a moving EOL target, and also means this
image is no longer the *only* one of the two that has no published, prebuilt
artifact.

To build locally:

```bash
docker build docker/freenet-gateway \
  --build-arg FREENET_VERSION=v0.2.132 \
  -t freenet-gateway:local
```

The build context is just `docker/freenet-gateway`, because nothing from the
rest of the repository is needed to assemble the image.

## Tests

The entrypoint's supervisor logic has its own test suite, covering everything
the node's suite covers plus the gateway-specific required-env-var check and
first-start keypair generation:

```bash
docker/freenet-gateway/test-entrypoint.sh
```
