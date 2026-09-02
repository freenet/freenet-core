# Freenet node container

Official image: `ghcr.io/freenet/freenet-core`, published for every stable
release. Tags are the release version (`v0.2.132`), the minor series
(`0.2`), and `latest` for the newest stable release. Images are built for
`linux/amd64` and `linux/arm64`.

## Run it

```bash
docker run -d --name freenet-node --network host \
  -v freenet-data:/data --restart unless-stopped \
  ghcr.io/freenet/freenet-core:latest
```

Or with the [compose file](docker-compose.yml) in this directory:

```bash
docker compose up -d
docker compose logs -f
```

Then open <http://127.0.0.1:7509/> to reach the node's dashboard and any
Freenet app it is serving.

## The node keeps itself up to date

**This image self-updates, and that is not optional decoration.** Freenet ships
releases frequently, sometimes several times a day, and peers are expected to
converge on a new release within hours. A node that falls behind does not merely
miss features: `min-compatible-version` is enforced as a hard gate at the
transport handshake, so once a node drops below the floor every peer refuses its
connections and it is cut off from the network.

The node detects a new release, exits with code 42 and expects a supervisor to
install the update and restart it. On other platforms that supervisor is systemd,
launchd or the Windows tray wrapper. In this image it is the container
entrypoint, which mirrors the same contract the generated systemd unit uses.

So you do **not** need Watchtower, a cron job, or a habit of running
`docker compose pull`. A container started once stays current on its own.

Practically, that means:

- The node runs from `/data/bin/freenet` on the volume, not from the image
  layer, because `freenet update` has to replace the running binary and needs
  somewhere writable. Nothing ever writes to the image, so the image itself
  stays immutable.
- An applied update survives `docker compose down && docker compose up`,
  because it lives on the volume.
- Pulling a newer image still helps: on start, the newer of (image binary,
  volume binary) wins. Pulling never rolls a self-updated node backwards.
- `docker exec freenet-node freenet --version` reports the version actually
  running, which may be ahead of the one the image shipped with.

Updating the image is still worth doing occasionally so a fresh container starts
from a recent binary rather than updating on first boot:

```bash
docker compose pull && docker compose up -d
```

### Turning auto-update off

Set `FREENET_DISABLE_AUTO_UPDATE=1`. Only do this on a private or offline test
network. On the real network the node will eventually fall below the minimum
compatible version and be refused by every peer. The entrypoint logs a loud
warning when this is set.

## Networking

The compose file uses `network_mode: host`, which is what you want on Linux.

Under Docker's default bridge network two things break:

- **The client API becomes unreachable.** It binds loopback, which under bridge
  is the *container's* loopback, so nothing on the host can reach it and no
  browser can open a Freenet app. Publishing the port does not help, because
  the API is not listening on an address the published port forwards to.
- **UDP peer-to-peer degrades.** Bridge NAT rewrites the source port of
  outbound packets so it no longer matches the published port, which works
  against hole punching and public-address discovery.

### Bridge fallback

If host networking is unavailable (Docker Desktop, or a policy that forbids it),
this works, with the caveat that the node contributes capacity to the network but
cannot serve apps to a browser:

```yaml
services:
  freenet-node:
    image: ghcr.io/freenet/freenet-core:latest
    ports:
      - "31337:31337/udp"
    volumes:
      - freenet-data:/data
    restart: unless-stopped
    stop_grace_period: 45s

volumes:
  freenet-data:
```

To also reach the client API in this mode, bind it inside the container to all
interfaces and publish it **on the host's loopback only**:

```yaml
    environment:
      - FREENET_WS_API_ADDRESS=::
    ports:
      - "31337:31337/udp"
      - "127.0.0.1:7509:7509"
```

Read [docs/client-api-exposure.md](../../docs/client-api-exposure.md) before
doing this on a machine you do not fully trust. That API is fully privileged: it
can read and modify contract state, identities and key material. Never publish
it on `0.0.0.0`, and put an authenticating reverse proxy in front of it if it has
to leave the machine.

## Ports

| Port | Protocol | Published | Purpose |
|------|----------|-----------|---------|
| 31337 | UDP | yes | Node transport. Peers connect here. |
| 7509 | TCP | loopback only | Client API and dashboard. Fully privileged. |

The transport port is pinned by the `NETWORK_PORT` environment variable. It has
to be pinned: with no explicit port the node binds a *random* free one, and no
published port mapping could reliably match it. Override it with
`-e NETWORK_PORT=…` if 31337 is taken, and change the published port to match.

## Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `NETWORK_PORT` | `31337` | UDP transport port |
| `WS_API_PORT` | `7509` | Client API port |
| `FREENET_WS_API_ADDRESS` | unset (loopback) | Bind address for the client API |
| `FREENET_ALLOWED_HOST` | unset | Extra hostnames accepted in the API `Host` header, for reverse proxies |
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
`freenet network`, so `docker run … ghcr.io/freenet/freenet-core --help` works.

## Health

The image declares a `HEALTHCHECK` that checks two things together: that the
node process the entrypoint started is still alive, and that `GET /v1/version`
answers. Both are needed. Under `network_mode: host`, `127.0.0.1:7509` is the
*host's* loopback, so a native Freenet install on the same machine would
otherwise make this container report healthy while its own node is dead.

A self-update window counts as healthy. The node is legitimately down while an
update installs, and Docker's `--start-period` covers container start only, so
without this every routine update would flip the container to unhealthy and
anything acting on health could kill it mid-update.

This is a **liveness** check. It says the node is up and serving, and nothing
about whether it has peer connections. The node exposes no dedicated health
route today.

```bash
docker inspect --format '{{.State.Health.Status}}' freenet-node
```

## Logs

The node logs to `docker logs` and to rotating files under `/data/logs` on the
volume. Both, deliberately: the console stream is what an operator reads, and
the files are what `freenet service report` collects for a diagnostic report.
The log directory is bounded (512 MiB by default, `FREENET_LOG_DIR_MAX_BYTES`).

```bash
docker compose logs -f
docker exec freenet-node ls /data/logs
```

Set `RUST_LOG` to change verbosity, for example `RUST_LOG=info,freenet=debug`.

## Restart behaviour

`restart: unless-stopped` means Docker restarts the container on any exit,
including the two the entrypoint treats as terminal: a clean shutdown, and exit
43 ("another Freenet instance is already running"). On a systemd install those
stop the service; here Docker starts it again. Exit 43 in particular usually
means something else already holds the port, most often a native Freenet
install on the same host under `network_mode: host`, and the container will
retry rather than stay down. Check `docker logs` if a container keeps
restarting, and stop the other node or change `NETWORK_PORT` and `WS_API_PORT`.

## Users and permissions

The entrypoint initializes the volume as root, then drops to the unprivileged
`freenet` user (uid/gid 1000) for everything else, including the node, the
update step and the supervise loop.

Running with `docker run --user` skips the initialization, so make the mounted
config, data and bin directories writable by that user first.

## How the image is built

The image does **not** compile Freenet. It downloads the statically linked musl
binary from the matching GitHub release, verifies it against the release's
`SHA256SUMS.txt`, and verifies that manifest against the ed25519 release-signing
key in [release-signing-key.der](release-signing-key.der).

That means the container runs the *same* artifact the release pipeline signed
and exercised in its auto-update canary, rather than a separately built binary
that could drift from it. It also makes multi-architecture images essentially
free and keeps the publish step fast enough to run on every release.

To build locally:

```bash
docker build docker/freenet-node \
  --build-arg FREENET_VERSION=v0.2.132 \
  -t freenet-node:local
```

The build context is just `docker/freenet-node`, because nothing from the rest
of the repository is needed to assemble the image.

`FREENET_VERSION` defaults to `latest`, which resolves to the newest stable
release at build time.

## Tests

The entrypoint's supervisor logic has its own test suite, which runs it against
a fake node binary and covers clean exit, exit 42 (update), exit 43 (already
running), crash restarts and SIGTERM forwarding:

```bash
docker/freenet-node/test-entrypoint.sh
```
