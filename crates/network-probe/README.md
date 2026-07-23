# network-probe

Nightly probe against the **production** Freenet network (issue #4665).
It behaves like a normal client — it only talks to node WebSocket APIs and
never links `freenet-core` — so it measures what a real user experiences.

Each `put-get` run:

1. PUTs 3 small contracts plus one ~1 MB contract through an existing
   gateway's WS API.
2. Boots an **ephemeral peer** (`freenet network` child process with an
   empty data dir) that joins the network like any new user. Having no
   replicas, its GETs are forced to route over the network.
3. GETs today's contracts through that peer and verifies byte-identity.
4. Re-GETs contracts published by previous runs (24 h / 48 h / 7 d windows,
   tracked in a persistent JSON manifest) and verifies their blake3 state
   hashes — real multi-day retention.
5. Prints one JSON line per operation on stdout and exits non-zero if
   anything failed. No retries by design: an operation that only succeeds
   on retry is the regression the probe exists to surface.

Scenarios are subcommands sharing the same infrastructure (client,
ephemeral node, manifest, report); future ones (update propagation,
subscriptions) are new modules under `src/scenarios/`.

## Production use (nova)

```bash
network-probe put-get \
  --gateway-ws ws://127.0.0.1:7509 \
  --manifest /path/persistent/network-probe-manifest.json \
  --ephemeral-network-port 32177   # UDP port opened for the probe on nova
```

With no `--gateway-spec`, the ephemeral peer bootstraps from the regular
gateway index, exactly like a production peer.

## Local smoke test

Terminal A — isolated local gateway (prints the spec to copy):

```bash
cargo build -p freenet
cargo run -p network-probe -- local-gateway \
  --freenet-bin target/debug/freenet --ws-port 7609
```

Terminal B — the probe against it:

```bash
cargo run -p network-probe -- put-get \
  --gateway-ws ws://127.0.0.1:7609 \
  --gateway-spec "<printed by terminal A>" \
  --freenet-bin target/debug/freenet \
  --ephemeral-network-port 31400 --ephemeral-ws-port 7519 \
  --manifest /tmp/probe-manifest.json --settle-secs 5
```

To exercise the retention path locally, age the manifest and rerun:
`jq '.runs[].timestamp -= 86400' manifest.json` puts the previous run in
the 24 h window.

Note: if a Freenet desktop app/node is already running on the machine,
keep the local gateway's `--ws-port` away from 7509 or the node's
single-instance check will refuse to start.
