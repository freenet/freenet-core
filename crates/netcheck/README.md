# netcheck

Synthetic end-to-end check against the **live** Freenet network (issue
#4665). It behaves like a normal client — it only talks to node WebSocket
APIs and never links `freenet-core` — so it measures what a real user
experiences, and it keeps working across core refactors.

Each `put-get` run:

1. PUTs 3 small contracts plus one ~1 MB contract through one gateway's
   WS API.
2. Boots an **ephemeral peer** (`freenet network` child process with an
   empty data dir) that joins the network **through a different gateway**.
   Having no replicas of its own, it cannot answer any of its own GETs.
3. GETs this run's contracts through that peer and verifies byte-identity.
4. Re-GETs contracts published by previous runs (24 h / 48 h / 7 d windows,
   tracked in a persistent JSON manifest) and verifies their blake3 state
   hashes.
5. Prints one JSON line per operation on stdout and exits non-zero if
   anything failed. No retries by design: an operation that only succeeds
   on retry is the regression netcheck exists to surface.

Scenarios are subcommands sharing the same infrastructure (client,
ephemeral node, manifest, report); future ones (update propagation,
subscriptions) are new modules under `src/scenarios/`.

## What each check actually proves

Every hop that handles a PUT stores the state locally, so a GET answered by
the gateway the PUT went through proves only that the node can hand back a
file it already has. That is why the getter enters through a different
gateway — but the guarantee is narrower than it looks, and it is worth
being precise about it:

**`--gateway-spec` pins the join, not the steady-state connection set.**
Once the ephemeral peer is in the ring, topology maintenance opens further
connections on its own, and the gateway that handled the PUTs can be one of
them (`--min-number-of-connections` defaults to 10, and the public index
lists only two gateways). A local two-gateway run shows this directly: the
peer was pinned to gateway B and reported connections to *both* B and A.
No client-side setting can force the answering peer to be someone else, so
netcheck records what happened instead of pretending otherwise — the
`ephemeral_peers` field of the run report carries the addresses actually
connected to. When `--gateway-spec` is omitted entirely, netcheck also
warns on stderr.

The practical consequence, and the way to read a run:

- **Same-day PUT/GET** is a liveness and transfer check. A peer holding the
  contract may be one hop away, so a success does not prove routing.
- **The 24 h / 48 h / 7 d re-GETs carry the findability signal.** Those
  contracts were published by a run that is long gone, against a topology
  that has since changed, and nothing guarantees any currently-connected
  peer still holds them. This is the part that regresses, and the part
  worth alerting on.

## Report format

One `"event":"run"` line with the conditions of the run, then one
`"event":"op"` line per operation:

```json
{"event":"run","run_id":"20260726-030000","gateway_ws":"ws://127.0.0.1:7509",
 "freenet_version":"Freenet version: 0.2.106 (c707af0f786e)",
 "pinned_gateways":["100.27.151.80:31337,c28123df…"],
 "ephemeral_peers":["100.27.151.80:31337"]}
{"event":"op","op":"get","age":"24h","label":"20260725-030000/small-0",…,"ok":true,"latency_ms":412}
```

`freenet_version` is what separates "the network broke" from "the release
that landed last night broke" — without it a failure cannot be attributed
after the fact.

## Production use (nova)

nova runs the primary gateway; the ephemeral peer is pinned to vega (the
secondary) so the GETs are routed. Resolve the address and key from the
public index rather than hardcoding them — the IP and the key can rotate:

```bash
VEGA_IP=$(getent hosts vega.locut.us | awk '{print $1}')
VEGA_KEY=$(curl -fsS https://freenet.org/keys/public.vega.gw.pem)

netcheck put-get \
  --gateway-ws ws://127.0.0.1:7509 \
  --gateway-spec "${VEGA_IP}:31337,${VEGA_KEY}" \
  --freenet-bin /usr/local/bin/freenet \
  --manifest    /home/runner/netcheck/manifest.json \
  --ephemeral-dir /home/runner/netcheck/run \
  --ephemeral-network-port 32177   # UDP port opened for netcheck on nova
```

`--freenet-bin` points at the **installed release** binary on purpose: the
ephemeral peer is the measuring instrument, so it should be the same
binary real users run. A peer built from an unreleased `main` would make
every failure ambiguous.

### Running alongside a real node

netcheck is designed to be safe on a host that also runs a production
gateway, and the guarantees are structural rather than conventional:

- The ephemeral node gets its own `--config-dir`/`--data-dir` and its own
  ports; it never reads or writes the real node's state.
- Child processes are killed **by process handle**, never by name — there
  is no `pkill` anywhere in this crate.
- The node's own single-instance check is read-only and scoped to its
  configured WS port: if the port is busy it logs and exits, it never
  signals the other process. Keep `--ephemeral-ws-port` (default 7519)
  away from the real node's port.
- `--disable-auto-update` is passed to the ephemeral node: it lives for
  minutes, and detecting a new release mid-run would make it exit 42 and
  fail the check for a reason unrelated to the network.

Two things the caller is responsible for:

- **`--ephemeral-dir`.** Left unset, the node works in an anonymous temp
  dir that is only cleaned up on a normal exit; a killed run (a cancelled
  CI job) leaks it. The node's hosting cache is budgeted at `RAM/8` capped
  at 1 GiB, so on a large host a leak is not small. Point it at a known
  path and clear that path before each run.
- **Disk headroom.** Check free space before running on a shared host and
  skip the run rather than filling the disk out from under the real node.

## Local testing, fully isolated

Nothing needs the live network to exercise the code path. Two local
gateways reproduce the nova/vega topology on one machine: PUT through A,
join the ephemeral peer through B.

Terminal A — gateway that receives the PUTs:

```bash
cargo build -p freenet
cargo run -p netcheck -- local-gateway \
  --freenet-bin target/debug/freenet --network-port 31338 --ws-port 7609
```

Terminal B — the gateway the getter joins through:

```bash
cargo run -p netcheck -- local-gateway \
  --freenet-bin target/debug/freenet --network-port 31339 --ws-port 7610
```

Terminal C — netcheck, PUTting via A and getting via B:

```bash
cargo run -p netcheck -- put-get \
  --gateway-ws ws://127.0.0.1:7609 \
  --gateway-spec "<spec printed by terminal B>" \
  --freenet-bin target/debug/freenet \
  --ephemeral-network-port 31400 --ephemeral-ws-port 7519 \
  --manifest /tmp/netcheck-manifest.json --settle-secs 5
```

To exercise the retention path, age the manifest and rerun:
`jq '.runs[].timestamp -= 86400' manifest.json` puts the previous run in
the 24 h window.

Note: if a Freenet desktop app or node is already running on the machine,
keep every `--ws-port` away from 7509 or the node's single-instance check
will refuse to start.

Caveat on what a local run proves: it validates the harness end to end —
compilation, PUT, join, routed GET, manifest, report — but not the signal.
Retention and findability on a two-node local network are trivially
satisfied; only the live network has the storage pressure and topology
that make those properties interesting.
