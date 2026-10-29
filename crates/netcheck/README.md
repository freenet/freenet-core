# netcheck

Synthetic end-to-end check against the **live** Freenet network (issue
#4665). It behaves like a normal client (it only talks to node WebSocket
APIs and never links `freenet-core`) so it measures what a real user
experiences, and it keeps working across core refactors.

Each `put-get` run:

1. PUTs 3 small contracts plus one ~1 MB contract through one gateway's
   WS API.
2. Boots an **ephemeral peer** (`freenet network` child process with an
   empty data dir) that joins the network **through a different gateway**.
   Having no replicas of its own, it cannot answer any of its own GETs.
3. GETs this run's contracts through that peer (verifying the contract's
   instance id, a hash of its code and parameters, and the blake3 of its
   state) together with contracts published by previous runs (24 h / 48 h / 7 d
   windows, tracked in a persistent JSON manifest, verified against their
   blake3 state hashes). The two are issued as **one interleaved
   sequence**: with a fixed 0 h → 24 h → 48 h → 7 d order, "how old the
   contract is" and "how late in the run the GET was issued" were the same
   variable, so an age effect could not be told apart from a
   within-run session effect. The order is shuffled from a seed derived
   from the run id, and that seed is logged next to the order it produced,
   so a run's order can be read back from its own report. (The seed alone
   does not regenerate it: the permutation also depends on how many ops
   the run had, which varies with which retention windows the manifest
   had populated.)
4. Prints one JSON line per operation on stdout and exits non-zero if
   anything failed. No retries by design: an operation that only succeeds
   on retry is the regression netcheck exists to surface.

`put-get` runs both phases in one process. `put` and `get` run them
separately, on different hosts, joined by a handoff file that carries the
run's PUT records and every GET target (this run's contracts and the
retention windows' contracts, resolved against the manifest on the PUT
host). The nightly uses the split form; see "Production use" below.

Scenarios are subcommands sharing the same infrastructure (client,
ephemeral node, manifest, report); future ones (update propagation,
subscriptions) are new modules under `src/scenarios/`.

## What each check actually proves

Every hop that handles a PUT stores the state locally, so a GET answered by
the gateway the PUT went through proves only that the node can hand back a
file it already has. That is why the getter enters through a different
gateway, but the guarantee is narrower than it looks, and it is worth
being precise about it:

**`--gateway-spec` pins the join, not the steady-state connection set.**
Once the ephemeral peer is in the ring, topology maintenance opens further
connections on its own, and the gateway that handled the PUTs can be one of
them (`--min-number-of-connections` defaults to 10, and the public index
lists only two gateways). A local two-gateway run shows this directly: the
peer was pinned to gateway B and reported connections to *both* B and A.
No client-side setting can force the answering peer to be someone else, so
netcheck records what happened instead of pretending otherwise: the
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
{"event":"op","seq":7,"op":"get","age":"24h","label":"20260725-030000/small-0",…,"ok":true,"latency_ms":412,"errors_ignored":0}
```

`freenet_version` is what separates "the network broke" from "the release
that landed last night broke". Without it a failure cannot be attributed
after the fact.

`latency_ms` is a measurement on **both** arms. A failed op reports how
long it actually took, not the configured `--op-timeout-secs`: reporting
the deadline for every failure hid the difference between a fast terminal
error from the node and the client giving up waiting, which is the
distinction the field exists to draw.

`seq` is the operation's 0-based position in the order the run actually
executed. It has to be recorded because nothing else preserves it: every
record of a run is published with the same timestamp, and line order is
lost once the report is parsed. Before the GET order was shuffled,
position could be recovered from `age`; now it cannot, so without `seq`
the shuffle would destroy the very information it was introduced to
disentangle.

`errors_ignored` counts incoming errors this operation attributed to
another contract and skipped. A server-side error for an op that already
hit its own deadline arrives during the *next* op's wait window; charging
it to that op stamps the wrong contract's key into the reported error.
Zero is emitted too, because an absent field would be indistinguishable
from a run that never counted. Read it precisely: it counts errors the
key filter skipped inside an op'''s own wait window. Errors arriving in the
gap *between* ops are discarded by the pre-op drain, unfiltered and
uncounted, so a zero means "the filter did not fire during this op", not
"no stale error existed anywhere near it". Errors that name no contract
stay unattributable and still fail whichever op is waiting, so this can
never swallow a real failure silently.

Both fields reach the jsonl report. Neither reaches the telemetry
dashboard yet: `insert_check_op` writes a fixed column list that has no
`seq` or `errors_ignored` column, so `netcheck emit` publishes them and
the ingest drops them without erroring. Read run order and skipped-error
counts from the jsonl artifact until that table gains the columns
(freenet/freenet-telemetry-dashboard#21).

## Production use (nova + a GitHub-hosted runner)

nova runs BOTH gateways: `freenet-gateway` on network port **31337** and
`freenet-gateway-2` on **31338**. The AWS gateway that used to be the second
host was retired in September 2026.

The nightly (`.github/workflows/netcheck-nightly.yml`, logic in
`scripts/netcheck-nightly.sh`) runs in three jobs:

1. **put**, on nova: `netcheck put` PUTs through gw1's loopback WS API
   (`ws://127.0.0.1:7509`), records the PUTs in the manifest at
   `~/netcheck/manifest.json` (which never leaves nova, so retention keeps
   working across nights), and writes `~/netcheck/handoff.json`, uploaded as
   the `netcheck-handoff` artifact.
2. **get**, on `ubuntu-latest`: downloads the handoff and the latest
   released `freenet` binary, and runs `netcheck get` with the getter pinned
   to **gw2**. Its stdout, the whole run's report including the PUTs, is the
   `netcheck-report` artifact.
3. **publish**, on nova: `netcheck emit` sends that report to the collector
   on nova's loopback, with vantage `github-hosted`. The PUT records carry
   their own vantage (nova's host name), since they ran there. When the PUT
   phase failed, or the get job left no report, it publishes what the PUT
   phase left instead, as a run that could not complete.

What the split changes for anyone reading the series:

- A "0h" GET now runs minutes after its PUT (the job handoff, a build, a
  release download), not about 10 s after. The run line's `put_get_gap_ms`
  says how long, up to the start of the GET phase; the getter's join (up to
  120 s) comes on top. `duration_ms` covers only the time spent in either
  phase.
- The getter's address, and so its ring location, changes every night. The
  run line records both (`ephemeral_addr`, `ephemeral_location`).
- The PUTs are recorded in the manifest before the GETs run, whatever they
  find, and a manifest that cannot be written no longer ends the night early:
  the GETs still run, and the run fails afterwards so the gap in later
  retention windows is not reported as a clean night.

**Why the getter is not on nova.** A node's ring location is derived from
its address with the host bits masked (a whole IPv4 /24 shares one
location), so a getter on nova sits at exactly the gateways' location, and
at that of every other node running there. A peer will not accept a joiner
at its own location (`should_accept` treats it as on neither side), and
routing toward that location ends among those same co-located nodes. From
2026-10-16, with several other nodes running on nova, the getter failed to
join within 120 s on most nights: the check was measuring nova's /24, not
the network.

**Why gw2.** The ephemeral peer must be pinned to a gateway OTHER than the
one the PUTs go through, or a GET can be answered by the node that just
stored the data, which proves transfer, not findability:

```bash
GW2_IP=$(getent hosts gw2.freenet.org | awk '{print $1}' | head -1)
GW2_KEY=$(curl -fsS https://freenet.org/keys/public.gw2.pem)
netcheck get --handoff handoff.json \
             --gateway-spec "${GW2_IP}:31338,${GW2_KEY}"
```

gw1 and gw2 are separate processes on the SAME host, so the GET is answered
by a different node than the PUT, but the PUT side no longer crosses a host
the way it did with the retired gateway. If an off-host gateway exists
again, prefer it here.


## Local testing, fully isolated

Nothing needs the live network to exercise the code path. Two local
gateways reproduce the production topology (gw1 and gw2) on one machine:
PUT through A, join the ephemeral peer through B.

Terminal A, gateway that receives the PUTs:

```bash
cargo build -p freenet
cargo run -p netcheck -- local-gateway \
  --freenet-bin target/debug/freenet --network-port 31338 --ws-port 7609
```

Terminal B, the gateway the getter joins through:

```bash
cargo run -p netcheck -- local-gateway \
  --freenet-bin target/debug/freenet --network-port 31339 --ws-port 7610
```

Terminal C, netcheck, PUTting via A and getting via B:

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

Caveat on what a local run proves: it validates the harness end to end,
compilation, PUT, join, routed GET, manifest, report, but not the signal.
Retention and findability on a two-node local network are trivially
satisfied; only the live network has the storage pressure and topology
that make those properties interesting.
