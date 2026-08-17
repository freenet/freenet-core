# Exporting node metrics to an OpenTelemetry collector

A freenet node can export its own metrics — transport, ring, contract queue,
process RSS — to any OTLP/HTTP collector. It is **off by default** and
completely separate from `telemetry-enabled`, which feeds the project's central
dashboard: turning one on or off has no effect on the other.

Design notes and the full instrument list are in
[`docs/design/otel-metrics-exporter.md`](design/otel-metrics-exporter.md).

## Turning it on

In `config.toml`:

```toml
otel-telemetry-enabled = true
otel-endpoint = "http://collector.example:4318"
```

or on the command line:

```bash
freenet network --otel-telemetry-enabled --otel-endpoint http://collector.example:4318
```

`--otel-telemetry-enabled=false` turns it off again without editing the file.
`FREENET_OTEL_TELEMETRY_ENABLED` works too, and unlike a plain flag it honors
`=false`.

Nodes started with `--id` (test networks, the integration harness) never
export, regardless of configuration.

## Settings

| `config.toml` key | Default | Meaning |
|---|---|---|
| `otel-telemetry-enabled` | `false` | Enable the exporter |
| `otel-endpoint` | none | Collector base URL, e.g. `http://collector:4318`. `/v1/metrics` is appended for you |
| `otel-auth-mode` | `disabled` | `disabled` sends no `Authorization` header; `freenet` sends a signed bearer token (below) |

The standard OpenTelemetry environment variables take priority over
`otel-endpoint`: `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`,
`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`,
`OTEL_EXPORTER_OTLP_TIMEOUT`, `OTEL_EXPORTER_OTLP_METRICS_TIMEOUT`,
`OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_METRIC_EXPORT_INTERVAL`
(default 60s). With none of them and no `otel-endpoint`, the exporter uses
`http://localhost:4318`.

**The two endpoint variables are not interchangeable.** The signal-specific
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` is used exactly as written, so it must
include the full path:

```bash
OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://collector:4318/v1/metrics   # full path
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318                      # base URL only
```

Getting this backwards produces a 404 on every export — and, in `freenet` auth
mode, an audience hash over the wrong path. Include the scheme in either form:
a bare `collector:4318` is accepted as a URL but cannot be sent, and the node
warns about it at startup.

The timeout variables are in **milliseconds** (`OTEL_EXPORTER_OTLP_TIMEOUT=10`
is 10ms, not 10 seconds, and every export will time out). The node warns on a
suspiciously small value.

`OTEL_EXPORTER_OTLP_COMPRESSION` is **not supported** — the compression
features are deliberately not compiled in, and setting the variable makes the
exporter fail to start, leaving the node with no metrics at all. The startup
warning names the cause.

When an environment variable overrides a configured `otel-endpoint`, the node
logs a warning at startup naming both, and the "OTel metrics exporter started"
line always names the endpoint actually in use.

## Authentication

For most setups leave `otel-auth-mode` at `disabled` and carry whatever
credentials your collector wants in `OTEL_EXPORTER_OTLP_HEADERS`:

```bash
OTEL_EXPORTER_OTLP_HEADERS="Authorization=Basic $(printf 'user:pass' | base64)"
```

An `Authorization` header set that way is never overwritten by the node — so
static header auth, the usual way OTLP endpoints are authenticated, already
works in the default mode without any freenet-specific configuration.

`otel-auth-mode = "freenet"` is for collectors that verify freenet node
identities. It adds

```
Authorization: Bearer freenet/<pubkey>/<audience>/<timestamp>/<signature>
```

to each export, where `<signature>` is an XEdDSA signature over the preceding
fields made with the node's transport key, `<pubkey>` is that key in base58,
and `<audience>` identifies the exact URL the export was sent to. It proves the
metrics came from the node they claim to, and the audience field means a token
sent to one collector cannot be replayed at another. Do not enable it for a
collector that does not check these tokens — it ships a signed assertion of
your node's identity to whatever it is pointed at.

`<audience>` is base58 of the first 16 bytes of `SHA-256` over the canonical
target, which is `{host}:{port}{path}` — host lowercased, port always explicit
(filled in from the scheme as 80 or 443 when the URL omits it), path verbatim,
any `user:password@` stripped, and no scheme. So an endpoint of
`http://collector.example:4318` produces the audience for
`collector.example:4318/v1/metrics`, which you can reproduce with:

```bash
# base58 has no standard CLI; this uses Python, which is universally available.
printf 'collector.example:4318/v1/metrics' \
  | openssl dgst -sha256 -binary | head -c 16 \
  | python3 -c 'import sys;d=sys.stdin.buffer.read();n=int.from_bytes(d,"big");a="123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";s=""
while n: n,r = divmod(n,58); s = a[r]+s
print("1"*(len(d)-len(d.lstrip(b"\0")))+s)'
```

If your collector rejects tokens with an audience mismatch, compare that value
against the endpoint in the node's "OTel metrics exporter started" log line —
the two must name the same URL, including the path your ingress finally
delivers to.

## Identifying a node

Every export batch carries two resource attributes:

- `freenet.node.pubkey` — the node's full transport public key, base58. This is
  the value a collector verifies the bearer token against.
- `freenet.node.fingerprint` — the short form shown in UIs and the local
  dashboard, for cross-referencing.

Neither contains an address, so a node keeps the same identity across IP
changes.

Both are exported in **every** auth mode, `disabled` included. `disabled`
withholds the SIGNATURE, not the identity: it means "do not assert to this
collector, cryptographically, that this node is who it says it is", not "do
not say which node this is" — the metrics would be useless without a node id
to group them by. If you do not want a node identifiable to a collector, do
not export to that collector.

## Notes

- `freenet.process.memory.rss` is Linux-only. On macOS and Windows the series
  is empty; that is expected, not a broken pipeline.
- Export failures never affect the node: a collector that is down or rejecting
  batches produces a `WARN` naming the endpoint and the reason, and nothing
  else. The warning is logged once per failing streak, with an `INFO` when
  exports recover — not once per 60s interval.
