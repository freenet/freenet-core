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
`otel-endpoint` and are handled by the SDK:
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`, `OTEL_EXPORTER_OTLP_ENDPOINT`,
`OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_TIMEOUT`,
`OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_METRIC_EXPORT_INTERVAL`
(default 60s). With none of them and no `otel-endpoint`, the exporter uses
`http://localhost:4318`.

When an environment variable overrides a configured `otel-endpoint`, the node
logs a warning at startup naming both, and the "OTel metrics exporter started"
line always names the endpoint actually in use.

## Authentication

For most setups leave `otel-auth-mode` at `disabled` and carry whatever
credentials your collector wants in `OTEL_EXPORTER_OTLP_HEADERS`:

```bash
OTEL_EXPORTER_OTLP_HEADERS="Authorization=Basic $(printf 'user:pass' | base64)"
```

An `Authorization` header set that way is never overwritten by the node.

`otel-auth-mode = "freenet"` is for collectors that verify freenet node
identities. It adds

```
Authorization: Bearer freenet/<pubkey>/<audience>/<timestamp>/<signature>
```

to each export, where `<signature>` is an XEdDSA signature over the preceding
fields made with the node's transport key, `<pubkey>` is that key in base58,
and `<audience>` is the collector's `host:port`. It proves the metrics came
from the node they claim to, and the audience field means a token sent to one
collector cannot be replayed at another. Do not enable it for a collector that
does not check these tokens — it ships a signed assertion of your node's
identity to whatever it is pointed at.

## Identifying a node

Every export batch carries two resource attributes:

- `freenet.node.pubkey` — the node's full transport public key, base58. This is
  the value a collector verifies the bearer token against.
- `freenet.node.fingerprint` — the short form shown in UIs and the local
  dashboard, for cross-referencing.

Neither contains an address, so a node keeps the same identity across IP
changes.

## Notes

- `freenet.process.memory.rss` is Linux-only. On macOS and Windows the series
  is empty; that is expected, not a broken pipeline.
- Export failures never affect the node: a collector that is down or rejecting
  batches produces a log line and nothing else.
