# Who can reach the client API

The node's **client API** is the local HTTP/WebSocket surface that applications
talk to: contract reads and writes, subscriptions, delegate messages, the
dashboard. It is fully privileged. Anything that can reach its address and port
can read and modify this node's contract state, its identities, and its key
material. There is no per-connection authentication in front of it.

So the only question that matters is **which hosts can open a socket to it**,
and that is what this page is about.

## The default

**Loopback, in both operation modes.** A node answers the client API on
`127.0.0.1` and `[::1]` only, so applications running on the same machine work
and nothing else can connect.

This is deliberately not tied to `--mode`. Running as a network peer is a
statement about the overlay — that this node routes, stores and forwards for
others. It says nothing about wanting the machine's control API driveable by
whoever else is on the wifi. Earlier releases conflated the two and defaulted
network-mode nodes to every interface.

## Serving clients on other machines

Pick the narrowest option that fits.

| Situation | Flag |
| --- | --- |
| A browser or `riverctl` on another machine on your LAN | `--ws-api-address ::` |
| Only one interface should answer | `--ws-api-address 192.0.2.10` |
| A VPN overlay you control (Tailscale, WireGuard) | `--allowed-source-cidrs 100.64.0.0/10` |
| A reverse proxy on **this** machine | nothing — loopback already works; add `--allowed-host your.domain` so the proxy's `Host` header is accepted |
| A reverse proxy on **another** machine | `--ws-api-address ::` **and** `--allowed-host your.domain` |

`--allowed-source-cidrs` widens the bind on its own, in network mode. It is
inert on a loopback socket — a non-private source can never reach `::1` — so
setting it can only have been meant as "serve non-local clients". Note what it
does *not* do: it never narrows anything. Loopback and the whole of RFC1918 /
IPv6 ULA are always accepted, and this flag adds to that list. Its net effect on
its own is "listen everywhere, accept the entire local network, plus this
range".

`--allowed-host` does **not** widen the bind. It is a `Host` header allowlist,
and it works perfectly on a loopback socket, which is where a same-host reverse
proxy talks to the node. That is also the only arrangement in which hosted
mode's per-user tokens are honoured at all.

## Keep the flag in the invocation

A node persists its resolved configuration to `config.toml` on every boot. That
means a wildcard or loopback `ws-api-address` sitting in that file is just as
likely to be the node's own past output as your choice, and the two are
indistinguishable by value. So those values are **re-derived on every boot**
rather than trusted, and the flag has to stay in whatever launches the node:

- **systemd** — `sudo systemctl edit freenet` (or the user unit) and add a
  drop-in overriding `ExecStart`. Do **not** hand-edit the generated unit file:
  that trips the installer's checksum, permanently marks the unit as
  user-modified, and opts the node out of every future unit-template change.
- **Docker** — `FREENET_WS_API_ADDRESS` in the compose `environment:` block.
- **By hand** — the flag on the command line, every time.

A **specific** address (`192.0.2.10`, `127.0.0.5`) is not a value this code ever
writes, so it is recognised as your choice and preserved untouched.

## Environment variables

These are read as an alternative to the flags, and all three are namespaced:

- `FREENET_WS_API_ADDRESS`
- `FREENET_ALLOWED_HOST`
- `FREENET_ALLOWED_SOURCE_CIDRS`

They were previously `WS_API_ADDRESS`, `ALLOWED_HOST` and
`ALLOWED_SOURCE_CIDRS`. Unnamespaced names that decide whether a privileged API
listens beyond the machine are too easy to set by accident in a shared
container, a CI runner, or a systemd environment. A leftover old-style variable
is reported at startup rather than silently ignored.

## What the node tells you at startup

Three messages, all after the logger is up:

- **The bind was re-derived and narrowed.** Fires once, on the first boot after
  upgrading a node that had been binding every interface. This is the only
  notice you get that clients on other machines have just lost access.
- **The bind was re-derived and widened.** Your config named a loopback address,
  and `--allowed-source-cidrs` widened past it. Said loudly because the socket
  ended up more exposed than the file it replaced.
- **Exposure warning.** The API is reachable from beyond this machine, on a
  node where connections share one namespace (hosted mode off, or a bind that
  hosted mode does not expect). Fires
  for a non-loopback bind, and for `--allowed-host` on a loopback bind with
  hosted mode off — a proxy terminates the connection itself, so every visitor
  arrives looking local and the source-IP filters cannot tell them apart.

## Rolling back

The narrowed `ws-api-address` stays in `config.toml`. An older binary reads it
as an explicit choice, so a rolled-back node comes back loopback-only and has no
message explaining why. Nothing fails to parse — no key changed — but the
previous reachability is not restored automatically. Add `--ws-api-address ::`
to the invocation if you need it back.

## Hosted mode

`--hosted-mode` gives each connection presenting a `userToken` its own
delegate-secret namespace. A hosted node serves its client API only to a
connection that presents a user token: a WebSocket upgrade without one is
refused with 403. A hosted node also refuses the live import and the migration
pull-import, which are for a user's own node. A token is honoured only from a
loopback source carrying
`X-Forwarded-Proto: https`, so a hosted node belongs behind a same-host TLS
proxy on a loopback bind. A non-loopback bind is not what hosted mode expects,
and the exposure warning fires for one regardless.

A hosted node also requires the user token on the permission-prompt routes and
shows each user only their own prompts. `/permission/pending`,
`/permission/{nonce}/respond` and the permission WebSocket
`/permission/events/ws` take the token in the `X-Freenet-User-Token` header (the
WebSocket also accepts `?userToken=`, since a browser cannot set a header on a
WebSocket handshake), under the same loopback and `X-Forwarded-Proto: https`
rule; without it they answer 403. A prompt belongs to the user whose delegate
run raised it: another user's prompt is not listed, not sent, and answers 404
like an unknown one. On a hosted node every delegate run belongs to a user (a
client request runs under its connection's user, an inter-delegate message
under the originating user, and the node runs no delegate on its own), so a
prompt with no user is refused rather than shown to anyone. One user token holds
at most 8 pending prompts; the node-wide limit of 32 is shared by all tokens.
The legacy `/permission/events` stream, the standalone
`/permission/{nonce}` page and the capability-grant routes
(`/permission/grants`, `/permission/grants/revoke`, `/permission/apps`) are not
served on a hosted node. The 64 permission-socket slots are node-wide and
shared by all users; a tab that gets no slot falls back to polling
`/permission/pending`. On a node that is not hosted these routes take no
token and behave as before.

The token travels in the URL query on the two WebSockets (the app socket and
the permission socket), since a browser cannot set a header on a WebSocket
handshake. The node records full request URIs only at debug level, which
release builds compile out; configure the reverse proxy's access log to omit
query strings.

On a loopback bind with hosted mode on, no exposure warning fires, including
when a reverse proxy is in front of the node: every client-API connection
there presents its own user token.
