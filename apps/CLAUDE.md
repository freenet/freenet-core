# Freenet Applications

## WHEN creating a new app

Follow `freenet-ping/`'s layout (`app/`, `contracts/`, `types/`): define the
interface in `types/` first, implement contract logic in `contracts/`, then
build with `cargo run -p fdev -- build`.

## WHEN testing an app

```
Need quick local test?
  → cargo run -p fdev -- test --gateways 1 --nodes 3 single-process

Need realistic network test?
  → cargo run -p fdev -- test --gateways 1 --nodes 5 network
```

## WHEN deploying

Build (`cargo run -p fdev -- build`), test locally, then see
https://freenet.org/resources/manual/ for publishing.

> **Note:** `freenet-email-app` has been extracted to its own repo: https://github.com/freenet/freenet-email
