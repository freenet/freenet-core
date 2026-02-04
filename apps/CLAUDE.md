# Freenet Applications

Example applications demonstrating Freenet development patterns.

## Applications

| App | Purpose | Components |
|-----|---------|------------|
| `freenet-ping/` | Simple pub-sub demo | Contract + CLI app |
| `freenet-email-app/` | Email system | Full WASM application |
| `freenet-microblogging/` | Social network | Web app example |

## Building Apps

```bash
# Build contracts with fdev
cargo run -p fdev -- build

# Install app binary
cargo install --path apps/freenet-ping/app
```

## Structure

Each app typically has:
```
app-name/
├── app/          # Application binary
├── contracts/    # WASM contracts
└── types/        # Shared type definitions
```

## Testing

Apps can be tested using:

```bash
# Single-process simulation
cargo run -p fdev -- test --gateways 1 --nodes 3 single-process

# Multi-process network
cargo run -p fdev -- test --gateways 1 --nodes 5 multi-process
```

## Contract Development

Contracts are compiled to WASM and deployed to the network:

1. Define contract interface in `types/`
2. Implement contract logic in `contracts/`
3. Build with `fdev build`
4. Test locally before publishing

See https://freenet.org/resources/manual/ for contract development guide.
