# Freenet Applications

## Trigger-Action Rules

### WHEN creating a new app

```
1. Create directory structure:
   app-name/
   ├── app/          # Application binary
   ├── contracts/    # WASM contracts
   └── types/        # Shared type definitions

2. Define interface first in types/
3. Implement contract logic in contracts/
4. Build with: cargo run -p fdev -- build
```

### WHEN building contracts

```
cargo run -p fdev -- build
```

### WHEN testing an app

```
Need quick local test?
  → cargo run -p fdev -- test --gateways 1 --nodes 3 single-process

Need realistic network test?
  → cargo run -p fdev -- test --gateways 1 --nodes 5 multi-process
```

### WHEN deploying

```
1. Build: cargo run -p fdev -- build
2. Test locally first
3. See https://freenet.org/resources/manual/ for publishing
```

## Available Apps

| App | Purpose | Use as reference for |
|-----|---------|---------------------|
| `freenet-ping/` | Pub-sub demo | Simple contract + CLI |
| `freenet-email-app/` | Email system | Full WASM app |
| `freenet-microblogging/` | Social network | Web frontend |
