# Code Style Guidelines

## Rust Conventions

- Use `rustfmt` defaults (run `cargo fmt`)
- Follow `clippy` lints (`cargo clippy --all-targets --all-features`)
- Prefer explicit error handling over `.unwrap()` in production code
- Use `thiserror` for custom error types

## Module Organization

```rust
// Ordering within modules:
// 1. Module declarations
mod submodule;

// 2. Imports (grouped: std, external, crate)
use std::collections::HashMap;
use tokio::sync::mpsc;
use crate::config::Config;

// 3. Type definitions
struct MyStruct { ... }

// 4. Trait implementations
impl MyTrait for MyStruct { ... }

// 5. Functions
fn helper() { ... }
```

## Error Handling

```rust
// Production code: explicit handling
match operation() {
    Ok(result) => process(result),
    Err(e) => handle_error(e),
}

// Test code: .unwrap() / .expect() acceptable
let result = operation().expect("operation should succeed");
```

## Async Patterns

- Use `tokio::select!` for multiplexing
- Prefer channels over shared state
- Document cancellation safety when relevant

## Key Abstractions

| Concern | Use | Location |
|---------|-----|----------|
| Time | `TimeSource` trait | `crates/core/src/simulation/` |
| RNG | `GlobalRng` | `crates/core/src/config/mod.rs` |
| Sockets | `Socket` trait | `crates/core/src/transport/` |

## Documentation

- Document public APIs with `///` comments
- Include examples for complex functions
- Explain "why" in implementation comments, not "what"
