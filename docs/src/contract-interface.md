# Contract Interface

## Terms

- [Contract State](glossary.md#contract-state) - data associated with a contract that can be retrieved by Applications and Delegates.
- [Delta](glossary.md#delta) - Represents a modification to some state - similar to a [diff](https://en.wikipedia.org/wiki/Diff) in source code
- [Parameters](glossary.md#parameters) - Data that forms part of a contract along with the WebAssembly code
- [State Summary](glossary.md#state-summary) - A compact summary of a contract's state that can be used to create a delta

## Interface

Locutus contracts must implement the contract interface from [crates/locutus-stdlib/src/contract_interface.rs](https://github.com/freenet/locutus/blob/main/crates/locutus-stdlib/src/contract_interface.rs):

```rust,no_run,noplayground
{{#include ../../crates/locutus-stdlib/src/contract_interface.rs:contractifce}}
```

`Parameters`, `State`, and `StateDelta` are all wrappers around simple `[u8]` byte arrays for maximum efficiency and flexibility.

## Contract Interaction

In the (hopefully) near future we'll be adding the ability for contracts to read each other's state while validating and updating their own, see [issue #167](https://github.com/freenet/locutus/issues/167) for the latest on this.
