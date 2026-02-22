//! Tests for the contract executor module.

mod conformance_tests;
mod mediator_tests;
mod runtime_pool_tests;
#[cfg(feature = "wasmtime-backend")]
mod wasm_conformance_tests;
