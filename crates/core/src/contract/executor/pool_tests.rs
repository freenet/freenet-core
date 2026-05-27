//! Tests for the contract executor module.

mod conformance_tests;
mod merge_rejected_tests;
mod non_idempotent_detector_tests;
mod related_contract_tests;
mod runtime_pool_tests;
mod subscriber_limit_tests;
mod subscriber_stress_tests;
#[cfg(feature = "wasmtime-backend")]
mod wasm_conformance_tests;
