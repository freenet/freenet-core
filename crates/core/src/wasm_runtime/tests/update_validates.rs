//! A contract declares that its `update_state` validates its own input and
//! output by exporting `__frnt__update_validates` with type `() -> i32`.
//! Presence of the export with that exact type is the whole declaration.

use std::sync::Arc;

use freenet_stdlib::prelude::*;

use super::super::{ContractRuntimeInterface, Runtime};
use crate::contract::storages::Storage;
use crate::util::tests::get_temp_dir;

async fn declares(wat: &str) -> bool {
    let wasm = wat::parse_str(wat).unwrap_or_else(|e| panic!("bad test WAT: {e}\n{wat}"));
    let temp_dir = get_temp_dir();
    let db = Storage::new(temp_dir.path()).await.unwrap();
    let mut contract_store =
        super::super::ContractStore::new(temp_dir.path().join("contract"), 10_000, db.clone())
            .unwrap();
    let delegate_store =
        super::super::DelegateStore::new(temp_dir.path().join("delegate"), 10_000, db.clone())
            .unwrap();
    let secrets_store =
        super::super::SecretsStore::new(temp_dir.path().join("secrets"), Default::default(), db)
            .unwrap();
    let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
        Arc::new(ContractCode::from(wasm)),
        vec![].into(),
    )));
    let key = contract.key();
    contract_store.store_contract(contract).unwrap();
    let mut runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();
    let first = runtime.update_validates(&key, &Parameters::from(vec![]));
    // The answer comes from the cached module and does not change.
    let second = runtime.update_validates(&key, &Parameters::from(vec![]));
    assert_eq!(first, second);
    first
}

#[tokio::test]
async fn an_export_of_the_right_type_opts_in() {
    assert!(
        declares(
            r#"(module (func (export "__frnt__update_validates") (result i32) (i32.const 1)))"#
        )
        .await
    );
}

#[tokio::test]
async fn the_return_value_is_not_consulted() {
    assert!(
        declares(
            r#"(module (func (export "__frnt__update_validates") (result i32) (i32.const 0)))"#
        )
        .await
    );
}

#[tokio::test]
async fn a_module_without_the_export_does_not_opt_in() {
    assert!(!declares(r#"(module (func (export "other") (result i32) (i32.const 1)))"#).await);
}

#[tokio::test]
async fn an_export_with_the_wrong_signature_does_not_opt_in() {
    for wat in [
        r#"(module (func (export "__frnt__update_validates") (result i64) (i64.const 1)))"#,
        r#"(module (func (export "__frnt__update_validates") (param i32) (result i32) (local.get 0)))"#,
        r#"(module (func (export "__frnt__update_validates")))"#,
        r#"(module (func (export "__frnt__update_validates") (result i32 i32) (i32.const 1) (i32.const 1)))"#,
    ] {
        assert!(!declares(wat).await, "{wat}");
    }
}

#[tokio::test]
async fn a_non_function_export_of_that_name_does_not_opt_in() {
    assert!(
        !declares(r#"(module (global (export "__frnt__update_validates") i32 (i32.const 1)))"#)
            .await
    );
}

/// Core reads the declaration from the export table and never runs it.
#[test]
fn the_export_is_never_called() {
    let src = include_str!("../engine/wasmtime_engine.rs");
    let body = src
        .split("fn module_declares_update_validates(")
        .nth(1)
        .and_then(|rest| rest.split("\n    }\n").next())
        .expect("module_declares_update_validates must exist");
    assert!(body.contains("get_export"), "{body}");
    for forbidden in ["call", "instantiate", "create_instance"] {
        assert!(!body.contains(forbidden), "{forbidden} in {body}");
    }
}
