//! A module outside the acceptance rule (`engine::compile_limits`) is refused
//! as `ContractExecError::InvalidModule` by the contract and delegate paths,
//! before anything is compiled, and a refused fresh contract is not left stored.

use std::sync::Arc;

use freenet_stdlib::prelude::*;

use super::super::engine::compile_limits::test_support::nested_blocks;
use super::super::{ContractExecError, ContractRuntimeInterface, Runtime, RuntimeInnerError};
use crate::contract::storages::Storage;
use crate::util::tests::get_temp_dir;

/// A module nested deeper than the limit allows.
fn over_the_limit() -> Vec<u8> {
    nested_blocks(5_000)
}

fn assert_invalid_module<T: std::fmt::Debug>(res: super::super::RuntimeResult<T>) {
    let err = res.expect_err("an over-limit module must be refused");
    assert!(
        matches!(
            err.deref(),
            RuntimeInnerError::ContractExecError(ContractExecError::InvalidModule(_))
        ),
        "expected InvalidModule, got {err}"
    );
}

async fn runtime(dir: &std::path::Path) -> Runtime {
    let db = Storage::new(dir).await.unwrap();
    let contract_store =
        super::super::ContractStore::new(dir.join("contract"), 10_000, db.clone()).unwrap();
    let delegate_store =
        super::super::DelegateStore::new(dir.join("delegate"), 10_000, db.clone()).unwrap();
    let secrets_store =
        super::super::SecretsStore::new(dir.join("secrets"), Default::default(), db).unwrap();
    Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap()
}

/// The contract path: preparing a call for stored code that is over a limit
/// fails with `InvalidModule`, for every entry point that prepares a call.
#[tokio::test(flavor = "multi_thread")]
async fn a_stored_contract_over_a_limit_is_an_invalid_module() {
    let temp_dir = get_temp_dir();
    let mut runtime = runtime(temp_dir.path()).await;
    let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
        Arc::new(ContractCode::from(over_the_limit())),
        vec![].into(),
    )));
    let key = contract.key();
    runtime.contract_store.store_contract(contract).unwrap();
    let params = Parameters::from(vec![1u8]);
    let state = WrappedState::new(vec![0; 8]);
    assert_invalid_module(runtime.validate_state(&key, &params, &state, &Default::default()));
    assert_invalid_module(runtime.summarize_state(&key, &params, &state));
    assert_invalid_module(runtime.get_state_delta(
        &key,
        &params,
        &state,
        &StateSummary::from(vec![]),
    ));
}

/// The contract path with the code supplied by the caller (a fresh PUT).
#[tokio::test(flavor = "multi_thread")]
async fn a_supplied_contract_over_a_limit_is_an_invalid_module() {
    let temp_dir = get_temp_dir();
    let mut runtime = runtime(temp_dir.path()).await;
    let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
        Arc::new(ContractCode::from(over_the_limit())),
        vec![].into(),
    )));
    assert_invalid_module(runtime.validate_state_with_contract(
        &contract.key(),
        &Parameters::from(vec![]),
        &WrappedState::new(vec![1, 2, 3]),
        &Default::default(),
        &contract,
    ));
}

/// The delegate path: running a delegate whose code is over a limit fails with
/// `InvalidModule`.
#[tokio::test(flavor = "multi_thread")]
async fn a_delegate_over_a_limit_is_an_invalid_module() {
    use super::super::DelegateRuntimeInterface;
    use chacha20poly1305::{
        XChaCha20Poly1305,
        aead::{AeadCore, KeyInit, OsRng},
    };

    let temp_dir = get_temp_dir();
    let mut runtime = runtime(temp_dir.path()).await;
    let delegate = DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
        &over_the_limit().into(),
        &vec![].into(),
    ))));
    runtime
        .delegate_store
        .store_delegate(delegate.clone())
        .unwrap();
    let cipher = XChaCha20Poly1305::new(&XChaCha20Poly1305::generate_key(&mut OsRng));
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    runtime
        .secret_store
        .register_delegate(delegate.key().clone(), cipher, nonce)
        .unwrap();
    assert_invalid_module(runtime.inbound_app_message(
        delegate.key(),
        &Parameters::from(vec![]),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(
            ApplicationMessage::new(vec![1, 2, 3]),
        )],
    ));
}
