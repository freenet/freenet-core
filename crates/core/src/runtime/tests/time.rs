//! A test WASM module that checkes that the `time` module in the std lib works correctly.

use wasmer::TypedFunction;

use super::{super::Runtime, TestSetup};

#[test]
fn now() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract("test_contract_2")?;
    let mut runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();

    let module = runtime.prepare_contract_call(&contract_key, &vec![].into(), 1_000)?;
    let f: TypedFunction<(), ()> = module
        .instance
        .exports
        .get_function("time_func")?
        .typed(&runtime.wasm_store)?;
    f.call(&mut runtime.wasm_store)?;
    std::mem::drop(temp_dir);
    Ok(())
}
