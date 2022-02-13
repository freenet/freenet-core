#!/usr/bin/bash
RUSTFLAGS="-C link-arg=--import-memory" cargo build --release --target wasm32-unknown-unknown/release/test_contract.wasm ./test_contract_host.wasm
cargo build --release --target wasm32-unknown-unknown/release/test_contract.wasm ./test_contract_prog.wasm  
