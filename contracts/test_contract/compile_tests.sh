#!/usr/bin/bash
RUSTFLAGS="-C link-arg=--import-memory" cargo build --release --target wasm32-unknown-unknown && \
	cp $CARGO_TARGET_DIR/wasm32-unknown-unknown/release/test_contract.wasm ./test_contract_host.wasm 
echo "Compiled using host memory" 

cargo build --release --target wasm32-unknown-unknown && \
	cp $CARGO_TARGET_DIR/wasm32-unknown-unknown/release/test_contract.wasm ./test_contract_guest.wasm 
echo "Compiled using module memory" 
