#!/usr/bin/bash
RUSTFLAGS="-C link-arg=--import-memory" cargo build --release --target wasm32-unknown-unknown && \
	cp $CARGO_TARGET_DIR/wasm32-unknown-unknown/release/test_contract.wasm ./test_contract_host.wasm 
RUSTFLAGS="-C link-arg=--import-memory" cargo build --release --target wasm32-wasi && \
	cp $CARGO_TARGET_DIR/wasm32-wasi/release/test_contract.wasm ./test_contract_host.wasi.wasm 
echo "Compiled using host memory" 

cargo build --release --target wasm32-unknown-unknown && \
	cp $CARGO_TARGET_DIR/wasm32-unknown-unknown/release/test_contract.wasm ./test_contract_guest.wasm 
cargo build --release --target wasm32-wasi && \
	cp $CARGO_TARGET_DIR/wasm32-wasi/release/test_contract.wasm ./test_contract_guest.wasi.wasm 
echo "Compiled using module memory" 

echo "Package state"
