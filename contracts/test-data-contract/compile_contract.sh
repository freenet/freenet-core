#!/usr/bin/bash
cargo build --release --target wasm32-unknown-unknown && \
	cp $CARGO_TARGET_DIR/wasm32-unknown-unknown/release/test_web_contract.wasm ./test_web_contract.wasm 
cargo build --release --target wasm32-wasi && \
	cp $CARGO_TARGET_DIR/wasm32-wasi/release/test_web_contract.wasm ./test_web_contract.wasi.wasm 
echo "Compiled using module memory" 
