# freenet-website-contract

Freenet contract for hosting signed, versioned static websites. Used by `fdev website` to publish
static sites to Freenet without writing any contract code.

## WASM Binary Management

The compiled WASM is **committed** at `crates/fdev/resources/website_contract.wasm` and embedded
into `fdev` at compile time via `include_bytes!`. It is NOT recompiled when building fdev.

This ensures all installations of the same fdev version produce identical contract keys for the
same signing keypair.

### When to rebuild

If you change the contract source (`src/lib.rs`) or its dependencies, you must manually rebuild
and commit the updated WASM:

```bash
cargo build -p freenet-website-contract \
    --target wasm32-unknown-unknown \
    --release \
    --no-default-features \
    --features freenet-main-contract,contract

cp target/wasm32-unknown-unknown/release/freenet_website_contract.wasm \
    crates/fdev/resources/website_contract.wasm
```

**Changing the WASM changes all contract keys.** Every user's `fdev website init` will produce a
different contract key with the new WASM. Existing websites published with the old WASM will
continue to work, but users cannot update them with a new fdev version unless they use
`--contract-wasm` to specify the old WASM.

## Contract Design

- **State format:** `[metadata_len: u64 BE][CBOR metadata][web_len: u64 BE][website.tar.xz]`
- **Metadata:** version (u32) + Ed25519 signature over `version_bytes || archive_bytes`
- **Parameters:** 32-byte Ed25519 verifying key (determines contract key)
- **Validation:** signature verification, version-only-increases on update
