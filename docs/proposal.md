# Freenet 2 Proposal

## Terminology

### Contract

### Payload

### PayloadUpdate

### Subscription

### Associated Contracts

## Contract interface

```rust
type Payload = [u8]
type Parameters = [u8]

struct Contract {
    operators : Wasm,
    parameters : Parameters,
}

/// Exported contract functions (WASM)
pub trait Contract {
    pub fn is_valid_payload(payload : &Payload) -> bool

    /// Modifies payload to account for payload_update, returns a map of associated contracts
    /// and the corresponding updates for those.
    pub fn is_valid_update(current : &Payload, update : &Payload) -> bool

    /// 
    pub fn get(getter : &Option<[u8]>, payload : &Option<Payload>) -> Result<Payload, Error>
}
```

## Contract Functions

```rust

pub trait Store {
    pub fn put(contract : &Contract, payload_update : &PayloadUpdate)

    pub fn get_contract_payload(contract : &Contract) -> Future<Payload>

    pub fn get_address_payload(address : &Address) -> Future<Payload>

    pub fn subscribe_contract(contract : &Contract) -> Subscription

    pub fn subscribe_address(address : &Address) -> Subscription
}
```

## Overall Architecture

An app is a Contract + WASM code to run in-browser, with access to Store via API.

## Enabling Technologies

* Wasmer - RUST runtime
* https://docs.rs/p2p/0.6.0/p2p/

## Open questions

### Could a request convert into another request, with the response going directly to the original requestor?