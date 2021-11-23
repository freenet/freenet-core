
# Computation authenticity

When nodes perform computations we can't assume they'll do them faithfully.

Our solution is to use the Karma system, when a node does a computation it
must stake some karma on the computation being correct, to be eligible to
be compensated for the computation.

While the stake is still "open" other nodes can verify the computation.
Nodes that verify successfully can share the reward.

If they discover the computation is incorrect then they can stake karma on their
correct computation and challenge the incorrect.

The incorrect computation contract would then 

# Terms

## contract

A contract is the fundamental unit of computation in the Locutus distributed computer.
It is web assembly code implementing two functions:

    * retrieve() : Requests the value associated with this contract and a specific parameter set
              This may involve requesting some data stored there, or perhaps computing
              the data

    * modify() : Modifies the value associated with this contract and a specific parameter set  
                 using a payload. This will typically include verifying the payload in some way.
                 
                 s a general rule, the order in which modifications are applied to a contract
                 should not affect the eventual contract state (ordering invariant).


## parameters
A set of parameters to the contract, these and the contract code are combined to determine
the address of the contract on the ring.

## state
This is the "memory" of the contract, nodes will cache this for their contracts. This will
typically be modified by sending an update message to the contract, although in some situations
(caching, distributed computation) it may be modified by a get().

## payload
This is code that can be sent to the contract, potentially causing it to modify its state.
The network mechanism should ensure that valid updates propagate rapidly to all nodes
holding that contract.

# UseCases


```rust

// QUESTIONS
// 1. How do updates propogate? Perhaps need separate execute_with_payload? or put?

trait Contract {

   /// Execute the contract, passing it parameters, return a result payload
    fn retrieve(&self, parameters : &[u8], budget : &Budget) -> &Result<[u8], ContractError>;

    /// Execute the contract, passing it parameters and a payload, returns an optional result payload
    fn modify(&self, parameters : &[u8], payload : &[u8], budget : &Budget) -> &Result<Option<[u8]>, ContractError>;
}

/// A SignedValueContract 
impl Contract for SignedValueContract {
    /// Execute the contract, passing it parameters, return a result payload
    fn retrieve(&self, parameters : &[u8], state : Optional<&[u8]>, budget : &Budget) -> &Result<[u8], ContractError> {
        if let Some(state) = state {
            return Result::Ok(decode_state(state));
        } else {
            return Result::Err(ContractError::NoState);
        }
    }

    fn modify(&self, parameters : &[u8], payload : &[u8], state : &mut [u8], budget : &Budget) -> Result<[u8], ContractError> {
        // The budget can be used to fail the computation early if it's clear it will be too 
        // expensive.  This is highly recommended but not required as budgets are enforced outside the
        // wasm sandbox.

        let parameters = decode_modify_parameters(parameters);
        if (payload.is_empty()) {
            // No payload, return the associated data
            return Result::Ok(self.associated_data());
        } else {
            let payload = decode_payload(payload);
            if (parameters.public_key.verify(&payload.value, &payload.signature)( P
            ) {
                // Valid signature, return the associated data
                associated_data().replace_with(payload.value);
            } else {
                // Invalid signature, return an error
                return Result::Err(ContractError::InvalidPayload);
            }
            )
        }
    }

```


enum ContractError {
    NoState,
    InvalidPayload,
    InsufficientCPUGas(estimate : Option<u64>),
    InsufficientStorageGas(estimate : Option<u64>),
    InsuffficientInboundBandwidth(estimate : Option<u64>),
    InsuffficientOutboundBandwidth(estimate : Option<u64>),
}

trait ContractStd {

    ///
    fn get(contract : [u8], parameters : [u8], budget : &Budget) -> [u8];

    /// Access the contract's associated data - this is the contract's state, it may be
    /// modified and potentially enlarged depending on remaining_storage_gas().
    fn associated_data() -> Vec<u8>;

}

struct Budget {
    cpu_gas : u64,
    storage_gas : u64,
    inbound_bandwidth_gas : u64,
    outbound_bandwidth_gas : u64,
}

