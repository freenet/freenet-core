## Contracts

Freenet is essentially a global decentralized key-value store where keys are
WebAssembly code called Contracts. Contracts are stored in the network,
along with their data or "state". The contract controls what state is permitted
and how it can be modified, and also how to efficiently synchronize state
between peers.

A contract's state is just a block of bytes, and can be anything from a simple
number to a complex data structure. The contract's code defines the state's
formatting. Even the serialization format is up to the contract, so it can be
anything from JSON to Bincode, or a custom binary format.

Network users can read a contract's state and subscribe to receive immediate
updates if the state is modified.

Contracts play a similar role in Freenet to databases and real-time
publish-subscribe mechanisms in traditional online services, while being
entirely decentralized, secure, and scalable.

<!-- toc -->

## Contract Operation

### State synchronization and merging

#### Fundamental Concepts

Contracts need to provide a mechanism to merge any two valid states, creating a
new state that integrates both. This process ensures the eventual consistency of
contract states in Freenet, a concept similar to [Conflict-free Replicated Data
Types](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type).

As a very simple example, if the contract's state is a single number, then the
contract could define the merging of two states as the maximum of the two numbers.

In mathematical terms, a contract defines a [commutative
monoid](https://mathworld.wolfram.com/CommutativeMonoid.html) on the contract's
state - but you can ignore this if you're not a mathematician.

#### Efficient State Synchronization

Naively we could transfer the entire state between peers, but this would be
inefficient for larger states. Instead, Freenet transmits only the
difference between states.

To do this a contract implements three functions:

- `summarize_state` - Returns a concise summary of the contract's
  state.
- `get_state_delta` - Compares the contract's state against the summary of
  another state and returns the difference between the two, the "delta".

- `update_state` - Applies a delta to the contract's state, updating it to
  bring it in sync with the other peer's contract state.

Contracts can implement these functions however they wish depending on the
type of data their state contains.

##### Step-by-step

PeerA and PeerB need to synchronize their states. The algorithm for efficient
state synchronization consists of the following steps:

1. **Summarize State by Initiator**: PeerA compiles a concise summary of its
   current state using the `summarize_state` function.

   - This summary is transmitted to PeerB

2. **Compare State at Receiver**: PeerB uses `get_state_delta` to compare the
   summary against its own state.

   - If they are different, proceed to the next step; if not, synchronization is
     complete.

3. **Send Delta**: If the states are different, PeerB calculates the delta and
   sends it to PeerA.

4. **Apply Delta**: PeerA applies this received delta to its state using
   `update_state`.

5. **Reverse Synchronization**: This process is repeated in the opposite direction.

This approach allows peers to synchronize state over the network while minimizing
data transfer.

### Blog Use Case

Consider a public blog contract. The state of this contract would be the blog's
content, including blog posts. The contract's code requires that new
posts can only be added if they are signed by the blog's owner. The owner's
public key is part of the contract's parameters.

The contract would summarize its state by returning a list of post identifiers,
and the state delta would be a list of new posts. The contract would apply the
delta by appending the new posts to its list of posts. The contract may have
a limit on the number of posts it can store, in which case it would remove old
posts to make room for new ones.

### Writing a Contract in Rust

Freenet Contracts can be written in any programming language that compiles to
WebAssembly, but as Freenet is written in Rust it is currently the best supported
language for writing contracts.

#### The `ContractInterface` Trait

Rust contracts implement the `ContractInterface` trait, which defines the
functions that the core calls to interact with the contract. This trait is
defined in the
[freenet-stdlib](https://github.com/freenet/freenet-stdlib/blob/f28e6716364b4e1c9ae8837344286393a2da4c82/rust/src/contract_interface.rs#L446).

```rust,no_run,noplayground
{{#include ../../../stdlib/rust/src/contract_interface.rs:contractifce}}
```

#### Flexibility versus Convenience

The `ContractInterface` trait is a low-level "Layer 0" API that provides direct
access to the contract's state and parameters. This API is useful for contracts
that require fine-grained control over their state, but can be cumbersome.

Soon we will provide higher-level APIs on top of Layer 0 that will sacrifice
some flexibility for ease of contract implementation.
