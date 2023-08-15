## Contracts

Freenet is essentially a global decentralized key-value store where keys are
WebAssembly code that we call "contracts". Contracts are stored in the network
along with their data or "state". The contract controls what state is permitted
and how it can be modified.

Network users can read a contract's state, and subscribe to receive immediate
updates if the state is modified.

Contracts play a similar role in Freenet to databases and realtime
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

In the language of mathematics, the contract defines a commutative monoid on the
contract's state. For example, if the contract's state is a single number, then
the contract could define the merging of two states as the sum of the two
numbers. However, these basic operations are too simple on their own but can be
combined with others to support the merging of more complex states.

#### Efficient State Synchronization

A naive approach to state synchronization would be to transfer the entire state
between peers, but this approach is very inefficient for large states. Instead,
Freenet contracts utilize a much more efficient and flexible approach to state
synchronization by providing an implementation of three functions:

* `summarize_state` - Returns a concise summary of the contract's
  state.
* `get_state_delta` - Compares the contract's state against the summary of
  another state and returns the difference between the two, the "delta".

* `update_state` - Applies a delta to the contract's state, updating it to
  bring it in sync with the other state.

Contracts can implement these functions however they wish depending on the
type of data being synchronized.

##### Step-by-step

PeerA and PeerB need to synchronize their states. The algorithm for efficient
   state synchronization comprises the following steps:

1. **Summarize State by Initiator**: PeerA compiles a concise summary of its
   current state using the `summarize_state` function.
   * This summary is transmitted to PeerB

2. **Compare State at Receiver**: PeerB uses `get_state_delta` to compare the
   summary against its own state.
   * If they are different, proceed to the next step; if not, synchronization is
     complete.

3. **Send Delta**: If the states are different, PeerB calculates the delta and
   sends it to PeerA.

4. **Apply Delta**: PeerA applies this received delta to its state using
   `update_state`.

5. **Reverse Synchronization**: This process is repeated in the opposite.

This approach allows peers to synchronize state over the network while minimizing
data transfer.

### Blog Use Case

Consider a public blog contract. The state of this contract would be the blog's
content, including a list of blog posts. The contract's code requires that new
posts can only be added if they are signed by the blog's owner, the owner's
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
functions that the kernel calls to interact with the contract. This trait is
defined in the
[locutus-stdlib](https://github.com/freenet/locutus/blob/main/crates/locutus-stdlib/src/contract_interface.rs#L424).

```rust,no_run,noplayground
{{#include ../../../crates/locutus-stdlib/src/contract_interface.rs:contractifce}}
```

#### Flexibility versus Convenience

The `ContractInterface` trait is a low-level "Layer 0" API that provides direct
access to the contract's state and parameters. This API is useful for contracts
that require fine-grained control over their state, but can be cumbersome.

We will provide higher-level APIs on top of Layer 0 that will sacrafice
some flexibility for ease of contract implementation.